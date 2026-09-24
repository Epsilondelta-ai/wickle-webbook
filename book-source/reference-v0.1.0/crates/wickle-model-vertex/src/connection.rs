use crate::{VertexAudience, VertexTokenContext, VertexTokenProvider, error};
use reqwest::{
    Client, Url,
    header::{HeaderMap, HeaderValue},
};
use serde_json::json;
use std::{
    fmt,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};
use wickle::*;
/// Explicit Cloud resource and transport configuration. No environment lookup.
#[derive(Clone, Debug)]
pub struct VertexOptions {
    /// Google Cloud resource project ID or number.
    pub project: String,
    /// Defaults to global. A regional or us/eu multi-region target is explicit.
    pub location: String,
    /// Selected REST API version; this adapter implements v1.
    pub api_version: String,
    /// Optional quota/billing project sent as x-goog-user-project.
    pub quota_project: Option<String>,
    /// Optional inference origin override; HTTPS or explicit loopback HTTP.
    pub endpoint_url: Option<String>,
    /// Optional metadata origin override. Defaults to the global control plane.
    pub metadata_url: Option<String>,
    /// Connection establishment bound.
    pub connect_timeout: Duration,
    /// HTTP request timeout, further limited by the core call deadline.
    pub request_timeout: Duration,
    /// Bound on raw response bytes.
    pub max_transport_bytes: usize,
    /// Bound on one SSE frame.
    pub max_event_bytes: usize,
    /// Bound on raw SSE records.
    pub max_protocol_events: usize,
}
impl VertexOptions {
    /// Configure the resource project with a global inference endpoint.
    pub fn new(project: impl Into<String>) -> Self {
        Self {
            project: project.into(),
            location: "global".into(),
            api_version: "v1".into(),
            quota_project: None,
            endpoint_url: None,
            metadata_url: None,
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(120),
            max_transport_bytes: 8 * 1024 * 1024,
            max_event_bytes: 1024 * 1024,
            max_protocol_events: 16384,
        }
    }
}
/// Scope-bound connection with an explicit credential revision and token provider.
#[derive(Clone)]
pub struct VertexConnection(pub(crate) Arc<Connection>);
#[derive(Clone)]
pub(crate) struct Connection {
    pub client: Client,
    pub base: Url,
    pub metadata_base: Url,
    pub options: VertexOptions,
    pub scope: Scope,
    pub binding: ModelPortBinding,
    pub target: JsonObject,
    pub tokens: Arc<dyn VertexTokenProvider>,
    pub clock: Arc<dyn Clock>,
}
impl fmt::Debug for VertexConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VertexConnection")
            .field("binding", &self.0.binding)
            .finish_non_exhaustive()
    }
}
impl VertexConnection {
    /// Construct without network requests or implicit ADC loading.
    pub fn new(
        scope: Scope,
        connection_ref: VersionedRef,
        tokens: Arc<dyn VertexTokenProvider>,
        options: VertexOptions,
    ) -> Result<Self, ContractError> {
        if !resource(&options.project)
            || !resource(&options.location)
            || options.quota_project.as_ref().is_some_and(|s| !resource(s))
            || options.api_version != "v1"
            || options.connect_timeout.is_zero()
            || options.request_timeout.is_zero()
            || options.max_transport_bytes == 0
            || options.max_event_bytes == 0
            || options.max_event_bytes > options.max_transport_bytes
            || options.max_protocol_events == 0
        {
            return Err(error(ErrorCode::InvalidConfiguration, "connection"));
        }
        let default = match options.location.as_str() {
            "global" => "https://aiplatform.googleapis.com/".into(),
            "us" | "eu" => format!(
                "https://aiplatform.{}.rep.googleapis.com/",
                options.location
            ),
            region => format!("https://{region}-aiplatform.googleapis.com/"),
        };
        let base = origin(options.endpoint_url.as_deref().unwrap_or(&default))?;
        let metadata_base = origin(
            options
                .metadata_url
                .as_deref()
                .unwrap_or("https://aiplatform.googleapis.com/"),
        )?;
        let target = JsonObject::from([
            ("project".into(), json!(options.project)),
            ("location".into(), json!(options.location)),
            ("endpoint".into(), json!(base.as_str())),
            ("metadata_endpoint".into(), json!(metadata_base.as_str())),
            ("quota_project".into(), json!(options.quota_project)),
        ]);
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(options.connect_timeout)
            .timeout(options.request_timeout)
            .build()
            .map_err(|_| error(ErrorCode::ComponentUnavailable, "client"))?;
        Ok(Self(Arc::new(Connection {
            client,
            base,
            metadata_base,
            options,
            scope,
            target,
            tokens,
            clock: Arc::new(SystemClock::new()),
            binding: ModelPortBinding {
                provider: Id::new("google-vertex")?,
                adapter: VersionedRef {
                    id: Id::new("wickle-model-vertex")?,
                    version: Id::new(env!("CARGO_PKG_VERSION"))?,
                },
                connection_ref,
            },
        })))
    }
    /// Inject the Host clock for expiry checks after credential resolution.
    pub fn with_clock(self, clock: Arc<dyn Clock>) -> Self {
        Self(Arc::new(Connection {
            clock,
            ..(*self.0).clone()
        }))
    }
    /// Actual provider, adapter and credential identities.
    pub fn binding(&self) -> ModelPortBinding {
        self.0.binding.clone()
    }
    /// Canonical resource, location, endpoints and quota identity.
    pub fn target(&self) -> &JsonObject {
        &self.0.target
    }
    /// Connection owner namespace.
    pub fn scope(&self) -> &Scope {
        &self.0.scope
    }
    /// The explicit supported operation and REST generation.
    pub fn api_contract(&self) -> ApiContract {
        ApiContract {
            operation: Id::new("stream_generate_content").expect("static"),
            version: Id::new("v1").expect("static"),
        }
    }
    pub(crate) fn validate(
        &self,
        route: &ResolvedModelRoute,
        scope: &Scope,
    ) -> Result<(), ContractError> {
        if scope != &self.0.scope {
            return Err(error(ErrorCode::AccessDenied, "scope"));
        }
        if !self.0.binding.matches_route(route)
            || route.target != self.0.target
            || route.api_contract != self.api_contract()
            || route.deployment_revision.is_some()
        {
            return Err(error(ErrorCode::ModelBindingInvalid, "route"));
        }
        Ok(())
    }
    pub(crate) async fn headers(
        &self,
        audience: VertexAudience,
        cancellation: &tokio_util::sync::CancellationToken,
        deadline: tokio::time::Instant,
    ) -> Result<HeaderMap, ContractError> {
        let context = VertexTokenContext {
            scope: &self.0.scope,
            project: &self.0.options.project,
            location: &self.0.options.location,
            audience,
            cancellation,
            deadline,
        };
        let token = tokio::select! {biased;
            _=cancellation.cancelled()=>return Err(error(ErrorCode::Cancelled,"token")),
            _=tokio::time::sleep_until(deadline)=>return Err(error(ErrorCode::DeadlineExceeded,"token")),
            result=self.0.tokens.token(&context)=>result.map_err(|_|error(ErrorCode::AccessDenied,"token"))?,
        };
        if let Some(expiry) = token.expires_at {
            let millis = u64::try_from(self.0.clock.now()?.utc_ms)
                .map_err(|_| error(ErrorCode::ClockUnavailable, "token_time"))?;
            let now = UNIX_EPOCH
                .checked_add(Duration::from_millis(millis))
                .ok_or_else(|| error(ErrorCode::ClockUnavailable, "token_time"))?;
            if expiry <= now {
                return Err(error(ErrorCode::AccessDenied, "expired_token"));
            }
        }
        let mut headers = HeaderMap::new();
        let mut value = HeaderValue::from_str(&format!("Bearer {}", token.value))
            .map_err(|_| error(ErrorCode::AccessDenied, "token"))?;
        value.set_sensitive(true);
        headers.insert("authorization", value);
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        if let Some(project) = &self.0.options.quota_project {
            headers.insert(
                "x-goog-user-project",
                HeaderValue::from_str(project)
                    .map_err(|_| error(ErrorCode::InvalidConfiguration, "quota_project"))?,
            );
        }
        Ok(headers)
    }
}
fn resource(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}
pub(crate) fn model_name(value: &str) -> Result<&str, ContractError> {
    let value = value.strip_prefix("models/").unwrap_or(value);
    if value.is_empty()
        || matches!(value, "." | "..")
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(error(ErrorCode::ModelBindingInvalid, "model_name"));
    }
    Ok(value)
}
fn origin(value: &str) -> Result<Url, ContractError> {
    let url = Url::parse(value).map_err(|_| error(ErrorCode::InvalidConfiguration, "endpoint"))?;
    let loopback = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(error(ErrorCode::InvalidConfiguration, "endpoint"));
    }
    Ok(url)
}
