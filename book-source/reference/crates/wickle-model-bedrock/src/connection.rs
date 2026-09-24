use crate::{BedrockCredentialProvider, error};
use reqwest::{Client, Url};
use serde_json::json;
use std::{fmt, sync::Arc, time::Duration};
use wickle::*;

/// AWS endpoint family, with its own SigV4 service identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BedrockEndpoint {
    /// Standard Bedrock runtime.
    Runtime,
    /// Bedrock Mantle.
    Mantle,
}
/// Explicit inference wire contract; endpoint and model support are checked separately.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BedrockOperation {
    /// Native Anthropic Messages with SSE.
    Messages,
    /// Runtime InvokeModelWithResponseStream with AWS event-stream framing.
    InvokeStream,
}
/// Preserve the exact model/profile selector separately from the route's model release.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BedrockSelector {
    /// Foundation model ID or foundation-model ARN.
    Foundation(String),
    /// Inference profile ID or ARN.
    InferenceProfile(String),
}
impl BedrockSelector {
    /// Actual inference selector, without changing prefixes or ARN encoding.
    pub fn value(&self) -> &str {
        match self {
            Self::Foundation(v) | Self::InferenceProfile(v) => v,
        }
    }
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Foundation(_) => "foundation",
            Self::InferenceProfile(_) => "inference_profile",
        }
    }
}
/// Explicit Host configuration. No environment variables are read by this crate.
#[derive(Clone, Debug)]
pub struct BedrockOptions {
    /// Request origin and signing region; not a claim about execution location.
    pub region: String,
    /// Inference endpoint family.
    pub endpoint: BedrockEndpoint,
    /// Selected wire operation.
    pub operation: BedrockOperation,
    /// Exact model/profile target.
    pub selector: BedrockSelector,
    /// Optional HTTPS origin override, or explicit loopback HTTP for fixtures.
    pub endpoint_url: Option<String>,
    /// Optional control-plane HTTPS origin override, or loopback test origin.
    pub metadata_url: Option<String>,
    /// Optional profile destination restriction, enforced by metadata inspection.
    /// Empty means no destination-location guarantee.
    pub allowed_destination_regions: Vec<String>,
    /// Connection establishment bound.
    pub connect_timeout: Duration,
    /// Bound on a physical request; the call context can impose a shorter deadline.
    pub request_timeout: Duration,
    /// Maximum raw transport bytes.
    pub max_transport_bytes: usize,
    /// Maximum one SSE record or AWS event-stream frame.
    pub max_event_bytes: usize,
    /// Maximum raw protocol events, independent of normalized ModelEvent limits.
    pub max_protocol_events: usize,
}
impl BedrockOptions {
    /// Default to the Runtime native Messages route for an explicitly chosen region.
    pub fn new(region: impl Into<String>, selector: BedrockSelector) -> Self {
        Self {
            region: region.into(),
            endpoint: BedrockEndpoint::Runtime,
            operation: BedrockOperation::Messages,
            selector,
            endpoint_url: None,
            metadata_url: None,
            allowed_destination_regions: vec![],
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(120),
            max_transport_bytes: 8 * 1024 * 1024,
            max_event_bytes: 1024 * 1024,
            max_protocol_events: 16384,
        }
    }
}
/// Scoped connection with an explicit credential revision.
#[derive(Clone)]
pub struct BedrockConnection(pub(crate) Arc<Connection>);
#[derive(Clone)]
pub(crate) struct Connection {
    pub client: Client,
    pub url: Url,
    pub metadata_base: Url,
    pub options: BedrockOptions,
    pub scope: Scope,
    pub binding: ModelPortBinding,
    pub target: JsonObject,
    pub credentials: Arc<dyn BedrockCredentialProvider>,
    pub clock: Arc<dyn Clock>,
}
impl fmt::Debug for BedrockConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BedrockConnection")
            .field("binding", &self.0.binding)
            .finish_non_exhaustive()
    }
}
impl BedrockConnection {
    /// Construct without network access or implicit AWS credential-chain loading.
    pub fn new(
        scope: Scope,
        connection_ref: VersionedRef,
        credentials: Arc<dyn BedrockCredentialProvider>,
        mut options: BedrockOptions,
    ) -> Result<Self, ContractError> {
        if !region(&options.region)
            || options.selector.value().is_empty()
            || options.selector.value().chars().any(char::is_control)
            || options.connect_timeout.is_zero()
            || options.request_timeout.is_zero()
            || options.max_transport_bytes == 0
            || options.max_event_bytes < 16
            || options.max_event_bytes > options.max_transport_bytes
            || options.max_protocol_events == 0
            || (options.endpoint == BedrockEndpoint::Mantle
                && options.operation == BedrockOperation::InvokeStream)
            || options
                .allowed_destination_regions
                .iter()
                .any(|v| !region(v))
            || (!options.allowed_destination_regions.is_empty()
                && matches!(options.selector, BedrockSelector::Foundation(_)))
        {
            return Err(error(ErrorCode::InvalidConfiguration, "connection"));
        }
        options.allowed_destination_regions.sort();
        options.allowed_destination_regions.dedup();
        let default = match options.endpoint {
            BedrockEndpoint::Runtime => {
                format!("https://bedrock-runtime.{}.amazonaws.com/", options.region)
            }
            BedrockEndpoint::Mantle => {
                format!("https://bedrock-mantle.{}.api.aws/", options.region)
            }
        };
        let base = origin(options.endpoint_url.as_deref().unwrap_or(&default))?;
        let mut url = base.clone();
        match options.operation {
            BedrockOperation::Messages => url.set_path("/anthropic/v1/messages"),
            BedrockOperation::InvokeStream => {
                url.path_segments_mut()
                    .map_err(|_| error(ErrorCode::InvalidConfiguration, "endpoint"))?
                    .clear()
                    .push("model")
                    .push(options.selector.value())
                    .push("invoke-with-response-stream");
            }
        }
        let metadata_default = format!("https://bedrock.{}.amazonaws.com/", options.region);
        let metadata_base = origin(options.metadata_url.as_deref().unwrap_or(&metadata_default))?;
        let target = JsonObject::from([
            ("endpoint".into(), json!(base.as_str())),
            (
                "endpoint_kind".into(),
                json!(match options.endpoint {
                    BedrockEndpoint::Runtime => "runtime",
                    BedrockEndpoint::Mantle => "mantle",
                }),
            ),
            ("region".into(), json!(options.region)),
            (
                "selector".into(),
                json!({"kind":options.selector.kind(),"value":options.selector.value()}),
            ),
            ("metadata_endpoint".into(), json!(metadata_base.as_str())),
            (
                "allowed_destination_regions".into(),
                json!(options.allowed_destination_regions),
            ),
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
            url,
            metadata_base,
            options,
            scope,
            target,
            credentials,
            clock: Arc::new(SystemClock::new()),
            binding: ModelPortBinding {
                provider: Id::new("aws-bedrock")?,
                adapter: VersionedRef {
                    id: Id::new("wickle-model-bedrock")?,
                    version: Id::new(env!("CARGO_PKG_VERSION"))?,
                },
                connection_ref,
            },
        })))
    }
    /// Use the Host's clock for request signing; timers still obey the call deadline.
    pub fn with_clock(self, clock: Arc<dyn Clock>) -> Self {
        Self(Arc::new(Connection {
            clock,
            ..(*self.0).clone()
        }))
    }
    /// Concrete provider/adapter/credential identity for the catalog.
    pub fn binding(&self) -> ModelPortBinding {
        self.0.binding.clone()
    }
    /// Canonical endpoint, selector, origin region and destination policy.
    pub fn target(&self) -> &JsonObject {
        &self.0.target
    }
    /// Owner namespace.
    pub fn scope(&self) -> &Scope {
        &self.0.scope
    }
    /// Operation and protocol version selected at construction.
    pub fn api_contract(&self) -> ApiContract {
        match self.0.options.operation {
            BedrockOperation::Messages => ApiContract {
                operation: Id::new("messages").expect("static"),
                version: Id::new("2023-06-01").expect("static"),
            },
            BedrockOperation::InvokeStream => ApiContract {
                operation: Id::new("invoke_model_with_response_stream").expect("static"),
                version: Id::new("bedrock-2023-05-31").expect("static"),
            },
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
        {
            return Err(error(ErrorCode::ModelBindingInvalid, "route"));
        }
        Ok(())
    }
    pub(crate) fn signing_service(&self) -> &'static str {
        match self.0.options.endpoint {
            BedrockEndpoint::Runtime => "bedrock",
            BedrockEndpoint::Mantle => "bedrock-mantle",
        }
    }
}
fn region(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
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
