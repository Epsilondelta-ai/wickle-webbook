use crate::{AzureCredentialProvider, error};
use reqwest::{Client, Url};
use serde_json::json;
use std::{fmt, sync::Arc, time::Duration};
use wickle::*;

/// Azure resource, deployment and finite transport bounds supplied by the Host.
#[derive(Debug, Clone)]
pub struct AzureOpenAiOptions {
    /// Resource origin, for example `https://example.openai.azure.com/`.
    pub endpoint: String,
    /// Exact deployed name sent as the Responses request's `model` field.
    pub deployment: String,
    /// Optional ARM account resource ID, required by the deployment inspector.
    pub resource_id: Option<String>,
    /// Maximum connection establishment time.
    pub connect_timeout: Duration,
    /// Maximum duration of one HTTP request.
    pub request_timeout: Duration,
    /// Maximum raw response bytes.
    pub max_transport_bytes: usize,
    /// Maximum raw bytes per SSE event.
    pub max_event_bytes: usize,
    /// Maximum raw SSE data records, independent of normalized event limits.
    pub max_protocol_events: usize,
}
impl AzureOpenAiOptions {
    /// Select a resource and deployment, with bounded default transport settings.
    pub fn new(endpoint: impl Into<String>, deployment: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            deployment: deployment.into(),
            resource_id: None,
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(120),
            max_transport_bytes: 8 * 1024 * 1024,
            max_event_bytes: 1024 * 1024,
            max_protocol_events: 16_384,
        }
    }
}

/// A scoped Azure connection; construction does not authenticate or access the network.
#[derive(Clone)]
pub struct AzureOpenAiConnection(pub(crate) Arc<Connection>);
pub(crate) struct Connection {
    pub client: Client,
    pub base: Url,
    pub scope: Scope,
    pub binding: ModelPortBinding,
    pub target: JsonObject,
    pub credentials: Arc<dyn AzureCredentialProvider>,
    pub options: AzureOpenAiOptions,
}
impl fmt::Debug for AzureOpenAiConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AzureOpenAiConnection")
            .field("binding", &self.0.binding)
            .finish_non_exhaustive()
    }
}
impl AzureOpenAiConnection {
    /// Bind explicit Host credentials to one resource, deployment and scope.
    pub fn new(
        scope: Scope,
        connection_ref: VersionedRef,
        credentials: Arc<dyn AzureCredentialProvider>,
        mut options: AzureOpenAiOptions,
    ) -> Result<Self, ContractError> {
        let origin = origin(&options.endpoint)?;
        if options.deployment.is_empty()
            || options
                .deployment
                .chars()
                .any(|c| c.is_whitespace() || c.is_control() || "/?#%".contains(c))
            || options.connect_timeout.is_zero()
            || options.request_timeout.is_zero()
            || options.max_transport_bytes == 0
            || options.max_event_bytes == 0
            || options.max_protocol_events == 0
            || options.max_event_bytes > options.max_transport_bytes
        {
            return Err(error(ErrorCode::InvalidConfiguration, "connection"));
        }
        options.endpoint = origin.as_str().into();
        let mut target = JsonObject::from([
            ("endpoint".into(), json!(options.endpoint)),
            ("deployment".into(), json!(options.deployment)),
        ]);
        if let Some(resource_id) = &options.resource_id {
            validate_resource_id(resource_id)?;
            target.insert("resource_id".into(), json!(resource_id));
        }
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(options.connect_timeout)
            .timeout(options.request_timeout)
            .build()
            .map_err(|_| error(ErrorCode::ComponentUnavailable, "client"))?;
        let base = origin
            .join("openai/v1/")
            .map_err(|_| error(ErrorCode::InvalidConfiguration, "endpoint"))?;
        Ok(Self(Arc::new(Connection {
            client,
            base,
            scope,
            target,
            credentials,
            options,
            binding: ModelPortBinding {
                provider: Id::new("azure-openai")?,
                adapter: VersionedRef {
                    id: Id::new("wickle-model-azure-openai")?,
                    version: Id::new(env!("CARGO_PKG_VERSION"))?,
                },
                connection_ref,
            },
        })))
    }
    /// Adapter and credential revision used for route dispatch.
    pub fn binding(&self) -> ModelPortBinding {
        self.0.binding.clone()
    }
    /// Canonical resource/deployment target to register in the model binding.
    pub fn target(&self) -> &JsonObject {
        &self.0.target
    }
    /// Connection owner used for authorization.
    pub fn scope(&self) -> &Scope {
        &self.0.scope
    }
    /// This adapter supports Responses v1, without a dated `api-version` query.
    pub fn api_contract() -> ApiContract {
        ApiContract {
            operation: Id::new("responses").expect("static identifier"),
            version: Id::new("v1").expect("static identifier"),
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
            || route.api_contract != Self::api_contract()
        {
            return Err(error(ErrorCode::ModelBindingInvalid, "route"));
        }
        Ok(())
    }
}
pub(crate) fn origin(value: &str) -> Result<Url, ContractError> {
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
pub(crate) fn validate_resource_id(value: &str) -> Result<(), ContractError> {
    let parts: Vec<_> = value.split('/').collect();
    if parts.len() != 9
        || !parts[0].is_empty()
        || !parts[1].eq_ignore_ascii_case("subscriptions")
        || !parts[3].eq_ignore_ascii_case("resourceGroups")
        || !parts[5].eq_ignore_ascii_case("providers")
        || !parts[6].eq_ignore_ascii_case("Microsoft.CognitiveServices")
        || !parts[7].eq_ignore_ascii_case("accounts")
        || parts[1..].iter().any(|part| {
            part.is_empty()
                || *part == "."
                || *part == ".."
                || part.chars().any(|c| c.is_control() || "?#%".contains(c))
        })
    {
        return Err(error(ErrorCode::InvalidConfiguration, "resource_id"));
    }
    Ok(())
}
