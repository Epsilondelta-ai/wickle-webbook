use std::{fmt, sync::Arc, time::Duration};

use reqwest::{
    Client, Url,
    header::{CONTENT_TYPE, HeaderMap, HeaderValue},
};
use serde_json::json;
use wickle::*;

use crate::error;

/// Explicit transport settings. The library does not read environment files.
#[derive(Debug, Clone)]
pub struct GeminiOptions {
    /// API base directory, normally `https://generativelanguage.googleapis.com/`.
    pub base_url: String,
    /// Explicit REST API generation: `v1` or `v1beta`.
    pub api_version: String,
    /// Finite time allowed to establish a connection.
    pub connect_timeout: Duration,
    /// Upper bound for any request; the core's deadline may be shorter.
    pub request_timeout: Duration,
    /// Maximum raw SSE/JSON response bytes, including protocol envelopes.
    pub max_transport_bytes: usize,
    /// Maximum buffered bytes in a normalized SSE event, including its JSON envelope.
    pub max_event_bytes: usize,
    /// Maximum SSE data frames, including provider metadata and progress events.
    pub max_protocol_events: usize,
}

impl Default for GeminiOptions {
    fn default() -> Self {
        Self {
            base_url: "https://generativelanguage.googleapis.com/".into(),
            api_version: "v1".into(),
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(120),
            max_transport_bytes: 8 * 1024 * 1024,
            max_event_bytes: 1024 * 1024,
            max_protocol_events: 16_384,
        }
    }
}

/// A scope-bound HTTP connection with an explicit credential revision.
#[derive(Clone)]
pub struct GeminiConnection(pub(crate) Arc<Connection>);

pub(crate) struct Connection {
    pub client: Client,
    pub base: Url,
    pub scope: Scope,
    pub binding: ModelPortBinding,
    pub target: JsonObject,
    pub options: GeminiOptions,
}

impl fmt::Debug for GeminiConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GeminiConnection")
            .field("binding", &self.0.binding)
            .finish_non_exhaustive()
    }
}

impl GeminiConnection {
    /// Create a client without a network call. Never log the supplied API key.
    /// HTTPS is required except for explicitly configured loopback test servers.
    pub fn new(
        scope: Scope,
        connection_ref: VersionedRef,
        api_key: &str,
        mut options: GeminiOptions,
    ) -> Result<Self, ContractError> {
        if api_key.is_empty()
            || api_key.chars().any(char::is_whitespace)
            || options.connect_timeout.is_zero()
            || options.request_timeout.is_zero()
            || options.max_transport_bytes == 0
            || options.max_event_bytes == 0
            || options.max_protocol_events == 0
            || options.max_event_bytes > options.max_transport_bytes
        {
            return Err(error(ErrorCode::InvalidConfiguration, "connection"));
        }
        let mut base = Url::parse(&options.base_url)
            .map_err(|_| error(ErrorCode::InvalidConfiguration, "base_url"))?;
        let loopback = base.host_str().is_some_and(|host| {
            let address = host
                .strip_prefix('[')
                .and_then(|value| value.strip_suffix(']'))
                .unwrap_or(host);
            host == "localhost"
                || address
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if !(base.scheme() == "https" || (base.scheme() == "http" && loopback))
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(error(ErrorCode::InvalidConfiguration, "base_url"));
        }
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }
        options.base_url = base.as_str().into();
        let mut headers = HeaderMap::new();
        let mut authorization = HeaderValue::from_str(api_key)
            .map_err(|_| error(ErrorCode::InvalidConfiguration, "credential"))?;
        authorization.set_sensitive(true);
        headers.insert("x-goog-api-key", authorization);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if !matches!(options.api_version.as_str(), "v1" | "v1beta") || base.path() != "/" {
            return Err(error(
                ErrorCode::InvalidConfiguration,
                "api_version_or_origin",
            ));
        }
        let target = JsonObject::from([("base_url".into(), json!(base.as_str()))]);
        let client = Client::builder()
            .default_headers(headers)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(options.connect_timeout)
            .timeout(options.request_timeout)
            .build()
            .map_err(|_| error(ErrorCode::ComponentUnavailable, "client"))?;
        Ok(Self(Arc::new(Connection {
            client,
            base,
            scope,
            target,
            options,
            binding: ModelPortBinding {
                provider: Id::new("google-gemini")?,
                adapter: VersionedRef {
                    id: Id::new("wickle-model-gemini")?,
                    version: Id::new(env!("CARGO_PKG_VERSION"))?,
                },
                connection_ref,
            },
        })))
    }
    /// Exact actual adapter and connection identities for catalog registration.
    pub fn binding(&self) -> ModelPortBinding {
        self.0.binding.clone()
    }
    /// Canonical endpoint identity required in the selected route.
    pub fn target(&self) -> &JsonObject {
        &self.0.target
    }
    /// Owner namespace for this connection.
    pub fn scope(&self) -> &Scope {
        &self.0.scope
    }
    /// The one supported API operation and protocol generation.
    pub fn api_contract(&self) -> ApiContract {
        ApiContract {
            operation: Id::new("stream_generate_content").expect("static identifier"),
            version: Id::new(&self.0.options.api_version).expect("static identifier"),
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
