# 26장 전체 Rust 구현과 테스트

[강의로](../26-azure.md) · [전체 변경 패치](../solutions/26-azure.patch)

기준 `0f63057ed587f2f456dc2e92bfd585036164220a`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle-model-azure-openai/src/auth.rs`

```rust
use crate::error;
use reqwest::header::{HeaderMap, HeaderValue};
use std::fmt;
use wickle::*;

/// The authentication audience requested by this adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AzureAudience {
    /// Azure OpenAI inference endpoint.
    Inference,
    /// Azure Resource Manager deployment metadata.
    Management,
}

/// Host-owned authorization lookup, with a bounded lifetime and explicit scope.
pub struct AzureCredentialContext<'a> {
    /// Connection owner; credentials must be authorized for this scope.
    pub scope: &'a Scope,
    /// Service audience; management always requires an Entra token.
    pub audience: AzureAudience,
    /// Cooperative cancellation while obtaining or refreshing a token.
    pub cancellation: &'a tokio_util::sync::CancellationToken,
    /// Effective deadline for authorization and its HTTP request.
    pub deadline: tokio::time::Instant,
}

/// Credential material is supplied by the Host and never written to a catalog.
#[derive(Clone)]
pub enum AzureCredential {
    /// Resource API key, sent in the `api-key` header for inference only.
    ApiKey(String),
    /// Entra access token for the requested audience, refreshed by the Host.
    EntraToken(String),
}
impl fmt::Debug for AzureCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ApiKey(_) => "AzureCredential::ApiKey([redacted])",
            Self::EntraToken(_) => "AzureCredential::EntraToken([redacted])",
        })
    }
}

/// Called once per physical request; the Host owns token acquisition and renewal.
pub trait AzureCredentialProvider: Send + Sync {
    /// Return credentials for the connection scope and requested service audience.
    fn credential<'a>(
        &'a self,
        context: &'a AzureCredentialContext<'a>,
    ) -> PortFuture<'a, AzureCredential>;
}
impl AzureCredentialProvider for AzureCredential {
    fn credential<'a>(
        &'a self,
        _: &'a AzureCredentialContext<'a>,
    ) -> PortFuture<'a, AzureCredential> {
        Box::pin(async { Ok(self.clone()) })
    }
}

pub(crate) async fn authorize(
    provider: &dyn AzureCredentialProvider,
    context: AzureCredentialContext<'_>,
) -> Result<HeaderMap, ContractError> {
    let credential = tokio::select! { biased;
        _ = context.cancellation.cancelled() => return Err(error(ErrorCode::Cancelled, "credential")),
        _ = tokio::time::sleep_until(context.deadline) => return Err(error(ErrorCode::DeadlineExceeded, "credential")),
        result = provider.credential(&context) => result.map_err(|_| error(ErrorCode::AccessDenied, "credential"))?,
    };
    let (name, value) = match credential {
        AzureCredential::ApiKey(key) if context.audience == AzureAudience::Inference => {
            ("api-key", key)
        }
        AzureCredential::EntraToken(token) => {
            validate(&token)?;
            ("authorization", format!("Bearer {token}"))
        }
        _ => return Err(error(ErrorCode::AccessDenied, "credential_audience")),
    };
    // HeaderValue's sensitive bit also keeps HTTP diagnostic formatting redacted.
    if name == "api-key" {
        validate(&value)?;
    }
    let mut value = HeaderValue::from_str(&value)
        .map_err(|_| error(ErrorCode::AccessDenied, "credential_format"))?;
    value.set_sensitive(true);
    let mut headers = HeaderMap::new();
    headers.insert(name, value);
    Ok(headers)
}
fn validate(value: &str) -> Result<(), ContractError> {
    if value.is_empty() || value.chars().any(char::is_whitespace) {
        return Err(error(ErrorCode::AccessDenied, "credential_format"));
    }
    Ok(())
}
```

## `crates/wickle-model-azure-openai/src/connection.rs`

```rust
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
```

## `crates/wickle-model-azure-openai/src/inspection.rs`

```rust
use crate::{
    AzureAudience, AzureCredentialContext, AzureCredentialProvider, AzureOpenAiConnection, auth,
    connection::origin, error,
};
use serde_json::{Value, json};
use std::sync::Arc;
use wickle::*;

/// Azure Resource Manager connection for deployment metadata, separate from inference.
#[derive(Debug, Clone)]
pub struct AzureInspectionOptions {
    /// Management origin; supports explicit loopback HTTP for transport tests.
    pub endpoint: String,
    /// Exact ARM protocol version, unrelated to the model or inference API version.
    pub api_version: String,
}
impl Default for AzureInspectionOptions {
    fn default() -> Self {
        Self {
            endpoint: "https://management.azure.com/".into(),
            api_version: "2025-06-01".into(),
        }
    }
}

/// Observes the actual model release behind a mutable Azure deployment.
#[derive(Clone)]
pub struct AzureOpenAiInspector {
    connection: AzureOpenAiConnection,
    credentials: Arc<dyn AzureCredentialProvider>,
    url: reqwest::Url,
    resource_id: String,
}
impl AzureOpenAiInspector {
    /// Configure an ARM GET with Host-supplied management credentials.
    /// No request occurs until `inspect`; a data-plane API key cannot authorize ARM.
    pub fn new(
        connection: AzureOpenAiConnection,
        credentials: Arc<dyn AzureCredentialProvider>,
        options: AzureInspectionOptions,
    ) -> Result<Self, ContractError> {
        // This schema is verified against the selected protocol generation only.
        if options.api_version != "2025-06-01" {
            return Err(error(
                ErrorCode::InvalidConfiguration,
                "management_api_version",
            ));
        }
        let account = connection
            .0
            .options
            .resource_id
            .as_ref()
            .ok_or_else(|| error(ErrorCode::InvalidConfiguration, "resource_id"))?;
        let resource_id = format!("{account}/deployments/{}", connection.0.options.deployment);
        let mut url = origin(&options.endpoint)?;
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| error(ErrorCode::InvalidConfiguration, "management_endpoint"))?;
            segments.clear();
            for segment in resource_id.trim_start_matches('/').split('/') {
                segments.push(segment);
            }
        }
        url.query_pairs_mut()
            .append_pair("api-version", &options.api_version);
        Ok(Self {
            connection,
            credentials,
            url,
            resource_id,
        })
    }
}
impl ModelRouteInspector for AzureOpenAiInspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        context: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            self.connection.validate(route, &context.scope)?;
            let operation = async {
                let headers = auth::authorize(
                    self.credentials.as_ref(),
                    AzureCredentialContext {
                        scope: &context.scope,
                        audience: AzureAudience::Management,
                        cancellation: &context.cancellation,
                        deadline: context.deadline,
                    },
                )
                .await?;
                let mut response = self
                    .connection
                    .0
                    .client
                    .get(self.url.clone())
                    .headers(headers)
                    .send()
                    .await
                    .map_err(|_| {
                        error(ErrorCode::ModelInspectionUnavailable, "management_request")
                    })?;
                if response.status().as_u16() == 404 {
                    return Ok(ModelRouteObservation {
                        route_digest: route.digest(),
                        availability: ModelRouteAvailability::Unavailable,
                        model_id: None,
                        model_version: None,
                        deployment_revision: None,
                        version_semantics: VersionSemantics::MutableDeployment,
                        evidence_ref: Id::new("azure.arm.deployments.get")?,
                    });
                }
                if !response.status().is_success() {
                    return Err(error(
                        ErrorCode::ModelInspectionUnavailable,
                        "management_status",
                    ));
                }
                let mut bytes = Vec::new();
                while let Some(chunk) = response
                    .chunk()
                    .await
                    .map_err(|_| error(ErrorCode::ModelInspectionUnavailable, "management_body"))?
                {
                    if bytes.len().saturating_add(chunk.len())
                        > 65_536.min(self.connection.0.options.max_transport_bytes)
                    {
                        return Err(error(
                            ErrorCode::ModelInspectionUnavailable,
                            "management_limit",
                        ));
                    }
                    bytes.extend_from_slice(&chunk);
                }
                let body = std::str::from_utf8(&bytes)
                    .ok()
                    .and_then(|text| parse_json(text).ok())
                    .ok_or_else(|| {
                        error(ErrorCode::ModelInspectionUnavailable, "management_json")
                    })?;
                if !body
                    .get("id")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value.eq_ignore_ascii_case(&self.resource_id))
                    || body.get("name").and_then(Value::as_str)
                        != Some(self.connection.0.options.deployment.as_str())
                    || !body
                        .get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|value| {
                            value.eq_ignore_ascii_case(
                                "Microsoft.CognitiveServices/accounts/deployments",
                            )
                        })
                {
                    return Err(error(ErrorCode::ModelVersionDrift, "management_target"));
                }
                if body
                    .pointer("/properties/model/format")
                    .and_then(Value::as_str)
                    != Some("OpenAI")
                {
                    return Err(error(
                        ErrorCode::ModelInspectionUnavailable,
                        "management_model_format",
                    ));
                }
                let model_id = identifier(&body, "/properties/model/name")?;
                let model_version = identifier(&body, "/properties/model/version")?;
                let availability = match body
                    .pointer("/properties/provisioningState")
                    .and_then(Value::as_str)
                {
                    Some("Succeeded") => ModelRouteAvailability::Available,
                    Some("Failed" | "Canceled" | "Deleted") => ModelRouteAvailability::Unavailable,
                    _ => ModelRouteAvailability::Unknown,
                };
                // A metadata fingerprint detects observed changes; it does not
                // make a mutable deployment immutable or prevent later upgrades.
                let revision = canonical_digest(&json!({
                    "resource_id": self.resource_id,
                    "model": body.pointer("/properties/model"),
                    "upgrade": body.pointer("/properties/versionUpgradeOption"),
                    "etag": body.get("etag"),
                }));
                Ok(ModelRouteObservation {
                    route_digest: route.digest(),
                    availability,
                    model_id: Some(model_id),
                    model_version: Some(model_version),
                    deployment_revision: Some(Id::new(revision.as_str())?),
                    version_semantics: VersionSemantics::MutableDeployment,
                    evidence_ref: Id::new("azure.arm.deployments.get.2025-06-01")?,
                })
            };
            tokio::select! { biased;
                _ = context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "inspection")),
                _ = tokio::time::sleep_until(context.deadline) => Err(error(ErrorCode::DeadlineExceeded, "inspection")),
                result = operation => result,
            }
        })
    }
}
fn identifier(value: &Value, path: &str) -> Result<Id, ContractError> {
    value
        .pointer(path)
        .and_then(Value::as_str)
        .and_then(|value| Id::new(value).ok())
        .ok_or_else(|| error(ErrorCode::ModelInspectionUnavailable, "management_model"))
}
```

## `crates/wickle-model-azure-openai/src/lib.rs`

```rust
//! Azure OpenAI Responses v1 with Host-owned authorization and explicit deployment identity.
//! Model metadata uses a separate Azure Resource Manager authorization path.
#![forbid(unsafe_code)]
mod auth;
mod connection;
mod inspection;
mod model;
pub use auth::{AzureAudience, AzureCredential, AzureCredentialContext, AzureCredentialProvider};
pub use connection::{AzureOpenAiConnection, AzureOpenAiOptions};
pub use inspection::{AzureInspectionOptions, AzureOpenAiInspector};
pub use model::AzureOpenAiModel;
use wickle::{ContractError, ErrorCode};
fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("azure_openai.{location}"))
}
```

## `crates/wickle-model-azure-openai/src/model.rs`

```rust
use crate::{AzureAudience, AzureCredentialContext, AzureOpenAiConnection, auth, error};
use futures_util::stream;
use reqwest::Response;
use serde_json::Value;
use std::collections::VecDeque;
use wickle::*;
use wickle_model_responses::{ResponsesDecoder, SseDecoder, encode_request};

/// One Azure OpenAI Responses POST per invocation, streamed into Wickle model events.
/// The adapter does not retry, run Tool handlers, or load environment variables.
#[derive(Debug, Clone)]
pub struct AzureOpenAiModel {
    connection: AzureOpenAiConnection,
}
impl AzureOpenAiModel {
    /// Bind an already configured connection without making a network request.
    pub fn new(connection: AzureOpenAiConnection) -> Self {
        Self { connection }
    }
}
impl ModelPort for AzureOpenAiModel {
    fn binding(&self) -> ModelPortBinding {
        self.connection.binding()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let state = State {
            connection: &self.connection,
            request,
            context,
            response: None,
            decoder: ResponsesDecoder::new(request, None),
            framing: SseDecoder::new(
                self.connection.0.options.max_transport_bytes,
                self.connection.0.options.max_event_bytes,
                self.connection.0.options.max_protocol_events,
            ),
            queue: VecDeque::new(),
            started: false,
            finished: false,
        };
        Box::pin(stream::unfold(state, |mut state| async move {
            loop {
                if !state.finished && state.context.cancellation.is_cancelled() {
                    state.queue.clear();
                    state.fail(error(ErrorCode::Cancelled, "stream"));
                }
                if !state.finished && tokio::time::Instant::now() >= state.context.deadline {
                    state.queue.clear();
                    state.fail(error(ErrorCode::DeadlineExceeded, "stream"));
                }
                if let Some(event) = state.queue.pop_front() {
                    return Some((event, state));
                }
                if state.finished {
                    return None;
                }
                if !state.started {
                    state.started = true;
                    if let Err(failure) = state.start().await {
                        state.fail(failure);
                    }
                    continue;
                }
                let result = {
                    let response = state
                        .response
                        .as_mut()
                        .expect("started response or finished state");
                    tokio::select! { biased;
                        _ = state.context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "stream")),
                        _ = tokio::time::sleep_until(state.context.deadline) => Err(error(ErrorCode::DeadlineExceeded, "stream")),
                        chunk = response.chunk() => chunk.map_err(transport_error),
                    }
                };
                match result {
                    Ok(Some(bytes)) => match state.framing.push(&bytes) {
                        Ok(events) => {
                            for event in events {
                                match state.decoder.event(event) {
                                    Ok(events) => state.queue.extend(events.into_iter().map(Ok)),
                                    Err(error) => {
                                        state.fail(error);
                                        break;
                                    }
                                }
                            }
                        }
                        Err(error) => state.fail(error),
                    },
                    Ok(None) => match state.framing.finish().and_then(|_| state.decoder.finish()) {
                        Ok(event) => {
                            state.queue.push_back(Ok(event));
                            state.finished = true;
                            state.response = None;
                        }
                        Err(error) => state.fail(error),
                    },
                    Err(error) => state.fail(error),
                }
            }
        }))
    }
}
struct State<'a> {
    connection: &'a AzureOpenAiConnection,
    request: &'a ModelRequest,
    context: &'a ModelCallContext,
    response: Option<Response>,
    decoder: ResponsesDecoder<'a>,
    framing: SseDecoder,
    queue: VecDeque<Result<ModelEvent, ContractError>>,
    started: bool,
    finished: bool,
}
impl State<'_> {
    async fn start(&mut self) -> Result<(), ContractError> {
        self.connection
            .validate(&self.request.route, &self.context.scope)?;
        if self.context.attempt_id != self.request.request_id {
            return Err(error(ErrorCode::RequestConflict, "attempt"));
        }
        self.request.validate()?;
        let mut value = encode_request(self.request)?;
        // The wire selects a deployment; the core route retains its underlying
        // model/release and the original route-bound opaque continuation.
        value["model"] = serde_json::json!(self.connection.0.options.deployment);
        let body =
            serde_json::to_vec(&value).map_err(|_| error(ErrorCode::InvalidJson, "request"))?;
        if body.len() > self.request.limits.max_input_bytes {
            return Err(error(ErrorCode::ModelCapabilityUnsupported, "request_size"));
        }
        let url = self
            .connection
            .0
            .base
            .join("responses")
            .map_err(|_| error(ErrorCode::InvalidConfiguration, "responses_url"))?;
        let headers = match auth::authorize(
            self.connection.0.credentials.as_ref(),
            AzureCredentialContext {
                scope: &self.context.scope,
                audience: AzureAudience::Inference,
                cancellation: &self.context.cancellation,
                deadline: self.context.deadline,
            },
        )
        .await
        {
            Ok(headers) => headers,
            Err(failure) if failure.code == ErrorCode::AccessDenied => {
                self.queue.push_back(Ok(ModelEvent::ResponseError {
                    kind: ModelFailureKind::Authentication,
                    metadata: self.decoder.metadata.clone(),
                }));
                self.finished = true;
                return Ok(());
            }
            Err(failure) => return Err(failure),
        };
        let operation = self
            .connection
            .0
            .client
            .post(url)
            .headers(headers)
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
            .header("accept-encoding", "identity")
            .body(body)
            .send();
        let mut response = tokio::select! { biased;
            _ = self.context.cancellation.cancelled() => return Err(error(ErrorCode::Cancelled, "request")),
            _ = tokio::time::sleep_until(self.context.deadline) => return Err(error(ErrorCode::DeadlineExceeded, "request")),
            result = operation => result.map_err(transport_error)?,
        };
        self.decoder.metadata.provider_request_id = response
            .headers()
            .get("apim-request-id")
            .or_else(|| response.headers().get("x-request-id"))
            .map(|value| {
                value
                    .to_str()
                    .map_err(|_| error(ErrorCode::InvalidContract, "request_id"))
                    .and_then(Id::new)
            })
            .transpose()?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let mut kind = match status {
                401 | 403 => ModelFailureKind::Authentication,
                404 => ModelFailureKind::Unavailable,
                408 | 504 => ModelFailureKind::Timeout,
                429 => ModelFailureKind::RateLimited,
                500..=599 => ModelFailureKind::Transport,
                _ => ModelFailureKind::Unsupported,
            };
            if status == 400 {
                let mut bytes = vec![];
                loop {
                    let chunk = tokio::select! { biased;
                        _ = self.context.cancellation.cancelled() => return Err(error(ErrorCode::Cancelled, "error_body")),
                        _ = tokio::time::sleep_until(self.context.deadline) => return Err(error(ErrorCode::DeadlineExceeded, "error_body")),
                        chunk = response.chunk() => chunk.map_err(transport_error)?,
                    };
                    let Some(chunk) = chunk else {
                        break;
                    };
                    if bytes.len().saturating_add(chunk.len())
                        > 65_536.min(self.connection.0.options.max_transport_bytes)
                    {
                        break;
                    }
                    bytes.extend_from_slice(&chunk);
                }
                if let Ok(value) = std::str::from_utf8(&bytes)
                    .map_err(|_| ())
                    .and_then(|text| parse_json(text).map_err(|_| ()))
                {
                    if value.pointer("/error/code").and_then(Value::as_str)
                        == Some("context_length_exceeded")
                    {
                        kind = ModelFailureKind::ContextOverflow;
                    }
                }
            }
            self.queue.push_back(Ok(ModelEvent::ResponseError {
                kind,
                metadata: self.decoder.metadata.clone(),
            }));
            self.finished = true;
            return Ok(());
        }
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim);
        if !content_type.is_some_and(|value| value.eq_ignore_ascii_case("text/event-stream")) {
            return Err(error(ErrorCode::InvalidContract, "content_type"));
        }
        if response
            .content_length()
            .is_some_and(|bytes| bytes > self.connection.0.options.max_transport_bytes as u64)
        {
            return Err(error(ErrorCode::InvalidContract, "response_size"));
        }
        self.response = Some(response);
        Ok(())
    }
    fn fail(&mut self, failure: ContractError) {
        self.finished = true;
        self.response = None;
        if matches!(
            failure.code,
            ErrorCode::Cancelled | ErrorCode::AccessDenied | ErrorCode::RequestConflict
        ) {
            self.queue.push_back(Err(failure));
            return;
        }
        let kind = match failure.code {
            ErrorCode::DeadlineExceeded => ModelFailureKind::Timeout,
            ErrorCode::ModelUnavailable => ModelFailureKind::Transport,
            ErrorCode::ModelOptionUnsupported
            | ErrorCode::ModelCapabilityUnsupported
            | ErrorCode::CapabilityUnsupported
            | ErrorCode::ModelBindingInvalid
            | ErrorCode::InvalidConfiguration => ModelFailureKind::Unsupported,
            _ => ModelFailureKind::Protocol,
        };
        self.queue.push_back(Ok(ModelEvent::ResponseError {
            kind,
            metadata: self.decoder.metadata.clone(),
        }));
    }
}
fn transport_error(error_value: reqwest::Error) -> ContractError {
    error(
        if error_value.is_timeout() {
            ErrorCode::DeadlineExceeded
        } else {
            ErrorCode::ModelUnavailable
        },
        "transport",
    )
}
```

## `crates/wickle-model-azure-openai/tests/responses.rs`

```rust
//! Azure inference/authentication and deployment metadata transport contracts.
mod support;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::*;
use wickle::*;
use wickle_model_azure_openai::*;

struct Tokens(AtomicUsize);
impl AzureCredentialProvider for Tokens {
    fn credential<'a>(
        &'a self,
        context: &'a AzureCredentialContext<'a>,
    ) -> PortFuture<'a, AzureCredential> {
        Box::pin(async move {
            assert_eq!(context.scope, &scope());
            assert_eq!(context.audience, AzureAudience::Inference);
            let number = self.0.fetch_add(1, Ordering::SeqCst);
            Ok(AzureCredential::EntraToken(format!("token-{number}")))
        })
    }
}

#[tokio::test]
async fn deployment_mapping_preserves_model_identity_and_refreshes_entra_per_request() {
    for entra in [false, true] {
        let mut reply = Reply::sse(&events("base-model", "42"));
        reply.headers = vec![("apim-request-id", "azure-request".into())];
        let server = Server::new(vec![reply, Reply::sse(&events("base-model", "43"))]).await;
        let tokens = Arc::new(Tokens(AtomicUsize::new(0)));
        let credentials: Arc<dyn AzureCredentialProvider> = if entra {
            tokens.clone()
        } else {
            Arc::new(AzureCredential::ApiKey("fixture-key".into()))
        };
        let connection = AzureOpenAiConnection::new(
            scope(),
            reference("account"),
            credentials,
            options(&server),
        )
        .unwrap();
        let model = AzureOpenAiModel::new(connection.clone());
        let mut request = request(&connection, "base-model");
        let original = request.route.clone();
        for answer in ["42", "43"] {
            let response =
                collect_model_response(&request, model.generate(&request, &context(&request)))
                    .await
                    .unwrap();
            assert_eq!(response.text, answer);
            assert_eq!(response.metadata.reported_model_id, Some(id("base-model")));
            assert!(response.metadata.reported_model_version.is_none());
            assert_eq!(response.metadata.usage.unwrap().output_tokens, Some(7));
            if answer == "42" {
                assert_eq!(
                    response.metadata.provider_request_id,
                    Some(id("azure-request"))
                );
            }
            request.request_id = id("second-attempt");
        }
        assert_eq!(request.route, original);
        assert_eq!(tokens.0.load(Ordering::SeqCst), if entra { 2 } else { 0 });
        let calls = server.requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        for (index, call) in calls.iter().enumerate() {
            assert_eq!(call.method, "POST");
            assert_eq!(call.path, "/openai/v1/responses");
            assert_eq!(call.body["model"], "finance-deployment");
            assert_eq!(call.body["reasoning"]["effort"], "medium");
            assert_eq!(call.body["store"], false);
            let headers = call.headers.to_ascii_lowercase();
            if entra {
                assert!(headers.contains(&format!("authorization: bearer token-{index}")));
                assert!(!headers.contains("api-key:"));
            } else {
                assert!(headers.contains("api-key: fixture-key"));
                assert!(!headers.contains("authorization:"));
            }
            assert!(!call.body.to_string().contains("hidden-workspace"));
            assert!(!call.body.to_string().contains("fixture-key"));
        }
    }
}

#[tokio::test]
async fn wrong_scope_route_api_and_credentials_never_make_an_http_request() {
    for case in [
        "scope",
        "target",
        "provider",
        "api",
        "options",
        "credential",
    ] {
        let server = Server::new(vec![]).await;
        let tokens = Arc::new(Tokens(AtomicUsize::new(0)));
        let credentials: Arc<dyn AzureCredentialProvider> = if case == "credential" {
            Arc::new(AzureCredential::ApiKey("bad\nkey".into()))
        } else {
            tokens.clone()
        };
        let connection = AzureOpenAiConnection::new(
            scope(),
            reference("account"),
            credentials,
            options(&server),
        )
        .unwrap();
        let model = AzureOpenAiModel::new(connection.clone());
        let mut request = request(&connection, "base-model");
        let mut context = context(&request);
        match case {
            "scope" => context.scope.workspace_id = id("another-workspace"),
            "target" => {
                request
                    .route
                    .target
                    .insert("deployment".into(), json!("another-deployment"));
            }
            "provider" => request.route.provider = id("openai"),
            "api" => request.route.api_contract.version = id("2025-04-01-preview"),
            "options" => {
                request.options.insert("model".into(), json!("injected"));
            }
            _ => {}
        }
        let failure = collect_model_response(&request, model.generate(&request, &context))
            .await
            .unwrap_err();
        if case == "credential" {
            assert_eq!(failure.kind, ModelFailureKind::Authentication);
        }
        assert!(server.requests.lock().unwrap().is_empty());
        assert_eq!(tokens.0.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn azure_tools_and_json_output_keep_original_route_bound_replay() {
    let call = json!({"id":"fc","type":"function_call","call_id":"call-1","name":"lookup","arguments":"{\"query\":\"figures\"}","status":"completed"});
    let first = vec![
        json!({"type":"response.created","response":{"id":"resp_1","model":"base-model","status":"in_progress"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"fc","type":"function_call","call_id":"call-1","name":"lookup","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":0,"item_id":"fc","delta":"{\"query\":\"figures\"}"}),
        json!({"type":"response.completed","response":{"id":"resp_1","model":"base-model","status":"completed","output":[call]}}),
    ];
    let server = Server::new(vec![
        Reply::sse(&first),
        Reply::sse(&events("base-model", "{\"answer\":42}")),
    ])
    .await;
    let connection = connection(&server);
    let model = AzureOpenAiModel::new(connection.clone());
    let mut request = request(&connection, "base-model");
    request.tools = vec![ModelTool {
        name: id("lookup"),
        description: "Read figures".into(),
        model_input_schema: json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}),
    }];
    let first = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(first.finish, ModelFinish::ToolCalls);
    assert_eq!(first.tool_calls[0].model_inputs["query"], "figures");
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![
            ModelContent::ToolCall {
                provider_call_id: id("call-1"),
                name: id("lookup"),
                arguments: JsonObject::from([("query".into(), json!("figures"))]),
            },
            ModelContent::Opaque {
                continuation: first.continuation[0].clone(),
            },
        ],
    });
    request.messages.push(ModelMessage {
        role: ModelRole::Tool,
        content: vec![ModelContent::ToolResult {
            provider_call_id: id("call-1"),
            content: json!({"answer":42}),
        }],
    });
    request.output = ModelOutput::JsonSchema {
        schema: json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}),
    };
    let response = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(parse_json(&response.text).unwrap(), json!({"answer":42}));
    let calls = server.requests.lock().unwrap();
    let input = calls[1].body["input"].as_array().unwrap();
    assert_eq!(
        input
            .iter()
            .filter(|value| value["type"] == "function_call")
            .count(),
        1
    );
    assert_eq!(calls[1].body["model"], "finance-deployment");
    assert_eq!(calls[1].body["text"]["format"]["strict"], true);
    assert_eq!(calls[0].body["tools"][0]["strict"], false);
}

fn inspection_context() -> ModelInspectionContext {
    ModelInspectionContext {
        scope: scope(),
        principal_ref: id("user"),
        capability_grant_ref: id("grant"),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(3),
    }
}
fn deployment(version: &str) -> Value {
    json!({"id":format!("{ACCOUNT}/deployments/finance-deployment"),"name":"finance-deployment","type":"Microsoft.CognitiveServices/accounts/deployments","properties":{"model":{"format":"OpenAI","name":"base-model","version":version},"provisioningState":"Succeeded","versionUpgradeOption":"OnceNewDefaultVersionAvailable"}})
}
fn inspector(
    connection: AzureOpenAiConnection,
    server: &Server,
    credentials: AzureCredential,
) -> AzureOpenAiInspector {
    AzureOpenAiInspector::new(
        connection,
        Arc::new(credentials),
        AzureInspectionOptions {
            endpoint: origin(server),
            ..Default::default()
        },
    )
    .unwrap()
}

#[tokio::test]
async fn arm_inspection_detects_model_upgrade_without_claiming_deployment_immutability() {
    let inference = Server::new(vec![]).await;
    let management = Server::new(vec![
        Reply::json(200, deployment("release")),
        Reply::json(200, deployment("next-release")),
    ])
    .await;
    let connection = connection(&inference);
    let mut request = request(&connection, "base-model");
    let inspector = inspector(
        connection,
        &management,
        AzureCredential::EntraToken("management-token".into()),
    );
    let first = inspector
        .inspect(&request.route, &inspection_context())
        .await
        .unwrap();
    assert_eq!(first.model_version, Some(id("release")));
    assert_eq!(first.version_semantics, VersionSemantics::MutableDeployment);
    first
        .validate(&request.route, VersionPolicy::AllowMutable)
        .unwrap();
    assert_eq!(
        first
            .validate(&request.route, VersionPolicy::RequirePinned)
            .unwrap_err()
            .code,
        ErrorCode::ModelVersionUnpinned
    );
    request.route.deployment_revision = first.deployment_revision.clone();
    let changed = inspector
        .inspect(&request.route, &inspection_context())
        .await
        .unwrap();
    assert_eq!(changed.model_version, Some(id("next-release")));
    assert_ne!(changed.deployment_revision, first.deployment_revision);
    assert_eq!(
        changed
            .validate(&request.route, VersionPolicy::AllowMutable)
            .unwrap_err()
            .code,
        ErrorCode::ModelVersionDrift
    );
    assert!(inference.requests.lock().unwrap().is_empty());
    for call in management.requests.lock().unwrap().iter() {
        assert_eq!(call.method, "GET");
        assert_eq!(
            call.path,
            format!("{ACCOUNT}/deployments/finance-deployment?api-version=2025-06-01")
        );
        assert!(
            call.headers
                .to_ascii_lowercase()
                .contains("authorization: bearer management-token")
        );
        assert!(!call.headers.contains("fixture-key"));
    }
}

#[tokio::test]
async fn metadata_unavailability_mismatch_and_wrong_audience_do_not_invent_a_version() {
    for case in [
        "missing",
        "wrong-target",
        "version-missing",
        "provisioning",
        "api-key",
    ] {
        let mut body = deployment("release");
        if case == "wrong-target" {
            body["id"] = json!(format!("{ACCOUNT}/deployments/other"));
        }
        if case == "version-missing" {
            body["properties"]["model"]
                .as_object_mut()
                .unwrap()
                .remove("version");
        }
        if case == "provisioning" {
            body["properties"]["provisioningState"] = json!("Updating");
        }
        let server = Server::new(vec![Reply::json(
            if case == "missing" { 404 } else { 200 },
            body,
        )])
        .await;
        let connection = connection(&server);
        let request = request(&connection, "base-model");
        let credential = if case == "api-key" {
            AzureCredential::ApiKey("data-plane-key".into())
        } else {
            AzureCredential::EntraToken("management-token".into())
        };
        let result = inspector(connection, &server, credential)
            .inspect(&request.route, &inspection_context())
            .await;
        match case {
            "missing" => {
                let value = result.unwrap();
                assert_eq!(value.availability, ModelRouteAvailability::Unavailable);
                assert!(value.model_version.is_none());
            }
            "provisioning" => assert_eq!(
                result.unwrap().availability,
                ModelRouteAvailability::Unknown
            ),
            _ => assert!(result.is_err(), "{case}"),
        }
        if case == "api-key" {
            assert!(server.requests.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn azure_errors_and_redirects_do_not_retry_or_forward_credentials() {
    let redirected = Server::new(vec![]).await;
    for (status, expected) in [
        (401, ModelFailureKind::Authentication),
        (429, ModelFailureKind::RateLimited),
        (503, ModelFailureKind::Transport),
        (307, ModelFailureKind::Unsupported),
    ] {
        let mut reply = Reply::json(status, json!({"error":{"message":"private diagnostics"}}));
        if status == 307 {
            reply
                .headers
                .push(("location", format!("{}responses", redirected.base)));
        }
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let request = request(&connection, "base-model");
        let model = AzureOpenAiModel::new(connection);
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, expected);
        assert!(!format!("{failure:?}").contains("private diagnostics"));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
    assert!(redirected.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancelled_or_truncated_streams_never_become_a_completed_response() {
    for cancel in [false, true] {
        let mut data = events("base-model", "partial");
        data.truncate(3);
        let mut reply = Reply::sse(&data);
        reply.stall = cancel;
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let request = request(&connection, "base-model");
        let model = AzureOpenAiModel::new(connection);
        let context = context(&request);
        let mut stream = model.generate(&request, &context);
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ModelEvent::TextDelta { .. }
        ));
        server.entered.notified().await;
        if cancel {
            context.cancellation.cancel();
        }
        let terminal = stream.next().await.unwrap();
        if cancel {
            assert_eq!(terminal.unwrap_err().code, ErrorCode::Cancelled);
        } else {
            assert!(matches!(
                terminal.unwrap(),
                ModelEvent::ResponseError {
                    kind: ModelFailureKind::Protocol,
                    ..
                }
            ));
        }
        assert!(stream.next().await.is_none());
        tokio::time::timeout(Duration::from_secs(1), server.closed.notified())
            .await
            .unwrap();
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

struct PendingCredential(tokio::sync::Notify);
impl AzureCredentialProvider for PendingCredential {
    fn credential<'a>(
        &'a self,
        _: &'a AzureCredentialContext<'a>,
    ) -> PortFuture<'a, AzureCredential> {
        Box::pin(async move {
            self.0.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_and_deadline_bound_host_token_refresh_before_http() {
    for cancel in [false, true] {
        let server = Server::new(vec![]).await;
        let credentials = Arc::new(PendingCredential(tokio::sync::Notify::new()));
        let connection = AzureOpenAiConnection::new(
            scope(),
            reference("account"),
            credentials.clone(),
            options(&server),
        )
        .unwrap();
        let model = AzureOpenAiModel::new(connection.clone());
        let request = request(&connection, "base-model");
        let mut context = context(&request);
        context.deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let mut stream = model.generate(&request, &context);
        let mut next = Box::pin(stream.next());
        tokio::select! {
            _ = credentials.0.notified() => {},
            value = &mut next => panic!("refresh completed unexpectedly: {value:?}"),
        }
        if cancel {
            context.cancellation.cancel();
        } else {
            tokio::time::advance(Duration::from_secs(2)).await;
        }
        let value = next.await.unwrap();
        if cancel {
            assert_eq!(value.unwrap_err().code, ErrorCode::Cancelled);
        } else {
            assert!(matches!(
                value.unwrap(),
                ModelEvent::ResponseError {
                    kind: ModelFailureKind::Timeout,
                    ..
                }
            ));
        }
        assert!(stream.next().await.is_none());
        assert!(server.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn arm_scope_and_target_rejection_precedes_credential_lookup() {
    for wrong_scope in [true, false] {
        let server = Server::new(vec![]).await;
        let connection = connection(&server);
        let mut request = request(&connection, "base-model");
        let credentials = Arc::new(Tokens(AtomicUsize::new(0)));
        let inspector = AzureOpenAiInspector::new(
            connection,
            credentials.clone(),
            AzureInspectionOptions {
                endpoint: origin(&server),
                ..Default::default()
            },
        )
        .unwrap();
        let mut context = inspection_context();
        if wrong_scope {
            context.scope.workspace_id = id("different-workspace");
        } else {
            request
                .route
                .target
                .insert("deployment".into(), json!("different-deployment"));
        }
        assert!(inspector.inspect(&request.route, &context).await.is_err());
        assert_eq!(credentials.0.load(Ordering::SeqCst), 0);
        assert!(server.requests.lock().unwrap().is_empty());
    }
}
```

## `crates/wickle-model-azure-openai/tests/support/mod.rs`

```rust
use serde_json::{Value, json};
use std::sync::Arc;
use wickle::*;
use wickle_model_azure_openai::*;

pub fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
pub fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
pub fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("hidden-workspace"),
        user_id: None,
    }
}
pub const ACCOUNT: &str = "/subscriptions/sub/resourceGroups/group/providers/Microsoft.CognitiveServices/accounts/account";
pub fn origin(server: &Server) -> String {
    server.base.trim_end_matches("v1/").into()
}
pub fn options(server: &Server) -> AzureOpenAiOptions {
    let mut options = AzureOpenAiOptions::new(origin(server), "finance-deployment");
    options.resource_id = Some(ACCOUNT.into());
    options
}
pub fn connection(server: &Server) -> AzureOpenAiConnection {
    AzureOpenAiConnection::new(
        scope(),
        reference("account"),
        Arc::new(AzureCredential::ApiKey("fixture-key".into())),
        options(server),
    )
    .unwrap()
}
pub fn request(connection: &AzureOpenAiConnection, model: &str) -> ModelRequest {
    let binding = connection.binding();
    ModelRequest {
        request_id: id("attempt"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference("primary"),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id(model),
            model_id: id(model),
            model_version: id("release"),
            version_semantics: VersionSemantics::MutableDeployment,
            provider: binding.provider,
            target: connection.target().clone(),
            deployment_revision: None,
            api_contract: AzureOpenAiConnection::api_contract(),
            adapter: binding.adapter,
            capability_revision: id("capabilities"),
            connection_ref: binding.connection_ref,
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find the requested figures".into(),
            }],
        }],
        tools: vec![],
        output: ModelOutput::Text {},
        max_output_tokens: 128.try_into().unwrap(),
        options: JsonObject::from([("reasoning_effort".into(), json!("medium"))]),
        limits: ModelResponseLimits {
            max_input_bytes: 32_768,
            max_response_bytes: 32_768,
            max_delta_bytes: 128,
            max_events: 256,
            max_tool_calls: 4,
        },
    }
}
pub fn context(request: &ModelRequest) -> ModelCallContext {
    ModelCallContext {
        attempt_id: request.request_id.clone(),
        run_id: id("run"),
        scope: scope(),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(5),
    }
}
pub fn text_item(text: &str) -> Value {
    json!({"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":text,"annotations":[]}]})
}
pub fn events(model: &str, text: &str) -> Vec<Value> {
    let item = text_item(text);
    vec![
        json!({"type":"response.created","response":{"id":"resp_1","model":model,"status":"in_progress","usage":null}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","status":"in_progress","content":[]}}),
        json!({"type":"response.output_text.delta","output_index":0,"item_id":"msg_1","content_index":0,"delta":text}),
        json!({"type":"response.output_text.done","output_index":0,"item_id":"msg_1","content_index":0,"text":text}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        json!({"type":"response.completed","response":{"id":"resp_1","model":model,"status":"completed","output":[item],"usage":{"input_tokens":11,"output_tokens":7}}}),
    ]
}

#[path = "../../../../tests/support/model_http.rs"]
mod http;
pub use http::*;
```

## `crates/wickle-model-openai/src/lib.rs`

```rust
//! OpenAI Responses over bounded HTTP/SSE, with Host-supplied credentials.
//!
//! A model stream represents exactly one POST. SDK retries, redirects, native
//! provider tools, conversation storage, and automatic truncation are disabled.

#![forbid(unsafe_code)]

mod connection;
mod inspection;
mod model;

pub use connection::{OpenAiConnection, OpenAiOptions};
pub use inspection::{OpenAiInspector, OpenAiSnapshot};
pub use model::OpenAiModel;

use wickle::{ContractError, ErrorCode};

fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("openai.{location}"))
}
```

## `crates/wickle-model-openai/src/model.rs`

```rust
use crate::{OpenAiConnection, error};
use futures_util::stream;
use reqwest::Response;
use serde_json::Value;
use std::collections::VecDeque;
use wickle::*;
use wickle_model_responses::{ResponsesDecoder, SseDecoder, encode_request};

/// One OpenAI Responses POST per invocation, streamed into Wickle model events.
/// The adapter does not retry, run Tool handlers, or load environment variables.
#[derive(Debug, Clone)]
pub struct OpenAiModel {
    connection: OpenAiConnection,
}
impl OpenAiModel {
    /// Bind an already configured connection without making a network request.
    pub fn new(connection: OpenAiConnection) -> Self {
        Self { connection }
    }
}
impl ModelPort for OpenAiModel {
    fn binding(&self) -> ModelPortBinding {
        self.connection.binding()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let state = State {
            connection: &self.connection,
            request,
            context,
            response: None,
            decoder: ResponsesDecoder::new(request, None),
            framing: SseDecoder::new(
                self.connection.0.options.max_transport_bytes,
                self.connection.0.options.max_event_bytes,
                self.connection.0.options.max_protocol_events,
            ),
            queue: VecDeque::new(),
            started: false,
            finished: false,
        };
        Box::pin(stream::unfold(state, |mut state| async move {
            loop {
                if !state.finished && state.context.cancellation.is_cancelled() {
                    state.queue.clear();
                    state.fail(error(ErrorCode::Cancelled, "stream"));
                }
                if !state.finished && tokio::time::Instant::now() >= state.context.deadline {
                    state.queue.clear();
                    state.fail(error(ErrorCode::DeadlineExceeded, "stream"));
                }
                if let Some(event) = state.queue.pop_front() {
                    return Some((event, state));
                }
                if state.finished {
                    return None;
                }
                if !state.started {
                    state.started = true;
                    if let Err(failure) = state.start().await {
                        state.fail(failure);
                    }
                    continue;
                }
                let result = {
                    let response = state
                        .response
                        .as_mut()
                        .expect("started response or finished state");
                    tokio::select! { biased;
                        _ = state.context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "stream")),
                        _ = tokio::time::sleep_until(state.context.deadline) => Err(error(ErrorCode::DeadlineExceeded, "stream")),
                        chunk = response.chunk() => chunk.map_err(transport_error),
                    }
                };
                match result {
                    Ok(Some(bytes)) => match state.framing.push(&bytes) {
                        Ok(events) => {
                            for event in events {
                                match state.decoder.event(event) {
                                    Ok(events) => state.queue.extend(events.into_iter().map(Ok)),
                                    Err(error) => {
                                        state.fail(error);
                                        break;
                                    }
                                }
                            }
                        }
                        Err(error) => state.fail(error),
                    },
                    Ok(None) => match state.framing.finish().and_then(|_| state.decoder.finish()) {
                        Ok(event) => {
                            state.queue.push_back(Ok(event));
                            state.finished = true;
                            state.response = None;
                        }
                        Err(error) => state.fail(error),
                    },
                    Err(error) => state.fail(error),
                }
            }
        }))
    }
}
struct State<'a> {
    connection: &'a OpenAiConnection,
    request: &'a ModelRequest,
    context: &'a ModelCallContext,
    response: Option<Response>,
    decoder: ResponsesDecoder<'a>,
    framing: SseDecoder,
    queue: VecDeque<Result<ModelEvent, ContractError>>,
    started: bool,
    finished: bool,
}
impl State<'_> {
    async fn start(&mut self) -> Result<(), ContractError> {
        self.connection
            .validate(&self.request.route, &self.context.scope)?;
        if self.context.attempt_id != self.request.request_id {
            return Err(error(ErrorCode::RequestConflict, "attempt"));
        }
        self.request.validate()?;
        let value = encode_request(self.request)?;
        let body =
            serde_json::to_vec(&value).map_err(|_| error(ErrorCode::InvalidJson, "request"))?;
        if body.len() > self.request.limits.max_input_bytes {
            return Err(error(ErrorCode::ModelCapabilityUnsupported, "request_size"));
        }
        let url = self
            .connection
            .0
            .base
            .join("responses")
            .map_err(|_| error(ErrorCode::InvalidConfiguration, "responses_url"))?;
        let operation = self
            .connection
            .0
            .client
            .post(url)
            .header("accept", "text/event-stream")
            .header("accept-encoding", "identity")
            .body(body)
            .send();
        let mut response = tokio::select! { biased;
            _ = self.context.cancellation.cancelled() => return Err(error(ErrorCode::Cancelled, "request")),
            _ = tokio::time::sleep_until(self.context.deadline) => return Err(error(ErrorCode::DeadlineExceeded, "request")),
            result = operation => result.map_err(transport_error)?,
        };
        self.decoder.metadata.provider_request_id = response
            .headers()
            .get("x-request-id")
            .map(|value| {
                value
                    .to_str()
                    .map_err(|_| error(ErrorCode::InvalidContract, "request_id"))
                    .and_then(Id::new)
            })
            .transpose()?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let mut kind = match status {
                401 | 403 => ModelFailureKind::Authentication,
                404 => ModelFailureKind::Unavailable,
                408 | 504 => ModelFailureKind::Timeout,
                429 => ModelFailureKind::RateLimited,
                500..=599 => ModelFailureKind::Transport,
                _ => ModelFailureKind::Unsupported,
            };
            if status == 400 {
                let mut bytes = vec![];
                loop {
                    let chunk = tokio::select! { biased;
                        _ = self.context.cancellation.cancelled() => return Err(error(ErrorCode::Cancelled, "error_body")),
                        _ = tokio::time::sleep_until(self.context.deadline) => return Err(error(ErrorCode::DeadlineExceeded, "error_body")),
                        chunk = response.chunk() => chunk.map_err(transport_error)?,
                    };
                    let Some(chunk) = chunk else {
                        break;
                    };
                    if bytes.len().saturating_add(chunk.len())
                        > 65_536.min(self.connection.0.options.max_transport_bytes)
                    {
                        break;
                    }
                    bytes.extend_from_slice(&chunk);
                }
                if let Ok(value) = std::str::from_utf8(&bytes)
                    .map_err(|_| ())
                    .and_then(|text| parse_json(text).map_err(|_| ()))
                {
                    if value.pointer("/error/code").and_then(Value::as_str)
                        == Some("context_length_exceeded")
                    {
                        kind = ModelFailureKind::ContextOverflow;
                    }
                }
            }
            self.queue.push_back(Ok(ModelEvent::ResponseError {
                kind,
                metadata: self.decoder.metadata.clone(),
            }));
            self.finished = true;
            return Ok(());
        }
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim);
        if !content_type.is_some_and(|value| value.eq_ignore_ascii_case("text/event-stream")) {
            return Err(error(ErrorCode::InvalidContract, "content_type"));
        }
        if response
            .content_length()
            .is_some_and(|bytes| bytes > self.connection.0.options.max_transport_bytes as u64)
        {
            return Err(error(ErrorCode::InvalidContract, "response_size"));
        }
        self.response = Some(response);
        Ok(())
    }
    fn fail(&mut self, failure: ContractError) {
        self.finished = true;
        self.response = None;
        if matches!(
            failure.code,
            ErrorCode::Cancelled | ErrorCode::AccessDenied | ErrorCode::RequestConflict
        ) {
            self.queue.push_back(Err(failure));
            return;
        }
        let kind = match failure.code {
            ErrorCode::DeadlineExceeded => ModelFailureKind::Timeout,
            ErrorCode::ModelUnavailable => ModelFailureKind::Transport,
            ErrorCode::ModelOptionUnsupported
            | ErrorCode::ModelCapabilityUnsupported
            | ErrorCode::CapabilityUnsupported
            | ErrorCode::ModelBindingInvalid
            | ErrorCode::InvalidConfiguration => ModelFailureKind::Unsupported,
            _ => ModelFailureKind::Protocol,
        };
        self.queue.push_back(Ok(ModelEvent::ResponseError {
            kind,
            metadata: self.decoder.metadata.clone(),
        }));
    }
}
fn transport_error(error_value: reqwest::Error) -> ContractError {
    error(
        if error_value.is_timeout() {
            ErrorCode::DeadlineExceeded
        } else {
            ErrorCode::ModelUnavailable
        },
        "transport",
    )
}
```

## `crates/wickle-model-openai/tests/support/mod.rs`

```rust
use serde_json::{Value, json};
use wickle::*;
use wickle_model_openai::*;

pub fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
pub fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
pub fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("hidden-workspace"),
        user_id: None,
    }
}
pub fn connection(server: &Server) -> OpenAiConnection {
    OpenAiConnection::new(
        scope(),
        reference("account"),
        "fixture-key-not-a-secret",
        OpenAiOptions {
            base_url: server.base.clone(),
            ..Default::default()
        },
    )
    .unwrap()
}
pub fn request(connection: &OpenAiConnection, model: &str) -> ModelRequest {
    let binding = connection.binding();
    ModelRequest {
        request_id: id("attempt"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference("primary"),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id(model),
            model_id: id(model),
            model_version: id("release"),
            version_semantics: VersionSemantics::Pinned,
            provider: binding.provider,
            target: connection.target().clone(),
            deployment_revision: None,
            api_contract: OpenAiConnection::api_contract(),
            adapter: binding.adapter,
            capability_revision: id("capabilities"),
            connection_ref: binding.connection_ref,
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find the requested figures".into(),
            }],
        }],
        tools: vec![],
        output: ModelOutput::Text {},
        max_output_tokens: 128.try_into().unwrap(),
        options: JsonObject::from([("reasoning_effort".into(), json!("medium"))]),
        limits: ModelResponseLimits {
            max_input_bytes: 32_768,
            max_response_bytes: 32_768,
            max_delta_bytes: 128,
            max_events: 256,
            max_tool_calls: 4,
        },
    }
}
pub fn context(request: &ModelRequest) -> ModelCallContext {
    ModelCallContext {
        attempt_id: request.request_id.clone(),
        run_id: id("run"),
        scope: scope(),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(5),
    }
}
pub fn text_item(text: &str) -> Value {
    json!({"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":text,"annotations":[]}]})
}
pub fn events(model: &str, text: &str) -> Vec<Value> {
    let item = text_item(text);
    vec![
        json!({"type":"response.created","response":{"id":"resp_1","model":model,"status":"in_progress","usage":null}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","status":"in_progress","content":[]}}),
        json!({"type":"response.output_text.delta","output_index":0,"item_id":"msg_1","content_index":0,"delta":text}),
        json!({"type":"response.output_text.done","output_index":0,"item_id":"msg_1","content_index":0,"text":text}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        json!({"type":"response.completed","response":{"id":"resp_1","model":model,"status":"completed","output":[item],"usage":{"input_tokens":11,"output_tokens":7}}}),
    ]
}

#[path = "../../../../tests/support/model_http.rs"]
mod http;
pub use http::*;
```

## `crates/wickle-model-responses/src/codec.rs`

```rust
use serde_json::{Value, json};
use wickle::*;

pub(crate) const REPLAY_KIND: &str = "wickle.openai.responses.v1";
fn failure(code: ErrorCode) -> ContractError {
    crate::error(code, "codec")
}

/// Encode a Responses request, preserving exact route-bound replay and model schemas.
pub fn encode_request(request: &ModelRequest) -> Result<Value, ContractError> {
    request.validate()?;
    if request.options.keys().any(|key| {
        !["reasoning_effort", "temperature", "top_p", "verbosity"].contains(&key.as_str())
    }) {
        return Err(failure(ErrorCode::ModelOptionUnsupported));
    }
    let mut input = Vec::new();
    for message in &request.messages {
        let opaque: Vec<_> = message
            .content
            .iter()
            .filter_map(|content| match content {
                ModelContent::Opaque { continuation } => Some(continuation),
                _ => None,
            })
            .collect();
        if !opaque.is_empty() {
            if opaque.len() != 1 || message.role != ModelRole::Assistant {
                return Err(failure(ErrorCode::ModelContextIncompatible));
            }
            let replay = opaque[0].data();
            let object = replay
                .as_object()
                .ok_or_else(|| failure(ErrorCode::ModelContextIncompatible))?;
            if object.len() != 2 || object.get("kind") != Some(&json!(REPLAY_KIND)) {
                return Err(failure(ErrorCode::ModelContextIncompatible));
            }
            let items = replay
                .get("items")
                .and_then(Value::as_array)
                .ok_or_else(|| failure(ErrorCode::ModelContextIncompatible))?;
            let decoded = inspect_output_items(items)?;
            let mut text = String::new();
            let mut calls = Vec::new();
            for content in &message.content {
                match content {
                    ModelContent::Text { text: value } => text.push_str(value),
                    ModelContent::ToolCall {
                        provider_call_id,
                        name,
                        arguments,
                    } => calls.push((provider_call_id.as_str(), name.as_str(), arguments)),
                    ModelContent::Opaque { .. } => {}
                    _ => return Err(failure(ErrorCode::ModelContextIncompatible)),
                }
            }
            if text != decoded.text || calls.len() != decoded.calls.len() {
                return Err(failure(ErrorCode::ModelContextIncompatible));
            }
            for ((call_id, name, arguments), original) in calls.iter().zip(&decoded.calls) {
                if *call_id != original.call_id
                    || *name != original.name
                    || serde_json::to_value(arguments)
                        .map_err(|_| failure(ErrorCode::InvalidContract))?
                        != parse_json(&original.arguments)?
                {
                    return Err(failure(ErrorCode::ModelContextIncompatible));
                }
            }
            // Replay the provider items once, in their original order. Do not also
            // append their normalized text and calls, which would duplicate them.
            input.extend(items.iter().cloned());
            continue;
        }
        let role = match message.role {
            ModelRole::System => "system",
            ModelRole::User => "user",
            ModelRole::Assistant => "assistant",
            ModelRole::Tool => "tool",
        };
        let mut parts = Vec::new();
        for content in &message.content {
            if message.role == ModelRole::Tool
                && !matches!(content, ModelContent::ToolResult { .. })
            {
                return Err(failure(ErrorCode::ModelContextIncompatible));
            }
            match content {
                ModelContent::Text { text } => parts.push(text.clone()),
                ModelContent::Json { value } => {
                    parts.push(
                        serde_json::to_string(value)
                            .map_err(|_| failure(ErrorCode::InvalidContract))?,
                    );
                }
                ModelContent::ToolCall {
                    provider_call_id,
                    name,
                    arguments,
                } => {
                    append_text(&mut input, role, &mut parts);
                    input.push(json!({"type":"function_call","call_id":provider_call_id,"name":name,"arguments":serde_json::to_string(arguments).map_err(|_| failure(ErrorCode::InvalidContract))?}));
                }
                ModelContent::ToolResult {
                    provider_call_id,
                    content,
                } => {
                    append_text(&mut input, role, &mut parts);
                    input.push(json!({"type":"function_call_output","call_id":provider_call_id,"output":serde_json::to_string(content).map_err(|_| failure(ErrorCode::InvalidContract))?}));
                }
                ModelContent::Opaque { .. } => unreachable!("opaque handled before normalization"),
            }
        }
        append_text(&mut input, role, &mut parts);
    }
    let tools: Vec<_> = request.tools.iter().map(|tool| json!({"type":"function","name":tool.name,"description":tool.description,"parameters":tool.model_input_schema,"strict":false})).collect();
    let mut payload = json!({"model":request.route.model_id,"input":input,"max_output_tokens":request.max_output_tokens.get(),"store":false,"stream":true,"truncation":"disabled","include":["reasoning.encrypted_content"]});
    if !tools.is_empty() {
        payload["tools"] = json!(tools);
        payload["parallel_tool_calls"] = json!(false);
        payload["tool_choice"] = json!("auto");
    }
    if let Some(effort) = request.options.get("reasoning_effort") {
        let effort = effort
            .as_str()
            .filter(|effort| {
                ["none", "minimal", "low", "medium", "high", "xhigh", "max"].contains(effort)
            })
            .ok_or_else(|| failure(ErrorCode::ModelOptionUnsupported))?;
        payload["reasoning"] = json!({"effort":effort});
    }
    for (key, maximum) in [("temperature", 2.0), ("top_p", 1.0)] {
        if let Some(value) = request.options.get(key) {
            if !value
                .as_f64()
                .is_some_and(|number| number.is_finite() && number >= 0.0 && number <= maximum)
            {
                return Err(failure(ErrorCode::ModelOptionUnsupported));
            }
            payload[key] = value.clone();
        }
    }
    if let ModelOutput::JsonSchema { schema } = &request.output {
        validate_output_schema(schema)?;
        payload["text"] = json!({"format":{"type":"json_schema","name":"agent_output","strict":true,"schema":schema}});
    }
    if let Some(value) = request.options.get("verbosity") {
        if !matches!(value.as_str(), Some("low" | "medium" | "high")) {
            return Err(failure(ErrorCode::ModelOptionUnsupported));
        }
        if payload.get("text").is_none() {
            payload["text"] = json!({});
        }
        payload["text"]["verbosity"] = value.clone();
    }
    Ok(payload)
}

fn append_text(input: &mut Vec<Value>, role: &str, parts: &mut Vec<String>) {
    if !parts.is_empty() {
        input.push(json!({"role":role,"content":parts.join("\n")}));
        parts.clear();
    }
}

pub(crate) struct OutputCall {
    pub index: u32,
    pub call_id: String,
    pub name: String,
    pub arguments: String,
}
pub(crate) struct OutputItems {
    pub text: String,
    pub calls: Vec<OutputCall>,
    pub refused: bool,
}
pub(crate) fn inspect_output_items(items: &[Value]) -> Result<OutputItems, ContractError> {
    let mut output = OutputItems {
        text: String::new(),
        calls: vec![],
        refused: false,
    };
    let mut call_ids = std::collections::BTreeSet::new();
    for (index, item) in items.iter().enumerate() {
        let object = item
            .as_object()
            .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
        if object
            .get("id")
            .is_some_and(|id| id.as_str().is_none_or(str::is_empty))
            || object
                .get("status")
                .is_some_and(|status| status != "completed")
        {
            return Err(failure(ErrorCode::InvalidContract));
        }
        let allowed: &[&str] = match item.get("type").and_then(Value::as_str) {
            Some("reasoning") => {
                let summary = item
                    .get("summary")
                    .and_then(Value::as_array)
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                if item
                    .get("encrypted_content")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
                    || summary.iter().any(|part| {
                        part.get("type") != Some(&json!("summary_text"))
                            || part.get("text").and_then(Value::as_str).is_none()
                    })
                {
                    return Err(failure(ErrorCode::ModelContextIncompatible));
                }
                if let Some(content) = item.get("content") {
                    let content = content
                        .as_array()
                        .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                    if content.iter().any(|part| {
                        part.get("type") != Some(&json!("reasoning_text"))
                            || part.get("text").and_then(Value::as_str).is_none()
                            || part.as_object().is_none_or(|part| {
                                part.keys()
                                    .any(|key| !matches!(key.as_str(), "type" | "text"))
                            })
                    }) {
                        return Err(failure(ErrorCode::InvalidContract));
                    }
                }
                // Preserve reasoning content inside route-bound opaque replay only.
                &[
                    "type",
                    "id",
                    "status",
                    "summary",
                    "encrypted_content",
                    "content",
                ]
            }
            Some("message") if item.get("role") == Some(&json!("assistant")) => {
                if item.get("phase").is_some_and(|phase| {
                    !phase.is_null()
                        && !matches!(phase.as_str(), Some("commentary" | "final_answer"))
                }) {
                    return Err(failure(ErrorCode::InvalidContract));
                }
                for part in item
                    .get("content")
                    .and_then(Value::as_array)
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?
                {
                    let text = match part.get("type").and_then(Value::as_str) {
                        Some("output_text") => part.get("text").and_then(Value::as_str),
                        Some("refusal") => {
                            output.refused = true;
                            part.get("refusal").and_then(Value::as_str)
                        }
                        _ => return Err(failure(ErrorCode::InvalidContract)),
                    }
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                    output.text.push_str(text);
                }
                &["type", "id", "status", "role", "content", "phase"]
            }
            Some("function_call") => {
                let call_id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                let arguments = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                if call_id.is_empty()
                    || call_id.len() > 256
                    || call_id.chars().any(|c| c.is_whitespace() || c.is_control())
                    || name.is_empty()
                    || name.len() > 64
                    || !name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                    || !call_ids.insert(call_id)
                    || !parse_json(arguments)?.is_object()
                {
                    return Err(failure(ErrorCode::InvalidContract));
                }
                output.calls.push(OutputCall {
                    index: index
                        .try_into()
                        .map_err(|_| failure(ErrorCode::InvalidContract))?,
                    call_id: call_id.into(),
                    name: name.into(),
                    arguments: arguments.into(),
                });
                &["type", "id", "status", "call_id", "name", "arguments"]
            }
            _ => return Err(failure(ErrorCode::CapabilityUnsupported)),
        };
        if object.keys().any(|key| !allowed.contains(&key.as_str())) {
            return Err(failure(ErrorCode::CapabilityUnsupported));
        }
    }
    if output.refused && !output.calls.is_empty() {
        return Err(failure(ErrorCode::InvalidContract));
    }
    Ok(output)
}

pub(crate) fn fragments(text: &str, maximum: usize) -> Result<Vec<String>, ContractError> {
    let mut remaining = text;
    let mut chunks = Vec::new();
    while !remaining.is_empty() {
        let mut boundary = maximum.min(remaining.len());
        while boundary > 0 && !remaining.is_char_boundary(boundary) {
            boundary -= 1;
        }
        if boundary == 0 {
            return Err(failure(ErrorCode::InvalidContract));
        }
        chunks.push(remaining[..boundary].into());
        remaining = &remaining[boundary..];
    }
    Ok(chunks)
}

fn validate_output_schema(schema: &Value) -> Result<(), ContractError> {
    if schema.get("type") != Some(&json!("object")) || schema.get("anyOf").is_some() {
        return Err(failure(ErrorCode::ModelCapabilityUnsupported));
    }
    fn node(schema: &Value, depth: usize) -> Result<(), ContractError> {
        let object = schema
            .as_object()
            .ok_or_else(|| failure(ErrorCode::ModelCapabilityUnsupported))?;
        if depth > 10
            || [
                "allOf",
                "oneOf",
                "not",
                "dependentRequired",
                "dependentSchemas",
                "if",
                "then",
                "else",
                "patternProperties",
                "propertyNames",
                "unevaluatedProperties",
                "prefixItems",
                "contains",
                "uniqueItems",
            ]
            .iter()
            .any(|key| object.contains_key(*key))
        {
            return Err(failure(ErrorCode::ModelCapabilityUnsupported));
        }
        if schema
            .get("$ref")
            .is_some_and(|value| value.as_str().is_none_or(|value| !value.starts_with('#')))
        {
            return Err(failure(ErrorCode::ModelCapabilityUnsupported));
        }
        let is_object = schema.get("type").is_some_and(|kind| {
            kind == "object"
                || kind
                    .as_array()
                    .is_some_and(|types| types.iter().any(|kind| kind == "object"))
        });
        if is_object {
            let properties = schema
                .get("properties")
                .and_then(Value::as_object)
                .ok_or_else(|| failure(ErrorCode::ModelCapabilityUnsupported))?;
            let required = schema
                .get("required")
                .and_then(Value::as_array)
                .ok_or_else(|| failure(ErrorCode::ModelCapabilityUnsupported))?;
            let names: std::collections::BTreeSet<_> =
                required.iter().filter_map(Value::as_str).collect();
            if schema.get("additionalProperties") != Some(&json!(false))
                || required.len() != names.len()
                || names.len() != properties.len()
                || properties.keys().any(|key| !names.contains(key.as_str()))
            {
                return Err(failure(ErrorCode::ModelCapabilityUnsupported));
            }
        }
        for key in ["properties", "$defs"] {
            if let Some(values) = object.get(key) {
                for child in values
                    .as_object()
                    .ok_or_else(|| failure(ErrorCode::ModelCapabilityUnsupported))?
                    .values()
                {
                    node(child, depth + 1)?;
                }
            }
        }
        if let Some(items) = object.get("items") {
            node(items, depth + 1)?;
        }
        if let Some(items) = object.get("anyOf") {
            for child in items
                .as_array()
                .ok_or_else(|| failure(ErrorCode::ModelCapabilityUnsupported))?
            {
                node(child, depth + 1)?;
            }
        }
        Ok(())
    }
    node(schema, 0)
}
```

## `crates/wickle-model-responses/src/lib.rs`

```rust
//! Shared Responses wire codecs. Provider authentication, endpoints, model
//! selection, and capability policy belong to the calling adapter and Host.
#![forbid(unsafe_code)]
mod codec;
mod response;
mod sse;
pub use codec::encode_request;
pub use response::Decoder as ResponsesDecoder;
pub use sse::{Decoder as SseDecoder, Event as SseEvent};
use wickle::{ContractError, ErrorCode};
fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("responses.{location}"))
}
```

## `crates/wickle-model-responses/src/response.rs`

```rust
use crate::{codec, error, sse};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use wickle::*;

struct Call {
    id: String,
    name: String,
    arguments: String,
}
/// Stateful Responses event validation for one physical model request.
pub struct Decoder<'a> {
    request: &'a ModelRequest,
    /// Provider-reported metadata, including an optional HTTP request identifier.
    pub metadata: ModelResponseMetadata,
    response_id: Option<String>,
    sequence: Option<u64>,
    items: BTreeMap<u32, (String, String)>,
    calls: BTreeMap<u32, Call>,
    parts: BTreeMap<(u32, u32), (String, String)>,
    text: String,
    refused: bool,
    bytes: usize,
    emitted: usize,
    terminal: Option<ModelEvent>,
    done: bool,
}
impl<'a> Decoder<'a> {
    /// Start an attempt without making an HTTP request.
    pub fn new(request: &'a ModelRequest, request_id: Option<Id>) -> Self {
        Self {
            request,
            metadata: ModelResponseMetadata {
                provider_request_id: request_id,
                ..Default::default()
            },
            response_id: None,
            sequence: None,
            items: BTreeMap::new(),
            calls: BTreeMap::new(),
            parts: BTreeMap::new(),
            text: String::new(),
            refused: false,
            bytes: 0,
            emitted: 0,
            terminal: None,
            done: false,
        }
    }
    /// Validate a framed provider event and emit normalized model deltas.
    pub fn event(&mut self, event: sse::Event) -> Result<Vec<ModelEvent>, ContractError> {
        if event.data == "[DONE]" {
            if self.terminal.is_none() || self.done {
                return Err(invalid());
            }
            self.done = true;
            return Ok(vec![]);
        }
        if self.terminal.is_some() || self.done {
            return Err(invalid());
        }
        let value = parse_json(&event.data).map_err(|_| invalid())?;
        let kind = string(&value, "type")?;
        if event
            .name
            .as_ref()
            .is_some_and(|name| !name.is_empty() && name != "message" && name != kind)
        {
            return Err(invalid());
        }
        if let Some(number) = value.get("sequence_number") {
            let number = number.as_u64().ok_or_else(invalid)?;
            if self.sequence.is_some_and(|previous| number <= previous) {
                return Err(invalid());
            }
            self.sequence = Some(number);
        }
        if let Some(id) = value.get("response_id") {
            self.identify(id.as_str().ok_or_else(invalid)?)?;
        }
        let mut output = vec![];
        match kind {
            "response.created" | "response.in_progress" => {
                let response = value.get("response").ok_or_else(invalid)?;
                self.identify(string(response, "id")?)?;
                self.read_metadata(response)?;
            }
            "response.output_item.added" => {
                let index = index(&value, "output_index")?;
                let item = value.get("item").ok_or_else(invalid)?;
                let item_id = string(item, "id")?;
                let item_type = string(item, "type")?;
                if !matches!(item_type, "message" | "reasoning" | "function_call") {
                    return Err(error(ErrorCode::CapabilityUnsupported, "output_item"));
                }
                if self.items.values().any(|(_, id)| id == item_id)
                    || self
                        .items
                        .insert(index, (item_type.into(), item_id.into()))
                        .is_some()
                {
                    return Err(invalid());
                }
                if item_type == "function_call" {
                    let call_id = string(item, "call_id")?;
                    let name = string(item, "name")?;
                    if self.calls.len() >= self.request.limits.max_tool_calls
                        || self.calls.values().any(|call| call.id == call_id)
                    {
                        return Err(invalid());
                    }
                    let arguments = string_allow_empty(item, "arguments")?;
                    self.charge(
                        call_id
                            .len()
                            .saturating_add(name.len())
                            .saturating_add(arguments.len()),
                    )?;
                    self.calls.insert(
                        index,
                        Call {
                            id: call_id.into(),
                            name: name.into(),
                            arguments: arguments.into(),
                        },
                    );
                    let pieces = self.fragments(arguments)?;
                    for (position, delta) in pieces.into_iter().enumerate() {
                        output.push(ModelEvent::ToolArgumentsDelta {
                            index,
                            provider_call_id: (position == 0).then(|| call_id.into()),
                            name: (position == 0).then(|| name.into()),
                            delta,
                        });
                    }
                }
            }
            "response.function_call_arguments.delta" => {
                let index = self.item(&value, "function_call")?;
                let delta = string_allow_empty(&value, "delta")?;
                self.charge(delta.len())?;
                self.calls
                    .get_mut(&index)
                    .ok_or_else(invalid)?
                    .arguments
                    .push_str(delta);
                for delta in self.fragments(delta)? {
                    output.push(ModelEvent::ToolArgumentsDelta {
                        index,
                        provider_call_id: None,
                        name: None,
                        delta,
                    });
                }
            }
            "response.function_call_arguments.done" => {
                let index = self.item(&value, "function_call")?;
                let call = self.calls.get(&index).ok_or_else(invalid)?;
                if call.arguments != string_allow_empty(&value, "arguments")?
                    || value
                        .get("name")
                        .is_some_and(|name| name.as_str() != Some(call.name.as_str()))
                {
                    return Err(invalid());
                }
            }
            "response.output_text.delta" | "response.refusal.delta" => {
                self.refused |= kind == "response.refusal.delta";
                let index = self.item(&value, "message")?;
                let part = index_value(&value, "content_index")?;
                let delta = string_allow_empty(&value, "delta")?;
                self.charge(delta.len())?;
                let part_kind = if kind == "response.refusal.delta" {
                    "refusal"
                } else {
                    "output_text"
                };
                let accumulated = self
                    .parts
                    .entry((index, part))
                    .or_insert_with(|| (part_kind.into(), String::new()));
                if accumulated.0 != part_kind {
                    return Err(invalid());
                }
                accumulated.1.push_str(delta);
                self.text.push_str(delta);
                for text in self.fragments(delta)? {
                    output.push(ModelEvent::TextDelta { text });
                }
            }
            "response.output_text.done" | "response.refusal.done" => {
                self.refused |= kind == "response.refusal.done";
                let index = self.item(&value, "message")?;
                let part = index_value(&value, "content_index")?;
                let key = if kind == "response.refusal.done" {
                    "refusal"
                } else {
                    "text"
                };
                let part_kind = if key == "refusal" {
                    "refusal"
                } else {
                    "output_text"
                };
                let accumulated = self
                    .parts
                    .entry((index, part))
                    .or_insert_with(|| (part_kind.into(), String::new()));
                if accumulated.0 != part_kind || accumulated.1 != string_allow_empty(&value, key)? {
                    return Err(invalid());
                }
            }
            "response.output_item.done" => {
                let index = index(&value, "output_index")?;
                let item = value.get("item").ok_or_else(invalid)?;
                let identity = self.items.get(&index).ok_or_else(invalid)?;
                if identity.0 != string(item, "type")? || identity.1 != string(item, "id")? {
                    return Err(invalid());
                }
                if identity.0 == "message" {
                    self.check_message(index, item)?;
                }
                if identity.0 == "function_call" {
                    let call = self.calls.get(&index).ok_or_else(invalid)?;
                    if call.id != string(item, "call_id")?
                        || call.name != string(item, "name")?
                        || call.arguments != string_allow_empty(item, "arguments")?
                    {
                        return Err(invalid());
                    }
                }
            }
            "response.completed" | "response.incomplete" | "response.failed" => {
                let response = value.get("response").ok_or_else(invalid)?;
                self.identify(string(response, "id")?)?;
                self.read_metadata(response)?;
                let expected = kind.strip_prefix("response.").expect("matched prefix");
                if string(response, "status")? != expected {
                    return Err(invalid());
                }
                self.terminal = Some(match expected {
                    "completed" => {
                        let items = response
                            .get("output")
                            .and_then(Value::as_array)
                            .ok_or_else(invalid)?;
                        let decoded = codec::inspect_output_items(items)?;
                        if decoded.refused != self.refused
                            || decoded.text != self.text
                            || decoded.calls.len() != self.calls.len()
                            || items.len() != self.items.len()
                        {
                            return Err(invalid());
                        }
                        for (position, item) in items.iter().enumerate() {
                            let key = u32::try_from(position).map_err(|_| invalid())?;
                            let identity = self.items.get(&key).ok_or_else(invalid)?;
                            if identity.0 != string(item, "type")?
                                || identity.1 != string(item, "id")?
                            {
                                return Err(invalid());
                            }
                        }
                        for (position, item) in items.iter().enumerate() {
                            if item["type"] == "message" {
                                self.check_message(
                                    u32::try_from(position).map_err(|_| invalid())?,
                                    item,
                                )?;
                            }
                        }
                        for call in &decoded.calls {
                            let prior = self.calls.get(&call.index).ok_or_else(invalid)?;
                            if call.call_id != prior.id
                                || call.name != prior.name
                                || call.arguments != prior.arguments
                            {
                                return Err(invalid());
                            }
                        }
                        let continuation = if items.is_empty() {
                            vec![]
                        } else {
                            let data = json!({"kind":codec::REPLAY_KIND,"items":items});
                            self.charge(serde_json::to_vec(&data).map_err(|_| invalid())?.len())?;
                            vec![OpaqueContinuation::new(&self.request.route, data)]
                        };
                        ModelEvent::ResponseCompleted {
                            finish: if decoded.refused {
                                ModelFinish::Refusal
                            } else if !decoded.calls.is_empty() {
                                ModelFinish::ToolCalls
                            } else {
                                ModelFinish::Stop
                            },
                            metadata: self.metadata.clone(),
                            continuation,
                        }
                    }
                    "incomplete" => ModelEvent::ResponseCompleted {
                        finish: match response
                            .pointer("/incomplete_details/reason")
                            .and_then(Value::as_str)
                        {
                            Some("max_output_tokens") => ModelFinish::Length,
                            Some("content_filter") => ModelFinish::Refusal,
                            _ => return Err(invalid()),
                        },
                        metadata: self.metadata.clone(),
                        continuation: vec![],
                    },
                    _ => ModelEvent::ResponseError {
                        kind: failure_kind(response.pointer("/error/code").and_then(Value::as_str)),
                        metadata: self.metadata.clone(),
                    },
                });
            }
            "error" => {
                self.terminal = Some(ModelEvent::ResponseError {
                    kind: failure_kind(value.get("code").and_then(Value::as_str)),
                    metadata: self.metadata.clone(),
                });
            }
            // Content boundaries, annotations, and reasoning progress are not
            // user text or Tool instructions. Final output preserves their items.
            "response.content_part.added"
            | "response.content_part.done"
            | "response.output_text.annotation.added"
            | "response.reasoning_summary_part.added"
            | "response.reasoning_summary_part.done"
            | "response.reasoning_summary_text.delta"
            | "response.reasoning_summary_text.done"
            | "response.reasoning_text.delta"
            | "response.reasoning_text.done" => {}
            _ => return Err(error(ErrorCode::CapabilityUnsupported, "stream_event")),
        }
        self.emitted = self
            .emitted
            .checked_add(output.len())
            .filter(|count| *count < self.request.limits.max_events)
            .ok_or_else(invalid)?;
        Ok(output)
    }
    /// Return the validated terminal only after the transport reaches a clean EOF.
    pub fn finish(&mut self) -> Result<ModelEvent, ContractError> {
        self.terminal.take().ok_or_else(invalid)
    }
    fn identify(&mut self, id: &str) -> Result<(), ContractError> {
        if id.is_empty() || self.response_id.as_ref().is_some_and(|old| old != id) {
            return Err(invalid());
        }
        self.response_id = Some(id.into());
        Ok(())
    }
    fn item(&self, value: &Value, kind: &str) -> Result<u32, ContractError> {
        let index = index(value, "output_index")?;
        let (actual, id) = self.items.get(&index).ok_or_else(invalid)?;
        if actual != kind || id != string(value, "item_id")? {
            return Err(invalid());
        }
        Ok(index)
    }
    fn check_message(&self, output_index: u32, item: &Value) -> Result<(), ContractError> {
        if string(item, "role")? != "assistant" {
            return Err(invalid());
        }
        let content = item
            .get("content")
            .and_then(Value::as_array)
            .ok_or_else(invalid)?;
        for (position, part) in content.iter().enumerate() {
            let content_index = u32::try_from(position).map_err(|_| invalid())?;
            let kind = string(part, "type")?;
            let key = match kind {
                "output_text" => "text",
                "refusal" => "refusal",
                _ => return Err(invalid()),
            };
            let text = string_allow_empty(part, key)?;
            match self.parts.get(&(output_index, content_index)) {
                Some((prior_kind, prior_text)) if prior_kind == kind && prior_text == text => {}
                None if text.is_empty() => {}
                _ => return Err(invalid()),
            }
        }
        if self
            .parts
            .keys()
            .any(|(item, part)| *item == output_index && *part as usize >= content.len())
        {
            return Err(invalid());
        }
        Ok(())
    }
    fn charge(&mut self, bytes: usize) -> Result<(), ContractError> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|count| *count <= self.request.limits.max_response_bytes)
            .ok_or_else(invalid)?;
        Ok(())
    }
    fn fragments(&self, text: &str) -> Result<Vec<String>, ContractError> {
        let maximum = self.request.limits.max_delta_bytes;
        let remaining = self
            .request
            .limits
            .max_events
            .saturating_sub(self.emitted + 1);
        if maximum == 0 || text.len() / maximum > remaining {
            return Err(invalid());
        }
        let pieces = if text.is_empty() {
            vec![String::new()]
        } else {
            codec::fragments(text, maximum)?
        };
        if pieces.len() > remaining {
            return Err(invalid());
        }
        Ok(pieces)
    }
    fn read_metadata(&mut self, response: &Value) -> Result<(), ContractError> {
        if let Some(model) = response.get("model").filter(|value| !value.is_null()) {
            let model = Id::new(model.as_str().ok_or_else(invalid)?).map_err(|_| invalid())?;
            if self
                .metadata
                .reported_model_id
                .as_ref()
                .is_some_and(|old| old != &model)
            {
                return Err(invalid());
            }
            self.metadata.reported_model_id = Some(model);
        }
        if let Some(usage) = response.get("usage").filter(|value| !value.is_null()) {
            if !usage.is_object() {
                return Err(invalid());
            }
            let count = |name| -> Result<Option<u64>, ContractError> {
                usage
                    .get(name)
                    .filter(|value| !value.is_null())
                    .map(|value| value.as_u64().ok_or_else(invalid))
                    .transpose()
            };
            self.metadata.usage = Some(ModelUsage {
                measurement: UsageMeasurement::Reported,
                input_tokens: count("input_tokens")?,
                output_tokens: count("output_tokens")?,
            });
        }
        Ok(())
    }
}
fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str, ContractError> {
    string_allow_empty(value, key).and_then(|value| {
        if value.is_empty() {
            Err(invalid())
        } else {
            Ok(value)
        }
    })
}
fn string_allow_empty<'a>(value: &'a Value, key: &str) -> Result<&'a str, ContractError> {
    value.get(key).and_then(Value::as_str).ok_or_else(invalid)
}
fn index(value: &Value, key: &str) -> Result<u32, ContractError> {
    index_value(value, key)
}
fn index_value(value: &Value, key: &str) -> Result<u32, ContractError> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|value| value.try_into().ok())
        .ok_or_else(invalid)
}
fn invalid() -> ContractError {
    error(ErrorCode::InvalidContract, "response")
}
pub(crate) fn failure_kind(code: Option<&str>) -> ModelFailureKind {
    match code {
        Some("context_length_exceeded") => ModelFailureKind::ContextOverflow,
        Some("rate_limit_exceeded" | "insufficient_quota") => ModelFailureKind::RateLimited,
        Some("server_error") => ModelFailureKind::Transport,
        Some("model_not_found") => ModelFailureKind::Unavailable,
        Some("invalid_request_error" | "invalid_prompt" | "unsupported_parameter") => {
            ModelFailureKind::Unsupported
        }
        _ => ModelFailureKind::Protocol,
    }
}
```

## `crates/wickle-model-responses/src/sse.rs`

```rust
use crate::error;
use wickle::{ContractError, ErrorCode};

/// A complete SSE data record.
pub struct Event {
    /// Optional SSE event name.
    pub name: Option<String>,
    /// UTF-8 data with multiline fields joined by newlines.
    pub data: String,
}

/// Incremental SSE framing; JSON and UTF-8 may be split across network chunks.
pub struct Decoder {
    line: Vec<u8>,
    data: Vec<u8>,
    name: Option<String>,
    skip_lf: bool,
    frame_bytes: usize,
    bytes: usize,
    frames: usize,
    max_bytes: usize,
    max_frame: usize,
    max_frames: usize,
}
impl Decoder {
    /// Set finite limits on total bytes, one event, and event count.
    pub fn new(max_bytes: usize, max_frame: usize, max_frames: usize) -> Self {
        Self {
            line: vec![],
            data: vec![],
            name: None,
            skip_lf: false,
            frame_bytes: 0,
            bytes: 0,
            frames: 0,
            max_bytes,
            max_frame,
            max_frames,
        }
    }
    /// Frame arbitrary network chunks without assuming UTF-8 or line boundaries.
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Event>, ContractError> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|count| *count <= self.max_bytes)
            .ok_or_else(limit)?;
        let mut events = vec![];
        for &byte in bytes {
            if self.skip_lf {
                self.skip_lf = false;
                if byte == b'\n' {
                    continue;
                }
            }
            self.frame_bytes = self
                .frame_bytes
                .checked_add(1)
                .filter(|count| *count <= self.max_frame)
                .ok_or_else(limit)?;
            match byte {
                b'\r' => {
                    self.line(&mut events)?;
                    self.skip_lf = true;
                }
                b'\n' => self.line(&mut events)?,
                byte => self.line.push(byte),
            }
        }
        Ok(events)
    }
    fn line(&mut self, events: &mut Vec<Event>) -> Result<(), ContractError> {
        if self.line.is_empty() {
            self.frame_bytes = 0;
            if !self.data.is_empty() {
                self.frames = self
                    .frames
                    .checked_add(1)
                    .filter(|count| *count <= self.max_frames)
                    .ok_or_else(limit)?;
                self.data.pop();
                let data =
                    String::from_utf8(std::mem::take(&mut self.data)).map_err(|_| invalid())?;
                events.push(Event {
                    name: self.name.take(),
                    data,
                });
            } else {
                self.name = None;
            }
            return Ok(());
        }
        let line = std::str::from_utf8(&self.line).map_err(|_| invalid())?;
        let (field, mut value) = line.split_once(':').unwrap_or((line, ""));
        if let Some(rest) = value.strip_prefix(' ') {
            value = rest;
        }
        match field {
            "data" => {
                self.data.extend_from_slice(value.as_bytes());
                self.data.push(b'\n');
            }
            "event" => {
                self.name = Some(value.to_owned());
            }
            _ => {} // SSE comments, id, retry, and extension fields do not reconnect.
        }
        self.line.clear();
        Ok(())
    }
    /// Reject a truncated final record.
    pub fn finish(&self) -> Result<(), ContractError> {
        if !self.line.is_empty() || !self.data.is_empty() || self.name.is_some() {
            return Err(invalid());
        }
        Ok(())
    }
}
fn invalid() -> ContractError {
    error(ErrorCode::InvalidContract, "sse")
}
fn limit() -> ContractError {
    error(ErrorCode::ContextBudgetExceeded, "sse_limit")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn byte_split_utf8_and_all_line_endings_preserve_event_data() {
        for ending in ["\n", "\r\n", "\r"] {
            let body = format!(
                ": heartbeat{ending}event: response.created{ending}data: {{\"text\":{ending}data: \"안녕\"}}{ending}{ending}"
            );
            let mut decoder = Decoder::new(4096, 1024, 2);
            let mut events = vec![];
            for byte in body.as_bytes() {
                events.extend(decoder.push(&[*byte]).unwrap());
            }
            decoder.finish().unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].name.as_deref(), Some("response.created"));
            assert_eq!(events[0].data, "{\"text\":\n\"안녕\"}");
        }
    }
    #[test]
    fn incomplete_frames_and_normalized_event_bounds_never_dispatch_a_partial_record() {
        let mut decoder = Decoder::new(1024, 128, 1);
        assert!(decoder.push(b"data: {\"x\":1}\n").unwrap().is_empty());
        assert!(decoder.finish().is_err());
        let mut decoder = Decoder::new(1024, 8, 2);
        assert!(decoder.push(b"data: too large\n\n").is_err());
        let mut decoder = Decoder::new(1024, 128, 1);
        assert_eq!(decoder.push(b"data: first\n\n").unwrap().len(), 1);
        assert!(decoder.push(b"data: second\n\n").is_err());
        let mut decoder = Decoder::new(4, 128, 2);
        assert!(decoder.push(b"12345").is_err());
    }
}
```

## `tests/support/azure_consumer.rs`

```rust
// Real loopback HTTP/SSE against the extracted Azure adapter package; no provider call.
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use wickle::*;
use wickle_model_azure_openai::*;
fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}/", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = vec![];
        let (header_end, length) = loop {
            let mut chunk = [0; 4096];
            let size = socket.read(&mut chunk).await.unwrap();
            assert!(size > 0);
            bytes.extend_from_slice(&chunk[..size]);
            assert!(bytes.len() < 65536);
            if let Some(end) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                let headers = std::str::from_utf8(&bytes[..end])
                    .unwrap()
                    .to_ascii_lowercase();
                let length = headers
                    .lines()
                    .find_map(|s| s.strip_prefix("content-length:"))
                    .unwrap()
                    .trim()
                    .parse::<usize>()
                    .unwrap();
                break (end + 4, length);
            }
        };
        while bytes.len() < header_end + length {
            let mut chunk = [0; 4096];
            let size = socket.read(&mut chunk).await.unwrap();
            assert!(size > 0);
            bytes.extend_from_slice(&chunk[..size]);
        }
        let headers = std::str::from_utf8(&bytes[..header_end])
            .unwrap()
            .to_ascii_lowercase();
        assert!(headers.starts_with("post /openai/v1/responses "));
        assert!(headers.contains("api-key: fixture-key"));
        let body =
            parse_json(std::str::from_utf8(&bytes[header_end..header_end + length]).unwrap())
                .unwrap();
        assert_eq!(body["model"], "fixture-deployment");
        assert_eq!(body["stream"], true);
        assert_eq!(body["store"], false);
        assert_eq!(body["reasoning"]["effort"], "medium");
        assert_eq!(body["text"]["format"]["strict"], true);
        assert!(!body.to_string().contains("private-workspace"));
        assert!(!body.to_string().contains("fixture-key"));
        let item = json!({"id":"message","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"{\"answer\":42}","annotations":[]}]});
        let events = vec![
            json!({"type":"response.created","response":{"id":"response","model":"fixture-model","status":"in_progress"}}),
            json!({"type":"response.output_item.added","output_index":0,"item":{"id":"message","type":"message","role":"assistant","content":[]}}),
            json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"message","delta":"{\"answer\":42}"}),
            json!({"type":"response.output_item.done","output_index":0,"item":item}),
            json!({"type":"response.completed","response":{"id":"response","model":"fixture-model","status":"completed","output":[item],"usage":{"input_tokens":12,"output_tokens":5}}}),
        ];
        let wire: String = events
            .into_iter()
            .map(|value| format!("data: {value}\n\n"))
            .collect();
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",wire.len()).as_bytes()).await.unwrap();
        for chunk in wire.as_bytes().chunks(5) {
            socket.write_all(chunk).await.unwrap();
            tokio::task::yield_now().await;
        }
        socket.shutdown().await.unwrap();
    });
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("private-workspace"),
        user_id: None,
    };
    let connection = AzureOpenAiConnection::new(
        scope.clone(),
        reference("account"),
        Arc::new(AzureCredential::ApiKey("fixture-key".into())),
        AzureOpenAiOptions::new(base, "fixture-deployment"),
    )?;
    let binding = connection.binding();
    let model = AzureOpenAiModel::new(connection.clone());
    let request = ModelRequest {
        request_id: id("attempt"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference("model"),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id("fixture-model"),
            model_id: id("fixture-model"),
            model_version: id("release"),
            version_semantics: VersionSemantics::Unverified,
            provider: binding.provider,
            target: connection.target().clone(),
            deployment_revision: None,
            api_contract: AzureOpenAiConnection::api_contract(),
            adapter: binding.adapter,
            capability_revision: id("caps"),
            connection_ref: binding.connection_ref,
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Return the fixture answer.".into(),
            }],
        }],
        tools: vec![],
        output: ModelOutput::JsonSchema {
            schema: json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}),
        },
        max_output_tokens: 128.try_into()?,
        options: JsonObject::from([("reasoning_effort".into(), json!("medium"))]),
        limits: ModelResponseLimits {
            max_input_bytes: 32768,
            max_response_bytes: 32768,
            max_delta_bytes: 128,
            max_events: 64,
            max_tool_calls: 0,
        },
    };
    let context = ModelCallContext {
        attempt_id: request.request_id.clone(),
        run_id: id("run"),
        scope,
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(10),
    };
    let response = collect_model_response(&request, model.generate(&request, &context)).await?;
    assert_eq!(parse_json(&response.text)?, json!({"answer":42}));
    assert_eq!(response.finish, ModelFinish::Stop);
    assert_eq!(
        response.metadata.reported_model_id,
        Some(id("fixture-model"))
    );
    assert!(response.metadata.reported_model_version.is_none());
    assert_eq!(
        response
            .metadata
            .usage
            .as_ref()
            .and_then(|u| u.output_tokens),
        Some(5)
    );
    server.await?;
    println!(
        "Azure consumer: extracted adapter performs one HTTP/SSE request, maps deployment separately from model identity, supplies scoped API-key authentication and options, decodes JSON and reported usage, and excludes Host context from the wire (local fixture, no provider network)"
    );
    Ok(())
}
```

## `tests/support/model_http.rs`

```rust
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
    task::JoinHandle,
};
use wickle::parse_json;
pub fn wire(events: &[Value]) -> Vec<u8> {
    let mut bytes = vec![];
    for (index, value) in events.iter().enumerate() {
        let mut value = value.clone();
        value["sequence_number"] = json!(index);
        bytes.extend_from_slice(
            format!(
                "event: {}\r\ndata: {}\r\n\r\n",
                value["type"].as_str().unwrap(),
                value
            )
            .as_bytes(),
        );
    }
    bytes
}
pub struct Reply {
    pub status: u16,
    pub content_type: &'static str,
    pub body: Vec<u8>,
    pub headers: Vec<(&'static str, String)>,
    pub stall: bool,
    pub chunk: usize,
}
impl Reply {
    pub fn sse(events: &[Value]) -> Self {
        Self {
            status: 200,
            content_type: "text/event-stream",
            body: wire(events),
            headers: vec![("x-request-id", "req-fixture".into())],
            stall: false,
            chunk: 7,
        }
    }
    pub fn json(status: u16, value: Value) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: serde_json::to_vec(&value).unwrap(),
            headers: vec![],
            stall: false,
            chunk: 4096,
        }
    }
}
#[derive(Debug)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub headers: String,
    pub body: Value,
}
pub struct Server {
    pub base: String,
    pub requests: Arc<Mutex<Vec<Request>>>,
    pub entered: Arc<Notify>,
    pub closed: Arc<Notify>,
    task: JoinHandle<()>,
}
impl Server {
    pub async fn new(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/v1/", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(vec![]));
        let records = requests.clone();
        let entered = Arc::new(Notify::new());
        let signal = entered.clone();
        let closed = Arc::new(Notify::new());
        let ended = closed.clone();
        let task = tokio::spawn(async move {
            let mut replies = std::collections::VecDeque::from(replies);
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let reply = replies.pop_front().unwrap_or_else(|| {
                    Reply::json(500, json!({"error":{"code":"unexpected_request"}}))
                });
                let mut bytes = vec![];
                let (header_end, length) = loop {
                    let mut block = [0; 4096];
                    let count = socket.read(&mut block).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&block[..count]);
                    assert!(bytes.len() <= 131_072);
                    if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let headers = std::str::from_utf8(&bytes[..index])
                            .unwrap()
                            .to_ascii_lowercase();
                        let length = headers
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length:"))
                            .map(|value| value.trim().parse::<usize>().unwrap())
                            .unwrap_or(0);
                        break (index + 4, length);
                    }
                };
                while bytes.len() < header_end + length {
                    let mut block = [0; 4096];
                    let count = socket.read(&mut block).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&block[..count]);
                }
                let headers = std::str::from_utf8(&bytes[..header_end])
                    .unwrap()
                    .to_owned();
                let mut first = headers.lines().next().unwrap().split_whitespace();
                records.lock().unwrap().push(Request {
                    method: first.next().unwrap().into(),
                    path: first.next().unwrap().into(),
                    headers: headers.clone(),
                    body: if length == 0 {
                        Value::Null
                    } else {
                        parse_json(
                            std::str::from_utf8(&bytes[header_end..header_end + length]).unwrap(),
                        )
                        .unwrap()
                    },
                });
                signal.notify_one();
                let extra: String = reply
                    .headers
                    .iter()
                    .map(|(key, value)| format!("{key}: {value}\r\n"))
                    .collect();
                let header = format!(
                    "HTTP/1.1 {} Response\r\nContent-Type: {}\r\nContent-Length: {}\r\n{}Connection: close\r\n\r\n",
                    reply.status,
                    reply.content_type,
                    reply.body.len() + if reply.stall { 100 } else { 0 },
                    extra
                );
                if socket.write_all(header.as_bytes()).await.is_err() {
                    ended.notify_one();
                    continue;
                }
                for chunk in reply.body.chunks(reply.chunk) {
                    if socket.write_all(chunk).await.is_err() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                if reply.stall {
                    let mut byte = [0];
                    let _ = socket.read(&mut byte).await;
                }
                let _ = socket.shutdown().await;
                ended.notify_one();
            }
        });
        Self {
            base,
            requests,
            entered,
            closed,
            task,
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
```
