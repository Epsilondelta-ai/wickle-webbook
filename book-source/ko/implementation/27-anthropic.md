# 27장 전체 Rust 구현과 테스트

[강의로](../27-anthropic.md) · [전체 변경 패치](../solutions/27-anthropic.patch)

기준 `cadda4c5015f262d428654b58c7afb822c4eb015`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle-model-anthropic/src/codec.rs`

```rust
use crate::error;
use serde_json::{Value, json};
use wickle::*;

pub(crate) const REPLAY_KIND: &str = "wickle.anthropic.messages.v1";
pub(crate) fn invalid() -> ContractError {
    error(ErrorCode::InvalidContract, "content")
}
pub(crate) fn encode_request(request: &ModelRequest) -> Result<Value, ContractError> {
    request.validate()?;
    if request.options.keys().any(|key| {
        !matches!(
            key.as_str(),
            "effort" | "thinking_mode" | "thinking_budget_tokens"
        )
    }) {
        return Err(error(ErrorCode::ModelOptionUnsupported, "options"));
    }
    let mut messages: Vec<Value> = vec![];
    let mut system = vec![];
    for message in &request.messages {
        let opaque: Vec<_> = message
            .content
            .iter()
            .filter_map(|part| {
                if let ModelContent::Opaque { continuation } = part {
                    Some(continuation)
                } else {
                    None
                }
            })
            .collect();
        let blocks = if opaque.is_empty() {
            let mut blocks = vec![];
            for part in &message.content {
                blocks.push(match part {
                    ModelContent::Text{text} if message.role != ModelRole::Tool => json!({"type":"text","text":text}),
                    ModelContent::Json{value} if message.role != ModelRole::Tool => json!({"type":"text","text":serde_json::to_string(value).map_err(|_|invalid())?}),
                    ModelContent::ToolCall{provider_call_id,name,arguments} if message.role==ModelRole::Assistant => json!({"type":"tool_use","id":provider_call_id,"name":name,"input":arguments}),
                    ModelContent::ToolResult{provider_call_id,content} if message.role==ModelRole::Tool => json!({"type":"tool_result","tool_use_id":provider_call_id,"content":serde_json::to_string(content).map_err(|_|invalid())?}),
                    _=>return Err(error(ErrorCode::ModelContextIncompatible,"message")),
                });
            }
            blocks
        } else {
            if opaque.len() != 1 || message.role != ModelRole::Assistant {
                return Err(invalid());
            }
            let data = opaque[0].data();
            if data.get("kind") != Some(&json!(REPLAY_KIND))
                || data.as_object().is_none_or(|o| o.len() != 2)
            {
                return Err(error(ErrorCode::ModelContextIncompatible, "replay"));
            }
            let blocks = data
                .get("blocks")
                .and_then(Value::as_array)
                .ok_or_else(invalid)?;
            let decoded = inspect_blocks(blocks)?;
            let text: String = message
                .content
                .iter()
                .filter_map(|p| {
                    if let ModelContent::Text { text } = p {
                        Some(text.as_str())
                    } else {
                        None
                    }
                })
                .collect();
            let calls: Vec<_> = message
                .content
                .iter()
                .filter_map(|p| {
                    if let ModelContent::ToolCall {
                        provider_call_id,
                        name,
                        arguments,
                    } = p
                    {
                        Some((provider_call_id, name, arguments))
                    } else {
                        None
                    }
                })
                .collect();
            if text != decoded.text
                || calls.len() != decoded.calls.len()
                || message.content.iter().any(|p| {
                    !matches!(
                        p,
                        ModelContent::Text { .. }
                            | ModelContent::ToolCall { .. }
                            | ModelContent::Opaque { .. }
                    )
                })
            {
                return Err(error(ErrorCode::ModelContextIncompatible, "replay"));
            }
            for ((id, name, args), original) in calls.iter().zip(&decoded.calls) {
                if id.as_str() != original.id
                    || name.as_str() != original.name
                    || serde_json::to_value(args).map_err(|_| invalid())? != original.input
                {
                    return Err(error(ErrorCode::ModelContextIncompatible, "replay"));
                }
            }
            let mut replay = blocks.clone();
            for block in &mut replay {
                if block["type"] == "tool_use" {
                    for key in ["caller", "toolset_name"] {
                        if block.get(key).is_some_and(Value::is_null) {
                            block.as_object_mut().ok_or_else(invalid)?.remove(key);
                        }
                    }
                }
            }
            replay
        };
        if message.role == ModelRole::System {
            if !messages.is_empty() {
                return Err(error(
                    ErrorCode::ModelContextIncompatible,
                    "system_position",
                ));
            }
            system.extend(blocks);
        } else {
            let role = if message.role == ModelRole::Assistant {
                "assistant"
            } else {
                "user"
            };
            if let Some(last) = messages.last_mut().filter(|last| last["role"] == role) {
                last["content"]
                    .as_array_mut()
                    .ok_or_else(invalid)?
                    .extend(blocks);
            } else {
                messages.push(json!({"role":role,"content":blocks}));
            }
        }
    }
    if messages.last().is_some_and(|m| m["role"] == "assistant") {
        return Err(error(
            ErrorCode::ModelContextIncompatible,
            "assistant_prefill",
        ));
    }
    let mut value = json!({"model":request.route.model_id,"messages":messages,"max_tokens":request.max_output_tokens,"stream":true});
    if !system.is_empty() {
        value["system"] = json!(system);
    }
    if !request.tools.is_empty() {
        value["tools"]=json!(request.tools.iter().map(|tool|json!({"name":tool.name,"description":tool.description,"input_schema":tool.model_input_schema})).collect::<Vec<_>>());
    }
    let mut output = serde_json::Map::new();
    if let Some(effort) = request.options.get("effort") {
        if !matches!(
            effort.as_str(),
            Some("low" | "medium" | "high" | "xhigh" | "max")
        ) {
            return Err(error(ErrorCode::ModelOptionUnsupported, "effort"));
        }
        output.insert("effort".into(), effort.clone());
    }
    if let ModelOutput::JsonSchema { schema } = &request.output {
        validate_output_schema(schema)?;
        output.insert(
            "format".into(),
            json!({"type":"json_schema","schema":schema}),
        );
    }
    if !output.is_empty() {
        value["output_config"] = Value::Object(output);
    }
    let mode = request.options.get("thinking_mode").and_then(Value::as_str);
    if request.options.contains_key("thinking_mode") && mode.is_none() {
        return Err(error(ErrorCode::ModelOptionUnsupported, "thinking"));
    }
    let budget = request.options.get("thinking_budget_tokens");
    match mode {
        Some("enabled") => {
            let budget = budget
                .and_then(Value::as_u64)
                .filter(|n| *n >= 1024 && *n < request.max_output_tokens.get())
                .ok_or_else(|| error(ErrorCode::ModelOptionUnsupported, "thinking_budget"))?;
            if matches!(
                request.route.model_id.as_str(),
                "claude-opus-5" | "claude-sonnet-5" | "claude-opus-4-7" | "claude-opus-4-8"
            ) {
                return Err(error(ErrorCode::ModelOptionUnsupported, "manual_thinking"));
            }
            value["thinking"] = json!({"type":"enabled","budget_tokens":budget});
        }
        Some("adaptive" | "disabled") => {
            if budget.is_some() {
                return Err(error(ErrorCode::ModelOptionUnsupported, "thinking_budget"));
            }
            if mode == Some("disabled")
                && request.route.model_id.as_str() == "claude-opus-5"
                && matches!(
                    request.options.get("effort").and_then(Value::as_str),
                    Some("xhigh" | "max")
                )
            {
                return Err(error(ErrorCode::ModelOptionUnsupported, "disabled_effort"));
            }
            value["thinking"] = json!({"type":mode});
        }
        None if budget.is_none() => {}
        _ => return Err(error(ErrorCode::ModelOptionUnsupported, "thinking")),
    }
    Ok(value)
}

pub(crate) struct Call {
    pub id: String,
    pub name: String,
    pub input: Value,
}
pub(crate) struct Blocks {
    pub text: String,
    pub calls: Vec<Call>,
}
pub(crate) fn inspect_blocks(blocks: &[Value]) -> Result<Blocks, ContractError> {
    let mut result = Blocks {
        text: String::new(),
        calls: vec![],
    };
    for block in blocks {
        let allowed: &[&str] = match string(block, "type")? {
            "text" => {
                result.text.push_str(string(block, "text")?);
                &["type", "text", "citations"]
            }
            "thinking" => {
                string(block, "thinking")?;
                nonempty(block, "signature")?;
                &["type", "thinking", "signature"]
            }
            "redacted_thinking" => {
                nonempty(block, "data")?;
                &["type", "data"]
            }
            "tool_use" => {
                if block
                    .get("caller")
                    .is_some_and(|v| !v.is_null() && v != &json!({"type":"direct"}))
                    || block.get("toolset_name").is_some_and(|v| !v.is_null())
                {
                    return Err(error(ErrorCode::CapabilityUnsupported, "tool_caller"));
                }
                let id = nonempty(block, "id")?;
                let name = nonempty(block, "name")?;
                let input = block
                    .get("input")
                    .filter(|v| v.is_object())
                    .ok_or_else(invalid)?;
                if result.calls.iter().any(|call| call.id == id) {
                    return Err(invalid());
                }
                result.calls.push(Call {
                    id: id.into(),
                    name: name.into(),
                    input: input.clone(),
                });
                &["type", "id", "name", "input", "caller", "toolset_name"]
            }
            _ => return Err(error(ErrorCode::CapabilityUnsupported, "content_type")),
        };
        if block
            .as_object()
            .ok_or_else(invalid)?
            .keys()
            .any(|key| !allowed.contains(&key.as_str()))
        {
            return Err(error(ErrorCode::CapabilityUnsupported, "content_fields"));
        }
    }
    Ok(result)
}
pub(crate) fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str, ContractError> {
    value.get(key).and_then(Value::as_str).ok_or_else(invalid)
}
pub(crate) fn nonempty<'a>(value: &'a Value, key: &str) -> Result<&'a str, ContractError> {
    let s = string(value, key)?;
    if s.is_empty() { Err(invalid()) } else { Ok(s) }
}
fn validate_output_schema(schema: &Value) -> Result<(), ContractError> {
    fn walk(schema: &Value) -> bool {
        let Some(object) = schema.as_object() else {
            return false;
        };
        if object.keys().any(|key| {
            matches!(
                key.as_str(),
                "minimum"
                    | "maximum"
                    | "exclusiveMinimum"
                    | "exclusiveMaximum"
                    | "multipleOf"
                    | "minLength"
                    | "maxLength"
                    | "pattern"
                    | "allOf"
                    | "not"
                    | "if"
                    | "then"
                    | "else"
            )
        }) {
            return false;
        }
        if (object.get("type") == Some(&json!("object")) || object.contains_key("properties"))
            && object.get("additionalProperties") != Some(&json!(false))
        {
            return false;
        }
        for key in ["properties", "$defs", "definitions"] {
            if let Some(map) = object.get(key) {
                if !map.as_object().is_some_and(|m| m.values().all(walk)) {
                    return false;
                }
            }
        }
        for key in ["items", "additionalProperties"] {
            if let Some(v) = object.get(key).filter(|v| v.is_object()) {
                if !walk(v) {
                    return false;
                }
            }
        }
        for key in ["anyOf", "oneOf", "prefixItems"] {
            if let Some(v) = object.get(key) {
                if !v.as_array().is_some_and(|a| a.iter().all(walk)) {
                    return false;
                }
            }
        }
        true
    }
    if !walk(schema) {
        return Err(error(ErrorCode::ModelCapabilityUnsupported, "json_schema"));
    }
    Ok(())
}
```

## `crates/wickle-model-anthropic/src/connection.rs`

```rust
use std::{fmt, sync::Arc, time::Duration};

use reqwest::{
    Client, Url,
    header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue},
};
use serde_json::json;
use wickle::*;

use crate::error;

/// Explicit transport settings. The library does not read environment files.
#[derive(Debug, Clone)]
pub struct AnthropicOptions {
    /// API base directory, normally `https://api.anthropic.com/`.
    pub base_url: String,
    /// Optional workspace header, required for a multi-workspace API key.
    pub workspace_id: Option<String>,
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

impl Default for AnthropicOptions {
    fn default() -> Self {
        Self {
            base_url: "https://api.anthropic.com/".into(),
            workspace_id: None,
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
pub struct AnthropicConnection(pub(crate) Arc<Connection>);

pub(crate) struct Connection {
    pub client: Client,
    pub base: Url,
    pub scope: Scope,
    pub binding: ModelPortBinding,
    pub target: JsonObject,
    pub options: AnthropicOptions,
}

impl fmt::Debug for AnthropicConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnthropicConnection")
            .field("binding", &self.0.binding)
            .finish_non_exhaustive()
    }
}

impl AnthropicConnection {
    /// Create a client without a network call. Never log the supplied API key.
    /// HTTPS is required except for explicitly configured loopback test servers.
    pub fn new(
        scope: Scope,
        connection_ref: VersionedRef,
        api_key: &str,
        mut options: AnthropicOptions,
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
        let mut authorization = HeaderValue::from_str(&format!("Bearer {api_key}"))
            .map_err(|_| error(ErrorCode::InvalidConfiguration, "credential"))?;
        authorization.set_sensitive(true);
        headers.insert(AUTHORIZATION, authorization);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let mut target = JsonObject::from([("base_url".into(), json!(base.as_str()))]);
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        if let Some(workspace) = &options.workspace_id {
            if workspace.is_empty() {
                return Err(error(ErrorCode::InvalidConfiguration, "workspace_id"));
            }
            headers.insert(
                "anthropic-workspace-id",
                HeaderValue::from_str(workspace)
                    .map_err(|_| error(ErrorCode::InvalidConfiguration, "workspace_id"))?,
            );
            target.insert("workspace_id".into(), json!(workspace));
        }
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
                provider: Id::new("anthropic")?,
                adapter: VersionedRef {
                    id: Id::new("wickle-model-anthropic")?,
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
    /// Canonical endpoint/workspace identity required in the selected route.
    pub fn target(&self) -> &JsonObject {
        &self.0.target
    }
    /// Owner namespace for this connection.
    pub fn scope(&self) -> &Scope {
        &self.0.scope
    }
    /// The one supported API operation and protocol generation.
    pub fn api_contract() -> ApiContract {
        ApiContract {
            operation: Id::new("messages").expect("static identifier"),
            version: Id::new("2023-06-01").expect("static identifier"),
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
            || route.deployment_revision.is_some()
        {
            return Err(error(ErrorCode::ModelBindingInvalid, "route"));
        }
        Ok(())
    }
}
```

## `crates/wickle-model-anthropic/src/inspection.rs`

```rust
use crate::{AnthropicConnection, error};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use wickle::*;

/// A provider-documented immutable model release, registered by the Host.
/// API availability alone, a date in a name, or a requested version is not proof.
#[derive(Debug, Clone)]
pub struct AnthropicSnapshot {
    /// Exact provider model identifier in the cited snapshot metadata.
    pub model_id: Id,
    /// Release identity established by that metadata.
    pub model_version: Id,
    /// Host-owned reference to the documentation or metadata establishing immutability.
    pub evidence_ref: Id,
}

/// Current account availability combined with explicit immutable-release evidence.
/// Unregistered model identifiers retain unknown release and unverified semantics.
#[derive(Clone)]
pub struct AnthropicInspector {
    connection: AnthropicConnection,
    snapshots: Arc<BTreeMap<Id, AnthropicSnapshot>>,
}
impl AnthropicInspector {
    /// Build an inspector without a network call. Duplicate identifiers are rejected.
    pub fn new(
        connection: AnthropicConnection,
        snapshots: Vec<AnthropicSnapshot>,
    ) -> Result<Self, ContractError> {
        let mut known = BTreeMap::new();
        for snapshot in snapshots {
            if known.insert(snapshot.model_id.clone(), snapshot).is_some() {
                return Err(error(ErrorCode::InvalidConfiguration, "snapshots"));
            }
        }
        Ok(Self {
            connection,
            snapshots: Arc::new(known),
        })
    }
}
impl ModelRouteInspector for AnthropicInspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        context: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            self.connection.validate(route, &context.scope)?;
            let mut url = self
                .connection
                .0
                .base
                .join("v1/models/")
                .map_err(|_| error(ErrorCode::InvalidConfiguration, "models_url"))?;
            url.path_segments_mut()
                .map_err(|_| error(ErrorCode::InvalidConfiguration, "models_url"))?
                .pop_if_empty()
                .push(route.model_id.as_str());
            let operation = async {
                let mut response = self
                    .connection
                    .0
                    .client
                    .get(url)
                    .send()
                    .await
                    .map_err(|_| error(ErrorCode::ModelInspectionUnavailable, "models_request"))?;
                if response.status().as_u16() == 404 {
                    return Ok(ModelRouteObservation {
                        route_digest: route.digest(),
                        availability: ModelRouteAvailability::Unavailable,
                        model_id: None,
                        model_version: None,
                        deployment_revision: None,
                        version_semantics: VersionSemantics::Unverified,
                        evidence_ref: Id::new("anthropic.models.retrieve")?,
                    });
                }
                if !response.status().is_success() {
                    return Err(error(
                        ErrorCode::ModelInspectionUnavailable,
                        "models_status",
                    ));
                }
                let mut bytes = vec![];
                while let Some(chunk) = response
                    .chunk()
                    .await
                    .map_err(|_| error(ErrorCode::ModelInspectionUnavailable, "models_body"))?
                {
                    if bytes.len().saturating_add(chunk.len())
                        > 65_536.min(self.connection.0.options.max_transport_bytes)
                    {
                        return Err(error(ErrorCode::ModelInspectionUnavailable, "models_limit"));
                    }
                    bytes.extend_from_slice(&chunk);
                }
                let body =
                    parse_json(std::str::from_utf8(&bytes).map_err(|_| {
                        error(ErrorCode::ModelInspectionUnavailable, "models_json")
                    })?)
                    .map_err(|_| error(ErrorCode::ModelInspectionUnavailable, "models_json"))?;
                if body.get("type").and_then(Value::as_str) != Some("model") {
                    return Err(error(
                        ErrorCode::ModelInspectionUnavailable,
                        "models_object",
                    ));
                }
                let model_id = body
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| error(ErrorCode::ModelInspectionUnavailable, "models_id"))?;
                if model_id != route.model_id.as_str() {
                    return Err(error(ErrorCode::ModelVersionDrift, "models_id"));
                }
                let snapshot = self.snapshots.get(&route.model_id);
                Ok(ModelRouteObservation {
                    route_digest: route.digest(),
                    availability: ModelRouteAvailability::Available,
                    model_id: Some(Id::new(model_id)?),
                    model_version: snapshot.map(|value| value.model_version.clone()),
                    deployment_revision: None,
                    version_semantics: if snapshot.is_some() {
                        VersionSemantics::Pinned
                    } else {
                        VersionSemantics::Unverified
                    },
                    evidence_ref: snapshot.map_or_else(
                        || Id::new("anthropic.models.retrieve"),
                        |value| Ok(value.evidence_ref.clone()),
                    )?,
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
```

## `crates/wickle-model-anthropic/src/lib.rs`

```rust
//! Anthropic Messages HTTP/SSE with Host-owned credentials and signed thinking replay.
#![forbid(unsafe_code)]
mod codec;
mod connection;
mod inspection;
mod model;
mod response;
pub use connection::{AnthropicConnection, AnthropicOptions};
pub use inspection::{AnthropicInspector, AnthropicSnapshot};
pub use model::AnthropicModel;
use wickle::{ContractError, ErrorCode};
fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("anthropic.{location}"))
}
```

## `crates/wickle-model-anthropic/src/model.rs`

```rust
use crate::{AnthropicConnection, error};
use crate::{codec::encode_request, response::Decoder as ResponsesDecoder};
use futures_util::stream;
use reqwest::Response;
use serde_json::Value;
use std::collections::VecDeque;
use wickle::*;
use wickle_model_responses::SseDecoder;

/// One Anthropic Messages POST per invocation, streamed into Wickle model events.
/// The adapter does not retry, run Tool handlers, or load environment variables.
#[derive(Debug, Clone)]
pub struct AnthropicModel {
    connection: AnthropicConnection,
}
impl AnthropicModel {
    /// Bind an already configured connection without making a network request.
    pub fn new(connection: AnthropicConnection) -> Self {
        Self { connection }
    }
}
impl ModelPort for AnthropicModel {
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
    connection: &'a AnthropicConnection,
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
            .join("v1/messages")
            .map_err(|_| error(ErrorCode::InvalidConfiguration, "messages_url"))?;
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
            .get("request-id")
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

## `crates/wickle-model-anthropic/src/response.rs`

```rust
use crate::{
    codec::{self, invalid, nonempty, string},
    error,
};
use serde_json::{Value, json};
use wickle::*;
use wickle_model_responses::SseEvent;

struct Block {
    value: Value,
    arguments: String,
    closed: bool,
}
pub(crate) struct Decoder<'a> {
    request: &'a ModelRequest,
    pub metadata: ModelResponseMetadata,
    message_id: Option<String>,
    blocks: Vec<Block>,
    stop: Option<String>,
    terminal: Option<ModelEvent>,
    bytes: usize,
    emitted: usize,
    tools: usize,
}
impl<'a> Decoder<'a> {
    pub fn new(request: &'a ModelRequest, request_id: Option<Id>) -> Self {
        Self {
            request,
            metadata: ModelResponseMetadata {
                provider_request_id: request_id,
                ..Default::default()
            },
            message_id: None,
            blocks: vec![],
            stop: None,
            terminal: None,
            bytes: 0,
            emitted: 0,
            tools: 0,
        }
    }
    pub fn event(&mut self, event: SseEvent) -> Result<Vec<ModelEvent>, ContractError> {
        if self.terminal.is_some() {
            return Err(invalid());
        }
        let value = parse_json(&event.data)?;
        let kind = nonempty(&value, "type")?;
        if event
            .name
            .as_ref()
            .is_some_and(|name| !name.is_empty() && name != "message" && name != kind)
        {
            return Err(invalid());
        }
        let mut output = vec![];
        match kind {
            "ping" => {}
            "message_start" => {
                if self.message_id.is_some() {
                    return Err(invalid());
                }
                let message = value.get("message").ok_or_else(invalid)?;
                if string(message, "type")? != "message"
                    || string(message, "role")? != "assistant"
                    || !message
                        .get("content")
                        .and_then(Value::as_array)
                        .is_some_and(Vec::is_empty)
                {
                    return Err(invalid());
                }
                self.message_id = Some(nonempty(message, "id")?.into());
                self.metadata.reported_model_id = Some(Id::new(nonempty(message, "model")?)?);
                self.usage(message.get("usage"))?;
            }
            "content_block_start" => {
                self.active()?;
                let index = index(&value)?;
                if index != self.blocks.len() || self.blocks.iter().any(|b| !b.closed) {
                    return Err(invalid());
                }
                let block = value.get("content_block").ok_or_else(invalid)?.clone();
                let mut arguments = String::new();
                match nonempty(&block, "type")? {
                    "text" => {
                        let text = string(&block, "text")?;
                        self.charge(text.len())?;
                        if !text.is_empty() {
                            for text in self.fragments(text)? {
                                output.push(ModelEvent::TextDelta { text });
                            }
                        }
                    }
                    "thinking" => {
                        self.charge(string(&block, "thinking")?.len())?;
                    }
                    "redacted_thinking" => {
                        self.charge(nonempty(&block, "data")?.len())?;
                    }
                    "tool_use" => {
                        self.tools += 1;
                        if self.tools > self.request.limits.max_tool_calls {
                            return Err(invalid());
                        }
                        let id = nonempty(&block, "id")?;
                        let name = nonempty(&block, "name")?;
                        if self
                            .blocks
                            .iter()
                            .any(|b| b.value.get("id") == Some(&json!(id)))
                        {
                            return Err(invalid());
                        }
                        let input = block
                            .get("input")
                            .and_then(Value::as_object)
                            .ok_or_else(invalid)?;
                        if !input.is_empty() {
                            arguments = serde_json::to_string(input).map_err(|_| invalid())?;
                        }
                        self.charge(id.len() + name.len() + arguments.len())?;
                        for (position, delta) in self.fragments(&arguments)?.into_iter().enumerate()
                        {
                            output.push(ModelEvent::ToolArgumentsDelta {
                                index: u32::try_from(index).map_err(|_| invalid())?,
                                provider_call_id: (position == 0).then(|| id.into()),
                                name: (position == 0).then(|| name.into()),
                                delta,
                            });
                        }
                    }
                    _ => return Err(error(ErrorCode::CapabilityUnsupported, "content_type")),
                }
                self.blocks.push(Block {
                    value: block,
                    arguments,
                    closed: false,
                });
            }
            "content_block_delta" => {
                self.active()?;
                let index = index(&value)?;
                let delta = value.get("delta").ok_or_else(invalid)?;
                let kind = nonempty(delta, "type")?;
                let (field, part_kind) = match kind {
                    "text_delta" => ("text", "text"),
                    "input_json_delta" => ("partial_json", "tool_use"),
                    "thinking_delta" => ("thinking", "thinking"),
                    "signature_delta" => ("signature", "thinking"),
                    _ => return Err(error(ErrorCode::CapabilityUnsupported, "delta_type")),
                };
                let fragment = string(delta, field)?;
                self.charge(fragment.len())?;
                let block = self.blocks.get_mut(index).ok_or_else(invalid)?;
                if block.closed || string(&block.value, "type")? != part_kind {
                    return Err(invalid());
                }
                if kind == "input_json_delta" {
                    block.arguments.push_str(fragment);
                } else {
                    let previous = block.value.get(field).and_then(Value::as_str).unwrap_or("");
                    block.value[field] = json!(format!("{previous}{fragment}"));
                }
                if kind == "text_delta" {
                    for text in self.fragments(fragment)? {
                        output.push(ModelEvent::TextDelta { text });
                    }
                }
                if kind == "input_json_delta" {
                    for delta in self.fragments(fragment)? {
                        output.push(ModelEvent::ToolArgumentsDelta {
                            index: u32::try_from(index).map_err(|_| invalid())?,
                            provider_call_id: None,
                            name: None,
                            delta,
                        });
                    }
                }
            }
            "content_block_stop" => {
                self.active()?;
                let index = index(&value)?;
                let block = self.blocks.get_mut(index).ok_or_else(invalid)?;
                if block.closed {
                    return Err(invalid());
                }
                block.closed = true;
                if block.value["type"] == "tool_use" && block.arguments.is_empty() {
                    block.arguments = "{}".into();
                    self.charge(2)?;
                    for delta in self.fragments("{}")? {
                        output.push(ModelEvent::ToolArgumentsDelta {
                            index: u32::try_from(index).map_err(|_| invalid())?,
                            provider_call_id: None,
                            name: None,
                            delta,
                        });
                    }
                }
            }
            "message_delta" => {
                if self.message_id.is_none() || self.blocks.iter().any(|b| !b.closed) {
                    return Err(invalid());
                }
                let delta = value.get("delta").ok_or_else(invalid)?;
                if let Some(stop) = delta.get("stop_reason").filter(|v| !v.is_null()) {
                    let stop = stop.as_str().ok_or_else(invalid)?;
                    if self.stop.as_deref().is_some_and(|old| old != stop) {
                        return Err(invalid());
                    }
                    self.stop = Some(stop.into());
                }
                self.usage(value.get("usage"))?;
            }
            "message_stop" => {
                if self.message_id.is_none() || self.blocks.iter().any(|b| !b.closed) {
                    return Err(invalid());
                }
                let stop = self.stop.as_deref().ok_or_else(invalid)?;
                let finish = match stop {
                    "end_turn" | "stop_sequence" if self.tools == 0 => Some(ModelFinish::Stop),
                    "tool_use" if self.tools > 0 => Some(ModelFinish::ToolCalls),
                    "max_tokens" => Some(ModelFinish::Length),
                    "refusal" => Some(ModelFinish::Refusal),
                    "pause_turn" => None,
                    "model_context_window_exceeded" => {
                        self.terminal = Some(ModelEvent::ResponseError {
                            kind: ModelFailureKind::ContextOverflow,
                            metadata: self.metadata.clone(),
                        });
                        return Ok(output);
                    }
                    _ => return Err(invalid()),
                };
                self.terminal = Some(if let Some(finish) = finish {
                    let mut continuation = vec![];
                    if matches!(finish, ModelFinish::Stop | ModelFinish::ToolCalls) {
                        for block in &mut self.blocks {
                            if block.value["type"] == "tool_use" {
                                block.value["input"] = parse_json(&block.arguments)?;
                            }
                        }
                        let blocks: Vec<_> = self.blocks.iter().map(|b| b.value.clone()).collect();
                        codec::inspect_blocks(&blocks)?;
                        if !blocks.is_empty() {
                            let data = json!({"kind":codec::REPLAY_KIND,"blocks":blocks});
                            self.charge(serde_json::to_vec(&data).map_err(|_| invalid())?.len())?;
                            continuation.push(OpaqueContinuation::new(&self.request.route, data));
                        }
                    }
                    ModelEvent::ResponseCompleted {
                        finish,
                        metadata: self.metadata.clone(),
                        continuation,
                    }
                } else {
                    ModelEvent::ResponseError {
                        kind: ModelFailureKind::Unsupported,
                        metadata: self.metadata.clone(),
                    }
                });
            }
            "error" => {
                let kind = match value.pointer("/error/type").and_then(Value::as_str) {
                    Some("authentication_error" | "permission_error") => {
                        ModelFailureKind::Authentication
                    }
                    Some("rate_limit_error") => ModelFailureKind::RateLimited,
                    Some("overloaded_error" | "api_error") => ModelFailureKind::Transport,
                    Some("not_found_error") => ModelFailureKind::Unavailable,
                    Some("invalid_request_error") => ModelFailureKind::Unsupported,
                    _ => ModelFailureKind::Protocol,
                };
                self.terminal = Some(ModelEvent::ResponseError {
                    kind,
                    metadata: self.metadata.clone(),
                });
            }
            _ => return Err(error(ErrorCode::CapabilityUnsupported, "stream_event")),
        }
        self.emitted = self
            .emitted
            .checked_add(output.len())
            .filter(|n| *n < self.request.limits.max_events)
            .ok_or_else(invalid)?;
        Ok(output)
    }
    pub fn finish(&mut self) -> Result<ModelEvent, ContractError> {
        self.terminal.take().ok_or_else(invalid)
    }
    fn active(&self) -> Result<(), ContractError> {
        if self.message_id.is_none() || self.stop.is_some() {
            Err(invalid())
        } else {
            Ok(())
        }
    }
    fn charge(&mut self, bytes: usize) -> Result<(), ContractError> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|n| *n <= self.request.limits.max_response_bytes)
            .ok_or_else(invalid)?;
        Ok(())
    }
    fn fragments(&self, text: &str) -> Result<Vec<String>, ContractError> {
        let max = self.request.limits.max_delta_bytes;
        if max == 0 {
            return Err(invalid());
        }
        let mut rest = text;
        let mut parts = vec![];
        while !rest.is_empty() {
            let mut end = rest.len().min(max);
            while !rest.is_char_boundary(end) {
                end -= 1;
            }
            if end == 0 {
                return Err(invalid());
            }
            parts.push(rest[..end].into());
            rest = &rest[end..];
            if parts.len() >= self.request.limits.max_events.saturating_sub(self.emitted) {
                return Err(invalid());
            }
        }
        if parts.is_empty() {
            parts.push(String::new());
        }
        Ok(parts)
    }
    fn usage(&mut self, value: Option<&Value>) -> Result<(), ContractError> {
        let Some(value) = value.filter(|v| !v.is_null()) else {
            return Ok(());
        };
        if !value.is_object() {
            return Err(invalid());
        }
        let usage = self.metadata.usage.get_or_insert(ModelUsage {
            measurement: UsageMeasurement::Reported,
            input_tokens: None,
            output_tokens: None,
        });
        for (field, slot) in [
            ("input_tokens", &mut usage.input_tokens),
            ("output_tokens", &mut usage.output_tokens),
        ] {
            if let Some(count) = value.get(field).filter(|v| !v.is_null()) {
                let count = count.as_u64().ok_or_else(invalid)?;
                if slot.is_some_and(|old| count < old) {
                    return Err(invalid());
                }
                *slot = Some(count);
            }
        }
        Ok(())
    }
}
fn index(value: &Value) -> Result<usize, ContractError> {
    value
        .get("index")
        .and_then(Value::as_u64)
        .and_then(|v| usize::try_from(v).ok())
        .ok_or_else(invalid)
}
```

## `crates/wickle-model-anthropic/tests/messages.rs`

```rust
//! Anthropic HTTP/SSE contracts, signed replay, cancellation and metadata.
mod support;
use futures_util::StreamExt;
use serde_json::{Value, json};
use support::*;
use wickle::*;
use wickle_model_anthropic::*;

#[tokio::test]
async fn thinking_before_text_and_cumulative_usage_are_not_misread_as_visible_output() {
    let mut data = events("claude-opus-5", "42");
    data.insert(2,json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"private reasoning"}}));
    let mut reply = Reply::sse(&data);
    reply.headers = vec![("request-id", "req-claude".into())];
    let server = Server::new(vec![reply]).await;
    let connection = AnthropicConnection::new(
        scope(),
        reference("account"),
        "fixture-key",
        AnthropicOptions {
            base_url: server.base.trim_end_matches("v1/").into(),
            workspace_id: Some("wrkspc_fixture".into()),
            ..Default::default()
        },
    )
    .unwrap();
    let request = request(&connection, "claude-opus-5");
    let model = AnthropicModel::new(connection);
    let response = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(response.text, "42");
    assert_eq!(response.finish, ModelFinish::Stop);
    assert_eq!(
        response.metadata.provider_request_id,
        Some(id("req-claude"))
    );
    let usage = response.metadata.usage.unwrap();
    assert_eq!(usage.input_tokens, Some(25));
    assert_eq!(usage.output_tokens, Some(15));
    assert!(response.metadata.reported_model_version.is_none());
    assert_eq!(
        response.continuation[0].data()["blocks"][0]["thinking"],
        "private reasoning"
    );
    assert_eq!(
        response.continuation[0].data()["blocks"][0]["signature"],
        "signature-fixture"
    );
    let calls = server.requests.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let call = &calls[0];
    assert_eq!(call.method, "POST");
    assert_eq!(call.path, "/v1/messages");
    let headers = call.headers.to_ascii_lowercase();
    assert!(headers.contains("authorization: bearer fixture-key"));
    assert!(headers.contains("anthropic-version: 2023-06-01"));
    assert!(headers.contains("anthropic-workspace-id: wrkspc_fixture"));
    assert_eq!(call.body["model"], "claude-opus-5");
    assert_eq!(call.body["output_config"]["effort"], "medium");
    assert!(call.body.get("thinking").is_none());
    assert!(call.body.get("fallbacks").is_none());
    assert!(!call.body.to_string().contains("hidden-workspace"));
    assert!(!call.body.to_string().contains("fixture-key"));
}

fn tool_events() -> Vec<Value> {
    let mut data = events("claude-opus-5", "");
    data.truncate(4);
    data.extend([
        json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"lookup","input":{},"caller":{"type":"direct"}}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"query\":"}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"\"alpha\"}"}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":20}}),
        json!({"type":"message_stop"}),
    ]);
    data
}
fn tool(request: &mut ModelRequest) {
    request.tools = vec![ModelTool {
        name: id("lookup"),
        description: "Read a record".into(),
        model_input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer"}},"required":["query"],"additionalProperties":false}),
    }];
}
#[tokio::test]
async fn signed_empty_thinking_and_tool_inputs_are_replayed_once_with_results() {
    let server = Server::new(vec![
        Reply::sse(&tool_events()),
        Reply::sse(&events("claude-opus-5", "{\"answer\":42}")),
    ])
    .await;
    let connection = connection(&server);
    let model = AnthropicModel::new(connection.clone());
    let mut request = request(&connection, "claude-opus-5");
    tool(&mut request);
    let response = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(response.tool_calls.len(), 1);
    assert_eq!(response.tool_calls[0].model_inputs["query"], "alpha");
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![
            ModelContent::ToolCall {
                provider_call_id: id("toolu_1"),
                name: id("lookup"),
                arguments: JsonObject::from([("query".into(), json!("alpha"))]),
            },
            ModelContent::Opaque {
                continuation: response.continuation[0].clone(),
            },
        ],
    });
    request.messages.push(ModelMessage {
        role: ModelRole::Tool,
        content: vec![ModelContent::ToolResult {
            provider_call_id: id("toolu_1"),
            content: json!({"answer":42}),
        }],
    });
    request.output = ModelOutput::JsonSchema {
        schema: json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}),
    };
    request.request_id = id("next-attempt");
    let response = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(parse_json(&response.text).unwrap(), json!({"answer":42}));
    {
        let calls = server.requests.lock().unwrap();
        let body = &calls[1].body;
        assert_eq!(body["messages"][1]["content"].as_array().unwrap().len(), 2);
        assert_eq!(
            body["messages"][1]["content"][0],
            json!({"type":"thinking","thinking":"","signature":"signature-fixture"})
        );
        assert_eq!(body["messages"][1]["content"][1]["type"], "tool_use");
        assert_eq!(
            body["messages"][1]["content"][1]["caller"],
            json!({"type":"direct"})
        );
        assert_eq!(body["messages"][2]["role"], "user");
        assert_eq!(body["messages"][2]["content"][0]["tool_use_id"], "toolu_1");
        assert_eq!(body["output_config"]["format"]["type"], "json_schema");
        assert_eq!(
            body["tools"][0]["input_schema"]["required"],
            json!(["query"])
        );
    }
    if let ModelContent::ToolCall { arguments, .. } = &mut request.messages[1].content[0] {
        arguments.insert("query".into(), json!("tampered"));
    }
    assert!(
        collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .is_err()
    );
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn invalid_scope_options_and_model_thinking_combinations_stop_before_http() {
    for case in [
        "scope", "target", "attempt", "binding", "api", "option", "effort", "manual", "disabled",
        "prefill", "schema",
    ] {
        let server = Server::new(vec![]).await;
        let connection = connection(&server);
        let model = AnthropicModel::new(connection.clone());
        let mut request = request(&connection, "claude-opus-5");
        let mut context = context(&request);
        match case {
            "scope" => context.scope.workspace_id = id("other"),
            "attempt" => context.attempt_id = id("other-attempt"),
            "binding" => request.route.connection_ref = reference("other-account"),
            "api" => request.route.api_contract.version = id("unsupported"),
            "target" => {
                request
                    .route
                    .target
                    .insert("base_url".into(), json!("https://wrong.example/"));
            }
            "option" => {
                request.options.insert("fallbacks".into(), json!("default"));
            }
            "effort" => {
                request.options.insert("effort".into(), json!("adaptive"));
            }
            "manual" => {
                request.max_output_tokens = 4096.try_into().unwrap();
                request
                    .options
                    .insert("thinking_mode".into(), json!("enabled"));
                request
                    .options
                    .insert("thinking_budget_tokens".into(), json!(1024));
            }
            "disabled" => {
                request
                    .options
                    .insert("thinking_mode".into(), json!("disabled"));
                request.options.insert("effort".into(), json!("max"));
            }
            "prefill" => request.messages.push(ModelMessage {
                role: ModelRole::Assistant,
                content: vec![ModelContent::Text {
                    text: "prefix".into(),
                }],
            }),
            "schema" => {
                request.output = ModelOutput::JsonSchema {
                    schema: json!({"type":"object","properties":{"n":{"type":"integer","minimum":1}},"additionalProperties":false}),
                }
            }
            _ => unreachable!(),
        }
        assert!(
            collect_model_response(&request, model.generate(&request, &context))
                .await
                .is_err(),
            "{case}"
        );
        assert!(server.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn truncated_unsigned_misordered_and_conflicting_streams_are_not_successes() {
    for case in [
        "truncated",
        "unsigned",
        "duplicate-start",
        "wrong-index",
        "open-block",
        "usage",
        "double-stop",
        "native-tool",
        "partial-json",
    ] {
        let mut data = if case == "partial-json" {
            tool_events()
        } else {
            events("claude-opus-5", "answer")
        };
        match case {
            "truncated" => {
                data.pop();
            }
            "unsigned" => {
                data.remove(2);
            }
            "duplicate-start" => data.insert(1, data[0].clone()),
            "wrong-index" => data[5]["index"] = json!(0),
            "open-block" => {
                data.remove(6);
            }
            "usage" => data[7]["usage"]["output_tokens"] = json!(0),
            "double-stop" => data.push(json!({"type":"message_stop"})),
            "native-tool" => data[4]["content_block"]["type"] = json!("server_tool_use"),
            "partial-json" => data[6]["delta"]["partial_json"] = json!("\"alpha\""),
            _ => unreachable!(),
        }
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let model = AnthropicModel::new(connection.clone());
        let mut request = request(&connection, "claude-opus-5");
        tool(&mut request);
        assert!(
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .is_err(),
            "{case}"
        );
    }
}

#[tokio::test]
async fn model_metadata_uses_anthropic_type_and_registered_snapshot_facts() {
    let server = Server::new(vec![
        Reply::json(
            200,
            json!({"id":"claude-opus-5","type":"model","display_name":"Claude Opus 5"}),
        ),
        Reply::json(200, json!({"id":"claude-opus-5","type":"model"})),
    ])
    .await;
    let connection = connection(&server);
    let request = request(&connection, "claude-opus-5");
    let context = ModelInspectionContext {
        scope: scope(),
        principal_ref: id("user"),
        capability_grant_ref: id("grant"),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(3),
    };
    let unknown = AnthropicInspector::new(connection.clone(), vec![])
        .unwrap()
        .inspect(&request.route, &context)
        .await
        .unwrap();
    assert!(unknown.model_version.is_none());
    assert_eq!(unknown.version_semantics, VersionSemantics::Unverified);
    let known = AnthropicInspector::new(
        connection,
        vec![AnthropicSnapshot {
            model_id: id("claude-opus-5"),
            model_version: id("release"),
            evidence_ref: id("documented-release"),
        }],
    )
    .unwrap()
    .inspect(&request.route, &context)
    .await
    .unwrap();
    known
        .validate(&request.route, VersionPolicy::RequirePinned)
        .unwrap();
    for call in server.requests.lock().unwrap().iter() {
        assert_eq!(call.method, "GET");
        assert_eq!(call.path, "/v1/models/claude-opus-5");
    }
}

#[tokio::test]
async fn cancellation_and_deadline_close_streams_without_retry() {
    for cancel in [true, false] {
        let mut data = events("claude-opus-5", "partial");
        data.truncate(6);
        let mut reply = Reply::sse(&data);
        reply.stall = true;
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let model = AnthropicModel::new(connection.clone());
        let request = request(&connection, "claude-opus-5");
        let mut context = context(&request);
        context.deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(300);
        let mut stream = model.generate(&request, &context);
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ModelEvent::TextDelta { .. }
        ));
        server.entered.notified().await;
        if cancel {
            context.cancellation.cancel();
        }
        let next = stream.next().await.unwrap();
        if cancel {
            assert_eq!(next.unwrap_err().code, ErrorCode::Cancelled);
        } else {
            assert!(matches!(
                next.unwrap(),
                ModelEvent::ResponseError {
                    kind: ModelFailureKind::Timeout,
                    ..
                }
            ));
        }
        assert!(stream.next().await.is_none());
        tokio::time::timeout(std::time::Duration::from_secs(1), server.closed.notified())
            .await
            .unwrap();
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn redacted_thinking_limits_and_error_modes_preserve_safe_completion() {
    for case in [
        "redacted",
        "length",
        "refusal",
        "pause",
        "limit",
        "native-caller",
    ] {
        let mut data = if case == "native-caller" {
            tool_events()
        } else {
            events("claude-opus-5", "answer")
        };
        match case {
            "redacted" => {
                data[1]["content_block"] =
                    json!({"type":"redacted_thinking","data":"opaque-redacted"});
                data.remove(2);
            }
            "length" => data[7]["delta"]["stop_reason"] = json!("max_tokens"),
            "refusal" => data[7]["delta"]["stop_reason"] = json!("refusal"),
            "pause" => data[7]["delta"]["stop_reason"] = json!("pause_turn"),
            "native-caller" => {
                data[4]["content_block"]["caller"] =
                    json!({"type":"code_execution_20260120","tool_id":"srvtoolu_1"})
            }
            _ => {}
        }
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let model = AnthropicModel::new(connection.clone());
        let mut request = request(&connection, "claude-opus-5");
        tool(&mut request);
        if case == "limit" {
            request.limits.max_response_bytes = 8;
        }
        let events: Vec<_> = model.generate(&request, &context(&request)).collect().await;
        let terminal = events.last().unwrap().as_ref().unwrap();
        match case {
            "redacted" => {
                let ModelEvent::ResponseCompleted { continuation, .. } = terminal else {
                    panic!("no redacted completion")
                };
                assert_eq!(
                    continuation[0].data()["blocks"][0],
                    json!({"type":"redacted_thinking","data":"opaque-redacted"})
                );
            }
            "length" => assert!(
                matches!(terminal,ModelEvent::ResponseCompleted{finish:ModelFinish::Length,continuation,..} if continuation.is_empty())
            ),
            "refusal" => assert!(
                matches!(terminal,ModelEvent::ResponseCompleted{finish:ModelFinish::Refusal,continuation,..} if continuation.is_empty())
            ),
            _ => assert!(
                matches!(terminal, ModelEvent::ResponseError { .. }),
                "{case}"
            ),
        }
    }
}

#[tokio::test]
async fn provider_http_and_stream_errors_are_not_retried_or_leaked() {
    let redirect = Server::new(vec![]).await;
    for (status, expected) in [
        (401, ModelFailureKind::Authentication),
        (429, ModelFailureKind::RateLimited),
        (529, ModelFailureKind::Transport),
        (307, ModelFailureKind::Unsupported),
        (200, ModelFailureKind::Transport),
    ] {
        let mut reply = if status == 200 {
            Reply::sse(&[
                json!({"type":"error","error":{"type":"overloaded_error","message":"provider private detail"}}),
            ])
        } else {
            Reply::json(
                status,
                json!({"error":{"type":"api_error","message":"provider private detail"}}),
            )
        };
        if status == 307 {
            reply.headers.push(("location", redirect.base.clone()));
        }
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let request = request(&connection, "claude-opus-5");
        let model = AnthropicModel::new(connection);
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, expected);
        assert!(!format!("{failure:?}").contains("provider private detail"));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
    assert!(redirect.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn unicode_fragments_reserve_exactly_one_terminal_event() {
    for (delta_limit, event_limit, valid) in [(3, 3, true), (3, 2, false), (2, 3, false)] {
        let server = Server::new(vec![Reply::sse(&events("claude-opus-5", "한글"))]).await;
        let connection = connection(&server);
        let model = AnthropicModel::new(connection.clone());
        let mut request = request(&connection, "claude-opus-5");
        request.limits.max_delta_bytes = delta_limit;
        request.limits.max_events = event_limit;
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        if valid {
            assert_eq!(result.unwrap().text, "한글");
        } else {
            assert!(result.is_err());
        }
    }
}

#[tokio::test]
async fn inspection_missing_drift_cancellation_and_size_limits_are_reported() {
    for case in ["missing", "drift", "large", "cancel"] {
        let mut body =
            json!({"type":"model","id":if case=="drift"{"other-model"}else{"claude-opus-5"}});
        if case == "large" {
            body["unused"] = json!("x".repeat(70000));
        }
        let mut reply = Reply::json(if case == "missing" { 404 } else { 200 }, body);
        reply.stall = case == "cancel";
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let request = request(&connection, "claude-opus-5");
        let inspector = AnthropicInspector::new(connection, vec![]).unwrap();
        let context = ModelInspectionContext {
            scope: scope(),
            principal_ref: id("user"),
            capability_grant_ref: id("grant"),
            cancellation: Default::default(),
            deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(3),
        };
        let mut pending = Box::pin(inspector.inspect(&request.route, &context));
        if case == "cancel" {
            tokio::select! { _=server.entered.notified()=>{}, result=&mut pending=>panic!("inspection completed early: {result:?}") }
            context.cancellation.cancel();
        }
        let result = pending.await;
        match case {
            "missing" => {
                let observation = result.unwrap();
                assert_eq!(
                    observation.availability,
                    ModelRouteAvailability::Unavailable
                );
                assert!(observation.model_version.is_none());
            }
            "drift" => assert_eq!(result.unwrap_err().code, ErrorCode::ModelVersionDrift),
            "cancel" => {
                assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
                tokio::time::timeout(std::time::Duration::from_secs(1), server.closed.notified())
                    .await
                    .unwrap();
            }
            _ => assert_eq!(
                result.unwrap_err().code,
                ErrorCode::ModelInspectionUnavailable
            ),
        }
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}
```

## `crates/wickle-model-anthropic/tests/support/mod.rs`

```rust
use serde_json::{Value, json};
use wickle::*;
use wickle_model_anthropic::*;

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
pub fn connection(server: &Server) -> AnthropicConnection {
    AnthropicConnection::new(
        scope(),
        reference("account"),
        "fixture-key-not-a-secret",
        AnthropicOptions {
            base_url: server.base.trim_end_matches("v1/").into(),
            ..Default::default()
        },
    )
    .unwrap()
}
pub fn request(connection: &AnthropicConnection, model: &str) -> ModelRequest {
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
            api_contract: AnthropicConnection::api_contract(),
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
        options: JsonObject::from([("effort".into(), json!("medium"))]),
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
pub fn events(model: &str, text: &str) -> Vec<Value> {
    vec![
        json!({"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":model,"content":[],"stop_reason":null,"usage":{"input_tokens":25,"output_tokens":1}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signature-fixture"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":text}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":15}}),
        json!({"type":"message_stop"}),
    ]
}

#[path = "../../../../tests/support/model_http.rs"]
mod http;
pub use http::*;
```

## `tests/support/anthropic_consumer.rs`

```rust
// Real loopback HTTP/SSE against the extracted Anthropic adapter package; no provider call.
use serde_json::json;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use wickle::*;
use wickle_model_anthropic::*;
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
        assert!(headers.starts_with("post /v1/messages "));
        assert!(headers.contains("anthropic-workspace-id: wrkspc_fixture"));
        let body =
            parse_json(std::str::from_utf8(&bytes[header_end..header_end + length]).unwrap())
                .unwrap();
        assert_eq!(body["model"], "fixture-model");
        assert_eq!(body["stream"], true);
        assert!(body.get("fallbacks").is_none());
        assert_eq!(body["output_config"]["effort"], "medium");
        assert_eq!(body["output_config"]["format"]["type"], "json_schema");
        assert!(!body.to_string().contains("private-workspace"));
        assert!(!body.to_string().contains("fixture-key"));
        let events = vec![
            json!({"type":"message_start","message":{"id":"message","type":"message","role":"assistant","model":"fixture-model","content":[],"usage":{"input_tokens":12,"output_tokens":1}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signed-fixture"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"{\"answer\":42}"}}),
            json!({"type":"content_block_stop","index":1}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}),
            json!({"type":"message_stop"}),
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
    let connection = AnthropicConnection::new(
        scope.clone(),
        reference("account"),
        "fixture-key",
        AnthropicOptions {
            base_url: base,
            workspace_id: Some("wrkspc_fixture".into()),
            ..Default::default()
        },
    )?;
    let binding = connection.binding();
    let model = AnthropicModel::new(connection.clone());
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
            api_contract: AnthropicConnection::api_contract(),
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
        options: JsonObject::from([("effort".into(), json!("medium"))]),
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
    assert_eq!(
        response.continuation[0].data()["blocks"][0]["signature"],
        "signed-fixture"
    );
    server.await?;
    println!(
        "Anthropic consumer: extracted adapter performs one HTTP/SSE request, preserves workspace identity, native output options, signed thinking and cumulative usage, decodes JSON and reported usage, and excludes Host context from the wire (local fixture, no provider network)"
    );
    Ok(())
}
```

## `tests/support/recovery_consumer.rs`

```rust
// Independent CLI consumer: real SQLite and separate Host processes, synthetic model.
#[allow(dead_code)]
mod host {
    include!("agent_consumer.rs");

    // Crash-boundary verification advances lease time explicitly. A slow disk or
    // a descheduled process must not expire the lease before the intended exit.
    struct RecoveryClock(i64);
    impl Clock for RecoveryClock {
        fn now(&self) -> Result<ClockReading, ContractError> {
            Ok(ClockReading { utc_ms: self.0, monotonic_ms: 1000 })
        }
        fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
            Box::pin(std::future::pending())
        }
    }
    fn worker(directory: &std::path::Path, mode: &str) -> Result<std::process::ExitStatus, Box<dyn std::error::Error>> {
        let mut child = std::process::Command::new(std::env::current_exe()?).arg(directory).arg(mode).spawn()?;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = child.try_wait()? { return Ok(status); }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill(); let _ = child.wait();
                return Err("recovery worker exceeded its wall-time watchdog".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    struct ProcessModel {
        inner: ExampleModel,
        directory: std::path::PathBuf,
        interrupt: bool,
    }
    impl ModelPort for ProcessModel {
        fn binding(&self) -> ModelPortBinding { self.inner.binding() }
        fn generate<'a>(&'a self, request: &'a ModelRequest, context: &'a ModelCallContext) -> PortStream<'a, ModelEvent> {
            use std::io::Write;
            let mut calls = std::fs::OpenOptions::new().create(true).append(true).open(self.directory.join("calls")).unwrap();
            writeln!(calls, "{}", context.attempt_id).unwrap();
            calls.sync_all().unwrap();
            std::fs::write(self.directory.join("run"), context.run_id.as_str()).unwrap();
            if self.interrupt { std::process::exit(73); }
            self.inner.generate(request, context)
        }
    }
    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        let args: Vec<_> = std::env::args_os().collect();
        if args.len() == 3 {
            return child(std::path::Path::new(&args[1]), args[2] == "interrupt").await;
        }
        let directory = TemporaryDatabase(std::env::temp_dir().join(format!("wickle-recovery-{}", RandomIdSource.next_id()?)));
        std::fs::create_dir(&directory.0)?;
        let first = worker(&directory.0, "interrupt")?;
        assert_eq!(first.code(), Some(73));
        let second = worker(&directory.0, "recover")?;
        assert!(second.success());
        let calls = std::fs::read_to_string(directory.0.join("calls"))?;
        let attempts: Vec<_> = calls.lines().collect();
        assert_eq!(attempts.len(), 2);
        assert_ne!(attempts[0], attempts[1]);
        println!("recovery consumer: abrupt Host exit, SQLite reopen under a new owner, original model step retained, two charged physical attempts, accepted command replay without another call (synthetic model, no provider network)");
        Ok(())
    }
    async fn child(directory: &std::path::Path, interrupt: bool) -> Result<(), Box<dyn std::error::Error>> {
        let scope = Scope { tenant_id: id("tenant"), workspace_id: id("workspace"), user_id: None };
        let store = Arc::new(SqliteStateStore::open(directory.join("state.sqlite"))?);
        let routing = routing_snapshot(&scope)?;
        let model = Arc::new(ProcessModel { inner: ExampleModel { route: routing.route_for_binding(&reference("first"))?, calls: AtomicUsize::new(0), fail: false }, directory: directory.to_owned(), interrupt });
        let policy = Arc::new(PolicyGate::new(Arc::new(ExamplePolicy), Duration::from_secs(1))?);
        let profile = AgentProfile::from_json(r#"{
            "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
            "name":"Assistant","description":"Recovery consumer","instructions":{"text":"Use supplied information"},
            "model_binding":"primary","tools":[],"skills":[],"connectors":[],
            "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
            "limits":{"max_model_calls":3,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":2,"max_elapsed_ms":60000}
        }"#)?;
        let mut settings = AgentSettings { require_durable: true, max_output_tokens: 128.try_into()?, ..AgentSettings::default() };
        if interrupt { settings.lease_ttl_ms = 1000; settings.heartbeat_interval_ms = 100; }
        let agent = create_agent(profile, AgentBindings {
            scope: scope.clone(), state: store.clone(), policy: policy.clone(), profile_resolver: Arc::new(Catalog),
            model_exchange: Arc::new(ModelExchange::new(model, policy).with_route_inspector(Arc::new(ExampleInspector), Duration::from_secs(1))?),
            router: Arc::new(PolicyModelRouter::new(routing)?), host_instructions: vec!["Preserve the requested output.".into()],
            system_inputs: SystemInputRegistry::new(vec![])?, clock: Arc::new(RecoveryClock(if interrupt { 1000 } else { 3000 })), ids: Arc::new(RandomIdSource),
            tools: None, system_input_resolver: None, external_receipt_verifier: None, components: None,
            context_sources: None, context_token_estimator: None, context_runtime: None, verification: None, skills: None, artifacts: None, hooks: None,
            token_estimator: Arc::new(Estimate), settings,
        })?;
        let context = ExecutionContext::new(ExecutionContextData { scope: scope.clone(), principal_ref: id("actor"), capability_grant_ref: id("grant"), trace_context: None, system_inputs: None }, Default::default());
        if interrupt {
            let request = RunRequest { request_id: id("request"), session_id: id("session"), input: vec![InputContent::Text { text: "Retrieve the available result".into() }], trigger: RunTrigger::User {}, model_options: JsonObject::from([("reasoning_effort".into(), json!("high"))]), output_contract: None };
            let handle = completed(agent.start(request, context.clone()).await?)?;
            // Regression: wall-time delay exceeds the fixture's short lease.
            std::thread::sleep(Duration::from_millis(1200));
            let outcome = handle.outcome(&context).await;
            return Err(format!("interruption did not occur: {outcome:?}").into());
        }
        let run_id = id(&std::fs::read_to_string(directory.join("run"))?);
        assert_eq!(store.acquire_lease(&scope, &run_id, &id("too-early"), 1000, 1000).await.unwrap_err().code, ErrorCode::LeaseBusy);
        let before = completed(agent.get_run_details(&run_id, &context).await?)?;
        let source = before.recovery_record(RandomIdSource.next_id()?)?;
        let command = ResumeCommand { run_id: run_id.clone(), expected_revision: before.revision, command_id: RandomIdSource.next_id()?, action: ResumeAction::Recover { recovery_ref: source.reference().clone() } };
        let handle = completed(agent.resume(command.clone(), context.clone()).await?)?;
        let result = completed(handle.outcome(&context).await?)?;
        assert_eq!(result.result.status(), RunStatus::Succeeded);
        assert_eq!(result.usage.model_calls, 2);
        assert_eq!(result.usage.recovery_attempts, 1);
        let saved = store.load(&scope, &run_id).await?;
        assert!(matches!(saved.snapshot.model_ledger[0].state, ModelAttemptState::Interrupted { .. }));
        assert_eq!(saved.snapshot.model_ledger[0].model_step_id, saved.snapshot.model_ledger[1].model_step_id);
        let replay = completed(agent.resume(command, context.clone()).await?)?;
        assert_eq!(completed(replay.outcome(&context).await?)?, result);
        Ok(())
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> { host::run().await }
```
