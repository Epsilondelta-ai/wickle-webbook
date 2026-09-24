# 25장 전체 Rust 구현과 테스트

[강의로](../25-openai.md) · [전체 변경 패치](../solutions/25-openai.patch)

기준 `fb3624adcc52fccd6cb57128cdbfa6a7806f1761`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle-model-openai/src/codec.rs`

```rust
use serde_json::{Value, json};
use wickle::*;

pub(crate) const REPLAY_KIND: &str = "wickle.openai.responses.v1";
fn failure(code: ErrorCode) -> ContractError {
    crate::error(code, "codec")
}

pub(crate) fn encode_request(request: &ModelRequest) -> Result<Value, ContractError> {
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

## `crates/wickle-model-openai/src/connection.rs`

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
pub struct OpenAiOptions {
    /// API base directory, normally `https://api.openai.com/v1/`.
    pub base_url: String,
    /// Optional OpenAI organization header, also included in route identity.
    pub organization_id: Option<String>,
    /// Optional OpenAI project header, also included in route identity.
    pub project_id: Option<String>,
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

impl Default for OpenAiOptions {
    fn default() -> Self {
        Self {
            base_url: "https://api.openai.com/v1/".into(),
            organization_id: None,
            project_id: None,
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
pub struct OpenAiConnection(pub(crate) Arc<Connection>);

pub(crate) struct Connection {
    pub client: Client,
    pub base: Url,
    pub scope: Scope,
    pub binding: ModelPortBinding,
    pub target: JsonObject,
    pub options: OpenAiOptions,
}

impl fmt::Debug for OpenAiConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiConnection")
            .field("binding", &self.0.binding)
            .finish_non_exhaustive()
    }
}

impl OpenAiConnection {
    /// Create a client without a network call. Never log the supplied API key.
    /// HTTPS is required except for explicitly configured loopback test servers.
    pub fn new(
        scope: Scope,
        connection_ref: VersionedRef,
        api_key: &str,
        mut options: OpenAiOptions,
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
        for (value, header, field) in [
            (
                &options.organization_id,
                "openai-organization",
                "organization_id",
            ),
            (&options.project_id, "openai-project", "project_id"),
        ] {
            if let Some(value) = value {
                if value.is_empty() {
                    return Err(error(ErrorCode::InvalidConfiguration, field));
                }
                headers.insert(
                    header,
                    HeaderValue::from_str(value)
                        .map_err(|_| error(ErrorCode::InvalidConfiguration, field))?,
                );
                target.insert(field.into(), json!(value));
            }
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
                provider: Id::new("openai")?,
                adapter: VersionedRef {
                    id: Id::new("wickle-model-openai")?,
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
    /// Canonical endpoint/project identity required in the selected route.
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
            || route.deployment_revision.is_some()
        {
            return Err(error(ErrorCode::ModelBindingInvalid, "route"));
        }
        Ok(())
    }
}
```

## `crates/wickle-model-openai/src/inspection.rs`

```rust
use crate::{OpenAiConnection, error};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use wickle::*;

/// A provider-documented immutable model release, registered by the Host.
/// API availability alone, a date in a name, or a requested version is not proof.
#[derive(Debug, Clone)]
pub struct OpenAiSnapshot {
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
pub struct OpenAiInspector {
    connection: OpenAiConnection,
    snapshots: Arc<BTreeMap<Id, OpenAiSnapshot>>,
}
impl OpenAiInspector {
    /// Build an inspector without a network call. Duplicate identifiers are rejected.
    pub fn new(
        connection: OpenAiConnection,
        snapshots: Vec<OpenAiSnapshot>,
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
impl ModelRouteInspector for OpenAiInspector {
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
                .join("models/")
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
                        evidence_ref: Id::new("openai.models.retrieve")?,
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
                if body.get("object").and_then(Value::as_str) != Some("model") {
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
                        || Id::new("openai.models.retrieve"),
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

## `crates/wickle-model-openai/src/lib.rs`

```rust
//! OpenAI Responses over bounded HTTP/SSE, with Host-supplied credentials.
//!
//! A model stream represents exactly one POST. SDK retries, redirects, native
//! provider tools, conversation storage, and automatic truncation are disabled.

#![forbid(unsafe_code)]

mod codec;
mod connection;
mod inspection;
mod model;
mod response;
mod sse;

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
use crate::{OpenAiConnection, codec, error, response, sse};
use futures_util::stream;
use reqwest::Response;
use serde_json::Value;
use std::collections::VecDeque;
use wickle::*;

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
            decoder: response::Decoder::new(request, None),
            framing: sse::Decoder::new(
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
    decoder: response::Decoder<'a>,
    framing: sse::Decoder,
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
        let value = codec::encode_request(self.request)?;
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

## `crates/wickle-model-openai/src/response.rs`

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
pub(crate) struct Decoder<'a> {
    request: &'a ModelRequest,
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

## `crates/wickle-model-openai/src/sse.rs`

```rust
use crate::error;
use wickle::{ContractError, ErrorCode};

pub(crate) struct Event {
    pub name: Option<String>,
    pub data: String,
}

/// Incremental SSE framing; JSON and UTF-8 may be split across network chunks.
pub(crate) struct Decoder {
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

## `crates/wickle-model-openai/tests/responses.rs`

```rust
//! Real HTTP/SSE boundaries, scoped credentials, version evidence, and lossless replay.
#[allow(dead_code)]
mod support;
use futures_util::StreamExt;
use serde_json::{Value, json};
use support::*;
use wickle::*;
use wickle_model_openai::*;

#[tokio::test]
async fn streaming_versions_options_and_reported_usage_do_not_leak_host_context() {
    let server = Server::new(vec![
        Reply::sse(&events("model-first", "안녕")),
        Reply::sse(&events("model-second", "second")),
    ])
    .await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    for (name, effort, text) in [
        ("model-first", "low", "안녕"),
        ("model-second", "high", "second"),
    ] {
        let mut request = request(&connection, name);
        request.route.model_version = id(name);
        request
            .options
            .insert("reasoning_effort".into(), json!(effort));
        let context = context(&request);
        let result = collect_model_response(&request, model.generate(&request, &context))
            .await
            .unwrap();
        assert_eq!(result.text, text);
        assert_eq!(result.finish, ModelFinish::Stop);
        assert_eq!(result.metadata.reported_model_id, Some(id(name)));
        assert!(result.metadata.reported_model_version.is_none());
        assert_eq!(
            result.metadata.usage,
            Some(ModelUsage {
                measurement: UsageMeasurement::Reported,
                input_tokens: Some(11),
                output_tokens: Some(7)
            })
        );
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (index, effort) in ["low", "high"].into_iter().enumerate() {
        assert_eq!(requests[index].path, "/v1/responses");
        assert_eq!(requests[index].method, "POST");
        assert_eq!(requests[index].body["reasoning"]["effort"], effort);
        assert_eq!(requests[index].body["stream"], true);
        assert_eq!(requests[index].body["store"], false);
        assert_eq!(requests[index].body["truncation"], "disabled");
        let encoded = requests[index].body.to_string();
        assert!(!encoded.contains("hidden-workspace"));
        assert!(!encoded.contains("fixture-key-not-a-secret"));
        assert!(
            requests[index]
                .headers
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-key-not-a-secret")
        );
    }
    assert!(!format!("{connection:?} {model:?}").contains("fixture-key-not-a-secret"));
}

#[tokio::test]
async fn structured_output_preserves_schema_and_rejects_unsupported_contracts_before_http() {
    let schema = json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false});
    let server = Server::new(vec![Reply::sse(&events("model", r#"{"answer":42}"#))]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    request.output = ModelOutput::JsonSchema {
        schema: schema.clone(),
    };
    request.options.insert("verbosity".into(), json!("low"));
    let result = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(parse_json(&result.text).unwrap(), json!({"answer":42}));
    let body = server.requests.lock().unwrap()[0].body.clone();
    assert_eq!(body["text"]["format"]["schema"], schema);
    assert_eq!(body["text"]["format"]["strict"], true);
    assert_eq!(body["text"]["verbosity"], "low");
    request.output = ModelOutput::JsonSchema {
        schema: json!({"type":"object","properties":{"optional":{"type":"string"}},"additionalProperties":false}),
    };
    let failure = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap_err();
    assert_eq!(failure.kind, ModelFailureKind::Unsupported);
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

fn function_events() -> Vec<Value> {
    let reasoning = json!({"id":"rs_1","type":"reasoning","summary":[],"encrypted_content":"ciphertext-fixture"});
    let call = json!({"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"query\":\"figures\"}","status":"completed"});
    vec![
        json!({"type":"response.created","response":{"id":"resp_1","model":"model","status":"in_progress"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"rs_1","type":"reasoning","summary":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":reasoning}),
        json!({"type":"response.output_item.added","output_index":1,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":"fc_1","delta":"{\"query\":"}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":"fc_1","delta":"\"figures\"}"}),
        json!({"type":"response.function_call_arguments.done","output_index":1,"item_id":"fc_1","arguments":"{\"query\":\"figures\"}"}),
        json!({"type":"response.output_item.done","output_index":1,"item":call}),
        json!({"type":"response.completed","response":{"id":"resp_1","model":"model","status":"completed","output":[reasoning,call]}}),
    ]
}
fn with_tool(request: &mut ModelRequest) {
    request.tools = vec![ModelTool {
        name: id("lookup"),
        description: "Read figures".into(),
        model_input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer"}},"required":["query"],"additionalProperties":false}),
    }];
}
#[tokio::test]
async fn function_fragments_and_full_reasoning_replay_preserve_original_order_once() {
    let server = Server::new(vec![
        Reply::sse(&function_events()),
        Reply::sse(&events("model", "received result")),
    ])
    .await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    with_tool(&mut request);
    let first = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(first.finish, ModelFinish::ToolCalls);
    assert_eq!(first.tool_calls.len(), 1);
    assert!(first.metadata.usage.is_none());
    assert_eq!(first.continuation.len(), 1);
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![
            ModelContent::ToolCall {
                provider_call_id: id("call_1"),
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
            provider_call_id: id("call_1"),
            content: json!({"result":73}),
        }],
    });
    request.request_id = id("second-attempt");
    let result = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(result.text, "received result");
    {
        let calls = server.requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        let input = calls[1].body["input"].as_array().unwrap();
        assert_eq!(
            input
                .iter()
                .filter(|item| item["type"] == "function_call")
                .count(),
            1
        );
        assert_eq!(input[1]["type"], "reasoning");
        assert_eq!(input[1]["encrypted_content"], "ciphertext-fixture");
        assert_eq!(input[2]["call_id"], "call_1");
        assert_eq!(input[3]["type"], "function_call_output");
        assert_eq!(
            parse_json(input[3]["output"].as_str().unwrap()).unwrap(),
            json!({"result":73})
        );
        assert_eq!(calls[0].body["tools"][0]["strict"], false);
        assert_eq!(
            calls[0].body["tools"][0]["parameters"]["required"],
            json!(["query"])
        );
    }
    if let ModelContent::ToolCall { arguments, .. } = &mut request.messages[1].content[0] {
        arguments.insert("query".into(), json!("changed"));
    }
    assert!(
        collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .is_err()
    );
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn truncated_conflicting_and_length_limited_streams_never_return_executable_calls() {
    for mode in ["truncated", "conflict", "length", "duplicate-terminal"] {
        let mut data = function_events();
        match mode {
            "truncated" => {
                data.truncate(5);
            }
            "conflict" => {
                data[5]["item_id"] = json!("another-item");
            }
            "length" => {
                data.truncate(5);
                data.push(json!({"type":"response.incomplete","response":{"id":"resp_1","model":"model","status":"incomplete","output":[],"incomplete_details":{"reason":"max_output_tokens"}}}));
            }
            _ => {
                data.push(data.last().unwrap().clone());
            }
        }
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let mut request = request(&connection, "model");
        with_tool(&mut request);
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        assert!(result.is_err(), "{mode} must not yield a complete call");
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn unknown_options_wrong_scope_and_wrong_target_never_reach_the_server() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    for field in [
        "store",
        "input",
        "tools",
        "background",
        "previous_response_id",
    ] {
        let mut request = request(&connection, "model");
        request.options.insert(field.into(), json!(true));
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, ModelFailureKind::Unsupported);
    }
    let mut request = request(&connection, "model");
    let mut caller = context(&request);
    caller.scope.workspace_id = id("other");
    assert_eq!(
        model
            .generate(&request, &caller)
            .next()
            .await
            .unwrap()
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    caller = context(&request);
    request
        .route
        .target
        .insert("project_id".into(), json!("different"));
    assert!(
        collect_model_response(&request, model.generate(&request, &caller))
            .await
            .is_err()
    );
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn provider_errors_and_redirects_are_not_retried_or_exposed_as_text() {
    for (status, expected) in [
        (401, ModelFailureKind::Authentication),
        (404, ModelFailureKind::Unavailable),
        (429, ModelFailureKind::RateLimited),
        (503, ModelFailureKind::Transport),
        (302, ModelFailureKind::Unsupported),
    ] {
        let redirect = Server::new(vec![Reply::sse(&events("model", "unexpected redirect"))]).await;
        let mut reply = Reply::json(
            status,
            json!({"error":{"message":"private diagnostic","code":"fixture"}}),
        );
        if status == 302 {
            reply
                .headers
                .push(("location", format!("{}responses", redirect.base)));
        }
        let server = Server::new(vec![
            reply,
            Reply::sse(&events("model", "unexpected retry")),
        ])
        .await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let request = request(&connection, "model");
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, expected);
        assert!(failure.partial_text().is_empty());
        assert_eq!(server.requests.lock().unwrap().len(), 1);
        assert!(redirect.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn cancellation_and_deadline_drop_an_unfinished_stream() {
    for cancel in [true, false] {
        let mut reply = Reply::sse(&events("model", "partial")[..3]);
        reply.stall = true;
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let request = request(&connection, "model");
        let mut caller = context(&request);
        if !cancel {
            caller.deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(100);
        }
        let mut stream = model.generate(&request, &caller);
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ModelEvent::TextDelta { .. }
        ));
        if cancel {
            caller.cancellation.cancel();
        }
        let next = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap();
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
async fn model_inspection_uses_registered_snapshot_facts_not_requested_versions_or_name_patterns() {
    let server = Server::new(vec![
        Reply::json(200, json!({"id":"model-2026-01-01","object":"model"})),
        Reply::json(200, json!({"id":"model-2026-01-01","object":"model"})),
    ])
    .await;
    let connection = connection(&server);
    let request = request(&connection, "model-2026-01-01");
    let context = ModelInspectionContext {
        scope: scope(),
        principal_ref: id("user"),
        capability_grant_ref: id("grant"),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(3),
    };
    let unknown = OpenAiInspector::new(connection.clone(), vec![])
        .unwrap()
        .inspect(&request.route, &context)
        .await
        .unwrap();
    assert!(unknown.model_version.is_none());
    assert_eq!(unknown.version_semantics, VersionSemantics::Unverified);
    assert!(
        unknown
            .validate(&request.route, VersionPolicy::RequirePinned)
            .is_err()
    );
    let known = OpenAiInspector::new(
        connection,
        vec![OpenAiSnapshot {
            model_id: id("model-2026-01-01"),
            model_version: id("actual-release"),
            evidence_ref: id("documented-snapshot"),
        }],
    )
    .unwrap()
    .inspect(&request.route, &context)
    .await
    .unwrap();
    assert_eq!(known.model_version, Some(id("actual-release")));
    assert_eq!(known.version_semantics, VersionSemantics::Pinned);
    assert_eq!(
        known
            .validate(&request.route, VersionPolicy::RequirePinned)
            .unwrap_err()
            .code,
        ErrorCode::ModelVersionDrift
    );
    let calls = server.requests.lock().unwrap();
    assert!(
        calls
            .iter()
            .all(|call| call.method == "GET" && call.path == "/v1/models/model-2026-01-01")
    );
}

#[tokio::test]
async fn refusal_cannot_be_relabelled_as_success_by_a_conflicting_terminal() {
    for valid in [false, true] {
        let mut data = events("model", "Unable to help");
        data[2]["type"] = json!("response.refusal.delta");
        data[3]["type"] = json!("response.refusal.done");
        data[3].as_object_mut().unwrap().remove("text");
        data[3]["refusal"] = json!("Unable to help");
        if valid {
            let part = json!({"type":"refusal","refusal":"Unable to help"});
            data[4]["item"]["content"] = json!([part]);
            data[5]["response"]["output"][0]["content"] = json!([part]);
        }
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let request = request(&connection, "model");
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        if valid {
            assert_eq!(result.unwrap().finish, ModelFinish::Refusal);
        } else {
            assert!(result.is_err());
        }
    }
}

#[tokio::test]
async fn interleaved_function_deltas_keep_independent_identity_and_arguments() {
    let mut data = vec![
        json!({"type":"response.created","response":{"id":"resp_1","model":"model","status":"in_progress"}}),
    ];
    let calls:Vec<_>=["one","two"].into_iter().enumerate().map(|(index,query)|json!({"id":format!("fc_{index}"),"type":"function_call","call_id":format!("call_{index}"),"name":"lookup","arguments":format!("{{\"query\":\"{query}\"}}"),"status":"completed"})).collect();
    for (index, call) in calls.iter().enumerate() {
        let mut item = call.clone();
        item["arguments"] = json!("");
        item.as_object_mut().unwrap().remove("status");
        data.push(json!({"type":"response.output_item.added","output_index":index,"item":item}));
    }
    for (index, delta) in [
        (0, "{\"query\":"),
        (1, "{\"query\":"),
        (1, "\"two\"}"),
        (0, "\"one\"}"),
    ] {
        data.push(json!({"type":"response.function_call_arguments.delta","output_index":index,"item_id":format!("fc_{index}"),"delta":delta}));
    }
    for (index, call) in calls.iter().enumerate() {
        data.push(json!({"type":"response.function_call_arguments.done","output_index":index,"item_id":format!("fc_{index}"),"arguments":call["arguments"]}));
        data.push(json!({"type":"response.output_item.done","output_index":index,"item":call}));
    }
    data.push(json!({"type":"response.completed","response":{"id":"resp_1","model":"model","status":"completed","output":calls}}));
    let server = Server::new(vec![Reply::sse(&data)]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    with_tool(&mut request);
    let result = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(result.tool_calls.len(), 2);
    assert_eq!(result.tool_calls[0].model_inputs["query"], "one");
    assert_eq!(result.tool_calls[1].model_inputs["query"], "two");
    assert_ne!(
        result.tool_calls[0].provider_call_id,
        result.tool_calls[1].provider_call_id
    );
}

#[tokio::test]
async fn stream_and_payload_limits_reject_oversized_or_reassigned_data_without_a_success() {
    for case in ["raw", "payload", "events", "identity", "sequence", "usage"] {
        let mut data = events("model", "candidate");
        if case == "identity" {
            data[5]["response"]["id"] = json!("other-response");
        }
        if case == "usage" {
            data[5]["response"]["usage"]["output_tokens"] = json!("7");
        }
        let mut reply = Reply::sse(&data);
        if case == "sequence" {
            let text = String::from_utf8(reply.body).unwrap();
            reply.body = text
                .replacen("\"sequence_number\":2", "\"sequence_number\":1", 1)
                .into_bytes();
        }
        let server = Server::new(vec![reply]).await;
        let connection = if case == "raw" {
            OpenAiConnection::new(
                scope(),
                reference("account"),
                "fixture-key-not-a-secret",
                OpenAiOptions {
                    base_url: server.base.clone(),
                    max_transport_bytes: 64,
                    max_event_bytes: 64,
                    ..Default::default()
                },
            )
            .unwrap()
        } else {
            connection(&server)
        };
        let model = OpenAiModel::new(connection.clone());
        let mut request = request(&connection, "model");
        if case == "payload" {
            request.limits.max_response_bytes = 4;
        }
        if case == "events" {
            request.limits.max_events = 1;
        }
        assert!(
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .is_err(),
            "{case}"
        );
    }
}

#[tokio::test]
async fn protocol_metadata_does_not_consume_the_normalized_model_event_allowance() {
    let server = Server::new(vec![Reply::sse(&events("model", "one delta"))]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    request.limits.max_events = 2;
    let result = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(result.text, "one delta");
}

#[tokio::test]
async fn final_content_cannot_move_between_messages_or_parts() {
    for mode in ["valid", "message", "part", "kind"] {
        let mut data = events("model", "A");
        data.truncate(4);
        let mut first = text_item("A");
        first["phase"] = json!("commentary");
        let mut second = text_item("B");
        second["id"] = json!("msg_2");
        second["phase"] = json!("final_answer");
        data.push(json!({"type":"response.output_item.added","output_index":1,"item":{"id":"msg_2","type":"message","role":"assistant","content":[]}}));
        data.push(json!({"type":"response.output_text.delta","output_index":1,"item_id":"msg_2","content_index":0,"delta":"B"}));
        match mode {
            "message" => {
                first["content"][0]["text"] = json!("AB");
                second["content"] = json!([]);
            }
            "part" => {
                first["content"] =
                    json!([{"type":"output_text","text":""},{"type":"output_text","text":"A"}]);
            }
            "kind" => {
                first["content"] = json!([{"type":"refusal","refusal":"A"}]);
            }
            _ => {}
        }
        data.push(json!({"type":"response.completed","response":{"id":"resp_1","model":"model","status":"completed","output":[first,second]}}));
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let request = request(&connection, "model");
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        if mode == "valid" {
            assert_eq!(result.unwrap().text, "AB");
        } else {
            assert!(result.is_err(), "accepted {mode} reassignment");
        }
    }
}
```

## `crates/wickle-model-openai/tests/support/mod.rs`

```rust
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
    task::JoinHandle,
};
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

## `crates/wickle-state-sqlite/tests/agent_recovery.rs`

```rust
//! A new process recovers a real durable run after its predecessor exits mid-call.
#[path = "../../wickle/tests/support/agent.rs"]
#[allow(dead_code)]
mod support;
use support as agent_support;
#[path = "support/context_recovery.rs"]
mod context_recovery;
#[path = "support/recovery_store.rs"]
mod recovery_store;
#[path = "../../wickle/tests/support/agent_resume.rs"]
#[allow(dead_code)]
mod tool_support;
use std::{
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};
use support::*;
use wickle::*;
use wickle_state_sqlite::SqliteStateStore;

// Serialize process fixtures to keep resource contention outside these tests.
static PROCESS_TESTS: std::sync::Mutex<()> = std::sync::Mutex::new(());

// Fault-boundary tests advance cross-process time explicitly. Synchronous fsync
// or scheduler stalls must not consume a one-second lease before the crash point.
// Real-time heartbeat and expiry behavior is covered by the agent/store tests.
struct ProcessClock {
    utc_ms: i64,
}
impl ProcessClock {
    fn new(replacement: bool) -> Self {
        Self {
            utc_ms: if replacement { 3000 } else { 1000 },
        }
    }
}
impl Clock for ProcessClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: self.utc_ms,
            monotonic_ms: 0,
        })
    }
    fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}

struct ProcessModel {
    inner: Arc<dyn ModelPort>,
    directory: std::path::PathBuf,
    interrupt: bool,
}
impl ModelPort for ProcessModel {
    fn binding(&self) -> ModelPortBinding {
        self.inner.binding()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        use std::io::Write;
        let mut calls = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join("calls"))
            .unwrap();
        writeln!(calls, "{}", context.attempt_id).unwrap();
        calls.sync_all().unwrap();
        std::fs::write(self.directory.join("run"), context.run_id.as_str()).unwrap();
        if self.interrupt {
            std::process::exit(73);
        }
        self.inner.generate(request, context)
    }
}
fn worker(directory: &Path, mode: &str) -> std::process::ExitStatus {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "recovery_worker", "--nocapture"])
        .env("WICKLE_RECOVERY_PROCESS_DIRECTORY", directory)
        .env("WICKLE_RECOVERY_PROCESS_MODE", mode)
        .stdin(Stdio::null())
        .spawn()
        .unwrap();
    let end = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if std::time::Instant::now() >= end {
            let _ = child.kill();
            let _ = child.wait();
            panic!("recovery process timed out");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
#[test]
fn a_replacement_process_recovers_an_interrupted_model_with_a_new_charged_attempt() {
    let _serial = PROCESS_TESTS.lock().unwrap();
    let directory = std::env::temp_dir().join(format!(
        "wickle-agent-recovery-{}",
        RandomIdSource.next_id().unwrap()
    ));
    std::fs::create_dir(&directory).unwrap();
    assert_eq!(worker(&directory, "interrupt").code(), Some(73));
    // The replacement clock starts after the original lease's expiry.
    assert!(worker(&directory, "recover").success());
    let calls = std::fs::read_to_string(directory.join("calls")).unwrap();
    let attempts: Vec<_> = calls.lines().collect();
    assert_eq!(attempts.len(), 2);
    assert_ne!(attempts[0], attempts[1]);
    assert!(directory.join("verified").exists());
    std::fs::remove_dir_all(directory).unwrap();
}
#[test]
#[ignore = "Child process fixture requires explicit private test configuration"]
fn recovery_worker() {
    let directory =
        std::path::PathBuf::from(std::env::var_os("WICKLE_RECOVERY_PROCESS_DIRECTORY").unwrap());
    let mode = std::env::var("WICKLE_RECOVERY_PROCESS_MODE").unwrap();
    if mode.starts_with("context-") {
        context_recovery::run_worker(&directory, &mode);
        return;
    }
    if mode.starts_with("tool-") {
        run_tool_worker(&directory, &mode);
        return;
    }
    let interrupt = mode == "interrupt";
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let fixture = Fixture::new(Response::Text, false);
            let store = Arc::new(SqliteStateStore::open(directory.join("state.sqlite")).unwrap());
            let mut bindings = fixture.bindings();
            bindings.state = store.clone();
            bindings.clock = Arc::new(ProcessClock::new(!interrupt));
            bindings.ids = Arc::new(RandomIdSource);
            bindings.model_exchange = Arc::new(
                ModelExchange::new(
                    Arc::new(ProcessModel {
                        inner: fixture.model.clone(),
                        directory: directory.clone(),
                        interrupt,
                    }),
                    bindings.policy.clone(),
                )
                .with_route_inspector(fixture.inspector.clone(), Duration::from_secs(1))
                .unwrap(),
            );
            let mut profile = profile();
            profile.limits.max_recovery_attempts = 2;
            profile.limits.max_elapsed_ms = 60000.try_into().unwrap();
            let agent = create_agent(profile, bindings).unwrap();
            if interrupt {
                let handle = completed(agent.start(request("request"), context()).await.unwrap());
                let _ = handle.outcome(&context()).await;
                panic!("model did not terminate process");
            }
            let run_id = id(&std::fs::read_to_string(directory.join("run")).unwrap());
            let before = store.load(&scope(), &run_id).await.unwrap();
            assert_eq!(before.snapshot.status, RunStatus::Running);
            assert!(matches!(
                before.snapshot.model_ledger[0].state,
                ModelAttemptState::Reserved {}
            ));
            // A new process cannot take ownership while the old lease is live.
            assert_eq!(
                store
                    .acquire_lease(&scope(), &run_id, &id("too-early"), 1000, 1000)
                    .await
                    .unwrap_err()
                    .code,
                ErrorCode::LeaseBusy,
            );
            let source = before.snapshot.recovery_record(id("source")).unwrap();
            let command = ResumeCommand {
                run_id: run_id.clone(),
                expected_revision: before.snapshot.revision,
                command_id: id("recover"),
                action: ResumeAction::Recover {
                    recovery_ref: source.reference().clone(),
                },
            };
            let handle = completed(agent.resume(command.clone(), context()).await.unwrap());
            let outcome = completed(handle.outcome(&context()).await.unwrap());
            assert_eq!(outcome.result.status(), RunStatus::Succeeded);
            let after = store.load(&scope(), &run_id).await.unwrap();
            assert_eq!(after.snapshot.usage.model_calls, 2);
            assert_eq!(after.snapshot.usage.recovery_attempts, 1);
            assert!(matches!(
                after.snapshot.model_ledger[0].state,
                ModelAttemptState::Interrupted { .. }
            ));
            assert_eq!(
                after.snapshot.model_ledger[0].model_step_id,
                after.snapshot.model_ledger[1].model_step_id
            );
            let replay = completed(agent.resume(command, context()).await.unwrap());
            assert_eq!(
                completed(replay.outcome(&context()).await.unwrap()),
                outcome
            );
            std::fs::write(directory.join("verified"), b"recovered and replayed").unwrap();
        });
}

struct ProcessTool {
    inner: Arc<dyn ToolExecutor>,
    name: &'static str,
    crash_on_write: bool,
    directory: std::path::PathBuf,
}
fn append(path: &Path, value: &str) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{value}").unwrap();
    file.sync_all().unwrap();
}
impl ToolExecutor for ProcessTool {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            append(&self.directory.join("tool-calls"), self.name);
            if self.name == "before" && self.crash_on_write {
                // Exceed the fixture lease in wall time, as a slow fsync can do.
                // Explicit test time must still reach the intended write/crash.
                std::thread::sleep(Duration::from_millis(1200));
            }
            if self.name != "target" {
                return self.inner.execute(args, context).await;
            }
            let path = self.directory.join("effect.json");
            // create_new makes any repeated write an observable test failure.
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path)
                .unwrap();
            file.write_all(&serde_json::to_vec(&serde_json::json!({"arguments":args,"attempt":context.attempt_id,"key":context.idempotency_key})).unwrap()).unwrap();
            file.sync_all().unwrap();
            if self.crash_on_write {
                std::process::exit(74);
            }
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: serde_json::json!("target"),
                },
                effect: ToolEffect::Applied,
                receipt: Some(serde_json::json!({"effect_id":"durable-write"})),
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolReconciliation> {
        Box::pin(async move {
            assert_eq!(self.name, "target");
            append(&self.directory.join("queries"), self.name);
            let evidence: serde_json::Value =
                serde_json::from_slice(&std::fs::read(self.directory.join("effect.json")).unwrap())
                    .unwrap();
            assert_eq!(evidence["arguments"], serde_json::to_value(args).unwrap());
            assert_eq!(
                evidence["attempt"],
                serde_json::to_value(&context.attempt_id).unwrap()
            );
            assert_eq!(
                evidence["key"],
                serde_json::to_value(&context.idempotency_key).unwrap()
            );
            Ok(ToolReconciliation::Known {
                result: ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Succeeded {
                        value: serde_json::json!("target"),
                    },
                    effect: ToolEffect::Applied,
                    receipt: Some(serde_json::json!({"effect_id":"durable-write"})),
                },
            })
        })
    }
}
#[test]
fn an_applied_write_is_reconciled_after_process_exit_or_remains_unknown_without_a_query_adapter() {
    let _serial = PROCESS_TESTS.lock().unwrap();
    for known in [true, false] {
        let directory = std::env::temp_dir().join(format!(
            "wickle-tool-recovery-{}",
            RandomIdSource.next_id().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let kind = if known { "known" } else { "unknown" };
        assert_eq!(
            worker(&directory, &format!("tool-{kind}-interrupt")).code(),
            Some(74)
        );
        let effect = std::fs::read(directory.join("effect.json")).unwrap();
        assert!(worker(&directory, &format!("tool-{kind}-recover")).success());
        assert_eq!(
            std::fs::read(directory.join("effect.json")).unwrap(),
            effect
        );
        let calls = std::fs::read_to_string(directory.join("tool-calls")).unwrap();
        assert_eq!(
            calls,
            if known {
                "before\ntarget\nafter\n"
            } else {
                "before\ntarget\n"
            }
        );
        if known {
            assert_eq!(
                std::fs::read_to_string(directory.join("queries")).unwrap(),
                "target\n"
            );
        } else {
            assert!(!directory.join("queries").exists());
        }
        assert!(directory.join("verified").exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
fn run_tool_worker(directory: &Path, mode: &str) {
    use std::sync::atomic::Ordering;
    let boundary = mode
        .strip_prefix("tool-boundary-")
        .map(|value| value.rsplit_once('-').unwrap().0);
    let known = mode.starts_with("tool-known-") || boundary.is_some();
    let interrupt = mode.ends_with("-interrupt");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut fixture = tool_support::Fixture::new(tool_support::Mode::External);
            fixture.profile.limits.max_elapsed_ms = 60000.try_into().unwrap();
            fixture.profile.limits.max_recovery_attempts = 4;
            let store = Arc::new(SqliteStateStore::open(directory.join("state.sqlite")).unwrap());
            let mut bindings = fixture.bindings();
            bindings.state = match (interrupt, boundary) {
                (true, Some(boundary)) => Arc::new(recovery_store::CrashStore {
                    inner: store.clone(),
                    boundary: boundary.into(),
                }),
                _ => store.clone(),
            };
            bindings.clock = Arc::new(ProcessClock::new(!interrupt));
            bindings.ids = Arc::new(RandomIdSource);
            if !interrupt {
                // The replacement exercises normal durable-run settings after
                // the explicit clock handoff expires the original short lease.
                let defaults = AgentSettings::default();
                bindings.settings.lease_ttl_ms = defaults.lease_ttl_ms;
                bindings.settings.heartbeat_interval_ms = defaults.heartbeat_interval_ms;
            }
            let mut registrations = vec![];
            for (index, name) in ["before", "target", "after"].into_iter().enumerate() {
                let mut descriptor = fixture
                    .registry
                    .get(&id(name))
                    .unwrap()
                    .compiled
                    .descriptor()
                    .clone();
                descriptor.reconcile = known && name == "target";
                registrations.push(ToolRegistration {
                    compiled: SchemaCompiler::new()
                        .compile(descriptor, &fixture.inputs)
                        .unwrap(),
                    executor: Arc::new(ProcessTool {
                        inner: fixture.tools[index].clone(),
                        name,
                        crash_on_write: boundary.is_none(),
                        directory: directory.to_owned(),
                    }),
                });
            }
            bindings.tools = Some(Arc::new(ToolRegistry::new(scope(), registrations).unwrap()));
            if !interrupt {
                let count = std::fs::read_to_string(directory.join("calls"))
                    .unwrap()
                    .lines()
                    .count();
                fixture.model.calls.store(count, Ordering::SeqCst);
                // A changed resolver must not replace the already saved foreign key.
                if boundary.is_none() || matches!(boundary, Some("bound" | "settled" | "terminal"))
                {
                    fixture.resolver.value.lock().unwrap().value =
                        serde_json::json!(tool_support::CHANGED_RECORD);
                }
            }
            bindings.model_exchange = Arc::new(
                ModelExchange::new(
                    Arc::new(ProcessModel {
                        inner: fixture.model.clone(),
                        directory: directory.to_owned(),
                        interrupt: false,
                    }),
                    bindings.policy.clone(),
                )
                .with_route_inspector(fixture.base.inspector.clone(), Duration::from_secs(1))
                .unwrap(),
            );
            let agent = create_agent(fixture.profile.clone(), bindings).unwrap();
            if interrupt {
                let handle = fixture.started(&agent).await;
                let outcome = handle.outcome(&context()).await;
                panic!("write did not terminate process: {outcome:?}");
            }
            let run_id = id(&std::fs::read_to_string(directory.join("run")).unwrap());
            let before = store.load(&scope(), &run_id).await.unwrap();
            if boundary == Some("terminal") {
                assert_eq!(before.snapshot.status, RunStatus::Succeeded);
                let replay = fixture.started(&agent).await;
                assert_eq!(
                    completed(replay.outcome(&context()).await.unwrap()),
                    before.snapshot.outcome.unwrap()
                );
                std::fs::write(directory.join("verified"), b"terminal replay").unwrap();
                return;
            }
            if boundary.is_none() {
                assert!(matches!(
                    before.snapshot.tool_ledger[1].state,
                    ToolCallState::Dispatching { .. }
                ));
            }
            let source = before.snapshot.recovery_record(id("write-source")).unwrap();
            let command = ResumeCommand {
                run_id: run_id.clone(),
                expected_revision: before.snapshot.revision,
                command_id: id("recover-write"),
                action: ResumeAction::Recover {
                    recovery_ref: source.reference().clone(),
                },
            };
            let handle = completed(agent.resume(command.clone(), context()).await.unwrap());
            let outcome = completed(handle.outcome(&context()).await.unwrap());
            assert_eq!(
                outcome.result.status(),
                if known {
                    RunStatus::Succeeded
                } else {
                    RunStatus::Waiting
                }
            );
            assert_eq!(outcome.unresolved_effects.len(), usize::from(!known));
            let after = store.load(&scope(), &run_id).await.unwrap();
            if boundary.is_none() || matches!(boundary, Some("bound" | "settled")) {
                assert_eq!(
                    after.snapshot.tool_ledger[1].call.bound_input_ref,
                    before.snapshot.tool_ledger[1].call.bound_input_ref
                );
                assert_eq!(fixture.resolver.calls.load(Ordering::SeqCst), 0);
            }
            let replay = completed(agent.resume(command, context()).await.unwrap());
            assert_eq!(
                completed(replay.outcome(&context()).await.unwrap()),
                outcome
            );
            std::fs::write(directory.join("verified"), b"effect preserved and replayed").unwrap();
        });
}

#[test]
fn durable_plan_binding_result_and_terminal_boundaries_resume_without_repeating_completed_work() {
    let _serial = PROCESS_TESTS.lock().unwrap();
    for boundary in ["before-plan", "plan", "bound", "settled", "terminal"] {
        let directory = std::env::temp_dir().join(format!(
            "wickle-boundary-recovery-{}",
            RandomIdSource.next_id().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        assert_eq!(
            worker(&directory, &format!("tool-boundary-{boundary}-interrupt")).code(),
            Some(75),
            "{boundary}"
        );
        let calls = std::fs::read_to_string(directory.join("tool-calls")).unwrap_or_default();
        assert_eq!(
            calls,
            match boundary {
                "before-plan" | "plan" => "",
                "bound" => "before\n",
                "settled" => "before\ntarget\n",
                _ => "before\ntarget\nafter\n",
            },
            "{boundary}"
        );
        assert!(
            worker(&directory, &format!("tool-boundary-{boundary}-recover")).success(),
            "{boundary}"
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("tool-calls")).unwrap(),
            "before\ntarget\nafter\n",
            "{boundary}"
        );
        assert!(!directory.join("queries").exists(), "{boundary}");
        assert_eq!(
            std::fs::read_to_string(directory.join("calls"))
                .unwrap()
                .lines()
                .count(),
            2,
            "{boundary}"
        );
        assert!(directory.join("verified").exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn compression_revision_process_boundaries_restore_only_complete_context_without_recompression() {
    let _serial = PROCESS_TESTS.lock().unwrap();
    for boundary in ["before-context", "context"] {
        let directory = std::env::temp_dir().join(format!(
            "wickle-context-recovery-{}",
            RandomIdSource.next_id().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        assert_eq!(
            worker(&directory, &format!("context-{boundary}-interrupt")).code(),
            Some(76)
        );
        assert!(worker(&directory, &format!("context-{boundary}-recover")).success());
        assert_eq!(
            std::fs::read_to_string(directory.join("reads")).unwrap(),
            "read\nread\n"
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("summaries"))
                .unwrap()
                .lines()
                .count(),
            if boundary == "context" { 1 } else { 2 }
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("calls"))
                .unwrap()
                .lines()
                .count(),
            3
        );
        assert!(directory.join("verified").exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
```

## `crates/wickle-state-sqlite/tests/support/context_recovery.rs`

```rust
//! Real compression and SQLite checkpoints on both sides of an abrupt process exit.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Model(AtomicUsize);
impl ModelPort for Model {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: id("fixture"),
            adapter: reference("adapter"),
            connection_ref: reference("connection"),
        }
    }
    fn generate<'a>(
        &'a self,
        _: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let index = self.0.fetch_add(1, Ordering::SeqCst);
        let (event, finish) = if index < 2 {
            (
                ModelEvent::ToolArgumentsDelta {
                    index: 0,
                    provider_call_id: Some(format!("read-{index}")),
                    name: Some("before".into()),
                    delta: "{\"query\":\"chunk\"}".into(),
                },
                ModelFinish::ToolCalls,
            )
        } else {
            (
                ModelEvent::TextDelta {
                    text: "The saved records were read.".into(),
                },
                ModelFinish::Stop,
            )
        };
        Box::pin(futures_util::stream::iter([
            Ok(event),
            Ok(ModelEvent::ResponseCompleted {
                finish,
                metadata: Default::default(),
                continuation: vec![],
            }),
        ]))
    }
}
struct Read(std::path::PathBuf);
impl ToolExecutor for Read {
    fn execute<'a>(
        &'a self,
        _: &'a JsonObject,
        _: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            append(&self.0.join("reads"), "read");
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: serde_json::json!("x".repeat(3500)),
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}
struct Summary(std::path::PathBuf);
impl HostContextCompactor for Summary {
    fn compact<'a>(
        &'a self,
        request: &'a CompactionRequest,
        _: &'a ContextStrategyContext,
    ) -> PortFuture<'a, String> {
        Box::pin(async move {
            assert!(!request.segments.is_empty());
            append(&self.0.join("summaries"), "summary");
            Ok("Older complete records were read; their observations remain in storage.".into())
        })
    }
}

pub fn run_worker(directory: &Path, mode: &str) {
    let interrupt = mode.ends_with("-interrupt");
    let committed = !mode.starts_with("context-before-");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut fixture = tool_support::Fixture::new(tool_support::Mode::External);
            fixture.profile.limits.max_elapsed_ms = 60000.try_into().unwrap();
            fixture.profile.limits.max_recovery_attempts = 4;
            let store = Arc::new(SqliteStateStore::open(directory.join("state.sqlite")).unwrap());
            let mut bindings = fixture.bindings();
            bindings.state = if interrupt {
                Arc::new(recovery_store::CrashStore {
                    inner: store.clone(),
                    boundary: if committed {
                        "context"
                    } else {
                        "before-context"
                    }
                    .into(),
                })
            } else {
                store.clone()
            };
            bindings.clock = Arc::new(ProcessClock::new(!interrupt));
            bindings.ids = Arc::new(RandomIdSource);
            bindings.settings.projection_limits.max_bytes = 8000;
            if !interrupt {
                bindings.settings.lease_ttl_ms = 30000;
                bindings.settings.heartbeat_interval_ms = 5000;
            }
            let mut descriptor = fixture
                .registry
                .get(&id("before"))
                .unwrap()
                .compiled
                .descriptor()
                .clone();
            descriptor.max_output_bytes = 65536.try_into().unwrap();
            bindings.tools = Some(Arc::new(
                ToolRegistry::new(
                    scope(),
                    vec![
                        ToolRegistration {
                            compiled: SchemaCompiler::new()
                                .compile(descriptor, &fixture.inputs)
                                .unwrap(),
                            executor: Arc::new(Read(directory.to_owned())),
                        },
                        fixture.registry.get(&id("target")).unwrap().clone(),
                        fixture.registry.get(&id("after")).unwrap().clone(),
                    ],
                )
                .unwrap(),
            ));
            bindings.context_runtime = Some(Arc::new(
                ContextRuntime::new(
                    scope(),
                    Arc::new(BoundedContextStrategy),
                    Some(ContextCompactor::Host {
                        definition: reference("summary"),
                        compressor: Arc::new(Summary(directory.to_owned())),
                    }),
                    ContextRewriteLimits::default(),
                )
                .unwrap(),
            ));
            let count = if interrupt {
                0
            } else {
                std::fs::read_to_string(directory.join("calls"))
                    .unwrap()
                    .lines()
                    .count()
            };
            bindings.model_exchange = Arc::new(
                ModelExchange::new(
                    Arc::new(ProcessModel {
                        inner: Arc::new(Model(AtomicUsize::new(count))),
                        directory: directory.to_owned(),
                        interrupt: false,
                    }),
                    bindings.policy.clone(),
                )
                .with_route_inspector(fixture.base.inspector.clone(), Duration::from_secs(1))
                .unwrap(),
            );
            let agent = create_agent(fixture.profile.clone(), bindings).unwrap();
            if interrupt {
                let handle = fixture.started(&agent).await;
                let result = handle.outcome(&context()).await;
                panic!("compression did not terminate process: {result:?}");
            }
            let run_id = id(&std::fs::read_to_string(directory.join("run")).unwrap());
            let before = store.load(&scope(), &run_id).await.unwrap();
            assert_eq!(before.snapshot.status, RunStatus::Running);
            assert_eq!(before.snapshot.context_revision_ref.is_some(), committed);
            assert_eq!(before.snapshot.context_decisions.is_empty(), !committed);
            assert_eq!(before.snapshot.usage.model_calls, 2);
            assert_eq!(before.snapshot.usage.tool_attempts, 2);
            let previous_revision = if let Some(reference) = &before.snapshot.context_revision_ref {
                Some(store.read_record(&scope(), reference).await.unwrap())
            } else {
                None
            };
            let source = before
                .snapshot
                .recovery_record(id("context-source"))
                .unwrap();
            let handle = completed(
                agent
                    .resume(
                        ResumeCommand {
                            run_id: run_id.clone(),
                            expected_revision: before.snapshot.revision,
                            command_id: id("recover-context"),
                            action: ResumeAction::Recover {
                                recovery_ref: source.reference().clone(),
                            },
                        },
                        context(),
                    )
                    .await
                    .unwrap(),
            );
            assert_eq!(
                completed(handle.outcome(&context()).await.unwrap())
                    .result
                    .status(),
                RunStatus::Succeeded
            );
            let after = store.load(&scope(), &run_id).await.unwrap();
            assert_eq!(after.snapshot.profile, before.snapshot.profile);
            assert_eq!(after.snapshot.system_inputs, before.snapshot.system_inputs);
            assert_eq!(after.snapshot.limits, before.snapshot.limits);
            assert_eq!(after.snapshot.usage.model_calls, 3);
            assert_eq!(after.snapshot.usage.tool_attempts, 2);
            assert_eq!(
                after.snapshot.usage.recovery_attempts,
                before.snapshot.usage.recovery_attempts + 1
            );
            for message in &before.messages {
                assert!(after.messages.contains(message));
            }
            for reservation in &before.snapshot.reservations {
                assert!(after.snapshot.reservations.contains(reservation));
            }
            let reference = after
                .snapshot
                .context_revision_ref
                .as_ref()
                .expect("a complete compressed revision is adopted");
            let revision = store.read_record(&scope(), reference).await.unwrap();
            if let Some(previous) = previous_revision {
                assert_eq!(
                    before.snapshot.context_revision_ref.as_ref(),
                    Some(reference)
                );
                assert_eq!(revision, previous);
                assert_eq!(
                    after.snapshot.context_decisions,
                    before.snapshot.context_decisions
                );
            }
            assert!(!after.snapshot.context_decisions.is_empty());
            std::fs::write(directory.join("verified"), b"complete context restored").unwrap();
        });
}
```

## `tests/support/openai_consumer.rs`

```rust
// Real loopback HTTP/SSE against the extracted OpenAI adapter package; no provider call.
use serde_json::json;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use wickle::*;
use wickle_model_openai::*;
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
    let base = format!("http://{}/v1/", listener.local_addr()?);
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
        assert!(headers.starts_with("post /v1/responses "));
        assert!(headers.contains("openai-project: fixture-project"));
        let body =
            parse_json(std::str::from_utf8(&bytes[header_end..header_end + length]).unwrap())
                .unwrap();
        assert_eq!(body["model"], "fixture-model");
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
    let connection = OpenAiConnection::new(
        scope.clone(),
        reference("account"),
        "fixture-key",
        OpenAiOptions {
            base_url: base,
            project_id: Some("fixture-project".into()),
            ..Default::default()
        },
    )?;
    let binding = connection.binding();
    let model = OpenAiModel::new(connection.clone());
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
            api_contract: OpenAiConnection::api_contract(),
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
        "OpenAI consumer: extracted adapter performs one HTTP/SSE request, preserves scoped endpoint/project identity and options, decodes JSON and reported usage, and excludes Host context from the wire (local fixture, no provider network)"
    );
    Ok(())
}
```
