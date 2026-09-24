# 56장 전체 구현과 변경 검사

[강의](../56-xai-contract.md) · [전체 변경 패치](../solutions/56-xai-contract.patch)

기준 `7d9c034a23ddb41fa397433f61ff0c8e5d4d72a2`. 이 단계에서 바뀐 Rust·manifest·Python 파일의 전체 내용이다. 이전 버전과의 정확한 교체 위치·삭제는 patch를 따른다. 다음 장의 코드와 섞지 않는다.

## `crates/wickle-model-responses/src/codec.rs`

```rust
use serde_json::{Value, json};
use wickle::*;

pub(crate) const REPLAY_KIND: &str = "wickle.openai.responses.v1";
pub(crate) const XAI_REPLAY_KIND: &str = "wickle.xai.responses.v1";
fn failure(code: ErrorCode) -> ContractError {
    crate::error(code, "codec")
}

/// Encode a Responses request, preserving exact route-bound replay and model schemas.
pub fn encode_request(request: &ModelRequest) -> Result<Value, ContractError> {
    encode(request, false)
}
/// Encode xAI's Responses dialect without relaxing OpenAI/Azure validation.
pub fn encode_xai_request(request: &ModelRequest) -> Result<Value, ContractError> {
    if request.tools.len() > 350 || request.options.contains_key("verbosity") {
        return Err(failure(ErrorCode::ModelCapabilityUnsupported));
    }
    if let Some(effort) = request.options.get("reasoning_effort") {
        let effort = effort
            .as_str()
            .ok_or_else(|| failure(ErrorCode::ModelOptionUnsupported))?;
        if !matches!(effort, "none" | "low" | "medium" | "high" | "xhigh")
            || (matches!(
                request.route.model_id.as_str(),
                "grok-4.7" | "grok-4.6" | "grok-4.5"
            ) && effort == "none")
        {
            return Err(failure(ErrorCode::ModelOptionUnsupported));
        }
    }
    encode(request, true)
}
fn encode(request: &ModelRequest, xai: bool) -> Result<Value, ContractError> {
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
            if object.len() != 2
                || object.get("kind")
                    != Some(&json!(if xai { XAI_REPLAY_KIND } else { REPLAY_KIND }))
            {
                return Err(failure(ErrorCode::ModelContextIncompatible));
            }
            let items = replay
                .get("items")
                .and_then(Value::as_array)
                .ok_or_else(|| failure(ErrorCode::ModelContextIncompatible))?;
            let decoded = inspect_output_items(items, xai)?;
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
                    || match parse_provider_arguments(
                        &original.arguments,
                        request.limits.max_input_bytes,
                    ) {
                        Ok(original) => **arguments != original,
                        // The core retains an empty canonical placeholder for an
                        // unparseable proposal. Replay the original opaque text so
                        // its paired Tool error can reach the next model turn.
                        Err(_) => !arguments.is_empty(),
                    }
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
    let tools: Vec<_> = request.tools.iter().map(|tool| {
        let mut declaration = json!({"type":"function","name":tool.name,"description":tool.description,"parameters":tool.model_input_schema});
        if !xai {
            declaration["strict"] = json!(match request.route.provider.as_str() {
                "openai" => crate::schema::strict_schema_supported(&tool.model_input_schema, request.route.model_id.as_str().starts_with("ft:")),
                "azure-openai" => crate::schema::azure_strict_schema_supported(&tool.model_input_schema),
                _ => false,
            });
        }
        declaration
    }).collect();
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
pub(crate) fn inspect_output_items(
    items: &[Value],
    xai: bool,
) -> Result<OutputItems, ContractError> {
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
        if object.get("id").is_some_and(|id| {
            id.as_str()
                .is_none_or(|id| id.is_empty() && !(xai && item["type"] == "reasoning"))
        }) || object
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
                {
                    return Err(failure(ErrorCode::InvalidContract));
                }
                // The provider envelope is valid even when proposed arguments are not.
                // Keep their exact bytes for the core's validation/repair loop.
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

## `crates/wickle-model-xai/src/lib.rs`

```rust
//! xAI Grok Responses with explicit scoped credentials and bounded replay.
#![forbid(unsafe_code)]
mod connection;
mod inspection;
mod model;
mod schema;
pub use connection::{XaiConnection, XaiOptions};
pub use inspection::{XaiInspector, XaiSnapshot};
pub use model::XaiModel;
pub use schema::XaiToolSchemaCompiler;
use wickle::{ContractError, ErrorCode};
fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("xai.{location}"))
}
```

## `crates/wickle-model-xai/src/model.rs`

```rust
use crate::{XaiConnection, error};
use futures_util::stream;
use reqwest::Response;
use std::collections::VecDeque;
use wickle::*;
use wickle_model_responses::{ResponsesDecoder, SseDecoder, encode_xai_request};

/// One xAI Responses POST per invocation, streamed into Wickle model events.
/// The adapter does not retry, run Tool handlers, or load environment variables.
#[derive(Debug, Clone)]
pub struct XaiModel {
    connection: XaiConnection,
}
impl XaiModel {
    /// Bind an already configured connection without making a network request.
    pub fn new(connection: XaiConnection) -> Self {
        Self { connection }
    }
}
impl ModelPort for XaiModel {
    fn tool_schema_compiler(&self) -> std::sync::Arc<dyn ProviderToolSchemaCompiler> {
        std::sync::Arc::new(crate::XaiToolSchemaCompiler)
    }
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
            decoder: ResponsesDecoder::for_xai(request, None),
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
    connection: &'a XaiConnection,
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
        let value = encode_xai_request(self.request)?;
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
        let response = tokio::select! { biased;
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
            let kind = match status {
                401 | 403 => ModelFailureKind::Authentication,
                404 => ModelFailureKind::Unavailable,
                408 | 504 => ModelFailureKind::Timeout,
                429 => ModelFailureKind::RateLimited,
                500..=599 => ModelFailureKind::Transport,
                _ => ModelFailureKind::Unsupported,
            };
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

## `crates/wickle-model-xai/src/schema.rs`

```rust
//! xAI projection preserves optional fields and delegates semantic gaps to the core.
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;
use wickle::*;

/// Versioned xAI Responses Tool schema projection with reversible field encodings.
#[derive(Debug, Clone, Copy, Default)]
pub struct XaiToolSchemaCompiler;
impl ProviderToolSchemaCompiler for XaiToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new("wickle-xai-tool-schema").expect("constant"),
            version: Id::new("1").expect("constant"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        if target.provider.as_str() != "xai"
            || target.api_contract.operation.as_str() != "responses"
            || target.api_contract.version.as_str() != "v1"
        {
            return Err(crate::error(
                ErrorCode::ModelCapabilityUnsupported,
                "schema_target",
            ));
        }
        let root = &tool.model_input_schema;
        let properties = root
            .get("properties")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                crate::error(ErrorCode::InvalidToolInputContract, "schema_properties")
            })?;
        let mut budget = Budget {
            nodes: 1024,
            bytes: 32 * 1024,
        };
        let mut projected = Map::new();
        let mut fields = vec![];
        for (name, schema) in properties {
            let native = lower(schema, root, 0, &mut BTreeSet::new(), &mut budget);
            let (schema, encoding) = match native {
                Some(schema) => (schema, ArgumentValueEncoding::Identity {}),
                None => (
                    json!({"type":"string"}),
                    ArgumentValueEncoding::JsonText { optional: false },
                ),
            };
            projected.insert(name.clone(), schema);
            fields.push(ArgumentFieldMapping {
                wire_name: name.clone(),
                canonical_name: name.clone(),
                encoding,
            });
        }
        let mut wire_tool = tool.clone();
        wire_tool.model_input_schema = json!({"type":"object","properties":projected,"required":root.get("required").cloned().unwrap_or_else(||json!([])),"additionalProperties":false});
        for key in ["minProperties", "maxProperties", "description", "title"] {
            if let Some(value) = root.get(key) {
                wire_tool.model_input_schema[key] = value.clone();
            }
        }
        // Even accepted keywords may be best-effort. Keep the complete canonical
        // guidance and original validation instead of claiming native equivalence.
        Ok(ProviderToolProjection {
            wire_tool,
            decode_plan: ArgumentDecodePlan::Fields { fields },
        })
    }
}
struct Budget {
    nodes: usize,
    bytes: usize,
}
impl Budget {
    fn charge(&mut self, value: &Value) -> Option<()> {
        let bytes = serde_json::to_vec(value).ok()?.len();
        self.nodes = self.nodes.checked_sub(1)?;
        self.bytes = self.bytes.checked_sub(bytes)?;
        Some(())
    }
}
fn lower(
    schema: &Value,
    root: &Value,
    depth: usize,
    visiting: &mut BTreeSet<String>,
    budget: &mut Budget,
) -> Option<Value> {
    if depth > 16 {
        return None;
    }
    budget.charge(schema)?;
    let node = schema.as_object()?;
    if [
        "$id",
        "$anchor",
        "$dynamicRef",
        "$dynamicAnchor",
        "patternProperties",
    ]
    .iter()
    .any(|key| node.contains_key(*key))
    {
        return None;
    }
    if let Some(reference) = node.get("$ref") {
        let reference = reference.as_str()?;
        if !reference.starts_with('#') || !visiting.insert(reference.into()) {
            return None;
        }
        let mut combined = root.pointer(&reference[1..])?.as_object()?.clone();
        for (key, value) in node.iter().filter(|(key, _)| key.as_str() != "$ref") {
            if combined.get(key).is_some_and(|old| old != value)
                && !matches!(
                    key.as_str(),
                    "description" | "title" | "default" | "$comment"
                )
            {
                return None;
            }
            combined.insert(key.clone(), value.clone());
        }
        let result = lower(&Value::Object(combined), root, depth + 1, visiting, budget);
        visiting.remove(reference);
        return result;
    }
    let mut result = Map::new();
    let kinds: Vec<&str> = match node.get("type") {
        Some(Value::String(kind)) => vec![kind.as_str()],
        Some(Value::Array(kinds)) => kinds.iter().map(Value::as_str).collect::<Option<_>>()?,
        None => vec![],
        _ => return None,
    };
    if kinds.iter().any(|kind| {
        !matches!(
            *kind,
            "string" | "number" | "integer" | "boolean" | "null" | "array" | "object"
        )
    }) {
        return None;
    }
    if let Some(kind) = node.get("type") {
        result.insert("type".into(), kind.clone());
    }
    if let Some(branches) = node.get("anyOf").or_else(|| node.get("oneOf")) {
        let branches = branches.as_array()?;
        if branches.is_empty() {
            return None;
        }
        result.insert(
            "anyOf".into(),
            Value::Array(
                branches
                    .iter()
                    .map(|branch| lower(branch, root, depth + 1, visiting, budget))
                    .collect::<Option<_>>()?,
            ),
        );
    }
    if let Some(branches) = node.get("allOf").and_then(Value::as_array) {
        if branches.len() == 1 {
            result.insert(
                "allOf".into(),
                Value::Array(vec![lower(
                    &branches[0],
                    root,
                    depth + 1,
                    visiting,
                    budget,
                )?]),
            );
        }
    }
    if kinds.contains(&"object") {
        let mut properties = Map::new();
        if let Some(children) = node.get("properties") {
            for (name, child) in children.as_object()? {
                properties.insert(
                    name.clone(),
                    lower(child, root, depth + 1, visiting, budget)?,
                );
            }
        }
        result.insert("properties".into(), Value::Object(properties));
        if let Some(required) = node.get("required") {
            result.insert("required".into(), required.clone());
        }
        // xAI defaults this to false; JSON Schema defaults it to true.
        let additional = match node.get("additionalProperties") {
            None => Value::Bool(true),
            Some(value) if value.is_boolean() => value.clone(),
            Some(value) => lower(value, root, depth + 1, visiting, budget)?,
        };
        result.insert("additionalProperties".into(), additional);
    }
    if kinds.contains(&"array") {
        if let Some(prefix) = node.get("prefixItems") {
            result.insert(
                "prefixItems".into(),
                Value::Array(
                    prefix
                        .as_array()?
                        .iter()
                        .map(|item| lower(item, root, depth + 1, visiting, budget))
                        .collect::<Option<_>>()?,
                ),
            );
        }
        result.insert(
            "items".into(),
            lower(node.get("items")?, root, depth + 1, visiting, budget)?,
        );
    }
    for key in ["enum", "const"] {
        if let Some(value) = node.get(key) {
            if key == "enum" && value.as_array().is_none_or(|values| values.is_empty()) {
                return None;
            }
            result.insert(key.into(), value.clone());
        }
    }
    if kinds.is_empty()
        && !result.contains_key("anyOf")
        && !result.contains_key("allOf")
        && !result.contains_key("enum")
        && !result.contains_key("const")
    {
        return None;
    }
    for key in [
        "description",
        "title",
        "default",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
        "minProperties",
        "maxProperties",
    ] {
        if let Some(value) = node.get(key) {
            result.insert(key.into(), value.clone());
        }
    }
    // Regex anchoring and character semantics differ. Keep patterns in canonical
    // guidance/core validation instead of narrowing valid canonical inputs.
    if let Some(format) = node.get("format").and_then(Value::as_str) {
        if matches!(
            format,
            "date" | "time" | "date-time" | "email" | "uuid" | "ipv4" | "ipv6" | "uri"
        ) {
            result.insert("format".into(), json!(format));
        }
    }
    Some(Value::Object(result))
}
```

## `crates/wickle-model-xai/tests/agent_contract.rs`

```rust
//! Actual HTTP/SSE through the core: provider projection, repair and system binding.
#[path = "../../wickle/tests/support/agent.rs"]
#[allow(dead_code)]
mod core_host;
#[allow(dead_code)]
mod support;
use core_host::{completed, id, reference, scope};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use wickle::*;
use wickle_model_xai::*;
const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";

struct Catalog(core_host::Catalog);
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        scope: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            let mut metadata = self.0.resolve(reference, scope).await?;
            if reference.kind == ComponentKind::Tool {
                metadata.model_name = Some(reference.id.clone());
            }
            if let Some(version) = &reference.version {
                metadata.reference.version = Some(version.clone());
            }
            Ok(metadata)
        })
    }
}
struct Inspector;
impl ModelRouteInspector for Inspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        _: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: ModelRouteAvailability::Available,
                model_id: Some(route.model_id.clone()),
                model_version: Some(route.model_version.clone()),
                deployment_revision: None,
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id("local-http-fixture"),
            })
        })
    }
}
struct Capture(Mutex<Vec<JsonObject>>);
impl ToolExecutor for Capture {
    fn execute<'a>(
        &'a self,
        call: &'a JsonObject,
        _: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.0.lock().unwrap().push(call.clone());
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: json!("observed"),
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}
fn call_reply(index: usize, arguments: &str) -> support::Reply {
    let response = format!("resp_{index}");
    let item = format!("fc_{index}");
    let call = format!("call_{index}");
    let reasoning = json!({"id":"","type":"reasoning","summary":[],"encrypted_content":format!("ciphertext-{index}")});
    let output = json!({"id":item,"type":"function_call","call_id":call,"name":"lookup","arguments":arguments,"status":"completed"});
    support::Reply::sse(&[
        json!({"type":"response.created","response":{"id":response,"model":support::MODEL,"status":"in_progress"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"","type":"reasoning","summary":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":reasoning}),
        json!({"type":"response.output_item.added","output_index":1,"item":{"id":item,"type":"function_call","call_id":call,"name":"lookup","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":item,"delta":arguments}),
        json!({"type":"response.function_call_arguments.done","output_index":1,"item_id":item,"arguments":arguments}),
        json!({"type":"response.output_item.done","output_index":1,"item":output}),
        json!({"type":"response.completed","response":{"id":response,"model":support::MODEL,"status":"completed","output":[reasoning,output]}}),
    ])
}
#[tokio::test]
async fn compiled_xai_contract_repairs_arguments_and_retries_only_through_the_core() {
    let invalid_constraint =
        json!({"query":"latest","limit":7,"note":null,"filter":"{\"category\":\"finance\"}"})
            .to_string();
    let invalid_json =
        json!({"query":"latest","limit":9,"note":null,"filter":"{not json"}).to_string();
    let valid =
        json!({"query":"latest","limit":9,"note":null,"filter":"{\"category\":\"finance\"}"})
            .to_string();
    let malformed = "{not json";
    let rounded = r#"{"query":"latest","limit":0.12345678901234567890123456789}"#;
    let server = support::Server::new(vec![
        support::Reply::json(503, json!({"error":{"code":503}})),
        call_reply(0, malformed),
        call_reply(4, rounded),
        call_reply(1, &invalid_constraint),
        call_reply(2, &invalid_json),
        call_reply(3, &valid),
        support::Reply::sse(&support::events(support::MODEL, "complete")),
    ])
    .await;
    let connection = XaiConnection::new(
        scope(),
        reference("account"),
        "fixture-key-not-a-secret",
        XaiOptions {
            base_url: server.base.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    let fixture = core_host::Fixture::new(core_host::Response::Text, false);
    let mut catalog = fixture.router.snapshot.catalog().clone();
    catalog.models[0].provider = id("xai");
    catalog.models[0].model_id = id(support::MODEL);
    catalog.models[0].model_version = id("fixture-release");
    catalog.models[0]
        .capabilities
        .features
        .insert(id("tool_calling"));
    catalog.models[0].capabilities.options_schema = json!({"type":"object","properties":{"reasoning_effort":{"enum":["low","medium","high"]}},"additionalProperties":false});
    catalog.bindings[0]
        .default_options
        .insert("reasoning_effort".into(), json!("high"));
    catalog.bindings[0].model = catalog.models[0].reference();
    catalog.bindings[0].requested_model = id(support::MODEL);
    catalog.bindings[0].adapter = connection.binding().adapter;
    catalog.bindings[0].connection_ref = connection.binding().connection_ref;
    catalog.bindings[0].api_contract = connection.api_contract();
    catalog.bindings[0].target = connection.target().clone();
    let properties: serde_json::Map<String, Value> = connection
        .target()
        .iter()
        .map(|(key, value)| (key.clone(), json!({"const":value})))
        .collect();
    let required: Vec<_> = connection.target().keys().cloned().collect();
    catalog.bindings[0].target_schema = json!({"type":"object","properties":properties,"required":required,"additionalProperties":false});
    catalog.bindings[0].capabilities = catalog.models[0].capabilities.clone();
    catalog.bindings[0].evidence[0].binding_digest = catalog.bindings[0]
        .contract_digest(&catalog.models[0])
        .unwrap();
    let snapshot = RoutingSnapshot::new(catalog, fixture.router.snapshot.policy().clone()).unwrap();
    let mut bindings = fixture.bindings();
    bindings.profile_resolver = Arc::new(Catalog(core_host::Catalog::default()));
    bindings.router = Arc::new(core_host::Router {
        snapshot,
        queries: AtomicUsize::new(0),
        snapshots: AtomicUsize::new(0),
    });
    bindings.model_exchange = Arc::new(
        ModelExchange::new(Arc::new(XaiModel::new(connection)), bindings.policy.clone())
            .with_route_inspector(Arc::new(Inspector), Duration::from_secs(1))
            .unwrap()
            .with_retry_policy(ModelRetryPolicy {
                max_retries: 1,
                backoff_ms: 0,
            }),
    );
    bindings.system_inputs = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    let tool = SchemaCompiler::new().compile(ToolDescriptor {
        tool: reference("lookup"), name: id("lookup"), description: "Read scoped data".into(),
        input_schema: json!({"type":"object","properties":{
            "query":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":10,"default":7},"note":{"type":["string","null"]},
            "filter":{"type":"object","properties":{"category":{"type":"string"},"term":false},"required":["category"],"additionalProperties":false},
            "workspace_id":{"type":"string","format":"uuid"}
        },"required":["query","workspace_id"],"additionalProperties":false,"if":{"properties":{"query":{"const":"latest"}}},"then":{"properties":{"limit":{"minimum":8}}}}),
        agent_parameters: vec!["query".into(),"limit".into(),"note".into(),"filter".into()], system_bindings: None,
        output_schema: json!({"type":"string"}), side_effect: ToolSideEffect::ReadOnly, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false, max_output_bytes: 4096.try_into().unwrap(),
    }, &bindings.system_inputs).unwrap();
    let wire_definition = tool.to_model_tool();
    let capture = Arc::new(Capture(Mutex::new(vec![])));
    bindings.tools = Some(Arc::new(
        ToolRegistry::new(
            scope(),
            vec![ToolRegistration {
                compiled: tool,
                executor: capture.clone(),
            }],
        )
        .unwrap(),
    ));
    let mut profile = core_host::profile();
    profile.limits.max_model_calls = 8.try_into().unwrap();
    profile.limits.max_recovery_attempts = 1;
    profile
        .model_options
        .insert("reasoning_effort".into(), json!("low"));
    profile.limits.max_tool_attempts = 2;
    profile.limits.max_repair_attempts = 4;
    profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
        tool_id: id("lookup"),
        version: id("1"),
        bindings: None,
        config: None,
    }));
    let agent = create_agent(profile, bindings).unwrap();
    let mut context = core_host::context();
    context.data.system_inputs = Some(SystemInputs::new(
        [("workspace_id".into(), json!(WORKSPACE))].into(),
    ));
    let mut request = core_host::request("contract");
    request
        .model_options
        .insert("reasoning_effort".into(), json!("medium"));
    let handle = completed(agent.start(request, context.clone()).await.unwrap());
    let outcome = completed(
        tokio::time::timeout(Duration::from_secs(10), handle.outcome(&context))
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        outcome.result.status(),
        RunStatus::Succeeded,
        "{:?}",
        outcome.result
    );
    assert_eq!(outcome.usage.repair_attempts, 4);
    assert_eq!(outcome.usage.recovery_attempts, 1);
    assert_eq!(outcome.usage.model_calls, 7);
    assert_eq!(outcome.usage.tool_attempts, 1);
    assert_eq!(
        capture.0.lock().unwrap().as_slice(),
        &[JsonObject::from([
            ("query".into(), json!("latest")),
            ("limit".into(), json!(9)),
            ("note".into(), Value::Null),
            ("filter".into(), json!({"category":"finance"})),
            ("workspace_id".into(), json!(WORKSPACE))
        ])]
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 7);
    assert_eq!(requests[0].body, requests[1].body);
    for request in requests.iter() {
        let declaration = &request.body["tools"][0];
        assert!(declaration.get("strict").is_none());
        assert_eq!(declaration["parameters"]["required"], json!(["query"]));
        assert!(
            declaration["parameters"]["properties"]
                .get("workspace_id")
                .is_none()
        );
        assert_eq!(
            declaration["parameters"]["properties"]["filter"]["type"],
            "string"
        );
        assert_eq!(request.path, "/v1/responses");
        assert!(
            request
                .headers
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-key-not-a-secret")
        );
        assert_eq!(request.body["reasoning"]["effort"], "medium");
        assert!(!request.body.to_string().contains(WORKSPACE));
    }
    let items = requests[6].body["input"].as_array().unwrap();
    let replayed: Vec<_> = items
        .iter()
        .filter(|item| item["type"] == "function_call")
        .map(|item| item["arguments"].as_str().unwrap())
        .collect();
    assert_eq!(
        replayed,
        vec![
            malformed,
            rounded,
            invalid_constraint.as_str(),
            invalid_json.as_str(),
            valid.as_str()
        ]
    );
    let encrypted: Vec<_> = items
        .iter()
        .filter(|item| item["type"] == "reasoning")
        .map(|item| item["encrypted_content"].as_str().unwrap())
        .collect();
    assert_eq!(
        encrypted,
        vec![
            "ciphertext-0",
            "ciphertext-4",
            "ciphertext-1",
            "ciphertext-2",
            "ciphertext-3"
        ]
    );
    assert!(wire_definition.model_input_schema.get("if").is_some());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
}
```

## `crates/wickle-model-xai/tests/schema.rs`

```rust
//! Provider schema semantics through compiled contracts and the actual HTTP encoder.
#[allow(dead_code)]
mod support;
use serde_json::{Value, json};
use support::*;
use wickle::*;
use wickle_model_xai::*;
fn original(schema: Value) -> CompiledTool {
    let parameters = schema["properties"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    SchemaCompiler::new()
        .compile(
            ToolDescriptor {
                tool: reference("lookup"),
                name: id("lookup"),
                description: "Read scoped data".into(),
                input_schema: schema,
                agent_parameters: parameters,
                system_bindings: None,
                output_schema: json!({"type":"string"}),
                side_effect: ToolSideEffect::ReadOnly,
                concurrency: ToolConcurrency::Serial,
                retry: ToolRetryPolicy::Never,
                reconcile: false,
                max_output_bytes: 4096.try_into().unwrap(),
            },
            &SystemInputRegistry::new(vec![]).unwrap(),
        )
        .unwrap()
}
#[tokio::test]
async fn optional_null_open_objects_and_single_intersections_keep_canonical_meaning_on_wire() {
    let server = Server::new(vec![Reply::sse(&events(MODEL, "done"))]).await;
    let connection = connection(&server);
    let model = XaiModel::new(connection.clone());
    let mut request = request(&connection, MODEL);
    let original = original(json!({"type":"object","properties":{
        "query":{"type":"string","pattern":"foo"},
        "note":{"type":["string","null"]},
        "limit":{"allOf":[{"type":"integer","minimum":1,"maximum":10}]},
        "open":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]},
        "anything":true
    },"required":["query"],"additionalProperties":false}));
    let contract = CompiledToolContract::compile(
        &original,
        ProviderToolTarget::for_route(&request.route),
        model.tool_schema_compiler().as_ref(),
        Default::default(),
    )
    .unwrap();
    for canonical in [
        JsonObject::from([("query".into(), json!("prefixfoosuffix"))]),
        JsonObject::from([
            ("query".into(), json!("foo")),
            ("note".into(), Value::Null),
            ("limit".into(), json!(3)),
            ("open".into(), json!({"name":"record","unlisted":42})),
            ("anything".into(), json!([true, 3, null])),
        ]),
    ] {
        original.validate_model_inputs(&canonical).unwrap();
        let wire = contract.encode_arguments(&canonical).unwrap();
        assert_eq!(
            contract
                .decode_arguments(&serde_json::to_string(&wire).unwrap(), Default::default())
                .unwrap(),
            canonical
        );
    }
    for invalid in [r#"{"query":"absent"}"#, r#"{"query":"foo","limit":0}"#] {
        let values = contract
            .decode_arguments(invalid, Default::default())
            .unwrap();
        assert!(original.validate_model_inputs(&values).is_err());
    }
    request.tools = vec![contract.wire_tool().clone()];
    for fragment in contract.constraint_fragments() {
        request.messages[0].content.push(ModelContent::Text {
            text: fragment.text.clone(),
        });
    }
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let calls = server.requests.lock().unwrap();
    let declaration = &calls[0].body["tools"][0];
    let wire = &declaration["parameters"];
    assert!(declaration.get("strict").is_none());
    assert_eq!(wire["required"], json!(["query"]));
    assert_eq!(
        wire["properties"]["note"]["type"],
        json!(["string", "null"])
    );
    assert_eq!(wire["properties"]["open"]["additionalProperties"], true);
    assert_eq!(wire["properties"]["limit"]["allOf"][0]["minimum"], 1);
    assert_eq!(wire["properties"]["limit"]["allOf"][0]["maximum"], 10);
    assert!(wire["properties"]["query"].get("pattern").is_none());
    assert_eq!(wire["properties"]["anything"]["type"], "string");
    assert!(
        contract
            .enforcement()
            .iter()
            .any(|rule| rule.canonical_pointer == "/properties/query/pattern"
                && rule.core
                && rule.context_text
                && !rule.provider_native)
    );
}
#[tokio::test]
async fn target_mismatch_and_non_disableable_reasoning_fail_before_http() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let model = XaiModel::new(connection.clone());
    let mut request = request(&connection, "grok-4.7");
    let original = original(
        json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}),
    );
    let mut target = ProviderToolTarget::for_route(&request.route);
    target.provider = id("openai");
    assert!(
        CompiledToolContract::compile(
            &original,
            target,
            model.tool_schema_compiler().as_ref(),
            Default::default()
        )
        .is_err()
    );
    request
        .options
        .insert("reasoning_effort".into(), json!("none"));
    assert!(
        collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .is_err()
    );
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn compact_branching_references_exhaust_a_shared_budget_and_fall_back_without_expanding() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let request = request(&connection, MODEL);
    let mut defs = serde_json::Map::new();
    defs.insert("level7".into(), json!({"type":"string"}));
    for level in (0..7).rev() {
        let props: serde_json::Map<String, Value> = (0..4)
            .map(|branch| {
                (
                    format!("branch{branch}"),
                    json!({"$ref":format!("#/$defs/level{}",level+1)}),
                )
            })
            .collect();
        let required: Vec<_> = props.keys().cloned().collect();
        defs.insert(format!("level{level}"),json!({"type":"object","properties":props,"required":required,"additionalProperties":false}));
    }
    let tool = ModelTool {
        name: id("lookup"),
        description: "Lookup".into(),
        model_input_schema: json!({"type":"object","properties":{"query":{"$ref":"#/$defs/level0"}},"required":["query"],"additionalProperties":false,"$defs":defs}),
    };
    let compiler = XaiToolSchemaCompiler;
    let projection = compiler
        .compile(&tool, &ProviderToolTarget::for_route(&request.route))
        .unwrap();
    assert_eq!(
        projection.wire_tool.model_input_schema["properties"]["query"],
        json!({"type":"string"})
    );
    let ArgumentDecodePlan::Fields { fields } = projection.decode_plan else {
        panic!("missing field codec")
    };
    assert!(matches!(
        fields[0].encoding,
        ArgumentValueEncoding::JsonText { optional: false }
    ));
}

#[tokio::test]
async fn recursive_shapes_and_conflicting_reference_siblings_roundtrip_without_overwriting_rules() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let request = request(&connection, MODEL);
    let canonical = original(json!({"type":"object","properties":{
        "bounded":{"$ref":"#/$defs/number","minimum":3},
        "tree":{"$ref":"#/$defs/node"}
    },"required":["bounded"],"additionalProperties":false,"$defs":{
        "number":{"type":"integer","minimum":1,"maximum":10},
        "node":{"type":"object","properties":{"next":{"$ref":"#/$defs/node"}},"additionalProperties":false}
    }}));
    let contract = CompiledToolContract::compile(
        &canonical,
        ProviderToolTarget::for_route(&request.route),
        &XaiToolSchemaCompiler,
        Default::default(),
    )
    .unwrap();
    let values = JsonObject::from([
        ("bounded".into(), json!(3)),
        ("tree".into(), json!({"next":{}})),
    ]);
    let encoded = contract.encode_arguments(&values).unwrap();
    assert!(encoded["bounded"].is_string());
    assert!(encoded["tree"].is_string());
    let decoded = contract
        .decode_arguments(
            &serde_json::to_string(&encoded).unwrap(),
            Default::default(),
        )
        .unwrap();
    assert_eq!(decoded, values);
    canonical.validate_model_inputs(&decoded).unwrap();
    for value in [2, 11] {
        let invalid = JsonObject::from([("bounded".into(), json!(value))]);
        let encoded = contract.encode_arguments(&invalid).unwrap();
        let decoded = contract
            .decode_arguments(
                &serde_json::to_string(&encoded).unwrap(),
                Default::default(),
            )
            .unwrap();
        assert!(canonical.validate_model_inputs(&decoded).is_err());
    }
}
```

## `crates/wickle/src/provider_tool_schema.rs`

```rust
//! Pure, bounded provider projection of an already separated Tool input contract.
use crate::{
    ApiContract, CompiledTool, ContractError, ErrorCode, Id, JsonDigest, JsonObject, ModelTool,
    VersionedRef, canonical_digest, parse_json, serialization::data_digest,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, fmt};

/// Exact provider protocol and capability revision used for compilation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderToolTarget {
    /// Exact model/release, absent only in older or manually unqualified contracts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<VersionedRef>,
    /// Provider namespace, including deployment-specific provider adapters.
    pub provider: Id,
    /// Exact operation and API version.
    pub api_contract: ApiContract,
    /// Pinned target capability revision.
    pub capability_revision: Id,
}
impl ProviderToolTarget {
    /// Capture the selected route without connection metadata or credentials.
    pub fn for_route(route: &crate::ResolvedModelRoute) -> Self {
        Self {
            model: Some(VersionedRef {
                id: route.model_id.clone(),
                version: route.model_version.clone(),
            }),
            provider: route.provider.clone(),
            api_contract: route.api_contract.clone(),
            capability_revision: route.capability_revision.clone(),
        }
    }
    fn matches_saved(&self, saved: &Self) -> bool {
        self.provider == saved.provider
            && self.api_contract == saved.api_contract
            && self.capability_revision == saved.capability_revision
            && saved
                .model
                .as_ref()
                .is_none_or(|model| self.model.as_ref() == Some(model))
    }
}
/// Finite bounds on compilation, persisted projection and incoming arguments.
#[derive(Debug, Clone, Copy)]
pub struct ProviderToolSchemaLimits {
    /// Maximum serialized canonical or wire schema/tool bytes.
    pub max_schema_bytes: usize,
    /// Maximum schema nesting before traversal or serialization.
    pub max_schema_depth: usize,
    /// Maximum total serialized compiled contract bytes, including explanations.
    pub max_contract_bytes: usize,
    /// Maximum provider argument bytes before parsing.
    pub max_argument_bytes: usize,
}
impl Default for ProviderToolSchemaLimits {
    fn default() -> Self {
        Self {
            max_schema_bytes: 65_536,
            max_schema_depth: 64,
            max_contract_bytes: 262_144,
            max_argument_bytes: 65_536,
        }
    }
}
/// Reversible representation of a single model-owned field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArgumentValueEncoding {
    /// Preserve the JSON value, including explicit null.
    Identity {},
    /// A JSON document encoded as a string. Optional values use [] for omission
    /// and `[value]` for a supplied value, keeping explicit null distinct.
    JsonText {
        /// Whether the string represents an optional zero-or-one value array.
        optional: bool,
    },
    /// Encode omission separately from null using an object envelope.
    Presence {
        /// Boolean discriminator: false means omitted, true means supplied.
        present_key: String,
        /// Required value member; must be null when present is false.
        value_key: String,
    },
}
/// One-to-one mapping from a wire property to an exposed canonical property.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArgumentFieldMapping {
    /// Property emitted by the provider.
    pub wire_name: String,
    /// Original model-owned property, never a system-owned property.
    pub canonical_name: String,
    /// Value and omission restoration rule.
    pub encoding: ArgumentValueEncoding,
}
/// Stored codec. It never guesses that null means omission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArgumentDecodePlan {
    /// Property names and values are unchanged.
    Identity {},
    /// Entire canonical argument object encoded as one JSON string property.
    JsonObjectText {
        /// The sole wire property; its decoded value must be an object.
        wire_name: String,
    },
    /// Explicit complete field mapping; unknown wire properties are errors.
    Fields {
        /// Ordered mappings with unique wire and canonical names.
        fields: Vec<ArgumentFieldMapping>,
    },
}
/// Compiler output before the core stamps original identity and explanations.
#[derive(Debug, Clone)]
pub struct ProviderToolProjection {
    /// The exact Tool definition submitted to the provider.
    pub wire_tool: ModelTool,
    /// Reversible normalization back into original model-owned arguments.
    pub decode_plan: ArgumentDecodePlan,
}
/// Pure trusted adapter extension. Input contains only the model-visible Tool;
/// hidden definitions, system values, credentials and runtime handles are absent.
pub trait ProviderToolSchemaCompiler: Send + Sync {
    /// Immutable implementation identity; change its version when output changes.
    fn reference(&self) -> VersionedRef;
    /// Preserve native constraints where supported. Unsupported representation
    /// must use a relaxed schema plus a reversible codec, never delete the Tool.
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError>;
}
/// Compiler for protocols that accept the original model-visible JSON Schema.
#[derive(Debug, Clone, Copy, Default)]
pub struct NativeToolSchemaCompiler;
impl ProviderToolSchemaCompiler for NativeToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new("wickle-native-tool-schema").expect("static id"),
            version: Id::new("1").expect("static version"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
        _: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        Ok(ProviderToolProjection {
            wire_tool: tool.clone(),
            decode_plan: ArgumentDecodePlan::Identity {},
        })
    }
}
/// Deterministic trusted explanation associated with this exact Tool projection.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolConstraintFragment {
    /// Stable content-addressed identity, ordered in the compiled contract.
    pub id: Id,
    /// Only canonical model-visible schema and codec instructions.
    pub text: String,
    /// Digest of the exact explanation text.
    pub digest: JsonDigest,
}
impl fmt::Debug for ToolConstraintFragment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolConstraintFragment")
            .field("id", &self.id)
            .field("digest", &self.digest)
            .finish()
    }
}
/// Where an original schema node is enforced. Core validation is always required.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolConstraintEnforcement {
    /// JSON pointer into the canonical model schema; empty denotes the whole schema.
    pub canonical_pointer: String,
    /// Confirmed native under an identical schema and identity codec. False is
    /// conservative: a relaxed wire schema can still enforce part of this node.
    pub provider_native: bool,
    /// Included in the canonical constraint explanation.
    pub context_text: bool,
    /// Original validation must occur after decoding, before execution.
    pub core: bool,
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContractData {
    schema_version: String,
    tool: VersionedRef,
    canonical_name: Id,
    descriptor_digest: JsonDigest,
    canonical_schema_digest: JsonDigest,
    compiler: VersionedRef,
    target: ProviderToolTarget,
    wire_tool: ModelTool,
    decode_plan: ArgumentDecodePlan,
    fragments: Vec<ToolConstraintFragment>,
    enforcement: Vec<ToolConstraintEnforcement>,
}
/// Immutable route-specific contract. Serialize only to protected storage; submit
/// wire_tool and constraint_fragments to the model, not the whole record.
#[derive(Clone, Serialize)]
pub struct CompiledToolContract {
    data: ContractData,
    digest: JsonDigest,
}
impl fmt::Debug for CompiledToolContract {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompiledToolContract")
            .field("tool", &self.data.tool)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}
impl CompiledToolContract {
    /// Compile only after canonical ownership separation and validate finite output.
    pub fn compile(
        tool: &CompiledTool,
        target: ProviderToolTarget,
        compiler: &dyn ProviderToolSchemaCompiler,
        limits: ProviderToolSchemaLimits,
    ) -> Result<Self, ContractError> {
        check_limits(limits)?;
        let visible = tool.to_model_tool();
        depth_bound(&visible.model_input_schema, limits.max_schema_depth)?;
        bounded(&visible, limits.max_schema_bytes)?;
        let reference = compiler.reference();
        let projection = compiler.compile(&visible, &target)?;
        if compiler.reference() != reference {
            return Err(invalid("provider_tool.compiler_revision"));
        }
        Self::build(
            tool,
            target,
            reference,
            projection,
            limits,
            "wickle.provider-tool-contract.v2",
        )
    }
    fn build(
        tool: &CompiledTool,
        target: ProviderToolTarget,
        compiler: VersionedRef,
        projection: ProviderToolProjection,
        limits: ProviderToolSchemaLimits,
        schema_version: &str,
    ) -> Result<Self, ContractError> {
        check_limits(limits)?;
        if !matches!(
            schema_version,
            "wickle.provider-tool-contract.v1" | "wickle.provider-tool-contract.v2"
        ) {
            return Err(invalid("provider_tool.schema_version"));
        }
        depth_bound(tool.model_input_schema(), limits.max_schema_depth)?;
        depth_bound(
            &projection.wire_tool.model_input_schema,
            limits.max_schema_depth,
        )?;
        bounded(&projection.wire_tool, limits.max_schema_bytes)?;
        let schema = &projection.wire_tool.model_input_schema;
        if !valid_name(projection.wire_tool.name.as_str())
            || schema.get("type") != Some(&json!("object"))
            || schema.get("additionalProperties") != Some(&json!(false))
        {
            return Err(invalid("provider_tool.wire_boundary"));
        }
        crate::tool_schema::compile_validator(schema)?;
        validate_codec(tool.model_input_schema(), schema, &projection.decode_plan)?;
        let identity = matches!(projection.decode_plan, ArgumentDecodePlan::Identity {});
        let explained = schema != tool.model_input_schema() || !identity;
        let fragments = if explained {
            let mut text = format!(
                "Tool {}: arguments must satisfy this canonical JSON Schema after decoding: {}\nDecode representation: {}. Field mappings restore wire_name to canonical_name. For a presence envelope, both members are required: true marks a supplied value (including explicit null); false with a null value placeholder means omission. Preserve omission and explicit null as distinct values.",
                projection.wire_tool.name,
                serde_json::to_string(tool.model_input_schema())
                    .map_err(|_| invalid("provider_tool.schema"))?,
                serde_json::to_string(&projection.decode_plan)
                    .map_err(|_| invalid("provider_tool.codec"))?
            );
            if schema_version == "wickle.provider-tool-contract.v1" {
                if matches!(&projection.decode_plan, ArgumentDecodePlan::Fields { fields } if fields.iter().any(|field| matches!(field.encoding, ArgumentValueEncoding::JsonText { .. })))
                {
                    text.push_str(" For json_text, the wire value is a JSON string parsed by the core. With optional=false it encodes the canonical value itself. With optional=true it must encode [] for omission or [value] for a supplied value, including [null] for explicit null. Nested optional properties remain absent inside that JSON document; do not replace absence with null.");
                }
                if matches!(
                    &projection.decode_plan,
                    ArgumentDecodePlan::JsonObjectText { .. }
                ) {
                    text.push_str(" For json_object_text, send exactly the named wire property as a JSON string containing the entire canonical argument object. Preserve absent properties and explicit null values inside it; do not include system-owned fields.");
                }
            } else {
                text = format!(
                    "Tool {}: arguments must satisfy this canonical JSON Schema after decoding: {}\nDecode representation: {}. Follow the declared wire schema and each field's encoding. Preserve omission and explicit null as distinct values.",
                    projection.wire_tool.name,
                    serde_json::to_string(tool.model_input_schema())
                        .map_err(|_| invalid("provider_tool.schema"))?,
                    serde_json::to_string(&projection.decode_plan)
                        .map_err(|_| invalid("provider_tool.codec"))?
                );
                match &projection.decode_plan {
                    ArgumentDecodePlan::Identity {} => text.push_str(" Identity values are sent directly as canonical values; do not stringify them or add an envelope."),
                    ArgumentDecodePlan::Fields { fields } => {
                        if fields.iter().any(|field|matches!(field.encoding, ArgumentValueEncoding::Identity {})) {
                            text.push_str(" Fields marked identity use the canonical value directly. Do not stringify it or add an envelope. Omit absent optional fields; send JSON null for an explicit null.");
                        }
                        if fields.iter().any(|field|matches!(field.encoding, ArgumentValueEncoding::Presence { .. })) {
                            text.push_str(" Only fields marked presence use the specified present_key and value_key envelope. Both members are required: true marks a supplied value (including null); false with a null placeholder means omission.");
                        }
                        if fields.iter().any(|field|matches!(field.encoding, ArgumentValueEncoding::JsonText { optional: false })) {
                            text.push_str(" Only fields marked json_text with optional=false use a JSON string encoding the canonical value itself. Nested absent properties stay absent.");
                        }
                        if fields.iter().any(|field|matches!(field.encoding, ArgumentValueEncoding::JsonText { optional: true })) {
                            text.push_str(" Only fields marked json_text with optional=true use a JSON string encoding [] for omission or [value] for a supplied value, including [null] for explicit null. Nested absent properties stay absent.");
                        }
                    }
                    ArgumentDecodePlan::JsonObjectText { .. } => text.push_str(" Send exactly the named wire property as a JSON string containing the entire canonical argument object. Preserve absent properties and explicit null; do not include system-owned fields."),
                }
            }
            let digest = data_digest(&text);
            vec![ToolConstraintFragment {
                id: Id::new(format!(
                    "tool-constraints-{}",
                    canonical_digest(&json!(text))
                ))?,
                text,
                digest,
            }]
        } else {
            vec![]
        };
        let mut enforcement = Vec::new();
        collect_enforcement(
            tool.model_input_schema(),
            schema,
            "",
            identity && schema == tool.model_input_schema(),
            explained,
            &mut enforcement,
        );
        let data = ContractData {
            schema_version: schema_version.into(),
            tool: tool.descriptor().tool.clone(),
            canonical_name: tool.descriptor().name.clone(),
            descriptor_digest: tool.descriptor_digest().clone(),
            canonical_schema_digest: tool.model_schema_digest().clone(),
            compiler,
            target,
            wire_tool: projection.wire_tool,
            decode_plan: projection.decode_plan,
            fragments,
            enforcement,
        };
        let result = Self {
            digest: data_digest(&data),
            data,
        };
        bounded(&result, limits.max_contract_bytes)?;
        Ok(result)
    }
    /// Restore against the trusted original Tool, destination and expected digest.
    /// This uses the saved codec and never invokes a newer compiler implementation.
    pub fn restore(
        text: &str,
        tool: &CompiledTool,
        target: &ProviderToolTarget,
        expected: &JsonDigest,
        limits: ProviderToolSchemaLimits,
    ) -> Result<Self, ContractError> {
        check_limits(limits)?;
        if text.len() > limits.max_contract_bytes {
            return Err(invalid("provider_tool.size"));
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Saved {
            data: ContractData,
            digest: JsonDigest,
        }
        let saved: Saved = serde_json::from_value(parse_json(text)?)
            .map_err(|_| invalid("provider_tool.record"))?;
        if &saved.digest != expected
            || data_digest(&saved.data) != *expected
            || !target.matches_saved(&saved.data.target)
        {
            return Err(invalid("provider_tool.identity"));
        }
        let rebuilt = Self::build(
            tool,
            saved.data.target.clone(),
            saved.data.compiler.clone(),
            ProviderToolProjection {
                wire_tool: saved.data.wire_tool.clone(),
                decode_plan: saved.data.decode_plan.clone(),
            },
            limits,
            &saved.data.schema_version,
        )?;
        if rebuilt.data != saved.data || rebuilt.digest != *expected {
            return Err(invalid("provider_tool.identity"));
        }
        Ok(rebuilt)
    }
    pub(crate) fn inspection(
        value: Value,
    ) -> Result<crate::inspection::SavedToolInspection, ContractError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Saved {
            data: ContractData,
            digest: JsonDigest,
        }
        let saved: Saved =
            serde_json::from_value(value).map_err(|_| invalid("provider_tool.record"))?;
        if !matches!(
            saved.data.schema_version.as_str(),
            "wickle.provider-tool-contract.v1" | "wickle.provider-tool-contract.v2"
        ) || data_digest(&saved.data) != saved.digest
        {
            return Err(invalid("provider_tool.identity"));
        }
        Ok(crate::inspection::SavedToolInspection {
            tool: saved.data.tool,
            canonical_name: saved.data.canonical_name,
            canonical_schema_digest: saved.data.canonical_schema_digest,
            compiler: saved.data.compiler,
            target: saved.data.target,
            wire_tool: saved.data.wire_tool,
            decode_plan_digest: data_digest(&saved.data.decode_plan),
            digest: saved.digest,
            fragments: saved.data.fragments,
            enforcement: saved.data.enforcement,
        })
    }
    /// Original model-facing Tool name to restore after provider name mapping.
    pub fn canonical_name(&self) -> &Id {
        &self.data.canonical_name
    }
    /// Original registered Tool identity.
    pub fn tool(&self) -> &VersionedRef {
        &self.data.tool
    }
    /// Frozen provider-facing Tool, excluding hidden input metadata.
    pub fn wire_tool(&self) -> &ModelTool {
        &self.data.wire_tool
    }
    /// Ordered trusted fragments that must accompany the Tool definition.
    pub fn constraint_fragments(&self) -> &[ToolConstraintFragment] {
        &self.data.fragments
    }
    /// Exact original constraint locations and enforcement mechanisms.
    pub fn enforcement(&self) -> &[ToolConstraintEnforcement] {
        &self.data.enforcement
    }
    /// Pinned compiler identity and version.
    pub fn compiler(&self) -> &VersionedRef {
        &self.data.compiler
    }
    /// Exact destination protocol/capability revision.
    pub fn target(&self) -> &ProviderToolTarget {
        &self.data.target
    }
    /// Protected compilation identity.
    pub fn digest(&self) -> &JsonDigest {
        &self.digest
    }
    /// Encode canonical historical model arguments for this exact provider
    /// representation. System inputs never belong in this map.
    pub fn encode_arguments(&self, input: &JsonObject) -> Result<JsonObject, ContractError> {
        match &self.data.decode_plan {
            ArgumentDecodePlan::Identity {} => Ok(input.clone()),
            ArgumentDecodePlan::JsonObjectText { wire_name } => Ok(JsonObject::from([(
                wire_name.clone(),
                Value::String(serde_json::to_string(input).map_err(|_| arguments())?),
            )])),
            ArgumentDecodePlan::Fields { fields } => {
                if input
                    .keys()
                    .any(|key| !fields.iter().any(|field| &field.canonical_name == key))
                {
                    return Err(arguments());
                }
                let mut output = JsonObject::new();
                for field in fields {
                    let value = input.get(&field.canonical_name);
                    match &field.encoding {
                        ArgumentValueEncoding::Identity {} => {
                            if let Some(value) = value {
                                output.insert(field.wire_name.clone(), value.clone());
                            }
                        }
                        ArgumentValueEncoding::JsonText { optional } => {
                            if *optional || value.is_some() {
                                let encoded = if *optional {
                                    serde_json::to_string(&value.into_iter().collect::<Vec<_>>())
                                } else {
                                    serde_json::to_string(value.expect("present value"))
                                }
                                .map_err(|_| arguments())?;
                                output.insert(field.wire_name.clone(), Value::String(encoded));
                            }
                        }
                        ArgumentValueEncoding::Presence {
                            present_key,
                            value_key,
                        } => {
                            let envelope = serde_json::Map::from_iter([
                                (present_key.clone(), Value::Bool(value.is_some())),
                                (value_key.clone(), value.cloned().unwrap_or(Value::Null)),
                            ]);
                            output.insert(field.wire_name.clone(), Value::Object(envelope));
                        }
                    }
                }
                Ok(output)
            }
        }
    }
    /// Restore model-owned names and values. Validation/defaults/system binding
    /// are separate boundaries; this does not authorize or execute the Tool.
    pub fn decode_arguments(
        &self,
        raw: &str,
        limits: ProviderToolSchemaLimits,
    ) -> Result<JsonObject, ContractError> {
        check_limits(limits)?;
        let object = parse_provider_arguments(raw, limits.max_argument_bytes)?;
        match &self.data.decode_plan {
            ArgumentDecodePlan::Identity {} => Ok(object.clone()),
            ArgumentDecodePlan::JsonObjectText { wire_name } => {
                if object.len() != 1 {
                    return Err(arguments());
                }
                let text = object
                    .get(wire_name)
                    .and_then(Value::as_str)
                    .ok_or_else(arguments)?;
                parse_provider_arguments(text, limits.max_argument_bytes)
            }
            ArgumentDecodePlan::Fields { fields } => {
                let mut result = JsonObject::new();
                for (name, value) in &object {
                    let mapping = fields
                        .iter()
                        .find(|field| &field.wire_name == name)
                        .ok_or_else(arguments)?;
                    let restored = match &mapping.encoding {
                        ArgumentValueEncoding::Identity {} => Some(value.clone()),
                        ArgumentValueEncoding::JsonText { optional } => {
                            let text = value.as_str().ok_or_else(arguments)?;
                            let parsed = parse_provider_value(text, limits.max_argument_bytes)?;
                            if *optional {
                                let values = parsed.as_array().ok_or_else(arguments)?;
                                match values.len() {
                                    0 => None,
                                    1 => Some(values[0].clone()),
                                    _ => return Err(arguments()),
                                }
                            } else {
                                Some(parsed)
                            }
                        }
                        ArgumentValueEncoding::Presence {
                            present_key,
                            value_key,
                        } => {
                            let envelope = value.as_object().ok_or_else(arguments)?;
                            match envelope.get(present_key).and_then(Value::as_bool) {
                                Some(false)
                                    if envelope.len() == 2
                                        && envelope.get(value_key) == Some(&Value::Null) =>
                                {
                                    None
                                }
                                Some(true) if envelope.len() == 2 => {
                                    Some(envelope.get(value_key).ok_or_else(arguments)?.clone())
                                }
                                _ => return Err(arguments()),
                            }
                        }
                    };
                    if let Some(value) = restored {
                        result.insert(mapping.canonical_name.clone(), value);
                    }
                }
                Ok(result)
            }
        }
    }
}
fn validate_codec(
    canonical: &Value,
    wire: &Value,
    plan: &ArgumentDecodePlan,
) -> Result<(), ContractError> {
    let canonical = canonical
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("provider_tool.canonical_properties"))?;
    let wire = wire
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("provider_tool.wire_properties"))?;
    match plan {
        ArgumentDecodePlan::Identity {} if canonical.keys().eq(wire.keys()) => Ok(()),
        ArgumentDecodePlan::JsonObjectText { wire_name }
            if wire.len() == 1
                && wire.get(wire_name).and_then(|value| value.get("type"))
                    == Some(&json!("string")) =>
        {
            Ok(())
        }
        ArgumentDecodePlan::Fields { fields } => {
            let mut from = BTreeSet::new();
            let mut to = BTreeSet::new();
            for field in fields {
                if !wire.contains_key(&field.wire_name)
                    || !canonical.contains_key(&field.canonical_name)
                    || !from.insert(&field.wire_name)
                    || !to.insert(&field.canonical_name)
                {
                    return Err(invalid("provider_tool.codec_mapping"));
                }
                if let ArgumentValueEncoding::Presence {
                    present_key,
                    value_key,
                } = &field.encoding
                {
                    if present_key.is_empty() || value_key.is_empty() || present_key == value_key {
                        return Err(invalid("provider_tool.presence_keys"));
                    }
                }
            }
            if from.len() != wire.len() || to.len() != canonical.len() {
                return Err(invalid("provider_tool.codec_coverage"));
            }
            Ok(())
        }
        _ => Err(invalid("provider_tool.codec_mapping")),
    }
}
fn collect_enforcement(
    canonical: &Value,
    wire: &Value,
    pointer: &str,
    identity: bool,
    text: bool,
    output: &mut Vec<ToolConstraintEnforcement>,
) {
    output.push(ToolConstraintEnforcement {
        canonical_pointer: pointer.into(),
        provider_native: identity && wire.pointer(pointer) == Some(canonical),
        context_text: text,
        core: true,
    });
    let Some(map) = canonical.as_object() else {
        return;
    };
    for (key, value) in map {
        if matches!(
            key.as_str(),
            "title"
                | "description"
                | "default"
                | "examples"
                | "$comment"
                | "$schema"
                | "$id"
                | "deprecated"
                | "readOnly"
                | "writeOnly"
        ) {
            continue;
        }
        let path = format!("{pointer}/{}", key.replace('~', "~0").replace('/', "~1"));
        match key.as_str() {
            "properties" | "$defs" | "definitions" | "dependentSchemas" | "patternProperties" => {
                if let Some(children) = value.as_object() {
                    for (name, child) in children {
                        collect_enforcement(
                            child,
                            wire,
                            &format!("{path}/{}", name.replace('~', "~0").replace('/', "~1")),
                            identity,
                            text,
                            output,
                        );
                    }
                }
            }
            "allOf" | "anyOf" | "oneOf" | "prefixItems" => {
                if let Some(children) = value.as_array() {
                    for (index, child) in children.iter().enumerate() {
                        collect_enforcement(
                            child,
                            wire,
                            &format!("{path}/{index}"),
                            identity,
                            text,
                            output,
                        );
                    }
                }
                output.push(ToolConstraintEnforcement {
                    canonical_pointer: path.clone(),
                    provider_native: identity && wire.pointer(&path) == Some(value),
                    context_text: text,
                    core: true,
                });
            }
            "items"
            | "additionalProperties"
            | "unevaluatedProperties"
            | "unevaluatedItems"
            | "contains"
            | "not"
            | "if"
            | "then"
            | "else"
            | "propertyNames" => collect_enforcement(value, wire, &path, identity, text, output),
            _ => output.push(ToolConstraintEnforcement {
                canonical_pointer: path.clone(),
                provider_native: identity && wire.pointer(&path) == Some(value),
                context_text: text,
                core: true,
            }),
        }
    }
}

fn bounded(value: &impl Serialize, max: usize) -> Result<(), ContractError> {
    if serde_json::to_vec(value)
        .map_err(|_| invalid("provider_tool.json"))?
        .len()
        > max
    {
        return Err(invalid("provider_tool.size"));
    }
    Ok(())
}
fn check_limits(limits: ProviderToolSchemaLimits) -> Result<(), ContractError> {
    if limits.max_schema_depth == 0
        || limits.max_schema_depth > 128
        || limits.max_schema_bytes == 0
        || limits.max_contract_bytes == 0
        || limits.max_argument_bytes == 0
    {
        return Err(invalid("provider_tool.limits"));
    }
    Ok(())
}
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn invalid(path: &str) -> ContractError {
    ContractError::new(ErrorCode::UnsupportedInputProjection, path)
}
fn arguments() -> ContractError {
    ContractError::new(ErrorCode::InvalidArguments, "provider_tool.arguments")
}

fn depth_bound(schema: &Value, max: usize) -> Result<(), ContractError> {
    let mut pending = vec![(schema, 0)];
    while let Some((value, depth)) = pending.pop() {
        if depth > max {
            return Err(invalid("provider_tool.depth"));
        }
        match value {
            Value::Object(map) => pending.extend(map.values().map(|value| (value, depth + 1))),
            Value::Array(array) => pending.extend(array.iter().map(|value| (value, depth + 1))),
            _ => {}
        }
    }
    Ok(())
}

// The legacy Value parser must remain unchanged for old digests. This new codec
// refuses values it cannot represent, rather than silently rounding model input.
fn numbers_preserved(raw: &serde_json::value::RawValue, parsed: &Value) -> bool {
    use serde_json::value::RawValue;
    match raw.get().as_bytes()[0] {
        b'{' => {
            let Ok(object) =
                serde_json::from_str::<std::collections::BTreeMap<String, &RawValue>>(raw.get())
            else {
                return false;
            };
            object.into_iter().all(|(key, raw)| {
                parsed
                    .get(&key)
                    .is_some_and(|value| numbers_preserved(raw, value))
            })
        }
        b'[' => {
            let Ok(array) = serde_json::from_str::<Vec<&RawValue>>(raw.get()) else {
                return false;
            };
            array.into_iter().enumerate().all(|(index, raw)| {
                parsed
                    .get(index)
                    .is_some_and(|value| numbers_preserved(raw, value))
            })
        }
        b'-' | b'0'..=b'9' => parsed.as_number().is_some_and(|number| {
            normalized_decimal(raw.get())
                .is_some_and(|original| Some(original) == normalized_decimal(&number.to_string()))
        }),
        _ => true,
    }
}
fn normalized_decimal(text: &str) -> Option<(bool, String, i128)> {
    let negative = text.starts_with('-');
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let (mantissa, exponent) = unsigned.split_once(['e', 'E']).unwrap_or((unsigned, "0"));
    let fraction = mantissa
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Some((false, "0".into(), 0));
    }
    let trimmed = digits.trim_end_matches('0');
    let exponent = exponent
        .parse::<i128>()
        .ok()?
        .checked_sub(fraction as i128)?
        .checked_add((digits.len() - trimmed.len()) as i128)?;
    Some((negative, trimmed.into(), exponent))
}

/// Parse model-owned provider arguments without silently rounding number tokens.
pub fn parse_provider_arguments(raw: &str, max_bytes: usize) -> Result<JsonObject, ContractError> {
    Ok(parse_provider_value(raw, max_bytes)?
        .as_object()
        .ok_or_else(arguments)?
        .clone()
        .into_iter()
        .collect())
}
fn parse_provider_value(raw: &str, max_bytes: usize) -> Result<Value, ContractError> {
    if raw.len() > max_bytes {
        return Err(arguments());
    }
    let value = parse_json(raw).map_err(|_| arguments())?;
    let original: &serde_json::value::RawValue =
        serde_json::from_str(raw).map_err(|_| arguments())?;
    if !numbers_preserved(original, &value) {
        return Err(ContractError::new(
            ErrorCode::InvalidArguments,
            "provider_tool.numeric_precision",
        ));
    }
    Ok(value)
}
```

## `crates/wickle/tests/provider_tool_schema.rs`

```rust
//! Provider projection retains original constraints and reversible presence semantics.
use serde_json::{Value, json};
use wickle::*;
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn reference(s: &str) -> VersionedRef {
    VersionedRef {
        id: id(s),
        version: id("1"),
    }
}
fn target() -> ProviderToolTarget {
    ProviderToolTarget {
        model: None,
        provider: id("example"),
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("1"),
        },
        capability_revision: id("caps-1"),
    }
}
fn tool() -> CompiledTool {
    let descriptor = ToolDescriptor::from_json(r##"{
      "tool":{"id":"search","version":"1"},"name":"search","description":"Search",
      "input_schema":{"type":"object","properties":{"query":{"$ref":"#/$defs/Query"},"note":{"type":["string","null"]},"workspace_id":{"type":"string","const":"private-workspace"}},"required":["query","workspace_id"],"additionalProperties":false,
      "$defs":{"Query":{"type":"string","minLength":2},"Private":{"const":"hidden-secret"}}},
      "agent_parameters":["query","note"],"output_schema":{"type":"string"},"max_output_bytes":100
    }"##).unwrap();
    let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    SchemaCompiler::new()
        .compile(descriptor, &registry)
        .unwrap()
}
struct Restricted;
impl ProviderToolSchemaCompiler for Restricted {
    fn reference(&self) -> VersionedRef {
        reference("restricted")
    }
    fn compile(
        &self,
        tool: &ModelTool,
        _: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        // The compiler receives only the model contract, never the full input schema.
        assert!(
            tool.model_input_schema["properties"]
                .get("workspace_id")
                .is_none()
        );
        assert!(tool.model_input_schema["$defs"].get("Private").is_none());
        Ok(ProviderToolProjection {
            wire_tool: ModelTool {
                name: id("wire_search"),
                description: tool.description.clone(),
                model_input_schema: json!({"type":"object","properties":{"q":{"type":"string"},"n":{"type":"object","properties":{"present":{"type":"boolean"},"value":{"type":["string","null"]}},"required":["present","value"],"additionalProperties":false}},"required":["q","n"],"additionalProperties":false}),
            },
            decode_plan: ArgumentDecodePlan::Fields {
                fields: vec![
                    ArgumentFieldMapping {
                        wire_name: "q".into(),
                        canonical_name: "query".into(),
                        encoding: ArgumentValueEncoding::Identity {},
                    },
                    ArgumentFieldMapping {
                        wire_name: "n".into(),
                        canonical_name: "note".into(),
                        encoding: ArgumentValueEncoding::Presence {
                            present_key: "present".into(),
                            value_key: "value".into(),
                        },
                    },
                ],
            },
        })
    }
}
#[test]
fn relaxed_schema_keeps_constraints_and_distinguishes_omission_null_and_values() {
    let tool = tool();
    let limits = ProviderToolSchemaLimits::default();
    let compiled = CompiledToolContract::compile(&tool, target(), &Restricted, limits).unwrap();
    assert_eq!(compiled.canonical_name(), &id("search"));
    assert_eq!(compiled.wire_tool().name, id("wire_search"));
    let omitted = compiled
        .decode_arguments(r#"{"q":"ok","n":{"present":false,"value":null}}"#, limits)
        .unwrap();
    assert!(!omitted.contains_key("note"));
    let null = compiled
        .decode_arguments(r#"{"q":"ok","n":{"present":true,"value":null}}"#, limits)
        .unwrap();
    assert_eq!(null["note"], Value::Null);
    tool.validate_model_inputs(&null).unwrap();
    let invalid = compiled
        .decode_arguments(r#"{"q":"x","n":{"present":false,"value":null}}"#, limits)
        .unwrap();
    assert!(tool.validate_model_inputs(&invalid).is_err());
    for raw in [
        r#"{"q":"ok","n":{"present":false,"value":"unexpected"}}"#,
        r#"{"q":"ok","n":{"present":false}}"#,
        r#"{"q":"ok","n":{"present":true}}"#,
        r#"{"q":"ok","n":null}"#,
        r#"{"q":"ok","workspace_id":"forged"}"#,
        r#"{"q":"a","q":"b"}"#,
    ] {
        assert_eq!(
            compiled.decode_arguments(raw, limits).unwrap_err().code,
            ErrorCode::InvalidArguments
        );
    }
    assert!(
        compiled
            .enforcement()
            .iter()
            .all(|item| item.core && item.context_text)
    );
    let fragment = &compiled.constraint_fragments()[0];
    // Confidentiality check of all model-visible surfaces, not prompt-quality testing.
    let visible = format!(
        "{}{}",
        serde_json::to_string(compiled.wire_tool()).unwrap(),
        fragment.text
    );
    for secret in ["workspace_id", "private-workspace", "hidden-secret"] {
        assert!(!visible.contains(secret));
    }
    assert_eq!(
        fragment.digest,
        versioned_digest_json(
            &serde_json::to_string(&fragment.text).unwrap(),
            CanonicalizationVersion::SortedJsonV1,
            JsonTextLimits::default()
        )
        .unwrap()
    );
}
#[test]
fn native_projection_restores_exactly_and_rejects_changed_destination_or_projection() {
    let tool = tool();
    let limits = ProviderToolSchemaLimits::default();
    let compiled =
        CompiledToolContract::compile(&tool, target(), &NativeToolSchemaCompiler, limits).unwrap();
    assert_eq!(compiled.wire_tool(), &tool.to_model_tool());
    assert!(compiled.constraint_fragments().is_empty());
    let text = serde_json::to_string(&compiled).unwrap();
    CompiledToolContract::restore(&text, &tool, &target(), compiled.digest(), limits).unwrap();
    let mut changed = target();
    changed.capability_revision = id("caps-2");
    assert!(
        CompiledToolContract::restore(&text, &tool, &changed, compiled.digest(), limits).is_err()
    );
    let mut record: Value = serde_json::from_str(&text).unwrap();
    record["data"]["wire_tool"]["description"] = json!("modified");
    assert!(
        CompiledToolContract::restore(
            &record.to_string(),
            &tool,
            &target(),
            compiled.digest(),
            limits
        )
        .is_err()
    );
    assert!(
        CompiledToolContract::compile(
            &tool,
            target(),
            &Restricted,
            ProviderToolSchemaLimits {
                max_contract_bytes: 128,
                ..limits
            }
        )
        .is_err()
    );
    assert!(
        CompiledToolContract::compile(
            &tool,
            target(),
            &Restricted,
            ProviderToolSchemaLimits {
                max_schema_depth: 1,
                ..limits
            }
        )
        .is_err()
    );
}

struct Broken(usize);
impl ProviderToolSchemaCompiler for Broken {
    fn reference(&self) -> VersionedRef {
        reference("broken")
    }
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        let mut projected = Restricted.compile(tool, target)?;
        let ArgumentDecodePlan::Fields { fields } = &mut projected.decode_plan else {
            unreachable!()
        };
        match self.0 {
            0 => fields[0].canonical_name = "workspace_id".into(),
            1 => fields[1].wire_name = fields[0].wire_name.clone(),
            2 => {
                fields.pop();
            }
            3 => {
                fields[1].encoding = ArgumentValueEncoding::Presence {
                    present_key: "value".into(),
                    value_key: "value".into(),
                }
            }
            4 => projected.wire_tool.model_input_schema["additionalProperties"] = json!(true),
            _ => projected.wire_tool.name = id("invalid.name"),
        }
        Ok(projected)
    }
}
#[test]
fn compiler_output_cannot_change_ownership_drop_fields_or_ambiguate_presence() {
    for case in 0..6 {
        assert!(
            CompiledToolContract::compile(
                &tool(),
                target(),
                &Broken(case),
                ProviderToolSchemaLimits::default()
            )
            .is_err()
        );
    }
}

#[test]
fn codecs_never_silently_round_numeric_arguments() {
    let mut descriptor = tool().descriptor().clone();
    descriptor.input_schema["properties"]["note"] = json!({"type":"number"});
    let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    let tool = SchemaCompiler::new()
        .compile(descriptor, &registry)
        .unwrap();
    let limits = ProviderToolSchemaLimits::default();
    let compiled =
        CompiledToolContract::compile(&tool, target(), &NativeToolSchemaCompiler, limits).unwrap();
    for number in [
        "18446744073709551617",
        "0.12345678901234567890123456789",
        "1e-999",
    ] {
        assert!(
            compiled
                .decode_arguments(&format!(r#"{{"query":"ok","note":{number}}}"#), limits)
                .is_err(),
            "changed numeric value: {number}"
        );
    }
    for number in ["18446744073709551615", "0.1", "1e2", "100.00", "-0.0"] {
        let decoded = compiled
            .decode_arguments(&format!(r#"{{"query":"ok","note":{number}}}"#), limits)
            .unwrap();
        tool.validate_model_inputs(&decoded).unwrap();
    }
}

struct JsonTextCompiler;
impl ProviderToolSchemaCompiler for JsonTextCompiler {
    fn reference(&self) -> VersionedRef {
        reference("json-text")
    }
    fn compile(
        &self,
        tool: &ModelTool,
        _: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        Ok(ProviderToolProjection {
            wire_tool: ModelTool {
                name: tool.name.clone(),
                description: tool.description.clone(),
                model_input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"note":{"type":"string"}},"required":["query","note"],"additionalProperties":false}),
            },
            decode_plan: ArgumentDecodePlan::Fields {
                fields: vec![
                    ArgumentFieldMapping {
                        wire_name: "query".into(),
                        canonical_name: "query".into(),
                        encoding: ArgumentValueEncoding::JsonText { optional: false },
                    },
                    ArgumentFieldMapping {
                        wire_name: "note".into(),
                        canonical_name: "note".into(),
                        encoding: ArgumentValueEncoding::JsonText { optional: true },
                    },
                ],
            },
        })
    }
}
#[test]
fn json_text_values_preserve_omission_null_and_numeric_precision() {
    let tool = tool();
    let limits = ProviderToolSchemaLimits::default();
    let contract =
        CompiledToolContract::compile(&tool, target(), &JsonTextCompiler, limits).unwrap();
    let missing = JsonObject::from([("query".into(), json!("facts"))]);
    let encoded = contract.encode_arguments(&missing).unwrap();
    assert_eq!(encoded["note"], json!("[]"));
    assert_eq!(
        contract
            .decode_arguments(&serde_json::to_string(&encoded).unwrap(), limits)
            .unwrap(),
        missing
    );
    let supplied = JsonObject::from([
        ("query".into(), json!("facts")),
        ("note".into(), Value::Null),
    ]);
    let encoded = contract.encode_arguments(&supplied).unwrap();
    assert_eq!(encoded["note"], json!("[null]"));
    assert_eq!(
        contract
            .decode_arguments(&serde_json::to_string(&encoded).unwrap(), limits)
            .unwrap(),
        supplied
    );
    for value in ["null", "[null,null]", "[", "[1.000000000000000001]"] {
        let raw = json!({"query":"\"facts\"","note":value}).to_string();
        assert_eq!(
            contract.decode_arguments(&raw, limits).unwrap_err().code,
            ErrorCode::InvalidArguments
        );
    }
    let record = serde_json::to_string(&contract).unwrap();
    let restored =
        CompiledToolContract::restore(&record, &tool, &target(), contract.digest(), limits)
            .unwrap();
    assert_eq!(
        restored
            .decode_arguments(&serde_json::to_string(&encoded).unwrap(), limits)
            .unwrap(),
        supplied
    );
}
#[test]
fn qualified_models_are_pinned_without_rewriting_an_older_unqualified_contract() {
    let tool = tool();
    let limits = ProviderToolSchemaLimits::default();
    let legacy =
        CompiledToolContract::compile(&tool, target(), &NativeToolSchemaCompiler, limits).unwrap();
    let mut current = target();
    current.model = Some(reference("model-snapshot"));
    let restored = CompiledToolContract::restore(
        &serde_json::to_string(&legacy).unwrap(),
        &tool,
        &current,
        legacy.digest(),
        limits,
    )
    .unwrap();
    assert!(restored.target().model.is_none());
    assert_eq!(restored.digest(), legacy.digest());
    let modern =
        CompiledToolContract::compile(&tool, current.clone(), &NativeToolSchemaCompiler, limits)
            .unwrap();
    current.model.as_mut().unwrap().version = id("different-release");
    assert!(
        CompiledToolContract::restore(
            &serde_json::to_string(&modern).unwrap(),
            &tool,
            &current,
            modern.digest(),
            limits
        )
        .is_err()
    );
}

#[test]
fn stored_v1_contract_keeps_original_guidance_digest_and_codec_while_new_runs_use_v2() {
    let original = tool();
    let text = include_str!("fixtures/provider-tool-contract-v1.json");
    let saved: Value = parse_json(text).unwrap();
    let digest: JsonDigest = serde_json::from_value(saved["digest"].clone()).unwrap();
    let restored =
        CompiledToolContract::restore(text, &original, &target(), &digest, Default::default())
            .unwrap();
    assert_eq!(serde_json::to_value(&restored).unwrap(), saved);
    let current =
        CompiledToolContract::compile(&original, target(), &Restricted, Default::default())
            .unwrap();
    assert_ne!(restored.digest(), current.digest());
    assert_eq!(restored.wire_tool(), current.wire_tool());
    for raw in [
        r#"{"q":"ok","n":{"present":false,"value":null}}"#,
        r#"{"q":"ok","n":{"present":true,"value":null}}"#,
    ] {
        let before = restored.decode_arguments(raw, Default::default()).unwrap();
        let after = current.decode_arguments(raw, Default::default()).unwrap();
        assert_eq!(before, after);
        original.validate_model_inputs(&after).unwrap();
    }
    let current_text = serde_json::to_string(&current).unwrap();
    let again = CompiledToolContract::restore(
        &current_text,
        &original,
        &target(),
        current.digest(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(again.digest(), current.digest());
    assert!(
        restored
            .decode_arguments(
                r#"{"q":"ok","n":{"present":false,"value":"extra"}}"#,
                Default::default()
            )
            .is_err()
    );
    let mut unknown = saved;
    unknown["data"]["schema_version"] = json!("wickle.provider-tool-contract.v999");
    let changed = versioned_digest_json(
        &serde_json::to_string(&unknown["data"]).unwrap(),
        CanonicalizationVersion::SortedJsonV1,
        JsonTextLimits::default(),
    )
    .unwrap();
    unknown["digest"] = serde_json::to_value(&changed).unwrap();
    assert!(
        CompiledToolContract::restore(
            &serde_json::to_string(&unknown).unwrap(),
            &original,
            &target(),
            &changed,
            Default::default()
        )
        .is_err()
    );
}
```
