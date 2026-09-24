# 28장 전체 Rust 구현과 테스트

[강의로](../28-bedrock.md) · [전체 변경 패치](../solutions/28-bedrock.patch)

기준 `7ec497148a959f4f1391c96e69edde4b695938df`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle-model-anthropic/src/codec.rs`

```rust
use crate::error;
use serde_json::{Value, json};
use wickle::*;

pub(crate) const REPLAY_KIND: &str = "wickle.anthropic.messages.v1";
pub(crate) fn invalid() -> ContractError {
    error(ErrorCode::InvalidContract, "content")
}
/// Encode the Messages wire contract while preserving route-bound content.
pub fn encode_request(request: &ModelRequest) -> Result<Value, ContractError> {
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
                request
                    .route
                    .model_id
                    .as_str()
                    .strip_prefix("anthropic.")
                    .unwrap_or(request.route.model_id.as_str()),
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
                && request
                    .route
                    .model_id
                    .as_str()
                    .strip_prefix("anthropic.")
                    .unwrap_or(request.route.model_id.as_str())
                    == "claude-opus-5"
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

/// Messages wire primitives for platform adapters with independent authentication.
pub mod protocol {
    pub use crate::codec::encode_request;
    pub use crate::response::Decoder as MessagesDecoder;
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
/// Validates one Messages stream independently of its HTTP or AWS framing.
pub struct Decoder<'a> {
    request: &'a ModelRequest,
    /// Provider-reported metadata, with an optional transport request identifier.
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
    /// Begin one physical attempt without making a network request.
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
    /// Consume one complete JSON Messages event and produce normalized deltas.
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
    /// Return a validated terminal only after clean transport EOF.
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

## `crates/wickle-model-bedrock/src/auth.rs`

```rust
use crate::error;
use aws_credential_types::Credentials;
use aws_sigv4::{
    http_request::{SignableBody, SignableRequest, SigningSettings, sign},
    sign::v4,
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::{
    fmt,
    time::{Duration, UNIX_EPOCH},
};
use wickle::*;

/// Distinguishes inference access from AWS control-plane model inspection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BedrockAudience {
    /// One model inference request.
    Inference,
    /// Foundation-model or inference-profile metadata.
    Metadata,
}
/// Host authorization context; the adapter does not load an environment chain.
pub struct BedrockCredentialContext<'a> {
    /// Authenticated connection owner.
    pub scope: &'a Scope,
    /// Requested service purpose.
    pub audience: BedrockAudience,
    /// Explicit request origin region.
    pub region: &'a str,
    /// Cancellation while loading or refreshing credentials.
    pub cancellation: &'a tokio_util::sync::CancellationToken,
    /// Deadline covering credential resolution and HTTP.
    pub deadline: tokio::time::Instant,
}
/// Host-supplied credentials. No credential material enters the catalog or model body.
#[derive(Clone)]
pub enum BedrockCredential {
    /// IAM credentials, optionally temporary, signed with the AWS SigV4 library.
    Aws(Credentials),
    /// Bedrock API key/token, usable for inference only.
    Bearer(String),
}
impl fmt::Debug for BedrockCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BedrockCredential([redacted])")
    }
}
/// Applications can adapt an AWS SDK credential chain without putting it in the core.
pub trait BedrockCredentialProvider: Send + Sync {
    /// Resolve credentials for this scope and service purpose.
    fn credential<'a>(
        &'a self,
        context: &'a BedrockCredentialContext<'a>,
    ) -> PortFuture<'a, BedrockCredential>;
}
impl BedrockCredentialProvider for BedrockCredential {
    fn credential<'a>(
        &'a self,
        _: &'a BedrockCredentialContext<'a>,
    ) -> PortFuture<'a, BedrockCredential> {
        Box::pin(async { Ok(self.clone()) })
    }
}
pub(crate) struct SigningRequest<'a> {
    pub method: &'a str,
    pub url: &'a reqwest::Url,
    pub body: &'a [u8],
    pub service: &'a str,
    pub clock: &'a dyn Clock,
    pub headers: HeaderMap,
}
pub(crate) async fn authorize(
    provider: &dyn BedrockCredentialProvider,
    context: BedrockCredentialContext<'_>,
    request: SigningRequest<'_>,
) -> Result<HeaderMap, ContractError> {
    let credential = tokio::select! {biased;
        _=context.cancellation.cancelled()=>return Err(error(ErrorCode::Cancelled,"credential")),
        _=tokio::time::sleep_until(context.deadline)=>return Err(error(ErrorCode::DeadlineExceeded,"credential")),
        result=provider.credential(&context)=>result.map_err(|_|error(ErrorCode::AccessDenied,"credential"))?,
    };
    let mut headers = request.headers;
    match credential {
        BedrockCredential::Bearer(token) => {
            if context.audience != BedrockAudience::Inference
                || token.is_empty()
                || token.chars().any(char::is_whitespace)
            {
                return Err(error(ErrorCode::AccessDenied, "bearer"));
            }
            let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| error(ErrorCode::AccessDenied, "bearer"))?;
            value.set_sensitive(true);
            headers.insert("authorization", value);
        }
        BedrockCredential::Aws(credentials) => {
            // A refresh can take time; check expiry and sign only after it completes.
            let millis = u64::try_from(request.clock.now()?.utc_ms)
                .map_err(|_| error(ErrorCode::ClockUnavailable, "signing_time"))?;
            let time = UNIX_EPOCH
                .checked_add(Duration::from_millis(millis))
                .ok_or_else(|| error(ErrorCode::ClockUnavailable, "signing_time"))?;
            if credentials.expiry().is_some_and(|expiry| expiry <= time) {
                return Err(error(ErrorCode::AccessDenied, "expired_credential"));
            }
            let identity = credentials.into();
            let parameters = v4::SigningParams::builder()
                .identity(&identity)
                .region(context.region)
                .name(request.service)
                .time(time)
                .settings(SigningSettings::default())
                .build()
                .map_err(|_| error(ErrorCode::AccessDenied, "signing_parameters"))?
                .into();
            let values: Vec<_> = headers
                .iter()
                .map(|(name, value)| value.to_str().map(|value| (name.as_str(), value)))
                .collect::<Result<_, _>>()
                .map_err(|_| error(ErrorCode::InvalidContract, "headers"))?;
            let signable = SignableRequest::new(
                request.method,
                request.url.as_str(),
                values.into_iter(),
                SignableBody::Bytes(request.body),
            )
            .map_err(|_| error(ErrorCode::AccessDenied, "signable_request"))?;
            let (instructions, _) = sign(signable, &parameters)
                .map_err(|_| error(ErrorCode::AccessDenied, "signature"))?
                .into_parts();
            if !instructions.params().is_empty() {
                return Err(error(ErrorCode::InvalidContract, "signature_query"));
            }
            let (signed, _) = instructions.into_parts();
            for header in signed {
                let name = HeaderName::from_bytes(header.name().as_bytes())
                    .map_err(|_| error(ErrorCode::InvalidContract, "signature_header"))?;
                let mut value = HeaderValue::from_str(header.value())
                    .map_err(|_| error(ErrorCode::InvalidContract, "signature_header"))?;
                value.set_sensitive(
                    header.sensitive() || name == "authorization" || name == "x-amz-security-token",
                );
                headers.insert(name, value);
            }
        }
    }
    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    struct FixedClock;
    impl Clock for FixedClock {
        fn now(&self) -> Result<ClockReading, ContractError> {
            Ok(ClockReading {
                utc_ms: 1735689600000,
                monotonic_ms: 0,
            })
        }
        fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
            Box::pin(std::future::pending())
        }
    }
    #[tokio::test]
    async fn signing_matches_an_independent_botocore_reference() {
        let scope = Scope {
            tenant_id: Id::new("tenant").unwrap(),
            workspace_id: Id::new("workspace").unwrap(),
            user_id: None,
        };
        let cancellation = Default::default();
        let credential = BedrockCredential::Aws(Credentials::new(
            "AKIDEXAMPLE",
            "test-secret",
            Some("test-session".into()),
            None,
            "fixture",
        ));
        let url =
            reqwest::Url::parse("https://bedrock-mantle.us-east-1.api.aws/anthropic/v1/messages")
                .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        let headers=authorize(&credential,BedrockCredentialContext{scope:&scope,audience:BedrockAudience::Inference,region:"us-east-1",cancellation:&cancellation,deadline:tokio::time::Instant::now()+std::time::Duration::from_secs(1)},SigningRequest{method:"POST",url:&url,body:br#"{"model":"anthropic.claude-opus-5","messages":[],"max_tokens":8,"stream":true}"#,service:"bedrock-mantle",clock:&FixedClock,headers}).await.unwrap();
        // Generated independently by botocore SigV4Auth, not the implementation under test.
        assert_eq!(
            headers["authorization"],
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20250101/us-east-1/bedrock-mantle/aws4_request, SignedHeaders=anthropic-version;content-type;host;x-amz-date;x-amz-security-token, Signature=dc66a56c9bce0eb42494f3b79f654bea31449e043e98573b9bb615dc7a1c44f1"
        );
        assert_eq!(headers["x-amz-date"], "20250101T000000Z");
        assert_eq!(headers["x-amz-security-token"], "test-session");
        assert!(headers["authorization"].is_sensitive());
        assert!(headers["x-amz-security-token"].is_sensitive());
    }
    #[tokio::test]
    async fn encoded_profile_arn_signature_matches_botocore() {
        let scope = Scope {
            tenant_id: Id::new("tenant").unwrap(),
            workspace_id: Id::new("workspace").unwrap(),
            user_id: None,
        };
        let cancellation = Default::default();
        let credential = BedrockCredential::Aws(Credentials::new(
            "AKIDEXAMPLE",
            "test-secret",
            Some("test-session".into()),
            None,
            "fixture",
        ));
        let url = reqwest::Url::parse("https://bedrock-runtime.us-east-1.amazonaws.com/model/arn:aws:bedrock:us-east-1:123456789012:inference-profile%2Fprofile/invoke-with-response-stream").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        let headers = authorize(
            &credential,
            BedrockCredentialContext {
                scope: &scope,
                audience: BedrockAudience::Inference,
                region: "us-east-1",
                cancellation: &cancellation,
                deadline: tokio::time::Instant::now() + Duration::from_secs(1),
            },
            SigningRequest {
                method: "POST",
                url: &url,
                body: br#"{"anthropic_version":"bedrock-2023-05-31","messages":[],"max_tokens":8}"#,
                service: "bedrock",
                clock: &FixedClock,
                headers,
            },
        )
        .await
        .unwrap();
        // Independent botocore reference includes canonical URI double encoding.
        assert_eq!(
            headers["authorization"],
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20250101/us-east-1/bedrock/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-security-token, Signature=6506f742d4c41cbb50b62a020f1c026d844d42a224fcec4aa114fa8489a623cc"
        );
    }
}
```

## `crates/wickle-model-bedrock/src/connection.rs`

```rust
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
```

## `crates/wickle-model-bedrock/src/framing.rs`

```rust
use crate::{BedrockOperation, BedrockOptions, error};
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::BytesMut;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use wickle::*;
use wickle_model_responses::{SseDecoder, SseEvent};

pub(crate) enum Framing {
    Sse(SseDecoder),
    Aws(AwsFrames),
}
impl Framing {
    pub fn new(options: &BedrockOptions) -> Self {
        match options.operation {
            BedrockOperation::Messages => Self::Sse(SseDecoder::new(
                options.max_transport_bytes,
                options.max_event_bytes,
                options.max_protocol_events,
            )),
            BedrockOperation::InvokeStream => Self::Aws(AwsFrames {
                buffer: BytesMut::new(),
                bytes: 0,
                frames: 0,
                max_bytes: options.max_transport_bytes,
                max_frame: options.max_event_bytes,
                max_frames: options.max_protocol_events,
            }),
        }
    }
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseEvent>, ContractError> {
        match self {
            Self::Sse(value) => value.push(bytes),
            Self::Aws(value) => value.push(bytes),
        }
    }
    pub fn finish(&self) -> Result<(), ContractError> {
        match self {
            Self::Sse(value) => value.finish(),
            Self::Aws(value) => {
                if value.buffer.is_empty() {
                    Ok(())
                } else {
                    Err(invalid())
                }
            }
        }
    }
}
pub(crate) struct AwsFrames {
    buffer: BytesMut,
    bytes: usize,
    frames: usize,
    max_bytes: usize,
    max_frame: usize,
    max_frames: usize,
}
impl AwsFrames {
    fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseEvent>, ContractError> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|n| *n <= self.max_bytes)
            .ok_or_else(invalid)?;
        self.buffer.extend_from_slice(bytes);
        let mut events = vec![];
        while self.buffer.len() >= 4 {
            let len =
                u32::from_be_bytes(self.buffer[..4].try_into().map_err(|_| invalid())?) as usize;
            if len < 16 || len > self.max_frame {
                return Err(invalid());
            }
            if self.buffer.len() < len {
                break;
            }
            self.frames = self
                .frames
                .checked_add(1)
                .filter(|n| *n <= self.max_frames)
                .ok_or_else(invalid)?;
            let frame = self.buffer.split_to(len).freeze();
            // The AWS library validates both prelude and message CRCs and headers.
            let message = aws_smithy_eventstream::frame::read_message_from(&frame[..])
                .map_err(|_| invalid())?;
            let mut headers = BTreeMap::new();
            for header in message.headers() {
                let name = header.name().as_str();
                if headers.contains_key(name) {
                    return Err(invalid());
                }
                headers.insert(name, header.value());
            }
            let text = |key| {
                headers
                    .get(key)
                    .and_then(|v| v.as_string().ok())
                    .map(|v| v.as_str())
            };
            match text(":message-type") {
                Some("event") if text(":event-type") == Some("chunk") => {
                    if text(":content-type").is_some_and(|v| v != "application/json") {
                        return Err(invalid());
                    }
                    let body = std::str::from_utf8(message.payload()).map_err(|_| invalid())?;
                    let body = parse_json(body)?;
                    let encoded = body
                        .get("bytes")
                        .and_then(Value::as_str)
                        .ok_or_else(invalid)?;
                    let decoded = STANDARD.decode(encoded).map_err(|_| invalid())?;
                    let data = String::from_utf8(decoded).map_err(|_| invalid())?;
                    // The Messages decoder owns JSON/block/terminal validation.
                    events.push(SseEvent { name: None, data });
                }
                Some("exception" | "error") => {
                    let kind = match text(":exception-type").or_else(|| text(":error-code")) {
                        Some("throttlingException") => "rate_limit_error",
                        Some("modelTimeoutException") => {
                            return Err(error(ErrorCode::DeadlineExceeded, "model_timeout"));
                        }
                        Some(
                            "internalServerException"
                            | "modelStreamErrorException"
                            | "serviceUnavailableException",
                        ) => "api_error",
                        Some("validationException") => "invalid_request_error",
                        Some("accessDeniedException") => "permission_error",
                        _ => return Err(invalid()),
                    };
                    events.push(SseEvent {
                        name: Some("error".into()),
                        data: json!({"type":"error","error":{"type":kind}}).to_string(),
                    });
                }
                _ => return Err(invalid()),
            }
        }
        Ok(events)
    }
}
fn invalid() -> ContractError {
    error(ErrorCode::InvalidContract, "aws_event_stream")
}
```

## `crates/wickle-model-bedrock/src/inspection.rs`

```rust
use crate::{
    BedrockAudience, BedrockConnection, BedrockCredentialContext, BedrockCredentialProvider,
    BedrockSelector, auth, error,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use wickle::*;

/// Provider-documented release facts, independent of inference-profile mutability.
#[derive(Clone, Debug)]
pub struct BedrockSnapshot {
    /// Exact foundation model ID reported by AWS metadata.
    pub model_id: Id,
    /// Release established by trusted provider evidence.
    pub model_version: Id,
    /// Host-owned evidence reference.
    pub evidence_ref: Id,
}
/// Queries AWS control-plane metadata with separately supplied IAM credentials.
#[derive(Clone)]
pub struct BedrockInspector {
    connection: BedrockConnection,
    credentials: Arc<dyn BedrockCredentialProvider>,
    snapshots: Arc<BTreeMap<Id, BedrockSnapshot>>,
}
impl BedrockInspector {
    /// Construct without network calls. A Bedrock bearer token cannot authorize this lookup.
    pub fn new(
        connection: BedrockConnection,
        credentials: Arc<dyn BedrockCredentialProvider>,
        snapshots: Vec<BedrockSnapshot>,
    ) -> Result<Self, ContractError> {
        let mut known = BTreeMap::new();
        for snapshot in snapshots {
            if known.insert(snapshot.model_id.clone(), snapshot).is_some() {
                return Err(error(ErrorCode::InvalidConfiguration, "snapshots"));
            }
        }
        Ok(Self {
            connection,
            credentials,
            snapshots: Arc::new(known),
        })
    }
}
impl ModelRouteInspector for BedrockInspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        context: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            self.connection.validate(route, &context.scope)?;
            let options = &self.connection.0.options;
            let profile = matches!(options.selector, BedrockSelector::InferenceProfile(_));
            let mut url = self.connection.0.metadata_base.clone();
            url.path_segments_mut()
                .map_err(|_| error(ErrorCode::InvalidConfiguration, "metadata_url"))?
                .clear()
                .push(if profile {
                    "inference-profiles"
                } else {
                    "foundation-models"
                })
                .push(options.selector.value());
            let operation = async {
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert(
                    "accept",
                    reqwest::header::HeaderValue::from_static("application/json"),
                );
                let headers = auth::authorize(
                    self.credentials.as_ref(),
                    BedrockCredentialContext {
                        scope: &context.scope,
                        audience: BedrockAudience::Metadata,
                        region: &options.region,
                        cancellation: &context.cancellation,
                        deadline: context.deadline,
                    },
                    auth::SigningRequest {
                        method: "GET",
                        url: &url,
                        body: &[],
                        service: "bedrock",
                        clock: self.connection.0.clock.as_ref(),
                        headers,
                    },
                )
                .await?;
                let mut response = self
                    .connection
                    .0
                    .client
                    .get(url)
                    .headers(headers)
                    .send()
                    .await
                    .map_err(|_| unavailable())?;
                if response.status().as_u16() == 404 {
                    return Ok(ModelRouteObservation {
                        route_digest: route.digest(),
                        availability: ModelRouteAvailability::Unavailable,
                        model_id: None,
                        model_version: None,
                        deployment_revision: None,
                        version_semantics: VersionSemantics::Unverified,
                        evidence_ref: Id::new("aws.bedrock.metadata")?,
                    });
                }
                if !response.status().is_success() {
                    return Err(unavailable());
                }
                let mut bytes = vec![];
                while let Some(chunk) = response.chunk().await.map_err(|_| unavailable())? {
                    if bytes.len().saturating_add(chunk.len())
                        > 65_536.min(options.max_transport_bytes)
                    {
                        return Err(unavailable());
                    }
                    bytes.extend_from_slice(&chunk);
                }
                let body = std::str::from_utf8(&bytes)
                    .ok()
                    .and_then(|s| parse_json(s).ok())
                    .ok_or_else(unavailable)?;
                let (model, availability, revision) = if profile {
                    if text(&body, "inferenceProfileId")? != options.selector.value()
                        && text(&body, "inferenceProfileArn")? != options.selector.value()
                    {
                        return Err(drift());
                    }
                    let models = body
                        .get("models")
                        .and_then(Value::as_array)
                        .filter(|m| !m.is_empty())
                        .ok_or_else(unavailable)?;
                    let mut actual = None;
                    for model in models {
                        let (region, id) = foundation_arn(text(model, "modelArn")?)?;
                        if !options.allowed_destination_regions.is_empty()
                            && !options
                                .allowed_destination_regions
                                .iter()
                                .any(|v| v == region)
                        {
                            return Err(error(ErrorCode::AccessDenied, "profile_destination"));
                        }
                        if actual.is_some_and(|old| old != id) {
                            return Err(drift());
                        }
                        actual = Some(id);
                    }
                    let revision = canonical_digest(
                        &json!({"arn":body.get("inferenceProfileArn"),"models":models,"status":body.get("status"),"updated_at":body.get("updatedAt")}),
                    );
                    (
                        Id::new(actual.ok_or_else(unavailable)?)?,
                        if text(&body, "status")? == "ACTIVE" {
                            ModelRouteAvailability::Available
                        } else {
                            ModelRouteAvailability::Unknown
                        },
                        Some(Id::new(revision.as_str())?),
                    )
                } else {
                    let details = body.get("modelDetails").ok_or_else(unavailable)?;
                    let id = text(details, "modelId")?;
                    let arn = text(details, "modelArn")?;
                    if options.selector.value() != id && options.selector.value() != arn {
                        return Err(drift());
                    }
                    let (_, arn_id) = foundation_arn(arn)?;
                    if arn_id != id || text(details, "providerName")? != "Anthropic" {
                        return Err(drift());
                    }
                    if details.get("responseStreamingSupported") != Some(&json!(true)) {
                        return Err(error(ErrorCode::ModelCapabilityUnsupported, "streaming"));
                    }
                    let available = match details
                        .pointer("/modelLifecycle/status")
                        .and_then(Value::as_str)
                    {
                        Some("ACTIVE" | "LEGACY") => ModelRouteAvailability::Available,
                        _ => ModelRouteAvailability::Unknown,
                    };
                    (Id::new(id)?, available, None)
                };
                let snapshot = self.snapshots.get(&model);
                Ok(ModelRouteObservation {
                    route_digest: route.digest(),
                    availability,
                    model_id: Some(model.clone()),
                    model_version: snapshot.map(|s| s.model_version.clone()),
                    deployment_revision: revision,
                    version_semantics: if profile {
                        VersionSemantics::MutableDeployment
                    } else if snapshot.is_some() {
                        VersionSemantics::Pinned
                    } else {
                        VersionSemantics::Unverified
                    },
                    evidence_ref: snapshot
                        .map(|s| s.evidence_ref.clone())
                        .unwrap_or(Id::new("aws.bedrock.metadata")?),
                })
            };
            tokio::select! {biased;
                _=context.cancellation.cancelled()=>Err(error(ErrorCode::Cancelled,"inspection")),
                _=tokio::time::sleep_until(context.deadline)=>Err(error(ErrorCode::DeadlineExceeded,"inspection")),
                result=operation=>result,
            }
        })
    }
}
fn text<'a>(v: &'a Value, key: &str) -> Result<&'a str, ContractError> {
    v.get(key)
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .ok_or_else(unavailable)
}
fn foundation_arn(value: &str) -> Result<(&str, &str), ContractError> {
    let fields: Vec<_> = value.splitn(6, ':').collect();
    if fields.len() != 6
        || fields[0] != "arn"
        || !matches!(
            fields[1],
            "aws" | "aws-cn" | "aws-us-gov" | "aws-iso" | "aws-iso-b" | "aws-iso-e" | "aws-iso-f"
        )
        || fields[2] != "bedrock"
        || fields[3].is_empty()
    {
        return Err(unavailable());
    }
    let id = fields[5]
        .strip_prefix("foundation-model/")
        .filter(|v| !v.is_empty() && !v.contains('/'))
        .ok_or_else(unavailable)?;
    Ok((fields[3], id))
}
fn unavailable() -> ContractError {
    error(ErrorCode::ModelInspectionUnavailable, "metadata")
}
fn drift() -> ContractError {
    error(ErrorCode::ModelVersionDrift, "metadata_target")
}
```

## `crates/wickle-model-bedrock/src/lib.rs`

```rust
//! AWS Bedrock Claude with explicit credentials, endpoint contracts and bounded streams.
#![forbid(unsafe_code)]
mod auth;
mod connection;
mod framing;
mod inspection;
mod model;
pub use auth::{
    BedrockAudience, BedrockCredential, BedrockCredentialContext, BedrockCredentialProvider,
};
pub use aws_credential_types::Credentials as AwsCredentials;
pub use connection::{
    BedrockConnection, BedrockEndpoint, BedrockOperation, BedrockOptions, BedrockSelector,
};
pub use inspection::{BedrockInspector, BedrockSnapshot};
pub use model::BedrockModel;
use wickle::{ContractError, ErrorCode};
fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("bedrock.{location}"))
}
```

## `crates/wickle-model-bedrock/src/model.rs`

```rust
use crate::{
    BedrockAudience, BedrockConnection, BedrockCredentialContext, BedrockEndpoint,
    BedrockOperation, auth, error, framing::Framing,
};
use futures_util::stream;
use reqwest::Response;
use serde_json::Value;
use std::collections::VecDeque;
use wickle::*;
use wickle_model_anthropic::protocol::{MessagesDecoder, encode_request};

/// One Bedrock Claude POST per invocation, streamed into Wickle model events.
/// The adapter does not retry, run Tool handlers, or load environment variables.
#[derive(Debug, Clone)]
pub struct BedrockModel {
    connection: BedrockConnection,
}
impl BedrockModel {
    /// Bind an already configured connection without making a network request.
    pub fn new(connection: BedrockConnection) -> Self {
        Self { connection }
    }
}
impl ModelPort for BedrockModel {
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
            decoder: MessagesDecoder::new(request, None),
            framing: Framing::new(&self.connection.0.options),
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
    connection: &'a BedrockConnection,
    request: &'a ModelRequest,
    context: &'a ModelCallContext,
    response: Option<Response>,
    decoder: MessagesDecoder<'a>,
    framing: Framing,
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
        if self.connection.0.options.endpoint == BedrockEndpoint::Mantle
            && matches!(self.request.output, ModelOutput::JsonSchema { .. })
        {
            return Err(error(
                ErrorCode::ModelCapabilityUnsupported,
                "mantle_json_output",
            ));
        }
        let mut value = encode_request(self.request)?;
        match self.connection.0.options.operation {
            BedrockOperation::Messages => {
                value["model"] = serde_json::json!(self.connection.0.options.selector.value())
            }
            BedrockOperation::InvokeStream => {
                let object = value.as_object_mut().expect("encoded object");
                object.remove("model");
                object.remove("stream");
                object.insert(
                    "anthropic_version".into(),
                    serde_json::json!("bedrock-2023-05-31"),
                );
            }
        }
        let body =
            serde_json::to_vec(&value).map_err(|_| error(ErrorCode::InvalidJson, "request"))?;
        if body.len() > self.request.limits.max_input_bytes {
            return Err(error(ErrorCode::ModelCapabilityUnsupported, "request_size"));
        }
        let url = self.connection.0.url.clone();
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "content-type",
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        headers.insert(
            "accept",
            reqwest::header::HeaderValue::from_static(self.content_type()),
        );
        headers.insert(
            "accept-encoding",
            reqwest::header::HeaderValue::from_static("identity"),
        );
        if self.connection.0.options.operation == BedrockOperation::Messages {
            headers.insert(
                "anthropic-version",
                reqwest::header::HeaderValue::from_static("2023-06-01"),
            );
        }
        let headers = match auth::authorize(
            self.connection.0.credentials.as_ref(),
            BedrockCredentialContext {
                scope: &self.context.scope,
                audience: BedrockAudience::Inference,
                region: &self.connection.0.options.region,
                cancellation: &self.context.cancellation,
                deadline: self.context.deadline,
            },
            auth::SigningRequest {
                method: "POST",
                url: &url,
                body: &body,
                service: self.connection.signing_service(),
                clock: self.connection.0.clock.as_ref(),
                headers,
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
            .or_else(|| response.headers().get("x-amzn-requestid"))
            .or_else(|| response.headers().get("x-amz-request-id"))
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
                413 => ModelFailureKind::ContextOverflow,
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
        if !content_type.is_some_and(|value| value.eq_ignore_ascii_case(self.content_type())) {
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
    fn content_type(&self) -> &'static str {
        match self.connection.0.options.operation {
            BedrockOperation::Messages => "text/event-stream",
            BedrockOperation::InvokeStream => "application/vnd.amazon.eventstream",
        }
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

## `crates/wickle-model-bedrock/tests/bedrock.rs`

```rust
//! AWS signing, endpoint, binary/SSE framing and metadata contracts without AWS calls.
mod support;
use aws_smithy_types::event_stream::{Header, HeaderValue, Message};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use support::*;
use wickle::*;
use wickle_model_bedrock::*;

fn frame(value: &Value) -> Vec<u8> {
    let payload =
        serde_json::to_vec(&json!({"bytes":STANDARD.encode(serde_json::to_vec(value).unwrap())}))
            .unwrap();
    let message = Message::new(payload)
        .add_header(Header::new(
            ":message-type",
            HeaderValue::String("event".into()),
        ))
        .add_header(Header::new(
            ":event-type",
            HeaderValue::String("chunk".into()),
        ))
        .add_header(Header::new(
            ":content-type",
            HeaderValue::String("application/json".into()),
        ));
    let mut bytes = vec![];
    aws_smithy_eventstream::frame::write_message_to(&message, &mut bytes).unwrap();
    bytes
}
fn reply(operation: BedrockOperation, data: &[Value]) -> Reply {
    let mut reply = Reply::sse(data);
    reply.headers = vec![("x-amzn-requestid", "aws-request".into())];
    if operation == BedrockOperation::InvokeStream {
        reply.content_type = "application/vnd.amazon.eventstream";
        reply.body = data.iter().flat_map(frame).collect();
        reply.chunk = 3;
    }
    reply
}
fn configured(
    server: &Server,
    operation: BedrockOperation,
    endpoint: BedrockEndpoint,
    bearer: bool,
) -> BedrockConnection {
    let mut options = options(server);
    options.operation = operation;
    options.endpoint = endpoint;
    let credential = if bearer {
        Arc::new(BedrockCredential::Bearer("test-bearer".into()))
            as Arc<dyn BedrockCredentialProvider>
    } else {
        credentials()
    };
    BedrockConnection::new(scope(), reference("account"), credential, options)
        .unwrap()
        .with_clock(Arc::new(FixedClock))
}

#[tokio::test]
async fn each_wire_operation_preserves_model_and_uses_its_own_auth_contract() {
    for (operation, endpoint, bearer) in [
        (BedrockOperation::Messages, BedrockEndpoint::Runtime, true),
        (BedrockOperation::Messages, BedrockEndpoint::Mantle, false),
        (
            BedrockOperation::InvokeStream,
            BedrockEndpoint::Runtime,
            false,
        ),
    ] {
        let server = Server::new(vec![reply(operation, &events(MODEL, "42"))]).await;
        let connection = configured(&server, operation, endpoint, bearer);
        let request = request(&connection, MODEL);
        let model = BedrockModel::new(connection);
        let response =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap();
        assert_eq!(response.text, "42");
        assert_eq!(
            response.metadata.provider_request_id,
            Some(id("aws-request"))
        );
        assert_eq!(response.metadata.reported_model_id, Some(id(MODEL)));
        assert_eq!(response.metadata.usage.unwrap().output_tokens, Some(15));
        assert_eq!(
            response.continuation[0].data()["blocks"][0]["signature"],
            "signature-fixture"
        );
        let calls = server.requests.lock().unwrap();
        assert_eq!(calls.len(), 1);
        let call = &calls[0];
        let headers = call.headers.to_ascii_lowercase();
        assert_eq!(call.method, "POST");
        if bearer {
            assert!(headers.contains("authorization: bearer test-bearer"));
            assert!(!headers.contains("x-amz-security-token"));
        } else {
            assert!(headers.contains("x-amz-security-token: test-session"));
            assert!(headers.contains(if endpoint == BedrockEndpoint::Mantle {
                "/us-east-1/bedrock-mantle/aws4_request"
            } else {
                "/us-east-1/bedrock/aws4_request"
            }));
            assert!(!headers.contains("test-secret"));
        }
        match operation {
            BedrockOperation::Messages => {
                assert_eq!(call.path, "/anthropic/v1/messages");
                assert_eq!(call.body["model"], MODEL);
                assert_eq!(call.body["stream"], true);
                assert!(call.body.get("anthropic_version").is_none());
                assert!(headers.contains("anthropic-version: 2023-06-01"));
            }
            BedrockOperation::InvokeStream => {
                assert_eq!(
                    call.path,
                    format!("/model/{MODEL}/invoke-with-response-stream")
                );
                assert_eq!(call.body["anthropic_version"], "bedrock-2023-05-31");
                assert!(call.body.get("model").is_none());
                assert!(call.body.get("stream").is_none());
            }
        }
        assert_eq!(call.body["output_config"]["effort"], "medium");
        assert!(!call.body.to_string().contains("hidden-workspace"));
    }
}

#[tokio::test]
async fn corrupted_truncated_or_oversized_aws_frames_never_complete() {
    for case in ["crc", "truncated", "length", "count"] {
        let mut response = reply(BedrockOperation::InvokeStream, &events(MODEL, "answer"));
        match case {
            "crc" => response.body[12] ^= 1,
            "truncated" => {
                response.body.pop();
            }
            "length" => response.body[..4].copy_from_slice(&u32::MAX.to_be_bytes()),
            _ => {}
        }
        let server = Server::new(vec![response]).await;
        let mut options = options(&server);
        options.operation = BedrockOperation::InvokeStream;
        if case == "count" {
            options.max_protocol_events = 1;
        }
        let connection =
            BedrockConnection::new(scope(), reference("account"), credentials(), options)
                .unwrap()
                .with_clock(Arc::new(FixedClock));
        let request = request(&connection, MODEL);
        let model = BedrockModel::new(connection);
        assert!(
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .is_err(),
            "{case}"
        );
    }
}

#[tokio::test]
async fn scope_contract_unsupported_features_and_expired_credentials_stop_before_http() {
    for case in ["scope", "route", "api", "json", "expired"] {
        let server = Server::new(vec![]).await;
        let mut options = options(&server);
        options.endpoint = BedrockEndpoint::Mantle;
        let credential: Arc<dyn BedrockCredentialProvider> = if case == "expired" {
            Arc::new(BedrockCredential::Aws(AwsCredentials::new(
                "AKIDEXAMPLE",
                "secret",
                None,
                Some(std::time::UNIX_EPOCH),
                "fixture",
            )))
        } else {
            credentials()
        };
        let connection = BedrockConnection::new(scope(), reference("account"), credential, options)
            .unwrap()
            .with_clock(Arc::new(FixedClock));
        let mut request = request(&connection, MODEL);
        let mut context = context(&request);
        let model = BedrockModel::new(connection);
        match case {
            "scope" => context.scope.workspace_id = id("other"),
            "route" => {
                request
                    .route
                    .target
                    .insert("region".into(), json!("us-west-2"));
            }
            "api" => request.route.api_contract.version = id("other"),
            "json" => {
                request.output = ModelOutput::JsonSchema {
                    schema: json!({"type":"object","properties":{},"additionalProperties":false}),
                }
            }
            _ => {}
        }
        let failure = collect_model_response(&request, model.generate(&request, &context))
            .await
            .unwrap_err();
        if case == "expired" {
            assert_eq!(failure.kind, ModelFailureKind::Authentication);
        }
        assert!(server.requests.lock().unwrap().is_empty());
    }
}

fn foundation() -> Value {
    json!({"modelDetails":{"modelId":MODEL,"modelArn":format!("arn:aws:bedrock:us-east-1::foundation-model/{MODEL}"),"providerName":"Anthropic","responseStreamingSupported":true,"modelLifecycle":{"status":"ACTIVE"}}})
}
fn profile() -> Value {
    json!({"inferenceProfileId":"profile","inferenceProfileArn":"arn:aws:bedrock:us-east-1:123456789012:inference-profile/profile","status":"ACTIVE","models":[{"modelArn":format!("arn:aws:bedrock:us-east-1::foundation-model/{MODEL}")},{"modelArn":format!("arn:aws:bedrock:us-west-2::foundation-model/{MODEL}")}],"updatedAt":"2025-01-01T00:00:00Z"})
}
fn inspect_context() -> ModelInspectionContext {
    ModelInspectionContext {
        scope: scope(),
        principal_ref: id("user"),
        capability_grant_ref: id("grant"),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(3),
    }
}
fn snapshot() -> BedrockSnapshot {
    BedrockSnapshot {
        model_id: id(MODEL),
        model_version: id("release"),
        evidence_ref: id("documented-claude-release"),
    }
}

#[tokio::test]
async fn foundation_metadata_needs_its_own_iam_access_and_actual_release_evidence() {
    let server = Server::new(vec![
        Reply::json(200, foundation()),
        Reply::json(200, foundation()),
    ])
    .await;
    let connection = connection(&server);
    let request = request(&connection, MODEL);
    let unknown = BedrockInspector::new(connection.clone(), credentials(), vec![])
        .unwrap()
        .inspect(&request.route, &inspect_context())
        .await
        .unwrap();
    assert!(unknown.model_version.is_none());
    assert_eq!(unknown.version_semantics, VersionSemantics::Unverified);
    let known = BedrockInspector::new(connection.clone(), credentials(), vec![snapshot()])
        .unwrap()
        .inspect(&request.route, &inspect_context())
        .await
        .unwrap();
    known
        .validate(&request.route, VersionPolicy::RequirePinned)
        .unwrap();
    let denied = BedrockInspector::new(
        connection,
        Arc::new(BedrockCredential::Bearer("inference-only".into())),
        vec![snapshot()],
    )
    .unwrap()
    .inspect(&request.route, &inspect_context())
    .await
    .unwrap_err();
    assert_eq!(denied.code, ErrorCode::AccessDenied);
    let calls = server.requests.lock().unwrap();
    assert_eq!(calls.len(), 2);
    for call in calls.iter() {
        assert_eq!(call.method, "GET");
        assert_eq!(call.path, format!("/foundation-models/{MODEL}"));
        assert!(call.headers.contains("/us-east-1/bedrock/aws4_request"));
        assert!(call.body.is_null());
    }
}

#[tokio::test]
async fn profile_destinations_and_revision_are_checked_separately_from_origin_region() {
    for mode in ["allowed", "destination", "model-drift"] {
        let mut changed = profile();
        if mode == "model-drift" {
            changed["models"][1]["modelArn"] =
                json!("arn:aws:bedrock:us-west-2::foundation-model/anthropic.other-model");
        } else {
            changed["updatedAt"] = json!("2025-02-01T00:00:00Z");
        }
        let server =
            Server::new(vec![Reply::json(200, profile()), Reply::json(200, changed)]).await;
        let mut options = options(&server);
        options.selector = BedrockSelector::InferenceProfile("profile".into());
        options.allowed_destination_regions = if mode == "destination" {
            vec!["us-east-1".into()]
        } else {
            vec!["us-east-1".into(), "us-west-2".into()]
        };
        let connection =
            BedrockConnection::new(scope(), reference("account"), credentials(), options)
                .unwrap()
                .with_clock(Arc::new(FixedClock));
        let mut request = request(&connection, MODEL);
        request.route.version_semantics = VersionSemantics::MutableDeployment;
        let inspector = BedrockInspector::new(connection, credentials(), vec![snapshot()]).unwrap();
        let first = inspector.inspect(&request.route, &inspect_context()).await;
        if mode == "destination" {
            assert_eq!(first.unwrap_err().code, ErrorCode::AccessDenied);
            continue;
        }
        let first = first.unwrap();
        first
            .validate(&request.route, VersionPolicy::AllowMutable)
            .unwrap();
        assert_eq!(first.version_semantics, VersionSemantics::MutableDeployment);
        request.route.deployment_revision = first.deployment_revision;
        let next = inspector.inspect(&request.route, &inspect_context()).await;
        if mode == "model-drift" {
            assert_eq!(next.unwrap_err().code, ErrorCode::ModelVersionDrift);
        } else {
            assert_eq!(
                next.unwrap()
                    .validate(&request.route, VersionPolicy::AllowMutable)
                    .unwrap_err()
                    .code,
                ErrorCode::ModelVersionDrift
            );
        }
        assert_eq!(
            server.requests.lock().unwrap()[0].path,
            "/inference-profiles/profile"
        );
    }
}

#[tokio::test]
async fn aws_exception_frames_and_http_failures_do_not_retry_or_expose_private_messages() {
    for binary in [false, true] {
        let mut response = Reply::json(429, json!({"message":"private details"}));
        if binary {
            let msg = Message::new(br#"{"message":"private details"}"#.to_vec())
                .add_header(Header::new(
                    ":message-type",
                    HeaderValue::String("exception".into()),
                ))
                .add_header(Header::new(
                    ":exception-type",
                    HeaderValue::String("throttlingException".into()),
                ));
            response.status = 200;
            response.content_type = "application/vnd.amazon.eventstream";
            response.body.clear();
            aws_smithy_eventstream::frame::write_message_to(&msg, &mut response.body).unwrap();
        }
        let server = Server::new(vec![response]).await;
        let connection = configured(
            &server,
            BedrockOperation::InvokeStream,
            BedrockEndpoint::Runtime,
            false,
        );
        let request = request(&connection, MODEL);
        let model = BedrockModel::new(connection);
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, ModelFailureKind::RateLimited);
        assert!(!format!("{failure:?}").contains("private details"));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn cancellation_and_deadline_close_both_stream_types_without_retry() {
    for operation in [BedrockOperation::Messages, BedrockOperation::InvokeStream] {
        for cancel in [true, false] {
            let mut data = events(MODEL, "partial");
            data.truncate(6);
            let mut response = reply(operation, &data);
            response.stall = true;
            let server = Server::new(vec![response]).await;
            let connection = configured(&server, operation, BedrockEndpoint::Runtime, false);
            let request = request(&connection, MODEL);
            let model = BedrockModel::new(connection);
            let mut context = context(&request);
            context.deadline = tokio::time::Instant::now() + Duration::from_secs(2);
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
            tokio::time::timeout(Duration::from_secs(1), server.closed.notified())
                .await
                .unwrap();
            assert_eq!(server.requests.lock().unwrap().len(), 1);
        }
    }
}

#[tokio::test]
async fn redirects_never_forward_signed_credentials() {
    let destination = Server::new(vec![]).await;
    let mut response = Reply::json(307, json!({}));
    response
        .headers
        .push(("location", destination.base.clone()));
    let server = Server::new(vec![response]).await;
    let connection = connection(&server);
    let request = request(&connection, MODEL);
    let model = BedrockModel::new(connection);
    assert!(
        collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .is_err()
    );
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    assert!(destination.requests.lock().unwrap().is_empty());
}

fn tool_events() -> Vec<Value> {
    let mut data = events(MODEL, "");
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
async fn profile_arn_and_signed_tool_continuation_survive_both_wire_operations() {
    let arn = "arn:aws:bedrock:us-east-1:123456789012:inference-profile/profile";
    for operation in [BedrockOperation::Messages, BedrockOperation::InvokeStream] {
        let server = Server::new(vec![
            reply(operation, &tool_events()),
            reply(operation, &events(MODEL, "42")),
        ])
        .await;
        let mut options = options(&server);
        options.operation = operation;
        options.selector = BedrockSelector::InferenceProfile(arn.into());
        let connection =
            BedrockConnection::new(scope(), reference("account"), credentials(), options)
                .unwrap()
                .with_clock(Arc::new(FixedClock));
        let mut request = request(&connection, MODEL);
        tool(&mut request);
        let model = BedrockModel::new(connection);
        let first = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(first.finish, ModelFinish::ToolCalls);
        assert_eq!(first.tool_calls[0].model_inputs["query"], "alpha");
        request.messages.push(ModelMessage {
            role: ModelRole::Assistant,
            content: vec![
                ModelContent::ToolCall {
                    provider_call_id: id("toolu_1"),
                    name: id("lookup"),
                    arguments: JsonObject::from([("query".into(), json!("alpha"))]),
                },
                ModelContent::Opaque {
                    continuation: first.continuation[0].clone(),
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
        request.request_id = id("next-attempt");
        let second = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(second.text, "42");
        let calls = server.requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        let body = &calls[1].body;
        let blocks = body["messages"][1]["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["signature"], "signature-fixture");
        assert_eq!(blocks[1]["id"], "toolu_1");
        assert_eq!(blocks[1]["input"]["query"], "alpha");
        assert_eq!(body["messages"][2]["content"][0]["tool_use_id"], "toolu_1");
        match operation {
            BedrockOperation::Messages => assert_eq!(body["model"], arn),
            BedrockOperation::InvokeStream => assert_eq!(
                calls[1].path,
                "/model/arn:aws:bedrock:us-east-1:123456789012:inference-profile%2Fprofile/invoke-with-response-stream"
            ),
        }
    }
}

struct AdvancingCredentials {
    clock: Arc<std::sync::atomic::AtomicI64>,
    expired: bool,
}
struct AdvancingClock(Arc<std::sync::atomic::AtomicI64>);
impl Clock for AdvancingClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: self.0.load(std::sync::atomic::Ordering::SeqCst),
            monotonic_ms: 0,
        })
    }
    fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}
impl BedrockCredentialProvider for AdvancingCredentials {
    fn credential<'a>(
        &'a self,
        _: &'a BedrockCredentialContext<'a>,
    ) -> PortFuture<'a, BedrockCredential> {
        Box::pin(async move {
            self.clock
                .store(1735689610000, std::sync::atomic::Ordering::SeqCst);
            Ok(BedrockCredential::Aws(AwsCredentials::new(
                "AKIDEXAMPLE",
                "secret",
                None,
                self.expired
                    .then_some(std::time::UNIX_EPOCH + Duration::from_secs(1735689605)),
                "fixture",
            )))
        })
    }
}
#[tokio::test]
async fn credential_expiry_and_signature_use_time_after_refresh() {
    for expired in [true, false] {
        let server = Server::new(vec![reply(
            BedrockOperation::Messages,
            &events(MODEL, "42"),
        )])
        .await;
        let clock = Arc::new(std::sync::atomic::AtomicI64::new(1735689600000));
        let credentials = Arc::new(AdvancingCredentials {
            clock: clock.clone(),
            expired,
        });
        let connection =
            BedrockConnection::new(scope(), reference("account"), credentials, options(&server))
                .unwrap()
                .with_clock(Arc::new(AdvancingClock(clock)));
        let request = request(&connection, MODEL);
        let model = BedrockModel::new(connection);
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        let calls = server.requests.lock().unwrap();
        if expired {
            assert_eq!(result.unwrap_err().kind, ModelFailureKind::Authentication);
            assert!(calls.is_empty());
        } else {
            assert_eq!(result.unwrap().text, "42");
            assert!(
                calls[0]
                    .headers
                    .to_ascii_lowercase()
                    .contains("x-amz-date: 20250101t000010z")
            );
        }
    }
}
struct PendingCredentials(tokio::sync::Notify);
impl BedrockCredentialProvider for PendingCredentials {
    fn credential<'a>(
        &'a self,
        _: &'a BedrockCredentialContext<'a>,
    ) -> PortFuture<'a, BedrockCredential> {
        Box::pin(async move {
            self.0.notify_one();
            std::future::pending().await
        })
    }
}
#[tokio::test]
async fn credential_lookup_obeys_cancellation_and_deadline_before_http() {
    for cancel in [true, false] {
        let server = Server::new(vec![]).await;
        let credentials = Arc::new(PendingCredentials(tokio::sync::Notify::new()));
        let connection = BedrockConnection::new(
            scope(),
            reference("account"),
            credentials.clone(),
            options(&server),
        )
        .unwrap();
        let request = request(&connection, MODEL);
        let model = BedrockModel::new(connection);
        let mut context = context(&request);
        context.deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let result = async { model.generate(&request, &context).next().await.unwrap() };
        let trigger = async {
            credentials.0.notified().await;
            if cancel {
                context.cancellation.cancel();
            }
        };
        let (result, _) = tokio::join!(result, trigger);
        if cancel {
            assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
        } else {
            assert!(matches!(
                result.unwrap(),
                ModelEvent::ResponseError {
                    kind: ModelFailureKind::Timeout,
                    ..
                }
            ));
        }
        assert!(server.requests.lock().unwrap().is_empty());
    }
}
```

## `crates/wickle-model-bedrock/tests/support/mod.rs`

```rust
use serde_json::{Value, json};
use wickle::*;
use wickle_model_bedrock::*;

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
pub const MODEL: &str = "anthropic.claude-opus-5";
pub fn origin(server: &Server) -> String {
    server.base.trim_end_matches("v1/").into()
}
pub struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: 1735689600000,
            monotonic_ms: 0,
        })
    }
    fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}
pub fn credentials() -> std::sync::Arc<dyn BedrockCredentialProvider> {
    std::sync::Arc::new(BedrockCredential::Aws(AwsCredentials::new(
        "AKIDEXAMPLE",
        "test-secret",
        Some("test-session".into()),
        None,
        "fixture",
    )))
}
pub fn options(server: &Server) -> BedrockOptions {
    let mut options = BedrockOptions::new("us-east-1", BedrockSelector::Foundation(MODEL.into()));
    options.endpoint_url = Some(origin(server));
    options.metadata_url = Some(origin(server));
    options
}
pub fn connection(server: &Server) -> BedrockConnection {
    BedrockConnection::new(
        scope(),
        reference("account"),
        credentials(),
        options(server),
    )
    .unwrap()
    .with_clock(std::sync::Arc::new(FixedClock))
}
pub fn request(connection: &BedrockConnection, model: &str) -> ModelRequest {
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
            api_contract: connection.api_contract(),
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

## `tests/support/bedrock_consumer.rs`

```rust
// Real loopback HTTP/SSE against the extracted Bedrock adapter package; no provider call.
use serde_json::json;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use wickle::*;
use wickle_model_bedrock::*;
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
        assert!(headers.starts_with("post /anthropic/v1/messages "));
        assert!(headers.contains("authorization: bearer fixture-key"));
        assert!(headers.contains("anthropic-version: 2023-06-01"));
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
    let mut options = BedrockOptions::new("us-east-1", BedrockSelector::Foundation("fixture-model".into()));
    options.endpoint_url = Some(base);
    let connection = BedrockConnection::new(
        scope.clone(), reference("account"),
        std::sync::Arc::new(BedrockCredential::Bearer("fixture-key".into())), options,
    )?;
    let binding = connection.binding();
    let model = BedrockModel::new(connection.clone());
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
            api_contract: connection.api_contract(),
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
        "Bedrock consumer: extracted adapter performs one HTTP/SSE request, uses explicit bearer authentication, preserves native output options, signed thinking and cumulative usage, decodes JSON and reported usage, and excludes Host context from the wire (local fixture, no provider network)"
    );
    Ok(())
}
```
