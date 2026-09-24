# 52장 전체 구현과 변경 검사

[강의](../52-anthropic-repair.md) · [전체 변경 패치](../solutions/52-anthropic-repair.patch)

기준 `b0da12cc9415c99326df5189c7918f311d4f3395`. 이 단계에서 바뀐 Rust·manifest·Python 파일의 전체 내용이다. 이전 버전과의 정확한 교체 위치·삭제는 patch를 따른다. 다음 장의 코드와 섞지 않는다.

## `crates/wickle-model-anthropic/Cargo.toml`

```toml
[package]
name = "wickle-model-anthropic"
version.workspace = true
license.workspace = true
edition.workspace = true
rust-version.workspace = true
repository.workspace = true
description = "Anthropic Messages model and metadata adapters for Wickle"
publish = false
include = ["Cargo.toml", "LICENSE", "src/**"]

[dependencies]
wickle-model-responses = { path = "../wickle-model-responses", version = "=0.1.0" }
wickle = { path = "../wickle", version = "=0.1.0" }
reqwest.workspace = true
serde_json = { workspace = true, features = ["raw_value"] }
futures-util.workspace = true
tokio.workspace = true
tokio-util.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["net", "io-util", "rt-multi-thread", "test-util"] }

[lints]
workspace = true
```

## `crates/wickle-model-anthropic/src/codec.rs`

```rust
use crate::error;
use serde_json::{Value, json};
use wickle::*;

pub(crate) const INVALID_REPLAY_KIND: &str = "wickle.anthropic.messages.v2";
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
    let mut invalid_tools = std::collections::BTreeMap::<String, String>::new();
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
                    ModelContent::ToolResult{provider_call_id,content} if message.role==ModelRole::Tool => {
                        let raw = invalid_tools.remove(provider_call_id.as_str());
                        let failed = raw.is_some();
                        let content = match raw { Some(raw) => json!({"INVALID_JSON":raw,"feedback":content}), None => content.clone() };
                        let mut block = json!({"type":"tool_result","tool_use_id":provider_call_id,"content":serde_json::to_string(&content).map_err(|_|invalid())?});
                        if failed { block["is_error"] = json!(true); }
                        block
                    },
                    _=>return Err(error(ErrorCode::ModelContextIncompatible,"message")),
                });
            }
            blocks
        } else {
            if opaque.len() != 1 || message.role != ModelRole::Assistant {
                return Err(invalid());
            }
            let data = opaque[0].data();
            let invalid_arguments = match data.get("kind").and_then(Value::as_str) {
                Some(REPLAY_KIND) if data.as_object().is_some_and(|o| o.len() == 2) => None,
                Some(INVALID_REPLAY_KIND) if data.as_object().is_some_and(|o| o.len() == 3) => {
                    Some(
                        data.get("invalid_arguments")
                            .and_then(Value::as_object)
                            .filter(|map| !map.is_empty())
                            .ok_or_else(invalid)?,
                    )
                }
                _ => return Err(error(ErrorCode::ModelContextIncompatible, "replay")),
            };
            let blocks = data
                .get("blocks")
                .and_then(Value::as_array)
                .ok_or_else(invalid)?;
            let decoded = inspect_blocks(blocks)?;
            if invalid_arguments.is_some_and(|map| {
                map.keys()
                    .any(|id| !decoded.calls.iter().any(|call| &call.id == id))
            }) {
                return Err(error(ErrorCode::ModelContextIncompatible, "replay"));
            }
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
                    || match invalid_arguments.and_then(|map| map.get(&original.id)) {
                        Some(raw) => {
                            let raw = raw.as_str().ok_or_else(invalid)?;
                            if parse_provider_arguments(raw, request.limits.max_input_bytes).is_ok()
                                || original.input != json!({})
                                || !args.is_empty()
                            {
                                true
                            } else {
                                invalid_tools
                                    .insert(original.id.clone(), raw.into())
                                    .is_some()
                            }
                        }
                        None => {
                            serde_json::to_value(args).map_err(|_| invalid())? != original.input
                        }
                    }
                {
                    return Err(error(ErrorCode::ModelContextIncompatible, "replay"));
                }
            }
            let mut replay = blocks.clone();
            for block in &mut replay {
                if block["type"] == "tool_use" {
                    if let Some(raw) = invalid_arguments.and_then(|map| {
                        block
                            .get("id")
                            .and_then(Value::as_str)
                            .and_then(|id| map.get(id))
                    }) {
                        block["input"] = json!({"INVALID_JSON":raw});
                    }
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
                "claude-opus-5"
                    | "claude-opus-5-5"
                    | "claude-sonnet-5"
                    | "claude-opus-4-7"
                    | "claude-opus-4-8"
            ) {
                return Err(error(ErrorCode::ModelOptionUnsupported, "manual_thinking"));
            }
            value["thinking"] = json!({"type":"enabled","budget_tokens":budget});
        }
        Some("adaptive" | "disabled") => {
            if budget.is_some() {
                return Err(error(ErrorCode::ModelOptionUnsupported, "thinking_budget"));
            }
            let model = request
                .route
                .model_id
                .as_str()
                .strip_prefix("anthropic.")
                .unwrap_or(request.route.model_id.as_str());
            if mode == Some("disabled")
                && (model == "claude-opus-5-5"
                    || (model == "claude-opus-5"
                        && matches!(
                            request.options.get("effort").and_then(Value::as_str),
                            Some("xhigh" | "max")
                        )))
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
        let (value, initial_input) = parse_event(&event.data)?;
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
                        let input = initial_input.ok_or_else(invalid)?;
                        // Only an empty object is the standard streaming placeholder.
                        // Everything else retains its original bytes for core validation.
                        if !parse_provider_arguments(input, self.request.limits.max_response_bytes)
                            .is_ok_and(|input| input.is_empty())
                        {
                            arguments = input.to_owned();
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
                        let mut invalid_arguments = serde_json::Map::new();
                        for block in &mut self.blocks {
                            if block.value["type"] == "tool_use" {
                                match parse_provider_arguments(
                                    &block.arguments,
                                    self.request.limits.max_response_bytes,
                                ) {
                                    Ok(input) => block.value["input"] = json!(input),
                                    Err(_) => {
                                        invalid_arguments.insert(
                                            nonempty(&block.value, "id")?.into(),
                                            json!(block.arguments),
                                        );
                                        block.value["input"] = json!({});
                                    }
                                }
                            }
                        }
                        let blocks: Vec<_> = self.blocks.iter().map(|b| b.value.clone()).collect();
                        codec::inspect_blocks(&blocks)?;
                        if !blocks.is_empty() {
                            let data = if invalid_arguments.is_empty() {
                                json!({"kind":codec::REPLAY_KIND,"blocks":blocks})
                            } else {
                                json!({"kind":codec::INVALID_REPLAY_KIND,"blocks":blocks,"invalid_arguments":invalid_arguments})
                            };
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

// Keep Tool input out of envelope parsing: duplicate keys, non-object values
// and numbers outside Value's representation belong to the core repair path.
// Strict parsing still sees every original byte outside that single value.
fn parse_event(event: &str) -> Result<(Value, Option<&str>), ContractError> {
    type RawMap<'a> = std::collections::BTreeMap<String, &'a serde_json::value::RawValue>;
    let fields: RawMap<'_> = serde_json::from_str(event).map_err(|_| invalid())?;
    let kind = fields
        .get("type")
        .and_then(|value| serde_json::from_str::<String>(value.get()).ok());
    if kind.as_deref() != Some("content_block_start") {
        return Ok((parse_json(event)?, None));
    }
    let block = *fields.get("content_block").ok_or_else(invalid)?;
    let block: RawMap<'_> = serde_json::from_str(block.get()).map_err(|_| invalid())?;
    let kind = block
        .get("type")
        .and_then(|value| serde_json::from_str::<String>(value.get()).ok());
    if kind.as_deref() != Some("tool_use") {
        return Ok((parse_json(event)?, None));
    }
    let input = *block.get("input").ok_or_else(invalid)?;
    let input = input.get();
    // Borrowed RawValue points into this event. Check the span before slicing;
    // retaining surrounding bytes also retains duplicate envelope keys for rejection.
    let start = (input.as_ptr() as usize)
        .checked_sub(event.as_ptr() as usize)
        .ok_or_else(invalid)?;
    let end = start.checked_add(input.len()).ok_or_else(invalid)?;
    if event.get(start..end) != Some(input) {
        return Err(invalid());
    }
    let envelope = format!(
        "{}{{}}{}",
        event.get(..start).ok_or_else(invalid)?,
        event.get(end..).ok_or_else(invalid)?
    );
    Ok((parse_json(&envelope)?, Some(input)))
}
```

## `crates/wickle-model-anthropic/tests/agent_contract.rs`

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
use wickle_model_anthropic::*;
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
fn call_events(index: usize, arguments: &str) -> Vec<Value> {
    let mut data = support::events("claude-opus-5", "");
    data.truncate(4);
    data[0]["message"]["id"] = json!(format!("msg_{index}"));
    data.extend([
        json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":format!("call_{index}"),"name":"lookup","input":{}}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":arguments}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":20}}),
        json!({"type":"message_stop"}),
    ]);
    data
}
#[tokio::test]
async fn native_schema_constraints_and_invalid_arguments_repair_before_scoped_execution() {
    let invalid_constraint =
        json!({"query":"latest","limit":7,"note":null,"filter":{"category":"finance"}}).to_string();
    let invalid_json =
        json!({"query":"latest","limit":9,"note":null,"filter":"invalid-shape"}).to_string();
    let valid =
        json!({"query":"latest","limit":9,"note":null,"filter":{"category":"finance"}}).to_string();
    let malformed = "{not json";
    let rounded = r#"{"query":"latest","limit":0.12345678901234567890123456789}"#;
    let server = support::Server::new(vec![
        support::Reply::sse(&call_events(0, malformed)),
        support::Reply::sse(&call_events(4, rounded)),
        support::Reply::sse(&call_events(1, &invalid_constraint)),
        support::Reply::sse(&call_events(2, &invalid_json)),
        support::Reply::sse(&call_events(3, &valid)),
        support::Reply::sse(&support::events("claude-opus-5", "complete")),
    ])
    .await;
    let connection = AnthropicConnection::new(
        scope(),
        reference("account"),
        "fixture-key-not-a-secret",
        AnthropicOptions {
            base_url: server.base.trim_end_matches("v1/").into(),
            ..Default::default()
        },
    )
    .unwrap();
    let fixture = core_host::Fixture::new(core_host::Response::Text, false);
    let mut catalog = fixture.router.snapshot.catalog().clone();
    catalog.models[0].provider = id("anthropic");
    catalog.models[0].model_id = id("claude-opus-5");
    catalog.models[0]
        .capabilities
        .features
        .insert(id("tool_calling"));
    catalog.bindings[0].model = catalog.models[0].reference();
    catalog.bindings[0].requested_model = id("claude-opus-5");
    catalog.bindings[0].adapter = connection.binding().adapter;
    catalog.bindings[0].connection_ref = connection.binding().connection_ref;
    catalog.bindings[0].api_contract = AnthropicConnection::api_contract();
    catalog.bindings[0].target = connection.target().clone();
    catalog.bindings[0].target_schema = json!({"type":"object","properties":{"base_url":{"type":"string"}},"required":["base_url"],"additionalProperties":false});
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
        ModelExchange::new(
            Arc::new(AnthropicModel::new(connection)),
            bindings.policy.clone(),
        )
        .with_route_inspector(Arc::new(Inspector), Duration::from_secs(1))
        .unwrap(),
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
            "filter":{"type":"object","properties":{"category":{"type":"string"},"term":{"type":"string"}},"required":["category"],"additionalProperties":false},
            "workspace_id":{"type":"string","format":"uuid"}
        },"required":["query","workspace_id"],"additionalProperties":false,"if":{"properties":{"query":{"const":"latest"}}},"then":{"properties":{"limit":{"minimum":8}}}}),
        agent_parameters: vec!["query".into(),"limit".into(),"note".into(),"filter".into()], system_bindings: None,
        output_schema: json!({"type":"string"}), side_effect: ToolSideEffect::ReadOnly, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false, max_output_bytes: 4096.try_into().unwrap(),
    }, &bindings.system_inputs).unwrap();
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
    profile.limits.max_model_calls = 7.try_into().unwrap();
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
    let handle = completed(
        agent
            .start(core_host::request("contract"), context.clone())
            .await
            .unwrap(),
    );
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
    assert_eq!(requests.len(), 6);
    for request in requests.iter() {
        assert_eq!(
            request.body["tools"][0]["input_schema"]["required"],
            json!(["query"])
        );
        assert_eq!(request.path, "/v1/messages");
        assert!(!request.body.to_string().contains(WORKSPACE));
    }
    let content: Vec<_> = requests[5].body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|message| message["content"].as_array().unwrap())
        .collect();
    let replayed: Vec<_> = content
        .iter()
        .filter(|item| item["type"] == "tool_use")
        .map(|item| item["input"].clone())
        .collect();
    assert_eq!(
        replayed,
        vec![
            json!({"INVALID_JSON":malformed}),
            json!({"INVALID_JSON":rounded}),
            parse_json(&invalid_constraint).unwrap(),
            parse_json(&invalid_json).unwrap(),
            parse_json(&valid).unwrap()
        ]
    );
    let failures: Vec<_> = content
        .iter()
        .filter(|item| item["type"] == "tool_result" && item["is_error"] == true)
        .collect();
    assert_eq!(failures.len(), 2);
    for (result, raw) in failures.into_iter().zip([malformed, rounded]) {
        assert_eq!(
            parse_json(result["content"].as_str().unwrap()).unwrap()["INVALID_JSON"],
            raw
        );
    }
    let thinking: Vec<_> = content
        .iter()
        .filter(|item| item["type"] == "thinking")
        .collect();
    assert_eq!(thinking.len(), 5);
    assert!(
        thinking
            .iter()
            .all(|block| block["signature"] == "signature-fixture")
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
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
    assert_eq!(
        response.continuation[0].data()["kind"],
        "wickle.anthropic.messages.v1"
    );
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
    ] {
        let mut data = events("claude-opus-5", "answer");
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

#[tokio::test]
async fn two_documented_release_ids_coexist_without_replacing_connection_state() {
    let releases = ["claude-opus-5", "claude-opus-4-8"];
    let server = Server::new(
        releases
            .iter()
            .map(|release| Reply::sse(&events(release, release)))
            .collect(),
    )
    .await;
    let connection = connection(&server);
    let model = AnthropicModel::new(connection.clone());
    for (index, release) in releases.iter().enumerate() {
        let mut request = request(&connection, release);
        request.route.binding = reference(&format!("binding-{index}"));
        request.route.model_version = id(&format!("fixture-revision-{index}"));
        let result = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(result.text, *release);
        assert_eq!(result.metadata.reported_model_id, Some(id(release)));
        assert!(result.metadata.reported_model_version.is_none());
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (request, release) in requests.iter().zip(releases) {
        assert_eq!(request.body["model"], release);
    }
}

fn invalid_tool_reply(raw: &str, initial: bool) -> Reply {
    let mut data = tool_events();
    data[5]["delta"]["partial_json"] = json!(raw);
    data.remove(6);
    if initial {
        data[4]["content_block"]["input"] = json!({"query":"initial"});
        data.remove(5);
    }
    let mut reply = Reply::sse(&data);
    if initial {
        reply.body = String::from_utf8(reply.body)
            .unwrap()
            .replace(
                r#""input":{"query":"initial"}"#,
                &format!("\"input\":{raw}"),
            )
            .into_bytes();
    }
    reply
}
#[tokio::test]
async fn complete_invalid_arguments_preserve_raw_evidence_and_signed_thinking_for_repair() {
    for (raw, initial) in [
        (r#"{"query":"unfinished""#, false),
        (r#"{"query":0.12345678901234567890123456789}"#, false),
        (r#"{"query":0.12345678901234567890123456789}"#, true),
        (r#"{"query":"one","query":"two"}"#, false),
        (r#"{"query":"one","query":"two"}"#, true),
        (r#"{"query":1e400}"#, true),
        (r#"["not an object"]"#, true),
        (r#"["not an object"]"#, false),
    ] {
        let server = Server::new(vec![
            invalid_tool_reply(raw, initial),
            Reply::sse(&events("claude-opus-5", "repair acknowledged")),
        ])
        .await;
        let connection = connection(&server);
        let model = AnthropicModel::new(connection.clone());
        let mut request = request(&connection, "claude-opus-5");
        tool(&mut request);
        let first = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(first.finish, ModelFinish::ToolCalls);
        assert_eq!(first.tool_calls[0].raw_arguments.as_deref(), Some(raw));
        assert!(first.tool_calls[0].model_inputs.is_empty());
        assert_eq!(
            first.tool_calls[0].validation,
            ToolCallValidation::InvalidArguments
        );
        assert_eq!(
            first.continuation[0].data()["kind"],
            "wickle.anthropic.messages.v2"
        );
        request.messages.push(ModelMessage {
            role: ModelRole::Assistant,
            content: vec![
                ModelContent::ToolCall {
                    provider_call_id: id("toolu_1"),
                    name: id("lookup"),
                    arguments: JsonObject::new(),
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
                content: json!({"status":"failed","error":{"code":"invalid_arguments"}}),
            }],
        });
        request.request_id = id("repair");
        let second = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(second.text, "repair acknowledged");
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let body = &requests[1].body;
        assert_eq!(
            body["messages"][1]["content"][0],
            json!({"type":"thinking","thinking":"","signature":"signature-fixture"})
        );
        assert_eq!(
            body["messages"][1]["content"][1]["input"],
            json!({"INVALID_JSON":raw})
        );
        let result = &body["messages"][2]["content"][0];
        assert_eq!(result["is_error"], true);
        assert_eq!(
            parse_json(result["content"].as_str().unwrap()).unwrap()["INVALID_JSON"],
            raw
        );
        drop(requests);
        for mode in ["valid-raw", "unknown-call", "changed-input", "extra-field"] {
            let mut changed = first.continuation[0].data().clone();
            match mode {
                "valid-raw" => changed["invalid_arguments"]["toolu_1"] = json!("{}"),
                "unknown-call" => changed["invalid_arguments"]["other"] = json!("{"),
                "changed-input" => changed["blocks"][1]["input"] = json!({"query":"tampered"}),
                _ => changed["extra"] = json!(true),
            }
            request.messages[1].content[1] = ModelContent::Opaque {
                continuation: OpaqueContinuation::new(&request.route, changed),
            };
            assert!(
                wickle_model_anthropic::protocol::encode_request(&request).is_err(),
                "{mode}"
            );
        }
        request.messages[1].content[1] = ModelContent::Opaque {
            continuation: first.continuation[0].clone(),
        };
        if let ModelContent::ToolCall { arguments, .. } = &mut request.messages[1].content[0] {
            arguments.insert("query".into(), json!("tampered"));
        }
        assert!(wickle_model_anthropic::protocol::encode_request(&request).is_err());
        assert_eq!(server.requests.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn length_limited_tool_input_never_becomes_a_repairable_completed_proposal() {
    let mut data = tool_events();
    data[6]["delta"]["partial_json"] = json!("\"cut off");
    data[8]["delta"]["stop_reason"] = json!("max_tokens");
    let server = Server::new(vec![Reply::sse(&data)]).await;
    let connection = connection(&server);
    let model = AnthropicModel::new(connection.clone());
    let mut request = request(&connection, "claude-opus-5");
    tool(&mut request);
    let result =
        collect_model_response(&request, model.generate(&request, &context(&request))).await;
    assert!(result.is_err());
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn opus_five_point_five_keeps_adaptive_thinking_and_rejects_disabled_or_manual_modes() {
    let server = Server::new(vec![Reply::sse(&events("claude-opus-5-5", "done"))]).await;
    let connection = connection(&server);
    let model = AnthropicModel::new(connection.clone());
    let mut request = request(&connection, "claude-opus-5-5");
    request.max_output_tokens = 4096.try_into().unwrap();
    for mode in ["disabled", "enabled"] {
        request.options.insert("thinking_mode".into(), json!(mode));
        if mode == "enabled" {
            request
                .options
                .insert("thinking_budget_tokens".into(), json!(1024));
        }
        assert!(
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .is_err()
        );
        assert!(server.requests.lock().unwrap().is_empty());
    }
    request.options.remove("thinking_budget_tokens");
    request
        .options
        .insert("thinking_mode".into(), json!("adaptive"));
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].body["thinking"], json!({"type":"adaptive"}));
    assert_eq!(requests[0].body["output_config"]["effort"], "medium");
}

#[tokio::test]
async fn initial_input_exemption_never_hides_duplicate_or_malformed_envelope_fields() {
    for mode in ["duplicate-index", "duplicate-input", "malformed-outer"] {
        let mut reply = invalid_tool_reply(r#"{"query":1e400}"#, true);
        let body = String::from_utf8(reply.body).unwrap();
        reply.body = match mode {
            "duplicate-index" => body.replacen("\"index\":1", "\"index\":1,\"index\":2", 1),
            "duplicate-input" => body.replacen("\"input\":", "\"input\":{},\"input\":", 1),
            _ => body.replacen("\"index\":1", "\"index\":1,\"unexpected\":", 1),
        }
        .into_bytes();
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let model = AnthropicModel::new(connection.clone());
        let mut request = request(&connection, "claude-opus-5");
        tool(&mut request);
        assert!(
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .is_err(),
            "{mode}"
        );
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn mixed_valid_and_invalid_calls_keep_each_result_and_error_marker_with_its_call() {
    let mut data = tool_events();
    data[5]["delta"]["partial_json"] = json!("{");
    data.remove(6);
    data.truncate(7);
    data.extend([
        json!({"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_2","name":"lookup","input":{}}}),
        json!({"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"query\":\"beta\"}"}}),
        json!({"type":"content_block_stop","index":2}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":20}}),
        json!({"type":"message_stop"}),
    ]);
    let server = Server::new(vec![
        Reply::sse(&data),
        Reply::sse(&events("claude-opus-5", "done")),
    ])
    .await;
    let connection = connection(&server);
    let model = AnthropicModel::new(connection.clone());
    let mut request = request(&connection, "claude-opus-5");
    tool(&mut request);
    let first = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(first.tool_calls.len(), 2);
    assert_eq!(
        first.tool_calls[0].validation,
        ToolCallValidation::InvalidArguments
    );
    let mut content: Vec<_> = first
        .tool_calls
        .iter()
        .map(|call| ModelContent::ToolCall {
            provider_call_id: call.provider_call_id.clone(),
            name: call.name.clone(),
            arguments: call.model_inputs.clone(),
        })
        .collect();
    content.push(ModelContent::Opaque {
        continuation: first.continuation[0].clone(),
    });
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content,
    });
    request.messages.push(ModelMessage {
        role: ModelRole::Tool,
        content: vec![
            ModelContent::ToolResult {
                provider_call_id: id("toolu_1"),
                content: json!({"error":"invalid_arguments"}),
            },
            ModelContent::ToolResult {
                provider_call_id: id("toolu_2"),
                content: json!({"answer":42}),
            },
        ],
    });
    request.request_id = id("second");
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let requests = server.requests.lock().unwrap();
    let body = &requests[1].body;
    assert_eq!(
        body["messages"][1]["content"][1]["input"],
        json!({"INVALID_JSON":"{"})
    );
    assert_eq!(
        body["messages"][1]["content"][2]["input"],
        json!({"query":"beta"})
    );
    let results = &body["messages"][2]["content"];
    assert_eq!(results[0]["tool_use_id"], "toolu_1");
    assert_eq!(results[0]["is_error"], true);
    assert_eq!(results[1]["tool_use_id"], "toolu_2");
    assert!(results[1].get("is_error").is_none());
    assert_eq!(
        parse_json(results[1]["content"].as_str().unwrap()).unwrap(),
        json!({"answer":42})
    );
}
```
