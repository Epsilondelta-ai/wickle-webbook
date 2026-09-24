# 31장 전체 Rust 구현과 테스트

[강의로](../31-xai.md) · [전체 변경 패치](../solutions/31-xai.patch)

기준 `9b0fb6a0afad516bf16025b3759de0495e57088e`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

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
            || (matches!(request.route.model_id.as_str(), "grok-4.6" | "grok-4.5")
                && effort == "none")
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
pub use codec::{encode_request, encode_xai_request};
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
    xai: bool,
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
            xai: false,
        }
    }
    /// Use xAI's documented reasoning identifiers and legacy/modern usage formats.
    pub fn for_xai(request: &'a ModelRequest, request_id: Option<Id>) -> Self {
        Self {
            xai: true,
            ..Self::new(request, request_id)
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
                let item_id = item_id(item, self.xai)?;
                let item_type = string(item, "type")?;
                if !matches!(item_type, "message" | "reasoning" | "function_call") {
                    return Err(error(ErrorCode::CapabilityUnsupported, "output_item"));
                }
                if (!item_id.is_empty() && self.items.values().any(|(_, id)| id == item_id))
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
                if identity.0 != string(item, "type")? || identity.1 != item_id(item, self.xai)? {
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
                        let decoded = codec::inspect_output_items(items, self.xai)?;
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
                                || identity.1 != item_id(item, self.xai)?
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
                            let data = json!({"kind":if self.xai {codec::XAI_REPLAY_KIND} else {codec::REPLAY_KIND},"items":items});
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
            self.metadata.usage = Some(if self.xai {
                xai_usage(usage)?
            } else {
                ModelUsage {
                    measurement: UsageMeasurement::Reported,
                    input_tokens: count("input_tokens")?,
                    output_tokens: count("output_tokens")?,
                }
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

fn item_id(item: &Value, xai: bool) -> Result<&str, ContractError> {
    if xai && item["type"] == "reasoning" {
        match item.get("id") {
            None => Ok(""),
            Some(value) => value.as_str().ok_or_else(invalid),
        }
    } else {
        string(item, "id")
    }
}
fn xai_usage(usage: &Value) -> Result<ModelUsage, ContractError> {
    let count = |key| -> Result<Option<u64>, ContractError> {
        usage
            .get(key)
            .filter(|v| !v.is_null())
            .map(|v| v.as_u64().ok_or_else(invalid))
            .transpose()
    };
    let modern_input = count("input_tokens")?;
    let prompt = count("prompt_tokens")?;
    if modern_input.zip(prompt).is_some_and(|(a, b)| a != b) {
        return Err(invalid());
    }
    let input = modern_input.or(prompt);
    let modern_output = count("output_tokens")?;
    let total = count("total_tokens")?;
    let derived = input
        .zip(total)
        .map(|(i, t)| t.checked_sub(i).ok_or_else(invalid))
        .transpose()?;
    if modern_output.zip(derived).is_some_and(|(a, b)| a != b) {
        return Err(invalid());
    }
    let output = modern_output.or(derived);
    let completion = count("completion_tokens")?;
    let reasoning = usage
        .pointer("/completion_tokens_details/reasoning_tokens")
        .filter(|v| !v.is_null())
        .map(|v| v.as_u64().ok_or_else(invalid))
        .transpose()?;
    if output.is_some_and(|out| {
        completion.is_some_and(|n| n > out) || reasoning.is_some_and(|n| n > out)
    }) {
        return Err(invalid());
    }
    // Legacy completion counts alone do not establish total reasoning-inclusive output.
    Ok(ModelUsage {
        measurement: UsageMeasurement::Reported,
        input_tokens: input,
        output_tokens: output,
    })
}
```

## `crates/wickle-model-xai/src/connection.rs`

```rust
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
pub struct XaiOptions {
    /// API base directory, normally `https://api.x.ai/v1/`.
    pub base_url: String,
    /// Explicit REST API generation: `v1`.
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

impl Default for XaiOptions {
    fn default() -> Self {
        Self {
            base_url: "https://api.x.ai/v1/".into(),
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
pub struct XaiConnection(pub(crate) Arc<Connection>);

pub(crate) struct Connection {
    pub client: Client,
    pub base: Url,
    pub scope: Scope,
    pub binding: ModelPortBinding,
    pub target: JsonObject,
    pub options: XaiOptions,
}

impl fmt::Debug for XaiConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("XaiConnection")
            .field("binding", &self.0.binding)
            .finish_non_exhaustive()
    }
}

impl XaiConnection {
    /// Create a client without a network call. Never log the supplied API key.
    /// HTTPS is required except for explicitly configured loopback test servers.
    pub fn new(
        scope: Scope,
        connection_ref: VersionedRef,
        api_key: &str,
        mut options: XaiOptions,
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
        headers.insert("authorization", authorization);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if options.api_version != "v1" || base.path() != "/v1/" {
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
                provider: Id::new("xai")?,
                adapter: VersionedRef {
                    id: Id::new("wickle-model-xai")?,
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
            operation: Id::new("responses").expect("static identifier"),
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
```

## `crates/wickle-model-xai/src/inspection.rs`

```rust
use crate::{XaiConnection, error};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use wickle::*;

/// A provider-documented immutable model release, registered by the Host.
/// API availability alone, a date in a name, or a requested version is not proof.
#[derive(Debug, Clone)]
pub struct XaiSnapshot {
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
pub struct XaiInspector {
    connection: XaiConnection,
    snapshots: Arc<BTreeMap<Id, XaiSnapshot>>,
}
impl XaiInspector {
    /// Build an inspector without a network call. Duplicate identifiers are rejected.
    pub fn new(
        connection: XaiConnection,
        snapshots: Vec<XaiSnapshot>,
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
impl ModelRouteInspector for XaiInspector {
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
                        evidence_ref: Id::new("xai.models.retrieve")?,
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
                        || Id::new("xai.models.retrieve"),
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

## `crates/wickle-model-xai/src/lib.rs`

```rust
//! xAI Grok Responses with explicit scoped credentials and bounded replay.
#![forbid(unsafe_code)]
mod connection;
mod inspection;
mod model;
pub use connection::{XaiConnection, XaiOptions};
pub use inspection::{XaiInspector, XaiSnapshot};
pub use model::XaiModel;
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

## `crates/wickle-model-xai/tests/responses.rs`

```rust
//! xAI-specific Responses contracts and shared-dialect isolation.
mod support;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::time::Duration;
use support::*;
use wickle::*;
use wickle_model_responses::{ResponsesDecoder, SseEvent};
use wickle_model_xai::*;

fn function_events() -> Vec<Value> {
    let reasoning =
        json!({"id":"","type":"reasoning","summary":[],"encrypted_content":"ciphertext-fixture"});
    let call = json!({"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"query\":\"figures\"}","status":"completed"});
    vec![
        json!({"type":"response.created","response":{"id":"resp_1","model":MODEL,"status":"in_progress"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"","type":"reasoning","summary":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":reasoning}),
        json!({"type":"response.output_item.added","output_index":1,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":"fc_1","delta":"{\"query\":"}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":"fc_1","delta":"\"figures\"}"}),
        json!({"type":"response.function_call_arguments.done","output_index":1,"item_id":"fc_1","arguments":"{\"query\":\"figures\"}"}),
        json!({"type":"response.output_item.done","output_index":1,"item":call}),
        json!({"type":"response.completed","response":{"id":"resp_1","model":MODEL,"status":"completed","output":[reasoning,call]}}),
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
async fn explicit_stateless_contract_and_effort_do_not_leak_host_context() {
    let server = Server::new(vec![
        Reply::sse(&events(MODEL, "answer")),
        Reply::sse(&events(MODEL, "answer")),
    ])
    .await;
    let connection = connection(&server);
    let model = XaiModel::new(connection.clone());
    for effort in ["high", "xhigh"] {
        let mut request = request(&connection, MODEL);
        request
            .options
            .insert("reasoning_effort".into(), json!(effort));
        let response =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap();
        assert_eq!(response.text, "answer");
        assert_eq!(response.metadata.reported_model_id, Some(id(MODEL)));
        assert!(response.metadata.reported_model_version.is_none());
        assert_eq!(response.metadata.usage.unwrap().output_tokens, Some(7));
    }
    let calls = server.requests.lock().unwrap();
    assert_eq!(calls.len(), 2);
    for (call, effort) in calls.iter().zip(["high", "xhigh"]) {
        assert_eq!(call.method, "POST");
        assert_eq!(call.path, "/v1/responses");
        assert!(
            call.headers
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-key-not-a-secret")
        );
        assert_eq!(call.body["store"], false);
        assert_eq!(call.body["include"], json!(["reasoning.encrypted_content"]));
        assert_eq!(call.body["reasoning"]["effort"], effort);
        assert!(!call.body.to_string().contains("hidden-workspace"));
        assert!(call.body.get("previous_response_id").is_none());
    }
}
#[tokio::test]
async fn empty_or_missing_reasoning_ids_are_replayed_without_relaxing_other_dialects() {
    for missing in [false, true] {
        let mut data = function_events();
        if missing {
            data[1]["item"].as_object_mut().unwrap().remove("id");
            data[2]["item"].as_object_mut().unwrap().remove("id");
            data[8]["response"]["output"][0]
                .as_object_mut()
                .unwrap()
                .remove("id");
        }
        let server = Server::new(vec![
            Reply::sse(&data),
            Reply::sse(&events(MODEL, "received")),
        ])
        .await;
        let connection = connection(&server);
        let model = XaiModel::new(connection.clone());
        let mut request = request(&connection, MODEL);
        with_tool(&mut request);
        let mut strict = ResponsesDecoder::new(&request, None);
        assert!(data.iter().any(|value| {
            strict
                .event(SseEvent {
                    name: None,
                    data: value.to_string(),
                })
                .is_err()
        }));
        let first = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(first.finish, ModelFinish::ToolCalls);
        assert_eq!(first.tool_calls[0].provider_call_id, id("call_1"));
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
        assert!(wickle_model_responses::encode_request(&request).is_err());
        let response =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap();
        assert_eq!(response.text, "received");
        {
            let calls = server.requests.lock().unwrap();
            let input = calls[1].body["input"].as_array().unwrap();
            assert_eq!(input[1], data[8]["response"]["output"][0]);
            assert_eq!(input[2]["call_id"], "call_1");
            assert_eq!(input[3]["type"], "function_call_output");
            assert_eq!(
                parse_json(input[3]["output"].as_str().unwrap()).unwrap(),
                json!({"result":73})
            );
            assert_eq!(
                input
                    .iter()
                    .filter(|i| i["type"] == "function_call")
                    .count(),
                1
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
}
#[tokio::test]
async fn legacy_usage_includes_reasoning_without_fabricating_missing_counts() {
    for (usage, input, output, valid) in [
        (
            json!({"prompt_tokens":32,"completion_tokens":9,"completion_tokens_details":{"reasoning_tokens":110},"total_tokens":151}),
            Some(32),
            Some(119),
            true,
        ),
        (
            json!({"input_tokens":32,"output_tokens":119,"total_tokens":151}),
            Some(32),
            Some(119),
            true,
        ),
        (
            json!({"prompt_tokens":32,"completion_tokens":9,"completion_tokens_details":{"reasoning_tokens":110}}),
            Some(32),
            None,
            true,
        ),
        (json!({}), None, None, true),
        (
            json!({"input_tokens":32,"prompt_tokens":33,"output_tokens":9}),
            None,
            None,
            false,
        ),
        (
            json!({"input_tokens":32,"output_tokens":9,"total_tokens":151}),
            None,
            None,
            false,
        ),
        (
            json!({"prompt_tokens":32,"completion_tokens":999,"total_tokens":151}),
            None,
            None,
            false,
        ),
    ] {
        let mut data = events(MODEL, "answer");
        data[5]["response"]["usage"] = usage;
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let request = request(&connection, MODEL);
        let model = XaiModel::new(connection);
        let response =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        if valid {
            let usage = response.unwrap().metadata.usage.unwrap();
            assert_eq!(usage.input_tokens, input);
            assert_eq!(usage.output_tokens, output);
        } else {
            assert!(response.is_err());
        }
    }
}
#[tokio::test]
async fn unsupported_options_scope_schema_and_service_tool_limit_fail_before_http() {
    for case in [
        "none",
        "minimal",
        "max",
        "verbosity",
        "scope",
        "api",
        "target",
        "tools",
        "schema",
    ] {
        let server = Server::new(vec![]).await;
        let connection = connection(&server);
        let mut request = request(&connection, MODEL);
        let mut context = context(&request);
        let model = XaiModel::new(connection);
        match case {
            "none" | "minimal" | "max" => {
                request
                    .options
                    .insert("reasoning_effort".into(), json!(case));
            }
            "verbosity" => {
                request.options.insert("verbosity".into(), json!("low"));
            }
            "scope" => context.scope.workspace_id = id("foreign"),
            "api" => request.route.api_contract.version = id("v2"),
            "target" => {
                request
                    .route
                    .target
                    .insert("base_url".into(), json!("https://other.example/v1/"));
            }
            "tools" => {
                request.limits.max_input_bytes = 1_000_000;
                request.tools=(0..351).map(|n|ModelTool{name:id(&format!("tool-{n}")),description:"Read".into(),model_input_schema:json!({"type":"object","properties":{},"additionalProperties":false})}).collect();
                request.validate().unwrap();
            }
            _ => {
                request.output = ModelOutput::JsonSchema {
                    schema: json!({"type":"object","properties":{"optional":{"type":"string"}},"additionalProperties":false}),
                }
            }
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
async fn absent_executable_ids_ciphertext_or_terminals_never_produce_calls() {
    for case in [
        "item-id",
        "call-id",
        "ciphertext",
        "eof",
        "native",
        "conflict",
    ] {
        let mut data = function_events();
        match case {
            "item-id" => data[3]["item"]["id"] = json!(""),
            "call-id" => data[3]["item"]["call_id"] = json!(""),
            "ciphertext" => {
                data[8]["response"]["output"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("encrypted_content");
            }
            "eof" => {
                data.pop();
            }
            "native" => data[3]["item"]["type"] = json!("web_search_call"),
            _ => data[8]["response"]["output"][1]["arguments"] = json!("{}"),
        }
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let mut request = request(&connection, MODEL);
        with_tool(&mut request);
        let model = XaiModel::new(connection);
        assert!(
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .is_err(),
            "{case}"
        );
    }
}
#[tokio::test]
async fn native_json_output_and_actual_model_reporting_remain_explicit() {
    let server = Server::new(vec![Reply::sse(&events(
        "replacement-model",
        "{\"answer\":42}",
    ))])
    .await;
    let connection = connection(&server);
    let mut request = request(&connection, MODEL);
    let schema = json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false});
    request.output = ModelOutput::JsonSchema {
        schema: schema.clone(),
    };
    let model = XaiModel::new(connection);
    let response = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(
        response.metadata.reported_model_id,
        Some(id("replacement-model"))
    );
    assert_eq!(request.route.model_id, id(MODEL));
    assert!(response.metadata.reported_model_version.is_none());
    let calls = server.requests.lock().unwrap();
    assert_eq!(calls[0].body["text"]["format"]["schema"], schema);
    assert_eq!(calls[0].body["text"]["format"]["strict"], true);
}
#[tokio::test]
async fn cancellation_deadline_errors_and_redirects_close_without_retry() {
    for cancel in [true, false] {
        let mut data = events(MODEL, "partial");
        data.truncate(3);
        let mut reply = Reply::sse(&data);
        reply.stall = true;
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let request = request(&connection, MODEL);
        let model = XaiModel::new(connection);
        let mut context = context(&request);
        context.deadline = tokio::time::Instant::now() + Duration::from_secs(1);
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
    let destination = Server::new(vec![]).await;
    for (status, kind) in [
        (307, ModelFailureKind::Unsupported),
        (401, ModelFailureKind::Authentication),
        (429, ModelFailureKind::RateLimited),
        (503, ModelFailureKind::Transport),
    ] {
        let mut reply = Reply::json(status, json!({"error":{"message":"private detail"}}));
        reply.headers.push(("location", destination.base.clone()));
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let request = request(&connection, MODEL);
        let model = XaiModel::new(connection);
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, kind);
        assert!(!format!("{failure:?}").contains("private detail"));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
    assert!(destination.requests.lock().unwrap().is_empty());
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
#[tokio::test]
async fn models_metadata_does_not_infer_immutable_versions_from_names() {
    let metadata = json!({"object":"model","id":MODEL,"created":12345});
    let server = Server::new(vec![
        Reply::json(200, metadata.clone()),
        Reply::json(200, metadata),
        Reply::json(200, json!({"object":"model","id":"replacement"})),
        Reply::json(404, json!({})),
    ])
    .await;
    let connection = connection(&server);
    let request = request(&connection, MODEL);
    let unknown = XaiInspector::new(connection.clone(), vec![])
        .unwrap()
        .inspect(&request.route, &inspection_context())
        .await
        .unwrap();
    assert_eq!(unknown.version_semantics, VersionSemantics::Unverified);
    assert!(unknown.model_version.is_none());
    let inspector = XaiInspector::new(
        connection,
        vec![XaiSnapshot {
            model_id: id(MODEL),
            model_version: id("release"),
            evidence_ref: id("explicit-immutable-evidence"),
        }],
    )
    .unwrap();
    inspector
        .inspect(&request.route, &inspection_context())
        .await
        .unwrap()
        .validate(&request.route, VersionPolicy::RequirePinned)
        .unwrap();
    assert_eq!(
        inspector
            .inspect(&request.route, &inspection_context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ModelVersionDrift
    );
    assert_eq!(
        inspector
            .inspect(&request.route, &inspection_context())
            .await
            .unwrap()
            .availability,
        ModelRouteAvailability::Unavailable
    );
    let calls = server.requests.lock().unwrap();
    assert_eq!(calls[0].method, "GET");
    assert_eq!(calls[0].path, format!("/v1/models/{MODEL}"));
}

#[tokio::test]
async fn metadata_cancellation_scope_and_size_limits_are_independent_of_inference() {
    for case in ["cancel", "deadline", "scope", "large"] {
        let mut response = Reply::json(200, json!({"object":"model","id":MODEL}));
        response.stall = matches!(case, "cancel" | "deadline");
        if case == "large" {
            response = Reply::json(
                200,
                json!({"object":"model","id":MODEL,"padding":"x".repeat(70_000)}),
            );
        }
        let server = Server::new(vec![response]).await;
        let connection = connection(&server);
        let request = request(&connection, MODEL);
        let inspector = XaiInspector::new(connection, vec![]).unwrap();
        let mut context = inspection_context();
        context.deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        if case == "scope" {
            context.scope.workspace_id = id("foreign");
        }
        let trigger = async {
            if case == "cancel" {
                server.entered.notified().await;
                context.cancellation.cancel();
            }
        };
        let (result, _) = tokio::join!(inspector.inspect(&request.route, &context), trigger);
        assert_eq!(
            result.unwrap_err().code,
            match case {
                "cancel" => ErrorCode::Cancelled,
                "deadline" => ErrorCode::DeadlineExceeded,
                "scope" => ErrorCode::AccessDenied,
                _ => ErrorCode::ModelInspectionUnavailable,
            }
        );
        if case == "scope" {
            assert!(server.requests.lock().unwrap().is_empty());
        } else if matches!(case, "cancel" | "deadline") {
            tokio::time::timeout(Duration::from_secs(1), server.closed.notified())
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn reasoning_exceptions_never_allow_invalid_executable_identities() {
    let mut cases = vec![(None, None, true)];
    for field in ["id", "call_id"] {
        for value in [None, Some(Value::Null), Some(json!(42)), Some(json!(""))] {
            cases.push((Some(field), value, false));
        }
    }
    for (field, value, valid) in cases {
        let mut call = json!({"type":"function_call","id":"fc_1","call_id":"call_1","name":"lookup","arguments":"{\"query\":\"figures\"}","status":"completed"});
        if let Some(field) = field {
            if let Some(value) = value {
                call[field] = value;
            } else {
                call.as_object_mut().unwrap().remove(field);
            }
        }
        let data = vec![
            json!({"type":"response.created","response":{"id":"r","model":MODEL,"status":"in_progress"}}),
            json!({"type":"response.output_item.added","output_index":0,"item":call}),
            json!({"type":"response.output_item.done","output_index":0,"item":call}),
            json!({"type":"response.completed","response":{"id":"r","model":MODEL,"status":"completed","output":[call]}}),
        ];
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let mut request = request(&connection, MODEL);
        with_tool(&mut request);
        let model = XaiModel::new(connection);
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        if valid {
            assert_eq!(result.unwrap().finish, ModelFinish::ToolCalls);
        } else {
            assert!(result.is_err(), "{field:?}");
        }
    }
}
```

## `crates/wickle-model-xai/tests/support/mod.rs`

```rust
use serde_json::{Value, json};
use wickle::*;
use wickle_model_xai::*;

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
pub fn connection(server: &Server) -> XaiConnection {
    XaiConnection::new(
        scope(),
        reference("account"),
        "fixture-key-not-a-secret",
        XaiOptions {
            base_url: server.base.clone(),
            ..Default::default()
        },
    )
    .unwrap()
}
pub fn request(connection: &XaiConnection, model: &str) -> ModelRequest {
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

pub const MODEL: &str = "grok-4.6";
```

## `tests/support/model_adapters_consumer.rs`

```rust
// Cross-provider dispatcher/authorization conformance using extracted adapter packages.
use futures_util::StreamExt;
use std::{net::TcpListener, sync::Arc, time::Duration};
use wickle::*;
use wickle_model_router::{ModelDispatcherEntry, RegistryModelDispatcher};
fn id(value: &str) -> Id {
    Id::new(value).expect("fixture identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn request(
    binding: ModelPortBinding,
    target: JsonObject,
    api_contract: ApiContract,
) -> ModelRequest {
    ModelRequest {
        request_id: id("attempt"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference("selected"),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id("fixture-model"),
            model_id: id("fixture-model"),
            model_version: id("fixture-release"),
            version_semantics: VersionSemantics::Unverified,
            provider: binding.provider,
            target,
            deployment_revision: None,
            api_contract,
            adapter: binding.adapter,
            capability_revision: id("caps"),
            connection_ref: binding.connection_ref,
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Read the authorized fixture".into(),
            }],
        }],
        tools: vec![],
        output: ModelOutput::Text {},
        max_output_tokens: 128.try_into().unwrap(),
        options: JsonObject::new(),
        limits: ModelResponseLimits {
            max_input_bytes: 32768,
            max_response_bytes: 32768,
            max_delta_bytes: 128,
            max_events: 64,
            max_tool_calls: 0,
        },
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let origin = format!("http://{}/", listener.local_addr()?);
    let v1 = format!("{origin}v1/");
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let mut providers: Vec<(Arc<dyn ModelPort>, ModelRequest)> = vec![];
    macro_rules! add {
        ($connection:expr,$model:path,$api:expr) => {{
            let connection = $connection?;
            let request = request(
                connection.binding(),
                connection.target().clone(),
                ($api)(&connection),
            );
            request.validate()?;
            let port: Arc<dyn ModelPort> = Arc::new(<$model>::new(connection));
            providers.push((port, request));
        }};
    }
    add!(
        wickle_model_openai::OpenAiConnection::new(
            scope.clone(),
            reference("shared"),
            "fixture-key",
            wickle_model_openai::OpenAiOptions {
                base_url: v1.clone(),
                ..Default::default()
            }
        ),
        wickle_model_openai::OpenAiModel,
        |_: &wickle_model_openai::OpenAiConnection| {
            wickle_model_openai::OpenAiConnection::api_contract()
        }
    );
    add!(
        wickle_model_azure_openai::AzureOpenAiConnection::new(
            scope.clone(),
            reference("shared"),
            Arc::new(wickle_model_azure_openai::AzureCredential::ApiKey(
                "fixture-key".into()
            )),
            wickle_model_azure_openai::AzureOpenAiOptions::new(&origin, "fixture-deployment")
        ),
        wickle_model_azure_openai::AzureOpenAiModel,
        |_: &wickle_model_azure_openai::AzureOpenAiConnection| {
            wickle_model_azure_openai::AzureOpenAiConnection::api_contract()
        }
    );
    add!(
        wickle_model_anthropic::AnthropicConnection::new(
            scope.clone(),
            reference("shared"),
            "fixture-key",
            wickle_model_anthropic::AnthropicOptions {
                base_url: origin.clone(),
                ..Default::default()
            }
        ),
        wickle_model_anthropic::AnthropicModel,
        |_: &wickle_model_anthropic::AnthropicConnection| {
            wickle_model_anthropic::AnthropicConnection::api_contract()
        }
    );
    let mut bedrock = wickle_model_bedrock::BedrockOptions::new(
        "us-east-1",
        wickle_model_bedrock::BedrockSelector::Foundation("fixture-model".into()),
    );
    bedrock.endpoint_url = Some(origin.clone());
    bedrock.metadata_url = Some(origin.clone());
    add!(
        wickle_model_bedrock::BedrockConnection::new(
            scope.clone(),
            reference("shared"),
            Arc::new(wickle_model_bedrock::BedrockCredential::Bearer(
                "fixture-key".into()
            )),
            bedrock
        ),
        wickle_model_bedrock::BedrockModel,
        |c: &wickle_model_bedrock::BedrockConnection| c.api_contract()
    );
    add!(
        wickle_model_gemini::GeminiConnection::new(
            scope.clone(),
            reference("shared"),
            "fixture-key",
            wickle_model_gemini::GeminiOptions {
                base_url: origin.clone(),
                api_version: "v1beta".into(),
                ..Default::default()
            }
        ),
        wickle_model_gemini::GeminiModel,
        |c: &wickle_model_gemini::GeminiConnection| c.api_contract()
    );
    let mut vertex = wickle_model_vertex::VertexOptions::new("fixture-project");
    vertex.endpoint_url = Some(origin.clone());
    vertex.metadata_url = Some(origin.clone());
    add!(
        wickle_model_vertex::VertexConnection::new(
            scope.clone(),
            reference("shared"),
            Arc::new(wickle_model_vertex::VertexToken::new(
                "fixture-token",
                None
            )?),
            vertex
        ),
        wickle_model_vertex::VertexModel,
        |c: &wickle_model_vertex::VertexConnection| c.api_contract()
    );
    add!(
        wickle_model_xai::XaiConnection::new(
            scope.clone(),
            reference("shared"),
            "fixture-key",
            wickle_model_xai::XaiOptions {
                base_url: v1,
                ..Default::default()
            }
        ),
        wickle_model_xai::XaiModel,
        |c: &wickle_model_xai::XaiConnection| c.api_contract()
    );
    let dispatcher = RegistryModelDispatcher::new(
        providers
            .iter()
            .map(|(port, _)| ModelDispatcherEntry {
                scope: scope.clone(),
                port: port.clone(),
            })
            .collect(),
    )?;
    for (expected, request) in &providers {
        let port = dispatcher.resolve(&scope, &request.route)?;
        assert!(Arc::ptr_eq(&port, expected));
        let mut foreign = scope.clone();
        foreign.workspace_id = id("foreign");
        assert_eq!(
            dispatcher
                .resolve(&foreign, &request.route)
                .err()
                .unwrap()
                .code,
            ErrorCode::ModelBindingInvalid
        );
        let mut missing = request.route.clone();
        missing.connection_ref.version = id("missing");
        assert_eq!(
            dispatcher.resolve(&scope, &missing).err().unwrap().code,
            ErrorCode::ModelBindingInvalid
        );
        let context = ModelCallContext {
            attempt_id: request.request_id.clone(),
            run_id: id("run"),
            scope: foreign,
            cancellation: Default::default(),
            deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        };
        let result = port.generate(request, &context).next().await.unwrap();
        assert_eq!(result.unwrap_err().code, ErrorCode::AccessDenied);
        let mut wrong = request.clone();
        wrong.route.api_contract.version = id("unsupported");
        let context = ModelCallContext {
            scope: scope.clone(),
            ..context
        };
        let result = port.generate(&wrong, &context).next().await.unwrap()?;
        assert!(matches!(
            result,
            ModelEvent::ResponseError {
                kind: ModelFailureKind::Unsupported,
                ..
            }
        ));
    }
    assert!(matches!(listener.accept(),Err(error) if error.kind()==std::io::ErrorKind::WouldBlock));
    println!(
        "Model adapter conformance: all seven concrete providers share one dispatcher without binding collisions; foreign scopes, missing revisions and incompatible API contracts fail before network I/O (extracted packages, no provider calls)"
    );
    Ok(())
}
```

## `tests/support/xai_consumer.rs`

```rust
// Real loopback HTTP/SSE against the extracted xAI adapter package; no provider call.
use serde_json::json;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use wickle::*;
use wickle_model_xai::*;
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
        assert!(headers.contains("authorization: bearer fixture-key"));
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
        let reasoning = json!({"id":"","type":"reasoning","summary":[],"encrypted_content":"ciphertext-fixture"});
        let events = vec![
            json!({"type":"response.created","response":{"id":"response","model":"fixture-model","status":"in_progress"}}),
            json!({"type":"response.output_item.added","output_index":0,"item":{"id":"message","type":"message","role":"assistant","content":[]}}),
            json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"message","delta":"{\"answer\":42}"}),
            json!({"type":"response.output_item.done","output_index":0,"item":item}),
            json!({"type":"response.output_item.added","output_index":1,"item":{"id":"","type":"reasoning","summary":[]}}),
            json!({"type":"response.output_item.done","output_index":1,"item":reasoning}),
            json!({"type":"response.completed","response":{"id":"response","model":"fixture-model","status":"completed","output":[item,reasoning],"usage":{"prompt_tokens":12,"completion_tokens":5,"completion_tokens_details":{"reasoning_tokens":3},"total_tokens":20}}}),
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
    let connection = XaiConnection::new(
        scope.clone(),
        reference("account"),
        "fixture-key",
        XaiOptions {
            base_url: base,
            ..Default::default()
        },
    )?;
    let binding = connection.binding();
    let model = XaiModel::new(connection.clone());
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
        Some(8)
    );
    assert_eq!(response.continuation[0].data()["items"][1]["id"], "");
    assert_eq!(response.continuation[0].data()["items"][1]["encrypted_content"], "ciphertext-fixture");
    server.await?;
    println!(
        "xAI consumer: extracted adapter performs one HTTP/SSE request, preserves scoped endpoint identity and options, decodes JSON and reasoning-inclusive legacy usage, and excludes Host context from the wire (local fixture, no provider network)"
    );
    Ok(())
}
```
