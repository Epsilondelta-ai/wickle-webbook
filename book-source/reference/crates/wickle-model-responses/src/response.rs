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
