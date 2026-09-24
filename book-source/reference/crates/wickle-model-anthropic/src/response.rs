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
