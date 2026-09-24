use crate::{
    codec::{KIND, RAW_KIND, inspect_part, invalid},
    error,
};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use wickle::*;
use wickle_model_responses::SseEvent;

/// Validates a single-candidate streamGenerateContent response through clean EOF.
pub struct Decoder<'a> {
    request: &'a ModelRequest,
    /// Provider-reported facts only; omitted versions and usage remain unknown.
    pub metadata: ModelResponseMetadata,
    parts: Vec<Value>,
    ids: Vec<String>,
    raw_parts: std::collections::BTreeMap<String, String>,
    seen_ids: BTreeSet<String>,
    finish: Option<ModelFinish>,
    bytes: usize,
    started: bool,
    vertex: bool,
}
impl<'a> Decoder<'a> {
    /// Start one physical attempt without network access.
    pub fn new(request: &'a ModelRequest) -> Self {
        Self {
            request,
            metadata: Default::default(),
            parts: vec![],
            ids: vec![],
            raw_parts: Default::default(),
            seen_ids: BTreeSet::new(),
            finish: None,
            bytes: 0,
            started: false,
            vertex: false,
        }
    }
    /// Use Vertex's complete function-call metadata while rejecting partial arguments.
    pub fn for_vertex(request: &'a ModelRequest) -> Self {
        Self {
            vertex: true,
            ..Self::new(request)
        }
    }
    /// Consume a complete SSE JSON record. It is never a Tool execution permit.
    pub fn event(&mut self, event: SseEvent) -> Result<Vec<ModelEvent>, ContractError> {
        if event.name.as_deref().is_some_and(|s| s != "message") {
            return Err(invalid());
        }
        let (value, raw_parts) =
            crate::raw::event(&event.data, self.request.limits.max_response_bytes)?;
        if !value.is_object() {
            return Err(invalid());
        }
        if value.get("error").is_some() {
            return Err(error(ErrorCode::ModelUnavailable, "stream_error"));
        }
        self.started = true;
        for (key, destination) in [
            ("responseId", &mut self.metadata.provider_request_id),
            ("modelVersion", &mut self.metadata.reported_model_version),
        ] {
            if let Some(v) = value.get(key) {
                let id = Id::new(v.as_str().ok_or_else(invalid)?)?;
                if destination.as_ref().is_some_and(|old| old != &id) {
                    return Err(invalid());
                }
                *destination = Some(id);
            }
        }
        if let Some(usage) = value.get("usageMetadata") {
            let number = |key| -> Result<Option<u64>, ContractError> {
                usage
                    .get(key)
                    .map(|v| v.as_u64().ok_or_else(invalid))
                    .transpose()
            };
            if !usage.is_object() {
                return Err(invalid());
            }
            let input = number("promptTokenCount")?;
            let candidates = number("candidatesTokenCount")?;
            let thoughts = number("thoughtsTokenCount")?;
            let total = number("totalTokenCount")?;
            let output = match (input, total, candidates, thoughts) {
                (Some(i), Some(t), c, r) => {
                    let out = t.checked_sub(i).ok_or_else(invalid)?;
                    if c.zip(r).is_some_and(|(c, r)| c.checked_add(r) != Some(out)) {
                        return Err(invalid());
                    }
                    Some(out)
                }
                (_, _, Some(c), Some(r)) => Some(c.checked_add(r).ok_or_else(invalid)?),
                _ => None,
            };
            let old = self.metadata.usage.as_ref();
            if old.is_some_and(|u| {
                input.zip(u.input_tokens).is_some_and(|(a, b)| a < b)
                    || output.zip(u.output_tokens).is_some_and(|(a, b)| a < b)
            }) {
                return Err(invalid());
            }
            self.metadata.usage = Some(ModelUsage {
                measurement: UsageMeasurement::Reported,
                input_tokens: input.or_else(|| old.and_then(|u| u.input_tokens)),
                output_tokens: output.or_else(|| old.and_then(|u| u.output_tokens)),
            });
        }
        if value
            .pointer("/promptFeedback/blockReason")
            .is_some_and(|v| v.as_str().is_some_and(|s| s != "BLOCK_REASON_UNSPECIFIED"))
        {
            if !self.parts.is_empty() || self.finish.is_some() {
                return Err(invalid());
            }
            self.finish = Some(ModelFinish::Refusal);
            return Ok(vec![]);
        }
        let candidates = match value.get("candidates") {
            Some(v) => v.as_array().ok_or_else(invalid)?,
            None => return Ok(vec![]),
        };
        if candidates.is_empty() {
            return Ok(vec![]);
        }
        if candidates.len() != 1 || self.finish.is_some() {
            return Err(invalid());
        }
        let candidate = &candidates[0];
        if !candidate.is_object()
            || candidate
                .get("index")
                .is_some_and(|v| v.as_u64() != Some(0))
        {
            return Err(invalid());
        }
        let mut output = vec![];
        if let Some(content) = candidate.get("content") {
            if content.get("role").is_some_and(|v| v != "model") {
                return Err(invalid());
            }
            let parts = content
                .get("parts")
                .and_then(Value::as_array)
                .ok_or_else(invalid)?;
            if parts.len() != raw_parts.len() {
                return Err(invalid());
            }
            for (part, raw) in parts.iter().zip(&raw_parts) {
                if part != &raw.value {
                    return Err(invalid());
                }
                self.bytes = self
                    .bytes
                    .checked_add(raw.original.len())
                    .filter(|n| *n <= self.request.limits.max_response_bytes)
                    .ok_or_else(invalid)?;
                let decoded = inspect_part(part, self.vertex)?;
                for text in chunks(&decoded.text, self.request.limits.max_delta_bytes)? {
                    output.push(ModelEvent::TextDelta { text });
                }
                if let Some(call) = decoded.call {
                    if self.ids.len() >= self.request.limits.max_tool_calls {
                        return Err(invalid());
                    }
                    let index = u32::try_from(self.ids.len()).map_err(|_| invalid())?;
                    let id = call.id.unwrap_or_else(|| {
                        format!(
                            "gemini-{}",
                            canonical_digest(&json!([self.request.request_id, index]))
                        )
                    });
                    Id::new(&id)?;
                    if !self.seen_ids.insert(id.clone()) {
                        return Err(invalid());
                    }
                    let args = raw
                        .arguments
                        .map(str::to_owned)
                        .unwrap_or_else(|| call.args.to_string());
                    for (n, delta) in chunks(&args, self.request.limits.max_delta_bytes)?
                        .into_iter()
                        .enumerate()
                    {
                        output.push(ModelEvent::ToolArgumentsDelta {
                            index,
                            provider_call_id: (n == 0).then(|| id.clone()),
                            name: (n == 0).then(|| call.name.clone()),
                            delta,
                        });
                    }
                    self.ids.push(id);
                }
                if raw.invalid_arguments {
                    self.raw_parts
                        .insert(self.parts.len().to_string(), raw.original.into());
                }
                self.parts.push(part.clone());
            }
        }
        if let Some(reason) = candidate.get("finishReason") {
            let reason = reason.as_str().ok_or_else(invalid)?;
            self.finish = match reason {
                "FINISH_REASON_UNSPECIFIED" => None,
                "STOP" => Some(if self.ids.is_empty() {
                    ModelFinish::Stop
                } else {
                    ModelFinish::ToolCalls
                }),
                "MAX_TOKENS" => Some(ModelFinish::Length),
                "SAFETY" | "RECITATION" | "BLOCKLIST" | "PROHIBITED_CONTENT" | "SPII" => {
                    Some(ModelFinish::Refusal)
                }
                _ => return Err(invalid()),
            };
        }
        Ok(output)
    }
    /// Produce completion only after the HTTP/SSE framing has cleanly ended.
    pub fn finish(&mut self) -> Result<ModelEvent, ContractError> {
        if !self.started {
            return Err(invalid());
        }
        let finish = self.finish.take().ok_or_else(invalid)?;
        Ok(ModelEvent::ResponseCompleted {
            finish,
            metadata: self.metadata.clone(),
            continuation: if self.parts.is_empty()
                || !matches!(finish, ModelFinish::Stop | ModelFinish::ToolCalls)
            {
                vec![]
            } else {
                vec![OpaqueContinuation::new(
                    &self.request.route,
                    if self.raw_parts.is_empty() {
                        json!({"kind":KIND,"parts":self.parts,"call_ids":self.ids})
                    } else {
                        json!({"kind":RAW_KIND,"parts":self.parts,"call_ids":self.ids,"raw_parts":self.raw_parts})
                    },
                )]
            },
        })
    }
}
fn chunks(value: &str, max: usize) -> Result<Vec<String>, ContractError> {
    let mut rest = value;
    let mut result = vec![];
    while !rest.is_empty() {
        let mut n = rest.len().min(max);
        while !rest.is_char_boundary(n) {
            n -= 1;
        }
        if n == 0 {
            return Err(invalid());
        }
        result.push(rest[..n].into());
        rest = &rest[n..];
    }
    Ok(result)
}
