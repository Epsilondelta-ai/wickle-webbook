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
