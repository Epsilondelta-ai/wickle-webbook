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
