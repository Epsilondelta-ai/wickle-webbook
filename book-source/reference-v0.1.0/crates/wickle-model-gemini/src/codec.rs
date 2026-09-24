use crate::{connection::model_name, error};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use wickle::*;

pub(crate) const KIND: &str = "wickle.gemini.generate_content.v1";
pub(crate) fn invalid() -> ContractError {
    error(ErrorCode::ModelContextIncompatible, "content")
}
/// Function declaration schema representation supported by the selected endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FunctionSchemaFormat {
    /// Stable Gemini API OpenAPI Schema fields, without additionalProperties.
    OpenApi,
    /// JSON Schema function declarations, supported by Gemini v1beta.
    JsonSchema,
}
/// Encode only authorized content and explicitly supported logical options.
pub fn encode_request(
    request: &ModelRequest,
    format: FunctionSchemaFormat,
) -> Result<Value, ContractError> {
    encode(request, format, false)
}
/// Encode Vertex v1 using JSON function schemas and its current text response format.
pub fn encode_vertex_request(request: &ModelRequest) -> Result<Value, ContractError> {
    let mut body = encode(request, FunctionSchemaFormat::JsonSchema, true)?;
    if !request.tools.is_empty() {
        body["toolConfig"] = json!({"functionCallingConfig":{"streamFunctionCallArguments":false}});
    }
    if let ModelOutput::JsonSchema { schema } = &request.output {
        let config = body["generationConfig"]
            .as_object_mut()
            .ok_or_else(invalid)?;
        config.remove("responseMimeType");
        config.remove("responseJsonSchema");
        config.insert(
            "responseFormat".into(),
            json!([{"text":{"mimeType":"APPLICATION_JSON","schema":schema}}]),
        );
    }
    Ok(body)
}
fn encode(
    request: &ModelRequest,
    format: FunctionSchemaFormat,
    vertex: bool,
) -> Result<Value, ContractError> {
    request.validate()?;
    let mut contents: Vec<Value> = vec![];
    let mut system = vec![];
    let mut calls: BTreeMap<String, (String, Option<String>)> = BTreeMap::new();
    let mut call_order: Vec<String> = vec![];
    let mut messages = request.messages.iter().peekable();
    while let Some(message) = messages.next() {
        let mut ordered: Vec<_> = message.content.iter().collect();
        if message.role == ModelRole::Assistant {
            call_order.clear();
        }
        if message.role == ModelRole::Tool {
            // Calls without wire IDs are associated by position. Results may arrive
            // in completion order, including separate contiguous Tool messages.
            while messages.peek().is_some_and(|m| m.role == ModelRole::Tool) {
                ordered.extend(messages.next().expect("peeked message").content.iter());
            }
            ordered.sort_by_key(|part| match part {
                ModelContent::ToolResult {
                    provider_call_id, ..
                } => call_order
                    .iter()
                    .position(|id| id == provider_call_id.as_str())
                    .unwrap_or(usize::MAX),
                _ => usize::MAX,
            });
        }
        let opaque: Vec<_> = message
            .content
            .iter()
            .filter_map(|p| {
                if let ModelContent::Opaque { continuation } = p {
                    Some(continuation)
                } else {
                    None
                }
            })
            .collect();
        let parts = if opaque.is_empty() {
            let mut parts = vec![];
            for part in ordered {
                parts.push(match part {
                    ModelContent::Text{text} if message.role!=ModelRole::Tool=>json!({"text":text}),
                    ModelContent::Json{value} if message.role!=ModelRole::Tool=>json!({"text":value.to_string()}),
                    ModelContent::ToolCall{provider_call_id,name,arguments} if message.role==ModelRole::Assistant=>{
                        call_order.push(provider_call_id.to_string());
                        calls.insert(provider_call_id.to_string(),(name.to_string(),Some(provider_call_id.to_string())));
                        json!({"functionCall":{"id":provider_call_id,"name":name,"args":arguments}})
                    },
                    ModelContent::ToolResult{provider_call_id,content} if message.role==ModelRole::Tool=>{
                        let (name,wire_id)=calls.get(provider_call_id.as_str()).ok_or_else(invalid)?;
                        let mut value=json!({"name":name,"response":if content.is_object(){content.clone()}else{json!({"result":content})}});
                        if let Some(id)=wire_id {value["id"]=json!(id);}
                        json!({"functionResponse":value})
                    },
                    _=>return Err(invalid()),
                });
            }
            parts
        } else {
            if opaque.len() != 1
                || message.role != ModelRole::Assistant
                || opaque[0].route_digest() != &request.route.digest()
            {
                return Err(invalid());
            }
            let data = opaque[0].data();
            if data["kind"] != KIND || data.as_object().is_none_or(|v| v.len() != 3) {
                return Err(invalid());
            }
            let parts = data["parts"].as_array().ok_or_else(invalid)?;
            let ids: Vec<String> =
                serde_json::from_value(data["call_ids"].clone()).map_err(|_| invalid())?;
            let mut text = String::new();
            let mut decoded = vec![];
            for part in parts {
                let item = inspect_part(part, vertex)?;
                text.push_str(&item.text);
                if let Some(call) = item.call {
                    decoded.push(call);
                }
            }
            let visible: String = message
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
            let projected: Vec<_> = message
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
            if text != visible
                || decoded.len() != projected.len()
                || ids.len() != decoded.len()
                || message.content.iter().any(|p| {
                    !matches!(
                        p,
                        ModelContent::Text { .. }
                            | ModelContent::ToolCall { .. }
                            | ModelContent::Opaque { .. }
                    )
                })
            {
                return Err(invalid());
            }
            for ((call, (id, name, args)), local) in decoded.iter().zip(projected).zip(&ids) {
                if id.as_str() != local
                    || name.as_str() != call.name
                    || serde_json::to_value(args).map_err(|_| invalid())? != call.args
                    || call.id.as_ref().is_some_and(|wire| wire != local)
                {
                    return Err(invalid());
                }
                call_order.push(local.clone());
                calls.insert(local.clone(), (call.name.clone(), call.id.clone()));
            }
            parts.clone()
        };
        if message.role == ModelRole::System {
            if !contents.is_empty() {
                return Err(invalid());
            }
            system.extend(parts);
        } else {
            let role = if message.role == ModelRole::Assistant {
                "model"
            } else {
                "user"
            };
            if let Some(last) = contents.last_mut().filter(|v| v["role"] == role) {
                last["parts"]
                    .as_array_mut()
                    .ok_or_else(invalid)?
                    .extend(parts);
            } else {
                contents.push(json!({"role":role,"parts":parts}));
            }
        }
    }
    if contents.last().is_some_and(|v| v["role"] == "model") {
        return Err(invalid());
    }
    let mut config = json!({"candidateCount":1,"maxOutputTokens":request.max_output_tokens});
    if request.max_output_tokens.get() > i32::MAX as u64 {
        return Err(error(
            ErrorCode::ModelOptionUnsupported,
            "max_output_tokens",
        ));
    }
    let mut thinking = serde_json::Map::new();
    for (key, value) in &request.options {
        match key.as_str() {
            "thinking_level" => {
                let level = value
                    .as_str()
                    .filter(|s| matches!(*s, "minimal" | "low" | "medium" | "high"))
                    .ok_or_else(|| error(ErrorCode::ModelOptionUnsupported, "thinking_level"))?;
                if matches!(
                    model_name(request.route.model_id.as_str())?,
                    "gemini-3.7-flash" | "gemini-3.8-flash"
                ) && level == "minimal"
                {
                    return Err(error(ErrorCode::ModelOptionUnsupported, "thinking_level"));
                }
                thinking.insert("thinkingLevel".into(), json!(level.to_ascii_uppercase()));
            }
            "thinking_budget_tokens" => {
                let n = value
                    .as_i64()
                    .filter(|n| *n >= -1 && *n <= i32::MAX as i64)
                    .ok_or_else(|| error(ErrorCode::ModelOptionUnsupported, "thinking_budget"))?;
                thinking.insert("thinkingBudget".into(), json!(n));
            }
            "temperature" | "top_p" => {
                let max = if key == "temperature" { 2.0 } else { 1.0 };
                let n = value
                    .as_f64()
                    .filter(|n| *n >= 0.0 && *n <= max)
                    .ok_or_else(|| error(ErrorCode::ModelOptionUnsupported, "sampling"))?;
                config[if key == "temperature" {
                    "temperature"
                } else {
                    "topP"
                }] = json!(n);
            }
            _ => return Err(error(ErrorCode::ModelOptionUnsupported, "options")),
        }
    }
    if thinking.len() > 1 {
        return Err(error(
            ErrorCode::ModelOptionUnsupported,
            "thinking_combination",
        ));
    }
    if !thinking.is_empty() {
        config["thinkingConfig"] = Value::Object(thinking);
    }
    if let ModelOutput::JsonSchema { schema } = &request.output {
        schema_value(schema, FunctionSchemaFormat::JsonSchema)?;
        config["responseMimeType"] = json!("application/json");
        config["responseJsonSchema"] = schema.clone();
    }
    let mut body = json!({"contents":contents,"generationConfig":config});
    if !system.is_empty() {
        body["systemInstruction"] = json!({"parts":system});
    }
    if !request.tools.is_empty() {
        let declarations = request
            .tools
            .iter()
            .map(|tool| {
                if tool.name.as_str().len() > 128
                    || !tool.name.as_str().bytes().all(|b| {
                        b.is_ascii_alphanumeric() || matches!(b, b'_' | b':' | b'.' | b'-')
                    })
                {
                    return Err(error(
                        ErrorCode::ModelCapabilityUnsupported,
                        "function_name",
                    ));
                }
                let schema = schema_value(&tool.model_input_schema, format)?;
                let mut value = json!({"name":tool.name,"description":tool.description});
                value[match format {
                    FunctionSchemaFormat::OpenApi => "parameters",
                    FunctionSchemaFormat::JsonSchema => "parametersJsonSchema",
                }] = schema;
                Ok(value)
            })
            .collect::<Result<Vec<_>, ContractError>>()?;
        body["tools"] = json!([{"functionDeclarations":declarations}]);
    }
    Ok(body)
}
fn schema_value(schema: &Value, format: FunctionSchemaFormat) -> Result<Value, ContractError> {
    let object = schema
        .as_object()
        .ok_or_else(|| error(ErrorCode::ModelCapabilityUnsupported, "schema"))?;
    if format == FunctionSchemaFormat::OpenApi
        && object.get("enum").is_some_and(|v| {
            object.get("type") != Some(&json!("string"))
                || !v.as_array().is_some_and(|a| a.iter().all(Value::is_string))
        })
    {
        return Err(error(ErrorCode::ModelCapabilityUnsupported, "schema_enum"));
    }
    let mut output = serde_json::Map::new();
    for (key, value) in object {
        let translated = match key.as_str() {
            "properties" => Value::Object(
                value
                    .as_object()
                    .ok_or_else(invalid)?
                    .iter()
                    .map(|(name, sub)| Ok((name.clone(), schema_value(sub, format)?)))
                    .collect::<Result<_, ContractError>>()?,
            ),
            "items" => schema_value(value, format)?,
            "anyOf" => Value::Array(
                value
                    .as_array()
                    .ok_or_else(invalid)?
                    .iter()
                    .map(|v| schema_value(v, format))
                    .collect::<Result<_, _>>()?,
            ),
            "additionalProperties" if format == FunctionSchemaFormat::JsonSchema => {
                if value.is_boolean() {
                    value.clone()
                } else {
                    schema_value(value, format)?
                }
            }
            "type" if format == FunctionSchemaFormat::OpenApi => {
                let t = value
                    .as_str()
                    .filter(|s| {
                        matches!(
                            *s,
                            "object" | "array" | "string" | "integer" | "number" | "boolean"
                        )
                    })
                    .ok_or_else(|| error(ErrorCode::ModelCapabilityUnsupported, "schema_type"))?;
                json!(t.to_ascii_uppercase())
            }
            "type" | "required" | "enum" | "description" | "title" | "minimum" | "maximum"
            | "minItems" | "maxItems" | "format" => value.clone(),
            _ => {
                return Err(error(
                    ErrorCode::ModelCapabilityUnsupported,
                    "schema_keyword",
                ));
            }
        };
        output.insert(key.clone(), translated);
    }
    Ok(Value::Object(output))
}
pub(crate) struct Call {
    pub id: Option<String>,
    pub name: String,
    pub args: Value,
}
pub(crate) struct Part {
    pub text: String,
    pub call: Option<Call>,
}
pub(crate) fn inspect_part(part: &Value, vertex: bool) -> Result<Part, ContractError> {
    let object = part.as_object().ok_or_else(invalid)?;
    if object.keys().any(|k| {
        !matches!(
            k.as_str(),
            "text" | "thought" | "thoughtSignature" | "functionCall"
        )
    }) {
        return Err(error(ErrorCode::CapabilityUnsupported, "part"));
    }
    if part.get("thought").is_some_and(|v| !v.is_boolean())
        || part
            .get("thoughtSignature")
            .is_some_and(|v| v.as_str().is_none_or(str::is_empty))
    {
        return Err(invalid());
    }
    let mut result = Part {
        text: String::new(),
        call: None,
    };
    if let Some(call) = part.get("functionCall") {
        if part.get("text").is_some() || part.get("thought") == Some(&json!(true)) {
            return Err(invalid());
        }
        let call = call.as_object().ok_or_else(invalid)?;
        if call.keys().any(|k| {
            !matches!(k.as_str(), "name" | "args" | "id")
                && !(vertex && matches!(k.as_str(), "willContinue" | "partialArgs"))
        }) || call.get("willContinue").is_some_and(|v| v != &json!(false))
            || call
                .get("partialArgs")
                .is_some_and(|v| v.as_array().is_none_or(|v| !v.is_empty()))
        {
            return Err(error(
                ErrorCode::CapabilityUnsupported,
                "partial_function_call",
            ));
        }
        let name = call
            .get("name")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(invalid)?;
        let id = call
            .get("id")
            .map(|v| {
                v.as_str()
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .ok_or_else(invalid)
            })
            .transpose()?;
        let args = call.get("args").cloned().unwrap_or_else(|| json!({}));
        if !args.is_object() {
            return Err(invalid());
        }
        result.call = Some(Call {
            id,
            name: name.into(),
            args,
        });
    } else if let Some(text) = part.get("text") {
        let text = text.as_str().ok_or_else(invalid)?;
        if part.get("thought") != Some(&json!(true)) {
            result.text = text.into();
        }
    } else if !object.contains_key("thoughtSignature") {
        return Err(invalid());
    }
    Ok(result)
}
