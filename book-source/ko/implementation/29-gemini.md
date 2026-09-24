# 29장 전체 Rust 구현과 테스트

[강의로](../29-gemini.md) · [전체 변경 패치](../solutions/29-gemini.patch)

기준 `ec2de798d4e6a6982f4370e31c81eaa82420b28d`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle-model-gemini/src/codec.rs`

```rust
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
                let item = inspect_part(part)?;
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
                if model_name(request.route.model_id.as_str())? == "gemini-3.8-flash"
                    && level == "minimal"
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
pub(crate) fn inspect_part(part: &Value) -> Result<Part, ContractError> {
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
        if call
            .keys()
            .any(|k| !matches!(k.as_str(), "name" | "args" | "id"))
        {
            return Err(invalid());
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
```

## `crates/wickle-model-gemini/src/connection.rs`

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
pub struct GeminiOptions {
    /// API base directory, normally `https://generativelanguage.googleapis.com/`.
    pub base_url: String,
    /// Explicit REST API generation: `v1` or `v1beta`.
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

impl Default for GeminiOptions {
    fn default() -> Self {
        Self {
            base_url: "https://generativelanguage.googleapis.com/".into(),
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
pub struct GeminiConnection(pub(crate) Arc<Connection>);

pub(crate) struct Connection {
    pub client: Client,
    pub base: Url,
    pub scope: Scope,
    pub binding: ModelPortBinding,
    pub target: JsonObject,
    pub options: GeminiOptions,
}

impl fmt::Debug for GeminiConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GeminiConnection")
            .field("binding", &self.0.binding)
            .finish_non_exhaustive()
    }
}

impl GeminiConnection {
    /// Create a client without a network call. Never log the supplied API key.
    /// HTTPS is required except for explicitly configured loopback test servers.
    pub fn new(
        scope: Scope,
        connection_ref: VersionedRef,
        api_key: &str,
        mut options: GeminiOptions,
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
        let mut authorization = HeaderValue::from_str(api_key)
            .map_err(|_| error(ErrorCode::InvalidConfiguration, "credential"))?;
        authorization.set_sensitive(true);
        headers.insert("x-goog-api-key", authorization);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if !matches!(options.api_version.as_str(), "v1" | "v1beta") || base.path() != "/" {
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
                provider: Id::new("google-gemini")?,
                adapter: VersionedRef {
                    id: Id::new("wickle-model-gemini")?,
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
            operation: Id::new("stream_generate_content").expect("static identifier"),
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

pub(crate) fn model_name(value: &str) -> Result<&str, ContractError> {
    let value = value.strip_prefix("models/").unwrap_or(value);
    if value.is_empty()
        || matches!(value, "." | "..")
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(error(ErrorCode::ModelBindingInvalid, "model_name"));
    }
    Ok(value)
}
```

## `crates/wickle-model-gemini/src/inspection.rs`

```rust
use crate::{GeminiConnection, error};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use wickle::*;

/// A provider-documented immutable model release, registered by the Host.
/// API availability alone, a date in a name, or a requested version is not proof.
#[derive(Debug, Clone)]
pub struct GeminiSnapshot {
    /// Exact provider model identifier in the cited snapshot metadata.
    pub model_id: Id,
    /// Release identity established by that metadata.
    pub model_version: Id,
    /// Expected version field from Models API metadata, distinct from inference modelVersion.
    pub metadata_version: Id,
    /// Host-owned reference to the documentation or metadata establishing immutability.
    pub evidence_ref: Id,
}

/// Current account availability combined with explicit immutable-release evidence.
/// Unregistered model identifiers retain unknown release and unverified semantics.
#[derive(Clone)]
pub struct GeminiInspector {
    connection: GeminiConnection,
    snapshots: Arc<BTreeMap<Id, GeminiSnapshot>>,
}
impl GeminiInspector {
    /// Build an inspector without a network call. Duplicate identifiers are rejected.
    pub fn new(
        connection: GeminiConnection,
        snapshots: Vec<GeminiSnapshot>,
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
impl ModelRouteInspector for GeminiInspector {
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
                .join(&format!(
                    "{}/models/",
                    self.connection.0.options.api_version
                ))
                .map_err(|_| error(ErrorCode::InvalidConfiguration, "models_url"))?;
            url.path_segments_mut()
                .map_err(|_| error(ErrorCode::InvalidConfiguration, "models_url"))?
                .pop_if_empty()
                .push(crate::connection::model_name(route.model_id.as_str())?);
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
                        evidence_ref: Id::new("gemini.models.retrieve")?,
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
                let model_id = body
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| error(ErrorCode::ModelInspectionUnavailable, "models_name"))?;
                if crate::connection::model_name(model_id)?
                    != crate::connection::model_name(route.model_id.as_str())?
                {
                    return Err(error(ErrorCode::ModelVersionDrift, "models_name"));
                }
                let methods = body
                    .get("supportedGenerationMethods")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        error(ErrorCode::ModelInspectionUnavailable, "models_methods")
                    })?;
                let available = methods.iter().any(|v| v == "generateContent");
                let snapshot = self.snapshots.get(&route.model_id);
                if let Some(snapshot) = snapshot {
                    if body.get("version").and_then(Value::as_str)
                        != Some(snapshot.metadata_version.as_str())
                    {
                        return Err(error(ErrorCode::ModelVersionDrift, "metadata_version"));
                    }
                }
                Ok(ModelRouteObservation {
                    route_digest: route.digest(),
                    availability: if available {
                        ModelRouteAvailability::Available
                    } else {
                        ModelRouteAvailability::Unavailable
                    },
                    model_id: Some(route.model_id.clone()),
                    model_version: snapshot.map(|value| value.model_version.clone()),
                    deployment_revision: None,
                    version_semantics: if snapshot.is_some() {
                        VersionSemantics::Pinned
                    } else {
                        VersionSemantics::Unverified
                    },
                    evidence_ref: snapshot.map_or_else(
                        || Id::new("gemini.models.retrieve"),
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

## `crates/wickle-model-gemini/src/lib.rs`

```rust
//! Gemini generateContent with explicit API versions, credentials and bounded streams.
#![forbid(unsafe_code)]
mod codec;
mod connection;
mod inspection;
mod model;
mod response;
pub use connection::{GeminiConnection, GeminiOptions};
pub use inspection::{GeminiInspector, GeminiSnapshot};
pub use model::GeminiModel;
/// generateContent wire primitives for independently authenticated platform adapters.
pub mod protocol {
    pub use crate::codec::{FunctionSchemaFormat, encode_request};
    pub use crate::response::Decoder as GenerateContentDecoder;
}
use wickle::{ContractError, ErrorCode};
fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("gemini.{location}"))
}
```

## `crates/wickle-model-gemini/src/model.rs`

```rust
use crate::{GeminiConnection, error};
use crate::{codec::encode_request, response::Decoder as GenerateContentDecoder};
use futures_util::stream;
use reqwest::Response;
use std::collections::VecDeque;
use wickle::*;
use wickle_model_responses::SseDecoder;

/// One Gemini Messages POST per invocation, streamed into Wickle model events.
/// The adapter does not retry, run Tool handlers, or load environment variables.
#[derive(Debug, Clone)]
pub struct GeminiModel {
    connection: GeminiConnection,
}
impl GeminiModel {
    /// Bind an already configured connection without making a network request.
    pub fn new(connection: GeminiConnection) -> Self {
        Self { connection }
    }
}
impl ModelPort for GeminiModel {
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
            decoder: GenerateContentDecoder::new(request),
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
    connection: &'a GeminiConnection,
    request: &'a ModelRequest,
    context: &'a ModelCallContext,
    response: Option<Response>,
    decoder: GenerateContentDecoder<'a>,
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
        let format = if self.connection.0.options.api_version == "v1" {
            crate::codec::FunctionSchemaFormat::OpenApi
        } else {
            crate::codec::FunctionSchemaFormat::JsonSchema
        };
        let value = encode_request(self.request, format)?;
        let body =
            serde_json::to_vec(&value).map_err(|_| error(ErrorCode::InvalidJson, "request"))?;
        if body.len() > self.request.limits.max_input_bytes {
            return Err(error(ErrorCode::ModelCapabilityUnsupported, "request_size"));
        }
        let name = crate::connection::model_name(self.request.route.model_id.as_str())?;
        let mut url = self
            .connection
            .0
            .base
            .join(&format!(
                "{}/models/{}:streamGenerateContent",
                self.connection.0.options.api_version, name
            ))
            .map_err(|_| error(ErrorCode::InvalidConfiguration, "generate_url"))?;
        url.query_pairs_mut().append_pair("alt", "sse");
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

## `crates/wickle-model-gemini/src/response.rs`

```rust
use crate::{
    codec::{KIND, inspect_part, invalid},
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
    seen_ids: BTreeSet<String>,
    finish: Option<ModelFinish>,
    bytes: usize,
    started: bool,
}
impl<'a> Decoder<'a> {
    /// Start one physical attempt without network access.
    pub fn new(request: &'a ModelRequest) -> Self {
        Self {
            request,
            metadata: Default::default(),
            parts: vec![],
            ids: vec![],
            seen_ids: BTreeSet::new(),
            finish: None,
            bytes: 0,
            started: false,
        }
    }
    /// Consume a complete SSE JSON record. It is never a Tool execution permit.
    pub fn event(&mut self, event: SseEvent) -> Result<Vec<ModelEvent>, ContractError> {
        if event.name.as_deref().is_some_and(|s| s != "message") {
            return Err(invalid());
        }
        let value = parse_json(&event.data)?;
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
            for part in parts {
                self.bytes = self
                    .bytes
                    .checked_add(part.to_string().len())
                    .filter(|n| *n <= self.request.limits.max_response_bytes)
                    .ok_or_else(invalid)?;
                let decoded = inspect_part(part)?;
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
                    let args = call.args.to_string();
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
                    json!({"kind":KIND,"parts":self.parts,"call_ids":self.ids}),
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
```

## `crates/wickle-model-gemini/tests/generate.rs`

```rust
//! Gemini REST/SSE contracts, signed replay and API-version-specific schemas.
mod support;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::time::Duration;
use support::*;
use wickle::*;
use wickle_model_gemini::*;

fn tool(request: &mut ModelRequest) {
    request.tools = vec![ModelTool {
        name: id("lookup"),
        description: "Look up an authorized record".into(),
        model_input_schema: json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}),
    }];
}
fn tool_events() -> Vec<Value> {
    vec![
        json!({"responseId":"tools","modelVersion":"fixture-release","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"name":"lookup","args":{"query":"alpha"}},"thoughtSignature":"signed-call"},{"functionCall":{"name":"lookup","args":{"query":"beta"},"id":"provider-call"}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":20,"candidatesTokenCount":5,"thoughtsTokenCount":7,"totalTokenCount":32}}),
    ]
}

#[tokio::test]
async fn explicit_versions_and_resource_prefix_preserve_wire_and_usage() {
    for version in ["v1", "v1beta"] {
        let server = Server::new(vec![reply(&events("결과 42"))]).await;
        let connection = GeminiConnection::new(
            scope(),
            reference("account"),
            "fixture-key",
            GeminiOptions {
                base_url: server.base.trim_end_matches("v1/").into(),
                api_version: version.into(),
                ..Default::default()
            },
        )
        .unwrap();
        let mut request = request(&connection, &format!("models/{MODEL}"));
        request.messages.insert(
            0,
            ModelMessage {
                role: ModelRole::System,
                content: vec![ModelContent::Text {
                    text: "Use authorized records".into(),
                }],
            },
        );
        let model = GeminiModel::new(connection);
        let response =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap();
        assert_eq!(response.text, "결과 42");
        assert_eq!(
            response.metadata.provider_request_id,
            Some(id("response-fixture"))
        );
        assert_eq!(
            response.metadata.reported_model_version,
            Some(id("fixture-release"))
        );
        assert!(response.metadata.reported_model_id.is_none());
        assert_eq!(response.metadata.usage.unwrap().output_tokens, Some(12));
        assert_eq!(
            response.continuation[0].data()["parts"][0]["thoughtSignature"],
            "signed-thought"
        );
        let calls = server.requests.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].method, "POST");
        assert_eq!(
            calls[0].path,
            format!("/{version}/models/{MODEL}:streamGenerateContent?alt=sse")
        );
        assert!(
            calls[0]
                .headers
                .to_ascii_lowercase()
                .contains("x-goog-api-key: fixture-key")
        );
        assert!(!calls[0].path.contains("fixture-key"));
        let body = &calls[0].body;
        assert_eq!(
            body["generationConfig"]["thinkingConfig"]["thinkingLevel"],
            "MEDIUM"
        );
        assert_eq!(
            body["systemInstruction"]["parts"][0]["text"],
            "Use authorized records"
        );
        assert_eq!(body["generationConfig"]["candidateCount"], 1);
        assert!(!body.to_string().contains("hidden-workspace"));
    }
}
#[tokio::test]
async fn parallel_calls_replay_signatures_without_inventing_wire_ids() {
    let server = Server::new(vec![reply(&tool_events()), reply(&events("42"))]).await;
    let connection = connection(&server);
    let mut request = request(&connection, MODEL);
    tool(&mut request);
    let model = GeminiModel::new(connection);
    let first = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(first.finish, ModelFinish::ToolCalls);
    assert_eq!(first.tool_calls.len(), 2);
    assert_ne!(
        first.tool_calls[0].provider_call_id,
        first.tool_calls[1].provider_call_id
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
        content: first
            .tool_calls
            .iter()
            .map(|call| ModelContent::ToolResult {
                provider_call_id: call.provider_call_id.clone(),
                content: json!({"answer":42}),
            })
            .collect(),
    });
    request.request_id = id("next-attempt");
    request.output = ModelOutput::JsonSchema {
        schema: json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}),
    };
    let response = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(response.text, "42");
    {
        let calls = server.requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        let body = &calls[1].body;
        assert_eq!(
            body["contents"][1]["parts"],
            tool_events()[0]["candidates"][0]["content"]["parts"]
        );
        assert!(
            body["contents"][2]["parts"][0]["functionResponse"]
                .get("id")
                .is_none()
        );
        assert_eq!(
            body["contents"][2]["parts"][1]["functionResponse"]["id"],
            "provider-call"
        );
        assert_eq!(
            body["contents"][2]["parts"][0]["functionResponse"]["name"],
            "lookup"
        );
        assert_eq!(
            body["tools"][0]["functionDeclarations"][0]["parametersJsonSchema"]["additionalProperties"],
            false
        );
        assert_eq!(
            body["generationConfig"]["responseMimeType"],
            "application/json"
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
#[tokio::test]
async fn stable_schema_cannot_silently_drop_closed_object_constraints() {
    let server = Server::new(vec![]).await;
    let connection = GeminiConnection::new(
        scope(),
        reference("account"),
        "fixture",
        GeminiOptions {
            base_url: server.base.trim_end_matches("v1/").into(),
            ..Default::default()
        },
    )
    .unwrap();
    let mut request = request(&connection, MODEL);
    tool(&mut request);
    let model = GeminiModel::new(connection);
    let failure = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap_err();
    assert_eq!(failure.kind, ModelFailureKind::Unsupported);
    assert!(server.requests.lock().unwrap().is_empty());
    request.tools[0]
        .model_input_schema
        .as_object_mut()
        .unwrap()
        .remove("additionalProperties");
    request.tools[0].model_input_schema["properties"]["query"]["enum"] = json!(["alpha", "beta"]);
    let encoded =
        protocol::encode_request(&request, protocol::FunctionSchemaFormat::OpenApi).unwrap();
    assert_eq!(
        encoded["tools"][0]["functionDeclarations"][0]["parameters"]["type"],
        "OBJECT"
    );
    assert_eq!(
        encoded["tools"][0]["functionDeclarations"][0]["parameters"]["properties"]["query"]["type"],
        "STRING"
    );
    request.tools[0].model_input_schema =
        json!({"type":"object","properties":{"choice":{"type":"integer","enum":[1,2]}}});
    let failure = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap_err();
    assert_eq!(failure.kind, ModelFailureKind::Unsupported);
    assert!(server.requests.lock().unwrap().is_empty());
}
#[tokio::test]
async fn unsupported_options_scope_and_route_fail_before_network() {
    for case in [
        "minimal", "combined", "option", "scope", "api", "model", "schema",
    ] {
        let server = Server::new(vec![]).await;
        let connection = connection(&server);
        let mut request = request(&connection, MODEL);
        let model = GeminiModel::new(connection);
        let mut context = context(&request);
        match case {
            "minimal" => {
                request
                    .options
                    .insert("thinking_level".into(), json!("minimal"));
            }
            "combined" => {
                request
                    .options
                    .insert("thinking_budget_tokens".into(), json!(100));
            }
            "option" => {
                request.options.insert("native_search".into(), json!(true));
            }
            "scope" => context.scope.workspace_id = id("other"),
            "api" => request.route.api_contract.version = id("v2"),
            "model" => request.route.model_id = id("models/models/invalid"),
            _ => {
                request.output = ModelOutput::JsonSchema {
                    schema: json!({"type":"object","properties":{},"additionalProperties":false,"not":{}}),
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
async fn malformed_incomplete_or_ambiguous_streams_never_complete() {
    for case in [
        "eof",
        "sse",
        "duplicate",
        "candidates",
        "signature",
        "native",
        "version",
        "usage",
        "limit",
    ] {
        let mut data = events("answer");
        match case {
            "eof" => {
                data[1]["candidates"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("finishReason");
            }
            "duplicate" => {
                data = tool_events();
                data[0]["candidates"][0]["content"]["parts"][0]["functionCall"]["id"] =
                    json!("provider-call");
            }
            "candidates" => {
                data[0]["candidates"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"index":1}));
            }
            "signature" => {
                data[0]["candidates"][0]["content"]["parts"][0]["thoughtSignature"] = json!(42)
            }
            "native" => {
                data[0]["candidates"][0]["content"]["parts"] =
                    json!([{"executableCode":{"code":"private"}}])
            }
            "version" => data[1]["modelVersion"] = json!("different"),
            "usage" => data[1]["usageMetadata"]["totalTokenCount"] = json!(3),
            _ => {}
        }
        let mut response = reply(&data);
        if case == "sse" {
            response.body.pop();
            response.body.pop();
        }
        let server = Server::new(vec![response]).await;
        let connection = connection(&server);
        let mut request = request(&connection, MODEL);
        tool(&mut request);
        let model = GeminiModel::new(connection);
        if case == "limit" {
            request.limits.max_response_bytes = 8;
        }
        assert!(
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .is_err(),
            "{case}"
        );
    }
}
#[tokio::test]
async fn refusal_and_length_are_not_successful_continuations() {
    for reason in ["SAFETY", "MAX_TOKENS", "prompt"] {
        let mut data = events("partial");
        data[1]["candidates"][0]["finishReason"] = json!(reason);
        if reason == "prompt" {
            data = vec![json!({"promptFeedback":{"blockReason":"SAFETY"}})];
        }
        let server = Server::new(vec![reply(&data)]).await;
        let connection = connection(&server);
        let request = request(&connection, MODEL);
        let model = GeminiModel::new(connection);
        let events: Vec<_> = model.generate(&request, &context(&request)).collect().await;
        let ModelEvent::ResponseCompleted {
            finish,
            continuation,
            ..
        } = events.last().unwrap().as_ref().unwrap()
        else {
            panic!("missing terminal")
        };
        assert_eq!(
            *finish,
            if reason == "MAX_TOKENS" {
                ModelFinish::Length
            } else {
                ModelFinish::Refusal
            }
        );
        assert!(continuation.is_empty());
    }
}
#[tokio::test]
async fn cancellation_deadline_and_redirect_do_not_leave_requests_running() {
    for cancel in [true, false] {
        let mut data = events("partial");
        data[1]["candidates"][0]
            .as_object_mut()
            .unwrap()
            .remove("finishReason");
        let mut response = reply(&data);
        response.stall = true;
        let server = Server::new(vec![response]).await;
        let connection = connection(&server);
        let request = request(&connection, MODEL);
        let model = GeminiModel::new(connection);
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
    let destination = Server::new(vec![]).await;
    let mut response = Reply::json(307, json!({}));
    response
        .headers
        .push(("location", destination.base.clone()));
    let server = Server::new(vec![response]).await;
    let connection = connection(&server);
    let request = request(&connection, MODEL);
    let model = GeminiModel::new(connection);
    assert!(
        collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .is_err()
    );
    assert!(destination.requests.lock().unwrap().is_empty());
}
fn metadata() -> Value {
    json!({"name":format!("models/{MODEL}"),"version":"001","supportedGenerationMethods":["generateContent"]})
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
fn snapshot() -> GeminiSnapshot {
    GeminiSnapshot {
        model_id: id(MODEL),
        model_version: id("fixture-release"),
        metadata_version: id("001"),
        evidence_ref: id("documented-snapshot"),
    }
}
#[tokio::test]
async fn metadata_observations_require_actual_release_evidence_and_detect_drift() {
    let mut changed = metadata();
    changed["version"] = json!("002");
    let server = Server::new(vec![
        Reply::json(200, metadata()),
        Reply::json(200, metadata()),
        Reply::json(200, changed),
        Reply::json(404, json!({})),
    ])
    .await;
    let connection = connection(&server);
    let request = request(&connection, MODEL);
    let unknown = GeminiInspector::new(connection.clone(), vec![])
        .unwrap()
        .inspect(&request.route, &inspect_context())
        .await
        .unwrap();
    assert_eq!(unknown.version_semantics, VersionSemantics::Unverified);
    assert!(unknown.model_version.is_none());
    let inspector = GeminiInspector::new(connection, vec![snapshot()]).unwrap();
    let known = inspector
        .inspect(&request.route, &inspect_context())
        .await
        .unwrap();
    known
        .validate(&request.route, VersionPolicy::RequirePinned)
        .unwrap();
    assert_eq!(
        inspector
            .inspect(&request.route, &inspect_context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ModelVersionDrift
    );
    assert_eq!(
        inspector
            .inspect(&request.route, &inspect_context())
            .await
            .unwrap()
            .availability,
        ModelRouteAvailability::Unavailable
    );
    assert_eq!(
        server.requests.lock().unwrap()[0].path,
        format!("/v1beta/models/{MODEL}")
    );
}

#[tokio::test]
async fn http_failures_are_classified_without_retry_or_private_error_text() {
    for (status, kind) in [
        (401, ModelFailureKind::Authentication),
        (404, ModelFailureKind::Unavailable),
        (429, ModelFailureKind::RateLimited),
        (503, ModelFailureKind::Transport),
    ] {
        let server = Server::new(vec![Reply::json(
            status,
            json!({"error":{"message":"private provider details"}}),
        )])
        .await;
        let connection = connection(&server);
        let request = request(&connection, MODEL);
        let model = GeminiModel::new(connection);
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, kind);
        assert!(!format!("{failure:?}").contains("private provider details"));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}
#[tokio::test]
async fn omitted_usage_is_unknown_and_late_usage_is_cumulative() {
    for case in ["none", "late", "partial"] {
        let late = case == "late";
        let mut data = events("answer");
        data[1].as_object_mut().unwrap().remove("usageMetadata");
        data[0].as_object_mut().unwrap().remove("modelVersion");
        if late {
            data.push(json!({"usageMetadata":{"promptTokenCount":20,"candidatesTokenCount":5,"thoughtsTokenCount":7,"totalTokenCount":32}}));
        }
        if case == "partial" {
            data.push(json!({"usageMetadata":{"candidatesTokenCount":5}}));
        }
        let server = Server::new(vec![reply(&data)]).await;
        let connection = connection(&server);
        let request = request(&connection, MODEL);
        let model = GeminiModel::new(connection);
        let response =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap();
        assert!(response.metadata.reported_model_version.is_none());
        if late {
            assert_eq!(response.metadata.usage.unwrap().output_tokens, Some(12));
        } else if case == "partial" {
            assert!(response.metadata.usage.unwrap().output_tokens.is_none());
        } else {
            assert!(response.metadata.usage.is_none());
        }
    }
}

#[tokio::test]
async fn idless_parallel_results_are_matched_in_original_call_order() {
    let mut data = tool_events();
    data[0]["candidates"][0]["content"]["parts"][1]["functionCall"]
        .as_object_mut()
        .unwrap()
        .remove("id");
    let server = Server::new(vec![reply(&data), reply(&events("done"))]).await;
    let connection = connection(&server);
    let mut request = request(&connection, MODEL);
    tool(&mut request);
    let model = GeminiModel::new(connection);
    let first = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let mut content: Vec<_> = first
        .tool_calls
        .iter()
        .map(|c| ModelContent::ToolCall {
            provider_call_id: c.provider_call_id.clone(),
            name: c.name.clone(),
            arguments: c.model_inputs.clone(),
        })
        .collect();
    content.push(ModelContent::Opaque {
        continuation: first.continuation[0].clone(),
    });
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content,
    });
    for (index, answer) in [(1, "beta result"), (0, "alpha result")] {
        request.messages.push(ModelMessage {
            role: ModelRole::Tool,
            content: vec![ModelContent::ToolResult {
                provider_call_id: first.tool_calls[index].provider_call_id.clone(),
                content: json!({"answer":answer}),
            }],
        });
    }
    request.request_id = id("next-attempt");
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let calls = server.requests.lock().unwrap();
    let parts = calls[1].body["contents"][2]["parts"].as_array().unwrap();
    assert_eq!(
        parts[0]["functionResponse"]["response"]["answer"],
        "alpha result"
    );
    assert_eq!(
        parts[1]["functionResponse"]["response"]["answer"],
        "beta result"
    );
    assert!(
        parts
            .iter()
            .all(|p| p["functionResponse"].get("id").is_none())
    );
}
```

## `crates/wickle-model-gemini/tests/support/mod.rs`

```rust
use serde_json::{Value, json};
use wickle::*;
use wickle_model_gemini::*;

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
pub fn connection(server: &Server) -> GeminiConnection {
    GeminiConnection::new(
        scope(),
        reference("account"),
        "fixture-key-not-a-secret",
        GeminiOptions {
            base_url: server.base.trim_end_matches("v1/").into(),
            api_version: "v1beta".into(),
            ..Default::default()
        },
    )
    .unwrap()
}
pub fn request(connection: &GeminiConnection, model: &str) -> ModelRequest {
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
            model_version: id("fixture-release"),
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
        options: JsonObject::from([("thinking_level".into(), json!("medium"))]),
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
pub const MODEL: &str = "gemini-3.8-flash";
pub fn events(text: &str) -> Vec<Value> {
    vec![
        json!({"responseId":"response-fixture","modelVersion":"fixture-release","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"private thought","thought":true,"thoughtSignature":"signed-thought"}]}}]}),
        json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":text,"thoughtSignature":"signed-answer"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":20,"candidatesTokenCount":5,"thoughtsTokenCount":7,"totalTokenCount":32}}),
    ]
}
pub fn reply(events: &[Value]) -> Reply {
    let mut reply = Reply::sse(&[]);
    reply.body = events
        .iter()
        .flat_map(|value| format!("data: {value}\n\n").into_bytes())
        .collect();
    reply.chunk = 3;
    reply
}

#[path = "../../../../tests/support/model_http.rs"]
mod http;
pub use http::*;
```

## `tests/support/gemini_consumer.rs`

```rust
// Real loopback HTTP/SSE against the extracted Gemini adapter package; no provider call.
use serde_json::json;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use wickle::*;
use wickle_model_gemini::*;
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
        assert!(headers.starts_with("post /v1beta/models/fixture-model:streamgeneratecontent?alt=sse "));
        assert!(headers.contains("x-goog-api-key: fixture-key"));
        let body =
            parse_json(std::str::from_utf8(&bytes[header_end..header_end + length]).unwrap())
                .unwrap();
        assert!(body.get("model").is_none());
        assert_eq!(body["generationConfig"]["candidateCount"], 1);
        assert!(body.get("fallbacks").is_none());
        assert_eq!(body["generationConfig"]["thinkingConfig"]["thinkingLevel"], "MEDIUM");
        assert_eq!(body["generationConfig"]["responseMimeType"], "application/json");
        assert!(!body.to_string().contains("private-workspace"));
        assert!(!body.to_string().contains("fixture-key"));
        let events = vec![
            json!({"responseId":"fixture-response","modelVersion":"fixture-release","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"","thought":true,"thoughtSignature":"signed-fixture"}]}}]}),
            json!({"candidates":[{"content":{"role":"model","parts":[{"text":"{\"answer\":42}"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":12,"candidatesTokenCount":5,"thoughtsTokenCount":3,"totalTokenCount":20}}),
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
    let connection = GeminiConnection::new(
        scope.clone(),
        reference("account"),
        "fixture-key",
        GeminiOptions {
            base_url: base,
            api_version: "v1beta".into(),
            ..Default::default()
        },
    )?;
    let binding = connection.binding();
    let model = GeminiModel::new(connection.clone());
    let request = ModelRequest {
        request_id: id("attempt"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference("model"),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id("fixture-model"),
            model_id: id("fixture-model"),
            model_version: id("fixture-release"),
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
        options: JsonObject::from([("thinking_level".into(), json!("medium"))]),
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
    assert!(response.metadata.reported_model_id.is_none());
    assert_eq!(response.metadata.reported_model_version, Some(id("fixture-release")));
    assert_eq!(
        response
            .metadata
            .usage
            .as_ref()
            .and_then(|u| u.output_tokens),
        Some(8)
    );
    assert_eq!(
        response.continuation[0].data()["parts"][0]["thoughtSignature"],
        "signed-fixture"
    );
    server.await?;
    println!(
        "Gemini consumer: extracted adapter performs one HTTP/SSE request, uses explicit API-key authentication and beta JSON schema, preserves signed thinking, decodes JSON and includes reasoning in reported output tokens, and excludes Host context from the wire (local fixture, no provider network)"
    );
    Ok(())
}
```
