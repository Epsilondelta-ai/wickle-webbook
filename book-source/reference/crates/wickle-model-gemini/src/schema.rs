//! Pure projection; original constraints remain in the core contract and context.
use crate::codec::FunctionSchemaFormat;
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;
use wickle::*;

/// Compile model-owned parameters for the selected GenerateContent schema dialect.
#[derive(Debug, Clone, Copy)]
pub struct GeminiToolSchemaCompiler {
    format: FunctionSchemaFormat,
}
impl GeminiToolSchemaCompiler {
    /// Select an explicit endpoint dialect without changing the requested API version.
    pub fn new(format: FunctionSchemaFormat) -> Self {
        Self { format }
    }
}
impl ProviderToolSchemaCompiler for GeminiToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new(match self.format {
                FunctionSchemaFormat::OpenApi => "wickle-gemini-openapi-schema",
                FunctionSchemaFormat::JsonSchema => "wickle-gemini-json-schema",
            })
            .expect("constant"),
            version: Id::new("1").expect("constant"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        if target.api_contract.operation.as_str() != "stream_generate_content"
            || !matches!(
                (
                    target.provider.as_str(),
                    target.api_contract.version.as_str(),
                    self.format
                ),
                ("google-gemini", "v1", FunctionSchemaFormat::OpenApi)
                    | ("google-gemini", "v1beta", FunctionSchemaFormat::JsonSchema)
                    | ("google-vertex", "v1", FunctionSchemaFormat::JsonSchema)
            )
        {
            return Err(invalid());
        }
        let root = &tool.model_input_schema;
        let properties = root
            .get("properties")
            .and_then(Value::as_object)
            .ok_or_else(invalid)?;
        let mut projected = Map::new();
        let mut fields = vec![];
        let mut budget = Expansion {
            nodes: 1024,
            bytes: 32 * 1024,
        };
        for (name, schema) in properties {
            let lowered =
                if budget.charge(serde_json::to_vec(name).map_err(|_| invalid())?.len() + 8) {
                    lower(
                        schema,
                        root,
                        self.format,
                        0,
                        &mut BTreeSet::new(),
                        &mut budget,
                    )
                } else {
                    None
                };
            let (schema, encoding) = match lowered {
                Some(schema) => (schema, ArgumentValueEncoding::Identity {}),
                None => (
                    json!({"type":"string"}),
                    ArgumentValueEncoding::JsonText { optional: false },
                ),
            };
            projected.insert(name.clone(), schema);
            fields.push(ArgumentFieldMapping {
                wire_name: name.clone(),
                canonical_name: name.clone(),
                encoding,
            });
        }
        let mut wire_tool = tool.clone();
        wire_tool.model_input_schema = json!({"type":"object","properties":projected,"required":root.get("required").cloned().unwrap_or_else(||json!([])),"additionalProperties":false});
        for key in ["minProperties", "maxProperties", "title", "description"] {
            if let Some(value) = root.get(key) {
                if matches!(key, "minProperties" | "maxProperties")
                    && value.as_u64().is_none_or(|value| value > i64::MAX as u64)
                {
                    continue;
                }
                wire_tool.model_input_schema[key] = value.clone();
            }
        }
        if fields
            .iter()
            .all(|field| matches!(field.encoding, ArgumentValueEncoding::Identity {}))
        {
            if let Some(value) = root.get("default") {
                wire_tool.model_input_schema["default"] = value.clone();
            }
        }
        // Fields deliberately retain canonical guidance even when value names and
        // shapes need no conversion. OpenAPI cannot represent closed objects, and
        // accepting JSON Schema does not attest every dialect keyword's enforcement.
        Ok(ProviderToolProjection {
            wire_tool,
            decode_plan: ArgumentDecodePlan::Fields { fields },
        })
    }
}
fn lower(
    schema: &Value,
    root: &Value,
    format: FunctionSchemaFormat,
    depth: usize,
    visiting: &mut BTreeSet<String>,
    budget: &mut Expansion,
) -> Option<Value> {
    if depth > 16 || budget.nodes == 0 || budget.bytes == 0 {
        return None;
    }
    if !budget.charge(serde_json::to_vec(schema).ok()?.len()) {
        return None;
    }
    let node = schema.as_object()?;
    if node.contains_key("$id")
        || node.contains_key("$anchor")
        || node.contains_key("$dynamicRef")
        || node.contains_key("$dynamicAnchor")
    {
        return None;
    }
    if let Some(reference) = node.get("$ref") {
        let reference = reference.as_str()?;
        if !reference.starts_with('#') || !visiting.insert(reference.into()) {
            return None;
        }
        let mut combined = root.pointer(&reference[1..])?.as_object()?.clone();
        for (key, value) in node.iter().filter(|(key, _)| key.as_str() != "$ref") {
            match combined.get(key) {
                None => {
                    combined.insert(key.clone(), value.clone());
                }
                Some(old) if old == value => {}
                Some(_)
                    if matches!(
                        key.as_str(),
                        "title" | "description" | "default" | "$comment"
                    ) =>
                {
                    combined.insert(key.clone(), value.clone());
                }
                Some(old)
                    if matches!(
                        key.as_str(),
                        "minimum"
                            | "minLength"
                            | "minItems"
                            | "minProperties"
                            | "maximum"
                            | "maxLength"
                            | "maxItems"
                            | "maxProperties"
                    ) && native_double(old)
                        && native_double(value) =>
                {
                    let lower_bound = key.starts_with("min");
                    if (lower_bound && value.as_f64()? > old.as_f64()?)
                        || (!lower_bound && value.as_f64()? < old.as_f64()?)
                    {
                        combined.insert(key.clone(), value.clone());
                    }
                }
                // Conflicting patterns/types/structures need an intersection that
                // cannot be represented by overwriting either original rule.
                _ => {
                    visiting.remove(reference);
                    return None;
                }
            }
        }
        let result = lower(
            &Value::Object(combined),
            root,
            format,
            depth + 1,
            visiting,
            budget,
        );
        visiting.remove(reference);
        return result;
    }
    if let Some(types) = node.get("type").and_then(Value::as_array) {
        if node.contains_key("anyOf") || node.contains_key("oneOf") {
            return None;
        }
        let mut branches = vec![];
        for kind in types {
            let kind = kind.as_str()?;
            let mut branch = node.clone();
            branch.insert("type".into(), json!(kind));
            if branch
                .get("default")
                .is_some_and(|value| !matches_type(value, kind))
            {
                branch.remove("default");
            }
            if let Some(values) = node.get("enum").and_then(Value::as_array) {
                let values: Vec<_> = values
                    .iter()
                    .filter(|value| matches_type(value, kind))
                    .cloned()
                    .collect();
                if values.is_empty() {
                    continue;
                }
                branch.insert("enum".into(), json!(values));
            }
            branches.push(lower(
                &Value::Object(branch),
                root,
                format,
                depth + 1,
                visiting,
                budget,
            )?);
        }
        return match branches.len() {
            0 => None,
            1 => branches.pop(),
            _ => Some(json!({"anyOf":branches})),
        };
    }
    let mut out = Map::new();
    if let Some(kind) = node.get("type") {
        let types: Vec<_> = if let Some(kind) = kind.as_str() {
            vec![kind]
        } else {
            kind.as_array()?
                .iter()
                .map(Value::as_str)
                .collect::<Option<_>>()?
        };
        if types.is_empty()
            || types.iter().any(|kind| {
                !matches!(
                    *kind,
                    "string" | "number" | "integer" | "boolean" | "object" | "array" | "null"
                )
            })
        {
            return None;
        }
        out.insert("type".into(), kind.clone());
        if types.contains(&"object") {
            // OpenAPI has no arbitrary-map schema. Keep those values as JSON text.
            if format == FunctionSchemaFormat::OpenApi
                && (node.get("additionalProperties") != Some(&Value::Bool(false))
                    || node.contains_key("patternProperties"))
            {
                return None;
            }
            let mut properties = Map::new();
            if let Some(children) = node.get("properties") {
                for (name, child) in children.as_object()? {
                    properties.insert(
                        name.clone(),
                        lower(child, root, format, depth + 1, visiting, budget)?,
                    );
                }
            }
            out.insert("properties".into(), Value::Object(properties));
            if let Some(required) = node.get("required") {
                out.insert("required".into(), required.clone());
            }
            if let Some(additional) = node.get("additionalProperties") {
                out.insert(
                    "additionalProperties".into(),
                    if additional.is_boolean() {
                        additional.clone()
                    } else {
                        lower(additional, root, format, depth + 1, visiting, budget)?
                    },
                );
            }
        }
        if types.contains(&"array") {
            // A tuple cannot safely be described by a single homogeneous item type.
            if node.contains_key("prefixItems") {
                return None;
            }
            out.insert(
                "items".into(),
                lower(
                    node.get("items")?,
                    root,
                    format,
                    depth + 1,
                    visiting,
                    budget,
                )?,
            );
        }
    } else if !node.contains_key("anyOf") && !node.contains_key("oneOf") {
        return None;
    }
    if let Some(branches) = node.get("anyOf").or_else(|| node.get("oneOf")) {
        let branches = branches
            .as_array()?
            .iter()
            .map(|branch| lower(branch, root, format, depth + 1, visiting, budget))
            .collect::<Option<Vec<_>>>()?;
        out.insert("anyOf".into(), Value::Array(branches));
    }
    if let Some(values) = node
        .get("enum")
        .cloned()
        .or_else(|| node.get("const").map(|value| json!([value])))
    {
        let string_enum = node.get("type") == Some(&json!("string"))
            && values
                .as_array()
                .is_some_and(|values| values.iter().all(Value::is_string));
        if format == FunctionSchemaFormat::JsonSchema || string_enum {
            out.insert("enum".into(), values);
        }
    }
    let constraints: &[&str] = match out.get("type").and_then(Value::as_str) {
        Some("string") => &["minLength", "maxLength", "pattern"],
        Some("integer" | "number") => &["minimum", "maximum"],
        Some("array") => &["minItems", "maxItems"],
        Some("object") => &["minProperties", "maxProperties"],
        _ => &[],
    };
    for key in constraints
        .iter()
        .copied()
        .chain(["description", "title", "default"])
    {
        if let Some(value) = node.get(key) {
            if matches!(key, "minimum" | "maximum") && !native_double(value) {
                continue;
            }
            if matches!(
                key,
                "minLength"
                    | "maxLength"
                    | "minItems"
                    | "maxItems"
                    | "minProperties"
                    | "maxProperties"
            ) && value.as_u64().is_none_or(|value| value > i64::MAX as u64)
            {
                continue;
            }
            out.insert(key.into(), value.clone());
        }
    }
    // Provider format/default annotations are not enforcement guarantees.
    let formats: &[&str] = match out.get("type").and_then(Value::as_str) {
        Some("string") => &["enum", "date-time"],
        Some("integer") => &["int32", "int64"],
        Some("number") => &["float", "double"],
        _ => &[],
    };
    if let Some(value) = node
        .get("format")
        .filter(|value| value.as_str().is_some_and(|value| formats.contains(&value)))
    {
        out.insert("format".into(), value.clone());
    }
    Some(Value::Object(out))
}
fn invalid() -> ContractError {
    ContractError::new(ErrorCode::UnsupportedInputProjection, "gemini.schema")
}

fn matches_type(value: &Value, kind: &str) -> bool {
    match kind {
        "string" => value.is_string(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        "number" => value.is_number(),
        "integer" => {
            value.as_i64().is_some()
                || value.as_u64().is_some()
                || value.as_f64().is_some_and(|number| number.fract() == 0.0)
        }
        _ => false,
    }
}

fn native_double(value: &Value) -> bool {
    let integer = value
        .as_u64()
        .or_else(|| value.as_i64().map(i64::unsigned_abs));
    if let Some(value) = integer {
        let bits = 64 - value.leading_zeros();
        bits <= 53 || value.trailing_zeros() >= bits - 53
    } else {
        value.as_f64().is_some()
    }
}

struct Expansion {
    nodes: usize,
    bytes: usize,
}
impl Expansion {
    fn charge(&mut self, bytes: usize) -> bool {
        if self.nodes == 0 || bytes > self.bytes {
            self.nodes = 0;
            self.bytes = 0;
            return false;
        }
        self.nodes -= 1;
        self.bytes -= bytes;
        true
    }
}
