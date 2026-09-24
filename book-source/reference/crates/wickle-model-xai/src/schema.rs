//! xAI projection preserves optional fields and delegates semantic gaps to the core.
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;
use wickle::*;

/// Versioned xAI Responses Tool schema projection with reversible field encodings.
#[derive(Debug, Clone, Copy, Default)]
pub struct XaiToolSchemaCompiler;
impl ProviderToolSchemaCompiler for XaiToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new("wickle-xai-tool-schema").expect("constant"),
            version: Id::new("1").expect("constant"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        if target.provider.as_str() != "xai"
            || target.api_contract.operation.as_str() != "responses"
            || target.api_contract.version.as_str() != "v1"
        {
            return Err(crate::error(
                ErrorCode::ModelCapabilityUnsupported,
                "schema_target",
            ));
        }
        let root = &tool.model_input_schema;
        let properties = root
            .get("properties")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                crate::error(ErrorCode::InvalidToolInputContract, "schema_properties")
            })?;
        let mut budget = Budget {
            nodes: 1024,
            bytes: 32 * 1024,
        };
        let mut projected = Map::new();
        let mut fields = vec![];
        for (name, schema) in properties {
            let native = lower(schema, root, 0, &mut BTreeSet::new(), &mut budget);
            let (schema, encoding) = match native {
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
        for key in ["minProperties", "maxProperties", "description", "title"] {
            if let Some(value) = root.get(key) {
                wire_tool.model_input_schema[key] = value.clone();
            }
        }
        // Even accepted keywords may be best-effort. Keep the complete canonical
        // guidance and original validation instead of claiming native equivalence.
        Ok(ProviderToolProjection {
            wire_tool,
            decode_plan: ArgumentDecodePlan::Fields { fields },
        })
    }
}
struct Budget {
    nodes: usize,
    bytes: usize,
}
impl Budget {
    fn charge(&mut self, value: &Value) -> Option<()> {
        let bytes = serde_json::to_vec(value).ok()?.len();
        self.nodes = self.nodes.checked_sub(1)?;
        self.bytes = self.bytes.checked_sub(bytes)?;
        Some(())
    }
}
fn lower(
    schema: &Value,
    root: &Value,
    depth: usize,
    visiting: &mut BTreeSet<String>,
    budget: &mut Budget,
) -> Option<Value> {
    if depth > 16 {
        return None;
    }
    budget.charge(schema)?;
    let node = schema.as_object()?;
    if [
        "$id",
        "$anchor",
        "$dynamicRef",
        "$dynamicAnchor",
        "patternProperties",
    ]
    .iter()
    .any(|key| node.contains_key(*key))
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
            if combined.get(key).is_some_and(|old| old != value)
                && !matches!(
                    key.as_str(),
                    "description" | "title" | "default" | "$comment"
                )
            {
                return None;
            }
            combined.insert(key.clone(), value.clone());
        }
        let result = lower(&Value::Object(combined), root, depth + 1, visiting, budget);
        visiting.remove(reference);
        return result;
    }
    let mut result = Map::new();
    let kinds: Vec<&str> = match node.get("type") {
        Some(Value::String(kind)) => vec![kind.as_str()],
        Some(Value::Array(kinds)) => kinds.iter().map(Value::as_str).collect::<Option<_>>()?,
        None => vec![],
        _ => return None,
    };
    if kinds.iter().any(|kind| {
        !matches!(
            *kind,
            "string" | "number" | "integer" | "boolean" | "null" | "array" | "object"
        )
    }) {
        return None;
    }
    if let Some(kind) = node.get("type") {
        result.insert("type".into(), kind.clone());
    }
    if let Some(branches) = node.get("anyOf").or_else(|| node.get("oneOf")) {
        let branches = branches.as_array()?;
        if branches.is_empty() {
            return None;
        }
        result.insert(
            "anyOf".into(),
            Value::Array(
                branches
                    .iter()
                    .map(|branch| lower(branch, root, depth + 1, visiting, budget))
                    .collect::<Option<_>>()?,
            ),
        );
    }
    if let Some(branches) = node.get("allOf").and_then(Value::as_array) {
        if branches.len() == 1 {
            result.insert(
                "allOf".into(),
                Value::Array(vec![lower(
                    &branches[0],
                    root,
                    depth + 1,
                    visiting,
                    budget,
                )?]),
            );
        }
    }
    if kinds.contains(&"object") {
        let mut properties = Map::new();
        if let Some(children) = node.get("properties") {
            for (name, child) in children.as_object()? {
                properties.insert(
                    name.clone(),
                    lower(child, root, depth + 1, visiting, budget)?,
                );
            }
        }
        result.insert("properties".into(), Value::Object(properties));
        if let Some(required) = node.get("required") {
            result.insert("required".into(), required.clone());
        }
        // xAI defaults this to false; JSON Schema defaults it to true.
        let additional = match node.get("additionalProperties") {
            None => Value::Bool(true),
            Some(value) if value.is_boolean() => value.clone(),
            Some(value) => lower(value, root, depth + 1, visiting, budget)?,
        };
        result.insert("additionalProperties".into(), additional);
    }
    if kinds.contains(&"array") {
        if let Some(prefix) = node.get("prefixItems") {
            result.insert(
                "prefixItems".into(),
                Value::Array(
                    prefix
                        .as_array()?
                        .iter()
                        .map(|item| lower(item, root, depth + 1, visiting, budget))
                        .collect::<Option<_>>()?,
                ),
            );
        }
        result.insert(
            "items".into(),
            lower(node.get("items")?, root, depth + 1, visiting, budget)?,
        );
    }
    for key in ["enum", "const"] {
        if let Some(value) = node.get(key) {
            if key == "enum" && value.as_array().is_none_or(|values| values.is_empty()) {
                return None;
            }
            result.insert(key.into(), value.clone());
        }
    }
    if kinds.is_empty()
        && !result.contains_key("anyOf")
        && !result.contains_key("allOf")
        && !result.contains_key("enum")
        && !result.contains_key("const")
    {
        return None;
    }
    for key in [
        "description",
        "title",
        "default",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
        "minProperties",
        "maxProperties",
    ] {
        if let Some(value) = node.get(key) {
            result.insert(key.into(), value.clone());
        }
    }
    // Regex anchoring and character semantics differ. Keep patterns in canonical
    // guidance/core validation instead of narrowing valid canonical inputs.
    if let Some(format) = node.get("format").and_then(Value::as_str) {
        if matches!(
            format,
            "date" | "time" | "date-time" | "email" | "uuid" | "ipv4" | "ipv6" | "uri"
        ) {
            result.insert("format".into(), json!(format));
        }
    }
    Some(Value::Object(result))
}
