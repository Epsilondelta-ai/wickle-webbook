//! Versioned strict projection. Original constraints are preserved by the core.
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;
use wickle::*;

/// Responses-compatible Tool schemas with reversible omission/value handling.
#[derive(Debug, Clone, Copy, Default)]
pub struct ResponsesToolSchemaCompiler;
impl ProviderToolSchemaCompiler for ResponsesToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new("wickle-responses-tool-schema").expect("constant"),
            version: Id::new("2").expect("constant"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        let restricted = target
            .model
            .as_ref()
            .is_none_or(|model| model.id.as_str().starts_with("ft:"));
        compile(tool, target, SchemaPolicy::openai(restricted))
    }
}

/// Azure Responses projection using its documented schema subset and limits.
#[derive(Debug, Clone, Copy, Default)]
pub struct AzureResponsesToolSchemaCompiler;
impl ProviderToolSchemaCompiler for AzureResponsesToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new("wickle-azure-responses-tool-schema").expect("constant"),
            version: Id::new("2").expect("constant"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        if target.api_contract.operation.as_str() != "responses" {
            return Err(invalid("operation"));
        }
        let policy = SchemaPolicy::azure();
        let properties = tool
            .model_input_schema
            .get("properties")
            .and_then(Value::as_object);
        if properties.is_some_and(|properties| properties.len() > policy.max_properties) {
            let mut wire_tool = tool.clone();
            wire_tool.model_input_schema = object_schema(Map::from_iter([(
                "arguments".into(),
                json!({"type":"string"}),
            )]));
            return Ok(ProviderToolProjection {
                wire_tool,
                decode_plan: ArgumentDecodePlan::JsonObjectText {
                    wire_name: "arguments".into(),
                },
            });
        }
        compile(tool, target, policy)
    }
}
#[derive(Clone, Copy)]
struct SchemaPolicy {
    restricted_constraints: bool,
    max_depth: usize,
    max_properties: usize,
    max_object_depth: usize,
}
impl SchemaPolicy {
    fn openai(restricted_constraints: bool) -> Self {
        Self {
            restricted_constraints,
            max_depth: 10,
            max_properties: 5000,
            max_object_depth: 10,
        }
    }
    fn azure() -> Self {
        Self {
            restricted_constraints: true,
            max_depth: 5,
            max_properties: 100,
            max_object_depth: 4,
        }
    }
}
fn compile(
    tool: &ModelTool,
    target: &ProviderToolTarget,
    policy: SchemaPolicy,
) -> Result<ProviderToolProjection, ContractError> {
    if target.api_contract.operation.as_str() != "responses" {
        return Err(invalid("operation"));
    }
    let fine_tuned = policy.restricted_constraints;
    if supported_with(&tool.model_input_schema, policy) {
        return Ok(ProviderToolProjection {
            wire_tool: tool.clone(),
            decode_plan: ArgumentDecodePlan::Identity {},
        });
    }
    let root = &tool.model_input_schema;
    let properties = root
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("properties"))?;
    let required: BTreeSet<_> = root
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    let mut wire = Map::new();
    let mut fields = vec![];
    let mut expansion = ExpansionBudget {
        nodes: 1024,
        bytes: 32 * 1024,
    };
    for (name, schema) in properties {
        let optional = !required.contains(name.as_str());
        let native = lower(
            schema,
            root,
            fine_tuned,
            1,
            &mut BTreeSet::new(),
            &mut expansion,
        );
        let (schema, encoding) = match native {
            Some(schema) if optional => (
                json!({"type":"object","properties":{"present":{"type":"boolean"},"value":{"anyOf":[schema,{"type":"null"}]}},"required":["present","value"],"additionalProperties":false}),
                ArgumentValueEncoding::Presence {
                    present_key: "present".into(),
                    value_key: "value".into(),
                },
            ),
            Some(schema) => (schema, ArgumentValueEncoding::Identity {}),
            None => (
                json_text_schema(optional),
                ArgumentValueEncoding::JsonText { optional },
            ),
        };
        wire.insert(name.clone(), schema);
        fields.push(ArgumentFieldMapping {
            wire_name: name.clone(),
            canonical_name: name.clone(),
            encoding,
        });
    }
    let mut projected = tool.clone();
    projected.model_input_schema = object_schema(wire);
    // Representation overhead must not exceed either provider limits or
    // the core's original per-schema byte bound. Compact the largest
    // remaining native field, preserving every canonical field and rule.
    while !supported_with(&projected.model_input_schema, policy)
        || serde_json::to_vec(&projected)
            .map_err(|_| invalid("json"))?
            .len()
            > ProviderToolSchemaLimits::default().max_schema_bytes
    {
        let candidate = fields
            .iter()
            .enumerate()
            .filter(|(_, field)| !matches!(field.encoding, ArgumentValueEncoding::JsonText { .. }))
            .max_by_key(|(_, field)| {
                projected.model_input_schema["properties"][&field.wire_name]
                    .to_string()
                    .len()
            })
            .map(|(index, _)| index)
            .ok_or_else(|| invalid("limits"))?;
        let field = &mut fields[candidate];
        let optional = !required.contains(field.canonical_name.as_str());
        projected.model_input_schema["properties"][&field.wire_name] = json_text_schema(optional);
        field.encoding = ArgumentValueEncoding::JsonText { optional };
    }
    Ok(ProviderToolProjection {
        wire_tool: projected,
        decode_plan: ArgumentDecodePlan::Fields { fields },
    })
}

fn object_schema(properties: Map<String, Value>) -> Value {
    let required: Vec<_> = properties.keys().cloned().collect();
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}
fn json_text_schema(_optional: bool) -> Value {
    // The frozen constraint fragment explains the encoding once for the Tool;
    // repeating it in every property needlessly consumes the schema byte budget.
    json!({"type":"string"})
}

// Bound work before expanding shared references. A final wire-size check alone
// cannot stop a small DAG from allocating a large intermediate tree.
struct ExpansionBudget {
    nodes: usize,
    bytes: usize,
}
impl ExpansionBudget {
    fn charge(&mut self, schema: &Value) -> Option<()> {
        let bytes = serde_json::to_vec(schema).ok()?.len();
        self.nodes = self.nodes.checked_sub(1)?;
        self.bytes = self.bytes.checked_sub(bytes)?;
        Some(())
    }
}
fn lower(
    schema: &Value,
    root: &Value,
    fine_tuned: bool,
    depth: usize,
    visiting: &mut BTreeSet<String>,
    expansion: &mut ExpansionBudget,
) -> Option<Value> {
    if depth > 8 {
        return None;
    }
    expansion.charge(schema)?;
    let node = schema.as_object()?;
    if let Some(reference) = node.get("$ref") {
        let reference = reference.as_str()?;
        if node
            .keys()
            .any(|key| !matches!(key.as_str(), "$ref" | "description" | "title" | "$comment"))
            || !reference.starts_with('#')
            || !visiting.insert(reference.into())
        {
            return None;
        }
        let target = root.pointer(&reference[1..])?;
        let result = lower(target, root, fine_tuned, depth + 1, visiting, expansion);
        visiting.remove(reference);
        return result;
    }
    if let Some(alternatives) = node.get("anyOf").or_else(|| node.get("oneOf")) {
        // Keeping only a union would lose sibling intersections. Use JSON text
        // for mixed forms rather than accidentally narrowing their values.
        if node.keys().any(|key| {
            !matches!(
                key.as_str(),
                "anyOf" | "oneOf" | "description" | "title" | "$comment"
            )
        }) {
            return None;
        }
        let branches: Vec<_> = alternatives
            .as_array()?
            .iter()
            .map(|branch| lower(branch, root, fine_tuned, depth + 1, visiting, expansion))
            .collect::<Option<_>>()?;
        return Some(json!({"anyOf":branches}));
    }
    let kind = node.get("type")?;
    let types = schema_types(kind)?;
    let mut result = Map::new();
    result.insert("type".into(), kind.clone());
    if types.contains(&"object") {
        if node.contains_key("patternProperties")
            || node.get("additionalProperties") != Some(&Value::Bool(false))
        {
            return None;
        }
        let properties = node.get("properties")?.as_object()?;
        if !all_required(node, properties) {
            return None;
        }
        let mut projected = Map::new();
        for (name, child) in properties {
            projected.insert(
                name.clone(),
                lower(child, root, fine_tuned, depth + 1, visiting, expansion)?,
            );
        }
        result.insert("properties".into(), Value::Object(projected));
        result.insert(
            "required".into(),
            node.get("required").cloned().unwrap_or_else(|| json!([])),
        );
        result.insert("additionalProperties".into(), Value::Bool(false));
    }
    if types.contains(&"array") {
        if node.contains_key("prefixItems") || node.get("items").is_some_and(Value::is_array) {
            return None;
        }
        result.insert(
            "items".into(),
            lower(
                node.get("items")?,
                root,
                fine_tuned,
                depth + 1,
                visiting,
                expansion,
            )?,
        );
    }
    if let Some(value) = node.get("enum") {
        if value.as_array().is_some_and(|values| {
            !values.is_empty()
                && values
                    .iter()
                    .all(|value| !value.is_object() && !value.is_array())
        }) {
            result.insert("enum".into(), value.clone());
        }
    } else if let Some(value) = node.get("const") {
        if !value.is_object() && !value.is_array() {
            result.insert("enum".into(), json!([value]));
        }
    }
    if !fine_tuned {
        for key in [
            "pattern",
            "minimum",
            "maximum",
            "exclusiveMinimum",
            "exclusiveMaximum",
            "multipleOf",
            "minItems",
            "maxItems",
        ] {
            if let Some(value) = node.get(key) {
                result.insert(key.into(), value.clone());
            }
        }
        if let Some(value) = node
            .get("format")
            .filter(|value| value.as_str().is_some_and(known_format))
        {
            result.insert("format".into(), value.clone());
        }
    }
    if let Some(description) = node.get("description").filter(|value| value.is_string()) {
        result.insert("description".into(), description.clone());
    }
    Some(Value::Object(result))
}
fn schema_types(value: &Value) -> Option<Vec<&str>> {
    let types = if let Some(kind) = value.as_str() {
        vec![kind]
    } else {
        let values = value.as_array()?;
        if values.len() != 2 || !values.iter().any(|kind| kind == "null") {
            return None;
        }
        values
            .iter()
            .map(Value::as_str)
            .collect::<Option<Vec<_>>>()?
    };
    types
        .iter()
        .all(|kind| {
            matches!(
                *kind,
                "string" | "number" | "integer" | "boolean" | "object" | "array" | "null"
            )
        })
        .then_some(types)
}
fn all_required(node: &Map<String, Value>, properties: &Map<String, Value>) -> bool {
    let Some(required) = node.get("required").and_then(Value::as_array) else {
        return properties.is_empty();
    };
    let names: BTreeSet<_> = required.iter().filter_map(Value::as_str).collect();
    names.len() == required.len()
        && names.len() == properties.len()
        && properties.keys().all(|name| names.contains(name.as_str()))
}
fn known_format(value: &str) -> bool {
    matches!(
        value,
        "date-time"
            | "time"
            | "date"
            | "duration"
            | "email"
            | "hostname"
            | "ipv4"
            | "ipv6"
            | "uuid"
    )
}
#[derive(Default)]
struct Bounds {
    properties: usize,
    enums: usize,
    characters: usize,
}
/// Check only; this never rewrites the already compiled wire schema.
pub(crate) fn strict_schema_supported(schema: &Value, fine_tuned: bool) -> bool {
    supported_with(schema, SchemaPolicy::openai(fine_tuned))
}
pub(crate) fn azure_strict_schema_supported(schema: &Value) -> bool {
    supported_with(schema, SchemaPolicy::azure())
}
fn supported_with(schema: &Value, policy: SchemaPolicy) -> bool {
    schema.get("type") == Some(&json!("object"))
        && schema.get("anyOf").is_none()
        && strict_node(schema, schema, policy, 0, &mut Bounds::default())
}
fn strict_node(
    schema: &Value,
    root: &Value,
    policy: SchemaPolicy,
    depth: usize,
    bounds: &mut Bounds,
) -> bool {
    let Some(node) = schema.as_object() else {
        return false;
    };
    if depth > policy.max_depth
        || node.keys().any(|key| {
            !matches!(
                key.as_str(),
                "type"
                    | "properties"
                    | "required"
                    | "additionalProperties"
                    | "items"
                    | "enum"
                    | "anyOf"
                    | "$defs"
                    | "$ref"
                    | "description"
                    | "title"
                    | "pattern"
                    | "format"
                    | "minimum"
                    | "maximum"
                    | "exclusiveMinimum"
                    | "exclusiveMaximum"
                    | "multipleOf"
                    | "minItems"
                    | "maxItems"
            )
        })
    {
        return false;
    }
    if policy.restricted_constraints
        && [
            "pattern",
            "format",
            "minimum",
            "maximum",
            "exclusiveMinimum",
            "exclusiveMaximum",
            "multipleOf",
            "minItems",
            "maxItems",
        ]
        .iter()
        .any(|key| node.contains_key(*key))
    {
        return false;
    }
    if node
        .get("format")
        .is_some_and(|value| !value.as_str().is_some_and(known_format))
    {
        return false;
    }
    if let Some(reference) = node.get("$ref") {
        if reference.as_str().is_none_or(|reference| {
            !reference.starts_with('#') || root.pointer(&reference[1..]).is_none()
        }) {
            return false;
        }
    }
    let types = match node.get("type") {
        Some(value) => match schema_types(value) {
            Some(types) => types,
            None => return false,
        },
        None if node.contains_key("anyOf") || node.contains_key("$ref") => vec![],
        None => return false,
    };
    if types.contains(&"object") {
        if depth > policy.max_object_depth {
            return false;
        }
        let Some(properties) = node.get("properties").and_then(Value::as_object) else {
            return false;
        };
        if node.get("additionalProperties") != Some(&Value::Bool(false))
            || !node.contains_key("required")
            || !all_required(node, properties)
        {
            return false;
        }
    }
    if types.contains(&"array") && !node.contains_key("items") {
        return false;
    }
    if let Some(values) = node.get("enum") {
        let Some(values) = values.as_array() else {
            return false;
        };
        if values.is_empty()
            || values
                .iter()
                .any(|value| value.is_array() || value.is_object())
        {
            return false;
        }
        bounds.enums = bounds.enums.saturating_add(values.len());
        let characters: usize = values
            .iter()
            .filter_map(Value::as_str)
            .map(|value| value.chars().count())
            .sum();
        if values.len() > 250 && characters > 15_000 {
            return false;
        }
        bounds.characters = bounds.characters.saturating_add(characters);
    }
    for key in ["properties", "$defs"] {
        if let Some(children) = node.get(key) {
            let Some(children) = children.as_object() else {
                return false;
            };
            if key == "properties" {
                bounds.properties = bounds.properties.saturating_add(children.len());
            }
            bounds.characters = bounds.characters.saturating_add(
                children
                    .keys()
                    .map(|name| name.chars().count())
                    .sum::<usize>(),
            );
            for child in children.values() {
                if !strict_node(child, root, policy, depth + 1, bounds) {
                    return false;
                }
            }
        }
    }
    if let Some(items) = node.get("items") {
        if !strict_node(items, root, policy, depth + 1, bounds) {
            return false;
        }
    }
    if let Some(branches) = node.get("anyOf") {
        let Some(branches) = branches.as_array() else {
            return false;
        };
        if branches.is_empty()
            || branches
                .iter()
                .any(|branch| !strict_node(branch, root, policy, depth + 1, bounds))
        {
            return false;
        }
    }
    bounds.properties <= policy.max_properties
        && bounds.enums <= 1000
        && bounds.characters <= 120_000
}
fn invalid(path: &str) -> ContractError {
    ContractError::new(
        ErrorCode::UnsupportedInputProjection,
        format!("responses.schema.{path}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reference_expansion_stops_before_exhausting_node_or_byte_work_limits() {
        let root = json!({"$defs":{"leaf":{"type":"string"}},"type":"object","properties":{"a":{"$ref":"#/$defs/leaf"},"b":{"$ref":"#/$defs/leaf"}},"required":["a","b"],"additionalProperties":false});
        for (nodes, bytes) in [(3, 32 * 1024), (1024, 16)] {
            let mut budget = ExpansionBudget { nodes, bytes };
            assert!(lower(&root, &root, false, 1, &mut BTreeSet::new(), &mut budget).is_none());
        }
        let mut budget = ExpansionBudget {
            nodes: 1024,
            bytes: 32 * 1024,
        };
        let expanded = lower(&root, &root, false, 1, &mut BTreeSet::new(), &mut budget).unwrap();
        assert_eq!(expanded["properties"]["a"], json!({"type":"string"}));
        assert_eq!(expanded["properties"]["b"], json!({"type":"string"}));
    }
}
