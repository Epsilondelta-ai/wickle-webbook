//! Pure, bounded provider projection of an already separated Tool input contract.
use crate::{
    ApiContract, CompiledTool, ContractError, ErrorCode, Id, JsonDigest, JsonObject, ModelTool,
    VersionedRef, canonical_digest, parse_json, serialization::data_digest,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, fmt};

/// Exact provider protocol and capability revision used for compilation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderToolTarget {
    /// Exact model/release, absent only in older or manually unqualified contracts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<VersionedRef>,
    /// Provider namespace, including deployment-specific provider adapters.
    pub provider: Id,
    /// Exact operation and API version.
    pub api_contract: ApiContract,
    /// Pinned target capability revision.
    pub capability_revision: Id,
}
impl ProviderToolTarget {
    /// Capture the selected route without connection metadata or credentials.
    pub fn for_route(route: &crate::ResolvedModelRoute) -> Self {
        Self {
            model: Some(VersionedRef {
                id: route.model_id.clone(),
                version: route.model_version.clone(),
            }),
            provider: route.provider.clone(),
            api_contract: route.api_contract.clone(),
            capability_revision: route.capability_revision.clone(),
        }
    }
    fn matches_saved(&self, saved: &Self) -> bool {
        self.provider == saved.provider
            && self.api_contract == saved.api_contract
            && self.capability_revision == saved.capability_revision
            && saved
                .model
                .as_ref()
                .is_none_or(|model| self.model.as_ref() == Some(model))
    }
}
/// Finite bounds on compilation, persisted projection and incoming arguments.
#[derive(Debug, Clone, Copy)]
pub struct ProviderToolSchemaLimits {
    /// Maximum serialized canonical or wire schema/tool bytes.
    pub max_schema_bytes: usize,
    /// Maximum schema nesting before traversal or serialization.
    pub max_schema_depth: usize,
    /// Maximum total serialized compiled contract bytes, including explanations.
    pub max_contract_bytes: usize,
    /// Maximum provider argument bytes before parsing.
    pub max_argument_bytes: usize,
}
impl Default for ProviderToolSchemaLimits {
    fn default() -> Self {
        Self {
            max_schema_bytes: 65_536,
            max_schema_depth: 64,
            max_contract_bytes: 262_144,
            max_argument_bytes: 65_536,
        }
    }
}
/// Reversible representation of a single model-owned field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArgumentValueEncoding {
    /// Preserve the JSON value, including explicit null.
    Identity {},
    /// A JSON document encoded as a string. Optional values use [] for omission
    /// and `[value]` for a supplied value, keeping explicit null distinct.
    JsonText {
        /// Whether the string represents an optional zero-or-one value array.
        optional: bool,
    },
    /// Encode omission separately from null using an object envelope.
    Presence {
        /// Boolean discriminator: false means omitted, true means supplied.
        present_key: String,
        /// Required value member; must be null when present is false.
        value_key: String,
    },
}
/// One-to-one mapping from a wire property to an exposed canonical property.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArgumentFieldMapping {
    /// Property emitted by the provider.
    pub wire_name: String,
    /// Original model-owned property, never a system-owned property.
    pub canonical_name: String,
    /// Value and omission restoration rule.
    pub encoding: ArgumentValueEncoding,
}
/// Stored codec. It never guesses that null means omission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArgumentDecodePlan {
    /// Property names and values are unchanged.
    Identity {},
    /// Entire canonical argument object encoded as one JSON string property.
    JsonObjectText {
        /// The sole wire property; its decoded value must be an object.
        wire_name: String,
    },
    /// Explicit complete field mapping; unknown wire properties are errors.
    Fields {
        /// Ordered mappings with unique wire and canonical names.
        fields: Vec<ArgumentFieldMapping>,
    },
}
/// Compiler output before the core stamps original identity and explanations.
#[derive(Debug, Clone)]
pub struct ProviderToolProjection {
    /// The exact Tool definition submitted to the provider.
    pub wire_tool: ModelTool,
    /// Reversible normalization back into original model-owned arguments.
    pub decode_plan: ArgumentDecodePlan,
}
/// Pure trusted adapter extension. Input contains only the model-visible Tool;
/// hidden definitions, system values, credentials and runtime handles are absent.
pub trait ProviderToolSchemaCompiler: Send + Sync {
    /// Immutable implementation identity; change its version when output changes.
    fn reference(&self) -> VersionedRef;
    /// Preserve native constraints where supported. Unsupported representation
    /// must use a relaxed schema plus a reversible codec, never delete the Tool.
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError>;
}
/// Compiler for protocols that accept the original model-visible JSON Schema.
#[derive(Debug, Clone, Copy, Default)]
pub struct NativeToolSchemaCompiler;
impl ProviderToolSchemaCompiler for NativeToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new("wickle-native-tool-schema").expect("static id"),
            version: Id::new("1").expect("static version"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
        _: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        Ok(ProviderToolProjection {
            wire_tool: tool.clone(),
            decode_plan: ArgumentDecodePlan::Identity {},
        })
    }
}
/// Deterministic trusted explanation associated with this exact Tool projection.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolConstraintFragment {
    /// Stable content-addressed identity, ordered in the compiled contract.
    pub id: Id,
    /// Only canonical model-visible schema and codec instructions.
    pub text: String,
    /// Digest of the exact explanation text.
    pub digest: JsonDigest,
}
impl fmt::Debug for ToolConstraintFragment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolConstraintFragment")
            .field("id", &self.id)
            .field("digest", &self.digest)
            .finish()
    }
}
/// Where an original schema node is enforced. Core validation is always required.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolConstraintEnforcement {
    /// JSON pointer into the canonical model schema; empty denotes the whole schema.
    pub canonical_pointer: String,
    /// Confirmed native under an identical schema and identity codec. False is
    /// conservative: a relaxed wire schema can still enforce part of this node.
    pub provider_native: bool,
    /// Included in the canonical constraint explanation.
    pub context_text: bool,
    /// Original validation must occur after decoding, before execution.
    pub core: bool,
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContractData {
    schema_version: String,
    tool: VersionedRef,
    canonical_name: Id,
    descriptor_digest: JsonDigest,
    canonical_schema_digest: JsonDigest,
    compiler: VersionedRef,
    target: ProviderToolTarget,
    wire_tool: ModelTool,
    decode_plan: ArgumentDecodePlan,
    fragments: Vec<ToolConstraintFragment>,
    enforcement: Vec<ToolConstraintEnforcement>,
}
/// Immutable route-specific contract. Serialize only to protected storage; submit
/// wire_tool and constraint_fragments to the model, not the whole record.
#[derive(Clone, Serialize)]
pub struct CompiledToolContract {
    data: ContractData,
    digest: JsonDigest,
}
impl fmt::Debug for CompiledToolContract {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompiledToolContract")
            .field("tool", &self.data.tool)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}
impl CompiledToolContract {
    /// Compile only after canonical ownership separation and validate finite output.
    pub fn compile(
        tool: &CompiledTool,
        target: ProviderToolTarget,
        compiler: &dyn ProviderToolSchemaCompiler,
        limits: ProviderToolSchemaLimits,
    ) -> Result<Self, ContractError> {
        check_limits(limits)?;
        let visible = tool.to_model_tool();
        depth_bound(&visible.model_input_schema, limits.max_schema_depth)?;
        bounded(&visible, limits.max_schema_bytes)?;
        let reference = compiler.reference();
        let projection = compiler.compile(&visible, &target)?;
        if compiler.reference() != reference {
            return Err(invalid("provider_tool.compiler_revision"));
        }
        Self::build(
            tool,
            target,
            reference,
            projection,
            limits,
            "wickle.provider-tool-contract.v2",
        )
    }
    fn build(
        tool: &CompiledTool,
        target: ProviderToolTarget,
        compiler: VersionedRef,
        projection: ProviderToolProjection,
        limits: ProviderToolSchemaLimits,
        schema_version: &str,
    ) -> Result<Self, ContractError> {
        check_limits(limits)?;
        if !matches!(
            schema_version,
            "wickle.provider-tool-contract.v1" | "wickle.provider-tool-contract.v2"
        ) {
            return Err(invalid("provider_tool.schema_version"));
        }
        depth_bound(tool.model_input_schema(), limits.max_schema_depth)?;
        depth_bound(
            &projection.wire_tool.model_input_schema,
            limits.max_schema_depth,
        )?;
        bounded(&projection.wire_tool, limits.max_schema_bytes)?;
        let schema = &projection.wire_tool.model_input_schema;
        if !valid_name(projection.wire_tool.name.as_str())
            || schema.get("type") != Some(&json!("object"))
            || schema.get("additionalProperties") != Some(&json!(false))
        {
            return Err(invalid("provider_tool.wire_boundary"));
        }
        crate::tool_schema::compile_validator(schema)?;
        validate_codec(tool.model_input_schema(), schema, &projection.decode_plan)?;
        let identity = matches!(projection.decode_plan, ArgumentDecodePlan::Identity {});
        let explained = schema != tool.model_input_schema() || !identity;
        let fragments = if explained {
            let mut text = format!(
                "Tool {}: arguments must satisfy this canonical JSON Schema after decoding: {}\nDecode representation: {}. Field mappings restore wire_name to canonical_name. For a presence envelope, both members are required: true marks a supplied value (including explicit null); false with a null value placeholder means omission. Preserve omission and explicit null as distinct values.",
                projection.wire_tool.name,
                serde_json::to_string(tool.model_input_schema())
                    .map_err(|_| invalid("provider_tool.schema"))?,
                serde_json::to_string(&projection.decode_plan)
                    .map_err(|_| invalid("provider_tool.codec"))?
            );
            if schema_version == "wickle.provider-tool-contract.v1" {
                if matches!(&projection.decode_plan, ArgumentDecodePlan::Fields { fields } if fields.iter().any(|field| matches!(field.encoding, ArgumentValueEncoding::JsonText { .. })))
                {
                    text.push_str(" For json_text, the wire value is a JSON string parsed by the core. With optional=false it encodes the canonical value itself. With optional=true it must encode [] for omission or [value] for a supplied value, including [null] for explicit null. Nested optional properties remain absent inside that JSON document; do not replace absence with null.");
                }
                if matches!(
                    &projection.decode_plan,
                    ArgumentDecodePlan::JsonObjectText { .. }
                ) {
                    text.push_str(" For json_object_text, send exactly the named wire property as a JSON string containing the entire canonical argument object. Preserve absent properties and explicit null values inside it; do not include system-owned fields.");
                }
            } else {
                text = format!(
                    "Tool {}: arguments must satisfy this canonical JSON Schema after decoding: {}\nDecode representation: {}. Follow the declared wire schema and each field's encoding. Preserve omission and explicit null as distinct values.",
                    projection.wire_tool.name,
                    serde_json::to_string(tool.model_input_schema())
                        .map_err(|_| invalid("provider_tool.schema"))?,
                    serde_json::to_string(&projection.decode_plan)
                        .map_err(|_| invalid("provider_tool.codec"))?
                );
                match &projection.decode_plan {
                    ArgumentDecodePlan::Identity {} => text.push_str(" Identity values are sent directly as canonical values; do not stringify them or add an envelope."),
                    ArgumentDecodePlan::Fields { fields } => {
                        if fields.iter().any(|field|matches!(field.encoding, ArgumentValueEncoding::Identity {})) {
                            text.push_str(" Fields marked identity use the canonical value directly. Do not stringify it or add an envelope. Omit absent optional fields; send JSON null for an explicit null.");
                        }
                        if fields.iter().any(|field|matches!(field.encoding, ArgumentValueEncoding::Presence { .. })) {
                            text.push_str(" Only fields marked presence use the specified present_key and value_key envelope. Both members are required: true marks a supplied value (including null); false with a null placeholder means omission.");
                        }
                        if fields.iter().any(|field|matches!(field.encoding, ArgumentValueEncoding::JsonText { optional: false })) {
                            text.push_str(" Only fields marked json_text with optional=false use a JSON string encoding the canonical value itself. Nested absent properties stay absent.");
                        }
                        if fields.iter().any(|field|matches!(field.encoding, ArgumentValueEncoding::JsonText { optional: true })) {
                            text.push_str(" Only fields marked json_text with optional=true use a JSON string encoding [] for omission or [value] for a supplied value, including [null] for explicit null. Nested absent properties stay absent.");
                        }
                    }
                    ArgumentDecodePlan::JsonObjectText { .. } => text.push_str(" Send exactly the named wire property as a JSON string containing the entire canonical argument object. Preserve absent properties and explicit null; do not include system-owned fields."),
                }
            }
            let digest = data_digest(&text);
            vec![ToolConstraintFragment {
                id: Id::new(format!(
                    "tool-constraints-{}",
                    canonical_digest(&json!(text))
                ))?,
                text,
                digest,
            }]
        } else {
            vec![]
        };
        let mut enforcement = Vec::new();
        collect_enforcement(
            tool.model_input_schema(),
            schema,
            "",
            identity && schema == tool.model_input_schema(),
            explained,
            &mut enforcement,
        );
        let data = ContractData {
            schema_version: schema_version.into(),
            tool: tool.descriptor().tool.clone(),
            canonical_name: tool.descriptor().name.clone(),
            descriptor_digest: tool.descriptor_digest().clone(),
            canonical_schema_digest: tool.model_schema_digest().clone(),
            compiler,
            target,
            wire_tool: projection.wire_tool,
            decode_plan: projection.decode_plan,
            fragments,
            enforcement,
        };
        let result = Self {
            digest: data_digest(&data),
            data,
        };
        bounded(&result, limits.max_contract_bytes)?;
        Ok(result)
    }
    /// Restore against the trusted original Tool, destination and expected digest.
    /// This uses the saved codec and never invokes a newer compiler implementation.
    pub fn restore(
        text: &str,
        tool: &CompiledTool,
        target: &ProviderToolTarget,
        expected: &JsonDigest,
        limits: ProviderToolSchemaLimits,
    ) -> Result<Self, ContractError> {
        check_limits(limits)?;
        if text.len() > limits.max_contract_bytes {
            return Err(invalid("provider_tool.size"));
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Saved {
            data: ContractData,
            digest: JsonDigest,
        }
        let saved: Saved = serde_json::from_value(parse_json(text)?)
            .map_err(|_| invalid("provider_tool.record"))?;
        if &saved.digest != expected
            || data_digest(&saved.data) != *expected
            || !target.matches_saved(&saved.data.target)
        {
            return Err(invalid("provider_tool.identity"));
        }
        let rebuilt = Self::build(
            tool,
            saved.data.target.clone(),
            saved.data.compiler.clone(),
            ProviderToolProjection {
                wire_tool: saved.data.wire_tool.clone(),
                decode_plan: saved.data.decode_plan.clone(),
            },
            limits,
            &saved.data.schema_version,
        )?;
        if rebuilt.data != saved.data || rebuilt.digest != *expected {
            return Err(invalid("provider_tool.identity"));
        }
        Ok(rebuilt)
    }
    pub(crate) fn inspection(
        value: Value,
    ) -> Result<crate::inspection::SavedToolInspection, ContractError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Saved {
            data: ContractData,
            digest: JsonDigest,
        }
        let saved: Saved =
            serde_json::from_value(value).map_err(|_| invalid("provider_tool.record"))?;
        if !matches!(
            saved.data.schema_version.as_str(),
            "wickle.provider-tool-contract.v1" | "wickle.provider-tool-contract.v2"
        ) || data_digest(&saved.data) != saved.digest
        {
            return Err(invalid("provider_tool.identity"));
        }
        Ok(crate::inspection::SavedToolInspection {
            tool: saved.data.tool,
            canonical_name: saved.data.canonical_name,
            canonical_schema_digest: saved.data.canonical_schema_digest,
            compiler: saved.data.compiler,
            target: saved.data.target,
            wire_tool: saved.data.wire_tool,
            decode_plan_digest: data_digest(&saved.data.decode_plan),
            digest: saved.digest,
            fragments: saved.data.fragments,
            enforcement: saved.data.enforcement,
        })
    }
    /// Original model-facing Tool name to restore after provider name mapping.
    pub fn canonical_name(&self) -> &Id {
        &self.data.canonical_name
    }
    /// Original registered Tool identity.
    pub fn tool(&self) -> &VersionedRef {
        &self.data.tool
    }
    /// Frozen provider-facing Tool, excluding hidden input metadata.
    pub fn wire_tool(&self) -> &ModelTool {
        &self.data.wire_tool
    }
    /// Ordered trusted fragments that must accompany the Tool definition.
    pub fn constraint_fragments(&self) -> &[ToolConstraintFragment] {
        &self.data.fragments
    }
    /// Exact original constraint locations and enforcement mechanisms.
    pub fn enforcement(&self) -> &[ToolConstraintEnforcement] {
        &self.data.enforcement
    }
    /// Pinned compiler identity and version.
    pub fn compiler(&self) -> &VersionedRef {
        &self.data.compiler
    }
    /// Exact destination protocol/capability revision.
    pub fn target(&self) -> &ProviderToolTarget {
        &self.data.target
    }
    /// Protected compilation identity.
    pub fn digest(&self) -> &JsonDigest {
        &self.digest
    }
    /// Encode canonical historical model arguments for this exact provider
    /// representation. System inputs never belong in this map.
    pub fn encode_arguments(&self, input: &JsonObject) -> Result<JsonObject, ContractError> {
        match &self.data.decode_plan {
            ArgumentDecodePlan::Identity {} => Ok(input.clone()),
            ArgumentDecodePlan::JsonObjectText { wire_name } => Ok(JsonObject::from([(
                wire_name.clone(),
                Value::String(serde_json::to_string(input).map_err(|_| arguments())?),
            )])),
            ArgumentDecodePlan::Fields { fields } => {
                if input
                    .keys()
                    .any(|key| !fields.iter().any(|field| &field.canonical_name == key))
                {
                    return Err(arguments());
                }
                let mut output = JsonObject::new();
                for field in fields {
                    let value = input.get(&field.canonical_name);
                    match &field.encoding {
                        ArgumentValueEncoding::Identity {} => {
                            if let Some(value) = value {
                                output.insert(field.wire_name.clone(), value.clone());
                            }
                        }
                        ArgumentValueEncoding::JsonText { optional } => {
                            if *optional || value.is_some() {
                                let encoded = if *optional {
                                    serde_json::to_string(&value.into_iter().collect::<Vec<_>>())
                                } else {
                                    serde_json::to_string(value.expect("present value"))
                                }
                                .map_err(|_| arguments())?;
                                output.insert(field.wire_name.clone(), Value::String(encoded));
                            }
                        }
                        ArgumentValueEncoding::Presence {
                            present_key,
                            value_key,
                        } => {
                            let envelope = serde_json::Map::from_iter([
                                (present_key.clone(), Value::Bool(value.is_some())),
                                (value_key.clone(), value.cloned().unwrap_or(Value::Null)),
                            ]);
                            output.insert(field.wire_name.clone(), Value::Object(envelope));
                        }
                    }
                }
                Ok(output)
            }
        }
    }
    /// Restore model-owned names and values. Validation/defaults/system binding
    /// are separate boundaries; this does not authorize or execute the Tool.
    pub fn decode_arguments(
        &self,
        raw: &str,
        limits: ProviderToolSchemaLimits,
    ) -> Result<JsonObject, ContractError> {
        check_limits(limits)?;
        let object = parse_provider_arguments(raw, limits.max_argument_bytes)?;
        match &self.data.decode_plan {
            ArgumentDecodePlan::Identity {} => Ok(object.clone()),
            ArgumentDecodePlan::JsonObjectText { wire_name } => {
                if object.len() != 1 {
                    return Err(arguments());
                }
                let text = object
                    .get(wire_name)
                    .and_then(Value::as_str)
                    .ok_or_else(arguments)?;
                parse_provider_arguments(text, limits.max_argument_bytes)
            }
            ArgumentDecodePlan::Fields { fields } => {
                let mut result = JsonObject::new();
                for (name, value) in &object {
                    let mapping = fields
                        .iter()
                        .find(|field| &field.wire_name == name)
                        .ok_or_else(arguments)?;
                    let restored = match &mapping.encoding {
                        ArgumentValueEncoding::Identity {} => Some(value.clone()),
                        ArgumentValueEncoding::JsonText { optional } => {
                            let text = value.as_str().ok_or_else(arguments)?;
                            let parsed = parse_provider_value(text, limits.max_argument_bytes)?;
                            if *optional {
                                let values = parsed.as_array().ok_or_else(arguments)?;
                                match values.len() {
                                    0 => None,
                                    1 => Some(values[0].clone()),
                                    _ => return Err(arguments()),
                                }
                            } else {
                                Some(parsed)
                            }
                        }
                        ArgumentValueEncoding::Presence {
                            present_key,
                            value_key,
                        } => {
                            let envelope = value.as_object().ok_or_else(arguments)?;
                            match envelope.get(present_key).and_then(Value::as_bool) {
                                Some(false)
                                    if envelope.len() == 2
                                        && envelope.get(value_key) == Some(&Value::Null) =>
                                {
                                    None
                                }
                                Some(true) if envelope.len() == 2 => {
                                    Some(envelope.get(value_key).ok_or_else(arguments)?.clone())
                                }
                                _ => return Err(arguments()),
                            }
                        }
                    };
                    if let Some(value) = restored {
                        result.insert(mapping.canonical_name.clone(), value);
                    }
                }
                Ok(result)
            }
        }
    }
}
fn validate_codec(
    canonical: &Value,
    wire: &Value,
    plan: &ArgumentDecodePlan,
) -> Result<(), ContractError> {
    let canonical = canonical
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("provider_tool.canonical_properties"))?;
    let wire = wire
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("provider_tool.wire_properties"))?;
    match plan {
        ArgumentDecodePlan::Identity {} if canonical.keys().eq(wire.keys()) => Ok(()),
        ArgumentDecodePlan::JsonObjectText { wire_name }
            if wire.len() == 1
                && wire.get(wire_name).and_then(|value| value.get("type"))
                    == Some(&json!("string")) =>
        {
            Ok(())
        }
        ArgumentDecodePlan::Fields { fields } => {
            let mut from = BTreeSet::new();
            let mut to = BTreeSet::new();
            for field in fields {
                if !wire.contains_key(&field.wire_name)
                    || !canonical.contains_key(&field.canonical_name)
                    || !from.insert(&field.wire_name)
                    || !to.insert(&field.canonical_name)
                {
                    return Err(invalid("provider_tool.codec_mapping"));
                }
                if let ArgumentValueEncoding::Presence {
                    present_key,
                    value_key,
                } = &field.encoding
                {
                    if present_key.is_empty() || value_key.is_empty() || present_key == value_key {
                        return Err(invalid("provider_tool.presence_keys"));
                    }
                }
            }
            if from.len() != wire.len() || to.len() != canonical.len() {
                return Err(invalid("provider_tool.codec_coverage"));
            }
            Ok(())
        }
        _ => Err(invalid("provider_tool.codec_mapping")),
    }
}
fn collect_enforcement(
    canonical: &Value,
    wire: &Value,
    pointer: &str,
    identity: bool,
    text: bool,
    output: &mut Vec<ToolConstraintEnforcement>,
) {
    output.push(ToolConstraintEnforcement {
        canonical_pointer: pointer.into(),
        provider_native: identity && wire.pointer(pointer) == Some(canonical),
        context_text: text,
        core: true,
    });
    let Some(map) = canonical.as_object() else {
        return;
    };
    for (key, value) in map {
        if matches!(
            key.as_str(),
            "title"
                | "description"
                | "default"
                | "examples"
                | "$comment"
                | "$schema"
                | "$id"
                | "deprecated"
                | "readOnly"
                | "writeOnly"
        ) {
            continue;
        }
        let path = format!("{pointer}/{}", key.replace('~', "~0").replace('/', "~1"));
        match key.as_str() {
            "properties" | "$defs" | "definitions" | "dependentSchemas" | "patternProperties" => {
                if let Some(children) = value.as_object() {
                    for (name, child) in children {
                        collect_enforcement(
                            child,
                            wire,
                            &format!("{path}/{}", name.replace('~', "~0").replace('/', "~1")),
                            identity,
                            text,
                            output,
                        );
                    }
                }
            }
            "allOf" | "anyOf" | "oneOf" | "prefixItems" => {
                if let Some(children) = value.as_array() {
                    for (index, child) in children.iter().enumerate() {
                        collect_enforcement(
                            child,
                            wire,
                            &format!("{path}/{index}"),
                            identity,
                            text,
                            output,
                        );
                    }
                }
                output.push(ToolConstraintEnforcement {
                    canonical_pointer: path.clone(),
                    provider_native: identity && wire.pointer(&path) == Some(value),
                    context_text: text,
                    core: true,
                });
            }
            "items"
            | "additionalProperties"
            | "unevaluatedProperties"
            | "unevaluatedItems"
            | "contains"
            | "not"
            | "if"
            | "then"
            | "else"
            | "propertyNames" => collect_enforcement(value, wire, &path, identity, text, output),
            _ => output.push(ToolConstraintEnforcement {
                canonical_pointer: path.clone(),
                provider_native: identity && wire.pointer(&path) == Some(value),
                context_text: text,
                core: true,
            }),
        }
    }
}

fn bounded(value: &impl Serialize, max: usize) -> Result<(), ContractError> {
    if serde_json::to_vec(value)
        .map_err(|_| invalid("provider_tool.json"))?
        .len()
        > max
    {
        return Err(invalid("provider_tool.size"));
    }
    Ok(())
}
fn check_limits(limits: ProviderToolSchemaLimits) -> Result<(), ContractError> {
    if limits.max_schema_depth == 0
        || limits.max_schema_depth > 128
        || limits.max_schema_bytes == 0
        || limits.max_contract_bytes == 0
        || limits.max_argument_bytes == 0
    {
        return Err(invalid("provider_tool.limits"));
    }
    Ok(())
}
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn invalid(path: &str) -> ContractError {
    ContractError::new(ErrorCode::UnsupportedInputProjection, path)
}
fn arguments() -> ContractError {
    ContractError::new(ErrorCode::InvalidArguments, "provider_tool.arguments")
}

fn depth_bound(schema: &Value, max: usize) -> Result<(), ContractError> {
    let mut pending = vec![(schema, 0)];
    while let Some((value, depth)) = pending.pop() {
        if depth > max {
            return Err(invalid("provider_tool.depth"));
        }
        match value {
            Value::Object(map) => pending.extend(map.values().map(|value| (value, depth + 1))),
            Value::Array(array) => pending.extend(array.iter().map(|value| (value, depth + 1))),
            _ => {}
        }
    }
    Ok(())
}

// The legacy Value parser must remain unchanged for old digests. This new codec
// refuses values it cannot represent, rather than silently rounding model input.
fn numbers_preserved(raw: &serde_json::value::RawValue, parsed: &Value) -> bool {
    use serde_json::value::RawValue;
    match raw.get().as_bytes()[0] {
        b'{' => {
            let Ok(object) =
                serde_json::from_str::<std::collections::BTreeMap<String, &RawValue>>(raw.get())
            else {
                return false;
            };
            object.into_iter().all(|(key, raw)| {
                parsed
                    .get(&key)
                    .is_some_and(|value| numbers_preserved(raw, value))
            })
        }
        b'[' => {
            let Ok(array) = serde_json::from_str::<Vec<&RawValue>>(raw.get()) else {
                return false;
            };
            array.into_iter().enumerate().all(|(index, raw)| {
                parsed
                    .get(index)
                    .is_some_and(|value| numbers_preserved(raw, value))
            })
        }
        b'-' | b'0'..=b'9' => parsed.as_number().is_some_and(|number| {
            normalized_decimal(raw.get())
                .is_some_and(|original| Some(original) == normalized_decimal(&number.to_string()))
        }),
        _ => true,
    }
}
fn normalized_decimal(text: &str) -> Option<(bool, String, i128)> {
    let negative = text.starts_with('-');
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let (mantissa, exponent) = unsigned.split_once(['e', 'E']).unwrap_or((unsigned, "0"));
    let fraction = mantissa
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Some((false, "0".into(), 0));
    }
    let trimmed = digits.trim_end_matches('0');
    let exponent = exponent
        .parse::<i128>()
        .ok()?
        .checked_sub(fraction as i128)?
        .checked_add((digits.len() - trimmed.len()) as i128)?;
    Some((negative, trimmed.into(), exponent))
}

/// Parse model-owned provider arguments without silently rounding number tokens.
pub fn parse_provider_arguments(raw: &str, max_bytes: usize) -> Result<JsonObject, ContractError> {
    Ok(parse_provider_value(raw, max_bytes)?
        .as_object()
        .ok_or_else(arguments)?
        .clone()
        .into_iter()
        .collect())
}
fn parse_provider_value(raw: &str, max_bytes: usize) -> Result<Value, ContractError> {
    if raw.len() > max_bytes {
        return Err(arguments());
    }
    let value = parse_json(raw).map_err(|_| arguments())?;
    let original: &serde_json::value::RawValue =
        serde_json::from_str(raw).map_err(|_| arguments())?;
    if !numbers_preserved(original, &value) {
        return Err(ContractError::new(
            ErrorCode::InvalidArguments,
            "provider_tool.numeric_precision",
        ));
    }
    Ok(value)
}
