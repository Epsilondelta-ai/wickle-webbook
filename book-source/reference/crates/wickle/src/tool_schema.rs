use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroU64,
    sync::Arc,
};

use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value};

use crate::{
    ContractError, ErrorCode, Id, JsonDigest, JsonObject, ModelTool, VersionedRef,
    canonical_digest, parse_json,
    serialization::{data_digest, optional},
};

/// Version of the deterministic tool-input projection contract.
pub const TOOL_SCHEMA_COMPILER_VERSION: &str = "wickle.tool-input-compiler.v1";

/// Trusted effect classification; it does not replace current execution policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolSideEffect {
    /// The reviewed handler performs no external business writes.
    ReadOnly,
    /// The handler can change external business state.
    Write,
    /// Effects are not yet classified; no read-only assumptions are made.
    #[default]
    Unknown,
}

/// Reviewed concurrency capability, further restricted by runtime policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolConcurrency {
    /// Execute one call at a time.
    #[default]
    Serial,
    /// Read-only implementation reviewed for parallel calls.
    ParallelRead,
}

/// Declared retry safety; retry attempts still require runtime permission and budget.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRetryPolicy {
    /// No automatic retry is declared safe.
    #[default]
    Never,
    /// Reviewed read-only operation can be repeated.
    ReadOnly,
    /// The implementation honors the same external idempotency key on retry.
    Idempotent,
}

/// Full trusted tool contract. Serialization is for Host configuration/protected
/// storage; use CompiledTool::to_model_tool to expose only model-owned schema.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolDescriptor {
    /// Exact registered handler identity and version.
    pub tool: VersionedRef,
    /// Portable model-facing ASCII name (letters, digits, underscore or hyphen).
    pub name: Id,
    /// Public description written for the model, without hidden input details.
    pub description: String,
    /// Complete handler input schema, including system-owned parameters.
    pub input_schema: Value,
    /// Explicit model-owned top-level property names. Omission is an error; [] is valid.
    pub agent_parameters: Vec<String>,
    /// Hidden parameter -> exact registered system key. Omission uses the same name.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub system_bindings: Option<BTreeMap<String, Id>>,
    /// Complete returned-value schema, validated later by the executor boundary.
    pub output_schema: Value,
    /// Trusted declared effect category.
    #[serde(default)]
    pub side_effect: ToolSideEffect,
    /// Serial unless explicitly reviewed otherwise.
    #[serde(default)]
    pub concurrency: ToolConcurrency,
    /// Declared safety, not an instruction to retry now.
    #[serde(default)]
    pub retry: ToolRetryPolicy,
    /// Whether the executor can check the status of an uncertain external effect.
    #[serde(default)]
    pub reconcile: bool,
    /// Finite maximum output payload size, enforced by the later executor.
    pub max_output_bytes: NonZeroU64,
}

impl ToolDescriptor {
    /// Decode without accepting duplicate keys or inventing an exposure allowlist.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        let value = parse_json(input).map_err(|_| invalid("tool_descriptor"))?;
        serde_json::from_value(value).map_err(|_| invalid("tool_descriptor"))
    }
}

impl fmt::Debug for ToolDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolDescriptor")
            .field("tool", &self.tool)
            .field("name", &self.name)
            .field("side_effect", &self.side_effect)
            .finish_non_exhaustive()
    }
}

/// Declared supplier of a system value; it contains no executable function or value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SystemInputSource {
    /// Value comes from the owned map supplied for this run.
    Run {},
    /// Value will come from a trusted registered read-only resolver.
    Resolver {
        /// Exact resolver implementation reference.
        resolver_ref: VersionedRef,
    },
}

impl Default for SystemInputSource {
    fn default() -> Self {
        Self::Run {}
    }
}

/// Schema and source for one exact system-input key. It does not hold the value.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemInputDefinition {
    /// Exact map key. Dots and slashes are literal characters, never paths.
    pub key: Id,
    /// Definition revision pinned into the compiled contract.
    pub version: Id,
    /// Schema checked against a supplied value by the later binding boundary.
    pub value_schema: Value,
    /// Run map by default, or one explicitly registered resolver.
    #[serde(default)]
    pub source: SystemInputSource,
}

impl fmt::Debug for SystemInputDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SystemInputDefinition")
            .field("key", &self.key)
            .field("version", &self.version)
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

/// Immutable metadata registry. Creation checks definitions, never value presence
/// or business ownership, and never calls a resolver or generates missing IDs.
#[derive(Clone, Default)]
pub struct SystemInputRegistry {
    definitions: BTreeMap<Id, SystemInputDefinition>,
}

impl SystemInputRegistry {
    /// Validate and own one unambiguous definition per exact key.
    pub fn new(definitions: Vec<SystemInputDefinition>) -> Result<Self, ContractError> {
        let mut registered = BTreeMap::new();
        for definition in definitions {
            if registered.contains_key(&definition.key) {
                return Err(invalid("system_input_registry.key"));
            }
            check_schema_document(&definition.value_schema)?;
            compile_validator(&definition.value_schema)?;
            registered.insert(definition.key.clone(), definition);
        }
        Ok(Self {
            definitions: registered,
        })
    }
    /// Read metadata by exact key; no string/path evaluation is performed.
    pub fn get(&self, key: &Id) -> Option<&SystemInputDefinition> {
        self.definitions.get(key)
    }
    /// Read the frozen definition map without exposing mutable registration state.
    pub fn definitions(&self) -> &BTreeMap<Id, SystemInputDefinition> {
        &self.definitions
    }
}

impl fmt::Debug for SystemInputRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SystemInputRegistry")
            .field("definition_count", &self.definitions.len())
            .finish()
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompiledToolData {
    compiler_version: String,
    descriptor: ToolDescriptor,
    model_input_schema: Value,
    system_bindings: BTreeMap<String, SystemInputDefinition>,
    descriptor_digest: JsonDigest,
    model_schema_digest: JsonDigest,
    bindings_digest: JsonDigest,
    digest: JsonDigest,
}

/// Frozen input split, hashes, and validators. Mutable definitions cannot change
/// this contract after compilation. Serialization is for protected storage only;
/// restoration requires recompilation against the current trusted registry and an
/// expected digest from trusted assembly metadata, not from the cached JSON itself.
#[derive(Clone)]
pub struct CompiledTool {
    data: CompiledToolData,
    model_validator: Arc<jsonschema::Validator>,
    execution_validator: Arc<jsonschema::Validator>,
}

impl Serialize for CompiledTool {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.data.serialize(serializer)
    }
}

impl fmt::Debug for CompiledTool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompiledTool")
            .field("tool", &self.data.descriptor.tool)
            .field("digest", &self.data.digest)
            .finish_non_exhaustive()
    }
}

impl CompiledTool {
    /// Full trusted descriptor; never substitute it for the model-facing view.
    pub fn descriptor(&self) -> &ToolDescriptor {
        &self.data.descriptor
    }
    /// Full handler input schema for the later binding/execution boundary.
    pub fn input_schema(&self) -> &Value {
        &self.data.descriptor.input_schema
    }
    /// Read-only model projection, excluding hidden fields and unreachable definitions.
    pub fn model_input_schema(&self) -> &Value {
        &self.data.model_input_schema
    }
    /// Exact hidden parameter -> frozen key/version/source/schema definitions.
    pub fn system_bindings(&self) -> &BTreeMap<String, SystemInputDefinition> {
        &self.data.system_bindings
    }
    /// Exact compiler contract used to create the projection.
    pub fn compiler_version(&self) -> &str {
        &self.data.compiler_version
    }
    /// Identity of the complete original tool descriptor.
    pub fn descriptor_digest(&self) -> &JsonDigest {
        &self.data.descriptor_digest
    }
    /// Identity of the model-visible schema.
    pub fn model_schema_digest(&self) -> &JsonDigest {
        &self.data.model_schema_digest
    }
    /// Identity of only the system definitions used by this tool.
    pub fn bindings_digest(&self) -> &JsonDigest {
        &self.data.bindings_digest
    }
    /// Identity of compiler version, original descriptor, projection, and selected bindings.
    pub fn digest(&self) -> &JsonDigest {
        &self.data.digest
    }
    /// Create the only tool view intended for inclusion in ModelRequest.
    pub fn to_model_tool(&self) -> ModelTool {
        ModelTool {
            name: self.data.descriptor.name.clone(),
            description: self.data.descriptor.description.clone(),
            model_input_schema: self.data.model_input_schema.clone(),
        }
    }
    /// Validate model-owned arguments without applying defaults or system values.
    pub fn validate_model_inputs(&self, input: &JsonObject) -> Result<(), ContractError> {
        if !self
            .model_validator
            .is_valid(&Value::Object(input.clone().into_iter().collect()))
        {
            return Err(ContractError::new(
                ErrorCode::InvalidArguments,
                "tool.model_inputs",
            ));
        }
        Ok(())
    }
    /// Apply declared top-level model defaults before canonical validation.
    /// Explicit null and present values are never replaced; system fields are absent.
    pub fn normalize_model_inputs(&self, input: &JsonObject) -> Result<JsonObject, ContractError> {
        let normalized = apply_model_defaults(self.model_input_schema(), input)?;
        self.validate_model_inputs(&normalized)?;
        Ok(normalized)
    }
    /// Validate already-bound execution arguments. This does not establish target
    /// existence/ownership or apply defaults; those remain later binding/policy work.
    pub fn validate_execution_inputs(&self, input: &JsonObject) -> Result<(), ContractError> {
        if !self
            .execution_validator
            .is_valid(&Value::Object(input.clone().into_iter().collect()))
        {
            return Err(ContractError::new(
                ErrorCode::InvalidArguments,
                "tool.execution_inputs",
            ));
        }
        Ok(())
    }
}

/// Compiler for explicit top-level input ownership. It performs no runtime binding,
/// business I/O, default application, or generation of UUIDs/foreign keys.
#[derive(Debug, Clone, Copy, Default)]
pub struct SchemaCompiler;

impl SchemaCompiler {
    /// Construct the compiler without loading runtime adapters or doing I/O.
    pub fn new() -> Self {
        Self
    }

    /// Validate a full tool definition and freeze its model/system input split.
    pub fn compile(
        &self,
        descriptor: ToolDescriptor,
        registry: &SystemInputRegistry,
    ) -> Result<CompiledTool, ContractError> {
        if !valid_name(descriptor.name.as_str())
            || (descriptor.concurrency == ToolConcurrency::ParallelRead
                && descriptor.side_effect != ToolSideEffect::ReadOnly)
            || (descriptor.retry == ToolRetryPolicy::ReadOnly
                && descriptor.side_effect != ToolSideEffect::ReadOnly)
        {
            return Err(invalid("tool_descriptor.execution"));
        }
        let root = descriptor
            .input_schema
            .as_object()
            .ok_or_else(|| invalid("input_schema"))?;
        let properties = root
            .get("properties")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("input_schema.properties"))?;
        let required = root
            .get("required")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("input_schema.required"))?;
        if root.get("type") != Some(&Value::String("object".into()))
            || root.get("additionalProperties") != Some(&Value::Bool(false))
        {
            return Err(unsupported("input_schema.object_boundary"));
        }
        let mut required_names = BTreeSet::new();
        for name in required {
            let name = name
                .as_str()
                .ok_or_else(|| invalid("input_schema.required"))?;
            if !properties.contains_key(name) || !required_names.insert(name) {
                return Err(invalid("input_schema.required"));
            }
        }
        let mut exposed = BTreeSet::new();
        for name in &descriptor.agent_parameters {
            if name.trim().is_empty()
                || !properties.contains_key(name)
                || !exposed.insert(name.as_str())
            {
                return Err(invalid("agent_parameters"));
            }
        }
        let mixed = exposed.len() != properties.len();
        check_schema_document(&descriptor.input_schema)?;
        check_schema_document(&descriptor.output_schema)?;
        let execution_validator = compile_validator(&descriptor.input_schema)?;
        compile_validator(&descriptor.output_schema)?;
        let mut bindings = BTreeMap::new();
        if let Some(explicit) = &descriptor.system_bindings {
            if explicit
                .keys()
                .any(|name| !properties.contains_key(name) || exposed.contains(name.as_str()))
            {
                return Err(invalid("system_bindings"));
            }
        }
        for name in properties
            .keys()
            .filter(|name| !exposed.contains(name.as_str()))
        {
            let key = descriptor
                .system_bindings
                .as_ref()
                .and_then(|bindings| bindings.get(name))
                .cloned()
                .map(Ok)
                .unwrap_or_else(|| Id::new(name.clone()).map_err(|_| invalid("system_bindings")))?;
            let definition = registry
                .get(&key)
                .ok_or_else(|| invalid("system_bindings.key"))?;
            bindings.insert(name.clone(), definition.clone());
        }
        let mut projected = Map::new();
        for (key, value) in root {
            match key.as_str() {
                "type" | "additionalProperties" | "$schema" => {
                    projected.insert(key.clone(), value.clone());
                }
                "properties" | "required" | "$defs" | "definitions" => {}
                "patternProperties" => return Err(unsupported("input_schema.patternProperties")),
                key if root_constraint(key) && !mixed => {
                    projected.insert(key.into(), value.clone());
                }
                // Root annotations can contain full execution examples, so they are
                // never copied. Selected field annotations remain explicitly visible.
                _ => {}
            }
        }
        if mixed {
            let conditions = project_model_conjunction(
                &descriptor.input_schema,
                &exposed,
                &descriptor.input_schema,
                0,
            )?;
            if let Value::Object(conditions) = conditions {
                for (key, value) in conditions {
                    if root_constraint(&key) {
                        projected.insert(key, value);
                    }
                }
            }
        }
        projected.insert(
            "properties".into(),
            Value::Object(
                properties
                    .iter()
                    .filter(|(name, _)| exposed.contains(name.as_str()))
                    .map(|(name, schema)| (name.clone(), schema.clone()))
                    .collect(),
            ),
        );
        projected.insert(
            "required".into(),
            Value::Array(
                required
                    .iter()
                    .filter(|name| exposed.contains(name.as_str().expect("checked required")))
                    .cloned()
                    .collect(),
            ),
        );
        let mut model_schema = Value::Object(projected);
        retain_reachable_definitions(&descriptor.input_schema, &mut model_schema)?;
        let model_validator = compile_validator(&model_schema)?;
        let descriptor_digest = data_digest(&descriptor);
        let model_schema_digest = canonical_digest(&model_schema);
        let bindings_digest = data_digest(&bindings);
        let digest = data_digest(&(
            TOOL_SCHEMA_COMPILER_VERSION,
            &descriptor_digest,
            &model_schema_digest,
            &bindings_digest,
        ));
        Ok(CompiledTool {
            data: CompiledToolData {
                compiler_version: TOOL_SCHEMA_COMPILER_VERSION.into(),
                descriptor,
                model_input_schema: model_schema,
                system_bindings: bindings,
                descriptor_digest,
                model_schema_digest,
                bindings_digest,
                digest,
            },
            model_validator: Arc::new(model_validator),
            execution_validator: Arc::new(execution_validator),
        })
    }

    /// Recompile protected cached data with a trusted registry and expected digest.
    /// Cached projection, bindings and hashes are compared; none is trusted as executable state.
    pub fn restore(
        &self,
        input: &str,
        registry: &SystemInputRegistry,
        expected_digest: &JsonDigest,
    ) -> Result<CompiledTool, ContractError> {
        let saved: CompiledToolData =
            serde_json::from_value(parse_json(input).map_err(|_| invalid("compiled_tool"))?)
                .map_err(|_| invalid("compiled_tool"))?;
        if saved.compiler_version != TOOL_SCHEMA_COMPILER_VERSION
            || &saved.digest != expected_digest
        {
            return Err(invalid("compiled_tool.digest"));
        }
        let compiled = self.compile(saved.descriptor.clone(), registry)?;
        if compiled.data != saved || compiled.digest() != expected_digest {
            return Err(invalid("compiled_tool.digest"));
        }
        Ok(compiled)
    }
}

fn invalid(path: &str) -> ContractError {
    ContractError::new(ErrorCode::InvalidToolInputContract, path)
}
fn unsupported(path: &str) -> ContractError {
    ContractError::new(ErrorCode::UnsupportedInputProjection, path)
}
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn root_constraint(key: &str) -> bool {
    matches!(
        key,
        "allOf"
            | "anyOf"
            | "oneOf"
            | "not"
            | "if"
            | "then"
            | "else"
            | "dependentRequired"
            | "dependentSchemas"
            | "minProperties"
            | "maxProperties"
            | "propertyNames"
            | "unevaluatedProperties"
            | "const"
            | "enum"
            | "$ref"
            | "minimum"
            | "maximum"
            | "exclusiveMinimum"
            | "exclusiveMaximum"
            | "multipleOf"
            | "minLength"
            | "maxLength"
            | "pattern"
            | "format"
            | "items"
            | "prefixItems"
            | "contains"
            | "minContains"
            | "maxContains"
            | "minItems"
            | "maxItems"
            | "uniqueItems"
            | "additionalItems"
            | "unevaluatedItems"
            | "contentSchema"
    )
}

type DefinitionKey = (String, String);

fn local_definition(reference: &str) -> Result<DefinitionKey, ContractError> {
    let segments: Vec<_> = reference.split('/').collect();
    if segments.len() != 3
        || segments[0] != "#"
        || !matches!(segments[1], "$defs" | "definitions")
        || segments[2].is_empty()
        || segments[2].contains('%')
    {
        return Err(unsupported("schema.$ref"));
    }
    let mut name = String::new();
    let mut chars = segments[2].chars();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            match chars.next() {
                Some('0') => name.push('~'),
                Some('1') => name.push('/'),
                _ => return Err(unsupported("schema.$ref")),
            }
        } else {
            name.push(ch);
        }
    }
    Ok((segments[1].into(), name))
}

fn definition<'a>(document: &'a Value, key: &DefinitionKey) -> Result<&'a Value, ContractError> {
    document
        .get(&key.0)
        .and_then(Value::as_object)
        .and_then(|definitions| definitions.get(&key.1))
        .ok_or_else(|| invalid("schema.$ref"))
}

/// Visit schema-bearing positions only. JSON values under examples/default/const
/// are data and are never interpreted as references or ownership declarations.
fn inspect_schema(
    schema: &Value,
    document: &Value,
    root: bool,
    references: &mut BTreeSet<DefinitionKey>,
) -> Result<(), ContractError> {
    let Some(map) = schema.as_object() else {
        return if schema.is_boolean() {
            Ok(())
        } else {
            Err(invalid("schema"))
        };
    };
    for key in [
        "$id",
        "$anchor",
        "$dynamicAnchor",
        "$dynamicRef",
        "$recursiveRef",
        "$recursiveAnchor",
        "$vocabulary",
    ] {
        if map.contains_key(key) {
            return Err(unsupported("schema.reference_scope"));
        }
    }
    if let Some(version) = map.get("$schema") {
        if version != "https://json-schema.org/draft/2020-12/schema" {
            return Err(unsupported("schema.version"));
        }
    }
    if let Some(reference) = map.get("$ref") {
        let key = local_definition(reference.as_str().ok_or_else(|| invalid("schema.$ref"))?)?;
        definition(document, &key)?;
        references.insert(key);
    }
    if !root && (map.contains_key("$defs") || map.contains_key("definitions")) {
        return Err(unsupported("schema.nested_definitions"));
    }
    for key in [
        "$defs",
        "definitions",
        "properties",
        "patternProperties",
        "dependentSchemas",
    ] {
        if let Some(values) = map.get(key) {
            let values = values.as_object().ok_or_else(|| invalid("schema"))?;
            for value in values.values() {
                inspect_schema(value, document, false, references)?;
            }
        }
    }
    for key in [
        "items",
        "additionalProperties",
        "unevaluatedProperties",
        "unevaluatedItems",
        "contains",
        "not",
        "if",
        "then",
        "else",
        "propertyNames",
        "contentSchema",
        "additionalItems",
    ] {
        if let Some(value) = map.get(key) {
            inspect_schema(value, document, false, references)?;
        }
    }
    for key in ["allOf", "anyOf", "oneOf", "prefixItems"] {
        if let Some(values) = map.get(key) {
            let values = values.as_array().ok_or_else(|| invalid("schema"))?;
            for value in values {
                inspect_schema(value, document, false, references)?;
            }
        }
    }
    Ok(())
}

fn check_schema_document(document: &Value) -> Result<(), ContractError> {
    inspect_schema(document, document, true, &mut BTreeSet::new())
}

fn retain_reachable_definitions(
    document: &Value,
    model_schema: &mut Value,
) -> Result<(), ContractError> {
    let mut pending = BTreeSet::new();
    inspect_schema(model_schema, document, true, &mut pending)?;
    let mut selected = BTreeSet::new();
    while let Some(key) = pending.pop_first() {
        if !selected.insert(key.clone()) {
            continue;
        }
        let value = definition(document, &key)?;
        inspect_schema(value, document, false, &mut pending)?;
    }
    let root = model_schema
        .as_object_mut()
        .expect("projected object schema");
    for (container, name) in selected {
        root.entry(container.clone())
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .expect("definition map")
            .insert(
                name.clone(),
                definition(document, &(container, name))?.clone(),
            );
    }
    Ok(())
}

struct NoSchemaRetrieval;
impl jsonschema::Retrieve for NoSchemaRetrieval {
    fn retrieve(
        &self,
        _: &jsonschema::Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err("external schema retrieval is disabled".into())
    }
}
pub(crate) fn compile_validator(schema: &Value) -> Result<jsonschema::Validator, ContractError> {
    jsonschema::draft202012::options()
        .should_validate_formats(true)
        .with_retriever(NoSchemaRetrieval)
        .build(schema)
        .map_err(|_| invalid("schema"))
}

// A root predicate is model-only only if hidden properties cannot change its truth.
// Conditions that depend on system fields stay in the full execution validator.
fn model_only_condition(
    node: &Value,
    exposed: &BTreeSet<&str>,
    document: &Value,
    depth: usize,
) -> Option<Value> {
    if depth > 64 {
        return None;
    }
    if node.is_boolean() {
        return Some(node.clone());
    }
    let map = node.as_object()?;
    let mut result = Map::new();
    for (key, value) in map {
        match key.as_str() {
            "properties" => {
                let props = value.as_object()?;
                if props.keys().any(|name| !exposed.contains(name.as_str())) {
                    return None;
                }
                result.insert(key.clone(), value.clone());
            }
            "required" => {
                if value
                    .as_array()?
                    .iter()
                    .any(|name| name.as_str().is_none_or(|name| !exposed.contains(name)))
                {
                    return None;
                }
                result.insert(key.clone(), value.clone());
            }
            "allOf" | "anyOf" | "oneOf" => {
                let parts: Option<Vec<_>> = value
                    .as_array()?
                    .iter()
                    .map(|part| model_only_condition(part, exposed, document, depth + 1))
                    .collect();
                let parts = parts?;
                if key == "allOf" {
                    result
                        .entry(key.clone())
                        .or_insert_with(|| Value::Array(Vec::new()))
                        .as_array_mut()?
                        .extend(parts);
                } else {
                    result.insert(key.clone(), Value::Array(parts));
                }
            }
            "not" | "if" | "then" | "else" => {
                result.insert(
                    key.clone(),
                    model_only_condition(value, exposed, document, depth + 1)?,
                );
            }
            "dependentRequired" => {
                let dependencies = value.as_object()?;
                for (name, required) in dependencies {
                    if !exposed.contains(name.as_str())
                        || required
                            .as_array()?
                            .iter()
                            .any(|name| name.as_str().is_none_or(|name| !exposed.contains(name)))
                    {
                        return None;
                    }
                }
                result.insert(key.clone(), value.clone());
            }
            "dependentSchemas" => {
                let mut dependencies = Map::new();
                for (name, schema) in value.as_object()? {
                    if !exposed.contains(name.as_str()) {
                        return None;
                    }
                    dependencies.insert(
                        name.clone(),
                        model_only_condition(schema, exposed, document, depth + 1)?,
                    );
                }
                result.insert(key.clone(), Value::Object(dependencies));
            }
            "$ref" => {
                let reference = value.as_str()?.strip_prefix('#')?;
                let resolved = model_only_condition(
                    document.pointer(reference)?,
                    exposed,
                    document,
                    depth + 1,
                )?;
                result
                    .entry("allOf")
                    .or_insert_with(|| Value::Array(Vec::new()))
                    .as_array_mut()?
                    .push(resolved);
            }
            "type" => {
                result.insert(key.clone(), value.clone());
            }
            key if root_constraint(key)
                || matches!(key, "additionalProperties" | "patternProperties") =>
            {
                return None;
            }
            // Root annotations may describe full execution inputs, so do not expose them.
            _ => {}
        }
    }
    Some(Value::Object(result))
}

// Keep necessary model-owned restrictions in a conjunction. This projection may
// weaken a whole execution predicate, so it must never be used as an if/not/XOR test.
fn project_model_conjunction(
    node: &Value,
    exposed: &BTreeSet<&str>,
    document: &Value,
    depth: usize,
) -> Result<Value, ContractError> {
    if depth > 64 {
        return Err(unsupported("input_schema.projection_depth"));
    }
    if node.is_boolean() {
        return Ok(node.clone());
    }
    let map = node
        .as_object()
        .ok_or_else(|| invalid("input_schema.condition"))?;
    let mut result = Map::new();
    let mut conjuncts = Vec::new();
    for (key, value) in map {
        match key.as_str() {
            "type" => {
                result.insert(key.clone(), value.clone());
            }
            "properties" => {
                let selected = value
                    .as_object()
                    .ok_or_else(|| invalid("input_schema.properties"))?
                    .iter()
                    .filter(|(name, _)| exposed.contains(name.as_str()))
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect();
                result.insert(key.clone(), Value::Object(selected));
            }
            "required" => {
                let selected = value
                    .as_array()
                    .ok_or_else(|| invalid("input_schema.required"))?
                    .iter()
                    .filter(|name| name.as_str().is_some_and(|name| exposed.contains(name)))
                    .cloned()
                    .collect();
                result.insert(key.clone(), Value::Array(selected));
            }
            "allOf" => {
                for child in value
                    .as_array()
                    .ok_or_else(|| invalid("input_schema.allOf"))?
                {
                    conjuncts.push(project_model_conjunction(
                        child,
                        exposed,
                        document,
                        depth + 1,
                    )?);
                }
            }
            "$ref" => {
                let pointer = value
                    .as_str()
                    .and_then(|reference| reference.strip_prefix('#'))
                    .ok_or_else(|| unsupported("input_schema.reference"))?;
                let child = document
                    .pointer(pointer)
                    .ok_or_else(|| unsupported("input_schema.reference"))?;
                conjuncts.push(project_model_conjunction(
                    child,
                    exposed,
                    document,
                    depth + 1,
                )?);
            }
            "dependentRequired" => {
                let mut dependencies = Map::new();
                for (name, required) in value
                    .as_object()
                    .ok_or_else(|| invalid("input_schema.dependentRequired"))?
                {
                    if exposed.contains(name.as_str()) {
                        let kept = required
                            .as_array()
                            .ok_or_else(|| invalid("input_schema.dependentRequired"))?
                            .iter()
                            .filter(|name| name.as_str().is_some_and(|name| exposed.contains(name)))
                            .cloned()
                            .collect();
                        dependencies.insert(name.clone(), Value::Array(kept));
                    }
                }
                result.insert(key.clone(), Value::Object(dependencies));
            }
            "dependentSchemas" => {
                let mut dependencies = Map::new();
                for (name, condition) in value
                    .as_object()
                    .ok_or_else(|| invalid("input_schema.dependentSchemas"))?
                {
                    if exposed.contains(name.as_str()) {
                        dependencies.insert(
                            name.clone(),
                            project_model_conjunction(condition, exposed, document, depth + 1)?,
                        );
                    }
                }
                result.insert(key.clone(), Value::Object(dependencies));
            }
            "if" | "then" | "else" => {}
            key if root_constraint(key) => {
                let wrapper = Value::Object(Map::from_iter([(key.into(), value.clone())]));
                if let Some(Value::Object(mut exact)) =
                    model_only_condition(&wrapper, exposed, document, depth + 1)
                {
                    if let Some(value) = exact.remove(key) {
                        result.insert(key.into(), value);
                    }
                }
            }
            _ => {}
        }
    }
    if let Some(condition) = map
        .get("if")
        .and_then(|condition| model_only_condition(condition, exposed, document, depth + 1))
    {
        result.insert("if".into(), condition);
        for branch in ["then", "else"] {
            if let Some(schema) = map.get(branch) {
                result.insert(
                    branch.into(),
                    project_model_conjunction(schema, exposed, document, depth + 1)?,
                );
            }
        }
    }
    if !conjuncts.is_empty() {
        result.insert("allOf".into(), Value::Array(conjuncts));
    }
    Ok(Value::Object(result))
}

pub(crate) fn normalize_model_input_schema(
    schema: &Value,
    input: &JsonObject,
) -> Result<JsonObject, ContractError> {
    let normalized = apply_model_defaults(schema, input)?;
    if !compile_validator(schema)?
        .is_valid(&Value::Object(normalized.clone().into_iter().collect()))
    {
        return Err(ContractError::new(
            ErrorCode::InvalidArguments,
            "tool.model_inputs",
        ));
    }
    Ok(normalized)
}
fn apply_model_defaults(schema: &Value, input: &JsonObject) -> Result<JsonObject, ContractError> {
    let Some(properties) = schema.get("properties") else {
        return Ok(input.clone());
    };
    let properties = properties
        .as_object()
        .ok_or_else(|| invalid("model_defaults.properties"))?;
    let mut normalized = input.clone();
    for (name, property) in properties {
        if normalized.contains_key(name) {
            continue;
        }
        let mut current = property;
        let mut visited = BTreeSet::new();
        loop {
            if let Some(default) = current.get("default") {
                normalized.insert(name.clone(), default.clone());
                break;
            }
            let Some(reference) = current.get("$ref").and_then(Value::as_str) else {
                break;
            };
            if !visited.insert(reference) {
                break;
            }
            current = schema
                .pointer(
                    reference
                        .strip_prefix('#')
                        .ok_or_else(|| invalid("model_defaults.reference"))?,
                )
                .ok_or_else(|| invalid("model_defaults.reference"))?;
        }
    }
    Ok(normalized)
}
