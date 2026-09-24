# 09장 전체 Rust 구현과 테스트

[강의로](../09-schema.md) · [전체 변경 패치](../solutions/09-schema.patch)

기준 `80fb9dfeb63749cb634d3a5e9bae30a4ff6f866e`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle/src/error.rs`

```rust
use serde::{Deserialize, Serialize};

/// Stable categories for contract and profile validation failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorCode {
    /// Current policy or exact owner scope denies access.
    AccessDenied,
    /// The trusted policy failed or panicked; no permission was granted.
    PolicyUnavailable,
    /// The call's finite deadline elapsed.
    DeadlineExceeded,
    /// The current operation was cancelled.
    Cancelled,
    /// The Host has not supplied the required asynchronous runtime.
    RuntimeUnavailable,
    /// A configured call, repair, or recovery budget has no remaining capacity.
    BudgetExceeded,
    /// A required time reading or timer could not be obtained.
    ClockUnavailable,
    /// A monotonic reading regressed or a resumed UTC clock predates saved progress.
    ClockRegression,
    /// The Host identifier source could not generate an internal execution identifier.
    IdGenerationFailed,
    /// Input is not unambiguous, finite JSON.
    InvalidJson,
    /// Input does not match a data contract.
    InvalidContract,
    /// Tool exposure, binding metadata, or a registered input schema is inconsistent.
    InvalidToolInputContract,
    /// The compiler cannot safely project this input schema or reference form.
    UnsupportedInputProjection,
    /// Model-owned or assembled tool arguments do not satisfy their input contract.
    InvalidArguments,
    /// A supplied system value does not satisfy its registered input contract.
    SystemInputInvalid,
    /// The document format is not supported.
    UnsupportedSchemaVersion,
    /// A reference or binding is missing or inconsistent.
    InvalidReference,
    /// A required component or exact version is unavailable.
    ComponentUnavailable,
    /// A component uses an unsupported metadata contract.
    UnsupportedContractVersion,
    /// Selected components do not supply a required capability.
    CapabilityUnsupported,
    /// A configuration does not satisfy its registered schema.
    InvalidConfiguration,
    /// A registered schema is invalid or requires unsupported resolution.
    InvalidSchema,
    /// A profile differs from the profile pinned to an existing execution.
    ProfileMismatch,
    /// Stored data violates checkpoint invariants.
    InvalidSnapshot,
    /// The requested run, session, or protected record is absent in this exact scope.
    StateNotFound,
    /// An existing request identity was reused with different logical input.
    RequestConflict,
    /// The session already has a running or waiting run.
    SessionBusy,
    /// A proposed run identifier already belongs to another request in this scope.
    RunConflict,
    /// The compare-and-swap revision no longer matches saved state.
    RevisionConflict,
    /// Another unexpired execution lease already owns the run.
    LeaseBusy,
    /// The execution lease expired or no longer matches its owner and generation.
    LeaseLost,
    /// A candidate change violates immutable data or state-transition rules.
    InvalidTransition,
    /// An event has a duplicate identity, invalid sequence, or inconsistent references.
    InvalidEvent,
    /// A message has a duplicate identity, invalid sequence, or wrong owning run.
    InvalidMessage,
    /// Immutable record content or a requested reference digest conflicts.
    RecordConflict,
    /// Authoritative storage is unavailable; no successful commit is implied.
    PersistenceUnavailable,
}

/// A validation error that does not retain submitted values or credentials.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code:?} at {path}")]
pub struct ContractError {
    /// Machine-readable failure category.
    pub code: ErrorCode,
    /// Contract field or reference location, without submitted values.
    pub path: String,
}

impl ContractError {
    /// Construct an error using a safe contract location.
    pub fn new(code: ErrorCode, path: impl Into<String>) -> Self {
        Self {
            code,
            path: path.into(),
        }
    }
}
```

## `crates/wickle/src/lib.rs`

```rust
//! Wickle, an agent engine for Rust applications.
//!
//! Profiles, scoped metadata resolution, and versioned execution data contracts.
//! Model calls use scoped ports and persisted attempt accounting. Tool dispatch
//! and the agent driver are not implemented yet.
//!
//! Runtime objects stay in Host code. Only documented data contracts are
//! serialized; successful decoding does not authenticate a caller.
//!
//! Internal modules are not extension points; use the root exports.
//! ```compile_fail
//! use wickle::serialization::canonical_digest;
//! ```

mod budget;
mod clock;
mod context;
mod error;
mod message;
mod model;
mod model_execution;
mod model_protocol;
mod policy;
mod profile;
mod resolution;
mod run;
mod serialization;
mod state;
mod tool_schema;
mod views;

pub use budget::{AttemptReservation, ReservationKind, RunBudget, RunTiming};
pub use clock::{Clock, ClockReading, IdSource, RandomIdSource, SystemClock};
pub use model_protocol::{
    ModelCallContext, ModelContent, ModelEvent, ModelFinish, ModelMessage, ModelOutput, ModelPort,
    ModelPortBinding, ModelProtocolError, ModelProtocolErrorCode, ModelRequest, ModelResponse,
    ModelResponseLimits, ModelResponseMetadata, ModelRole, ModelTool, OpaqueContinuation,
    ProposedToolCall, ToolCallValidation, collect_model_response,
};
pub use policy::{
    ApprovalChallenge, Guarded, PolicyAction, PolicyContext, PolicyDecision, PolicyGate,
    PolicyPort, PolicyRequest, ToolPolicyInput,
};
pub use state::{
    AdmissionInput, AdmissionResult, CommitInput, EventPage, MAX_EVENT_PAGE_SIZE, MemoryStateStore,
    ProtectedRecord, RunLease, StateStore, StateStoreCapabilities, StoredRun,
};
pub use tool_schema::{
    CompiledTool, SchemaCompiler, SystemInputDefinition, SystemInputRegistry, SystemInputSource,
    TOOL_SCHEMA_COMPILER_VERSION, ToolConcurrency, ToolDescriptor, ToolRetryPolicy, ToolSideEffect,
};
pub use views::{ArtifactView, EventView, RunView};

pub use context::{
    ExecutionContext, ExecutionContextData, PortFuture, PortStream, Scope, SystemInputs,
};
pub use error::{ContractError, ErrorCode};
pub use message::{
    ArtifactRef, ContentBlock, EvidenceRef, Failure, InputContent, Message, MessageOrigin,
    MessageRole, RecordRef, ToolCall, ToolResult, ToolResultStatus, Visibility,
};
pub use model::{
    ApiContract, ModelAttemptState, ModelFailureKind, ModelInvocationRecord, ModelPurpose,
    ModelUsage, ResolvedModelRoute, RouteRequest, UsageMeasurement, VersionPolicy,
    VersionSemantics,
};
pub use model_execution::{
    ModelExchange, ModelExchangeOutcome, ModelRetryPolicy, StoredModelResponse,
};
pub use profile::{
    AdapterBindingRef, AgentProfile, CatalogHookRef, CatalogSourceRef, CatalogToolRef,
    CompletionPolicy, ConnectorBindingRef, ContextPolicy, ContextSourceBinding, ContextSourceRef,
    ContextTrigger, ExportRef, HookPosition, HookRef, InstructionAsset, InstructionText,
    Instructions, OutputContract, PROFILE_SCHEMA_VERSION, ProfileSchemaVersion, RunLimits,
    SkillRef, ToolBindingRef, VersionedRef,
};
pub use resolution::{
    ComponentKind, ComponentMetadata, ComponentRef, ExportKind, ExportMetadata, ProfileResolver,
    ProfileValidator, ResolvedComponent, ResolvedProfile,
};
pub use run::{
    ApprovalTarget, BudgetKind, BudgetUsage, CompletionBasis, EphemeralEvent, InputRequest,
    OutcomeResult, RUN_EVENT_SCHEMA_VERSION, RUN_SNAPSHOT_SCHEMA_VERSION, ResumeAction,
    ResumeCommand, RunEvent, RunEventPayload, RunEventSchemaVersion, RunOutcome, RunPhase,
    RunRequest, RunSnapshot, RunSnapshotSchemaVersion, RunStatus, RunTrigger, SessionSchemaVersion,
    SessionSnapshot, SourceExecutionState, SystemInputSnapshotRef, ToolCallState, ToolLedgerEntry,
    VerificationSummary, VerificationVerdict, WaitState, WaitTarget, admission_digest,
};
pub use serialization::{
    Id, JsonDigest, JsonObject, canonical_digest, canonical_digest_json, parse_json,
};
```

## `crates/wickle/src/tool_schema.rs`

```rust
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
                key if root_constraint(key) => {
                    if mixed {
                        return Err(unsupported("input_schema.cross_parameter_constraint"));
                    }
                    projected.insert(key.into(), value.clone());
                }
                // Root annotations can contain full execution examples, so they are
                // never copied. Selected field annotations remain explicitly visible.
                _ => {}
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
fn compile_validator(schema: &Value) -> Result<jsonschema::Validator, ContractError> {
    jsonschema::draft202012::options()
        .should_validate_formats(true)
        .with_retriever(NoSchemaRetrieval)
        .build(schema)
        .map_err(|_| invalid("schema"))
}
```

## `crates/wickle/tests/tool_schema.rs`

```rust
//! Explicit tool input ownership, schema projection, and pinned compilation.
use futures_util::stream;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use wickle::*;

const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}

fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn descriptor() -> ToolDescriptor {
    ToolDescriptor {
        tool: reference("search"),
        name: id("search"),
        description: "Search records".into(),
        input_schema: json!({
            "type":"object",
            "properties":{
                "query":{"type":"string","minLength":1},
                "limit":{"type":"integer","minimum":1,"default":10},
                "workspace_id":{"type":"string","format":"uuid"}
            },
            "required":["query","workspace_id"],
            "additionalProperties":false
        }),
        agent_parameters: vec!["query".into(), "limit".into()],
        system_bindings: None,
        output_schema: json!({"type":"array","items":{"type":"string"}}),
        side_effect: ToolSideEffect::ReadOnly,
        concurrency: ToolConcurrency::Serial,
        retry: ToolRetryPolicy::Never,
        reconcile: false,
        max_output_bytes: 4096.try_into().unwrap(),
    }
}

fn definition(key: &str, value_schema: Value) -> SystemInputDefinition {
    SystemInputDefinition {
        key: id(key),
        version: id("definition-1"),
        value_schema,
        source: SystemInputSource::Run {},
    }
}

fn registry() -> SystemInputRegistry {
    SystemInputRegistry::new(vec![definition(
        "workspace_id",
        json!({"type":"string","format":"uuid"}),
    )])
    .unwrap()
}

#[test]
fn projection_validates_model_and_execution_inputs_at_separate_boundaries() {
    let compiled = SchemaCompiler::new()
        .compile(descriptor(), &registry())
        .unwrap();
    assert_eq!(
        compiled.model_input_schema(),
        &json!({
            "type":"object",
            "properties":{
                "query":{"type":"string","minLength":1},
                "limit":{"type":"integer","minimum":1,"default":10}
            },
            "required":["query"],
            "additionalProperties":false
        })
    );
    let model_inputs = object(json!({"query":"recent results"}));
    compiled.validate_model_inputs(&model_inputs).unwrap();
    assert!(compiled.validate_execution_inputs(&model_inputs).is_err());
    let complete = object(json!({"query":"recent results","workspace_id":WORKSPACE}));
    compiled.validate_execution_inputs(&complete).unwrap();
    assert!(compiled.validate_model_inputs(&complete).is_err());
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"x","limit":0})))
            .is_err()
    );
    assert!(
        compiled
            .validate_execution_inputs(&object(json!({"query":"x","workspace_id":"not-a-uuid"})))
            .is_err()
    );
    assert!(
        compiled
            .validate_execution_inputs(&object(
                json!({"query":"x","workspace_id":WORKSPACE,"user_id":"extra"})
            ))
            .is_err()
    );
}

#[test]
fn agent_allowlist_must_be_present_explicit_unique_and_known() {
    for value in [None, Some(Value::Null)] {
        let mut encoded = serde_json::to_value(descriptor()).unwrap();
        if let Some(value) = value {
            encoded["agent_parameters"] = value;
        } else {
            encoded.as_object_mut().unwrap().remove("agent_parameters");
        }
        assert!(ToolDescriptor::from_json(&encoded.to_string()).is_err());
    }
    for parameters in [vec!["query", "query"], vec!["query", "unknown"]] {
        let mut tool = descriptor();
        tool.agent_parameters = parameters.into_iter().map(str::to_owned).collect();
        assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());
    }
}

#[test]
fn empty_and_complete_allowlists_have_explicit_input_semantics() {
    let all_system = SystemInputRegistry::new(vec![
        definition("query", json!({"type":"string"})),
        definition("limit", json!({"type":"integer"})),
        definition("workspace_id", json!({"type":"string","format":"uuid"})),
    ])
    .unwrap();
    let mut hidden = descriptor();
    hidden.agent_parameters.clear();
    let compiled = SchemaCompiler::new().compile(hidden, &all_system).unwrap();
    assert_eq!(
        compiled.model_input_schema(),
        &json!({
            "type":"object","properties":{},"required":[],"additionalProperties":false
        })
    );
    compiled.validate_model_inputs(&JsonObject::new()).unwrap();
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"x"})))
            .is_err()
    );
    assert_eq!(compiled.system_bindings().len(), 3);

    let mut visible = descriptor();
    visible.agent_parameters.push("workspace_id".into());
    let empty_registry = SystemInputRegistry::new(vec![]).unwrap();
    let compiled = SchemaCompiler::new()
        .compile(visible, &empty_registry)
        .unwrap();
    assert!(compiled.system_bindings().is_empty());
    compiled
        .validate_model_inputs(&object(json!({"query":"x","workspace_id":WORKSPACE})))
        .unwrap();
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"x"})))
            .is_err()
    );
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"x","workspace_id":"invalid"})))
            .is_err()
    );
}

#[test]
fn aliases_resolve_registered_metadata_and_cannot_reassign_model_owned_parameters() {
    let registry = SystemInputRegistry::new(vec![definition(
        "active_workspace",
        json!({"type":"string","format":"uuid"}),
    )])
    .unwrap();
    let mut aliased = descriptor();
    aliased.system_bindings = Some(BTreeMap::from([(
        "workspace_id".into(),
        id("active_workspace"),
    )]));
    let compiled = SchemaCompiler::new().compile(aliased, &registry).unwrap();
    assert_eq!(
        compiled.system_bindings()["workspace_id"].key,
        id("active_workspace")
    );
    assert_eq!(
        compiled.system_bindings()["workspace_id"].version,
        id("definition-1")
    );

    for (parameter, key) in [
        ("query", "active_workspace"),
        ("missing", "active_workspace"),
        ("workspace_id", "unregistered"),
    ] {
        let mut invalid = descriptor();
        invalid.system_bindings = Some(BTreeMap::from([(parameter.into(), id(key))]));
        assert!(SchemaCompiler::new().compile(invalid, &registry).is_err());
    }
    // A registered definition is required even before runtime values are supplied.
    assert!(
        SchemaCompiler::new()
            .compile(descriptor(), &SystemInputRegistry::new(vec![]).unwrap())
            .is_err()
    );
}

#[test]
fn one_system_key_cannot_have_competing_definitions_or_supply_sources() {
    let first = definition("workspace_id", json!({"type":"string"}));
    assert!(SystemInputRegistry::new(vec![first.clone(), first.clone()]).is_err());
    let mut other_version = first.clone();
    other_version.version = id("definition-2");
    assert!(SystemInputRegistry::new(vec![first.clone(), other_version]).is_err());
    let mut resolver = first.clone();
    resolver.source = SystemInputSource::Resolver {
        resolver_ref: reference("current_workspace"),
    };
    assert!(SystemInputRegistry::new(vec![first, resolver]).is_err());
}

#[test]
fn root_annotations_and_hidden_definitions_are_excluded_while_selected_constraints_survive() {
    let mut tool = descriptor();
    tool.input_schema["description"] = json!("Internal execution values");
    tool.input_schema["default"] = json!({"query":"default","workspace_id":WORKSPACE});
    tool.input_schema["examples"] = json!([{"query":"example","workspace_id":WORKSPACE}]);
    tool.input_schema["properties"]["workspace_id"]["default"] = json!(WORKSPACE);
    tool.input_schema["properties"]["workspace_id"]["examples"] = json!([WORKSPACE]);
    tool.input_schema["properties"]["query"] = json!({"$ref":"#/$defs/Query"});
    tool.input_schema["$defs"] = json!({
        "Query":{"$ref":"#/$defs/ShortText","description":"A selected query"},
        "ShortText":{"type":"string","minLength":2,"examples":["alpha"]},
        "Hidden":{"type":"string","default":WORKSPACE},
        "Unused":{"type":"object","examples":[{"workspace_id":WORKSPACE}]}
    });
    let compiled = SchemaCompiler::new().compile(tool, &registry()).unwrap();
    let projected = compiled.model_input_schema();
    assert!(projected.get("description").is_none());
    assert!(projected.get("default").is_none());
    assert!(projected.get("examples").is_none());
    assert!(projected["properties"].get("workspace_id").is_none());
    assert_eq!(
        projected["$defs"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        ["Query".to_owned(), "ShortText".to_owned()]
            .into_iter()
            .collect()
    );
    assert_eq!(
        projected["$defs"]["Query"]["description"],
        json!("A selected query")
    );
    assert_eq!(
        projected["$defs"]["ShortText"]["examples"],
        json!(["alpha"])
    );
    assert_eq!(projected["properties"]["limit"]["default"], json!(10));
    compiled
        .validate_model_inputs(&object(json!({"query":"alpha"})))
        .unwrap();
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"a"})))
            .is_err()
    );
}

#[test]
fn local_definitions_shared_with_hidden_properties_remain_when_the_model_reaches_them() {
    let mut tool = descriptor();
    tool.input_schema["properties"]["query"] = json!({"$ref":"#/definitions/Identifier"});
    tool.input_schema["properties"]["workspace_id"] = json!({"$ref":"#/definitions/Identifier"});
    tool.input_schema["definitions"] = json!({"Identifier":{"type":"string","format":"uuid"}});
    let compiled = SchemaCompiler::new().compile(tool, &registry()).unwrap();
    compiled
        .validate_model_inputs(&object(json!({"query":WORKSPACE})))
        .unwrap();
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"not-an-id"})))
            .is_err()
    );
    assert!(
        compiled.model_input_schema()["properties"]
            .get("workspace_id")
            .is_none()
    );
    assert_eq!(
        compiled.model_input_schema()["definitions"]["Identifier"]["format"],
        json!("uuid")
    );
}

#[test]
fn selected_annotation_data_does_not_create_schema_reference_edges() {
    let annotation = json!({"$ref":"https://schemas.example.invalid/data-not-schema"});
    let mut tool = descriptor();
    tool.input_schema["properties"]["query"] = json!({
        "type":"object", "properties":{"$ref":{"type":"string"}},
        "required":["$ref"], "additionalProperties":false,
        "default":annotation, "examples":[annotation]
    });
    let compiled = SchemaCompiler::new().compile(tool, &registry()).unwrap();
    compiled
        .validate_model_inputs(&object(json!({"query":annotation})))
        .unwrap();
    assert_eq!(
        compiled.model_input_schema()["properties"]["query"]["default"],
        annotation
    );
    assert_eq!(
        compiled.model_input_schema()["properties"]["query"]["examples"],
        json!([annotation])
    );
}

#[test]
fn unsupported_reference_forms_fail_instead_of_being_silently_projected() {
    for reference in [
        "#/properties/workspace_id",
        "#/$defs/Missing",
        "https://schemas.example.invalid/input.json",
        "file:///not-a-schema.json",
    ] {
        let mut tool = descriptor();
        tool.input_schema["properties"]["query"] = json!({"$ref":reference});
        assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());
    }
    let mut nested_pointer = descriptor();
    nested_pointer.input_schema["properties"]["query"] =
        json!({"$ref":"#/$defs/Query/properties/nested"});
    nested_pointer.input_schema["$defs"] = json!({
        "Query":{"type":"object","properties":{"nested":{"type":"string"}}}
    });
    assert!(
        SchemaCompiler::new()
            .compile(nested_pointer, &registry())
            .is_err()
    );
    for schema in [
        json!({"$dynamicRef":"#node"}),
        json!({"type":"string","$anchor":"node"}),
        json!({"type":"string","$id":"https://schemas.example.invalid/local"}),
        json!({"type":"object","$defs":{"Nested":{"type":"string"}},"properties":{}}),
    ] {
        let mut tool = descriptor();
        tool.input_schema["properties"]["query"] = schema;
        assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());
    }
}

#[test]
fn mixed_sources_reject_cross_parameter_conditions_but_all_agent_conditions_are_preserved() {
    for (keyword, condition) in [
        ("allOf", json!([{"required":["workspace_id"]}])),
        (
            "anyOf",
            json!([{"required":["workspace_id"]},{"required":["query"]}]),
        ),
        (
            "oneOf",
            json!([{"required":["workspace_id"]},{"required":["limit"]}]),
        ),
        ("dependentRequired", json!({"query":["workspace_id"]})),
        ("minProperties", json!(2)),
        ("patternProperties", json!({"^private_":{"type":"string"}})),
    ] {
        let mut tool = descriptor();
        tool.input_schema[keyword] = condition;
        assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());
    }
    let mut conditional = descriptor();
    conditional.input_schema["if"] = json!({"properties":{"query":{"const":"strict"}}});
    conditional.input_schema["then"] = json!({"$ref":"#/$defs/NeedsLimit"});
    conditional.input_schema["$defs"] = json!({"NeedsLimit":{"required":["limit"]}});
    conditional.input_schema["examples"] =
        json!([{"query":"root annotation","workspace_id":WORKSPACE}]);
    conditional.input_schema["default"] = json!({"query":"root default","workspace_id":WORKSPACE});
    assert!(
        SchemaCompiler::new()
            .compile(conditional.clone(), &registry())
            .is_err()
    );
    conditional.agent_parameters.push("workspace_id".into());
    let compiled = SchemaCompiler::new()
        .compile(conditional, &registry())
        .unwrap();
    assert!(compiled.model_input_schema().get("examples").is_none());
    assert!(compiled.model_input_schema().get("default").is_none());
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"strict","workspace_id":WORKSPACE})))
            .is_err()
    );
    compiled
        .validate_model_inputs(&object(
            json!({"query":"strict","limit":2,"workspace_id":WORKSPACE}),
        ))
        .unwrap();
    compiled
        .validate_model_inputs(&object(
            json!({"query":"ordinary","workspace_id":WORKSPACE}),
        ))
        .unwrap();
}

#[test]
fn the_declared_top_level_object_contract_cannot_be_weakened_or_ambiguous() {
    for (keyword, replacement) in [
        ("type", json!("array")),
        ("additionalProperties", json!(true)),
        ("properties", json!([])),
        ("required", json!(["query", "absent"])),
        ("required", json!(["query", "query"])),
    ] {
        let mut tool = descriptor();
        tool.input_schema[keyword] = replacement;
        assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());
    }
    for keyword in ["type", "properties", "required", "additionalProperties"] {
        let mut tool = descriptor();
        tool.input_schema.as_object_mut().unwrap().remove(keyword);
        assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());
    }
}

#[test]
fn nested_objects_are_owned_whole_and_dotted_root_names_are_literal_names() {
    let mut tool = descriptor();
    tool.input_schema["properties"]["query"] = json!({
        "type":"object", "properties":{"workspace_id":{"type":"string"}},
        "required":["workspace_id"], "additionalProperties":false
    });
    let compiled = SchemaCompiler::new()
        .compile(tool.clone(), &registry())
        .unwrap();
    compiled
        .validate_model_inputs(&object(
            json!({"query":{"workspace_id":"model-owned-nested-value"}}),
        ))
        .unwrap();
    tool.agent_parameters = vec!["query.workspace_id".into()];
    assert!(
        SchemaCompiler::new()
            .compile(tool.clone(), &registry())
            .is_err()
    );
    tool.agent_parameters = vec!["query".into(), "limit".into()];
    tool.system_bindings = Some(BTreeMap::from([(
        "query.workspace_id".into(),
        id("workspace_id"),
    )]));
    assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());

    let mut dotted = descriptor();
    dotted.input_schema["properties"]["query.name"] = json!({"type":"string"});
    dotted.agent_parameters.push("query.name".into());
    let compiled = SchemaCompiler::new().compile(dotted, &registry()).unwrap();
    compiled
        .validate_model_inputs(&object(json!({"query":"x","query.name":"literal"})))
        .unwrap();
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"x","name":"not-the-literal-key"})))
            .is_err()
    );
}

#[tokio::test]
async fn compiled_model_tool_rejects_a_model_supplied_hidden_uuid_in_real_response_collection() {
    let compiled = SchemaCompiler::new()
        .compile(descriptor(), &registry())
        .unwrap();
    let route = ResolvedModelRoute {
        binding: reference("model"),
        catalog_revision: id("catalog"),
        routing_policy_revision: id("policy"),
        requested_model: id("model"),
        model_id: id("model"),
        model_version: id("release"),
        version_semantics: VersionSemantics::Pinned,
        provider: id("scripted"),
        target: JsonObject::new(),
        deployment_revision: None,
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        adapter: reference("adapter"),
        capability_revision: id("capabilities"),
        connection_ref: reference("connection"),
    };
    let request = ModelRequest {
        request_id: id("request"),
        purpose: ModelPurpose::Agent,
        route,
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Search records".into(),
            }],
        }],
        tools: vec![compiled.to_model_tool()],
        output: ModelOutput::Text {},
        max_output_tokens: 128.try_into().unwrap(),
        limits: ModelResponseLimits {
            max_input_bytes: 16384,
            max_response_bytes: 4096,
            max_delta_bytes: 1024,
            max_events: 8,
            max_tool_calls: 2,
        },
    };
    let events = vec![
        ModelEvent::ToolArgumentsDelta {
            index: 0,
            provider_call_id: Some("hidden".into()),
            name: Some("search".into()),
            delta: json!({"query":"x","workspace_id":WORKSPACE}).to_string(),
        },
        ModelEvent::ToolArgumentsDelta {
            index: 1,
            provider_call_id: Some("model-only".into()),
            name: Some("search".into()),
            delta: json!({"query":"x"}).to_string(),
        },
        ModelEvent::ResponseCompleted {
            finish: ModelFinish::ToolCalls,
            metadata: ModelResponseMetadata::default(),
            continuation: vec![],
        },
    ];
    let response =
        collect_model_response(&request, Box::pin(stream::iter(events.into_iter().map(Ok))))
            .await
            .unwrap();
    assert_eq!(
        response.tool_calls[0].validation,
        ToolCallValidation::InvalidArguments
    );
    assert_eq!(response.tool_calls[1].validation, ToolCallValidation::Valid);
    assert_eq!(
        response.tool_calls[0].model_inputs["workspace_id"],
        json!(WORKSPACE)
    );
}

#[test]
fn restored_compilation_rejects_cached_projection_or_definition_changes() {
    let compiler = SchemaCompiler::new();
    let registry = registry();
    let compiled = compiler.compile(descriptor(), &registry).unwrap();
    let encoded = serde_json::to_string(&compiled).unwrap();
    let restored = compiler
        .restore(&encoded, &registry, compiled.digest())
        .unwrap();
    restored
        .validate_model_inputs(&object(json!({"query":"valid"})))
        .unwrap();
    assert!(
        restored
            .validate_model_inputs(&object(json!({"query":"valid","workspace_id":WORKSPACE})))
            .is_err()
    );
    let mut projection = serde_json::to_value(&compiled).unwrap();
    projection["model_input_schema"]["additionalProperties"] = json!(true);
    assert!(
        compiler
            .restore(&projection.to_string(), &registry, compiled.digest())
            .is_err()
    );
    let mut definition = registry.get(&id("workspace_id")).unwrap().clone();
    definition.version = id("definition-2");
    let changed = SystemInputRegistry::new(vec![definition]).unwrap();
    assert!(
        compiler
            .restore(&encoded, &changed, compiled.digest())
            .is_err()
    );
    let mut tool = descriptor();
    tool.input_schema["properties"]["workspace_id"]["description"] =
        json!("Updated hidden contract");
    let changed_tool = compiler.compile(tool, &registry).unwrap();
    assert_eq!(
        changed_tool.model_input_schema(),
        compiled.model_input_schema()
    );
    assert_ne!(
        changed_tool.descriptor_digest(),
        compiled.descriptor_digest()
    );
    assert_ne!(changed_tool.digest(), compiled.digest());
    assert!(
        compiler
            .restore(
                &serde_json::to_string(&changed_tool).unwrap(),
                &registry,
                compiled.digest()
            )
            .is_err()
    );
}

#[test]
fn declared_execution_safety_cannot_mark_writes_as_parallel_or_read_only_retries() {
    let compiler = SchemaCompiler::new();
    let mut tool = descriptor();
    tool.side_effect = ToolSideEffect::Write;
    tool.concurrency = ToolConcurrency::ParallelRead;
    assert!(compiler.compile(tool.clone(), &registry()).is_err());
    tool.concurrency = ToolConcurrency::Serial;
    tool.retry = ToolRetryPolicy::ReadOnly;
    assert!(compiler.compile(tool.clone(), &registry()).is_err());
    tool.retry = ToolRetryPolicy::Idempotent;
    let compiled = compiler.compile(tool, &registry()).unwrap();
    assert_eq!(compiled.descriptor().side_effect, ToolSideEffect::Write);
    assert_eq!(compiled.descriptor().retry, ToolRetryPolicy::Idempotent);
}
```

## `tests/support/tool_schema_consumer.rs`

```rust
use futures_util::stream;
use serde_json::json;
use std::collections::BTreeMap;
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}

fn registry(revision: &str) -> Result<SystemInputRegistry, ContractError> {
    SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("active_workspace_id"),
        version: id(revision),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
}

fn model_request(tool: ModelTool) -> ModelRequest {
    ModelRequest {
        request_id: id("model-request"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference("local-model"),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id("example-model"),
            model_id: id("example-model"),
            model_version: id("1"),
            version_semantics: VersionSemantics::Pinned,
            provider: id("example-provider"),
            target: JsonObject::new(),
            deployment_revision: None,
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("1"),
            },
            adapter: reference("example-adapter"),
            capability_revision: id("capabilities"),
            connection_ref: reference("connection"),
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find recent reports".into(),
            }],
        }],
        tools: vec![tool],
        output: ModelOutput::Text {},
        max_output_tokens: 256.try_into().unwrap(),
        limits: ModelResponseLimits {
            max_input_bytes: 16_384,
            max_response_bytes: 4096,
            max_delta_bytes: 1024,
            max_events: 8,
            max_tool_calls: 1,
        },
    }
}

async fn propose(
    request: &ModelRequest,
    inputs: &JsonObject,
) -> Result<ModelResponse, ModelProtocolError> {
    collect_model_response(
        request,
        Box::pin(stream::iter([
            Ok(ModelEvent::ToolArgumentsDelta {
                index: 0,
                provider_call_id: Some("call".into()),
                name: Some("search_reports".into()),
                delta: serde_json::to_string(inputs).unwrap(),
            }),
            Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::ToolCalls,
                metadata: ModelResponseMetadata::default(),
                continuation: vec![],
            }),
        ])),
    )
    .await
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptor = ToolDescriptor::from_json(
        r##"{
      "tool":{"id":"report-search","version":"1"},
      "name":"search_reports","description":"Search reports in the current workspace",
      "input_schema":{
        "type":"object",
        "properties":{
          "query":{"$ref":"#/$defs/Query"},
          "limit":{"type":"integer","minimum":1,"default":10},
          "workspace_id":{"$ref":"#/$defs/WorkspaceId"}
        },
        "required":["query","workspace_id"],"additionalProperties":false,
        "examples":[{"query":"private example","workspace_id":"f7fba7f5-f8e7-4f44-b885-45d0688e9f33"}],
        "$defs":{
          "Query":{"type":"string","minLength":1},
          "WorkspaceId":{"type":"string","format":"uuid"},
          "Unused":{"type":"string","description":"unrelated internal schema"}
        }
      },
      "agent_parameters":["query","limit"],
      "system_bindings":{"workspace_id":"active_workspace_id"},
      "output_schema":{"type":"array","items":{"type":"string"}},
      "side_effect":"read_only","concurrency":"serial","retry":"never","reconcile":false,
      "max_output_bytes":4096
    }"##,
    )?;
    let registry = registry("1")?;
    let compiler = SchemaCompiler::new();
    let compiled = compiler.compile(descriptor, &registry)?;
    let schema = compiled.model_input_schema();
    assert_eq!(schema["required"], json!(["query"]));
    assert_eq!(schema["additionalProperties"], false);
    assert!(schema["properties"].get("workspace_id").is_none());
    assert!(schema.get("examples").is_none());
    assert!(schema["$defs"].get("WorkspaceId").is_none());
    assert!(schema["$defs"].get("Unused").is_none());
    assert!(schema["$defs"].get("Query").is_some());
    assert_eq!(
        compiled.system_bindings()["workspace_id"].key,
        id("active_workspace_id")
    );
    let model_inputs = BTreeMap::from([("query".into(), json!("recent results"))]);
    compiled.validate_model_inputs(&model_inputs)?;
    // Validation alone does not apply defaults; the binder owns that operation.
    let mut full = model_inputs.clone();
    full.insert(
        "workspace_id".into(),
        json!("f7fba7f5-f8e7-4f44-b885-45d0688e9f33"),
    );
    assert!(compiled.validate_model_inputs(&full).is_err());
    compiled.validate_execution_inputs(&full)?;
    let mut invalid_full = full.clone();
    invalid_full.insert("workspace_id".into(), json!("an-invented-hash"));
    assert!(compiled.validate_execution_inputs(&invalid_full).is_err());
    assert!(compiled.validate_execution_inputs(&model_inputs).is_err());
    let request = model_request(compiled.to_model_tool());
    assert_eq!(
        propose(&request, &model_inputs).await?.tool_calls[0].validation,
        ToolCallValidation::Valid
    );
    assert_eq!(
        propose(&request, &full).await?.tool_calls[0].validation,
        ToolCallValidation::InvalidArguments
    );
    let saved = serde_json::to_string(&compiled)?;
    let restored = compiler.restore(&saved, &registry, compiled.digest())?;
    assert_eq!(restored.digest(), compiled.digest());
    assert_eq!(restored.model_input_schema(), compiled.model_input_schema());
    let changed_registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("active_workspace_id"),
        version: id("2"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])?;
    assert!(
        compiler
            .restore(&saved, &changed_registry, compiled.digest())
            .is_err()
    );
    println!(
        "tool schema consumer: query/limit exposed; hidden schema omitted; hidden input rejected by the model boundary; full UUID schema checked; compiled identity preserved and changed registry rejected"
    );
    Ok(())
}
```
