# 38장 전체 구현과 변경 검사

[강의](../38-canonical-json.md) · [전체 변경 패치](../solutions/38-canonical-json.patch)

기준 `5a849675a58b3f5abf19b971ad943c85949f24e5`. 이 단계에서 바뀐 Rust·manifest·Python 파일의 전체 내용이다. 이전 버전과의 정확한 교체 위치·삭제는 patch를 따른다. 다음 장의 코드와 섞지 않는다.

## `crates/wickle/Cargo.toml`

```toml
[package]
name = "wickle"
version.workspace = true
license.workspace = true
edition.workspace = true
rust-version.workspace = true
repository.workspace = true
description = "An extensible agent engine for Rust applications"
publish = false
include = ["Cargo.toml", "LICENSE", "src/**"]

[dependencies]
getrandom.workspace = true
futures-util.workspace = true
jsonschema.workspace = true
serde.workspace = true
serde_json = { workspace = true, features = ["raw_value"] }
sha2.workspace = true
thiserror.workspace = true
tokio.workspace = true
tokio-util.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["rt-multi-thread", "test-util"] }
serde_json = { workspace = true, features = ["preserve_order"] }

[lints]
workspace = true
```

## `crates/wickle/src/canonical.rs`

```rust
//! Versioned canonicalization at the JSON text boundary. Never round-trip new
//! numeric tokens through floating point or reinterpret an old digest version.
use std::{collections::BTreeMap, fmt};

use crate::{ContractError, ErrorCode, JsonDigest, canonical_digest_json};
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, Visitor},
};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

/// Rules recorded alongside a submitted request, independent of runtime version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CanonicalizationVersion {
    /// Historical Value-based encoding. Existing stored comparisons keep this rule.
    SortedJsonV1,
    /// Object ordering and fixed string escaping with original number lexemes.
    WickleCanonicalJsonV1,
}
use serde::Serialize;

/// Resource bounds applied before allocating the canonical representation.
#[derive(Debug, Clone, Copy)]
pub struct JsonTextLimits {
    /// Maximum input UTF-8 bytes.
    pub max_bytes: usize,
    /// Maximum nested object/array depth (a scalar has depth zero).
    pub max_depth: usize,
}
impl Default for JsonTextLimits {
    fn default() -> Self {
        Self {
            max_bytes: 1024 * 1024,
            max_depth: 128,
        }
    }
}
fn invalid() -> ContractError {
    ContractError::new(ErrorCode::InvalidJson, "$")
}

/// Canonicalize strict JSON, preserving every number token exactly.
///
/// Duplicate keys (including equivalent escaped spellings), trailing JSON,
/// invalid syntax and resource-limit violations are errors. Limits may be lowered;
/// depth is capped at 128 to bound stack use even with untrusted configuration.
/// Output string escaping is serde_json's JSON encoder; Unicode is not normalized.
pub fn canonicalize_json_text(
    input: &str,
    limits: JsonTextLimits,
) -> Result<Vec<u8>, ContractError> {
    if input.len() > limits.max_bytes || limits.max_depth > 128 {
        return Err(invalid());
    }
    // Bound nesting before RawValue scans the document. Quoted braces are data.
    let (mut depth, mut quoted, mut escape) = (0usize, false, false);
    for b in input.bytes() {
        if quoted {
            if escape {
                escape = false;
            } else if b == b'\\' {
                escape = true;
            } else if b == b'"' {
                quoted = false;
            }
        } else {
            match b {
                b'"' => quoted = true,
                b'{' | b'[' => {
                    depth += 1;
                    if depth > limits.max_depth {
                        return Err(invalid());
                    }
                }
                b'}' | b']' => {
                    depth = depth.checked_sub(1).ok_or_else(invalid)?;
                }
                _ => {}
            }
        }
    }
    let raw: &RawValue = serde_json::from_str(input).map_err(|_| invalid())?;
    let mut out = Vec::with_capacity(input.len());
    encode(raw, &mut out)?;
    Ok(out)
}

struct Object<'a>(BTreeMap<String, &'a RawValue>);
impl<'de> Deserialize<'de> for Object<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct ObjectVisitor;
        impl<'de> Visitor<'de> for ObjectVisitor {
            type Value = Object<'de>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an object with unique keys")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut result = BTreeMap::new();
                while let Some(key) = map.next_key::<String>()? {
                    if result.contains_key(&key) {
                        return Err(de::Error::custom("duplicate object key"));
                    }
                    result.insert(key, map.next_value::<&'de RawValue>()?);
                }
                Ok(Object(result))
            }
        }
        d.deserialize_map(ObjectVisitor)
    }
}
fn encode(raw: &RawValue, out: &mut Vec<u8>) -> Result<(), ContractError> {
    let text = raw.get();
    match text.as_bytes()[0] {
        b'{' => {
            let object: Object<'_> = serde_json::from_str(text).map_err(|_| invalid())?;
            out.push(b'{');
            for (i, (key, value)) in object.0.into_iter().enumerate() {
                if i != 0 {
                    out.push(b',');
                }
                serde_json::to_writer(&mut *out, &key).map_err(|_| invalid())?;
                out.push(b':');
                encode(value, out)?;
            }
            out.push(b'}');
        }
        b'[' => {
            let array: Vec<&RawValue> = serde_json::from_str(text).map_err(|_| invalid())?;
            out.push(b'[');
            for (i, value) in array.into_iter().enumerate() {
                if i != 0 {
                    out.push(b',');
                }
                encode(value, out)?;
            }
            out.push(b']');
        }
        b'"' => {
            let value: String = serde_json::from_str(text).map_err(|_| invalid())?;
            serde_json::to_writer(out, &value).map_err(|_| invalid())?;
        }
        _ => out.extend_from_slice(text.as_bytes()), // RawValue already validated JSON grammar.
    }
    Ok(())
}

/// Hash a submitted JSON document using its explicitly selected stored rules.
///
/// New rules preserve number lexemes; the legacy branch intentionally uses the
/// historical parser/encoder and keeps its original range limitations.
pub fn versioned_digest_json(
    input: &str,
    version: CanonicalizationVersion,
    limits: JsonTextLimits,
) -> Result<JsonDigest, ContractError> {
    let bytes = canonicalize_json_text(input, limits)?;
    match version {
        CanonicalizationVersion::SortedJsonV1 => canonical_digest_json(input),
        CanonicalizationVersion::WickleCanonicalJsonV1 => {
            let hex: String = Sha256::digest(bytes)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            JsonDigest::try_from(format!("wickle-canonical-json-v1:sha256:{hex}"))
        }
    }
}
```

## `crates/wickle/src/lib.rs`

```rust
//! Wickle, an agent engine for Rust applications.
//!
//! Profiles, scoped metadata resolution, and versioned execution data contracts.
//! The agent driver runs model/tool loops with scoped ports, separate system
//! inputs, persisted attempt accounting, and explicit effect outcomes.
//!
//! Runtime objects stay in Host code. Only documented data contracts are
//! serialized; successful decoding does not authenticate a caller.
//!
//! Internal modules are not extension points; use the root exports.
//! ```compile_fail
//! use wickle::serialization::canonical_digest;
//! ```

mod agent;
mod artifacts;
mod budget;
mod canonical;
mod clock;
mod component_runtime;
mod context;
mod context_projection;
mod context_source;
mod context_strategy;
mod error;
mod hooks;
mod input_binding;
mod message;
mod model;
mod model_catalog;
mod model_dispatch;
mod model_execution;
mod model_protocol;
mod model_routing;
mod policy;
mod profile;
mod resolution;
mod run;
mod serialization;
pub use canonical::{
    CanonicalizationVersion, JsonTextLimits, canonicalize_json_text, versioned_digest_json,
};
mod skills;
mod state;
mod tool_execution;
mod tool_schema;
mod views;

pub use agent::{
    Agent, AgentBindings, AgentSettings, CancelReceipt, ComponentReleaseView, HookObservationView,
    ModelTokenEstimator, PersistenceFailure, RunHandle, UnconfirmedToolEffect, create_agent,
};
pub use artifacts::{
    ArtifactCallContext, ArtifactData, ArtifactInput, ArtifactLimits, ArtifactMetadata,
    ArtifactPreview, ArtifactRuntime, ArtifactStore, MemoryArtifactStore,
};
pub use budget::{AttemptReservation, ReservationKind, RunBudget, RunTiming};
pub use clock::{Clock, ClockReading, IdSource, RandomIdSource, SystemClock};
pub use component_runtime::{
    AdapterBindingState, AdapterCloseContext, AdapterDefinition, AdapterExportDefinition,
    AdapterExportInstance, AdapterFactory, AdapterInitContext, AdapterInstance, BoundCapabilities,
    ComponentBindContext, ComponentBindPurpose, ComponentRelease, ComponentReleaseContext,
    ComponentReleaseFailure, ComponentReleaseReport, ComponentResolveContext, ComponentRuntime,
    ResolvedAdapterBinding, ResolvedAssembly, ResolvedConnection, ResolvedHookBinding,
    ResolvedToolBinding,
};
pub use context_projection::{
    CONTEXT_ASSEMBLER_VERSION, ContextAssembler, ContextItem, ContextLifetime, ContextOrigin,
    ContextPriority, ContextProjection, InstructionAssetContent, PinnedPromptTool, ProjectionInput,
    ProjectionLimits, PromptSnapshot, PromptToolBinding, ScopedOpaque, SkillManifest,
};
pub use context_source::{
    ContextBatch, ContextCallContext, ContextRequest, ContextResult, ContextSource,
    ContextSourceDefinition, ContextSourcePlan, ContextSourceRegistration, ContextSourceRegistry,
    ContextSourceRuntime, ContextSourceUsage, ContextTokenEstimator, ContextUseRequest,
    PlannedContextSource, ResolvedSourceBinding,
};
pub use context_strategy::{
    BoundedContextStrategy, CompactionRequest, ContextCompactor, ContextDecision, ContextPlan,
    ContextPreview, ContextRevision, ContextRewriteLimits, ContextRuntime, ContextSegment,
    ContextSelectionInput, ContextStrategy, ContextStrategyContext, ContextStrategyDefinition,
    HostContextCompactor, ModelCompactorConfig,
};
pub use hooks::{
    HookApplication, HookApplicationRecord, HookContext, HookContextAddition, HookDefinition,
    HookHandler, HookInput, HookObservation, HookObservationStatus, HookOutput, HookPlan,
    HookRegistration, HookRegistry, HookRuntime, HookTarget, HookTransform,
};
pub use input_binding::{
    BoundSystemInput, BoundToolInput, InputBinder, InputBindingLimits, ResolvedSystemInput,
    RunSystemInputs, SystemInputResolveContext, SystemInputResolveRequest, SystemInputResolver,
    ToolBindingResult,
};
pub use model_catalog::{
    CatalogRequirements, ModelAlias, ModelBinding, ModelCapabilities, ModelCatalog,
    ModelCatalogSnapshot, ModelDefinition, ModelDefinitionRef, ModelEvidence, ModelLifecycle,
    ModelSupportStatus, ModelValidationEvidence, ModelValidationKind, ResolvedCatalogBinding,
};
pub use model_protocol::{
    ModelCallContext, ModelContent, ModelEvent, ModelFinish, ModelMessage, ModelOutput, ModelPort,
    ModelPortBinding, ModelProtocolError, ModelProtocolErrorCode, ModelRequest, ModelResponse,
    ModelResponseLimits, ModelResponseMetadata, ModelRole, ModelTool, OpaqueContinuation,
    ProposedToolCall, ToolCallValidation, collect_model_response,
};
pub use model_routing::{
    MAX_ROUTE_FALLBACKS, MAX_ROUTING_RULES, ModelRouter, ROUTING_SNAPSHOT_VERSION, RouteSelection,
    RouteSelectionReason, RoutingPolicy, RoutingRule, RoutingSnapshot,
};
pub use policy::{
    ApprovalChallenge, Guarded, PolicyAction, PolicyContext, PolicyDecision, PolicyGate,
    PolicyPort, PolicyRequest, ToolApproval, ToolPolicyInput,
};
pub use skills::{
    LoadedSkill, PlannedSkill, SkillBindings, SkillCallContext, SkillDefinition, SkillLimits,
    SkillPlan, SkillResolver, SkillRuntime,
};
pub use state::{
    AdmissionInput, AdmissionResult, CommitInput, EventPage, MAX_EVENT_PAGE_SIZE, MemoryStateStore,
    ProtectedRecord, RunLease, STATE_STORE_CHECKPOINT_VERSION, StateStore, StateStoreCapabilities,
    StateStoreCheckpoint, StoredRun,
};
pub use tool_execution::{
    ExternalReceiptContext, ExternalReceiptRequest, ExternalReceiptVerifier,
    PreparedToolResolution, SerialToolRound, ToolEffect, ToolExecutionContext, ToolExecutionLimits,
    ToolExecutionOutcome, ToolExecutionResult, ToolExecutor, ToolRegistration, ToolRegistry,
    ToolRoundOutcome,
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
pub use model_dispatch::{
    ModelDispatcher, ModelInspectionContext, ModelRouteAvailability, ModelRouteInspector,
    ModelRouteObservation,
};
pub use model_execution::{
    ModelExchange, ModelExchangeOutcome, ModelProjectionContext, ModelRequestProjector,
    ModelRetryPolicy, ProjectedModelRequest, RoutedModelInput, StoredModelResponse,
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
    ResumeCommand, ResumeReceipt, RunEvent, RunEventPayload, RunEventSchemaVersion, RunOutcome,
    RunPhase, RunRequest, RunSnapshot, RunSnapshotSchemaVersion, RunStatus, RunTrigger,
    SessionSchemaVersion, SessionSnapshot, SourceExecutionState, SystemInputSnapshotRef,
    ToolCallState, ToolLedgerEntry, VerificationSummary, VerificationVerdict, WaitState,
    WaitTarget, admission_digest,
};
pub use serialization::{
    Id, JsonDigest, JsonObject, canonical_digest, canonical_digest_json, parse_json,
};

mod verification;
pub use verification::{
    OutputSchemaDefinition, VerificationCandidate, VerificationDecision, VerificationInput,
    VerificationLimits, VerificationModel, VerificationModelRequest, VerificationPlan,
    VerificationRuntime, Verifier, VerifierContext, VerifierDefinition,
};

pub use verification::SchemaVerifier;

mod future;

pub use tool_execution::ToolReconciliation;

mod recovery;
pub use recovery::RecoveryReceipt;
```

## `crates/wickle/src/serialization.rs`

```rust
use std::{collections::BTreeMap, fmt};

use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Number, Value};
use sha2::{Digest as _, Sha256};

use crate::{ContractError, ErrorCode};

/// An opaque, nonblank identifier. No UUID, SemVer, or provider naming is assumed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Id(String);

impl Id {
    /// Validate an identifier without normalizing its spelling.
    pub fn new(value: impl Into<String>) -> Result<Self, ContractError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ContractError::new(ErrorCode::InvalidContract, "identifier"));
        }
        Ok(Self(value))
    }

    /// Return the original identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Id {
    type Error = ContractError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<Id> for String {
    fn from(value: Id) -> Self {
        value.0
    }
}
impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A SHA-256 digest carrying its canonicalization version.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct JsonDigest(String);

impl JsonDigest {
    /// Return the encoding version, algorithm, and hexadecimal digest.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for JsonDigest {
    type Error = ContractError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let valid = value
            .strip_prefix("sorted-json-v1:sha256:")
            .or_else(|| value.strip_prefix("wickle-canonical-json-v1:sha256:"))
            .is_some_and(|hex| {
                hex.len() == 64
                    && hex
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            });
        if !valid {
            return Err(ContractError::new(ErrorCode::InvalidContract, "digest"));
        }
        Ok(Self(value))
    }
}
impl From<JsonDigest> for String {
    fn from(value: JsonDigest) -> Self {
        value.0
    }
}
impl fmt::Display for JsonDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A JSON object containing data, not executable objects.
pub type JsonObject = BTreeMap<String, Value>;

/// Parse JSON, rejecting duplicate object keys and nonfinite numbers.
pub fn parse_json(input: &str) -> Result<Value, ContractError> {
    serde_json::from_str::<StrictJson>(input)
        .map(|value| value.0)
        .map_err(|_| ContractError::new(ErrorCode::InvalidJson, "$"))
}

struct StrictJson(Value);

impl<'de> Deserialize<'de> for StrictJson {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct JsonVisitor;
        impl<'de> Visitor<'de> for JsonVisitor {
            type Value = StrictJson;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("unambiguous finite JSON")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(StrictJson(v.into()))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(StrictJson(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(StrictJson(v.into()))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
                Number::from_f64(v)
                    .map(|n| StrictJson(Value::Number(n)))
                    .ok_or_else(|| E::custom("nonfinite number"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(StrictJson(v.into()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(StrictJson(v.into()))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(StrictJson(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = seq.next_element::<StrictJson>()? {
                    values.push(value.0);
                }
                Ok(StrictJson(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut values = Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom("duplicate object key"));
                    }
                    values.insert(key, map.next_value::<StrictJson>()?.0);
                }
                Ok(StrictJson(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(JsonVisitor)
    }
}

/// Hash JSON after sorting object keys recursively; retain array order and values.
///
/// This is not RFC 8785. Numbers retain serde_json's representation: `1` and
/// `1.0`, and `0` and `-0.0`, remain distinct. Use [`canonical_digest_json`] when
/// reading text so duplicate keys and nonfinite numbers are rejected first.
pub fn canonical_digest(value: &Value) -> JsonDigest {
    let bytes = canonical_json_bytes(value);
    let hex: String = Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    JsonDigest(format!("sorted-json-v1:sha256:{hex}"))
}

pub(crate) fn canonical_json_bytes(value: &Value) -> Vec<u8> {
    fn ordered(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let keys: BTreeMap<_, _> = map.iter().collect();
                Value::Object(
                    keys.into_iter()
                        .map(|(k, v)| (k.clone(), ordered(v)))
                        .collect(),
                )
            }
            Value::Array(values) => Value::Array(values.iter().map(ordered).collect()),
            other => other.clone(),
        }
    }
    serde_json::to_vec(&ordered(value)).expect("JSON values serialize into a byte vector")
}

/// Parse strict JSON and compute its versioned digest.
pub fn canonical_digest_json(input: &str) -> Result<JsonDigest, ContractError> {
    parse_json(input).map(|value| canonical_digest(&value))
}

pub(crate) fn optional<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

pub(crate) fn decode<T: serde::de::DeserializeOwned>(
    input: &str,
    version: Option<&str>,
) -> Result<T, ContractError> {
    let value = parse_json(input)?;
    if let Some(expected) = version {
        let actual = value
            .get("schema_version")
            .and_then(Value::as_str)
            .ok_or_else(|| ContractError::new(ErrorCode::InvalidContract, "schema_version"))?;
        if actual != expected {
            return Err(ContractError::new(
                ErrorCode::UnsupportedSchemaVersion,
                "schema_version",
            ));
        }
    }
    serde_json::from_value(value).map_err(|_| ContractError::new(ErrorCode::InvalidContract, "$"))
}

pub(crate) fn data_digest(value: &impl Serialize) -> JsonDigest {
    canonical_digest(&serde_json::to_value(value).expect("contract DTOs contain only JSON data"))
}
```

## `crates/wickle/tests/versioned_json.rs`

```rust
//! Behavior checks for versioned JSON text contracts.
use serde_json::json;
use wickle::{
    CanonicalizationVersion as Version, CompletionPolicy, JsonDigest, JsonTextLimits,
    canonical_digest_json, canonicalize_json_text, versioned_digest_json,
};

fn canonical(input: &str) -> Vec<u8> {
    canonicalize_json_text(input, JsonTextLimits::default()).unwrap()
}
#[test]
fn large_numbers_and_exponents_are_preserved_without_float_rounding() {
    let input = r#"{"z":1E+003,"a":[184467440737095516160001,-0,1.00,1e9999]}"#;
    assert_eq!(
        canonical(input),
        br#"{"a":[184467440737095516160001,-0,1.00,1e9999],"z":1E+003}"#
    );
    for (a, b) in [
        ("1", "1.0"),
        ("1e3", "1E+003"),
        ("0", "-0"),
        ("184467440737095516160001", "184467440737095516160002"),
    ] {
        assert_ne!(
            versioned_digest_json(a, Version::WickleCanonicalJsonV1, JsonTextLimits::default())
                .unwrap(),
            versioned_digest_json(b, Version::WickleCanonicalJsonV1, JsonTextLimits::default())
                .unwrap()
        );
    }
}
#[test]
fn string_escaping_key_order_and_array_order_have_stable_meaning() {
    assert_eq!(
        canonical(r#"{"z":[2,1],"\u0061":"\u0062"}"#),
        br#"{"a":"b","z":[2,1]}"#
    );
    assert_ne!(canonical(r#"[2,1]"#), canonical(r#"[1,2]"#));
    assert_ne!(canonical(r#""é""#), canonical(r#""e\u0301""#));
    let digest = versioned_digest_json(
        "{}",
        Version::WickleCanonicalJsonV1,
        JsonTextLimits::default(),
    )
    .unwrap();
    assert_eq!(
        digest.as_str(),
        "wickle-canonical-json-v1:sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
    );
    assert_eq!(
        JsonDigest::try_from(digest.as_str().to_owned()).unwrap(),
        digest
    );
}
#[test]
fn ambiguous_invalid_or_oversized_text_cannot_be_hashed() {
    for input in [
        r#"{"a":1,"\u0061":2}"#,
        r#"{"nested":{"a":1,"a":2}}"#,
        r#"[1,]"#,
        "01",
        "+1",
        "NaN",
        "Infinity",
        "{}{}",
        r#""\ud800""#,
    ] {
        assert!(
            canonicalize_json_text(input, JsonTextLimits::default()).is_err(),
            "{input}"
        );
    }
    assert!(
        canonicalize_json_text(
            "null",
            JsonTextLimits {
                max_bytes: 3,
                max_depth: 1
            }
        )
        .is_err()
    );
    assert!(
        canonicalize_json_text(
            "[[0]]",
            JsonTextLimits {
                max_bytes: 100,
                max_depth: 1
            }
        )
        .is_err()
    );
    assert!(
        canonicalize_json_text(
            "[0]",
            JsonTextLimits {
                max_bytes: 100,
                max_depth: 1
            }
        )
        .is_ok()
    );
    assert!(
        canonicalize_json_text(
            r#""[{\"}]""#,
            JsonTextLimits {
                max_bytes: 100,
                max_depth: 0
            }
        )
        .is_ok()
    );
    assert!(
        canonicalize_json_text(
            "0",
            JsonTextLimits {
                max_bytes: 100,
                max_depth: 129
            }
        )
        .is_err()
    );
}
#[test]
fn saved_legacy_rules_are_not_silently_replaced() {
    for input in [
        "{}",
        r#"{"z":1,"a":{"x":[3,1],"b":2}}"#,
        "1.0",
        "-0.0",
        "1e3",
    ] {
        assert_eq!(
            versioned_digest_json(input, Version::SortedJsonV1, JsonTextLimits::default()).unwrap(),
            canonical_digest_json(input).unwrap()
        );
    }
    assert_eq!(
        serde_json::to_string(&Version::WickleCanonicalJsonV1).unwrap(),
        "\"wickle-canonical-json-v1\""
    );
    assert!(serde_json::from_str::<Version>("\"unknown\"").is_err());
}
#[test]
fn completion_policy_accepts_only_the_two_tagged_contracts() {
    for input in [
        json!({"mode":"turn_end"}),
        json!({"mode":"verified","verifier_ref":{"id":"report","version":"1"}}),
    ] {
        let policy: CompletionPolicy = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(serde_json::to_value(policy).unwrap(), input);
    }
    for input in [
        json!("turn_end"),
        json!(null),
        json!({}),
        json!({"mode":"unknown"}),
        json!({"mode":"verified"}),
        json!({"mode":"turn_end","verifier_ref":{"id":"a","version":"1"}}),
        json!({"mode":"verified","verifier_ref":null}),
        json!({"mode":"verified","verifier_ref":{"id":"","version":"1"}}),
        json!({"mode":"verified","verifier_ref":{"id":"a","version":""}}),
        json!({"mode":"verified","verifier_ref":{"id":"a"}}),
        json!({"mode":"verified","verifier_ref":{"id":"a","version":"1","extra":true}}),
        json!({"mode":"turn_end","extra":true}),
    ] {
        assert!(
            serde_json::from_value::<CompletionPolicy>(input.clone()).is_err(),
            "{input}"
        );
    }
}
```

## `tests/support/canonical_consumer.rs`

```rust
use wickle::{CanonicalizationVersion, JsonTextLimits, canonical_digest_json, canonicalize_json_text, versioned_digest_json};
fn main() {
    let limits = JsonTextLimits::default();
    let request = r#"{"amount":184467440737095516160001,"scale":1E+003}"#;
    assert_eq!(canonicalize_json_text(request, limits).unwrap(), request.as_bytes());
    let digest = versioned_digest_json(request, CanonicalizationVersion::WickleCanonicalJsonV1, limits).unwrap();
    let changed = request.replace("160001", "160002");
    assert_ne!(digest, versioned_digest_json(&changed, CanonicalizationVersion::WickleCanonicalJsonV1, limits).unwrap());
    assert!(canonicalize_json_text(r#"{"amount":1,"amount":2}"#, limits).is_err());
    assert_eq!(versioned_digest_json("1e3", CanonicalizationVersion::SortedJsonV1, limits).unwrap(), canonical_digest_json("1e3").unwrap());
    println!("canonical consumer: exact large numbers, exponent spelling, duplicate rejection and legacy digest selection passed");
}
```

## `tests/support/report_process_consumer.rs`

```rust
// Independent result-generation Host: real subprocesses, SQLite and file output.
#[allow(dead_code)]
mod host {
    include!("adapter_consumer.rs");
    use tokio::io::AsyncWriteExt;
    fn io_error(_: std::io::Error) -> ContractError {
        ContractError::new(ErrorCode::ComponentUnavailable, "report.file")
    }
    async fn trace(directory: &std::path::Path, value: Value) -> Result<(), ContractError> {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(directory.join("trace.jsonl"))
            .await
            .map_err(io_error)?;
        file.write_all(format!("{value}\n").as_bytes())
            .await
            .map_err(io_error)?;
        file.sync_all().await.map_err(io_error)
    }
    struct FileFactory {
        inner: Arc<Factory>,
        directory: std::path::PathBuf,
    }
    impl AdapterFactory for FileFactory {
        fn open<'a>(
            &'a self,
            context: &'a AdapterInitContext,
        ) -> PortFuture<'a, Arc<dyn AdapterInstance>> {
            Box::pin(async move {
                let inner = self.inner.open(context).await?;
                trace(&self.directory,json!({"kind":"open","pid":std::process::id(),"binding":context.execution.binding_set_id})).await?;
                Ok(Arc::new(FileInstance {
                    inner,
                    directory: self.directory.clone(),
                    closed: AtomicBool::new(false),
                }) as Arc<dyn AdapterInstance>)
            })
        }
    }
    struct FileInstance {
        inner: Arc<dyn AdapterInstance>,
        directory: std::path::PathBuf,
        closed: AtomicBool,
    }
    impl AdapterInstance for FileInstance {
        fn exports(&self) -> Vec<AdapterExportInstance> {
            self.inner
                .exports()
                .into_iter()
                .map(|export| match export {
                    AdapterExportInstance::Tool {
                        export_id,
                        descriptor,
                        executor,
                    } => AdapterExportInstance::Tool {
                        export_id,
                        descriptor,
                        executor: Arc::new(FileWriter {
                            inner: executor,
                            directory: self.directory.clone(),
                        }),
                    },
                    other => other,
                })
                .collect()
        }
        fn close<'a>(&'a self, context: &'a AdapterCloseContext) -> PortFuture<'a, ()> {
            Box::pin(async move {
                self.inner.close(context).await?;
                if !self.closed.swap(true, Ordering::SeqCst) {
                    trace(&self.directory,json!({"kind":"close","pid":std::process::id(),"binding":context.binding_set_id})).await?;
                }
                Ok(())
            })
        }
    }
    struct FileWriter {
        inner: Arc<dyn ToolExecutor>,
        directory: std::path::PathBuf,
    }
    impl ToolExecutor for FileWriter {
        fn execute<'a>(
            &'a self,
            args: &'a JsonObject,
            context: &'a ToolExecutionContext,
        ) -> PortFuture<'a, ToolExecutionResult> {
            Box::pin(async move {
                // The delegate validates the approval actor, segment and frozen inputs.
                let mut result = self.inner.execute(args, context).await?;
                if !matches!(&result.outcome, ToolExecutionOutcome::Succeeded { .. }) {
                    return Ok(result);
                }
                let report = json!({"query":args["query"],"workspace_id":args["workspace_id"],"record_id":args["record_id"]});
                let path = self.directory.join(format!(
                    "{}.json",
                    args["record_id"].as_str().expect("validated UUID")
                ));
                let mut file = tokio::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .await
                    .map_err(io_error)?;
                file.write_all(report.to_string().as_bytes())
                    .await
                    .map_err(io_error)?;
                file.sync_all().await.map_err(io_error)?;
                let digest = canonical_digest(&report);
                trace(&self.directory,json!({"kind":"write","pid":std::process::id(),"binding":context.binding_set_id,"digest":digest})).await?;
                result.receipt = Some(
                    json!({"effect_id":"report-file","record_id":args["record_id"],"content_hash":digest}),
                );
                Ok(result)
            })
        }
    }
    fn file_registry(
        scope: &Scope,
        factory: Arc<dyn AdapterFactory>,
    ) -> Result<AdapterRegistry, ContractError> {
        let definition = definition();
        let value = json!({"thread_id":"prepared-report-thread"});
        let state = AdapterBindingState {
            scope: scope.clone(),
            session_id: id("session"),
            adapter_binding: id("reports"),
            adapter: reference("report-adapter"),
            definition_digest: definition.digest(),
            state_ref: ProtectedRecord::new(id("prepared-mapping"), 1, value.clone())
                .reference()
                .clone(),
            value,
        };
        AdapterRegistry::new(
            scope.clone(),
            vec![AdapterRegistration {
                definition,
                factory,
            }],
            vec![ConnectionRegistration {
                binding: ConnectorBindingRef {
                    binding_id: id("data"),
                    connector_id: id("report-service"),
                    version: id("1"),
                },
                metadata: metadata(ComponentKind::Connector, "report-service"),
                connection_ref: reference("report-account"),
            }],
            vec![],
            vec![],
            vec![state],
        )
    }
    fn file_agent(
        scope: &Scope,
        store: Arc<SqliteStateStore>,
        model: Arc<Model>,
        resolver: Arc<Resolver>,
        counters: Arc<Counters>,
        directory: std::path::PathBuf,
    ) -> Result<(Agent, Arc<Catalog>), ContractError> {
        let registry = Arc::new(file_registry(
            scope,
            Arc::new(FileFactory {
                inner: Arc::new(Factory {
                    store: store.clone(),
                    counters,
                }),
                directory,
            }),
        )?);
        let catalog = Arc::new(Catalog {
            registry: registry.clone(),
            calls: AtomicUsize::new(0),
        });
        let policy = Arc::new(PolicyGate::new(Arc::new(Policy), Duration::from_secs(5))?);
        let clock = Arc::new(SystemClock::new());
        let runtime = Arc::new(AdapterRuntime::new(
            registry,
            store.clone(),
            policy.clone(),
            clock.clone(),
        ));
        // The Run budget includes both process lifetimes and the approval gap.
        // Each child has a separate 90-second wall watchdog below; this is a
        // recovery-contract example, not a 30-second end-to-end latency test.
        let profile = AgentProfile::from_json(
            r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"writer","version":"1",
        "name":"Writer","description":"Synthetic adapter consumer","instructions":{"text":"Write the report after authorization"},
        "model_binding":"primary","tools":[{"adapter_binding":"reports","export_id":"save","alias":"write"}],"skills":[],
        "connectors":[{"binding_id":"data","connector_id":"report-service","version":"1"}],
        "adapters":[{"binding_id":"reports","adapter_id":"report-adapter","version":"1","connections":{"main":"data"}}],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":3,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":120000}
    }"#,
        )?;
        Ok((
            create_agent(
                profile,
                AgentBindings {
                    scope: scope.clone(),
                    state: store,
                    policy: policy.clone(),
                    profile_resolver: catalog.clone(),
                    model_exchange: Arc::new(
                        ModelExchange::new(model, policy)
                            .with_route_inspector(Arc::new(Inspector), Duration::from_secs(5))?,
                    ),
                    router: Arc::new(PolicyModelRouter::new(routing(scope)?)?),
                    host_instructions: vec!["Use only authorized inputs.".into()],
                    system_inputs: system_inputs()?,
                    tools: None,
                    hooks: None,
                    components: Some(runtime),
                    context_sources: None,
                    context_token_estimator: None,
                    context_runtime: None,
                    verification: None,
                    skills: None,
                    artifacts: None,
                    system_input_resolver: Some(resolver),
                    external_receipt_verifier: None,
                    clock,
                    ids: Arc::new(RandomIdSource),
                    token_estimator: Arc::new(Estimate),
                    settings: AgentSettings {
                        require_durable: true,
                        // An abrupt wait-worker exit may retain the normal
                        // 30-second lease. Allow handoff to outlive that lease;
                        // the parent watchdog also includes execution/cleanup.
                        start_timeout_ms: 60_000,
                        max_output_tokens: 128.try_into().unwrap(),
                        ..Default::default()
                    },
                },
            )?,
            catalog,
        ))
    }

    async fn worker(
        directory: &std::path::Path,
        mode: &str,
    ) -> Result<std::process::ExitStatus, Box<dyn std::error::Error>> {
        let mut child = std::process::Command::new(std::env::current_exe()?)
            .arg(directory)
            .arg(mode)
            .spawn()?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        loop {
            if let Some(status) = child.try_wait()? {
                return Ok(status);
            }
            if tokio::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err("report worker watchdog expired".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        let args: Vec<_> = std::env::args_os().collect();
        if args.len() == 3 {
            return child(
                std::path::Path::new(&args[1]),
                args[2].to_str().ok_or("invalid mode")?,
            )
            .await;
        }
        let directory = TemporaryDatabase(
            std::env::temp_dir().join(format!("wickle-report-{}", RandomIdSource.next_id()?)),
        );
        std::fs::create_dir(&directory.0)?;
        assert_eq!(worker(&directory.0, "wait").await?.code(), Some(73));
        assert!(!directory.0.join(format!("{RECORD}.json")).exists());
        assert!(worker(&directory.0, "resume").await?.success());
        let report: Value =
            serde_json::from_slice(&std::fs::read(directory.0.join(format!("{RECORD}.json")))?)?;
        assert_eq!(
            report,
            json!({"query":"report","workspace_id":WORKSPACE,"record_id":RECORD})
        );
        assert!(!directory.0.join(format!("{NEW_RECORD}.json")).exists());
        let events: Vec<Value> = std::fs::read_to_string(directory.0.join("trace.jsonl"))?
            .lines()
            .map(parse_json)
            .collect::<Result<_, _>>()?;
        let opens: Vec<_> = events.iter().filter(|e| e["kind"] == "open").collect();
        let closes: Vec<_> = events.iter().filter(|e| e["kind"] == "close").collect();
        let writes: Vec<_> = events.iter().filter(|e| e["kind"] == "write").collect();
        assert_eq!(opens.len(), 2);
        assert_eq!(closes.len(), 2);
        assert_eq!(writes.len(), 1);
        assert_ne!(opens[0]["pid"], opens[1]["pid"]);
        assert_ne!(opens[0]["binding"], opens[1]["binding"]);
        for open in opens {
            assert!(closes.iter().any(|close|close["binding"]==open["binding"] && close["pid"]==open["pid"]));
        }
        assert_eq!(writes[0]["digest"], json!(canonical_digest(&report)));
        println!(
            "report consumer: real approval wait, abrupt process exit, new process/binding, one durable file write with original UUIDs, explicit release and duplicate resume without extra work passed"
        );
        Ok(())
    }
    async fn child(
        directory: &std::path::Path,
        mode: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !matches!(mode, "wait" | "resume") {
            return Err("invalid worker mode".into());
        }
        let scope = Scope {
            tenant_id: id("tenant"),
            workspace_id: id("workspace"),
            user_id: None,
        };
        let store = Arc::new(SqliteStateStore::open(directory.join("state.sqlite3"))?);
        let counters = Arc::new(Counters::default());
        let model = Arc::new(Model {
            route: routing(&scope)?.route_for_binding(&reference("primary"))?,
            propose: mode == "wait",
            calls: AtomicUsize::new(0),
        });
        let resolver = Arc::new(Resolver {
            value: if mode == "wait" { RECORD } else { NEW_RECORD },
            revision: if mode == "wait" {
                "record-A"
            } else {
                "record-B"
            },
            calls: AtomicUsize::new(0),
        });
        let (agent, _catalog) = file_agent(
            &scope,
            store.clone(),
            model.clone(),
            resolver.clone(),
            counters.clone(),
            directory.to_owned(),
        )?;
        if mode == "wait" {
            let caller = context(&scope, false);
            let request = RunRequest {
                request_id: id("request"),
                session_id: id("session"),
                input: vec![InputContent::Text {
                    text: "Write the report".into(),
                }],
                trigger: RunTrigger::User {},
                model_options: JsonObject::new(),
                output_contract: None,
            };
            let handle = completed(agent.start(request, caller.clone()).await?)?;
            assert_eq!(
                completed(handle.outcome(&caller).await?)?.result.status(),
                RunStatus::Waiting
            );
            release_finished(&handle, &caller).await?;
            let saved = store.load(&scope, handle.run_id()).await?;
            let wait = saved.snapshot.wait.ok_or("missing wait")?;
            let WaitTarget::Approval { target } = wait.target else {
                return Err("not an approval wait".into());
            };
            let command = ResumeCommand {
                run_id: handle.run_id().clone(),
                expected_revision: saved.snapshot.revision,
                command_id: id("approve-report"),
                action: ResumeAction::Approve {
                    wait_id: wait.wait_id,
                    target,
                },
            };
            tokio::fs::write(
                directory.join("command.json"),
                serde_json::to_vec(&command)?,
            )
            .await?;
            assert_eq!(counters.writes.load(Ordering::SeqCst), 0);
            std::process::exit(73);
        }
        let command =
            ResumeCommand::from_json(&std::fs::read_to_string(directory.join("command.json"))?)?;
        let reviewer = context(&scope, true);
        let handle = completed(agent.resume(command.clone(), reviewer.clone()).await?)?;
        let outcome = completed(handle.outcome(&reviewer).await?)?;
        assert_eq!(
            outcome.result.status(),
            RunStatus::Succeeded,
            "resume result: {:?}, usage: {:?}",
            outcome.result,
            outcome.usage
        );
        release_finished(&handle, &reviewer).await?;
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        assert_eq!(counters.writes.load(Ordering::SeqCst), 1);
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
        let replay = completed(agent.resume(command, reviewer.clone()).await?)?;
        assert_eq!(completed(replay.outcome(&reviewer).await?)?, outcome);
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        assert_eq!(counters.opens.load(Ordering::SeqCst), 1);
        assert_eq!(counters.closes.load(Ordering::SeqCst), 1);
        assert_eq!(counters.writes.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    host::run().await
}
```
