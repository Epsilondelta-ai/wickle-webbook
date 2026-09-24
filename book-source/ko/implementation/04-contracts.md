# 04장 전체 Rust 구현과 테스트

[강의로](../04-contracts.md) · [전체 변경 패치](../solutions/04-contracts.patch)

기준 `4c63cf3e202303e5b415c52c505f1f44d491053c`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle/src/context.rs`

```rust
use std::fmt;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    ContractError, Id, JsonObject,
    serialization::{decode, optional},
};

/// Boxed, Send future used by dynamically injected ports.
pub type PortFuture<'a, T> = futures_util::future::BoxFuture<'a, Result<T, ContractError>>;

/// Boxed, Send stream used by dynamically injected ports.
pub type PortStream<'a, T> = futures_util::stream::BoxStream<'a, Result<T, ContractError>>;

/// Opaque resource scope. Deserializing it does not authenticate a caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    /// Host tenant identifier.
    pub tenant_id: Id,
    /// Host workspace identifier.
    pub workspace_id: Id,
    /// Optional user scope; explicit null is rejected.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub user_id: Option<Id>,
}

/// Owned tool inputs supplied by the Host, redacted from Debug output.
/// Serialization is for protected storage, never automatic model projection.
#[derive(Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SystemInputs(JsonObject);

impl SystemInputs {
    /// Take ownership of the supplied input map.
    pub fn new(values: JsonObject) -> Self {
        Self(values)
    }
    /// Explicit access for an authorized binder or storage implementation.
    pub fn values(&self) -> &JsonObject {
        &self.0
    }
}

impl fmt::Debug for SystemInputs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SystemInputs(<redacted>)")
    }
}

/// Serializable Host context data, separate from cancellation and runtime clients.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionContextData {
    /// Authenticated resource scope supplied by the Host.
    pub scope: Scope,
    /// Host-authenticated principal reference.
    pub principal_ref: Id,
    /// Current capability grant reference, not a grant issued by the model.
    pub capability_grant_ref: Id,
    /// Optional tracing correlation data.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub trace_context: Option<JsonObject>,
    /// Missing means start defaults to empty, resume uses the stored snapshot.
    /// An explicit empty object is preserved; null is rejected.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub system_inputs: Option<SystemInputs>,
}

impl ExecutionContextData {
    /// Decode context data without authenticating it or creating runtime objects.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        decode(input, None)
    }
}

/// Host-owned runtime context; it cannot be constructed by deserializing JSON.
///
/// ```compile_fail
/// fn persist_runtime(context: &wickle::ExecutionContext) {
///     serde_json::to_string(context).unwrap();
/// }
/// ```
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    /// Owned, authenticated context data.
    pub data: ExecutionContextData,
    /// A runtime signal, never part of profile or checkpoint JSON.
    pub cancellation: CancellationToken,
}

impl ExecutionContext {
    /// Attach a Host-provided cancellation signal to owned context data.
    pub fn new(data: ExecutionContextData, cancellation: CancellationToken) -> Self {
        Self { data, cancellation }
    }
}
```

## `crates/wickle/src/error.rs`

```rust
use serde::{Deserialize, Serialize};

/// Stable categories for contract and profile validation failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorCode {
    /// Input is not unambiguous, finite JSON.
    InvalidJson,
    /// Input does not match a data contract.
    InvalidContract,
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
//! Model calls, tool dispatch, and the agent driver are not implemented yet.
//!
//! Runtime objects stay in Host code. Only documented data contracts are
//! serialized; successful decoding does not authenticate a caller.
//!
//! Internal modules are not extension points; use the root exports.
//! ```compile_fail
//! use wickle::serialization::canonical_digest;
//! ```

mod context;
mod error;
mod message;
mod model;
mod profile;
mod resolution;
mod run;
mod serialization;

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

## `crates/wickle/src/message.rs`

```rust
use crate::{Id, JsonDigest, JsonObject, Scope, serialization::optional};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

/// An authorized reference to immutable stored data, not the referenced payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordRef {
    /// Store-specific record identifier.
    pub record_id: Id,
    /// Exact stored revision.
    pub revision: u64,
    /// Digest of the referenced contract data.
    pub digest: JsonDigest,
}

/// Artifact metadata. Reading bytes still requires current scope authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    /// Artifact identifier.
    pub artifact_id: Id,
    /// Scope that owns the artifact.
    pub scope: Scope,
    /// Media type of the stored bytes.
    pub media_type: Id,
    /// Original byte length.
    pub size_bytes: u64,
    /// Store-defined content hash, separate from JSON contract digests.
    pub content_hash: Id,
}

/// Provenance for a source passage or fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceRef {
    /// Source identifier.
    pub source_id: Id,
    /// Exact source version/revision.
    pub version: Id,
    /// Source-specific passage location.
    pub location: Id,
    /// Original source content hash.
    pub content_hash: Id,
    /// Optional quoted passage.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub quote: Option<String>,
}

/// User-supplied or final-output content; cannot inject tool calls or provider state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum InputContent {
    /// Text content.
    Text {
        /// Text body.
        text: String,
    },
    /// JSON data, not executable objects.
    Json {
        /// JSON value; explicit JSON null is valid content.
        value: serde_json::Value,
    },
    /// Artifact metadata.
    Artifact {
        /// Artifact reference.
        reference: ArtifactRef,
    },
    /// Evidence metadata.
    Evidence {
        /// Source reference.
        reference: EvidenceRef,
    },
}

/// Model-owned tool arguments and provenance, separate from system execution inputs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolCall {
    /// Core call identifier.
    pub call_id: Id,
    /// Original model request identifier.
    pub model_request_id: Id,
    /// Provider-local call identifier, scoped by model_request_id.
    pub provider_call_id: Id,
    /// Model-facing tool name.
    pub tool_name: Id,
    /// Original model-supplied inputs, never replaced with execution_args.
    pub model_inputs: JsonObject,
    /// Pinned descriptor identity.
    pub descriptor_digest: JsonDigest,
    /// Protected bound-input record, once binding succeeds.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub bound_input_ref: Option<RecordRef>,
}

/// Outcome of one tool dispatch or a pre-dispatch denial.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolResultStatus {
    /// Tool completed successfully.
    Succeeded,
    /// Tool failed with a classified error.
    Failed,
    /// Policy or validation denied execution.
    Denied,
    /// Execution was cancelled.
    Cancelled,
    /// External effect status is not known.
    Unknown,
}

/// A safe structured failure; raw SDK errors belong in protected Host diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Failure {
    /// Registered error/reason code.
    pub code: Id,
    /// Optional protected diagnostic reference.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub diagnostic_ref: Option<RecordRef>,
}

/// Tool observation explicitly paired with its call message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolResult {
    /// Core call identifier.
    pub call_id: Id,
    /// Message containing the matching call.
    pub call_message_id: Id,
    /// Explicit execution status.
    pub status: ToolResultStatus,
    /// Bounded model-visible observations or references.
    pub content: Vec<InputContent>,
    /// Protected receipt, retained even if output processing fails.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub effect_receipt_ref: Option<RecordRef>,
    /// Classified failure, when applicable.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub error: Option<Failure>,
}

/// Transcript content. Provider replay data is a scoped, protected reference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContentBlock {
    /// Ordinary displayable content.
    Content {
        /// Text, JSON, artifact, or evidence.
        content: InputContent,
    },
    /// A complete model tool call.
    ToolCall {
        /// Call and model-only arguments.
        call: ToolCall,
    },
    /// Paired tool observation.
    ToolResult {
        /// Observation and receipt reference.
        result: ToolResult,
    },
    /// Opaque continuation data bound to one provider/route.
    ProviderOpaque {
        /// Registered provider key.
        provider: Id,
        /// Route that can interpret the protected block.
        route_digest: JsonDigest,
        /// Protected replay-data record.
        data_ref: RecordRef,
    },
}

/// Logical message role before provider-specific projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    /// Host/profile instruction role.
    System,
    /// User input.
    User,
    /// Model output.
    Assistant,
    /// Tool observation.
    Tool,
}

/// Provenance of content; a wire role does not grant authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageOrigin {
    /// Trusted Host instructions.
    Host,
    /// Pinned profile instructions.
    Profile,
    /// User input.
    User,
    /// Model response.
    Model,
    /// Loaded skill data.
    Skill,
    /// Tool observation.
    Tool,
    /// Retrieved external data.
    Retrieval,
    /// Retrieved memory.
    Memory,
    /// Verifier feedback.
    Verification,
    /// Synthetic recovery bookkeeping.
    Recovery,
}

/// Intended projection surfaces; authorization is still enforced at use time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    /// Protected execution data only.
    Internal,
    /// Model projection only.
    Model,
    /// User presentation only.
    User,
    /// Both model projection and user presentation.
    UserAndModel,
}

/// An original transcript record, not a provider request or UI event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    /// Unique message identity.
    pub message_id: Id,
    /// Owning run.
    pub run_id: Id,
    /// Monotonic stored message sequence.
    pub sequence: NonZeroU64,
    /// Logical role.
    pub role: MessageRole,
    /// Original content blocks.
    pub content: Vec<ContentBlock>,
    /// Source provenance.
    pub origin: MessageOrigin,
    /// Intended projections.
    pub visibility: Visibility,
}
```

## `crates/wickle/src/model.rs`

```rust
use crate::{
    Id, JsonDigest, JsonObject, Scope, VersionedRef,
    serialization::{data_digest, optional},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, num::NonZeroU64};

/// Logical purpose of a model call; all purposes consume the run's model budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelPurpose {
    /// Agent reasoning.
    Agent,
    /// Candidate verification.
    Verification,
    /// Context compression.
    Compaction,
}

/// Version semantics declared by trusted metadata, never inferred from a name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionSemantics {
    /// Immutable model and target.
    Pinned,
    /// Alias that can point to another release.
    Alias,
    /// Deployment that can change independently of its name.
    MutableDeployment,
    /// Immutability has not been verified.
    Unverified,
}

/// Host routing constraint on mutable model targets.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionPolicy {
    /// Only verified immutable targets are eligible.
    #[default]
    RequirePinned,
    /// The Host explicitly permits mutable targets.
    AllowMutable,
}

/// Provider protocol identity, separate from model release and deployment names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiContract {
    /// Protocol operation, such as messages or generateContent.
    pub operation: Id,
    /// Exact API contract/header version.
    pub version: Id,
}

/// Immutable selection data for one model target. Provider keys are extensible.
/// Provider adapters validate target fields; credentials live in Host bindings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedModelRoute {
    /// Exact Host binding revision.
    pub binding: VersionedRef,
    /// Catalog snapshot revision.
    pub catalog_revision: Id,
    /// Routing policy snapshot revision.
    pub routing_policy_revision: Id,
    /// Original model or alias requested by routing policy.
    pub requested_model: Id,
    /// Resolved provider model identifier.
    pub model_id: Id,
    /// Exact opaque release/version string.
    pub model_version: Id,
    /// Declared version semantics.
    pub version_semantics: VersionSemantics,
    /// Registered service key, not a closed list of vendors.
    pub provider: Id,
    /// Nonsecret target metadata validated by the selected adapter.
    pub target: JsonObject,
    /// Optional independent deployment revision.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub deployment_revision: Option<Id>,
    /// API operation and version.
    pub api_contract: ApiContract,
    /// Exact adapter implementation version.
    pub adapter: VersionedRef,
    /// Revision of validated capabilities for this exact combination.
    pub capability_revision: Id,
    /// Host connection reference/revision; never a raw credential.
    pub connection_ref: VersionedRef,
}

impl ResolvedModelRoute {
    /// Identity of every selected route field; computed to avoid stale stored hashes.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }
}

/// A classified model failure, before any retry/fallback policy is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelFailureKind {
    /// Provider did not respond within the call deadline.
    Timeout,
    /// Provider throttled the call.
    RateLimited,
    /// Transport failed.
    Transport,
    /// Response could not be interpreted safely.
    Protocol,
    /// Input exceeded model context limits.
    ContextOverflow,
    /// Authentication failed.
    Authentication,
    /// Required functionality is unsupported.
    Unsupported,
}

/// Selection request; the router returns data and does not invoke a model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteRequest {
    /// Profile's Host binding/routing configuration name.
    pub model_binding: Id,
    /// Purpose of this call.
    pub purpose: ModelPurpose,
    /// Features required by the projected input.
    pub required_capabilities: BTreeSet<Id>,
    /// Estimated input tokens, distinct from reported usage.
    pub input_tokens: u64,
    /// Reserved output tokens.
    pub max_output_tokens: NonZeroU64,
    /// Authenticated routing/data scope.
    pub scope: Scope,
    /// Explicitly allowed binding names.
    pub allowed_bindings: Vec<Id>,
    /// Default is require_pinned.
    #[serde(default)]
    pub version_policy: VersionPolicy,
    /// Prior choice, if evaluating an explicit fallback.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub previous_route: Option<ResolvedModelRoute>,
    /// Classified reason for considering another route.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub previous_failure: Option<ModelFailureKind>,
}

/// Whether token counts were measured by the provider or estimated by the Host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageMeasurement {
    /// Reported by the provider.
    Reported,
    /// Estimated locally.
    Estimated,
}

/// Model token usage. Missing counts remain unknown, not zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelUsage {
    /// Provenance of the counts.
    pub measurement: UsageMeasurement,
    /// Input tokens when known.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub input_tokens: Option<u64>,
    /// Output tokens when known.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub output_tokens: Option<u64>,
}

/// Reservation/result state of one physical model attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelAttemptState {
    /// Budget was reserved before dispatch.
    Reserved {},
    /// A complete response was recorded.
    Completed {},
    /// A classified failure was recorded.
    Failed {
        /// Failure classification, without raw request/response data.
        kind: ModelFailureKind,
    },
    /// Dispatch/result is not yet known after interruption.
    Unknown {},
}

/// Durable record of one physical invocation and the selected model version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelInvocationRecord {
    /// Owning run.
    pub run_id: Id,
    /// Logical model step, stable across transport retries.
    pub model_step_id: Id,
    /// Unique physical attempt.
    pub attempt_id: Id,
    /// Purpose charged to the same run budget.
    pub purpose: ModelPurpose,
    /// Selected route, including all relevant versions.
    pub route: ResolvedModelRoute,
    /// Host-defined selection reason code.
    pub selection_reason: Id,
    /// Identity of the projected model request.
    pub request_digest: JsonDigest,
    /// Invocation state.
    pub state: ModelAttemptState,
    /// Provider correlation identifier, when reported.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub provider_request_id: Option<Id>,
    /// Model actually reported by the response; never filled from the requested ID.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub reported_model_id: Option<Id>,
    /// Version actually reported by the response.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub reported_model_version: Option<Id>,
    /// Missing usage is unknown.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub usage: Option<ModelUsage>,
}
```

## `crates/wickle/src/profile.rs`

```rust
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
};

use serde::{Deserialize, Serialize};

use crate::{
    ContractError, ErrorCode, Id, JsonDigest, JsonObject,
    serialization::{data_digest, decode, optional},
};

/// Current profile wire format; independent of the crate version.
pub const PROFILE_SCHEMA_VERSION: &str = "wickle.agent-profile.v1";

/// Supported profile wire versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProfileSchemaVersion {
    /// The first Wickle profile format.
    #[serde(rename = "wickle.agent-profile.v1")]
    V1,
}

/// A reference to an exact, opaque asset or definition version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionedRef {
    /// Registered identifier.
    pub id: Id,
    /// Exact version, without provider-specific parsing.
    pub version: Id,
}

/// Inline instructions or a pinned instruction asset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Instructions {
    /// Literal instruction text, never executed as code.
    Text(InstructionText),
    /// A registered versioned asset.
    Asset(InstructionAsset),
}

/// Literal instruction data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstructionText {
    /// Profile instructions, subordinate to Host policies.
    pub text: String,
}

/// Reference to instruction data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstructionAsset {
    /// Pinned asset reference.
    pub asset_ref: VersionedRef,
}

/// One catalog tool or one selected adapter export; the forms cannot be mixed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ToolBindingRef {
    /// A catalog tool at an exact version.
    Catalog(CatalogToolRef),
    /// A selected adapter export.
    Export(ExportRef),
}

/// Configuration for a registered catalog tool.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogToolRef {
    /// Catalog identifier.
    pub tool_id: Id,
    /// Exact tool version.
    pub version: Id,
    /// Named connector references, never connection credentials.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub bindings: Option<BTreeMap<Id, Id>>,
    /// Nonsecret configuration validated against the registered schema.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub config: Option<JsonObject>,
}

/// An explicitly selected export from a profile adapter binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportRef {
    /// Adapter binding in the same profile.
    pub adapter_binding: Id,
    /// Export identifier in the adapter definition.
    pub export_id: Id,
    /// Optional model-facing name, not an authorization identity.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub alias: Option<Id>,
}

/// A skill and its nonsecret settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillRef {
    /// Catalog skill identifier.
    pub skill_id: Id,
    /// Exact skill version.
    pub version: Id,
    /// Settings checked by the registered skill schema.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub config: Option<JsonObject>,
}

/// A connection identity whose credentials and endpoint are supplied by the Host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorBindingRef {
    /// Name used by tools and adapters in this profile.
    pub binding_id: Id,
    /// Registered connector identifier.
    pub connector_id: Id,
    /// Exact connector contract version.
    pub version: Id,
}

/// A trusted adapter definition selected by data references.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterBindingRef {
    /// Local binding name.
    pub binding_id: Id,
    /// Registered adapter identifier; not a module or executable path.
    pub adapter_id: Id,
    /// Exact adapter version.
    pub version: Id,
    /// Nonsecret adapter settings.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub config: Option<JsonObject>,
    /// Required connection name to profile connector binding mapping.
    pub connections: BTreeMap<Id, Id>,
}

/// Lifecycle positions supported by the core contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookPosition {
    /// Once after admission.
    BeforeRun,
    /// Before a logical model step.
    BeforeModel,
    /// Before binding model-owned tool inputs.
    BeforeTool,
    /// After a tool result is committed.
    AfterTool,
    /// After a terminal outcome is committed.
    AfterRun,
}

/// A catalog hook at a fixed lifecycle position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogHookRef {
    /// Registered hook identifier.
    pub hook_id: Id,
    /// Exact hook version.
    pub version: Id,
    /// Requested position, checked against metadata.
    pub position: HookPosition,
}

/// An explicit hook selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HookRef {
    /// A registered standalone hook.
    Catalog(CatalogHookRef),
    /// An adapter export with its registered position.
    Export(ExportRef),
}

/// A standalone source reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogSourceRef {
    /// Registered source identifier.
    pub source_id: Id,
    /// Exact source version.
    pub version: Id,
}

/// A source from the catalog or an adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ContextSourceRef {
    /// Catalog source.
    Catalog(CatalogSourceRef),
    /// Adapter source export.
    Export(ExportRef),
}

/// Automatic context collection points.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextTrigger {
    /// Once per run.
    #[default]
    RunStart,
    /// Once per logical model step.
    BeforeModel,
}

/// A context source with explicit finite limits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSourceBinding {
    /// Selected source.
    pub source: ContextSourceRef,
    /// Defaults to run_start.
    #[serde(default)]
    pub trigger: ContextTrigger,
    /// Whether unavailability stops the run; defaults to false.
    #[serde(default)]
    pub required: bool,
    /// Maximum call duration in milliseconds.
    pub timeout_ms: NonZeroU64,
    /// Maximum returned items.
    pub max_items: NonZeroU64,
    /// Maximum returned bytes.
    pub max_bytes: NonZeroU64,
    /// Maximum estimated tokens.
    pub max_tokens: NonZeroU64,
}

/// Context strategy selection. Custom strategies require an exact version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextPolicy {
    /// The built-in bounded strategy or a registered strategy identifier.
    pub strategy: Id,
    /// Required for registered custom strategies.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub version: Option<Id>,
    /// Nonsecret strategy configuration.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub config: Option<JsonObject>,
}

/// The condition that makes a candidate a successful outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompletionPolicy {
    /// The model ends its turn; this does not verify external business success.
    TurnEnd {},
    /// An explicitly selected verifier must accept the candidate.
    Verified {
        /// Exact verifier definition.
        verifier_ref: VersionedRef,
    },
}

impl Default for CompletionPolicy {
    fn default() -> Self {
        Self::TurnEnd {}
    }
}

/// Expected final output representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutputContract {
    /// Text output.
    Text {},
    /// Structured output checked against a registered schema asset.
    JsonSchema {
        /// Exact schema asset.
        schema_ref: VersionedRef,
    },
}

/// Finite execution budgets. Zero tool, repair, or recovery attempts prohibit them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunLimits {
    /// All physical model calls, including verification and compaction.
    pub max_model_calls: NonZeroU64,
    /// Physical tool dispatch attempts; zero disables tools.
    pub max_tool_attempts: u64,
    /// Candidate repair attempts; zero disables repair.
    pub max_repair_attempts: u64,
    /// Recovery attempts; zero disables recovery.
    pub max_recovery_attempts: u64,
    /// Wall elapsed time since admission, including waits, in milliseconds.
    pub max_elapsed_ms: NonZeroU64,
}

/// Serializable agent configuration. It cannot contain runtime trait objects.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentProfile {
    /// Profile wire version.
    pub schema_version: ProfileSchemaVersion,
    /// Stable agent identity.
    pub agent_id: Id,
    /// Immutable profile version.
    pub version: Id,
    /// Display name.
    pub name: String,
    /// Work description.
    pub description: String,
    /// Instructions or an exact asset reference.
    pub instructions: Instructions,
    /// Host-registered model binding or routing configuration name.
    pub model_binding: Id,
    /// Selected tools; an explicit empty list is permitted.
    pub tools: Vec<ToolBindingRef>,
    /// Selected skills; an explicit empty list is permitted.
    pub skills: Vec<SkillRef>,
    /// Host connection references, never credentials.
    pub connectors: Vec<ConnectorBindingRef>,
    /// Optional adapter selections. Missing and empty remain distinct data.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub adapters: Option<Vec<AdapterBindingRef>>,
    /// Optional automatic context sources.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub context_sources: Option<Vec<ContextSourceBinding>>,
    /// Optional lifecycle hook selections.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub hooks: Option<Vec<HookRef>>,
    /// Context selection policy.
    pub context_policy: ContextPolicy,
    /// Defaults to turn_end.
    #[serde(default)]
    pub completion_policy: CompletionPolicy,
    /// Final output requirements.
    pub output_contract: OutputContract,
    /// Finite execution limits.
    pub limits: RunLimits,
    /// Namespaced data checked by registered extension schemas.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub extensions: Option<BTreeMap<Id, serde_json::Value>>,
}

impl AgentProfile {
    /// Decode a strict profile and validate its internal references.
    /// External availability and capabilities are checked by ProfileValidator.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        let profile: Self = decode(input, Some(PROFILE_SCHEMA_VERSION))?;
        profile.validate_structure()?;
        Ok(profile)
    }

    /// Digest the complete profile, including versions and optional-field presence.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }

    /// Check profile-local bindings without opening adapters or contacting models.
    pub fn validate_structure(&self) -> Result<(), ContractError> {
        let invalid = |path| ContractError::new(ErrorCode::InvalidReference, path);
        let mut connectors = BTreeSet::new();
        for item in &self.connectors {
            if !connectors.insert(&item.binding_id) {
                return Err(invalid("connectors.binding_id"));
            }
        }
        let mut adapters = BTreeSet::new();
        for item in self.adapters.iter().flatten() {
            if !adapters.insert(&item.binding_id) {
                return Err(invalid("adapters.binding_id"));
            }
            if item.connections.values().any(|id| !connectors.contains(id)) {
                return Err(invalid("adapters.connections"));
            }
        }
        let check_export = |item: &ExportRef| {
            if !adapters.contains(&item.adapter_binding) {
                Err(invalid("adapter_binding"))
            } else {
                Ok(())
            }
        };
        for tool in &self.tools {
            match tool {
                ToolBindingRef::Catalog(item) => {
                    if item
                        .bindings
                        .iter()
                        .flat_map(|v| v.values())
                        .any(|id| !connectors.contains(id))
                    {
                        return Err(invalid("tools.bindings"));
                    }
                }
                ToolBindingRef::Export(item) => check_export(item)?,
            }
        }
        for source in self.context_sources.iter().flatten() {
            if let ContextSourceRef::Export(item) = &source.source {
                check_export(item)?;
            }
        }
        for hook in self.hooks.iter().flatten() {
            if let HookRef::Export(item) = hook {
                check_export(item)?;
            }
        }
        if self.context_policy.strategy.as_str() == "bounded" {
            if self.context_policy.version.is_some()
                || self
                    .context_policy
                    .config
                    .as_ref()
                    .is_some_and(|c| !c.is_empty())
            {
                return Err(invalid("context_policy"));
            }
        } else if self.context_policy.version.is_none() {
            return Err(invalid("context_policy.version"));
        }
        for namespace in self.extensions.iter().flat_map(|items| items.keys()) {
            if !namespace.as_str().contains('.') || namespace.as_str().split('.').any(str::is_empty)
            {
                return Err(invalid("extensions.namespace"));
            }
        }
        Ok(())
    }
}
```

## `crates/wickle/src/resolution.rs`

```rust
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::{
    AgentProfile, CompletionPolicy, ContextSourceRef, ContractError, ErrorCode, ExportRef,
    HookPosition, HookRef, Id, Instructions, JsonDigest, JsonObject, OutputContract, PortFuture,
    Scope, ToolBindingRef, VersionedRef,
    serialization::{data_digest, optional},
};

/// Kinds of metadata resolved before execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentKind {
    /// Host model binding or routing configuration.
    ModelBinding,
    /// Standalone tool definition.
    Tool,
    /// Skill manifest.
    Skill,
    /// Connector contract.
    Connector,
    /// Adapter manifest.
    Adapter,
    /// Standalone context source.
    ContextSource,
    /// Lifecycle hook.
    Hook,
    /// Custom context strategy.
    ContextStrategy,
    /// Candidate verifier.
    Verifier,
    /// Pinned instruction data.
    InstructionAsset,
    /// Output schema asset.
    OutputSchema,
    /// Registered namespace and extension data schema.
    Extension,
}

/// A metadata lookup. Only model bindings and extension schemas may omit a version.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentRef {
    /// Expected component kind.
    pub kind: ComponentKind,
    /// Registered identifier.
    pub id: Id,
    /// Exact version or a Host-resolved binding/schema revision.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub version: Option<Id>,
}

/// Export kinds declared by trusted adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportKind {
    /// Callable tool.
    Tool,
    /// Automatic context source.
    ContextSource,
    /// Lifecycle hook.
    Hook,
    /// Host-owned event consumer, not activated by a profile.
    EventConsumer,
}

/// Metadata for one selectable adapter export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportMetadata {
    /// Adapter-local export identifier.
    pub export_id: Id,
    /// Implemented contract kind.
    pub kind: ExportKind,
    /// Metadata contract version; currently 1.
    pub contract_version: u32,
    /// Already-normalized tool name when this export is a tool.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub model_name: Option<Id>,
    /// Fixed position when this export is a hook.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub hook_position: Option<HookPosition>,
    /// Capabilities supplied only when this export is selected.
    pub capabilities: BTreeSet<Id>,
    /// Capabilities required by this selection.
    pub required_capabilities: BTreeSet<Id>,
}

/// Trusted, immutable metadata. It contains no factories, SDK clients, or credentials.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentMetadata {
    /// Resolved kind, identifier, and exact version. The version must be present.
    pub reference: ComponentRef,
    /// Metadata contract version; currently 1.
    pub contract_version: u32,
    /// Registered manifest digest.
    pub manifest_digest: JsonDigest,
    /// Draft 2020-12 schema for nonsecret configuration data.
    pub config_schema: Value,
    /// Other components that must be explicitly selected in the profile.
    pub dependencies: Vec<ComponentRef>,
    /// Capabilities provided by a standalone component.
    pub capabilities: BTreeSet<Id>,
    /// Capabilities that must be provided by the selected components/exports.
    pub required_capabilities: BTreeSet<Id>,
    /// Required names in the tool/adapter connection mapping.
    pub required_connections: BTreeSet<Id>,
    /// Already-normalized name for a catalog tool.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub model_name: Option<Id>,
    /// Position implemented by a catalog hook.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub hook_position: Option<HookPosition>,
    /// Declared adapter exports; declaration alone does not activate them.
    pub exports: Vec<ExportMetadata>,
}

/// Host-injected, scope-aware metadata resolution. No component is opened here.
pub trait ProfileResolver: Send + Sync {
    /// Resolve exactly the requested component, or return a typed error.
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        scope: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata>;
}

/// Frozen reference and definition identity recorded with a resolved profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedComponent {
    /// Exact kind, identity, and version.
    pub reference: ComponentRef,
    /// Hash of the full metadata, including config schema and capabilities.
    pub definition_digest: JsonDigest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolvedProfileData {
    profile: AgentProfile,
    scope: Scope,
    profile_digest: JsonDigest,
    components: Vec<ResolvedComponent>,
    resolution_digest: JsonDigest,
}

/// Immutable, scope-bound profile data, separate from runtime component instances.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ResolvedProfile(ResolvedProfileData);

impl<'de> Deserialize<'de> for ResolvedProfile {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let resolved = Self(ResolvedProfileData::deserialize(deserializer)?);
        resolved
            .validate_snapshot()
            .map_err(serde::de::Error::custom)?;
        Ok(resolved)
    }
}

impl ResolvedProfile {
    /// The pinned profile data.
    pub fn profile(&self) -> &AgentProfile {
        &self.0.profile
    }
    /// The scope used for reference resolution.
    pub fn scope(&self) -> &Scope {
        &self.0.scope
    }
    /// Identity of the original profile data.
    pub fn profile_digest(&self) -> &JsonDigest {
        &self.0.profile_digest
    }
    /// Identity of both the profile and resolved metadata in its scope.
    pub fn resolution_digest(&self) -> &JsonDigest {
        &self.0.resolution_digest
    }
    /// Exact component identities retained for later binding or recovery.
    pub fn components(&self) -> &[ResolvedComponent] {
        &self.0.components
    }

    /// Reject a different facade profile or scope before using an existing run.
    pub fn ensure_matches(
        &self,
        profile: &AgentProfile,
        scope: &Scope,
    ) -> Result<(), ContractError> {
        if profile.digest() != self.0.profile_digest || scope != &self.0.scope {
            return Err(ContractError::new(
                ErrorCode::ProfileMismatch,
                "resolved_profile",
            ));
        }
        Ok(())
    }

    /// Reject a newly resolved version or metadata snapshot replacing the saved one.
    pub fn ensure_same_resolution(&self, other: &Self) -> Result<(), ContractError> {
        if self.0.resolution_digest != other.0.resolution_digest {
            return Err(ContractError::new(
                ErrorCode::ProfileMismatch,
                "resolution_digest",
            ));
        }
        Ok(())
    }

    fn validate_snapshot(&self) -> Result<(), ContractError> {
        self.0.profile.validate_structure()?;
        let unique: BTreeSet<_> = self.0.components.iter().map(|c| &c.reference).collect();
        if self.0.profile.digest() != self.0.profile_digest
            || unique.len() != self.0.components.len()
            || self
                .0
                .components
                .iter()
                .any(|c| c.reference.version.is_none())
            || resolution_digest(&self.0.profile_digest, &self.0.scope, &self.0.components)
                != self.0.resolution_digest
        {
            return Err(ContractError::new(
                ErrorCode::InvalidSnapshot,
                "resolved_profile",
            ));
        }
        Ok(())
    }
}

fn resolution_digest(
    profile: &JsonDigest,
    scope: &Scope,
    components: &[ResolvedComponent],
) -> JsonDigest {
    data_digest(&(profile, scope, components))
}

/// Validates structure, scoped references, schemas, and required capabilities.
/// This runtime service is not serializable and never constructs tool handlers.
pub struct ProfileValidator<'a> {
    resolver: &'a dyn ProfileResolver,
}

impl<'a> ProfileValidator<'a> {
    /// Use a trusted Host metadata resolver.
    pub fn new(resolver: &'a dyn ProfileResolver) -> Self {
        Self { resolver }
    }

    /// Resolve and validate a profile before admission. No model or tool is run.
    pub async fn validate(
        &self,
        profile: &AgentProfile,
        scope: &Scope,
    ) -> Result<ResolvedProfile, ContractError> {
        profile.validate_structure()?;
        let mut checks = Checks::new(self.resolver, scope);
        checks
            .component(
                ComponentKind::ModelBinding,
                &profile.model_binding,
                None,
                None,
                None,
            )
            .await?;
        for item in &profile.connectors {
            checks
                .component(
                    ComponentKind::Connector,
                    &item.connector_id,
                    Some(&item.version),
                    None,
                    None,
                )
                .await?;
        }
        for item in profile.adapters.iter().flatten() {
            let metadata = checks
                .component(
                    ComponentKind::Adapter,
                    &item.adapter_id,
                    Some(&item.version),
                    item.config.as_ref(),
                    Some(&item.connections),
                )
                .await?;
            checks.adapters.insert(item.binding_id.clone(), metadata);
        }
        for item in &profile.tools {
            match item {
                ToolBindingRef::Catalog(item) => {
                    let metadata = checks
                        .component(
                            ComponentKind::Tool,
                            &item.tool_id,
                            Some(&item.version),
                            item.config.as_ref(),
                            item.bindings.as_ref(),
                        )
                        .await?;
                    checks.tool_name(
                        metadata
                            .model_name
                            .as_ref()
                            .ok_or_else(|| invalid("tools.model_name"))?,
                    )?;
                }
                ToolBindingRef::Export(item) => checks.export(item, ExportKind::Tool)?,
            }
        }
        for item in &profile.skills {
            checks
                .component(
                    ComponentKind::Skill,
                    &item.skill_id,
                    Some(&item.version),
                    item.config.as_ref(),
                    None,
                )
                .await?;
        }
        for item in profile.context_sources.iter().flatten() {
            match &item.source {
                ContextSourceRef::Catalog(item) => {
                    checks
                        .component(
                            ComponentKind::ContextSource,
                            &item.source_id,
                            Some(&item.version),
                            None,
                            None,
                        )
                        .await?;
                }
                ContextSourceRef::Export(item) => checks.export(item, ExportKind::ContextSource)?,
            }
        }
        for item in profile.hooks.iter().flatten() {
            match item {
                HookRef::Catalog(item) => {
                    let metadata = checks
                        .component(
                            ComponentKind::Hook,
                            &item.hook_id,
                            Some(&item.version),
                            None,
                            None,
                        )
                        .await?;
                    if metadata.hook_position != Some(item.position) {
                        return Err(invalid("hooks.position"));
                    }
                }
                HookRef::Export(item) => checks.export(item, ExportKind::Hook)?,
            }
        }
        if let Instructions::Asset(item) = &profile.instructions {
            checks
                .asset(ComponentKind::InstructionAsset, &item.asset_ref)
                .await?;
        }
        if let CompletionPolicy::Verified { verifier_ref } = &profile.completion_policy {
            checks.asset(ComponentKind::Verifier, verifier_ref).await?;
        }
        if let OutputContract::JsonSchema { schema_ref } = &profile.output_contract {
            checks
                .asset(ComponentKind::OutputSchema, schema_ref)
                .await?;
        }
        if let Some(version) = &profile.context_policy.version {
            checks
                .component(
                    ComponentKind::ContextStrategy,
                    &profile.context_policy.strategy,
                    Some(version),
                    profile.context_policy.config.as_ref(),
                    None,
                )
                .await?;
        }
        for (namespace, value) in profile.extensions.iter().flatten() {
            let reference = ComponentRef {
                kind: ComponentKind::Extension,
                id: namespace.clone(),
                version: None,
            };
            let metadata = checks.resolve(reference).await?;
            validate_configuration(&metadata.config_schema, value)?;
        }
        checks.finish()?;
        let components: Vec<_> = checks
            .definitions
            .values()
            .map(|definition| ResolvedComponent {
                reference: definition.reference.clone(),
                definition_digest: data_digest(definition),
            })
            .collect();
        let digest = profile.digest();
        Ok(ResolvedProfile(ResolvedProfileData {
            profile: profile.clone(),
            scope: scope.clone(),
            resolution_digest: resolution_digest(&digest, scope, &components),
            profile_digest: digest,
            components,
        }))
    }
}

struct Checks<'a> {
    resolver: &'a dyn ProfileResolver,
    scope: &'a Scope,
    definitions: BTreeMap<ComponentRef, ComponentMetadata>,
    adapters: BTreeMap<Id, ComponentMetadata>,
    capabilities: BTreeSet<Id>,
    required: BTreeSet<Id>,
    names: BTreeSet<Id>,
    selected_exports: BTreeSet<(Id, Id)>,
}

impl<'a> Checks<'a> {
    fn new(resolver: &'a dyn ProfileResolver, scope: &'a Scope) -> Self {
        Self {
            resolver,
            scope,
            definitions: BTreeMap::new(),
            adapters: BTreeMap::new(),
            capabilities: BTreeSet::new(),
            required: BTreeSet::new(),
            names: BTreeSet::new(),
            selected_exports: BTreeSet::new(),
        }
    }

    async fn resolve(
        &mut self,
        requested: ComponentRef,
    ) -> Result<ComponentMetadata, ContractError> {
        if let Some(existing) = self.definitions.get(&requested) {
            return Ok(existing.clone());
        }
        let metadata = self.resolver.resolve(&requested, self.scope).await?;
        if metadata.reference.kind != requested.kind
            || metadata.reference.id != requested.id
            || metadata.reference.version.is_none()
            || requested
                .version
                .as_ref()
                .is_some_and(|version| Some(version) != metadata.reference.version.as_ref())
        {
            return Err(ContractError::new(
                ErrorCode::ComponentUnavailable,
                "component.reference",
            ));
        }
        if metadata.contract_version != 1 {
            return Err(ContractError::new(
                ErrorCode::UnsupportedContractVersion,
                "component.contract_version",
            ));
        }
        let mut export_ids = BTreeSet::new();
        if metadata
            .exports
            .iter()
            .any(|e| !export_ids.insert(&e.export_id))
        {
            return Err(invalid("adapter.exports"));
        }
        self.required
            .extend(metadata.required_capabilities.iter().cloned());
        if requested.kind != ComponentKind::Adapter {
            self.capabilities
                .extend(metadata.capabilities.iter().cloned());
        }
        self.definitions.insert(requested, metadata.clone());
        Ok(metadata)
    }

    async fn component(
        &mut self,
        kind: ComponentKind,
        id: &Id,
        version: Option<&Id>,
        config: Option<&JsonObject>,
        connections: Option<&BTreeMap<Id, Id>>,
    ) -> Result<ComponentMetadata, ContractError> {
        let metadata = self
            .resolve(ComponentRef {
                kind,
                id: id.clone(),
                version: version.cloned(),
            })
            .await?;
        validate_configuration(
            &metadata.config_schema,
            &serde_json::to_value(config.cloned().unwrap_or_default()).expect("JSON object"),
        )?;
        if metadata
            .required_connections
            .iter()
            .any(|key| !connections.is_some_and(|c| c.contains_key(key)))
        {
            return Err(invalid("component.connections"));
        }
        Ok(metadata)
    }

    async fn asset(
        &mut self,
        kind: ComponentKind,
        reference: &VersionedRef,
    ) -> Result<(), ContractError> {
        self.component(kind, &reference.id, Some(&reference.version), None, None)
            .await
            .map(|_| ())
    }

    fn tool_name(&mut self, name: &Id) -> Result<(), ContractError> {
        if !self.names.insert(name.clone()) {
            return Err(invalid("tools.model_name"));
        }
        Ok(())
    }

    fn export(&mut self, reference: &ExportRef, kind: ExportKind) -> Result<(), ContractError> {
        let adapter = self
            .adapters
            .get(&reference.adapter_binding)
            .ok_or_else(|| invalid("adapter_binding"))?;
        let export = adapter
            .exports
            .iter()
            .find(|e| e.export_id == reference.export_id)
            .ok_or_else(|| invalid("export_id"))?
            .clone();
        if export.kind != kind {
            return Err(invalid("export.kind"));
        }
        if export.contract_version != 1 {
            return Err(ContractError::new(
                ErrorCode::UnsupportedContractVersion,
                "export.contract_version",
            ));
        }
        if !self.selected_exports.insert((
            reference.adapter_binding.clone(),
            reference.export_id.clone(),
        )) {
            return Err(invalid("export.duplicate"));
        }
        if kind == ExportKind::Tool {
            self.tool_name(
                reference
                    .alias
                    .as_ref()
                    .or(export.model_name.as_ref())
                    .ok_or_else(|| invalid("export.model_name"))?,
            )?;
        } else if reference.alias.is_some() {
            return Err(invalid("export.alias"));
        }
        if kind == ExportKind::Hook && export.hook_position.is_none() {
            return Err(invalid("export.hook_position"));
        }
        self.capabilities.extend(export.capabilities);
        self.required.extend(export.required_capabilities);
        Ok(())
    }

    fn finish(&self) -> Result<(), ContractError> {
        for metadata in self.definitions.values() {
            for dependency in &metadata.dependencies {
                if !self
                    .definitions
                    .values()
                    .any(|d| &d.reference == dependency)
                {
                    return Err(ContractError::new(
                        ErrorCode::ComponentUnavailable,
                        "component.dependencies",
                    ));
                }
            }
        }
        if !self.required.is_subset(&self.capabilities) {
            return Err(ContractError::new(
                ErrorCode::CapabilityUnsupported,
                "component.required_capabilities",
            ));
        }
        Ok(())
    }
}

fn invalid(path: &str) -> ContractError {
    ContractError::new(ErrorCode::InvalidReference, path)
}

fn validate_configuration(schema: &Value, value: &Value) -> Result<(), ContractError> {
    fn check_schema(schema: &Value) -> bool {
        let Some(map) = schema.as_object() else {
            return schema.is_boolean();
        };
        if map.contains_key("$dynamicRef") || map.contains_key("$recursiveRef") {
            return false;
        }
        if map
            .get("$ref")
            .is_some_and(|r| !r.as_str().is_some_and(|s| s.starts_with('#')))
        {
            return false;
        }
        if map
            .get("$schema")
            .is_some_and(|v| v != "https://json-schema.org/draft/2020-12/schema")
        {
            return false;
        }
        for key in [
            "$defs",
            "definitions",
            "properties",
            "patternProperties",
            "dependentSchemas",
        ] {
            if let Some(values) = map.get(key).and_then(Value::as_object) {
                if !values.values().all(check_schema) {
                    return false;
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
        ] {
            if let Some(value) = map.get(key) {
                if !check_schema(value) {
                    return false;
                }
            }
        }
        for key in ["allOf", "anyOf", "oneOf", "prefixItems"] {
            if let Some(values) = map.get(key).and_then(Value::as_array) {
                if !values.iter().all(check_schema) {
                    return false;
                }
            }
        }
        true
    }
    if !check_schema(schema) {
        return Err(ContractError::new(
            ErrorCode::InvalidSchema,
            "config_schema",
        ));
    }
    let validator = jsonschema::draft202012::options()
        .should_validate_formats(true)
        .build(schema)
        .map_err(|_| ContractError::new(ErrorCode::InvalidSchema, "config_schema"))?;
    if !validator.is_valid(value) {
        return Err(ContractError::new(
            ErrorCode::InvalidConfiguration,
            "config",
        ));
    }
    Ok(())
}
```

## `crates/wickle/src/run.rs`

```rust
use crate::{
    ArtifactRef, CompletionPolicy, ContractError, ErrorCode, Failure, Id, InputContent, JsonDigest,
    ModelInvocationRecord, RecordRef, ResolvedProfile, RunLimits, Scope, ToolCall, ToolResult,
    VersionedRef,
    serialization::{data_digest, decode, optional},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
};

/// Current run checkpoint format, independent of profile and event formats.
pub const RUN_SNAPSHOT_SCHEMA_VERSION: &str = "wickle.run-snapshot.v1";
/// Current durable event format.
pub const RUN_EVENT_SCHEMA_VERSION: &str = "wickle.run-event.v1";

/// Supported run checkpoint versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunSnapshotSchemaVersion {
    /// First checkpoint format.
    #[serde(rename = "wickle.run-snapshot.v1")]
    V1,
}

/// Supported session checkpoint versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionSchemaVersion {
    /// First session format.
    #[serde(rename = "wickle.session-snapshot.v1")]
    V1,
}

/// Supported durable event versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunEventSchemaVersion {
    /// First durable event format.
    #[serde(rename = "wickle.run-event.v1")]
    V1,
}

/// Why a Host submitted a run. Trigger data does not authenticate its sender.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunTrigger {
    /// Direct user request.
    User {},
    /// An event verified by the Host.
    Event {
        /// Host event identity.
        source_id: Id,
    },
    /// A schedule occurrence computed by the Host.
    Schedule {
        /// Occurrence identity, not a cron expression for the core to run.
        source_id: Id,
    },
    /// Child execution requested by a Host orchestration layer.
    Child {
        /// Parent run identity. Execution capability is checked separately.
        parent_run_id: Id,
    },
}

/// Caller request data; trusted execution context is supplied separately.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunRequest {
    /// Host-generated idempotency identity within scope and session.
    pub request_id: Id,
    /// Session whose pinned profile will be used.
    pub session_id: Id,
    /// User data, without injected tool calls or provider continuation state.
    pub input: Vec<InputContent>,
    /// Verified trigger provenance.
    pub trigger: RunTrigger,
    /// Optional output override; Host policy must authorize its use.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub output_contract: Option<crate::OutputContract>,
}

impl RunRequest {
    /// Decode caller data without granting authority or creating a run.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        decode(input, None)
    }
}

/// Exact request for human input, bound to the originating call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputRequest {
    /// Stable input request identity.
    pub input_request_id: Id,
    /// Call that must receive the answer.
    pub call_id: Id,
    /// Question shown by the Host.
    pub question: String,
    /// Optional exact schema for the answer.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub schema_ref: Option<VersionedRef>,
}

/// Exact operation or candidate to which approval applies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApprovalTarget {
    /// Tool approval binds the final execution input digest.
    Tool {
        /// Core call identity.
        call_id: Id,
        /// Digest that includes system-owned inputs.
        binding_digest: JsonDigest,
    },
    /// Review of a fixed candidate.
    Candidate {
        /// Stored candidate identity.
        candidate_ref: RecordRef,
        /// Exact verifier definition.
        verifier_ref: VersionedRef,
    },
}

/// Typed reason a run waits without making additional model calls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitTarget {
    /// Explicit approval of fixed data.
    Approval {
        /// Approval target.
        target: ApprovalTarget,
    },
    /// Answer to a recorded input request.
    Input {
        /// Input request.
        request: InputRequest,
    },
    /// Confirmation of an uncertain external effect.
    External {
        /// Call with uncertain effect.
        call_id: Id,
        /// Stable external idempotency/reconciliation key.
        effect_key: Id,
    },
}

/// Saved wait identity, target, and optional expiry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitState {
    /// Unique wait identity used to reject stale answers.
    pub wait_id: Id,
    /// Data or effect being awaited.
    pub target: WaitTarget,
    /// UTC milliseconds since Unix epoch; the run deadline still applies.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub expires_at_ms: Option<i64>,
}

/// A specific answer or recovery request; none of these grant execution permission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResumeAction {
    /// Accept a fixed approval target.
    Approve {
        /// Matching wait identity.
        wait_id: Id,
        /// Exact target presented for approval.
        target: ApprovalTarget,
    },
    /// Reject a fixed approval target.
    Deny {
        /// Matching wait identity.
        wait_id: Id,
        /// Exact target presented for approval.
        target: ApprovalTarget,
        /// User-supplied rejection reason.
        reason: String,
    },
    /// Supply data for a recorded input request.
    Input {
        /// Matching wait identity.
        wait_id: Id,
        /// Answer data, validated against the saved request by the resume handler.
        answer: serde_json::Value,
    },
    /// Supply a protected receipt for an external effect.
    External {
        /// Matching wait identity.
        wait_id: Id,
        /// Evidence to be verified by the authorized handler.
        receipt_ref: RecordRef,
    },
    /// Resume an interrupted nonterminal execution.
    Recover {
        /// Host-verified recovery evidence.
        recovery_ref: RecordRef,
    },
}

/// Idempotent resume command; state/policy enforcement is performed by the driver.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeCommand {
    /// Run to resume.
    pub run_id: Id,
    /// Revision the caller observed.
    pub expected_revision: u64,
    /// Deduplicates retries of the same decision.
    pub command_id: Id,
    /// Typed decision or recovery evidence.
    pub action: ResumeAction,
}

impl ResumeCommand {
    /// Decode an unambiguous command without executing or authorizing it.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        decode(input, None)
    }
}

/// Public run status categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Actively processing.
    Running,
    /// Persisted wait.
    Waiting,
    /// Completion policy satisfied.
    Succeeded,
    /// Unrecoverable failure.
    Failed,
    /// Explicit cancellation completed.
    Cancelled,
    /// A finite execution budget was exhausted.
    Exhausted,
}

impl RunStatus {
    /// Whether this status cannot be resumed as the same run.
    pub fn is_terminal(self) -> bool {
        !matches!(self, Self::Running | Self::Waiting)
    }
}

/// Driver phases; transition execution belongs to the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPhase {
    /// Admission validation and initial storage.
    Admission,
    /// Context and request preparation.
    Prepare,
    /// One model invocation.
    Model,
    /// Tool round processing.
    Tool,
    /// Output and completion checks.
    Verify,
    /// Saved wait.
    Waiting,
    /// Terminal outcome committed.
    Finish,
}

/// Stored budget consumption; usage measurement and reservation happen elsewhere.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetUsage {
    /// Reserved physical model attempts.
    pub model_calls: u64,
    /// Reserved physical tool attempts.
    pub tool_attempts: u64,
    /// Candidate repair attempts.
    pub repair_attempts: u64,
    /// Execution recovery attempts.
    pub recovery_attempts: u64,
    /// Elapsed milliseconds including waits.
    pub elapsed_ms: u64,
}

/// The budget that stopped an execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetKind {
    /// Model attempts.
    ModelCalls,
    /// Tool attempts.
    ToolAttempts,
    /// Repairs.
    RepairAttempts,
    /// Recoveries.
    RecoveryAttempts,
    /// Elapsed wall time.
    Elapsed,
}

/// What supports a successful outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionBasis {
    /// The model ended its turn; external business success is not asserted.
    TurnEnded,
    /// A pinned verifier accepted the candidate.
    Verified,
}

/// Recorded verifier classification, separate from transport errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationVerdict {
    /// Candidate accepted.
    Pass,
    /// Candidate needs revision.
    Revise,
    /// Human review required.
    Wait,
    /// Candidate rejected.
    Fail,
}

/// Evidence supporting the recorded verifier decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationSummary {
    /// Verifier actually used.
    pub verifier_ref: VersionedRef,
    /// Exact evaluation criteria.
    pub criteria_ref: VersionedRef,
    /// Decision classification.
    pub verdict: VerificationVerdict,
    /// Protected evidence records.
    pub evidence: Vec<RecordRef>,
}

/// Outcome-specific data. Success always names its completion basis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutcomeResult {
    /// Saved waiting outcome for the current execution segment.
    Waiting {
        /// Wait data.
        wait: WaitState,
    },
    /// Completion policy satisfied.
    Succeeded {
        /// Why completion was accepted.
        completion_basis: CompletionBasis,
    },
    /// Execution failed.
    Failed {
        /// Classified failure.
        failure: Failure,
    },
    /// Cancellation completed; existing effects remain in their records.
    Cancelled {
        /// Cancellation reason.
        reason: String,
    },
    /// Execution budget exhausted.
    Exhausted {
        /// Exhausted budget.
        budget: BudgetKind,
    },
}

impl OutcomeResult {
    /// Public status of this outcome.
    pub fn status(&self) -> RunStatus {
        match self {
            Self::Waiting { .. } => RunStatus::Waiting,
            Self::Succeeded { .. } => RunStatus::Succeeded,
            Self::Failed { .. } => RunStatus::Failed,
            Self::Cancelled { .. } => RunStatus::Cancelled,
            Self::Exhausted { .. } => RunStatus::Exhausted,
        }
    }
}

/// Stored outcome; it is the authority for completion, not an event or text delta.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunOutcome {
    /// Outcome classification and required status-specific data.
    pub result: OutcomeResult,
    /// Final or partial output.
    pub output: Vec<InputContent>,
    /// Produced artifact metadata.
    pub artifacts: Vec<ArtifactRef>,
    /// Consumption recorded at this checkpoint.
    pub usage: BudgetUsage,
    /// Exact checkpoint revision.
    pub checkpoint_revision: u64,
    /// Optional verifier evidence; mandatory for verified success.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub verification: Option<VerificationSummary>,
    /// Effects that must not be blindly repeated.
    pub unresolved_effects: Vec<RecordRef>,
}

impl RunOutcome {
    /// Check required evidence for a verified success.
    pub fn validate(&self) -> Result<(), ContractError> {
        if matches!(self.result, OutcomeResult::Succeeded { .. })
            && !self.unresolved_effects.is_empty()
        {
            return Err(ContractError::new(
                ErrorCode::InvalidContract,
                "outcome.unresolved_effects",
            ));
        }
        if matches!(
            self.result,
            OutcomeResult::Succeeded {
                completion_basis: CompletionBasis::Verified
            }
        ) && !self
            .verification
            .as_ref()
            .is_some_and(|v| v.verdict == VerificationVerdict::Pass)
        {
            return Err(ContractError::new(
                ErrorCode::InvalidContract,
                "outcome.verification",
            ));
        }
        Ok(())
    }
}

/// State of one planned tool call; this does not execute state transitions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolCallState {
    /// Plan is saved and no dispatch is recorded.
    Planned {},
    /// Dispatch was reserved and may have happened.
    Dispatching {
        /// Physical attempt identity.
        attempt_id: Id,
        /// Stable key for external deduplication/reconciliation.
        idempotency_key: Id,
    },
    /// Result was recorded.
    Settled {
        /// Paired tool result.
        result: ToolResult,
    },
    /// Effect is unknown after interruption.
    Unknown {
        /// Uncertain attempt identity.
        attempt_id: Id,
        /// Original external effect key.
        idempotency_key: Id,
    },
}

/// Planned model arguments and the corresponding dispatch/result state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolLedgerEntry {
    /// Original call and protected bound-input reference.
    pub call: ToolCall,
    /// Dispatch/result state.
    pub state: ToolCallState,
}

/// Protected system-input storage reference and versions used in request identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemInputSnapshotRef {
    /// Protected storage location; excluded from logical request identity.
    pub snapshot_ref: RecordRef,
    /// Digest of the validated owned values, including an explicit empty map.
    pub values_digest: JsonDigest,
    /// Exact registered input-definition versions.
    pub definition_versions: BTreeMap<Id, Id>,
}

/// Digest of logical start input, independent of a storage record's location.
/// A missing system-input map is the empty map for start; resume preserves the
/// separate missing/empty distinction in ExecutionContextData.
pub fn admission_digest(
    request: &RunRequest,
    profile: &ResolvedProfile,
    system_inputs: Option<&SystemInputSnapshotRef>,
) -> JsonDigest {
    let empty_digest = crate::canonical_digest(&serde_json::json!({}));
    let empty_versions = BTreeMap::new();
    let (values, versions) = system_inputs
        .map(|s| (&s.values_digest, &s.definition_versions))
        .unwrap_or((&empty_digest, &empty_versions));
    data_digest(&(request, profile.profile_digest(), values, versions))
}

/// Session metadata pinned across requests. A store enforces the active-run rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSnapshot {
    /// Session document version.
    pub schema_version: SessionSchemaVersion,
    /// Session identity.
    pub session_id: Id,
    /// Owning scope.
    pub scope: Scope,
    /// Pinned profile identity.
    pub profile_digest: JsonDigest,
    /// Pinned prompt data reference.
    pub prompt_snapshot: RecordRef,
    /// Current transcript revision.
    pub transcript_revision: u64,
    /// One active running/waiting run, or omission when none exists.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub active_run_id: Option<Id>,
}

/// Saved context-source execution position for retry and resume reuse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceExecutionState {
    /// Exact source selection, including adapter binding when applicable.
    pub source: crate::ContextSourceRef,
    /// Stable context request identity.
    pub context_request_id: Id,
    /// Collection trigger.
    pub trigger: crate::ContextTrigger,
    /// Required for a before_model collection.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub model_step_id: Option<Id>,
    /// Committed context batch, including empty/unavailable results.
    pub batch_ref: RecordRef,
}

/// Run checkpoint DTO. Use `from_json` or `validate` at the storage boundary.
/// Protected inputs are references, not automatically exposed execution arguments.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSnapshot {
    /// Checkpoint document version.
    pub schema_version: RunSnapshotSchemaVersion,
    /// Run identity.
    pub run_id: Id,
    /// Original caller request.
    pub request: RunRequest,
    /// Logical input digest used for deduplication.
    pub request_digest: JsonDigest,
    /// Scope used for storage, policy, tools, and resume.
    pub scope: Scope,
    /// Immutable profile and resolved definition identities.
    pub profile: ResolvedProfile,
    /// Current execution status.
    pub status: RunStatus,
    /// Current driver phase.
    pub phase: RunPhase,
    /// Current logical model step, if allocated.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub model_step_id: Option<Id>,
    /// Effective limits, no greater than profile limits.
    pub limits: RunLimits,
    /// Saved usage/reservations.
    pub usage: BudgetUsage,
    /// Physical model attempt records.
    pub model_ledger: Vec<ModelInvocationRecord>,
    /// Saved tool plans and states.
    pub tool_ledger: Vec<ToolLedgerEntry>,
    /// Pinned, protected system values and their contract revisions.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub system_inputs: Option<SystemInputSnapshotRef>,
    /// Saved wait data, only while waiting.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub wait: Option<WaitState>,
    /// Last waiting or terminal outcome.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub outcome: Option<RunOutcome>,
    /// Pinned assembly metadata, without process-local handler objects.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub assembly_ref: Option<RecordRef>,
    /// Committed context batches.
    pub context_batches: Vec<RecordRef>,
    /// Saved collection positions.
    pub source_states: Vec<SourceExecutionState>,
    /// Compare-and-swap revision.
    pub revision: u64,
    /// Last durable event sequence; ephemeral deltas do not consume it.
    pub last_event_seq: u64,
}

impl RunSnapshot {
    /// Decode a known checkpoint version and verify static consistency.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        let snapshot: Self = decode(input, Some(RUN_SNAPSHOT_SCHEMA_VERSION))?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Check static checkpoint invariants without performing recovery or authorization.
    pub fn validate(&self) -> Result<(), ContractError> {
        let invalid = |path| ContractError::new(ErrorCode::InvalidSnapshot, path);
        if &self.scope != self.profile.scope()
            || self.request_digest
                != admission_digest(&self.request, &self.profile, self.system_inputs.as_ref())
        {
            return Err(invalid("request_digest"));
        }
        let requested = &self.profile.profile().limits;
        if self.limits.max_model_calls > requested.max_model_calls
            || self.limits.max_tool_attempts > requested.max_tool_attempts
            || self.limits.max_repair_attempts > requested.max_repair_attempts
            || self.limits.max_recovery_attempts > requested.max_recovery_attempts
            || self.limits.max_elapsed_ms > requested.max_elapsed_ms
        {
            return Err(invalid("limits"));
        }
        match self.status {
            RunStatus::Running
                if matches!(self.phase, RunPhase::Waiting | RunPhase::Finish)
                    || self.wait.is_some()
                    || self.outcome.is_some() =>
            {
                return Err(invalid("status"));
            }
            RunStatus::Waiting if self.phase != RunPhase::Waiting || self.wait.is_none() => {
                return Err(invalid("wait"));
            }
            s if s.is_terminal()
                && (self.phase != RunPhase::Finish
                    || self.wait.is_some()
                    || self.outcome.is_none()) =>
            {
                return Err(invalid("outcome"));
            }
            _ => {}
        }
        if let Some(outcome) = &self.outcome {
            outcome.validate()?;
            if outcome.result.status() != self.status
                || outcome.checkpoint_revision != self.revision
                || outcome.usage != self.usage
            {
                return Err(invalid("outcome"));
            }
            if let OutcomeResult::Waiting { wait } = &outcome.result {
                if self.wait.as_ref() != Some(wait) {
                    return Err(invalid("outcome.wait"));
                }
            }
            if let OutcomeResult::Succeeded { completion_basis } = &outcome.result {
                match (&self.profile.profile().completion_policy, completion_basis) {
                    (CompletionPolicy::TurnEnd {}, CompletionBasis::TurnEnded) => {}
                    (CompletionPolicy::Verified { verifier_ref }, CompletionBasis::Verified)
                        if outcome
                            .verification
                            .as_ref()
                            .is_some_and(|v| &v.verifier_ref == verifier_ref) => {}
                    _ => return Err(invalid("outcome.completion_basis")),
                }
            }
        }
        let mut calls = BTreeSet::new();
        for entry in &self.tool_ledger {
            if !calls.insert(&entry.call.call_id) {
                return Err(invalid("tool_ledger.call_id"));
            }
            match &entry.state {
                ToolCallState::Dispatching { .. } | ToolCallState::Unknown { .. }
                    if entry.call.bound_input_ref.is_none() =>
                {
                    return Err(invalid("tool_ledger.bound_input_ref"));
                }
                ToolCallState::Settled { result } if result.call_id != entry.call.call_id => {
                    return Err(invalid("tool_ledger.result.call_id"));
                }
                _ => {}
            }
            if self.status == RunStatus::Succeeded
                && !matches!(&entry.state, ToolCallState::Settled { result } if result.status != crate::ToolResultStatus::Unknown)
            {
                return Err(invalid("tool_ledger.unsettled"));
            }
        }
        let mut attempts = BTreeSet::new();
        if self
            .model_ledger
            .iter()
            .any(|a| a.run_id != self.run_id || !attempts.insert(&a.attempt_id))
        {
            return Err(invalid("model_ledger.attempt_id"));
        }
        if self
            .source_states
            .iter()
            .any(|s| (s.trigger == crate::ContextTrigger::BeforeModel) != s.model_step_id.is_some())
        {
            return Err(invalid("source_states.model_step_id"));
        }
        Ok(())
    }
}

/// Durable facts reference stored records rather than copying protected inputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum RunEventPayload {
    /// Admission committed.
    #[serde(rename = "run.started")]
    RunStarted {
        /// Accepted request reference.
        request_ref: RecordRef,
        /// Pinned profile identity.
        profile_digest: JsonDigest,
    },
    /// Tool plan committed.
    #[serde(rename = "tool.planned")]
    ToolPlanned {
        /// Protected call record.
        call_ref: RecordRef,
    },
    /// Tool result committed.
    #[serde(rename = "tool.settled")]
    ToolSettled {
        /// Protected result record.
        result_ref: RecordRef,
    },
    /// Verifier decision committed.
    #[serde(rename = "verification.completed")]
    VerificationCompleted {
        /// Recorded verification evidence.
        verification_ref: RecordRef,
    },
    /// Wait committed.
    #[serde(rename = "run.waiting")]
    RunWaiting {
        /// Recorded wait.
        wait_ref: RecordRef,
    },
    /// Resume command consumed.
    #[serde(rename = "run.resumed")]
    RunResumed {
        /// Consumed command record.
        command_ref: RecordRef,
    },
    /// Terminal outcome committed.
    #[serde(rename = "run.finished")]
    RunFinished {
        /// Authoritative outcome record.
        outcome_ref: RecordRef,
    },
    /// Model route and invocation identity committed.
    #[serde(rename = "model.route_selected")]
    ModelRouteSelected {
        /// Invocation record.
        invocation_ref: RecordRef,
        /// Selected route identity.
        route_digest: JsonDigest,
    },
}

/// A durable event committed atomically with authoritative state by the store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunEvent {
    /// Independent event wire version.
    pub schema_version: RunEventSchemaVersion,
    /// Stable event identity for deduplication.
    pub event_id: Id,
    /// Owning scope.
    pub scope: Scope,
    /// Owning run.
    pub run_id: Id,
    /// Owning session.
    pub session_id: Id,
    /// Positive durable event sequence.
    pub seq: NonZeroU64,
    /// UTC milliseconds since Unix epoch.
    pub timestamp_ms: i64,
    /// Typed event data with authorized record references.
    pub payload: RunEventPayload,
}

impl RunEvent {
    /// Decode a known event format without replaying or dispatching it.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        decode(input, Some(RUN_EVENT_SCHEMA_VERSION))
    }
}

/// Non-durable presentation hints; these carry no durable sequence or completion claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum EphemeralEvent {
    /// Candidate text from an incomplete model response.
    CandidateTextDelta {
        /// Owning run.
        run_id: Id,
        /// Physical model attempt.
        attempt_id: Id,
        /// Candidate text, not a committed final answer.
        text: String,
    },
}
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

/// A SHA-256 digest using the versioned `sorted-json-v1` encoding.
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
    let bytes =
        serde_json::to_vec(&ordered(value)).expect("JSON values serialize into a byte vector");
    let hex: String = Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    JsonDigest(format!("sorted-json-v1:sha256:{hex}"))
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

## `crates/wickle/tests/contracts.rs`

```rust
//! Behavioral checks for validation, persisted contracts, and profile identity.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn digest(value: &str) -> JsonDigest {
    canonical_digest(&json!(value))
}
fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    }
}
fn reference(kind: ComponentKind, name: &str) -> ComponentRef {
    ComponentRef {
        kind,
        id: id(name),
        version: if kind == ComponentKind::ModelBinding || kind == ComponentKind::Extension {
            None
        } else {
            Some(id("1.0.0"))
        },
    }
}

fn profile_value() -> Value {
    json!({
        "schema_version": "wickle.agent-profile.v1", "agent_id": "research", "version": "1.0.0",
        "name": "Research assistant", "description": "Find information with sources",
        "instructions": {"text": "Use available evidence."}, "model_binding": "primary",
        "tools": [{"tool_id": "documents.search", "version": "1.0.0", "bindings": {"main": "knowledge"}, "config": {"limit": 5}}],
        "skills": [], "connectors": [{"binding_id": "knowledge", "connector_id": "document-store", "version": "1.0.0"}],
        "context_policy": {"strategy": "bounded"}, "output_contract": {"type": "text"},
        "limits": {"max_model_calls": 8, "max_tool_attempts": 12, "max_repair_attempts": 0, "max_recovery_attempts": 2, "max_elapsed_ms": 30000}
    })
}
fn profile() -> AgentProfile {
    AgentProfile::from_json(&profile_value().to_string()).unwrap()
}

struct Catalog(BTreeMap<ComponentRef, ComponentMetadata>);

impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        requested_scope: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            if requested_scope != &scope() {
                return Err(ContractError::new(ErrorCode::ComponentUnavailable, "scope"));
            }
            self.0
                .get(reference)
                .cloned()
                .ok_or_else(|| ContractError::new(ErrorCode::ComponentUnavailable, "reference"))
        })
    }
}

fn metadata(key: &ComponentRef) -> ComponentMetadata {
    let mut resolved = key.clone();
    resolved.version = Some(id("1.0.0"));
    ComponentMetadata {
        reference: resolved,
        contract_version: 1,
        manifest_digest: digest("manifest"),
        config_schema: json!({"type":"object","additionalProperties":false}),
        dependencies: vec![],
        capabilities: BTreeSet::new(),
        required_capabilities: BTreeSet::new(),
        required_connections: BTreeSet::new(),
        model_name: None,
        hook_position: None,
        exports: vec![],
    }
}

fn catalog() -> Catalog {
    let mut definitions = BTreeMap::new();
    let model = reference(ComponentKind::ModelBinding, "primary");
    let mut model_meta = metadata(&model);
    model_meta.capabilities.insert(id("model.tool_calling"));
    definitions.insert(model, model_meta);
    let connector = reference(ComponentKind::Connector, "document-store");
    definitions.insert(connector.clone(), metadata(&connector));
    let tool = reference(ComponentKind::Tool, "documents.search");
    let mut tool_meta = metadata(&tool);
    tool_meta.config_schema = json!({"type":"object","properties":{"limit":{"type":"integer","minimum":1,"maximum":50}},"required":["limit"],"additionalProperties":false});
    tool_meta.required_connections.insert(id("main"));
    tool_meta
        .required_capabilities
        .insert(id("model.tool_calling"));
    tool_meta.capabilities.insert(id("documents.search"));
    tool_meta.model_name = Some(id("documents_search"));
    definitions.insert(tool, tool_meta);
    Catalog(definitions)
}

async fn resolved() -> ResolvedProfile {
    ProfileValidator::new(&catalog())
        .validate(&profile(), &scope())
        .await
        .unwrap()
}

#[test]
fn digest_matches_independent_sha256_vectors_and_sorts_nested_objects() {
    assert_eq!(
        canonical_digest_json("{}").unwrap().as_str(),
        "sorted-json-v1:sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
    );
    let a = r#"{"z":1,"a":{"x":[3,1],"b":2}}"#;
    let b = r#"{ "a": {"b":2,"x":[3,1]}, "z":1 }"#;
    let actual = canonical_digest_json(a).unwrap();
    assert_eq!(
        actual.as_str(),
        "sorted-json-v1:sha256:b2db09df32697403c319dfb8cd57f51a8400eb9943753795ccbb34d1383f01a2"
    );
    assert_eq!(actual, canonical_digest_json(b).unwrap());
    for changed in [
        r#"{"z":1,"a":{"x":[1,3],"b":2}}"#,
        r#"{"z":2,"a":{"x":[3,1],"b":2}}"#,
    ] {
        assert_ne!(actual, canonical_digest_json(changed).unwrap());
    }
    assert_ne!(
        canonical_digest_json("1").unwrap(),
        canonical_digest_json("1.0").unwrap()
    );
    assert_ne!(
        canonical_digest_json("0").unwrap(),
        canonical_digest_json("-0.0").unwrap()
    );
}

#[test]
fn ambiguous_and_non_json_input_is_rejected_instead_of_being_normalized() {
    for input in [
        "NaN",
        "Infinity",
        "-Infinity",
        "1e400",
        "undefined",
        r#"{"a":1,"a":2}"#,
        r#"{"nested":{"a":1,"a":2}}"#,
        "{} {}",
    ] {
        assert_eq!(
            canonical_digest_json(input).unwrap_err().code,
            ErrorCode::InvalidJson,
            "{input}"
        );
    }
}

#[test]
fn profile_rejects_unknown_fields_runtime_objects_invalid_limits_and_null_options() {
    let invalid = [
        ("api_key", json!("credential-value")),
        ("runtime_bindings", json!({"model":"client"})),
        ("sdk_client", json!({})),
        ("adapters", Value::Null),
        ("hooks", Value::Null),
        ("context_sources", Value::Null),
        ("extensions", Value::Null),
        (
            "instructions",
            json!({"text":"a","module_path":"untrusted-code"}),
        ),
        ("completion_policy", json!({"mode":"verified"})),
        (
            "completion_policy",
            json!({"mode":"turn_end","verifier_ref":{"id":"extra","version":"1"}}),
        ),
        (
            "output_contract",
            json!({"type":"text","schema_ref":{"id":"extra","version":"1"}}),
        ),
        (
            "tools",
            json!([{"tool_id":"documents.search","version":"1.0.0","adapter_binding":"mixed","export_id":"search"}]),
        ),
    ];
    for (key, value) in invalid {
        let mut input = profile_value();
        input[key] = value;
        assert_eq!(
            AgentProfile::from_json(&input.to_string())
                .expect_err(&format!("accepted invalid field: {key}"))
                .code,
            ErrorCode::InvalidContract,
            "{key}"
        );
    }
    for value in [json!(0), json!(-1), json!(1.5), Value::Null] {
        let mut input = profile_value();
        input["limits"]["max_model_calls"] = value;
        assert_eq!(
            AgentProfile::from_json(&input.to_string())
                .unwrap_err()
                .code,
            ErrorCode::InvalidContract
        );
    }
    for field in ["schema_version", "tools", "model_binding", "limits"] {
        let mut input = profile_value();
        input.as_object_mut().unwrap().remove(field);
        assert!(
            AgentProfile::from_json(&input.to_string()).is_err(),
            "missing {field}"
        );
    }
    let mut input = profile_value();
    input["schema_version"] = json!("wickle.agent-profile.v99");
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedSchemaVersion
    );
}

#[test]
fn optional_fields_preserve_presence_and_zero_means_disabled() {
    let absent = profile();
    assert_eq!(absent.completion_policy, CompletionPolicy::TurnEnd {});
    assert_eq!(absent.limits.max_repair_attempts, 0);
    let mut input = profile_value();
    input["adapters"] = json!([]);
    let empty = AgentProfile::from_json(&input.to_string()).unwrap();
    assert!(absent.adapters.is_none());
    assert_eq!(empty.adapters, Some(vec![]));
    assert_ne!(absent.digest(), empty.digest());
    assert_eq!(
        AgentProfile::from_json(&serde_json::to_string(&empty).unwrap()).unwrap(),
        empty
    );
}

#[test]
fn local_binding_errors_are_rejected_before_metadata_resolution() {
    let mut input = profile_value();
    input["tools"][0]["bindings"]["main"] = json!("missing");
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
    let mut input = profile_value();
    let duplicate = input["connectors"][0].clone();
    input["connectors"].as_array_mut().unwrap().push(duplicate);
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .path,
        "connectors.binding_id"
    );
    let mut input = profile_value();
    input["tools"] = json!([{"adapter_binding":"missing","export_id":"search"}]);
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .path,
        "adapter_binding"
    );
    let mut input = profile_value();
    input["context_policy"] = json!({"strategy":"custom"});
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .path,
        "context_policy.version"
    );
}

#[tokio::test]
async fn resolver_accepts_registered_components_and_freezes_their_full_definition_identity() {
    let profile = profile();
    let catalog = catalog();
    let pinned = ProfileValidator::new(&catalog)
        .validate(&profile, &scope())
        .await
        .unwrap();
    assert_eq!(pinned.components().len(), 3);
    assert!(
        pinned
            .components()
            .iter()
            .all(|c| c.reference.version.as_ref() == Some(&id("1.0.0")))
    );
    let restored: ResolvedProfile =
        serde_json::from_str(&serde_json::to_string(&pinned).unwrap()).unwrap();
    restored.ensure_matches(&profile, &scope()).unwrap();
    restored.ensure_same_resolution(&pinned).unwrap();
    let mut changed_catalog = catalog;
    changed_catalog
        .0
        .get_mut(&reference(ComponentKind::ModelBinding, "primary"))
        .unwrap()
        .manifest_digest = digest("new definition");
    let newer = ProfileValidator::new(&changed_catalog)
        .validate(&profile, &scope())
        .await
        .unwrap();
    assert_eq!(
        pinned.ensure_same_resolution(&newer).unwrap_err().code,
        ErrorCode::ProfileMismatch
    );
}

#[tokio::test]
async fn unavailable_dependencies_wrong_versions_and_missing_capabilities_fail_resolution() {
    let p = profile();
    let mut missing = catalog();
    missing
        .0
        .remove(&reference(ComponentKind::Tool, "documents.search"));
    assert_eq!(
        ProfileValidator::new(&missing)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    let mut wrong = catalog();
    wrong
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .reference
        .version = Some(id("2.0.0"));
    assert_eq!(
        ProfileValidator::new(&wrong)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    let mut unsupported = catalog();
    unsupported
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .contract_version = 2;
    assert_eq!(
        ProfileValidator::new(&unsupported)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedContractVersion
    );
    let mut no_capability = catalog();
    no_capability
        .0
        .get_mut(&reference(ComponentKind::ModelBinding, "primary"))
        .unwrap()
        .capabilities
        .clear();
    assert_eq!(
        ProfileValidator::new(&no_capability)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
    let mut no_dependency = catalog();
    no_dependency
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .dependencies
        .push(reference(ComponentKind::Tool, "skills.load"));
    assert_eq!(
        ProfileValidator::new(&no_dependency)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    let mut no_connection = p.clone();
    if let ToolBindingRef::Catalog(tool) = &mut no_connection.tools[0] {
        tool.bindings = None;
    }
    assert_eq!(
        ProfileValidator::new(&catalog())
            .validate(&no_connection, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
}

#[tokio::test]
async fn registered_configuration_schema_rejects_wrong_types_ranges_and_credential_fields() {
    for config in [
        json!({"limit":0}),
        json!({"limit":"5"}),
        json!({"limit":51}),
        json!({"limit":5,"api_key":"credential-value"}),
    ] {
        let mut input = profile_value();
        input["tools"][0]["config"] = config;
        let p = AgentProfile::from_json(&input.to_string()).unwrap();
        let error = ProfileValidator::new(&catalog())
            .validate(&p, &scope())
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidConfiguration);
        assert!(!error.to_string().contains("credential-value"));
        assert!(!format!("{error:?}").contains("credential-value"));
    }
}

#[tokio::test]
async fn external_schema_references_fail_but_literal_reference_data_is_not_executed() {
    for schema in [
        json!({"$ref":"https://unavailable.invalid/schema"}),
        json!({"properties":{"limit":{"$ref":"file:///tmp/schema"}}}),
        json!({"$dynamicRef":"#anchor"}),
        json!({"type":"integer"}),
    ] {
        let mut c = catalog();
        c.0.get_mut(&reference(ComponentKind::Tool, "documents.search"))
            .unwrap()
            .config_schema = schema;
        let error = ProfileValidator::new(&c)
            .validate(&profile(), &scope())
            .await
            .unwrap_err();
        assert!(matches!(
            error.code,
            ErrorCode::InvalidSchema | ErrorCode::InvalidConfiguration
        ));
    }
    let mut c = catalog();
    let meta =
        c.0.get_mut(&reference(ComponentKind::Tool, "documents.search"))
            .unwrap();
    meta.config_schema = json!({"$defs":{"limit":{"type":"integer","minimum":1}},"type":"object","properties":{"limit":{"$ref":"#/$defs/limit"}},"required":["limit"],"additionalProperties":false,"default":{"$ref":"https://example.invalid/literal-data"}});
    ProfileValidator::new(&c)
        .validate(&profile(), &scope())
        .await
        .unwrap();
}

#[tokio::test]
async fn extensions_require_registered_namespaces_and_valid_data() {
    let mut input = profile_value();
    input["extensions"] = json!({"bad":{}});
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
    input["extensions"] = json!({"example.settings":{"enabled":true}});
    let p = AgentProfile::from_json(&input.to_string()).unwrap();
    assert_eq!(
        ProfileValidator::new(&catalog())
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    let mut c = catalog();
    let key = reference(ComponentKind::Extension, "example.settings");
    let mut definition = metadata(&key);
    definition.config_schema = json!({"type":"object","properties":{"enabled":{"type":"boolean"}},"additionalProperties":false});
    c.0.insert(key, definition);
    ProfileValidator::new(&c)
        .validate(&p, &scope())
        .await
        .unwrap();
    input["extensions"]["example.settings"]["enabled"] = json!(1);
    assert_eq!(
        ProfileValidator::new(&c)
            .validate(
                &AgentProfile::from_json(&input.to_string()).unwrap(),
                &scope()
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidConfiguration
    );
}

#[tokio::test]
async fn registered_formats_are_asserted_instead_of_treated_as_annotations() {
    let mut catalog = catalog();
    catalog
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .config_schema = json!({
        "type": "object", "properties": {"example_uuid": {"type": "string", "format": "uuid"}},
        "required": ["example_uuid"], "additionalProperties": false
    });
    let mut input = profile_value();
    input["tools"][0]["config"] = json!({"example_uuid": "not-a-uuid"});
    let invalid = AgentProfile::from_json(&input.to_string()).unwrap();
    assert_eq!(
        ProfileValidator::new(&catalog)
            .validate(&invalid, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidConfiguration
    );
    input["tools"][0]["config"] = json!({"example_uuid": "123e4567-e89b-12d3-a456-426614174000"});
    ProfileValidator::new(&catalog)
        .validate(
            &AgentProfile::from_json(&input.to_string()).unwrap(),
            &scope(),
        )
        .await
        .unwrap();
}

fn adapter_profile() -> (AgentProfile, Catalog) {
    let mut value = profile_value();
    value["tools"] =
        json!([{"adapter_binding":"documents","export_id":"search","alias":"search_documents"}]);
    value["adapters"] = json!([{"binding_id":"documents","adapter_id":"document-tools","version":"1.0.0","connections":{"main":"knowledge"}}]);
    let mut c = catalog();
    let key = reference(ComponentKind::Adapter, "document-tools");
    let mut definition = metadata(&key);
    definition.required_connections.insert(id("main"));
    definition.exports.push(ExportMetadata {
        export_id: id("search"),
        kind: ExportKind::Tool,
        contract_version: 1,
        model_name: Some(id("documents_search")),
        hook_position: None,
        capabilities: BTreeSet::from([id("documents.search")]),
        required_capabilities: BTreeSet::from([id("model.tool_calling")]),
    });
    definition.exports.push(ExportMetadata {
        export_id: id("unused"),
        kind: ExportKind::Tool,
        contract_version: 1,
        model_name: Some(id("unused")),
        hook_position: None,
        capabilities: BTreeSet::from([id("unused.capability")]),
        required_capabilities: BTreeSet::new(),
    });
    c.0.insert(key, definition);
    (AgentProfile::from_json(&value.to_string()).unwrap(), c)
}

#[tokio::test]
async fn adapter_exports_must_exist_match_kind_and_be_selected_to_supply_capabilities() {
    let (p, c) = adapter_profile();
    ProfileValidator::new(&c)
        .validate(&p, &scope())
        .await
        .unwrap();
    let mut wrong_kind = catalog();
    let (_, mut definitions) = adapter_profile();
    let key = reference(ComponentKind::Adapter, "document-tools");
    let mut definition = definitions.0.remove(&key).unwrap();
    definition.exports[0].kind = ExportKind::ContextSource;
    wrong_kind.0.insert(key.clone(), definition);
    assert_eq!(
        ProfileValidator::new(&wrong_kind)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
    let mut missing = p.clone();
    if let ToolBindingRef::Export(export) = &mut missing.tools[0] {
        export.export_id = id("missing");
    }
    assert_eq!(
        ProfileValidator::new(&c)
            .validate(&missing, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
    let mut needs_unselected = c;
    needs_unselected
        .0
        .get_mut(&key)
        .unwrap()
        .required_capabilities
        .insert(id("unused.capability"));
    assert_eq!(
        ProfileValidator::new(&needs_unselected)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
    let mut duplicate = p.clone();
    duplicate.tools.push(duplicate.tools[0].clone());
    assert_eq!(
        ProfileValidator::new(&adapter_profile().1)
            .validate(&duplicate, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
}

#[tokio::test]
async fn saved_profiles_reject_changed_instructions_versions_scope_and_tampered_serialization() {
    let pinned = resolved().await;
    let p = profile();
    let mut changed = p.clone();
    changed.version = id("2.0.0");
    assert_eq!(
        pinned.ensure_matches(&changed, &scope()).unwrap_err().code,
        ErrorCode::ProfileMismatch
    );
    let mut changed = p.clone();
    changed.instructions = Instructions::Text(InstructionText {
        text: "Changed behavior".into(),
    });
    assert_eq!(
        pinned.ensure_matches(&changed, &scope()).unwrap_err().code,
        ErrorCode::ProfileMismatch
    );
    let mut other_scope = scope();
    other_scope.tenant_id = id("other");
    assert_eq!(
        pinned.ensure_matches(&p, &other_scope).unwrap_err().code,
        ErrorCode::ProfileMismatch
    );
    let mut stored = serde_json::to_value(&pinned).unwrap();
    stored["profile"]["version"] = json!("new");
    assert!(serde_json::from_value::<ResolvedProfile>(stored).is_err());
}

#[test]
fn system_inputs_preserve_absent_empty_and_owned_values_without_debug_leakage() {
    let mut input = json!({"scope":{"tenant_id":"t","workspace_id":"w"},"principal_ref":"p","capability_grant_ref":"g"});
    let absent = ExecutionContextData::from_json(&input.to_string()).unwrap();
    assert!(absent.system_inputs.is_none());
    input["system_inputs"] = json!({});
    let empty = ExecutionContextData::from_json(&input.to_string()).unwrap();
    assert!(empty.system_inputs.as_ref().unwrap().values().is_empty());
    input["system_inputs"] = Value::Null;
    assert!(ExecutionContextData::from_json(&input.to_string()).is_err());
    input["system_inputs"] = json!({"workspace_id":"private-workspace-value"});
    let stored = ExecutionContextData::from_json(&input.to_string()).unwrap();
    input["system_inputs"]["workspace_id"] = json!("mutated");
    assert_eq!(
        stored.system_inputs.as_ref().unwrap().values()["workspace_id"],
        json!("private-workspace-value")
    );
    assert!(!format!("{stored:?}").contains("private-workspace-value"));
    let roundtrip =
        ExecutionContextData::from_json(&serde_json::to_string(&stored).unwrap()).unwrap();
    assert_eq!(roundtrip, stored);
}

fn record(name: &str) -> RecordRef {
    RecordRef {
        record_id: id(name),
        revision: 1,
        digest: digest(name),
    }
}
fn request() -> RunRequest {
    RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Find supporting evidence".into(),
        }],
        trigger: RunTrigger::User {},
        output_contract: None,
    }
}

async fn checkpoint() -> RunSnapshot {
    let p = resolved().await;
    let request = request();
    let system_inputs = Some(SystemInputSnapshotRef {
        snapshot_ref: record("protected-inputs"),
        values_digest: digest("owned inputs"),
        definition_versions: BTreeMap::from([(id("workspace_id"), id("1"))]),
    });
    let wait = WaitState {
        wait_id: id("approval"),
        target: WaitTarget::Approval {
            target: ApprovalTarget::Tool {
                call_id: id("call"),
                binding_digest: digest("bound args"),
            },
        },
        expires_at_ms: Some(100000),
    };
    RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id("run"),
        request_digest: admission_digest(&request, &p, system_inputs.as_ref()),
        request,
        scope: scope(),
        limits: p.profile().limits.clone(),
        profile: p,
        status: RunStatus::Waiting,
        phase: RunPhase::Waiting,
        model_step_id: Some(id("step")),
        usage: BudgetUsage {
            model_calls: 1,
            ..BudgetUsage::default()
        },
        model_ledger: vec![],
        tool_ledger: vec![ToolLedgerEntry {
            call: ToolCall {
                call_id: id("call"),
                model_request_id: id("model-request"),
                provider_call_id: id("provider-call"),
                tool_name: id("documents_search"),
                model_inputs: BTreeMap::from([("query".into(), json!("evidence"))]),
                descriptor_digest: digest("descriptor"),
                bound_input_ref: Some(record("bound-inputs")),
            },
            state: ToolCallState::Planned {},
        }],
        system_inputs,
        wait: Some(wait),
        outcome: None,
        assembly_ref: Some(record("assembly")),
        context_batches: vec![record("context-batch")],
        source_states: vec![],
        revision: 4,
        last_event_seq: 7,
    }
}

#[tokio::test]
async fn approval_checkpoint_roundtrip_preserves_the_target_and_deduplication_identity() {
    let snapshot = checkpoint().await;
    snapshot.validate().unwrap();
    let restored = RunSnapshot::from_json(&serde_json::to_string(&snapshot).unwrap()).unwrap();
    assert_eq!(restored, snapshot);
    let target = match &restored.wait.as_ref().unwrap().target {
        WaitTarget::Approval { target } => target.clone(),
        _ => unreachable!(),
    };
    let command = ResumeCommand {
        run_id: restored.run_id.clone(),
        expected_revision: restored.revision,
        command_id: id("decision"),
        action: ResumeAction::Approve {
            wait_id: restored.wait.as_ref().unwrap().wait_id.clone(),
            target,
        },
    };
    assert_eq!(
        ResumeCommand::from_json(&serde_json::to_string(&command).unwrap()).unwrap(),
        command
    );
    let mut relocated = snapshot.system_inputs.clone().unwrap();
    relocated.snapshot_ref = record("new-storage-location");
    assert_eq!(
        snapshot.request_digest,
        admission_digest(&snapshot.request, &snapshot.profile, Some(&relocated))
    );
    relocated.values_digest = digest("different inputs");
    assert_ne!(
        snapshot.request_digest,
        admission_digest(&snapshot.request, &snapshot.profile, Some(&relocated))
    );
    relocated = snapshot.system_inputs.clone().unwrap();
    relocated
        .definition_versions
        .insert(id("workspace_id"), id("2"));
    assert_ne!(
        snapshot.request_digest,
        admission_digest(&snapshot.request, &snapshot.profile, Some(&relocated))
    );
    let empty = SystemInputSnapshotRef {
        snapshot_ref: record("empty-inputs"),
        values_digest: canonical_digest(&json!({})),
        definition_versions: BTreeMap::new(),
    };
    assert_eq!(
        admission_digest(&snapshot.request, &snapshot.profile, None),
        admission_digest(&snapshot.request, &snapshot.profile, Some(&empty))
    );
}

#[tokio::test]
async fn catalog_and_export_names_cannot_create_ambiguous_tool_routing() {
    let (mut profile, mut catalog) = adapter_profile();
    profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
        tool_id: id("documents.search"),
        version: id("1.0.0"),
        bindings: Some(BTreeMap::from([(id("main"), id("knowledge"))])),
        config: Some(BTreeMap::from([("limit".into(), json!(5))])),
    }));
    catalog
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .model_name = Some(id("search_documents"));
    assert_eq!(
        ProfileValidator::new(&catalog)
            .validate(&profile, &scope())
            .await
            .unwrap_err()
            .path,
        "tools.model_name"
    );
}

#[tokio::test]
async fn checkpoint_rejects_inconsistent_state_budget_inputs_and_dispatch_records() {
    let valid = checkpoint().await;
    let mut malformed = valid.clone();
    malformed.wait = None;
    assert_eq!(
        malformed.validate().unwrap_err().code,
        ErrorCode::InvalidSnapshot
    );
    let mut malformed = valid.clone();
    malformed.limits.max_tool_attempts += 1;
    assert_eq!(malformed.validate().unwrap_err().path, "limits");
    let mut malformed = valid.clone();
    malformed.request.request_id = id("changed");
    assert_eq!(malformed.validate().unwrap_err().path, "request_digest");
    let mut malformed = valid.clone();
    malformed.tool_ledger[0].call.bound_input_ref = None;
    malformed.tool_ledger[0].state = ToolCallState::Dispatching {
        attempt_id: id("attempt"),
        idempotency_key: id("effect"),
    };
    assert_eq!(
        malformed.validate().unwrap_err().path,
        "tool_ledger.bound_input_ref"
    );
    let mut malformed = valid.clone();
    malformed.tool_ledger.push(malformed.tool_ledger[0].clone());
    assert_eq!(
        malformed.validate().unwrap_err().path,
        "tool_ledger.call_id"
    );
    let mut stored = serde_json::to_value(&valid).unwrap();
    stored["schema_version"] = json!("wickle.run-snapshot.v2");
    assert_eq!(
        RunSnapshot::from_json(&stored.to_string())
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedSchemaVersion
    );
}

#[tokio::test]
async fn success_requires_a_matching_completion_basis_and_verified_success_requires_evidence() {
    let mut snapshot = checkpoint().await;
    snapshot.status = RunStatus::Succeeded;
    snapshot.phase = RunPhase::Finish;
    snapshot.wait = None;
    snapshot.outcome = Some(RunOutcome {
        result: OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded,
        },
        output: vec![InputContent::Text {
            text: "Candidate answer".into(),
        }],
        artifacts: vec![],
        usage: snapshot.usage.clone(),
        checkpoint_revision: snapshot.revision,
        verification: None,
        unresolved_effects: vec![],
    });
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "tool_ledger.unsettled"
    );
    snapshot.tool_ledger[0].state = ToolCallState::Settled {
        result: ToolResult {
            call_id: id("call"),
            call_message_id: id("call-message"),
            status: ToolResultStatus::Succeeded,
            content: vec![InputContent::Text {
                text: "Evidence found".into(),
            }],
            effect_receipt_ref: None,
            error: None,
        },
    };
    snapshot.validate().unwrap();
    snapshot
        .outcome
        .as_mut()
        .unwrap()
        .unresolved_effects
        .push(record("unknown-effect"));
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "outcome.unresolved_effects"
    );
    snapshot
        .outcome
        .as_mut()
        .unwrap()
        .unresolved_effects
        .clear();
    snapshot.outcome.as_mut().unwrap().result = OutcomeResult::Succeeded {
        completion_basis: CompletionBasis::Verified,
    };
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "outcome.verification"
    );
    snapshot.outcome.as_mut().unwrap().verification = Some(VerificationSummary {
        verifier_ref: VersionedRef {
            id: id("verifier"),
            version: id("1"),
        },
        criteria_ref: VersionedRef {
            id: id("criteria"),
            version: id("1"),
        },
        verdict: VerificationVerdict::Pass,
        evidence: vec![record("evidence")],
    });
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "outcome.completion_basis"
    );
}

#[test]
fn event_and_input_contracts_reject_unsupported_versions_and_execution_injection() {
    let event = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("event"),
        scope: scope(),
        run_id: id("run"),
        session_id: id("session"),
        seq: 1.try_into().unwrap(),
        timestamp_ms: 1000,
        payload: RunEventPayload::RunFinished {
            outcome_ref: record("outcome"),
        },
    };
    assert_eq!(
        RunEvent::from_json(&serde_json::to_string(&event).unwrap()).unwrap(),
        event
    );
    let mut value = serde_json::to_value(&event).unwrap();
    value["schema_version"] = json!("wickle.run-event.v2");
    assert_eq!(
        RunEvent::from_json(&value.to_string()).unwrap_err().code,
        ErrorCode::UnsupportedSchemaVersion
    );
    let mut value = serde_json::to_value(&event).unwrap();
    value["seq"] = json!(0);
    assert!(RunEvent::from_json(&value.to_string()).is_err());
    let mut value = serde_json::to_value(request()).unwrap();
    value["input"] = json!([{"type":"tool_call","call":{"tool_name":"unapproved"}}]);
    assert!(RunRequest::from_json(&value.to_string()).is_err());
    let mut value = serde_json::to_value(request()).unwrap();
    value["trigger"] = json!({"kind":"user","source_id":"forged"});
    assert!(RunRequest::from_json(&value.to_string()).is_err());
    assert!(
        serde_json::from_value::<ModelAttemptState>(json!({"state":"completed","kind":"timeout"}))
            .is_err()
    );
    assert!(
        serde_json::from_value::<ToolCallState>(
            json!({"state":"planned","idempotency_key":"unexpected"})
        )
        .is_err()
    );
}

#[test]
fn route_roundtrip_keeps_model_api_deployment_and_adapter_versions_distinct() {
    let route = ResolvedModelRoute {
        binding: VersionedRef {
            id: id("binding"),
            version: id("binding-revision"),
        },
        catalog_revision: id("catalog-1"),
        routing_policy_revision: id("policy-1"),
        requested_model: id("requested-alias"),
        model_id: id("model-family"),
        model_version: id("release-A"),
        version_semantics: VersionSemantics::MutableDeployment,
        provider: id("custom-provider"),
        target: BTreeMap::from([("deployment".into(), json!("deployment-name"))]),
        deployment_revision: Some(id("deployment-revision")),
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("api-contract-version"),
        },
        adapter: VersionedRef {
            id: id("adapter"),
            version: id("adapter-version"),
        },
        capability_revision: id("capabilities-1"),
        connection_ref: VersionedRef {
            id: id("connection"),
            version: id("connection-revision"),
        },
    };
    let restored: ResolvedModelRoute =
        serde_json::from_str(&serde_json::to_string(&route).unwrap()).unwrap();
    assert_eq!(restored, route);
    let original = route.digest();
    let mut changed = route.clone();
    changed.model_version = id("release-B");
    assert_ne!(changed.digest(), original);
    changed = route.clone();
    changed.api_contract.version = id("different-api-version");
    assert_ne!(changed.digest(), original);
    changed = route.clone();
    changed.deployment_revision = Some(id("new-deployment-revision"));
    assert_ne!(changed.digest(), original);
    let record = ModelInvocationRecord {
        run_id: id("run"),
        model_step_id: id("step"),
        attempt_id: id("attempt"),
        purpose: ModelPurpose::Agent,
        route,
        selection_reason: id("policy-default"),
        request_digest: digest("request"),
        state: ModelAttemptState::Completed {},
        provider_request_id: None,
        reported_model_id: None,
        reported_model_version: None,
        usage: None,
    };
    let restored: ModelInvocationRecord =
        serde_json::from_str(&serde_json::to_string(&record).unwrap()).unwrap();
    assert_eq!(restored.reported_model_version, None);
    assert_eq!(restored.usage, None);
}
```

## `tests/support/consumer.rs`

```rust
use std::{collections::BTreeSet, error::Error};

use serde_json::json;
use wickle::{
    AgentProfile, ComponentKind, ComponentMetadata, ComponentRef, ContractError, ErrorCode, Id,
    PortFuture, ProfileResolver, ProfileValidator, ResolvedProfile, Scope, canonical_digest,
};

const PROFILE: &str = r#"{
  "schema_version": "wickle.agent-profile.v1",
  "agent_id": "information-assistant", "version": "1.0.0",
  "name": "Information assistant", "description": "Organize information",
  "instructions": {"text": "Use available evidence."},
  "model_binding": "primary", "tools": [], "skills": [], "connectors": [],
  "context_policy": {"strategy": "bounded"}, "output_contract": {"type": "text"},
  "limits": {"max_model_calls": 8, "max_tool_attempts": 0,
    "max_repair_attempts": 0, "max_recovery_attempts": 0, "max_elapsed_ms": 30000}
}"#;

fn id(value: &str) -> Id {
    Id::new(value).expect("sample identifiers are nonblank")
}

// A small Host resolver for an explicitly registered model binding.
// This is metadata resolution, not a model client or an agent driver.
struct HostResolver {
    scope: Scope,
}

impl ProfileResolver for HostResolver {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        scope: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            if scope != &self.scope
                || reference.kind != ComponentKind::ModelBinding
                || reference.id.as_str() != "primary"
                || reference.version.is_some()
            {
                return Err(ContractError::new(
                    ErrorCode::ComponentUnavailable,
                    "model_binding",
                ));
            }
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    kind: ComponentKind::ModelBinding,
                    id: id("primary"),
                    version: Some(id("binding-revision-1")),
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!({"binding": "primary", "revision": 1})),
                config_schema: json!({"type": "object", "additionalProperties": false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: None,
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let paths: Vec<_> = std::env::args().skip(1).collect();
    let input = match paths.first() {
        Some(path) => std::fs::read_to_string(path)?,
        None => PROFILE.to_owned(),
    };
    let profile = AgentProfile::from_json(&input)?;
    let scope = Scope {
        tenant_id: id("example-tenant"),
        workspace_id: id("example-workspace"),
        user_id: None,
    };
    let resolver = HostResolver {
        scope: scope.clone(),
    };
    let resolved = ProfileValidator::new(&resolver)
        .validate(&profile, &scope)
        .await?;
    let stored = serde_json::to_string(&resolved)?;
    let restored: ResolvedProfile = serde_json::from_str(&stored)?;
    restored.ensure_matches(&profile, &scope)?;
    restored.ensure_same_resolution(&resolved)?;
    println!(
        "Validated {} at version {}",
        restored.profile().agent_id,
        restored.profile().version
    );
    println!("Resolved {} component(s)", restored.components().len());
    println!("Profile digest: {}", restored.profile_digest());
    println!("Persisted profile restored and matched");
    if let Some(path) = paths.get(1) {
        let candidate = AgentProfile::from_json(&std::fs::read_to_string(path)?)?;
        restored.ensure_matches(&candidate, &scope)?;
        println!("Candidate matches the pinned profile");
    }
    Ok(())
}
```
