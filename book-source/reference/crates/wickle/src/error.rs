use serde::{Deserialize, Serialize};

/// Stable categories for contract and profile validation failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorCode {
    /// The verifier could not complete its check; this is not a quality rejection.
    VerificationUnavailable,
    /// Current policy or exact owner scope denies access.
    AccessDenied,
    /// Artifact identity, content, metadata, or size violates its immutable contract.
    InvalidArtifact,
    /// Artifact policy requires an explicit Host approval before storage access.
    ArtifactApprovalRequired,
    /// A Skill manifest, body, dependency or loader result violates its pinned contract.
    InvalidSkill,
    /// Context selection would split a complete group or remove protected data.
    InvalidContextSelection,
    /// A compressor did not produce a smaller usable context projection.
    ContextCompactionNoReduction,
    /// A model compressor failed or returned an unsupported completion.
    ContextCompactionFailed,
    /// Current Skill policy requires explicit Host approval before loading or use.
    SkillApprovalRequired,
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
    /// The requested model, version, binding or alias is not registered.
    ModelNotRegistered,
    /// An exact model/API/adapter/target binding or its evidence is inconsistent.
    ModelBindingInvalid,
    /// The required feature or declared capability limit is unsupported.
    ModelCapabilityUnsupported,
    /// Provider options do not satisfy the model and exact-binding contracts.
    ModelOptionUnsupported,
    /// The model or deployment is not verified immutable under the requested policy.
    ModelVersionUnpinned,
    /// Current observed model or deployment metadata differs from the pinned route.
    ModelVersionDrift,
    /// A bounded target inspection failed or could not establish availability.
    ModelInspectionUnavailable,
    /// A prior physical attempt has no known result and needs explicit recovery.
    ModelAttemptUnresolved,
    /// The selected release is retired or otherwise unavailable.
    ModelUnavailable,
    /// Catalog scope-independent revision, identity or serialized integrity differs.
    ModelCatalogMismatch,
    /// Static routing configuration has duplicate, missing or unsupported constraints.
    ModelRoutingInvalid,
    /// Saved routing metadata, request identity or selected route differs.
    ModelRoutingMismatch,
    /// Static routing constraints forbid this target or fallback reason.
    ModelRouteDenied,
    /// No eligible candidate remains in the finite permitted fallback list.
    ModelRoutesExhausted,
    /// Required support evidence is absent, failed, or of an insufficient kind.
    ModelSupportInsufficient,
    /// Estimated input plus reserved output exceeds this model/binding context budget.
    ModelContextIncompatible,
    /// Tool exposure, binding metadata, or a registered input schema is inconsistent.
    InvalidToolInputContract,
    /// The compiler cannot safely project this input schema or reference form.
    UnsupportedInputProjection,
    /// Model-owned or assembled tool arguments do not satisfy their input contract.
    InvalidArguments,
    /// An external receipt did not establish the tool effect; its saved wait remains.
    ToolEffectUnresolved,
    /// A supplied system value does not satisfy its registered input contract.
    SystemInputInvalid,
    /// A required registered system value is absent; the model must not invent it.
    SystemInputMissing,
    /// A read-only system-value resolver is unavailable or failed safely.
    SystemInputUnavailable,
    /// Supplied/resumed values or pinned input metadata differ from the saved snapshot.
    SystemInputsMismatch,
    /// Lookup permission requires separate Host approval before a target is known.
    SystemInputApprovalRequired,
    /// Resolver-count or serialized input-size bounds were exceeded.
    InputBindingLimitExceeded,
    /// Context identity, provenance structure, or call/result protocol is invalid.
    InvalidContext,
    /// Context scope, pinned assets, or protected-record identity does not match.
    ContextMismatch,
    /// Required context cannot fit the explicit byte or item bounds without truncation.
    ContextBudgetExceeded,
    /// A required context source is explicitly unavailable.
    ContextSourceUnavailable,
    /// Context access requires approval through a separate interactive operation.
    ContextApprovalRequired,
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
    /// A protected record was deliberately expired by the storage retention policy.
    RecordExpired,
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
    /// Authorized run diagnostics when durable storage is unavailable.
    pub persistence: Option<Box<crate::PersistenceFailure>>,
}

impl ContractError {
    /// Construct an error using a safe contract location.
    pub fn new(code: ErrorCode, path: impl Into<String>) -> Self {
        Self {
            code,
            path: path.into(),
            persistence: None,
        }
    }
}
