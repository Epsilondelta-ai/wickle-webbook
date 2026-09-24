use crate::{
    ArtifactRef, AttemptReservation, CompletionPolicy, ContractError, ErrorCode, Failure, Id,
    InputContent, JsonDigest, JsonObject, ModelAttemptState, ModelInvocationRecord, RecordRef,
    ReservationKind, ResolvedProfile, RunLimits, RunTiming, Scope, ToolCall, ToolResult,
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
    /// Logical model options authorized by the Host and pinned with the admitted request.
    /// Catalog schemas define supported keys; credentials and raw provider bodies do not belong here.
    #[serde(default, skip_serializing_if = "JsonObject::is_empty")]
    pub model_options: JsonObject,
    /// Requested per-model output cap; positive, authorized and bounded by Host/profile/model limits.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_output_tokens: Option<NonZeroU64>,
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

/// Durable acceptance of one resume command and the segment it continued.
/// Values and references are protected run data, not public event payloads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeReceipt {
    /// Exact command, including the decision or answer used for deduplication.
    pub command: ResumeCommand,
    /// Protected immutable copy used by the resumed event.
    pub command_ref: RecordRef,
    /// Revision at which this command was accepted and its new segment began.
    pub accepted_revision: u64,
    /// Whether the original wait or Run deadline had already elapsed at acceptance.
    /// An expired approval is not execution authority.
    #[serde(default)]
    pub expired: bool,
    /// Start revision of the preceding segment; zero identifies the initial segment.
    pub previous_segment_start_revision: u64,
    /// Original saved Waiting outcome returned by handles for the preceding segment.
    pub previous_outcome_ref: RecordRef,
    /// Last durable event in the preceding segment.
    pub previous_last_event_seq: u64,
    /// Authenticated actor who authorized this command.
    pub actor_ref: Id,
    /// Current Host grant used when accepting the command.
    pub capability_grant_ref: Id,
}

/// Public run status categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Actively processing.
    Running,
    /// Persisted wait.
    Waiting,
    /// Saved nonterminal segment interruption; requires the new execution contract.
    Interrupted,
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
        !matches!(self, Self::Running | Self::Waiting | Self::Interrupted)
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
    /// Execution interval stopped with a saved checkpoint; the Run is not terminal.
    Interrupted {
        /// Core-issued cause, checkpoint and unresolved-effect evidence.
        interruption: crate::InterruptionRecord,
    },
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
            Self::Interrupted { .. } => RunStatus::Interrupted,
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
    /// Host-schema-validated business state, separate from the core result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_state: Option<crate::AppState>,
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
        if let OutcomeResult::Interrupted { interruption } = &self.result {
            if !interruption.recoverable
                || interruption.checkpoint_revision >= self.checkpoint_revision
                || !matches!(
                    interruption.cause,
                    crate::InterruptionCause::HostShutdown
                        | crate::InterruptionCause::SegmentStopped
                )
                || interruption.unresolved_effects != self.unresolved_effects
            {
                return Err(ContractError::new(
                    ErrorCode::InvalidContract,
                    "outcome.interruption",
                ));
            }
        }
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
    /// A charged attempt was stopped for approval before entering the executor.
    ApprovalPending {
        /// Reservation that remains charged even though execution did not start.
        attempt_id: Id,
        /// Frozen effect key reused if execution is later authorized.
        idempotency_key: Id,
    },
    /// A no-effect input request is awaiting an answer instead of reentering its executor.
    InputPending {
        /// Charged attempt that requested the input.
        attempt_id: Id,
        /// Original effect identity, retained while the call is incomplete.
        idempotency_key: Id,
        /// Core-generated question tied to this call. Its exact compiled output
        /// schema validates the answer when schema_ref is absent.
        request: InputRequest,
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
    /// Latest validated cumulative context view; the original transcript is retained.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub context_revision_ref: Option<RecordRef>,
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
    /// Admission-pinned interruption policy/configuration and app-state schema.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub interruption_plan_ref: Option<RecordRef>,
    /// Append-only protected stop decisions, including callback fallback diagnostics.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub interruption_records: Vec<RecordRef>,
    /// Current Host-defined business state; never interpreted as core status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_state: Option<crate::AppState>,
    /// Immutable logical-step submissions and their transcript boundaries.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_step_inputs: Vec<RecordRef>,
    /// Append-only prepared inputs, including auxiliary model purposes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prepared_steps: Vec<RecordRef>,
    /// Current Agent preparation; cleared when a new logical step begins.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub active_prepared_step: Option<RecordRef>,
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
    /// Original admission/deadline and persisted monotonic elapsed-time anchor.
    pub timing: RunTiming,
    /// Append-only charged attempt reservations, preserved across errors and resume.
    pub reservations: Vec<AttemptReservation>,
    /// Append-only resume acceptances and prior segment outcomes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resume_receipts: Vec<ResumeReceipt>,
    /// Accepted recoveries of interrupted Running segments.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recovery_receipts: Vec<crate::RecoveryReceipt>,
    /// Exact selected lifecycle definitions pinned before any hook executes.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub hook_plan_ref: Option<RecordRef>,
    /// Applied lifecycle transformations, preserved in their invocation order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hook_applications: Vec<crate::HookApplication>,
    /// Exact source definitions, limits, and token-estimator revision pinned at admission.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub source_plan_ref: Option<RecordRef>,
    /// Exact selected Skill manifests, configuration, and loader contract pinned at admission.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub skill_plan_ref: Option<RecordRef>,
    /// Context strategy, compressor and bounds pinned for this Run.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub context_plan_ref: Option<RecordRef>,
    /// Latest cumulative view inherited from or committed to the owning session.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub context_revision_ref: Option<RecordRef>,
    /// Completed compression decisions, including rejected inputs that must not repeat.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context_decisions: Vec<RecordRef>,
    /// Pinned output format and verifier criteria.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub verification_plan_ref: Option<RecordRef>,
    /// Candidate awaiting a quality decision or completion.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub candidate_ref: Option<RecordRef>,
    /// Append-only candidate decisions and format/transport failures.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verification_records: Vec<RecordRef>,
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
    /// Immutable catalog and routing policy used by this run's model calls.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub routing_snapshot_ref: Option<RecordRef>,
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
        if self.status == RunStatus::Interrupted
            && (self.interruption_plan_ref.is_none()
                || self.interruption_records.is_empty()
                || self.wait.is_some()
                || matches!(self.phase, RunPhase::Waiting | RunPhase::Finish)
                || !matches!(
                    self.outcome.as_ref().map(|outcome| &outcome.result),
                    Some(OutcomeResult::Interrupted { .. })
                ))
        {
            return Err(invalid("interrupted.evidence"));
        }
        crate::budget::validate_budget(self)?;
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
                || outcome.app_state != self.app_state
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
        let mut commands = BTreeSet::new();
        let mut segment = 0;
        if self
            .resume_receipts
            .windows(2)
            .any(|pair| pair[0].accepted_revision >= pair[1].accepted_revision)
            || self
                .recovery_receipts
                .windows(2)
                .any(|pair| pair[0].accepted_revision >= pair[1].accepted_revision)
        {
            return Err(invalid("command_receipts.order"));
        }
        let mut receipts: Vec<_> = self
            .resume_receipts
            .iter()
            .map(|receipt| {
                (
                    &receipt.command,
                    receipt.accepted_revision,
                    receipt.previous_segment_start_revision,
                    receipt.previous_last_event_seq,
                    false,
                )
            })
            .chain(self.recovery_receipts.iter().map(|receipt| {
                (
                    &receipt.command,
                    receipt.accepted_revision,
                    receipt.previous_segment_start_revision,
                    receipt.previous_last_event_seq,
                    true,
                )
            }))
            .collect();
        receipts.sort_by_key(|receipt| receipt.1);
        for (command, accepted_revision, previous_segment, previous_event, recovery) in receipts {
            if command.run_id != self.run_id
                || !commands.insert(&command.command_id)
                || accepted_revision > self.revision
                || command.expected_revision.checked_add(1) != Some(accepted_revision)
                || previous_segment != segment
                || command.expected_revision < segment
                || previous_event > self.last_event_seq
                || matches!(command.action, ResumeAction::Recover { .. }) != recovery
            {
                return Err(invalid("command_receipts"));
            }
            segment = accepted_revision;
        }
        let mut calls = BTreeSet::new();
        for entry in &self.tool_ledger {
            if !calls.insert(&entry.call.call_id) {
                return Err(invalid("tool_ledger.call_id"));
            }
            let unregistered_safe = match &entry.state {
                ToolCallState::Planned {} => true,
                ToolCallState::Settled { result } => {
                    result.effect == crate::ToolEffect::NotApplied
                        && matches!(
                            result.status,
                            crate::ToolResultStatus::Failed
                                | crate::ToolResultStatus::Denied
                                | crate::ToolResultStatus::Cancelled
                        )
                }
                _ => false,
            };
            if entry.call.descriptor_digest.is_none()
                && (entry.call.bound_input_ref.is_some() || !unregistered_safe)
            {
                return Err(invalid("tool_ledger.unregistered"));
            }
            match &entry.state {
                ToolCallState::Dispatching { attempt_id, .. } | ToolCallState::Unknown { attempt_id, .. } | ToolCallState::ApprovalPending { attempt_id, .. } | ToolCallState::InputPending { attempt_id, .. }
                    if !self.reservations.iter().any(|reservation| &reservation.attempt_id == attempt_id
                        && matches!(&reservation.kind, ReservationKind::Tool { call_id } if call_id == &entry.call.call_id)) =>
                {
                    return Err(invalid("tool_ledger.reservation"));
                }
                ToolCallState::Dispatching { .. } | ToolCallState::Unknown { .. } | ToolCallState::ApprovalPending { .. } | ToolCallState::InputPending { .. }
                    if entry.call.bound_input_ref.is_none() =>
                {
                    return Err(invalid("tool_ledger.bound_input_ref"));
                }
                ToolCallState::Settled { result } if result.call_id != entry.call.call_id => {
                    return Err(invalid("tool_ledger.result.call_id"));
                }
                ToolCallState::InputPending { request, .. }
                    if request.call_id != entry.call.call_id || request.question.trim().is_empty() =>
                {
                    return Err(invalid("tool_ledger.input_request"));
                }
                _ => {}
            }
            if self.status == RunStatus::Succeeded
                && !matches!(&entry.state, ToolCallState::Settled { result } if result.status != crate::ToolResultStatus::Unknown && result.effect != crate::ToolEffect::Unknown)
            {
                return Err(invalid("tool_ledger.unsettled"));
            }
        }
        let mut attempts = BTreeSet::new();
        let model_reservations: BTreeMap<_, _> = self
            .reservations
            .iter()
            .filter_map(|reservation| match reservation.kind {
                ReservationKind::Model { purpose } => Some((&reservation.attempt_id, purpose)),
                _ => None,
            })
            .collect();
        for invocation in &self.model_ledger {
            if invocation.run_id != self.run_id || !attempts.insert(&invocation.attempt_id) {
                return Err(invalid("model_ledger.attempt_id"));
            }
            if model_reservations.get(&invocation.attempt_id) != Some(&invocation.purpose) {
                return Err(invalid("model_ledger.reservation"));
            }
            let settled = matches!(
                invocation.state,
                ModelAttemptState::Completed {} | ModelAttemptState::Failed { .. }
            );
            if settled != invocation.response_ref.is_some() {
                return Err(invalid("model_ledger.response_ref"));
            }
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
    /// Confirmed nonterminal stop, committed with its decision and outcome.
    #[serde(rename = "run.interrupted")]
    RunInterrupted {
        /// Saved interval outcome.
        outcome_ref: RecordRef,
        /// Protected policy/default decision and safe callback diagnostics.
        decision_ref: RecordRef,
    },
    /// A separately stored context view was adopted; original messages were not changed.
    ContextRewritten {
        /// Exact protected cumulative revision.
        revision_ref: RecordRef,
    },
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
    /// Read-only reconciliation evidence committed with its corrected result.
    #[serde(rename = "tool.reconciled")]
    ToolReconciled {
        /// Protected identity, recovery reservation and result evidence.
        reconciliation_ref: RecordRef,
    },
    /// A recorded result cannot establish whether an external operation applied.
    #[serde(rename = "tool.unresolved")]
    ToolUnresolved {
        /// Protected paired Unknown result.
        result_ref: RecordRef,
        /// Uncertain physical attempt whose reservation remains charged.
        attempt_id: Id,
        /// Original external effect key, retained for reconciliation.
        idempotency_key: Id,
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
        /// Exact interval outcome. Absent only in legacy or outcome-less checkpoints.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome_ref: Option<RecordRef>,
        /// Recorded wait.
        wait_ref: RecordRef,
    },
    /// Resume command consumed.
    #[serde(rename = "run.resumed")]
    RunResumed {
        /// Consumed command record.
        command_ref: RecordRef,
    },
    /// Interrupted Running segment recovered under a new lease.
    #[serde(rename = "run.recovered")]
    RunRecovered {
        /// Exact accepted recovery receipt, including the original checkpoint reference.
        recovery_receipt_ref: RecordRef,
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
