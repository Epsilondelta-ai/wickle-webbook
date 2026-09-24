//! Persistable execution contracts. Transaction implementations and driver
//! integration use these records; declarations alone do not enable recovery.
use crate::{
    CanonicalizationVersion, ContractError, ErrorCode, Id, JsonDigest, JsonObject, JsonTextLimits,
    PortFuture, RecordRef, ResumeCommand, RunLease, RunOutcome, Scope, StoredRun, VersionedRef,
    canonicalize_json_text, versioned_digest_json,
};
use serde::{Deserialize, Serialize};
use std::{fmt, num::NonZeroU64};

/// Version of protected segment and prepared-step records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionRecordVersion {
    /// First execution-record contract; stored independently of legacy Run checkpoints.
    #[serde(rename = "wickle.execution-record.v1")]
    V1,
}

/// Original submitted data, kept separate from resolved defaults and runtime handles.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestSnapshot {
    schema_version: RequestSnapshotVersion,
    canonicalization: CanonicalizationVersion,
    /// Original submitted profile identity, not a newly resolved definition.
    pub profile_ref: VersionedRef,
    request_json: String,
    system_inputs_json: String,
    system_inputs_provided: bool,
    digest: JsonDigest,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum RequestSnapshotVersion {
    #[serde(rename = "wickle.request-snapshot.v1")]
    V1,
}
impl fmt::Debug for RequestSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestSnapshot")
            .field("canonicalization", &self.canonicalization)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}
impl RequestSnapshot {
    /// Capture validated caller JSON without resolving a catalog or binding.
    /// Start's absent system inputs become an empty object. Empty/omitted model options are normalized identically; other submitted
    /// fields retain their explicit presence. Request schema/authorization are the
    /// admission boundary's responsibility; capture alone does not admit a Run.
    pub fn capture(
        profile_ref: VersionedRef,
        request_json: &str,
        system_inputs_json: Option<&str>,
        limits: JsonTextLimits,
    ) -> Result<Self, ContractError> {
        let system_inputs_provided = system_inputs_json.is_some();
        let request = canonicalize_json_text(request_json, limits)?;
        let system = canonicalize_json_text(system_inputs_json.unwrap_or("{}"), limits)?;
        if request.first() != Some(&b'{') || system.first() != Some(&b'{') {
            return Err(ContractError::new(
                ErrorCode::InvalidContract,
                "request_snapshot.object",
            ));
        }
        let request_json = String::from_utf8(request)
            .map_err(|_| ContractError::new(ErrorCode::InvalidJson, "request_snapshot"))?;
        let system_inputs_json = String::from_utf8(system)
            .map_err(|_| ContractError::new(ErrorCode::InvalidJson, "request_snapshot"))?;
        let canonicalization = CanonicalizationVersion::WickleCanonicalJsonV1;
        let digest = Self::compute(
            &profile_ref,
            &request_json,
            &system_inputs_json,
            canonicalization,
            limits,
        )?;
        Ok(Self {
            schema_version: RequestSnapshotVersion::V1,
            canonicalization,
            profile_ref,
            request_json,
            system_inputs_json,
            system_inputs_provided,
            digest,
        })
    }
    fn compute(
        profile: &VersionedRef,
        request: &str,
        system: &str,
        version: CanonicalizationVersion,
        limits: JsonTextLimits,
    ) -> Result<JsonDigest, ContractError> {
        // Omitted and empty model option maps are the same submitted override.
        // RawValue keeps nested numeric tokens intact while removing only that key.
        let mut fields: std::collections::BTreeMap<String, Box<serde_json::value::RawValue>> =
            serde_json::from_str(request).map_err(|_| {
                ContractError::new(ErrorCode::InvalidJson, "request_snapshot.request")
            })?;
        if let Some(options) = fields.get("model_options") {
            if !options.get().starts_with('{') {
                return Err(ContractError::new(
                    ErrorCode::InvalidContract,
                    "request_snapshot.model_options",
                ));
            }
            if options.get() == "{}" {
                fields.remove("model_options");
            }
        }
        let request = serde_json::to_string(&fields)
            .map_err(|_| ContractError::new(ErrorCode::InvalidJson, "request_snapshot.request"))?;
        let profile = serde_json::to_string(profile)
            .map_err(|_| ContractError::new(ErrorCode::InvalidJson, "request_snapshot.profile"))?;
        let envelope = format!(
            "{{\"profile_ref\":{profile},\"request\":{request},\"system_inputs\":{system}}}"
        );
        versioned_digest_json(&envelope, version, limits)
    }
    /// Validate a stored record before comparison. Never trust a supplied digest.
    pub fn validate(&self, limits: JsonTextLimits) -> Result<(), ContractError> {
        let request = canonicalize_json_text(&self.request_json, limits)?;
        let system = canonicalize_json_text(&self.system_inputs_json, limits)?;
        if request.first() != Some(&b'{')
            || system.first() != Some(&b'{')
            || (!self.system_inputs_provided && system != b"{}")
            || Self::compute(
                &self.profile_ref,
                &self.request_json,
                &self.system_inputs_json,
                self.canonicalization,
                limits,
            )? != self.digest
        {
            return Err(ContractError::new(
                ErrorCode::InvalidSnapshot,
                "request_snapshot",
            ));
        }
        Ok(())
    }
    /// Compare another submission using this stored record's normalization and
    /// canonicalization rules, not the newer candidate's computed digest.
    pub fn matches_submission(
        &self,
        candidate: &Self,
        limits: JsonTextLimits,
    ) -> Result<bool, ContractError> {
        self.validate(limits)?;
        candidate.validate(limits)?;
        Ok(Self::compute(
            &candidate.profile_ref,
            &candidate.request_json,
            &candidate.system_inputs_json,
            self.canonicalization,
            limits,
        )? == self.digest)
    }
    /// Version that must be used when comparing a resubmission.
    pub fn canonicalization(&self) -> CanonicalizationVersion {
        self.canonicalization
    }
    /// Digest of the submitted profile, request and system input envelope.
    pub fn digest(&self) -> &JsonDigest {
        &self.digest
    }
    /// Privileged access to the submitted request JSON, retaining number lexemes.
    pub fn request_json(&self) -> &str {
        &self.request_json
    }
    /// Whether Start explicitly supplied system inputs. Omission still hashes as {}.
    pub fn system_inputs_provided(&self) -> bool {
        self.system_inputs_provided
    }
    /// Privileged access to protected system inputs. Never include in model context.
    pub fn system_inputs_json(&self) -> &str {
        &self.system_inputs_json
    }
}

/// Application-defined state, distinct from core execution status.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppState {
    /// Registered Host schema namespace.
    pub namespace: Id,
    /// Host-defined business status.
    pub status: Id,
    /// Validated against the registered Host schema before persistence.
    pub metadata: JsonObject,
}
impl fmt::Debug for AppState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppState")
            .field("namespace", &self.namespace)
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}
/// Why a segment stopped; protected causes cannot be overridden by app policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterruptionCause {
    /// Host is shutting down or handing ownership off.
    HostShutdown,
    /// Segment stop without a durable user cancellation.
    SegmentStopped,
    /// Explicit user cancellation.
    UserCancel,
    /// Run budget or deadline exhausted.
    BudgetExhausted,
    /// Ownership/fencing no longer valid.
    OwnershipLost,
    /// Snapshot cannot safely be recovered.
    RecoveryUnavailable,
}
/// Persisted evidence of a stopped execution segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InterruptionRecord {
    /// Segment whose execution stopped.
    pub segment_id: Id,
    /// Classified cause, not inferred from observer disconnection.
    pub cause: InterruptionCause,
    /// Last confirmed checkpoint revision.
    pub checkpoint_revision: u64,
    /// Whether a valid saved recovery path exists.
    pub recoverable: bool,
    /// Existing uncertain effects; policy must not remove these.
    pub unresolved_effects: Vec<RecordRef>,
}
/// Outcome for a particular segment. Resuming creates a different record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SegmentOutcome {
    /// Confirmed interval outcome: waiting, interrupted, or terminal.
    Settled {
        /// Saved authoritative outcome.
        outcome: Box<RunOutcome>,
    },
    /// Recoverable stop, not a terminal Run outcome.
    Interrupted {
        /// Stop/recovery evidence.
        interruption: InterruptionRecord,
    },
}
/// Immutable identity and result of one accepted execution interval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSegment {
    /// Explicit protected-record schema version.
    pub schema_version: ExecutionRecordVersion,
    /// Original execution principal; a reviewer/control submitter cannot replace it.
    pub execution_principal_ref: Id,
    /// Run that owns this segment.
    pub run_id: Id,
    /// Never reused when the Run is resumed.
    pub segment_id: Id,
    /// Revision at acceptance.
    pub accepted_revision: u64,
    /// Present after waiting, interruption or terminal settlement.
    pub outcome: Option<SegmentOutcome>,
    /// Last event belonging to this settled interval, immutable after a new interval starts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_event_seq: Option<u64>,
    /// Original checkpoint when recovery/control archives an unsettled interval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_snapshot_ref: Option<RecordRef>,
    /// App state snapshot at settlement; never defines core status.
    pub app_state: Option<AppState>,
}
/// Frozen prepared-model inputs. References belong to the same authorized scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedStepRecord {
    /// Independently pinned logical input and original conversation boundary.
    pub step_input: RecordRef,
    /// Exact namespace owning all protected references.
    pub scope: crate::Scope,
    /// Owning execution.
    pub run_id: Id,
    /// Pinned resolved profile identity.
    pub profile_digest: JsonDigest,
    /// Optional pinned component assembly.
    pub assembly_ref: Option<RecordRef>,
    /// Final provider input identity independent of physical attempt IDs.
    pub projection_fingerprint: JsonDigest,
    /// Initial preparation or the explicit reason for changing the projection.
    pub change_reason: Id,
    /// Explicit protected-record schema version.
    pub schema_version: ExecutionRecordVersion,
    /// Agent, verification or compaction invocation budget/policy purpose.
    pub purpose: crate::ModelPurpose,
    /// Protected compiled-provider contract records for this exact projection.
    pub compiled_tools: Vec<RecordRef>,
    /// Logical model step identity.
    pub model_step_id: Id,
    /// Changes when the model input changes, not on identical transport retries.
    pub projection_revision: NonZeroU64,
    /// Pinned route, effective options and their provenance.
    pub model_configuration: RecordRef,
    /// Pinned canonical tool set and compiled provider contracts.
    pub tool_set: RecordRef,
    /// Persisted assembled prompt/input projection.
    pub context_projection: RecordRef,
    /// Compiler and assembler contract identity.
    pub assembler: VersionedRef,
}
/// A durable control request. Submission is not a completed state transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlAction {
    /// Permanently cancel a Run, preserving known/unknown effects.
    Cancel {
        /// Auditable reason identifier.
        reason: Id,
    },
    /// Stop the active segment without declaring Run success.
    Stop {
        /// Why the Host stopped execution.
        cause: InterruptionCause,
    },
    /// Explicitly process a Run deadline that has elapsed.
    Expire,
}
/// Deduplicated control command, authenticated by the Host before submission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlCommand {
    /// Stable command identity within a Run.
    pub command_id: Id,
    /// Requesting principal, distinct from the original execution principal.
    pub principal_ref: Id,
    /// Authorized action.
    pub action: ControlAction,
}
/// Why a transaction creates or claims a segment.
#[derive(Debug, Clone)]
pub enum SegmentStart {
    /// Claim the segment already identified by admission.
    Initial,
    /// Consume a matching approval/input/recovery command.
    Resume(ResumeCommand),
    /// Consume an already persisted control command, without model/tool execution.
    Control(Id),
}
/// Inputs for atomic command consumption, lease acquisition and segment creation.
#[derive(Clone)]
pub struct BeginSegmentRequest {
    /// Proposed validated checkpoint/events for resume or control. Initial claim has None.
    pub transition: Option<SegmentTransition>,
    /// Owning Run.
    pub run_id: Id,
    /// CAS revision before this transaction.
    pub expected_revision: u64,
    /// Proposed new segment ID (initial claims use admission's ID).
    pub segment_id: Id,
    /// New execution owner.
    pub owner: Id,
    /// Trusted clock value.
    pub now_ms: i64,
    /// Bounded positive ownership TTL.
    pub lease_ttl_ms: NonZeroU64,
    /// Command or initial admission claim.
    pub start: SegmentStart,
}
/// Result of atomic segment acceptance; replay must not launch another driver.
#[derive(Clone)]
pub struct BeginSegmentResult {
    /// Saved execution state.
    pub state: StoredRun,
    /// Accepted or previously accepted segment.
    pub segment: ExecutionSegment,
    /// Only a new owner receives a lease. Replays have None.
    pub lease: Option<RunLease>,
}
/// Acknowledgment of durable command acceptance, not execution completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlReceipt {
    /// Owning Run.
    pub run_id: Id,
    /// Deduplicated command.
    pub command_id: Id,
    /// Segment that processed it, if already consumed.
    pub processed_segment_id: Option<Id>,
}
/// Atomic operations that storage implementations must supply before the new
/// segment driver can be enabled. No default read/then/write emulation is safe.
/// StateStore integration requires the same transaction as its snapshot/events.
pub trait ExecutionTransactions: Send + Sync {
    /// Read protected segment/command history without mutation; Host authorizes access.
    fn read_execution<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
    ) -> PortFuture<'a, ExecutionHistory>;

    /// Atomically deduplicate/consume the command, check CAS and allocate ownership.
    fn begin_segment<'a>(
        &'a self,
        scope: &'a Scope,
        request: BeginSegmentRequest,
    ) -> PortFuture<'a, BeginSegmentResult>;
    /// Atomically persist a command ID and payload; conflicting reuse is rejected.
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        command: ControlCommand,
    ) -> PortFuture<'a, ControlReceipt>;
}

/// Allowed callback proposals. None may declare success or overwrite effect evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterruptionAction {
    /// Use the core's cause-specific default.
    UseDefault,
    /// Save a recoverable interrupted segment when the core permits it.
    Pause,
    /// Cancel when the cause permits this transition.
    Cancel,
    /// Fail when the cause permits this transition.
    Fail,
}
/// Limited immutable data exposed to the application interruption policy.
#[derive(Debug, Clone)]
pub struct InterruptionInfo {
    /// Last confirmed core cursor, not an app-defined phase.
    pub phase: crate::RunPhase,
    /// Actual persistence/recovery capabilities of the bound store.
    pub store_capabilities: crate::StateStoreCapabilities,
    /// Fixed nonsecret callback configuration.
    pub configuration: JsonObject,
    /// Resource scope; this does not grant authorization.
    pub scope: Scope,
    /// Stopped Run identity.
    pub run_id: Id,
    /// Fixed interruption evidence.
    pub interruption: InterruptionRecord,
    /// Current app state without mutable access to the Run.
    pub app_state: Option<AppState>,
}
/// Policy result; core validation and persistence remain mandatory.
#[derive(Debug, Clone)]
pub struct InterruptionDecision {
    /// Suggested transition.
    pub action: InterruptionAction,
    /// Optional Host-schema-validated business state.
    pub app_state: Option<AppState>,
}
/// A versioned application policy with no state-store or tool execution handle.
pub trait InterruptionPolicy: Send + Sync {
    /// Immutable identity pinned at admission.
    fn identity(&self) -> VersionedRef;
    /// Cooperatively compute a proposal. The driver owns timeout and fallback.
    fn decide<'a>(&'a self, info: &'a InterruptionInfo) -> PortFuture<'a, InterruptionDecision>;
}

impl ExecutionSegment {
    /// Check structural settlement invariants before persistence; authorization,
    /// Host app-state schema and atomic ledger transitions remain store/driver checks.
    pub fn validate(&self) -> Result<(), ContractError> {
        let invalid = || ContractError::new(ErrorCode::InvalidSnapshot, "execution_segment");
        if self.source_snapshot_ref.is_some()
            && !matches!(self.outcome, Some(SegmentOutcome::Interrupted { .. }))
        {
            return Err(invalid());
        }
        if self.last_event_seq.is_some_and(|seq| seq == 0)
            || (self.outcome.is_none() && self.last_event_seq.is_some())
        {
            return Err(invalid());
        }
        match &self.outcome {
            Some(SegmentOutcome::Settled { outcome }) => {
                outcome.validate()?;
                if outcome.checkpoint_revision < self.accepted_revision
                    || outcome.app_state != self.app_state
                {
                    return Err(invalid());
                }
            }
            Some(SegmentOutcome::Interrupted { interruption })
                if interruption.segment_id != self.segment_id
                    || interruption.checkpoint_revision < self.accepted_revision
                    || !interruption.recoverable
                    || !matches!(
                        interruption.cause,
                        InterruptionCause::HostShutdown | InterruptionCause::SegmentStopped
                    ) =>
            {
                return Err(invalid());
            }
            Some(SegmentOutcome::Interrupted { .. }) => {}
            None => {}
        }
        Ok(())
    }
}

/// A proposed checkpoint delta; the transaction supplies its own validated lease.
#[derive(Clone)]
pub struct SegmentTransition {
    /// Next checkpoint at expected_revision + 1.
    pub snapshot: crate::RunSnapshot,
    /// New transcript messages.
    pub messages: Vec<crate::Message>,
    /// Matching durable events.
    pub events: Vec<crate::RunEvent>,
    /// Immutable records referenced by the new checkpoint/events.
    pub records: Vec<crate::ProtectedRecord>,
}
/// Persisted control command and its optional processing result.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredControlCommand {
    /// Original authenticated command payload.
    pub command: ControlCommand,
    /// Segment that consumed it; absent means pending, not failed.
    pub processed_segment_id: Option<Id>,
}
/// Command deduplication evidence bound to an accepted segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedSegmentCommand {
    /// Stable command identity.
    pub command_id: Id,
    /// Original normalized command payload digest.
    pub payload_digest: JsonDigest,
    /// Segment created by this command.
    pub segment_id: Id,
}
/// Protected execution history stored atomically with a Run's checkpoint/events.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionHistory {
    /// Owning Run.
    pub run_id: Id,
    /// Authenticated original execution actor.
    pub execution_principal_ref: Id,
    /// Admission-pinned grant. Legacy histories without this field require draining.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_grant_ref: Option<Id>,
    /// Submitted request evidence, separate from effective configuration.
    pub submitted: Option<RequestSnapshot>,
    /// Initial segment has been claimed at least once. Retrying a claim is not recovery.
    pub initial_claimed: bool,
    /// Ordered immutable past segments and the current segment.
    pub segments: Vec<ExecutionSegment>,
    /// Accepted resume/control command identities.
    pub accepted_commands: Vec<AcceptedSegmentCommand>,
    /// Pending and processed durable controls.
    pub controls: Vec<StoredControlCommand>,
}
impl fmt::Debug for ExecutionHistory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionHistory")
            .field("run_id", &self.run_id)
            .field("segment_count", &self.segments.len())
            .finish_non_exhaustive()
    }
}
