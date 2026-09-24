# 08장 전체 Rust 구현과 테스트

[강의로](../08-model.md) · [전체 변경 패치](../solutions/08-model.patch)

기준 `d910e8cd8268c42235b1a9157016c548e860d628`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle/src/budget.rs`

```rust
use std::{
    collections::BTreeSet,
    future::Future,
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    BudgetKind, BudgetUsage, Clock, CommitInput, ContractError, ErrorCode, Id, IdSource,
    ModelPurpose, RunLease, RunLimits, RunSnapshot, RunStatus, Scope, StateStore,
};

/// Persisted time anchors. Within an execution segment, elapsed time comes from
/// the monotonic clock; UTC is used only to attach a new segment to saved progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunTiming {
    /// Original admission UTC milliseconds; immutable across waiting and resume.
    pub started_at_ms: i64,
    /// Original finite UTC deadline; immutable across waiting and resume.
    pub deadline_at_ms: i64,
    /// Admission time plus saved elapsed time, preserving the monotonic high-water mark.
    pub last_observed_at_ms: i64,
}

impl RunTiming {
    /// Establish a finite deadline at admission without reading a clock implicitly.
    pub fn new(started_at_ms: i64, max_elapsed_ms: u64) -> Result<Self, ContractError> {
        let duration = i64::try_from(max_elapsed_ms)
            .ok()
            .filter(|ms| *ms > 0)
            .ok_or_else(|| failure(ErrorCode::InvalidContract, "timing.duration"))?;
        let deadline_at_ms = started_at_ms
            .checked_add(duration)
            .ok_or_else(|| failure(ErrorCode::InvalidContract, "timing.deadline"))?;
        Ok(Self {
            started_at_ms,
            deadline_at_ms,
            last_observed_at_ms: started_at_ms,
        })
    }
}

/// The counter charged by one saved reservation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReservationKind {
    /// One physical model call. All purposes share the model-call limit.
    Model {
        /// Agent, verification, or compaction purpose for accounting.
        purpose: ModelPurpose,
    },
    /// One physical tool dispatch, associated with a previously planned call.
    Tool {
        /// Stable logical tool call identity; not a generated business foreign key.
        call_id: Id,
    },
    /// One candidate-repair decision; any resulting model call is charged separately.
    Repair {},
    /// One recovery decision; any resulting external call is charged separately.
    Recovery {},
}

impl ReservationKind {
    /// Counter whose capacity this reservation consumes.
    pub fn budget_kind(&self) -> BudgetKind {
        match self {
            Self::Model { .. } => BudgetKind::ModelCalls,
            Self::Tool { .. } => BudgetKind::ToolAttempts,
            Self::Repair {} => BudgetKind::RepairAttempts,
            Self::Recovery {} => BudgetKind::RecoveryAttempts,
        }
    }
}

/// An immutable charged reservation saved before constructing an external call.
/// It is not a success claim or proof that an external effect occurred. Result
/// settlement belongs to the corresponding model/tool ledger; reservations are
/// never automatically refunded after errors, cancellation, or Future drop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptReservation {
    /// New physical attempt or decision identity from the Host identifier source.
    pub attempt_id: Id,
    /// Charged operation and its accounting purpose.
    pub kind: ReservationKind,
    /// Effective UTC time at the reservation boundary, based on monotonic progress.
    pub reserved_at_ms: i64,
}

/// Budget and cancellation boundaries for one execution segment of a saved run.
/// This helper does not spawn a driver, retry operations, authorize business data,
/// or reconcile external effects. The driver owns its Future independently of
/// presentation handles. Dropping this value or its waiter does not cancel the run.
pub struct RunBudget {
    store: Arc<dyn StateStore>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
    scope: Scope,
    run_id: Id,
    lease: RunLease,
    cancellation: CancellationToken,
    timing: RunTiming,
    anchor_monotonic_ms: u64,
    anchor_elapsed_ms: u64,
    last_monotonic_ms: Mutex<u64>,
}

impl RunBudget {
    /// Attach to the original saved time budget and usage. A resumed UTC clock
    /// behind the saved high-water mark is rejected instead of extending time.
    /// A waiting run may attach a deadline observer, but cannot reserve new work.
    #[allow(clippy::too_many_arguments)]
    pub async fn attach(
        store: Arc<dyn StateStore>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdSource>,
        scope: Scope,
        run_id: Id,
        lease: RunLease,
        cancellation: CancellationToken,
    ) -> Result<Self, ContractError> {
        if lease.scope != scope || lease.run_id != run_id {
            return Err(failure(ErrorCode::AccessDenied, "scope"));
        }
        let saved = store.load(&scope, &run_id).await?;
        saved.snapshot.validate()?;
        if saved.snapshot.status.is_terminal() {
            return Err(failure(ErrorCode::InvalidTransition, "run.status"));
        }
        let reading = clock.now()?;
        let downtime = reading
            .utc_ms
            .checked_sub(saved.snapshot.timing.last_observed_at_ms)
            .filter(|delta| *delta >= 0)
            .ok_or_else(|| failure(ErrorCode::ClockRegression, "clock.resume"))?
            as u64;
        let anchor_elapsed_ms = saved
            .snapshot
            .usage
            .elapsed_ms
            .checked_add(downtime)
            .ok_or_else(|| failure(ErrorCode::ClockUnavailable, "clock.elapsed"))?;
        Ok(Self {
            store,
            clock,
            ids,
            scope,
            run_id,
            lease,
            cancellation,
            timing: saved.snapshot.timing,
            anchor_monotonic_ms: reading.monotonic_ms,
            anchor_elapsed_ms,
            last_monotonic_ms: Mutex::new(reading.monotonic_ms),
        })
    }

    /// Monotonic elapsed time in this segment, including the saved duration and
    /// downtime observed when attaching. Wall-clock jumps during the segment do
    /// not reset or distort this elapsed value.
    pub fn elapsed_ms(&self) -> Result<u64, ContractError> {
        // Serialize local clock readings so concurrent callers cannot observe them
        // out of order. The guard is released before any asynchronous work.
        let mut last = self
            .last_monotonic_ms
            .lock()
            .map_err(|_| failure(ErrorCode::ClockUnavailable, "clock.monotonic"))?;
        let now = self.clock.now()?.monotonic_ms;
        if now < *last {
            return Err(failure(ErrorCode::ClockRegression, "clock.monotonic"));
        }
        *last = now;
        let delta = now
            .checked_sub(self.anchor_monotonic_ms)
            .ok_or_else(|| failure(ErrorCode::ClockRegression, "clock.monotonic"))?;
        self.anchor_elapsed_ms
            .checked_add(delta)
            .ok_or_else(|| failure(ErrorCode::ClockUnavailable, "clock.elapsed"))
    }

    /// Recheck current cancellation, deadline, run status, and the store's
    /// authoritative lease. Counter limits are checked individually at reservation.
    pub async fn check_boundary(&self) -> Result<(), ContractError> {
        self.check_cancelled()?;
        let saved = self.store.load(&self.scope, &self.run_id).await?;
        self.validate_running(&saved.snapshot)?;
        let (_, now_ms) = self.progress(saved.snapshot.usage.elapsed_ms)?;
        let current_lease = self
            .store
            .check_lease(&self.scope, &self.run_id, &self.lease, now_ms)
            .await?;
        // The store operation may itself have waited while cancellation or time changed.
        let (_, after_check_ms) = self.progress(saved.snapshot.usage.elapsed_ms)?;
        if after_check_ms >= current_lease.expires_at_ms {
            return Err(failure(ErrorCode::LeaseLost, "lease"));
        }
        Ok(())
    }

    /// Save one new attempt and increment its counter atomically under the lease
    /// and expected revision. A CAS failure reserves nothing and is not retried here.
    /// The returned record is not a dispatch permit: use execute, or recheck the
    /// boundary immediately before a separately managed external call.
    pub async fn reserve(
        &self,
        kind: ReservationKind,
    ) -> Result<AttemptReservation, ContractError> {
        self.check_cancelled()?;
        let saved = self.store.load(&self.scope, &self.run_id).await?;
        self.validate_running(&saved.snapshot)?;
        let mut snapshot = saved.snapshot;
        self.progress(snapshot.usage.elapsed_ms)?;
        charge(&mut snapshot.usage, &snapshot.limits, &kind)?;
        let attempt_id = self.ids.next_id()?;
        let (elapsed_ms, now_ms) = self.progress(snapshot.usage.elapsed_ms)?;
        let reservation = AttemptReservation {
            attempt_id,
            kind,
            reserved_at_ms: now_ms,
        };
        let expected_revision = snapshot.revision;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| failure(ErrorCode::RevisionConflict, "revision"))?;
        snapshot.usage.elapsed_ms = elapsed_ms;
        snapshot.timing.last_observed_at_ms = now_ms;
        snapshot.reservations.push(reservation.clone());
        self.store
            .commit(
                &self.scope,
                &self.run_id,
                CommitInput {
                    expected_revision,
                    lease: self.lease.clone(),
                    now_ms,
                    snapshot,
                    messages: Vec::new(),
                    events: Vec::new(),
                    records: Vec::new(),
                },
            )
            .await?;
        Ok(reservation)
    }

    /// Reserve, then recheck cancellation/time/lease before constructing the call.
    /// Cancellation or timeout while awaiting a dispatched call leaves its charged
    /// reservation intact; the external effect may require reconciliation. This
    /// method performs no policy, schema, or result-settlement validation.
    /// The closure represents one already-prepared operation. Further external
    /// calls or retries require their own reservation and boundary check.
    pub async fn execute<T, F, Fut>(
        &self,
        kind: ReservationKind,
        operation: F,
    ) -> Result<T, ContractError>
    where
        F: FnOnce(AttemptReservation) -> Fut,
        Fut: Future<Output = Result<T, ContractError>>,
    {
        let reservation = self.reserve(kind).await?;
        self.check_boundary().await?;
        let operation = operation(reservation);
        tokio::pin!(operation);
        tokio::select! {
            biased;
            stopped = self.wait_for_cancellation_or_deadline() => {
                stopped?;
                Err(failure(ErrorCode::DeadlineExceeded, "budget.elapsed"))
            }
            result = &mut operation => {
                self.progress(0)?;
                result
            }
        }
    }

    /// Observe cancellation or the original deadline without consuming call budget.
    /// Dropping this Future only stops observation; it never changes cancellation.
    pub async fn wait_for_cancellation_or_deadline(&self) -> Result<(), ContractError> {
        loop {
            self.progress(0)?;
            let duration = self
                .timing
                .deadline_at_ms
                .checked_sub(self.timing.started_at_ms)
                .and_then(|duration| u64::try_from(duration).ok())
                .ok_or_else(|| failure(ErrorCode::ClockUnavailable, "timing.deadline"))?;
            let remaining = duration
                .checked_sub(self.anchor_elapsed_ms)
                .ok_or_else(|| failure(ErrorCode::DeadlineExceeded, "budget.elapsed"))?;
            let deadline = self
                .anchor_monotonic_ms
                .checked_add(remaining)
                .ok_or_else(|| failure(ErrorCode::ClockUnavailable, "clock.deadline"))?;
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => return Err(failure(ErrorCode::Cancelled, "budget")),
                result = self.clock.sleep_until(deadline) => { result?; }
            }
        }
    }

    pub(crate) fn scope(&self) -> &Scope {
        &self.scope
    }
    pub(crate) fn run_id(&self) -> &Id {
        &self.run_id
    }
    pub(crate) fn lease(&self) -> &RunLease {
        &self.lease
    }
    pub(crate) fn store(&self) -> &Arc<dyn StateStore> {
        &self.store
    }
    pub(crate) fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub(crate) fn call_deadline(&self) -> Result<tokio::time::Instant, ContractError> {
        let (_, now) = self.progress(0)?;
        let remaining = self
            .timing
            .deadline_at_ms
            .checked_sub(now)
            .and_then(|ms| u64::try_from(ms).ok())
            .ok_or_else(|| failure(ErrorCode::DeadlineExceeded, "budget.elapsed"))?;
        tokio::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(remaining))
            .ok_or_else(|| failure(ErrorCode::ClockUnavailable, "clock.deadline"))
    }

    // State settlement is allowed after cancellation/deadline if the lease still
    // permits a commit. The caller keeps the saved elapsed high-water mark.
    pub(crate) fn settlement_time(&self, saved_elapsed: u64) -> Result<(u64, i64), ContractError> {
        let elapsed = self.elapsed_ms()?.max(saved_elapsed);
        let now = i64::try_from(elapsed)
            .ok()
            .and_then(|ms| self.timing.started_at_ms.checked_add(ms))
            .ok_or_else(|| failure(ErrorCode::ClockUnavailable, "clock.elapsed"))?;
        Ok((elapsed, now))
    }

    pub(crate) async fn backoff(&self, delay_ms: u64) -> Result<(), ContractError> {
        self.check_boundary().await?;
        let deadline = self
            .clock
            .now()?
            .monotonic_ms
            .checked_add(delay_ms)
            .ok_or_else(|| failure(ErrorCode::ClockUnavailable, "clock.backoff"))?;
        tokio::select! {
            biased;
            stopped = self.wait_for_cancellation_or_deadline() => stopped,
            result = self.clock.sleep_until(deadline) => result,
        }
    }

    fn check_cancelled(&self) -> Result<(), ContractError> {
        if self.cancellation.is_cancelled() {
            Err(failure(ErrorCode::Cancelled, "budget"))
        } else {
            Ok(())
        }
    }

    fn validate_running(&self, snapshot: &RunSnapshot) -> Result<(), ContractError> {
        if snapshot.status != RunStatus::Running {
            return Err(failure(ErrorCode::InvalidTransition, "run.status"));
        }
        if snapshot.timing.started_at_ms != self.timing.started_at_ms
            || snapshot.timing.deadline_at_ms != self.timing.deadline_at_ms
        {
            return Err(failure(ErrorCode::InvalidSnapshot, "timing"));
        }
        Ok(())
    }

    fn progress(&self, saved_elapsed_ms: u64) -> Result<(u64, i64), ContractError> {
        self.check_cancelled()?;
        let elapsed_ms = self.elapsed_ms()?.max(saved_elapsed_ms);
        let elapsed = i64::try_from(elapsed_ms)
            .map_err(|_| failure(ErrorCode::ClockUnavailable, "clock.elapsed"))?;
        let now_ms = self
            .timing
            .started_at_ms
            .checked_add(elapsed)
            .ok_or_else(|| failure(ErrorCode::ClockUnavailable, "clock.elapsed"))?;
        if now_ms >= self.timing.deadline_at_ms {
            return Err(failure(ErrorCode::DeadlineExceeded, "budget.elapsed"));
        }
        Ok((elapsed_ms, now_ms))
    }
}

fn failure(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}

fn charge(
    usage: &mut BudgetUsage,
    limits: &RunLimits,
    kind: &ReservationKind,
) -> Result<(), ContractError> {
    let (counter, limit, path) = match kind {
        ReservationKind::Model { .. } => (
            &mut usage.model_calls,
            limits.max_model_calls.get(),
            "budget.model_calls",
        ),
        ReservationKind::Tool { .. } => (
            &mut usage.tool_attempts,
            limits.max_tool_attempts,
            "budget.tool_attempts",
        ),
        ReservationKind::Repair {} => (
            &mut usage.repair_attempts,
            limits.max_repair_attempts,
            "budget.repair_attempts",
        ),
        ReservationKind::Recovery {} => (
            &mut usage.recovery_attempts,
            limits.max_recovery_attempts,
            "budget.recovery_attempts",
        ),
    };
    if *counter >= limit {
        return Err(failure(ErrorCode::BudgetExceeded, path));
    }
    *counter += 1;
    Ok(())
}

pub(crate) fn validate_budget(snapshot: &RunSnapshot) -> Result<(), ContractError> {
    let invalid = || failure(ErrorCode::InvalidSnapshot, "budget");
    let expected = RunTiming::new(
        snapshot.timing.started_at_ms,
        snapshot.limits.max_elapsed_ms.get(),
    )
    .map_err(|_| invalid())?;
    let elapsed = i64::try_from(snapshot.usage.elapsed_ms).map_err(|_| invalid())?;
    if snapshot.timing.deadline_at_ms != expected.deadline_at_ms
        || snapshot.timing.last_observed_at_ms
            != snapshot
                .timing
                .started_at_ms
                .checked_add(elapsed)
                .ok_or_else(invalid)?
        || snapshot.usage.model_calls > snapshot.limits.max_model_calls.get()
        || snapshot.usage.tool_attempts > snapshot.limits.max_tool_attempts
        || snapshot.usage.repair_attempts > snapshot.limits.max_repair_attempts
        || snapshot.usage.recovery_attempts > snapshot.limits.max_recovery_attempts
    {
        return Err(invalid());
    }
    let mut attempts = BTreeSet::new();
    let mut counts = BudgetUsage::default();
    for reservation in &snapshot.reservations {
        if !attempts.insert(&reservation.attempt_id)
            || reservation.reserved_at_ms < snapshot.timing.started_at_ms
            || reservation.reserved_at_ms >= snapshot.timing.deadline_at_ms
            || reservation.reserved_at_ms > snapshot.timing.last_observed_at_ms
        {
            return Err(invalid());
        }
        charge(&mut counts, &snapshot.limits, &reservation.kind).map_err(|_| invalid())?;
    }
    if counts.model_calls != snapshot.usage.model_calls
        || counts.tool_attempts != snapshot.usage.tool_attempts
        || counts.repair_attempts != snapshot.usage.repair_attempts
        || counts.recovery_attempts != snapshot.usage.recovery_attempts
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn validate_budget_transition(
    previous: &RunSnapshot,
    next: &RunSnapshot,
) -> Result<(), ContractError> {
    let invalid = || failure(ErrorCode::InvalidTransition, "budget");
    if previous.timing.started_at_ms != next.timing.started_at_ms
        || previous.timing.deadline_at_ms != next.timing.deadline_at_ms
        || !next.reservations.starts_with(&previous.reservations)
    {
        return Err(invalid());
    }
    let mut expected = previous.usage.clone();
    for added in &next.reservations[previous.reservations.len()..] {
        charge(&mut expected, &previous.limits, &added.kind).map_err(|_| invalid())?;
    }
    if expected.model_calls != next.usage.model_calls
        || expected.tool_attempts != next.usage.tool_attempts
        || expected.repair_attempts != next.usage.repair_attempts
        || expected.recovery_attempts != next.usage.recovery_attempts
    {
        return Err(invalid());
    }
    Ok(())
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

## `crates/wickle/src/model.rs`

```rust
use crate::{
    Id, JsonDigest, JsonObject, RecordRef, Scope, VersionedRef,
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
    /// Protected complete or failed response, including bounded partial text.
    /// This is retained even when a later recovery reservation is exhausted.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub response_ref: Option<RecordRef>,
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

## `crates/wickle/src/model_execution.rs`

```rust
use std::{panic::AssertUnwindSafe, sync::Arc};

use futures_util::FutureExt;

use crate::{
    CommitInput, ContractError, ErrorCode, ExecutionContext, Guarded, Id, ModelAttemptState,
    ModelCallContext, ModelFailureKind, ModelInvocationRecord, ModelPort, ModelProtocolError,
    ModelRequest, ModelResponse, ModelResponseMetadata, PolicyAction, PolicyGate, PolicyRequest,
    ProtectedRecord, ReservationKind, RunBudget, RunEvent, RunEventPayload, RunEventSchemaVersion,
    RunPhase, collect_model_response,
};

/// Explicit, bounded retries within the same already selected route. Router
/// fallback and context reduction are separate operations owned by the driver.
#[derive(Debug, Clone, Copy, Default)]
pub struct ModelRetryPolicy {
    /// Additional physical requests after the first; zero disables retries.
    pub max_retries: u32,
    /// Backoff on the run's injected clock, bounded by its original deadline.
    pub backoff_ms: u64,
}

/// Complete response or a classified failure with bounded partial text. Failed
/// attempts never publish a partially assembled tool plan through this value.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelExchangeOutcome {
    /// The full stream ended with a valid completion.
    Completed {
        /// Unexecuted proposals; the driver must persist and validate tool plans.
        response: ModelResponse,
    },
    /// No complete response was accepted after the configured recovery allowance.
    Failed {
        /// Safe classification, optional reported usage, and bounded partial text.
        failure: ModelProtocolError,
    },
}

/// Protected response body tied to one physical attempt and exact route. Reading
/// it requires Store authorization; it is never an automatic public run view.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredModelResponse {
    /// The physical model request/attempt identifier.
    pub request_id: Id,
    /// Route that produced the success or failure.
    pub route_digest: crate::JsonDigest,
    /// Accepted response or bounded failed-response data.
    pub outcome: ModelExchangeOutcome,
}

/// Core-owned model execution boundary. Each physical attempt is visible in the
/// budget and invocation ledger; an adapter still performs exactly one request.
/// This does not run tools, choose fallback routes, or advance an agent loop.
pub struct ModelExchange {
    model: Arc<dyn ModelPort>,
    policy: Arc<PolicyGate>,
    retry: ModelRetryPolicy,
}

impl ModelExchange {
    /// Bind one adapter and current policy gate, with physical retries disabled.
    pub fn new(model: Arc<dyn ModelPort>, policy: Arc<PolicyGate>) -> Self {
        Self {
            model,
            policy,
            retry: ModelRetryPolicy::default(),
        }
    }

    /// Configure finite same-route recovery. Run model/recovery limits still apply.
    pub fn with_retry_policy(mut self, retry: ModelRetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Validate and authorize each attempt, persist its reservation and selected
    /// route, then collect exactly one adapter stream. A retry uses a new physical
    /// request/attempt ID while retaining the caller's logical request ID as the
    /// model step. The driver separately stores the accepted response/transcript
    /// and tool plans before dispatching any proposed tool. Complete and failed
    /// responses are retained under each invocation's protected response_ref, so
    /// a later budget/cancellation error does not discard prior partial output.
    pub async fn generate(
        &self,
        request: &ModelRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<Guarded<ModelExchangeOutcome>, ContractError> {
        for retry_number in 0..=self.retry.max_retries {
            self.validate(request, context, budget)?;
            budget.check_boundary().await?;
            if let Guarded::ApprovalRequired(challenge) =
                self.authorize(request, context, budget).await?
            {
                return Ok(Guarded::ApprovalRequired(challenge));
            }
            let reservation = budget
                .reserve(ReservationKind::Model {
                    purpose: request.purpose,
                })
                .await?;
            let mut physical_request = request.clone();
            physical_request.request_id = reservation.attempt_id.clone();
            let invocation = ModelInvocationRecord {
                run_id: budget.run_id().clone(),
                model_step_id: request.request_id.clone(),
                attempt_id: reservation.attempt_id.clone(),
                purpose: request.purpose,
                route: request.route.clone(),
                selection_reason: Id::new(if retry_number == 0 {
                    "requested_route"
                } else {
                    "same_route_retry"
                })?,
                request_digest: physical_request.digest(),
                state: ModelAttemptState::Reserved {},
                response_ref: None,
                provider_request_id: None,
                reported_model_id: None,
                reported_model_version: None,
                usage: None,
            };
            self.record_start(budget, invocation).await?;
            // Neither a saved reservation nor earlier authorization grants lasting
            // permission. Check the physical request and current policy again.
            self.validate(&physical_request, context, budget)?;
            if let Guarded::ApprovalRequired(challenge) =
                self.authorize(&physical_request, context, budget).await?
            {
                return Ok(Guarded::ApprovalRequired(challenge));
            }
            budget.check_boundary().await?;
            if context.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let cancellation = budget.cancellation().child_token();
            let _cancel_adapter_on_drop = cancellation.clone().drop_guard();
            let call_context = ModelCallContext {
                attempt_id: reservation.attempt_id.clone(),
                run_id: budget.run_id().clone(),
                scope: context.data.scope.clone(),
                cancellation,
                deadline: budget.call_deadline()?,
            };
            // Construct the stream only after all entry checks. The adapter's
            // lifetime ends with this attempt; it must not spawn untracked retries.
            let attempt = AssertUnwindSafe(async {
                collect_model_response(
                    &physical_request,
                    self.model.generate(&physical_request, &call_context),
                )
                .await
            })
            .catch_unwind();
            let result = tokio::select! {
                biased;
                _ = context.cancellation.cancelled() => Err(cancelled()),
                stopped = budget.wait_for_cancellation_or_deadline() => {
                    match stopped {
                        Err(error) => Err(error),
                        Ok(()) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "model.deadline")),
                    }
                }
                response = attempt => response.map_err(|_| ContractError::new(ErrorCode::InvalidContract, "model.adapter")),
            };
            // Signal adapter shutdown before any potentially slow settlement I/O.
            call_context.cancellation.cancel();
            let response = match result {
                Ok(response) => response,
                Err(error) => {
                    self.record_end(
                        budget,
                        &reservation.attempt_id,
                        ModelAttemptState::Unknown {},
                        &ModelResponseMetadata::default(),
                        None,
                    )
                    .await?;
                    return Err(error);
                }
            };
            let (state, metadata) = match &response {
                Ok(response) => (ModelAttemptState::Completed {}, &response.metadata),
                Err(failure) => (
                    ModelAttemptState::Failed { kind: failure.kind },
                    failure.metadata.as_ref(),
                ),
            };
            let stored_response = StoredModelResponse {
                request_id: reservation.attempt_id.clone(),
                route_digest: physical_request.route.digest(),
                outcome: match &response {
                    Ok(response) => ModelExchangeOutcome::Completed {
                        response: response.clone(),
                    },
                    Err(failure) => ModelExchangeOutcome::Failed {
                        failure: failure.clone(),
                    },
                },
            };
            let response_record = ProtectedRecord::new(
                Id::new(format!("model-response-{}", reservation.attempt_id))?,
                1,
                serde_json::to_value(&stored_response).map_err(|_| revision_error())?,
            );
            self.record_end(
                budget,
                &reservation.attempt_id,
                state,
                metadata,
                Some(response_record),
            )
            .await?;
            if context.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            budget.check_boundary().await?;
            match response {
                Ok(response) => {
                    return Ok(Guarded::Completed(ModelExchangeOutcome::Completed {
                        response,
                    }));
                }
                Err(failure) => {
                    if retry_number == self.retry.max_retries || !recoverable(failure.kind) {
                        return Ok(Guarded::Completed(ModelExchangeOutcome::Failed { failure }));
                    }
                    budget.reserve(ReservationKind::Recovery {}).await?;
                    tokio::select! {
                        biased;
                        _ = context.cancellation.cancelled() => return Err(cancelled()),
                        result = budget.backoff(self.retry.backoff_ms) => result?,
                    }
                }
            }
        }
        unreachable!("a finite attempt loop always returns its last result")
    }

    fn validate(
        &self,
        request: &ModelRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        if &context.data.scope != budget.scope() {
            return Err(ContractError::new(ErrorCode::AccessDenied, "scope"));
        }
        request.validate()?;
        if !self.model.binding().matches_route(&request.route) {
            return Err(ContractError::new(
                ErrorCode::InvalidReference,
                "model.binding",
            ));
        }
        Ok(())
    }

    async fn authorize(
        &self,
        request: &ModelRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<Guarded<()>, ContractError> {
        let policy_request = PolicyRequest {
            owner_scope: budget.scope().clone(),
            resource_id: budget.run_id().clone(),
            action: PolicyAction::InvokeModel {
                route_digest: request.route.digest(),
                purpose: request.purpose,
            },
        };
        tokio::select! {
            biased;
            stopped = budget.wait_for_cancellation_or_deadline() => match stopped {
                Err(error) => Err(error),
                Ok(()) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "model.policy")),
            },
            decision = self.policy.guard(&policy_request, context, Some(budget.call_deadline()?), None, || async { Ok(()) }) => decision,
        }
    }

    async fn record_start(
        &self,
        budget: &RunBudget,
        invocation: ModelInvocationRecord,
    ) -> Result<(), ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(revision_error)?;
        snapshot.last_event_seq = snapshot
            .last_event_seq
            .checked_add(1)
            .ok_or_else(revision_error)?;
        snapshot.phase = RunPhase::Model;
        snapshot.model_step_id = Some(invocation.model_step_id.clone());
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        let record = ProtectedRecord::new(
            Id::new(format!("model-invocation-{}", invocation.attempt_id))?,
            1,
            serde_json::to_value(&invocation).map_err(|_| revision_error())?,
        );
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: Id::new(format!("model-route-{}", invocation.attempt_id))?,
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            session_id: snapshot.request.session_id.clone(),
            seq: snapshot
                .last_event_seq
                .try_into()
                .map_err(|_| revision_error())?,
            timestamp_ms: now,
            payload: RunEventPayload::ModelRouteSelected {
                invocation_ref: record.reference().clone(),
                route_digest: invocation.route.digest(),
            },
        };
        snapshot.model_ledger.push(invocation);
        budget
            .store()
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages: vec![],
                    events: vec![event],
                    records: vec![record],
                },
            )
            .await?;
        Ok(())
    }

    async fn record_end(
        &self,
        budget: &RunBudget,
        attempt_id: &Id,
        state: ModelAttemptState,
        metadata: &ModelResponseMetadata,
        response_record: Option<ProtectedRecord>,
    ) -> Result<(), ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let invocation = snapshot
            .model_ledger
            .iter_mut()
            .find(|entry| &entry.attempt_id == attempt_id)
            .ok_or_else(|| ContractError::new(ErrorCode::InvalidSnapshot, "model.attempt"))?;
        invocation.state = state;
        invocation.response_ref = response_record
            .as_ref()
            .map(|record| record.reference().clone());
        invocation.provider_request_id = metadata.provider_request_id.clone();
        invocation.reported_model_id = metadata.reported_model_id.clone();
        invocation.reported_model_version = metadata.reported_model_version.clone();
        invocation.usage = metadata.usage.clone();
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(revision_error)?;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        budget
            .store()
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages: vec![],
                    events: vec![],
                    records: response_record.into_iter().collect(),
                },
            )
            .await?;
        Ok(())
    }
}

fn cancelled() -> ContractError {
    ContractError::new(ErrorCode::Cancelled, "model")
}
fn revision_error() -> ContractError {
    ContractError::new(ErrorCode::RevisionConflict, "model.ledger")
}
fn recoverable(kind: ModelFailureKind) -> bool {
    matches!(
        kind,
        ModelFailureKind::Timeout
            | ModelFailureKind::RateLimited
            | ModelFailureKind::Transport
            | ModelFailureKind::Protocol
    )
}
```

## `crates/wickle/src/model_protocol.rs`

```rust
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, io,
    num::NonZeroU64,
};

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::{
    ContractError, ErrorCode, Id, JsonDigest, JsonObject, ModelFailureKind, ModelPurpose,
    ModelUsage, PortStream, ResolvedModelRoute, Scope, VersionedRef, parse_json,
    serialization::data_digest,
};

/// Adapter-owned provider, implementation, and credential-binding identities.
/// Actual credentials remain in the adapter instance, never in a model request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelPortBinding {
    /// Registered service key, distinct for direct and hosted provider paths.
    pub provider: Id,
    /// Exact adapter implementation identity.
    pub adapter: VersionedRef,
    /// Host-owned credential/connection binding revision.
    pub connection_ref: VersionedRef,
}

impl ModelPortBinding {
    /// Require all adapter identities to match the immutable selected route.
    pub fn matches_route(&self, route: &ResolvedModelRoute) -> bool {
        self.provider == route.provider
            && self.adapter == route.adapter
            && self.connection_ref == route.connection_ref
    }
}

/// One physical model request's runtime context, without credentials or system inputs.
#[derive(Debug, Clone)]
pub struct ModelCallContext {
    /// Budget reservation and physical invocation identity.
    pub attempt_id: Id,
    /// Owning execution.
    pub run_id: Id,
    /// Authenticated data/execution scope supplied by the Host.
    pub scope: Scope,
    /// Cooperative cancellation signal.
    pub cancellation: CancellationToken,
    /// Effective deadline for this physical invocation.
    pub deadline: tokio::time::Instant,
}

/// A single physical provider invocation, without an internal agent loop or retry.
/// Adapters disable hidden SDK retries and provider-native tool execution. They
/// enforce wire/body limits while decoding, report safe typed errors, and end the
/// stream after the one request. Credentials are obtained through their binding.
pub trait ModelPort: Send + Sync {
    /// Identities of the adapter and connection actually used by this instance.
    fn binding(&self) -> ModelPortBinding;
    /// Generate one response. Partial argument deltas are never execution commands.
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent>;
}

/// Roles in an already-authorized model projection, separate from stored messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelRole {
    /// Trusted instructions selected by the core's projection layer.
    System,
    /// User data selected for this invocation.
    User,
    /// Prior model content and proposed calls.
    Assistant,
    /// Observations paired with prior proposed calls.
    Tool,
}

/// Provider continuation data pinned to an exact route, including its versions.
/// Explicit serialization is for protected storage/provider replay only.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpaqueContinuation {
    route_digest: JsonDigest,
    data: Value,
}

impl OpaqueContinuation {
    /// Bind provider-replay data to the route that produced it.
    pub fn new(route: &ResolvedModelRoute, data: Value) -> Self {
        Self {
            route_digest: route.digest(),
            data,
        }
    }
    /// Exact route identity required before replaying these bytes.
    pub fn route_digest(&self) -> &JsonDigest {
        &self.route_digest
    }
    /// Explicit privileged access for the matching provider adapter.
    pub fn data(&self) -> &Value {
        &self.data
    }
}

impl fmt::Debug for OpaqueContinuation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpaqueContinuation")
            .field("route_digest", &self.route_digest)
            .field("data", &"<redacted>")
            .finish()
    }
}

/// Provider-facing content selected explicitly by the core projection layer.
/// No variant contains ExecutionContext, complete system inputs, or storage records.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelContent {
    /// Authorized text, including instructions or observations as indicated by role.
    Text {
        /// Text sent to the selected model.
        text: String,
    },
    /// Explicitly selected JSON data; it conveys no authority by itself.
    Json {
        /// Model-visible JSON.
        value: Value,
    },
    /// A prior model-proposed call, containing only model-owned arguments.
    ToolCall {
        /// Provider/projection call identity, paired with its observation.
        provider_call_id: Id,
        /// Normalized model-facing tool name.
        name: Id,
        /// Model-owned arguments, never merged system execution arguments.
        arguments: JsonObject,
    },
    /// A limited observation without receipts or hidden input maps.
    ToolResult {
        /// Matching call in the preceding assistant tool round.
        provider_call_id: Id,
        /// Explicitly selected model-visible observation.
        content: Value,
    },
    /// Provider-replay data permitted only on its original exact route.
    Opaque {
        /// Protected continuation selected for provider replay.
        continuation: OpaqueContinuation,
    },
}

impl fmt::Debug for ModelContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Text { .. } => "text",
            Self::Json { .. } => "json",
            Self::ToolCall { .. } => "tool_call",
            Self::ToolResult { .. } => "tool_result",
            Self::Opaque { .. } => "opaque",
        };
        f.debug_struct("ModelContent")
            .field("type", &kind)
            .finish_non_exhaustive()
    }
}

/// A projected message, not an original transcript record or client-submitted role.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelMessage {
    /// Role selected after validating provenance and allowed instruction placement.
    pub role: ModelRole,
    /// Model-visible content only.
    pub content: Vec<ModelContent>,
}

/// Only the compiled model-facing portion of a registered tool definition.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelTool {
    /// Portable normalized ASCII name: letters, digits, underscore or hyphen, 1..64 bytes.
    pub name: Id,
    /// Model-facing description; hidden input descriptions are excluded by the compiler.
    pub description: String,
    /// Derived input schema, without system-owned fields or their definitions/examples.
    pub model_input_schema: Value,
}

impl fmt::Debug for ModelTool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelTool")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// Provider output-mode request. Final candidate verification belongs to the driver.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelOutput {
    /// Ordinary text output.
    Text {},
    /// Structured output with an already-resolved, model-visible schema.
    JsonSchema {
        /// Resolved output schema, without storage lookup references.
        schema: Value,
    },
}

impl fmt::Debug for ModelOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Text {} => "ModelOutput::Text",
            Self::JsonSchema { .. } => "ModelOutput::JsonSchema(<redacted>)",
        })
    }
}

/// Finite decoding bounds, independent of token usage and total run budgets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelResponseLimits {
    /// Maximum serialized logical request size, including projection and schemas.
    pub max_input_bytes: usize,
    /// Cumulative UTF-8 response payload bytes, including call metadata and opaque data.
    pub max_response_bytes: usize,
    /// Maximum bytes in one text or tool-argument fragment.
    pub max_delta_bytes: usize,
    /// Maximum stream events, including empty fragments and terminal events.
    pub max_events: usize,
    /// Maximum distinct proposed calls; zero prohibits tool-call output.
    pub max_tool_calls: usize,
}

/// Immutable-for-invocation logical request containing only a prepared projection.
/// Explicit serialization is for protected storage/transport codecs, not public logs.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRequest {
    /// Request identity, scoped to this physical provider invocation.
    pub request_id: Id,
    /// Agent, verification, or compaction accounting purpose.
    pub purpose: ModelPurpose,
    /// Exact selected provider/model/API/connection revisions.
    pub route: ResolvedModelRoute,
    /// Authorized projection; never the raw transcript or ExecutionContext.
    pub messages: Vec<ModelMessage>,
    /// Derived model-facing tool definitions only.
    pub tools: Vec<ModelTool>,
    /// Requested provider output format, distinct from final outcome verification.
    pub output: ModelOutput,
    /// Finite provider output-token request.
    pub max_output_tokens: NonZeroU64,
    /// Input and response decoding limits selected for this route.
    pub limits: ModelResponseLimits,
}

impl fmt::Debug for ModelRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelRequest")
            .field("request_id", &self.request_id)
            .field("purpose", &self.purpose)
            .field("route_digest", &self.route.digest())
            .field("message_count", &self.messages.len())
            .field("tool_count", &self.tools.len())
            .finish_non_exhaustive()
    }
}

impl ModelRequest {
    /// Hash the complete prepared request, without introducing runtime credentials.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }

    /// Check finite bounds, projected protocol, schemas and continuation route identity.
    /// Host policy and actual adapter-binding checks are separate execution boundaries.
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.limits.max_input_bytes == 0
            || self.limits.max_response_bytes == 0
            || self.limits.max_delta_bytes == 0
            || self.limits.max_events == 0
        {
            return Err(invalid_request("model_request.limits"));
        }
        json_size(self, self.limits.max_input_bytes)
            .map_err(|_| invalid_request("model_request.input_size"))?;
        let mut names = BTreeSet::new();
        for tool in &self.tools {
            if !valid_name(tool.name.as_str())
                || !names.insert(&tool.name)
                || tool.model_input_schema.get("type").and_then(Value::as_str) != Some("object")
            {
                return Err(invalid_request("model_request.tools"));
            }
            compile_schema(&tool.model_input_schema)?;
        }
        if let ModelOutput::JsonSchema { schema } = &self.output {
            compile_schema(schema)?;
        }
        let mut pending_calls = BTreeSet::new();
        for message in &self.messages {
            if !pending_calls.is_empty() && message.role != ModelRole::Tool {
                return Err(invalid_request("model_request.tool_results"));
            }
            let mut round_calls = BTreeSet::new();
            for content in &message.content {
                match content {
                    ModelContent::ToolCall {
                        provider_call_id,
                        name,
                        ..
                    } => {
                        if message.role != ModelRole::Assistant
                            || !valid_call_id(provider_call_id.as_str())
                            || !valid_name(name.as_str())
                            || !round_calls.insert(provider_call_id.clone())
                        {
                            return Err(invalid_request("model_request.tool_calls"));
                        }
                    }
                    ModelContent::ToolResult {
                        provider_call_id, ..
                    } => {
                        if message.role != ModelRole::Tool
                            || !pending_calls.remove(provider_call_id)
                        {
                            return Err(invalid_request("model_request.tool_results"));
                        }
                    }
                    ModelContent::Opaque { continuation } => {
                        if message.role != ModelRole::Assistant
                            || continuation.route_digest() != &self.route.digest()
                        {
                            return Err(invalid_request("model_request.continuation"));
                        }
                    }
                    ModelContent::Text { .. } | ModelContent::Json { .. } => {
                        if message.role == ModelRole::Tool {
                            return Err(invalid_request("model_request.tool_results"));
                        }
                    }
                }
            }
            pending_calls.extend(round_calls);
        }
        if !pending_calls.is_empty() {
            return Err(invalid_request("model_request.tool_results"));
        }
        Ok(())
    }
}

/// Provider finish classification, normalized independently of provider strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelFinish {
    /// A complete answer without proposed tool calls.
    Stop,
    /// A complete response proposing one or more calls.
    ToolCalls,
    /// Truncated output. Assembly returns an error and no executable call plan.
    Length,
    /// Explicit refusal without a tool plan; not a successful business outcome.
    Refusal,
}

/// Facts actually reported by the provider; omitted identifiers and usage stay unknown.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelResponseMetadata {
    /// Provider correlation identity, if reported.
    pub provider_request_id: Option<Id>,
    /// Actual response model, not copied from the requested route as a guess.
    pub reported_model_id: Option<Id>,
    /// Actual reported release/version, if supplied by the provider.
    pub reported_model_version: Option<Id>,
    /// Reported or explicitly estimated token usage, never implicit zeroes.
    pub usage: Option<ModelUsage>,
}

impl fmt::Debug for ModelResponseMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelResponseMetadata")
            .field("usage", &self.usage)
            .finish_non_exhaustive()
    }
}

/// Untrusted provider response fragments. None is a core ToolCall or dispatch permit.
#[derive(Clone, PartialEq)]
pub enum ModelEvent {
    /// Candidate text fragment.
    TextDelta {
        /// UTF-8 text, subject to per-fragment and aggregate bounds.
        text: String,
    },
    /// Fragment of one proposed call. Identity/name may arrive in an earlier fragment.
    ToolArgumentsDelta {
        /// Stable provider output index, allowing interleaved call fragments.
        index: u32,
        /// Complete provider identity when available; conflicting replacements fail.
        provider_call_id: Option<String>,
        /// Complete normalized name when available; conflicting replacements fail.
        name: Option<String>,
        /// JSON argument fragment, never executed before full response validation.
        delta: String,
    },
    /// Complete logical response; the adapter must subsequently end this request stream.
    ResponseCompleted {
        /// Normalized finish classification.
        finish: ModelFinish,
        /// Actually reported response metadata.
        metadata: ModelResponseMetadata,
        /// Protected continuation data bound to this request's route.
        continuation: Vec<OpaqueContinuation>,
    },
    /// Safe classified provider failure, without raw SDK error strings.
    ResponseError {
        /// Classification used by the core's bounded recovery policy.
        kind: ModelFailureKind,
        /// Facts actually reported before failure.
        metadata: ModelResponseMetadata,
    },
}

impl fmt::Debug for ModelEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::TextDelta { .. } => "text_delta",
            Self::ToolArgumentsDelta { .. } => "tool_arguments_delta",
            Self::ResponseCompleted { .. } => "response_completed",
            Self::ResponseError { .. } => "response_error",
        };
        f.debug_struct("ModelEvent")
            .field("type", &kind)
            .finish_non_exhaustive()
    }
}

/// Protocol validation only; even Valid proposals require the later tool boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallValidation {
    /// Known model-facing tool and conforming model-owned arguments.
    Valid,
    /// Not among the tools advertised for this request; execution is prohibited.
    UnknownTool,
    /// Model-owned arguments fail the advertised schema; execution is prohibited.
    InvalidArguments,
}

/// A complete provider proposal, awaiting core call-ID allocation and protected planning.
/// It cannot be passed as a core ToolCall without explicit later materialization.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposedToolCall {
    /// Provider-local identity, scoped by the ModelResponse request_id.
    pub provider_call_id: Id,
    /// Normalized model-facing name.
    pub name: Id,
    /// Parsed model-owned JSON object; no system parameters have been injected.
    pub model_inputs: JsonObject,
    /// Preliminary schema/availability result, not policy or tool authorization.
    pub validation: ToolCallValidation,
}

impl fmt::Debug for ProposedToolCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProposedToolCall")
            .field("name", &self.name)
            .field("validation", &self.validation)
            .finish_non_exhaustive()
    }
}

/// A complete protocol response, distinct from a verified agent outcome or persisted plan.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelResponse {
    /// Request that scopes provider call identifiers.
    pub request_id: Id,
    /// Exact route that produced this response and continuation data.
    pub route_digest: JsonDigest,
    /// Complete candidate text; final output verification remains separate.
    pub text: String,
    /// Complete proposals, including structured rejection reasons for invalid tools.
    pub tool_calls: Vec<ProposedToolCall>,
    /// Complete stop/tool/refusal classification.
    pub finish: ModelFinish,
    /// Provider-reported facts.
    pub metadata: ModelResponseMetadata,
    /// Protected provider-replay data.
    pub continuation: Vec<OpaqueContinuation>,
}

impl fmt::Debug for ModelResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelResponse")
            .field("request_id", &self.request_id)
            .field("finish", &self.finish)
            .field("tool_count", &self.tool_calls.len())
            .finish_non_exhaustive()
    }
}

/// Safe, stable assembly/recovery reasons without submitted fragments or SDK messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelProtocolErrorCode {
    /// Invalid projection, schema, or request bounds.
    InvalidRequest,
    /// The stream failed before a complete response was received.
    StreamFailure,
    /// EOF arrived without a terminal response.
    MissingCompletion,
    /// Another terminal or any event followed completion.
    UnexpectedEvent,
    /// Arguments were malformed, ambiguous, or not a JSON object.
    InvalidArguments,
    /// A provider call identity was missing, malformed, or replaced.
    InvalidCallId,
    /// A model-facing name was missing, malformed, or replaced.
    InvalidToolName,
    /// Different proposed calls reused the same provider-local identity.
    DuplicateCallId,
    /// The normalized finish classification conflicts with the actual proposed calls.
    FinishMismatch,
    /// A response event, byte, fragment or tool count exceeded its finite bound.
    ResponseLimitExceeded,
    /// Continuation data belongs to a different route or version.
    RouteMismatch,
    /// The adapter reported a typed provider failure.
    ProviderFailure,
    /// Output was truncated, even if individual argument fragments looked complete.
    OutputTruncated,
}

/// A failed response may expose bounded candidate text explicitly, never partial calls.
/// Serialization is for protected failure records, never automatic public logging.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelProtocolError {
    /// Recovery classification; the core decides whether another paid attempt is allowed.
    pub kind: ModelFailureKind,
    /// Safe protocol/provider reason.
    pub code: ModelProtocolErrorCode,
    /// Metadata actually observed; missing values remain unknown.
    pub metadata: Box<ModelResponseMetadata>,
    partial_text: String,
}

impl ModelProtocolError {
    /// Construct a sanitized failure, without embedding an arbitrary SDK error.
    pub fn new(
        kind: ModelFailureKind,
        code: ModelProtocolErrorCode,
        metadata: ModelResponseMetadata,
    ) -> Self {
        Self {
            kind,
            code,
            metadata: Box::new(metadata),
            partial_text: String::new(),
        }
    }
    /// Explicit access to bounded, unverified text; it is not a completed answer.
    pub fn partial_text(&self) -> &str {
        &self.partial_text
    }
}

impl fmt::Debug for ModelProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelProtocolError")
            .field("kind", &self.kind)
            .field("code", &self.code)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for ModelProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {:?}", self.kind, self.code)
    }
}
impl std::error::Error for ModelProtocolError {}

#[derive(Default)]
struct PartialCall {
    provider_call_id: Option<String>,
    name: Option<String>,
    arguments: String,
}

struct ResponseAssembly {
    text: String,
    calls: BTreeMap<u32, PartialCall>,
    bytes: usize,
    events: usize,
    terminal: Option<(ModelFinish, ModelResponseMetadata, Vec<OpaqueContinuation>)>,
}

impl ResponseAssembly {
    fn error(&self, code: ModelProtocolErrorCode) -> ModelProtocolError {
        let mut error = ModelProtocolError::new(
            ModelFailureKind::Protocol,
            code,
            self.terminal
                .as_ref()
                .map_or_else(ModelResponseMetadata::default, |(_, metadata, _)| {
                    metadata.clone()
                }),
        );
        error.partial_text = self.text.clone();
        error
    }
    fn add_bytes(&mut self, count: usize, maximum: usize) -> Result<(), ModelProtocolError> {
        let total = self
            .bytes
            .checked_add(count)
            .filter(|n| *n <= maximum)
            .ok_or_else(|| self.error(ModelProtocolErrorCode::ResponseLimitExceeded))?;
        self.bytes = total;
        Ok(())
    }
}

/// Collect one logical response through EOF. No partial call escapes on failure.
/// The caller must bound time/cancellation around collection: a stream that never
/// yields cannot be stopped by event/byte limits alone. No tools are executed here.
pub async fn collect_model_response(
    request: &ModelRequest,
    mut stream: PortStream<'_, ModelEvent>,
) -> Result<ModelResponse, ModelProtocolError> {
    request.validate().map_err(|_| {
        ModelProtocolError::new(
            ModelFailureKind::Protocol,
            ModelProtocolErrorCode::InvalidRequest,
            ModelResponseMetadata::default(),
        )
    })?;
    let mut assembly = ResponseAssembly {
        text: String::new(),
        calls: BTreeMap::new(),
        bytes: 0,
        events: 0,
        terminal: None,
    };
    while let Some(next) = stream.next().await {
        if assembly.terminal.is_some() {
            return Err(assembly.error(ModelProtocolErrorCode::UnexpectedEvent));
        }
        assembly.events = assembly
            .events
            .checked_add(1)
            .filter(|count| *count <= request.limits.max_events)
            .ok_or_else(|| assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded))?;
        let event = next.map_err(|_| {
            let mut error = assembly.error(ModelProtocolErrorCode::StreamFailure);
            error.kind = ModelFailureKind::Transport;
            error
        })?;
        match event {
            ModelEvent::TextDelta { text } => {
                if text.len() > request.limits.max_delta_bytes {
                    return Err(assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded));
                }
                assembly.add_bytes(text.len(), request.limits.max_response_bytes)?;
                assembly.text.push_str(&text);
            }
            ModelEvent::ToolArgumentsDelta {
                index,
                provider_call_id,
                name,
                delta,
            } => {
                if delta.len() > request.limits.max_delta_bytes
                    || (!assembly.calls.contains_key(&index)
                        && assembly.calls.len() >= request.limits.max_tool_calls)
                {
                    return Err(assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded));
                }
                if provider_call_id
                    .as_ref()
                    .is_some_and(|id| !valid_call_id(id))
                {
                    return Err(assembly.error(ModelProtocolErrorCode::InvalidCallId));
                }
                if name.as_ref().is_some_and(|name| !valid_name(name)) {
                    return Err(assembly.error(ModelProtocolErrorCode::InvalidToolName));
                }
                let bytes = delta
                    .len()
                    .saturating_add(provider_call_id.as_ref().map_or(0, String::len))
                    .saturating_add(name.as_ref().map_or(0, String::len));
                assembly.add_bytes(bytes, request.limits.max_response_bytes)?;
                if let Some(existing) = assembly.calls.get(&index) {
                    if existing
                        .provider_call_id
                        .as_ref()
                        .zip(provider_call_id.as_ref())
                        .is_some_and(|(old, new)| old != new)
                    {
                        return Err(assembly.error(ModelProtocolErrorCode::InvalidCallId));
                    }
                    if existing
                        .name
                        .as_ref()
                        .zip(name.as_ref())
                        .is_some_and(|(old, new)| old != new)
                    {
                        return Err(assembly.error(ModelProtocolErrorCode::InvalidToolName));
                    }
                }
                let call = assembly.calls.entry(index).or_default();
                if let Some(id) = provider_call_id {
                    call.provider_call_id = Some(id);
                }
                if let Some(name) = name {
                    call.name = Some(name);
                }
                call.arguments.push_str(&delta);
            }
            ModelEvent::ResponseCompleted {
                finish,
                metadata,
                continuation,
            } => {
                let mut bytes = metadata_bytes(&metadata)
                    .ok_or_else(|| assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded))?;
                for item in &continuation {
                    if item.route_digest() != &request.route.digest() {
                        return Err(assembly.error(ModelProtocolErrorCode::RouteMismatch));
                    }
                    let size = json_size(item.data(), request.limits.max_response_bytes).map_err(
                        |_| assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded),
                    )?;
                    bytes = bytes.checked_add(size).ok_or_else(|| {
                        assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded)
                    })?;
                }
                assembly.add_bytes(bytes, request.limits.max_response_bytes)?;
                assembly.terminal = Some((finish, metadata, continuation));
            }
            ModelEvent::ResponseError { kind, metadata } => {
                let bytes = metadata_bytes(&metadata)
                    .ok_or_else(|| assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded))?;
                assembly.add_bytes(bytes, request.limits.max_response_bytes)?;
                let mut error = ModelProtocolError::new(
                    kind,
                    ModelProtocolErrorCode::ProviderFailure,
                    metadata,
                );
                error.partial_text = assembly.text;
                return Err(error);
            }
        }
    }
    let (finish, metadata, continuation) = assembly
        .terminal
        .as_ref()
        .ok_or_else(|| assembly.error(ModelProtocolErrorCode::MissingCompletion))?;
    if *finish == ModelFinish::Length {
        return Err(assembly.error(ModelProtocolErrorCode::OutputTruncated));
    }
    if (*finish == ModelFinish::ToolCalls) != !assembly.calls.is_empty() {
        return Err(assembly.error(ModelProtocolErrorCode::FinishMismatch));
    }
    let mut tool_calls = Vec::new();
    let mut seen_ids = BTreeSet::new();
    for call in assembly.calls.values() {
        let provider_call_id = call
            .provider_call_id
            .as_ref()
            .ok_or_else(|| assembly.error(ModelProtocolErrorCode::InvalidCallId))?;
        if !seen_ids.insert(provider_call_id) {
            return Err(assembly.error(ModelProtocolErrorCode::DuplicateCallId));
        }
        let name = call
            .name
            .as_ref()
            .ok_or_else(|| assembly.error(ModelProtocolErrorCode::InvalidToolName))?;
        let value = parse_json(&call.arguments)
            .map_err(|_| assembly.error(ModelProtocolErrorCode::InvalidArguments))?;
        let object = value
            .as_object()
            .ok_or_else(|| assembly.error(ModelProtocolErrorCode::InvalidArguments))?;
        let validation = match request.tools.iter().find(|tool| tool.name.as_str() == name) {
            None => ToolCallValidation::UnknownTool,
            Some(tool) => {
                let validator = compile_schema(&tool.model_input_schema)
                    .map_err(|_| assembly.error(ModelProtocolErrorCode::InvalidRequest))?;
                if validator.is_valid(&value) {
                    ToolCallValidation::Valid
                } else {
                    ToolCallValidation::InvalidArguments
                }
            }
        };
        tool_calls.push(ProposedToolCall {
            provider_call_id: Id::new(provider_call_id.clone())
                .map_err(|_| assembly.error(ModelProtocolErrorCode::InvalidCallId))?,
            name: Id::new(name.clone())
                .map_err(|_| assembly.error(ModelProtocolErrorCode::InvalidToolName))?,
            model_inputs: object
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            validation,
        });
    }
    Ok(ModelResponse {
        request_id: request.request_id.clone(),
        route_digest: request.route.digest(),
        text: assembly.text,
        tool_calls,
        finish: *finish,
        metadata: metadata.clone(),
        continuation: continuation.clone(),
    })
}

fn invalid_request(path: &str) -> ContractError {
    ContractError::new(ErrorCode::InvalidContract, path)
}

fn valid_call_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value.chars().any(|c| c.is_whitespace() || c.is_control())
}
fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
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
fn compile_schema(schema: &Value) -> Result<jsonschema::Validator, ContractError> {
    jsonschema::draft202012::options()
        .should_validate_formats(true)
        .with_retriever(NoSchemaRetrieval)
        .build(schema)
        .map_err(|_| invalid_request("model_request.schema"))
}

struct ByteCounter {
    count: usize,
    maximum: usize,
}
impl io::Write for ByteCounter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.count = self
            .count
            .checked_add(buffer.len())
            .filter(|count| *count <= self.maximum)
            .ok_or_else(|| io::Error::other("serialized value exceeds its limit"))?;
        Ok(buffer.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn json_size(value: &impl Serialize, maximum: usize) -> Result<usize, ()> {
    let mut counter = ByteCounter { count: 0, maximum };
    serde_json::to_writer(&mut counter, value).map_err(|_| ())?;
    Ok(counter.count)
}
fn metadata_bytes(metadata: &ModelResponseMetadata) -> Option<usize> {
    [
        &metadata.provider_request_id,
        &metadata.reported_model_id,
        &metadata.reported_model_version,
    ]
    .into_iter()
    .flatten()
    .try_fold(0_usize, |size, id| size.checked_add(id.as_str().len()))
}
```

## `crates/wickle/src/run.rs`

```rust
use crate::{
    ArtifactRef, AttemptReservation, CompletionPolicy, ContractError, ErrorCode, Failure, Id,
    InputContent, JsonDigest, ModelAttemptState, ModelInvocationRecord, RecordRef, ReservationKind,
    ResolvedProfile, RunLimits, RunTiming, Scope, ToolCall, ToolResult, VersionedRef,
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
    /// Original admission/deadline and persisted monotonic elapsed-time anchor.
    pub timing: RunTiming,
    /// Append-only charged attempt reservations, preserved across errors and resume.
    pub reservations: Vec<AttemptReservation>,
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

## `crates/wickle/src/state.rs`

```rust
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroU64,
    sync::{Mutex, MutexGuard},
};

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{
    ApprovalTarget, BudgetUsage, ContentBlock, ContractError, ErrorCode, Id, Message,
    ModelAttemptState, ModelExchangeOutcome, ModelFinish, ModelInvocationRecord, OutcomeResult,
    PortFuture, RecordRef, ResumeAction, ResumeCommand, RunEvent, RunEventPayload, RunPhase,
    RunSnapshot, RunStatus, Scope, SessionSchemaVersion, SessionSnapshot, StoredModelResponse,
    ToolCall, ToolCallState, ToolResult, VerificationSummary, WaitState, WaitTarget,
    admission_digest, canonical_digest,
};

/// Guarantees offered by a state-store implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateStoreCapabilities {
    /// Records survive process termination.
    pub durable: bool,
    /// Execution leases coordinate independent processes.
    pub cross_process_leases: bool,
    /// Committed events can be replayed in sequence order.
    pub event_replay: bool,
}

/// Immutable, scope-owned data stored with its referencing state and events.
/// Access requires Host authorization; Debug never prints the payload.
#[derive(Clone, PartialEq)]
pub struct ProtectedRecord {
    reference: RecordRef,
    value: Value,
}

impl ProtectedRecord {
    /// Compute the reference digest from owned data. A revision is immutable.
    pub fn new(record_id: Id, revision: u64, value: Value) -> Self {
        Self {
            reference: RecordRef {
                record_id,
                revision,
                digest: canonical_digest(&value),
            },
            value,
        }
    }

    /// Exact immutable record identity, without its payload.
    pub fn reference(&self) -> &RecordRef {
        &self.reference
    }

    /// Explicit privileged access, never an automatic public/model projection.
    pub fn value(&self) -> &Value {
        &self.value
    }
}

impl fmt::Debug for ProtectedRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtectedRecord")
            .field("reference", &self.reference)
            .field("value", &"<redacted>")
            .finish()
    }
}

/// Initial records accepted atomically for a newly admitted run.
#[derive(Clone)]
pub struct AdmissionInput {
    /// Running/admission checkpoint at revision zero.
    pub snapshot: RunSnapshot,
    /// Session-pinned prompt record; reused unchanged by subsequent runs.
    pub prompt_snapshot: RecordRef,
    /// New messages, numbered consecutively across the session.
    pub messages: Vec<Message>,
    /// One run.started event at sequence one, referencing the accepted request.
    pub events: Vec<RunEvent>,
    /// New immutable records, available to references in this transaction.
    pub records: Vec<ProtectedRecord>,
    /// Reject implementations that cannot preserve state across process termination.
    pub require_durable: bool,
}

/// An owned protected checkpoint and its complete session transcript.
/// Use PolicyGate views to select data for less privileged callers.
#[derive(Clone, PartialEq)]
pub struct StoredRun {
    /// Current run checkpoint and protected record references.
    pub snapshot: RunSnapshot,
    /// Current session metadata, including its active run.
    pub session: SessionSnapshot,
    /// Append-only session transcript, including messages from earlier runs.
    pub messages: Vec<Message>,
}

impl fmt::Debug for StoredRun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredRun")
            .field("run_id", &self.snapshot.run_id)
            .field("revision", &self.snapshot.revision)
            .field("message_count", &self.messages.len())
            .finish_non_exhaustive()
    }
}

/// Admission reports whether it created a run or found the original request.
#[derive(Debug, Clone, PartialEq)]
pub struct AdmissionResult {
    /// False for identical request replay; candidate records are not applied.
    pub created: bool,
    /// Existing or newly admitted run, with its original pinned data.
    pub state: StoredRun,
}

/// Store-issued lease identity. Possession is not Host authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLease {
    /// Exact resource namespace.
    pub scope: Scope,
    /// Run owned by this lease.
    pub run_id: Id,
    /// Worker identity supplied by trusted runtime code.
    pub owner: Id,
    /// Increasing generation retained across expiration and release.
    pub fencing_token: u64,
    /// Expiration reported when issued. Validation uses the store's current expiry,
    /// so renewal does not invalidate copies of the same owner/fencing generation.
    pub expires_at_ms: i64,
}

/// A complete candidate checkpoint and append-only data for one atomic commit.
#[derive(Clone)]
pub struct CommitInput {
    /// Compare-and-swap revision of the currently saved checkpoint.
    pub expected_revision: u64,
    /// Current unexpired execution lease.
    pub lease: RunLease,
    /// Trusted current UTC milliseconds, also used to reject expired leases.
    pub now_ms: i64,
    /// Next checkpoint, at expected_revision + 1.
    pub snapshot: RunSnapshot,
    /// New messages, continuing the session sequence.
    pub messages: Vec<Message>,
    /// New events, continuing the run sequence.
    pub events: Vec<RunEvent>,
    /// Immutable records to insert in the same transaction.
    pub records: Vec<ProtectedRecord>,
}

/// A bounded, ordered page of protected durable events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventPage {
    /// Events strictly after the supplied cursor.
    pub events: Vec<RunEvent>,
    /// Cursor for the next page, unchanged for an empty page.
    pub next_after_seq: u64,
    /// More events were available when this page was read.
    pub has_more: bool,
    /// Oldest retained sequence; None when no events are stored.
    pub first_available_seq: Option<NonZeroU64>,
    /// Latest committed sequence when this page was read.
    pub last_available_seq: u64,
}

/// Largest event page accepted by the reference store.
pub const MAX_EVENT_PAGE_SIZE: usize = 1_000;

/// Trusted core storage port. Scope isolation is enforced by the store itself.
/// The facade separately applies current PolicyGate authorization. No raw load or
/// record reference grants permission to publish the returned data.
pub trait StateStore: Send + Sync {
    /// Describe storage and coordination guarantees.
    fn capabilities(&self) -> StateStoreCapabilities;
    /// Atomically deduplicate a request and reserve its session's active-run slot.
    fn admit<'a>(
        &'a self,
        scope: &'a Scope,
        input: AdmissionInput,
    ) -> PortFuture<'a, AdmissionResult>;
    /// Load owned state and the complete session transcript.
    fn load<'a>(&'a self, scope: &'a Scope, run_id: &'a Id) -> PortFuture<'a, StoredRun>;
    /// Read session-pinned metadata without changing its active run.
    fn load_session<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
    ) -> PortFuture<'a, SessionSnapshot>;
    /// Validate owner/generation against the current stored expiry without renewing.
    /// Return the latest lease metadata, including any concurrent heartbeat renewal.
    fn check_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, RunLease>;
    /// Acquire a new generation after any previous lease has expired or been released.
    fn acquire_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        owner: &'a Id,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease>;
    /// Renew an unexpired generation; an expired lease cannot be revived.
    fn renew_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease>;
    /// Release only the currently owned unexpired generation.
    fn release_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, ()>;
    /// Validate and commit state, transcript, records and events atomically.
    fn commit<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun>;
    /// Replay a bounded page. Retention gaps must not silently skip missing events.
    fn read_events<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        after_seq: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage>;
    /// Read an exact scope-owned immutable record after separate Host authorization.
    fn read_record<'a>(
        &'a self,
        scope: &'a Scope,
        reference: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord>;
}

type ScopeKey = (Id, Id, Option<Id>);
type RecordKey = (Id, u64);

#[derive(Default)]
struct ScopeState {
    sessions: BTreeMap<Id, SessionState>,
    runs: BTreeMap<Id, RunState>,
    requests: BTreeMap<(Id, Id), Id>,
    records: BTreeMap<RecordKey, ProtectedRecord>,
    event_ids: BTreeSet<Id>,
    message_ids: BTreeSet<Id>,
}

struct SessionState {
    snapshot: SessionSnapshot,
    messages: Vec<Message>,
}

struct RunState {
    snapshot: RunSnapshot,
    events: Vec<RunEvent>,
    lease: Option<RunLease>,
    last_fencing_token: u64,
}

/// Process-local reference store. It retains all committed data for its lifetime.
/// A single short critical section validates and applies each transaction; no
/// external calls or awaits occur while the lock is held. It provides neither
/// process-restart durability nor coordination between separate processes.
#[derive(Default)]
pub struct MemoryStateStore {
    scopes: Mutex<BTreeMap<ScopeKey, ScopeState>>,
}

impl MemoryStateStore {
    /// Construct an empty store without creating a runtime or doing I/O.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<MutexGuard<'_, BTreeMap<ScopeKey, ScopeState>>, ContractError> {
        self.scopes
            .lock()
            .map_err(|_| error(ErrorCode::PersistenceUnavailable, "state_store"))
    }
}

impl StateStore for MemoryStateStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        StateStoreCapabilities {
            durable: false,
            cross_process_leases: false,
            event_replay: true,
        }
    }

    fn admit<'a>(
        &'a self,
        scope: &'a Scope,
        input: AdmissionInput,
    ) -> PortFuture<'a, AdmissionResult> {
        Box::pin(async move {
            if input.require_durable {
                return Err(error(ErrorCode::CapabilityUnsupported, "store.durable"));
            }
            check_scope(scope, &input.snapshot.scope)?;
            if scope != input.snapshot.profile.scope()
                || input.snapshot.request_digest
                    != admission_digest(
                        &input.snapshot.request,
                        &input.snapshot.profile,
                        input.snapshot.system_inputs.as_ref(),
                    )
            {
                return Err(error(ErrorCode::InvalidSnapshot, "request_digest"));
            }
            let mut scopes = self.lock()?;
            let empty = ScopeState::default();
            let state = scopes.get(&scope_key(scope)).unwrap_or(&empty);
            let request_key = (
                input.snapshot.request.session_id.clone(),
                input.snapshot.request.request_id.clone(),
            );
            if let Some(run_id) = state.requests.get(&request_key) {
                let previous = stored_run(state, run_id)?;
                if previous.snapshot.request_digest != input.snapshot.request_digest {
                    return Err(error(ErrorCode::RequestConflict, "request"));
                }
                return Ok(AdmissionResult {
                    created: false,
                    state: previous,
                });
            }
            input.snapshot.validate()?;
            if input.snapshot.revision != 0
                || input.snapshot.status != RunStatus::Running
                || input.snapshot.phase != RunPhase::Admission
                || !input.snapshot.model_ledger.is_empty()
                || !input.snapshot.tool_ledger.is_empty()
                || !input.snapshot.reservations.is_empty()
                || input.snapshot.usage != BudgetUsage::default()
            {
                return Err(error(ErrorCode::InvalidSnapshot, "admission"));
            }
            if state.runs.contains_key(&input.snapshot.run_id) {
                return Err(error(ErrorCode::RunConflict, "run_id"));
            }
            let session_id = &input.snapshot.request.session_id;
            let previous_session = state.sessions.get(session_id);
            if let Some(session) = previous_session {
                if session.snapshot.profile_digest != *input.snapshot.profile.profile_digest()
                    || session.snapshot.prompt_snapshot != input.prompt_snapshot
                {
                    return Err(error(ErrorCode::ProfileMismatch, "session.profile"));
                }
                if session.snapshot.active_run_id.is_some() {
                    return Err(error(ErrorCode::SessionBusy, "session"));
                }
            }
            let additions = validate_records(state, &input.records)?;
            record_value(state, &additions, &input.prompt_snapshot)?;
            validate_snapshot_refs(state, &additions, &input.snapshot)?;
            validate_events(state, &additions, &input.snapshot, 0, &input.events, true)?;
            let previous_sequence = previous_session.map_or(0, |s| s.snapshot.transcript_revision);
            let transcript_revision = validate_messages(
                state,
                &additions,
                &input.snapshot.run_id,
                previous_sequence,
                &input.messages,
            )?;
            let mut messages = previous_session.map_or_else(Vec::new, |s| s.messages.clone());
            messages.extend(input.messages);
            let session = SessionSnapshot {
                schema_version: SessionSchemaVersion::V1,
                session_id: session_id.clone(),
                scope: scope.clone(),
                profile_digest: input.snapshot.profile.profile_digest().clone(),
                prompt_snapshot: input.prompt_snapshot,
                transcript_revision,
                active_run_id: Some(input.snapshot.run_id.clone()),
            };
            let result = StoredRun {
                snapshot: input.snapshot.clone(),
                session: session.clone(),
                messages: messages.clone(),
            };
            // All fallible checks precede these mutations.
            let state = scopes.entry(scope_key(scope)).or_default();
            state.records.extend(additions);
            state
                .message_ids
                .extend(messages.iter().map(|m| m.message_id.clone()));
            state
                .event_ids
                .extend(input.events.iter().map(|e| e.event_id.clone()));
            state
                .requests
                .insert(request_key, input.snapshot.run_id.clone());
            state.sessions.insert(
                session.session_id.clone(),
                SessionState {
                    snapshot: session,
                    messages,
                },
            );
            state.runs.insert(
                input.snapshot.run_id.clone(),
                RunState {
                    snapshot: input.snapshot,
                    events: input.events,
                    lease: None,
                    last_fencing_token: 0,
                },
            );
            Ok(AdmissionResult {
                created: true,
                state: result,
            })
        })
    }

    fn load<'a>(&'a self, scope: &'a Scope, run_id: &'a Id) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            let scopes = self.lock()?;
            stored_run(namespace(&scopes, scope)?, run_id)
        })
    }

    fn load_session<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
    ) -> PortFuture<'a, SessionSnapshot> {
        Box::pin(async move {
            let scopes = self.lock()?;
            namespace(&scopes, scope)?
                .sessions
                .get(session_id)
                .map(|session| session.snapshot.clone())
                .ok_or_else(not_found)
        })
    }

    fn check_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, RunLease> {
        Box::pin(async move {
            let scopes = self.lock()?;
            let run = namespace(&scopes, scope)?
                .runs
                .get(run_id)
                .ok_or_else(not_found)?;
            validate_lease(run, scope, run_id, lease, now_ms)?;
            Ok(run.lease.as_ref().expect("validated lease").clone())
        })
    }

    fn acquire_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        owner: &'a Id,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease> {
        Box::pin(async move {
            let expires_at_ms = lease_expiry(now_ms, ttl_ms)?;
            let mut scopes = self.lock()?;
            let run = run_mut(&mut scopes, scope, run_id)?;
            if run.snapshot.status.is_terminal() {
                return Err(error(ErrorCode::InvalidTransition, "run.status"));
            }
            if run.lease.as_ref().is_some_and(|l| l.expires_at_ms > now_ms) {
                return Err(error(ErrorCode::LeaseBusy, "lease"));
            }
            let fencing_token = run
                .last_fencing_token
                .checked_add(1)
                .ok_or_else(|| error(ErrorCode::InvalidContract, "lease.fencing_token"))?;
            let lease = RunLease {
                scope: scope.clone(),
                run_id: run_id.clone(),
                owner: owner.clone(),
                fencing_token,
                expires_at_ms,
            };
            run.last_fencing_token = fencing_token;
            run.lease = Some(lease.clone());
            Ok(lease)
        })
    }

    fn renew_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease> {
        Box::pin(async move {
            let expires_at_ms = lease_expiry(now_ms, ttl_ms)?;
            let mut scopes = self.lock()?;
            let run = run_mut(&mut scopes, scope, run_id)?;
            validate_lease(run, scope, run_id, lease, now_ms)?;
            let renewed = RunLease {
                expires_at_ms,
                ..lease.clone()
            };
            run.lease = Some(renewed.clone());
            Ok(renewed)
        })
    }

    fn release_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let mut scopes = self.lock()?;
            let run = run_mut(&mut scopes, scope, run_id)?;
            validate_lease(run, scope, run_id, lease, now_ms)?;
            run.lease = None;
            Ok(())
        })
    }

    fn commit<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            check_scope(scope, &input.snapshot.scope)?;
            let mut scopes = self.lock()?;
            let state = namespace(&scopes, scope)?;
            let run = state.runs.get(run_id).ok_or_else(not_found)?;
            validate_lease(run, scope, run_id, &input.lease, input.now_ms)?;
            if run.snapshot.revision != input.expected_revision {
                return Err(error(ErrorCode::RevisionConflict, "revision"));
            }
            validate_transition(&run.snapshot, &input.snapshot)?;
            let additions = validate_records(state, &input.records)?;
            validate_snapshot_refs(state, &additions, &input.snapshot)?;
            validate_events(
                state,
                &additions,
                &input.snapshot,
                run.snapshot.last_event_seq,
                &input.events,
                false,
            )?;
            let session = state
                .sessions
                .get(&run.snapshot.request.session_id)
                .ok_or_else(not_found)?;
            if session.snapshot.active_run_id.as_ref() != Some(run_id) {
                return Err(error(ErrorCode::InvalidTransition, "session.active_run_id"));
            }
            let transcript_revision = validate_messages(
                state,
                &additions,
                run_id,
                session.snapshot.transcript_revision,
                &input.messages,
            )?;
            let mut session_snapshot = session.snapshot.clone();
            session_snapshot.transcript_revision = transcript_revision;
            if input.snapshot.status.is_terminal() {
                session_snapshot.active_run_id = None;
            }
            let mut messages = session.messages.clone();
            messages.extend(input.messages);
            let result = StoredRun {
                snapshot: input.snapshot.clone(),
                session: session_snapshot.clone(),
                messages: messages.clone(),
            };
            let state = scopes
                .get_mut(&scope_key(scope))
                .expect("validated namespace");
            state.records.extend(additions);
            state
                .message_ids
                .extend(messages.iter().map(|m| m.message_id.clone()));
            state
                .event_ids
                .extend(input.events.iter().map(|e| e.event_id.clone()));
            state.sessions.insert(
                session_snapshot.session_id.clone(),
                SessionState {
                    snapshot: session_snapshot,
                    messages,
                },
            );
            let run = state.runs.get_mut(run_id).expect("validated run");
            run.snapshot = input.snapshot;
            run.events.extend(input.events);
            if run.snapshot.status.is_terminal() {
                run.lease = None;
            }
            Ok(result)
        })
    }

    fn read_events<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        after_seq: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage> {
        Box::pin(async move {
            if limit == 0 || limit > MAX_EVENT_PAGE_SIZE {
                return Err(error(ErrorCode::InvalidContract, "events.limit"));
            }
            let scopes = self.lock()?;
            let run = namespace(&scopes, scope)?
                .runs
                .get(run_id)
                .ok_or_else(not_found)?;
            let mut available = run.events.iter().filter(|e| e.seq.get() > after_seq);
            let events: Vec<_> = available.by_ref().take(limit).cloned().collect();
            Ok(EventPage {
                next_after_seq: events.last().map_or(after_seq, |e| e.seq.get()),
                has_more: available.next().is_some(),
                first_available_seq: run.events.first().map(|e| e.seq),
                last_available_seq: run.snapshot.last_event_seq,
                events,
            })
        })
    }

    fn read_record<'a>(
        &'a self,
        scope: &'a Scope,
        reference: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        Box::pin(async move {
            let scopes = self.lock()?;
            let record = namespace(&scopes, scope)?
                .records
                .get(&record_key(reference))
                .ok_or_else(not_found)?;
            if record.reference != *reference {
                return Err(error(ErrorCode::RecordConflict, "record.reference"));
            }
            Ok(record.clone())
        })
    }
}

fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}

fn not_found() -> ContractError {
    error(ErrorCode::StateNotFound, "state")
}

fn scope_key(scope: &Scope) -> ScopeKey {
    (
        scope.tenant_id.clone(),
        scope.workspace_id.clone(),
        scope.user_id.clone(),
    )
}

fn record_key(reference: &RecordRef) -> RecordKey {
    (reference.record_id.clone(), reference.revision)
}

fn check_scope(expected: &Scope, actual: &Scope) -> Result<(), ContractError> {
    if expected != actual {
        Err(error(ErrorCode::AccessDenied, "scope"))
    } else {
        Ok(())
    }
}

fn namespace<'a>(
    scopes: &'a BTreeMap<ScopeKey, ScopeState>,
    scope: &Scope,
) -> Result<&'a ScopeState, ContractError> {
    scopes.get(&scope_key(scope)).ok_or_else(not_found)
}

fn run_mut<'a>(
    scopes: &'a mut BTreeMap<ScopeKey, ScopeState>,
    scope: &Scope,
    run_id: &Id,
) -> Result<&'a mut RunState, ContractError> {
    scopes
        .get_mut(&scope_key(scope))
        .and_then(|state| state.runs.get_mut(run_id))
        .ok_or_else(not_found)
}

fn stored_run(state: &ScopeState, run_id: &Id) -> Result<StoredRun, ContractError> {
    let run = state.runs.get(run_id).ok_or_else(not_found)?;
    let session = state
        .sessions
        .get(&run.snapshot.request.session_id)
        .ok_or_else(not_found)?;
    Ok(StoredRun {
        snapshot: run.snapshot.clone(),
        session: session.snapshot.clone(),
        messages: session.messages.clone(),
    })
}

fn lease_expiry(now_ms: i64, ttl_ms: u64) -> Result<i64, ContractError> {
    let ttl = i64::try_from(ttl_ms)
        .ok()
        .filter(|ttl| *ttl > 0)
        .ok_or_else(|| error(ErrorCode::InvalidContract, "lease.ttl_ms"))?;
    now_ms
        .checked_add(ttl)
        .ok_or_else(|| error(ErrorCode::InvalidContract, "lease.expires_at_ms"))
}

fn validate_lease(
    run: &RunState,
    scope: &Scope,
    run_id: &Id,
    provided: &RunLease,
    now_ms: i64,
) -> Result<(), ContractError> {
    if &provided.scope != scope
        || &provided.run_id != run_id
        || !run.lease.as_ref().is_some_and(|stored| {
            stored.owner == provided.owner
                && stored.fencing_token == provided.fencing_token
                && now_ms < stored.expires_at_ms
        })
    {
        return Err(error(ErrorCode::LeaseLost, "lease"));
    }
    Ok(())
}

fn validate_records(
    state: &ScopeState,
    records: &[ProtectedRecord],
) -> Result<BTreeMap<RecordKey, ProtectedRecord>, ContractError> {
    let mut additions = BTreeMap::new();
    for record in records {
        let key = record_key(&record.reference);
        if state
            .records
            .get(&key)
            .or_else(|| additions.get(&key))
            .is_some_and(|existing| existing != record)
        {
            return Err(error(ErrorCode::RecordConflict, "records"));
        }
        additions.insert(key, record.clone());
    }
    Ok(additions)
}

fn record_value<'a>(
    state: &'a ScopeState,
    additions: &'a BTreeMap<RecordKey, ProtectedRecord>,
    reference: &RecordRef,
) -> Result<&'a Value, ContractError> {
    let record = additions
        .get(&record_key(reference))
        .or_else(|| state.records.get(&record_key(reference)))
        .ok_or_else(not_found)?;
    if &record.reference != reference {
        return Err(error(ErrorCode::RecordConflict, "record.reference"));
    }
    Ok(&record.value)
}

fn validate_snapshot_refs(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    let mut references = Vec::new();
    if let Some(inputs) = &snapshot.system_inputs {
        references.push(&inputs.snapshot_ref);
    }
    for invocation in &snapshot.model_ledger {
        if let Some(reference) = &invocation.response_ref {
            validate_model_response(state, additions, invocation, reference)?;
        }
    }
    references.extend(snapshot.assembly_ref.iter());
    references.extend(&snapshot.context_batches);
    references.extend(snapshot.source_states.iter().map(|s| &s.batch_ref));
    references.extend(
        snapshot
            .tool_ledger
            .iter()
            .filter_map(|entry| entry.call.bound_input_ref.as_ref()),
    );
    for entry in &snapshot.tool_ledger {
        if let ToolCallState::Settled { result } = &entry.state {
            references.extend(tool_result_refs(result));
        }
    }
    if let Some(wait) = &snapshot.wait {
        if let WaitTarget::Approval {
            target: ApprovalTarget::Candidate { candidate_ref, .. },
        } = &wait.target
        {
            references.push(candidate_ref);
        }
    }
    if let Some(outcome) = &snapshot.outcome {
        references.extend(&outcome.unresolved_effects);
        if let Some(verification) = &outcome.verification {
            references.extend(&verification.evidence);
        }
        if let OutcomeResult::Failed { failure } = &outcome.result {
            references.extend(failure.diagnostic_ref.iter());
        }
    }
    for reference in references {
        record_value(state, additions, reference)?;
    }
    Ok(())
}

fn validate_model_response(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    invocation: &ModelInvocationRecord,
    reference: &RecordRef,
) -> Result<(), ContractError> {
    let invalid = || error(ErrorCode::InvalidSnapshot, "model_ledger.response_ref");
    let saved: StoredModelResponse =
        serde_json::from_value(record_value(state, additions, reference)?.clone())
            .map_err(|_| invalid())?;
    let route_digest = invocation.route.digest();
    if saved.request_id != invocation.attempt_id || saved.route_digest != route_digest {
        return Err(invalid());
    }
    let metadata = match (&invocation.state, &saved.outcome) {
        (ModelAttemptState::Completed {}, ModelExchangeOutcome::Completed { response }) => {
            let mut call_ids = BTreeSet::new();
            if response.request_id != invocation.attempt_id
                || response.route_digest != route_digest
                || response
                    .continuation
                    .iter()
                    .any(|continuation| continuation.route_digest() != &route_digest)
                || response.finish == ModelFinish::Length
                || (response.finish == ModelFinish::ToolCalls) != !response.tool_calls.is_empty()
                || response
                    .tool_calls
                    .iter()
                    .any(|call| !call_ids.insert(&call.provider_call_id))
            {
                return Err(invalid());
            }
            &response.metadata
        }
        (ModelAttemptState::Failed { kind }, ModelExchangeOutcome::Failed { failure })
            if *kind == failure.kind =>
        {
            &failure.metadata
        }
        _ => return Err(invalid()),
    };
    if metadata.provider_request_id != invocation.provider_request_id
        || metadata.reported_model_id != invocation.reported_model_id
        || metadata.reported_model_version != invocation.reported_model_version
        || metadata.usage != invocation.usage
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_messages(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    run_id: &Id,
    last_sequence: u64,
    messages: &[Message],
) -> Result<u64, ContractError> {
    let mut sequence = last_sequence;
    let mut seen = BTreeSet::new();
    for message in messages {
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::InvalidMessage, "messages.sequence"))?;
        if &message.run_id != run_id
            || message.sequence.get() != sequence
            || state.message_ids.contains(&message.message_id)
            || !seen.insert(&message.message_id)
        {
            return Err(error(ErrorCode::InvalidMessage, "messages"));
        }
        for content in &message.content {
            let references = match content {
                ContentBlock::Content { .. } => Vec::new(),
                ContentBlock::ToolCall { call } => call.bound_input_ref.iter().collect(),
                ContentBlock::ToolResult { result } => tool_result_refs(result),
                ContentBlock::ProviderOpaque { data_ref, .. } => vec![data_ref],
            };
            for reference in references {
                record_value(state, additions, reference)?;
            }
        }
    }
    Ok(sequence)
}

fn tool_result_refs(result: &ToolResult) -> Vec<&RecordRef> {
    result
        .effect_receipt_ref
        .iter()
        .chain(
            result
                .error
                .iter()
                .flat_map(|error| error.diagnostic_ref.iter()),
        )
        .collect()
}

fn validate_events(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    previous_seq: u64,
    events: &[RunEvent],
    admission: bool,
) -> Result<(), ContractError> {
    let mut sequence = previous_seq;
    let mut seen = BTreeSet::new();
    let mut started = 0;
    let mut finished = 0;
    for event in events {
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::InvalidEvent, "events.seq"))?;
        if event.scope != snapshot.scope
            || event.run_id != snapshot.run_id
            || event.session_id != snapshot.request.session_id
            || event.seq.get() != sequence
            || state.event_ids.contains(&event.event_id)
            || !seen.insert(&event.event_id)
        {
            return Err(error(ErrorCode::InvalidEvent, "events"));
        }
        let reference = match &event.payload {
            RunEventPayload::RunStarted {
                request_ref,
                profile_digest,
            } => {
                started += 1;
                if !admission
                    || profile_digest != snapshot.profile.profile_digest()
                    || record_value(state, additions, request_ref)?
                        != &serde_json::to_value(&snapshot.request)
                            .map_err(|_| error(ErrorCode::InvalidContract, "request"))?
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_started"));
                }
                request_ref
            }
            RunEventPayload::RunFinished { outcome_ref } => {
                finished += 1;
                let outcome = snapshot
                    .outcome
                    .as_ref()
                    .filter(|_| snapshot.status.is_terminal())
                    .ok_or_else(|| error(ErrorCode::InvalidEvent, "events.run_finished"))?;
                if record_value(state, additions, outcome_ref)?
                    != &serde_json::to_value(outcome)
                        .map_err(|_| error(ErrorCode::InvalidContract, "outcome"))?
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_finished"));
                }
                outcome_ref
            }
            RunEventPayload::ToolPlanned { call_ref } => {
                let call: ToolCall = event_record(state, additions, call_ref)?;
                if !snapshot.tool_ledger.iter().any(|entry| entry.call == call) {
                    return Err(error(ErrorCode::InvalidEvent, "events.tool_planned"));
                }
                call_ref
            }
            RunEventPayload::ToolSettled { result_ref } => {
                let result: ToolResult = event_record(state, additions, result_ref)?;
                if !snapshot.tool_ledger.iter().any(|entry| {
                    matches!(
                        &entry.state, ToolCallState::Settled { result: saved } if *saved == result
                    )
                }) {
                    return Err(error(ErrorCode::InvalidEvent, "events.tool_settled"));
                }
                result_ref
            }
            RunEventPayload::VerificationCompleted { verification_ref } => {
                // Additional verification history needs an explicit checkpoint contract.
                // A standalone event cannot substitute for the saved verification record.
                let verification: VerificationSummary =
                    event_record(state, additions, verification_ref)?;
                if snapshot
                    .outcome
                    .as_ref()
                    .and_then(|outcome| outcome.verification.as_ref())
                    != Some(&verification)
                {
                    return Err(error(
                        ErrorCode::InvalidEvent,
                        "events.verification_completed",
                    ));
                }
                verification_ref
            }
            RunEventPayload::RunWaiting { wait_ref } => {
                let wait: WaitState = event_record(state, additions, wait_ref)?;
                if snapshot.status != RunStatus::Waiting || snapshot.wait.as_ref() != Some(&wait) {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_waiting"));
                }
                wait_ref
            }
            RunEventPayload::RunResumed { command_ref } => {
                let command: ResumeCommand = event_record(state, additions, command_ref)?;
                let previous = state.runs.get(&snapshot.run_id).ok_or_else(not_found)?;
                if snapshot.status != RunStatus::Running
                    || command.run_id != snapshot.run_id
                    || command.expected_revision != previous.snapshot.revision
                    || !resume_target_matches(&previous.snapshot, &command.action)
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_resumed"));
                }
                match &command.action {
                    ResumeAction::External { receipt_ref, .. } => {
                        record_value(state, additions, receipt_ref)?;
                    }
                    ResumeAction::Recover { recovery_ref } => {
                        record_value(state, additions, recovery_ref)?;
                    }
                    _ => {}
                }
                command_ref
            }
            RunEventPayload::ModelRouteSelected {
                invocation_ref,
                route_digest,
            } => {
                let invocation: ModelInvocationRecord =
                    event_record(state, additions, invocation_ref)?;
                if invocation.route.digest() != *route_digest
                    || !snapshot.model_ledger.contains(&invocation)
                {
                    return Err(error(
                        ErrorCode::InvalidEvent,
                        "events.model_route_selected",
                    ));
                }
                invocation_ref
            }
        };
        record_value(state, additions, reference)?;
    }
    if sequence != snapshot.last_event_seq
        || (admission && (started != 1 || events.len() != 1))
        || (snapshot.status.is_terminal() && finished != 1)
    {
        return Err(error(ErrorCode::InvalidEvent, "events"));
    }
    Ok(())
}

fn event_record<T: DeserializeOwned>(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    reference: &RecordRef,
) -> Result<T, ContractError> {
    serde_json::from_value(record_value(state, additions, reference)?.clone())
        .map_err(|_| error(ErrorCode::InvalidEvent, "events.record"))
}

fn resume_target_matches(previous: &RunSnapshot, action: &ResumeAction) -> bool {
    match action {
        ResumeAction::Recover { .. } => previous.status == RunStatus::Running,
        ResumeAction::Approve { wait_id, target }
        | ResumeAction::Deny {
            wait_id, target, ..
        } => previous.wait.as_ref().is_some_and(|wait| {
            &wait.wait_id == wait_id
                && matches!(&wait.target, WaitTarget::Approval { target: saved } if saved == target)
        }),
        ResumeAction::Input { wait_id, .. } => previous.wait.as_ref().is_some_and(|wait| {
            &wait.wait_id == wait_id && matches!(wait.target, WaitTarget::Input { .. })
        }),
        ResumeAction::External { wait_id, .. } => previous.wait.as_ref().is_some_and(|wait| {
            &wait.wait_id == wait_id && matches!(wait.target, WaitTarget::External { .. })
        }),
    }
}

fn validate_transition(previous: &RunSnapshot, next: &RunSnapshot) -> Result<(), ContractError> {
    if previous.status.is_terminal() {
        return Err(error(ErrorCode::InvalidTransition, "run.status"));
    }
    next.validate()?;
    crate::budget::validate_budget_transition(previous, next)?;
    if previous.run_id != next.run_id
        || previous.request != next.request
        || previous.request_digest != next.request_digest
        || previous.scope != next.scope
        || previous.system_inputs != next.system_inputs
        || previous.limits != next.limits
        || next.revision
            != previous
                .revision
                .checked_add(1)
                .ok_or_else(|| error(ErrorCode::RevisionConflict, "revision"))?
        || (previous.assembly_ref.is_some() && previous.assembly_ref != next.assembly_ref)
    {
        return Err(error(
            ErrorCode::InvalidTransition,
            "snapshot.immutable_fields",
        ));
    }
    if previous.profile != next.profile {
        return Err(error(ErrorCode::ProfileMismatch, "snapshot.profile"));
    }
    if previous.tool_ledger.len() > next.tool_ledger.len() {
        return Err(error(ErrorCode::InvalidTransition, "tool_ledger"));
    }
    for (old, new) in previous.tool_ledger.iter().zip(&next.tool_ledger) {
        let mut call = old.call.clone();
        if call.bound_input_ref.is_none() {
            call.bound_input_ref = new.call.bound_input_ref.clone();
        }
        if call != new.call
            || (matches!(old.state, ToolCallState::Settled { .. }) && old != new)
            || (matches!(
                old.state,
                ToolCallState::Dispatching { .. } | ToolCallState::Unknown { .. }
            ) && matches!(new.state, ToolCallState::Planned { .. }))
        {
            return Err(error(ErrorCode::InvalidTransition, "tool_ledger"));
        }
        if let (
            ToolCallState::Dispatching {
                attempt_id: old_attempt,
                idempotency_key: old_key,
            }
            | ToolCallState::Unknown {
                attempt_id: old_attempt,
                idempotency_key: old_key,
            },
            ToolCallState::Dispatching {
                attempt_id: new_attempt,
                idempotency_key: new_key,
            }
            | ToolCallState::Unknown {
                attempt_id: new_attempt,
                idempotency_key: new_key,
            },
        ) = (&old.state, &new.state)
        {
            let retry = matches!(old.state, ToolCallState::Unknown { .. })
                && matches!(new.state, ToolCallState::Dispatching { .. });
            if old_key != new_key || (!retry && old_attempt != new_attempt) {
                return Err(error(ErrorCode::InvalidTransition, "tool_ledger.attempt"));
            }
        }
    }
    if previous.model_ledger.len() > next.model_ledger.len()
        || previous
            .model_ledger
            .iter()
            .zip(&next.model_ledger)
            .any(|(old, new)| {
                old.run_id != new.run_id
                    || old.model_step_id != new.model_step_id
                    || old.attempt_id != new.attempt_id
                    || old.purpose != new.purpose
                    || old.route != new.route
                    || old.selection_reason != new.selection_reason
                    || old.request_digest != new.request_digest
                    || (matches!(
                        old.state,
                        ModelAttemptState::Completed {} | ModelAttemptState::Failed { .. }
                    ) && old != new)
                    || (matches!(old.state, ModelAttemptState::Unknown {})
                        && matches!(new.state, ModelAttemptState::Reserved {}))
            })
    {
        return Err(error(ErrorCode::InvalidTransition, "model_ledger"));
    }
    let old = &previous.usage;
    let new = &next.usage;
    if new.model_calls < old.model_calls
        || new.tool_attempts < old.tool_attempts
        || new.repair_attempts < old.repair_attempts
        || new.recovery_attempts < old.recovery_attempts
        || new.elapsed_ms < old.elapsed_ms
    {
        return Err(error(ErrorCode::InvalidTransition, "usage"));
    }
    Ok(())
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
        timing: RunTiming::new(0, p.profile().limits.max_elapsed_ms.get()).unwrap(),
        reservations: vec![AttemptReservation {
            attempt_id: id("first-model-attempt"),
            kind: ReservationKind::Model {
                purpose: ModelPurpose::Agent,
            },
            reserved_at_ms: 0,
        }],
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
        response_ref: None,
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

## `crates/wickle/tests/model_execution.rs`

```rust
//! Policy, persisted accounting, and recovery around a single-call model port.
use futures_util::{StreamExt, stream};
use serde_json::json;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use wickle::*;

#[allow(dead_code)]
mod support;
use support::{admission, id, scope};

fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn request(provider: &str) -> ModelRequest {
    ModelRequest {
        request_id: id("logical-step"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference(provider),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id("model"),
            model_id: id("model"),
            model_version: id("release"),
            version_semantics: VersionSemantics::Pinned,
            provider: id(provider),
            target: JsonObject::new(),
            deployment_revision: None,
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("v1"),
            },
            adapter: reference(&format!("{provider}-adapter")),
            capability_revision: id("capabilities"),
            connection_ref: reference(&format!("{provider}-connection")),
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find evidence".into(),
            }],
        }],
        tools: vec![],
        output: ModelOutput::Text {},
        max_output_tokens: 32.try_into().unwrap(),
        limits: ModelResponseLimits {
            max_input_bytes: 8192,
            max_response_bytes: 4096,
            max_delta_bytes: 1024,
            max_events: 32,
            max_tool_calls: 0,
        },
    }
}

struct Policy {
    calls: AtomicUsize,
    deny_at: AtomicUsize,
    approval_at: AtomicUsize,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            deny_at: AtomicUsize::new(usize::MAX),
            approval_at: AtomicUsize::new(usize::MAX),
        }
    }
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        _: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        let count = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(async move {
            Ok(if count >= self.deny_at.load(Ordering::SeqCst) {
                PolicyDecision::Deny {
                    reason: id("revoked"),
                }
            } else if count >= self.approval_at.load(Ordering::SeqCst) {
                PolicyDecision::RequireApproval {
                    reason: id("review"),
                }
            } else {
                PolicyDecision::Allow {}
            })
        })
    }
}

enum Reply {
    Complete,
    Fail(ModelFailureKind),
    IncompleteTool,
    Pending,
}
struct ScriptedModel {
    store: Arc<MemoryStateStore>,
    binding: ModelPortBinding,
    replies: Mutex<VecDeque<Reply>>,
    observed: Mutex<Vec<(ModelRequest, Id, CancellationToken)>>,
    entered: Notify,
}
impl ModelPort for ScriptedModel {
    fn binding(&self) -> ModelPortBinding {
        self.binding.clone()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        self.observed.lock().unwrap().push((
            request.clone(),
            context.attempt_id.clone(),
            context.cancellation.clone(),
        ));
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("no hidden extra physical call");
        let start = stream::once(async move {
            let saved = self
                .store
                .load(&context.scope, &context.run_id)
                .await
                .unwrap();
            let attempt = saved
                .snapshot
                .model_ledger
                .iter()
                .find(|entry| entry.attempt_id == context.attempt_id)
                .unwrap();
            assert!(matches!(attempt.state, ModelAttemptState::Reserved {}));
            assert_eq!(attempt.request_digest, request.digest());
            assert_eq!(attempt.route, request.route);
            assert!(
                saved
                    .snapshot
                    .reservations
                    .iter()
                    .any(|entry| entry.attempt_id == context.attempt_id)
            );
            self.entered.notify_one();
            Ok(ModelEvent::TextDelta {
                text: "candidate".into(),
            })
        });
        let tail: PortStream<'a, ModelEvent> = match reply {
            Reply::Complete => Box::pin(stream::iter([Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::Stop,
                metadata: ModelResponseMetadata {
                    provider_request_id: Some(id("provider-response")),
                    usage: Some(ModelUsage {
                        measurement: UsageMeasurement::Reported,
                        input_tokens: Some(3),
                        output_tokens: Some(2),
                    }),
                    ..ModelResponseMetadata::default()
                },
                continuation: vec![],
            })])),
            Reply::Fail(kind) => Box::pin(stream::iter([Ok(ModelEvent::ResponseError {
                kind,
                metadata: ModelResponseMetadata::default(),
            })])),
            Reply::IncompleteTool => Box::pin(stream::iter([Ok(ModelEvent::ToolArgumentsDelta {
                index: 0,
                provider_call_id: Some("tool".into()),
                name: Some("search".into()),
                delta: "{\"query\":".into(),
            })])),
            Reply::Pending => Box::pin(stream::pending()),
        };
        Box::pin(start.chain(tail))
    }
}
struct Fixture {
    store: Arc<MemoryStateStore>,
    budget: RunBudget,
    lease: RunLease,
    context: ExecutionContext,
    policy: Arc<Policy>,
}
impl Fixture {
    async fn new(models: u64, recovery: u64) -> Self {
        Self::with_records(models, recovery, Arc::new(RandomIdSource), vec![]).await
    }
    async fn with_records(
        models: u64,
        recovery: u64,
        ids: Arc<dyn IdSource>,
        records: Vec<ProtectedRecord>,
    ) -> Self {
        let clock = Arc::new(SystemClock::new());
        let now = clock.now().unwrap().utc_ms;
        let store = Arc::new(MemoryStateStore::new());
        let mut input = admission("run", "request", "session", "Find evidence", "1").await;
        input.snapshot.limits.max_model_calls = models.try_into().unwrap();
        input.snapshot.limits.max_recovery_attempts = recovery;
        input.snapshot.timing =
            RunTiming::new(now, input.snapshot.limits.max_elapsed_ms.get()).unwrap();
        input.events[0].timestamp_ms = now;
        input.records.extend(records);
        store.admit(&scope(), input).await.unwrap();
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("worker"), now, 20_000)
            .await
            .unwrap();
        let context = ExecutionContext::new(
            ExecutionContextData {
                scope: scope(),
                principal_ref: id("caller"),
                capability_grant_ref: id("grant"),
                trace_context: None,
                system_inputs: Some(SystemInputs::new(JsonObject::from([(
                    "database_fk".into(),
                    json!("host-only-value"),
                )]))),
            },
            CancellationToken::new(),
        );
        let budget = RunBudget::attach(
            store.clone(),
            clock,
            ids,
            scope(),
            id("run"),
            lease.clone(),
            context.cancellation.clone(),
        )
        .await
        .unwrap();
        Self {
            store,
            budget,
            lease,
            context,
            policy: Arc::new(Policy::default()),
        }
    }
    fn model(&self, provider: &str, replies: Vec<Reply>) -> Arc<ScriptedModel> {
        let route = request(provider).route;
        Arc::new(ScriptedModel {
            store: self.store.clone(),
            binding: ModelPortBinding {
                provider: route.provider,
                adapter: route.adapter,
                connection_ref: route.connection_ref,
            },
            replies: Mutex::new(replies.into()),
            observed: Mutex::new(vec![]),
            entered: Notify::new(),
        })
    }
    fn exchange(&self, model: Arc<dyn ModelPort>, retries: u32) -> ModelExchange {
        ModelExchange::new(
            model,
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap()),
        )
        .with_retry_policy(ModelRetryPolicy {
            max_retries: retries,
            backoff_ms: 0,
        })
    }
    async fn saved(&self) -> RunSnapshot {
        self.store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
    }
}

#[tokio::test]
async fn each_retry_has_its_own_saved_attempt_and_rechecks_policy() {
    let fixture = Fixture::new(3, 1).await;
    let model = fixture.model(
        "first",
        vec![Reply::Fail(ModelFailureKind::RateLimited), Reply::Complete],
    );
    let exchange = fixture.exchange(model.clone(), 2);
    let original = request("first");
    let result = exchange
        .generate(&original, &fixture.context, &fixture.budget)
        .await
        .unwrap();
    let Guarded::Completed(ModelExchangeOutcome::Completed { response }) = result else {
        panic!("expected completed response")
    };
    let saved = fixture.saved().await;
    assert_eq!(
        (saved.usage.model_calls, saved.usage.recovery_attempts),
        (2, 1)
    );
    assert_eq!(saved.model_ledger.len(), 2);
    assert_eq!(
        saved.model_ledger[0].state,
        ModelAttemptState::Failed {
            kind: ModelFailureKind::RateLimited
        }
    );
    assert_eq!(saved.model_ledger[1].state, ModelAttemptState::Completed {});
    assert_eq!(saved.model_ledger[1].reported_model_id, None);
    assert_eq!(saved.model_ledger[1].reported_model_version, None);
    assert_eq!(
        saved.model_ledger[1].usage.as_ref().unwrap().output_tokens,
        Some(2)
    );
    assert_eq!(response.request_id, saved.model_ledger[1].attempt_id);
    {
        let observed = model.observed.lock().unwrap();
        assert_eq!(observed.len(), 2);
        assert_ne!(observed[0].1, observed[1].1);
        for (request, attempt, _) in observed.iter() {
            assert_eq!(&request.request_id, attempt);
            assert_eq!(request.route, original.route);
            assert_eq!(request.messages, original.messages);
            // A real Host value exists but no model request surface automatically copies it.
            assert!(
                !serde_json::to_string(request)
                    .unwrap()
                    .contains("host-only-value")
            );
        }
    }
    assert!(
        saved
            .model_ledger
            .iter()
            .all(|entry| entry.model_step_id == original.request_id)
    );
    assert_eq!(fixture.policy.calls.load(Ordering::SeqCst), 4);
    let events = fixture
        .store
        .read_events(&scope(), &id("run"), 0, 10)
        .await
        .unwrap();
    assert_eq!(events.events.len(), 3);
}

#[tokio::test]
async fn exhausted_recovery_and_model_budgets_each_stop_new_requests() {
    for (models, recovery) in [(8, 1), (1, 1)] {
        let fixture = Fixture::new(models, recovery).await;
        let model = fixture.model(
            "first",
            vec![
                Reply::Fail(ModelFailureKind::Transport),
                Reply::Fail(ModelFailureKind::Transport),
            ],
        );
        let exchange = fixture.exchange(model.clone(), 100);
        assert_eq!(
            exchange
                .generate(&request("first"), &fixture.context, &fixture.budget)
                .await
                .unwrap_err()
                .code,
            ErrorCode::BudgetExceeded
        );
        let expected = if models == 1 { 1 } else { 2 };
        assert_eq!(model.observed.lock().unwrap().len(), expected);
        assert_eq!(fixture.saved().await.usage.model_calls, expected as u64);
    }
}

#[tokio::test]
async fn authentication_capability_and_unchanged_context_overflow_are_not_retried() {
    for kind in [
        ModelFailureKind::Authentication,
        ModelFailureKind::Unsupported,
        ModelFailureKind::ContextOverflow,
    ] {
        let fixture = Fixture::new(4, 1).await;
        let model = fixture.model("first", vec![Reply::Fail(kind)]);
        let exchange = fixture.exchange(model.clone(), 3);
        let Guarded::Completed(ModelExchangeOutcome::Failed { failure }) = exchange
            .generate(&request("first"), &fixture.context, &fixture.budget)
            .await
            .unwrap()
        else {
            panic!("expected classified failure")
        };
        assert_eq!(failure.kind, kind);
        assert_eq!(model.observed.lock().unwrap().len(), 1);
        assert_eq!(fixture.saved().await.usage.recovery_attempts, 0);
    }
}

#[tokio::test]
async fn default_retry_is_disabled_and_partial_tools_never_become_a_complete_plan() {
    let fixture = Fixture::new(4, 1).await;
    let model = fixture.model("first", vec![Reply::IncompleteTool]);
    let exchange = fixture.exchange(model.clone(), 0);
    let mut request = request("first");
    request.limits.max_tool_calls = 1;
    let Guarded::Completed(ModelExchangeOutcome::Failed { failure }) = exchange
        .generate(&request, &fixture.context, &fixture.budget)
        .await
        .unwrap()
    else {
        panic!("expected incomplete response rejection")
    };
    assert_eq!(failure.kind, ModelFailureKind::Protocol);
    assert_eq!(failure.partial_text(), "candidate");
    let saved = fixture.saved().await;
    assert!(saved.tool_ledger.is_empty());
    assert_eq!(saved.usage.tool_attempts, 0);
    assert_eq!(saved.usage.recovery_attempts, 0);
    assert_eq!(model.observed.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn initial_denial_and_revocation_after_reservation_both_prevent_adapter_entry() {
    for deny_at in [1, 2, 3] {
        let fixture = Fixture::new(4, 1).await;
        fixture.policy.deny_at.store(deny_at, Ordering::SeqCst);
        let model = fixture.model("first", vec![Reply::Fail(ModelFailureKind::RateLimited)]);
        let exchange = fixture.exchange(model.clone(), 1);
        assert_eq!(
            exchange
                .generate(&request("first"), &fixture.context, &fixture.budget)
                .await
                .unwrap_err()
                .code,
            ErrorCode::AccessDenied
        );
        assert_eq!(
            model.observed.lock().unwrap().len(),
            usize::from(deny_at == 3)
        );
        assert_eq!(
            fixture.saved().await.usage.model_calls,
            u64::from(deny_at >= 2)
        );
    }
}

#[tokio::test]
async fn approval_does_not_dispatch_and_unreserved_approval_consumes_no_budget() {
    for approval_at in [1, 2] {
        let fixture = Fixture::new(4, 1).await;
        fixture
            .policy
            .approval_at
            .store(approval_at, Ordering::SeqCst);
        let model = fixture.model("first", vec![]);
        let exchange = fixture.exchange(model.clone(), 0);
        assert!(matches!(
            exchange
                .generate(&request("first"), &fixture.context, &fixture.budget)
                .await
                .unwrap(),
            Guarded::ApprovalRequired(_)
        ));
        assert!(model.observed.lock().unwrap().is_empty());
        assert_eq!(
            fixture.saved().await.usage.model_calls,
            u64::from(approval_at == 2)
        );
    }
}

#[tokio::test]
async fn wrong_connection_scope_or_opaque_route_is_rejected_before_reservation() {
    let fixture = Fixture::new(4, 1).await;
    let model = fixture.model("second", vec![]);
    let exchange = fixture.exchange(model.clone(), 0);
    assert!(
        exchange
            .generate(&request("first"), &fixture.context, &fixture.budget)
            .await
            .is_err()
    );
    let mut foreign_context = fixture.context.clone();
    foreign_context.data.scope.workspace_id = id("foreign");
    assert_eq!(
        exchange
            .generate(&request("second"), &foreign_context, &fixture.budget)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    let mut changed = request("second");
    changed.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![ModelContent::Opaque {
            continuation: OpaqueContinuation::new(
                &request("first").route,
                json!({"signature":"first-private"}),
            ),
        }],
    });
    assert!(
        exchange
            .generate(&changed, &fixture.context, &fixture.budget)
            .await
            .is_err()
    );
    assert!(model.observed.lock().unwrap().is_empty());
    assert_eq!(fixture.saved().await.usage.model_calls, 0);
}

#[tokio::test]
async fn cancellation_drops_the_adapter_signal_and_retains_an_unknown_charged_attempt() {
    let fixture = Fixture::new(4, 1).await;
    let model = fixture.model("first", vec![Reply::Pending]);
    let exchange = fixture.exchange(model.clone(), 2);
    let request = request("first");
    let execution = exchange.generate(&request, &fixture.context, &fixture.budget);
    let cancel = async {
        model.entered.notified().await;
        fixture.context.cancellation.cancel();
    };
    let (result, ()) = tokio::join!(execution, cancel);
    assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
    let saved = fixture.saved().await;
    assert_eq!(saved.usage.model_calls, 1);
    assert_eq!(saved.usage.recovery_attempts, 0);
    assert_eq!(saved.model_ledger[0].state, ModelAttemptState::Unknown {});
    assert!(model.observed.lock().unwrap()[0].2.is_cancelled());
}

#[tokio::test]
async fn exhausted_recovery_retains_the_last_partial_failure_in_protected_storage() {
    let fixture = Fixture::new(4, 0).await;
    let model = fixture.model("first", vec![Reply::Fail(ModelFailureKind::Transport)]);
    let exchange = fixture.exchange(model.clone(), 1);
    assert_eq!(
        exchange
            .generate(&request("first"), &fixture.context, &fixture.budget)
            .await
            .unwrap_err()
            .code,
        ErrorCode::BudgetExceeded
    );
    let saved = fixture.saved().await;
    let record = fixture
        .store
        .read_record(
            &scope(),
            saved.model_ledger[0].response_ref.as_ref().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(record.value()["outcome"]["result"], "failed");
    assert_eq!(record.value()["outcome"]["failure"]["kind"], "transport");
    assert_eq!(
        record.value()["outcome"]["failure"]["partial_text"],
        "candidate"
    );
    assert_eq!(model.observed.lock().unwrap().len(), 1);
    let mut foreign = scope();
    foreign.workspace_id = id("other");
    assert!(
        fixture
            .store
            .read_record(&foreign, record.reference())
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn caller_cancellation_interrupts_backoff_even_with_an_independent_budget_token() {
    let mut fixture = Fixture::new(4, 1).await;
    fixture.context.cancellation = CancellationToken::new();
    let model = fixture.model("first", vec![Reply::Fail(ModelFailureKind::Transport)]);
    let exchange = ModelExchange::new(
        model.clone(),
        Arc::new(PolicyGate::new(fixture.policy.clone(), Duration::from_secs(1)).unwrap()),
    )
    .with_retry_policy(ModelRetryPolicy {
        max_retries: 1,
        backoff_ms: 1000,
    });
    let request = request("first");
    let cancel = async {
        loop {
            if fixture.saved().await.usage.recovery_attempts == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        fixture.context.cancellation.cancel();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_millis(5), async {
        tokio::join!(
            exchange.generate(&request, &fixture.context, &fixture.budget),
            cancel
        )
    })
    .await
    .expect("caller cancellation must not wait for the one-second backoff");
    assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
    assert_eq!(model.observed.lock().unwrap().len(), 1);
}

struct PendingPolicy {
    entered: Notify,
}
impl PolicyPort for PendingPolicy {
    fn authorize<'a>(
        &'a self,
        _: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test(start_paused = true)]
async fn budget_cancellation_interrupts_policy_even_with_an_independent_caller_token() {
    let mut fixture = Fixture::new(4, 1).await;
    let budget_cancellation = fixture.context.cancellation.clone();
    fixture.context.cancellation = CancellationToken::new();
    let policy = Arc::new(PendingPolicy {
        entered: Notify::new(),
    });
    let model = fixture.model("first", vec![]);
    let exchange = ModelExchange::new(
        model.clone(),
        Arc::new(PolicyGate::new(policy.clone(), Duration::from_secs(1)).unwrap()),
    );
    let request = request("first");
    let cancel = async {
        policy.entered.notified().await;
        budget_cancellation.cancel();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_millis(5), async {
        tokio::join!(
            exchange.generate(&request, &fixture.context, &fixture.budget),
            cancel
        )
    })
    .await
    .expect("budget cancellation must not wait for the policy timeout");
    assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
    assert!(model.observed.lock().unwrap().is_empty());
    assert_eq!(fixture.saved().await.usage.model_calls, 0);
}

struct FixedAttempt;
impl IdSource for FixedAttempt {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id("fixed-attempt"))
    }
}

#[tokio::test]
async fn failed_invocation_or_response_persistence_never_causes_an_untracked_retry() {
    for (record_id, expected_calls) in [
        ("model-invocation-fixed-attempt", 0),
        ("model-response-fixed-attempt", 1),
    ] {
        let existing = ProtectedRecord::new(id(record_id), 1, json!({"original":"immutable"}));
        let fixture =
            Fixture::with_records(4, 1, Arc::new(FixedAttempt), vec![existing.clone()]).await;
        let model = fixture.model("first", vec![Reply::Complete]);
        let exchange = fixture.exchange(model.clone(), 5);
        assert_eq!(
            exchange
                .generate(&request("first"), &fixture.context, &fixture.budget)
                .await
                .unwrap_err()
                .code,
            ErrorCode::RecordConflict
        );
        assert_eq!(model.observed.lock().unwrap().len(), expected_calls);
        let saved = fixture.saved().await;
        assert_eq!(saved.usage.model_calls, 1);
        assert_eq!(saved.usage.recovery_attempts, 0);
        assert_eq!(saved.model_ledger.len(), expected_calls);
        if expected_calls == 1 {
            assert_eq!(saved.model_ledger[0].state, ModelAttemptState::Reserved {});
            assert_eq!(saved.model_ledger[0].response_ref, None);
        }
        assert_eq!(
            fixture
                .store
                .read_record(&scope(), existing.reference())
                .await
                .unwrap()
                .value(),
            existing.value()
        );
    }
}

#[tokio::test]
async fn a_completed_ledger_entry_requires_the_exact_typed_response_and_reservation() {
    let fixture = Fixture::new(4, 1).await;
    fixture.policy.approval_at.store(2, Ordering::SeqCst);
    let model = fixture.model("first", vec![]);
    let exchange = fixture.exchange(model, 0);
    exchange
        .generate(&request("first"), &fixture.context, &fixture.budget)
        .await
        .unwrap();
    let before = fixture.saved().await;
    let entry = &before.model_ledger[0];
    let metadata = ModelResponseMetadata::default();
    let body = StoredModelResponse {
        request_id: entry.attempt_id.clone(),
        route_digest: entry.route.digest(),
        outcome: ModelExchangeOutcome::Completed {
            response: ModelResponse {
                request_id: entry.attempt_id.clone(),
                route_digest: entry.route.digest(),
                text: "Completed candidate".into(),
                tool_calls: vec![],
                finish: ModelFinish::Stop,
                metadata,
                continuation: vec![],
            },
        },
    };
    let valid = serde_json::to_value(&body).unwrap();
    let mut wrong_attempt = valid.clone();
    wrong_attempt["request_id"] = json!("another-attempt");
    let mut wrong_route = valid.clone();
    wrong_route["route_digest"] = serde_json::to_value(request("second").route.digest()).unwrap();
    let mut wrong_metadata = valid.clone();
    wrong_metadata["outcome"]["response"]["metadata"]["reported_model_id"] =
        json!("unreported-model");
    let mut wrong_finish = valid.clone();
    wrong_finish["outcome"]["response"]["finish"] = json!("tool_calls");
    for (index, value) in [
        json!({"unrelated":"record"}),
        wrong_attempt,
        wrong_route,
        wrong_metadata,
        wrong_finish,
    ]
    .into_iter()
    .enumerate()
    {
        let record = ProtectedRecord::new(id(&format!("invalid-{index}")), 1, value);
        let mut commit = support::prepared(
            &before,
            fixture.lease.clone(),
            before.timing.last_observed_at_ms,
        );
        commit.snapshot.model_ledger[0].state = ModelAttemptState::Completed {};
        commit.snapshot.model_ledger[0].response_ref = Some(record.reference().clone());
        commit.records.push(record);
        assert_eq!(
            fixture
                .store
                .commit(&scope(), &id("run"), commit)
                .await
                .unwrap_err()
                .code,
            ErrorCode::InvalidSnapshot
        );
        assert_eq!(fixture.saved().await, before);
    }
    let mut missing = before.clone();
    missing.model_ledger[0].state = ModelAttemptState::Completed {};
    assert!(missing.validate().is_err());
    let mut unreserved = before.clone();
    unreserved.model_ledger[0].attempt_id = id("unreserved");
    assert!(unreserved.validate().is_err());
    let record = ProtectedRecord::new(id("valid-response"), 1, valid);
    let mut commit = support::prepared(
        &before,
        fixture.lease.clone(),
        before.timing.last_observed_at_ms,
    );
    commit.snapshot.model_ledger[0].state = ModelAttemptState::Completed {};
    commit.snapshot.model_ledger[0].response_ref = Some(record.reference().clone());
    commit.records.push(record.clone());
    let saved = fixture
        .store
        .commit(&scope(), &id("run"), commit)
        .await
        .unwrap();
    assert_eq!(
        saved.snapshot.model_ledger[0].response_ref.as_ref(),
        Some(record.reference())
    );
}
```

## `crates/wickle/tests/model_protocol.rs`

```rust
//! Bounded response assembly and provider projection isolation.
use futures_util::{StreamExt, stream};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}

fn route(provider: &str) -> ResolvedModelRoute {
    ResolvedModelRoute {
        binding: reference(provider),
        catalog_revision: id("catalog-1"),
        routing_policy_revision: id("policy-1"),
        requested_model: id("model"),
        model_id: id("model"),
        model_version: id("release-1"),
        version_semantics: VersionSemantics::Pinned,
        provider: id(provider),
        target: JsonObject::from([("region".into(), json!("region-a"))]),
        deployment_revision: None,
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        adapter: reference(&format!("{provider}-adapter")),
        capability_revision: id("capabilities-1"),
        connection_ref: reference(&format!("{provider}-connection")),
    }
}

fn request() -> ModelRequest {
    ModelRequest {
        request_id: id("request"),
        purpose: ModelPurpose::Agent,
        route: route("first"),
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find the requested records".into(),
            }],
        }],
        tools: vec![ModelTool {
            name: id("search"),
            description: "Search available records".into(),
            model_input_schema: json!({
                "type":"object", "required":["query"], "additionalProperties":false,
                "properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1}}
            }),
        }],
        output: ModelOutput::Text {},
        max_output_tokens: 128.try_into().unwrap(),
        limits: ModelResponseLimits {
            max_input_bytes: 16_384,
            max_response_bytes: 4096,
            max_delta_bytes: 1024,
            max_events: 32,
            max_tool_calls: 4,
        },
    }
}

fn text(value: &str) -> ModelEvent {
    ModelEvent::TextDelta { text: value.into() }
}

fn tool(index: u32, provider_id: Option<&str>, name: Option<&str>, delta: &str) -> ModelEvent {
    ModelEvent::ToolArgumentsDelta {
        index,
        provider_call_id: provider_id.map(str::to_owned),
        name: name.map(str::to_owned),
        delta: delta.into(),
    }
}

fn completed(finish: ModelFinish) -> ModelEvent {
    ModelEvent::ResponseCompleted {
        finish,
        metadata: ModelResponseMetadata::default(),
        continuation: vec![],
    }
}

async fn collect(
    request: &ModelRequest,
    events: Vec<ModelEvent>,
) -> Result<ModelResponse, ModelProtocolError> {
    collect_model_response(request, Box::pin(stream::iter(events.into_iter().map(Ok)))).await
}

#[tokio::test]
async fn interleaved_tool_fragments_preserve_separate_arguments_and_unreported_metadata() {
    let request = request();
    let response = collect(
        &request,
        vec![
            text("Searching "),
            tool(0, Some("call-a"), Some("search"), r#"{"query":"ali"#),
            tool(1, Some("call-b"), Some("search"), r#"{"query":"\u03"#),
            text("records"),
            tool(0, None, None, r#"ce","limit":3}"#),
            tool(1, None, None, r#"bb"}"#),
            ModelEvent::ResponseCompleted {
                finish: ModelFinish::ToolCalls,
                metadata: ModelResponseMetadata {
                    provider_request_id: Some(id("provider-request")),
                    reported_model_id: None,
                    reported_model_version: None,
                    usage: Some(ModelUsage {
                        measurement: UsageMeasurement::Reported,
                        input_tokens: Some(12),
                        output_tokens: None,
                    }),
                },
                continuation: vec![],
            },
        ],
    )
    .await
    .unwrap();
    assert_eq!(response.request_id, request.request_id);
    assert_eq!(response.route_digest, request.route.digest());
    assert_eq!(response.text, "Searching records");
    assert_eq!(response.tool_calls.len(), 2);
    assert_eq!(response.tool_calls[0].provider_call_id, id("call-a"));
    assert_eq!(
        response.tool_calls[0].model_inputs,
        JsonObject::from([("query".into(), json!("alice")), ("limit".into(), json!(3)),])
    );
    assert_eq!(response.tool_calls[1].provider_call_id, id("call-b"));
    assert_eq!(response.tool_calls[1].model_inputs["query"], json!("λ"));
    assert!(
        response
            .tool_calls
            .iter()
            .all(|call| call.validation == ToolCallValidation::Valid)
    );
    assert_eq!(response.metadata.reported_model_id, None);
    assert_eq!(response.metadata.reported_model_version, None);
    assert_eq!(response.metadata.usage.unwrap().output_tokens, None);
}

#[tokio::test]
async fn fragments_and_length_limited_responses_never_become_complete_tool_proposals() {
    for events in [
        vec![tool(
            0,
            Some("call"),
            Some("search"),
            r#"{"query":"unfinished"#,
        )],
        vec![tool(
            0,
            Some("call"),
            Some("search"),
            r#"{"query":"complete"}"#,
        )],
        vec![
            tool(0, Some("call"), Some("search"), r#"{"query":"complete"}"#),
            completed(ModelFinish::Length),
        ],
        vec![
            text("partial"),
            ModelEvent::ResponseError {
                kind: ModelFailureKind::Transport,
                metadata: ModelResponseMetadata::default(),
            },
        ],
    ] {
        assert!(collect(&request(), events).await.is_err());
    }
    let disconnected = stream::iter(vec![
        Ok(tool(
            0,
            Some("call"),
            Some("search"),
            r#"{"query":"complete"}"#,
        )),
        Err(ContractError::new(
            ErrorCode::PersistenceUnavailable,
            "transport",
        )),
    ]);
    assert!(
        collect_model_response(&request(), Box::pin(disconnected))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn contradictory_or_repeated_terminals_and_events_after_completion_are_rejected() {
    for events in [
        vec![completed(ModelFinish::Stop), completed(ModelFinish::Stop)],
        vec![
            text("complete"),
            completed(ModelFinish::Stop),
            text("late fragment"),
        ],
        vec![
            tool(0, Some("call"), Some("search"), r#"{"query":"x"}"#),
            completed(ModelFinish::Stop),
        ],
        vec![text("no call"), completed(ModelFinish::ToolCalls)],
        vec![
            tool(0, Some("call"), Some("search"), r#"{"query":"x"}"#),
            completed(ModelFinish::Refusal),
        ],
    ] {
        assert!(collect(&request(), events).await.is_err());
    }
}

#[tokio::test]
async fn malformed_or_ambiguous_argument_json_is_rejected_before_returning_calls() {
    for arguments in [
        r#"{"query":"first","query":"second"}"#,
        r#"{"query":"x","nested":{"key":1,"key":2}}"#,
        r#"{"query":"x","limit":NaN}"#,
        r#"{"query":"x"} trailing"#,
        "[]",
        "null",
        "true",
        "",
        r#"{"query":"unfinished"#,
    ] {
        assert!(
            collect(
                &request(),
                vec![
                    tool(0, Some("call"), Some("search"), arguments),
                    completed(ModelFinish::ToolCalls),
                ]
            )
            .await
            .is_err(),
            "accepted arguments: {arguments}"
        );
    }
}

#[tokio::test]
async fn tool_schema_and_unknown_name_failures_remain_explicit_unexecutable_proposals() {
    let response = collect(
        &request(),
        vec![
            tool(0, Some("unknown"), Some("unregistered"), r#"{"query":"x"}"#),
            tool(1, Some("wrong-type"), Some("search"), r#"{"query":7}"#),
            tool(
                2,
                Some("hidden-input"),
                Some("search"),
                r#"{"query":"x","workspace_id":"invented"}"#,
            ),
            tool(
                3,
                Some("valid"),
                Some("search"),
                r#"{"query":"x","limit":2}"#,
            ),
            completed(ModelFinish::ToolCalls),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        response.tool_calls[0].validation,
        ToolCallValidation::UnknownTool
    );
    assert_eq!(
        response.tool_calls[1].validation,
        ToolCallValidation::InvalidArguments
    );
    assert_eq!(
        response.tool_calls[2].validation,
        ToolCallValidation::InvalidArguments
    );
    assert_eq!(response.tool_calls[3].validation, ToolCallValidation::Valid);
    assert_eq!(
        response.tool_calls[2].model_inputs["workspace_id"],
        json!("invented")
    );
}

#[tokio::test]
async fn provider_call_collisions_and_metadata_reassignment_are_rejected() {
    for events in [
        vec![
            tool(0, Some("duplicate"), Some("search"), r#"{"query":"a"}"#),
            tool(1, Some("duplicate"), Some("search"), r#"{"query":"b"}"#),
        ],
        vec![
            tool(0, Some("original"), Some("search"), "{"),
            tool(0, Some("replacement"), None, r#""query":"a"}"#),
        ],
        vec![
            tool(0, Some("call"), Some("search"), "{"),
            tool(0, None, Some("other"), r#""query":"a"}"#),
        ],
        vec![tool(0, None, None, r#"{"query":"a"}"#)],
    ] {
        let mut events = events;
        events.push(completed(ModelFinish::ToolCalls));
        assert!(collect(&request(), events).await.is_err());
    }
    for (provider_id, name) in [
        ("", "search"),
        (" \n", "search"),
        ("call", ""),
        ("call", " \n"),
    ] {
        assert!(
            collect(
                &request(),
                vec![
                    tool(0, Some(provider_id), Some(name), r#"{"query":"a"}"#),
                    completed(ModelFinish::ToolCalls),
                ]
            )
            .await
            .is_err()
        );
    }
}

#[tokio::test]
async fn response_byte_delta_and_tool_count_limits_fail_without_truncating_to_success() {
    let mut limited = request();
    limited.limits.max_response_bytes = 3;
    limited.limits.max_delta_bytes = 2;
    assert!(
        collect(
            &limited,
            vec![text("é"), text("é"), completed(ModelFinish::Stop)]
        )
        .await
        .is_err()
    );
    limited = request();
    limited.limits.max_delta_bytes = 3;
    assert!(
        collect(&limited, vec![text("éé"), completed(ModelFinish::Stop)])
            .await
            .is_err()
    );
    limited = request();
    limited.limits.max_tool_calls = 1;
    assert!(
        collect(
            &limited,
            vec![
                tool(0, Some("first"), Some("search"), r#"{"query":"a"}"#),
                tool(1, Some("second"), Some("search"), r#"{"query":"b"}"#),
                completed(ModelFinish::ToolCalls),
            ]
        )
        .await
        .is_err()
    );
    limited.limits.max_tool_calls = 0;
    assert!(
        collect(
            &limited,
            vec![
                tool(0, Some("first"), Some("search"), r#"{"query":"a"}"#),
                completed(ModelFinish::ToolCalls)
            ]
        )
        .await
        .is_err()
    );
    let response = collect(
        &limited,
        vec![text("text remains valid"), completed(ModelFinish::Stop)],
    )
    .await
    .unwrap();
    assert_eq!(response.text, "text remains valid");
}

#[tokio::test]
async fn empty_deltas_cannot_bypass_the_finite_event_limit() {
    let mut limited = request();
    limited.limits.max_events = 3;
    let at_limit = collect(
        &limited,
        vec![text("first"), text(" second"), completed(ModelFinish::Stop)],
    )
    .await
    .unwrap();
    assert_eq!(at_limit.text, "first second");
    let observed = Arc::new(AtomicUsize::new(0));
    let count = observed.clone();
    let events = stream::repeat_with(|| Ok(text(""))).inspect(move |_| {
        count.fetch_add(1, Ordering::SeqCst);
    });
    assert!(
        collect_model_response(&limited, Box::pin(events))
            .await
            .is_err()
    );
    assert_eq!(observed.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn wire_identifiers_use_the_protocol_rules_without_requiring_uuid_format() {
    for (provider_id, name) in [
        ("call\nname".to_owned(), "search".to_owned()),
        ("x".repeat(257), "search".to_owned()),
        ("call".to_owned(), "search tool".to_owned()),
        ("call".to_owned(), "search.tool".to_owned()),
        ("call".to_owned(), "검색".to_owned()),
        ("call".to_owned(), "x".repeat(65)),
    ] {
        assert!(
            collect(
                &request(),
                vec![
                    tool(0, Some(&provider_id), Some(&name), r#"{"query":"a"}"#),
                    completed(ModelFinish::ToolCalls),
                ],
            )
            .await
            .is_err()
        );
    }
    let response = collect(
        &request(),
        vec![
            tool(
                0,
                Some("provider:opaque/call"),
                Some("search"),
                r#"{"query":"a"}"#,
            ),
            completed(ModelFinish::ToolCalls),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        response.tool_calls[0].provider_call_id,
        id("provider:opaque/call")
    );
}

#[tokio::test]
async fn opaque_continuation_requires_the_exact_provider_route_and_release() {
    let mut original = request();
    let opaque =
        OpaqueContinuation::new(&original.route, json!({"thought_signature":"opaque-data"}));
    original.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![ModelContent::Opaque {
            continuation: opaque.clone(),
        }],
    });
    assert!(
        collect(
            &original,
            vec![text("continued"), completed(ModelFinish::Stop)]
        )
        .await
        .is_ok()
    );
    let mut altered_routes = vec![route("second")];
    let mut changed = original.route.clone();
    changed.model_version = id("release-2");
    altered_routes.push(changed);
    let mut changed = original.route.clone();
    changed.api_contract.version = id("v2");
    altered_routes.push(changed);
    let mut changed = original.route.clone();
    changed.connection_ref.version = id("rotated-connection");
    altered_routes.push(changed);
    let mut changed = original.route.clone();
    changed.target.insert("region".into(), json!("region-b"));
    altered_routes.push(changed);
    for changed in altered_routes {
        let mut incompatible = original.clone();
        incompatible.route = changed;
        assert!(
            collect(
                &incompatible,
                vec![text("must not continue"), completed(ModelFinish::Stop)]
            )
            .await
            .is_err()
        );
    }
    let foreign_opaque = OpaqueContinuation::new(&route("second"), json!({"signature":"foreign"}));
    assert!(
        collect(
            &request(),
            vec![
                text("answer"),
                ModelEvent::ResponseCompleted {
                    finish: ModelFinish::Stop,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![foreign_opaque],
                }
            ]
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn preflight_rejects_mismatched_port_bindings_and_oversized_input() {
    let request = request();
    let binding = ModelPortBinding {
        provider: request.route.provider.clone(),
        adapter: request.route.adapter.clone(),
        connection_ref: request.route.connection_ref.clone(),
    };
    assert!(binding.matches_route(&request.route));
    let mut wrong = binding.clone();
    wrong.provider = id("second");
    assert!(!wrong.matches_route(&request.route));
    wrong = binding.clone();
    wrong.adapter.version = id("different-adapter");
    assert!(!wrong.matches_route(&request.route));
    wrong = binding;
    wrong.connection_ref.version = id("different-credential-binding");
    assert!(!wrong.matches_route(&request.route));
    let mut oversized = request;
    oversized.limits.max_input_bytes = 1;
    assert!(oversized.validate().is_err());
}
```

## `tests/support/model_consumer.rs`

```rust
use futures_util::stream;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
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

fn route(provider: &str) -> ResolvedModelRoute {
    ResolvedModelRoute {
        binding: reference(provider),
        catalog_revision: id("catalog-1"),
        routing_policy_revision: id("policy-1"),
        requested_model: id("example-model"),
        model_id: id("example-model"),
        model_version: id("release-1"),
        version_semantics: VersionSemantics::Pinned,
        provider: id(provider),
        target: JsonObject::new(),
        deployment_revision: None,
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        adapter: reference(&format!("{provider}-adapter")),
        capability_revision: id("capabilities-1"),
        connection_ref: reference(&format!("{provider}-connection")),
    }
}

fn request(provider: &str) -> ModelRequest {
    ModelRequest {
        request_id: id(&format!("{provider}-request")),
        purpose: ModelPurpose::Agent,
        route: route(provider),
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Return the available information".into(),
            }],
        }],
        tools: vec![],
        output: ModelOutput::Text {},
        max_output_tokens: 32.try_into().unwrap(),
        limits: ModelResponseLimits {
            max_input_bytes: 4096,
            max_response_bytes: 1024,
            max_delta_bytes: 256,
            max_events: 8,
            max_tool_calls: 0,
        },
    }
}

#[derive(Debug, PartialEq)]
struct ObservedCall {
    connection: VersionedRef,
    credential: &'static str,
    opaque_blocks: usize,
}

// These are synthetic Host-owned credentials. No real accounts or keys are used.
struct FirstModel {
    observed: Arc<Mutex<Vec<ObservedCall>>>,
}
struct SecondModel {
    observed: Arc<Mutex<Vec<ObservedCall>>>,
}

fn binding(provider: &str) -> ModelPortBinding {
    let route = route(provider);
    ModelPortBinding {
        provider: route.provider,
        adapter: route.adapter,
        connection_ref: route.connection_ref,
    }
}

fn observe(observed: &Mutex<Vec<ObservedCall>>, request: &ModelRequest, credential: &'static str) {
    observed.lock().unwrap().push(ObservedCall {
        connection: request.route.connection_ref.clone(),
        credential,
        opaque_blocks: request
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .filter(|content| matches!(content, ModelContent::Opaque { .. }))
            .count(),
    });
}

impl ModelPort for FirstModel {
    fn binding(&self) -> ModelPortBinding {
        binding("first")
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        observe(&self.observed, request, "synthetic-first-credential");
        Box::pin(stream::iter(vec![
            Ok(ModelEvent::TextDelta {
                text: "first response".into(),
            }),
            Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::Stop,
                metadata: ModelResponseMetadata::default(),
                continuation: vec![OpaqueContinuation::new(
                    &request.route,
                    json!({"signature":"first-only"}),
                )],
            }),
        ]))
    }
}

impl ModelPort for SecondModel {
    fn binding(&self) -> ModelPortBinding {
        binding("second")
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        observe(&self.observed, request, "synthetic-second-credential");
        Box::pin(stream::iter(vec![
            Ok(ModelEvent::TextDelta {
                text: "second ".into(),
            }),
            Ok(ModelEvent::TextDelta {
                text: "response".into(),
            }),
            Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::Stop,
                metadata: ModelResponseMetadata::default(),
                continuation: vec![],
            }),
        ]))
    }
}

// Minimal Host dispatch demonstrating the public protocol. It does not implement
// routing policy, retries, budgets, or the agent's model/tool loop.
async fn invoke(
    registry: &BTreeMap<Id, Arc<dyn ModelPort>>,
    request: &ModelRequest,
) -> Result<ModelResponse, Box<dyn std::error::Error>> {
    request.validate()?;
    let port = registry
        .get(&request.route.provider)
        .ok_or_else(|| ContractError::new(ErrorCode::ComponentUnavailable, "provider"))?;
    if !port.binding().matches_route(&request.route) {
        return Err(ContractError::new(ErrorCode::InvalidReference, "connection_binding").into());
    }
    let context = ModelCallContext {
        attempt_id: id(&format!("{}-attempt", request.request_id)),
        run_id: id("run"),
        scope: Scope {
            tenant_id: id("tenant"),
            workspace_id: id("workspace"),
            user_id: None,
        },
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(5),
    };
    Ok(collect_model_response(request, port.generate(request, &context)).await?)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let first_calls = Arc::new(Mutex::new(Vec::new()));
    let second_calls = Arc::new(Mutex::new(Vec::new()));
    let registry: BTreeMap<Id, Arc<dyn ModelPort>> = BTreeMap::from([
        (
            id("first"),
            Arc::new(FirstModel {
                observed: first_calls.clone(),
            }) as Arc<dyn ModelPort>,
        ),
        (
            id("second"),
            Arc::new(SecondModel {
                observed: second_calls.clone(),
            }) as Arc<dyn ModelPort>,
        ),
    ]);
    let first = invoke(&registry, &request("first")).await?;
    assert_eq!(first.text, "first response");
    assert_eq!(first.continuation.len(), 1);
    assert_eq!(first.metadata.reported_model_version, None);

    let mut incompatible = request("second");
    incompatible.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![ModelContent::Opaque {
            continuation: first.continuation[0].clone(),
        }],
    });
    assert!(invoke(&registry, &incompatible).await.is_err());
    assert!(second_calls.lock().unwrap().is_empty());
    incompatible = request("second");
    incompatible.route.connection_ref = route("first").connection_ref;
    assert!(invoke(&registry, &incompatible).await.is_err());
    assert!(second_calls.lock().unwrap().is_empty());

    // A Host may build a new projection from ordinary conversation content when
    // it can preserve the required meaning. Foreign opaque data is not copied.
    let second = invoke(&registry, &request("second")).await?;
    assert_eq!(second.text, "second response");
    assert!(second.continuation.is_empty());
    assert_eq!(
        *first_calls.lock().unwrap(),
        vec![ObservedCall {
            connection: route("first").connection_ref,
            credential: "synthetic-first-credential",
            opaque_blocks: 0,
        }]
    );
    assert_eq!(
        *second_calls.lock().unwrap(),
        vec![ObservedCall {
            connection: route("second").connection_ref,
            credential: "synthetic-second-credential",
            opaque_blocks: 0,
        }]
    );
    println!(
        "model consumer: two concrete adapters through dyn ModelPort; one call per connection; foreign opaque and credential bindings rejected before dispatch"
    );
    Ok(())
}
```
