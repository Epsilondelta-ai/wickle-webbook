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

pub(crate) fn charge(
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
