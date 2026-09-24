//! Reservations and stop boundaries exercised against a real state store.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Barrier, Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use wickle::*;

#[allow(dead_code)]
mod support;
use support::{admission, id, scope};

#[derive(Default)]
struct FakeClock {
    reading: Mutex<(i64, u64)>,
    changed: Notify,
}
impl FakeClock {
    fn set(&self, utc_ms: i64, monotonic_ms: u64) {
        *self.reading.lock().unwrap() = (utc_ms, monotonic_ms);
        self.changed.notify_waiters();
    }
}
impl Clock for FakeClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        let (utc_ms, monotonic_ms) = *self.reading.lock().unwrap();
        Ok(ClockReading {
            utc_ms,
            monotonic_ms,
        })
    }
    fn sleep_until<'a>(&'a self, deadline: u64) -> PortFuture<'a, ()> {
        Box::pin(async move {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.now()?.monotonic_ms >= deadline {
                    return Ok(());
                }
                changed.await;
            }
        })
    }
}
#[derive(Default)]
struct FixedIds(AtomicUsize);
impl IdSource for FixedIds {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id(&format!(
            "attempt-{}",
            self.0.fetch_add(1, Ordering::SeqCst)
        )))
    }
}
struct RepeatedId;
impl IdSource for RepeatedId {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id("same-attempt"))
    }
}

enum CommitControl {
    Race(Barrier),
    Pause { entered: Notify, release: Semaphore },
    Fail,
    ExpireDuringLeaseRead(Arc<FakeClock>),
}
struct ControlledStore {
    inner: Arc<MemoryStateStore>,
    control: CommitControl,
}
impl StateStore for ControlledStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
        request_id: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        self.inner.find_request(scope, session_id, request_id)
    }
    fn admit<'a>(&'a self, s: &'a Scope, input: AdmissionInput) -> PortFuture<'a, AdmissionResult> {
        self.inner.admit(s, input)
    }
    fn load<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, StoredRun> {
        self.inner.load(s, r)
    }
    fn load_session<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, SessionSnapshot> {
        self.inner.load_session(s, r)
    }
    fn check_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        lease: &'a RunLease,
        now: i64,
    ) -> PortFuture<'a, RunLease> {
        Box::pin(async move {
            let current = self.inner.check_lease(s, r, lease, now).await?;
            if let CommitControl::ExpireDuringLeaseRead(clock) = &self.control {
                clock.set(current.expires_at_ms, current.expires_at_ms as u64);
            }
            Ok(current)
        })
    }
    fn acquire_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        owner: &'a Id,
        now: i64,
        ttl: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.acquire_lease(s, r, owner, now, ttl)
    }
    fn renew_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        lease: &'a RunLease,
        now: i64,
        ttl: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.renew_lease(s, r, lease, now, ttl)
    }
    fn release_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        lease: &'a RunLease,
        now: i64,
    ) -> PortFuture<'a, ()> {
        self.inner.release_lease(s, r, lease, now)
    }
    fn read_events<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        after: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage> {
        self.inner.read_events(s, r, after, limit)
    }
    fn read_record<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        self.inner.read_record(s, r)
    }
    fn commit<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            match &self.control {
                CommitControl::Race(barrier) => {
                    barrier.wait().await;
                }
                CommitControl::Fail => {
                    return Err(ContractError::new(
                        ErrorCode::PersistenceUnavailable,
                        "injected.commit",
                    ));
                }
                CommitControl::Pause { .. } | CommitControl::ExpireDuringLeaseRead(_) => {}
            }
            let saved = self.inner.commit(s, r, input).await?;
            if let CommitControl::Pause { entered, release } = &self.control {
                entered.notify_one();
                release.acquire().await.unwrap().forget();
            }
            Ok(saved)
        })
    }
}
struct Fixture {
    store: Arc<MemoryStateStore>,
    clock: Arc<FakeClock>,
    ids: Arc<dyn IdSource>,
    lease: RunLease,
    cancellation: CancellationToken,
}
impl Fixture {
    async fn new(models: u64, tools: u64, repair: u64, recovery: u64) -> Self {
        let store = Arc::new(MemoryStateStore::new());
        let mut input = admission("run", "request", "session", "Read evidence", "1").await;
        input.snapshot.limits.max_model_calls = models.try_into().unwrap();
        input.snapshot.limits.max_tool_attempts = tools;
        input.snapshot.limits.max_repair_attempts = repair;
        input.snapshot.limits.max_recovery_attempts = recovery;
        input.snapshot.limits.max_elapsed_ms = 100.try_into().unwrap();
        input.snapshot.timing = RunTiming::new(0, 100).unwrap();
        store.admit(&scope(), input).await.unwrap();
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("worker"), 0, 200)
            .await
            .unwrap();
        Self {
            store,
            clock: Arc::new(FakeClock::default()),
            ids: Arc::new(FixedIds::default()),
            lease,
            cancellation: CancellationToken::new(),
        }
    }
    async fn budget(&self, store: Arc<dyn StateStore>) -> RunBudget {
        RunBudget::attach(
            store,
            self.clock.clone(),
            self.ids.clone(),
            scope(),
            id("run"),
            self.lease.clone(),
            self.cancellation.clone(),
        )
        .await
        .unwrap()
    }
    async fn saved(&self) -> RunSnapshot {
        self.store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
    }
}
fn model(purpose: ModelPurpose) -> ReservationKind {
    ReservationKind::Model { purpose }
}
fn counted(counter: &AtomicUsize) -> std::future::Ready<Result<(), ContractError>> {
    counter.fetch_add(1, Ordering::SeqCst);
    std::future::ready(Ok(()))
}

#[tokio::test]
async fn model_purposes_share_a_limit_and_do_not_consume_the_tool_budget() {
    let fixture = Fixture::new(3, 1, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    let calls = AtomicUsize::new(0);
    for purpose in [
        ModelPurpose::Agent,
        ModelPurpose::Verification,
        ModelPurpose::Compaction,
    ] {
        budget
            .execute(model(purpose), |_| counted(&calls))
            .await
            .unwrap();
    }
    let error = budget
        .execute(model(ModelPurpose::Agent), |_| counted(&calls))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::BudgetExceeded);
    budget
        .execute(
            ReservationKind::Tool {
                call_id: id("planned-call"),
            },
            |_| counted(&calls),
        )
        .await
        .unwrap();
    for kind in [
        ReservationKind::Repair {},
        ReservationKind::Recovery {},
        ReservationKind::Tool {
            call_id: id("another-call"),
        },
    ] {
        assert_eq!(
            budget
                .execute(kind, |_| counted(&calls))
                .await
                .unwrap_err()
                .code,
            ErrorCode::BudgetExceeded
        );
    }
    let saved = fixture.saved().await;
    assert_eq!(
        (
            calls.load(Ordering::SeqCst),
            saved.usage.model_calls,
            saved.usage.tool_attempts
        ),
        (4, 3, 1)
    );
    assert_eq!(saved.reservations.len(), 4);
}

#[tokio::test]
async fn the_last_model_slot_can_only_authorize_one_competing_dispatch() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let store = Arc::new(ControlledStore {
        inner: fixture.store.clone(),
        control: CommitControl::Race(Barrier::new(2)),
    });
    let left = fixture.budget(store.clone()).await;
    let right = fixture.budget(store).await;
    let calls = AtomicUsize::new(0);
    let (left, right) = tokio::join!(
        left.execute(model(ModelPurpose::Agent), |_| counted(&calls)),
        right.execute(model(ModelPurpose::Verification), |_| counted(&calls)),
    );
    let results = [left, right];
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results.into_iter().find_map(Result::err).unwrap().code,
        ErrorCode::RevisionConflict
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let saved = fixture.saved().await;
    assert_eq!((saved.usage.model_calls, saved.reservations.len()), (1, 1));
}

#[tokio::test]
async fn failed_persistence_never_constructs_the_call_or_charges_the_store() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let store = Arc::new(ControlledStore {
        inner: fixture.store.clone(),
        control: CommitControl::Fail,
    });
    let budget = fixture.budget(store).await;
    let calls = AtomicUsize::new(0);
    assert_eq!(
        budget
            .execute(model(ModelPurpose::Agent), |_| counted(&calls))
            .await
            .unwrap_err()
            .code,
        ErrorCode::PersistenceUnavailable
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.saved().await.usage.model_calls, 0);
}

#[tokio::test]
async fn a_stop_during_reservation_blocks_dispatch_without_refunding_the_saved_attempt() {
    for stop in [
        ErrorCode::Cancelled,
        ErrorCode::DeadlineExceeded,
        ErrorCode::LeaseLost,
    ] {
        let fixture = Fixture::new(2, 0, 0, 0).await;
        let store = Arc::new(ControlledStore {
            inner: fixture.store.clone(),
            control: CommitControl::Pause {
                entered: Notify::new(),
                release: Semaphore::new(0),
            },
        });
        let budget = fixture.budget(store.clone()).await;
        let calls = AtomicUsize::new(0);
        let execute = budget.execute(model(ModelPurpose::Agent), |_| counted(&calls));
        let interrupt = async {
            let CommitControl::Pause { entered, release } = &store.control else {
                unreachable!()
            };
            entered.notified().await;
            match stop {
                ErrorCode::Cancelled => fixture.cancellation.cancel(),
                ErrorCode::DeadlineExceeded => fixture.clock.set(100, 100),
                ErrorCode::LeaseLost => {
                    fixture
                        .store
                        .release_lease(&scope(), &id("run"), &fixture.lease, 0)
                        .await
                        .unwrap();
                    fixture
                        .store
                        .acquire_lease(&scope(), &id("run"), &id("replacement"), 0, 200)
                        .await
                        .unwrap();
                }
                _ => unreachable!(),
            }
            release.add_permits(1);
        };
        let (result, ()) = tokio::join!(execute, interrupt);
        assert_eq!(result.unwrap_err().code, stop);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let saved = fixture.saved().await;
        assert_eq!((saved.usage.model_calls, saved.reservations.len()), (1, 1));
    }
}

#[tokio::test]
async fn an_unknown_attempt_stays_charged_after_reattach_and_retry_gets_a_new_id() {
    let fixture = Fixture::new(2, 0, 1, 1).await;
    let first = fixture.budget(fixture.store.clone()).await;
    first
        .execute(model(ModelPurpose::Agent), |_| async {
            Err::<(), _>(ContractError::new(
                ErrorCode::PersistenceUnavailable,
                "response.unknown",
            ))
        })
        .await
        .unwrap_err();
    fixture.clock.set(10, 10);
    let resumed = fixture.budget(fixture.store.clone()).await;
    resumed
        .execute(model(ModelPurpose::Agent), |_| async { Ok(()) })
        .await
        .unwrap();
    resumed.reserve(ReservationKind::Repair {}).await.unwrap();
    resumed.reserve(ReservationKind::Recovery {}).await.unwrap();
    let saved = fixture.saved().await;
    assert_eq!(saved.usage.model_calls, 2);
    assert_eq!(saved.usage.repair_attempts, 1);
    assert_eq!(saved.usage.recovery_attempts, 1);
    assert_ne!(
        saved.reservations[0].attempt_id,
        saved.reservations[1].attempt_id
    );
    assert_eq!(saved.reservations[0].reserved_at_ms, 0);
    assert_eq!(saved.reservations[1].reserved_at_ms, 10);
    assert_eq!(saved.usage.elapsed_ms, 10);
    assert_eq!(saved.timing.deadline_at_ms, 100);
}

#[tokio::test]
async fn reused_attempt_identity_cannot_authorize_a_second_physical_call() {
    let mut fixture = Fixture::new(2, 0, 0, 0).await;
    fixture.ids = Arc::new(RepeatedId);
    let budget = fixture.budget(fixture.store.clone()).await;
    let calls = AtomicUsize::new(0);
    budget
        .execute(model(ModelPurpose::Agent), |_| counted(&calls))
        .await
        .unwrap();
    assert_eq!(
        budget
            .execute(model(ModelPurpose::Agent), |_| counted(&calls))
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidSnapshot
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.saved().await.usage.model_calls, 1);
}

#[tokio::test]
async fn active_elapsed_uses_monotonic_time_and_reattach_counts_offline_time() {
    let fixture = Fixture::new(3, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    fixture.clock.set(50_000, 10);
    budget.reserve(model(ModelPurpose::Agent)).await.unwrap();
    fixture.clock.set(-50_000, 20);
    budget
        .reserve(model(ModelPurpose::Verification))
        .await
        .unwrap();
    assert_eq!(fixture.saved().await.usage.elapsed_ms, 20);
    let regression = RunBudget::attach(
        fixture.store.clone(),
        fixture.clock.clone(),
        fixture.ids.clone(),
        scope(),
        id("run"),
        fixture.lease.clone(),
        fixture.cancellation.clone(),
    )
    .await;
    assert!(matches!(
        regression,
        Err(ContractError {
            code: ErrorCode::ClockRegression,
            ..
        })
    ));
    fixture.clock.set(95, 40);
    let resumed = fixture.budget(fixture.store.clone()).await;
    assert_eq!(resumed.elapsed_ms().unwrap(), 95);
    fixture.clock.set(100, 45);
    let calls = AtomicUsize::new(0);
    assert_eq!(
        resumed
            .execute(model(ModelPurpose::Compaction), |_| counted(&calls))
            .await
            .unwrap_err()
            .code,
        ErrorCode::DeadlineExceeded
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.saved().await.usage.model_calls, 2);
}

#[tokio::test]
async fn waiting_observation_consumes_time_but_no_model_attempts() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let snapshot = fixture.saved().await;
    let mut commit = support::prepared(&snapshot, fixture.lease.clone(), 0);
    commit.snapshot.status = RunStatus::Waiting;
    commit.snapshot.phase = RunPhase::Waiting;
    commit.snapshot.wait = Some(WaitState {
        wait_id: id("wait"),
        target: WaitTarget::Approval {
            target: ApprovalTarget::Tool {
                call_id: id("call"),
                binding_digest: canonical_digest(&serde_json::json!({})),
            },
        },
        expires_at_ms: None,
    });
    // A wait target needs a matching planned tool in the saved checkpoint.
    commit.snapshot.tool_ledger.push(ToolLedgerEntry {
        call: ToolCall {
            provider_arguments: None,
            call_id: id("call"),
            model_request_id: id("model"),
            provider_call_id: id("provider-call"),
            tool_name: id("read"),
            model_inputs: JsonObject::new(),
            descriptor_digest: Some(canonical_digest(&serde_json::json!({}))),
            bound_input_ref: None,
        },
        state: ToolCallState::Planned {},
    });
    fixture
        .store
        .commit(&scope(), &id("run"), commit)
        .await
        .unwrap();
    let waiting = fixture.budget(fixture.store.clone()).await;
    let observer = waiting.wait_for_cancellation_or_deadline();
    tokio::pin!(observer);
    assert!(futures_util::poll!(observer.as_mut()).is_pending());
    fixture.clock.set(100, 100);
    assert_eq!(
        observer.await.unwrap_err().code,
        ErrorCode::DeadlineExceeded
    );
    assert_eq!(fixture.saved().await.usage.model_calls, 0);
    let resumed = fixture.budget(fixture.store.clone()).await;
    assert_eq!(resumed.elapsed_ms().unwrap(), 100);
}

#[tokio::test]
async fn cancellation_after_an_effect_does_not_undo_it_or_refund_the_attempt() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    let effects = AtomicUsize::new(0);
    let error = budget
        .execute(model(ModelPurpose::Agent), |_| async {
            effects.fetch_add(1, Ordering::SeqCst);
            fixture.cancellation.cancel();
            Ok(())
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Cancelled);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.saved().await.usage.model_calls, 1);
    assert_eq!(
        budget
            .execute(model(ModelPurpose::Agent), |_| counted(&effects))
            .await
            .unwrap_err()
            .code,
        ErrorCode::Cancelled
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dropping_a_waiter_does_not_cancel_execution_and_a_clock_timer_wakes_at_deadline() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    let mut observer = Box::pin(budget.wait_for_cancellation_or_deadline());
    assert!(futures_util::poll!(observer.as_mut()).is_pending());
    drop(observer);
    budget
        .execute(model(ModelPurpose::Agent), |_| async { Ok(()) })
        .await
        .unwrap();
    assert!(!fixture.cancellation.is_cancelled());
    let clock = SystemClock::new();
    let reading = clock.now().unwrap();
    clock.sleep_until(reading.monotonic_ms + 1).await.unwrap();
    assert!(clock.now().unwrap().monotonic_ms > reading.monotonic_ms);
}

#[tokio::test]
async fn an_in_flight_operation_is_interrupted_when_the_original_deadline_arrives() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    let calls = AtomicUsize::new(0);
    let entered = Notify::new();
    let execution = budget.execute(model(ModelPurpose::Agent), |_| {
        calls.fetch_add(1, Ordering::SeqCst);
        entered.notify_one();
        std::future::pending::<Result<(), ContractError>>()
    });
    tokio::pin!(execution);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        tokio::select! {
            result = &mut execution => panic!("operation should still be in flight: {result:?}"),
            _ = entered.notified() => {},
        }
    })
    .await
    .unwrap();
    fixture.clock.set(100, 100);
    assert_eq!(
        execution.await.unwrap_err().code,
        ErrorCode::DeadlineExceeded
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.saved().await.usage.model_calls, 1);
}

#[tokio::test]
async fn lease_expiration_while_checking_storage_blocks_the_next_call() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    fixture
        .store
        .renew_lease(&scope(), &id("run"), &fixture.lease, 0, 10)
        .await
        .unwrap();
    let store = Arc::new(ControlledStore {
        inner: fixture.store.clone(),
        control: CommitControl::ExpireDuringLeaseRead(fixture.clock.clone()),
    });
    let budget = fixture.budget(store).await;
    let calls = AtomicUsize::new(0);
    assert_eq!(
        budget
            .execute(model(ModelPurpose::Agent), |_| counted(&calls))
            .await
            .unwrap_err()
            .code,
        ErrorCode::LeaseLost
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.saved().await.usage.model_calls, 1);
}

#[tokio::test]
async fn a_monotonic_clock_regression_cannot_extend_a_live_segment() {
    let fixture = Fixture::new(2, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    fixture.clock.set(20, 20);
    budget.reserve(model(ModelPurpose::Agent)).await.unwrap();
    fixture.clock.set(30, 10);
    let calls = AtomicUsize::new(0);
    assert_eq!(
        budget
            .execute(model(ModelPurpose::Agent), |_| counted(&calls))
            .await
            .unwrap_err()
            .code,
        ErrorCode::ClockRegression
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.saved().await.usage.model_calls, 1);
}

#[tokio::test]
async fn saved_attempts_and_original_deadline_cannot_be_refunded_or_replaced() {
    let fixture = Fixture::new(2, 1, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    budget.reserve(model(ModelPurpose::Agent)).await.unwrap();
    let before = fixture.saved().await;
    let mut refund = support::prepared(&before, fixture.lease.clone(), 0);
    refund.snapshot.reservations.clear();
    refund.snapshot.usage.model_calls = 0;
    assert_eq!(
        fixture
            .store
            .commit(&scope(), &id("run"), refund)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidTransition
    );
    let mut reset = support::prepared(&before, fixture.lease.clone(), 0);
    reset.snapshot.timing = RunTiming::new(1, 100).unwrap();
    reset.snapshot.reservations[0].reserved_at_ms = 1;
    assert_eq!(
        fixture
            .store
            .commit(&scope(), &id("run"), reset)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidTransition
    );
    let mut extra = support::prepared(&before, fixture.lease.clone(), 0);
    extra.snapshot.usage.model_calls += 1;
    assert_eq!(
        fixture
            .store
            .commit(&scope(), &id("run"), extra)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidSnapshot
    );
    assert_eq!(fixture.saved().await, before);
}

#[tokio::test]
async fn restoring_a_snapshot_rejects_missing_reservations_and_forged_counts() {
    let fixture = Fixture::new(2, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    budget.reserve(model(ModelPurpose::Agent)).await.unwrap();
    let snapshot = fixture.saved().await;
    let mut missing = snapshot.clone();
    missing.reservations.clear();
    let mut forged = snapshot.clone();
    forged.usage.model_calls = 2;
    let mut duplicated = snapshot.clone();
    duplicated
        .reservations
        .push(snapshot.reservations[0].clone());
    duplicated.usage.model_calls = 2;
    for invalid in [missing, forged, duplicated] {
        assert_eq!(
            RunSnapshot::from_json(&serde_json::to_string(&invalid).unwrap())
                .unwrap_err()
                .code,
            ErrorCode::InvalidSnapshot
        );
    }
    assert_eq!(
        RunSnapshot::from_json(&serde_json::to_string(&snapshot).unwrap()).unwrap(),
        snapshot
    );
}

impl wickle::ExecutionTransactions for ControlledStore {
    fn read_execution<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
    ) -> wickle::PortFuture<'a, wickle::ExecutionHistory> {
        self.inner.read_execution(scope, run_id)
    }
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
        command: wickle::ControlCommand,
    ) -> wickle::PortFuture<'a, wickle::ControlReceipt> {
        self.inner.submit_control_command(scope, run_id, command)
    }
    fn begin_segment<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        request: wickle::BeginSegmentRequest,
    ) -> wickle::PortFuture<'a, wickle::BeginSegmentResult> {
        self.inner.begin_segment(scope, request)
    }
}
