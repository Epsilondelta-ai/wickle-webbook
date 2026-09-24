use super::*;

/// Metadata from the latest successfully observed checkpoint, not a terminal result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistenceFailure {
    /// Run whose authoritative storage became unavailable.
    pub run_id: Id,
    /// Latest revision confirmed by a successful store operation in this Agent.
    pub last_confirmed_revision: u64,
    /// Dispatched effects whose results were not confirmed at that revision.
    pub unconfirmed_effects: Vec<UnconfirmedToolEffect>,
}
/// An uncertain call identity; this does not contain its system inputs or output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnconfirmedToolEffect {
    /// Stable planned call.
    pub call_id: Id,
    /// Charged physical execution attempt.
    pub attempt_id: Id,
    /// Original external effect key.
    pub idempotency_key: Id,
    /// Protected frozen argument record, when present.
    pub bound_input_ref: Option<RecordRef>,
}
#[derive(Default)]
pub(super) struct ObservedState(Mutex<BTreeMap<Id, PersistenceFailure>>);
impl ObservedState {
    fn observe(&self, snapshot: &RunSnapshot) {
        let notice = PersistenceFailure {
            run_id: snapshot.run_id.clone(),
            last_confirmed_revision: snapshot.revision,
            unconfirmed_effects: snapshot
                .tool_ledger
                .iter()
                .filter_map(|entry| {
                    let (attempt_id, idempotency_key) = match &entry.state {
                        ToolCallState::Dispatching {
                            attempt_id,
                            idempotency_key,
                        }
                        | ToolCallState::Unknown {
                            attempt_id,
                            idempotency_key,
                        } => (attempt_id, idempotency_key),
                        _ => return None,
                    };
                    Some(UnconfirmedToolEffect {
                        call_id: entry.call.call_id.clone(),
                        attempt_id: attempt_id.clone(),
                        idempotency_key: idempotency_key.clone(),
                        bound_input_ref: entry.call.bound_input_ref.clone(),
                    })
                })
                .collect(),
        };
        if let Ok(mut saved) = self.0.lock() {
            if saved
                .get(&snapshot.run_id)
                .is_none_or(|previous| previous.last_confirmed_revision <= snapshot.revision)
            {
                saved.insert(snapshot.run_id.clone(), notice);
            }
        }
    }
    pub(super) fn attach(&self, run_id: &Id, mut error: ContractError) -> ContractError {
        if error.code == ErrorCode::PersistenceUnavailable {
            error.persistence = self
                .0
                .lock()
                .ok()
                .and_then(|saved| saved.get(run_id).cloned())
                .map(Box::new);
        }
        error
    }
}
pub(super) struct ObservedStore {
    pub inner: Arc<dyn StateStore>,
    pub observed: Arc<ObservedState>,
}
impl StateStore for ObservedStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
        request_id: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        Box::pin(async move {
            let result = self
                .inner
                .find_request(scope, session_id, request_id)
                .await?;
            if let Some(saved) = &result {
                self.observed.observe(&saved.snapshot);
            }
            Ok(result)
        })
    }
    fn admit<'a>(
        &'a self,
        scope: &'a Scope,
        input: AdmissionInput,
    ) -> PortFuture<'a, AdmissionResult> {
        Box::pin(async move {
            let result = self.inner.admit(scope, input).await?;
            self.observed.observe(&result.state.snapshot);
            Ok(result)
        })
    }
    fn load<'a>(&'a self, scope: &'a Scope, run_id: &'a Id) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            let result = self.inner.load(scope, run_id).await?;
            self.observed.observe(&result.snapshot);
            Ok(result)
        })
    }
    fn load_session<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
    ) -> PortFuture<'a, SessionSnapshot> {
        self.inner.load_session(scope, session_id)
    }
    fn check_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.check_lease(scope, run_id, lease, now_ms)
    }
    fn acquire_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        owner: &'a Id,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner
            .acquire_lease(scope, run_id, owner, now_ms, ttl_ms)
    }
    fn renew_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.renew_lease(scope, run_id, lease, now_ms, ttl_ms)
    }
    fn release_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, ()> {
        self.inner.release_lease(scope, run_id, lease, now_ms)
    }
    fn commit<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            let result = self.inner.commit(scope, run_id, input).await?;
            self.observed.observe(&result.snapshot);
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
        self.inner.read_events(scope, run_id, after_seq, limit)
    }
    fn read_record<'a>(
        &'a self,
        scope: &'a Scope,
        reference: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        self.inner.read_record(scope, reference)
    }
    fn record_hook_observation<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        report: HookObservation,
    ) -> PortFuture<'a, ()> {
        self.inner.record_hook_observation(scope, run_id, report)
    }
    fn read_hook_observations<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
    ) -> PortFuture<'a, Vec<HookObservation>> {
        self.inner.read_hook_observations(scope, run_id)
    }
}

impl crate::ExecutionTransactions for ObservedStore {
    fn read_execution<'a>(
        &'a self,
        scope: &'a crate::Scope,
        run_id: &'a crate::Id,
    ) -> crate::PortFuture<'a, crate::ExecutionHistory> {
        self.inner.read_execution(scope, run_id)
    }
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a crate::Scope,
        run_id: &'a crate::Id,
        command: crate::ControlCommand,
    ) -> crate::PortFuture<'a, crate::ControlReceipt> {
        self.inner.submit_control_command(scope, run_id, command)
    }
    fn begin_segment<'a>(
        &'a self,
        scope: &'a crate::Scope,
        request: crate::BeginSegmentRequest,
    ) -> crate::PortFuture<'a, crate::BeginSegmentResult> {
        Box::pin(async move {
            let result = self.inner.begin_segment(scope, request).await?;
            self.observed.observe(&result.state.snapshot);
            Ok(result)
        })
    }
}
