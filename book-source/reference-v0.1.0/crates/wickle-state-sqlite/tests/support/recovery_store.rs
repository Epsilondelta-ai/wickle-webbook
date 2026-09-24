//! Process termination at durable state boundaries, without running destructors.
use std::sync::Arc;
use wickle::*;
use wickle_state_sqlite::SqliteStateStore;
pub struct CrashStore {
    pub inner: Arc<SqliteStateStore>,
    pub boundary: String,
}
impl StateStore for CrashStore {
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
    fn admit<'a>(
        &'a self,
        scope: &'a Scope,
        input: AdmissionInput,
    ) -> PortFuture<'a, AdmissionResult> {
        self.inner.admit(scope, input)
    }
    fn load<'a>(&'a self, scope: &'a Scope, run_id: &'a Id) -> PortFuture<'a, StoredRun> {
        self.inner.load(scope, run_id)
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
            let planned = input
                .events
                .iter()
                .any(|event| matches!(event.payload, RunEventPayload::ToolPlanned { .. }));
            if self.boundary == "before-plan" && planned {
                std::process::exit(75);
            }
            let rewritten = input
                .events
                .iter()
                .any(|event| matches!(event.payload, RunEventPayload::ContextRewritten { .. }));
            if self.boundary == "before-context" && rewritten {
                std::process::exit(76);
            }
            let stop = match self.boundary.as_str() {
                "context" => rewritten,
                "plan" => planned,
                "bound" => input.snapshot.tool_ledger.iter().any(|entry| {
                    entry.call.tool_name.as_str() == "target"
                        && entry.call.bound_input_ref.is_some()
                        && matches!(entry.state, ToolCallState::Planned {})
                }),
                "settled" => input.snapshot.tool_ledger.iter().any(|entry| {
                    entry.call.tool_name.as_str() == "target"
                        && matches!(entry.state, ToolCallState::Settled { .. })
                }),
                "terminal" => input.snapshot.status.is_terminal(),
                _ => false,
            };
            let result = self.inner.commit(scope, run_id, input).await?;
            if stop {
                std::process::exit(if self.boundary == "context" { 76 } else { 75 });
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
