//! Process termination at durable state boundaries, without running destructors.
use std::sync::Arc;
use wickle::*;
use wickle_state_sqlite::SqliteStateStore;
pub struct CrashStore {
    pub inner: Arc<SqliteStateStore>,
    pub boundary: String,
    pub kill_marker: Option<std::path::PathBuf>,
}
impl CrashStore {
    async fn stop(&self, code: i32) -> ! {
        if let Some(marker) = &self.kill_marker {
            std::fs::write(marker, b"ready\n").unwrap();
            std::future::pending::<()>().await;
        }
        std::process::exit(code)
    }
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
        Box::pin(async move {
            let result = self.inner.admit(scope, input).await?;
            if self.boundary == "admitted" {
                self.stop(75).await;
            }
            Ok(result)
        })
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
            let prepared =
                !input.snapshot.prepared_steps.is_empty() && input.snapshot.model_ledger.is_empty();
            if self.boundary == "before-prepared" && prepared {
                self.stop(75).await;
            }
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
                "wait" => input.snapshot.status == RunStatus::Waiting,
                "prepared" => prepared,
                "reserved" => input
                    .snapshot
                    .model_ledger
                    .iter()
                    .any(|entry| matches!(entry.state, ModelAttemptState::Reserved {})),
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
                self.stop(if self.boundary == "context" { 76 } else { 75 })
                    .await;
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

impl wickle::ExecutionTransactions for CrashStore {
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
        Box::pin(async move {
            let answer = matches!(&request.start, SegmentStart::Resume(command) if matches!(command.action, ResumeAction::Input { .. }));
            if answer && self.boundary == "before-command" {
                self.stop(75).await;
            }
            let result = self.inner.begin_segment(scope, request).await?;
            if answer && self.boundary == "accepted-command" {
                self.stop(75).await;
            }
            Ok(result)
        })
    }
}
