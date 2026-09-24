//! Stored tool plans and observable executor effects for tool-round contract tests.

use super::support::{admission, event, id, prepared, scope};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use wickle::*;

pub const OWNED: &str = "11111111-1111-4111-8111-111111111111";
pub const FOREIGN: &str = "22222222-2222-4222-8222-222222222222";
pub fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
pub fn input_registry() -> SystemInputRegistry {
    SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap()
}
fn compiled(name: &str, effect: ToolSideEffect, registry: &SystemInputRegistry) -> CompiledTool {
    SchemaCompiler::new().compile(ToolDescriptor{tool:reference(name),name:id(name),description:format!("{name} records"),input_schema:json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"default":10},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),agent_parameters:vec!["query".into(),"limit".into()],system_bindings:None,output_schema:json!({"type":"string"}),side_effect:effect,concurrency:ToolConcurrency::Serial,retry:ToolRetryPolicy::Never,reconcile:false,max_output_bytes:4096.try_into().unwrap()},registry).unwrap()
}
struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id("1")),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("fixture")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| reference.id.clone()),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}
pub struct ClockSource {
    origin: tokio::time::Instant,
}
impl ClockSource {
    fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
        }
    }
}
impl Clock for ClockSource {
    fn now(&self) -> Result<ClockReading, ContractError> {
        let elapsed = self.origin.elapsed().as_millis() as u64;
        Ok(ClockReading {
            utc_ms: elapsed as i64,
            monotonic_ms: elapsed,
        })
    }
    fn sleep_until<'a>(&'a self, deadline: u64) -> PortFuture<'a, ()> {
        Box::pin(async move {
            tokio::time::sleep_until(self.origin + Duration::from_millis(deadline)).await;
            Ok(())
        })
    }
}
#[derive(Default)]
pub struct Ids(AtomicUsize);
impl IdSource for Ids {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id(&format!(
            "tool-record-{}",
            self.0.fetch_add(1, Ordering::SeqCst)
        )))
    }
}
#[derive(Default)]
pub struct Policy {
    pub denied: Mutex<Option<Id>>,
    pub approval: Mutex<Option<Id>>,
    pub calls: Mutex<Vec<ToolPolicyInput>>,
    pub reject_foreign: AtomicUsize,
    pub revoke_on_check: AtomicUsize,
    pub approve_on_check: AtomicUsize,
    pub store: Option<Arc<MemoryStateStore>>,
    pub before_approval: Mutex<Option<ToolLedgerEntry>>,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::ExecuteTool { input } | PolicyAction::ReconcileTool { input, .. } =
                &request.action
            {
                let count = {
                    let mut calls = self.calls.lock().unwrap();
                    calls.push(input.clone());
                    calls.len()
                };
                if self.denied.lock().unwrap().as_ref() == Some(&input.call_id)
                    || (self.reject_foreign.load(Ordering::SeqCst) > 0
                        && input.execution_args().get("workspace_id") != Some(&json!(OWNED)))
                    || (self.revoke_on_check.load(Ordering::SeqCst) > 0
                        && count >= self.revoke_on_check.load(Ordering::SeqCst))
                {
                    return Ok(PolicyDecision::Deny {
                        reason: id("not_owned_or_revoked"),
                    });
                }
                if self.approve_on_check.load(Ordering::SeqCst) > 0
                    && count >= self.approve_on_check.load(Ordering::SeqCst)
                {
                    let saved = self
                        .store
                        .as_ref()
                        .unwrap()
                        .load(&request.owner_scope, &request.resource_id)
                        .await?;
                    *self.before_approval.lock().unwrap() = Some(
                        saved
                            .snapshot
                            .tool_ledger
                            .into_iter()
                            .find(|entry| entry.call.call_id == input.call_id)
                            .unwrap(),
                    );
                    return Ok(PolicyDecision::RequireApproval {
                        reason: id("late_review"),
                    });
                }
                if self.approval.lock().unwrap().as_ref() == Some(&input.call_id) {
                    return Ok(PolicyDecision::RequireApproval {
                        reason: id("review_required"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}
#[derive(Clone, Copy)]
pub enum Action {
    Success,
    WrongOutput,
    Pending,
    Panic,
    Error,
    DeclaredFailure,
    DeclaredUnknown,
    Cancel,
}
#[derive(Debug, Clone, PartialEq)]
pub struct Invocation {
    pub call_id: Id,
    pub attempt_id: Id,
    pub idempotency_key: Id,
    pub args: JsonObject,
}
pub struct Executor {
    pub reconciled: Mutex<Vec<Invocation>>,
    pub block_reconciliation: AtomicBool,
    pub reconciliation_entered: Notify,
    pub reconciliation_release: Notify,
    pub action: Action,
    pub side_effect: ToolSideEffect,
    pub calls: AtomicUsize,
    pub applied: AtomicUsize,
    pub observed: Mutex<Vec<Invocation>>,
    pub order: Arc<Mutex<Vec<Id>>>,
    pub store: Arc<MemoryStateStore>,
    pub cancel: Mutex<Option<CancellationToken>>,
    pub entered: Notify,
}
impl ToolExecutor for Executor {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.order.lock().unwrap().push(context.call_id.clone());
            self.observed.lock().unwrap().push(Invocation {
                call_id: context.call_id.clone(),
                attempt_id: context.attempt_id.clone(),
                idempotency_key: context.idempotency_key.clone(),
                args: args.clone(),
            });
            let saved = self.store.load(&scope(), &id("run")).await?;
            let entry = saved
                .snapshot
                .tool_ledger
                .iter()
                .find(|entry| entry.call.call_id == context.call_id)
                .unwrap();
            assert!(
                matches!(&entry.state,ToolCallState::Dispatching{attempt_id,idempotency_key} if attempt_id==&context.attempt_id&&idempotency_key==&context.idempotency_key)
            );
            assert!(entry.call.bound_input_ref.is_some());
            assert_eq!(context.scope, scope());
            if self.side_effect != ToolSideEffect::ReadOnly {
                self.applied.fetch_add(1, Ordering::SeqCst);
            }
            self.entered.notify_one();
            let effect = if self.side_effect == ToolSideEffect::ReadOnly {
                ToolEffect::NotApplied
            } else {
                ToolEffect::Applied
            };
            let receipt = (effect == ToolEffect::Applied)
                .then(|| json!({"effect_id":"external-effect","value":args["query"]}));
            match self.action {
                Action::Success => Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Succeeded {
                        value: json!("observed result"),
                    },
                    effect,
                    receipt,
                }),
                Action::WrongOutput => Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Succeeded { value: json!(42) },
                    effect,
                    receipt,
                }),
                Action::DeclaredFailure => Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Failed {
                        code: id("tool_failed"),
                    },
                    effect,
                    receipt,
                }),
                Action::DeclaredUnknown => Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Failed {
                        code: id("unknown_effect"),
                    },
                    effect: ToolEffect::Unknown,
                    receipt,
                }),
                Action::Pending => std::future::pending().await,
                Action::Panic => panic!("synthetic executor panic after entry"),
                Action::Error => Err(ContractError::new(
                    ErrorCode::InvalidContract,
                    "synthetic.transport",
                )),
                Action::Cancel => {
                    self.cancel.lock().unwrap().as_ref().unwrap().cancel();
                    std::future::pending().await
                }
            }
        })
    }
    fn reconcile<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolReconciliation> {
        Box::pin(async move {
            let query = Invocation {
                call_id: context.call_id.clone(),
                attempt_id: context.attempt_id.clone(),
                idempotency_key: context.idempotency_key.clone(),
                args: args.clone(),
            };
            self.reconciled.lock().unwrap().push(query.clone());
            self.reconciliation_entered.notify_one();
            if self.block_reconciliation.load(Ordering::SeqCst) {
                self.reconciliation_release.notified().await;
            }
            if !self.observed.lock().unwrap().contains(&query) {
                return Ok(ToolReconciliation::Unknown);
            }
            let effect = if self.side_effect == ToolSideEffect::ReadOnly {
                ToolEffect::NotApplied
            } else {
                ToolEffect::Applied
            };
            Ok(ToolReconciliation::Known {
                result: ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Succeeded {
                        value: json!("observed result"),
                    },
                    effect,
                    receipt: (effect == ToolEffect::Applied)
                        .then(|| json!({"effect_id":"external-effect","value":args["query"]})),
                },
            })
        })
    }
}

pub struct Fixture {
    pub store: Arc<MemoryStateStore>,
    pub policy: Arc<Policy>,
    pub clock: Arc<ClockSource>,
    pub ids: Arc<Ids>,
    pub context: ExecutionContext,
    pub registry: Arc<ToolRegistry>,
    pub input_registry: Arc<SystemInputRegistry>,
    pub compiled: Vec<CompiledTool>,
    pub executors: Vec<Arc<Executor>>,
    pub order: Arc<Mutex<Vec<Id>>>,
    pub lease: RunLease,
}
impl Fixture {
    pub async fn new(tools: &[(&str, ToolSideEffect, Action)], system_value: Option<&str>) -> Self {
        Self::build(tools, system_value, false, 1).await
    }
    pub async fn reconcilable(
        tools: &[(&str, ToolSideEffect, Action)],
        system_value: Option<&str>,
    ) -> Self {
        Self::build(tools, system_value, true, 1).await
    }
    pub async fn reconcilable_with_limit(
        tools: &[(&str, ToolSideEffect, Action)],
        system_value: Option<&str>,
        recovery_attempts: u64,
    ) -> Self {
        Self::build(tools, system_value, true, recovery_attempts).await
    }
    async fn build(
        tools: &[(&str, ToolSideEffect, Action)],
        system_value: Option<&str>,
        reconcile: bool,
        recovery_attempts: u64,
    ) -> Self {
        let store = Arc::new(MemoryStateStore::new());
        let input_registry = Arc::new(input_registry());
        let order = Arc::new(Mutex::new(vec![]));
        let mut compiled_tools = vec![];
        let mut executors = vec![];
        let mut registrations = vec![];
        for (name, side_effect, action) in tools {
            let compiled = compiled(name, *side_effect, &input_registry);
            let compiled = if reconcile {
                let mut descriptor = compiled.descriptor().clone();
                descriptor.reconcile = true;
                SchemaCompiler::new()
                    .compile(descriptor, &input_registry)
                    .unwrap()
            } else {
                compiled
            };
            let executor = Arc::new(Executor {
                reconciled: Mutex::new(vec![]),
                block_reconciliation: AtomicBool::new(false),
                reconciliation_entered: Notify::new(),
                reconciliation_release: Notify::new(),
                action: *action,
                side_effect: *side_effect,
                calls: AtomicUsize::new(0),
                applied: AtomicUsize::new(0),
                observed: Mutex::new(vec![]),
                order: order.clone(),
                store: store.clone(),
                cancel: Mutex::new(None),
                entered: Notify::new(),
            });
            registrations.push(ToolRegistration {
                compiled: compiled.clone(),
                executor: executor.clone(),
            });
            compiled_tools.push(compiled);
            executors.push(executor);
        }
        let registry = Arc::new(ToolRegistry::new(scope(), registrations).unwrap());
        let supplied =
            system_value.map(|value| SystemInputs::new(object(json!({"workspace_id":value}))));
        let fixed = RunSystemInputs::capture(scope(), supplied.clone(), &input_registry).unwrap();
        let record = fixed.to_record(id("run-inputs"), 7);
        let input_ref = fixed.snapshot_ref(record.reference()).unwrap();
        let mut input = admission("run", "request", "session", "Read evidence", "1").await;
        let mut profile = input.snapshot.profile.profile().clone();
        profile.tools = compiled_tools
            .iter()
            .map(|tool| {
                ToolBindingRef::Catalog(CatalogToolRef {
                    tool_id: tool.descriptor().tool.id.clone(),
                    version: tool.descriptor().tool.version.clone(),
                    bindings: None,
                    config: None,
                })
            })
            .collect();
        profile.limits.max_tool_attempts = 8;
        profile.limits.max_recovery_attempts = recovery_attempts;
        input.snapshot.profile = ProfileValidator::new(&Catalog)
            .validate(&profile, &scope())
            .await
            .unwrap();
        input.snapshot.limits = profile.limits.clone();
        input.snapshot.system_inputs = Some(input_ref);
        input.snapshot.request_digest = admission_digest(
            &input.snapshot.request,
            &input.snapshot.profile,
            input.snapshot.system_inputs.as_ref(),
        );
        let RunEventPayload::RunStarted { profile_digest, .. } = &mut input.events[0].payload
        else {
            unreachable!()
        };
        *profile_digest = input.snapshot.profile.profile_digest().clone();
        input.records.push(record);
        store.admit(&scope(), input).await.unwrap();
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("worker"), 0, 20000)
            .await
            .unwrap();
        let context = ExecutionContext::new(
            ExecutionContextData {
                scope: scope(),
                principal_ref: id("caller"),
                capability_grant_ref: id("grant"),
                trace_context: None,
                system_inputs: supplied,
            },
            Default::default(),
        );
        Self {
            policy: Arc::new(Policy {
                store: Some(store.clone()),
                ..Policy::default()
            }),
            store,
            clock: Arc::new(ClockSource::new()),
            ids: Arc::new(Ids::default()),
            context,
            registry,
            input_registry,
            compiled: compiled_tools,
            executors,
            order,
            lease,
        }
    }
    pub async fn plan(&self, calls: &[(&str, &str, JsonObject)]) {
        let saved = self.store.load(&scope(), &id("run")).await.unwrap();
        let mut update = prepared(
            &saved.snapshot,
            self.lease.clone(),
            self.clock.now().unwrap().utc_ms,
        );
        update.snapshot.phase = RunPhase::Tool;
        let mut content = vec![];
        for (call_id, name, args) in calls {
            let descriptor_digest = self
                .compiled
                .iter()
                .find(|tool| tool.descriptor().name.as_str() == *name)
                .map(|tool| tool.descriptor_digest().clone());
            let call = ToolCall {
                provider_arguments: None,
                call_id: id(call_id),
                model_request_id: id("model-request"),
                provider_call_id: id(&format!("provider-{call_id}")),
                tool_name: id(name),
                model_inputs: args.clone(),
                descriptor_digest,
                bound_input_ref: None,
            };
            let record = ProtectedRecord::new(
                id(&format!("plan-{call_id}")),
                1,
                serde_json::to_value(&call).unwrap(),
            );
            update.snapshot.tool_ledger.push(ToolLedgerEntry {
                call: call.clone(),
                state: ToolCallState::Planned {},
            });
            update.snapshot.last_event_seq += 1;
            update.events.push(event(
                &id("run"),
                &id("session"),
                &scope(),
                update.snapshot.last_event_seq,
                RunEventPayload::ToolPlanned {
                    call_ref: record.reference().clone(),
                },
            ));
            update.records.push(record);
            content.push(ContentBlock::ToolCall { call });
        }
        update.messages.push(Message {
            source_model_request_id: None,
            message_id: id("call-message"),
            run_id: id("run"),
            sequence: (saved.session.transcript_revision + 1).try_into().unwrap(),
            role: MessageRole::Assistant,
            origin: MessageOrigin::Model,
            visibility: Visibility::UserAndModel,
            content,
        });
        self.store
            .commit(&scope(), &id("run"), update)
            .await
            .unwrap();
    }
    pub async fn budget(&self, store: Arc<dyn StateStore>) -> RunBudget {
        RunBudget::attach(
            store,
            self.clock.clone(),
            self.ids.clone(),
            scope(),
            id("run"),
            self.lease.clone(),
            self.context.cancellation.clone(),
        )
        .await
        .unwrap()
    }
    pub fn round(&self) -> SerialToolRound {
        let policy =
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap());
        let binder = Arc::new(InputBinder::new(
            self.input_registry.clone(),
            None,
            policy.clone(),
            self.ids.clone(),
        ));
        SerialToolRound::new(self.registry.clone(), binder, policy, self.ids.clone())
    }
    pub async fn execute(&self) -> Result<ToolRoundOutcome, ContractError> {
        self.round()
            .execute(
                &id("model-request"),
                &self.context,
                &self.budget(self.store.clone()).await,
            )
            .await
    }
    pub async fn saved(&self) -> StoredRun {
        self.store.load(&scope(), &id("run")).await.unwrap()
    }
}

#[derive(Clone, Copy)]
pub enum FailStage {
    Binding,
    Reservation,
    Dispatch,
    Result,
    Correction,
}
pub struct FaultStore {
    pub inner: Arc<MemoryStateStore>,
    pub stage: FailStage,
    pub lose_ack: bool,
    pub failures: AtomicUsize,
}
impl StateStore for FaultStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        s: &'a Scope,
        session: &'a Id,
        request: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        self.inner.find_request(s, session, request)
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
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.check_lease(s, r, l, n)
    }
    fn acquire_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        o: &'a Id,
        n: i64,
        t: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.acquire_lease(s, r, o, n, t)
    }
    fn renew_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
        t: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.renew_lease(s, r, l, n, t)
    }
    fn release_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, ()> {
        self.inner.release_lease(s, r, l, n)
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
            let previous = self.inner.load(s, r).await?.snapshot;
            let should_fail = match self.stage {
                FailStage::Binding => {
                    previous.tool_ledger[0].call.bound_input_ref.is_none()
                        && input.snapshot.tool_ledger[0].call.bound_input_ref.is_some()
                }
                FailStage::Reservation => {
                    input.snapshot.usage.tool_attempts > previous.usage.tool_attempts
                }
                FailStage::Dispatch => {
                    matches!(
                        input.snapshot.tool_ledger[0].state,
                        ToolCallState::Dispatching { .. }
                    ) && !matches!(
                        previous.tool_ledger[0].state,
                        ToolCallState::Dispatching { .. }
                    )
                }
                FailStage::Correction => input
                    .events
                    .iter()
                    .any(|event| matches!(event.payload, RunEventPayload::ToolReconciled { .. })),
                FailStage::Result => {
                    matches!(
                        input.snapshot.tool_ledger[0].state,
                        ToolCallState::Settled { .. } | ToolCallState::Unknown { .. }
                    ) && matches!(
                        previous.tool_ledger[0].state,
                        ToolCallState::Dispatching { .. }
                    )
                }
            };
            if should_fail {
                self.failures.fetch_add(1, Ordering::SeqCst);
                if self.lose_ack {
                    self.inner.commit(s, r, input).await?;
                }
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "tool.transaction",
                ));
            }
            self.inner.commit(s, r, input).await
        })
    }
}

impl wickle::ExecutionTransactions for FaultStore {
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
