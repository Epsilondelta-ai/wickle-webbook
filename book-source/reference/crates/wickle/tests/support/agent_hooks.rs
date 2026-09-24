//! Hook fixtures observe actual executor, policy, model, and persistence boundaries.

use super::resume_support;
use futures_util::stream;
pub use resume_support::{
    WORKSPACE, completed, context, gate, id, object, reference, request, scope,
};
use serde_json::json;
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};
use wickle::*;

pub struct Catalog {
    pub definitions: Vec<HookDefinition>,
}
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(reference.version.clone().unwrap_or_else(|| id("1"))),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("hook-fixture")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| reference.id.clone()),
                hook_position: self
                    .definitions
                    .iter()
                    .find(|definition| definition.hook.id == reference.id)
                    .map(|definition| definition.position),
                exports: vec![],
            })
        })
    }
}

pub struct Policy {
    pub approval: AtomicBool,
    pub deny: AtomicBool,
    pub fail: AtomicBool,
    pub deny_hooks: AtomicBool,
    pub seen: Mutex<Vec<ToolPolicyInput>>,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if matches!(request.action, PolicyAction::InvokeHook { .. })
                && self.deny_hooks.load(Ordering::SeqCst)
            {
                return Ok(PolicyDecision::Deny {
                    reason: id("hook-revoked"),
                });
            }
            if let PolicyAction::ExecuteTool { input } = &request.action {
                self.seen.lock().unwrap().push(input.clone());
                if self.fail.load(Ordering::SeqCst) {
                    return Err(ContractError::new(
                        ErrorCode::PolicyUnavailable,
                        "fixture.policy",
                    ));
                }
                if self.deny.load(Ordering::SeqCst) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("host-denied"),
                    });
                }
                if self.approval.load(Ordering::SeqCst)
                    && input.tool.id == id("target")
                    && input.approval().is_none()
                {
                    return Ok(PolicyDecision::RequireApproval {
                        reason: id("target-approval"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}

pub struct RetryModel {
    pub inner: Arc<resume_support::Model>,
    pub attempts: AtomicUsize,
    pub fail_first: AtomicBool,
}
impl ModelPort for RetryModel {
    fn binding(&self) -> ModelPortBinding {
        self.inner.binding()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 && self.fail_first.load(Ordering::SeqCst) {
            Box::pin(stream::iter([Ok(ModelEvent::ResponseError {
                kind: ModelFailureKind::Transport,
                metadata: ModelResponseMetadata::default(),
            })]))
        } else {
            self.inner.generate(request, context)
        }
    }
}

pub struct Fixture {
    pub base: resume_support::Fixture,
    pub policy: Arc<Policy>,
    pub model: Arc<RetryModel>,
    pub hooks: Vec<Arc<Hook>>,
    pub order: Arc<Mutex<Vec<String>>>,
    pub store: Arc<FaultStore>,
}
impl Fixture {
    pub fn new() -> Self {
        let base = resume_support::Fixture::new(resume_support::Mode::Approval);
        let model = Arc::new(RetryModel {
            inner: base.model.clone(),
            attempts: AtomicUsize::new(0),
            fail_first: AtomicBool::new(false),
        });
        let store = Arc::new(FaultStore {
            inner: base.base.store.clone(),
            transform_failure: AtomicUsize::new(0),
            observer_failure: AtomicBool::new(false),
            transform_writes: AtomicUsize::new(0),
        });
        Self {
            base,
            policy: Arc::new(Policy {
                approval: AtomicBool::new(false),
                deny: AtomicBool::new(false),
                fail: AtomicBool::new(false),
                deny_hooks: AtomicBool::new(false),
                seen: Mutex::new(vec![]),
            }),
            model,
            hooks: vec![],
            order: Arc::new(Mutex::new(vec![])),
            store,
        }
    }
    pub fn add(
        &mut self,
        name: &str,
        position: HookPosition,
        behavior: Behavior,
        priority: i32,
        required: bool,
    ) -> Arc<Hook> {
        let hook = Arc::new(Hook {
            definition: HookDefinition {
                hook: reference(name),
                position,
                priority,
                required,
                timeout_ms: if matches!(behavior, Behavior::Pending) {
                    20
                } else {
                    5000
                },
                max_output_bytes: 4096,
            },
            behavior,
            suffix: Mutex::new(name.to_owned()),
            calls: AtomicUsize::new(0),
            order: self.order.clone(),
            store: self.base.base.store.clone(),
            entered: Notify::new(),
            release: Semaphore::new(0),
            seen: Mutex::new(vec![]),
            entered_at: Mutex::new(vec![]),
        });
        self.hooks.push(hook.clone());
        hook
    }
    pub fn bindings(&self) -> AgentBindings {
        let mut bindings = self.base.bindings();
        bindings.state = self.store.clone();
        let policy =
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(5)).unwrap());
        // Successful hook chains exercise several persisted boundaries. Short
        // timing limits belong to the explicit paused-clock deadline tests.
        bindings.settings.lease_ttl_ms = 30_000;
        bindings.settings.heartbeat_interval_ms = 5_000;
        bindings.policy = policy.clone();
        bindings.profile_resolver = Arc::new(Catalog {
            definitions: self
                .hooks
                .iter()
                .map(|hook| hook.definition.clone())
                .collect(),
        });
        bindings.model_exchange = Arc::new(
            ModelExchange::new(self.model.clone(), policy)
                .with_route_inspector(self.base.base.inspector.clone(), Duration::from_secs(1))
                .unwrap()
                .with_retry_policy(ModelRetryPolicy {
                    max_retries: 1,
                    backoff_ms: 0,
                }),
        );
        let registry = Arc::new(
            HookRegistry::new(
                scope(),
                self.hooks
                    .iter()
                    .map(|hook| HookRegistration {
                        definition: hook.definition.clone(),
                        handler: hook.clone(),
                    })
                    .collect(),
            )
            .unwrap(),
        );
        bindings.hooks = Some(Arc::new(HookRuntime::new(
            bindings.state.clone(),
            bindings.policy.clone(),
            bindings.clock.clone(),
            bindings.ids.clone(),
            registry,
        )));
        bindings
    }
    pub fn profile(&self) -> AgentProfile {
        let mut profile = self.base.profile.clone();
        profile.limits.max_recovery_attempts = 1;
        profile.hooks = Some(
            self.hooks
                .iter()
                .map(|hook| {
                    HookRef::Catalog(CatalogHookRef {
                        hook_id: hook.definition.hook.id.clone(),
                        version: hook.definition.hook.version.clone(),
                        position: hook.definition.position,
                    })
                })
                .collect(),
        );
        profile
    }
    pub fn agent(&self) -> Agent {
        create_agent(self.profile(), self.bindings()).unwrap()
    }
    pub async fn started(&self, agent: &Agent) -> RunHandle {
        self.base.started(agent).await
    }
    pub async fn outcome(&self, handle: &RunHandle) -> RunOutcome {
        self.base.outcome(handle).await
    }
    pub async fn saved(&self, handle: &RunHandle) -> StoredRun {
        self.base.saved(handle).await
    }
}

#[derive(Clone, Copy)]
pub enum Behavior {
    Context,
    Append,
    RemoveQuery,
    Hidden,
    InvalidValue,
    Deny,
    Error,
    Pending,
    Panic,
    Observe,
    Pause,
    WrongVariant,
    Oversized,
}
pub struct Hook {
    pub definition: HookDefinition,
    pub behavior: Behavior,
    pub suffix: Mutex<String>,
    pub calls: AtomicUsize,
    pub order: Arc<Mutex<Vec<String>>>,
    pub store: Arc<MemoryStateStore>,
    pub entered: Notify,
    pub release: Semaphore,
    pub seen: Mutex<Vec<(HookInput, HookContext)>>,
    pub entered_at: Mutex<Vec<tokio::time::Instant>>,
}
impl HookHandler for Hook {
    fn call<'a>(
        &'a self,
        input: &'a HookInput,
        context: &'a HookContext,
    ) -> PortFuture<'a, HookOutput> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered_at
                .lock()
                .unwrap()
                .push(tokio::time::Instant::now());
            self.order.lock().unwrap().push(context.hook.id.to_string());
            self.seen
                .lock()
                .unwrap()
                .push((input.clone(), context.clone()));
            assert_eq!(context.scope, scope());
            assert_eq!(context.hook, self.definition.hook);
            assert_eq!(context.target.position(), self.definition.position);
            match &context.target {
                HookTarget::AfterTool {
                    call_id,
                    result_ref,
                } => {
                    let stored = self.store.load(&scope(), &context.run_id).await?;
                    let result: ToolResult = serde_json::from_value(
                        self.store
                            .read_record(&scope(), result_ref)
                            .await?
                            .value()
                            .clone(),
                    )
                    .unwrap();
                    let entry = stored
                        .snapshot
                        .tool_ledger
                        .iter()
                        .find(|entry| &entry.call.call_id == call_id)
                        .unwrap();
                    assert!(
                        matches!(&entry.state, ToolCallState::Settled { result: saved } if saved == &result)
                    );
                    let HookInput::AfterTool {
                        status,
                        effect,
                        content,
                        error_code,
                        ..
                    } = input
                    else {
                        panic!("observer input mismatch")
                    };
                    assert_eq!(
                        (*status, *effect, content, error_code),
                        (
                            result.status,
                            result.effect,
                            &result.content,
                            &result.error.as_ref().map(|error| error.code.clone())
                        )
                    );
                }
                HookTarget::AfterRun {
                    outcome_ref,
                    revision,
                } => {
                    let stored = self.store.load(&scope(), &context.run_id).await?;
                    assert!(stored.snapshot.status.is_terminal());
                    assert_eq!(stored.snapshot.revision, *revision);
                    let value = self.store.read_record(&scope(), outcome_ref).await?;
                    assert_eq!(
                        serde_json::from_value::<RunOutcome>(value.value().clone()).unwrap(),
                        stored.snapshot.outcome.unwrap()
                    );
                }
                _ => {}
            }
            self.entered.notify_one();
            match self.behavior {
                Behavior::Error => {
                    return Err(ContractError::new(
                        ErrorCode::InvalidContract,
                        "raw-hook-diagnostic",
                    ));
                }
                Behavior::Pending => return std::future::pending().await,
                Behavior::Panic => panic!("synthetic hook panic"),
                Behavior::Pause => {
                    self.release.acquire().await.unwrap().forget();
                }
                Behavior::WrongVariant => return Ok(HookOutput::Observed {}),
                _ => {}
            }
            match input {
                HookInput::BeforeRun { .. } | HookInput::BeforeModel { .. } => {
                    let value = if matches!(self.behavior, Behavior::Oversized) {
                        json!("x".repeat(5000))
                    } else {
                        json!({"marker":self.suffix.lock().unwrap().clone(),"target":context.target})
                    };
                    Ok(HookOutput::Context {
                        additions: vec![HookContextAddition {
                            content: vec![InputContent::Json { value }],
                            priority: ContextPriority::Required,
                        }],
                    })
                }
                HookInput::BeforeTool {
                    tool,
                    original_model_inputs,
                    model_inputs,
                    ..
                } => {
                    assert_eq!(
                        tool.model_input_schema["properties"]
                            .as_object()
                            .unwrap()
                            .keys()
                            .cloned()
                            .collect::<Vec<_>>(),
                        vec!["query".to_string()]
                    );
                    assert!(original_model_inputs.get("workspace_id").is_none());
                    let mut model_inputs = model_inputs.clone();
                    match self.behavior {
                        Behavior::Append => {
                            let query = model_inputs["query"].as_str().unwrap();
                            model_inputs.insert(
                                "query".into(),
                                json!(format!("{query}|{}", self.suffix.lock().unwrap())),
                            );
                        }
                        Behavior::RemoveQuery => {
                            model_inputs.remove("query");
                        }
                        Behavior::Hidden => {
                            model_inputs.insert("workspace_id".into(), json!(WORKSPACE));
                        }
                        Behavior::InvalidValue => {
                            model_inputs.insert("query".into(), json!(42));
                        }
                        _ => {}
                    }
                    Ok(HookOutput::Tool {
                        model_inputs,
                        deny: matches!(self.behavior, Behavior::Deny).then(|| id("hook-denied")),
                    })
                }
                HookInput::AfterTool { .. } | HookInput::AfterRun { .. } => {
                    Ok(HookOutput::Observed {})
                }
            }
        })
    }
}

pub async fn observations(handle: &RunHandle, expected: usize) -> HookObservationView {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let view = completed(handle.hook_observations(&context()).await.unwrap());
            if view.local_error.is_some() || view.reports.len() >= expected {
                return view;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("observer reports must finish")
}

pub struct FaultStore {
    pub inner: Arc<MemoryStateStore>,
    pub transform_failure: AtomicUsize,
    pub observer_failure: AtomicBool,
    pub transform_writes: AtomicUsize,
}
impl StateStore for FaultStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        s: &'a Scope,
        session: &'a Id,
        r: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        self.inner.find_request(s, session, r)
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
            let current = self.inner.load(s, r).await?;
            if input.snapshot.hook_applications.len() > current.snapshot.hook_applications.len() {
                self.transform_writes.fetch_add(1, Ordering::SeqCst);
                match self.transform_failure.swap(0, Ordering::SeqCst) {
                    1 => {
                        return Err(ContractError::new(
                            ErrorCode::PersistenceUnavailable,
                            "hook.commit",
                        ));
                    }
                    2 => {
                        self.inner.commit(s, r, input).await?;
                        return Err(ContractError::new(
                            ErrorCode::PersistenceUnavailable,
                            "hook.ack",
                        ));
                    }
                    _ => {}
                }
            }
            self.inner.commit(s, r, input).await
        })
    }
    fn record_hook_observation<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        report: HookObservation,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            if self.observer_failure.load(Ordering::SeqCst) {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "hook.observer_report",
                ));
            }
            self.inner.record_hook_observation(s, r, report).await
        })
    }
    fn read_hook_observations<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
    ) -> PortFuture<'a, Vec<HookObservation>> {
        self.inner.read_hook_observations(s, r)
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
