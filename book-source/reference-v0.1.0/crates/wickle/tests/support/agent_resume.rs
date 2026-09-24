//! Observable model, policy, resolver, and tool behavior across saved execution segments.

use super::agent_support;
pub use agent_support::{completed, context, id, reference, request, scope};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
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

pub const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";
pub const RECORD: &str = "22222222-2222-4222-8222-222222222222";
pub const CHANGED_RECORD: &str = "33333333-3333-4333-8333-333333333333";
pub fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
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
                manifest_digest: canonical_digest(&json!("resume-tools")),
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

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Approval,
    LateApproval,
    Input,
    External,
}

pub struct Policy {
    pub mode: Mode,
    pub deny_resume: AtomicBool,
    pub deny_execute: AtomicBool,
    pub deny_cancel: AtomicBool,
    pub deny_receipt_read: AtomicBool,
    pub deny_details: AtomicBool,
    pub details_checks: AtomicUsize,
    pub require_resume_approval: AtomicBool,
    pub target_checks: AtomicUsize,
    pub resume_checks: Mutex<Vec<(ResumeCommand, Id)>>,
    pub tool_checks: Mutex<Vec<(ToolPolicyInput, Id)>>,
    pub pause_next_resume: AtomicBool,
    pub resume_entered: Notify,
    pub resume_release: Semaphore,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if matches!(request.action, PolicyAction::ReadRunDetails {}) {
                self.details_checks.fetch_add(1, Ordering::SeqCst);
                if self.deny_details.load(Ordering::SeqCst) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("details_revoked"),
                    });
                }
            }
            if let PolicyAction::ResumeRun { command, .. } = &request.action {
                self.resume_checks
                    .lock()
                    .unwrap()
                    .push(((**command).clone(), context.principal_ref.clone()));
                if self.pause_next_resume.swap(false, Ordering::SeqCst) {
                    self.resume_entered.notify_one();
                    self.resume_release.acquire().await.unwrap().forget();
                }
                if self.deny_resume.load(Ordering::SeqCst) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("resume_revoked"),
                    });
                }
                if self.require_resume_approval.load(Ordering::SeqCst) {
                    return Ok(PolicyDecision::RequireApproval {
                        reason: id("resume_review"),
                    });
                }
            }
            if matches!(request.action, PolicyAction::CancelRun {})
                && self.deny_cancel.load(Ordering::SeqCst)
            {
                return Ok(PolicyDecision::Deny {
                    reason: id("cancel_revoked"),
                });
            }
            if matches!(request.action, PolicyAction::ReadRecord { .. })
                && self.deny_receipt_read.load(Ordering::SeqCst)
            {
                return Ok(PolicyDecision::Deny {
                    reason: id("receipt_revoked"),
                });
            }
            if let PolicyAction::ExecuteTool { input } = &request.action {
                self.tool_checks
                    .lock()
                    .unwrap()
                    .push((input.clone(), context.principal_ref.clone()));
                if input.tool.id == id("target") {
                    let check = self.target_checks.fetch_add(1, Ordering::SeqCst) + 1;
                    if self.deny_execute.load(Ordering::SeqCst)
                        || input.execution_args().get("workspace_id") != Some(&json!(WORKSPACE))
                    {
                        return Ok(PolicyDecision::Deny {
                            reason: id("target_revoked"),
                        });
                    }
                    if input.approval().is_none()
                        && (self.mode == Mode::Approval
                            || (self.mode == Mode::LateApproval && check >= 3))
                    {
                        return Ok(PolicyDecision::RequireApproval {
                            reason: id("target_review"),
                        });
                    }
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}

pub struct Resolver {
    pub calls: AtomicUsize,
    pub value: Mutex<ResolvedSystemInput>,
}
impl SystemInputResolver for Resolver {
    fn resolve<'a>(
        &'a self,
        request: &'a SystemInputResolveRequest,
        context: &'a SystemInputResolveContext,
    ) -> PortFuture<'a, Option<ResolvedSystemInput>> {
        Box::pin(async move {
            assert_eq!(request.key, id("record_id"));
            assert_eq!(context.scope, scope());
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(self.value.lock().unwrap().clone()))
        })
    }
}

pub struct Model {
    pub calls: AtomicUsize,
    pub requests: Mutex<Vec<ModelRequest>>,
    pub hold_final: AtomicBool,
    pub final_entered: Notify,
    pub final_release: Semaphore,
}
impl ModelPort for Model {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: id("fixture"),
            adapter: reference("adapter"),
            connection_ref: reference("connection"),
        }
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        let completion = |finish| {
            Ok(ModelEvent::ResponseCompleted {
                finish,
                metadata: ModelResponseMetadata::default(),
                continuation: vec![],
            })
        };
        if attempt == 0 {
            let mut events: Vec<_> = ["before", "target", "after"]
                .iter()
                .enumerate()
                .map(|(index, name)| {
                    Ok(ModelEvent::ToolArgumentsDelta {
                        index: index as u32,
                        provider_call_id: Some(format!("provider-{index}")),
                        name: Some((*name).into()),
                        delta: serde_json::to_string(&json!({"query":name})).unwrap(),
                    })
                })
                .collect();
            events.push(completion(ModelFinish::ToolCalls));
            Box::pin(stream::iter(events))
        } else {
            Box::pin(
                stream::once(async move {
                    if self.hold_final.load(Ordering::SeqCst) {
                        self.final_entered.notify_one();
                        self.final_release.acquire().await.unwrap().forget();
                    }
                    Ok(ModelEvent::TextDelta {
                        text: "Recorded observations processed".into(),
                    })
                })
                .chain(stream::iter([completion(ModelFinish::Stop)])),
            )
        }
    }
}

pub struct Invocation {
    pub arguments: JsonObject,
    pub context: ToolExecutionContext,
}
pub struct Tool {
    pub name: &'static str,
    pub mode: Mode,
    pub calls: AtomicUsize,
    pub applied: AtomicUsize,
    pub apply_write: AtomicBool,
    pub seen: Mutex<Vec<Invocation>>,
    pub order: Arc<Mutex<Vec<&'static str>>>,
}
impl ToolExecutor for Tool {
    fn execute<'a>(
        &'a self,
        arguments: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.seen.lock().unwrap().push(Invocation {
                arguments: arguments.clone(),
                context: context.clone(),
            });
            self.order.lock().unwrap().push(self.name);
            if self.name != "target" {
                return Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Succeeded {
                        value: json!(self.name),
                    },
                    effect: ToolEffect::NotApplied,
                    receipt: None,
                });
            }
            if self.mode == Mode::Input {
                return Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::InputRequired {
                        question: "Which report should be selected?".into(),
                    },
                    effect: ToolEffect::NotApplied,
                    receipt: None,
                });
            }
            if self.apply_write.load(Ordering::SeqCst) {
                self.applied.fetch_add(1, Ordering::SeqCst);
            }
            if self.mode == Mode::External {
                return Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Failed {
                        code: id("response_lost"),
                    },
                    effect: ToolEffect::Unknown,
                    receipt: None,
                });
            }
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: json!("target"),
                },
                effect: ToolEffect::Applied,
                receipt: Some(
                    json!({"effect_id":"stored-effect","record_id":arguments["record_id"]}),
                ),
            })
        })
    }
}

pub struct Fixture {
    pub base: agent_support::Fixture,
    pub model: Arc<Model>,
    pub policy: Arc<Policy>,
    pub resolver: Arc<Resolver>,
    pub tools: Vec<Arc<Tool>>,
    pub registry: Arc<ToolRegistry>,
    pub inputs: SystemInputRegistry,
    pub profile: AgentProfile,
    pub order: Arc<Mutex<Vec<&'static str>>>,
    pub store: Arc<CommandStore>,
    pub verifier: Arc<Verifier>,
}
impl Fixture {
    pub fn new(mode: Mode) -> Self {
        let base = agent_support::Fixture::new(agent_support::Response::Text, false);
        let inputs = SystemInputRegistry::new(vec![
            SystemInputDefinition {
                key: id("workspace_id"),
                version: id("1"),
                value_schema: json!({"type":"string","format":"uuid"}),
                source: SystemInputSource::Run {},
            },
            SystemInputDefinition {
                key: id("record_id"),
                version: id("1"),
                value_schema: json!({"type":"string","format":"uuid"}),
                source: SystemInputSource::Resolver {
                    resolver_ref: reference("records"),
                },
            },
        ])
        .unwrap();
        let mut profile = agent_support::profile();
        profile.limits.max_tool_attempts = 6;
        let order = Arc::new(Mutex::new(vec![]));
        let mut tools = vec![];
        let mut registrations = vec![];
        for name in ["before", "target", "after"] {
            let target = name == "target";
            let compiled = SchemaCompiler::new().compile(ToolDescriptor {
                tool: reference(name), name: id(name), description: format!("Observe {name}"),
                input_schema: if target { json!({"type":"object","properties":{"query":{"type":"string"},"workspace_id":{"type":"string","format":"uuid"},"record_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id","record_id"],"additionalProperties":false}) }
                    else { json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}) },
                agent_parameters: vec!["query".into()], system_bindings: None,
                output_schema: if target && mode == Mode::Input { json!({"type":"object","properties":{"selection":{"type":"string","enum":["annual","quarterly"]}},"required":["selection"],"additionalProperties":false}) } else { json!({"type":"string"}) },
                side_effect: if target && mode != Mode::Input { ToolSideEffect::Write } else { ToolSideEffect::ReadOnly },
                concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: mode == Mode::External,
                max_output_bytes: 4096.try_into().unwrap(),
            }, &inputs).unwrap();
            let tool = Arc::new(Tool {
                name,
                mode,
                calls: AtomicUsize::new(0),
                applied: AtomicUsize::new(0),
                apply_write: AtomicBool::new(true),
                seen: Mutex::new(vec![]),
                order: order.clone(),
            });
            registrations.push(ToolRegistration {
                compiled,
                executor: tool.clone(),
            });
            tools.push(tool);
            profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
                tool_id: id(name),
                version: id("1"),
                bindings: None,
                config: None,
            }));
        }
        let store = Arc::new(CommandStore {
            inner: base.store.clone(),
            mode: AtomicUsize::new(0),
            consumed_commits: AtomicUsize::new(0),
            wait_timeout_ms: AtomicUsize::new(0),
            pause_record: Mutex::new(None),
            record_entered: Notify::new(),
            record_release: Semaphore::new(0),
            proof: ProtectedRecord::new(
                id("external-proof"),
                1,
                json!({"source":"trusted-fixture-service","proof":"verified-write"}),
            ),
            forged: ProtectedRecord::new(
                id("forged-proof"),
                1,
                json!({"source":"caller","proof":"verified-write"}),
            ),
            entered: Notify::new(),
            release: Semaphore::new(0),
        });
        let verifier = Arc::new(Verifier {
            target: tools[1].clone(),
            calls: AtomicUsize::new(0),
            mode: AtomicUsize::new(0),
            seen: Mutex::new(vec![]),
        });
        Self {
            base,
            profile,
            order,
            model: Arc::new(Model {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(vec![]),
                hold_final: AtomicBool::new(false),
                final_entered: Notify::new(),
                final_release: Semaphore::new(0),
            }),
            policy: Arc::new(Policy {
                mode,
                deny_resume: AtomicBool::new(false),
                deny_execute: AtomicBool::new(false),
                deny_cancel: AtomicBool::new(false),
                deny_receipt_read: AtomicBool::new(false),
                deny_details: AtomicBool::new(false),
                details_checks: AtomicUsize::new(0),
                require_resume_approval: AtomicBool::new(false),
                target_checks: AtomicUsize::new(0),
                resume_checks: Mutex::new(vec![]),
                tool_checks: Mutex::new(vec![]),
                pause_next_resume: AtomicBool::new(false),
                resume_entered: Notify::new(),
                resume_release: Semaphore::new(0),
            }),
            resolver: Arc::new(Resolver {
                calls: AtomicUsize::new(0),
                value: Mutex::new(ResolvedSystemInput {
                    value: json!(RECORD),
                    revision: id("record-revision-1"),
                }),
            }),
            tools,
            registry: Arc::new(ToolRegistry::new(scope(), registrations).unwrap()),
            inputs,
            store,
            verifier,
        }
    }
    pub fn bindings(&self) -> AgentBindings {
        let mut bindings = self.base.bindings();
        let mut router = agent_support::Router::new();
        let mut catalog = router.snapshot.catalog().clone();
        catalog.models[0]
            .capabilities
            .features
            .insert(id("tool_calling"));
        catalog.bindings[0]
            .capabilities
            .features
            .insert(id("tool_calling"));
        catalog.bindings[0].evidence[0].binding_digest = catalog.bindings[0]
            .contract_digest(&catalog.models[0])
            .unwrap();
        router.snapshot = RoutingSnapshot::new(catalog, router.snapshot.policy().clone()).unwrap();
        let policy =
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(5)).unwrap());
        bindings.router = Arc::new(router);
        bindings.profile_resolver = Arc::new(Catalog);
        bindings.policy = policy.clone();
        bindings.model_exchange = Arc::new(
            ModelExchange::new(self.model.clone(), policy)
                .with_route_inspector(self.base.inspector.clone(), Duration::from_secs(1))
                .unwrap(),
        );
        bindings.tools = Some(self.registry.clone());
        bindings.system_inputs = self.inputs.clone();
        bindings.system_input_resolver = Some(self.resolver.clone());
        bindings.state = self.store.clone();
        bindings.external_receipt_verifier = Some(self.verifier.clone());
        bindings
    }
    pub fn agent(&self) -> Agent {
        create_agent(self.profile.clone(), self.bindings()).unwrap()
    }
    pub async fn started(&self, agent: &Agent) -> RunHandle {
        let mut execution = context();
        execution.data.system_inputs =
            Some(SystemInputs::new(object(json!({"workspace_id":WORKSPACE}))));
        completed(agent.start(request("request"), execution).await.unwrap())
    }
    pub async fn outcome(&self, handle: &RunHandle) -> RunOutcome {
        completed(
            tokio::time::timeout(Duration::from_secs(5), handle.outcome(&context()))
                .await
                .expect("segment outcome must resolve")
                .unwrap(),
        )
    }
    pub async fn saved(&self, handle: &RunHandle) -> StoredRun {
        self.base
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
    }
    pub async fn approve(&self, handle: &RunHandle, command_id: &str) -> ResumeCommand {
        let saved = self.saved(handle).await;
        let wait = saved.snapshot.wait.unwrap();
        let WaitTarget::Approval { target } = wait.target else {
            panic!("expected approval wait")
        };
        ResumeCommand {
            run_id: handle.run_id().clone(),
            expected_revision: saved.snapshot.revision,
            command_id: id(command_id),
            action: ResumeAction::Approve {
                wait_id: wait.wait_id,
                target,
            },
        }
    }
}

pub async fn gate(entered: &Notify) {
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("observable operation must enter");
}

pub struct Verifier {
    pub target: Arc<Tool>,
    pub calls: AtomicUsize,
    pub mode: AtomicUsize,
    pub seen: Mutex<Vec<ExternalReceiptRequest>>,
}
impl ExternalReceiptVerifier for Verifier {
    fn verify<'a>(
        &'a self,
        request: &'a ExternalReceiptRequest,
        context: &'a ExternalReceiptContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.seen.lock().unwrap().push(request.clone());
            let execution = self.target.seen.lock().unwrap()[0].context.clone();
            assert_eq!(request.call.call_id, execution.call_id);
            assert_eq!(request.attempt_id, execution.attempt_id);
            assert_eq!(request.idempotency_key, execution.idempotency_key);
            assert_eq!(context.scope, execution.scope);
            assert_eq!(
                request.bound_input.execution_args()["record_id"],
                json!(RECORD)
            );
            if request.receipt
                != json!({"source":"trusted-fixture-service","proof":"verified-write"})
            {
                return Err(ContractError::new(
                    ErrorCode::AccessDenied,
                    "receipt.signature",
                ));
            }
            if self.mode.load(Ordering::SeqCst) == 1 {
                return Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Failed {
                        code: id("still_unknown"),
                    },
                    effect: ToolEffect::Unknown,
                    receipt: None,
                });
            }
            if self.mode.load(Ordering::SeqCst) == 3 {
                assert_eq!(self.target.applied.load(Ordering::SeqCst), 0);
                return Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Failed {
                        code: id("not_applied"),
                    },
                    effect: ToolEffect::NotApplied,
                    receipt: Some(request.receipt.clone()),
                });
            }
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: if self.mode.load(Ordering::SeqCst) == 2 {
                        json!(42)
                    } else {
                        json!("verified target")
                    },
                },
                effect: ToolEffect::Applied,
                receipt: Some(request.receipt.clone()),
            })
        })
    }
}

/// Fail only resume-command consumption, independently of result or heartbeat writes.
pub struct CommandStore {
    pub inner: Arc<MemoryStateStore>,
    pub mode: AtomicUsize,
    pub consumed_commits: AtomicUsize,
    pub wait_timeout_ms: AtomicUsize,
    pub pause_record: Mutex<Option<RecordRef>>,
    pub record_entered: Notify,
    pub record_release: Semaphore,
    pub proof: ProtectedRecord,
    pub forged: ProtectedRecord,
    pub entered: Notify,
    pub release: Semaphore,
}
impl StateStore for CommandStore {
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
    fn admit<'a>(
        &'a self,
        s: &'a Scope,
        mut input: AdmissionInput,
    ) -> PortFuture<'a, AdmissionResult> {
        input
            .records
            .extend([self.proof.clone(), self.forged.clone()]);
        self.inner.admit(s, input)
    }
    fn load<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 6 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }
            self.inner.load(s, r).await
        })
    }
    fn load_session<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, SessionSnapshot> {
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 6 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }
            self.inner.load_session(s, r).await
        })
    }
    fn check_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, RunLease> {
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 6 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }
            self.inner.check_lease(s, r, l, n).await
        })
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
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 6 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }
            self.inner.renew_lease(s, r, l, n, t).await
        })
    }
    fn release_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 6 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }
            self.inner.release_lease(s, r, l, n).await
        })
    }
    fn read_events<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        after: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage> {
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 6 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }
            self.inner.read_events(s, r, after, limit).await
        })
    }
    fn read_record<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 6 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }
            let pause = {
                let mut reference = self.pause_record.lock().unwrap();
                if reference.as_ref() == Some(r) {
                    reference.take();
                    true
                } else {
                    false
                }
            };
            if pause {
                self.record_entered.notify_one();
                self.record_release.acquire().await.unwrap().forget();
            }
            self.inner.read_record(s, r).await
        })
    }
    fn commit<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        mut input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 6 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }
            if self.mode.load(Ordering::SeqCst) == 5
                && input
                    .events
                    .iter()
                    .any(|event| matches!(event.payload, RunEventPayload::ToolUnresolved { .. }))
            {
                self.mode.store(6, Ordering::SeqCst);
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }

            if self.mode.load(Ordering::SeqCst) == 4
                && input
                    .events
                    .iter()
                    .any(|event| matches!(event.payload, RunEventPayload::ToolUnresolved { .. }))
            {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "tool.result.commit",
                ));
            }
            let timeout = self.wait_timeout_ms.load(Ordering::SeqCst);
            if timeout > 0
                && input
                    .events
                    .iter()
                    .any(|event| matches!(event.payload, RunEventPayload::RunWaiting { .. }))
            {
                // A Host can constrain an individual wait without shortening the Run budget.
                // Rewrite the newly created wait and its paired protected outcome atomically.
                let old_wait = serde_json::to_value(input.snapshot.wait.as_ref().unwrap()).unwrap();
                let old_outcome =
                    serde_json::to_value(input.snapshot.outcome.as_ref().unwrap()).unwrap();
                let wait = input.snapshot.wait.as_mut().unwrap();
                wait.expires_at_ms =
                    Some(input.snapshot.timing.last_observed_at_ms + timeout as i64);
                if let OutcomeResult::Waiting { wait: saved_wait } =
                    &mut input.snapshot.outcome.as_mut().unwrap().result
                {
                    *saved_wait = wait.clone();
                }
                let new_wait = serde_json::to_value(wait).unwrap();
                let new_outcome =
                    serde_json::to_value(input.snapshot.outcome.as_ref().unwrap()).unwrap();
                let mut wait_ref = None;
                for record in &mut input.records {
                    let value = if record.value() == &old_wait {
                        Some(new_wait.clone())
                    } else if record.value() == &old_outcome {
                        Some(new_outcome.clone())
                    } else {
                        None
                    };
                    if let Some(value) = value {
                        let is_wait = record.value() == &old_wait;
                        *record = ProtectedRecord::new(
                            record.reference().record_id.clone(),
                            record.reference().revision,
                            value,
                        );
                        if is_wait {
                            wait_ref = Some(record.reference().clone());
                        }
                    }
                }
                for event in &mut input.events {
                    if let RunEventPayload::RunWaiting {
                        wait_ref: reference,
                    } = &mut event.payload
                    {
                        *reference = wait_ref.clone().expect("paired wait record must exist");
                    }
                }
            }
            if !input
                .events
                .iter()
                .any(|event| matches!(event.payload, RunEventPayload::RunResumed { .. }))
            {
                return self.inner.commit(s, r, input).await;
            }
            self.consumed_commits.fetch_add(1, Ordering::SeqCst);
            match self.mode.swap(0, Ordering::SeqCst) {
                1 => Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "resume.commit",
                )),
                2 => {
                    self.inner.commit(s, r, input).await?;
                    Err(ContractError::new(
                        ErrorCode::PersistenceUnavailable,
                        "resume.ack",
                    ))
                }
                3 => {
                    self.entered.notify_one();
                    self.release.acquire().await.unwrap().forget();
                    self.inner.commit(s, r, input).await
                }
                _ => self.inner.commit(s, r, input).await,
            }
        })
    }
}
