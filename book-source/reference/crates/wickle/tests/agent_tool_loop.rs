//! Agent-level model/tool loops preserve binding boundaries, waits, and effect outcomes.

#[path = "support/agent.rs"]
#[allow(dead_code)]
mod agent_support;
#[path = "support/agent_hooks.rs"]
#[allow(dead_code)]
mod hooks_support;
#[path = "support/agent_resume.rs"]
#[allow(dead_code)]
mod resume_support;
#[path = "support/context_sources.rs"]
#[allow(dead_code, unused_imports)]
mod source_support;
use agent_support::{completed, context, id, profile, reference, request, scope};
use futures_util::stream;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use wickle::*;

const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";
fn object(value: Value) -> JsonObject {
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
                manifest_digest: canonical_digest(&json!("tool-loop-catalog")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| reference.id.clone()),
                hook_position: (reference.kind == ComponentKind::Hook)
                    .then_some(HookPosition::BeforeModel),
                exports: vec![],
            })
        })
    }
}
#[derive(Default)]
struct Policy {
    mode: AtomicUsize,
    tool_checks: AtomicUsize,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 4
                && matches!(request.action, PolicyAction::ReadArtifact { .. })
            {
                return Ok(PolicyDecision::Deny {
                    reason: id("artifact_revoked"),
                });
            }
            if self.mode.load(Ordering::SeqCst) == 5
                && matches!(request.action, PolicyAction::RewriteContext { .. })
            {
                return Ok(PolicyDecision::Deny {
                    reason: id("context_denied"),
                });
            }
            if let PolicyAction::ExecuteTool { .. } = &request.action {
                let check = self.tool_checks.fetch_add(1, Ordering::SeqCst) + 1;
                match self.mode.load(Ordering::SeqCst) {
                    1 => {
                        return Ok(PolicyDecision::Deny {
                            reason: id("denied"),
                        });
                    }
                    2 => {
                        return Ok(PolicyDecision::RequireApproval {
                            reason: id("review"),
                        });
                    }
                    3 if check >= 3 => {
                        return Ok(PolicyDecision::RequireApproval {
                            reason: id("late_review"),
                        });
                    }
                    _ => {}
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}

struct Model {
    plans: Vec<(&'static str, JsonObject)>,
    calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
    rounds: AtomicUsize,
    compactions: AtomicUsize,
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
        if request.purpose == ModelPurpose::Compaction {
            self.compactions.fetch_add(1, Ordering::SeqCst);
            return Box::pin(stream::iter(vec![Ok(ModelEvent::TextDelta {text:"Earlier records were read successfully; their detailed observations remain in storage.".into()}),Ok(ModelEvent::ResponseCompleted {finish:ModelFinish::Stop,metadata:Default::default(),continuation:vec![]})]));
        }
        let events = if attempt - self.compactions.load(Ordering::SeqCst)
            < self.rounds.load(Ordering::SeqCst)
        {
            let mut events: Vec<_> = self
                .plans
                .iter()
                .enumerate()
                .map(|(index, (name, arguments))| {
                    Ok(ModelEvent::ToolArgumentsDelta {
                        index: index as u32,
                        provider_call_id: Some(format!("provider-{index}")),
                        name: Some((*name).into()),
                        delta: serde_json::to_string(arguments).unwrap(),
                    })
                })
                .collect();
            events.push(Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::ToolCalls,
                metadata: ModelResponseMetadata::default(),
                continuation: vec![],
            }));
            events
        } else {
            vec![
                Ok(ModelEvent::TextDelta {
                    text: "All observations processed".into(),
                }),
                Ok(ModelEvent::ResponseCompleted {
                    finish: ModelFinish::Stop,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![],
                }),
            ]
        };
        Box::pin(stream::iter(events))
    }
}
#[derive(Clone, Copy)]
enum Behavior {
    Success,
    InvalidOutput,
    Unknown,
    Pending,
}
struct Tool {
    name: &'static str,
    behavior: Behavior,
    effect: ToolEffect,
    calls: AtomicUsize,
    applied: AtomicUsize,
    arguments: Mutex<Vec<JsonObject>>,
    order: Arc<Mutex<Vec<&'static str>>>,
    entered: Notify,
}
impl ToolExecutor for Tool {
    fn execute<'a>(
        &'a self,
        arguments: &'a JsonObject,
        _: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.arguments.lock().unwrap().push(arguments.clone());
            self.order.lock().unwrap().push(self.name);
            if self.effect == ToolEffect::Applied {
                self.applied.fetch_add(1, Ordering::SeqCst);
            }
            self.entered.notify_one();
            match self.behavior {
            Behavior::Pending=>std::future::pending().await,
            Behavior::Unknown=>Ok(ToolExecutionResult{outcome:ToolExecutionOutcome::Failed{code:id("lost_response")},effect:ToolEffect::Unknown,receipt:None}),
            Behavior::Success|Behavior::InvalidOutput=>Ok(ToolExecutionResult{
                outcome:ToolExecutionOutcome::Succeeded{value:if matches!(self.behavior,Behavior::InvalidOutput){json!(42)}else{json!(format!("{} observation",self.name))}},effect:self.effect,
                receipt:(self.effect==ToolEffect::Applied).then(||json!({"private_receipt":"only-for-storage","target":arguments["workspace_id"]})),
            }),
        }
        })
    }
}

struct Fixture {
    base: agent_support::Fixture,
    model: Arc<Model>,
    policy: Arc<Policy>,
    tools: Vec<Arc<Tool>>,
    registry: Arc<ToolRegistry>,
    inputs: SystemInputRegistry,
    profile: AgentProfile,
    order: Arc<Mutex<Vec<&'static str>>>,
}
impl Fixture {
    fn new(plans: Vec<(&'static str, JsonObject)>, write_behavior: Behavior) -> Self {
        let base = agent_support::Fixture::new(agent_support::Response::Text, false);
        let inputs = SystemInputRegistry::new(vec![SystemInputDefinition {
            key: id("workspace_id"),
            version: id("1"),
            value_schema: json!({"type":"string","format":"uuid"}),
            source: SystemInputSource::Run {},
        }])
        .unwrap();
        let order = Arc::new(Mutex::new(vec![]));
        let mut tools = vec![];
        let mut registrations = vec![];
        let mut profile = profile();
        profile.limits.max_tool_attempts = 4;
        for (name, effect, behavior) in [
            ("read", ToolSideEffect::ReadOnly, Behavior::Success),
            ("write", ToolSideEffect::Write, write_behavior),
        ] {
            let compiled=SchemaCompiler::new().compile(ToolDescriptor{tool:reference(name),name:id(name),description:format!("{name} records"),input_schema:json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"default":10},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),agent_parameters:vec!["query".into(),"limit".into()],system_bindings:None,output_schema:json!({"type":"string"}),side_effect:effect,concurrency:ToolConcurrency::Serial,retry:ToolRetryPolicy::Never,reconcile:false,max_output_bytes:4096.try_into().unwrap()},&inputs).unwrap();
            let executor = Arc::new(Tool {
                name,
                behavior,
                effect: if effect == ToolSideEffect::ReadOnly {
                    ToolEffect::NotApplied
                } else {
                    ToolEffect::Applied
                },
                calls: AtomicUsize::new(0),
                applied: AtomicUsize::new(0),
                arguments: Mutex::new(vec![]),
                order: order.clone(),
                entered: Notify::new(),
            });
            registrations.push(ToolRegistration {
                compiled,
                executor: executor.clone(),
            });
            tools.push(executor);
            profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
                tool_id: id(name),
                version: id("1"),
                bindings: None,
                config: None,
            }));
        }
        Self {
            base,
            model: Arc::new(Model {
                plans,
                calls: AtomicUsize::new(0),
                requests: Mutex::new(vec![]),
                rounds: AtomicUsize::new(1),
                compactions: AtomicUsize::new(0),
            }),
            policy: Arc::new(Policy::default()),
            tools,
            registry: Arc::new(ToolRegistry::new(scope(), registrations).unwrap()),
            inputs,
            profile,
            order,
        }
    }
    fn agent(&self) -> Agent {
        create_agent(self.profile.clone(), self.bindings()).unwrap()
    }
    fn bindings(&self) -> AgentBindings {
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
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap());
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
        bindings.system_input_resolver = None;
        bindings.settings.tool_execution_limits = ToolExecutionLimits {
            timeout_ms: 30,
            max_receipt_bytes: 4096,
        };
        bindings
    }
    async fn start(&self, agent: &Agent) -> RunHandle {
        let mut context = context();
        context.data.system_inputs =
            Some(SystemInputs::new(object(json!({"workspace_id":WORKSPACE}))));
        completed(agent.start(request("request"), context).await.unwrap())
    }
    async fn outcome(&self, handle: &RunHandle) -> RunOutcome {
        completed(handle.outcome(&context()).await.unwrap())
    }
}

struct ArtifactTool {
    inner: Arc<Tool>,
    reference: ArtifactRef,
    evidence: EvidenceRef,
}
struct LongRead {
    text: String,
    calls: AtomicUsize,
    content: Vec<InputContent>,
}
impl ToolExecutor for LongRead {
    fn execute<'a>(
        &'a self,
        _: &'a JsonObject,
        _: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ToolExecutionResult {
                outcome: if self.content.is_empty() {
                    ToolExecutionOutcome::Succeeded {
                        value: json!(self.text),
                    }
                } else {
                    ToolExecutionOutcome::SucceededWithContent {
                        value: json!(self.text),
                        content: self.content.clone(),
                    }
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}
struct Summary {
    calls: AtomicUsize,
    requests: Mutex<Vec<CompactionRequest>>,
    bad: bool,
}
struct ContextAudit(AtomicUsize);
#[tokio::test]
async fn a_missing_compaction_route_is_rejected_before_agent_execution() {
    let f = Fixture::new(
        vec![("read", object(json!({"query":"small"})))],
        Behavior::Success,
    );
    let (mut bindings, _) = long_bindings(&f, 32);
    bindings.context_runtime = Some(Arc::new(
        ContextRuntime::new(
            scope(),
            Arc::new(BoundedContextStrategy),
            Some(ContextCompactor::Model(ModelCompactorConfig {
                model_binding: id("primary"),
                options: None,
                max_output_tokens: 128.try_into().unwrap(),
            })),
            ContextRewriteLimits::default(),
        )
        .unwrap(),
    ));
    let agent = create_agent(f.profile.clone(), bindings).unwrap();
    assert_eq!(
        agent
            .start(request("missing-route"), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ModelRouteDenied
    );
    assert_eq!(f.model.calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn compaction_preserves_typed_artifact_and_evidence_anchors_from_removed_rounds() {
    let f = Fixture::new(
        vec![("read", object(json!({"query":"chunk"})))],
        Behavior::Success,
    );
    f.model.rounds.store(3, Ordering::SeqCst);
    let (mut bindings, _) = long_bindings(&f, 3500);
    let artifacts = Arc::new(
        ArtifactRuntime::new(
            Arc::new(MemoryArtifactStore::default()),
            bindings.policy.clone(),
            bindings.ids.clone(),
            ArtifactLimits::default(),
        )
        .unwrap(),
    );
    let metadata = artifacts
        .put(
            ArtifactInput {
                media_type: id("text/plain"),
                bytes: b"original evidence".to_vec(),
                source: Some(reference("report")),
            },
            &context(),
            None,
        )
        .await
        .unwrap();
    let evidence = artifacts
        .evidence(
            &metadata.reference,
            id("line-1"),
            Some("evidence".into()),
            &context(),
            None,
        )
        .await
        .unwrap();
    let content = vec![
        InputContent::Artifact {
            reference: metadata.reference.clone(),
        },
        InputContent::Evidence {
            reference: evidence.clone(),
        },
    ];
    let reader = Arc::new(LongRead {
        text: "x".repeat(3500),
        calls: AtomicUsize::new(0),
        content: content.clone(),
    });
    let registered = bindings.tools.as_ref().unwrap();
    bindings.tools = Some(Arc::new(
        ToolRegistry::new(
            scope(),
            vec![
                ToolRegistration {
                    compiled: registered.get(&id("read")).unwrap().compiled.clone(),
                    executor: reader,
                },
                registered.get(&id("write")).unwrap().clone(),
            ],
        )
        .unwrap(),
    ));
    bindings.artifacts = Some(artifacts);
    bindings.context_runtime = Some(Arc::new(
        ContextRuntime::new(
            scope(),
            Arc::new(BoundedContextStrategy),
            Some(ContextCompactor::Host {
                definition: reference("summary"),
                compressor: Arc::new(Summary {
                    calls: AtomicUsize::new(0),
                    requests: Mutex::new(vec![]),
                    bad: false,
                }),
            }),
            ContextRewriteLimits::default(),
        )
        .unwrap(),
    ));
    let handle = f
        .start(&create_agent(f.profile.clone(), bindings).unwrap())
        .await;
    assert_eq!(
        f.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
    let record = f
        .base
        .store
        .read_record(
            &scope(),
            saved.snapshot.context_revision_ref.as_ref().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        record.value()["anchors"],
        serde_json::to_value(content).unwrap()
    );
    assert!(f.model.requests.lock().unwrap().last().unwrap().messages.iter().flat_map(|message|&message.content).any(|item|matches!(item,ModelContent::Json {value} if value["origin"]=="compaction"&&value["content"].as_array().is_some_and(|items|items.iter().any(|item|item["type"]=="evidence"&&item["content_hash"]==json!(evidence.content_hash))))));
    let image = serde_json::to_value(f.base.store.export_checkpoint(&scope()).unwrap()).unwrap();
    assert_prepared_corruption_rejected(&image, "artifacts");
}
struct PausedSummary {
    entered: Notify,
    release: tokio::sync::Semaphore,
    calls: AtomicUsize,
}
impl HostContextCompactor for PausedSummary {
    fn compact<'a>(
        &'a self,
        _: &'a CompactionRequest,
        _: &'a ContextStrategyContext,
    ) -> PortFuture<'a, String> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
            Ok("late summary".into())
        })
    }
}
#[tokio::test]
async fn cancelled_and_timed_out_compactors_cannot_adopt_late_results() {
    for cancel in [true, false] {
        let f = Fixture::new(
            vec![("read", object(json!({"query":"chunk"})))],
            Behavior::Success,
        );
        f.model.rounds.store(2, Ordering::SeqCst);
        let (mut bindings, _) = long_bindings(&f, 3500);
        let summary = Arc::new(PausedSummary {
            entered: Notify::new(),
            release: tokio::sync::Semaphore::new(0),
            calls: AtomicUsize::new(0),
        });
        bindings.context_runtime = Some(Arc::new(
            ContextRuntime::new(
                scope(),
                Arc::new(BoundedContextStrategy),
                Some(ContextCompactor::Host {
                    definition: reference("paused-summary"),
                    compressor: summary.clone(),
                }),
                ContextRewriteLimits {
                    timeout_ms: if cancel { 1000 } else { 100 },
                    ..Default::default()
                },
            )
            .unwrap(),
        ));
        let handle = f
            .start(&create_agent(f.profile.clone(), bindings).unwrap())
            .await;
        tokio::time::timeout(Duration::from_secs(3), summary.entered.notified())
            .await
            .unwrap();
        if cancel {
            handle.cancel(id("stop"), &context()).await.unwrap();
        }
        let outcome = f.outcome(&handle).await;
        assert_eq!(
            outcome.result.status(),
            if cancel {
                RunStatus::Cancelled
            } else {
                RunStatus::Failed
            }
        );
        let before = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
        assert!(before.snapshot.context_revision_ref.is_none());
        assert_eq!(f.model.calls.load(Ordering::SeqCst), 2);
        summary.release.add_permits(1);
        tokio::task::yield_now().await;
        assert_eq!(
            f.base.store.load(&scope(), handle.run_id()).await.unwrap(),
            before
        );
        assert_eq!(summary.calls.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn context_commit_failure_does_not_publish_a_candidate_and_ack_loss_does_not_recompress() {
    for acknowledge_lost in [false, true] {
        let f = Fixture::new(
            vec![("read", object(json!({"query":"chunk"})))],
            Behavior::Success,
        );
        f.model.rounds.store(2, Ordering::SeqCst);
        let (mut bindings, _) = long_bindings(&f, 3500);
        let summary = Arc::new(Summary {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(vec![]),
            bad: false,
        });
        let store = Arc::new(agent_support::FinalCommitStore::new(
            f.base.store.clone(),
            if acknowledge_lost {
                agent_support::FinalCommitMode::LoseContextAcknowledgement
            } else {
                agent_support::FinalCommitMode::RejectContext
            },
        ));
        bindings.state = store.clone();
        bindings.context_runtime = Some(Arc::new(
            ContextRuntime::new(
                scope(),
                Arc::new(BoundedContextStrategy),
                Some(ContextCompactor::Host {
                    definition: reference("summary"),
                    compressor: summary.clone(),
                }),
                ContextRewriteLimits::default(),
            )
            .unwrap(),
        ));
        let handle = f
            .start(&create_agent(f.profile.clone(), bindings).unwrap())
            .await;
        let result = handle.outcome(&context()).await;
        let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
        assert_eq!(summary.calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.context_attempts.load(Ordering::SeqCst), 1);
        if acknowledge_lost {
            assert_eq!(
                completed(result.unwrap()).result.status(),
                RunStatus::Succeeded
            );
            assert!(saved.snapshot.context_revision_ref.is_some());
            assert_eq!(f.model.calls.load(Ordering::SeqCst), 3);
        } else {
            assert_eq!(result.unwrap_err().code, ErrorCode::PersistenceUnavailable);
            assert!(saved.snapshot.context_revision_ref.is_none());
            assert!(saved.snapshot.context_decisions.is_empty());
            assert_eq!(f.model.calls.load(Ordering::SeqCst), 2);
        }
    }
}
struct PartialSelection;
impl ContextStrategy for PartialSelection {
    fn definition(&self) -> ContextStrategyDefinition {
        BoundedContextStrategy.definition()
    }
    fn select<'a>(
        &'a self,
        input: &'a ContextSelectionInput,
        _: &'a ContextStrategyContext,
    ) -> PortFuture<'a, Vec<Id>> {
        Box::pin(async move { Ok(vec![input.segments[0].message_ids[0].clone()]) })
    }
}
#[tokio::test]
async fn context_permission_and_partial_group_selection_fail_before_the_compressor() {
    for denied in [true, false] {
        let f = Fixture::new(
            vec![("read", object(json!({"query":"chunk"})))],
            Behavior::Success,
        );
        f.model.rounds.store(3, Ordering::SeqCst);
        if denied {
            f.policy.mode.store(5, Ordering::SeqCst);
        }
        let (mut bindings, _) = long_bindings(&f, 3500);
        let summary = Arc::new(Summary {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(vec![]),
            bad: false,
        });
        bindings.context_runtime = Some(Arc::new(
            ContextRuntime::new(
                scope(),
                if denied {
                    Arc::new(BoundedContextStrategy)
                } else {
                    Arc::new(PartialSelection)
                },
                Some(ContextCompactor::Host {
                    definition: reference("summary"),
                    compressor: summary.clone(),
                }),
                ContextRewriteLimits::default(),
            )
            .unwrap(),
        ));
        let handle = f
            .start(&create_agent(f.profile.clone(), bindings).unwrap())
            .await;
        assert_eq!(f.outcome(&handle).await.result.status(), RunStatus::Failed);
        assert_eq!(summary.calls.load(Ordering::SeqCst), 0);
        assert_eq!(f.model.calls.load(Ordering::SeqCst), 2);
        assert!(
            f.base
                .store
                .load(&scope(), handle.run_id())
                .await
                .unwrap()
                .snapshot
                .context_revision_ref
                .is_none()
        );
    }
}
impl HookHandler for ContextAudit {
    fn call<'a>(&'a self, _: &'a HookInput, _: &'a HookContext) -> PortFuture<'a, HookOutput> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(HookOutput::Context { additions: vec![] })
        })
    }
}
fn compaction_router(bindings: &mut AgentBindings) {
    let snapshot = bindings.router.snapshot();
    let mut policy = snapshot.policy().clone();
    let mut rule = policy.rules[0].clone();
    rule.purpose = ModelPurpose::Compaction;
    policy.rules.push(rule);
    let mut router = agent_support::Router::new();
    router.snapshot = RoutingSnapshot::new(snapshot.catalog().clone(), policy).unwrap();
    bindings.router = Arc::new(router);
}
#[tokio::test]
async fn model_compaction_uses_the_run_budget_without_replacing_the_agent_step_or_running_agent_hooks()
 {
    let mut f = Fixture::new(
        vec![("read", object(json!({"query":"chunk"})))],
        Behavior::Success,
    );
    f.model.rounds.store(3, Ordering::SeqCst);
    f.profile.limits.max_model_calls = 8.try_into().unwrap();
    f.profile.hooks = Some(vec![HookRef::Catalog(CatalogHookRef {
        hook_id: id("audit"),
        version: id("1"),
        position: HookPosition::BeforeModel,
    })]);
    let (mut bindings, reader) = long_bindings(&f, 3500);
    compaction_router(&mut bindings);
    bindings.context_runtime = Some(Arc::new(
        ContextRuntime::new(
            scope(),
            Arc::new(BoundedContextStrategy),
            Some(ContextCompactor::Model(ModelCompactorConfig {
                model_binding: id("primary"),
                options: None,
                max_output_tokens: 128.try_into().unwrap(),
            })),
            ContextRewriteLimits::default(),
        )
        .unwrap(),
    ));
    let audit = Arc::new(ContextAudit(AtomicUsize::new(0)));
    bindings.hooks = Some(Arc::new(HookRuntime::new(
        bindings.state.clone(),
        bindings.policy.clone(),
        bindings.clock.clone(),
        bindings.ids.clone(),
        Arc::new(
            HookRegistry::new(
                scope(),
                vec![HookRegistration {
                    definition: HookDefinition {
                        hook: reference("audit"),
                        position: HookPosition::BeforeModel,
                        priority: 0,
                        required: true,
                        timeout_ms: 1000,
                        max_output_bytes: 4096,
                    },
                    handler: audit.clone(),
                }],
            )
            .unwrap(),
        ),
    )));
    let agent = create_agent(f.profile.clone(), bindings).unwrap();
    let handle = f.start(&agent).await;
    assert_eq!(
        f.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(reader.calls.load(Ordering::SeqCst), 3);
    assert_eq!(audit.0.load(Ordering::SeqCst), 4);
    assert_eq!(f.model.compactions.load(Ordering::SeqCst), 2);
    let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
    assert_eq!(saved.snapshot.usage.model_calls, 6);
    let last = saved
        .snapshot
        .model_ledger
        .iter()
        .rev()
        .find(|invocation| invocation.purpose == ModelPurpose::Agent)
        .unwrap();
    assert_eq!(
        saved.snapshot.model_step_id.as_ref(),
        Some(&last.model_step_id)
    );
    assert_eq!(
        saved
            .snapshot
            .model_ledger
            .iter()
            .filter(|invocation| invocation.purpose == ModelPurpose::Compaction)
            .count(),
        2
    );
    let checkpoint = f.base.store.export_checkpoint(&scope()).unwrap();
    StateStoreCheckpoint::from_json(
        &serde_json::to_string(&checkpoint).unwrap(),
        &scope(),
        &checkpoint.digest(),
    )
    .unwrap();
}
#[tokio::test]
async fn context_that_fits_does_not_invoke_the_configured_compressor() {
    let f = Fixture::new(
        vec![("read", object(json!({"query":"small"})))],
        Behavior::Success,
    );
    let (mut bindings, _) = long_bindings(&f, 32);
    let summary = Arc::new(Summary {
        calls: AtomicUsize::new(0),
        requests: Mutex::new(vec![]),
        bad: false,
    });
    bindings.context_runtime = Some(Arc::new(
        ContextRuntime::new(
            scope(),
            Arc::new(BoundedContextStrategy),
            Some(ContextCompactor::Host {
                definition: reference("summary"),
                compressor: summary.clone(),
            }),
            ContextRewriteLimits::default(),
        )
        .unwrap(),
    ));
    let handle = f
        .start(&create_agent(f.profile.clone(), bindings).unwrap())
        .await;
    assert_eq!(
        f.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(summary.calls.load(Ordering::SeqCst), 0);
    assert!(
        f.base
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .context_revision_ref
            .is_none()
    );
}
#[tokio::test]
async fn exhausted_model_capacity_does_not_start_an_auxiliary_compaction() {
    let mut f = Fixture::new(
        vec![("read", object(json!({"query":"chunk"})))],
        Behavior::Success,
    );
    f.model.rounds.store(3, Ordering::SeqCst);
    f.profile.limits.max_model_calls = 2.try_into().unwrap();
    let (mut bindings, _) = long_bindings(&f, 3500);
    compaction_router(&mut bindings);
    bindings.context_runtime = Some(Arc::new(
        ContextRuntime::new(
            scope(),
            Arc::new(BoundedContextStrategy),
            Some(ContextCompactor::Model(ModelCompactorConfig {
                model_binding: id("primary"),
                options: None,
                max_output_tokens: 128.try_into().unwrap(),
            })),
            ContextRewriteLimits::default(),
        )
        .unwrap(),
    ));
    let handle = f
        .start(&create_agent(f.profile.clone(), bindings).unwrap())
        .await;
    assert_ne!(
        f.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(f.model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(f.model.compactions.load(Ordering::SeqCst), 0);
}
impl HostContextCompactor for Summary {
    fn compact<'a>(
        &'a self,
        request: &'a CompactionRequest,
        _: &'a ContextStrategyContext,
    ) -> PortFuture<'a, String> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.requests.lock().unwrap().push(request.clone());
            Ok(if self.bad {
                "not smaller ".repeat(1000)
            } else {
                format!(
                    "{} older complete groups were read; their original observations remain available.",
                    request.segments.len()
                )
            })
        })
    }
}
fn long_bindings(f: &Fixture, bytes: usize) -> (AgentBindings, Arc<LongRead>) {
    let mut bindings = f.bindings();
    let reader = Arc::new(LongRead {
        text: "x".repeat(bytes),
        calls: AtomicUsize::new(0),
        content: vec![],
    });
    let mut descriptor = f
        .registry
        .get(&id("read"))
        .unwrap()
        .compiled
        .descriptor()
        .clone();
    descriptor.max_output_bytes = 65536.try_into().unwrap();
    let read = ToolRegistration {
        compiled: SchemaCompiler::new()
            .compile(descriptor, &f.inputs)
            .unwrap(),
        executor: reader.clone(),
    };
    let write = f.registry.get(&id("write")).unwrap().clone();
    bindings.tools = Some(Arc::new(
        ToolRegistry::new(scope(), vec![read, write]).unwrap(),
    ));
    bindings.settings.projection_limits.max_bytes = 8000;
    bindings.settings.lease_ttl_ms = 30_000;
    bindings.settings.heartbeat_interval_ms = 5000;
    (bindings, reader)
}
#[tokio::test]
async fn bounded_context_compaction_preserves_requests_latest_round_and_original_history() {
    let f = Fixture::new(
        vec![("read", object(json!({"query":"chunk"})))],
        Behavior::Success,
    );
    f.model.rounds.store(3, Ordering::SeqCst);
    let (mut bindings, reader) = long_bindings(&f, 3500);
    let summary = Arc::new(Summary {
        calls: AtomicUsize::new(0),
        requests: Mutex::new(vec![]),
        bad: false,
    });
    bindings.context_runtime = Some(Arc::new(
        ContextRuntime::new(
            scope(),
            Arc::new(BoundedContextStrategy),
            Some(ContextCompactor::Host {
                definition: reference("summary"),
                compressor: summary.clone(),
            }),
            ContextRewriteLimits::default(),
        )
        .unwrap(),
    ));
    let agent = create_agent(f.profile.clone(), bindings).unwrap();
    let handle = f.start(&agent).await;
    assert_eq!(
        f.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(reader.calls.load(Ordering::SeqCst), 3);
    assert_eq!(f.model.calls.load(Ordering::SeqCst), 4);
    assert!(summary.calls.load(Ordering::SeqCst) > 0);
    let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
    assert_eq!(saved.messages.len(), 8);
    let reference = saved.snapshot.context_revision_ref.as_ref().unwrap();
    assert_eq!(saved.session.context_revision_ref.as_ref(), Some(reference));
    let plan_record = f
        .base
        .store
        .read_record(&scope(), saved.snapshot.context_plan_ref.as_ref().unwrap())
        .await
        .unwrap();
    let plan = ContextPlan::restore(&plan_record, &saved.snapshot.profile).unwrap();
    let record = f.base.store.read_record(&scope(), reference).await.unwrap();
    let revision =
        ContextRevision::restore(&record, &plan, &scope(), &id("session"), &saved.messages)
            .unwrap();
    assert!(revision.covered_message_ids().iter().all(|id| {
        saved
            .messages
            .iter()
            .any(|message| &message.message_id == id && message.role != MessageRole::User)
    }));
    {
        let requests = f.model.requests.lock().unwrap();
        let latest = requests.last().unwrap();
        assert_eq!(observations(latest).len(), 1);
        let summary_position=latest.messages.iter().position(|message|message.content.iter().any(|content|matches!(content,ModelContent::Json {value} if value["origin"]=="compaction"))).unwrap();
        let user_position = latest
            .messages
            .iter()
            .position(|message| {
                message.role == ModelRole::User
                    && message
                        .content
                        .iter()
                        .any(|content| matches!(content, ModelContent::Text { .. }))
            })
            .unwrap();
        let result_position = latest
            .messages
            .iter()
            .rposition(|message| message.role == ModelRole::Tool)
            .unwrap();
        assert!(summary_position < user_position && user_position < result_position);
        assert!(latest.messages.iter().flat_map(|message|&message.content).any(|content|matches!(content,ModelContent::Json{value} if value["origin"]=="compaction")));
    }

    let checkpoint = f.base.store.export_checkpoint(&scope()).unwrap();
    StateStoreCheckpoint::from_json(
        &serde_json::to_string(&checkpoint).unwrap(),
        &scope(),
        &checkpoint.digest(),
    )
    .unwrap();
    let calls = summary.calls.load(Ordering::SeqCst);
    let replay = f.start(&agent).await;
    f.outcome(&replay).await;
    assert_eq!(summary.calls.load(Ordering::SeqCst), calls);
    let mut next = context();
    next.data.system_inputs = Some(SystemInputs::new(object(json!({"workspace_id":WORKSPACE}))));
    let next = completed(agent.start(request("next-run"), next).await.unwrap());
    f.outcome(&next).await;
    assert_eq!(
        f.base
            .store
            .load(&scope(), next.run_id())
            .await
            .unwrap()
            .snapshot
            .context_revision_ref
            .as_ref(),
        Some(reference)
    );
    assert_eq!(summary.calls.load(Ordering::SeqCst), calls);
    let mut corrupted =
        serde_json::to_value(f.base.store.export_checkpoint(&scope()).unwrap()).unwrap();
    let parent = record.value()["parent"].clone();
    corrupted["sessions"][0]["snapshot"]["context_revision_ref"] = parent.clone();
    for run in corrupted["runs"].as_array_mut().unwrap() {
        run["snapshot"]["context_revision_ref"] = parent.clone();
    }
    if parent.is_null() {
        corrupted["sessions"][0]["snapshot"]
            .as_object_mut()
            .unwrap()
            .remove("context_revision_ref");
        for run in corrupted["runs"].as_array_mut().unwrap() {
            run["snapshot"]
                .as_object_mut()
                .unwrap()
                .remove("context_revision_ref");
        }
    }
    assert!(
        StateStoreCheckpoint::from_json(
            &corrupted.to_string(),
            &scope(),
            &canonical_digest(&corrupted)
        )
        .is_err()
    );
    let mut changed = record.value().clone();
    changed["covered_message_ids"]
        .as_array_mut()
        .unwrap()
        .push(json!(saved.messages[0].message_id));
    let changed = ProtectedRecord::new(reference.record_id.clone(), reference.revision, changed);
    assert_eq!(
        ContextRevision::restore(&changed, &plan, &scope(), &id("session"), &saved.messages)
            .unwrap_err()
            .code,
        ErrorCode::InvalidContextSelection
    );
}
#[tokio::test]
async fn context_rejection_keeps_original_history_and_does_not_repeat_the_compressor() {
    let f = Fixture::new(
        vec![("read", object(json!({"query":"chunk"})))],
        Behavior::Success,
    );
    f.model.rounds.store(3, Ordering::SeqCst);
    let (mut bindings, reader) = long_bindings(&f, 3500);
    let summary = Arc::new(Summary {
        calls: AtomicUsize::new(0),
        requests: Mutex::new(vec![]),
        bad: true,
    });
    bindings.context_runtime = Some(Arc::new(
        ContextRuntime::new(
            scope(),
            Arc::new(BoundedContextStrategy),
            Some(ContextCompactor::Host {
                definition: reference("summary"),
                compressor: summary.clone(),
            }),
            ContextRewriteLimits::default(),
        )
        .unwrap(),
    ));
    let agent = create_agent(f.profile.clone(), bindings).unwrap();
    let handle = f.start(&agent).await;
    let outcome = f.outcome(&handle).await;
    assert_eq!(outcome.result.status(), RunStatus::Failed);
    assert!(outcome.output.is_empty());
    assert_eq!(summary.calls.load(Ordering::SeqCst), 1);
    let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
    assert!(saved.snapshot.context_revision_ref.is_none());
    assert_eq!(saved.snapshot.context_decisions.len(), 1);
    assert_eq!(reader.calls.load(Ordering::SeqCst), 2);
    let replay = f.start(&agent).await;
    assert_eq!(f.outcome(&replay).await, outcome);
    assert_eq!(summary.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn context_preview_keeps_the_original_latest_tool_result_without_a_model_compression_call() {
    let f = Fixture::new(
        vec![("read", object(json!({"query":"chunk"})))],
        Behavior::Success,
    );
    let (mut bindings, reader) = long_bindings(&f, 20000);
    let artifacts = Arc::new(
        ArtifactRuntime::new(
            Arc::new(MemoryArtifactStore::default()),
            bindings.policy.clone(),
            bindings.ids.clone(),
            ArtifactLimits::default(),
        )
        .unwrap(),
    );
    bindings.artifacts = Some(artifacts.clone());
    let agent = create_agent(f.profile.clone(), bindings).unwrap();
    let handle = f.start(&agent).await;
    assert_eq!(
        f.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
    let reference = saved.snapshot.context_revision_ref.as_ref().unwrap();
    let plan = ContextPlan::restore(
        &f.base
            .store
            .read_record(&scope(), saved.snapshot.context_plan_ref.as_ref().unwrap())
            .await
            .unwrap(),
        &saved.snapshot.profile,
    )
    .unwrap();
    let revision = ContextRevision::restore(
        &f.base.store.read_record(&scope(), reference).await.unwrap(),
        &plan,
        &scope(),
        &id("session"),
        &saved.messages,
    )
    .unwrap();
    assert!(revision.summary().is_none());
    assert_eq!(revision.previews().len(), 1);
    assert_eq!(f.model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(reader.calls.load(Ordering::SeqCst), 1);
    let data = artifacts
        .get(&revision.previews()[0].preview.reference, &context(), None)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&data.bytes).unwrap(),
        json!(reader.text)
    );
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[0].state else {
        panic!("settled read")
    };
    assert_eq!(
        result.content,
        vec![InputContent::Json {
            value: json!(reader.text)
        }]
    );
}
struct RejectArtifactPut(AtomicUsize);
impl ArtifactStore for RejectArtifactPut {
    fn put<'a>(
        &'a self,
        _: &'a Id,
        _: &'a ArtifactInput,
        _: &'a ArtifactCallContext,
    ) -> PortFuture<'a, ArtifactMetadata> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(ContractError::new(
                ErrorCode::PersistenceUnavailable,
                "fixture.artifact_put",
            ))
        })
    }
    fn stat<'a>(
        &'a self,
        _: &'a ArtifactRef,
        _: &'a ArtifactCallContext,
    ) -> PortFuture<'a, ArtifactMetadata> {
        Box::pin(async {
            Err(ContractError::new(
                ErrorCode::StateNotFound,
                "fixture.artifact",
            ))
        })
    }
    fn get<'a>(
        &'a self,
        _: &'a ArtifactRef,
        _: u64,
        _: &'a ArtifactCallContext,
    ) -> PortFuture<'a, ArtifactData> {
        Box::pin(async {
            Err(ContractError::new(
                ErrorCode::StateNotFound,
                "fixture.artifact",
            ))
        })
    }
}
struct ArtifactWritingTool {
    inner: Arc<Tool>,
    artifacts: Arc<ArtifactRuntime>,
}
impl ToolExecutor for ArtifactWritingTool {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        call: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            let mut completion = self.inner.execute(args, call).await?;
            let context = ExecutionContext::new(
                ExecutionContextData {
                    scope: call.scope.clone(),
                    principal_ref: call.principal_ref.clone(),
                    capability_grant_ref: call.capability_grant_ref.clone(),
                    trace_context: None,
                    system_inputs: None,
                },
                call.cancellation.clone(),
            );
            if self
                .artifacts
                .put(
                    ArtifactInput {
                        media_type: id("text/plain"),
                        bytes: b"generated report".to_vec(),
                        source: None,
                    },
                    &context,
                    Some(call.deadline),
                )
                .await
                .is_err()
            {
                completion.outcome = ToolExecutionOutcome::Failed {
                    code: id("artifact_store_unavailable"),
                };
            }
            Ok(completion)
        })
    }
}
#[tokio::test]
async fn artifact_storage_failure_after_a_business_write_keeps_its_receipt_without_reexecution() {
    let f = Fixture::new(
        vec![("write", object(json!({"query":"report"})))],
        Behavior::Success,
    );
    let mut bindings = f.bindings();
    let store = Arc::new(RejectArtifactPut(AtomicUsize::new(0)));
    let artifacts = Arc::new(
        ArtifactRuntime::new(
            store.clone(),
            bindings.policy.clone(),
            bindings.ids.clone(),
            ArtifactLimits::default(),
        )
        .unwrap(),
    );
    let mut registrations = vec![];
    for name in ["read", "write"] {
        let entry = f.registry.get(&id(name)).unwrap();
        registrations.push(ToolRegistration {
            compiled: entry.compiled.clone(),
            executor: if name == "write" {
                Arc::new(ArtifactWritingTool {
                    inner: f.tools[1].clone(),
                    artifacts: artifacts.clone(),
                })
            } else {
                entry.executor.clone()
            },
        });
    }
    bindings.tools = Some(Arc::new(ToolRegistry::new(scope(), registrations).unwrap()));
    bindings.artifacts = Some(artifacts);
    let agent = create_agent(f.profile.clone(), bindings).unwrap();
    let handle = f.start(&agent).await;
    f.outcome(&handle).await;
    let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[0].state else {
        panic!("known effect")
    };
    assert_eq!(result.status, ToolResultStatus::Failed);
    assert_eq!(result.effect, ToolEffect::Applied);
    assert_eq!(
        result.error.as_ref().unwrap().code,
        id("artifact_store_unavailable")
    );
    let receipt = f
        .base
        .store
        .read_record(&scope(), result.effect_receipt_ref.as_ref().unwrap())
        .await
        .unwrap();
    assert_eq!(receipt.value()["receipt"]["target"], json!(WORKSPACE));
    let replay = f.start(&agent).await;
    f.outcome(&replay).await;
    assert_eq!(f.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(store.0.load(Ordering::SeqCst), 1);
    assert_eq!(f.model.calls.load(Ordering::SeqCst), 2);
}
struct RevokeArtifact {
    policy: Arc<Policy>,
    inner: Arc<agent_support::Inspector>,
}
impl ModelRouteInspector for RevokeArtifact {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        context: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            let observation = self.inner.inspect(route, context).await?;
            if self.inner.calls.load(Ordering::SeqCst) == 2 {
                self.policy.mode.store(4, Ordering::SeqCst);
            }
            Ok(observation)
        })
    }
}
#[tokio::test]
async fn artifact_access_is_rechecked_after_route_inspection_before_model_dispatch() {
    let f = Fixture::new(
        vec![("write", object(json!({"query":"report"})))],
        Behavior::Success,
    );
    let mut bindings = f.bindings();
    let artifacts = Arc::new(
        ArtifactRuntime::new(
            Arc::new(MemoryArtifactStore::default()),
            bindings.policy.clone(),
            bindings.ids.clone(),
            ArtifactLimits::default(),
        )
        .unwrap(),
    );
    let metadata = artifacts
        .put(
            ArtifactInput {
                media_type: id("text/plain"),
                bytes: b"Original evidence".to_vec(),
                source: Some(reference("report")),
            },
            &context(),
            None,
        )
        .await
        .unwrap();
    let evidence = artifacts
        .evidence(&metadata.reference, id("line-1"), None, &context(), None)
        .await
        .unwrap();
    let mut registrations = vec![];
    for name in ["read", "write"] {
        let entry = f.registry.get(&id(name)).unwrap();
        registrations.push(ToolRegistration {
            compiled: entry.compiled.clone(),
            executor: if name == "write" {
                Arc::new(ArtifactTool {
                    inner: f.tools[1].clone(),
                    reference: metadata.reference.clone(),
                    evidence: evidence.clone(),
                })
            } else {
                entry.executor.clone()
            },
        });
    }
    bindings.tools = Some(Arc::new(ToolRegistry::new(scope(), registrations).unwrap()));
    bindings.artifacts = Some(artifacts.clone());
    bindings.model_exchange = Arc::new(
        ModelExchange::new(f.model.clone(), bindings.policy.clone())
            .with_route_inspector(
                Arc::new(RevokeArtifact {
                    policy: f.policy.clone(),
                    inner: f.base.inspector.clone(),
                }),
                Duration::from_secs(1),
            )
            .unwrap(),
    );
    let agent = create_agent(f.profile.clone(), bindings).unwrap();
    let handle = f.start(&agent).await;
    assert_eq!(f.outcome(&handle).await.result.status(), RunStatus::Failed);
    assert_eq!(f.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(f.model.calls.load(Ordering::SeqCst), 1);
    let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
    assert!(
        matches!(&saved.snapshot.tool_ledger[0].state,ToolCallState::Settled {result} if result.status==ToolResultStatus::Succeeded&&result.effect==ToolEffect::Applied&&result.effect_receipt_ref.is_some())
    );
    assert_eq!(
        artifacts
            .get(&metadata.reference, &context(), None)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
}
impl ToolExecutor for ArtifactTool {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            let mut result = self.inner.execute(args, context).await?;
            let ToolExecutionOutcome::Succeeded { value } = &result.outcome else {
                panic!("successful write fixture")
            };
            result.outcome = ToolExecutionOutcome::SucceededWithContent {
                value: value.clone(),
                content: vec![
                    InputContent::Artifact {
                        reference: self.reference.clone(),
                    },
                    InputContent::Evidence {
                        reference: self.evidence.clone(),
                    },
                ],
            };
            Ok(result)
        })
    }
}
#[tokio::test]
async fn artifact_references_bound_large_outputs_and_validation_failure_preserves_applied_receipts()
{
    for corrupt in [false, true] {
        let f = Fixture::new(
            vec![("write", object(json!({"query":"report"})))],
            Behavior::Success,
        );
        let mut bindings = f.bindings();
        let artifacts = Arc::new(
            ArtifactRuntime::new(
                Arc::new(MemoryArtifactStore::default()),
                bindings.policy.clone(),
                bindings.ids.clone(),
                ArtifactLimits::default(),
            )
            .unwrap(),
        );
        let original = "Original report evidence. ".repeat(1000);
        let metadata = artifacts
            .put(
                ArtifactInput {
                    media_type: id("text/plain"),
                    bytes: original.as_bytes().to_vec(),
                    source: Some(reference("report-source")),
                },
                &context(),
                None,
            )
            .await
            .unwrap();
        let evidence = artifacts
            .evidence(
                &metadata.reference,
                id("paragraph-1"),
                Some("Original report evidence.".into()),
                &context(),
                None,
            )
            .await
            .unwrap();
        let mut selected = metadata.reference.clone();
        if corrupt {
            selected.content_hash = id("sha256:wrong");
        }
        let mut registrations = vec![];
        for name in ["read", "write"] {
            let entry = f.registry.get(&id(name)).unwrap();
            registrations.push(ToolRegistration {
                compiled: entry.compiled.clone(),
                executor: if name == "write" {
                    Arc::new(ArtifactTool {
                        inner: f.tools[1].clone(),
                        reference: selected.clone(),
                        evidence: evidence.clone(),
                    })
                } else {
                    entry.executor.clone()
                },
            });
        }
        bindings.tools = Some(Arc::new(ToolRegistry::new(scope(), registrations).unwrap()));
        bindings.artifacts = Some(artifacts.clone());
        let agent = create_agent(f.profile.clone(), bindings).unwrap();
        let handle = f.start(&agent).await;
        let outcome = f.outcome(&handle).await;
        assert_eq!(f.tools[1].applied.load(Ordering::SeqCst), 1);
        let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
        let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[0].state else {
            panic!("known write result")
        };
        assert_eq!(result.effect, ToolEffect::Applied);
        let receipt = f
            .base
            .store
            .read_record(&scope(), result.effect_receipt_ref.as_ref().unwrap())
            .await
            .unwrap();
        assert_eq!(
            receipt.value()["receipt"]["private_receipt"],
            json!("only-for-storage")
        );
        if corrupt {
            assert_eq!(result.status, ToolResultStatus::Failed);
            assert!(result.content.is_empty());
            assert!(outcome.artifacts.is_empty());
        } else {
            assert_eq!(result.status, ToolResultStatus::Succeeded);
            assert_eq!(outcome.artifacts, vec![metadata.reference.clone()]);
            assert_eq!(
                artifacts
                    .get(&metadata.reference, &context(), None)
                    .await
                    .unwrap()
                    .bytes,
                original.as_bytes()
            );
            assert!(
                serde_json::to_vec(&f.model.requests.lock().unwrap()[1])
                    .unwrap()
                    .len()
                    < original.len()
            );
        }
        let replay = f.start(&agent).await;
        assert_eq!(f.outcome(&replay).await, outcome);
        assert_eq!(f.tools[1].applied.load(Ordering::SeqCst), 1);
    }
}

fn default_plans() -> Vec<(&'static str, JsonObject)> {
    vec![
        ("read", object(json!({"query":"first"}))),
        ("write", object(json!({"query":"second","limit":2}))),
    ]
}
fn observations(request: &ModelRequest) -> Vec<(&Id, &Value)> {
    request
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            ModelContent::ToolResult {
                provider_call_id,
                content,
            } => Some((provider_call_id, content)),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn an_agent_executes_two_calls_then_receives_only_the_safe_observations_and_original_arguments()
 {
    let fixture = Fixture::new(default_plans(), Behavior::Success);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(*fixture.order.lock().unwrap(), vec!["read", "write"]);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        fixture.tools[0].arguments.lock().unwrap()[0],
        object(json!({"query":"first","limit":10,"workspace_id":WORKSPACE}))
    );
    assert_eq!(
        fixture.tools[1].arguments.lock().unwrap()[0],
        object(json!({"query":"second","limit":2,"workspace_id":WORKSPACE}))
    );
    let requests = fixture.model.requests.lock().unwrap();
    for tool in &requests[0].tools {
        assert_eq!(
            tool.model_input_schema["properties"]
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>(),
            ["query".to_owned(), "limit".to_owned()]
                .into_iter()
                .collect()
        );
    }
    let calls: Vec<_> = requests[1]
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            ModelContent::ToolCall {
                provider_call_id,
                name,
                arguments,
            } => Some((provider_call_id, name, arguments)),
            _ => None,
        })
        .collect();
    assert_eq!(
        calls,
        vec![
            (
                &id("provider-0"),
                &id("read"),
                &object(json!({"query":"first"}))
            ),
            (
                &id("provider-1"),
                &id("write"),
                &object(json!({"query":"second","limit":2}))
            )
        ]
    );
    assert_eq!(
        observations(&requests[1]),
        vec![
            (
                &id("provider-0"),
                &json!({"status":"succeeded","effect":"not_applied","content":[{"type":"json","value":"read observation"}]})
            ),
            (
                &id("provider-1"),
                &json!({"status":"succeeded","effect":"applied","content":[{"type":"json","value":"write observation"}]})
            )
        ]
    );
}

#[tokio::test]
async fn unknown_invalid_and_denied_calls_return_errors_to_the_model_without_executing() {
    for case in 0..3 {
        let plan = match case {
            0 => ("unregistered", object(json!({"query":"x"}))),
            1 => (
                "read",
                object(json!({"query":"x","workspace_id":WORKSPACE})),
            ),
            _ => ("read", object(json!({"query":"x"}))),
        };
        let mut fixture = Fixture::new(vec![plan], Behavior::Success);
        fixture.profile.limits.max_repair_attempts = 1;
        if case == 2 {
            fixture.policy.mode.store(1, Ordering::SeqCst);
        }
        let agent = fixture.agent();
        let handle = fixture.start(&agent).await;
        let outcome = fixture.outcome(&handle).await;
        assert_eq!(outcome.result.status(), RunStatus::Succeeded);
        assert_eq!(outcome.usage.tool_attempts, 0);
        assert_eq!(outcome.usage.repair_attempts, if case == 2 { 0 } else { 1 });
        assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
        assert!(fixture.order.lock().unwrap().is_empty());
        let requests = fixture.model.requests.lock().unwrap();
        let results = observations(&requests[1]);
        assert_eq!(results.len(), 1);
        assert_ne!(results[0].1["status"], json!("succeeded"));
        assert_eq!(results[0].1["effect"], json!("not_applied"));
        assert!(results[0].1.get("error").is_some());
    }
}

#[tokio::test]
async fn an_applied_write_with_invalid_output_reaches_the_model_as_failure_and_is_not_replayed() {
    let fixture = Fixture::new(
        vec![("write", object(json!({"query":"x"})))],
        Behavior::InvalidOutput,
    );
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[0].state else {
        panic!("write result missing")
    };
    assert_eq!(result.status, ToolResultStatus::Failed);
    assert_eq!(result.effect, ToolEffect::Applied);
    let record = fixture
        .base
        .store
        .read_record(&scope(), result.effect_receipt_ref.as_ref().unwrap())
        .await
        .unwrap();
    assert_eq!(
        record.value()["receipt"],
        json!({"private_receipt":"only-for-storage","target":WORKSPACE})
    );
    {
        let requests = fixture.model.requests.lock().unwrap();
        assert_eq!(observations(&requests[1])[0].1["effect"], json!("applied"));
        assert_eq!(observations(&requests[1])[0].1["status"], json!("failed"));
    }
    let replay = fixture.start(&agent).await;
    assert_eq!(replay.run_id(), handle.run_id());
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn approval_waits_with_a_fixed_binding_before_later_tools_or_model_calls() {
    let fixture = Fixture::new(default_plans(), Behavior::Success);
    fixture.policy.mode.store(2, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(outcome.result.status(), RunStatus::Waiting);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert!(fixture.order.lock().unwrap().is_empty());
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    assert!(saved.snapshot.tool_ledger[0].call.bound_input_ref.is_some());
    assert!(saved.snapshot.tool_ledger[1].call.bound_input_ref.is_none());
    let wait = saved.snapshot.wait.as_ref().unwrap();
    assert!(matches!(wait.target, WaitTarget::Approval { .. }));
    assert_eq!(saved.session.active_run_id.as_ref(), Some(handle.run_id()));
}

#[tokio::test]
async fn approval_required_after_dispatch_reservation_becomes_a_fixed_agent_wait() {
    let fixture = Fixture::new(default_plans(), Behavior::Success);
    fixture.policy.mode.store(3, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(outcome.result.status(), RunStatus::Waiting);
    assert_eq!(fixture.policy.tool_checks.load(Ordering::SeqCst), 3);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert!(fixture.order.lock().unwrap().is_empty());
    assert!(
        fixture
            .tools
            .iter()
            .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
    );
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    let entry = &saved.snapshot.tool_ledger[0];
    let ToolCallState::ApprovalPending { attempt_id, .. } = &entry.state else {
        panic!("an unexecuted reserved call must remain pending approval")
    };
    let bound_ref = entry.call.bound_input_ref.as_ref().unwrap();
    let record = fixture
        .base
        .store
        .read_record(&scope(), bound_ref)
        .await
        .unwrap();
    let compiled = &fixture.registry.get(&id("read")).unwrap().compiled;
    let bound = BoundToolInput::restore(
        &record,
        compiled,
        &scope(),
        handle.run_id(),
        &entry.call,
        saved.snapshot.system_inputs.as_ref(),
    )
    .unwrap();
    assert_eq!(
        bound.execution_args(),
        &object(json!({"query":"first","limit":10,"workspace_id":WORKSPACE}))
    );
    assert_eq!(
        saved.snapshot.wait.as_ref().unwrap().target,
        WaitTarget::Approval {
            target: ApprovalTarget::Tool {
                call_id: entry.call.call_id.clone(),
                binding_digest: bound.binding_digest().clone(),
            },
        }
    );
    assert_eq!(saved.snapshot.usage.tool_attempts, 1);
    let reservations: Vec<_> = saved
        .snapshot
        .reservations
        .iter()
        .filter(|reservation| matches!(reservation.kind, ReservationKind::Tool { .. }))
        .collect();
    assert_eq!(reservations.len(), 1);
    assert_eq!(&reservations[0].attempt_id, attempt_id);
    assert_eq!(
        reservations[0].kind,
        ReservationKind::Tool {
            call_id: entry.call.call_id.clone()
        }
    );
    assert!(matches!(
        saved.snapshot.tool_ledger[1].state,
        ToolCallState::Planned {}
    ));
    assert!(saved.snapshot.tool_ledger[1].call.bound_input_ref.is_none());
    assert_eq!(saved.session.active_run_id.as_ref(), Some(handle.run_id()));
}

#[tokio::test]
async fn uncertain_write_waits_and_does_not_run_the_later_tool_or_next_model() {
    let fixture = Fixture::new(
        vec![
            ("write", object(json!({"query":"x"}))),
            ("read", object(json!({"query":"y"}))),
        ],
        Behavior::Unknown,
    );
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(outcome.result.status(), RunStatus::Waiting);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[0].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    assert!(matches!(
        saved.snapshot.tool_ledger[0].state,
        ToolCallState::Unknown { .. }
    ));
    assert!(matches!(
        saved.snapshot.wait.as_ref().unwrap().target,
        WaitTarget::External { .. }
    ));
    assert!(!outcome.unresolved_effects.is_empty());
}

#[tokio::test]
async fn using_the_last_model_slot_still_executes_its_saved_tool_plan_before_exhaustion() {
    let mut fixture = Fixture::new(default_plans(), Behavior::Success);
    fixture.profile.limits.max_model_calls = 1.try_into().unwrap();
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::ModelCalls
        }
    );
    assert_eq!(outcome.usage.tool_attempts, 2);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert!(fixture.base.store.load(&scope(),handle.run_id()).await.unwrap().snapshot.tool_ledger.iter().all(|entry|matches!(&entry.state,ToolCallState::Settled{result} if result.status==ToolResultStatus::Succeeded)));
}

#[tokio::test]
async fn tool_budget_exhaustion_settles_the_unstarted_plan_without_a_second_model_call() {
    let mut fixture = Fixture::new(default_plans(), Behavior::Success);
    fixture.profile.limits.max_tool_attempts = 1;
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::ToolAttempts
        }
    );
    assert_eq!(fixture.tools[0].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[1].state else {
        panic!("unstarted call remains orphaned")
    };
    assert_eq!(result.effect, ToolEffect::NotApplied);
    assert_ne!(result.status, ToolResultStatus::Succeeded);
}

#[tokio::test]
async fn cancelling_an_entered_write_retains_its_unknown_effect_and_closes_unstarted_calls() {
    let fixture = Fixture::new(
        vec![
            ("write", object(json!({"query":"x"}))),
            ("read", object(json!({"query":"y"}))),
        ],
        Behavior::Pending,
    );
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    tokio::time::timeout(Duration::from_secs(5), fixture.tools[1].entered.notified())
        .await
        .expect("write must enter before cancellation is requested");
    let receipt = completed(handle.cancel(id("stop"), &context()).await.unwrap());
    assert!(receipt.processed_segment_id.is_none());
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(outcome.result.status(), RunStatus::Cancelled);
    assert!(!outcome.unresolved_effects.is_empty());
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    assert!(matches!(
        saved.snapshot.tool_ledger[0].state,
        ToolCallState::Unknown { .. }
    ));
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[1].state else {
        panic!("unstarted call not closed")
    };
    assert_eq!(result.effect, ToolEffect::NotApplied);
    assert_eq!(fixture.tools[0].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn the_run_deadline_keeps_an_entered_write_unknown_and_closes_the_remaining_plan() {
    let mut fixture = Fixture::new(
        vec![
            ("write", object(json!({"query":"x"}))),
            ("read", object(json!({"query":"y"}))),
        ],
        Behavior::Pending,
    );
    fixture.profile.limits.max_elapsed_ms = 20.try_into().unwrap();
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    );
    assert!(!outcome.unresolved_effects.is_empty());
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    assert!(matches!(
        saved.snapshot.tool_ledger[0].state,
        ToolCallState::Unknown { .. }
    ));
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[1].state else {
        panic!("unstarted call not closed")
    };
    assert_eq!(result.effect, ToolEffect::NotApplied);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[0].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

struct RepairOnce(AtomicUsize);
impl Verifier for RepairOnce {
    fn definition(&self) -> VerifierDefinition {
        VerifierDefinition {
            verifier_ref: reference("review"),
            criteria_ref: reference("review-criteria"),
            criteria: "Synthetic revision decision for effect preservation testing.".into(),
            configuration: Default::default(),
        }
    }
    fn verify<'a>(
        &'a self,
        input: &'a VerificationInput,
        _: &'a VerifierContext<'a>,
    ) -> PortFuture<'a, VerificationDecision> {
        Box::pin(async move {
            assert!(!input.candidate.evidence_message_ids.is_empty());
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(VerificationDecision::Revise {
                    feedback: "Revise the explanation using the completed operation.".into(),
                })
            } else {
                Ok(VerificationDecision::Pass {})
            }
        })
    }
}
#[tokio::test]
async fn verifier_repair_does_not_repeat_an_applied_business_write() {
    let mut fixture = Fixture::new(
        vec![("write", object(json!({"query":"apply change"})))],
        Behavior::Success,
    );
    fixture.profile.completion_policy = CompletionPolicy::Verified {
        verifier_ref: reference("review"),
    };
    fixture.profile.limits.max_repair_attempts = 1;
    let verifier = Arc::new(RepairOnce(AtomicUsize::new(0)));
    let mut bindings = fixture.bindings();
    bindings.verification = Some(Arc::new(
        VerificationRuntime::new(
            scope(),
            vec![],
            vec![verifier.clone()],
            VerificationLimits::default(),
        )
        .unwrap(),
    ));
    let agent = create_agent(fixture.profile.clone(), bindings).unwrap();
    let handle = fixture.start(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::Verified
        }
    );
    assert_eq!(outcome.usage.repair_attempts, 1);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 3);
    assert_eq!(verifier.0.load(Ordering::SeqCst), 2);
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    assert_eq!(saved.snapshot.tool_ledger.len(), 1);
    assert!(
        matches!(&saved.snapshot.tool_ledger[0].state,ToolCallState::Settled{result} if result.effect==ToolEffect::Applied&&result.effect_receipt_ref.is_some())
    );
    let replay = fixture.start(&agent).await;
    assert_eq!(fixture.outcome(&replay).await, outcome);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn compacted_source_history_keeps_immutable_lineage_and_rechecks_current_acl() {
    let mut f = Fixture::new(
        vec![("read", object(json!({"query":"chunk"})))],
        Behavior::Success,
    );
    f.model.rounds.store(3, Ordering::SeqCst);
    let (mut bindings, _) = long_bindings(&f, 3500);
    let mut source_fixture = source_support::Fixture::new();
    let source = source_fixture.add(
        "records",
        ContextTrigger::RunStart,
        true,
        vec![source_support::Reply::Ready, source_support::Reply::Empty],
    );
    f.profile.context_sources = Some(source_fixture.bindings.clone());
    let registry = Arc::new(
        ContextSourceRegistry::new(
            scope(),
            vec![ContextSourceRegistration {
                selection: source.selection.clone(),
                definition: source.definition.clone(),
                source: source.clone(),
            }],
        )
        .unwrap(),
    );
    bindings.context_sources = Some(Arc::new(
        ContextSourceRuntime::new(
            bindings.state.clone(),
            bindings.policy.clone(),
            bindings.clock.clone(),
            bindings.ids.clone(),
            registry,
            source_fixture.estimator.clone(),
        )
        .unwrap(),
    ));
    bindings.context_token_estimator = Some(source_fixture.estimator.clone());
    let summary = Arc::new(Summary {
        calls: AtomicUsize::new(0),
        requests: Mutex::new(vec![]),
        bad: false,
    });
    bindings.context_runtime = Some(Arc::new(
        ContextRuntime::new(
            scope(),
            Arc::new(BoundedContextStrategy),
            Some(ContextCompactor::Host {
                definition: reference("summary"),
                compressor: summary.clone(),
            }),
            ContextRewriteLimits::default(),
        )
        .unwrap(),
    ));
    let agent = create_agent(f.profile.clone(), bindings).unwrap();
    let handle = f.start(&agent).await;
    assert_eq!(
        f.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert!(summary.calls.load(Ordering::SeqCst) > 0);
    let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
    let record = f
        .base
        .store
        .read_record(
            &scope(),
            saved.snapshot.context_revision_ref.as_ref().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        record.value()["source_lineage"],
        json!([{
            "run_id":handle.run_id(), "batch_ref":saved.snapshot.context_batches[0]
        }])
    );
    let image = serde_json::to_value(f.base.store.export_checkpoint(&scope()).unwrap()).unwrap();
    for corruption in [
        "lineage",
        "boundary",
        "fragments",
        "source-selection",
        "tool-set",
        "cached-tool",
        "cached-provider-contract",
        "compiler",
        "fingerprint",
    ] {
        assert_prepared_corruption_rejected(&image, corruption);
    }
    // The second Run has an empty fresh selection, so only the summary's old
    // source can cause this denial; no second provider inference is allowed.
    source.revoked.store(true, Ordering::SeqCst);
    let before = f.model.calls.load(Ordering::SeqCst);
    let second = completed(
        agent
            .start(request("next-source-run"), context())
            .await
            .unwrap(),
    );
    assert_eq!(f.outcome(&second).await.result.status(), RunStatus::Failed);
    assert_eq!(f.model.calls.load(Ordering::SeqCst), before);
    assert!(
        source
            .use_requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request.derived
                && request.consumer_run_id == *second.run_id()
                && request.request.run_id == *handle.run_id())
    );
}

struct PresenceCompiler(AtomicUsize);
impl ProviderToolSchemaCompiler for PresenceCompiler {
    fn reference(&self) -> VersionedRef {
        reference("presence-compiler")
    }
    fn compile(
        &self,
        tool: &ModelTool,
        _: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let mut properties = serde_json::Map::new();
        let mut fields = vec![];
        for name in tool.model_input_schema["properties"]
            .as_object()
            .unwrap()
            .keys()
        {
            let wire_name = format!("p_{name}");
            properties.insert(wire_name.clone(), json!({"type":"object","properties":{"present":{"type":"boolean"},"value":{}},"required":["present","value"],"additionalProperties":false}));
            fields.push(ArgumentFieldMapping {
                canonical_name: name.clone(),
                wire_name,
                encoding: ArgumentValueEncoding::Presence {
                    present_key: "present".into(),
                    value_key: "value".into(),
                },
            });
        }
        let required: Vec<_> = properties.keys().cloned().collect();
        Ok(ProviderToolProjection {
            wire_tool: ModelTool {
                name: id(&format!("wire_{}", tool.name)),
                description: tool.description.clone(),
                model_input_schema: json!({"type":"object","properties":properties,"required":required,"additionalProperties":false}),
            },
            decode_plan: ArgumentDecodePlan::Fields { fields },
        })
    }
}
struct WireModel {
    binding: ModelPortBinding,
    compiler: Arc<PresenceCompiler>,
    requests: Mutex<Vec<ModelRequest>>,
    retry_first: bool,
    unadvertised_name: bool,
}
impl ModelPort for WireModel {
    fn binding(&self) -> ModelPortBinding {
        self.binding.clone()
    }
    fn tool_schema_compiler(&self) -> Arc<dyn ProviderToolSchemaCompiler> {
        self.compiler.clone()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let mut requests = self.requests.lock().unwrap();
        let index = requests.len();
        requests.push(request.clone());
        drop(requests);
        if self.retry_first && index == 0 {
            return Box::pin(stream::iter([Ok(ModelEvent::ResponseError {
                kind: ModelFailureKind::Transport,
                metadata: Default::default(),
            })]));
        }
        if index == usize::from(self.retry_first) {
            let (name, args) = if self.unadvertised_name {
                ("read", json!({"query":"records"}))
            } else {
                (
                    "wire_read",
                    json!({"p_query":{"present":true,"value":"records"},"p_limit":{"present":false,"value":null}}),
                )
            };
            Box::pin(stream::iter([
                Ok(ModelEvent::ToolArgumentsDelta {
                    index: 0,
                    provider_call_id: Some("wire-call".into()),
                    name: Some(name.into()),
                    delta: args.to_string(),
                }),
                Ok(ModelEvent::ResponseCompleted {
                    finish: ModelFinish::ToolCalls,
                    metadata: Default::default(),
                    continuation: vec![],
                }),
            ]))
        } else {
            Box::pin(stream::iter([
                Ok(ModelEvent::TextDelta {
                    text: "Completed".into(),
                }),
                Ok(ModelEvent::ResponseCompleted {
                    finish: ModelFinish::Stop,
                    metadata: Default::default(),
                    continuation: vec![],
                }),
            ]))
        }
    }
}
#[tokio::test]
async fn provider_presence_codec_executes_canonical_inputs_and_reencodes_history_without_recompiling_retry()
 {
    for unadvertised in [false, true] {
        let mut f = Fixture::new(vec![], Behavior::Success);
        f.profile.limits.max_recovery_attempts = 1;
        f.profile.limits.max_repair_attempts = 1;
        let mut bindings = f.bindings();
        let compiler = Arc::new(PresenceCompiler(AtomicUsize::new(0)));
        let model = Arc::new(WireModel {
            binding: f.model.binding(),
            compiler: compiler.clone(),
            requests: Mutex::new(vec![]),
            retry_first: !unadvertised,
            unadvertised_name: unadvertised,
        });
        bindings.model_exchange = Arc::new(
            ModelExchange::new(model.clone(), bindings.policy.clone())
                .with_route_inspector(f.base.inspector.clone(), Duration::from_secs(5))
                .unwrap()
                .with_retry_policy(ModelRetryPolicy {
                    max_retries: 1,
                    backoff_ms: 0,
                }),
        );
        let agent = create_agent(f.profile.clone(), bindings).unwrap();
        let handle = f.start(&agent).await;
        assert_eq!(
            f.outcome(&handle).await.result.status(),
            RunStatus::Succeeded
        );
        assert_eq!(
            f.tools[0].calls.load(Ordering::SeqCst),
            usize::from(!unadvertised)
        );
        assert_eq!(f.tools[1].calls.load(Ordering::SeqCst), 0);
        let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
        assert_eq!(saved.snapshot.prepared_steps.len(), 2);
        assert_eq!(compiler.0.load(Ordering::SeqCst), 4);
        let requests = model.requests.lock().unwrap();
        assert!(
            requests.iter().all(|request| request
                .tools
                .iter()
                .all(|tool| tool.model_input_schema["properties"]
                    .get("workspace_id")
                    .is_none()))
        );
        if !unadvertised {
            assert_eq!(
                saved.snapshot.model_ledger[0].prepared_step_ref,
                saved.snapshot.model_ledger[1].prepared_step_ref
            );
            assert_ne!(
                saved.snapshot.model_ledger[1].prepared_step_ref,
                saved.snapshot.model_ledger[2].prepared_step_ref
            );
            assert_eq!(
                f.tools[0].arguments.lock().unwrap()[0],
                object(json!({"query":"records","limit":10,"workspace_id":WORKSPACE}))
            );
            assert!(requests.last().unwrap().messages.iter().flat_map(|message| &message.content).any(|content|
                matches!(content, ModelContent::ToolCall { name, arguments, .. } if name == &id("wire_read") && arguments == &object(json!({"p_query":{"present":true,"value":"records"},"p_limit":{"present":false,"value":null}})))));
            assert!(
                saved.snapshot.tool_ledger[0]
                    .call
                    .provider_arguments
                    .as_ref()
                    .unwrap()
                    .compiled_contract_ref
                    .is_some()
            );
        } else {
            let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[0].state else {
                panic!("settled unknown call")
            };
            assert_eq!(result.error.as_ref().unwrap().code, id("unknown_tool"));
            assert_eq!(saved.snapshot.usage.repair_attempts, 1);
        }
    }
}

fn checkpoint_record_mut<'a>(image: &'a mut Value, reference: &Value) -> &'a mut Value {
    &mut image["records"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|record| &record["reference"] == reference)
        .unwrap()["value"]
}
fn replace_checkpoint_reference(value: &mut Value, old: &Value, new: &Value) {
    if value == old {
        *value = new.clone();
        return;
    }
    match value {
        Value::Array(values) => values
            .iter_mut()
            .for_each(|value| replace_checkpoint_reference(value, old, new)),
        Value::Object(values) => values
            .values_mut()
            .for_each(|value| replace_checkpoint_reference(value, old, new)),
        _ => {}
    }
}
fn rehash_checkpoint(image: &mut Value) {
    for _ in 0..16 {
        let changes: Vec<_> = image["records"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|record| {
                let digest = serde_json::to_value(canonical_digest(&record["value"])).unwrap();
                if record["reference"]["digest"] == digest {
                    return None;
                }
                let old = record["reference"].clone();
                let mut new = old.clone();
                new["digest"] = digest;
                Some((old, new))
            })
            .collect();
        if changes.is_empty() {
            return;
        }
        for (old, new) in changes {
            replace_checkpoint_reference(image, &old, &new);
        }
    }
    panic!("checkpoint reference cycle");
}
fn assert_prepared_corruption_rejected(image: &Value, corruption: &str) {
    StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(image))
        .unwrap();
    let mut changed = image.clone();
    let root_ref = changed["runs"][0]["snapshot"]["prepared_steps"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()
        .clone();
    let root = checkpoint_record_mut(&mut changed, &root_ref).clone();
    match corruption {
        "artifacts" => {
            checkpoint_record_mut(&mut changed, &root["context_projection"])["provenance"]["artifacts"] =
                json!([])
        }
        "lineage" => {
            checkpoint_record_mut(&mut changed, &root["context_projection"])["provenance"]["source_lineage"] =
                json!([])
        }
        "boundary" => {
            let projection = checkpoint_record_mut(&mut changed, &root["context_projection"]);
            projection["provenance"]["through_sequence"] = json!(0);
            projection["provenance"]["source_lineage"] = json!([]);
        }
        "fragments" => {
            checkpoint_record_mut(&mut changed, &root["context_projection"])["provenance"]["fragments"] =
                json!([])
        }
        "source-selection" => {
            let projection = checkpoint_record_mut(&mut changed, &root["context_projection"]);
            projection["provenance"]["source_batches"] = json!([]);
            projection["provenance"]["fragments"] = json!([]);
        }
        "tool-set" => {
            checkpoint_record_mut(&mut changed, &root["tool_set"])["entries"][0]["manifest"]["compiled_digest"] =
                json!(canonical_digest(&json!("different contract")))
        }
        "cached-tool" => {
            checkpoint_record_mut(&mut changed, &root["tool_set"])["entries"][0]["compiled"]["descriptor"]
                ["description"] = json!("changed cached definition");
        }
        "cached-provider-contract" => {
            checkpoint_record_mut(&mut changed, &root["compiled_tools"][0])["data"]["wire_tool"]
                ["description"] = json!("changed cached wire definition");
        }
        "compiler" => {
            let contract = checkpoint_record_mut(&mut changed, &root["compiled_tools"][0]);
            contract["data"]["canonical_name"] = json!("unregistered");
            contract["digest"] = json!(canonical_digest(&contract["data"]));
        }
        "fingerprint" => {
            checkpoint_record_mut(&mut changed, &root_ref)["projection_fingerprint"] =
                json!(canonical_digest(&json!("different input")))
        }
        _ => panic!("unknown corruption"),
    }
    rehash_checkpoint(&mut changed);
    assert!(
        StateStoreCheckpoint::from_json(
            &changed.to_string(),
            &scope(),
            &canonical_digest(&changed)
        )
        .is_err(),
        "accepted {corruption} omission or mismatch with all containing record hashes recomputed"
    );
}

/// Deterministic work clock: every clock read advances execution time, while
/// sleep waits cooperatively. Ready-only model/store work must let renewals run.
struct WorkClock(std::sync::atomic::AtomicU64);
impl Clock for WorkClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        let tick = self.0.fetch_add(10, Ordering::SeqCst);
        Ok(ClockReading {
            utc_ms: 1000 + tick as i64,
            monotonic_ms: tick,
        })
    }
    fn sleep_until<'a>(&'a self, deadline: u64) -> PortFuture<'a, ()> {
        Box::pin(async move {
            while self.0.load(Ordering::SeqCst) < deadline {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
    }
}
#[tokio::test]
async fn ready_only_execution_yields_to_lease_renewal_between_guarded_operations() {
    let fixture = Fixture::new(
        vec![("read", object(json!({"query":"cached records"})))],
        Behavior::Success,
    );
    let mut bindings = fixture.bindings();
    bindings.clock = Arc::new(WorkClock(std::sync::atomic::AtomicU64::new(0)));
    bindings.settings.lease_ttl_ms = 300;
    bindings.settings.heartbeat_interval_ms = 30;
    let agent = create_agent(fixture.profile.clone(), bindings).unwrap();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.tools[0].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
    assert!(
        fixture
            .base
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .usage
            .elapsed_ms
            > 300
    );
}

#[tokio::test]
async fn execution_stop_preserves_an_uncertain_write_and_the_unstarted_remainder() {
    let f = Fixture::new(
        vec![
            ("write", object(json!({"query":"apply"}))),
            ("read", object(json!({"query":"inspect"}))),
        ],
        Behavior::Pending,
    );
    let agent = f.agent();
    let handle = f.start(&agent).await;
    tokio::time::timeout(Duration::from_secs(5), f.tools[1].entered.notified())
        .await
        .unwrap();
    assert_eq!(
        completed(
            handle
                .stop_execution(InterruptionCause::HostShutdown, &context())
                .await
                .unwrap()
        ),
        ExecutionStopReceipt::Requested
    );
    let outcome = f.outcome(&handle).await;
    assert_eq!(outcome.result.status(), RunStatus::Interrupted);
    assert_eq!(outcome.unresolved_effects.len(), 1);
    assert_eq!(f.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(f.tools[0].calls.load(Ordering::SeqCst), 0);
    let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
    assert!(matches!(
        saved.snapshot.tool_ledger[0].state,
        ToolCallState::Unknown { .. }
    ));
    assert!(matches!(
        saved.snapshot.tool_ledger[1].state,
        ToolCallState::Planned { .. }
    ));
    let checkpoint = f.base.store.export_checkpoint(&scope()).unwrap();
    StateStoreCheckpoint::from_json(
        &serde_json::to_string(&checkpoint).unwrap(),
        &scope(),
        &checkpoint.digest(),
    )
    .unwrap();
    let mut image = serde_json::to_value(&checkpoint).unwrap();
    let old = serde_json::to_value(&outcome.unresolved_effects).unwrap();
    replace_checkpoint_reference(&mut image, &old, &json!([]));
    rehash_checkpoint(&mut image);
    assert!(
        StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(&image))
            .is_err()
    );
}

struct DependentModel {
    calls: AtomicUsize,
}
impl ModelPort for DependentModel {
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
        let step = self.calls.fetch_add(1, Ordering::SeqCst);
        let observed = observations(request);
        assert_eq!(observed.len(), step);
        let (event, finish) = match step {
            0 => (
                ModelEvent::ToolArgumentsDelta {
                    index: 0,
                    provider_call_id: Some("search-call".into()),
                    name: Some("search".into()),
                    delta: json!({"query":"annual report"}).to_string(),
                },
                ModelFinish::ToolCalls,
            ),
            1 => {
                assert_eq!(observed[0].0, &id("search-call"));
                let key = observed[0].1["content"][0]["value"].as_str().unwrap();
                (
                    ModelEvent::ToolArgumentsDelta {
                        index: 0,
                        provider_call_id: Some("read-call".into()),
                        name: Some("read".into()),
                        delta: json!({"query":key}).to_string(),
                    },
                    ModelFinish::ToolCalls,
                )
            }
            2 => {
                assert_eq!(observed[1].0, &id("read-call"));
                let body = observed[1].1["content"][0]["value"].as_str().unwrap();
                (
                    ModelEvent::TextDelta { text: body.into() },
                    ModelFinish::Stop,
                )
            }
            _ => panic!("unexpected extra model call"),
        };
        Box::pin(stream::iter(vec![
            Ok(event),
            Ok(ModelEvent::ResponseCompleted {
                finish,
                metadata: Default::default(),
                continuation: vec![],
            }),
        ]))
    }
}
struct DependentTool {
    expected_query: String,
    result: String,
    calls: AtomicUsize,
}
impl ToolExecutor for DependentTool {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        _: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            assert_eq!(args.get("query"), Some(&json!(self.expected_query)));
            assert_eq!(args.get("workspace_id"), Some(&json!(WORKSPACE)));
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: json!(self.result),
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}
#[tokio::test]
async fn search_observation_drives_a_later_read_call_before_the_final_answer() {
    let fixture = Fixture::new(vec![], Behavior::Success);
    let key = format!("report-{}", RandomIdSource.next_id().unwrap());
    let body = format!("document-{}", RandomIdSource.next_id().unwrap());
    let search = Arc::new(DependentTool {
        expected_query: "annual report".into(),
        result: key.clone(),
        calls: AtomicUsize::new(0),
    });
    let read = Arc::new(DependentTool {
        expected_query: key,
        result: body.clone(),
        calls: AtomicUsize::new(0),
    });
    let model = Arc::new(DependentModel {
        calls: AtomicUsize::new(0),
    });
    let mut bindings = fixture.bindings();
    bindings.model_exchange = Arc::new(
        ModelExchange::new(model.clone(), bindings.policy.clone())
            .with_route_inspector(fixture.base.inspector.clone(), Duration::from_secs(1))
            .unwrap(),
    );
    let mut profile = fixture.profile.clone();
    profile.tools.clear();
    let mut tools = vec![];
    for (name, executor) in [("search", search.clone()), ("read", read.clone())] {
        let mut descriptor = fixture
            .registry
            .get(&id("read"))
            .unwrap()
            .compiled
            .descriptor()
            .clone();
        descriptor.tool = reference(name);
        descriptor.name = id(name);
        tools.push(ToolRegistration {
            compiled: SchemaCompiler::new()
                .compile(descriptor, &fixture.inputs)
                .unwrap(),
            executor,
        });
        profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
            tool_id: id(name),
            version: id("1"),
            bindings: None,
            config: None,
        }));
    }
    bindings.tools = Some(Arc::new(ToolRegistry::new(scope(), tools).unwrap()));
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.start(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(outcome.output, vec![InputContent::Text { text: body }]);
    assert_eq!(model.calls.load(Ordering::SeqCst), 3);
    assert_eq!(search.calls.load(Ordering::SeqCst), 1);
    assert_eq!(read.calls.load(Ordering::SeqCst), 1);
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    assert_eq!(saved.snapshot.tool_ledger.len(), 2);
    assert_ne!(
        saved.snapshot.tool_ledger[0].call.model_request_id,
        saved.snapshot.tool_ledger[1].call.model_request_id
    );
}
