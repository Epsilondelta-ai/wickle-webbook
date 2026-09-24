// Real SQLite and budgeted context compaction with synthetic model and inspector.
// This consumer makes no provider network calls and does not test a production data service.
use futures_util::{TryStreamExt, stream};
use serde_json::json;
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;

use wickle_model_router::PolicyModelRouter;
use wickle_state_sqlite::SqliteStateStore;

fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn completed<T>(value: Guarded<T>) -> Result<T, Box<dyn std::error::Error>> {
    match value {
        Guarded::Completed(value) => Ok(value),
        Guarded::ApprovalRequired(_) => Err("unexpected approval".into()),
    }
}

struct Inspector;
impl ModelRouteInspector for Inspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        _: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        // Fixture echo only: production inspectors must fetch authoritative metadata.
        Box::pin(async move {
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: ModelRouteAvailability::Available,
                model_id: Some(route.model_id.clone()),
                model_version: Some(route.model_version.clone()),
                deployment_revision: None,
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id("synthetic-inspection"),
            })
        })
    }
}
struct Estimate;
impl ModelTokenEstimator for Estimate {
    fn estimate(&self, _: &ModelRequest) -> Result<u64, ContractError> {
        // Deliberately synthetic estimate for this fixed fixture, not a tokenizer.
        Ok(512)
    }
}

fn routing(scope: &Scope) -> Result<RoutingSnapshot, ContractError> {
    let capabilities = ModelCapabilities {
        revision: id("features"),
        features: BTreeSet::from([id("text"), id("tool_calling")]),
        options_schema: json!({"type":"object","additionalProperties":false}),
        context_window: 4096.try_into().unwrap(),
        max_output_tokens: 512.try_into().unwrap(),
    };
    let model = ModelDefinition {
        model_key: id("synthetic"),
        family: id("synthetic"),
        provider: id("synthetic"),
        model_id: id("fixture-model"),
        model_version: id("release-1"),
        version_semantics: VersionSemantics::Pinned,
        lifecycle: ModelLifecycle::Active,
        capabilities: capabilities.clone(),
        evidence: vec![],
    };
    let mut binding = ModelBinding {
            default_options: Default::default(),
        binding: reference("primary"),
        model: model.reference(),
        requested_model: model.model_id.clone(),
        adapter: reference("synthetic-adapter"),
        connection_ref: reference("synthetic-connection"),
        target: JsonObject::new(),
        target_schema: json!({"type":"object","additionalProperties":false}),
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        deployment_revision: None,
        version_semantics: VersionSemantics::Pinned,
        capabilities,
        support: ModelSupportStatus::ContractTested,
        evidence: vec![],
    };
    binding.evidence.push(ModelValidationEvidence {
        kind: ModelValidationKind::ContractTest,
        binding_digest: binding.contract_digest(&model)?,
        checked_at_ms: 1000,
        evidence_ref: id("synthetic-contract-test"),
        passed: true,
    });
    RoutingSnapshot::new(
        ModelCatalogSnapshot {
            revision: id("catalog-1"),
            scope: scope.clone(),
            models: vec![model],
            bindings: vec![binding],
            aliases: vec![],
        },
        RoutingPolicy {
            revision: id("policy-1"),
            scope: scope.clone(),
            rules: vec![RoutingRule {
                model_binding: id("primary"),
                purpose: ModelPurpose::Agent,
                primary: reference("primary"),
                fallbacks: vec![],
                fallback_on: vec![],
                version_policy: VersionPolicy::RequirePinned,
                min_support: ModelSupportStatus::ContractTested,
            }],
        },
    )
}

struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        request: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(request.version.clone().unwrap_or_else(|| id("1"))),
                    ..request.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!(request.id)),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: None,
                hook_position: None,
                exports: vec![],
            })
        })
    }
}
struct Policy;
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        _: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async { Ok(PolicyDecision::Allow {}) })
    }
}
struct Reader(AtomicUsize);
impl ToolExecutor for Reader {
    fn execute<'a>(
        &'a self,
        _: &'a JsonObject,
        _: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            let index = self.0.fetch_add(1, Ordering::SeqCst);
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: json!(format!("record-{index}: {}", "detail ".repeat(500))),
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}
struct Metadata;
impl ProfileResolver for Metadata {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        scope: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            let mut metadata = Catalog.resolve(reference, scope).await?;
            if reference.kind == ComponentKind::Tool {
                metadata.model_name = Some(id("read_record"));
            }
            Ok(metadata)
        })
    }
}
struct Model {
    agent: AtomicUsize,
    compaction: AtomicUsize,
}
impl ModelPort for Model {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: id("synthetic"),
            adapter: reference("synthetic-adapter"),
            connection_ref: reference("synthetic-connection"),
        }
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let events = if request.purpose == ModelPurpose::Compaction {
            self.compaction.fetch_add(1, Ordering::SeqCst);
            vec![ModelEvent::TextDelta{text:"Earlier complete record reads are summarized; original records remain in storage.".into()},ModelEvent::ResponseCompleted{finish:ModelFinish::Stop,metadata:Default::default(),continuation:vec![]}]
        } else {
            let index = self.agent.fetch_add(1, Ordering::SeqCst);
            if index < 3 {
                vec![
                    ModelEvent::ToolArgumentsDelta {
                        index: 0,
                        provider_call_id: Some(format!("read-{index}")),
                        name: Some("read_record".into()),
                        delta: "{}".into(),
                    },
                    ModelEvent::ResponseCompleted {
                        finish: ModelFinish::ToolCalls,
                        metadata: Default::default(),
                        continuation: vec![],
                    },
                ]
            } else {
                vec![
                    ModelEvent::TextDelta {
                        text: "Records processed".into(),
                    },
                    ModelEvent::ResponseCompleted {
                        finish: ModelFinish::Stop,
                        metadata: Default::default(),
                        continuation: vec![],
                    },
                ]
            }
        };
        Box::pin(stream::iter(events.into_iter().map(Ok)))
    }
}
fn make_agent(
    profile: AgentProfile,
    context: &ExecutionContext,
    store: Arc<SqliteStateStore>,
    reader: Arc<Reader>,
    model: Arc<Model>,
    policy: Arc<PolicyGate>,
) -> Result<Agent, ContractError> {
    let snapshot = routing(&context.data.scope)?;
    let mut routes = snapshot.policy().clone();
    let mut auxiliary = routes.rules[0].clone();
    auxiliary.purpose = ModelPurpose::Compaction;
    routes.rules.push(auxiliary);
    let router = Arc::new(PolicyModelRouter::new(RoutingSnapshot::new(
        snapshot.catalog().clone(),
        routes,
    )?)?);
    let inputs = SystemInputRegistry::new(vec![])?;
    let compiled=SchemaCompiler::new().compile(ToolDescriptor{tool:reference("read"),name:id("read_record"),description:"Read the next synthetic record".into(),input_schema:json!({"type":"object","properties":{},"required":[],"additionalProperties":false}),agent_parameters:vec![],system_bindings:None,output_schema:json!({"type":"string"}),side_effect:ToolSideEffect::ReadOnly,concurrency:ToolConcurrency::Serial,retry:ToolRetryPolicy::Never,reconcile:false,max_output_bytes:16384.try_into().unwrap()},&inputs)?;
    let runtime = Arc::new(ContextRuntime::new(
        context.data.scope.clone(),
        Arc::new(BoundedContextStrategy),
        Some(ContextCompactor::Model(ModelCompactorConfig {
            model_binding: id("primary"),
            options: None,
            max_output_tokens: 128.try_into().unwrap(),
        })),
        ContextRewriteLimits::default(),
    )?);
    create_agent(
        profile,
        AgentBindings {
            interruption_policy: None,
            scope: context.data.scope.clone(),
            state: store,
            policy: policy.clone(),
            profile_resolver: Arc::new(Metadata),
            model_exchange: Arc::new(
                ModelExchange::new(model, policy)
                    .with_route_inspector(Arc::new(Inspector), Duration::from_secs(1))?,
            ),
            router,
            host_instructions: vec![
                "Preserve the current request and complete Tool observations.".into(),
            ],
            system_inputs: inputs,
            tools: Some(Arc::new(ToolRegistry::new(
                context.data.scope.clone(),
                vec![ToolRegistration {
                    compiled,
                    executor: reader,
                }],
            )?)),
            system_input_resolver: None,
            external_receipt_verifier: None,
            hooks: None,
            components: None,
            context_sources: None,
            context_token_estimator: None,
            context_runtime: Some(runtime), verification: None,
            skills: None,
            artifacts: None,
            clock: Arc::new(SystemClock::new()),
            ids: Arc::new(RandomIdSource),
            token_estimator: Arc::new(Estimate),
            settings: AgentSettings {
                require_durable: true,
                max_output_tokens: 128.try_into().unwrap(),
                projection_limits: ProjectionLimits {
                    max_bytes: 6500,
                    max_items: 1024,
                },
                ..Default::default()
            },
        },
    )
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("example"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id("reader"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    );
    let policy = Arc::new(PolicyGate::new(Arc::new(Policy), Duration::from_secs(1))?);
    let path = std::env::temp_dir().join(format!(
        "wickle-compaction-consumer-{}.sqlite3",
        RandomIdSource.next_id()?
    ));
    let store = Arc::new(SqliteStateStore::open(&path)?);
    let profile = AgentProfile::from_json(
        r#"{"schema_version":"wickle.agent-profile.v1","agent_id":"example","version":"1","name":"Context example","description":"Bounded conversation compaction","instructions":{"text":"Read the requested records."},"model_binding":"primary","tools":[{"tool_id":"read","version":"1"}],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},"limits":{"max_model_calls":8,"max_tool_attempts":3,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}}"#,
    )?;
    let reader = Arc::new(Reader(AtomicUsize::new(0)));
    let model = Arc::new(Model {
        agent: AtomicUsize::new(0),
        compaction: AtomicUsize::new(0),
    });
    let agent = make_agent(
        profile.clone(),
        &context,
        store.clone(),
        reader.clone(),
        model.clone(),
        policy.clone(),
    )?;
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Read three records and retain their context.".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: Default::default(),
        max_output_tokens: None,
        output_contract: None,
    };
    let handle = completed(agent.start(request.clone(), context.clone()).await?)?;
    let outcome = completed(handle.outcome(&context).await?)?;
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(reader.0.load(Ordering::SeqCst), 3);
    assert_eq!(model.agent.load(Ordering::SeqCst), 4);
    assert!(model.compaction.load(Ordering::SeqCst) > 0);
    let saved = store.load(&scope, handle.run_id()).await?;
    assert_eq!(saved.messages.len(), 8);
    assert_eq!(
        saved.snapshot.usage.model_calls,
        (model.agent.load(Ordering::SeqCst) + model.compaction.load(Ordering::SeqCst)) as u64
    );
    let reference = saved
        .snapshot
        .context_revision_ref
        .as_ref()
        .expect("saved context revision");
    assert_eq!(saved.session.context_revision_ref.as_ref(), Some(reference));
    let plan = ContextPlan::restore(
        &store
            .read_record(&scope, saved.snapshot.context_plan_ref.as_ref().unwrap())
            .await?,
        &saved.snapshot.profile,
    )?;
    let revision = ContextRevision::restore(
        &store.read_record(&scope, reference).await?,
        &plan,
        &scope,
        &request.session_id,
        &saved.messages,
    )?;
    assert!(revision.summary().is_some());
    assert!(!revision.covered_message_ids().is_empty());
    let events: Vec<_> = handle.events(0, context.clone()).try_collect().await?;
    assert!(
        events
            .iter()
            .any(|event| event.event_type == "context.rewritten")
    );
    let counts = (
        reader.0.load(Ordering::SeqCst),
        model.agent.load(Ordering::SeqCst),
        model.compaction.load(Ordering::SeqCst),
    );
    drop(agent);
    drop(store);
    let reopened = Arc::new(SqliteStateStore::open(&path)?);
    assert_eq!(reopened.load(&scope, handle.run_id()).await?, saved);
    let restored = make_agent(
        profile,
        &context,
        reopened,
        reader.clone(),
        model.clone(),
        policy,
    )?;
    let replay = completed(restored.start(request, context.clone()).await?)?;
    assert_eq!(completed(replay.outcome(&context).await?)?, outcome);
    assert_eq!(
        (
            reader.0.load(Ordering::SeqCst),
            model.agent.load(Ordering::SeqCst),
            model.compaction.load(Ordering::SeqCst)
        ),
        counts
    );
    println!(
        "compaction consumer: complete past rounds summarized; latest round and original transcript retained; auxiliary model calls charged; real SQLite revision/event restoration; fresh Host replay made no additional calls (synthetic model, no network)"
    );
    Ok(())
}
