// Real SQLite, structured output, and deterministic candidate verification.
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
        features: BTreeSet::from([id("text"), id("json_output")]),
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

struct Model(AtomicUsize);
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
        _: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let index = self.0.fetch_add(1, Ordering::SeqCst);
        assert!(index < 2, "unexpected extra model call");
        let text = json!({"amount":if index==0{5}else{11}}).to_string();
        Box::pin(stream::iter([
            Ok(ModelEvent::TextDelta { text }),
            Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::Stop,
                metadata: Default::default(),
                continuation: vec![],
            }),
        ]))
    }
}
fn make_agent(
    profile: AgentProfile,
    context: &ExecutionContext,
    store: Arc<SqliteStateStore>,
    model: Arc<Model>,
    policy: Arc<PolicyGate>,
) -> Result<Agent, ContractError> {
    let format = json!({"type":"object","properties":{"amount":{"type":"integer"}},"required":["amount"],"additionalProperties":false});
    let criteria = json!({"type":"object","properties":{"amount":{"type":"integer","minimum":10}},"required":["amount"],"additionalProperties":false});
    let verifier = SchemaVerifier::new(
        VerifierDefinition {
            verifier_ref: reference("quality"),
            criteria_ref: reference("minimum-amount"),
            criteria: "Amount must be an integer at least ten.".into(),
            configuration: Default::default(),
        },
        criteria,
    )?;
    let verification = Arc::new(VerificationRuntime::new(
        context.data.scope.clone(),
        vec![OutputSchemaDefinition {
            schema_ref: reference("output"),
            schema: format,
        }],
        vec![Arc::new(verifier)],
        VerificationLimits::default(),
    )?);
    create_agent(
        profile,
        AgentBindings {
            interruption_policy: None,
            scope: context.data.scope.clone(),
            state: store,
            policy: policy.clone(),
            profile_resolver: Arc::new(Catalog),
            model_exchange: Arc::new(
                ModelExchange::new(model, policy)
                    .with_route_inspector(Arc::new(Inspector), Duration::from_secs(1))?,
            ),
            router: Arc::new(PolicyModelRouter::new(routing(&context.data.scope)?)?),
            host_instructions: vec![
                "Use the configured output contract and review feedback.".into(),
            ],
            system_inputs: SystemInputRegistry::new(vec![])?,
            tools: None,
            system_input_resolver: None,
            external_receipt_verifier: None,
            hooks: None,
            components: None,
            context_sources: None,
            context_token_estimator: None,
            context_runtime: None,
            verification: Some(verification),
            skills: None,
            artifacts: None,
            clock: Arc::new(SystemClock::new()),
            ids: Arc::new(RandomIdSource),
            token_estimator: Arc::new(Estimate),
            settings: AgentSettings {
                require_durable: true,
                max_output_tokens: 128.try_into().unwrap(),
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
            principal_ref: id("reviewer"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    );
    let policy = Arc::new(PolicyGate::new(Arc::new(Policy), Duration::from_secs(1))?);
    let model = Arc::new(Model(AtomicUsize::new(0)));
    let path = std::env::temp_dir().join(format!(
        "wickle-verification-consumer-{}.sqlite3",
        RandomIdSource.next_id()?
    ));
    let store = Arc::new(SqliteStateStore::open(&path)?);
    let profile = AgentProfile::from_json(
        r#"{"schema_version":"wickle.agent-profile.v1","agent_id":"example","version":"1","name":"Verification example","description":"Structured candidate repair","instructions":{"text":"Return a valid amount."},"model_binding":"primary","tools":[],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"completion_policy":{"mode":"verified","verifier_ref":{"id":"quality","version":"1"}},"output_contract":{"type":"json_schema","schema_ref":{"id":"output","version":"1"}},"limits":{"max_model_calls":3,"max_tool_attempts":0,"max_repair_attempts":1,"max_recovery_attempts":0,"max_elapsed_ms":30000}}"#,
    )?;
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Supply an amount of at least ten.".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: Default::default(),
        max_output_tokens: None,
        output_contract: None,
    };
    let agent = make_agent(
        profile.clone(),
        &context,
        store.clone(),
        model.clone(),
        policy.clone(),
    )?;
    let handle = completed(agent.start(request.clone(), context.clone()).await?)?;
    let outcome = completed(handle.outcome(&context).await?)?;
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::Verified
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Json {
            value: json!({"amount":11})
        }]
    );
    assert_eq!(outcome.usage.model_calls, 2);
    assert_eq!(outcome.usage.repair_attempts, 1);
    assert_eq!(
        outcome.verification.as_ref().unwrap().criteria_ref,
        reference("minimum-amount")
    );
    let saved = store.load(&scope, handle.run_id()).await?;
    assert_eq!(
        saved
            .messages
            .iter()
            .filter(|message| message.origin == MessageOrigin::Verification)
            .count(),
        1
    );
    assert_eq!(saved.snapshot.verification_records.len(), 3);
    let events: Vec<_> = handle.events(0, context.clone()).try_collect().await?;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "verification.completed")
            .count(),
        2
    );
    drop(agent);
    drop(store);
    let reopened = Arc::new(SqliteStateStore::open(&path)?);
    assert_eq!(reopened.load(&scope, handle.run_id()).await?, saved);
    let restored = make_agent(profile, &context, reopened, model.clone(), policy)?;
    let replay = completed(restored.start(request, context.clone()).await?)?;
    assert_eq!(completed(replay.outcome(&context).await?)?, outcome);
    assert_eq!(model.0.load(Ordering::SeqCst), 2);
    println!(
        "verification consumer: JSON output and separate criteria enforced; one repair charged; feedback provenance preserved; real SQLite candidate/verdict restoration; fresh Host replay made no additional calls (synthetic model, no network)"
    );
    Ok(())
}
