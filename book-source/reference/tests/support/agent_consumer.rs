// Synthetic adapters and metadata inspector: no provider network calls are made.
// The fixed clock supports deterministic accounting; this does not test timeouts.
use futures_util::{TryStreamExt, stream};
use serde_json::json;
use std::collections::BTreeSet;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;
use wickle_model_router::{ModelDispatcherEntry, PolicyModelRouter, RegistryModelDispatcher};
use wickle_state_sqlite::SqliteStateStore;

fn id(s: &str) -> Id {
    Id::new(s).expect("example identifiers")
}
struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        r: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id("revision-1")),
                    ..r.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("registered model")),
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

fn reference(name: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id("1"),
    }
}
fn routing_snapshot(scope: &Scope) -> Result<RoutingSnapshot, ContractError> {
    let mut models = vec![];
    let mut bindings = vec![];
    for name in ["first", "second"] {
        let capabilities = ModelCapabilities {
            revision: id("capabilities"),
            features: BTreeSet::from([id("text")]),
            options_schema: json!({"type":"object","properties":{"reasoning_effort":{"enum":["low","medium","high"]}},"additionalProperties":false}),
            context_window: 4096.try_into().unwrap(),
            max_output_tokens: 512.try_into().unwrap(),
        };
        let model = ModelDefinition {
            model_key: id(name),
            family: id("example"),
            provider: id(name),
            model_id: id("example-model"),
            model_version: id("release-1"),
            version_semantics: VersionSemantics::Pinned,
            lifecycle: ModelLifecycle::Active,
            capabilities: capabilities.clone(),
            evidence: vec![],
        };
        let mut binding = ModelBinding {
            default_options: JsonObject::from([("reasoning_effort".into(), json!("low"))]),
            binding: reference(name),
            model: model.reference(),
            requested_model: model.model_id.clone(),
            adapter: reference("adapter"),
            connection_ref: reference(&format!("{name}-account")),
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
            evidence_ref: id("synthetic-fixture"),
            passed: true,
        });
        models.push(model);
        bindings.push(binding);
    }
    RoutingSnapshot::new(
        ModelCatalogSnapshot {
            revision: id("catalog"),
            scope: scope.clone(),
            models,
            bindings,
            aliases: vec![],
        },
        RoutingPolicy {
            revision: id("policy"),
            scope: scope.clone(),
            rules: vec![RoutingRule {
                model_binding: id("primary"),
                purpose: ModelPurpose::Agent,
                primary: reference("first"),
                fallbacks: vec![reference("second")],
                fallback_on: vec![ModelFailureKind::RateLimited],
                version_policy: VersionPolicy::RequirePinned,
                min_support: ModelSupportStatus::ContractTested,
            }],
        },
    )
}
struct TemporaryDatabase(std::path::PathBuf);
impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct ExampleClock;
impl Clock for ExampleClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: 1000,
            monotonic_ms: 1000,
        })
    }
    fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}
struct ExamplePolicy;
impl PolicyPort for ExamplePolicy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::InvokeModel { route, .. } = &request.action {
                if route.connection_ref.id == id(&format!("{}-account", route.provider)) {
                    return Ok(PolicyDecision::Allow {});
                }
            }
            Ok(
                if matches!(request.action, PolicyAction::InvokeModel { .. }) {
                    PolicyDecision::Deny {
                        reason: id("unknown-account"),
                    }
                } else {
                    PolicyDecision::Allow {}
                },
            )
        })
    }
}
struct ExampleInspector;
// This echo is a fixture only. A real inspector must read authoritative provider
// metadata instead of presenting requested values as independently observed facts.
impl ModelRouteInspector for ExampleInspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        _: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: ModelRouteAvailability::Available,
                model_id: Some(route.model_id.clone()),
                model_version: Some(route.model_version.clone()),
                deployment_revision: None,
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id("synthetic-metadata-check"),
            })
        })
    }
}
struct ExampleModel {
    hold: AtomicBool,
    entered: tokio::sync::Notify,
    route: ResolvedModelRoute,
    calls: AtomicUsize,
    fail: AtomicBool,
}
impl ModelPort for ExampleModel {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: self.route.provider.clone(),
            adapter: self.route.adapter.clone(),
            connection_ref: self.route.connection_ref.clone(),
        }
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        assert_eq!(
            request.options.get("reasoning_effort"),
            Some(&json!("high"))
        );
        assert_eq!(request.route, self.route);
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.hold.load(Ordering::SeqCst) {
            self.entered.notify_one();
            return Box::pin(stream::pending());
        }
        let events = if self.fail.load(Ordering::SeqCst) {
            vec![Ok(ModelEvent::ResponseError {
                kind: ModelFailureKind::RateLimited,
                metadata: ModelResponseMetadata::default(),
            })]
        } else {
            vec![
                Ok(ModelEvent::TextDelta {
                    text: format!("{} provider result", self.route.provider),
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
// Application metadata remains separate from the runtime status.
struct MaintenancePolicy;
impl InterruptionPolicy for MaintenancePolicy {
    fn identity(&self) -> VersionedRef { reference("maintenance-policy") }
    fn decide<'a>(&'a self, info: &'a InterruptionInfo) -> PortFuture<'a, InterruptionDecision> {
        Box::pin(async move {
            Ok(if matches!(&info.interruption.cause, InterruptionCause::HostShutdown) {
                InterruptionDecision {
                    action: InterruptionAction::Pause,
                    app_state: Some(AppState { namespace: id("operations"), status: id("maintenance"), metadata: info.configuration.clone() }),
                }
            } else {
                InterruptionDecision { action: InterruptionAction::UseDefault, app_state: None }
            })
        })
    }
}
struct Estimate;
impl ModelTokenEstimator for Estimate {
    fn estimate(&self, request: &ModelRequest) -> Result<u64, ContractError> {
        // Conservative test estimate; it is not provider-measured token usage.
        serde_json::to_vec(request)
            .map(|bytes| bytes.len() as u64)
            .map_err(|_| ContractError::new(ErrorCode::InvalidContext, "example.estimate"))
    }
}
fn completed<T>(value: Guarded<T>) -> Result<T, Box<dyn std::error::Error>> {
    match value {
        Guarded::Completed(value) => Ok(value),
        Guarded::ApprovalRequired(_) => Err("unexpected approval".into()),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let temporary = TemporaryDatabase(
        std::env::temp_dir().join(format!("wickle-agent-{}", RandomIdSource.next_id()?)),
    );
    std::fs::create_dir(&temporary.0)?;
    let database = temporary.0.join("state.sqlite3");
    let store = Arc::new(SqliteStateStore::open(&database)?);
    let snapshot = routing_snapshot(&scope)?;
    let first = Arc::new(ExampleModel {
        hold: AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
        route: snapshot.route_for_binding(&reference("first"))?,
        calls: AtomicUsize::new(0),
        fail: AtomicBool::new(true),
    });
    let second = Arc::new(ExampleModel {
        hold: AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
        route: snapshot.route_for_binding(&reference("second"))?,
        calls: AtomicUsize::new(0),
        fail: AtomicBool::new(false),
    });
    let policy = Arc::new(PolicyGate::new(
        Arc::new(ExamplePolicy),
        Duration::from_secs(1),
    )?);
    let exchange = Arc::new(
        ModelExchange::with_dispatcher(
            Arc::new(RegistryModelDispatcher::new(vec![
                ModelDispatcherEntry {
                    scope: scope.clone(),
                    port: first.clone(),
                },
                ModelDispatcherEntry {
                    scope: scope.clone(),
                    port: second.clone(),
                },
            ])?),
            policy.clone(),
        )
        .with_route_inspector(Arc::new(ExampleInspector), Duration::from_secs(1))?,
    );
    let profile = AgentProfile::from_json(
        r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
        "name":"Assistant","description":"Agent consumer","instructions":{"text":"Use supplied information"},
        "model_binding":"primary","model_options":{"reasoning_effort":"medium"},"tools":[],"skills":[],"connectors":[],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":4,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":2,"max_elapsed_ms":10000}
    }"#,
    )?;
    let agent = create_agent(
        profile,
        AgentBindings {
            interruption_policy: Some(InterruptionPolicyBinding {
                policy: Arc::new(MaintenancePolicy),
                configuration: JsonObject::from([("reason".into(), json!("maintenance"))]),
                app_state_schema: Some(AppStateSchema {
                    namespace: id("operations"),
                    schema: json!({"type":"object","properties":{"namespace":{"const":"operations"},"status":{"enum":["maintenance"]},"metadata":{"type":"object","properties":{"reason":{"type":"string"}},"required":["reason"],"additionalProperties":false}},"required":["namespace","status","metadata"],"additionalProperties":false}),
                }),
                timeout_ms: 1000.try_into()?,
            }),
            scope: scope.clone(),
            state: store.clone(),
            policy,
            profile_resolver: Arc::new(Catalog),
            model_exchange: exchange,
            router: Arc::new(PolicyModelRouter::new(snapshot)?),
            host_instructions: vec!["Preserve the requested output.".into()],
            system_inputs: SystemInputRegistry::new(vec![])?,
            clock: Arc::new(ExampleClock),
            ids: Arc::new(RandomIdSource),
            tools: None,
            system_input_resolver: None, external_receipt_verifier: None, components: None, context_sources: None, context_token_estimator: None, context_runtime: None, verification: None, skills: None, artifacts: None, hooks: None,
            token_estimator: Arc::new(Estimate),
            settings: AgentSettings {
                max_output_tokens: 128.try_into()?,
                require_durable: true,
                ..AgentSettings::default()
            },
        },
    )?;
    assert_eq!(first.calls.load(Ordering::SeqCst), 0);
    assert_eq!(second.calls.load(Ordering::SeqCst), 0);
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id("actor"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    );
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Retrieve the available result".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::from([("reasoning_effort".into(), json!("high"))]),
        max_output_tokens: None,
        output_contract: None,
    };
    let handle = completed(agent.start(request.clone(), context.clone()).await?)?;
    let run_id = handle.run_id().clone();
    let mut events = handle.events(0, context.clone());
    use futures_util::StreamExt;
    let started = events.next().await.ok_or("missing admission event")??;
    assert_eq!(started.event_type, "run.started");
    drop(events);
    let outcome = completed(handle.outcome(&context).await?)?;
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "second provider result".into()
        }]
    );
    assert_eq!(outcome.usage.model_calls, 2);
    assert_eq!(outcome.usage.recovery_attempts, 1);
    let replay = completed(agent.start(request.clone(), context.clone()).await?)?;
    assert_eq!(replay.run_id(), &run_id);
    assert_eq!(completed(replay.outcome(&context).await?)?, outcome);
    let submitted = store.read_execution(&scope, &run_id).await?.submitted.ok_or("submitted identity missing")?;
    submitted.validate(JsonTextLimits::default())?;
    let mut changed = request.clone();
    changed.model_options.insert("reasoning_effort".into(), json!("changed"));
    assert_eq!(agent.start(changed, context.clone()).await.unwrap_err().code, ErrorCode::RequestConflict);
    assert_eq!(store.read_execution(&scope, &run_id).await?.submitted.as_ref().map(|s|s.digest()), Some(submitted.digest()));

    assert_eq!(first.calls.load(Ordering::SeqCst), 1);
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    let view = completed(agent.get_run(&run_id, &context).await?)?;
    assert_eq!(view.status, RunStatus::Succeeded);
    assert!(!view.deadline_expired);
    let events: Vec<_> = handle
        .events(started.seq.get(), context.clone())
        .try_collect()
        .await?;
    assert_eq!(
        events.last().ok_or("missing finish event")?.event_type,
        "run.finished"
    );
    drop(store);
    let restored = SqliteStateStore::open(&database)?
        .load(&scope, &run_id)
        .await?;
    assert_eq!(restored.snapshot.outcome, Some(outcome));
    assert!(restored.session.active_run_id.is_none());
    // Exercise a cooperative stop against the packaged library and SQLite store.
    first.hold.store(true, Ordering::SeqCst);
    let stopped_request = RunRequest {
        request_id: id("maintenance-request"),
        session_id: id("maintenance-session"),
        ..request
    };
    let stopped = completed(agent.start(stopped_request.clone(), context.clone()).await?)?;
    tokio::time::timeout(Duration::from_secs(5), first.entered.notified()).await?;
    assert_eq!(completed(stopped.stop_execution(InterruptionCause::HostShutdown, &context).await?)?, ExecutionStopReceipt::Requested);
    let stopped_outcome = completed(tokio::time::timeout(Duration::from_secs(5), stopped.outcome(&context)).await??)?;
    assert!(matches!(&stopped_outcome.result, OutcomeResult::Interrupted { interruption }
        if interruption.cause == InterruptionCause::HostShutdown && interruption.recoverable));
    assert_eq!(stopped_outcome.app_state.as_ref().map(|state| &state.status), Some(&id("maintenance")));
    let reopened = SqliteStateStore::open(&database)?;
    let saved = reopened.load(&scope, stopped.run_id()).await?;
    assert_eq!(saved.snapshot.outcome, Some(stopped_outcome.clone()));
    assert_eq!(saved.session.active_run_id.as_ref(), Some(stopped.run_id()));
    let replay = completed(agent.start(stopped_request.clone(), context.clone()).await?)?;
    assert_eq!(completed(replay.outcome(&context).await?)?, stopped_outcome);
    assert_eq!(first.calls.load(Ordering::SeqCst), 2);
    let cancellation = ControlCommand { command_id: id("cancel-maintenance"), principal_ref: context.data.principal_ref.clone(), action: ControlAction::Cancel { reason: id("withdrawn") } };
    let receipt = completed(agent.submit_control_command(stopped.run_id().clone(), cancellation.clone(), context.clone()).await?)?;
    assert!(receipt.processed_segment_id.is_some());
    assert_eq!(completed(stopped.outcome(&context).await?)?, stopped_outcome);
    let latest = completed(agent.start(stopped_request, context.clone()).await?)?;
    assert_eq!(Some(latest.segment_id()), receipt.processed_segment_id.as_ref());
    assert_eq!(completed(latest.outcome(&context).await?)?.result.status(), RunStatus::Cancelled);
    let before_read = SqliteStateStore::open(&database)?.load(&scope, stopped.run_id()).await?;
    assert_eq!(completed(agent.get_control_receipt(stopped.run_id(), &cancellation.command_id, &context).await?)?, receipt);
    assert_eq!(SqliteStateStore::open(&database)?.load(&scope, stopped.run_id()).await?, before_read);
    assert_eq!(completed(agent.submit_control_command(stopped.run_id().clone(), cancellation, context.clone()).await?)?, receipt);
    assert_eq!(first.calls.load(Ordering::SeqCst), 2);
    let prepared_id = restored.snapshot.prepared_steps.last().ok_or("missing saved preparation")?.record_id.clone();
    let inspection = completed(agent.inspect_step(&run_id, StepRef::Prepared { record_id: prepared_id }, &context, InspectionOptions::default()).await?)?;
    assert_eq!(inspection.status, InspectionStatus::Found);
    let composition = inspection.composition.as_ref().ok_or("missing composition")?;
    assert_eq!(composition.recorded_run_revision, restored.snapshot.revision);
    assert_eq!(composition.recorded_run_outcome.as_ref().and_then(|outcome| outcome.completion_basis), Some(CompletionBasis::TurnEnded));
    assert!(composition.attempts.iter().any(|attempt| attempt.evidence.contains(&InspectionEvidence::ResponseObserved)));
    assert_eq!(composition.model.as_ref().and_then(|model| model.configuration.as_ref()).and_then(|configuration| configuration.effective.get("reasoning_effort")), Some(&json!("high")));
    assert_eq!(composition.model.as_ref().and_then(|model| model.configuration.as_ref()).and_then(|configuration| configuration.sources.get("reasoning_effort")), Some(&ModelOptionSource::Run));
    let encoded = serde_json::to_value(&inspection)?;
    assert!(encoded["composition"]["model"].get("connection_ref").is_none());
    assert!(encoded["composition"]["model"].get("target").is_none());
    assert_eq!(SqliteStateStore::open(&database)?.load(&scope, &run_id).await?, restored);
    assert_eq!(first.calls.load(Ordering::SeqCst), 2);
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    // Recover a separate cooperatively interrupted interval through the public
    // API. The synthetic primary becomes available after the stop.
    let recover_request = RunRequest { request_id: id("recoverable-request"), session_id: id("recoverable-session"), input: vec![InputContent::Text { text: "Continue after maintenance".into() }], trigger: RunTrigger::User {}, model_options: JsonObject::from([("reasoning_effort".into(), json!("high"))]), max_output_tokens: None, output_contract: None };
    let paused = completed(agent.start(recover_request.clone(), context.clone()).await?)?;
    tokio::time::timeout(Duration::from_secs(5), first.entered.notified()).await?;
    completed(paused.stop_execution(InterruptionCause::HostShutdown, &context).await?)?;
    let paused_outcome = completed(paused.outcome(&context).await?)?;
    assert_eq!(paused_outcome.result.status(), RunStatus::Interrupted);
    let checkpoint = SqliteStateStore::open(&database)?.load(&scope, paused.run_id()).await?;
    let source = checkpoint.snapshot.recovery_record(id("maintenance-recovery"))?;
    first.hold.store(false, Ordering::SeqCst);
    first.fail.store(false, Ordering::SeqCst);
    let command = ResumeCommand { run_id: paused.run_id().clone(), expected_revision: checkpoint.snapshot.revision, command_id: id("recover-maintenance"), action: ResumeAction::Recover { recovery_ref: source.reference().clone() } };
    let recovered = completed(agent.resume(command.clone(), context.clone()).await?)?;
    let recovered_outcome = completed(recovered.outcome(&context).await?)?;
    assert_eq!(recovered_outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(recovered_outcome.output, vec![InputContent::Text { text: "first provider result".into() }]);
    assert_ne!(paused.segment_id(), recovered.segment_id());
    assert_eq!(completed(paused.outcome(&context).await?)?, paused_outcome);
    let duplicate = completed(agent.resume(command, context.clone()).await?)?;
    assert_eq!(duplicate.segment_id(), recovered.segment_id());
    assert_eq!(completed(duplicate.outcome(&context).await?)?, recovered_outcome);
    assert_eq!(first.calls.load(Ordering::SeqCst), 4);
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    println!(
        "agent consumer: pure construction, detached execution after observer drop, fallback under shared budgets, stored outcome and event replay, duplicate request without new model calls, SQLite reopen, custom interruption state and recovery, immutable prior outcomes, option provenance, durable replay and read-only stored composition inspection"
    );
    Ok(())
}
