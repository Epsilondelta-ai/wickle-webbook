// Synthetic model, tool, resolver, and metadata inspector; no provider network or
// business database calls. Real SQLite persists an approval wait across Host instances.
use futures_util::{TryStreamExt, stream};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;
use wickle_model_router::PolicyModelRouter;
use wickle_state_sqlite::SqliteStateStore;

const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";
fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .expect("object fixture")
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
fn completed<T>(value: Guarded<T>) -> Result<T, Box<dyn std::error::Error>> {
    match value {
        Guarded::Completed(value) => Ok(value),
        Guarded::ApprovalRequired(_) => Err("unexpected approval".into()),
    }
}

struct Catalog {
    calls: AtomicUsize,
}
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(
                        reference
                            .version
                            .clone()
                            .unwrap_or_else(|| id("model-binding-revision")),
                    ),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("synthetic registered component")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| id("write")),
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
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::ExecuteTool { input } = &request.action {
                if input.execution_args().get("workspace_id") != Some(&json!(WORKSPACE)) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("foreign-workspace"),
                    });
                }
                let approved = input.approval().is_some_and(|approval| {
                    approval.actor_ref() == &id("reviewer")
                        && approval.capability_grant_ref() == &id("reviewer-grant")
                        && context.principal_ref == &id("reviewer")
                });
                if !approved {
                    return Ok(PolicyDecision::RequireApproval {
                        reason: id("write-review"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
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

const RECORD: &str = "22222222-2222-4222-8222-222222222222";
const NEW_RECORD: &str = "33333333-3333-4333-8333-333333333333";

struct Model {
    route: ResolvedModelRoute,
    propose: bool,
    calls: AtomicUsize,
}
impl ModelPort for Model {
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
            self.calls.fetch_add(1, Ordering::SeqCst),
            0,
            "each segment must call its model once"
        );
        assert_eq!(request.route, self.route);
        assert_eq!(request.tools.len(), 1);
        assert_eq!(request.tools[0].name, id("write"));
        assert_eq!(
            request.tools[0].model_input_schema["properties"],
            json!({"query":{"type":"string"}})
        );
        let finish;
        let mut events;
        if self.propose {
            finish = ModelFinish::ToolCalls;
            events = vec![Ok(ModelEvent::ToolArgumentsDelta {
                index: 0,
                provider_call_id: Some("write-report".into()),
                name: Some("write".into()),
                delta: r#"{"query":"report"}"#.into(),
            })];
        } else {
            let calls: Vec<_> = request
                .messages
                .iter()
                .flat_map(|message| &message.content)
                .filter_map(|content| match content {
                    ModelContent::ToolCall {
                        provider_call_id,
                        arguments,
                        ..
                    } => Some((provider_call_id.clone(), arguments.clone())),
                    _ => None,
                })
                .collect();
            assert_eq!(
                calls,
                vec![(id("write-report"), object(json!({"query":"report"})))]
            );
            let observations: Vec<_> = request
                .messages
                .iter()
                .flat_map(|message| &message.content)
                .filter_map(|content| match content {
                    ModelContent::ToolResult {
                        provider_call_id,
                        content,
                    } => Some((provider_call_id.clone(), content.clone())),
                    _ => None,
                })
                .collect();
            assert_eq!(
                observations,
                vec![(
                    id("write-report"),
                    json!({"status":"succeeded","effect":"applied","content":[{"type":"json","value":"written"}]})
                )]
            );
            finish = ModelFinish::Stop;
            events = vec![Ok(ModelEvent::TextDelta {
                text: "Report written.".into(),
            })];
        }
        events.push(Ok(ModelEvent::ResponseCompleted {
            finish,
            metadata: ModelResponseMetadata::default(),
            continuation: vec![],
        }));
        Box::pin(stream::iter(events))
    }
}
struct Resolver {
    value: &'static str,
    revision: &'static str,
    calls: AtomicUsize,
}
impl SystemInputResolver for Resolver {
    fn resolve<'a>(
        &'a self,
        request: &'a SystemInputResolveRequest,
        context: &'a SystemInputResolveContext,
    ) -> PortFuture<'a, Option<ResolvedSystemInput>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.key, id("record_id"));
        assert_eq!(context.principal_ref, id("requester"));
        Box::pin(async move {
            Ok(Some(ResolvedSystemInput {
                value: json!(self.value),
                revision: id(self.revision),
            }))
        })
    }
}
struct Writer {
    calls: AtomicUsize,
    seen: Mutex<Vec<(JsonObject, Id)>>,
}
impl ToolExecutor for Writer {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        assert_eq!(
            self.calls.fetch_add(1, Ordering::SeqCst),
            0,
            "the saved write may execute once"
        );
        assert_eq!(
            args,
            &object(json!({"query":"report","workspace_id":WORKSPACE,"record_id":RECORD}))
        );
        assert_eq!(context.principal_ref, id("reviewer"));
        assert_eq!(context.capability_grant_ref, id("reviewer-grant"));
        self.seen
            .lock()
            .unwrap()
            .push((args.clone(), context.call_id.clone()));
        Box::pin(async move {
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: json!("written"),
                },
                effect: ToolEffect::Applied,
                receipt: Some(json!({"effect_id":"synthetic-write","record_id":args["record_id"]})),
            })
        })
    }
}
fn registry(
    scope: &Scope,
    writer: Arc<Writer>,
) -> Result<(SystemInputRegistry, ToolRegistry), ContractError> {
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
    ])?;
    let compiled = SchemaCompiler::new().compile(ToolDescriptor {
        tool: reference("write"), name: id("write"), description: "Write an authorized report".into(),
        input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"workspace_id":{"type":"string","format":"uuid"},"record_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id","record_id"],"additionalProperties":false}),
        agent_parameters: vec!["query".into()], system_bindings: None, output_schema: json!({"type":"string"}),
        side_effect: ToolSideEffect::Write, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false,
        max_output_bytes: 1024.try_into().unwrap(),
    }, &inputs)?;
    Ok((
        inputs,
        ToolRegistry::new(
            scope.clone(),
            vec![ToolRegistration {
                compiled,
                executor: writer,
            }],
        )?,
    ))
}
fn agent(
    scope: &Scope,
    store: Arc<SqliteStateStore>,
    model: Arc<Model>,
    writer: Arc<Writer>,
    resolver: Arc<Resolver>,
    catalog: Arc<Catalog>,
) -> Result<Agent, ContractError> {
    let (system_inputs, tools) = registry(scope, writer)?;
    let policy = Arc::new(PolicyGate::new(Arc::new(Policy), Duration::from_secs(1))?);
    let profile = AgentProfile::from_json(
        r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"writer","version":"1",
        "name":"Writer","description":"Synthetic approval resume consumer","instructions":{"text":"Write the report after authorization"},
        "model_binding":"primary","tools":[{"tool_id":"write","version":"1"}],"skills":[],"connectors":[],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":3,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}
    }"#,
    )?;
    create_agent(
        profile,
        AgentBindings {
            scope: scope.clone(),
            state: store,
            policy: policy.clone(),
            profile_resolver: catalog,
            model_exchange: Arc::new(
                ModelExchange::new(model, policy)
                    .with_route_inspector(Arc::new(Inspector), Duration::from_secs(1))?,
            ),
            router: Arc::new(PolicyModelRouter::new(routing(scope)?)?),
            host_instructions: vec!["Use only authorized inputs.".into()],
            system_inputs,
            tools: Some(Arc::new(tools)),
            system_input_resolver: Some(resolver),
            external_receipt_verifier: None, components: None, context_sources: None, context_token_estimator: None, context_runtime: None, verification: None, skills: None, artifacts: None, hooks: None,
            clock: Arc::new(SystemClock::new()),
            ids: Arc::new(RandomIdSource),
            token_estimator: Arc::new(Estimate),
            settings: AgentSettings {
                require_durable: true,
                max_output_tokens: 128.try_into().unwrap(),
                ..AgentSettings::default()
            },
        },
    )
}
fn context(scope: &Scope, reviewer: bool) -> ExecutionContext {
    ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id(if reviewer { "reviewer" } else { "requester" }),
            capability_grant_ref: id(if reviewer {
                "reviewer-grant"
            } else {
                "requester-grant"
            }),
            trace_context: None,
            system_inputs: if reviewer {
                None
            } else {
                Some(SystemInputs::new(object(json!({"workspace_id":WORKSPACE}))))
            },
        },
        Default::default(),
    )
}
struct TemporaryDatabase(std::path::PathBuf);
impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
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
        std::env::temp_dir().join(format!("wickle-resume-{}", RandomIdSource.next_id()?)),
    );
    std::fs::create_dir(&temporary.0)?;
    let database = temporary.0.join("state.sqlite3");
    let store = Arc::new(SqliteStateStore::open(&database)?);
    let model = Arc::new(Model {
        route: routing(&scope)?.route_for_binding(&reference("primary"))?,
        propose: true,
        calls: AtomicUsize::new(0),
    });
    let writer = Arc::new(Writer {
        calls: AtomicUsize::new(0),
        seen: Mutex::new(vec![]),
    });
    let resolver = Arc::new(Resolver {
        value: RECORD,
        revision: "record-A",
        calls: AtomicUsize::new(0),
    });
    let catalog = Arc::new(Catalog {
        calls: AtomicUsize::new(0),
    });
    let initial_agent = agent(
        &scope,
        store.clone(),
        model.clone(),
        writer.clone(),
        resolver.clone(),
        catalog.clone(),
    )?;
    let caller = context(&scope, false);
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Write the report".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    };
    let handle = completed(initial_agent.start(request, caller.clone()).await?)?;
    let waiting = completed(handle.outcome(&caller).await?)?;
    assert_eq!(waiting.result.status(), RunStatus::Waiting);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(writer.calls.load(Ordering::SeqCst), 0);
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    let run_id = handle.run_id().clone();
    let saved = store.load(&scope, &run_id).await?;
    let wait = saved.snapshot.wait.clone().ok_or("missing wait")?;
    let WaitTarget::Approval { target } = wait.target else {
        return Err("expected tool approval".into());
    };
    let command = ResumeCommand {
        run_id: run_id.clone(),
        expected_revision: saved.snapshot.revision,
        command_id: id("approve-report"),
        action: ResumeAction::Approve {
            wait_id: wait.wait_id,
            target,
        },
    };
    let bound_ref = saved.snapshot.tool_ledger[0]
        .call
        .bound_input_ref
        .clone()
        .ok_or("missing binding")?;
    let bound_record = store.read_record(&scope, &bound_ref).await?;
    let (_, tools) = registry(&scope, writer.clone())?;
    let bound = BoundToolInput::restore(
        &bound_record,
        &tools.get(&id("write")).ok_or("tool missing")?.compiled,
        &scope,
        &run_id,
        &saved.snapshot.tool_ledger[0].call,
        saved.snapshot.system_inputs.as_ref(),
    )?;
    assert_eq!(
        bound.execution_args(),
        &object(json!({"query":"report","workspace_id":WORKSPACE,"record_id":RECORD}))
    );
    assert_eq!(
        bound.system_inputs()["record_id"]
            .resolved
            .as_ref()
            .ok_or("record missing")?
            .revision,
        id("record-A")
    );
    let events: Vec<_> = handle.events(0, caller.clone()).try_collect().await?;
    assert_eq!(
        events.last().ok_or("missing wait event")?.event_type,
        "run.waiting"
    );
    let old_store = Arc::downgrade(&store);
    let old_model = Arc::downgrade(&model);
    let old_resolver = Arc::downgrade(&resolver);
    drop(tools);
    drop(handle);
    drop(initial_agent);
    drop(store);
    drop(model);
    drop(writer);
    drop(resolver);
    drop(catalog);
    // A saved wait ends its driver; verify no previous Host instance remains alive.
    tokio::time::timeout(Duration::from_secs(5), async {
        while old_store.upgrade().is_some()
            || old_model.upgrade().is_some()
            || old_resolver.upgrade().is_some()
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;

    let reopened = Arc::new(SqliteStateStore::open(&database)?);
    let restored = reopened.load(&scope, &run_id).await?;
    assert_eq!(restored.snapshot, saved.snapshot);
    assert_eq!(
        restored.session.prompt_snapshot,
        saved.session.prompt_snapshot
    );
    let model = Arc::new(Model {
        route: routing(&scope)?.route_for_binding(&reference("primary"))?,
        propose: false,
        calls: AtomicUsize::new(0),
    });
    let writer = Arc::new(Writer {
        calls: AtomicUsize::new(0),
        seen: Mutex::new(vec![]),
    });
    let resolver = Arc::new(Resolver {
        value: NEW_RECORD,
        revision: "record-B",
        calls: AtomicUsize::new(0),
    });
    let catalog = Arc::new(Catalog {
        calls: AtomicUsize::new(0),
    });
    let resumed_agent = agent(
        &scope,
        reopened.clone(),
        model.clone(),
        writer.clone(),
        resolver.clone(),
        catalog.clone(),
    )?;
    let reviewer = context(&scope, true);
    let resumed = completed(
        resumed_agent
            .resume(command.clone(), reviewer.clone())
            .await?,
    )?;
    assert_eq!(resumed.run_id(), &run_id);
    let outcome = completed(resumed.outcome(&reviewer).await?)?;
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "Report written.".into()
        }]
    );
    assert_eq!(
        (outcome.usage.model_calls, outcome.usage.tool_attempts),
        (2, 1)
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(writer.calls.load(Ordering::SeqCst), 1);
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
    assert_eq!(catalog.calls.load(Ordering::SeqCst), 0);
    let finished = reopened.load(&scope, &run_id).await?;
    assert_eq!(
        finished.snapshot.tool_ledger[0]
            .call
            .bound_input_ref
            .as_ref(),
        Some(&bound_ref)
    );
    assert_eq!(
        finished.snapshot.system_inputs,
        saved.snapshot.system_inputs
    );
    assert_eq!(
        finished.snapshot.routing_snapshot_ref,
        saved.snapshot.routing_snapshot_ref
    );
    assert_eq!(finished.snapshot.resume_receipts.len(), 1);
    let acceptance = &finished.snapshot.resume_receipts[0];
    assert_eq!(acceptance.command, command);
    assert_eq!(acceptance.actor_ref, id("reviewer"));
    assert_eq!(
        acceptance.previous_last_event_seq,
        saved.snapshot.last_event_seq
    );
    let previous = reopened
        .read_record(&scope, &acceptance.previous_outcome_ref)
        .await?;
    assert_eq!(
        serde_json::from_value::<RunOutcome>(previous.value().clone())?,
        waiting
    );
    let continued: Vec<_> = resumed
        .events(saved.snapshot.last_event_seq, reviewer.clone())
        .try_collect()
        .await?;
    assert_eq!(continued[0].seq.get(), saved.snapshot.last_event_seq + 1);
    assert_eq!(continued[0].event_type, "run.resumed");
    assert_eq!(
        continued.last().ok_or("missing finish event")?.event_type,
        "run.finished"
    );
    let before_replay = (
        model.calls.load(Ordering::SeqCst),
        writer.calls.load(Ordering::SeqCst),
        resolver.calls.load(Ordering::SeqCst),
        catalog.calls.load(Ordering::SeqCst),
    );
    let replay = completed(resumed_agent.resume(command, reviewer.clone()).await?)?;
    assert_eq!(completed(replay.outcome(&reviewer).await?)?, outcome);
    assert_eq!(
        (
            model.calls.load(Ordering::SeqCst),
            writer.calls.load(Ordering::SeqCst),
            resolver.calls.load(Ordering::SeqCst),
            catalog.calls.load(Ordering::SeqCst)
        ),
        before_replay
    );
    assert_eq!(
        reopened.load(&scope, &run_id).await?.snapshot,
        finished.snapshot
    );
    println!(
        "resume consumer: real SQLite wait/reopen; same Run and frozen inputs; new reviewer; one write; contiguous events; duplicate command adds no model, tool, resolver, or metadata calls (synthetic Host ports, no provider network)"
    );
    Ok(())
}
