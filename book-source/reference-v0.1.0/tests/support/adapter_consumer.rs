// Real SQLite and AdapterRuntime with synthetic model, tool, resolver, and inspector.
// No provider network or business database calls are made by this consumer.
use futures_util::{TryStreamExt, stream};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;
use wickle_adapter_runtime::{
    AdapterRegistration, AdapterRegistry, AdapterRuntime, ConnectionRegistration,
};
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

struct Policy;
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::ExecuteTool { input } = &request.action {
                assert_eq!(
                    input.selection(),
                    Some(&ToolBindingRef::Export(ExportRef {
                        adapter_binding: id("reports"),
                        export_id: id("save"),
                        alias: Some(id("write"))
                    }))
                );
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

fn metadata(kind: ComponentKind, name: &str) -> ComponentMetadata {
    ComponentMetadata {
        reference: ComponentRef {
            kind,
            id: id(name),
            version: Some(id("1")),
        },
        contract_version: 1,
        manifest_digest: canonical_digest(&json!(name)),
        config_schema: json!({"type":"object","additionalProperties":false}),
        dependencies: vec![],
        capabilities: BTreeSet::new(),
        required_capabilities: BTreeSet::new(),
        required_connections: BTreeSet::new(),
        model_name: None,
        hook_position: None,
        exports: vec![],
    }
}
struct Catalog {
    registry: Arc<AdapterRegistry>,
    calls: AtomicUsize,
}
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        request: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if request.kind == ComponentKind::ModelBinding {
                return Ok(metadata(ComponentKind::ModelBinding, request.id.as_str()));
            }
            self.registry.component_metadata(request).ok_or_else(|| {
                ContractError::new(ErrorCode::ComponentUnavailable, "catalog.reference")
            })
        })
    }
}
fn descriptor() -> ToolDescriptor {
    ToolDescriptor {
        tool: reference("save"),
        name: id("save"),
        description: "Write an authorized report".into(),
        input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"workspace_id":{"type":"string","format":"uuid"},"record_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id","record_id"],"additionalProperties":false}),
        agent_parameters: vec!["query".into()],
        system_bindings: None,
        output_schema: json!({"type":"string"}),
        side_effect: ToolSideEffect::Write,
        concurrency: ToolConcurrency::Serial,
        retry: ToolRetryPolicy::Never,
        reconcile: false,
        max_output_bytes: 1024.try_into().unwrap(),
    }
}
fn definition() -> AdapterDefinition {
    let export = ExportMetadata {
        export_id: id("save"),
        kind: ExportKind::Tool,
        contract_version: 1,
        model_name: Some(id("save")),
        hook_position: None,
        capabilities: BTreeSet::new(),
        required_capabilities: BTreeSet::new(),
    };
    let mut metadata = metadata(ComponentKind::Adapter, "report-adapter");
    metadata.required_connections.insert(id("main"));
    metadata.exports.push(export.clone());
    AdapterDefinition {
        metadata,
        exports: vec![AdapterExportDefinition::Tool {
            metadata: export,
            descriptor: Box::new(descriptor()),
        }],
    }
}
fn system_inputs() -> Result<SystemInputRegistry, ContractError> {
    SystemInputRegistry::new(vec![
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
}
#[derive(Default)]
struct Counters {
    opens: AtomicUsize,
    closes: AtomicUsize,
    writes: AtomicUsize,
    initialized: Mutex<Vec<(Id, Id, Value)>>,
}
struct Factory {
    store: Arc<SqliteStateStore>,
    counters: Arc<Counters>,
}
impl AdapterFactory for Factory {
    fn open<'a>(
        &'a self,
        context: &'a AdapterInitContext,
    ) -> PortFuture<'a, Arc<dyn AdapterInstance>> {
        Box::pin(async move {
            let saved = self
                .store
                .load(&context.execution.scope, &context.execution.run_id)
                .await?;
            assert_eq!(saved.snapshot.status, RunStatus::Running);
            assert!(saved.snapshot.assembly_ref.is_some());
            self.store
                .check_lease(
                    &context.execution.scope,
                    &context.execution.run_id,
                    context.execution.lease.as_ref().expect("execution lease"),
                    SystemClock::new().now()?.utc_ms,
                )
                .await?;
            assert_eq!(
                context.selected_exports,
                vec![ExportRef {
                    adapter_binding: id("reports"),
                    export_id: id("save"),
                    alias: Some(id("write"))
                }]
            );
            assert_eq!(
                context.binding.connections[&id("main")].connection_ref,
                reference("report-account")
            );
            let mapping = &context
                .binding
                .binding_state
                .as_ref()
                .expect("Host-prepared mapping")
                .value;
            assert_eq!(mapping, &json!({"thread_id":"prepared-report-thread"}));
            self.counters.opens.fetch_add(1, Ordering::SeqCst);
            self.counters.initialized.lock().unwrap().push((
                context.execution.binding_set_id.clone(),
                context.execution.principal_ref.clone(),
                mapping.clone(),
            ));
            Ok(Arc::new(Instance {
                scope: context.execution.scope.clone(),
                run_id: context.execution.run_id.clone(),
                binding_set: context.execution.binding_set_id.clone(),
                counters: self.counters.clone(),
                closed: AtomicBool::new(false),
                writer: Arc::new(Writer {
                    scope: context.execution.scope.clone(),
                    run_id: context.execution.run_id.clone(),
                    binding_set: context.execution.binding_set_id.clone(),
                    counters: self.counters.clone(),
                }),
            }) as Arc<dyn AdapterInstance>)
        })
    }
}
struct Writer {
    scope: Scope,
    run_id: Id,
    binding_set: Id,
    counters: Arc<Counters>,
}
impl ToolExecutor for Writer {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            assert_eq!(context.scope, self.scope);
            assert_eq!(context.run_id, self.run_id);
            assert_eq!(context.binding_set_id.as_ref(), Some(&self.binding_set));
            assert_eq!(context.principal_ref, id("reviewer"));
            assert_eq!(context.capability_grant_ref, id("reviewer-grant"));
            assert_eq!(
                args,
                &object(json!({"query":"report","workspace_id":WORKSPACE,"record_id":RECORD}))
            );
            assert_eq!(self.counters.writes.fetch_add(1, Ordering::SeqCst), 0);
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: json!("written"),
                },
                effect: ToolEffect::Applied,
                receipt: Some(
                    json!({"effect_id":"synthetic-report-write","record_id":args["record_id"]}),
                ),
            })
        })
    }
}
struct Instance {
    scope: Scope,
    run_id: Id,
    binding_set: Id,
    counters: Arc<Counters>,
    closed: AtomicBool,
    writer: Arc<Writer>,
}
impl AdapterInstance for Instance {
    fn exports(&self) -> Vec<AdapterExportInstance> {
        vec![AdapterExportInstance::Tool {
            export_id: id("save"),
            descriptor: Box::new(descriptor()),
            executor: self.writer.clone(),
        }]
    }
    fn close<'a>(&'a self, context: &'a AdapterCloseContext) -> PortFuture<'a, ()> {
        Box::pin(async move {
            assert_eq!(context.scope, self.scope);
            assert_eq!(context.run_id, self.run_id);
            assert_eq!(context.binding_set_id, self.binding_set);
            assert_eq!(context.adapter_binding, id("reports"));
            if !self.closed.swap(true, Ordering::SeqCst) {
                self.counters.closes.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        })
    }
}
fn registry(scope: &Scope, factory: Arc<Factory>) -> Result<AdapterRegistry, ContractError> {
    let definition = definition();
    let value = json!({"thread_id":"prepared-report-thread"});
    let state = AdapterBindingState {
        scope: scope.clone(),
        session_id: id("session"),
        adapter_binding: id("reports"),
        adapter: reference("report-adapter"),
        definition_digest: definition.digest(),
        state_ref: ProtectedRecord::new(id("prepared-mapping"), 1, value.clone())
            .reference()
            .clone(),
        value,
    };
    AdapterRegistry::new(
        scope.clone(),
        vec![AdapterRegistration {
            definition,
            factory,
        }],
        vec![ConnectionRegistration {
            binding: ConnectorBindingRef {
                binding_id: id("data"),
                connector_id: id("report-service"),
                version: id("1"),
            },
            metadata: metadata(ComponentKind::Connector, "report-service"),
            connection_ref: reference("report-account"),
        }],
        vec![],
        vec![],
        vec![state],
    )
}
fn agent(
    scope: &Scope,
    store: Arc<SqliteStateStore>,
    model: Arc<Model>,
    resolver: Arc<Resolver>,
    counters: Arc<Counters>,
) -> Result<(Agent, Arc<Catalog>), ContractError> {
    let registry = Arc::new(registry(
        scope,
        Arc::new(Factory {
            store: store.clone(),
            counters,
        }),
    )?);
    let catalog = Arc::new(Catalog {
        registry: registry.clone(),
        calls: AtomicUsize::new(0),
    });
    let policy = Arc::new(PolicyGate::new(Arc::new(Policy), Duration::from_secs(5))?);
    let clock = Arc::new(SystemClock::new());
    let runtime = Arc::new(AdapterRuntime::new(
        registry,
        store.clone(),
        policy.clone(),
        clock.clone(),
    ));
    let profile = AgentProfile::from_json(
        r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"writer","version":"1",
        "name":"Writer","description":"Synthetic adapter consumer","instructions":{"text":"Write the report after authorization"},
        "model_binding":"primary","tools":[{"adapter_binding":"reports","export_id":"save","alias":"write"}],"skills":[],
        "connectors":[{"binding_id":"data","connector_id":"report-service","version":"1"}],
        "adapters":[{"binding_id":"reports","adapter_id":"report-adapter","version":"1","connections":{"main":"data"}}],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":3,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}
    }"#,
    )?;
    Ok((
        create_agent(
            profile,
            AgentBindings {
                scope: scope.clone(),
                state: store,
                policy: policy.clone(),
                profile_resolver: catalog.clone(),
                model_exchange: Arc::new(
                    ModelExchange::new(model, policy)
                        .with_route_inspector(Arc::new(Inspector), Duration::from_secs(5))?,
                ),
                router: Arc::new(PolicyModelRouter::new(routing(scope)?)?),
                host_instructions: vec!["Use only authorized inputs.".into()],
                system_inputs: system_inputs()?,
                tools: None,
                hooks: None,
                components: Some(runtime),
                context_sources: None,
                context_token_estimator: None, context_runtime: None, verification: None, skills: None, artifacts: None,
                system_input_resolver: Some(resolver),
                external_receipt_verifier: None,
                clock,
                ids: Arc::new(RandomIdSource),
                token_estimator: Arc::new(Estimate),
                settings: AgentSettings {
                    require_durable: true,
                    max_output_tokens: 128.try_into().unwrap(),
                    ..Default::default()
                },
            },
        )?,
        catalog,
    ))
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
async fn release_finished(
    handle: &RunHandle,
    context: &ExecutionContext,
) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let view = completed(handle.component_release(context).await?)?;
            if let Some(error) = view.local_error {
                return Err::<_, Box<dyn std::error::Error>>(Box::new(error));
            }
            if let Some(report) = view.report {
                assert!(report.failures.is_empty());
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?
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
        std::env::temp_dir().join(format!("wickle-adapter-{}", RandomIdSource.next_id()?)),
    );
    std::fs::create_dir(&temporary.0)?;
    let database = temporary.0.join("state.sqlite3");
    let store = Arc::new(SqliteStateStore::open(&database)?);
    let counters = Arc::new(Counters::default());
    let model = Arc::new(Model {
        route: routing(&scope)?.route_for_binding(&reference("primary"))?,
        propose: true,
        calls: AtomicUsize::new(0),
    });
    let resolver = Arc::new(Resolver {
        value: RECORD,
        revision: "record-A",
        calls: AtomicUsize::new(0),
    });
    let (initial, catalog) = agent(
        &scope,
        store.clone(),
        model.clone(),
        resolver.clone(),
        counters.clone(),
    )?;
    assert_eq!(counters.opens.load(Ordering::SeqCst), 0);
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
    let original = completed(initial.start(request.clone(), caller.clone()).await?)?;
    let waiting = completed(original.outcome(&caller).await?)?;
    assert_eq!(waiting.result.status(), RunStatus::Waiting);
    release_finished(&original, &caller).await?;
    assert_eq!(
        (
            counters.opens.load(Ordering::SeqCst),
            counters.closes.load(Ordering::SeqCst),
            counters.writes.load(Ordering::SeqCst)
        ),
        (1, 1, 0)
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    let run_id = original.run_id().clone();
    let saved = store.load(&scope, &run_id).await?;
    let wait = saved.snapshot.wait.clone().ok_or("missing wait")?;
    let WaitTarget::Approval { target } = wait.target else {
        return Err("expected approval wait".into());
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
    let weak_store = Arc::downgrade(&store);
    let weak_model = Arc::downgrade(&model);
    let weak_resolver = Arc::downgrade(&resolver);
    drop(original);
    drop(initial);
    drop(catalog);
    drop(store);
    drop(model);
    drop(resolver);
    tokio::time::timeout(Duration::from_secs(5), async {
        while weak_store.upgrade().is_some()
            || weak_model.upgrade().is_some()
            || weak_resolver.upgrade().is_some()
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;

    let reopened = Arc::new(SqliteStateStore::open(&database)?);
    assert_eq!(
        reopened.load(&scope, &run_id).await?.snapshot,
        saved.snapshot
    );
    let model = Arc::new(Model {
        route: routing(&scope)?.route_for_binding(&reference("primary"))?,
        propose: false,
        calls: AtomicUsize::new(0),
    });
    let resolver = Arc::new(Resolver {
        value: NEW_RECORD,
        revision: "record-B",
        calls: AtomicUsize::new(0),
    });
    let (continued, catalog) = agent(
        &scope,
        reopened.clone(),
        model.clone(),
        resolver.clone(),
        counters.clone(),
    )?;
    let reviewer = context(&scope, true);
    let resumed = completed(continued.resume(command.clone(), reviewer.clone()).await?)?;
    assert_eq!(resumed.run_id(), &run_id);
    let result = completed(resumed.outcome(&reviewer).await?)?;
    assert_eq!(
        result.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded
        }
    );
    assert_eq!(
        result.output,
        vec![InputContent::Text {
            text: "Report written.".into()
        }]
    );
    release_finished(&resumed, &reviewer).await?;
    assert_eq!(
        (
            counters.opens.load(Ordering::SeqCst),
            counters.closes.load(Ordering::SeqCst),
            counters.writes.load(Ordering::SeqCst)
        ),
        (2, 2, 1)
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
    assert_eq!(catalog.calls.load(Ordering::SeqCst), 0);
    {
        let instances = counters.initialized.lock().unwrap();
        assert_ne!(instances[0].0, instances[1].0);
        assert_eq!(instances[0].1, id("requester"));
        assert_eq!(instances[1].1, id("reviewer"));
        assert_eq!(instances[0].2, instances[1].2);
    }
    let finished = reopened.load(&scope, &run_id).await?;
    assert_eq!(finished.snapshot.assembly_ref, saved.snapshot.assembly_ref);
    assert_eq!(
        finished.snapshot.system_inputs,
        saved.snapshot.system_inputs
    );
    assert_eq!(
        finished.snapshot.tool_ledger[0].call,
        saved.snapshot.tool_ledger[0].call
    );
    let previous = reopened
        .read_record(
            &scope,
            &finished.snapshot.resume_receipts[0].previous_outcome_ref,
        )
        .await?;
    assert_eq!(
        serde_json::from_value::<RunOutcome>(previous.value().clone())?,
        waiting
    );
    let events: Vec<_> = resumed
        .events(saved.snapshot.last_event_seq, reviewer.clone())
        .try_collect()
        .await?;
    assert_eq!(events[0].seq.get(), saved.snapshot.last_event_seq + 1);
    assert_eq!(
        events.last().ok_or("missing finished event")?.event_type,
        "run.finished"
    );
    let replay = completed(continued.resume(command, reviewer.clone()).await?)?;
    assert_eq!(completed(replay.outcome(&reviewer).await?)?, result);
    let start_replay = completed(continued.start(request, caller.clone()).await?)?;
    assert_eq!(completed(start_replay.outcome(&caller).await?)?, result);
    assert_eq!(
        (
            counters.opens.load(Ordering::SeqCst),
            counters.closes.load(Ordering::SeqCst),
            counters.writes.load(Ordering::SeqCst)
        ),
        (2, 2, 1)
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
    assert_eq!(catalog.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        reopened.load(&scope, &run_id).await?.snapshot,
        finished.snapshot
    );
    println!(
        "adapter consumer: real SQLite wait/reopen/resume; fresh adapter instances and binding sets; frozen mapping/system inputs; one write; explicit close; request and command replay add no factory/model/tool calls (synthetic Host, no provider network)"
    );
    Ok(())
}
