//! Real Agent segments assemble scoped adapter exports and explicitly release their instances.

#[path = "../../wickle/tests/support/agent.rs"]
#[allow(dead_code)]
mod agent_support;
#[path = "../../wickle/tests/support/mod.rs"]
#[allow(dead_code)]
mod core_fixture;
#[allow(dead_code)]
mod support;
use futures_util::stream;
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::*;
use wickle::*;
use wickle_adapter_runtime::*;

struct Catalog(Arc<AdapterRegistry>);
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            if reference.kind == ComponentKind::ModelBinding {
                return Ok(metadata(ComponentKind::ModelBinding, reference.id.as_str()));
            }
            self.0.component_metadata(reference).ok_or_else(|| {
                ContractError::new(ErrorCode::ComponentUnavailable, "fixture.component")
            })
        })
    }
}
struct Model {
    calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
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
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        let mut events = if call == 0 {
            (0..3)
                .map(|index| {
                    Ok(ModelEvent::ToolArgumentsDelta {
                        index,
                        provider_call_id: Some(format!("call-{index}")),
                        name: Some(format!("search_{index}")),
                        delta: json!({"query":format!("query-{index}")}).to_string(),
                    })
                })
                .collect::<Vec<_>>()
        } else {
            vec![Ok(ModelEvent::TextDelta {
                text: "Observed results".into(),
            })]
        };
        events.push(Ok(ModelEvent::ResponseCompleted {
            finish: if call == 0 {
                ModelFinish::ToolCalls
            } else {
                ModelFinish::Stop
            },
            metadata: Default::default(),
            continuation: vec![],
        }));
        Box::pin(stream::iter(events))
    }
}
struct AgentFixture {
    adapters: Fixture,
    base: agent_support::Fixture,
    model: Arc<Model>,
    registry: Arc<AdapterRegistry>,
    profile: AgentProfile,
}
impl AgentFixture {
    fn new() -> Self {
        let mut adapters = Fixture::new();
        adapters.add_adapter("writer");
        adapters.add_adapter("last");
        let AdapterExportDefinition::Tool { descriptor, .. } =
            &mut adapters.definitions[1].exports[0]
        else {
            unreachable!()
        };
        descriptor.side_effect = ToolSideEffect::Write;
        adapters.factories[1] = Arc::new(Factory::new(
            adapters.definitions[1].clone(),
            adapters.events.clone(),
            adapters.store.clone(),
        ));
        let value = json!({"thread_id":"host-prepared-thread"});
        adapters.states.push(AdapterBindingState {
            scope: scope(),
            session_id: id("session"),
            adapter_binding: id("binding-1"),
            adapter: reference("writer"),
            definition_digest: adapters.definitions[1].digest(),
            state_ref: ProtectedRecord::new(id("mapping"), 1, value.clone())
                .reference()
                .clone(),
            value,
        });
        let registry = Arc::new(adapters.registry().unwrap());
        Self {
            adapters,
            base: agent_support::Fixture::new(agent_support::Response::Text, false),
            model: Arc::new(Model {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(vec![]),
            }),
            registry,
            profile: multi_profile(&["adapter", "writer", "last"]),
        }
    }
    fn bindings(&self) -> AgentBindings {
        let mut bindings = self.base.bindings();
        bindings.state = self.adapters.store.clone();
        bindings.clock = self.adapters.clock.clone();
        bindings.profile_resolver = Arc::new(Catalog(self.registry.clone()));
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
        bindings.router = Arc::new(router);
        let policy = Arc::new(
            PolicyGate::new(self.adapters.policy.clone(), Duration::from_secs(5)).unwrap(),
        );
        bindings.policy = policy.clone();
        bindings.model_exchange = Arc::new(
            ModelExchange::new(self.model.clone(), policy)
                .with_route_inspector(self.base.inspector.clone(), Duration::from_secs(5))
                .unwrap(),
        );
        bindings.system_inputs = inputs();
        bindings.tools = None;
        bindings.hooks = None;
        bindings.components = Some(Arc::new(self.adapters.runtime(self.registry.clone())));
        bindings.settings.lease_ttl_ms = 30_000;
        bindings.settings.heartbeat_interval_ms = 5_000;
        bindings
    }
    fn agent(&self) -> Agent {
        create_agent(self.profile.clone(), self.bindings()).unwrap()
    }
    async fn start(&self, agent: &Agent) -> RunHandle {
        let mut context = agent_support::context();
        context.data.system_inputs = Some(SystemInputs::new(object(
            json!({"workspace_id":"11111111-1111-4111-8111-111111111111"}),
        )));
        agent_support::completed(
            agent
                .start(agent_support::request("request"), context)
                .await
                .unwrap(),
        )
    }
    async fn outcome(&self, handle: &RunHandle) -> RunOutcome {
        agent_support::completed(
            tokio::time::timeout(
                Duration::from_secs(10),
                handle.outcome(&agent_support::context()),
            )
            .await
            .unwrap()
            .unwrap(),
        )
    }
}
async fn released(handle: &RunHandle) -> ComponentReleaseView {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let report = agent_support::completed(
                handle
                    .component_release(&agent_support::context())
                    .await
                    .unwrap(),
            );
            if report.report.is_some() || report.local_error.is_some() {
                return report;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn approval_wait_releases_all_instances_and_resume_uses_new_bindings_with_the_frozen_mapping()
{
    let fixture = AgentFixture::new();
    fixture
        .adapters
        .policy
        .require_approval
        .store(1, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.start(&agent).await;
    let waiting = fixture.outcome(&original).await;
    assert_eq!(waiting.result.status(), RunStatus::Waiting);
    assert!(
        released(&original)
            .await
            .report
            .unwrap()
            .failures
            .is_empty()
    );
    let saved = fixture
        .adapters
        .store
        .load(&scope(), original.run_id())
        .await
        .unwrap();
    assert_eq!(
        *fixture.adapters.events.lock().unwrap(),
        vec![
            "open:binding-0",
            "open:binding-1",
            "open:binding-2",
            "close:binding-2",
            "close:binding-1",
            "close:binding-0"
        ]
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.adapters.factories[0].instances.lock().unwrap()[0]
            .executor
            .calls
            .load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        fixture.adapters.factories[1].instances.lock().unwrap()[0]
            .executor
            .calls
            .load(Ordering::SeqCst),
        0
    );
    let wait = saved.snapshot.wait.clone().unwrap();
    let WaitTarget::Approval { target } = wait.target else {
        panic!("approval required")
    };
    let command = ResumeCommand {
        run_id: original.run_id().clone(),
        expected_revision: saved.snapshot.revision,
        command_id: id("approve"),
        action: ResumeAction::Approve {
            wait_id: wait.wait_id,
            target,
        },
    };
    let mut reviewer = agent_support::context();
    reviewer.data.principal_ref = id("reviewer");
    reviewer.data.capability_grant_ref = id("reviewer-grant");
    let resumed = agent_support::completed(agent.resume(command.clone(), reviewer).await.unwrap());
    let outcome = fixture.outcome(&resumed).await;
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert!(released(&resumed).await.report.unwrap().failures.is_empty());
    let initial = fixture.adapters.factories[1].observed.lock().unwrap()[0].clone();
    let next = fixture.adapters.factories[1].observed.lock().unwrap()[1].clone();
    assert_ne!(
        initial.execution.binding_set_id,
        next.execution.binding_set_id
    );
    assert_eq!(initial.binding.binding_state, next.binding.binding_state);
    assert_eq!(
        next.binding.binding_state.unwrap().value,
        json!({"thread_id":"host-prepared-thread"})
    );
    assert_eq!(next.execution.principal_ref, id("caller"));
    let final_saved = fixture
        .adapters
        .store
        .load(&scope(), original.run_id())
        .await
        .unwrap();
    assert_eq!(
        saved.snapshot.assembly_ref,
        final_saved.snapshot.assembly_ref
    );
    assert_eq!(
        saved.snapshot.tool_ledger[1].call,
        final_saved.snapshot.tool_ledger[1].call
    );
    assert_eq!(
        fixture.adapters.factories[0].instances.lock().unwrap()[1]
            .executor
            .calls
            .load(Ordering::SeqCst),
        0
    );
    let writer = fixture.adapters.factories[1].instances.lock().unwrap()[1].clone();
    assert_eq!(writer.executor.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        writer.executor.seen.lock().unwrap()[0].0["workspace_id"],
        json!("11111111-1111-4111-8111-111111111111")
    );
    let replay = agent_support::completed(
        agent
            .resume(command, agent_support::context())
            .await
            .unwrap(),
    );
    assert_eq!(fixture.outcome(&replay).await, outcome);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
    assert!(
        fixture
            .adapters
            .factories
            .iter()
            .all(|factory| factory.opens.load(Ordering::SeqCst) == 2)
    );
    assert!(fixture.adapters.factories.iter().all(|factory| {
        factory
            .instances
            .lock()
            .unwrap()
            .iter()
            .all(|instance| instance.close_calls.load(Ordering::SeqCst) == 1)
    }));
}

#[tokio::test]
async fn initialization_and_dispatch_permissions_remain_separate_for_adapter_exports() {
    let fixture = AgentFixture::new();
    fixture.adapters.policy.deny_tool.store(1, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    fixture.outcome(&handle).await;
    released(&handle).await;
    assert!(
        fixture
            .adapters
            .factories
            .iter()
            .all(|factory| factory.opens.load(Ordering::SeqCst) == 1)
    );
    assert!(fixture.adapters.factories.iter().all(|factory| {
        factory.instances.lock().unwrap()[0]
            .executor
            .calls
            .load(Ordering::SeqCst)
            == 0
    }));
    let actions = fixture.adapters.policy.seen.lock().unwrap();
    let selections: Vec<_> = actions
        .iter()
        .filter_map(|action| match action {
            PolicyAction::ExecuteTool { input } => input.selection().cloned(),
            _ => None,
        })
        .collect();
    assert_eq!(selections, fixture.profile.tools);
}

#[tokio::test]
async fn close_errors_are_reported_after_success_without_changing_the_saved_outcome() {
    let fixture = AgentFixture::new();
    fixture.adapters.factories[1]
        .close_behavior
        .store(1, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    let result = fixture.outcome(&handle).await;
    assert_eq!(result.result.status(), RunStatus::Succeeded);
    let before = fixture
        .adapters
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    let report = released(&handle).await;
    assert!(report.local_error.is_none());
    assert_eq!(
        report.report.unwrap().failures[0].adapter_binding,
        id("binding-1")
    );
    assert_eq!(
        fixture
            .adapters
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot,
        before.snapshot
    );
    assert_eq!(fixture.outcome(&handle).await, result);
    assert!(fixture.adapters.factories.iter().all(|factory| {
        factory.instances.lock().unwrap()[0]
            .close_calls
            .load(Ordering::SeqCst)
            == 1
    }));
}

#[test]
fn component_runtime_and_direct_tool_bindings_cannot_be_mixed() {
    let fixture = AgentFixture::new();
    let mut bindings = fixture.bindings();
    bindings.tools = Some(Arc::new(ToolRegistry::new(scope(), vec![]).unwrap()));
    assert!(create_agent(fixture.profile.clone(), bindings).is_err());
    assert!(
        fixture
            .adapters
            .factories
            .iter()
            .all(|factory| factory.opens.load(Ordering::SeqCst) == 0)
    );
}

struct WrongRuntime {
    inner: Arc<dyn ComponentRuntime>,
}
struct ForwardRelease {
    original: BoundCapabilities,
}
impl ComponentRelease for ForwardRelease {
    fn invalidate(&self) {}
    fn release<'a>(
        &'a self,
        context: &'a ComponentReleaseContext,
    ) -> PortFuture<'a, ComponentReleaseReport> {
        self.original.release(context)
    }
}
impl ComponentRuntime for WrongRuntime {
    fn resolve<'a>(
        &'a self,
        profile: &'a ResolvedProfile,
        context: &'a ComponentResolveContext,
    ) -> PortFuture<'a, ResolvedAssembly> {
        self.inner.resolve(profile, context)
    }
    fn bind<'a>(
        &'a self,
        assembly: &'a ResolvedAssembly,
        context: &'a ComponentBindContext,
    ) -> PortFuture<'a, BoundCapabilities> {
        Box::pin(async move {
            let original = self.inner.bind(assembly, context).await?;
            if context.purpose == ComponentBindPurpose::ObserversOnly {
                return Ok(original);
            }
            let mut entries = vec![];
            for (index, binding) in assembly.tools().iter().enumerate() {
                let mut descriptor = binding.compiled.descriptor().clone();
                if index == 0 {
                    descriptor.output_schema = json!({"type":"integer"});
                }
                entries.push((
                    binding.selection.clone(),
                    ToolRegistration {
                        compiled: SchemaCompiler::new().compile(descriptor, &inputs())?,
                        executor: original
                            .tools()
                            .get(&binding.compiled.descriptor().name)
                            .unwrap()
                            .executor
                            .clone(),
                    },
                ));
            }
            BoundCapabilities::new(
                context.scope.clone(),
                context.run_id.clone(),
                context.binding_set_id.clone(),
                Arc::new(ToolRegistry::from_bindings(context.scope.clone(), entries)?),
                original.hooks().clone(),
                original.sources().clone(),
                Arc::new(ForwardRelease { original }),
            )
        })
    }
}

#[tokio::test]
async fn a_custom_runtime_cannot_replace_the_pinned_contract_and_its_resources_are_released() {
    let fixture = AgentFixture::new();
    fixture.adapters.factories[1]
        .close_behavior
        .store(1, Ordering::SeqCst);
    let mut bindings = fixture.bindings();
    bindings.components = Some(Arc::new(WrongRuntime {
        inner: bindings.components.take().unwrap(),
    }));
    let agent = create_agent(fixture.profile.clone(), bindings).unwrap();
    let handle = fixture.start(&agent).await;
    let result = fixture.outcome(&handle).await;
    assert!(
        matches!(&result.result,OutcomeResult::Failed{failure} if failure.code==id("context_mismatch"))
    );
    let report = released(&handle).await;
    assert!(
        report
            .report
            .unwrap()
            .failures
            .iter()
            .any(|failure| failure.adapter_binding == id("binding-1"))
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
    assert!(fixture.adapters.factories.iter().all(|factory| {
        factory.instances.lock().unwrap()[0]
            .executor
            .calls
            .load(Ordering::SeqCst)
            == 0
    }));
    assert!(fixture.adapters.factories.iter().all(|factory| {
        factory.instances.lock().unwrap()[0]
            .close_calls
            .load(Ordering::SeqCst)
            == 1
    }));
}

#[tokio::test(start_paused = true)]
async fn cleanup_releases_the_lease_before_bounded_adapter_close_and_preserves_the_outcome() {
    let fixture = AgentFixture::new();
    fixture.adapters.factories[2]
        .close_behavior
        .store(2, Ordering::SeqCst);
    let mut bindings = fixture.bindings();
    bindings.components = Some(Arc::new(
        fixture
            .adapters
            .runtime(fixture.registry.clone())
            .with_settings(AdapterRuntimeSettings {
                close_timeout_ms: 30_000,
                ..Default::default()
            })
            .unwrap(),
    ));
    let agent = create_agent(fixture.profile.clone(), bindings).unwrap();
    let handle = fixture.start(&agent).await;
    let result = fixture.outcome(&handle).await;
    assert_eq!(result.result.status(), RunStatus::Succeeded);
    let instance = fixture.adapters.factories[2].instances.lock().unwrap()[0].clone();
    tokio::time::timeout(Duration::from_secs(1), instance.close_entered.notified())
        .await
        .unwrap();
    let image =
        serde_json::to_value(fixture.adapters.store.export_checkpoint(&scope()).unwrap()).unwrap();
    assert!(image["runs"][0]["lease"].is_null());
    tokio::time::advance(Duration::from_millis(5001)).await;
    let release = released(&handle).await;
    assert!(
        release
            .local_error
            .as_ref()
            .is_some_and(|error| error.code == ErrorCode::DeadlineExceeded)
            || release
                .report
                .as_ref()
                .is_some_and(|report| !report.failures.is_empty()),
        "cleanup report: {release:?}"
    );
    assert_eq!(fixture.outcome(&handle).await, result);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(instance.close_calls.load(Ordering::SeqCst), 1);
}

struct ScopedModel {
    owner: Scope,
    query: String,
    calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
}
impl ModelPort for ScopedModel {
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
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        assert_eq!(context.scope, self.owner);
        self.requests.lock().unwrap().push(request.clone());
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        let event = if first {
            ModelEvent::ToolArgumentsDelta {
                index: 0,
                provider_call_id: Some("lookup".into()),
                name: Some("search_0".into()),
                delta: json!({"query":self.query}).to_string(),
            }
        } else {
            ModelEvent::TextDelta {
                text: "complete".into(),
            }
        };
        Box::pin(stream::iter(vec![
            Ok(event),
            Ok(ModelEvent::ResponseCompleted {
                finish: if first {
                    ModelFinish::ToolCalls
                } else {
                    ModelFinish::Stop
                },
                metadata: Default::default(),
                continuation: vec![],
            }),
        ]))
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simultaneous_tenants_keep_user_thread_mappings_and_mutable_instances_separate() {
    let adapters = Fixture::new();
    let factory = adapters.factories[0].clone();
    factory.behavior.store(7, Ordering::SeqCst);
    let mut cases = vec![];
    for (tenant, user, workspace, thread, query) in [
        (
            "tenant-a",
            "user-a",
            "11111111-1111-4111-8111-111111111111",
            "thread-a",
            "query-a",
        ),
        (
            "tenant-b",
            "user-b",
            "22222222-2222-4222-8222-222222222222",
            "thread-b",
            "query-b",
        ),
    ] {
        let owner = Scope {
            tenant_id: id(tenant),
            workspace_id: id("workspace"),
            user_id: Some(id(user)),
        };
        let state = json!({"thread_id":thread,"external_user":user});
        let mapping = AdapterBindingState {
            scope: owner.clone(),
            session_id: id("session"),
            adapter_binding: id("binding-0"),
            adapter: reference("adapter"),
            definition_digest: adapters.definitions[0].digest(),
            state_ref: ProtectedRecord::new(id(&format!("mapping-{tenant}")), 1, state.clone())
                .reference()
                .clone(),
            value: state,
        };
        let registry = Arc::new(
            AdapterRegistry::new(
                owner.clone(),
                vec![AdapterRegistration {
                    definition: adapters.definitions[0].clone(),
                    factory: factory.clone(),
                }],
                adapters.connections.clone(),
                vec![],
                vec![],
                vec![mapping],
            )
            .unwrap(),
        );
        let base = agent_support::Fixture::new(agent_support::Response::Text, false);
        let model = Arc::new(ScopedModel {
            owner: owner.clone(),
            query: query.into(),
            calls: AtomicUsize::new(0),
            requests: Mutex::new(vec![]),
        });
        let mut bindings = base.bindings();
        bindings.scope = owner.clone();
        bindings.state = adapters.store.clone();
        bindings.clock = adapters.clock.clone();
        bindings.ids = Arc::new(RandomIdSource);
        bindings.profile_resolver = Arc::new(Catalog(registry.clone()));
        let mut catalog = base.router.snapshot.catalog().clone();
        let mut policy = base.router.snapshot.policy().clone();
        catalog.scope = owner.clone();
        policy.scope = owner.clone();
        catalog.models[0]
            .capabilities
            .features
            .insert(id("tool_calling"));
        catalog.bindings[0].capabilities = catalog.models[0].capabilities.clone();
        catalog.bindings[0].evidence[0].binding_digest = catalog.bindings[0]
            .contract_digest(&catalog.models[0])
            .unwrap();
        bindings.router = Arc::new(agent_support::Router {
            snapshot: RoutingSnapshot::new(catalog, policy).unwrap(),
            queries: AtomicUsize::new(0),
            snapshots: AtomicUsize::new(0),
        });
        bindings.policy =
            Arc::new(PolicyGate::new(adapters.policy.clone(), Duration::from_secs(5)).unwrap());
        bindings.model_exchange = Arc::new(
            ModelExchange::new(model.clone(), bindings.policy.clone())
                .with_route_inspector(base.inspector.clone(), Duration::from_secs(5))
                .unwrap(),
        );
        bindings.system_inputs = inputs();
        bindings.components = Some(Arc::new(adapters.runtime(registry)));
        bindings.tools = None;
        bindings.settings.lease_ttl_ms = 30000;
        bindings.settings.heartbeat_interval_ms = 5000;
        let agent = create_agent(multi_profile(&["adapter"]), bindings).unwrap();
        let mut context = agent_support::context();
        context.data.scope = owner.clone();
        context.data.principal_ref = id(user);
        context.data.system_inputs =
            Some(SystemInputs::new(object(json!({"workspace_id":workspace}))));
        let start_agent = agent.clone();
        let start_context = context.clone();
        let start = tokio::spawn(async move {
            agent_support::completed(
                start_agent
                    .start(agent_support::request("same-request"), start_context)
                    .await
                    .unwrap(),
            )
        });
        cases.push((agent, context, model, workspace, thread, query, start));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while factory.observed.lock().unwrap().len() != 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    // Both Runs are inside the shared factory before either may create an instance.
    assert!(factory.instances.lock().unwrap().is_empty());
    factory.release.add_permits(2);
    let mut finished = vec![];
    for (agent, context, model, workspace, thread, query, start) in cases {
        let handle = start.await.unwrap();
        let outcome = agent_support::completed(
            tokio::time::timeout(Duration::from_secs(10), handle.outcome(&context))
                .await
                .unwrap()
                .unwrap(),
        );
        assert_eq!(
            outcome.result.status(),
            RunStatus::Succeeded,
            "{:?}",
            outcome
        );
        assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        let observed = factory
            .observed
            .lock()
            .unwrap()
            .iter()
            .find(|item| item.execution.scope == context.data.scope)
            .cloned()
            .unwrap();
        assert_eq!(observed.execution.principal_ref, context.data.principal_ref);
        assert_eq!(
            observed.binding.binding_state.unwrap().value,
            json!({"thread_id":thread,"external_user":context.data.principal_ref})
        );
        let instance = factory
            .instances
            .lock()
            .unwrap()
            .iter()
            .find(|item| item.scope == context.data.scope)
            .cloned()
            .unwrap();
        assert_eq!(instance.run_id, *handle.run_id());
        assert_eq!(instance.executor.calls.load(Ordering::SeqCst), 1);
        let seen = instance.executor.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0["workspace_id"], workspace);
        assert_eq!(seen[0].0["query"], query);
        assert_eq!(seen[0].1.scope, context.data.scope);
        drop(seen);
        for request in model.requests.lock().unwrap().iter() {
            let encoded = serde_json::to_string(request).unwrap();
            assert!(!encoded.contains(workspace));
            assert!(!encoded.contains(thread));
        }
        finished.push((agent, context, handle));
    }
    assert_eq!(factory.instances.lock().unwrap().len(), 2);
    assert_eq!(
        adapters
            .store
            .load(&finished[0].1.data.scope, finished[1].2.run_id())
            .await
            .unwrap_err()
            .code,
        ErrorCode::StateNotFound
    );
    assert_eq!(
        finished[0]
            .0
            .get_run(finished[0].2.run_id(), &finished[1].1)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
}
