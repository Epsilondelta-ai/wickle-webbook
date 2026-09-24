# 58장 전체 구현과 변경 검사

[강의](../58-recovery-audit.md) · [전체 변경 패치](../solutions/58-recovery-audit.patch)

기준 `1fbb643b1ae798b1312a305e9869b8a2a3b456b9`. 이 단계에서 바뀐 Rust·manifest·Python 파일의 전체 내용이다. 이전 버전과의 정확한 교체 위치·삭제는 patch를 따른다. 다음 장의 코드와 섞지 않는다.

## `crates/wickle-adapter-runtime/tests/agent_components.rs`

```rust
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
```

## `crates/wickle-model-azure-openai/tests/responses.rs`

```rust
//! Azure inference/authentication and deployment metadata transport contracts.
mod support;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::*;
use wickle::*;
use wickle_model_azure_openai::*;

struct Tokens(AtomicUsize);
impl AzureCredentialProvider for Tokens {
    fn credential<'a>(
        &'a self,
        context: &'a AzureCredentialContext<'a>,
    ) -> PortFuture<'a, AzureCredential> {
        Box::pin(async move {
            assert_eq!(context.scope, &scope());
            assert_eq!(context.audience, AzureAudience::Inference);
            let number = self.0.fetch_add(1, Ordering::SeqCst);
            Ok(AzureCredential::EntraToken(format!("token-{number}")))
        })
    }
}

#[tokio::test]
async fn deployment_mapping_preserves_model_identity_and_refreshes_entra_per_request() {
    for entra in [false, true] {
        let mut reply = Reply::sse(&events("base-model", "42"));
        reply.headers = vec![("apim-request-id", "azure-request".into())];
        let server = Server::new(vec![reply, Reply::sse(&events("base-model", "43"))]).await;
        let tokens = Arc::new(Tokens(AtomicUsize::new(0)));
        let credentials: Arc<dyn AzureCredentialProvider> = if entra {
            tokens.clone()
        } else {
            Arc::new(AzureCredential::ApiKey("fixture-key".into()))
        };
        let connection = AzureOpenAiConnection::new(
            scope(),
            reference("account"),
            credentials,
            options(&server),
        )
        .unwrap();
        let model = AzureOpenAiModel::new(connection.clone());
        let mut request = request(&connection, "base-model");
        let original = request.route.clone();
        for answer in ["42", "43"] {
            let response =
                collect_model_response(&request, model.generate(&request, &context(&request)))
                    .await
                    .unwrap();
            assert_eq!(response.text, answer);
            assert_eq!(response.metadata.reported_model_id, Some(id("base-model")));
            assert!(response.metadata.reported_model_version.is_none());
            assert_eq!(response.metadata.usage.unwrap().output_tokens, Some(7));
            if answer == "42" {
                assert_eq!(
                    response.metadata.provider_request_id,
                    Some(id("azure-request"))
                );
            }
            request.request_id = id("second-attempt");
        }
        assert_eq!(request.route, original);
        assert_eq!(tokens.0.load(Ordering::SeqCst), if entra { 2 } else { 0 });
        let calls = server.requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        for (index, call) in calls.iter().enumerate() {
            assert_eq!(call.method, "POST");
            assert_eq!(call.path, "/openai/v1/responses");
            assert_eq!(call.body["model"], "finance-deployment");
            assert_eq!(call.body["reasoning"]["effort"], "medium");
            assert_eq!(call.body["store"], false);
            let headers = call.headers.to_ascii_lowercase();
            if entra {
                assert!(headers.contains(&format!("authorization: bearer token-{index}")));
                assert!(!headers.contains("api-key:"));
            } else {
                assert!(headers.contains("api-key: fixture-key"));
                assert!(!headers.contains("authorization:"));
            }
            assert!(!call.body.to_string().contains("hidden-workspace"));
            assert!(!call.body.to_string().contains("fixture-key"));
        }
    }
}

#[tokio::test]
async fn wrong_scope_route_api_and_credentials_never_make_an_http_request() {
    for case in [
        "scope",
        "target",
        "provider",
        "api",
        "options",
        "credential",
    ] {
        let server = Server::new(vec![]).await;
        let tokens = Arc::new(Tokens(AtomicUsize::new(0)));
        let credentials: Arc<dyn AzureCredentialProvider> = if case == "credential" {
            Arc::new(AzureCredential::ApiKey("bad\nkey".into()))
        } else {
            tokens.clone()
        };
        let connection = AzureOpenAiConnection::new(
            scope(),
            reference("account"),
            credentials,
            options(&server),
        )
        .unwrap();
        let model = AzureOpenAiModel::new(connection.clone());
        let mut request = request(&connection, "base-model");
        let mut context = context(&request);
        match case {
            "scope" => context.scope.workspace_id = id("another-workspace"),
            "target" => {
                request
                    .route
                    .target
                    .insert("deployment".into(), json!("another-deployment"));
            }
            "provider" => request.route.provider = id("openai"),
            "api" => request.route.api_contract.version = id("2025-04-01-preview"),
            "options" => {
                request.options.insert("model".into(), json!("injected"));
            }
            _ => {}
        }
        let failure = collect_model_response(&request, model.generate(&request, &context))
            .await
            .unwrap_err();
        if case == "credential" {
            assert_eq!(failure.kind, ModelFailureKind::Authentication);
        }
        assert!(server.requests.lock().unwrap().is_empty());
        assert_eq!(tokens.0.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn azure_tools_and_json_output_keep_original_route_bound_replay() {
    let call = json!({"id":"fc","type":"function_call","call_id":"call-1","name":"lookup","arguments":"{\"query\":\"figures\"}","status":"completed"});
    let first = vec![
        json!({"type":"response.created","response":{"id":"resp_1","model":"base-model","status":"in_progress"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"fc","type":"function_call","call_id":"call-1","name":"lookup","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":0,"item_id":"fc","delta":"{\"query\":\"figures\"}"}),
        json!({"type":"response.completed","response":{"id":"resp_1","model":"base-model","status":"completed","output":[call]}}),
    ];
    let server = Server::new(vec![
        Reply::sse(&first),
        Reply::sse(&events("base-model", "{\"answer\":42}")),
    ])
    .await;
    let connection = connection(&server);
    let model = AzureOpenAiModel::new(connection.clone());
    let mut request = request(&connection, "base-model");
    request.tools = vec![ModelTool {
        name: id("lookup"),
        description: "Read figures".into(),
        model_input_schema: json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}),
    }];
    let first = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(first.finish, ModelFinish::ToolCalls);
    assert_eq!(first.tool_calls[0].model_inputs["query"], "figures");
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![
            ModelContent::ToolCall {
                provider_call_id: id("call-1"),
                name: id("lookup"),
                arguments: JsonObject::from([("query".into(), json!("figures"))]),
            },
            ModelContent::Opaque {
                continuation: first.continuation[0].clone(),
            },
        ],
    });
    request.messages.push(ModelMessage {
        role: ModelRole::Tool,
        content: vec![ModelContent::ToolResult {
            provider_call_id: id("call-1"),
            content: json!({"answer":42}),
        }],
    });
    request.output = ModelOutput::JsonSchema {
        schema: json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}),
    };
    let response = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(parse_json(&response.text).unwrap(), json!({"answer":42}));
    let calls = server.requests.lock().unwrap();
    let input = calls[1].body["input"].as_array().unwrap();
    assert_eq!(
        input
            .iter()
            .filter(|value| value["type"] == "function_call")
            .count(),
        1
    );
    assert_eq!(calls[1].body["model"], "finance-deployment");
    assert_eq!(calls[1].body["text"]["format"]["strict"], true);
    assert_eq!(calls[0].body["tools"][0]["strict"], true);
}

fn inspection_context() -> ModelInspectionContext {
    ModelInspectionContext {
        scope: scope(),
        principal_ref: id("user"),
        capability_grant_ref: id("grant"),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(3),
    }
}
fn deployment(version: &str) -> Value {
    json!({"id":format!("{ACCOUNT}/deployments/finance-deployment"),"name":"finance-deployment","type":"Microsoft.CognitiveServices/accounts/deployments","properties":{"model":{"format":"OpenAI","name":"base-model","version":version},"provisioningState":"Succeeded","versionUpgradeOption":"OnceNewDefaultVersionAvailable"}})
}
fn inspector(
    connection: AzureOpenAiConnection,
    server: &Server,
    credentials: AzureCredential,
) -> AzureOpenAiInspector {
    AzureOpenAiInspector::new(
        connection,
        Arc::new(credentials),
        AzureInspectionOptions {
            endpoint: origin(server),
            ..Default::default()
        },
    )
    .unwrap()
}

#[tokio::test]
async fn arm_inspection_detects_model_upgrade_without_claiming_deployment_immutability() {
    let inference = Server::new(vec![]).await;
    let management = Server::new(vec![
        Reply::json(200, deployment("release")),
        Reply::json(200, deployment("next-release")),
    ])
    .await;
    let connection = connection(&inference);
    let mut request = request(&connection, "base-model");
    let inspector = inspector(
        connection,
        &management,
        AzureCredential::EntraToken("management-token".into()),
    );
    let first = inspector
        .inspect(&request.route, &inspection_context())
        .await
        .unwrap();
    assert_eq!(first.model_version, Some(id("release")));
    assert_eq!(first.version_semantics, VersionSemantics::MutableDeployment);
    first
        .validate(&request.route, VersionPolicy::AllowMutable)
        .unwrap();
    assert_eq!(
        first
            .validate(&request.route, VersionPolicy::RequirePinned)
            .unwrap_err()
            .code,
        ErrorCode::ModelVersionUnpinned
    );
    request.route.deployment_revision = first.deployment_revision.clone();
    let changed = inspector
        .inspect(&request.route, &inspection_context())
        .await
        .unwrap();
    assert_eq!(changed.model_version, Some(id("next-release")));
    assert_ne!(changed.deployment_revision, first.deployment_revision);
    assert_eq!(
        changed
            .validate(&request.route, VersionPolicy::AllowMutable)
            .unwrap_err()
            .code,
        ErrorCode::ModelVersionDrift
    );
    assert!(inference.requests.lock().unwrap().is_empty());
    for call in management.requests.lock().unwrap().iter() {
        assert_eq!(call.method, "GET");
        assert_eq!(
            call.path,
            format!("{ACCOUNT}/deployments/finance-deployment?api-version=2025-06-01")
        );
        assert!(
            call.headers
                .to_ascii_lowercase()
                .contains("authorization: bearer management-token")
        );
        assert!(!call.headers.contains("fixture-key"));
    }
}

#[tokio::test]
async fn metadata_unavailability_mismatch_and_wrong_audience_do_not_invent_a_version() {
    for case in [
        "missing",
        "wrong-target",
        "version-missing",
        "provisioning",
        "api-key",
    ] {
        let mut body = deployment("release");
        if case == "wrong-target" {
            body["id"] = json!(format!("{ACCOUNT}/deployments/other"));
        }
        if case == "version-missing" {
            body["properties"]["model"]
                .as_object_mut()
                .unwrap()
                .remove("version");
        }
        if case == "provisioning" {
            body["properties"]["provisioningState"] = json!("Updating");
        }
        let server = Server::new(vec![Reply::json(
            if case == "missing" { 404 } else { 200 },
            body,
        )])
        .await;
        let connection = connection(&server);
        let request = request(&connection, "base-model");
        let credential = if case == "api-key" {
            AzureCredential::ApiKey("data-plane-key".into())
        } else {
            AzureCredential::EntraToken("management-token".into())
        };
        let result = inspector(connection, &server, credential)
            .inspect(&request.route, &inspection_context())
            .await;
        match case {
            "missing" => {
                let value = result.unwrap();
                assert_eq!(value.availability, ModelRouteAvailability::Unavailable);
                assert!(value.model_version.is_none());
            }
            "provisioning" => assert_eq!(
                result.unwrap().availability,
                ModelRouteAvailability::Unknown
            ),
            _ => assert!(result.is_err(), "{case}"),
        }
        if case == "api-key" {
            assert!(server.requests.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn azure_errors_and_redirects_do_not_retry_or_forward_credentials() {
    let redirected = Server::new(vec![]).await;
    for (status, expected) in [
        (401, ModelFailureKind::Authentication),
        (429, ModelFailureKind::RateLimited),
        (503, ModelFailureKind::Transport),
        (307, ModelFailureKind::Unsupported),
    ] {
        let mut reply = Reply::json(status, json!({"error":{"message":"private diagnostics"}}));
        if status == 307 {
            reply
                .headers
                .push(("location", format!("{}responses", redirected.base)));
        }
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let request = request(&connection, "base-model");
        let model = AzureOpenAiModel::new(connection);
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, expected);
        assert!(!format!("{failure:?}").contains("private diagnostics"));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
    assert!(redirected.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancelled_or_truncated_streams_never_become_a_completed_response() {
    for cancel in [false, true] {
        let mut data = events("base-model", "partial");
        data.truncate(3);
        let mut reply = Reply::sse(&data);
        reply.stall = cancel;
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let request = request(&connection, "base-model");
        let model = AzureOpenAiModel::new(connection);
        let context = context(&request);
        let mut stream = model.generate(&request, &context);
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ModelEvent::TextDelta { .. }
        ));
        server.entered.notified().await;
        if cancel {
            context.cancellation.cancel();
        }
        let terminal = stream.next().await.unwrap();
        if cancel {
            assert_eq!(terminal.unwrap_err().code, ErrorCode::Cancelled);
        } else {
            assert!(matches!(
                terminal.unwrap(),
                ModelEvent::ResponseError {
                    kind: ModelFailureKind::Protocol,
                    ..
                }
            ));
        }
        assert!(stream.next().await.is_none());
        tokio::time::timeout(Duration::from_secs(1), server.closed.notified())
            .await
            .unwrap();
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

struct PendingCredential(tokio::sync::Notify);
impl AzureCredentialProvider for PendingCredential {
    fn credential<'a>(
        &'a self,
        _: &'a AzureCredentialContext<'a>,
    ) -> PortFuture<'a, AzureCredential> {
        Box::pin(async move {
            self.0.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_and_deadline_bound_host_token_refresh_before_http() {
    for cancel in [false, true] {
        let server = Server::new(vec![]).await;
        let credentials = Arc::new(PendingCredential(tokio::sync::Notify::new()));
        let connection = AzureOpenAiConnection::new(
            scope(),
            reference("account"),
            credentials.clone(),
            options(&server),
        )
        .unwrap();
        let model = AzureOpenAiModel::new(connection.clone());
        let request = request(&connection, "base-model");
        let mut context = context(&request);
        context.deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let mut stream = model.generate(&request, &context);
        let mut next = Box::pin(stream.next());
        tokio::select! {
            _ = credentials.0.notified() => {},
            value = &mut next => panic!("refresh completed unexpectedly: {value:?}"),
        }
        if cancel {
            context.cancellation.cancel();
        } else {
            tokio::time::advance(Duration::from_secs(2)).await;
        }
        let value = next.await.unwrap();
        if cancel {
            assert_eq!(value.unwrap_err().code, ErrorCode::Cancelled);
        } else {
            assert!(matches!(
                value.unwrap(),
                ModelEvent::ResponseError {
                    kind: ModelFailureKind::Timeout,
                    ..
                }
            ));
        }
        assert!(stream.next().await.is_none());
        assert!(server.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn arm_scope_and_target_rejection_precedes_credential_lookup() {
    for wrong_scope in [true, false] {
        let server = Server::new(vec![]).await;
        let connection = connection(&server);
        let mut request = request(&connection, "base-model");
        let credentials = Arc::new(Tokens(AtomicUsize::new(0)));
        let inspector = AzureOpenAiInspector::new(
            connection,
            credentials.clone(),
            AzureInspectionOptions {
                endpoint: origin(&server),
                ..Default::default()
            },
        )
        .unwrap();
        let mut context = inspection_context();
        if wrong_scope {
            context.scope.workspace_id = id("different-workspace");
        } else {
            request
                .route
                .target
                .insert("deployment".into(), json!("different-deployment"));
        }
        assert!(inspector.inspect(&request.route, &context).await.is_err());
        assert_eq!(credentials.0.load(Ordering::SeqCst), 0);
        assert!(server.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn two_documented_releases_keep_deployment_and_model_identity_distinct() {
    let releases = ["gpt-6-astra", "gpt-5.6-sol"];
    let server = Server::new(
        releases
            .iter()
            .map(|release| Reply::sse(&events(release, release)))
            .collect(),
    )
    .await;
    let mut bindings = vec![];
    for (index, release) in releases.iter().enumerate() {
        let mut options = options(&server);
        options.deployment = format!("deployment-{index}");
        let connection = AzureOpenAiConnection::new(
            scope(),
            reference(&format!("account-{index}")),
            Arc::new(AzureCredential::ApiKey("fixture-key".into())),
            options,
        )
        .unwrap();
        let mut request = request(&connection, release);
        request.route.model_version = id(if index == 0 {
            "2026-09-03"
        } else {
            "2026-07-09"
        });
        bindings.push((AzureOpenAiModel::new(connection), request));
    }
    for ((model, request), release) in bindings.iter().zip(releases) {
        let result = collect_model_response(request, model.generate(request, &context(request)))
            .await
            .unwrap();
        assert_eq!(result.text, release);
        assert_eq!(result.metadata.reported_model_id, Some(id(release)));
        assert!(result.metadata.reported_model_version.is_none());
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (index, request) in requests.iter().enumerate() {
        assert_eq!(request.body["model"], format!("deployment-{index}"));
        assert_eq!(request.path, "/openai/v1/responses");
    }
}

fn compiled_tool(schema: Value) -> CompiledTool {
    let parameters = schema["properties"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    SchemaCompiler::new()
        .compile(
            ToolDescriptor {
                tool: reference("lookup"),
                name: id("lookup"),
                description: "Read data".into(),
                input_schema: schema,
                agent_parameters: parameters,
                system_bindings: None,
                output_schema: json!({"type":"string"}),
                side_effect: ToolSideEffect::ReadOnly,
                concurrency: ToolConcurrency::Serial,
                retry: ToolRetryPolicy::Never,
                reconcile: false,
                max_output_bytes: 4096.try_into().unwrap(),
            },
            &SystemInputRegistry::new(vec![]).unwrap(),
        )
        .unwrap()
}

#[tokio::test]
async fn azure_projection_respects_its_native_limits_and_preserves_validation_and_release_identity()
{
    let server = Server::new(vec![Reply::sse(&events("base-model", "done"))]).await;
    let connection = connection(&server);
    let model = AzureOpenAiModel::new(connection.clone());
    let mut request = request(&connection, "base-model");
    let original = compiled_tool(
        json!({"type":"object","properties":{"query":{"type":"string","pattern":"^[a-z]+$","minLength":2}},"required":["query"],"additionalProperties":false}),
    );
    let target = ProviderToolTarget::for_route(&request.route);
    let contract = CompiledToolContract::compile(
        &original,
        target.clone(),
        model.tool_schema_compiler().as_ref(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        contract.wire_tool().model_input_schema["properties"]["query"],
        json!({"type":"string"})
    );
    let decoded = contract
        .decode_arguments(r#"{"query":"INVALID"}"#, Default::default())
        .unwrap();
    assert!(original.validate_model_inputs(&decoded).is_err());
    assert!(
        contract
            .enforcement()
            .iter()
            .any(
                |entry| entry.canonical_pointer == "/properties/query/pattern"
                    && entry.core
                    && entry.context_text
                    && !entry.provider_native
            )
    );
    let saved = serde_json::to_string(&contract).unwrap();
    CompiledToolContract::restore(
        &saved,
        &original,
        &target,
        contract.digest(),
        Default::default(),
    )
    .unwrap();
    request.route.model_version = id("new-release");
    assert!(
        CompiledToolContract::restore(
            &saved,
            &original,
            &ProviderToolTarget::for_route(&request.route),
            contract.digest(),
            Default::default()
        )
        .is_err()
    );
    request.route.model_version = id("release");
    request.tools = vec![contract.wire_tool().clone()];
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let calls = server.requests.lock().unwrap();
    assert_eq!(calls[0].body["tools"][0]["strict"], true);
    assert_eq!(
        calls[0].body["tools"][0]["parameters"],
        contract.wire_tool().model_input_schema
    );
    assert_eq!(calls[0].body["model"], "finance-deployment");
}

#[tokio::test]
async fn azure_compacts_wide_and_deep_values_without_narrowing_the_canonical_shape() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let model = AzureOpenAiModel::new(connection.clone());
    let target = ProviderToolTarget::for_route(&request(&connection, "base-model").route);
    let properties: serde_json::Map<String, Value> = (0..100)
        .map(|i| (format!("field{i}"), json!({"type":"string"})))
        .collect();
    let required: Vec<_> = properties.keys().cloned().collect();
    let wide = json!({"type":"object","properties":properties,"required":required,"additionalProperties":false});
    let wide_value: serde_json::Map<String, Value> = (0..100)
        .map(|i| (format!("field{i}"), json!("value")))
        .collect();
    let mut deep =
        json!({"type":"object","properties":{},"required":[],"additionalProperties":false});
    let mut deep_value = json!({});
    for _ in 0..5 {
        deep = json!({"type":"object","properties":{"child":deep},"required":["child"],"additionalProperties":false});
        deep_value = json!({"child":deep_value});
    }
    for (schema, value) in [(wide, Value::Object(wide_value)), (deep, deep_value)] {
        let original = compiled_tool(
            json!({"type":"object","properties":{"query":schema},"required":["query"],"additionalProperties":false}),
        );
        let azure = CompiledToolContract::compile(
            &original,
            target.clone(),
            model.tool_schema_compiler().as_ref(),
            Default::default(),
        )
        .unwrap();
        let openai = CompiledToolContract::compile(
            &original,
            target.clone(),
            &wickle_model_responses::ResponsesToolSchemaCompiler,
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            openai.wire_tool().model_input_schema,
            *original.model_input_schema()
        );
        assert_eq!(
            azure.wire_tool().model_input_schema["properties"]["query"],
            json!({"type":"string"})
        );
        assert_ne!(azure.compiler(), openai.compiler());
        let canonical = JsonObject::from([("query".into(), value)]);
        let encoded = azure.encode_arguments(&canonical).unwrap();
        let decoded = azure
            .decode_arguments(
                &serde_json::to_string(&encoded).unwrap(),
                Default::default(),
            )
            .unwrap();
        assert_eq!(decoded, canonical);
        original.validate_model_inputs(&decoded).unwrap();
        let invalid = azure
            .decode_arguments(r#"{"query":"42"}"#, Default::default())
            .unwrap();
        assert!(original.validate_model_inputs(&invalid).is_err());
    }
}

#[tokio::test]
async fn top_level_property_limit_uses_one_json_object_without_exposing_system_fields() {
    let server = Server::new(vec![
        Reply::sse(&events("base-model", "done")),
        Reply::sse(&events("base-model", "done")),
    ])
    .await;
    let connection = connection(&server);
    let model = AzureOpenAiModel::new(connection.clone());
    for count in [100, 101] {
        let properties: serde_json::Map<String, Value> = (0..count)
            .map(|i| (format!("field{i}"), json!({"type":["string","null"]})))
            .collect();
        let mut required: Vec<_> = properties.keys().cloned().collect();
        // Omission/null preservation is checked on the packed case.
        if count == 101 {
            required.retain(|name| name != "field0");
        }
        let original = compiled_tool(
            json!({"type":"object","properties":properties,"required":required,"additionalProperties":false}),
        );
        let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
            key: id("workspace_id"),
            version: id("1"),
            value_schema: json!({"type":"string","format":"uuid"}),
            source: SystemInputSource::Run {},
        }])
        .unwrap();
        let mut descriptor = original.descriptor().clone();
        descriptor.input_schema["properties"]["workspace_id"] =
            json!({"type":"string","format":"uuid","description":"hidden-system-schema"});
        descriptor.input_schema["required"]
            .as_array_mut()
            .unwrap()
            .push(json!("workspace_id"));
        let original = SchemaCompiler::new()
            .compile(descriptor, &registry)
            .unwrap();
        let mut request = request(&connection, "base-model");
        let target = ProviderToolTarget::for_route(&request.route);
        let contract = CompiledToolContract::compile(
            &original,
            target.clone(),
            model.tool_schema_compiler().as_ref(),
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            contract.wire_tool().model_input_schema["properties"]
                .as_object()
                .unwrap()
                .len(),
            if count == 100 { 100 } else { 1 }
        );
        let canonical: JsonObject = (0..count)
            .filter(|i| count == 100 || *i != 0)
            .map(|i| {
                (
                    format!("field{i}"),
                    if i == 1 { Value::Null } else { json!("value") },
                )
            })
            .collect();
        let encoded = contract.encode_arguments(&canonical).unwrap();
        let decoded = contract
            .decode_arguments(
                &serde_json::to_string(&encoded).unwrap(),
                Default::default(),
            )
            .unwrap();
        assert_eq!(decoded, canonical);
        original.validate_model_inputs(&decoded).unwrap();
        let saved = serde_json::to_string(&contract).unwrap();
        let restored = CompiledToolContract::restore(
            &saved,
            &original,
            &target,
            contract.digest(),
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            restored
                .decode_arguments(
                    &serde_json::to_string(&encoded).unwrap(),
                    Default::default()
                )
                .unwrap(),
            canonical
        );
        for fragment in contract.constraint_fragments() {
            assert!(!fragment.text.contains("hidden-system-schema"));
        }
        if count == 101 {
            for invalid in [
                json!({"arguments":"{"}),
                json!({"arguments":"[]"}),
                json!({"arguments":{}}),
                json!({"arguments":"{}","extra":1}),
                json!({}),
                json!({"arguments":"{\"field1\":1,\"field1\":2}"}),
                json!({"arguments":"{\"field1\":0.1234567890123456789012345}"}),
            ] {
                assert!(
                    contract
                        .decode_arguments(&invalid.to_string(), Default::default())
                        .is_err()
                );
            }
            let invalid = contract
                .decode_arguments(r#"{"arguments":"{}"}"#, Default::default())
                .unwrap();
            assert!(original.validate_model_inputs(&invalid).is_err());
            let hidden = contract
                .decode_arguments(
                    r#"{"arguments":"{\"workspace_id\":\"invented\"}"}"#,
                    Default::default(),
                )
                .unwrap();
            assert!(original.validate_model_inputs(&hidden).is_err());
        }
        request.tools = vec![contract.wire_tool().clone()];
        collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
    }
    for request in server.requests.lock().unwrap().iter() {
        assert_eq!(request.body["tools"][0]["strict"], true);
    }
}

#[tokio::test]
async fn empty_object_depth_boundaries_preserve_native_schema_with_bounded_compiler() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let target = ProviderToolTarget::for_route(&request(&connection, "base-model").route);
    for levels in [5, 6, 11] {
        let mut schema =
            json!({"type":"object","properties":{},"required":[],"additionalProperties":false});
        for _ in 1..levels {
            schema = json!({"type":"object","properties":{"child":schema},"required":["child"],"additionalProperties":false});
        }
        let original = compiled_tool(schema);
        let openai = CompiledToolContract::compile(
            &original,
            target.clone(),
            &wickle_model_responses::ResponsesToolSchemaCompiler,
            Default::default(),
        )
        .unwrap();
        assert_eq!(openai.compiler().version, id("2"));
        assert_eq!(
            openai.wire_tool().model_input_schema,
            *original.model_input_schema()
        );
        let azure = CompiledToolContract::compile(
            &original,
            target.clone(),
            &wickle_model_responses::AzureResponsesToolSchemaCompiler,
            Default::default(),
        )
        .unwrap();
        if levels == 5 {
            assert_eq!(
                azure.wire_tool().model_input_schema,
                *original.model_input_schema()
            );
        } else {
            assert_eq!(
                azure.wire_tool().model_input_schema["properties"]["child"],
                json!({"type":"string"})
            );
        }
    }
}
```

## `crates/wickle-model-openai/tests/responses.rs`

```rust
//! Real HTTP/SSE boundaries, scoped credentials, version evidence, and lossless replay.
#[allow(dead_code)]
mod support;
use futures_util::StreamExt;
use serde_json::{Value, json};
use support::*;
use wickle::*;
use wickle_model_openai::*;

#[tokio::test]
async fn streaming_versions_options_and_reported_usage_do_not_leak_host_context() {
    let server = Server::new(vec![
        Reply::sse(&events("model-first", "안녕")),
        Reply::sse(&events("model-second", "second")),
    ])
    .await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    for (name, effort, text) in [
        ("model-first", "low", "안녕"),
        ("model-second", "high", "second"),
    ] {
        let mut request = request(&connection, name);
        request.route.model_version = id(name);
        request
            .options
            .insert("reasoning_effort".into(), json!(effort));
        let context = context(&request);
        let result = collect_model_response(&request, model.generate(&request, &context))
            .await
            .unwrap();
        assert_eq!(result.text, text);
        assert_eq!(result.finish, ModelFinish::Stop);
        assert_eq!(result.metadata.reported_model_id, Some(id(name)));
        assert!(result.metadata.reported_model_version.is_none());
        assert_eq!(
            result.metadata.usage,
            Some(ModelUsage {
                measurement: UsageMeasurement::Reported,
                input_tokens: Some(11),
                output_tokens: Some(7)
            })
        );
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (index, effort) in ["low", "high"].into_iter().enumerate() {
        assert_eq!(requests[index].path, "/v1/responses");
        assert_eq!(requests[index].method, "POST");
        assert_eq!(requests[index].body["reasoning"]["effort"], effort);
        assert_eq!(requests[index].body["stream"], true);
        assert_eq!(requests[index].body["store"], false);
        assert_eq!(requests[index].body["truncation"], "disabled");
        let encoded = requests[index].body.to_string();
        assert!(!encoded.contains("hidden-workspace"));
        assert!(!encoded.contains("fixture-key-not-a-secret"));
        assert!(
            requests[index]
                .headers
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-key-not-a-secret")
        );
    }
    assert!(!format!("{connection:?} {model:?}").contains("fixture-key-not-a-secret"));
}

#[tokio::test]
async fn structured_output_preserves_schema_and_rejects_unsupported_contracts_before_http() {
    let schema = json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false});
    let server = Server::new(vec![Reply::sse(&events("model", r#"{"answer":42}"#))]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    request.output = ModelOutput::JsonSchema {
        schema: schema.clone(),
    };
    request.options.insert("verbosity".into(), json!("low"));
    let result = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(parse_json(&result.text).unwrap(), json!({"answer":42}));
    let body = server.requests.lock().unwrap()[0].body.clone();
    assert_eq!(body["text"]["format"]["schema"], schema);
    assert_eq!(body["text"]["format"]["strict"], true);
    assert_eq!(body["text"]["verbosity"], "low");
    request.output = ModelOutput::JsonSchema {
        schema: json!({"type":"object","properties":{"optional":{"type":"string"}},"additionalProperties":false}),
    };
    let failure = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap_err();
    assert_eq!(failure.kind, ModelFailureKind::Unsupported);
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

fn function_events() -> Vec<Value> {
    let reasoning = json!({"id":"rs_1","type":"reasoning","summary":[],"encrypted_content":"ciphertext-fixture"});
    let call = json!({"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"query\":\"figures\"}","status":"completed"});
    vec![
        json!({"type":"response.created","response":{"id":"resp_1","model":"model","status":"in_progress"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"rs_1","type":"reasoning","summary":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":reasoning}),
        json!({"type":"response.output_item.added","output_index":1,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":"fc_1","delta":"{\"query\":"}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":"fc_1","delta":"\"figures\"}"}),
        json!({"type":"response.function_call_arguments.done","output_index":1,"item_id":"fc_1","arguments":"{\"query\":\"figures\"}"}),
        json!({"type":"response.output_item.done","output_index":1,"item":call}),
        json!({"type":"response.completed","response":{"id":"resp_1","model":"model","status":"completed","output":[reasoning,call]}}),
    ]
}
fn with_tool(request: &mut ModelRequest) {
    request.tools = vec![ModelTool {
        name: id("lookup"),
        description: "Read figures".into(),
        model_input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer"}},"required":["query"],"additionalProperties":false}),
    }];
}
#[tokio::test]
async fn function_fragments_and_full_reasoning_replay_preserve_original_order_once() {
    let server = Server::new(vec![
        Reply::sse(&function_events()),
        Reply::sse(&events("model", "received result")),
    ])
    .await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    with_tool(&mut request);
    let first = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(first.finish, ModelFinish::ToolCalls);
    assert_eq!(first.tool_calls.len(), 1);
    assert!(first.metadata.usage.is_none());
    assert_eq!(first.continuation.len(), 1);
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![
            ModelContent::ToolCall {
                provider_call_id: id("call_1"),
                name: id("lookup"),
                arguments: JsonObject::from([("query".into(), json!("figures"))]),
            },
            ModelContent::Opaque {
                continuation: first.continuation[0].clone(),
            },
        ],
    });
    request.messages.push(ModelMessage {
        role: ModelRole::Tool,
        content: vec![ModelContent::ToolResult {
            provider_call_id: id("call_1"),
            content: json!({"result":73}),
        }],
    });
    request.request_id = id("second-attempt");
    let result = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(result.text, "received result");
    {
        let calls = server.requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        let input = calls[1].body["input"].as_array().unwrap();
        assert_eq!(
            input
                .iter()
                .filter(|item| item["type"] == "function_call")
                .count(),
            1
        );
        assert_eq!(input[1]["type"], "reasoning");
        assert_eq!(input[1]["encrypted_content"], "ciphertext-fixture");
        assert_eq!(input[2]["call_id"], "call_1");
        assert_eq!(input[3]["type"], "function_call_output");
        assert_eq!(
            parse_json(input[3]["output"].as_str().unwrap()).unwrap(),
            json!({"result":73})
        );
        assert_eq!(calls[0].body["tools"][0]["strict"], false);
        assert_eq!(
            calls[0].body["tools"][0]["parameters"]["required"],
            json!(["query"])
        );
    }
    if let ModelContent::ToolCall { arguments, .. } = &mut request.messages[1].content[0] {
        arguments.insert("query".into(), json!("changed"));
    }
    assert!(
        collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .is_err()
    );
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn truncated_conflicting_and_length_limited_streams_never_return_executable_calls() {
    for mode in ["truncated", "conflict", "length", "duplicate-terminal"] {
        let mut data = function_events();
        match mode {
            "truncated" => {
                data.truncate(5);
            }
            "conflict" => {
                data[5]["item_id"] = json!("another-item");
            }
            "length" => {
                data.truncate(5);
                data.push(json!({"type":"response.incomplete","response":{"id":"resp_1","model":"model","status":"incomplete","output":[],"incomplete_details":{"reason":"max_output_tokens"}}}));
            }
            _ => {
                data.push(data.last().unwrap().clone());
            }
        }
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let mut request = request(&connection, "model");
        with_tool(&mut request);
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        assert!(result.is_err(), "{mode} must not yield a complete call");
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn unknown_options_wrong_scope_and_wrong_target_never_reach_the_server() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    for field in [
        "store",
        "input",
        "tools",
        "background",
        "previous_response_id",
    ] {
        let mut request = request(&connection, "model");
        request.options.insert(field.into(), json!(true));
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, ModelFailureKind::Unsupported);
    }
    let mut request = request(&connection, "model");
    let mut caller = context(&request);
    caller.scope.workspace_id = id("other");
    assert_eq!(
        model
            .generate(&request, &caller)
            .next()
            .await
            .unwrap()
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    caller = context(&request);
    request
        .route
        .target
        .insert("project_id".into(), json!("different"));
    assert!(
        collect_model_response(&request, model.generate(&request, &caller))
            .await
            .is_err()
    );
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn provider_errors_and_redirects_are_not_retried_or_exposed_as_text() {
    for (status, expected) in [
        (401, ModelFailureKind::Authentication),
        (404, ModelFailureKind::Unavailable),
        (429, ModelFailureKind::RateLimited),
        (503, ModelFailureKind::Transport),
        (302, ModelFailureKind::Unsupported),
    ] {
        let redirect = Server::new(vec![Reply::sse(&events("model", "unexpected redirect"))]).await;
        let mut reply = Reply::json(
            status,
            json!({"error":{"message":"private diagnostic","code":"fixture"}}),
        );
        if status == 302 {
            reply
                .headers
                .push(("location", format!("{}responses", redirect.base)));
        }
        let server = Server::new(vec![
            reply,
            Reply::sse(&events("model", "unexpected retry")),
        ])
        .await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let request = request(&connection, "model");
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, expected);
        assert!(failure.partial_text().is_empty());
        assert_eq!(server.requests.lock().unwrap().len(), 1);
        assert!(redirect.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn cancellation_and_deadline_drop_an_unfinished_stream() {
    for cancel in [true, false] {
        let mut reply = Reply::sse(&events("model", "partial")[..3]);
        reply.stall = true;
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let request = request(&connection, "model");
        let mut caller = context(&request);
        if !cancel {
            caller.deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(100);
        }
        let mut stream = model.generate(&request, &caller);
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ModelEvent::TextDelta { .. }
        ));
        if cancel {
            caller.cancellation.cancel();
        }
        let next = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap();
        if cancel {
            assert_eq!(next.unwrap_err().code, ErrorCode::Cancelled);
        } else {
            assert!(matches!(
                next.unwrap(),
                ModelEvent::ResponseError {
                    kind: ModelFailureKind::Timeout,
                    ..
                }
            ));
        }
        assert!(stream.next().await.is_none());
        tokio::time::timeout(std::time::Duration::from_secs(1), server.closed.notified())
            .await
            .unwrap();
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn model_inspection_uses_registered_snapshot_facts_not_requested_versions_or_name_patterns() {
    let server = Server::new(vec![
        Reply::json(200, json!({"id":"model-2026-01-01","object":"model"})),
        Reply::json(200, json!({"id":"model-2026-01-01","object":"model"})),
    ])
    .await;
    let connection = connection(&server);
    let request = request(&connection, "model-2026-01-01");
    let context = ModelInspectionContext {
        scope: scope(),
        principal_ref: id("user"),
        capability_grant_ref: id("grant"),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(3),
    };
    let unknown = OpenAiInspector::new(connection.clone(), vec![])
        .unwrap()
        .inspect(&request.route, &context)
        .await
        .unwrap();
    assert!(unknown.model_version.is_none());
    assert_eq!(unknown.version_semantics, VersionSemantics::Unverified);
    assert!(
        unknown
            .validate(&request.route, VersionPolicy::RequirePinned)
            .is_err()
    );
    let known = OpenAiInspector::new(
        connection,
        vec![OpenAiSnapshot {
            model_id: id("model-2026-01-01"),
            model_version: id("actual-release"),
            evidence_ref: id("documented-snapshot"),
        }],
    )
    .unwrap()
    .inspect(&request.route, &context)
    .await
    .unwrap();
    assert_eq!(known.model_version, Some(id("actual-release")));
    assert_eq!(known.version_semantics, VersionSemantics::Pinned);
    assert_eq!(
        known
            .validate(&request.route, VersionPolicy::RequirePinned)
            .unwrap_err()
            .code,
        ErrorCode::ModelVersionDrift
    );
    let calls = server.requests.lock().unwrap();
    assert!(
        calls
            .iter()
            .all(|call| call.method == "GET" && call.path == "/v1/models/model-2026-01-01")
    );
}

#[tokio::test]
async fn refusal_cannot_be_relabelled_as_success_by_a_conflicting_terminal() {
    for valid in [false, true] {
        let mut data = events("model", "Unable to help");
        data[2]["type"] = json!("response.refusal.delta");
        data[3]["type"] = json!("response.refusal.done");
        data[3].as_object_mut().unwrap().remove("text");
        data[3]["refusal"] = json!("Unable to help");
        if valid {
            let part = json!({"type":"refusal","refusal":"Unable to help"});
            data[4]["item"]["content"] = json!([part]);
            data[5]["response"]["output"][0]["content"] = json!([part]);
        }
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let request = request(&connection, "model");
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        if valid {
            assert_eq!(result.unwrap().finish, ModelFinish::Refusal);
        } else {
            assert!(result.is_err());
        }
    }
}

#[tokio::test]
async fn interleaved_function_deltas_keep_independent_identity_and_arguments() {
    let mut data = vec![
        json!({"type":"response.created","response":{"id":"resp_1","model":"model","status":"in_progress"}}),
    ];
    let calls:Vec<_>=["one","two"].into_iter().enumerate().map(|(index,query)|json!({"id":format!("fc_{index}"),"type":"function_call","call_id":format!("call_{index}"),"name":"lookup","arguments":format!("{{\"query\":\"{query}\"}}"),"status":"completed"})).collect();
    for (index, call) in calls.iter().enumerate() {
        let mut item = call.clone();
        item["arguments"] = json!("");
        item.as_object_mut().unwrap().remove("status");
        data.push(json!({"type":"response.output_item.added","output_index":index,"item":item}));
    }
    for (index, delta) in [
        (0, "{\"query\":"),
        (1, "{\"query\":"),
        (1, "\"two\"}"),
        (0, "\"one\"}"),
    ] {
        data.push(json!({"type":"response.function_call_arguments.delta","output_index":index,"item_id":format!("fc_{index}"),"delta":delta}));
    }
    for (index, call) in calls.iter().enumerate() {
        data.push(json!({"type":"response.function_call_arguments.done","output_index":index,"item_id":format!("fc_{index}"),"arguments":call["arguments"]}));
        data.push(json!({"type":"response.output_item.done","output_index":index,"item":call}));
    }
    data.push(json!({"type":"response.completed","response":{"id":"resp_1","model":"model","status":"completed","output":calls}}));
    let server = Server::new(vec![Reply::sse(&data)]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    with_tool(&mut request);
    let result = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(result.tool_calls.len(), 2);
    assert_eq!(result.tool_calls[0].model_inputs["query"], "one");
    assert_eq!(result.tool_calls[1].model_inputs["query"], "two");
    assert_ne!(
        result.tool_calls[0].provider_call_id,
        result.tool_calls[1].provider_call_id
    );
}

#[tokio::test]
async fn stream_and_payload_limits_reject_oversized_or_reassigned_data_without_a_success() {
    for case in ["raw", "payload", "events", "identity", "sequence", "usage"] {
        let mut data = events("model", "candidate");
        if case == "identity" {
            data[5]["response"]["id"] = json!("other-response");
        }
        if case == "usage" {
            data[5]["response"]["usage"]["output_tokens"] = json!("7");
        }
        let mut reply = Reply::sse(&data);
        if case == "sequence" {
            let text = String::from_utf8(reply.body).unwrap();
            reply.body = text
                .replacen("\"sequence_number\":2", "\"sequence_number\":1", 1)
                .into_bytes();
        }
        let server = Server::new(vec![reply]).await;
        let connection = if case == "raw" {
            OpenAiConnection::new(
                scope(),
                reference("account"),
                "fixture-key-not-a-secret",
                OpenAiOptions {
                    base_url: server.base.clone(),
                    max_transport_bytes: 64,
                    max_event_bytes: 64,
                    ..Default::default()
                },
            )
            .unwrap()
        } else {
            connection(&server)
        };
        let model = OpenAiModel::new(connection.clone());
        let mut request = request(&connection, "model");
        if case == "payload" {
            request.limits.max_response_bytes = 4;
        }
        if case == "events" {
            request.limits.max_events = 1;
        }
        assert!(
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .is_err(),
            "{case}"
        );
    }
}

#[tokio::test]
async fn protocol_metadata_does_not_consume_the_normalized_model_event_allowance() {
    let server = Server::new(vec![Reply::sse(&events("model", "one delta"))]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    request.limits.max_events = 2;
    let result = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(result.text, "one delta");
}

#[tokio::test]
async fn final_content_cannot_move_between_messages_or_parts() {
    for mode in ["valid", "message", "part", "kind"] {
        let mut data = events("model", "A");
        data.truncate(4);
        let mut first = text_item("A");
        first["phase"] = json!("commentary");
        let mut second = text_item("B");
        second["id"] = json!("msg_2");
        second["phase"] = json!("final_answer");
        data.push(json!({"type":"response.output_item.added","output_index":1,"item":{"id":"msg_2","type":"message","role":"assistant","content":[]}}));
        data.push(json!({"type":"response.output_text.delta","output_index":1,"item_id":"msg_2","content_index":0,"delta":"B"}));
        match mode {
            "message" => {
                first["content"][0]["text"] = json!("AB");
                second["content"] = json!([]);
            }
            "part" => {
                first["content"] =
                    json!([{"type":"output_text","text":""},{"type":"output_text","text":"A"}]);
            }
            "kind" => {
                first["content"] = json!([{"type":"refusal","refusal":"A"}]);
            }
            _ => {}
        }
        data.push(json!({"type":"response.completed","response":{"id":"resp_1","model":"model","status":"completed","output":[first,second]}}));
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let request = request(&connection, "model");
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        if mode == "valid" {
            assert_eq!(result.unwrap().text, "AB");
        } else {
            assert!(result.is_err(), "accepted {mode} reassignment");
        }
    }
}

#[tokio::test]
async fn two_documented_release_ids_coexist_without_replacing_connection_state() {
    let releases = ["gpt-6-astra", "gpt-5.6-sol"];
    let server = Server::new(
        releases
            .iter()
            .map(|release| Reply::sse(&events(release, release)))
            .collect(),
    )
    .await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    for (index, release) in releases.iter().enumerate() {
        let mut request = request(&connection, release);
        request.route.binding = reference(&format!("binding-{index}"));
        request.route.model_version = id(&format!("fixture-revision-{index}"));
        let result = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(result.text, *release);
        assert_eq!(result.metadata.reported_model_id, Some(id(release)));
        assert!(result.metadata.reported_model_version.is_none());
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (request, release) in requests.iter().zip(releases) {
        assert_eq!(request.body["model"], release);
    }
}

fn compiled_input(schema: Value) -> CompiledTool {
    let properties = schema["properties"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    SchemaCompiler::new()
        .compile(
            ToolDescriptor {
                tool: reference("lookup"),
                name: id("lookup"),
                description: "Read data".into(),
                input_schema: schema,
                agent_parameters: properties,
                system_bindings: None,
                output_schema: json!({"type":"string"}),
                side_effect: ToolSideEffect::ReadOnly,
                concurrency: ToolConcurrency::Serial,
                retry: ToolRetryPolicy::Never,
                reconcile: false,
                max_output_bytes: 4096.try_into().unwrap(),
            },
            &SystemInputRegistry::new(vec![]).unwrap(),
        )
        .unwrap()
}
#[tokio::test]
async fn compiled_optional_and_nested_inputs_use_strict_wire_and_restore_the_original_contract() {
    let server = Server::new(vec![Reply::sse(&events("model", "done"))]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    let original = compiled_input(json!({"type":"object","properties":{
        "query":{"type":"string","minLength":2,"pattern":"^[a-z]+$"},
        "note":{"type":["string","null"]},
        "filter":{"type":"object","properties":{"category":{"type":"string"},"term":{"type":"string"}},"required":["category"],"additionalProperties":false}
    },"required":["query"],"additionalProperties":false,"if":{"properties":{"query":{"const":"latest"}}},"then":{"required":["note"]}}));
    let contract = CompiledToolContract::compile(
        &original,
        ProviderToolTarget::for_route(&request.route),
        model.tool_schema_compiler().as_ref(),
        ProviderToolSchemaLimits::default(),
    )
    .unwrap();
    request.tools = vec![contract.wire_tool().clone()];
    for fragment in contract.constraint_fragments() {
        request.messages[0].content.push(ModelContent::Text {
            text: fragment.text.clone(),
        });
    }
    let canonical = JsonObject::from([
        ("query".into(), json!("latest")),
        ("note".into(), Value::Null),
        ("filter".into(), json!({"category":"finance"})),
    ]);
    let wire = contract.encode_arguments(&canonical).unwrap();
    assert_eq!(wire["note"], json!({"present":true,"value":null}));
    assert_eq!(
        parse_json(wire["filter"].as_str().unwrap()).unwrap(),
        json!([{"category":"finance"}])
    );
    assert_eq!(
        contract
            .decode_arguments(
                &serde_json::to_string(&wire).unwrap(),
                ProviderToolSchemaLimits::default()
            )
            .unwrap(),
        canonical
    );
    let omitted = JsonObject::from([("query".into(), json!("other"))]);
    let encoded = contract.encode_arguments(&omitted).unwrap();
    assert_eq!(
        contract
            .decode_arguments(
                &serde_json::to_string(&encoded).unwrap(),
                ProviderToolSchemaLimits::default()
            )
            .unwrap(),
        omitted
    );
    assert!(
        contract
            .enforcement()
            .iter()
            .any(|constraint| constraint.canonical_pointer == "/if"
                && constraint.context_text
                && !constraint.provider_native)
    );
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let body = &server.requests.lock().unwrap()[0].body;
    assert_eq!(body["tools"][0]["strict"], true);
    assert_eq!(
        body["tools"][0]["parameters"],
        contract.wire_tool().model_input_schema
    );
    assert_eq!(
        body["tools"][0]["parameters"]["required"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        body["tools"][0]["parameters"]["properties"]["query"]["pattern"],
        "^[a-z]+$"
    );
    assert!(
        body["tools"][0]["parameters"]["properties"]["query"]
            .get("minLength")
            .is_none()
    );
}

#[tokio::test]
async fn model_qualified_compilation_respects_fine_tuned_native_constraint_limits() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let original = compiled_input(
        json!({"type":"object","properties":{"query":{"type":"string","pattern":"^[a-z]+$"}},"required":["query"],"additionalProperties":false}),
    );
    let mut route = request(&connection, "gpt-6-astra").route;
    let normal = CompiledToolContract::compile(
        &original,
        ProviderToolTarget::for_route(&route),
        model.tool_schema_compiler().as_ref(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        normal.wire_tool().model_input_schema,
        *original.model_input_schema()
    );
    route.model_id = id("ft:base:fixture:release");
    let tuned = CompiledToolContract::compile(
        &original,
        ProviderToolTarget::for_route(&route),
        model.tool_schema_compiler().as_ref(),
        Default::default(),
    )
    .unwrap();
    assert!(
        tuned.wire_tool().model_input_schema["properties"]["query"]
            .get("pattern")
            .is_none()
    );
    assert!(
        tuned
            .enforcement()
            .iter()
            .any(
                |constraint| constraint.canonical_pointer == "/properties/query/pattern"
                    && constraint.core
                    && constraint.context_text
            )
    );
    assert_ne!(normal.digest(), tuned.digest());
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn oversized_native_enum_uses_reversible_text_without_losing_original_validation() {
    let server = Server::new(vec![Reply::sse(&events("model", "done"))]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    let values: Vec<_> = (0..1001).map(|index| format!("choice-{index}")).collect();
    let original = compiled_input(
        json!({"type":"object","properties":{"query":{"type":"string","enum":values}},"required":["query"],"additionalProperties":false}),
    );
    let contract = CompiledToolContract::compile(
        &original,
        ProviderToolTarget::for_route(&request.route),
        model.tool_schema_compiler().as_ref(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        contract.wire_tool().model_input_schema["properties"]["query"],
        json!({"type":"string"})
    );
    for (value, valid) in [("choice-1000", true), ("invented-choice", false)] {
        let canonical = JsonObject::from([("query".into(), json!(value))]);
        let encoded = contract.encode_arguments(&canonical).unwrap();
        let restored = contract
            .decode_arguments(
                &serde_json::to_string(&encoded).unwrap(),
                Default::default(),
            )
            .unwrap();
        assert_eq!(restored, canonical);
        assert_eq!(original.validate_model_inputs(&restored).is_ok(), valid);
    }
    request.tools = vec![contract.wire_tool().clone()];
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(
        server.requests.lock().unwrap()[0].body["tools"][0]["strict"],
        true
    );
}

#[tokio::test]
async fn shared_reference_work_budget_falls_back_without_losing_canonical_values() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let request = request(&connection, "model");
    let mut defs = serde_json::Map::new();
    defs.insert("level3".into(),json!({"type":"string","minLength":1,"description":"A leaf selected from a shared reference graph"}));
    let mut value = json!("selected");
    for level in (0..3).rev() {
        let properties: serde_json::Map<String, Value> = (0..5)
            .map(|index| {
                (
                    format!("branch{index}"),
                    json!({"$ref":format!("#/$defs/level{}",level+1)}),
                )
            })
            .collect();
        let required: Vec<_> = properties.keys().cloned().collect();
        defs.insert(format!("level{level}"),json!({"type":"object","properties":properties,"required":required,"additionalProperties":false}));
        value = Value::Object(
            (0..5)
                .map(|index| (format!("branch{index}"), value.clone()))
                .collect(),
        );
    }
    let canonical = compiled_input(
        json!({"type":"object","properties":{"a":{"$ref":"#/$defs/level0"},"b":{"$ref":"#/$defs/level0"}},"required":["a","b"],"additionalProperties":false,"if":{"required":["a"]},"then":{"required":["b"]},"$defs":defs}),
    );
    let contract = CompiledToolContract::compile(
        &canonical,
        ProviderToolTarget::for_route(&request.route),
        &wickle_model_responses::ResponsesToolSchemaCompiler,
        Default::default(),
    )
    .unwrap();
    // Each field alone fits native limits. Expansion work across both fields must
    // share one budget rather than reset at each reference or top-level property.
    assert_eq!(
        contract.wire_tool().model_input_schema["properties"]["a"]["type"],
        "object"
    );
    assert_eq!(
        contract.wire_tool().model_input_schema["properties"]["b"]["type"],
        "string"
    );
    let values = JsonObject::from([("a".into(), value.clone()), ("b".into(), value)]);
    canonical.validate_model_inputs(&values).unwrap();
    let encoded = contract.encode_arguments(&values).unwrap();
    assert!(encoded["b"].is_string());
    assert_eq!(
        contract
            .decode_arguments(
                &serde_json::to_string(&encoded).unwrap(),
                Default::default()
            )
            .unwrap(),
        values
    );
    let mut invalid = values;
    invalid.get_mut("b").unwrap()["branch0"]["branch0"]["branch0"] = json!(7);
    let encoded = contract.encode_arguments(&invalid).unwrap();
    let decoded = contract
        .decode_arguments(
            &serde_json::to_string(&encoded).unwrap(),
            Default::default(),
        )
        .unwrap();
    assert!(canonical.validate_model_inputs(&decoded).is_err());
}
```

## `crates/wickle-model-responses/src/schema.rs`

```rust
//! Versioned strict projection. Original constraints are preserved by the core.
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;
use wickle::*;

/// Responses-compatible Tool schemas with reversible omission/value handling.
#[derive(Debug, Clone, Copy, Default)]
pub struct ResponsesToolSchemaCompiler;
impl ProviderToolSchemaCompiler for ResponsesToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new("wickle-responses-tool-schema").expect("constant"),
            version: Id::new("2").expect("constant"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        let restricted = target
            .model
            .as_ref()
            .is_none_or(|model| model.id.as_str().starts_with("ft:"));
        compile(tool, target, SchemaPolicy::openai(restricted))
    }
}

/// Azure Responses projection using its documented schema subset and limits.
#[derive(Debug, Clone, Copy, Default)]
pub struct AzureResponsesToolSchemaCompiler;
impl ProviderToolSchemaCompiler for AzureResponsesToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new("wickle-azure-responses-tool-schema").expect("constant"),
            version: Id::new("2").expect("constant"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        if target.api_contract.operation.as_str() != "responses" {
            return Err(invalid("operation"));
        }
        let policy = SchemaPolicy::azure();
        let properties = tool
            .model_input_schema
            .get("properties")
            .and_then(Value::as_object);
        if properties.is_some_and(|properties| properties.len() > policy.max_properties) {
            let mut wire_tool = tool.clone();
            wire_tool.model_input_schema = object_schema(Map::from_iter([(
                "arguments".into(),
                json!({"type":"string"}),
            )]));
            return Ok(ProviderToolProjection {
                wire_tool,
                decode_plan: ArgumentDecodePlan::JsonObjectText {
                    wire_name: "arguments".into(),
                },
            });
        }
        compile(tool, target, policy)
    }
}
#[derive(Clone, Copy)]
struct SchemaPolicy {
    restricted_constraints: bool,
    max_depth: usize,
    max_properties: usize,
    max_object_depth: usize,
}
impl SchemaPolicy {
    fn openai(restricted_constraints: bool) -> Self {
        Self {
            restricted_constraints,
            max_depth: 10,
            max_properties: 5000,
            max_object_depth: 10,
        }
    }
    fn azure() -> Self {
        Self {
            restricted_constraints: true,
            max_depth: 5,
            max_properties: 100,
            max_object_depth: 4,
        }
    }
}
fn compile(
    tool: &ModelTool,
    target: &ProviderToolTarget,
    policy: SchemaPolicy,
) -> Result<ProviderToolProjection, ContractError> {
    if target.api_contract.operation.as_str() != "responses" {
        return Err(invalid("operation"));
    }
    let fine_tuned = policy.restricted_constraints;
    if supported_with(&tool.model_input_schema, policy) {
        return Ok(ProviderToolProjection {
            wire_tool: tool.clone(),
            decode_plan: ArgumentDecodePlan::Identity {},
        });
    }
    let root = &tool.model_input_schema;
    let properties = root
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("properties"))?;
    let required: BTreeSet<_> = root
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    let mut wire = Map::new();
    let mut fields = vec![];
    let mut expansion = ExpansionBudget {
        nodes: 1024,
        bytes: 32 * 1024,
    };
    for (name, schema) in properties {
        let optional = !required.contains(name.as_str());
        let native = lower(
            schema,
            root,
            fine_tuned,
            1,
            &mut BTreeSet::new(),
            &mut expansion,
        );
        let (schema, encoding) = match native {
            Some(schema) if optional => (
                json!({"type":"object","properties":{"present":{"type":"boolean"},"value":{"anyOf":[schema,{"type":"null"}]}},"required":["present","value"],"additionalProperties":false}),
                ArgumentValueEncoding::Presence {
                    present_key: "present".into(),
                    value_key: "value".into(),
                },
            ),
            Some(schema) => (schema, ArgumentValueEncoding::Identity {}),
            None => (
                json_text_schema(optional),
                ArgumentValueEncoding::JsonText { optional },
            ),
        };
        wire.insert(name.clone(), schema);
        fields.push(ArgumentFieldMapping {
            wire_name: name.clone(),
            canonical_name: name.clone(),
            encoding,
        });
    }
    let mut projected = tool.clone();
    projected.model_input_schema = object_schema(wire);
    // Representation overhead must not exceed either provider limits or
    // the core's original per-schema byte bound. Compact the largest
    // remaining native field, preserving every canonical field and rule.
    while !supported_with(&projected.model_input_schema, policy)
        || serde_json::to_vec(&projected)
            .map_err(|_| invalid("json"))?
            .len()
            > ProviderToolSchemaLimits::default().max_schema_bytes
    {
        let candidate = fields
            .iter()
            .enumerate()
            .filter(|(_, field)| !matches!(field.encoding, ArgumentValueEncoding::JsonText { .. }))
            .max_by_key(|(_, field)| {
                projected.model_input_schema["properties"][&field.wire_name]
                    .to_string()
                    .len()
            })
            .map(|(index, _)| index)
            .ok_or_else(|| invalid("limits"))?;
        let field = &mut fields[candidate];
        let optional = !required.contains(field.canonical_name.as_str());
        projected.model_input_schema["properties"][&field.wire_name] = json_text_schema(optional);
        field.encoding = ArgumentValueEncoding::JsonText { optional };
    }
    Ok(ProviderToolProjection {
        wire_tool: projected,
        decode_plan: ArgumentDecodePlan::Fields { fields },
    })
}

fn object_schema(properties: Map<String, Value>) -> Value {
    let required: Vec<_> = properties.keys().cloned().collect();
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}
fn json_text_schema(_optional: bool) -> Value {
    // The frozen constraint fragment explains the encoding once for the Tool;
    // repeating it in every property needlessly consumes the schema byte budget.
    json!({"type":"string"})
}

// Bound work before expanding shared references. A final wire-size check alone
// cannot stop a small DAG from allocating a large intermediate tree.
struct ExpansionBudget {
    nodes: usize,
    bytes: usize,
}
impl ExpansionBudget {
    fn charge(&mut self, schema: &Value) -> Option<()> {
        let bytes = serde_json::to_vec(schema).ok()?.len();
        self.nodes = self.nodes.checked_sub(1)?;
        self.bytes = self.bytes.checked_sub(bytes)?;
        Some(())
    }
}
fn lower(
    schema: &Value,
    root: &Value,
    fine_tuned: bool,
    depth: usize,
    visiting: &mut BTreeSet<String>,
    expansion: &mut ExpansionBudget,
) -> Option<Value> {
    if depth > 8 {
        return None;
    }
    expansion.charge(schema)?;
    let node = schema.as_object()?;
    if let Some(reference) = node.get("$ref") {
        let reference = reference.as_str()?;
        if node
            .keys()
            .any(|key| !matches!(key.as_str(), "$ref" | "description" | "title" | "$comment"))
            || !reference.starts_with('#')
            || !visiting.insert(reference.into())
        {
            return None;
        }
        let target = root.pointer(&reference[1..])?;
        let result = lower(target, root, fine_tuned, depth + 1, visiting, expansion);
        visiting.remove(reference);
        return result;
    }
    if let Some(alternatives) = node.get("anyOf").or_else(|| node.get("oneOf")) {
        // Keeping only a union would lose sibling intersections. Use JSON text
        // for mixed forms rather than accidentally narrowing their values.
        if node.keys().any(|key| {
            !matches!(
                key.as_str(),
                "anyOf" | "oneOf" | "description" | "title" | "$comment"
            )
        }) {
            return None;
        }
        let branches: Vec<_> = alternatives
            .as_array()?
            .iter()
            .map(|branch| lower(branch, root, fine_tuned, depth + 1, visiting, expansion))
            .collect::<Option<_>>()?;
        return Some(json!({"anyOf":branches}));
    }
    let kind = node.get("type")?;
    let types = schema_types(kind)?;
    let mut result = Map::new();
    result.insert("type".into(), kind.clone());
    if types.contains(&"object") {
        if node.contains_key("patternProperties")
            || node.get("additionalProperties") != Some(&Value::Bool(false))
        {
            return None;
        }
        let properties = node.get("properties")?.as_object()?;
        if !all_required(node, properties) {
            return None;
        }
        let mut projected = Map::new();
        for (name, child) in properties {
            projected.insert(
                name.clone(),
                lower(child, root, fine_tuned, depth + 1, visiting, expansion)?,
            );
        }
        result.insert("properties".into(), Value::Object(projected));
        result.insert(
            "required".into(),
            node.get("required").cloned().unwrap_or_else(|| json!([])),
        );
        result.insert("additionalProperties".into(), Value::Bool(false));
    }
    if types.contains(&"array") {
        if node.contains_key("prefixItems") || node.get("items").is_some_and(Value::is_array) {
            return None;
        }
        result.insert(
            "items".into(),
            lower(
                node.get("items")?,
                root,
                fine_tuned,
                depth + 1,
                visiting,
                expansion,
            )?,
        );
    }
    if let Some(value) = node.get("enum") {
        if value.as_array().is_some_and(|values| {
            !values.is_empty()
                && values
                    .iter()
                    .all(|value| !value.is_object() && !value.is_array())
        }) {
            result.insert("enum".into(), value.clone());
        }
    } else if let Some(value) = node.get("const") {
        if !value.is_object() && !value.is_array() {
            result.insert("enum".into(), json!([value]));
        }
    }
    if !fine_tuned {
        for key in [
            "pattern",
            "minimum",
            "maximum",
            "exclusiveMinimum",
            "exclusiveMaximum",
            "multipleOf",
            "minItems",
            "maxItems",
        ] {
            if let Some(value) = node.get(key) {
                result.insert(key.into(), value.clone());
            }
        }
        if let Some(value) = node
            .get("format")
            .filter(|value| value.as_str().is_some_and(known_format))
        {
            result.insert("format".into(), value.clone());
        }
    }
    if let Some(description) = node.get("description").filter(|value| value.is_string()) {
        result.insert("description".into(), description.clone());
    }
    Some(Value::Object(result))
}
fn schema_types(value: &Value) -> Option<Vec<&str>> {
    let types = if let Some(kind) = value.as_str() {
        vec![kind]
    } else {
        let values = value.as_array()?;
        if values.len() != 2 || !values.iter().any(|kind| kind == "null") {
            return None;
        }
        values
            .iter()
            .map(Value::as_str)
            .collect::<Option<Vec<_>>>()?
    };
    types
        .iter()
        .all(|kind| {
            matches!(
                *kind,
                "string" | "number" | "integer" | "boolean" | "object" | "array" | "null"
            )
        })
        .then_some(types)
}
fn all_required(node: &Map<String, Value>, properties: &Map<String, Value>) -> bool {
    let Some(required) = node.get("required").and_then(Value::as_array) else {
        return properties.is_empty();
    };
    let names: BTreeSet<_> = required.iter().filter_map(Value::as_str).collect();
    names.len() == required.len()
        && names.len() == properties.len()
        && properties.keys().all(|name| names.contains(name.as_str()))
}
fn known_format(value: &str) -> bool {
    matches!(
        value,
        "date-time"
            | "time"
            | "date"
            | "duration"
            | "email"
            | "hostname"
            | "ipv4"
            | "ipv6"
            | "uuid"
    )
}
#[derive(Default)]
struct Bounds {
    properties: usize,
    enums: usize,
    characters: usize,
}
/// Check only; this never rewrites the already compiled wire schema.
pub(crate) fn strict_schema_supported(schema: &Value, fine_tuned: bool) -> bool {
    supported_with(schema, SchemaPolicy::openai(fine_tuned))
}
pub(crate) fn azure_strict_schema_supported(schema: &Value) -> bool {
    supported_with(schema, SchemaPolicy::azure())
}
fn supported_with(schema: &Value, policy: SchemaPolicy) -> bool {
    schema.get("type") == Some(&json!("object"))
        && schema.get("anyOf").is_none()
        && strict_node(schema, schema, policy, 0, &mut Bounds::default())
}
fn strict_node(
    schema: &Value,
    root: &Value,
    policy: SchemaPolicy,
    depth: usize,
    bounds: &mut Bounds,
) -> bool {
    let Some(node) = schema.as_object() else {
        return false;
    };
    if depth > policy.max_depth
        || node.keys().any(|key| {
            !matches!(
                key.as_str(),
                "type"
                    | "properties"
                    | "required"
                    | "additionalProperties"
                    | "items"
                    | "enum"
                    | "anyOf"
                    | "$defs"
                    | "$ref"
                    | "description"
                    | "title"
                    | "pattern"
                    | "format"
                    | "minimum"
                    | "maximum"
                    | "exclusiveMinimum"
                    | "exclusiveMaximum"
                    | "multipleOf"
                    | "minItems"
                    | "maxItems"
            )
        })
    {
        return false;
    }
    if policy.restricted_constraints
        && [
            "pattern",
            "format",
            "minimum",
            "maximum",
            "exclusiveMinimum",
            "exclusiveMaximum",
            "multipleOf",
            "minItems",
            "maxItems",
        ]
        .iter()
        .any(|key| node.contains_key(*key))
    {
        return false;
    }
    if node
        .get("format")
        .is_some_and(|value| !value.as_str().is_some_and(known_format))
    {
        return false;
    }
    if let Some(reference) = node.get("$ref") {
        if reference.as_str().is_none_or(|reference| {
            !reference.starts_with('#') || root.pointer(&reference[1..]).is_none()
        }) {
            return false;
        }
    }
    let types = match node.get("type") {
        Some(value) => match schema_types(value) {
            Some(types) => types,
            None => return false,
        },
        None if node.contains_key("anyOf") || node.contains_key("$ref") => vec![],
        None => return false,
    };
    if types.contains(&"object") {
        if depth > policy.max_object_depth {
            return false;
        }
        let Some(properties) = node.get("properties").and_then(Value::as_object) else {
            return false;
        };
        if node.get("additionalProperties") != Some(&Value::Bool(false))
            || !node.contains_key("required")
            || !all_required(node, properties)
        {
            return false;
        }
    }
    if types.contains(&"array") && !node.contains_key("items") {
        return false;
    }
    if let Some(values) = node.get("enum") {
        let Some(values) = values.as_array() else {
            return false;
        };
        if values.is_empty()
            || values
                .iter()
                .any(|value| value.is_array() || value.is_object())
        {
            return false;
        }
        bounds.enums = bounds.enums.saturating_add(values.len());
        let characters: usize = values
            .iter()
            .filter_map(Value::as_str)
            .map(|value| value.chars().count())
            .sum();
        if values.len() > 250 && characters > 15_000 {
            return false;
        }
        bounds.characters = bounds.characters.saturating_add(characters);
    }
    for key in ["properties", "$defs"] {
        if let Some(children) = node.get(key) {
            let Some(children) = children.as_object() else {
                return false;
            };
            if key == "properties" {
                bounds.properties = bounds.properties.saturating_add(children.len());
            }
            bounds.characters = bounds.characters.saturating_add(
                children
                    .keys()
                    .map(|name| name.chars().count())
                    .sum::<usize>(),
            );
            for child in children.values() {
                if !strict_node(child, root, policy, depth + 1, bounds) {
                    return false;
                }
            }
        }
    }
    if let Some(items) = node.get("items") {
        if !strict_node(items, root, policy, depth + 1, bounds) {
            return false;
        }
    }
    if let Some(branches) = node.get("anyOf") {
        let Some(branches) = branches.as_array() else {
            return false;
        };
        if branches.is_empty()
            || branches
                .iter()
                .any(|branch| !strict_node(branch, root, policy, depth + 1, bounds))
        {
            return false;
        }
    }
    bounds.properties <= policy.max_properties
        && bounds.enums <= 1000
        && bounds.characters <= 120_000
}
fn invalid(path: &str) -> ContractError {
    ContractError::new(
        ErrorCode::UnsupportedInputProjection,
        format!("responses.schema.{path}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reference_expansion_stops_before_exhausting_node_or_byte_work_limits() {
        let root = json!({"$defs":{"leaf":{"type":"string"}},"type":"object","properties":{"a":{"$ref":"#/$defs/leaf"},"b":{"$ref":"#/$defs/leaf"}},"required":["a","b"],"additionalProperties":false});
        for (nodes, bytes) in [(3, 32 * 1024), (1024, 16)] {
            let mut budget = ExpansionBudget { nodes, bytes };
            assert!(lower(&root, &root, false, 1, &mut BTreeSet::new(), &mut budget).is_none());
        }
        let mut budget = ExpansionBudget {
            nodes: 1024,
            bytes: 32 * 1024,
        };
        let expanded = lower(&root, &root, false, 1, &mut BTreeSet::new(), &mut budget).unwrap();
        assert_eq!(expanded["properties"]["a"], json!({"type":"string"}));
        assert_eq!(expanded["properties"]["b"], json!({"type":"string"}));
    }
}
```

## `crates/wickle-state-sqlite/tests/agent_recovery.rs`

```rust
//! A new process recovers a real durable run after its predecessor exits mid-call.
#[path = "../../wickle/tests/support/agent.rs"]
#[allow(dead_code)]
mod support;
use support as agent_support;
#[path = "support/command_recovery.rs"]
mod command_recovery;
#[path = "support/context_recovery.rs"]
mod context_recovery;
#[path = "support/recovery_store.rs"]
mod recovery_store;
#[path = "../../wickle/tests/support/agent_resume.rs"]
#[allow(dead_code)]
mod tool_support;
use std::{
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};
use support::*;
use wickle::*;
use wickle_state_sqlite::SqliteStateStore;

// Serialize process fixtures to keep resource contention outside these tests.
static PROCESS_TESTS: std::sync::Mutex<()> = std::sync::Mutex::new(());

// Fault-boundary tests advance cross-process time explicitly. Synchronous fsync
// or scheduler stalls must not consume a one-second lease before the crash point.
// Real-time heartbeat and expiry behavior is covered by the agent/store tests.
struct ProcessClock {
    utc_ms: i64,
}
impl ProcessClock {
    fn new(replacement: bool) -> Self {
        Self {
            utc_ms: if replacement { 3000 } else { 1000 },
        }
    }
}
impl Clock for ProcessClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: self.utc_ms,
            monotonic_ms: 0,
        })
    }
    fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}

struct ProcessModel {
    inner: Arc<dyn ModelPort>,
    directory: std::path::PathBuf,
    interrupt: bool,
}
impl ModelPort for ProcessModel {
    fn binding(&self) -> ModelPortBinding {
        self.inner.binding()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        use std::io::Write;
        let mut calls = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join("calls"))
            .unwrap();
        writeln!(calls, "{}", context.attempt_id).unwrap();
        calls.sync_all().unwrap();
        std::fs::write(self.directory.join("run"), context.run_id.as_str()).unwrap();
        if self.interrupt {
            std::process::exit(73);
        }
        self.inner.generate(request, context)
    }
}
fn worker(directory: &Path, mode: &str) -> std::process::ExitStatus {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "recovery_worker", "--nocapture"])
        .env("WICKLE_RECOVERY_PROCESS_DIRECTORY", directory)
        .env("WICKLE_RECOVERY_PROCESS_MODE", mode)
        .stdin(Stdio::null())
        .spawn()
        .unwrap();
    let end = std::time::Instant::now() + Duration::from_secs(30);
    let mut killed = false;
    loop {
        if !killed
            && mode.ends_with("-interrupt")
            && std::fs::read_to_string(directory.join("kill-ready"))
                .ok()
                .as_deref()
                == Some("ready\n")
        {
            child.kill().unwrap();
            killed = true;
        }
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if std::time::Instant::now() >= end {
            let _ = child.kill();
            let _ = child.wait();
            panic!("recovery process timed out");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
#[test]
fn a_replacement_process_recovers_an_interrupted_model_with_a_new_charged_attempt() {
    let _serial = PROCESS_TESTS.lock().unwrap();
    let directory = std::env::temp_dir().join(format!(
        "wickle-agent-recovery-{}",
        RandomIdSource.next_id().unwrap()
    ));
    std::fs::create_dir(&directory).unwrap();
    assert_eq!(worker(&directory, "interrupt").code(), Some(73));
    // The replacement clock starts after the original lease's expiry.
    assert!(worker(&directory, "recover").success());
    let calls = std::fs::read_to_string(directory.join("calls")).unwrap();
    let attempts: Vec<_> = calls.lines().collect();
    assert_eq!(attempts.len(), 2);
    assert_ne!(attempts[0], attempts[1]);
    assert!(directory.join("verified").exists());
    std::fs::remove_dir_all(directory).unwrap();
}
#[test]
#[ignore = "Child process fixture requires explicit private test configuration"]
fn recovery_worker() {
    let directory =
        std::path::PathBuf::from(std::env::var_os("WICKLE_RECOVERY_PROCESS_DIRECTORY").unwrap());
    let mode = std::env::var("WICKLE_RECOVERY_PROCESS_MODE").unwrap();
    if mode.starts_with("command-") {
        command_recovery::run_worker(&directory, &mode);
        return;
    }
    if mode.starts_with("context-") {
        context_recovery::run_worker(&directory, &mode);
        return;
    }
    if mode.starts_with("tool-") {
        run_tool_worker(&directory, &mode);
        return;
    }
    let interrupt = mode == "interrupt";
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let fixture = Fixture::new(Response::Text, false);
            let store = Arc::new(SqliteStateStore::open(directory.join("state.sqlite")).unwrap());
            let mut bindings = fixture.bindings();
            bindings.state = store.clone();
            bindings.clock = Arc::new(ProcessClock::new(!interrupt));
            bindings.ids = Arc::new(RandomIdSource);
            bindings.model_exchange = Arc::new(
                ModelExchange::new(
                    Arc::new(ProcessModel {
                        inner: fixture.model.clone(),
                        directory: directory.clone(),
                        interrupt,
                    }),
                    bindings.policy.clone(),
                )
                .with_route_inspector(fixture.inspector.clone(), Duration::from_secs(1))
                .unwrap(),
            );
            let mut profile = profile();
            profile.limits.max_recovery_attempts = 2;
            profile.limits.max_elapsed_ms = 60000.try_into().unwrap();
            let agent = create_agent(profile, bindings).unwrap();
            if interrupt {
                let handle = completed(agent.start(request("request"), context()).await.unwrap());
                let _ = handle.outcome(&context()).await;
                panic!("model did not terminate process");
            }
            let run_id = id(&std::fs::read_to_string(directory.join("run")).unwrap());
            let before = store.load(&scope(), &run_id).await.unwrap();
            assert_eq!(before.snapshot.status, RunStatus::Running);
            assert!(matches!(
                before.snapshot.model_ledger[0].state,
                ModelAttemptState::Reserved {}
            ));
            // A new process cannot take ownership while the old lease is live.
            assert_eq!(
                store
                    .acquire_lease(&scope(), &run_id, &id("too-early"), 1000, 1000)
                    .await
                    .unwrap_err()
                    .code,
                ErrorCode::LeaseBusy,
            );
            let source = before.snapshot.recovery_record(id("source")).unwrap();
            let command = ResumeCommand {
                run_id: run_id.clone(),
                expected_revision: before.snapshot.revision,
                command_id: id("recover"),
                action: ResumeAction::Recover {
                    recovery_ref: source.reference().clone(),
                },
            };
            let handle = completed(agent.resume(command.clone(), context()).await.unwrap());
            let outcome = completed(handle.outcome(&context()).await.unwrap());
            assert_eq!(outcome.result.status(), RunStatus::Succeeded);
            let after = store.load(&scope(), &run_id).await.unwrap();
            assert_eq!(after.snapshot.usage.model_calls, 2);
            assert_eq!(after.snapshot.usage.recovery_attempts, 1);
            assert!(matches!(
                after.snapshot.model_ledger[0].state,
                ModelAttemptState::Interrupted { .. }
            ));
            assert_eq!(
                after.snapshot.model_ledger[0].model_step_id,
                after.snapshot.model_ledger[1].model_step_id
            );
            let replay = completed(agent.resume(command, context()).await.unwrap());
            assert_eq!(
                completed(replay.outcome(&context()).await.unwrap()),
                outcome
            );
            std::fs::write(directory.join("verified"), b"recovered and replayed").unwrap();
        });
}

struct ProcessTool {
    inner: Arc<dyn ToolExecutor>,
    name: &'static str,
    crash_on_write: bool,
    directory: std::path::PathBuf,
}
fn append(path: &Path, value: &str) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{value}").unwrap();
    file.sync_all().unwrap();
}
impl ToolExecutor for ProcessTool {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            append(&self.directory.join("tool-calls"), self.name);
            if self.name == "before" && self.crash_on_write {
                // Exceed the fixture lease in wall time, as a slow fsync can do.
                // Explicit test time must still reach the intended write/crash.
                std::thread::sleep(Duration::from_millis(1200));
            }
            if self.name != "target" {
                return self.inner.execute(args, context).await;
            }
            let path = self.directory.join("effect.json");
            // create_new makes any repeated write an observable test failure.
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path)
                .unwrap();
            file.write_all(&serde_json::to_vec(&serde_json::json!({"arguments":args,"attempt":context.attempt_id,"key":context.idempotency_key})).unwrap()).unwrap();
            file.sync_all().unwrap();
            if self.crash_on_write {
                std::process::exit(74);
            }
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: serde_json::json!("target"),
                },
                effect: ToolEffect::Applied,
                receipt: Some(serde_json::json!({"effect_id":"durable-write"})),
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolReconciliation> {
        Box::pin(async move {
            assert_eq!(self.name, "target");
            append(&self.directory.join("queries"), self.name);
            let evidence: serde_json::Value =
                serde_json::from_slice(&std::fs::read(self.directory.join("effect.json")).unwrap())
                    .unwrap();
            assert_eq!(evidence["arguments"], serde_json::to_value(args).unwrap());
            assert_eq!(
                evidence["attempt"],
                serde_json::to_value(&context.attempt_id).unwrap()
            );
            assert_eq!(
                evidence["key"],
                serde_json::to_value(&context.idempotency_key).unwrap()
            );
            Ok(ToolReconciliation::Known {
                result: ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Succeeded {
                        value: serde_json::json!("target"),
                    },
                    effect: ToolEffect::Applied,
                    receipt: Some(serde_json::json!({"effect_id":"durable-write"})),
                },
            })
        })
    }
}
#[test]
fn an_applied_write_is_reconciled_after_process_exit_or_remains_unknown_without_a_query_adapter() {
    let _serial = PROCESS_TESTS.lock().unwrap();
    for known in [true, false] {
        let directory = std::env::temp_dir().join(format!(
            "wickle-tool-recovery-{}",
            RandomIdSource.next_id().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let kind = if known { "known" } else { "unknown" };
        assert_eq!(
            worker(&directory, &format!("tool-{kind}-interrupt")).code(),
            Some(74)
        );
        let effect = std::fs::read(directory.join("effect.json")).unwrap();
        assert!(worker(&directory, &format!("tool-{kind}-recover")).success());
        assert_eq!(
            std::fs::read(directory.join("effect.json")).unwrap(),
            effect
        );
        let calls = std::fs::read_to_string(directory.join("tool-calls")).unwrap();
        assert_eq!(
            calls,
            if known {
                "before\ntarget\nafter\n"
            } else {
                "before\ntarget\n"
            }
        );
        if known {
            assert_eq!(
                std::fs::read_to_string(directory.join("queries")).unwrap(),
                "target\n"
            );
        } else {
            assert!(!directory.join("queries").exists());
        }
        assert!(directory.join("verified").exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
fn run_tool_worker(directory: &Path, mode: &str) {
    use std::sync::atomic::Ordering;
    let boundary = mode
        .strip_prefix("tool-boundary-")
        .map(|value| value.rsplit_once('-').unwrap().0);
    let known = mode.starts_with("tool-known-") || boundary.is_some();
    let interrupt = mode.ends_with("-interrupt");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut fixture = tool_support::Fixture::new(tool_support::Mode::External);
            fixture.profile.limits.max_elapsed_ms = 60000.try_into().unwrap();
            fixture.profile.limits.max_recovery_attempts = 4;
            let store = Arc::new(SqliteStateStore::open(directory.join("state.sqlite")).unwrap());
            let mut bindings = fixture.bindings();
            bindings.state = match (interrupt, boundary) {
                (true, Some(boundary)) => Arc::new(recovery_store::CrashStore {
                    inner: store.clone(),
                    boundary: boundary.into(),
                    kill_marker: matches!(
                        boundary,
                        "admitted" | "before-prepared" | "prepared" | "reserved"
                    )
                    .then(|| directory.join("kill-ready")),
                }),
                _ => store.clone(),
            };
            bindings.clock = Arc::new(ProcessClock::new(!interrupt));
            bindings.ids = Arc::new(RandomIdSource);
            if !interrupt {
                // The replacement exercises normal durable-run settings after
                // the explicit clock handoff expires the original short lease.
                let defaults = AgentSettings::default();
                bindings.settings.lease_ttl_ms = defaults.lease_ttl_ms;
                bindings.settings.heartbeat_interval_ms = defaults.heartbeat_interval_ms;
            }
            let mut registrations = vec![];
            for (index, name) in ["before", "target", "after"].into_iter().enumerate() {
                let mut descriptor = fixture
                    .registry
                    .get(&id(name))
                    .unwrap()
                    .compiled
                    .descriptor()
                    .clone();
                descriptor.reconcile = known && name == "target";
                registrations.push(ToolRegistration {
                    compiled: SchemaCompiler::new()
                        .compile(descriptor, &fixture.inputs)
                        .unwrap(),
                    executor: Arc::new(ProcessTool {
                        inner: fixture.tools[index].clone(),
                        name,
                        crash_on_write: boundary.is_none(),
                        directory: directory.to_owned(),
                    }),
                });
            }
            bindings.tools = Some(Arc::new(ToolRegistry::new(scope(), registrations).unwrap()));
            if !interrupt {
                let count = std::fs::read_to_string(directory.join("calls"))
                    .unwrap_or_default()
                    .lines()
                    .count();
                fixture.model.calls.store(count, Ordering::SeqCst);
                // A changed resolver must not replace the already saved foreign key.
                if boundary.is_none() || matches!(boundary, Some("bound" | "settled" | "terminal"))
                {
                    fixture.resolver.value.lock().unwrap().value =
                        serde_json::json!(tool_support::CHANGED_RECORD);
                }
            }
            bindings.model_exchange = Arc::new(
                ModelExchange::new(
                    Arc::new(ProcessModel {
                        inner: fixture.model.clone(),
                        directory: directory.to_owned(),
                        interrupt: false,
                    }),
                    bindings.policy.clone(),
                )
                .with_route_inspector(fixture.base.inspector.clone(), Duration::from_secs(1))
                .unwrap(),
            );
            let agent = create_agent(fixture.profile.clone(), bindings).unwrap();
            if interrupt {
                let handle = fixture.started(&agent).await;
                let outcome = handle.outcome(&context()).await;
                panic!("write did not terminate process: {outcome:?}");
            }
            let run_id = match std::fs::read_to_string(directory.join("run")) {
                Ok(value) => id(&value),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    store
                        .find_request(&scope(), &id("session"), &id("request"))
                        .await
                        .unwrap()
                        .expect("durable admission")
                        .snapshot
                        .run_id
                }
                Err(error) => panic!("run identifier: {error}"),
            };
            let before = store.load(&scope(), &run_id).await.unwrap();
            match boundary {
                Some("admitted" | "before-prepared") => {
                    assert!(before.snapshot.prepared_steps.is_empty());
                    assert!(before.snapshot.model_ledger.is_empty());
                }
                Some("prepared") => {
                    assert!(!before.snapshot.prepared_steps.is_empty());
                    assert!(before.snapshot.model_ledger.is_empty());
                }
                Some("reserved") => {
                    assert!(!before.snapshot.prepared_steps.is_empty());
                    assert_eq!(before.snapshot.model_ledger.len(), 1);
                    assert!(matches!(
                        before.snapshot.model_ledger[0].state,
                        ModelAttemptState::Reserved {}
                    ));
                }
                _ => {}
            }
            if boundary == Some("terminal") {
                assert_eq!(before.snapshot.status, RunStatus::Succeeded);
                let replay = fixture.started(&agent).await;
                assert_eq!(
                    completed(replay.outcome(&context()).await.unwrap()),
                    before.snapshot.outcome.unwrap()
                );
                std::fs::write(directory.join("verified"), b"terminal replay").unwrap();
                return;
            }
            if boundary.is_none() {
                assert!(matches!(
                    before.snapshot.tool_ledger[1].state,
                    ToolCallState::Dispatching { .. }
                ));
            }
            let source = before.snapshot.recovery_record(id("write-source")).unwrap();
            let command = ResumeCommand {
                run_id: run_id.clone(),
                expected_revision: before.snapshot.revision,
                command_id: id("recover-write"),
                action: ResumeAction::Recover {
                    recovery_ref: source.reference().clone(),
                },
            };
            let handle = completed(agent.resume(command.clone(), context()).await.unwrap());
            let outcome = completed(handle.outcome(&context()).await.unwrap());
            assert_eq!(
                outcome.result.status(),
                if known {
                    RunStatus::Succeeded
                } else {
                    RunStatus::Waiting
                }
            );
            assert_eq!(outcome.unresolved_effects.len(), usize::from(!known));
            let after = store.load(&scope(), &run_id).await.unwrap();
            if matches!(
                boundary,
                Some("admitted" | "before-prepared" | "prepared" | "reserved")
            ) {
                assert_eq!(
                    after.snapshot.usage.model_calls,
                    if boundary == Some("reserved") { 3 } else { 2 }
                );
                for prepared in &before.snapshot.prepared_steps {
                    assert!(after.snapshot.prepared_steps.contains(prepared));
                }
                if boundary == Some("reserved") {
                    assert!(matches!(
                        after.snapshot.model_ledger[0].state,
                        ModelAttemptState::Interrupted { .. }
                    ));
                }
            }

            if boundary.is_none() || matches!(boundary, Some("bound" | "settled")) {
                assert_eq!(
                    after.snapshot.tool_ledger[1].call.bound_input_ref,
                    before.snapshot.tool_ledger[1].call.bound_input_ref
                );
                assert_eq!(fixture.resolver.calls.load(Ordering::SeqCst), 0);
            }
            let replay = completed(agent.resume(command, context()).await.unwrap());
            assert_eq!(
                completed(replay.outcome(&context()).await.unwrap()),
                outcome
            );
            std::fs::write(directory.join("verified"), b"effect preserved and replayed").unwrap();
        });
}

#[test]
fn durable_plan_binding_result_and_terminal_boundaries_resume_without_repeating_completed_work() {
    let _serial = PROCESS_TESTS.lock().unwrap();
    for boundary in ["before-plan", "plan", "bound", "settled", "terminal"] {
        let directory = std::env::temp_dir().join(format!(
            "wickle-boundary-recovery-{}",
            RandomIdSource.next_id().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        assert_eq!(
            worker(&directory, &format!("tool-boundary-{boundary}-interrupt")).code(),
            Some(75),
            "{boundary}"
        );
        let calls = std::fs::read_to_string(directory.join("tool-calls")).unwrap_or_default();
        assert_eq!(
            calls,
            match boundary {
                "before-plan" | "plan" => "",
                "bound" => "before\n",
                "settled" => "before\ntarget\n",
                _ => "before\ntarget\nafter\n",
            },
            "{boundary}"
        );
        assert!(
            worker(&directory, &format!("tool-boundary-{boundary}-recover")).success(),
            "{boundary}"
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("tool-calls")).unwrap(),
            "before\ntarget\nafter\n",
            "{boundary}"
        );
        assert!(!directory.join("queries").exists(), "{boundary}");
        assert_eq!(
            std::fs::read_to_string(directory.join("calls"))
                .unwrap()
                .lines()
                .count(),
            2,
            "{boundary}"
        );
        assert!(directory.join("verified").exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn compression_revision_process_boundaries_restore_only_complete_context_without_recompression() {
    let _serial = PROCESS_TESTS.lock().unwrap();
    for boundary in ["before-context", "context"] {
        let directory = std::env::temp_dir().join(format!(
            "wickle-context-recovery-{}",
            RandomIdSource.next_id().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        assert_eq!(
            worker(&directory, &format!("context-{boundary}-interrupt")).code(),
            Some(76)
        );
        assert!(worker(&directory, &format!("context-{boundary}-recover")).success());
        assert_eq!(
            std::fs::read_to_string(directory.join("reads")).unwrap(),
            "read\nread\n"
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("summaries"))
                .unwrap()
                .lines()
                .count(),
            if boundary == "context" { 1 } else { 2 }
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("calls"))
                .unwrap()
                .lines()
                .count(),
            3
        );
        assert!(directory.join("verified").exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn forced_process_termination_preserves_admission_preparation_and_dispatch_reservations() {
    let _serial = PROCESS_TESTS.lock().unwrap();
    for boundary in ["admitted", "before-prepared", "prepared", "reserved"] {
        let directory = std::env::temp_dir().join(format!(
            "wickle-prepared-kill-{}",
            RandomIdSource.next_id().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let status = worker(&directory, &format!("tool-boundary-{boundary}-interrupt"));
        assert!(!status.success(), "{boundary}");
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(
                status.signal(),
                Some(9),
                "parent must SIGKILL the paused worker"
            );
        }
        assert_eq!(
            std::fs::read_to_string(directory.join("calls")).unwrap_or_default(),
            ""
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("tool-calls")).unwrap_or_default(),
            ""
        );
        assert!(
            worker(&directory, &format!("tool-boundary-{boundary}-recover")).success(),
            "{boundary}"
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("tool-calls")).unwrap(),
            "before\ntarget\nafter\n"
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("calls"))
                .unwrap()
                .lines()
                .count(),
            2
        );
        assert!(directory.join("verified").exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn forced_process_termination_never_half_consumes_an_input_command_or_replays_its_tool() {
    let _serial = PROCESS_TESTS.lock().unwrap();
    for boundary in ["wait", "before-command", "accepted-command"] {
        let directory = std::env::temp_dir().join(format!(
            "wickle-command-kill-{}",
            RandomIdSource.next_id().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let status = worker(&directory, &format!("command-{boundary}-interrupt"));
        assert!(!status.success());
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(9));
        }
        assert_eq!(
            std::fs::read_to_string(directory.join("tool-calls")).unwrap(),
            "before\ntarget\n"
        );
        assert!(
            worker(&directory, &format!("command-{boundary}-recover")).success(),
            "{boundary}"
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("tool-calls")).unwrap(),
            "before\ntarget\nafter\n"
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("calls"))
                .unwrap()
                .lines()
                .count(),
            2
        );
        assert!(directory.join("verified").exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
```

## `crates/wickle-state-sqlite/tests/execution_store.rs`

```rust
//! Same atomic execution contract through real SQLite transactions and reopen.
use std::sync::Arc;
use wickle::*;
use wickle_state_sqlite::SqliteStateStore;
#[allow(dead_code)]
#[path = "../../wickle/tests/support/mod.rs"]
mod core;
use core::*;
#[path = "../../wickle/tests/support/execution_store.rs"]
mod suite;
#[allow(dead_code)]
mod support;
#[tokio::test]
async fn sqlite_execution_transactions_survive_reopen() {
    let database = support::Database::new();
    let store = Arc::new(SqliteStateStore::open(database.path()).unwrap());
    suite::atomic_execution_contract(store.clone()).await;
    let expected = store
        .read_execution(&scope(), &id("atomic-run"))
        .await
        .unwrap();
    drop(store);
    let reopened = SqliteStateStore::open(database.path()).unwrap();
    assert_eq!(
        reopened
            .read_execution(&scope(), &id("atomic-run"))
            .await
            .unwrap(),
        expected
    );
}
#[tokio::test]
async fn sqlite_recovery_rolls_back_and_replays_the_same_segment() {
    let database = support::Database::new();
    let store = Arc::new(SqliteStateStore::open(database.path()).unwrap());
    suite::atomic_recovery_contract(store.clone()).await;
    let history = store
        .read_execution(&scope(), &id("recover-run"))
        .await
        .unwrap();
    drop(store);
    assert_eq!(
        SqliteStateStore::open(database.path())
            .unwrap()
            .read_execution(&scope(), &id("recover-run"))
            .await
            .unwrap(),
        history
    );
}

#[tokio::test]
async fn legacy_terminal_rows_are_read_without_rewrite_then_upgrade_on_new_admission() {
    let database = support::Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    let first = store
        .admit(
            &scope(),
            admission("legacy", "legacy-request", "legacy-session", "input", "1").await,
        )
        .await
        .unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("legacy"), &id("owner"), 0, 10000)
        .await
        .unwrap();
    let terminal = store
        .commit(
            &scope(),
            &id("legacy"),
            finished(&first.state.snapshot, lease, 1),
        )
        .await
        .unwrap();
    drop(store);
    let conn = rusqlite::Connection::open(database.path()).unwrap();
    let raw: String = conn
        .query_row(
            "SELECT checkpoint_json FROM wickle_scope_checkpoints",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut legacy: serde_json::Value = serde_json::from_str(&raw).unwrap();
    legacy["schema_version"] = serde_json::json!("wickle.state-store.v1");
    legacy.as_object_mut().unwrap().remove("executions");
    conn.execute(
        "UPDATE wickle_scope_checkpoints SET checkpoint_json=?1,checksum=?2",
        rusqlite::params![legacy.to_string(), canonical_digest(&legacy).as_str()],
    )
    .unwrap();
    let store = SqliteStateStore::open(database.path()).unwrap();
    assert_eq!(store.load(&scope(), &id("legacy")).await.unwrap(), terminal);
    let after_read: String = conn
        .query_row(
            "SELECT checkpoint_json FROM wickle_scope_checkpoints",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(after_read, legacy.to_string());
    store
        .admit(
            &scope(),
            admission("new", "new-request", "new-session", "input", "1").await,
        )
        .await
        .unwrap();
    let after_write: String = conn
        .query_row(
            "SELECT checkpoint_json FROM wickle_scope_checkpoints",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&after_write).unwrap()["schema_version"],
        "wickle.state-store.v2"
    );
    assert_eq!(store.load(&scope(), &id("legacy")).await.unwrap(), terminal);
    assert!(store.read_execution(&scope(), &id("legacy")).await.is_err());
    assert!(store.read_execution(&scope(), &id("new")).await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_conflicting_submissions_keep_only_the_winning_sqlite_snapshot() {
    let database = support::Database::new();
    suite::conflicting_submissions_race([
        Arc::new(SqliteStateStore::open(database.path()).unwrap()),
        Arc::new(SqliteStateStore::open(database.path()).unwrap()),
    ])
    .await;
    let reopened = SqliteStateStore::open(database.path()).unwrap();
    let saved = reopened
        .find_request(&scope(), &id("race-session"), &id("same-key"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        reopened
            .load_session(&scope(), &id("race-session"))
            .await
            .unwrap()
            .active_run_id,
        Some(saved.snapshot.run_id)
    );
}
```

## `crates/wickle-state-sqlite/tests/state_store.rs`

```rust
//! Real-file transactions, process isolation, and restoration of protected execution state.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use rusqlite::Connection;
use serde_json::{Value, json};
use wickle::*;
use wickle_state_sqlite::SqliteStateStore;

#[allow(dead_code)]
#[path = "../../wickle/tests/support/mod.rs"]
mod core;
mod support;
#[path = "support/workers.rs"]
mod workers;

use core::{event, finished, id, prepared, scope};
use support::{Database, durable_admission};

#[tokio::test]
async fn durable_admission_replays_the_original_run_and_releases_a_finished_session_after_reopen() {
    let database = Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    assert!(store.capabilities().durable);
    let first = store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap();
    assert!(first.created);
    drop(store);
    let store = SqliteStateStore::open(database.path()).unwrap();
    let replay = store
        .admit(
            &scope(),
            durable_admission("replacement", "request", "session").await,
        )
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state, first.state);
    assert_eq!(
        store
            .find_request(&scope(), &id("session"), &id("request"))
            .await
            .unwrap(),
        Some(first.state.clone())
    );
    assert!(
        store
            .find_request(
                &Scope {
                    workspace_id: id("foreign"),
                    ..scope()
                },
                &id("session"),
                &id("request")
            )
            .await
            .unwrap()
            .is_none()
    );
    let mut changed = durable_admission("changed", "request", "session").await;
    changed.snapshot.request.input = vec![InputContent::Text {
        text: "Different input".into(),
    }];
    changed.snapshot.request_digest =
        admission_digest(&changed.snapshot.request, &changed.snapshot.profile, None);
    assert_eq!(
        store.admit(&scope(), changed).await.unwrap_err().code,
        ErrorCode::RequestConflict
    );
    assert_eq!(
        store
            .admit(
                &scope(),
                durable_admission("other", "other", "session").await
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::SessionBusy
    );
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 0, 10_000)
        .await
        .unwrap();
    store
        .commit(
            &scope(),
            &id("run"),
            finished(&first.state.snapshot, lease, 1),
        )
        .await
        .unwrap();
    drop(store);
    let store = SqliteStateStore::open(database.path()).unwrap();
    let mut second = durable_admission("second", "second", "session").await;
    assert_eq!(
        store
            .find_request(&scope(), &id("session"), &id("request"))
            .await
            .unwrap()
            .unwrap()
            .snapshot
            .status,
        RunStatus::Succeeded
    );
    second.messages[0].sequence = 2.try_into().unwrap();
    assert!(store.admit(&scope(), second).await.unwrap().created);
    assert_eq!(
        store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .status,
        RunStatus::Succeeded
    );
    let replay = store
        .admit(
            &scope(),
            durable_admission("another", "request", "session").await,
        )
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state.snapshot.run_id, id("run"));
}

#[tokio::test]
async fn failed_transactions_leave_no_records_or_events_and_stale_revisions_cannot_commit() {
    let database = Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    let initial = store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap()
        .state;
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 0, 10_000)
        .await
        .unwrap();
    let mut invalid = finished(&initial.snapshot, lease.clone(), 1);
    let unpublished = ProtectedRecord::new(id("unpublished"), 1, json!({"value":"candidate"}));
    invalid.records.push(unpublished.clone());
    invalid.events[0].seq = 9.try_into().unwrap();
    assert!(store.commit(&scope(), &id("run"), invalid).await.is_err());
    assert_eq!(store.load(&scope(), &id("run")).await.unwrap(), initial);
    assert!(
        store
            .read_record(&scope(), unpublished.reference())
            .await
            .is_err()
    );
    assert_eq!(
        store
            .read_events(&scope(), &id("run"), 0, 10)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
    let update = prepared(&initial.snapshot, lease.clone(), 2);
    let saved = store
        .commit(&scope(), &id("run"), update.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .commit(&scope(), &id("run"), update)
            .await
            .unwrap_err()
            .code,
        ErrorCode::RevisionConflict
    );
    let mut final_update = finished(&saved.snapshot, lease, 3);
    final_update.records.push(unpublished.clone());
    store
        .commit(&scope(), &id("run"), final_update)
        .await
        .unwrap();
    drop(store);
    let store = SqliteStateStore::open(database.path()).unwrap();
    assert_eq!(
        store
            .read_record(&scope(), unpublished.reference())
            .await
            .unwrap(),
        unpublished
    );
    let page = store.read_events(&scope(), &id("run"), 0, 1).await.unwrap();
    assert!(page.has_more);
    assert_eq!(page.next_after_seq, 1);
    let next = store
        .read_events(&scope(), &id("run"), page.next_after_seq, 1)
        .await
        .unwrap();
    assert!(!next.has_more);
    assert_eq!(next.next_after_seq, 2);
    assert!(matches!(
        next.events[0].payload,
        RunEventPayload::RunFinished { .. }
    ));
    for foreign in [
        Scope {
            tenant_id: id("foreign"),
            ..scope()
        },
        Scope {
            workspace_id: id("foreign-workspace"),
            ..scope()
        },
        Scope {
            user_id: Some(id("foreign-user")),
            ..scope()
        },
    ] {
        assert!(store.load(&foreign, &id("run")).await.is_err());
        assert!(store.load_session(&foreign, &id("session")).await.is_err());
        assert!(
            store
                .read_record(&foreign, unpublished.reference())
                .await
                .is_err()
        );
        assert!(
            store
                .read_events(&foreign, &id("run"), 0, 10)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn renewed_leases_and_released_fence_counters_survive_separate_connections_and_reopen() {
    let database = Database::new();
    let first = SqliteStateStore::open(database.path()).unwrap();
    first
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap();
    let lease = first
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 10_000)
        .await
        .unwrap();
    let renewed = first
        .renew_lease(&scope(), &id("run"), &lease, 105, 20_000)
        .await
        .unwrap();
    drop(first);
    let next = SqliteStateStore::open(database.path()).unwrap();
    let after_original_expiry = lease.expires_at_ms + 100;
    assert_eq!(
        next.check_lease(&scope(), &id("run"), &lease, after_original_expiry)
            .await
            .unwrap()
            .expires_at_ms,
        renewed.expires_at_ms
    );
    assert_eq!(
        next.acquire_lease(
            &scope(),
            &id("run"),
            &id("new-owner"),
            after_original_expiry + 1,
            10_000
        )
        .await
        .unwrap_err()
        .code,
        ErrorCode::LeaseBusy
    );
    let now = renewed.expires_at_ms + 1;
    let takeover = next
        .acquire_lease(&scope(), &id("run"), &id("owner"), now, 10_000)
        .await
        .unwrap();
    assert!(takeover.fencing_token > lease.fencing_token);
    assert_eq!(
        next.check_lease(&scope(), &id("run"), &lease, now + 1)
            .await
            .unwrap_err()
            .code,
        ErrorCode::LeaseLost
    );
    next.release_lease(&scope(), &id("run"), &takeover, now + 2)
        .await
        .unwrap();
    drop(next);
    let reopened = SqliteStateStore::open(database.path()).unwrap();
    let after_release = reopened
        .acquire_lease(&scope(), &id("run"), &id("owner"), now + 3, 10_000)
        .await
        .unwrap();
    assert!(after_release.fencing_token > takeover.fencing_token);
    assert_eq!(
        reopened
            .renew_lease(&scope(), &id("run"), &takeover, now + 4, 10_000)
            .await
            .unwrap_err()
            .code,
        ErrorCode::LeaseLost
    );
}

#[tokio::test]
async fn historical_wait_and_resume_events_remain_valid_after_terminal_reopen() {
    let database = Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    let saved = store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap()
        .state;
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 0, 10_000)
        .await
        .unwrap();
    // This low-level store test preserves generic candidate-review history;
    // actual tool approvals are exercised by the Agent resume consumer.
    let candidate = ProtectedRecord::new(id("candidate"), 1, json!({"selection":"saved"}));
    let target = ApprovalTarget::Candidate {
        candidate_ref: candidate.reference().clone(),
        verifier_ref: VersionedRef {
            id: id("reviewer"),
            version: id("1"),
        },
    };
    let wait = WaitState {
        wait_id: id("wait"),
        target: WaitTarget::Approval {
            target: target.clone(),
        },
        expires_at_ms: None,
    };
    let record = ProtectedRecord::new(id("wait-record"), 1, serde_json::to_value(&wait).unwrap());
    let mut update = prepared(&saved.snapshot, lease.clone(), 1);
    update.snapshot.status = RunStatus::Waiting;
    update.snapshot.phase = RunPhase::Waiting;
    update.snapshot.wait = Some(wait);
    update.snapshot.outcome = Some(RunOutcome {
        app_state: None,
        result: OutcomeResult::Waiting {
            wait: update.snapshot.wait.clone().unwrap(),
        },
        output: vec![],
        artifacts: vec![],
        usage: update.snapshot.usage.clone(),
        checkpoint_revision: update.snapshot.revision,
        verification: None,
        unresolved_effects: vec![],
    });
    let prior_outcome = ProtectedRecord::new(
        id("prior-outcome"),
        1,
        serde_json::to_value(update.snapshot.outcome.as_ref().unwrap()).unwrap(),
    );
    let prior_outcome_ref = prior_outcome.reference().clone();
    update.records.extend([candidate, prior_outcome]);
    update.snapshot.last_event_seq += 1;
    update.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        2,
        RunEventPayload::RunWaiting {
            outcome_ref: Some(prior_outcome_ref.clone()),
            wait_ref: record.reference().clone(),
        },
    ));
    update.events[0].timestamp_ms = 1;
    update.records.push(record);
    store.commit(&scope(), &id("run"), update).await.unwrap();
    store
        .release_lease(&scope(), &id("run"), &lease, 2)
        .await
        .unwrap();
    drop(store);
    let store = SqliteStateStore::open(database.path()).unwrap();
    assert_eq!(
        store
            .admit(
                &scope(),
                durable_admission("other", "other", "session").await
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::SessionBusy
    );
    let saved = store.load(&scope(), &id("run")).await.unwrap();
    assert_eq!(saved.snapshot.status, RunStatus::Waiting);
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("new-owner"), 3, 10_000)
        .await
        .unwrap();
    let command = ResumeCommand {
        run_id: id("run"),
        expected_revision: saved.snapshot.revision,
        command_id: id("resume"),
        action: ResumeAction::Approve {
            wait_id: id("wait"),
            target,
        },
    };
    let record = ProtectedRecord::new(
        id("resume-record"),
        1,
        serde_json::to_value(&command).unwrap(),
    );
    let mut update = prepared(&saved.snapshot, lease.clone(), 4);
    update.snapshot.status = RunStatus::Running;
    update.snapshot.wait = None;
    update.snapshot.outcome = None;
    update.snapshot.timing.last_observed_at_ms = 4;
    update.snapshot.usage.elapsed_ms = 4;
    update.snapshot.resume_receipts.push(ResumeReceipt {
        command: command.clone(),
        command_ref: record.reference().clone(),
        accepted_revision: update.snapshot.revision,
        expired: false,
        previous_segment_start_revision: 0,
        previous_outcome_ref: prior_outcome_ref,
        previous_last_event_seq: saved.snapshot.last_event_seq,
        actor_ref: id("reviewer"),
        capability_grant_ref: id("reviewer-grant"),
    });
    update.snapshot.last_event_seq += 1;
    update.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        3,
        RunEventPayload::RunResumed {
            command_ref: record.reference().clone(),
        },
    ));
    update.records.push(record);
    update.events[0].timestamp_ms = 4;
    let resumed = store.commit(&scope(), &id("run"), update).await.unwrap();
    store
        .commit(&scope(), &id("run"), finished(&resumed.snapshot, lease, 5))
        .await
        .unwrap();
    drop(store);
    let reopened = SqliteStateStore::open(database.path()).unwrap();
    assert_eq!(
        reopened
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .status,
        RunStatus::Succeeded
    );
    assert_eq!(
        reopened
            .read_events(&scope(), &id("run"), 0, 10)
            .await
            .unwrap()
            .events
            .len(),
        4
    );
}

#[tokio::test]
async fn independent_processes_deduplicate_admission_and_compete_for_an_active_session() {
    for duplicate in [true, false] {
        let database = Database::new();
        drop(SqliteStateStore::open(database.path()).unwrap());
        let mut first =
            workers::Worker::spawn(&database, "first", "admit", json!({"request":"request"}));
        let mut second = workers::Worker::spawn(
            &database,
            "second",
            "admit",
            json!({"request":if duplicate { "request" } else { "other-request" }}),
        );
        workers::wait(&first.ready);
        workers::wait(&second.ready);
        workers::signal(&database.file("gate"));
        let results = [first.finish(), second.finish()];
        assert_eq!(
            results
                .iter()
                .filter(|result| result["created"] == true)
                .count(),
            1
        );
        if duplicate {
            assert_eq!(results[0]["run"], results[1]["run"]);
            assert_eq!(
                results
                    .iter()
                    .filter(|result| result["created"] == false)
                    .count(),
                1
            );
        } else {
            assert_eq!(
                results
                    .iter()
                    .filter(|result| result["error"] == "SessionBusy")
                    .count(),
                1
            );
        }
    }
}

#[tokio::test]
async fn an_old_process_cannot_write_after_another_process_takes_over_its_lease() {
    let database = Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap();
    let mut first = workers::Worker::spawn(&database, "first", "lease_hold", json!({}));
    let mut second = workers::Worker::spawn(&database, "second", "lease_hold", json!({}));
    workers::wait(&first.ready);
    workers::wait(&second.ready);
    workers::signal(&database.file("gate"));
    workers::wait(&first.result);
    workers::wait(&second.result);
    let initial = [workers::read(&first.result), workers::read(&second.result)];
    assert_eq!(
        initial
            .iter()
            .filter(|result| result["error"] == "LeaseBusy")
            .count(),
        1
    );
    let winner = initial
        .iter()
        .position(|result| result["fence"].is_u64())
        .unwrap();
    let acquired = &initial[winner];
    let fence = acquired["fence"].as_u64().unwrap();
    let mut replacement = workers::Worker::spawn(
        &database,
        "replacement",
        "takeover",
        json!({"now":acquired["expires_at_ms"].as_i64().unwrap() + 1,"owner":if winner == 0 {"first"} else {"second"}}),
    );
    let result = replacement.finish();
    assert!(result["fence"].as_u64().unwrap() > fence);
    assert_eq!(result["revision"], 1);
    workers::signal(&database.file("resume"));
    let completed = [first.finish(), second.finish()];
    let rejected = completed
        .iter()
        .find(|result| result["stale_error"] == "LeaseLost")
        .unwrap();
    assert_eq!(rejected["observed_revision"], 1);
    assert_eq!(
        store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .revision,
        1
    );
}

#[tokio::test]
async fn committed_protected_state_recovers_after_process_exit_without_destructors() {
    let database = Database::new();
    let mut worker = workers::Worker::spawn(&database, "committer", "commit_exit", json!({}));
    let expected = worker.finish();
    let mut wal_path = database.path().into_os_string();
    wal_path.push("-wal");
    let wal = std::fs::metadata(std::path::PathBuf::from(wal_path))
        .expect("the exited worker must leave its committed WAL for recovery");
    assert!(wal.len() > 0, "the committed WAL must not be empty");
    let reopened = SqliteStateStore::open(database.path()).unwrap();
    let actual = reopened.load(&scope(), &id("run")).await.unwrap();
    assert_eq!(
        serde_json::to_value(&actual.snapshot).unwrap(),
        expected["snapshot"]
    );
    assert_eq!(
        serde_json::to_value(&actual.session).unwrap(),
        expected["session"]
    );
    assert_eq!(
        serde_json::to_value(&actual.messages).unwrap(),
        expected["messages"]
    );
    assert_eq!(actual.snapshot.usage.model_calls, 1);
    assert_eq!(actual.snapshot.usage.tool_attempts, 1);
    assert_eq!(actual.snapshot.reservations.len(), 2);
    assert_eq!(
        actual
            .snapshot
            .system_inputs
            .as_ref()
            .unwrap()
            .snapshot_ref
            .revision,
        7
    );
    assert!(
        actual.snapshot.model_ledger[0]
            .reported_model_version
            .is_none()
    );
    for record in expected["records"].as_array().unwrap() {
        let reference: RecordRef = serde_json::from_value(record["reference"].clone()).unwrap();
        assert_eq!(
            reopened
                .read_record(&scope(), &reference)
                .await
                .unwrap()
                .value(),
            &record["value"]
        );
    }
    let events = reopened
        .read_events(&scope(), &id("run"), 0, 100)
        .await
        .unwrap();
    assert_eq!(events.events.len(), 5);
    assert_eq!(events.last_available_seq, 5);
    assert!(actual.session.active_run_id.is_none());
}

#[tokio::test]
async fn a_busy_writer_times_out_and_killing_an_uncommitted_writer_rolls_back_its_image() {
    let database = Database::new();
    let store =
        SqliteStateStore::open_with_busy_timeout(database.path(), Duration::from_millis(20))
            .unwrap();
    let initial = store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap()
        .state;
    let lock = Connection::open(database.path()).unwrap();
    lock.execute_batch("BEGIN IMMEDIATE").unwrap();
    let started = Instant::now();
    assert_eq!(
        store
            .acquire_lease(&scope(), &id("run"), &id("owner"), 0, 100)
            .await
            .unwrap_err()
            .code,
        ErrorCode::PersistenceUnavailable
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "busy timeout was not bounded"
    );
    lock.execute_batch("ROLLBACK").unwrap();
    let mut worker = workers::Worker::spawn(&database, "uncommitted", "uncommitted", json!({}));
    workers::wait(&worker.ready);
    // A WAL reader sees the previous committed image while the writer is uncommitted.
    assert_eq!(store.load(&scope(), &id("run")).await.unwrap(), initial);
    worker.terminate();
    let reopened = SqliteStateStore::open(database.path()).unwrap();
    assert_eq!(reopened.load(&scope(), &id("run")).await.unwrap(), initial);
    assert_eq!(
        reopened
            .read_events(&scope(), &id("run"), 0, 10)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn time_waiting_for_a_write_lock_cannot_revive_a_lease() {
    for renew in [false, true] {
        let database = Database::new();
        let store = Arc::new(
            SqliteStateStore::open_with_busy_timeout(database.path(), Duration::from_secs(2))
                .unwrap(),
        );
        let saved = store
            .admit(
                &scope(),
                durable_admission("run", "request", "session").await,
            )
            .await
            .unwrap()
            .state;
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 40)
            .await
            .unwrap();
        let request_time = lease.expires_at_ms - 40;
        let connection = Connection::open(database.path()).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let task = {
            let store = store.clone();
            let entered = entered.clone();
            tokio::spawn(async move {
                entered.notify_one();
                if renew {
                    store
                        .renew_lease(&scope(), &id("run"), &lease, request_time, 10_000)
                        .await
                        .map(|_| ())
                } else {
                    store
                        .commit(
                            &scope(),
                            &id("run"),
                            prepared(&saved.snapshot, lease, request_time),
                        )
                        .await
                        .map(|_| ())
                }
            })
        };
        entered.notified().await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        connection.execute_batch("ROLLBACK").unwrap();
        assert_eq!(task.await.unwrap().unwrap_err().code, ErrorCode::LeaseLost);
        assert_eq!(
            store
                .load(&scope(), &id("run"))
                .await
                .unwrap()
                .snapshot
                .revision,
            0
        );
    }
}

fn rewrite_checkpoint(database: &Database, change: impl FnOnce(&mut Value)) {
    let connection = Connection::open(database.path()).unwrap();
    let encoded: String = connection
        .query_row(
            "SELECT checkpoint_json FROM wickle_scope_checkpoints",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut value: Value = serde_json::from_str(&encoded).unwrap();
    change(&mut value);
    let checksum = canonical_digest(&value);
    assert_eq!(
        connection
            .execute(
                "UPDATE wickle_scope_checkpoints SET checkpoint_json=?1,checksum=?2",
                rusqlite::params![value.to_string(), checksum.as_str()]
            )
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn unknown_database_versions_and_corrupted_scope_images_are_rejected() {
    // This is a user table, not one of SQLite's reserved sqlite_* internal objects.
    let foreign = Database::new();
    let connection = Connection::open(foreign.path()).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE sqliteuser(value INTEGER); INSERT INTO sqliteuser VALUES (42);",
        )
        .unwrap();
    assert!(SqliteStateStore::open(foreign.path()).is_err());
    assert_eq!(
        connection
            .query_row("SELECT value FROM sqliteuser", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        42
    );

    for (statement, expected) in [
        (
            "PRAGMA user_version=999",
            ErrorCode::UnsupportedSchemaVersion,
        ),
        ("PRAGMA application_id=1", ErrorCode::InvalidContract),
        (
            "UPDATE wickle_metadata SET schema_version=999",
            ErrorCode::UnsupportedSchemaVersion,
        ),
    ] {
        let database = Database::new();
        drop(SqliteStateStore::open(database.path()).unwrap());
        Connection::open(database.path())
            .unwrap()
            .execute_batch(statement)
            .unwrap();
        assert_eq!(
            SqliteStateStore::open(database.path()).unwrap_err().code,
            expected
        );
    }
    let changed_schema = Database::new();
    let store = SqliteStateStore::open(changed_schema.path()).unwrap();
    store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap();
    drop(store);
    Connection::open(changed_schema.path())
        .unwrap()
        .execute_batch(
            "BEGIN IMMEDIATE;
         ALTER TABLE wickle_scope_checkpoints RENAME TO old_scope_checkpoints;
         CREATE TABLE wickle_scope_checkpoints (
             scope_key TEXT NOT NULL, checkpoint_json TEXT NOT NULL, checksum TEXT NOT NULL
         ) STRICT;
         INSERT INTO wickle_scope_checkpoints SELECT * FROM old_scope_checkpoints;
         DROP TABLE old_scope_checkpoints;
         COMMIT;",
        )
        .unwrap();
    assert!(
        SqliteStateStore::open(changed_schema.path()).is_err(),
        "a schema without its uniqueness constraint was accepted"
    );
    for fault in [
        "version",
        "scope",
        "duplicate_run",
        "duplicate_record",
        "duplicate_message",
        "duplicate_event",
        "record_body",
        "missing_record",
        "active_slot",
        "fence",
        "checksum",
    ] {
        let database = Database::new();
        let store = SqliteStateStore::open(database.path()).unwrap();
        store
            .admit(
                &scope(),
                durable_admission("run", "request", "session").await,
            )
            .await
            .unwrap();
        store
            .acquire_lease(&scope(), &id("run"), &id("owner"), 0, 10_000)
            .await
            .unwrap();
        if fault == "duplicate_event" {
            store
                .admit(
                    &scope(),
                    durable_admission("other", "other-request", "other-session").await,
                )
                .await
                .unwrap();
        }
        drop(store);
        rewrite_checkpoint(&database, |image| match fault {
            "version" => image["schema_version"] = json!("wickle.state-store.v999"),
            "scope" => image["scope"]["tenant_id"] = json!("foreign"),
            "duplicate_run" => {
                let copy = image["runs"][0].clone();
                image["runs"].as_array_mut().unwrap().push(copy);
            }
            "duplicate_record" => {
                let copy = image["records"][0].clone();
                image["records"].as_array_mut().unwrap().push(copy);
            }
            "duplicate_message" => {
                let mut copy = image["sessions"][0]["messages"][0].clone();
                copy["sequence"] = json!(2);
                image["sessions"][0]["messages"]
                    .as_array_mut()
                    .unwrap()
                    .push(copy);
                image["sessions"][0]["snapshot"]["transcript_revision"] = json!(2);
            }
            "duplicate_event" => {
                image["runs"][1]["events"][0]["event_id"] =
                    image["runs"][0]["events"][0]["event_id"].clone()
            }
            "record_body" => {
                let prompt = image["records"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|record| record["reference"]["record_id"] == "prompt-session")
                    .unwrap();
                prompt["value"]["instructions"] = json!("Changed instructions");
            }
            "missing_record" => image["records"]
                .as_array_mut()
                .unwrap()
                .retain(|record| record["reference"]["record_id"] != "prompt-session"),
            "active_slot" => {
                image["sessions"][0]["snapshot"]
                    .as_object_mut()
                    .unwrap()
                    .remove("active_run_id");
            }
            "fence" => image["runs"][0]["last_fencing_token"] = json!(0),
            "checksum" => {}
            _ => unreachable!(),
        });
        if fault == "checksum" {
            let wrong = canonical_digest(&json!("different image"));
            Connection::open(database.path())
                .unwrap()
                .execute(
                    "UPDATE wickle_scope_checkpoints SET checksum=?1",
                    [wrong.as_str()],
                )
                .unwrap();
        }
        let error = match SqliteStateStore::open(database.path()) {
            Err(error) => error,
            Ok(store) => store
                .load(&scope(), &id("run"))
                .await
                .expect_err("corrupt checkpoint was accepted"),
        };
        if fault == "version" {
            assert_eq!(error.code, ErrorCode::UnsupportedSchemaVersion);
        }
    }
}

#[tokio::test]
async fn an_open_handle_does_not_silently_replace_a_deleted_or_different_database() {
    let database = Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap();
    std::fs::remove_file(database.path()).unwrap();
    assert!(store.load(&scope(), &id("run")).await.is_err());
    assert!(
        !database.path().exists(),
        "a read recreated the deleted database"
    );
    let replacement = SqliteStateStore::open(database.path()).unwrap();
    replacement
        .admit(
            &scope(),
            durable_admission("replacement", "replacement", "session").await,
        )
        .await
        .unwrap();
    assert!(
        store.load(&scope(), &id("replacement")).await.is_err(),
        "old handle accepted a different store identity"
    );
    assert!(replacement.load(&scope(), &id("replacement")).await.is_ok());
}

#[test]
#[ignore = "Subprocess fixture; only parent process tests supply its required input"]
fn process_worker() {
    workers::run();
}

#[tokio::test]
async fn warmed_checkpoints_observe_external_updates_and_revalidate_changed_bytes() {
    let database = Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    let initial = store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap()
        .state;
    let cloned = store.clone();
    assert_eq!(cloned.load(&scope(), &id("run")).await.unwrap(), initial);
    let other = SqliteStateStore::open(database.path()).unwrap();
    other
        .admit(
            &scope(),
            durable_admission("other", "request-2", "session-2").await,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .find_request(&scope(), &id("session-2"), &id("request-2"))
            .await
            .unwrap()
            .unwrap()
            .snapshot
            .run_id,
        id("other")
    );
    let connection = Connection::open(database.path()).unwrap();
    let (original, checksum): (String, String) = connection
        .query_row(
            "SELECT checkpoint_json,checksum FROM wickle_scope_checkpoints",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let mut corrupted: Value = serde_json::from_str(&original).unwrap();
    corrupted["schema_version"] = json!("unknown-checkpoint-format");
    // Even a correctly recomputed outer digest cannot skip full validation.
    connection
        .execute(
            "UPDATE wickle_scope_checkpoints SET checkpoint_json=?1,checksum=?2",
            rusqlite::params![corrupted.to_string(), canonical_digest(&corrupted).as_str()],
        )
        .unwrap();
    assert!(store.load(&scope(), &id("run")).await.is_err());
    assert!(cloned.load(&scope(), &id("run")).await.is_err());
    connection
        .execute(
            "UPDATE wickle_scope_checkpoints SET checkpoint_json=?1,checksum=?2",
            rusqlite::params![original, checksum],
        )
        .unwrap();
    assert_eq!(store.load(&scope(), &id("run")).await.unwrap(), initial);
    let mut foreign = scope();
    foreign.workspace_id = id("foreign");
    assert!(store.load(&foreign, &id("run")).await.is_err());
}

#[tokio::test]
async fn a_failed_sql_write_never_publishes_mutated_cached_lease_state() {
    let database = Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    let initial = store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap()
        .state;
    assert_eq!(store.load(&scope(), &id("run")).await.unwrap(), initial);
    let connection = Connection::open(database.path()).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_write BEFORE UPDATE ON wickle_scope_checkpoints BEGIN SELECT RAISE(ABORT,'injected commit failure'); END;").unwrap();
    assert!(
        store
            .acquire_lease(&scope(), &id("run"), &id("failed-owner"), 0, 10_000)
            .await
            .is_err()
    );
    connection
        .execute_batch("DROP TRIGGER reject_write;")
        .unwrap();
    // An uncommitted lease must not survive under the previous row's cache key.
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("next-owner"), 1, 10_000)
        .await
        .unwrap();
    store
        .check_lease(&scope(), &id("run"), &lease, 2)
        .await
        .unwrap();
    assert_eq!(store.load(&scope(), &id("run")).await.unwrap(), initial);
}

#[tokio::test]
async fn killing_an_uncommitted_legacy_upgrade_preserves_old_data_and_allows_one_later_upgrade() {
    let database = Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    let initial = store
        .admit(
            &scope(),
            core::admission("legacy", "legacy-request", "legacy-session", "old", "1").await,
        )
        .await
        .unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("legacy"), &id("owner"), 0, 1000)
        .await
        .unwrap();
    let terminal = store
        .commit(
            &scope(),
            &id("legacy"),
            finished(&initial.state.snapshot, lease, 1),
        )
        .await
        .unwrap();
    drop(store);
    let connection = Connection::open(database.path()).unwrap();
    let text: String = connection
        .query_row(
            "SELECT checkpoint_json FROM wickle_scope_checkpoints",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut legacy: Value = serde_json::from_str(&text).unwrap();
    legacy["schema_version"] = json!("wickle.state-store.v1");
    legacy.as_object_mut().unwrap().remove("executions");
    connection
        .execute(
            "UPDATE wickle_scope_checkpoints SET checkpoint_json=?1,checksum=?2",
            rusqlite::params![legacy.to_string(), canonical_digest(&legacy).as_str()],
        )
        .unwrap();
    // The real shared state engine builds the upgrade image. The subprocess
    // pauses its SQLite image write before COMMIT, without a production test hook.
    let memory = MemoryStateStore::from_checkpoint(
        StateStoreCheckpoint::from_json(&legacy.to_string(), &scope(), &canonical_digest(&legacy))
            .unwrap(),
    );
    let input = core::admission("new", "new-request", "new-session", "new", "1").await;
    memory.admit(&scope(), input.clone()).await.unwrap();
    let upgraded = serde_json::to_value(memory.export_checkpoint(&scope()).unwrap()).unwrap();
    assert_eq!(upgraded["schema_version"], "wickle.state-store.v2");
    let old_reader = SqliteStateStore::open(database.path()).unwrap();
    let mut worker = workers::Worker::spawn(
        &database,
        "upgrade",
        "uncommitted-upgrade",
        json!({"image":upgraded}),
    );
    workers::wait(&worker.ready);
    assert_eq!(
        old_reader.load(&scope(), &id("legacy")).await.unwrap(),
        terminal
    );
    worker.terminate();
    drop(old_reader);
    let after_kill: String = connection
        .query_row(
            "SELECT checkpoint_json FROM wickle_scope_checkpoints",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(after_kill, legacy.to_string());
    let reopened = SqliteStateStore::open(database.path()).unwrap();
    let accepted = reopened.admit(&scope(), input.clone()).await.unwrap();
    assert!(accepted.created);
    let replay = reopened.admit(&scope(), input).await.unwrap();
    assert!(!replay.created);
    assert_eq!(accepted.state, replay.state);
    assert_eq!(
        reopened.load(&scope(), &id("legacy")).await.unwrap(),
        terminal
    );
    let after_commit: String = connection
        .query_row(
            "SELECT checkpoint_json FROM wickle_scope_checkpoints",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&after_commit).unwrap(),
        upgraded
    );
}
```

## `crates/wickle-state-sqlite/tests/support/command_recovery.rs`

```rust
//! Actual kill/restart at saved wait and atomic input-command/segment boundaries.
use super::*;
use serde_json::json;
use std::sync::atomic::Ordering;
struct LoggedTool {
    inner: Arc<dyn ToolExecutor>,
    name: &'static str,
    directory: std::path::PathBuf,
}
impl ToolExecutor for LoggedTool {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            append(&self.directory.join("tool-calls"), self.name);
            self.inner.execute(args, context).await
        })
    }
}
pub fn run_worker(directory: &Path, mode: &str) {
    let interrupt = mode.ends_with("-interrupt");
    let boundary = mode
        .strip_prefix("command-")
        .unwrap()
        .rsplit_once('-')
        .unwrap()
        .0;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut fixture = tool_support::Fixture::new(tool_support::Mode::Input);
            fixture.profile.limits.max_elapsed_ms = 60000.try_into().unwrap();
            fixture.profile.limits.max_recovery_attempts = 4;
            let store = Arc::new(SqliteStateStore::open(directory.join("state.sqlite")).unwrap());
            let mut bindings = fixture.bindings();
            bindings.state = if interrupt {
                Arc::new(recovery_store::CrashStore {
                    inner: store.clone(),
                    boundary: boundary.into(),
                    kill_marker: Some(directory.join("kill-ready")),
                })
            } else {
                store.clone()
            };
            bindings.clock = Arc::new(ProcessClock::new(!interrupt));
            bindings.ids = Arc::new(RandomIdSource);
            let registrations = ["before", "target", "after"]
                .into_iter()
                .enumerate()
                .map(|(index, name)| ToolRegistration {
                    compiled: fixture.registry.get(&id(name)).unwrap().compiled.clone(),
                    executor: Arc::new(LoggedTool {
                        inner: fixture.tools[index].clone(),
                        name,
                        directory: directory.to_owned(),
                    }),
                })
                .collect();
            bindings.tools = Some(Arc::new(ToolRegistry::new(scope(), registrations).unwrap()));
            if !interrupt {
                fixture.model.calls.store(
                    std::fs::read_to_string(directory.join("calls"))
                        .unwrap()
                        .lines()
                        .count(),
                    Ordering::SeqCst,
                );
            }
            bindings.model_exchange = Arc::new(
                ModelExchange::new(
                    Arc::new(ProcessModel {
                        inner: fixture.model.clone(),
                        directory: directory.to_owned(),
                        interrupt: false,
                    }),
                    bindings.policy.clone(),
                )
                .with_route_inspector(fixture.base.inspector.clone(), Duration::from_secs(1))
                .unwrap(),
            );
            let agent = create_agent(fixture.profile.clone(), bindings).unwrap();
            if interrupt {
                let handle = fixture.started(&agent).await;
                let outcome = completed(handle.outcome(&context()).await.unwrap());
                assert_eq!(outcome.result.status(), RunStatus::Waiting);
            }
            let saved = store
                .find_request(&scope(), &id("session"), &id("request"))
                .await
                .unwrap()
                .unwrap();
            let run_id = saved.snapshot.run_id.clone();
            let before = store.read_execution(&scope(), &run_id).await.unwrap();
            let old_wait = before.segments[0].outcome.clone();
            let command_path = directory.join("answer.json");
            let answer: ResumeCommand = match std::fs::read_to_string(&command_path) {
                Ok(text) => serde_json::from_str(&text).unwrap(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let command = ResumeCommand {
                        run_id: run_id.clone(),
                        expected_revision: saved.snapshot.revision,
                        command_id: id("answer"),
                        action: ResumeAction::Input {
                            wait_id: saved.snapshot.wait.as_ref().unwrap().wait_id.clone(),
                            answer: json!({"selection":"annual"}),
                        },
                    };
                    std::fs::write(&command_path, serde_json::to_vec(&command).unwrap()).unwrap();
                    command
                }
                Err(error) => panic!("saved command: {error}"),
            };
            if !interrupt {
                assert_eq!(
                    before.accepted_commands.len(),
                    usize::from(boundary == "accepted-command")
                );
                assert_eq!(
                    before.segments.len(),
                    if boundary == "accepted-command" { 2 } else { 1 }
                );
            }
            let handle = if !interrupt && boundary == "accepted-command" {
                let source = saved
                    .snapshot
                    .recovery_record(id("command-recovery-source"))
                    .unwrap();
                completed(
                    agent
                        .resume(
                            ResumeCommand {
                                run_id: run_id.clone(),
                                expected_revision: saved.snapshot.revision,
                                command_id: id("recover-answer"),
                                action: ResumeAction::Recover {
                                    recovery_ref: source.reference().clone(),
                                },
                            },
                            context(),
                        )
                        .await
                        .unwrap(),
                )
            } else {
                completed(agent.resume(answer.clone(), context()).await.unwrap())
            };
            let outcome = completed(handle.outcome(&context()).await.unwrap());
            if interrupt {
                panic!("command boundary failed to pause");
            }
            assert_eq!(
                outcome.result.status(),
                RunStatus::Succeeded,
                "{:?}",
                outcome
            );
            let finished = store.load(&scope(), &run_id).await.unwrap();
            let target = &finished.snapshot.tool_ledger[1];
            let ToolCallState::Settled { result } = &target.state else {
                panic!("input not settled")
            };
            assert_eq!(
                result.content,
                vec![InputContent::Json {
                    value: json!({"selection":"annual"})
                }]
            );
            let history = store.read_execution(&scope(), &run_id).await.unwrap();
            assert_eq!(history.segments[0].outcome, old_wait);
            assert_eq!(
                finished
                    .snapshot
                    .resume_receipts
                    .iter()
                    .filter(|receipt| receipt.command.command_id == id("answer"))
                    .count(),
                1
            );
            let checkpoint = store.load(&scope(), &run_id).await.unwrap();
            let duplicate = completed(agent.resume(answer, context()).await.unwrap());
            let _old_interval = completed(duplicate.outcome(&context()).await.unwrap());
            assert_eq!(store.load(&scope(), &run_id).await.unwrap(), checkpoint);
            std::fs::write(
                directory.join("verified"),
                b"input command atomically consumed without reexecution",
            )
            .unwrap();
        });
}
```

## `crates/wickle-state-sqlite/tests/support/context_recovery.rs`

```rust
//! Real compression and SQLite checkpoints on both sides of an abrupt process exit.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Model(AtomicUsize);
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
        _: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let index = self.0.fetch_add(1, Ordering::SeqCst);
        let (event, finish) = if index < 2 {
            (
                ModelEvent::ToolArgumentsDelta {
                    index: 0,
                    provider_call_id: Some(format!("read-{index}")),
                    name: Some("before".into()),
                    delta: "{\"query\":\"chunk\"}".into(),
                },
                ModelFinish::ToolCalls,
            )
        } else {
            (
                ModelEvent::TextDelta {
                    text: "The saved records were read.".into(),
                },
                ModelFinish::Stop,
            )
        };
        Box::pin(futures_util::stream::iter([
            Ok(event),
            Ok(ModelEvent::ResponseCompleted {
                finish,
                metadata: Default::default(),
                continuation: vec![],
            }),
        ]))
    }
}
struct Read(std::path::PathBuf);
impl ToolExecutor for Read {
    fn execute<'a>(
        &'a self,
        _: &'a JsonObject,
        _: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            append(&self.0.join("reads"), "read");
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: serde_json::json!("x".repeat(3500)),
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}
struct Summary(std::path::PathBuf);
impl HostContextCompactor for Summary {
    fn compact<'a>(
        &'a self,
        request: &'a CompactionRequest,
        _: &'a ContextStrategyContext,
    ) -> PortFuture<'a, String> {
        Box::pin(async move {
            assert!(!request.segments.is_empty());
            append(&self.0.join("summaries"), "summary");
            Ok("Older complete records were read; their observations remain in storage.".into())
        })
    }
}

pub fn run_worker(directory: &Path, mode: &str) {
    let interrupt = mode.ends_with("-interrupt");
    let committed = !mode.starts_with("context-before-");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut fixture = tool_support::Fixture::new(tool_support::Mode::External);
            fixture.profile.limits.max_elapsed_ms = 60000.try_into().unwrap();
            fixture.profile.limits.max_recovery_attempts = 4;
            let store = Arc::new(SqliteStateStore::open(directory.join("state.sqlite")).unwrap());
            let mut bindings = fixture.bindings();
            bindings.state = if interrupt {
                Arc::new(recovery_store::CrashStore {
                    kill_marker: None,
                    inner: store.clone(),
                    boundary: if committed {
                        "context"
                    } else {
                        "before-context"
                    }
                    .into(),
                })
            } else {
                store.clone()
            };
            bindings.clock = Arc::new(ProcessClock::new(!interrupt));
            bindings.ids = Arc::new(RandomIdSource);
            bindings.settings.projection_limits.max_bytes = 8000;
            if !interrupt {
                bindings.settings.lease_ttl_ms = 30000;
                bindings.settings.heartbeat_interval_ms = 5000;
            }
            let mut descriptor = fixture
                .registry
                .get(&id("before"))
                .unwrap()
                .compiled
                .descriptor()
                .clone();
            descriptor.max_output_bytes = 65536.try_into().unwrap();
            bindings.tools = Some(Arc::new(
                ToolRegistry::new(
                    scope(),
                    vec![
                        ToolRegistration {
                            compiled: SchemaCompiler::new()
                                .compile(descriptor, &fixture.inputs)
                                .unwrap(),
                            executor: Arc::new(Read(directory.to_owned())),
                        },
                        fixture.registry.get(&id("target")).unwrap().clone(),
                        fixture.registry.get(&id("after")).unwrap().clone(),
                    ],
                )
                .unwrap(),
            ));
            bindings.context_runtime = Some(Arc::new(
                ContextRuntime::new(
                    scope(),
                    Arc::new(BoundedContextStrategy),
                    Some(ContextCompactor::Host {
                        definition: reference("summary"),
                        compressor: Arc::new(Summary(directory.to_owned())),
                    }),
                    ContextRewriteLimits::default(),
                )
                .unwrap(),
            ));
            let count = if interrupt {
                0
            } else {
                std::fs::read_to_string(directory.join("calls"))
                    .unwrap()
                    .lines()
                    .count()
            };
            bindings.model_exchange = Arc::new(
                ModelExchange::new(
                    Arc::new(ProcessModel {
                        inner: Arc::new(Model(AtomicUsize::new(count))),
                        directory: directory.to_owned(),
                        interrupt: false,
                    }),
                    bindings.policy.clone(),
                )
                .with_route_inspector(fixture.base.inspector.clone(), Duration::from_secs(1))
                .unwrap(),
            );
            let agent = create_agent(fixture.profile.clone(), bindings).unwrap();
            if interrupt {
                let handle = fixture.started(&agent).await;
                let result = handle.outcome(&context()).await;
                panic!("compression did not terminate process: {result:?}");
            }
            let run_id = id(&std::fs::read_to_string(directory.join("run")).unwrap());
            let before = store.load(&scope(), &run_id).await.unwrap();
            assert_eq!(before.snapshot.status, RunStatus::Running);
            assert_eq!(before.snapshot.context_revision_ref.is_some(), committed);
            assert_eq!(before.snapshot.context_decisions.is_empty(), !committed);
            assert_eq!(before.snapshot.usage.model_calls, 2);
            assert_eq!(before.snapshot.usage.tool_attempts, 2);
            let previous_revision = if let Some(reference) = &before.snapshot.context_revision_ref {
                Some(store.read_record(&scope(), reference).await.unwrap())
            } else {
                None
            };
            let source = before
                .snapshot
                .recovery_record(id("context-source"))
                .unwrap();
            let handle = completed(
                agent
                    .resume(
                        ResumeCommand {
                            run_id: run_id.clone(),
                            expected_revision: before.snapshot.revision,
                            command_id: id("recover-context"),
                            action: ResumeAction::Recover {
                                recovery_ref: source.reference().clone(),
                            },
                        },
                        context(),
                    )
                    .await
                    .unwrap(),
            );
            assert_eq!(
                completed(handle.outcome(&context()).await.unwrap())
                    .result
                    .status(),
                RunStatus::Succeeded
            );
            let after = store.load(&scope(), &run_id).await.unwrap();
            assert_eq!(after.snapshot.profile, before.snapshot.profile);
            assert_eq!(after.snapshot.system_inputs, before.snapshot.system_inputs);
            assert_eq!(after.snapshot.limits, before.snapshot.limits);
            assert_eq!(after.snapshot.usage.model_calls, 3);
            assert_eq!(after.snapshot.usage.tool_attempts, 2);
            assert_eq!(
                after.snapshot.usage.recovery_attempts,
                before.snapshot.usage.recovery_attempts + 1
            );
            for message in &before.messages {
                assert!(after.messages.contains(message));
            }
            for reservation in &before.snapshot.reservations {
                assert!(after.snapshot.reservations.contains(reservation));
            }
            let reference = after
                .snapshot
                .context_revision_ref
                .as_ref()
                .expect("a complete compressed revision is adopted");
            let revision = store.read_record(&scope(), reference).await.unwrap();
            if let Some(previous) = previous_revision {
                assert_eq!(
                    before.snapshot.context_revision_ref.as_ref(),
                    Some(reference)
                );
                assert_eq!(revision, previous);
                assert_eq!(
                    after.snapshot.context_decisions,
                    before.snapshot.context_decisions
                );
            }
            assert!(!after.snapshot.context_decisions.is_empty());
            std::fs::write(directory.join("verified"), b"complete context restored").unwrap();
        });
}
```

## `crates/wickle-state-sqlite/tests/support/recovery_store.rs`

```rust
//! Process termination at durable state boundaries, without running destructors.
use std::sync::Arc;
use wickle::*;
use wickle_state_sqlite::SqliteStateStore;
pub struct CrashStore {
    pub inner: Arc<SqliteStateStore>,
    pub boundary: String,
    pub kill_marker: Option<std::path::PathBuf>,
}
impl CrashStore {
    async fn stop(&self, code: i32) -> ! {
        if let Some(marker) = &self.kill_marker {
            std::fs::write(marker, b"ready\n").unwrap();
            std::future::pending::<()>().await;
        }
        std::process::exit(code)
    }
}
impl StateStore for CrashStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
        request_id: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        self.inner.find_request(scope, session_id, request_id)
    }
    fn admit<'a>(
        &'a self,
        scope: &'a Scope,
        input: AdmissionInput,
    ) -> PortFuture<'a, AdmissionResult> {
        Box::pin(async move {
            let result = self.inner.admit(scope, input).await?;
            if self.boundary == "admitted" {
                self.stop(75).await;
            }
            Ok(result)
        })
    }
    fn load<'a>(&'a self, scope: &'a Scope, run_id: &'a Id) -> PortFuture<'a, StoredRun> {
        self.inner.load(scope, run_id)
    }
    fn load_session<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
    ) -> PortFuture<'a, SessionSnapshot> {
        self.inner.load_session(scope, session_id)
    }
    fn check_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.check_lease(scope, run_id, lease, now_ms)
    }
    fn acquire_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        owner: &'a Id,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner
            .acquire_lease(scope, run_id, owner, now_ms, ttl_ms)
    }
    fn renew_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.renew_lease(scope, run_id, lease, now_ms, ttl_ms)
    }
    fn release_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, ()> {
        self.inner.release_lease(scope, run_id, lease, now_ms)
    }
    fn commit<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            let prepared =
                !input.snapshot.prepared_steps.is_empty() && input.snapshot.model_ledger.is_empty();
            if self.boundary == "before-prepared" && prepared {
                self.stop(75).await;
            }
            let planned = input
                .events
                .iter()
                .any(|event| matches!(event.payload, RunEventPayload::ToolPlanned { .. }));
            if self.boundary == "before-plan" && planned {
                std::process::exit(75);
            }
            let rewritten = input
                .events
                .iter()
                .any(|event| matches!(event.payload, RunEventPayload::ContextRewritten { .. }));
            if self.boundary == "before-context" && rewritten {
                std::process::exit(76);
            }
            let stop = match self.boundary.as_str() {
                "wait" => input.snapshot.status == RunStatus::Waiting,
                "prepared" => prepared,
                "reserved" => input
                    .snapshot
                    .model_ledger
                    .iter()
                    .any(|entry| matches!(entry.state, ModelAttemptState::Reserved {})),
                "context" => rewritten,
                "plan" => planned,
                "bound" => input.snapshot.tool_ledger.iter().any(|entry| {
                    entry.call.tool_name.as_str() == "target"
                        && entry.call.bound_input_ref.is_some()
                        && matches!(entry.state, ToolCallState::Planned {})
                }),
                "settled" => input.snapshot.tool_ledger.iter().any(|entry| {
                    entry.call.tool_name.as_str() == "target"
                        && matches!(entry.state, ToolCallState::Settled { .. })
                }),
                "terminal" => input.snapshot.status.is_terminal(),
                _ => false,
            };
            let result = self.inner.commit(scope, run_id, input).await?;
            if stop {
                self.stop(if self.boundary == "context" { 76 } else { 75 })
                    .await;
            }
            Ok(result)
        })
    }
    fn read_events<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        after_seq: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage> {
        self.inner.read_events(scope, run_id, after_seq, limit)
    }
    fn read_record<'a>(
        &'a self,
        scope: &'a Scope,
        reference: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        self.inner.read_record(scope, reference)
    }
    fn record_hook_observation<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        report: HookObservation,
    ) -> PortFuture<'a, ()> {
        self.inner.record_hook_observation(scope, run_id, report)
    }
    fn read_hook_observations<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
    ) -> PortFuture<'a, Vec<HookObservation>> {
        self.inner.read_hook_observations(scope, run_id)
    }
}

impl wickle::ExecutionTransactions for CrashStore {
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
        Box::pin(async move {
            let answer = matches!(&request.start, SegmentStart::Resume(command) if matches!(command.action, ResumeAction::Input { .. }));
            if answer && self.boundary == "before-command" {
                self.stop(75).await;
            }
            let result = self.inner.begin_segment(scope, request).await?;
            if answer && self.boundary == "accepted-command" {
                self.stop(75).await;
            }
            Ok(result)
        })
    }
}
```

## `crates/wickle-state-sqlite/tests/support/workers.rs`

```rust
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use serde_json::{Value, json};
use wickle::*;
use wickle_state_sqlite::SqliteStateStore;

use crate::{
    core::{id, prepared, scope},
    support::{Database, durable_admission, populate_protected_run},
};

pub struct Worker {
    child: Child,
    pub result: PathBuf,
    pub ready: PathBuf,
}
impl Worker {
    pub fn spawn(database: &Database, actor: &str, mode: &str, details: Value) -> Self {
        let result = database.file(&format!("{actor}.result"));
        let ready = database.file(&format!("{actor}.ready"));
        let config = json!({
            "database":database.path(), "result":result, "ready":ready,
            "gate":database.file("gate"), "resume":database.file("resume"),
            "mode":mode, "actor":actor, "details":details,
        });
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "process_worker", "--nocapture"])
            .env("WICKLE_SQLITE_TEST_WORKER", config.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        Self {
            child,
            result,
            ready,
        }
    }
    pub fn finish(&mut self) -> Value {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success(), "SQLite subprocess failed");
                return read(&self.result);
            }
            assert!(
                Instant::now() < deadline,
                "SQLite subprocess did not finish"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    pub fn terminate(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn wait(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "SQLite subprocess barrier timed out"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}
pub fn signal(path: &Path) {
    std::fs::write(path, b"ready").unwrap();
}
pub fn read(path: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}
fn write(path: &Path, value: Value) {
    let temporary = path.with_extension("writing");
    std::fs::write(&temporary, serde_json::to_vec(&value).unwrap()).unwrap();
    std::fs::rename(temporary, path).unwrap();
}

pub fn run() {
    let encoded = std::env::var("WICKLE_SQLITE_TEST_WORKER")
        .expect("This ignored fixture requires an explicit parent test command");
    let config: Value = serde_json::from_str(&encoded).unwrap();
    let database = Path::new(config["database"].as_str().unwrap());
    let result = Path::new(config["result"].as_str().unwrap());
    let ready = Path::new(config["ready"].as_str().unwrap());
    let actor = config["actor"].as_str().unwrap();
    let mode = config["mode"].as_str().unwrap();
    if mode == "uncommitted-upgrade" {
        let connection = rusqlite::Connection::open(database).unwrap();
        let image = &config["details"]["image"];
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        connection
            .execute(
                "UPDATE wickle_scope_checkpoints SET checkpoint_json=?1,checksum=?2",
                rusqlite::params![image.to_string(), canonical_digest(image).as_str()],
            )
            .unwrap();
        signal(ready);
        loop {
            std::thread::park();
        }
    }
    if mode == "uncommitted" {
        let connection = rusqlite::Connection::open(database).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE; UPDATE wickle_scope_checkpoints SET checkpoint_json='uncommitted invalid image', checksum='uncommitted';").unwrap();
        signal(ready);
        loop {
            std::thread::park();
        }
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let store = SqliteStateStore::open(database).unwrap();
        signal(ready);
        if matches!(mode, "admit" | "lease_hold") {
            wait(Path::new(config["gate"].as_str().unwrap()));
        }
        match mode {
            "admit" => {
                let request = config["details"]["request"].as_str().unwrap();
                let receipt = store.admit(&scope(), durable_admission(actor, request, "session").await).await;
                write(result, match receipt {
                    Ok(receipt) => json!({"created":receipt.created,"run":receipt.state.snapshot.run_id}),
                    Err(error) => json!({"error":format!("{:?}",error.code)}),
                });
            }
            "lease_hold" => {
                match store.acquire_lease(&scope(), &id("run"), &id(actor), 100, 10_000).await {
                    Ok(lease) => {
                        write(result, json!({"fence":lease.fencing_token,"expires_at_ms":lease.expires_at_ms}));
                        wait(Path::new(config["resume"].as_str().unwrap()));
                        let latest = store.load(&scope(), &id("run")).await.unwrap();
                        let error = store.commit(&scope(), &id("run"), prepared(&latest.snapshot, lease.clone(), lease.expires_at_ms + 10)).await.unwrap_err();
                        write(result, json!({"fence":lease.fencing_token,"stale_error":format!("{:?}",error.code),"observed_revision":latest.snapshot.revision}));
                    }
                    Err(error) => write(result, json!({"error":format!("{:?}",error.code)})),
                }
            }
            "takeover" => {
                let now = config["details"]["now"].as_i64().unwrap();
                let owner = config["details"]["owner"].as_str().unwrap();
                let lease = store.acquire_lease(&scope(), &id("run"), &id(owner), now, 10_000).await.unwrap();
                let saved = store.load(&scope(), &id("run")).await.unwrap();
                let committed = store.commit(&scope(), &id("run"), prepared(&saved.snapshot, lease.clone(), now + 1)).await.unwrap();
                write(result, json!({"fence":lease.fencing_token,"revision":committed.snapshot.revision}));
            }
            "commit_exit" => {
                let retained_connection = rusqlite::Connection::open(database).unwrap();
                let _: i64 = retained_connection.query_row(
                    "SELECT count(*) FROM wickle_scope_checkpoints", [], |row| row.get(0)
                ).unwrap();
                let retained_store = std::sync::Arc::new(store);
                let saved = populate_protected_run(retained_store.clone()).await;
                write(result, saved);
                // Keep an initialized WAL connection alive so operation-level closes
                // cannot perform last-connection cleanup before this abrupt exit.
                std::process::exit(0);
            }
            _ => panic!("Unknown SQLite subprocess fixture mode"),
        }
    });
}
```

## `crates/wickle/src/agent.rs`

```rust
use crate::*;
use futures_util::{FutureExt, stream};
use std::{
    collections::BTreeMap,
    fmt,
    num::NonZeroU64,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

mod admission;
mod artifacts;
mod components;
mod control;
mod driver;
mod hooks;
mod inspection;
mod interruption;
mod persistence;
pub use persistence::{PersistenceFailure, UnconfirmedToolEffect};
mod recovery;
mod resume;
mod segment;
mod sources;
mod tools;
mod verification;
use components::SegmentBindings;

/// Host tokenizer or conservative estimator. This synchronous callback must not
/// perform I/O; returned tokens are estimates, not provider-reported usage.
pub trait ModelTokenEstimator: Send + Sync {
    /// Estimate the complete prepared request for its exact route.
    fn estimate(&self, request: &ModelRequest) -> Result<u64, ContractError>;
}

/// Finite runtime bounds, independent of the profile's total execution budgets.
#[derive(Debug, Clone)]
pub struct AgentSettings {
    /// Upper bound for cooperative interruption callbacks; default one second.
    pub interruption_timeout_ms: u64,
    /// Separate bounded cleanup window; default five seconds, not new work time.
    pub cleanup_timeout_ms: u64,
    /// Lease duration renewed by the detached driver.
    pub lease_ttl_ms: u64,
    /// Renewal interval; at most one third of the lease duration.
    pub heartbeat_interval_ms: u64,
    /// Maximum delay between durable observer polls.
    pub observer_poll_ms: u64,
    /// Maximum events read per page.
    pub event_page_size: usize,
    /// Deadline for admission preparation callbacks, before durable admission.
    pub start_timeout_ms: u64,
    /// Maximum serialized RunRequest bytes.
    pub max_request_bytes: usize,
    /// Reserved output-token limit for the initial text-model call.
    pub max_output_tokens: NonZeroU64,
    /// Model request and response bounds.
    pub response_limits: ModelResponseLimits,
    /// Context byte/item bounds, distinct from token estimates.
    pub projection_limits: ProjectionLimits,
    /// Per-tool callback and receipt limits; total attempts still use RunLimits.
    pub tool_execution_limits: ToolExecutionLimits,
    /// Require a durable StateStore at admission.
    pub require_durable: bool,
}
impl Default for AgentSettings {
    fn default() -> Self {
        Self {
            interruption_timeout_ms: 1000,
            cleanup_timeout_ms: 5000,
            lease_ttl_ms: 30_000,
            heartbeat_interval_ms: 5_000,
            observer_poll_ms: 100,
            event_page_size: 64,
            start_timeout_ms: 30_000,
            max_request_bytes: 1_048_576,
            max_output_tokens: NonZeroU64::new(1024).expect("positive default"),
            response_limits: ModelResponseLimits {
                max_input_bytes: 1_048_576,
                max_response_bytes: 262_144,
                max_delta_bytes: 65_536,
                max_events: 4096,
                max_tool_calls: 16,
            },
            projection_limits: ProjectionLimits {
                max_bytes: 1_048_576,
                max_items: 1024,
            },
            tool_execution_limits: ToolExecutionLimits::default(),
            require_durable: false,
        }
    }
}
impl AgentSettings {
    /// Validate finite bounds without calling a runtime component.
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.interruption_timeout_ms == 0
            || self.cleanup_timeout_ms == 0
            || self.interruption_timeout_ms > 86_400_000
            || self.cleanup_timeout_ms > 86_400_000
            || self.lease_ttl_ms == 0
            || self.lease_ttl_ms > 86_400_000
            || self.heartbeat_interval_ms == 0
            || self.heartbeat_interval_ms > self.lease_ttl_ms / 3
            || self.observer_poll_ms == 0
            || self.observer_poll_ms > 60_000
            || self.start_timeout_ms == 0
            || self.start_timeout_ms > 86_400_000
            || self.event_page_size == 0
            || self.event_page_size > MAX_EVENT_PAGE_SIZE
            || self.max_request_bytes == 0
            || self.projection_limits.max_bytes == 0
            || self.projection_limits.max_items == 0
            || self.response_limits.max_input_bytes == 0
            || self.response_limits.max_response_bytes == 0
            || self.response_limits.max_delta_bytes == 0
            || self.response_limits.max_events == 0
            || self.tool_execution_limits.timeout_ms == 0
            || self.tool_execution_limits.timeout_ms > 86_400_000
            || self.tool_execution_limits.max_receipt_bytes == 0
        {
            return Err(fail(ErrorCode::InvalidConfiguration, "agent.settings"));
        }
        Ok(())
    }
}

/// Already-created Host components for one exact scope. Creating an Agent does
/// not invoke these ports, open connections, start tasks or read environment data.
pub struct AgentBindings {
    /// Optional versioned stop policy and business-state schema; omission uses the core default.
    pub interruption_policy: Option<InterruptionPolicyBinding>,
    /// Fixed tenant/workspace/user namespace; validated against routing at start.
    pub scope: Scope,
    /// Durable or explicitly process-local state implementation.
    pub state: Arc<dyn StateStore>,
    /// Current authorization gate.
    pub policy: Arc<PolicyGate>,
    /// Approved profile metadata resolver, called only for new requests.
    pub profile_resolver: Arc<dyn ProfileResolver>,
    /// Configured model exchange with dispatcher and route inspector.
    pub model_exchange: Arc<ModelExchange>,
    /// Exact catalog and policy snapshot for newly admitted runs.
    pub router: Arc<dyn ModelRouter>,
    /// Trusted instructions pinned in the session prefix.
    pub host_instructions: Vec<String>,
    /// Registered system-input metadata; values arrive through ExecutionContext.
    pub system_inputs: SystemInputRegistry,
    /// Existing tool executors and compiled contracts, restricted to this scope.
    pub tools: Option<Arc<ToolRegistry>>,
    /// Optional read-only source for registered resolver-owned system inputs.
    pub system_input_resolver: Option<Arc<dyn SystemInputResolver>>,
    /// Optional read-only verifier for externally supplied effect receipts.
    pub external_receipt_verifier: Option<Arc<dyn ExternalReceiptVerifier>>,
    /// Optional scope-bound lifecycle runtime; selected definitions are pinned at admission.
    pub hooks: Option<Arc<HookRuntime>>,
    /// Optional component assembly/runtime. It owns all catalog and exported Tool/Hook/Source selections.
    /// Direct tools/hooks/context_sources cannot also be supplied when this is configured.
    pub components: Option<Arc<dyn ComponentRuntime>>,
    /// Directly supplied scoped context sources, used when components is None.
    pub context_sources: Option<Arc<ContextSourceRuntime>>,
    /// Versioned Host estimate for source items in component mode; never byte-as-token usage.
    pub context_token_estimator: Option<Arc<dyn ContextTokenEstimator>>,
    /// Exact Skill manifests and explicitly registered instruction loader.
    pub skills: Option<Arc<SkillRuntime>>,
    /// Scoped artifact access for Tool results and model-visible references.
    pub artifacts: Option<Arc<ArtifactRuntime>>,
    /// Optional bounded selector/compressor; omission uses bounded selection and previews only.
    pub context_runtime: Option<Arc<ContextRuntime>>,
    /// Output schemas and approved read-only verifiers.
    pub verification: Option<Arc<VerificationRuntime>>,
    /// Time source and timers.
    pub clock: Arc<dyn Clock>,
    /// New internal run/message/event identities, never business foreign keys.
    pub ids: Arc<dyn IdSource>,
    /// Route-specific token estimate callback.
    pub token_estimator: Arc<dyn ModelTokenEstimator>,
    /// Finite runtime limits.
    pub settings: AgentSettings,
}

/// Scope-bound Agent facade. Clone shares local driver ownership and observations.
#[derive(Clone)]
pub struct Agent {
    inner: Arc<Inner>,
}
struct Inner {
    profile: AgentProfile,
    bindings: AgentBindings,
    context: Arc<ContextRuntime>,
    verification: Arc<VerificationRuntime>,
    runs: Mutex<BTreeMap<Id, Arc<LocalRun>>>,
    observed: Arc<persistence::ObservedState>,
}
struct LocalRun {
    segment_start_revision: u64,
    cancel: CancellationToken,
    reason: Mutex<Option<Id>>,
    stop_cause: Mutex<Option<InterruptionCause>>,
    cleanup_deadline: Mutex<Option<tokio::time::Instant>>,
    error: Mutex<Option<ContractError>>,
    observer_error: Mutex<Option<ContractError>>,
    release_report: Mutex<Option<ComponentReleaseReport>>,
    release_error: Mutex<Option<ContractError>>,
    pending_observations: Mutex<Vec<(HookTarget, HookInput)>>,
    done: AtomicBool,
    notify: Notify,
}
impl LocalRun {
    fn new(segment_start_revision: u64) -> Self {
        Self {
            segment_start_revision,
            cancel: CancellationToken::new(),
            reason: Mutex::new(None),
            stop_cause: Mutex::new(None),
            cleanup_deadline: Mutex::new(None),
            error: Mutex::new(None),
            observer_error: Mutex::new(None),
            release_report: Mutex::new(None),
            release_error: Mutex::new(None),
            pending_observations: Mutex::new(vec![]),
            done: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }
}
impl fmt::Debug for Agent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Agent")
            .field("agent_id", &self.inner.profile.agent_id)
            .finish_non_exhaustive()
    }
}

/// Validate structural settings and binding scopes without invoking Host callbacks.
/// Profile requirements are resolved only when admitting a new request.
/// Tools, adapters, skills, context strategies, and verifiers use existing Host bindings.
/// Instruction asset loading and generic extension execution remain unsupported.
pub fn create_agent(
    profile: AgentProfile,
    mut bindings: AgentBindings,
) -> Result<Agent, ContractError> {
    profile.validate_structure()?;
    bindings.settings.validate()?;
    let context = match &bindings.context_runtime {
        Some(context) => context.clone(),
        None => Arc::new(ContextRuntime::bounded(bindings.scope.clone())?),
    };
    if context.scope() != &bindings.scope {
        return Err(fail(ErrorCode::AccessDenied, "agent.context_scope"));
    }
    let verification = match &bindings.verification {
        Some(runtime) => runtime.clone(),
        None => Arc::new(VerificationRuntime::text(bindings.scope.clone())?),
    };
    if verification.scope != bindings.scope {
        return Err(fail(ErrorCode::AccessDenied, "agent.verification_scope"));
    }
    if bindings
        .skills
        .as_ref()
        .is_some_and(|value| value.scope() != &bindings.scope)
        || bindings
            .tools
            .as_ref()
            .is_some_and(|value| value.scope() != &bindings.scope)
        || bindings
            .hooks
            .as_ref()
            .is_some_and(|value| value.scope() != &bindings.scope)
        || bindings
            .context_sources
            .as_ref()
            .is_some_and(|value| value.scope() != &bindings.scope)
    {
        return Err(fail(ErrorCode::AccessDenied, "agent.binding_scope"));
    }
    if bindings.components.is_some()
        && (bindings.tools.is_some()
            || bindings.hooks.is_some()
            || bindings.context_sources.is_some())
    {
        return Err(fail(
            ErrorCode::InvalidConfiguration,
            "agent.component_authority",
        ));
    }
    let observed = Arc::new(persistence::ObservedState::default());
    bindings.state = Arc::new(persistence::ObservedStore {
        inner: bindings.state.clone(),
        observed: observed.clone(),
    });
    Ok(Agent {
        inner: Arc::new(Inner {
            profile,
            bindings,
            context,
            verification,
            runs: Mutex::new(BTreeMap::new()),
            observed,
        }),
    })
}

/// A durable observer. Dropping this value or its streams does not cancel the driver.
#[derive(Clone)]
pub struct RunHandle {
    segment_id: Id,
    agent: Agent,
    run_id: Id,
    segment_start_revision: u64,
    local: Option<Arc<LocalRun>>,
}
impl fmt::Debug for RunHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunHandle")
            .field("run_id", &self.run_id)
            .field("segment_id", &self.segment_id)
            .finish_non_exhaustive()
    }
}

/// Result of an authorized cancellation request, separate from stored RunOutcome.
pub type CancelReceipt = ControlReceipt;

/// Protected observer reports and a local report-persistence failure, independent
/// of the saved execution outcome. An observer does not change business success.
#[derive(Debug)]
pub struct HookObservationView {
    /// Reports that the StateStore actually accepted.
    pub reports: Vec<HookObservation>,
    /// A local failure to persist an observer report, when this handle knows it.
    pub local_error: Option<ContractError>,
}

/// Local component cleanup information. It never replaces a stored RunOutcome.
#[derive(Debug)]
pub struct ComponentReleaseView {
    /// Completed release report for this handle's execution segment, when available.
    pub report: Option<ComponentReleaseReport>,
    /// Failure to finish the bounded release protocol, distinct from execution failure.
    pub local_error: Option<ContractError>,
}

impl Agent {
    /// Admit through an owned coordinator. Caller-future disconnection after polling
    /// does not abort durable admission or its detached driver.
    pub async fn start(
        &self,
        request: RunRequest,
        context: ExecutionContext,
    ) -> Result<Guarded<RunHandle>, ContractError> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| fail(ErrorCode::RuntimeUnavailable, "agent.runtime"))?;
        let agent = self.clone();
        runtime
            .spawn(async move { agent.admit(request, context).await })
            .await
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.coordinator"))?
    }
    /// Read minimal saved metadata under current permission.
    pub async fn get_run(
        &self,
        run_id: &Id,
        context: &ExecutionContext,
    ) -> Result<Guarded<RunView>, ContractError> {
        self.check_scope(context)?;
        let saved = caller_read(
            context,
            None,
            self.inner
                .bindings
                .state
                .load(&self.inner.bindings.scope, run_id),
        )
        .await?;
        self.inner
            .bindings
            .policy
            .run_view(
                &saved.snapshot,
                self.inner.bindings.clock.as_ref(),
                context,
                None,
            )
            .await
    }
    /// Read protected saved state under the separate details permission.
    pub async fn get_run_details(
        &self,
        run_id: &Id,
        context: &ExecutionContext,
    ) -> Result<Guarded<RunSnapshot>, ContractError> {
        self.check_scope(context)?;
        let loaded = caller_read(
            context,
            None,
            self.inner
                .bindings
                .state
                .load(&self.inner.bindings.scope, run_id),
        )
        .await;
        let saved = match loaded {
            Ok(saved) => saved,
            Err(error) if error.code == ErrorCode::PersistenceUnavailable => {
                return self.persistence_error(run_id, context, error).await;
            }
            Err(error) => return Err(error),
        };
        self.inner
            .bindings
            .policy
            .run_details(&saved.snapshot, context, None)
            .await
    }
    /// Consume one authorized, fixed wait decision in an owned coordinator.
    /// A duplicate command returns its existing segment without restarting work.
    pub async fn resume(
        &self,
        command: ResumeCommand,
        context: ExecutionContext,
    ) -> Result<Guarded<RunHandle>, ContractError> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| fail(ErrorCode::RuntimeUnavailable, "agent.runtime"))?;
        self.check_scope(&context)?;
        let run_id = command.run_id.clone();
        let disclosure_context = context.clone();
        let agent = self.clone();
        // Build the owned coordinator outside the caller's poll stack before
        // handing it to Tokio; recovery can nest several large state futures.
        let result = runtime
            .spawn(crate::future::boxed(|| async move {
                agent.resume_command(command, context).await
            }))
            .await
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.resume"))?;
        match result {
            Err(error) if error.code == ErrorCode::PersistenceUnavailable => {
                self.persistence_error(&run_id, &disclosure_context, error)
                    .await
            }
            other => other,
        }
    }
    async fn persistence_error<T>(
        &self,
        run_id: &Id,
        context: &ExecutionContext,
        error: ContractError,
    ) -> Result<Guarded<T>, ContractError> {
        self.check_scope(context)?;
        let request = PolicyRequest {
            owner_scope: self.inner.bindings.scope.clone(),
            resource_id: run_id.clone(),
            action: PolicyAction::ReadRunDetails {},
        };
        self.inner
            .bindings
            .policy
            .guard(&request, context, None, None, || async {
                Err(self.inner.observed.attach(run_id, error))
            })
            .await
    }
    fn check_scope(&self, context: &ExecutionContext) -> Result<(), ContractError> {
        if context.data.scope != self.inner.bindings.scope {
            return Err(fail(ErrorCode::AccessDenied, "scope"));
        }
        Ok(())
    }
    async fn handle(
        &self,
        run_id: Id,
        segment_start_revision: u64,
    ) -> Result<RunHandle, ContractError> {
        let history = self
            .inner
            .bindings
            .state
            .read_execution(&self.inner.bindings.scope, &run_id)
            .await?;
        let segment = history
            .segments
            .iter()
            .find(|segment| segment.accepted_revision == segment_start_revision)
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.segment"))?;
        self.segment_handle(run_id, segment_start_revision, segment.segment_id.clone())
    }
    async fn latest_handle(&self, run_id: Id) -> Result<RunHandle, ContractError> {
        let bindings = &self.inner.bindings;
        let history = match bindings
            .state
            .read_execution(&bindings.scope, &run_id)
            .await
        {
            Ok(history) => history,
            Err(error) if error.code == ErrorCode::CapabilityUnsupported => {
                let saved = bindings.state.load(&bindings.scope, &run_id).await?;
                if !saved.snapshot.status.is_terminal() {
                    return Err(error);
                }
                // A deterministic read-only view ID does not invent execution ownership.
                let segment_id = Id::new(format!(
                    "legacy-view:{}",
                    crate::serialization::data_digest(&(&run_id, saved.snapshot.revision))
                ))?;
                return self.segment_handle(run_id, segment_revision(&saved.snapshot), segment_id);
            }
            Err(error) => return Err(error),
        };
        let segment = history
            .segments
            .last()
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.segment"))?;
        self.segment_handle(
            run_id,
            segment.accepted_revision,
            segment.segment_id.clone(),
        )
    }
    fn segment_handle(
        &self,
        run_id: Id,
        segment_start_revision: u64,
        segment_id: Id,
    ) -> Result<RunHandle, ContractError> {
        let local = self
            .inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .get(&run_id)
            .filter(|local| local.segment_start_revision == segment_start_revision)
            .cloned();
        Ok(RunHandle {
            segment_id,
            agent: self.clone(),
            run_id,
            segment_start_revision,
            local,
        })
    }
}

impl RunHandle {
    /// Inspect cleanup for this process's segment after current details authorization.
    pub async fn component_release(
        &self,
        context: &ExecutionContext,
    ) -> Result<Guarded<ComponentReleaseView>, ContractError> {
        self.agent.check_scope(context)?;
        let request = PolicyRequest {
            owner_scope: self.agent.inner.bindings.scope.clone(),
            resource_id: self.run_id.clone(),
            action: PolicyAction::ReadRunDetails {},
        };
        self.agent
            .inner
            .bindings
            .policy
            .guard(&request, context, None, None, || async {
                let local = self.current_local()?;
                let report = local
                    .as_ref()
                    .map(|local| {
                        local
                            .release_report
                            .lock()
                            .map(|report| report.clone())
                            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.release_state"))
                    })
                    .transpose()?
                    .flatten();
                let local_error = local
                    .as_ref()
                    .map(|local| {
                        local
                            .release_error
                            .lock()
                            .map(|error| error.clone())
                            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.release_state"))
                    })
                    .transpose()?
                    .flatten();
                Ok(ComponentReleaseView {
                    report,
                    local_error,
                })
            })
            .await
    }
    /// Read committed hook observations under current protected-details permission.
    pub async fn hook_observations(
        &self,
        context: &ExecutionContext,
    ) -> Result<Guarded<HookObservationView>, ContractError> {
        self.agent.check_scope(context)?;
        let bindings = &self.agent.inner.bindings;
        let request = PolicyRequest {
            owner_scope: bindings.scope.clone(),
            resource_id: self.run_id.clone(),
            action: PolicyAction::ReadRunDetails {},
        };
        if let Guarded::ApprovalRequired(challenge) = bindings
            .policy
            .guard(&request, context, None, None, || async { Ok(()) })
            .await?
        {
            return Ok(Guarded::ApprovalRequired(challenge));
        }
        let reports = caller_read(
            context,
            None,
            bindings
                .state
                .read_hook_observations(&bindings.scope, &self.run_id),
        )
        .await?;
        if reports
            .iter()
            .any(|report| report.scope != bindings.scope || report.run_id != self.run_id)
        {
            return Err(fail(
                ErrorCode::InvalidSnapshot,
                "agent.hook_observation_scope",
            ));
        }
        let local_error = self
            .current_local()?
            .map(|local| {
                local
                    .observer_error
                    .lock()
                    .map(|error| error.clone())
                    .map_err(|_| fail(ErrorCode::InvalidContract, "agent.observer_state"))
            })
            .transpose()?
            .flatten();
        bindings
            .policy
            .guard(&request, context, None, None, || async {
                Ok(HookObservationView {
                    reports,
                    local_error,
                })
            })
            .await
    }
    /// Immutable execution interval identity. Resuming creates a different handle.
    pub fn segment_id(&self) -> &Id {
        &self.segment_id
    }
    /// Stable saved run identity.
    pub fn run_id(&self) -> &Id {
        &self.run_id
    }
    /// Wait for an authorized saved outcome. Observer cancellation never cancels execution.
    pub async fn outcome(
        &self,
        context: &ExecutionContext,
    ) -> Result<Guarded<RunOutcome>, ContractError> {
        loop {
            let snapshot = match self.agent.get_run_details(&self.run_id, context).await? {
                Guarded::Completed(snapshot) => snapshot,
                Guarded::ApprovalRequired(challenge) => {
                    return Ok(Guarded::ApprovalRequired(challenge));
                }
            };
            let history = caller_read(
                context,
                None,
                self.agent
                    .inner
                    .bindings
                    .state
                    .read_execution(&snapshot.scope, &self.run_id),
            )
            .await;
            let outcome = match history {
                Ok(history) => {
                    let segment = history
                        .segments
                        .iter()
                        .find(|segment| {
                            segment.segment_id == self.segment_id
                                && segment.accepted_revision == self.segment_start_revision
                        })
                        .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.segment"))?;
                    match &segment.outcome {
                        Some(SegmentOutcome::Settled { outcome }) => Some(outcome.as_ref().clone()),
                        Some(SegmentOutcome::Interrupted { interruption }) => {
                            let reference =
                                segment.source_snapshot_ref.as_ref().ok_or_else(|| {
                                    fail(
                                        ErrorCode::ComponentUnavailable,
                                        "agent.legacy_segment_source",
                                    )
                                })?;
                            let record = caller_read(
                                context,
                                None,
                                self.agent
                                    .inner
                                    .bindings
                                    .state
                                    .read_record(&snapshot.scope, reference),
                            )
                            .await?;
                            if record.reference() != reference {
                                return Err(fail(
                                    ErrorCode::InvalidSnapshot,
                                    "agent.segment_source",
                                ));
                            }
                            let source: RunSnapshot =
                                serde_json::from_value(record.value().clone()).map_err(|_| {
                                    fail(ErrorCode::InvalidSnapshot, "agent.segment_source")
                                })?;
                            Some(RunOutcome {
                                result: OutcomeResult::Interrupted {
                                    interruption: interruption.clone(),
                                },
                                output: vec![],
                                artifacts: vec![],
                                usage: source.usage,
                                checkpoint_revision: source.revision,
                                verification: None,
                                unresolved_effects: interruption.unresolved_effects.clone(),
                                app_state: source.app_state,
                            })
                        }
                        None => None,
                    }
                }
                Err(error)
                    if error.code == ErrorCode::CapabilityUnsupported
                        && snapshot.status.is_terminal() =>
                {
                    snapshot.outcome.clone()
                }
                Err(error) => return Err(error),
            };
            if let Some(outcome) = outcome {
                let request = PolicyRequest {
                    owner_scope: snapshot.scope.clone(),
                    resource_id: self.run_id.clone(),
                    action: PolicyAction::ReadRunDetails {},
                };
                return self
                    .agent
                    .inner
                    .bindings
                    .policy
                    .guard(&request, context, None, None, || async { Ok(outcome) })
                    .await;
            }
            self.local_error()
                .map_err(|error| self.agent.inner.observed.attach(&self.run_id, error))?;
            self.wait(context).await?;
        }
    }
    /// Replay durable event metadata with fresh permission checks on every page
    /// and event. Polling has no channel backpressure on execution.
    pub fn events(
        &self,
        after_seq: u64,
        context: ExecutionContext,
    ) -> PortStream<'static, EventView> {
        let handle = self.clone();
        Box::pin(stream::try_unfold(
            (handle, context, after_seq, Vec::<RunEvent>::new()),
            |(handle, context, mut cursor, mut pending)| async move {
                loop {
                    if !pending.is_empty() {
                        let event = pending.remove(0);
                        let saved = caller_read(
                            &context,
                            None,
                            handle
                                .agent
                                .inner
                                .bindings
                                .state
                                .load(&handle.agent.inner.bindings.scope, &handle.run_id),
                        )
                        .await?;
                        if handle
                            .segment_end(&saved.snapshot, &context)
                            .await?
                            .is_some_and(|end| event.seq.get() > end)
                        {
                            return Ok(None);
                        }
                        let event = match handle
                            .agent
                            .inner
                            .bindings
                            .policy
                            .event_view(&event, &context, None)
                            .await?
                        {
                            Guarded::Completed(event) => event,
                            Guarded::ApprovalRequired(_) => {
                                return Err(fail(ErrorCode::AccessDenied, "agent.events.approval"));
                            }
                        };
                        cursor = event.seq.get();
                        return Ok(Some((event, (handle, context, cursor, pending))));
                    }
                    handle.agent.check_scope(&context)?;
                    let bindings = &handle.agent.inner.bindings;
                    let policy = PolicyRequest {
                        owner_scope: bindings.scope.clone(),
                        resource_id: handle.run_id.clone(),
                        action: PolicyAction::ReadEvents {},
                    };
                    match bindings
                        .policy
                        .guard(&policy, &context, None, None, || {
                            caller_read(
                                &context,
                                None,
                                bindings.state.read_events(
                                    &bindings.scope,
                                    &handle.run_id,
                                    cursor,
                                    bindings.settings.event_page_size,
                                ),
                            )
                        })
                        .await?
                    {
                        Guarded::ApprovalRequired(_) => {
                            return Err(fail(ErrorCode::AccessDenied, "agent.events.approval"));
                        }
                        Guarded::Completed(page) => {
                            pending = page.events;
                        }
                    }
                    if !pending.is_empty() {
                        continue;
                    }
                    let saved = caller_read(
                        &context,
                        None,
                        bindings.state.load(&bindings.scope, &handle.run_id),
                    )
                    .await?;
                    if let Some(end) = handle.segment_end(&saved.snapshot, &context).await? {
                        if cursor >= end {
                            return Ok(None);
                        }
                        continue;
                    }
                    handle.local_error()?;
                    handle.wait(&context).await?;
                }
            },
        ))
    }
    /// Cooperatively stop this execution interval without cancelling the Run.
    /// NotLocal does not submit a remote command; the Host must deliver it to its Worker.
    pub async fn stop_execution(
        &self,
        cause: InterruptionCause,
        context: &ExecutionContext,
    ) -> Result<Guarded<ExecutionStopReceipt>, ContractError> {
        if !matches!(
            cause,
            InterruptionCause::HostShutdown | InterruptionCause::SegmentStopped
        ) {
            return Err(fail(ErrorCode::InvalidContract, "interruption.stop_cause"));
        }
        self.agent.check_scope(context)?;
        let bindings = &self.agent.inner.bindings;
        let request = PolicyRequest {
            owner_scope: bindings.scope.clone(),
            resource_id: self.run_id.clone(),
            action: PolicyAction::StopExecution { cause },
        };
        bindings
            .policy
            .guard(&request, context, None, None, || async {
                let saved = caller_read(
                    context,
                    None,
                    bindings.state.load(&bindings.scope, &self.run_id),
                )
                .await?;
                if saved.snapshot.status != RunStatus::Running
                    || segment_revision(&saved.snapshot) != self.segment_start_revision
                {
                    return Ok(ExecutionStopReceipt::AlreadySettled);
                }
                if saved.snapshot.interruption_plan_ref.is_none() {
                    return Err(fail(
                        ErrorCode::ComponentUnavailable,
                        "interruption.legacy_plan",
                    ));
                }
                let Some(local) = self.current_local()? else {
                    return Ok(ExecutionStopReceipt::NotLocal);
                };
                if local.done.load(Ordering::Acquire) {
                    return Ok(ExecutionStopReceipt::NotLocal);
                }
                let mut stop = local
                    .stop_cause
                    .lock()
                    .map_err(|_| fail(ErrorCode::InvalidContract, "interruption.stop_state"))?;
                if stop.is_none() {
                    *stop = Some(cause);
                }
                local.cancel.cancel();
                Ok(ExecutionStopReceipt::Requested)
            })
            .await
    }
    /// Submit a durable cancellation under current authority. Use
    /// Agent::submit_control_command when the caller supplies an idempotency key.
    pub async fn cancel(
        &self,
        reason: Id,
        context: &ExecutionContext,
    ) -> Result<Guarded<CancelReceipt>, ContractError> {
        self.agent
            .submit_control_command(
                self.run_id.clone(),
                ControlCommand {
                    command_id: self.agent.inner.bindings.ids.next_id()?,
                    principal_ref: context.data.principal_ref.clone(),
                    action: ControlAction::Cancel { reason },
                },
                context.clone(),
            )
            .await
    }
    fn local_error(&self) -> Result<(), ContractError> {
        if let Some(local) = self.current_local()? {
            if local.done.load(Ordering::Acquire) {
                if let Some(error) = local
                    .error
                    .lock()
                    .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
                    .clone()
                {
                    return Err(error);
                }
            }
        }
        Ok(())
    }
    fn current_local(&self) -> Result<Option<Arc<LocalRun>>, ContractError> {
        Ok(self
            .agent
            .inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .get(&self.run_id)
            .filter(|local| local.segment_start_revision == self.segment_start_revision)
            .cloned()
            .or_else(|| self.local.clone()))
    }
    async fn segment_end(
        &self,
        snapshot: &RunSnapshot,
        context: &ExecutionContext,
    ) -> Result<Option<u64>, ContractError> {
        let history = caller_read(
            context,
            None,
            self.agent
                .inner
                .bindings
                .state
                .read_execution(&snapshot.scope, &self.run_id),
        )
        .await;
        match history {
            Ok(history) => {
                let segment = history
                    .segments
                    .iter()
                    .find(|segment| segment.accepted_revision == self.segment_start_revision)
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.segment"))?;
                if let Some(last) = segment.last_event_seq {
                    return Ok(Some(last));
                }
            }
            Err(error) if error.code == ErrorCode::CapabilityUnsupported => {}
            Err(error) => return Err(error),
        }
        if let Some(receipt) = snapshot
            .resume_receipts
            .iter()
            .find(|receipt| receipt.previous_segment_start_revision == self.segment_start_revision)
        {
            return Ok(Some(receipt.previous_last_event_seq));
        }
        if let Some(receipt) = snapshot
            .recovery_receipts
            .iter()
            .find(|receipt| receipt.previous_segment_start_revision == self.segment_start_revision)
        {
            return Ok(Some(receipt.previous_last_event_seq));
        }
        if segment_revision(snapshot) != self.segment_start_revision {
            return Err(fail(ErrorCode::InvalidSnapshot, "agent.segment"));
        }
        Ok((snapshot.status.is_terminal()
            || matches!(snapshot.status, RunStatus::Waiting | RunStatus::Interrupted))
        .then_some(snapshot.last_event_seq))
    }
    async fn wait(&self, context: &ExecutionContext) -> Result<(), ContractError> {
        tokio::select! { biased;
            _ = context.cancellation.cancelled() => Err(fail(ErrorCode::Cancelled, "agent.observer")),
            _ = tokio::time::sleep(Duration::from_millis(self.agent.inner.bindings.settings.observer_poll_ms)) => Ok(()),
        }
    }
}

fn fail(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
fn segment_revision(snapshot: &RunSnapshot) -> u64 {
    snapshot
        .resume_receipts
        .last()
        .map_or(0, |receipt| receipt.accepted_revision)
        .max(
            snapshot
                .recovery_receipts
                .last()
                .map_or(0, |receipt| receipt.accepted_revision),
        )
}

// Only read-only caller operations use this helper. Cancelling a read drops its
// future without signalling the independent driver or cancelling a durable write.
async fn caller_read<T>(
    context: &ExecutionContext,
    timeout: Option<Duration>,
    future: impl std::future::Future<Output = Result<T, ContractError>>,
) -> Result<T, ContractError> {
    let deadline = async {
        match timeout {
            Some(timeout) => tokio::time::sleep(timeout).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::select! { biased;
        _ = context.cancellation.cancelled() => Err(fail(ErrorCode::Cancelled, "agent.read")),
        _ = deadline => Err(fail(ErrorCode::DeadlineExceeded, "agent.read")),
        result = future => result,
    }
}

impl Agent {
    pub(super) fn validate_new_configuration(&self) -> Result<(), ContractError> {
        let profile = &self.inner.profile;
        let bindings = &self.inner.bindings;
        let context = &self.inner.context;
        let verification = &self.inner.verification;
        if !matches!(profile.instructions, Instructions::Text(_))
            || (!profile.skills.is_empty() && bindings.skills.is_none())
            || (bindings.components.is_none()
                && (!profile.connectors.is_empty()
                    || profile.adapters.as_ref().is_some_and(|v| !v.is_empty())))
            || profile.extensions.as_ref().is_some_and(|v| !v.is_empty())
        {
            return Err(fail(ErrorCode::CapabilityUnsupported, "agent.profile"));
        }
        context.plan(profile, &bindings.scope)?;
        if verification.scope != bindings.scope {
            return Err(fail(ErrorCode::AccessDenied, "agent.verification_scope"));
        }
        verification.plan(profile, None)?;

        if bindings
            .skills
            .as_ref()
            .is_some_and(|skills| skills.scope() != &bindings.scope)
        {
            return Err(fail(ErrorCode::AccessDenied, "agent.skills_scope"));
        }
        if bindings.components.is_some()
            && (bindings.tools.is_some()
                || bindings.hooks.is_some()
                || bindings.context_sources.is_some())
        {
            return Err(fail(
                ErrorCode::InvalidConfiguration,
                "agent.component_authority",
            ));
        }
        if bindings.components.is_none() {
            match &bindings.context_sources {
                Some(sources) => {
                    if sources.scope() != &bindings.scope {
                        return Err(fail(ErrorCode::AccessDenied, "agent.sources_scope"));
                    }
                    sources.plan(profile)?;
                }
                None if profile
                    .context_sources
                    .as_ref()
                    .is_some_and(|sources| !sources.is_empty()) =>
                {
                    return Err(fail(ErrorCode::CapabilityUnsupported, "agent.sources"));
                }
                None => {}
            }
            match &bindings.hooks {
                Some(hooks) => {
                    if hooks.scope() != &bindings.scope {
                        return Err(fail(ErrorCode::AccessDenied, "agent.hooks_scope"));
                    }
                    hooks.plan(profile)?;
                }
                None if profile
                    .hooks
                    .as_ref()
                    .is_some_and(|hooks| !hooks.is_empty()) =>
                {
                    return Err(fail(ErrorCode::CapabilityUnsupported, "agent.hooks"));
                }
                None => {}
            }
            match &bindings.tools {
                Some(registry) => {
                    if registry.scope() != &bindings.scope {
                        return Err(fail(ErrorCode::AccessDenied, "agent.tools_scope"));
                    }
                    registry.prompt_bindings(profile)?;
                }
                None if !profile.tools.is_empty() => {
                    return Err(fail(ErrorCode::CapabilityUnsupported, "agent.tools"));
                }
                None => {}
            }
        }
        if bindings.components.is_some()
            && profile
                .context_sources
                .as_ref()
                .is_some_and(|sources| !sources.is_empty())
            && bindings.context_token_estimator.is_none()
        {
            return Err(fail(
                ErrorCode::InvalidConfiguration,
                "agent.source_estimator",
            ));
        }

        Ok(())
    }
}
```

## `crates/wickle/src/views.rs`

```rust
use crate::{
    ArtifactRef, BudgetUsage, ContractError, ExecutionContext, Guarded, Id, PolicyAction,
    PolicyGate, PolicyRequest, RunEvent, RunEventPayload, RunPhase, RunSnapshot, RunStatus,
};
use serde::Serialize;
use std::num::NonZeroU64;
use tokio::time::Instant;

/// Minimal run metadata. Protected records and outcome internals are not included.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunView {
    /// Run identity.
    pub run_id: Id,
    /// Session identity.
    pub session_id: Id,
    /// Current status.
    pub status: RunStatus,
    /// Current phase.
    pub phase: RunPhase,
    /// Snapshot revision.
    pub revision: u64,
    /// Whether the original Run deadline has elapsed while this Run is nonterminal.
    /// This observation does not expire the Run or advance its saved usage.
    pub deadline_expired: bool,
    /// Saved usage, not inferred provider consumption.
    pub usage: BudgetUsage,
}

/// Authorized artifact metadata without an embedded storage location or payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactView {
    /// Artifact identity.
    pub artifact_id: Id,
    /// Stored media type.
    pub media_type: Id,
    /// Original byte size.
    pub size_bytes: u64,
    /// Store-defined content hash.
    pub content_hash: Id,
}

/// Event metadata without protected record references from its payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EventView {
    /// Event identity.
    pub event_id: Id,
    /// Owning run.
    pub run_id: Id,
    /// Owning session.
    pub session_id: Id,
    /// Durable sequence.
    pub seq: NonZeroU64,
    /// UTC milliseconds since Unix epoch.
    pub timestamp_ms: i64,
    /// Durable event kind.
    pub event_type: &'static str,
}

impl PolicyGate {
    /// Authorize using the stored run scope and select public fields explicitly.
    pub async fn run_view(
        &self,
        snapshot: &RunSnapshot,
        clock: &dyn crate::Clock,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<Guarded<RunView>, ContractError> {
        let request = PolicyRequest {
            owner_scope: snapshot.scope.clone(),
            resource_id: snapshot.run_id.clone(),
            action: PolicyAction::ReadRun {},
        };
        self.guard(&request, context, deadline, None, || async {
            let deadline_expired = if snapshot.status.is_terminal() {
                false
            } else {
                let now = clock.now()?.utc_ms;
                if now < snapshot.timing.last_observed_at_ms {
                    return Err(ContractError::new(
                        crate::ErrorCode::ClockRegression,
                        "views.run_clock",
                    ));
                }
                now >= snapshot.timing.deadline_at_ms
            };
            Ok(RunView {
                run_id: snapshot.run_id.clone(),
                session_id: snapshot.request.session_id.clone(),
                status: snapshot.status,
                phase: snapshot.phase,
                revision: snapshot.revision,
                usage: snapshot.usage.clone(),
                deadline_expired,
            })
        })
        .await
    }

    /// Read a full protected checkpoint only through the distinct details action.
    pub async fn run_details(
        &self,
        snapshot: &RunSnapshot,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<Guarded<RunSnapshot>, ContractError> {
        let request = PolicyRequest {
            owner_scope: snapshot.scope.clone(),
            resource_id: snapshot.run_id.clone(),
            action: PolicyAction::ReadRunDetails {},
        };
        self.guard(&request, context, deadline, None, || async {
            Ok(snapshot.clone())
        })
        .await
    }

    /// Authorize against authoritative artifact metadata before selecting its view.
    pub async fn artifact_view(
        &self,
        artifact: &ArtifactRef,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<Guarded<ArtifactView>, ContractError> {
        let request = PolicyRequest {
            owner_scope: artifact.scope.clone(),
            resource_id: artifact.artifact_id.clone(),
            action: PolicyAction::ReadArtifact {},
        };
        self.guard(&request, context, deadline, None, || async {
            Ok(ArtifactView {
                artifact_id: artifact.artifact_id.clone(),
                media_type: artifact.media_type.clone(),
                size_bytes: artifact.size_bytes,
                content_hash: artifact.content_hash.clone(),
            })
        })
        .await
    }

    /// Authorize a stored event without publishing its protected payload references.
    pub async fn event_view(
        &self,
        event: &RunEvent,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<Guarded<EventView>, ContractError> {
        let request = PolicyRequest {
            owner_scope: event.scope.clone(),
            resource_id: event.run_id.clone(),
            action: PolicyAction::ReadEvents {},
        };
        self.guard(&request, context, deadline, None, || async {
            let event_type = match &event.payload {
                RunEventPayload::RunStarted { .. } => "run.started",
                RunEventPayload::ContextRewritten { .. } => "context.rewritten",
                RunEventPayload::ToolPlanned { .. } => "tool.planned",
                RunEventPayload::ToolSettled { .. } => "tool.settled",
                RunEventPayload::ToolReconciled { .. } => "tool.reconciled",
                RunEventPayload::ToolUnresolved { .. } => "tool.unresolved",
                RunEventPayload::VerificationCompleted { .. } => "verification.completed",
                RunEventPayload::RunWaiting { .. } => "run.waiting",
                RunEventPayload::RunInterrupted { .. } => "run.interrupted",
                RunEventPayload::RunResumed { .. } => "run.resumed",
                RunEventPayload::RunRecovered { .. } => "run.recovered",
                RunEventPayload::RunFinished { .. } => "run.finished",
                RunEventPayload::ModelRouteSelected { .. } => "model.route_selected",
            };
            Ok(EventView {
                event_id: event.event_id.clone(),
                run_id: event.run_id.clone(),
                session_id: event.session_id.clone(),
                seq: event.seq,
                timestamp_ms: event.timestamp_ms,
                event_type,
            })
        })
        .await
    }
}
```

## `crates/wickle/tests/agent_control.rs`

```rust
//! Durable controls settle owned intervals without replaying business operations.
#[path = "support/agent.rs"]
#[allow(dead_code)]
mod agent_support;
#[path = "support/agent_resume.rs"]
#[allow(dead_code)]
mod support;
use std::{sync::atomic::Ordering, time::Duration};
use support::*;
use wickle::*;

fn command(name: &str, action: ControlAction) -> ControlCommand {
    ControlCommand {
        command_id: id(name),
        principal_ref: context().data.principal_ref,
        action,
    }
}

#[tokio::test]
async fn another_agent_can_submit_a_durable_cancel_that_the_owning_driver_consumes() {
    let fixture = agent_support::Fixture::new(agent_support::Response::Text, true);
    let owner = fixture.agent();
    let handle = fixture.started(&owner, "request").await;
    fixture.model.entered.notified().await;
    let remote = fixture.agent();
    let cancel = command(
        "remote-cancel",
        ControlAction::Cancel {
            reason: id("shutdown"),
        },
    );
    let receipt = completed(
        remote
            .submit_control_command(handle.run_id().clone(), cancel.clone(), context())
            .await
            .unwrap(),
    );
    assert!(receipt.processed_segment_id.is_none());
    let outcome = completed(
        tokio::time::timeout(Duration::from_secs(3), handle.outcome(&context()))
            .await
            .unwrap()
            .unwrap(),
    );
    assert!(matches!(outcome.result, OutcomeResult::Cancelled { .. }));
    let history = fixture
        .store
        .read_execution(&scope(), handle.run_id())
        .await
        .unwrap();
    assert_eq!(history.controls.len(), 1);
    assert_eq!(
        history.controls[0].processed_segment_id,
        Some(history.segments[0].segment_id.clone())
    );
    let repeated = completed(
        remote
            .submit_control_command(handle.run_id().clone(), cancel.clone(), context())
            .await
            .unwrap(),
    );
    assert_eq!(
        repeated.processed_segment_id,
        history.controls[0].processed_segment_id
    );
    assert_eq!(
        fixture
            .store
            .read_execution(&scope(), handle.run_id())
            .await
            .unwrap(),
        history
    );
    let changed = ControlCommand {
        action: ControlAction::Cancel {
            reason: id("changed"),
        },
        ..cancel
    };
    assert_eq!(
        remote
            .submit_control_command(handle.run_id().clone(), changed, context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::RequestConflict
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn idle_cancel_has_its_own_segment_and_old_handle_and_queries_do_not_change_it() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let old = fixture.started(&agent).await;
    let waiting = fixture.outcome(&old).await;
    let cancel = command(
        "idle-cancel",
        ControlAction::Cancel {
            reason: id("withdrawn"),
        },
    );
    let receipt = completed(
        agent
            .submit_control_command(old.run_id().clone(), cancel.clone(), context())
            .await
            .unwrap(),
    );
    assert!(receipt.processed_segment_id.is_some());
    assert_eq!(fixture.outcome(&old).await, waiting);
    let history = fixture
        .base
        .store
        .read_execution(&scope(), old.run_id())
        .await
        .unwrap();
    assert_eq!(history.segments.len(), 2);
    assert_eq!(
        history.segments[1].segment_id,
        receipt.processed_segment_id.clone().unwrap()
    );
    assert_ne!(old.segment_id(), &history.segments[1].segment_id);
    let duplicate = fixture.started(&agent).await;
    assert_eq!(duplicate.segment_id(), &history.segments[1].segment_id);
    assert_eq!(
        fixture.outcome(&duplicate).await.result.status(),
        RunStatus::Cancelled
    );
    let before = fixture.base.store.export_checkpoint(&scope()).unwrap();
    assert_eq!(
        completed(
            agent
                .get_control_receipt(old.run_id(), &receipt.command_id, &context())
                .await
                .unwrap()
        ),
        receipt
    );
    for _ in 0..3 {
        assert_eq!(
            completed(agent.get_run(old.run_id(), &context()).await.unwrap()).status,
            RunStatus::Cancelled
        );
    }
    assert_eq!(
        fixture
            .base
            .store
            .export_checkpoint(&scope())
            .unwrap()
            .digest(),
        before.digest()
    );
    let repeated = completed(
        agent
            .submit_control_command(old.run_id().clone(), cancel, context())
            .await
            .unwrap(),
    );
    assert_eq!(
        repeated.processed_segment_id,
        Some(history.segments[1].segment_id.clone())
    );
    assert_eq!(
        fixture
            .base
            .store
            .read_execution(&scope(), old.run_id())
            .await
            .unwrap(),
        history
    );
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn explicit_idle_expiry_preserves_the_old_wait_and_dispatches_nothing() {
    let mut fixture = Fixture::new(Mode::Approval);
    fixture.profile.limits.max_elapsed_ms = 100.try_into().unwrap();
    let agent = fixture.agent();
    let old = fixture.started(&agent).await;
    let waiting = fixture.outcome(&old).await;
    let expire = command("expire", ControlAction::Expire);
    let receipt = completed(
        agent
            .submit_control_command(old.run_id().clone(), expire.clone(), context())
            .await
            .unwrap(),
    );
    assert!(receipt.processed_segment_id.is_none());
    tokio::time::advance(Duration::from_millis(101)).await;
    let finished = completed(
        agent
            .process_control_command(old.run_id().clone(), expire.command_id, context())
            .await
            .unwrap(),
    );
    assert!(finished.processed_segment_id.is_some());
    assert_eq!(fixture.outcome(&old).await, waiting);
    let latest = fixture.saved(&old).await;
    assert!(matches!(
        latest.snapshot.outcome.unwrap().result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    ));
    assert!(latest.session.active_run_id.is_none());
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelling_an_interrupted_run_preserves_the_interrupted_handle() {
    let fixture = agent_support::Fixture::new(agent_support::Response::Text, true);
    let agent = fixture.agent();
    let old = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    completed(
        old.stop_execution(InterruptionCause::HostShutdown, &context())
            .await
            .unwrap(),
    );
    let interrupted = completed(old.outcome(&context()).await.unwrap());
    assert_eq!(interrupted.result.status(), RunStatus::Interrupted);
    let receipt = completed(
        old.cancel(id("cancel-interrupted"), &context())
            .await
            .unwrap(),
    );
    assert!(receipt.processed_segment_id.is_some());
    assert_eq!(
        completed(old.outcome(&context()).await.unwrap()),
        interrupted
    );
    assert_eq!(
        completed(agent.get_run(old.run_id(), &context()).await.unwrap()).status,
        RunStatus::Cancelled
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn competing_idle_cancel_and_expire_settle_once_and_record_both_receipts() {
    let mut fixture = Fixture::new(Mode::Approval);
    fixture.profile.limits.max_elapsed_ms = 100.try_into().unwrap();
    let agent = fixture.agent();
    let old = fixture.started(&agent).await;
    let waiting = fixture.outcome(&old).await;
    tokio::time::advance(Duration::from_millis(101)).await;
    let (cancel, expire) = tokio::join!(
        agent.submit_control_command(
            old.run_id().clone(),
            command(
                "cancel",
                ControlAction::Cancel {
                    reason: id("withdrawn")
                }
            ),
            context()
        ),
        agent.submit_control_command(
            old.run_id().clone(),
            command("expire", ControlAction::Expire),
            context()
        ),
    );
    assert!(completed(cancel.unwrap()).processed_segment_id.is_some());
    assert!(completed(expire.unwrap()).processed_segment_id.is_some());
    let history = fixture
        .base
        .store
        .read_execution(&scope(), old.run_id())
        .await
        .unwrap();
    assert_eq!(history.segments.len(), 2);
    assert_eq!(history.controls.len(), 2);
    assert_eq!(fixture.outcome(&old).await, waiting);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn checkpoint_import_rejects_forged_historical_outcomes_and_event_boundaries() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let old = fixture.started(&agent).await;
    fixture.outcome(&old).await;
    completed(old.cancel(id("cancel"), &context()).await.unwrap());
    let checkpoint = fixture.base.store.export_checkpoint(&scope()).unwrap();
    let original = serde_json::to_value(&checkpoint).unwrap();
    StateStoreCheckpoint::from_json(&original.to_string(), &scope(), &checkpoint.digest()).unwrap();
    for mutation in ["outcome", "boundary", "missing_boundary"] {
        let mut image = original.clone();
        let segment = &mut image["executions"][0]["segments"][0];
        match mutation {
            "outcome" => {
                segment["outcome"]["outcome"]["output"] =
                    serde_json::to_value(vec![InputContent::Text {
                        text: "forged previous answer".into(),
                    }])
                    .unwrap()
            }
            "boundary" => segment["last_event_seq"] = serde_json::json!(1),
            "missing_boundary" => {
                segment.as_object_mut().unwrap().remove("last_event_seq");
            }
            _ => unreachable!(),
        }
        assert!(
            StateStoreCheckpoint::from_json(
                &image.to_string(),
                &scope(),
                &canonical_digest(&image)
            )
            .is_err(),
            "accepted {mutation}"
        );
    }
}

#[tokio::test]
async fn remote_stop_is_consumed_as_a_recoverable_interruption() {
    let fixture = agent_support::Fixture::new(agent_support::Response::Text, true);
    let owner = fixture.agent();
    let handle = fixture.started(&owner, "request").await;
    fixture.model.entered.notified().await;
    let remote = fixture.agent();
    let stop = command(
        "remote-stop",
        ControlAction::Stop {
            cause: InterruptionCause::HostShutdown,
        },
    );
    let receipt = completed(
        remote
            .submit_control_command(handle.run_id().clone(), stop, context())
            .await
            .unwrap(),
    );
    assert!(receipt.processed_segment_id.is_none());
    let outcome = completed(
        tokio::time::timeout(Duration::from_secs(3), handle.outcome(&context()))
            .await
            .unwrap()
            .unwrap(),
    );
    assert!(
        matches!(outcome.result, OutcomeResult::Interrupted { interruption } if interruption.cause == InterruptionCause::HostShutdown)
    );
    let processed = completed(
        remote
            .get_control_receipt(handle.run_id(), &receipt.command_id, &context())
            .await
            .unwrap(),
    );
    assert_eq!(
        processed.processed_segment_id.as_ref(),
        Some(handle.segment_id())
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

struct StopPolicy(InterruptionAction);
impl InterruptionPolicy for StopPolicy {
    fn identity(&self) -> VersionedRef {
        reference("stop-policy")
    }
    fn decide<'a>(&'a self, _: &'a InterruptionInfo) -> PortFuture<'a, InterruptionDecision> {
        Box::pin(async move {
            Ok(InterruptionDecision {
                action: self.0,
                app_state: None,
            })
        })
    }
}
#[tokio::test]
async fn durable_stop_records_custom_pause_cancel_and_fail_decisions() {
    for (action, expected) in [
        (InterruptionAction::Pause, RunStatus::Interrupted),
        (InterruptionAction::Cancel, RunStatus::Cancelled),
        (InterruptionAction::Fail, RunStatus::Failed),
    ] {
        let fixture = agent_support::Fixture::new(agent_support::Response::Text, true);
        let mut bindings = fixture.bindings();
        bindings.interruption_policy = Some(InterruptionPolicyBinding {
            policy: std::sync::Arc::new(StopPolicy(action)),
            configuration: JsonObject::new(),
            app_state_schema: None,
            timeout_ms: 1000.try_into().unwrap(),
        });
        let owner = create_agent(agent_support::profile(), bindings).unwrap();
        let handle = fixture.started(&owner, "request").await;
        fixture.model.entered.notified().await;
        let remote = fixture.agent();
        let receipt = completed(
            remote
                .submit_control_command(
                    handle.run_id().clone(),
                    command(
                        "stop",
                        ControlAction::Stop {
                            cause: InterruptionCause::SegmentStopped,
                        },
                    ),
                    context(),
                )
                .await
                .unwrap(),
        );
        let outcome = completed(
            tokio::time::timeout(Duration::from_secs(3), handle.outcome(&context()))
                .await
                .unwrap()
                .unwrap(),
        );
        assert_eq!(outcome.result.status(), expected);
        let completed = completed(
            remote
                .get_control_receipt(handle.run_id(), &receipt.command_id, &context())
                .await
                .unwrap(),
        );
        assert_eq!(
            completed.processed_segment_id.as_ref(),
            Some(handle.segment_id())
        );
        assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn active_expiry_is_pending_before_the_deadline_and_consumed_when_the_budget_ends() {
    let fixture = agent_support::Fixture::new(agent_support::Response::Text, true);
    let mut profile = agent_support::profile();
    profile.limits.max_elapsed_ms = 100.try_into().unwrap();
    let owner = create_agent(profile, fixture.bindings()).unwrap();
    let handle = fixture.started(&owner, "request").await;
    fixture.model.entered.notified().await;
    let remote = fixture.agent();
    let receipt = completed(
        remote
            .submit_control_command(
                handle.run_id().clone(),
                command("expire", ControlAction::Expire),
                context(),
            )
            .await
            .unwrap(),
    );
    assert!(receipt.processed_segment_id.is_none());
    assert!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .outcome
            .is_none()
    );
    tokio::time::advance(Duration::from_millis(101)).await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert!(matches!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    ));
    let processed = completed(
        remote
            .get_control_receipt(handle.run_id(), &receipt.command_id, &context())
            .await
            .unwrap(),
    );
    assert_eq!(
        processed.processed_segment_id.as_ref(),
        Some(handle.segment_id())
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn denied_worker_processing_and_receipt_reads_leave_the_pending_command_unchanged() {
    let mut fixture = Fixture::new(Mode::Approval);
    fixture.profile.limits.max_elapsed_ms = 100.try_into().unwrap();
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    fixture.outcome(&handle).await;
    let receipt = completed(
        agent
            .submit_control_command(
                handle.run_id().clone(),
                command("expire", ControlAction::Expire),
                context(),
            )
            .await
            .unwrap(),
    );
    tokio::time::advance(Duration::from_millis(101)).await;
    fixture
        .policy
        .deny_control_processing
        .store(true, Ordering::SeqCst);
    fixture.policy.deny_details.store(true, Ordering::SeqCst);
    let before = fixture.base.store.export_checkpoint(&scope()).unwrap();
    assert_eq!(
        agent
            .process_control_command(
                handle.run_id().clone(),
                receipt.command_id.clone(),
                context()
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(
        agent
            .get_control_receipt(handle.run_id(), &receipt.command_id, &context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(
        fixture
            .base
            .store
            .export_checkpoint(&scope())
            .unwrap()
            .digest(),
        before.digest()
    );
    fixture
        .policy
        .deny_control_processing
        .store(false, Ordering::SeqCst);
    fixture.policy.deny_details.store(false, Ordering::SeqCst);
    assert!(
        completed(
            agent
                .process_control_command(handle.run_id().clone(), receipt.command_id, context())
                .await
                .unwrap()
        )
        .processed_segment_id
        .is_some()
    );
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn idle_deadline_observation_does_not_settle_waits_or_change_saved_state() {
    let mut fixture = Fixture::new(Mode::Approval);
    fixture.profile.limits.max_elapsed_ms = 100.try_into().unwrap();
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    let waiting = fixture.outcome(&handle).await;
    let before = fixture.base.store.export_checkpoint(&scope()).unwrap();
    let usage = fixture.saved(&handle).await.snapshot.usage;
    assert!(!completed(agent.get_run(handle.run_id(), &context()).await.unwrap()).deadline_expired);
    let saved = fixture.saved(&handle).await;
    let remaining =
        (saved.snapshot.timing.deadline_at_ms - fixture.base.clock.now().unwrap().utc_ms) as u64;
    tokio::time::advance(Duration::from_millis(remaining - 1)).await;
    assert!(!completed(agent.get_run(handle.run_id(), &context()).await.unwrap()).deadline_expired);
    tokio::time::advance(Duration::from_millis(1)).await;
    for _ in 0..2 {
        let view = completed(agent.get_run(handle.run_id(), &context()).await.unwrap());
        assert_eq!(view.status, RunStatus::Waiting);
        assert!(view.deadline_expired);
        assert_eq!(view.usage, usage);
    }
    assert_eq!(
        fixture
            .base
            .store
            .export_checkpoint(&scope())
            .unwrap()
            .digest(),
        before.digest()
    );
    assert_eq!(fixture.outcome(&handle).await, waiting);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    completed(
        agent
            .submit_control_command(
                handle.run_id().clone(),
                command("expire-observed", ControlAction::Expire),
                context(),
            )
            .await
            .unwrap(),
    );
    let view = completed(agent.get_run(handle.run_id(), &context()).await.unwrap());
    assert_eq!(view.status, RunStatus::Exhausted);
    assert!(!view.deadline_expired);
}

#[tokio::test(start_paused = true)]
async fn interrupted_deadline_observation_is_read_only_at_the_original_deadline() {
    let fixture = agent_support::Fixture::new(agent_support::Response::Text, true);
    let mut profile = agent_support::profile();
    profile.limits.max_elapsed_ms = 100.try_into().unwrap();
    let agent = create_agent(profile, fixture.bindings()).unwrap();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    completed(
        handle
            .stop_execution(InterruptionCause::HostShutdown, &context())
            .await
            .unwrap(),
    );
    let interrupted = completed(handle.outcome(&context()).await.unwrap());
    let before = fixture.store.export_checkpoint(&scope()).unwrap();
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    let remaining =
        (saved.snapshot.timing.deadline_at_ms - fixture.clock.now().unwrap().utc_ms) as u64;
    tokio::time::advance(Duration::from_millis(remaining)).await;
    let view = completed(agent.get_run(handle.run_id(), &context()).await.unwrap());
    assert_eq!(view.status, RunStatus::Interrupted);
    assert!(view.deadline_expired);
    assert_eq!(
        fixture.store.export_checkpoint(&scope()).unwrap().digest(),
        before.digest()
    );
    assert_eq!(
        completed(handle.outcome(&context()).await.unwrap()),
        interrupted
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}
```

## `crates/wickle/tests/agent_tool_loop.rs`

```rust
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
```

## `crates/wickle/tests/context_projection.rs`

```rust
//! Pinned prompts, provenance, protected-input boundaries, and atomic context selection.

use serde_json::{Value, json};
use std::collections::BTreeSet;
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn versioned(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
fn record(value: &str) -> RecordRef {
    RecordRef {
        record_id: id(value),
        revision: 1,
        digest: canonical_digest(&json!(value)),
    }
}
fn compiled_tool(name: &str) -> CompiledTool {
    let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    SchemaCompiler::new().compile(ToolDescriptor {
        tool:versioned(name), name:id(name), description:format!("{name} records"),
        input_schema:json!({"type":"object","properties":{"query":{"type":"string"},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters:vec!["query".into()], system_bindings:None,
        output_schema:json!({"type":"string"}), side_effect:ToolSideEffect::ReadOnly,
        concurrency:ToolConcurrency::Serial, retry:ToolRetryPolicy::Never, reconcile:false,
        max_output_bytes:4096.try_into().unwrap(),
    }, &registry).unwrap()
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
                    version: Some(reference.version.clone().unwrap_or_else(|| id("1"))),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(
                    &json!({"id":reference.id,"version":reference.version}),
                ),
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

fn route() -> ResolvedModelRoute {
    ResolvedModelRoute {
        binding: versioned("model"),
        catalog_revision: id("catalog"),
        routing_policy_revision: id("policy"),
        requested_model: id("model"),
        model_id: id("model"),
        model_version: id("release"),
        version_semantics: VersionSemantics::Pinned,
        provider: id("provider"),
        target: JsonObject::new(),
        deployment_revision: None,
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        adapter: versioned("adapter"),
        capability_revision: id("capabilities"),
        connection_ref: versioned("connection"),
    }
}

struct Fixture {
    scope: Scope,
    profile: ResolvedProfile,
    prompt: PromptSnapshot,
    prompt_digest: JsonDigest,
    tools: Vec<CompiledTool>,
    skill: SkillManifest,
    request: RunRequest,
    run_id: Id,
    step_id: Id,
    request_message_id: Id,
}

impl Fixture {
    async fn new() -> Self {
        let scope = Scope {
            tenant_id: id("tenant"),
            workspace_id: id("workspace"),
            user_id: None,
        };
        let profile=AgentProfile::from_json(r#"{
            "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
            "name":"Assistant","description":"Context fixture","instructions":{"text":"Profile instructions"},
            "model_binding":"model","tools":[{"tool_id":"search","version":"1"},{"tool_id":"read","version":"1"}],
            "skills":[{"skill_id":"analysis","version":"1"}],"connectors":[],
            "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
            "limits":{"max_model_calls":4,"max_tool_attempts":4,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
        }"#).unwrap();
        let profile = ProfileValidator::new(&Catalog)
            .validate(&profile, &scope)
            .await
            .unwrap();
        let tools = vec![compiled_tool("search"), compiled_tool("read")];
        let skill = SkillManifest {
            skill: versioned("analysis"),
            name: "Analysis".into(),
            description: "Analyze evidence".into(),
            manifest_digest: canonical_digest(&json!("analysis manifest")),
        };
        let bindings = profile
            .profile()
            .tools
            .iter()
            .cloned()
            .zip(tools.iter().cloned())
            .map(|(selection, compiled)| PromptToolBinding {
                selection,
                compiled,
            })
            .collect();
        let prompt = PromptSnapshot::create(
            &profile,
            vec!["Host rule A".into(), "Host rule B".into()],
            None,
            bindings,
            vec![skill.clone()],
        )
        .unwrap();
        let prompt_digest = prompt.digest();
        Self {
            scope,
            profile,
            prompt,
            prompt_digest,
            tools,
            skill,
            request: RunRequest {
                request_id: id("user-request"),
                session_id: id("session"),
                input: vec![InputContent::Text {
                    text: "Current requested work".into(),
                }],
                trigger: RunTrigger::User {},
                model_options: JsonObject::new(),
                max_output_tokens: None,
                output_contract: None,
            },
            run_id: id("current-run"),
            step_id: id("step"),
            request_message_id: id("current-message"),
        }
    }
    fn current_message(&self, sequence: u64) -> Message {
        Message {
            source_model_request_id: None,
            message_id: self.request_message_id.clone(),
            run_id: self.run_id.clone(),
            sequence: sequence.try_into().unwrap(),
            role: MessageRole::User,
            content: self
                .request
                .input
                .iter()
                .cloned()
                .map(|content| ContentBlock::Content { content })
                .collect(),
            origin: MessageOrigin::User,
            visibility: Visibility::UserAndModel,
        }
    }
    fn input<'a>(
        &'a self,
        transcript: &'a [Message],
        items: &'a [ContextItem],
        opaque: &'a [ScopedOpaque],
    ) -> ProjectionInput<'a> {
        ProjectionInput {
            tool_contracts: &[],
            profile: &self.profile,
            scope: &self.scope,
            run_id: &self.run_id,
            model_step_id: &self.step_id,
            current_request: &self.request,
            current_request_message_id: &self.request_message_id,
            transcript,
            context_items: items,
            opaque_records: opaque,
            expected_prompt_digest: &self.prompt_digest,
            request_id: self.step_id.clone(),
            purpose: ModelPurpose::Agent,
            route: route(),
            output: ModelOutput::Text {},
            max_output_tokens: 128.try_into().unwrap(),
            options: JsonObject::new(),
            response_limits: ModelResponseLimits {
                max_input_bytes: 100_000,
                max_response_bytes: 4096,
                max_delta_bytes: 1024,
                max_events: 32,
                max_tool_calls: 4,
            },
            limits: ProjectionLimits {
                max_bytes: 100_000,
                max_items: 100,
            },
        }
    }
    fn item(&self, name: &str, origin: ContextOrigin, priority: ContextPriority) -> ContextItem {
        ContextItem::new(
            id(name),
            origin,
            versioned(name),
            self.scope.clone(),
            vec![InputContent::Text {
                text: format!("data for {name}"),
            }],
            ContextLifetime::Run {
                run_id: self.run_id.clone(),
            },
            priority,
        )
    }
}

fn message(
    run: &str,
    sequence: u64,
    role: MessageRole,
    origin: MessageOrigin,
    content: Vec<ContentBlock>,
) -> Message {
    Message {
        source_model_request_id: None,
        message_id: id(&format!("message-{sequence}")),
        run_id: id(run),
        sequence: sequence.try_into().unwrap(),
        role,
        origin,
        content,
        visibility: Visibility::UserAndModel,
    }
}

#[tokio::test]
async fn host_model_options_reach_the_port_without_becoming_prompt_content() {
    use std::{sync::Mutex, time::Duration};
    struct ObserveOptions(Mutex<Option<JsonObject>>);
    impl ModelPort for ObserveOptions {
        fn binding(&self) -> ModelPortBinding {
            let route = route();
            ModelPortBinding {
                provider: route.provider,
                adapter: route.adapter,
                connection_ref: route.connection_ref,
            }
        }
        fn generate<'a>(
            &'a self,
            request: &'a ModelRequest,
            _: &'a ModelCallContext,
        ) -> PortStream<'a, ModelEvent> {
            *self.0.lock().unwrap() = Some(request.options.clone());
            Box::pin(futures_util::stream::iter([Ok(
                ModelEvent::ResponseCompleted {
                    finish: ModelFinish::Stop,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![],
                },
            )]))
        }
    }
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let assembler = ContextAssembler::new();
    let baseline = assembler
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let options = object(json!({"reasoning_effort":"high", "provider_mode":{"budget":32}}));
    let mut input = fixture.input(&transcript, &[], &[]);
    input.options = options.clone();
    let projected = assembler.project(&fixture.prompt, input).unwrap();
    assert_eq!(projected.request.messages, baseline.request.messages);
    assert_eq!(projected.request.tools, baseline.request.tools);
    assert_ne!(projected.request.digest(), baseline.request.digest());
    let port = ObserveOptions(Mutex::new(None));
    let call = ModelCallContext {
        attempt_id: id("attempt"),
        run_id: fixture.run_id.clone(),
        scope: fixture.scope.clone(),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(1),
    };
    collect_model_response(&projected.request, port.generate(&projected.request, &call))
        .await
        .unwrap();
    assert_eq!(*port.0.lock().unwrap(), Some(options.clone()));
    let mut limited = fixture.input(&transcript, &[], &[]);
    limited.options = options;
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len();
    assert_eq!(
        assembler
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}
fn text(value: &str) -> ContentBlock {
    ContentBlock::Content {
        content: InputContent::Text { text: value.into() },
    }
}
fn round(run: &str, sequence: u64, label: &str, body: &str, tool: &CompiledTool) -> Vec<Message> {
    let call_message = id(&format!("message-{sequence}"));
    let call = ToolCall {
        provider_arguments: None,
        call_id: id(label),
        model_request_id: id(&format!("request-{label}")),
        provider_call_id: id(&format!("provider-{label}")),
        tool_name: id("search"),
        model_inputs: object(json!({"query":label})),
        descriptor_digest: Some(tool.descriptor_digest().clone()),
        bound_input_ref: Some(record("bound-private-input")),
    };
    let result = ToolResult {
        call_id: call.call_id.clone(),
        call_message_id: call_message,
        status: ToolResultStatus::Failed,
        effect: ToolEffect::NotApplied,
        content: vec![InputContent::Text { text: body.into() }],
        effect_receipt_ref: Some(record("private-effect-receipt")),
        skill_ref: None,
        error: Some(Failure {
            code: id("unavailable"),
            diagnostic_ref: Some(record("private-diagnostic")),
        }),
    };
    vec![
        message(
            run,
            sequence,
            MessageRole::Assistant,
            MessageOrigin::Model,
            vec![ContentBlock::ToolCall { call }],
        ),
        message(
            run,
            sequence + 1,
            MessageRole::Tool,
            MessageOrigin::Tool,
            vec![ContentBlock::ToolResult { result }],
        ),
    ]
}

#[tokio::test]
async fn host_profile_and_skill_prefixes_are_pinned_and_tools_use_only_compiled_model_schemas() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert_eq!(projected.prompt_digest, fixture.prompt_digest);
    assert_eq!(
        projected.request.messages[0],
        ModelMessage {
            role: ModelRole::System,
            content: vec![
                ModelContent::Text {
                    text: "Host rule A".into()
                },
                ModelContent::Text {
                    text: "Host rule B".into()
                }
            ]
        }
    );
    assert_eq!(
        projected.request.messages[1],
        ModelMessage {
            role: ModelRole::System,
            content: vec![ModelContent::Text {
                text: "Profile instructions".into()
            }]
        }
    );
    assert_eq!(
        projected.request.tools,
        fixture
            .tools
            .iter()
            .map(CompiledTool::to_model_tool)
            .collect::<Vec<_>>()
    );
    for tool in &projected.request.tools {
        assert!(
            tool.model_input_schema["properties"]
                .get("workspace_id")
                .is_none()
        );
    }
    assert_eq!(
        projected.selected_message_ids,
        vec![fixture.request_message_id.clone()]
    );
    projected.request.validate().unwrap();
    let restored = PromptSnapshot::restore(
        &serde_json::to_string(&fixture.prompt).unwrap(),
        &fixture.prompt_digest,
        &fixture.profile,
        &fixture.scope,
    )
    .unwrap();
    let repeated = ContextAssembler::new()
        .project(&restored, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert_eq!(projected.request, repeated.request);
}

#[tokio::test]
async fn current_request_is_exactly_the_persisted_message_and_is_never_silently_replaced() {
    let fixture = Fixture::new().await;
    let current = fixture.current_message(1);
    for transcript in [
        vec![],
        vec![Message {
            content: vec![text("A different request")],
            ..current.clone()
        }],
        vec![Message {
            visibility: Visibility::Internal,
            ..current.clone()
        }],
        vec![Message {
            run_id: id("other-run"),
            ..current.clone()
        }],
    ] {
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
    let transcript = vec![current];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert_eq!(
        projected.request.messages.last().unwrap(),
        &ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Current requested work".into()
            }]
        }
    );
    assert_eq!(
        projected
            .selected_message_ids
            .iter()
            .filter(|message_id| *message_id == &fixture.request_message_id)
            .count(),
        1
    );
}

#[tokio::test]
async fn tool_projection_keeps_model_arguments_and_public_observations_without_execution_records() {
    let fixture = Fixture::new().await;
    let mut transcript = vec![fixture.current_message(1)];
    transcript.extend(round(
        "current-run",
        2,
        "call",
        "public observation",
        &fixture.tools[0],
    ));
    let mut internal = message(
        "current-run",
        4,
        MessageRole::User,
        MessageOrigin::Recovery,
        vec![ContentBlock::Content {
            content: InputContent::Json {
                value: json!({"system_inputs":{"workspace_id":"11111111-1111-4111-8111-111111111111"},"execution_args":{"query":"call","workspace_id":"11111111-1111-4111-8111-111111111111"},"raw_diagnostic":"private"}),
            },
        }],
    );
    internal.visibility = Visibility::Internal;
    transcript.push(internal);
    let original = transcript.clone();
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let contents: Vec<_> = projected
        .request
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .collect();
    let call = contents
        .iter()
        .find_map(|content| match content {
            ModelContent::ToolCall {
                provider_call_id,
                name,
                arguments,
            } => Some((provider_call_id, name, arguments)),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        call,
        (
            &id("provider-call"),
            &id("search"),
            &object(json!({"query":"call"}))
        )
    );
    let result = contents
        .iter()
        .find_map(|content| match content {
            ModelContent::ToolResult {
                provider_call_id,
                content,
            } => Some((provider_call_id, content)),
            _ => None,
        })
        .unwrap();
    assert_eq!(result.0, &id("provider-call"));
    assert_eq!(
        result.1,
        &json!({"status":"failed","effect":"not_applied","content":[{"type":"text","text":"public observation"}],"error":{"code":"unavailable"}})
    );
    assert_eq!(projected.request.messages.len(), 6);
    assert_eq!(
        projected.selected_message_ids,
        vec![
            fixture.request_message_id.clone(),
            id("message-2"),
            id("message-3")
        ]
    );
    assert_eq!(transcript, original);
    projected.request.validate().unwrap();
}

#[tokio::test]
async fn explicit_unknown_correction_projects_one_confirmed_result_without_changing_history() {
    let fixture = Fixture::new().await;
    let mut transcript = vec![fixture.current_message(1)];
    transcript.extend(round(
        "current-run",
        2,
        "call",
        "uncertain",
        &fixture.tools[0],
    ));
    let ContentBlock::ToolResult { result } = &mut transcript[2].content[0] else {
        unreachable!()
    };
    result.status = ToolResultStatus::Unknown;
    result.effect = ToolEffect::Unknown;
    let prior_digest = canonical_digest(&serde_json::to_value(&*result).unwrap());
    let confirmed = ToolResult {
        status: ToolResultStatus::Succeeded,
        effect: ToolEffect::Applied,
        content: vec![InputContent::Text {
            text: "confirmed receipt".into(),
        }],
        error: None,
        ..result.clone()
    };
    transcript.push(message(
        "current-run",
        4,
        MessageRole::Tool,
        MessageOrigin::Tool,
        vec![ContentBlock::ToolResultCorrection {
            previous_message_id: id("message-3"),
            previous_result_digest: prior_digest,
            result: confirmed,
        }],
    ));
    let original = transcript.clone();
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let results: Vec<_> = projected
        .request
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| {
            if let ModelContent::ToolResult {
                provider_call_id,
                content,
            } = content
            {
                Some((provider_call_id, content))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        results,
        vec![(
            &id("provider-call"),
            &json!({"status":"succeeded","effect":"applied","content":[{"type":"text","text":"confirmed receipt"}]})
        )]
    );
    assert!(projected.selected_message_ids.contains(&id("message-4")));
    assert!(!projected.selected_message_ids.contains(&id("message-3")));
    assert_eq!(transcript, original);
    projected.request.validate().unwrap();

    for fault in 0..7 {
        let mut changed = transcript.clone();
        match fault {
            0 => {
                let ContentBlock::ToolResultCorrection {
                    previous_result_digest,
                    ..
                } = &mut changed[3].content[0]
                else {
                    unreachable!()
                };
                *previous_result_digest = canonical_digest(&json!("different"));
            }
            1 => {
                let ContentBlock::ToolResultCorrection { result, .. } = &mut changed[3].content[0]
                else {
                    unreachable!()
                };
                result.call_id = id("foreign-call");
            }
            2 => changed[3].origin = MessageOrigin::User,
            3 => {
                let ContentBlock::ToolResult { result } = &mut changed[2].content[0] else {
                    unreachable!()
                };
                result.status = ToolResultStatus::Succeeded;
                result.effect = ToolEffect::Applied;
                let digest = canonical_digest(&serde_json::to_value(&*result).unwrap());
                let ContentBlock::ToolResultCorrection {
                    previous_result_digest,
                    ..
                } = &mut changed[3].content[0]
                else {
                    unreachable!()
                };
                *previous_result_digest = digest;
            }
            4 => {
                let mut duplicate = changed[3].clone();
                duplicate.message_id = id("second-correction");
                duplicate.sequence = 5.try_into().unwrap();
                changed.push(duplicate);
            }
            5 => {
                changed[3].sequence = 5.try_into().unwrap();
                changed.insert(
                    3,
                    message(
                        "current-run",
                        4,
                        MessageRole::Assistant,
                        MessageOrigin::Model,
                        vec![text("premature continuation")],
                    ),
                );
            }
            6 => {
                let ContentBlock::ToolResult { result } = &mut changed[2].content[0] else {
                    unreachable!()
                };
                result.effect = ToolEffect::Applied;
                let digest = canonical_digest(&serde_json::to_value(&*result).unwrap());
                let ContentBlock::ToolResultCorrection {
                    previous_result_digest,
                    result,
                    ..
                } = &mut changed[3].content[0]
                else {
                    unreachable!()
                };
                *previous_result_digest = digest;
                result.effect = ToolEffect::NotApplied;
            }
            _ => unreachable!(),
        }
        let error = ContextAssembler::new()
            .project(&fixture.prompt, fixture.input(&changed, &[], &[]))
            .unwrap_err();
        assert_eq!(error.path, "transcript.tool_correction", "fault {fault}");
    }
}

#[tokio::test]
async fn data_context_never_adds_system_authority_or_replaces_the_pinned_prefix() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let items = vec![
        fixture.item(
            "retrieval",
            ContextOrigin::Retrieval,
            ContextPriority::Required,
        ),
        fixture.item("memory", ContextOrigin::Memory, ContextPriority::Required),
        fixture.item(
            "verification",
            ContextOrigin::Verification,
            ContextPriority::Required,
        ),
    ];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &items, &[]))
        .unwrap();
    assert_eq!(projected.selected_context_ids.len(), 3);
    let system = |request: ModelRequest| {
        request
            .messages
            .into_iter()
            .filter(|message| message.role == ModelRole::System)
            .collect::<Vec<_>>()
    };
    assert_eq!(system(projected.request), system(baseline.request));
    let mut forged = serde_json::to_value(&items[0]).unwrap();
    forged["origin"] = json!("host");
    assert!(serde_json::from_value::<ContextItem>(forged).is_err());
}

#[tokio::test]
async fn context_items_validate_scope_and_digest_and_exclude_other_run_or_step_lifetimes() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let original = fixture.item(
        "source",
        ContextOrigin::Retrieval,
        ContextPriority::Required,
    );
    let mut wrong_scope = original.clone();
    wrong_scope.scope.tenant_id = id("other-tenant");
    let mut wrong_run = original.clone();
    wrong_run.lifetime = ContextLifetime::Run {
        run_id: id("other-run"),
    };
    let mut wrong_step = original.clone();
    wrong_step.lifetime = ContextLifetime::Step {
        run_id: fixture.run_id.clone(),
        model_step_id: id("other-step"),
    };
    let mut changed_content = original;
    changed_content.content = vec![InputContent::Text {
        text: "changed data".into(),
    }];
    let with_valid_digest = |item: ContextItem| {
        ContextItem::new(
            item.item_id,
            item.origin,
            item.source_ref,
            item.scope,
            item.content,
            item.lifetime,
            item.priority_class,
        )
    };
    for item in [with_valid_digest(wrong_scope), changed_content] {
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[item], &[]))
                .is_err()
        );
    }
    for item in [with_valid_digest(wrong_run), with_valid_digest(wrong_step)] {
        let items = vec![item];
        let projected = ContextAssembler::new()
            .project(&fixture.prompt, fixture.input(&transcript, &items, &[]))
            .unwrap();
        assert!(projected.selected_context_ids.is_empty());
        assert_eq!(projected.dropped_context_ids, vec![id("source")]);
    }
}

#[tokio::test]
async fn a_current_tool_round_cannot_be_incomplete_or_have_an_unpaired_result() {
    let fixture = Fixture::new().await;
    let complete = round("current-run", 2, "call", "observation", &fixture.tools[0]);
    for transcript in [
        vec![fixture.current_message(1), complete[0].clone()],
        vec![fixture.current_message(1), complete[1].clone()],
        vec![
            fixture.current_message(1),
            complete[0].clone(),
            message(
                "current-run",
                3,
                MessageRole::User,
                MessageOrigin::User,
                vec![text("interleaved")],
            ),
            Message {
                sequence: 4.try_into().unwrap(),
                ..complete[1].clone()
            },
        ],
    ] {
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
}

#[tokio::test]
async fn raw_transcript_system_roles_cannot_extend_or_replace_the_pinned_prefix() {
    let fixture = Fixture::new().await;
    for origin in [
        MessageOrigin::Host,
        MessageOrigin::Profile,
        MessageOrigin::Retrieval,
        MessageOrigin::Memory,
    ] {
        let transcript = vec![
            fixture.current_message(1),
            message(
                "current-run",
                2,
                MessageRole::System,
                origin,
                vec![text("untrusted transcript instructions")],
            ),
        ];
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
}

#[tokio::test]
async fn visibility_filtering_and_message_references_cannot_separate_a_tool_call_from_its_result() {
    let fixture = Fixture::new().await;
    for hidden in [0, 1] {
        let mut messages = round("current-run", 2, "call", "observation", &fixture.tools[0]);
        messages[hidden].visibility = Visibility::Internal;
        let mut transcript = vec![fixture.current_message(1)];
        transcript.extend(messages);
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
    let mut messages = round("current-run", 2, "call", "observation", &fixture.tools[0]);
    let ContentBlock::ToolResult { result } = &mut messages[1].content[0] else {
        unreachable!()
    };
    result.call_message_id = id("some-other-call-message");
    let mut transcript = vec![fixture.current_message(1)];
    transcript.extend(messages);
    assert!(
        ContextAssembler::new()
            .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
            .is_err()
    );
}

#[tokio::test]
async fn bounded_selection_discards_an_older_run_as_a_whole_and_retains_the_latest_complete_round()
{
    let fixture = Fixture::new().await;
    let large = "old ".repeat(512);
    let mut transcript = vec![message(
        "old-run",
        1,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Old work")],
    )];
    transcript.extend(round("old-run", 2, "old-call", &large, &fixture.tools[0]));
    transcript.push(message(
        "old-run",
        4,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Old answer")],
    ));
    transcript.push(message(
        "recent-run",
        5,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Recent work")],
    ));
    transcript.extend(round(
        "recent-run",
        6,
        "recent-call",
        "Recent observation",
        &fixture.tools[0],
    ));
    transcript.push(message(
        "recent-run",
        8,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Recent answer")],
    ));
    transcript.push(fixture.current_message(9));
    let original = transcript.clone();
    let full = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let full_bytes = serde_json::to_vec(&full.request).unwrap().len();
    let mut bounded = fixture.input(&transcript, &[], &[]);
    bounded.limits.max_bytes = full_bytes - large.len() / 2;
    let byte_limit = bounded.limits.max_bytes;
    let selected = ContextAssembler::new()
        .project(&fixture.prompt, bounded)
        .unwrap();
    assert_eq!(
        selected.selected_message_ids,
        vec![
            id("message-5"),
            id("message-6"),
            id("message-7"),
            id("message-8"),
            fixture.request_message_id.clone()
        ]
    );
    assert_eq!(
        selected.dropped_message_ids,
        vec![
            id("message-1"),
            id("message-2"),
            id("message-3"),
            id("message-4")
        ]
    );
    assert!(serde_json::to_vec(&selected.request).unwrap().len() <= byte_limit);
    selected.request.validate().unwrap();
    assert_eq!(transcript, original);
}

#[tokio::test]
async fn required_current_work_and_prefix_report_budget_exhaustion_instead_of_truncation() {
    let fixture = Fixture::new().await;
    let current = vec![fixture.current_message(1)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&current, &[], &[]))
        .unwrap();
    let count = baseline
        .request
        .messages
        .iter()
        .map(|message| message.content.len())
        .sum::<usize>()
        + baseline.request.tools.len();
    let mut item_limited = fixture.input(&current, &[], &[]);
    item_limited.limits.max_items = count - 1;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, item_limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
    let mut input_limited = fixture.input(&current, &[], &[]);
    input_limited.response_limits.max_input_bytes = 1;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, input_limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
    let mut transcript = current;
    transcript.extend(round(
        "current-run",
        2,
        "current-call",
        &"data ".repeat(400),
        &fixture.tools[0],
    ));
    let mut bounded = fixture.input(&transcript, &[], &[]);
    bounded.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 64;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, bounded)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn optional_context_can_be_removed_but_required_context_cannot_be_silently_dropped() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let mut optional = fixture.item(
        "optional",
        ContextOrigin::Retrieval,
        ContextPriority::Optional,
    );
    optional = ContextItem::new(
        optional.item_id,
        optional.origin,
        optional.source_ref,
        optional.scope,
        vec![InputContent::Text {
            text: "reference data ".repeat(1000),
        }],
        optional.lifetime,
        optional.priority_class,
    );
    let items = vec![optional.clone()];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let mut limited = fixture.input(&transcript, &items, &[]);
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 128;
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, limited)
        .unwrap();
    assert!(projected.selected_context_ids.is_empty());
    assert_eq!(projected.dropped_context_ids, vec![id("optional")]);
    let required = ContextItem::new(
        optional.item_id,
        optional.origin,
        optional.source_ref,
        optional.scope,
        optional.content,
        optional.lifetime,
        ContextPriority::Required,
    );
    let required_items = vec![required];
    let mut limited = fixture.input(&transcript, &required_items, &[]);
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 128;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn pinned_snapshot_rejects_other_scope_profile_and_recomputed_replacement_contents() {
    let fixture = Fixture::new().await;
    let mut foreign = fixture.scope.clone();
    foreign.tenant_id = id("other-tenant");
    assert!(
        fixture
            .prompt
            .validate_for(&fixture.profile, &foreign, &fixture.prompt_digest)
            .is_err()
    );
    let mut changed_profile = fixture.profile.profile().clone();
    changed_profile.version = id("2.0.0");
    let changed_profile = ProfileValidator::new(&Catalog)
        .validate(&changed_profile, &fixture.scope)
        .await
        .unwrap();
    assert!(
        fixture
            .prompt
            .validate_for(&changed_profile, &fixture.scope, &fixture.prompt_digest)
            .is_err()
    );
    let bindings = fixture
        .profile
        .profile()
        .tools
        .iter()
        .cloned()
        .zip(fixture.tools.iter().cloned())
        .map(|(selection, compiled)| PromptToolBinding {
            selection,
            compiled,
        })
        .collect();
    let replacement = PromptSnapshot::create(
        &fixture.profile,
        vec!["Replacement Host policy".into()],
        None,
        bindings,
        vec![fixture.skill.clone()],
    )
    .unwrap();
    assert!(
        replacement
            .validate_for(&fixture.profile, &fixture.scope, &fixture.prompt_digest)
            .is_err()
    );
    assert!(
        PromptSnapshot::restore(
            &serde_json::to_string(&replacement).unwrap(),
            &fixture.prompt_digest,
            &fixture.profile,
            &fixture.scope
        )
        .is_err()
    );
}

#[tokio::test]
async fn prompt_creation_rejects_unselected_tools_and_changed_skill_versions() {
    let fixture = Fixture::new().await;
    let mut extra = fixture
        .profile
        .profile()
        .tools
        .iter()
        .cloned()
        .zip(fixture.tools.iter().cloned())
        .map(|(selection, compiled)| PromptToolBinding {
            selection,
            compiled,
        })
        .collect::<Vec<_>>();
    extra.push(PromptToolBinding {
        selection: ToolBindingRef::Catalog(CatalogToolRef {
            tool_id: id("unselected"),
            version: id("1"),
            bindings: None,
            config: None,
        }),
        compiled: compiled_tool("unselected"),
    });
    assert!(
        PromptSnapshot::create(
            &fixture.profile,
            vec!["Host rule".into()],
            None,
            extra,
            vec![fixture.skill.clone()]
        )
        .is_err()
    );
    let bindings = fixture
        .profile
        .profile()
        .tools
        .iter()
        .cloned()
        .zip(fixture.tools.iter().cloned())
        .map(|(selection, compiled)| PromptToolBinding {
            selection,
            compiled,
        })
        .collect();
    let mut changed = fixture.skill.clone();
    changed.skill.version = id("2");
    assert!(
        PromptSnapshot::create(
            &fixture.profile,
            vec!["Host rule".into()],
            None,
            bindings,
            vec![changed]
        )
        .is_err()
    );
}

#[tokio::test]
async fn opaque_replay_requires_matching_protected_record_scope_provider_and_route() {
    let fixture = Fixture::new().await;
    let continuation =
        OpaqueContinuation::new(&route(), json!({"signature":"provider continuation"}));
    let reference = RecordRef {
        record_id: id("opaque"),
        revision: 1,
        digest: canonical_digest(&serde_json::to_value(&continuation).unwrap()),
    };
    let mut transcript = vec![fixture.current_message(1)];
    transcript.push(message(
        "current-run",
        2,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![ContentBlock::ProviderOpaque {
            provider: id("provider"),
            route_digest: route().digest(),
            data_ref: reference.clone(),
        }],
    ));
    let records = vec![ScopedOpaque {
        scope: fixture.scope.clone(),
        reference: reference.clone(),
        provider: id("provider"),
        continuation: continuation.clone(),
    }];
    let accepted = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &records))
        .unwrap();
    assert!(accepted.request.messages.iter().flat_map(|message|&message.content).any(|content|matches!(content,ModelContent::Opaque{continuation:found} if found==&continuation)));
    let mut changed = fixture.input(&transcript, &[], &records);
    changed.route.connection_ref.version = id("different-connection");
    assert!(
        ContextAssembler::new()
            .project(&fixture.prompt, changed)
            .is_err()
    );
    let mut wrong_scope = records.clone();
    wrong_scope[0].scope.tenant_id = id("other-tenant");
    assert!(
        ContextAssembler::new()
            .project(
                &fixture.prompt,
                fixture.input(&transcript, &[], &wrong_scope)
            )
            .is_err()
    );
    let mut wrong_provider = records.clone();
    wrong_provider[0].provider = id("other-provider");
    assert!(
        ContextAssembler::new()
            .project(
                &fixture.prompt,
                fixture.input(&transcript, &[], &wrong_provider)
            )
            .is_err()
    );
    let mut wrong_record = records;
    wrong_record[0].reference = record("different-record");
    assert!(
        ContextAssembler::new()
            .project(
                &fixture.prompt,
                fixture.input(&transcript, &[], &wrong_record)
            )
            .is_err()
    );
}

#[tokio::test]
async fn the_latest_tool_round_from_an_earlier_run_is_required_context() {
    let fixture = Fixture::new().await;
    let current = vec![fixture.current_message(5)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&current, &[], &[]))
        .unwrap();
    let mut transcript = vec![message(
        "previous-run",
        1,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Previous work")],
    )];
    transcript.extend(round(
        "previous-run",
        2,
        "previous-call",
        &"data ".repeat(400),
        &fixture.tools[0],
    ));
    transcript.push(message(
        "previous-run",
        4,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Previous answer")],
    ));
    transcript.extend(current);
    let unbounded = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert!(unbounded.selected_message_ids.contains(&id("message-2")));
    assert!(unbounded.selected_message_ids.contains(&id("message-3")));
    let mut limited = fixture.input(&transcript, &[], &[]);
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 64;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn an_older_unknown_effect_is_not_dropped_when_a_newer_round_exists() {
    let fixture = Fixture::new().await;
    let large = "unknown effect ".repeat(128);
    let mut transcript = vec![message(
        "unknown-run",
        1,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Earlier work")],
    )];
    let mut uncertain = round("unknown-run", 2, "unknown-call", &large, &fixture.tools[0]);
    let ContentBlock::ToolResult { result } = &mut uncertain[1].content[0] else {
        unreachable!()
    };
    result.status = ToolResultStatus::Unknown;
    result.effect = ToolEffect::Unknown;
    transcript.extend(uncertain);
    transcript.push(message(
        "unknown-run",
        4,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Effect not confirmed")],
    ));
    transcript.push(message(
        "recent-run",
        5,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Recent work")],
    ));
    transcript.extend(round(
        "recent-run",
        6,
        "recent-call",
        "Recent observation",
        &fixture.tools[0],
    ));
    transcript.push(message(
        "recent-run",
        8,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Recent answer")],
    ));
    transcript.push(fixture.current_message(9));
    let full = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert!(full.selected_message_ids.contains(&id("message-3")));
    let mut limited = fixture.input(&transcript, &[], &[]);
    limited.limits.max_bytes = serde_json::to_vec(&full.request).unwrap().len() - large.len() / 2;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn loaded_skill_context_uses_its_pinned_version_without_gaining_system_authority() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let loaded = ContextItem::new(
        id("loaded-skill"),
        ContextOrigin::Skill,
        fixture.skill.skill.clone(),
        fixture.scope.clone(),
        vec![InputContent::Text {
            text: "Loaded task instructions".into(),
        }],
        ContextLifetime::Run {
            run_id: fixture.run_id.clone(),
        },
        ContextPriority::Required,
    );
    let items = vec![loaded.clone()];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &items, &[]))
        .unwrap();
    assert_eq!(projected.selected_context_ids, vec![id("loaded-skill")]);
    let system = |request: ModelRequest| {
        request
            .messages
            .into_iter()
            .filter(|message| message.role == ModelRole::System)
            .collect::<Vec<_>>()
    };
    assert_eq!(system(projected.request), system(baseline.request));
    let changed = ContextItem::new(
        loaded.item_id,
        loaded.origin,
        VersionedRef {
            id: loaded.source_ref.id,
            version: id("2"),
        },
        loaded.scope,
        loaded.content,
        loaded.lifetime,
        loaded.priority_class,
    );
    assert!(
        ContextAssembler::new()
            .project(&fixture.prompt, fixture.input(&transcript, &[changed], &[]))
            .is_err()
    );
}

#[tokio::test]
async fn a_saved_prompt_from_another_assembler_is_rejected_even_with_its_authentic_digest() {
    let fixture = Fixture::new().await;
    let original = serde_json::to_value(&fixture.prompt).unwrap();
    let mut changed = original.clone();
    changed["assembler_version"] = json!("wickle.context-assembler.v999");
    let changed_digest = canonical_digest(&changed);
    assert_ne!(changed_digest, fixture.prompt_digest);
    assert_eq!(
        PromptSnapshot::restore(
            &changed.to_string(),
            &changed_digest,
            &fixture.profile,
            &fixture.scope
        )
        .unwrap_err()
        .code,
        ErrorCode::ContextMismatch
    );
    let restored = PromptSnapshot::restore(
        &original.to_string(),
        &fixture.prompt_digest,
        &fixture.profile,
        &fixture.scope,
    )
    .unwrap();
    assert_eq!(serde_json::to_value(restored).unwrap(), original);
}
```

## `crates/wickle/tests/execution_store.rs`

```rust
//! Atomic execution ownership, commands, rollback and checkpoint validation.
use serde_json::json;
use std::sync::Arc;
use wickle::*;
#[allow(dead_code)]
mod support;
use support::*;
#[path = "support/execution_store.rs"]
mod suite;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_store_atomically_claims_segments_and_consumes_controls() {
    suite::atomic_execution_contract(Arc::new(MemoryStateStore::new())).await;
}
#[tokio::test]
async fn checkpoint_preserves_history_and_rejects_corrupted_execution_identity() {
    let store = MemoryStateStore::new();
    store
        .admit(
            &scope(),
            admission("run", "request", "session", "input", "1").await,
        )
        .await
        .unwrap();
    let checkpoint = store.export_checkpoint(&scope()).unwrap();
    let encoded = serde_json::to_string(&checkpoint).unwrap();
    let restored = MemoryStateStore::from_checkpoint(
        StateStoreCheckpoint::from_json(&encoded, &scope(), &checkpoint.digest()).unwrap(),
    );
    assert_eq!(
        restored.read_execution(&scope(), &id("run")).await.unwrap(),
        store.read_execution(&scope(), &id("run")).await.unwrap()
    );
    let mut corrupt: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    corrupt["executions"][0]["segments"][0]["execution_principal_ref"] = json!("replacement");
    assert!(
        StateStoreCheckpoint::from_json(
            &corrupt.to_string(),
            &scope(),
            &canonical_digest(&corrupt)
        )
        .is_err()
    );
    // An authentic legacy-format graph remains readable, but has no invented actor/segment evidence.
    let mut legacy: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    legacy["schema_version"] = json!("wickle.state-store.v1");
    legacy.as_object_mut().unwrap().remove("executions");
    let restored = MemoryStateStore::from_checkpoint(
        StateStoreCheckpoint::from_json(&legacy.to_string(), &scope(), &canonical_digest(&legacy))
            .unwrap(),
    );
    assert_eq!(
        restored
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .run_id,
        id("run")
    );
    assert_eq!(
        restored
            .read_execution(&scope(), &id("run"))
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
    assert_eq!(
        restored
            .admit(
                &scope(),
                admission("other", "other", "other", "input", "1").await
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
}
#[tokio::test]
async fn recovery_acceptance_and_lease_are_one_transaction() {
    suite::atomic_recovery_contract(Arc::new(MemoryStateStore::new())).await;
}

#[tokio::test]
async fn version_two_rejects_missing_active_history_even_when_other_histories_remain() {
    let store = MemoryStateStore::new();
    for run in ["first", "second"] {
        store
            .admit(&scope(), admission(run, run, run, "input", "1").await)
            .await
            .unwrap();
    }
    let mut image = serde_json::to_value(store.export_checkpoint(&scope()).unwrap()).unwrap();
    image["executions"].as_array_mut().unwrap().remove(0);
    assert!(
        StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(&image))
            .is_err()
    );
}
#[tokio::test]
async fn restored_receipts_must_match_the_original_recovery_command_and_segment() {
    let store = Arc::new(MemoryStateStore::new());
    suite::atomic_recovery_contract(store.clone()).await;
    let image = serde_json::to_value(store.export_checkpoint(&scope()).unwrap()).unwrap();
    for kind in ["missing", "digest", "segment"] {
        let mut changed = image.clone();
        match kind {
            "missing" => changed["executions"][0]["accepted_commands"] = json!([]),
            "digest" => {
                changed["executions"][0]["accepted_commands"][0]["payload_digest"] =
                    json!(canonical_digest(&json!(false)))
            }
            _ => {
                changed["executions"][0]["accepted_commands"][0]["segment_id"] =
                    changed["executions"][0]["segments"][0]["segment_id"].clone()
            }
        }
        assert!(
            StateStoreCheckpoint::from_json(
                &changed.to_string(),
                &scope(),
                &canonical_digest(&changed)
            )
            .is_err(),
            "{kind}"
        );
    }
}

#[tokio::test]
async fn version_two_cannot_silently_reclassify_new_terminal_runs_as_legacy() {
    let store = Arc::new(MemoryStateStore::new());
    suite::atomic_execution_contract(store.clone()).await;
    store
        .admit(
            &scope(),
            admission("other", "other", "other", "input", "1").await,
        )
        .await
        .unwrap();
    let mut image = serde_json::to_value(store.export_checkpoint(&scope()).unwrap()).unwrap();
    let histories = image["executions"].as_array_mut().unwrap();
    let index = histories
        .iter()
        .position(|v| v["run_id"] == "atomic-run")
        .unwrap();
    histories.remove(index);
    assert!(
        StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(&image))
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_conflicting_submissions_keep_only_the_winning_request_snapshot() {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
    suite::conflicting_submissions_race([store.clone(), store]).await;
}
```

## `crates/wickle/tests/policy.rs`

```rust
//! Authorization behavior with injected Host policies and protected stored records.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::pending,
    num::NonZeroU64,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use serde_json::{Value, json};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn digest(value: &str) -> JsonDigest {
    canonical_digest(&json!(value))
}

fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    }
}

fn context(owner: Scope) -> ExecutionContext {
    ExecutionContext::new(
        ExecutionContextData {
            scope: owner,
            principal_ref: id("originator"),
            capability_grant_ref: id("member-grant"),
            trace_context: None,
            system_inputs: Some(SystemInputs::new(BTreeMap::from([(
                "private_host_value".into(),
                json!("protected-host-value"),
            )]))),
        },
        CancellationToken::new(),
    )
}

fn request(action: PolicyAction) -> PolicyRequest {
    PolicyRequest {
        owner_scope: scope(),
        resource_id: id("run"),
        action,
    }
}

#[derive(Clone)]
enum Behavior {
    Decision(PolicyDecision),
    Error,
    PanicBeforeFuture,
    PanicInFuture,
    Pending,
    CancelThenAllow,
}

#[derive(Clone)]
struct Observed {
    request: PolicyRequest,
    scope: Scope,
    principal: Id,
    grant: Id,
}

struct HostPolicy {
    behavior: Mutex<Behavior>,
    calls: AtomicUsize,
    observed: Mutex<Vec<Observed>>,
}

impl HostPolicy {
    fn new(behavior: Behavior) -> Arc<Self> {
        Arc::new(Self {
            behavior: Mutex::new(behavior),
            calls: AtomicUsize::new(0),
            observed: Mutex::new(Vec::new()),
        })
    }

    fn gate(self: &Arc<Self>) -> PolicyGate {
        PolicyGate::new(self.clone(), Duration::from_millis(50)).unwrap()
    }
}

impl PolicyPort for HostPolicy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.observed.lock().unwrap().push(Observed {
            request: request.clone(),
            scope: context.scope.clone(),
            principal: context.principal_ref.clone(),
            grant: context.capability_grant_ref.clone(),
        });
        let behavior = self.behavior.lock().unwrap().clone();
        if matches!(behavior, Behavior::PanicBeforeFuture) {
            panic!("policy callback panicked before returning a future");
        }
        Box::pin(async move {
            match behavior {
                Behavior::Decision(decision) => Ok(decision),
                Behavior::Error => Err(ContractError::new(
                    ErrorCode::InvalidContract,
                    "private-policy-diagnostic",
                )),
                Behavior::PanicInFuture => panic!("policy future panicked"),
                Behavior::Pending => pending().await,
                Behavior::CancelThenAllow => {
                    context.cancellation.cancel();
                    Ok(PolicyDecision::Allow {})
                }
                Behavior::PanicBeforeFuture => unreachable!(),
            }
        })
    }
}

#[derive(Default)]
struct OperationCounts {
    constructed: AtomicUsize,
    executed: AtomicUsize,
}

impl OperationCounts {
    async fn guarded(
        &self,
        gate: &PolicyGate,
        request: &PolicyRequest,
        context: &ExecutionContext,
        deadline: Option<Instant>,
        restriction: Option<PolicyDecision>,
    ) -> Result<Guarded<u32>, ContractError> {
        gate.guard(request, context, deadline, restriction, || {
            self.constructed.fetch_add(1, Ordering::SeqCst);
            async {
                self.executed.fetch_add(1, Ordering::SeqCst);
                Ok(42)
            }
        })
        .await
    }

    fn assert_calls(&self, expected: usize) {
        assert_eq!(self.constructed.load(Ordering::SeqCst), expected);
        assert_eq!(self.executed.load(Ordering::SeqCst), expected);
    }
}

#[tokio::test]
async fn every_control_boundary_requires_exact_tenant_workspace_and_user_scope() {
    let policy = HostPolicy::new(Behavior::Decision(PolicyDecision::Allow {}));
    let gate = policy.gate();
    let actions = [
        PolicyAction::ReadRun {},
        PolicyAction::ReadRunDetails {},
        PolicyAction::ReadArtifact {},
        PolicyAction::ReadEvents {},
        PolicyAction::ResumeRun {
            command: Box::new(ResumeCommand {
                run_id: id("run"),
                expected_revision: 0,
                command_id: id("resume-command"),
                action: ResumeAction::Recover {
                    recovery_ref: record("recovery"),
                },
            }),
            binding_digest: None,
        },
        PolicyAction::CancelRun {},
    ];
    let calls = OperationCounts::default();
    for owner_user in [None, Some(id("owner"))] {
        let mut owner = scope();
        owner.user_id = owner_user;
        let mut foreign_tenant = owner.clone();
        foreign_tenant.tenant_id = id("other-tenant");
        let mut foreign_workspace = owner.clone();
        foreign_workspace.workspace_id = id("other-workspace");
        let mut foreign_user = owner.clone();
        foreign_user.user_id = Some(id("other-user"));
        let mut different_presence = owner.clone();
        different_presence.user_id = if owner.user_id.is_some() {
            None
        } else {
            Some(id("owner"))
        };
        for action in &actions {
            let mut request = request(action.clone());
            request.owner_scope = owner.clone();
            for wrong_scope in [
                &foreign_tenant,
                &foreign_workspace,
                &foreign_user,
                &different_presence,
            ] {
                let error = calls
                    .guarded(&gate, &request, &context(wrong_scope.clone()), None, None)
                    .await
                    .unwrap_err();
                assert_eq!(error.code, ErrorCode::AccessDenied);
            }
        }
    }
    calls.assert_calls(0);
    assert_eq!(policy.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        calls
            .guarded(
                &gate,
                &request(PolicyAction::ReadRun {}),
                &context(scope()),
                None,
                None,
            )
            .await
            .unwrap(),
        Guarded::Completed(42)
    );
    calls.assert_calls(1);
    assert_eq!(policy.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn denial_errors_panics_timeout_and_cancellation_do_not_construct_operations() {
    let cases = [
        (
            Behavior::Decision(PolicyDecision::Deny {
                reason: id("membership_revoked"),
            }),
            ErrorCode::AccessDenied,
        ),
        (Behavior::Error, ErrorCode::PolicyUnavailable),
        (Behavior::PanicBeforeFuture, ErrorCode::PolicyUnavailable),
        (Behavior::PanicInFuture, ErrorCode::PolicyUnavailable),
        (Behavior::Pending, ErrorCode::DeadlineExceeded),
        (Behavior::CancelThenAllow, ErrorCode::Cancelled),
    ];
    for (behavior, expected) in cases {
        let policy = HostPolicy::new(behavior);
        let calls = OperationCounts::default();
        let error = calls
            .guarded(
                &policy.gate(),
                &request(PolicyAction::CancelRun {}),
                &context(scope()),
                None,
                Some(PolicyDecision::Allow {}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, expected);
        assert_eq!(error.path, "policy");
        calls.assert_calls(0);
        assert_eq!(policy.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn preexisting_cancellation_or_deadline_prevents_even_policy_entry() {
    let policy = HostPolicy::new(Behavior::Decision(PolicyDecision::Allow {}));
    let gate = policy.gate();
    let request = request(PolicyAction::StartRun {});
    let cancelled = context(scope());
    cancelled.cancellation.cancel();
    let calls = OperationCounts::default();
    assert_eq!(
        calls
            .guarded(&gate, &request, &cancelled, None, None)
            .await
            .unwrap_err()
            .code,
        ErrorCode::Cancelled
    );
    assert_eq!(
        calls
            .guarded(
                &gate,
                &request,
                &context(scope()),
                Some(Instant::now()),
                None,
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::DeadlineExceeded
    );
    calls.assert_calls(0);
    assert_eq!(policy.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn caller_deadline_bounds_a_pending_policy_before_its_configured_timeout() {
    let policy = HostPolicy::new(Behavior::Pending);
    let gate = policy.gate();
    let start = Instant::now();
    let calls = OperationCounts::default();
    let error = calls
        .guarded(
            &gate,
            &request(PolicyAction::ReadEvents {}),
            &context(scope()),
            Some(start + Duration::from_millis(5)),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::DeadlineExceeded);
    assert!(start.elapsed() < Duration::from_millis(50));
    calls.assert_calls(0);
}

#[tokio::test]
async fn cancellation_while_policy_is_pending_stops_before_dispatch() {
    let policy = HostPolicy::new(Behavior::Pending);
    let gate = policy.gate();
    let context = context(scope());
    let calls = OperationCounts::default();
    let request = request(PolicyAction::ReadArtifact {});
    let cancellation = async {
        tokio::task::yield_now().await;
        context.cancellation.cancel();
    };
    let (result, ()) = tokio::join!(
        calls.guarded(&gate, &request, &context, None, None),
        cancellation,
    );
    assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
    calls.assert_calls(0);
    assert_eq!(policy.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn every_access_rechecks_the_current_grant_after_revocation() {
    let policy = HostPolicy::new(Behavior::Decision(PolicyDecision::Allow {}));
    let gate = policy.gate();
    let context = context(scope());
    let request = request(PolicyAction::ReadRun {});
    let calls = OperationCounts::default();
    assert_eq!(
        calls
            .guarded(&gate, &request, &context, None, None)
            .await
            .unwrap(),
        Guarded::Completed(42)
    );
    *policy.behavior.lock().unwrap() = Behavior::Decision(PolicyDecision::Deny {
        reason: id("membership_revoked"),
    });
    assert_eq!(
        calls
            .guarded(&gate, &request, &context, None, None)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    calls.assert_calls(1);
    assert_eq!(policy.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn restrictions_preserve_host_denial_and_approval_and_can_only_reduce_access() {
    let allow = PolicyDecision::Allow {};
    let deny = PolicyDecision::Deny {
        reason: id("host_denied"),
    };
    let approval = PolicyDecision::RequireApproval {
        reason: id("host_review"),
    };
    let other_deny = PolicyDecision::Deny {
        reason: id("hook_denied"),
    };
    let other_approval = PolicyDecision::RequireApproval {
        reason: id("hook_review"),
    };
    let cases = [
        (deny.clone(), allow.clone(), deny.clone()),
        (deny.clone(), other_deny, deny.clone()),
        (deny.clone(), approval.clone(), deny.clone()),
        (approval.clone(), allow.clone(), approval.clone()),
        (approval.clone(), other_approval, approval.clone()),
        (approval.clone(), deny.clone(), deny.clone()),
        (allow.clone(), deny.clone(), deny),
        (allow, approval.clone(), approval),
    ];
    for (host, restriction, expected) in cases {
        let gate = HostPolicy::new(Behavior::Decision(host)).gate();
        let request = request(PolicyAction::ReadRun {});
        let context = context(scope());
        assert_eq!(
            gate.check(&request, &context, None, Some(restriction.clone()))
                .await
                .unwrap(),
            expected
        );
        let calls = OperationCounts::default();
        let actual = calls
            .guarded(&gate, &request, &context, None, Some(restriction))
            .await;
        match expected {
            PolicyDecision::Deny { .. } => {
                assert_eq!(actual.unwrap_err().code, ErrorCode::AccessDenied);
            }
            PolicyDecision::RequireApproval { reason } => match actual.unwrap() {
                Guarded::ApprovalRequired(challenge) => assert_eq!(challenge.reason, reason),
                Guarded::Completed(_) => panic!("approval requirement was bypassed"),
            },
            PolicyDecision::Allow {} => unreachable!(),
        }
        calls.assert_calls(0);
    }
}

fn tool_request(document_id: &str) -> PolicyRequest {
    request(PolicyAction::ExecuteTool {
        input: ToolPolicyInput::new(
            id("call"),
            VersionedRef {
                id: id("documents.read"),
                version: id("1.0.0"),
            },
            digest("descriptor"),
            digest("binding"),
            BTreeMap::from([
                ("document_id".into(), json!(document_id)),
                ("query".into(), json!("revenue")),
            ]),
        ),
    })
}

struct ResourcePolicy(BTreeMap<String, Scope>);

impl PolicyPort for ResourcePolicy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            let PolicyAction::ExecuteTool { input } = &request.action else {
                return Ok(PolicyDecision::Deny {
                    reason: id("unsupported_action"),
                });
            };
            let owner = input
                .execution_args()
                .get("document_id")
                .and_then(Value::as_str)
                .and_then(|key| self.0.get(key));
            Ok(if owner == Some(context.scope) {
                PolicyDecision::Allow {}
            } else {
                PolicyDecision::Deny {
                    reason: id("target_unavailable"),
                }
            })
        })
    }
}

#[tokio::test]
async fn actual_bound_target_must_exist_and_belong_to_scope_before_business_operation() {
    let owned = "bc005010-d3e8-4cb8-b1fd-f6ff02c90ca6";
    let foreign = "c11cf1bb-47a2-455c-a6f8-6d7e217cd195";
    let missing = "d3a805bd-a7ea-4224-a39c-511097c43af8";
    let mut foreign_scope = scope();
    foreign_scope.tenant_id = id("other-tenant");
    let gate = PolicyGate::new(
        Arc::new(ResourcePolicy(BTreeMap::from([
            (owned.into(), scope()),
            (foreign.into(), foreign_scope),
        ]))),
        Duration::from_secs(1),
    )
    .unwrap();
    let calls = OperationCounts::default();
    let mut context = context(scope());
    context.data.system_inputs = Some(SystemInputs::new(BTreeMap::from([(
        "document_id".into(),
        json!(owned),
    )])));
    for target in [foreign, missing] {
        assert_eq!(
            calls
                .guarded(&gate, &tool_request(target), &context, None, None)
                .await
                .unwrap_err()
                .code,
            ErrorCode::AccessDenied
        );
    }
    calls.assert_calls(0);
    assert_eq!(
        calls
            .guarded(&gate, &tool_request(owned), &context, None, None)
            .await
            .unwrap(),
        Guarded::Completed(42)
    );
    calls.assert_calls(1);
}

#[tokio::test]
async fn approval_keeps_the_bound_action_while_authenticating_a_different_reviewer() {
    let policy = HostPolicy::new(Behavior::Decision(PolicyDecision::RequireApproval {
        reason: id("review_required"),
    }));
    let gate = policy.gate();
    let request = tool_request("protected-original-target");
    let original = context(scope());
    let mut reviewer = context(scope());
    reviewer.data.principal_ref = id("reviewer");
    reviewer.data.capability_grant_ref = id("review-grant");
    reviewer.data.system_inputs = Some(SystemInputs::new(BTreeMap::from([(
        "document_id".into(),
        json!("replacement-target"),
    )])));
    let calls = OperationCounts::default();
    let mut challenges = Vec::new();
    for context in [&original, &reviewer] {
        match calls
            .guarded(
                &gate,
                &request,
                context,
                None,
                Some(PolicyDecision::Allow {}),
            )
            .await
            .unwrap()
        {
            Guarded::ApprovalRequired(challenge) => challenges.push(challenge),
            Guarded::Completed(_) => panic!("approval unexpectedly executed the operation"),
        }
    }
    calls.assert_calls(0);
    assert_eq!(challenges[0], challenges[1]);
    assert_eq!(challenges[0].scope, request.owner_scope);
    assert_eq!(challenges[0].request_digest, request.digest());
    let observed = policy.observed.lock().unwrap();
    assert_eq!(observed[0].request, request);
    assert_eq!(observed[1].request, request);
    assert_eq!(observed[0].scope, request.owner_scope);
    assert_eq!(observed[1].scope, request.owner_scope);
    assert_eq!(observed[0].principal, original.data.principal_ref);
    assert_eq!(observed[1].principal, reviewer.data.principal_ref);
    assert_eq!(observed[1].grant, reviewer.data.capability_grant_ref);
    assert!(!format!("{request:?}").contains("protected-original-target"));
    assert!(matches!(
        request.action,
        PolicyAction::ExecuteTool { ref input }
            if input.execution_args()["document_id"] == json!("protected-original-target")
    ));
}

#[tokio::test]
async fn approval_identity_changes_for_arguments_versions_descriptors_bindings_and_scope() {
    let gate = HostPolicy::new(Behavior::Decision(PolicyDecision::RequireApproval {
        reason: id("review_required"),
    }))
    .gate();
    let original = tool_request("original-target");
    let mut variants = vec![tool_request("changed-target")];
    let mut changed_version = original.clone();
    if let PolicyAction::ExecuteTool { input } = &mut changed_version.action {
        input.tool.version = id("2.0.0");
    }
    variants.push(changed_version);
    let mut changed_descriptor = original.clone();
    if let PolicyAction::ExecuteTool { input } = &mut changed_descriptor.action {
        input.descriptor_digest = digest("new descriptor");
    }
    variants.push(changed_descriptor);
    let mut changed_binding = original.clone();
    if let PolicyAction::ExecuteTool { input } = &mut changed_binding.action {
        input.binding_digest = digest("new binding");
    }
    variants.push(changed_binding);
    let mut changed_scope = original.clone();
    changed_scope.owner_scope.workspace_id = id("another-workspace");
    variants.push(changed_scope);
    let calls = OperationCounts::default();
    for request in variants {
        let challenge = calls
            .guarded(
                &gate,
                &request,
                &context(request.owner_scope.clone()),
                None,
                None,
            )
            .await
            .unwrap();
        let Guarded::ApprovalRequired(challenge) = challenge else {
            panic!("changed action was executed without approval");
        };
        assert_ne!(challenge.request_digest, original.digest());
        assert_eq!(challenge.request_digest, request.digest());
        assert_eq!(challenge.scope, request.owner_scope);
    }
    calls.assert_calls(0);
}

struct ModelCatalog;

impl ProfileResolver for ModelCatalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            if reference.kind != ComponentKind::ModelBinding || reference.id != id("primary") {
                return Err(ContractError::new(
                    ErrorCode::ComponentUnavailable,
                    "reference",
                ));
            }
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id("1.0.0")),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: digest("model manifest"),
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

fn record(name: &str) -> RecordRef {
    RecordRef {
        record_id: id(name),
        revision: 1,
        digest: digest(name),
    }
}

async fn snapshot() -> RunSnapshot {
    let profile = AgentProfile::from_json(
        &json!({
            "schema_version":"wickle.agent-profile.v1", "agent_id":"research", "version":"1.0.0",
            "name":"Research", "description":"Summarize documents", "instructions":{"text":"private-profile-instructions"},
            "model_binding":"primary", "tools":[], "skills":[], "connectors":[],
            "context_policy":{"strategy":"bounded"}, "output_contract":{"type":"text"},
            "limits":{"max_model_calls":4,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
        })
        .to_string(),
    )
    .unwrap();
    let profile = ProfileValidator::new(&ModelCatalog)
        .validate(&profile, &scope())
        .await
        .unwrap();
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "private-user-input".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        max_output_tokens: None,
        output_contract: None,
    };
    let system_inputs = Some(SystemInputSnapshotRef {
        snapshot_ref: record("protected-system-inputs"),
        values_digest: digest("private-system-map"),
        definition_versions: BTreeMap::from([(id("workspace_id"), id("1"))]),
    });
    let usage = BudgetUsage {
        model_calls: 1,
        ..BudgetUsage::default()
    };
    RunSnapshot {
        interruption_plan_ref: None,
        interruption_records: vec![],
        app_state: None,
        model_step_inputs: vec![],
        prepared_steps: vec![],
        active_prepared_step: None,
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id("run"),
        request_digest: admission_digest(&request, &profile, system_inputs.as_ref()),
        request,
        scope: scope(),
        timing: RunTiming::new(0, profile.profile().limits.max_elapsed_ms.get()).unwrap(),
        resume_receipts: vec![],
        recovery_receipts: vec![],
        hook_plan_ref: None,
        source_plan_ref: None,
        skill_plan_ref: None,
        context_plan_ref: None,
        context_revision_ref: None,
        context_decisions: vec![],
        verification_plan_ref: None,
        candidate_ref: None,
        verification_records: vec![],
        hook_applications: vec![],
        reservations: vec![AttemptReservation {
            attempt_id: id("first-model-attempt"),
            kind: ReservationKind::Model {
                purpose: ModelPurpose::Agent,
            },
            reserved_at_ms: 0,
        }],
        limits: profile.profile().limits.clone(),
        profile,
        status: RunStatus::Failed,
        phase: RunPhase::Finish,
        model_step_id: None,
        usage: usage.clone(),
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs,
        wait: None,
        outcome: Some(RunOutcome {
            app_state: None,
            result: OutcomeResult::Failed {
                failure: Failure {
                    code: id("provider_unavailable"),
                    diagnostic_ref: Some(record("protected-diagnostic")),
                },
            },
            output: vec![InputContent::Text {
                text: "private-partial-output".into(),
            }],
            artifacts: vec![],
            usage,
            checkpoint_revision: 3,
            verification: None,
            unresolved_effects: vec![],
        }),
        assembly_ref: Some(record("protected-assembly")),
        routing_snapshot_ref: None,
        context_batches: vec![record("protected-context-batch")],
        source_states: vec![],
        revision: 3,
        last_event_seq: 2,
    }
}

fn artifact() -> ArtifactRef {
    ArtifactRef {
        artifact_id: id("artifact"),
        scope: scope(),
        media_type: id("text/plain"),
        size_bytes: 7,
        content_hash: id("sha256-abcdef"),
    }
}

fn event() -> RunEvent {
    RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("event"),
        scope: scope(),
        run_id: id("run"),
        session_id: id("session"),
        seq: NonZeroU64::new(2).unwrap(),
        timestamp_ms: 1000,
        payload: RunEventPayload::RunFinished {
            outcome_ref: record("protected-outcome"),
        },
    }
}

#[tokio::test]
async fn authorized_minimal_views_serialize_only_public_metadata_and_use_distinct_actions() {
    let policy = HostPolicy::new(Behavior::Decision(PolicyDecision::Allow {}));
    let gate = policy.gate();
    let context = context(scope());
    let snapshot = snapshot().await;
    snapshot.validate().unwrap();
    let Guarded::Completed(run) = gate
        .run_view(&snapshot, &SystemClock::new(), &context, None)
        .await
        .unwrap()
    else {
        panic!("expected authorized public run view");
    };
    assert_eq!(
        serde_json::to_value(run).unwrap(),
        json!({"run_id":"run","session_id":"session","status":"failed","phase":"finish","revision":3,"deadline_expired":false,
            "usage":{"model_calls":1,"tool_attempts":0,"repair_attempts":0,"recovery_attempts":0,"elapsed_ms":0}})
    );
    let Guarded::Completed(artifact) = gate
        .artifact_view(&artifact(), &context, None)
        .await
        .unwrap()
    else {
        panic!("expected authorized artifact metadata");
    };
    assert_eq!(
        serde_json::to_value(artifact).unwrap(),
        json!({"artifact_id":"artifact","media_type":"text/plain","size_bytes":7,"content_hash":"sha256-abcdef"})
    );
    let Guarded::Completed(event) = gate.event_view(&event(), &context, None).await.unwrap() else {
        panic!("expected authorized event metadata");
    };
    assert_eq!(
        serde_json::to_value(event).unwrap(),
        json!({"event_id":"event","run_id":"run","session_id":"session","seq":2,"timestamp_ms":1000,"event_type":"run.finished"})
    );
    let observed = policy.observed.lock().unwrap();
    assert_eq!(observed[0].request.action, PolicyAction::ReadRun {});
    assert_eq!(observed[1].request.action, PolicyAction::ReadArtifact {});
    assert_eq!(observed[1].request.resource_id, id("artifact"));
    assert_eq!(observed[2].request.action, PolicyAction::ReadEvents {});
    assert_eq!(observed[2].request.resource_id, id("run"));
}

#[tokio::test]
async fn public_and_protected_views_reject_claimed_scopes_different_from_stored_owners() {
    let policy = HostPolicy::new(Behavior::Decision(PolicyDecision::Allow {}));
    let gate = policy.gate();
    let snapshot = snapshot().await;
    let mut wrong_scopes = vec![scope(); 3];
    wrong_scopes[0].tenant_id = id("other-tenant");
    wrong_scopes[1].workspace_id = id("other-workspace");
    wrong_scopes[2].user_id = Some(id("other-user"));
    for wrong_scope in wrong_scopes {
        let context = context(wrong_scope);
        assert_eq!(
            gate.run_view(&snapshot, &SystemClock::new(), &context, None)
                .await
                .unwrap_err()
                .code,
            ErrorCode::AccessDenied
        );
        assert_eq!(
            gate.run_details(&snapshot, &context, None)
                .await
                .unwrap_err()
                .code,
            ErrorCode::AccessDenied
        );
        assert_eq!(
            gate.artifact_view(&artifact(), &context, None)
                .await
                .unwrap_err()
                .code,
            ErrorCode::AccessDenied
        );
        assert_eq!(
            gate.event_view(&event(), &context, None)
                .await
                .unwrap_err()
                .code,
            ErrorCode::AccessDenied
        );
    }
    assert_eq!(policy.calls.load(Ordering::SeqCst), 0);
}

struct PublicOnlyPolicy;

impl PolicyPort for PublicOnlyPolicy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            Ok(match request.action {
                PolicyAction::ReadRun {} | PolicyAction::ReadEvents {} => PolicyDecision::Allow {},
                _ => PolicyDecision::Deny {
                    reason: id("detail_access_not_granted"),
                },
            })
        })
    }
}

#[tokio::test]
async fn public_read_permission_does_not_grant_access_to_protected_run_details() {
    let gate = PolicyGate::new(Arc::new(PublicOnlyPolicy), Duration::from_secs(1)).unwrap();
    let context = context(scope());
    let snapshot = snapshot().await;
    assert!(matches!(
        gate.run_view(&snapshot, &SystemClock::new(), &context, None)
            .await
            .unwrap(),
        Guarded::Completed(_)
    ));
    assert!(matches!(
        gate.event_view(&event(), &context, None).await.unwrap(),
        Guarded::Completed(_)
    ));
    assert_eq!(
        gate.run_details(&snapshot, &context, None)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    let policy = HostPolicy::new(Behavior::Decision(PolicyDecision::Allow {}));
    let Guarded::Completed(details) = policy
        .gate()
        .run_details(&snapshot, &context, None)
        .await
        .unwrap()
    else {
        panic!("explicit detail permission did not grant the protected view");
    };
    assert_eq!(details, snapshot);
    assert_eq!(
        policy.observed.lock().unwrap()[0].request.action,
        PolicyAction::ReadRunDetails {}
    );
}

struct ViewClock {
    calls: AtomicUsize,
    utc_ms: Option<i64>,
}
impl Clock for ViewClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.utc_ms
            .map(|utc_ms| ClockReading {
                utc_ms,
                monotonic_ms: 0,
            })
            .ok_or_else(|| ContractError::new(ErrorCode::ClockUnavailable, "fixture.clock"))
    }
    fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
        Box::pin(pending())
    }
}
#[tokio::test]
async fn run_view_reads_clock_only_after_authorization_and_never_hides_clock_errors() {
    let mut snapshot = snapshot().await;
    let clock = ViewClock {
        calls: AtomicUsize::new(0),
        utc_ms: None,
    };
    let allowed = HostPolicy::new(Behavior::Decision(PolicyDecision::Allow {})).gate();
    let terminal = match allowed
        .run_view(&snapshot, &clock, &context(scope()), None)
        .await
        .unwrap()
    {
        Guarded::Completed(view) => view,
        _ => panic!("unexpected approval"),
    };
    assert!(!terminal.deadline_expired);
    assert_eq!(clock.calls.load(Ordering::SeqCst), 0);
    snapshot.status = RunStatus::Running;
    snapshot.phase = RunPhase::Prepare;
    snapshot.outcome = None;
    snapshot.validate().unwrap();
    let denied = HostPolicy::new(Behavior::Decision(PolicyDecision::Deny {
        reason: id("revoked"),
    }))
    .gate();
    assert_eq!(
        denied
            .run_view(&snapshot, &clock, &context(scope()), None)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(clock.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        allowed
            .run_view(&snapshot, &clock, &context(scope()), None)
            .await
            .unwrap_err()
            .code,
        ErrorCode::ClockUnavailable
    );
    assert_eq!(clock.calls.load(Ordering::SeqCst), 1);
    let regressed = ViewClock {
        calls: AtomicUsize::new(0),
        utc_ms: Some(snapshot.timing.last_observed_at_ms - 1),
    };
    assert_eq!(
        allowed
            .run_view(&snapshot, &regressed, &context(scope()), None)
            .await
            .unwrap_err()
            .code,
        ErrorCode::ClockRegression
    );
}
```

## `crates/wickle/tests/support/execution_store.rs`

```rust
use super::*;
use std::sync::Arc;

pub async fn atomic_execution_contract(store: Arc<dyn StateStore>) {
    let s = scope();
    let run = id("atomic-run");
    let initial = store
        .admit(
            &s,
            admission(run.as_str(), "request", "session", "input", "1").await,
        )
        .await
        .unwrap()
        .state;
    let history = store.read_execution(&s, &run).await.unwrap();
    assert!(!history.initial_claimed);
    let first_id = history.segments[0].segment_id.clone();
    let claim = |segment_id| BeginSegmentRequest {
        transition: None,
        run_id: run.clone(),
        expected_revision: 0,
        segment_id,
        owner: id("worker"),
        now_ms: 0,
        lease_ttl_ms: 1000.try_into().unwrap(),
        start: SegmentStart::Initial,
    };
    // A failure after tentative lease acquisition must not publish the lease.
    assert!(
        store
            .begin_segment(&s, claim(id("wrong-initial-id")))
            .await
            .is_err()
    );
    assert!(
        !store
            .read_execution(&s, &run)
            .await
            .unwrap()
            .initial_claimed
    );
    let lease = store
        .acquire_lease(&s, &run, &id("rollback-probe"), 0, 1000)
        .await
        .unwrap();
    store.release_lease(&s, &run, &lease, 0).await.unwrap();
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let mut claims = Vec::new();
    for request in [claim(first_id.clone()), claim(first_id)] {
        let store = store.clone();
        let owner = s.clone();
        let gate = gate.clone();
        claims.push(tokio::spawn(async move {
            gate.wait().await;
            store.begin_segment(&owner, request).await
        }));
    }
    let b = claims.pop().unwrap().await.unwrap();
    let a = claims.pop().unwrap().await.unwrap();
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(
        usize::from(a.lease.is_some()) + usize::from(b.lease.is_some()),
        1
    );
    let lease = a.lease.or(b.lease).unwrap();
    store.release_lease(&s, &run, &lease, 1).await.unwrap();
    let command = ControlCommand {
        command_id: id("cancel"),
        principal_ref: id("reviewer"),
        action: ControlAction::Cancel { reason: id("stop") },
    };
    let (a, b) = tokio::join!(
        store.submit_control_command(&s, &run, command.clone()),
        store.submit_control_command(&s, &run, command.clone())
    );
    assert_eq!(a.unwrap(), b.unwrap());
    assert_eq!(
        store.read_execution(&s, &run).await.unwrap().controls.len(),
        1
    );
    let mut changed = command;
    changed.principal_ref = id("different-user");
    assert_eq!(
        store
            .submit_control_command(&s, &run, changed)
            .await
            .unwrap_err()
            .code,
        ErrorCode::RequestConflict
    );
    let mut commit = finished(&initial.snapshot, lease, 2);
    commit.snapshot.status = RunStatus::Cancelled;
    commit.snapshot.outcome.as_mut().unwrap().result = OutcomeResult::Cancelled {
        reason: "stop".into(),
    };
    let outcome = ProtectedRecord::new(
        id("cancel-outcome"),
        1,
        serde_json::to_value(commit.snapshot.outcome.as_ref().unwrap()).unwrap(),
    );
    commit.events[0].payload = RunEventPayload::RunFinished {
        outcome_ref: outcome.reference().clone(),
    };
    let transition = SegmentTransition {
        snapshot: commit.snapshot,
        messages: commit.messages,
        events: commit.events,
        records: vec![outcome],
    };
    let request = BeginSegmentRequest {
        transition: Some(transition),
        run_id: run.clone(),
        expected_revision: 0,
        segment_id: id("cancel-segment"),
        owner: id("controller"),
        now_ms: 2,
        lease_ttl_ms: 1000.try_into().unwrap(),
        start: SegmentStart::Control(id("cancel")),
    };
    let mut reused = request.clone();
    reused.segment_id = history.segments[0].segment_id.clone();
    assert!(
        store.begin_segment(&s, reused).await.is_err(),
        "a control must allocate a new segment ID"
    );
    let (a, b) = tokio::join!(
        store.begin_segment(&s, request.clone()),
        store.begin_segment(&s, request)
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(a.segment, b.segment);
    assert!(a.lease.is_none() && b.lease.is_none());
    assert_eq!(a.state.snapshot.status, RunStatus::Cancelled);
    let history = store.read_execution(&s, &run).await.unwrap();
    assert_eq!(history.segments.len(), 2);
    assert_eq!(history.accepted_commands.len(), 1);
    assert_eq!(
        history.controls[0].processed_segment_id,
        Some(id("cancel-segment"))
    );
    assert_eq!(history.execution_principal_ref, id("execution-principal"));
    assert!(matches!(
        history.segments[0].outcome,
        Some(SegmentOutcome::Interrupted { .. })
    ));
    assert!(matches!(
        history.segments[1].outcome,
        Some(SegmentOutcome::Settled { .. })
    ));
    assert!(
        store
            .load(&s, &run)
            .await
            .unwrap()
            .session
            .active_run_id
            .is_none()
    );
    let before = store.read_events(&s, &run, 0, 100).await.unwrap();
    store
        .submit_control_command(
            &s,
            &run,
            ControlCommand {
                command_id: id("terminal-noop"),
                principal_ref: id("reviewer"),
                action: ControlAction::Expire,
            },
        )
        .await
        .unwrap();
    let after = store.read_execution(&s, &run).await.unwrap();
    assert_eq!(after.segments, history.segments);
    assert_eq!(after.controls.len(), history.controls.len() + 1);
    assert_eq!(
        after.controls.last().unwrap().processed_segment_id,
        Some(id("cancel-segment"))
    );
    assert_eq!(store.read_events(&s, &run, 0, 100).await.unwrap(), before);
    let foreign = Scope {
        workspace_id: id("foreign"),
        ..s
    };
    assert!(store.read_execution(&foreign, &run).await.is_err());
}

pub async fn atomic_recovery_contract(store: Arc<dyn StateStore>) {
    let s = scope();
    let run = id("recover-run");
    let saved = store
        .admit(
            &s,
            admission(
                run.as_str(),
                "recover-request",
                "recover-session",
                "input",
                "1",
            )
            .await,
        )
        .await
        .unwrap()
        .state;
    let source = saved.snapshot.recovery_record(id("source")).unwrap();
    let command = ResumeCommand {
        run_id: run.clone(),
        expected_revision: 0,
        command_id: id("recover-once"),
        action: ResumeAction::Recover {
            recovery_ref: source.reference().clone(),
        },
    };
    let command_record = ProtectedRecord::new(
        id("recovery-command"),
        1,
        serde_json::to_value(&command).unwrap(),
    );
    let receipt = RecoveryReceipt {
        command: command.clone(),
        command_ref: command_record.reference().clone(),
        source_snapshot_ref: source.reference().clone(),
        accepted_revision: 1,
        previous_segment_start_revision: 0,
        previous_last_event_seq: 1,
        actor_ref: id("reviewer"),
        capability_grant_ref: id("grant"),
        expired: false,
        recovery_attempt_id: Some(id("recovery-budget")),
    };
    let receipt_record = ProtectedRecord::new(
        id("recovery-receipt"),
        1,
        serde_json::to_value(&receipt).unwrap(),
    );
    let mut next = saved.snapshot.clone();
    next.revision = 1;
    next.phase = RunPhase::Prepare;
    next.recovery_receipts.push(receipt);
    next.usage.recovery_attempts += 1;
    next.reservations.push(AttemptReservation {
        attempt_id: id("recovery-budget"),
        kind: ReservationKind::Recovery {},
        reserved_at_ms: 0,
    });
    next.last_event_seq = 2;
    let mut e = event(
        &run,
        &id("recover-session"),
        &s,
        2,
        RunEventPayload::RunRecovered {
            recovery_receipt_ref: receipt_record.reference().clone(),
        },
    );
    e.timestamp_ms = 0;
    let transition = SegmentTransition {
        snapshot: next,
        messages: vec![],
        events: vec![e],
        records: vec![source, command_record, receipt_record],
    };
    let request = BeginSegmentRequest {
        transition: Some(transition),
        run_id: run.clone(),
        expected_revision: 0,
        segment_id: id("recovered"),
        owner: id("recover-worker"),
        now_ms: 0,
        lease_ttl_ms: 1000.try_into().unwrap(),
        start: SegmentStart::Resume(command),
    };
    let mut broken = request.clone();
    broken.transition.as_mut().unwrap().events.clear();
    assert!(store.begin_segment(&s, broken).await.is_err());
    assert_eq!(store.load(&s, &run).await.unwrap(), saved);
    assert!(
        store
            .read_execution(&s, &run)
            .await
            .unwrap()
            .accepted_commands
            .is_empty()
    );
    let before = store.read_execution(&s, &run).await.unwrap();
    let accepted = store.begin_segment(&s, request.clone()).await.unwrap();
    assert!(accepted.lease.is_some());
    let replay = store.begin_segment(&s, request.clone()).await.unwrap();
    assert!(replay.lease.is_none());
    assert_eq!(accepted.segment, replay.segment);
    assert_eq!(replay.state.snapshot.usage.recovery_attempts, 1);
    let mut conflict = request;
    conflict.expected_revision = 1;
    if let SegmentStart::Resume(c) = &mut conflict.start {
        c.expected_revision = 1;
    }
    assert!(matches!(
        store.begin_segment(&s, conflict).await,
        Err(ContractError {
            code: ErrorCode::RequestConflict,
            ..
        })
    ));
    let after = store.read_execution(&s, &run).await.unwrap();
    assert_eq!(after.segments.len(), before.segments.len() + 1);
    assert_eq!(after.accepted_commands.len(), 1);
    assert_eq!(after.execution_principal_ref, id("execution-principal"));
}

pub async fn conflicting_submissions_race(stores: [Arc<dyn StateStore>; 2]) {
    let store = stores[0].clone();
    let mut left = admission("race-left", "same-key", "race-session", "left input", "1").await;
    let mut right = admission("race-right", "same-key", "race-session", "right input", "1").await;
    for input in [&mut left, &mut right] {
        let profile = input.snapshot.profile.profile();
        input.submitted = Some(
            RequestSnapshot::capture(
                VersionedRef {
                    id: profile.agent_id.clone(),
                    version: profile.version.clone(),
                },
                &serde_json::to_string(&input.snapshot.request).unwrap(),
                None,
                Default::default(),
            )
            .unwrap(),
        );
    }
    let submitted = [
        left.submitted.clone().unwrap(),
        right.submitted.clone().unwrap(),
    ];
    let expected = [left.snapshot.clone(), right.snapshot.clone()];
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let mut workers = Vec::new();
    for (store, input) in stores.into_iter().zip([left, right]) {
        let gate = gate.clone();
        workers.push(tokio::spawn(async move {
            gate.wait().await;
            store.admit(&scope(), input).await
        }));
    }
    let a = workers.pop().unwrap().await.unwrap();
    let b = workers.pop().unwrap().await.unwrap();
    let (winner, loser) = match (a, b) {
        (Ok(winner), Err(loser)) | (Err(loser), Ok(winner)) => (winner, loser),
        _ => panic!("conflicting submissions must have exactly one admitted winner"),
    };
    assert!(winner.created);
    assert_eq!(loser.code, ErrorCode::RequestConflict);
    let loaded = store
        .find_request(&scope(), &id("race-session"), &id("same-key"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded, winner.state);
    assert!(expected.iter().any(|snapshot| snapshot == &loaded.snapshot));
    let history = store
        .read_execution(&scope(), &loaded.snapshot.run_id)
        .await
        .unwrap();
    let winner_index = expected
        .iter()
        .position(|snapshot| snapshot.run_id == loaded.snapshot.run_id)
        .unwrap();
    assert_eq!(history.submitted.as_ref(), Some(&submitted[winner_index]));
    let losing_id = expected
        .iter()
        .find(|snapshot| snapshot.run_id != loaded.snapshot.run_id)
        .unwrap()
        .run_id
        .clone();
    assert_eq!(
        store.load(&scope(), &losing_id).await.unwrap_err().code,
        ErrorCode::StateNotFound
    );
    assert_eq!(
        store
            .load_session(&scope(), &id("race-session"))
            .await
            .unwrap()
            .active_run_id,
        Some(loaded.snapshot.run_id)
    );
}
```
