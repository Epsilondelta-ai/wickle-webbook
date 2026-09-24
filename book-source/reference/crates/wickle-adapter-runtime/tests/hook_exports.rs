//! The same Hook code can be selected through distinct scoped adapter bindings.
#[path = "../../wickle/tests/support/agent.rs"]
#[allow(dead_code)]
mod agent_support;
#[path = "../../wickle/tests/support/mod.rs"]
#[allow(dead_code)]
mod core_fixture;
#[allow(dead_code)]
mod support;
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use support::*;
use wickle::*;
use wickle_adapter_runtime::*;

struct Hooks {
    definition: AdapterDefinition,
    calls: Arc<Mutex<Vec<(HookRef, HookTarget, Id)>>>,
    closed: Arc<Mutex<Vec<Id>>>,
}
struct Instance {
    binding: Id,
    exports: Vec<AdapterExportInstance>,
    closed: Arc<Mutex<Vec<Id>>>,
}
struct Handler {
    binding: Id,
    calls: Arc<Mutex<Vec<(HookRef, HookTarget, Id)>>>,
}
impl HookHandler for Handler {
    fn call<'a>(
        &'a self,
        input: &'a HookInput,
        context: &'a HookContext,
    ) -> PortFuture<'a, HookOutput> {
        Box::pin(async move {
            let selection = context
                .selection
                .clone()
                .expect("export source must be retained");
            let HookRef::Export(export) = &selection else {
                panic!("real export selection required");
            };
            assert_eq!(export.adapter_binding, self.binding);
            self.calls.lock().unwrap().push((
                selection,
                context.target.clone(),
                context.binding_set_id.clone().unwrap(),
            ));
            Ok(match input {
                HookInput::BeforeRun { .. } => HookOutput::Context {
                    additions: vec![HookContextAddition {
                        content: vec![InputContent::Json {
                            value: json!({"binding":self.binding}),
                        }],
                        priority: ContextPriority::Required,
                    }],
                },
                HookInput::AfterRun { status, .. } => {
                    assert_eq!(*status, RunStatus::Succeeded);
                    HookOutput::Observed {}
                }
                _ => panic!("unselected lifecycle position"),
            })
        })
    }
}
impl AdapterFactory for Hooks {
    fn open<'a>(
        &'a self,
        context: &'a AdapterInitContext,
    ) -> PortFuture<'a, Arc<dyn AdapterInstance>> {
        Box::pin(async move {
            let exports = context
                .selected_exports
                .iter()
                .map(|selection| {
                    let AdapterExportDefinition::Hook { definition, .. } = self
                        .definition
                        .exports
                        .iter()
                        .find(|export| export.metadata().export_id == selection.export_id)
                        .unwrap()
                    else {
                        panic!("Hook-only factory");
                    };
                    AdapterExportInstance::Hook {
                        export_id: selection.export_id.clone(),
                        definition: definition.clone(),
                        handler: Arc::new(Handler {
                            binding: context.binding.binding.binding_id.clone(),
                            calls: self.calls.clone(),
                        }),
                    }
                })
                .collect();
            Ok(Arc::new(Instance {
                binding: context.binding.binding.binding_id.clone(),
                exports,
                closed: self.closed.clone(),
            }) as Arc<dyn AdapterInstance>)
        })
    }
}
impl AdapterInstance for Instance {
    fn exports(&self) -> Vec<AdapterExportInstance> {
        self.exports.clone()
    }
    fn close<'a>(&'a self, context: &'a AdapterCloseContext) -> PortFuture<'a, ()> {
        Box::pin(async move {
            assert_eq!(context.adapter_binding, self.binding);
            self.closed.lock().unwrap().push(self.binding.clone());
            Ok(())
        })
    }
}
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
            self.0
                .component_metadata(reference)
                .ok_or_else(|| ContractError::new(ErrorCode::ComponentUnavailable, "test.catalog"))
        })
    }
}

#[tokio::test]
async fn identical_hook_exports_keep_distinct_selection_policy_and_observation_records() {
    let fixture = Fixture::new();
    let mut definition = definition("adapter");
    definition
        .exports
        .retain(|export| matches!(export, AdapterExportDefinition::Hook { .. }));
    let mut observer = definition.exports[0].clone();
    let AdapterExportDefinition::Hook {
        metadata,
        definition: hook,
    } = &mut observer
    else {
        unreachable!()
    };
    metadata.export_id = id("observe");
    metadata.hook_position = Some(HookPosition::AfterRun);
    hook.hook = reference("observe");
    hook.position = HookPosition::AfterRun;
    definition.exports.push(observer);
    definition.metadata.exports = definition
        .exports
        .iter()
        .map(|export| export.metadata().clone())
        .collect();
    let calls = Arc::new(Mutex::new(vec![]));
    let closed = Arc::new(Mutex::new(vec![]));
    let factory = Arc::new(Hooks {
        definition: definition.clone(),
        calls: calls.clone(),
        closed: closed.clone(),
    });
    let mut a = connection();
    a.binding.binding_id = id("data-a");
    a.connection_ref = reference("account-a");
    let mut b = connection();
    b.binding.binding_id = id("data-b");
    b.connection_ref = reference("account-b");
    let registry = Arc::new(
        AdapterRegistry::new(
            scope(),
            vec![AdapterRegistration {
                definition,
                factory,
            }],
            vec![a.clone(), b.clone()],
            vec![],
            vec![],
            vec![],
        )
        .unwrap(),
    );
    let mut profile = multi_profile(&["adapter", "adapter"]);
    profile.tools.clear();
    profile.connectors = vec![a.binding, b.binding];
    profile.hooks = Some(vec![]);
    for (index, adapter) in profile.adapters.as_mut().unwrap().iter_mut().enumerate() {
        adapter
            .connections
            .insert(id("main"), id(if index == 0 { "data-a" } else { "data-b" }));
        for export in ["prepare", "observe"] {
            profile
                .hooks
                .as_mut()
                .unwrap()
                .push(HookRef::Export(ExportRef {
                    adapter_binding: adapter.binding_id.clone(),
                    export_id: id(export),
                    alias: None,
                }));
        }
    }
    let base = agent_support::Fixture::new(agent_support::Response::Text, false);
    let mut bindings = base.bindings();
    bindings.state = fixture.store.clone();
    bindings.clock = fixture.clock.clone();
    bindings.system_inputs = inputs();
    bindings.profile_resolver = Arc::new(Catalog(registry.clone()));
    bindings.policy =
        Arc::new(PolicyGate::new(fixture.policy.clone(), Duration::from_secs(5)).unwrap());
    bindings.components = Some(Arc::new(AdapterRuntime::new(
        registry,
        bindings.state.clone(),
        bindings.policy.clone(),
        bindings.clock.clone(),
    )));
    bindings.settings.lease_ttl_ms = 30_000;
    bindings.settings.heartbeat_interval_ms = 5_000;
    let agent = create_agent(profile.clone(), bindings).unwrap();
    let context = agent_support::context();
    let handle = agent_support::completed(
        agent
            .start(agent_support::request("hooks"), context.clone())
            .await
            .unwrap(),
    );
    let outcome = agent_support::completed(handle.outcome(&context).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if agent_support::completed(handle.component_release(&context).await.unwrap())
                .report
                .is_some()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let reports = fixture
        .store
        .read_hook_observations(&scope(), handle.run_id())
        .await
        .unwrap();
    assert_eq!(reports.len(), 2);
    assert_eq!(reports[0].hook, reports[1].hook);
    assert_ne!(reports[0].selection, reports[1].selection);
    assert!(
        reports
            .iter()
            .all(|report| report.status == HookObservationStatus::Completed)
    );
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert_eq!(saved.snapshot.hook_applications.len(), 2);
    assert_ne!(
        saved.snapshot.hook_applications[0].selection,
        saved.snapshot.hook_applications[1].selection
    );
    assert_eq!(calls.lock().unwrap().len(), 4);
    assert_eq!(
        *closed.lock().unwrap(),
        vec![id("binding-1"), id("binding-0")]
    );
    let hook_selections: Vec<_> = fixture
        .policy
        .seen
        .lock()
        .unwrap()
        .iter()
        .filter_map(|action| match action {
            PolicyAction::InvokeHook { selection, .. } => selection.clone(),
            _ => None,
        })
        .collect();
    assert_eq!(hook_selections.len(), 4);
    assert!(
        profile
            .hooks
            .as_ref()
            .unwrap()
            .iter()
            .all(|selection| hook_selections.contains(selection))
    );
    let checkpoint = fixture.store.export_checkpoint(&scope()).unwrap();
    let restored = MemoryStateStore::from_checkpoint(
        StateStoreCheckpoint::from_json(
            &serde_json::to_string(&checkpoint).unwrap(),
            &scope(),
            &checkpoint.digest(),
        )
        .unwrap(),
    );
    assert_eq!(
        restored
            .read_hook_observations(&scope(), handle.run_id())
            .await
            .unwrap(),
        reports
    );
}
