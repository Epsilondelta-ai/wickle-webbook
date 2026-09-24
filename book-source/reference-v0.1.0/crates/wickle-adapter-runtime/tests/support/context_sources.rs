//! Source adapters are recreated per segment while batches remain in the actual StateStore.

use super::{agent_support, common};
pub use common::{id, object, reference, scope};
use futures_util::stream;
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;
use wickle_adapter_runtime::*;

pub fn source_definition() -> ContextSourceDefinition {
    ContextSourceDefinition {
        source: reference("recall"),
        origin: ContextOrigin::Memory,
        contract_version: 1,
    }
}
pub fn binding(source: ContextSourceRef, trigger: ContextTrigger) -> ContextSourceBinding {
    ContextSourceBinding {
        source,
        trigger,
        required: true,
        timeout_ms: 5000.try_into().unwrap(),
        max_items: 2.try_into().unwrap(),
        max_bytes: 4096.try_into().unwrap(),
        max_tokens: 100.try_into().unwrap(),
    }
}
pub struct Estimate;
impl ContextTokenEstimator for Estimate {
    fn version(&self) -> VersionedRef {
        reference("source-estimate")
    }
    fn estimate(&self, items: &[ContextItem]) -> Result<u64, ContractError> {
        Ok(items.len() as u64 * 8)
    }
}
#[derive(Default)]
pub struct Source {
    pub queries: Mutex<Vec<(ContextRequest, ContextCallContext)>>,
    pub uses: Mutex<Vec<(ContextUseRequest, ContextCallContext)>>,
    pub revoked: AtomicBool,
}
impl ContextSource for Source {
    fn provide<'a>(
        &'a self,
        request: &'a ContextRequest,
        context: &'a ContextCallContext,
    ) -> PortFuture<'a, ContextResult> {
        Box::pin(async move {
            self.queries
                .lock()
                .unwrap()
                .push((request.clone(), context.clone()));
            assert_eq!(request.scope, context.scope);
            assert_eq!(request.run_id, context.run_id);
            assert_eq!(request.binding.source, context.source);
            let lifetime = match request.binding.trigger {
                ContextTrigger::RunStart => ContextLifetime::Run {
                    run_id: request.run_id.clone(),
                },
                ContextTrigger::BeforeModel => ContextLifetime::Step {
                    run_id: request.run_id.clone(),
                    model_step_id: request.model_step_id.clone().unwrap(),
                },
            };
            Ok(ContextResult::Ready {
                items: vec![ContextItem::new(
                    id("local-item"),
                    request.definition.origin,
                    request.definition.source.clone(),
                    request.scope.clone(),
                    vec![InputContent::Json {
                        value: json!({"fact":"source-observation","trigger":request.binding.trigger}),
                    }],
                    lifetime,
                    ContextPriority::Required,
                )],
                source_revision: Some(id("data-revision")),
                reported_usage: None,
            })
        })
    }
    fn authorize_use<'a>(
        &'a self,
        request: &'a ContextUseRequest,
        context: &'a ContextCallContext,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            self.uses
                .lock()
                .unwrap()
                .push((request.clone(), context.clone()));
            assert_eq!(request.items[0].item_id, id("local-item"));
            if self.revoked.load(Ordering::SeqCst) {
                Err(ContractError::new(ErrorCode::AccessDenied, "source.acl"))
            } else {
                Ok(())
            }
        })
    }
}
pub struct Tool {
    pub calls: AtomicUsize,
}
impl ToolExecutor for Tool {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        _: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(
                args["workspace_id"],
                json!("11111111-1111-4111-8111-111111111111")
            );
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: json!("observed"),
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}
pub struct Instance {
    pub context: AdapterInitContext,
    pub source: Arc<Source>,
    pub tool: Arc<Tool>,
    pub closes: AtomicUsize,
}
impl AdapterInstance for Instance {
    fn exports(&self) -> Vec<AdapterExportInstance> {
        self.context
            .selected_exports
            .iter()
            .map(|selection| match selection.export_id.as_str() {
                "search" => AdapterExportInstance::Tool {
                    export_id: id("search"),
                    descriptor: Box::new(common::descriptor("search")),
                    executor: self.tool.clone(),
                },
                "recall" => AdapterExportInstance::ContextSource {
                    export_id: id("recall"),
                    definition: source_definition(),
                    source: self.source.clone(),
                },
                _ => panic!("unselected metadata must not activate"),
            })
            .collect()
    }
    fn close<'a>(&'a self, context: &'a AdapterCloseContext) -> PortFuture<'a, ()> {
        Box::pin(async move {
            assert_eq!(
                context.binding_set_id,
                self.context.execution.binding_set_id
            );
            assert_eq!(context.run_id, self.context.execution.run_id);
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}
pub struct Factory {
    pub instances: Mutex<Vec<Arc<Instance>>>,
    pub store: Arc<MemoryStateStore>,
    pub revoke_new: AtomicBool,
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
            assert!(saved.snapshot.assembly_ref.is_some());
            let source = Arc::new(Source::default());
            source
                .revoked
                .store(self.revoke_new.load(Ordering::SeqCst), Ordering::SeqCst);
            let instance = Arc::new(Instance {
                context: context.clone(),
                source,
                tool: Arc::new(Tool {
                    calls: AtomicUsize::new(0),
                }),
                closes: AtomicUsize::new(0),
            });
            self.instances.lock().unwrap().push(instance.clone());
            Ok(instance as Arc<dyn AdapterInstance>)
        })
    }
}
pub struct Policy {
    pub approval: AtomicBool,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::ExecuteTool { input } = &request.action {
                if self.approval.load(Ordering::SeqCst) && input.approval().is_none() {
                    return Ok(PolicyDecision::RequireApproval {
                        reason: id("review"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}
struct Catalog(Arc<AdapterRegistry>);
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        request: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            if request.kind == ComponentKind::ModelBinding {
                return Ok(common::metadata(
                    ComponentKind::ModelBinding,
                    request.id.as_str(),
                ));
            }
            self.0.component_metadata(request).ok_or_else(|| {
                ContractError::new(ErrorCode::ComponentUnavailable, "source.metadata")
            })
        })
    }
}
pub struct Model {
    pub calls: AtomicUsize,
    pub requests: Mutex<Vec<ModelRequest>>,
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
            vec![Ok(ModelEvent::ToolArgumentsDelta {
                index: 0,
                provider_call_id: Some("search-call".into()),
                name: Some("search_records".into()),
                delta: json!({"query":"report"}).to_string(),
            })]
        } else {
            vec![Ok(ModelEvent::TextDelta {
                text: "Source and tool observations used".into(),
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
pub struct Fixture {
    pub base: agent_support::Fixture,
    pub factory: Arc<Factory>,
    pub catalog_source: Arc<Source>,
    pub registry: Arc<AdapterRegistry>,
    pub profile: AgentProfile,
    pub model: Arc<Model>,
    pub policy: Arc<Policy>,
}
impl Fixture {
    pub fn new(catalog: bool, both_triggers: bool) -> Self {
        let base = agent_support::Fixture::new(agent_support::Response::Text, false);
        let factory = Arc::new(Factory {
            instances: Mutex::new(vec![]),
            store: base.store.clone(),
            revoke_new: AtomicBool::new(false),
        });
        let catalog_source = Arc::new(Source::default());
        let mut selected = common::profile();
        let source = if catalog {
            ContextSourceRef::Catalog(CatalogSourceRef {
                source_id: id("recall"),
                version: id("1"),
            })
        } else {
            ContextSourceRef::Export(ExportRef {
                adapter_binding: id("records"),
                export_id: id("recall"),
                alias: None,
            })
        };
        selected.context_sources = Some(vec![binding(source.clone(), ContextTrigger::RunStart)]);
        if both_triggers {
            selected
                .context_sources
                .as_mut()
                .unwrap()
                .push(binding(source.clone(), ContextTrigger::BeforeModel));
        }
        let mut registry = AdapterRegistry::new(
            scope(),
            vec![AdapterRegistration {
                definition: common::definition("adapter"),
                factory: factory.clone(),
            }],
            vec![common::connection()],
            vec![],
            vec![],
            vec![],
        )
        .unwrap();
        if catalog {
            registry = registry
                .with_sources(vec![CatalogSourceRegistration {
                    metadata: common::metadata(ComponentKind::ContextSource, "recall"),
                    source: ContextSourceRegistration {
                        selection: source,
                        definition: source_definition(),
                        source: catalog_source.clone(),
                    },
                }])
                .unwrap();
        }
        Self {
            base,
            factory,
            catalog_source,
            registry: Arc::new(registry),
            profile: selected,
            model: Arc::new(Model {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(vec![]),
            }),
            policy: Arc::new(Policy {
                approval: AtomicBool::new(false),
            }),
        }
    }
    pub fn runtime(&self) -> Arc<AdapterRuntime> {
        Arc::new(AdapterRuntime::new(
            self.registry.clone(),
            self.base.store.clone(),
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(5)).unwrap()),
            self.base.clock.clone(),
        ))
    }
    pub fn bindings(&self) -> AgentBindings {
        let mut bindings = self.base.bindings();
        bindings.components = Some(self.runtime());
        bindings.profile_resolver = Arc::new(Catalog(self.registry.clone()));
        bindings.system_inputs = common::inputs();
        bindings.context_token_estimator = Some(Arc::new(Estimate));
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
        bindings.policy =
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(5)).unwrap());
        bindings.model_exchange = Arc::new(
            ModelExchange::new(self.model.clone(), bindings.policy.clone())
                .with_route_inspector(self.base.inspector.clone(), Duration::from_secs(5))
                .unwrap(),
        );
        bindings.settings.lease_ttl_ms = 30_000;
        bindings.settings.heartbeat_interval_ms = 5_000;
        bindings
    }
    pub fn agent(&self) -> Agent {
        create_agent(self.profile.clone(), self.bindings()).unwrap()
    }
    pub async fn start(&self, agent: &Agent) -> RunHandle {
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
    pub async fn outcome(&self, handle: &RunHandle) -> RunOutcome {
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
    pub async fn released(&self, handle: &RunHandle) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let view = agent_support::completed(
                    handle
                        .component_release(&agent_support::context())
                        .await
                        .unwrap(),
                );
                assert!(view.local_error.is_none());
                if let Some(report) = view.report {
                    assert!(report.failures.is_empty());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
    }
}
