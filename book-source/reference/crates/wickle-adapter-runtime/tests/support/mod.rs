//! Synthetic adapter metadata and observable lifecycle fixtures; no external services.

use super::core_fixture;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};
use wickle::*;
use wickle_adapter_runtime::*;

pub fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
pub fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
pub fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    }
}
pub fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
pub fn metadata(kind: ComponentKind, name: &str) -> ComponentMetadata {
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
pub fn export_metadata(name: &str, kind: ExportKind) -> ExportMetadata {
    ExportMetadata {
        export_id: id(name),
        kind,
        contract_version: 1,
        model_name: (kind == ExportKind::Tool).then(|| id(name)),
        hook_position: (kind == ExportKind::Hook).then_some(HookPosition::BeforeRun),
        capabilities: BTreeSet::new(),
        required_capabilities: BTreeSet::new(),
    }
}
pub fn descriptor(name: &str) -> ToolDescriptor {
    ToolDescriptor {
        tool: reference(name),
        name: id(name),
        description: format!("Read {name}"),
        input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters: vec!["query".into()],
        system_bindings: None,
        output_schema: json!({"type":"string"}),
        side_effect: ToolSideEffect::ReadOnly,
        concurrency: ToolConcurrency::Serial,
        retry: ToolRetryPolicy::Never,
        reconcile: false,
        max_output_bytes: 4096.try_into().unwrap(),
    }
}
pub fn definition(name: &str) -> AdapterDefinition {
    let tool = export_metadata("search", ExportKind::Tool);
    let source = export_metadata("recall", ExportKind::ContextSource);
    let consumer = export_metadata("record", ExportKind::EventConsumer);
    let hook = export_metadata("prepare", ExportKind::Hook);
    let mut metadata = metadata(ComponentKind::Adapter, name);
    metadata.config_schema = json!({"type":"object","properties":{"collection":{"type":"string","minLength":1}},"required":["collection"],"additionalProperties":false});
    metadata.required_connections.insert(id("main"));
    metadata.exports = vec![tool.clone(), source.clone(), consumer.clone(), hook.clone()];
    AdapterDefinition {
        metadata,
        exports: vec![
            AdapterExportDefinition::Tool {
                metadata: tool,
                descriptor: Box::new(descriptor("search")),
            },
            AdapterExportDefinition::ContextSource {
                metadata: source,
                definition: ContextSourceDefinition {
                    source: reference("recall"),
                    origin: ContextOrigin::Memory,
                    contract_version: 1,
                },
            },
            AdapterExportDefinition::EventConsumer { metadata: consumer },
            AdapterExportDefinition::Hook {
                metadata: hook,
                definition: HookDefinition {
                    hook: reference("prepare"),
                    position: HookPosition::BeforeRun,
                    priority: 0,
                    required: true,
                    timeout_ms: 1000,
                    max_output_bytes: 4096,
                },
            },
        ],
    }
}
pub fn inputs() -> SystemInputRegistry {
    SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap()
}
pub fn profile() -> AgentProfile {
    AgentProfile::from_json(r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1",
        "name":"Assistant","description":"Adapter runtime fixture","instructions":{"text":"Use supplied observations"},
        "model_binding":"primary","tools":[{"adapter_binding":"records","export_id":"search","alias":"search_records"}],"skills":[],
        "connectors":[{"binding_id":"data","connector_id":"database","version":"1"}],
        "adapters":[{"binding_id":"records","adapter_id":"adapter","version":"1","config":{"collection":"reports"},"connections":{"main":"data"}}],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":4,"max_tool_attempts":4,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}
    }"#).unwrap()
}
pub fn connection() -> ConnectionRegistration {
    ConnectionRegistration {
        binding: ConnectorBindingRef {
            binding_id: id("data"),
            connector_id: id("database"),
            version: id("1"),
        },
        metadata: metadata(ComponentKind::Connector, "database"),
        connection_ref: reference("database-account"),
    }
}

pub struct MetadataCatalog {
    pub components: Vec<ComponentMetadata>,
}
impl ProfileResolver for MetadataCatalog {
    fn resolve<'a>(
        &'a self,
        requested: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            if requested.kind == ComponentKind::ModelBinding {
                return Ok(metadata(ComponentKind::ModelBinding, requested.id.as_str()));
            }
            self.components
                .iter()
                .find(|component| {
                    component.reference.kind == requested.kind
                        && component.reference.id == requested.id
                        && (requested.version.is_none()
                            || component.reference.version == requested.version)
                })
                .cloned()
                .ok_or_else(|| {
                    ContractError::new(ErrorCode::ComponentUnavailable, "fixture.component")
                })
        })
    }
}

pub struct RegistryResolver<'a>(pub &'a AdapterRegistry);
impl ProfileResolver for RegistryResolver<'_> {
    fn resolve<'a>(
        &'a self,
        requested: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            if requested.kind == ComponentKind::ModelBinding {
                return Ok(metadata(ComponentKind::ModelBinding, requested.id.as_str()));
            }
            self.0.component_metadata(requested).ok_or_else(|| {
                ContractError::new(ErrorCode::ComponentUnavailable, "fixture.component")
            })
        })
    }
}
pub struct Fixture {
    pub definitions: Vec<AdapterDefinition>,
    pub factories: Vec<Arc<Factory>>,
    pub connections: Vec<ConnectionRegistration>,
    pub states: Vec<AdapterBindingState>,
    pub events: Arc<Mutex<Vec<String>>>,
    pub store: Arc<MemoryStateStore>,
    pub clock: Arc<TestClock>,
    pub policy: Arc<Policy>,
}
impl Fixture {
    pub fn new() -> Self {
        let definition = definition("adapter");
        let events = Arc::new(Mutex::new(vec![]));
        let store = Arc::new(MemoryStateStore::new());
        let factory = Arc::new(Factory::new(
            definition.clone(),
            events.clone(),
            store.clone(),
        ));
        Self {
            definitions: vec![definition],
            factories: vec![factory],
            connections: vec![connection()],
            states: vec![],
            events,
            store,
            clock: Arc::new(TestClock {
                origin: tokio::time::Instant::now(),
            }),
            policy: Arc::new(Policy {
                deny_open: AtomicUsize::new(0),
                deny_tool: AtomicUsize::new(0),
                require_approval: AtomicUsize::new(0),
                seen: Mutex::new(vec![]),
            }),
        }
    }
    pub fn registry(&self) -> Result<AdapterRegistry, ContractError> {
        AdapterRegistry::new(
            scope(),
            self.definitions
                .iter()
                .zip(&self.factories)
                .map(|(definition, factory)| AdapterRegistration {
                    definition: definition.clone(),
                    factory: factory.clone(),
                })
                .collect(),
            self.connections.clone(),
            vec![],
            vec![],
            self.states.clone(),
        )
    }
    pub async fn resolve(
        &self,
        registry: &AdapterRegistry,
        profile: &AgentProfile,
    ) -> Result<ResolvedAssembly, ContractError> {
        let resolved = ProfileValidator::new(&RegistryResolver(registry))
            .validate(profile, &scope())
            .await?;
        registry.resolve(&resolved, &resolve_context())
    }
    pub fn runtime(&self, registry: Arc<AdapterRegistry>) -> AdapterRuntime {
        AdapterRuntime::new(
            registry,
            self.store.clone(),
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(5)).unwrap()),
            self.clock.clone(),
        )
    }
    pub async fn admit(
        &self,
        registry: &AdapterRegistry,
        profile: &AgentProfile,
        run: &str,
        segment: &str,
    ) -> (ResolvedAssembly, ComponentBindContext) {
        let resolved = ProfileValidator::new(&RegistryResolver(registry))
            .validate(profile, &scope())
            .await
            .unwrap();
        let assembly = registry.resolve(&resolved, &resolve_context()).unwrap();
        let mut admission = core_fixture::admission(run, run, "session", "Read records", "1").await;
        admission.snapshot.profile = resolved;
        admission.snapshot.limits = profile.limits.clone();
        admission.snapshot.timing = RunTiming::new(0, profile.limits.max_elapsed_ms.get()).unwrap();
        let system = RunSystemInputs::capture(
            scope(),
            Some(SystemInputs::new(object(
                json!({"workspace_id":"11111111-1111-4111-8111-111111111111"}),
            ))),
            &inputs(),
        )
        .unwrap();
        let system_record = system.to_record(id(&format!("system-{run}")), 1);
        admission.snapshot.system_inputs =
            Some(system.snapshot_ref(system_record.reference()).unwrap());
        admission.records.push(system_record);
        let prompt = PromptSnapshot::create(
            &admission.snapshot.profile,
            vec!["Trusted Host".into()],
            None,
            assembly
                .tools()
                .iter()
                .map(|tool| PromptToolBinding {
                    selection: tool.selection.clone(),
                    compiled: tool.compiled.clone(),
                })
                .collect(),
            vec![],
        )
        .unwrap();
        let prompt_record = ProtectedRecord::new(
            id(&format!("assembly-prompt-{run}")),
            1,
            serde_json::to_value(prompt).unwrap(),
        );
        admission.prompt_snapshot = prompt_record.reference().clone();
        admission.records.push(prompt_record);
        admission.snapshot.request_digest = admission_digest(
            &admission.snapshot.request,
            &admission.snapshot.profile,
            admission.snapshot.system_inputs.as_ref(),
        );
        let record = ProtectedRecord::new(
            id(&format!("assembly-{run}")),
            1,
            serde_json::to_value(&assembly).unwrap(),
        );
        admission.snapshot.assembly_ref = Some(record.reference().clone());
        admission.records.push(record);
        if let RunEventPayload::RunStarted { profile_digest, .. } = &mut admission.events[0].payload
        {
            *profile_digest = admission.snapshot.profile.profile_digest().clone();
        }
        self.store.admit(&scope(), admission).await.unwrap();
        let lease = self
            .store
            .acquire_lease(
                &scope(),
                &id(run),
                &id("worker"),
                self.clock.now().unwrap().utc_ms,
                30_000,
            )
            .await
            .unwrap();
        (
            assembly,
            ComponentBindContext {
                scope: scope(),
                run_id: id(run),
                session_id: id("session"),
                binding_set_id: id(segment),
                principal_ref: id("actor"),
                capability_grant_ref: id("grant"),
                lease: Some(lease),
                purpose: ComponentBindPurpose::Execution,
                cancellation: Default::default(),
                deadline: tokio::time::Instant::now() + Duration::from_secs(30),
            },
        )
    }
    pub fn add_adapter(&mut self, name: &str) {
        let definition = definition(name);
        self.factories.push(Arc::new(Factory::new(
            definition.clone(),
            self.events.clone(),
            self.store.clone(),
        )));
        self.definitions.push(definition);
    }
}
pub struct TestClock {
    origin: tokio::time::Instant,
}
impl Clock for TestClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        let ms = self.origin.elapsed().as_millis() as u64;
        Ok(ClockReading {
            utc_ms: ms as i64,
            monotonic_ms: ms,
        })
    }
    fn sleep_until<'a>(&'a self, ms: u64) -> PortFuture<'a, ()> {
        Box::pin(async move {
            tokio::time::sleep_until(self.origin + Duration::from_millis(ms)).await;
            Ok(())
        })
    }
}
pub struct Policy {
    pub deny_open: AtomicUsize,
    pub deny_tool: AtomicUsize,
    pub require_approval: AtomicUsize,
    pub seen: Mutex<Vec<PolicyAction>>,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            self.seen.lock().unwrap().push(request.action.clone());
            if let PolicyAction::ExecuteTool { input } = &request.action {
                if self.deny_tool.load(Ordering::SeqCst) == 0
                    && self.require_approval.load(Ordering::SeqCst) > 0
                    && matches!(input.selection(),Some(ToolBindingRef::Export(export)) if export.adapter_binding==id("binding-1"))
                    && input.approval().is_none()
                {
                    return Ok(PolicyDecision::RequireApproval {
                        reason: id("review-write"),
                    });
                }
            }
            let denied = if matches!(request.action, PolicyAction::ExecuteTool { .. }) {
                self.deny_tool.load(Ordering::SeqCst) > 0
            } else {
                self.deny_open.load(Ordering::SeqCst) > 0
            };
            Ok(if denied {
                PolicyDecision::Deny {
                    reason: id("revoked"),
                }
            } else {
                PolicyDecision::Allow {}
            })
        })
    }
}
pub fn release_context(context: &ComponentBindContext) -> ComponentReleaseContext {
    ComponentReleaseContext {
        scope: context.scope.clone(),
        run_id: context.run_id.clone(),
        binding_set_id: context.binding_set_id.clone(),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(5),
    }
}
pub fn execution_context(context: &ComponentBindContext) -> ToolExecutionContext {
    ToolExecutionContext {
        run_id: context.run_id.clone(),
        binding_set_id: Some(context.binding_set_id.clone()),
        call_id: id("call"),
        attempt_id: id("attempt"),
        idempotency_key: id("effect-key"),
        scope: context.scope.clone(),
        principal_ref: context.principal_ref.clone(),
        capability_grant_ref: context.capability_grant_ref.clone(),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(5),
    }
}
pub fn multi_profile(names: &[&str]) -> AgentProfile {
    let mut selected = profile();
    selected.tools.clear();
    selected.adapters = Some(vec![]);
    for (index, name) in names.iter().enumerate() {
        let binding = id(&format!("binding-{index}"));
        selected.adapters.as_mut().unwrap().push(AdapterBindingRef {
            binding_id: binding.clone(),
            adapter_id: id(name),
            version: id("1"),
            config: Some(object(json!({"collection":"reports"}))),
            connections: BTreeMap::from([(id("main"), id("data"))]),
        });
        selected.tools.push(ToolBindingRef::Export(ExportRef {
            adapter_binding: binding,
            export_id: id("search"),
            alias: Some(id(&format!("search_{index}"))),
        }));
    }
    selected
}
pub fn resolve_context() -> ComponentResolveContext {
    ComponentResolveContext {
        scope: scope(),
        session_id: id("session"),
        principal_ref: id("actor"),
        capability_grant_ref: id("grant"),
        system_inputs: inputs(),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(30),
    }
}
pub struct Factory {
    pub definition: AdapterDefinition,
    pub opens: AtomicUsize,
    pub behavior: AtomicUsize,
    pub close_behavior: AtomicUsize,
    pub instances: Mutex<Vec<Arc<Instance>>>,
    pub observed: Mutex<Vec<AdapterInitContext>>,
    pub events: Arc<Mutex<Vec<String>>>,
    pub store: Arc<MemoryStateStore>,
    pub entered: Notify,
    pub release: Semaphore,
}
impl Factory {
    pub fn new(
        definition: AdapterDefinition,
        events: Arc<Mutex<Vec<String>>>,
        store: Arc<MemoryStateStore>,
    ) -> Self {
        Self {
            definition,
            opens: AtomicUsize::new(0),
            behavior: AtomicUsize::new(0),
            close_behavior: AtomicUsize::new(0),
            instances: Mutex::new(vec![]),
            observed: Mutex::new(vec![]),
            events,
            store,
            entered: Notify::new(),
            release: Semaphore::new(0),
        }
    }
}
impl AdapterFactory for Factory {
    fn open<'a>(
        &'a self,
        context: &'a AdapterInitContext,
    ) -> PortFuture<'a, Arc<dyn AdapterInstance>> {
        if self.behavior.load(Ordering::SeqCst) == 6 {
            self.opens.fetch_add(1, Ordering::SeqCst);
            self.events
                .lock()
                .unwrap()
                .push(format!("open:{}", context.binding.binding.binding_id));
            panic!("synthetic panic while constructing the factory future");
        }
        Box::pin(async move {
            self.opens.fetch_add(1, Ordering::SeqCst);
            self.events
                .lock()
                .unwrap()
                .push(format!("open:{}", context.binding.binding.binding_id));
            self.observed.lock().unwrap().push(context.clone());
            let saved = self
                .store
                .load(&context.execution.scope, &context.execution.run_id)
                .await?;
            assert!(saved.snapshot.assembly_ref.is_some());
            assert_eq!(
                saved.snapshot.request.session_id,
                context.execution.session_id
            );
            if context.execution.purpose == ComponentBindPurpose::Execution {
                assert!(context.execution.lease.is_some());
                self.store
                    .check_lease(
                        &context.execution.scope,
                        &context.execution.run_id,
                        context.execution.lease.as_ref().unwrap(),
                        saved.snapshot.timing.last_observed_at_ms,
                    )
                    .await?;
            }
            self.entered.notify_one();
            match self.behavior.load(Ordering::SeqCst) {
                1 => {
                    return Err(ContractError::new(
                        ErrorCode::InvalidContract,
                        "fixture.open",
                    ));
                }
                4 => return std::future::pending().await,
                5 => panic!("synthetic factory panic"),
                7 => {
                    self.release.acquire().await.unwrap().forget();
                }
                _ => {}
            }
            let executor = Arc::new(Executor {
                reconciliations: AtomicUsize::new(0),
                calls: AtomicUsize::new(0),
                seen: Mutex::new(vec![]),
                effect:if self.definition.exports.iter().any(|export|matches!(export,AdapterExportDefinition::Tool{descriptor,..} if descriptor.side_effect==ToolSideEffect::Write)){ToolEffect::Applied}else{ToolEffect::NotApplied},
            });
            let mut exports = vec![];
            for selected in &context.selected_exports {
                let export = self
                    .definition
                    .exports
                    .iter()
                    .find(|export| export.metadata().export_id == selected.export_id)
                    .unwrap();
                match export {
                    AdapterExportDefinition::Tool {
                        metadata,
                        descriptor,
                    } => {
                        let mut descriptor = descriptor.clone();
                        if self.behavior.load(Ordering::SeqCst) == 2 {
                            descriptor.output_schema = json!({"type":"integer"});
                        }
                        exports.push(AdapterExportInstance::Tool {
                            export_id: metadata.export_id.clone(),
                            descriptor,
                            executor: executor.clone(),
                        });
                    }
                    AdapterExportDefinition::Hook {
                        metadata,
                        definition,
                    } => exports.push(AdapterExportInstance::Hook {
                        export_id: metadata.export_id.clone(),
                        definition: definition.clone(),
                        handler: Arc::new(Hook),
                    }),
                    _ => panic!("metadata-only export must never activate"),
                }
            }
            if self.behavior.load(Ordering::SeqCst) == 3 {
                exports.push(AdapterExportInstance::Tool {
                    export_id: id("unselected"),
                    descriptor: Box::new(descriptor("unselected")),
                    executor: executor.clone(),
                });
            }
            let instance = Arc::new(Instance {
                binding: context.binding.binding.binding_id.clone(),
                scope: context.execution.scope.clone(),
                run_id: context.execution.run_id.clone(),
                binding_set_id: context.execution.binding_set_id.clone(),
                exports,
                executor,
                close_calls: AtomicUsize::new(0),
                close_entered: Notify::new(),
                close_behavior: self.close_behavior.load(Ordering::SeqCst),
                events: self.events.clone(),
            });
            self.instances.lock().unwrap().push(instance.clone());
            Ok(instance as Arc<dyn AdapterInstance>)
        })
    }
}
pub struct Executor {
    pub reconciliations: AtomicUsize,
    pub calls: AtomicUsize,
    pub effect: ToolEffect,
    pub seen: Mutex<Vec<(JsonObject, ToolExecutionContext)>>,
}
impl ToolExecutor for Executor {
    fn execute<'a>(
        &'a self,
        arguments: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.seen
                .lock()
                .unwrap()
                .push((arguments.clone(), context.clone()));
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: json!("found"),
                },
                effect: self.effect,
                receipt: (self.effect == ToolEffect::Applied).then(
                    || json!({"effect_id":"fixture-effect","target":arguments["workspace_id"]}),
                ),
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        arguments: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolReconciliation> {
        Box::pin(async move {
            self.reconciliations.fetch_add(1, Ordering::SeqCst);
            let known = self.seen.lock().unwrap().iter().any(|(args, prior)| {
                args == arguments
                    && prior.idempotency_key == context.idempotency_key
                    && prior.call_id == context.call_id
                    && prior.attempt_id == context.attempt_id
            });
            if !known {
                return Ok(ToolReconciliation::Unknown);
            }
            Ok(ToolReconciliation::Known {
                result: ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Succeeded {
                        value: json!("found"),
                    },
                    effect: self.effect,
                    receipt: (self.effect == ToolEffect::Applied).then(
                        || json!({"effect_id":"fixture-effect","target":arguments["workspace_id"]}),
                    ),
                },
            })
        })
    }
}
struct Hook;
impl HookHandler for Hook {
    fn call<'a>(&'a self, _: &'a HookInput, _: &'a HookContext) -> PortFuture<'a, HookOutput> {
        Box::pin(async { Ok(HookOutput::Context { additions: vec![] }) })
    }
}
pub struct Instance {
    pub binding: Id,
    pub scope: Scope,
    pub run_id: Id,
    pub binding_set_id: Id,
    pub exports: Vec<AdapterExportInstance>,
    pub executor: Arc<Executor>,
    pub close_calls: AtomicUsize,
    pub close_entered: Notify,
    pub close_behavior: usize,
    pub events: Arc<Mutex<Vec<String>>>,
}
impl AdapterInstance for Instance {
    fn exports(&self) -> Vec<AdapterExportInstance> {
        self.exports.clone()
    }
    fn close<'a>(&'a self, context: &'a AdapterCloseContext) -> PortFuture<'a, ()> {
        Box::pin(async move {
            assert_eq!(context.scope, self.scope);
            assert_eq!(context.run_id, self.run_id);
            assert_eq!(context.binding_set_id, self.binding_set_id);
            assert_eq!(context.adapter_binding, self.binding);
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            self.close_entered.notify_one();
            self.events
                .lock()
                .unwrap()
                .push(format!("close:{}", self.binding));
            match self.close_behavior {
                1 => Err(ContractError::new(
                    ErrorCode::InvalidContract,
                    "fixture.close",
                )),
                2 => std::future::pending().await,
                3 => panic!("synthetic close panic"),
                _ => Ok(()),
            }
        })
    }
}
