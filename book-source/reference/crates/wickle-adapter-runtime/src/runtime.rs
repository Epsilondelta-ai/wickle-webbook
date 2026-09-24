use crate::{
    AdapterRegistry,
    lifecycle::{ReleaseOwner, ScopedHook, ScopedSource, ScopedTool, SegmentLifetime},
};
use futures_util::FutureExt;
use std::{
    collections::BTreeMap,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use wickle::*;

/// Finite initialization and individual cleanup limits. The caller also supplies
/// a segment deadline and a total release deadline.
#[derive(Debug, Clone, Copy)]
pub struct AdapterRuntimeSettings {
    /// Maximum time for one factory, further bounded by the initialization deadline.
    pub open_timeout_ms: u64,
    /// Maximum time for one close, further bounded by the total release deadline.
    pub close_timeout_ms: u64,
}
impl Default for AdapterRuntimeSettings {
    fn default() -> Self {
        Self {
            open_timeout_ms: 30_000,
            close_timeout_ms: 1_000,
        }
    }
}

/// Scope-bound reference runtime. It stages all selected instances privately,
/// attests their returned contracts, and publishes one scoped binding set.
#[derive(Clone)]
pub struct AdapterRuntime {
    registry: Arc<AdapterRegistry>,
    state: Arc<dyn StateStore>,
    policy: Arc<PolicyGate>,
    clock: Arc<dyn Clock>,
    settings: AdapterRuntimeSettings,
    failed_cleanup: Arc<Mutex<BTreeMap<(Id, Id), ComponentReleaseReport>>>,
}
impl AdapterRuntime {
    /// Construct without metadata lookup, factory invocation, or external I/O.
    pub fn new(
        registry: Arc<AdapterRegistry>,
        state: Arc<dyn StateStore>,
        policy: Arc<PolicyGate>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            registry,
            state,
            policy,
            clock,
            settings: AdapterRuntimeSettings::default(),
            failed_cleanup: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
    /// Reject zero or excessive timeouts before any factory can run.
    pub fn with_settings(
        mut self,
        settings: AdapterRuntimeSettings,
    ) -> Result<Self, ContractError> {
        if settings.open_timeout_ms == 0
            || settings.open_timeout_ms > 30_000
            || settings.close_timeout_ms == 0
            || settings.close_timeout_ms > 30_000
        {
            return Err(error(ErrorCode::InvalidConfiguration, "adapter.timeouts"));
        }
        self.settings = settings;
        Ok(self)
    }
    /// Consume a process-local report if rollback of failed initialization had
    /// cleanup failures. These diagnostics do not change a Run outcome.
    pub fn take_failed_cleanup_report(
        &self,
        run_id: &Id,
        binding_set_id: &Id,
    ) -> Result<Option<ComponentReleaseReport>, ContractError> {
        Ok(self
            .failed_cleanup
            .lock()
            .map_err(|_| error(ErrorCode::InvalidContract, "adapter.cleanup_report"))?
            .remove(&(run_id.clone(), binding_set_id.clone())))
    }
    async fn check_admission(
        &self,
        assembly: &ResolvedAssembly,
        context: &ComponentBindContext,
    ) -> Result<(), ContractError> {
        if assembly.scope() != &context.scope
            || self.registry.scope() != &context.scope
            || assembly.session_id() != &context.session_id
        {
            return Err(error(ErrorCode::AccessDenied, "adapter.scope"));
        }
        let saved = bounded(
            &context.cancellation,
            context.deadline,
            self.state.load(&context.scope, &context.run_id),
        )
        .await?;
        let reference = saved
            .snapshot
            .assembly_ref
            .as_ref()
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "adapter.admission"))?;
        if saved.snapshot.request.session_id != context.session_id
            || reference.digest != assembly.digest()
            || saved.snapshot.profile.resolution_digest() != assembly.profile_resolution_digest()
        {
            return Err(error(ErrorCode::ProfileMismatch, "adapter.admission"));
        }
        let record = bounded(
            &context.cancellation,
            context.deadline,
            self.state.read_record(&context.scope, reference),
        )
        .await?;
        if record.reference() != reference || canonical_digest(record.value()) != assembly.digest()
        {
            return Err(error(ErrorCode::InvalidSnapshot, "adapter.assembly_record"));
        }
        match context.purpose {
            ComponentBindPurpose::Execution => {
                if saved.snapshot.status != RunStatus::Running {
                    return Err(error(ErrorCode::InvalidTransition, "adapter.execution"));
                }
                let lease = context
                    .lease
                    .as_ref()
                    .ok_or_else(|| error(ErrorCode::LeaseLost, "adapter.lease"))?;
                let now = self.clock.now()?.utc_ms;
                let current = bounded(
                    &context.cancellation,
                    context.deadline,
                    self.state
                        .check_lease(&context.scope, &context.run_id, lease, now),
                )
                .await?;
                if self.clock.now()?.utc_ms >= current.expires_at_ms {
                    return Err(error(ErrorCode::LeaseLost, "adapter.lease"));
                }
            }
            ComponentBindPurpose::ObserversOnly => {
                if saved.snapshot.outcome.is_none()
                    || (saved.snapshot.status != RunStatus::Waiting
                        && !saved.snapshot.status.is_terminal())
                {
                    return Err(error(ErrorCode::InvalidTransition, "adapter.observers"));
                }
            }
        }
        Ok(())
    }
    async fn authorize_binding(
        &self,
        binding: &ResolvedAdapterBinding,
        context: &ComponentBindContext,
    ) -> Result<(), ContractError> {
        let request = PolicyRequest {
            owner_scope: context.scope.clone(),
            resource_id: context.run_id.clone(),
            action: PolicyAction::BindAdapter {
                binding_id: binding.binding.binding_id.clone(),
                adapter: VersionedRef {
                    id: binding.binding.adapter_id.clone(),
                    version: binding.binding.version.clone(),
                },
                definition_digest: binding.definition_digest.clone(),
                connections: binding
                    .connections
                    .iter()
                    .map(|(name, connection)| (name.clone(), connection.connection_ref.clone()))
                    .collect(),
                binding_set_id: context.binding_set_id.clone(),
                purpose: context.purpose,
            },
        };
        authorize(
            &self.policy,
            &request,
            &execution_context(context),
            context.deadline,
        )
        .await
    }
    async fn bind_owned(
        &self,
        assembly: ResolvedAssembly,
        context: ComponentBindContext,
    ) -> Result<BoundCapabilities, ContractError> {
        self.check_admission(&assembly, &context).await?;
        self.registry.ensure_assembly(&assembly)?;
        let lifetime = Arc::new(SegmentLifetime {
            scope: context.scope.clone(),
            run_id: context.run_id.clone(),
            binding_set_id: context.binding_set_id.clone(),
            stopped: CancellationToken::new(),
        });
        let mut instances: Vec<(Id, Arc<dyn AdapterInstance>)> = vec![];
        let mut opened_bindings = vec![];
        let staged = AssertUnwindSafe(async {
            let mut exports: BTreeMap<(Id, Id), AdapterExportInstance> = BTreeMap::new();
            for binding in assembly.adapters() {
                let selected = active_exports(&assembly, binding, context.purpose)?;
                if selected.is_empty() {
                    continue;
                }
                self.authorize_binding(binding, &context).await?;
                self.check_admission(&assembly, &context).await?;
                let registration = self
                    .registry
                    .adapter(&VersionedRef {
                        id: binding.binding.adapter_id.clone(),
                        version: binding.binding.version.clone(),
                    })
                    .ok_or_else(|| error(ErrorCode::ComponentUnavailable, "adapter.factory"))?;
                let mut execution = context.clone();
                execution.deadline = execution.deadline.min(
                    tokio::time::Instant::now()
                        + Duration::from_millis(self.settings.open_timeout_ms),
                );
                execution.cancellation = context.cancellation.child_token();
                let initialization = AdapterInitContext {
                    execution,
                    binding: binding.clone(),
                    selected_exports: selected.clone(),
                };
                let _cancel = initialization.execution.cancellation.clone().drop_guard();
                let instance = bounded(
                    &initialization.execution.cancellation,
                    initialization.execution.deadline,
                    async {
                        AssertUnwindSafe(async { registration.factory.open(&initialization).await })
                            .catch_unwind()
                            .await
                            .map_err(|_| error(ErrorCode::InvalidContract, "adapter.open"))?
                            .map_err(|failure| error(failure.code, "adapter.open"))
                    },
                )
                .await?;
                instances.push((binding.binding.binding_id.clone(), instance.clone()));
                opened_bindings.push(binding.clone());
                let returned = std::panic::catch_unwind(AssertUnwindSafe(|| instance.exports()))
                    .map_err(|_| error(ErrorCode::InvalidContract, "adapter.exports"))?;
                if returned.len() != selected.len() {
                    return Err(error(ErrorCode::InvalidContract, "adapter.export_count"));
                }
                for export in returned {
                    let export_id = match &export {
                        AdapterExportInstance::Tool { export_id, .. }
                        | AdapterExportInstance::Hook { export_id, .. }
                        | AdapterExportInstance::ContextSource { export_id, .. } => export_id,
                    };
                    let selection = selected
                        .iter()
                        .find(|selection| &selection.export_id == export_id)
                        .ok_or_else(|| {
                            error(ErrorCode::InvalidReference, "adapter.unselected_export")
                        })?;
                    attest_export(assembly.export(selection)?, &export)?;
                    if exports
                        .insert(
                            (binding.binding.binding_id.clone(), export_id.clone()),
                            export,
                        )
                        .is_some()
                    {
                        return Err(error(
                            ErrorCode::InvalidReference,
                            "adapter.duplicate_export",
                        ));
                    }
                }
            }
            self.registry.ensure_assembly(&assembly)?;
            self.check_admission(&assembly, &context).await?;
            for binding in &opened_bindings {
                self.authorize_binding(binding, &context).await?;
            }
            let metadata_tools =
                ToolRegistry::metadata(context.scope.clone(), assembly.tools().to_vec())?;
            let mut tools = vec![];
            for binding in assembly.tools() {
                let executor: Arc<dyn ToolExecutor> = if context.purpose
                    == ComponentBindPurpose::ObserversOnly
                {
                    metadata_tools
                        .get(&binding.compiled.descriptor().name)
                        .ok_or_else(|| error(ErrorCode::InvalidReference, "adapter.metadata_tool"))?
                        .executor
                        .clone()
                } else {
                    match &binding.selection {
                        ToolBindingRef::Catalog(reference) => self
                            .registry
                            .catalog_tool(&VersionedRef {
                                id: reference.tool_id.clone(),
                                version: reference.version.clone(),
                            })
                            .ok_or_else(|| {
                                error(ErrorCode::ComponentUnavailable, "adapter.catalog_tool")
                            })?
                            .tool
                            .executor
                            .clone(),
                        ToolBindingRef::Export(reference) => match exports.get(&(
                            reference.adapter_binding.clone(),
                            reference.export_id.clone(),
                        )) {
                            Some(AdapterExportInstance::Tool { executor, .. }) => executor.clone(),
                            _ => {
                                return Err(error(
                                    ErrorCode::InvalidReference,
                                    "adapter.tool_export",
                                ));
                            }
                        },
                    }
                };
                tools.push((
                    binding.selection.clone(),
                    ToolRegistration {
                        compiled: binding.compiled.clone(),
                        executor: Arc::new(ScopedTool {
                            lifetime: lifetime.clone(),
                            executor,
                        }),
                    },
                ));
            }
            let mut hooks = vec![];
            for binding in assembly.hooks() {
                let active = context.purpose == ComponentBindPurpose::Execution
                    || matches!(
                        binding.definition.position,
                        HookPosition::AfterTool | HookPosition::AfterRun
                    );
                let handler: Arc<dyn HookHandler> = if !active {
                    Arc::new(InactiveHook)
                } else {
                    match &binding.selection {
                        HookRef::Catalog(reference) => self
                            .registry
                            .catalog_hook(&VersionedRef {
                                id: reference.hook_id.clone(),
                                version: reference.version.clone(),
                            })
                            .ok_or_else(|| {
                                error(ErrorCode::ComponentUnavailable, "adapter.catalog_hook")
                            })?
                            .hook
                            .handler
                            .clone(),
                        HookRef::Export(reference) => match exports.get(&(
                            reference.adapter_binding.clone(),
                            reference.export_id.clone(),
                        )) {
                            Some(AdapterExportInstance::Hook { handler, .. }) => handler.clone(),
                            _ => {
                                return Err(error(
                                    ErrorCode::InvalidReference,
                                    "adapter.hook_export",
                                ));
                            }
                        },
                    }
                };
                hooks.push((
                    binding.selection.clone(),
                    HookRegistration {
                        definition: binding.definition.clone(),
                        handler: Arc::new(ScopedHook {
                            lifetime: lifetime.clone(),
                            handler,
                        }),
                    },
                ));
            }
            let sources = if context.purpose == ComponentBindPurpose::ObserversOnly {
                ContextSourceRegistry::metadata(context.scope.clone(), assembly.sources().to_vec())?
            } else {
                let mut sources: Vec<ContextSourceRegistration> = Vec::new();
                for binding in assembly.sources() {
                    if sources
                        .iter()
                        .any(|entry| entry.selection == binding.binding.source)
                    {
                        continue;
                    }
                    let source = match &binding.binding.source {
                        ContextSourceRef::Catalog(reference) => self
                            .registry
                            .catalog_source(&VersionedRef {
                                id: reference.source_id.clone(),
                                version: reference.version.clone(),
                            })
                            .ok_or_else(|| {
                                error(ErrorCode::ComponentUnavailable, "adapter.catalog_source")
                            })?
                            .source
                            .source
                            .clone(),
                        ContextSourceRef::Export(reference) => match exports.get(&(
                            reference.adapter_binding.clone(),
                            reference.export_id.clone(),
                        )) {
                            Some(AdapterExportInstance::ContextSource { source, .. }) => {
                                source.clone()
                            }
                            _ => {
                                return Err(error(
                                    ErrorCode::InvalidReference,
                                    "adapter.source_export",
                                ));
                            }
                        },
                    };
                    sources.push(ContextSourceRegistration {
                        selection: binding.binding.source.clone(),
                        definition: binding.definition.clone(),
                        source: Arc::new(ScopedSource {
                            lifetime: lifetime.clone(),
                            selection: binding.binding.source.clone(),
                            definition: binding.definition.clone(),
                            source,
                        }),
                    });
                }
                ContextSourceRegistry::new(context.scope.clone(), sources)?
            };
            Ok::<_, ContractError>((
                Arc::new(ToolRegistry::from_bindings(context.scope.clone(), tools)?),
                Arc::new(HookRegistry::from_bindings(context.scope.clone(), hooks)?),
                Arc::new(sources),
            ))
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(error(ErrorCode::InvalidContract, "adapter.staging")));
        let release = Arc::new(ReleaseOwner::new(
            lifetime.clone(),
            instances,
            self.settings.close_timeout_ms,
        ));
        match staged {
            Ok((tools, hooks, sources)) => BoundCapabilities::new(
                context.scope,
                context.run_id,
                context.binding_set_id,
                tools,
                hooks,
                sources,
                release,
            ),
            Err(failure) => {
                let report = release.release(&cleanup_context(&context)).await;
                if let Ok(report) = report {
                    if !report.failures.is_empty() {
                        if let Ok(mut reports) = self.failed_cleanup.lock() {
                            reports.insert((context.run_id, context.binding_set_id), report);
                        }
                    }
                }
                Err(failure)
            }
        }
    }
}

impl ComponentRuntime for AdapterRuntime {
    fn resolve<'a>(
        &'a self,
        profile: &'a ResolvedProfile,
        context: &'a ComponentResolveContext,
    ) -> PortFuture<'a, ResolvedAssembly> {
        Box::pin(async move {
            if &context.scope != self.registry.scope() || profile.scope() != &context.scope {
                return Err(error(ErrorCode::AccessDenied, "adapter.scope"));
            }
            let execution = ExecutionContext::new(
                ExecutionContextData {
                    scope: context.scope.clone(),
                    principal_ref: context.principal_ref.clone(),
                    capability_grant_ref: context.capability_grant_ref.clone(),
                    trace_context: None,
                    system_inputs: None,
                },
                context.cancellation.clone(),
            );
            let request = PolicyRequest {
                owner_scope: context.scope.clone(),
                resource_id: context.session_id.clone(),
                action: PolicyAction::ResolveComponents {
                    profile_resolution_digest: profile.resolution_digest().clone(),
                },
            };
            authorize(&self.policy, &request, &execution, context.deadline).await?;
            let assembly = self.registry.resolve(profile, context)?;
            if context.cancellation.is_cancelled()
                || tokio::time::Instant::now() >= context.deadline
            {
                return Err(error(ErrorCode::DeadlineExceeded, "adapter.resolve"));
            }
            Ok(assembly)
        })
    }
    fn bind<'a>(
        &'a self,
        assembly: &'a ResolvedAssembly,
        context: &'a ComponentBindContext,
    ) -> PortFuture<'a, BoundCapabilities> {
        let runtime = self.clone();
        let assembly = assembly.clone();
        let context = context.clone();
        Box::pin(async move {
            let executor = tokio::runtime::Handle::try_current()
                .map_err(|_| error(ErrorCode::RuntimeUnavailable, "adapter.runtime"))?;
            let (sender, receiver) = tokio::sync::oneshot::channel();
            executor.spawn(async move {
                let result = runtime.bind_owned(assembly, context.clone()).await;
                if let Err(Ok(bound)) = sender.send(result) {
                    let _ = bound.release(&cleanup_context(&context)).await;
                }
            });
            receiver
                .await
                .map_err(|_| error(ErrorCode::InvalidContract, "adapter.coordinator"))?
        })
    }
}

struct InactiveHook;
impl HookHandler for InactiveHook {
    fn call<'a>(&'a self, _: &'a HookInput, _: &'a HookContext) -> PortFuture<'a, HookOutput> {
        Box::pin(async {
            Err(error(
                ErrorCode::CapabilityUnsupported,
                "adapter.inactive_hook",
            ))
        })
    }
}
fn active_exports(
    assembly: &ResolvedAssembly,
    binding: &ResolvedAdapterBinding,
    purpose: ComponentBindPurpose,
) -> Result<Vec<ExportRef>, ContractError> {
    binding
        .selected_exports
        .iter()
        .filter_map(|selection| {
            let export = match assembly.export(selection) {
                Ok(export) => export,
                Err(error) => return Some(Err(error)),
            };
            let active = match export {
                AdapterExportDefinition::Tool { .. }
                | AdapterExportDefinition::ContextSource { .. } => {
                    purpose == ComponentBindPurpose::Execution
                }
                AdapterExportDefinition::Hook { definition, .. } => {
                    purpose == ComponentBindPurpose::Execution
                        || matches!(
                            definition.position,
                            HookPosition::AfterTool | HookPosition::AfterRun
                        )
                }
                _ => false,
            };
            active.then(|| Ok(selection.clone()))
        })
        .collect()
}
fn attest_export(
    expected: &AdapterExportDefinition,
    actual: &AdapterExportInstance,
) -> Result<(), ContractError> {
    let matches = match (expected, actual) {
        (
            AdapterExportDefinition::Tool {
                metadata,
                descriptor: expected,
            },
            AdapterExportInstance::Tool {
                export_id,
                descriptor,
                ..
            },
        ) => metadata.export_id == *export_id && expected == descriptor,
        (
            AdapterExportDefinition::Hook {
                metadata,
                definition: expected,
            },
            AdapterExportInstance::Hook {
                export_id,
                definition,
                ..
            },
        ) => metadata.export_id == *export_id && expected == definition,
        (
            AdapterExportDefinition::ContextSource {
                metadata,
                definition: expected,
            },
            AdapterExportInstance::ContextSource {
                export_id,
                definition,
                ..
            },
        ) => metadata.export_id == *export_id && expected == definition,
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(error(ErrorCode::ProfileMismatch, "adapter.export_contract"))
    }
}
fn execution_context(context: &ComponentBindContext) -> ExecutionContext {
    ExecutionContext::new(
        ExecutionContextData {
            scope: context.scope.clone(),
            principal_ref: context.principal_ref.clone(),
            capability_grant_ref: context.capability_grant_ref.clone(),
            trace_context: None,
            system_inputs: None,
        },
        context.cancellation.clone(),
    )
}
fn cleanup_context(context: &ComponentBindContext) -> ComponentReleaseContext {
    ComponentReleaseContext {
        scope: context.scope.clone(),
        run_id: context.run_id.clone(),
        binding_set_id: context.binding_set_id.clone(),
        cancellation: CancellationToken::new(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(30),
    }
}
async fn authorize(
    policy: &PolicyGate,
    request: &PolicyRequest,
    context: &ExecutionContext,
    deadline: tokio::time::Instant,
) -> Result<(), ContractError> {
    match policy.check(request, context, Some(deadline), None).await? {
        PolicyDecision::Allow {} => Ok(()),
        _ => Err(error(ErrorCode::AccessDenied, "adapter.policy")),
    }
}
async fn bounded<T>(
    cancellation: &CancellationToken,
    deadline: tokio::time::Instant,
    future: impl Future<Output = Result<T, ContractError>>,
) -> Result<T, ContractError> {
    tokio::select! { biased;
        _ = cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "adapter.operation")),
        _ = tokio::time::sleep_until(deadline) => Err(error(ErrorCode::DeadlineExceeded, "adapter.operation")),
        result = future => result,
    }
}
fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
