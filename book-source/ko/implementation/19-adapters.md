# 19장 전체 Rust 구현과 테스트

[강의로](../19-adapters.md) · [전체 변경 패치](../solutions/19-adapters.patch)

기준 `5dcc36182a452fa1549c2fd700c54fd3f0d3293a`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle-adapter-runtime/src/lib.rs`

```rust
//! Host-side assembly and scoped lifetimes for approved Wickle adapters.
//!
//! Definitions and factories are registered by the embedding application.
//! Profile data never specifies executable paths or grants connection access.

mod lifecycle;
mod registry;
mod runtime;

pub use registry::{
    AdapterRegistration, AdapterRegistry, CatalogHookRegistration, CatalogToolRegistration,
    ConnectionRegistration,
};
pub use runtime::{AdapterRuntime, AdapterRuntimeSettings};
```

## `crates/wickle-adapter-runtime/src/lifecycle.rs`

```rust
use futures_util::FutureExt;
use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use wickle::*;

pub(crate) struct SegmentLifetime {
    pub scope: Scope,
    pub run_id: Id,
    pub binding_set_id: Id,
    pub stopped: CancellationToken,
}
impl SegmentLifetime {
    fn check(&self, scope: &Scope, binding_set: Option<&Id>) -> Result<(), ContractError> {
        if scope != &self.scope || binding_set != Some(&self.binding_set_id) {
            return Err(ContractError::new(
                ErrorCode::AccessDenied,
                "adapter.segment",
            ));
        }
        if self.stopped.is_cancelled() {
            return Err(ContractError::new(
                ErrorCode::InvalidTransition,
                "adapter.released",
            ));
        }
        Ok(())
    }
}

pub(crate) struct ScopedTool {
    pub lifetime: Arc<SegmentLifetime>,
    pub executor: Arc<dyn ToolExecutor>,
}
impl ToolExecutor for ScopedTool {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.lifetime
                .check(&context.scope, context.binding_set_id.as_ref())?;
            if context.run_id != self.lifetime.run_id {
                return Err(ContractError::new(ErrorCode::AccessDenied, "adapter.run"));
            }
            let mut controlled = context.clone();
            controlled.cancellation = CancellationToken::new();
            let _cancel = controlled.cancellation.clone().drop_guard();
            tokio::select! { biased;
                _ = self.lifetime.stopped.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "adapter.released")),
                _ = context.cancellation.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "adapter.tool")),
                _ = tokio::time::sleep_until(context.deadline) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "adapter.tool")),
                result = self.executor.execute(args, &controlled) => result,
            }
        })
    }
}

pub(crate) struct ScopedHook {
    pub lifetime: Arc<SegmentLifetime>,
    pub handler: Arc<dyn HookHandler>,
}
impl HookHandler for ScopedHook {
    fn call<'a>(
        &'a self,
        input: &'a HookInput,
        context: &'a HookContext,
    ) -> PortFuture<'a, HookOutput> {
        Box::pin(async move {
            self.lifetime
                .check(&context.scope, context.binding_set_id.as_ref())?;
            if context.run_id != self.lifetime.run_id {
                return Err(ContractError::new(ErrorCode::AccessDenied, "adapter.run"));
            }
            let mut controlled = context.clone();
            controlled.cancellation = CancellationToken::new();
            let _cancel = controlled.cancellation.clone().drop_guard();
            tokio::select! { biased;
                _ = self.lifetime.stopped.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "adapter.released")),
                _ = context.cancellation.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "adapter.hook")),
                _ = tokio::time::sleep_until(context.deadline) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "adapter.hook")),
                result = self.handler.call(input, &controlled) => result,
            }
        })
    }
}

struct ReleaseState {
    pending: Vec<(Id, Arc<dyn AdapterInstance>)>,
    report: ComponentReleaseReport,
}
pub(crate) struct ReleaseOwner {
    pub lifetime: Arc<SegmentLifetime>,
    close_timeout_ms: u64,
    state: Mutex<ReleaseState>,
}
impl ReleaseOwner {
    pub fn new(
        lifetime: Arc<SegmentLifetime>,
        instances: Vec<(Id, Arc<dyn AdapterInstance>)>,
        close_timeout_ms: u64,
    ) -> Self {
        Self {
            lifetime,
            close_timeout_ms,
            state: Mutex::new(ReleaseState {
                pending: instances,
                report: ComponentReleaseReport::default(),
            }),
        }
    }
}
impl ComponentRelease for ReleaseOwner {
    fn invalidate(&self) {
        self.lifetime.stopped.cancel();
    }

    fn release<'a>(
        &'a self,
        context: &'a ComponentReleaseContext,
    ) -> PortFuture<'a, ComponentReleaseReport> {
        Box::pin(async move {
            if context.scope != self.lifetime.scope
                || context.run_id != self.lifetime.run_id
                || context.binding_set_id != self.lifetime.binding_set_id
            {
                return Err(ContractError::new(
                    ErrorCode::AccessDenied,
                    "adapter.release_scope",
                ));
            }
            self.invalidate();
            let mut state = tokio::select! { biased;
                _ = context.cancellation.cancelled() => return Err(ContractError::new(ErrorCode::Cancelled, "adapter.release")),
                _ = tokio::time::sleep_until(context.deadline) => return Err(ContractError::new(ErrorCode::DeadlineExceeded, "adapter.release")),
                state = self.state.lock() => state,
            };
            while let Some((binding, instance)) = state.pending.last().cloned() {
                let deadline = (tokio::time::Instant::now()
                    + Duration::from_millis(self.close_timeout_ms))
                .min(context.deadline);
                let close = AdapterCloseContext {
                    scope: context.scope.clone(),
                    run_id: context.run_id.clone(),
                    binding_set_id: context.binding_set_id.clone(),
                    adapter_binding: binding.clone(),
                    cancellation: context.cancellation.child_token(),
                    deadline,
                };
                let _cancel = close.cancellation.clone().drop_guard();
                let operation =
                    AssertUnwindSafe(async { instance.close(&close).await }).catch_unwind();
                let result = tokio::select! { biased;
                    _ = context.cancellation.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "adapter.close")),
                    _ = tokio::time::sleep_until(deadline) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "adapter.close")),
                    result = operation => result.unwrap_or_else(|_| Err(ContractError::new(ErrorCode::InvalidContract, "adapter.close"))),
                };
                close.cancellation.cancel();
                state.pending.pop();
                if let Err(error) = result {
                    let code = serde_json::to_value(error.code)
                        .ok()
                        .and_then(|value| value.as_str().map(str::to_owned))
                        .unwrap_or_else(|| "invalid_contract".into());
                    state.report.failures.push(ComponentReleaseFailure {
                        adapter_binding: binding,
                        code: Id::new(code)?,
                    });
                }
            }
            Ok(state.report.clone())
        })
    }
}
```

## `crates/wickle-adapter-runtime/src/registry.rs`

```rust
use std::sync::Arc;
use wickle::*;

/// Trusted definition associated with an existing Host factory.
#[derive(Clone)]
pub struct AdapterRegistration {
    /// Immutable complete metadata and export contracts.
    pub definition: AdapterDefinition,
    /// Approved Host code; registration does not open it.
    pub factory: Arc<dyn AdapterFactory>,
}
/// Host connector registration contains only metadata and a connection revision.
pub type ConnectionRegistration = ResolvedConnection;
/// Existing catalog tool implementation and its full profile metadata.
#[derive(Clone)]
pub struct CatalogToolRegistration {
    /// Exact metadata resolved by the profile resolver.
    pub metadata: ComponentMetadata,
    /// Existing compiled contract and Host executor.
    pub tool: ToolRegistration,
}
/// Existing catalog Hook implementation and its full profile metadata.
#[derive(Clone)]
pub struct CatalogHookRegistration {
    /// Exact metadata resolved by the profile resolver.
    pub metadata: ComponentMetadata,
    /// Existing lifecycle definition and callback.
    pub hook: HookRegistration,
}

/// Immutable scope-local metadata and implementation registry. Construction and
/// resolution never invoke factories, tools, hooks, source readers or consumers.
pub struct AdapterRegistry {
    pub(crate) scope: Scope,
    pub(crate) adapters: Vec<AdapterRegistration>,
    pub(crate) connections: Vec<ConnectionRegistration>,
    pub(crate) tools: Vec<CatalogToolRegistration>,
    pub(crate) hooks: Vec<CatalogHookRegistration>,
    pub(crate) states: Vec<AdapterBindingState>,
}
impl std::fmt::Debug for AdapterRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdapterRegistry")
            .field("adapter_count", &self.adapters.len())
            .field("connection_count", &self.connections.len())
            .finish_non_exhaustive()
    }
}
impl AdapterRegistry {
    /// Validate a complete scope-local catalog without calling any implementation.
    pub fn new(
        scope: Scope,
        adapters: Vec<AdapterRegistration>,
        connections: Vec<ConnectionRegistration>,
        tools: Vec<CatalogToolRegistration>,
        hooks: Vec<CatalogHookRegistration>,
        states: Vec<AdapterBindingState>,
    ) -> Result<Self, ContractError> {
        let invalid = || error(ErrorCode::InvalidContract, "registry.duplicate");
        for (index, adapter) in adapters.iter().enumerate() {
            adapter.definition.validate()?;
            if adapters[..index].iter().any(|prior| {
                prior.definition.metadata.reference == adapter.definition.metadata.reference
            }) {
                return Err(invalid());
            }
        }
        for (index, connection) in connections.iter().enumerate() {
            if connection.metadata.reference.kind != ComponentKind::Connector
                || connection.metadata.reference.id != connection.binding.connector_id
                || connection.metadata.reference.version.as_ref()
                    != Some(&connection.binding.version)
                || connections[..index].iter().any(|prior| {
                    prior.binding.binding_id == connection.binding.binding_id
                        || (prior.metadata.reference == connection.metadata.reference
                            && prior.metadata != connection.metadata)
                })
            {
                return Err(invalid());
            }
        }
        for (index, tool) in tools.iter().enumerate() {
            if tool.metadata.reference.kind != ComponentKind::Tool
                || tool.metadata.reference.id != tool.tool.compiled.descriptor().tool.id
                || tool.metadata.reference.version.as_ref()
                    != Some(&tool.tool.compiled.descriptor().tool.version)
                || tool.metadata.model_name.as_ref() != Some(&tool.tool.compiled.descriptor().name)
                || tools[..index]
                    .iter()
                    .any(|prior| prior.metadata.reference == tool.metadata.reference)
            {
                return Err(invalid());
            }
        }
        for (index, hook) in hooks.iter().enumerate() {
            hook.hook.definition.validate()?;
            if hook.metadata.reference.kind != ComponentKind::Hook
                || hook.metadata.reference.id != hook.hook.definition.hook.id
                || hook.metadata.reference.version.as_ref()
                    != Some(&hook.hook.definition.hook.version)
                || hook.metadata.hook_position != Some(hook.hook.definition.position)
                || hooks[..index]
                    .iter()
                    .any(|prior| prior.metadata.reference == hook.metadata.reference)
            {
                return Err(invalid());
            }
        }
        for (index, state) in states.iter().enumerate() {
            if state.scope != scope
                || canonical_digest(&state.value) != state.state_ref.digest
                || states[..index].iter().any(|prior| {
                    prior.session_id == state.session_id
                        && prior.adapter_binding == state.adapter_binding
                        && prior.adapter == state.adapter
                        && prior.definition_digest == state.definition_digest
                })
            {
                return Err(invalid());
            }
        }
        Ok(Self {
            scope,
            adapters,
            connections,
            tools,
            hooks,
            states,
        })
    }
    /// Exact namespace for all registrations and prepared binding states.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Exact factory lookup. Alias/model-name guessing is never used.
    pub fn adapter(&self, reference: &VersionedRef) -> Option<&AdapterRegistration> {
        self.adapters.iter().find(|adapter| {
            adapter.definition.metadata.reference.id == reference.id
                && adapter.definition.metadata.reference.version.as_ref()
                    == Some(&reference.version)
        })
    }
    /// Look up one exact native catalog tool version.
    pub fn catalog_tool(&self, reference: &VersionedRef) -> Option<&CatalogToolRegistration> {
        self.tools
            .iter()
            .find(|entry| entry.tool.compiled.descriptor().tool == *reference)
    }
    /// Look up one exact native catalog Hook version.
    pub fn catalog_hook(&self, reference: &VersionedRef) -> Option<&CatalogHookRegistration> {
        self.hooks
            .iter()
            .find(|entry| entry.hook.definition.hook == *reference)
    }
    /// Metadata for a Host ProfileResolver; unrelated model/assets remain in the
    /// Host's resolver and do not become adapter-owned components.
    pub fn component_metadata(&self, reference: &ComponentRef) -> Option<ComponentMetadata> {
        self.adapters
            .iter()
            .map(|entry| &entry.definition.metadata)
            .chain(self.connections.iter().map(|entry| &entry.metadata))
            .chain(self.tools.iter().map(|entry| &entry.metadata))
            .chain(self.hooks.iter().map(|entry| &entry.metadata))
            .find(|metadata| &metadata.reference == reference)
            .cloned()
    }
    /// Resolve and attest one complete profile selection without external I/O.
    pub fn resolve(
        &self,
        profile: &ResolvedProfile,
        context: &ComponentResolveContext,
    ) -> Result<ResolvedAssembly, ContractError> {
        if &context.scope != self.scope() || profile.scope() != self.scope() {
            return Err(error(ErrorCode::AccessDenied, "registry.scope"));
        }
        if context.cancellation.is_cancelled() {
            return Err(error(ErrorCode::Cancelled, "registry.resolve"));
        }
        if tokio::time::Instant::now() >= context.deadline {
            return Err(error(ErrorCode::DeadlineExceeded, "registry.resolve"));
        }
        let selected = profile.profile();
        let connections = selected
            .connectors
            .iter()
            .map(|selected| {
                self.connections
                    .iter()
                    .find(|entry| &entry.binding == selected)
                    .cloned()
                    .ok_or_else(|| error(ErrorCode::ComponentUnavailable, "registry.connection"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut adapters = Vec::new();
        for binding in selected.adapters.iter().flatten() {
            let reference = VersionedRef {
                id: binding.adapter_id.clone(),
                version: binding.version.clone(),
            };
            let registered = self
                .adapter(&reference)
                .ok_or_else(|| error(ErrorCode::ComponentUnavailable, "registry.adapter"))?;
            let selected_exports = selected
                .tools
                .iter()
                .filter_map(|tool| {
                    if let ToolBindingRef::Export(export) = tool {
                        Some(export)
                    } else {
                        None
                    }
                })
                .chain(selected.hooks.iter().flatten().filter_map(|hook| {
                    if let HookRef::Export(export) = hook {
                        Some(export)
                    } else {
                        None
                    }
                }))
                .filter(|export| export.adapter_binding == binding.binding_id)
                .cloned()
                .collect();
            let mapped = binding
                .connections
                .iter()
                .map(|(name, id)| {
                    connections
                        .iter()
                        .find(|connection| &connection.binding.binding_id == id)
                        .cloned()
                        .map(|connection| (name.clone(), connection))
                        .ok_or_else(|| {
                            error(ErrorCode::InvalidReference, "registry.connection_binding")
                        })
                })
                .collect::<Result<_, _>>()?;
            let digest = registered.definition.digest();
            let state = self
                .states
                .iter()
                .find(|state| {
                    state.session_id == context.session_id
                        && state.adapter_binding == binding.binding_id
                        && state.adapter == reference
                        && state.definition_digest == digest
                })
                .cloned();
            adapters.push(ResolvedAdapterBinding {
                binding: binding.clone(),
                definition: registered.definition.clone(),
                definition_digest: digest,
                connections: mapped,
                selected_exports,
                binding_state: state,
            });
        }
        let lookup_export = |reference: &ExportRef| {
            adapters
                .iter()
                .find(|adapter| adapter.binding.binding_id == reference.adapter_binding)
                .and_then(|adapter| {
                    adapter
                        .definition
                        .exports
                        .iter()
                        .find(|export| export.metadata().export_id == reference.export_id)
                })
                .ok_or_else(|| error(ErrorCode::InvalidReference, "registry.export"))
        };
        let mut tools = Vec::new();
        for selection in &selected.tools {
            let (metadata, mut descriptor) = match selection {
                ToolBindingRef::Catalog(reference) => {
                    let entry = self
                        .catalog_tool(&VersionedRef {
                            id: reference.tool_id.clone(),
                            version: reference.version.clone(),
                        })
                        .ok_or_else(|| error(ErrorCode::ComponentUnavailable, "registry.tool"))?;
                    (
                        Some(entry.metadata.clone()),
                        entry.tool.compiled.descriptor().clone(),
                    )
                }
                ToolBindingRef::Export(reference) => {
                    let AdapterExportDefinition::Tool { descriptor, .. } =
                        lookup_export(reference)?
                    else {
                        return Err(error(ErrorCode::InvalidReference, "registry.export_kind"));
                    };
                    (None, descriptor.as_ref().clone())
                }
            };
            if let ToolBindingRef::Export(ExportRef {
                alias: Some(alias), ..
            }) = selection
            {
                descriptor.name = alias.clone();
            }
            tools.push(ResolvedToolBinding {
                selection: selection.clone(),
                metadata,
                compiled: SchemaCompiler::new().compile(descriptor, &context.system_inputs)?,
            });
        }
        let mut hooks = Vec::new();
        for selection in selected.hooks.iter().flatten() {
            let (metadata, definition) = match selection {
                HookRef::Catalog(reference) => {
                    let entry = self
                        .catalog_hook(&VersionedRef {
                            id: reference.hook_id.clone(),
                            version: reference.version.clone(),
                        })
                        .ok_or_else(|| error(ErrorCode::ComponentUnavailable, "registry.hook"))?;
                    (Some(entry.metadata.clone()), entry.hook.definition.clone())
                }
                HookRef::Export(reference) => {
                    let AdapterExportDefinition::Hook { definition, .. } =
                        lookup_export(reference)?
                    else {
                        return Err(error(ErrorCode::InvalidReference, "registry.export_kind"));
                    };
                    (None, definition.clone())
                }
            };
            hooks.push(ResolvedHookBinding {
                selection: selection.clone(),
                metadata,
                definition,
            });
        }
        ResolvedAssembly::new(profile, context, connections, adapters, tools, hooks)
    }
    /// Refuse metadata/connection/state drift before opening any resource.
    pub fn ensure_assembly(&self, assembly: &ResolvedAssembly) -> Result<(), ContractError> {
        let mismatch = || error(ErrorCode::ProfileMismatch, "registry.assembly");
        if assembly.scope() != self.scope() {
            return Err(mismatch());
        }
        for connection in assembly.connections() {
            if !self.connections.contains(connection) {
                return Err(mismatch());
            }
        }
        for binding in assembly.adapters() {
            let entry = self
                .adapter(&VersionedRef {
                    id: binding.binding.adapter_id.clone(),
                    version: binding.binding.version.clone(),
                })
                .ok_or_else(mismatch)?;
            if entry.definition != binding.definition {
                return Err(mismatch());
            }
            if let Some(state) = &binding.binding_state {
                if !self.states.contains(state) {
                    return Err(mismatch());
                }
            }
        }
        for binding in assembly.tools() {
            if let ToolBindingRef::Catalog(reference) = &binding.selection {
                let entry = self
                    .catalog_tool(&VersionedRef {
                        id: reference.tool_id.clone(),
                        version: reference.version.clone(),
                    })
                    .ok_or_else(mismatch)?;
                if binding.metadata.as_ref() != Some(&entry.metadata)
                    || entry.tool.compiled.descriptor() != binding.compiled.descriptor()
                {
                    return Err(mismatch());
                }
            }
        }
        for binding in assembly.hooks() {
            if let HookRef::Catalog(reference) = &binding.selection {
                let entry = self
                    .catalog_hook(&VersionedRef {
                        id: reference.hook_id.clone(),
                        version: reference.version.clone(),
                    })
                    .ok_or_else(mismatch)?;
                if binding.metadata.as_ref() != Some(&entry.metadata)
                    || entry.hook.definition != binding.definition
                {
                    return Err(mismatch());
                }
            }
        }
        Ok(())
    }
}
fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
```

## `crates/wickle-adapter-runtime/src/runtime.rs`

```rust
use crate::{
    AdapterRegistry,
    lifecycle::{ReleaseOwner, ScopedHook, ScopedTool, SegmentLifetime},
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
                        | AdapterExportInstance::Hook { export_id, .. } => export_id,
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
            Ok::<_, ContractError>((
                Arc::new(ToolRegistry::from_bindings(context.scope.clone(), tools)?),
                Arc::new(HookRegistry::from_bindings(context.scope.clone(), hooks)?),
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
            Ok((tools, hooks)) => BoundCapabilities::new(
                context.scope,
                context.run_id,
                context.binding_set_id,
                tools,
                hooks,
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
                AdapterExportDefinition::Tool { .. } => purpose == ComponentBindPurpose::Execution,
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
```

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
    assert_eq!(next.execution.principal_ref, id("reviewer"));
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
```

## `crates/wickle-adapter-runtime/tests/hook_exports.rs`

```rust
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
```

## `crates/wickle-adapter-runtime/tests/runtime_lifecycle.rs`

```rust
//! Adapter resources open after durable admission and close without repeating business effects.

#[path = "../../wickle/tests/support/mod.rs"]
#[allow(dead_code)]
mod core_fixture;
#[allow(dead_code)]
mod support;
use serde_json::json;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use support::*;
use wickle::*;
use wickle_adapter_runtime::*;

#[tokio::test]
async fn an_exact_tool_subset_opens_only_after_admission_and_lease_then_releases_once() {
    let fixture = Fixture::new();
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture.runtime(registry.clone());
    let assembly = fixture.resolve(&registry, &profile()).await.unwrap();
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    let (admitted, context) = fixture.admit(&registry, &profile(), "run", "segment").await;
    let bound = runtime.bind(&admitted, &context).await.unwrap();
    assert_eq!(
        serde_json::to_value(assembly).unwrap(),
        serde_json::to_value(admitted).unwrap()
    );
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.factories[0].observed.lock().unwrap()[0].selected_exports,
        vec![ExportRef {
            adapter_binding: id("records"),
            export_id: id("search"),
            alias: Some(id("search_records"))
        }]
    );
    assert!(bound.tools().get(&id("search_records")).is_some());
    assert!(bound.tools().get(&id("recall")).is_none());
    assert!(bound.tools().get(&id("record")).is_none());
    assert_eq!(bound.scope(), &scope());
    assert_eq!(bound.binding_set_id(), &context.binding_set_id);
    let report = bound.release(&release_context(&context)).await.unwrap();
    assert!(report.failures.is_empty());
    assert_eq!(
        bound.release(&release_context(&context)).await.unwrap(),
        report
    );
    assert_eq!(
        *fixture.events.lock().unwrap(),
        vec!["open:records", "close:records"]
    );
    assert_eq!(
        fixture.factories[0].instances.lock().unwrap()[0]
            .close_calls
            .load(Ordering::SeqCst),
        1
    );
}

#[tokio::test]
async fn missing_admission_wrong_scope_or_lost_lease_prevents_factory_entry() {
    let fixture = Fixture::new();
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture.runtime(registry.clone());
    let (assembly, context) = fixture.admit(&registry, &profile(), "run", "segment").await;
    let mut absent = context.clone();
    absent.run_id = id("not-admitted");
    assert!(runtime.bind(&assembly, &absent).await.is_err());
    let mut foreign = context.clone();
    foreign.scope.tenant_id = id("foreign");
    assert!(runtime.bind(&assembly, &foreign).await.is_err());
    let mut lease_missing = context.clone();
    lease_missing.lease = None;
    assert!(runtime.bind(&assembly, &lease_missing).await.is_err());
    fixture
        .store
        .release_lease(
            &scope(),
            &context.run_id,
            context.lease.as_ref().unwrap(),
            fixture.clock.now().unwrap().utc_ms,
        )
        .await
        .unwrap();
    assert!(runtime.bind(&assembly, &context).await.is_err());
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_later_open_failure_closes_earlier_instances_in_reverse_without_publishing_tools() {
    for behavior in [1, 5, 6] {
        let mut fixture = Fixture::new();
        fixture.add_adapter("second");
        fixture.add_adapter("third");
        fixture.factories[2]
            .behavior
            .store(behavior, Ordering::SeqCst);
        let selected = multi_profile(&["adapter", "second", "third"]);
        let registry = Arc::new(fixture.registry().unwrap());
        let runtime = fixture.runtime(registry.clone());
        let (assembly, context) = fixture.admit(&registry, &selected, "run", "segment").await;
        assert!(runtime.bind(&assembly, &context).await.is_err());
        assert_eq!(
            *fixture.events.lock().unwrap(),
            vec![
                "open:binding-0",
                "open:binding-1",
                "open:binding-2",
                "close:binding-1",
                "close:binding-0"
            ]
        );
        for factory in &fixture.factories[..2] {
            let instances = factory.instances.lock().unwrap();
            assert_eq!(instances[0].executor.calls.load(Ordering::SeqCst), 0);
            assert_eq!(instances[0].close_calls.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn wrong_runtime_descriptor_or_unrequested_exports_fail_attestation_and_close_the_instance() {
    for behavior in [2, 3] {
        let fixture = Fixture::new();
        fixture.factories[0]
            .behavior
            .store(behavior, Ordering::SeqCst);
        let registry = Arc::new(fixture.registry().unwrap());
        let runtime = fixture.runtime(registry.clone());
        let (assembly, context) = fixture.admit(&registry, &profile(), "run", "segment").await;
        assert!(runtime.bind(&assembly, &context).await.is_err());
        assert_eq!(
            *fixture.events.lock().unwrap(),
            vec!["open:records", "close:records"]
        );
        assert_eq!(
            fixture.factories[0].instances.lock().unwrap()[0]
                .executor
                .calls
                .load(Ordering::SeqCst),
            0
        );
    }
}

#[tokio::test(start_paused = true)]
async fn close_error_panic_and_timeout_do_not_skip_remaining_reverse_cleanup_or_change_the_run() {
    for behavior in [1, 2, 3] {
        let mut fixture = Fixture::new();
        fixture.add_adapter("second");
        fixture.factories[1]
            .close_behavior
            .store(behavior, Ordering::SeqCst);
        let selected = multi_profile(&["adapter", "second"]);
        let registry = Arc::new(fixture.registry().unwrap());
        let runtime = fixture
            .runtime(registry.clone())
            .with_settings(AdapterRuntimeSettings {
                open_timeout_ms: 30_000,
                close_timeout_ms: 10,
            })
            .unwrap();
        let (assembly, context) = fixture.admit(&registry, &selected, "run", "segment").await;
        let bound = runtime.bind(&assembly, &context).await.unwrap();
        let saved = fixture.store.load(&scope(), &context.run_id).await.unwrap();
        let report = bound.release(&release_context(&context)).await.unwrap();
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].adapter_binding, id("binding-1"));
        assert_eq!(
            *fixture.events.lock().unwrap(),
            vec![
                "open:binding-0",
                "open:binding-1",
                "close:binding-1",
                "close:binding-0"
            ]
        );
        assert_eq!(
            fixture
                .store
                .load(&scope(), &context.run_id)
                .await
                .unwrap()
                .snapshot,
            saved.snapshot
        );
        assert_eq!(
            bound.release(&release_context(&context)).await.unwrap(),
            report
        );
        assert_eq!(
            fixture.factories[0].instances.lock().unwrap()[0]
                .close_calls
                .load(Ordering::SeqCst),
            1
        );
    }
}

#[tokio::test]
async fn surviving_tool_handles_cannot_execute_after_release_or_with_another_segment_or_scope() {
    let fixture = Fixture::new();
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture.runtime(registry.clone());
    let (assembly, context) = fixture.admit(&registry, &profile(), "run", "segment").await;
    let bound = runtime.bind(&assembly, &context).await.unwrap();
    let tool = bound
        .tools()
        .get(&id("search_records"))
        .unwrap()
        .executor
        .clone();
    let arguments =
        object(json!({"query":"report","workspace_id":"11111111-1111-4111-8111-111111111111"}));
    let execution = execution_context(&context);
    assert!(tool.execute(&arguments, &execution).await.is_ok());
    let mut other = execution.clone();
    other.binding_set_id = Some(id("other-segment"));
    assert!(tool.execute(&arguments, &other).await.is_err());
    other = execution.clone();
    other.run_id = id("other-run");
    assert!(tool.execute(&arguments, &other).await.is_err());
    other = execution.clone();
    other.scope.workspace_id = id("other-workspace");
    assert!(tool.execute(&arguments, &other).await.is_err());
    bound.release(&release_context(&context)).await.unwrap();
    assert!(tool.execute(&arguments, &execution).await.is_err());
    assert_eq!(
        fixture.factories[0].instances.lock().unwrap()[0]
            .executor
            .calls
            .load(Ordering::SeqCst),
        1
    );
    let mut next = context.clone();
    next.binding_set_id = id("fresh-segment");
    let next_bound = runtime.bind(&assembly, &next).await.unwrap();
    assert!(
        next_bound
            .tools()
            .get(&id("search_records"))
            .unwrap()
            .executor
            .execute(&arguments, &execution_context(&next))
            .await
            .is_ok()
    );
    assert!(
        tool.execute(&arguments, &execution_context(&next))
            .await
            .is_err()
    );
    next_bound.release(&release_context(&next)).await.unwrap();
    assert_eq!(fixture.factories[0].instances.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn two_bindings_of_the_same_export_keep_separate_instances_and_arguments() {
    let fixture = Fixture::new();
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture.runtime(registry.clone());
    let selected = multi_profile(&["adapter", "adapter"]);
    let (assembly, context) = fixture.admit(&registry, &selected, "run", "segment").await;
    let bound = runtime.bind(&assembly, &context).await.unwrap();
    for index in 0..2 {
        let name = id(&format!("search_{index}"));
        let tool = bound.tools().get(&name).unwrap();
        assert_eq!(bound.tools().selection(&name), Some(&selected.tools[index]));
        let arguments = object(
            json!({"query":format!("binding-{index}"),"workspace_id":"11111111-1111-4111-8111-111111111111"}),
        );
        tool.executor
            .execute(&arguments, &execution_context(&context))
            .await
            .unwrap();
    }
    {
        let instances = fixture.factories[0].instances.lock().unwrap();
        assert_eq!(instances.len(), 2);
        assert_eq!(
            instances[0].executor.seen.lock().unwrap()[0].0["query"],
            json!("binding-0")
        );
        assert_eq!(
            instances[1].executor.seen.lock().unwrap()[0].0["query"],
            json!("binding-1")
        );
    }
    bound.release(&release_context(&context)).await.unwrap();
}

#[tokio::test]
async fn initialization_requires_current_permission_and_opens_only_after_it_is_granted() {
    let fixture = Fixture::new();
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture.runtime(registry.clone());
    let (assembly, context) = fixture.admit(&registry, &profile(), "run", "segment").await;
    fixture.policy.deny_open.store(1, Ordering::SeqCst);
    assert!(runtime.bind(&assembly, &context).await.is_err());
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    fixture.policy.deny_open.store(0, Ordering::SeqCst);
    let bound = runtime.bind(&assembly, &context).await.unwrap();
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 1);
    bound.release(&release_context(&context)).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_timed_out_later_factory_still_releases_the_already_open_instance() {
    let mut fixture = Fixture::new();
    fixture.add_adapter("second");
    fixture.factories[1].behavior.store(4, Ordering::SeqCst);
    let selected = multi_profile(&["adapter", "second"]);
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture
        .runtime(registry.clone())
        .with_settings(AdapterRuntimeSettings {
            open_timeout_ms: 10,
            close_timeout_ms: 10,
        })
        .unwrap();
    let (assembly, context) = fixture.admit(&registry, &selected, "run", "segment").await;
    let started = tokio::time::Instant::now();
    assert!(runtime.bind(&assembly, &context).await.is_err());
    assert!(started.elapsed() <= Duration::from_millis(30));
    assert_eq!(
        *fixture.events.lock().unwrap(),
        vec!["open:binding-0", "open:binding-1", "close:binding-0"]
    );
    assert_eq!(
        fixture.factories[0].instances.lock().unwrap()[0]
            .executor
            .calls
            .load(Ordering::SeqCst),
        0
    );
}

#[tokio::test]
async fn observer_only_binding_requires_a_committed_outcome_and_never_activates_a_tool_only_adapter()
 {
    let fixture = Fixture::new();
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture.runtime(registry.clone());
    let (assembly, context) = fixture.admit(&registry, &profile(), "run", "segment").await;
    let mut observer = context.clone();
    observer.binding_set_id = id("observer-segment");
    observer.purpose = ComponentBindPurpose::ObserversOnly;
    observer.lease = None;
    assert!(runtime.bind(&assembly, &observer).await.is_err());
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    let saved = fixture.store.load(&scope(), &context.run_id).await.unwrap();
    let finished = core_fixture::finished(
        &saved.snapshot,
        context.lease.clone().unwrap(),
        fixture.clock.now().unwrap().utc_ms,
    );
    fixture
        .store
        .commit(&scope(), &context.run_id, finished)
        .await
        .unwrap();
    let bound = runtime.bind(&assembly, &observer).await.unwrap();
    let metadata_tool = bound.tools().get(&id("search_records")).unwrap();
    let rejected = metadata_tool
        .executor
        .execute(
            &object(
                json!({"query":"report","workspace_id":"11111111-1111-4111-8111-111111111111"}),
            ),
            &execution_context(&observer),
        )
        .await
        .unwrap();
    assert_eq!(
        rejected.outcome,
        ToolExecutionOutcome::Failed {
            code: id("component_unavailable")
        }
    );
    assert_eq!(rejected.effect, ToolEffect::NotApplied);
    assert!(rejected.receipt.is_none());
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    assert!(
        bound
            .release(&release_context(&observer))
            .await
            .unwrap()
            .failures
            .is_empty()
    );
}

#[tokio::test]
async fn dropping_a_bind_waiter_closes_late_instances_instead_of_publishing_or_leaking_them() {
    let mut fixture = Fixture::new();
    fixture.add_adapter("second");
    fixture.factories[1].behavior.store(7, Ordering::SeqCst);
    let selected = multi_profile(&["adapter", "second"]);
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture.runtime(registry.clone());
    let (assembly, context) = fixture.admit(&registry, &selected, "run", "segment").await;
    let mut binding = runtime.bind(&assembly, &context);
    assert!(futures_util::poll!(binding.as_mut()).is_pending());
    tokio::time::timeout(
        Duration::from_secs(5),
        fixture.factories[1].entered.notified(),
    )
    .await
    .unwrap();
    drop(binding);
    fixture.factories[1].release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if fixture
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| event.starts_with("close:"))
                .count()
                == 2
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        *fixture.events.lock().unwrap(),
        vec![
            "open:binding-0",
            "open:binding-1",
            "close:binding-1",
            "close:binding-0"
        ]
    );
    assert!(fixture.factories.iter().all(|factory| {
        factory.instances.lock().unwrap()[0]
            .executor
            .calls
            .load(Ordering::SeqCst)
            == 0
    }));
}

#[tokio::test]
async fn permission_revoked_while_the_factory_opens_prevents_publication_and_closes_the_instance() {
    let fixture = Fixture::new();
    fixture.factories[0].behavior.store(7, Ordering::SeqCst);
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture.runtime(registry.clone());
    let (assembly, context) = fixture.admit(&registry, &profile(), "run", "segment").await;
    let mut binding = runtime.bind(&assembly, &context);
    assert!(futures_util::poll!(binding.as_mut()).is_pending());
    tokio::time::timeout(
        Duration::from_secs(5),
        fixture.factories[0].entered.notified(),
    )
    .await
    .unwrap();
    fixture.policy.deny_open.store(1, Ordering::SeqCst);
    fixture.factories[0].release.add_permits(1);
    assert!(binding.await.is_err());
    assert_eq!(
        *fixture.events.lock().unwrap(),
        vec!["open:records", "close:records"]
    );
    assert_eq!(
        fixture.factories[0].instances.lock().unwrap()[0]
            .executor
            .calls
            .load(Ordering::SeqCst),
        0
    );
}
```

## `crates/wickle-adapter-runtime/tests/runtime_registry.rs`

```rust
//! Metadata resolution validates exact selections without opening adapter instances.

#[path = "../../wickle/tests/support/mod.rs"]
#[allow(dead_code)]
mod core_fixture;
#[allow(dead_code)]
mod support;
use serde_json::json;
use std::sync::atomic::Ordering;
use support::*;
use wickle::*;

#[tokio::test]
async fn selecting_a_tool_does_not_activate_declared_context_or_event_exports() {
    let fixture = Fixture::new();
    let registry = fixture.registry().unwrap();
    let assembly = fixture.resolve(&registry, &profile()).await.unwrap();
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    assert!(fixture.events.lock().unwrap().is_empty());
    assert_eq!(
        assembly.adapters()[0].selected_exports,
        vec![ExportRef {
            adapter_binding: id("records"),
            export_id: id("search"),
            alias: Some(id("search_records"))
        }]
    );
    assert_eq!(assembly.tools().len(), 1);
    assert_eq!(
        assembly.tools()[0].compiled.descriptor().name,
        id("search_records")
    );
    assert!(assembly.hooks().is_empty());
}

#[test]
fn duplicate_adapter_and_connection_registrations_are_rejected_without_callbacks() {
    let mut fixture = Fixture::new();
    fixture.definitions.push(fixture.definitions[0].clone());
    fixture.factories.push(fixture.factories[0].clone());
    assert!(fixture.registry().is_err());
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    let mut fixture = Fixture::new();
    fixture.connections.push(fixture.connections[0].clone());
    assert!(fixture.registry().is_err());
    assert!(fixture.events.lock().unwrap().is_empty());
}

#[test]
fn descriptor_export_kind_name_and_protocol_version_must_match_the_registered_definition() {
    for case in 0..4 {
        let mut fixture = Fixture::new();
        match case {
            0 => {
                let AdapterExportDefinition::Tool { metadata, .. } =
                    &mut fixture.definitions[0].exports[0]
                else {
                    unreachable!()
                };
                metadata.kind = ExportKind::ContextSource;
            }
            1 => {
                let AdapterExportDefinition::Tool { descriptor, .. } =
                    &mut fixture.definitions[0].exports[0]
                else {
                    unreachable!()
                };
                descriptor.name = id("other-name");
            }
            2 => fixture.definitions[0].metadata.contract_version = 2,
            3 => {
                fixture.definitions[0].metadata.exports[0].contract_version = 2;
                let AdapterExportDefinition::Tool { metadata, .. } =
                    &mut fixture.definitions[0].exports[0]
                else {
                    unreachable!()
                };
                metadata.contract_version = 2;
            }
            _ => unreachable!(),
        }
        assert!(
            fixture.registry().is_err(),
            "mismatched case {case} must not register"
        );
        assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn wrong_version_config_connection_or_export_selection_fails_before_factory_open() {
    let fixture = Fixture::new();
    let registry = fixture.registry().unwrap();
    for case in 0..5 {
        let mut selected = profile();
        match case {
            0 => selected.adapters.as_mut().unwrap()[0].version = id("2"),
            1 => {
                selected.adapters.as_mut().unwrap()[0].config =
                    Some(object(json!({"collection":42})))
            }
            2 => {
                selected.adapters.as_mut().unwrap()[0]
                    .connections
                    .remove(&id("main"));
            }
            3 => {
                let ToolBindingRef::Export(export) = &mut selected.tools[0] else {
                    unreachable!()
                };
                export.export_id = id("recall"); // Existing export of the wrong kind.
            }
            4 => {
                let ToolBindingRef::Export(export) = &mut selected.tools[0] else {
                    unreachable!()
                };
                export.export_id = id("not-registered");
            }
            _ => unreachable!(),
        }
        assert!(
            fixture.resolve(&registry, &selected).await.is_err(),
            "selection case {case} must fail"
        );
    }
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    assert!(fixture.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn model_alias_collisions_are_rejected_instead_of_overwriting_another_binding() {
    let fixture = Fixture::new();
    let registry = fixture.registry().unwrap();
    let mut selected = profile();
    let mut second = selected.adapters.as_ref().unwrap()[0].clone();
    second.binding_id = id("other-records");
    selected.adapters.as_mut().unwrap().push(second);
    selected.tools.push(ToolBindingRef::Export(ExportRef {
        adapter_binding: id("other-records"),
        export_id: id("search"),
        alias: Some(id("search_records")),
    }));
    assert!(fixture.resolve(&registry, &selected).await.is_err());
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn exact_assembly_rejects_changed_descriptor_connection_and_host_mapping_revisions() {
    for case in 0..3 {
        let mut fixture = Fixture::new();
        let value = json!({"thread_id":"prepared-thread"});
        fixture.states.push(AdapterBindingState {
            scope: scope(),
            session_id: id("session"),
            adapter_binding: id("records"),
            adapter: reference("adapter"),
            definition_digest: fixture.definitions[0].digest(),
            state_ref: ProtectedRecord::new(id("mapping"), 1, value.clone())
                .reference()
                .clone(),
            value,
        });
        let registry = fixture.registry().unwrap();
        let assembly = fixture.resolve(&registry, &profile()).await.unwrap();
        assert_eq!(
            assembly.adapters()[0].binding_state.as_ref().unwrap().value,
            json!({"thread_id":"prepared-thread"})
        );
        assert!(registry.ensure_assembly(&assembly).is_ok());
        match case {
            0 => {
                let AdapterExportDefinition::Tool { descriptor, .. } =
                    &mut fixture.definitions[0].exports[0]
                else {
                    unreachable!()
                };
                descriptor.output_schema = json!({"type":"integer"});
            }
            1 => fixture.connections[0].connection_ref.version = id("different-account-revision"),
            2 => {
                fixture.states[0].value = json!({"thread_id":"replacement-thread"});
                fixture.states[0].state_ref =
                    ProtectedRecord::new(id("mapping"), 2, fixture.states[0].value.clone())
                        .reference()
                        .clone();
            }
            _ => unreachable!(),
        }
        let changed = fixture.registry().unwrap();
        assert!(changed.ensure_assembly(&assembly).is_err());
        assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn binding_state_is_selected_by_scope_session_and_binding_without_creating_a_new_mapping() {
    let mut fixture = Fixture::new();
    let value = json!({"thread_id":"prepared-thread"});
    fixture.states.push(AdapterBindingState {
        scope: scope(),
        session_id: id("session"),
        adapter_binding: id("records"),
        adapter: reference("adapter"),
        definition_digest: fixture.definitions[0].digest(),
        state_ref: ProtectedRecord::new(id("mapping"), 1, value.clone())
            .reference()
            .clone(),
        value,
    });
    let registry = fixture.registry().unwrap();
    let selected = profile();
    let profile = ProfileValidator::new(&RegistryResolver(&registry))
        .validate(&selected, &scope())
        .await
        .unwrap();
    let mut context = resolve_context();
    let original = registry.resolve(&profile, &context).unwrap();
    assert!(original.adapters()[0].binding_state.is_some());
    context.session_id = id("another-session");
    assert!(
        registry.resolve(&profile, &context).unwrap().adapters()[0]
            .binding_state
            .is_none()
    );
    context.scope.tenant_id = id("foreign");
    assert!(registry.resolve(&profile, &context).is_err());
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn missing_system_input_definitions_and_explicit_unsupported_sources_fail_resolution() {
    let fixture = Fixture::new();
    let registry = fixture.registry().unwrap();
    let selected = profile();
    let resolved = ProfileValidator::new(&RegistryResolver(&registry))
        .validate(&selected, &scope())
        .await
        .unwrap();
    let mut context = resolve_context();
    context.system_inputs = SystemInputRegistry::new(vec![]).unwrap();
    assert!(registry.resolve(&resolved, &context).is_err());
    let mut value = serde_json::to_value(&selected).unwrap();
    value["context_sources"] = json!([{"source":{"adapter_binding":"records","export_id":"recall"},"trigger":"run_start","required":false,"timeout_ms":100,"max_items":1,"max_bytes":1024,"max_tokens":100}]);
    let with_source = AgentProfile::from_json(&value.to_string()).unwrap();
    let resolved = ProfileValidator::new(&RegistryResolver(&registry))
        .validate(&with_source, &scope())
        .await
        .unwrap();
    assert!(registry.resolve(&resolved, &resolve_context()).is_err());
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn internally_consistent_restored_profiles_cannot_select_one_logical_export_twice() {
    let fixture = Fixture::new();
    let registry = fixture.registry().unwrap();
    let original = profile();
    let resolved = ProfileValidator::new(&RegistryResolver(&registry))
        .validate(&original, &scope())
        .await
        .unwrap();
    let assembly = registry.resolve(&resolved, &resolve_context()).unwrap();
    let mut changed_profile = original.clone();
    let duplicate = ToolBindingRef::Export(ExportRef {
        adapter_binding: id("records"),
        export_id: id("search"),
        alias: Some(id("second_alias")),
    });
    changed_profile.tools.push(duplicate.clone());
    let mut restored = serde_json::to_value(&resolved).unwrap();
    restored["profile"] = serde_json::to_value(&changed_profile).unwrap();
    restored["profile_digest"] = serde_json::to_value(changed_profile.digest()).unwrap();
    restored["resolution_digest"] = serde_json::to_value(canonical_digest(&json!([
        restored["profile_digest"],
        restored["scope"],
        restored["components"]
    ])))
    .unwrap();
    let forged: ResolvedProfile = serde_json::from_value(restored).unwrap();
    let rejected = registry.resolve(&forged, &resolve_context()).unwrap_err();
    assert_eq!(rejected.path, "assembly.duplicate_export");
    let mut data = serde_json::to_value(&assembly).unwrap();
    data["profile_resolution_digest"] = serde_json::to_value(forged.resolution_digest()).unwrap();
    data["adapters"][0]["selected_exports"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::to_value(&duplicate).unwrap());
    let mut descriptor = assembly.tools()[0].compiled.descriptor().clone();
    descriptor.name = id("second_alias");
    let compiled = SchemaCompiler::new()
        .compile(descriptor, &inputs())
        .unwrap();
    let mut extra = data["tools"][0].clone();
    extra["selection"] = serde_json::to_value(&duplicate).unwrap();
    extra["compiled"] = serde_json::to_value(&compiled).unwrap();
    extra["compiled_digest"] = serde_json::to_value(compiled.digest()).unwrap();
    data["tools"].as_array_mut().unwrap().push(extra);
    let rejected = ResolvedAssembly::restore(
        &data.to_string(),
        &forged,
        &inputs(),
        &canonical_digest(&data),
    )
    .unwrap_err();
    assert_eq!(rejected.path, "assembly.duplicate_export");
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
}
```

## `crates/wickle-adapter-runtime/tests/support/mod.rs`

```rust
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
            AdapterExportDefinition::ContextSource { metadata: source },
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
                close_behavior: self.close_behavior.load(Ordering::SeqCst),
                events: self.events.clone(),
            });
            self.instances.lock().unwrap().push(instance.clone());
            Ok(instance as Arc<dyn AdapterInstance>)
        })
    }
}
pub struct Executor {
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
mod components;
mod driver;
mod hooks;
mod resume;
mod tools;
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
        if self.lease_ttl_ms == 0
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
    /// Optional component assembly/runtime. It owns all catalog and exported Tool/Hook selections.
    /// Direct tools/hooks cannot also be supplied when this is configured.
    pub components: Option<Arc<dyn ComponentRuntime>>,
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
    runs: Mutex<BTreeMap<Id, Arc<LocalRun>>>,
}
struct LocalRun {
    segment_start_revision: u64,
    cancel: CancellationToken,
    reason: Mutex<Option<Id>>,
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

/// Validate the initial text/turn-end runtime without invoking any Host callback.
/// Catalog tools use already-created executors. Asset loaders, adapter exports,
/// verifiers and extension execution require their separate runtime bindings.
pub fn create_agent(
    profile: AgentProfile,
    bindings: AgentBindings,
) -> Result<Agent, ContractError> {
    profile.validate_structure()?;
    bindings.settings.validate()?;
    if !matches!(profile.instructions, Instructions::Text(_))
        || !matches!(profile.output_contract, OutputContract::Text {})
        || !matches!(profile.completion_policy, CompletionPolicy::TurnEnd {})
        || !profile.skills.is_empty()
        || (bindings.components.is_none()
            && (!profile.connectors.is_empty()
                || profile.adapters.as_ref().is_some_and(|v| !v.is_empty())))
        || profile
            .context_sources
            .as_ref()
            .is_some_and(|v| !v.is_empty())
        || profile.extensions.as_ref().is_some_and(|v| !v.is_empty())
        || profile.context_policy.strategy.as_str() != "bounded"
    {
        return Err(fail(ErrorCode::CapabilityUnsupported, "agent.profile"));
    }
    if bindings.components.is_some() && (bindings.tools.is_some() || bindings.hooks.is_some()) {
        return Err(fail(
            ErrorCode::InvalidConfiguration,
            "agent.component_authority",
        ));
    }
    if bindings.components.is_none() {
        match &bindings.hooks {
            Some(hooks) => {
                if hooks.scope() != &bindings.scope {
                    return Err(fail(ErrorCode::AccessDenied, "agent.hooks_scope"));
                }
                hooks.plan(&profile)?;
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
                registry.prompt_bindings(&profile)?;
            }
            None if !profile.tools.is_empty() => {
                return Err(fail(ErrorCode::CapabilityUnsupported, "agent.tools"));
            }
            None => {}
        }
    }
    Ok(Agent {
        inner: Arc::new(Inner {
            profile,
            bindings,
            runs: Mutex::new(BTreeMap::new()),
        }),
    })
}

/// A durable observer. Dropping this value or its streams does not cancel the driver.
#[derive(Clone)]
pub struct RunHandle {
    agent: Agent,
    run_id: Id,
    segment_start_revision: u64,
    local: Option<Arc<LocalRun>>,
}
impl fmt::Debug for RunHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunHandle")
            .field("run_id", &self.run_id)
            .finish_non_exhaustive()
    }
}

/// Result of an authorized cancellation request, separate from stored RunOutcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReceipt {
    /// Signalled this process's live driver. Cancellation is not yet committed.
    Requested,
    /// The saved run is already terminal; its outcome was not changed.
    AlreadyTerminal,
    /// No local driver is owned here. No remote cancellation was accepted or sent.
    NotLocal,
}

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
            .run_view(&saved.snapshot, context, None)
            .await
    }
    /// Read protected saved state under the separate details permission.
    pub async fn get_run_details(
        &self,
        run_id: &Id,
        context: &ExecutionContext,
    ) -> Result<Guarded<RunSnapshot>, ContractError> {
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
        let agent = self.clone();
        runtime
            .spawn(async move { agent.resume_command(command, context).await })
            .await
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.resume"))?
    }
    fn check_scope(&self, context: &ExecutionContext) -> Result<(), ContractError> {
        if context.data.scope != self.inner.bindings.scope {
            return Err(fail(ErrorCode::AccessDenied, "scope"));
        }
        Ok(())
    }
    fn handle(&self, run_id: Id, segment_start_revision: u64) -> Result<RunHandle, ContractError> {
        let local = self
            .inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .get(&run_id)
            .filter(|local| local.segment_start_revision == segment_start_revision)
            .cloned();
        Ok(RunHandle {
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
            if let Some(receipt) = snapshot.resume_receipts.iter().find(|receipt| {
                receipt.previous_segment_start_revision == self.segment_start_revision
            }) {
                let record = caller_read(
                    context,
                    None,
                    self.agent
                        .inner
                        .bindings
                        .state
                        .read_record(&snapshot.scope, &receipt.previous_outcome_ref),
                )
                .await?;
                if record.reference() != &receipt.previous_outcome_ref {
                    return Err(fail(ErrorCode::InvalidSnapshot, "agent.segment_reference"));
                }
                let outcome = serde_json::from_value(record.value().clone())
                    .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.segment_outcome"))?;
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
            if segment_revision(&snapshot) != self.segment_start_revision {
                return Err(fail(ErrorCode::InvalidSnapshot, "agent.segment"));
            }
            if let Some(outcome) = snapshot.outcome {
                return Ok(Guarded::Completed(outcome));
            }
            self.local_error()?;
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
                            .segment_end(&saved.snapshot)?
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
                    if let Some(end) = handle.segment_end(&saved.snapshot)? {
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
    /// Signal only a locally owned driver after current CancelRun authorization.
    pub async fn cancel(
        &self,
        reason: Id,
        context: &ExecutionContext,
    ) -> Result<Guarded<CancelReceipt>, ContractError> {
        self.agent.check_scope(context)?;
        let bindings = &self.agent.inner.bindings;
        let saved = caller_read(
            context,
            None,
            bindings.state.load(&bindings.scope, &self.run_id),
        )
        .await?;
        let policy = PolicyRequest {
            owner_scope: saved.snapshot.scope,
            resource_id: self.run_id.clone(),
            action: PolicyAction::CancelRun {},
        };
        bindings
            .policy
            .guard(&policy, context, None, None, || async {
                if saved.snapshot.status.is_terminal() {
                    return Ok(CancelReceipt::AlreadyTerminal);
                }
                if saved.snapshot.status == RunStatus::Waiting {
                    return self
                        .agent
                        .cancel_waiting(self.run_id.clone(), reason, context.clone())
                        .await;
                }
                let current = self
                    .agent
                    .inner
                    .runs
                    .lock()
                    .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
                    .get(&self.run_id)
                    .cloned();
                if let Some(local) = current {
                    if !local.done.load(Ordering::Acquire) {
                        *local
                            .reason
                            .lock()
                            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.cancel"))? =
                            Some(reason);
                        local.cancel.cancel();
                        return Ok(CancelReceipt::Requested);
                    }
                }
                Ok(CancelReceipt::NotLocal)
            })
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
    fn segment_end(&self, snapshot: &RunSnapshot) -> Result<Option<u64>, ContractError> {
        if let Some(receipt) = snapshot
            .resume_receipts
            .iter()
            .find(|receipt| receipt.previous_segment_start_revision == self.segment_start_revision)
        {
            return Ok(Some(receipt.previous_last_event_seq));
        }
        if segment_revision(snapshot) != self.segment_start_revision {
            return Err(fail(ErrorCode::InvalidSnapshot, "agent.segment"));
        }
        Ok(
            (snapshot.status.is_terminal() || snapshot.status == RunStatus::Waiting)
                .then_some(snapshot.last_event_seq),
        )
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
```

## `crates/wickle/src/agent/admission.rs`

```rust
use super::*;
use std::panic::AssertUnwindSafe;

impl Agent {
    pub(super) async fn admit(
        &self,
        request: RunRequest,
        context: ExecutionContext,
    ) -> Result<Guarded<RunHandle>, ContractError> {
        self.check_scope(&context)?;
        let bindings = &self.inner.bindings;
        if serde_json::to_vec(&request)
            .map_err(|_| fail(ErrorCode::InvalidJson, "agent.request"))?
            .len()
            > bindings.settings.max_request_bytes
        {
            return Err(fail(ErrorCode::InvalidContract, "agent.request_size"));
        }
        if request
            .output_contract
            .as_ref()
            .is_some_and(|value| !matches!(value, OutputContract::Text {}))
            || request
                .input
                .iter()
                .any(|content| !matches!(content, InputContent::Text { .. }))
        {
            return Err(fail(ErrorCode::CapabilityUnsupported, "agent.request"));
        }
        let policy = PolicyRequest {
            owner_scope: bindings.scope.clone(),
            resource_id: request.request_id.clone(),
            action: PolicyAction::StartRun {},
        };
        if let Guarded::ApprovalRequired(challenge) = bindings
            .policy
            .guard(&policy, &context, None, None, || async { Ok(()) })
            .await?
        {
            return Ok(Guarded::ApprovalRequired(challenge));
        }
        let read_timeout = Some(Duration::from_millis(bindings.settings.start_timeout_ms));
        if let Some(saved) = caller_read(
            &context,
            read_timeout,
            bindings
                .state
                .find_request(&bindings.scope, &request.session_id, &request.request_id),
        )
        .await?
        {
            caller_read(
                &context,
                read_timeout,
                self.validate_replay(&request, &context, &saved),
            )
            .await?;
            let segment = segment_revision(&saved.snapshot);
            return Ok(Guarded::Completed(
                self.handle(saved.snapshot.run_id, segment)?,
            ));
        }
        // Preparation may be cancelled or time out. Once durable admission begins,
        // this owned coordinator waits for its result even if the caller disconnects.
        let prepared = AssertUnwindSafe(self.prepare(request.clone(), &context)).catch_unwind();
        let (input, prompt) = tokio::select! { biased;
            _ = context.cancellation.cancelled() => return Err(fail(ErrorCode::Cancelled, "agent.admission")),
            _ = tokio::time::sleep(Duration::from_millis(bindings.settings.start_timeout_ms)) => return Err(fail(ErrorCode::DeadlineExceeded, "agent.admission")),
            result = prepared => result.map_err(|_| fail(ErrorCode::InvalidContract, "agent.preparation"))??,
        };
        // Current admission permission is checked again after metadata preparation.
        if let Guarded::ApprovalRequired(challenge) = bindings
            .policy
            .guard(&policy, &context, None, None, || async { Ok(()) })
            .await?
        {
            return Ok(Guarded::ApprovalRequired(challenge));
        }
        let candidate_id = input.snapshot.run_id.clone();
        let admission = match AssertUnwindSafe(bindings.state.admit(&bindings.scope, input))
            .catch_unwind()
            .await
        {
            Ok(result) => result,
            Err(_) => Err(fail(ErrorCode::InvalidContract, "agent.admission")),
        };
        let result = match admission {
            Ok(result) => result,
            Err(original) => {
                // A lost commit acknowledgement must not leave our admitted run
                // without a driver or create a second request on retry.
                match bindings
                    .state
                    .find_request(&bindings.scope, &request.session_id, &request.request_id)
                    .await
                {
                    Ok(Some(saved)) => {
                        self.validate_replay(&request, &context, &saved).await?;
                        AdmissionResult {
                            created: saved.snapshot.run_id == candidate_id,
                            state: saved,
                        }
                    }
                    _ => return Err(original),
                }
            }
        };
        if !result.created {
            self.validate_replay(&request, &context, &result.state)
                .await?;
            return Ok(Guarded::Completed(self.handle(
                result.state.snapshot.run_id.clone(),
                segment_revision(&result.state.snapshot),
            )?));
        }
        let run_id = result.state.snapshot.run_id;
        let local = Arc::new(LocalRun::new(0));
        self.inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .insert(run_id.clone(), local.clone());
        let agent = self.clone();
        let driver_id = run_id.clone();
        let driver_local = local.clone();
        let mut data = context.data;
        // Runtime tool values remain in protected storage. The model driver has
        // no reason to carry the admission map into model callbacks.
        data.system_inputs = None;
        let driver_context = ExecutionContext::new(data, local.cancel.clone());
        tokio::spawn(async move {
            let result =
                AssertUnwindSafe(agent.drive(&driver_id, prompt, driver_context, &driver_local))
                    .catch_unwind()
                    .await;
            let error = match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(fail(error.code, "agent.driver")),
                Err(_) => Some(fail(ErrorCode::InvalidContract, "agent.driver")),
            };
            let completed = error.is_none() && !agent.keep_local(&driver_local);
            if let Ok(mut saved) = driver_local.error.lock() {
                *saved = error;
            }
            driver_local.done.store(true, Ordering::Release);
            driver_local.notify.notify_waiters();
            if completed {
                if let Ok(mut runs) = agent.inner.runs.lock() {
                    if runs
                        .get(&driver_id)
                        .is_some_and(|current| Arc::ptr_eq(current, &driver_local))
                    {
                        runs.remove(&driver_id);
                    }
                }
            }
        });
        Ok(Guarded::Completed(RunHandle {
            agent: self.clone(),
            run_id,
            segment_start_revision: 0,
            local: Some(local),
        }))
    }

    async fn validate_replay(
        &self,
        request: &RunRequest,
        context: &ExecutionContext,
        saved: &StoredRun,
    ) -> Result<(), ContractError> {
        if self.inner.profile.digest() != *saved.snapshot.profile.profile_digest()
            || admission_digest(
                request,
                &saved.snapshot.profile,
                saved.snapshot.system_inputs.as_ref(),
            ) != saved.snapshot.request_digest
        {
            return Err(fail(ErrorCode::RequestConflict, "agent.request"));
        }
        if let Some(reference) = &saved.snapshot.system_inputs {
            let record = self
                .inner
                .bindings
                .state
                .read_record(&self.inner.bindings.scope, &reference.snapshot_ref)
                .await?;
            let values =
                RunSystemInputs::from_value(record.value(), reference, &saved.snapshot.scope)?;
            // start omission means empty input. Only resume may reuse saved values
            // through an omitted map, and this path handles start replay exclusively.
            let empty = SystemInputs::default();
            values.validate_resume(Some(context.data.system_inputs.as_ref().unwrap_or(&empty)))?;
        } else if context
            .data
            .system_inputs
            .as_ref()
            .is_some_and(|values| !values.values().is_empty())
        {
            return Err(fail(ErrorCode::SystemInputsMismatch, "agent.system_inputs"));
        }
        Ok(())
    }

    async fn prepare(
        &self,
        request: RunRequest,
        context: &ExecutionContext,
    ) -> Result<(AdmissionInput, PromptSnapshot), ContractError> {
        let bindings = &self.inner.bindings;
        let routing = bindings.router.snapshot().clone();
        if routing.scope() != &bindings.scope {
            return Err(fail(ErrorCode::AccessDenied, "agent.router_scope"));
        }
        let profile = ProfileValidator::new(bindings.profile_resolver.as_ref())
            .validate(&self.inner.profile, &bindings.scope)
            .await?;
        let assembly = if let Some(runtime) = &bindings.components {
            let resolve_context = ComponentResolveContext {
                scope: bindings.scope.clone(),
                session_id: request.session_id.clone(),
                principal_ref: context.data.principal_ref.clone(),
                capability_grant_ref: context.data.capability_grant_ref.clone(),
                system_inputs: bindings.system_inputs.clone(),
                cancellation: context.cancellation.child_token(),
                deadline: tokio::time::Instant::now()
                    + Duration::from_millis(bindings.settings.start_timeout_ms),
            };
            let resolved = runtime.resolve(&profile, &resolve_context).await?;
            resolve_context.cancellation.cancel();
            if resolved.scope() != &bindings.scope
                || resolved.session_id() != &request.session_id
                || resolved.profile_resolution_digest() != profile.resolution_digest()
            {
                return Err(fail(ErrorCode::InvalidSnapshot, "agent.assembly"));
            }
            Some(resolved)
        } else {
            None
        };
        let tool_bindings = if let Some(assembly) = &assembly {
            ToolRegistry::metadata(bindings.scope.clone(), assembly.tools().to_vec())?
                .prompt_bindings(profile.profile())?
        } else {
            bindings
                .tools
                .as_ref()
                .map(|tools| tools.prompt_bindings(profile.profile()))
                .transpose()?
                .unwrap_or_default()
        };
        let session = match bindings
            .state
            .load_session(&bindings.scope, &request.session_id)
            .await
        {
            Ok(session) => Some(session),
            Err(error) if error.code == ErrorCode::StateNotFound => None,
            Err(error) => return Err(error),
        };
        let (prompt, prompt_record, sequence) = if let Some(session) = session {
            let record = bindings
                .state
                .read_record(&bindings.scope, &session.prompt_snapshot)
                .await?;
            let prompt = PromptSnapshot::restore(
                &serde_json::to_string(record.value())
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.prompt"))?,
                &record.reference().digest,
                &profile,
                &bindings.scope,
            )?;
            (
                prompt,
                record,
                session.transcript_revision.checked_add(1).ok_or_else(|| {
                    fail(ErrorCode::InvalidSnapshot, "session.transcript_revision")
                })?,
            )
        } else {
            let prompt = PromptSnapshot::create(
                &profile,
                bindings.host_instructions.clone(),
                None,
                tool_bindings.clone(),
                vec![],
            )?;
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(&prompt)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.prompt"))?,
            );
            (prompt, record, 1)
        };
        if prompt.tools().len() != tool_bindings.len()
            || prompt
                .tools()
                .iter()
                .zip(&tool_bindings)
                .any(|(pinned, binding)| {
                    pinned.selection != binding.selection
                        || pinned.compiled_digest != *binding.compiled.digest()
                        || pinned.descriptor_digest != *binding.compiled.descriptor_digest()
                        || pinned.model_tool != binding.compiled.to_model_tool()
                })
        {
            return Err(fail(ErrorCode::ContextMismatch, "agent.pinned_tools"));
        }
        let inputs = RunSystemInputs::capture(
            bindings.scope.clone(),
            context.data.system_inputs.clone(),
            &bindings.system_inputs,
        )?;
        let inputs_record = inputs.to_record(bindings.ids.next_id()?, 1);
        let inputs_ref = inputs.snapshot_ref(inputs_record.reference())?;
        let request_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(&request)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.request"))?,
        );
        let routing_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(&routing)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.routing"))?,
        );
        let run_id = bindings.ids.next_id()?;
        let hook_plan = if let Some(assembly) = &assembly {
            Some(
                HookRegistry::metadata(bindings.scope.clone(), assembly.hooks().to_vec())?
                    .plan(profile.profile())?,
            )
        } else {
            bindings
                .hooks
                .as_ref()
                .map(|hooks| hooks.plan(profile.profile()))
                .transpose()?
        };
        let hook_record = hook_plan
            .as_ref()
            .map(|plan| {
                Ok::<_, ContractError>(ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(plan)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.hooks"))?,
                ))
            })
            .transpose()?;
        let assembly_record = assembly
            .as_ref()
            .map(|assembly| {
                Ok::<_, ContractError>(ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(assembly)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.assembly"))?,
                ))
            })
            .transpose()?;
        let now = bindings.clock.now()?.utc_ms;
        let snapshot = RunSnapshot {
            schema_version: RunSnapshotSchemaVersion::V1,
            run_id: run_id.clone(),
            request_digest: admission_digest(&request, &profile, Some(&inputs_ref)),
            request: request.clone(),
            scope: bindings.scope.clone(),
            limits: profile.profile().limits.clone(),
            timing: RunTiming::new(now, profile.profile().limits.max_elapsed_ms.get())?,
            profile,
            status: RunStatus::Running,
            phase: RunPhase::Admission,
            model_step_id: None,
            usage: BudgetUsage::default(),
            reservations: vec![],
            model_ledger: vec![],
            tool_ledger: vec![],
            system_inputs: Some(inputs_ref),
            wait: None,
            outcome: None,
            assembly_ref: assembly_record
                .as_ref()
                .map(|record| record.reference().clone()),
            routing_snapshot_ref: Some(routing_record.reference().clone()),
            context_batches: vec![],
            source_states: vec![],
            revision: 0,
            resume_receipts: vec![],
            hook_plan_ref: hook_record
                .as_ref()
                .map(|record| record.reference().clone()),
            hook_applications: vec![],
            last_event_seq: 1,
        };
        let message = Message {
            message_id: bindings.ids.next_id()?,
            run_id: run_id.clone(),
            sequence: sequence
                .try_into()
                .map_err(|_| fail(ErrorCode::InvalidSnapshot, "message.sequence"))?,
            role: MessageRole::User,
            content: request
                .input
                .into_iter()
                .map(|content| ContentBlock::Content { content })
                .collect(),
            origin: MessageOrigin::User,
            visibility: Visibility::UserAndModel,
        };
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: bindings.ids.next_id()?,
            scope: bindings.scope.clone(),
            run_id,
            session_id: snapshot.request.session_id.clone(),
            seq: NonZeroU64::new(1).expect("initial sequence"),
            timestamp_ms: now,
            payload: RunEventPayload::RunStarted {
                request_ref: request_record.reference().clone(),
                profile_digest: snapshot.profile.profile_digest().clone(),
            },
        };
        Ok((
            AdmissionInput {
                snapshot,
                prompt_snapshot: prompt_record.reference().clone(),
                require_durable: bindings.settings.require_durable,
                messages: vec![message],
                events: vec![event],
                records: [
                    vec![request_record, prompt_record, inputs_record, routing_record],
                    hook_record.into_iter().collect(),
                    assembly_record.into_iter().collect(),
                ]
                .concat(),
            },
            prompt,
        ))
    }
}
```

## `crates/wickle/src/agent/components.rs`

```rust
use super::*;
use std::panic::AssertUnwindSafe;

/// Owned per-segment registries. The facade's shared bindings never change.
pub(super) struct SegmentBindings {
    pub context: ExecutionContext,
    pub tools: Arc<ToolRegistry>,
    pub hooks: Option<Arc<HookRuntime>>,
    pub owned: Option<BoundCapabilities>,
}
impl SegmentBindings {
    pub fn binding_set_id(&self) -> Option<&Id> {
        self.owned.as_ref().map(BoundCapabilities::binding_set_id)
    }
}

impl Agent {
    pub(super) fn direct_segment(
        &self,
        context: ExecutionContext,
    ) -> Result<SegmentBindings, ContractError> {
        Ok(SegmentBindings {
            context,
            tools: self
                .inner
                .bindings
                .tools
                .clone()
                .unwrap_or(Arc::new(ToolRegistry::new(
                    self.inner.bindings.scope.clone(),
                    vec![],
                )?)),
            hooks: self.inner.bindings.hooks.clone(),
            owned: None,
        })
    }
    pub(super) async fn saved_assembly(
        &self,
        saved: &StoredRun,
    ) -> Result<Option<ResolvedAssembly>, ContractError> {
        let Some(reference) = &saved.snapshot.assembly_ref else {
            if self.inner.bindings.components.is_some() {
                return Err(fail(ErrorCode::InvalidSnapshot, "components.assembly"));
            }
            return Ok(None);
        };
        let bindings = &self.inner.bindings;
        let inputs = if let Some(reference) = &saved.snapshot.system_inputs {
            let record = bindings
                .state
                .read_record(&saved.snapshot.scope, &reference.snapshot_ref)
                .await?;
            let inputs =
                RunSystemInputs::from_value(record.value(), reference, &saved.snapshot.scope)?;
            SystemInputRegistry::new(inputs.definitions().values().cloned().collect())?
        } else {
            bindings.system_inputs.clone()
        };
        let record = bindings
            .state
            .read_record(&saved.snapshot.scope, reference)
            .await?;
        if record.reference() != reference {
            return Err(fail(ErrorCode::InvalidSnapshot, "components.assembly"));
        }
        let assembly = ResolvedAssembly::restore(
            &serde_json::to_string(record.value())
                .map_err(|_| fail(ErrorCode::InvalidJson, "components.assembly"))?,
            &saved.snapshot.profile,
            &inputs,
            &reference.digest,
        )?;
        if assembly.session_id() != &saved.snapshot.request.session_id {
            return Err(fail(ErrorCode::InvalidSnapshot, "components.session"));
        }
        Ok(Some(assembly))
    }
    pub(super) async fn metadata_segment(
        &self,
        saved: &StoredRun,
        context: ExecutionContext,
    ) -> Result<SegmentBindings, ContractError> {
        match caller_read(
            &context,
            Some(Duration::from_millis(
                self.inner.bindings.settings.start_timeout_ms,
            )),
            self.saved_assembly(saved),
        )
        .await?
        {
            Some(assembly) => Ok(SegmentBindings {
                context,
                tools: Arc::new(ToolRegistry::metadata(
                    saved.snapshot.scope.clone(),
                    assembly.tools().to_vec(),
                )?),
                hooks: None,
                owned: None,
            }),
            None => self.direct_segment(context),
        }
    }
    pub(super) async fn bind_segment(
        &self,
        saved: &StoredRun,
        context: ExecutionContext,
        lease: Option<&RunLease>,
        purpose: ComponentBindPurpose,
        budget: Option<&RunBudget>,
        local: &LocalRun,
    ) -> Result<SegmentBindings, ContractError> {
        let Some(runtime) = &self.inner.bindings.components else {
            return self.direct_segment(context);
        };
        let assembly = caller_read(
            &context,
            Some(Duration::from_millis(
                self.inner.bindings.settings.start_timeout_ms,
            )),
            self.saved_assembly(saved),
        )
        .await?
        .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "components.assembly"))?;
        let bindings = &self.inner.bindings;
        if let Some(budget) = budget {
            budget.check_boundary().await?;
        }
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(bindings.settings.start_timeout_ms);
        let deadline = if let Some(budget) = budget {
            deadline.min(budget.call_deadline()?)
        } else {
            deadline
        };
        let bind_context = ComponentBindContext {
            scope: saved.snapshot.scope.clone(),
            run_id: saved.snapshot.run_id.clone(),
            session_id: saved.snapshot.request.session_id.clone(),
            binding_set_id: bindings.ids.next_id()?,
            principal_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
            lease: lease.cloned(),
            purpose,
            cancellation: context.cancellation.child_token(),
            deadline,
        };
        // The runtime retains staging ownership and rollback when this bounded
        // caller disconnects or drops its bind future.
        let _cancel = bind_context.cancellation.clone().drop_guard();
        let owned = caller_read(
            &context,
            Some(deadline.saturating_duration_since(tokio::time::Instant::now())),
            async {
                AssertUnwindSafe(runtime.bind(&assembly, &bind_context))
                    .catch_unwind()
                    .await
                    .map_err(|_| fail(ErrorCode::InvalidContract, "components.bind"))?
                    .map_err(|error| fail(error.code, "components.bind"))
            },
        )
        .await?;
        bind_context.cancellation.cancel();
        if owned.scope() != &saved.snapshot.scope
            || owned.run_id() != &saved.snapshot.run_id
            || owned.binding_set_id() != &bind_context.binding_set_id
        {
            self.release_bound(&owned, local).await;
            return Err(fail(ErrorCode::AccessDenied, "components.bound_identity"));
        }
        let contracts = (|| {
            let tools = owned
                .tools()
                .prompt_bindings(saved.snapshot.profile.profile())?;
            if tools.len() != assembly.tools().len()
                || tools
                    .iter()
                    .zip(assembly.tools())
                    .any(|(actual, expected)| {
                        actual.selection != expected.selection
                            || actual.compiled.digest() != expected.compiled.digest()
                            || actual.compiled.to_model_tool() != expected.compiled.to_model_tool()
                    })
            {
                return Err(fail(ErrorCode::ContextMismatch, "components.bound_tools"));
            }
            let expected =
                HookRegistry::metadata(saved.snapshot.scope.clone(), assembly.hooks().to_vec())?
                    .plan(saved.snapshot.profile.profile())?;
            if owned.hooks().plan(saved.snapshot.profile.profile())? != expected {
                return Err(fail(ErrorCode::ContextMismatch, "components.bound_hooks"));
            }
            Ok::<_, ContractError>(())
        })();
        if let Err(error) = contracts {
            self.release_bound(&owned, local).await;
            return Err(error);
        }
        let hook_runtime = HookRuntime::new(
            bindings.state.clone(),
            bindings.policy.clone(),
            bindings.clock.clone(),
            bindings.ids.clone(),
            owned.hooks().clone(),
        )
        .with_binding_set_id(owned.binding_set_id().clone());
        Ok(SegmentBindings {
            context,
            tools: owned.tools().clone(),
            hooks: Some(Arc::new(hook_runtime)),
            owned: Some(owned),
        })
    }
    async fn release_capabilities(
        &self,
        owned: &BoundCapabilities,
    ) -> Result<ComponentReleaseReport, ContractError> {
        let context = ComponentReleaseContext {
            scope: owned.scope().clone(),
            run_id: owned.run_id().clone(),
            binding_set_id: owned.binding_set_id().clone(),
            cancellation: CancellationToken::new(),
            deadline: tokio::time::Instant::now() + Duration::from_secs(30),
        };
        let _cancel = context.cancellation.clone().drop_guard();
        tokio::time::timeout_at(
            context.deadline,
            AssertUnwindSafe(owned.release(&context)).catch_unwind(),
        )
        .await
        .map_err(|_| fail(ErrorCode::DeadlineExceeded, "components.release"))?
        .map_err(|_| fail(ErrorCode::InvalidContract, "components.release"))?
        .map_err(|error| fail(error.code, "components.release"))
    }
    pub(super) async fn release_segment(&self, segment: &SegmentBindings, local: &LocalRun) {
        let Some(owned) = &segment.owned else {
            return;
        };
        self.release_bound(owned, local).await;
    }
    async fn release_bound(&self, owned: &BoundCapabilities, local: &LocalRun) {
        match self.release_capabilities(owned).await {
            Ok(report) => {
                if let Ok(mut slot) = local.release_report.lock() {
                    match slot.as_mut() {
                        Some(previous) => previous.failures.extend(report.failures),
                        None => *slot = Some(report),
                    }
                }
            }
            Err(error) => {
                if let Ok(mut slot) = local.release_error.lock() {
                    *slot = Some(error);
                }
            }
        }
    }
    pub(super) fn keep_local(&self, local: &LocalRun) -> bool {
        local
            .observer_error
            .lock()
            .is_ok_and(|error| error.is_some())
            || local
                .release_error
                .lock()
                .is_ok_and(|error| error.is_some())
            || local.release_report.lock().is_ok_and(|report| {
                report
                    .as_ref()
                    .is_some_and(|report| !report.failures.is_empty())
            })
    }
    pub(super) async fn observe_pending(
        &self,
        run_id: &Id,
        segment: &SegmentBindings,
        local: &LocalRun,
    ) {
        let pending = local
            .pending_observations
            .lock()
            .map(|mut pending| std::mem::take(&mut *pending))
            .unwrap_or_default();
        for (target, input) in pending {
            self.remember_observer_error(
                local,
                self.after_tool(run_id, target, input, segment).await,
            );
        }
    }
    pub(super) async fn cleanup_observers(
        &self,
        saved: &StoredRun,
        context: &ExecutionContext,
        local: &LocalRun,
        observations: Vec<(HookTarget, HookInput)>,
    ) {
        let mut data = context.data.clone();
        data.system_inputs = None;
        let context = ExecutionContext::new(data, CancellationToken::new());
        if let Ok(mut pending) = local.pending_observations.lock() {
            pending.extend(observations);
        }
        let opened = self
            .bind_segment(
                saved,
                context,
                None,
                ComponentBindPurpose::ObserversOnly,
                None,
                local,
            )
            .await;
        let segment = match opened {
            Ok(segment) => segment,
            Err(error) => {
                if let Ok(mut slot) = local.release_error.lock() {
                    *slot = Some(fail(error.code, "components.observer_bind"));
                }
                return;
            }
        };
        let observed = AssertUnwindSafe(async {
            self.observe_pending(&saved.snapshot.run_id, &segment, local)
                .await;
            self.after_run(saved, &segment, local).await;
        })
        .catch_unwind()
        .await;
        if observed.is_err() {
            self.remember_observer_error(
                local,
                Some(fail(ErrorCode::InvalidContract, "components.observer")),
            );
        }
        self.release_segment(&segment, local).await;
    }
}
```

## `crates/wickle/src/agent/driver.rs`

```rust
use super::*;
use std::panic::AssertUnwindSafe;

impl Agent {
    pub(super) async fn drive(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        context: ExecutionContext,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let bindings = &self.inner.bindings;
        let now = bindings.clock.now()?.utc_ms;
        let lease = bindings
            .state
            .acquire_lease(
                &bindings.scope,
                run_id,
                &bindings.ids.next_id()?,
                now,
                bindings.settings.lease_ttl_ms,
            )
            .await?;
        self.drive_leased(run_id, prompt, context, local, lease, false)
            .await
    }

    pub(super) async fn drive_leased(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        context: ExecutionContext,
        local: &Arc<LocalRun>,
        lease: RunLease,
        expired: bool,
    ) -> Result<(), ContractError> {
        let bindings = &self.inner.bindings;
        let budget = match RunBudget::attach(
            bindings.state.clone(),
            bindings.clock.clone(),
            bindings.ids.clone(),
            bindings.scope.clone(),
            run_id.clone(),
            lease.clone(),
            local.cancel.clone(),
        )
        .await
        {
            Ok(budget) => Arc::new(budget),
            Err(error) => {
                self.release_owned(run_id, &lease).await;
                return Err(error);
            }
        };
        let stop = CancellationToken::new();
        let heartbeat_agent = self.clone();
        let heartbeat_budget = budget.clone();
        let heartbeat_lease = lease.clone();
        let heartbeat_id = run_id.clone();
        let heartbeat_stop = stop.clone();
        let heartbeat_local = local.clone();
        let heartbeat = tokio::spawn(async move {
            let result = AssertUnwindSafe(heartbeat_agent.heartbeat(
                &heartbeat_id,
                heartbeat_lease,
                &heartbeat_budget,
                &heartbeat_stop,
            ))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(fail(ErrorCode::LeaseLost, "agent.heartbeat")));
            if let Err(error) = &result {
                if let Ok(mut slot) = heartbeat_local.error.lock() {
                    *slot = Some(error.clone());
                }
                heartbeat_local.cancel.cancel();
            }
            result
        });
        let mut segment = None;
        let result = AssertUnwindSafe(async {
            let saved = bindings.state.load(&bindings.scope, run_id).await?;
            let metadata = self.metadata_segment(&saved, context.clone()).await?;
            if expired {
                segment = Some(metadata);
                self.finish(
                    run_id,
                    PreparedOutcome {
                        result: OutcomeResult::Exhausted {
                            budget: BudgetKind::Elapsed,
                        },
                        output: vec![],
                        continuation: vec![],
                        unresolved_effects: vec![],
                    },
                    &budget,
                    segment.as_ref().expect("metadata segment"),
                    local,
                )
                .await
            } else {
                match self
                    .bind_segment(
                        &saved,
                        context.clone(),
                        Some(&lease),
                        ComponentBindPurpose::Execution,
                        Some(&budget),
                        local,
                    )
                    .await
                {
                    Ok(bound) => {
                        segment = Some(bound);
                        self.observe_pending(
                            run_id,
                            segment.as_ref().expect("bound segment"),
                            local,
                        )
                        .await;
                        self.run_segment(
                            run_id,
                            prompt,
                            segment.as_ref().expect("bound segment"),
                            &budget,
                            &lease,
                            local,
                        )
                        .await
                    }
                    Err(error) => {
                        segment = Some(metadata);
                        if matches!(
                            error.code,
                            ErrorCode::LeaseLost
                                | ErrorCode::PersistenceUnavailable
                                | ErrorCode::RevisionConflict
                        ) {
                            return Err(error);
                        }
                        self.finish(
                            run_id,
                            PreparedOutcome {
                                result: match error.code {
                                    ErrorCode::Cancelled => OutcomeResult::Cancelled {
                                        reason: local
                                            .reason
                                            .lock()
                                            .map_err(|_| {
                                                fail(ErrorCode::InvalidContract, "agent.cancel")
                                            })?
                                            .as_ref()
                                            .map(ToString::to_string)
                                            .unwrap_or_else(|| "cancelled".into()),
                                    },
                                    ErrorCode::DeadlineExceeded | ErrorCode::BudgetExceeded => {
                                        OutcomeResult::Exhausted {
                                            budget: BudgetKind::Elapsed,
                                        }
                                    }
                                    _ => failed(&enum_name(&error.code)),
                                },
                                output: vec![],
                                continuation: vec![],
                                unresolved_effects: vec![],
                            },
                            &budget,
                            segment.as_ref().expect("metadata segment"),
                            local,
                        )
                        .await
                    }
                }
            }
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(fail(ErrorCode::InvalidContract, "agent.driver")));
        stop.cancel();
        let heartbeat_result = heartbeat
            .await
            .map_err(|_| fail(ErrorCode::LeaseLost, "agent.heartbeat"))?;
        let latest = bindings.state.load(&bindings.scope, run_id).await;
        if let Some(segment) = segment.as_ref() {
            if let Ok(saved) = &latest {
                if saved.snapshot.status.is_terminal() {
                    if bindings.components.is_some() && segment.owned.is_none() {
                        if expired {
                            self.cleanup_observers(saved, &context, local, vec![]).await;
                        } else if let Ok(mut slot) = local.release_error.lock() {
                            if slot.is_none() {
                                *slot = Some(fail(
                                    ErrorCode::ComponentUnavailable,
                                    "components.observers_not_bound",
                                ));
                            }
                        }
                    } else {
                        self.after_run(saved, segment, local).await;
                    }
                }
            }
            self.release_segment(segment, local).await;
        }
        if let Ok((_, now)) = budget.settlement_time(0) {
            let _ = bindings
                .state
                .release_lease(&bindings.scope, run_id, &lease, now)
                .await;
        }
        if latest.as_ref().is_ok_and(|saved| {
            saved.snapshot.status.is_terminal() || saved.snapshot.status == RunStatus::Waiting
        }) {
            return Ok(());
        }
        result.and(heartbeat_result)
    }

    async fn heartbeat(
        &self,
        run_id: &Id,
        mut lease: RunLease,
        budget: &RunBudget,
        stop: &CancellationToken,
    ) -> Result<(), ContractError> {
        let bindings = &self.inner.bindings;
        loop {
            let reading = bindings.clock.now()?;
            let next = reading
                .monotonic_ms
                .checked_add(bindings.settings.heartbeat_interval_ms)
                .ok_or_else(|| fail(ErrorCode::ClockUnavailable, "agent.heartbeat"))?;
            tokio::select! { biased;
                _ = stop.cancelled() => return Ok(()),
                result = bindings.clock.sleep_until(next) => result?,
            }
            let (_, now) = budget.settlement_time(0)?;
            let renewal = bindings.state.renew_lease(
                &bindings.scope,
                run_id,
                &lease,
                now,
                bindings.settings.lease_ttl_ms,
            );
            let remaining = lease
                .expires_at_ms
                .checked_sub(now)
                .and_then(|value| u64::try_from(value).ok())
                .filter(|value| *value > 0)
                .ok_or_else(|| fail(ErrorCode::LeaseLost, "agent.heartbeat"))?;
            let result = tokio::select! { biased;
                _ = stop.cancelled() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_millis(remaining)) => Err(fail(ErrorCode::LeaseLost, "agent.heartbeat")),
                result = renewal => result,
            };
            match result {
                Ok(current) => lease = current,
                Err(error) => return Err(error),
            }
        }
    }

    async fn run_segment(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        segment: &SegmentBindings,
        budget: &RunBudget,
        lease: &RunLease,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let context = &segment.context;
        let mut waiting = None;
        let saved = self
            .inner
            .bindings
            .state
            .load(budget.scope(), run_id)
            .await?;
        let mut pending_round = saved.snapshot.tool_ledger.iter().find(|entry| !matches!(&entry.state, ToolCallState::Settled { result } if result.status != ToolResultStatus::Unknown && result.effect != ToolEffect::Unknown)).map(|entry| entry.call.model_request_id.clone());
        let attempt = loop {
            if let Some(request_id) = pending_round.take() {
                let round = self.tool_round(budget, segment).await?;
                let result = round.execute(&request_id, context, budget).await;
                self.remember_observer_error(local, round.observer_error());
                match result {
                    Ok(ToolRoundOutcome::Completed) => {}
                    Ok(outcome) => {
                        waiting = Some(self.tool_wait(outcome, budget).await?);
                        break None;
                    }
                    Err(error) => break Some(Err(error)),
                }
            }
            match self
                .generate(run_id, prompt.clone(), segment, budget, lease)
                .await
            {
                Ok(Guarded::Completed(ModelExchangeOutcome::Completed { response }))
                    if response.finish == ModelFinish::ToolCalls =>
                {
                    if let Err(error) = self.plan_tools(&response, &prompt, budget).await {
                        break Some(Err(error));
                    }
                    let round = self.tool_round(budget, segment).await?;
                    let result = round.execute(&response.request_id, context, budget).await;
                    self.remember_observer_error(local, round.observer_error());
                    match result {
                        Ok(ToolRoundOutcome::Completed) => continue,
                        Ok(outcome) => {
                            waiting = Some(self.tool_wait(outcome, budget).await?);
                            break None;
                        }
                        Err(error) => break Some(Err(error)),
                    }
                }
                result => break Some(result),
            }
        };
        if let Some(error) = local
            .error
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .clone()
        {
            return Err(error);
        }
        if let Some((wait, unresolved_effects)) = waiting {
            return self
                .finish(
                    run_id,
                    PreparedOutcome {
                        result: OutcomeResult::Waiting { wait },
                        output: vec![],
                        continuation: vec![],
                        unresolved_effects,
                    },
                    budget,
                    segment,
                    local,
                )
                .await;
        }
        let attempt = attempt.expect("non-waiting loop result");
        let mut continuation = vec![];
        let (result, output) = match attempt {
            Ok(Guarded::Completed(ModelExchangeOutcome::Completed { response }))
                if response.finish == ModelFinish::Stop && response.tool_calls.is_empty() =>
            {
                continuation = response.continuation;
                (
                    OutcomeResult::Succeeded {
                        completion_basis: CompletionBasis::TurnEnded,
                    },
                    vec![InputContent::Text {
                        text: response.text,
                    }],
                )
            }
            Ok(Guarded::Completed(ModelExchangeOutcome::Completed { response })) => (
                failed(if response.finish == ModelFinish::Refusal {
                    "model_refusal"
                } else {
                    "tool_execution_unsupported"
                }),
                vec![],
            ),
            Ok(Guarded::Completed(ModelExchangeOutcome::Failed { failure })) => (
                failed(&format!("model_{}", enum_name(&failure.kind))),
                if failure.partial_text().is_empty() {
                    vec![]
                } else {
                    vec![InputContent::Text {
                        text: failure.partial_text().to_owned(),
                    }]
                },
            ),
            Ok(Guarded::ApprovalRequired(_)) => (failed("approval_runtime_unsupported"), vec![]),
            Err(error)
                if matches!(
                    error.code,
                    ErrorCode::LeaseLost
                        | ErrorCode::RevisionConflict
                        | ErrorCode::PersistenceUnavailable
                        | ErrorCode::StateNotFound
                        | ErrorCode::ClockUnavailable
                        | ErrorCode::ClockRegression
                        | ErrorCode::InvalidTransition
                        | ErrorCode::InvalidSnapshot
                        | ErrorCode::InvalidEvent
                        | ErrorCode::RecordConflict
                ) =>
            {
                return Err(error);
            }
            Err(error) if error.code == ErrorCode::Cancelled => (
                OutcomeResult::Cancelled {
                    reason: local
                        .reason
                        .lock()
                        .map_err(|_| fail(ErrorCode::InvalidContract, "agent.cancel"))?
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "cancelled".into()),
                },
                vec![],
            ),
            Err(error) if error.code == ErrorCode::DeadlineExceeded => (
                OutcomeResult::Exhausted {
                    budget: BudgetKind::Elapsed,
                },
                vec![],
            ),
            Err(error) if error.code == ErrorCode::BudgetExceeded => {
                let kind = match error.path.as_str() {
                    "budget.model_calls" => BudgetKind::ModelCalls,
                    "budget.tool_attempts" => BudgetKind::ToolAttempts,
                    "budget.repair_attempts" => BudgetKind::RepairAttempts,
                    "budget.recovery_attempts" => BudgetKind::RecoveryAttempts,
                    _ => BudgetKind::Elapsed,
                };
                (OutcomeResult::Exhausted { budget: kind }, vec![])
            }
            Err(error) => (failed(&enum_name(&error.code)), vec![]),
        };
        self.finish(
            run_id,
            PreparedOutcome {
                result,
                output,
                continuation,
                unresolved_effects: vec![],
            },
            budget,
            segment,
            local,
        )
        .await
    }

    async fn generate(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        segment: &SegmentBindings,
        budget: &RunBudget,
        lease: &RunLease,
    ) -> Result<Guarded<ModelExchangeOutcome>, ContractError> {
        let context = &segment.context;
        let bindings = &self.inner.bindings;
        budget.check_boundary().await?;
        let run_context = self.before_run(budget, segment).await?;
        let mut snapshot = bindings.state.load(&bindings.scope, run_id).await?.snapshot;
        let expected_revision = snapshot.revision;
        let step = bindings.ids.next_id()?;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.prepare"))?;
        snapshot.phase = RunPhase::Prepare;
        snapshot.model_step_id = Some(step.clone());
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        let saved = bindings
            .state
            .commit(
                &bindings.scope,
                run_id,
                CommitInput {
                    expected_revision,
                    lease: lease.clone(),
                    now_ms: now,
                    snapshot,
                    messages: vec![],
                    events: vec![],
                    records: vec![],
                },
            )
            .await?;
        let router = bindings.router.snapshot();
        let rule = router
            .policy()
            .rules
            .iter()
            .find(|rule| {
                rule.model_binding == saved.snapshot.profile.profile().model_binding
                    && rule.purpose == ModelPurpose::Agent
            })
            .ok_or_else(|| fail(ErrorCode::ModelRouteDenied, "agent.routing"))?;
        let context_items = self
            .before_model(
                &step,
                saved.snapshot.request.input.clone(),
                run_context,
                segment,
                budget,
            )
            .await?;
        let input = RoutedModelInput {
            model_step_id: step,
            routing: RouteRequest {
                model_binding: saved.snapshot.profile.profile().model_binding.clone(),
                purpose: ModelPurpose::Agent,
                required_capabilities: if prompt.tools().is_empty() {
                    std::collections::BTreeSet::from([Id::new("text")?])
                } else {
                    std::collections::BTreeSet::from([Id::new("text")?, Id::new("tool_calling")?])
                },
                input_tokens: 0,
                max_output_tokens: bindings.settings.max_output_tokens,
                options: saved.snapshot.request.model_options.clone(),
                scope: bindings.scope.clone(),
                allowed_bindings: std::iter::once(&rule.primary)
                    .chain(&rule.fallbacks)
                    .map(|binding| binding.id.clone())
                    .collect(),
                version_policy: rule.version_policy,
                previous_route: None,
                previous_failure: None,
            },
        };
        let projector = Projector {
            saved,
            prompt,
            settings: bindings.settings.clone(),
            estimator: bindings.token_estimator.clone(),
            state: bindings.state.clone(),
            context_items,
        };
        bindings
            .model_exchange
            .generate_routed(
                bindings.router.as_ref(),
                &input,
                &projector,
                context,
                budget,
            )
            .await
    }

    async fn finish(
        &self,
        run_id: &Id,
        candidate: PreparedOutcome,
        budget: &RunBudget,
        segment: &SegmentBindings,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let PreparedOutcome {
            mut result,
            mut output,
            continuation,
            mut unresolved_effects,
        } = candidate;
        let bindings = &self.inner.bindings;
        let lease = budget.lease();
        let mut saved = bindings.state.load(&bindings.scope, run_id).await?;
        if saved.snapshot.status.is_terminal() {
            return Ok(());
        }
        if unresolved_effects.is_empty() && saved.snapshot.tool_ledger.iter().any(|entry| matches!(entry.state, ToolCallState::Unknown { .. }) || matches!(&entry.state, ToolCallState::Settled { result } if result.effect == ToolEffect::Unknown)) {
            if let Some(receipt) = saved.snapshot.resume_receipts.last() {
                let record = bindings.state.read_record(&bindings.scope, &receipt.previous_outcome_ref).await?;
                let previous: RunOutcome = serde_json::from_value(record.value().clone()).map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.previous_outcome"))?;
                unresolved_effects = previous.unresolved_effects;
            }
        }
        if output.is_empty() && !matches!(result, OutcomeResult::Succeeded { .. }) {
            output = self.saved_partial_output(&saved.snapshot).await?;
        }
        // Finalization remains possible after cancellation/deadline, but only
        // under the stored lease. A stop during these reads also closes untouched
        // plans; it never invents a result for an uncertain dispatched operation.
        let mut cleaned = false;
        let (elapsed, now) = loop {
            let (_, check_at) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
            let current_lease = bindings
                .state
                .check_lease(&bindings.scope, run_id, lease, check_at)
                .await?;
            let (elapsed, now) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
            if now >= current_lease.expires_at_ms {
                return Err(fail(ErrorCode::LeaseLost, "agent.finish"));
            }
            if matches!(
                result,
                OutcomeResult::Succeeded { .. } | OutcomeResult::Waiting { .. }
            ) {
                if local.cancel.is_cancelled() {
                    result = OutcomeResult::Cancelled {
                        reason: local
                            .reason
                            .lock()
                            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.cancel"))?
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "cancelled".into()),
                    };
                } else if elapsed >= saved.snapshot.limits.max_elapsed_ms.get() {
                    result = OutcomeResult::Exhausted {
                        budget: BudgetKind::Elapsed,
                    };
                }
            }
            if !matches!(
                result,
                OutcomeResult::Succeeded { .. } | OutcomeResult::Waiting { .. }
            ) && saved.snapshot.tool_ledger.iter().any(|entry| {
                matches!(
                    entry.state,
                    ToolCallState::Planned {}
                        | ToolCallState::ApprovalPending { .. }
                        | ToolCallState::InputPending { .. }
                )
            }) {
                if cleaned {
                    return Err(fail(ErrorCode::InvalidTransition, "agent.pending_tools"));
                }
                self.settle_unstarted_tools(
                    &saved.snapshot,
                    segment,
                    budget,
                    matches!(result, OutcomeResult::Cancelled { .. }),
                    local,
                )
                .await?;
                saved = bindings.state.load(&bindings.scope, run_id).await?;
                cleaned = true;
                continue;
            }
            break (elapsed, now);
        };
        let mut snapshot = saved.snapshot;
        let expected_revision = snapshot.revision;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.finish"))?;
        snapshot.last_event_seq = snapshot
            .last_event_seq
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.event"))?;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        snapshot.status = result.status();
        snapshot.phase = if snapshot.status == RunStatus::Waiting {
            RunPhase::Waiting
        } else {
            RunPhase::Finish
        };
        snapshot.wait = if let OutcomeResult::Waiting { wait } = &result {
            Some(wait.clone())
        } else {
            None
        };
        if let OutcomeResult::Failed { failure } = &mut result {
            failure.diagnostic_ref = snapshot
                .model_ledger
                .last()
                .and_then(|entry| entry.response_ref.clone());
        }
        let outcome = RunOutcome {
            result,
            output: output.clone(),
            artifacts: vec![],
            usage: snapshot.usage.clone(),
            checkpoint_revision: snapshot.revision,
            verification: None,
            unresolved_effects,
        };
        let record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(&outcome)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.outcome"))?,
        );
        let wait_record = snapshot
            .wait
            .as_ref()
            .map(|wait| {
                Ok::<_, ContractError>(ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(wait)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.wait"))?,
                ))
            })
            .transpose()?;
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: bindings.ids.next_id()?,
            scope: bindings.scope.clone(),
            run_id: run_id.clone(),
            session_id: snapshot.request.session_id.clone(),
            seq: snapshot
                .last_event_seq
                .try_into()
                .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.event"))?,
            timestamp_ms: now,
            payload: if let Some(wait_record) = &wait_record {
                RunEventPayload::RunWaiting {
                    wait_ref: wait_record.reference().clone(),
                }
            } else {
                RunEventPayload::RunFinished {
                    outcome_ref: record.reference().clone(),
                }
            },
        };
        let mut records = vec![record];
        records.extend(wait_record);
        let mut content: Vec<_> = output
            .into_iter()
            .map(|content| ContentBlock::Content { content })
            .collect();
        if snapshot.status == RunStatus::Succeeded {
            for continuation in continuation {
                let route = &snapshot
                    .model_ledger
                    .last()
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.continuation"))?
                    .route;
                if continuation.route_digest() != &route.digest() {
                    return Err(fail(
                        ErrorCode::ModelContextIncompatible,
                        "agent.continuation",
                    ));
                }
                let record = ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(&continuation)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.continuation"))?,
                );
                content.push(ContentBlock::ProviderOpaque {
                    provider: route.provider.clone(),
                    route_digest: route.digest(),
                    data_ref: record.reference().clone(),
                });
                records.push(record);
            }
        }
        let messages = if content.is_empty() || snapshot.status != RunStatus::Succeeded {
            vec![]
        } else {
            vec![Message {
                message_id: bindings.ids.next_id()?,
                run_id: run_id.clone(),
                sequence: saved
                    .session
                    .transcript_revision
                    .checked_add(1)
                    .and_then(NonZeroU64::new)
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "message.sequence"))?,
                role: MessageRole::Assistant,
                content,
                origin: MessageOrigin::Model,
                visibility: Visibility::UserAndModel,
            }]
        };
        snapshot.outcome = Some(outcome);
        bindings
            .state
            .commit(
                &bindings.scope,
                run_id,
                CommitInput {
                    expected_revision,
                    lease: lease.clone(),
                    now_ms: now,
                    snapshot,
                    messages,
                    events: vec![event],
                    records,
                },
            )
            .await?;
        local.notify.notify_waiters();
        Ok(())
    }

    async fn saved_partial_output(
        &self,
        snapshot: &RunSnapshot,
    ) -> Result<Vec<InputContent>, ContractError> {
        let Some(step) = &snapshot.model_step_id else {
            return Ok(vec![]);
        };
        let Some(invocation) = snapshot.model_ledger.iter().rev().find(|invocation| {
            &invocation.model_step_id == step
                && invocation.run_id == snapshot.run_id
                && invocation.response_ref.is_some()
        }) else {
            return Ok(vec![]);
        };
        let reference = invocation
            .response_ref
            .as_ref()
            .expect("filtered response reference");
        let record = self
            .inner
            .bindings
            .state
            .read_record(&snapshot.scope, reference)
            .await?;
        if record.reference() != reference {
            return Err(fail(ErrorCode::InvalidSnapshot, "agent.partial_response"));
        }
        let response: StoredModelResponse = serde_json::from_value(record.value().clone())
            .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.partial_response"))?;
        if response.request_id != invocation.attempt_id
            || response.route_digest != invocation.route.digest()
        {
            return Err(fail(ErrorCode::InvalidSnapshot, "agent.partial_response"));
        }
        let text = match response.outcome {
            ModelExchangeOutcome::Completed { response } => response.text,
            ModelExchangeOutcome::Failed { failure } => failure.partial_text().to_owned(),
        };
        Ok(if text.is_empty() {
            vec![]
        } else {
            vec![InputContent::Text { text }]
        })
    }
}

struct PreparedOutcome {
    result: OutcomeResult,
    output: Vec<InputContent>,
    continuation: Vec<OpaqueContinuation>,
    unresolved_effects: Vec<RecordRef>,
}

struct Projector {
    saved: StoredRun,
    prompt: PromptSnapshot,
    settings: AgentSettings,
    estimator: Arc<dyn ModelTokenEstimator>,
    state: Arc<dyn StateStore>,
    context_items: Vec<ContextItem>,
}
impl ModelRequestProjector for Projector {
    fn project<'a>(
        &'a self,
        selection: &'a RouteSelection,
        input: &'a RoutedModelInput,
        context: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ProjectedModelRequest> {
        Box::pin(async move {
            if context.cancellation.is_cancelled() {
                return Err(fail(ErrorCode::Cancelled, "agent.projection"));
            }
            let request_message = self
                .saved
                .messages
                .iter()
                .find(|message| {
                    message.run_id == self.saved.snapshot.run_id
                        && message.role == MessageRole::User
                })
                .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.request_message"))?;
            let mut opaque_records: Vec<ScopedOpaque> = vec![];
            for message in &self.saved.messages {
                if !matches!(
                    message.visibility,
                    Visibility::Model | Visibility::UserAndModel
                ) {
                    continue;
                }
                for content in &message.content {
                    if let ContentBlock::ProviderOpaque {
                        provider,
                        route_digest,
                        data_ref,
                    } = content
                    {
                        if provider != &selection.route.provider
                            || route_digest != &selection.route.digest()
                        {
                            return Err(fail(
                                ErrorCode::ModelContextIncompatible,
                                "agent.opaque_route",
                            ));
                        }
                        if opaque_records
                            .iter()
                            .any(|record| &record.reference == data_ref)
                        {
                            continue;
                        }
                        let record = self.state.read_record(&context.scope, data_ref).await?;
                        if record.reference() != data_ref {
                            return Err(fail(
                                ErrorCode::ModelContextIncompatible,
                                "agent.opaque_record",
                            ));
                        }
                        let continuation: OpaqueContinuation =
                            serde_json::from_value(record.value().clone()).map_err(|_| {
                                fail(ErrorCode::ModelContextIncompatible, "agent.opaque_record")
                            })?;
                        if continuation.route_digest() != route_digest
                            || canonical_digest(record.value()) != data_ref.digest
                        {
                            return Err(fail(
                                ErrorCode::ModelContextIncompatible,
                                "agent.opaque_record",
                            ));
                        }
                        opaque_records.push(ScopedOpaque {
                            scope: context.scope.clone(),
                            reference: data_ref.clone(),
                            provider: provider.clone(),
                            continuation,
                        });
                    }
                }
            }
            let projection = ContextAssembler::new().project(
                &self.prompt,
                ProjectionInput {
                    profile: &self.saved.snapshot.profile,
                    scope: &context.scope,
                    run_id: &self.saved.snapshot.run_id,
                    model_step_id: &input.model_step_id,
                    current_request: &self.saved.snapshot.request,
                    current_request_message_id: &request_message.message_id,
                    transcript: &self.saved.messages,
                    context_items: &self.context_items,
                    opaque_records: &opaque_records,
                    expected_prompt_digest: &self.saved.session.prompt_snapshot.digest,
                    request_id: input.model_step_id.clone(),
                    purpose: input.routing.purpose,
                    route: selection.route.clone(),
                    output: ModelOutput::Text {},
                    max_output_tokens: self.settings.max_output_tokens,
                    options: input.routing.options.clone(),
                    response_limits: self.settings.response_limits.clone(),
                    limits: self.settings.projection_limits,
                },
            )?;
            let input_tokens = self.estimator.estimate(&projection.request)?;
            Ok(ProjectedModelRequest {
                request: projection.request,
                input_tokens,
            })
        })
    }
}
fn enum_name(value: &impl serde::Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "invalid_contract".into())
}
fn failed(code: &str) -> OutcomeResult {
    OutcomeResult::Failed {
        failure: Failure {
            code: Id::new(code).expect("nonempty static classification"),
            diagnostic_ref: None,
        },
    }
}
```

## `crates/wickle/src/agent/hooks.rs`

```rust
use super::*;

impl Agent {
    pub(super) async fn before_run(
        &self,
        budget: &RunBudget,
        segment: &SegmentBindings,
    ) -> Result<Vec<ContextItem>, ContractError> {
        let Some(hooks) = &segment.hooks else {
            return Ok(vec![]);
        };
        let saved = self
            .inner
            .bindings
            .state
            .load(budget.scope(), budget.run_id())
            .await?;
        let transformed = hooks
            .transform(
                HookTarget::BeforeRun,
                HookInput::BeforeRun {
                    user_input: saved.snapshot.request.input.clone(),
                    context_items: vec![],
                },
                &segment.context,
                budget,
            )
            .await?;
        Ok(transformed.context_items)
    }
    pub(super) async fn before_model(
        &self,
        step: &Id,
        user_input: Vec<InputContent>,
        context_items: Vec<ContextItem>,
        segment: &SegmentBindings,
        budget: &RunBudget,
    ) -> Result<Vec<ContextItem>, ContractError> {
        let Some(hooks) = &segment.hooks else {
            return Ok(context_items);
        };
        let transformed = hooks
            .transform(
                HookTarget::BeforeModel {
                    model_step_id: step.clone(),
                },
                HookInput::BeforeModel {
                    user_input,
                    context_items,
                },
                &segment.context,
                budget,
            )
            .await?;
        Ok(transformed.context_items)
    }
    pub(super) fn remember_observer_error(&self, local: &LocalRun, error: Option<ContractError>) {
        if let Some(error) = error {
            if let Ok(mut slot) = local.observer_error.lock() {
                *slot = Some(fail(error.code, "hooks.observer_report"));
            }
        }
    }
    pub(super) async fn after_run(
        &self,
        saved: &StoredRun,
        segment: &SegmentBindings,
        local: &LocalRun,
    ) {
        let Some(hooks) = &segment.hooks else {
            return;
        };
        let Some(outcome) = &saved.snapshot.outcome else {
            return;
        };
        if !saved.snapshot.status.is_terminal() {
            return;
        }
        let mut data = segment.context.data.clone();
        data.system_inputs = None;
        let cleanup = ExecutionContext::new(data, CancellationToken::new());
        let observed = caller_read(&cleanup, Some(Duration::from_secs(30)), async {
            let page = self
                .inner
                .bindings
                .state
                .read_events(
                    &saved.snapshot.scope,
                    &saved.snapshot.run_id,
                    saved.snapshot.last_event_seq.saturating_sub(1),
                    1,
                )
                .await?;
            let Some(RunEvent {
                payload: RunEventPayload::RunFinished { outcome_ref },
                ..
            }) = page.events.last()
            else {
                return Err(fail(ErrorCode::InvalidSnapshot, "hooks.terminal_event"));
            };
            hooks
                .observe(
                    &saved.snapshot.run_id,
                    HookTarget::AfterRun {
                        outcome_ref: outcome_ref.clone(),
                        revision: saved.snapshot.revision,
                    },
                    HookInput::run_observed(outcome),
                    &cleanup,
                )
                .await
        })
        .await;
        self.remember_observer_error(local, observed.err());
    }
    pub(super) async fn after_tool(
        &self,
        run_id: &Id,
        target: HookTarget,
        input: HookInput,
        segment: &SegmentBindings,
    ) -> Option<ContractError> {
        let Some(hooks) = &segment.hooks else {
            return None;
        };
        let mut data = segment.context.data.clone();
        data.system_inputs = None;
        let cleanup = ExecutionContext::new(data, CancellationToken::new());
        hooks
            .observe(run_id, target, input, &cleanup)
            .await
            .err()
            .map(|error| fail(error.code, "hooks.observer_report"))
    }
}
```

## `crates/wickle/src/agent/resume.rs`

```rust
use super::*;
use std::panic::AssertUnwindSafe;

impl Agent {
    pub(super) async fn resume_command(
        &self,
        command: ResumeCommand,
        context: ExecutionContext,
    ) -> Result<Guarded<RunHandle>, ContractError> {
        self.check_scope(&context)?;
        if matches!(
            command.action,
            ResumeAction::Recover { .. }
                | ResumeAction::Approve {
                    target: ApprovalTarget::Candidate { .. },
                    ..
                }
                | ResumeAction::Deny {
                    target: ApprovalTarget::Candidate { .. },
                    ..
                }
        ) {
            return Err(fail(
                ErrorCode::CapabilityUnsupported,
                "agent.resume_action",
            ));
        }
        if serde_json::to_vec(&command)
            .map_err(|_| fail(ErrorCode::InvalidJson, "agent.command"))?
            .len()
            > self.inner.bindings.settings.max_request_bytes
        {
            return Err(fail(ErrorCode::InvalidContract, "agent.command_size"));
        }
        let saved = self
            .resume_read(
                &context,
                self.inner
                    .bindings
                    .state
                    .load(&self.inner.bindings.scope, &command.run_id),
            )
            .await?;
        if let Guarded::ApprovalRequired(challenge) = self
            .authorize_resume(
                &command,
                &context,
                saved_binding_digest(&saved.snapshot, &command),
            )
            .await?
        {
            return Ok(Guarded::ApprovalRequired(challenge));
        }
        self.resume_inputs(&saved.snapshot, &context).await?;
        if let Some(receipt) = accepted(&saved.snapshot, &command)? {
            return Ok(Guarded::Completed(
                self.handle(command.run_id, receipt.accepted_revision)?,
            ));
        }
        validate_wait(&saved.snapshot, &command)?;
        let lease = match self.waiting_lease(&command.run_id, &context).await {
            Ok(lease) => lease,
            Err(error) => {
                let latest = self
                    .resume_read(
                        &context,
                        self.inner
                            .bindings
                            .state
                            .load(&self.inner.bindings.scope, &command.run_id),
                    )
                    .await?;
                if let Some(receipt) = accepted(&latest.snapshot, &command)? {
                    return Ok(Guarded::Completed(
                        self.handle(command.run_id, receipt.accepted_revision)?,
                    ));
                }
                return Err(error);
            }
        };
        let result = self.resume_owned(&command, &context, &lease).await;
        match result {
            Ok((receipt, prompt, true, observer_error)) => {
                Ok(Guarded::Completed(self.launch_resumed(
                    command.run_id,
                    &receipt,
                    prompt,
                    context,
                    lease,
                    observer_error,
                )?))
            }
            Ok((receipt, _, false, _)) => {
                self.release_owned(&command.run_id, &lease).await;
                Ok(Guarded::Completed(
                    self.handle(command.run_id, receipt.accepted_revision)?,
                ))
            }
            Err(error) => {
                self.release_owned(&command.run_id, &lease).await;
                Err(error)
            }
        }
    }

    async fn resume_owned(
        &self,
        command: &ResumeCommand,
        context: &ExecutionContext,
        lease: &RunLease,
    ) -> Result<
        (
            ResumeReceipt,
            PromptSnapshot,
            bool,
            Vec<(HookTarget, HookInput)>,
        ),
        ContractError,
    > {
        let bindings = &self.inner.bindings;
        let saved = self
            .resume_read(
                context,
                bindings.state.load(&bindings.scope, &command.run_id),
            )
            .await?;
        let prompt = self.restore_resume_runtime(&saved, context).await?;
        if let Some(receipt) = accepted(&saved.snapshot, command)? {
            return Ok((receipt.clone(), prompt, false, vec![]));
        }
        validate_wait(&saved.snapshot, command)?;
        self.resume_inputs(&saved.snapshot, context).await?;
        let wait = saved
            .snapshot
            .wait
            .as_ref()
            .expect("validated waiting snapshot");
        let budget = RunBudget::attach(
            bindings.state.clone(),
            bindings.clock.clone(),
            bindings.ids.clone(),
            bindings.scope.clone(),
            command.run_id.clone(),
            lease.clone(),
            CancellationToken::new(),
        )
        .await?;
        let (_, now) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
        let expires_at_ms = saved
            .snapshot
            .timing
            .deadline_at_ms
            .min(wait.expires_at_ms.unwrap_or(i64::MAX));
        let expired = now >= expires_at_ms;
        let mut fixed_binding_digest = saved_binding_digest(&saved.snapshot, command);
        let prepared = if expired {
            None
        } else {
            let (call, bound) = self.resume_bound(&saved, context).await?;
            fixed_binding_digest = Some(bound.binding_digest().clone());
            if let Guarded::ApprovalRequired(_) = self
                .authorize_resume(command, context, fixed_binding_digest.clone())
                .await?
            {
                return Err(fail(ErrorCode::AccessDenied, "agent.resume_approval"));
            }
            let segment = self.metadata_segment(&saved, context.clone()).await?;
            let round = self.tool_round(&budget, &segment).await?;
            match &command.action {
                ResumeAction::Approve { .. } => None,
                ResumeAction::Deny { .. } => Some(round.prepare_denial(
                    &saved,
                    &call.call_id,
                    &bound,
                    Id::new("approval_denied")?,
                    now,
                )?),
                ResumeAction::Input { answer, .. } => {
                    let WaitTarget::Input { request } = &wait.target else {
                        return Err(fail(ErrorCode::InvalidReference, "agent.input_wait"));
                    };
                    Some(round.prepare_input(&saved, request, &bound, answer.clone(), now)?)
                }
                ResumeAction::External { receipt_ref, .. } => {
                    let verifier =
                        bindings.external_receipt_verifier.as_ref().ok_or_else(|| {
                            fail(ErrorCode::CapabilityUnsupported, "agent.external_verifier")
                        })?;
                    let entry = saved
                        .snapshot
                        .tool_ledger
                        .iter()
                        .find(|entry| entry.call.call_id == call.call_id)
                        .expect("resolved call");
                    let ToolCallState::Unknown {
                        attempt_id,
                        idempotency_key,
                    } = &entry.state
                    else {
                        return Err(fail(ErrorCode::InvalidTransition, "agent.external_wait"));
                    };
                    self.authorize_receipt(receipt_ref, context).await?;
                    let record = self
                        .resume_read(
                            context,
                            bindings.state.read_record(&bindings.scope, receipt_ref),
                        )
                        .await?;
                    if record.reference() != receipt_ref {
                        return Err(fail(ErrorCode::InvalidSnapshot, "agent.receipt_reference"));
                    }
                    if serde_json::to_vec(record.value())
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.receipt"))?
                        .len()
                        > bindings.settings.max_request_bytes
                    {
                        return Err(fail(ErrorCode::InvalidArguments, "agent.receipt_size"));
                    }
                    let request = ExternalReceiptRequest {
                        call,
                        attempt_id: attempt_id.clone(),
                        idempotency_key: idempotency_key.clone(),
                        bound_input: bound.clone(),
                        receipt_ref: receipt_ref.clone(),
                        receipt: record.value().clone(),
                    };
                    self.authorize_receipt(receipt_ref, context).await?;
                    let (_, now) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
                    let current_lease = bindings
                        .state
                        .check_lease(&bindings.scope, &command.run_id, lease, now)
                        .await?;
                    let (_, now) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
                    if now >= current_lease.expires_at_ms {
                        return Err(fail(ErrorCode::LeaseLost, "agent.external_verifier"));
                    }
                    let remaining = saved
                        .snapshot
                        .timing
                        .deadline_at_ms
                        .min(wait.expires_at_ms.unwrap_or(i64::MAX))
                        .min(current_lease.expires_at_ms)
                        .saturating_sub(now)
                        .max(0) as u64;
                    let timeout = Duration::from_millis(
                        bindings
                            .settings
                            .start_timeout_ms
                            .min(bindings.settings.lease_ttl_ms / 2)
                            .min(remaining),
                    );
                    let cancellation = CancellationToken::new();
                    let _cancel = cancellation.clone().drop_guard();
                    let verification = ExternalReceiptContext {
                        scope: bindings.scope.clone(),
                        principal_ref: context.data.principal_ref.clone(),
                        capability_grant_ref: context.data.capability_grant_ref.clone(),
                        cancellation,
                        deadline: tokio::time::Instant::now() + timeout,
                    };
                    let verified = caller_read(context, Some(timeout), async {
                        AssertUnwindSafe(verifier.verify(&request, &verification))
                            .catch_unwind()
                            .await
                            .map_err(|_| {
                                fail(ErrorCode::InvalidContract, "agent.receipt_verifier")
                            })?
                            .map_err(|error| fail(error.code, "agent.receipt_verifier"))
                    })
                    .await?;
                    verification.cancellation.cancel();
                    self.authorize_receipt(receipt_ref, context).await?;
                    Some(round.prepare_external(
                        &saved,
                        &request.call.call_id,
                        &bound,
                        verified,
                        now,
                    )?)
                }
                ResumeAction::Recover { .. } => {
                    return Err(fail(ErrorCode::CapabilityUnsupported, "agent.recovery"));
                }
            }
        };
        if let Guarded::ApprovalRequired(_) = self
            .authorize_resume(command, context, fixed_binding_digest)
            .await?
        {
            return Err(fail(ErrorCode::AccessDenied, "agent.resume_approval"));
        }
        if context.cancellation.is_cancelled() {
            return Err(fail(ErrorCode::Cancelled, "agent.resume"));
        }
        let old_outcome = saved
            .snapshot
            .outcome
            .as_ref()
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.wait_outcome"))?;
        let outcome_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(old_outcome)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.wait_outcome"))?,
        );
        let command_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(command)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.command"))?,
        );
        let mut receipt = ResumeReceipt {
            command: command.clone(),
            command_ref: command_record.reference().clone(),
            accepted_revision: saved
                .snapshot
                .revision
                .checked_add(1)
                .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.resume"))?,
            previous_segment_start_revision: segment_revision(&saved.snapshot),
            previous_outcome_ref: outcome_record.reference().clone(),
            previous_last_event_seq: saved.snapshot.last_event_seq,
            actor_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
            expired,
        };
        let mut records = vec![outcome_record, command_record];
        let mut messages = vec![];
        let mut events = vec![];
        let mut snapshot = saved.snapshot;
        let observation = prepared.as_ref().and_then(|prepared| {
            if let RunEventPayload::ToolSettled { result_ref } = &prepared.event.payload {
                Some((
                    HookTarget::AfterTool {
                        call_id: prepared.result.call_id.clone(),
                        result_ref: result_ref.clone(),
                    },
                    HookInput::tool_observed(&prepared.result.call_id, &prepared.result),
                ))
            } else {
                None
            }
        });
        if let Some(prepared) = prepared {
            apply_resolution(
                &mut snapshot,
                &mut messages,
                &mut events,
                &mut records,
                prepared,
            )?;
        }
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let current_lease = bindings
            .state
            .check_lease(&bindings.scope, &command.run_id, lease, now)
            .await?;
        let (elapsed, now) = budget.settlement_time(elapsed)?;
        if now >= current_lease.expires_at_ms {
            return Err(fail(ErrorCode::LeaseLost, "agent.resume"));
        }
        receipt.expired = now >= expires_at_ms;
        snapshot.revision = receipt.accepted_revision;
        snapshot.status = RunStatus::Running;
        snapshot.phase = RunPhase::Tool;
        snapshot.wait = None;
        snapshot.outcome = None;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        snapshot.last_event_seq = snapshot
            .last_event_seq
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::InvalidEvent, "agent.resume"))?;
        snapshot.resume_receipts.push(receipt.clone());
        events.push(RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: bindings.ids.next_id()?,
            scope: bindings.scope.clone(),
            run_id: command.run_id.clone(),
            session_id: snapshot.request.session_id.clone(),
            seq: snapshot
                .last_event_seq
                .try_into()
                .map_err(|_| fail(ErrorCode::InvalidEvent, "agent.resume"))?,
            timestamp_ms: now,
            payload: RunEventPayload::RunResumed {
                command_ref: receipt.command_ref.clone(),
            },
        });
        let commit = bindings
            .state
            .commit(
                &bindings.scope,
                &command.run_id,
                CommitInput {
                    expected_revision: command.expected_revision,
                    lease: lease.clone(),
                    now_ms: now,
                    snapshot,
                    messages,
                    events,
                    records,
                },
            )
            .await;
        if let Err(error) = commit {
            let latest = bindings
                .state
                .load(&bindings.scope, &command.run_id)
                .await?;
            if accepted(&latest.snapshot, command)?.is_some_and(|saved| saved == &receipt) {
                return Ok((receipt, prompt, true, observation.into_iter().collect()));
            }
            return Err(error);
        }
        Ok((receipt, prompt, true, observation.into_iter().collect()))
    }

    async fn authorize_resume(
        &self,
        command: &ResumeCommand,
        context: &ExecutionContext,
        binding_digest: Option<JsonDigest>,
    ) -> Result<Guarded<()>, ContractError> {
        let request = PolicyRequest {
            owner_scope: self.inner.bindings.scope.clone(),
            resource_id: command.run_id.clone(),
            action: PolicyAction::ResumeRun {
                command: Box::new(command.clone()),
                binding_digest,
            },
        };
        self.inner
            .bindings
            .policy
            .guard(&request, context, None, None, || async { Ok(()) })
            .await
    }
    async fn authorize_receipt(
        &self,
        reference: &RecordRef,
        context: &ExecutionContext,
    ) -> Result<(), ContractError> {
        let request = PolicyRequest {
            owner_scope: self.inner.bindings.scope.clone(),
            resource_id: reference.record_id.clone(),
            action: PolicyAction::ReadRecord {},
        };
        match self
            .inner
            .bindings
            .policy
            .guard(&request, context, None, None, || async { Ok(()) })
            .await?
        {
            Guarded::Completed(()) => Ok(()),
            Guarded::ApprovalRequired(_) => {
                Err(fail(ErrorCode::AccessDenied, "agent.receipt_read"))
            }
        }
    }
    async fn resume_inputs(
        &self,
        snapshot: &RunSnapshot,
        context: &ExecutionContext,
    ) -> Result<(), ContractError> {
        if self.inner.profile.digest() != *snapshot.profile.profile_digest() {
            return Err(fail(ErrorCode::ProfileMismatch, "agent.resume_profile"));
        }
        if let Some(reference) = &snapshot.system_inputs {
            let record = self
                .resume_read(
                    context,
                    self.inner
                        .bindings
                        .state
                        .read_record(&snapshot.scope, &reference.snapshot_ref),
                )
                .await?;
            RunSystemInputs::from_value(record.value(), reference, &snapshot.scope)?
                .validate_resume(context.data.system_inputs.as_ref())?;
        } else if context
            .data
            .system_inputs
            .as_ref()
            .is_some_and(|inputs| !inputs.values().is_empty())
        {
            return Err(fail(ErrorCode::SystemInputsMismatch, "agent.system_inputs"));
        }
        Ok(())
    }
    async fn restore_resume_runtime(
        &self,
        saved: &StoredRun,
        context: &ExecutionContext,
    ) -> Result<PromptSnapshot, ContractError> {
        let bindings = &self.inner.bindings;
        let record = self
            .resume_read(
                context,
                bindings
                    .state
                    .read_record(&bindings.scope, &saved.session.prompt_snapshot),
            )
            .await?;
        let prompt = PromptSnapshot::restore(
            &serde_json::to_string(record.value())
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.prompt"))?,
            &saved.session.prompt_snapshot.digest,
            &saved.snapshot.profile,
            &bindings.scope,
        )?;
        let metadata = self.metadata_segment(saved, context.clone()).await?;
        let tools = metadata
            .tools
            .prompt_bindings(saved.snapshot.profile.profile())?;
        if tools.len() != prompt.tools().len()
            || tools.iter().zip(prompt.tools()).any(|(tool, pinned)| {
                tool.selection != pinned.selection
                    || tool.compiled.digest() != &pinned.compiled_digest
            })
        {
            return Err(fail(ErrorCode::ContextMismatch, "agent.pinned_tools"));
        }
        let expected = saved
            .snapshot
            .routing_snapshot_ref
            .as_ref()
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.routing"))?;
        let current =
            std::panic::catch_unwind(AssertUnwindSafe(|| bindings.router.snapshot().digest()))
                .map_err(|_| fail(ErrorCode::ModelRoutingMismatch, "agent.router"))?;
        if current != expected.digest {
            return Err(fail(
                ErrorCode::ModelRoutingMismatch,
                "agent.pinned_routing",
            ));
        }
        if let Some(assembly) = self.saved_assembly(saved).await? {
            let plan = HookRegistry::metadata(bindings.scope.clone(), assembly.hooks().to_vec())?
                .plan(saved.snapshot.profile.profile())?;
            if saved
                .snapshot
                .hook_plan_ref
                .as_ref()
                .map(|reference| &reference.digest)
                != Some(&plan.digest())
            {
                return Err(fail(ErrorCode::ContextMismatch, "agent.pinned_hooks"));
            }
        } else {
            match (&saved.snapshot.hook_plan_ref, &bindings.hooks) {
                (Some(reference), Some(hooks))
                    if hooks.plan(saved.snapshot.profile.profile())?.digest()
                        == reference.digest => {}
                (None, None) => {}
                (None, Some(hooks))
                    if hooks
                        .plan(saved.snapshot.profile.profile())?
                        .definitions()
                        .is_empty() => {}
                _ => return Err(fail(ErrorCode::ContextMismatch, "agent.pinned_hooks")),
            }
        }
        Ok(prompt)
    }
    async fn resume_bound(
        &self,
        saved: &StoredRun,
        context: &ExecutionContext,
    ) -> Result<(ToolCall, BoundToolInput), ContractError> {
        let wait = saved
            .snapshot
            .wait
            .as_ref()
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.wait"))?;
        let call_id = match &wait.target {
            WaitTarget::Approval {
                target: ApprovalTarget::Tool { call_id, .. },
            }
            | WaitTarget::External { call_id, .. } => call_id,
            WaitTarget::Input { request } => &request.call_id,
            _ => return Err(fail(ErrorCode::CapabilityUnsupported, "agent.wait")),
        };
        let call = saved
            .snapshot
            .tool_ledger
            .iter()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.wait_call"))?
            .call
            .clone();
        let metadata = self.metadata_segment(saved, context.clone()).await?;
        let registered = metadata
            .tools
            .get(&call.tool_name)
            .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "agent.wait_tool"))?;
        let reference = call
            .bound_input_ref
            .as_ref()
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.wait_binding"))?;
        let record = self
            .resume_read(
                context,
                self.inner
                    .bindings
                    .state
                    .read_record(&saved.snapshot.scope, reference),
            )
            .await?;
        let bound = BoundToolInput::restore(
            &record,
            &registered.compiled,
            &saved.snapshot.scope,
            &saved.snapshot.run_id,
            &call,
            saved.snapshot.system_inputs.as_ref(),
        )?;
        if let WaitTarget::Approval {
            target: ApprovalTarget::Tool { binding_digest, .. },
        } = &wait.target
        {
            if binding_digest != bound.binding_digest() {
                return Err(fail(ErrorCode::InvalidSnapshot, "agent.wait_binding"));
            }
        }
        Ok((call, bound))
    }

    async fn waiting_lease(
        &self,
        run_id: &Id,
        context: &ExecutionContext,
    ) -> Result<RunLease, ContractError> {
        let bindings = &self.inner.bindings;
        let owner = bindings.ids.next_id()?;
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(bindings.settings.start_timeout_ms);
        loop {
            if context.cancellation.is_cancelled() {
                return Err(fail(ErrorCode::Cancelled, "agent.resume"));
            }
            let saved = self
                .resume_read(context, bindings.state.load(&bindings.scope, run_id))
                .await?;
            if saved.snapshot.status != RunStatus::Waiting {
                return Err(fail(ErrorCode::InvalidTransition, "agent.wait"));
            }
            match bindings
                .state
                .acquire_lease(
                    &bindings.scope,
                    run_id,
                    &owner,
                    bindings.clock.now()?.utc_ms,
                    bindings.settings.lease_ttl_ms,
                )
                .await
            {
                Ok(lease) => return Ok(lease),
                Err(error) if error.code == ErrorCode::LeaseBusy => {}
                Err(error) => return Err(error),
            }
            tokio::select! { biased;
                _ = context.cancellation.cancelled() => return Err(fail(ErrorCode::Cancelled, "agent.resume")),
                _ = tokio::time::sleep_until(deadline) => return Err(fail(ErrorCode::LeaseBusy, "agent.wait_handoff")),
                _ = tokio::time::sleep(Duration::from_millis(bindings.settings.observer_poll_ms.min(20))) => {},
            }
        }
    }
    pub(super) async fn release_owned(&self, run_id: &Id, lease: &RunLease) {
        if let Ok(now) = self.inner.bindings.clock.now() {
            let _ = self
                .inner
                .bindings
                .state
                .release_lease(&self.inner.bindings.scope, run_id, lease, now.utc_ms)
                .await;
        }
    }
    async fn resume_read<T>(
        &self,
        context: &ExecutionContext,
        future: impl std::future::Future<Output = Result<T, ContractError>>,
    ) -> Result<T, ContractError> {
        caller_read(
            context,
            Some(Duration::from_millis(
                self.inner.bindings.settings.start_timeout_ms,
            )),
            future,
        )
        .await
    }
    fn launch_resumed(
        &self,
        run_id: Id,
        receipt: &ResumeReceipt,
        prompt: PromptSnapshot,
        context: ExecutionContext,
        lease: RunLease,
        observer_error: Vec<(HookTarget, HookInput)>,
    ) -> Result<RunHandle, ContractError> {
        let segment_start_revision = receipt.accepted_revision;
        let expired = receipt.expired;
        let local = Arc::new(LocalRun::new(segment_start_revision));
        *local
            .pending_observations
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.observers"))? = observer_error;
        self.inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .insert(run_id.clone(), local.clone());
        let agent = self.clone();
        let driver_id = run_id.clone();
        let driver_local = local.clone();
        let mut data = context.data;
        data.system_inputs = None;
        let context = ExecutionContext::new(data, local.cancel.clone());
        tokio::spawn(async move {
            let result = AssertUnwindSafe(agent.drive_leased(
                &driver_id,
                prompt,
                context,
                &driver_local,
                lease,
                expired,
            ))
            .catch_unwind()
            .await;
            let error = match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(fail(error.code, "agent.driver")),
                Err(_) => Some(fail(ErrorCode::InvalidContract, "agent.driver")),
            };
            let completed = error.is_none() && !agent.keep_local(&driver_local);
            if let Ok(mut slot) = driver_local.error.lock() {
                *slot = error;
            }
            driver_local.done.store(true, Ordering::Release);
            driver_local.notify.notify_waiters();
            if completed {
                if let Ok(mut runs) = agent.inner.runs.lock() {
                    if runs
                        .get(&driver_id)
                        .is_some_and(|current| Arc::ptr_eq(current, &driver_local))
                    {
                        runs.remove(&driver_id);
                    }
                }
            }
        });
        Ok(RunHandle {
            agent: self.clone(),
            run_id,
            segment_start_revision,
            local: Some(local),
        })
    }

    pub(super) async fn cancel_waiting(
        &self,
        run_id: Id,
        reason: Id,
        context: ExecutionContext,
    ) -> Result<CancelReceipt, ContractError> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| fail(ErrorCode::RuntimeUnavailable, "agent.runtime"))?;
        let agent = self.clone();
        runtime
            .spawn(async move { agent.cancel_waiting_owned(run_id, reason, context).await })
            .await
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.cancel"))?
    }
    async fn cancel_waiting_owned(
        &self,
        run_id: Id,
        reason: Id,
        context: ExecutionContext,
    ) -> Result<CancelReceipt, ContractError> {
        let bindings = &self.inner.bindings;
        let lease = match self.waiting_lease(&run_id, &context).await {
            Ok(lease) => lease,
            Err(error) => {
                if bindings
                    .state
                    .load(&bindings.scope, &run_id)
                    .await?
                    .snapshot
                    .status
                    .is_terminal()
                {
                    return Ok(CancelReceipt::AlreadyTerminal);
                }
                return Err(error);
            }
        };
        let result = async {
            let mut saved = self
                .resume_read(&context, bindings.state.load(&bindings.scope, &run_id))
                .await?;
            if saved.snapshot.status.is_terminal() {
                return Ok(CancelReceipt::AlreadyTerminal);
            }
            if saved.snapshot.status != RunStatus::Waiting {
                return Err(fail(ErrorCode::InvalidTransition, "agent.cancel_wait"));
            }
            let policy = PolicyRequest {
                owner_scope: bindings.scope.clone(),
                resource_id: run_id.clone(),
                action: PolicyAction::CancelRun {},
            };
            if let Guarded::ApprovalRequired(_) = bindings
                .policy
                .guard(&policy, &context, None, None, || async { Ok(()) })
                .await?
            {
                return Err(fail(ErrorCode::AccessDenied, "agent.cancel_wait"));
            }
            let budget = RunBudget::attach(
                bindings.state.clone(),
                bindings.clock.clone(),
                bindings.ids.clone(),
                bindings.scope.clone(),
                run_id.clone(),
                lease.clone(),
                CancellationToken::new(),
            )
            .await?;
            let segment = self.metadata_segment(&saved, context.clone()).await?;
            let round = self.tool_round(&budget, &segment).await?;
            let expected_revision = saved.snapshot.revision;
            let mut messages = vec![];
            let mut events = vec![];
            let mut records = vec![];
            let mut observations = vec![];
            let calls: Vec<_> = saved
                .snapshot
                .tool_ledger
                .iter()
                .filter(|entry| {
                    matches!(
                        entry.state,
                        ToolCallState::Planned {}
                            | ToolCallState::ApprovalPending { .. }
                            | ToolCallState::InputPending { .. }
                    )
                })
                .map(|entry| entry.call.call_id.clone())
                .collect();
            for call in calls {
                let (_, now) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
                let prepared = round.prepare_unstarted(
                    &saved,
                    &call,
                    ToolResultStatus::Cancelled,
                    Id::new("cancelled")?,
                    now,
                )?;
                if let RunEventPayload::ToolSettled { result_ref } = &prepared.event.payload {
                    observations.push((
                        HookTarget::AfterTool {
                            call_id: prepared.result.call_id.clone(),
                            result_ref: result_ref.clone(),
                        },
                        HookInput::tool_observed(&prepared.result.call_id, &prepared.result),
                    ));
                }
                saved.session.transcript_revision += 1;
                saved.messages.push(prepared.message.clone());
                apply_resolution(
                    &mut saved.snapshot,
                    &mut messages,
                    &mut events,
                    &mut records,
                    prepared,
                )?;
            }
            let (elapsed, now) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
            let mut snapshot = saved.snapshot;
            snapshot.revision = snapshot
                .revision
                .checked_add(1)
                .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.cancel_wait"))?;
            snapshot.status = RunStatus::Cancelled;
            snapshot.phase = RunPhase::Finish;
            snapshot.wait = None;
            snapshot.usage.elapsed_ms = elapsed;
            snapshot.timing.last_observed_at_ms = now;
            let previous = snapshot
                .outcome
                .take()
                .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.wait_outcome"))?;
            let outcome = RunOutcome {
                result: OutcomeResult::Cancelled {
                    reason: reason.to_string(),
                },
                output: previous.output,
                artifacts: previous.artifacts,
                usage: snapshot.usage.clone(),
                checkpoint_revision: snapshot.revision,
                verification: None,
                unresolved_effects: previous.unresolved_effects,
            };
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(&outcome)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.cancel_wait"))?,
            );
            snapshot.last_event_seq = snapshot
                .last_event_seq
                .checked_add(1)
                .ok_or_else(|| fail(ErrorCode::InvalidEvent, "agent.cancel_wait"))?;
            events.push(RunEvent {
                schema_version: RunEventSchemaVersion::V1,
                event_id: bindings.ids.next_id()?,
                scope: bindings.scope.clone(),
                run_id: run_id.clone(),
                session_id: snapshot.request.session_id.clone(),
                seq: snapshot
                    .last_event_seq
                    .try_into()
                    .map_err(|_| fail(ErrorCode::InvalidEvent, "agent.cancel_wait"))?,
                timestamp_ms: now,
                payload: RunEventPayload::RunFinished {
                    outcome_ref: record.reference().clone(),
                },
            });
            records.push(record);
            snapshot.outcome = Some(outcome);
            let commit = bindings
                .state
                .commit(
                    &bindings.scope,
                    &run_id,
                    CommitInput {
                        expected_revision,
                        lease: lease.clone(),
                        now_ms: now,
                        snapshot,
                        messages,
                        events,
                        records,
                    },
                )
                .await;
            if let Err(error) = commit {
                if bindings
                    .state
                    .load(&bindings.scope, &run_id)
                    .await?
                    .snapshot
                    .status
                    != RunStatus::Cancelled
                {
                    return Err(error);
                }
            }
            let saved = bindings.state.load(&bindings.scope, &run_id).await?;
            let local = Arc::new(LocalRun::new(segment_revision(&saved.snapshot)));
            self.cleanup_observers(&saved, &context, &local, observations)
                .await;
            local.done.store(true, Ordering::Release);
            if self.keep_local(&local) {
                self.inner
                    .runs
                    .lock()
                    .map_err(|_| fail(ErrorCode::InvalidContract, "agent.observer_state"))?
                    .insert(run_id.clone(), local);
            }
            Ok(CancelReceipt::Requested)
        }
        .await;
        self.release_owned(&run_id, &lease).await;
        result
    }
}

fn accepted<'a>(
    snapshot: &'a RunSnapshot,
    command: &ResumeCommand,
) -> Result<Option<&'a ResumeReceipt>, ContractError> {
    let receipt = snapshot
        .resume_receipts
        .iter()
        .find(|receipt| receipt.command.command_id == command.command_id);
    if receipt.is_some_and(|receipt| &receipt.command != command) {
        return Err(fail(ErrorCode::RequestConflict, "agent.resume_command"));
    }
    Ok(receipt)
}
fn saved_binding_digest(snapshot: &RunSnapshot, command: &ResumeCommand) -> Option<JsonDigest> {
    if let Some(receipt) = snapshot
        .resume_receipts
        .iter()
        .find(|receipt| receipt.command.command_id == command.command_id)
    {
        return match &receipt.command.action {
            ResumeAction::Approve {
                target: ApprovalTarget::Tool { binding_digest, .. },
                ..
            }
            | ResumeAction::Deny {
                target: ApprovalTarget::Tool { binding_digest, .. },
                ..
            } => Some(binding_digest.clone()),
            _ => None,
        };
    }
    match snapshot.wait.as_ref().map(|wait| &wait.target) {
        Some(WaitTarget::Approval {
            target: ApprovalTarget::Tool { binding_digest, .. },
        }) => Some(binding_digest.clone()),
        _ => None,
    }
}
fn validate_wait(snapshot: &RunSnapshot, command: &ResumeCommand) -> Result<(), ContractError> {
    if snapshot.status != RunStatus::Waiting {
        return Err(fail(ErrorCode::InvalidTransition, "agent.wait"));
    }
    if snapshot.revision != command.expected_revision {
        return Err(fail(ErrorCode::RevisionConflict, "agent.resume"));
    }
    let wait = snapshot
        .wait
        .as_ref()
        .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.wait"))?;
    let matched = match (&wait.target, &command.action) {
        (
            WaitTarget::Approval { target },
            ResumeAction::Approve {
                wait_id,
                target: supplied,
            }
            | ResumeAction::Deny {
                wait_id,
                target: supplied,
                ..
            },
        ) => wait_id == &wait.wait_id && supplied == target,
        (WaitTarget::Input { .. }, ResumeAction::Input { wait_id, .. })
        | (WaitTarget::External { .. }, ResumeAction::External { wait_id, .. }) => {
            wait_id == &wait.wait_id
        }
        _ => false,
    };
    if !matched {
        return Err(fail(ErrorCode::InvalidReference, "agent.wait_target"));
    }
    Ok(())
}
fn apply_resolution(
    snapshot: &mut RunSnapshot,
    messages: &mut Vec<Message>,
    events: &mut Vec<RunEvent>,
    records: &mut Vec<ProtectedRecord>,
    prepared: PreparedToolResolution,
) -> Result<(), ContractError> {
    let entry = snapshot
        .tool_ledger
        .iter_mut()
        .find(|entry| entry.call.call_id == prepared.result.call_id)
        .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.resume_call"))?;
    entry.state = prepared.state;
    snapshot.last_event_seq = prepared.event.seq.get();
    messages.push(prepared.message);
    events.push(prepared.event);
    records.extend(prepared.records);
    Ok(())
}
```

## `crates/wickle/src/agent/tools.rs`

```rust
use super::*;

impl Agent {
    pub(super) async fn tool_round(
        &self,
        budget: &RunBudget,
        segment: &SegmentBindings,
    ) -> Result<SerialToolRound, ContractError> {
        let bindings = &self.inner.bindings;
        let registry = segment.tools.clone();
        let saved = bindings.state.load(budget.scope(), budget.run_id()).await?;
        let definitions = if let Some(reference) = &saved.snapshot.system_inputs {
            let record = bindings
                .state
                .read_record(budget.scope(), &reference.snapshot_ref)
                .await?;
            let inputs = RunSystemInputs::from_value(record.value(), reference, budget.scope())?;
            SystemInputRegistry::new(inputs.definitions().values().cloned().collect())?
        } else {
            bindings.system_inputs.clone()
        };
        let binder = Arc::new(InputBinder::new(
            Arc::new(definitions),
            bindings.system_input_resolver.clone(),
            bindings.policy.clone(),
            bindings.ids.clone(),
        ));
        let mut round = SerialToolRound::new(
            registry,
            binder,
            bindings.policy.clone(),
            bindings.ids.clone(),
        )
        .with_limits(bindings.settings.tool_execution_limits)?;
        if let Some(hooks) = &segment.hooks {
            round = round.with_hooks(hooks.clone());
        }
        if let Some(binding_set_id) = segment.binding_set_id() {
            round = round.with_binding_set_id(binding_set_id.clone());
        }
        Ok(round)
    }

    /// Commit the original complete model plan before any resolver or tool runs.
    pub(super) async fn plan_tools(
        &self,
        response: &ModelResponse,
        prompt: &PromptSnapshot,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        budget.check_boundary().await?;
        let bindings = &self.inner.bindings;
        let saved = bindings.state.load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        if response.finish != ModelFinish::ToolCalls
            || response.tool_calls.is_empty()
            || snapshot
                .tool_ledger
                .iter()
                .any(|entry| entry.call.model_request_id == response.request_id)
        {
            return Err(fail(ErrorCode::InvalidTransition, "agent.tool_plan"));
        }
        let invocation = snapshot
            .model_ledger
            .iter()
            .find(|invocation| {
                invocation.attempt_id == response.request_id
                    && matches!(invocation.state, ModelAttemptState::Completed {})
            })
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.tool_response"))?;
        if invocation.route.digest() != response.route_digest {
            return Err(fail(ErrorCode::ModelRoutingMismatch, "agent.tool_response"));
        }
        let provider = invocation.route.provider.clone();
        let route_digest = invocation.route.digest();
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let mut content = Vec::new();
        if !response.text.is_empty() {
            content.push(ContentBlock::Content {
                content: InputContent::Text {
                    text: response.text.clone(),
                },
            });
        }
        let mut records = vec![];
        let mut events = vec![];
        for proposed in &response.tool_calls {
            let descriptor_digest = prompt
                .tools()
                .iter()
                .find(|tool| tool.model_tool.name == proposed.name)
                .map(|tool| tool.descriptor_digest.clone());
            let call = ToolCall {
                call_id: bindings.ids.next_id()?,
                model_request_id: response.request_id.clone(),
                provider_call_id: proposed.provider_call_id.clone(),
                tool_name: proposed.name.clone(),
                model_inputs: proposed.model_inputs.clone(),
                descriptor_digest,
                bound_input_ref: None,
            };
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(&call)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.tool_plan"))?,
            );
            snapshot.last_event_seq = snapshot
                .last_event_seq
                .checked_add(1)
                .ok_or_else(|| fail(ErrorCode::InvalidEvent, "agent.tool_plan"))?;
            events.push(RunEvent {
                schema_version: RunEventSchemaVersion::V1,
                event_id: bindings.ids.next_id()?,
                scope: budget.scope().clone(),
                run_id: budget.run_id().clone(),
                session_id: snapshot.request.session_id.clone(),
                seq: snapshot
                    .last_event_seq
                    .try_into()
                    .map_err(|_| fail(ErrorCode::InvalidEvent, "agent.tool_plan"))?,
                timestamp_ms: now,
                payload: RunEventPayload::ToolPlanned {
                    call_ref: record.reference().clone(),
                },
            });
            records.push(record);
            content.push(ContentBlock::ToolCall { call: call.clone() });
            snapshot.tool_ledger.push(ToolLedgerEntry {
                call,
                state: ToolCallState::Planned {},
            });
        }
        for continuation in &response.continuation {
            if continuation.route_digest() != &route_digest {
                return Err(fail(
                    ErrorCode::ModelContextIncompatible,
                    "agent.continuation",
                ));
            }
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(continuation)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.continuation"))?,
            );
            content.push(ContentBlock::ProviderOpaque {
                provider: provider.clone(),
                route_digest: route_digest.clone(),
                data_ref: record.reference().clone(),
            });
            records.push(record);
        }
        let message = Message {
            message_id: bindings.ids.next_id()?,
            run_id: budget.run_id().clone(),
            sequence: saved
                .session
                .transcript_revision
                .checked_add(1)
                .and_then(NonZeroU64::new)
                .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.tool_plan"))?,
            role: MessageRole::Assistant,
            content,
            origin: MessageOrigin::Model,
            visibility: Visibility::UserAndModel,
        };
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.tool_plan"))?;
        snapshot.phase = RunPhase::Tool;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        bindings
            .state
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages: vec![message],
                    events,
                    records,
                },
            )
            .await?;
        Ok(())
    }

    pub(super) async fn tool_wait(
        &self,
        outcome: ToolRoundOutcome,
        budget: &RunBudget,
    ) -> Result<(WaitState, Vec<RecordRef>), ContractError> {
        let bindings = &self.inner.bindings;
        let (target, unresolved) = match outcome {
            ToolRoundOutcome::ApprovalRequired {
                call_id,
                binding_digest,
                ..
            } => (
                WaitTarget::Approval {
                    target: ApprovalTarget::Tool {
                        call_id,
                        binding_digest,
                    },
                },
                vec![],
            ),
            ToolRoundOutcome::Unresolved {
                call_id,
                result_ref,
            } => {
                let saved = bindings.state.load(budget.scope(), budget.run_id()).await?;
                let entry = saved
                    .snapshot
                    .tool_ledger
                    .iter()
                    .find(|entry| entry.call.call_id == call_id)
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.unresolved_tool"))?;
                let ToolCallState::Unknown {
                    idempotency_key, ..
                } = &entry.state
                else {
                    return Err(fail(ErrorCode::InvalidTransition, "agent.unresolved_tool"));
                };
                (
                    WaitTarget::External {
                        call_id,
                        effect_key: idempotency_key.clone(),
                    },
                    vec![result_ref],
                )
            }
            ToolRoundOutcome::InputRequired { request } => (WaitTarget::Input { request }, vec![]),
            ToolRoundOutcome::Completed => {
                return Err(fail(ErrorCode::InvalidTransition, "agent.tool_wait"));
            }
        };
        Ok((
            WaitState {
                wait_id: bindings.ids.next_id()?,
                target,
                expires_at_ms: Some(
                    bindings
                        .state
                        .load(budget.scope(), budget.run_id())
                        .await?
                        .snapshot
                        .timing
                        .deadline_at_ms,
                ),
            },
            unresolved,
        ))
    }

    pub(super) async fn settle_unstarted_tools(
        &self,
        snapshot: &RunSnapshot,
        segment: &SegmentBindings,
        budget: &RunBudget,
        cancelled: bool,
        local: &LocalRun,
    ) -> Result<(), ContractError> {
        let requests: std::collections::BTreeSet<_> = snapshot
            .tool_ledger
            .iter()
            .filter(|entry| {
                matches!(
                    entry.state,
                    ToolCallState::Planned {}
                        | ToolCallState::ApprovalPending { .. }
                        | ToolCallState::InputPending { .. }
                )
            })
            .map(|entry| entry.call.model_request_id.clone())
            .collect();
        if requests.is_empty() {
            return Ok(());
        }
        let round = self.tool_round(budget, segment).await?;
        for request in requests {
            round
                .settle_unstarted(
                    &request,
                    if cancelled {
                        ToolResultStatus::Cancelled
                    } else {
                        ToolResultStatus::Failed
                    },
                    Id::new(if cancelled {
                        "cancelled"
                    } else {
                        "run_stopped"
                    })?,
                    &segment.context,
                    budget,
                )
                .await?;
            self.remember_observer_error(local, round.observer_error());
        }
        Ok(())
    }
}
```

## `crates/wickle/src/component_runtime.rs`

```rust
//! Immutable assembly data and Host-owned adapter lifetime contracts.

use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, fmt, sync::Arc};
use tokio_util::sync::CancellationToken;

/// Version of the protected resolved-assembly wire contract.
pub const RESOLVED_ASSEMBLY_SCHEMA_VERSION: &str = "wickle.resolved-assembly.v1";

/// Full immutable manifest; listing it does not open the adapter.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterDefinition {
    /// Existing component metadata, matched to the resolved profile digest.
    pub metadata: ComponentMetadata,
    /// Full declared export contracts, including unselected metadata-only exports.
    pub exports: Vec<AdapterExportDefinition>,
}
impl fmt::Debug for AdapterDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdapterDefinition")
            .field("reference", &self.metadata.reference)
            .field("export_count", &self.exports.len())
            .finish_non_exhaustive()
    }
}
/// Full contracts for a declared adapter export.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AdapterExportDefinition {
    /// Executable tool contract; compilation fixes its input ownership.
    Tool {
        /// Matching export metadata.
        metadata: ExportMetadata,
        /// Full trusted descriptor.
        descriptor: Box<ToolDescriptor>,
    },
    /// Lifecycle Hook at one exact position.
    Hook {
        /// Matching export metadata.
        metadata: ExportMetadata,
        /// Exact Hook behavior/bounds.
        definition: HookDefinition,
    },
    /// Metadata only until the context-source runtime is provided.
    ContextSource {
        /// Declared source metadata.
        metadata: ExportMetadata,
    },
    /// Host delivery metadata; never activated by an Agent Run.
    EventConsumer {
        /// Declared consumer metadata.
        metadata: ExportMetadata,
    },
}
impl AdapterExportDefinition {
    /// Shared metadata used by profile selection.
    pub fn metadata(&self) -> &ExportMetadata {
        match self {
            Self::Tool { metadata, .. }
            | Self::Hook { metadata, .. }
            | Self::ContextSource { metadata }
            | Self::EventConsumer { metadata } => metadata,
        }
    }
}
/// A Host-prepared connection revision without credentials or SDK objects.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedConnection {
    /// Profile-local connector binding.
    pub binding: ConnectorBindingRef,
    /// Exact connector metadata matched to the resolved profile.
    pub metadata: ComponentMetadata,
    /// Host connection/account configuration revision, not a secret.
    pub connection_ref: VersionedRef,
}
/// Optional Host-prepared binding state. Resolve selects an existing immutable
/// mapping; opening an adapter does not create or update it implicitly.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterBindingState {
    /// Exact namespace of this private mapping.
    pub scope: Scope,
    /// Session for which the mapping was prepared.
    pub session_id: Id,
    /// Profile-local adapter binding.
    pub adapter_binding: Id,
    /// Exact adapter identity/version.
    pub adapter: VersionedRef,
    /// Full AdapterDefinition identity.
    pub definition_digest: JsonDigest,
    /// Immutable identity of the protected mapping value.
    pub state_ref: RecordRef,
    /// Host-prepared data; never automatically projected to a model.
    pub value: Value,
}
impl fmt::Debug for AdapterBindingState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AdapterBindingState(<protected>)")
    }
}
/// One selected adapter binding with all nonsecret metadata pinned.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedAdapterBinding {
    /// Original profile declaration, including config and named mappings.
    pub binding: AdapterBindingRef,
    /// Complete immutable definition.
    pub definition: AdapterDefinition,
    /// Digest of metadata plus all declared full export contracts.
    pub definition_digest: JsonDigest,
    /// Required connection name to exact Host connector revision.
    pub connections: BTreeMap<Id, ResolvedConnection>,
    /// Selected Tool/Hook export identities only.
    pub selected_exports: Vec<ExportRef>,
    /// Optional preexisting, scope/session-bound Host mapping.
    pub binding_state: Option<AdapterBindingState>,
}
/// A real profile Tool selection and its compiled model/system input split.
#[derive(Debug, Clone)]
pub struct ResolvedToolBinding {
    /// Original Catalog or Export selection; aliases never replace authority.
    pub selection: ToolBindingRef,
    /// Catalog metadata; export metadata is retained by its adapter definition.
    pub metadata: Option<ComponentMetadata>,
    /// Exact post-alias compiled contract used by prompt and binder.
    pub compiled: CompiledTool,
}
/// A real profile Hook selection and its exact lifecycle contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedHookBinding {
    /// Original Catalog or Export selection.
    pub selection: HookRef,
    /// Catalog metadata; absent for an adapter export.
    pub metadata: Option<ComponentMetadata>,
    /// Pinned Hook contract.
    pub definition: HookDefinition,
}
/// Caller controls for metadata-only resolution, with no connection open permission.
#[derive(Debug, Clone)]
pub struct ComponentResolveContext {
    /// Exact namespace.
    pub scope: Scope,
    /// Session whose Host binding state may be selected.
    pub session_id: Id,
    /// Current authenticated actor.
    pub principal_ref: Id,
    /// Current Host grant.
    pub capability_grant_ref: Id,
    /// Metadata definitions, not system-input values.
    pub system_inputs: SystemInputRegistry,
    /// Cooperative cancellation.
    pub cancellation: CancellationToken,
    /// Finite metadata deadline.
    pub deadline: tokio::time::Instant,
}
/// Which exports may be activated for this execution segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentBindPurpose {
    /// Selected Tools and lifecycle Hooks for a running segment.
    Execution,
    /// Only after_tool/after_run observers for already committed outcomes.
    ObserversOnly,
}
/// Admission identity and controls checked before any adapter opens.
#[derive(Debug, Clone)]
pub struct ComponentBindContext {
    /// Exact namespace.
    pub scope: Scope,
    /// Admitted Run identity.
    pub run_id: Id,
    /// Owning session.
    pub session_id: Id,
    /// Fresh process-local segment identity; never reused on resume.
    pub binding_set_id: Id,
    /// Current authenticated actor.
    pub principal_ref: Id,
    /// Current Host grant.
    pub capability_grant_ref: Id,
    /// Required current lease for execution; terminal cleanup may omit it.
    pub lease: Option<RunLease>,
    /// Exports allowed to open in this segment.
    pub purpose: ComponentBindPurpose,
    /// Cooperative cancellation for initialization.
    pub cancellation: CancellationToken,
    /// Finite initialization deadline.
    pub deadline: tokio::time::Instant,
}
/// Fixed metadata presented to one approved Factory.
#[derive(Clone)]
pub struct AdapterInitContext {
    /// Exact segment controls and authenticated identity.
    pub execution: ComponentBindContext,
    /// Original resolved binding; no credentials or mutable Store handle.
    pub binding: ResolvedAdapterBinding,
    /// Only the Tool/Hook exports activated for this segment purpose.
    pub selected_exports: Vec<ExportRef>,
}
impl fmt::Debug for AdapterInitContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AdapterInitContext(<protected>)")
    }
}
/// Returned implementation for a selected adapter export.
#[derive(Clone)]
pub enum AdapterExportInstance {
    /// Full returned descriptor is attested against the pinned original contract.
    Tool {
        /// Adapter-local export identity.
        export_id: Id,
        /// Original descriptor before a profile alias is applied.
        descriptor: Box<ToolDescriptor>,
        /// Existing executor.
        executor: Arc<dyn ToolExecutor>,
    },
    /// Returned Hook definition must match the pinned definition.
    Hook {
        /// Adapter-local export identity.
        export_id: Id,
        /// Exact original Hook definition.
        definition: HookDefinition,
        /// Existing Hook handler.
        handler: Arc<dyn HookHandler>,
    },
}
/// Controls for one adapter's explicit asynchronous close.
#[derive(Debug, Clone)]
pub struct AdapterCloseContext {
    /// Exact namespace.
    pub scope: Scope,
    /// Owning Run.
    pub run_id: Id,
    /// Segment being released.
    pub binding_set_id: Id,
    /// Binding being closed.
    pub adapter_binding: Id,
    /// Fresh cooperative cleanup signal.
    pub cancellation: CancellationToken,
    /// Finite cleanup deadline.
    pub deadline: tokio::time::Instant,
}
/// Host-created resource. The runtime never exposes its exports until every
/// selected instance is ready and its returned contracts have been attested.
pub trait AdapterInstance: Send + Sync {
    /// Return exactly the exports requested for this instance.
    fn exports(&self) -> Vec<AdapterExportInstance>;
    /// Idempotent, explicitly awaited cleanup. Implementations also own cleanup
    /// of resources created before open returns an instance or an error.
    fn close<'a>(&'a self, context: &'a AdapterCloseContext) -> PortFuture<'a, ()>;
}
/// Trusted Host code registered by exact adapter definition, never by a profile
/// executable path. Failure/cancellation must clean its partial initialization.
pub trait AdapterFactory: Send + Sync {
    /// Create an instance only after admission and current authorization checks.
    fn open<'a>(
        &'a self,
        context: &'a AdapterInitContext,
    ) -> PortFuture<'a, Arc<dyn AdapterInstance>>;
}
/// Fresh controls for releasing a bound execution segment.
#[derive(Debug, Clone)]
pub struct ComponentReleaseContext {
    /// Exact namespace.
    pub scope: Scope,
    /// Owning Run.
    pub run_id: Id,
    /// Segment whose exports become unusable.
    pub binding_set_id: Id,
    /// Cooperative cleanup cancellation signal.
    pub cancellation: CancellationToken,
    /// Finite total release deadline.
    pub deadline: tokio::time::Instant,
}
/// Safe error from one adapter release; no raw resource or SDK values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentReleaseFailure {
    /// Failed profile-local adapter binding.
    pub adapter_binding: Id,
    /// Safe classified cleanup code.
    pub code: Id,
}
/// Cleanup observations, independent of the already committed Run outcome.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentReleaseReport {
    /// Failures in attempted reverse release order.
    pub failures: Vec<ComponentReleaseFailure>,
}
/// Host lifetime owner; invalidate wrappers before reverse bounded close begins.
pub trait ComponentRelease: Send + Sync {
    /// Immediately revoke wrappers even when asynchronous close cannot run.
    fn invalidate(&self);
    /// Explicit, idempotent release; Drop is not an asynchronous cleanup mechanism.
    fn release<'a>(
        &'a self,
        context: &'a ComponentReleaseContext,
    ) -> PortFuture<'a, ComponentReleaseReport>;
}
impl Drop for BoundCapabilities {
    fn drop(&mut self) {
        self.release.invalidate();
    }
}
/// Non-clone segment owner exposing only fully staged scoped registries.
pub struct BoundCapabilities {
    scope: Scope,
    run_id: Id,
    binding_set_id: Id,
    tools: Arc<ToolRegistry>,
    hooks: Arc<HookRegistry>,
    release: Arc<dyn ComponentRelease>,
}
impl BoundCapabilities {
    /// Publish ready registries from a trusted runtime after all attestation.
    pub fn new(
        scope: Scope,
        run_id: Id,
        binding_set_id: Id,
        tools: Arc<ToolRegistry>,
        hooks: Arc<HookRegistry>,
        release: Arc<dyn ComponentRelease>,
    ) -> Result<Self, ContractError> {
        if tools.scope() != &scope || hooks.scope() != &scope {
            return Err(component_error(ErrorCode::AccessDenied, "components.scope"));
        }
        Ok(Self {
            scope,
            run_id,
            binding_set_id,
            tools,
            hooks,
            release,
        })
    }
    /// Exact namespace.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Owning Run.
    pub fn run_id(&self) -> &Id {
        &self.run_id
    }
    /// Process-local segment identity.
    pub fn binding_set_id(&self) -> &Id {
        &self.binding_set_id
    }
    /// Ready Tool bindings; surviving clones must still enforce the lifetime token.
    pub fn tools(&self) -> &Arc<ToolRegistry> {
        &self.tools
    }
    /// Ready Hook bindings and fixed selection metadata.
    pub fn hooks(&self) -> &Arc<HookRegistry> {
        &self.hooks
    }
    /// Explicit cleanup independent of result status.
    pub fn release<'a>(
        &'a self,
        context: &'a ComponentReleaseContext,
    ) -> PortFuture<'a, ComponentReleaseReport> {
        Box::pin(async move {
            if context.scope != self.scope
                || context.run_id != self.run_id
                || context.binding_set_id != self.binding_set_id
            {
                return Err(component_error(
                    ErrorCode::AccessDenied,
                    "components.release_scope",
                ));
            }
            self.release.release(context).await
        })
    }
}
impl fmt::Debug for BoundCapabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundCapabilities")
            .field("binding_set_id", &self.binding_set_id)
            .finish_non_exhaustive()
    }
}
/// Core injection boundary. Implementations resolve data before admission and
/// privately stage scoped resources only after the admitted assembly is fixed.
pub trait ComponentRuntime: Send + Sync {
    /// Resolve metadata without opening adapters or executing tools/hooks.
    fn resolve<'a>(
        &'a self,
        profile: &'a ResolvedProfile,
        context: &'a ComponentResolveContext,
    ) -> PortFuture<'a, ResolvedAssembly>;
    /// Bind an admitted assembly with a fresh segment identity.
    fn bind<'a>(
        &'a self,
        assembly: &'a ResolvedAssembly,
        context: &'a ComponentBindContext,
    ) -> PortFuture<'a, BoundCapabilities>;
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredToolBinding {
    selection: ToolBindingRef,
    metadata: Option<ComponentMetadata>,
    compiled: Value,
    compiled_digest: JsonDigest,
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AssemblyData {
    schema_version: String,
    scope: Scope,
    session_id: Id,
    profile_resolution_digest: JsonDigest,
    system_inputs: Vec<SystemInputDefinition>,
    connections: Vec<ResolvedConnection>,
    adapters: Vec<ResolvedAdapterBinding>,
    tools: Vec<StoredToolBinding>,
    hooks: Vec<ResolvedHookBinding>,
}
/// Validated immutable metadata used across process-local binding segments.
#[derive(Clone)]
pub struct ResolvedAssembly {
    data: AssemblyData,
    tools: Vec<ResolvedToolBinding>,
}
impl ResolvedAssembly {
    /// Freeze an assembly resolved from trusted metadata before opening resources.
    pub fn new(
        profile: &ResolvedProfile,
        context: &ComponentResolveContext,
        connections: Vec<ResolvedConnection>,
        adapters: Vec<ResolvedAdapterBinding>,
        tools: Vec<ResolvedToolBinding>,
        hooks: Vec<ResolvedHookBinding>,
    ) -> Result<Self, ContractError> {
        let data = AssemblyData {
            schema_version: RESOLVED_ASSEMBLY_SCHEMA_VERSION.into(),
            scope: context.scope.clone(),
            session_id: context.session_id.clone(),
            profile_resolution_digest: profile.resolution_digest().clone(),
            system_inputs: context
                .system_inputs
                .definitions()
                .values()
                .cloned()
                .collect(),
            connections,
            adapters,
            tools: tools
                .iter()
                .map(|tool| {
                    Ok(StoredToolBinding {
                        selection: tool.selection.clone(),
                        metadata: tool.metadata.clone(),
                        compiled: serde_json::to_value(&tool.compiled).map_err(|_| {
                            component_error(ErrorCode::InvalidJson, "assembly.tool")
                        })?,
                        compiled_digest: tool.compiled.digest().clone(),
                    })
                })
                .collect::<Result<Vec<_>, ContractError>>()?,
            hooks,
        };
        let assembly = Self { data, tools };
        assembly.validate(profile, &context.system_inputs)?;
        Ok(assembly)
    }
    /// Exact namespace.
    pub fn scope(&self) -> &Scope {
        &self.data.scope
    }
    /// Session binding-state namespace.
    pub fn session_id(&self) -> &Id {
        &self.data.session_id
    }
    /// Full resolved profile metadata identity.
    pub fn profile_resolution_digest(&self) -> &JsonDigest {
        &self.data.profile_resolution_digest
    }
    /// Complete frozen assembly identity, excluding runtime instances.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(&self.data)
    }
    /// All profile connectors and Host connection revisions.
    pub fn connections(&self) -> &[ResolvedConnection] {
        &self.data.connections
    }
    /// Adapters in stable profile initialization order.
    pub fn adapters(&self) -> &[ResolvedAdapterBinding] {
        &self.data.adapters
    }
    /// Tools in profile selection order.
    pub fn tools(&self) -> &[ResolvedToolBinding] {
        &self.tools
    }
    /// Hook selections with their pinned contracts.
    pub fn hooks(&self) -> &[ResolvedHookBinding] {
        &self.data.hooks
    }
    /// Frozen system-key definitions, without runtime values.
    pub fn system_inputs(&self) -> &[SystemInputDefinition] {
        &self.data.system_inputs
    }
    /// Rebuild compiled validators only from the exact saved schemas and definitions.
    pub fn restore(
        json: &str,
        profile: &ResolvedProfile,
        registry: &SystemInputRegistry,
        expected_digest: &JsonDigest,
    ) -> Result<Self, ContractError> {
        let data: AssemblyData = serde_json::from_value(parse_json(json)?)
            .map_err(|_| component_error(ErrorCode::InvalidSnapshot, "assembly"))?;
        if &crate::serialization::data_digest(&data) != expected_digest {
            return Err(component_error(
                ErrorCode::InvalidSnapshot,
                "assembly.digest",
            ));
        }
        let tools = data
            .tools
            .iter()
            .map(|tool| {
                Ok(ResolvedToolBinding {
                    selection: tool.selection.clone(),
                    metadata: tool.metadata.clone(),
                    compiled: SchemaCompiler::new().restore(
                        &tool.compiled.to_string(),
                        registry,
                        &tool.compiled_digest,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, ContractError>>()?;
        let assembly = Self { data, tools };
        assembly.validate(profile, registry)?;
        Ok(assembly)
    }
    /// Cross-check profile selections, every metadata digest, config, connection,
    /// compiler and system-input contract. This method opens no component.
    pub fn validate(
        &self,
        profile: &ResolvedProfile,
        registry: &SystemInputRegistry,
    ) -> Result<(), ContractError> {
        let invalid = || component_error(ErrorCode::InvalidSnapshot, "assembly.contract");
        let selected = profile.profile();
        if self.data.schema_version != RESOLVED_ASSEMBLY_SCHEMA_VERSION {
            return Err(component_error(
                ErrorCode::UnsupportedSchemaVersion,
                "assembly.schema_version",
            ));
        }
        if self.scope() != profile.scope()
            || self.profile_resolution_digest() != profile.resolution_digest()
            || self.data.system_inputs
                != registry.definitions().values().cloned().collect::<Vec<_>>()
            || selected
                .context_sources
                .as_ref()
                .is_some_and(|sources| !sources.is_empty())
            || self.connections().len() != selected.connectors.len()
            || self.adapters().len() != selected.adapters.as_ref().map_or(0, Vec::len)
            || self.tools().len() != selected.tools.len()
            || self.hooks().len() != selected.hooks.as_ref().map_or(0, Vec::len)
        {
            return Err(invalid());
        }
        let mut connection_ids = std::collections::BTreeSet::new();
        for connection in self.connections() {
            if !selected.connectors.contains(&connection.binding)
                || !connection_ids.insert(&connection.binding.binding_id)
                || connection.metadata.reference
                    != (ComponentRef {
                        kind: ComponentKind::Connector,
                        id: connection.binding.connector_id.clone(),
                        version: Some(connection.binding.version.clone()),
                    })
            {
                return Err(invalid());
            }
            attest_metadata(profile, &connection.metadata)?;
        }
        for (adapter, selection) in self
            .adapters()
            .iter()
            .zip(selected.adapters.iter().flatten())
        {
            adapter.definition.validate()?;
            if &adapter.binding != selection
                || adapter.definition_digest != adapter.definition.digest()
                || adapter.definition.metadata.reference
                    != (ComponentRef {
                        kind: ComponentKind::Adapter,
                        id: selection.adapter_id.clone(),
                        version: Some(selection.version.clone()),
                    })
            {
                return Err(invalid());
            }
            attest_metadata(profile, &adapter.definition.metadata)?;
            validate_config(
                &adapter.definition.metadata.config_schema,
                selection.config.as_ref(),
            )?;
            if adapter.connections.len() != selection.connections.len()
                || !adapter
                    .definition
                    .metadata
                    .required_connections
                    .iter()
                    .all(|name| selection.connections.contains_key(name))
            {
                return Err(invalid());
            }
            for (name, binding_id) in &selection.connections {
                let expected = self
                    .connections()
                    .iter()
                    .find(|connection| &connection.binding.binding_id == binding_id)
                    .ok_or_else(invalid)?;
                if adapter.connections.get(name) != Some(expected) {
                    return Err(invalid());
                }
            }
            let wanted: Vec<_> = selected
                .tools
                .iter()
                .filter_map(|tool| {
                    if let ToolBindingRef::Export(export) = tool {
                        Some(export)
                    } else {
                        None
                    }
                })
                .chain(selected.hooks.iter().flatten().filter_map(|hook| {
                    if let HookRef::Export(export) = hook {
                        Some(export)
                    } else {
                        None
                    }
                }))
                .filter(|export| export.adapter_binding == selection.binding_id)
                .cloned()
                .collect();
            if adapter.selected_exports != wanted {
                return Err(invalid());
            }
            let mut selected_ids = std::collections::BTreeSet::new();
            if adapter
                .selected_exports
                .iter()
                .any(|export| !selected_ids.insert((&export.adapter_binding, &export.export_id)))
            {
                return Err(component_error(
                    ErrorCode::InvalidReference,
                    "assembly.duplicate_export",
                ));
            }
            if let Some(state) = &adapter.binding_state {
                if &state.scope != self.scope()
                    || &state.session_id != self.session_id()
                    || state.adapter_binding != selection.binding_id
                    || state.adapter
                        != (VersionedRef {
                            id: selection.adapter_id.clone(),
                            version: selection.version.clone(),
                        })
                    || state.definition_digest != adapter.definition_digest
                    || canonical_digest(&state.value) != state.state_ref.digest
                {
                    return Err(invalid());
                }
            }
        }
        let mut names = std::collections::BTreeSet::new();
        for (binding, selection) in self.tools().iter().zip(&selected.tools) {
            if &binding.selection != selection
                || !names.insert(binding.compiled.descriptor().name.clone())
            {
                return Err(invalid());
            }
            let original = match selection {
                ToolBindingRef::Catalog(reference) => {
                    let metadata = binding.metadata.as_ref().ok_or_else(invalid)?;
                    attest_metadata(profile, metadata)?;
                    if metadata.reference
                        != (ComponentRef {
                            kind: ComponentKind::Tool,
                            id: reference.tool_id.clone(),
                            version: Some(reference.version.clone()),
                        })
                        || metadata.model_name.as_ref() != Some(&binding.compiled.descriptor().name)
                        || binding.compiled.descriptor().tool
                            != (VersionedRef {
                                id: reference.tool_id.clone(),
                                version: reference.version.clone(),
                            })
                    {
                        return Err(invalid());
                    }
                    validate_config(&metadata.config_schema, reference.config.as_ref())?;
                    let mappings = reference.bindings.clone().unwrap_or_default();
                    if !metadata
                        .required_connections
                        .iter()
                        .all(|name| mappings.contains_key(name))
                        || mappings.values().any(|id| {
                            !self
                                .connections()
                                .iter()
                                .any(|connection| &connection.binding.binding_id == id)
                        })
                    {
                        return Err(invalid());
                    }
                    binding.compiled.descriptor().clone()
                }
                ToolBindingRef::Export(export) => {
                    if binding.metadata.is_some() {
                        return Err(invalid());
                    }
                    let definition = self.export(export)?;
                    let AdapterExportDefinition::Tool { descriptor, .. } = definition else {
                        return Err(invalid());
                    };
                    let mut descriptor = descriptor.as_ref().clone();
                    if let Some(alias) = &export.alias {
                        descriptor.name = alias.clone();
                    }
                    descriptor
                }
            };
            if SchemaCompiler::new().compile(original, registry)?.digest()
                != binding.compiled.digest()
            {
                return Err(invalid());
            }
        }
        for (binding, selection) in self.hooks().iter().zip(selected.hooks.iter().flatten()) {
            if &binding.selection != selection {
                return Err(invalid());
            }
            binding.definition.validate()?;
            match selection {
                HookRef::Catalog(reference) => {
                    let metadata = binding.metadata.as_ref().ok_or_else(invalid)?;
                    attest_metadata(profile, metadata)?;
                    if metadata.reference
                        != (ComponentRef {
                            kind: ComponentKind::Hook,
                            id: reference.hook_id.clone(),
                            version: Some(reference.version.clone()),
                        })
                        || metadata.hook_position != Some(reference.position)
                        || binding.definition.hook
                            != (VersionedRef {
                                id: reference.hook_id.clone(),
                                version: reference.version.clone(),
                            })
                        || binding.definition.position != reference.position
                    {
                        return Err(invalid());
                    }
                }
                HookRef::Export(export) => {
                    if export.alias.is_some() || binding.metadata.is_some() {
                        return Err(invalid());
                    }
                    let AdapterExportDefinition::Hook { definition, .. } = self.export(export)?
                    else {
                        return Err(invalid());
                    };
                    if definition != &binding.definition {
                        return Err(invalid());
                    }
                }
            }
        }
        Ok(())
    }
    /// Inspect the pinned original definition behind an Export selection.
    pub fn export(&self, selection: &ExportRef) -> Result<&AdapterExportDefinition, ContractError> {
        self.adapters()
            .iter()
            .find(|adapter| adapter.binding.binding_id == selection.adapter_binding)
            .and_then(|adapter| {
                adapter
                    .definition
                    .exports
                    .iter()
                    .find(|export| export.metadata().export_id == selection.export_id)
            })
            .ok_or_else(|| component_error(ErrorCode::InvalidReference, "assembly.export"))
    }
}
impl AdapterDefinition {
    /// Full metadata and export descriptor identity.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
    /// Validate exact metadata kinds, positions and original names.
    pub fn validate(&self) -> Result<(), ContractError> {
        let invalid = || component_error(ErrorCode::InvalidContract, "adapter.definition");
        if self.metadata.reference.kind != ComponentKind::Adapter
            || self.metadata.reference.version.is_none()
            || self.metadata.contract_version != 1
            || self.exports.len() != self.metadata.exports.len()
        {
            return Err(invalid());
        }
        crate::tool_schema::compile_validator(&self.metadata.config_schema)?;
        let mut ids = std::collections::BTreeSet::new();
        for export in &self.exports {
            let metadata = export.metadata();
            if !ids.insert(&metadata.export_id)
                || metadata.contract_version != 1
                || !self.metadata.exports.contains(metadata)
            {
                return Err(invalid());
            }
            let valid = match export {
                AdapterExportDefinition::Tool { descriptor, .. } => {
                    metadata.kind == ExportKind::Tool
                        && metadata.model_name.as_ref() == Some(&descriptor.name)
                        && metadata.hook_position.is_none()
                }
                AdapterExportDefinition::Hook { definition, .. } => {
                    definition.validate()?;
                    metadata.kind == ExportKind::Hook
                        && metadata.hook_position == Some(definition.position)
                        && metadata.model_name.is_none()
                }
                AdapterExportDefinition::ContextSource { .. } => {
                    metadata.kind == ExportKind::ContextSource
                        && metadata.model_name.is_none()
                        && metadata.hook_position.is_none()
                }
                AdapterExportDefinition::EventConsumer { .. } => {
                    metadata.kind == ExportKind::EventConsumer
                        && metadata.model_name.is_none()
                        && metadata.hook_position.is_none()
                }
            };
            if !valid {
                return Err(invalid());
            }
        }
        Ok(())
    }
}
fn attest_metadata(
    profile: &ResolvedProfile,
    metadata: &ComponentMetadata,
) -> Result<(), ContractError> {
    if metadata.contract_version != 1
        || !profile.components().iter().any(|component| {
            component.reference == metadata.reference
                && component.definition_digest == crate::serialization::data_digest(metadata)
        })
    {
        return Err(component_error(
            ErrorCode::ProfileMismatch,
            "assembly.metadata",
        ));
    }
    Ok(())
}
fn validate_config(schema: &Value, config: Option<&JsonObject>) -> Result<(), ContractError> {
    let value = serde_json::to_value(config.cloned().unwrap_or_default())
        .map_err(|_| component_error(ErrorCode::InvalidJson, "assembly.config"))?;
    if !crate::tool_schema::compile_validator(schema)?.is_valid(&value) {
        return Err(component_error(
            ErrorCode::InvalidConfiguration,
            "assembly.config",
        ));
    }
    Ok(())
}
impl Serialize for ResolvedAssembly {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.data.serialize(serializer)
    }
}
impl fmt::Debug for ResolvedAssembly {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedAssembly")
            .field("adapter_count", &self.data.adapters.len())
            .field("tool_count", &self.tools.len())
            .finish_non_exhaustive()
    }
}
fn component_error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
```

## `crates/wickle/src/hooks.rs`

```rust
//! Bounded lifecycle transformations and observations. Hooks receive selected
//! data, never a mutable Run, credentials, or the system-input map.

use crate::*;
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};
use tokio_util::sync::CancellationToken;

mod records;
mod runtime;
pub(crate) use records::validate_application_chain;

/// Exact callback contract pinned for one Run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookDefinition {
    /// Registered identity and exact version.
    pub hook: VersionedRef,
    /// Single permitted lifecycle position.
    pub position: HookPosition,
    /// Lower priorities execute first; IDs break ties.
    pub priority: i32,
    /// Only optional before_run callback failures may continue as warnings.
    pub required: bool,
    /// Positive finite callback timeout, at most one day.
    pub timeout_ms: u64,
    /// Positive byte bound on serialized callback output.
    pub max_output_bytes: usize,
}
impl HookDefinition {
    /// Identity of the complete callback contract, including its bounds.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
    /// Reject unbounded callback contracts before admission.
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.timeout_ms == 0
            || self.timeout_ms > 86_400_000
            || self.max_output_bytes == 0
            || self.max_output_bytes > 16_777_216
        {
            return Err(hook_error(ErrorCode::InvalidContract, "hooks.definition"));
        }
        Ok(())
    }
}

/// A stable logical lifecycle target, independent of physical retry attempts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HookTarget {
    /// Initial admitted Run preparation.
    BeforeRun,
    /// One logical step, reused across transport retry and route fallback.
    BeforeModel {
        /// Logical step identity.
        model_step_id: Id,
    },
    /// One original planned call, before system binding.
    BeforeTool {
        /// Core logical call identity.
        call_id: Id,
    },
    /// Already committed tool observation.
    AfterTool {
        /// Core logical call identity.
        call_id: Id,
        /// Exact protected ToolResult.
        result_ref: RecordRef,
    },
    /// Already committed terminal outcome.
    AfterRun {
        /// Exact protected RunOutcome.
        outcome_ref: RecordRef,
        /// Terminal snapshot revision.
        revision: u64,
    },
}
impl HookTarget {
    /// Lifecycle position fixed by this target.
    pub fn position(&self) -> HookPosition {
        match self {
            Self::BeforeRun => HookPosition::BeforeRun,
            Self::BeforeModel { .. } => HookPosition::BeforeModel,
            Self::BeforeTool { .. } => HookPosition::BeforeTool,
            Self::AfterTool { .. } => HookPosition::AfterTool,
            Self::AfterRun { .. } => HookPosition::AfterRun,
        }
    }
}

/// Selected safe data. Raw tool receipts, opaque continuations and hidden inputs
/// are deliberately absent. Data payloads accept only text and JSON content.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HookInput {
    /// Original user request and prior additions in this transform chain.
    BeforeRun {
        /// Original user content.
        user_input: Vec<InputContent>,
        /// Accumulated safe context.
        context_items: Vec<ContextItem>,
    },
    /// Route-independent data for a logical model step.
    BeforeModel {
        /// Original user content.
        user_input: Vec<InputContent>,
        /// Already supplied context and chain additions.
        context_items: Vec<ContextItem>,
    },
    /// Model-visible contract and original/effective argument maps.
    BeforeTool {
        /// Model-facing descriptor only.
        tool: ModelTool,
        /// Pinned full-descriptor identity, without its hidden schema.
        descriptor_digest: JsonDigest,
        /// Pinned compiler contract identity.
        compiled_digest: JsonDigest,
        /// Unchanged original model proposal.
        original_model_inputs: JsonObject,
        /// Current transform-chain value.
        model_inputs: JsonObject,
    },
    /// Safe committed observation, excluding receipt and diagnostic references.
    AfterTool {
        /// Original logical call identity.
        call_id: Id,
        /// Committed completion status.
        status: ToolResultStatus,
        /// Committed effect status.
        effect: ToolEffect,
        /// Safe text/JSON output only.
        content: Vec<InputContent>,
        /// Safe classified failure, if any.
        error_code: Option<Id>,
    },
    /// Safe terminal summary, excluding artifact and protected-record references.
    AfterRun {
        /// Terminal status.
        status: RunStatus,
        /// Safe text/JSON response only.
        output: Vec<InputContent>,
        /// Charged execution usage.
        usage: BudgetUsage,
    },
}
impl fmt::Debug for HookInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HookInput(<protected>)")
    }
}
impl HookInput {
    /// Derive a reference-free summary from a committed tool result.
    pub fn tool_observed(call_id: &Id, result: &ToolResult) -> Self {
        Self::AfterTool {
            call_id: call_id.clone(),
            status: result.status,
            effect: result.effect,
            content: safe_summary(&result.content),
            error_code: result.error.as_ref().map(|error| error.code.clone()),
        }
    }
    /// Derive a reference-free summary from an authoritative terminal outcome.
    pub fn run_observed(outcome: &RunOutcome) -> Self {
        Self::AfterRun {
            status: outcome.result.status(),
            output: safe_summary(&outcome.output),
            usage: outcome.usage.clone(),
        }
    }
    /// Digest of the exact safe callback input.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
    fn validate(&self, target: &HookTarget) -> Result<(), ContractError> {
        let valid = match (self, target) {
            (
                Self::BeforeRun {
                    user_input,
                    context_items,
                },
                HookTarget::BeforeRun,
            )
            | (
                Self::BeforeModel {
                    user_input,
                    context_items,
                },
                HookTarget::BeforeModel { .. },
            ) => {
                safe_content(user_input)
                    && context_items.iter().all(|item| safe_content(&item.content))
            }
            (
                Self::BeforeTool {
                    tool,
                    original_model_inputs,
                    model_inputs,
                    ..
                },
                HookTarget::BeforeTool { .. },
            ) => {
                let validator = crate::tool_schema::compile_validator(&tool.model_input_schema)?;
                validator.is_valid(
                    &serde_json::to_value(original_model_inputs)
                        .map_err(|_| hook_error(ErrorCode::InvalidJson, "hooks.input"))?,
                ) && validator.is_valid(
                    &serde_json::to_value(model_inputs)
                        .map_err(|_| hook_error(ErrorCode::InvalidJson, "hooks.input"))?,
                )
            }
            (
                Self::AfterTool {
                    call_id, content, ..
                },
                HookTarget::AfterTool {
                    call_id: target, ..
                },
            ) => call_id == target && safe_content(content),
            (Self::AfterRun { status, output, .. }, HookTarget::AfterRun { .. }) => {
                status.is_terminal() && safe_content(output)
            }
            _ => false,
        };
        if !valid {
            return Err(hook_error(ErrorCode::InvalidContract, "hooks.input"));
        }
        Ok(())
    }
}
fn safe_content(content: &[InputContent]) -> bool {
    content.iter().all(|content| {
        matches!(
            content,
            InputContent::Text { .. } | InputContent::Json { .. }
        )
    })
}
fn safe_summary(content: &[InputContent]) -> Vec<InputContent> {
    content
        .iter()
        .filter(|item| matches!(item, InputContent::Text { .. } | InputContent::Json { .. }))
        .cloned()
        .collect()
}

/// Additional data; the core assigns provenance, scope, ID and lifetime.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookContextAddition {
    /// Text/JSON data, never raw message or opaque blocks.
    pub content: Vec<InputContent>,
    /// Required data must fit in full; importance grants no authority.
    pub priority: ContextPriority,
}
/// Position-specific output, never an internal-state patch or a next callback.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HookOutput {
    /// Append data during before_run or before_model.
    Context {
        /// Complete additions to validate and persist.
        additions: Vec<HookContextAddition>,
    },
    /// Replace only model-owned arguments, optionally denying this call.
    Tool {
        /// Must still satisfy the model-visible input schema.
        model_inputs: JsonObject,
        /// Safe denial code; absence grants no permission.
        deny: Option<Id>,
    },
    /// Observer completed; cannot change a tool or Run result.
    Observed {},
}
impl fmt::Debug for HookOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HookOutput(<protected>)")
    }
}

/// Current authorization and finite callback controls, without system inputs.
#[derive(Debug, Clone)]
pub struct HookContext {
    /// Original adapter export authority; absent for direct catalog hooks.
    pub selection: Option<HookRef>,
    /// Host runtime segment; absent for directly injected catalog hooks.
    pub binding_set_id: Option<Id>,
    /// Exact namespace.
    pub scope: Scope,
    /// Owning Run identity.
    pub run_id: Id,
    /// Exact selected callback identity.
    pub hook: VersionedRef,
    /// Logical lifecycle target.
    pub target: HookTarget,
    /// Current authenticated actor.
    pub principal_ref: Id,
    /// Current Host grant.
    pub capability_grant_ref: Id,
    /// Cancelled on timeout, cancellation or callback completion.
    pub cancellation: CancellationToken,
    /// Finite callback deadline.
    pub deadline: tokio::time::Instant,
}
/// Trusted Host callback. It must not hide required business writes or dispatch
/// another execution; in-process Rust code is not an isolation sandbox.
pub trait HookHandler: Send + Sync {
    /// Apply one bounded transformation or read-only observation.
    fn call<'a>(
        &'a self,
        input: &'a HookInput,
        context: &'a HookContext,
    ) -> PortFuture<'a, HookOutput>;
}
/// Exact contract associated with an existing callback.
#[derive(Clone)]
pub struct HookRegistration {
    /// Immutable selected contract.
    pub definition: HookDefinition,
    /// Existing Host-owned implementation.
    pub handler: Arc<dyn HookHandler>,
}
/// Scope-bound immutable callback catalog.
pub struct HookRegistry {
    scope: Scope,
    entries: Vec<HookRegistration>,
    selections: Vec<HookRef>,
}
impl HookRegistry {
    /// Register direct catalog callbacks without invoking them.
    pub fn new(scope: Scope, entries: Vec<HookRegistration>) -> Result<Self, ContractError> {
        Self::from_bindings(
            scope,
            entries
                .into_iter()
                .map(|entry| (catalog_selection(&entry.definition), entry))
                .collect(),
        )
    }
    /// Preserve real catalog/export selections, including distinct bindings of
    /// the same native Hook definition. Only selected handlers can be called.
    pub fn from_bindings(
        scope: Scope,
        mut bindings: Vec<(HookRef, HookRegistration)>,
    ) -> Result<Self, ContractError> {
        if bindings.len() > 64 {
            return Err(hook_error(ErrorCode::InvalidContract, "hooks.count"));
        }
        let mut seen = std::collections::BTreeSet::new();
        for (selection, entry) in &bindings {
            entry.definition.validate()?;
            let valid = match selection {
                HookRef::Catalog(_) => selection == &catalog_selection(&entry.definition),
                HookRef::Export(export) => export.alias.is_none(),
            };
            if !valid || !seen.insert(crate::serialization::data_digest(selection)) {
                return Err(hook_error(ErrorCode::InvalidReference, "hooks.duplicate"));
            }
        }
        bindings.sort_by(|(a, ea), (b, eb)| {
            ea.definition
                .priority
                .cmp(&eb.definition.priority)
                .then(ea.definition.hook.id.cmp(&eb.definition.hook.id))
                .then(
                    crate::serialization::data_digest(a)
                        .as_str()
                        .cmp(crate::serialization::data_digest(b).as_str()),
                )
        });
        let (selections, entries) = bindings.into_iter().unzip();
        Ok(Self {
            scope,
            entries,
            selections,
        })
    }
    /// Metadata-only registry for admission and non-executing settlement paths.
    pub fn metadata(
        scope: Scope,
        bindings: Vec<ResolvedHookBinding>,
    ) -> Result<Self, ContractError> {
        Self::from_bindings(
            scope,
            bindings
                .into_iter()
                .map(|binding| {
                    (
                        binding.selection,
                        HookRegistration {
                            definition: binding.definition,
                            handler: Arc::new(MetadataHook),
                        },
                    )
                })
                .collect(),
        )
    }
    /// Exact registered namespace.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Pin exact profile selections in priority/identity order.
    pub fn plan(&self, profile: &AgentProfile) -> Result<HookPlan, ContractError> {
        let mut definitions = Vec::new();
        let mut selections = Vec::new();
        let wanted = profile.hooks.as_deref().unwrap_or_default();
        for (selection, entry) in self.selections.iter().zip(&self.entries) {
            if wanted.contains(selection) {
                definitions.push(entry.definition.clone());
                selections.push(export_selection(selection));
            }
        }
        if selections.iter().all(Option::is_none) {
            selections.clear();
        }
        let plan = HookPlan {
            scope: self.scope.clone(),
            definitions,
            selections,
        };
        plan.validate(profile)?;
        Ok(plan)
    }
    fn get(&self, hook: &VersionedRef, selection: Option<&HookRef>) -> Option<&HookRegistration> {
        self.entries
            .iter()
            .zip(&self.selections)
            .find(|(entry, registered)| {
                &entry.definition.hook == hook && export_selection(registered).as_ref() == selection
            })
            .map(|(entry, _)| entry)
    }
}
struct MetadataHook;
impl HookHandler for MetadataHook {
    fn call<'a>(&'a self, _: &'a HookInput, _: &'a HookContext) -> PortFuture<'a, HookOutput> {
        Box::pin(async {
            Err(hook_error(
                ErrorCode::ComponentUnavailable,
                "hooks.metadata_only",
            ))
        })
    }
}
fn catalog_selection(definition: &HookDefinition) -> HookRef {
    HookRef::Catalog(CatalogHookRef {
        hook_id: definition.hook.id.clone(),
        version: definition.hook.version.clone(),
        position: definition.position,
    })
}
fn export_selection(selection: &HookRef) -> Option<HookRef> {
    if matches!(selection, HookRef::Export(_)) {
        Some(selection.clone())
    } else {
        None
    }
}

/// Immutable selected definitions and optional real export identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookPlan {
    scope: Scope,
    definitions: Vec<HookDefinition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    selections: Vec<Option<HookRef>>,
}
impl HookPlan {
    /// Exact owning namespace.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Selected definitions in stable execution order.
    pub fn definitions(&self) -> &[HookDefinition] {
        &self.definitions
    }
    /// Export authority for the definition at this index; direct catalog is None.
    pub fn selection(&self, index: usize) -> Option<&HookRef> {
        self.selections.get(index).and_then(Option::as_ref)
    }
    /// Exact definition behind a native Hook ID and real export selection.
    pub fn definition_for(
        &self,
        hook: &VersionedRef,
        selection: Option<&HookRef>,
    ) -> Option<&HookDefinition> {
        self.definitions
            .iter()
            .enumerate()
            .find(|(index, definition)| {
                &definition.hook == hook && self.selection(*index) == selection
            })
            .map(|(_, definition)| definition)
    }
    /// Complete scope/definitions/order/selection identity.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
    /// Restore only the trusted protected plan identity.
    pub fn restore(
        json: &str,
        scope: &Scope,
        expected_digest: &JsonDigest,
    ) -> Result<Self, ContractError> {
        let plan: Self = serde_json::from_value(parse_json(json)?)
            .map_err(|_| hook_error(ErrorCode::InvalidSnapshot, "hooks.plan"))?;
        if plan.scope() != scope || &plan.digest() != expected_digest {
            return Err(hook_error(
                ErrorCode::InvalidSnapshot,
                "hooks.plan_identity",
            ));
        }
        plan.validate_order()?;
        Ok(plan)
    }
    /// Match the original profile selections without rewriting export identities.
    pub fn validate(&self, profile: &AgentProfile) -> Result<(), ContractError> {
        self.validate_order()?;
        let selected = profile.hooks.as_deref().unwrap_or_default();
        let actual: Vec<_> = self
            .definitions
            .iter()
            .enumerate()
            .map(|(index, definition)| {
                self.selection(index)
                    .cloned()
                    .unwrap_or_else(|| catalog_selection(definition))
            })
            .collect();
        if selected.len() != actual.len()
            || selected
                .iter()
                .any(|selection| actual.iter().filter(|item| *item == selection).count() != 1)
        {
            return Err(hook_error(
                ErrorCode::InvalidSnapshot,
                "hooks.plan_selection",
            ));
        }
        Ok(())
    }
    fn validate_order(&self) -> Result<(), ContractError> {
        if self.definitions.len() > 64
            || (!self.selections.is_empty() && self.selections.len() != self.definitions.len())
        {
            return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.plan_count"));
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut prior = None;
        for (index, definition) in self.definitions.iter().enumerate() {
            definition.validate()?;
            let selection = self
                .selection(index)
                .cloned()
                .unwrap_or_else(|| catalog_selection(definition));
            if self.selection(index).is_some_and(
                |selection| !matches!(selection,HookRef::Export(export) if export.alias.is_none()),
            ) {
                return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.plan_export"));
            }
            let digest = crate::serialization::data_digest(&selection);
            let key = (
                definition.priority,
                definition.hook.id.clone(),
                digest.as_str().to_owned(),
            );
            if !seen.insert(digest) || prior.as_ref().is_some_and(|previous| previous > &key) {
                return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.plan_order"));
            }
            prior = Some(key);
        }
        Ok(())
    }
}

/// One durably applied transform. The result record retains input and output so
/// restart never substitutes freshly transformed arguments for the saved ones.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookApplication {
    /// Original adapter export authority; absent for direct catalog hooks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<HookRef>,
    /// Exact selected hook.
    pub hook: VersionedRef,
    /// Complete pinned definition identity.
    pub definition_digest: JsonDigest,
    /// Logical lifecycle target.
    pub target: HookTarget,
    /// Digest of the safe input seen by this callback.
    pub input_digest: JsonDigest,
    /// Exact stored transformation or classified optional failure.
    pub result_ref: RecordRef,
}
/// Protected body referenced by HookApplication.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookApplicationRecord {
    /// Original adapter export authority; absent for direct catalog hooks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<HookRef>,
    /// Exact namespace.
    pub scope: Scope,
    /// Owning Run.
    pub run_id: Id,
    /// Exact selected hook.
    pub hook: VersionedRef,
    /// Definition identity.
    pub definition_digest: JsonDigest,
    /// Logical target.
    pub target: HookTarget,
    /// Input before this callback's transformation.
    pub input: HookInput,
    /// Validated output; absent only for a permitted optional callback failure.
    pub output: Option<HookOutput>,
    /// Core-stamped data created from Context additions.
    pub context_items: Vec<ContextItem>,
    /// Safe classified optional failure.
    pub failure: Option<Id>,
}
impl fmt::Debug for HookApplicationRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HookApplicationRecord(<protected>)")
    }
}
/// Current chain result, including its stored application prefix.
#[derive(Clone)]
pub struct HookTransform {
    /// Accumulated supplied context and newly added data.
    pub context_items: Vec<ContextItem>,
    /// Final model-owned arguments, only for before_tool.
    pub model_inputs: Option<JsonObject>,
    /// A persisted denial, never overridden by another Hook.
    pub deny: Option<Id>,
    /// Applied definitions in deterministic chain order.
    pub applications: Vec<HookApplication>,
}
impl fmt::Debug for HookTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HookTransform")
            .field("applications", &self.applications.len())
            .field("denied", &self.deny.is_some())
            .finish_non_exhaustive()
    }
}
/// Observer result that cannot alter an already committed outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum HookObservationStatus {
    /// Read-only callback completed.
    Completed,
    /// Callback or output contract failed safely.
    Failed {
        /// Safe error code, never raw callback text.
        code: Id,
    },
}
/// Immutable observation report stored outside the Run snapshot/event endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookObservation {
    /// Original adapter export authority; absent for direct catalog hooks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<HookRef>,
    /// Exact namespace.
    pub scope: Scope,
    /// Owning Run.
    pub run_id: Id,
    /// Exact selected observer.
    pub hook: VersionedRef,
    /// Complete contract identity.
    pub definition_digest: JsonDigest,
    /// Exact committed ToolResult or terminal outcome.
    pub target: HookTarget,
    /// Digest of the reference-free observer input.
    pub input_digest: JsonDigest,
    /// Classified callback result.
    pub status: HookObservationStatus,
    /// Report time; it does not advance Run time or revision.
    pub timestamp_ms: i64,
}

/// Executes selected callbacks, with authority and persistence owned by the core.
pub struct HookRuntime {
    binding_set_id: Option<Id>,
    store: Arc<dyn StateStore>,
    policy: Arc<PolicyGate>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
    registry: Arc<HookRegistry>,
}
fn hook_error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
```

## `crates/wickle/src/hooks/records.rs`

```rust
use super::*;

impl HookApplicationRecord {
    /// Restore one exact protected application; this does not grant permission
    /// to invoke a Hook or substitute for chain and original-call validation.
    pub fn restore(
        record: &ProtectedRecord,
        plan: &HookPlan,
        application: &HookApplication,
        scope: &Scope,
        run_id: &Id,
    ) -> Result<Self, ContractError> {
        let invalid = || hook_error(ErrorCode::InvalidSnapshot, "hooks.application");
        let value: Self = serde_json::from_value(record.value().clone()).map_err(|_| invalid())?;
        if record.reference() != &application.result_ref
            || plan.scope() != scope
            || &value.scope != scope
            || &value.run_id != run_id
            || value.hook != application.hook
            || value.selection != application.selection
            || value.definition_digest != application.definition_digest
            || value.target != application.target
            || value.input.digest() != application.input_digest
            || crate::serialization::data_digest(&value) != record.reference().digest
        {
            return Err(invalid());
        }
        let definition = plan
            .definition_for(&value.hook, value.selection.as_ref())
            .filter(|definition| {
                definition.digest() == value.definition_digest
                    && definition.position == value.target.position()
            })
            .ok_or_else(invalid)?;
        value.input.validate(&value.target)?;
        match (&value.output, &value.failure) {
            (Some(output), None) => {
                validate_output(definition, &value.input, output)?;
                match output {
                    HookOutput::Context { additions } => {
                        if additions.len() != value.context_items.len() {
                            return Err(invalid());
                        }
                        for (addition, item) in additions.iter().zip(&value.context_items) {
                            let expected = stamped(
                                item.item_id.clone(),
                                scope,
                                run_id,
                                definition,
                                &value.target,
                                addition,
                            )?;
                            if item != &expected {
                                return Err(invalid());
                            }
                        }
                    }
                    _ if !value.context_items.is_empty() => return Err(invalid()),
                    _ => {}
                }
            }
            (None, Some(_))
                if definition.position == HookPosition::BeforeRun
                    && !definition.required
                    && value.context_items.is_empty() => {}
            _ => return Err(invalid()),
        }
        Ok(value)
    }
}

pub(super) fn validate_output(
    definition: &HookDefinition,
    input: &HookInput,
    output: &HookOutput,
) -> Result<(), ContractError> {
    let invalid = || hook_error(ErrorCode::InvalidContract, "hooks.output");
    if serde_json::to_vec(output).map_err(|_| invalid())?.len() > definition.max_output_bytes {
        return Err(invalid());
    }
    let valid = match (definition.position, input, output) {
        (
            HookPosition::BeforeRun,
            HookInput::BeforeRun { .. },
            HookOutput::Context { additions },
        )
        | (
            HookPosition::BeforeModel,
            HookInput::BeforeModel { .. },
            HookOutput::Context { additions },
        ) => additions
            .iter()
            .all(|addition| safe_content(&addition.content)),
        (
            HookPosition::BeforeTool,
            HookInput::BeforeTool { tool, .. },
            HookOutput::Tool { model_inputs, .. },
        ) => crate::tool_schema::compile_validator(&tool.model_input_schema)?
            .is_valid(&serde_json::to_value(model_inputs).map_err(|_| invalid())?),
        (HookPosition::AfterTool, HookInput::AfterTool { .. }, HookOutput::Observed {})
        | (HookPosition::AfterRun, HookInput::AfterRun { .. }, HookOutput::Observed {}) => true,
        _ => false,
    };
    if !valid {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn stamped(
    id: Id,
    scope: &Scope,
    run_id: &Id,
    definition: &HookDefinition,
    target: &HookTarget,
    addition: &HookContextAddition,
) -> Result<ContextItem, ContractError> {
    let lifetime = match target {
        HookTarget::BeforeRun => ContextLifetime::Run {
            run_id: run_id.clone(),
        },
        HookTarget::BeforeModel { model_step_id } => ContextLifetime::Step {
            run_id: run_id.clone(),
            model_step_id: model_step_id.clone(),
        },
        _ => {
            return Err(hook_error(
                ErrorCode::InvalidContract,
                "hooks.context_target",
            ));
        }
    };
    Ok(ContextItem::new(
        id,
        ContextOrigin::Hook,
        definition.hook.clone(),
        scope.clone(),
        addition.content.clone(),
        lifetime,
        addition.priority,
    ))
}

pub(super) fn apply(
    input: &mut HookInput,
    record: &HookApplicationRecord,
) -> Result<Option<Id>, ContractError> {
    if &record.input != input {
        return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.chain_input"));
    }
    match (&record.output, input) {
        (
            Some(HookOutput::Context { .. }),
            HookInput::BeforeRun { context_items, .. }
            | HookInput::BeforeModel { context_items, .. },
        ) => context_items.extend(record.context_items.clone()),
        (
            Some(HookOutput::Tool { model_inputs, deny }),
            HookInput::BeforeTool {
                model_inputs: current,
                ..
            },
        ) => {
            *current = model_inputs.clone();
            return Ok(deny.clone());
        }
        (None, _) if record.failure.is_some() => {}
        _ => return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.chain_output")),
    }
    Ok(None)
}

pub(super) fn transformed(
    input: HookInput,
    deny: Option<Id>,
    applications: Vec<HookApplication>,
) -> HookTransform {
    match input {
        HookInput::BeforeRun { context_items, .. }
        | HookInput::BeforeModel { context_items, .. } => HookTransform {
            context_items,
            model_inputs: None,
            deny,
            applications,
        },
        HookInput::BeforeTool { model_inputs, .. } => HookTransform {
            context_items: vec![],
            model_inputs: Some(model_inputs),
            deny,
            applications,
        },
        _ => unreachable!("validated transformation input"),
    }
}

pub(crate) fn validate_application_chain(
    plan: &HookPlan,
    snapshot: &RunSnapshot,
    records: &[ProtectedRecord],
) -> Result<(), ContractError> {
    plan.validate(snapshot.profile.profile())?;
    if plan.scope() != &snapshot.scope {
        return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.scope"));
    }
    let mut run_context = Vec::new();
    for application in snapshot
        .hook_applications
        .iter()
        .filter(|application| application.target == HookTarget::BeforeRun)
    {
        let record = records
            .iter()
            .find(|record| record.reference() == &application.result_ref)
            .ok_or_else(|| hook_error(ErrorCode::InvalidSnapshot, "hooks.run_context"))?;
        let value = HookApplicationRecord::restore(
            record,
            plan,
            application,
            &snapshot.scope,
            &snapshot.run_id,
        )?;
        run_context.extend(value.context_items);
    }
    let mut targets: Vec<HookTarget> = vec![];
    let mut ids = std::collections::BTreeSet::new();
    for application in &snapshot.hook_applications {
        if !ids.insert(application.result_ref.record_id.clone()) {
            return Err(hook_error(
                ErrorCode::InvalidSnapshot,
                "hooks.duplicate_application",
            ));
        }
        if !targets.contains(&application.target) {
            targets.push(application.target.clone());
        }
    }
    for target in targets {
        let definitions: Vec<_> = plan
            .definitions()
            .iter()
            .enumerate()
            .filter(|(_, definition)| definition.position == target.position())
            .map(|(index, definition)| (definition, plan.selection(index)))
            .collect();
        let applications: Vec<_> = snapshot
            .hook_applications
            .iter()
            .filter(|application| application.target == target)
            .collect();
        if applications.len() > definitions.len() {
            return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.chain_length"));
        }
        let mut current = None;
        let mut denied = false;
        for (application, (definition, selection)) in applications.into_iter().zip(definitions) {
            if denied
                || application.hook != definition.hook
                || application.selection.as_ref() != selection
                || application.definition_digest != definition.digest()
            {
                return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.chain_order"));
            }
            let record = records
                .iter()
                .find(|record| record.reference() == &application.result_ref)
                .ok_or_else(|| hook_error(ErrorCode::InvalidSnapshot, "hooks.record_missing"))?;
            let record = HookApplicationRecord::restore(
                record,
                plan,
                application,
                &snapshot.scope,
                &snapshot.run_id,
            )?;
            if current.is_none() {
                match (&target, &record.input) {
                    (
                        HookTarget::BeforeRun,
                        HookInput::BeforeRun {
                            user_input,
                            context_items,
                        },
                    ) if user_input == &snapshot.request.input && context_items.is_empty() => {}
                    (
                        HookTarget::BeforeModel { model_step_id },
                        HookInput::BeforeModel {
                            user_input,
                            context_items,
                        },
                    ) if user_input == &snapshot.request.input
                        && context_items == &run_context
                        && (snapshot.model_step_id.as_ref() == Some(model_step_id)
                            || snapshot
                                .model_ledger
                                .iter()
                                .any(|invocation| &invocation.model_step_id == model_step_id)) => {}
                    (
                        HookTarget::BeforeTool { call_id },
                        HookInput::BeforeTool {
                            tool,
                            descriptor_digest,
                            original_model_inputs,
                            model_inputs,
                            ..
                        },
                    ) => {
                        let call = snapshot
                            .tool_ledger
                            .iter()
                            .find(|entry| &entry.call.call_id == call_id)
                            .ok_or_else(|| hook_error(ErrorCode::InvalidSnapshot, "hooks.call"))?;
                        if call.call.tool_name != tool.name
                            || call.call.descriptor_digest.as_ref() != Some(descriptor_digest)
                            || &call.call.model_inputs != original_model_inputs
                            || model_inputs != original_model_inputs
                        {
                            return Err(hook_error(
                                ErrorCode::InvalidSnapshot,
                                "hooks.original_inputs",
                            ));
                        }
                    }
                    _ => return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.target_input")),
                }
                current = Some(record.input.clone());
            }
            denied = apply(current.as_mut().expect("initialized"), &record)?.is_some();
        }
    }
    Ok(())
}
```

## `crates/wickle/src/hooks/runtime.rs`

```rust
use super::*;
use futures_util::FutureExt;
use std::{future::Future, panic::AssertUnwindSafe, time::Duration};

impl HookRuntime {
    /// Inject existing components. Transform persistence always uses the RunBudget's
    /// authoritative store; the injected store is used after the Run has ended.
    pub fn new(
        store: Arc<dyn StateStore>,
        policy: Arc<PolicyGate>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdSource>,
        registry: Arc<HookRegistry>,
    ) -> Self {
        Self {
            binding_set_id: None,
            store,
            policy,
            clock,
            ids,
            registry,
        }
    }
    /// Forward the current Host segment identity to scoped export wrappers.
    pub fn with_binding_set_id(mut self, binding_set_id: Id) -> Self {
        self.binding_set_id = Some(binding_set_id);
        self
    }
    /// Scope under which callbacks are registered.
    pub fn scope(&self) -> &Scope {
        self.registry.scope()
    }
    /// Pure construction of the exact selected plan.
    pub fn plan(&self, profile: &AgentProfile) -> Result<HookPlan, ContractError> {
        self.registry.plan(profile)
    }

    async fn pinned_plan(
        &self,
        snapshot: &RunSnapshot,
        store: &dyn StateStore,
    ) -> Result<HookPlan, ContractError> {
        if &snapshot.scope != self.scope() {
            return Err(hook_error(ErrorCode::AccessDenied, "hooks.scope"));
        }
        let expected = self.plan(snapshot.profile.profile())?;
        let reference = snapshot
            .hook_plan_ref
            .as_ref()
            .ok_or_else(|| hook_error(ErrorCode::InvalidSnapshot, "hooks.plan_ref"))?;
        let record = store.read_record(&snapshot.scope, reference).await?;
        if record.reference() != reference {
            return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.plan_record"));
        }
        let plan = HookPlan::restore(
            &serde_json::to_string(record.value())
                .map_err(|_| hook_error(ErrorCode::InvalidJson, "hooks.plan"))?,
            &snapshot.scope,
            &reference.digest,
        )?;
        if plan != expected {
            return Err(hook_error(ErrorCode::ProfileMismatch, "hooks.plan"));
        }
        Ok(plan)
    }

    /// Read and fold only an already persisted prefix; this never invokes a Hook.
    pub async fn saved_transform(
        &self,
        snapshot: &RunSnapshot,
        target: &HookTarget,
    ) -> Result<Option<HookTransform>, ContractError> {
        let plan = self.pinned_plan(snapshot, self.store.as_ref()).await?;
        let applications: Vec<_> = snapshot
            .hook_applications
            .iter()
            .filter(|application| &application.target == target)
            .cloned()
            .collect();
        let mut current = None;
        let mut deny = None;
        for application in &applications {
            let record = self
                .store
                .read_record(&snapshot.scope, &application.result_ref)
                .await?;
            let value = HookApplicationRecord::restore(
                &record,
                &plan,
                application,
                &snapshot.scope,
                &snapshot.run_id,
            )?;
            if current.is_none() {
                current = Some(value.input.clone());
            }
            if deny.is_some() {
                return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.denied_chain"));
            }
            deny = records::apply(current.as_mut().expect("first input"), &value)?;
        }
        Ok(current.map(|input| records::transformed(input, deny, applications)))
    }

    /// Reuse each saved application before executing the remaining selected chain.
    /// Invalid output always fails closed; only optional before_run callback
    /// failure is persisted as a warning and permits the next callback.
    pub async fn transform(
        &self,
        target: HookTarget,
        mut input: HookInput,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<HookTransform, ContractError> {
        self.check_scope(context, budget.scope())?;
        input.validate(&target)?;
        if matches!(
            target,
            HookTarget::AfterTool { .. } | HookTarget::AfterRun { .. }
        ) {
            return Err(hook_error(
                ErrorCode::InvalidContract,
                "hooks.transform_target",
            ));
        }
        budget.check_boundary().await?;
        let saved = bounded(
            context,
            Some(budget),
            budget.call_deadline()?,
            budget.store().load(budget.scope(), budget.run_id()),
        )
        .await?;
        let plan = bounded(
            context,
            Some(budget),
            budget.call_deadline()?,
            self.pinned_plan(&saved.snapshot, budget.store().as_ref()),
        )
        .await?;
        bounded(
            context,
            Some(budget),
            budget.call_deadline()?,
            self.validate_live_input(&saved, &target, &input, budget.store().as_ref()),
        )
        .await?;
        let definitions: Vec<_> = plan
            .definitions()
            .iter()
            .enumerate()
            .filter(|(_, definition)| definition.position == target.position())
            .map(|(index, definition)| (definition.clone(), plan.selection(index).cloned()))
            .collect();
        let mut applications = Vec::new();
        let mut deny = None;
        for (definition, selection) in definitions {
            budget.check_boundary().await?;
            if context.cancellation.is_cancelled() {
                return Err(hook_error(ErrorCode::Cancelled, "hooks.transform"));
            }
            let current = bounded(
                context,
                Some(budget),
                budget.call_deadline()?,
                budget.store().load(budget.scope(), budget.run_id()),
            )
            .await?;
            if let Some(application) =
                current
                    .snapshot
                    .hook_applications
                    .iter()
                    .find(|application| {
                        application.target == target
                            && application.hook == definition.hook
                            && application.selection == selection
                    })
            {
                let record = bounded(
                    context,
                    Some(budget),
                    budget.call_deadline()?,
                    budget
                        .store()
                        .read_record(budget.scope(), &application.result_ref),
                )
                .await?;
                let value = HookApplicationRecord::restore(
                    &record,
                    &plan,
                    application,
                    budget.scope(),
                    budget.run_id(),
                )?;
                deny = records::apply(&mut input, &value)?;
                applications.push(application.clone());
                if deny.is_some() {
                    break;
                }
                continue;
            }
            let callback = self
                .invoke(
                    &definition,
                    selection.as_ref(),
                    &target,
                    &input,
                    budget.run_id(),
                    context,
                    Some(budget),
                    None,
                )
                .await?;
            let (output, failure) = match callback {
                Ok(output) => {
                    records::validate_output(&definition, &input, &output)?;
                    (Some(output), None)
                }
                Err(code) if target == HookTarget::BeforeRun && !definition.required => {
                    (None, Some(code))
                }
                Err(_) => return Err(hook_error(ErrorCode::InvalidContract, "hooks.callback")),
            };
            let context_items = if let Some(HookOutput::Context { additions }) = &output {
                additions
                    .iter()
                    .map(|addition| {
                        records::stamped(
                            self.ids.next_id()?,
                            budget.scope(),
                            budget.run_id(),
                            &definition,
                            &target,
                            addition,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                vec![]
            };
            let value = HookApplicationRecord {
                selection: selection.clone(),
                scope: budget.scope().clone(),
                run_id: budget.run_id().clone(),
                hook: definition.hook.clone(),
                definition_digest: definition.digest(),
                target: target.clone(),
                input: input.clone(),
                output,
                context_items,
                failure,
            };
            let record = ProtectedRecord::new(
                self.ids.next_id()?,
                1,
                serde_json::to_value(&value)
                    .map_err(|_| hook_error(ErrorCode::InvalidJson, "hooks.application"))?,
            );
            let application = HookApplication {
                selection: selection.clone(),
                hook: definition.hook.clone(),
                definition_digest: definition.digest(),
                target: target.clone(),
                input_digest: input.digest(),
                result_ref: record.reference().clone(),
            };
            budget.check_boundary().await?;
            let mut snapshot = bounded(
                context,
                Some(budget),
                budget.call_deadline()?,
                budget.store().load(budget.scope(), budget.run_id()),
            )
            .await?
            .snapshot;
            if snapshot.hook_applications.iter().any(|prior| {
                prior.target == target
                    && prior.hook == definition.hook
                    && prior.selection == selection
            }) {
                return Err(hook_error(ErrorCode::RevisionConflict, "hooks.application"));
            }
            let expected_revision = snapshot.revision;
            let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
            snapshot.revision = snapshot
                .revision
                .checked_add(1)
                .ok_or_else(|| hook_error(ErrorCode::RevisionConflict, "hooks.revision"))?;
            snapshot.usage.elapsed_ms = elapsed;
            snapshot.timing.last_observed_at_ms = now;
            snapshot.hook_applications.push(application.clone());
            bounded(
                context,
                Some(budget),
                budget.call_deadline()?,
                budget.store().commit(
                    budget.scope(),
                    budget.run_id(),
                    CommitInput {
                        expected_revision,
                        lease: budget.lease().clone(),
                        now_ms: now,
                        snapshot,
                        messages: vec![],
                        events: vec![],
                        records: vec![record],
                    },
                ),
            )
            .await?;
            deny = records::apply(&mut input, &value)?;
            applications.push(application);
            if deny.is_some() {
                break;
            }
        }
        Ok(records::transformed(input, deny, applications))
    }

    /// Observe committed data with finite cleanup time independent of the expired
    /// Run budget. Stored reports suppress repeat delivery; no crash replay or
    /// exactly-once external effect guarantee is provided.
    pub async fn observe(
        &self,
        run_id: &Id,
        target: HookTarget,
        input: HookInput,
        context: &ExecutionContext,
    ) -> Result<(), ContractError> {
        self.check_scope(context, self.scope())?;
        input.validate(&target)?;
        if !matches!(
            target,
            HookTarget::AfterTool { .. } | HookTarget::AfterRun { .. }
        ) {
            return Err(hook_error(
                ErrorCode::InvalidContract,
                "hooks.observer_target",
            ));
        }
        // Terminal cleanup is independent of the execution token that may have
        // been cancelled to produce this outcome. Current Host policy still runs.
        let cleanup_context;
        let context = if matches!(target, HookTarget::AfterRun { .. }) {
            let mut data = context.data.clone();
            data.system_inputs = None;
            cleanup_context = ExecutionContext::new(data, CancellationToken::new());
            &cleanup_context
        } else {
            context
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let saved = bounded(
            context,
            None,
            deadline,
            self.store.load(self.scope(), run_id),
        )
        .await?;
        let plan = bounded(
            context,
            None,
            deadline,
            self.pinned_plan(&saved.snapshot, self.store.as_ref()),
        )
        .await?;
        bounded(
            context,
            None,
            deadline,
            self.validate_live_input(&saved, &target, &input, self.store.as_ref()),
        )
        .await?;
        let prior = bounded(
            context,
            None,
            deadline,
            self.store.read_hook_observations(self.scope(), run_id),
        )
        .await?;
        let observer_deadline = if matches!(target, HookTarget::AfterTool { .. }) {
            let remaining = saved
                .snapshot
                .timing
                .deadline_at_ms
                .saturating_sub(self.clock.now()?.utc_ms)
                .max(0) as u64;
            deadline.min(tokio::time::Instant::now() + Duration::from_millis(remaining))
        } else {
            deadline
        };
        for (index, definition) in plan
            .definitions()
            .iter()
            .enumerate()
            .filter(|(_, definition)| definition.position == target.position())
        {
            let selection = plan.selection(index);
            if prior.iter().any(|report| {
                report.hook == definition.hook
                    && report.selection.as_ref() == selection
                    && report.definition_digest == definition.digest()
                    && report.target == target
            }) {
                continue;
            }
            let observed = self
                .invoke(
                    definition,
                    selection,
                    &target,
                    &input,
                    run_id,
                    context,
                    None,
                    Some(observer_deadline),
                )
                .await;
            let status = match observed {
                Ok(Ok(output)) => match records::validate_output(definition, &input, &output) {
                    Ok(()) => HookObservationStatus::Completed,
                    Err(error) => HookObservationStatus::Failed {
                        code: code_id(error.code)?,
                    },
                },
                Ok(Err(code)) => HookObservationStatus::Failed { code },
                Err(error) => HookObservationStatus::Failed {
                    code: code_id(error.code)?,
                },
            };
            let report = HookObservation {
                selection: selection.cloned(),
                scope: self.scope().clone(),
                run_id: run_id.clone(),
                hook: definition.hook.clone(),
                definition_digest: definition.digest(),
                target: target.clone(),
                input_digest: input.digest(),
                status,
                timestamp_ms: self.clock.now()?.utc_ms,
            };
            // Report failures remain separate from the already committed result.
            // A fresh cleanup token allows recording that the caller cancelled.
            let mut data = context.data.clone();
            data.system_inputs = None;
            let cleanup = ExecutionContext::new(data, CancellationToken::new());
            bounded(
                &cleanup,
                None,
                tokio::time::Instant::now() + Duration::from_secs(30),
                self.store
                    .record_hook_observation(self.scope(), run_id, report),
            )
            .await?;
        }
        Ok(())
    }

    fn check_scope(&self, context: &ExecutionContext, scope: &Scope) -> Result<(), ContractError> {
        if self.scope() != scope || &context.data.scope != scope {
            return Err(hook_error(ErrorCode::AccessDenied, "hooks.scope"));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn invoke(
        &self,
        definition: &HookDefinition,
        selection: Option<&HookRef>,
        target: &HookTarget,
        input: &HookInput,
        run_id: &Id,
        context: &ExecutionContext,
        budget: Option<&RunBudget>,
        external_deadline: Option<tokio::time::Instant>,
    ) -> Result<Result<HookOutput, Id>, ContractError> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(definition.timeout_ms);
        let deadline = if let Some(budget) = budget {
            deadline.min(budget.call_deadline()?)
        } else {
            deadline
        };
        let deadline = external_deadline.map_or(deadline, |external| deadline.min(external));
        let cancellation = context.cancellation.child_token();
        let _cancel = cancellation.clone().drop_guard();
        let mut data = context.data.clone();
        data.system_inputs = None;
        let policy_context = ExecutionContext::new(data, cancellation.clone());
        let request = PolicyRequest {
            owner_scope: self.scope().clone(),
            resource_id: run_id.clone(),
            action: PolicyAction::InvokeHook {
                selection: selection.cloned(),
                hook: definition.hook.clone(),
                definition_digest: definition.digest(),
                target: target.clone(),
            },
        };
        let decision = bounded(
            &policy_context,
            budget,
            deadline,
            self.policy
                .check(&request, &policy_context, Some(deadline), None),
        )
        .await?;
        if decision != (PolicyDecision::Allow {}) {
            return Err(hook_error(ErrorCode::AccessDenied, "hooks.policy"));
        }
        if let Some(budget) = budget {
            budget.check_boundary().await?;
        }
        let entry = self
            .registry
            .get(&definition.hook, selection)
            .filter(|entry| entry.definition == *definition)
            .ok_or_else(|| hook_error(ErrorCode::ComponentUnavailable, "hooks.handler"))?;
        let hook_context = HookContext {
            selection: selection.cloned(),
            binding_set_id: self.binding_set_id.clone(),
            scope: self.scope().clone(),
            run_id: run_id.clone(),
            hook: definition.hook.clone(),
            target: target.clone(),
            principal_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
            cancellation: cancellation.clone(),
            deadline,
        };
        let operation = AssertUnwindSafe(async { entry.handler.call(input, &hook_context).await })
            .catch_unwind();
        let result = tokio::select! {biased;
            _=context.cancellation.cancelled()=>Err(hook_error(ErrorCode::Cancelled,"hooks.callback")),
            stopped=run_stopped(budget)=>Err(stopped),
            _=tokio::time::sleep_until(deadline)=>Ok(Err(code_id(ErrorCode::DeadlineExceeded)?)),
            result=operation=>Ok(match result{Ok(Ok(output))=>Ok(output),Ok(Err(error))=>Err(code_id(error.code)?),Err(_)=>Err(code_id(ErrorCode::InvalidContract)?)})
        };
        cancellation.cancel();
        result
    }

    async fn validate_live_input(
        &self,
        saved: &StoredRun,
        target: &HookTarget,
        input: &HookInput,
        store: &dyn StateStore,
    ) -> Result<(), ContractError> {
        let invalid = || hook_error(ErrorCode::InvalidSnapshot, "hooks.target_input");
        match (target, input) {
            (
                HookTarget::BeforeRun,
                HookInput::BeforeRun {
                    user_input,
                    context_items,
                },
            ) if user_input == &saved.snapshot.request.input && context_items.is_empty() => {}
            (
                HookTarget::BeforeModel { model_step_id },
                HookInput::BeforeModel {
                    user_input,
                    context_items,
                },
            ) if user_input == &saved.snapshot.request.input
                && saved.snapshot.model_step_id.as_ref() == Some(model_step_id) =>
            {
                let mut expected = Vec::new();
                for application in saved
                    .snapshot
                    .hook_applications
                    .iter()
                    .filter(|application| application.target == HookTarget::BeforeRun)
                {
                    let record = store
                        .read_record(&saved.snapshot.scope, &application.result_ref)
                        .await?;
                    let value: HookApplicationRecord =
                        serde_json::from_value(record.value().clone()).map_err(|_| invalid())?;
                    expected.extend(value.context_items);
                }
                if context_items != &expected {
                    return Err(invalid());
                }
            }
            (
                HookTarget::BeforeTool { call_id },
                HookInput::BeforeTool {
                    tool,
                    descriptor_digest,
                    compiled_digest,
                    original_model_inputs,
                    model_inputs,
                    ..
                },
            ) => {
                let call = &saved
                    .snapshot
                    .tool_ledger
                    .iter()
                    .find(|entry| &entry.call.call_id == call_id)
                    .ok_or_else(invalid)?
                    .call;
                if call.bound_input_ref.is_some()
                    || original_model_inputs != &call.model_inputs
                    || model_inputs != original_model_inputs
                    || call.descriptor_digest.as_ref() != Some(descriptor_digest)
                {
                    return Err(invalid());
                }
                let record = store
                    .read_record(&saved.snapshot.scope, &saved.session.prompt_snapshot)
                    .await?;
                let prompt = PromptSnapshot::restore(
                    &serde_json::to_string(record.value()).map_err(|_| invalid())?,
                    &saved.session.prompt_snapshot.digest,
                    &saved.snapshot.profile,
                    &saved.snapshot.scope,
                )?;
                if !prompt.tools().iter().any(|entry| {
                    entry.model_tool == *tool
                        && &entry.descriptor_digest == descriptor_digest
                        && &entry.compiled_digest == compiled_digest
                }) {
                    return Err(invalid());
                }
            }
            (
                HookTarget::AfterTool {
                    call_id,
                    result_ref,
                },
                HookInput::AfterTool { .. },
            ) => {
                let record = store.read_record(&saved.snapshot.scope, result_ref).await?;
                let result: ToolResult =
                    serde_json::from_value(record.value().clone()).map_err(|_| invalid())?;
                if record.reference() != result_ref
                    || &result.call_id != call_id
                    || &HookInput::tool_observed(call_id, &result) != input
                {
                    return Err(invalid());
                }
            }
            (
                HookTarget::AfterRun {
                    outcome_ref,
                    revision,
                },
                HookInput::AfterRun { .. },
            ) => {
                let record = store
                    .read_record(&saved.snapshot.scope, outcome_ref)
                    .await?;
                let outcome: RunOutcome =
                    serde_json::from_value(record.value().clone()).map_err(|_| invalid())?;
                if record.reference() != outcome_ref
                    || !saved.snapshot.status.is_terminal()
                    || saved.snapshot.revision != *revision
                    || saved.snapshot.outcome.as_ref() != Some(&outcome)
                    || &HookInput::run_observed(&outcome) != input
                {
                    return Err(invalid());
                }
            }
            _ => return Err(invalid()),
        }
        if matches!(
            target,
            HookTarget::AfterTool { .. } | HookTarget::AfterRun { .. }
        ) {
            let mut after = 0;
            let mut found = false;
            loop {
                let page = store
                    .read_events(&saved.snapshot.scope, &saved.snapshot.run_id, after, 256)
                    .await?;
                found |= page
                    .events
                    .iter()
                    .any(|event| match (target, &event.payload) {
                        (
                            HookTarget::AfterTool { result_ref, .. },
                            RunEventPayload::ToolSettled {
                                result_ref: reference,
                            }
                            | RunEventPayload::ToolUnresolved {
                                result_ref: reference,
                                ..
                            },
                        ) => result_ref == reference,
                        (
                            HookTarget::AfterRun { outcome_ref, .. },
                            RunEventPayload::RunFinished {
                                outcome_ref: reference,
                            },
                        ) => outcome_ref == reference,
                        _ => false,
                    });
                if found || !page.has_more {
                    break;
                }
                if page.next_after_seq <= after {
                    return Err(invalid());
                }
                after = page.next_after_seq;
            }
            if !found {
                return Err(invalid());
            }
        }
        Ok(())
    }
}

async fn run_stopped(budget: Option<&RunBudget>) -> ContractError {
    if let Some(budget) = budget {
        budget
            .wait_for_cancellation_or_deadline()
            .await
            .err()
            .unwrap_or_else(|| hook_error(ErrorCode::DeadlineExceeded, "hooks.run"))
    } else {
        std::future::pending().await
    }
}
async fn bounded<T>(
    context: &ExecutionContext,
    budget: Option<&RunBudget>,
    deadline: tokio::time::Instant,
    operation: impl Future<Output = Result<T, ContractError>>,
) -> Result<T, ContractError> {
    tokio::select! {biased;
        _=context.cancellation.cancelled()=>Err(hook_error(ErrorCode::Cancelled,"hooks.operation")),
        stopped=run_stopped(budget)=>Err(stopped),
        _=tokio::time::sleep_until(deadline)=>Err(hook_error(ErrorCode::DeadlineExceeded,"hooks.operation")),
        result=operation=>result,
    }
}
fn code_id(code: ErrorCode) -> Result<Id, ContractError> {
    Id::new(
        serde_json::to_value(code)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .unwrap_or_else(|| "invalid_contract".into()),
    )
}
```

## `crates/wickle/src/input_binding.rs`

```rust
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    io,
    panic::AssertUnwindSafe,
    sync::Arc,
};

use futures_util::FutureExt;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::{
    CommitInput, CompiledTool, ContractError, ErrorCode, ExecutionContext, ExecutionContextData,
    Id, IdSource, JsonDigest, JsonObject, PolicyAction, PolicyDecision, PolicyGate, PolicyRequest,
    PortFuture, ProtectedRecord, RecordRef, RunBudget, RunSnapshot, Scope, SystemInputDefinition,
    SystemInputRegistry, SystemInputSnapshotRef, SystemInputSource, SystemInputs, ToolBindingRef,
    ToolCall, ToolCallState, ToolPolicyInput, VersionedRef, serialization::data_digest,
    tool_schema::compile_validator,
};

const RUN_INPUT_VERSION: &str = "wickle.run-system-inputs.v1";
const BOUND_INPUT_VERSION: &str = "wickle.bound-tool-input.v1";

/// Finite resolver and input-size bounds. They are independent of model token budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputBindingLimits {
    /// Maximum distinct resolver keys read for one new call; zero disables resolver reads.
    pub max_resolver_calls: usize,
    /// Maximum serialized bytes in one resolved or run-supplied value.
    pub max_value_bytes: usize,
    /// Maximum protected run-input or bound-input record size.
    pub max_bound_bytes: usize,
}
impl Default for InputBindingLimits {
    fn default() -> Self {
        Self {
            max_resolver_calls: 64,
            max_value_bytes: 65_536,
            max_bound_bytes: 1_048_576,
        }
    }
}
impl InputBindingLimits {
    fn validate(self) -> Result<(), ContractError> {
        if self.max_value_bytes == 0 || self.max_bound_bytes == 0 {
            return Err(error(ErrorCode::InvalidContract, "input_binding.limits"));
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunInputData {
    schema_version: String,
    scope: Scope,
    values: SystemInputs,
    definitions: BTreeMap<Id, SystemInputDefinition>,
}

/// Owned admission-time values and definition metadata. No resolver executes during
/// capture, and a missing value is not replaced by a schema default or generated ID.
#[derive(Clone)]
pub struct RunSystemInputs {
    data: RunInputData,
}

impl Serialize for RunSystemInputs {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.data.serialize(serializer)
    }
}
impl fmt::Debug for RunSystemInputs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunSystemInputs")
            .field("value_count", &self.data.values.values().len())
            .field("definition_count", &self.data.definitions.len())
            .finish_non_exhaustive()
    }
}

impl RunSystemInputs {
    /// Validate supplied keys/types and freeze owned values with the default finite bounds.
    pub fn capture(
        scope: Scope,
        supplied: Option<SystemInputs>,
        registry: &SystemInputRegistry,
    ) -> Result<Self, ContractError> {
        Self::capture_with_limits(scope, supplied, registry, InputBindingLimits::default())
    }
    /// Capture using explicit finite size bounds. Missing registered keys are allowed.
    pub fn capture_with_limits(
        scope: Scope,
        supplied: Option<SystemInputs>,
        registry: &SystemInputRegistry,
        limits: InputBindingLimits,
    ) -> Result<Self, ContractError> {
        limits.validate()?;
        let snapshot = Self {
            data: RunInputData {
                schema_version: RUN_INPUT_VERSION.into(),
                scope,
                values: supplied.unwrap_or_default(),
                definitions: registry.definitions().clone(),
            },
        };
        snapshot.validate_data()?;
        check_size(&snapshot, limits.max_bound_bytes)?;
        for value in snapshot.values().values() {
            check_size(value, limits.max_value_bytes)?;
        }
        Ok(snapshot)
    }
    /// Explicit access for the trusted binder; never automatic model projection.
    pub fn values(&self) -> &JsonObject {
        self.data.values.values()
    }
    /// Definition revisions and schemas pinned at admission.
    pub fn definitions(&self) -> &BTreeMap<Id, SystemInputDefinition> {
        &self.data.definitions
    }
    /// Exact owning scope of these values.
    pub fn scope(&self) -> &Scope {
        &self.data.scope
    }
    /// Digest of the complete protected serialized snapshot.
    pub fn digest(&self) -> JsonDigest {
        data_digest(&self.data)
    }
    /// Create the immutable record to include in the admission transaction.
    pub fn to_record(&self, record_id: Id, revision: u64) -> ProtectedRecord {
        ProtectedRecord::new(
            record_id,
            revision,
            serde_json::to_value(self).expect("serializable input data"),
        )
    }
    /// Create the run checkpoint reference after verifying its protected record identity.
    pub fn snapshot_ref(
        &self,
        record: &RecordRef,
    ) -> Result<SystemInputSnapshotRef, ContractError> {
        if record.digest != self.digest() {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "system_inputs.record",
            ));
        }
        Ok(SystemInputSnapshotRef {
            snapshot_ref: record.clone(),
            values_digest: data_digest(self.values()),
            definition_versions: self
                .definitions()
                .iter()
                .map(|(key, definition)| (key.clone(), definition.version.clone()))
                .collect(),
        })
    }
    /// Restore exact stored data and verify every pinned definition against the registry.
    /// Additional unrelated registry keys do not replace or enlarge the saved snapshot.
    pub fn restore(
        record: &ProtectedRecord,
        reference: &SystemInputSnapshotRef,
        scope: &Scope,
        registry: &SystemInputRegistry,
    ) -> Result<Self, ContractError> {
        if record.reference() != &reference.snapshot_ref {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "system_inputs.record",
            ));
        }
        let snapshot = Self::from_value(record.value(), reference, scope)?;
        if snapshot
            .definitions()
            .iter()
            .any(|(key, definition)| registry.get(key) != Some(definition))
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "system_inputs.definitions",
            ));
        }
        Ok(snapshot)
    }
    /// Omission reuses saved values. Any supplied map, including an empty map, must match.
    pub fn validate_resume(&self, supplied: Option<&SystemInputs>) -> Result<(), ContractError> {
        if supplied.is_some_and(|values| data_digest(values.values()) != data_digest(self.values()))
        {
            return Err(error(ErrorCode::SystemInputsMismatch, "system_inputs"));
        }
        Ok(())
    }
    fn validate_data(&self) -> Result<(), ContractError> {
        if self.data.schema_version != RUN_INPUT_VERSION
            || self
                .definitions()
                .iter()
                .any(|(key, definition)| key != &definition.key)
        {
            return Err(error(
                ErrorCode::SystemInputInvalid,
                "system_inputs.snapshot",
            ));
        }
        SystemInputRegistry::new(self.definitions().values().cloned().collect())
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "system_inputs.definitions"))?;
        for (key, value) in self.values() {
            let key = Id::new(key.clone())
                .map_err(|_| error(ErrorCode::SystemInputInvalid, "system_inputs.key"))?;
            let definition = self
                .definitions()
                .get(&key)
                .ok_or_else(|| error(ErrorCode::SystemInputInvalid, "system_inputs.key"))?;
            if !matches!(definition.source, SystemInputSource::Run {}) {
                return Err(error(ErrorCode::SystemInputInvalid, "system_inputs.source"));
            }
            validate_value(definition, value)?;
        }
        Ok(())
    }
    pub(crate) fn from_value(
        value: &Value,
        reference: &SystemInputSnapshotRef,
        scope: &Scope,
    ) -> Result<Self, ContractError> {
        let data: RunInputData = serde_json::from_value(value.clone())
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "system_inputs.snapshot"))?;
        let snapshot = Self { data };
        snapshot.validate_data()?;
        if snapshot.scope() != scope
            || snapshot.snapshot_ref(&reference.snapshot_ref)? != *reference
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "system_inputs.snapshot",
            ));
        }
        Ok(snapshot)
    }
}

/// One exact read-only resolver lookup, without other system values or credentials.
#[derive(Clone)]
pub struct SystemInputResolveRequest {
    /// Original selected adapter export; absent for a directly registered catalog tool.
    pub selection: Option<ToolBindingRef>,
    /// Registered key being requested.
    pub key: Id,
    /// Pinned value-definition revision.
    pub definition_version: Id,
    /// Exact resolver implementation selected by the definition.
    pub resolver_ref: VersionedRef,
    /// Normalized model-owned arguments only, including declared top-level defaults.
    pub model_inputs: JsonObject,
}
impl fmt::Debug for SystemInputResolveRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SystemInputResolveRequest")
            .field("key", &self.key)
            .field("definition_version", &self.definition_version)
            .finish_non_exhaustive()
    }
}

/// Current actor and execution bounds supplied to a trusted read-only resolver.
#[derive(Debug, Clone)]
pub struct SystemInputResolveContext {
    /// Authenticated scope, not a value extracted from the model's arguments.
    pub scope: Scope,
    /// Current principal; it does not rewrite the run's original system-input values.
    pub principal_ref: Id,
    /// Current capability grant, checked by policy and the resolver's own backend.
    pub capability_grant_ref: Id,
    /// Current owning run.
    pub run_id: Id,
    /// Original logical call identity.
    pub call_id: Id,
    /// Deadline for this lookup.
    pub deadline: tokio::time::Instant,
    /// Child cancellation signal linked to both execution and caller cancellation.
    pub cancellation: CancellationToken,
}

/// Data and source revision returned by a resolver, or recorded from a run snapshot.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedSystemInput {
    /// Supplied JSON value; explicit null is different from an absent result.
    pub value: Value,
    /// Source data revision, not a newly invented foreign key.
    pub revision: Id,
}
impl fmt::Debug for ResolvedSystemInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ResolvedSystemInput(<redacted>)")
    }
}

/// Trusted read-only lookup port. It must honor scope, principal, deadline and
/// cancellation, and must not hide business writes or create missing foreign keys.
pub trait SystemInputResolver: Send + Sync {
    /// Read one exact registered key; None means absent, not JSON null.
    fn resolve<'a>(
        &'a self,
        request: &'a SystemInputResolveRequest,
        context: &'a SystemInputResolveContext,
    ) -> PortFuture<'a, Option<ResolvedSystemInput>>;
}

/// One hidden parameter's fixed source, revision and optional value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundSystemInput {
    /// Registry key, which may differ from the handler parameter name.
    pub key: Id,
    /// Value-definition version pinned by the compiler and admission snapshot.
    pub definition_version: Id,
    /// Run snapshot or exact resolver implementation.
    pub source: SystemInputSource,
    /// None is absence; Some with value:null is an explicitly supplied null.
    pub resolved: Option<ResolvedSystemInput>,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundInputData {
    schema_version: String,
    scope: Scope,
    run_id: Id,
    call_id: Id,
    tool: VersionedRef,
    #[serde(
        default,
        deserialize_with = "crate::serialization::optional",
        skip_serializing_if = "Option::is_none"
    )]
    selection: Option<ToolBindingRef>,
    descriptor_digest: JsonDigest,
    compiled_digest: JsonDigest,
    compiler_version: String,
    original_model_inputs: JsonObject,
    #[serde(
        default,
        deserialize_with = "crate::serialization::optional",
        skip_serializing_if = "Option::is_none"
    )]
    effective_model_inputs: Option<JsonObject>,
    #[serde(
        default,
        deserialize_with = "crate::serialization::optional",
        skip_serializing_if = "Option::is_none"
    )]
    transformation_ref: Option<RecordRef>,
    normalized_model_inputs: JsonObject,
    run_inputs_ref: Option<SystemInputSnapshotRef>,
    system_inputs: BTreeMap<String, BoundSystemInput>,
    execution_args: JsonObject,
}

/// Immutable execution inputs. Serialization is only for protected storage/policy,
/// never a replacement for the original model ToolCall or its transcript message.
#[derive(Clone, Serialize)]
pub struct BoundToolInput {
    data: BoundInputData,
    binding_digest: JsonDigest,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundInputRecord {
    data: BoundInputData,
    binding_digest: JsonDigest,
}

impl fmt::Debug for BoundToolInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundToolInput")
            .field("call_id", &self.data.call_id)
            .field("tool", &self.data.tool)
            .field("binding_digest", &self.binding_digest)
            .finish_non_exhaustive()
    }
}

impl BoundToolInput {
    /// Exact owning resource scope.
    pub fn scope(&self) -> &Scope {
        &self.data.scope
    }
    /// Owning run identity.
    pub fn run_id(&self) -> &Id {
        &self.data.run_id
    }
    /// Stable logical call identity.
    pub fn call_id(&self) -> &Id {
        &self.data.call_id
    }
    /// Exact registered tool identity.
    pub fn tool(&self) -> &VersionedRef {
        &self.data.tool
    }
    /// Original export selection when this call is supplied by a scoped adapter.
    pub fn selection(&self) -> Option<&ToolBindingRef> {
        self.data.selection.as_ref()
    }
    /// Original descriptor digest.
    pub fn descriptor_digest(&self) -> &JsonDigest {
        &self.data.descriptor_digest
    }
    /// Compiler, schema and selected system-definition identity.
    pub fn compiled_digest(&self) -> &JsonDigest {
        &self.data.compiled_digest
    }
    /// Pinned compiler contract version.
    pub fn compiler_version(&self) -> &str {
        &self.data.compiler_version
    }
    /// Unmodified arguments originally recorded for the model call.
    pub fn original_model_inputs(&self) -> &JsonObject {
        &self.data.original_model_inputs
    }
    /// Validated hook-transformed arguments, or the unchanged original arguments.
    pub fn effective_model_inputs(&self) -> &JsonObject {
        self.data
            .effective_model_inputs
            .as_ref()
            .unwrap_or(&self.data.original_model_inputs)
    }
    /// Exact saved final transformation record, when hooks transformed this call.
    pub fn transformation_ref(&self) -> Option<&RecordRef> {
        self.data.transformation_ref.as_ref()
    }
    /// Effective model arguments plus declared optional top-level defaults.
    pub fn normalized_model_inputs(&self) -> &JsonObject {
        &self.data.normalized_model_inputs
    }
    /// Only hidden parameters needed by this tool, with fixed absence/value metadata.
    pub fn system_inputs(&self) -> &BTreeMap<String, BoundSystemInput> {
        &self.data.system_inputs
    }
    /// Full handler arguments; privileged access, never automatic model echo.
    pub fn execution_args(&self) -> &JsonObject {
        &self.data.execution_args
    }
    /// Digest over exact inputs, tool/compiler identity, source revisions, scope and call.
    pub fn binding_digest(&self) -> &JsonDigest {
        &self.binding_digest
    }
    /// Build the existing final-value policy input without introducing a new ownership port.
    pub fn policy_input(&self) -> ToolPolicyInput {
        let input = ToolPolicyInput::new(
            self.data.call_id.clone(),
            self.data.tool.clone(),
            self.data.descriptor_digest.clone(),
            self.binding_digest.clone(),
            self.data.execution_args.clone(),
        );
        match &self.data.selection {
            Some(selection) => input.with_selection(selection.clone()),
            None => input,
        }
    }
    /// Exact action checked for allow/deny/approval after all values are fixed.
    pub fn policy_request(&self) -> PolicyRequest {
        PolicyRequest {
            owner_scope: self.data.scope.clone(),
            resource_id: self.data.run_id.clone(),
            action: PolicyAction::ExecuteTool {
                input: self.policy_input(),
            },
        }
    }
    pub(crate) fn policy_request_for_run(&self, snapshot: &crate::RunSnapshot) -> PolicyRequest {
        let mut input = self.policy_input();
        if snapshot.scope == self.data.scope && snapshot.run_id == self.data.run_id {
            let receipt = snapshot.resume_receipts.iter().rev().find(|receipt| {
                matches!(&receipt.command.action,
                    crate::ResumeAction::Approve { target: crate::ApprovalTarget::Tool { call_id, binding_digest }, .. }
                    | crate::ResumeAction::Deny { target: crate::ApprovalTarget::Tool { call_id, binding_digest }, .. }
                    if call_id == &self.data.call_id && binding_digest == &self.binding_digest)
            });
            if let Some(receipt) = receipt.filter(|receipt| {
                !receipt.expired
                    && matches!(receipt.command.action, crate::ResumeAction::Approve { .. })
            }) {
                input = input.with_approval(receipt);
            }
        }
        PolicyRequest {
            owner_scope: self.data.scope.clone(),
            resource_id: self.data.run_id.clone(),
            action: PolicyAction::ExecuteTool { input },
        }
    }
    /// Restore protected inputs using the saved ledger call's exact record reference
    /// and the currently supplied compiled contract.
    pub fn restore(
        record: &ProtectedRecord,
        compiled: &CompiledTool,
        scope: &Scope,
        run_id: &Id,
        call: &ToolCall,
        run_inputs_ref: Option<&SystemInputSnapshotRef>,
    ) -> Result<Self, ContractError> {
        let bound = Self::from_value(record.value())?;
        if call.bound_input_ref.as_ref() != Some(record.reference())
            || data_digest(&bound) != record.reference().digest
        {
            return Err(error(ErrorCode::SystemInputsMismatch, "bound_input.record"));
        }
        bound.validate_identity(scope, run_id, call, run_inputs_ref)?;
        bound.validate_compiled(compiled)?;
        Ok(bound)
    }
    fn from_value(value: &Value) -> Result<Self, ContractError> {
        let record: BoundInputRecord = serde_json::from_value(value.clone())
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "bound_input"))?;
        let bound = Self {
            data: record.data,
            binding_digest: record.binding_digest,
        };
        if bound.data.schema_version != BOUND_INPUT_VERSION
            || data_digest(&bound.data) != bound.binding_digest
            || data_digest(&bound) != crate::canonical_digest(value)
        {
            return Err(error(ErrorCode::SystemInputsMismatch, "bound_input.digest"));
        }
        let mut execution = bound.data.normalized_model_inputs.clone();
        if bound.data.effective_model_inputs.is_some() != bound.data.transformation_ref.is_some() {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.transformation",
            ));
        }
        if bound
            .effective_model_inputs()
            .iter()
            .any(|(key, value)| execution.get(key) != Some(value))
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.model_inputs",
            ));
        }
        let mut sources: BTreeMap<&Id, &BoundSystemInput> = BTreeMap::new();
        for (parameter, input) in &bound.data.system_inputs {
            if execution.contains_key(parameter) {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.ownership",
                ));
            }
            if sources
                .insert(&input.key, input)
                .is_some_and(|previous| previous != input)
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.sources",
                ));
            }
            if let Some(resolved) = &input.resolved {
                execution.insert(parameter.clone(), resolved.value.clone());
            }
        }
        if execution != bound.data.execution_args {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.execution_args",
            ));
        }
        Ok(bound)
    }
    fn validate_identity(
        &self,
        scope: &Scope,
        run_id: &Id,
        call: &ToolCall,
        run_inputs_ref: Option<&SystemInputSnapshotRef>,
    ) -> Result<(), ContractError> {
        if self.scope() != scope
            || self.run_id() != run_id
            || self.call_id() != &call.call_id
            || Some(self.descriptor_digest()) != call.descriptor_digest.as_ref()
            || self.original_model_inputs() != &call.model_inputs
            || self.data.run_inputs_ref.as_ref() != run_inputs_ref
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.identity",
            ));
        }
        Ok(())
    }
    fn validate_compiled(&self, compiled: &CompiledTool) -> Result<(), ContractError> {
        if self.compiled_digest() != compiled.digest()
            || self.compiler_version() != compiled.compiler_version()
            || self.tool() != &compiled.descriptor().tool
            || self.descriptor_digest() != compiled.descriptor_digest()
            || self.system_inputs().len() != compiled.system_bindings().len()
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.compiled",
            ));
        }
        compiled.validate_model_inputs(self.original_model_inputs())?;
        compiled.validate_model_inputs(self.effective_model_inputs())?;
        if normalize_model_inputs(compiled, self.effective_model_inputs())?
            != *self.normalized_model_inputs()
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.normalization",
            ));
        }
        for (parameter, definition) in compiled.system_bindings() {
            let input = self
                .system_inputs()
                .get(parameter)
                .ok_or_else(|| error(ErrorCode::SystemInputsMismatch, "bound_input.parameters"))?;
            if input.key != definition.key
                || input.definition_version != definition.version
                || input.source != definition.source
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.definitions",
                ));
            }
            if let Some(value) = &input.resolved {
                validate_value(definition, &value.value)?;
            }
        }
        compiled
            .validate_execution_inputs(self.execution_args())
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "tool.execution_inputs"))
    }
}

/// A saved candidate and the decision observed at binding time, not a reusable
/// dispatch permit. The executor must recheck current policy/budgets before I/O.
#[derive(Debug)]
pub struct ToolBindingResult {
    /// Owned immutable protected input.
    pub input: BoundToolInput,
    /// Record stored atomically with the call's bound_input_ref.
    pub reference: RecordRef,
    /// Allow or require_approval. Deny is returned as an error without saving a new candidate.
    pub decision: PolicyDecision,
}

/// Default normalization, registered system-value lookup and immutable candidate persistence.
/// This version starts from the original model input. Hook transformations require
/// a separate recorded path and never overwrite the original ToolCall.
pub struct InputBinder {
    registry: Arc<SystemInputRegistry>,
    resolver: Option<Arc<dyn SystemInputResolver>>,
    policy: Arc<PolicyGate>,
    ids: Arc<dyn IdSource>,
    limits: InputBindingLimits,
}

impl InputBinder {
    /// Wire trusted metadata, optional read-only resolver, policy, and internal record IDs.
    pub fn new(
        registry: Arc<SystemInputRegistry>,
        resolver: Option<Arc<dyn SystemInputResolver>>,
        policy: Arc<PolicyGate>,
        ids: Arc<dyn IdSource>,
    ) -> Self {
        Self {
            registry,
            resolver,
            policy,
            ids,
            limits: InputBindingLimits::default(),
        }
    }
    /// Set finite lookup/value/candidate bounds. Zero lookups disables resolver sources.
    pub fn with_limits(mut self, limits: InputBindingLimits) -> Result<Self, ContractError> {
        limits.validate()?;
        self.limits = limits;
        Ok(self)
    }

    /// Reuse an existing saved binding, or bind and atomically save a new candidate.
    /// Every path checks current policy; an existing call never re-queries its resolver.
    pub async fn bind(
        &self,
        compiled: &CompiledTool,
        call_id: &Id,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<ToolBindingResult, ContractError> {
        boundary(context, budget).await?;
        let saved = bounded(
            context,
            budget,
            budget.store().load(budget.scope(), budget.run_id()),
        )
        .await?;
        let call = saved
            .snapshot
            .tool_ledger
            .iter()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool_call"))?
            .call
            .clone();
        check_selection(&saved.snapshot, compiled, &call)?;
        let selection = resolved_tool_selection(&saved.snapshot, compiled, context, budget).await?;
        let run_inputs = match &saved.snapshot.system_inputs {
            Some(reference) => {
                boundary(context, budget).await?;
                let record = bounded(
                    context,
                    budget,
                    budget
                        .store()
                        .read_record(budget.scope(), &reference.snapshot_ref),
                )
                .await?;
                let inputs =
                    RunSystemInputs::restore(&record, reference, budget.scope(), &self.registry)?;
                inputs.validate_resume(context.data.system_inputs.as_ref())?;
                Some(inputs)
            }
            None => {
                if context
                    .data
                    .system_inputs
                    .as_ref()
                    .is_some_and(|values| !values.values().is_empty())
                {
                    return Err(error(ErrorCode::SystemInputsMismatch, "system_inputs"));
                }
                if !compiled.system_bindings().is_empty() {
                    return Err(error(
                        ErrorCode::SystemInputMissing,
                        "system_inputs.snapshot",
                    ));
                }
                None
            }
        };
        for definition in compiled.system_bindings().values() {
            if self.registry.get(&definition.key) != Some(definition)
                || run_inputs
                    .as_ref()
                    .and_then(|inputs| inputs.definitions().get(&definition.key))
                    != Some(definition)
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "system_inputs.definitions",
                ));
            }
        }
        let transformed =
            saved_tool_transform(&saved.snapshot, compiled, &call, context, budget).await?;
        if let Some(reference) = &call.bound_input_ref {
            boundary(context, budget).await?;
            let record = bounded(
                context,
                budget,
                budget.store().read_record(budget.scope(), reference),
            )
            .await?;
            let input = BoundToolInput::restore(
                &record,
                compiled,
                budget.scope(),
                budget.run_id(),
                &call,
                saved.snapshot.system_inputs.as_ref(),
            )?;
            validate_bound_record(record.value(), &saved.snapshot, &call, run_inputs.as_ref())?;
            if input.selection() != selection.as_ref() {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.selection",
                ));
            }
            if input.transformation_ref() != transformed.as_ref().map(|(_, reference)| reference)
                || input.effective_model_inputs()
                    != transformed
                        .as_ref()
                        .map_or(&call.model_inputs, |(inputs, _)| inputs)
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.transformation",
                ));
            }
            check_size(&input, self.limits.max_bound_bytes)?;
            for value in input
                .system_inputs()
                .values()
                .filter_map(|input| input.resolved.as_ref())
            {
                check_size(&value.value, self.limits.max_value_bytes)?;
            }
            let decision = self
                .authorize(
                    &input.policy_request_for_run(&saved.snapshot),
                    context,
                    budget,
                    false,
                )
                .await?;
            boundary(context, budget).await?;
            return Ok(ToolBindingResult {
                input,
                reference: reference.clone(),
                decision,
            });
        }
        if !matches!(
            saved
                .snapshot
                .tool_ledger
                .iter()
                .find(|entry| &entry.call.call_id == call_id)
                .expect("found call")
                .state,
            ToolCallState::Planned {}
        ) {
            return Err(error(ErrorCode::InvalidTransition, "tool_call.state"));
        }
        let effective = transformed
            .as_ref()
            .map_or(&call.model_inputs, |(inputs, _)| inputs);
        let normalized = normalize_model_inputs(compiled, effective)?;
        check_size(&normalized, self.limits.max_bound_bytes)?;
        let mut execution_args = normalized.clone();
        let mut system_inputs = BTreeMap::new();
        let mut values: BTreeMap<Id, Option<ResolvedSystemInput>> = BTreeMap::new();
        let mut resolver_calls = 0;
        for (parameter, definition) in compiled.system_bindings() {
            let resolved = if let Some(cached) = values.get(&definition.key) {
                cached.clone()
            } else {
                let value = match &definition.source {
                    SystemInputSource::Run {} => run_inputs
                        .as_ref()
                        .and_then(|inputs| inputs.values().get(definition.key.as_str()))
                        .cloned()
                        .map(|value| ResolvedSystemInput {
                            value,
                            revision: Id::new(
                                saved
                                    .snapshot
                                    .system_inputs
                                    .as_ref()
                                    .expect("required run snapshot")
                                    .snapshot_ref
                                    .revision
                                    .to_string(),
                            )
                            .expect("numeric revision"),
                        }),
                    SystemInputSource::Resolver { resolver_ref } => {
                        if resolver_calls >= self.limits.max_resolver_calls {
                            return Err(limit_error());
                        }
                        let request = PolicyRequest {
                            owner_scope: budget.scope().clone(),
                            resource_id: budget.run_id().clone(),
                            action: PolicyAction::ResolveSystemInput {
                                selection: selection.clone(),
                                tool: compiled.descriptor().tool.clone(),
                                call_id: call_id.clone(),
                                descriptor_digest: compiled.descriptor_digest().clone(),
                                compiled_digest: compiled.digest().clone(),
                                key: definition.key.clone(),
                                definition_version: definition.version.clone(),
                                resolver_ref: resolver_ref.clone(),
                            },
                        };
                        self.authorize(&request, context, budget, true).await?;
                        let resolver = self.resolver.as_ref().ok_or_else(|| {
                            error(ErrorCode::SystemInputUnavailable, "system_input.resolver")
                        })?;
                        boundary(context, budget).await?;
                        let request = SystemInputResolveRequest {
                            selection: selection.clone(),
                            key: definition.key.clone(),
                            definition_version: definition.version.clone(),
                            resolver_ref: resolver_ref.clone(),
                            model_inputs: normalized.clone(),
                        };
                        let child = budget.cancellation().child_token();
                        let lookup_context = SystemInputResolveContext {
                            scope: budget.scope().clone(),
                            principal_ref: context.data.principal_ref.clone(),
                            capability_grant_ref: context.data.capability_grant_ref.clone(),
                            run_id: budget.run_id().clone(),
                            call_id: call_id.clone(),
                            deadline: budget.call_deadline()?,
                            cancellation: child.clone(),
                        };
                        let lookup = AssertUnwindSafe(async {
                            resolver.resolve(&request, &lookup_context).await
                        })
                        .catch_unwind();
                        tokio::pin!(lookup);
                        let guard = child.drop_guard();
                        resolver_calls += 1;
                        let answer = bounded(context, budget, async {
                            lookup
                                .await
                                .map_err(|_| {
                                    error(
                                        ErrorCode::SystemInputUnavailable,
                                        "system_input.resolver",
                                    )
                                })?
                                .map_err(|_| {
                                    error(
                                        ErrorCode::SystemInputUnavailable,
                                        "system_input.resolver",
                                    )
                                })
                        })
                        .await;
                        drop(guard);
                        let answer = answer?;
                        boundary(context, budget).await?;
                        answer
                    }
                };
                if let Some(resolved) = &value {
                    check_size(&resolved.value, self.limits.max_value_bytes)?;
                    validate_value(definition, &resolved.value)?;
                }
                values.insert(definition.key.clone(), value.clone());
                value
            };
            if let Some(value) = &resolved {
                execution_args.insert(parameter.clone(), value.value.clone());
            } else if required_parameter(compiled, parameter) {
                return Err(error(
                    ErrorCode::SystemInputMissing,
                    &system_input_path(&definition.key),
                ));
            }
            system_inputs.insert(
                parameter.clone(),
                BoundSystemInput {
                    key: definition.key.clone(),
                    definition_version: definition.version.clone(),
                    source: definition.source.clone(),
                    resolved,
                },
            );
        }
        compiled
            .validate_execution_inputs(&execution_args)
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "tool.execution_inputs"))?;
        let data = BoundInputData {
            schema_version: BOUND_INPUT_VERSION.into(),
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            call_id: call_id.clone(),
            tool: compiled.descriptor().tool.clone(),
            selection,
            descriptor_digest: compiled.descriptor_digest().clone(),
            compiled_digest: compiled.digest().clone(),
            compiler_version: compiled.compiler_version().into(),
            original_model_inputs: call.model_inputs.clone(),
            effective_model_inputs: transformed.as_ref().map(|(inputs, _)| inputs.clone()),
            transformation_ref: transformed.map(|(_, reference)| reference),
            normalized_model_inputs: normalized,
            run_inputs_ref: saved.snapshot.system_inputs.clone(),
            system_inputs,
            execution_args,
        };
        let input = BoundToolInput {
            binding_digest: data_digest(&data),
            data,
        };
        check_size(&input, self.limits.max_bound_bytes)?;
        let decision = self
            .authorize(
                &input.policy_request_for_run(&saved.snapshot),
                context,
                budget,
                false,
            )
            .await?;
        boundary(context, budget).await?;
        let record = ProtectedRecord::new(
            self.ids.next_id()?,
            1,
            serde_json::to_value(&input).expect("bound input serialization"),
        );
        let reference = record.reference().clone();
        let mut next = saved.snapshot;
        let expected_revision = next.revision;
        let (elapsed, now_ms) = budget.settlement_time(next.usage.elapsed_ms)?;
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::RevisionConflict, "revision"))?;
        next.usage.elapsed_ms = elapsed;
        next.timing.last_observed_at_ms = now_ms;
        next.tool_ledger
            .iter_mut()
            .find(|entry| &entry.call.call_id == call_id)
            .expect("found call")
            .call
            .bound_input_ref = Some(reference.clone());
        bounded(
            context,
            budget,
            budget.store().commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms,
                    snapshot: next,
                    messages: Vec::new(),
                    events: Vec::new(),
                    records: vec![record],
                },
            ),
        )
        .await?;
        boundary(context, budget).await?;
        Ok(ToolBindingResult {
            input,
            reference,
            decision,
        })
    }

    async fn authorize(
        &self,
        request: &PolicyRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
        lookup: bool,
    ) -> Result<PolicyDecision, ContractError> {
        boundary(context, budget).await?;
        let deadline = budget.call_deadline()?;
        let child = budget.cancellation().child_token();
        let policy_context = ExecutionContext::new(
            ExecutionContextData {
                scope: context.data.scope.clone(),
                principal_ref: context.data.principal_ref.clone(),
                capability_grant_ref: context.data.capability_grant_ref.clone(),
                trace_context: None,
                system_inputs: None,
            },
            child.clone(),
        );
        let check = self
            .policy
            .check(request, &policy_context, Some(deadline), None);
        tokio::pin!(check);
        let guard = child.drop_guard();
        let result = bounded(context, budget, &mut check).await;
        drop(guard);
        let decision = result?;
        boundary(context, budget).await?;
        match decision {
            PolicyDecision::Deny { .. } => Err(error(ErrorCode::AccessDenied, "policy")),
            PolicyDecision::RequireApproval { .. } if lookup => Err(error(
                ErrorCode::SystemInputApprovalRequired,
                "system_input.lookup",
            )),
            decision => Ok(decision),
        }
    }
}

fn check_selection(
    snapshot: &RunSnapshot,
    compiled: &CompiledTool,
    call: &ToolCall,
) -> Result<(), ContractError> {
    if call.descriptor_digest.as_ref() != Some(compiled.descriptor_digest())
        || !snapshot
            .profile
            .profile()
            .tools
            .iter()
            .any(|selection| match selection {
                ToolBindingRef::Catalog(reference) => {
                    reference.tool_id == compiled.descriptor().tool.id
                        && reference.version == compiled.descriptor().tool.version
                        && call.tool_name == compiled.descriptor().name
                }
                ToolBindingRef::Export(export) => {
                    export.alias.as_ref().unwrap_or(&compiled.descriptor().name) == &call.tool_name
                        && snapshot
                            .profile
                            .profile()
                            .adapters
                            .as_ref()
                            .is_some_and(|adapters| {
                                adapters
                                    .iter()
                                    .any(|adapter| adapter.binding_id == export.adapter_binding)
                            })
                }
            })
    {
        return Err(error(
            ErrorCode::InvalidToolInputContract,
            "tool_call.descriptor",
        ));
    }
    Ok(())
}

fn normalize_model_inputs(
    compiled: &CompiledTool,
    original: &JsonObject,
) -> Result<JsonObject, ContractError> {
    compiled.validate_model_inputs(original)?;
    let mut normalized = original.clone();
    let properties = compiled
        .model_input_schema()
        .get("properties")
        .and_then(Value::as_object)
        .expect("compiled properties");
    for parameter in &compiled.descriptor().agent_parameters {
        if normalized.contains_key(parameter) || required_parameter(compiled, parameter) {
            continue;
        }
        let mut schema = &properties[parameter];
        let mut seen = BTreeSet::new();
        loop {
            if let Some(default) = schema.get("default") {
                normalized.insert(parameter.clone(), default.clone());
                break;
            }
            let Some(reference) = schema.get("$ref").and_then(Value::as_str) else {
                break;
            };
            if !seen.insert(reference) {
                break;
            }
            let pointer = reference
                .strip_prefix('#')
                .ok_or_else(|| error(ErrorCode::InvalidToolInputContract, "model_defaults"))?;
            schema = compiled
                .model_input_schema()
                .pointer(pointer)
                .ok_or_else(|| error(ErrorCode::InvalidToolInputContract, "model_defaults"))?;
        }
    }
    compiled.validate_model_inputs(&normalized)?;
    Ok(normalized)
}
fn required_parameter(compiled: &CompiledTool, parameter: &str) -> bool {
    compiled
        .input_schema()
        .get("required")
        .and_then(Value::as_array)
        .is_some_and(|required| required.iter().any(|name| name.as_str() == Some(parameter)))
}
fn validate_value(definition: &SystemInputDefinition, value: &Value) -> Result<(), ContractError> {
    let validator = compile_validator(&definition.value_schema).map_err(|_| {
        error(
            ErrorCode::SystemInputInvalid,
            &system_input_path(&definition.key),
        )
    })?;
    if !validator.is_valid(value) {
        return Err(error(
            ErrorCode::SystemInputInvalid,
            &system_input_path(&definition.key),
        ));
    }
    Ok(())
}

fn system_input_path(key: &Id) -> String {
    // Only registered metadata is named; JSON escaping prevents control characters
    // or punctuation from being interpreted as a path or leaking a supplied value.
    format!(
        "system_inputs[{}]",
        serde_json::to_string(key.as_str()).expect("serializable key")
    )
}

async fn boundary(context: &ExecutionContext, budget: &RunBudget) -> Result<(), ContractError> {
    if &context.data.scope != budget.scope() {
        return Err(error(ErrorCode::AccessDenied, "scope"));
    }
    if context.cancellation.is_cancelled() {
        return Err(error(ErrorCode::Cancelled, "input_binding"));
    }
    bounded(context, budget, budget.check_boundary()).await
}
async fn bounded<T>(
    context: &ExecutionContext,
    budget: &RunBudget,
    future: impl Future<Output = Result<T, ContractError>>,
) -> Result<T, ContractError> {
    if context.cancellation.is_cancelled() {
        return Err(error(ErrorCode::Cancelled, "input_binding"));
    }
    tokio::select! {
        biased;
        _ = context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "input_binding")),
        stopped = budget.wait_for_cancellation_or_deadline() => { stopped?; Err(error(ErrorCode::DeadlineExceeded, "input_binding")) },
        result = future => {
            if context.cancellation.is_cancelled() || budget.cancellation().is_cancelled() { return Err(error(ErrorCode::Cancelled, "input_binding")); }
            budget.call_deadline()?;
            result
        }
    }
}

async fn resolved_tool_selection(
    snapshot: &RunSnapshot,
    compiled: &CompiledTool,
    context: &ExecutionContext,
    budget: &RunBudget,
) -> Result<Option<ToolBindingRef>, ContractError> {
    let Some(reference) = &snapshot.assembly_ref else {
        if snapshot
            .profile
            .profile()
            .tools
            .iter()
            .any(|selection| matches!(selection, ToolBindingRef::Export(_)))
        {
            return Err(error(ErrorCode::InvalidSnapshot, "bound_input.assembly"));
        }
        return Ok(None);
    };
    let inputs = if let Some(reference) = &snapshot.system_inputs {
        let record = bounded(
            context,
            budget,
            budget
                .store()
                .read_record(budget.scope(), &reference.snapshot_ref),
        )
        .await?;
        let input = RunSystemInputs::from_value(record.value(), reference, budget.scope())?;
        SystemInputRegistry::new(input.definitions().values().cloned().collect())?
    } else {
        SystemInputRegistry::default()
    };
    let record = bounded(
        context,
        budget,
        budget.store().read_record(budget.scope(), reference),
    )
    .await?;
    let assembly = crate::ResolvedAssembly::restore(
        &serde_json::to_string(record.value())
            .map_err(|_| error(ErrorCode::InvalidJson, "bound_input.assembly"))?,
        &snapshot.profile,
        &inputs,
        &reference.digest,
    )?;
    let matches: Vec<_> = assembly
        .tools()
        .iter()
        .filter(|binding| {
            binding.compiled.digest() == compiled.digest()
                && binding.compiled.descriptor().name == compiled.descriptor().name
        })
        .collect();
    if matches.len() != 1 {
        return Err(error(
            ErrorCode::SystemInputsMismatch,
            "bound_input.selection",
        ));
    }
    Ok(match &matches[0].selection {
        selection @ ToolBindingRef::Export(_) => Some(selection.clone()),
        _ => None,
    })
}

async fn saved_tool_transform(
    snapshot: &RunSnapshot,
    compiled: &CompiledTool,
    call: &ToolCall,
    context: &ExecutionContext,
    budget: &RunBudget,
) -> Result<Option<(JsonObject, RecordRef)>, ContractError> {
    let Some(reference) = &snapshot.hook_plan_ref else {
        return Ok(None);
    };
    let record = bounded(
        context,
        budget,
        budget.store().read_record(budget.scope(), reference),
    )
    .await?;
    let plan = crate::HookPlan::restore(
        &serde_json::to_string(record.value())
            .map_err(|_| error(ErrorCode::InvalidJson, "hooks.plan"))?,
        budget.scope(),
        &reference.digest,
    )?;
    let definitions: Vec<_> = plan
        .definitions()
        .iter()
        .filter(|definition| definition.position == crate::HookPosition::BeforeTool)
        .collect();
    if definitions.is_empty() {
        return Ok(None);
    }
    let target = crate::HookTarget::BeforeTool {
        call_id: call.call_id.clone(),
    };
    let applications: Vec<_> = snapshot
        .hook_applications
        .iter()
        .filter(|application| application.target == target)
        .collect();
    if applications.len() != definitions.len() {
        return Err(error(
            ErrorCode::InvalidTransition,
            "hooks.before_tool_missing",
        ));
    }
    let mut inputs = call.model_inputs.clone();
    for (definition, application) in definitions.iter().zip(&applications) {
        if definition.hook != application.hook {
            return Err(error(ErrorCode::InvalidSnapshot, "hooks.order"));
        }
        let record = bounded(
            context,
            budget,
            budget
                .store()
                .read_record(budget.scope(), &application.result_ref),
        )
        .await?;
        let record = crate::HookApplicationRecord::restore(
            &record,
            &plan,
            application,
            budget.scope(),
            budget.run_id(),
        )?;
        let crate::HookInput::BeforeTool {
            tool,
            descriptor_digest,
            compiled_digest,
            original_model_inputs,
            model_inputs,
        } = &record.input
        else {
            return Err(error(ErrorCode::InvalidSnapshot, "hooks.tool_input"));
        };
        if tool != &compiled.to_model_tool()
            || descriptor_digest != compiled.descriptor_digest()
            || compiled_digest != compiled.digest()
            || original_model_inputs != &call.model_inputs
            || model_inputs != &inputs
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "hooks.tool_identity",
            ));
        }
        let Some(crate::HookOutput::Tool { model_inputs, deny }) = record.output else {
            return Err(error(ErrorCode::InvalidSnapshot, "hooks.tool_output"));
        };
        if deny.is_some() {
            return Err(error(ErrorCode::AccessDenied, "hooks.tool_denied"));
        }
        compiled.validate_model_inputs(&model_inputs)?;
        inputs = model_inputs;
    }
    Ok(Some((
        inputs,
        applications
            .last()
            .expect("nonempty definitions")
            .result_ref
            .clone(),
    )))
}

/// Validate the exact saved transformation that a bound candidate claims to use.
pub(crate) fn validate_bound_transformation(
    value: &Value,
    snapshot: &RunSnapshot,
    call: &ToolCall,
    transformation: Option<&Value>,
) -> Result<(), ContractError> {
    let bound = BoundToolInput::from_value(value)?;
    let target = crate::HookTarget::BeforeTool {
        call_id: call.call_id.clone(),
    };
    let application = snapshot
        .hook_applications
        .iter()
        .rev()
        .find(|application| application.target == target);
    if bound.transformation_ref() != application.map(|application| &application.result_ref) {
        return Err(error(
            ErrorCode::SystemInputsMismatch,
            "bound_input.transform_reference",
        ));
    }
    match (bound.transformation_ref(), transformation) {
        (None, None) => Ok(()),
        (Some(reference), Some(value)) => {
            if crate::canonical_digest(value) != reference.digest {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.transform_digest",
                ));
            }
            let record: crate::HookApplicationRecord = serde_json::from_value(value.clone())
                .map_err(|_| error(ErrorCode::InvalidSnapshot, "bound_input.transform_record"))?;
            let crate::HookInput::BeforeTool {
                descriptor_digest,
                compiled_digest,
                original_model_inputs,
                ..
            } = record.input
            else {
                return Err(error(
                    ErrorCode::InvalidSnapshot,
                    "bound_input.transform_input",
                ));
            };
            if record.scope != snapshot.scope
                || record.run_id != snapshot.run_id
                || record.target != target
                || &descriptor_digest != bound.descriptor_digest()
                || &compiled_digest != bound.compiled_digest()
                || original_model_inputs != call.model_inputs
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.transform_identity",
                ));
            }
            match record.output {
                Some(crate::HookOutput::Tool {
                    model_inputs,
                    deny: None,
                }) if &model_inputs == bound.effective_model_inputs() => Ok(()),
                _ => Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.transform_output",
                )),
            }
        }
        _ => Err(error(
            ErrorCode::SystemInputsMismatch,
            "bound_input.transform_record",
        )),
    }
}

pub(crate) fn validate_bound_record(
    value: &Value,
    snapshot: &RunSnapshot,
    call: &ToolCall,
    run_inputs: Option<&RunSystemInputs>,
) -> Result<(), ContractError> {
    let input = BoundToolInput::from_value(value)?;
    input.validate_identity(
        &snapshot.scope,
        &snapshot.run_id,
        call,
        snapshot.system_inputs.as_ref(),
    )?;
    if input
        .selection()
        .is_some_and(|selection| !snapshot.profile.profile().tools.contains(selection))
    {
        return Err(error(
            ErrorCode::SystemInputsMismatch,
            "bound_input.selection",
        ));
    }
    for bound in input.system_inputs().values() {
        let data = run_inputs
            .ok_or_else(|| error(ErrorCode::SystemInputsMismatch, "bound_input.run_snapshot"))?;
        let definition = data
            .definitions()
            .get(&bound.key)
            .ok_or_else(|| error(ErrorCode::SystemInputsMismatch, "bound_input.definition"))?;
        if definition.version != bound.definition_version || definition.source != bound.source {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.definition",
            ));
        }
        if let Some(value) = &bound.resolved {
            validate_value(definition, &value.value)?;
        }
        if matches!(bound.source, SystemInputSource::Run {}) {
            let expected = data.values().get(bound.key.as_str());
            if bound.resolved.as_ref().map(|resolved| &resolved.value) != expected
                || bound.resolved.as_ref().is_some_and(|resolved| {
                    resolved.revision.as_str()
                        != snapshot
                            .system_inputs
                            .as_ref()
                            .expect("snapshot supplied")
                            .snapshot_ref
                            .revision
                            .to_string()
                })
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.run_value",
                ));
            }
        }
    }
    Ok(())
}

struct ByteCounter {
    total: usize,
    limit: usize,
}
impl io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.total = self
            .total
            .checked_add(bytes.len())
            .filter(|size| *size <= self.limit)
            .ok_or_else(|| io::Error::other("input size limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn check_size(value: &impl Serialize, limit: usize) -> Result<(), ContractError> {
    serde_json::to_writer(&mut ByteCounter { total: 0, limit }, value).map_err(|_| limit_error())
}
fn limit_error() -> ContractError {
    error(ErrorCode::InputBindingLimitExceeded, "input_binding.limits")
}
fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
```

## `crates/wickle/src/lib.rs`

```rust
//! Wickle, an agent engine for Rust applications.
//!
//! Profiles, scoped metadata resolution, and versioned execution data contracts.
//! The agent driver runs model/tool loops with scoped ports, separate system
//! inputs, persisted attempt accounting, and explicit effect outcomes.
//!
//! Runtime objects stay in Host code. Only documented data contracts are
//! serialized; successful decoding does not authenticate a caller.
//!
//! Internal modules are not extension points; use the root exports.
//! ```compile_fail
//! use wickle::serialization::canonical_digest;
//! ```

mod agent;
mod budget;
mod clock;
mod component_runtime;
mod context;
mod context_projection;
mod error;
mod hooks;
mod input_binding;
mod message;
mod model;
mod model_catalog;
mod model_dispatch;
mod model_execution;
mod model_protocol;
mod model_routing;
mod policy;
mod profile;
mod resolution;
mod run;
mod serialization;
mod state;
mod tool_execution;
mod tool_schema;
mod views;

pub use agent::{
    Agent, AgentBindings, AgentSettings, CancelReceipt, ComponentReleaseView, HookObservationView,
    ModelTokenEstimator, RunHandle, create_agent,
};
pub use budget::{AttemptReservation, ReservationKind, RunBudget, RunTiming};
pub use clock::{Clock, ClockReading, IdSource, RandomIdSource, SystemClock};
pub use component_runtime::{
    AdapterBindingState, AdapterCloseContext, AdapterDefinition, AdapterExportDefinition,
    AdapterExportInstance, AdapterFactory, AdapterInitContext, AdapterInstance, BoundCapabilities,
    ComponentBindContext, ComponentBindPurpose, ComponentRelease, ComponentReleaseContext,
    ComponentReleaseFailure, ComponentReleaseReport, ComponentResolveContext, ComponentRuntime,
    ResolvedAdapterBinding, ResolvedAssembly, ResolvedConnection, ResolvedHookBinding,
    ResolvedToolBinding,
};
pub use context_projection::{
    CONTEXT_ASSEMBLER_VERSION, ContextAssembler, ContextItem, ContextLifetime, ContextOrigin,
    ContextPriority, ContextProjection, InstructionAssetContent, PinnedPromptTool, ProjectionInput,
    ProjectionLimits, PromptSnapshot, PromptToolBinding, ScopedOpaque, SkillManifest,
};
pub use hooks::{
    HookApplication, HookApplicationRecord, HookContext, HookContextAddition, HookDefinition,
    HookHandler, HookInput, HookObservation, HookObservationStatus, HookOutput, HookPlan,
    HookRegistration, HookRegistry, HookRuntime, HookTarget, HookTransform,
};
pub use input_binding::{
    BoundSystemInput, BoundToolInput, InputBinder, InputBindingLimits, ResolvedSystemInput,
    RunSystemInputs, SystemInputResolveContext, SystemInputResolveRequest, SystemInputResolver,
    ToolBindingResult,
};
pub use model_catalog::{
    CatalogRequirements, ModelAlias, ModelBinding, ModelCapabilities, ModelCatalog,
    ModelCatalogSnapshot, ModelDefinition, ModelDefinitionRef, ModelEvidence, ModelLifecycle,
    ModelSupportStatus, ModelValidationEvidence, ModelValidationKind, ResolvedCatalogBinding,
};
pub use model_protocol::{
    ModelCallContext, ModelContent, ModelEvent, ModelFinish, ModelMessage, ModelOutput, ModelPort,
    ModelPortBinding, ModelProtocolError, ModelProtocolErrorCode, ModelRequest, ModelResponse,
    ModelResponseLimits, ModelResponseMetadata, ModelRole, ModelTool, OpaqueContinuation,
    ProposedToolCall, ToolCallValidation, collect_model_response,
};
pub use model_routing::{
    MAX_ROUTE_FALLBACKS, MAX_ROUTING_RULES, ModelRouter, ROUTING_SNAPSHOT_VERSION, RouteSelection,
    RouteSelectionReason, RoutingPolicy, RoutingRule, RoutingSnapshot,
};
pub use policy::{
    ApprovalChallenge, Guarded, PolicyAction, PolicyContext, PolicyDecision, PolicyGate,
    PolicyPort, PolicyRequest, ToolApproval, ToolPolicyInput,
};
pub use state::{
    AdmissionInput, AdmissionResult, CommitInput, EventPage, MAX_EVENT_PAGE_SIZE, MemoryStateStore,
    ProtectedRecord, RunLease, STATE_STORE_CHECKPOINT_VERSION, StateStore, StateStoreCapabilities,
    StateStoreCheckpoint, StoredRun,
};
pub use tool_execution::{
    ExternalReceiptContext, ExternalReceiptRequest, ExternalReceiptVerifier,
    PreparedToolResolution, SerialToolRound, ToolEffect, ToolExecutionContext, ToolExecutionLimits,
    ToolExecutionOutcome, ToolExecutionResult, ToolExecutor, ToolRegistration, ToolRegistry,
    ToolRoundOutcome,
};
pub use tool_schema::{
    CompiledTool, SchemaCompiler, SystemInputDefinition, SystemInputRegistry, SystemInputSource,
    TOOL_SCHEMA_COMPILER_VERSION, ToolConcurrency, ToolDescriptor, ToolRetryPolicy, ToolSideEffect,
};
pub use views::{ArtifactView, EventView, RunView};

pub use context::{
    ExecutionContext, ExecutionContextData, PortFuture, PortStream, Scope, SystemInputs,
};
pub use error::{ContractError, ErrorCode};
pub use message::{
    ArtifactRef, ContentBlock, EvidenceRef, Failure, InputContent, Message, MessageOrigin,
    MessageRole, RecordRef, ToolCall, ToolResult, ToolResultStatus, Visibility,
};
pub use model::{
    ApiContract, ModelAttemptState, ModelFailureKind, ModelInvocationRecord, ModelPurpose,
    ModelUsage, ResolvedModelRoute, RouteRequest, UsageMeasurement, VersionPolicy,
    VersionSemantics,
};
pub use model_dispatch::{
    ModelDispatcher, ModelInspectionContext, ModelRouteAvailability, ModelRouteInspector,
    ModelRouteObservation,
};
pub use model_execution::{
    ModelExchange, ModelExchangeOutcome, ModelProjectionContext, ModelRequestProjector,
    ModelRetryPolicy, ProjectedModelRequest, RoutedModelInput, StoredModelResponse,
};
pub use profile::{
    AdapterBindingRef, AgentProfile, CatalogHookRef, CatalogSourceRef, CatalogToolRef,
    CompletionPolicy, ConnectorBindingRef, ContextPolicy, ContextSourceBinding, ContextSourceRef,
    ContextTrigger, ExportRef, HookPosition, HookRef, InstructionAsset, InstructionText,
    Instructions, OutputContract, PROFILE_SCHEMA_VERSION, ProfileSchemaVersion, RunLimits,
    SkillRef, ToolBindingRef, VersionedRef,
};
pub use resolution::{
    ComponentKind, ComponentMetadata, ComponentRef, ExportKind, ExportMetadata, ProfileResolver,
    ProfileValidator, ResolvedComponent, ResolvedProfile,
};
pub use run::{
    ApprovalTarget, BudgetKind, BudgetUsage, CompletionBasis, EphemeralEvent, InputRequest,
    OutcomeResult, RUN_EVENT_SCHEMA_VERSION, RUN_SNAPSHOT_SCHEMA_VERSION, ResumeAction,
    ResumeCommand, ResumeReceipt, RunEvent, RunEventPayload, RunEventSchemaVersion, RunOutcome,
    RunPhase, RunRequest, RunSnapshot, RunSnapshotSchemaVersion, RunStatus, RunTrigger,
    SessionSchemaVersion, SessionSnapshot, SourceExecutionState, SystemInputSnapshotRef,
    ToolCallState, ToolLedgerEntry, VerificationSummary, VerificationVerdict, WaitState,
    WaitTarget, admission_digest,
};
pub use serialization::{
    Id, JsonDigest, JsonObject, canonical_digest, canonical_digest_json, parse_json,
};
```

## `crates/wickle/src/policy.rs`

```rust
use std::{fmt, future::Future, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{
    ContractError, ErrorCode, ExecutionContext, Id, JsonDigest, JsonObject, ModelPurpose,
    PortFuture, Scope, VersionedRef, serialization::data_digest,
};

/// Final, bound tool inputs visible to the trusted policy implementation.
/// Serialized values require protected storage and must not enter model/UI logs.
#[derive(Clone, PartialEq, Serialize)]
pub struct ToolPolicyInput {
    /// Core call identity.
    pub call_id: Id,
    /// Exact tool identity and version.
    pub tool: VersionedRef,
    /// Pinned descriptor identity.
    pub descriptor_digest: JsonDigest,
    /// Binding identity computed by the trusted input binder.
    pub binding_digest: JsonDigest,
    execution_args: JsonObject,
    #[serde(skip_serializing_if = "Option::is_none")]
    approval: Option<ToolApproval>,
    #[serde(skip_serializing_if = "Option::is_none")]
    selection: Option<crate::ToolBindingRef>,
}

impl ToolPolicyInput {
    /// Own the binder's final arguments. The gate never invents missing IDs.
    pub fn new(
        call_id: Id,
        tool: VersionedRef,
        descriptor_digest: JsonDigest,
        binding_digest: JsonDigest,
        execution_args: JsonObject,
    ) -> Self {
        Self {
            call_id,
            tool,
            descriptor_digest,
            binding_digest,
            execution_args,
            approval: None,
            selection: None,
        }
    }
    /// Inspect the full arguments to check actual target existence and ownership.
    pub fn execution_args(&self) -> &JsonObject {
        &self.execution_args
    }
    /// A recorded approval of this exact binding. The current policy still decides
    /// whether the actor may execute; this evidence never overrides a Deny.
    pub fn approval(&self) -> Option<&ToolApproval> {
        self.approval.as_ref()
    }
    /// Original selected catalog tool or adapter binding/export. A model alias
    /// alone never identifies the authorized external connection.
    pub fn selection(&self) -> Option<&crate::ToolBindingRef> {
        self.selection.as_ref()
    }
    pub(crate) fn with_selection(mut self, selection: crate::ToolBindingRef) -> Self {
        self.selection = Some(selection);
        self
    }
    pub(crate) fn with_approval(mut self, receipt: &crate::ResumeReceipt) -> Self {
        self.approval = Some(ToolApproval {
            command_id: receipt.command.command_id.clone(),
            command_ref: receipt.command_ref.clone(),
            accepted_revision: receipt.accepted_revision,
            actor_ref: receipt.actor_ref.clone(),
            capability_grant_ref: receipt.capability_grant_ref.clone(),
        });
        self
    }
}

/// Core-validated evidence that an authenticated actor approved a fixed tool binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ToolApproval {
    command_id: Id,
    command_ref: crate::RecordRef,
    accepted_revision: u64,
    actor_ref: Id,
    capability_grant_ref: Id,
}
impl ToolApproval {
    /// Accepted command identity.
    pub fn command_id(&self) -> &Id {
        &self.command_id
    }
    /// Protected command record, for authorized auditing.
    pub fn command_ref(&self) -> &crate::RecordRef {
        &self.command_ref
    }
    /// Revision at which approval was committed.
    pub fn accepted_revision(&self) -> u64 {
        self.accepted_revision
    }
    /// Authenticated approver.
    pub fn actor_ref(&self) -> &Id {
        &self.actor_ref
    }
    /// Host grant checked when approval was accepted.
    pub fn capability_grant_ref(&self) -> &Id {
        &self.capability_grant_ref
    }
}

impl fmt::Debug for ToolPolicyInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolPolicyInput")
            .field("call_id", &self.call_id)
            .field("tool", &self.tool)
            .field("descriptor_digest", &self.descriptor_digest)
            .field("binding_digest", &self.binding_digest)
            .field("execution_args", &"<redacted>")
            .finish()
    }
}

/// Operation being authorized; data access and protected-detail access differ.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum PolicyAction {
    /// Admit a new run.
    StartRun {},
    /// Read minimal run metadata.
    ReadRun {},
    /// Read the protected checkpoint, separately from the public view.
    ReadRunDetails {},
    /// Resume a recorded wait or interruption.
    ResumeRun {
        /// Exact command, so authorization distinguishes approving, denying,
        /// answering and supplying an external receipt.
        command: Box<crate::ResumeCommand>,
        /// Fixed tool binding when the saved wait belongs to a tool.
        binding_digest: Option<JsonDigest>,
    },
    /// Request cancellation.
    CancelRun {},
    /// Read artifact data/metadata.
    ReadArtifact {},
    /// Write an artifact in the owning scope.
    WriteArtifact {},
    /// Read minimal event metadata.
    ReadEvents {},
    /// Read a protected record referenced by an event or checkpoint.
    ReadRecord {},
    /// Use scoped data in model context.
    UseContext {},
    /// Invoke one selected lifecycle hook under its pinned definition and target.
    InvokeHook {
        /// Exact selected hook version.
        hook: VersionedRef,
        /// Original adapter binding/export; absent for catalog hooks.
        selection: Option<crate::HookRef>,
        /// Immutable execution definition.
        definition_digest: JsonDigest,
        /// Exact lifecycle invocation scope within the Run.
        target: crate::HookTarget,
    },
    /// Resolve approved component metadata before admission, without opening a connection.
    ResolveComponents {
        /// Identity of the profile and metadata being assembled.
        profile_resolution_digest: JsonDigest,
    },
    /// Open one adapter for a scoped execution or observer segment.
    BindAdapter {
        /// Profile-local binding, independent of exported model aliases.
        binding_id: Id,
        /// Exact registered adapter implementation version.
        adapter: VersionedRef,
        /// Full pinned definition including export contracts.
        definition_digest: JsonDigest,
        /// Named Host account/connection revisions, without credentials.
        connections: std::collections::BTreeMap<Id, VersionedRef>,
        /// Fresh scope-bound execution segment identity.
        binding_set_id: Id,
        /// Whether business tools or only observers may be activated.
        purpose: crate::ComponentBindPurpose,
    },
    /// Read one registered system key before the final target value is known.
    ResolveSystemInput {
        /// Exact tool requesting the lookup.
        tool: VersionedRef,
        /// Original adapter export selection, absent for catalog tools.
        selection: Option<crate::ToolBindingRef>,
        /// Logical call whose binding is being prepared.
        call_id: Id,
        /// Pinned tool descriptor identity.
        descriptor_digest: JsonDigest,
        /// Compiled input contract identity.
        compiled_digest: JsonDigest,
        /// Exact registry key, never a path expression.
        key: Id,
        /// Pinned system-input definition revision.
        definition_version: Id,
        /// Exact read-only resolver implementation.
        resolver_ref: VersionedRef,
    },
    /// Send input to a selected model route.
    InvokeModel {
        /// Exact provider, target, model and connection metadata for current authorization.
        route: Box<crate::ResolvedModelRoute>,
        /// Purpose being authorized.
        purpose: ModelPurpose,
    },
    /// Dispatch one tool using final validated inputs.
    ExecuteTool {
        /// Final inputs, including system-owned parameters.
        input: ToolPolicyInput,
    },
}

/// An operation on an authoritative resource identity.
/// Obtain owner_scope from trusted stored metadata, not a caller's claimed scope.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PolicyRequest {
    /// Stored owner scope; user_id=None is not a wildcard.
    pub owner_scope: Scope,
    /// Run, artifact, event stream, record, context, or tool resource identity.
    pub resource_id: Id,
    /// Exact proposed action.
    pub action: PolicyAction,
}

impl PolicyRequest {
    /// Identity of the full proposed action and owning scope, including tool inputs.
    /// It excludes the approving principal so a new authorized reviewer can act.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }
}

/// Current authenticated policy context, without the run's whole system-input map.
pub struct PolicyContext<'a> {
    /// Current authenticated resource scope.
    pub scope: &'a Scope,
    /// Current principal, distinct from resource scope and original tool inputs.
    pub principal_ref: &'a Id,
    /// Current grant reference; the Host checks membership and revocation.
    pub capability_grant_ref: &'a Id,
    /// Cooperative cancellation signal.
    pub cancellation: &'a CancellationToken,
    /// Effective policy deadline on the monotonic clock.
    pub deadline: Instant,
}

/// Host authorization decision. A reason is an informational code, not a grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
pub enum PolicyDecision {
    /// This exact action is currently allowed.
    Allow {},
    /// The action is denied.
    Deny {
        /// Safe reason code, without bound values or SDK error text.
        reason: Id,
    },
    /// The action requires an approval flow before it can be performed.
    RequireApproval {
        /// Safe reason code.
        reason: Id,
    },
}

impl PolicyDecision {
    /// Intersect a Host decision with a restriction; an allow never removes a denial
    /// or an approval requirement. Existing Host reasons take precedence.
    pub fn restrict(self, restriction: Self) -> Self {
        match (self, restriction) {
            (denied @ Self::Deny { .. }, _) | (_, denied @ Self::Deny { .. }) => denied,
            (approval @ Self::RequireApproval { .. }, _)
            | (_, approval @ Self::RequireApproval { .. }) => approval,
            _ => Self::Allow {},
        }
    }
}

/// Trusted Host policy. Implement actual resource/membership/FK checks here.
pub trait PolicyPort: Send + Sync {
    /// Check the current grant against the exact bound action without performing it.
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision>;
}

/// An approval request bound to an exact action, not a reusable permission token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApprovalChallenge {
    /// Scope whose resource will be affected.
    pub scope: Scope,
    /// Resource identity.
    pub resource_id: Id,
    /// Digest includes final tool input, descriptor/version, and scope.
    pub request_digest: JsonDigest,
    /// Safe reason code for the Host's approval UI.
    pub reason: Id,
}

/// Result of a guarded operation. Approval-required never invokes the operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Guarded<T> {
    /// Operation completed after a current authorization check.
    Completed(T),
    /// No operation was invoked; the Host/runtime must handle this approval request.
    ApprovalRequired(ApprovalChallenge),
}

/// Current authorization with exact scope matching, timeout, and cancellation.
/// This does not authenticate caller-supplied JSON or provide a sandbox for Host code.
pub struct PolicyGate {
    policy: Arc<dyn PolicyPort>,
    timeout: Duration,
}

impl PolicyGate {
    /// Configure a finite, positive policy timeout without creating a runtime.
    pub fn new(policy: Arc<dyn PolicyPort>, timeout: Duration) -> Result<Self, ContractError> {
        if timeout.is_zero() || Instant::now().checked_add(timeout).is_none() {
            return Err(ContractError::new(
                ErrorCode::InvalidContract,
                "policy.timeout",
            ));
        }
        Ok(Self { policy, timeout })
    }

    /// Check the current Host decision. Every call rechecks policy; permits are not cached.
    pub async fn check(
        &self,
        request: &PolicyRequest,
        context: &ExecutionContext,
        deadline: Option<Instant>,
        restriction: Option<PolicyDecision>,
    ) -> Result<PolicyDecision, ContractError> {
        if request.owner_scope != context.data.scope {
            return Err(ContractError::new(ErrorCode::AccessDenied, "scope"));
        }
        if context.cancellation.is_cancelled() {
            return Err(ContractError::new(ErrorCode::Cancelled, "policy"));
        }
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(ContractError::new(ErrorCode::RuntimeUnavailable, "policy"));
        }
        let now = Instant::now();
        let policy_deadline = now
            .checked_add(self.timeout)
            .ok_or_else(|| ContractError::new(ErrorCode::InvalidContract, "policy.timeout"))?;
        let effective = deadline.map_or(policy_deadline, |d| d.min(policy_deadline));
        if effective <= now {
            return Err(ContractError::new(ErrorCode::DeadlineExceeded, "policy"));
        }
        let decision = AssertUnwindSafe(async {
            tokio::select! {
                biased;
                _ = context.cancellation.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "policy")),
                _ = tokio::time::sleep_until(effective) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "policy")),
                result = self.policy.authorize(request, PolicyContext {
                    scope: &context.data.scope, principal_ref: &context.data.principal_ref,
                    capability_grant_ref: &context.data.capability_grant_ref,
                    cancellation: &context.cancellation, deadline: effective,
                }) => result.map_err(|_| ContractError::new(ErrorCode::PolicyUnavailable, "policy")),
            }
        }).catch_unwind().await.map_err(|_| ContractError::new(ErrorCode::PolicyUnavailable, "policy"))??;
        if context.cancellation.is_cancelled() {
            return Err(ContractError::new(ErrorCode::Cancelled, "policy"));
        }
        if Instant::now() >= effective {
            return Err(ContractError::new(ErrorCode::DeadlineExceeded, "policy"));
        }
        Ok(match restriction {
            Some(other) => decision.restrict(other),
            None => decision,
        })
    }

    /// Invoke a closure only after current policy allows it. Future construction is
    /// also delayed until authorization. The operation owns its I/O cancellation and
    /// effect reconciliation; dropping a future is not treated as external rollback.
    pub async fn guard<T, F, Fut>(
        &self,
        request: &PolicyRequest,
        context: &ExecutionContext,
        deadline: Option<Instant>,
        restriction: Option<PolicyDecision>,
        operation: F,
    ) -> Result<Guarded<T>, ContractError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, ContractError>>,
    {
        match self.check(request, context, deadline, restriction).await? {
            PolicyDecision::Allow {} => operation().await.map(Guarded::Completed),
            PolicyDecision::Deny { .. } => {
                Err(ContractError::new(ErrorCode::AccessDenied, "policy"))
            }
            PolicyDecision::RequireApproval { reason } => {
                Ok(Guarded::ApprovalRequired(ApprovalChallenge {
                    scope: request.owner_scope.clone(),
                    resource_id: request.resource_id.clone(),
                    request_digest: request.digest(),
                    reason,
                }))
            }
        }
    }
}
```

## `crates/wickle/src/state.rs`

```rust
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroU64,
    sync::{Mutex, MutexGuard},
};

use serde::de::DeserializeOwned;
use serde_json::Value;

mod checkpoint;
mod hook_state;
pub use checkpoint::{STATE_STORE_CHECKPOINT_VERSION, StateStoreCheckpoint};
use hook_state::{validate_hook_observation, validate_hook_snapshot, validate_hook_transition};

use crate::{
    ApprovalTarget, BudgetUsage, ContentBlock, ContractError, ErrorCode, Id, Message,
    ModelAttemptState, ModelExchangeOutcome, ModelFinish, ModelInvocationRecord, OutcomeResult,
    PortFuture, RecordRef, ResumeAction, ResumeCommand, RunEvent, RunEventPayload, RunPhase,
    RunSnapshot, RunStatus, Scope, SessionSchemaVersion, SessionSnapshot, StoredModelResponse,
    ToolCall, ToolCallState, ToolResult, VerificationSummary, WaitState, WaitTarget,
    admission_digest, canonical_digest,
};

/// Guarantees offered by a state-store implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateStoreCapabilities {
    /// Records survive process termination.
    pub durable: bool,
    /// Execution leases coordinate independent processes.
    pub cross_process_leases: bool,
    /// Committed events can be replayed in sequence order.
    pub event_replay: bool,
}

/// Immutable, scope-owned data stored with its referencing state and events.
/// Access requires Host authorization; Debug never prints the payload.
#[derive(Clone, PartialEq)]
pub struct ProtectedRecord {
    reference: RecordRef,
    value: Value,
}

impl ProtectedRecord {
    /// Compute the reference digest from owned data. A revision is immutable.
    pub fn new(record_id: Id, revision: u64, value: Value) -> Self {
        Self {
            reference: RecordRef {
                record_id,
                revision,
                digest: canonical_digest(&value),
            },
            value,
        }
    }

    /// Exact immutable record identity, without its payload.
    pub fn reference(&self) -> &RecordRef {
        &self.reference
    }

    /// Explicit privileged access, never an automatic public/model projection.
    pub fn value(&self) -> &Value {
        &self.value
    }
}

impl fmt::Debug for ProtectedRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtectedRecord")
            .field("reference", &self.reference)
            .field("value", &"<redacted>")
            .finish()
    }
}

/// Initial records accepted atomically for a newly admitted run.
#[derive(Clone)]
pub struct AdmissionInput {
    /// Running/admission checkpoint at revision zero.
    pub snapshot: RunSnapshot,
    /// Session-pinned prompt record; reused unchanged by subsequent runs.
    pub prompt_snapshot: RecordRef,
    /// New messages, numbered consecutively across the session.
    pub messages: Vec<Message>,
    /// One run.started event at sequence one, referencing the accepted request.
    pub events: Vec<RunEvent>,
    /// New immutable records, available to references in this transaction.
    pub records: Vec<ProtectedRecord>,
    /// Reject implementations that cannot preserve state across process termination.
    pub require_durable: bool,
}

/// An owned protected checkpoint and its complete session transcript.
/// Use PolicyGate views to select data for less privileged callers.
#[derive(Clone, PartialEq)]
pub struct StoredRun {
    /// Current run checkpoint and protected record references.
    pub snapshot: RunSnapshot,
    /// Current session metadata, including its active run.
    pub session: SessionSnapshot,
    /// Append-only session transcript, including messages from earlier runs.
    pub messages: Vec<Message>,
}

impl fmt::Debug for StoredRun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredRun")
            .field("run_id", &self.snapshot.run_id)
            .field("revision", &self.snapshot.revision)
            .field("message_count", &self.messages.len())
            .finish_non_exhaustive()
    }
}

/// Admission reports whether it created a run or found the original request.
#[derive(Debug, Clone, PartialEq)]
pub struct AdmissionResult {
    /// False for identical request replay; candidate records are not applied.
    pub created: bool,
    /// Existing or newly admitted run, with its original pinned data.
    pub state: StoredRun,
}

/// Store-issued lease identity. Possession is not Host authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLease {
    /// Exact resource namespace.
    pub scope: Scope,
    /// Run owned by this lease.
    pub run_id: Id,
    /// Worker identity supplied by trusted runtime code.
    pub owner: Id,
    /// Increasing generation retained across expiration and release.
    pub fencing_token: u64,
    /// Expiration reported when issued. Validation uses the store's current expiry,
    /// so renewal does not invalidate copies of the same owner/fencing generation.
    pub expires_at_ms: i64,
}

/// A complete candidate checkpoint and append-only data for one atomic commit.
#[derive(Clone)]
pub struct CommitInput {
    /// Compare-and-swap revision of the currently saved checkpoint.
    pub expected_revision: u64,
    /// Current unexpired execution lease.
    pub lease: RunLease,
    /// Trusted current UTC milliseconds, also used to reject expired leases.
    pub now_ms: i64,
    /// Next checkpoint, at expected_revision + 1.
    pub snapshot: RunSnapshot,
    /// New messages, continuing the session sequence.
    pub messages: Vec<Message>,
    /// New events, continuing the run sequence.
    pub events: Vec<RunEvent>,
    /// Immutable records to insert in the same transaction.
    pub records: Vec<ProtectedRecord>,
}

/// A bounded, ordered page of protected durable events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventPage {
    /// Events strictly after the supplied cursor.
    pub events: Vec<RunEvent>,
    /// Cursor for the next page, unchanged for an empty page.
    pub next_after_seq: u64,
    /// More events were available when this page was read.
    pub has_more: bool,
    /// Oldest retained sequence; None when no events are stored.
    pub first_available_seq: Option<NonZeroU64>,
    /// Latest committed sequence when this page was read.
    pub last_available_seq: u64,
}

/// Largest event page accepted by the reference store.
pub const MAX_EVENT_PAGE_SIZE: usize = 1_000;

/// Trusted core storage port. Scope isolation is enforced by the store itself.
/// The facade separately applies current PolicyGate authorization. No raw load or
/// record reference grants permission to publish the returned data.
pub trait StateStore: Send + Sync {
    /// Describe storage and coordination guarantees.
    fn capabilities(&self) -> StateStoreCapabilities;
    /// Find the original request before re-resolving current profile or routing metadata.
    /// Missing scope/request returns None. Atomic admission remains the final deduplication boundary.
    fn find_request<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
        request_id: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>>;
    /// Atomically deduplicate a request and reserve its session's active-run slot.
    fn admit<'a>(
        &'a self,
        scope: &'a Scope,
        input: AdmissionInput,
    ) -> PortFuture<'a, AdmissionResult>;
    /// Load owned state and the complete session transcript.
    fn load<'a>(&'a self, scope: &'a Scope, run_id: &'a Id) -> PortFuture<'a, StoredRun>;
    /// Read session-pinned metadata without changing its active run.
    fn load_session<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
    ) -> PortFuture<'a, SessionSnapshot>;
    /// Validate owner/generation against the current stored expiry without renewing.
    /// Return the latest lease metadata, including any concurrent heartbeat renewal.
    fn check_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, RunLease>;
    /// Acquire a new generation after any previous lease has expired or been released.
    fn acquire_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        owner: &'a Id,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease>;
    /// Renew an unexpired generation; an expired lease cannot be revived.
    fn renew_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease>;
    /// Release only the currently owned unexpired generation.
    fn release_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, ()>;
    /// Validate and commit state, transcript, records and events atomically.
    fn commit<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun>;
    /// Replay a bounded page. Retention gaps must not silently skip missing events.
    fn read_events<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        after_seq: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage>;
    /// Read an exact scope-owned immutable record after separate Host authorization.
    fn read_record<'a>(
        &'a self,
        scope: &'a Scope,
        reference: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord>;
    /// Append a report about an already committed result without changing its
    /// outcome, snapshot revision, session ownership, or durable event sequence.
    fn record_hook_observation<'a>(
        &'a self,
        _scope: &'a Scope,
        _run_id: &'a Id,
        _report: crate::HookObservation,
    ) -> PortFuture<'a, ()> {
        Box::pin(async {
            Err(error(
                ErrorCode::CapabilityUnsupported,
                "store.hook_observations",
            ))
        })
    }
    /// Read protected lifecycle observation reports after Host authorization.
    fn read_hook_observations<'a>(
        &'a self,
        _scope: &'a Scope,
        _run_id: &'a Id,
    ) -> PortFuture<'a, Vec<crate::HookObservation>> {
        Box::pin(async {
            Err(error(
                ErrorCode::CapabilityUnsupported,
                "store.hook_observations",
            ))
        })
    }
}

type ScopeKey = (Id, Id, Option<Id>);
type RecordKey = (Id, u64);

#[derive(Clone, Default)]
struct ScopeState {
    sessions: BTreeMap<Id, SessionState>,
    runs: BTreeMap<Id, RunState>,
    requests: BTreeMap<(Id, Id), Id>,
    records: BTreeMap<RecordKey, ProtectedRecord>,
    event_ids: BTreeSet<Id>,
    message_ids: BTreeSet<Id>,
    hook_observations: BTreeMap<Id, Vec<crate::HookObservation>>,
}

#[derive(Clone)]
struct SessionState {
    snapshot: SessionSnapshot,
    messages: Vec<Message>,
}

#[derive(Clone)]
struct RunState {
    snapshot: RunSnapshot,
    events: Vec<RunEvent>,
    lease: Option<RunLease>,
    last_fencing_token: u64,
}

/// Process-local reference store. It retains all committed data for its lifetime.
/// A single short critical section validates and applies each transaction; no
/// external calls or awaits occur while the lock is held. It provides neither
/// process-restart durability nor coordination between separate processes.
#[derive(Default)]
pub struct MemoryStateStore {
    scopes: Mutex<BTreeMap<ScopeKey, ScopeState>>,
}

impl MemoryStateStore {
    /// Construct an empty store without creating a runtime or doing I/O.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<MutexGuard<'_, BTreeMap<ScopeKey, ScopeState>>, ContractError> {
        self.scopes
            .lock()
            .map_err(|_| error(ErrorCode::PersistenceUnavailable, "state_store"))
    }
}

impl StateStore for MemoryStateStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        StateStoreCapabilities {
            durable: false,
            cross_process_leases: false,
            event_replay: true,
        }
    }

    fn find_request<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
        request_id: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        Box::pin(async move {
            let scopes = self.lock()?;
            let Some(state) = scopes.get(&scope_key(scope)) else {
                return Ok(None);
            };
            state
                .requests
                .get(&(session_id.clone(), request_id.clone()))
                .map(|run_id| stored_run(state, run_id))
                .transpose()
        })
    }

    fn admit<'a>(
        &'a self,
        scope: &'a Scope,
        input: AdmissionInput,
    ) -> PortFuture<'a, AdmissionResult> {
        Box::pin(async move {
            if input.require_durable {
                return Err(error(ErrorCode::CapabilityUnsupported, "store.durable"));
            }
            check_scope(scope, &input.snapshot.scope)?;
            if scope != input.snapshot.profile.scope()
                || input.snapshot.request_digest
                    != admission_digest(
                        &input.snapshot.request,
                        &input.snapshot.profile,
                        input.snapshot.system_inputs.as_ref(),
                    )
            {
                return Err(error(ErrorCode::InvalidSnapshot, "request_digest"));
            }
            let mut scopes = self.lock()?;
            let empty = ScopeState::default();
            let state = scopes.get(&scope_key(scope)).unwrap_or(&empty);
            let request_key = (
                input.snapshot.request.session_id.clone(),
                input.snapshot.request.request_id.clone(),
            );
            if let Some(run_id) = state.requests.get(&request_key) {
                let previous = stored_run(state, run_id)?;
                if previous.snapshot.request_digest != input.snapshot.request_digest {
                    return Err(error(ErrorCode::RequestConflict, "request"));
                }
                return Ok(AdmissionResult {
                    created: false,
                    state: previous,
                });
            }
            input.snapshot.validate()?;
            if input.snapshot.revision != 0
                || input.snapshot.status != RunStatus::Running
                || input.snapshot.phase != RunPhase::Admission
                || !input.snapshot.model_ledger.is_empty()
                || !input.snapshot.tool_ledger.is_empty()
                || !input.snapshot.reservations.is_empty()
                || !input.snapshot.resume_receipts.is_empty()
                || !input.snapshot.hook_applications.is_empty()
                || input.snapshot.usage != BudgetUsage::default()
            {
                return Err(error(ErrorCode::InvalidSnapshot, "admission"));
            }
            if state.runs.contains_key(&input.snapshot.run_id) {
                return Err(error(ErrorCode::RunConflict, "run_id"));
            }
            let session_id = &input.snapshot.request.session_id;
            let previous_session = state.sessions.get(session_id);
            if let Some(session) = previous_session {
                if session.snapshot.profile_digest != *input.snapshot.profile.profile_digest()
                    || session.snapshot.prompt_snapshot != input.prompt_snapshot
                {
                    return Err(error(ErrorCode::ProfileMismatch, "session.profile"));
                }
                if session.snapshot.active_run_id.is_some() {
                    return Err(error(ErrorCode::SessionBusy, "session"));
                }
            }
            let additions = validate_records(state, &input.records)?;
            record_value(state, &additions, &input.prompt_snapshot)?;
            validate_snapshot_refs(state, &additions, &input.snapshot)?;
            validate_events(
                state,
                &additions,
                &input.snapshot,
                0,
                &input.events,
                true,
                &input.messages,
            )?;
            let previous_sequence = previous_session.map_or(0, |s| s.snapshot.transcript_revision);
            let transcript_revision = validate_messages(
                state,
                &additions,
                &input.snapshot.run_id,
                previous_sequence,
                &input.messages,
            )?;
            let mut messages = previous_session.map_or_else(Vec::new, |s| s.messages.clone());
            messages.extend(input.messages);
            let session = SessionSnapshot {
                schema_version: SessionSchemaVersion::V1,
                session_id: session_id.clone(),
                scope: scope.clone(),
                profile_digest: input.snapshot.profile.profile_digest().clone(),
                prompt_snapshot: input.prompt_snapshot,
                transcript_revision,
                active_run_id: Some(input.snapshot.run_id.clone()),
            };
            let result = StoredRun {
                snapshot: input.snapshot.clone(),
                session: session.clone(),
                messages: messages.clone(),
            };
            // All fallible checks precede these mutations.
            let state = scopes.entry(scope_key(scope)).or_default();
            state.records.extend(additions);
            state
                .message_ids
                .extend(messages.iter().map(|m| m.message_id.clone()));
            state
                .event_ids
                .extend(input.events.iter().map(|e| e.event_id.clone()));
            state
                .requests
                .insert(request_key, input.snapshot.run_id.clone());
            state.sessions.insert(
                session.session_id.clone(),
                SessionState {
                    snapshot: session,
                    messages,
                },
            );
            state.runs.insert(
                input.snapshot.run_id.clone(),
                RunState {
                    snapshot: input.snapshot,
                    events: input.events,
                    lease: None,
                    last_fencing_token: 0,
                },
            );
            Ok(AdmissionResult {
                created: true,
                state: result,
            })
        })
    }

    fn load<'a>(&'a self, scope: &'a Scope, run_id: &'a Id) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            let scopes = self.lock()?;
            stored_run(namespace(&scopes, scope)?, run_id)
        })
    }

    fn load_session<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
    ) -> PortFuture<'a, SessionSnapshot> {
        Box::pin(async move {
            let scopes = self.lock()?;
            namespace(&scopes, scope)?
                .sessions
                .get(session_id)
                .map(|session| session.snapshot.clone())
                .ok_or_else(not_found)
        })
    }

    fn check_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, RunLease> {
        Box::pin(async move {
            let scopes = self.lock()?;
            let run = namespace(&scopes, scope)?
                .runs
                .get(run_id)
                .ok_or_else(not_found)?;
            validate_lease(run, scope, run_id, lease, now_ms)?;
            Ok(run.lease.as_ref().expect("validated lease").clone())
        })
    }

    fn acquire_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        owner: &'a Id,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease> {
        Box::pin(async move {
            let expires_at_ms = lease_expiry(now_ms, ttl_ms)?;
            let mut scopes = self.lock()?;
            let run = run_mut(&mut scopes, scope, run_id)?;
            if run.snapshot.status.is_terminal() {
                return Err(error(ErrorCode::InvalidTransition, "run.status"));
            }
            if run.lease.as_ref().is_some_and(|l| l.expires_at_ms > now_ms) {
                return Err(error(ErrorCode::LeaseBusy, "lease"));
            }
            let fencing_token = run
                .last_fencing_token
                .checked_add(1)
                .ok_or_else(|| error(ErrorCode::InvalidContract, "lease.fencing_token"))?;
            let lease = RunLease {
                scope: scope.clone(),
                run_id: run_id.clone(),
                owner: owner.clone(),
                fencing_token,
                expires_at_ms,
            };
            run.last_fencing_token = fencing_token;
            run.lease = Some(lease.clone());
            Ok(lease)
        })
    }

    fn renew_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease> {
        Box::pin(async move {
            let expires_at_ms = lease_expiry(now_ms, ttl_ms)?;
            let mut scopes = self.lock()?;
            let run = run_mut(&mut scopes, scope, run_id)?;
            validate_lease(run, scope, run_id, lease, now_ms)?;
            let renewed = RunLease {
                expires_at_ms,
                ..lease.clone()
            };
            run.lease = Some(renewed.clone());
            Ok(renewed)
        })
    }

    fn release_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let mut scopes = self.lock()?;
            let run = run_mut(&mut scopes, scope, run_id)?;
            validate_lease(run, scope, run_id, lease, now_ms)?;
            run.lease = None;
            Ok(())
        })
    }

    fn commit<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            check_scope(scope, &input.snapshot.scope)?;
            let mut scopes = self.lock()?;
            let state = namespace(&scopes, scope)?;
            let run = state.runs.get(run_id).ok_or_else(not_found)?;
            validate_lease(run, scope, run_id, &input.lease, input.now_ms)?;
            if run.snapshot.revision != input.expected_revision {
                return Err(error(ErrorCode::RevisionConflict, "revision"));
            }
            validate_transition(&run.snapshot, &input.snapshot)?;
            if input.events.iter().any(|event| {
                matches!(event.payload, RunEventPayload::RunResumed { .. })
                    && event.timestamp_ms > input.now_ms
            }) {
                return Err(error(ErrorCode::InvalidEvent, "events.resume_time"));
            }
            if let Some(receipt) = input
                .snapshot
                .resume_receipts
                .last()
                .filter(|receipt| receipt.accepted_revision == input.snapshot.revision)
            {
                let expired = input.now_ms >= run.snapshot.timing.deadline_at_ms
                    || run
                        .snapshot
                        .wait
                        .as_ref()
                        .and_then(|wait| wait.expires_at_ms)
                        .is_some_and(|deadline| input.now_ms >= deadline);
                // A durable adapter may advance the lease-check time after
                // queue/lock delay. Crossing expiry must not turn a stale
                // on-time decision into an accepted approval.
                if receipt.expired != expired {
                    return Err(error(
                        ErrorCode::DeadlineExceeded,
                        "resume.acceptance_expiry",
                    ));
                }
            }
            let additions = validate_records(state, &input.records)?;
            validate_snapshot_refs(state, &additions, &input.snapshot)?;
            validate_events(
                state,
                &additions,
                &input.snapshot,
                run.snapshot.last_event_seq,
                &input.events,
                false,
                &input.messages,
            )?;
            let session = state
                .sessions
                .get(&run.snapshot.request.session_id)
                .ok_or_else(not_found)?;
            if session.snapshot.active_run_id.as_ref() != Some(run_id) {
                return Err(error(ErrorCode::InvalidTransition, "session.active_run_id"));
            }
            let transcript_revision = validate_messages(
                state,
                &additions,
                run_id,
                session.snapshot.transcript_revision,
                &input.messages,
            )?;
            let mut session_snapshot = session.snapshot.clone();
            let history: Vec<_> = run.events.iter().chain(&input.events).collect();
            let transcript: Vec<_> = session.messages.iter().chain(&input.messages).collect();
            validate_resume_history(state, &additions, &input.snapshot, &history, &transcript)?;
            session_snapshot.transcript_revision = transcript_revision;
            if input.snapshot.status.is_terminal() {
                session_snapshot.active_run_id = None;
            }
            let mut messages = session.messages.clone();
            messages.extend(input.messages);
            let result = StoredRun {
                snapshot: input.snapshot.clone(),
                session: session_snapshot.clone(),
                messages: messages.clone(),
            };
            let state = scopes
                .get_mut(&scope_key(scope))
                .expect("validated namespace");
            state.records.extend(additions);
            state
                .message_ids
                .extend(messages.iter().map(|m| m.message_id.clone()));
            state
                .event_ids
                .extend(input.events.iter().map(|e| e.event_id.clone()));
            state.sessions.insert(
                session_snapshot.session_id.clone(),
                SessionState {
                    snapshot: session_snapshot,
                    messages,
                },
            );
            let run = state.runs.get_mut(run_id).expect("validated run");
            run.snapshot = input.snapshot;
            run.events.extend(input.events);
            if run.snapshot.status.is_terminal() {
                run.lease = None;
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
        Box::pin(async move {
            if limit == 0 || limit > MAX_EVENT_PAGE_SIZE {
                return Err(error(ErrorCode::InvalidContract, "events.limit"));
            }
            let scopes = self.lock()?;
            let run = namespace(&scopes, scope)?
                .runs
                .get(run_id)
                .ok_or_else(not_found)?;
            let mut available = run.events.iter().filter(|e| e.seq.get() > after_seq);
            let events: Vec<_> = available.by_ref().take(limit).cloned().collect();
            Ok(EventPage {
                next_after_seq: events.last().map_or(after_seq, |e| e.seq.get()),
                has_more: available.next().is_some(),
                first_available_seq: run.events.first().map(|e| e.seq),
                last_available_seq: run.snapshot.last_event_seq,
                events,
            })
        })
    }

    fn read_record<'a>(
        &'a self,
        scope: &'a Scope,
        reference: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        Box::pin(async move {
            let scopes = self.lock()?;
            let record = namespace(&scopes, scope)?
                .records
                .get(&record_key(reference))
                .ok_or_else(not_found)?;
            if record.reference != *reference {
                return Err(error(ErrorCode::RecordConflict, "record.reference"));
            }
            Ok(record.clone())
        })
    }
    fn record_hook_observation<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        report: crate::HookObservation,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let mut scopes = self.lock()?;
            let state = scopes.get_mut(&scope_key(scope)).ok_or_else(not_found)?;
            validate_hook_observation(state, scope, run_id, &report)?;
            let reports = state.hook_observations.entry(run_id.clone()).or_default();
            if let Some(existing) = reports.iter().find(|existing| {
                existing.hook == report.hook
                    && existing.selection == report.selection
                    && existing.target == report.target
            }) {
                return if existing == &report {
                    Ok(())
                } else {
                    Err(error(ErrorCode::RecordConflict, "hooks.observation"))
                };
            }
            reports.push(report);
            Ok(())
        })
    }
    fn read_hook_observations<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
    ) -> PortFuture<'a, Vec<crate::HookObservation>> {
        Box::pin(async move {
            let scopes = self.lock()?;
            let state = namespace(&scopes, scope)?;
            if !state.runs.contains_key(run_id) {
                return Err(not_found());
            }
            Ok(state
                .hook_observations
                .get(run_id)
                .cloned()
                .unwrap_or_default())
        })
    }
}

fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}

fn not_found() -> ContractError {
    error(ErrorCode::StateNotFound, "state")
}

fn scope_key(scope: &Scope) -> ScopeKey {
    (
        scope.tenant_id.clone(),
        scope.workspace_id.clone(),
        scope.user_id.clone(),
    )
}

fn record_key(reference: &RecordRef) -> RecordKey {
    (reference.record_id.clone(), reference.revision)
}

fn check_scope(expected: &Scope, actual: &Scope) -> Result<(), ContractError> {
    if expected != actual {
        Err(error(ErrorCode::AccessDenied, "scope"))
    } else {
        Ok(())
    }
}

fn namespace<'a>(
    scopes: &'a BTreeMap<ScopeKey, ScopeState>,
    scope: &Scope,
) -> Result<&'a ScopeState, ContractError> {
    scopes.get(&scope_key(scope)).ok_or_else(not_found)
}

fn run_mut<'a>(
    scopes: &'a mut BTreeMap<ScopeKey, ScopeState>,
    scope: &Scope,
    run_id: &Id,
) -> Result<&'a mut RunState, ContractError> {
    scopes
        .get_mut(&scope_key(scope))
        .and_then(|state| state.runs.get_mut(run_id))
        .ok_or_else(not_found)
}

fn stored_run(state: &ScopeState, run_id: &Id) -> Result<StoredRun, ContractError> {
    let run = state.runs.get(run_id).ok_or_else(not_found)?;
    let session = state
        .sessions
        .get(&run.snapshot.request.session_id)
        .ok_or_else(not_found)?;
    Ok(StoredRun {
        snapshot: run.snapshot.clone(),
        session: session.snapshot.clone(),
        messages: session.messages.clone(),
    })
}

fn lease_expiry(now_ms: i64, ttl_ms: u64) -> Result<i64, ContractError> {
    let ttl = i64::try_from(ttl_ms)
        .ok()
        .filter(|ttl| *ttl > 0)
        .ok_or_else(|| error(ErrorCode::InvalidContract, "lease.ttl_ms"))?;
    now_ms
        .checked_add(ttl)
        .ok_or_else(|| error(ErrorCode::InvalidContract, "lease.expires_at_ms"))
}

fn validate_lease(
    run: &RunState,
    scope: &Scope,
    run_id: &Id,
    provided: &RunLease,
    now_ms: i64,
) -> Result<(), ContractError> {
    if &provided.scope != scope
        || &provided.run_id != run_id
        || !run.lease.as_ref().is_some_and(|stored| {
            stored.owner == provided.owner
                && stored.fencing_token == provided.fencing_token
                && now_ms < stored.expires_at_ms
        })
    {
        return Err(error(ErrorCode::LeaseLost, "lease"));
    }
    Ok(())
}

fn validate_records(
    state: &ScopeState,
    records: &[ProtectedRecord],
) -> Result<BTreeMap<RecordKey, ProtectedRecord>, ContractError> {
    let mut additions = BTreeMap::new();
    for record in records {
        let key = record_key(&record.reference);
        if state
            .records
            .get(&key)
            .or_else(|| additions.get(&key))
            .is_some_and(|existing| existing != record)
        {
            return Err(error(ErrorCode::RecordConflict, "records"));
        }
        additions.insert(key, record.clone());
    }
    Ok(additions)
}

fn record_value<'a>(
    state: &'a ScopeState,
    additions: &'a BTreeMap<RecordKey, ProtectedRecord>,
    reference: &RecordRef,
) -> Result<&'a Value, ContractError> {
    let record = additions
        .get(&record_key(reference))
        .or_else(|| state.records.get(&record_key(reference)))
        .ok_or_else(not_found)?;
    if &record.reference != reference {
        return Err(error(ErrorCode::RecordConflict, "record.reference"));
    }
    Ok(&record.value)
}

fn validate_snapshot_refs(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    validate_hook_snapshot(state, additions, snapshot)?;
    let mut references = Vec::new();
    for receipt in &snapshot.resume_receipts {
        let command: ResumeCommand = event_record(state, additions, &receipt.command_ref)?;
        let outcome: crate::RunOutcome =
            event_record(state, additions, &receipt.previous_outcome_ref)?;
        let crate::OutcomeResult::Waiting { wait } = &outcome.result else {
            return Err(error(ErrorCode::InvalidSnapshot, "resume_receipts.outcome"));
        };
        outcome.validate()?;
        if command != receipt.command
            || outcome.checkpoint_revision != command.expected_revision
            || !action_matches_wait(wait, &command.action)
        {
            return Err(error(ErrorCode::InvalidSnapshot, "resume_receipts.records"));
        }
        for reference in &outcome.unresolved_effects {
            record_value(state, additions, reference)?;
        }
    }
    if let Some(reference) = &snapshot.routing_snapshot_ref {
        let value = record_value(state, additions, reference)?;
        let routing = crate::RoutingSnapshot::restore(
            &serde_json::to_string(value)
                .map_err(|_| error(ErrorCode::InvalidSnapshot, "routing"))?,
            &snapshot.scope,
            &reference.digest,
        )?;
        for invocation in &snapshot.model_ledger {
            routing.validate_route(&invocation.route)?;
            if invocation.inspection_ref.is_none()
                || !routing.policy().rules.iter().any(|rule| {
                    rule.model_binding == snapshot.profile.profile().model_binding
                        && rule.purpose == invocation.purpose
                        && (rule.primary == invocation.route.binding
                            || rule.fallbacks.contains(&invocation.route.binding))
                })
            {
                return Err(error(ErrorCode::InvalidSnapshot, "routing.invocation"));
            }
            let reference = invocation
                .inspection_ref
                .as_ref()
                .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "routing.inspection"))?;
            let observation: crate::ModelRouteObservation =
                serde_json::from_value(record_value(state, additions, reference)?.clone())
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "routing.inspection"))?;
            let require_pinned = invocation.route.version_semantics
                == crate::VersionSemantics::Pinned
                || routing.policy().rules.iter().any(|rule| {
                    rule.model_binding == snapshot.profile.profile().model_binding
                        && rule.purpose == invocation.purpose
                        && rule.version_policy == crate::VersionPolicy::RequirePinned
                });
            observation.validate(
                &invocation.route,
                if require_pinned {
                    crate::VersionPolicy::RequirePinned
                } else {
                    crate::VersionPolicy::AllowMutable
                },
            )?;
        }
    }
    let run_inputs = snapshot
        .system_inputs
        .as_ref()
        .map(|inputs| {
            crate::RunSystemInputs::from_value(
                record_value(state, additions, &inputs.snapshot_ref)?,
                inputs,
                &snapshot.scope,
            )
            .map_err(|_| error(ErrorCode::InvalidSnapshot, "system_inputs"))
        })
        .transpose()?;
    for invocation in &snapshot.model_ledger {
        if let Some(reference) = invocation
            .inspection_ref
            .as_ref()
            .filter(|_| snapshot.routing_snapshot_ref.is_none())
        {
            let observation: crate::ModelRouteObservation =
                serde_json::from_value(record_value(state, additions, reference)?.clone())
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "model.inspection"))?;
            observation.validate(&invocation.route, crate::VersionPolicy::AllowMutable)?;
        }
        if let Some(reference) = &invocation.response_ref {
            validate_model_response(state, additions, invocation, reference)?;
        }
    }
    if let Some(reference) = &snapshot.assembly_ref {
        let registry = crate::SystemInputRegistry::new(
            run_inputs
                .as_ref()
                .map(|inputs| inputs.definitions().values().cloned().collect())
                .unwrap_or_default(),
        )?;
        let value = record_value(state, additions, reference)?;
        let assembly = crate::ResolvedAssembly::restore(
            &serde_json::to_string(value)
                .map_err(|_| error(ErrorCode::InvalidSnapshot, "assembly"))?,
            &snapshot.profile,
            &registry,
            &reference.digest,
        )?;
        if assembly.session_id() != &snapshot.request.session_id {
            return Err(error(ErrorCode::InvalidSnapshot, "assembly.session"));
        }
        if let Some(session) = state.sessions.get(&snapshot.request.session_id) {
            let value = record_value(state, additions, &session.snapshot.prompt_snapshot)?;
            let prompt = crate::PromptSnapshot::restore(
                &serde_json::to_string(value)
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "assembly.prompt"))?,
                &session.snapshot.prompt_snapshot.digest,
                &snapshot.profile,
                &snapshot.scope,
            )?;
            if prompt.tools().len() != assembly.tools().len()
                || prompt
                    .tools()
                    .iter()
                    .zip(assembly.tools())
                    .any(|(pinned, binding)| {
                        pinned.selection != binding.selection
                            || &pinned.compiled_digest != binding.compiled.digest()
                            || pinned.model_tool != binding.compiled.to_model_tool()
                    })
            {
                return Err(error(ErrorCode::InvalidSnapshot, "assembly.prompt_tools"));
            }
        }
    }
    references.extend(&snapshot.context_batches);
    references.extend(snapshot.source_states.iter().map(|s| &s.batch_ref));
    for entry in &snapshot.tool_ledger {
        if let Some(reference) = &entry.call.bound_input_ref {
            crate::input_binding::validate_bound_record(
                record_value(state, additions, reference)?,
                snapshot,
                &entry.call,
                run_inputs.as_ref(),
            )
            .map_err(|_| error(ErrorCode::InvalidSnapshot, "bound_input"))?;
            let bound = record_value(state, additions, reference)?;
            let transform_ref = bound
                .get("data")
                .and_then(|data| data.get("transformation_ref"))
                .map(|value| {
                    serde_json::from_value::<RecordRef>(value.clone()).map_err(|_| {
                        error(ErrorCode::InvalidSnapshot, "bound_input.transformation_ref")
                    })
                })
                .transpose()?;
            let transformed = transform_ref
                .as_ref()
                .map(|reference| record_value(state, additions, reference))
                .transpose()?;
            crate::input_binding::validate_bound_transformation(
                bound,
                snapshot,
                &entry.call,
                transformed,
            )?;
        }
    }
    for entry in &snapshot.tool_ledger {
        if let ToolCallState::Settled { result } = &entry.state {
            references.extend(tool_result_refs(result));
        }
    }
    if let Some(wait) = &snapshot.wait {
        if let WaitTarget::Approval {
            target: ApprovalTarget::Candidate { candidate_ref, .. },
        } = &wait.target
        {
            references.push(candidate_ref);
        }
    }
    if let Some(outcome) = &snapshot.outcome {
        references.extend(&outcome.unresolved_effects);
        if let Some(verification) = &outcome.verification {
            references.extend(&verification.evidence);
        }
        if let OutcomeResult::Failed { failure } = &outcome.result {
            references.extend(failure.diagnostic_ref.iter());
        }
    }
    for reference in references {
        record_value(state, additions, reference)?;
    }
    Ok(())
}

fn validate_model_response(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    invocation: &ModelInvocationRecord,
    reference: &RecordRef,
) -> Result<(), ContractError> {
    let invalid = || error(ErrorCode::InvalidSnapshot, "model_ledger.response_ref");
    let saved: StoredModelResponse =
        serde_json::from_value(record_value(state, additions, reference)?.clone())
            .map_err(|_| invalid())?;
    let route_digest = invocation.route.digest();
    if saved.request_id != invocation.attempt_id || saved.route_digest != route_digest {
        return Err(invalid());
    }
    let metadata = match (&invocation.state, &saved.outcome) {
        (ModelAttemptState::Completed {}, ModelExchangeOutcome::Completed { response }) => {
            let mut call_ids = BTreeSet::new();
            if response.request_id != invocation.attempt_id
                || response.route_digest != route_digest
                || response
                    .continuation
                    .iter()
                    .any(|continuation| continuation.route_digest() != &route_digest)
                || response.finish == ModelFinish::Length
                || (response.finish == ModelFinish::ToolCalls) != !response.tool_calls.is_empty()
                || response
                    .tool_calls
                    .iter()
                    .any(|call| !call_ids.insert(&call.provider_call_id))
            {
                return Err(invalid());
            }
            &response.metadata
        }
        (ModelAttemptState::Failed { kind }, ModelExchangeOutcome::Failed { failure })
            if *kind == failure.kind =>
        {
            &failure.metadata
        }
        _ => return Err(invalid()),
    };
    if metadata.provider_request_id != invocation.provider_request_id
        || metadata.reported_model_id != invocation.reported_model_id
        || metadata.reported_model_version != invocation.reported_model_version
        || metadata.usage != invocation.usage
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_messages(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    run_id: &Id,
    last_sequence: u64,
    messages: &[Message],
) -> Result<u64, ContractError> {
    let mut sequence = last_sequence;
    let mut seen = BTreeSet::new();
    for message in messages {
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::InvalidMessage, "messages.sequence"))?;
        if &message.run_id != run_id
            || message.sequence.get() != sequence
            || state.message_ids.contains(&message.message_id)
            || !seen.insert(&message.message_id)
        {
            return Err(error(ErrorCode::InvalidMessage, "messages"));
        }
        for content in &message.content {
            if matches!(content, ContentBlock::ToolResultCorrection { .. }) {
                let mut history: Vec<_> = state
                    .sessions
                    .values()
                    .flat_map(|session| &session.messages)
                    .filter(|prior| prior.run_id == *run_id && prior.sequence <= message.sequence)
                    .cloned()
                    .collect();
                for addition in messages
                    .iter()
                    .filter(|addition| addition.sequence <= message.sequence)
                {
                    if !history
                        .iter()
                        .any(|prior| prior.message_id == addition.message_id)
                    {
                        history.push(addition.clone());
                    }
                }
                history.sort_by_key(|item| item.sequence);
                crate::message::tool_corrections(&history)?;
            }
            let references = match content {
                ContentBlock::Content { .. } => Vec::new(),
                ContentBlock::ToolCall { call } => call.bound_input_ref.iter().collect(),
                ContentBlock::ToolResult { result }
                | ContentBlock::ToolResultCorrection { result, .. } => tool_result_refs(result),
                ContentBlock::ProviderOpaque { data_ref, .. } => vec![data_ref],
            };
            for reference in references {
                record_value(state, additions, reference)?;
            }
        }
    }
    Ok(sequence)
}

fn validate_tool_pair(
    state: &ScopeState,
    snapshot: &RunSnapshot,
    additions: &[Message],
    result: &ToolResult,
) -> Result<(), ContractError> {
    let existing = state
        .sessions
        .get(&snapshot.request.session_id)
        .map_or(&[][..], |session| session.messages.as_slice());
    let entry = snapshot
        .tool_ledger
        .iter()
        .find(|entry| entry.call.call_id == result.call_id)
        .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.result_call"))?;
    let paired = existing.iter().chain(additions).any(|message| {
        message.message_id == result.call_message_id
            && message.run_id == snapshot.run_id
            && message.role == crate::MessageRole::Assistant
            && message.origin == crate::MessageOrigin::Model
            && message.content.iter().any(|content| {
                let ContentBlock::ToolCall { call } = content else {
                    return false;
                };
                let mut original = call.clone();
                if original.bound_input_ref.is_none() {
                    original.bound_input_ref = entry.call.bound_input_ref.clone();
                }
                original == entry.call
            })
    });
    if !paired {
        return Err(error(ErrorCode::InvalidSnapshot, "tool.result_message"));
    }
    Ok(())
}

fn tool_result_refs(result: &ToolResult) -> Vec<&RecordRef> {
    result
        .effect_receipt_ref
        .iter()
        .chain(
            result
                .error
                .iter()
                .flat_map(|error| error.diagnostic_ref.iter()),
        )
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn validate_events(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    previous_seq: u64,
    events: &[RunEvent],
    admission: bool,
    messages: &[Message],
) -> Result<(), ContractError> {
    let mut sequence = previous_seq;
    let mut seen = BTreeSet::new();
    let mut started = 0;
    let mut finished = 0;
    let mut resumed = 0;
    for message in messages {
        for content in &message.content {
            if let ContentBlock::ToolResultCorrection { result, .. } = content {
                let previous = state.runs.get(&snapshot.run_id).ok_or_else(not_found)?;
                if !matches!(previous.snapshot.wait.as_ref().map(|wait| &wait.target), Some(WaitTarget::External { call_id, .. }) if call_id == &result.call_id)
                    || !snapshot.resume_receipts.last().is_some_and(|receipt| {
                        receipt.accepted_revision == snapshot.revision
                            && matches!(receipt.command.action, ResumeAction::External { .. })
                    })
                    || !events.iter().any(|event| match &event.payload {
                        RunEventPayload::ToolSettled { result_ref } => {
                            event_record::<ToolResult>(state, additions, result_ref)
                                .is_ok_and(|saved| saved == *result)
                        }
                        _ => false,
                    })
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.tool_correction"));
                }
            }
        }
    }
    for event in events {
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::InvalidEvent, "events.seq"))?;
        if event.scope != snapshot.scope
            || event.run_id != snapshot.run_id
            || event.session_id != snapshot.request.session_id
            || event.seq.get() != sequence
            || state.event_ids.contains(&event.event_id)
            || !seen.insert(&event.event_id)
        {
            return Err(error(ErrorCode::InvalidEvent, "events"));
        }
        let reference = match &event.payload {
            RunEventPayload::RunStarted {
                request_ref,
                profile_digest,
            } => {
                started += 1;
                if !admission
                    || profile_digest != snapshot.profile.profile_digest()
                    || record_value(state, additions, request_ref)?
                        != &serde_json::to_value(&snapshot.request)
                            .map_err(|_| error(ErrorCode::InvalidContract, "request"))?
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_started"));
                }
                request_ref
            }
            RunEventPayload::RunFinished { outcome_ref } => {
                finished += 1;
                let outcome = snapshot
                    .outcome
                    .as_ref()
                    .filter(|_| snapshot.status.is_terminal())
                    .ok_or_else(|| error(ErrorCode::InvalidEvent, "events.run_finished"))?;
                if record_value(state, additions, outcome_ref)?
                    != &serde_json::to_value(outcome)
                        .map_err(|_| error(ErrorCode::InvalidContract, "outcome"))?
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_finished"));
                }
                outcome_ref
            }
            RunEventPayload::ToolPlanned { call_ref } => {
                let call: ToolCall = event_record(state, additions, call_ref)?;
                if !snapshot.tool_ledger.iter().any(|entry| entry.call == call) {
                    return Err(error(ErrorCode::InvalidEvent, "events.tool_planned"));
                }
                call_ref
            }
            RunEventPayload::ToolSettled { result_ref } => {
                let result: ToolResult = event_record(state, additions, result_ref)?;
                if !snapshot.tool_ledger.iter().any(|entry| {
                    matches!(
                        &entry.state, ToolCallState::Settled { result: saved } if *saved == result
                    )
                }) {
                    return Err(error(ErrorCode::InvalidEvent, "events.tool_settled"));
                }
                if state.runs.get(&snapshot.run_id).is_some_and(|previous| previous.snapshot.tool_ledger.iter()
                    .any(|entry| entry.call.call_id == result.call_id && matches!(entry.state, ToolCallState::Unknown { .. })))
                    && !messages.iter().flat_map(|message| &message.content)
                        .any(|content| matches!(content, ContentBlock::ToolResultCorrection { result: corrected, .. } if corrected == &result))
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.tool_correction_missing"));
                }
                validate_tool_pair(state, snapshot, messages, &result)?;
                result_ref
            }
            RunEventPayload::ToolUnresolved {
                result_ref,
                attempt_id,
                idempotency_key,
            } => {
                let result: ToolResult = event_record(state, additions, result_ref)?;
                if result.status != crate::ToolResultStatus::Unknown || result.effect != crate::ToolEffect::Unknown
                    || !snapshot.tool_ledger.iter().any(|entry| entry.call.call_id == result.call_id
                        && matches!(&entry.state, ToolCallState::Unknown { attempt_id: saved, idempotency_key: key }
                            if saved == attempt_id && key == idempotency_key))
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.tool_unresolved"));
                }
                validate_tool_pair(state, snapshot, messages, &result)?;
                for reference in tool_result_refs(&result) {
                    record_value(state, additions, reference)?;
                }
                result_ref
            }
            RunEventPayload::VerificationCompleted { verification_ref } => {
                // Additional verification history needs an explicit checkpoint contract.
                // A standalone event cannot substitute for the saved verification record.
                let verification: VerificationSummary =
                    event_record(state, additions, verification_ref)?;
                if snapshot
                    .outcome
                    .as_ref()
                    .and_then(|outcome| outcome.verification.as_ref())
                    != Some(&verification)
                {
                    return Err(error(
                        ErrorCode::InvalidEvent,
                        "events.verification_completed",
                    ));
                }
                verification_ref
            }
            RunEventPayload::RunWaiting { wait_ref } => {
                let wait: WaitState = event_record(state, additions, wait_ref)?;
                if snapshot.status != RunStatus::Waiting || snapshot.wait.as_ref() != Some(&wait) {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_waiting"));
                }
                wait_ref
            }
            RunEventPayload::RunResumed { command_ref } => {
                resumed += 1;
                let command: ResumeCommand = event_record(state, additions, command_ref)?;
                let previous = state.runs.get(&snapshot.run_id).ok_or_else(not_found)?;
                if snapshot.status != RunStatus::Running
                    || command.run_id != snapshot.run_id
                    || command.expected_revision != previous.snapshot.revision
                    || !resume_target_matches(&previous.snapshot, &command.action)
                    || !snapshot.resume_receipts.last().is_some_and(|receipt| {
                        receipt.command_ref == *command_ref
                            && receipt.command == command
                            && receipt.accepted_revision == snapshot.revision
                    })
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_resumed"));
                }
                let receipt = snapshot
                    .resume_receipts
                    .last()
                    .expect("receipt checked above");
                let prior: crate::RunOutcome =
                    event_record(state, additions, &receipt.previous_outcome_ref)?;
                if previous.snapshot.outcome.as_ref() != Some(&prior)
                    || event.timestamp_ms != snapshot.timing.last_observed_at_ms
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.resume_outcome"));
                }
                match &command.action {
                    ResumeAction::External { receipt_ref, .. } => {
                        record_value(state, additions, receipt_ref)?;
                    }
                    ResumeAction::Recover { recovery_ref } => {
                        record_value(state, additions, recovery_ref)?;
                    }
                    _ => {}
                }
                command_ref
            }
            RunEventPayload::ModelRouteSelected {
                invocation_ref,
                route_digest,
            } => {
                let invocation: ModelInvocationRecord =
                    event_record(state, additions, invocation_ref)?;
                if invocation.route.digest() != *route_digest
                    || !snapshot.model_ledger.contains(&invocation)
                {
                    return Err(error(
                        ErrorCode::InvalidEvent,
                        "events.model_route_selected",
                    ));
                }
                invocation_ref
            }
        };
        record_value(state, additions, reference)?;
    }
    if sequence != snapshot.last_event_seq
        || (admission && (started != 1 || events.len() != 1))
        || (snapshot.status.is_terminal() && finished != 1)
        || (!admission
            && resumed
                != snapshot.resume_receipts.len().saturating_sub(
                    state
                        .runs
                        .get(&snapshot.run_id)
                        .ok_or_else(not_found)?
                        .snapshot
                        .resume_receipts
                        .len(),
                ))
    {
        return Err(error(ErrorCode::InvalidEvent, "events"));
    }
    if !admission {
        let previous = &state
            .runs
            .get(&snapshot.run_id)
            .ok_or_else(not_found)?
            .snapshot;
        for old in &previous.tool_ledger {
            let Some(new) = snapshot
                .tool_ledger
                .iter()
                .find(|entry| entry.call.call_id == old.call.call_id)
            else {
                continue;
            };
            if !matches!(old.state, ToolCallState::Unknown { .. })
                || !matches!(new.state, ToolCallState::Settled { .. })
            {
                continue;
            }
            if !snapshot.resume_receipts.last().is_some_and(|receipt| {
                    receipt.accepted_revision == snapshot.revision
                        && matches!(receipt.command.action, ResumeAction::External { .. })
                        && matches!(previous.wait.as_ref().map(|wait| &wait.target), Some(WaitTarget::External { call_id, .. }) if call_id == &old.call.call_id)
                }) || !messages.iter().any(|message| matches!(message.content.as_slice(),
                    [ContentBlock::ToolResultCorrection { result, .. }] if result.call_id == old.call.call_id))
            { return Err(error(ErrorCode::InvalidEvent, "events.tool_correction_missing")); }
        }
        if snapshot
            .resume_receipts
            .last()
            .is_some_and(|receipt| receipt.accepted_revision == snapshot.revision)
        {
            let target = match &previous.wait.as_ref().ok_or_else(not_found)?.target {
                WaitTarget::Input { request } => Some((&request.call_id, Some(request))),
                WaitTarget::Approval {
                    target: ApprovalTarget::Tool { call_id, .. },
                } => Some((call_id, None)),
                _ => None,
            };
            if let Some((call_id, input)) = target {
                let old = previous
                    .tool_ledger
                    .iter()
                    .find(|entry| &entry.call.call_id == call_id)
                    .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "resume.call"))?;
                if input.is_some_and(|request| !matches!(&old.state, ToolCallState::InputPending { request: pending, .. } if pending == request))
                    || (input.is_none() && !matches!(old.state, ToolCallState::Planned {} | ToolCallState::ApprovalPending { .. }))
                { return Err(error(ErrorCode::InvalidTransition, "resume.call_state")); }
            }
        }
    }
    Ok(())
}

fn event_record<T: DeserializeOwned>(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    reference: &RecordRef,
) -> Result<T, ContractError> {
    serde_json::from_value(record_value(state, additions, reference)?.clone())
        .map_err(|_| error(ErrorCode::InvalidEvent, "events.record"))
}

/// Replay the causal facts shared by live commits and durable checkpoint restore.
/// A receipt does not by itself authorize rewriting a result: its preceding wait,
/// intervening settlement, and transcript observation must identify the same call.
fn validate_resume_history(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    events: &[&RunEvent],
    messages: &[&Message],
) -> Result<(), ContractError> {
    let invalid = || error(ErrorCode::InvalidSnapshot, "resume.history");
    let resumed: Vec<_> = events
        .iter()
        .copied()
        .filter(|event| matches!(event.payload, RunEventPayload::RunResumed { .. }))
        .collect();
    if resumed.len() != snapshot.resume_receipts.len() {
        return Err(invalid());
    }
    let mut previous_resume_seq = 0;
    let mut corrected_calls = BTreeSet::new();
    let mut authorized_corrections = BTreeSet::new();
    for (event, receipt) in resumed.into_iter().zip(&snapshot.resume_receipts) {
        let RunEventPayload::RunResumed { command_ref } = &event.payload else {
            unreachable!()
        };
        if command_ref != &receipt.command_ref
            || event.seq.get() <= receipt.previous_last_event_seq
            || receipt.previous_last_event_seq <= previous_resume_seq
            || event.timestamp_ms > snapshot.timing.last_observed_at_ms
        {
            return Err(invalid());
        }
        previous_resume_seq = event.seq.get();
        let prior: crate::RunOutcome =
            event_record(state, additions, &receipt.previous_outcome_ref)?;
        let OutcomeResult::Waiting { wait } = &prior.result else {
            return Err(invalid());
        };
        let waiting_event = events
            .iter()
            .find(|event| event.seq.get() == receipt.previous_last_event_seq)
            .ok_or_else(invalid)?;
        let RunEventPayload::RunWaiting { wait_ref } = &waiting_event.payload else {
            return Err(invalid());
        };
        let saved_wait: WaitState = event_record(state, additions, wait_ref)?;
        let expired = event.timestamp_ms >= snapshot.timing.deadline_at_ms
            || wait
                .expires_at_ms
                .is_some_and(|deadline| event.timestamp_ms >= deadline);
        if &saved_wait != wait
            || receipt.expired != expired
            || waiting_event.timestamp_ms > event.timestamp_ms
        {
            return Err(invalid());
        }
        let between: Vec<_> = events
            .iter()
            .copied()
            .filter(|candidate| {
                candidate.seq.get() > receipt.previous_last_event_seq && candidate.seq < event.seq
            })
            .collect();
        // Approval records permission only; execution belongs to the following
        // segment. Candidate verification has its separate runtime contract.
        if matches!(receipt.command.action, ResumeAction::Approve { .. })
            || matches!(
                wait.target,
                WaitTarget::Approval {
                    target: ApprovalTarget::Candidate { .. }
                }
            )
        {
            if !between.is_empty() {
                return Err(invalid());
            }
            if let WaitTarget::Approval {
                target:
                    ApprovalTarget::Tool {
                        call_id,
                        binding_digest,
                    },
            } = &wait.target
            {
                validate_resume_binding(state, additions, snapshot, call_id, binding_digest)?;
            }
            continue;
        }
        if receipt.expired && between.is_empty() {
            continue;
        }
        let [settled] = between.as_slice() else {
            return Err(invalid());
        };
        let RunEventPayload::ToolSettled { result_ref } = &settled.payload else {
            return Err(invalid());
        };
        let result: ToolResult = event_record(state, additions, result_ref)?;
        if !snapshot.tool_ledger.iter().any(|entry| {
            matches!(&entry.state,
            ToolCallState::Settled { result: current } if current == &result)
        }) {
            return Err(invalid());
        }
        match (&receipt.command.action, &wait.target) {
            (ResumeAction::Input { answer, .. }, WaitTarget::Input { request }) => {
                if result.call_id != request.call_id
                    || result.status != crate::ToolResultStatus::Succeeded
                    || result.effect != crate::ToolEffect::NotApplied
                    || result.content
                        != [crate::InputContent::Json {
                            value: answer.clone(),
                        }]
                    || result.error.is_some()
                    || result.effect_receipt_ref.is_some()
                {
                    return Err(invalid());
                }
                validate_resume_result_message(snapshot, messages, &result)?;
            }
            (
                ResumeAction::Deny { .. },
                WaitTarget::Approval {
                    target:
                        ApprovalTarget::Tool {
                            call_id,
                            binding_digest,
                        },
                },
            ) => {
                validate_resume_binding(state, additions, snapshot, call_id, binding_digest)?;
                if &result.call_id != call_id
                    || result.status != crate::ToolResultStatus::Denied
                    || result.effect != crate::ToolEffect::NotApplied
                    || !result.content.is_empty()
                    || result.effect_receipt_ref.is_some()
                {
                    return Err(invalid());
                }
                validate_resume_result_message(snapshot, messages, &result)?;
            }
            (
                ResumeAction::External { receipt_ref, .. },
                WaitTarget::External {
                    call_id,
                    effect_key,
                },
            ) => {
                record_value(state, additions, receipt_ref)?;
                if &result.call_id != call_id
                    || result.effect == crate::ToolEffect::Unknown
                    || result.status == crate::ToolResultStatus::Unknown
                {
                    return Err(invalid());
                }
                let unknown_event = events.iter().rev().find(|candidate| {
                    candidate.seq.get() < receipt.previous_last_event_seq
                        && matches!(&candidate.payload, RunEventPayload::ToolUnresolved { result_ref, idempotency_key, .. }
                            if idempotency_key == effect_key && event_record::<ToolResult>(state, additions, result_ref)
                                .is_ok_and(|unknown| &unknown.call_id == call_id))
                }).ok_or_else(invalid)?;
                let RunEventPayload::ToolUnresolved { result_ref, .. } = &unknown_event.payload
                else {
                    unreachable!()
                };
                let unknown: ToolResult = event_record(state, additions, result_ref)?;
                let digest =
                    canonical_digest(&serde_json::to_value(&unknown).map_err(|_| invalid())?);
                let matching: Vec<_> = messages.iter().filter(|message| {
                    message.run_id == snapshot.run_id && matches!(message.content.as_slice(),
                        [ContentBlock::ToolResultCorrection { previous_message_id, previous_result_digest, result: corrected }]
                        if corrected == &result && previous_result_digest == &digest
                            && messages.iter().any(|prior| prior.message_id == *previous_message_id
                                && prior.run_id == snapshot.run_id && matches!(prior.content.as_slice(),
                                    [ContentBlock::ToolResult { result: previous }] if previous == &unknown)))
                }).collect();
                let [correction] = matching.as_slice() else {
                    return Err(invalid());
                };
                if !authorized_corrections.insert(correction.message_id.clone())
                    || !corrected_calls.insert(call_id.clone())
                {
                    return Err(invalid());
                }
            }
            _ => return Err(invalid()),
        }
    }
    for message in messages
        .iter()
        .filter(|message| message.run_id == snapshot.run_id)
    {
        if message
            .content
            .iter()
            .any(|content| matches!(content, ContentBlock::ToolResultCorrection { .. }))
            && !authorized_corrections.contains(&message.message_id)
        {
            return Err(invalid());
        }
    }
    for event in events {
        if let RunEventPayload::ToolUnresolved { result_ref, .. } = &event.payload {
            let unknown: ToolResult = event_record(state, additions, result_ref)?;
            if snapshot.tool_ledger.iter().any(|entry| {
                entry.call.call_id == unknown.call_id
                    && matches!(entry.state, ToolCallState::Settled { .. })
            }) && !corrected_calls.contains(&unknown.call_id)
            {
                return Err(invalid());
            }
        }
    }
    Ok(())
}

fn validate_resume_binding(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    call_id: &Id,
    binding_digest: &crate::JsonDigest,
) -> Result<(), ContractError> {
    let invalid = || error(ErrorCode::InvalidSnapshot, "resume.binding");
    let reference = snapshot
        .tool_ledger
        .iter()
        .find(|entry| &entry.call.call_id == call_id)
        .and_then(|entry| entry.call.bound_input_ref.as_ref())
        .ok_or_else(invalid)?;
    // validate_snapshot_refs already validates this typed protected binding.
    if record_value(state, additions, reference)?.get("binding_digest")
        != Some(&serde_json::to_value(binding_digest).map_err(|_| invalid())?)
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_resume_result_message(
    snapshot: &RunSnapshot,
    messages: &[&Message],
    result: &ToolResult,
) -> Result<(), ContractError> {
    let count = messages.iter().filter(|message| message.run_id == snapshot.run_id
        && message.role == crate::MessageRole::Tool && message.origin == crate::MessageOrigin::Tool
        && matches!(message.content.as_slice(), [ContentBlock::ToolResult { result: saved }] if saved == result)).count();
    if count != 1 {
        return Err(error(ErrorCode::InvalidSnapshot, "resume.result_message"));
    }
    Ok(())
}

fn resume_target_matches(previous: &RunSnapshot, action: &ResumeAction) -> bool {
    if let ResumeAction::Recover { .. } = action {
        return previous.status == RunStatus::Running;
    }
    previous
        .wait
        .as_ref()
        .is_some_and(|wait| action_matches_wait(wait, action))
}

fn action_matches_wait(wait: &WaitState, action: &ResumeAction) -> bool {
    match action {
        ResumeAction::Recover { .. } => false,
        ResumeAction::Approve { wait_id, target }
        | ResumeAction::Deny {
            wait_id, target, ..
        } => {
            &wait.wait_id == wait_id
                && matches!(&wait.target, WaitTarget::Approval { target: saved } if saved == target)
        }
        ResumeAction::Input { wait_id, .. } => {
            &wait.wait_id == wait_id && matches!(wait.target, WaitTarget::Input { .. })
        }
        ResumeAction::External { wait_id, .. } => {
            &wait.wait_id == wait_id && matches!(wait.target, WaitTarget::External { .. })
        }
    }
}

fn validate_transition(previous: &RunSnapshot, next: &RunSnapshot) -> Result<(), ContractError> {
    if previous.status.is_terminal() {
        return Err(error(ErrorCode::InvalidTransition, "run.status"));
    }
    next.validate()?;
    validate_hook_transition(previous, next)?;
    crate::budget::validate_budget_transition(previous, next)?;
    if !next.resume_receipts.starts_with(&previous.resume_receipts)
        || next.resume_receipts.len() > previous.resume_receipts.len() + 1
    {
        return Err(error(ErrorCode::InvalidTransition, "resume_receipts"));
    }
    let resumed = next.resume_receipts.len() != previous.resume_receipts.len();
    if resumed {
        let receipt = next.resume_receipts.last().expect("new receipt");
        if previous.status != RunStatus::Waiting
            || next.status != RunStatus::Running
            || next.outcome.is_some()
            || next.wait.is_some()
            || receipt.accepted_revision != next.revision
            || receipt.command.expected_revision != previous.revision
            || receipt.previous_last_event_seq != previous.last_event_seq
            || !resume_target_matches(previous, &receipt.command.action)
        {
            return Err(error(
                ErrorCode::InvalidTransition,
                "resume_receipts.acceptance",
            ));
        }
    } else if previous.status == RunStatus::Waiting && next.status == RunStatus::Running {
        return Err(error(
            ErrorCode::InvalidTransition,
            "resume_receipts.missing",
        ));
    }
    if previous.run_id != next.run_id
        || previous.request != next.request
        || previous.request_digest != next.request_digest
        || previous.scope != next.scope
        || previous.system_inputs != next.system_inputs
        || previous.limits != next.limits
        || next.revision
            != previous
                .revision
                .checked_add(1)
                .ok_or_else(|| error(ErrorCode::RevisionConflict, "revision"))?
        || previous.assembly_ref != next.assembly_ref
        || (previous.routing_snapshot_ref.is_some()
            && previous.routing_snapshot_ref != next.routing_snapshot_ref)
        || (previous.routing_snapshot_ref.is_none()
            && next.routing_snapshot_ref.is_some()
            && previous.usage.model_calls != 0)
    {
        return Err(error(
            ErrorCode::InvalidTransition,
            "snapshot.immutable_fields",
        ));
    }
    if previous.profile != next.profile {
        return Err(error(ErrorCode::ProfileMismatch, "snapshot.profile"));
    }
    if previous.tool_ledger.len() > next.tool_ledger.len() {
        return Err(error(ErrorCode::InvalidTransition, "tool_ledger"));
    }
    for (old, new) in previous.tool_ledger.iter().zip(&next.tool_ledger) {
        let mut call = old.call.clone();
        if call.bound_input_ref.is_none() {
            call.bound_input_ref = new.call.bound_input_ref.clone();
        }
        if call != new.call
            || (matches!(old.state, ToolCallState::Settled { .. }) && old != new)
            || (matches!(
                old.state,
                ToolCallState::Dispatching { .. }
                    | ToolCallState::Unknown { .. }
                    | ToolCallState::ApprovalPending { .. }
                    | ToolCallState::InputPending { .. }
            ) && matches!(new.state, ToolCallState::Planned { .. }))
            || (matches!(old.state, ToolCallState::Unknown { .. })
                && matches!(new.state, ToolCallState::ApprovalPending { .. }))
            || (matches!(old.state, ToolCallState::ApprovalPending { .. })
                && matches!(new.state, ToolCallState::Unknown { .. }))
            || (matches!(old.state, ToolCallState::InputPending { .. })
                && !matches!(
                    new.state,
                    ToolCallState::InputPending { .. } | ToolCallState::Settled { .. }
                ))
            || (matches!(new.state, ToolCallState::InputPending { .. })
                && !matches!(
                    old.state,
                    ToolCallState::Dispatching { .. } | ToolCallState::InputPending { .. }
                ))
        {
            return Err(error(ErrorCode::InvalidTransition, "tool_ledger"));
        }
        if let (
            ToolCallState::Dispatching {
                attempt_id: old_attempt,
                idempotency_key: old_key,
            }
            | ToolCallState::Unknown {
                attempt_id: old_attempt,
                idempotency_key: old_key,
            }
            | ToolCallState::ApprovalPending {
                attempt_id: old_attempt,
                idempotency_key: old_key,
            }
            | ToolCallState::InputPending {
                attempt_id: old_attempt,
                idempotency_key: old_key,
                ..
            },
            ToolCallState::Dispatching {
                attempt_id: new_attempt,
                idempotency_key: new_key,
            }
            | ToolCallState::Unknown {
                attempt_id: new_attempt,
                idempotency_key: new_key,
            }
            | ToolCallState::ApprovalPending {
                attempt_id: new_attempt,
                idempotency_key: new_key,
            }
            | ToolCallState::InputPending {
                attempt_id: new_attempt,
                idempotency_key: new_key,
                ..
            },
        ) = (&old.state, &new.state)
        {
            let retry = matches!(
                old.state,
                ToolCallState::Unknown { .. } | ToolCallState::ApprovalPending { .. }
            ) && matches!(new.state, ToolCallState::Dispatching { .. });
            if old_key != new_key || (!retry && old_attempt != new_attempt) {
                return Err(error(ErrorCode::InvalidTransition, "tool_ledger.attempt"));
            }
        }
        if let (
            ToolCallState::InputPending {
                request: before, ..
            },
            ToolCallState::InputPending { request: after, .. },
        ) = (&old.state, &new.state)
        {
            if before != after {
                return Err(error(
                    ErrorCode::InvalidTransition,
                    "tool_ledger.input_request",
                ));
            }
        }
    }
    if previous.model_ledger.len() > next.model_ledger.len()
        || previous
            .model_ledger
            .iter()
            .zip(&next.model_ledger)
            .any(|(old, new)| {
                old.run_id != new.run_id
                    || old.model_step_id != new.model_step_id
                    || old.attempt_id != new.attempt_id
                    || old.purpose != new.purpose
                    || old.route != new.route
                    || old.selection_reason != new.selection_reason
                    || old.request_digest != new.request_digest
                    || old.inspection_ref != new.inspection_ref
                    || (matches!(
                        old.state,
                        ModelAttemptState::Completed {} | ModelAttemptState::Failed { .. }
                    ) && old != new)
                    || (matches!(old.state, ModelAttemptState::Unknown {})
                        && matches!(new.state, ModelAttemptState::Reserved {}))
            })
    {
        return Err(error(ErrorCode::InvalidTransition, "model_ledger"));
    }
    let old = &previous.usage;
    let new = &next.usage;
    if new.model_calls < old.model_calls
        || new.tool_attempts < old.tool_attempts
        || new.repair_attempts < old.repair_attempts
        || new.recovery_attempts < old.recovery_attempts
        || new.elapsed_ms < old.elapsed_ms
    {
        return Err(error(ErrorCode::InvalidTransition, "usage"));
    }
    Ok(())
}
```

## `crates/wickle/src/state/checkpoint.rs`

```rust
use super::*;
use crate::{JsonDigest, RunOutcome, RunRequest, serialization::data_digest};
use serde::{Deserialize, Serialize, Serializer};

/// Version of the protected, scope-local memory-store checkpoint format.
pub const STATE_STORE_CHECKPOINT_VERSION: &str = "wickle.state-store.v1";

/// An owned, validated scope graph. Explicit serialization contains protected
/// transcript and input data and is intended only for authorized storage adapters.
/// No caller can mutate its state or deserialize it without full validation.
#[derive(Clone)]
pub struct StateStoreCheckpoint {
    scope: Scope,
    state: ScopeState,
}

impl fmt::Debug for StateStoreCheckpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateStoreCheckpoint")
            .field("session_count", &self.state.sessions.len())
            .field("run_count", &self.state.runs.len())
            .field("record_count", &self.state.records.len())
            .finish_non_exhaustive()
    }
}

#[derive(Serialize)]
struct CheckpointView<'a> {
    schema_version: &'static str,
    scope: &'a Scope,
    sessions: Vec<SessionView<'a>>,
    runs: Vec<RunView<'a>>,
    records: Vec<RecordView<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    hook_observations: Vec<&'a crate::HookObservation>,
}
#[derive(Serialize)]
struct SessionView<'a> {
    snapshot: &'a SessionSnapshot,
    messages: &'a [Message],
}
#[derive(Serialize)]
struct RunView<'a> {
    snapshot: &'a RunSnapshot,
    events: &'a [RunEvent],
    lease: Option<LeaseData>,
    last_fencing_token: u64,
}
#[derive(Serialize)]
struct RecordView<'a> {
    reference: &'a RecordRef,
    value: &'a Value,
}

impl Serialize for StateStoreCheckpoint {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        CheckpointView {
            schema_version: STATE_STORE_CHECKPOINT_VERSION,
            scope: &self.scope,
            sessions: self
                .state
                .sessions
                .values()
                .map(|session| SessionView {
                    snapshot: &session.snapshot,
                    messages: &session.messages,
                })
                .collect(),
            runs: self
                .state
                .runs
                .values()
                .map(|run| RunView {
                    snapshot: &run.snapshot,
                    events: &run.events,
                    lease: run.lease.as_ref().map(LeaseData::from),
                    last_fencing_token: run.last_fencing_token,
                })
                .collect(),
            records: self
                .state
                .records
                .values()
                .map(|record| RecordView {
                    reference: record.reference(),
                    value: record.value(),
                })
                .collect(),
            hook_observations: self.state.hook_observations.values().flatten().collect(),
        }
        .serialize(serializer)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointData {
    schema_version: String,
    scope: Scope,
    sessions: Vec<SessionData>,
    runs: Vec<RunData>,
    records: Vec<RecordData>,
    #[serde(default)]
    hook_observations: Vec<crate::HookObservation>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionData {
    snapshot: SessionSnapshot,
    messages: Vec<Message>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunData {
    snapshot: RunSnapshot,
    events: Vec<RunEvent>,
    lease: Option<LeaseData>,
    last_fencing_token: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordData {
    reference: RecordRef,
    value: Value,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaseData {
    scope: Scope,
    run_id: Id,
    owner: Id,
    fencing_token: u64,
    expires_at_ms: i64,
}
impl From<&RunLease> for LeaseData {
    fn from(lease: &RunLease) -> Self {
        Self {
            scope: lease.scope.clone(),
            run_id: lease.run_id.clone(),
            owner: lease.owner.clone(),
            fencing_token: lease.fencing_token,
            expires_at_ms: lease.expires_at_ms,
        }
    }
}
impl From<LeaseData> for RunLease {
    fn from(lease: LeaseData) -> Self {
        Self {
            scope: lease.scope,
            run_id: lease.run_id,
            owner: lease.owner,
            fencing_token: lease.fencing_token,
            expires_at_ms: lease.expires_at_ms,
        }
    }
}

impl StateStoreCheckpoint {
    /// Exact namespace covered by the protected checkpoint.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Canonical identity of the serialized scope graph, excluding derived indexes.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }
    /// Parse a known version and validate scope, trusted digest, current state,
    /// historical typed records and derived indexes. Collection order is the stable
    /// key order produced by export; malformed or noncanonical images are rejected.
    pub fn from_json(
        input: &str,
        scope: &Scope,
        expected_digest: &JsonDigest,
    ) -> Result<Self, ContractError> {
        let value = crate::parse_json(input)?;
        if value.get("schema_version").and_then(Value::as_str)
            != Some(STATE_STORE_CHECKPOINT_VERSION)
        {
            return Err(error(
                ErrorCode::UnsupportedSchemaVersion,
                "checkpoint.schema_version",
            ));
        }
        if canonical_digest(&value) != *expected_digest {
            return Err(invalid("checkpoint.digest"));
        }
        let data: CheckpointData =
            serde_json::from_value(value).map_err(|_| invalid("checkpoint"))?;
        if &data.scope != scope {
            return Err(error(ErrorCode::AccessDenied, "checkpoint.scope"));
        }
        let checkpoint = restore_graph(data)?;
        if checkpoint.digest() != *expected_digest {
            return Err(invalid("checkpoint.canonical_form"));
        }
        Ok(checkpoint)
    }
}

impl MemoryStateStore {
    /// Copy only the requested namespace without performing I/O or exposing live
    /// mutable references. Unknown namespaces return StateNotFound.
    pub fn export_checkpoint(&self, scope: &Scope) -> Result<StateStoreCheckpoint, ContractError> {
        let scopes = self.lock()?;
        Ok(StateStoreCheckpoint {
            scope: scope.clone(),
            state: namespace(&scopes, scope)?.clone(),
        })
    }
    /// Move an already validated private checkpoint into a new process-local store.
    /// This does not perform a second graph validation or claim durable capabilities.
    pub fn from_checkpoint(checkpoint: StateStoreCheckpoint) -> Self {
        Self {
            scopes: Mutex::new(BTreeMap::from([(
                scope_key(&checkpoint.scope),
                checkpoint.state,
            )])),
        }
    }
}

fn restore_graph(data: CheckpointData) -> Result<StateStoreCheckpoint, ContractError> {
    if data.schema_version != STATE_STORE_CHECKPOINT_VERSION {
        return Err(invalid("checkpoint.schema_version"));
    }
    let mut state = ScopeState::default();
    for record in data.records {
        if canonical_digest(&record.value) != record.reference.digest {
            return Err(invalid("checkpoint.record_digest"));
        }
        let key = record_key(&record.reference);
        if state
            .records
            .insert(
                key,
                ProtectedRecord {
                    reference: record.reference,
                    value: record.value,
                },
            )
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_record"));
        }
    }
    for session in data.sessions {
        if session.snapshot.scope != data.scope {
            return Err(invalid("checkpoint.session_scope"));
        }
        if state
            .sessions
            .insert(
                session.snapshot.session_id.clone(),
                SessionState {
                    snapshot: session.snapshot,
                    messages: session.messages,
                },
            )
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_session"));
        }
    }
    for run in data.runs {
        if run.snapshot.scope != data.scope {
            return Err(invalid("checkpoint.run_scope"));
        }
        run.snapshot.validate()?;
        let session = state
            .sessions
            .get(&run.snapshot.request.session_id)
            .ok_or_else(|| invalid("checkpoint.run_session"))?;
        if session.snapshot.profile_digest != *run.snapshot.profile.profile_digest() {
            return Err(invalid("checkpoint.session_profile"));
        }
        if run.snapshot.revision > 0 && run.last_fencing_token == 0 {
            return Err(invalid("checkpoint.fencing_generation"));
        }
        if let Some(lease) = &run.lease {
            if lease.scope != data.scope
                || lease.run_id != run.snapshot.run_id
                || lease.fencing_token == 0
                || lease.fencing_token != run.last_fencing_token
                || run.snapshot.status.is_terminal()
            {
                return Err(invalid("checkpoint.lease"));
            }
        }
        if run.snapshot.revision == 0
            && (run.snapshot.status != RunStatus::Running
                || run.snapshot.phase != RunPhase::Admission
                || run.snapshot.usage != BudgetUsage::default()
                || !run.snapshot.reservations.is_empty()
                || !run.snapshot.model_ledger.is_empty()
                || !run.snapshot.tool_ledger.is_empty()
                || run.events.len() != 1)
        {
            return Err(invalid("checkpoint.admission"));
        }
        let request = (
            run.snapshot.request.session_id.clone(),
            run.snapshot.request.request_id.clone(),
        );
        if state
            .requests
            .insert(request, run.snapshot.run_id.clone())
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_request"));
        }
        let run_id = run.snapshot.run_id.clone();
        if state
            .runs
            .insert(
                run_id,
                RunState {
                    snapshot: run.snapshot,
                    events: run.events,
                    lease: run.lease.map(Into::into),
                    last_fencing_token: run.last_fencing_token,
                },
            )
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_run"));
        }
    }
    let empty = BTreeMap::new();
    let mut message_ids = BTreeSet::new();
    for session in state.sessions.values() {
        record_value(&state, &empty, &session.snapshot.prompt_snapshot)?;
        let active: Vec<_> = state
            .runs
            .values()
            .filter(|run| {
                run.snapshot.request.session_id == session.snapshot.session_id
                    && !run.snapshot.status.is_terminal()
            })
            .collect();
        if active.len() > 1
            || active.first().map(|run| &run.snapshot.run_id)
                != session.snapshot.active_run_id.as_ref()
        {
            return Err(invalid("checkpoint.active_run"));
        }
        if !state
            .runs
            .values()
            .any(|run| run.snapshot.request.session_id == session.snapshot.session_id)
        {
            return Err(invalid("checkpoint.orphan_session"));
        }
        let mut sequence = 0;
        let mut seen_runs = BTreeSet::new();
        let mut previous_run = None;
        for message in &session.messages {
            let run = state
                .runs
                .get(&message.run_id)
                .ok_or_else(|| invalid("checkpoint.message_run"))?;
            if run.snapshot.request.session_id != session.snapshot.session_id
                || !message_ids.insert(message.message_id.clone())
            {
                return Err(invalid("checkpoint.message_identity"));
            }
            if previous_run != Some(&message.run_id) {
                if !seen_runs.insert(&message.run_id) {
                    return Err(invalid("checkpoint.message_run_order"));
                }
                previous_run = Some(&message.run_id);
            }
            sequence = validate_messages(
                &state,
                &empty,
                &message.run_id,
                sequence,
                std::slice::from_ref(message),
            )?;
        }
        if sequence != session.snapshot.transcript_revision {
            return Err(invalid("checkpoint.transcript_revision"));
        }
        if let Some(active_run) = &session.snapshot.active_run_id {
            if seen_runs.contains(active_run) && previous_run != Some(active_run) {
                return Err(invalid("checkpoint.active_run_order"));
            }
        }
    }
    let mut event_ids = BTreeSet::new();
    for run in state.runs.values() {
        validate_snapshot_refs(&state, &empty, &run.snapshot)?;
        validate_history(&state, run, &mut event_ids)?;
    }
    for report in data.hook_observations {
        validate_hook_observation(&state, &data.scope, &report.run_id, &report)?;
        let reports = state
            .hook_observations
            .entry(report.run_id.clone())
            .or_default();
        if reports.iter().any(|existing| {
            existing.hook == report.hook
                && existing.selection == report.selection
                && existing.target == report.target
        }) {
            return Err(invalid("checkpoint.hook_observation_duplicate"));
        }
        reports.push(report);
    }
    state.message_ids = message_ids;
    state.event_ids = event_ids;
    Ok(StateStoreCheckpoint {
        scope: data.scope,
        state,
    })
}

fn validate_history(
    state: &ScopeState,
    run: &RunState,
    event_ids: &mut BTreeSet<Id>,
) -> Result<(), ContractError> {
    let empty = BTreeMap::new();
    let mut sequence = 0_u64;
    let mut started = 0;
    let mut finished = 0;
    let mut resumed = 0;
    let mut unresolved_keys = BTreeMap::new();
    for event in &run.events {
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| invalid("checkpoint.event_sequence"))?;
        if event.scope != run.snapshot.scope
            || event.run_id != run.snapshot.run_id
            || event.session_id != run.snapshot.request.session_id
            || event.seq.get() != sequence
            || !event_ids.insert(event.event_id.clone())
        {
            return Err(invalid("checkpoint.event_identity"));
        }
        match &event.payload {
            RunEventPayload::RunStarted {
                request_ref,
                profile_digest,
            } => {
                started += 1;
                let request: RunRequest = event_record(state, &empty, request_ref)?;
                if sequence != 1
                    || request != run.snapshot.request
                    || profile_digest != run.snapshot.profile.profile_digest()
                {
                    return Err(invalid("checkpoint.run_started"));
                }
            }
            RunEventPayload::RunFinished { outcome_ref } => {
                finished += 1;
                let outcome: RunOutcome = event_record(state, &empty, outcome_ref)?;
                if !run.snapshot.status.is_terminal()
                    || run.snapshot.outcome.as_ref() != Some(&outcome)
                {
                    return Err(invalid("checkpoint.run_finished"));
                }
            }
            RunEventPayload::ToolPlanned { call_ref } => {
                let mut call: ToolCall = event_record(state, &empty, call_ref)?;
                let current = run
                    .snapshot
                    .tool_ledger
                    .iter()
                    .find(|entry| entry.call.call_id == call.call_id)
                    .ok_or_else(|| invalid("checkpoint.tool_planned"))?;
                if call.bound_input_ref.is_none() {
                    call.bound_input_ref = current.call.bound_input_ref.clone();
                }
                if call != current.call {
                    return Err(invalid("checkpoint.tool_planned"));
                }
            }
            RunEventPayload::ToolSettled { result_ref } => {
                let result: ToolResult = event_record(state, &empty, result_ref)?;
                if !run.snapshot.tool_ledger.iter().any(|entry| matches!(&entry.state, ToolCallState::Settled { result: current } if current == &result)) {
                    return Err(invalid("checkpoint.tool_settled"));
                }
                validate_tool_pair(state, &run.snapshot, &[], &result)?;
                for reference in tool_result_refs(&result) {
                    record_value(state, &empty, reference)?;
                }
            }
            RunEventPayload::ToolUnresolved {
                result_ref,
                attempt_id,
                idempotency_key,
            } => {
                let result: ToolResult = event_record(state, &empty, result_ref)?;
                if result.status != crate::ToolResultStatus::Unknown || result.effect != crate::ToolEffect::Unknown
                    || !run.snapshot.tool_ledger.iter().any(|entry| entry.call.call_id == result.call_id)
                    || !run.snapshot.reservations.iter().any(|reservation| &reservation.attempt_id == attempt_id
                        && matches!(&reservation.kind, crate::ReservationKind::Tool { call_id } if call_id == &result.call_id))
                {
                    return Err(invalid("checkpoint.tool_unresolved"));
                }
                let entry = run
                    .snapshot
                    .tool_ledger
                    .iter()
                    .find(|entry| entry.call.call_id == result.call_id)
                    .expect("call membership checked above");
                let current_key = match &entry.state {
                    ToolCallState::Dispatching {
                        idempotency_key, ..
                    }
                    | ToolCallState::ApprovalPending {
                        idempotency_key, ..
                    }
                    | ToolCallState::Unknown {
                        idempotency_key, ..
                    } => Some(idempotency_key),
                    _ => None,
                };
                if current_key.is_some_and(|key| key != idempotency_key)
                    || unresolved_keys
                        .insert(result.call_id.clone(), idempotency_key)
                        .is_some_and(|key| key != idempotency_key)
                {
                    return Err(invalid("checkpoint.tool_unresolved_key"));
                }
                validate_tool_pair(state, &run.snapshot, &[], &result)?;
                for reference in tool_result_refs(&result) {
                    record_value(state, &empty, reference)?;
                }
            }
            RunEventPayload::VerificationCompleted { verification_ref } => {
                let verification: VerificationSummary =
                    event_record(state, &empty, verification_ref)?;
                for reference in &verification.evidence {
                    record_value(state, &empty, reference)?;
                }
            }
            RunEventPayload::RunWaiting { wait_ref } => {
                let wait: WaitState = event_record(state, &empty, wait_ref)?;
                if let WaitTarget::Approval {
                    target: ApprovalTarget::Candidate { candidate_ref, .. },
                } = &wait.target
                {
                    record_value(state, &empty, candidate_ref)?;
                }
            }
            RunEventPayload::RunResumed { command_ref } => {
                resumed += 1;
                let command: ResumeCommand = event_record(state, &empty, command_ref)?;
                if command.run_id != run.snapshot.run_id
                    || command.expected_revision >= run.snapshot.revision
                    || !run.snapshot.resume_receipts.iter().any(|receipt| {
                        receipt.command_ref == *command_ref
                            && receipt.command == command
                            && event.seq.get() > receipt.previous_last_event_seq
                    })
                {
                    return Err(invalid("checkpoint.run_resumed"));
                }
                let reference = match &command.action {
                    ResumeAction::External { receipt_ref, .. } => Some(receipt_ref),
                    ResumeAction::Recover { recovery_ref } => Some(recovery_ref),
                    ResumeAction::Approve {
                        target: ApprovalTarget::Candidate { candidate_ref, .. },
                        ..
                    }
                    | ResumeAction::Deny {
                        target: ApprovalTarget::Candidate { candidate_ref, .. },
                        ..
                    } => Some(candidate_ref),
                    _ => None,
                };
                if let Some(reference) = reference {
                    record_value(state, &empty, reference)?;
                }
            }
            RunEventPayload::ModelRouteSelected {
                invocation_ref,
                route_digest,
            } => {
                let old: ModelInvocationRecord = event_record(state, &empty, invocation_ref)?;
                let current = run
                    .snapshot
                    .model_ledger
                    .iter()
                    .find(|current| current.attempt_id == old.attempt_id)
                    .ok_or_else(|| invalid("checkpoint.model_route"))?;
                if old.run_id != current.run_id
                    || old.model_step_id != current.model_step_id
                    || old.purpose != current.purpose
                    || old.route != current.route
                    || old.selection_reason != current.selection_reason
                    || old.request_digest != current.request_digest
                    || old.inspection_ref != current.inspection_ref
                    || old.route.digest() != *route_digest
                    || (matches!(
                        old.state,
                        ModelAttemptState::Completed {} | ModelAttemptState::Failed { .. }
                    ) && &old != current)
                    || (matches!(old.state, ModelAttemptState::Unknown {})
                        && matches!(current.state, ModelAttemptState::Reserved {}))
                {
                    return Err(invalid("checkpoint.model_route"));
                }
                if let Some(reference) = &old.response_ref {
                    validate_model_response(state, &empty, &old, reference)?;
                }
            }
        }
    }
    if started != 1
        || sequence != run.snapshot.last_event_seq
        || finished != usize::from(run.snapshot.status.is_terminal())
        || resumed != run.snapshot.resume_receipts.len()
    {
        return Err(invalid("checkpoint.events"));
    }
    let events: Vec<_> = run.events.iter().collect();
    let messages: Vec<_> = state
        .sessions
        .get(&run.snapshot.request.session_id)
        .ok_or_else(|| invalid("checkpoint.session"))?
        .messages
        .iter()
        .collect();
    validate_resume_history(state, &empty, &run.snapshot, &events, &messages)?;
    Ok(())
}

fn invalid(path: &str) -> ContractError {
    error(ErrorCode::InvalidSnapshot, path)
}
```

## `crates/wickle/src/state/hook_state.rs`

```rust
use super::*;
use crate::{HookInput, HookPlan, HookPosition, HookTarget};

fn load_plan(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<Option<HookPlan>, ContractError> {
    let Some(reference) = &snapshot.hook_plan_ref else {
        if !snapshot.hook_applications.is_empty()
            || snapshot
                .profile
                .profile()
                .hooks
                .as_ref()
                .is_some_and(|hooks| !hooks.is_empty())
        {
            return Err(error(ErrorCode::InvalidSnapshot, "hooks.plan_missing"));
        }
        return Ok(None);
    };
    let value = record_value(state, additions, reference)?;
    let plan = HookPlan::restore(
        &serde_json::to_string(value)
            .map_err(|_| error(ErrorCode::InvalidSnapshot, "hooks.plan"))?,
        &snapshot.scope,
        &reference.digest,
    )?;
    plan.validate(snapshot.profile.profile())?;
    Ok(Some(plan))
}

pub(super) fn validate_hook_snapshot(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    let Some(plan) = load_plan(state, additions, snapshot)? else {
        return Ok(());
    };
    let records = snapshot
        .hook_applications
        .iter()
        .map(|application| {
            let value = record_value(state, additions, &application.result_ref)?;
            Ok(ProtectedRecord::new(
                application.result_ref.record_id.clone(),
                application.result_ref.revision,
                value.clone(),
            ))
        })
        .collect::<Result<Vec<_>, ContractError>>()?;
    crate::hooks::validate_application_chain(&plan, snapshot, &records)?;
    let expected = |position| {
        plan.definitions()
            .iter()
            .filter(|definition| definition.position == position)
            .count()
    };
    let applied = |target: &HookTarget| {
        snapshot
            .hook_applications
            .iter()
            .filter(|application| &application.target == target)
            .count()
    };
    if snapshot.usage.model_calls > 0
        && applied(&HookTarget::BeforeRun) != expected(HookPosition::BeforeRun)
    {
        return Err(error(
            ErrorCode::InvalidSnapshot,
            "hooks.before_run_incomplete",
        ));
    }
    for invocation in &snapshot.model_ledger {
        if applied(&HookTarget::BeforeModel {
            model_step_id: invocation.model_step_id.clone(),
        }) != expected(HookPosition::BeforeModel)
        {
            return Err(error(
                ErrorCode::InvalidSnapshot,
                "hooks.before_model_incomplete",
            ));
        }
    }
    for entry in &snapshot.tool_ledger {
        if entry.call.bound_input_ref.is_some() {
            let target = HookTarget::BeforeTool {
                call_id: entry.call.call_id.clone(),
            };
            if applied(&target) != expected(HookPosition::BeforeTool) {
                return Err(error(
                    ErrorCode::InvalidSnapshot,
                    "hooks.before_tool_incomplete",
                ));
            }
            for (_, record) in snapshot
                .hook_applications
                .iter()
                .zip(&records)
                .filter(|(application, _)| application.target == target)
            {
                let result: crate::HookApplicationRecord =
                    serde_json::from_value(record.value().clone())
                        .map_err(|_| error(ErrorCode::InvalidSnapshot, "hooks.application"))?;
                if matches!(
                    result.output,
                    Some(crate::HookOutput::Tool { deny: Some(_), .. })
                ) {
                    return Err(error(ErrorCode::InvalidSnapshot, "hooks.bound_denied"));
                }
            }
        }
    }
    if !records.is_empty() {
        let session = state
            .sessions
            .get(&snapshot.request.session_id)
            .ok_or_else(not_found)?;
        let value = record_value(state, additions, &session.snapshot.prompt_snapshot)?;
        let prompt = crate::PromptSnapshot::restore(
            &serde_json::to_string(value)
                .map_err(|_| error(ErrorCode::InvalidSnapshot, "hooks.prompt"))?,
            &session.snapshot.prompt_snapshot.digest,
            &snapshot.profile,
            &snapshot.scope,
        )?;
        for record in &records {
            let application: crate::HookApplicationRecord =
                serde_json::from_value(record.value().clone())
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "hooks.application"))?;
            if let HookInput::BeforeTool {
                tool,
                descriptor_digest,
                compiled_digest,
                ..
            } = &application.input
            {
                if !prompt.tools().iter().any(|pinned| {
                    &pinned.model_tool == tool
                        && &pinned.descriptor_digest == descriptor_digest
                        && &pinned.compiled_digest == compiled_digest
                }) {
                    return Err(error(ErrorCode::InvalidSnapshot, "hooks.tool_contract"));
                }
            }
        }
    }
    Ok(())
}

pub(super) fn validate_hook_transition(
    previous: &RunSnapshot,
    next: &RunSnapshot,
) -> Result<(), ContractError> {
    if previous.hook_plan_ref != next.hook_plan_ref
        || !next
            .hook_applications
            .starts_with(&previous.hook_applications)
    {
        return Err(error(ErrorCode::InvalidTransition, "hooks.immutable"));
    }
    for added in &next.hook_applications[previous.hook_applications.len()..] {
        match &added.target {
            HookTarget::BeforeRun
                if previous.usage.model_calls == 0 && previous.tool_ledger.is_empty() => {}
            HookTarget::BeforeModel { model_step_id }
                if next.model_step_id.as_ref() == Some(model_step_id)
                    && !previous
                        .model_ledger
                        .iter()
                        .any(|invocation| &invocation.model_step_id == model_step_id) => {}
            HookTarget::BeforeTool { call_id }
                if previous.tool_ledger.iter().any(|entry| {
                    &entry.call.call_id == call_id
                        && entry.call.bound_input_ref.is_none()
                        && matches!(entry.state, ToolCallState::Planned {})
                }) => {}
            _ => return Err(error(ErrorCode::InvalidTransition, "hooks.target")),
        }
    }
    Ok(())
}

pub(super) fn validate_hook_observation(
    state: &ScopeState,
    scope: &Scope,
    run_id: &Id,
    report: &crate::HookObservation,
) -> Result<(), ContractError> {
    check_scope(scope, &report.scope)?;
    if run_id != &report.run_id {
        return Err(error(ErrorCode::InvalidReference, "hooks.observation_run"));
    }
    let run = state.runs.get(run_id).ok_or_else(not_found)?;
    let empty = BTreeMap::new();
    let plan = load_plan(state, &empty, &run.snapshot)?
        .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "hooks.plan_missing"))?;
    if !plan
        .definition_for(&report.hook, report.selection.as_ref())
        .is_some_and(|definition| {
            definition.digest() == report.definition_digest
                && definition.position == report.target.position()
        })
    {
        return Err(error(
            ErrorCode::InvalidReference,
            "hooks.observer_definition",
        ));
    }
    let (input, committed_at) = match &report.target {
        HookTarget::AfterTool {
            call_id,
            result_ref,
        } => {
            let result: ToolResult = event_record(state, &empty, result_ref)?;
            let event = run
                .events
                .iter()
                .find(|event| match &event.payload {
                    RunEventPayload::ToolSettled { result_ref: saved }
                    | RunEventPayload::ToolUnresolved {
                        result_ref: saved, ..
                    } => saved == result_ref,
                    _ => false,
                })
                .ok_or_else(|| error(ErrorCode::InvalidReference, "hooks.observed_result"))?;
            if call_id != &result.call_id
                || !run
                    .snapshot
                    .tool_ledger
                    .iter()
                    .any(|entry| &entry.call.call_id == call_id)
            {
                return Err(error(ErrorCode::InvalidReference, "hooks.observed_call"));
            }
            (
                HookInput::tool_observed(call_id, &result),
                event.timestamp_ms,
            )
        }
        HookTarget::AfterRun {
            outcome_ref,
            revision,
        } => {
            let outcome: crate::RunOutcome = event_record(state, &empty, outcome_ref)?;
            let event = run
                .events
                .iter()
                .find(|event| {
                    matches!(&event.payload,
                RunEventPayload::RunFinished { outcome_ref: saved } if saved == outcome_ref)
                })
                .ok_or_else(|| error(ErrorCode::InvalidReference, "hooks.observed_outcome"))?;
            if !run.snapshot.status.is_terminal()
                || &run.snapshot.revision != revision
                || run.snapshot.outcome.as_ref() != Some(&outcome)
            {
                return Err(error(ErrorCode::InvalidReference, "hooks.observed_outcome"));
            }
            (HookInput::run_observed(&outcome), event.timestamp_ms)
        }
        _ => return Err(error(ErrorCode::InvalidReference, "hooks.observer_target")),
    };
    if report.timestamp_ms < committed_at
        || report.input_digest
            != canonical_digest(
                &serde_json::to_value(&input)
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "hooks.observer_input"))?,
            )
    {
        return Err(error(ErrorCode::InvalidSnapshot, "hooks.observer_input"));
    }
    Ok(())
}
```

## `crates/wickle/src/tool_execution.rs`

```rust
use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, fmt, sync::Arc};
use tokio_util::sync::CancellationToken;

mod resume;
mod round;

/// Identity and controls for one physical tool call. Credentials and unrelated
/// system inputs remain in the executor's Host-owned binding.
#[derive(Debug, Clone)]
pub struct ToolExecutionContext {
    /// Owning Run, checked independently of the process-local binding set.
    pub run_id: Id,
    /// Scoped runtime segment; absent for directly injected catalog executors.
    pub binding_set_id: Option<Id>,
    /// Logical call whose plan and bound input were already saved.
    pub call_id: Id,
    /// Charged physical attempt, already recorded before execution.
    pub attempt_id: Id,
    /// Stable external deduplication identity across recovery of this call.
    pub idempotency_key: Id,
    /// Exact authorized namespace.
    pub scope: Scope,
    /// Current authenticated actor.
    pub principal_ref: Id,
    /// Current Host authorization grant.
    pub capability_grant_ref: Id,
    /// Cancelled when the attempt stops, including timeout or caller cancellation.
    pub cancellation: CancellationToken,
    /// Finite execution deadline.
    pub deadline: tokio::time::Instant,
}

/// Effect information attested by the trusted executor, independent of output validation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolEffect {
    /// The executor confirms no external business write occurred.
    NotApplied,
    /// An external business write is confirmed; its receipt must be retained.
    Applied,
    /// Whether an external business write occurred could not be established.
    #[default]
    Unknown,
}

/// A handler's safe result; it cannot replace core call identities or ledger state.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolExecutionOutcome {
    /// Returned value to check against the pinned output schema.
    Succeeded {
        /// Raw returned JSON; only a validated, bounded value becomes model content.
        value: Value,
    },
    /// Classified handler failure, independent of whether a write happened.
    Failed {
        /// Safe registered failure code, without SDK error messages or payloads.
        code: Id,
    },
    /// Ask the Host for the value that will complete this call, without rerunning
    /// the executor. Requires NotApplied and no receipt. The pinned output schema
    /// validates the answer; this does not suspend and resume handler code.
    InputRequired {
        /// Bounded question displayed to the authorized caller.
        question: String,
    },
}

/// Explicit completion and effect receipt. Serialize only for protected storage.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolExecutionResult {
    /// Returned value or safe failure classification.
    pub outcome: ToolExecutionOutcome,
    /// Observed external effect status.
    pub effect: ToolEffect,
    /// Original effect receipt, required for a confirmed Applied result.
    pub receipt: Option<Value>,
}
impl fmt::Debug for ToolExecutionOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Succeeded { .. } => "ToolExecutionOutcome::Succeeded(<protected>)",
            Self::Failed { .. } => "ToolExecutionOutcome::Failed(<classified>)",
            Self::InputRequired { .. } => "ToolExecutionOutcome::InputRequired(<protected>)",
        })
    }
}
impl fmt::Debug for ToolExecutionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolExecutionResult")
            .field("effect", &self.effect)
            .field("has_receipt", &self.receipt.is_some())
            .finish_non_exhaustive()
    }
}

/// Exactly one physical execution. Implementations must not hide retry loops or
/// spawn untracked operations; effect uncertainty must be reported honestly.
pub trait ToolExecutor: Send + Sync {
    /// Execute only the final policy-approved arguments, not the original model
    /// map, full system-input snapshot, or caller-supplied tool identities.
    fn execute<'a>(
        &'a self,
        execution_args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult>;
}

/// Exact saved effect and protected evidence presented to a trusted Host verifier.
/// The Host must authenticate the receipt and its ownership, not merely compare
/// caller-supplied IDs. This value must not be sent to a model or ordinary logs.
#[derive(Clone)]
pub struct ExternalReceiptRequest {
    /// Original saved logical call, including its immutable input reference.
    pub call: ToolCall,
    /// Original uncertain physical attempt; no new execution is requested.
    pub attempt_id: Id,
    /// Original external deduplication identity.
    pub idempotency_key: Id,
    /// Restored, policy-authorized final arguments for that same call.
    pub bound_input: BoundToolInput,
    /// Exact record authorized and retrieved by the Agent before verification.
    pub receipt_ref: RecordRef,
    /// Protected record contents, not an untrusted substitute for the reference.
    pub receipt: Value,
}
impl fmt::Debug for ExternalReceiptRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ExternalReceiptRequest(<protected>)")
    }
}

/// Current authorization and finite controls for a read-only receipt inspection.
#[derive(Debug, Clone)]
pub struct ExternalReceiptContext {
    /// Exact namespace of the waiting run and receipt.
    pub scope: Scope,
    /// Current authenticated actor.
    pub principal_ref: Id,
    /// Current Host authorization grant.
    pub capability_grant_ref: Id,
    /// Cancelled when verification stops or times out.
    pub cancellation: CancellationToken,
    /// Finite callback deadline.
    pub deadline: tokio::time::Instant,
}

/// Read-only Host attestation of a previously uncertain effect. It must not
/// execute or retry the business operation. Unknown leaves the wait unresolved.
pub trait ExternalReceiptVerifier: Send + Sync {
    /// Return an authenticated result for the original call and frozen target.
    fn verify<'a>(
        &'a self,
        request: &'a ExternalReceiptRequest,
        context: &'a ExternalReceiptContext,
    ) -> PortFuture<'a, ToolExecutionResult>;
}

/// Prepared settlement for one authorized resume command. No state is committed
/// here: the Agent saves this alongside command consumption and RunResumed in one
/// transaction. Protected values deliberately have no ordinary Debug output.
pub struct PreparedToolResolution {
    /// Final observation for the original logical call.
    pub result: ToolResult,
    /// Replacement ledger state for that call.
    pub state: ToolCallState,
    /// Paired result, or explicit correction of the old Unknown observation.
    pub message: Message,
    /// Immutable result and diagnostic/effect records needed by the settlement.
    pub records: Vec<ProtectedRecord>,
    /// Next ToolSettled event; the Agent sequences RunResumed after it.
    pub event: RunEvent,
}
impl fmt::Debug for PreparedToolResolution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PreparedToolResolution(<protected>)")
    }
}

struct AttemptIdentity<'a> {
    scope: &'a Scope,
    attempt_id: &'a Id,
    idempotency_key: &'a Id,
}

/// A trusted Host associates one compiled contract with an existing executor.
/// Factory-level code/manifest attestation is separate from this registration.
#[derive(Clone)]
pub struct ToolRegistration {
    /// Exact descriptor and model-input projection.
    pub compiled: CompiledTool,
    /// Existing scoped executor; construction and credentials remain in Host code.
    pub executor: Arc<dyn ToolExecutor>,
}
impl fmt::Debug for ToolRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolRegistration")
            .field("compiled", &self.compiled)
            .finish_non_exhaustive()
    }
}

/// Scope-bound, immutable mapping of exact tool contracts to existing executors.
#[derive(Debug)]
pub struct ToolRegistry {
    scope: Scope,
    entries: BTreeMap<Id, ToolRegistration>,
    selections: BTreeMap<Id, ToolBindingRef>,
}
impl ToolRegistry {
    /// Register without invoking handlers; duplicate names and exact tool identities fail.
    pub fn new(scope: Scope, entries: Vec<ToolRegistration>) -> Result<Self, ContractError> {
        let mut registered = BTreeMap::new();
        for entry in entries {
            if registered.values().any(|prior: &ToolRegistration| {
                prior.compiled.descriptor().tool == entry.compiled.descriptor().tool
            }) || registered
                .insert(entry.compiled.descriptor().name.clone(), entry)
                .is_some()
            {
                return Err(error(
                    ErrorCode::InvalidToolInputContract,
                    "tools.duplicate",
                ));
            }
        }
        let selections = registered
            .iter()
            .map(|(name, entry)| {
                (
                    name.clone(),
                    ToolBindingRef::Catalog(CatalogToolRef {
                        tool_id: entry.compiled.descriptor().tool.id.clone(),
                        version: entry.compiled.descriptor().tool.version.clone(),
                        bindings: None,
                        config: None,
                    }),
                )
            })
            .collect();
        Ok(Self {
            scope,
            entries: registered,
            selections,
        })
    }
    /// Construct a fully attested registry without replacing real Export selections
    /// by catalog aliases. Duplicate visible names or selections are rejected.
    pub fn from_bindings(
        scope: Scope,
        bindings: Vec<(ToolBindingRef, ToolRegistration)>,
    ) -> Result<Self, ContractError> {
        let mut entries = BTreeMap::new();
        let mut selections = BTreeMap::new();
        for (selection, entry) in bindings {
            let valid = match &selection {
                ToolBindingRef::Catalog(reference) => {
                    reference.tool_id == entry.compiled.descriptor().tool.id
                        && reference.version == entry.compiled.descriptor().tool.version
                }
                ToolBindingRef::Export(reference) => reference
                    .alias
                    .as_ref()
                    .is_none_or(|alias| alias == &entry.compiled.descriptor().name),
            };
            if !valid
                || selections.values().any(|prior| prior == &selection)
                || entries.contains_key(&entry.compiled.descriptor().name)
            {
                return Err(error(
                    ErrorCode::InvalidToolInputContract,
                    "tools.selection",
                ));
            }
            selections.insert(entry.compiled.descriptor().name.clone(), selection);
            entries.insert(entry.compiled.descriptor().name.clone(), entry);
        }
        Ok(Self {
            scope,
            entries,
            selections,
        })
    }
    /// Metadata-only view for input validation and cancellation/expiry settlement.
    /// Its placeholder executors perform no I/O and report NotApplied unavailable.
    pub fn metadata(
        scope: Scope,
        bindings: Vec<ResolvedToolBinding>,
    ) -> Result<Self, ContractError> {
        Self::from_bindings(
            scope,
            bindings
                .into_iter()
                .map(|binding| {
                    (
                        binding.selection,
                        ToolRegistration {
                            compiled: binding.compiled,
                            executor: Arc::new(MetadataTool),
                        },
                    )
                })
                .collect(),
        )
    }
    /// Actual authority selection behind a model-visible name.
    pub fn selection(&self, name: &Id) -> Option<&ToolBindingRef> {
        self.selections.get(name)
    }
    /// Exact namespace under which handlers were registered.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Inspect an exact portable name without executing it or resolving an alias.
    pub fn get(&self, name: &Id) -> Option<&ToolRegistration> {
        self.entries.get(name)
    }
    /// Return only profile-selected contracts in profile order. Adapter exports
    /// require their separate runtime factory and are not implicitly opened here.
    pub fn prompt_bindings(
        &self,
        profile: &AgentProfile,
    ) -> Result<Vec<PromptToolBinding>, ContractError> {
        profile
            .tools
            .iter()
            .map(|selection| {
                let entry = self
                    .entries
                    .iter()
                    .find(|(name, entry)| match selection {
                        ToolBindingRef::Catalog(reference) => {
                            entry.compiled.descriptor().tool.id == reference.tool_id
                                && entry.compiled.descriptor().tool.version == reference.version
                        }
                        ToolBindingRef::Export(_) => self.selections.get(*name) == Some(selection),
                    })
                    .ok_or_else(|| error(ErrorCode::ComponentUnavailable, "tools.selection"))?;
                Ok(PromptToolBinding {
                    selection: selection.clone(),
                    compiled: entry.1.compiled.clone(),
                })
            })
            .collect()
    }
}
struct MetadataTool;
impl ToolExecutor for MetadataTool {
    fn execute<'a>(
        &'a self,
        _: &'a JsonObject,
        _: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async {
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Failed {
                    code: Id::new("component_unavailable")?,
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}

/// Per-attempt bounds. The Run still owns total attempts, recovery and elapsed time.
#[derive(Debug, Clone, Copy)]
pub struct ToolExecutionLimits {
    /// Maximum elapsed time for one executor callback.
    pub timeout_ms: u64,
    /// Maximum raw effect-receipt size accepted from a handler.
    pub max_receipt_bytes: usize,
}
impl Default for ToolExecutionLimits {
    fn default() -> Self {
        Self {
            timeout_ms: 30_000,
            max_receipt_bytes: 65_536,
        }
    }
}

/// Whether the complete saved round is safe to follow with another model step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolRoundOutcome {
    /// Every planned call has a settled result and no unknown external effect remains.
    Completed,
    /// A fixed bound candidate requires the separate approval runtime.
    ApprovalRequired {
        /// Call whose immutable candidate was saved.
        call_id: Id,
        /// Current safe policy reason.
        reason: Id,
        /// Exact saved candidate; approval cannot rebind its system inputs.
        bound_input_ref: RecordRef,
        /// Identity of the final model-and-system argument binding.
        binding_digest: JsonDigest,
    },
    /// A no-effect input tool has saved its question and original attempt.
    InputRequired {
        /// Stable request answered through an authorized resume command.
        request: InputRequest,
    },
    /// A prior or current attempt requires explicit effect reconciliation.
    Unresolved {
        /// Call that prevents further tool and model dispatch.
        call_id: Id,
        /// Protected uncertainty observation committed with the matching event.
        result_ref: RecordRef,
    },
}

/// Serial execution of a previously committed model tool round.
pub struct SerialToolRound {
    binding_set_id: Option<Id>,
    registry: Arc<ToolRegistry>,
    binder: Arc<InputBinder>,
    policy: Arc<PolicyGate>,
    ids: Arc<dyn IdSource>,
    limits: ToolExecutionLimits,
    hooks: Option<Arc<HookRuntime>>,
    observer_error: std::sync::Mutex<Option<ContractError>>,
}
impl SerialToolRound {
    /// Inject existing bindings; no tool is run or looked up externally here.
    pub fn new(
        registry: Arc<ToolRegistry>,
        binder: Arc<InputBinder>,
        policy: Arc<PolicyGate>,
        ids: Arc<dyn IdSource>,
    ) -> Self {
        Self {
            binding_set_id: None,
            registry,
            binder,
            policy,
            ids,
            limits: ToolExecutionLimits::default(),
            hooks: None,
            observer_error: std::sync::Mutex::new(None),
        }
    }
    /// Pin the Host runtime segment identity forwarded to wrapped exports.
    pub fn with_binding_set_id(mut self, binding_set_id: Id) -> Self {
        self.binding_set_id = Some(binding_set_id);
        self
    }
    /// Require finite nonzero timeout and receipt limits.
    pub fn with_limits(mut self, limits: ToolExecutionLimits) -> Result<Self, ContractError> {
        if limits.timeout_ms == 0 || limits.timeout_ms > 86_400_000 || limits.max_receipt_bytes == 0
        {
            return Err(error(ErrorCode::InvalidConfiguration, "tools.limits"));
        }
        self.limits = limits;
        Ok(self)
    }
    /// Connect the pinned lifecycle runtime without running a callback.
    pub fn with_hooks(mut self, hooks: Arc<HookRuntime>) -> Self {
        self.hooks = Some(hooks);
        self
    }
    /// A local observer-report persistence error, separate from Tool execution.
    pub fn observer_error(&self) -> Option<ContractError> {
        self.observer_error
            .lock()
            .ok()
            .and_then(|error| error.clone())
    }
}
fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
```

## `crates/wickle/src/tool_execution/round.rs`

```rust
use super::*;
use futures_util::FutureExt;
use std::{panic::AssertUnwindSafe, time::Duration};

impl SerialToolRound {
    /// Execute only calls from one committed physical model response, in saved order.
    /// Existing settled results are reused, and uncertain attempts are never retried.
    pub async fn execute(
        &self,
        model_request_id: &Id,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<ToolRoundOutcome, ContractError> {
        self.scope(context, budget)?;
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        if let Some(entry) = saved.snapshot.tool_ledger.iter().find(|entry| {
            matches!(entry.state, ToolCallState::Unknown { .. } | ToolCallState::Dispatching { .. })
                || matches!(&entry.state, ToolCallState::Settled { result } if result.status == ToolResultStatus::Unknown || result.effect == ToolEffect::Unknown)
        }) {
            return self.existing_uncertainty(entry, budget).await;
        }
        if let Some(request) = saved.snapshot.tool_ledger.iter().find_map(|entry| {
            if let ToolCallState::InputPending { request, .. } = &entry.state {
                Some(request.clone())
            } else {
                None
            }
        }) {
            return Ok(ToolRoundOutcome::InputRequired { request });
        }
        let call_ids: Vec<_> = saved
            .snapshot
            .tool_ledger
            .iter()
            .filter(|entry| &entry.call.model_request_id == model_request_id)
            .map(|entry| entry.call.call_id.clone())
            .collect();
        for call_id in call_ids {
            self.boundary(context, budget).await?;
            let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
            let entry = saved
                .snapshot
                .tool_ledger
                .iter()
                .find(|entry| entry.call.call_id == call_id)
                .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
            match &entry.state {
                ToolCallState::Settled { result }
                    if result.status != ToolResultStatus::Unknown
                        && result.effect != ToolEffect::Unknown =>
                {
                    continue;
                }
                ToolCallState::Planned {} | ToolCallState::ApprovalPending { .. } => {}
                ToolCallState::InputPending { request, .. } => {
                    return Ok(ToolRoundOutcome::InputRequired {
                        request: request.clone(),
                    });
                }
                _ => return self.existing_uncertainty(entry, budget).await,
            }
            let call = entry.call.clone();
            let pending_key = if let ToolCallState::ApprovalPending {
                idempotency_key, ..
            } = &entry.state
            {
                Some(idempotency_key.clone())
            } else {
                None
            };
            let call_message_id = call_message(&saved, &call)?;
            let registered = self.registry.get(&call.tool_name);
            let Some(registered) = registered.filter(|entry| {
                call.descriptor_digest.as_ref() == Some(entry.compiled.descriptor_digest())
            }) else {
                self.reject(
                    &call,
                    call_message_id,
                    ToolResultStatus::Failed,
                    "unknown_tool",
                    budget,
                    context,
                )
                .await?;
                continue;
            };
            if registered
                .compiled
                .validate_model_inputs(&call.model_inputs)
                .is_err()
            {
                self.reject(
                    &call,
                    call_message_id,
                    ToolResultStatus::Failed,
                    "invalid_arguments",
                    budget,
                    context,
                )
                .await?;
                continue;
            }
            if call.bound_input_ref.is_none() {
                if let Some(hooks) = &self.hooks {
                    let transformed = hooks
                        .transform(
                            HookTarget::BeforeTool {
                                call_id: call_id.clone(),
                            },
                            HookInput::BeforeTool {
                                tool: registered.compiled.to_model_tool(),
                                descriptor_digest: registered.compiled.descriptor_digest().clone(),
                                compiled_digest: registered.compiled.digest().clone(),
                                original_model_inputs: call.model_inputs.clone(),
                                model_inputs: call.model_inputs.clone(),
                            },
                            context,
                            budget,
                        )
                        .await?;
                    if let Some(reason) = transformed.deny {
                        self.reject(
                            &call,
                            call_message_id,
                            ToolResultStatus::Denied,
                            reason.as_str(),
                            budget,
                            context,
                        )
                        .await?;
                        continue;
                    }
                    if let Some(inputs) = transformed.model_inputs {
                        registered.compiled.validate_model_inputs(&inputs)?;
                    }
                }
            }
            let bound = match self
                .binder
                .bind(&registered.compiled, &call_id, context, budget)
                .await
            {
                Ok(bound) => bound,
                Err(error) if control_or_storage(error.code) => return Err(error),
                Err(error) => {
                    let status = if error.code == ErrorCode::AccessDenied {
                        ToolResultStatus::Denied
                    } else {
                        ToolResultStatus::Failed
                    };
                    self.reject(
                        &call,
                        call_message_id,
                        status,
                        &code_name(error.code),
                        budget,
                        context,
                    )
                    .await?;
                    continue;
                }
            };
            if let PolicyDecision::RequireApproval { reason } = bound.decision {
                return Ok(ToolRoundOutcome::ApprovalRequired {
                    call_id,
                    reason,
                    bound_input_ref: bound.reference,
                    binding_digest: bound.input.binding_digest().clone(),
                });
            }
            match self.authorize(&bound.input, context, budget).await? {
                PolicyDecision::RequireApproval { reason } => {
                    return Ok(ToolRoundOutcome::ApprovalRequired {
                        call_id,
                        reason,
                        bound_input_ref: bound.reference,
                        binding_digest: bound.input.binding_digest().clone(),
                    });
                }
                PolicyDecision::Deny { .. } => {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Denied,
                        "access_denied",
                        budget,
                        context,
                    )
                    .await?;
                    continue;
                }
                PolicyDecision::Allow {} => {}
            }
            let reservation = budget
                .reserve(ReservationKind::Tool {
                    call_id: call_id.clone(),
                })
                .await?;
            let key = if let Some(key) = pending_key {
                key
            } else {
                Id::new(format!(
                    "tool-effect-{}",
                    canonical_digest(&serde_json::json!([
                        budget.scope(),
                        budget.run_id(),
                        call_id,
                        bound.input.binding_digest()
                    ]))
                ))?
            };
            self.dispatching(&call_id, &reservation.attempt_id, &key, budget)
                .await?;
            let gate = async {
                self.boundary(context, budget).await?;
                let decision = self.authorize(&bound.input, context, budget).await?;
                self.boundary(context, budget).await?;
                Ok::<_, ContractError>(decision)
            }
            .await;
            match gate {
                Ok(PolicyDecision::Allow {}) => {}
                Ok(PolicyDecision::RequireApproval { reason }) => {
                    self.approval_pending(&call_id, &reservation.attempt_id, &key, budget)
                        .await?;
                    return Ok(ToolRoundOutcome::ApprovalRequired {
                        call_id,
                        reason,
                        bound_input_ref: bound.reference,
                        binding_digest: bound.input.binding_digest().clone(),
                    });
                }
                Ok(PolicyDecision::Deny { .. }) => {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Denied,
                        "access_denied",
                        budget,
                        context,
                    )
                    .await?;
                    continue;
                }
                Err(error)
                    if matches!(
                        error.code,
                        ErrorCode::Cancelled | ErrorCode::DeadlineExceeded
                    ) =>
                {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Cancelled,
                        &code_name(error.code),
                        budget,
                        context,
                    )
                    .await?;
                    return Err(error);
                }
                Err(error) if control_or_storage(error.code) => return Err(error),
                Err(error) => {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Denied,
                        &code_name(error.code),
                        budget,
                        context,
                    )
                    .await?;
                    continue;
                }
            }
            let cancellation = budget.cancellation().child_token();
            let _cancel = cancellation.clone().drop_guard();
            let run_deadline = match budget.call_deadline() {
                Ok(deadline) => deadline,
                Err(error) if error.code == ErrorCode::DeadlineExceeded => {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Cancelled,
                        "deadline_exceeded",
                        budget,
                        context,
                    )
                    .await?;
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
            let deadline = tokio::time::Instant::now()
                .checked_add(Duration::from_millis(self.limits.timeout_ms))
                .ok_or_else(|| error(ErrorCode::InvalidConfiguration, "tool.timeout"))?
                .min(run_deadline);
            let execution = ToolExecutionContext {
                run_id: budget.run_id().clone(),
                binding_set_id: self.binding_set_id.clone(),
                call_id: call_id.clone(),
                attempt_id: reservation.attempt_id.clone(),
                idempotency_key: key.clone(),
                scope: budget.scope().clone(),
                principal_ref: context.data.principal_ref.clone(),
                capability_grant_ref: context.data.capability_grant_ref.clone(),
                cancellation,
                deadline,
            };
            let mut entered = false;
            let result = {
                let operation = AssertUnwindSafe(async {
                    entered = true;
                    registered
                        .executor
                        .execute(bound.input.execution_args(), &execution)
                        .await
                })
                .catch_unwind();
                tokio::select! { biased;
                    _ = context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "tool.execution")),
                    _ = tokio::time::sleep_until(deadline) => Err(error(ErrorCode::DeadlineExceeded, "tool.execution")),
                    stopped = budget.wait_for_cancellation_or_deadline() => match stopped { Err(error) => Err(error), Ok(()) => Err(error(ErrorCode::DeadlineExceeded, "tool.execution")) },
                    result = operation => result.unwrap_or_else(|_| Err(error(ErrorCode::InvalidContract, "tool.executor"))),
                }
            };
            execution.cancellation.cancel();
            let completion = match result {
                Ok(result) => result,
                Err(error) => ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Failed {
                        code: Id::new(code_name(error.code))?,
                    },
                    effect: if entered
                        && registered.compiled.descriptor().side_effect != ToolSideEffect::ReadOnly
                    {
                        ToolEffect::Unknown
                    } else {
                        ToolEffect::NotApplied
                    },
                    receipt: None,
                },
            };
            if let ToolExecutionOutcome::InputRequired { question } = &completion.outcome {
                let question_bytes = serde_json::to_vec(question)
                    .map_err(|_| error(ErrorCode::InvalidJson, "tool.input_question"))?
                    .len();
                if completion.effect == ToolEffect::NotApplied
                    && completion.receipt.is_none()
                    && !question.trim().is_empty()
                    && question_bytes as u64
                        <= registered.compiled.descriptor().max_output_bytes.get()
                {
                    let request = InputRequest {
                        input_request_id: self.ids.next_id()?,
                        call_id: call_id.clone(),
                        question: question.clone(),
                        schema_ref: None,
                    };
                    self.input_pending(&execution, &request, budget).await?;
                    return Ok(ToolRoundOutcome::InputRequired { request });
                }
            }
            let (result, records) = self.validate_output(
                &call,
                call_message_id,
                AttemptIdentity {
                    scope: &execution.scope,
                    attempt_id: &execution.attempt_id,
                    idempotency_key: &execution.idempotency_key,
                },
                &registered.compiled,
                completion,
            )?;
            let unresolved = result.effect == ToolEffect::Unknown;
            let state = if unresolved {
                ToolCallState::Unknown {
                    attempt_id: execution.attempt_id.clone(),
                    idempotency_key: key.clone(),
                }
            } else {
                ToolCallState::Settled {
                    result: result.clone(),
                }
            };
            let result_ref = self
                .settle(&call_id, state, result, records, budget, context)
                .await?;
            if unresolved {
                return Ok(ToolRoundOutcome::Unresolved {
                    call_id,
                    result_ref,
                });
            }
        }
        Ok(ToolRoundOutcome::Completed)
    }

    /// Close unstarted plans and input requests confirmed to have no effect when
    /// a segment ends. Unknown and potentially dispatched calls are untouched.
    pub async fn settle_unstarted(
        &self,
        model_request_id: &Id,
        status: ToolResultStatus,
        code: Id,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        self.scope(context, budget)?;
        if !matches!(
            status,
            ToolResultStatus::Failed | ToolResultStatus::Denied | ToolResultStatus::Cancelled
        ) {
            return Err(error(ErrorCode::InvalidContract, "tool.settlement"));
        }
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        for entry in &saved.snapshot.tool_ledger {
            if &entry.call.model_request_id == model_request_id
                && matches!(
                    entry.state,
                    ToolCallState::Planned {}
                        | ToolCallState::ApprovalPending { .. }
                        | ToolCallState::InputPending { .. }
                )
            {
                self.reject(
                    &entry.call,
                    call_message(&saved, &entry.call)?,
                    status,
                    code.as_str(),
                    budget,
                    context,
                )
                .await?;
            }
        }
        Ok(())
    }

    fn scope(&self, context: &ExecutionContext, budget: &RunBudget) -> Result<(), ContractError> {
        if &context.data.scope != budget.scope() || self.registry.scope() != budget.scope() {
            return Err(error(ErrorCode::AccessDenied, "scope"));
        }
        Ok(())
    }
    async fn boundary(
        &self,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        self.scope(context, budget)?;
        if context.cancellation.is_cancelled() {
            return Err(error(ErrorCode::Cancelled, "tool"));
        }
        budget.check_boundary().await
    }
    async fn authorize(
        &self,
        input: &BoundToolInput,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<PolicyDecision, ContractError> {
        let saved = tokio::select! { biased;
            _ = context.cancellation.cancelled() => return Err(error(ErrorCode::Cancelled, "tool.policy")),
            stopped = budget.wait_for_cancellation_or_deadline() => return match stopped { Err(error) => Err(error), Ok(()) => Err(error(ErrorCode::DeadlineExceeded, "tool.policy")) },
            result = budget.store().load(budget.scope(), budget.run_id()) => result?,
        };
        let request = input.policy_request_for_run(&saved.snapshot);
        tokio::select! { biased;
            _ = context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "tool.policy")),
            stopped = budget.wait_for_cancellation_or_deadline() => match stopped { Err(error) => Err(error), Ok(()) => Err(error(ErrorCode::DeadlineExceeded, "tool.policy")) },
            result = self.policy.check(&request, context, Some(budget.call_deadline()?), None) => result,
        }
    }
    async fn dispatching(
        &self,
        call_id: &Id,
        attempt_id: &Id,
        key: &Id,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let entry = snapshot
            .tool_ledger
            .iter_mut()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
        if !matches!(
            entry.state,
            ToolCallState::Planned {} | ToolCallState::ApprovalPending { .. }
        ) || entry.call.bound_input_ref.is_none()
        {
            return Err(error(ErrorCode::InvalidTransition, "tool.dispatch"));
        }
        if let ToolCallState::ApprovalPending {
            idempotency_key, ..
        } = &entry.state
        {
            if idempotency_key != key {
                return Err(error(ErrorCode::InvalidTransition, "tool.idempotency_key"));
            }
        }
        entry.state = ToolCallState::Dispatching {
            attempt_id: attempt_id.clone(),
            idempotency_key: key.clone(),
        };
        self.commit(snapshot, vec![], vec![], vec![], budget).await
    }
    async fn approval_pending(
        &self,
        call_id: &Id,
        attempt_id: &Id,
        key: &Id,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let entry = snapshot
            .tool_ledger
            .iter_mut()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
        if !matches!(&entry.state, ToolCallState::Dispatching { attempt_id: current, idempotency_key } if current == attempt_id && idempotency_key == key)
        {
            return Err(error(ErrorCode::InvalidTransition, "tool.approval"));
        }
        entry.state = ToolCallState::ApprovalPending {
            attempt_id: attempt_id.clone(),
            idempotency_key: key.clone(),
        };
        self.commit(snapshot, vec![], vec![], vec![], budget).await
    }
    async fn input_pending(
        &self,
        execution: &ToolExecutionContext,
        request: &InputRequest,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let entry = snapshot
            .tool_ledger
            .iter_mut()
            .find(|entry| entry.call.call_id == execution.call_id)
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
        if !matches!(&entry.state, ToolCallState::Dispatching { attempt_id, idempotency_key }
            if attempt_id == &execution.attempt_id && idempotency_key == &execution.idempotency_key)
        {
            return Err(error(ErrorCode::InvalidTransition, "tool.input_pending"));
        }
        entry.state = ToolCallState::InputPending {
            attempt_id: execution.attempt_id.clone(),
            idempotency_key: execution.idempotency_key.clone(),
            request: request.clone(),
        };
        self.commit(snapshot, vec![], vec![], vec![], budget).await
    }
    async fn reject(
        &self,
        call: &ToolCall,
        call_message_id: Id,
        status: ToolResultStatus,
        code: &str,
        budget: &RunBudget,
        context: &ExecutionContext,
    ) -> Result<(), ContractError> {
        let result = ToolResult {
            call_id: call.call_id.clone(),
            call_message_id,
            status,
            effect: ToolEffect::NotApplied,
            content: vec![],
            effect_receipt_ref: None,
            error: Some(Failure {
                code: Id::new(code)?,
                diagnostic_ref: None,
            }),
        };
        self.settle(
            &call.call_id,
            ToolCallState::Settled {
                result: result.clone(),
            },
            result,
            vec![],
            budget,
            context,
        )
        .await?;
        Ok(())
    }

    pub(super) fn validate_output(
        &self,
        call: &ToolCall,
        call_message_id: Id,
        attempt: AttemptIdentity<'_>,
        compiled: &CompiledTool,
        completion: ToolExecutionResult,
    ) -> Result<(ToolResult, Vec<ProtectedRecord>), ContractError> {
        let effect = completion.effect;
        let receipt_bytes = completion
            .receipt
            .as_ref()
            .map(|receipt| serde_json::to_vec(receipt).map(|bytes| bytes.len()))
            .transpose()
            .map_err(|_| error(ErrorCode::InvalidJson, "tool.receipt"))?;
        let receipt_oversized =
            receipt_bytes.is_some_and(|size| size > self.limits.max_receipt_bytes);
        let raw_receipt = if receipt_oversized {
            serde_json::json!({"omitted":true,"bytes":receipt_bytes,"digest":canonical_digest(completion.receipt.as_ref().expect("oversized receipt"))})
        } else {
            completion.receipt.clone().unwrap_or(Value::Null)
        };
        let mut status = ToolResultStatus::Succeeded;
        let mut code = None;
        let mut content = vec![];
        let raw_output = match &completion.outcome {
            ToolExecutionOutcome::Succeeded { value } => {
                let bytes = serde_json::to_vec(value)
                    .map_err(|_| error(ErrorCode::InvalidJson, "tool.output"))?
                    .len();
                if bytes as u64 > compiled.descriptor().max_output_bytes.get() {
                    status = ToolResultStatus::Failed;
                    code = Some(Id::new("tool_output_too_large")?);
                    serde_json::json!({"omitted":true,"bytes":bytes,"digest":canonical_digest(value)})
                } else {
                    if !crate::tool_schema::compile_validator(&compiled.descriptor().output_schema)?
                        .is_valid(value)
                    {
                        status = ToolResultStatus::Failed;
                        code = Some(Id::new("invalid_tool_output")?);
                    } else {
                        content.push(InputContent::Json {
                            value: value.clone(),
                        });
                    }
                    value.clone()
                }
            }
            ToolExecutionOutcome::Failed { code: failure } => {
                status = if failure.as_str() == "cancelled" {
                    ToolResultStatus::Cancelled
                } else {
                    ToolResultStatus::Failed
                };
                code = Some(failure.clone());
                Value::Null
            }
            ToolExecutionOutcome::InputRequired { .. } => {
                // Only a bounded no-effect request is accepted before this path.
                // Invalid requests retain any reported effect and receipt.
                status = ToolResultStatus::Failed;
                code = Some(Id::new("invalid_input_request")?);
                Value::Null
            }
        };
        if effect == ToolEffect::Unknown {
            status = ToolResultStatus::Unknown;
            code = Some(Id::new("tool_effect_unknown")?);
            content.clear();
        } else if receipt_oversized {
            status = ToolResultStatus::Failed;
            code = Some(Id::new("effect_receipt_too_large")?);
            content.clear();
        } else if effect == ToolEffect::Applied && completion.receipt.is_none() {
            status = ToolResultStatus::Failed;
            code = Some(Id::new("effect_receipt_missing")?);
            content.clear();
        } else if effect == ToolEffect::Applied
            && compiled.descriptor().side_effect == ToolSideEffect::ReadOnly
        {
            status = ToolResultStatus::Failed;
            code = Some(Id::new("tool_effect_contract")?);
            content.clear();
        }
        let record = ProtectedRecord::new(
            self.ids.next_id()?,
            1,
            serde_json::json!({
                "scope":attempt.scope,"call_id":call.call_id,"attempt_id":attempt.attempt_id,"idempotency_key":attempt.idempotency_key,
                "effect":effect,"receipt":raw_receipt,"receipt_omitted":receipt_oversized,"output":raw_output,"error_code":code,
            }),
        );
        let reference = record.reference().clone();
        Ok((
            ToolResult {
                call_id: call.call_id.clone(),
                call_message_id,
                status,
                effect,
                content,
                effect_receipt_ref: (effect != ToolEffect::NotApplied
                    || completion.receipt.is_some())
                .then(|| reference.clone()),
                error: code.map(|code| Failure {
                    code,
                    diagnostic_ref: Some(reference),
                }),
            },
            vec![record],
        ))
    }

    async fn settle(
        &self,
        call_id: &Id,
        state: ToolCallState,
        result: ToolResult,
        mut records: Vec<ProtectedRecord>,
        budget: &RunBudget,
        context: &ExecutionContext,
    ) -> Result<RecordRef, ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let entry = snapshot
            .tool_ledger
            .iter_mut()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
        if matches!(
            entry.state,
            ToolCallState::Settled { .. } | ToolCallState::Unknown { .. }
        ) {
            return Err(error(ErrorCode::InvalidTransition, "tool.settlement"));
        }
        entry.state = state.clone();
        let intended_state = state.clone();
        let observer_input = HookInput::tool_observed(call_id, &result);
        let record = ProtectedRecord::new(
            self.ids.next_id()?,
            1,
            serde_json::to_value(&result)
                .map_err(|_| error(ErrorCode::InvalidJson, "tool.result"))?,
        );
        let reference = record.reference().clone();
        let intended_value = record.value().clone();
        records.push(record);
        snapshot.last_event_seq = snapshot
            .last_event_seq
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::InvalidEvent, "tool.event"))?;
        let (_, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: self.ids.next_id()?,
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            session_id: snapshot.request.session_id.clone(),
            seq: snapshot
                .last_event_seq
                .try_into()
                .map_err(|_| error(ErrorCode::InvalidEvent, "tool.event"))?,
            timestamp_ms: now,
            payload: match state {
                ToolCallState::Unknown {
                    attempt_id,
                    idempotency_key,
                } => RunEventPayload::ToolUnresolved {
                    result_ref: reference.clone(),
                    attempt_id,
                    idempotency_key,
                },
                _ => RunEventPayload::ToolSettled {
                    result_ref: reference.clone(),
                },
            },
        };
        let message = Message {
            message_id: self.ids.next_id()?,
            run_id: budget.run_id().clone(),
            sequence: saved
                .session
                .transcript_revision
                .checked_add(1)
                .and_then(std::num::NonZeroU64::new)
                .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.message"))?,
            role: MessageRole::Tool,
            content: vec![ContentBlock::ToolResult { result }],
            origin: MessageOrigin::Tool,
            visibility: Visibility::UserAndModel,
        };
        let committed = self
            .commit(snapshot, vec![message], vec![event], records, budget)
            .await;
        if let Err(error) = committed {
            let restored = budget.store().read_record(budget.scope(), &reference).await;
            if !restored.is_ok_and(|record| {
                record.reference() == &reference && record.value() == &intended_value
            }) {
                return Err(error);
            }
            let Ok(saved) = budget.store().load(budget.scope(), budget.run_id()).await else {
                return Err(error);
            };
            let found = saved
                .snapshot
                .tool_ledger
                .iter()
                .find(|entry| &entry.call.call_id == call_id);
            if !found.is_some_and(|entry| entry.state == intended_state) {
                return Err(error);
            }
        }
        if let Some(hooks) = &self.hooks {
            let mut data = context.data.clone();
            data.system_inputs = None;
            let cleanup = ExecutionContext::new(data, CancellationToken::new());
            if let Err(error) = hooks
                .observe(
                    budget.run_id(),
                    HookTarget::AfterTool {
                        call_id: call_id.clone(),
                        result_ref: reference.clone(),
                    },
                    observer_input,
                    &cleanup,
                )
                .await
            {
                if let Ok(mut slot) = self.observer_error.lock() {
                    *slot = Some(ContractError::new(error.code, "hooks.observer_report"));
                }
            }
        }
        Ok(reference)
    }
    async fn commit(
        &self,
        mut snapshot: RunSnapshot,
        messages: Vec<Message>,
        events: Vec<RunEvent>,
        records: Vec<ProtectedRecord>,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let expected_revision = snapshot.revision;
        let (_, check_at) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let lease = budget
            .store()
            .check_lease(budget.scope(), budget.run_id(), budget.lease(), check_at)
            .await?;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        if now >= lease.expires_at_ms {
            return Err(error(ErrorCode::LeaseLost, "tool.lease"));
        }
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::RevisionConflict, "tool.revision"))?;
        snapshot.phase = RunPhase::Tool;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        budget
            .store()
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages,
                    events,
                    records,
                },
            )
            .await?;
        Ok(())
    }
    async fn existing_uncertainty(
        &self,
        entry: &ToolLedgerEntry,
        budget: &RunBudget,
    ) -> Result<ToolRoundOutcome, ContractError> {
        let ToolCallState::Unknown {
            attempt_id,
            idempotency_key,
        } = &entry.state
        else {
            return Err(error(
                ErrorCode::InvalidTransition,
                "tool.unresolved_dispatch",
            ));
        };
        let mut after = 0;
        loop {
            let page = budget
                .store()
                .read_events(budget.scope(), budget.run_id(), after, MAX_EVENT_PAGE_SIZE)
                .await?;
            for event in &page.events {
                if let RunEventPayload::ToolUnresolved {
                    result_ref,
                    attempt_id: saved_attempt,
                    idempotency_key: saved_key,
                } = &event.payload
                {
                    if saved_attempt == attempt_id && saved_key == idempotency_key {
                        return Ok(ToolRoundOutcome::Unresolved {
                            call_id: entry.call.call_id.clone(),
                            result_ref: result_ref.clone(),
                        });
                    }
                }
            }
            if !page.has_more {
                return Err(error(ErrorCode::InvalidSnapshot, "tool.unresolved_result"));
            }
            after = page.next_after_seq;
        }
    }
}

pub(super) fn call_message(saved: &StoredRun, call: &ToolCall) -> Result<Id, ContractError> {
    let messages: Vec<_> = saved.messages.iter().filter(|message| message.run_id == saved.snapshot.run_id && message.role == MessageRole::Assistant && message.content.iter().any(|content| matches!(content, ContentBlock::ToolCall { call: candidate } if candidate.call_id == call.call_id && candidate.model_request_id == call.model_request_id && candidate.provider_call_id == call.provider_call_id && candidate.tool_name == call.tool_name && candidate.model_inputs == call.model_inputs && candidate.descriptor_digest == call.descriptor_digest))).collect();
    if messages.len() != 1 {
        return Err(error(ErrorCode::InvalidSnapshot, "tool.call_message"));
    }
    Ok(messages[0].message_id.clone())
}
fn code_name(code: ErrorCode) -> String {
    serde_json::to_value(code)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "invalid_contract".into())
}
fn control_or_storage(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::Cancelled
            | ErrorCode::DeadlineExceeded
            | ErrorCode::BudgetExceeded
            | ErrorCode::LeaseLost
            | ErrorCode::RevisionConflict
            | ErrorCode::PersistenceUnavailable
            | ErrorCode::StateNotFound
            | ErrorCode::ClockUnavailable
            | ErrorCode::ClockRegression
    )
}
```

## `crates/wickle/tests/support/agent.rs`

```rust
//! Deterministic Host components for agent runtime lifecycle tests.

use futures_util::{StreamExt, stream};
use serde_json::json;
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};
use wickle::*;

pub fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
pub fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    }
}
pub fn reference(name: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id("1"),
    }
}
pub fn profile() -> AgentProfile {
    AgentProfile::from_json(r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
        "name":"Assistant","description":"Runtime fixture","instructions":{"text":"Use supplied records"},
        "model_binding":"primary","tools":[],"skills":[],"connectors":[],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":4,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
    }"#).unwrap()
}
pub fn request(name: &str) -> RunRequest {
    RunRequest {
        request_id: id(name),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Retrieve the requested information".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    }
}
pub fn context() -> ExecutionContext {
    ExecutionContext::new(
        ExecutionContextData {
            scope: scope(),
            principal_ref: id("caller"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    )
}
pub fn completed<T>(result: Guarded<T>) -> T {
    match result {
        Guarded::Completed(value) => value,
        Guarded::ApprovalRequired(_) => panic!("unexpected approval"),
    }
}

pub struct TestClock {
    origin: tokio::time::Instant,
}
impl TestClock {
    pub fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
        }
    }
}
impl Clock for TestClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        let elapsed = self.origin.elapsed().as_millis() as u64;
        Ok(ClockReading {
            utc_ms: 1000 + elapsed as i64,
            monotonic_ms: elapsed,
        })
    }
    fn sleep_until<'a>(&'a self, deadline: u64) -> PortFuture<'a, ()> {
        Box::pin(async move {
            tokio::time::sleep_until(self.origin + Duration::from_millis(deadline)).await;
            Ok(())
        })
    }
}
#[derive(Default)]
pub struct Ids(pub AtomicUsize);
impl IdSource for Ids {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id(&format!("id-{}", self.0.fetch_add(1, Ordering::SeqCst))))
    }
}

#[derive(Default)]
pub struct Catalog {
    pub calls: AtomicUsize,
    pub revision: AtomicUsize,
}
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id(&format!(
                        "revision-{}",
                        self.revision.load(Ordering::SeqCst)
                    ))),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!(self.revision.load(Ordering::SeqCst))),
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

#[derive(Default)]
pub struct Policy {
    pub calls: AtomicUsize,
    pub deny: AtomicUsize,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let deny = match self.deny.load(Ordering::SeqCst) {
                1 => matches!(
                    request.action,
                    PolicyAction::ReadRun {}
                        | PolicyAction::ReadRunDetails {}
                        | PolicyAction::ReadEvents {}
                ),
                2 => matches!(request.action, PolicyAction::CancelRun {}),
                3 => matches!(request.action, PolicyAction::StartRun {}),
                _ => false,
            };
            Ok(if deny {
                PolicyDecision::Deny {
                    reason: id("denied"),
                }
            } else {
                PolicyDecision::Allow {}
            })
        })
    }
}

pub struct Router {
    pub snapshot: RoutingSnapshot,
    pub queries: AtomicUsize,
    pub snapshots: AtomicUsize,
}
impl Router {
    pub fn new() -> Self {
        Self::for_provider("fixture")
    }
    pub fn for_provider(provider: &str) -> Self {
        let capabilities = ModelCapabilities {
            revision: id("capabilities"),
            features: [id("text")].into_iter().collect(),
            options_schema: json!({"type":"object","properties":{"effort":{"enum":["low","high"]}},"additionalProperties":false}),
            context_window: 8192.try_into().unwrap(),
            max_output_tokens: 2048.try_into().unwrap(),
        };
        let model = ModelDefinition {
            model_key: id("model"),
            family: id("fixture"),
            provider: id(provider),
            model_id: id("fixture-model"),
            model_version: id("release"),
            version_semantics: VersionSemantics::Pinned,
            lifecycle: ModelLifecycle::Active,
            capabilities: capabilities.clone(),
            evidence: vec![],
        };
        let mut binding = ModelBinding {
            binding: reference("route"),
            model: model.reference(),
            requested_model: model.model_id.clone(),
            adapter: reference("adapter"),
            connection_ref: reference("connection"),
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
            binding_digest: binding.contract_digest(&model).unwrap(),
            checked_at_ms: 1000,
            evidence_ref: id("fixture-proof"),
            passed: true,
        });
        let snapshot = RoutingSnapshot::new(
            ModelCatalogSnapshot {
                revision: id("catalog"),
                scope: scope(),
                models: vec![model],
                bindings: vec![binding],
                aliases: vec![],
            },
            RoutingPolicy {
                revision: id("policy"),
                scope: scope(),
                rules: vec![RoutingRule {
                    model_binding: id("primary"),
                    purpose: ModelPurpose::Agent,
                    primary: reference("route"),
                    fallbacks: vec![],
                    fallback_on: vec![],
                    version_policy: VersionPolicy::RequirePinned,
                    min_support: ModelSupportStatus::ContractTested,
                }],
            },
        )
        .unwrap();
        Self {
            snapshot,
            queries: AtomicUsize::new(0),
            snapshots: AtomicUsize::new(0),
        }
    }
}
impl ModelRouter for Router {
    fn snapshot(&self) -> &RoutingSnapshot {
        self.snapshots.fetch_add(1, Ordering::SeqCst);
        &self.snapshot
    }
    fn resolve<'a>(&'a self, request: &'a RouteRequest) -> PortFuture<'a, RouteSelection> {
        Box::pin(async move {
            self.queries.fetch_add(1, Ordering::SeqCst);
            let selected = RouteSelection {
                route: self.snapshot.route_for_binding(&reference("route"))?,
                reason: if request.previous_route.is_some() {
                    RouteSelectionReason::Reuse
                } else {
                    RouteSelectionReason::Initial
                },
                candidate_index: 0,
                routing_snapshot_digest: self.snapshot.digest(),
                request_digest: request.digest(),
            };
            self.snapshot.validate_selection(request, &selected)?;
            Ok(selected)
        })
    }
}

pub struct Inspector {
    pub calls: AtomicUsize,
}
impl ModelRouteInspector for Inspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        _: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: ModelRouteAvailability::Available,
                model_id: Some(id("fixture-model")),
                model_version: Some(id("release")),
                deployment_revision: None,
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id("synthetic-inspection"),
            })
        })
    }
}
pub struct Estimator {
    pub calls: AtomicUsize,
    pub tokens: AtomicUsize,
}
impl ModelTokenEstimator for Estimator {
    fn estimate(&self, _: &ModelRequest) -> Result<u64, ContractError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.tokens.load(Ordering::SeqCst) as u64)
    }
}

#[derive(Clone, Copy)]
pub enum Response {
    Text,
    WithContinuation,
    TransportFailure,
    Truncated,
    WaitAfterText,
    Panic,
    Tool,
}
pub struct Model {
    pub calls: AtomicUsize,
    pub entered: Notify,
    pub release: Semaphore,
    pub response: Response,
    pub requests: Mutex<Vec<ModelRequest>>,
    pub gated: bool,
    pub port_binding: ModelPortBinding,
}
impl Model {
    pub fn new(response: Response, gated: bool) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Semaphore::new(0),
            response,
            requests: Mutex::new(vec![]),
            gated,
            port_binding: ModelPortBinding {
                provider: id("fixture"),
                adapter: reference("adapter"),
                connection_ref: reference("connection"),
            },
        }
    }
}
impl ModelPort for Model {
    fn binding(&self) -> ModelPortBinding {
        self.port_binding.clone()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        self.entered.notify_one();
        Box::pin(
            stream::once(async move {
                if self.gated {
                    self.release.acquire().await.unwrap().forget();
                }
                if matches!(self.response, Response::Panic) {
                    panic!("synthetic adapter panic");
                }
                let mut events = vec![Ok(ModelEvent::TextDelta {
                    text: "candidate answer".into(),
                })];
                match self.response {
                    Response::Text | Response::Panic | Response::WithContinuation => {
                        events.push(Ok(ModelEvent::ResponseCompleted {
                            finish: ModelFinish::Stop,
                            metadata: ModelResponseMetadata::default(),
                            continuation: if matches!(self.response, Response::WithContinuation) {
                                vec![OpaqueContinuation::new(
                                    &request.route,
                                    json!({"signature":"fixture-signature"}),
                                )]
                            } else {
                                vec![]
                            },
                        }))
                    }
                    Response::TransportFailure => events.push(Ok(ModelEvent::ResponseError {
                        kind: ModelFailureKind::Transport,
                        metadata: ModelResponseMetadata::default(),
                    })),
                    Response::Truncated => events.push(Ok(ModelEvent::ResponseCompleted {
                        finish: ModelFinish::Length,
                        metadata: ModelResponseMetadata::default(),
                        continuation: vec![],
                    })),
                    Response::Tool => {
                        events.push(Ok(ModelEvent::ToolArgumentsDelta {
                            index: 0,
                            provider_call_id: Some("call".into()),
                            name: Some("unregistered".into()),
                            delta: "{}".into(),
                        }));
                        events.push(Ok(ModelEvent::ResponseCompleted {
                            finish: ModelFinish::ToolCalls,
                            metadata: ModelResponseMetadata::default(),
                            continuation: vec![],
                        }));
                    }
                    Response::WaitAfterText => {}
                }
                let trailing = if matches!(self.response, Response::WaitAfterText) {
                    stream::pending().boxed()
                } else {
                    stream::empty().boxed()
                };
                stream::iter(events).chain(trailing)
            })
            .flatten(),
        )
    }
}

pub struct Fixture {
    pub store: Arc<MemoryStateStore>,
    pub policy: Arc<Policy>,
    pub catalog: Arc<Catalog>,
    pub router: Arc<Router>,
    pub inspector: Arc<Inspector>,
    pub estimator: Arc<Estimator>,
    pub model: Arc<Model>,
    pub clock: Arc<TestClock>,
    pub ids: Arc<Ids>,
}

#[derive(Clone, Copy)]
pub enum FinalCommitMode {
    PassThrough,
    Reject,
    LoseAcknowledgement,
    Pause,
    PauseEmptyEventPage,
}
pub struct FinalCommitStore {
    pub inner: Arc<MemoryStateStore>,
    pub mode: FinalCommitMode,
    pub final_entered: Notify,
    pub release: Semaphore,
    pub final_attempts: AtomicUsize,
    pub empty_page_entered: Notify,
    pub empty_page_release: Semaphore,
    paused_empty_page: AtomicBool,
    pub block_read: AtomicUsize,
    pub read_entered: Notify,
}
impl FinalCommitStore {
    pub fn new(inner: Arc<MemoryStateStore>, mode: FinalCommitMode) -> Self {
        Self {
            inner,
            mode,
            final_entered: Notify::new(),
            release: Semaphore::new(0),
            final_attempts: AtomicUsize::new(0),
            empty_page_entered: Notify::new(),
            empty_page_release: Semaphore::new(0),
            paused_empty_page: AtomicBool::new(false),
            block_read: AtomicUsize::new(0),
            read_entered: Notify::new(),
        }
    }
}
impl StateStore for FinalCommitStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        s: &'a Scope,
        session: &'a Id,
        request: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        Box::pin(async move {
            if self
                .block_read
                .compare_exchange(3, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.read_entered.notify_one();
                std::future::pending::<()>().await;
            }
            self.inner.find_request(s, session, request).await
        })
    }
    fn admit<'a>(&'a self, s: &'a Scope, input: AdmissionInput) -> PortFuture<'a, AdmissionResult> {
        self.inner.admit(s, input)
    }
    fn load<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            if self
                .block_read
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.read_entered.notify_one();
                std::future::pending::<()>().await;
            }
            self.inner.load(s, r).await
        })
    }
    fn load_session<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, SessionSnapshot> {
        self.inner.load_session(s, r)
    }
    fn check_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.check_lease(s, r, l, n)
    }
    fn acquire_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        o: &'a Id,
        n: i64,
        t: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.acquire_lease(s, r, o, n, t)
    }
    fn renew_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
        t: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.renew_lease(s, r, l, n, t)
    }
    fn release_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, ()> {
        self.inner.release_lease(s, r, l, n)
    }
    fn read_events<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        after: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage> {
        Box::pin(async move {
            if self
                .block_read
                .compare_exchange(2, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.read_entered.notify_one();
                std::future::pending::<()>().await;
            }
            let page = self.inner.read_events(s, r, after, limit).await?;
            if matches!(self.mode, FinalCommitMode::PauseEmptyEventPage)
                && page.events.is_empty()
                && !self.paused_empty_page.swap(true, Ordering::SeqCst)
            {
                self.empty_page_entered.notify_one();
                self.empty_page_release.acquire().await.unwrap().forget();
            }
            Ok(page)
        })
    }
    fn read_record<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        self.inner.read_record(s, r)
    }
    fn commit<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            if !input.snapshot.status.is_terminal() {
                return self.inner.commit(s, r, input).await;
            }
            self.final_attempts.fetch_add(1, Ordering::SeqCst);
            self.final_entered.notify_one();
            match self.mode {
                FinalCommitMode::Reject => Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "final.commit",
                )),
                FinalCommitMode::LoseAcknowledgement => {
                    self.inner.commit(s, r, input).await?;
                    Err(ContractError::new(
                        ErrorCode::PersistenceUnavailable,
                        "final.ack",
                    ))
                }
                FinalCommitMode::Pause => {
                    self.release.acquire().await.unwrap().forget();
                    self.inner.commit(s, r, input).await
                }
                FinalCommitMode::PauseEmptyEventPage | FinalCommitMode::PassThrough => {
                    self.inner.commit(s, r, input).await
                }
            }
        })
    }
}
impl Fixture {
    pub fn new(response: Response, gated: bool) -> Self {
        Self {
            store: Arc::new(MemoryStateStore::new()),
            policy: Arc::new(Policy::default()),
            catalog: Arc::new(Catalog::default()),
            router: Arc::new(Router::new()),
            inspector: Arc::new(Inspector {
                calls: AtomicUsize::new(0),
            }),
            estimator: Arc::new(Estimator {
                calls: AtomicUsize::new(0),
                tokens: AtomicUsize::new(32),
            }),
            model: Arc::new(Model::new(response, gated)),
            clock: Arc::new(TestClock::new()),
            ids: Arc::new(Ids::default()),
        }
    }
    pub fn bindings(&self) -> AgentBindings {
        let gate = Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap());
        AgentBindings {
            scope: scope(),
            state: self.store.clone(),
            policy: gate.clone(),
            profile_resolver: self.catalog.clone(),
            model_exchange: Arc::new(
                ModelExchange::new(self.model.clone(), gate)
                    .with_route_inspector(self.inspector.clone(), Duration::from_secs(1))
                    .unwrap(),
            ),
            router: self.router.clone(),
            host_instructions: vec!["Trusted host rules".into()],
            system_inputs: SystemInputRegistry::new(vec![]).unwrap(),
            clock: self.clock.clone(),
            ids: self.ids.clone(),
            tools: None,
            system_input_resolver: None,
            external_receipt_verifier: None,
            components: None,
            hooks: None,
            token_estimator: self.estimator.clone(),
            settings: AgentSettings {
                observer_poll_ms: 1,
                heartbeat_interval_ms: 100,
                lease_ttl_ms: 1000,
                max_output_tokens: 128.try_into().unwrap(),
                ..AgentSettings::default()
            },
        }
    }
    pub fn agent(&self) -> Agent {
        create_agent(profile(), self.bindings()).unwrap()
    }
    pub async fn started(&self, agent: &Agent, name: &str) -> RunHandle {
        completed(agent.start(request(name), context()).await.unwrap())
    }
}
```

## `tests/support/adapter_consumer.rs`

```rust
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
```

## `tests/support/agent_consumer.rs`

```rust
// Synthetic adapters and metadata inspector: no provider network calls are made.
// The fixed clock supports deterministic accounting; this does not test timeouts.
use futures_util::{TryStreamExt, stream};
use serde_json::json;
use std::collections::BTreeSet;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
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
            options_schema: json!({"type":"object","properties":{"reasoning_effort":{"enum":["high"]}},"additionalProperties":false}),
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
    route: ResolvedModelRoute,
    calls: AtomicUsize,
    fail: bool,
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
        let events = if self.fail {
            vec![Ok(ModelEvent::ResponseError {
                kind: ModelFailureKind::RateLimited,
                metadata: ModelResponseMetadata::default(),
            })]
        } else {
            vec![
                Ok(ModelEvent::TextDelta {
                    text: "second provider result".into(),
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
        route: snapshot.route_for_binding(&reference("first"))?,
        calls: AtomicUsize::new(0),
        fail: true,
    });
    let second = Arc::new(ExampleModel {
        route: snapshot.route_for_binding(&reference("second"))?,
        calls: AtomicUsize::new(0),
        fail: false,
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
        "model_binding":"primary","tools":[],"skills":[],"connectors":[],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":4,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":2,"max_elapsed_ms":10000}
    }"#,
    )?;
    let agent = create_agent(
        profile,
        AgentBindings {
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
            system_input_resolver: None, external_receipt_verifier: None, components: None, hooks: None,
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
    let replay = completed(agent.start(request, context.clone()).await?)?;
    assert_eq!(replay.run_id(), &run_id);
    assert_eq!(completed(replay.outcome(&context).await?)?, outcome);
    assert_eq!(first.calls.load(Ordering::SeqCst), 1);
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    let view = completed(agent.get_run(&run_id, &context).await?)?;
    assert_eq!(view.status, RunStatus::Succeeded);
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
    println!(
        "agent consumer: pure construction, detached execution after observer drop, fallback under shared budgets, stored outcome and event replay, duplicate request without new model calls, SQLite reopen"
    );
    Ok(())
}
```

## `tests/support/hooks_consumer.rs`

```rust
// Synthetic model, tool, and metadata inspector. No provider network or business
// database calls occur. SQLite is real; lifecycle transforms and observer reports
// use the public Agent API and survive reopening the store.
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
                model_name: (reference.kind == ComponentKind::Tool).then(|| id("search")),
                hook_position: match reference.id.as_str() {
                    "run-data" => Some(HookPosition::BeforeRun),
                    "step-data" => Some(HookPosition::BeforeModel),
                    "normalize" => Some(HookPosition::BeforeTool),
                    "tool-observer" => Some(HookPosition::AfterTool),
                    "run-observer" => Some(HookPosition::AfterRun),
                    _ => None,
                },
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
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::ExecuteTool { input } = &request.action {
                if input.execution_args().get("workspace_id") != Some(&json!(WORKSPACE)) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("foreign-workspace"),
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

struct Model {
    route: ResolvedModelRoute,
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
        assert_eq!(request.route, self.route);
        assert_eq!(request.tools.len(), 1);
        assert_eq!(request.tools[0].name, id("search"));
        let context_items: Vec<_> = request
            .messages
            .iter()
            .flat_map(|message| {
                message.content.iter().filter_map(|content| match content {
                    ModelContent::Json { value } if value["kind"] == "context_data" => {
                        assert_eq!(message.role, ModelRole::User);
                        Some(value)
                    }
                    _ => None,
                })
            })
            .collect();
        assert_eq!(context_items.len(), 2);
        assert!(context_items.iter().all(|value| value["origin"] == "hook"));
        assert_eq!(
            context_items
                .iter()
                .map(|value| value["source_ref"]["id"].as_str().unwrap())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["run-data", "step-data"])
        );
        assert_eq!(
            request.tools[0].model_input_schema["properties"],
            json!({"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"default":2}})
        );
        // A concrete protected UUID exists in this execution; its absence is a data-boundary check.
        assert!(!serde_json::to_string(request).unwrap().contains(WORKSPACE));
        let events = match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => vec![
                Ok(ModelEvent::ToolArgumentsDelta {
                    index: 0,
                    provider_call_id: Some("search-alpha".into()),
                    name: Some("search".into()),
                    delta: r#"{"query":"alpha"}"#.into(),
                }),
                Ok(ModelEvent::ToolArgumentsDelta {
                    index: 1,
                    provider_call_id: Some("search-beta".into()),
                    name: Some("search".into()),
                    delta: r#"{"query":"beta","limit":3}"#.into(),
                }),
                Ok(ModelEvent::ResponseCompleted {
                    finish: ModelFinish::ToolCalls,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![],
                }),
            ],
            1 => {
                let calls: Vec<_> = request
                    .messages
                    .iter()
                    .flat_map(|message| &message.content)
                    .filter_map(|content| {
                        if let ModelContent::ToolCall {
                            provider_call_id,
                            arguments,
                            ..
                        } = content
                        {
                            Some((provider_call_id.clone(), arguments.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();
                assert_eq!(
                    calls,
                    vec![
                        (id("search-alpha"), object(json!({"query":"alpha"}))),
                        (id("search-beta"), object(json!({"query":"beta","limit":3})))
                    ]
                );
                let results: Vec<_> = request
                    .messages
                    .iter()
                    .flat_map(|message| &message.content)
                    .filter_map(|content| {
                        if let ModelContent::ToolResult {
                            provider_call_id,
                            content,
                        } = content
                        {
                            Some((provider_call_id.clone(), content.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();
                assert_eq!(results.len(), 2);
                for (index, query) in ["alpha", "beta"].iter().enumerate() {
                    assert_eq!(results[index].0, calls[index].0);
                    assert_eq!(
                        results[index].1,
                        json!({"status":"succeeded","effect":"not_applied","content":[{"type":"json","value":{"query":format!("{query}|hook"),"count":index+2}}]})
                    );
                }
                vec![
                    Ok(ModelEvent::TextDelta {
                        text: "Alpha has 2 results; beta has 3.".into(),
                    }),
                    Ok(ModelEvent::ResponseCompleted {
                        finish: ModelFinish::Stop,
                        metadata: ModelResponseMetadata::default(),
                        continuation: vec![],
                    }),
                ]
            }
            _ => panic!("duplicate start must not invoke the model again"),
        };
        Box::pin(stream::iter(events))
    }
}
struct Search {
    arguments: Mutex<Vec<JsonObject>>,
}
impl ToolExecutor for Search {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        assert_eq!(args.get("workspace_id"), Some(&json!(WORKSPACE)));
        assert_eq!(context.scope.workspace_id, id("workspace"));
        assert_eq!(context.principal_ref, id("actor"));
        let mut arguments = self.arguments.lock().unwrap();
        let index = arguments.len();
        assert_eq!(
            args,
            &object(if index == 0 {
                json!({"query":"alpha|hook","limit":2,"workspace_id":WORKSPACE})
            } else {
                json!({"query":"beta|hook","limit":3,"workspace_id":WORKSPACE})
            })
        );
        arguments.push(args.clone());
        drop(arguments);
        Box::pin(async move {
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: json!({"query":args["query"],"count":args["limit"]}),
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}
fn registry(
    scope: &Scope,
    search: Arc<Search>,
) -> Result<(SystemInputRegistry, ToolRegistry), ContractError> {
    let inputs = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])?;
    let compiled = SchemaCompiler::new().compile(ToolDescriptor { tool: reference("search"), name: id("search"), description: "Search authorized records".into(),
        input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"default":2},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters: vec!["query".into(),"limit".into()], system_bindings: None,
        output_schema: json!({"type":"object","properties":{"query":{"type":"string"},"count":{"type":"integer"}},"required":["query","count"],"additionalProperties":false}),
        side_effect: ToolSideEffect::ReadOnly, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false, max_output_bytes: 1024.try_into().unwrap() }, &inputs)?;
    Ok((
        inputs,
        ToolRegistry::new(
            scope.clone(),
            vec![ToolRegistration {
                compiled,
                executor: search,
            }],
        )?,
    ))
}
struct Hooks {
    calls: AtomicUsize,
}
impl HookHandler for Hooks {
    fn call<'a>(
        &'a self,
        input: &'a HookInput,
        context: &'a HookContext,
    ) -> PortFuture<'a, HookOutput> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(match input {
                HookInput::BeforeRun { .. } | HookInput::BeforeModel { .. } => {
                    HookOutput::Context {
                        additions: vec![HookContextAddition {
                            content: vec![InputContent::Json {
                                value: json!({"marker":context.hook.id}),
                            }],
                            priority: ContextPriority::Required,
                        }],
                    }
                }
                HookInput::BeforeTool {
                    original_model_inputs,
                    model_inputs,
                    ..
                } => {
                    assert_eq!(model_inputs, original_model_inputs);
                    let mut inputs = model_inputs.clone();
                    inputs.insert(
                        "query".into(),
                        json!(format!("{}|hook", model_inputs["query"].as_str().unwrap())),
                    );
                    HookOutput::Tool {
                        model_inputs: inputs,
                        deny: None,
                    }
                }
                HookInput::AfterTool { status, effect, .. } => {
                    assert_eq!(*status, ToolResultStatus::Succeeded);
                    assert_eq!(*effect, ToolEffect::NotApplied);
                    HookOutput::Observed {}
                }
                HookInput::AfterRun { status, .. } => {
                    assert_eq!(*status, RunStatus::Succeeded);
                    HookOutput::Observed {}
                }
            })
        })
    }
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
        std::env::temp_dir().join(format!("wickle-hooks-{}", RandomIdSource.next_id()?)),
    );
    std::fs::create_dir(&temporary.0)?;
    let database = temporary.0.join("state.sqlite3");
    let routing = routing(&scope)?;
    let model = Arc::new(Model {
        route: routing.route_for_binding(&reference("primary"))?,
        calls: AtomicUsize::new(0),
    });
    let search = Arc::new(Search {
        arguments: Mutex::new(vec![]),
    });
    let catalog = Arc::new(Catalog {
        calls: AtomicUsize::new(0),
    });
    let policy = Arc::new(PolicyGate::new(Arc::new(Policy), Duration::from_secs(1))?);
    let exchange = Arc::new(
        ModelExchange::new(model.clone(), policy.clone())
            .with_route_inspector(Arc::new(Inspector), Duration::from_secs(1))?,
    );
    let router = Arc::new(PolicyModelRouter::new(routing)?);
    let profile = AgentProfile::from_json(
        r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1",
        "name":"Assistant","description":"Synthetic tool loop consumer","instructions":{"text":"Search then summarize the observations"},
        "model_binding":"primary","tools":[{"tool_id":"search","version":"1"}],"skills":[],"connectors":[],
        "hooks":[{"hook_id":"run-data","version":"1","position":"before_run"},{"hook_id":"step-data","version":"1","position":"before_model"},{"hook_id":"normalize","version":"1","position":"before_tool"},{"hook_id":"tool-observer","version":"1","position":"after_tool"},{"hook_id":"run-observer","version":"1","position":"after_run"}],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":4,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}
    }"#,
    )?;
    let hook = Arc::new(Hooks {
        calls: AtomicUsize::new(0),
    });
    let make_agent = |store: Arc<SqliteStateStore>| -> Result<Agent, ContractError> {
        let (system_inputs, tools) = registry(&scope, search.clone())?;
        let registry = HookRegistry::new(
            scope.clone(),
            [
                ("run-data", HookPosition::BeforeRun),
                ("step-data", HookPosition::BeforeModel),
                ("normalize", HookPosition::BeforeTool),
                ("tool-observer", HookPosition::AfterTool),
                ("run-observer", HookPosition::AfterRun),
            ]
            .into_iter()
            .map(|(name, position)| HookRegistration {
                definition: HookDefinition {
                    hook: reference(name),
                    position,
                    priority: 0,
                    required: true,
                    timeout_ms: 1000,
                    max_output_bytes: 4096,
                },
                handler: hook.clone(),
            })
            .collect(),
        )?;
        let runtime = Arc::new(HookRuntime::new(
            store.clone(),
            policy.clone(),
            Arc::new(SystemClock::new()),
            Arc::new(RandomIdSource),
            Arc::new(registry),
        ));
        create_agent(
            profile.clone(),
            AgentBindings {
                scope: scope.clone(),
                state: store,
                policy: policy.clone(),
                profile_resolver: catalog.clone(),
                model_exchange: exchange.clone(),
                router: router.clone(),
                host_instructions: vec!["Use only the authorized workspace.".into()],
                system_inputs,
                tools: Some(Arc::new(tools)),
                system_input_resolver: None,
                external_receipt_verifier: None, components: None,
                hooks: Some(runtime),
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
    };
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id("actor"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: Some(SystemInputs::new(object(json!({"workspace_id":WORKSPACE})))),
        },
        Default::default(),
    );
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Compare alpha and beta".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    };
    let store = Arc::new(SqliteStateStore::open(&database)?);
    let agent = make_agent(store.clone())?;
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 0);
    assert!(search.arguments.lock().unwrap().is_empty());
    let handle = completed(agent.start(request.clone(), context.clone()).await?)?;
    let run_id = handle.run_id().clone();
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
            text: "Alpha has 2 results; beta has 3.".into()
        }]
    );
    assert_eq!(
        (outcome.usage.model_calls, outcome.usage.tool_attempts),
        (2, 2)
    );
    let events: Vec<_> = handle.events(0, context.clone()).try_collect().await?;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "tool.planned")
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "tool.settled")
            .count(),
        2
    );
    assert_eq!(
        events.last().ok_or("missing terminal event")?.event_type,
        "run.finished"
    );
    let saved_before_reports = store.load(&scope, &run_id).await?;
    let reports = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let view = completed(handle.hook_observations(&context).await?)?;
            if let Some(error) = view.local_error {
                return Err::<_, Box<dyn std::error::Error>>(Box::new(error));
            }
            if view.reports.len() == 3 {
                return Ok(view.reports);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await??;
    assert!(
        reports
            .iter()
            .all(|report| report.status == HookObservationStatus::Completed)
    );
    let saved = store.load(&scope, &run_id).await?;
    assert_eq!(saved.snapshot, saved_before_reports.snapshot);
    assert_eq!(saved.snapshot.hook_applications.len(), 5);
    for application in &saved.snapshot.hook_applications {
        let record = store.read_record(&scope, &application.result_ref).await?;
        let result: HookApplicationRecord = serde_json::from_value(record.value().clone())?;
        assert_eq!(result.hook, application.hook);
        assert!(result.failure.is_none());
        assert!(
            result
                .context_items
                .iter()
                .all(|item| item.origin == ContextOrigin::Hook && item.scope == scope)
        );
    }
    assert_eq!(hook.calls.load(Ordering::SeqCst), 8);
    assert_eq!(
        saved.snapshot.tool_ledger[0].call.model_inputs,
        object(json!({"query":"alpha"}))
    );
    for entry in &saved.snapshot.tool_ledger {
        assert!(entry.call.bound_input_ref.is_some());
        assert!(
            matches!(&entry.state, ToolCallState::Settled { result } if result.status == ToolResultStatus::Succeeded && result.effect == ToolEffect::NotApplied)
        );
    }
    drop(handle);
    drop(agent);
    drop(store);

    let reopened = Arc::new(SqliteStateStore::open(&database)?);
    let restored = reopened.load(&scope, &run_id).await?;
    assert_eq!(restored.snapshot.outcome, Some(outcome.clone()));
    assert_eq!(restored.snapshot.tool_ledger, saved.snapshot.tool_ledger);
    assert!(restored.session.active_run_id.is_none());
    assert_eq!(
        reopened.read_hook_observations(&scope, &run_id).await?,
        reports
    );
    let resolver_calls = catalog.calls.load(Ordering::SeqCst);
    let replay_agent = make_agent(reopened)?;
    let replay = completed(replay_agent.start(request, context.clone()).await?)?;
    assert_eq!(replay.run_id(), &run_id);
    assert_eq!(completed(replay.outcome(&context).await?)?, outcome);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(search.arguments.lock().unwrap().len(), 2);
    assert_eq!(catalog.calls.load(Ordering::SeqCst), resolver_calls);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 8);
    println!(
        "hooks consumer: core-stamped Run/step context; original/effective tool arguments; committed tool/Run reports; real SQLite reopen and replay without repeated model/tool/hooks (synthetic Host, no provider network)"
    );
    Ok(())
}
```

## `tests/support/resume_consumer.rs`

```rust
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
            external_receipt_verifier: None, components: None, hooks: None,
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
```

## `tests/support/tool_loop_consumer.rs`

```rust
// Synthetic model, tool, and metadata inspector. No provider network or business
// database calls occur. SQLite is real; the assertions exercise the public Agent API.
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
                model_name: (reference.kind == ComponentKind::Tool).then(|| id("search")),
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
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::ExecuteTool { input } = &request.action {
                if input.execution_args().get("workspace_id") != Some(&json!(WORKSPACE)) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("foreign-workspace"),
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

struct Model {
    route: ResolvedModelRoute,
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
        assert_eq!(request.route, self.route);
        assert_eq!(request.tools.len(), 1);
        assert_eq!(request.tools[0].name, id("search"));
        assert_eq!(
            request.tools[0].model_input_schema["properties"],
            json!({"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"default":2}})
        );
        // A concrete protected UUID exists in this execution; its absence is a data-boundary check.
        assert!(!serde_json::to_string(request).unwrap().contains(WORKSPACE));
        let events = match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => vec![
                Ok(ModelEvent::ToolArgumentsDelta {
                    index: 0,
                    provider_call_id: Some("search-alpha".into()),
                    name: Some("search".into()),
                    delta: r#"{"query":"alpha"}"#.into(),
                }),
                Ok(ModelEvent::ToolArgumentsDelta {
                    index: 1,
                    provider_call_id: Some("search-beta".into()),
                    name: Some("search".into()),
                    delta: r#"{"query":"beta","limit":3}"#.into(),
                }),
                Ok(ModelEvent::ResponseCompleted {
                    finish: ModelFinish::ToolCalls,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![],
                }),
            ],
            1 => {
                let calls: Vec<_> = request
                    .messages
                    .iter()
                    .flat_map(|message| &message.content)
                    .filter_map(|content| {
                        if let ModelContent::ToolCall {
                            provider_call_id,
                            arguments,
                            ..
                        } = content
                        {
                            Some((provider_call_id.clone(), arguments.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();
                assert_eq!(
                    calls,
                    vec![
                        (id("search-alpha"), object(json!({"query":"alpha"}))),
                        (id("search-beta"), object(json!({"query":"beta","limit":3})))
                    ]
                );
                let results: Vec<_> = request
                    .messages
                    .iter()
                    .flat_map(|message| &message.content)
                    .filter_map(|content| {
                        if let ModelContent::ToolResult {
                            provider_call_id,
                            content,
                        } = content
                        {
                            Some((provider_call_id.clone(), content.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();
                assert_eq!(results.len(), 2);
                for (index, query) in ["alpha", "beta"].iter().enumerate() {
                    assert_eq!(results[index].0, calls[index].0);
                    assert_eq!(
                        results[index].1,
                        json!({"status":"succeeded","effect":"not_applied","content":[{"type":"json","value":{"query":query,"count":index+2}}]})
                    );
                }
                vec![
                    Ok(ModelEvent::TextDelta {
                        text: "Alpha has 2 results; beta has 3.".into(),
                    }),
                    Ok(ModelEvent::ResponseCompleted {
                        finish: ModelFinish::Stop,
                        metadata: ModelResponseMetadata::default(),
                        continuation: vec![],
                    }),
                ]
            }
            _ => panic!("duplicate start must not invoke the model again"),
        };
        Box::pin(stream::iter(events))
    }
}
struct Search {
    arguments: Mutex<Vec<JsonObject>>,
}
impl ToolExecutor for Search {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        assert_eq!(args.get("workspace_id"), Some(&json!(WORKSPACE)));
        assert_eq!(context.scope.workspace_id, id("workspace"));
        assert_eq!(context.principal_ref, id("actor"));
        let mut arguments = self.arguments.lock().unwrap();
        let index = arguments.len();
        assert_eq!(
            args,
            &object(if index == 0 {
                json!({"query":"alpha","limit":2,"workspace_id":WORKSPACE})
            } else {
                json!({"query":"beta","limit":3,"workspace_id":WORKSPACE})
            })
        );
        arguments.push(args.clone());
        drop(arguments);
        Box::pin(async move {
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: json!({"query":args["query"],"count":args["limit"]}),
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}
fn registry(
    scope: &Scope,
    search: Arc<Search>,
) -> Result<(SystemInputRegistry, ToolRegistry), ContractError> {
    let inputs = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])?;
    let compiled = SchemaCompiler::new().compile(ToolDescriptor { tool: reference("search"), name: id("search"), description: "Search authorized records".into(),
        input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"default":2},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters: vec!["query".into(),"limit".into()], system_bindings: None,
        output_schema: json!({"type":"object","properties":{"query":{"type":"string"},"count":{"type":"integer"}},"required":["query","count"],"additionalProperties":false}),
        side_effect: ToolSideEffect::ReadOnly, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false, max_output_bytes: 1024.try_into().unwrap() }, &inputs)?;
    Ok((
        inputs,
        ToolRegistry::new(
            scope.clone(),
            vec![ToolRegistration {
                compiled,
                executor: search,
            }],
        )?,
    ))
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
        std::env::temp_dir().join(format!("wickle-tool-loop-{}", RandomIdSource.next_id()?)),
    );
    std::fs::create_dir(&temporary.0)?;
    let database = temporary.0.join("state.sqlite3");
    let routing = routing(&scope)?;
    let model = Arc::new(Model {
        route: routing.route_for_binding(&reference("primary"))?,
        calls: AtomicUsize::new(0),
    });
    let search = Arc::new(Search {
        arguments: Mutex::new(vec![]),
    });
    let catalog = Arc::new(Catalog {
        calls: AtomicUsize::new(0),
    });
    let policy = Arc::new(PolicyGate::new(Arc::new(Policy), Duration::from_secs(1))?);
    let exchange = Arc::new(
        ModelExchange::new(model.clone(), policy.clone())
            .with_route_inspector(Arc::new(Inspector), Duration::from_secs(1))?,
    );
    let router = Arc::new(PolicyModelRouter::new(routing)?);
    let profile = AgentProfile::from_json(
        r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1",
        "name":"Assistant","description":"Synthetic tool loop consumer","instructions":{"text":"Search then summarize the observations"},
        "model_binding":"primary","tools":[{"tool_id":"search","version":"1"}],"skills":[],"connectors":[],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":4,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}
    }"#,
    )?;
    let make_agent = |store: Arc<SqliteStateStore>| -> Result<Agent, ContractError> {
        let (system_inputs, tools) = registry(&scope, search.clone())?;
        create_agent(
            profile.clone(),
            AgentBindings {
                scope: scope.clone(),
                state: store,
                policy: policy.clone(),
                profile_resolver: catalog.clone(),
                model_exchange: exchange.clone(),
                router: router.clone(),
                host_instructions: vec!["Use only the authorized workspace.".into()],
                system_inputs,
                tools: Some(Arc::new(tools)),
                system_input_resolver: None, external_receipt_verifier: None, components: None, hooks: None,
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
    };
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id("actor"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: Some(SystemInputs::new(object(json!({"workspace_id":WORKSPACE})))),
        },
        Default::default(),
    );
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Compare alpha and beta".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    };
    let store = Arc::new(SqliteStateStore::open(&database)?);
    let agent = make_agent(store.clone())?;
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert!(search.arguments.lock().unwrap().is_empty());
    let handle = completed(agent.start(request.clone(), context.clone()).await?)?;
    let run_id = handle.run_id().clone();
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
            text: "Alpha has 2 results; beta has 3.".into()
        }]
    );
    assert_eq!(
        (outcome.usage.model_calls, outcome.usage.tool_attempts),
        (2, 2)
    );
    let events: Vec<_> = handle.events(0, context.clone()).try_collect().await?;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "tool.planned")
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "tool.settled")
            .count(),
        2
    );
    assert_eq!(
        events.last().ok_or("missing terminal event")?.event_type,
        "run.finished"
    );
    let saved = store.load(&scope, &run_id).await?;
    assert_eq!(
        saved.snapshot.tool_ledger[0].call.model_inputs,
        object(json!({"query":"alpha"}))
    );
    for entry in &saved.snapshot.tool_ledger {
        assert!(entry.call.bound_input_ref.is_some());
        assert!(
            matches!(&entry.state, ToolCallState::Settled { result } if result.status == ToolResultStatus::Succeeded && result.effect == ToolEffect::NotApplied)
        );
    }
    drop(handle);
    drop(agent);
    drop(store);

    let reopened = Arc::new(SqliteStateStore::open(&database)?);
    let restored = reopened.load(&scope, &run_id).await?;
    assert_eq!(restored.snapshot.outcome, Some(outcome.clone()));
    assert_eq!(restored.snapshot.tool_ledger, saved.snapshot.tool_ledger);
    assert!(restored.session.active_run_id.is_none());
    let resolver_calls = catalog.calls.load(Ordering::SeqCst);
    let replay_agent = make_agent(reopened)?;
    let replay = completed(replay_agent.start(request, context.clone()).await?)?;
    assert_eq!(replay.run_id(), &run_id);
    assert_eq!(completed(replay.outcome(&context).await?)?, outcome);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(search.arguments.lock().unwrap().len(), 2);
    assert_eq!(catalog.calls.load(Ordering::SeqCst), resolver_calls);
    println!(
        "tool loop consumer: two serial calls with system UUID binding and model-only arguments; final model response; SQLite reopen and request replay without additional model, tool, or resolver calls"
    );
    Ok(())
}
```
