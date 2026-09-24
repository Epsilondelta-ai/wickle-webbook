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
    /// A read-only context source with collection and cached-use authorization.
    ContextSource {
        /// Declared source metadata.
        metadata: ExportMetadata,
        /// Exact automatic-source origin and contract version.
        definition: ContextSourceDefinition,
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
            | Self::ContextSource { metadata, .. }
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
    /// Read-only context source; the same scoped instance authorizes cached use.
    ContextSource {
        /// Adapter-local export identity.
        export_id: Id,
        /// Exact native source version and permitted origin.
        definition: ContextSourceDefinition,
        /// Scoped provider and current ACL checker.
        source: Arc<dyn ContextSource>,
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
    sources: Arc<ContextSourceRegistry>,
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
        sources: Arc<ContextSourceRegistry>,
        release: Arc<dyn ComponentRelease>,
    ) -> Result<Self, ContractError> {
        if tools.scope() != &scope || hooks.scope() != &scope || sources.scope() != &scope {
            return Err(component_error(ErrorCode::AccessDenied, "components.scope"));
        }
        Ok(Self {
            scope,
            run_id,
            binding_set_id,
            tools,
            hooks,
            sources,
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
    /// Scoped read-only providers and their immutable selection metadata.
    pub fn sources(&self) -> &Arc<ContextSourceRegistry> {
        &self.sources
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    sources: Vec<ResolvedSourceBinding>,
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
        sources: Vec<ResolvedSourceBinding>,
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
            sources,
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
    /// Automatic sources in their original profile order.
    pub fn sources(&self) -> &[ResolvedSourceBinding] {
        &self.data.sources
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
            || self.connections().len() != selected.connectors.len()
            || self.adapters().len() != selected.adapters.as_ref().map_or(0, Vec::len)
            || self.tools().len() != selected.tools.len()
            || self.hooks().len() != selected.hooks.as_ref().map_or(0, Vec::len)
            || self.sources().len() != selected.context_sources.as_ref().map_or(0, Vec::len)
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
            let mut source_exports = std::collections::BTreeSet::new();
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
                .chain(
                    selected
                        .context_sources
                        .iter()
                        .flatten()
                        .filter_map(|source| {
                            if let ContextSourceRef::Export(export) = &source.source {
                                Some(export)
                            } else {
                                None
                            }
                        })
                        .filter(|export| {
                            source_exports
                                .insert((export.adapter_binding.clone(), export.export_id.clone()))
                        }),
                )
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
        let mut source_slots = std::collections::BTreeSet::new();
        for (binding, selection) in self
            .sources()
            .iter()
            .zip(selected.context_sources.iter().flatten())
        {
            binding.definition.validate()?;
            if &binding.binding != selection
                || !source_slots.insert(crate::canonical_digest(&serde_json::json!([
                    selection.source,
                    selection.trigger
                ])))
            {
                return Err(invalid());
            }
            match &selection.source {
                ContextSourceRef::Catalog(reference) => {
                    let metadata = binding.metadata.as_ref().ok_or_else(invalid)?;
                    attest_metadata(profile, metadata)?;
                    if metadata.reference
                        != (ComponentRef {
                            kind: ComponentKind::ContextSource,
                            id: reference.source_id.clone(),
                            version: Some(reference.version.clone()),
                        })
                        || binding.definition.source
                            != (VersionedRef {
                                id: reference.source_id.clone(),
                                version: reference.version.clone(),
                            })
                    {
                        return Err(invalid());
                    }
                }
                ContextSourceRef::Export(export) => {
                    if export.alias.is_some() || binding.metadata.is_some() {
                        return Err(invalid());
                    }
                    let AdapterExportDefinition::ContextSource { definition, .. } =
                        self.export(export)?
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
                AdapterExportDefinition::ContextSource { definition, .. } => {
                    definition.validate()?;
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
