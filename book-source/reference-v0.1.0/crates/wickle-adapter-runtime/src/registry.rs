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

/// Existing catalog source and its independently validated component metadata.
#[derive(Clone)]
pub struct CatalogSourceRegistration {
    /// Exact catalog metadata used by ProfileResolver.
    pub metadata: ComponentMetadata,
    /// Scoped read-only provider and native source definition.
    pub source: ContextSourceRegistration,
}

/// Immutable scope-local metadata and implementation registry. Construction and
/// resolution never invoke factories, tools, hooks, source readers or consumers.
pub struct AdapterRegistry {
    pub(crate) scope: Scope,
    pub(crate) adapters: Vec<AdapterRegistration>,
    pub(crate) connections: Vec<ConnectionRegistration>,
    pub(crate) tools: Vec<CatalogToolRegistration>,
    pub(crate) hooks: Vec<CatalogHookRegistration>,
    pub(crate) sources: Vec<CatalogSourceRegistration>,
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
            sources: vec![],
            states,
        })
    }
    /// Register catalog sources without invoking their provide or ACL callbacks.
    pub fn with_sources(
        mut self,
        sources: Vec<CatalogSourceRegistration>,
    ) -> Result<Self, ContractError> {
        for (index, entry) in sources.iter().enumerate() {
            entry.source.definition.validate()?;
            let ContextSourceRef::Catalog(reference) = &entry.source.selection else {
                return Err(error(
                    ErrorCode::InvalidReference,
                    "registry.source_selection",
                ));
            };
            if entry.metadata.reference.kind != ComponentKind::ContextSource
                || entry.metadata.reference.id != reference.source_id
                || entry.metadata.reference.version.as_ref() != Some(&reference.version)
                || entry.source.definition.source
                    != (VersionedRef {
                        id: reference.source_id.clone(),
                        version: reference.version.clone(),
                    })
                || sources[..index]
                    .iter()
                    .any(|prior| prior.source.selection == entry.source.selection)
            {
                return Err(error(ErrorCode::InvalidContract, "registry.source"));
            }
        }
        self.sources = sources;
        Ok(self)
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
    /// Lookup the exact source code revision without reading source data.
    pub fn catalog_source(&self, reference: &VersionedRef) -> Option<&CatalogSourceRegistration> {
        self.sources
            .iter()
            .find(|entry| entry.source.definition.source == *reference)
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
            .chain(self.sources.iter().map(|entry| &entry.metadata))
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
            let mut source_exports = std::collections::BTreeSet::new();
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
                .chain(
                    selected
                        .context_sources
                        .iter()
                        .flatten()
                        .filter_map(|binding| {
                            if let ContextSourceRef::Export(export) = &binding.source {
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
        let mut sources = Vec::new();
        for binding in selected.context_sources.iter().flatten() {
            let (metadata, definition) = match &binding.source {
                ContextSourceRef::Catalog(reference) => {
                    let entry = self
                        .catalog_source(&VersionedRef {
                            id: reference.source_id.clone(),
                            version: reference.version.clone(),
                        })
                        .ok_or_else(|| error(ErrorCode::ComponentUnavailable, "registry.source"))?;
                    (
                        Some(entry.metadata.clone()),
                        entry.source.definition.clone(),
                    )
                }
                ContextSourceRef::Export(reference) => {
                    let AdapterExportDefinition::ContextSource { definition, .. } =
                        lookup_export(reference)?
                    else {
                        return Err(error(ErrorCode::InvalidReference, "registry.source_kind"));
                    };
                    (None, definition.clone())
                }
            };
            sources.push(ResolvedSourceBinding {
                binding: binding.clone(),
                definition,
                metadata,
            });
        }
        ResolvedAssembly::new(
            profile,
            context,
            connections,
            adapters,
            tools,
            hooks,
            sources,
        )
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
        for binding in assembly.sources() {
            if let ContextSourceRef::Catalog(reference) = &binding.binding.source {
                let entry = self
                    .catalog_source(&VersionedRef {
                        id: reference.source_id.clone(),
                        version: reference.version.clone(),
                    })
                    .ok_or_else(mismatch)?;
                if binding.metadata.as_ref() != Some(&entry.metadata)
                    || binding.definition != entry.source.definition
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
