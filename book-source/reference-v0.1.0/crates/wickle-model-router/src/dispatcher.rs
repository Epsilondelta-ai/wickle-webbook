use std::{collections::BTreeMap, fmt, sync::Arc};

use wickle::{
    ContractError, ErrorCode, Id, ModelDispatcher, ModelPort, ModelPortBinding, ResolvedModelRoute,
    Scope,
};

/// An already-created provider adapter authorized for one exact scope.
#[derive(Clone)]
pub struct ModelDispatcherEntry {
    /// Tenant/workspace/user namespace; None is not a wildcard user.
    pub scope: Scope,
    /// Existing adapter with its own provider, version and credential binding.
    pub port: Arc<dyn ModelPort>,
}

impl fmt::Debug for ModelDispatcherEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelDispatcherEntry")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct BindingKey {
    scope: (Id, Id, Option<Id>),
    provider: Id,
    adapter: (Id, Id),
    connection: (Id, Id),
}

impl BindingKey {
    fn new(scope: &Scope, binding: ModelPortBinding) -> Self {
        Self {
            scope: (
                scope.tenant_id.clone(),
                scope.workspace_id.clone(),
                scope.user_id.clone(),
            ),
            provider: binding.provider,
            adapter: (binding.adapter.id, binding.adapter.version),
            connection: (binding.connection_ref.id, binding.connection_ref.version),
        }
    }
}

/// Immutable mapping to existing adapters. Lookup performs no client creation,
/// I/O, model call or route selection; it cannot fall back to another account.
pub struct RegistryModelDispatcher {
    ports: BTreeMap<BindingKey, Arc<dyn ModelPort>>,
}

impl fmt::Debug for RegistryModelDispatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryModelDispatcher")
            .field("entry_count", &self.ports.len())
            .finish()
    }
}

impl RegistryModelDispatcher {
    /// Capture exact identities and reject ambiguous registrations, including two
    /// registrations of the same adapter instance under the same scope and key.
    pub fn new(entries: Vec<ModelDispatcherEntry>) -> Result<Self, ContractError> {
        let mut ports = BTreeMap::new();
        for entry in entries {
            let key = BindingKey::new(&entry.scope, entry.port.binding());
            if ports.insert(key, entry.port).is_some() {
                return Err(ContractError::new(
                    ErrorCode::ModelBindingInvalid,
                    "dispatcher.duplicate_binding",
                ));
            }
        }
        Ok(Self { ports })
    }
}

impl ModelDispatcher for RegistryModelDispatcher {
    fn resolve(
        &self,
        scope: &Scope,
        route: &ResolvedModelRoute,
    ) -> Result<Arc<dyn ModelPort>, ContractError> {
        let key = BindingKey::new(
            scope,
            ModelPortBinding {
                provider: route.provider.clone(),
                adapter: route.adapter.clone(),
                connection_ref: route.connection_ref.clone(),
            },
        );
        let port = self.ports.get(&key).ok_or_else(|| {
            ContractError::new(ErrorCode::ModelBindingInvalid, "dispatcher.binding")
        })?;
        // Registered ports are trusted implementations, but a changed reported
        // identity must never reuse an entry that was keyed before the change.
        if !port.binding().matches_route(route) {
            return Err(ContractError::new(
                ErrorCode::ModelBindingInvalid,
                "dispatcher.binding",
            ));
        }
        Ok(port.clone())
    }
}
