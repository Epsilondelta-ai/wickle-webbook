//! Immutable, scope-bound model catalogs and static routing for Wickle applications.
//!
//! Catalog lookups do not invoke models, load environment variables, or select a
//! fallback. Routers select only configured targets; they do not dispatch calls.
//! Metadata contracts live in `wickle`; this crate depends on the core.

mod dispatcher;
mod routing;
pub use dispatcher::{ModelDispatcherEntry, RegistryModelDispatcher};
pub use routing::{FixedModelRouter, PolicyModelRouter};

use serde::{Serialize, Serializer};
use wickle::{
    ContractError, ErrorCode, Id, JsonDigest, ModelCatalog, ModelCatalogSnapshot, ModelDefinition,
    ModelDefinitionRef, PortFuture, ResolvedCatalogBinding, Scope, VersionedRef,
};

/// Privately owned, validated catalog revision. Returned records are independent
/// copies; editing them cannot change future lookups or an already pinned revision.
#[derive(Debug, Clone)]
pub struct ImmutableModelCatalog {
    snapshot: ModelCatalogSnapshot,
}

impl Serialize for ImmutableModelCatalog {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.snapshot.serialize(serializer)
    }
}

impl ImmutableModelCatalog {
    /// Validate and own a catalog without network, model, or environment access.
    pub fn new(snapshot: ModelCatalogSnapshot) -> Result<Self, ContractError> {
        snapshot.validate()?;
        Ok(Self { snapshot })
    }
    /// Inspect the immutable snapshot without obtaining mutable access.
    pub fn snapshot(&self) -> &ModelCatalogSnapshot {
        &self.snapshot
    }
    /// Identity of the protected serialized snapshot.
    pub fn digest(&self) -> JsonDigest {
        self.snapshot.digest()
    }
    /// Restore against a trusted expected digest and revalidate every contract.
    pub fn restore(input: &str, expected_digest: &JsonDigest) -> Result<Self, ContractError> {
        let snapshot: ModelCatalogSnapshot = serde_json::from_value(wickle::parse_json(input)?)
            .map_err(|_| ContractError::new(ErrorCode::ModelCatalogMismatch, "catalog"))?;
        if &snapshot.digest() != expected_digest {
            return Err(ContractError::new(
                ErrorCode::ModelCatalogMismatch,
                "catalog.digest",
            ));
        }
        Self::new(snapshot)
    }
    fn check(&self, scope: &Scope, revision: &Id) -> Result<(), ContractError> {
        if scope != &self.snapshot.scope {
            return Err(ContractError::new(ErrorCode::AccessDenied, "catalog.scope"));
        }
        if revision != &self.snapshot.revision {
            return Err(ContractError::new(
                ErrorCode::ModelCatalogMismatch,
                "catalog.revision",
            ));
        }
        Ok(())
    }
}

impl ModelCatalog for ImmutableModelCatalog {
    fn revision(&self) -> &Id {
        &self.snapshot.revision
    }
    fn scope(&self) -> &Scope {
        &self.snapshot.scope
    }
    fn get_model<'a>(
        &'a self,
        scope: &'a Scope,
        revision: &'a Id,
        reference: &'a ModelDefinitionRef,
    ) -> PortFuture<'a, ModelDefinition> {
        Box::pin(async move {
            self.check(scope, revision)?;
            self.snapshot
                .models
                .iter()
                .find(|model| model.reference() == *reference)
                .cloned()
                .ok_or_else(|| ContractError::new(ErrorCode::ModelNotRegistered, "catalog.model"))
        })
    }
    fn get_binding<'a>(
        &'a self,
        scope: &'a Scope,
        revision: &'a Id,
        reference: &'a VersionedRef,
    ) -> PortFuture<'a, ResolvedCatalogBinding> {
        Box::pin(async move {
            self.check(scope, revision)?;
            let binding = self
                .snapshot
                .bindings
                .iter()
                .find(|binding| binding.binding == *reference)
                .ok_or_else(|| {
                    ContractError::new(ErrorCode::ModelNotRegistered, "catalog.binding")
                })?;
            let model = self
                .snapshot
                .models
                .iter()
                .find(|model| model.reference() == binding.model)
                .ok_or_else(|| {
                    ContractError::new(ErrorCode::ModelNotRegistered, "catalog.model")
                })?;
            Ok(ResolvedCatalogBinding {
                catalog_revision: self.snapshot.revision.clone(),
                model: model.clone(),
                binding: binding.clone(),
            })
        })
    }
    fn resolve_alias<'a>(
        &'a self,
        scope: &'a Scope,
        revision: &'a Id,
        provider: &'a Id,
        alias: &'a Id,
    ) -> PortFuture<'a, ModelDefinitionRef> {
        Box::pin(async move {
            self.check(scope, revision)?;
            self.snapshot
                .aliases
                .iter()
                .find(|entry| &entry.provider == provider && &entry.alias == alias)
                .map(|entry| entry.target.clone())
                .ok_or_else(|| ContractError::new(ErrorCode::ModelNotRegistered, "catalog.alias"))
        })
    }
}
