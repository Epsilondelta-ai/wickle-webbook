# 14장 전체 Rust 구현과 테스트

[강의로](../14-routing.md) · [전체 변경 패치](../solutions/14-routing.patch)

기준 `1f36da1996dffb48a6eee15de40ff477a133803f`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle-model-router/src/dispatcher.rs`

```rust
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
```

## `crates/wickle-model-router/src/lib.rs`

```rust
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
```

## `crates/wickle-model-router/src/routing.rs`

```rust
use crate::ImmutableModelCatalog;
use wickle::{
    CatalogRequirements, ContractError, ErrorCode, ModelCatalog, ModelRouter, ModelSupportStatus,
    PortFuture, RouteRequest, RouteSelection, RouteSelectionReason, RoutingSnapshot, VersionPolicy,
};

/// Deterministic static selection over a privately owned catalog and policy.
/// No model call, metadata probe, credential lookup, retry or fallback I/O occurs here.
#[derive(Debug, Clone)]
pub struct PolicyModelRouter {
    snapshot: RoutingSnapshot,
    catalog: ImmutableModelCatalog,
}

impl PolicyModelRouter {
    /// Own one validated snapshot. A later catalog object cannot replace its data.
    pub fn new(snapshot: RoutingSnapshot) -> Result<Self, ContractError> {
        let catalog = ImmutableModelCatalog::new(snapshot.catalog().clone())?;
        Ok(Self { snapshot, catalog })
    }

    async fn select(&self, request: &RouteRequest) -> Result<RouteSelection, ContractError> {
        if &request.scope != self.snapshot.scope() {
            return Err(error(ErrorCode::AccessDenied, "routing.scope"));
        }
        let rule = self
            .snapshot
            .policy()
            .rules
            .iter()
            .find(|rule| {
                rule.model_binding == request.model_binding && rule.purpose == request.purpose
            })
            .ok_or_else(|| error(ErrorCode::ModelRouteDenied, "routing.rule"))?;
        if rule.min_support < ModelSupportStatus::ContractTested {
            return Err(error(
                ErrorCode::ModelSupportInsufficient,
                "routing.min_support",
            ));
        }
        let candidates: Vec<_> = std::iter::once(&rule.primary)
            .chain(&rule.fallbacks)
            .collect();
        let previous_index = match &request.previous_route {
            Some(previous) => {
                let index = candidates
                    .iter()
                    .position(|reference| **reference == previous.binding)
                    .ok_or_else(|| {
                        error(ErrorCode::ModelRoutingMismatch, "routing.previous_route")
                    })?;
                if self.snapshot.route_for_binding(candidates[index])? != *previous {
                    return Err(error(
                        ErrorCode::ModelRoutingMismatch,
                        "routing.previous_route",
                    ));
                }
                Some(index)
            }
            None => None,
        };
        let (start, end, reason) = match (previous_index, request.previous_failure) {
            (None, None) => (0, 1, RouteSelectionReason::Initial),
            (Some(index), None) => (index, index + 1, RouteSelectionReason::Reuse),
            (None, Some(_)) => {
                return Err(error(
                    ErrorCode::ModelRoutingInvalid,
                    "routing.previous_failure",
                ));
            }
            (Some(index), Some(failure)) => {
                if !rule.fallback_on.contains(&failure) {
                    return Err(error(
                        ErrorCode::ModelRouteDenied,
                        "routing.fallback_reason",
                    ));
                }
                (
                    index + 1,
                    candidates.len(),
                    RouteSelectionReason::Fallback { failure },
                )
            }
        };
        if start == end {
            return Err(error(ErrorCode::ModelRoutesExhausted, "routing.candidates"));
        }
        let requirements = CatalogRequirements {
            features: request.required_capabilities.clone(),
            options: request.options.clone(),
            input_tokens: request.input_tokens,
            max_output_tokens: request.max_output_tokens,
            version_policy: if rule.version_policy == VersionPolicy::RequirePinned
                || request.version_policy == VersionPolicy::RequirePinned
            {
                VersionPolicy::RequirePinned
            } else {
                VersionPolicy::AllowMutable
            },
            min_support: rule.min_support,
        };
        let fallback = matches!(reason, RouteSelectionReason::Fallback { .. });
        let mut permitted = false;
        for (index, reference) in candidates.iter().enumerate().take(end).skip(start) {
            if !request.allowed_bindings.contains(&reference.id) {
                if !fallback {
                    return Err(error(
                        ErrorCode::ModelRouteDenied,
                        "routing.allowed_bindings",
                    ));
                }
                continue;
            }
            permitted = true;
            let binding = self
                .catalog
                .get_binding(&request.scope, self.catalog.revision(), reference)
                .await?;
            if let Err(error) = binding.validate(&requirements) {
                if !fallback {
                    return Err(error);
                }
                continue;
            }
            let selection = RouteSelection {
                route: self.snapshot.route_for_binding(reference)?,
                reason,
                candidate_index: index,
                routing_snapshot_digest: self.snapshot.digest(),
                request_digest: request.digest(),
            };
            self.snapshot.validate_selection(request, &selection)?;
            return Ok(selection);
        }
        Err(if permitted {
            error(ErrorCode::ModelRoutesExhausted, "routing.candidates")
        } else {
            error(ErrorCode::ModelRouteDenied, "routing.allowed_bindings")
        })
    }
}

impl ModelRouter for PolicyModelRouter {
    fn snapshot(&self) -> &RoutingSnapshot {
        &self.snapshot
    }
    fn resolve<'a>(&'a self, request: &'a RouteRequest) -> PortFuture<'a, RouteSelection> {
        Box::pin(self.select(request))
    }
}

/// The same scoped routing contract restricted to one exact target and no fallback.
/// Different logical profile names or purposes may explicitly select that target.
#[derive(Debug, Clone)]
pub struct FixedModelRouter {
    inner: PolicyModelRouter,
}

impl FixedModelRouter {
    /// Reject multi-target policies instead of quietly choosing their first binding.
    pub fn new(snapshot: RoutingSnapshot) -> Result<Self, ContractError> {
        let first = &snapshot.policy().rules[0].primary;
        if snapshot
            .policy()
            .rules
            .iter()
            .any(|rule| !rule.fallbacks.is_empty() || &rule.primary != first)
        {
            return Err(error(
                ErrorCode::ModelRoutingInvalid,
                "routing.fixed_target",
            ));
        }
        Ok(Self {
            inner: PolicyModelRouter::new(snapshot)?,
        })
    }
}

impl ModelRouter for FixedModelRouter {
    fn snapshot(&self) -> &RoutingSnapshot {
        self.inner.snapshot()
    }
    fn resolve<'a>(&'a self, request: &'a RouteRequest) -> PortFuture<'a, RouteSelection> {
        self.inner.resolve(request)
    }
}

fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
```

## `crates/wickle-model-router/tests/dispatcher.rs`

```rust
//! Exact registry identity and metadata observations, without provider calls.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use serde_json::json;
use wickle::*;
use wickle_model_router::{ModelDispatcherEntry, RegistryModelDispatcher};

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    }
}
fn route() -> ResolvedModelRoute {
    ResolvedModelRoute {
        binding: reference("binding"),
        catalog_revision: id("catalog"),
        routing_policy_revision: id("policy"),
        requested_model: id("preferred"),
        model_id: id("model"),
        model_version: id("release"),
        version_semantics: VersionSemantics::Pinned,
        provider: id("provider"),
        target: [("deployment".into(), json!("deployment"))]
            .into_iter()
            .collect(),
        deployment_revision: Some(id("deployment-revision")),
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("api-version"),
        },
        adapter: reference("adapter"),
        capability_revision: id("capabilities"),
        connection_ref: reference("connection"),
    }
}
struct Port {
    binding: Mutex<ModelPortBinding>,
    calls: AtomicUsize,
}
impl Port {
    fn new(route: &ResolvedModelRoute) -> Arc<Self> {
        Arc::new(Self {
            binding: Mutex::new(ModelPortBinding {
                provider: route.provider.clone(),
                adapter: route.adapter.clone(),
                connection_ref: route.connection_ref.clone(),
            }),
            calls: AtomicUsize::new(0),
        })
    }
}
impl ModelPort for Port {
    fn binding(&self) -> ModelPortBinding {
        self.binding.lock().unwrap().clone()
    }
    fn generate<'a>(
        &'a self,
        _: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        panic!("registry lookup must not invoke a provider")
    }
}

#[test]
fn exact_scope_provider_adapter_and_connection_revisions_select_independent_ports() {
    let base = route();
    let mut cases = vec![(scope(), base.clone())];
    cases.extend([
        (
            Scope {
                tenant_id: id("other"),
                ..scope()
            },
            base.clone(),
        ),
        (
            Scope {
                workspace_id: id("other"),
                ..scope()
            },
            base.clone(),
        ),
        (
            Scope {
                user_id: Some(id("user")),
                ..scope()
            },
            base.clone(),
        ),
        (
            scope(),
            ResolvedModelRoute {
                provider: id("other"),
                ..base.clone()
            },
        ),
        (
            scope(),
            ResolvedModelRoute {
                adapter: reference("other-adapter"),
                ..base.clone()
            },
        ),
        (
            scope(),
            ResolvedModelRoute {
                adapter: VersionedRef {
                    version: id("2"),
                    ..base.adapter.clone()
                },
                ..base.clone()
            },
        ),
        (
            scope(),
            ResolvedModelRoute {
                connection_ref: reference("other-connection"),
                ..base.clone()
            },
        ),
        (
            scope(),
            ResolvedModelRoute {
                connection_ref: VersionedRef {
                    version: id("2"),
                    ..base.connection_ref.clone()
                },
                ..base
            },
        ),
    ]);
    let ports: Vec<_> = cases.iter().map(|(_, route)| Port::new(route)).collect();
    let registry: Arc<dyn ModelDispatcher> = Arc::new(
        RegistryModelDispatcher::new(
            cases
                .iter()
                .zip(&ports)
                .map(|((scope, _), port)| ModelDispatcherEntry {
                    scope: scope.clone(),
                    port: port.clone(),
                })
                .collect(),
        )
        .unwrap(),
    );
    for ((scope, route), port) in cases.iter().zip(&ports) {
        let expected: Arc<dyn ModelPort> = port.clone();
        assert!(Arc::ptr_eq(
            &registry.resolve(scope, route).unwrap(),
            &expected
        ));
        assert_eq!(port.calls.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn missing_exact_keys_never_fall_back_and_duplicate_registration_is_rejected() {
    let base = route();
    let port = Port::new(&base);
    let entry = ModelDispatcherEntry {
        scope: scope(),
        port: port.clone(),
    };
    assert_eq!(
        RegistryModelDispatcher::new(vec![entry.clone(), entry.clone()])
            .unwrap_err()
            .code,
        ErrorCode::ModelBindingInvalid
    );
    let registry = RegistryModelDispatcher::new(vec![entry]).unwrap();
    for owner in [
        Scope {
            tenant_id: id("foreign"),
            ..scope()
        },
        Scope {
            workspace_id: id("foreign"),
            ..scope()
        },
        Scope {
            user_id: Some(id("user")),
            ..scope()
        },
    ] {
        assert!(registry.resolve(&owner, &base).is_err());
    }
    for changed in [
        ResolvedModelRoute {
            provider: id("other"),
            ..base.clone()
        },
        ResolvedModelRoute {
            adapter: VersionedRef {
                version: id("2"),
                ..base.adapter.clone()
            },
            ..base.clone()
        },
        ResolvedModelRoute {
            connection_ref: VersionedRef {
                version: id("2"),
                ..base.connection_ref.clone()
            },
            ..base.clone()
        },
    ] {
        assert!(registry.resolve(&scope(), &changed).is_err());
    }
    port.binding.lock().unwrap().connection_ref = reference("replacement-account");
    assert!(registry.resolve(&scope(), &base).is_err());
    assert_eq!(port.calls.load(Ordering::SeqCst), 0);
}

fn observation(route: &ResolvedModelRoute) -> ModelRouteObservation {
    ModelRouteObservation {
        route_digest: route.digest(),
        availability: ModelRouteAvailability::Available,
        model_id: Some(route.model_id.clone()),
        model_version: Some(route.model_version.clone()),
        deployment_revision: route.deployment_revision.clone(),
        version_semantics: VersionSemantics::Pinned,
        evidence_ref: id("synthetic-metadata-evidence"),
    }
}

#[test]
fn observed_route_and_known_model_or_deployment_changes_are_always_drift() {
    let route = route();
    let known = observation(&route);
    known
        .validate(&route, VersionPolicy::RequirePinned)
        .unwrap();
    let changes = [
        ModelRouteObservation {
            route_digest: canonical_digest(&json!("another route")),
            ..known.clone()
        },
        ModelRouteObservation {
            model_id: Some(id("other model")),
            ..known.clone()
        },
        ModelRouteObservation {
            model_version: Some(id("other version")),
            ..known.clone()
        },
        ModelRouteObservation {
            deployment_revision: Some(id("other deployment revision")),
            ..known
        },
    ];
    for changed in changes {
        for policy in [VersionPolicy::RequirePinned, VersionPolicy::AllowMutable] {
            assert_eq!(
                changed.validate(&route, policy).unwrap_err().code,
                ErrorCode::ModelVersionDrift
            );
        }
    }
}

#[test]
fn unknown_observations_are_not_filled_from_requested_versions_or_promoted_to_pinned() {
    let route = route();
    let known = observation(&route);
    for incomplete in [
        ModelRouteObservation {
            model_id: None,
            ..known.clone()
        },
        ModelRouteObservation {
            model_version: None,
            ..known.clone()
        },
        ModelRouteObservation {
            deployment_revision: None,
            ..known.clone()
        },
        ModelRouteObservation {
            version_semantics: VersionSemantics::Unverified,
            ..known.clone()
        },
    ] {
        let original = incomplete.clone();
        assert_eq!(
            incomplete
                .validate(&route, VersionPolicy::RequirePinned)
                .unwrap_err()
                .code,
            ErrorCode::ModelVersionUnpinned
        );
        incomplete
            .validate(&route, VersionPolicy::AllowMutable)
            .unwrap();
        assert_eq!(incomplete, original);
    }
    let mutable_route = ResolvedModelRoute {
        version_semantics: VersionSemantics::MutableDeployment,
        ..route
    };
    let apparently_pinned = observation(&mutable_route);
    assert_eq!(
        apparently_pinned
            .validate(&mutable_route, VersionPolicy::RequirePinned)
            .unwrap_err()
            .code,
        ErrorCode::ModelVersionUnpinned
    );
    apparently_pinned
        .validate(&mutable_route, VersionPolicy::AllowMutable)
        .unwrap();
}

#[test]
fn availability_failures_and_unknown_fields_do_not_become_successful_metadata() {
    let route = route();
    for (availability, expected) in [
        (
            ModelRouteAvailability::Unavailable,
            ErrorCode::ModelUnavailable,
        ),
        (
            ModelRouteAvailability::Unknown,
            ErrorCode::ModelInspectionUnavailable,
        ),
    ] {
        let observed = ModelRouteObservation {
            availability,
            ..observation(&route)
        };
        for policy in [VersionPolicy::RequirePinned, VersionPolicy::AllowMutable] {
            assert_eq!(
                observed.validate(&route, policy).unwrap_err().code,
                expected
            );
        }
    }
    let known = observation(&route);
    let mut encoded = serde_json::to_value(&known).unwrap();
    encoded["provider_response"] = json!({"model":"not an inspection field"});
    assert!(serde_json::from_value::<ModelRouteObservation>(encoded).is_err());
    let encoded = serde_json::to_string(&known).unwrap();
    let restored: ModelRouteObservation = serde_json::from_str(&encoded).unwrap();
    restored
        .validate(&route, VersionPolicy::RequirePinned)
        .unwrap();
}
```

## `crates/wickle-model-router/tests/routed_execution.rs`

```rust
//! Core-owned attempts, policy, inspection and fallback through real routing and dispatch.

use std::{sync::Arc, time::Duration};

use serde_json::json;
use wickle::*;
use wickle_model_router::PolicyModelRouter;

#[allow(dead_code)]
#[path = "../../wickle/tests/support/mod.rs"]
mod core;
#[path = "support/routed.rs"]
mod support;
use core::{id, scope};
use support::*;

#[tokio::test]
async fn retry_and_fallback_share_saved_budgets_and_keep_exact_accounts_and_inspection_evidence() {
    let fixture = Fixture::new(
        vec![
            Reply::Fail(ModelFailureKind::RateLimited),
            Reply::Fail(ModelFailureKind::RateLimited),
        ],
        vec![Reply::Complete],
    )
    .await;
    let input = fixture.input("step");
    let result = fixture
        .exchange(1)
        .generate_routed(
            &fixture.router,
            &input,
            &fixture.projector,
            &fixture.context(),
            &fixture.budget().await,
        )
        .await
        .unwrap();
    let response = completed(result);
    assert_eq!(fixture.call_counts(), (2, 1));
    assert_eq!(
        response.route_digest,
        fixture.second.calls.lock().unwrap()[0].route.digest()
    );
    let saved = fixture.saved().await;
    assert_eq!(saved.usage.model_calls, 3);
    assert_eq!(saved.usage.recovery_attempts, 2);
    assert_eq!(saved.model_ledger.len(), 3);
    assert_eq!(
        saved
            .model_ledger
            .iter()
            .map(|entry| entry.selection_reason.as_str())
            .collect::<Vec<_>>(),
        ["initial_route", "same_route_retry", "fallback_rate_limited"]
    );
    assert_ne!(
        saved.model_ledger[0].attempt_id,
        saved.model_ledger[1].attempt_id
    );
    assert_ne!(
        saved.model_ledger[1].attempt_id,
        saved.model_ledger[2].attempt_id
    );
    for entry in &saved.model_ledger {
        let reference = entry
            .inspection_ref
            .as_ref()
            .expect("inspection evidence must be persisted");
        let record = fixture
            .store
            .read_record(&scope(), reference)
            .await
            .unwrap();
        let observed: ModelRouteObservation =
            serde_json::from_value(record.value().clone()).unwrap();
        observed
            .validate(&entry.route, VersionPolicy::RequirePinned)
            .unwrap();
        assert_eq!(
            observed.model_version.as_ref(),
            Some(&entry.route.model_version)
        );
        assert!(entry.reported_model_version.is_none());
        let policies = fixture.policy.calls.lock().unwrap();
        assert!(
            policies
                .iter()
                .filter(|(route, _)| route == &entry.route)
                .count()
                >= 2
        );
    }
    for request in fixture
        .first
        .calls
        .lock()
        .unwrap()
        .iter()
        .chain(fixture.second.calls.lock().unwrap().iter())
    {
        assert_eq!(request.options, input.routing.options);
        assert_eq!(request.messages[0].role, ModelRole::User);
    }
    assert_eq!(
        saved.model_ledger[0].route.connection_ref.id,
        id("primary-account")
    );
    assert_eq!(
        saved.model_ledger[2].route.connection_ref.id,
        id("fallback-account")
    );
    assert_eq!(fixture.inspector.calls.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn denied_fallback_is_neither_projected_nor_inspected_nor_dispatched() {
    let fixture = Fixture::new(
        vec![Reply::Fail(ModelFailureKind::RateLimited)],
        vec![Reply::Complete],
    )
    .await;
    *fixture.policy.denied_provider.lock().unwrap() = Some(id("provider-b"));
    let error = fixture
        .exchange(0)
        .generate_routed(
            &fixture.router,
            &fixture.input("step"),
            &fixture.projector,
            &fixture.context(),
            &fixture.budget().await,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::AccessDenied);
    assert_eq!(fixture.call_counts(), (1, 0));
    assert_eq!(fixture.inspector.calls.lock().unwrap().len(), 1);
    assert_eq!(fixture.projector.calls.lock().unwrap().len(), 1);
    let requests = fixture.policy.calls.lock().unwrap();
    let denied = requests
        .iter()
        .find(|(route, _)| route.provider == id("provider-b"))
        .unwrap();
    assert_eq!(denied.0.target["region"], json!("fallback-region"));
    assert_eq!(denied.0.connection_ref.id, id("fallback-account"));
}

#[tokio::test]
async fn completed_steps_reuse_saved_response_but_recheck_permission_and_projection_identity() {
    let fixture = Fixture::new(vec![Reply::Complete], vec![]).await;
    let input = fixture.input("step");
    let exchange = fixture.exchange(0);
    let first = completed(
        exchange
            .generate_routed(
                &fixture.router,
                &input,
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            )
            .await
            .unwrap(),
    );
    let revision = fixture.saved().await.revision;
    let second = completed(
        exchange
            .generate_routed(
                &fixture.router,
                &input,
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            )
            .await
            .unwrap(),
    );
    assert_eq!(first, second);
    assert_eq!(fixture.call_counts(), (1, 0));
    assert_eq!(fixture.inspector.calls.lock().unwrap().len(), 1);
    assert_eq!(fixture.saved().await.revision, revision);
    *fixture.projector.mode.lock().unwrap() = Projection::DifferentContent;
    assert_eq!(
        exchange
            .generate_routed(
                &fixture.router,
                &input,
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::RequestConflict
    );
    *fixture.projector.mode.lock().unwrap() = Projection::Valid;
    *fixture.policy.denied_provider.lock().unwrap() = Some(id("provider-a"));
    assert_eq!(
        exchange
            .generate_routed(
                &fixture.router,
                &input,
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(fixture.call_counts(), (1, 0));
}

#[tokio::test]
async fn a_pinned_routing_snapshot_cannot_be_replaced_by_a_new_policy_or_catalog() {
    let fixture = Fixture::new(vec![Reply::Complete], vec![]).await;
    let exchange = fixture.exchange(0);
    exchange
        .generate_routed(
            &fixture.router,
            &fixture.input("step"),
            &fixture.projector,
            &fixture.context(),
            &fixture.budget().await,
        )
        .await
        .unwrap();
    for catalog_change in [false, true] {
        let mut catalog = fixture.snapshot.catalog().clone();
        let mut policy = fixture.snapshot.policy().clone();
        if catalog_change {
            catalog.revision = id("catalog-2");
        } else {
            policy.revision = id("policy-2");
        }
        let replacement =
            PolicyModelRouter::new(RoutingSnapshot::new(catalog, policy).unwrap()).unwrap();
        assert_eq!(
            exchange
                .generate_routed(
                    &replacement,
                    &fixture.input("another-step"),
                    &fixture.projector,
                    &fixture.context(),
                    &fixture.budget().await
                )
                .await
                .unwrap_err()
                .code,
            ErrorCode::ModelRoutingMismatch
        );
    }
    let saved = fixture.saved().await;
    let reference = saved.routing_snapshot_ref.unwrap();
    let record = fixture
        .store
        .read_record(&scope(), &reference)
        .await
        .unwrap();
    let restored =
        RoutingSnapshot::restore(&record.value().to_string(), &scope(), &reference.digest).unwrap();
    assert_eq!(restored.digest(), fixture.snapshot.digest());
    assert_eq!(fixture.call_counts(), (1, 0));
}

#[tokio::test]
async fn malformed_route_projection_options_tokens_and_foreign_opaque_stop_before_model_calls() {
    for mode in [
        Projection::WrongRoute,
        Projection::WrongOptions,
        Projection::TooManyTokens,
        Projection::OldOpaque,
    ] {
        let fixture = Fixture::new(vec![Reply::Complete], vec![]).await;
        *fixture.projector.mode.lock().unwrap() = mode;
        assert!(
            fixture
                .exchange(0)
                .generate_routed(
                    &fixture.router,
                    &fixture.input("step"),
                    &fixture.projector,
                    &fixture.context(),
                    &fixture.budget().await
                )
                .await
                .is_err()
        );
        assert_eq!(fixture.call_counts(), (0, 0));
        assert!(fixture.inspector.calls.lock().unwrap().is_empty());
        assert_eq!(fixture.saved().await.usage.model_calls, 0);
    }
    let fixture = Fixture::new(vec![Reply::Complete], vec![]).await;
    let mut input = fixture.input("step");
    input.routing.scope.workspace_id = id("foreign");
    assert_eq!(
        fixture
            .exchange(0)
            .generate_routed(
                &fixture.router,
                &input,
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert!(fixture.projector.calls.lock().unwrap().is_empty());
    assert_eq!(fixture.call_counts(), (0, 0));
}

#[tokio::test]
async fn actual_tools_and_json_output_require_capabilities_even_when_the_host_requested_only_text()
{
    for (mode, feature) in [
        (Projection::UsesTools, "tool_calling"),
        (Projection::UsesJson, "json_output"),
    ] {
        for supported in [false, true] {
            let mut fixture = Fixture::new(vec![Reply::Complete], vec![]).await;
            if supported {
                fixture.enable_feature(feature);
            }
            *fixture.projector.mode.lock().unwrap() = mode;
            let input = fixture.input("step");
            assert_eq!(
                input.routing.required_capabilities,
                [id("text")].into_iter().collect()
            );
            let result = fixture
                .exchange(0)
                .generate_routed(
                    &fixture.router,
                    &input,
                    &fixture.projector,
                    &fixture.context(),
                    &fixture.budget().await,
                )
                .await;
            if supported {
                completed(result.unwrap());
                assert_eq!(fixture.call_counts(), (1, 0));
            } else {
                assert!(result.is_err());
                assert_eq!(fixture.call_counts(), (0, 0));
                assert!(fixture.inspector.calls.lock().unwrap().is_empty());
            }
        }
    }
}

#[tokio::test]
async fn routed_invocations_need_valid_inspection_records_in_both_commits_and_restored_checkpoints()
{
    // The valid branch prevents unrelated checkpoint-shape errors from satisfying the rejection cases.
    for corruption in ["valid", "missing", "unverified"] {
        let fixture = Fixture::new(vec![Reply::Complete], vec![]).await;
        fixture
            .exchange(0)
            .generate_routed(
                &fixture.router,
                &fixture.input("step"),
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            )
            .await
            .unwrap();
        let attempt = fixture
            .budget()
            .await
            .reserve(ReservationKind::Model {
                purpose: ModelPurpose::Agent,
            })
            .await
            .unwrap();
        let saved = fixture.saved().await;
        let mut invocation = saved.model_ledger[0].clone();
        invocation.attempt_id = attempt.attempt_id;
        invocation.model_step_id = id("new-step");
        invocation.state = ModelAttemptState::Reserved {};
        invocation.response_ref = None;
        invocation.provider_request_id = None;
        invocation.reported_model_id = None;
        invocation.reported_model_version = None;
        invocation.usage = None;
        let mut records = vec![];
        if corruption == "missing" {
            invocation.inspection_ref = None;
        }
        if corruption == "unverified" {
            let existing = fixture
                .store
                .read_record(&scope(), invocation.inspection_ref.as_ref().unwrap())
                .await
                .unwrap();
            let mut observation: ModelRouteObservation =
                serde_json::from_value(existing.value().clone()).unwrap();
            observation.version_semantics = VersionSemantics::Unverified;
            let changed = ProtectedRecord::new(
                id("unverified-inspection"),
                1,
                serde_json::to_value(observation).unwrap(),
            );
            invocation.inspection_ref = Some(changed.reference().clone());
            records.push(changed);
        }
        let mut update = core::prepared(&saved, fixture.lease.clone(), 0);
        update.snapshot.model_ledger.push(invocation);
        update.records = records.clone();
        let checkpoint = fixture.store.export_checkpoint(&scope()).unwrap();
        let mut value = serde_json::to_value(checkpoint).unwrap();
        value["runs"][0]["snapshot"] = serde_json::to_value(&update.snapshot).unwrap();
        for record in records {
            value["records"]
                .as_array_mut()
                .unwrap()
                .push(json!({"reference":record.reference(),"value":record.value()}));
        }
        let restored = StateStoreCheckpoint::from_json(
            &value.to_string(),
            &scope(),
            &canonical_digest(&value),
        );
        let committed = fixture.store.commit(&scope(), &id("run"), update).await;
        assert_eq!(
            restored.is_ok(),
            corruption == "valid",
            "checkpoint inspection contract: {corruption}"
        );
        assert_eq!(
            committed.is_ok(),
            corruption == "valid",
            "commit inspection contract: {corruption}"
        );
    }
}

#[tokio::test]
async fn inspector_failures_are_bounded_and_do_not_spend_physical_model_calls() {
    for mode in [Inspection::Pending, Inspection::Panics] {
        let fixture = Fixture::new(vec![Reply::Complete], vec![]).await;
        *fixture.inspector.mode.lock().unwrap() = mode;
        let exchange = fixture
            .exchange(0)
            .with_route_inspector(fixture.inspector.clone(), Duration::from_millis(30))
            .unwrap();
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            exchange.generate_routed(
                &fixture.router,
                &fixture.input("step"),
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::ModelInspectionUnavailable);
        assert_eq!(fixture.call_counts(), (0, 0));
        assert_eq!(fixture.saved().await.usage.model_calls, 0);
        assert!(
            fixture
                .inspector
                .tokens
                .lock()
                .unwrap()
                .iter()
                .all(|token| token.is_cancelled())
        );
    }
}

#[tokio::test]
async fn agent_verification_and_compaction_use_the_same_budget_and_policy_dispatch_path() {
    let fixture = Fixture::new(
        vec![Reply::Complete, Reply::Complete, Reply::Complete],
        vec![],
    )
    .await;
    for (step, purpose) in [
        ("agent", ModelPurpose::Agent),
        ("verify", ModelPurpose::Verification),
        ("compact", ModelPurpose::Compaction),
    ] {
        let mut input = fixture.input(step);
        input.routing.purpose = purpose;
        completed(
            fixture
                .exchange(0)
                .generate_routed(
                    &fixture.router,
                    &input,
                    &fixture.projector,
                    &fixture.context(),
                    &fixture.budget().await,
                )
                .await
                .unwrap(),
        );
        assert!(
            fixture
                .policy
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|(_, checked_purpose)| checked_purpose == &purpose)
        );
    }
    let saved = fixture.saved().await;
    assert_eq!(saved.usage.model_calls, 3);
    assert_eq!(saved.usage.recovery_attempts, 0);
    assert_eq!(
        saved
            .model_ledger
            .iter()
            .map(|entry| entry.purpose)
            .collect::<Vec<_>>(),
        [
            ModelPurpose::Agent,
            ModelPurpose::Verification,
            ModelPurpose::Compaction
        ]
    );
    assert_eq!(fixture.call_counts(), (3, 0));
    assert_eq!(fixture.inspector.calls.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn exhausted_model_or_recovery_budget_prevents_fallback_dispatch_and_keeps_partial_failure() {
    for (model_calls, recoveries) in [(1, 4), (8, 0)] {
        let fixture = Fixture::with_limits(
            vec![Reply::Fail(ModelFailureKind::RateLimited)],
            vec![],
            model_calls,
            recoveries,
        )
        .await;
        let error = fixture
            .exchange(0)
            .generate_routed(
                &fixture.router,
                &fixture.input("step"),
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::BudgetExceeded);
        assert_eq!(fixture.call_counts(), (1, 0));
        let saved = fixture.saved().await;
        assert_eq!(saved.usage.model_calls, 1);
        assert!(saved.usage.recovery_attempts <= recoveries);
        let record = fixture
            .store
            .read_record(
                &scope(),
                saved.model_ledger[0].response_ref.as_ref().unwrap(),
            )
            .await
            .unwrap();
        let stored: StoredModelResponse = serde_json::from_value(record.value().clone()).unwrap();
        let ModelExchangeOutcome::Failed { failure } = stored.outcome else {
            panic!("partial failure was lost")
        };
        assert_eq!(failure.kind, ModelFailureKind::RateLimited);
        assert_eq!(failure.partial_text(), "Partial response");
    }
}

#[tokio::test]
async fn caller_cancellation_reaches_a_hanging_inspector_even_with_a_distinct_run_token() {
    let fixture = Arc::new(Fixture::new(vec![Reply::Complete], vec![]).await);
    *fixture.inspector.mode.lock().unwrap() = Inspection::Pending;
    let context = fixture.context();
    let cancel = context.cancellation.clone();
    let task = {
        let fixture = fixture.clone();
        tokio::spawn(async move {
            fixture
                .exchange(0)
                .generate_routed(
                    &fixture.router,
                    &fixture.input("step"),
                    &fixture.projector,
                    &context,
                    &fixture.budget().await,
                )
                .await
        })
    };
    fixture.inspector.entered.notified().await;
    cancel.cancel();
    assert_eq!(task.await.unwrap().unwrap_err().code, ErrorCode::Cancelled);
    assert_eq!(fixture.call_counts(), (0, 0));
    assert!(fixture.inspector.tokens.lock().unwrap()[0].is_cancelled());
}

#[tokio::test]
async fn observed_drift_and_unavailability_use_only_the_explicit_finite_fallback_list() {
    for mode in [
        Inspection::DriftPrimary,
        Inspection::UnavailablePrimary,
        Inspection::UnavailableAll,
    ] {
        let fixture = Fixture::new(vec![], vec![Reply::Complete]).await;
        *fixture.inspector.mode.lock().unwrap() = mode;
        let result = fixture
            .exchange(0)
            .generate_routed(
                &fixture.router,
                &fixture.input("step"),
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            )
            .await;
        if matches!(mode, Inspection::UnavailableAll) {
            assert_eq!(result.unwrap_err().code, ErrorCode::ModelRoutesExhausted);
            assert_eq!(fixture.call_counts(), (0, 0));
        } else {
            completed(result.unwrap());
            assert_eq!(fixture.call_counts(), (0, 1));
        }
        assert_eq!(fixture.inspector.calls.lock().unwrap().len(), 2);
        assert_eq!(fixture.saved().await.usage.recovery_attempts, 1);
    }
}

#[tokio::test]
async fn unsettled_or_unknown_tool_effects_block_new_model_steps_and_fallback() {
    for state in [ToolCallState::Planned {}, unknown_tool_result()] {
        let fixture = Fixture::new(vec![Reply::Complete], vec![]).await;
        seed_tool(&fixture, state).await;
        let error = fixture
            .exchange(0)
            .generate_routed(
                &fixture.router,
                &fixture.input("step"),
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidTransition);
        assert_eq!(fixture.call_counts(), (0, 0));
        assert!(fixture.inspector.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn an_interrupted_physical_attempt_cannot_be_silently_reissued_on_resume() {
    let fixture = Arc::new(Fixture::new(vec![Reply::Pending, Reply::Complete], vec![]).await);
    let context = fixture.context();
    let cancel = context.cancellation.clone();
    let task = {
        let fixture = fixture.clone();
        tokio::spawn(async move {
            fixture
                .exchange(0)
                .generate_routed(
                    &fixture.router,
                    &fixture.input("step"),
                    &fixture.projector,
                    &context,
                    &fixture.budget().await,
                )
                .await
        })
    };
    fixture.first.entered.notified().await;
    cancel.cancel();
    assert_eq!(task.await.unwrap().unwrap_err().code, ErrorCode::Cancelled);
    assert!(matches!(
        fixture.saved().await.model_ledger[0].state,
        ModelAttemptState::Unknown {}
    ));
    assert_eq!(
        fixture
            .exchange(0)
            .generate_routed(
                &fixture.router,
                &fixture.input("step"),
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::ModelAttemptUnresolved
    );
    assert_eq!(fixture.call_counts(), (1, 0));
    assert_eq!(
        fixture
            .exchange(0)
            .generate_routed(
                &fixture.router,
                &fixture.input("different-step"),
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::ModelAttemptUnresolved
    );
    assert_eq!(fixture.call_counts(), (1, 0));
}
```

## `crates/wickle-model-router/tests/routing.rs`

```rust
//! Exact routing, purpose-specific constraints, finite fallback, and pinned restoration.

use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc};
use wickle::*;
use wickle_model_router::{FixedModelRouter, PolicyModelRouter};

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn versioned(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
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
fn features() -> BTreeSet<Id> {
    [id("text"), id("tools")].into_iter().collect()
}

fn model(name: &str) -> ModelDefinition {
    ModelDefinition {
        model_key: id(name),
        family: id("shared-family"),
        provider: id(&format!("provider-{name}")),
        model_id: id("wire-model"),
        model_version: id(&format!("release-{name}")),
        version_semantics: VersionSemantics::Pinned,
        lifecycle: ModelLifecycle::Active,
        capabilities: ModelCapabilities {
            revision: id("model-capabilities"),
            features: features(),
            options_schema: json!({"type":"object","properties":{"reasoning_effort":{"enum":["low","high"]}},"required":[],"additionalProperties":false}),
            context_window: 4096.try_into().unwrap(),
            max_output_tokens: 512.try_into().unwrap(),
        },
        evidence: vec![ModelEvidence {
            source_ref: id("synthetic-model-metadata"),
            observed_at_ms: 1000,
        }],
    }
}
fn proof(binding: &mut ModelBinding, model: &ModelDefinition) {
    binding.evidence = vec![ModelValidationEvidence {
        kind: ModelValidationKind::ContractTest,
        binding_digest: binding.contract_digest(model).unwrap(),
        checked_at_ms: 1000,
        evidence_ref: id("synthetic-proof"),
        passed: true,
    }];
}
fn binding(name: &str, model: &ModelDefinition) -> ModelBinding {
    let mut binding = ModelBinding {
        binding: versioned(name),
        model: model.reference(),
        requested_model: model.model_id.clone(),
        adapter: versioned(&format!("adapter-{name}")),
        connection_ref: versioned(&format!("connection-{name}")),
        target: object(
            json!({"region":format!("region-{name}"),"deployment":format!("deployment-{name}")}),
        ),
        target_schema: json!({"type":"object","properties":{"region":{"type":"string"},"deployment":{"type":"string"}},"required":["region","deployment"],"additionalProperties":false}),
        api_contract: ApiContract {
            operation: id(&format!("operation-{name}")),
            version: id(&format!("api-{name}")),
        },
        deployment_revision: Some(id(&format!("deployment-revision-{name}"))),
        version_semantics: VersionSemantics::Pinned,
        capabilities: ModelCapabilities {
            revision: id(&format!("capabilities-{name}")),
            features: features(),
            options_schema: model.capabilities.options_schema.clone(),
            context_window: 1024.try_into().unwrap(),
            max_output_tokens: 128.try_into().unwrap(),
        },
        support: ModelSupportStatus::ContractTested,
        evidence: vec![],
    };
    proof(&mut binding, model);
    binding
}
fn catalog() -> ModelCatalogSnapshot {
    let models: Vec<_> = ["primary", "second", "third", "outside"]
        .into_iter()
        .map(model)
        .collect();
    let bindings = models
        .iter()
        .map(|model| binding(model.model_key.as_str(), model))
        .collect();
    ModelCatalogSnapshot {
        revision: id("catalog-1"),
        scope: scope(),
        models,
        bindings,
        aliases: vec![],
    }
}
fn rule(purpose: ModelPurpose, primary: &str, fallbacks: &[&str]) -> RoutingRule {
    RoutingRule {
        model_binding: id("logical"),
        purpose,
        primary: versioned(primary),
        fallbacks: fallbacks.iter().map(|name| versioned(name)).collect(),
        fallback_on: vec![
            ModelFailureKind::Transport,
            ModelFailureKind::Unavailable,
            ModelFailureKind::VersionDrift,
        ],
        version_policy: VersionPolicy::RequirePinned,
        min_support: ModelSupportStatus::ContractTested,
    }
}
fn policy(rules: Vec<RoutingRule>) -> RoutingPolicy {
    RoutingPolicy {
        revision: id("policy-1"),
        scope: scope(),
        rules,
    }
}
fn snapshot(catalog: ModelCatalogSnapshot, rules: Vec<RoutingRule>) -> RoutingSnapshot {
    RoutingSnapshot::new(catalog, policy(rules)).unwrap()
}
fn router() -> PolicyModelRouter {
    PolicyModelRouter::new(snapshot(
        catalog(),
        vec![rule(ModelPurpose::Agent, "primary", &["second", "third"])],
    ))
    .unwrap()
}
fn request() -> RouteRequest {
    RouteRequest {
        model_binding: id("logical"),
        purpose: ModelPurpose::Agent,
        required_capabilities: [id("text")].into_iter().collect(),
        input_tokens: 100,
        max_output_tokens: 32.try_into().unwrap(),
        options: JsonObject::new(),
        scope: scope(),
        allowed_bindings: vec![id("primary"), id("second"), id("third"), id("outside")],
        version_policy: VersionPolicy::RequirePinned,
        previous_route: None,
        previous_failure: None,
    }
}

#[tokio::test]
async fn purposes_choose_exact_independent_provider_api_region_and_connection_bindings() {
    let catalog = catalog();
    let router = PolicyModelRouter::new(snapshot(
        catalog.clone(),
        vec![
            rule(ModelPurpose::Agent, "primary", &[]),
            rule(ModelPurpose::Verification, "second", &[]),
            rule(ModelPurpose::Compaction, "third", &[]),
        ],
    ))
    .unwrap();
    for (purpose, index) in [
        (ModelPurpose::Agent, 0),
        (ModelPurpose::Verification, 1),
        (ModelPurpose::Compaction, 2),
    ] {
        let mut input = request();
        input.purpose = purpose;
        let selected = router.resolve(&input).await.unwrap();
        let expected = &catalog.bindings[index];
        let definition = &catalog.models[index];
        assert_eq!(selected.route.binding, expected.binding);
        assert_eq!(selected.route.provider, definition.provider);
        assert_eq!(selected.route.model_version, definition.model_version);
        assert_eq!(selected.route.target, expected.target);
        assert_eq!(selected.route.api_contract, expected.api_contract);
        assert_eq!(selected.route.adapter, expected.adapter);
        assert_eq!(selected.route.connection_ref, expected.connection_ref);
        assert_eq!(
            selected.route.deployment_revision,
            expected.deployment_revision
        );
        assert_eq!(selected.route.catalog_revision, id("catalog-1"));
        assert_eq!(selected.route.routing_policy_revision, id("policy-1"));
        assert_eq!(selected.reason, RouteSelectionReason::Initial);
        assert_eq!(selected.candidate_index, 0);
        assert_eq!(selected.request_digest, input.digest());
        assert_eq!(selected.routing_snapshot_digest, router.snapshot().digest());
    }
}

#[tokio::test]
async fn exact_scope_and_declared_purpose_are_required_without_implicit_defaults() {
    let router = router();
    for scope in [
        Scope {
            tenant_id: id("foreign"),
            ..scope()
        },
        Scope {
            workspace_id: id("foreign"),
            ..scope()
        },
        Scope {
            user_id: Some(id("user")),
            ..scope()
        },
    ] {
        let mut input = request();
        input.scope = scope;
        assert_eq!(
            router.resolve(&input).await.unwrap_err().code,
            ErrorCode::AccessDenied
        );
    }
    let mut input = request();
    input.purpose = ModelPurpose::Verification;
    assert!(router.resolve(&input).await.is_err());
    input = request();
    input.model_binding = id("undeclared");
    assert!(router.resolve(&input).await.is_err());
}

#[tokio::test]
async fn caller_allowlists_only_narrow_the_exact_policy_candidates() {
    let router = router();
    for allowed in [vec![], vec![id("outside")], vec![id("second")]] {
        let mut input = request();
        input.allowed_bindings = allowed;
        assert_eq!(
            router.resolve(&input).await.unwrap_err().code,
            ErrorCode::ModelRouteDenied
        );
    }
    let mut input = request();
    let first = router.resolve(&input).await.unwrap();
    input.previous_route = Some(first.route);
    input.previous_failure = Some(ModelFailureKind::Transport);
    input.allowed_bindings = vec![id("third"), id("outside")];
    let selected = router.resolve(&input).await.unwrap();
    assert_eq!(selected.route.binding, versioned("third"));
    assert_eq!(selected.candidate_index, 2);
    input.allowed_bindings = vec![id("outside")];
    assert_eq!(
        router.resolve(&input).await.unwrap_err().code,
        ErrorCode::ModelRouteDenied
    );
}

#[tokio::test]
async fn fallback_advances_in_declared_order_and_never_wraps_or_retries_the_previous_candidate() {
    let router = router();
    let mut input = request();
    for (index, name) in ["primary", "second", "third"].into_iter().enumerate() {
        let selected = router.resolve(&input).await.unwrap();
        assert_eq!(selected.candidate_index, index);
        assert_eq!(selected.route.binding, versioned(name));
        if index > 0 {
            assert_eq!(
                selected.reason,
                RouteSelectionReason::Fallback {
                    failure: ModelFailureKind::Transport
                }
            );
        }
        input.previous_route = Some(selected.route);
        input.previous_failure = Some(ModelFailureKind::Transport);
    }
    assert_eq!(
        router.resolve(&input).await.unwrap_err().code,
        ErrorCode::ModelRoutesExhausted
    );
}

#[tokio::test]
async fn fallback_requires_a_saved_route_and_an_explicitly_permitted_failure() {
    let router = router();
    let mut input = request();
    input.previous_failure = Some(ModelFailureKind::Transport);
    assert_eq!(
        router.resolve(&input).await.unwrap_err().code,
        ErrorCode::ModelRoutingInvalid
    );
    input = request();
    input.previous_route = Some(router.resolve(&input).await.unwrap().route);
    input.previous_failure = Some(ModelFailureKind::Authentication);
    assert_eq!(
        router.resolve(&input).await.unwrap_err().code,
        ErrorCode::ModelRouteDenied
    );
    input.previous_failure = None;
    let reused = router.resolve(&input).await.unwrap();
    assert_eq!(reused.route, input.previous_route.unwrap());
    assert_eq!(reused.reason, RouteSelectionReason::Reuse);
}

#[tokio::test]
async fn unavailable_primary_does_not_select_a_healthy_fallback_without_a_failure_transition() {
    let mut catalog = catalog();
    catalog.models[0].lifecycle = ModelLifecycle::Retired;
    proof(&mut catalog.bindings[0], &catalog.models[0]);
    let router = PolicyModelRouter::new(snapshot(
        catalog,
        vec![rule(ModelPurpose::Agent, "primary", &["second"])],
    ))
    .unwrap();
    assert_eq!(
        router.resolve(&request()).await.unwrap_err().code,
        ErrorCode::ModelUnavailable
    );
}

#[tokio::test]
async fn options_features_and_token_space_are_checked_without_mutating_the_request() {
    let router = router();
    let mut input = request();
    input.options = object(json!({"reasoning_effort":"high"}));
    let original = input.clone();
    router.resolve(&input).await.unwrap();
    assert_eq!(input, original);
    for options in [
        json!({"reasoning_effort":"unregistered-level"}),
        json!({"unknown":true}),
        json!({"reasoning_effort":3}),
    ] {
        input.options = object(options);
        assert_eq!(
            router.resolve(&input).await.unwrap_err().code,
            ErrorCode::ModelOptionUnsupported
        );
    }
    input = request();
    input
        .required_capabilities
        .insert(id("unavailable-feature"));
    assert_eq!(
        router.resolve(&input).await.unwrap_err().code,
        ErrorCode::ModelCapabilityUnsupported
    );
    input = request();
    input.max_output_tokens = 129.try_into().unwrap();
    assert!(router.resolve(&input).await.is_err());
    input = request();
    input.input_tokens = 1000;
    assert!(router.resolve(&input).await.is_err());
    input.input_tokens = u64::MAX;
    assert!(router.resolve(&input).await.is_err());
}

#[tokio::test]
async fn fallback_skips_incompatible_bindings_without_dropping_requested_options() {
    let mut catalog = catalog();
    catalog.bindings[1].capabilities.options_schema = json!({"type":"object","properties":{"reasoning_effort":{"enum":["low"]}},"required":[],"additionalProperties":false});
    proof(&mut catalog.bindings[1], &catalog.models[1]);
    let router = PolicyModelRouter::new(snapshot(
        catalog,
        vec![rule(ModelPurpose::Agent, "primary", &["second", "third"])],
    ))
    .unwrap();
    let mut input = request();
    input.options = object(json!({"reasoning_effort":"high"}));
    input.previous_route = Some(router.resolve(&input).await.unwrap().route);
    input.previous_failure = Some(ModelFailureKind::Transport);
    let original = input.clone();
    let selected = router.resolve(&input).await.unwrap();
    assert_eq!(selected.route.binding, versioned("third"));
    assert_eq!(selected.candidate_index, 2);
    assert_eq!(input, original);
    input.allowed_bindings = vec![id("second")];
    assert_eq!(
        router.resolve(&input).await.unwrap_err().code,
        ErrorCode::ModelRoutesExhausted
    );
}

#[tokio::test]
async fn request_and_policy_version_requirements_can_only_be_tightened() {
    for (policy_version, request_version, allowed) in [
        (
            VersionPolicy::RequirePinned,
            VersionPolicy::AllowMutable,
            false,
        ),
        (
            VersionPolicy::AllowMutable,
            VersionPolicy::RequirePinned,
            false,
        ),
        (
            VersionPolicy::AllowMutable,
            VersionPolicy::AllowMutable,
            true,
        ),
    ] {
        let mut catalog = catalog();
        catalog.bindings[0].version_semantics = VersionSemantics::MutableDeployment;
        proof(&mut catalog.bindings[0], &catalog.models[0]);
        let mut rule = rule(ModelPurpose::Agent, "primary", &[]);
        rule.version_policy = policy_version;
        let router = PolicyModelRouter::new(snapshot(catalog, vec![rule])).unwrap();
        let mut input = request();
        input.version_policy = request_version;
        let result = router.resolve(&input).await;
        assert_eq!(result.is_ok(), allowed);
        if let Ok(selected) = result {
            assert_eq!(
                selected.route.version_semantics,
                VersionSemantics::MutableDeployment
            );
        }
    }
}

#[tokio::test]
async fn live_support_requirement_cannot_be_satisfied_by_contract_only_evidence() {
    let mut rule = rule(ModelPurpose::Agent, "primary", &[]);
    rule.min_support = ModelSupportStatus::LiveVerified;
    let router = PolicyModelRouter::new(snapshot(catalog(), vec![rule])).unwrap();
    assert_eq!(
        router.resolve(&request()).await.unwrap_err().code,
        ErrorCode::ModelSupportInsufficient
    );
}

#[tokio::test]
async fn changed_previous_route_metadata_is_rejected_even_when_the_binding_name_matches() {
    let router = router();
    let original = router.resolve(&request()).await.unwrap().route;
    for variant in 0..5 {
        let mut previous = original.clone();
        match variant {
            0 => previous.connection_ref.version = id("changed"),
            1 => {
                previous
                    .target
                    .insert("region".into(), json!("foreign-region"));
            }
            2 => previous.model_version = id("changed"),
            3 => previous.catalog_revision = id("changed"),
            4 => previous.api_contract.version = id("changed"),
            _ => unreachable!(),
        }
        let mut input = request();
        input.previous_route = Some(previous);
        assert_eq!(
            router.resolve(&input).await.unwrap_err().code,
            ErrorCode::ModelRoutingMismatch
        );
    }
}

#[tokio::test]
async fn restoring_pinned_routing_ignores_a_new_catalog_and_new_policy_default() {
    let old_snapshot = snapshot(
        catalog(),
        vec![rule(ModelPurpose::Agent, "primary", &["second"])],
    );
    let encoded = serde_json::to_string(&old_snapshot).unwrap();
    let digest = old_snapshot.digest();
    let old_router = PolicyModelRouter::new(old_snapshot).unwrap();
    let original = old_router.resolve(&request()).await.unwrap();
    let mut new_catalog = catalog();
    new_catalog.revision = id("catalog-2");
    new_catalog.models[0].model_version = id("new-release");
    new_catalog.bindings[0].model = new_catalog.models[0].reference();
    proof(&mut new_catalog.bindings[0], &new_catalog.models[0]);
    let mut new_policy = policy(vec![rule(ModelPurpose::Agent, "second", &[])]);
    new_policy.revision = id("policy-2");
    let new_router =
        PolicyModelRouter::new(RoutingSnapshot::new(new_catalog, new_policy).unwrap()).unwrap();
    assert_eq!(
        new_router.resolve(&request()).await.unwrap().route.binding,
        versioned("second")
    );
    let restored = RoutingSnapshot::restore(&encoded, &scope(), &digest).unwrap();
    let restored = PolicyModelRouter::new(restored).unwrap();
    let mut input = request();
    input.previous_route = Some(original.route.clone());
    let selected = restored.resolve(&input).await.unwrap();
    assert_eq!(selected.route, original.route);
    assert_eq!(selected.route.model_version, id("release-primary"));
    assert_eq!(selected.route.catalog_revision, id("catalog-1"));
    assert!(new_router.resolve(&input).await.is_err());
}

#[test]
fn snapshot_restore_requires_the_saved_scope_digest_version_and_valid_policy_references() {
    let saved = snapshot(
        catalog(),
        vec![rule(ModelPurpose::Agent, "primary", &["second"])],
    );
    let encoded = serde_json::to_string(&saved).unwrap();
    assert!(
        RoutingSnapshot::restore(
            &encoded,
            &Scope {
                tenant_id: id("foreign"),
                ..scope()
            },
            &saved.digest()
        )
        .is_err()
    );
    let mut changed = serde_json::to_value(&saved).unwrap();
    changed["policy"]["rules"][0]["primary"] = serde_json::to_value(versioned("second")).unwrap();
    assert!(RoutingSnapshot::restore(&changed.to_string(), &scope(), &saved.digest()).is_err());
    changed = serde_json::to_value(&saved).unwrap();
    changed["schema_version"] = json!("wickle.routing-snapshot.future");
    let digest = canonical_digest(&changed);
    assert_eq!(
        RoutingSnapshot::restore(&changed.to_string(), &scope(), &digest)
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedSchemaVersion
    );
    changed = serde_json::to_value(&saved).unwrap();
    changed["policy"]["rules"][0]["primary"] = serde_json::to_value(versioned("missing")).unwrap();
    let digest = canonical_digest(&changed);
    assert!(RoutingSnapshot::restore(&changed.to_string(), &scope(), &digest).is_err());
}

#[test]
fn policy_rejects_duplicate_and_unknown_candidates_or_repeated_failure_classes() {
    let base = rule(ModelPurpose::Agent, "primary", &["second"]);
    assert!(RoutingSnapshot::new(catalog(), policy(vec![base.clone(), base.clone()])).is_err());
    let mut duplicate = base.clone();
    duplicate.fallbacks = vec![duplicate.primary.clone()];
    assert!(RoutingSnapshot::new(catalog(), policy(vec![duplicate])).is_err());
    let mut missing = base.clone();
    missing.primary = versioned("missing");
    assert!(RoutingSnapshot::new(catalog(), policy(vec![missing])).is_err());
    let mut repeated = base.clone();
    repeated.fallback_on = vec![ModelFailureKind::Transport, ModelFailureKind::Transport];
    assert!(RoutingSnapshot::new(catalog(), policy(vec![repeated])).is_err());
}

#[tokio::test]
async fn fixed_and_policy_routers_share_the_dynamic_contract_but_fixed_rejects_multiple_targets() {
    let saved = snapshot(
        catalog(),
        vec![
            rule(ModelPurpose::Agent, "primary", &[]),
            rule(ModelPurpose::Verification, "primary", &[]),
        ],
    );
    let routers: Vec<Arc<dyn ModelRouter>> = vec![
        Arc::new(FixedModelRouter::new(saved.clone()).unwrap()),
        Arc::new(PolicyModelRouter::new(saved).unwrap()),
    ];
    let mut selected = Vec::new();
    for router in routers {
        selected.push(router.resolve(&request()).await.unwrap().route);
    }
    assert_eq!(selected[0], selected[1]);
    assert!(
        FixedModelRouter::new(snapshot(
            catalog(),
            vec![rule(ModelPurpose::Agent, "primary", &["second"])]
        ))
        .is_err()
    );
    assert!(
        FixedModelRouter::new(snapshot(
            catalog(),
            vec![
                rule(ModelPurpose::Agent, "primary", &[]),
                rule(ModelPurpose::Verification, "second", &[])
            ]
        ))
        .is_err()
    );
}

#[tokio::test]
async fn custom_selection_cannot_forge_binding_metadata_reason_or_provenance() {
    let router = router();
    let input = request();
    let original = router.resolve(&input).await.unwrap();
    router
        .snapshot()
        .validate_selection(&input, &original)
        .unwrap();
    for variant in 0..6 {
        let mut selected = original.clone();
        match variant {
            0 => selected.route.connection_ref.version = id("foreign-connection"),
            1 => selected.candidate_index = 1,
            2 => selected.reason = RouteSelectionReason::Reuse,
            3 => selected.routing_snapshot_digest = canonical_digest(&json!("another snapshot")),
            4 => selected.request_digest = canonical_digest(&json!("another request")),
            5 => {
                selected
                    .route
                    .target
                    .insert("region".into(), json!("foreign-region"));
            }
            _ => unreachable!(),
        }
        assert!(
            router
                .snapshot()
                .validate_selection(&input, &selected)
                .is_err()
        );
    }
    let mut changed = input;
    changed.options = object(json!({"reasoning_effort":"high"}));
    assert!(
        router
            .snapshot()
            .validate_selection(&changed, &original)
            .is_err()
    );
}

#[tokio::test]
async fn a_custom_router_cannot_skip_an_earlier_eligible_fallback_even_with_valid_route_digests() {
    let router = router();
    let mut input = request();
    input.previous_route = Some(router.resolve(&input).await.unwrap().route);
    input.previous_failure = Some(ModelFailureKind::Transport);
    let correct = router.resolve(&input).await.unwrap();
    assert_eq!(correct.route.binding, versioned("second"));
    let mut forged = correct.clone();
    forged.route = router
        .snapshot()
        .route_for_binding(&versioned("third"))
        .unwrap();
    forged.candidate_index = 2;
    assert_eq!(
        router
            .snapshot()
            .validate_selection(&input, &forged)
            .unwrap_err()
            .code,
        ErrorCode::ModelRoutingMismatch
    );
    input.allowed_bindings = vec![id("third")];
    forged.request_digest = input.digest();
    router
        .snapshot()
        .validate_selection(&input, &forged)
        .unwrap();
    forged.reason = RouteSelectionReason::Fallback {
        failure: ModelFailureKind::VersionDrift,
    };
    assert!(
        router
            .snapshot()
            .validate_selection(&input, &forged)
            .is_err()
    );
}

#[tokio::test]
async fn selection_validation_rechecks_actual_requirements_after_request_digest_is_updated() {
    let router = router();
    let input = request();
    let original = router.resolve(&input).await.unwrap();
    for variant in 0..4 {
        let mut changed = input.clone();
        match variant {
            0 => changed.allowed_bindings = vec![id("outside")],
            1 => {
                changed
                    .required_capabilities
                    .insert(id("unavailable-feature"));
            }
            2 => changed.options = object(json!({"unsupported_option":true})),
            3 => changed.max_output_tokens = 129.try_into().unwrap(),
            _ => unreachable!(),
        }
        let mut selected = original.clone();
        selected.request_digest = changed.digest();
        assert!(
            router
                .snapshot()
                .validate_selection(&changed, &selected)
                .is_err()
        );
    }
}

#[test]
fn policy_candidate_and_rule_bounds_reject_otherwise_valid_distinct_registrations() {
    let mut metadata = catalog();
    let definition = metadata.models[0].clone();
    let mut fallbacks = Vec::new();
    for index in 0..=MAX_ROUTE_FALLBACKS {
        let name = format!("fallback-{index}");
        let candidate = binding(&name, &definition);
        fallbacks.push(candidate.binding.clone());
        metadata.bindings.push(candidate);
    }
    let mut bounded = rule(ModelPurpose::Agent, "primary", &[]);
    bounded.fallbacks = fallbacks;
    assert!(RoutingSnapshot::new(metadata.clone(), policy(vec![bounded.clone()])).is_err());
    bounded.fallbacks.pop();
    RoutingSnapshot::new(metadata, policy(vec![bounded])).unwrap();
    let mut rules = Vec::new();
    for index in 0..=MAX_ROUTING_RULES {
        let mut next = rule(ModelPurpose::Agent, "primary", &[]);
        next.model_binding = id(&format!("logical-{index}"));
        rules.push(next);
    }
    assert!(RoutingSnapshot::new(catalog(), policy(rules.clone())).is_err());
    rules.pop();
    RoutingSnapshot::new(catalog(), policy(rules)).unwrap();
}

#[tokio::test]
async fn planned_metadata_can_be_stored_but_cannot_authorize_execution_selection() {
    let mut rule = rule(ModelPurpose::Agent, "primary", &[]);
    rule.min_support = ModelSupportStatus::Planned;
    let snapshot = snapshot(catalog(), vec![rule]);
    let router = PolicyModelRouter::new(snapshot.clone()).unwrap();
    let input = request();
    assert_eq!(
        router.resolve(&input).await.unwrap_err().code,
        ErrorCode::ModelSupportInsufficient
    );
    let selected = RouteSelection {
        route: snapshot.route_for_binding(&versioned("primary")).unwrap(),
        reason: RouteSelectionReason::Initial,
        candidate_index: 0,
        routing_snapshot_digest: snapshot.digest(),
        request_digest: input.digest(),
    };
    assert_eq!(
        snapshot
            .validate_selection(&input, &selected)
            .unwrap_err()
            .code,
        ErrorCode::ModelSupportInsufficient
    );
}
```

## `crates/wickle-model-router/tests/support/routed.rs`

```rust
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::stream;
use serde_json::json;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use wickle::*;
use wickle_model_router::{ModelDispatcherEntry, PolicyModelRouter, RegistryModelDispatcher};

use crate::core::{self, id, scope};

pub fn reference(name: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id("1"),
    }
}
pub fn options() -> JsonObject {
    [("effort".into(), json!("high"))].into_iter().collect()
}

pub fn routing_snapshot() -> RoutingSnapshot {
    let mut models = Vec::new();
    let mut bindings = Vec::new();
    for (name, provider) in [("primary", "provider-a"), ("fallback", "provider-b")] {
        let capabilities = ModelCapabilities {
            revision: id("capabilities"),
            features: [id("text")].into_iter().collect(),
            options_schema: json!({"type":"object","properties":{"effort":{"enum":["high","low"]}},"required":[],"additionalProperties":false}),
            context_window: 4096.try_into().unwrap(),
            max_output_tokens: 1024.try_into().unwrap(),
        };
        let model = ModelDefinition {
            model_key: id(name),
            family: id("fixture-family"),
            provider: id(provider),
            model_id: id(&format!("{name}-model")),
            model_version: id(&format!("{name}-release")),
            version_semantics: VersionSemantics::Pinned,
            lifecycle: ModelLifecycle::Active,
            capabilities: capabilities.clone(),
            evidence: vec![ModelEvidence {
                source_ref: id("fixture-manifest"),
                observed_at_ms: 1,
            }],
        };
        let mut binding = ModelBinding {
            binding: reference(name),
            model: model.reference(),
            requested_model: model.model_id.clone(),
            adapter: reference("adapter"),
            connection_ref: reference(&format!("{name}-account")),
            target: [("region".into(), json!(format!("{name}-region")))]
                .into_iter()
                .collect(),
            target_schema: json!({"type":"object","properties":{"region":{"type":"string"}},"required":["region"],"additionalProperties":false}),
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("protocol"),
            },
            deployment_revision: Some(id(&format!("{name}-deployment"))),
            version_semantics: VersionSemantics::Pinned,
            capabilities,
            support: ModelSupportStatus::ContractTested,
            evidence: vec![],
        };
        binding.evidence.push(ModelValidationEvidence {
            kind: ModelValidationKind::ContractTest,
            binding_digest: binding.contract_digest(&model).unwrap(),
            checked_at_ms: 1,
            evidence_ref: id("fixture-contract"),
            passed: true,
        });
        models.push(model);
        bindings.push(binding);
    }
    let catalog = ModelCatalogSnapshot {
        revision: id("catalog-1"),
        scope: scope(),
        models,
        bindings,
        aliases: vec![],
    };
    let rules = [
        ModelPurpose::Agent,
        ModelPurpose::Verification,
        ModelPurpose::Compaction,
    ]
    .into_iter()
    .map(|purpose| RoutingRule {
        model_binding: id("primary"),
        purpose,
        primary: reference("primary"),
        fallbacks: vec![reference("fallback")],
        fallback_on: vec![
            ModelFailureKind::RateLimited,
            ModelFailureKind::Transport,
            ModelFailureKind::Unavailable,
            ModelFailureKind::VersionDrift,
        ],
        version_policy: VersionPolicy::RequirePinned,
        min_support: ModelSupportStatus::ContractTested,
    })
    .collect();
    RoutingSnapshot::new(
        catalog,
        RoutingPolicy {
            revision: id("policy-1"),
            scope: scope(),
            rules,
        },
    )
    .unwrap()
}

pub struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: 0,
            monotonic_ms: 0,
        })
    }
    fn sleep_until<'a>(&'a self, deadline: u64) -> PortFuture<'a, ()> {
        if deadline == 0 {
            Box::pin(async { Ok(()) })
        } else {
            Box::pin(std::future::pending())
        }
    }
}
#[derive(Default)]
pub struct Ids(AtomicUsize);
impl IdSource for Ids {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id(&format!(
            "attempt-{}",
            self.0.fetch_add(1, Ordering::SeqCst)
        )))
    }
}

#[derive(Clone, Copy)]
pub enum Reply {
    Complete,
    Fail(ModelFailureKind),
    Pending,
}

pub struct Model {
    binding: ModelPortBinding,
    replies: Mutex<VecDeque<Reply>>,
    pub calls: Mutex<Vec<ModelRequest>>,
    pub entered: Notify,
}
impl Model {
    fn new(binding: &ModelBinding, replies: Vec<Reply>, snapshot: &RoutingSnapshot) -> Self {
        let model = snapshot
            .catalog()
            .models
            .iter()
            .find(|model| model.reference() == binding.model)
            .unwrap();
        Self {
            binding: ModelPortBinding {
                provider: model.provider.clone(),
                adapter: binding.adapter.clone(),
                connection_ref: binding.connection_ref.clone(),
            },
            replies: Mutex::new(replies.into()),
            calls: Mutex::new(vec![]),
            entered: Notify::new(),
        }
    }
}
impl ModelPort for Model {
    fn binding(&self) -> ModelPortBinding {
        self.binding.clone()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        assert!(
            self.binding.matches_route(&request.route),
            "provider/account binding mismatch"
        );
        assert_eq!(request.request_id, context.attempt_id);
        assert_eq!(context.scope, scope());
        self.calls.lock().unwrap().push(request.clone());
        self.entered.notify_one();
        match self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra physical model request")
        {
            Reply::Complete => Box::pin(stream::iter([
                Ok(ModelEvent::TextDelta {
                    text: "Completed response".into(),
                }),
                Ok(ModelEvent::ResponseCompleted {
                    finish: ModelFinish::Stop,
                    metadata: ModelResponseMetadata {
                        provider_request_id: Some(id("actual-provider-request")),
                        reported_model_id: Some(id("actually-reported-model")),
                        reported_model_version: None,
                        usage: None,
                    },
                    continuation: vec![],
                }),
            ])),
            Reply::Fail(kind) => Box::pin(stream::iter([
                Ok(ModelEvent::TextDelta {
                    text: "Partial response".into(),
                }),
                Ok(ModelEvent::ResponseError {
                    kind,
                    metadata: ModelResponseMetadata::default(),
                }),
            ])),
            Reply::Pending => Box::pin(stream::pending()),
        }
    }
}

#[derive(Default)]
pub struct Policy {
    pub denied_provider: Mutex<Option<Id>>,
    pub calls: Mutex<Vec<(ResolvedModelRoute, ModelPurpose)>>,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::InvokeModel { route, purpose } = &request.action {
                self.calls
                    .lock()
                    .unwrap()
                    .push((route.as_ref().clone(), *purpose));
                if self.denied_provider.lock().unwrap().as_ref() == Some(&route.provider) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("destination-denied"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}

#[derive(Clone, Copy, Default)]
pub enum Inspection {
    #[default]
    Healthy,
    DriftPrimary,
    UnavailablePrimary,
    UnavailableAll,
    Pending,
    Panics,
}
#[derive(Default)]
pub struct Inspector {
    pub mode: Mutex<Inspection>,
    pub calls: Mutex<Vec<ResolvedModelRoute>>,
    pub tokens: Mutex<Vec<CancellationToken>>,
    pub entered: Notify,
}
impl ModelRouteInspector for Inspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        context: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            assert_eq!(context.scope, scope());
            self.calls.lock().unwrap().push(route.clone());
            self.tokens
                .lock()
                .unwrap()
                .push(context.cancellation.clone());
            self.entered.notify_one();
            let mode = *self.mode.lock().unwrap();
            match mode {
                Inspection::Pending => std::future::pending::<()>().await,
                Inspection::Panics => panic!("fixture inspector panic"),
                _ => {}
            }
            let primary = route.binding.id == id("primary");
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: if matches!(mode, Inspection::UnavailableAll)
                    || (primary && matches!(mode, Inspection::UnavailablePrimary))
                {
                    ModelRouteAvailability::Unavailable
                } else {
                    ModelRouteAvailability::Available
                },
                model_id: Some(route.model_id.clone()),
                model_version: Some(if primary && matches!(mode, Inspection::DriftPrimary) {
                    id("changed-release")
                } else {
                    route.model_version.clone()
                }),
                deployment_revision: route.deployment_revision.clone(),
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id(&format!("inspection-{}", route.binding.id)),
            })
        })
    }
}

#[derive(Clone, Copy, Default)]
pub enum Projection {
    #[default]
    Valid,
    WrongRoute,
    WrongOptions,
    TooManyTokens,
    OldOpaque,
    DifferentContent,
    UsesTools,
    UsesJson,
}
#[derive(Default)]
pub struct Projector {
    pub mode: Mutex<Projection>,
    pub calls: Mutex<Vec<ResolvedModelRoute>>,
}
impl ModelRequestProjector for Projector {
    fn project<'a>(
        &'a self,
        selection: &'a RouteSelection,
        input: &'a RoutedModelInput,
        _: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ProjectedModelRequest> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(selection.route.clone());
            let mode = *self.mode.lock().unwrap();
            let mut request = ModelRequest {
                request_id: input.model_step_id.clone(),
                purpose: input.routing.purpose,
                route: selection.route.clone(),
                messages: vec![ModelMessage {
                    role: ModelRole::User,
                    content: vec![ModelContent::Text {
                        text: if matches!(mode, Projection::DifferentContent) {
                            "Changed required context"
                        } else {
                            "Preserved required context"
                        }
                        .into(),
                    }],
                }],
                tools: vec![],
                output: ModelOutput::Text {},
                max_output_tokens: input.routing.max_output_tokens,
                options: input.routing.options.clone(),
                limits: ModelResponseLimits {
                    max_input_bytes: 16384,
                    max_response_bytes: 4096,
                    max_delta_bytes: 1024,
                    max_events: 8,
                    max_tool_calls: 0,
                },
            };
            match mode {
                Projection::WrongRoute => {
                    request.route.connection_ref = reference("different-account")
                }
                Projection::WrongOptions => {
                    request.options.insert("effort".into(), json!("low"));
                }
                Projection::OldOpaque => {
                    let old = ResolvedModelRoute {
                        provider: id("old-provider"),
                        ..selection.route.clone()
                    };
                    request.messages.push(ModelMessage {
                        role: ModelRole::Assistant,
                        content: vec![ModelContent::Opaque {
                            continuation: OpaqueContinuation::new(
                                &old,
                                json!({"private_replay":"old"}),
                            ),
                        }],
                    });
                }
                Projection::UsesTools => {
                    request.tools = vec![ModelTool {
                        name: id("search"),
                        description: "Search records".into(),
                        model_input_schema: json!({"type":"object","properties":{},"required":[],"additionalProperties":false}),
                    }];
                }
                Projection::UsesJson => {
                    request.output = ModelOutput::JsonSchema {
                        schema: json!({"type":"object"}),
                    };
                }
                _ => {}
            }
            Ok(ProjectedModelRequest {
                request,
                input_tokens: if matches!(mode, Projection::TooManyTokens) {
                    4096
                } else {
                    100
                },
            })
        })
    }
}

pub struct Fixture {
    pub store: Arc<MemoryStateStore>,
    pub lease: RunLease,
    pub ids: Arc<Ids>,
    pub snapshot: RoutingSnapshot,
    pub router: PolicyModelRouter,
    pub first: Arc<Model>,
    pub second: Arc<Model>,
    pub policy: Arc<Policy>,
    pub inspector: Arc<Inspector>,
    pub projector: Projector,
}
impl Fixture {
    pub async fn new(first: Vec<Reply>, second: Vec<Reply>) -> Self {
        Self::with_limits(first, second, 8, 4).await
    }
    pub async fn with_limits(
        first: Vec<Reply>,
        second: Vec<Reply>,
        model_calls: u64,
        recoveries: u64,
    ) -> Self {
        let store = Arc::new(MemoryStateStore::new());
        let mut input =
            core::admission("run", "request", "session", "Preserved request", "1").await;
        let mut profile = serde_json::to_value(input.snapshot.profile.profile()).unwrap();
        profile["limits"]["max_model_calls"] = json!(model_calls);
        profile["limits"]["max_recovery_attempts"] = json!(recoveries);
        input.snapshot.profile = ProfileValidator::new(&core::Catalog { revision: "1" })
            .validate(
                &AgentProfile::from_json(&profile.to_string()).unwrap(),
                &scope(),
            )
            .await
            .unwrap();
        input.snapshot.limits = input.snapshot.profile.profile().limits.clone();
        input.snapshot.request.model_options = options();
        input.snapshot.request_digest =
            admission_digest(&input.snapshot.request, &input.snapshot.profile, None);
        let RunEventPayload::RunStarted {
            request_ref,
            profile_digest,
        } = &mut input.events[0].payload
        else {
            unreachable!()
        };
        *profile_digest = input.snapshot.profile.profile_digest().clone();
        let record = ProtectedRecord::new(
            request_ref.record_id.clone(),
            request_ref.revision,
            serde_json::to_value(&input.snapshot.request).unwrap(),
        );
        let previous = request_ref.clone();
        *request_ref = record.reference().clone();
        *input
            .records
            .iter_mut()
            .find(|record| record.reference() == &previous)
            .unwrap() = record;
        store.admit(&scope(), input).await.unwrap();
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("worker"), 0, 20_000)
            .await
            .unwrap();
        let snapshot = routing_snapshot();
        let first = Arc::new(Model::new(
            &snapshot.catalog().bindings[0],
            first,
            &snapshot,
        ));
        let second = Arc::new(Model::new(
            &snapshot.catalog().bindings[1],
            second,
            &snapshot,
        ));
        let router = PolicyModelRouter::new(snapshot.clone()).unwrap();
        Self {
            store,
            lease,
            ids: Arc::new(Ids::default()),
            snapshot,
            router,
            first,
            second,
            policy: Arc::new(Policy::default()),
            inspector: Arc::new(Inspector::default()),
            projector: Projector::default(),
        }
    }
    pub fn input(&self, step: &str) -> RoutedModelInput {
        RoutedModelInput {
            model_step_id: id(step),
            routing: RouteRequest {
                model_binding: id("primary"),
                purpose: ModelPurpose::Agent,
                required_capabilities: [id("text")].into_iter().collect(),
                input_tokens: 1,
                max_output_tokens: 64.try_into().unwrap(),
                options: options(),
                scope: scope(),
                allowed_bindings: vec![id("primary"), id("fallback")],
                version_policy: VersionPolicy::RequirePinned,
                previous_route: None,
                previous_failure: None,
            },
        }
    }
    pub fn enable_feature(&mut self, feature: &str) {
        let mut catalog = self.snapshot.catalog().clone();
        for model in &mut catalog.models {
            model.capabilities.features.insert(id(feature));
        }
        for binding in &mut catalog.bindings {
            binding.capabilities.features.insert(id(feature));
            let model = catalog
                .models
                .iter()
                .find(|model| model.reference() == binding.model)
                .unwrap();
            binding.evidence[0].binding_digest = binding.contract_digest(model).unwrap();
        }
        self.snapshot = RoutingSnapshot::new(catalog, self.snapshot.policy().clone()).unwrap();
        self.router = PolicyModelRouter::new(self.snapshot.clone()).unwrap();
    }
    pub fn context(&self) -> ExecutionContext {
        ExecutionContext::new(
            ExecutionContextData {
                scope: scope(),
                principal_ref: id("actor"),
                capability_grant_ref: id("grant"),
                trace_context: None,
                system_inputs: None,
            },
            CancellationToken::new(),
        )
    }
    pub async fn budget(&self) -> RunBudget {
        RunBudget::attach(
            self.store.clone(),
            Arc::new(FixedClock),
            self.ids.clone(),
            scope(),
            id("run"),
            self.lease.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap()
    }
    pub fn exchange(&self, retries: u32) -> ModelExchange {
        let dispatcher = RegistryModelDispatcher::new(vec![
            ModelDispatcherEntry {
                scope: scope(),
                port: self.first.clone(),
            },
            ModelDispatcherEntry {
                scope: scope(),
                port: self.second.clone(),
            },
        ])
        .unwrap();
        ModelExchange::with_dispatcher(
            Arc::new(dispatcher),
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap()),
        )
        .with_route_inspector(self.inspector.clone(), Duration::from_secs(1))
        .unwrap()
        .with_retry_policy(ModelRetryPolicy {
            max_retries: retries,
            backoff_ms: 0,
        })
    }
    pub async fn saved(&self) -> RunSnapshot {
        self.store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
    }
    pub fn call_counts(&self) -> (usize, usize) {
        (
            self.first.calls.lock().unwrap().len(),
            self.second.calls.lock().unwrap().len(),
        )
    }
}

pub fn completed(outcome: Guarded<ModelExchangeOutcome>) -> ModelResponse {
    let Guarded::Completed(ModelExchangeOutcome::Completed { response }) = outcome else {
        panic!("expected complete response")
    };
    response
}

pub async fn seed_tool(fixture: &Fixture, state: ToolCallState) {
    let saved = fixture.saved().await;
    let call = ToolCall {
        call_id: id("unsettled-call"),
        model_request_id: id("earlier-attempt"),
        provider_call_id: id("earlier-call"),
        tool_name: id("tool"),
        model_inputs: JsonObject::new(),
        descriptor_digest: canonical_digest(&json!("descriptor")),
        bound_input_ref: None,
    };
    let mut change = core::prepared(&saved, fixture.lease.clone(), 0);
    change
        .snapshot
        .tool_ledger
        .push(ToolLedgerEntry { call, state });
    fixture
        .store
        .commit(&scope(), &id("run"), change)
        .await
        .unwrap();
}

pub fn unknown_tool_result() -> ToolCallState {
    ToolCallState::Settled {
        result: ToolResult {
            call_id: id("unsettled-call"),
            call_message_id: id("earlier-message"),
            status: ToolResultStatus::Unknown,
            content: vec![],
            effect_receipt_ref: None,
            error: None,
        },
    }
}
```

## `crates/wickle/src/error.rs`

```rust
use serde::{Deserialize, Serialize};

/// Stable categories for contract and profile validation failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorCode {
    /// Current policy or exact owner scope denies access.
    AccessDenied,
    /// The trusted policy failed or panicked; no permission was granted.
    PolicyUnavailable,
    /// The call's finite deadline elapsed.
    DeadlineExceeded,
    /// The current operation was cancelled.
    Cancelled,
    /// The Host has not supplied the required asynchronous runtime.
    RuntimeUnavailable,
    /// A configured call, repair, or recovery budget has no remaining capacity.
    BudgetExceeded,
    /// A required time reading or timer could not be obtained.
    ClockUnavailable,
    /// A monotonic reading regressed or a resumed UTC clock predates saved progress.
    ClockRegression,
    /// The Host identifier source could not generate an internal execution identifier.
    IdGenerationFailed,
    /// Input is not unambiguous, finite JSON.
    InvalidJson,
    /// Input does not match a data contract.
    InvalidContract,
    /// The requested model, version, binding or alias is not registered.
    ModelNotRegistered,
    /// An exact model/API/adapter/target binding or its evidence is inconsistent.
    ModelBindingInvalid,
    /// The required feature or declared capability limit is unsupported.
    ModelCapabilityUnsupported,
    /// Provider options do not satisfy the model and exact-binding contracts.
    ModelOptionUnsupported,
    /// The model or deployment is not verified immutable under the requested policy.
    ModelVersionUnpinned,
    /// Current observed model or deployment metadata differs from the pinned route.
    ModelVersionDrift,
    /// A bounded target inspection failed or could not establish availability.
    ModelInspectionUnavailable,
    /// A prior physical attempt has no known result and needs explicit recovery.
    ModelAttemptUnresolved,
    /// The selected release is retired or otherwise unavailable.
    ModelUnavailable,
    /// Catalog scope-independent revision, identity or serialized integrity differs.
    ModelCatalogMismatch,
    /// Static routing configuration has duplicate, missing or unsupported constraints.
    ModelRoutingInvalid,
    /// Saved routing metadata, request identity or selected route differs.
    ModelRoutingMismatch,
    /// Static routing constraints forbid this target or fallback reason.
    ModelRouteDenied,
    /// No eligible candidate remains in the finite permitted fallback list.
    ModelRoutesExhausted,
    /// Required support evidence is absent, failed, or of an insufficient kind.
    ModelSupportInsufficient,
    /// Estimated input plus reserved output exceeds this model/binding context budget.
    ModelContextIncompatible,
    /// Tool exposure, binding metadata, or a registered input schema is inconsistent.
    InvalidToolInputContract,
    /// The compiler cannot safely project this input schema or reference form.
    UnsupportedInputProjection,
    /// Model-owned or assembled tool arguments do not satisfy their input contract.
    InvalidArguments,
    /// A supplied system value does not satisfy its registered input contract.
    SystemInputInvalid,
    /// A required registered system value is absent; the model must not invent it.
    SystemInputMissing,
    /// A read-only system-value resolver is unavailable or failed safely.
    SystemInputUnavailable,
    /// Supplied/resumed values or pinned input metadata differ from the saved snapshot.
    SystemInputsMismatch,
    /// Lookup permission requires separate Host approval before a target is known.
    SystemInputApprovalRequired,
    /// Resolver-count or serialized input-size bounds were exceeded.
    InputBindingLimitExceeded,
    /// Context identity, provenance structure, or call/result protocol is invalid.
    InvalidContext,
    /// Context scope, pinned assets, or protected-record identity does not match.
    ContextMismatch,
    /// Required context cannot fit the explicit byte or item bounds without truncation.
    ContextBudgetExceeded,
    /// The document format is not supported.
    UnsupportedSchemaVersion,
    /// A reference or binding is missing or inconsistent.
    InvalidReference,
    /// A required component or exact version is unavailable.
    ComponentUnavailable,
    /// A component uses an unsupported metadata contract.
    UnsupportedContractVersion,
    /// Selected components do not supply a required capability.
    CapabilityUnsupported,
    /// A configuration does not satisfy its registered schema.
    InvalidConfiguration,
    /// A registered schema is invalid or requires unsupported resolution.
    InvalidSchema,
    /// A profile differs from the profile pinned to an existing execution.
    ProfileMismatch,
    /// Stored data violates checkpoint invariants.
    InvalidSnapshot,
    /// The requested run, session, or protected record is absent in this exact scope.
    StateNotFound,
    /// An existing request identity was reused with different logical input.
    RequestConflict,
    /// The session already has a running or waiting run.
    SessionBusy,
    /// A proposed run identifier already belongs to another request in this scope.
    RunConflict,
    /// The compare-and-swap revision no longer matches saved state.
    RevisionConflict,
    /// Another unexpired execution lease already owns the run.
    LeaseBusy,
    /// The execution lease expired or no longer matches its owner and generation.
    LeaseLost,
    /// A candidate change violates immutable data or state-transition rules.
    InvalidTransition,
    /// An event has a duplicate identity, invalid sequence, or inconsistent references.
    InvalidEvent,
    /// A message has a duplicate identity, invalid sequence, or wrong owning run.
    InvalidMessage,
    /// Immutable record content or a requested reference digest conflicts.
    RecordConflict,
    /// Authoritative storage is unavailable; no successful commit is implied.
    PersistenceUnavailable,
}

/// A validation error that does not retain submitted values or credentials.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code:?} at {path}")]
pub struct ContractError {
    /// Machine-readable failure category.
    pub code: ErrorCode,
    /// Contract field or reference location, without submitted values.
    pub path: String,
}

impl ContractError {
    /// Construct an error using a safe contract location.
    pub fn new(code: ErrorCode, path: impl Into<String>) -> Self {
        Self {
            code,
            path: path.into(),
        }
    }
}
```

## `crates/wickle/src/lib.rs`

```rust
//! Wickle, an agent engine for Rust applications.
//!
//! Profiles, scoped metadata resolution, and versioned execution data contracts.
//! Model calls use scoped ports and persisted attempt accounting. Tool dispatch
//! and the agent driver are not implemented yet.
//!
//! Runtime objects stay in Host code. Only documented data contracts are
//! serialized; successful decoding does not authenticate a caller.
//!
//! Internal modules are not extension points; use the root exports.
//! ```compile_fail
//! use wickle::serialization::canonical_digest;
//! ```

mod budget;
mod clock;
mod context;
mod context_projection;
mod error;
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
mod tool_schema;
mod views;

pub use budget::{AttemptReservation, ReservationKind, RunBudget, RunTiming};
pub use clock::{Clock, ClockReading, IdSource, RandomIdSource, SystemClock};
pub use context_projection::{
    CONTEXT_ASSEMBLER_VERSION, ContextAssembler, ContextItem, ContextLifetime, ContextOrigin,
    ContextPriority, ContextProjection, InstructionAssetContent, PinnedPromptTool, ProjectionInput,
    ProjectionLimits, PromptSnapshot, PromptToolBinding, ScopedOpaque, SkillManifest,
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
    PolicyPort, PolicyRequest, ToolPolicyInput,
};
pub use state::{
    AdmissionInput, AdmissionResult, CommitInput, EventPage, MAX_EVENT_PAGE_SIZE, MemoryStateStore,
    ProtectedRecord, RunLease, STATE_STORE_CHECKPOINT_VERSION, StateStore, StateStoreCapabilities,
    StateStoreCheckpoint, StoredRun,
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
    ResumeCommand, RunEvent, RunEventPayload, RunEventSchemaVersion, RunOutcome, RunPhase,
    RunRequest, RunSnapshot, RunSnapshotSchemaVersion, RunStatus, RunTrigger, SessionSchemaVersion,
    SessionSnapshot, SourceExecutionState, SystemInputSnapshotRef, ToolCallState, ToolLedgerEntry,
    VerificationSummary, VerificationVerdict, WaitState, WaitTarget, admission_digest,
};
pub use serialization::{
    Id, JsonDigest, JsonObject, canonical_digest, canonical_digest_json, parse_json,
};
```

## `crates/wickle/src/model.rs`

```rust
use crate::{
    Id, JsonDigest, JsonObject, RecordRef, Scope, VersionedRef,
    serialization::{data_digest, optional},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fmt, num::NonZeroU64};

/// Logical purpose of a model call; all purposes consume the run's model budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelPurpose {
    /// Agent reasoning.
    Agent,
    /// Candidate verification.
    Verification,
    /// Context compression.
    Compaction,
}

/// Version semantics declared by trusted metadata, never inferred from a name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionSemantics {
    /// Immutable model and target.
    Pinned,
    /// Alias that can point to another release.
    Alias,
    /// Deployment that can change independently of its name.
    MutableDeployment,
    /// Immutability has not been verified.
    Unverified,
}

/// Host routing constraint on mutable model targets.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionPolicy {
    /// Only verified immutable targets are eligible.
    #[default]
    RequirePinned,
    /// The Host explicitly permits mutable targets.
    AllowMutable,
}

/// Provider protocol identity, separate from model release and deployment names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiContract {
    /// Protocol operation, such as messages or generateContent.
    pub operation: Id,
    /// Exact API contract/header version.
    pub version: Id,
}

/// Immutable selection data for one model target. Provider keys are extensible.
/// Provider adapters validate target fields; credentials live in Host bindings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedModelRoute {
    /// Exact Host binding revision.
    pub binding: VersionedRef,
    /// Catalog snapshot revision.
    pub catalog_revision: Id,
    /// Routing policy snapshot revision.
    pub routing_policy_revision: Id,
    /// Original model or alias requested by routing policy.
    pub requested_model: Id,
    /// Resolved provider model identifier.
    pub model_id: Id,
    /// Exact opaque release/version string.
    pub model_version: Id,
    /// Declared version semantics.
    pub version_semantics: VersionSemantics,
    /// Registered service key, not a closed list of vendors.
    pub provider: Id,
    /// Nonsecret target metadata validated by the selected adapter.
    pub target: JsonObject,
    /// Optional independent deployment revision.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub deployment_revision: Option<Id>,
    /// API operation and version.
    pub api_contract: ApiContract,
    /// Exact adapter implementation version.
    pub adapter: VersionedRef,
    /// Revision of validated capabilities for this exact combination.
    pub capability_revision: Id,
    /// Host connection reference/revision; never a raw credential.
    pub connection_ref: VersionedRef,
}

impl ResolvedModelRoute {
    /// Identity of every selected route field; computed to avoid stale stored hashes.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }
}

/// A classified model failure, before any retry/fallback policy is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelFailureKind {
    /// Provider did not respond within the call deadline.
    Timeout,
    /// Provider throttled the call.
    RateLimited,
    /// Transport failed.
    Transport,
    /// Response could not be interpreted safely.
    Protocol,
    /// Input exceeded model context limits.
    ContextOverflow,
    /// Authentication failed.
    Authentication,
    /// Required functionality is unsupported.
    Unsupported,
    /// The selected target is no longer available.
    Unavailable,
    /// Current model/deployment metadata differs from the pinned route.
    VersionDrift,
}

/// Selection request; the router returns data and does not invoke a model.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteRequest {
    /// Profile's Host binding/routing configuration name.
    pub model_binding: Id,
    /// Purpose of this call.
    pub purpose: ModelPurpose,
    /// Features required by the projected input.
    pub required_capabilities: BTreeSet<Id>,
    /// Estimated input tokens, distinct from reported usage.
    pub input_tokens: u64,
    /// Reserved output tokens.
    pub max_output_tokens: NonZeroU64,
    /// Host-owned logical options that each candidate's catalog schemas must accept.
    /// No provider wire format or reasoning-effort vocabulary is implied by these keys.
    #[serde(default, skip_serializing_if = "JsonObject::is_empty")]
    pub options: JsonObject,
    /// Authenticated routing/data scope.
    pub scope: Scope,
    /// Explicitly allowed binding names.
    pub allowed_bindings: Vec<Id>,
    /// Default is require_pinned.
    #[serde(default)]
    pub version_policy: VersionPolicy,
    /// Prior choice, if evaluating an explicit fallback.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub previous_route: Option<ResolvedModelRoute>,
    /// Classified reason for considering another route.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub previous_failure: Option<ModelFailureKind>,
}

impl fmt::Debug for RouteRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RouteRequest")
            .field("model_binding", &self.model_binding)
            .field("purpose", &self.purpose)
            .field("required_capabilities", &self.required_capabilities)
            .field("input_tokens", &self.input_tokens)
            .field("max_output_tokens", &self.max_output_tokens)
            .field("option_count", &self.options.len())
            .field("version_policy", &self.version_policy)
            .finish_non_exhaustive()
    }
}

/// Whether token counts were measured by the provider or estimated by the Host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageMeasurement {
    /// Reported by the provider.
    Reported,
    /// Estimated locally.
    Estimated,
}

/// Model token usage. Missing counts remain unknown, not zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelUsage {
    /// Provenance of the counts.
    pub measurement: UsageMeasurement,
    /// Input tokens when known.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub input_tokens: Option<u64>,
    /// Output tokens when known.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub output_tokens: Option<u64>,
}

/// Reservation/result state of one physical model attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelAttemptState {
    /// Budget was reserved before dispatch.
    Reserved {},
    /// A complete response was recorded.
    Completed {},
    /// A classified failure was recorded.
    Failed {
        /// Failure classification, without raw request/response data.
        kind: ModelFailureKind,
    },
    /// Dispatch/result is not yet known after interruption.
    Unknown {},
}

/// Durable record of one physical invocation and the selected model version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelInvocationRecord {
    /// Owning run.
    pub run_id: Id,
    /// Logical model step, stable across transport retries.
    pub model_step_id: Id,
    /// Unique physical attempt.
    pub attempt_id: Id,
    /// Purpose charged to the same run budget.
    pub purpose: ModelPurpose,
    /// Selected route, including all relevant versions.
    pub route: ResolvedModelRoute,
    /// Host-defined selection reason code.
    pub selection_reason: Id,
    /// Identity of the projected model request.
    pub request_digest: JsonDigest,
    /// Invocation state.
    pub state: ModelAttemptState,
    /// Protected current-target inspection, distinct from provider response metadata.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub inspection_ref: Option<RecordRef>,
    /// Protected complete or failed response, including bounded partial text.
    /// This is retained even when a later recovery reservation is exhausted.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub response_ref: Option<RecordRef>,
    /// Provider correlation identifier, when reported.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub provider_request_id: Option<Id>,
    /// Model actually reported by the response; never filled from the requested ID.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub reported_model_id: Option<Id>,
    /// Version actually reported by the response.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub reported_model_version: Option<Id>,
    /// Missing usage is unknown.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub usage: Option<ModelUsage>,
}
```

## `crates/wickle/src/model_dispatch.rs`

```rust
use std::{fmt, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    ContractError, ErrorCode, Id, JsonDigest, ModelPort, PortFuture, ResolvedModelRoute, Scope,
    VersionPolicy, VersionSemantics, serialization::optional,
};

/// Resolve an already-selected route to an existing, scope-bound model adapter.
/// This is a synchronous registry lookup: it must not perform I/O, create clients,
/// choose fallback targets, or invoke a model. The caller rechecks the returned
/// port's binding before dispatch and retains ownership of policy and budgets.
pub trait ModelDispatcher: Send + Sync {
    /// Return the adapter for this exact provider, implementation and connection.
    fn resolve(
        &self,
        scope: &Scope,
        route: &ResolvedModelRoute,
    ) -> Result<Arc<dyn ModelPort>, ContractError>;
}

/// Current authenticated actor and finite bounds for a metadata lookup.
/// No prompt, system-input map, or credential payload is exposed here.
#[derive(Debug, Clone)]
pub struct ModelInspectionContext {
    /// Authenticated scope in which the selected target may be inspected.
    pub scope: Scope,
    /// Current principal, independent of the original routing configuration.
    pub principal_ref: Id,
    /// Current Host grant reference; the inspector's backend enforces its scope.
    pub capability_grant_ref: Id,
    /// Cooperative signal that the caller cancels when the inspection ends.
    pub cancellation: CancellationToken,
    /// Effective deadline for this metadata lookup.
    pub deadline: tokio::time::Instant,
}

/// Availability reported by a trusted metadata inspection, not a model response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelRouteAvailability {
    /// The target was reported available when inspected.
    Available,
    /// The target was reported unavailable; no substitute is selected here.
    Unavailable,
    /// Availability could not be established and must not be treated as available.
    Unknown,
}

/// Facts observed by the Host's read-only metadata inspector. These facts never
/// populate provider-reported response fields or prove a later request's result.
/// The Host authenticates the evidence; validation checks its declared consistency.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRouteObservation {
    /// Exact selected route that the inspector checked.
    pub route_digest: JsonDigest,
    /// Availability at inspection time.
    pub availability: ModelRouteAvailability,
    /// Model identifier actually established by inspection, when known.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub model_id: Option<Id>,
    /// Release actually established by inspection, when known.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub model_version: Option<Id>,
    /// Deployment revision actually established by inspection, when known.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub deployment_revision: Option<Id>,
    /// Observed version guarantee; never inferred from identifier spelling.
    pub version_semantics: VersionSemantics,
    /// Host-owned evidence reference; raw service payloads remain outside this DTO.
    pub evidence_ref: Id,
}

impl fmt::Debug for ModelRouteObservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelRouteObservation")
            .field("route_digest", &self.route_digest)
            .field("availability", &self.availability)
            .field("version_semantics", &self.version_semantics)
            .finish_non_exhaustive()
    }
}

impl ModelRouteObservation {
    /// Check availability, exact target identity and the requested version guarantee.
    /// Unknown facts remain unknown. An observation cannot silently replace the
    /// selected route, upgrade its declared semantics, or guarantee no later drift.
    pub fn validate(
        &self,
        route: &ResolvedModelRoute,
        version_policy: VersionPolicy,
    ) -> Result<(), ContractError> {
        if self.route_digest != route.digest()
            || self
                .model_id
                .as_ref()
                .is_some_and(|id| id != &route.model_id)
            || self
                .model_version
                .as_ref()
                .is_some_and(|version| version != &route.model_version)
            || self
                .deployment_revision
                .as_ref()
                .zip(route.deployment_revision.as_ref())
                .is_some_and(|(actual, expected)| actual != expected)
        {
            return Err(ContractError::new(
                ErrorCode::ModelVersionDrift,
                "model.inspection.target",
            ));
        }
        match self.availability {
            ModelRouteAvailability::Unavailable => {
                return Err(ContractError::new(
                    ErrorCode::ModelUnavailable,
                    "model.inspection.availability",
                ));
            }
            ModelRouteAvailability::Unknown => {
                return Err(ContractError::new(
                    ErrorCode::ModelInspectionUnavailable,
                    "model.inspection.availability",
                ));
            }
            ModelRouteAvailability::Available => {}
        }
        if version_policy == VersionPolicy::RequirePinned
            && (route.version_semantics != VersionSemantics::Pinned
                || self.version_semantics != VersionSemantics::Pinned
                || self.model_id.is_none()
                || self.model_version.is_none()
                || (route.deployment_revision.is_some()
                    && self.deployment_revision != route.deployment_revision))
        {
            return Err(ContractError::new(
                ErrorCode::ModelVersionUnpinned,
                "model.inspection.version",
            ));
        }
        Ok(())
    }
}

/// Trusted read-only provider/deployment metadata lookup. It must not run model
/// inference, rewrite routes, or hide retry loops. The caller authorizes lookup,
/// bounds time/cancellation, validates the result and records its evidence.
pub trait ModelRouteInspector: Send + Sync {
    /// Inspect only the supplied route under the current authenticated context.
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        context: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation>;
}
```

## `crates/wickle/src/model_execution.rs`

```rust
use std::{panic::AssertUnwindSafe, sync::Arc};

use futures_util::FutureExt;

mod routed;
pub use routed::{
    ModelProjectionContext, ModelRequestProjector, ProjectedModelRequest, RoutedModelInput,
};

use crate::{
    CommitInput, ContractError, ErrorCode, ExecutionContext, Guarded, Id, ModelAttemptState,
    ModelCallContext, ModelFailureKind, ModelInvocationRecord, ModelPort, ModelProtocolError,
    ModelRequest, ModelResponse, ModelResponseMetadata, PolicyAction, PolicyGate, PolicyRequest,
    ProtectedRecord, ReservationKind, RunBudget, RunEvent, RunEventPayload, RunEventSchemaVersion,
    RunPhase, collect_model_response,
};

/// Explicit, bounded retries within the same already selected route. Router
/// fallback and context reduction are separate operations owned by the driver.
#[derive(Debug, Clone, Copy, Default)]
pub struct ModelRetryPolicy {
    /// Additional physical requests after the first; zero disables retries.
    pub max_retries: u32,
    /// Backoff on the run's injected clock, bounded by its original deadline.
    pub backoff_ms: u64,
}

/// Complete response or a classified failure with bounded partial text. Failed
/// attempts never publish a partially assembled tool plan through this value.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelExchangeOutcome {
    /// The full stream ended with a valid completion.
    Completed {
        /// Unexecuted proposals; the driver must persist and validate tool plans.
        response: ModelResponse,
    },
    /// No complete response was accepted after the configured recovery allowance.
    Failed {
        /// Safe classification, optional reported usage, and bounded partial text.
        failure: ModelProtocolError,
    },
}

/// Protected response body tied to one physical attempt and exact route. Reading
/// it requires Store authorization; it is never an automatic public run view.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredModelResponse {
    /// The physical model request/attempt identifier.
    pub request_id: Id,
    /// Route that produced the success or failure.
    pub route_digest: crate::JsonDigest,
    /// Accepted response or bounded failed-response data.
    pub outcome: ModelExchangeOutcome,
}

/// Core-owned model execution boundary. Each physical attempt is visible in the
/// budget and invocation ledger; an adapter still performs exactly one request.
/// This does not run tools, choose fallback routes, or advance an agent loop.
pub struct ModelExchange {
    models: Models,
    policy: Arc<PolicyGate>,
    retry: ModelRetryPolicy,
    inspector: Option<(Arc<dyn crate::ModelRouteInspector>, std::time::Duration)>,
}

enum Models {
    Single(Arc<dyn ModelPort>),
    Dispatcher(Arc<dyn crate::ModelDispatcher>),
}

impl ModelExchange {
    /// Bind one adapter and current policy gate, with physical retries disabled.
    pub fn new(model: Arc<dyn ModelPort>, policy: Arc<PolicyGate>) -> Self {
        Self {
            models: Models::Single(model),
            policy,
            retry: ModelRetryPolicy::default(),
            inspector: None,
        }
    }

    /// Use an exact scoped adapter registry without inventing a shared credential binding.
    pub fn with_dispatcher(
        dispatcher: Arc<dyn crate::ModelDispatcher>,
        policy: Arc<PolicyGate>,
    ) -> Self {
        Self {
            models: Models::Dispatcher(dispatcher),
            policy,
            retry: ModelRetryPolicy::default(),
            inspector: None,
        }
    }

    /// Require bounded current metadata inspection for routed calls and their retries.
    pub fn with_route_inspector(
        mut self,
        inspector: Arc<dyn crate::ModelRouteInspector>,
        timeout: std::time::Duration,
    ) -> Result<Self, ContractError> {
        if timeout.is_zero() {
            return Err(ContractError::new(
                ErrorCode::InvalidConfiguration,
                "model.inspection_timeout",
            ));
        }
        self.inspector = Some((inspector, timeout));
        Ok(self)
    }

    /// Configure finite same-route recovery. Run model/recovery limits still apply.
    pub fn with_retry_policy(mut self, retry: ModelRetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Validate and authorize each attempt, persist its reservation and selected
    /// route, then collect exactly one adapter stream. A retry uses a new physical
    /// request/attempt ID while retaining the caller's logical request ID as the
    /// model step. The driver separately stores the accepted response/transcript
    /// and tool plans before dispatching any proposed tool. Complete and failed
    /// responses are retained under each invocation's protected response_ref, so
    /// a later budget/cancellation error does not discard prior partial output.
    pub async fn generate(
        &self,
        request: &ModelRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<Guarded<ModelExchangeOutcome>, ContractError> {
        if budget
            .store()
            .load(budget.scope(), budget.run_id())
            .await?
            .snapshot
            .routing_snapshot_ref
            .is_some()
        {
            return Err(ContractError::new(
                ErrorCode::ModelRoutingMismatch,
                "model.routing_required",
            ));
        }
        self.generate_inner(request, context, budget, None).await
    }

    async fn generate_inner(
        &self,
        request: &ModelRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
        routed: Option<(&crate::RouteSelection, crate::VersionPolicy)>,
    ) -> Result<Guarded<ModelExchangeOutcome>, ContractError> {
        for retry_number in 0..=self.retry.max_retries {
            let model = self.resolve_model(request, context, budget)?;
            budget.check_boundary().await?;
            if let Guarded::ApprovalRequired(challenge) =
                self.authorize(request, context, budget).await?
            {
                return Ok(Guarded::ApprovalRequired(challenge));
            }
            let observation = if let Some((_, version_policy)) = routed {
                Some(
                    self.inspect_route(request, version_policy, context, budget)
                        .await?,
                )
            } else {
                None
            };
            let reservation = budget
                .reserve(ReservationKind::Model {
                    purpose: request.purpose,
                })
                .await?;
            let mut physical_request = request.clone();
            physical_request.request_id = reservation.attempt_id.clone();
            let inspection_record = observation
                .map(|observation| {
                    Ok::<_, ContractError>(ProtectedRecord::new(
                        Id::new(format!("model-inspection-{}", reservation.attempt_id))?,
                        1,
                        serde_json::to_value(observation).map_err(|_| revision_error())?,
                    ))
                })
                .transpose()?;
            let invocation = ModelInvocationRecord {
                run_id: budget.run_id().clone(),
                model_step_id: request.request_id.clone(),
                attempt_id: reservation.attempt_id.clone(),
                purpose: request.purpose,
                route: request.route.clone(),
                selection_reason: Id::new(if retry_number != 0 {
                    "same_route_retry"
                } else if let Some((selection, _)) = routed {
                    routed::reason_code(selection.reason)
                } else {
                    "requested_route"
                })?,
                request_digest: physical_request.digest(),
                state: ModelAttemptState::Reserved {},
                inspection_ref: inspection_record
                    .as_ref()
                    .map(|record| record.reference().clone()),
                response_ref: None,
                provider_request_id: None,
                reported_model_id: None,
                reported_model_version: None,
                usage: None,
            };
            self.record_start(budget, invocation, inspection_record)
                .await?;
            // Neither a saved reservation nor earlier authorization grants lasting
            // permission. Check the physical request and current policy again.
            self.validate(&physical_request, context, budget, model.as_ref())?;
            if let Guarded::ApprovalRequired(challenge) =
                self.authorize(&physical_request, context, budget).await?
            {
                return Ok(Guarded::ApprovalRequired(challenge));
            }
            budget.check_boundary().await?;
            if context.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let cancellation = budget.cancellation().child_token();
            let _cancel_adapter_on_drop = cancellation.clone().drop_guard();
            let call_context = ModelCallContext {
                attempt_id: reservation.attempt_id.clone(),
                run_id: budget.run_id().clone(),
                scope: context.data.scope.clone(),
                cancellation,
                deadline: budget.call_deadline()?,
            };
            // Construct the stream only after all entry checks. The adapter's
            // lifetime ends with this attempt; it must not spawn untracked retries.
            let attempt = AssertUnwindSafe(async {
                collect_model_response(
                    &physical_request,
                    model.generate(&physical_request, &call_context),
                )
                .await
            })
            .catch_unwind();
            let result = tokio::select! {
                biased;
                _ = context.cancellation.cancelled() => Err(cancelled()),
                stopped = budget.wait_for_cancellation_or_deadline() => {
                    match stopped {
                        Err(error) => Err(error),
                        Ok(()) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "model.deadline")),
                    }
                }
                response = attempt => response.map_err(|_| ContractError::new(ErrorCode::InvalidContract, "model.adapter")),
            };
            // Signal adapter shutdown before any potentially slow settlement I/O.
            call_context.cancellation.cancel();
            let response = match result {
                Ok(response) => response,
                Err(error) => {
                    self.record_end(
                        budget,
                        &reservation.attempt_id,
                        ModelAttemptState::Unknown {},
                        &ModelResponseMetadata::default(),
                        None,
                    )
                    .await?;
                    return Err(error);
                }
            };
            let (state, metadata) = match &response {
                Ok(response) => (ModelAttemptState::Completed {}, &response.metadata),
                Err(failure) => (
                    ModelAttemptState::Failed { kind: failure.kind },
                    failure.metadata.as_ref(),
                ),
            };
            let stored_response = StoredModelResponse {
                request_id: reservation.attempt_id.clone(),
                route_digest: physical_request.route.digest(),
                outcome: match &response {
                    Ok(response) => ModelExchangeOutcome::Completed {
                        response: response.clone(),
                    },
                    Err(failure) => ModelExchangeOutcome::Failed {
                        failure: failure.clone(),
                    },
                },
            };
            let response_record = ProtectedRecord::new(
                Id::new(format!("model-response-{}", reservation.attempt_id))?,
                1,
                serde_json::to_value(&stored_response).map_err(|_| revision_error())?,
            );
            self.record_end(
                budget,
                &reservation.attempt_id,
                state,
                metadata,
                Some(response_record),
            )
            .await?;
            if context.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            budget.check_boundary().await?;
            match response {
                Ok(response) => {
                    return Ok(Guarded::Completed(ModelExchangeOutcome::Completed {
                        response,
                    }));
                }
                Err(failure) => {
                    if retry_number == self.retry.max_retries || !recoverable(failure.kind) {
                        return Ok(Guarded::Completed(ModelExchangeOutcome::Failed { failure }));
                    }
                    budget.reserve(ReservationKind::Recovery {}).await?;
                    tokio::select! {
                        biased;
                        _ = context.cancellation.cancelled() => return Err(cancelled()),
                        result = budget.backoff(self.retry.backoff_ms) => result?,
                    }
                }
            }
        }
        unreachable!("a finite attempt loop always returns its last result")
    }

    fn validate(
        &self,
        request: &ModelRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
        model: &dyn ModelPort,
    ) -> Result<(), ContractError> {
        if &context.data.scope != budget.scope() {
            return Err(ContractError::new(ErrorCode::AccessDenied, "scope"));
        }
        request.validate()?;
        if !model.binding().matches_route(&request.route) {
            return Err(ContractError::new(
                ErrorCode::InvalidReference,
                "model.binding",
            ));
        }
        Ok(())
    }

    fn resolve_model(
        &self,
        request: &ModelRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<Arc<dyn ModelPort>, ContractError> {
        if &context.data.scope != budget.scope() {
            return Err(ContractError::new(ErrorCode::AccessDenied, "scope"));
        }
        let model = std::panic::catch_unwind(AssertUnwindSafe(|| match &self.models {
            Models::Single(model) => Ok(model.clone()),
            Models::Dispatcher(dispatcher) => dispatcher.resolve(budget.scope(), &request.route),
        }))
        .map_err(|_| ContractError::new(ErrorCode::ComponentUnavailable, "model.dispatcher"))??;
        self.validate(request, context, budget, model.as_ref())?;
        Ok(model)
    }

    async fn authorize(
        &self,
        request: &ModelRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<Guarded<()>, ContractError> {
        let policy_request = PolicyRequest {
            owner_scope: budget.scope().clone(),
            resource_id: budget.run_id().clone(),
            action: PolicyAction::InvokeModel {
                route: Box::new(request.route.clone()),
                purpose: request.purpose,
            },
        };
        tokio::select! {
            biased;
            stopped = budget.wait_for_cancellation_or_deadline() => match stopped {
                Err(error) => Err(error),
                Ok(()) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "model.policy")),
            },
            decision = self.policy.guard(&policy_request, context, Some(budget.call_deadline()?), None, || async { Ok(()) }) => decision,
        }
    }

    async fn record_start(
        &self,
        budget: &RunBudget,
        invocation: ModelInvocationRecord,
        inspection: Option<ProtectedRecord>,
    ) -> Result<(), ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(revision_error)?;
        snapshot.last_event_seq = snapshot
            .last_event_seq
            .checked_add(1)
            .ok_or_else(revision_error)?;
        snapshot.phase = RunPhase::Model;
        snapshot.model_step_id = Some(invocation.model_step_id.clone());
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        let record = ProtectedRecord::new(
            Id::new(format!("model-invocation-{}", invocation.attempt_id))?,
            1,
            serde_json::to_value(&invocation).map_err(|_| revision_error())?,
        );
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: Id::new(format!("model-route-{}", invocation.attempt_id))?,
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            session_id: snapshot.request.session_id.clone(),
            seq: snapshot
                .last_event_seq
                .try_into()
                .map_err(|_| revision_error())?,
            timestamp_ms: now,
            payload: RunEventPayload::ModelRouteSelected {
                invocation_ref: record.reference().clone(),
                route_digest: invocation.route.digest(),
            },
        };
        snapshot.model_ledger.push(invocation);
        let mut records = vec![record];
        records.extend(inspection);
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
                    messages: vec![],
                    events: vec![event],
                    records,
                },
            )
            .await?;
        Ok(())
    }

    async fn record_end(
        &self,
        budget: &RunBudget,
        attempt_id: &Id,
        state: ModelAttemptState,
        metadata: &ModelResponseMetadata,
        response_record: Option<ProtectedRecord>,
    ) -> Result<(), ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let invocation = snapshot
            .model_ledger
            .iter_mut()
            .find(|entry| &entry.attempt_id == attempt_id)
            .ok_or_else(|| ContractError::new(ErrorCode::InvalidSnapshot, "model.attempt"))?;
        invocation.state = state;
        invocation.response_ref = response_record
            .as_ref()
            .map(|record| record.reference().clone());
        invocation.provider_request_id = metadata.provider_request_id.clone();
        invocation.reported_model_id = metadata.reported_model_id.clone();
        invocation.reported_model_version = metadata.reported_model_version.clone();
        invocation.usage = metadata.usage.clone();
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(revision_error)?;
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
                    messages: vec![],
                    events: vec![],
                    records: response_record.into_iter().collect(),
                },
            )
            .await?;
        Ok(())
    }
}

fn cancelled() -> ContractError {
    ContractError::new(ErrorCode::Cancelled, "model")
}
fn revision_error() -> ContractError {
    ContractError::new(ErrorCode::RevisionConflict, "model.ledger")
}
fn recoverable(kind: ModelFailureKind) -> bool {
    matches!(
        kind,
        ModelFailureKind::Timeout
            | ModelFailureKind::RateLimited
            | ModelFailureKind::Transport
            | ModelFailureKind::Protocol
    )
}
```

## `crates/wickle/src/model_execution/routed.rs`

```rust
use super::*;
use crate::{
    ModelInspectionContext, ModelPurpose, ModelRouter, PortFuture, ResolvedModelRoute,
    RouteRequest, RouteSelection, RouteSelectionReason, RoutingSnapshot, ToolCallState,
    VersionPolicy,
};
use tokio_util::sync::CancellationToken;

/// One logical model step. Its physical retries receive separate attempt IDs.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RoutedModelInput {
    /// Stable step identifier chosen by the driver and retained during recovery.
    pub model_step_id: Id,
    /// Host requirements; saved history supplies the previous route and failure.
    pub routing: RouteRequest,
}

/// Current Host context for a bounded route-specific projection.
pub struct ModelProjectionContext {
    /// Exact authenticated namespace.
    pub scope: crate::Scope,
    /// Current principal for any separately authorized context reads.
    pub principal_ref: Id,
    /// Current Host capability grant.
    pub capability_grant_ref: Id,
    /// Cancelled when projection completes, fails, or its caller stops.
    pub cancellation: CancellationToken,
    /// Finite deadline inherited from the Run.
    pub deadline: tokio::time::Instant,
}

/// A fully prepared request and its route-specific input-token estimate.
#[derive(Debug, Clone)]
pub struct ProjectedModelRequest {
    /// Exact selected route, purpose, logical step, options and output budget.
    pub request: ModelRequest,
    /// Host/tokenizer estimate for this final projection, not a byte count.
    pub input_tokens: u64,
}

/// Trusted Host projection port. It preserves required context and builds a fresh
/// request for the exact selected route; it must not invoke a model or run a Tool.
pub trait ModelRequestProjector: Send + Sync {
    /// Project immutable transcript/context into the selected provider's contract.
    fn project<'a>(
        &'a self,
        selection: &'a RouteSelection,
        input: &'a RoutedModelInput,
        context: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ProjectedModelRequest>;
}

impl ModelExchange {
    /// Resolve, project, inspect, and execute a logical model step with finite
    /// explicit fallback. The same Run budgets account for every recovery and
    /// physical call. This does not advance an agent loop or execute Tools.
    ///
    /// The catalog/policy snapshot is pinned before the first physical attempt.
    /// Completed responses for this step are reused after current authorization
    /// and request-identity checks. Unresolved attempts require explicit recovery;
    /// this method never silently resends them. Projection is trusted Host code:
    /// it must preserve required context when changing providers.
    pub async fn generate_routed(
        &self,
        router: &dyn ModelRouter,
        input: &RoutedModelInput,
        projector: &dyn ModelRequestProjector,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<Guarded<ModelExchangeOutcome>, ContractError> {
        if self.inspector.is_none() {
            return Err(failure(ErrorCode::InvalidConfiguration, "model.inspector"));
        }
        if input.routing.scope != context.data.scope || &input.routing.scope != budget.scope() {
            return Err(failure(ErrorCode::AccessDenied, "routing.scope"));
        }
        if input.routing.previous_route.is_some() || input.routing.previous_failure.is_some() {
            return Err(failure(
                ErrorCode::ModelRoutingInvalid,
                "routing.history_is_stored",
            ));
        }
        budget.check_boundary().await?;
        let pinned = router.snapshot().clone();
        self.pin_routing(&pinned, context, budget).await?;
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        if input.routing.model_binding != saved.snapshot.profile.profile().model_binding {
            return Err(failure(
                ErrorCode::ModelRouteDenied,
                "routing.profile_binding",
            ));
        }
        if input.routing.purpose == ModelPurpose::Agent
            && input.routing.options != saved.snapshot.request.model_options
        {
            return Err(failure(ErrorCode::RequestConflict, "routing.model_options"));
        }
        if saved.snapshot.tool_ledger.iter().any(|entry| !matches!(&entry.state,
            ToolCallState::Settled { result } if result.status != crate::ToolResultStatus::Unknown
        )) {
            return Err(failure(ErrorCode::InvalidTransition, "routing.unsettled_tools"));
        }
        self.pin_step_input(input, context, budget).await?;
        if saved.snapshot.model_ledger.iter().any(|attempt| {
            matches!(
                attempt.state,
                ModelAttemptState::Reserved {} | ModelAttemptState::Unknown {}
            )
        }) {
            return Err(failure(
                ErrorCode::ModelAttemptUnresolved,
                "routing.attempt",
            ));
        }
        let previous = saved
            .snapshot
            .model_ledger
            .iter()
            .rev()
            .find(|attempt| attempt.model_step_id == input.model_step_id)
            .cloned();
        let mut routing = input.routing.clone();
        let mut replay = None;
        if let Some(previous) = previous {
            if previous.purpose != routing.purpose {
                return Err(failure(
                    ErrorCode::ModelRoutingMismatch,
                    "routing.step_purpose",
                ));
            }
            routing.previous_route = Some(previous.route.clone());
            match previous.state {
                ModelAttemptState::Completed {} => replay = Some(previous),
                ModelAttemptState::Failed { kind } => routing.previous_failure = Some(kind),
                ModelAttemptState::Reserved {} | ModelAttemptState::Unknown {} => {
                    return Err(failure(
                        ErrorCode::ModelAttemptUnresolved,
                        "routing.attempt",
                    ));
                }
            }
        }
        // A custom router must also advance monotonically through the pinned list.
        let mut previous_index = None;
        for _ in 0..=crate::MAX_ROUTE_FALLBACKS {
            budget.check_boundary().await?;
            if context.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let selection = external(
                async { router.resolve(&routing).await },
                context,
                budget,
                ErrorCode::ModelRoutingInvalid,
            )
            .await?;
            if router.snapshot().digest() != pinned.digest() {
                return Err(failure(ErrorCode::ModelRoutingMismatch, "routing.snapshot"));
            }
            pinned.validate_selection(&routing, &selection)?;
            if previous_index.is_some_and(|index| selection.candidate_index <= index) {
                return Err(failure(
                    ErrorCode::ModelRoutingMismatch,
                    "routing.fallback_order",
                ));
            }
            if matches!(selection.reason, RouteSelectionReason::Fallback { .. }) {
                budget.reserve(ReservationKind::Recovery {}).await?;
            }
            // Authorize the exact destination before a Host projection or metadata lookup.
            if let Guarded::ApprovalRequired(challenge) = self
                .authorize_route(&selection.route, routing.purpose, context, budget)
                .await?
            {
                return Ok(Guarded::ApprovalRequired(challenge));
            }
            let projection_context = ModelProjectionContext {
                scope: budget.scope().clone(),
                principal_ref: context.data.principal_ref.clone(),
                capability_grant_ref: context.data.capability_grant_ref.clone(),
                cancellation: budget.cancellation().child_token(),
                deadline: budget.call_deadline()?,
            };
            let _cancel = projection_context.cancellation.clone().drop_guard();
            let prepared = external(
                async {
                    projector
                        .project(&selection, input, &projection_context)
                        .await
                },
                context,
                budget,
                ErrorCode::ModelContextIncompatible,
            )
            .await?;
            projection_context.cancellation.cancel();
            validate_projection(&prepared.request, input, &selection)?;
            // Validate the final route-specific estimate, not the earlier candidate estimate.
            let mut final_requirements = routing.clone();
            final_requirements.input_tokens = prepared.input_tokens;
            final_requirements
                .required_capabilities
                .extend(prepared.request.required_capabilities());
            final_requirements.previous_route = Some(selection.route.clone());
            final_requirements.previous_failure = None;
            let mut final_selection = selection.clone();
            final_selection.reason = RouteSelectionReason::Reuse;
            final_selection.request_digest = final_requirements.digest();
            pinned.validate_selection(&final_requirements, &final_selection)?;
            if let Some(previous) = replay.take() {
                let mut physical = prepared.request.clone();
                physical.request_id = previous.attempt_id;
                if physical.digest() != previous.request_digest {
                    return Err(failure(
                        ErrorCode::RequestConflict,
                        "routing.replay_projection",
                    ));
                }
                if let Guarded::ApprovalRequired(challenge) =
                    self.authorize(&prepared.request, context, budget).await?
                {
                    return Ok(Guarded::ApprovalRequired(challenge));
                }
                let reference = previous
                    .response_ref
                    .ok_or_else(|| failure(ErrorCode::InvalidSnapshot, "routing.response"))?;
                let record = budget
                    .store()
                    .read_record(budget.scope(), &reference)
                    .await?;
                let response: StoredModelResponse = serde_json::from_value(record.value().clone())
                    .map_err(|_| failure(ErrorCode::InvalidSnapshot, "routing.response"))?;
                budget.check_boundary().await?;
                if context.cancellation.is_cancelled() {
                    return Err(cancelled());
                }
                return Ok(Guarded::Completed(response.outcome));
            }
            let rule = pinned
                .policy()
                .rules
                .iter()
                .find(|rule| {
                    rule.model_binding == routing.model_binding && rule.purpose == routing.purpose
                })
                .ok_or_else(|| failure(ErrorCode::ModelRouteDenied, "routing.rule"))?;
            let version_policy = if selection.route.version_semantics
                == crate::VersionSemantics::Pinned
                || rule.version_policy == VersionPolicy::RequirePinned
                || routing.version_policy == VersionPolicy::RequirePinned
            {
                VersionPolicy::RequirePinned
            } else {
                VersionPolicy::AllowMutable
            };
            let result = self
                .generate_inner(
                    &prepared.request,
                    context,
                    budget,
                    Some((&selection, version_policy)),
                )
                .await;
            let cause = match result {
                Ok(Guarded::Completed(ModelExchangeOutcome::Failed { failure: ref error })) => {
                    error.kind
                }
                Err(ref error) if error.code == ErrorCode::ModelVersionDrift => {
                    ModelFailureKind::VersionDrift
                }
                Err(ref error) if error.code == ErrorCode::ModelUnavailable => {
                    ModelFailureKind::Unavailable
                }
                other => return other,
            };
            if !rule.fallback_on.contains(&cause) {
                return result;
            }
            previous_index = Some(selection.candidate_index);
            routing.previous_route = Some(selection.route);
            routing.previous_failure = Some(cause);
        }
        Err(failure(
            ErrorCode::ModelRoutesExhausted,
            "routing.candidates",
        ))
    }

    async fn pin_routing(
        &self,
        routing: &RoutingSnapshot,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        if routing.scope() != budget.scope() {
            return Err(failure(ErrorCode::AccessDenied, "routing.scope"));
        }
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        if let Some(reference) = &saved.snapshot.routing_snapshot_ref {
            let record = budget
                .store()
                .read_record(budget.scope(), reference)
                .await?;
            let restored = RoutingSnapshot::restore(
                &serde_json::to_string(record.value()).map_err(|_| revision_error())?,
                budget.scope(),
                &reference.digest,
            )?;
            if restored.digest() != routing.digest() {
                return Err(failure(
                    ErrorCode::ModelRoutingMismatch,
                    "routing.pinned_snapshot",
                ));
            }
            return Ok(());
        }
        if saved.snapshot.usage.model_calls != 0 {
            return Err(failure(
                ErrorCode::ModelRoutingMismatch,
                "routing.already_started",
            ));
        }
        if context.cancellation.is_cancelled() {
            return Err(cancelled());
        }
        budget.check_boundary().await?;
        let record = ProtectedRecord::new(
            Id::new(format!("model-routing-{}", budget.run_id()))?,
            1,
            serde_json::to_value(routing).map_err(|_| revision_error())?,
        );
        let mut snapshot = saved.snapshot;
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(revision_error)?;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        snapshot.routing_snapshot_ref = Some(record.reference().clone());
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
                    messages: vec![],
                    events: vec![],
                    records: vec![record],
                },
            )
            .await?;
        budget.check_boundary().await?;
        Ok(())
    }

    async fn pin_step_input(
        &self,
        input: &RoutedModelInput,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let key =
            crate::canonical_digest(&serde_json::json!([budget.run_id(), input.model_step_id]));
        let record = ProtectedRecord::new(
            Id::new(format!("model-step-{key}"))?,
            1,
            serde_json::json!({"schema_version":"wickle.model-step.v1", "run_id":budget.run_id(), "input":input}),
        );
        match budget
            .store()
            .read_record(budget.scope(), record.reference())
            .await
        {
            Ok(_) => return Ok(()),
            Err(error) if error.code == ErrorCode::StateNotFound => {}
            Err(error) if error.code == ErrorCode::RecordConflict => {
                return Err(failure(ErrorCode::RequestConflict, "routing.step_input"));
            }
            Err(error) => return Err(error),
        }
        budget.check_boundary().await?;
        if context.cancellation.is_cancelled() {
            return Err(cancelled());
        }
        let mut snapshot = budget
            .store()
            .load(budget.scope(), budget.run_id())
            .await?
            .snapshot;
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(revision_error)?;
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
                    messages: vec![],
                    events: vec![],
                    records: vec![record],
                },
            )
            .await?;
        budget.check_boundary().await?;
        Ok(())
    }

    pub(super) async fn inspect_route(
        &self,
        request: &ModelRequest,
        version_policy: VersionPolicy,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<crate::ModelRouteObservation, ContractError> {
        let (inspector, timeout) = self
            .inspector
            .as_ref()
            .ok_or_else(|| failure(ErrorCode::InvalidConfiguration, "model.inspector"))?;
        budget.check_boundary().await?;
        let deadline = tokio::time::Instant::now()
            .checked_add(*timeout)
            .ok_or_else(|| failure(ErrorCode::InvalidConfiguration, "model.inspection_timeout"))?
            .min(budget.call_deadline()?);
        let inspection = ModelInspectionContext {
            scope: budget.scope().clone(),
            principal_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
            cancellation: budget.cancellation().child_token(),
            deadline,
        };
        let _cancel = inspection.cancellation.clone().drop_guard();
        let observation = tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => return Err(failure(ErrorCode::ModelInspectionUnavailable, "model.inspection_timeout")),
            result = external(async { inspector.inspect(&request.route, &inspection).await }, context, budget, ErrorCode::ModelInspectionUnavailable) => result?,
        };
        inspection.cancellation.cancel();
        observation.validate(&request.route, version_policy)?;
        budget.check_boundary().await?;
        if context.cancellation.is_cancelled() {
            return Err(cancelled());
        }
        Ok(observation)
    }

    async fn authorize_route(
        &self,
        route: &ResolvedModelRoute,
        purpose: ModelPurpose,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<Guarded<()>, ContractError> {
        let request = PolicyRequest {
            owner_scope: budget.scope().clone(),
            resource_id: budget.run_id().clone(),
            action: PolicyAction::InvokeModel {
                route: Box::new(route.clone()),
                purpose,
            },
        };
        external(
            self.policy.guard(
                &request,
                context,
                Some(budget.call_deadline()?),
                None,
                || async { Ok(()) },
            ),
            context,
            budget,
            ErrorCode::PolicyUnavailable,
        )
        .await
    }
}

fn validate_projection(
    request: &ModelRequest,
    input: &RoutedModelInput,
    selection: &RouteSelection,
) -> Result<(), ContractError> {
    if request.request_id != input.model_step_id
        || request.route != selection.route
        || request.purpose != input.routing.purpose
        || request.options != input.routing.options
        || request.max_output_tokens != input.routing.max_output_tokens
    {
        return Err(failure(
            ErrorCode::ModelContextIncompatible,
            "routing.projection",
        ));
    }
    request.validate()
}

async fn external<T>(
    future: impl std::future::Future<Output = Result<T, ContractError>>,
    context: &ExecutionContext,
    budget: &RunBudget,
    code: ErrorCode,
) -> Result<T, ContractError> {
    tokio::select! {
        biased;
        _ = context.cancellation.cancelled() => Err(cancelled()),
        stopped = budget.wait_for_cancellation_or_deadline() => match stopped {
            Err(error) => Err(error), Ok(()) => Err(failure(ErrorCode::DeadlineExceeded, "model.routing")),
        },
        result = AssertUnwindSafe(future).catch_unwind() => result.map_err(|_| failure(code, "model.routing_callback"))?.map_err(|error| failure(error.code, "model.routing_callback")),
    }
}

fn failure(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}

pub(super) fn reason_code(reason: RouteSelectionReason) -> &'static str {
    match reason {
        RouteSelectionReason::Initial => "initial_route",
        RouteSelectionReason::Reuse => "saved_route_reuse",
        RouteSelectionReason::Fallback { failure } => match failure {
            ModelFailureKind::Timeout => "fallback_timeout",
            ModelFailureKind::RateLimited => "fallback_rate_limited",
            ModelFailureKind::Transport => "fallback_transport",
            ModelFailureKind::Protocol => "fallback_protocol",
            ModelFailureKind::ContextOverflow => "fallback_context_overflow",
            ModelFailureKind::Authentication => "fallback_authentication",
            ModelFailureKind::Unsupported => "fallback_unsupported",
            ModelFailureKind::Unavailable => "fallback_unavailable",
            ModelFailureKind::VersionDrift => "fallback_version_drift",
        },
    }
}
```

## `crates/wickle/src/model_protocol.rs`

```rust
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, io,
    num::NonZeroU64,
};

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::{
    ContractError, ErrorCode, Id, JsonDigest, JsonObject, ModelFailureKind, ModelPurpose,
    ModelUsage, PortStream, ResolvedModelRoute, Scope, VersionedRef, parse_json,
    serialization::data_digest,
};

/// Adapter-owned provider, implementation, and credential-binding identities.
/// Actual credentials remain in the adapter instance, never in a model request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelPortBinding {
    /// Registered service key, distinct for direct and hosted provider paths.
    pub provider: Id,
    /// Exact adapter implementation identity.
    pub adapter: VersionedRef,
    /// Host-owned credential/connection binding revision.
    pub connection_ref: VersionedRef,
}

impl ModelPortBinding {
    /// Require all adapter identities to match the immutable selected route.
    pub fn matches_route(&self, route: &ResolvedModelRoute) -> bool {
        self.provider == route.provider
            && self.adapter == route.adapter
            && self.connection_ref == route.connection_ref
    }
}

/// One physical model request's runtime context, without credentials or system inputs.
#[derive(Debug, Clone)]
pub struct ModelCallContext {
    /// Budget reservation and physical invocation identity.
    pub attempt_id: Id,
    /// Owning execution.
    pub run_id: Id,
    /// Authenticated data/execution scope supplied by the Host.
    pub scope: Scope,
    /// Cooperative cancellation signal.
    pub cancellation: CancellationToken,
    /// Effective deadline for this physical invocation.
    pub deadline: tokio::time::Instant,
}

/// A single physical provider invocation, without an internal agent loop or retry.
/// Adapters disable hidden SDK retries and provider-native tool execution. They
/// enforce wire/body limits while decoding, report safe typed errors, and end the
/// stream after the one request. Credentials are obtained through their binding.
pub trait ModelPort: Send + Sync {
    /// Identities of the adapter and connection actually used by this instance.
    fn binding(&self) -> ModelPortBinding;
    /// Generate one response. Partial argument deltas are never execution commands.
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent>;
}

/// Roles in an already-authorized model projection, separate from stored messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelRole {
    /// Trusted instructions selected by the core's projection layer.
    System,
    /// User data selected for this invocation.
    User,
    /// Prior model content and proposed calls.
    Assistant,
    /// Observations paired with prior proposed calls.
    Tool,
}

/// Provider continuation data pinned to an exact route, including its versions.
/// Explicit serialization is for protected storage/provider replay only.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpaqueContinuation {
    route_digest: JsonDigest,
    data: Value,
}

impl OpaqueContinuation {
    /// Bind provider-replay data to the route that produced it.
    pub fn new(route: &ResolvedModelRoute, data: Value) -> Self {
        Self {
            route_digest: route.digest(),
            data,
        }
    }
    /// Exact route identity required before replaying these bytes.
    pub fn route_digest(&self) -> &JsonDigest {
        &self.route_digest
    }
    /// Explicit privileged access for the matching provider adapter.
    pub fn data(&self) -> &Value {
        &self.data
    }
}

impl fmt::Debug for OpaqueContinuation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpaqueContinuation")
            .field("route_digest", &self.route_digest)
            .field("data", &"<redacted>")
            .finish()
    }
}

/// Provider-facing content selected explicitly by the core projection layer.
/// No variant contains ExecutionContext, complete system inputs, or storage records.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelContent {
    /// Authorized text, including instructions or observations as indicated by role.
    Text {
        /// Text sent to the selected model.
        text: String,
    },
    /// Explicitly selected JSON data; it conveys no authority by itself.
    Json {
        /// Model-visible JSON.
        value: Value,
    },
    /// A prior model-proposed call, containing only model-owned arguments.
    ToolCall {
        /// Provider/projection call identity, paired with its observation.
        provider_call_id: Id,
        /// Normalized model-facing tool name.
        name: Id,
        /// Model-owned arguments, never merged system execution arguments.
        arguments: JsonObject,
    },
    /// A limited observation without receipts or hidden input maps.
    ToolResult {
        /// Matching call in the preceding assistant tool round.
        provider_call_id: Id,
        /// Explicitly selected model-visible observation.
        content: Value,
    },
    /// Provider-replay data permitted only on its original exact route.
    Opaque {
        /// Protected continuation selected for provider replay.
        continuation: OpaqueContinuation,
    },
}

impl fmt::Debug for ModelContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Text { .. } => "text",
            Self::Json { .. } => "json",
            Self::ToolCall { .. } => "tool_call",
            Self::ToolResult { .. } => "tool_result",
            Self::Opaque { .. } => "opaque",
        };
        f.debug_struct("ModelContent")
            .field("type", &kind)
            .finish_non_exhaustive()
    }
}

/// A projected message, not an original transcript record or client-submitted role.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelMessage {
    /// Role selected after validating provenance and allowed instruction placement.
    pub role: ModelRole,
    /// Model-visible content only.
    pub content: Vec<ModelContent>,
}

/// Only the compiled model-facing portion of a registered tool definition.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelTool {
    /// Portable normalized ASCII name: letters, digits, underscore or hyphen, 1..64 bytes.
    pub name: Id,
    /// Model-facing description; hidden input descriptions are excluded by the compiler.
    pub description: String,
    /// Derived input schema, without system-owned fields or their definitions/examples.
    pub model_input_schema: Value,
}

impl fmt::Debug for ModelTool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelTool")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// Provider output-mode request. Final candidate verification belongs to the driver.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelOutput {
    /// Ordinary text output.
    Text {},
    /// Structured output with an already-resolved, model-visible schema.
    JsonSchema {
        /// Resolved output schema, without storage lookup references.
        schema: Value,
    },
}

impl fmt::Debug for ModelOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Text {} => "ModelOutput::Text",
            Self::JsonSchema { .. } => "ModelOutput::JsonSchema(<redacted>)",
        })
    }
}

/// Finite decoding bounds, independent of token usage and total run budgets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelResponseLimits {
    /// Maximum serialized logical request size, including projection and schemas.
    pub max_input_bytes: usize,
    /// Cumulative UTF-8 response payload bytes, including call metadata and opaque data.
    pub max_response_bytes: usize,
    /// Maximum bytes in one text or tool-argument fragment.
    pub max_delta_bytes: usize,
    /// Maximum stream events, including empty fragments and terminal events.
    pub max_events: usize,
    /// Maximum distinct proposed calls; zero prohibits tool-call output.
    pub max_tool_calls: usize,
}

/// Immutable-for-invocation logical request containing only a prepared projection.
/// Explicit serialization is for protected storage/transport codecs, not public logs.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRequest {
    /// Request identity, scoped to this physical provider invocation.
    pub request_id: Id,
    /// Agent, verification, or compaction accounting purpose.
    pub purpose: ModelPurpose,
    /// Exact selected provider/model/API/connection revisions.
    pub route: ResolvedModelRoute,
    /// Authorized projection; never the raw transcript or ExecutionContext.
    pub messages: Vec<ModelMessage>,
    /// Derived model-facing tool definitions only.
    pub tools: Vec<ModelTool>,
    /// Requested provider output format, distinct from final outcome verification.
    pub output: ModelOutput,
    /// Finite provider output-token request.
    pub max_output_tokens: NonZeroU64,
    /// Host-owned logical options; the Host/router must validate selected catalog schemas.
    /// Adapters explicitly map supported keys to their API; this is not a raw wire-body merge.
    #[serde(default, skip_serializing_if = "JsonObject::is_empty")]
    pub options: JsonObject,
    /// Input and response decoding limits selected for this route.
    pub limits: ModelResponseLimits,
}

impl fmt::Debug for ModelRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelRequest")
            .field("request_id", &self.request_id)
            .field("purpose", &self.purpose)
            .field("route_digest", &self.route.digest())
            .field("message_count", &self.messages.len())
            .field("tool_count", &self.tools.len())
            .finish_non_exhaustive()
    }
}

impl ModelRequest {
    /// Capabilities implied by the prepared request: `text`, plus `tool_calling`
    /// for tool schemas and `json_output` for a structured output contract.
    /// Host requirements may add further registered features.
    pub fn required_capabilities(&self) -> std::collections::BTreeSet<Id> {
        let mut features = std::collections::BTreeSet::from([
            Id::new("text").expect("static capability identifier")
        ]);
        if !self.tools.is_empty() {
            features.insert(Id::new("tool_calling").expect("static capability identifier"));
        }
        if matches!(self.output, ModelOutput::JsonSchema { .. }) {
            features.insert(Id::new("json_output").expect("static capability identifier"));
        }
        features
    }

    /// Hash the complete prepared request, without introducing runtime credentials.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }

    /// Check finite bounds, projected protocol, schemas and continuation route identity.
    /// Host policy and actual adapter-binding checks are separate execution boundaries.
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.limits.max_input_bytes == 0
            || self.limits.max_response_bytes == 0
            || self.limits.max_delta_bytes == 0
            || self.limits.max_events == 0
        {
            return Err(invalid_request("model_request.limits"));
        }
        json_size(self, self.limits.max_input_bytes)
            .map_err(|_| invalid_request("model_request.input_size"))?;
        let mut names = BTreeSet::new();
        for tool in &self.tools {
            if !valid_name(tool.name.as_str())
                || !names.insert(&tool.name)
                || tool.model_input_schema.get("type").and_then(Value::as_str) != Some("object")
            {
                return Err(invalid_request("model_request.tools"));
            }
            compile_schema(&tool.model_input_schema)?;
        }
        if let ModelOutput::JsonSchema { schema } = &self.output {
            compile_schema(schema)?;
        }
        let mut pending_calls = BTreeSet::new();
        for message in &self.messages {
            if !pending_calls.is_empty() && message.role != ModelRole::Tool {
                return Err(invalid_request("model_request.tool_results"));
            }
            let mut round_calls = BTreeSet::new();
            for content in &message.content {
                match content {
                    ModelContent::ToolCall {
                        provider_call_id,
                        name,
                        ..
                    } => {
                        if message.role != ModelRole::Assistant
                            || !valid_call_id(provider_call_id.as_str())
                            || !valid_name(name.as_str())
                            || !round_calls.insert(provider_call_id.clone())
                        {
                            return Err(invalid_request("model_request.tool_calls"));
                        }
                    }
                    ModelContent::ToolResult {
                        provider_call_id, ..
                    } => {
                        if message.role != ModelRole::Tool
                            || !pending_calls.remove(provider_call_id)
                        {
                            return Err(invalid_request("model_request.tool_results"));
                        }
                    }
                    ModelContent::Opaque { continuation } => {
                        if message.role != ModelRole::Assistant
                            || continuation.route_digest() != &self.route.digest()
                        {
                            return Err(invalid_request("model_request.continuation"));
                        }
                    }
                    ModelContent::Text { .. } | ModelContent::Json { .. } => {
                        if message.role == ModelRole::Tool {
                            return Err(invalid_request("model_request.tool_results"));
                        }
                    }
                }
            }
            pending_calls.extend(round_calls);
        }
        if !pending_calls.is_empty() {
            return Err(invalid_request("model_request.tool_results"));
        }
        Ok(())
    }
}

/// Provider finish classification, normalized independently of provider strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelFinish {
    /// A complete answer without proposed tool calls.
    Stop,
    /// A complete response proposing one or more calls.
    ToolCalls,
    /// Truncated output. Assembly returns an error and no executable call plan.
    Length,
    /// Explicit refusal without a tool plan; not a successful business outcome.
    Refusal,
}

/// Facts actually reported by the provider; omitted identifiers and usage stay unknown.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelResponseMetadata {
    /// Provider correlation identity, if reported.
    pub provider_request_id: Option<Id>,
    /// Actual response model, not copied from the requested route as a guess.
    pub reported_model_id: Option<Id>,
    /// Actual reported release/version, if supplied by the provider.
    pub reported_model_version: Option<Id>,
    /// Reported or explicitly estimated token usage, never implicit zeroes.
    pub usage: Option<ModelUsage>,
}

impl fmt::Debug for ModelResponseMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelResponseMetadata")
            .field("usage", &self.usage)
            .finish_non_exhaustive()
    }
}

/// Untrusted provider response fragments. None is a core ToolCall or dispatch permit.
#[derive(Clone, PartialEq)]
pub enum ModelEvent {
    /// Candidate text fragment.
    TextDelta {
        /// UTF-8 text, subject to per-fragment and aggregate bounds.
        text: String,
    },
    /// Fragment of one proposed call. Identity/name may arrive in an earlier fragment.
    ToolArgumentsDelta {
        /// Stable provider output index, allowing interleaved call fragments.
        index: u32,
        /// Complete provider identity when available; conflicting replacements fail.
        provider_call_id: Option<String>,
        /// Complete normalized name when available; conflicting replacements fail.
        name: Option<String>,
        /// JSON argument fragment, never executed before full response validation.
        delta: String,
    },
    /// Complete logical response; the adapter must subsequently end this request stream.
    ResponseCompleted {
        /// Normalized finish classification.
        finish: ModelFinish,
        /// Actually reported response metadata.
        metadata: ModelResponseMetadata,
        /// Protected continuation data bound to this request's route.
        continuation: Vec<OpaqueContinuation>,
    },
    /// Safe classified provider failure, without raw SDK error strings.
    ResponseError {
        /// Classification used by the core's bounded recovery policy.
        kind: ModelFailureKind,
        /// Facts actually reported before failure.
        metadata: ModelResponseMetadata,
    },
}

impl fmt::Debug for ModelEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::TextDelta { .. } => "text_delta",
            Self::ToolArgumentsDelta { .. } => "tool_arguments_delta",
            Self::ResponseCompleted { .. } => "response_completed",
            Self::ResponseError { .. } => "response_error",
        };
        f.debug_struct("ModelEvent")
            .field("type", &kind)
            .finish_non_exhaustive()
    }
}

/// Protocol validation only; even Valid proposals require the later tool boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallValidation {
    /// Known model-facing tool and conforming model-owned arguments.
    Valid,
    /// Not among the tools advertised for this request; execution is prohibited.
    UnknownTool,
    /// Model-owned arguments fail the advertised schema; execution is prohibited.
    InvalidArguments,
}

/// A complete provider proposal, awaiting core call-ID allocation and protected planning.
/// It cannot be passed as a core ToolCall without explicit later materialization.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposedToolCall {
    /// Provider-local identity, scoped by the ModelResponse request_id.
    pub provider_call_id: Id,
    /// Normalized model-facing name.
    pub name: Id,
    /// Parsed model-owned JSON object; no system parameters have been injected.
    pub model_inputs: JsonObject,
    /// Preliminary schema/availability result, not policy or tool authorization.
    pub validation: ToolCallValidation,
}

impl fmt::Debug for ProposedToolCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProposedToolCall")
            .field("name", &self.name)
            .field("validation", &self.validation)
            .finish_non_exhaustive()
    }
}

/// A complete protocol response, distinct from a verified agent outcome or persisted plan.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelResponse {
    /// Request that scopes provider call identifiers.
    pub request_id: Id,
    /// Exact route that produced this response and continuation data.
    pub route_digest: JsonDigest,
    /// Complete candidate text; final output verification remains separate.
    pub text: String,
    /// Complete proposals, including structured rejection reasons for invalid tools.
    pub tool_calls: Vec<ProposedToolCall>,
    /// Complete stop/tool/refusal classification.
    pub finish: ModelFinish,
    /// Provider-reported facts.
    pub metadata: ModelResponseMetadata,
    /// Protected provider-replay data.
    pub continuation: Vec<OpaqueContinuation>,
}

impl fmt::Debug for ModelResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelResponse")
            .field("request_id", &self.request_id)
            .field("finish", &self.finish)
            .field("tool_count", &self.tool_calls.len())
            .finish_non_exhaustive()
    }
}

/// Safe, stable assembly/recovery reasons without submitted fragments or SDK messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelProtocolErrorCode {
    /// Invalid projection, schema, or request bounds.
    InvalidRequest,
    /// The stream failed before a complete response was received.
    StreamFailure,
    /// EOF arrived without a terminal response.
    MissingCompletion,
    /// Another terminal or any event followed completion.
    UnexpectedEvent,
    /// Arguments were malformed, ambiguous, or not a JSON object.
    InvalidArguments,
    /// A provider call identity was missing, malformed, or replaced.
    InvalidCallId,
    /// A model-facing name was missing, malformed, or replaced.
    InvalidToolName,
    /// Different proposed calls reused the same provider-local identity.
    DuplicateCallId,
    /// The normalized finish classification conflicts with the actual proposed calls.
    FinishMismatch,
    /// A response event, byte, fragment or tool count exceeded its finite bound.
    ResponseLimitExceeded,
    /// Continuation data belongs to a different route or version.
    RouteMismatch,
    /// The adapter reported a typed provider failure.
    ProviderFailure,
    /// Output was truncated, even if individual argument fragments looked complete.
    OutputTruncated,
}

/// A failed response may expose bounded candidate text explicitly, never partial calls.
/// Serialization is for protected failure records, never automatic public logging.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelProtocolError {
    /// Recovery classification; the core decides whether another paid attempt is allowed.
    pub kind: ModelFailureKind,
    /// Safe protocol/provider reason.
    pub code: ModelProtocolErrorCode,
    /// Metadata actually observed; missing values remain unknown.
    pub metadata: Box<ModelResponseMetadata>,
    partial_text: String,
}

impl ModelProtocolError {
    /// Construct a sanitized failure, without embedding an arbitrary SDK error.
    pub fn new(
        kind: ModelFailureKind,
        code: ModelProtocolErrorCode,
        metadata: ModelResponseMetadata,
    ) -> Self {
        Self {
            kind,
            code,
            metadata: Box::new(metadata),
            partial_text: String::new(),
        }
    }
    /// Explicit access to bounded, unverified text; it is not a completed answer.
    pub fn partial_text(&self) -> &str {
        &self.partial_text
    }
}

impl fmt::Debug for ModelProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelProtocolError")
            .field("kind", &self.kind)
            .field("code", &self.code)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for ModelProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {:?}", self.kind, self.code)
    }
}
impl std::error::Error for ModelProtocolError {}

#[derive(Default)]
struct PartialCall {
    provider_call_id: Option<String>,
    name: Option<String>,
    arguments: String,
}

struct ResponseAssembly {
    text: String,
    calls: BTreeMap<u32, PartialCall>,
    bytes: usize,
    events: usize,
    terminal: Option<(ModelFinish, ModelResponseMetadata, Vec<OpaqueContinuation>)>,
}

impl ResponseAssembly {
    fn error(&self, code: ModelProtocolErrorCode) -> ModelProtocolError {
        let mut error = ModelProtocolError::new(
            ModelFailureKind::Protocol,
            code,
            self.terminal
                .as_ref()
                .map_or_else(ModelResponseMetadata::default, |(_, metadata, _)| {
                    metadata.clone()
                }),
        );
        error.partial_text = self.text.clone();
        error
    }
    fn add_bytes(&mut self, count: usize, maximum: usize) -> Result<(), ModelProtocolError> {
        let total = self
            .bytes
            .checked_add(count)
            .filter(|n| *n <= maximum)
            .ok_or_else(|| self.error(ModelProtocolErrorCode::ResponseLimitExceeded))?;
        self.bytes = total;
        Ok(())
    }
}

/// Collect one logical response through EOF. No partial call escapes on failure.
/// The caller must bound time/cancellation around collection: a stream that never
/// yields cannot be stopped by event/byte limits alone. No tools are executed here.
pub async fn collect_model_response(
    request: &ModelRequest,
    mut stream: PortStream<'_, ModelEvent>,
) -> Result<ModelResponse, ModelProtocolError> {
    request.validate().map_err(|_| {
        ModelProtocolError::new(
            ModelFailureKind::Protocol,
            ModelProtocolErrorCode::InvalidRequest,
            ModelResponseMetadata::default(),
        )
    })?;
    let mut assembly = ResponseAssembly {
        text: String::new(),
        calls: BTreeMap::new(),
        bytes: 0,
        events: 0,
        terminal: None,
    };
    while let Some(next) = stream.next().await {
        if assembly.terminal.is_some() {
            return Err(assembly.error(ModelProtocolErrorCode::UnexpectedEvent));
        }
        assembly.events = assembly
            .events
            .checked_add(1)
            .filter(|count| *count <= request.limits.max_events)
            .ok_or_else(|| assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded))?;
        let event = next.map_err(|_| {
            let mut error = assembly.error(ModelProtocolErrorCode::StreamFailure);
            error.kind = ModelFailureKind::Transport;
            error
        })?;
        match event {
            ModelEvent::TextDelta { text } => {
                if text.len() > request.limits.max_delta_bytes {
                    return Err(assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded));
                }
                assembly.add_bytes(text.len(), request.limits.max_response_bytes)?;
                assembly.text.push_str(&text);
            }
            ModelEvent::ToolArgumentsDelta {
                index,
                provider_call_id,
                name,
                delta,
            } => {
                if delta.len() > request.limits.max_delta_bytes
                    || (!assembly.calls.contains_key(&index)
                        && assembly.calls.len() >= request.limits.max_tool_calls)
                {
                    return Err(assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded));
                }
                if provider_call_id
                    .as_ref()
                    .is_some_and(|id| !valid_call_id(id))
                {
                    return Err(assembly.error(ModelProtocolErrorCode::InvalidCallId));
                }
                if name.as_ref().is_some_and(|name| !valid_name(name)) {
                    return Err(assembly.error(ModelProtocolErrorCode::InvalidToolName));
                }
                let bytes = delta
                    .len()
                    .saturating_add(provider_call_id.as_ref().map_or(0, String::len))
                    .saturating_add(name.as_ref().map_or(0, String::len));
                assembly.add_bytes(bytes, request.limits.max_response_bytes)?;
                if let Some(existing) = assembly.calls.get(&index) {
                    if existing
                        .provider_call_id
                        .as_ref()
                        .zip(provider_call_id.as_ref())
                        .is_some_and(|(old, new)| old != new)
                    {
                        return Err(assembly.error(ModelProtocolErrorCode::InvalidCallId));
                    }
                    if existing
                        .name
                        .as_ref()
                        .zip(name.as_ref())
                        .is_some_and(|(old, new)| old != new)
                    {
                        return Err(assembly.error(ModelProtocolErrorCode::InvalidToolName));
                    }
                }
                let call = assembly.calls.entry(index).or_default();
                if let Some(id) = provider_call_id {
                    call.provider_call_id = Some(id);
                }
                if let Some(name) = name {
                    call.name = Some(name);
                }
                call.arguments.push_str(&delta);
            }
            ModelEvent::ResponseCompleted {
                finish,
                metadata,
                continuation,
            } => {
                let mut bytes = metadata_bytes(&metadata)
                    .ok_or_else(|| assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded))?;
                for item in &continuation {
                    if item.route_digest() != &request.route.digest() {
                        return Err(assembly.error(ModelProtocolErrorCode::RouteMismatch));
                    }
                    let size = json_size(item.data(), request.limits.max_response_bytes).map_err(
                        |_| assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded),
                    )?;
                    bytes = bytes.checked_add(size).ok_or_else(|| {
                        assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded)
                    })?;
                }
                assembly.add_bytes(bytes, request.limits.max_response_bytes)?;
                assembly.terminal = Some((finish, metadata, continuation));
            }
            ModelEvent::ResponseError { kind, metadata } => {
                let bytes = metadata_bytes(&metadata)
                    .ok_or_else(|| assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded))?;
                assembly.add_bytes(bytes, request.limits.max_response_bytes)?;
                let mut error = ModelProtocolError::new(
                    kind,
                    ModelProtocolErrorCode::ProviderFailure,
                    metadata,
                );
                error.partial_text = assembly.text;
                return Err(error);
            }
        }
    }
    let (finish, metadata, continuation) = assembly
        .terminal
        .as_ref()
        .ok_or_else(|| assembly.error(ModelProtocolErrorCode::MissingCompletion))?;
    if *finish == ModelFinish::Length {
        return Err(assembly.error(ModelProtocolErrorCode::OutputTruncated));
    }
    if (*finish == ModelFinish::ToolCalls) != !assembly.calls.is_empty() {
        return Err(assembly.error(ModelProtocolErrorCode::FinishMismatch));
    }
    let mut tool_calls = Vec::new();
    let mut seen_ids = BTreeSet::new();
    for call in assembly.calls.values() {
        let provider_call_id = call
            .provider_call_id
            .as_ref()
            .ok_or_else(|| assembly.error(ModelProtocolErrorCode::InvalidCallId))?;
        if !seen_ids.insert(provider_call_id) {
            return Err(assembly.error(ModelProtocolErrorCode::DuplicateCallId));
        }
        let name = call
            .name
            .as_ref()
            .ok_or_else(|| assembly.error(ModelProtocolErrorCode::InvalidToolName))?;
        let value = parse_json(&call.arguments)
            .map_err(|_| assembly.error(ModelProtocolErrorCode::InvalidArguments))?;
        let object = value
            .as_object()
            .ok_or_else(|| assembly.error(ModelProtocolErrorCode::InvalidArguments))?;
        let validation = match request.tools.iter().find(|tool| tool.name.as_str() == name) {
            None => ToolCallValidation::UnknownTool,
            Some(tool) => {
                let validator = compile_schema(&tool.model_input_schema)
                    .map_err(|_| assembly.error(ModelProtocolErrorCode::InvalidRequest))?;
                if validator.is_valid(&value) {
                    ToolCallValidation::Valid
                } else {
                    ToolCallValidation::InvalidArguments
                }
            }
        };
        tool_calls.push(ProposedToolCall {
            provider_call_id: Id::new(provider_call_id.clone())
                .map_err(|_| assembly.error(ModelProtocolErrorCode::InvalidCallId))?,
            name: Id::new(name.clone())
                .map_err(|_| assembly.error(ModelProtocolErrorCode::InvalidToolName))?,
            model_inputs: object
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            validation,
        });
    }
    Ok(ModelResponse {
        request_id: request.request_id.clone(),
        route_digest: request.route.digest(),
        text: assembly.text,
        tool_calls,
        finish: *finish,
        metadata: metadata.clone(),
        continuation: continuation.clone(),
    })
}

fn invalid_request(path: &str) -> ContractError {
    ContractError::new(ErrorCode::InvalidContract, path)
}

fn valid_call_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value.chars().any(|c| c.is_whitespace() || c.is_control())
}
fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

struct NoSchemaRetrieval;
impl jsonschema::Retrieve for NoSchemaRetrieval {
    fn retrieve(
        &self,
        _: &jsonschema::Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err("external schema retrieval is disabled".into())
    }
}
fn compile_schema(schema: &Value) -> Result<jsonschema::Validator, ContractError> {
    jsonschema::draft202012::options()
        .should_validate_formats(true)
        .with_retriever(NoSchemaRetrieval)
        .build(schema)
        .map_err(|_| invalid_request("model_request.schema"))
}

struct ByteCounter {
    count: usize,
    maximum: usize,
}
impl io::Write for ByteCounter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.count = self
            .count
            .checked_add(buffer.len())
            .filter(|count| *count <= self.maximum)
            .ok_or_else(|| io::Error::other("serialized value exceeds its limit"))?;
        Ok(buffer.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn json_size(value: &impl Serialize, maximum: usize) -> Result<usize, ()> {
    let mut counter = ByteCounter { count: 0, maximum };
    serde_json::to_writer(&mut counter, value).map_err(|_| ())?;
    Ok(counter.count)
}
fn metadata_bytes(metadata: &ModelResponseMetadata) -> Option<usize> {
    [
        &metadata.provider_request_id,
        &metadata.reported_model_id,
        &metadata.reported_model_version,
    ]
    .into_iter()
    .flatten()
    .try_fold(0_usize, |size, id| size.checked_add(id.as_str().len()))
}
```

## `crates/wickle/src/model_routing.rs`

```rust
use crate::{
    CatalogRequirements, ContractError, ErrorCode, Id, JsonDigest, ModelCatalogSnapshot,
    ModelFailureKind, ModelPurpose, ModelSupportStatus, PortFuture, ResolvedCatalogBinding,
    ResolvedModelRoute, RouteRequest, Scope, VersionPolicy, VersionedRef,
    serialization::data_digest,
};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Protected catalog-and-policy snapshot format.
pub const ROUTING_SNAPSHOT_VERSION: &str = "wickle.routing-snapshot.v1";
/// Maximum explicitly ordered fallback targets in one purpose rule.
pub const MAX_ROUTE_FALLBACKS: usize = 16;
/// Maximum profile-binding/purpose rules in one immutable policy.
pub const MAX_ROUTING_RULES: usize = 128;

/// Static selection constraints, distinct from current PolicyPort authorization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingRule {
    /// Profile's logical Host binding name.
    pub model_binding: Id,
    /// Agent, verification, or compaction invocation purpose.
    pub purpose: ModelPurpose,
    /// Exact default target, without implicit latest lookup.
    pub primary: VersionedRef,
    /// Exact permitted fallback targets, visited only in this order.
    pub fallbacks: Vec<VersionedRef>,
    /// Explicit classified failures permitting progression to later targets.
    pub fallback_on: Vec<ModelFailureKind>,
    /// Immutable target requirement; a request can tighten but never relax it.
    #[serde(default)]
    pub version_policy: VersionPolicy,
    /// Required evidence stage. Execution defaults to contract-tested support.
    #[serde(default = "contract_tested")]
    pub min_support: ModelSupportStatus,
}

fn contract_tested() -> ModelSupportStatus {
    ModelSupportStatus::ContractTested
}

/// Exact-scope policy owned by the Host. Target/data boundaries are the registered
/// revisions in each rule, including their provider, connection, API and target.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingPolicy {
    /// Immutable policy revision.
    pub revision: Id,
    /// Exact tenant/workspace/user namespace.
    pub scope: Scope,
    /// Unique logical binding/purpose rules, with no implicit purpose fallback.
    pub rules: Vec<RoutingRule>,
}

/// Privately owned catalog and policy for admission and resume. Serialize only
/// into Host configuration or protected storage; Debug omits catalog payloads.
#[derive(Clone, PartialEq, Serialize)]
pub struct RoutingSnapshot {
    schema_version: String,
    catalog: ModelCatalogSnapshot,
    policy: RoutingPolicy,
}

impl fmt::Debug for RoutingSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoutingSnapshot")
            .field("catalog_revision", &self.catalog.revision)
            .field("policy_revision", &self.policy.revision)
            .field("rule_count", &self.policy.rules.len())
            .finish_non_exhaustive()
    }
}

impl RoutingSnapshot {
    /// Validate and own exact metadata, without retrieving a newer catalog.
    pub fn new(
        catalog: ModelCatalogSnapshot,
        policy: RoutingPolicy,
    ) -> Result<Self, ContractError> {
        catalog.validate()?;
        if policy.scope != catalog.scope {
            return Err(error(ErrorCode::AccessDenied, "routing.scope"));
        }
        if policy.rules.is_empty() || policy.rules.len() > MAX_ROUTING_RULES {
            return Err(error(ErrorCode::ModelRoutingInvalid, "routing.rules"));
        }
        for (index, rule) in policy.rules.iter().enumerate() {
            if policy.rules[..index].iter().any(|other| {
                other.model_binding == rule.model_binding && other.purpose == rule.purpose
            }) || rule.fallbacks.len() > MAX_ROUTE_FALLBACKS
                || rule
                    .fallback_on
                    .iter()
                    .enumerate()
                    .any(|(index, kind)| rule.fallback_on[..index].contains(kind))
            {
                return Err(error(ErrorCode::ModelRoutingInvalid, "routing.rule"));
            }
            let candidates: Vec<_> = std::iter::once(&rule.primary)
                .chain(&rule.fallbacks)
                .collect();
            for (index, reference) in candidates.iter().enumerate() {
                if candidates[..index].contains(reference) {
                    return Err(error(
                        ErrorCode::ModelRoutingInvalid,
                        "routing.duplicate_candidate",
                    ));
                }
                if !catalog
                    .bindings
                    .iter()
                    .any(|binding| &binding.binding == *reference)
                {
                    return Err(error(ErrorCode::ModelNotRegistered, "routing.binding"));
                }
            }
        }
        Ok(Self {
            schema_version: ROUTING_SNAPSHOT_VERSION.into(),
            catalog,
            policy,
        })
    }
    /// Exact immutable catalog, including all model and binding revisions.
    pub fn catalog(&self) -> &ModelCatalogSnapshot {
        &self.catalog
    }
    /// Exact immutable policy; this does not grant current access permission.
    pub fn policy(&self) -> &RoutingPolicy {
        &self.policy
    }
    /// Exact data namespace shared by catalog and policy.
    pub fn scope(&self) -> &Scope {
        &self.catalog.scope
    }
    /// Digest of the complete protected snapshot.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }
    /// Reconstruct one exact policy-listed route from saved metadata. This checks
    /// membership, not current access, lifecycle or invocation requirements.
    pub fn route_for_binding(
        &self,
        reference: &VersionedRef,
    ) -> Result<ResolvedModelRoute, ContractError> {
        if !self
            .policy
            .rules
            .iter()
            .any(|rule| &rule.primary == reference || rule.fallbacks.contains(reference))
        {
            return Err(error(ErrorCode::ModelRouteDenied, "routing.binding"));
        }
        let resolved = self.resolved_binding(reference)?;
        Ok(ResolvedModelRoute {
            binding: resolved.binding.binding.clone(),
            catalog_revision: self.catalog.revision.clone(),
            routing_policy_revision: self.policy.revision.clone(),
            requested_model: resolved.binding.requested_model.clone(),
            model_id: resolved.model.model_id.clone(),
            model_version: resolved.model.model_version.clone(),
            version_semantics: resolved.effective_version_semantics(),
            provider: resolved.model.provider.clone(),
            target: resolved.binding.target.clone(),
            deployment_revision: resolved.binding.deployment_revision.clone(),
            api_contract: resolved.binding.api_contract.clone(),
            adapter: resolved.binding.adapter.clone(),
            capability_revision: resolved.binding.capabilities.revision.clone(),
            connection_ref: resolved.binding.connection_ref.clone(),
        })
    }
    /// Check a saved route's entire identity against a policy-listed catalog binding.
    /// Historical membership does not establish current availability or permission.
    pub fn validate_route(&self, route: &ResolvedModelRoute) -> Result<(), ContractError> {
        if self.route_for_binding(&route.binding)? != *route {
            return Err(error(ErrorCode::ModelRoutingMismatch, "routing.route"));
        }
        Ok(())
    }
    /// Validate a custom router result before trusting its target or provenance.
    /// Enforces the same static constraints and ordered progression as the policy.
    pub fn validate_selection(
        &self,
        request: &RouteRequest,
        selection: &RouteSelection,
    ) -> Result<(), ContractError> {
        if &request.scope != self.scope() {
            return Err(error(ErrorCode::AccessDenied, "routing.scope"));
        }
        if selection.routing_snapshot_digest != self.digest()
            || selection.request_digest != request.digest()
        {
            return Err(error(
                ErrorCode::ModelRoutingMismatch,
                "routing.selection_digest",
            ));
        }
        let rule = self
            .policy
            .rules
            .iter()
            .find(|rule| {
                rule.model_binding == request.model_binding && rule.purpose == request.purpose
            })
            .ok_or_else(|| error(ErrorCode::ModelRouteDenied, "routing.rule"))?;
        if rule.min_support < ModelSupportStatus::ContractTested {
            return Err(error(
                ErrorCode::ModelSupportInsufficient,
                "routing.min_support",
            ));
        }
        let candidates: Vec<_> = std::iter::once(&rule.primary)
            .chain(&rule.fallbacks)
            .collect();
        let selected = candidates
            .get(selection.candidate_index)
            .ok_or_else(|| error(ErrorCode::ModelRoutingMismatch, "routing.candidate_index"))?;
        if selection.route.binding != **selected {
            return Err(error(
                ErrorCode::ModelRoutingMismatch,
                "routing.candidate_index",
            ));
        }
        self.validate_route(&selection.route)?;
        let previous = match &request.previous_route {
            Some(route) => {
                self.validate_route(route)?;
                Some(
                    candidates
                        .iter()
                        .position(|candidate| **candidate == route.binding)
                        .ok_or_else(|| {
                            error(ErrorCode::ModelRoutingMismatch, "routing.previous_route")
                        })?,
                )
            }
            None => None,
        };
        let earlier = match (previous, request.previous_failure, selection.reason) {
            (None, None, RouteSelectionReason::Initial) if selection.candidate_index == 0 => None,
            (Some(index), None, RouteSelectionReason::Reuse)
                if selection.candidate_index == index =>
            {
                None
            }
            (Some(index), Some(failure), RouteSelectionReason::Fallback { failure: reason })
                if failure == reason && selection.candidate_index > index =>
            {
                if !rule.fallback_on.contains(&failure) {
                    return Err(error(
                        ErrorCode::ModelRouteDenied,
                        "routing.fallback_reason",
                    ));
                }
                Some(index + 1)
            }
            _ => {
                return Err(error(
                    ErrorCode::ModelRoutingMismatch,
                    "routing.selection_reason",
                ));
            }
        };
        if !request.allowed_bindings.contains(&selected.id) {
            return Err(error(
                ErrorCode::ModelRouteDenied,
                "routing.allowed_bindings",
            ));
        }
        let requirements = CatalogRequirements {
            features: request.required_capabilities.clone(),
            options: request.options.clone(),
            input_tokens: request.input_tokens,
            max_output_tokens: request.max_output_tokens,
            version_policy: if rule.version_policy == VersionPolicy::RequirePinned
                || request.version_policy == VersionPolicy::RequirePinned
            {
                VersionPolicy::RequirePinned
            } else {
                VersionPolicy::AllowMutable
            },
            min_support: rule.min_support,
        };
        self.resolved_binding(selected)?.validate(&requirements)?;
        if let Some(start) = earlier {
            for candidate in &candidates[start..selection.candidate_index] {
                if request.allowed_bindings.contains(&candidate.id)
                    && self
                        .resolved_binding(candidate)?
                        .validate(&requirements)
                        .is_ok()
                {
                    return Err(error(
                        ErrorCode::ModelRoutingMismatch,
                        "routing.candidate_order",
                    ));
                }
            }
        }
        Ok(())
    }

    fn resolved_binding(
        &self,
        reference: &VersionedRef,
    ) -> Result<ResolvedCatalogBinding, ContractError> {
        let binding = self
            .catalog
            .bindings
            .iter()
            .find(|binding| &binding.binding == reference)
            .ok_or_else(|| error(ErrorCode::ModelNotRegistered, "routing.binding"))?;
        let model = self
            .catalog
            .models
            .iter()
            .find(|model| model.reference() == binding.model)
            .ok_or_else(|| error(ErrorCode::ModelNotRegistered, "routing.model"))?;
        Ok(ResolvedCatalogBinding {
            catalog_revision: self.catalog.revision.clone(),
            model: model.clone(),
            binding: binding.clone(),
        })
    }
    /// Restore against a trusted scope and digest, then revalidate the saved data.
    pub fn restore(
        input: &str,
        scope: &Scope,
        expected_digest: &JsonDigest,
    ) -> Result<Self, ContractError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Data {
            schema_version: String,
            catalog: ModelCatalogSnapshot,
            policy: RoutingPolicy,
        }
        let value = crate::parse_json(input)?;
        if value
            .get("schema_version")
            .and_then(serde_json::Value::as_str)
            != Some(ROUTING_SNAPSHOT_VERSION)
        {
            return Err(error(
                ErrorCode::UnsupportedSchemaVersion,
                "routing.schema_version",
            ));
        }
        if &crate::canonical_digest(&value) != expected_digest {
            return Err(error(ErrorCode::ModelRoutingMismatch, "routing.digest"));
        }
        let data: Data = serde_json::from_value(value)
            .map_err(|_| error(ErrorCode::ModelRoutingInvalid, "routing.snapshot"))?;
        if &data.catalog.scope != scope || &data.policy.scope != scope {
            return Err(error(ErrorCode::AccessDenied, "routing.scope"));
        }
        let snapshot = Self::new(data.catalog, data.policy)?;
        if data.schema_version != snapshot.schema_version || &snapshot.digest() != expected_digest {
            return Err(error(ErrorCode::ModelRoutingMismatch, "routing.digest"));
        }
        Ok(snapshot)
    }
}

/// Why a route was selected, separate from provider-reported metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RouteSelectionReason {
    /// First choice of the exact default target.
    Initial,
    /// Revalidation of a saved exact route, without progression.
    Reuse,
    /// Explicit progression after a policy-permitted failure.
    Fallback {
        /// The caller's classified failure; the router does not invoke a model to obtain it.
        failure: ModelFailureKind,
    },
}

/// One static selection. A driver must still check current authorization, target
/// availability, projection compatibility and the shared Run budget before I/O.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteSelection {
    /// Complete immutable provider/model/API/target/connection selection.
    pub route: ResolvedModelRoute,
    /// Initial choice, exact reuse, or explicit fallback reason.
    pub reason: RouteSelectionReason,
    /// Zero denotes primary; larger indices follow the finite policy list.
    pub candidate_index: usize,
    /// Identity of the saved catalog and policy used for this decision.
    pub routing_snapshot_digest: JsonDigest,
    /// Identity of the full RouteRequest, including options and allowed targets.
    pub request_digest: JsonDigest,
}

/// Static routing port. It never generates model output or executes a Tool.
pub trait ModelRouter: Send + Sync {
    /// Immutable metadata used by this instance; resume must use the saved snapshot.
    fn snapshot(&self) -> &RoutingSnapshot;
    /// Select or revalidate a route. The caller supplies the latest saved previous
    /// route and explicit failure; the router does not track attempts or retry I/O.
    fn resolve<'a>(&'a self, request: &'a RouteRequest) -> PortFuture<'a, RouteSelection>;
}

impl RouteRequest {
    /// Identity of every routing input, including scope, purpose, limits and options.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }
}

fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
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
        }
    }
    /// Inspect the full arguments to check actual target existence and ownership.
    pub fn execution_args(&self) -> &JsonObject {
        &self.execution_args
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
        /// Idempotent command identity.
        command_id: Id,
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
    /// Read one registered system key before the final target value is known.
    ResolveSystemInput {
        /// Exact tool requesting the lookup.
        tool: VersionedRef,
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

## `crates/wickle/src/run.rs`

```rust
use crate::{
    ArtifactRef, AttemptReservation, CompletionPolicy, ContractError, ErrorCode, Failure, Id,
    InputContent, JsonDigest, JsonObject, ModelAttemptState, ModelInvocationRecord, RecordRef,
    ReservationKind, ResolvedProfile, RunLimits, RunTiming, Scope, ToolCall, ToolResult,
    VersionedRef,
    serialization::{data_digest, decode, optional},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
};

/// Current run checkpoint format, independent of profile and event formats.
pub const RUN_SNAPSHOT_SCHEMA_VERSION: &str = "wickle.run-snapshot.v1";
/// Current durable event format.
pub const RUN_EVENT_SCHEMA_VERSION: &str = "wickle.run-event.v1";

/// Supported run checkpoint versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunSnapshotSchemaVersion {
    /// First checkpoint format.
    #[serde(rename = "wickle.run-snapshot.v1")]
    V1,
}

/// Supported session checkpoint versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionSchemaVersion {
    /// First session format.
    #[serde(rename = "wickle.session-snapshot.v1")]
    V1,
}

/// Supported durable event versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunEventSchemaVersion {
    /// First durable event format.
    #[serde(rename = "wickle.run-event.v1")]
    V1,
}

/// Why a Host submitted a run. Trigger data does not authenticate its sender.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunTrigger {
    /// Direct user request.
    User {},
    /// An event verified by the Host.
    Event {
        /// Host event identity.
        source_id: Id,
    },
    /// A schedule occurrence computed by the Host.
    Schedule {
        /// Occurrence identity, not a cron expression for the core to run.
        source_id: Id,
    },
    /// Child execution requested by a Host orchestration layer.
    Child {
        /// Parent run identity. Execution capability is checked separately.
        parent_run_id: Id,
    },
}

/// Caller request data; trusted execution context is supplied separately.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunRequest {
    /// Host-generated idempotency identity within scope and session.
    pub request_id: Id,
    /// Session whose pinned profile will be used.
    pub session_id: Id,
    /// User data, without injected tool calls or provider continuation state.
    pub input: Vec<InputContent>,
    /// Verified trigger provenance.
    pub trigger: RunTrigger,
    /// Logical model options authorized by the Host and pinned with the admitted request.
    /// Catalog schemas define supported keys; credentials and raw provider bodies do not belong here.
    #[serde(default, skip_serializing_if = "JsonObject::is_empty")]
    pub model_options: JsonObject,
    /// Optional output override; Host policy must authorize its use.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub output_contract: Option<crate::OutputContract>,
}

impl RunRequest {
    /// Decode caller data without granting authority or creating a run.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        decode(input, None)
    }
}

/// Exact request for human input, bound to the originating call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputRequest {
    /// Stable input request identity.
    pub input_request_id: Id,
    /// Call that must receive the answer.
    pub call_id: Id,
    /// Question shown by the Host.
    pub question: String,
    /// Optional exact schema for the answer.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub schema_ref: Option<VersionedRef>,
}

/// Exact operation or candidate to which approval applies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApprovalTarget {
    /// Tool approval binds the final execution input digest.
    Tool {
        /// Core call identity.
        call_id: Id,
        /// Digest that includes system-owned inputs.
        binding_digest: JsonDigest,
    },
    /// Review of a fixed candidate.
    Candidate {
        /// Stored candidate identity.
        candidate_ref: RecordRef,
        /// Exact verifier definition.
        verifier_ref: VersionedRef,
    },
}

/// Typed reason a run waits without making additional model calls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitTarget {
    /// Explicit approval of fixed data.
    Approval {
        /// Approval target.
        target: ApprovalTarget,
    },
    /// Answer to a recorded input request.
    Input {
        /// Input request.
        request: InputRequest,
    },
    /// Confirmation of an uncertain external effect.
    External {
        /// Call with uncertain effect.
        call_id: Id,
        /// Stable external idempotency/reconciliation key.
        effect_key: Id,
    },
}

/// Saved wait identity, target, and optional expiry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitState {
    /// Unique wait identity used to reject stale answers.
    pub wait_id: Id,
    /// Data or effect being awaited.
    pub target: WaitTarget,
    /// UTC milliseconds since Unix epoch; the run deadline still applies.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub expires_at_ms: Option<i64>,
}

/// A specific answer or recovery request; none of these grant execution permission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResumeAction {
    /// Accept a fixed approval target.
    Approve {
        /// Matching wait identity.
        wait_id: Id,
        /// Exact target presented for approval.
        target: ApprovalTarget,
    },
    /// Reject a fixed approval target.
    Deny {
        /// Matching wait identity.
        wait_id: Id,
        /// Exact target presented for approval.
        target: ApprovalTarget,
        /// User-supplied rejection reason.
        reason: String,
    },
    /// Supply data for a recorded input request.
    Input {
        /// Matching wait identity.
        wait_id: Id,
        /// Answer data, validated against the saved request by the resume handler.
        answer: serde_json::Value,
    },
    /// Supply a protected receipt for an external effect.
    External {
        /// Matching wait identity.
        wait_id: Id,
        /// Evidence to be verified by the authorized handler.
        receipt_ref: RecordRef,
    },
    /// Resume an interrupted nonterminal execution.
    Recover {
        /// Host-verified recovery evidence.
        recovery_ref: RecordRef,
    },
}

/// Idempotent resume command; state/policy enforcement is performed by the driver.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeCommand {
    /// Run to resume.
    pub run_id: Id,
    /// Revision the caller observed.
    pub expected_revision: u64,
    /// Deduplicates retries of the same decision.
    pub command_id: Id,
    /// Typed decision or recovery evidence.
    pub action: ResumeAction,
}

impl ResumeCommand {
    /// Decode an unambiguous command without executing or authorizing it.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        decode(input, None)
    }
}

/// Public run status categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Actively processing.
    Running,
    /// Persisted wait.
    Waiting,
    /// Completion policy satisfied.
    Succeeded,
    /// Unrecoverable failure.
    Failed,
    /// Explicit cancellation completed.
    Cancelled,
    /// A finite execution budget was exhausted.
    Exhausted,
}

impl RunStatus {
    /// Whether this status cannot be resumed as the same run.
    pub fn is_terminal(self) -> bool {
        !matches!(self, Self::Running | Self::Waiting)
    }
}

/// Driver phases; transition execution belongs to the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPhase {
    /// Admission validation and initial storage.
    Admission,
    /// Context and request preparation.
    Prepare,
    /// One model invocation.
    Model,
    /// Tool round processing.
    Tool,
    /// Output and completion checks.
    Verify,
    /// Saved wait.
    Waiting,
    /// Terminal outcome committed.
    Finish,
}

/// Stored budget consumption; usage measurement and reservation happen elsewhere.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetUsage {
    /// Reserved physical model attempts.
    pub model_calls: u64,
    /// Reserved physical tool attempts.
    pub tool_attempts: u64,
    /// Candidate repair attempts.
    pub repair_attempts: u64,
    /// Execution recovery attempts.
    pub recovery_attempts: u64,
    /// Elapsed milliseconds including waits.
    pub elapsed_ms: u64,
}

/// The budget that stopped an execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetKind {
    /// Model attempts.
    ModelCalls,
    /// Tool attempts.
    ToolAttempts,
    /// Repairs.
    RepairAttempts,
    /// Recoveries.
    RecoveryAttempts,
    /// Elapsed wall time.
    Elapsed,
}

/// What supports a successful outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionBasis {
    /// The model ended its turn; external business success is not asserted.
    TurnEnded,
    /// A pinned verifier accepted the candidate.
    Verified,
}

/// Recorded verifier classification, separate from transport errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationVerdict {
    /// Candidate accepted.
    Pass,
    /// Candidate needs revision.
    Revise,
    /// Human review required.
    Wait,
    /// Candidate rejected.
    Fail,
}

/// Evidence supporting the recorded verifier decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationSummary {
    /// Verifier actually used.
    pub verifier_ref: VersionedRef,
    /// Exact evaluation criteria.
    pub criteria_ref: VersionedRef,
    /// Decision classification.
    pub verdict: VerificationVerdict,
    /// Protected evidence records.
    pub evidence: Vec<RecordRef>,
}

/// Outcome-specific data. Success always names its completion basis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutcomeResult {
    /// Saved waiting outcome for the current execution segment.
    Waiting {
        /// Wait data.
        wait: WaitState,
    },
    /// Completion policy satisfied.
    Succeeded {
        /// Why completion was accepted.
        completion_basis: CompletionBasis,
    },
    /// Execution failed.
    Failed {
        /// Classified failure.
        failure: Failure,
    },
    /// Cancellation completed; existing effects remain in their records.
    Cancelled {
        /// Cancellation reason.
        reason: String,
    },
    /// Execution budget exhausted.
    Exhausted {
        /// Exhausted budget.
        budget: BudgetKind,
    },
}

impl OutcomeResult {
    /// Public status of this outcome.
    pub fn status(&self) -> RunStatus {
        match self {
            Self::Waiting { .. } => RunStatus::Waiting,
            Self::Succeeded { .. } => RunStatus::Succeeded,
            Self::Failed { .. } => RunStatus::Failed,
            Self::Cancelled { .. } => RunStatus::Cancelled,
            Self::Exhausted { .. } => RunStatus::Exhausted,
        }
    }
}

/// Stored outcome; it is the authority for completion, not an event or text delta.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunOutcome {
    /// Outcome classification and required status-specific data.
    pub result: OutcomeResult,
    /// Final or partial output.
    pub output: Vec<InputContent>,
    /// Produced artifact metadata.
    pub artifacts: Vec<ArtifactRef>,
    /// Consumption recorded at this checkpoint.
    pub usage: BudgetUsage,
    /// Exact checkpoint revision.
    pub checkpoint_revision: u64,
    /// Optional verifier evidence; mandatory for verified success.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub verification: Option<VerificationSummary>,
    /// Effects that must not be blindly repeated.
    pub unresolved_effects: Vec<RecordRef>,
}

impl RunOutcome {
    /// Check required evidence for a verified success.
    pub fn validate(&self) -> Result<(), ContractError> {
        if matches!(self.result, OutcomeResult::Succeeded { .. })
            && !self.unresolved_effects.is_empty()
        {
            return Err(ContractError::new(
                ErrorCode::InvalidContract,
                "outcome.unresolved_effects",
            ));
        }
        if matches!(
            self.result,
            OutcomeResult::Succeeded {
                completion_basis: CompletionBasis::Verified
            }
        ) && !self
            .verification
            .as_ref()
            .is_some_and(|v| v.verdict == VerificationVerdict::Pass)
        {
            return Err(ContractError::new(
                ErrorCode::InvalidContract,
                "outcome.verification",
            ));
        }
        Ok(())
    }
}

/// State of one planned tool call; this does not execute state transitions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolCallState {
    /// Plan is saved and no dispatch is recorded.
    Planned {},
    /// Dispatch was reserved and may have happened.
    Dispatching {
        /// Physical attempt identity.
        attempt_id: Id,
        /// Stable key for external deduplication/reconciliation.
        idempotency_key: Id,
    },
    /// Result was recorded.
    Settled {
        /// Paired tool result.
        result: ToolResult,
    },
    /// Effect is unknown after interruption.
    Unknown {
        /// Uncertain attempt identity.
        attempt_id: Id,
        /// Original external effect key.
        idempotency_key: Id,
    },
}

/// Planned model arguments and the corresponding dispatch/result state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolLedgerEntry {
    /// Original call and protected bound-input reference.
    pub call: ToolCall,
    /// Dispatch/result state.
    pub state: ToolCallState,
}

/// Protected system-input storage reference and versions used in request identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemInputSnapshotRef {
    /// Protected storage location; excluded from logical request identity.
    pub snapshot_ref: RecordRef,
    /// Digest of the validated owned values, including an explicit empty map.
    pub values_digest: JsonDigest,
    /// Exact registered input-definition versions.
    pub definition_versions: BTreeMap<Id, Id>,
}

/// Digest of logical start input, independent of a storage record's location.
/// A missing system-input map is the empty map for start; resume preserves the
/// separate missing/empty distinction in ExecutionContextData.
pub fn admission_digest(
    request: &RunRequest,
    profile: &ResolvedProfile,
    system_inputs: Option<&SystemInputSnapshotRef>,
) -> JsonDigest {
    let empty_digest = crate::canonical_digest(&serde_json::json!({}));
    let empty_versions = BTreeMap::new();
    let (values, versions) = system_inputs
        .map(|s| (&s.values_digest, &s.definition_versions))
        .unwrap_or((&empty_digest, &empty_versions));
    data_digest(&(request, profile.profile_digest(), values, versions))
}

/// Session metadata pinned across requests. A store enforces the active-run rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSnapshot {
    /// Session document version.
    pub schema_version: SessionSchemaVersion,
    /// Session identity.
    pub session_id: Id,
    /// Owning scope.
    pub scope: Scope,
    /// Pinned profile identity.
    pub profile_digest: JsonDigest,
    /// Pinned prompt data reference.
    pub prompt_snapshot: RecordRef,
    /// Current transcript revision.
    pub transcript_revision: u64,
    /// One active running/waiting run, or omission when none exists.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub active_run_id: Option<Id>,
}

/// Saved context-source execution position for retry and resume reuse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceExecutionState {
    /// Exact source selection, including adapter binding when applicable.
    pub source: crate::ContextSourceRef,
    /// Stable context request identity.
    pub context_request_id: Id,
    /// Collection trigger.
    pub trigger: crate::ContextTrigger,
    /// Required for a before_model collection.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub model_step_id: Option<Id>,
    /// Committed context batch, including empty/unavailable results.
    pub batch_ref: RecordRef,
}

/// Run checkpoint DTO. Use `from_json` or `validate` at the storage boundary.
/// Protected inputs are references, not automatically exposed execution arguments.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSnapshot {
    /// Checkpoint document version.
    pub schema_version: RunSnapshotSchemaVersion,
    /// Run identity.
    pub run_id: Id,
    /// Original caller request.
    pub request: RunRequest,
    /// Logical input digest used for deduplication.
    pub request_digest: JsonDigest,
    /// Scope used for storage, policy, tools, and resume.
    pub scope: Scope,
    /// Immutable profile and resolved definition identities.
    pub profile: ResolvedProfile,
    /// Current execution status.
    pub status: RunStatus,
    /// Current driver phase.
    pub phase: RunPhase,
    /// Current logical model step, if allocated.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub model_step_id: Option<Id>,
    /// Effective limits, no greater than profile limits.
    pub limits: RunLimits,
    /// Saved usage/reservations.
    pub usage: BudgetUsage,
    /// Original admission/deadline and persisted monotonic elapsed-time anchor.
    pub timing: RunTiming,
    /// Append-only charged attempt reservations, preserved across errors and resume.
    pub reservations: Vec<AttemptReservation>,
    /// Physical model attempt records.
    pub model_ledger: Vec<ModelInvocationRecord>,
    /// Saved tool plans and states.
    pub tool_ledger: Vec<ToolLedgerEntry>,
    /// Pinned, protected system values and their contract revisions.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub system_inputs: Option<SystemInputSnapshotRef>,
    /// Saved wait data, only while waiting.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub wait: Option<WaitState>,
    /// Last waiting or terminal outcome.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub outcome: Option<RunOutcome>,
    /// Pinned assembly metadata, without process-local handler objects.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub assembly_ref: Option<RecordRef>,
    /// Immutable catalog and routing policy used by this run's model calls.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub routing_snapshot_ref: Option<RecordRef>,
    /// Committed context batches.
    pub context_batches: Vec<RecordRef>,
    /// Saved collection positions.
    pub source_states: Vec<SourceExecutionState>,
    /// Compare-and-swap revision.
    pub revision: u64,
    /// Last durable event sequence; ephemeral deltas do not consume it.
    pub last_event_seq: u64,
}

impl RunSnapshot {
    /// Decode a known checkpoint version and verify static consistency.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        let snapshot: Self = decode(input, Some(RUN_SNAPSHOT_SCHEMA_VERSION))?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Check static checkpoint invariants without performing recovery or authorization.
    pub fn validate(&self) -> Result<(), ContractError> {
        let invalid = |path| ContractError::new(ErrorCode::InvalidSnapshot, path);
        crate::budget::validate_budget(self)?;
        if &self.scope != self.profile.scope()
            || self.request_digest
                != admission_digest(&self.request, &self.profile, self.system_inputs.as_ref())
        {
            return Err(invalid("request_digest"));
        }
        let requested = &self.profile.profile().limits;
        if self.limits.max_model_calls > requested.max_model_calls
            || self.limits.max_tool_attempts > requested.max_tool_attempts
            || self.limits.max_repair_attempts > requested.max_repair_attempts
            || self.limits.max_recovery_attempts > requested.max_recovery_attempts
            || self.limits.max_elapsed_ms > requested.max_elapsed_ms
        {
            return Err(invalid("limits"));
        }
        match self.status {
            RunStatus::Running
                if matches!(self.phase, RunPhase::Waiting | RunPhase::Finish)
                    || self.wait.is_some()
                    || self.outcome.is_some() =>
            {
                return Err(invalid("status"));
            }
            RunStatus::Waiting if self.phase != RunPhase::Waiting || self.wait.is_none() => {
                return Err(invalid("wait"));
            }
            s if s.is_terminal()
                && (self.phase != RunPhase::Finish
                    || self.wait.is_some()
                    || self.outcome.is_none()) =>
            {
                return Err(invalid("outcome"));
            }
            _ => {}
        }
        if let Some(outcome) = &self.outcome {
            outcome.validate()?;
            if outcome.result.status() != self.status
                || outcome.checkpoint_revision != self.revision
                || outcome.usage != self.usage
            {
                return Err(invalid("outcome"));
            }
            if let OutcomeResult::Waiting { wait } = &outcome.result {
                if self.wait.as_ref() != Some(wait) {
                    return Err(invalid("outcome.wait"));
                }
            }
            if let OutcomeResult::Succeeded { completion_basis } = &outcome.result {
                match (&self.profile.profile().completion_policy, completion_basis) {
                    (CompletionPolicy::TurnEnd {}, CompletionBasis::TurnEnded) => {}
                    (CompletionPolicy::Verified { verifier_ref }, CompletionBasis::Verified)
                        if outcome
                            .verification
                            .as_ref()
                            .is_some_and(|v| &v.verifier_ref == verifier_ref) => {}
                    _ => return Err(invalid("outcome.completion_basis")),
                }
            }
        }
        let mut calls = BTreeSet::new();
        for entry in &self.tool_ledger {
            if !calls.insert(&entry.call.call_id) {
                return Err(invalid("tool_ledger.call_id"));
            }
            match &entry.state {
                ToolCallState::Dispatching { .. } | ToolCallState::Unknown { .. }
                    if entry.call.bound_input_ref.is_none() =>
                {
                    return Err(invalid("tool_ledger.bound_input_ref"));
                }
                ToolCallState::Settled { result } if result.call_id != entry.call.call_id => {
                    return Err(invalid("tool_ledger.result.call_id"));
                }
                _ => {}
            }
            if self.status == RunStatus::Succeeded
                && !matches!(&entry.state, ToolCallState::Settled { result } if result.status != crate::ToolResultStatus::Unknown)
            {
                return Err(invalid("tool_ledger.unsettled"));
            }
        }
        let mut attempts = BTreeSet::new();
        let model_reservations: BTreeMap<_, _> = self
            .reservations
            .iter()
            .filter_map(|reservation| match reservation.kind {
                ReservationKind::Model { purpose } => Some((&reservation.attempt_id, purpose)),
                _ => None,
            })
            .collect();
        for invocation in &self.model_ledger {
            if invocation.run_id != self.run_id || !attempts.insert(&invocation.attempt_id) {
                return Err(invalid("model_ledger.attempt_id"));
            }
            if model_reservations.get(&invocation.attempt_id) != Some(&invocation.purpose) {
                return Err(invalid("model_ledger.reservation"));
            }
            let settled = matches!(
                invocation.state,
                ModelAttemptState::Completed {} | ModelAttemptState::Failed { .. }
            );
            if settled != invocation.response_ref.is_some() {
                return Err(invalid("model_ledger.response_ref"));
            }
        }
        if self
            .source_states
            .iter()
            .any(|s| (s.trigger == crate::ContextTrigger::BeforeModel) != s.model_step_id.is_some())
        {
            return Err(invalid("source_states.model_step_id"));
        }
        Ok(())
    }
}

/// Durable facts reference stored records rather than copying protected inputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum RunEventPayload {
    /// Admission committed.
    #[serde(rename = "run.started")]
    RunStarted {
        /// Accepted request reference.
        request_ref: RecordRef,
        /// Pinned profile identity.
        profile_digest: JsonDigest,
    },
    /// Tool plan committed.
    #[serde(rename = "tool.planned")]
    ToolPlanned {
        /// Protected call record.
        call_ref: RecordRef,
    },
    /// Tool result committed.
    #[serde(rename = "tool.settled")]
    ToolSettled {
        /// Protected result record.
        result_ref: RecordRef,
    },
    /// Verifier decision committed.
    #[serde(rename = "verification.completed")]
    VerificationCompleted {
        /// Recorded verification evidence.
        verification_ref: RecordRef,
    },
    /// Wait committed.
    #[serde(rename = "run.waiting")]
    RunWaiting {
        /// Recorded wait.
        wait_ref: RecordRef,
    },
    /// Resume command consumed.
    #[serde(rename = "run.resumed")]
    RunResumed {
        /// Consumed command record.
        command_ref: RecordRef,
    },
    /// Terminal outcome committed.
    #[serde(rename = "run.finished")]
    RunFinished {
        /// Authoritative outcome record.
        outcome_ref: RecordRef,
    },
    /// Model route and invocation identity committed.
    #[serde(rename = "model.route_selected")]
    ModelRouteSelected {
        /// Invocation record.
        invocation_ref: RecordRef,
        /// Selected route identity.
        route_digest: JsonDigest,
    },
}

/// A durable event committed atomically with authoritative state by the store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunEvent {
    /// Independent event wire version.
    pub schema_version: RunEventSchemaVersion,
    /// Stable event identity for deduplication.
    pub event_id: Id,
    /// Owning scope.
    pub scope: Scope,
    /// Owning run.
    pub run_id: Id,
    /// Owning session.
    pub session_id: Id,
    /// Positive durable event sequence.
    pub seq: NonZeroU64,
    /// UTC milliseconds since Unix epoch.
    pub timestamp_ms: i64,
    /// Typed event data with authorized record references.
    pub payload: RunEventPayload,
}

impl RunEvent {
    /// Decode a known event format without replaying or dispatching it.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        decode(input, Some(RUN_EVENT_SCHEMA_VERSION))
    }
}

/// Non-durable presentation hints; these carry no durable sequence or completion claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum EphemeralEvent {
    /// Candidate text from an incomplete model response.
    CandidateTextDelta {
        /// Owning run.
        run_id: Id,
        /// Physical model attempt.
        attempt_id: Id,
        /// Candidate text, not a committed final answer.
        text: String,
    },
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
pub use checkpoint::{STATE_STORE_CHECKPOINT_VERSION, StateStoreCheckpoint};

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
            validate_events(state, &additions, &input.snapshot, 0, &input.events, true)?;
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
            let additions = validate_records(state, &input.records)?;
            validate_snapshot_refs(state, &additions, &input.snapshot)?;
            validate_events(
                state,
                &additions,
                &input.snapshot,
                run.snapshot.last_event_seq,
                &input.events,
                false,
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
    let mut references = Vec::new();
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
    references.extend(snapshot.assembly_ref.iter());
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
            let references = match content {
                ContentBlock::Content { .. } => Vec::new(),
                ContentBlock::ToolCall { call } => call.bound_input_ref.iter().collect(),
                ContentBlock::ToolResult { result } => tool_result_refs(result),
                ContentBlock::ProviderOpaque { data_ref, .. } => vec![data_ref],
            };
            for reference in references {
                record_value(state, additions, reference)?;
            }
        }
    }
    Ok(sequence)
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

fn validate_events(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    previous_seq: u64,
    events: &[RunEvent],
    admission: bool,
) -> Result<(), ContractError> {
    let mut sequence = previous_seq;
    let mut seen = BTreeSet::new();
    let mut started = 0;
    let mut finished = 0;
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
                let command: ResumeCommand = event_record(state, additions, command_ref)?;
                let previous = state.runs.get(&snapshot.run_id).ok_or_else(not_found)?;
                if snapshot.status != RunStatus::Running
                    || command.run_id != snapshot.run_id
                    || command.expected_revision != previous.snapshot.revision
                    || !resume_target_matches(&previous.snapshot, &command.action)
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_resumed"));
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
    {
        return Err(error(ErrorCode::InvalidEvent, "events"));
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

fn resume_target_matches(previous: &RunSnapshot, action: &ResumeAction) -> bool {
    match action {
        ResumeAction::Recover { .. } => previous.status == RunStatus::Running,
        ResumeAction::Approve { wait_id, target }
        | ResumeAction::Deny {
            wait_id, target, ..
        } => previous.wait.as_ref().is_some_and(|wait| {
            &wait.wait_id == wait_id
                && matches!(&wait.target, WaitTarget::Approval { target: saved } if saved == target)
        }),
        ResumeAction::Input { wait_id, .. } => previous.wait.as_ref().is_some_and(|wait| {
            &wait.wait_id == wait_id && matches!(wait.target, WaitTarget::Input { .. })
        }),
        ResumeAction::External { wait_id, .. } => previous.wait.as_ref().is_some_and(|wait| {
            &wait.wait_id == wait_id && matches!(wait.target, WaitTarget::External { .. })
        }),
    }
}

fn validate_transition(previous: &RunSnapshot, next: &RunSnapshot) -> Result<(), ContractError> {
    if previous.status.is_terminal() {
        return Err(error(ErrorCode::InvalidTransition, "run.status"));
    }
    next.validate()?;
    crate::budget::validate_budget_transition(previous, next)?;
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
        || (previous.assembly_ref.is_some() && previous.assembly_ref != next.assembly_ref)
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
                ToolCallState::Dispatching { .. } | ToolCallState::Unknown { .. }
            ) && matches!(new.state, ToolCallState::Planned { .. }))
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
            },
            ToolCallState::Dispatching {
                attempt_id: new_attempt,
                idempotency_key: new_key,
            }
            | ToolCallState::Unknown {
                attempt_id: new_attempt,
                idempotency_key: new_key,
            },
        ) = (&old.state, &new.state)
        {
            let retry = matches!(old.state, ToolCallState::Unknown { .. })
                && matches!(new.state, ToolCallState::Dispatching { .. });
            if old_key != new_key || (!retry && old_attempt != new_attempt) {
                return Err(error(ErrorCode::InvalidTransition, "tool_ledger.attempt"));
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
                let command: ResumeCommand = event_record(state, &empty, command_ref)?;
                if command.run_id != run.snapshot.run_id
                    || command.expected_revision >= run.snapshot.revision
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
    {
        return Err(invalid("checkpoint.events"));
    }
    Ok(())
}

fn invalid(path: &str) -> ContractError {
    error(ErrorCode::InvalidSnapshot, path)
}
```

## `crates/wickle/tests/contracts.rs`

```rust
//! Behavioral checks for validation, persisted contracts, and profile identity.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};
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
fn reference(kind: ComponentKind, name: &str) -> ComponentRef {
    ComponentRef {
        kind,
        id: id(name),
        version: if kind == ComponentKind::ModelBinding || kind == ComponentKind::Extension {
            None
        } else {
            Some(id("1.0.0"))
        },
    }
}

fn profile_value() -> Value {
    json!({
        "schema_version": "wickle.agent-profile.v1", "agent_id": "research", "version": "1.0.0",
        "name": "Research assistant", "description": "Find information with sources",
        "instructions": {"text": "Use available evidence."}, "model_binding": "primary",
        "tools": [{"tool_id": "documents.search", "version": "1.0.0", "bindings": {"main": "knowledge"}, "config": {"limit": 5}}],
        "skills": [], "connectors": [{"binding_id": "knowledge", "connector_id": "document-store", "version": "1.0.0"}],
        "context_policy": {"strategy": "bounded"}, "output_contract": {"type": "text"},
        "limits": {"max_model_calls": 8, "max_tool_attempts": 12, "max_repair_attempts": 0, "max_recovery_attempts": 2, "max_elapsed_ms": 30000}
    })
}
fn profile() -> AgentProfile {
    AgentProfile::from_json(&profile_value().to_string()).unwrap()
}

struct Catalog(BTreeMap<ComponentRef, ComponentMetadata>);

impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        requested_scope: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            if requested_scope != &scope() {
                return Err(ContractError::new(ErrorCode::ComponentUnavailable, "scope"));
            }
            self.0
                .get(reference)
                .cloned()
                .ok_or_else(|| ContractError::new(ErrorCode::ComponentUnavailable, "reference"))
        })
    }
}

fn metadata(key: &ComponentRef) -> ComponentMetadata {
    let mut resolved = key.clone();
    resolved.version = Some(id("1.0.0"));
    ComponentMetadata {
        reference: resolved,
        contract_version: 1,
        manifest_digest: digest("manifest"),
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

fn catalog() -> Catalog {
    let mut definitions = BTreeMap::new();
    let model = reference(ComponentKind::ModelBinding, "primary");
    let mut model_meta = metadata(&model);
    model_meta.capabilities.insert(id("model.tool_calling"));
    definitions.insert(model, model_meta);
    let connector = reference(ComponentKind::Connector, "document-store");
    definitions.insert(connector.clone(), metadata(&connector));
    let tool = reference(ComponentKind::Tool, "documents.search");
    let mut tool_meta = metadata(&tool);
    tool_meta.config_schema = json!({"type":"object","properties":{"limit":{"type":"integer","minimum":1,"maximum":50}},"required":["limit"],"additionalProperties":false});
    tool_meta.required_connections.insert(id("main"));
    tool_meta
        .required_capabilities
        .insert(id("model.tool_calling"));
    tool_meta.capabilities.insert(id("documents.search"));
    tool_meta.model_name = Some(id("documents_search"));
    definitions.insert(tool, tool_meta);
    Catalog(definitions)
}

async fn resolved() -> ResolvedProfile {
    ProfileValidator::new(&catalog())
        .validate(&profile(), &scope())
        .await
        .unwrap()
}

#[test]
fn digest_matches_independent_sha256_vectors_and_sorts_nested_objects() {
    assert_eq!(
        canonical_digest_json("{}").unwrap().as_str(),
        "sorted-json-v1:sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
    );
    let a = r#"{"z":1,"a":{"x":[3,1],"b":2}}"#;
    let b = r#"{ "a": {"b":2,"x":[3,1]}, "z":1 }"#;
    let actual = canonical_digest_json(a).unwrap();
    assert_eq!(
        actual.as_str(),
        "sorted-json-v1:sha256:b2db09df32697403c319dfb8cd57f51a8400eb9943753795ccbb34d1383f01a2"
    );
    assert_eq!(actual, canonical_digest_json(b).unwrap());
    for changed in [
        r#"{"z":1,"a":{"x":[1,3],"b":2}}"#,
        r#"{"z":2,"a":{"x":[3,1],"b":2}}"#,
    ] {
        assert_ne!(actual, canonical_digest_json(changed).unwrap());
    }
    assert_ne!(
        canonical_digest_json("1").unwrap(),
        canonical_digest_json("1.0").unwrap()
    );
    assert_ne!(
        canonical_digest_json("0").unwrap(),
        canonical_digest_json("-0.0").unwrap()
    );
}

#[test]
fn ambiguous_and_non_json_input_is_rejected_instead_of_being_normalized() {
    for input in [
        "NaN",
        "Infinity",
        "-Infinity",
        "1e400",
        "undefined",
        r#"{"a":1,"a":2}"#,
        r#"{"nested":{"a":1,"a":2}}"#,
        "{} {}",
    ] {
        assert_eq!(
            canonical_digest_json(input).unwrap_err().code,
            ErrorCode::InvalidJson,
            "{input}"
        );
    }
}

#[test]
fn profile_rejects_unknown_fields_runtime_objects_invalid_limits_and_null_options() {
    let invalid = [
        ("api_key", json!("credential-value")),
        ("runtime_bindings", json!({"model":"client"})),
        ("sdk_client", json!({})),
        ("adapters", Value::Null),
        ("hooks", Value::Null),
        ("context_sources", Value::Null),
        ("extensions", Value::Null),
        (
            "instructions",
            json!({"text":"a","module_path":"untrusted-code"}),
        ),
        ("completion_policy", json!({"mode":"verified"})),
        (
            "completion_policy",
            json!({"mode":"turn_end","verifier_ref":{"id":"extra","version":"1"}}),
        ),
        (
            "output_contract",
            json!({"type":"text","schema_ref":{"id":"extra","version":"1"}}),
        ),
        (
            "tools",
            json!([{"tool_id":"documents.search","version":"1.0.0","adapter_binding":"mixed","export_id":"search"}]),
        ),
    ];
    for (key, value) in invalid {
        let mut input = profile_value();
        input[key] = value;
        assert_eq!(
            AgentProfile::from_json(&input.to_string())
                .expect_err(&format!("accepted invalid field: {key}"))
                .code,
            ErrorCode::InvalidContract,
            "{key}"
        );
    }
    for value in [json!(0), json!(-1), json!(1.5), Value::Null] {
        let mut input = profile_value();
        input["limits"]["max_model_calls"] = value;
        assert_eq!(
            AgentProfile::from_json(&input.to_string())
                .unwrap_err()
                .code,
            ErrorCode::InvalidContract
        );
    }
    for field in ["schema_version", "tools", "model_binding", "limits"] {
        let mut input = profile_value();
        input.as_object_mut().unwrap().remove(field);
        assert!(
            AgentProfile::from_json(&input.to_string()).is_err(),
            "missing {field}"
        );
    }
    let mut input = profile_value();
    input["schema_version"] = json!("wickle.agent-profile.v99");
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedSchemaVersion
    );
}

#[test]
fn optional_fields_preserve_presence_and_zero_means_disabled() {
    let absent = profile();
    assert_eq!(absent.completion_policy, CompletionPolicy::TurnEnd {});
    assert_eq!(absent.limits.max_repair_attempts, 0);
    let mut input = profile_value();
    input["adapters"] = json!([]);
    let empty = AgentProfile::from_json(&input.to_string()).unwrap();
    assert!(absent.adapters.is_none());
    assert_eq!(empty.adapters, Some(vec![]));
    assert_ne!(absent.digest(), empty.digest());
    assert_eq!(
        AgentProfile::from_json(&serde_json::to_string(&empty).unwrap()).unwrap(),
        empty
    );
}

#[test]
fn local_binding_errors_are_rejected_before_metadata_resolution() {
    let mut input = profile_value();
    input["tools"][0]["bindings"]["main"] = json!("missing");
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
    let mut input = profile_value();
    let duplicate = input["connectors"][0].clone();
    input["connectors"].as_array_mut().unwrap().push(duplicate);
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .path,
        "connectors.binding_id"
    );
    let mut input = profile_value();
    input["tools"] = json!([{"adapter_binding":"missing","export_id":"search"}]);
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .path,
        "adapter_binding"
    );
    let mut input = profile_value();
    input["context_policy"] = json!({"strategy":"custom"});
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .path,
        "context_policy.version"
    );
}

#[tokio::test]
async fn resolver_accepts_registered_components_and_freezes_their_full_definition_identity() {
    let profile = profile();
    let catalog = catalog();
    let pinned = ProfileValidator::new(&catalog)
        .validate(&profile, &scope())
        .await
        .unwrap();
    assert_eq!(pinned.components().len(), 3);
    assert!(
        pinned
            .components()
            .iter()
            .all(|c| c.reference.version.as_ref() == Some(&id("1.0.0")))
    );
    let restored: ResolvedProfile =
        serde_json::from_str(&serde_json::to_string(&pinned).unwrap()).unwrap();
    restored.ensure_matches(&profile, &scope()).unwrap();
    restored.ensure_same_resolution(&pinned).unwrap();
    let mut changed_catalog = catalog;
    changed_catalog
        .0
        .get_mut(&reference(ComponentKind::ModelBinding, "primary"))
        .unwrap()
        .manifest_digest = digest("new definition");
    let newer = ProfileValidator::new(&changed_catalog)
        .validate(&profile, &scope())
        .await
        .unwrap();
    assert_eq!(
        pinned.ensure_same_resolution(&newer).unwrap_err().code,
        ErrorCode::ProfileMismatch
    );
}

#[tokio::test]
async fn unavailable_dependencies_wrong_versions_and_missing_capabilities_fail_resolution() {
    let p = profile();
    let mut missing = catalog();
    missing
        .0
        .remove(&reference(ComponentKind::Tool, "documents.search"));
    assert_eq!(
        ProfileValidator::new(&missing)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    let mut wrong = catalog();
    wrong
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .reference
        .version = Some(id("2.0.0"));
    assert_eq!(
        ProfileValidator::new(&wrong)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    let mut unsupported = catalog();
    unsupported
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .contract_version = 2;
    assert_eq!(
        ProfileValidator::new(&unsupported)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedContractVersion
    );
    let mut no_capability = catalog();
    no_capability
        .0
        .get_mut(&reference(ComponentKind::ModelBinding, "primary"))
        .unwrap()
        .capabilities
        .clear();
    assert_eq!(
        ProfileValidator::new(&no_capability)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
    let mut no_dependency = catalog();
    no_dependency
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .dependencies
        .push(reference(ComponentKind::Tool, "skills.load"));
    assert_eq!(
        ProfileValidator::new(&no_dependency)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    let mut no_connection = p.clone();
    if let ToolBindingRef::Catalog(tool) = &mut no_connection.tools[0] {
        tool.bindings = None;
    }
    assert_eq!(
        ProfileValidator::new(&catalog())
            .validate(&no_connection, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
}

#[tokio::test]
async fn registered_configuration_schema_rejects_wrong_types_ranges_and_credential_fields() {
    for config in [
        json!({"limit":0}),
        json!({"limit":"5"}),
        json!({"limit":51}),
        json!({"limit":5,"api_key":"credential-value"}),
    ] {
        let mut input = profile_value();
        input["tools"][0]["config"] = config;
        let p = AgentProfile::from_json(&input.to_string()).unwrap();
        let error = ProfileValidator::new(&catalog())
            .validate(&p, &scope())
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidConfiguration);
        assert!(!error.to_string().contains("credential-value"));
        assert!(!format!("{error:?}").contains("credential-value"));
    }
}

#[tokio::test]
async fn external_schema_references_fail_but_literal_reference_data_is_not_executed() {
    for schema in [
        json!({"$ref":"https://unavailable.invalid/schema"}),
        json!({"properties":{"limit":{"$ref":"file:///tmp/schema"}}}),
        json!({"$dynamicRef":"#anchor"}),
        json!({"type":"integer"}),
    ] {
        let mut c = catalog();
        c.0.get_mut(&reference(ComponentKind::Tool, "documents.search"))
            .unwrap()
            .config_schema = schema;
        let error = ProfileValidator::new(&c)
            .validate(&profile(), &scope())
            .await
            .unwrap_err();
        assert!(matches!(
            error.code,
            ErrorCode::InvalidSchema | ErrorCode::InvalidConfiguration
        ));
    }
    let mut c = catalog();
    let meta =
        c.0.get_mut(&reference(ComponentKind::Tool, "documents.search"))
            .unwrap();
    meta.config_schema = json!({"$defs":{"limit":{"type":"integer","minimum":1}},"type":"object","properties":{"limit":{"$ref":"#/$defs/limit"}},"required":["limit"],"additionalProperties":false,"default":{"$ref":"https://example.invalid/literal-data"}});
    ProfileValidator::new(&c)
        .validate(&profile(), &scope())
        .await
        .unwrap();
}

#[tokio::test]
async fn extensions_require_registered_namespaces_and_valid_data() {
    let mut input = profile_value();
    input["extensions"] = json!({"bad":{}});
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
    input["extensions"] = json!({"example.settings":{"enabled":true}});
    let p = AgentProfile::from_json(&input.to_string()).unwrap();
    assert_eq!(
        ProfileValidator::new(&catalog())
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    let mut c = catalog();
    let key = reference(ComponentKind::Extension, "example.settings");
    let mut definition = metadata(&key);
    definition.config_schema = json!({"type":"object","properties":{"enabled":{"type":"boolean"}},"additionalProperties":false});
    c.0.insert(key, definition);
    ProfileValidator::new(&c)
        .validate(&p, &scope())
        .await
        .unwrap();
    input["extensions"]["example.settings"]["enabled"] = json!(1);
    assert_eq!(
        ProfileValidator::new(&c)
            .validate(
                &AgentProfile::from_json(&input.to_string()).unwrap(),
                &scope()
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidConfiguration
    );
}

#[tokio::test]
async fn registered_formats_are_asserted_instead_of_treated_as_annotations() {
    let mut catalog = catalog();
    catalog
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .config_schema = json!({
        "type": "object", "properties": {"example_uuid": {"type": "string", "format": "uuid"}},
        "required": ["example_uuid"], "additionalProperties": false
    });
    let mut input = profile_value();
    input["tools"][0]["config"] = json!({"example_uuid": "not-a-uuid"});
    let invalid = AgentProfile::from_json(&input.to_string()).unwrap();
    assert_eq!(
        ProfileValidator::new(&catalog)
            .validate(&invalid, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidConfiguration
    );
    input["tools"][0]["config"] = json!({"example_uuid": "123e4567-e89b-12d3-a456-426614174000"});
    ProfileValidator::new(&catalog)
        .validate(
            &AgentProfile::from_json(&input.to_string()).unwrap(),
            &scope(),
        )
        .await
        .unwrap();
}

fn adapter_profile() -> (AgentProfile, Catalog) {
    let mut value = profile_value();
    value["tools"] =
        json!([{"adapter_binding":"documents","export_id":"search","alias":"search_documents"}]);
    value["adapters"] = json!([{"binding_id":"documents","adapter_id":"document-tools","version":"1.0.0","connections":{"main":"knowledge"}}]);
    let mut c = catalog();
    let key = reference(ComponentKind::Adapter, "document-tools");
    let mut definition = metadata(&key);
    definition.required_connections.insert(id("main"));
    definition.exports.push(ExportMetadata {
        export_id: id("search"),
        kind: ExportKind::Tool,
        contract_version: 1,
        model_name: Some(id("documents_search")),
        hook_position: None,
        capabilities: BTreeSet::from([id("documents.search")]),
        required_capabilities: BTreeSet::from([id("model.tool_calling")]),
    });
    definition.exports.push(ExportMetadata {
        export_id: id("unused"),
        kind: ExportKind::Tool,
        contract_version: 1,
        model_name: Some(id("unused")),
        hook_position: None,
        capabilities: BTreeSet::from([id("unused.capability")]),
        required_capabilities: BTreeSet::new(),
    });
    c.0.insert(key, definition);
    (AgentProfile::from_json(&value.to_string()).unwrap(), c)
}

#[tokio::test]
async fn adapter_exports_must_exist_match_kind_and_be_selected_to_supply_capabilities() {
    let (p, c) = adapter_profile();
    ProfileValidator::new(&c)
        .validate(&p, &scope())
        .await
        .unwrap();
    let mut wrong_kind = catalog();
    let (_, mut definitions) = adapter_profile();
    let key = reference(ComponentKind::Adapter, "document-tools");
    let mut definition = definitions.0.remove(&key).unwrap();
    definition.exports[0].kind = ExportKind::ContextSource;
    wrong_kind.0.insert(key.clone(), definition);
    assert_eq!(
        ProfileValidator::new(&wrong_kind)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
    let mut missing = p.clone();
    if let ToolBindingRef::Export(export) = &mut missing.tools[0] {
        export.export_id = id("missing");
    }
    assert_eq!(
        ProfileValidator::new(&c)
            .validate(&missing, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
    let mut needs_unselected = c;
    needs_unselected
        .0
        .get_mut(&key)
        .unwrap()
        .required_capabilities
        .insert(id("unused.capability"));
    assert_eq!(
        ProfileValidator::new(&needs_unselected)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
    let mut duplicate = p.clone();
    duplicate.tools.push(duplicate.tools[0].clone());
    assert_eq!(
        ProfileValidator::new(&adapter_profile().1)
            .validate(&duplicate, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
}

#[tokio::test]
async fn saved_profiles_reject_changed_instructions_versions_scope_and_tampered_serialization() {
    let pinned = resolved().await;
    let p = profile();
    let mut changed = p.clone();
    changed.version = id("2.0.0");
    assert_eq!(
        pinned.ensure_matches(&changed, &scope()).unwrap_err().code,
        ErrorCode::ProfileMismatch
    );
    let mut changed = p.clone();
    changed.instructions = Instructions::Text(InstructionText {
        text: "Changed behavior".into(),
    });
    assert_eq!(
        pinned.ensure_matches(&changed, &scope()).unwrap_err().code,
        ErrorCode::ProfileMismatch
    );
    let mut other_scope = scope();
    other_scope.tenant_id = id("other");
    assert_eq!(
        pinned.ensure_matches(&p, &other_scope).unwrap_err().code,
        ErrorCode::ProfileMismatch
    );
    let mut stored = serde_json::to_value(&pinned).unwrap();
    stored["profile"]["version"] = json!("new");
    assert!(serde_json::from_value::<ResolvedProfile>(stored).is_err());
}

#[test]
fn system_inputs_preserve_absent_empty_and_owned_values_without_debug_leakage() {
    let mut input = json!({"scope":{"tenant_id":"t","workspace_id":"w"},"principal_ref":"p","capability_grant_ref":"g"});
    let absent = ExecutionContextData::from_json(&input.to_string()).unwrap();
    assert!(absent.system_inputs.is_none());
    input["system_inputs"] = json!({});
    let empty = ExecutionContextData::from_json(&input.to_string()).unwrap();
    assert!(empty.system_inputs.as_ref().unwrap().values().is_empty());
    input["system_inputs"] = Value::Null;
    assert!(ExecutionContextData::from_json(&input.to_string()).is_err());
    input["system_inputs"] = json!({"workspace_id":"private-workspace-value"});
    let stored = ExecutionContextData::from_json(&input.to_string()).unwrap();
    input["system_inputs"]["workspace_id"] = json!("mutated");
    assert_eq!(
        stored.system_inputs.as_ref().unwrap().values()["workspace_id"],
        json!("private-workspace-value")
    );
    assert!(!format!("{stored:?}").contains("private-workspace-value"));
    let roundtrip =
        ExecutionContextData::from_json(&serde_json::to_string(&stored).unwrap()).unwrap();
    assert_eq!(roundtrip, stored);
}

fn record(name: &str) -> RecordRef {
    RecordRef {
        record_id: id(name),
        revision: 1,
        digest: digest(name),
    }
}
fn request() -> RunRequest {
    RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Find supporting evidence".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    }
}

#[test]
fn absent_model_options_preserve_existing_request_encodings_and_digests() {
    let mut legacy = serde_json::to_value(request()).unwrap();
    legacy.as_object_mut().unwrap().remove("model_options");
    let restored = RunRequest::from_json(&legacy.to_string()).unwrap();
    assert!(restored.model_options.is_empty());
    assert_eq!(
        canonical_digest(&serde_json::to_value(restored).unwrap()),
        canonical_digest(&legacy)
    );
    legacy["model_options"] = json!(null);
    assert!(RunRequest::from_json(&legacy.to_string()).is_err());
}

async fn checkpoint() -> RunSnapshot {
    let p = resolved().await;
    let request = request();
    let system_inputs = Some(SystemInputSnapshotRef {
        snapshot_ref: record("protected-inputs"),
        values_digest: digest("owned inputs"),
        definition_versions: BTreeMap::from([(id("workspace_id"), id("1"))]),
    });
    let wait = WaitState {
        wait_id: id("approval"),
        target: WaitTarget::Approval {
            target: ApprovalTarget::Tool {
                call_id: id("call"),
                binding_digest: digest("bound args"),
            },
        },
        expires_at_ms: Some(100000),
    };
    RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id("run"),
        request_digest: admission_digest(&request, &p, system_inputs.as_ref()),
        request,
        scope: scope(),
        timing: RunTiming::new(0, p.profile().limits.max_elapsed_ms.get()).unwrap(),
        reservations: vec![AttemptReservation {
            attempt_id: id("first-model-attempt"),
            kind: ReservationKind::Model {
                purpose: ModelPurpose::Agent,
            },
            reserved_at_ms: 0,
        }],
        limits: p.profile().limits.clone(),
        profile: p,
        status: RunStatus::Waiting,
        phase: RunPhase::Waiting,
        model_step_id: Some(id("step")),
        usage: BudgetUsage {
            model_calls: 1,
            ..BudgetUsage::default()
        },
        model_ledger: vec![],
        tool_ledger: vec![ToolLedgerEntry {
            call: ToolCall {
                call_id: id("call"),
                model_request_id: id("model-request"),
                provider_call_id: id("provider-call"),
                tool_name: id("documents_search"),
                model_inputs: BTreeMap::from([("query".into(), json!("evidence"))]),
                descriptor_digest: digest("descriptor"),
                bound_input_ref: Some(record("bound-inputs")),
            },
            state: ToolCallState::Planned {},
        }],
        system_inputs,
        wait: Some(wait),
        outcome: None,
        assembly_ref: Some(record("assembly")),
        routing_snapshot_ref: None,
        context_batches: vec![record("context-batch")],
        source_states: vec![],
        revision: 4,
        last_event_seq: 7,
    }
}

#[tokio::test]
async fn approval_checkpoint_roundtrip_preserves_the_target_and_deduplication_identity() {
    let snapshot = checkpoint().await;
    snapshot.validate().unwrap();
    let restored = RunSnapshot::from_json(&serde_json::to_string(&snapshot).unwrap()).unwrap();
    assert_eq!(restored, snapshot);
    let target = match &restored.wait.as_ref().unwrap().target {
        WaitTarget::Approval { target } => target.clone(),
        _ => unreachable!(),
    };
    let command = ResumeCommand {
        run_id: restored.run_id.clone(),
        expected_revision: restored.revision,
        command_id: id("decision"),
        action: ResumeAction::Approve {
            wait_id: restored.wait.as_ref().unwrap().wait_id.clone(),
            target,
        },
    };
    assert_eq!(
        ResumeCommand::from_json(&serde_json::to_string(&command).unwrap()).unwrap(),
        command
    );
    let mut relocated = snapshot.system_inputs.clone().unwrap();
    relocated.snapshot_ref = record("new-storage-location");
    assert_eq!(
        snapshot.request_digest,
        admission_digest(&snapshot.request, &snapshot.profile, Some(&relocated))
    );
    relocated.values_digest = digest("different inputs");
    assert_ne!(
        snapshot.request_digest,
        admission_digest(&snapshot.request, &snapshot.profile, Some(&relocated))
    );
    relocated = snapshot.system_inputs.clone().unwrap();
    relocated
        .definition_versions
        .insert(id("workspace_id"), id("2"));
    assert_ne!(
        snapshot.request_digest,
        admission_digest(&snapshot.request, &snapshot.profile, Some(&relocated))
    );
    let empty = SystemInputSnapshotRef {
        snapshot_ref: record("empty-inputs"),
        values_digest: canonical_digest(&json!({})),
        definition_versions: BTreeMap::new(),
    };
    assert_eq!(
        admission_digest(&snapshot.request, &snapshot.profile, None),
        admission_digest(&snapshot.request, &snapshot.profile, Some(&empty))
    );
}

#[tokio::test]
async fn catalog_and_export_names_cannot_create_ambiguous_tool_routing() {
    let (mut profile, mut catalog) = adapter_profile();
    profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
        tool_id: id("documents.search"),
        version: id("1.0.0"),
        bindings: Some(BTreeMap::from([(id("main"), id("knowledge"))])),
        config: Some(BTreeMap::from([("limit".into(), json!(5))])),
    }));
    catalog
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .model_name = Some(id("search_documents"));
    assert_eq!(
        ProfileValidator::new(&catalog)
            .validate(&profile, &scope())
            .await
            .unwrap_err()
            .path,
        "tools.model_name"
    );
}

#[tokio::test]
async fn checkpoint_rejects_inconsistent_state_budget_inputs_and_dispatch_records() {
    let valid = checkpoint().await;
    let mut malformed = valid.clone();
    malformed.wait = None;
    assert_eq!(
        malformed.validate().unwrap_err().code,
        ErrorCode::InvalidSnapshot
    );
    let mut malformed = valid.clone();
    malformed.limits.max_tool_attempts += 1;
    assert_eq!(malformed.validate().unwrap_err().path, "limits");
    let mut malformed = valid.clone();
    malformed.request.request_id = id("changed");
    assert_eq!(malformed.validate().unwrap_err().path, "request_digest");
    let mut malformed = valid.clone();
    malformed.tool_ledger[0].call.bound_input_ref = None;
    malformed.tool_ledger[0].state = ToolCallState::Dispatching {
        attempt_id: id("attempt"),
        idempotency_key: id("effect"),
    };
    assert_eq!(
        malformed.validate().unwrap_err().path,
        "tool_ledger.bound_input_ref"
    );
    let mut malformed = valid.clone();
    malformed.tool_ledger.push(malformed.tool_ledger[0].clone());
    assert_eq!(
        malformed.validate().unwrap_err().path,
        "tool_ledger.call_id"
    );
    let mut stored = serde_json::to_value(&valid).unwrap();
    stored["schema_version"] = json!("wickle.run-snapshot.v2");
    assert_eq!(
        RunSnapshot::from_json(&stored.to_string())
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedSchemaVersion
    );
}

#[tokio::test]
async fn success_requires_a_matching_completion_basis_and_verified_success_requires_evidence() {
    let mut snapshot = checkpoint().await;
    snapshot.status = RunStatus::Succeeded;
    snapshot.phase = RunPhase::Finish;
    snapshot.wait = None;
    snapshot.outcome = Some(RunOutcome {
        result: OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded,
        },
        output: vec![InputContent::Text {
            text: "Candidate answer".into(),
        }],
        artifacts: vec![],
        usage: snapshot.usage.clone(),
        checkpoint_revision: snapshot.revision,
        verification: None,
        unresolved_effects: vec![],
    });
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "tool_ledger.unsettled"
    );
    snapshot.tool_ledger[0].state = ToolCallState::Settled {
        result: ToolResult {
            call_id: id("call"),
            call_message_id: id("call-message"),
            status: ToolResultStatus::Succeeded,
            content: vec![InputContent::Text {
                text: "Evidence found".into(),
            }],
            effect_receipt_ref: None,
            error: None,
        },
    };
    snapshot.validate().unwrap();
    snapshot
        .outcome
        .as_mut()
        .unwrap()
        .unresolved_effects
        .push(record("unknown-effect"));
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "outcome.unresolved_effects"
    );
    snapshot
        .outcome
        .as_mut()
        .unwrap()
        .unresolved_effects
        .clear();
    snapshot.outcome.as_mut().unwrap().result = OutcomeResult::Succeeded {
        completion_basis: CompletionBasis::Verified,
    };
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "outcome.verification"
    );
    snapshot.outcome.as_mut().unwrap().verification = Some(VerificationSummary {
        verifier_ref: VersionedRef {
            id: id("verifier"),
            version: id("1"),
        },
        criteria_ref: VersionedRef {
            id: id("criteria"),
            version: id("1"),
        },
        verdict: VerificationVerdict::Pass,
        evidence: vec![record("evidence")],
    });
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "outcome.completion_basis"
    );
}

#[test]
fn event_and_input_contracts_reject_unsupported_versions_and_execution_injection() {
    let event = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("event"),
        scope: scope(),
        run_id: id("run"),
        session_id: id("session"),
        seq: 1.try_into().unwrap(),
        timestamp_ms: 1000,
        payload: RunEventPayload::RunFinished {
            outcome_ref: record("outcome"),
        },
    };
    assert_eq!(
        RunEvent::from_json(&serde_json::to_string(&event).unwrap()).unwrap(),
        event
    );
    let mut value = serde_json::to_value(&event).unwrap();
    value["schema_version"] = json!("wickle.run-event.v2");
    assert_eq!(
        RunEvent::from_json(&value.to_string()).unwrap_err().code,
        ErrorCode::UnsupportedSchemaVersion
    );
    let mut value = serde_json::to_value(&event).unwrap();
    value["seq"] = json!(0);
    assert!(RunEvent::from_json(&value.to_string()).is_err());
    let mut value = serde_json::to_value(request()).unwrap();
    value["input"] = json!([{"type":"tool_call","call":{"tool_name":"unapproved"}}]);
    assert!(RunRequest::from_json(&value.to_string()).is_err());
    let mut value = serde_json::to_value(request()).unwrap();
    value["trigger"] = json!({"kind":"user","source_id":"forged"});
    assert!(RunRequest::from_json(&value.to_string()).is_err());
    assert!(
        serde_json::from_value::<ModelAttemptState>(json!({"state":"completed","kind":"timeout"}))
            .is_err()
    );
    assert!(
        serde_json::from_value::<ToolCallState>(
            json!({"state":"planned","idempotency_key":"unexpected"})
        )
        .is_err()
    );
}

#[test]
fn route_roundtrip_keeps_model_api_deployment_and_adapter_versions_distinct() {
    let route = ResolvedModelRoute {
        binding: VersionedRef {
            id: id("binding"),
            version: id("binding-revision"),
        },
        catalog_revision: id("catalog-1"),
        routing_policy_revision: id("policy-1"),
        requested_model: id("requested-alias"),
        model_id: id("model-family"),
        model_version: id("release-A"),
        version_semantics: VersionSemantics::MutableDeployment,
        provider: id("custom-provider"),
        target: BTreeMap::from([("deployment".into(), json!("deployment-name"))]),
        deployment_revision: Some(id("deployment-revision")),
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("api-contract-version"),
        },
        adapter: VersionedRef {
            id: id("adapter"),
            version: id("adapter-version"),
        },
        capability_revision: id("capabilities-1"),
        connection_ref: VersionedRef {
            id: id("connection"),
            version: id("connection-revision"),
        },
    };
    let restored: ResolvedModelRoute =
        serde_json::from_str(&serde_json::to_string(&route).unwrap()).unwrap();
    assert_eq!(restored, route);
    let original = route.digest();
    let mut changed = route.clone();
    changed.model_version = id("release-B");
    assert_ne!(changed.digest(), original);
    changed = route.clone();
    changed.api_contract.version = id("different-api-version");
    assert_ne!(changed.digest(), original);
    changed = route.clone();
    changed.deployment_revision = Some(id("new-deployment-revision"));
    assert_ne!(changed.digest(), original);
    let record = ModelInvocationRecord {
        run_id: id("run"),
        model_step_id: id("step"),
        attempt_id: id("attempt"),
        purpose: ModelPurpose::Agent,
        route,
        selection_reason: id("policy-default"),
        request_digest: digest("request"),
        state: ModelAttemptState::Completed {},
        inspection_ref: None,
        response_ref: None,
        provider_request_id: None,
        reported_model_id: None,
        reported_model_version: None,
        usage: None,
    };
    let restored: ModelInvocationRecord =
        serde_json::from_str(&serde_json::to_string(&record).unwrap()).unwrap();
    assert_eq!(restored.reported_model_version, None);
    assert_eq!(restored.usage, None);
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
            command_id: id("resume-command"),
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
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id("run"),
        request_digest: admission_digest(&request, &profile, system_inputs.as_ref()),
        request,
        scope: scope(),
        timing: RunTiming::new(0, profile.profile().limits.max_elapsed_ms.get()).unwrap(),
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
    let Guarded::Completed(run) = gate.run_view(&snapshot, &context, None).await.unwrap() else {
        panic!("expected authorized public run view");
    };
    assert_eq!(
        serde_json::to_value(run).unwrap(),
        json!({"run_id":"run","session_id":"session","status":"failed","phase":"finish","revision":3,
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
            gate.run_view(&snapshot, &context, None)
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
        gate.run_view(&snapshot, &context, None).await.unwrap(),
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
```

## `crates/wickle/tests/support/mod.rs`

```rust
//! Shared realistic run fixtures for storage and execution boundary tests.

use serde_json::json;
use std::collections::BTreeSet;
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

pub struct Catalog {
    pub revision: &'static str,
}
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id(self.revision)),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!(self.revision)),
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

pub fn event(
    run: &Id,
    session: &Id,
    owner: &Scope,
    seq: u64,
    payload: RunEventPayload,
) -> RunEvent {
    RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id(&format!("event-{run}-{seq}")),
        scope: owner.clone(),
        run_id: run.clone(),
        session_id: session.clone(),
        seq: seq.try_into().unwrap(),
        timestamp_ms: 1000 + seq as i64,
        payload,
    }
}

pub async fn admission(
    run: &str,
    request_id: &str,
    session: &str,
    text: &str,
    revision: &'static str,
) -> AdmissionInput {
    let owner = scope();
    let profile=AgentProfile::from_json(&json!({
        "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
        "name":"Assistant","description":"State contract example","instructions":{"text":"Use evidence"},
        "model_binding":"primary","tools":[],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":8,"max_tool_attempts":4,"max_repair_attempts":1,"max_recovery_attempts":1,"max_elapsed_ms":10000}
    }).to_string()).unwrap();
    let profile = ProfileValidator::new(&Catalog { revision })
        .validate(&profile, &owner)
        .await
        .unwrap();
    let request = RunRequest {
        request_id: id(request_id),
        session_id: id(session),
        input: vec![InputContent::Text { text: text.into() }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    };
    let request_record = ProtectedRecord::new(
        id(&format!("request-{run}")),
        1,
        serde_json::to_value(&request).unwrap(),
    );
    let prompt_record = ProtectedRecord::new(
        id(&format!("prompt-{session}")),
        1,
        json!({"instructions":"Use evidence"}),
    );
    let snapshot = RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id(run),
        request_digest: admission_digest(&request, &profile, None),
        request: request.clone(),
        scope: owner.clone(),
        timing: RunTiming::new(0, profile.profile().limits.max_elapsed_ms.get()).unwrap(),
        reservations: vec![],
        limits: profile.profile().limits.clone(),
        profile,
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        usage: BudgetUsage::default(),
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: None,
        wait: None,
        outcome: None,
        assembly_ref: None,
        routing_snapshot_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    let started = event(
        &snapshot.run_id,
        &request.session_id,
        &owner,
        1,
        RunEventPayload::RunStarted {
            request_ref: request_record.reference().clone(),
            profile_digest: snapshot.profile.profile_digest().clone(),
        },
    );
    let message = Message {
        message_id: id(&format!("message-{run}")),
        run_id: snapshot.run_id.clone(),
        sequence: 1.try_into().unwrap(),
        role: MessageRole::User,
        content: vec![ContentBlock::Content {
            content: InputContent::Text { text: text.into() },
        }],
        origin: MessageOrigin::User,
        visibility: Visibility::UserAndModel,
    };
    AdmissionInput {
        snapshot,
        prompt_snapshot: prompt_record.reference().clone(),
        messages: vec![message],
        events: vec![started],
        records: vec![request_record, prompt_record],
        require_durable: false,
    }
}

pub fn prepared(snapshot: &RunSnapshot, lease: RunLease, now: i64) -> CommitInput {
    let mut next = snapshot.clone();
    next.revision += 1;
    next.phase = RunPhase::Prepare;
    CommitInput {
        expected_revision: snapshot.revision,
        lease,
        now_ms: now,
        snapshot: next,
        messages: vec![],
        events: vec![],
        records: vec![],
    }
}

pub fn finished(snapshot: &RunSnapshot, lease: RunLease, now: i64) -> CommitInput {
    let mut next = snapshot.clone();
    next.revision += 1;
    next.last_event_seq += 1;
    next.status = RunStatus::Succeeded;
    next.phase = RunPhase::Finish;
    let outcome = RunOutcome {
        result: OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded,
        },
        output: vec![InputContent::Text {
            text: "Completed".into(),
        }],
        artifacts: vec![],
        usage: next.usage.clone(),
        checkpoint_revision: next.revision,
        verification: None,
        unresolved_effects: vec![],
    };
    let record = ProtectedRecord::new(
        id(&format!("outcome-{}", next.run_id)),
        1,
        serde_json::to_value(&outcome).unwrap(),
    );
    next.outcome = Some(outcome);
    let finished = event(
        &next.run_id,
        &next.request.session_id,
        &next.scope,
        next.last_event_seq,
        RunEventPayload::RunFinished {
            outcome_ref: record.reference().clone(),
        },
    );
    CommitInput {
        expected_revision: snapshot.revision,
        lease,
        now_ms: now,
        snapshot: next,
        messages: vec![],
        events: vec![finished],
        records: vec![record],
    }
}
```

## `tests/support/budget_consumer.rs`

```rust
use serde_json::json;
use std::{cell::Cell, collections::BTreeSet, sync::Arc};
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
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
                manifest_digest: canonical_digest(&json!("example model binding")),
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

fn context(scope: &Scope) -> ExecutionContext {
    ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id("caller"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    )
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let profile = AgentProfile::from_json(
        r#"{
          "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
          "name":"Assistant","description":"Budget example","instructions":{"text":"Use evidence"},
          "model_binding":"primary","tools":[],"skills":[],"connectors":[],
          "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
          "limits":{"max_model_calls":1,"max_tool_attempts":3,"max_repair_attempts":0,
                    "max_recovery_attempts":1,"max_elapsed_ms":30000}
        }"#,
    )?;
    let profile = ProfileValidator::new(&Catalog)
        .validate(&profile, &scope)
        .await?;
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Count available records".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    };
    let request_record =
        ProtectedRecord::new(id("request-record"), 1, serde_json::to_value(&request)?);
    let prompt = ProtectedRecord::new(id("prompt"), 1, json!({"text":"Use evidence"}));
    let clock = Arc::new(SystemClock::new());
    let started_at = clock.now()?.utc_ms;
    let run_id = id("run");
    let snapshot = RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: run_id.clone(),
        request_digest: admission_digest(&request, &profile, None),
        request: request.clone(),
        scope: scope.clone(),
        limits: profile.profile().limits.clone(),
        profile,
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        usage: BudgetUsage::default(),
        timing: RunTiming::new(started_at, 30000)?,
        reservations: vec![],
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: None,
        wait: None,
        outcome: None,
        assembly_ref: None,
        routing_snapshot_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    let started = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("started"),
        scope: scope.clone(),
        run_id: run_id.clone(),
        session_id: request.session_id.clone(),
        seq: 1.try_into()?,
        timestamp_ms: started_at,
        payload: RunEventPayload::RunStarted {
            request_ref: request_record.reference().clone(),
            profile_digest: snapshot.profile.profile_digest().clone(),
        },
    };
    let store = Arc::new(MemoryStateStore::new());
    store
        .admit(
            &scope,
            AdmissionInput {
                snapshot,
                prompt_snapshot: prompt.reference().clone(),
                messages: vec![Message {
                    message_id: id("user-message"),
                    run_id: run_id.clone(),
                    sequence: 1.try_into()?,
                    role: MessageRole::User,
                    content: vec![ContentBlock::Content {
                        content: request.input[0].clone(),
                    }],
                    origin: MessageOrigin::User,
                    visibility: Visibility::UserAndModel,
                }],
                events: vec![started],
                records: vec![request_record, prompt],
                require_durable: false,
            },
        )
        .await?;
    let lease = store
        .acquire_lease(&scope, &run_id, &id("worker"), clock.now()?.utc_ms, 30000)
        .await?;
    let execution = context(&scope);
    let budget = RunBudget::attach(
        store.clone(),
        clock.clone(),
        Arc::new(RandomIdSource),
        scope.clone(),
        run_id.clone(),
        lease.clone(),
        execution.cancellation.clone(),
    )
    .await?;
    let dispatches = Cell::new(0);
    let model_attempt = budget
        .execute(
            ReservationKind::Model {
                purpose: ModelPurpose::Agent,
            },
            |reservation| {
                dispatches.set(dispatches.get() + 1);
                async move { Ok(reservation.attempt_id) }
            },
        )
        .await?;
    let refused = budget
        .execute(
            ReservationKind::Model {
                purpose: ModelPurpose::Compaction,
            },
            |_| {
                dispatches.set(dispatches.get() + 1);
                async { Ok(()) }
            },
        )
        .await;
    assert_eq!(refused.unwrap_err().code, ErrorCode::BudgetExceeded);
    assert_eq!(dispatches.get(), 1);

    // This example exercises reservation boundaries; a full driver owns policy,
    // tool schemas, actual provider/tool dispatch, and result settlement.
    let count = budget
        .execute(
            ReservationKind::Tool {
                call_id: id("count-records"),
            },
            |_| {
                dispatches.set(dispatches.get() + 1);
                async { Ok(["first", "second"].len()) }
            },
        )
        .await?;
    assert_eq!(count, 2);
    assert_eq!(dispatches.get(), 2);

    // Save a reservation without reporting an execution result, then detach.
    let unsettled = budget
        .reserve(ReservationKind::Tool {
            call_id: id("inspect-record"),
        })
        .await?;
    execution.cancellation.cancel();
    let cancelled = budget
        .execute(
            ReservationKind::Tool {
                call_id: id("cancelled-call"),
            },
            |_| {
                dispatches.set(dispatches.get() + 1);
                async { Ok(()) }
            },
        )
        .await;
    assert_eq!(cancelled.unwrap_err().code, ErrorCode::Cancelled);
    assert_eq!(dispatches.get(), 2);
    drop(budget);

    let restored = store.load(&scope, &run_id).await?.snapshot;
    let restored = RunSnapshot::from_json(&serde_json::to_string(&restored)?)?;
    assert_eq!(restored.usage.model_calls, 1);
    assert_eq!(restored.usage.tool_attempts, 2);
    assert_eq!(restored.reservations.len(), 3);
    assert!(restored.reservations.contains(&unsettled));
    assert!(
        restored
            .reservations
            .iter()
            .any(|saved| saved.attempt_id == model_attempt)
    );
    let resumed = RunBudget::attach(
        store.clone(),
        clock,
        Arc::new(RandomIdSource),
        scope.clone(),
        run_id.clone(),
        lease,
        context(&scope).cancellation,
    )
    .await?;
    let before = store.load(&scope, &run_id).await?.snapshot.reservations;
    assert_eq!(
        resumed
            .reserve(ReservationKind::Model {
                purpose: ModelPurpose::Verification,
            })
            .await
            .unwrap_err()
            .code,
        ErrorCode::BudgetExceeded
    );
    assert_eq!(
        store.load(&scope, &run_id).await?.snapshot.reservations,
        before
    );
    println!(
        "budget consumer: model limit preserved; tool budget independent; cancellation dispatched 0 additional calls; unsettled reservation retained after reattach"
    );
    Ok(())
}
```

## `tests/support/context_consumer.rs`

```rust
use serde_json::json;
use std::collections::BTreeSet;
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
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
                    version: reference.version.clone().or_else(|| Some(id("1"))),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!(reference.id)),
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
fn compiled_tool() -> Result<CompiledTool, ContractError> {
    let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string"}),
        source: SystemInputSource::Run {},
    }])?;
    SchemaCompiler::new().compile(ToolDescriptor {
        tool: reference("search"), name: id("search"), description: "Search available evidence".into(),
        input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"workspace_id":{"type":"string","description":"Internal workspace key"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters: vec!["query".into()], system_bindings: None, output_schema: json!({"type":"array"}), side_effect: ToolSideEffect::ReadOnly, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false, max_output_bytes: 4096.try_into().unwrap(),
    }, &registry)
}
fn route() -> ResolvedModelRoute {
    ResolvedModelRoute {
        binding: reference("primary"),
        catalog_revision: id("catalog"),
        routing_policy_revision: id("policy"),
        requested_model: id("example"),
        model_id: id("example"),
        model_version: id("1"),
        version_semantics: VersionSemantics::Pinned,
        provider: id("example"),
        target: JsonObject::new(),
        deployment_revision: None,
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("1"),
        },
        adapter: reference("example"),
        capability_revision: id("capabilities"),
        connection_ref: reference("connection"),
    }
}
fn request(run: &str, text: &str) -> RunRequest {
    RunRequest {
        request_id: id(&format!("request-{run}")),
        session_id: id("session"),
        input: vec![InputContent::Text { text: text.into() }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::from([("reasoning_effort".into(), json!("high"))]),
        output_contract: None,
    }
}
fn admission(
    profile: &ResolvedProfile,
    prompt: &ProtectedRecord,
    run: &str,
    first_sequence: u64,
    started_at_ms: i64,
    text: &str,
) -> AdmissionInput {
    let request = request(run, text);
    let request_record = ProtectedRecord::new(
        id(&format!("input-{run}")),
        1,
        serde_json::to_value(&request).unwrap(),
    );
    let scope = profile.scope().clone();
    let snapshot = RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id(run),
        request_digest: admission_digest(&request, profile, None),
        request: request.clone(),
        scope: scope.clone(),
        profile: profile.clone(),
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        limits: profile.profile().limits.clone(),
        usage: BudgetUsage::default(),
        timing: RunTiming::new(started_at_ms, 10000).unwrap(),
        reservations: vec![],
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: None,
        wait: None,
        outcome: None,
        assembly_ref: None,
        routing_snapshot_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    AdmissionInput {
        snapshot,
        prompt_snapshot: prompt.reference().clone(),
        require_durable: false,
        messages: vec![Message {
            message_id: id(&format!("user-{run}")),
            run_id: id(run),
            sequence: first_sequence.try_into().unwrap(),
            role: MessageRole::User,
            content: request
                .input
                .into_iter()
                .map(|content| ContentBlock::Content { content })
                .collect(),
            origin: MessageOrigin::User,
            visibility: Visibility::UserAndModel,
        }],
        events: vec![RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: id(&format!("start-{run}")),
            scope,
            run_id: id(run),
            session_id: id("session"),
            seq: 1.try_into().unwrap(),
            timestamp_ms: started_at_ms,
            payload: RunEventPayload::RunStarted {
                request_ref: request_record.reference().clone(),
                profile_digest: profile.profile_digest().clone(),
            },
        }],
        records: vec![request_record, prompt.clone()],
    }
}
fn project(
    prompt: &PromptSnapshot,
    stored: &StoredRun,
) -> Result<ContextProjection, ContractError> {
    let step = id(&format!("step-{}", stored.snapshot.run_id));
    ContextAssembler::new().project(
        prompt,
        ProjectionInput {
            profile: &stored.snapshot.profile,
            scope: &stored.snapshot.scope,
            run_id: &stored.snapshot.run_id,
            model_step_id: &step,
            current_request: &stored.snapshot.request,
            current_request_message_id: &id(&format!("user-{}", stored.snapshot.run_id)),
            transcript: &stored.messages,
            context_items: &[],
            opaque_records: &[],
            expected_prompt_digest: &stored.session.prompt_snapshot.digest,
            request_id: step.clone(),
            purpose: ModelPurpose::Agent,
            route: route(),
            output: ModelOutput::Text {},
            max_output_tokens: 128.try_into().unwrap(),
            options: stored.snapshot.request.model_options.clone(),
            response_limits: ModelResponseLimits {
                max_input_bytes: 32_768,
                max_response_bytes: 4096,
                max_delta_bytes: 1024,
                max_events: 16,
                max_tool_calls: 1,
            },
            limits: ProjectionLimits {
                max_bytes: 32_768,
                max_items: 30,
            },
        },
    )
}

fn projected_user_occurrences(projection: &ContextProjection, text: &str) -> usize {
    projection
        .request
        .messages
        .iter()
        .filter(|message| message.role == ModelRole::User)
        .flat_map(|message| &message.content)
        .filter(|content| matches!(content, ModelContent::Text { text: value } if value == text))
        .count()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let profile = AgentProfile::from_json(
        r#"{
      "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1",
      "name":"Assistant","description":"Context example","instructions":{"text":"Summarize available evidence"},
      "model_binding":"primary","tools":[{"tool_id":"search","version":"1"}],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
      "limits":{"max_model_calls":4,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
    }"#,
    )?;
    let profile = ProfileValidator::new(&Catalog)
        .validate(&profile, &scope)
        .await?;
    let tool = compiled_tool()?;
    let prompt = PromptSnapshot::create(
        &profile,
        vec!["Only report actions supported by supplied observations.".into()],
        None,
        vec![PromptToolBinding {
            selection: profile.profile().tools[0].clone(),
            compiled: tool.clone(),
        }],
        vec![],
    )?;
    let prompt_record = ProtectedRecord::new(id("prompt"), 1, serde_json::to_value(&prompt)?);
    assert_eq!(prompt.digest(), prompt_record.reference().digest);
    let store = MemoryStateStore::new();
    let mut first_input = admission(
        &profile,
        &prompt_record,
        "first",
        1,
        1000,
        "Review the available evidence",
    );
    first_input.messages.push(Message {
        message_id: id("private-state"),
        run_id: id("first"),
        sequence: 2.try_into()?,
        role: MessageRole::System,
        content: vec![ContentBlock::Content {
            content: InputContent::Json {
                value: json!({"workspace_id":"host-only-database-value"}),
            },
        }],
        origin: MessageOrigin::Host,
        visibility: Visibility::Internal,
    });
    let first = store.admit(&scope, first_input).await?.state;
    let first_projection = project(&prompt, &first)?;
    assert_eq!(first_projection.request.options, first.snapshot.request.model_options);
    assert_eq!(first_projection.request.messages[0].role, ModelRole::System);
    assert_eq!(
        projected_user_occurrences(&first_projection, "Review the available evidence"),
        1
    );
    assert!(
        first_projection.request.tools[0].model_input_schema["properties"]
            .get("workspace_id")
            .is_none()
    );
    assert!(
        !serde_json::to_string(&first_projection.request)?.contains("host-only-database-value")
    );
    assert_eq!(
        first_projection
            .selected_message_ids
            .iter()
            .filter(|message| **message == id("user-first"))
            .count(),
        1
    );

    // Finish this demonstration run without claiming that a model or tool executed.
    let lease = store
        .acquire_lease(&scope, &id("first"), &id("worker"), 1000, 1000)
        .await?;
    let mut snapshot = first.snapshot.clone();
    snapshot.revision = 1;
    snapshot.last_event_seq = 2;
    snapshot.status = RunStatus::Cancelled;
    snapshot.phase = RunPhase::Finish;
    snapshot.usage.elapsed_ms = 1;
    snapshot.timing.last_observed_at_ms = 1001;
    let outcome = RunOutcome {
        result: OutcomeResult::Cancelled {
            reason: "Demonstration complete".into(),
        },
        output: vec![],
        artifacts: vec![],
        usage: snapshot.usage.clone(),
        checkpoint_revision: 1,
        verification: None,
        unresolved_effects: vec![],
    };
    let outcome_record =
        ProtectedRecord::new(id("outcome-first"), 1, serde_json::to_value(&outcome)?);
    snapshot.outcome = Some(outcome);
    store
        .commit(
            &scope,
            &id("first"),
            CommitInput {
                expected_revision: 0,
                lease,
                now_ms: 1001,
                snapshot,
                messages: vec![],
                records: vec![outcome_record.clone()],
                events: vec![RunEvent {
                    schema_version: RunEventSchemaVersion::V1,
                    event_id: id("finish-first"),
                    scope: scope.clone(),
                    run_id: id("first"),
                    session_id: id("session"),
                    seq: 2.try_into()?,
                    timestamp_ms: 1001,
                    payload: RunEventPayload::RunFinished {
                        outcome_ref: outcome_record.reference().clone(),
                    },
                }],
            },
        )
        .await?;

    let changed = PromptSnapshot::create(
        &profile,
        vec!["Changed operating policy".into()],
        None,
        vec![PromptToolBinding {
            selection: profile.profile().tools[0].clone(),
            compiled: tool,
        }],
        vec![],
    )?;
    let changed_record =
        ProtectedRecord::new(id("changed-prompt"), 1, serde_json::to_value(&changed)?);
    assert!(
        store
            .admit(
                &scope,
                admission(&profile, &changed_record, "wrong", 3, 1002, "Continue")
            )
            .await
            .is_err()
    );
    let second = store
        .admit(
            &scope,
            admission(
                &profile,
                &prompt_record,
                "second",
                3,
                1002,
                "Now give a concise summary",
            ),
        )
        .await?
        .state;
    let saved_prompt = store
        .read_record(&scope, &second.session.prompt_snapshot)
        .await?;
    let restored = PromptSnapshot::restore(
        &serde_json::to_string(saved_prompt.value())?,
        &second.session.prompt_snapshot.digest,
        &second.snapshot.profile,
        &scope,
    )?;
    let second_projection = project(&restored, &second)?;
    assert_eq!(
        projected_user_occurrences(&second_projection, "Now give a concise summary"),
        1
    );
    assert_eq!(
        first_projection.prompt_digest,
        second_projection.prompt_digest
    );
    assert_eq!(
        first_projection.request.tools,
        second_projection.request.tools
    );
    assert_eq!(
        second_projection
            .selected_message_ids
            .iter()
            .filter(|message| **message == id("user-second"))
            .count(),
        1
    );
    assert!(
        !serde_json::to_string(&second_projection.request)?.contains("host-only-database-value")
    );
    println!(
        "context consumer: two stored runs share the pinned prompt/tool schema; changed prompt refused; current request appears once; internal execution data excluded"
    );
    Ok(())
}
```

## `tests/support/input_binding_consumer.rs`

```rust
use serde_json::json;
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;

const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";
const REPORT_A: &str = "22222222-2222-4222-8222-222222222222";
const REPORT_B: &str = "33333333-3333-4333-8333-333333333333";
fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
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
                    version: r.version.clone().or_else(|| Some(id("1"))),
                    ..r.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!(r.id)),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (r.kind == ComponentKind::Tool).then(|| r.id.clone()),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}
struct OwnedTargets;
impl PolicyPort for OwnedTargets {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::ExecuteTool { input } = &request.action {
                let args = input.execution_args();
                let owned = match input.tool.id.as_str() {
                    "search" => {
                        args.get("workspace_id").and_then(|v| v.as_str()) == Some(WORKSPACE)
                    }
                    "read_report" => matches!(
                        args.get("report_id").and_then(|v| v.as_str()),
                        Some(REPORT_A | REPORT_B)
                    ),
                    _ => false,
                };
                if !owned {
                    return Ok(PolicyDecision::Deny {
                        reason: id("target_not_owned"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}
struct CurrentReport {
    value: Mutex<ResolvedSystemInput>,
    calls: AtomicUsize,
}
impl SystemInputResolver for CurrentReport {
    fn resolve<'a>(
        &'a self,
        request: &'a SystemInputResolveRequest,
        _: &'a SystemInputResolveContext,
    ) -> PortFuture<'a, Option<ResolvedSystemInput>> {
        assert_eq!(request.key, id("current_report_id"));
        self.calls.fetch_add(1, Ordering::SeqCst);
        let value = self.value.lock().unwrap().clone();
        Box::pin(async move { Ok(Some(value)) })
    }
}
fn tool(
    name: &str,
    input_schema: serde_json::Value,
    agent_parameters: Vec<String>,
) -> ToolDescriptor {
    ToolDescriptor {
        tool: reference(name),
        name: id(name),
        description: "Read authorized data".into(),
        input_schema,
        agent_parameters,
        system_bindings: None,
        output_schema: json!({"type":"string"}),
        side_effect: ToolSideEffect::ReadOnly,
        concurrency: ToolConcurrency::Serial,
        retry: ToolRetryPolicy::Never,
        reconcile: false,
        max_output_bytes: 4096.try_into().unwrap(),
    }
}
async fn plan(
    store: &MemoryStateStore,
    scope: &Scope,
    run: &Id,
    lease: &RunLease,
    clock: &SystemClock,
    call_id: &str,
    compiled: &CompiledTool,
    model_inputs: JsonObject,
) -> Result<(), ContractError> {
    let saved = store.load(scope, run).await?;
    let mut snapshot = saved.snapshot;
    let expected_revision = snapshot.revision;
    snapshot.revision += 1;
    snapshot.phase = RunPhase::Tool;
    let call = ToolCall {
        call_id: id(call_id),
        model_request_id: id("model-request"),
        provider_call_id: id(&format!("provider-{call_id}")),
        tool_name: compiled.descriptor().name.clone(),
        model_inputs,
        descriptor_digest: compiled.descriptor_digest().clone(),
        bound_input_ref: None,
    };
    snapshot.tool_ledger.push(ToolLedgerEntry {
        call,
        state: ToolCallState::Planned {},
    });
    store
        .commit(
            scope,
            run,
            CommitInput {
                expected_revision,
                lease: lease.clone(),
                now_ms: clock.now()?.utc_ms,
                snapshot,
                messages: vec![],
                events: vec![],
                records: vec![],
            },
        )
        .await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let registry = Arc::new(SystemInputRegistry::new(vec![
        SystemInputDefinition {
            key: id("workspace_id"),
            version: id("1"),
            value_schema: json!({"type":"string","format":"uuid"}),
            source: SystemInputSource::Run {},
        },
        SystemInputDefinition {
            key: id("unused_key"),
            version: id("1"),
            value_schema: json!({"type":"string"}),
            source: SystemInputSource::Run {},
        },
        SystemInputDefinition {
            key: id("current_report_id"),
            version: id("1"),
            value_schema: json!({"type":"string","format":"uuid"}),
            source: SystemInputSource::Resolver {
                resolver_ref: reference("current-report"),
            },
        },
    ])?);
    let search = SchemaCompiler::new().compile(tool("search", json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","default":10},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}), vec!["query".into(),"limit".into()]), &registry)?;
    let mut report = tool(
        "read_report",
        json!({"type":"object","properties":{"report_id":{"type":"string","format":"uuid"}},"required":["report_id"],"additionalProperties":false}),
        vec![],
    );
    report.system_bindings = Some(std::collections::BTreeMap::from([(
        "report_id".into(),
        id("current_report_id"),
    )]));
    let report = SchemaCompiler::new().compile(report, &registry)?;
    let values = SystemInputs::new(JsonObject::from([
        ("workspace_id".into(), json!(WORKSPACE)),
        ("unused_key".into(), json!("not a tool argument")),
    ]));
    let captured = RunSystemInputs::capture(scope.clone(), Some(values.clone()), &registry)?;
    let input_record = captured.to_record(id("run-inputs"), 1);
    let input_ref = captured.snapshot_ref(input_record.reference())?;
    let profile = AgentProfile::from_json(
        r#"{
      "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1",
      "name":"Assistant","description":"Binding example","instructions":{"text":"Use available evidence"},"model_binding":"primary",
      "tools":[{"tool_id":"search","version":"1"},{"tool_id":"read_report","version":"1"}],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
      "limits":{"max_model_calls":4,"max_tool_attempts":4,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}
    }"#,
    )?;
    let profile = ProfileValidator::new(&Catalog)
        .validate(&profile, &scope)
        .await?;
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Read recent results".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    };
    let request_record =
        ProtectedRecord::new(id("request-data"), 1, serde_json::to_value(&request)?);
    let prompt = ProtectedRecord::new(
        id("prompt"),
        1,
        json!({"instructions":"Use available evidence"}),
    );
    let clock = Arc::new(SystemClock::new());
    let now = clock.now()?.utc_ms;
    let run = id("run");
    let snapshot = RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: run.clone(),
        request_digest: admission_digest(&request, &profile, Some(&input_ref)),
        request: request.clone(),
        scope: scope.clone(),
        limits: profile.profile().limits.clone(),
        profile,
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        usage: BudgetUsage::default(),
        timing: RunTiming::new(now, 30000)?,
        reservations: vec![],
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: Some(input_ref.clone()),
        wait: None,
        outcome: None,
        assembly_ref: None,
        routing_snapshot_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    let started = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("started"),
        scope: scope.clone(),
        run_id: run.clone(),
        session_id: request.session_id.clone(),
        seq: 1.try_into()?,
        timestamp_ms: now,
        payload: RunEventPayload::RunStarted {
            request_ref: request_record.reference().clone(),
            profile_digest: snapshot.profile.profile_digest().clone(),
        },
    };
    let store = Arc::new(MemoryStateStore::new());
    store
        .admit(
            &scope,
            AdmissionInput {
                snapshot,
                prompt_snapshot: prompt.reference().clone(),
                messages: vec![Message {
                    message_id: id("user-message"),
                    run_id: run.clone(),
                    sequence: 1.try_into()?,
                    role: MessageRole::User,
                    content: vec![ContentBlock::Content {
                        content: request.input[0].clone(),
                    }],
                    origin: MessageOrigin::User,
                    visibility: Visibility::UserAndModel,
                }],
                events: vec![started],
                records: vec![request_record, prompt, input_record.clone()],
                require_durable: false,
            },
        )
        .await?;
    let lease = store
        .acquire_lease(&scope, &run, &id("worker"), now, 30000)
        .await?;
    let mut context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id("caller"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: Some(values),
        },
        Default::default(),
    );
    let budget = RunBudget::attach(
        store.clone(),
        clock.clone(),
        Arc::new(RandomIdSource),
        scope.clone(),
        run.clone(),
        lease.clone(),
        context.cancellation.clone(),
    )
    .await?;
    let resolver = Arc::new(CurrentReport {
        value: Mutex::new(ResolvedSystemInput {
            value: json!(REPORT_A),
            revision: id("revision-1"),
        }),
        calls: AtomicUsize::new(0),
    });
    let binder = InputBinder::new(
        registry.clone(),
        Some(resolver.clone()),
        Arc::new(PolicyGate::new(
            Arc::new(OwnedTargets),
            Duration::from_secs(1),
        )?),
        Arc::new(RandomIdSource),
    );
    plan(
        &store,
        &scope,
        &run,
        &lease,
        &clock,
        "search-call",
        &search,
        JsonObject::from([("query".into(), json!("recent results"))]),
    )
    .await?;
    let search_result = binder
        .bind(&search, &id("search-call"), &context, &budget)
        .await?;
    assert_eq!(
        serde_json::to_value(search_result.input.execution_args())?,
        json!({"query":"recent results","limit":10,"workspace_id":WORKSPACE})
    );
    assert_eq!(
        serde_json::to_value(search_result.input.original_model_inputs())?,
        json!({"query":"recent results"})
    );
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
    context.data.system_inputs = None;
    plan(
        &store,
        &scope,
        &run,
        &lease,
        &clock,
        "report-first",
        &report,
        JsonObject::new(),
    )
    .await?;
    let first = binder
        .bind(&report, &id("report-first"), &context, &budget)
        .await?;
    *resolver.value.lock().unwrap() = ResolvedSystemInput {
        value: json!(REPORT_B),
        revision: id("revision-2"),
    };
    let cached = binder
        .bind(&report, &id("report-first"), &context, &budget)
        .await?;
    assert_eq!(cached.reference, first.reference);
    assert_eq!(cached.input.execution_args()["report_id"], json!(REPORT_A));
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    plan(
        &store,
        &scope,
        &run,
        &lease,
        &clock,
        "report-next",
        &report,
        JsonObject::new(),
    )
    .await?;
    let next = binder
        .bind(&report, &id("report-next"), &context, &budget)
        .await?;
    assert_eq!(next.input.execution_args()["report_id"], json!(REPORT_B));
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 2);
    let restored = RunSystemInputs::restore(&input_record, &input_ref, &scope, &registry)?;
    restored.validate_resume(None)?;
    assert!(
        restored
            .validate_resume(Some(&SystemInputs::default()))
            .is_err()
    );
    println!(
        "input binding consumer: model query + default limit + Host workspace; unused key omitted; cached target fixed; new call resolves the new report; omitted resume inputs reuse the snapshot"
    );
    Ok(())
}
```

## `tests/support/routing_consumer.rs`

```rust
// Synthetic adapters and metadata inspector: no provider network calls are made.
// The fixed clock supports deterministic accounting; this does not test timeouts.
use futures_util::stream;
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
            Ok(PolicyDecision::Deny {
                reason: id("unknown-account"),
            })
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
struct ExampleProjector;
impl ModelRequestProjector for ExampleProjector {
    fn project<'a>(
        &'a self,
        selection: &'a RouteSelection,
        input: &'a RoutedModelInput,
        _: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ProjectedModelRequest> {
        Box::pin(async move {
            Ok(ProjectedModelRequest {
                input_tokens: 32,
                request: ModelRequest {
                    request_id: input.model_step_id.clone(),
                    purpose: input.routing.purpose,
                    route: selection.route.clone(),
                    messages: vec![ModelMessage {
                        role: ModelRole::User,
                        content: vec![ModelContent::Text {
                            text: "Inspect stored state".into(),
                        }],
                    }],
                    tools: vec![],
                    output: ModelOutput::Text {},
                    max_output_tokens: input.routing.max_output_tokens,
                    options: input.routing.options.clone(),
                    limits: ModelResponseLimits {
                        max_input_bytes: 8192,
                        max_response_bytes: 4096,
                        max_delta_bytes: 1024,
                        max_events: 8,
                        max_tool_calls: 0,
                    },
                },
            })
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let profile = AgentProfile::from_json(
        r#"{
      "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
      "name":"Assistant","description":"Storage example","instructions":{"text":"Use evidence"},
      "model_binding":"primary","tools":[],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
      "limits":{"max_model_calls":4,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":2,"max_elapsed_ms":10000}
    }"#,
    )?;
    let profile = ProfileValidator::new(&Catalog)
        .validate(&profile, &scope)
        .await?;
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Inspect stored state".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::from([("reasoning_effort".into(), json!("high"))]),
        output_contract: None,
    };
    let request_record =
        ProtectedRecord::new(id("request-record"), 1, serde_json::to_value(&request)?);
    let prompt = ProtectedRecord::new(id("prompt"), 1, json!({"text":"Use evidence"}));
    let snapshot = RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id("run"),
        request_digest: admission_digest(&request, &profile, None),
        request: request.clone(),
        scope: scope.clone(),
        timing: RunTiming::new(1000, profile.profile().limits.max_elapsed_ms.get()).unwrap(),
        reservations: vec![],
        limits: profile.profile().limits.clone(),
        profile,
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        usage: BudgetUsage::default(),
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: None,
        wait: None,
        outcome: None,
        assembly_ref: None,
        routing_snapshot_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    let started = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("started"),
        scope: scope.clone(),
        run_id: id("run"),
        session_id: id("session"),
        seq: 1.try_into()?,
        timestamp_ms: 1000,
        payload: RunEventPayload::RunStarted {
            request_ref: request_record.reference().clone(),
            profile_digest: snapshot.profile.profile_digest().clone(),
        },
    };
    let message = Message {
        message_id: id("user-message"),
        run_id: id("run"),
        sequence: 1.try_into()?,
        role: MessageRole::User,
        content: vec![ContentBlock::Content {
            content: request.input[0].clone(),
        }],
        origin: MessageOrigin::User,
        visibility: Visibility::UserAndModel,
    };
    let input = AdmissionInput {
        snapshot,
        prompt_snapshot: prompt.reference().clone(),
        messages: vec![message],
        events: vec![started],
        records: vec![request_record, prompt],
        require_durable: true,
    };
    let temporary = TemporaryDatabase(
        std::env::temp_dir().join(format!("wickle-routing-{}", RandomIdSource.next_id()?)),
    );
    std::fs::create_dir(&temporary.0)?;
    let database = temporary.0.join("state.sqlite3");
    let store = Arc::new(SqliteStateStore::open(&database)?);
    store.admit(&scope, input).await?;
    let lease = store
        .acquire_lease(&scope, &id("run"), &id("worker"), 1000, 10_000)
        .await?;
    let budget = RunBudget::attach(
        store.clone(),
        Arc::new(ExampleClock),
        Arc::new(RandomIdSource),
        scope.clone(),
        id("run"),
        lease.clone(),
        Default::default(),
    )
    .await?;
    let snapshot = routing_snapshot(&scope)?;
    let router = PolicyModelRouter::new(snapshot.clone())?;
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
    let exchange = ModelExchange::with_dispatcher(
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
        Arc::new(PolicyGate::new(
            Arc::new(ExamplePolicy),
            Duration::from_secs(1),
        )?),
    )
    .with_route_inspector(Arc::new(ExampleInspector), Duration::from_secs(1))?;
    let input = RoutedModelInput {
        model_step_id: id("step"),
        routing: RouteRequest {
            model_binding: id("primary"),
            purpose: ModelPurpose::Agent,
            required_capabilities: BTreeSet::from([id("text")]),
            input_tokens: 32,
            max_output_tokens: 128.try_into()?,
            options: JsonObject::from([("reasoning_effort".into(), json!("high"))]),
            scope: scope.clone(),
            allowed_bindings: vec![id("first"), id("second")],
            version_policy: VersionPolicy::RequirePinned,
            previous_route: None,
            previous_failure: None,
        },
    };
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
    let first_result = exchange
        .generate_routed(&router, &input, &ExampleProjector, &context, &budget)
        .await?;
    assert!(
        matches!(&first_result, Guarded::Completed(ModelExchangeOutcome::Completed { response }) if response.text == "second provider result")
    );
    let saved = store.load(&scope, &id("run")).await?;
    assert_eq!(saved.snapshot.usage.model_calls, 2);
    assert_eq!(saved.snapshot.usage.recovery_attempts, 1);
    assert_eq!(
        saved.snapshot.model_ledger[1].selection_reason,
        id("fallback_rate_limited")
    );
    for invocation in &saved.snapshot.model_ledger {
        assert!(invocation.reported_model_version.is_none());
        let reference = invocation
            .inspection_ref
            .as_ref()
            .ok_or("inspection record missing")?;
        let record = store.read_record(&scope, reference).await?;
        let observation: ModelRouteObservation = serde_json::from_value(record.value().clone())?;
        observation.validate(&invocation.route, VersionPolicy::RequirePinned)?;
    }
    // Reopen persisted routing, observation, and step-input records from SQLite.
    drop(budget);
    drop(store);
    let restored = Arc::new(SqliteStateStore::open(&database)?);
    let resumed_budget = RunBudget::attach(
        restored.clone(),
        Arc::new(ExampleClock),
        Arc::new(RandomIdSource),
        scope.clone(),
        id("run"),
        lease,
        Default::default(),
    )
    .await?;
    let second_result = exchange
        .generate_routed(
            &router,
            &input,
            &ExampleProjector,
            &context,
            &resumed_budget,
        )
        .await?;
    assert!(
        matches!(&second_result, Guarded::Completed(ModelExchangeOutcome::Completed { response }) if response.text == "second provider result")
    );
    assert_eq!(first.calls.load(Ordering::SeqCst), 1);
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        restored.load(&scope, &id("run")).await?.snapshot.revision,
        saved.snapshot.revision
    );
    println!(
        "routing consumer: separate accounts selected; rate-limit fallback charged two model calls and one recovery; effort preserved; complete step reused after SQLite reopen with zero new calls"
    );
    Ok(())
}
```

## `tests/support/sqlite_consumer.rs`

```rust
use serde_json::json;
use std::collections::BTreeSet;
use wickle::*;
use wickle_state_sqlite::SqliteStateStore;

struct TemporaryStore(std::path::PathBuf);
impl TemporaryStore {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "wickle-sqlite-consumer-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir(&directory)?;
        Ok(Self(directory))
    }
}
impl Drop for TemporaryStore {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    if std::env::args().nth(1).as_deref() == Some("verify") {
        let database = std::env::args_os()
            .nth(2)
            .ok_or("database argument missing")?;
        let parent: u32 = std::env::args()
            .nth(3)
            .ok_or("parent process missing")?
            .parse()?;
        assert_ne!(parent, std::process::id());
        let store = SqliteStateStore::open(database)?;
        let restored = store.load(&scope, &id("run")).await?;
        assert_eq!(restored.snapshot.status, RunStatus::Succeeded);
        assert_eq!(restored.snapshot.revision, 1);
        assert_eq!(
            restored.snapshot.request.model_options.get("reasoning_effort"),
            Some(&json!("high"))
        );
        assert!(restored.session.active_run_id.is_none());
        let outcome = restored
            .snapshot
            .outcome
            .as_ref()
            .ok_or("outcome missing")?;
        assert_eq!(
            outcome.output,
            vec![InputContent::Text {
                text: "Stored result".into()
            }]
        );
        let reference = ProtectedRecord::new(id("outcome"), 1, serde_json::to_value(outcome)?);
        assert_eq!(
            store
                .read_record(&scope, reference.reference())
                .await?
                .value(),
            reference.value()
        );
        let events = store.read_events(&scope, &id("run"), 0, 10).await?;
        assert_eq!(events.events.len(), 2);
        assert_eq!(events.last_available_seq, 2);
        println!("SQLite child: reopened completed run, outcome record, and two committed events");
        return Ok(());
    }
    let temporary = TemporaryStore::new()?;
    let database = temporary.0.join("state.sqlite3");
    let profile = AgentProfile::from_json(
        r#"{
      "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
      "name":"Assistant","description":"Storage example","instructions":{"text":"Use evidence"},
      "model_binding":"primary","tools":[],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
      "limits":{"max_model_calls":4,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
    }"#,
    )?;
    let profile = ProfileValidator::new(&Catalog)
        .validate(&profile, &scope)
        .await?;
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Inspect stored state".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: std::collections::BTreeMap::from([
            ("reasoning_effort".into(), json!("high")),
        ]),
        output_contract: None,
    };
    let request_record =
        ProtectedRecord::new(id("request-record"), 1, serde_json::to_value(&request)?);
    let prompt = ProtectedRecord::new(id("prompt"), 1, json!({"text":"Use evidence"}));
    let snapshot = RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id("run"),
        request_digest: admission_digest(&request, &profile, None),
        request: request.clone(),
        scope: scope.clone(),
        timing: RunTiming::new(1000, profile.profile().limits.max_elapsed_ms.get()).unwrap(),
        reservations: vec![],
        limits: profile.profile().limits.clone(),
        profile,
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        usage: BudgetUsage::default(),
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: None,
        wait: None,
        outcome: None,
        assembly_ref: None,
        routing_snapshot_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    let started = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("started"),
        scope: scope.clone(),
        run_id: id("run"),
        session_id: id("session"),
        seq: 1.try_into()?,
        timestamp_ms: 1000,
        payload: RunEventPayload::RunStarted {
            request_ref: request_record.reference().clone(),
            profile_digest: snapshot.profile.profile_digest().clone(),
        },
    };
    let message = Message {
        message_id: id("user-message"),
        run_id: id("run"),
        sequence: 1.try_into()?,
        role: MessageRole::User,
        content: vec![ContentBlock::Content {
            content: request.input[0].clone(),
        }],
        origin: MessageOrigin::User,
        visibility: Visibility::UserAndModel,
    };
    let input = AdmissionInput {
        snapshot,
        prompt_snapshot: prompt.reference().clone(),
        messages: vec![message],
        events: vec![started],
        records: vec![request_record, prompt],
        require_durable: true,
    };
    let store = SqliteStateStore::open(&database)?;
    assert!(store.capabilities().durable);
    assert!(store.capabilities().cross_process_leases);
    let first = store.admit(&scope, input.clone()).await?;
    let replay = store.admit(&scope, input).await?;
    assert!(first.created);
    assert!(!replay.created);
    assert_eq!(first.state, replay.state);
    println!(
        "admission: created={}, retry_created={}, run={}",
        first.created, replay.created, replay.state.snapshot.run_id
    );
    let lease = store
        .acquire_lease(&scope, &id("run"), &id("worker"), 1000, 10_000)
        .await?;
    let mut next = first.state.snapshot.clone();
    next.revision = 1;
    next.last_event_seq = 2;
    next.status = RunStatus::Succeeded;
    next.phase = RunPhase::Finish;
    let outcome = RunOutcome {
        result: OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded,
        },
        output: vec![InputContent::Text {
            text: "Stored result".into(),
        }],
        artifacts: vec![],
        usage: next.usage.clone(),
        checkpoint_revision: 1,
        verification: None,
        unresolved_effects: vec![],
    };
    let outcome_record = ProtectedRecord::new(id("outcome"), 1, serde_json::to_value(&outcome)?);
    next.outcome = Some(outcome);
    let event = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("finished"),
        scope: scope.clone(),
        run_id: id("run"),
        session_id: id("session"),
        seq: 2.try_into()?,
        timestamp_ms: 1001,
        payload: RunEventPayload::RunFinished {
            outcome_ref: outcome_record.reference().clone(),
        },
    };
    let result = store
        .commit(
            &scope,
            &id("run"),
            CommitInput {
                expected_revision: 0,
                lease,
                now_ms: 1001,
                snapshot: next,
                messages: vec![],
                events: vec![event],
                records: vec![outcome_record],
            },
        )
        .await?;
    println!(
        "committed: revision={}, status={:?}, active_run={:?}",
        result.snapshot.revision, result.snapshot.status, result.session.active_run_id
    );
    let events = store.read_events(&scope, &id("run"), 0, 10).await?;
    println!(
        "event replay: count={}, last_seq={}",
        events.events.len(),
        events.last_available_seq
    );
    let foreign = Scope {
        tenant_id: id("another-tenant"),
        ..scope.clone()
    };
    let rejected = store.load(&foreign, &id("run")).await;
    assert!(matches!(&rejected, Err(error) if error.code == ErrorCode::StateNotFound));
    println!(
        "foreign scope: {}",
        match rejected {
            Err(error) => format!("{:?}", error.code),
            Ok(_) => "UNEXPECTED ACCESS".into(),
        }
    );
    drop(store);
    let status = std::process::Command::new(std::env::current_exe()?)
        .arg("verify")
        .arg(&database)
        .arg(std::process::id().to_string())
        .status()?;
    if !status.success() {
        return Err("independent SQLite reader failed".into());
    }
    println!(
        "SQLite consumer: atomic admission/commit, scope isolation, and independent process restoration passed"
    );
    Ok(())
}
```

## `tests/support/state_consumer.rs`

```rust
use serde_json::json;
use std::collections::BTreeSet;
use wickle::*;

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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let profile = AgentProfile::from_json(
        r#"{
      "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
      "name":"Assistant","description":"Storage example","instructions":{"text":"Use evidence"},
      "model_binding":"primary","tools":[],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
      "limits":{"max_model_calls":4,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
    }"#,
    )?;
    let profile = ProfileValidator::new(&Catalog)
        .validate(&profile, &scope)
        .await?;
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Inspect stored state".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    };
    let request_record =
        ProtectedRecord::new(id("request-record"), 1, serde_json::to_value(&request)?);
    let prompt = ProtectedRecord::new(id("prompt"), 1, json!({"text":"Use evidence"}));
    let snapshot = RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id("run"),
        request_digest: admission_digest(&request, &profile, None),
        request: request.clone(),
        scope: scope.clone(),
        timing: RunTiming::new(1000, profile.profile().limits.max_elapsed_ms.get()).unwrap(),
        reservations: vec![],
        limits: profile.profile().limits.clone(),
        profile,
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        usage: BudgetUsage::default(),
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: None,
        wait: None,
        outcome: None,
        assembly_ref: None,
        routing_snapshot_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    let started = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("started"),
        scope: scope.clone(),
        run_id: id("run"),
        session_id: id("session"),
        seq: 1.try_into()?,
        timestamp_ms: 1000,
        payload: RunEventPayload::RunStarted {
            request_ref: request_record.reference().clone(),
            profile_digest: snapshot.profile.profile_digest().clone(),
        },
    };
    let message = Message {
        message_id: id("user-message"),
        run_id: id("run"),
        sequence: 1.try_into()?,
        role: MessageRole::User,
        content: vec![ContentBlock::Content {
            content: request.input[0].clone(),
        }],
        origin: MessageOrigin::User,
        visibility: Visibility::UserAndModel,
    };
    let input = AdmissionInput {
        snapshot,
        prompt_snapshot: prompt.reference().clone(),
        messages: vec![message],
        events: vec![started],
        records: vec![request_record, prompt],
        require_durable: false,
    };
    let store = MemoryStateStore::new();
    let first = store.admit(&scope, input.clone()).await?;
    let replay = store.admit(&scope, input).await?;
    println!(
        "admission: created={}, retry_created={}, run={}",
        first.created, replay.created, replay.state.snapshot.run_id
    );
    let lease = store
        .acquire_lease(&scope, &id("run"), &id("worker"), 1000, 100)
        .await?;
    let mut next = first.state.snapshot.clone();
    next.revision = 1;
    next.last_event_seq = 2;
    next.status = RunStatus::Succeeded;
    next.phase = RunPhase::Finish;
    let outcome = RunOutcome {
        result: OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded,
        },
        output: vec![InputContent::Text {
            text: "Stored result".into(),
        }],
        artifacts: vec![],
        usage: next.usage.clone(),
        checkpoint_revision: 1,
        verification: None,
        unresolved_effects: vec![],
    };
    let outcome_record = ProtectedRecord::new(id("outcome"), 1, serde_json::to_value(&outcome)?);
    next.outcome = Some(outcome);
    let event = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("finished"),
        scope: scope.clone(),
        run_id: id("run"),
        session_id: id("session"),
        seq: 2.try_into()?,
        timestamp_ms: 1001,
        payload: RunEventPayload::RunFinished {
            outcome_ref: outcome_record.reference().clone(),
        },
    };
    let result = store
        .commit(
            &scope,
            &id("run"),
            CommitInput {
                expected_revision: 0,
                lease,
                now_ms: 1001,
                snapshot: next,
                messages: vec![],
                events: vec![event],
                records: vec![outcome_record],
            },
        )
        .await?;
    println!(
        "committed: revision={}, status={:?}, active_run={:?}",
        result.snapshot.revision, result.snapshot.status, result.session.active_run_id
    );
    let events = store.read_events(&scope, &id("run"), 0, 10).await?;
    println!(
        "event replay: count={}, last_seq={}",
        events.events.len(),
        events.last_available_seq
    );
    let foreign = Scope {
        tenant_id: id("another-tenant"),
        ..scope
    };
    let rejected = store.load(&foreign, &id("run")).await;
    println!(
        "foreign scope: {}",
        match rejected {
            Err(error) => format!("{:?}", error.code),
            Ok(_) => "UNEXPECTED ACCESS".into(),
        }
    );
    Ok(())
}
```
