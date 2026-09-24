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
