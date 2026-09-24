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
