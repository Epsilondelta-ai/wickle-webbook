//! Versioned catalog identity, exact binding validation, and immutable scoped lookups.

use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use wickle::*;
use wickle_model_router::ImmutableModelCatalog;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    }
}
fn reference(name: &str, version: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id(version),
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
fn features(values: &[&str]) -> BTreeSet<Id> {
    values.iter().map(|value| id(value)).collect()
}

fn model(provider: &str, version: &str) -> ModelDefinition {
    ModelDefinition {
        model_key: id("shared"),
        family: id("family"),
        provider: id(provider),
        model_id: id("wire-model"),
        model_version: id(version),
        version_semantics: VersionSemantics::Pinned,
        lifecycle: ModelLifecycle::Active,
        capabilities: ModelCapabilities {
            revision: id("model-capabilities"),
            features: features(&["text", "tools", "streaming"]),
            options_schema: json!({"type":"object","properties":{"temperature":{"type":"number","minimum":0,"maximum":2},"reasoning":{"enum":["low","high"]}},"required":[],"additionalProperties":false}),
            context_window: 4096.try_into().unwrap(),
            max_output_tokens: 512.try_into().unwrap(),
        },
        evidence: vec![ModelEvidence {
            source_ref: id("synthetic-release-metadata"),
            observed_at_ms: 1000,
        }],
    }
}
fn model_ref(model: &ModelDefinition) -> ModelDefinitionRef {
    ModelDefinitionRef {
        provider: model.provider.clone(),
        model_key: model.model_key.clone(),
        model_version: model.model_version.clone(),
    }
}
fn proof(binding: &mut ModelBinding, model: &ModelDefinition) {
    binding.evidence = vec![ModelValidationEvidence {
        kind: ModelValidationKind::ContractTest,
        binding_digest: binding.contract_digest(model).unwrap(),
        checked_at_ms: 1000,
        evidence_ref: id("synthetic-contract-proof"),
        passed: true,
    }];
}
fn binding(name: &str, version: &str, model: &ModelDefinition) -> ModelBinding {
    let mut binding = ModelBinding {
        default_options: Default::default(),
        binding: reference(name, version),
        model: model_ref(model),
        requested_model: model.model_id.clone(),
        adapter: reference("adapter", "1"),
        connection_ref: reference("connection", "1"),
        target: object(json!({"deployment":"deployment-a","region":"region-a"})),
        target_schema: json!({"type":"object","properties":{"deployment":{"type":"string","minLength":1},"region":{"enum":["region-a"]}},"required":["deployment","region"],"additionalProperties":false}),
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        deployment_revision: Some(id("deployment-revision-1")),
        version_semantics: VersionSemantics::Pinned,
        capabilities: ModelCapabilities {
            revision: id("binding-capabilities"),
            features: features(&["text", "tools"]),
            options_schema: json!({"type":"object","properties":{"temperature":{"type":"number","minimum":0,"maximum":1}},"required":[],"additionalProperties":false}),
            context_window: 1024.try_into().unwrap(),
            max_output_tokens: 128.try_into().unwrap(),
        },
        support: ModelSupportStatus::ContractTested,
        evidence: vec![],
    };
    proof(&mut binding, model);
    binding
}
fn snapshot(
    models: Vec<ModelDefinition>,
    bindings: Vec<ModelBinding>,
    aliases: Vec<ModelAlias>,
) -> ModelCatalogSnapshot {
    ModelCatalogSnapshot {
        revision: id("catalog-1"),
        scope: scope(),
        models,
        bindings,
        aliases,
    }
}
fn requirements() -> CatalogRequirements {
    CatalogRequirements {
        features: features(&["text"]),
        options: JsonObject::new(),
        input_tokens: 100,
        max_output_tokens: 32.try_into().unwrap(),
        version_policy: VersionPolicy::RequirePinned,
        min_support: ModelSupportStatus::ContractTested,
    }
}

#[tokio::test]
async fn the_same_provider_and_model_id_keep_two_releases_and_binding_revisions_available() {
    let first = model("provider-a", "release-one");
    let second = model("provider-a", "release-two");
    let first_binding = binding("primary", "first-binding-revision", &first);
    let second_binding = binding("primary", "second-binding-revision", &second);
    let catalog = ImmutableModelCatalog::new(snapshot(
        vec![first.clone(), second.clone()],
        vec![first_binding.clone(), second_binding.clone()],
        vec![],
    ))
    .unwrap();
    assert_eq!(
        catalog
            .get_model(&scope(), &id("catalog-1"), &model_ref(&first))
            .await
            .unwrap()
            .model_version,
        id("release-one")
    );
    assert_eq!(
        catalog
            .get_model(&scope(), &id("catalog-1"), &model_ref(&second))
            .await
            .unwrap()
            .model_version,
        id("release-two")
    );
    let first_resolved = catalog
        .get_binding(&scope(), &id("catalog-1"), &first_binding.binding)
        .await
        .unwrap();
    let second_resolved = catalog
        .get_binding(&scope(), &id("catalog-1"), &second_binding.binding)
        .await
        .unwrap();
    assert_eq!(
        first_resolved.model.model_id,
        second_resolved.model.model_id
    );
    assert_eq!(first_resolved.model.model_version, id("release-one"));
    assert_eq!(second_resolved.model.model_version, id("release-two"));
    assert_ne!(
        first_resolved
            .binding
            .contract_digest(&first_resolved.model)
            .unwrap(),
        second_resolved
            .binding
            .contract_digest(&second_resolved.model)
            .unwrap()
    );
    let mut missing = model_ref(&first);
    missing.model_version = id("unregistered-release");
    assert!(
        catalog
            .get_model(&scope(), &id("catalog-1"), &missing)
            .await
            .is_err()
    );
    assert!(
        catalog
            .get_binding(
                &scope(),
                &id("catalog-1"),
                &reference("primary", "unregistered-revision")
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn provider_api_deployment_and_adapter_combinations_do_not_collapse_into_one_model_identity()
{
    let direct = model("provider-a", "release-one");
    let hosted = model("provider-b", "release-one");
    let first = binding("first", "1", &direct);
    let mut second = binding("second", "1", &direct);
    second.api_contract = ApiContract {
        operation: id("other-operation"),
        version: id("other-api-version"),
    };
    second.adapter = reference("adapter", "2");
    second.connection_ref = reference("another-connection", "2");
    second
        .target
        .insert("deployment".into(), json!("deployment-b"));
    second.deployment_revision = Some(id("deployment-revision-2"));
    proof(&mut second, &direct);
    let third = binding("third", "1", &hosted);
    let catalog = ImmutableModelCatalog::new(snapshot(
        vec![direct, hosted],
        vec![first.clone(), second.clone(), third.clone()],
        vec![],
    ))
    .unwrap();
    let mut digests = BTreeSet::new();
    for expected in [first, second, third] {
        let actual = catalog
            .get_binding(&scope(), &id("catalog-1"), &expected.binding)
            .await
            .unwrap();
        assert_eq!(actual.binding, expected);
        assert_eq!(actual.model.provider, expected.model.provider);
        digests.insert(actual.binding.contract_digest(&actual.model).unwrap());
    }
    assert_eq!(digests.len(), 3);
}

#[tokio::test]
async fn explicit_aliases_keep_requested_names_separate_from_resolved_release_metadata() {
    let first = model("provider-a", "release-one");
    let second = model("provider-b", "release-two");
    let aliases = vec![
        ModelAlias {
            provider: first.provider.clone(),
            alias: id("friendly"),
            target: model_ref(&first),
        },
        ModelAlias {
            provider: second.provider.clone(),
            alias: id("friendly"),
            target: model_ref(&second),
        },
    ];
    let mut selected = binding("primary", "1", &first);
    selected.requested_model = id("friendly");
    proof(&mut selected, &first);
    let catalog = ImmutableModelCatalog::new(snapshot(
        vec![first.clone(), second.clone()],
        vec![selected.clone()],
        aliases,
    ))
    .unwrap();
    assert_eq!(
        catalog
            .resolve_alias(
                &scope(),
                &id("catalog-1"),
                &id("provider-a"),
                &id("friendly")
            )
            .await
            .unwrap(),
        model_ref(&first)
    );
    assert_eq!(
        catalog
            .resolve_alias(
                &scope(),
                &id("catalog-1"),
                &id("provider-b"),
                &id("friendly")
            )
            .await
            .unwrap(),
        model_ref(&second)
    );
    let resolved = catalog
        .get_binding(&scope(), &id("catalog-1"), &selected.binding)
        .await
        .unwrap();
    assert_eq!(resolved.binding.requested_model, id("friendly"));
    assert_eq!(resolved.model.model_id, id("wire-model"));
    assert_eq!(resolved.model.model_version, id("release-one"));
    assert!(
        catalog
            .resolve_alias(
                &scope(),
                &id("catalog-1"),
                &id("provider-a"),
                &id("unregistered")
            )
            .await
            .is_err()
    );
}

#[test]
fn aliases_require_exact_registered_leaves_and_do_not_follow_alias_chains_or_cycles() {
    let model = model("provider-a", "release-one");
    let alias_target = |name: &str| ModelDefinitionRef {
        model_key: id(name),
        ..model_ref(&model)
    };
    for aliases in [
        vec![ModelAlias {
            provider: model.provider.clone(),
            alias: id("first"),
            target: alias_target("missing"),
        }],
        vec![
            ModelAlias {
                provider: model.provider.clone(),
                alias: id("first"),
                target: alias_target("second"),
            },
            ModelAlias {
                provider: model.provider.clone(),
                alias: id("second"),
                target: model_ref(&model),
            },
        ],
        vec![
            ModelAlias {
                provider: model.provider.clone(),
                alias: id("first"),
                target: alias_target("second"),
            },
            ModelAlias {
                provider: model.provider.clone(),
                alias: id("second"),
                target: alias_target("first"),
            },
        ],
    ] {
        assert!(
            ImmutableModelCatalog::new(snapshot(vec![model.clone()], vec![], aliases)).is_err()
        );
    }
    for name in [model.model_key.clone(), model.model_id.clone()] {
        assert!(
            ImmutableModelCatalog::new(snapshot(
                vec![model.clone()],
                vec![],
                vec![ModelAlias {
                    provider: model.provider.clone(),
                    alias: name,
                    target: model_ref(&model)
                }]
            ))
            .is_err()
        );
    }
}

#[tokio::test]
async fn pinned_semantics_come_from_metadata_instead_of_dates_in_model_names() {
    for semantics in [
        VersionSemantics::Pinned,
        VersionSemantics::Alias,
        VersionSemantics::MutableDeployment,
        VersionSemantics::Unverified,
    ] {
        let mut model = model("custom-provider", "opaque-release-label");
        model.model_id = id(if semantics == VersionSemantics::Pinned {
            "undated-fixed-model"
        } else {
            "model-2026-09-12"
        });
        model.version_semantics = semantics;
        let mut selected = binding("primary", "1", &model);
        selected.version_semantics = semantics;
        proof(&mut selected, &model);
        let catalog =
            ImmutableModelCatalog::new(snapshot(vec![model], vec![selected.clone()], vec![]))
                .unwrap();
        let resolved = catalog
            .get_binding(&scope(), &id("catalog-1"), &selected.binding)
            .await
            .unwrap();
        assert_eq!(resolved.effective_version_semantics(), semantics);
        assert_eq!(
            resolved.validate(&requirements()).is_ok(),
            semantics == VersionSemantics::Pinned
        );
        let mut mutable = requirements();
        mutable.version_policy = VersionPolicy::AllowMutable;
        resolved.validate(&mutable).unwrap();
    }
}

#[tokio::test]
async fn each_binding_checks_its_own_features_options_and_token_limits() {
    let model = model("provider-a", "release-one");
    let first = binding("first", "1", &model);
    let mut second = binding("second", "1", &model);
    second.capabilities.revision = id("second-capabilities");
    second.capabilities.features = features(&["text"]);
    second.capabilities.options_schema = json!({"type":"object","properties":{"reasoning":{"enum":["low","high"]}},"required":[],"additionalProperties":false});
    second.capabilities.max_output_tokens = 64.try_into().unwrap();
    proof(&mut second, &model);
    let catalog = ImmutableModelCatalog::new(snapshot(
        vec![model],
        vec![first.clone(), second.clone()],
        vec![],
    ))
    .unwrap();
    let first = catalog
        .get_binding(&scope(), &id("catalog-1"), &first.binding)
        .await
        .unwrap();
    let second = catalog
        .get_binding(&scope(), &id("catalog-1"), &second.binding)
        .await
        .unwrap();
    let mut request = requirements();
    request.options = object(json!({"temperature":0.5}));
    first.validate(&request).unwrap();
    assert!(second.validate(&request).is_err());
    request.options = object(json!({"reasoning":"high"}));
    second.validate(&request).unwrap();
    assert!(first.validate(&request).is_err());
    request.options = JsonObject::new();
    request.features.insert(id("tools"));
    first.validate(&request).unwrap();
    assert!(second.validate(&request).is_err());
    request.features = features(&["text"]);
    request.max_output_tokens = 96.try_into().unwrap();
    first.validate(&request).unwrap();
    // The request supplies an upper bound; the selected binding lowers it to 64.
    second.validate(&request).unwrap();
    request = requirements();
    request.input_tokens = 1000;
    assert!(first.validate(&request).is_err());
    request.input_tokens = u64::MAX;
    assert!(first.validate(&request).is_err());
}

#[test]
fn bindings_cannot_advertise_features_or_limits_beyond_the_model_definition() {
    let original = model("provider-a", "release-one");
    for variant in 0..3 {
        let mut model = original.clone();
        let mut selected = binding("primary", "1", &model);
        selected.support = ModelSupportStatus::Planned;
        selected.evidence.clear();
        match variant {
            0 => model.capabilities.features = features(&["text"]),
            1 => model.capabilities.context_window = 768.try_into().unwrap(),
            2 => model.capabilities.max_output_tokens = 64.try_into().unwrap(),
            _ => unreachable!(),
        }
        assert!(ImmutableModelCatalog::new(snapshot(vec![model], vec![selected], vec![])).is_err());
    }
}

#[tokio::test]
async fn requested_options_must_satisfy_both_model_and_binding_schemas_without_coercion() {
    let mut model = model("provider-a", "release-one");
    model.capabilities.options_schema["properties"]["temperature"]["maximum"] = json!(0.2);
    let selected = binding("primary", "1", &model);
    let catalog =
        ImmutableModelCatalog::new(snapshot(vec![model], vec![selected.clone()], vec![])).unwrap();
    let resolved = catalog
        .get_binding(&scope(), &id("catalog-1"), &selected.binding)
        .await
        .unwrap();
    let mut request = requirements();
    request.options = object(json!({"temperature":0.1}));
    resolved.validate(&request).unwrap();
    for options in [
        json!({"temperature":0.5}),
        json!({"temperature":"0.1"}),
        json!({"unknown_option":true}),
    ] {
        request.options = object(options);
        assert!(resolved.validate(&request).is_err());
    }
}

#[tokio::test]
async fn a_pinned_binding_or_model_alone_cannot_upgrade_unpinned_metadata() {
    for (model_semantics, binding_semantics, expected) in [
        (
            VersionSemantics::Pinned,
            VersionSemantics::MutableDeployment,
            VersionSemantics::MutableDeployment,
        ),
        (
            VersionSemantics::Unverified,
            VersionSemantics::Pinned,
            VersionSemantics::Unverified,
        ),
    ] {
        let mut model = model("provider-a", "release-one");
        model.version_semantics = model_semantics;
        let mut selected = binding("primary", "1", &model);
        selected.version_semantics = binding_semantics;
        proof(&mut selected, &model);
        let catalog =
            ImmutableModelCatalog::new(snapshot(vec![model], vec![selected.clone()], vec![]))
                .unwrap();
        let resolved = catalog
            .get_binding(&scope(), &id("catalog-1"), &selected.binding)
            .await
            .unwrap();
        assert_eq!(resolved.effective_version_semantics(), expected);
        assert!(resolved.validate(&requirements()).is_err());
    }
}

#[test]
fn target_schemas_and_model_references_reject_mixed_or_unregistered_bindings() {
    let model = model("provider-a", "release-one");
    let mut wrong_target = binding("primary", "1", &model);
    wrong_target.target.insert("deployment".into(), json!(42));
    wrong_target.support = ModelSupportStatus::Planned;
    wrong_target.evidence.clear();
    assert!(
        ImmutableModelCatalog::new(snapshot(vec![model.clone()], vec![wrong_target], vec![]))
            .is_err()
    );
    let mut wrong_model = binding("primary", "1", &model);
    wrong_model.model.model_version = id("missing-release");
    wrong_model.support = ModelSupportStatus::Planned;
    wrong_model.evidence.clear();
    assert!(
        ImmutableModelCatalog::new(snapshot(vec![model.clone()], vec![wrong_model], vec![]))
            .is_err()
    );
    let mut wrong_provider = binding("primary", "1", &model);
    wrong_provider.model.provider = id("unregistered-provider");
    wrong_provider.support = ModelSupportStatus::Planned;
    wrong_provider.evidence.clear();
    assert!(
        ImmutableModelCatalog::new(snapshot(vec![model], vec![wrong_provider], vec![])).is_err()
    );
}

#[test]
fn duplicate_definitions_binding_revisions_and_aliases_are_registration_errors() {
    let model = model("provider-a", "release-one");
    let selected = binding("primary", "1", &model);
    assert!(
        ImmutableModelCatalog::new(snapshot(
            vec![model.clone(), model.clone()],
            vec![selected.clone()],
            vec![]
        ))
        .is_err()
    );
    assert!(
        ImmutableModelCatalog::new(snapshot(
            vec![model.clone()],
            vec![selected.clone(), selected],
            vec![]
        ))
        .is_err()
    );
    let alias = ModelAlias {
        provider: model.provider.clone(),
        alias: id("friendly"),
        target: model_ref(&model),
    };
    assert!(
        ImmutableModelCatalog::new(snapshot(vec![model], vec![], vec![alias.clone(), alias]))
            .is_err()
    );
}

#[tokio::test]
async fn inspection_preserves_lifecycle_while_execution_requirements_reject_unavailable_models() {
    for lifecycle in [
        ModelLifecycle::Active,
        ModelLifecycle::Deprecated,
        ModelLifecycle::Retired,
        ModelLifecycle::Unavailable,
    ] {
        let mut model = model("provider-a", "release-one");
        model.lifecycle = lifecycle;
        let selected = binding("primary", "1", &model);
        let catalog =
            ImmutableModelCatalog::new(snapshot(vec![model], vec![selected.clone()], vec![]))
                .unwrap();
        let resolved = catalog
            .get_binding(&scope(), &id("catalog-1"), &selected.binding)
            .await
            .unwrap();
        assert_eq!(resolved.model.lifecycle, lifecycle);
        assert_eq!(
            resolved.validate(&requirements()).is_ok(),
            matches!(
                lifecycle,
                ModelLifecycle::Active | ModelLifecycle::Deprecated
            )
        );
    }
}

#[tokio::test]
async fn support_status_requires_matching_successful_evidence_and_is_not_promoted_implicitly() {
    let model = model("provider-a", "release-one");
    let mut planned = binding("planned", "1", &model);
    planned.support = ModelSupportStatus::Planned;
    planned.evidence.clear();
    let catalog =
        ImmutableModelCatalog::new(snapshot(vec![model.clone()], vec![planned.clone()], vec![]))
            .unwrap();
    let resolved = catalog
        .get_binding(&scope(), &id("catalog-1"), &planned.binding)
        .await
        .unwrap();
    assert_eq!(resolved.binding.support, ModelSupportStatus::Planned);
    assert!(resolved.validate(&requirements()).is_err());
    let mut preparation = requirements();
    preparation.min_support = ModelSupportStatus::Planned;
    resolved.validate(&preparation).unwrap();

    let contract = binding("primary", "1", &model);
    let mut no_contract_proof = contract.clone();
    no_contract_proof.evidence.clear();
    assert!(
        ImmutableModelCatalog::new(snapshot(
            vec![model.clone()],
            vec![no_contract_proof],
            vec![]
        ))
        .is_err()
    );
    let mut live = contract.clone();
    live.support = ModelSupportStatus::LiveVerified;
    assert!(
        ImmutableModelCatalog::new(snapshot(vec![model.clone()], vec![live.clone()], vec![]))
            .is_err()
    );
    live.evidence.push(ModelValidationEvidence {
        kind: ModelValidationKind::LiveCheck,
        binding_digest: live.contract_digest(&model).unwrap(),
        checked_at_ms: 2000,
        evidence_ref: id("synthetic-live-proof"),
        passed: false,
    });
    assert!(
        ImmutableModelCatalog::new(snapshot(vec![model.clone()], vec![live.clone()], vec![]))
            .is_err()
    );
    live.evidence.last_mut().unwrap().passed = true;
    let catalog =
        ImmutableModelCatalog::new(snapshot(vec![model.clone()], vec![live.clone()], vec![]))
            .unwrap();
    let resolved = catalog
        .get_binding(&scope(), &id("catalog-1"), &live.binding)
        .await
        .unwrap();
    let mut requirements = requirements();
    requirements.min_support = ModelSupportStatus::LiveVerified;
    resolved.validate(&requirements).unwrap();
    live.evidence.last_mut().unwrap().binding_digest = canonical_digest(&json!("another-binding"));
    assert!(ImmutableModelCatalog::new(snapshot(vec![model], vec![live], vec![])).is_err());
}

#[test]
fn proof_for_one_binding_cannot_be_reused_after_a_target_api_or_adapter_change() {
    let model = model("provider-a", "release-one");
    let mut verified = binding("primary", "same-revision", &model);
    verified.support = ModelSupportStatus::LiveVerified;
    verified.evidence.push(ModelValidationEvidence {
        kind: ModelValidationKind::LiveCheck,
        binding_digest: verified.contract_digest(&model).unwrap(),
        checked_at_ms: 2000,
        evidence_ref: id("synthetic-live-proof"),
        passed: true,
    });
    ImmutableModelCatalog::new(snapshot(
        vec![model.clone()],
        vec![verified.clone()],
        vec![],
    ))
    .unwrap();
    for variant in 0..5 {
        let mut changed = verified.clone();
        match variant {
            0 => {
                changed
                    .target
                    .insert("deployment".into(), json!("another-valid-deployment"));
            }
            1 => changed.api_contract.operation = id("another-operation"),
            2 => changed.api_contract.version = id("another-api-version"),
            3 => changed.adapter.version = id("another-adapter-version"),
            4 => changed.deployment_revision = Some(id("another-deployment-revision")),
            _ => unreachable!(),
        }
        // The binding reference and its previously valid proof are deliberately unchanged.
        assert!(
            ImmutableModelCatalog::new(snapshot(vec![model.clone()], vec![changed], vec![]))
                .is_err()
        );
    }
}

#[tokio::test]
async fn catalog_revision_and_full_scope_are_required_for_every_lookup() {
    let model = model("provider-a", "release-one");
    let selected = binding("primary", "1", &model);
    let alias = ModelAlias {
        provider: model.provider.clone(),
        alias: id("friendly"),
        target: model_ref(&model),
    };
    let catalog = ImmutableModelCatalog::new(snapshot(
        vec![model.clone()],
        vec![selected.clone()],
        vec![alias],
    ))
    .unwrap();
    for (owner, revision) in [
        (
            Scope {
                tenant_id: id("foreign"),
                ..scope()
            },
            id("catalog-1"),
        ),
        (
            Scope {
                user_id: Some(id("user")),
                ..scope()
            },
            id("catalog-1"),
        ),
        (scope(), id("catalog-2")),
    ] {
        assert!(
            catalog
                .get_model(&owner, &revision, &model_ref(&model))
                .await
                .is_err()
        );
        assert!(
            catalog
                .get_binding(&owner, &revision, &selected.binding)
                .await
                .is_err()
        );
        assert!(
            catalog
                .resolve_alias(&owner, &revision, &model.provider, &id("friendly"))
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn restoring_a_catalog_keeps_the_pinned_revision_and_revalidates_changed_content() {
    let model = model("provider-a", "release-one");
    let first = binding("primary", "1", &model);
    let original =
        ImmutableModelCatalog::new(snapshot(vec![model.clone()], vec![first.clone()], vec![]))
            .unwrap();
    let serialized = serde_json::to_string(&original).unwrap();
    let restored = ImmutableModelCatalog::restore(&serialized, &original.digest()).unwrap();
    assert_eq!(
        restored
            .get_binding(&scope(), &id("catalog-1"), &first.binding)
            .await
            .unwrap()
            .binding,
        first
    );
    let mut next_binding = binding("primary", "2", &model);
    next_binding
        .target
        .insert("deployment".into(), json!("replacement"));
    proof(&mut next_binding, &model);
    let mut next = snapshot(vec![model], vec![next_binding], vec![]);
    next.revision = id("catalog-2");
    let next = ImmutableModelCatalog::new(next).unwrap();
    assert!(
        ImmutableModelCatalog::restore(&serde_json::to_string(&next).unwrap(), &original.digest())
            .is_err()
    );
    assert_eq!(
        original
            .get_binding(&scope(), &id("catalog-1"), &first.binding)
            .await
            .unwrap()
            .binding
            .target["deployment"],
        json!("deployment-a")
    );
    assert!(
        next.get_binding(&scope(), &id("catalog-2"), &first.binding)
            .await
            .is_err()
    );

    let mut corrupt = original.snapshot().clone();
    corrupt.bindings[0]
        .target
        .insert("unexpected".into(), json!(true));
    corrupt.bindings[0].support = ModelSupportStatus::Planned;
    corrupt.bindings[0].evidence.clear();
    let encoded = serde_json::to_value(&corrupt).unwrap();
    assert!(
        ImmutableModelCatalog::restore(&encoded.to_string(), &canonical_digest(&encoded)).is_err()
    );
}

struct CountingCatalog {
    inner: Arc<ImmutableModelCatalog>,
    reads: AtomicUsize,
}
impl ModelCatalog for CountingCatalog {
    fn revision(&self) -> &Id {
        self.inner.revision()
    }
    fn scope(&self) -> &Scope {
        self.inner.scope()
    }
    fn get_model<'a>(
        &'a self,
        scope: &'a Scope,
        revision: &'a Id,
        model: &'a ModelDefinitionRef,
    ) -> PortFuture<'a, ModelDefinition> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.get_model(scope, revision, model).await
        })
    }
    fn get_binding<'a>(
        &'a self,
        scope: &'a Scope,
        revision: &'a Id,
        binding: &'a VersionedRef,
    ) -> PortFuture<'a, ResolvedCatalogBinding> {
        self.inner.get_binding(scope, revision, binding)
    }
    fn resolve_alias<'a>(
        &'a self,
        scope: &'a Scope,
        revision: &'a Id,
        provider: &'a Id,
        alias: &'a Id,
    ) -> PortFuture<'a, ModelDefinitionRef> {
        self.inner.resolve_alias(scope, revision, provider, alias)
    }
}

#[tokio::test]
async fn distinct_concrete_catalogs_can_be_read_through_the_public_dynamic_trait() {
    let model = model("new-provider-without-core-changes", "release/opaque");
    let inner = Arc::new(
        ImmutableModelCatalog::new(snapshot(vec![model.clone()], vec![], vec![])).unwrap(),
    );
    let counted = Arc::new(CountingCatalog {
        inner: inner.clone(),
        reads: AtomicUsize::new(0),
    });
    let catalogs: Vec<Arc<dyn ModelCatalog>> = vec![inner, counted.clone()];
    for catalog in catalogs {
        let actual = catalog
            .get_model(&scope(), &id("catalog-1"), &model_ref(&model))
            .await
            .unwrap();
        assert_eq!(actual.model_version, id("release/opaque"));
        assert_eq!(actual.provider, model.provider);
    }
    assert_eq!(counted.reads.load(Ordering::SeqCst), 1);
}
