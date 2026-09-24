# 12장 전체 Rust 구현과 테스트

[강의로](../12-catalog.md) · [전체 변경 패치](../solutions/12-catalog.patch)

기준 `3fdf792ffef46db13679472f3116b8f252e1550a`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle-model-router/src/lib.rs`

```rust
//! Immutable, scope-bound model catalog for Wickle applications.
//!
//! Catalog lookups do not invoke models, load environment variables, or select a
//! fallback. Metadata contracts live in `wickle`; this crate depends on the core.

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

## `crates/wickle-model-router/tests/catalog.rs`

```rust
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
    assert!(second.validate(&request).is_err());
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
```

## `crates/wickle/src/context_projection.rs`

```rust
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, io,
    num::NonZeroU64,
};

use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Value, json};

use crate::{
    AgentProfile, CompiledTool, ComponentKind, ContentBlock, ContractError, ErrorCode, Id,
    InputContent, Instructions, JsonDigest, JsonObject, Message, MessageOrigin, MessageRole,
    ModelContent, ModelMessage, ModelOutput, ModelPurpose, ModelRequest, ModelResponseLimits,
    ModelRole, ModelTool, OpaqueContinuation, RecordRef, ResolvedComponent, ResolvedModelRoute,
    ResolvedProfile, RunRequest, Scope, ToolBindingRef, ToolResultStatus, VersionedRef, Visibility,
    parse_json, serialization::data_digest,
};

/// Version of the session prefix and byte-bounded projection contract.
pub const CONTEXT_ASSEMBLER_VERSION: &str = "wickle.context-assembler.v1";

/// Instruction data already resolved and authorized by the Host; no loader is invoked here.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstructionAssetContent {
    /// Exact instruction asset selected by the profile.
    pub asset: VersionedRef,
    /// Complete text to pin; it is never silently truncated.
    pub text: String,
}

impl fmt::Debug for InstructionAssetContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InstructionAssetContent")
            .field("asset", &self.asset)
            .finish_non_exhaustive()
    }
}

/// A trusted assembly's mapping from a selected profile reference to its compiled tool.
/// The later adapter factory must attest that an export actually supplies this descriptor.
#[derive(Debug, Clone)]
pub struct PromptToolBinding {
    /// Exact selected catalog reference or adapter export, including alias/configuration.
    pub selection: ToolBindingRef,
    /// Validated immutable input split; its full schema is not copied into the prefix.
    pub compiled: CompiledTool,
}

/// Initial skill listing metadata, deliberately separate from skill body loading.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillManifest {
    /// Exact selected skill identity and version.
    pub skill: VersionedRef,
    /// Short public listing name.
    pub name: String,
    /// Public purpose description, not an automatically executed instruction body.
    pub description: String,
    /// Trusted catalog manifest identity pinned with this listing.
    pub manifest_digest: JsonDigest,
}

impl fmt::Debug for SkillManifest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SkillManifest")
            .field("skill", &self.skill)
            .field("manifest_digest", &self.manifest_digest)
            .finish_non_exhaustive()
    }
}

/// Model-facing part of a tool pinned into the session prefix.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedPromptTool {
    /// Exact profile selection, retaining alias and binding identity.
    pub selection: ToolBindingRef,
    /// Exact underlying tool descriptor identity.
    pub tool: VersionedRef,
    /// Compiler contract used for input projection.
    pub compiler_version: String,
    /// Full compiled input-contract digest, without its hidden schemas or values.
    pub compiled_digest: JsonDigest,
    /// Original descriptor identity used by stored core ToolCall records.
    pub descriptor_digest: JsonDigest,
    /// Identity of the derived model-input schema.
    pub model_schema_digest: JsonDigest,
    /// Only the model-visible tool schema and public description.
    pub model_tool: ModelTool,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PromptData {
    assembler_version: String,
    scope: Scope,
    profile: AgentProfile,
    initial_resolution_digest: JsonDigest,
    pinned_components: Vec<ResolvedComponent>,
    host_instructions: Vec<String>,
    profile_asset: Option<InstructionAssetContent>,
    tools: Vec<PinnedPromptTool>,
    skills: Vec<SkillManifest>,
}

/// Owned session prefix. It can be serialized for protected storage but cannot be
/// deserialized without verifying a trusted expected digest, scope and profile.
/// Its digest equals the digest of the serialized value stored by ProtectedRecord.
#[derive(Clone)]
pub struct PromptSnapshot {
    data: PromptData,
    digest: JsonDigest,
}

impl Serialize for PromptSnapshot {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.data.serialize(serializer)
    }
}
impl fmt::Debug for PromptSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PromptSnapshot")
            .field("digest", &self.digest)
            .field("tool_count", &self.data.tools.len())
            .field("skill_count", &self.data.skills.len())
            .finish_non_exhaustive()
    }
}

impl PromptSnapshot {
    /// Pin already-authorized assets in profile order. This does not create adapter
    /// factories, fetch instructions, or load skill bodies. Profile text cannot
    /// delete or replace the independently owned Host message. Actual instruction
    /// adherence within a provider's system channel still requires evaluation;
    /// execution permissions are enforced separately by PolicyGate.
    pub fn create(
        profile: &ResolvedProfile,
        host_instructions: Vec<String>,
        profile_asset: Option<InstructionAssetContent>,
        mut tools: Vec<PromptToolBinding>,
        mut skills: Vec<SkillManifest>,
    ) -> Result<Self, ContractError> {
        if tools.len() != profile.profile().tools.len()
            || skills.len() != profile.profile().skills.len()
        {
            return Err(invalid("prompt.selections"));
        }
        let mut pinned_tools = Vec::new();
        for selection in &profile.profile().tools {
            let index = tools
                .iter()
                .position(|binding| &binding.selection == selection)
                .ok_or_else(|| invalid("prompt.tools"))?;
            let binding = tools.remove(index);
            let mut model_tool = binding.compiled.to_model_tool();
            if let ToolBindingRef::Export(export) = selection {
                if let Some(alias) = &export.alias {
                    model_tool.name = alias.clone();
                }
            }
            pinned_tools.push(PinnedPromptTool {
                selection: selection.clone(),
                tool: binding.compiled.descriptor().tool.clone(),
                compiler_version: binding.compiled.compiler_version().into(),
                compiled_digest: binding.compiled.digest().clone(),
                descriptor_digest: binding.compiled.descriptor_digest().clone(),
                model_schema_digest: binding.compiled.model_schema_digest().clone(),
                model_tool,
            });
        }
        let mut pinned_skills = Vec::new();
        for selection in &profile.profile().skills {
            let index = skills
                .iter()
                .position(|manifest| {
                    manifest.skill.id == selection.skill_id
                        && manifest.skill.version == selection.version
                })
                .ok_or_else(|| invalid("prompt.skills"))?;
            pinned_skills.push(skills.remove(index));
        }
        let data = PromptData {
            assembler_version: CONTEXT_ASSEMBLER_VERSION.into(),
            scope: profile.scope().clone(),
            profile: profile.profile().clone(),
            initial_resolution_digest: profile.resolution_digest().clone(),
            pinned_components: non_model_components(profile),
            host_instructions,
            profile_asset,
            tools: pinned_tools,
            skills: pinned_skills,
        };
        let snapshot = Self {
            digest: data_digest(&data),
            data,
        };
        snapshot.validate_data()?;
        Ok(snapshot)
    }

    /// Canonical identity of the exact protected serialized prefix.
    pub fn digest(&self) -> JsonDigest {
        self.digest.clone()
    }
    /// Read pinned public tool metadata and identities, without hidden input schemas.
    pub fn tools(&self) -> &[PinnedPromptTool] {
        &self.data.tools
    }
    /// Read the original selected skill listings, without fetching newer versions.
    pub fn skills(&self) -> &[SkillManifest] {
        &self.data.skills
    }
    /// Read the authenticated scope in which this prefix was pinned.
    pub fn scope(&self) -> &Scope {
        &self.data.scope
    }

    /// Restore a protected record using its trusted digest and the current run's
    /// resolved profile. A new run may resolve a different model binding only.
    /// Resume must continue to use the original run's profile and selected route;
    /// this method is not an authorization to replace either during a run.
    pub fn restore(
        input: &str,
        expected_digest: &JsonDigest,
        profile: &ResolvedProfile,
        scope: &Scope,
    ) -> Result<Self, ContractError> {
        let data: PromptData =
            serde_json::from_value(parse_json(input).map_err(|_| invalid("prompt"))?)
                .map_err(|_| invalid("prompt"))?;
        let snapshot = Self {
            digest: data_digest(&data),
            data,
        };
        snapshot.validate_for(profile, scope, expected_digest)?;
        Ok(snapshot)
    }

    /// Require the stored prefix identity, scope, profile, and all non-model assets.
    pub fn validate_for(
        &self,
        profile: &ResolvedProfile,
        scope: &Scope,
        expected_digest: &JsonDigest,
    ) -> Result<(), ContractError> {
        if &self.digest != expected_digest
            || &self.data.scope != scope
            || profile.scope() != scope
            || self.data.profile.digest() != *profile.profile_digest()
            || self.data.pinned_components != non_model_components(profile)
        {
            return Err(mismatch("prompt"));
        }
        self.validate_data()
    }

    fn validate_data(&self) -> Result<(), ContractError> {
        if self.data.assembler_version != CONTEXT_ASSEMBLER_VERSION
            || data_digest(&self.data) != self.digest
        {
            return Err(mismatch("prompt.version"));
        }
        match (&self.data.profile.instructions, &self.data.profile_asset) {
            (Instructions::Text(_), None) => {}
            (Instructions::Asset(reference), Some(asset)) if reference.asset_ref == asset.asset => {
            }
            _ => return Err(mismatch("prompt.instructions")),
        }
        if self.data.tools.len() != self.data.profile.tools.len()
            || self.data.skills.len() != self.data.profile.skills.len()
        {
            return Err(mismatch("prompt.selections"));
        }
        let mut names = BTreeSet::new();
        for (selection, tool) in self.data.profile.tools.iter().zip(&self.data.tools) {
            if selection != &tool.selection
                || !names.insert(&tool.model_tool.name)
                || crate::canonical_digest(&tool.model_tool.model_input_schema)
                    != tool.model_schema_digest
            {
                return Err(mismatch("prompt.tools"));
            }
            match selection {
                ToolBindingRef::Catalog(reference) => {
                    if tool.tool.id != reference.tool_id || tool.tool.version != reference.version {
                        return Err(mismatch("prompt.tools"));
                    }
                }
                ToolBindingRef::Export(export) => {
                    let adapter = self
                        .data
                        .profile
                        .adapters
                        .as_ref()
                        .and_then(|adapters| {
                            adapters
                                .iter()
                                .find(|adapter| adapter.binding_id == export.adapter_binding)
                        })
                        .ok_or_else(|| mismatch("prompt.export"))?;
                    if !self.data.pinned_components.iter().any(|component| {
                        component.reference.kind == ComponentKind::Adapter
                            && component.reference.id == adapter.adapter_id
                            && component.reference.version.as_ref() == Some(&adapter.version)
                    }) || export
                        .alias
                        .as_ref()
                        .is_some_and(|alias| alias != &tool.model_tool.name)
                    {
                        return Err(mismatch("prompt.export"));
                    }
                }
            }
        }
        for (selected, manifest) in self.data.profile.skills.iter().zip(&self.data.skills) {
            if selected.skill_id != manifest.skill.id || selected.version != manifest.skill.version
            {
                return Err(mismatch("prompt.skills"));
            }
        }
        Ok(())
    }

    fn prefix(&self) -> Vec<ModelMessage> {
        let profile_text = match &self.data.profile.instructions {
            Instructions::Text(instructions) => instructions.text.clone(),
            Instructions::Asset(_) => self
                .data
                .profile_asset
                .as_ref()
                .expect("validated instruction asset")
                .text
                .clone(),
        };
        let mut messages = vec![
            ModelMessage {
                role: ModelRole::System,
                content: self
                    .data
                    .host_instructions
                    .iter()
                    .map(|text| ModelContent::Text { text: text.clone() })
                    .collect(),
            },
            ModelMessage {
                role: ModelRole::System,
                content: vec![ModelContent::Text { text: profile_text }],
            },
        ];
        if !self.data.skills.is_empty() {
            messages.push(ModelMessage {
                role: ModelRole::User,
                content: vec![ModelContent::Json {
                    value: json!({"kind":"available_skills", "skills":self.data.skills}),
                }],
            });
        }
        messages
    }
}

fn non_model_components(profile: &ResolvedProfile) -> Vec<ResolvedComponent> {
    profile
        .components()
        .iter()
        .filter(|component| component.reference.kind != ComponentKind::ModelBinding)
        .cloned()
        .collect()
}

/// Source classification of already-authorized context data. None grants system authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextOrigin {
    /// Additional user-provided context, distinct from the preserved original request.
    User,
    /// Data associated with a selected pinned skill; no loader runs here.
    Skill,
    /// Data associated with a selected tool.
    Tool,
    /// External retrieved data, not trusted instructions.
    Retrieval,
    /// Recalled memory, not a policy grant.
    Memory,
    /// Verification feedback, not a Host instruction replacement.
    Verification,
}

/// Scope of context lifetime. An item outside its lifetime is explicitly omitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextLifetime {
    /// Context valid throughout one session.
    Session {
        /// Owning session.
        session_id: Id,
    },
    /// Context valid during one run.
    Run {
        /// Owning run.
        run_id: Id,
    },
    /// Context valid only for one logical model step.
    Step {
        /// Owning run.
        run_id: Id,
        /// Logical step, preserved across physical retries.
        model_step_id: Id,
    },
}

/// Selection importance, independent of source authority and provider role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextPriority {
    /// Fail if this active item cannot fit in full.
    Required,
    /// Include whole if remaining bounds permit it.
    Optional,
}

/// Data with explicit source, scope, integrity and lifetime. Constructing this DTO
/// does not authenticate provenance; callers must authorize sources before supply.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextItem {
    /// Stable source item identity.
    pub item_id: Id,
    /// Claimed source classification, retained in a data envelope.
    pub origin: ContextOrigin,
    /// Exact source/asset identity and version.
    pub source_ref: VersionedRef,
    /// Authenticated source scope supplied by the Host.
    pub scope: Scope,
    /// Explicitly selected content, not a system map or raw protected record.
    pub content: Vec<InputContent>,
    /// Digest of all other fields, checked again at projection.
    pub digest: JsonDigest,
    /// Session/run/step applicability.
    pub lifetime: ContextLifetime,
    /// Required versus optional selection, without elevated instruction authority.
    pub priority_class: ContextPriority,
}

impl ContextItem {
    /// Own supplied data and compute its source/lifetime/content identity.
    pub fn new(
        item_id: Id,
        origin: ContextOrigin,
        source_ref: VersionedRef,
        scope: Scope,
        content: Vec<InputContent>,
        lifetime: ContextLifetime,
        priority_class: ContextPriority,
    ) -> Self {
        let digest = data_digest(&(
            &item_id,
            origin,
            &source_ref,
            &scope,
            &content,
            &lifetime,
            priority_class,
        ));
        Self {
            item_id,
            origin,
            source_ref,
            scope,
            content,
            digest,
            lifetime,
            priority_class,
        }
    }
    fn valid_digest(&self) -> bool {
        self.digest
            == data_digest(&(
                &self.item_id,
                self.origin,
                &self.source_ref,
                &self.scope,
                &self.content,
                &self.lifetime,
                self.priority_class,
            ))
    }
}
impl fmt::Debug for ContextItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContextItem")
            .field("item_id", &self.item_id)
            .field("origin", &self.origin)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

/// Already-authorized typed provider replay data, not a generic JSON record loader.
#[derive(Debug, Clone)]
pub struct ScopedOpaque {
    /// Scope from which the protected record was read.
    pub scope: Scope,
    /// Exact reference whose digest covers the serialized OpaqueContinuation.
    pub reference: RecordRef,
    /// Provider that owns the record.
    pub provider: Id,
    /// Typed continuation with an exact route identity.
    pub continuation: OpaqueContinuation,
}

/// Finite projection size, separate from model token context capacity and usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionLimits {
    /// Maximum serialized final ModelRequest bytes, including schemas and metadata.
    pub max_bytes: usize,
    /// Maximum projected content blocks plus model tool definitions.
    pub max_items: usize,
}

/// Read-only projection inputs. Transcript must come from a trusted, scoped session
/// store; Message alone cannot authenticate its owner or prove history completeness.
pub struct ProjectionInput<'a> {
    /// Original resolved profile of this run; never re-resolve it during resume.
    pub profile: &'a ResolvedProfile,
    /// Authenticated execution scope.
    pub scope: &'a Scope,
    /// Current owning run.
    pub run_id: &'a Id,
    /// Logical step, identical to request_id before physical invocation allocation.
    pub model_step_id: &'a Id,
    /// Original persisted run request.
    pub current_request: &'a RunRequest,
    /// Exact stored user message containing that request, to prevent duplication.
    pub current_request_message_id: &'a Id,
    /// Owned-store history borrowed without mutation, including the current user message.
    pub transcript: &'a [Message],
    /// Already-authorized context items; no external source is queried here.
    pub context_items: &'a [ContextItem],
    /// Already-authorized opaque records with typed scope/provider/route metadata.
    pub opaque_records: &'a [ScopedOpaque],
    /// Trusted digest from the session's pinned prompt record.
    pub expected_prompt_digest: &'a JsonDigest,
    /// Logical step identity; ModelExchange later assigns a separate physical request ID.
    pub request_id: Id,
    /// Accounting purpose of this invocation.
    pub purpose: ModelPurpose,
    /// Already-selected immutable model route.
    pub route: ResolvedModelRoute,
    /// Already-resolved requested output mode.
    pub output: ModelOutput,
    /// Provider output-token request, not an estimate of input bytes.
    pub max_output_tokens: NonZeroU64,
    /// Host-owned logical options preserved in the final ModelRequest, outside prompt content.
    /// The selected catalog schemas and adapter define supported keys and wire mapping.
    pub options: JsonObject,
    /// Provider request/response decoding bounds.
    pub response_limits: ModelResponseLimits,
    /// Byte/item projection bounds, not a tokenizer or model context-window check.
    pub limits: ProjectionLimits,
}

/// Separate model projection and explicit selection provenance. No original messages change.
#[derive(Debug)]
pub struct ContextProjection {
    /// Complete prepared model request.
    pub request: ModelRequest,
    /// Original message identities represented in the model request.
    pub selected_message_ids: Vec<Id>,
    /// Original message identities omitted by visibility or whole-run selection.
    pub dropped_message_ids: Vec<Id>,
    /// Active supplied context items included in full.
    pub selected_context_ids: Vec<Id>,
    /// Context items omitted by lifetime or optional-item bounds.
    pub dropped_context_ids: Vec<Id>,
    /// Identity of the unchanged session prefix.
    pub prompt_digest: JsonDigest,
}

/// Prefix reuse and conservative selection without retrieval, loading or compaction.
#[derive(Debug, Clone, Copy, Default)]
pub struct ContextAssembler;

struct RunGroup {
    run_id: Id,
    messages: Vec<(Id, ModelMessage)>,
    has_tool_round: bool,
    has_unknown: bool,
}

impl ContextAssembler {
    /// Construct an assembler without doing I/O.
    pub fn new() -> Self {
        Self
    }

    /// Preserve the fixed prefix and all model-visible current-run messages. Older
    /// complete runs and optional items are added newest first without splitting
    /// tool rounds. The latest visible tool round and runs with unknown tool results
    /// are mandatory. Any unfinished round or oversized required input fails.
    /// Byte bounds do not claim to estimate or enforce provider token context size.
    pub fn project(
        &self,
        snapshot: &PromptSnapshot,
        input: ProjectionInput<'_>,
    ) -> Result<ContextProjection, ContractError> {
        snapshot.validate_for(input.profile, input.scope, input.expected_prompt_digest)?;
        if input.request_id != *input.model_step_id
            || input.limits.max_bytes == 0
            || input.limits.max_items == 0
        {
            return Err(invalid("projection.identity_or_limits"));
        }
        validate_current_request(&input)?;
        let groups = project_transcript(snapshot, &input)?;
        let current = groups
            .iter()
            .position(|group| &group.run_id == input.run_id)
            .ok_or_else(|| invalid("projection.current_run"))?;
        if current + 1 != groups.len() {
            return Err(invalid("projection.incomplete_round"));
        }
        let mut selected_groups = BTreeSet::from([current]);
        if let Some(index) = groups.iter().rposition(|group| group.has_tool_round) {
            selected_groups.insert(index);
        }
        selected_groups.extend(
            groups
                .iter()
                .enumerate()
                .filter(|(_, group)| group.has_unknown)
                .map(|(index, _)| index),
        );
        let mut context = Vec::new();
        let mut active = Vec::new();
        let mut seen_context = BTreeSet::new();
        for item in input.context_items {
            if !seen_context.insert(&item.item_id) || !item.valid_digest() {
                return Err(invalid("context_item.digest"));
            }
            if &item.scope != input.scope {
                return Err(mismatch("context_item.scope"));
            }
            if item.origin == ContextOrigin::Skill
                && !snapshot
                    .data
                    .skills
                    .iter()
                    .any(|manifest| manifest.skill == item.source_ref)
            {
                return Err(mismatch("context_item.skill"));
            }
            if item.origin == ContextOrigin::Tool
                && !snapshot
                    .data
                    .tools
                    .iter()
                    .any(|tool| tool.tool == item.source_ref)
            {
                return Err(mismatch("context_item.tool"));
            }
            let applicable = match &item.lifetime {
                ContextLifetime::Session { session_id } => {
                    session_id == &input.current_request.session_id
                }
                ContextLifetime::Run { run_id } => run_id == input.run_id,
                ContextLifetime::Step {
                    run_id,
                    model_step_id,
                } => run_id == input.run_id && model_step_id == input.model_step_id,
            };
            let message = if applicable {
                Some(ModelMessage {
                    role: ModelRole::User,
                    content: vec![ModelContent::Json {
                        value: json!({
                            "kind":"context_data", "item_id":item.item_id, "origin":item.origin,
                            "source_ref":item.source_ref,
                            "content":item.content.iter().map(|content| safe_value(content, input.scope)).collect::<Result<Vec<_>,_>>()?
                        }),
                    }],
                })
            } else {
                None
            };
            active.push(applicable);
            context.push(message);
        }
        let mut selected_context: BTreeSet<usize> = input
            .context_items
            .iter()
            .enumerate()
            .filter(|(index, item)| {
                active[*index] && item.priority_class == ContextPriority::Required
            })
            .map(|(index, _)| index)
            .collect();
        let make_request = |selected_groups: &BTreeSet<usize>,
                            selected_context: &BTreeSet<usize>| {
            let mut messages = snapshot.prefix();
            for index in selected_groups {
                messages.extend(
                    groups[*index]
                        .messages
                        .iter()
                        .map(|(_, message)| message.clone()),
                );
            }
            for index in selected_context {
                messages.push(context[*index].as_ref().expect("active context").clone());
            }
            ModelRequest {
                request_id: input.request_id.clone(),
                purpose: input.purpose,
                route: input.route.clone(),
                messages,
                tools: snapshot
                    .data
                    .tools
                    .iter()
                    .map(|tool| tool.model_tool.clone())
                    .collect(),
                output: input.output.clone(),
                max_output_tokens: input.max_output_tokens,
                options: input.options.clone(),
                limits: input.response_limits.clone(),
            }
        };
        if !fits(
            &make_request(&selected_groups, &selected_context),
            &input.limits,
        ) {
            return Err(budget());
        }
        for index in (0..current).rev() {
            if selected_groups.contains(&index) {
                continue;
            }
            selected_groups.insert(index);
            if !fits(
                &make_request(&selected_groups, &selected_context),
                &input.limits,
            ) {
                selected_groups.remove(&index);
            }
        }
        for index in (0..input.context_items.len()).rev() {
            if !active[index]
                || input.context_items[index].priority_class == ContextPriority::Required
            {
                continue;
            }
            selected_context.insert(index);
            if !fits(
                &make_request(&selected_groups, &selected_context),
                &input.limits,
            ) {
                selected_context.remove(&index);
            }
        }
        let request = make_request(&selected_groups, &selected_context);
        request
            .validate()
            .map_err(|_| invalid("projection.model_request"))?;
        let selected_message_ids: Vec<_> = selected_groups
            .iter()
            .flat_map(|index| groups[*index].messages.iter().map(|(id, _)| id.clone()))
            .collect();
        let selected_ids: BTreeSet<_> = selected_message_ids.iter().collect();
        Ok(ContextProjection {
            request,
            dropped_message_ids: input
                .transcript
                .iter()
                .filter(|message| !selected_ids.contains(&message.message_id))
                .map(|message| message.message_id.clone())
                .collect(),
            selected_message_ids,
            selected_context_ids: selected_context
                .iter()
                .map(|index| input.context_items[*index].item_id.clone())
                .collect(),
            dropped_context_ids: input
                .context_items
                .iter()
                .enumerate()
                .filter(|(index, _)| !selected_context.contains(index))
                .map(|(_, item)| item.item_id.clone())
                .collect(),
            prompt_digest: snapshot.digest(),
        })
    }
}

fn validate_current_request(input: &ProjectionInput<'_>) -> Result<(), ContractError> {
    let message = input
        .transcript
        .iter()
        .find(|message| &message.message_id == input.current_request_message_id)
        .ok_or_else(|| invalid("projection.current_request"))?;
    if &message.run_id != input.run_id
        || message.role != MessageRole::User
        || message.origin != MessageOrigin::User
        || !visible(message)
    {
        return Err(invalid("projection.current_request"));
    }
    let contents: Option<Vec<_>> = message
        .content
        .iter()
        .map(|block| match block {
            ContentBlock::Content { content } => Some(content),
            _ => None,
        })
        .collect();
    if contents.as_deref()
        != Some(
            input
                .current_request
                .input
                .iter()
                .collect::<Vec<_>>()
                .as_slice(),
        )
    {
        return Err(mismatch("projection.current_request"));
    }
    Ok(())
}

struct PendingCall {
    message_id: Id,
    provider_call_id: Id,
    visible: bool,
    known: bool,
}

fn project_transcript(
    snapshot: &PromptSnapshot,
    input: &ProjectionInput<'_>,
) -> Result<Vec<RunGroup>, ContractError> {
    let mut groups = Vec::new();
    let mut seen_messages = BTreeSet::new();
    let mut seen_runs = BTreeSet::new();
    let mut previous_sequence = 0;
    let mut cursor = 0;
    while cursor < input.transcript.len() {
        let run_id = input.transcript[cursor].run_id.clone();
        if !seen_runs.insert(run_id.clone()) {
            return Err(invalid("transcript.run_order"));
        }
        let end = input.transcript[cursor..]
            .iter()
            .position(|message| message.run_id != run_id)
            .map_or(input.transcript.len(), |offset| cursor + offset);
        let mut projected = Vec::new();
        let mut has_tool_round = false;
        let mut has_unknown = false;
        let mut pending: BTreeMap<Id, PendingCall> = BTreeMap::new();
        let mut seen_calls = BTreeSet::new();
        for message in &input.transcript[cursor..end] {
            if message.sequence.get() <= previous_sequence
                || !seen_messages.insert(&message.message_id)
            {
                return Err(invalid("transcript.order"));
            }
            previous_sequence = message.sequence.get();
            let is_visible = visible(message);
            if is_visible {
                match message.role {
                    MessageRole::System => return Err(invalid("transcript.system_role")),
                    MessageRole::Assistant if message.origin != MessageOrigin::Model => {
                        return Err(invalid("transcript.origin"));
                    }
                    MessageRole::Tool if message.origin != MessageOrigin::Tool => {
                        return Err(invalid("transcript.origin"));
                    }
                    MessageRole::User
                        if matches!(
                            message.origin,
                            MessageOrigin::Host
                                | MessageOrigin::Profile
                                | MessageOrigin::Model
                                | MessageOrigin::Tool
                        ) =>
                    {
                        return Err(invalid("transcript.origin"));
                    }
                    _ => {}
                }
                if !pending.is_empty() && message.role != MessageRole::Tool {
                    return Err(invalid("transcript.incomplete_round"));
                }
            }
            let mut content = Vec::new();
            for block in &message.content {
                match block {
                    ContentBlock::ToolCall { call } => {
                        has_tool_round |= is_visible;
                        if message.role != MessageRole::Assistant
                            || message.origin != MessageOrigin::Model
                            || !seen_calls.insert(&call.call_id)
                        {
                            return Err(invalid("transcript.tool_call"));
                        }
                        let tool = snapshot
                            .data
                            .tools
                            .iter()
                            .find(|tool| tool.model_tool.name == call.tool_name);
                        if tool.is_some_and(|tool| tool.descriptor_digest != call.descriptor_digest)
                        {
                            return Err(mismatch("transcript.descriptor"));
                        }
                        pending.insert(
                            call.call_id.clone(),
                            PendingCall {
                                message_id: message.message_id.clone(),
                                provider_call_id: call.provider_call_id.clone(),
                                visible: is_visible,
                                known: tool.is_some(),
                            },
                        );
                        if is_visible {
                            content.push(ModelContent::ToolCall {
                                provider_call_id: call.provider_call_id.clone(),
                                name: call.tool_name.clone(),
                                arguments: call.model_inputs.clone(),
                            });
                        }
                    }
                    ContentBlock::ToolResult { result } => {
                        if message.role != MessageRole::Tool
                            || message.origin != MessageOrigin::Tool
                        {
                            return Err(invalid("transcript.tool_result"));
                        }
                        let call = pending
                            .remove(&result.call_id)
                            .ok_or_else(|| invalid("transcript.tool_result"))?;
                        if call.message_id != result.call_message_id
                            || call.visible != is_visible
                            || (!call.known && result.status == ToolResultStatus::Succeeded)
                        {
                            return Err(invalid("transcript.tool_pair"));
                        }
                        if result.status == ToolResultStatus::Unknown {
                            if !is_visible {
                                return Err(invalid("transcript.hidden_unknown_effect"));
                            }
                            has_unknown = true;
                        }
                        if is_visible {
                            let values = result
                                .content
                                .iter()
                                .map(|item| safe_value(item, input.scope))
                                .collect::<Result<Vec<_>, _>>()?;
                            let mut value = json!({"status":result.status,"content":values});
                            if let Some(failure) = &result.error {
                                value["error"] = json!({"code":failure.code});
                            }
                            content.push(ModelContent::ToolResult {
                                provider_call_id: call.provider_call_id,
                                content: value,
                            });
                        }
                    }
                    ContentBlock::Content { content: item } if is_visible => {
                        if message.role == MessageRole::Tool {
                            return Err(invalid("transcript.tool_result"));
                        }
                        content.push(safe_content(item, input.scope)?);
                    }
                    ContentBlock::ProviderOpaque {
                        provider,
                        route_digest,
                        data_ref,
                    } if is_visible => {
                        if message.role != MessageRole::Assistant
                            || message.origin != MessageOrigin::Model
                            || provider != &input.route.provider
                            || route_digest != &input.route.digest()
                        {
                            return Err(mismatch("transcript.opaque_route"));
                        }
                        let records: Vec<_> = input
                            .opaque_records
                            .iter()
                            .filter(|record| &record.reference == data_ref)
                            .collect();
                        if records.len() != 1 {
                            return Err(mismatch("transcript.opaque_record"));
                        }
                        let record = records[0];
                        if &record.scope != input.scope
                            || &record.provider != provider
                            || record.continuation.route_digest() != route_digest
                            || data_digest(&record.continuation) != data_ref.digest
                        {
                            return Err(mismatch("transcript.opaque_record"));
                        }
                        content.push(ModelContent::Opaque {
                            continuation: record.continuation.clone(),
                        });
                    }
                    _ => {}
                }
            }
            if is_visible && !content.is_empty() {
                let role = match message.role {
                    MessageRole::User => ModelRole::User,
                    MessageRole::Assistant => ModelRole::Assistant,
                    MessageRole::Tool => ModelRole::Tool,
                    MessageRole::System => unreachable!("visible System rejected"),
                };
                if message.role == MessageRole::User && message.origin != MessageOrigin::User {
                    let values = content
                        .iter()
                        .map(|content| {
                            serde_json::to_value(content).expect("model content serialization")
                        })
                        .collect::<Vec<_>>();
                    content = vec![ModelContent::Json {
                        value: json!({"kind":"transcript_data", "origin":message.origin,
                        "source_message_id":message.message_id, "content":values}),
                    }];
                }
                projected.push((message.message_id.clone(), ModelMessage { role, content }));
            }
        }
        if !pending.is_empty() {
            return Err(invalid("transcript.incomplete_round"));
        }
        groups.push(RunGroup {
            run_id,
            messages: projected,
            has_tool_round,
            has_unknown,
        });
        cursor = end;
    }
    Ok(groups)
}

fn visible(message: &Message) -> bool {
    matches!(
        message.visibility,
        Visibility::Model | Visibility::UserAndModel
    )
}

fn safe_content(content: &InputContent, scope: &Scope) -> Result<ModelContent, ContractError> {
    match content {
        InputContent::Text { text } => Ok(ModelContent::Text { text: text.clone() }),
        InputContent::Json { value } => Ok(ModelContent::Json {
            value: value.clone(),
        }),
        _ => Ok(ModelContent::Json {
            value: safe_value(content, scope)?,
        }),
    }
}

fn safe_value(content: &InputContent, scope: &Scope) -> Result<Value, ContractError> {
    Ok(match content {
        InputContent::Text { text } => json!({"type":"text","text":text}),
        InputContent::Json { value } => json!({"type":"json","value":value}),
        InputContent::Artifact { reference } => {
            if &reference.scope != scope {
                return Err(mismatch("context.artifact_scope"));
            }
            json!({"type":"artifact","artifact_id":reference.artifact_id,"media_type":reference.media_type,
                "size_bytes":reference.size_bytes,"content_hash":reference.content_hash})
        }
        InputContent::Evidence { reference } => {
            let mut value = json!({"type":"evidence","source_id":reference.source_id,"version":reference.version,
                "location":reference.location,"content_hash":reference.content_hash});
            if let Some(quote) = &reference.quote {
                value["quote"] = json!(quote);
            }
            value
        }
    })
}

struct ByteCounter {
    written: usize,
    limit: usize,
}
impl io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.written = self
            .written
            .checked_add(bytes.len())
            .filter(|size| *size <= self.limit)
            .ok_or_else(|| io::Error::other("projection limit exceeded"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn fits(request: &ModelRequest, limits: &ProjectionLimits) -> bool {
    let count = request
        .messages
        .iter()
        .try_fold(request.tools.len(), |count, message| {
            count.checked_add(message.content.len())
        });
    if count.is_none_or(|count| count > limits.max_items) {
        return false;
    }
    serde_json::to_writer(
        &mut ByteCounter {
            written: 0,
            limit: limits.max_bytes.min(request.limits.max_input_bytes),
        },
        request,
    )
    .is_ok()
}
fn invalid(path: &str) -> ContractError {
    ContractError::new(ErrorCode::InvalidContext, path)
}
fn mismatch(path: &str) -> ContractError {
    ContractError::new(ErrorCode::ContextMismatch, path)
}
fn budget() -> ContractError {
    ContractError::new(
        ErrorCode::ContextBudgetExceeded,
        "projection.required_input",
    )
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
    /// The selected release is retired or otherwise unavailable.
    ModelUnavailable,
    /// Catalog scope-independent revision, identity or serialized integrity differs.
    ModelCatalogMismatch,
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
mod model_execution;
mod model_protocol;
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
pub use policy::{
    ApprovalChallenge, Guarded, PolicyAction, PolicyContext, PolicyDecision, PolicyGate,
    PolicyPort, PolicyRequest, ToolPolicyInput,
};
pub use state::{
    AdmissionInput, AdmissionResult, CommitInput, EventPage, MAX_EVENT_PAGE_SIZE, MemoryStateStore,
    ProtectedRecord, RunLease, StateStore, StateStoreCapabilities, StoredRun,
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
pub use model_execution::{
    ModelExchange, ModelExchangeOutcome, ModelRetryPolicy, StoredModelResponse,
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

## `crates/wickle/src/model_catalog.rs`

```rust
use crate::{
    ApiContract, ContractError, ErrorCode, Id, JsonDigest, JsonObject, PortFuture, Scope,
    VersionPolicy, VersionSemantics, VersionedRef, serialization::data_digest,
    tool_schema::compile_validator,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroU64,
};

/// Exact provider-qualified catalog reference; version strings are opaque.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelDefinitionRef {
    /// Registered service path, without a closed provider enum.
    pub provider: Id,
    /// Host catalog key, distinct from the provider model identifier.
    pub model_key: Id,
    /// Exact version selected within this key and provider.
    pub model_version: Id,
}

/// Lifecycle known at this catalog revision, not a live availability probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelLifecycle {
    /// Available according to supplied metadata.
    Active,
    /// Still usable but marked for replacement.
    Deprecated,
    /// Retired; invocation validation rejects it without finding a replacement.
    Retired,
    /// Known unavailable in this environment.
    Unavailable,
}

/// Informational evidence supplied by the trusted Host; it is not fetched here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelEvidence {
    /// Host record or documentation reference supporting this metadata.
    pub source_ref: Id,
    /// UTC milliseconds when the metadata was checked.
    pub observed_at_ms: i64,
}

/// Feature and option contract for a model ceiling or one exact binding.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCapabilities {
    /// Revision of these features, schemas and token limits.
    pub revision: Id,
    /// Explicit supported features; provider/family names do not imply features.
    pub features: BTreeSet<Id>,
    /// Closed object schema for provider options. Defaults are not inserted.
    pub options_schema: Value,
    /// Finite input-plus-reserved-output token capacity.
    pub context_window: NonZeroU64,
    /// Finite maximum requested output tokens.
    pub max_output_tokens: NonZeroU64,
}
impl fmt::Debug for ModelCapabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelCapabilities")
            .field("revision", &self.revision)
            .field("features", &self.features)
            .field("context_window", &self.context_window)
            .field("max_output_tokens", &self.max_output_tokens)
            .finish_non_exhaustive()
    }
}
impl ModelCapabilities {
    /// Validate finite limits and the closed option contract without I/O.
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.max_output_tokens > self.context_window {
            return Err(failure(
                ErrorCode::ModelCapabilityUnsupported,
                "capabilities.token_limits",
            ));
        }
        closed_schema(
            &self.options_schema,
            ErrorCode::ModelOptionUnsupported,
            "capabilities.options_schema",
        )?;
        Ok(())
    }
    /// Validate supplied options without dropping unsupported keys or inserting defaults.
    pub fn validate_options(&self, options: &JsonObject) -> Result<(), ContractError> {
        let validator = closed_schema(
            &self.options_schema,
            ErrorCode::ModelOptionUnsupported,
            "capabilities.options_schema",
        )?;
        if !validator.is_valid(&Value::Object(options.clone().into_iter().collect())) {
            return Err(failure(ErrorCode::ModelOptionUnsupported, "model.options"));
        }
        Ok(())
    }
}

/// One exact release in one service path. Other versions/providers coexist explicitly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelDefinition {
    /// Local model key, optionally shared across explicitly registered versions.
    pub model_key: Id,
    /// Informational family; it grants no shared capabilities.
    pub family: Id,
    /// Registered service path.
    pub provider: Id,
    /// Exact provider model identifier, distinct from deployment name.
    pub model_id: Id,
    /// Opaque release/version identifier.
    pub model_version: Id,
    /// Explicit Host metadata; never inferred from the spelling of a model ID.
    pub version_semantics: VersionSemantics,
    /// Lifecycle at this revision.
    pub lifecycle: ModelLifecycle,
    /// Model ceilings, never a substitute for binding-specific checks.
    pub capabilities: ModelCapabilities,
    /// Metadata provenance, distinct from binding support verification.
    pub evidence: Vec<ModelEvidence>,
}
impl ModelDefinition {
    /// Exact lookup key for this release and provider.
    pub fn reference(&self) -> ModelDefinitionRef {
        ModelDefinitionRef {
            provider: self.provider.clone(),
            model_key: self.model_key.clone(),
            model_version: self.model_version.clone(),
        }
    }
    /// Check model metadata without contacting a provider.
    pub fn validate(&self) -> Result<(), ContractError> {
        self.capabilities.validate()
    }
}

/// Recorded support stage. Registration alone is not live verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelSupportStatus {
    /// Planned, with no execution-support claim.
    Planned,
    /// A successful matching contract or stronger live check is recorded.
    ContractTested,
    /// A successful matching live check is recorded.
    LiveVerified,
}

/// Origin of a validation receipt; fixture tests cannot imply live provider support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelValidationKind {
    /// Offline protocol/contract verification.
    ContractTest,
    /// Actual provider/deployment verification reported by the trusted Host.
    LiveCheck,
}

/// Evidence tied to the tested combination. Structure and target are checked here;
/// the Host is responsible for the authenticity and truth of the external report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelValidationEvidence {
    /// Contract fixture or live provider check.
    pub kind: ModelValidationKind,
    /// Exact model/API/adapter/connection/target/capability contract tested.
    pub binding_digest: JsonDigest,
    /// UTC milliseconds when the check completed.
    pub checked_at_ms: i64,
    /// Trusted Host validation record, never fetched by this catalog.
    pub evidence_ref: Id,
    /// A failed check never establishes support.
    pub passed: bool,
}

/// Exact model/deployment/API/adapter contract. Credentials stay in connection
/// bindings. Serialization is for Host configuration or protected storage.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelBinding {
    /// Exact binding identity and revision.
    pub binding: VersionedRef,
    /// Exact provider-qualified model reference.
    pub model: ModelDefinitionRef,
    /// Original model ID/key or explicitly registered alias.
    pub requested_model: Id,
    /// Exact adapter implementation.
    pub adapter: VersionedRef,
    /// Host connection/credential reference, never a raw key or SDK client.
    pub connection_ref: VersionedRef,
    /// Nonsecret deployment/project/region target data.
    pub target: JsonObject,
    /// Closed target schema for this exact API/adapter.
    pub target_schema: Value,
    /// Exact operation and API version, independent of model release.
    pub api_contract: ApiContract,
    /// Independent deployment or inference-profile revision, when applicable.
    pub deployment_revision: Option<Id>,
    /// Target immutability; a pinned model does not upgrade this declaration.
    pub version_semantics: VersionSemantics,
    /// Exact-combination features/options, restricted by the model ceilings.
    pub capabilities: ModelCapabilities,
    /// Recorded support stage justified by matching evidence.
    pub support: ModelSupportStatus,
    /// Validation receipts for this exact contract.
    pub evidence: Vec<ModelValidationEvidence>,
}
impl fmt::Debug for ModelBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelBinding")
            .field("binding", &self.binding)
            .field("model", &self.model)
            .field("adapter", &self.adapter)
            .field("support", &self.support)
            .finish_non_exhaustive()
    }
}
impl ModelBinding {
    /// Identity covered by evidence. Support labels/receipts and lifecycle annotations
    /// are excluded to avoid circular evidence and preserve historical check records.
    pub fn contract_digest(&self, model: &ModelDefinition) -> Result<JsonDigest, ContractError> {
        if self.model != model.reference() {
            return Err(failure(ErrorCode::ModelBindingInvalid, "binding.model"));
        }
        Ok(data_digest(&(
            (
                &model.model_key,
                &model.family,
                &model.provider,
                &model.model_id,
                &model.model_version,
                model.version_semantics,
                &model.capabilities,
            ),
            (
                &self.binding,
                &self.model,
                &self.requested_model,
                &self.adapter,
                &self.connection_ref,
                &self.target,
                &self.target_schema,
                &self.api_contract,
                &self.deployment_revision,
                self.version_semantics,
                &self.capabilities,
            ),
        )))
    }
    /// Validate the exact target, capability ceilings and recorded support evidence.
    pub fn validate(&self, model: &ModelDefinition) -> Result<(), ContractError> {
        model.validate()?;
        self.capabilities.validate()?;
        if self.model != model.reference()
            || !self
                .capabilities
                .features
                .is_subset(&model.capabilities.features)
            || self.capabilities.context_window > model.capabilities.context_window
            || self.capabilities.max_output_tokens > model.capabilities.max_output_tokens
        {
            return Err(failure(
                ErrorCode::ModelBindingInvalid,
                "binding.capabilities",
            ));
        }
        self.validate_target()?;
        let digest = self.contract_digest(model)?;
        if self
            .evidence
            .iter()
            .any(|evidence| evidence.binding_digest != digest)
        {
            return Err(failure(
                ErrorCode::ModelBindingInvalid,
                "binding.evidence_target",
            ));
        }
        let has_contract = self.evidence.iter().any(|evidence| evidence.passed);
        let has_live = self
            .evidence
            .iter()
            .any(|evidence| evidence.passed && evidence.kind == ModelValidationKind::LiveCheck);
        if (self.support >= ModelSupportStatus::ContractTested && !has_contract)
            || (self.support == ModelSupportStatus::LiveVerified && !has_live)
        {
            return Err(failure(
                ErrorCode::ModelSupportInsufficient,
                "binding.evidence",
            ));
        }
        Ok(())
    }
    /// Validate target fields against this binding's own API/adapter schema.
    pub fn validate_target(&self) -> Result<(), ContractError> {
        let validator = closed_schema(
            &self.target_schema,
            ErrorCode::ModelBindingInvalid,
            "binding.target_schema",
        )?;
        if !validator.is_valid(&Value::Object(self.target.clone().into_iter().collect())) {
            return Err(failure(ErrorCode::ModelBindingInvalid, "binding.target"));
        }
        Ok(())
    }
}

/// Direct alias mapping; its target is an exact definition, never another alias.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelAlias {
    /// Provider namespace for this alias.
    pub provider: Id,
    /// Explicit alias spelling.
    pub alias: Id,
    /// Exact same-provider release.
    pub target: ModelDefinitionRef,
}

/// Complete catalog input. The concrete catalog privately owns a validated copy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCatalogSnapshot {
    /// Exact revision required on each lookup.
    pub revision: Id,
    /// Exact tenant/workspace/user namespace.
    pub scope: Scope,
    /// Explicit versions; there is no implicit latest entry.
    pub models: Vec<ModelDefinition>,
    /// Independently versioned API/deployment bindings.
    pub bindings: Vec<ModelBinding>,
    /// Explicit direct mappings at this revision.
    pub aliases: Vec<ModelAlias>,
}
impl ModelCatalogSnapshot {
    /// Hash the complete snapshot, including scope, lifecycle and evidence.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }
    /// Validate duplicate identities, references, alias consistency and binding contracts.
    pub fn validate(&self) -> Result<(), ContractError> {
        let mut models = BTreeMap::new();
        for model in &self.models {
            model.validate()?;
            if models.insert(model.reference(), model).is_some() {
                return Err(failure(ErrorCode::ModelCatalogMismatch, "catalog.models"));
            }
        }
        let mut aliases = BTreeMap::new();
        for alias in &self.aliases {
            if alias.provider != alias.target.provider
                || !models.contains_key(&alias.target)
                || self.models.iter().any(|model| {
                    model.provider == alias.provider
                        && (model.model_key == alias.alias || model.model_id == alias.alias)
                })
                || aliases
                    .insert((&alias.provider, &alias.alias), &alias.target)
                    .is_some()
            {
                return Err(failure(ErrorCode::ModelCatalogMismatch, "catalog.aliases"));
            }
        }
        let mut bindings = BTreeSet::new();
        for binding in &self.bindings {
            if !bindings.insert((&binding.binding.id, &binding.binding.version)) {
                return Err(failure(ErrorCode::ModelCatalogMismatch, "catalog.bindings"));
            }
            let model = models
                .get(&binding.model)
                .ok_or_else(|| failure(ErrorCode::ModelNotRegistered, "binding.model"))?;
            if binding.requested_model != model.model_id
                && binding.requested_model != model.model_key
                && aliases
                    .get(&(&binding.model.provider, &binding.requested_model))
                    .copied()
                    != Some(&binding.model)
            {
                return Err(failure(
                    ErrorCode::ModelBindingInvalid,
                    "binding.requested_model",
                ));
            }
            binding.validate(model)?;
        }
        Ok(())
    }
}

/// Explicit request requirements. Input tokens are Host estimates, not bytes or reported usage.
#[derive(Clone, PartialEq)]
pub struct CatalogRequirements {
    /// Features required by the prepared request.
    pub features: BTreeSet<Id>,
    /// Options that must pass both model and binding schemas unchanged.
    pub options: JsonObject,
    /// Host-estimated input tokens for this route.
    pub input_tokens: u64,
    /// Maximum output tokens reserved for this call.
    pub max_output_tokens: NonZeroU64,
    /// Explicit acceptance policy for mutable or unverified targets.
    pub version_policy: VersionPolicy,
    /// Minimum recorded support stage accepted for this call.
    pub min_support: ModelSupportStatus,
}
impl fmt::Debug for CatalogRequirements {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogRequirements")
            .field("features", &self.features)
            .field("input_tokens", &self.input_tokens)
            .field("max_output_tokens", &self.max_output_tokens)
            .finish_non_exhaustive()
    }
}

/// Exact metadata lookup, not a selected runtime route or provider-reported result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedCatalogBinding {
    /// Revision from which this pair was read.
    pub catalog_revision: Id,
    /// Exact resolved release metadata.
    pub model: ModelDefinition,
    /// Exact binding, retaining requested_model separately from the resolved release.
    pub binding: ModelBinding,
}
impl ResolvedCatalogBinding {
    /// Preserve mutable/unverified semantics in either the model or target.
    pub fn effective_version_semantics(&self) -> VersionSemantics {
        match (self.model.version_semantics, self.binding.version_semantics) {
            (VersionSemantics::Pinned, VersionSemantics::Pinned) => VersionSemantics::Pinned,
            (VersionSemantics::Unverified, _) | (_, VersionSemantics::Unverified) => {
                VersionSemantics::Unverified
            }
            (_, target) if target != VersionSemantics::Pinned => target,
            (model, _) => model,
        }
    }
    /// Reject unsupported/retired/unpinned choices without changing their model,
    /// target, features or options. No routing or model invocation is performed.
    pub fn validate(&self, requirements: &CatalogRequirements) -> Result<(), ContractError> {
        self.binding.validate(&self.model)?;
        if matches!(
            self.model.lifecycle,
            ModelLifecycle::Retired | ModelLifecycle::Unavailable
        ) {
            return Err(failure(ErrorCode::ModelUnavailable, "model.lifecycle"));
        }
        if self.binding.support < requirements.min_support {
            return Err(failure(
                ErrorCode::ModelSupportInsufficient,
                "binding.support",
            ));
        }
        if requirements.version_policy == VersionPolicy::RequirePinned
            && self.effective_version_semantics() != VersionSemantics::Pinned
        {
            return Err(failure(
                ErrorCode::ModelVersionUnpinned,
                "binding.version_semantics",
            ));
        }
        if !requirements
            .features
            .is_subset(&self.model.capabilities.features)
            || !requirements
                .features
                .is_subset(&self.binding.capabilities.features)
        {
            return Err(failure(
                ErrorCode::ModelCapabilityUnsupported,
                "model.features",
            ));
        }
        self.model
            .capabilities
            .validate_options(&requirements.options)?;
        self.binding
            .capabilities
            .validate_options(&requirements.options)?;
        if requirements.max_output_tokens > self.model.capabilities.max_output_tokens
            || requirements.max_output_tokens > self.binding.capabilities.max_output_tokens
            || requirements
                .input_tokens
                .checked_add(requirements.max_output_tokens.get())
                .is_none_or(|tokens| {
                    tokens > self.model.capabilities.context_window.get()
                        || tokens > self.binding.capabilities.context_window.get()
                })
        {
            return Err(failure(
                ErrorCode::ModelContextIncompatible,
                "model.token_limits",
            ));
        }
        Ok(())
    }
}

/// Scope- and revision-bound metadata port. The Host supplies trusted data;
/// implementations do not infer releases, fetch credentials, or invoke models.
pub trait ModelCatalog: Send + Sync {
    /// Exact immutable revision represented by this instance.
    fn revision(&self) -> &Id;
    /// Exact metadata namespace, with no user-scope wildcards.
    fn scope(&self) -> &Scope;
    /// Read exact model metadata, including retired records for inspection.
    fn get_model<'a>(
        &'a self,
        scope: &'a Scope,
        revision: &'a Id,
        reference: &'a ModelDefinitionRef,
    ) -> PortFuture<'a, ModelDefinition>;
    /// Read an exact binding and definition. Validate eligibility before execution.
    fn get_binding<'a>(
        &'a self,
        scope: &'a Scope,
        revision: &'a Id,
        reference: &'a VersionedRef,
    ) -> PortFuture<'a, ResolvedCatalogBinding>;
    /// Resolve only a registered direct mapping, without fallback or latest lookup.
    fn resolve_alias<'a>(
        &'a self,
        scope: &'a Scope,
        revision: &'a Id,
        provider: &'a Id,
        alias: &'a Id,
    ) -> PortFuture<'a, ModelDefinitionRef>;
}

fn closed_schema(
    schema: &Value,
    code: ErrorCode,
    path: &str,
) -> Result<jsonschema::Validator, ContractError> {
    if schema.get("type").and_then(Value::as_str) != Some("object")
        || schema.get("additionalProperties") != Some(&Value::Bool(false))
    {
        return Err(failure(code, path));
    }
    compile_validator(schema).map_err(|_| failure(code, path))
}
fn failure(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
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

## `crates/wickle/tests/context_projection.rs`

```rust
//! Pinned prompts, provenance, protected-input boundaries, and atomic context selection.

use serde_json::{Value, json};
use std::collections::BTreeSet;
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn versioned(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
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
fn record(value: &str) -> RecordRef {
    RecordRef {
        record_id: id(value),
        revision: 1,
        digest: canonical_digest(&json!(value)),
    }
}
fn compiled_tool(name: &str) -> CompiledTool {
    let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    SchemaCompiler::new().compile(ToolDescriptor {
        tool:versioned(name), name:id(name), description:format!("{name} records"),
        input_schema:json!({"type":"object","properties":{"query":{"type":"string"},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters:vec!["query".into()], system_bindings:None,
        output_schema:json!({"type":"string"}), side_effect:ToolSideEffect::ReadOnly,
        concurrency:ToolConcurrency::Serial, retry:ToolRetryPolicy::Never, reconcile:false,
        max_output_bytes:4096.try_into().unwrap(),
    }, &registry).unwrap()
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
                    version: Some(reference.version.clone().unwrap_or_else(|| id("1"))),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(
                    &json!({"id":reference.id,"version":reference.version}),
                ),
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

fn route() -> ResolvedModelRoute {
    ResolvedModelRoute {
        binding: versioned("model"),
        catalog_revision: id("catalog"),
        routing_policy_revision: id("policy"),
        requested_model: id("model"),
        model_id: id("model"),
        model_version: id("release"),
        version_semantics: VersionSemantics::Pinned,
        provider: id("provider"),
        target: JsonObject::new(),
        deployment_revision: None,
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        adapter: versioned("adapter"),
        capability_revision: id("capabilities"),
        connection_ref: versioned("connection"),
    }
}

struct Fixture {
    scope: Scope,
    profile: ResolvedProfile,
    prompt: PromptSnapshot,
    prompt_digest: JsonDigest,
    tools: Vec<CompiledTool>,
    skill: SkillManifest,
    request: RunRequest,
    run_id: Id,
    step_id: Id,
    request_message_id: Id,
}

impl Fixture {
    async fn new() -> Self {
        let scope = Scope {
            tenant_id: id("tenant"),
            workspace_id: id("workspace"),
            user_id: None,
        };
        let profile=AgentProfile::from_json(r#"{
            "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
            "name":"Assistant","description":"Context fixture","instructions":{"text":"Profile instructions"},
            "model_binding":"model","tools":[{"tool_id":"search","version":"1"},{"tool_id":"read","version":"1"}],
            "skills":[{"skill_id":"analysis","version":"1"}],"connectors":[],
            "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
            "limits":{"max_model_calls":4,"max_tool_attempts":4,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
        }"#).unwrap();
        let profile = ProfileValidator::new(&Catalog)
            .validate(&profile, &scope)
            .await
            .unwrap();
        let tools = vec![compiled_tool("search"), compiled_tool("read")];
        let skill = SkillManifest {
            skill: versioned("analysis"),
            name: "Analysis".into(),
            description: "Analyze evidence".into(),
            manifest_digest: canonical_digest(&json!("analysis manifest")),
        };
        let bindings = profile
            .profile()
            .tools
            .iter()
            .cloned()
            .zip(tools.iter().cloned())
            .map(|(selection, compiled)| PromptToolBinding {
                selection,
                compiled,
            })
            .collect();
        let prompt = PromptSnapshot::create(
            &profile,
            vec!["Host rule A".into(), "Host rule B".into()],
            None,
            bindings,
            vec![skill.clone()],
        )
        .unwrap();
        let prompt_digest = prompt.digest();
        Self {
            scope,
            profile,
            prompt,
            prompt_digest,
            tools,
            skill,
            request: RunRequest {
                request_id: id("user-request"),
                session_id: id("session"),
                input: vec![InputContent::Text {
                    text: "Current requested work".into(),
                }],
                trigger: RunTrigger::User {},
                model_options: JsonObject::new(),
                output_contract: None,
            },
            run_id: id("current-run"),
            step_id: id("step"),
            request_message_id: id("current-message"),
        }
    }
    fn current_message(&self, sequence: u64) -> Message {
        Message {
            message_id: self.request_message_id.clone(),
            run_id: self.run_id.clone(),
            sequence: sequence.try_into().unwrap(),
            role: MessageRole::User,
            content: self
                .request
                .input
                .iter()
                .cloned()
                .map(|content| ContentBlock::Content { content })
                .collect(),
            origin: MessageOrigin::User,
            visibility: Visibility::UserAndModel,
        }
    }
    fn input<'a>(
        &'a self,
        transcript: &'a [Message],
        items: &'a [ContextItem],
        opaque: &'a [ScopedOpaque],
    ) -> ProjectionInput<'a> {
        ProjectionInput {
            profile: &self.profile,
            scope: &self.scope,
            run_id: &self.run_id,
            model_step_id: &self.step_id,
            current_request: &self.request,
            current_request_message_id: &self.request_message_id,
            transcript,
            context_items: items,
            opaque_records: opaque,
            expected_prompt_digest: &self.prompt_digest,
            request_id: self.step_id.clone(),
            purpose: ModelPurpose::Agent,
            route: route(),
            output: ModelOutput::Text {},
            max_output_tokens: 128.try_into().unwrap(),
            options: JsonObject::new(),
            response_limits: ModelResponseLimits {
                max_input_bytes: 100_000,
                max_response_bytes: 4096,
                max_delta_bytes: 1024,
                max_events: 32,
                max_tool_calls: 4,
            },
            limits: ProjectionLimits {
                max_bytes: 100_000,
                max_items: 100,
            },
        }
    }
    fn item(&self, name: &str, origin: ContextOrigin, priority: ContextPriority) -> ContextItem {
        ContextItem::new(
            id(name),
            origin,
            versioned(name),
            self.scope.clone(),
            vec![InputContent::Text {
                text: format!("data for {name}"),
            }],
            ContextLifetime::Run {
                run_id: self.run_id.clone(),
            },
            priority,
        )
    }
}

fn message(
    run: &str,
    sequence: u64,
    role: MessageRole,
    origin: MessageOrigin,
    content: Vec<ContentBlock>,
) -> Message {
    Message {
        message_id: id(&format!("message-{sequence}")),
        run_id: id(run),
        sequence: sequence.try_into().unwrap(),
        role,
        origin,
        content,
        visibility: Visibility::UserAndModel,
    }
}

#[tokio::test]
async fn host_model_options_reach_the_port_without_becoming_prompt_content() {
    use std::{sync::Mutex, time::Duration};
    struct ObserveOptions(Mutex<Option<JsonObject>>);
    impl ModelPort for ObserveOptions {
        fn binding(&self) -> ModelPortBinding {
            let route = route();
            ModelPortBinding {
                provider: route.provider,
                adapter: route.adapter,
                connection_ref: route.connection_ref,
            }
        }
        fn generate<'a>(
            &'a self,
            request: &'a ModelRequest,
            _: &'a ModelCallContext,
        ) -> PortStream<'a, ModelEvent> {
            *self.0.lock().unwrap() = Some(request.options.clone());
            Box::pin(futures_util::stream::iter([Ok(
                ModelEvent::ResponseCompleted {
                    finish: ModelFinish::Stop,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![],
                },
            )]))
        }
    }
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let assembler = ContextAssembler::new();
    let baseline = assembler
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let options = object(json!({"reasoning_effort":"high", "provider_mode":{"budget":32}}));
    let mut input = fixture.input(&transcript, &[], &[]);
    input.options = options.clone();
    let projected = assembler.project(&fixture.prompt, input).unwrap();
    assert_eq!(projected.request.messages, baseline.request.messages);
    assert_eq!(projected.request.tools, baseline.request.tools);
    assert_ne!(projected.request.digest(), baseline.request.digest());
    let port = ObserveOptions(Mutex::new(None));
    let call = ModelCallContext {
        attempt_id: id("attempt"),
        run_id: fixture.run_id.clone(),
        scope: fixture.scope.clone(),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(1),
    };
    collect_model_response(&projected.request, port.generate(&projected.request, &call))
        .await
        .unwrap();
    assert_eq!(*port.0.lock().unwrap(), Some(options.clone()));
    let mut limited = fixture.input(&transcript, &[], &[]);
    limited.options = options;
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len();
    assert_eq!(
        assembler
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}
fn text(value: &str) -> ContentBlock {
    ContentBlock::Content {
        content: InputContent::Text { text: value.into() },
    }
}
fn round(run: &str, sequence: u64, label: &str, body: &str, tool: &CompiledTool) -> Vec<Message> {
    let call_message = id(&format!("message-{sequence}"));
    let call = ToolCall {
        call_id: id(label),
        model_request_id: id(&format!("request-{label}")),
        provider_call_id: id(&format!("provider-{label}")),
        tool_name: id("search"),
        model_inputs: object(json!({"query":label})),
        descriptor_digest: tool.descriptor_digest().clone(),
        bound_input_ref: Some(record("bound-private-input")),
    };
    let result = ToolResult {
        call_id: call.call_id.clone(),
        call_message_id: call_message,
        status: ToolResultStatus::Failed,
        content: vec![InputContent::Text { text: body.into() }],
        effect_receipt_ref: Some(record("private-effect-receipt")),
        error: Some(Failure {
            code: id("unavailable"),
            diagnostic_ref: Some(record("private-diagnostic")),
        }),
    };
    vec![
        message(
            run,
            sequence,
            MessageRole::Assistant,
            MessageOrigin::Model,
            vec![ContentBlock::ToolCall { call }],
        ),
        message(
            run,
            sequence + 1,
            MessageRole::Tool,
            MessageOrigin::Tool,
            vec![ContentBlock::ToolResult { result }],
        ),
    ]
}

#[tokio::test]
async fn host_profile_and_skill_prefixes_are_pinned_and_tools_use_only_compiled_model_schemas() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert_eq!(projected.prompt_digest, fixture.prompt_digest);
    assert_eq!(
        projected.request.messages[0],
        ModelMessage {
            role: ModelRole::System,
            content: vec![
                ModelContent::Text {
                    text: "Host rule A".into()
                },
                ModelContent::Text {
                    text: "Host rule B".into()
                }
            ]
        }
    );
    assert_eq!(
        projected.request.messages[1],
        ModelMessage {
            role: ModelRole::System,
            content: vec![ModelContent::Text {
                text: "Profile instructions".into()
            }]
        }
    );
    assert_eq!(
        projected.request.tools,
        fixture
            .tools
            .iter()
            .map(CompiledTool::to_model_tool)
            .collect::<Vec<_>>()
    );
    for tool in &projected.request.tools {
        assert!(
            tool.model_input_schema["properties"]
                .get("workspace_id")
                .is_none()
        );
    }
    assert_eq!(
        projected.selected_message_ids,
        vec![fixture.request_message_id.clone()]
    );
    projected.request.validate().unwrap();
    let restored = PromptSnapshot::restore(
        &serde_json::to_string(&fixture.prompt).unwrap(),
        &fixture.prompt_digest,
        &fixture.profile,
        &fixture.scope,
    )
    .unwrap();
    let repeated = ContextAssembler::new()
        .project(&restored, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert_eq!(projected.request, repeated.request);
}

#[tokio::test]
async fn current_request_is_exactly_the_persisted_message_and_is_never_silently_replaced() {
    let fixture = Fixture::new().await;
    let current = fixture.current_message(1);
    for transcript in [
        vec![],
        vec![Message {
            content: vec![text("A different request")],
            ..current.clone()
        }],
        vec![Message {
            visibility: Visibility::Internal,
            ..current.clone()
        }],
        vec![Message {
            run_id: id("other-run"),
            ..current.clone()
        }],
    ] {
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
    let transcript = vec![current];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert_eq!(
        projected.request.messages.last().unwrap(),
        &ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Current requested work".into()
            }]
        }
    );
    assert_eq!(
        projected
            .selected_message_ids
            .iter()
            .filter(|message_id| *message_id == &fixture.request_message_id)
            .count(),
        1
    );
}

#[tokio::test]
async fn tool_projection_keeps_model_arguments_and_public_observations_without_execution_records() {
    let fixture = Fixture::new().await;
    let mut transcript = vec![fixture.current_message(1)];
    transcript.extend(round(
        "current-run",
        2,
        "call",
        "public observation",
        &fixture.tools[0],
    ));
    let mut internal = message(
        "current-run",
        4,
        MessageRole::User,
        MessageOrigin::Recovery,
        vec![ContentBlock::Content {
            content: InputContent::Json {
                value: json!({"system_inputs":{"workspace_id":"11111111-1111-4111-8111-111111111111"},"execution_args":{"query":"call","workspace_id":"11111111-1111-4111-8111-111111111111"},"raw_diagnostic":"private"}),
            },
        }],
    );
    internal.visibility = Visibility::Internal;
    transcript.push(internal);
    let original = transcript.clone();
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let contents: Vec<_> = projected
        .request
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .collect();
    let call = contents
        .iter()
        .find_map(|content| match content {
            ModelContent::ToolCall {
                provider_call_id,
                name,
                arguments,
            } => Some((provider_call_id, name, arguments)),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        call,
        (
            &id("provider-call"),
            &id("search"),
            &object(json!({"query":"call"}))
        )
    );
    let result = contents
        .iter()
        .find_map(|content| match content {
            ModelContent::ToolResult {
                provider_call_id,
                content,
            } => Some((provider_call_id, content)),
            _ => None,
        })
        .unwrap();
    assert_eq!(result.0, &id("provider-call"));
    assert_eq!(
        result.1,
        &json!({"status":"failed","content":[{"type":"text","text":"public observation"}],"error":{"code":"unavailable"}})
    );
    assert_eq!(projected.request.messages.len(), 6);
    assert_eq!(
        projected.selected_message_ids,
        vec![
            fixture.request_message_id.clone(),
            id("message-2"),
            id("message-3")
        ]
    );
    assert_eq!(transcript, original);
    projected.request.validate().unwrap();
}

#[tokio::test]
async fn data_context_never_adds_system_authority_or_replaces_the_pinned_prefix() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let items = vec![
        fixture.item(
            "retrieval",
            ContextOrigin::Retrieval,
            ContextPriority::Required,
        ),
        fixture.item("memory", ContextOrigin::Memory, ContextPriority::Required),
        fixture.item(
            "verification",
            ContextOrigin::Verification,
            ContextPriority::Required,
        ),
    ];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &items, &[]))
        .unwrap();
    assert_eq!(projected.selected_context_ids.len(), 3);
    let system = |request: ModelRequest| {
        request
            .messages
            .into_iter()
            .filter(|message| message.role == ModelRole::System)
            .collect::<Vec<_>>()
    };
    assert_eq!(system(projected.request), system(baseline.request));
    let mut forged = serde_json::to_value(&items[0]).unwrap();
    forged["origin"] = json!("host");
    assert!(serde_json::from_value::<ContextItem>(forged).is_err());
}

#[tokio::test]
async fn context_items_validate_scope_and_digest_and_exclude_other_run_or_step_lifetimes() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let original = fixture.item(
        "source",
        ContextOrigin::Retrieval,
        ContextPriority::Required,
    );
    let mut wrong_scope = original.clone();
    wrong_scope.scope.tenant_id = id("other-tenant");
    let mut wrong_run = original.clone();
    wrong_run.lifetime = ContextLifetime::Run {
        run_id: id("other-run"),
    };
    let mut wrong_step = original.clone();
    wrong_step.lifetime = ContextLifetime::Step {
        run_id: fixture.run_id.clone(),
        model_step_id: id("other-step"),
    };
    let mut changed_content = original;
    changed_content.content = vec![InputContent::Text {
        text: "changed data".into(),
    }];
    let with_valid_digest = |item: ContextItem| {
        ContextItem::new(
            item.item_id,
            item.origin,
            item.source_ref,
            item.scope,
            item.content,
            item.lifetime,
            item.priority_class,
        )
    };
    for item in [with_valid_digest(wrong_scope), changed_content] {
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[item], &[]))
                .is_err()
        );
    }
    for item in [with_valid_digest(wrong_run), with_valid_digest(wrong_step)] {
        let items = vec![item];
        let projected = ContextAssembler::new()
            .project(&fixture.prompt, fixture.input(&transcript, &items, &[]))
            .unwrap();
        assert!(projected.selected_context_ids.is_empty());
        assert_eq!(projected.dropped_context_ids, vec![id("source")]);
    }
}

#[tokio::test]
async fn a_current_tool_round_cannot_be_incomplete_or_have_an_unpaired_result() {
    let fixture = Fixture::new().await;
    let complete = round("current-run", 2, "call", "observation", &fixture.tools[0]);
    for transcript in [
        vec![fixture.current_message(1), complete[0].clone()],
        vec![fixture.current_message(1), complete[1].clone()],
        vec![
            fixture.current_message(1),
            complete[0].clone(),
            message(
                "current-run",
                3,
                MessageRole::User,
                MessageOrigin::User,
                vec![text("interleaved")],
            ),
            Message {
                sequence: 4.try_into().unwrap(),
                ..complete[1].clone()
            },
        ],
    ] {
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
}

#[tokio::test]
async fn raw_transcript_system_roles_cannot_extend_or_replace_the_pinned_prefix() {
    let fixture = Fixture::new().await;
    for origin in [
        MessageOrigin::Host,
        MessageOrigin::Profile,
        MessageOrigin::Retrieval,
        MessageOrigin::Memory,
    ] {
        let transcript = vec![
            fixture.current_message(1),
            message(
                "current-run",
                2,
                MessageRole::System,
                origin,
                vec![text("untrusted transcript instructions")],
            ),
        ];
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
}

#[tokio::test]
async fn visibility_filtering_and_message_references_cannot_separate_a_tool_call_from_its_result() {
    let fixture = Fixture::new().await;
    for hidden in [0, 1] {
        let mut messages = round("current-run", 2, "call", "observation", &fixture.tools[0]);
        messages[hidden].visibility = Visibility::Internal;
        let mut transcript = vec![fixture.current_message(1)];
        transcript.extend(messages);
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
    let mut messages = round("current-run", 2, "call", "observation", &fixture.tools[0]);
    let ContentBlock::ToolResult { result } = &mut messages[1].content[0] else {
        unreachable!()
    };
    result.call_message_id = id("some-other-call-message");
    let mut transcript = vec![fixture.current_message(1)];
    transcript.extend(messages);
    assert!(
        ContextAssembler::new()
            .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
            .is_err()
    );
}

#[tokio::test]
async fn bounded_selection_discards_an_older_run_as_a_whole_and_retains_the_latest_complete_round()
{
    let fixture = Fixture::new().await;
    let large = "old ".repeat(512);
    let mut transcript = vec![message(
        "old-run",
        1,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Old work")],
    )];
    transcript.extend(round("old-run", 2, "old-call", &large, &fixture.tools[0]));
    transcript.push(message(
        "old-run",
        4,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Old answer")],
    ));
    transcript.push(message(
        "recent-run",
        5,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Recent work")],
    ));
    transcript.extend(round(
        "recent-run",
        6,
        "recent-call",
        "Recent observation",
        &fixture.tools[0],
    ));
    transcript.push(message(
        "recent-run",
        8,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Recent answer")],
    ));
    transcript.push(fixture.current_message(9));
    let original = transcript.clone();
    let full = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let full_bytes = serde_json::to_vec(&full.request).unwrap().len();
    let mut bounded = fixture.input(&transcript, &[], &[]);
    bounded.limits.max_bytes = full_bytes - large.len() / 2;
    let byte_limit = bounded.limits.max_bytes;
    let selected = ContextAssembler::new()
        .project(&fixture.prompt, bounded)
        .unwrap();
    assert_eq!(
        selected.selected_message_ids,
        vec![
            id("message-5"),
            id("message-6"),
            id("message-7"),
            id("message-8"),
            fixture.request_message_id.clone()
        ]
    );
    assert_eq!(
        selected.dropped_message_ids,
        vec![
            id("message-1"),
            id("message-2"),
            id("message-3"),
            id("message-4")
        ]
    );
    assert!(serde_json::to_vec(&selected.request).unwrap().len() <= byte_limit);
    selected.request.validate().unwrap();
    assert_eq!(transcript, original);
}

#[tokio::test]
async fn required_current_work_and_prefix_report_budget_exhaustion_instead_of_truncation() {
    let fixture = Fixture::new().await;
    let current = vec![fixture.current_message(1)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&current, &[], &[]))
        .unwrap();
    let count = baseline
        .request
        .messages
        .iter()
        .map(|message| message.content.len())
        .sum::<usize>()
        + baseline.request.tools.len();
    let mut item_limited = fixture.input(&current, &[], &[]);
    item_limited.limits.max_items = count - 1;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, item_limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
    let mut input_limited = fixture.input(&current, &[], &[]);
    input_limited.response_limits.max_input_bytes = 1;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, input_limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
    let mut transcript = current;
    transcript.extend(round(
        "current-run",
        2,
        "current-call",
        &"data ".repeat(400),
        &fixture.tools[0],
    ));
    let mut bounded = fixture.input(&transcript, &[], &[]);
    bounded.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 64;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, bounded)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn optional_context_can_be_removed_but_required_context_cannot_be_silently_dropped() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let mut optional = fixture.item(
        "optional",
        ContextOrigin::Retrieval,
        ContextPriority::Optional,
    );
    optional = ContextItem::new(
        optional.item_id,
        optional.origin,
        optional.source_ref,
        optional.scope,
        vec![InputContent::Text {
            text: "reference data ".repeat(1000),
        }],
        optional.lifetime,
        optional.priority_class,
    );
    let items = vec![optional.clone()];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let mut limited = fixture.input(&transcript, &items, &[]);
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 128;
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, limited)
        .unwrap();
    assert!(projected.selected_context_ids.is_empty());
    assert_eq!(projected.dropped_context_ids, vec![id("optional")]);
    let required = ContextItem::new(
        optional.item_id,
        optional.origin,
        optional.source_ref,
        optional.scope,
        optional.content,
        optional.lifetime,
        ContextPriority::Required,
    );
    let required_items = vec![required];
    let mut limited = fixture.input(&transcript, &required_items, &[]);
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 128;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn pinned_snapshot_rejects_other_scope_profile_and_recomputed_replacement_contents() {
    let fixture = Fixture::new().await;
    let mut foreign = fixture.scope.clone();
    foreign.tenant_id = id("other-tenant");
    assert!(
        fixture
            .prompt
            .validate_for(&fixture.profile, &foreign, &fixture.prompt_digest)
            .is_err()
    );
    let mut changed_profile = fixture.profile.profile().clone();
    changed_profile.version = id("2.0.0");
    let changed_profile = ProfileValidator::new(&Catalog)
        .validate(&changed_profile, &fixture.scope)
        .await
        .unwrap();
    assert!(
        fixture
            .prompt
            .validate_for(&changed_profile, &fixture.scope, &fixture.prompt_digest)
            .is_err()
    );
    let bindings = fixture
        .profile
        .profile()
        .tools
        .iter()
        .cloned()
        .zip(fixture.tools.iter().cloned())
        .map(|(selection, compiled)| PromptToolBinding {
            selection,
            compiled,
        })
        .collect();
    let replacement = PromptSnapshot::create(
        &fixture.profile,
        vec!["Replacement Host policy".into()],
        None,
        bindings,
        vec![fixture.skill.clone()],
    )
    .unwrap();
    assert!(
        replacement
            .validate_for(&fixture.profile, &fixture.scope, &fixture.prompt_digest)
            .is_err()
    );
    assert!(
        PromptSnapshot::restore(
            &serde_json::to_string(&replacement).unwrap(),
            &fixture.prompt_digest,
            &fixture.profile,
            &fixture.scope
        )
        .is_err()
    );
}

#[tokio::test]
async fn prompt_creation_rejects_unselected_tools_and_changed_skill_versions() {
    let fixture = Fixture::new().await;
    let mut extra = fixture
        .profile
        .profile()
        .tools
        .iter()
        .cloned()
        .zip(fixture.tools.iter().cloned())
        .map(|(selection, compiled)| PromptToolBinding {
            selection,
            compiled,
        })
        .collect::<Vec<_>>();
    extra.push(PromptToolBinding {
        selection: ToolBindingRef::Catalog(CatalogToolRef {
            tool_id: id("unselected"),
            version: id("1"),
            bindings: None,
            config: None,
        }),
        compiled: compiled_tool("unselected"),
    });
    assert!(
        PromptSnapshot::create(
            &fixture.profile,
            vec!["Host rule".into()],
            None,
            extra,
            vec![fixture.skill.clone()]
        )
        .is_err()
    );
    let bindings = fixture
        .profile
        .profile()
        .tools
        .iter()
        .cloned()
        .zip(fixture.tools.iter().cloned())
        .map(|(selection, compiled)| PromptToolBinding {
            selection,
            compiled,
        })
        .collect();
    let mut changed = fixture.skill.clone();
    changed.skill.version = id("2");
    assert!(
        PromptSnapshot::create(
            &fixture.profile,
            vec!["Host rule".into()],
            None,
            bindings,
            vec![changed]
        )
        .is_err()
    );
}

#[tokio::test]
async fn opaque_replay_requires_matching_protected_record_scope_provider_and_route() {
    let fixture = Fixture::new().await;
    let continuation =
        OpaqueContinuation::new(&route(), json!({"signature":"provider continuation"}));
    let reference = RecordRef {
        record_id: id("opaque"),
        revision: 1,
        digest: canonical_digest(&serde_json::to_value(&continuation).unwrap()),
    };
    let mut transcript = vec![fixture.current_message(1)];
    transcript.push(message(
        "current-run",
        2,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![ContentBlock::ProviderOpaque {
            provider: id("provider"),
            route_digest: route().digest(),
            data_ref: reference.clone(),
        }],
    ));
    let records = vec![ScopedOpaque {
        scope: fixture.scope.clone(),
        reference: reference.clone(),
        provider: id("provider"),
        continuation: continuation.clone(),
    }];
    let accepted = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &records))
        .unwrap();
    assert!(accepted.request.messages.iter().flat_map(|message|&message.content).any(|content|matches!(content,ModelContent::Opaque{continuation:found} if found==&continuation)));
    let mut changed = fixture.input(&transcript, &[], &records);
    changed.route.connection_ref.version = id("different-connection");
    assert!(
        ContextAssembler::new()
            .project(&fixture.prompt, changed)
            .is_err()
    );
    let mut wrong_scope = records.clone();
    wrong_scope[0].scope.tenant_id = id("other-tenant");
    assert!(
        ContextAssembler::new()
            .project(
                &fixture.prompt,
                fixture.input(&transcript, &[], &wrong_scope)
            )
            .is_err()
    );
    let mut wrong_provider = records.clone();
    wrong_provider[0].provider = id("other-provider");
    assert!(
        ContextAssembler::new()
            .project(
                &fixture.prompt,
                fixture.input(&transcript, &[], &wrong_provider)
            )
            .is_err()
    );
    let mut wrong_record = records;
    wrong_record[0].reference = record("different-record");
    assert!(
        ContextAssembler::new()
            .project(
                &fixture.prompt,
                fixture.input(&transcript, &[], &wrong_record)
            )
            .is_err()
    );
}

#[tokio::test]
async fn the_latest_tool_round_from_an_earlier_run_is_required_context() {
    let fixture = Fixture::new().await;
    let current = vec![fixture.current_message(5)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&current, &[], &[]))
        .unwrap();
    let mut transcript = vec![message(
        "previous-run",
        1,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Previous work")],
    )];
    transcript.extend(round(
        "previous-run",
        2,
        "previous-call",
        &"data ".repeat(400),
        &fixture.tools[0],
    ));
    transcript.push(message(
        "previous-run",
        4,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Previous answer")],
    ));
    transcript.extend(current);
    let unbounded = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert!(unbounded.selected_message_ids.contains(&id("message-2")));
    assert!(unbounded.selected_message_ids.contains(&id("message-3")));
    let mut limited = fixture.input(&transcript, &[], &[]);
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 64;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn an_older_unknown_effect_is_not_dropped_when_a_newer_round_exists() {
    let fixture = Fixture::new().await;
    let large = "unknown effect ".repeat(128);
    let mut transcript = vec![message(
        "unknown-run",
        1,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Earlier work")],
    )];
    let mut uncertain = round("unknown-run", 2, "unknown-call", &large, &fixture.tools[0]);
    let ContentBlock::ToolResult { result } = &mut uncertain[1].content[0] else {
        unreachable!()
    };
    result.status = ToolResultStatus::Unknown;
    transcript.extend(uncertain);
    transcript.push(message(
        "unknown-run",
        4,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Effect not confirmed")],
    ));
    transcript.push(message(
        "recent-run",
        5,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Recent work")],
    ));
    transcript.extend(round(
        "recent-run",
        6,
        "recent-call",
        "Recent observation",
        &fixture.tools[0],
    ));
    transcript.push(message(
        "recent-run",
        8,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Recent answer")],
    ));
    transcript.push(fixture.current_message(9));
    let full = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert!(full.selected_message_ids.contains(&id("message-3")));
    let mut limited = fixture.input(&transcript, &[], &[]);
    limited.limits.max_bytes = serde_json::to_vec(&full.request).unwrap().len() - large.len() / 2;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn loaded_skill_context_uses_its_pinned_version_without_gaining_system_authority() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let loaded = ContextItem::new(
        id("loaded-skill"),
        ContextOrigin::Skill,
        fixture.skill.skill.clone(),
        fixture.scope.clone(),
        vec![InputContent::Text {
            text: "Loaded task instructions".into(),
        }],
        ContextLifetime::Run {
            run_id: fixture.run_id.clone(),
        },
        ContextPriority::Required,
    );
    let items = vec![loaded.clone()];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &items, &[]))
        .unwrap();
    assert_eq!(projected.selected_context_ids, vec![id("loaded-skill")]);
    let system = |request: ModelRequest| {
        request
            .messages
            .into_iter()
            .filter(|message| message.role == ModelRole::System)
            .collect::<Vec<_>>()
    };
    assert_eq!(system(projected.request), system(baseline.request));
    let changed = ContextItem::new(
        loaded.item_id,
        loaded.origin,
        VersionedRef {
            id: loaded.source_ref.id,
            version: id("2"),
        },
        loaded.scope,
        loaded.content,
        loaded.lifetime,
        loaded.priority_class,
    );
    assert!(
        ContextAssembler::new()
            .project(&fixture.prompt, fixture.input(&transcript, &[changed], &[]))
            .is_err()
    );
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

## `crates/wickle/tests/model_execution.rs`

```rust
//! Policy, persisted accounting, and recovery around a single-call model port.
use futures_util::{StreamExt, stream};
use serde_json::json;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use wickle::*;

#[allow(dead_code)]
mod support;
use support::{admission, id, scope};

fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn request(provider: &str) -> ModelRequest {
    ModelRequest {
        request_id: id("logical-step"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference(provider),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id("model"),
            model_id: id("model"),
            model_version: id("release"),
            version_semantics: VersionSemantics::Pinned,
            provider: id(provider),
            target: JsonObject::new(),
            deployment_revision: None,
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("v1"),
            },
            adapter: reference(&format!("{provider}-adapter")),
            capability_revision: id("capabilities"),
            connection_ref: reference(&format!("{provider}-connection")),
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find evidence".into(),
            }],
        }],
        tools: vec![],
        output: ModelOutput::Text {},
        max_output_tokens: 32.try_into().unwrap(),
        options: JsonObject::new(),
        limits: ModelResponseLimits {
            max_input_bytes: 8192,
            max_response_bytes: 4096,
            max_delta_bytes: 1024,
            max_events: 32,
            max_tool_calls: 0,
        },
    }
}

struct Policy {
    calls: AtomicUsize,
    deny_at: AtomicUsize,
    approval_at: AtomicUsize,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            deny_at: AtomicUsize::new(usize::MAX),
            approval_at: AtomicUsize::new(usize::MAX),
        }
    }
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        _: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        let count = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(async move {
            Ok(if count >= self.deny_at.load(Ordering::SeqCst) {
                PolicyDecision::Deny {
                    reason: id("revoked"),
                }
            } else if count >= self.approval_at.load(Ordering::SeqCst) {
                PolicyDecision::RequireApproval {
                    reason: id("review"),
                }
            } else {
                PolicyDecision::Allow {}
            })
        })
    }
}

enum Reply {
    Complete,
    Fail(ModelFailureKind),
    IncompleteTool,
    Pending,
}
struct ScriptedModel {
    store: Arc<MemoryStateStore>,
    binding: ModelPortBinding,
    replies: Mutex<VecDeque<Reply>>,
    observed: Mutex<Vec<(ModelRequest, Id, CancellationToken)>>,
    entered: Notify,
}
impl ModelPort for ScriptedModel {
    fn binding(&self) -> ModelPortBinding {
        self.binding.clone()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        self.observed.lock().unwrap().push((
            request.clone(),
            context.attempt_id.clone(),
            context.cancellation.clone(),
        ));
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("no hidden extra physical call");
        let start = stream::once(async move {
            let saved = self
                .store
                .load(&context.scope, &context.run_id)
                .await
                .unwrap();
            let attempt = saved
                .snapshot
                .model_ledger
                .iter()
                .find(|entry| entry.attempt_id == context.attempt_id)
                .unwrap();
            assert!(matches!(attempt.state, ModelAttemptState::Reserved {}));
            assert_eq!(attempt.request_digest, request.digest());
            assert_eq!(attempt.route, request.route);
            assert!(
                saved
                    .snapshot
                    .reservations
                    .iter()
                    .any(|entry| entry.attempt_id == context.attempt_id)
            );
            self.entered.notify_one();
            Ok(ModelEvent::TextDelta {
                text: "candidate".into(),
            })
        });
        let tail: PortStream<'a, ModelEvent> = match reply {
            Reply::Complete => Box::pin(stream::iter([Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::Stop,
                metadata: ModelResponseMetadata {
                    provider_request_id: Some(id("provider-response")),
                    usage: Some(ModelUsage {
                        measurement: UsageMeasurement::Reported,
                        input_tokens: Some(3),
                        output_tokens: Some(2),
                    }),
                    ..ModelResponseMetadata::default()
                },
                continuation: vec![],
            })])),
            Reply::Fail(kind) => Box::pin(stream::iter([Ok(ModelEvent::ResponseError {
                kind,
                metadata: ModelResponseMetadata::default(),
            })])),
            Reply::IncompleteTool => Box::pin(stream::iter([Ok(ModelEvent::ToolArgumentsDelta {
                index: 0,
                provider_call_id: Some("tool".into()),
                name: Some("search".into()),
                delta: "{\"query\":".into(),
            })])),
            Reply::Pending => Box::pin(stream::pending()),
        };
        Box::pin(start.chain(tail))
    }
}
struct Fixture {
    store: Arc<MemoryStateStore>,
    budget: RunBudget,
    lease: RunLease,
    context: ExecutionContext,
    policy: Arc<Policy>,
}
impl Fixture {
    async fn new(models: u64, recovery: u64) -> Self {
        Self::with_records(models, recovery, Arc::new(RandomIdSource), vec![]).await
    }
    async fn with_records(
        models: u64,
        recovery: u64,
        ids: Arc<dyn IdSource>,
        records: Vec<ProtectedRecord>,
    ) -> Self {
        let clock = Arc::new(SystemClock::new());
        let now = clock.now().unwrap().utc_ms;
        let store = Arc::new(MemoryStateStore::new());
        let mut input = admission("run", "request", "session", "Find evidence", "1").await;
        input.snapshot.limits.max_model_calls = models.try_into().unwrap();
        input.snapshot.limits.max_recovery_attempts = recovery;
        input.snapshot.timing =
            RunTiming::new(now, input.snapshot.limits.max_elapsed_ms.get()).unwrap();
        input.events[0].timestamp_ms = now;
        input.records.extend(records);
        store.admit(&scope(), input).await.unwrap();
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("worker"), now, 20_000)
            .await
            .unwrap();
        let context = ExecutionContext::new(
            ExecutionContextData {
                scope: scope(),
                principal_ref: id("caller"),
                capability_grant_ref: id("grant"),
                trace_context: None,
                system_inputs: Some(SystemInputs::new(JsonObject::from([(
                    "database_fk".into(),
                    json!("host-only-value"),
                )]))),
            },
            CancellationToken::new(),
        );
        let budget = RunBudget::attach(
            store.clone(),
            clock,
            ids,
            scope(),
            id("run"),
            lease.clone(),
            context.cancellation.clone(),
        )
        .await
        .unwrap();
        Self {
            store,
            budget,
            lease,
            context,
            policy: Arc::new(Policy::default()),
        }
    }
    fn model(&self, provider: &str, replies: Vec<Reply>) -> Arc<ScriptedModel> {
        let route = request(provider).route;
        Arc::new(ScriptedModel {
            store: self.store.clone(),
            binding: ModelPortBinding {
                provider: route.provider,
                adapter: route.adapter,
                connection_ref: route.connection_ref,
            },
            replies: Mutex::new(replies.into()),
            observed: Mutex::new(vec![]),
            entered: Notify::new(),
        })
    }
    fn exchange(&self, model: Arc<dyn ModelPort>, retries: u32) -> ModelExchange {
        ModelExchange::new(
            model,
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap()),
        )
        .with_retry_policy(ModelRetryPolicy {
            max_retries: retries,
            backoff_ms: 0,
        })
    }
    async fn saved(&self) -> RunSnapshot {
        self.store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
    }
}

#[tokio::test]
async fn each_retry_has_its_own_saved_attempt_and_rechecks_policy() {
    let fixture = Fixture::new(3, 1).await;
    let model = fixture.model(
        "first",
        vec![Reply::Fail(ModelFailureKind::RateLimited), Reply::Complete],
    );
    let exchange = fixture.exchange(model.clone(), 2);
    let mut original = request("first");
    original.options = JsonObject::from([("reasoning_effort".into(), json!("high"))]);
    let result = exchange
        .generate(&original, &fixture.context, &fixture.budget)
        .await
        .unwrap();
    let Guarded::Completed(ModelExchangeOutcome::Completed { response }) = result else {
        panic!("expected completed response")
    };
    let saved = fixture.saved().await;
    assert_eq!(
        (saved.usage.model_calls, saved.usage.recovery_attempts),
        (2, 1)
    );
    assert_eq!(saved.model_ledger.len(), 2);
    assert_eq!(
        saved.model_ledger[0].state,
        ModelAttemptState::Failed {
            kind: ModelFailureKind::RateLimited
        }
    );
    assert_eq!(saved.model_ledger[1].state, ModelAttemptState::Completed {});
    assert_eq!(saved.model_ledger[1].reported_model_id, None);
    assert_eq!(saved.model_ledger[1].reported_model_version, None);
    assert_eq!(
        saved.model_ledger[1].usage.as_ref().unwrap().output_tokens,
        Some(2)
    );
    assert_eq!(response.request_id, saved.model_ledger[1].attempt_id);
    {
        let observed = model.observed.lock().unwrap();
        assert_eq!(observed.len(), 2);
        assert_ne!(observed[0].1, observed[1].1);
        for (request, attempt, _) in observed.iter() {
            assert_eq!(&request.request_id, attempt);
            assert_eq!(request.route, original.route);
            assert_eq!(request.messages, original.messages);
            assert_eq!(request.options, original.options);
            // A real Host value exists but no model request surface automatically copies it.
            assert!(
                !serde_json::to_string(request)
                    .unwrap()
                    .contains("host-only-value")
            );
        }
    }
    assert!(
        saved
            .model_ledger
            .iter()
            .all(|entry| entry.model_step_id == original.request_id)
    );
    assert_eq!(fixture.policy.calls.load(Ordering::SeqCst), 4);
    let events = fixture
        .store
        .read_events(&scope(), &id("run"), 0, 10)
        .await
        .unwrap();
    assert_eq!(events.events.len(), 3);
}

#[tokio::test]
async fn exhausted_recovery_and_model_budgets_each_stop_new_requests() {
    for (models, recovery) in [(8, 1), (1, 1)] {
        let fixture = Fixture::new(models, recovery).await;
        let model = fixture.model(
            "first",
            vec![
                Reply::Fail(ModelFailureKind::Transport),
                Reply::Fail(ModelFailureKind::Transport),
            ],
        );
        let exchange = fixture.exchange(model.clone(), 100);
        assert_eq!(
            exchange
                .generate(&request("first"), &fixture.context, &fixture.budget)
                .await
                .unwrap_err()
                .code,
            ErrorCode::BudgetExceeded
        );
        let expected = if models == 1 { 1 } else { 2 };
        assert_eq!(model.observed.lock().unwrap().len(), expected);
        assert_eq!(fixture.saved().await.usage.model_calls, expected as u64);
    }
}

#[tokio::test]
async fn authentication_capability_and_unchanged_context_overflow_are_not_retried() {
    for kind in [
        ModelFailureKind::Authentication,
        ModelFailureKind::Unsupported,
        ModelFailureKind::ContextOverflow,
    ] {
        let fixture = Fixture::new(4, 1).await;
        let model = fixture.model("first", vec![Reply::Fail(kind)]);
        let exchange = fixture.exchange(model.clone(), 3);
        let Guarded::Completed(ModelExchangeOutcome::Failed { failure }) = exchange
            .generate(&request("first"), &fixture.context, &fixture.budget)
            .await
            .unwrap()
        else {
            panic!("expected classified failure")
        };
        assert_eq!(failure.kind, kind);
        assert_eq!(model.observed.lock().unwrap().len(), 1);
        assert_eq!(fixture.saved().await.usage.recovery_attempts, 0);
    }
}

#[tokio::test]
async fn default_retry_is_disabled_and_partial_tools_never_become_a_complete_plan() {
    let fixture = Fixture::new(4, 1).await;
    let model = fixture.model("first", vec![Reply::IncompleteTool]);
    let exchange = fixture.exchange(model.clone(), 0);
    let mut request = request("first");
    request.limits.max_tool_calls = 1;
    let Guarded::Completed(ModelExchangeOutcome::Failed { failure }) = exchange
        .generate(&request, &fixture.context, &fixture.budget)
        .await
        .unwrap()
    else {
        panic!("expected incomplete response rejection")
    };
    assert_eq!(failure.kind, ModelFailureKind::Protocol);
    assert_eq!(failure.partial_text(), "candidate");
    let saved = fixture.saved().await;
    assert!(saved.tool_ledger.is_empty());
    assert_eq!(saved.usage.tool_attempts, 0);
    assert_eq!(saved.usage.recovery_attempts, 0);
    assert_eq!(model.observed.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn initial_denial_and_revocation_after_reservation_both_prevent_adapter_entry() {
    for deny_at in [1, 2, 3] {
        let fixture = Fixture::new(4, 1).await;
        fixture.policy.deny_at.store(deny_at, Ordering::SeqCst);
        let model = fixture.model("first", vec![Reply::Fail(ModelFailureKind::RateLimited)]);
        let exchange = fixture.exchange(model.clone(), 1);
        assert_eq!(
            exchange
                .generate(&request("first"), &fixture.context, &fixture.budget)
                .await
                .unwrap_err()
                .code,
            ErrorCode::AccessDenied
        );
        assert_eq!(
            model.observed.lock().unwrap().len(),
            usize::from(deny_at == 3)
        );
        assert_eq!(
            fixture.saved().await.usage.model_calls,
            u64::from(deny_at >= 2)
        );
    }
}

#[tokio::test]
async fn approval_does_not_dispatch_and_unreserved_approval_consumes_no_budget() {
    for approval_at in [1, 2] {
        let fixture = Fixture::new(4, 1).await;
        fixture
            .policy
            .approval_at
            .store(approval_at, Ordering::SeqCst);
        let model = fixture.model("first", vec![]);
        let exchange = fixture.exchange(model.clone(), 0);
        assert!(matches!(
            exchange
                .generate(&request("first"), &fixture.context, &fixture.budget)
                .await
                .unwrap(),
            Guarded::ApprovalRequired(_)
        ));
        assert!(model.observed.lock().unwrap().is_empty());
        assert_eq!(
            fixture.saved().await.usage.model_calls,
            u64::from(approval_at == 2)
        );
    }
}

#[tokio::test]
async fn wrong_connection_scope_or_opaque_route_is_rejected_before_reservation() {
    let fixture = Fixture::new(4, 1).await;
    let model = fixture.model("second", vec![]);
    let exchange = fixture.exchange(model.clone(), 0);
    assert!(
        exchange
            .generate(&request("first"), &fixture.context, &fixture.budget)
            .await
            .is_err()
    );
    let mut foreign_context = fixture.context.clone();
    foreign_context.data.scope.workspace_id = id("foreign");
    assert_eq!(
        exchange
            .generate(&request("second"), &foreign_context, &fixture.budget)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    let mut changed = request("second");
    changed.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![ModelContent::Opaque {
            continuation: OpaqueContinuation::new(
                &request("first").route,
                json!({"signature":"first-private"}),
            ),
        }],
    });
    assert!(
        exchange
            .generate(&changed, &fixture.context, &fixture.budget)
            .await
            .is_err()
    );
    assert!(model.observed.lock().unwrap().is_empty());
    assert_eq!(fixture.saved().await.usage.model_calls, 0);
}

#[tokio::test]
async fn cancellation_drops_the_adapter_signal_and_retains_an_unknown_charged_attempt() {
    let fixture = Fixture::new(4, 1).await;
    let model = fixture.model("first", vec![Reply::Pending]);
    let exchange = fixture.exchange(model.clone(), 2);
    let request = request("first");
    let execution = exchange.generate(&request, &fixture.context, &fixture.budget);
    let cancel = async {
        model.entered.notified().await;
        fixture.context.cancellation.cancel();
    };
    let (result, ()) = tokio::join!(execution, cancel);
    assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
    let saved = fixture.saved().await;
    assert_eq!(saved.usage.model_calls, 1);
    assert_eq!(saved.usage.recovery_attempts, 0);
    assert_eq!(saved.model_ledger[0].state, ModelAttemptState::Unknown {});
    assert!(model.observed.lock().unwrap()[0].2.is_cancelled());
}

#[tokio::test]
async fn exhausted_recovery_retains_the_last_partial_failure_in_protected_storage() {
    let fixture = Fixture::new(4, 0).await;
    let model = fixture.model("first", vec![Reply::Fail(ModelFailureKind::Transport)]);
    let exchange = fixture.exchange(model.clone(), 1);
    assert_eq!(
        exchange
            .generate(&request("first"), &fixture.context, &fixture.budget)
            .await
            .unwrap_err()
            .code,
        ErrorCode::BudgetExceeded
    );
    let saved = fixture.saved().await;
    let record = fixture
        .store
        .read_record(
            &scope(),
            saved.model_ledger[0].response_ref.as_ref().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(record.value()["outcome"]["result"], "failed");
    assert_eq!(record.value()["outcome"]["failure"]["kind"], "transport");
    assert_eq!(
        record.value()["outcome"]["failure"]["partial_text"],
        "candidate"
    );
    assert_eq!(model.observed.lock().unwrap().len(), 1);
    let mut foreign = scope();
    foreign.workspace_id = id("other");
    assert!(
        fixture
            .store
            .read_record(&foreign, record.reference())
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn caller_cancellation_interrupts_backoff_even_with_an_independent_budget_token() {
    let mut fixture = Fixture::new(4, 1).await;
    fixture.context.cancellation = CancellationToken::new();
    let model = fixture.model("first", vec![Reply::Fail(ModelFailureKind::Transport)]);
    let exchange = ModelExchange::new(
        model.clone(),
        Arc::new(PolicyGate::new(fixture.policy.clone(), Duration::from_secs(1)).unwrap()),
    )
    .with_retry_policy(ModelRetryPolicy {
        max_retries: 1,
        backoff_ms: 1000,
    });
    let request = request("first");
    let cancel = async {
        loop {
            if fixture.saved().await.usage.recovery_attempts == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        fixture.context.cancellation.cancel();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_millis(5), async {
        tokio::join!(
            exchange.generate(&request, &fixture.context, &fixture.budget),
            cancel
        )
    })
    .await
    .expect("caller cancellation must not wait for the one-second backoff");
    assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
    assert_eq!(model.observed.lock().unwrap().len(), 1);
}

struct PendingPolicy {
    entered: Notify,
}
impl PolicyPort for PendingPolicy {
    fn authorize<'a>(
        &'a self,
        _: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test(start_paused = true)]
async fn budget_cancellation_interrupts_policy_even_with_an_independent_caller_token() {
    let mut fixture = Fixture::new(4, 1).await;
    let budget_cancellation = fixture.context.cancellation.clone();
    fixture.context.cancellation = CancellationToken::new();
    let policy = Arc::new(PendingPolicy {
        entered: Notify::new(),
    });
    let model = fixture.model("first", vec![]);
    let exchange = ModelExchange::new(
        model.clone(),
        Arc::new(PolicyGate::new(policy.clone(), Duration::from_secs(1)).unwrap()),
    );
    let request = request("first");
    let cancel = async {
        policy.entered.notified().await;
        budget_cancellation.cancel();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_millis(5), async {
        tokio::join!(
            exchange.generate(&request, &fixture.context, &fixture.budget),
            cancel
        )
    })
    .await
    .expect("budget cancellation must not wait for the policy timeout");
    assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
    assert!(model.observed.lock().unwrap().is_empty());
    assert_eq!(fixture.saved().await.usage.model_calls, 0);
}

struct FixedAttempt;
impl IdSource for FixedAttempt {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id("fixed-attempt"))
    }
}

#[tokio::test]
async fn failed_invocation_or_response_persistence_never_causes_an_untracked_retry() {
    for (record_id, expected_calls) in [
        ("model-invocation-fixed-attempt", 0),
        ("model-response-fixed-attempt", 1),
    ] {
        let existing = ProtectedRecord::new(id(record_id), 1, json!({"original":"immutable"}));
        let fixture =
            Fixture::with_records(4, 1, Arc::new(FixedAttempt), vec![existing.clone()]).await;
        let model = fixture.model("first", vec![Reply::Complete]);
        let exchange = fixture.exchange(model.clone(), 5);
        assert_eq!(
            exchange
                .generate(&request("first"), &fixture.context, &fixture.budget)
                .await
                .unwrap_err()
                .code,
            ErrorCode::RecordConflict
        );
        assert_eq!(model.observed.lock().unwrap().len(), expected_calls);
        let saved = fixture.saved().await;
        assert_eq!(saved.usage.model_calls, 1);
        assert_eq!(saved.usage.recovery_attempts, 0);
        assert_eq!(saved.model_ledger.len(), expected_calls);
        if expected_calls == 1 {
            assert_eq!(saved.model_ledger[0].state, ModelAttemptState::Reserved {});
            assert_eq!(saved.model_ledger[0].response_ref, None);
        }
        assert_eq!(
            fixture
                .store
                .read_record(&scope(), existing.reference())
                .await
                .unwrap()
                .value(),
            existing.value()
        );
    }
}

#[tokio::test]
async fn a_completed_ledger_entry_requires_the_exact_typed_response_and_reservation() {
    let fixture = Fixture::new(4, 1).await;
    fixture.policy.approval_at.store(2, Ordering::SeqCst);
    let model = fixture.model("first", vec![]);
    let exchange = fixture.exchange(model, 0);
    exchange
        .generate(&request("first"), &fixture.context, &fixture.budget)
        .await
        .unwrap();
    let before = fixture.saved().await;
    let entry = &before.model_ledger[0];
    let metadata = ModelResponseMetadata::default();
    let body = StoredModelResponse {
        request_id: entry.attempt_id.clone(),
        route_digest: entry.route.digest(),
        outcome: ModelExchangeOutcome::Completed {
            response: ModelResponse {
                request_id: entry.attempt_id.clone(),
                route_digest: entry.route.digest(),
                text: "Completed candidate".into(),
                tool_calls: vec![],
                finish: ModelFinish::Stop,
                metadata,
                continuation: vec![],
            },
        },
    };
    let valid = serde_json::to_value(&body).unwrap();
    let mut wrong_attempt = valid.clone();
    wrong_attempt["request_id"] = json!("another-attempt");
    let mut wrong_route = valid.clone();
    wrong_route["route_digest"] = serde_json::to_value(request("second").route.digest()).unwrap();
    let mut wrong_metadata = valid.clone();
    wrong_metadata["outcome"]["response"]["metadata"]["reported_model_id"] =
        json!("unreported-model");
    let mut wrong_finish = valid.clone();
    wrong_finish["outcome"]["response"]["finish"] = json!("tool_calls");
    for (index, value) in [
        json!({"unrelated":"record"}),
        wrong_attempt,
        wrong_route,
        wrong_metadata,
        wrong_finish,
    ]
    .into_iter()
    .enumerate()
    {
        let record = ProtectedRecord::new(id(&format!("invalid-{index}")), 1, value);
        let mut commit = support::prepared(
            &before,
            fixture.lease.clone(),
            before.timing.last_observed_at_ms,
        );
        commit.snapshot.model_ledger[0].state = ModelAttemptState::Completed {};
        commit.snapshot.model_ledger[0].response_ref = Some(record.reference().clone());
        commit.records.push(record);
        assert_eq!(
            fixture
                .store
                .commit(&scope(), &id("run"), commit)
                .await
                .unwrap_err()
                .code,
            ErrorCode::InvalidSnapshot
        );
        assert_eq!(fixture.saved().await, before);
    }
    let mut missing = before.clone();
    missing.model_ledger[0].state = ModelAttemptState::Completed {};
    assert!(missing.validate().is_err());
    let mut unreserved = before.clone();
    unreserved.model_ledger[0].attempt_id = id("unreserved");
    assert!(unreserved.validate().is_err());
    let record = ProtectedRecord::new(id("valid-response"), 1, valid);
    let mut commit = support::prepared(
        &before,
        fixture.lease.clone(),
        before.timing.last_observed_at_ms,
    );
    commit.snapshot.model_ledger[0].state = ModelAttemptState::Completed {};
    commit.snapshot.model_ledger[0].response_ref = Some(record.reference().clone());
    commit.records.push(record.clone());
    let saved = fixture
        .store
        .commit(&scope(), &id("run"), commit)
        .await
        .unwrap();
    assert_eq!(
        saved.snapshot.model_ledger[0].response_ref.as_ref(),
        Some(record.reference())
    );
}
```

## `crates/wickle/tests/model_protocol.rs`

```rust
//! Bounded response assembly and provider projection isolation.
use futures_util::{StreamExt, stream};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}

fn route(provider: &str) -> ResolvedModelRoute {
    ResolvedModelRoute {
        binding: reference(provider),
        catalog_revision: id("catalog-1"),
        routing_policy_revision: id("policy-1"),
        requested_model: id("model"),
        model_id: id("model"),
        model_version: id("release-1"),
        version_semantics: VersionSemantics::Pinned,
        provider: id(provider),
        target: JsonObject::from([("region".into(), json!("region-a"))]),
        deployment_revision: None,
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        adapter: reference(&format!("{provider}-adapter")),
        capability_revision: id("capabilities-1"),
        connection_ref: reference(&format!("{provider}-connection")),
    }
}

fn request() -> ModelRequest {
    ModelRequest {
        request_id: id("request"),
        purpose: ModelPurpose::Agent,
        route: route("first"),
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find the requested records".into(),
            }],
        }],
        tools: vec![ModelTool {
            name: id("search"),
            description: "Search available records".into(),
            model_input_schema: json!({
                "type":"object", "required":["query"], "additionalProperties":false,
                "properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1}}
            }),
        }],
        output: ModelOutput::Text {},
        max_output_tokens: 128.try_into().unwrap(),
        options: JsonObject::new(),
        limits: ModelResponseLimits {
            max_input_bytes: 16_384,
            max_response_bytes: 4096,
            max_delta_bytes: 1024,
            max_events: 32,
            max_tool_calls: 4,
        },
    }
}

fn text(value: &str) -> ModelEvent {
    ModelEvent::TextDelta { text: value.into() }
}

fn tool(index: u32, provider_id: Option<&str>, name: Option<&str>, delta: &str) -> ModelEvent {
    ModelEvent::ToolArgumentsDelta {
        index,
        provider_call_id: provider_id.map(str::to_owned),
        name: name.map(str::to_owned),
        delta: delta.into(),
    }
}

fn completed(finish: ModelFinish) -> ModelEvent {
    ModelEvent::ResponseCompleted {
        finish,
        metadata: ModelResponseMetadata::default(),
        continuation: vec![],
    }
}

async fn collect(
    request: &ModelRequest,
    events: Vec<ModelEvent>,
) -> Result<ModelResponse, ModelProtocolError> {
    collect_model_response(request, Box::pin(stream::iter(events.into_iter().map(Ok)))).await
}

#[tokio::test]
async fn interleaved_tool_fragments_preserve_separate_arguments_and_unreported_metadata() {
    let request = request();
    let response = collect(
        &request,
        vec![
            text("Searching "),
            tool(0, Some("call-a"), Some("search"), r#"{"query":"ali"#),
            tool(1, Some("call-b"), Some("search"), r#"{"query":"\u03"#),
            text("records"),
            tool(0, None, None, r#"ce","limit":3}"#),
            tool(1, None, None, r#"bb"}"#),
            ModelEvent::ResponseCompleted {
                finish: ModelFinish::ToolCalls,
                metadata: ModelResponseMetadata {
                    provider_request_id: Some(id("provider-request")),
                    reported_model_id: None,
                    reported_model_version: None,
                    usage: Some(ModelUsage {
                        measurement: UsageMeasurement::Reported,
                        input_tokens: Some(12),
                        output_tokens: None,
                    }),
                },
                continuation: vec![],
            },
        ],
    )
    .await
    .unwrap();
    assert_eq!(response.request_id, request.request_id);
    assert_eq!(response.route_digest, request.route.digest());
    assert_eq!(response.text, "Searching records");
    assert_eq!(response.tool_calls.len(), 2);
    assert_eq!(response.tool_calls[0].provider_call_id, id("call-a"));
    assert_eq!(
        response.tool_calls[0].model_inputs,
        JsonObject::from([("query".into(), json!("alice")), ("limit".into(), json!(3)),])
    );
    assert_eq!(response.tool_calls[1].provider_call_id, id("call-b"));
    assert_eq!(response.tool_calls[1].model_inputs["query"], json!("λ"));
    assert!(
        response
            .tool_calls
            .iter()
            .all(|call| call.validation == ToolCallValidation::Valid)
    );
    assert_eq!(response.metadata.reported_model_id, None);
    assert_eq!(response.metadata.reported_model_version, None);
    assert_eq!(response.metadata.usage.unwrap().output_tokens, None);
}

#[tokio::test]
async fn fragments_and_length_limited_responses_never_become_complete_tool_proposals() {
    for events in [
        vec![tool(
            0,
            Some("call"),
            Some("search"),
            r#"{"query":"unfinished"#,
        )],
        vec![tool(
            0,
            Some("call"),
            Some("search"),
            r#"{"query":"complete"}"#,
        )],
        vec![
            tool(0, Some("call"), Some("search"), r#"{"query":"complete"}"#),
            completed(ModelFinish::Length),
        ],
        vec![
            text("partial"),
            ModelEvent::ResponseError {
                kind: ModelFailureKind::Transport,
                metadata: ModelResponseMetadata::default(),
            },
        ],
    ] {
        assert!(collect(&request(), events).await.is_err());
    }
    let disconnected = stream::iter(vec![
        Ok(tool(
            0,
            Some("call"),
            Some("search"),
            r#"{"query":"complete"}"#,
        )),
        Err(ContractError::new(
            ErrorCode::PersistenceUnavailable,
            "transport",
        )),
    ]);
    assert!(
        collect_model_response(&request(), Box::pin(disconnected))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn contradictory_or_repeated_terminals_and_events_after_completion_are_rejected() {
    for events in [
        vec![completed(ModelFinish::Stop), completed(ModelFinish::Stop)],
        vec![
            text("complete"),
            completed(ModelFinish::Stop),
            text("late fragment"),
        ],
        vec![
            tool(0, Some("call"), Some("search"), r#"{"query":"x"}"#),
            completed(ModelFinish::Stop),
        ],
        vec![text("no call"), completed(ModelFinish::ToolCalls)],
        vec![
            tool(0, Some("call"), Some("search"), r#"{"query":"x"}"#),
            completed(ModelFinish::Refusal),
        ],
    ] {
        assert!(collect(&request(), events).await.is_err());
    }
}

#[tokio::test]
async fn malformed_or_ambiguous_argument_json_is_rejected_before_returning_calls() {
    for arguments in [
        r#"{"query":"first","query":"second"}"#,
        r#"{"query":"x","nested":{"key":1,"key":2}}"#,
        r#"{"query":"x","limit":NaN}"#,
        r#"{"query":"x"} trailing"#,
        "[]",
        "null",
        "true",
        "",
        r#"{"query":"unfinished"#,
    ] {
        assert!(
            collect(
                &request(),
                vec![
                    tool(0, Some("call"), Some("search"), arguments),
                    completed(ModelFinish::ToolCalls),
                ]
            )
            .await
            .is_err(),
            "accepted arguments: {arguments}"
        );
    }
}

#[tokio::test]
async fn tool_schema_and_unknown_name_failures_remain_explicit_unexecutable_proposals() {
    let response = collect(
        &request(),
        vec![
            tool(0, Some("unknown"), Some("unregistered"), r#"{"query":"x"}"#),
            tool(1, Some("wrong-type"), Some("search"), r#"{"query":7}"#),
            tool(
                2,
                Some("hidden-input"),
                Some("search"),
                r#"{"query":"x","workspace_id":"invented"}"#,
            ),
            tool(
                3,
                Some("valid"),
                Some("search"),
                r#"{"query":"x","limit":2}"#,
            ),
            completed(ModelFinish::ToolCalls),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        response.tool_calls[0].validation,
        ToolCallValidation::UnknownTool
    );
    assert_eq!(
        response.tool_calls[1].validation,
        ToolCallValidation::InvalidArguments
    );
    assert_eq!(
        response.tool_calls[2].validation,
        ToolCallValidation::InvalidArguments
    );
    assert_eq!(response.tool_calls[3].validation, ToolCallValidation::Valid);
    assert_eq!(
        response.tool_calls[2].model_inputs["workspace_id"],
        json!("invented")
    );
}

#[tokio::test]
async fn provider_call_collisions_and_metadata_reassignment_are_rejected() {
    for events in [
        vec![
            tool(0, Some("duplicate"), Some("search"), r#"{"query":"a"}"#),
            tool(1, Some("duplicate"), Some("search"), r#"{"query":"b"}"#),
        ],
        vec![
            tool(0, Some("original"), Some("search"), "{"),
            tool(0, Some("replacement"), None, r#""query":"a"}"#),
        ],
        vec![
            tool(0, Some("call"), Some("search"), "{"),
            tool(0, None, Some("other"), r#""query":"a"}"#),
        ],
        vec![tool(0, None, None, r#"{"query":"a"}"#)],
    ] {
        let mut events = events;
        events.push(completed(ModelFinish::ToolCalls));
        assert!(collect(&request(), events).await.is_err());
    }
    for (provider_id, name) in [
        ("", "search"),
        (" \n", "search"),
        ("call", ""),
        ("call", " \n"),
    ] {
        assert!(
            collect(
                &request(),
                vec![
                    tool(0, Some(provider_id), Some(name), r#"{"query":"a"}"#),
                    completed(ModelFinish::ToolCalls),
                ]
            )
            .await
            .is_err()
        );
    }
}

#[tokio::test]
async fn response_byte_delta_and_tool_count_limits_fail_without_truncating_to_success() {
    let mut limited = request();
    limited.limits.max_response_bytes = 3;
    limited.limits.max_delta_bytes = 2;
    assert!(
        collect(
            &limited,
            vec![text("é"), text("é"), completed(ModelFinish::Stop)]
        )
        .await
        .is_err()
    );
    limited = request();
    limited.limits.max_delta_bytes = 3;
    assert!(
        collect(&limited, vec![text("éé"), completed(ModelFinish::Stop)])
            .await
            .is_err()
    );
    limited = request();
    limited.limits.max_tool_calls = 1;
    assert!(
        collect(
            &limited,
            vec![
                tool(0, Some("first"), Some("search"), r#"{"query":"a"}"#),
                tool(1, Some("second"), Some("search"), r#"{"query":"b"}"#),
                completed(ModelFinish::ToolCalls),
            ]
        )
        .await
        .is_err()
    );
    limited.limits.max_tool_calls = 0;
    assert!(
        collect(
            &limited,
            vec![
                tool(0, Some("first"), Some("search"), r#"{"query":"a"}"#),
                completed(ModelFinish::ToolCalls)
            ]
        )
        .await
        .is_err()
    );
    let response = collect(
        &limited,
        vec![text("text remains valid"), completed(ModelFinish::Stop)],
    )
    .await
    .unwrap();
    assert_eq!(response.text, "text remains valid");
}

#[tokio::test]
async fn empty_deltas_cannot_bypass_the_finite_event_limit() {
    let mut limited = request();
    limited.limits.max_events = 3;
    let at_limit = collect(
        &limited,
        vec![text("first"), text(" second"), completed(ModelFinish::Stop)],
    )
    .await
    .unwrap();
    assert_eq!(at_limit.text, "first second");
    let observed = Arc::new(AtomicUsize::new(0));
    let count = observed.clone();
    let events = stream::repeat_with(|| Ok(text(""))).inspect(move |_| {
        count.fetch_add(1, Ordering::SeqCst);
    });
    assert!(
        collect_model_response(&limited, Box::pin(events))
            .await
            .is_err()
    );
    assert_eq!(observed.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn wire_identifiers_use_the_protocol_rules_without_requiring_uuid_format() {
    for (provider_id, name) in [
        ("call\nname".to_owned(), "search".to_owned()),
        ("x".repeat(257), "search".to_owned()),
        ("call".to_owned(), "search tool".to_owned()),
        ("call".to_owned(), "search.tool".to_owned()),
        ("call".to_owned(), "검색".to_owned()),
        ("call".to_owned(), "x".repeat(65)),
    ] {
        assert!(
            collect(
                &request(),
                vec![
                    tool(0, Some(&provider_id), Some(&name), r#"{"query":"a"}"#),
                    completed(ModelFinish::ToolCalls),
                ],
            )
            .await
            .is_err()
        );
    }
    let response = collect(
        &request(),
        vec![
            tool(
                0,
                Some("provider:opaque/call"),
                Some("search"),
                r#"{"query":"a"}"#,
            ),
            completed(ModelFinish::ToolCalls),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        response.tool_calls[0].provider_call_id,
        id("provider:opaque/call")
    );
}

#[tokio::test]
async fn opaque_continuation_requires_the_exact_provider_route_and_release() {
    let mut original = request();
    let opaque =
        OpaqueContinuation::new(&original.route, json!({"thought_signature":"opaque-data"}));
    original.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![ModelContent::Opaque {
            continuation: opaque.clone(),
        }],
    });
    assert!(
        collect(
            &original,
            vec![text("continued"), completed(ModelFinish::Stop)]
        )
        .await
        .is_ok()
    );
    let mut altered_routes = vec![route("second")];
    let mut changed = original.route.clone();
    changed.model_version = id("release-2");
    altered_routes.push(changed);
    let mut changed = original.route.clone();
    changed.api_contract.version = id("v2");
    altered_routes.push(changed);
    let mut changed = original.route.clone();
    changed.connection_ref.version = id("rotated-connection");
    altered_routes.push(changed);
    let mut changed = original.route.clone();
    changed.target.insert("region".into(), json!("region-b"));
    altered_routes.push(changed);
    for changed in altered_routes {
        let mut incompatible = original.clone();
        incompatible.route = changed;
        assert!(
            collect(
                &incompatible,
                vec![text("must not continue"), completed(ModelFinish::Stop)]
            )
            .await
            .is_err()
        );
    }
    let foreign_opaque = OpaqueContinuation::new(&route("second"), json!({"signature":"foreign"}));
    assert!(
        collect(
            &request(),
            vec![
                text("answer"),
                ModelEvent::ResponseCompleted {
                    finish: ModelFinish::Stop,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![foreign_opaque],
                }
            ]
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn preflight_rejects_mismatched_port_bindings_and_oversized_input() {
    let request = request();
    let binding = ModelPortBinding {
        provider: request.route.provider.clone(),
        adapter: request.route.adapter.clone(),
        connection_ref: request.route.connection_ref.clone(),
    };
    assert!(binding.matches_route(&request.route));
    let mut wrong = binding.clone();
    wrong.provider = id("second");
    assert!(!wrong.matches_route(&request.route));
    wrong = binding.clone();
    wrong.adapter.version = id("different-adapter");
    assert!(!wrong.matches_route(&request.route));
    wrong = binding;
    wrong.connection_ref.version = id("different-credential-binding");
    assert!(!wrong.matches_route(&request.route));
    let mut oversized = request;
    oversized.limits.max_input_bytes = 1;
    assert!(oversized.validate().is_err());
}

#[test]
fn host_options_affect_request_identity_and_bounds_without_changing_empty_request_encoding() {
    let mut request = request();
    let mut legacy = serde_json::to_value(&request).unwrap();
    legacy.as_object_mut().unwrap().remove("options");
    let restored: ModelRequest = serde_json::from_value(legacy.clone()).unwrap();
    assert_eq!(restored.digest(), canonical_digest(&legacy));
    assert_eq!(restored, request);
    let original_digest = request.digest();
    request
        .options
        .insert("reasoning_effort".into(), json!("high"));
    request.validate().unwrap();
    assert_ne!(request.digest(), original_digest);
    let high_digest = request.digest();
    request
        .options
        .insert("reasoning_effort".into(), json!("low"));
    assert_ne!(request.digest(), high_digest);
    request.options.insert(
        "reasoning_effort".into(),
        json!("x".repeat(request.limits.max_input_bytes)),
    );
    assert_eq!(
        request.validate().unwrap_err().path,
        "model_request.input_size"
    );
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

## `crates/wickle/tests/state.rs`

```rust
//! Atomic admission, persistence, leases, and scope isolation of the memory store.

use serde_json::json;
use std::{collections::BTreeSet, sync::Arc};
use wickle::*;

mod support;
use support::*;

#[tokio::test]
async fn admitted_model_options_are_fixed_for_replay_and_later_commits() {
    fn with_effort(mut input: AdmissionInput, effort: &str) -> AdmissionInput {
        input.snapshot.request.model_options =
            JsonObject::from([("reasoning_effort".into(), json!(effort))]);
        input.snapshot.request_digest =
            admission_digest(&input.snapshot.request, &input.snapshot.profile, None);
        let RunEventPayload::RunStarted { request_ref, .. } = &mut input.events[0].payload else {
            unreachable!()
        };
        let record = ProtectedRecord::new(
            request_ref.record_id.clone(),
            request_ref.revision,
            serde_json::to_value(&input.snapshot.request).unwrap(),
        );
        let old_ref = request_ref.clone();
        *request_ref = record.reference().clone();
        *input
            .records
            .iter_mut()
            .find(|record| record.reference() == &old_ref)
            .unwrap() = record;
        input
    }
    let store = MemoryStateStore::new();
    let first = with_effort(
        admission("run", "request", "session", "input", "1").await,
        "high",
    );
    let expected_options = first.snapshot.request.model_options.clone();
    let original = store.admit(&scope(), first).await.unwrap().state;
    let replay = with_effort(
        admission("replacement", "request", "session", "input", "2").await,
        "high",
    );
    let replay = store.admit(&scope(), replay).await.unwrap();
    assert!(!replay.created);
    assert_eq!(
        replay.state.snapshot.request.model_options,
        expected_options
    );
    let changed = with_effort(
        admission("replacement", "request", "session", "input", "2").await,
        "low",
    );
    assert_ne!(
        changed.snapshot.request_digest,
        original.snapshot.request_digest
    );
    assert_eq!(
        store.admit(&scope(), changed).await.unwrap_err().code,
        ErrorCode::RequestConflict
    );

    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("worker"), 1, 100)
        .await
        .unwrap();
    let mut change = prepared(&original.snapshot, lease, 2);
    change
        .snapshot
        .request
        .model_options
        .insert("reasoning_effort".into(), json!("low"));
    change.snapshot.request_digest =
        admission_digest(&change.snapshot.request, &change.snapshot.profile, None);
    assert_eq!(
        store
            .commit(&scope(), &id("run"), change)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidTransition
    );
    let saved = store.load(&scope(), &id("run")).await.unwrap().snapshot;
    let restored = RunSnapshot::from_json(&serde_json::to_string(&saved).unwrap()).unwrap();
    assert_eq!(restored.request.model_options, expected_options);
    assert_eq!(restored.request_digest, original.snapshot.request_digest);
}

#[tokio::test]
async fn identical_retries_return_the_original_run_without_replacing_resolved_metadata() {
    let store = MemoryStateStore::new();
    let first = admission("run-a", "request", "session", "input", "1").await;
    let receipt = store.admit(&scope(), first.clone()).await.unwrap();
    assert!(receipt.created);
    let retry = admission("run-b", "request", "session", "input", "2").await;
    let replay = store.admit(&scope(), retry).await.unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state.snapshot.run_id, id("run-a"));
    assert_eq!(
        replay.state.snapshot.profile.resolution_digest(),
        first.snapshot.profile.resolution_digest()
    );
    assert_eq!(replay.state.messages.len(), 1);
    let changed = admission("run-c", "request", "session", "different input", "1").await;
    assert!(store.admit(&scope(), changed).await.is_err());
    assert_eq!(
        store
            .load(&scope(), &id("run-a"))
            .await
            .unwrap()
            .snapshot
            .revision,
        0
    );
    let events = store
        .read_events(&scope(), &id("run-a"), 0, 100)
        .await
        .unwrap();
    assert_eq!(events.events.len(), 1);
}

#[tokio::test]
async fn concurrent_duplicate_admission_creates_exactly_one_run() {
    let store = Arc::new(MemoryStateStore::new());
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut handles = vec![];
    for n in 0..8 {
        let input = admission(&format!("run-{n}"), "request", "session", "input", "1").await;
        let store = store.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            store.admit(&scope(), input).await.unwrap()
        }));
    }
    let mut created = 0;
    let mut ids = BTreeSet::new();
    for h in handles {
        let result = h.await.unwrap();
        created += usize::from(result.created);
        ids.insert(result.state.snapshot.run_id);
    }
    assert_eq!(created, 1);
    assert_eq!(ids.len(), 1);
}

#[tokio::test]
async fn distinct_concurrent_requests_create_only_one_active_run_in_the_session() {
    let store = Arc::new(MemoryStateStore::new());
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut handles = Vec::new();
    for n in 0..8 {
        let input = admission(
            &format!("run-{n}"),
            &format!("request-{n}"),
            "session",
            "input",
            "1",
        )
        .await;
        let store = store.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            store.admit(&scope(), input).await
        }));
    }
    let mut accepted = Vec::new();
    let mut rejected = 0;
    for handle in handles {
        match handle.await.unwrap() {
            Ok(result) => accepted.push(result.state.snapshot.run_id),
            Err(_) => rejected += 1,
        }
    }
    assert_eq!(accepted.len(), 1);
    assert_eq!(rejected, 7);
    assert_eq!(
        store
            .load_session(&scope(), &id("session"))
            .await
            .unwrap()
            .active_run_id
            .as_ref(),
        accepted.first()
    );
}

#[tokio::test]
async fn waiting_keeps_the_session_busy_even_after_the_worker_releases_its_lease() {
    let store = MemoryStateStore::new();
    store
        .admit(
            &scope(),
            admission("run", "request", "session", "input", "1").await,
        )
        .await
        .unwrap();
    let state = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut update = prepared(&state.snapshot, lease.clone(), 101);
    let wait = WaitState {
        wait_id: id("wait"),
        target: WaitTarget::Input {
            request: InputRequest {
                input_request_id: id("input-request"),
                call_id: id("input-call"),
                question: "Choose a source".into(),
                schema_ref: None,
            },
        },
        expires_at_ms: Some(1000),
    };
    let record = ProtectedRecord::new(id("wait-record"), 1, serde_json::to_value(&wait).unwrap());
    update.snapshot.status = RunStatus::Waiting;
    update.snapshot.phase = RunPhase::Waiting;
    update.snapshot.wait = Some(wait);
    update.snapshot.last_event_seq = 2;
    update.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        2,
        RunEventPayload::RunWaiting {
            wait_ref: record.reference().clone(),
        },
    ));
    update.records.push(record);
    store.commit(&scope(), &id("run"), update).await.unwrap();
    store
        .release_lease(&scope(), &id("run"), &lease, 102)
        .await
        .unwrap();
    assert!(
        store
            .admit(
                &scope(),
                admission("other", "other-request", "session", "other", "1").await
            )
            .await
            .is_err()
    );
    let replay = store
        .admit(
            &scope(),
            admission("replacement", "request", "session", "input", "2").await,
        )
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state.snapshot.status, RunStatus::Waiting);
}

#[tokio::test]
async fn competing_requests_cannot_share_an_active_session_and_terminal_commit_releases_it() {
    let store = MemoryStateStore::new();
    let first = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), first).await.unwrap();
    assert!(
        store
            .admit(
                &scope(),
                admission("other", "other-request", "session", "other", "1").await
            )
            .await
            .is_err()
    );
    let state = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 50)
        .await
        .unwrap();
    store
        .commit(&scope(), &id("run"), finished(&state.snapshot, lease, 101))
        .await
        .unwrap();
    assert!(
        store
            .load_session(&scope(), &id("session"))
            .await
            .unwrap()
            .active_run_id
            .is_none()
    );
    let mut second = admission("other", "other-request", "session", "other", "1").await;
    second.messages[0].sequence = 2.try_into().unwrap();
    assert!(store.admit(&scope(), second).await.unwrap().created);
    assert_eq!(
        store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .status,
        RunStatus::Succeeded
    );
    let replay = store
        .admit(
            &scope(),
            admission("replacement", "request", "session", "input", "2").await,
        )
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state.snapshot.run_id, id("run"));
}

#[tokio::test]
async fn lease_expiry_fencing_and_revision_conflicts_are_independent() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), input).await.unwrap();
    let first = store
        .acquire_lease(&scope(), &id("run"), &id("owner-a"), 100, 10)
        .await
        .unwrap();
    assert!(
        store
            .acquire_lease(&scope(), &id("run"), &id("owner-b"), 109, 10)
            .await
            .is_err()
    );
    assert!(
        store
            .renew_lease(&scope(), &id("run"), &first, 110, 10)
            .await
            .is_err()
    );
    let second = store
        .acquire_lease(&scope(), &id("run"), &id("owner-b"), 110, 10)
        .await
        .unwrap();
    assert!(second.fencing_token > first.fencing_token);
    let state = store.load(&scope(), &id("run")).await.unwrap();
    assert!(
        store
            .commit(
                &scope(),
                &id("run"),
                prepared(&state.snapshot, first.clone(), 111)
            )
            .await
            .is_err()
    );
    let update = prepared(&state.snapshot, second.clone(), 111);
    store
        .commit(&scope(), &id("run"), update.clone())
        .await
        .unwrap();
    assert!(store.commit(&scope(), &id("run"), update).await.is_err());
    assert!(
        store
            .renew_lease(&scope(), &id("run"), &first, 111, 20)
            .await
            .is_err()
    );
    let renewed = store
        .renew_lease(&scope(), &id("run"), &second, 119, 20)
        .await
        .unwrap();
    assert_eq!(renewed.fencing_token, second.fencing_token);
    let current = store.load(&scope(), &id("run")).await.unwrap();
    // Heartbeat renews expiry without invalidating the driver's same-generation copy.
    store
        .commit(
            &scope(),
            &id("run"),
            prepared(&current.snapshot, second.clone(), 125),
        )
        .await
        .unwrap();
    store
        .release_lease(&scope(), &id("run"), &renewed, 130)
        .await
        .unwrap();
    let third = store
        .acquire_lease(&scope(), &id("run"), &id("owner-c"), 130, 20)
        .await
        .unwrap();
    assert!(third.fencing_token > renewed.fencing_token);
    let current = store.load(&scope(), &id("run")).await.unwrap();
    let mut forged = third.clone();
    forged.expires_at_ms = i64::MAX;
    assert_eq!(
        store
            .commit(
                &scope(),
                &id("run"),
                prepared(&current.snapshot, forged, 150)
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::LeaseLost
    );
}

#[tokio::test]
async fn an_event_cannot_announce_a_wait_absent_from_the_committed_snapshot() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    let wrong_payload = input.records[0].reference().clone();
    store.admit(&scope(), input).await.unwrap();
    let before = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut update = prepared(&before.snapshot, lease, 101);
    update.snapshot.last_event_seq = 2;
    update.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        2,
        RunEventPayload::RunWaiting {
            wait_ref: wrong_payload,
        },
    ));
    assert_eq!(
        store
            .commit(&scope(), &id("run"), update)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidEvent
    );
    assert_eq!(store.load(&scope(), &id("run")).await.unwrap(), before);
    assert_eq!(
        store
            .read_events(&scope(), &id("run"), 0, 100)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
}

#[tokio::test]
async fn uncertain_tool_effects_keep_the_original_attempt_and_idempotency_key() {
    struct StationaryClock;
    impl Clock for StationaryClock {
        fn now(&self) -> Result<ClockReading, ContractError> {
            Ok(ClockReading {
                utc_ms: 102,
                monotonic_ms: 0,
            })
        }
        fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
            Box::pin(std::future::pending())
        }
    }
    struct AllowPolicy;
    impl PolicyPort for AllowPolicy {
        fn authorize<'a>(
            &'a self,
            _: &'a PolicyRequest,
            _: PolicyContext<'a>,
        ) -> PortFuture<'a, PolicyDecision> {
            Box::pin(async { Ok(PolicyDecision::Allow {}) })
        }
    }
    let store = Arc::new(MemoryStateStore::new());
    let registry = Arc::new(SystemInputRegistry::default());
    let compiled = SchemaCompiler::new().compile(ToolDescriptor {
        tool: VersionedRef { id: id("tool"), version: id("1") }, name: id("tool"), description: "Write a record".into(),
        input_schema: json!({"type":"object","properties":{},"required":[],"additionalProperties":false}), agent_parameters: vec![], system_bindings: None,
        output_schema: json!(true), side_effect: ToolSideEffect::Write, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: true, max_output_bytes: 1024.try_into().unwrap(),
    }, &registry).unwrap();
    let mut input = admission("run", "request", "session", "input", "1").await;
    let mut profile = input.snapshot.profile.profile().clone();
    profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
        tool_id: id("tool"),
        version: id("1"),
        bindings: None,
        config: None,
    }));
    input.snapshot.profile = ProfileValidator::new(&Catalog { revision: "1" })
        .validate(&profile, &scope())
        .await
        .unwrap();
    input.snapshot.request_digest =
        admission_digest(&input.snapshot.request, &input.snapshot.profile, None);
    if let RunEventPayload::RunStarted { profile_digest, .. } = &mut input.events[0].payload {
        *profile_digest = input.snapshot.profile.profile_digest().clone();
    }
    store.admit(&scope(), input).await.unwrap();
    let before = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut plan = prepared(&before.snapshot, lease.clone(), 101);
    let call = ToolCall {
        call_id: id("call"),
        model_request_id: id("model-request"),
        provider_call_id: id("provider-call"),
        tool_name: id("tool"),
        model_inputs: Default::default(),
        descriptor_digest: compiled.descriptor_digest().clone(),
        bound_input_ref: None,
    };
    let call_record =
        ProtectedRecord::new(id("planned-call"), 1, serde_json::to_value(&call).unwrap());
    plan.snapshot.phase = RunPhase::Tool;
    plan.snapshot.last_event_seq = 2;
    plan.snapshot.tool_ledger.push(ToolLedgerEntry {
        call,
        state: ToolCallState::Planned {},
    });
    plan.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        2,
        RunEventPayload::ToolPlanned {
            call_ref: call_record.reference().clone(),
        },
    ));
    plan.records.push(call_record);
    store.commit(&scope(), &id("run"), plan).await.unwrap();
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope(),
            principal_ref: id("caller"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    );
    let budget = RunBudget::attach(
        store.clone(),
        Arc::new(StationaryClock),
        Arc::new(RandomIdSource),
        scope(),
        id("run"),
        lease.clone(),
        context.cancellation.clone(),
    )
    .await
    .unwrap();
    let binder = InputBinder::new(
        registry,
        None,
        Arc::new(
            PolicyGate::new(Arc::new(AllowPolicy), std::time::Duration::from_secs(1)).unwrap(),
        ),
        Arc::new(RandomIdSource),
    );
    binder
        .bind(&compiled, &id("call"), &context, &budget)
        .await
        .unwrap();
    let reservation = budget
        .reserve(ReservationKind::Tool {
            call_id: id("call"),
        })
        .await
        .unwrap();
    let planned = store.load(&scope(), &id("run")).await.unwrap();
    let mut dispatch = prepared(&planned.snapshot, lease.clone(), 102);
    dispatch.snapshot.phase = RunPhase::Tool;
    dispatch.snapshot.tool_ledger[0].state = ToolCallState::Dispatching {
        attempt_id: reservation.attempt_id.clone(),
        idempotency_key: id("effect-key"),
    };
    let dispatched = store.commit(&scope(), &id("run"), dispatch).await.unwrap();
    let mut lost = prepared(&dispatched.snapshot, lease.clone(), 103);
    lost.snapshot.phase = RunPhase::Tool;
    lost.snapshot.tool_ledger[0].state = ToolCallState::Unknown {
        attempt_id: id("attempt-b"),
        idempotency_key: id("different-key"),
    };
    assert_eq!(
        store
            .commit(&scope(), &id("run"), lost)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidTransition
    );
    let mut lost = prepared(&dispatched.snapshot, lease, 103);
    lost.snapshot.phase = RunPhase::Tool;
    lost.snapshot.tool_ledger[0].state = ToolCallState::Unknown {
        attempt_id: reservation.attempt_id.clone(),
        idempotency_key: id("effect-key"),
    };
    let saved = store.commit(&scope(), &id("run"), lost).await.unwrap();
    assert!(
        matches!(&saved.snapshot.tool_ledger[0].state, ToolCallState::Unknown { attempt_id, idempotency_key } if attempt_id == &reservation.attempt_id && idempotency_key == &id("effect-key"))
    );
}

#[tokio::test]
async fn invalid_multi_event_commit_does_not_partially_publish_records_state_or_messages() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), input).await.unwrap();
    let before = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut update = prepared(&before.snapshot, lease, 101);
    let wait = WaitState {
        wait_id: id("new-wait"),
        target: WaitTarget::Input {
            request: InputRequest {
                input_request_id: id("input-request"),
                call_id: id("input-call"),
                question: "Choose a source".into(),
                schema_ref: None,
            },
        },
        expires_at_ms: None,
    };
    let record = ProtectedRecord::new(id("new-record"), 1, serde_json::to_value(&wait).unwrap());
    let reference = record.reference().clone();
    update.records.push(record);
    update.events = vec![
        event(
            &id("run"),
            &id("session"),
            &scope(),
            2,
            RunEventPayload::RunWaiting {
                wait_ref: reference.clone(),
            },
        ),
        event(
            &id("run"),
            &id("session"),
            &scope(),
            2,
            RunEventPayload::RunWaiting {
                wait_ref: reference.clone(),
            },
        ),
    ];
    update.snapshot.last_event_seq = 2;
    update.snapshot.status = RunStatus::Waiting;
    update.snapshot.phase = RunPhase::Waiting;
    update.snapshot.wait = Some(wait);
    let mut message = before.messages[0].clone();
    message.message_id = id("new-message");
    message.sequence = 2.try_into().unwrap();
    update.messages.push(message);
    assert!(store.commit(&scope(), &id("run"), update).await.is_err());
    let after = store.load(&scope(), &id("run")).await.unwrap();
    assert_eq!(after.snapshot, before.snapshot);
    assert_eq!(after.messages, before.messages);
    assert_eq!(after.session, before.session);
    assert!(store.read_record(&scope(), &reference).await.is_err());
    assert_eq!(
        store
            .read_events(&scope(), &id("run"), 0, 100)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
}

#[tokio::test]
async fn commits_cannot_replace_request_or_resolved_profile_and_reads_return_owned_snapshots() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), input).await.unwrap();
    let before = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut update = prepared(&before.snapshot, lease.clone(), 101);
    update.snapshot.request.input = vec![InputContent::Text {
        text: "replacement".into(),
    }];
    update.snapshot.request_digest =
        admission_digest(&update.snapshot.request, &update.snapshot.profile, None);
    assert!(store.commit(&scope(), &id("run"), update).await.is_err());
    let replacement = admission("run", "request", "session", "input", "2").await;
    let mut update = prepared(&before.snapshot, lease, 101);
    update.snapshot.profile = replacement.snapshot.profile;
    assert!(store.commit(&scope(), &id("run"), update).await.is_err());
    let mut copy = store.load(&scope(), &id("run")).await.unwrap();
    copy.messages.clear();
    copy.snapshot.request.input.clear();
    let after = store.load(&scope(), &id("run")).await.unwrap();
    assert_eq!(after.snapshot, before.snapshot);
    assert_eq!(after.messages, before.messages);
}

#[tokio::test]
async fn every_store_surface_is_scoped_and_memory_does_not_claim_durability() {
    let store = MemoryStateStore::new();
    let capabilities = store.capabilities();
    assert!(
        !capabilities.durable && !capabilities.cross_process_leases && capabilities.event_replay
    );
    let mut durable = admission(
        "durable",
        "durable-request",
        "durable-session",
        "input",
        "1",
    )
    .await;
    durable.require_durable = true;
    assert!(store.admit(&scope(), durable).await.is_err());
    let input = admission("run", "request", "session", "input", "1").await;
    let record = input.records[0].reference().clone();
    store.admit(&scope(), input).await.unwrap();
    let snapshot = store.load(&scope(), &id("run")).await.unwrap().snapshot;
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    for foreign in [
        Scope {
            tenant_id: id("other"),
            ..scope()
        },
        Scope {
            workspace_id: id("other"),
            ..scope()
        },
        Scope {
            user_id: Some(id("other")),
            ..scope()
        },
    ] {
        assert!(store.load(&foreign, &id("run")).await.is_err());
        assert!(store.load_session(&foreign, &id("session")).await.is_err());
        assert!(
            store
                .read_events(&foreign, &id("run"), 0, 100)
                .await
                .is_err()
        );
        assert!(store.read_record(&foreign, &record).await.is_err());
        assert!(
            store
                .acquire_lease(&foreign, &id("run"), &id("owner"), 101, 10)
                .await
                .is_err()
        );
        assert!(
            store
                .renew_lease(&foreign, &id("run"), &lease, 101, 10)
                .await
                .is_err()
        );
        assert!(
            store
                .commit(
                    &foreign,
                    &id("run"),
                    prepared(&snapshot, lease.clone(), 101)
                )
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn event_pages_are_exclusive_ordered_replayable_and_preserved_after_completion() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), input).await.unwrap();
    let snapshot = store.load(&scope(), &id("run")).await.unwrap().snapshot;
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    store
        .commit(&scope(), &id("run"), finished(&snapshot, lease, 101))
        .await
        .unwrap();
    let first = store.read_events(&scope(), &id("run"), 0, 1).await.unwrap();
    assert_eq!(first.events.len(), 1);
    assert!(first.has_more);
    assert_eq!(first.next_after_seq, 1);
    let second = store
        .read_events(&scope(), &id("run"), first.next_after_seq, 1)
        .await
        .unwrap();
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.events[0].seq.get(), 2);
    assert!(!second.has_more);
    assert_eq!(
        store
            .read_events(&scope(), &id("run"), 1, 100)
            .await
            .unwrap()
            .events,
        second.events
    );
    assert!(
        store
            .read_events(&scope(), &id("run"), 2, 100)
            .await
            .unwrap()
            .events
            .is_empty()
    );
    assert!(
        store
            .acquire_lease(&scope(), &id("run"), &id("owner"), 102, 100)
            .await
            .is_err()
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

## `crates/wickle/tests/tool_schema.rs`

```rust
//! Explicit tool input ownership, schema projection, and pinned compilation.
use futures_util::stream;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use wickle::*;

const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
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
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn descriptor() -> ToolDescriptor {
    ToolDescriptor {
        tool: reference("search"),
        name: id("search"),
        description: "Search records".into(),
        input_schema: json!({
            "type":"object",
            "properties":{
                "query":{"type":"string","minLength":1},
                "limit":{"type":"integer","minimum":1,"default":10},
                "workspace_id":{"type":"string","format":"uuid"}
            },
            "required":["query","workspace_id"],
            "additionalProperties":false
        }),
        agent_parameters: vec!["query".into(), "limit".into()],
        system_bindings: None,
        output_schema: json!({"type":"array","items":{"type":"string"}}),
        side_effect: ToolSideEffect::ReadOnly,
        concurrency: ToolConcurrency::Serial,
        retry: ToolRetryPolicy::Never,
        reconcile: false,
        max_output_bytes: 4096.try_into().unwrap(),
    }
}

fn definition(key: &str, value_schema: Value) -> SystemInputDefinition {
    SystemInputDefinition {
        key: id(key),
        version: id("definition-1"),
        value_schema,
        source: SystemInputSource::Run {},
    }
}

fn registry() -> SystemInputRegistry {
    SystemInputRegistry::new(vec![definition(
        "workspace_id",
        json!({"type":"string","format":"uuid"}),
    )])
    .unwrap()
}

#[test]
fn projection_validates_model_and_execution_inputs_at_separate_boundaries() {
    let compiled = SchemaCompiler::new()
        .compile(descriptor(), &registry())
        .unwrap();
    assert_eq!(
        compiled.model_input_schema(),
        &json!({
            "type":"object",
            "properties":{
                "query":{"type":"string","minLength":1},
                "limit":{"type":"integer","minimum":1,"default":10}
            },
            "required":["query"],
            "additionalProperties":false
        })
    );
    let model_inputs = object(json!({"query":"recent results"}));
    compiled.validate_model_inputs(&model_inputs).unwrap();
    assert!(compiled.validate_execution_inputs(&model_inputs).is_err());
    let complete = object(json!({"query":"recent results","workspace_id":WORKSPACE}));
    compiled.validate_execution_inputs(&complete).unwrap();
    assert!(compiled.validate_model_inputs(&complete).is_err());
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"x","limit":0})))
            .is_err()
    );
    assert!(
        compiled
            .validate_execution_inputs(&object(json!({"query":"x","workspace_id":"not-a-uuid"})))
            .is_err()
    );
    assert!(
        compiled
            .validate_execution_inputs(&object(
                json!({"query":"x","workspace_id":WORKSPACE,"user_id":"extra"})
            ))
            .is_err()
    );
}

#[test]
fn agent_allowlist_must_be_present_explicit_unique_and_known() {
    for value in [None, Some(Value::Null)] {
        let mut encoded = serde_json::to_value(descriptor()).unwrap();
        if let Some(value) = value {
            encoded["agent_parameters"] = value;
        } else {
            encoded.as_object_mut().unwrap().remove("agent_parameters");
        }
        assert!(ToolDescriptor::from_json(&encoded.to_string()).is_err());
    }
    for parameters in [vec!["query", "query"], vec!["query", "unknown"]] {
        let mut tool = descriptor();
        tool.agent_parameters = parameters.into_iter().map(str::to_owned).collect();
        assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());
    }
}

#[test]
fn empty_and_complete_allowlists_have_explicit_input_semantics() {
    let all_system = SystemInputRegistry::new(vec![
        definition("query", json!({"type":"string"})),
        definition("limit", json!({"type":"integer"})),
        definition("workspace_id", json!({"type":"string","format":"uuid"})),
    ])
    .unwrap();
    let mut hidden = descriptor();
    hidden.agent_parameters.clear();
    let compiled = SchemaCompiler::new().compile(hidden, &all_system).unwrap();
    assert_eq!(
        compiled.model_input_schema(),
        &json!({
            "type":"object","properties":{},"required":[],"additionalProperties":false
        })
    );
    compiled.validate_model_inputs(&JsonObject::new()).unwrap();
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"x"})))
            .is_err()
    );
    assert_eq!(compiled.system_bindings().len(), 3);

    let mut visible = descriptor();
    visible.agent_parameters.push("workspace_id".into());
    let empty_registry = SystemInputRegistry::new(vec![]).unwrap();
    let compiled = SchemaCompiler::new()
        .compile(visible, &empty_registry)
        .unwrap();
    assert!(compiled.system_bindings().is_empty());
    compiled
        .validate_model_inputs(&object(json!({"query":"x","workspace_id":WORKSPACE})))
        .unwrap();
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"x"})))
            .is_err()
    );
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"x","workspace_id":"invalid"})))
            .is_err()
    );
}

#[test]
fn aliases_resolve_registered_metadata_and_cannot_reassign_model_owned_parameters() {
    let registry = SystemInputRegistry::new(vec![definition(
        "active_workspace",
        json!({"type":"string","format":"uuid"}),
    )])
    .unwrap();
    let mut aliased = descriptor();
    aliased.system_bindings = Some(BTreeMap::from([(
        "workspace_id".into(),
        id("active_workspace"),
    )]));
    let compiled = SchemaCompiler::new().compile(aliased, &registry).unwrap();
    assert_eq!(
        compiled.system_bindings()["workspace_id"].key,
        id("active_workspace")
    );
    assert_eq!(
        compiled.system_bindings()["workspace_id"].version,
        id("definition-1")
    );

    for (parameter, key) in [
        ("query", "active_workspace"),
        ("missing", "active_workspace"),
        ("workspace_id", "unregistered"),
    ] {
        let mut invalid = descriptor();
        invalid.system_bindings = Some(BTreeMap::from([(parameter.into(), id(key))]));
        assert!(SchemaCompiler::new().compile(invalid, &registry).is_err());
    }
    // A registered definition is required even before runtime values are supplied.
    assert!(
        SchemaCompiler::new()
            .compile(descriptor(), &SystemInputRegistry::new(vec![]).unwrap())
            .is_err()
    );
}

#[test]
fn one_system_key_cannot_have_competing_definitions_or_supply_sources() {
    let first = definition("workspace_id", json!({"type":"string"}));
    assert!(SystemInputRegistry::new(vec![first.clone(), first.clone()]).is_err());
    let mut other_version = first.clone();
    other_version.version = id("definition-2");
    assert!(SystemInputRegistry::new(vec![first.clone(), other_version]).is_err());
    let mut resolver = first.clone();
    resolver.source = SystemInputSource::Resolver {
        resolver_ref: reference("current_workspace"),
    };
    assert!(SystemInputRegistry::new(vec![first, resolver]).is_err());
}

#[test]
fn root_annotations_and_hidden_definitions_are_excluded_while_selected_constraints_survive() {
    let mut tool = descriptor();
    tool.input_schema["description"] = json!("Internal execution values");
    tool.input_schema["default"] = json!({"query":"default","workspace_id":WORKSPACE});
    tool.input_schema["examples"] = json!([{"query":"example","workspace_id":WORKSPACE}]);
    tool.input_schema["properties"]["workspace_id"]["default"] = json!(WORKSPACE);
    tool.input_schema["properties"]["workspace_id"]["examples"] = json!([WORKSPACE]);
    tool.input_schema["properties"]["query"] = json!({"$ref":"#/$defs/Query"});
    tool.input_schema["$defs"] = json!({
        "Query":{"$ref":"#/$defs/ShortText","description":"A selected query"},
        "ShortText":{"type":"string","minLength":2,"examples":["alpha"]},
        "Hidden":{"type":"string","default":WORKSPACE},
        "Unused":{"type":"object","examples":[{"workspace_id":WORKSPACE}]}
    });
    let compiled = SchemaCompiler::new().compile(tool, &registry()).unwrap();
    let projected = compiled.model_input_schema();
    assert!(projected.get("description").is_none());
    assert!(projected.get("default").is_none());
    assert!(projected.get("examples").is_none());
    assert!(projected["properties"].get("workspace_id").is_none());
    assert_eq!(
        projected["$defs"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        ["Query".to_owned(), "ShortText".to_owned()]
            .into_iter()
            .collect()
    );
    assert_eq!(
        projected["$defs"]["Query"]["description"],
        json!("A selected query")
    );
    assert_eq!(
        projected["$defs"]["ShortText"]["examples"],
        json!(["alpha"])
    );
    assert_eq!(projected["properties"]["limit"]["default"], json!(10));
    compiled
        .validate_model_inputs(&object(json!({"query":"alpha"})))
        .unwrap();
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"a"})))
            .is_err()
    );
}

#[test]
fn local_definitions_shared_with_hidden_properties_remain_when_the_model_reaches_them() {
    let mut tool = descriptor();
    tool.input_schema["properties"]["query"] = json!({"$ref":"#/definitions/Identifier"});
    tool.input_schema["properties"]["workspace_id"] = json!({"$ref":"#/definitions/Identifier"});
    tool.input_schema["definitions"] = json!({"Identifier":{"type":"string","format":"uuid"}});
    let compiled = SchemaCompiler::new().compile(tool, &registry()).unwrap();
    compiled
        .validate_model_inputs(&object(json!({"query":WORKSPACE})))
        .unwrap();
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"not-an-id"})))
            .is_err()
    );
    assert!(
        compiled.model_input_schema()["properties"]
            .get("workspace_id")
            .is_none()
    );
    assert_eq!(
        compiled.model_input_schema()["definitions"]["Identifier"]["format"],
        json!("uuid")
    );
}

#[test]
fn selected_annotation_data_does_not_create_schema_reference_edges() {
    let annotation = json!({"$ref":"https://schemas.example.invalid/data-not-schema"});
    let mut tool = descriptor();
    tool.input_schema["properties"]["query"] = json!({
        "type":"object", "properties":{"$ref":{"type":"string"}},
        "required":["$ref"], "additionalProperties":false,
        "default":annotation, "examples":[annotation]
    });
    let compiled = SchemaCompiler::new().compile(tool, &registry()).unwrap();
    compiled
        .validate_model_inputs(&object(json!({"query":annotation})))
        .unwrap();
    assert_eq!(
        compiled.model_input_schema()["properties"]["query"]["default"],
        annotation
    );
    assert_eq!(
        compiled.model_input_schema()["properties"]["query"]["examples"],
        json!([annotation])
    );
}

#[test]
fn unsupported_reference_forms_fail_instead_of_being_silently_projected() {
    for reference in [
        "#/properties/workspace_id",
        "#/$defs/Missing",
        "https://schemas.example.invalid/input.json",
        "file:///not-a-schema.json",
    ] {
        let mut tool = descriptor();
        tool.input_schema["properties"]["query"] = json!({"$ref":reference});
        assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());
    }
    let mut nested_pointer = descriptor();
    nested_pointer.input_schema["properties"]["query"] =
        json!({"$ref":"#/$defs/Query/properties/nested"});
    nested_pointer.input_schema["$defs"] = json!({
        "Query":{"type":"object","properties":{"nested":{"type":"string"}}}
    });
    assert!(
        SchemaCompiler::new()
            .compile(nested_pointer, &registry())
            .is_err()
    );
    for schema in [
        json!({"$dynamicRef":"#node"}),
        json!({"type":"string","$anchor":"node"}),
        json!({"type":"string","$id":"https://schemas.example.invalid/local"}),
        json!({"type":"object","$defs":{"Nested":{"type":"string"}},"properties":{}}),
    ] {
        let mut tool = descriptor();
        tool.input_schema["properties"]["query"] = schema;
        assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());
    }
}

#[test]
fn mixed_sources_reject_cross_parameter_conditions_but_all_agent_conditions_are_preserved() {
    for (keyword, condition) in [
        ("allOf", json!([{"required":["workspace_id"]}])),
        (
            "anyOf",
            json!([{"required":["workspace_id"]},{"required":["query"]}]),
        ),
        (
            "oneOf",
            json!([{"required":["workspace_id"]},{"required":["limit"]}]),
        ),
        ("dependentRequired", json!({"query":["workspace_id"]})),
        ("minProperties", json!(2)),
        ("patternProperties", json!({"^private_":{"type":"string"}})),
    ] {
        let mut tool = descriptor();
        tool.input_schema[keyword] = condition;
        assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());
    }
    let mut conditional = descriptor();
    conditional.input_schema["if"] = json!({"properties":{"query":{"const":"strict"}}});
    conditional.input_schema["then"] = json!({"$ref":"#/$defs/NeedsLimit"});
    conditional.input_schema["$defs"] = json!({"NeedsLimit":{"required":["limit"]}});
    conditional.input_schema["examples"] =
        json!([{"query":"root annotation","workspace_id":WORKSPACE}]);
    conditional.input_schema["default"] = json!({"query":"root default","workspace_id":WORKSPACE});
    assert!(
        SchemaCompiler::new()
            .compile(conditional.clone(), &registry())
            .is_err()
    );
    conditional.agent_parameters.push("workspace_id".into());
    let compiled = SchemaCompiler::new()
        .compile(conditional, &registry())
        .unwrap();
    assert!(compiled.model_input_schema().get("examples").is_none());
    assert!(compiled.model_input_schema().get("default").is_none());
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"strict","workspace_id":WORKSPACE})))
            .is_err()
    );
    compiled
        .validate_model_inputs(&object(
            json!({"query":"strict","limit":2,"workspace_id":WORKSPACE}),
        ))
        .unwrap();
    compiled
        .validate_model_inputs(&object(
            json!({"query":"ordinary","workspace_id":WORKSPACE}),
        ))
        .unwrap();
}

#[test]
fn the_declared_top_level_object_contract_cannot_be_weakened_or_ambiguous() {
    for (keyword, replacement) in [
        ("type", json!("array")),
        ("additionalProperties", json!(true)),
        ("properties", json!([])),
        ("required", json!(["query", "absent"])),
        ("required", json!(["query", "query"])),
    ] {
        let mut tool = descriptor();
        tool.input_schema[keyword] = replacement;
        assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());
    }
    for keyword in ["type", "properties", "required", "additionalProperties"] {
        let mut tool = descriptor();
        tool.input_schema.as_object_mut().unwrap().remove(keyword);
        assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());
    }
}

#[test]
fn nested_objects_are_owned_whole_and_dotted_root_names_are_literal_names() {
    let mut tool = descriptor();
    tool.input_schema["properties"]["query"] = json!({
        "type":"object", "properties":{"workspace_id":{"type":"string"}},
        "required":["workspace_id"], "additionalProperties":false
    });
    let compiled = SchemaCompiler::new()
        .compile(tool.clone(), &registry())
        .unwrap();
    compiled
        .validate_model_inputs(&object(
            json!({"query":{"workspace_id":"model-owned-nested-value"}}),
        ))
        .unwrap();
    tool.agent_parameters = vec!["query.workspace_id".into()];
    assert!(
        SchemaCompiler::new()
            .compile(tool.clone(), &registry())
            .is_err()
    );
    tool.agent_parameters = vec!["query".into(), "limit".into()];
    tool.system_bindings = Some(BTreeMap::from([(
        "query.workspace_id".into(),
        id("workspace_id"),
    )]));
    assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());

    let mut dotted = descriptor();
    dotted.input_schema["properties"]["query.name"] = json!({"type":"string"});
    dotted.agent_parameters.push("query.name".into());
    let compiled = SchemaCompiler::new().compile(dotted, &registry()).unwrap();
    compiled
        .validate_model_inputs(&object(json!({"query":"x","query.name":"literal"})))
        .unwrap();
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"x","name":"not-the-literal-key"})))
            .is_err()
    );
}

#[tokio::test]
async fn compiled_model_tool_rejects_a_model_supplied_hidden_uuid_in_real_response_collection() {
    let compiled = SchemaCompiler::new()
        .compile(descriptor(), &registry())
        .unwrap();
    let route = ResolvedModelRoute {
        binding: reference("model"),
        catalog_revision: id("catalog"),
        routing_policy_revision: id("policy"),
        requested_model: id("model"),
        model_id: id("model"),
        model_version: id("release"),
        version_semantics: VersionSemantics::Pinned,
        provider: id("scripted"),
        target: JsonObject::new(),
        deployment_revision: None,
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        adapter: reference("adapter"),
        capability_revision: id("capabilities"),
        connection_ref: reference("connection"),
    };
    let request = ModelRequest {
        request_id: id("request"),
        purpose: ModelPurpose::Agent,
        route,
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Search records".into(),
            }],
        }],
        tools: vec![compiled.to_model_tool()],
        output: ModelOutput::Text {},
        max_output_tokens: 128.try_into().unwrap(),
        options: JsonObject::new(),
        limits: ModelResponseLimits {
            max_input_bytes: 16384,
            max_response_bytes: 4096,
            max_delta_bytes: 1024,
            max_events: 8,
            max_tool_calls: 2,
        },
    };
    let events = vec![
        ModelEvent::ToolArgumentsDelta {
            index: 0,
            provider_call_id: Some("hidden".into()),
            name: Some("search".into()),
            delta: json!({"query":"x","workspace_id":WORKSPACE}).to_string(),
        },
        ModelEvent::ToolArgumentsDelta {
            index: 1,
            provider_call_id: Some("model-only".into()),
            name: Some("search".into()),
            delta: json!({"query":"x"}).to_string(),
        },
        ModelEvent::ResponseCompleted {
            finish: ModelFinish::ToolCalls,
            metadata: ModelResponseMetadata::default(),
            continuation: vec![],
        },
    ];
    let response =
        collect_model_response(&request, Box::pin(stream::iter(events.into_iter().map(Ok))))
            .await
            .unwrap();
    assert_eq!(
        response.tool_calls[0].validation,
        ToolCallValidation::InvalidArguments
    );
    assert_eq!(response.tool_calls[1].validation, ToolCallValidation::Valid);
    assert_eq!(
        response.tool_calls[0].model_inputs["workspace_id"],
        json!(WORKSPACE)
    );
}

#[test]
fn restored_compilation_rejects_cached_projection_or_definition_changes() {
    let compiler = SchemaCompiler::new();
    let registry = registry();
    let compiled = compiler.compile(descriptor(), &registry).unwrap();
    let encoded = serde_json::to_string(&compiled).unwrap();
    let restored = compiler
        .restore(&encoded, &registry, compiled.digest())
        .unwrap();
    restored
        .validate_model_inputs(&object(json!({"query":"valid"})))
        .unwrap();
    assert!(
        restored
            .validate_model_inputs(&object(json!({"query":"valid","workspace_id":WORKSPACE})))
            .is_err()
    );
    let mut projection = serde_json::to_value(&compiled).unwrap();
    projection["model_input_schema"]["additionalProperties"] = json!(true);
    assert!(
        compiler
            .restore(&projection.to_string(), &registry, compiled.digest())
            .is_err()
    );
    let mut definition = registry.get(&id("workspace_id")).unwrap().clone();
    definition.version = id("definition-2");
    let changed = SystemInputRegistry::new(vec![definition]).unwrap();
    assert!(
        compiler
            .restore(&encoded, &changed, compiled.digest())
            .is_err()
    );
    let mut tool = descriptor();
    tool.input_schema["properties"]["workspace_id"]["description"] =
        json!("Updated hidden contract");
    let changed_tool = compiler.compile(tool, &registry).unwrap();
    assert_eq!(
        changed_tool.model_input_schema(),
        compiled.model_input_schema()
    );
    assert_ne!(
        changed_tool.descriptor_digest(),
        compiled.descriptor_digest()
    );
    assert_ne!(changed_tool.digest(), compiled.digest());
    assert!(
        compiler
            .restore(
                &serde_json::to_string(&changed_tool).unwrap(),
                &registry,
                compiled.digest()
            )
            .is_err()
    );
}

#[test]
fn declared_execution_safety_cannot_mark_writes_as_parallel_or_read_only_retries() {
    let compiler = SchemaCompiler::new();
    let mut tool = descriptor();
    tool.side_effect = ToolSideEffect::Write;
    tool.concurrency = ToolConcurrency::ParallelRead;
    assert!(compiler.compile(tool.clone(), &registry()).is_err());
    tool.concurrency = ToolConcurrency::Serial;
    tool.retry = ToolRetryPolicy::ReadOnly;
    assert!(compiler.compile(tool.clone(), &registry()).is_err());
    tool.retry = ToolRetryPolicy::Idempotent;
    let compiled = compiler.compile(tool, &registry()).unwrap();
    assert_eq!(compiled.descriptor().side_effect, ToolSideEffect::Write);
    assert_eq!(compiled.descriptor().retry, ToolRetryPolicy::Idempotent);
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

## `tests/support/catalog_consumer.rs`

```rust
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use wickle::*;
use wickle_model_router::ImmutableModelCatalog;

fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}

fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}

fn definition(version: &str, features: &[&str]) -> ModelDefinition {
    ModelDefinition {
        model_key: id("example-model"),
        family: id("example-family"),
        provider: id("example-provider"),
        model_id: id("provider-model"),
        model_version: id(version),
        version_semantics: VersionSemantics::Pinned,
        lifecycle: ModelLifecycle::Active,
        capabilities: ModelCapabilities {
            revision: id(version),
            features: features.iter().map(|value| id(value)).collect(),
            options_schema: json!({
                "type":"object", "properties":{}, "additionalProperties":false
            }),
            context_window: 8192.try_into().unwrap(),
            max_output_tokens: 2048.try_into().unwrap(),
        },
        evidence: vec![ModelEvidence {
            source_ref: id("example-provider-manifest"),
            observed_at_ms: 1000,
        }],
    }
}

fn definition_ref(model: &ModelDefinition) -> ModelDefinitionRef {
    ModelDefinitionRef {
        provider: model.provider.clone(),
        model_key: model.model_key.clone(),
        model_version: model.model_version.clone(),
    }
}

fn binding(model: &ModelDefinition, name: &str) -> Result<ModelBinding, ContractError> {
    let mut binding = ModelBinding {
        binding: reference(name),
        model: definition_ref(model),
        requested_model: model.model_id.clone(),
        adapter: reference("example-adapter"),
        connection_ref: reference("example-connection"),
        target: BTreeMap::from([("region".into(), json!("example-region"))]),
        target_schema: json!({
            "type":"object", "properties":{"region":{"const":"example-region"}},
            "required":["region"], "additionalProperties":false
        }),
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("api-contract-1"),
        },
        deployment_revision: Some(id(name)),
        version_semantics: VersionSemantics::Pinned,
        capabilities: model.capabilities.clone(),
        support: ModelSupportStatus::ContractTested,
        evidence: vec![],
    };
    binding.evidence.push(ModelValidationEvidence {
        kind: ModelValidationKind::ContractTest,
        binding_digest: binding.contract_digest(model)?,
        checked_at_ms: 1001,
        evidence_ref: id("example-contract-fixture-result"),
        passed: true,
    });
    Ok(binding)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let first = definition("release-a", &["text"]);
    let second = definition("release-b", &["text", "tool_calling"]);
    let snapshot = ModelCatalogSnapshot {
        revision: id("catalog-1"),
        scope: scope.clone(),
        bindings: vec![
            binding(&first, "deployment-a")?,
            binding(&second, "deployment-b")?,
        ],
        aliases: vec![ModelAlias {
            provider: first.provider.clone(),
            alias: id("preferred-model"),
            target: definition_ref(&first),
        }],
        models: vec![first, second],
    };
    let original = ImmutableModelCatalog::new(snapshot.clone())?;
    let saved = serde_json::to_string(original.snapshot())?;
    let digest = original.digest().clone();
    let catalog: Arc<dyn ModelCatalog> = Arc::new(original);
    let first_binding = catalog
        .get_binding(&scope, &id("catalog-1"), &reference("deployment-a"))
        .await?;
    let second_binding = catalog
        .get_binding(&scope, &id("catalog-1"), &reference("deployment-b"))
        .await?;
    assert_eq!(first_binding.model.model_id, second_binding.model.model_id);
    assert_ne!(
        first_binding.model.model_version,
        second_binding.model.model_version
    );
    let requirements = CatalogRequirements {
        features: BTreeSet::from([id("tool_calling")]),
        options: JsonObject::new(),
        input_tokens: 1024,
        max_output_tokens: 256.try_into().unwrap(),
        version_policy: VersionPolicy::RequirePinned,
        min_support: ModelSupportStatus::ContractTested,
    };
    assert!(first_binding.validate(&requirements).is_err());
    second_binding.validate(&requirements)?;

    let mut next = snapshot;
    next.revision = id("catalog-2");
    next.aliases[0].target = definition_ref(&second_binding.model);
    let newer = ImmutableModelCatalog::new(next)?;
    assert_eq!(
        newer
            .resolve_alias(
                &scope,
                &id("catalog-2"),
                &id("example-provider"),
                &id("preferred-model")
            )
            .await?
            .model_version,
        id("release-b")
    );
    let restored = ImmutableModelCatalog::restore(&saved, &digest)?;
    assert_eq!(
        restored
            .resolve_alias(
                &scope,
                &id("catalog-1"),
                &id("example-provider"),
                &id("preferred-model")
            )
            .await?
            .model_version,
        id("release-a")
    );
    assert!(
        restored
            .get_binding(&scope, &id("catalog-2"), &reference("deployment-a"))
            .await
            .is_err()
    );
    let mut another_scope = scope.clone();
    another_scope.workspace_id = id("another-workspace");
    assert!(
        catalog
            .get_binding(&another_scope, &id("catalog-1"), &reference("deployment-b"))
            .await
            .is_err()
    );
    println!(
        "catalog consumer: two versions coexist; capabilities differ; saved alias survives a newer snapshot; wrong scope and revision rejected; no model calls made"
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

## `tests/support/model_consumer.rs`

```rust
use futures_util::stream;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
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

fn route(provider: &str) -> ResolvedModelRoute {
    ResolvedModelRoute {
        binding: reference(provider),
        catalog_revision: id("catalog-1"),
        routing_policy_revision: id("policy-1"),
        requested_model: id("example-model"),
        model_id: id("example-model"),
        model_version: id("release-1"),
        version_semantics: VersionSemantics::Pinned,
        provider: id(provider),
        target: JsonObject::new(),
        deployment_revision: None,
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        adapter: reference(&format!("{provider}-adapter")),
        capability_revision: id("capabilities-1"),
        connection_ref: reference(&format!("{provider}-connection")),
    }
}

fn request(provider: &str) -> ModelRequest {
    ModelRequest {
        request_id: id(&format!("{provider}-request")),
        purpose: ModelPurpose::Agent,
        route: route(provider),
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Return the available information".into(),
            }],
        }],
        tools: vec![],
        output: ModelOutput::Text {},
        max_output_tokens: 32.try_into().unwrap(),
        options: JsonObject::from([("reasoning_effort".into(), json!("high"))]),
        limits: ModelResponseLimits {
            max_input_bytes: 4096,
            max_response_bytes: 1024,
            max_delta_bytes: 256,
            max_events: 8,
            max_tool_calls: 0,
        },
    }
}

#[derive(Debug, PartialEq)]
struct ObservedCall {
    connection: VersionedRef,
    credential: &'static str,
    opaque_blocks: usize,
    options: JsonObject,
}

// These are synthetic Host-owned credentials. No real accounts or keys are used.
struct FirstModel {
    observed: Arc<Mutex<Vec<ObservedCall>>>,
}
struct SecondModel {
    observed: Arc<Mutex<Vec<ObservedCall>>>,
}

fn binding(provider: &str) -> ModelPortBinding {
    let route = route(provider);
    ModelPortBinding {
        provider: route.provider,
        adapter: route.adapter,
        connection_ref: route.connection_ref,
    }
}

fn observe(observed: &Mutex<Vec<ObservedCall>>, request: &ModelRequest, credential: &'static str) {
    observed.lock().unwrap().push(ObservedCall {
        connection: request.route.connection_ref.clone(),
        credential,
        options: request.options.clone(),
        opaque_blocks: request
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .filter(|content| matches!(content, ModelContent::Opaque { .. }))
            .count(),
    });
}

impl ModelPort for FirstModel {
    fn binding(&self) -> ModelPortBinding {
        binding("first")
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        observe(&self.observed, request, "synthetic-first-credential");
        Box::pin(stream::iter(vec![
            Ok(ModelEvent::TextDelta {
                text: "first response".into(),
            }),
            Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::Stop,
                metadata: ModelResponseMetadata::default(),
                continuation: vec![OpaqueContinuation::new(
                    &request.route,
                    json!({"signature":"first-only"}),
                )],
            }),
        ]))
    }
}

impl ModelPort for SecondModel {
    fn binding(&self) -> ModelPortBinding {
        binding("second")
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        observe(&self.observed, request, "synthetic-second-credential");
        Box::pin(stream::iter(vec![
            Ok(ModelEvent::TextDelta {
                text: "second ".into(),
            }),
            Ok(ModelEvent::TextDelta {
                text: "response".into(),
            }),
            Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::Stop,
                metadata: ModelResponseMetadata::default(),
                continuation: vec![],
            }),
        ]))
    }
}

// Minimal Host dispatch demonstrating the public protocol. It does not implement
// routing policy, retries, budgets, or the agent's model/tool loop.
async fn invoke(
    registry: &BTreeMap<Id, Arc<dyn ModelPort>>,
    request: &ModelRequest,
) -> Result<ModelResponse, Box<dyn std::error::Error>> {
    request.validate()?;
    let port = registry
        .get(&request.route.provider)
        .ok_or_else(|| ContractError::new(ErrorCode::ComponentUnavailable, "provider"))?;
    if !port.binding().matches_route(&request.route) {
        return Err(ContractError::new(ErrorCode::InvalidReference, "connection_binding").into());
    }
    let context = ModelCallContext {
        attempt_id: id(&format!("{}-attempt", request.request_id)),
        run_id: id("run"),
        scope: Scope {
            tenant_id: id("tenant"),
            workspace_id: id("workspace"),
            user_id: None,
        },
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(5),
    };
    Ok(collect_model_response(request, port.generate(request, &context)).await?)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let first_calls = Arc::new(Mutex::new(Vec::new()));
    let second_calls = Arc::new(Mutex::new(Vec::new()));
    let registry: BTreeMap<Id, Arc<dyn ModelPort>> = BTreeMap::from([
        (
            id("first"),
            Arc::new(FirstModel {
                observed: first_calls.clone(),
            }) as Arc<dyn ModelPort>,
        ),
        (
            id("second"),
            Arc::new(SecondModel {
                observed: second_calls.clone(),
            }) as Arc<dyn ModelPort>,
        ),
    ]);
    let first = invoke(&registry, &request("first")).await?;
    assert_eq!(first.text, "first response");
    assert_eq!(first.continuation.len(), 1);
    assert_eq!(first.metadata.reported_model_version, None);

    let mut incompatible = request("second");
    incompatible.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![ModelContent::Opaque {
            continuation: first.continuation[0].clone(),
        }],
    });
    assert!(invoke(&registry, &incompatible).await.is_err());
    assert!(second_calls.lock().unwrap().is_empty());
    incompatible = request("second");
    incompatible.route.connection_ref = route("first").connection_ref;
    assert!(invoke(&registry, &incompatible).await.is_err());
    assert!(second_calls.lock().unwrap().is_empty());

    // A Host may build a new projection from ordinary conversation content when
    // it can preserve the required meaning. Foreign opaque data is not copied.
    let second = invoke(&registry, &request("second")).await?;
    assert_eq!(second.text, "second response");
    assert!(second.continuation.is_empty());
    assert_eq!(
        *first_calls.lock().unwrap(),
        vec![ObservedCall {
            connection: route("first").connection_ref,
            credential: "synthetic-first-credential",
            opaque_blocks: 0,
            options: JsonObject::from([("reasoning_effort".into(), json!("high"))]),
        }]
    );
    assert_eq!(
        *second_calls.lock().unwrap(),
        vec![ObservedCall {
            connection: route("second").connection_ref,
            credential: "synthetic-second-credential",
            opaque_blocks: 0,
            options: JsonObject::from([("reasoning_effort".into(), json!("high"))]),
        }]
    );
    println!(
        "model consumer: two concrete adapters through dyn ModelPort; one call per connection; foreign opaque and credential bindings rejected before dispatch"
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

## `tests/support/tool_schema_consumer.rs`

```rust
use futures_util::stream;
use serde_json::json;
use std::collections::BTreeMap;
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

fn registry(revision: &str) -> Result<SystemInputRegistry, ContractError> {
    SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("active_workspace_id"),
        version: id(revision),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
}

fn model_request(tool: ModelTool) -> ModelRequest {
    ModelRequest {
        request_id: id("model-request"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference("local-model"),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id("example-model"),
            model_id: id("example-model"),
            model_version: id("1"),
            version_semantics: VersionSemantics::Pinned,
            provider: id("example-provider"),
            target: JsonObject::new(),
            deployment_revision: None,
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("1"),
            },
            adapter: reference("example-adapter"),
            capability_revision: id("capabilities"),
            connection_ref: reference("connection"),
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find recent reports".into(),
            }],
        }],
        tools: vec![tool],
        output: ModelOutput::Text {},
        max_output_tokens: 256.try_into().unwrap(),
        options: JsonObject::new(),
        limits: ModelResponseLimits {
            max_input_bytes: 16_384,
            max_response_bytes: 4096,
            max_delta_bytes: 1024,
            max_events: 8,
            max_tool_calls: 1,
        },
    }
}

async fn propose(
    request: &ModelRequest,
    inputs: &JsonObject,
) -> Result<ModelResponse, ModelProtocolError> {
    collect_model_response(
        request,
        Box::pin(stream::iter([
            Ok(ModelEvent::ToolArgumentsDelta {
                index: 0,
                provider_call_id: Some("call".into()),
                name: Some("search_reports".into()),
                delta: serde_json::to_string(inputs).unwrap(),
            }),
            Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::ToolCalls,
                metadata: ModelResponseMetadata::default(),
                continuation: vec![],
            }),
        ])),
    )
    .await
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptor = ToolDescriptor::from_json(
        r##"{
      "tool":{"id":"report-search","version":"1"},
      "name":"search_reports","description":"Search reports in the current workspace",
      "input_schema":{
        "type":"object",
        "properties":{
          "query":{"$ref":"#/$defs/Query"},
          "limit":{"type":"integer","minimum":1,"default":10},
          "workspace_id":{"$ref":"#/$defs/WorkspaceId"}
        },
        "required":["query","workspace_id"],"additionalProperties":false,
        "examples":[{"query":"private example","workspace_id":"f7fba7f5-f8e7-4f44-b885-45d0688e9f33"}],
        "$defs":{
          "Query":{"type":"string","minLength":1},
          "WorkspaceId":{"type":"string","format":"uuid"},
          "Unused":{"type":"string","description":"unrelated internal schema"}
        }
      },
      "agent_parameters":["query","limit"],
      "system_bindings":{"workspace_id":"active_workspace_id"},
      "output_schema":{"type":"array","items":{"type":"string"}},
      "side_effect":"read_only","concurrency":"serial","retry":"never","reconcile":false,
      "max_output_bytes":4096
    }"##,
    )?;
    let registry = registry("1")?;
    let compiler = SchemaCompiler::new();
    let compiled = compiler.compile(descriptor, &registry)?;
    let schema = compiled.model_input_schema();
    assert_eq!(schema["required"], json!(["query"]));
    assert_eq!(schema["additionalProperties"], false);
    assert!(schema["properties"].get("workspace_id").is_none());
    assert!(schema.get("examples").is_none());
    assert!(schema["$defs"].get("WorkspaceId").is_none());
    assert!(schema["$defs"].get("Unused").is_none());
    assert!(schema["$defs"].get("Query").is_some());
    assert_eq!(
        compiled.system_bindings()["workspace_id"].key,
        id("active_workspace_id")
    );
    let model_inputs = BTreeMap::from([("query".into(), json!("recent results"))]);
    compiled.validate_model_inputs(&model_inputs)?;
    // Validation alone does not apply defaults; the binder owns that operation.
    let mut full = model_inputs.clone();
    full.insert(
        "workspace_id".into(),
        json!("f7fba7f5-f8e7-4f44-b885-45d0688e9f33"),
    );
    assert!(compiled.validate_model_inputs(&full).is_err());
    compiled.validate_execution_inputs(&full)?;
    let mut invalid_full = full.clone();
    invalid_full.insert("workspace_id".into(), json!("an-invented-hash"));
    assert!(compiled.validate_execution_inputs(&invalid_full).is_err());
    assert!(compiled.validate_execution_inputs(&model_inputs).is_err());
    let request = model_request(compiled.to_model_tool());
    assert_eq!(
        propose(&request, &model_inputs).await?.tool_calls[0].validation,
        ToolCallValidation::Valid
    );
    assert_eq!(
        propose(&request, &full).await?.tool_calls[0].validation,
        ToolCallValidation::InvalidArguments
    );
    let saved = serde_json::to_string(&compiled)?;
    let restored = compiler.restore(&saved, &registry, compiled.digest())?;
    assert_eq!(restored.digest(), compiled.digest());
    assert_eq!(restored.model_input_schema(), compiled.model_input_schema());
    let changed_registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("active_workspace_id"),
        version: id("2"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])?;
    assert!(
        compiler
            .restore(&saved, &changed_registry, compiled.digest())
            .is_err()
    );
    println!(
        "tool schema consumer: query/limit exposed; hidden schema omitted; hidden input rejected by the model boundary; full UUID schema checked; compiled identity preserved and changed registry rejected"
    );
    Ok(())
}
```
