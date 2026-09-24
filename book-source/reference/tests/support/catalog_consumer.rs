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
            default_options: Default::default(),
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
