// Synthetic catalog evidence and capabilities; no provider availability is claimed.
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
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

fn definition(provider: &str, version: &str, wire: &str, features: &[&str]) -> ModelDefinition {
    ModelDefinition {
        model_key: id("example-model"),
        family: id("example-family"),
        provider: id(provider),
        model_id: id(wire),
        model_version: id(version),
        version_semantics: VersionSemantics::Pinned,
        lifecycle: ModelLifecycle::Active,
        capabilities: ModelCapabilities {
            revision: id(version),
            features: features.iter().map(|value| id(value)).collect(),
            options_schema: json!({
                "type":"object", "properties":{"fixture_option":{"const":version}}, "additionalProperties":false
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
        connection_ref: reference(name),
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
    let specs = [
        ("openai", "gpt-6-astra", "gpt-5.6-sol"),
        ("azure-openai", "gpt-6-astra", "gpt-5.6-sol"),
        ("anthropic", "claude-opus-5", "claude-opus-4-8"),
        (
            "aws-bedrock",
            "anthropic.claude-opus-5",
            "anthropic.claude-opus-4-8",
        ),
        ("google-gemini", "gemini-3.8-flash", "gemini-3.7-flash"),
        ("google-vertex", "gemini-3.8-flash", "gemini-3.7-flash"),
        ("xai", "grok-4.6", "grok-4.5"),
    ];
    let mut snapshot = ModelCatalogSnapshot {
        revision: id("catalog-1"),
        scope: scope.clone(),
        models: vec![],
        bindings: vec![],
        aliases: vec![],
    };
    for (provider, first, second) in specs {
        let a = definition(provider, "fixture-r1", first, &["text"]);
        let b = definition(provider, "fixture-r2", second, &["text", "tool_calling"]);
        snapshot.aliases.push(ModelAlias {
            provider: id(provider),
            alias: id("preferred"),
            target: definition_ref(&a),
        });
        snapshot
            .bindings
            .push(binding(&a, &format!("{provider}-a"))?);
        snapshot
            .bindings
            .push(binding(&b, &format!("{provider}-b"))?);
        snapshot.models.extend([a, b]);
    }
    let catalog = ImmutableModelCatalog::new(snapshot.clone())?;
    let saved = serde_json::to_string(catalog.snapshot())?;
    let requirements = CatalogRequirements {
        features: BTreeSet::from([id("tool_calling")]),
        options: JsonObject::new(),
        input_tokens: 1024,
        max_output_tokens: 256.try_into().unwrap(),
        version_policy: VersionPolicy::RequirePinned,
        min_support: ModelSupportStatus::ContractTested,
    };
    for (provider, first, second) in specs {
        let a = catalog
            .get_binding(
                &scope,
                &id("catalog-1"),
                &reference(&format!("{provider}-a")),
            )
            .await?;
        let b = catalog
            .get_binding(
                &scope,
                &id("catalog-1"),
                &reference(&format!("{provider}-b")),
            )
            .await?;
        assert_eq!(a.model.provider, id(provider));
        assert_eq!(b.model.provider, id(provider));
        assert_eq!(a.model.model_id, id(first));
        assert_eq!(b.model.model_id, id(second));
        assert_eq!(a.model.model_key, b.model.model_key);
        assert_ne!(a.model.model_version, b.model.model_version);
        assert!(a.validate(&requirements).is_err());
        b.validate(&requirements)?;
        let wrong = JsonObject::from([("fixture_option".into(), json!("fixture-r2"))]);
        assert!(a.binding.capabilities.validate_options(&wrong).is_err());
        b.binding.capabilities.validate_options(&wrong)?;
        let mut live = requirements.clone();
        live.min_support = ModelSupportStatus::LiveVerified;
        assert!(b.validate(&live).is_err());
        let mut upgraded = snapshot.clone();
        upgraded.revision = id("catalog-2");
        upgraded
            .aliases
            .iter_mut()
            .find(|alias| alias.provider == id(provider))
            .unwrap()
            .target = definition_ref(&b.model);
        let new = ImmutableModelCatalog::new(upgraded)?;
        for (other, _, _) in specs {
            let target = new
                .resolve_alias(&scope, &id("catalog-2"), &id(other), &id("preferred"))
                .await?;
            assert_eq!(
                target.model_version,
                id(if other == provider {
                    "fixture-r2"
                } else {
                    "fixture-r1"
                })
            );
        }
        let restored = ImmutableModelCatalog::restore(&saved, &catalog.digest())?;
        assert_eq!(
            restored
                .resolve_alias(&scope, &id("catalog-1"), &id(provider), &id("preferred"))
                .await?
                .model_version,
            id("fixture-r1")
        );
        assert!(
            restored
                .get_binding(
                    &scope,
                    &id("catalog-2"),
                    &reference(&format!("{provider}-a"))
                )
                .await
                .is_err()
        );
    }
    let mut forged = snapshot.clone();
    forged.bindings[0].support = ModelSupportStatus::LiveVerified;
    assert!(ImmutableModelCatalog::new(forged).is_err());
    let mut planned = snapshot.clone();
    planned.bindings[1].support = ModelSupportStatus::Planned;
    planned.bindings[1].evidence.clear();
    let planned = ImmutableModelCatalog::new(planned)?;
    assert!(
        planned
            .get_binding(&scope, &id("catalog-1"), &reference("openai-b"))
            .await?
            .validate(&requirements)
            .is_err()
    );
    let mut retired = snapshot.clone();
    retired.models[1].lifecycle = ModelLifecycle::Retired;
    retired.bindings[1] = binding(&retired.models[1], "openai-b")?;
    let retired = ImmutableModelCatalog::new(retired)?;
    assert!(
        retired
            .get_binding(&scope, &id("catalog-1"), &reference("openai-b"))
            .await?
            .validate(&requirements)
            .is_err()
    );
    let mut changed: serde_json::Value = serde_json::from_str(&saved)?;
    changed["bindings"][0]["target"]["region"] = json!("different-region");
    assert!(ImmutableModelCatalog::restore(&changed.to_string(), &catalog.digest()).is_err());
    let mut foreign = scope.clone();
    foreign.workspace_id = id("foreign");
    assert!(
        catalog
            .get_binding(&foreign, &id("catalog-1"), &reference("openai-a"))
            .await
            .is_err()
    );
    println!(
        "version matrix: 7 provider namespaces / 14 coexisting catalog identities; capability differences, scoped alias upgrades, immutable restoration, target drift and planned/live/retired gates passed (synthetic evidence, no provider calls)"
    );
    Ok(())
}
