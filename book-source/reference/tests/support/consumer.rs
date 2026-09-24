use std::{collections::BTreeSet, error::Error};

use serde_json::json;
use wickle::{
    AgentProfile, ComponentKind, ComponentMetadata, ComponentRef, ContractError, ErrorCode, Id,
    PortFuture, ProfileResolver, ProfileValidator, ResolvedProfile, Scope, canonical_digest,
};

const PROFILE: &str = r#"{
  "schema_version": "wickle.agent-profile.v1",
  "agent_id": "information-assistant", "version": "1.0.0",
  "name": "Information assistant", "description": "Organize information",
  "instructions": {"text": "Use available evidence."},
  "model_binding": "primary", "tools": [], "skills": [], "connectors": [],
  "context_policy": {"strategy": "bounded"}, "output_contract": {"type": "text"},
  "limits": {"max_model_calls": 8, "max_tool_attempts": 0,
    "max_repair_attempts": 0, "max_recovery_attempts": 0, "max_elapsed_ms": 30000}
}"#;

fn id(value: &str) -> Id {
    Id::new(value).expect("sample identifiers are nonblank")
}

// A small Host resolver for an explicitly registered model binding.
// This is metadata resolution, not a model client or an agent driver.
struct HostResolver {
    scope: Scope,
}

impl ProfileResolver for HostResolver {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        scope: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            if scope != &self.scope
                || reference.kind != ComponentKind::ModelBinding
                || reference.id.as_str() != "primary"
                || reference.version.is_some()
            {
                return Err(ContractError::new(
                    ErrorCode::ComponentUnavailable,
                    "model_binding",
                ));
            }
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    kind: ComponentKind::ModelBinding,
                    id: id("primary"),
                    version: Some(id("binding-revision-1")),
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!({"binding": "primary", "revision": 1})),
                config_schema: json!({"type": "object", "additionalProperties": false}),
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
async fn main() -> Result<(), Box<dyn Error>> {
    let paths: Vec<_> = std::env::args().skip(1).collect();
    let input = match paths.first() {
        Some(path) => std::fs::read_to_string(path)?,
        None => PROFILE.to_owned(),
    };
    let profile = AgentProfile::from_json(&input)?;
    let scope = Scope {
        tenant_id: id("example-tenant"),
        workspace_id: id("example-workspace"),
        user_id: None,
    };
    let resolver = HostResolver {
        scope: scope.clone(),
    };
    let resolved = ProfileValidator::new(&resolver)
        .validate(&profile, &scope)
        .await?;
    let stored = serde_json::to_string(&resolved)?;
    let restored: ResolvedProfile = serde_json::from_str(&stored)?;
    restored.ensure_matches(&profile, &scope)?;
    restored.ensure_same_resolution(&resolved)?;
    println!(
        "Validated {} at version {}",
        restored.profile().agent_id,
        restored.profile().version
    );
    println!("Resolved {} component(s)", restored.components().len());
    println!("Profile digest: {}", restored.profile_digest());
    println!("Persisted profile restored and matched");
    if let Some(path) = paths.get(1) {
        let candidate = AgentProfile::from_json(&std::fs::read_to_string(path)?)?;
        restored.ensure_matches(&candidate, &scope)?;
        println!("Candidate matches the pinned profile");
    }
    Ok(())
}
