//! Version-pinned Skill listings and explicit, read-only instruction loading.

use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, fmt, sync::Arc};
use tokio_util::sync::CancellationToken;

pub(crate) mod records;
mod runtime;

/// Complete immutable Skill manifest, without its instruction body.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillDefinition {
    /// Exact Skill identity and version.
    pub skill: VersionedRef,
    /// Short model-visible listing name.
    pub name: String,
    /// Purpose shown before the body is loaded.
    pub description: String,
    /// SHA-256 of the complete original UTF-8 body.
    pub body_hash: Id,
    /// Complete original UTF-8 body size.
    pub body_bytes: u64,
    /// Versioned supporting data; never automatically executed or read as code.
    pub assets: Vec<ArtifactRef>,
    /// Capabilities that must be supplied by actually selected Tools.
    pub required_tool_capabilities: BTreeSet<Id>,
    /// Schema for this Skill's nonsecret profile configuration.
    pub config_schema: Value,
}
impl SkillDefinition {
    /// Immutable identity including body hash, assets and dependency requirements.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
    /// Body-free listing for a pinned prompt.
    pub fn listing(&self) -> SkillManifest {
        SkillManifest {
            skill: self.skill.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            manifest_digest: self.digest(),
        }
    }
    /// Metadata a Host ProfileResolver can return for this exact Skill.
    pub fn metadata(&self) -> ComponentMetadata {
        let mut required = self.required_tool_capabilities.clone();
        required.insert(Id::new("skill_loading").expect("constant capability"));
        ComponentMetadata {
            reference: ComponentRef {
                kind: ComponentKind::Skill,
                id: self.skill.id.clone(),
                version: Some(self.skill.version.clone()),
            },
            contract_version: 1,
            manifest_digest: self.digest(),
            config_schema: self.config_schema.clone(),
            dependencies: vec![],
            capabilities: BTreeSet::new(),
            required_capabilities: required,
            required_connections: BTreeSet::new(),
            model_name: None,
            hook_position: None,
            exports: vec![],
        }
    }
    /// Compute the original byte hash used in a trusted manifest.
    pub fn hash_body(body: &str) -> Result<Id, ContractError> {
        crate::artifacts::content_hash(body.as_bytes())
    }
    fn validate(&self, scope: &Scope, limits: SkillLimits) -> Result<(), ContractError> {
        if self.name.trim().is_empty()
            || self.name.len() > 256
            || self.description.len() > 8192
            || self.body_bytes == 0
            || self.body_bytes > limits.max_body_bytes
            || self.assets.len() > limits.max_assets
            || self.body_hash.as_str().len() != 71
            || !self.body_hash.as_str().starts_with("sha256:")
            || !self.body_hash.as_str()[7..]
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
        {
            return Err(skill_error(ErrorCode::InvalidSkill, "skill.definition"));
        }
        if self.assets.iter().any(|asset| &asset.scope != scope) {
            return Err(skill_error(ErrorCode::AccessDenied, "skill.asset_scope"));
        }
        crate::tool_schema::compile_validator(&self.config_schema)?;
        Ok(())
    }
}
/// Finite instruction loading and active-context bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillLimits {
    /// Maximum complete body size; oversized bodies are not truncated.
    pub max_body_bytes: u64,
    /// Maximum combined bytes of distinct loaded Skill bodies in one Run.
    pub max_total_body_bytes: u64,
    /// Maximum supporting references per Skill.
    pub max_assets: usize,
    /// Maximum read/authorization callback time.
    pub timeout_ms: u64,
}
impl Default for SkillLimits {
    fn default() -> Self {
        Self {
            max_body_bytes: 32 * 1024,
            max_total_body_bytes: 128 * 1024,
            max_assets: 16,
            timeout_ms: 30_000,
        }
    }
}
impl SkillLimits {
    fn validate(self) -> Result<(), ContractError> {
        if self.max_body_bytes == 0
            || self.max_body_bytes > 1024 * 1024
            || self.max_total_body_bytes < self.max_body_bytes
            || self.max_total_body_bytes > 4 * 1024 * 1024
            || self.max_assets > 64
            || self.timeout_ms == 0
            || self.timeout_ms > 86_400_000
        {
            return Err(skill_error(ErrorCode::InvalidConfiguration, "skill.limits"));
        }
        Ok(())
    }
}
/// Current caller identity and finite controls, without the whole Agent state.
#[derive(Debug, Clone)]
pub struct SkillCallContext {
    /// Owning namespace.
    pub scope: Scope,
    /// Current Run.
    pub run_id: Id,
    /// Current authenticated principal.
    pub principal_ref: Id,
    /// Current Host authorization grant.
    pub capability_grant_ref: Id,
    /// Cooperative cancellation.
    pub cancellation: CancellationToken,
    /// Effective finite deadline.
    pub deadline: tokio::time::Instant,
}
/// Read-only Host access to exact Skill versions. Loading never executes scripts.
pub trait SkillResolver: Send + Sync {
    /// Return the complete UTF-8 instruction body for the pinned manifest/configuration.
    fn load<'a>(
        &'a self,
        selection: &'a SkillRef,
        definition: &'a SkillDefinition,
        context: &'a SkillCallContext,
    ) -> PortFuture<'a, String>;
    /// Check current access to the original saved version without replacing its body.
    fn authorize_use<'a>(
        &'a self,
        loaded: &'a LoadedSkill,
        context: &'a SkillCallContext,
    ) -> PortFuture<'a, ()>;
}
/// Host components for one scoped Skill runtime.
pub struct SkillBindings {
    /// Owning namespace.
    pub scope: Scope,
    /// Same store used by the Agent and loader Tool.
    pub state: Arc<dyn StateStore>,
    /// Current Host policy.
    pub policy: Arc<PolicyGate>,
    /// Read-only body and current-access implementation.
    pub resolver: Arc<dyn SkillResolver>,
    /// Required when manifests declare supporting artifacts.
    pub artifacts: Option<Arc<ArtifactRuntime>>,
}
/// One exact selected manifest and its nonsecret configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedSkill {
    /// Original profile reference and configuration.
    pub selection: SkillRef,
    /// Exact instruction and support-asset manifest.
    pub definition: SkillDefinition,
    /// Exact Host metadata attested by the resolved profile.
    pub metadata: ComponentMetadata,
}
/// Immutable Skill assembly pinned with admission and the session listing.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillPlan {
    schema_version: String,
    scope: Scope,
    loader: ToolBindingRef,
    loader_descriptor_digest: JsonDigest,
    skills: Vec<PlannedSkill>,
    tool_capabilities: BTreeSet<Id>,
    tool_metadata: Vec<ComponentMetadata>,
    limits: SkillLimits,
}
impl SkillPlan {
    /// Pinned body-free listings in profile order.
    pub fn listings(&self) -> Vec<SkillManifest> {
        self.skills
            .iter()
            .map(|entry| entry.definition.listing())
            .collect()
    }
    /// Exact selected manifests/configurations.
    pub fn skills(&self) -> &[PlannedSkill] {
        &self.skills
    }
    /// Canonical complete plan identity.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
    /// Exact loader Tool selection, distinct from its visible alias.
    pub fn loader(&self) -> &ToolBindingRef {
        &self.loader
    }
    pub(crate) fn loader_digest(&self) -> &JsonDigest {
        &self.loader_descriptor_digest
    }
    pub(crate) fn max_total_body_bytes(&self) -> u64 {
        self.limits.max_total_body_bytes
    }
    /// Restore the protected plan and validate selection/dependency invariants.
    pub fn restore(
        record: &ProtectedRecord,
        profile: &ResolvedProfile,
    ) -> Result<Self, ContractError> {
        let plan: Self = serde_json::from_value(record.value().clone())
            .map_err(|_| skill_error(ErrorCode::InvalidSnapshot, "skill.plan"))?;
        if plan.digest() != record.reference().digest
            || plan.schema_version != "wickle.skill-plan.v1"
            || &plan.scope != profile.scope()
            || plan
                .skills
                .iter()
                .map(|entry| &entry.selection)
                .collect::<Vec<_>>()
                != profile.profile().skills.iter().collect::<Vec<_>>()
            || !profile.profile().tools.contains(&plan.loader)
        {
            return Err(skill_error(
                ErrorCode::InvalidSnapshot,
                "skill.plan_identity",
            ));
        }
        plan.limits.validate()?;
        if crate::serialization::data_digest(&runtime::loader_descriptor(plan.limits)?)
            != plan.loader_descriptor_digest
        {
            return Err(skill_error(
                ErrorCode::InvalidSnapshot,
                "skill.loader_contract",
            ));
        }
        if runtime::tool_capabilities(profile, &plan.tool_metadata)? != plan.tool_capabilities {
            return Err(skill_error(
                ErrorCode::InvalidSnapshot,
                "skill.tool_capabilities",
            ));
        }
        let mut seen = BTreeSet::new();
        for entry in &plan.skills {
            runtime::attest(profile, &entry.metadata)?;
            if entry.metadata.reference.kind != ComponentKind::Skill
                || entry.metadata.reference.id != entry.definition.skill.id
                || entry.metadata.reference.version.as_ref()
                    != Some(&entry.definition.skill.version)
                || entry.metadata.manifest_digest != entry.definition.digest()
            {
                return Err(skill_error(ErrorCode::InvalidSnapshot, "skill.metadata"));
            }
            entry.definition.validate(&plan.scope, plan.limits)?;
            if entry.selection.skill_id != entry.definition.skill.id
                || entry.selection.version != entry.definition.skill.version
                || !seen.insert(entry.definition.digest())
                || !entry
                    .definition
                    .required_tool_capabilities
                    .is_subset(&plan.tool_capabilities)
                || !crate::tool_schema::compile_validator(&entry.definition.config_schema)?
                    .is_valid(
                        &serde_json::to_value(entry.selection.config.clone().unwrap_or_default())
                            .map_err(|_| skill_error(ErrorCode::InvalidJson, "skill.config"))?,
                    )
            {
                return Err(skill_error(
                    ErrorCode::InvalidSnapshot,
                    "skill.plan_selection",
                ));
            }
        }
        Ok(plan)
    }
}
impl fmt::Debug for SkillPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SkillPlan(<protected>)")
    }
}
/// Complete loaded instructions saved with their original call, never partial previews.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadedSkill {
    schema_version: String,
    scope: Scope,
    run_id: Id,
    call_id: Id,
    selection: SkillRef,
    definition_digest: JsonDigest,
    body: String,
    assets: Vec<ArtifactRef>,
}
impl LoadedSkill {
    /// Exact selected Skill and configuration.
    pub fn selection(&self) -> &SkillRef {
        &self.selection
    }
    /// Complete instruction body, for authorized consumers only.
    pub fn body(&self) -> &str {
        &self.body
    }
    /// Original support assets; reading them is separately authorized.
    pub fn assets(&self) -> &[ArtifactRef] {
        &self.assets
    }
    /// Digest of the manifest used to load these bytes.
    pub fn definition_digest(&self) -> &JsonDigest {
        &self.definition_digest
    }
    pub(crate) fn validate(
        &self,
        plan: &SkillPlan,
        run_id: &Id,
        call_id: &Id,
    ) -> Result<(), ContractError> {
        let entry = plan
            .skills
            .iter()
            .find(|entry| entry.selection == self.selection)
            .ok_or_else(|| skill_error(ErrorCode::InvalidSkill, "skill.selection"))?;
        if self.schema_version != "wickle.loaded-skill.v1"
            || self.scope != plan.scope
            || &self.run_id != run_id
            || &self.call_id != call_id
            || self.definition_digest != entry.definition.digest()
            || self.body.len() as u64 != entry.definition.body_bytes
            || SkillDefinition::hash_body(&self.body)? != entry.definition.body_hash
            || self.assets != entry.definition.assets
        {
            return Err(skill_error(ErrorCode::InvalidSkill, "skill.loaded_body"));
        }
        Ok(())
    }
    pub(crate) fn summary(&self) -> Value {
        json!({"kind":"loaded_skill","skill":self.selection,"definition_digest":self.definition_digest})
    }
    pub(crate) fn matches_input(&self, input: &BoundToolInput) -> bool {
        self.matches_args(input.execution_args())
    }
    pub(crate) fn matches_args(&self, args: &JsonObject) -> bool {
        args.get("skill_id").and_then(Value::as_str) == Some(self.selection.skill_id.as_str())
            && args.get("version").and_then(Value::as_str) == Some(self.selection.version.as_str())
    }
    pub(crate) fn context_item(&self) -> Result<ContextItem, ContractError> {
        let mut content = vec![InputContent::Text {
            text: self.body.clone(),
        }];
        content.extend(
            self.assets
                .iter()
                .cloned()
                .map(|reference| InputContent::Artifact { reference }),
        );
        Ok(ContextItem::new(
            Id::new(format!(
                "skill-{}",
                canonical_digest(&json!([
                    self.run_id,
                    self.selection,
                    self.definition_digest
                ]))
            ))?,
            ContextOrigin::Skill,
            VersionedRef {
                id: self.selection.skill_id.clone(),
                version: self.selection.version.clone(),
            },
            self.scope.clone(),
            content,
            ContextLifetime::Run {
                run_id: self.run_id.clone(),
            },
            ContextPriority::Required,
        ))
    }
}
impl fmt::Debug for LoadedSkill {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LoadedSkill(<protected>)")
    }
}
/// Scoped loader, fixed manifests and current-use authorization for an Agent.
pub struct SkillRuntime {
    bindings: SkillBindings,
    definitions: Vec<SkillDefinition>,
    loader: ToolBindingRef,
    compiled: CompiledTool,
    limits: SkillLimits,
}
fn skill_error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
