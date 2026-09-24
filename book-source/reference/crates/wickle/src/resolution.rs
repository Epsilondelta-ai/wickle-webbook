use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::{
    AgentProfile, CompletionPolicy, ContextSourceRef, ContractError, ErrorCode, ExportRef,
    HookPosition, HookRef, Id, Instructions, JsonDigest, JsonObject, OutputContract, PortFuture,
    Scope, ToolBindingRef, VersionedRef,
    serialization::{data_digest, optional},
};

/// Kinds of metadata resolved before execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentKind {
    /// Host model binding or routing configuration.
    ModelBinding,
    /// Standalone tool definition.
    Tool,
    /// Skill manifest.
    Skill,
    /// Connector contract.
    Connector,
    /// Adapter manifest.
    Adapter,
    /// Standalone context source.
    ContextSource,
    /// Lifecycle hook.
    Hook,
    /// Custom context strategy.
    ContextStrategy,
    /// Candidate verifier.
    Verifier,
    /// Pinned instruction data.
    InstructionAsset,
    /// Output schema asset.
    OutputSchema,
    /// Registered namespace and extension data schema.
    Extension,
}

/// A metadata lookup. Only model bindings and extension schemas may omit a version.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentRef {
    /// Expected component kind.
    pub kind: ComponentKind,
    /// Registered identifier.
    pub id: Id,
    /// Exact version or a Host-resolved binding/schema revision.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub version: Option<Id>,
}

/// Export kinds declared by trusted adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportKind {
    /// Callable tool.
    Tool,
    /// Automatic context source.
    ContextSource,
    /// Lifecycle hook.
    Hook,
    /// Host-owned event consumer, not activated by a profile.
    EventConsumer,
}

/// Metadata for one selectable adapter export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportMetadata {
    /// Adapter-local export identifier.
    pub export_id: Id,
    /// Implemented contract kind.
    pub kind: ExportKind,
    /// Metadata contract version; currently 1.
    pub contract_version: u32,
    /// Already-normalized tool name when this export is a tool.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub model_name: Option<Id>,
    /// Fixed position when this export is a hook.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub hook_position: Option<HookPosition>,
    /// Capabilities supplied only when this export is selected.
    pub capabilities: BTreeSet<Id>,
    /// Capabilities required by this selection.
    pub required_capabilities: BTreeSet<Id>,
}

/// Trusted, immutable metadata. It contains no factories, SDK clients, or credentials.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentMetadata {
    /// Resolved kind, identifier, and exact version. The version must be present.
    pub reference: ComponentRef,
    /// Metadata contract version; currently 1.
    pub contract_version: u32,
    /// Registered manifest digest.
    pub manifest_digest: JsonDigest,
    /// Draft 2020-12 schema for nonsecret configuration data.
    pub config_schema: Value,
    /// Other components that must be explicitly selected in the profile.
    pub dependencies: Vec<ComponentRef>,
    /// Capabilities provided by a standalone component.
    pub capabilities: BTreeSet<Id>,
    /// Capabilities that must be provided by the selected components/exports.
    pub required_capabilities: BTreeSet<Id>,
    /// Required names in the tool/adapter connection mapping.
    pub required_connections: BTreeSet<Id>,
    /// Already-normalized name for a catalog tool.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub model_name: Option<Id>,
    /// Position implemented by a catalog hook.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub hook_position: Option<HookPosition>,
    /// Declared adapter exports; declaration alone does not activate them.
    pub exports: Vec<ExportMetadata>,
}

/// Host-injected, scope-aware metadata resolution. No component is opened here.
pub trait ProfileResolver: Send + Sync {
    /// Resolve exactly the requested component, or return a typed error.
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        scope: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata>;
}

/// Frozen reference and definition identity recorded with a resolved profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedComponent {
    /// Exact kind, identity, and version.
    pub reference: ComponentRef,
    /// Hash of the full metadata, including config schema and capabilities.
    pub definition_digest: JsonDigest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolvedProfileData {
    profile: AgentProfile,
    scope: Scope,
    profile_digest: JsonDigest,
    components: Vec<ResolvedComponent>,
    resolution_digest: JsonDigest,
}

/// Immutable, scope-bound profile data, separate from runtime component instances.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ResolvedProfile(ResolvedProfileData);

impl<'de> Deserialize<'de> for ResolvedProfile {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let resolved = Self(ResolvedProfileData::deserialize(deserializer)?);
        resolved
            .validate_snapshot()
            .map_err(serde::de::Error::custom)?;
        Ok(resolved)
    }
}

impl ResolvedProfile {
    /// The pinned profile data.
    pub fn profile(&self) -> &AgentProfile {
        &self.0.profile
    }
    /// The scope used for reference resolution.
    pub fn scope(&self) -> &Scope {
        &self.0.scope
    }
    /// Identity of the original profile data.
    pub fn profile_digest(&self) -> &JsonDigest {
        &self.0.profile_digest
    }
    /// Identity of both the profile and resolved metadata in its scope.
    pub fn resolution_digest(&self) -> &JsonDigest {
        &self.0.resolution_digest
    }
    /// Exact component identities retained for later binding or recovery.
    pub fn components(&self) -> &[ResolvedComponent] {
        &self.0.components
    }

    /// Reject a different facade profile or scope before using an existing run.
    pub fn ensure_matches(
        &self,
        profile: &AgentProfile,
        scope: &Scope,
    ) -> Result<(), ContractError> {
        if profile.digest() != self.0.profile_digest || scope != &self.0.scope {
            return Err(ContractError::new(
                ErrorCode::ProfileMismatch,
                "resolved_profile",
            ));
        }
        Ok(())
    }

    /// Reject a newly resolved version or metadata snapshot replacing the saved one.
    pub fn ensure_same_resolution(&self, other: &Self) -> Result<(), ContractError> {
        if self.0.resolution_digest != other.0.resolution_digest {
            return Err(ContractError::new(
                ErrorCode::ProfileMismatch,
                "resolution_digest",
            ));
        }
        Ok(())
    }

    fn validate_snapshot(&self) -> Result<(), ContractError> {
        self.0.profile.validate_structure()?;
        let unique: BTreeSet<_> = self.0.components.iter().map(|c| &c.reference).collect();
        if self.0.profile.digest() != self.0.profile_digest
            || unique.len() != self.0.components.len()
            || self
                .0
                .components
                .iter()
                .any(|c| c.reference.version.is_none())
            || resolution_digest(&self.0.profile_digest, &self.0.scope, &self.0.components)
                != self.0.resolution_digest
        {
            return Err(ContractError::new(
                ErrorCode::InvalidSnapshot,
                "resolved_profile",
            ));
        }
        Ok(())
    }
}

fn resolution_digest(
    profile: &JsonDigest,
    scope: &Scope,
    components: &[ResolvedComponent],
) -> JsonDigest {
    data_digest(&(profile, scope, components))
}

/// Validates structure, scoped references, schemas, and required capabilities.
/// This runtime service is not serializable and never constructs tool handlers.
pub struct ProfileValidator<'a> {
    resolver: &'a dyn ProfileResolver,
}

impl<'a> ProfileValidator<'a> {
    /// Use a trusted Host metadata resolver.
    pub fn new(resolver: &'a dyn ProfileResolver) -> Self {
        Self { resolver }
    }

    /// Resolve and validate a profile before admission. No model or tool is run.
    pub async fn validate(
        &self,
        profile: &AgentProfile,
        scope: &Scope,
    ) -> Result<ResolvedProfile, ContractError> {
        profile.validate_structure()?;
        let mut checks = Checks::new(self.resolver, scope);
        checks
            .component(
                ComponentKind::ModelBinding,
                &profile.model_binding,
                None,
                None,
                None,
            )
            .await?;
        for item in &profile.connectors {
            checks
                .component(
                    ComponentKind::Connector,
                    &item.connector_id,
                    Some(&item.version),
                    None,
                    None,
                )
                .await?;
        }
        for item in profile.adapters.iter().flatten() {
            let metadata = checks
                .component(
                    ComponentKind::Adapter,
                    &item.adapter_id,
                    Some(&item.version),
                    item.config.as_ref(),
                    Some(&item.connections),
                )
                .await?;
            checks.adapters.insert(item.binding_id.clone(), metadata);
        }
        for item in &profile.tools {
            match item {
                ToolBindingRef::Catalog(item) => {
                    let metadata = checks
                        .component(
                            ComponentKind::Tool,
                            &item.tool_id,
                            Some(&item.version),
                            item.config.as_ref(),
                            item.bindings.as_ref(),
                        )
                        .await?;
                    checks.tool_name(
                        metadata
                            .model_name
                            .as_ref()
                            .ok_or_else(|| invalid("tools.model_name"))?,
                    )?;
                }
                ToolBindingRef::Export(item) => checks.export(item, ExportKind::Tool)?,
            }
        }
        for item in &profile.skills {
            checks
                .component(
                    ComponentKind::Skill,
                    &item.skill_id,
                    Some(&item.version),
                    item.config.as_ref(),
                    None,
                )
                .await?;
        }
        let mut source_slots = BTreeSet::new();
        for item in profile.context_sources.iter().flatten() {
            if !source_slots.insert(data_digest(&(&item.source, item.trigger))) {
                return Err(invalid("context_sources.duplicate"));
            }
            match &item.source {
                ContextSourceRef::Catalog(item) => {
                    checks
                        .component(
                            ComponentKind::ContextSource,
                            &item.source_id,
                            Some(&item.version),
                            None,
                            None,
                        )
                        .await?;
                }
                ContextSourceRef::Export(item) => checks.export(item, ExportKind::ContextSource)?,
            }
        }
        for item in profile.hooks.iter().flatten() {
            match item {
                HookRef::Catalog(item) => {
                    let metadata = checks
                        .component(
                            ComponentKind::Hook,
                            &item.hook_id,
                            Some(&item.version),
                            None,
                            None,
                        )
                        .await?;
                    if metadata.hook_position != Some(item.position) {
                        return Err(invalid("hooks.position"));
                    }
                }
                HookRef::Export(item) => checks.export(item, ExportKind::Hook)?,
            }
        }
        if let Instructions::Asset(item) = &profile.instructions {
            checks
                .asset(ComponentKind::InstructionAsset, &item.asset_ref)
                .await?;
        }
        if let CompletionPolicy::Verified { verifier_ref } = &profile.completion_policy {
            checks.asset(ComponentKind::Verifier, verifier_ref).await?;
        }
        if let OutputContract::JsonSchema { schema_ref } = &profile.output_contract {
            checks
                .asset(ComponentKind::OutputSchema, schema_ref)
                .await?;
        }
        if let Some(version) = &profile.context_policy.version {
            checks
                .component(
                    ComponentKind::ContextStrategy,
                    &profile.context_policy.strategy,
                    Some(version),
                    profile.context_policy.config.as_ref(),
                    None,
                )
                .await?;
        }
        for (namespace, value) in profile.extensions.iter().flatten() {
            let reference = ComponentRef {
                kind: ComponentKind::Extension,
                id: namespace.clone(),
                version: None,
            };
            let metadata = checks.resolve(reference).await?;
            validate_configuration(&metadata.config_schema, value)?;
        }
        checks.finish()?;
        let components: Vec<_> = checks
            .definitions
            .values()
            .map(|definition| ResolvedComponent {
                reference: definition.reference.clone(),
                definition_digest: data_digest(definition),
            })
            .collect();
        let digest = profile.digest();
        Ok(ResolvedProfile(ResolvedProfileData {
            profile: profile.clone(),
            scope: scope.clone(),
            resolution_digest: resolution_digest(&digest, scope, &components),
            profile_digest: digest,
            components,
        }))
    }
}

struct Checks<'a> {
    resolver: &'a dyn ProfileResolver,
    scope: &'a Scope,
    definitions: BTreeMap<ComponentRef, ComponentMetadata>,
    adapters: BTreeMap<Id, ComponentMetadata>,
    capabilities: BTreeSet<Id>,
    required: BTreeSet<Id>,
    names: BTreeSet<Id>,
    selected_exports: BTreeSet<(Id, Id)>,
}

impl<'a> Checks<'a> {
    fn new(resolver: &'a dyn ProfileResolver, scope: &'a Scope) -> Self {
        Self {
            resolver,
            scope,
            definitions: BTreeMap::new(),
            adapters: BTreeMap::new(),
            capabilities: BTreeSet::new(),
            required: BTreeSet::new(),
            names: BTreeSet::new(),
            selected_exports: BTreeSet::new(),
        }
    }

    async fn resolve(
        &mut self,
        requested: ComponentRef,
    ) -> Result<ComponentMetadata, ContractError> {
        if let Some(existing) = self.definitions.get(&requested) {
            return Ok(existing.clone());
        }
        let metadata = self.resolver.resolve(&requested, self.scope).await?;
        if metadata.reference.kind != requested.kind
            || metadata.reference.id != requested.id
            || metadata.reference.version.is_none()
            || requested
                .version
                .as_ref()
                .is_some_and(|version| Some(version) != metadata.reference.version.as_ref())
        {
            return Err(ContractError::new(
                ErrorCode::ComponentUnavailable,
                "component.reference",
            ));
        }
        if metadata.contract_version != 1 {
            return Err(ContractError::new(
                ErrorCode::UnsupportedContractVersion,
                "component.contract_version",
            ));
        }
        let mut export_ids = BTreeSet::new();
        if metadata
            .exports
            .iter()
            .any(|e| !export_ids.insert(&e.export_id))
        {
            return Err(invalid("adapter.exports"));
        }
        self.required
            .extend(metadata.required_capabilities.iter().cloned());
        if requested.kind != ComponentKind::Adapter {
            self.capabilities
                .extend(metadata.capabilities.iter().cloned());
        }
        self.definitions.insert(requested, metadata.clone());
        Ok(metadata)
    }

    async fn component(
        &mut self,
        kind: ComponentKind,
        id: &Id,
        version: Option<&Id>,
        config: Option<&JsonObject>,
        connections: Option<&BTreeMap<Id, Id>>,
    ) -> Result<ComponentMetadata, ContractError> {
        let metadata = self
            .resolve(ComponentRef {
                kind,
                id: id.clone(),
                version: version.cloned(),
            })
            .await?;
        validate_configuration(
            &metadata.config_schema,
            &serde_json::to_value(config.cloned().unwrap_or_default()).expect("JSON object"),
        )?;
        if metadata
            .required_connections
            .iter()
            .any(|key| !connections.is_some_and(|c| c.contains_key(key)))
        {
            return Err(invalid("component.connections"));
        }
        Ok(metadata)
    }

    async fn asset(
        &mut self,
        kind: ComponentKind,
        reference: &VersionedRef,
    ) -> Result<(), ContractError> {
        self.component(kind, &reference.id, Some(&reference.version), None, None)
            .await
            .map(|_| ())
    }

    fn tool_name(&mut self, name: &Id) -> Result<(), ContractError> {
        if !self.names.insert(name.clone()) {
            return Err(invalid("tools.model_name"));
        }
        Ok(())
    }

    fn export(&mut self, reference: &ExportRef, kind: ExportKind) -> Result<(), ContractError> {
        let adapter = self
            .adapters
            .get(&reference.adapter_binding)
            .ok_or_else(|| invalid("adapter_binding"))?;
        let export = adapter
            .exports
            .iter()
            .find(|e| e.export_id == reference.export_id)
            .ok_or_else(|| invalid("export_id"))?
            .clone();
        if export.kind != kind {
            return Err(invalid("export.kind"));
        }
        if export.contract_version != 1 {
            return Err(ContractError::new(
                ErrorCode::UnsupportedContractVersion,
                "export.contract_version",
            ));
        }
        if !self.selected_exports.insert((
            reference.adapter_binding.clone(),
            reference.export_id.clone(),
        )) && kind != ExportKind::ContextSource
        {
            return Err(invalid("export.duplicate"));
        }
        if kind == ExportKind::Tool {
            self.tool_name(
                reference
                    .alias
                    .as_ref()
                    .or(export.model_name.as_ref())
                    .ok_or_else(|| invalid("export.model_name"))?,
            )?;
        } else if reference.alias.is_some() {
            return Err(invalid("export.alias"));
        }
        if kind == ExportKind::Hook && export.hook_position.is_none() {
            return Err(invalid("export.hook_position"));
        }
        self.capabilities.extend(export.capabilities);
        self.required.extend(export.required_capabilities);
        Ok(())
    }

    fn finish(&self) -> Result<(), ContractError> {
        for metadata in self.definitions.values() {
            for dependency in &metadata.dependencies {
                if !self
                    .definitions
                    .values()
                    .any(|d| &d.reference == dependency)
                {
                    return Err(ContractError::new(
                        ErrorCode::ComponentUnavailable,
                        "component.dependencies",
                    ));
                }
            }
        }
        if !self.required.is_subset(&self.capabilities) {
            return Err(ContractError::new(
                ErrorCode::CapabilityUnsupported,
                "component.required_capabilities",
            ));
        }
        Ok(())
    }
}

fn invalid(path: &str) -> ContractError {
    ContractError::new(ErrorCode::InvalidReference, path)
}

fn validate_configuration(schema: &Value, value: &Value) -> Result<(), ContractError> {
    fn check_schema(schema: &Value) -> bool {
        let Some(map) = schema.as_object() else {
            return schema.is_boolean();
        };
        if map.contains_key("$dynamicRef") || map.contains_key("$recursiveRef") {
            return false;
        }
        if map
            .get("$ref")
            .is_some_and(|r| !r.as_str().is_some_and(|s| s.starts_with('#')))
        {
            return false;
        }
        if map
            .get("$schema")
            .is_some_and(|v| v != "https://json-schema.org/draft/2020-12/schema")
        {
            return false;
        }
        for key in [
            "$defs",
            "definitions",
            "properties",
            "patternProperties",
            "dependentSchemas",
        ] {
            if let Some(values) = map.get(key).and_then(Value::as_object) {
                if !values.values().all(check_schema) {
                    return false;
                }
            }
        }
        for key in [
            "items",
            "additionalProperties",
            "unevaluatedProperties",
            "unevaluatedItems",
            "contains",
            "not",
            "if",
            "then",
            "else",
            "propertyNames",
            "contentSchema",
        ] {
            if let Some(value) = map.get(key) {
                if !check_schema(value) {
                    return false;
                }
            }
        }
        for key in ["allOf", "anyOf", "oneOf", "prefixItems"] {
            if let Some(values) = map.get(key).and_then(Value::as_array) {
                if !values.iter().all(check_schema) {
                    return false;
                }
            }
        }
        true
    }
    if !check_schema(schema) {
        return Err(ContractError::new(
            ErrorCode::InvalidSchema,
            "config_schema",
        ));
    }
    let validator = jsonschema::draft202012::options()
        .should_validate_formats(true)
        .build(schema)
        .map_err(|_| ContractError::new(ErrorCode::InvalidSchema, "config_schema"))?;
    if !validator.is_valid(value) {
        return Err(ContractError::new(
            ErrorCode::InvalidConfiguration,
            "config",
        ));
    }
    Ok(())
}
