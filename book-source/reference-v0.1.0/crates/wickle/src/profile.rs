use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
};

use serde::{Deserialize, Serialize};

use crate::{
    ContractError, ErrorCode, Id, JsonDigest, JsonObject,
    serialization::{data_digest, decode, optional},
};

/// Current profile wire format; independent of the crate version.
pub const PROFILE_SCHEMA_VERSION: &str = "wickle.agent-profile.v1";

/// Supported profile wire versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProfileSchemaVersion {
    /// The first Wickle profile format.
    #[serde(rename = "wickle.agent-profile.v1")]
    V1,
}

/// A reference to an exact, opaque asset or definition version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionedRef {
    /// Registered identifier.
    pub id: Id,
    /// Exact version, without provider-specific parsing.
    pub version: Id,
}

/// Inline instructions or a pinned instruction asset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Instructions {
    /// Literal instruction text, never executed as code.
    Text(InstructionText),
    /// A registered versioned asset.
    Asset(InstructionAsset),
}

/// Literal instruction data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstructionText {
    /// Profile instructions, subordinate to Host policies.
    pub text: String,
}

/// Reference to instruction data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstructionAsset {
    /// Pinned asset reference.
    pub asset_ref: VersionedRef,
}

/// One catalog tool or one selected adapter export; the forms cannot be mixed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ToolBindingRef {
    /// A catalog tool at an exact version.
    Catalog(CatalogToolRef),
    /// A selected adapter export.
    Export(ExportRef),
}

/// Configuration for a registered catalog tool.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogToolRef {
    /// Catalog identifier.
    pub tool_id: Id,
    /// Exact tool version.
    pub version: Id,
    /// Named connector references, never connection credentials.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub bindings: Option<BTreeMap<Id, Id>>,
    /// Nonsecret configuration validated against the registered schema.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub config: Option<JsonObject>,
}

/// An explicitly selected export from a profile adapter binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportRef {
    /// Adapter binding in the same profile.
    pub adapter_binding: Id,
    /// Export identifier in the adapter definition.
    pub export_id: Id,
    /// Optional model-facing name, not an authorization identity.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub alias: Option<Id>,
}

/// A skill and its nonsecret settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillRef {
    /// Catalog skill identifier.
    pub skill_id: Id,
    /// Exact skill version.
    pub version: Id,
    /// Settings checked by the registered skill schema.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub config: Option<JsonObject>,
}

/// A connection identity whose credentials and endpoint are supplied by the Host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorBindingRef {
    /// Name used by tools and adapters in this profile.
    pub binding_id: Id,
    /// Registered connector identifier.
    pub connector_id: Id,
    /// Exact connector contract version.
    pub version: Id,
}

/// A trusted adapter definition selected by data references.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterBindingRef {
    /// Local binding name.
    pub binding_id: Id,
    /// Registered adapter identifier; not a module or executable path.
    pub adapter_id: Id,
    /// Exact adapter version.
    pub version: Id,
    /// Nonsecret adapter settings.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub config: Option<JsonObject>,
    /// Required connection name to profile connector binding mapping.
    pub connections: BTreeMap<Id, Id>,
}

/// Lifecycle positions supported by the core contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookPosition {
    /// Once after admission.
    BeforeRun,
    /// Before a logical model step.
    BeforeModel,
    /// Before binding model-owned tool inputs.
    BeforeTool,
    /// After a tool result is committed.
    AfterTool,
    /// After a terminal outcome is committed.
    AfterRun,
}

/// A catalog hook at a fixed lifecycle position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogHookRef {
    /// Registered hook identifier.
    pub hook_id: Id,
    /// Exact hook version.
    pub version: Id,
    /// Requested position, checked against metadata.
    pub position: HookPosition,
}

/// An explicit hook selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HookRef {
    /// A registered standalone hook.
    Catalog(CatalogHookRef),
    /// An adapter export with its registered position.
    Export(ExportRef),
}

/// A standalone source reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogSourceRef {
    /// Registered source identifier.
    pub source_id: Id,
    /// Exact source version.
    pub version: Id,
}

/// A source from the catalog or an adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ContextSourceRef {
    /// Catalog source.
    Catalog(CatalogSourceRef),
    /// Adapter source export.
    Export(ExportRef),
}

/// Automatic context collection points.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextTrigger {
    /// Once per run.
    #[default]
    RunStart,
    /// Once per logical model step.
    BeforeModel,
}

/// A context source with explicit finite limits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSourceBinding {
    /// Selected source.
    pub source: ContextSourceRef,
    /// Defaults to run_start.
    #[serde(default)]
    pub trigger: ContextTrigger,
    /// Whether unavailability stops the run; defaults to false.
    #[serde(default)]
    pub required: bool,
    /// Maximum call duration in milliseconds.
    pub timeout_ms: NonZeroU64,
    /// Maximum returned items.
    pub max_items: NonZeroU64,
    /// Maximum returned bytes.
    pub max_bytes: NonZeroU64,
    /// Maximum estimated tokens.
    pub max_tokens: NonZeroU64,
}

/// Context strategy selection. Custom strategies require an exact version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextPolicy {
    /// The built-in bounded strategy or a registered strategy identifier.
    pub strategy: Id,
    /// Required for registered custom strategies.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub version: Option<Id>,
    /// Nonsecret strategy configuration.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub config: Option<JsonObject>,
}

/// The condition that makes a candidate a successful outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompletionPolicy {
    /// The model ends its turn; this does not verify external business success.
    TurnEnd {},
    /// An explicitly selected verifier must accept the candidate.
    Verified {
        /// Exact verifier definition.
        verifier_ref: VersionedRef,
    },
}

impl Default for CompletionPolicy {
    fn default() -> Self {
        Self::TurnEnd {}
    }
}

/// Expected final output representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutputContract {
    /// Text output.
    Text {},
    /// Structured output checked against a registered schema asset.
    JsonSchema {
        /// Exact schema asset.
        schema_ref: VersionedRef,
    },
}

/// Finite execution budgets. Zero tool, repair, or recovery attempts prohibit them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunLimits {
    /// All physical model calls, including verification and compaction.
    pub max_model_calls: NonZeroU64,
    /// Physical tool dispatch attempts; zero disables tools.
    pub max_tool_attempts: u64,
    /// Candidate repair attempts; zero disables repair.
    pub max_repair_attempts: u64,
    /// Recovery attempts; zero disables recovery.
    pub max_recovery_attempts: u64,
    /// Wall elapsed time since admission, including waits, in milliseconds.
    pub max_elapsed_ms: NonZeroU64,
}

/// Serializable agent configuration. It cannot contain runtime trait objects.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentProfile {
    /// Profile wire version.
    pub schema_version: ProfileSchemaVersion,
    /// Stable agent identity.
    pub agent_id: Id,
    /// Immutable profile version.
    pub version: Id,
    /// Display name.
    pub name: String,
    /// Work description.
    pub description: String,
    /// Instructions or an exact asset reference.
    pub instructions: Instructions,
    /// Host-registered model binding or routing configuration name.
    pub model_binding: Id,
    /// Selected tools; an explicit empty list is permitted.
    pub tools: Vec<ToolBindingRef>,
    /// Selected skills; an explicit empty list is permitted.
    pub skills: Vec<SkillRef>,
    /// Host connection references, never credentials.
    pub connectors: Vec<ConnectorBindingRef>,
    /// Optional adapter selections. Missing and empty remain distinct data.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub adapters: Option<Vec<AdapterBindingRef>>,
    /// Optional automatic context sources.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub context_sources: Option<Vec<ContextSourceBinding>>,
    /// Optional lifecycle hook selections.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub hooks: Option<Vec<HookRef>>,
    /// Context selection policy.
    pub context_policy: ContextPolicy,
    /// Defaults to turn_end.
    #[serde(default)]
    pub completion_policy: CompletionPolicy,
    /// Final output requirements.
    pub output_contract: OutputContract,
    /// Finite execution limits.
    pub limits: RunLimits,
    /// Namespaced data checked by registered extension schemas.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub extensions: Option<BTreeMap<Id, serde_json::Value>>,
}

impl AgentProfile {
    /// Decode a strict profile and validate its internal references.
    /// External availability and capabilities are checked by ProfileValidator.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        let profile: Self = decode(input, Some(PROFILE_SCHEMA_VERSION))?;
        profile.validate_structure()?;
        Ok(profile)
    }

    /// Digest the complete profile, including versions and optional-field presence.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }

    /// Check profile-local bindings without opening adapters or contacting models.
    pub fn validate_structure(&self) -> Result<(), ContractError> {
        let invalid = |path| ContractError::new(ErrorCode::InvalidReference, path);
        let mut connectors = BTreeSet::new();
        for item in &self.connectors {
            if !connectors.insert(&item.binding_id) {
                return Err(invalid("connectors.binding_id"));
            }
        }
        let mut adapters = BTreeSet::new();
        for item in self.adapters.iter().flatten() {
            if !adapters.insert(&item.binding_id) {
                return Err(invalid("adapters.binding_id"));
            }
            if item.connections.values().any(|id| !connectors.contains(id)) {
                return Err(invalid("adapters.connections"));
            }
        }
        let check_export = |item: &ExportRef| {
            if !adapters.contains(&item.adapter_binding) {
                Err(invalid("adapter_binding"))
            } else {
                Ok(())
            }
        };
        for tool in &self.tools {
            match tool {
                ToolBindingRef::Catalog(item) => {
                    if item
                        .bindings
                        .iter()
                        .flat_map(|v| v.values())
                        .any(|id| !connectors.contains(id))
                    {
                        return Err(invalid("tools.bindings"));
                    }
                }
                ToolBindingRef::Export(item) => check_export(item)?,
            }
        }
        for source in self.context_sources.iter().flatten() {
            if let ContextSourceRef::Export(item) = &source.source {
                check_export(item)?;
            }
        }
        for hook in self.hooks.iter().flatten() {
            if let HookRef::Export(item) = hook {
                check_export(item)?;
            }
        }
        if self.context_policy.strategy.as_str() == "bounded" {
            if self.context_policy.version.is_some()
                || self
                    .context_policy
                    .config
                    .as_ref()
                    .is_some_and(|c| !c.is_empty())
            {
                return Err(invalid("context_policy"));
            }
        } else if self.context_policy.version.is_none() {
            return Err(invalid("context_policy.version"));
        }
        for namespace in self.extensions.iter().flat_map(|items| items.keys()) {
            if !namespace.as_str().contains('.') || namespace.as_str().split('.').any(str::is_empty)
            {
                return Err(invalid("extensions.namespace"));
            }
        }
        Ok(())
    }
}
