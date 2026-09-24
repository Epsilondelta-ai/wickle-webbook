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
        crate::validate_inference_options(options)?;
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
    /// Inference defaults for this exact binding. Final merged values are schema-validated.
    #[serde(default, skip_serializing_if = "JsonObject::is_empty")]
    pub default_options: JsonObject,
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
        let legacy = data_digest(&(
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
        ));
        Ok(if self.default_options.is_empty() {
            legacy
        } else {
            data_digest(&("binding-default-options-v1", legacy, &self.default_options))
        })
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
        crate::validate_inference_options(&self.default_options)?;
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
        let effective =
            crate::merge_model_options(&self.binding.default_options, &requirements.options);
        self.model.capabilities.validate_options(&effective)?;
        self.binding.capabilities.validate_options(&effective)?;
        let output_cap = requirements
            .max_output_tokens
            .min(self.model.capabilities.max_output_tokens)
            .min(self.binding.capabilities.max_output_tokens);
        if requirements
            .input_tokens
            .checked_add(output_cap.get())
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
