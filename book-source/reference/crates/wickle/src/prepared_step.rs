//! Immutable model preparation and the execution contracts advertised with it.
use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, fmt};

/// One model-visible name and its original registered execution contract.
/// Serialize only to protected storage; this includes hidden schema metadata.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedToolSetEntry {
    /// Session-pinned identity, selection/export, and model-only schema.
    pub manifest: PinnedPromptTool,
    /// Protected original compiled input contract, never sent to a model.
    compiled: Value,
}
impl fmt::Debug for ResolvedToolSetEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedToolSetEntry")
            .field("tool", &self.manifest.tool)
            .finish_non_exhaustive()
    }
}
impl ResolvedToolSetEntry {
    /// Pin a trusted binding against the exact session manifest.
    pub fn new(manifest: PinnedPromptTool, tool: &CompiledTool) -> Result<Self, ContractError> {
        if manifest.tool != tool.descriptor().tool
            || manifest.compiled_digest != *tool.digest()
            || manifest.descriptor_digest != *tool.descriptor_digest()
            || manifest.model_schema_digest != *tool.model_schema_digest()
            || manifest.compiler_version != tool.compiler_version()
            || manifest.model_tool != tool.to_model_tool()
        {
            return Err(invalid("prepared.tool_manifest"));
        }
        Ok(Self {
            manifest,
            compiled: serde_json::to_value(tool).map_err(|_| invalid("prepared.tool"))?,
        })
    }
    pub(crate) fn restore_cached(
        &self,
        cache: &mut BTreeMap<String, CompiledTool>,
    ) -> Result<CompiledTool, ContractError> {
        let key = self.manifest.compiled_digest.to_string();
        if let Some(tool) = cache.get(&key) {
            // A digest hit never substitutes for checking the complete saved value.
            if Self::new(self.manifest.clone(), tool)? != *self {
                return Err(invalid("prepared.tool_cache_identity"));
            }
            return Ok(tool.clone());
        }
        let tool = self.restore_tool()?;
        cache.insert(key, tool.clone());
        Ok(tool)
    }
    /// Restore the saved contract, not a current registry replacement.
    pub fn restore_tool(&self) -> Result<CompiledTool, ContractError> {
        let bindings: BTreeMap<String, SystemInputDefinition> = serde_json::from_value(
            self.compiled
                .get("system_bindings")
                .cloned()
                .ok_or_else(|| invalid("prepared.tool_bindings"))?,
        )
        .map_err(|_| invalid("prepared.tool_bindings"))?;
        let mut definitions = BTreeMap::new();
        for definition in bindings.into_values() {
            if definitions
                .insert(definition.key.clone(), definition.clone())
                .is_some_and(|prior| prior != definition)
            {
                return Err(invalid("prepared.tool_bindings"));
            }
        }
        let registry = SystemInputRegistry::new(definitions.into_values().collect())?;
        let tool = SchemaCompiler::new().restore(
            &self.compiled.to_string(),
            &registry,
            &self.manifest.compiled_digest,
        )?;
        Self::new(self.manifest.clone(), &tool)?;
        Ok(tool)
    }
}
/// Protected, ordered mapping between advertised Tools and execution definitions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedToolSet {
    /// Versioned storage format.
    pub schema_version: String,
    /// Exact namespace.
    pub scope: Scope,
    /// Owning execution.
    pub run_id: Id,
    /// Entries retain the approved profile order.
    pub entries: Vec<ResolvedToolSetEntry>,
}
impl ResolvedToolSet {
    /// Validate every cached contract and reject ambiguous model names.
    pub fn validate(&self) -> Result<(), ContractError> {
        self.validate_shape()?;
        for entry in &self.entries {
            entry.restore_tool()?;
        }
        Ok(())
    }
    pub(crate) fn validate_shape(&self) -> Result<(), ContractError> {
        if self.schema_version != "wickle.resolved-tool-set.v1" {
            return Err(invalid("prepared.tool_set_version"));
        }
        let mut names = std::collections::BTreeSet::new();
        for entry in &self.entries {
            if !names.insert(&entry.manifest.model_tool.name) {
                return Err(invalid("prepared.tool_set_names"));
            }
        }
        Ok(())
    }
}
/// Saved selection and access-check evidence, without runtime authorization grants.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionProvenance {
    /// Exact cumulative context revision used when this projection was prepared.
    pub context_revision_ref: Option<RecordRef>,
    /// Immutable conversation boundary used to derive historical dependencies.
    pub through_sequence: u64,
    /// Active source batches used by automatic collection.
    pub source_batches: Vec<RecordRef>,
    /// Dependencies of historical data and summaries.
    pub source_lineage: Vec<ContextLineage>,
    /// Artifacts whose current access must be checked again.
    pub artifacts: Vec<ArtifactRef>,
    /// Exact core-issued source observations, in committed order.
    pub fragments: Vec<ContextFragment>,
    /// Original message identities selected for projection.
    pub selected_message_ids: Vec<Id>,
    /// Original message identities omitted by bounded selection.
    pub dropped_message_ids: Vec<Id>,
    /// Active context identities included in full.
    pub selected_context_ids: Vec<Id>,
    /// Context identities omitted by lifetime or size bounds.
    pub dropped_context_ids: Vec<Id>,
}
/// Protected final model input. A physical retry changes only its attempt ID.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedModelProjection {
    /// Versioned storage format.
    pub schema_version: String,
    /// Exact namespace.
    pub scope: Scope,
    /// Owning Run.
    pub run_id: Id,
    /// Fully compiled provider-facing model request with its logical step ID.
    pub request: ModelRequest,
    /// Estimate of this final compiled projection.
    pub input_tokens: u64,
    /// Frozen selection and current-access dependencies.
    pub provenance: ProjectionProvenance,
}
impl fmt::Debug for PreparedModelProjection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedModelProjection")
            .field("run_id", &self.run_id)
            .field("request_id", &self.request.request_id)
            .finish_non_exhaustive()
    }
}
impl PreparedModelProjection {
    /// Input identity independent of physical or logical request identifiers.
    pub fn fingerprint(&self) -> JsonDigest {
        let mut value = serde_json::to_value(&self.request).expect("model request serialization");
        value
            .as_object_mut()
            .expect("model request object")
            .remove("request_id");
        crate::serialization::data_digest(&value)
    }
}
fn invalid(path: &str) -> ContractError {
    ContractError::new(ErrorCode::InvalidSnapshot, path)
}

pub(crate) fn revision_artifacts(revision: &ContextRevision) -> Vec<ArtifactRef> {
    revision
        .previews
        .iter()
        .map(|preview| preview.preview.reference.clone())
        .chain(revision.anchors.iter().filter_map(|content| match content {
            InputContent::Artifact { reference } => Some(reference.clone()),
            _ => None,
        }))
        .collect()
}
/// Rebuild typed artifact dependencies from the actual wire projection and
/// independently stored Tool/context history, rather than trusting a cached list.
pub(crate) fn projected_artifacts(
    request: &ModelRequest,
    messages: &[Message],
    extra: &[ArtifactRef],
    scope: &Scope,
) -> Result<Vec<ArtifactRef>, ContractError> {
    let mut known: Vec<_> = messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            ContentBlock::ToolResult { result }
            | ContentBlock::ToolResultCorrection { result, .. } => Some(result),
            _ => None,
        })
        .flat_map(|result| &result.content)
        .filter_map(|content| match content {
            InputContent::Artifact { reference } => Some(reference),
            _ => None,
        })
        .collect();
    known.extend(extra.iter());
    let mut selected: Vec<ArtifactRef> = Vec::new();
    for message in &request.messages {
        for content in &message.content {
            let (value, strict) = match content {
                ModelContent::ToolResult { content, .. } => (content, true),
                ModelContent::Json { value }
                    if value["kind"] == "context_data" && value["origin"] == "compaction" =>
                {
                    (value, false)
                }
                _ => continue,
            };
            for item in value
                .get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if item.get("type").and_then(Value::as_str) != Some("artifact") {
                    continue;
                }
                let reference = known.iter().find(|reference| item == &serde_json::json!({
                    "type":"artifact","artifact_id":reference.artifact_id,"media_type":reference.media_type,
                    "size_bytes":reference.size_bytes,"content_hash":reference.content_hash
                }));
                let Some(reference) = reference else {
                    if strict {
                        return Err(ContractError::new(
                            ErrorCode::InvalidArtifact,
                            "prepared.artifact_projection",
                        ));
                    }
                    continue;
                };
                if &reference.scope != scope {
                    return Err(ContractError::new(
                        ErrorCode::AccessDenied,
                        "prepared.artifact_scope",
                    ));
                }
                if !selected.contains(*reference) {
                    selected.push((*reference).clone());
                }
            }
        }
    }
    Ok(selected)
}
