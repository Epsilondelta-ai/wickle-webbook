//! Inference-only option layering, provenance and route-specific output budgets.
use crate::{ContractError, ErrorCode, Id, JsonObject, ModelPurpose, RunSnapshot};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt, num::NonZeroU64};

/// Layer that supplied an effective top-level option value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelOptionSource {
    /// Default of the selected physical binding.
    Binding,
    /// Agent profile override.
    Profile,
    /// Caller override for this Run.
    Run,
    /// Explicit verification/compaction configuration, never inherited from the agent.
    Purpose,
}
/// Validated inference settings pinned with a physical model invocation.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfiguration {
    /// Logical overrides after Profile/Run layering, before binding defaults.
    pub requested: JsonObject,
    /// Validated final options sent to the adapter.
    pub effective: JsonObject,
    /// Origin of each effective top-level key.
    pub sources: BTreeMap<String, ModelOptionSource>,
    /// Exact model-ceiling option schema revision.
    pub model_schema_revision: Id,
    /// Exact selected binding option schema revision.
    pub binding_schema_revision: Id,
    /// Host/Profile/Run upper bound before the selected model ceiling.
    pub requested_max_output_tokens: NonZeroU64,
    /// Actual finite per-call output upper bound.
    pub max_output_tokens: NonZeroU64,
}
impl fmt::Debug for ModelConfiguration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelConfiguration")
            .field("option_count", &self.effective.len())
            .field("model_schema_revision", &self.model_schema_revision)
            .field("binding_schema_revision", &self.binding_schema_revision)
            .field("max_output_tokens", &self.max_output_tokens)
            .finish_non_exhaustive()
    }
}
/// Reject transport, credential and wire-owned controls masquerading as inference options.
/// Provider-specific option schemas must still validate the complete effective map.
pub fn validate_inference_options(options: &JsonObject) -> Result<(), ContractError> {
    for key in options.keys() {
        let normalized: String = key
            .chars()
            .filter(|c| *c != '_' && *c != '-')
            .flat_map(char::to_lowercase)
            .collect();
        if matches!(
            normalized.as_str(),
            "model"
                | "modelid"
                | "deployment"
                | "deploymentid"
                | "deploymentname"
                | "baseurl"
                | "endpoint"
                | "endpointurl"
                | "headers"
                | "authorization"
                | "apikey"
                | "accesstoken"
                | "credential"
                | "credentials"
                | "connection"
                | "connectionref"
                | "tools"
                | "toolchoice"
                | "messages"
                | "input"
                | "system"
                | "systemprompt"
                | "stream"
                | "maxretries"
                | "retry"
                | "retryconfig"
                | "retrypolicy"
                | "timeout"
                | "timeoutms"
                | "maxoutputtokens"
                | "maxtokens"
                | "maxcompletiontokens"
                | "apiversion"
                | "extrabody"
                | "extraheaders"
                | "transport"
                | "httpclient"
                | "region"
                | "location"
                | "project"
        ) {
            return Err(ContractError::new(
                ErrorCode::ModelOptionUnsupported,
                "model.reserved_option",
            ));
        }
    }
    Ok(())
}
/// Replace complete top-level values; never recursively merge nested objects.
pub fn merge_model_options(defaults: &JsonObject, overrides: &JsonObject) -> JsonObject {
    let mut merged = defaults.clone();
    merged.extend(overrides.clone());
    merged
}
pub(crate) fn agent_options(snapshot: &RunSnapshot) -> JsonObject {
    merge_model_options(
        &snapshot.profile.profile().model_options,
        &snapshot.request.model_options,
    )
}
pub(crate) fn requested_sources(
    snapshot: &RunSnapshot,
    purpose: ModelPurpose,
    requested: &JsonObject,
) -> BTreeMap<String, ModelOptionSource> {
    if purpose != ModelPurpose::Agent {
        return requested
            .keys()
            .map(|key| (key.clone(), ModelOptionSource::Purpose))
            .collect();
    }
    requested
        .keys()
        .map(|key| {
            (
                key.clone(),
                if snapshot.request.model_options.contains_key(key) {
                    ModelOptionSource::Run
                } else {
                    ModelOptionSource::Profile
                },
            )
        })
        .collect()
}

pub(crate) fn output_cap(snapshot: &RunSnapshot, host_cap: NonZeroU64) -> NonZeroU64 {
    snapshot
        .profile
        .profile()
        .limits
        .max_output_tokens
        .into_iter()
        .chain(snapshot.request.max_output_tokens)
        .fold(host_cap, std::cmp::min)
}
