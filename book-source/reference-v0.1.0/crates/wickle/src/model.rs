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
    /// The selected target is no longer available.
    Unavailable,
    /// Current model/deployment metadata differs from the pinned route.
    VersionDrift,
}

/// Selection request; the router returns data and does not invoke a model.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteRequest {
    /// Host logical binding for this purpose; Agent calls use the profile selection.
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
    /// A fenced recovery accepted that no complete response was saved.
    Interrupted {
        /// Recovery command that closed this attempt without refunding its budget.
        recovery_command_id: Id,
    },
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
    /// Protected current-target inspection, distinct from provider response metadata.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub inspection_ref: Option<RecordRef>,
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
