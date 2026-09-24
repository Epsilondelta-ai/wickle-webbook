use std::{fmt, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    ContractError, ErrorCode, Id, JsonDigest, ModelPort, PortFuture, ResolvedModelRoute, Scope,
    VersionPolicy, VersionSemantics, serialization::optional,
};

/// Resolve an already-selected route to an existing, scope-bound model adapter.
/// This is a synchronous registry lookup: it must not perform I/O, create clients,
/// choose fallback targets, or invoke a model. The caller rechecks the returned
/// port's binding before dispatch and retains ownership of policy and budgets.
pub trait ModelDispatcher: Send + Sync {
    /// Return the adapter for this exact provider, implementation and connection.
    fn resolve(
        &self,
        scope: &Scope,
        route: &ResolvedModelRoute,
    ) -> Result<Arc<dyn ModelPort>, ContractError>;
}

/// Current authenticated actor and finite bounds for a metadata lookup.
/// No prompt, system-input map, or credential payload is exposed here.
#[derive(Debug, Clone)]
pub struct ModelInspectionContext {
    /// Authenticated scope in which the selected target may be inspected.
    pub scope: Scope,
    /// Current principal, independent of the original routing configuration.
    pub principal_ref: Id,
    /// Current Host grant reference; the inspector's backend enforces its scope.
    pub capability_grant_ref: Id,
    /// Cooperative signal that the caller cancels when the inspection ends.
    pub cancellation: CancellationToken,
    /// Effective deadline for this metadata lookup.
    pub deadline: tokio::time::Instant,
}

/// Availability reported by a trusted metadata inspection, not a model response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelRouteAvailability {
    /// The target was reported available when inspected.
    Available,
    /// The target was reported unavailable; no substitute is selected here.
    Unavailable,
    /// Availability could not be established and must not be treated as available.
    Unknown,
}

/// Facts observed by the Host's read-only metadata inspector. These facts never
/// populate provider-reported response fields or prove a later request's result.
/// The Host authenticates the evidence; validation checks its declared consistency.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRouteObservation {
    /// Exact selected route that the inspector checked.
    pub route_digest: JsonDigest,
    /// Availability at inspection time.
    pub availability: ModelRouteAvailability,
    /// Model identifier actually established by inspection, when known.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub model_id: Option<Id>,
    /// Release actually established by inspection, when known.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub model_version: Option<Id>,
    /// Deployment revision actually established by inspection, when known.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub deployment_revision: Option<Id>,
    /// Observed version guarantee; never inferred from identifier spelling.
    pub version_semantics: VersionSemantics,
    /// Host-owned evidence reference; raw service payloads remain outside this DTO.
    pub evidence_ref: Id,
}

impl fmt::Debug for ModelRouteObservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelRouteObservation")
            .field("route_digest", &self.route_digest)
            .field("availability", &self.availability)
            .field("version_semantics", &self.version_semantics)
            .finish_non_exhaustive()
    }
}

impl ModelRouteObservation {
    /// Check availability, exact target identity and the requested version guarantee.
    /// Unknown facts remain unknown. An observation cannot silently replace the
    /// selected route, upgrade its declared semantics, or guarantee no later drift.
    pub fn validate(
        &self,
        route: &ResolvedModelRoute,
        version_policy: VersionPolicy,
    ) -> Result<(), ContractError> {
        if self.route_digest != route.digest()
            || self
                .model_id
                .as_ref()
                .is_some_and(|id| id != &route.model_id)
            || self
                .model_version
                .as_ref()
                .is_some_and(|version| version != &route.model_version)
            || self
                .deployment_revision
                .as_ref()
                .zip(route.deployment_revision.as_ref())
                .is_some_and(|(actual, expected)| actual != expected)
        {
            return Err(ContractError::new(
                ErrorCode::ModelVersionDrift,
                "model.inspection.target",
            ));
        }
        match self.availability {
            ModelRouteAvailability::Unavailable => {
                return Err(ContractError::new(
                    ErrorCode::ModelUnavailable,
                    "model.inspection.availability",
                ));
            }
            ModelRouteAvailability::Unknown => {
                return Err(ContractError::new(
                    ErrorCode::ModelInspectionUnavailable,
                    "model.inspection.availability",
                ));
            }
            ModelRouteAvailability::Available => {}
        }
        if version_policy == VersionPolicy::RequirePinned
            && (route.version_semantics != VersionSemantics::Pinned
                || self.version_semantics != VersionSemantics::Pinned
                || self.model_id.is_none()
                || self.model_version.is_none()
                || (route.deployment_revision.is_some()
                    && self.deployment_revision != route.deployment_revision))
        {
            return Err(ContractError::new(
                ErrorCode::ModelVersionUnpinned,
                "model.inspection.version",
            ));
        }
        Ok(())
    }
}

/// Trusted read-only provider/deployment metadata lookup. It must not run model
/// inference, rewrite routes, or hide retry loops. The caller authorizes lookup,
/// bounds time/cancellation, validates the result and records its evidence.
pub trait ModelRouteInspector: Send + Sync {
    /// Inspect only the supplied route under the current authenticated context.
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        context: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation>;
}
