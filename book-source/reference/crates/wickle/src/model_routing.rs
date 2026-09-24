use crate::{
    CatalogRequirements, ContractError, ErrorCode, Id, JsonDigest, ModelCatalogSnapshot,
    ModelFailureKind, ModelPurpose, ModelSupportStatus, PortFuture, ResolvedCatalogBinding,
    ResolvedModelRoute, RouteRequest, Scope, VersionPolicy, VersionedRef,
    serialization::data_digest,
};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Protected catalog-and-policy snapshot format.
pub const ROUTING_SNAPSHOT_VERSION: &str = "wickle.routing-snapshot.v1";
/// Maximum explicitly ordered fallback targets in one purpose rule.
pub const MAX_ROUTE_FALLBACKS: usize = 16;
/// Maximum profile-binding/purpose rules in one immutable policy.
pub const MAX_ROUTING_RULES: usize = 128;

/// Static selection constraints, distinct from current PolicyPort authorization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingRule {
    /// Profile's logical Host binding name.
    pub model_binding: Id,
    /// Agent, verification, or compaction invocation purpose.
    pub purpose: ModelPurpose,
    /// Exact default target, without implicit latest lookup.
    pub primary: VersionedRef,
    /// Exact permitted fallback targets, visited only in this order.
    pub fallbacks: Vec<VersionedRef>,
    /// Explicit classified failures permitting progression to later targets.
    pub fallback_on: Vec<ModelFailureKind>,
    /// Immutable target requirement; a request can tighten but never relax it.
    #[serde(default)]
    pub version_policy: VersionPolicy,
    /// Required evidence stage. Execution defaults to contract-tested support.
    #[serde(default = "contract_tested")]
    pub min_support: ModelSupportStatus,
}

fn contract_tested() -> ModelSupportStatus {
    ModelSupportStatus::ContractTested
}

/// Exact-scope policy owned by the Host. Target/data boundaries are the registered
/// revisions in each rule, including their provider, connection, API and target.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingPolicy {
    /// Immutable policy revision.
    pub revision: Id,
    /// Exact tenant/workspace/user namespace.
    pub scope: Scope,
    /// Unique logical binding/purpose rules, with no implicit purpose fallback.
    pub rules: Vec<RoutingRule>,
}

/// Privately owned catalog and policy for admission and resume. Serialize only
/// into Host configuration or protected storage; Debug omits catalog payloads.
#[derive(Clone, PartialEq, Serialize)]
pub struct RoutingSnapshot {
    schema_version: String,
    catalog: ModelCatalogSnapshot,
    policy: RoutingPolicy,
}

impl fmt::Debug for RoutingSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoutingSnapshot")
            .field("catalog_revision", &self.catalog.revision)
            .field("policy_revision", &self.policy.revision)
            .field("rule_count", &self.policy.rules.len())
            .finish_non_exhaustive()
    }
}

impl RoutingSnapshot {
    /// Validate and own exact metadata, without retrieving a newer catalog.
    pub fn new(
        catalog: ModelCatalogSnapshot,
        policy: RoutingPolicy,
    ) -> Result<Self, ContractError> {
        catalog.validate()?;
        if policy.scope != catalog.scope {
            return Err(error(ErrorCode::AccessDenied, "routing.scope"));
        }
        if policy.rules.is_empty() || policy.rules.len() > MAX_ROUTING_RULES {
            return Err(error(ErrorCode::ModelRoutingInvalid, "routing.rules"));
        }
        for (index, rule) in policy.rules.iter().enumerate() {
            if policy.rules[..index].iter().any(|other| {
                other.model_binding == rule.model_binding && other.purpose == rule.purpose
            }) || rule.fallbacks.len() > MAX_ROUTE_FALLBACKS
                || rule
                    .fallback_on
                    .iter()
                    .enumerate()
                    .any(|(index, kind)| rule.fallback_on[..index].contains(kind))
            {
                return Err(error(ErrorCode::ModelRoutingInvalid, "routing.rule"));
            }
            let candidates: Vec<_> = std::iter::once(&rule.primary)
                .chain(&rule.fallbacks)
                .collect();
            for (index, reference) in candidates.iter().enumerate() {
                if candidates[..index].contains(reference) {
                    return Err(error(
                        ErrorCode::ModelRoutingInvalid,
                        "routing.duplicate_candidate",
                    ));
                }
                if !catalog
                    .bindings
                    .iter()
                    .any(|binding| &binding.binding == *reference)
                {
                    return Err(error(ErrorCode::ModelNotRegistered, "routing.binding"));
                }
            }
        }
        Ok(Self {
            schema_version: ROUTING_SNAPSHOT_VERSION.into(),
            catalog,
            policy,
        })
    }
    /// Exact immutable catalog, including all model and binding revisions.
    pub fn catalog(&self) -> &ModelCatalogSnapshot {
        &self.catalog
    }
    /// Exact immutable policy; this does not grant current access permission.
    pub fn policy(&self) -> &RoutingPolicy {
        &self.policy
    }
    /// Exact data namespace shared by catalog and policy.
    pub fn scope(&self) -> &Scope {
        &self.catalog.scope
    }
    /// Digest of the complete protected snapshot.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }
    /// Reconstruct one exact policy-listed route from saved metadata. This checks
    /// membership, not current access, lifecycle or invocation requirements.
    pub fn route_for_binding(
        &self,
        reference: &VersionedRef,
    ) -> Result<ResolvedModelRoute, ContractError> {
        if !self
            .policy
            .rules
            .iter()
            .any(|rule| &rule.primary == reference || rule.fallbacks.contains(reference))
        {
            return Err(error(ErrorCode::ModelRouteDenied, "routing.binding"));
        }
        let resolved = self.resolved_binding(reference)?;
        Ok(ResolvedModelRoute {
            binding: resolved.binding.binding.clone(),
            catalog_revision: self.catalog.revision.clone(),
            routing_policy_revision: self.policy.revision.clone(),
            requested_model: resolved.binding.requested_model.clone(),
            model_id: resolved.model.model_id.clone(),
            model_version: resolved.model.model_version.clone(),
            version_semantics: resolved.effective_version_semantics(),
            provider: resolved.model.provider.clone(),
            target: resolved.binding.target.clone(),
            deployment_revision: resolved.binding.deployment_revision.clone(),
            api_contract: resolved.binding.api_contract.clone(),
            adapter: resolved.binding.adapter.clone(),
            capability_revision: resolved.binding.capabilities.revision.clone(),
            connection_ref: resolved.binding.connection_ref.clone(),
        })
    }
    /// Check a saved route's entire identity against a policy-listed catalog binding.
    /// Historical membership does not establish current availability or permission.
    pub fn validate_route(&self, route: &ResolvedModelRoute) -> Result<(), ContractError> {
        if self.route_for_binding(&route.binding)? != *route {
            return Err(error(ErrorCode::ModelRoutingMismatch, "routing.route"));
        }
        Ok(())
    }
    /// Validate a custom router result before trusting its target or provenance.
    /// Enforces the same static constraints and ordered progression as the policy.
    pub fn validate_selection(
        &self,
        request: &RouteRequest,
        selection: &RouteSelection,
    ) -> Result<(), ContractError> {
        if &request.scope != self.scope() {
            return Err(error(ErrorCode::AccessDenied, "routing.scope"));
        }
        if selection.routing_snapshot_digest != self.digest()
            || selection.request_digest != request.digest()
        {
            return Err(error(
                ErrorCode::ModelRoutingMismatch,
                "routing.selection_digest",
            ));
        }
        let rule = self
            .policy
            .rules
            .iter()
            .find(|rule| {
                rule.model_binding == request.model_binding && rule.purpose == request.purpose
            })
            .ok_or_else(|| error(ErrorCode::ModelRouteDenied, "routing.rule"))?;
        if rule.min_support < ModelSupportStatus::ContractTested {
            return Err(error(
                ErrorCode::ModelSupportInsufficient,
                "routing.min_support",
            ));
        }
        let candidates: Vec<_> = std::iter::once(&rule.primary)
            .chain(&rule.fallbacks)
            .collect();
        let selected = candidates
            .get(selection.candidate_index)
            .ok_or_else(|| error(ErrorCode::ModelRoutingMismatch, "routing.candidate_index"))?;
        if selection.route.binding != **selected {
            return Err(error(
                ErrorCode::ModelRoutingMismatch,
                "routing.candidate_index",
            ));
        }
        self.validate_route(&selection.route)?;
        let previous = match &request.previous_route {
            Some(route) => {
                self.validate_route(route)?;
                Some(
                    candidates
                        .iter()
                        .position(|candidate| **candidate == route.binding)
                        .ok_or_else(|| {
                            error(ErrorCode::ModelRoutingMismatch, "routing.previous_route")
                        })?,
                )
            }
            None => None,
        };
        let earlier = match (previous, request.previous_failure, selection.reason) {
            (None, None, RouteSelectionReason::Initial) if selection.candidate_index == 0 => None,
            (Some(index), None, RouteSelectionReason::Reuse)
                if selection.candidate_index == index =>
            {
                None
            }
            (Some(index), Some(failure), RouteSelectionReason::Fallback { failure: reason })
                if failure == reason && selection.candidate_index > index =>
            {
                if !rule.fallback_on.contains(&failure) {
                    return Err(error(
                        ErrorCode::ModelRouteDenied,
                        "routing.fallback_reason",
                    ));
                }
                Some(index + 1)
            }
            _ => {
                return Err(error(
                    ErrorCode::ModelRoutingMismatch,
                    "routing.selection_reason",
                ));
            }
        };
        if !request.allowed_bindings.contains(&selected.id) {
            return Err(error(
                ErrorCode::ModelRouteDenied,
                "routing.allowed_bindings",
            ));
        }
        let requirements = CatalogRequirements {
            features: request.required_capabilities.clone(),
            options: request.options.clone(),
            input_tokens: request.input_tokens,
            max_output_tokens: request.max_output_tokens,
            version_policy: if rule.version_policy == VersionPolicy::RequirePinned
                || request.version_policy == VersionPolicy::RequirePinned
            {
                VersionPolicy::RequirePinned
            } else {
                VersionPolicy::AllowMutable
            },
            min_support: rule.min_support,
        };
        self.resolved_binding(selected)?.validate(&requirements)?;
        if let Some(start) = earlier {
            for candidate in &candidates[start..selection.candidate_index] {
                if request.allowed_bindings.contains(&candidate.id)
                    && self
                        .resolved_binding(candidate)?
                        .validate(&requirements)
                        .is_ok()
                {
                    return Err(error(
                        ErrorCode::ModelRoutingMismatch,
                        "routing.candidate_order",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Resolve and validate inference settings against the pinned destination.
    pub fn model_configuration(
        &self,
        route: &crate::ResolvedModelRoute,
        requested: &crate::JsonObject,
        sources: &std::collections::BTreeMap<String, crate::ModelOptionSource>,
        upper_bound: std::num::NonZeroU64,
    ) -> Result<crate::ModelConfiguration, ContractError> {
        self.validate_route(route)?;
        let resolved = self.resolved_binding(&route.binding)?;
        let effective = crate::merge_model_options(&resolved.binding.default_options, requested);
        resolved.model.capabilities.validate_options(&effective)?;
        resolved.binding.capabilities.validate_options(&effective)?;
        if sources.keys().ne(requested.keys()) {
            return Err(error(
                ErrorCode::InvalidConfiguration,
                "model.option_sources",
            ));
        }
        let mut origins = resolved
            .binding
            .default_options
            .keys()
            .map(|key| (key.clone(), crate::ModelOptionSource::Binding))
            .collect::<std::collections::BTreeMap<_, _>>();
        origins.extend(sources.clone());
        Ok(crate::ModelConfiguration {
            requested: requested.clone(),
            effective,
            sources: origins,
            model_schema_revision: resolved.model.capabilities.revision.clone(),
            binding_schema_revision: resolved.binding.capabilities.revision.clone(),
            requested_max_output_tokens: upper_bound,
            max_output_tokens: upper_bound
                .min(resolved.model.capabilities.max_output_tokens)
                .min(resolved.binding.capabilities.max_output_tokens),
        })
    }

    fn resolved_binding(
        &self,
        reference: &VersionedRef,
    ) -> Result<ResolvedCatalogBinding, ContractError> {
        let binding = self
            .catalog
            .bindings
            .iter()
            .find(|binding| &binding.binding == reference)
            .ok_or_else(|| error(ErrorCode::ModelNotRegistered, "routing.binding"))?;
        let model = self
            .catalog
            .models
            .iter()
            .find(|model| model.reference() == binding.model)
            .ok_or_else(|| error(ErrorCode::ModelNotRegistered, "routing.model"))?;
        Ok(ResolvedCatalogBinding {
            catalog_revision: self.catalog.revision.clone(),
            model: model.clone(),
            binding: binding.clone(),
        })
    }
    /// Restore against a trusted scope and digest, then revalidate the saved data.
    pub fn restore(
        input: &str,
        scope: &Scope,
        expected_digest: &JsonDigest,
    ) -> Result<Self, ContractError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Data {
            schema_version: String,
            catalog: ModelCatalogSnapshot,
            policy: RoutingPolicy,
        }
        let value = crate::parse_json(input)?;
        if value
            .get("schema_version")
            .and_then(serde_json::Value::as_str)
            != Some(ROUTING_SNAPSHOT_VERSION)
        {
            return Err(error(
                ErrorCode::UnsupportedSchemaVersion,
                "routing.schema_version",
            ));
        }
        if &crate::canonical_digest(&value) != expected_digest {
            return Err(error(ErrorCode::ModelRoutingMismatch, "routing.digest"));
        }
        let data: Data = serde_json::from_value(value)
            .map_err(|_| error(ErrorCode::ModelRoutingInvalid, "routing.snapshot"))?;
        if &data.catalog.scope != scope || &data.policy.scope != scope {
            return Err(error(ErrorCode::AccessDenied, "routing.scope"));
        }
        let snapshot = Self::new(data.catalog, data.policy)?;
        if data.schema_version != snapshot.schema_version || &snapshot.digest() != expected_digest {
            return Err(error(ErrorCode::ModelRoutingMismatch, "routing.digest"));
        }
        Ok(snapshot)
    }
}

/// Why a route was selected, separate from provider-reported metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RouteSelectionReason {
    /// First choice of the exact default target.
    Initial,
    /// Revalidation of a saved exact route, without progression.
    Reuse,
    /// Explicit progression after a policy-permitted failure.
    Fallback {
        /// The caller's classified failure; the router does not invoke a model to obtain it.
        failure: ModelFailureKind,
    },
}

/// One static selection. A driver must still check current authorization, target
/// availability, projection compatibility and the shared Run budget before I/O.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteSelection {
    /// Complete immutable provider/model/API/target/connection selection.
    pub route: ResolvedModelRoute,
    /// Initial choice, exact reuse, or explicit fallback reason.
    pub reason: RouteSelectionReason,
    /// Zero denotes primary; larger indices follow the finite policy list.
    pub candidate_index: usize,
    /// Identity of the saved catalog and policy used for this decision.
    pub routing_snapshot_digest: JsonDigest,
    /// Identity of the full RouteRequest, including options and allowed targets.
    pub request_digest: JsonDigest,
}

/// Static routing port. It never generates model output or executes a Tool.
pub trait ModelRouter: Send + Sync {
    /// Immutable metadata used by this instance; resume must use the saved snapshot.
    fn snapshot(&self) -> &RoutingSnapshot;
    /// Select or revalidate a route. The caller supplies the latest saved previous
    /// route and explicit failure; the router does not track attempts or retry I/O.
    fn resolve<'a>(&'a self, request: &'a RouteRequest) -> PortFuture<'a, RouteSelection>;
}

impl RouteRequest {
    /// Identity of every routing input, including scope, purpose, limits and options.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }
}

fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
