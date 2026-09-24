use crate::ImmutableModelCatalog;
use wickle::{
    CatalogRequirements, ContractError, ErrorCode, ModelCatalog, ModelRouter, ModelSupportStatus,
    PortFuture, RouteRequest, RouteSelection, RouteSelectionReason, RoutingSnapshot, VersionPolicy,
};

/// Deterministic static selection over a privately owned catalog and policy.
/// No model call, metadata probe, credential lookup, retry or fallback I/O occurs here.
#[derive(Debug, Clone)]
pub struct PolicyModelRouter {
    snapshot: RoutingSnapshot,
    catalog: ImmutableModelCatalog,
}

impl PolicyModelRouter {
    /// Own one validated snapshot. A later catalog object cannot replace its data.
    pub fn new(snapshot: RoutingSnapshot) -> Result<Self, ContractError> {
        let catalog = ImmutableModelCatalog::new(snapshot.catalog().clone())?;
        Ok(Self { snapshot, catalog })
    }

    async fn select(&self, request: &RouteRequest) -> Result<RouteSelection, ContractError> {
        if &request.scope != self.snapshot.scope() {
            return Err(error(ErrorCode::AccessDenied, "routing.scope"));
        }
        let rule = self
            .snapshot
            .policy()
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
        let previous_index = match &request.previous_route {
            Some(previous) => {
                let index = candidates
                    .iter()
                    .position(|reference| **reference == previous.binding)
                    .ok_or_else(|| {
                        error(ErrorCode::ModelRoutingMismatch, "routing.previous_route")
                    })?;
                if self.snapshot.route_for_binding(candidates[index])? != *previous {
                    return Err(error(
                        ErrorCode::ModelRoutingMismatch,
                        "routing.previous_route",
                    ));
                }
                Some(index)
            }
            None => None,
        };
        let (start, end, reason) = match (previous_index, request.previous_failure) {
            (None, None) => (0, 1, RouteSelectionReason::Initial),
            (Some(index), None) => (index, index + 1, RouteSelectionReason::Reuse),
            (None, Some(_)) => {
                return Err(error(
                    ErrorCode::ModelRoutingInvalid,
                    "routing.previous_failure",
                ));
            }
            (Some(index), Some(failure)) => {
                if !rule.fallback_on.contains(&failure) {
                    return Err(error(
                        ErrorCode::ModelRouteDenied,
                        "routing.fallback_reason",
                    ));
                }
                (
                    index + 1,
                    candidates.len(),
                    RouteSelectionReason::Fallback { failure },
                )
            }
        };
        if start == end {
            return Err(error(ErrorCode::ModelRoutesExhausted, "routing.candidates"));
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
        let fallback = matches!(reason, RouteSelectionReason::Fallback { .. });
        let mut permitted = false;
        for (index, reference) in candidates.iter().enumerate().take(end).skip(start) {
            if !request.allowed_bindings.contains(&reference.id) {
                if !fallback {
                    return Err(error(
                        ErrorCode::ModelRouteDenied,
                        "routing.allowed_bindings",
                    ));
                }
                continue;
            }
            permitted = true;
            let binding = self
                .catalog
                .get_binding(&request.scope, self.catalog.revision(), reference)
                .await?;
            if let Err(error) = binding.validate(&requirements) {
                if !fallback {
                    return Err(error);
                }
                continue;
            }
            let selection = RouteSelection {
                route: self.snapshot.route_for_binding(reference)?,
                reason,
                candidate_index: index,
                routing_snapshot_digest: self.snapshot.digest(),
                request_digest: request.digest(),
            };
            self.snapshot.validate_selection(request, &selection)?;
            return Ok(selection);
        }
        Err(if permitted {
            error(ErrorCode::ModelRoutesExhausted, "routing.candidates")
        } else {
            error(ErrorCode::ModelRouteDenied, "routing.allowed_bindings")
        })
    }
}

impl ModelRouter for PolicyModelRouter {
    fn snapshot(&self) -> &RoutingSnapshot {
        &self.snapshot
    }
    fn resolve<'a>(&'a self, request: &'a RouteRequest) -> PortFuture<'a, RouteSelection> {
        Box::pin(self.select(request))
    }
}

/// The same scoped routing contract restricted to one exact target and no fallback.
/// Different logical profile names or purposes may explicitly select that target.
#[derive(Debug, Clone)]
pub struct FixedModelRouter {
    inner: PolicyModelRouter,
}

impl FixedModelRouter {
    /// Reject multi-target policies instead of quietly choosing their first binding.
    pub fn new(snapshot: RoutingSnapshot) -> Result<Self, ContractError> {
        let first = &snapshot.policy().rules[0].primary;
        if snapshot
            .policy()
            .rules
            .iter()
            .any(|rule| !rule.fallbacks.is_empty() || &rule.primary != first)
        {
            return Err(error(
                ErrorCode::ModelRoutingInvalid,
                "routing.fixed_target",
            ));
        }
        Ok(Self {
            inner: PolicyModelRouter::new(snapshot)?,
        })
    }
}

impl ModelRouter for FixedModelRouter {
    fn snapshot(&self) -> &RoutingSnapshot {
        self.inner.snapshot()
    }
    fn resolve<'a>(&'a self, request: &'a RouteRequest) -> PortFuture<'a, RouteSelection> {
        self.inner.resolve(request)
    }
}

fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
