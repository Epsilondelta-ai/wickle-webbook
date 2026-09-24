use super::*;
use crate::{
    ModelInspectionContext, ModelPurpose, ModelRouter, PortFuture, ResolvedModelRoute,
    RouteRequest, RouteSelection, RouteSelectionReason, RoutingSnapshot, ToolCallState,
    VersionPolicy,
};
use tokio_util::sync::CancellationToken;

/// One logical model step. Its physical retries receive separate attempt IDs.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutedModelInput {
    /// Stable step identifier chosen by the driver and retained during recovery.
    pub model_step_id: Id,
    /// Host requirements; saved history supplies the previous route and failure.
    pub routing: RouteRequest,
}

/// Current Host context for a bounded route-specific projection.
pub struct ModelProjectionContext {
    /// Exact authenticated namespace.
    pub scope: crate::Scope,
    /// Current principal for any separately authorized context reads.
    pub principal_ref: Id,
    /// Current Host capability grant.
    pub capability_grant_ref: Id,
    /// Cancelled when projection completes, fails, or its caller stops.
    pub cancellation: CancellationToken,
    /// Finite deadline inherited from the Run.
    pub deadline: tokio::time::Instant,
}

/// A fully prepared request and its route-specific input-token estimate.
#[derive(Debug, Clone)]
pub struct ProjectedModelRequest {
    /// Exact selected route, purpose, logical step, options and output budget.
    pub request: ModelRequest,
    /// Host/tokenizer estimate for this final projection, not a byte count.
    pub input_tokens: u64,
}

/// Trusted Host projection port. It preserves required context and builds a fresh
/// request for the exact selected route; it must not invoke a model or run a Tool.
pub trait ModelRequestProjector: Send + Sync {
    /// Project immutable transcript/context into the selected provider's contract.
    fn project<'a>(
        &'a self,
        selection: &'a RouteSelection,
        input: &'a RoutedModelInput,
        context: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ProjectedModelRequest>;
    /// Recheck access to already projected context without fetching or changing
    /// its contents. Called for every physical attempt, including same-route
    /// retries, and before a saved model response is reused.
    fn authorize_use<'a>(
        &'a self,
        _selection: &'a RouteSelection,
        _input: &'a RoutedModelInput,
        _context: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

pub(super) struct ContextUseGate<'a> {
    pub projector: &'a dyn ModelRequestProjector,
    pub selection: &'a RouteSelection,
    pub input: &'a RoutedModelInput,
}
impl ContextUseGate<'_> {
    pub(super) async fn check(
        &self,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let controls = ModelProjectionContext {
            scope: budget.scope().clone(),
            principal_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
            cancellation: budget.cancellation().child_token(),
            deadline: budget.call_deadline()?,
        };
        let _cancel = controls.cancellation.clone().drop_guard();
        external(
            async {
                self.projector
                    .authorize_use(self.selection, self.input, &controls)
                    .await
            },
            context,
            budget,
            ErrorCode::InvalidContext,
        )
        .await
        .map_err(|error| {
            // Only model inspection failures can request target fallback. A
            // context provider must not turn its failed use check into routing.
            if matches!(
                error.code,
                ErrorCode::ModelUnavailable | ErrorCode::ModelVersionDrift
            ) {
                failure(ErrorCode::InvalidContext, "model.context_use")
            } else {
                error
            }
        })
    }
}

impl ModelExchange {
    /// Resolve, project, inspect, and execute a logical model step with finite
    /// explicit fallback. The same Run budgets account for every recovery and
    /// physical call. This does not advance an agent loop or execute Tools.
    ///
    /// The catalog/policy snapshot is pinned before the first physical attempt.
    /// Completed responses for this step are reused after current authorization
    /// and request-identity checks. Unresolved attempts require explicit recovery;
    /// this method never silently resends them. Projection is trusted Host code:
    /// it must preserve required context when changing providers.
    pub async fn generate_routed(
        &self,
        router: &dyn ModelRouter,
        input: &RoutedModelInput,
        projector: &dyn ModelRequestProjector,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<Guarded<ModelExchangeOutcome>, ContractError> {
        if self.inspector.is_none() {
            return Err(failure(ErrorCode::InvalidConfiguration, "model.inspector"));
        }
        if input.routing.scope != context.data.scope || &input.routing.scope != budget.scope() {
            return Err(failure(ErrorCode::AccessDenied, "routing.scope"));
        }
        if input.routing.previous_route.is_some() || input.routing.previous_failure.is_some() {
            return Err(failure(
                ErrorCode::ModelRoutingInvalid,
                "routing.history_is_stored",
            ));
        }
        budget.check_boundary().await?;
        let pinned = router.snapshot().clone();
        self.pin_routing(&pinned, context, budget).await?;
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        if input.routing.purpose == ModelPurpose::Agent
            && input.routing.model_binding != saved.snapshot.profile.profile().model_binding
        {
            return Err(failure(
                ErrorCode::ModelRouteDenied,
                "routing.profile_binding",
            ));
        }
        if input.routing.purpose == ModelPurpose::Agent
            && input.routing.options != saved.snapshot.request.model_options
        {
            return Err(failure(ErrorCode::RequestConflict, "routing.model_options"));
        }
        if saved.snapshot.tool_ledger.iter().any(|entry| !matches!(&entry.state,
            ToolCallState::Settled { result } if result.status != crate::ToolResultStatus::Unknown && result.effect != crate::ToolEffect::Unknown
        )) {
            return Err(failure(ErrorCode::InvalidTransition, "routing.unsettled_tools"));
        }
        self.pin_step_input(input, context, budget).await?;
        if saved.snapshot.model_ledger.iter().any(|attempt| {
            matches!(
                attempt.state,
                ModelAttemptState::Reserved {} | ModelAttemptState::Unknown {}
            )
        }) {
            return Err(failure(
                ErrorCode::ModelAttemptUnresolved,
                "routing.attempt",
            ));
        }
        let previous = saved
            .snapshot
            .model_ledger
            .iter()
            .rev()
            .find(|attempt| attempt.model_step_id == input.model_step_id)
            .cloned();
        let mut routing = input.routing.clone();
        let mut replay = None;
        let mut interrupted = None;
        if let Some(previous) = previous {
            if previous.purpose != routing.purpose {
                return Err(failure(
                    ErrorCode::ModelRoutingMismatch,
                    "routing.step_purpose",
                ));
            }
            routing.previous_route = Some(previous.route.clone());
            match previous.state {
                ModelAttemptState::Completed {} => replay = Some(previous),
                ModelAttemptState::Interrupted { .. } => interrupted = Some(previous),
                ModelAttemptState::Failed { kind } => routing.previous_failure = Some(kind),
                ModelAttemptState::Reserved {} | ModelAttemptState::Unknown {} => {
                    return Err(failure(
                        ErrorCode::ModelAttemptUnresolved,
                        "routing.attempt",
                    ));
                }
            }
        }
        // A custom router must also advance monotonically through the pinned list.
        let mut previous_index = None;
        for _ in 0..=crate::MAX_ROUTE_FALLBACKS {
            budget.check_boundary().await?;
            if context.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let selection = external(
                async { router.resolve(&routing).await },
                context,
                budget,
                ErrorCode::ModelRoutingInvalid,
            )
            .await?;
            if router.snapshot().digest() != pinned.digest() {
                return Err(failure(ErrorCode::ModelRoutingMismatch, "routing.snapshot"));
            }
            pinned.validate_selection(&routing, &selection)?;
            if previous_index.is_some_and(|index| selection.candidate_index <= index) {
                return Err(failure(
                    ErrorCode::ModelRoutingMismatch,
                    "routing.fallback_order",
                ));
            }
            if matches!(selection.reason, RouteSelectionReason::Fallback { .. }) {
                budget.reserve(ReservationKind::Recovery {}).await?;
            }
            // Authorize the exact destination before a Host projection or metadata lookup.
            if let Guarded::ApprovalRequired(challenge) = self
                .authorize_route(&selection.route, routing.purpose, context, budget)
                .await?
            {
                return Ok(Guarded::ApprovalRequired(challenge));
            }
            let projection_context = ModelProjectionContext {
                scope: budget.scope().clone(),
                principal_ref: context.data.principal_ref.clone(),
                capability_grant_ref: context.data.capability_grant_ref.clone(),
                cancellation: budget.cancellation().child_token(),
                deadline: budget.call_deadline()?,
            };
            let _cancel = projection_context.cancellation.clone().drop_guard();
            let prepared = external(
                async {
                    projector
                        .project(&selection, input, &projection_context)
                        .await
                },
                context,
                budget,
                ErrorCode::ModelContextIncompatible,
            )
            .await?;
            projection_context.cancellation.cancel();
            validate_projection(&prepared.request, input, &selection)?;
            // Validate the final route-specific estimate, not the earlier candidate estimate.
            let mut final_requirements = routing.clone();
            final_requirements.input_tokens = prepared.input_tokens;
            final_requirements
                .required_capabilities
                .extend(prepared.request.required_capabilities());
            final_requirements.previous_route = Some(selection.route.clone());
            final_requirements.previous_failure = None;
            let mut final_selection = selection.clone();
            final_selection.reason = RouteSelectionReason::Reuse;
            final_selection.request_digest = final_requirements.digest();
            pinned.validate_selection(&final_requirements, &final_selection)?;
            if let Some(previous) = interrupted.take() {
                let mut physical = prepared.request.clone();
                physical.request_id = previous.attempt_id;
                if physical.digest() != previous.request_digest {
                    return Err(failure(
                        ErrorCode::RequestConflict,
                        "routing.recovery_projection",
                    ));
                }
            }
            if let Some(previous) = replay.take() {
                let mut physical = prepared.request.clone();
                physical.request_id = previous.attempt_id;
                if physical.digest() != previous.request_digest {
                    return Err(failure(
                        ErrorCode::RequestConflict,
                        "routing.replay_projection",
                    ));
                }
                if let Guarded::ApprovalRequired(challenge) =
                    self.authorize(&prepared.request, context, budget).await?
                {
                    return Ok(Guarded::ApprovalRequired(challenge));
                }
                let reference = previous
                    .response_ref
                    .ok_or_else(|| failure(ErrorCode::InvalidSnapshot, "routing.response"))?;
                let record = budget
                    .store()
                    .read_record(budget.scope(), &reference)
                    .await?;
                let response: StoredModelResponse = serde_json::from_value(record.value().clone())
                    .map_err(|_| failure(ErrorCode::InvalidSnapshot, "routing.response"))?;
                ContextUseGate {
                    projector,
                    selection: &selection,
                    input,
                }
                .check(context, budget)
                .await?;
                budget.check_boundary().await?;
                if context.cancellation.is_cancelled() {
                    return Err(cancelled());
                }
                return Ok(Guarded::Completed(response.outcome));
            }
            let rule = pinned
                .policy()
                .rules
                .iter()
                .find(|rule| {
                    rule.model_binding == routing.model_binding && rule.purpose == routing.purpose
                })
                .ok_or_else(|| failure(ErrorCode::ModelRouteDenied, "routing.rule"))?;
            let version_policy = if selection.route.version_semantics
                == crate::VersionSemantics::Pinned
                || rule.version_policy == VersionPolicy::RequirePinned
                || routing.version_policy == VersionPolicy::RequirePinned
            {
                VersionPolicy::RequirePinned
            } else {
                VersionPolicy::AllowMutable
            };
            // Keep nested auxiliary exchanges within the default executor stack budget.
            let result = crate::future::boxed(|| {
                self.generate_inner(
                    &prepared.request,
                    context,
                    budget,
                    Some((&selection, version_policy)),
                    Some(ContextUseGate {
                        projector,
                        selection: &selection,
                        input,
                    }),
                )
            })
            .await;
            let cause = match result {
                Ok(Guarded::Completed(ModelExchangeOutcome::Failed { failure: ref error })) => {
                    error.kind
                }
                Err(ref error) if error.code == ErrorCode::ModelVersionDrift => {
                    ModelFailureKind::VersionDrift
                }
                Err(ref error) if error.code == ErrorCode::ModelUnavailable => {
                    ModelFailureKind::Unavailable
                }
                other => return other,
            };
            if !rule.fallback_on.contains(&cause) {
                return result;
            }
            previous_index = Some(selection.candidate_index);
            routing.previous_route = Some(selection.route);
            routing.previous_failure = Some(cause);
        }
        Err(failure(
            ErrorCode::ModelRoutesExhausted,
            "routing.candidates",
        ))
    }

    async fn pin_routing(
        &self,
        routing: &RoutingSnapshot,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        if routing.scope() != budget.scope() {
            return Err(failure(ErrorCode::AccessDenied, "routing.scope"));
        }
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        if let Some(reference) = &saved.snapshot.routing_snapshot_ref {
            let record = budget
                .store()
                .read_record(budget.scope(), reference)
                .await?;
            let restored = RoutingSnapshot::restore(
                &serde_json::to_string(record.value()).map_err(|_| revision_error())?,
                budget.scope(),
                &reference.digest,
            )?;
            if restored.digest() != routing.digest() {
                return Err(failure(
                    ErrorCode::ModelRoutingMismatch,
                    "routing.pinned_snapshot",
                ));
            }
            return Ok(());
        }
        if saved.snapshot.usage.model_calls != 0 {
            return Err(failure(
                ErrorCode::ModelRoutingMismatch,
                "routing.already_started",
            ));
        }
        if context.cancellation.is_cancelled() {
            return Err(cancelled());
        }
        budget.check_boundary().await?;
        let record = ProtectedRecord::new(
            Id::new(format!("model-routing-{}", budget.run_id()))?,
            1,
            serde_json::to_value(routing).map_err(|_| revision_error())?,
        );
        let mut snapshot = saved.snapshot;
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(revision_error)?;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        snapshot.routing_snapshot_ref = Some(record.reference().clone());
        budget
            .store()
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages: vec![],
                    events: vec![],
                    records: vec![record],
                },
            )
            .await?;
        budget.check_boundary().await?;
        Ok(())
    }

    async fn pin_step_input(
        &self,
        input: &RoutedModelInput,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let key =
            crate::canonical_digest(&serde_json::json!([budget.run_id(), input.model_step_id]));
        let record = ProtectedRecord::new(
            Id::new(format!("model-step-{key}"))?,
            1,
            serde_json::json!({"schema_version":"wickle.model-step.v1", "run_id":budget.run_id(), "input":input}),
        );
        match budget
            .store()
            .read_record(budget.scope(), record.reference())
            .await
        {
            Ok(_) => return Ok(()),
            Err(error) if error.code == ErrorCode::StateNotFound => {}
            Err(error) if error.code == ErrorCode::RecordConflict => {
                return Err(failure(ErrorCode::RequestConflict, "routing.step_input"));
            }
            Err(error) => return Err(error),
        }
        budget.check_boundary().await?;
        if context.cancellation.is_cancelled() {
            return Err(cancelled());
        }
        let mut snapshot = budget
            .store()
            .load(budget.scope(), budget.run_id())
            .await?
            .snapshot;
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(revision_error)?;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        budget
            .store()
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages: vec![],
                    events: vec![],
                    records: vec![record],
                },
            )
            .await?;
        budget.check_boundary().await?;
        Ok(())
    }

    pub(super) async fn inspect_route(
        &self,
        request: &ModelRequest,
        version_policy: VersionPolicy,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<crate::ModelRouteObservation, ContractError> {
        let (inspector, timeout) = self
            .inspector
            .as_ref()
            .ok_or_else(|| failure(ErrorCode::InvalidConfiguration, "model.inspector"))?;
        budget.check_boundary().await?;
        let deadline = tokio::time::Instant::now()
            .checked_add(*timeout)
            .ok_or_else(|| failure(ErrorCode::InvalidConfiguration, "model.inspection_timeout"))?
            .min(budget.call_deadline()?);
        let inspection = ModelInspectionContext {
            scope: budget.scope().clone(),
            principal_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
            cancellation: budget.cancellation().child_token(),
            deadline,
        };
        let _cancel = inspection.cancellation.clone().drop_guard();
        let observation = tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => return Err(failure(ErrorCode::ModelInspectionUnavailable, "model.inspection_timeout")),
            result = external(async { inspector.inspect(&request.route, &inspection).await }, context, budget, ErrorCode::ModelInspectionUnavailable) => result?,
        };
        inspection.cancellation.cancel();
        observation.validate(&request.route, version_policy)?;
        budget.check_boundary().await?;
        if context.cancellation.is_cancelled() {
            return Err(cancelled());
        }
        Ok(observation)
    }

    async fn authorize_route(
        &self,
        route: &ResolvedModelRoute,
        purpose: ModelPurpose,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<Guarded<()>, ContractError> {
        let request = PolicyRequest {
            owner_scope: budget.scope().clone(),
            resource_id: budget.run_id().clone(),
            action: PolicyAction::InvokeModel {
                route: Box::new(route.clone()),
                purpose,
            },
        };
        external(
            self.policy.guard(
                &request,
                context,
                Some(budget.call_deadline()?),
                None,
                || async { Ok(()) },
            ),
            context,
            budget,
            ErrorCode::PolicyUnavailable,
        )
        .await
    }
}

fn validate_projection(
    request: &ModelRequest,
    input: &RoutedModelInput,
    selection: &RouteSelection,
) -> Result<(), ContractError> {
    if request.request_id != input.model_step_id
        || request.route != selection.route
        || request.purpose != input.routing.purpose
        || request.options != input.routing.options
        || request.max_output_tokens != input.routing.max_output_tokens
    {
        return Err(failure(
            ErrorCode::ModelContextIncompatible,
            "routing.projection",
        ));
    }
    request.validate()
}

async fn external<T>(
    future: impl std::future::Future<Output = Result<T, ContractError>>,
    context: &ExecutionContext,
    budget: &RunBudget,
    code: ErrorCode,
) -> Result<T, ContractError> {
    tokio::select! {
        biased;
        _ = context.cancellation.cancelled() => Err(cancelled()),
        stopped = budget.wait_for_cancellation_or_deadline() => match stopped {
            Err(error) => Err(error), Ok(()) => Err(failure(ErrorCode::DeadlineExceeded, "model.routing")),
        },
        result = AssertUnwindSafe(future).catch_unwind() => result.map_err(|_| failure(code, "model.routing_callback"))?.map_err(|error| failure(error.code, "model.routing_callback")),
    }
}

fn failure(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}

pub(super) fn reason_code(reason: RouteSelectionReason) -> &'static str {
    match reason {
        RouteSelectionReason::Initial => "initial_route",
        RouteSelectionReason::Reuse => "saved_route_reuse",
        RouteSelectionReason::Fallback { failure } => match failure {
            ModelFailureKind::Timeout => "fallback_timeout",
            ModelFailureKind::RateLimited => "fallback_rate_limited",
            ModelFailureKind::Transport => "fallback_transport",
            ModelFailureKind::Protocol => "fallback_protocol",
            ModelFailureKind::ContextOverflow => "fallback_context_overflow",
            ModelFailureKind::Authentication => "fallback_authentication",
            ModelFailureKind::Unsupported => "fallback_unsupported",
            ModelFailureKind::Unavailable => "fallback_unavailable",
            ModelFailureKind::VersionDrift => "fallback_version_drift",
        },
    }
}
