use std::{panic::AssertUnwindSafe, sync::Arc};

use futures_util::FutureExt;

mod prepared;
mod routed;
pub use routed::{
    ModelProjectionContext, ModelRequestProjector, ProjectedModelRequest, RoutedModelInput,
};

use crate::{
    CommitInput, ContractError, ErrorCode, ExecutionContext, Guarded, Id, ModelAttemptState,
    ModelCallContext, ModelFailureKind, ModelPort, ModelProtocolError, ModelRequest, ModelResponse,
    ModelResponseMetadata, PolicyAction, PolicyGate, PolicyRequest, ProtectedRecord,
    ReservationKind, RunBudget, collect_model_response,
};

/// Explicit, bounded retries within the same already selected route. Router
/// fallback and context reduction are separate operations owned by the driver.
#[derive(Debug, Clone, Copy, Default)]
pub struct ModelRetryPolicy {
    /// Additional physical requests after the first; zero disables retries.
    pub max_retries: u32,
    /// Backoff on the run's injected clock, bounded by its original deadline.
    pub backoff_ms: u64,
}

/// Complete response or a classified failure with bounded partial text. Failed
/// attempts never publish a partially assembled tool plan through this value.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelExchangeOutcome {
    /// The full stream ended with a valid completion.
    Completed {
        /// Unexecuted proposals; the driver must persist and validate tool plans.
        response: ModelResponse,
    },
    /// No complete response was accepted after the configured recovery allowance.
    Failed {
        /// Safe classification, optional reported usage, and bounded partial text.
        failure: ModelProtocolError,
    },
}

/// Protected response body tied to one physical attempt and exact route. Reading
/// it requires Store authorization; it is never an automatic public run view.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredModelResponse {
    /// The physical model request/attempt identifier.
    pub request_id: Id,
    /// Route that produced the success or failure.
    pub route_digest: crate::JsonDigest,
    /// Accepted response or bounded failed-response data.
    pub outcome: ModelExchangeOutcome,
}

/// Core-owned model execution boundary. Each physical attempt is visible in the
/// budget and invocation ledger; an adapter still performs exactly one request.
/// This does not run tools, choose fallback routes, or advance an agent loop.
pub struct ModelExchange {
    models: Models,
    policy: Arc<PolicyGate>,
    retry: ModelRetryPolicy,
    inspector: Option<(Arc<dyn crate::ModelRouteInspector>, std::time::Duration)>,
}

enum Models {
    Single(Arc<dyn ModelPort>),
    Dispatcher(Arc<dyn crate::ModelDispatcher>),
}

impl ModelExchange {
    /// Bind one adapter and current policy gate, with physical retries disabled.
    pub fn new(model: Arc<dyn ModelPort>, policy: Arc<PolicyGate>) -> Self {
        Self {
            models: Models::Single(model),
            policy,
            retry: ModelRetryPolicy::default(),
            inspector: None,
        }
    }

    /// Use an exact scoped adapter registry without inventing a shared credential binding.
    pub fn with_dispatcher(
        dispatcher: Arc<dyn crate::ModelDispatcher>,
        policy: Arc<PolicyGate>,
    ) -> Self {
        Self {
            models: Models::Dispatcher(dispatcher),
            policy,
            retry: ModelRetryPolicy::default(),
            inspector: None,
        }
    }

    /// Require bounded current metadata inspection for routed calls and their retries.
    pub fn with_route_inspector(
        mut self,
        inspector: Arc<dyn crate::ModelRouteInspector>,
        timeout: std::time::Duration,
    ) -> Result<Self, ContractError> {
        if timeout.is_zero() {
            return Err(ContractError::new(
                ErrorCode::InvalidConfiguration,
                "model.inspection_timeout",
            ));
        }
        self.inspector = Some((inspector, timeout));
        Ok(self)
    }

    /// Configure finite same-route recovery. Run model/recovery limits still apply.
    pub fn with_retry_policy(mut self, retry: ModelRetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Validate and authorize each attempt, persist its reservation and selected
    /// route, then collect exactly one adapter stream. A retry uses a new physical
    /// request/attempt ID while retaining the caller's logical request ID as the
    /// model step. The driver separately stores the accepted response/transcript
    /// and tool plans before dispatching any proposed tool. Complete and failed
    /// responses are retained under each invocation's protected response_ref, so
    /// a later budget/cancellation error does not discard prior partial output.
    pub async fn generate(
        &self,
        request: &ModelRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<Guarded<ModelExchangeOutcome>, ContractError> {
        if budget
            .store()
            .load(budget.scope(), budget.run_id())
            .await?
            .snapshot
            .routing_snapshot_ref
            .is_some()
        {
            return Err(ContractError::new(
                ErrorCode::ModelRoutingMismatch,
                "model.routing_required",
            ));
        }
        self.generate_inner(request, context, budget, None, None)
            .await
    }

    async fn generate_inner(
        &self,
        request: &ModelRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
        routed: Option<(&crate::RouteSelection, crate::VersionPolicy)>,
        context_use: Option<routed::ContextUseGate<'_>>,
    ) -> Result<Guarded<ModelExchangeOutcome>, ContractError> {
        for retry_number in 0..=self.retry.max_retries {
            let model = self.resolve_model(request, context, budget)?;
            budget.check_boundary().await?;
            if let Guarded::ApprovalRequired(challenge) =
                self.authorize(request, context, budget).await?
            {
                return Ok(Guarded::ApprovalRequired(challenge));
            }
            if let Some(gate) = &context_use {
                gate.check(context, budget).await?;
            }
            let observation = if let Some((_, version_policy)) = routed {
                Some(
                    self.inspect_route(request, version_policy, context, budget)
                        .await?,
                )
            } else {
                None
            };
            let reason = Id::new(if retry_number != 0 {
                "same_route_retry"
            } else if let Some((selection, _)) = routed {
                routed::reason_code(selection.reason)
            } else {
                "requested_route"
            })?;
            let reservation = crate::future::boxed(|| {
                budget.reserve_model(
                    request,
                    context_use.as_ref().map(|gate| gate.configuration),
                    context_use.as_ref().map(|gate| gate.prepared_step_ref),
                    reason,
                    observation,
                )
            })
            .await?;
            let mut physical_request = request.clone();
            physical_request.request_id = reservation.attempt_id.clone();
            // Neither a saved reservation nor earlier authorization grants lasting
            // permission. Check the physical request and current policy again.
            self.validate(&physical_request, context, budget, model.as_ref())?;
            if let Guarded::ApprovalRequired(challenge) =
                self.authorize(&physical_request, context, budget).await?
            {
                return Ok(Guarded::ApprovalRequired(challenge));
            }
            if let Some(gate) = &context_use {
                gate.check(context, budget).await?;
            }
            budget.check_boundary().await?;
            if context.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let cancellation = budget.cancellation().child_token();
            let _cancel_adapter_on_drop = cancellation.clone().drop_guard();
            let call_context = ModelCallContext {
                attempt_id: reservation.attempt_id.clone(),
                run_id: budget.run_id().clone(),
                scope: context.data.scope.clone(),
                cancellation,
                deadline: budget.call_deadline()?,
            };
            // Construct the stream only after all entry checks. The adapter's
            // lifetime ends with this attempt; it must not spawn untracked retries.
            let attempt = AssertUnwindSafe(async {
                collect_model_response(
                    &physical_request,
                    model.generate(&physical_request, &call_context),
                )
                .await
            })
            .catch_unwind();
            let result = tokio::select! {
                biased;
                _ = context.cancellation.cancelled() => Err(cancelled()),
                stopped = budget.wait_for_cancellation_or_deadline() => {
                    match stopped {
                        Err(error) => Err(error),
                        Ok(()) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "model.deadline")),
                    }
                }
                response = attempt => response.map_err(|_| ContractError::new(ErrorCode::InvalidContract, "model.adapter")),
            };
            // Signal adapter shutdown before any potentially slow settlement I/O.
            call_context.cancellation.cancel();
            let response = match result {
                Ok(response) => response,
                Err(error) => {
                    self.record_end(
                        budget,
                        &reservation.attempt_id,
                        ModelAttemptState::Unknown {},
                        &ModelResponseMetadata::default(),
                        None,
                    )
                    .await?;
                    return Err(error);
                }
            };
            let (state, metadata) = match &response {
                Ok(response) => (ModelAttemptState::Completed {}, &response.metadata),
                Err(failure) => (
                    ModelAttemptState::Failed { kind: failure.kind },
                    failure.metadata.as_ref(),
                ),
            };
            let stored_response = StoredModelResponse {
                request_id: reservation.attempt_id.clone(),
                route_digest: physical_request.route.digest(),
                outcome: match &response {
                    Ok(response) => ModelExchangeOutcome::Completed {
                        response: response.clone(),
                    },
                    Err(failure) => ModelExchangeOutcome::Failed {
                        failure: failure.clone(),
                    },
                },
            };
            let response_record = ProtectedRecord::new(
                Id::new(format!("model-response-{}", reservation.attempt_id))?,
                1,
                serde_json::to_value(&stored_response).map_err(|_| revision_error())?,
            );
            self.record_end(
                budget,
                &reservation.attempt_id,
                state,
                metadata,
                Some(response_record),
            )
            .await?;
            if context.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            budget.check_boundary().await?;
            match response {
                Ok(response) => {
                    return Ok(Guarded::Completed(ModelExchangeOutcome::Completed {
                        response,
                    }));
                }
                Err(failure) => {
                    if retry_number == self.retry.max_retries || !recoverable(failure.kind) {
                        return Ok(Guarded::Completed(ModelExchangeOutcome::Failed { failure }));
                    }
                    budget.reserve(ReservationKind::Recovery {}).await?;
                    tokio::select! {
                        biased;
                        _ = context.cancellation.cancelled() => return Err(cancelled()),
                        result = budget.backoff(self.retry.backoff_ms) => result?,
                    }
                }
            }
        }
        unreachable!("a finite attempt loop always returns its last result")
    }

    fn validate(
        &self,
        request: &ModelRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
        model: &dyn ModelPort,
    ) -> Result<(), ContractError> {
        if &context.data.scope != budget.scope() {
            return Err(ContractError::new(ErrorCode::AccessDenied, "scope"));
        }
        request.validate()?;
        if !model.binding().matches_route(&request.route) {
            return Err(ContractError::new(
                ErrorCode::InvalidReference,
                "model.binding",
            ));
        }
        Ok(())
    }

    fn resolve_route(
        &self,
        scope: &crate::Scope,
        route: &crate::ResolvedModelRoute,
    ) -> Result<Arc<dyn ModelPort>, ContractError> {
        let port = std::panic::catch_unwind(AssertUnwindSafe(|| match &self.models {
            Models::Single(model) => Ok(model.clone()),
            Models::Dispatcher(dispatcher) => dispatcher.resolve(scope, route),
        }))
        .map_err(|_| ContractError::new(ErrorCode::ComponentUnavailable, "model.dispatcher"))??;
        if !port.binding().matches_route(route) {
            return Err(ContractError::new(
                ErrorCode::ModelBindingInvalid,
                "model.binding",
            ));
        }
        Ok(port)
    }
    fn resolve_model(
        &self,
        request: &ModelRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<Arc<dyn ModelPort>, ContractError> {
        if &context.data.scope != budget.scope() {
            return Err(ContractError::new(ErrorCode::AccessDenied, "scope"));
        }
        let model = std::panic::catch_unwind(AssertUnwindSafe(|| match &self.models {
            Models::Single(model) => Ok(model.clone()),
            Models::Dispatcher(dispatcher) => dispatcher.resolve(budget.scope(), &request.route),
        }))
        .map_err(|_| ContractError::new(ErrorCode::ComponentUnavailable, "model.dispatcher"))??;
        self.validate(request, context, budget, model.as_ref())?;
        Ok(model)
    }

    async fn authorize(
        &self,
        request: &ModelRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<Guarded<()>, ContractError> {
        let policy_request = PolicyRequest {
            owner_scope: budget.scope().clone(),
            resource_id: budget.run_id().clone(),
            action: PolicyAction::InvokeModel {
                route: Box::new(request.route.clone()),
                purpose: request.purpose,
            },
        };
        tokio::select! {
            biased;
            stopped = budget.wait_for_cancellation_or_deadline() => match stopped {
                Err(error) => Err(error),
                Ok(()) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "model.policy")),
            },
            decision = self.policy.guard(&policy_request, context, Some(budget.call_deadline()?), None, || async { Ok(()) }) => decision,
        }
    }

    async fn record_end(
        &self,
        budget: &RunBudget,
        attempt_id: &Id,
        state: ModelAttemptState,
        metadata: &ModelResponseMetadata,
        response_record: Option<ProtectedRecord>,
    ) -> Result<(), ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let invocation = snapshot
            .model_ledger
            .iter_mut()
            .find(|entry| &entry.attempt_id == attempt_id)
            .ok_or_else(|| ContractError::new(ErrorCode::InvalidSnapshot, "model.attempt"))?;
        invocation.state = state;
        invocation.response_ref = response_record
            .as_ref()
            .map(|record| record.reference().clone());
        invocation.provider_request_id = metadata.provider_request_id.clone();
        invocation.reported_model_id = metadata.reported_model_id.clone();
        invocation.reported_model_version = metadata.reported_model_version.clone();
        invocation.usage = metadata.usage.clone();
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
                    control_commands: vec![],
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages: vec![],
                    events: vec![],
                    records: response_record.into_iter().collect(),
                },
            )
            .await?;
        Ok(())
    }
}

fn cancelled() -> ContractError {
    ContractError::new(ErrorCode::Cancelled, "model")
}
fn revision_error() -> ContractError {
    ContractError::new(ErrorCode::RevisionConflict, "model.ledger")
}
fn recoverable(kind: ModelFailureKind) -> bool {
    matches!(
        kind,
        ModelFailureKind::Timeout
            | ModelFailureKind::RateLimited
            | ModelFailureKind::Transport
            | ModelFailureKind::Protocol
    )
}
