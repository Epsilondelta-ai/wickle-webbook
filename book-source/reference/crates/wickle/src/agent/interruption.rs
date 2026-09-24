use super::*;
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;

impl Agent {
    pub(super) async fn interruption_plan(
        &self,
        snapshot: &RunSnapshot,
    ) -> Result<(RecordRef, InterruptionPlan), ContractError> {
        let reference = snapshot
            .interruption_plan_ref
            .as_ref()
            .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "interruption.plan"))?;
        let record = self
            .inner
            .bindings
            .state
            .read_record(&snapshot.scope, reference)
            .await?;
        let plan: InterruptionPlan = serde_json::from_value(record.value().clone())
            .map_err(|_| fail(ErrorCode::InvalidSnapshot, "interruption.plan"))?;
        plan.validate()?;
        Ok((reference.clone(), plan))
    }
    pub(super) async fn validate_interruption_binding(
        &self,
        snapshot: &RunSnapshot,
    ) -> Result<(), ContractError> {
        let (_, saved) = self.interruption_plan(snapshot).await?;
        let current = InterruptionPlan::capture(
            self.inner.bindings.interruption_policy.as_ref(),
            saved.timeout_ms,
        )?;
        if current.policy != saved.policy
            || current.configuration != saved.configuration
            || current.app_state_schema != saved.app_state_schema
            || current.timeout_ms != saved.timeout_ms
        {
            return Err(fail(
                ErrorCode::ComponentUnavailable,
                "interruption.pinned_policy",
            ));
        }
        Ok(())
    }
    pub(super) async fn interruption_decision(
        &self,
        saved: &StoredRun,
        cause: InterruptionCause,
        unresolved: &[RecordRef],
    ) -> Result<InterruptionDecisionRecord, ContractError> {
        let bindings = &self.inner.bindings;
        let (plan_ref, plan) = self.interruption_plan(&saved.snapshot).await?;
        let history = bindings
            .state
            .read_execution(&saved.snapshot.scope, &saved.snapshot.run_id)
            .await?;
        let segment = history
            .segments
            .last()
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "interruption.segment"))?;
        let interruption = InterruptionRecord {
            segment_id: segment.segment_id.clone(),
            cause,
            checkpoint_revision: saved.snapshot.revision,
            recoverable: matches!(
                cause,
                InterruptionCause::HostShutdown | InterruptionCause::SegmentStopped
            ) && saved.snapshot.validate().is_ok(),
            unresolved_effects: unresolved.to_vec(),
        };
        let info = InterruptionInfo {
            scope: saved.snapshot.scope.clone(),
            run_id: saved.snapshot.run_id.clone(),
            phase: saved.snapshot.phase,
            store_capabilities: bindings.state.capabilities(),
            configuration: plan.configuration.clone(),
            interruption: interruption.clone(),
            app_state: saved.snapshot.app_state.clone(),
        };
        let mut record = InterruptionDecisionRecord {
            schema_version: "wickle.interruption-decision.v1".into(),
            scope: saved.snapshot.scope.clone(),
            run_id: saved.snapshot.run_id.clone(),
            plan_ref,
            interruption,
            action: InterruptionAction::UseDefault,
            app_state: saved.snapshot.app_state.clone(),
            callback_error: None,
        };
        let implementation = if plan.policy == InterruptionPlan::default_identity() {
            None
        } else {
            bindings.interruption_policy.as_ref().filter(|binding| {
                std::panic::catch_unwind(AssertUnwindSafe(|| binding.policy.identity()))
                    .is_ok_and(|identity| identity == plan.policy)
                    && binding.configuration == plan.configuration
                    && binding.app_state_schema == plan.app_state_schema
            })
        };
        if plan.policy != InterruptionPlan::default_identity() && implementation.is_none() {
            record.callback_error = Some(Id::new("component_unavailable")?);
        }
        if let Some(binding) = implementation {
            let timeout = Duration::from_millis(
                plan.timeout_ms
                    .min(bindings.settings.interruption_timeout_ms),
            );
            let result = tokio::time::timeout(
                timeout,
                AssertUnwindSafe(async { binding.policy.decide(&info).await }).catch_unwind(),
            )
            .await;
            match result {
                Ok(Ok(Ok(decision))) => {
                    let protected = matches!(
                        cause,
                        InterruptionCause::UserCancel
                            | InterruptionCause::BudgetExhausted
                            | InterruptionCause::OwnershipLost
                    );
                    if (protected && decision.action != InterruptionAction::UseDefault)
                        || (decision.action == InterruptionAction::Pause
                            && !record.interruption.recoverable)
                        || decision
                            .app_state
                            .as_ref()
                            .is_some_and(|state| plan.validate_app_state(state).is_err())
                    {
                        record.callback_error = Some(Id::new("invalid_decision")?);
                    } else {
                        record.action = decision.action;
                        if decision.app_state.is_some() {
                            record.app_state = decision.app_state;
                        }
                    }
                }
                Err(_) => record.callback_error = Some(Id::new("callback_timeout")?),
                Ok(Err(_)) => record.callback_error = Some(Id::new("callback_panic")?),
                Ok(Ok(Err(_))) => record.callback_error = Some(Id::new("callback_error")?),
            }
        }
        Ok(record)
    }
}
pub(super) fn interruption_result(
    decision: &InterruptionDecisionRecord,
    original: &OutcomeResult,
) -> OutcomeResult {
    match decision.interruption.cause {
        InterruptionCause::UserCancel | InterruptionCause::BudgetExhausted => original.clone(),
        InterruptionCause::OwnershipLost => original.clone(), // No driver may commit after this cause.
        InterruptionCause::RecoveryUnavailable => super::driver::failed("recovery_unavailable"),
        InterruptionCause::HostShutdown | InterruptionCause::SegmentStopped => {
            match decision.action {
                InterruptionAction::Cancel => OutcomeResult::Cancelled {
                    reason: "execution_stop_policy".into(),
                },
                InterruptionAction::Fail => super::driver::failed("execution_stop_policy"),
                InterruptionAction::Pause | InterruptionAction::UseDefault
                    if decision.interruption.recoverable =>
                {
                    OutcomeResult::Interrupted {
                        interruption: decision.interruption.clone(),
                    }
                }
                _ => super::driver::failed("recovery_unavailable"),
            }
        }
    }
}
