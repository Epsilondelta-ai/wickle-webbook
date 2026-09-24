use super::*;
use crate::*;
fn invalid(path: &str) -> ContractError {
    error(ErrorCode::InvalidSnapshot, path)
}

pub(super) fn validate(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    let Some(reference) = &snapshot.interruption_plan_ref else {
        if snapshot.app_state.is_some()
            || !snapshot.interruption_records.is_empty()
            || snapshot.status == RunStatus::Interrupted
        {
            return Err(invalid("interruption.plan_missing"));
        }
        return Ok(());
    };
    let plan: InterruptionPlan = event_record(state, additions, reference)?;
    plan.validate()?;
    if let Some(app_state) = &snapshot.app_state {
        plan.validate_app_state(app_state)?;
    }
    let mut seen = BTreeSet::new();
    let mut last = None;
    for reference in &snapshot.interruption_records {
        if !seen.insert(record_key(reference)) {
            return Err(invalid("interruption.duplicate"));
        }
        let record: InterruptionDecisionRecord = event_record(state, additions, reference)?;
        if record.schema_version != "wickle.interruption-decision.v1"
            || record.scope != snapshot.scope
            || record.run_id != snapshot.run_id
            || Some(&record.plan_ref) != snapshot.interruption_plan_ref.as_ref()
            || record.interruption.checkpoint_revision >= snapshot.revision
        {
            return Err(invalid("interruption.record"));
        }
        if let Some(app_state) = &record.app_state {
            plan.validate_app_state(app_state)?;
        }
        if matches!(
            record.interruption.cause,
            InterruptionCause::UserCancel
                | InterruptionCause::BudgetExhausted
                | InterruptionCause::OwnershipLost
        ) && record.action != InterruptionAction::UseDefault
        {
            return Err(invalid("interruption.protected_cause"));
        }
        for reference in &record.interruption.unresolved_effects {
            let result: ToolResult = event_record(state, additions, reference)?;
            if result.status != ToolResultStatus::Unknown || result.effect != ToolEffect::Unknown {
                return Err(invalid("interruption.effect_reference"));
            }
            validate_tool_pair(state, snapshot, &[], &result)?;
        }
        if let Some(history) = state.executions.get(&snapshot.run_id) {
            if !history.segments.iter().any(|segment| {
                segment.segment_id == record.interruption.segment_id
                    && segment.accepted_revision <= record.interruption.checkpoint_revision
            }) {
                return Err(invalid("interruption.segment"));
            }
        }
        let segment_outcome = state
            .executions
            .get(&snapshot.run_id)
            .and_then(|history| {
                history
                    .segments
                    .iter()
                    .find(|segment| segment.segment_id == record.interruption.segment_id)
            })
            .and_then(|segment| match &segment.outcome {
                Some(SegmentOutcome::Settled { outcome }) => Some(outcome.as_ref()),
                _ => None,
            });
        let outcome = segment_outcome
            .or_else(|| {
                snapshot.outcome.as_ref().filter(|outcome| {
                    record.interruption.checkpoint_revision.checked_add(1)
                        == Some(outcome.checkpoint_revision)
                })
            })
            .ok_or_else(|| invalid("interruption.segment_outcome"))?;
        validate_result(&record, outcome, &snapshot.limits)?;
        last = Some(record);
    }
    if let Some(last) = &last {
        if snapshot.app_state != last.app_state {
            return Err(invalid("interruption.app_state"));
        }
    }
    if let Some(RunOutcome {
        result: OutcomeResult::Interrupted { interruption },
        ..
    }) = &snapshot.outcome
    {
        let last = last.ok_or_else(|| invalid("interruption.record_missing"))?;
        if &last.interruption != interruption
            || !matches!(
                last.action,
                InterruptionAction::Pause | InterruptionAction::UseDefault
            )
        {
            return Err(invalid("interruption.outcome"));
        }
    }
    Ok(())
}
pub(super) fn validate_event(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    outcome: &RunOutcome,
    decision_ref: &RecordRef,
) -> Result<(), ContractError> {
    outcome.validate()?;
    let OutcomeResult::Interrupted { interruption } = &outcome.result else {
        return Err(invalid("interruption.event_outcome"));
    };
    let decision: InterruptionDecisionRecord = event_record(state, additions, decision_ref)?;
    if !snapshot.interruption_records.contains(decision_ref)
        || decision.interruption != *interruption
        || decision.app_state != outcome.app_state
        || decision.scope != snapshot.scope
        || decision.run_id != snapshot.run_id
    {
        return Err(invalid("interruption.event"));
    }
    Ok(())
}

fn validate_result(
    decision: &InterruptionDecisionRecord,
    outcome: &RunOutcome,
    limits: &RunLimits,
) -> Result<(), ContractError> {
    outcome.validate()?;
    if decision.interruption.checkpoint_revision.checked_add(1) != Some(outcome.checkpoint_revision)
        || decision.app_state != outcome.app_state
        || decision.interruption.unresolved_effects != outcome.unresolved_effects
        || decision.interruption.recoverable
            != matches!(outcome.result, OutcomeResult::Interrupted { .. })
    {
        return Err(invalid("interruption.result_identity"));
    }
    let allowed = match decision.interruption.cause {
        InterruptionCause::UserCancel => {
            decision.action == InterruptionAction::UseDefault
                && matches!(outcome.result, OutcomeResult::Cancelled { .. })
        }
        InterruptionCause::BudgetExhausted => {
            decision.action == InterruptionAction::UseDefault
                && match outcome.result {
                    OutcomeResult::Exhausted {
                        budget: BudgetKind::ModelCalls,
                    } => outcome.usage.model_calls >= limits.max_model_calls.get(),
                    OutcomeResult::Exhausted {
                        budget: BudgetKind::ToolAttempts,
                    } => outcome.usage.tool_attempts >= limits.max_tool_attempts,
                    OutcomeResult::Exhausted {
                        budget: BudgetKind::RepairAttempts,
                    } => outcome.usage.repair_attempts >= limits.max_repair_attempts,
                    OutcomeResult::Exhausted {
                        budget: BudgetKind::RecoveryAttempts,
                    } => outcome.usage.recovery_attempts >= limits.max_recovery_attempts,
                    OutcomeResult::Exhausted {
                        budget: BudgetKind::Elapsed,
                    } => outcome.usage.elapsed_ms >= limits.max_elapsed_ms.get(),
                    _ => false,
                }
        }
        InterruptionCause::OwnershipLost => false,
        InterruptionCause::RecoveryUnavailable => {
            matches!(&outcome.result, OutcomeResult::Failed { failure } if failure.code.as_str() == "recovery_unavailable")
        }
        InterruptionCause::HostShutdown | InterruptionCause::SegmentStopped => {
            match decision.action {
                InterruptionAction::UseDefault | InterruptionAction::Pause
                    if decision.interruption.recoverable =>
                {
                    matches!(&outcome.result, OutcomeResult::Interrupted { interruption } if interruption == &decision.interruption)
                }
                InterruptionAction::UseDefault => {
                    matches!(&outcome.result, OutcomeResult::Failed { failure } if failure.code.as_str() == "recovery_unavailable")
                }
                InterruptionAction::Cancel => {
                    matches!(&outcome.result, OutcomeResult::Cancelled { reason } if reason == "execution_stop_policy")
                }
                InterruptionAction::Fail => {
                    matches!(&outcome.result, OutcomeResult::Failed { failure } if failure.code.as_str() == "execution_stop_policy")
                }
                _ => false,
            }
        }
    };
    if !allowed {
        return Err(invalid("interruption.result_cause"));
    }
    Ok(())
}

pub(super) fn validate_history_events(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    events: &[RunEvent],
) -> Result<(), ContractError> {
    for reference in &snapshot.interruption_records {
        let decision: InterruptionDecisionRecord = event_record(state, additions, reference)?;
        let mut matches = vec![];
        for event in events {
            let outcome_ref = match &event.payload {
                RunEventPayload::RunInterrupted {
                    outcome_ref,
                    decision_ref,
                } if decision_ref == reference => Some(outcome_ref),
                RunEventPayload::RunFinished { outcome_ref } => {
                    let outcome: RunOutcome = event_record(state, additions, outcome_ref)?;
                    (decision.interruption.checkpoint_revision.checked_add(1)
                        == Some(outcome.checkpoint_revision))
                    .then_some(outcome_ref)
                }
                _ => None,
            };
            if let Some(outcome_ref) = outcome_ref {
                let outcome: RunOutcome = event_record(state, additions, outcome_ref)?;
                validate_result(&decision, &outcome, &snapshot.limits)?;
                matches.push(event.seq.get());
            }
        }
        if matches.len() != 1 {
            return Err(invalid("interruption.event_coverage"));
        }
        let through = matches[0];
        let mut uncertain: BTreeMap<Id, Vec<RecordRef>> = BTreeMap::new();
        for event in events.iter().filter(|event| event.seq.get() <= through) {
            match &event.payload {
                RunEventPayload::ToolUnresolved { result_ref, .. } => {
                    let result: ToolResult = event_record(state, additions, result_ref)?;
                    uncertain
                        .entry(result.call_id)
                        .or_default()
                        .push(result_ref.clone());
                }
                RunEventPayload::ToolSettled { result_ref } => {
                    let result: ToolResult = event_record(state, additions, result_ref)?;
                    if result.effect != ToolEffect::Unknown
                        && result.status != ToolResultStatus::Unknown
                    {
                        uncertain.remove(&result.call_id);
                    }
                }
                _ => {}
            }
        }
        let expected: BTreeSet<_> = uncertain.values().flatten().map(record_key).collect();
        let actual: BTreeSet<_> = decision
            .interruption
            .unresolved_effects
            .iter()
            .map(record_key)
            .collect();
        if actual != expected || actual.len() != decision.interruption.unresolved_effects.len() {
            return Err(invalid("interruption.effect_evidence"));
        }
    }
    Ok(())
}
