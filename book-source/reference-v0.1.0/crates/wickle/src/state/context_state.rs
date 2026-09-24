use super::*;
use crate::{
    ContextDecision, ContextPlan, ContextRevision, ModelPurpose, context_strategy::records,
};

fn record(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    reference: &RecordRef,
) -> Result<ProtectedRecord, ContractError> {
    Ok(ProtectedRecord::new(
        reference.record_id.clone(),
        reference.revision,
        record_value(state, additions, reference)?.clone(),
    ))
}
pub(super) fn revision(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    reference: &RecordRef,
    snapshot: &RunSnapshot,
) -> Result<ContextRevision, ContractError> {
    let record = record(state, additions, reference)?;
    let value: ContextRevision = serde_json::from_value(record.value().clone())
        .map_err(|_| error(ErrorCode::InvalidSnapshot, "context.revision"))?;
    let plan = ContextPlan::restore(
        &self::record(state, additions, &value.plan_ref)?,
        &snapshot.profile,
    )?;
    let session = state
        .sessions
        .get(&snapshot.request.session_id)
        .ok_or_else(not_found)?;
    ContextRevision::restore(
        &record,
        &plan,
        &snapshot.scope,
        &snapshot.request.session_id,
        &session.messages,
    )
}
pub(super) fn validate_snapshot(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    let Some(reference) = &snapshot.context_plan_ref else {
        if snapshot.context_revision_ref.is_some() || !snapshot.context_decisions.is_empty() {
            return Err(error(ErrorCode::InvalidSnapshot, "context.plan_missing"));
        }
        return Ok(());
    };
    let plan = ContextPlan::restore(&record(state, additions, reference)?, &snapshot.profile)?;
    let active = snapshot
        .context_revision_ref
        .as_ref()
        .map(|reference| revision(state, additions, reference, snapshot))
        .transpose()?;
    if let Some(previous) = state.runs.get(&snapshot.run_id).map(|run| &run.snapshot) {
        if previous.context_revision_ref != snapshot.context_revision_ref {
            let active = active
                .as_ref()
                .ok_or_else(|| error(ErrorCode::InvalidTransition, "context.removal"))?;
            let session = state
                .sessions
                .get(&snapshot.request.session_id)
                .ok_or_else(not_found)?;
            if active.parent != previous.context_revision_ref
                || active.parent != session.snapshot.context_revision_ref
                || active.run_id != snapshot.run_id
                || Some(&active.model_step_id) != snapshot.model_step_id.as_ref()
                || &active.plan_ref != reference
                || active.through_sequence != session.snapshot.transcript_revision
            {
                return Err(error(ErrorCode::InvalidTransition, "context.adoption"));
            }
            if let Some(parent) = &active.parent {
                let parent = revision(state, additions, parent, snapshot)?;
                if parent
                    .covered_message_ids
                    .iter()
                    .any(|id| !active.covered_message_ids.contains(id))
                    || !active.previews.starts_with(&parent.previews)
                {
                    return Err(error(ErrorCode::InvalidTransition, "context.history_loss"));
                }
            }
            let parent = active
                .parent
                .as_ref()
                .map(|reference| revision(state, additions, reference, snapshot))
                .transpose()?;
            let changed_summary = active.summary.as_ref()
                != parent.as_ref().and_then(|parent| parent.summary.as_ref())
                || active.covered_message_ids.as_slice()
                    != parent
                        .as_ref()
                        .map_or(&[][..], |parent| parent.covered_message_ids.as_slice());
            if changed_summary
                && !snapshot.context_decisions.iter().any(|reference| {
                    event_record::<ContextDecision>(state, additions, reference).is_ok_and(
                        |decision| decision.revision_ref == snapshot.context_revision_ref,
                    )
                })
            {
                return Err(error(
                    ErrorCode::InvalidTransition,
                    "context.unrecorded_compaction",
                ));
            }
            if !changed_summary
                && active.previews.len()
                    == parent.as_ref().map_or(0, |parent| parent.previews.len())
            {
                return Err(error(
                    ErrorCode::InvalidTransition,
                    "context.empty_revision",
                ));
            }
        }
    }
    if snapshot.context_decisions.len() as u64 > plan.limits.max_compactions {
        return Err(error(ErrorCode::InvalidSnapshot, "context.decision_count"));
    }
    let mut requests = BTreeSet::new();
    for reference in &snapshot.context_decisions {
        let decision: ContextDecision =
            serde_json::from_value(record_value(state, additions, reference)?.clone())
                .map_err(|_| error(ErrorCode::InvalidSnapshot, "context.decision"))?;
        if decision.schema_version != "wickle.context-decision.v1"
            || decision.scope != snapshot.scope
            || decision.run_id != snapshot.run_id
            || decision.request.scope != snapshot.scope
            || decision.request.run_id != snapshot.run_id
            || decision.request.request_id != decision.request_id
            || decision.request.current_input != snapshot.request.input
            || decision.request_digest != crate::serialization::data_digest(&decision.request)
            || decision.revision_ref.is_some() == decision.failure.is_some()
            || !requests.insert(decision.request_id.clone())
        {
            return Err(error(
                ErrorCode::InvalidSnapshot,
                "context.decision_identity",
            ));
        }
        if snapshot.model_step_id.as_ref() != Some(&decision.model_step_id)
            && !snapshot.model_ledger.iter().any(|invocation| {
                invocation.purpose == ModelPurpose::Agent
                    && invocation.model_step_id == decision.model_step_id
            })
        {
            return Err(error(ErrorCode::InvalidSnapshot, "context.decision_step"));
        }
        let parent = decision
            .source_revision_ref
            .as_ref()
            .map(|reference| revision(state, additions, reference, snapshot))
            .transpose()?;
        if decision.request.previous_summary
            != parent.as_ref().and_then(|parent| parent.summary.clone())
            || parent
                .as_ref()
                .is_some_and(|parent| !decision.previews.starts_with(&parent.previews))
        {
            return Err(error(ErrorCode::InvalidSnapshot, "context.decision_parent"));
        }
        let session = state
            .sessions
            .get(&snapshot.request.session_id)
            .ok_or_else(not_found)?;
        let history: Vec<_> = session
            .messages
            .iter()
            .filter(|message| message.sequence.get() <= decision.through_sequence)
            .cloned()
            .collect();
        if history.last().map(|message| message.sequence.get()) != Some(decision.through_sequence) {
            return Err(error(
                ErrorCode::InvalidSnapshot,
                "context.decision_sequence",
            ));
        }
        records::validate_previews(&history, &decision.previews, &snapshot.scope)?;
        let covered = parent
            .as_ref()
            .map_or(&[][..], |parent| parent.covered_message_ids.as_slice());
        let view = records::transform(&history, covered, &decision.previews)?;
        let eligible = records::segments(&view, &snapshot.scope)?;
        if decision
            .request
            .segments
            .iter()
            .any(|segment| !eligible.contains(segment))
        {
            return Err(error(ErrorCode::InvalidSnapshot, "context.decision_source"));
        }
        let selected: Vec<_> = decision
            .request
            .segments
            .iter()
            .flat_map(|segment| segment.message_ids.clone())
            .collect();
        records::validate_selection(&view, &selected)?;
        if let Some(reference) = &decision.revision_ref {
            let applied = revision(state, additions, reference, snapshot)?;
            let all: Vec<_> = history
                .iter()
                .filter(|message| {
                    covered.contains(&message.message_id) || selected.contains(&message.message_id)
                })
                .map(|message| message.message_id.clone())
                .collect();
            if applied.parent != decision.source_revision_ref
                || applied.run_id != snapshot.run_id
                || applied.model_step_id != decision.model_step_id
                || applied.covered_message_ids != all
                || applied.previews != decision.previews
            {
                return Err(error(ErrorCode::InvalidSnapshot, "context.decision_result"));
            }
            if matches!(
                plan.compactor,
                Some(crate::context_strategy::CompactorIdentity::Model { .. })
            ) {
                let invocation = snapshot
                    .model_ledger
                    .iter()
                    .rev()
                    .find(|invocation| {
                        invocation.purpose == ModelPurpose::Compaction
                            && invocation.model_step_id == decision.request_id
                            && matches!(invocation.state, crate::ModelAttemptState::Completed {})
                    })
                    .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "context.model_evidence"))?;
                let response: crate::StoredModelResponse = event_record(
                    state,
                    additions,
                    invocation.response_ref.as_ref().ok_or_else(not_found)?,
                )?;
                if !matches!(response.outcome,crate::ModelExchangeOutcome::Completed {response} if response.finish==crate::ModelFinish::Stop&&Some(response.text.as_str())==applied.summary.as_deref())
                {
                    return Err(error(ErrorCode::InvalidSnapshot, "context.model_summary"));
                }
            }
        }
    }
    Ok(())
}
pub(super) fn validate_update(
    previous: &RunSnapshot,
    next: &RunSnapshot,
    events: &[RunEvent],
) -> Result<(), ContractError> {
    if previous.context_plan_ref != next.context_plan_ref
        || !next
            .context_decisions
            .starts_with(&previous.context_decisions)
        || next.context_decisions.len() > previous.context_decisions.len() + 1
    {
        return Err(error(
            ErrorCode::InvalidTransition,
            "context.plan_or_decisions",
        ));
    }
    let changes: Vec<_> = events
        .iter()
        .filter_map(|event| {
            if let RunEventPayload::ContextRewritten { revision_ref } = &event.payload {
                Some(revision_ref)
            } else {
                None
            }
        })
        .collect();
    if previous.context_revision_ref == next.context_revision_ref {
        if !changes.is_empty() {
            return Err(error(ErrorCode::InvalidEvent, "context.unchanged"));
        }
    } else if changes.as_slice()
        != next
            .context_revision_ref
            .iter()
            .collect::<Vec<_>>()
            .as_slice()
    {
        return Err(error(ErrorCode::InvalidEvent, "context.missing_event"));
    }
    Ok(())
}
pub(super) fn validate_history(
    state: &ScopeState,
    snapshot: &RunSnapshot,
    events: &[RunEvent],
) -> Result<(), ContractError> {
    let latest = events
        .iter()
        .filter_map(|event| {
            if let RunEventPayload::ContextRewritten { revision_ref } = &event.payload {
                Some(revision_ref)
            } else {
                None
            }
        })
        .next_back();
    if latest.is_some() && latest != snapshot.context_revision_ref.as_ref() {
        return Err(error(ErrorCode::InvalidSnapshot, "context.latest_event"));
    }
    let empty = BTreeMap::new();
    for event in events {
        if let RunEventPayload::ContextRewritten { revision_ref } = &event.payload {
            let value = revision(state, &empty, revision_ref, snapshot)?;
            if value.run_id != snapshot.run_id || value.session_id != snapshot.request.session_id {
                return Err(error(ErrorCode::InvalidEvent, "context.event_owner"));
            }
        }
    }
    Ok(())
}
pub(super) fn validate_session(
    state: &ScopeState,
    session: &SessionState,
) -> Result<(), ContractError> {
    let empty = BTreeMap::new();
    let mut current = session.snapshot.context_revision_ref.clone();
    let mut ancestors = BTreeSet::new();
    while let Some(reference) = current {
        if !ancestors.insert(reference.digest.clone()) {
            return Err(error(ErrorCode::InvalidSnapshot, "context.parent_cycle"));
        }
        let value: ContextRevision =
            serde_json::from_value(record_value(state, &empty, &reference)?.clone())
                .map_err(|_| error(ErrorCode::InvalidSnapshot, "context.session_revision"))?;
        if value.scope != session.snapshot.scope || value.session_id != session.snapshot.session_id
        {
            return Err(error(ErrorCode::InvalidSnapshot, "context.session_owner"));
        }
        current = value.parent;
    }
    for run in state
        .runs
        .values()
        .filter(|run| run.snapshot.request.session_id == session.snapshot.session_id)
    {
        if run
            .snapshot
            .context_revision_ref
            .as_ref()
            .is_some_and(|reference| !ancestors.contains(&reference.digest))
        {
            return Err(error(
                ErrorCode::InvalidSnapshot,
                "context.session_rollback",
            ));
        }
    }
    Ok(())
}
