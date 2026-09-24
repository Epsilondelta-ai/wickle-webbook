use super::*;
use crate::verification::VerificationRecord;
use crate::{InputContent, MessageRole};
use crate::{VerificationCandidate, VerificationDecision, VerificationPlan};

fn invalid() -> ContractError {
    error(ErrorCode::InvalidSnapshot, "verification.records")
}
fn plan(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<Option<VerificationPlan>, ContractError> {
    snapshot
        .verification_plan_ref
        .as_ref()
        .map(|reference| {
            let record = ProtectedRecord::new(
                reference.record_id.clone(),
                reference.revision,
                record_value(state, additions, reference)?.clone(),
            );
            VerificationPlan::restore(&record, snapshot)
        })
        .transpose()
}
fn candidate(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    plan: &VerificationPlan,
    reference: &RecordRef,
) -> Result<VerificationCandidate, ContractError> {
    let candidate: VerificationCandidate = event_record(state, additions, reference)?;
    if candidate.scope != snapshot.scope || candidate.run_id != snapshot.run_id {
        return Err(invalid());
    }
    let session = state
        .sessions
        .get(&snapshot.request.session_id)
        .ok_or_else(invalid)?;
    let history: Vec<_> = session
        .messages
        .iter()
        .filter(|message| message.sequence.get() <= candidate.through_sequence)
        .collect();
    if history.last().map(|message| message.sequence.get()) != Some(candidate.through_sequence) {
        return Err(invalid());
    }
    let evidence: Vec<_> = if plan.verifier.is_some() {
        history
            .iter()
            .filter(|message| {
                message.run_id == snapshot.run_id
                    && message
                        .content
                        .iter()
                        .any(|block| matches!(block, ContentBlock::ToolResult { .. }))
            })
            .map(|message| message.message_id.clone())
            .collect()
    } else {
        vec![]
    };
    if candidate.evidence_message_ids != evidence {
        return Err(invalid());
    }
    let invocation = snapshot
        .model_ledger
        .iter()
        .find(|entry| {
            entry.purpose == crate::ModelPurpose::Agent
                && entry.model_step_id == candidate.model_step_id
                && entry.response_ref.as_ref() == Some(&candidate.response_ref)
        })
        .ok_or_else(invalid)?;
    if !matches!(invocation.state, crate::ModelAttemptState::Completed {}) {
        return Err(invalid());
    }
    let stored: crate::StoredModelResponse =
        event_record(state, additions, &candidate.response_ref)?;
    let crate::ModelExchangeOutcome::Completed { response } = stored.outcome else {
        return Err(invalid());
    };
    if response.finish != crate::ModelFinish::Stop || !response.tool_calls.is_empty() {
        return Err(invalid());
    }
    let (output, format_error) = match plan.parse(&response.text) {
        Ok(output) => (output, None),
        Err(error) => (
            vec![InputContent::Text {
                text: response.text,
            }],
            Some(error),
        ),
    };
    if candidate.output != output || candidate.format_error != format_error {
        return Err(invalid());
    }
    Ok(candidate)
}
pub(super) fn validate(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    let Some(plan) = plan(state, additions, snapshot)? else {
        if snapshot.candidate_ref.is_some() || !snapshot.verification_records.is_empty() {
            return Err(invalid());
        }
        return Ok(());
    };
    let active = snapshot
        .candidate_ref
        .as_ref()
        .map(|reference| candidate(state, additions, snapshot, &plan, reference))
        .transpose()?;
    let mut seen = std::collections::BTreeSet::new();
    let mut latest: Option<VerificationRecord> = None;
    let mut repairs = std::collections::BTreeSet::new();
    for reference in &snapshot.verification_records {
        if !seen.insert(reference.digest.clone()) {
            return Err(invalid());
        }
        let result: VerificationRecord = event_record(state, additions, reference)?;
        let source = candidate(state, additions, snapshot, &plan, &result.candidate_ref)?;
        if result.schema_version != "wickle.verification-record.v1"
            || result.scope != snapshot.scope
            || result.run_id != snapshot.run_id
            || result.decision.is_some() == result.error.is_some()
        {
            return Err(invalid());
        }
        if let Some(summary) = &result.summary {
            let definition = plan.verifier.as_ref().ok_or_else(invalid)?;
            let stored: VerificationSummary = event_record(
                state,
                additions,
                result.summary_ref.as_ref().ok_or_else(invalid)?,
            )?;
            if stored != *summary
                || summary.verifier_ref != definition.verifier_ref
                || summary.criteria_ref != definition.criteria_ref
                || source.format_error.is_some()
                || result.decision.as_ref().map(|decision| decision.verdict())
                    != Some(summary.verdict)
                || summary.evidence != vec![result.candidate_ref.clone()]
            {
                return Err(invalid());
            }
        } else if result.summary_ref.is_some()
            || result.error.is_none() && source.format_error.is_none() && plan.verifier.is_some()
        {
            return Err(invalid());
        }
        if source.format_error.is_some()
            && !matches!(result.decision, Some(VerificationDecision::Revise { .. }))
        {
            return Err(invalid());
        }
        if let Some(command_ref) = &result.review_command_ref {
            let receipt = snapshot
                .resume_receipts
                .iter()
                .find(|receipt| &receipt.command_ref == command_ref && !receipt.expired)
                .ok_or_else(invalid)?;
            let expected = ApprovalTarget::Candidate {
                candidate_ref: result.candidate_ref.clone(),
                verifier_ref: plan
                    .verifier
                    .as_ref()
                    .ok_or_else(invalid)?
                    .verifier_ref
                    .clone(),
            };
            match (&receipt.command.action, &result.decision) {
                (ResumeAction::Approve { target, .. }, Some(VerificationDecision::Pass {}))
                    if target == &expected => {}
                (
                    ResumeAction::Deny { target, reason, .. },
                    Some(VerificationDecision::Fail { reason: actual }),
                ) if target == &expected && actual == reason => {}
                _ => return Err(invalid()),
            }
            if !latest.as_ref().is_some_and(|prior| {
                prior.candidate_ref == result.candidate_ref
                    && matches!(prior.decision, Some(VerificationDecision::Wait { .. }))
            }) {
                return Err(invalid());
            }
        }
        if let Some(repair) = &result.repair_ref {
            if !repairs.insert(repair.clone()) {
                return Err(invalid());
            }
            if !snapshot.reservations.iter().any(|reservation| {
                &reservation.attempt_id == repair
                    && matches!(reservation.kind, crate::ReservationKind::Repair {})
            }) || !matches!(result.decision, Some(VerificationDecision::Revise { .. }))
                || !latest.as_ref().is_some_and(|prior| {
                    prior.candidate_ref == result.candidate_ref
                        && prior.decision == result.decision
                        && prior.repair_ref.is_none()
                })
            {
                return Err(invalid());
            }
        }
        latest = Some(result);
    }
    if let Some(wait) = &snapshot.wait {
        if let WaitTarget::Approval {
            target:
                ApprovalTarget::Candidate {
                    candidate_ref,
                    verifier_ref,
                },
        } = &wait.target
        {
            if snapshot.candidate_ref.as_ref() != Some(candidate_ref)
                || plan.verifier.as_ref().map(|v| &v.verifier_ref) != Some(verifier_ref)
                || !latest.as_ref().is_some_and(|result| {
                    result.candidate_ref == *candidate_ref
                        && matches!(result.decision, Some(VerificationDecision::Wait { .. }))
                })
            {
                return Err(invalid());
            }
        }
    }
    if let Some(outcome) = &snapshot.outcome {
        if matches!(outcome.result, OutcomeResult::Succeeded { .. }) {
            let active = active.as_ref().ok_or_else(invalid)?;
            let result = latest.as_ref().ok_or_else(invalid)?;
            if Some(&result.candidate_ref) != snapshot.candidate_ref.as_ref()
                || !matches!(result.decision, Some(VerificationDecision::Pass {}))
                || active.format_error.is_some()
                || outcome.output != active.output
                || outcome.verification != result.summary
            {
                return Err(invalid());
            }
        }
        if let Some(summary) = &outcome.verification {
            if latest.as_ref().and_then(|result| result.summary.as_ref()) != Some(summary) {
                return Err(invalid());
            }
        }
    }
    Ok(())
}
pub(super) fn update(previous: &RunSnapshot, next: &RunSnapshot) -> Result<(), ContractError> {
    if previous.verification_plan_ref != next.verification_plan_ref
        || !next
            .verification_records
            .starts_with(&previous.verification_records)
        || next.verification_records.len() > previous.verification_records.len() + 1
    {
        return Err(invalid());
    }
    if previous.candidate_ref.is_some()
        && next.candidate_ref.is_some()
        && previous.candidate_ref != next.candidate_ref
    {
        return Err(invalid());
    }
    if previous.candidate_ref.is_some()
        && next.candidate_ref.is_none()
        && next.verification_records.len() != previous.verification_records.len() + 1
    {
        return Err(invalid());
    }
    Ok(())
}
pub(super) fn event(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    reference: &RecordRef,
) -> Result<(), ContractError> {
    let summary: VerificationSummary = event_record(state, additions, reference)?;
    if snapshot.verification_plan_ref.is_none() {
        if snapshot
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.verification.as_ref())
            != Some(&summary)
        {
            return Err(invalid());
        }
        return Ok(());
    }
    if !snapshot.verification_records.iter().any(|record| {
        event_record::<VerificationRecord>(state, additions, record).is_ok_and(|record| {
            record.summary_ref.as_ref() == Some(reference)
                && record.summary.as_ref() == Some(&summary)
        })
    }) {
        return Err(invalid());
    }
    Ok(())
}

fn summaries(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<Vec<RecordRef>, ContractError> {
    let mut references = vec![];
    for reference in &snapshot.verification_records {
        let record: VerificationRecord = event_record(state, additions, reference)?;
        if let Some(reference) = record.summary_ref {
            if !references.contains(&reference) {
                references.push(reference);
            }
        }
    }
    Ok(references)
}
pub(super) fn transition(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    previous: &RunSnapshot,
    next: &RunSnapshot,
    messages: &[Message],
    events: &[RunEvent],
) -> Result<(), ContractError> {
    update(previous, next)?;
    if next.verification_plan_ref.is_none() {
        return Ok(());
    }
    let previous_summaries = summaries(state, additions, previous)?;
    let expected: Vec<_> = summaries(state, additions, next)?
        .into_iter()
        .filter(|reference| !previous_summaries.contains(reference))
        .collect();
    let actual: Vec<_> = events
        .iter()
        .filter_map(|event| {
            if let RunEventPayload::VerificationCompleted { verification_ref } = &event.payload {
                Some(verification_ref.clone())
            } else {
                None
            }
        })
        .collect();
    if actual != expected {
        return Err(error(ErrorCode::InvalidEvent, "verification.missing_event"));
    }
    if previous.candidate_ref.is_some() && next.candidate_ref.is_none() {
        let record: VerificationRecord = event_record(
            state,
            additions,
            next.verification_records.last().ok_or_else(invalid)?,
        )?;
        if Some(&record.candidate_ref) != previous.candidate_ref.as_ref()
            || record.repair_ref.is_none()
        {
            return Err(invalid());
        }
        let VerificationDecision::Revise { feedback } = record.decision.ok_or_else(invalid)? else {
            return Err(invalid());
        };
        let candidate: VerificationCandidate =
            event_record(state, additions, &record.candidate_ref)?;
        let [answer, revision] = messages else {
            return Err(invalid());
        };
        let answer_content: Vec<_> = answer
            .content
            .iter()
            .filter_map(|block| {
                if let ContentBlock::Content { content } = block {
                    Some(content.clone())
                } else {
                    None
                }
            })
            .collect();
        if answer.role != MessageRole::Assistant
            || answer.origin != crate::MessageOrigin::Model
            || answer.visibility != crate::Visibility::Model
            || answer_content != candidate.output
            || answer
                .source_model_request_id
                .as_ref()
                .is_some_and(|attempt| {
                    !next.model_ledger.iter().any(|entry| {
                        &entry.attempt_id == attempt
                            && entry.response_ref.as_ref() == Some(&candidate.response_ref)
                    })
                })
            || revision.role != MessageRole::User
            || revision.origin != crate::MessageOrigin::Verification
            || revision.visibility != crate::Visibility::Model
            || revision.content
                != vec![ContentBlock::Content {
                    content: InputContent::Json {
                        value: serde_json::json!({"kind":"verification_feedback","candidate_digest":record.candidate_ref.digest,"feedback":feedback}),
                    },
                }]
        {
            return Err(invalid());
        }
    } else if messages
        .iter()
        .any(|message| message.origin == crate::MessageOrigin::Verification)
    {
        return Err(invalid());
    }
    Ok(())
}
pub(super) fn history(
    state: &ScopeState,
    snapshot: &RunSnapshot,
    events: &[RunEvent],
) -> Result<(), ContractError> {
    if snapshot.verification_plan_ref.is_none() {
        return Ok(());
    }
    let expected = summaries(state, &BTreeMap::new(), snapshot)?;
    let actual: Vec<_> = events
        .iter()
        .filter_map(|event| {
            if let RunEventPayload::VerificationCompleted { verification_ref } = &event.payload {
                Some(verification_ref.clone())
            } else {
                None
            }
        })
        .collect();
    if actual != expected {
        return Err(error(
            ErrorCode::InvalidSnapshot,
            "verification.event_history",
        ));
    }
    let empty = BTreeMap::new();
    let session = state
        .sessions
        .get(&snapshot.request.session_id)
        .ok_or_else(invalid)?;
    let mut expected_feedback = 0;
    for reference in &snapshot.verification_records {
        let record: VerificationRecord = event_record(state, &empty, reference)?;
        if record.repair_ref.is_none() {
            continue;
        }
        expected_feedback += 1;
        let candidate: VerificationCandidate = event_record(state, &empty, &record.candidate_ref)?;
        let Some(VerificationDecision::Revise { feedback }) = record.decision else {
            return Err(invalid());
        };
        let answer = session
            .messages
            .iter()
            .find(|message| message.sequence.get() == candidate.through_sequence.saturating_add(1))
            .ok_or_else(invalid)?;
        let revision = session
            .messages
            .iter()
            .find(|message| message.sequence.get() == candidate.through_sequence.saturating_add(2))
            .ok_or_else(invalid)?;
        let output: Vec<_> = answer
            .content
            .iter()
            .filter_map(|block| {
                if let ContentBlock::Content { content } = block {
                    Some(content.clone())
                } else {
                    None
                }
            })
            .collect();
        if answer.run_id != snapshot.run_id
            || answer.origin != crate::MessageOrigin::Model
            || output != candidate.output
            || answer
                .source_model_request_id
                .as_ref()
                .is_some_and(|attempt| {
                    !snapshot.model_ledger.iter().any(|entry| {
                        &entry.attempt_id == attempt
                            && entry.response_ref.as_ref() == Some(&candidate.response_ref)
                    })
                })
            || revision.run_id != snapshot.run_id
            || revision.origin != crate::MessageOrigin::Verification
            || revision.visibility != crate::Visibility::Model
            || revision.content
                != vec![ContentBlock::Content {
                    content: InputContent::Json {
                        value: serde_json::json!({"kind":"verification_feedback","candidate_digest":record.candidate_ref.digest,"feedback":feedback}),
                    },
                }]
        {
            return Err(invalid());
        }
    }
    if session
        .messages
        .iter()
        .filter(|message| {
            message.run_id == snapshot.run_id
                && message.origin == crate::MessageOrigin::Verification
        })
        .count()
        != expected_feedback
    {
        return Err(invalid());
    }
    Ok(())
}
