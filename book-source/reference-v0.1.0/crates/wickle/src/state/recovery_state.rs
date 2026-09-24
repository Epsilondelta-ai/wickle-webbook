use super::*;
use crate::RecoveryReceipt;
fn invalid() -> ContractError {
    error(ErrorCode::InvalidSnapshot, "recovery.receipt")
}

pub(super) fn validate(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    if snapshot.recovery_receipts.is_empty() {
        if snapshot
            .model_ledger
            .iter()
            .any(|attempt| matches!(attempt.state, ModelAttemptState::Interrupted { .. }))
        {
            return Err(invalid());
        }
        return Ok(());
    }
    validate_recovered(state, additions, snapshot)
}

// Keep source-checkpoint decoding off ordinary admission/commit stack frames.
#[inline(never)]
fn validate_recovered(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    let mut reservations = BTreeSet::new();
    for receipt in &snapshot.recovery_receipts {
        let command: ResumeCommand = event_record(state, additions, &receipt.command_ref)?;
        let source: Box<RunSnapshot> =
            event_record(state, additions, &receipt.source_snapshot_ref)?;
        source.validate()?;
        if command != receipt.command
            || source.status != RunStatus::Running
            || source.scope != snapshot.scope
            || source.run_id != snapshot.run_id
            || source.request != snapshot.request
            || source.profile != snapshot.profile
            || source.revision != command.expected_revision
            || source.last_event_seq != receipt.previous_last_event_seq
            || !matches!(&command.action,ResumeAction::Recover{recovery_ref} if recovery_ref==&receipt.source_snapshot_ref)
        {
            return Err(invalid());
        }
        match &receipt.recovery_attempt_id {
            Some(id) if !receipt.expired => {
                if !reservations.insert(id.clone())
                    || source
                        .reservations
                        .iter()
                        .any(|value| &value.attempt_id == id)
                    || !snapshot.reservations.iter().any(|value| {
                        &value.attempt_id == id && value.kind == crate::ReservationKind::Recovery {}
                    })
                {
                    return Err(invalid());
                }
            }
            None if receipt.expired => {}
            _ => return Err(invalid()),
        }
        let segment = source
            .resume_receipts
            .last()
            .map_or(0, |value| value.accepted_revision)
            .max(
                source
                    .recovery_receipts
                    .last()
                    .map_or(0, |value| value.accepted_revision),
            );
        if segment != receipt.previous_segment_start_revision {
            return Err(invalid());
        }
    }
    for attempt in &snapshot.model_ledger {
        if let ModelAttemptState::Interrupted {
            recovery_command_id,
        } = &attempt.state
        {
            let receipt = snapshot
                .recovery_receipts
                .iter()
                .find(|receipt| &receipt.command.command_id == recovery_command_id)
                .ok_or_else(invalid)?;
            if receipt.expired {
                return Err(invalid());
            }
            let source: Box<RunSnapshot> =
                event_record(state, additions, &receipt.source_snapshot_ref)?;
            let old = source
                .model_ledger
                .iter()
                .find(|old| old.attempt_id == attempt.attempt_id)
                .ok_or_else(invalid)?;
            if !matches!(
                old.state,
                ModelAttemptState::Reserved {} | ModelAttemptState::Unknown {}
            ) {
                return Err(invalid());
            }
            let mut expected = old.clone();
            expected.state = attempt.state.clone();
            if &expected != attempt {
                return Err(invalid());
            }
        }
    }
    Ok(())
}
pub(super) fn transition(
    previous: &RunSnapshot,
    next: &RunSnapshot,
    events: &[RunEvent],
) -> Result<(), ContractError> {
    if !next
        .recovery_receipts
        .starts_with(&previous.recovery_receipts)
        || next.recovery_receipts.len() > previous.recovery_receipts.len() + 1
    {
        return Err(invalid());
    }
    let recovered: Vec<_> = events
        .iter()
        .filter(|event| matches!(event.payload, RunEventPayload::RunRecovered { .. }))
        .collect();
    if next.recovery_receipts.len() == previous.recovery_receipts.len() {
        if !recovered.is_empty() {
            return Err(invalid());
        }
        return Ok(());
    }
    let receipt = next.recovery_receipts.last().ok_or_else(invalid)?;
    if previous
        .recovery_record(receipt.source_snapshot_ref.record_id.clone())?
        .reference()
        != &receipt.source_snapshot_ref
    {
        return Err(invalid());
    }
    let added = next
        .reservations
        .get(previous.reservations.len()..)
        .ok_or_else(invalid)?;
    if receipt.expired {
        if !added.is_empty() {
            return Err(invalid());
        }
    } else if added.len() != 1
        || receipt.recovery_attempt_id.as_ref() != Some(&added[0].attempt_id)
        || added[0].kind != (crate::ReservationKind::Recovery {})
    {
        return Err(invalid());
    }
    if previous.status != RunStatus::Running
        || next.status != RunStatus::Running
        || next.resume_receipts != previous.resume_receipts
        || receipt.accepted_revision != next.revision
        || receipt.command.expected_revision != previous.revision
        || receipt.previous_last_event_seq != previous.last_event_seq
        || recovered.len() != 1
    {
        return Err(invalid());
    }
    Ok(())
}
pub(super) fn event(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    event: &RunEvent,
    reference: &RecordRef,
) -> Result<(), ContractError> {
    let receipt: RecoveryReceipt = event_record(state, additions, reference)?;
    if !snapshot.recovery_receipts.contains(&receipt)
        || event.seq.get() != receipt.previous_last_event_seq + 1
    {
        return Err(invalid());
    }
    let source: Box<RunSnapshot> = event_record(state, additions, &receipt.source_snapshot_ref)?;
    if let Some(id) = &receipt.recovery_attempt_id {
        if !snapshot
            .reservations
            .iter()
            .any(|value| &value.attempt_id == id && value.reserved_at_ms == event.timestamp_ms)
        {
            return Err(invalid());
        }
    }
    if receipt.expired != (event.timestamp_ms >= source.timing.deadline_at_ms) {
        return Err(invalid());
    }
    Ok(())
}
pub(super) fn history(
    state: &ScopeState,
    snapshot: &RunSnapshot,
    events: &[RunEvent],
) -> Result<(), ContractError> {
    let recovered: Vec<_> = events
        .iter()
        .filter(|event| matches!(event.payload, RunEventPayload::RunRecovered { .. }))
        .collect();
    if recovered.len() != snapshot.recovery_receipts.len() {
        return Err(invalid());
    }
    for (event, receipt) in recovered.into_iter().zip(&snapshot.recovery_receipts) {
        let RunEventPayload::RunRecovered {
            recovery_receipt_ref,
        } = &event.payload
        else {
            unreachable!()
        };
        let actual: RecoveryReceipt = event_record(state, &BTreeMap::new(), recovery_receipt_ref)?;
        if &actual != receipt {
            return Err(invalid());
        }
    }
    Ok(())
}
