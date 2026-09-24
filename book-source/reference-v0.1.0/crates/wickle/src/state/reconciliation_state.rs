use super::*;
use crate::{ReservationKind, ToolEffect, ToolResultStatus, tool_execution::ReconciliationRecord};

/// Validate the causal evidence that authorizes a read-only correction.
pub(super) fn corrections(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    events: &[&RunEvent],
    messages: &[&Message],
) -> Result<BTreeSet<Id>, ContractError> {
    let invalid = || error(ErrorCode::InvalidEvent, "tool.reconciliation");
    let mut corrected = BTreeSet::new();
    let mut queries = BTreeSet::new();
    for event in events {
        let RunEventPayload::ToolReconciled { reconciliation_ref } = &event.payload else {
            continue;
        };
        let record: ReconciliationRecord = event_record(state, additions, reconciliation_ref)?;
        if record.schema_version != "wickle.tool-reconciliation.v1"
            || record.scope != snapshot.scope
            || record.run_id != snapshot.run_id
            || !queries.insert(record.recovery_attempt_id.clone())
            || !corrected.insert(record.correction_message_id.clone())
        {
            return Err(invalid());
        }
        let entry = snapshot
            .tool_ledger
            .iter()
            .find(|entry| entry.call.call_id == record.call_id)
            .ok_or_else(invalid)?;
        if entry.call.bound_input_ref.as_ref() != Some(&record.binding_ref) {
            return Err(invalid());
        }
        record_value(state, additions, &record.binding_ref)?;
        if !snapshot.reservations.iter().any(|reservation|reservation.attempt_id==record.attempt_id&&matches!(&reservation.kind,ReservationKind::Tool{call_id} if call_id==&record.call_id))||!snapshot.reservations.iter().any(|reservation|reservation.attempt_id==record.recovery_attempt_id&&reservation.kind==ReservationKind::Recovery{}&&reservation.reserved_at_ms<=event.timestamp_ms){return Err(invalid());}
        let result: ToolResult = event_record(state, additions, &record.result_ref)?;
        if result.call_id != record.call_id
            || result.effect == ToolEffect::Unknown
            || result.status == ToolResultStatus::Unknown
            || !matches!(&entry.state,ToolCallState::Settled{result:saved} if saved==&result)
        {
            return Err(invalid());
        }
        if !events.iter().any(|next|next.seq.get()==event.seq.get()+1&&next.timestamp_ms==event.timestamp_ms&&matches!(&next.payload,RunEventPayload::ToolSettled{result_ref} if result_ref==&record.result_ref)){return Err(invalid());}
        let message = messages
            .iter()
            .find(|message| message.message_id == record.correction_message_id)
            .ok_or_else(invalid)?;
        let [
            ContentBlock::ToolResultCorrection {
                previous_message_id,
                previous_result_digest,
                result: corrected_result,
            },
        ] = message.content.as_slice()
        else {
            return Err(invalid());
        };
        if message.run_id != snapshot.run_id || corrected_result != &result {
            return Err(invalid());
        }
        let session = state
            .sessions
            .get(&snapshot.request.session_id)
            .ok_or_else(invalid)?;
        let original = session
            .messages
            .iter()
            .find(|message| &message.message_id == previous_message_id)
            .ok_or_else(invalid)?;
        let previous = original
            .content
            .iter()
            .find_map(|block| match block {
                ContentBlock::ToolResult { result } if result.call_id == record.call_id => {
                    Some(result)
                }
                _ => None,
            })
            .ok_or_else(invalid)?;
        if previous.effect != ToolEffect::Unknown
            || previous.status != ToolResultStatus::Unknown
            || canonical_digest(&serde_json::to_value(previous).map_err(|_| invalid())?)
                != *previous_result_digest
        {
            return Err(invalid());
        }
        let prior_events = state
            .runs
            .get(&snapshot.run_id)
            .ok_or_else(invalid)?
            .events
            .iter()
            .chain(events.iter().copied());
        if !prior_events.into_iter().any(|prior|prior.seq<event.seq&&matches!(&prior.payload,RunEventPayload::ToolUnresolved{result_ref,attempt_id,idempotency_key} if attempt_id==&record.attempt_id&&idempotency_key==&record.idempotency_key&&event_record::<ToolResult>(state,additions,result_ref).is_ok_and(|value|value==*previous))){return Err(invalid());}
    }
    Ok(corrected)
}
