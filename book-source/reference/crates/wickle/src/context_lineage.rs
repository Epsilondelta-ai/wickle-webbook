//! Source-batch relationships for derived conversation data.
use crate::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Immutable source observation used by a covered model/Tool conversation round.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextLineage {
    /// Original Run that committed the source observation.
    pub run_id: Id,
    /// Whole immutable batch; its fragment records preserve exact item versions.
    pub batch_ref: RecordRef,
}
/// Derive dependencies from model-step and Tool-call evidence, in transcript and
/// committed batch order. A legacy unanchored model message conservatively uses
/// its Run's observed sources rather than inventing a precise association.
pub(crate) fn derive_lineage(
    messages: &[Message],
    snapshots: &[&RunSnapshot],
    batches: &[ContextBatch],
) -> Result<Vec<ContextLineage>, ContractError> {
    let mut lineage = Vec::new();
    for message in messages.iter().filter(|message| {
        matches!(
            message.visibility,
            Visibility::Model | Visibility::UserAndModel
        ) && matches!(message.origin, MessageOrigin::Model | MessageOrigin::Tool)
    }) {
        let snapshot = snapshots
            .iter()
            .find(|snapshot| snapshot.run_id == message.run_id)
            .ok_or_else(|| ContractError::new(ErrorCode::InvalidSnapshot, "context.lineage_run"))?;
        let mut attempts = BTreeSet::new();
        for block in &message.content {
            match block {
                ContentBlock::ToolCall { call } => {
                    attempts.insert(call.model_request_id.clone());
                }
                ContentBlock::ToolResult { result }
                | ContentBlock::ToolResultCorrection { result, .. } => {
                    let call = snapshot
                        .tool_ledger
                        .iter()
                        .find(|entry| entry.call.call_id == result.call_id)
                        .ok_or_else(|| {
                            ContractError::new(ErrorCode::InvalidSnapshot, "context.lineage_call")
                        })?;
                    attempts.insert(call.call.model_request_id.clone());
                }
                _ => {}
            }
        }
        if let Some(attempt) = &message.source_model_request_id {
            if !snapshot.model_ledger.iter().any(|invocation| {
                &invocation.attempt_id == attempt
                    && invocation.purpose == ModelPurpose::Agent
                    && matches!(invocation.state, ModelAttemptState::Completed {})
            }) {
                return Err(ContractError::new(
                    ErrorCode::InvalidSnapshot,
                    "context.lineage_attempt",
                ));
            }
            attempts.insert(attempt.clone());
        }
        if attempts.is_empty()
            && !snapshot.status.is_terminal()
            && !snapshot.context_batches.is_empty()
        {
            return Err(ContractError::new(
                ErrorCode::ContextMismatch,
                "context.unanchored_active_history",
            ));
        }
        let steps: BTreeSet<_> = snapshot
            .model_ledger
            .iter()
            .filter(|invocation| attempts.contains(&invocation.attempt_id))
            .map(|invocation| &invocation.model_step_id)
            .collect();
        for batch in batches
            .iter()
            .filter(|batch| batch.request().run_id == snapshot.run_id)
        {
            if batch
                .request()
                .model_step_id
                .as_ref()
                .is_some_and(|step| !steps.is_empty() && !steps.contains(step))
            {
                continue;
            }
            let dependency = ContextLineage {
                run_id: snapshot.run_id.clone(),
                batch_ref: batch.reference(),
            };
            if !lineage.contains(&dependency) {
                lineage.push(dependency);
            }
        }
    }
    Ok(lineage)
}
