//! Evidence and command identity for resuming interrupted Running executions.
use crate::*;
use serde::{Deserialize, Serialize};

/// A consumed recovery command. Its source checkpoint has no fabricated outcome.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryReceipt {
    /// Exact idempotent command accepted by the core.
    pub command: ResumeCommand,
    /// Stored original command.
    pub command_ref: RecordRef,
    /// Exact Running checkpoint observed by the caller before recovery.
    pub source_snapshot_ref: RecordRef,
    /// Revision that atomically accepted this recovery.
    pub accepted_revision: u64,
    /// Execution segment superseded by this recovery.
    pub previous_segment_start_revision: u64,
    /// Last event belonging to the interrupted segment.
    pub previous_last_event_seq: u64,
    /// Current authorized recovery actor.
    pub actor_ref: Id,
    /// Current Host grant, never a replacement for policy checks.
    pub capability_grant_ref: Id,
    /// Whether the original Run deadline had already elapsed at acceptance.
    pub expired: bool,
    /// Recovery budget reservation accepted atomically; absent only for expired Runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_attempt_id: Option<Id>,
}
impl RunSnapshot {
    /// Construct a referenceable recovery checkpoint without reading or writing storage.
    /// Pass its reference in ResumeAction::Recover. The core compares its value to
    /// authoritative state under a new lease and stores it with acceptance.
    pub fn recovery_record(&self, record_id: Id) -> Result<ProtectedRecord, ContractError> {
        self.validate()?;
        if !matches!(self.status, RunStatus::Running | RunStatus::Interrupted) {
            return Err(ContractError::new(
                ErrorCode::InvalidTransition,
                "recovery.source_status",
            ));
        }
        Ok(ProtectedRecord::new(
            record_id,
            1,
            serde_json::to_value(self)
                .map_err(|_| ContractError::new(ErrorCode::InvalidJson, "recovery.source"))?,
        ))
    }
}
