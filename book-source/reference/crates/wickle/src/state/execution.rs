use super::*;
use crate::serialization::data_digest;
use crate::{
    AcceptedSegmentCommand, BeginSegmentRequest, BeginSegmentResult, ControlAction, ControlCommand,
    ControlReceipt, ExecutionHistory, ExecutionRecordVersion, ExecutionSegment,
    ExecutionTransactions, InterruptionCause, InterruptionRecord, JsonTextLimits, RequestSnapshot,
    SegmentOutcome, SegmentStart, StoredControlCommand,
};
fn invalid(path: &str) -> ContractError {
    error(ErrorCode::InvalidSnapshot, path)
}

pub(super) fn initial_history(
    snapshot: &RunSnapshot,
    actor: &Id,
    grant: &Id,
    submitted: Option<&RequestSnapshot>,
) -> Result<ExecutionHistory, ContractError> {
    if let Some(value) = submitted {
        validate_submission(value, snapshot)?;
    }
    Ok(ExecutionHistory {
        run_id: snapshot.run_id.clone(),
        execution_principal_ref: actor.clone(),
        execution_grant_ref: Some(grant.clone()),
        submitted: submitted.cloned(),
        initial_claimed: false,
        segments: vec![ExecutionSegment {
            schema_version: ExecutionRecordVersion::V1,
            execution_principal_ref: actor.clone(),
            run_id: snapshot.run_id.clone(),
            segment_id: segment_id(&snapshot.run_id, 0)?,
            accepted_revision: 0,
            outcome: None,
            last_event_seq: None,
            source_snapshot_ref: None,
            app_state: None,
        }],
        accepted_commands: vec![],
        controls: vec![],
    })
}
pub(super) fn segment_id(run: &Id, revision: u64) -> Result<Id, ContractError> {
    Id::new(format!("segment:{}", data_digest(&(run, revision))))
}
fn interrupted(previous: &RunSnapshot, segment: &ExecutionSegment) -> SegmentOutcome {
    SegmentOutcome::Interrupted {
        interruption: InterruptionRecord {
            segment_id: segment.segment_id.clone(),
            cause: InterruptionCause::SegmentStopped,
            checkpoint_revision: previous.revision,
            recoverable: true,
            unresolved_effects: previous
                .tool_ledger
                .iter()
                .filter(|entry| {
                    matches!(
                        entry.state,
                        ToolCallState::Dispatching { .. } | ToolCallState::Unknown { .. }
                    )
                })
                .filter_map(|entry| entry.call.bound_input_ref.clone())
                .collect(),
        },
    }
}
pub(super) fn archived_source(
    snapshot: &RunSnapshot,
    segment: &ExecutionSegment,
) -> Result<ProtectedRecord, ContractError> {
    Ok(ProtectedRecord::new(
        Id::new(format!(
            "segment-source:{}",
            data_digest(&(&snapshot.run_id, &segment.segment_id, snapshot.revision))
        ))?,
        1,
        serde_json::to_value(snapshot).map_err(|_| invalid("execution.source"))?,
    ))
}
pub(super) fn advance_history(
    history: &ExecutionHistory,
    previous: &RunSnapshot,
    next: &RunSnapshot,
    override_id: Option<&Id>,
) -> Result<ExecutionHistory, ContractError> {
    let mut result = history.clone();
    let command = next
        .resume_receipts
        .last()
        .filter(|r| r.accepted_revision == next.revision)
        .map(|r| &r.command)
        .or_else(|| {
            next.recovery_receipts
                .last()
                .filter(|r| r.accepted_revision == next.revision)
                .map(|r| &r.command)
        });
    let last = result
        .segments
        .last_mut()
        .ok_or_else(|| invalid("execution.segment"))?;
    let new_segment = command.is_some()
        || last.outcome.is_some()
        || override_id.is_some_and(|id| *id != last.segment_id);
    if new_segment {
        if last.outcome.is_none() {
            last.outcome = Some(interrupted(previous, last));
            last.last_event_seq = Some(previous.last_event_seq);
            last.source_snapshot_ref = Some(archived_source(previous, last)?.reference().clone());
        }
        let id = override_id
            .cloned()
            .unwrap_or(segment_id(&next.run_id, next.revision)?);
        if result.segments.iter().any(|s| s.segment_id == id) {
            return Err(error(ErrorCode::RequestConflict, "execution.segment_id"));
        }
        result.segments.push(ExecutionSegment {
            schema_version: ExecutionRecordVersion::V1,
            execution_principal_ref: history.execution_principal_ref.clone(),
            run_id: next.run_id.clone(),
            segment_id: id,
            accepted_revision: next.revision,
            outcome: None,
            last_event_seq: None,
            source_snapshot_ref: None,
            app_state: result.segments.last().and_then(|s| s.app_state.clone()),
        });
    }
    let current = result
        .segments
        .last_mut()
        .ok_or_else(|| invalid("execution.segment"))?;
    current.app_state = next.app_state.clone();
    if let Some(outcome) = &next.outcome {
        current.last_event_seq = Some(next.last_event_seq);
        current.outcome = Some(SegmentOutcome::Settled {
            outcome: Box::new(outcome.clone()),
        });
    }
    if let Some(command) = command {
        if result
            .accepted_commands
            .iter()
            .any(|c| c.command_id == command.command_id)
            || result
                .controls
                .iter()
                .any(|c| c.command.command_id == command.command_id)
        {
            return Err(error(ErrorCode::RequestConflict, "execution.command_id"));
        }
        result.accepted_commands.push(AcceptedSegmentCommand {
            command_id: command.command_id.clone(),
            payload_digest: data_digest(command),
            segment_id: current.segment_id.clone(),
        });
    }
    result.initial_claimed = true;
    validate_history(&result, next)?;
    Ok(result)
}
pub(super) fn validate_history(
    history: &ExecutionHistory,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    if history.run_id != snapshot.run_id || history.segments.is_empty() {
        return Err(invalid("execution.run"));
    }
    if let Some(submitted) = &history.submitted {
        validate_submission(submitted, snapshot)?;
    }
    let mut ids = BTreeSet::new();
    let mut revision = 0;
    for (i, segment) in history.segments.iter().enumerate() {
        segment.validate()?;
        if segment.run_id != history.run_id
            || segment.execution_principal_ref != history.execution_principal_ref
            || !ids.insert(segment.segment_id.clone())
            || (i == 0 && segment.accepted_revision != 0)
            || (i > 0 && segment.accepted_revision <= revision)
            || segment.accepted_revision > snapshot.revision
            || (i + 1 < history.segments.len() && segment.outcome.is_none())
        {
            return Err(invalid("execution.history"));
        }
        if segment
            .last_event_seq
            .is_some_and(|seq| seq > snapshot.last_event_seq)
        {
            return Err(invalid("execution.event_boundary"));
        }
        if let Some(next) = history.segments.get(i + 1) {
            if let (Some(previous), Some(next)) = (segment.last_event_seq, next.last_event_seq) {
                if previous >= next {
                    return Err(invalid("execution.event_boundary"));
                }
            }
            let settled_at = match &segment.outcome {
                Some(SegmentOutcome::Settled { outcome }) => outcome.checkpoint_revision,
                Some(SegmentOutcome::Interrupted { interruption }) => {
                    interruption.checkpoint_revision
                }
                None => return Err(invalid("execution.unsettled_history")),
            };
            if settled_at >= next.accepted_revision {
                return Err(invalid("execution.interval_overlap"));
            }
        }
        revision = segment.accepted_revision;
    }
    let current = history
        .segments
        .last()
        .ok_or_else(|| invalid("execution.segment"))?;
    if current.outcome.is_some()
        && current
            .last_event_seq
            .is_some_and(|seq| seq != snapshot.last_event_seq)
    {
        return Err(invalid("execution.event_boundary"));
    }
    for (revision, sequence) in snapshot
        .resume_receipts
        .iter()
        .map(|r| (r.previous_segment_start_revision, r.previous_last_event_seq))
        .chain(
            snapshot
                .recovery_receipts
                .iter()
                .map(|r| (r.previous_segment_start_revision, r.previous_last_event_seq)),
        )
    {
        if history
            .segments
            .iter()
            .find(|segment| segment.accepted_revision == revision)
            .is_some_and(|segment| segment.last_event_seq.is_some_and(|seq| seq != sequence))
        {
            return Err(invalid("execution.event_boundary"));
        }
    }
    if current.app_state != snapshot.app_state {
        return Err(invalid("execution.app_state"));
    }
    match (&snapshot.outcome, &current.outcome) {
        (Some(expected), Some(SegmentOutcome::Settled { outcome }))
            if expected == outcome.as_ref() => {}
        (None, None) => {}
        (None, Some(SegmentOutcome::Interrupted { .. }))
            if snapshot.status == RunStatus::Interrupted => {}
        _ => return Err(invalid("execution.current_outcome")),
    }
    if !history.initial_claimed && (snapshot.revision > 0 || history.segments.len() > 1) {
        return Err(invalid("execution.initial_claim"));
    }
    let mut commands = BTreeSet::new();
    for command in &history.accepted_commands {
        if !commands.insert(command.command_id.clone()) || !ids.contains(&command.segment_id) {
            return Err(invalid("execution.accepted"));
        }
    }
    let mut expected = BTreeMap::new();
    for (command, revision) in snapshot
        .resume_receipts
        .iter()
        .map(|r| (&r.command, r.accepted_revision))
        .chain(
            snapshot
                .recovery_receipts
                .iter()
                .map(|r| (&r.command, r.accepted_revision)),
        )
    {
        if expected
            .insert(command.command_id.clone(), (data_digest(command), revision))
            .is_some()
        {
            return Err(invalid("execution.receipt_duplicate"));
        }
    }
    for control in &history.controls {
        if let Some(id) = &control.processed_segment_id {
            let segment = history
                .segments
                .iter()
                .find(|s| &s.segment_id == id)
                .ok_or_else(|| invalid("execution.control_segment"))?;
            if expected
                .insert(
                    control.command.command_id.clone(),
                    (data_digest(&control.command), segment.accepted_revision),
                )
                .is_some()
            {
                return Err(invalid("execution.receipt_duplicate"));
            }
        }
    }
    if expected.len() != history.accepted_commands.len() {
        return Err(invalid("execution.receipt_coverage"));
    }
    for accepted in &history.accepted_commands {
        let segment = history
            .segments
            .iter()
            .find(|s| s.segment_id == accepted.segment_id)
            .ok_or_else(|| invalid("execution.receipt_segment"))?;
        if expected.get(&accepted.command_id)
            != Some(&(accepted.payload_digest.clone(), segment.accepted_revision))
        {
            return Err(invalid("execution.receipt_payload"));
        }
    }
    let mut controls = BTreeSet::new();
    for control in &history.controls {
        validate_control(&control.command)?;
        let accepted = history
            .accepted_commands
            .iter()
            .find(|c| c.command_id == control.command.command_id);
        match (&control.processed_segment_id, accepted) {
            (Some(segment), Some(receipt))
                if *segment == receipt.segment_id
                    && receipt.payload_digest == data_digest(&control.command) => {}
            (None, None) => {}
            _ => return Err(invalid("execution.control_receipt")),
        }

        if !controls.insert(control.command.command_id.clone())
            || control
                .processed_segment_id
                .as_ref()
                .is_some_and(|id| !ids.contains(id))
        {
            return Err(invalid("execution.controls"));
        }
    }
    Ok(())
}
pub(super) fn complete_controls(
    history: &mut ExecutionHistory,
    snapshot: &RunSnapshot,
    commands: &[Id],
    now_ms: i64,
) -> Result<(), ContractError> {
    let segment = history
        .segments
        .last()
        .ok_or_else(|| invalid("execution.segment"))?
        .segment_id
        .clone();
    let mut seen = BTreeSet::new();
    for id in commands {
        if !seen.insert(id) {
            return Err(invalid("control.duplicate"));
        }
        let control = history
            .controls
            .iter_mut()
            .find(|control| control.command.command_id == *id)
            .ok_or_else(|| invalid("control.missing"))?;
        if control.processed_segment_id.is_some() {
            return Err(error(ErrorCode::RequestConflict, "control.processed"));
        }
        let result = snapshot.outcome.as_ref().map(|outcome| &outcome.result);
        let allowed = match &control.command.action {
            ControlAction::Cancel { reason } => {
                matches!(result, Some(OutcomeResult::Cancelled { reason: actual }) if actual == reason.as_str())
            }
            ControlAction::Expire => {
                now_ms >= snapshot.timing.deadline_at_ms
                    && matches!(
                        result,
                        Some(OutcomeResult::Exhausted {
                            budget: crate::BudgetKind::Elapsed
                        })
                    )
            }
            ControlAction::Stop { cause } => {
                matches!(result, Some(OutcomeResult::Interrupted { interruption }) if interruption.cause == *cause)
                    || matches!(
                        result,
                        Some(
                            OutcomeResult::Cancelled { .. }
                                | OutcomeResult::Failed { .. }
                                | OutcomeResult::Exhausted { .. }
                        )
                    )
            }
        };
        if !allowed {
            return Err(error(ErrorCode::InvalidTransition, "control.outcome"));
        }
        control.processed_segment_id = Some(segment.clone());
        history.accepted_commands.push(AcceptedSegmentCommand {
            command_id: id.clone(),
            payload_digest: data_digest(&control.command),
            segment_id: segment.clone(),
        });
    }
    validate_history(history, snapshot)
}

fn validate_control(command: &ControlCommand) -> Result<(), ContractError> {
    if matches!(command.action,ControlAction::Stop{cause} if !matches!(cause,InterruptionCause::HostShutdown|InterruptionCause::SegmentStopped))
    {
        return Err(error(ErrorCode::InvalidContract, "control.stop_cause"));
    }
    Ok(())
}

impl ExecutionTransactions for MemoryStateStore {
    fn read_execution<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
    ) -> PortFuture<'a, ExecutionHistory> {
        Box::pin(async move {
            let scopes = self.lock()?;
            let state = namespace(&scopes, scope)?;
            if !state.runs.contains_key(run_id) {
                return Err(not_found());
            }
            state.executions.get(run_id).cloned().ok_or_else(|| {
                error(
                    ErrorCode::CapabilityUnsupported,
                    "execution.legacy_checkpoint",
                )
            })
        })
    }
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        command: ControlCommand,
    ) -> PortFuture<'a, ControlReceipt> {
        Box::pin(async move {
            validate_control(&command)?;
            let mut scopes = self.lock()?;
            let state = scopes.get_mut(&scope_key(scope)).ok_or_else(not_found)?;
            let run = state.runs.get(run_id).ok_or_else(not_found)?;
            let history = state.executions.get_mut(run_id).ok_or_else(|| {
                error(
                    ErrorCode::CapabilityUnsupported,
                    "execution.legacy_checkpoint",
                )
            })?;
            let no_op = run.snapshot.status.is_terminal()
                || (matches!(command.action, ControlAction::Stop { .. })
                    && run.snapshot.outcome.is_some());
            if let Some(index) = history
                .controls
                .iter()
                .position(|item| item.command.command_id == command.command_id)
            {
                if history.controls[index].command != command {
                    return Err(error(ErrorCode::RequestConflict, "control.command_id"));
                }
                if history.controls[index].processed_segment_id.is_none() && no_op {
                    let segment_id = history
                        .segments
                        .last()
                        .ok_or_else(|| invalid("execution.segment"))?
                        .segment_id
                        .clone();
                    history.controls[index].processed_segment_id = Some(segment_id.clone());
                    history.accepted_commands.push(AcceptedSegmentCommand {
                        command_id: command.command_id.clone(),
                        payload_digest: data_digest(&command),
                        segment_id,
                    });
                    validate_history(history, &run.snapshot)?;
                }
                return Ok(ControlReceipt {
                    run_id: run_id.clone(),
                    command_id: command.command_id,
                    processed_segment_id: history.controls[index].processed_segment_id.clone(),
                });
            }
            if history
                .accepted_commands
                .iter()
                .any(|c| c.command_id == command.command_id)
            {
                return Err(error(ErrorCode::RequestConflict, "control.command_id"));
            }
            let processed_segment_id = if no_op {
                Some(
                    history
                        .segments
                        .last()
                        .ok_or_else(|| invalid("execution.segment"))?
                        .segment_id
                        .clone(),
                )
            } else {
                None
            };
            let receipt = ControlReceipt {
                run_id: run_id.clone(),
                command_id: command.command_id.clone(),
                processed_segment_id: processed_segment_id.clone(),
            };
            if let Some(segment_id) = &processed_segment_id {
                history.accepted_commands.push(AcceptedSegmentCommand {
                    command_id: command.command_id.clone(),
                    payload_digest: data_digest(&command),
                    segment_id: segment_id.clone(),
                });
            }
            history.controls.push(StoredControlCommand {
                command,
                processed_segment_id,
            });
            validate_history(history, &run.snapshot)?;
            Ok(receipt)
        })
    }
    fn begin_segment<'a>(
        &'a self,
        scope: &'a Scope,
        request: BeginSegmentRequest,
    ) -> PortFuture<'a, BeginSegmentResult> {
        Box::pin(async move {
            // A private working copy is validated synchronously under the scope
            // mutex. Publish once only after all lease/commit checks succeed.
            let mut scopes = self.lock()?;
            let previous = namespace(&scopes, scope)?.clone();
            let working = MemoryStateStore {
                scopes: Mutex::new(BTreeMap::from([(scope_key(scope), previous)])),
            };
            let result = working.begin_segment_now(scope, request)?;
            let mut committed = working
                .scopes
                .into_inner()
                .map_err(|_| error(ErrorCode::PersistenceUnavailable, "execution.transaction"))?;
            scopes.insert(
                scope_key(scope),
                committed.remove(&scope_key(scope)).ok_or_else(not_found)?,
            );
            Ok(result)
        })
    }
}
impl MemoryStateStore {
    fn begin_segment_now(
        &self,
        scope: &Scope,
        request: BeginSegmentRequest,
    ) -> Result<BeginSegmentResult, ContractError> {
        let (saved, history) = {
            let scopes = self.lock()?;
            let state = namespace(&scopes, scope)?;
            (
                stored_run(state, &request.run_id)?,
                state
                    .executions
                    .get(&request.run_id)
                    .cloned()
                    .ok_or_else(|| {
                        error(
                            ErrorCode::CapabilityUnsupported,
                            "execution.legacy_checkpoint",
                        )
                    })?,
            )
        };
        let current = history
            .segments
            .last()
            .ok_or_else(|| invalid("execution.segment"))?;
        let payload = match &request.start {
            SegmentStart::Initial => None,
            SegmentStart::Resume(command) => {
                if command.run_id != request.run_id
                    || command.expected_revision != request.expected_revision
                {
                    return Err(error(ErrorCode::RequestConflict, "execution.command"));
                }
                Some((command.command_id.clone(), data_digest(command)))
            }
            SegmentStart::Control(id) => {
                let command = history
                    .controls
                    .iter()
                    .find(|c| c.command.command_id == *id)
                    .ok_or_else(not_found)?;
                Some((id.clone(), data_digest(&command.command)))
            }
        };
        if let Some((id, digest)) = &payload {
            if let Some(receipt) = history
                .accepted_commands
                .iter()
                .find(|c| c.command_id == *id)
            {
                if receipt.payload_digest != *digest {
                    return Err(error(ErrorCode::RequestConflict, "execution.command"));
                }
                let segment = history
                    .segments
                    .iter()
                    .find(|s| s.segment_id == receipt.segment_id)
                    .cloned()
                    .ok_or_else(|| invalid("execution.receipt"))?;
                return Ok(BeginSegmentResult {
                    state: saved,
                    segment,
                    lease: None,
                });
            }
        } else if history.initial_claimed {
            if request.segment_id != history.segments[0].segment_id {
                return Err(error(ErrorCode::RequestConflict, "execution.initial_id"));
            }
            return Ok(BeginSegmentResult {
                state: saved,
                segment: history.segments[0].clone(),
                lease: None,
            });
        }
        if saved.snapshot.revision != request.expected_revision {
            return Err(error(ErrorCode::RevisionConflict, "execution.revision"));
        }
        if saved.snapshot.status.is_terminal() {
            if matches!(request.start, SegmentStart::Control(_)) {
                return Ok(BeginSegmentResult {
                    state: saved,
                    segment: current.clone(),
                    lease: None,
                });
            }
            return Err(error(ErrorCode::InvalidTransition, "execution.terminal"));
        }
        if !matches!(request.start, SegmentStart::Initial)
            && history
                .segments
                .iter()
                .any(|s| s.segment_id == request.segment_id)
        {
            return Err(error(ErrorCode::RequestConflict, "execution.segment_id"));
        }
        let lease = self.acquire_lease_now(
            scope,
            &request.run_id,
            &request.owner,
            request.now_ms,
            request.lease_ttl_ms.get(),
        )?;
        if matches!(request.start, SegmentStart::Initial) {
            if request.transition.is_some()
                || request.segment_id != current.segment_id
                || saved.snapshot.phase != RunPhase::Admission
            {
                return Err(error(ErrorCode::InvalidTransition, "execution.initial"));
            }
            let mut scopes = self.lock()?;
            scopes
                .get_mut(&scope_key(scope))
                .ok_or_else(not_found)?
                .executions
                .get_mut(&request.run_id)
                .ok_or_else(not_found)?
                .initial_claimed = true;
            return Ok(BeginSegmentResult {
                state: saved,
                segment: current.clone(),
                lease: Some(lease),
            });
        }
        let transition = request
            .transition
            .ok_or_else(|| error(ErrorCode::InvalidContract, "execution.transition"))?;
        match &request.start {
            SegmentStart::Resume(command) => {
                let actual = transition
                    .snapshot
                    .resume_receipts
                    .last()
                    .filter(|r| r.accepted_revision == transition.snapshot.revision)
                    .map(|r| &r.command)
                    .or_else(|| {
                        transition
                            .snapshot
                            .recovery_receipts
                            .last()
                            .filter(|r| r.accepted_revision == transition.snapshot.revision)
                            .map(|r| &r.command)
                    });
                if actual != Some(command) {
                    return Err(error(
                        ErrorCode::InvalidContract,
                        "execution.resume_transition",
                    ));
                }
            }
            SegmentStart::Control(id) => {
                let control = history
                    .controls
                    .iter()
                    .find(|c| c.command.command_id == *id)
                    .ok_or_else(not_found)?;
                let allowed = match &control.command.action {
                    ControlAction::Cancel { reason } => {
                        transition.snapshot.status == RunStatus::Cancelled
                            && matches!(transition.snapshot.outcome.as_ref().map(|o| &o.result), Some(OutcomeResult::Cancelled { reason: actual }) if actual == reason.as_str())
                    }
                    ControlAction::Expire => {
                        request.now_ms >= saved.snapshot.timing.deadline_at_ms
                            && transition.snapshot.status == RunStatus::Exhausted
                            && matches!(
                                transition.snapshot.outcome.as_ref().map(|o| &o.result),
                                Some(OutcomeResult::Exhausted {
                                    budget: crate::BudgetKind::Elapsed
                                })
                            )
                    }
                    ControlAction::Stop { .. } => {
                        return Err(error(
                            ErrorCode::CapabilityUnsupported,
                            "execution.stop_checkpoint",
                        ));
                    }
                };
                if !allowed {
                    return Err(error(
                        ErrorCode::InvalidTransition,
                        "execution.control_transition",
                    ));
                }
            }
            SegmentStart::Initial => unreachable!(),
        }
        let result = self.commit_now(
            scope,
            &request.run_id,
            CommitInput {
                control_commands: vec![],
                expected_revision: request.expected_revision,
                lease: lease.clone(),
                now_ms: request.now_ms,
                snapshot: transition.snapshot,
                messages: transition.messages,
                events: transition.events,
                records: transition.records,
            },
            Some(&request.segment_id),
        )?;
        let mut scopes = self.lock()?;
        let state = scopes.get_mut(&scope_key(scope)).ok_or_else(not_found)?;
        let history = state
            .executions
            .get_mut(&request.run_id)
            .ok_or_else(not_found)?;
        if let SegmentStart::Control(id) = request.start {
            let (_, digest) = payload.ok_or_else(|| invalid("execution.control"))?;
            history.accepted_commands.push(AcceptedSegmentCommand {
                command_id: id.clone(),
                payload_digest: digest,
                segment_id: request.segment_id.clone(),
            });
            history
                .controls
                .iter_mut()
                .find(|c| c.command.command_id == id)
                .ok_or_else(not_found)?
                .processed_segment_id = Some(request.segment_id);
        }
        validate_history(history, &result.snapshot)?;
        let segment = history.segments.last().cloned().ok_or_else(not_found)?;
        let lease = if result.snapshot.status.is_terminal() {
            None
        } else {
            Some(lease)
        };
        Ok(BeginSegmentResult {
            state: result,
            segment,
            lease,
        })
    }
}

fn validate_submission(
    submitted: &RequestSnapshot,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    submitted.validate(JsonTextLimits::default())?;
    let request = crate::RunRequest::from_json(submitted.request_json())?;
    let expected_system = snapshot
        .system_inputs
        .as_ref()
        .map(|s| s.values_digest.clone())
        .unwrap_or_else(|| crate::canonical_digest(&serde_json::json!({})));
    if request != snapshot.request
        || submitted.profile_ref.id != snapshot.profile.profile().agent_id
        || submitted.profile_ref.version != snapshot.profile.profile().version
        || crate::canonical_digest_json(submitted.system_inputs_json())? != expected_system
    {
        return Err(invalid("execution.submitted_snapshot"));
    }
    Ok(())
}

/// Historical interval data must agree with the immutable settlement event,
/// not merely with another copy inside the execution history.
pub(super) fn validate_settlements(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    history: &ExecutionHistory,
    events: &[&RunEvent],
) -> Result<(), ContractError> {
    for segment in &history.segments {
        let Some(settlement) = &segment.outcome else {
            continue;
        };
        let sequence = match segment.last_event_seq {
            Some(sequence) => sequence,
            None if history.execution_grant_ref.is_none() => continue, // Legacy intervals are read-only.
            None => return Err(invalid("execution.event_boundary_missing")),
        };
        let event = events
            .iter()
            .find(|event| event.seq.get() == sequence)
            .ok_or_else(|| invalid("execution.settlement_event"))?;
        match settlement {
            SegmentOutcome::Settled { outcome } => {
                let reference = match &event.payload {
                    RunEventPayload::RunFinished { outcome_ref }
                    | RunEventPayload::RunInterrupted { outcome_ref, .. } => outcome_ref,
                    RunEventPayload::RunWaiting {
                        wait_ref,
                        outcome_ref,
                    } => {
                        let wait: crate::WaitState = event_record(state, additions, wait_ref)?;
                        if !matches!(&outcome.result, OutcomeResult::Waiting { wait: actual } if *actual == wait)
                        {
                            return Err(invalid("execution.wait_outcome"));
                        }
                        match outcome_ref {
                            Some(reference) => reference,
                            None if history.execution_grant_ref.is_none() => continue,
                            None => return Err(invalid("execution.wait_outcome_missing")),
                        }
                    }
                    _ => return Err(invalid("execution.settlement_event")),
                };
                let stored: crate::RunOutcome = event_record(state, additions, reference)?;
                if &stored != outcome.as_ref() {
                    return Err(invalid("execution.settlement_outcome"));
                }
            }
            SegmentOutcome::Interrupted { interruption } => {
                let reference = segment
                    .source_snapshot_ref
                    .as_ref()
                    .ok_or_else(|| invalid("execution.interrupted_source"))?;
                let source: RunSnapshot = event_record(state, additions, reference)?;
                source.validate()?;
                if source.request != snapshot.request
                    || source.profile != snapshot.profile
                    || source.limits != snapshot.limits
                    || source.system_inputs != snapshot.system_inputs
                {
                    return Err(invalid("execution.interrupted_identity"));
                }
                if let Some(receipt) = snapshot.recovery_receipts.iter().find(|receipt| {
                    receipt.previous_segment_start_revision == segment.accepted_revision
                }) {
                    let accepted: RunSnapshot =
                        event_record(state, additions, &receipt.source_snapshot_ref)?;
                    if accepted != source {
                        return Err(invalid("execution.interrupted_source"));
                    }
                }
                if source.run_id != snapshot.run_id
                    || source.scope != snapshot.scope
                    || source.revision != interruption.checkpoint_revision
                    || source.last_event_seq != sequence
                    || source.app_state != segment.app_state
                    || interrupted(&source, segment) != *settlement
                {
                    return Err(invalid("execution.interrupted_source"));
                }
            }
        }
    }
    Ok(())
}
