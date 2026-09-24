use super::*;
use crate::{JsonDigest, RunOutcome, RunRequest, serialization::data_digest};
use serde::{Deserialize, Serialize, Serializer};

/// Version of the protected, scope-local memory-store checkpoint format.
pub const STATE_STORE_CHECKPOINT_VERSION: &str = "wickle.state-store.v2";
const LEGACY_CHECKPOINT_VERSION: &str = "wickle.state-store.v1";

/// An owned, validated scope graph. Explicit serialization contains protected
/// transcript and input data and is intended only for authorized storage adapters.
/// No caller can mutate its state or deserialize it without full validation.
#[derive(Clone)]
pub struct StateStoreCheckpoint {
    scope: Scope,
    state: ScopeState,
}

impl fmt::Debug for StateStoreCheckpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateStoreCheckpoint")
            .field("session_count", &self.state.sessions.len())
            .field("run_count", &self.state.runs.len())
            .field("record_count", &self.state.records.len())
            .finish_non_exhaustive()
    }
}

#[derive(Serialize)]
struct CheckpointView<'a> {
    schema_version: &'static str,
    scope: &'a Scope,
    sessions: Vec<SessionView<'a>>,
    runs: Vec<RunView<'a>>,
    records: Vec<RecordView<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    hook_observations: Vec<&'a crate::HookObservation>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    executions: Vec<&'a crate::ExecutionHistory>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    legacy_runs: Vec<&'a Id>,
}
#[derive(Serialize)]
struct SessionView<'a> {
    snapshot: &'a SessionSnapshot,
    messages: &'a [Message],
}
#[derive(Serialize)]
struct RunView<'a> {
    snapshot: &'a RunSnapshot,
    events: &'a [RunEvent],
    lease: Option<LeaseData>,
    last_fencing_token: u64,
}
#[derive(Serialize)]
struct RecordView<'a> {
    reference: &'a RecordRef,
    value: &'a Value,
}

impl Serialize for StateStoreCheckpoint {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        CheckpointView {
            schema_version: if self.state.executions.is_empty() {
                LEGACY_CHECKPOINT_VERSION
            } else {
                STATE_STORE_CHECKPOINT_VERSION
            },
            executions: self.state.executions.values().collect(),
            legacy_runs: if self.state.executions.is_empty() {
                Vec::new()
            } else {
                self.state.legacy_runs.iter().collect()
            },
            scope: &self.scope,
            sessions: self
                .state
                .sessions
                .values()
                .map(|session| SessionView {
                    snapshot: &session.snapshot,
                    messages: &session.messages,
                })
                .collect(),
            runs: self
                .state
                .runs
                .values()
                .map(|run| RunView {
                    snapshot: &run.snapshot,
                    events: &run.events,
                    lease: run.lease.as_ref().map(LeaseData::from),
                    last_fencing_token: run.last_fencing_token,
                })
                .collect(),
            records: self
                .state
                .records
                .values()
                .map(|record| RecordView {
                    reference: record.reference(),
                    value: record.value(),
                })
                .collect(),
            hook_observations: self.state.hook_observations.values().flatten().collect(),
        }
        .serialize(serializer)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointData {
    schema_version: String,
    scope: Scope,
    sessions: Vec<SessionData>,
    runs: Vec<RunData>,
    records: Vec<RecordData>,
    #[serde(default)]
    hook_observations: Vec<crate::HookObservation>,
    #[serde(default)]
    executions: Vec<crate::ExecutionHistory>,
    #[serde(default)]
    legacy_runs: Vec<Id>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionData {
    snapshot: SessionSnapshot,
    messages: Vec<Message>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunData {
    snapshot: RunSnapshot,
    events: Vec<RunEvent>,
    lease: Option<LeaseData>,
    last_fencing_token: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordData {
    reference: RecordRef,
    value: Value,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaseData {
    scope: Scope,
    run_id: Id,
    owner: Id,
    fencing_token: u64,
    expires_at_ms: i64,
}
impl From<&RunLease> for LeaseData {
    fn from(lease: &RunLease) -> Self {
        Self {
            scope: lease.scope.clone(),
            run_id: lease.run_id.clone(),
            owner: lease.owner.clone(),
            fencing_token: lease.fencing_token,
            expires_at_ms: lease.expires_at_ms,
        }
    }
}
impl From<LeaseData> for RunLease {
    fn from(lease: LeaseData) -> Self {
        Self {
            scope: lease.scope,
            run_id: lease.run_id,
            owner: lease.owner,
            fencing_token: lease.fencing_token,
            expires_at_ms: lease.expires_at_ms,
        }
    }
}

impl StateStoreCheckpoint {
    /// Exact namespace covered by the protected checkpoint.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Canonical identity of the serialized scope graph, excluding derived indexes.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }
    /// Parse a known version and validate scope, trusted digest, current state,
    /// historical typed records and derived indexes. Collection order is the stable
    /// key order produced by export; malformed or noncanonical images are rejected.
    pub fn from_json(
        input: &str,
        scope: &Scope,
        expected_digest: &JsonDigest,
    ) -> Result<Self, ContractError> {
        let value = crate::parse_json(input)?;
        if !matches!(
            value.get("schema_version").and_then(Value::as_str),
            Some(STATE_STORE_CHECKPOINT_VERSION | LEGACY_CHECKPOINT_VERSION)
        ) {
            return Err(error(
                ErrorCode::UnsupportedSchemaVersion,
                "checkpoint.schema_version",
            ));
        }
        if canonical_digest(&value) != *expected_digest {
            return Err(invalid("checkpoint.digest"));
        }
        let data: CheckpointData =
            serde_json::from_value(value).map_err(|_| invalid("checkpoint"))?;
        if &data.scope != scope {
            return Err(error(ErrorCode::AccessDenied, "checkpoint.scope"));
        }
        let checkpoint = restore_graph(data)?;
        if checkpoint.digest() != *expected_digest {
            return Err(invalid("checkpoint.canonical_form"));
        }
        Ok(checkpoint)
    }
}

impl MemoryStateStore {
    /// Copy only the requested namespace without performing I/O or exposing live
    /// mutable references. Unknown namespaces return StateNotFound.
    pub fn export_checkpoint(&self, scope: &Scope) -> Result<StateStoreCheckpoint, ContractError> {
        let scopes = self.lock()?;
        Ok(StateStoreCheckpoint {
            scope: scope.clone(),
            state: namespace(&scopes, scope)?.clone(),
        })
    }
    /// Move an already validated private checkpoint into a new process-local store.
    /// This does not perform a second graph validation or claim durable capabilities.
    pub fn from_checkpoint(checkpoint: StateStoreCheckpoint) -> Self {
        Self {
            scopes: Mutex::new(BTreeMap::from([(
                scope_key(&checkpoint.scope),
                checkpoint.state,
            )])),
        }
    }
}

fn restore_graph(data: CheckpointData) -> Result<StateStoreCheckpoint, ContractError> {
    if data.schema_version != STATE_STORE_CHECKPOINT_VERSION
        && data.schema_version != LEGACY_CHECKPOINT_VERSION
    {
        return Err(invalid("checkpoint.schema_version"));
    }
    if data.schema_version == LEGACY_CHECKPOINT_VERSION
        && (!data.executions.is_empty() || !data.legacy_runs.is_empty())
    {
        return Err(invalid("checkpoint.legacy_execution"));
    }
    let execution_records = data.executions.clone();
    let mut state = ScopeState::default();
    for record in data.records {
        if canonical_digest(&record.value) != record.reference.digest {
            return Err(invalid("checkpoint.record_digest"));
        }
        let key = record_key(&record.reference);
        if state
            .records
            .insert(
                key,
                ProtectedRecord {
                    reference: record.reference,
                    value: record.value,
                },
            )
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_record"));
        }
    }
    for session in data.sessions {
        if session.snapshot.scope != data.scope {
            return Err(invalid("checkpoint.session_scope"));
        }
        if state
            .sessions
            .insert(
                session.snapshot.session_id.clone(),
                SessionState {
                    snapshot: session.snapshot,
                    messages: session.messages,
                },
            )
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_session"));
        }
    }
    for run in data.runs {
        if run.snapshot.scope != data.scope {
            return Err(invalid("checkpoint.run_scope"));
        }
        run.snapshot.validate()?;
        let session = state
            .sessions
            .get(&run.snapshot.request.session_id)
            .ok_or_else(|| invalid("checkpoint.run_session"))?;
        if session.snapshot.profile_digest != *run.snapshot.profile.profile_digest() {
            return Err(invalid("checkpoint.session_profile"));
        }
        if run.snapshot.revision > 0 && run.last_fencing_token == 0 {
            return Err(invalid("checkpoint.fencing_generation"));
        }
        if let Some(lease) = &run.lease {
            if lease.scope != data.scope
                || lease.run_id != run.snapshot.run_id
                || lease.fencing_token == 0
                || lease.fencing_token != run.last_fencing_token
                || run.snapshot.status.is_terminal()
            {
                return Err(invalid("checkpoint.lease"));
            }
        }
        if run.snapshot.revision == 0
            && (run.snapshot.status != RunStatus::Running
                || run.snapshot.phase != RunPhase::Admission
                || run.snapshot.usage != BudgetUsage::default()
                || !run.snapshot.reservations.is_empty()
                || !run.snapshot.model_ledger.is_empty()
                || !run.snapshot.tool_ledger.is_empty()
                || run.events.len() != 1)
        {
            return Err(invalid("checkpoint.admission"));
        }
        let request = (
            run.snapshot.request.session_id.clone(),
            run.snapshot.request.request_id.clone(),
        );
        if state
            .requests
            .insert(request, run.snapshot.run_id.clone())
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_request"));
        }
        let run_id = run.snapshot.run_id.clone();
        if state
            .runs
            .insert(
                run_id,
                RunState {
                    snapshot: run.snapshot,
                    events: run.events,
                    lease: run.lease.map(Into::into),
                    last_fencing_token: run.last_fencing_token,
                },
            )
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_run"));
        }
    }
    let empty = BTreeMap::new();
    let mut message_ids = BTreeSet::new();
    for session in state.sessions.values() {
        super::context_state::validate_session(&state, session)?;
        record_value(&state, &empty, &session.snapshot.prompt_snapshot)?;
        let active: Vec<_> = state
            .runs
            .values()
            .filter(|run| {
                run.snapshot.request.session_id == session.snapshot.session_id
                    && !run.snapshot.status.is_terminal()
            })
            .collect();
        if active.len() > 1
            || active.first().map(|run| &run.snapshot.run_id)
                != session.snapshot.active_run_id.as_ref()
        {
            return Err(invalid("checkpoint.active_run"));
        }
        if !state
            .runs
            .values()
            .any(|run| run.snapshot.request.session_id == session.snapshot.session_id)
        {
            return Err(invalid("checkpoint.orphan_session"));
        }
        let mut sequence = 0;
        let mut seen_runs = BTreeSet::new();
        let mut previous_run = None;
        for message in &session.messages {
            let run = state
                .runs
                .get(&message.run_id)
                .ok_or_else(|| invalid("checkpoint.message_run"))?;
            if run.snapshot.request.session_id != session.snapshot.session_id
                || !message_ids.insert(message.message_id.clone())
            {
                return Err(invalid("checkpoint.message_identity"));
            }
            if previous_run != Some(&message.run_id) {
                if !seen_runs.insert(&message.run_id) {
                    return Err(invalid("checkpoint.message_run_order"));
                }
                previous_run = Some(&message.run_id);
            }
            sequence = validate_messages(
                &state,
                &empty,
                &message.run_id,
                sequence,
                std::slice::from_ref(message),
            )?;
        }
        if sequence != session.snapshot.transcript_revision {
            return Err(invalid("checkpoint.transcript_revision"));
        }
        if let Some(active_run) = &session.snapshot.active_run_id {
            if seen_runs.contains(active_run) && previous_run != Some(active_run) {
                return Err(invalid("checkpoint.active_run_order"));
            }
        }
    }
    for execution in execution_records {
        let run = state
            .runs
            .get(&execution.run_id)
            .ok_or_else(|| invalid("checkpoint.execution_run"))?;
        super::execution::validate_history(&execution, &run.snapshot)?;
        let events: Vec<_> = run.events.iter().collect();
        super::execution::validate_settlements(&state, &empty, &run.snapshot, &execution, &events)?;
        for segment in &execution.segments {
            let effects = match &segment.outcome {
                Some(crate::SegmentOutcome::Interrupted { interruption }) => {
                    &interruption.unresolved_effects
                }
                Some(crate::SegmentOutcome::Settled { outcome }) => &outcome.unresolved_effects,
                None => continue,
            };
            for reference in effects {
                record_value(&state, &BTreeMap::new(), reference)?;
            }
        }

        if state
            .executions
            .insert(execution.run_id.clone(), execution)
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_execution"));
        }
    }
    let mut event_ids = BTreeSet::new();
    for run in state.runs.values() {
        validate_snapshot_refs(&state, &empty, &run.snapshot)?;
        validate_history(&state, run, &mut event_ids)?;
    }
    for report in data.hook_observations {
        validate_hook_observation(&state, &data.scope, &report.run_id, &report)?;
        let reports = state
            .hook_observations
            .entry(report.run_id.clone())
            .or_default();
        if reports.iter().any(|existing| {
            existing.hook == report.hook
                && existing.selection == report.selection
                && existing.target == report.target
        }) {
            return Err(invalid("checkpoint.hook_observation_duplicate"));
        }
        reports.push(report);
    }
    state.message_ids = message_ids;
    state.event_ids = event_ids;
    if data.schema_version == LEGACY_CHECKPOINT_VERSION {
        state.legacy_runs = state.runs.keys().cloned().collect();
    } else {
        for id in data.legacy_runs {
            let run = state
                .runs
                .get(&id)
                .ok_or_else(|| invalid("checkpoint.legacy_run"))?;
            if !run.snapshot.status.is_terminal()
                || state.executions.contains_key(&id)
                || !state.legacy_runs.insert(id)
            {
                return Err(invalid("checkpoint.legacy_run"));
            }
        }
        if state
            .runs
            .keys()
            .any(|id| !state.executions.contains_key(id) && !state.legacy_runs.contains(id))
        {
            return Err(invalid("checkpoint.execution_missing"));
        }
    }
    Ok(StateStoreCheckpoint {
        scope: data.scope,
        state,
    })
}

fn validate_history(
    state: &ScopeState,
    run: &RunState,
    event_ids: &mut BTreeSet<Id>,
) -> Result<(), ContractError> {
    super::recovery_state::history(state, &run.snapshot, &run.events)?;
    super::context_state::validate_history(state, &run.snapshot, &run.events)?;
    super::verification_state::history(state, &run.snapshot, &run.events)?;
    let empty = BTreeMap::new();
    let mut sequence = 0_u64;
    let mut started = 0;
    let mut finished = 0;
    let mut resumed = 0;
    let mut unresolved_keys = BTreeMap::new();
    for event in &run.events {
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| invalid("checkpoint.event_sequence"))?;
        if event.scope != run.snapshot.scope
            || event.run_id != run.snapshot.run_id
            || event.session_id != run.snapshot.request.session_id
            || event.seq.get() != sequence
            || !event_ids.insert(event.event_id.clone())
        {
            return Err(invalid("checkpoint.event_identity"));
        }
        match &event.payload {
            RunEventPayload::ContextRewritten { revision_ref } => {
                let revision =
                    super::context_state::revision(state, &empty, revision_ref, &run.snapshot)?;
                if revision.run_id != run.snapshot.run_id {
                    return Err(invalid("checkpoint.context_event"));
                }
            }
            RunEventPayload::RunRecovered {
                recovery_receipt_ref,
            } => {
                super::recovery_state::event(
                    state,
                    &empty,
                    &run.snapshot,
                    event,
                    recovery_receipt_ref,
                )?;
            }
            RunEventPayload::RunStarted {
                request_ref,
                profile_digest,
            } => {
                started += 1;
                let request: RunRequest = event_record(state, &empty, request_ref)?;
                if sequence != 1
                    || request != run.snapshot.request
                    || profile_digest != run.snapshot.profile.profile_digest()
                {
                    return Err(invalid("checkpoint.run_started"));
                }
            }
            RunEventPayload::RunInterrupted {
                outcome_ref,
                decision_ref,
            } => {
                let outcome: RunOutcome = event_record(state, &empty, outcome_ref)?;
                interruption_state::validate_event(
                    state,
                    &empty,
                    &run.snapshot,
                    &outcome,
                    decision_ref,
                )?;
            }
            RunEventPayload::RunFinished { outcome_ref } => {
                finished += 1;
                let outcome: RunOutcome = event_record(state, &empty, outcome_ref)?;
                if !run.snapshot.status.is_terminal()
                    || run.snapshot.outcome.as_ref() != Some(&outcome)
                {
                    return Err(invalid("checkpoint.run_finished"));
                }
            }
            RunEventPayload::ToolPlanned { call_ref } => {
                let mut call: ToolCall = event_record(state, &empty, call_ref)?;
                let current = run
                    .snapshot
                    .tool_ledger
                    .iter()
                    .find(|entry| entry.call.call_id == call.call_id)
                    .ok_or_else(|| invalid("checkpoint.tool_planned"))?;
                if call.bound_input_ref.is_none() {
                    call.bound_input_ref = current.call.bound_input_ref.clone();
                }
                if call != current.call {
                    return Err(invalid("checkpoint.tool_planned"));
                }
            }
            RunEventPayload::ToolReconciled { reconciliation_ref } => {
                record_value(state, &empty, reconciliation_ref)?;
            }
            RunEventPayload::ToolSettled { result_ref } => {
                let result: ToolResult = event_record(state, &empty, result_ref)?;
                if !run.snapshot.tool_ledger.iter().any(|entry| matches!(&entry.state, ToolCallState::Settled { result: current } if current == &result)) {
                    return Err(invalid("checkpoint.tool_settled"));
                }
                validate_tool_pair(state, &run.snapshot, &[], &result)?;
                for reference in tool_result_refs(&result) {
                    record_value(state, &empty, reference)?;
                }
            }
            RunEventPayload::ToolUnresolved {
                result_ref,
                attempt_id,
                idempotency_key,
            } => {
                let result: ToolResult = event_record(state, &empty, result_ref)?;
                if result.status != crate::ToolResultStatus::Unknown || result.effect != crate::ToolEffect::Unknown
                    || !run.snapshot.tool_ledger.iter().any(|entry| entry.call.call_id == result.call_id)
                    || !run.snapshot.reservations.iter().any(|reservation| &reservation.attempt_id == attempt_id
                        && matches!(&reservation.kind, crate::ReservationKind::Tool { call_id } if call_id == &result.call_id))
                {
                    return Err(invalid("checkpoint.tool_unresolved"));
                }
                let entry = run
                    .snapshot
                    .tool_ledger
                    .iter()
                    .find(|entry| entry.call.call_id == result.call_id)
                    .expect("call membership checked above");
                let current_key = match &entry.state {
                    ToolCallState::Dispatching {
                        idempotency_key, ..
                    }
                    | ToolCallState::ApprovalPending {
                        idempotency_key, ..
                    }
                    | ToolCallState::Unknown {
                        idempotency_key, ..
                    } => Some(idempotency_key),
                    _ => None,
                };
                if current_key.is_some_and(|key| key != idempotency_key)
                    || unresolved_keys
                        .insert(result.call_id.clone(), idempotency_key)
                        .is_some_and(|key| key != idempotency_key)
                {
                    return Err(invalid("checkpoint.tool_unresolved_key"));
                }
                validate_tool_pair(state, &run.snapshot, &[], &result)?;
                for reference in tool_result_refs(&result) {
                    record_value(state, &empty, reference)?;
                }
            }
            RunEventPayload::VerificationCompleted { verification_ref } => {
                let verification: VerificationSummary =
                    event_record(state, &empty, verification_ref)?;
                for reference in &verification.evidence {
                    record_value(state, &empty, reference)?;
                }
            }
            RunEventPayload::RunWaiting { wait_ref, .. } => {
                let wait: WaitState = event_record(state, &empty, wait_ref)?;
                if let WaitTarget::Approval {
                    target: ApprovalTarget::Candidate { candidate_ref, .. },
                } = &wait.target
                {
                    record_value(state, &empty, candidate_ref)?;
                }
            }
            RunEventPayload::RunResumed { command_ref } => {
                resumed += 1;
                let command: ResumeCommand = event_record(state, &empty, command_ref)?;
                if command.run_id != run.snapshot.run_id
                    || command.expected_revision >= run.snapshot.revision
                    || !run.snapshot.resume_receipts.iter().any(|receipt| {
                        receipt.command_ref == *command_ref
                            && receipt.command == command
                            && event.seq.get() > receipt.previous_last_event_seq
                    })
                {
                    return Err(invalid("checkpoint.run_resumed"));
                }
                let reference = match &command.action {
                    ResumeAction::External { receipt_ref, .. } => Some(receipt_ref),
                    ResumeAction::Recover { recovery_ref } => Some(recovery_ref),
                    ResumeAction::Approve {
                        target: ApprovalTarget::Candidate { candidate_ref, .. },
                        ..
                    }
                    | ResumeAction::Deny {
                        target: ApprovalTarget::Candidate { candidate_ref, .. },
                        ..
                    } => Some(candidate_ref),
                    _ => None,
                };
                if let Some(reference) = reference {
                    record_value(state, &empty, reference)?;
                }
            }
            RunEventPayload::ModelRouteSelected {
                invocation_ref,
                route_digest,
            } => {
                let old: ModelInvocationRecord = event_record(state, &empty, invocation_ref)?;
                let current = run
                    .snapshot
                    .model_ledger
                    .iter()
                    .find(|current| current.attempt_id == old.attempt_id)
                    .ok_or_else(|| invalid("checkpoint.model_route"))?;
                if old.run_id != current.run_id
                    || old.model_step_id != current.model_step_id
                    || old.purpose != current.purpose
                    || old.route != current.route
                    || old.selection_reason != current.selection_reason
                    || old.request_digest != current.request_digest
                    || old.inspection_ref != current.inspection_ref
                    || old.route.digest() != *route_digest
                    || (matches!(
                        old.state,
                        ModelAttemptState::Completed {}
                            | ModelAttemptState::Failed { .. }
                            | ModelAttemptState::Interrupted { .. }
                    ) && &old != current)
                    || (matches!(old.state, ModelAttemptState::Unknown {})
                        && matches!(current.state, ModelAttemptState::Reserved {}))
                {
                    return Err(invalid("checkpoint.model_route"));
                }
                if let Some(reference) = &old.response_ref {
                    validate_model_response(state, &empty, &old, reference)?;
                }
            }
        }
    }
    if started != 1
        || sequence != run.snapshot.last_event_seq
        || finished != usize::from(run.snapshot.status.is_terminal())
        || resumed != run.snapshot.resume_receipts.len()
    {
        return Err(invalid("checkpoint.events"));
    }
    interruption_state::validate_history_events(state, &empty, &run.snapshot, &run.events)?;
    let events: Vec<_> = run.events.iter().collect();
    let messages: Vec<_> = state
        .sessions
        .get(&run.snapshot.request.session_id)
        .ok_or_else(|| invalid("checkpoint.session"))?
        .messages
        .iter()
        .collect();
    validate_resume_history(state, &empty, &run.snapshot, &events, &messages)?;
    Ok(())
}

fn invalid(path: &str) -> ContractError {
    error(ErrorCode::InvalidSnapshot, path)
}
