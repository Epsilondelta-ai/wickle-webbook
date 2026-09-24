use super::*;

impl SerialToolRound {
    /// Prepare a denied observation for an exact saved approval candidate. The
    /// Agent must authorize and atomically consume the matching Deny command.
    pub fn prepare_denial(
        &self,
        saved: &StoredRun,
        call_id: &Id,
        bound: &BoundToolInput,
        code: Id,
        now_ms: i64,
    ) -> Result<PreparedToolResolution, ContractError> {
        let (entry, _) = self.resolution_call(saved, call_id, bound)?;
        if !matches!(
            entry.state,
            ToolCallState::Planned {} | ToolCallState::ApprovalPending { .. }
        ) || !matches!(saved.snapshot.wait.as_ref().map(|wait| &wait.target),
                Some(WaitTarget::Approval { target: ApprovalTarget::Tool { call_id: target, binding_digest } })
                    if target == call_id && binding_digest == bound.binding_digest())
        {
            return Err(error(ErrorCode::InvalidTransition, "tool.denial"));
        }
        self.prepare_unstarted(saved, call_id, ToolResultStatus::Denied, code, now_ms)
    }

    /// Prepare cancellation or rejection of a saved no-effect call, including an
    /// unknown tool name whose binding was never created. The Agent authorizes
    /// the operation and commits all settlements with the terminal outcome; an
    /// uncertain or potentially dispatched call cannot use this path.
    pub fn prepare_unstarted(
        &self,
        saved: &StoredRun,
        call_id: &Id,
        status: ToolResultStatus,
        code: Id,
        now_ms: i64,
    ) -> Result<PreparedToolResolution, ContractError> {
        if saved.snapshot.scope != *self.registry.scope() {
            return Err(error(ErrorCode::AccessDenied, "tool.resume_scope"));
        }
        let entry = saved
            .snapshot
            .tool_ledger
            .iter()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool.resume_call"))?;
        if saved.snapshot.status != RunStatus::Waiting
            || !matches!(
                status,
                ToolResultStatus::Failed | ToolResultStatus::Denied | ToolResultStatus::Cancelled
            )
            || !matches!(
                entry.state,
                ToolCallState::Planned {}
                    | ToolCallState::ApprovalPending { .. }
                    | ToolCallState::InputPending { .. }
            )
        {
            return Err(error(ErrorCode::InvalidTransition, "tool.unstarted"));
        }
        let result = ToolResult {
            call_id: call_id.clone(),
            call_message_id: round::call_message(saved, &entry.call)?,
            status,
            effect: ToolEffect::NotApplied,
            content: vec![],
            effect_receipt_ref: None,
            skill_ref: None,
            error: Some(Failure {
                code,
                diagnostic_ref: None,
            }),
        };
        self.prepared_resolution(saved, result, vec![], None, now_ms)
    }

    /// Validate an answer against the original compiled output schema and prepare
    /// the original call's completion. Invalid answers do not consume the wait;
    /// neither the input resolver nor the tool executor is invoked.
    pub fn prepare_input(
        &self,
        saved: &StoredRun,
        request: &InputRequest,
        bound: &BoundToolInput,
        answer: Value,
        now_ms: i64,
    ) -> Result<PreparedToolResolution, ContractError> {
        let (entry, compiled) = self.resolution_call(saved, &request.call_id, bound)?;
        let ToolCallState::InputPending {
            attempt_id,
            idempotency_key,
            request: pending,
        } = &entry.state
        else {
            return Err(error(ErrorCode::InvalidTransition, "tool.input"));
        };
        if pending != request
            || request.schema_ref.is_some()
            || !matches!(saved.snapshot.wait.as_ref().map(|wait| &wait.target),
                Some(WaitTarget::Input { request: waiting }) if waiting == request)
        {
            return Err(error(ErrorCode::InvalidReference, "tool.input_request"));
        }
        let bytes = serde_json::to_vec(&answer)
            .map_err(|_| error(ErrorCode::InvalidJson, "tool.input_answer"))?
            .len();
        if bytes as u64 > compiled.descriptor().max_output_bytes.get()
            || !crate::tool_schema::compile_validator(&compiled.descriptor().output_schema)?
                .is_valid(&answer)
        {
            return Err(error(ErrorCode::InvalidArguments, "tool.input_answer"));
        }
        let (result, records) = self.validate_output(
            &entry.call,
            round::call_message(saved, &entry.call)?,
            AttemptIdentity {
                scope: &saved.snapshot.scope,
                run_id: &saved.snapshot.run_id,
                attempt_id,
                idempotency_key,
            },
            compiled,
            bound,
            ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded { value: answer },
                effect: ToolEffect::NotApplied,
                receipt: None,
            },
        )?;
        self.prepared_resolution(saved, result, records, None, now_ms)
    }

    /// Prepare a correction from a trusted, authorized read-only verification.
    /// Unknown cannot consume the wait. Output errors preserve confirmed effects
    /// and receipt evidence, and never trigger a new business execution.
    pub fn prepare_external(
        &self,
        saved: &StoredRun,
        call_id: &Id,
        bound: &BoundToolInput,
        verified: ToolExecutionResult,
        now_ms: i64,
    ) -> Result<PreparedToolResolution, ContractError> {
        let (entry, compiled) = self.resolution_call(saved, call_id, bound)?;
        let ToolCallState::Unknown {
            attempt_id,
            idempotency_key,
        } = &entry.state
        else {
            return Err(error(ErrorCode::InvalidTransition, "tool.external"));
        };
        if !matches!(saved.snapshot.wait.as_ref().map(|wait| &wait.target),
            Some(WaitTarget::External { call_id: target, effect_key })
                if target == call_id && effect_key == idempotency_key)
        {
            return Err(error(ErrorCode::InvalidReference, "tool.external_wait"));
        }
        if verified.effect == ToolEffect::Unknown {
            return Err(error(ErrorCode::ToolEffectUnresolved, "tool.external"));
        }
        let call_message_id = round::call_message(saved, &entry.call)?;
        let mut previous = saved.messages.iter().flat_map(|message| {
            message.content.iter().filter_map(|content| match content {
                ContentBlock::ToolResult { result }
                    if message.run_id == saved.snapshot.run_id
                        && message.role == MessageRole::Tool
                        && message.origin == MessageOrigin::Tool
                        && result.call_id == *call_id
                        && result.call_message_id == call_message_id
                        && result.status == ToolResultStatus::Unknown
                        && result.effect == ToolEffect::Unknown =>
                {
                    Some((message.message_id.clone(), result))
                }
                _ => None,
            })
        });
        let (previous_message_id, previous_result) = previous
            .next()
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.unknown_result"))?;
        if previous.next().is_some() {
            return Err(error(ErrorCode::InvalidSnapshot, "tool.unknown_result"));
        }
        let previous_result_digest = canonical_digest(
            &serde_json::to_value(previous_result)
                .map_err(|_| error(ErrorCode::InvalidJson, "tool.unknown_result"))?,
        );
        let (result, records) = self.validate_output(
            &entry.call,
            call_message_id,
            AttemptIdentity {
                scope: &saved.snapshot.scope,
                run_id: &saved.snapshot.run_id,
                attempt_id,
                idempotency_key,
            },
            compiled,
            bound,
            verified,
        )?;
        self.prepared_resolution(
            saved,
            result,
            records,
            Some((previous_message_id, previous_result_digest)),
            now_ms,
        )
    }

    fn resolution_call<'a>(
        &'a self,
        saved: &'a StoredRun,
        call_id: &Id,
        bound: &BoundToolInput,
    ) -> Result<(&'a ToolLedgerEntry, &'a CompiledTool), ContractError> {
        if saved.snapshot.scope != *self.registry.scope() {
            return Err(error(ErrorCode::AccessDenied, "tool.resume_scope"));
        }
        if saved.snapshot.status != RunStatus::Waiting {
            return Err(error(ErrorCode::InvalidTransition, "tool.resume_status"));
        }
        let entry = saved
            .snapshot
            .tool_ledger
            .iter()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool.resume_call"))?;
        let compiled = &self
            .registry
            .get(&entry.call.tool_name)
            .ok_or_else(|| error(ErrorCode::ComponentUnavailable, "tool.resume_contract"))?
            .compiled;
        // Reconstruct only to verify the exact immutable record reference. This
        // neither substitutes for the Agent's scoped retrieval nor performs I/O.
        let reference = entry
            .call
            .bound_input_ref
            .as_ref()
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool.bound_input"))?;
        let record = ProtectedRecord::new(
            reference.record_id.clone(),
            reference.revision,
            serde_json::to_value(bound)
                .map_err(|_| error(ErrorCode::InvalidJson, "tool.bound_input"))?,
        );
        BoundToolInput::restore(
            &record,
            compiled,
            &saved.snapshot.scope,
            &saved.snapshot.run_id,
            &entry.call,
            saved.snapshot.system_inputs.as_ref(),
        )?;
        Ok((entry, compiled))
    }

    pub(super) fn prepared_resolution(
        &self,
        saved: &StoredRun,
        result: ToolResult,
        mut records: Vec<ProtectedRecord>,
        correction: Option<(Id, JsonDigest)>,
        now_ms: i64,
    ) -> Result<PreparedToolResolution, ContractError> {
        if now_ms < saved.snapshot.timing.last_observed_at_ms {
            return Err(error(ErrorCode::ClockRegression, "tool.resume_time"));
        }
        let record = ProtectedRecord::new(
            self.ids.next_id()?,
            1,
            serde_json::to_value(&result)
                .map_err(|_| error(ErrorCode::InvalidJson, "tool.result"))?,
        );
        let result_ref = record.reference().clone();
        records.push(record);
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: self.ids.next_id()?,
            scope: saved.snapshot.scope.clone(),
            run_id: saved.snapshot.run_id.clone(),
            session_id: saved.snapshot.request.session_id.clone(),
            seq: saved
                .snapshot
                .last_event_seq
                .checked_add(1)
                .and_then(std::num::NonZeroU64::new)
                .ok_or_else(|| error(ErrorCode::InvalidEvent, "tool.resume_event"))?,
            timestamp_ms: now_ms,
            payload: RunEventPayload::ToolSettled { result_ref },
        };
        let content = if let Some((previous_message_id, previous_result_digest)) = correction {
            ContentBlock::ToolResultCorrection {
                previous_message_id,
                previous_result_digest,
                result: result.clone(),
            }
        } else {
            ContentBlock::ToolResult {
                result: result.clone(),
            }
        };
        let message = Message {
            message_id: self.ids.next_id()?,
            run_id: saved.snapshot.run_id.clone(),
            sequence: saved
                .session
                .transcript_revision
                .checked_add(1)
                .and_then(std::num::NonZeroU64::new)
                .ok_or_else(|| error(ErrorCode::InvalidMessage, "tool.resume_message"))?,
            role: MessageRole::Tool,
            content: vec![content],
            origin: MessageOrigin::Tool,
            visibility: Visibility::UserAndModel,
        };
        Ok(PreparedToolResolution {
            state: ToolCallState::Settled {
                result: result.clone(),
            },
            result,
            message,
            records,
            event,
        })
    }
}
