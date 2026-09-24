use super::*;
use futures_util::FutureExt;
use std::{panic::AssertUnwindSafe, time::Duration};

impl SerialToolRound {
    /// Read a prior attempt's effect under current policy and the recovery budget.
    /// This never calls execute or changes its frozen inputs. A Known observation
    /// is not yet a settled result: output/receipt validation and atomic storage
    /// must complete before the driver may continue.
    pub async fn inspect_effect(
        &self,
        call_id: &Id,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<ToolReconciliation, ContractError> {
        self.inspect_effect_reserved(call_id, context, budget)
            .await
            .map(|(result, _)| result)
    }
    async fn inspect_effect_reserved(
        &self,
        call_id: &Id,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<(ToolReconciliation, Option<Id>), ContractError> {
        if context.cancellation.is_cancelled() {
            return Err(error(ErrorCode::Cancelled, "tool.reconcile"));
        }
        if self.registry.scope() != budget.scope() || &context.data.scope != budget.scope() {
            return Err(error(ErrorCode::AccessDenied, "tool.reconcile_scope"));
        }
        budget.check_boundary().await?;
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let entry = saved
            .snapshot
            .tool_ledger
            .iter()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool.reconcile_call"))?;
        let (attempt_id, idempotency_key) = match &entry.state {
            ToolCallState::Dispatching {
                attempt_id,
                idempotency_key,
            }
            | ToolCallState::Unknown {
                attempt_id,
                idempotency_key,
            } => (attempt_id.clone(), idempotency_key.clone()),
            _ => return Err(error(ErrorCode::InvalidTransition, "tool.reconcile_state")),
        };
        let registration = self
            .registry
            .get(&entry.call.tool_name)
            .ok_or_else(|| error(ErrorCode::ComponentUnavailable, "tool.reconcile_contract"))?;
        if !registration.compiled.descriptor().reconcile {
            return Ok((ToolReconciliation::Unknown, None));
        }
        let reference = entry
            .call
            .bound_input_ref
            .as_ref()
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.reconcile_binding"))?;
        let record = budget
            .store()
            .read_record(budget.scope(), reference)
            .await?;
        if record.reference() != reference {
            return Err(error(ErrorCode::InvalidSnapshot, "tool.reconcile_binding"));
        }
        let bound = BoundToolInput::restore(
            &record,
            &registration.compiled,
            budget.scope(),
            budget.run_id(),
            &entry.call,
            saved.snapshot.system_inputs.as_ref(),
        )?;
        let mut input = ToolPolicyInput::new(
            call_id.clone(),
            registration.compiled.descriptor().tool.clone(),
            registration.compiled.descriptor_digest().clone(),
            bound.binding_digest().clone(),
            bound.execution_args().clone(),
        );
        if let Some(selection) = self.registry.selection(&entry.call.tool_name) {
            input = input.with_selection(selection.clone());
        }
        let request = PolicyRequest {
            owner_scope: budget.scope().clone(),
            resource_id: call_id.clone(),
            action: PolicyAction::ReconcileTool {
                input,
                attempt_id: attempt_id.clone(),
                idempotency_key: idempotency_key.clone(),
            },
        };
        let deadline = budget
            .call_deadline()?
            .min(tokio::time::Instant::now() + Duration::from_millis(self.limits.timeout_ms));
        match self
            .policy
            .check(&request, context, Some(deadline), None)
            .await?
        {
            PolicyDecision::Allow {} => {}
            _ => return Err(error(ErrorCode::AccessDenied, "tool.reconcile_policy")),
        }
        let reservation = budget.reserve(ReservationKind::Recovery {}).await?;
        budget.check_boundary().await?;
        let cancellation = context.cancellation.child_token();
        let _cancel = cancellation.clone().drop_guard();
        let execution = ToolExecutionContext {
            run_id: budget.run_id().clone(),
            binding_set_id: self.binding_set_id.clone(),
            call_id: call_id.clone(),
            attempt_id,
            idempotency_key,
            scope: budget.scope().clone(),
            principal_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
            cancellation: cancellation.clone(),
            deadline,
        };
        let result = tokio::select! {biased;
            _=cancellation.cancelled()=>return Err(error(ErrorCode::Cancelled,"tool.reconcile")),
            result=budget.wait_for_cancellation_or_deadline()=>return result.and_then(|_|Err(error(ErrorCode::DeadlineExceeded,"tool.reconcile"))),
            _=tokio::time::sleep_until(deadline)=>return Ok((ToolReconciliation::Unknown,Some(reservation.attempt_id))),
            result=AssertUnwindSafe(registration.executor.reconcile(bound.execution_args(),&execution)).catch_unwind()=>result.unwrap_or_else(|_|Err(error(ErrorCode::InvalidContract,"tool.reconcile")))?,
        };
        budget.check_boundary().await?;
        match self
            .policy
            .check(&request, context, Some(deadline), None)
            .await?
        {
            PolicyDecision::Allow {} => Ok((result, Some(reservation.attempt_id))),
            _ => Err(error(ErrorCode::AccessDenied, "tool.reconcile_policy")),
        }
    }
}

impl SerialToolRound {
    /// Reconcile one uncertain attempt and atomically persist its observation.
    /// A previous Unknown observation is corrected rather than overwritten. A
    /// replay of a settled call performs no additional query or execution.
    pub async fn reconcile_call(
        &self,
        call_id: &Id,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<ToolResult, ContractError> {
        if self.registry.scope() != budget.scope() || &context.data.scope != budget.scope() {
            return Err(error(ErrorCode::AccessDenied, "tool.reconcile_scope"));
        }
        budget.check_boundary().await?;
        let before = budget.store().load(budget.scope(), budget.run_id()).await?;
        let prior = before
            .snapshot
            .tool_ledger
            .iter()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool.reconcile_call"))?;
        if let ToolCallState::Settled { result } = &prior.state {
            return Ok(result.clone());
        }
        let (observation, recovery_attempt_id) = self
            .inspect_effect_reserved(call_id, context, budget)
            .await?;
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let entry = saved
            .snapshot
            .tool_ledger
            .iter()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool.reconcile_call"))?;
        if let ToolCallState::Settled { result } = &entry.state {
            return Ok(result.clone());
        }
        if entry != prior {
            return Err(error(ErrorCode::RevisionConflict, "tool.reconcile_source"));
        }
        let (attempt_id, key) = match &entry.state {
            ToolCallState::Dispatching {
                attempt_id,
                idempotency_key,
            }
            | ToolCallState::Unknown {
                attempt_id,
                idempotency_key,
            } => (attempt_id, idempotency_key),
            _ => return Err(error(ErrorCode::InvalidTransition, "tool.reconcile_state")),
        };
        let call_message_id = round::call_message(&saved, &entry.call)?;
        let completion = match observation {
            ToolReconciliation::Known { result } if result.effect != ToolEffect::Unknown => {
                Some(result)
            }
            _ => None,
        };
        let Some(completion) = completion else {
            if matches!(entry.state, ToolCallState::Unknown { .. }) {
                return saved
                    .messages
                    .iter()
                    .flat_map(|message| &message.content)
                    .find_map(|block| match block {
                        ContentBlock::ToolResult { result }
                            if &result.call_id == call_id
                                && result.effect == ToolEffect::Unknown =>
                        {
                            Some(result.clone())
                        }
                        _ => None,
                    })
                    .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.reconcile_unknown"));
            }
            let result = ToolResult {
                call_id: call_id.clone(),
                call_message_id,
                status: ToolResultStatus::Unknown,
                effect: ToolEffect::Unknown,
                content: vec![],
                effect_receipt_ref: None,
                skill_ref: None,
                error: Some(Failure {
                    code: Id::new("recovery_unconfirmed")?,
                    diagnostic_ref: None,
                }),
            };
            self.settle(
                call_id,
                ToolCallState::Unknown {
                    attempt_id: attempt_id.clone(),
                    idempotency_key: key.clone(),
                },
                result.clone(),
                vec![],
                budget,
                context,
            )
            .await?;
            return Ok(result);
        };
        let compiled = &self
            .registry
            .get(&entry.call.tool_name)
            .ok_or_else(|| error(ErrorCode::ComponentUnavailable, "tool.reconcile_contract"))?
            .compiled;
        let input_ref = entry
            .call
            .bound_input_ref
            .as_ref()
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.reconcile_binding"))?;
        let bound = BoundToolInput::restore(
            &budget
                .store()
                .read_record(budget.scope(), input_ref)
                .await?,
            compiled,
            budget.scope(),
            budget.run_id(),
            &entry.call,
            saved.snapshot.system_inputs.as_ref(),
        )?;
        let (mut result, records) = self.validate_output(
            &entry.call,
            call_message_id,
            AttemptIdentity {
                scope: budget.scope(),
                run_id: budget.run_id(),
                attempt_id,
                idempotency_key: key,
            },
            compiled,
            &bound,
            completion,
        )?;
        self.validate_artifact_result(&mut result, context, budget.call_deadline()?)
            .await;
        if matches!(entry.state, ToolCallState::Dispatching { .. }) {
            self.settle(
                call_id,
                ToolCallState::Settled {
                    result: result.clone(),
                },
                result.clone(),
                records,
                budget,
                context,
            )
            .await?;
            return Ok(result);
        }
        let mut originals = saved.messages.iter().filter_map(|message| {
            message.content.iter().find_map(|block| match block {
                ContentBlock::ToolResult { result }
                    if &result.call_id == call_id && result.effect == ToolEffect::Unknown =>
                {
                    Some((message.message_id.clone(), result))
                }
                _ => None,
            })
        });
        let (message_id, original) = originals
            .next()
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.reconcile_original"))?;
        if originals.next().is_some() {
            return Err(error(ErrorCode::InvalidSnapshot, "tool.reconcile_original"));
        }
        let digest = canonical_digest(
            &serde_json::to_value(original)
                .map_err(|_| error(ErrorCode::InvalidJson, "tool.reconcile_original"))?,
        );
        let (_, now) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
        let mut prepared = self.prepared_resolution(
            &saved,
            result.clone(),
            records,
            Some((message_id, digest)),
            now,
        )?;
        let RunEventPayload::ToolSettled { result_ref } = &prepared.event.payload else {
            unreachable!("prepared settlement");
        };
        let evidence = ReconciliationRecord {
            schema_version: "wickle.tool-reconciliation.v1".into(),
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            call_id: call_id.clone(),
            attempt_id: attempt_id.clone(),
            idempotency_key: key.clone(),
            binding_ref: input_ref.clone(),
            recovery_attempt_id: recovery_attempt_id
                .ok_or_else(|| error(ErrorCode::InvalidContract, "tool.reconcile_reservation"))?,
            result_ref: result_ref.clone(),
            correction_message_id: prepared.message.message_id.clone(),
            actor_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
        };
        let record = ProtectedRecord::new(
            self.ids.next_id()?,
            1,
            serde_json::to_value(evidence)
                .map_err(|_| error(ErrorCode::InvalidJson, "tool.reconcile_record"))?,
        );
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: self.ids.next_id()?,
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            session_id: saved.snapshot.request.session_id.clone(),
            seq: prepared.event.seq,
            timestamp_ms: now,
            payload: RunEventPayload::ToolReconciled {
                reconciliation_ref: record.reference().clone(),
            },
        };
        prepared.event.seq = (prepared.event.seq.get() + 1)
            .try_into()
            .map_err(|_| error(ErrorCode::InvalidEvent, "tool.reconcile_event"))?;
        prepared.records.push(record);
        let mut snapshot = saved.snapshot;
        snapshot
            .tool_ledger
            .iter_mut()
            .find(|entry| &entry.call.call_id == call_id)
            .expect("validated call")
            .state = prepared.state.clone();
        snapshot.last_event_seq = prepared.event.seq.get();
        let expected = prepared.state;
        if let Err(error) = self
            .commit(
                snapshot,
                vec![prepared.message],
                vec![event, prepared.event],
                prepared.records,
                budget,
            )
            .await
        {
            if !budget
                .store()
                .load(budget.scope(), budget.run_id())
                .await
                .is_ok_and(|saved| {
                    saved
                        .snapshot
                        .tool_ledger
                        .iter()
                        .any(|entry| &entry.call.call_id == call_id && entry.state == expected)
                })
            {
                return Err(error);
            }
        }
        Ok(result)
    }
}
