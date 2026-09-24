use super::*;

impl Agent {
    pub(super) async fn recover_command(
        &self,
        command: ResumeCommand,
        context: ExecutionContext,
    ) -> Result<Guarded<RunHandle>, ContractError> {
        let bindings = &self.inner.bindings;
        let saved = self
            .resume_read(
                &context,
                bindings.state.load(&bindings.scope, &command.run_id),
            )
            .await?;
        if let Guarded::ApprovalRequired(challenge) =
            self.authorize_resume(&command, &context, None).await?
        {
            return Ok(Guarded::ApprovalRequired(challenge));
        }
        self.resume_inputs(&saved.snapshot, &context).await?;
        if let Some(receipt) = replayed(&saved.snapshot, &command)? {
            return Ok(Guarded::Completed(
                self.handle(command.run_id, receipt.accepted_revision)?,
            ));
        }
        validate_source(&saved.snapshot, &command)?;
        let lease = bindings
            .state
            .acquire_lease(
                &bindings.scope,
                &command.run_id,
                &bindings.ids.next_id()?,
                bindings.clock.now()?.utc_ms,
                bindings.settings.lease_ttl_ms,
            )
            .await?;
        let result = self.accept_recovery(&command, &context, &lease).await;
        match result {
            Ok((receipt, prompt, true)) => Ok(Guarded::Completed(self.launch_segment(
                command.run_id,
                (receipt.accepted_revision, receipt.expired),
                prompt,
                context,
                lease,
                vec![],
            )?)),
            Ok((receipt, _, false)) => {
                self.release_owned(&command.run_id, &lease).await;
                Ok(Guarded::Completed(
                    self.handle(command.run_id, receipt.accepted_revision)?,
                ))
            }
            Err(error) => {
                self.release_owned(&command.run_id, &lease).await;
                Err(error)
            }
        }
    }
    async fn accept_recovery(
        &self,
        command: &ResumeCommand,
        context: &ExecutionContext,
        lease: &RunLease,
    ) -> Result<(RecoveryReceipt, PromptSnapshot, bool), ContractError> {
        let bindings = &self.inner.bindings;
        let saved = self
            .resume_read(
                context,
                bindings.state.load(&bindings.scope, &command.run_id),
            )
            .await?;
        let prompt = self.restore_resume_runtime(&saved, context).await?;
        if let Some(receipt) = replayed(&saved.snapshot, command)? {
            return Ok((receipt.clone(), prompt, false));
        }
        let source = validate_source(&saved.snapshot, command)?;
        self.resume_inputs(&saved.snapshot, context).await?;
        let budget = RunBudget::attach(
            bindings.state.clone(),
            bindings.clock.clone(),
            bindings.ids.clone(),
            bindings.scope.clone(),
            command.run_id.clone(),
            lease.clone(),
            context.cancellation.clone(),
        )
        .await?;
        if let Guarded::ApprovalRequired(_) = self.authorize_resume(command, context, None).await? {
            return Err(fail(ErrorCode::AccessDenied, "recovery.policy"));
        }
        if context.cancellation.is_cancelled() {
            return Err(fail(ErrorCode::Cancelled, "recovery.accept"));
        }
        let (elapsed, now) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
        let expired = now >= saved.snapshot.timing.deadline_at_ms;
        let mut usage = saved.snapshot.usage.clone();
        let reservation = if expired {
            None
        } else {
            crate::budget::charge(
                &mut usage,
                &saved.snapshot.limits,
                &ReservationKind::Recovery {},
            )?;
            Some(AttemptReservation {
                attempt_id: bindings.ids.next_id()?,
                kind: ReservationKind::Recovery {},
                reserved_at_ms: now,
            })
        };
        let command_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(command)
                .map_err(|_| fail(ErrorCode::InvalidJson, "recovery.command"))?,
        );
        let receipt = RecoveryReceipt {
            command: command.clone(),
            command_ref: command_record.reference().clone(),
            source_snapshot_ref: source.reference().clone(),
            accepted_revision: saved
                .snapshot
                .revision
                .checked_add(1)
                .ok_or_else(|| fail(ErrorCode::RevisionConflict, "recovery.revision"))?,
            previous_segment_start_revision: segment_revision(&saved.snapshot),
            previous_last_event_seq: saved.snapshot.last_event_seq,
            actor_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
            expired,
            recovery_attempt_id: reservation.as_ref().map(|value| value.attempt_id.clone()),
        };
        let record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(&receipt)
                .map_err(|_| fail(ErrorCode::InvalidJson, "recovery.receipt"))?,
        );
        let interrupted = saved
            .snapshot
            .tool_ledger
            .iter()
            .filter_map(|entry| {
                let ToolCallState::Dispatching {
                    attempt_id,
                    idempotency_key,
                } = &entry.state
                else {
                    return None;
                };
                Some(
                    crate::tool_execution::call_message(&saved, &entry.call).map(
                        |call_message_id| {
                            (
                                ToolResult {
                                    call_id: entry.call.call_id.clone(),
                                    call_message_id,
                                    status: ToolResultStatus::Unknown,
                                    effect: ToolEffect::Unknown,
                                    content: vec![],
                                    effect_receipt_ref: None,
                                    skill_ref: None,
                                    error: Some(Failure {
                                        code: Id::new("recovery_unconfirmed")
                                            .expect("static identifier"),
                                        diagnostic_ref: None,
                                    }),
                                },
                                attempt_id.clone(),
                                idempotency_key.clone(),
                            )
                        },
                    ),
                )
            })
            .collect::<Result<Vec<_>, ContractError>>()?;
        let mut snapshot = saved.snapshot;
        snapshot.revision = receipt.accepted_revision;
        snapshot.last_event_seq += 1;
        snapshot.usage = usage;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.reservations.extend(reservation);
        if !expired {
            for attempt in &mut snapshot.model_ledger {
                if matches!(
                    attempt.state,
                    ModelAttemptState::Reserved {} | ModelAttemptState::Unknown {}
                ) {
                    attempt.state = ModelAttemptState::Interrupted {
                        recovery_command_id: command.command_id.clone(),
                    };
                }
            }
        }
        snapshot.timing.last_observed_at_ms = now;
        snapshot.recovery_receipts.push(receipt.clone());
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: bindings.ids.next_id()?,
            scope: bindings.scope.clone(),
            run_id: command.run_id.clone(),
            session_id: snapshot.request.session_id.clone(),
            seq: snapshot
                .last_event_seq
                .try_into()
                .map_err(|_| fail(ErrorCode::InvalidEvent, "recovery.sequence"))?,
            timestamp_ms: now,
            payload: RunEventPayload::RunRecovered {
                recovery_receipt_ref: record.reference().clone(),
            },
        };
        // Unknown observations belong to recovery acceptance itself: expiry or
        // component binding failure must not erase a possibly applied effect.
        let mut events = vec![event];
        let mut messages = vec![];
        let mut records = vec![source, command_record, record];
        for (result, attempt_id, idempotency_key) in interrupted {
            let entry = snapshot
                .tool_ledger
                .iter_mut()
                .find(|entry| entry.call.call_id == result.call_id)
                .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "recovery.tool"))?;
            entry.state = ToolCallState::Unknown {
                attempt_id: attempt_id.clone(),
                idempotency_key: idempotency_key.clone(),
            };
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(&result)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "recovery.tool_result"))?,
            );
            snapshot.last_event_seq = snapshot
                .last_event_seq
                .checked_add(1)
                .ok_or_else(|| fail(ErrorCode::InvalidEvent, "recovery.sequence"))?;
            events.push(RunEvent {
                schema_version: RunEventSchemaVersion::V1,
                event_id: bindings.ids.next_id()?,
                scope: bindings.scope.clone(),
                run_id: command.run_id.clone(),
                session_id: snapshot.request.session_id.clone(),
                seq: snapshot
                    .last_event_seq
                    .try_into()
                    .map_err(|_| fail(ErrorCode::InvalidEvent, "recovery.sequence"))?,
                timestamp_ms: now,
                payload: RunEventPayload::ToolUnresolved {
                    result_ref: record.reference().clone(),
                    attempt_id,
                    idempotency_key,
                },
            });
            messages.push(Message {
                message_id: bindings.ids.next_id()?,
                run_id: command.run_id.clone(),
                sequence: saved
                    .session
                    .transcript_revision
                    .checked_add(messages.len() as u64 + 1)
                    .and_then(NonZeroU64::new)
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "recovery.message_sequence"))?,
                role: MessageRole::Tool,
                content: vec![ContentBlock::ToolResult { result }],
                origin: MessageOrigin::Tool,
                visibility: Visibility::UserAndModel,
            });
            records.push(record);
        }
        let result = bindings
            .state
            .commit(
                &bindings.scope,
                &command.run_id,
                CommitInput {
                    expected_revision: command.expected_revision,
                    lease: lease.clone(),
                    now_ms: now,
                    snapshot,
                    messages,
                    events,
                    records,
                },
            )
            .await;
        if let Err(error) = result {
            let latest = bindings
                .state
                .load(&bindings.scope, &command.run_id)
                .await?;
            if !replayed(&latest.snapshot, command)?.is_some_and(|actual| actual == &receipt) {
                return Err(error);
            }
        }
        Ok((receipt, prompt, true))
    }
}
fn replayed<'a>(
    snapshot: &'a RunSnapshot,
    command: &ResumeCommand,
) -> Result<Option<&'a RecoveryReceipt>, ContractError> {
    if snapshot
        .resume_receipts
        .iter()
        .any(|receipt| receipt.command.command_id == command.command_id)
    {
        return Err(fail(ErrorCode::RequestConflict, "recovery.command"));
    }
    if let Some(receipt) = snapshot
        .recovery_receipts
        .iter()
        .find(|receipt| receipt.command.command_id == command.command_id)
    {
        if receipt.command != *command {
            return Err(fail(ErrorCode::RequestConflict, "recovery.command"));
        }
        return Ok(Some(receipt));
    }
    Ok(None)
}
fn validate_source(
    snapshot: &RunSnapshot,
    command: &ResumeCommand,
) -> Result<ProtectedRecord, ContractError> {
    if snapshot.status != RunStatus::Running {
        return Err(fail(ErrorCode::InvalidTransition, "recovery.status"));
    }
    if snapshot.revision != command.expected_revision {
        return Err(fail(ErrorCode::RevisionConflict, "recovery.revision"));
    }
    let ResumeAction::Recover { recovery_ref } = &command.action else {
        return Err(fail(ErrorCode::InvalidContract, "recovery.command"));
    };
    let source = snapshot.recovery_record(recovery_ref.record_id.clone())?;
    if source.reference() != recovery_ref {
        return Err(fail(ErrorCode::RequestConflict, "recovery.source"));
    }
    Ok(source)
}
