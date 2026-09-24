use super::*;

impl Agent {
    pub(super) async fn tool_round(
        &self,
        budget: &RunBudget,
        segment: &SegmentBindings,
    ) -> Result<SerialToolRound, ContractError> {
        let bindings = &self.inner.bindings;
        let registry = segment.tools.clone();
        let saved = bindings.state.load(budget.scope(), budget.run_id()).await?;
        let definitions = if let Some(reference) = &saved.snapshot.system_inputs {
            let record = bindings
                .state
                .read_record(budget.scope(), &reference.snapshot_ref)
                .await?;
            let inputs = RunSystemInputs::from_value(record.value(), reference, budget.scope())?;
            SystemInputRegistry::new(inputs.definitions().values().cloned().collect())?
        } else {
            bindings.system_inputs.clone()
        };
        let binder = Arc::new(InputBinder::new(
            Arc::new(definitions),
            bindings.system_input_resolver.clone(),
            bindings.policy.clone(),
            bindings.ids.clone(),
        ));
        let mut round = SerialToolRound::new(
            registry,
            binder,
            bindings.policy.clone(),
            bindings.ids.clone(),
        )
        .with_limits(bindings.settings.tool_execution_limits)?;
        if let Some(hooks) = &segment.hooks {
            round = round.with_hooks(hooks.clone());
        }
        if let Some(artifacts) = &bindings.artifacts {
            round = round.with_artifacts(artifacts.clone());
        }
        if saved.snapshot.skill_plan_ref.is_some() {
            let skills = bindings
                .skills
                .as_ref()
                .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "agent.skills"))?;
            round = round.with_skill_plan(skills.saved_plan(&saved.snapshot).await?);
        }
        if let Some(binding_set_id) = segment.binding_set_id() {
            round = round.with_binding_set_id(binding_set_id.clone());
        }
        Ok(round)
    }

    /// Commit the original complete model plan before any resolver or tool runs.
    pub(super) async fn plan_tools(
        &self,
        response: &ModelResponse,
        prompt: &PromptSnapshot,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        budget.check_boundary().await?;
        let bindings = &self.inner.bindings;
        let saved = bindings.state.load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        if response.finish != ModelFinish::ToolCalls
            || response.tool_calls.is_empty()
            || snapshot
                .tool_ledger
                .iter()
                .any(|entry| entry.call.model_request_id == response.request_id)
        {
            return Err(fail(ErrorCode::InvalidTransition, "agent.tool_plan"));
        }
        let invocation = snapshot
            .model_ledger
            .iter()
            .find(|invocation| {
                invocation.attempt_id == response.request_id
                    && matches!(invocation.state, ModelAttemptState::Completed {})
            })
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.tool_response"))?;
        if invocation.route.digest() != response.route_digest {
            return Err(fail(ErrorCode::ModelRoutingMismatch, "agent.tool_response"));
        }
        let provider = invocation.route.provider.clone();
        let route_digest = invocation.route.digest();
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let mut content = Vec::new();
        if !response.text.is_empty() {
            content.push(ContentBlock::Content {
                content: InputContent::Text {
                    text: response.text.clone(),
                },
            });
        }
        let mut records = vec![];
        let mut events = vec![];
        for proposed in &response.tool_calls {
            let descriptor_digest = prompt
                .tools()
                .iter()
                .find(|tool| tool.model_tool.name == proposed.name)
                .map(|tool| tool.descriptor_digest.clone());
            let call = ToolCall {
                call_id: bindings.ids.next_id()?,
                model_request_id: response.request_id.clone(),
                provider_call_id: proposed.provider_call_id.clone(),
                tool_name: proposed.name.clone(),
                model_inputs: proposed.model_inputs.clone(),
                descriptor_digest,
                bound_input_ref: None,
            };
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(&call)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.tool_plan"))?,
            );
            snapshot.last_event_seq = snapshot
                .last_event_seq
                .checked_add(1)
                .ok_or_else(|| fail(ErrorCode::InvalidEvent, "agent.tool_plan"))?;
            events.push(RunEvent {
                schema_version: RunEventSchemaVersion::V1,
                event_id: bindings.ids.next_id()?,
                scope: budget.scope().clone(),
                run_id: budget.run_id().clone(),
                session_id: snapshot.request.session_id.clone(),
                seq: snapshot
                    .last_event_seq
                    .try_into()
                    .map_err(|_| fail(ErrorCode::InvalidEvent, "agent.tool_plan"))?,
                timestamp_ms: now,
                payload: RunEventPayload::ToolPlanned {
                    call_ref: record.reference().clone(),
                },
            });
            records.push(record);
            content.push(ContentBlock::ToolCall { call: call.clone() });
            snapshot.tool_ledger.push(ToolLedgerEntry {
                call,
                state: ToolCallState::Planned {},
            });
        }
        for continuation in &response.continuation {
            if continuation.route_digest() != &route_digest {
                return Err(fail(
                    ErrorCode::ModelContextIncompatible,
                    "agent.continuation",
                ));
            }
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(continuation)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.continuation"))?,
            );
            content.push(ContentBlock::ProviderOpaque {
                provider: provider.clone(),
                route_digest: route_digest.clone(),
                data_ref: record.reference().clone(),
            });
            records.push(record);
        }
        let message = Message {
            message_id: bindings.ids.next_id()?,
            run_id: budget.run_id().clone(),
            sequence: saved
                .session
                .transcript_revision
                .checked_add(1)
                .and_then(NonZeroU64::new)
                .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.tool_plan"))?,
            role: MessageRole::Assistant,
            content,
            origin: MessageOrigin::Model,
            visibility: Visibility::UserAndModel,
        };
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.tool_plan"))?;
        snapshot.phase = RunPhase::Tool;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        bindings
            .state
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages: vec![message],
                    events,
                    records,
                },
            )
            .await?;
        Ok(())
    }

    pub(super) async fn tool_wait(
        &self,
        outcome: ToolRoundOutcome,
        budget: &RunBudget,
    ) -> Result<(WaitState, Vec<RecordRef>), ContractError> {
        let bindings = &self.inner.bindings;
        let (target, unresolved) = match outcome {
            ToolRoundOutcome::ApprovalRequired {
                call_id,
                binding_digest,
                ..
            } => (
                WaitTarget::Approval {
                    target: ApprovalTarget::Tool {
                        call_id,
                        binding_digest,
                    },
                },
                vec![],
            ),
            ToolRoundOutcome::Unresolved {
                call_id,
                result_ref,
            } => {
                let saved = bindings.state.load(budget.scope(), budget.run_id()).await?;
                let entry = saved
                    .snapshot
                    .tool_ledger
                    .iter()
                    .find(|entry| entry.call.call_id == call_id)
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.unresolved_tool"))?;
                let ToolCallState::Unknown {
                    idempotency_key, ..
                } = &entry.state
                else {
                    return Err(fail(ErrorCode::InvalidTransition, "agent.unresolved_tool"));
                };
                (
                    WaitTarget::External {
                        call_id,
                        effect_key: idempotency_key.clone(),
                    },
                    vec![result_ref],
                )
            }
            ToolRoundOutcome::InputRequired { request } => (WaitTarget::Input { request }, vec![]),
            ToolRoundOutcome::Completed => {
                return Err(fail(ErrorCode::InvalidTransition, "agent.tool_wait"));
            }
        };
        Ok((
            WaitState {
                wait_id: bindings.ids.next_id()?,
                target,
                expires_at_ms: Some(
                    bindings
                        .state
                        .load(budget.scope(), budget.run_id())
                        .await?
                        .snapshot
                        .timing
                        .deadline_at_ms,
                ),
            },
            unresolved,
        ))
    }

    pub(super) async fn settle_unstarted_tools(
        &self,
        snapshot: &RunSnapshot,
        segment: &SegmentBindings,
        budget: &RunBudget,
        cancelled: bool,
        local: &LocalRun,
    ) -> Result<(), ContractError> {
        let requests: std::collections::BTreeSet<_> = snapshot
            .tool_ledger
            .iter()
            .filter(|entry| {
                matches!(
                    entry.state,
                    ToolCallState::Planned {}
                        | ToolCallState::ApprovalPending { .. }
                        | ToolCallState::InputPending { .. }
                )
            })
            .map(|entry| entry.call.model_request_id.clone())
            .collect();
        if requests.is_empty() {
            return Ok(());
        }
        let round = self.tool_round(budget, segment).await?;
        for request in requests {
            round
                .settle_unstarted(
                    &request,
                    if cancelled {
                        ToolResultStatus::Cancelled
                    } else {
                        ToolResultStatus::Failed
                    },
                    Id::new(if cancelled {
                        "cancelled"
                    } else {
                        "run_stopped"
                    })?,
                    &segment.context,
                    budget,
                )
                .await?;
            self.remember_observer_error(local, round.observer_error());
        }
        Ok(())
    }
}
