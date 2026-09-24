use super::*;

impl Agent {
    /// Charge at most once per rejected Tool round, including recovery after reservation.
    pub(super) async fn reserve_tool_repair(
        &self,
        request_id: &Id,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let saved = self
            .inner
            .bindings
            .state
            .load(budget.scope(), budget.run_id())
            .await?;
        let invalid = saved.snapshot.tool_ledger.iter().any(|entry| {
            &entry.call.model_request_id == request_id && crate::budget::needs_tool_repair(entry)
        });
        let reserved = saved.snapshot.reservations.iter().any(|reservation| matches!(&reservation.kind, ReservationKind::ToolRepair { model_request_id } if model_request_id == request_id));
        if invalid && !reserved {
            budget
                .reserve(ReservationKind::ToolRepair {
                    model_request_id: request_id.clone(),
                })
                .await?;
        }
        Ok(())
    }

    pub(super) async fn tool_round(
        &self,
        budget: &RunBudget,
        segment: &SegmentBindings,
    ) -> Result<SerialToolRound, ContractError> {
        let saved = self
            .inner
            .bindings
            .state
            .load(budget.scope(), budget.run_id())
            .await?;
        self.tool_round_for_snapshot(&saved, segment).await
    }

    pub(super) async fn tool_round_for_snapshot(
        &self,
        saved: &StoredRun,
        segment: &SegmentBindings,
    ) -> Result<SerialToolRound, ContractError> {
        let bindings = &self.inner.bindings;
        let registry = segment.tools.clone();
        let definitions = if let Some(reference) = &saved.snapshot.system_inputs {
            let record = bindings
                .state
                .read_record(&saved.snapshot.scope, &reference.snapshot_ref)
                .await?;
            let inputs =
                RunSystemInputs::from_value(record.value(), reference, &saved.snapshot.scope)?;
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
        let mut prepared_tools = Vec::new();
        if let Some(reference) = &invocation.prepared_step_ref {
            let root_record = bindings
                .state
                .read_record(budget.scope(), reference)
                .await?;
            let root: PreparedStepRecord = serde_json::from_value(root_record.value().clone())
                .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.prepared_step"))?;
            let tool_record = bindings
                .state
                .read_record(budget.scope(), &root.tool_set)
                .await?;
            let tool_set: ResolvedToolSet = serde_json::from_value(tool_record.value().clone())
                .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.prepared_tool_set"))?;
            tool_set.validate_shape()?;
            if tool_set.entries.len() != root.compiled_tools.len() {
                return Err(fail(ErrorCode::InvalidSnapshot, "agent.prepared_tool_set"));
            }
            let target = ProviderToolTarget::for_route(&invocation.route);
            for (entry, reference) in tool_set.entries.into_iter().zip(root.compiled_tools) {
                if !prompt.tools().contains(&entry.manifest) {
                    return Err(fail(ErrorCode::InvalidSnapshot, "agent.prepared_manifest"));
                }
                let tool = entry.restore_tool()?;
                let record = bindings
                    .state
                    .read_record(budget.scope(), &reference)
                    .await?;
                let digest: JsonDigest =
                    serde_json::from_value(record.value()["digest"].clone())
                        .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.prepared_contract"))?;
                let contract = CompiledToolContract::restore(
                    &record.value().to_string(),
                    &tool,
                    &target,
                    &digest,
                    ProviderToolSchemaLimits::default(),
                )?;
                prepared_tools.push((entry.manifest, contract, reference));
            }
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
            let prepared = prepared_tools
                .iter()
                .find(|(_, contract, _)| contract.wire_tool().name == proposed.name);
            let (name, model_inputs, descriptor_digest, contract_ref) =
                if let Some((manifest, contract, reference)) = prepared {
                    let raw = proposed.raw_arguments.clone().unwrap_or_else(|| {
                        serde_json::to_string(&proposed.model_inputs).expect("model arguments")
                    });
                    let decoded = match contract
                        .decode_arguments(&raw, ProviderToolSchemaLimits::default())
                    {
                        Ok(value) => value,
                        Err(error) if error.code == ErrorCode::InvalidArguments => {
                            JsonObject::new()
                        }
                        Err(error) => return Err(error),
                    };
                    (
                        contract.canonical_name().clone(),
                        decoded,
                        Some(manifest.descriptor_digest.clone()),
                        Some(reference.clone()),
                    )
                } else {
                    let digest = if invocation.prepared_step_ref.is_none() {
                        prompt
                            .tools()
                            .iter()
                            .find(|tool| tool.model_tool.name == proposed.name)
                            .map(|tool| tool.descriptor_digest.clone())
                    } else {
                        None
                    };
                    (
                        proposed.name.clone(),
                        proposed.model_inputs.clone(),
                        digest,
                        None,
                    )
                };
            let call = ToolCall {
                provider_arguments: proposed.raw_arguments.as_ref().map(|raw| {
                    ProviderToolArguments {
                        name: proposed.name.clone(),
                        raw: raw.clone(),
                        compiled_contract_ref: contract_ref,
                    }
                }),
                call_id: bindings.ids.next_id()?,
                model_request_id: response.request_id.clone(),
                provider_call_id: proposed.provider_call_id.clone(),
                tool_name: name,
                model_inputs,
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
            source_model_request_id: Some(response.request_id.clone()),
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
                    control_commands: vec![],
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
