use super::*;
use std::{collections::BTreeSet, panic::AssertUnwindSafe};

impl Agent {
    pub(super) async fn drive(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        context: ExecutionContext,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let bindings = &self.inner.bindings;
        let now = bindings.clock.now()?.utc_ms;
        let lease = bindings
            .state
            .acquire_lease(
                &bindings.scope,
                run_id,
                &bindings.ids.next_id()?,
                now,
                bindings.settings.lease_ttl_ms,
            )
            .await?;
        self.drive_leased(run_id, prompt, context, local, lease, false)
            .await
    }

    pub(super) async fn drive_leased(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        context: ExecutionContext,
        local: &Arc<LocalRun>,
        lease: RunLease,
        expired: bool,
    ) -> Result<(), ContractError> {
        let bindings = &self.inner.bindings;
        let budget = match RunBudget::attach(
            bindings.state.clone(),
            bindings.clock.clone(),
            bindings.ids.clone(),
            bindings.scope.clone(),
            run_id.clone(),
            lease.clone(),
            local.cancel.clone(),
        )
        .await
        {
            Ok(budget) => Arc::new(budget),
            Err(error) => {
                self.release_owned(run_id, &lease).await;
                return Err(error);
            }
        };
        let stop = CancellationToken::new();
        let heartbeat_agent = self.clone();
        let heartbeat_budget = budget.clone();
        let heartbeat_lease = lease.clone();
        let heartbeat_id = run_id.clone();
        let heartbeat_stop = stop.clone();
        let heartbeat_local = local.clone();
        let heartbeat = tokio::spawn(async move {
            let result = AssertUnwindSafe(heartbeat_agent.heartbeat(
                &heartbeat_id,
                heartbeat_lease,
                &heartbeat_budget,
                &heartbeat_stop,
            ))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(fail(ErrorCode::LeaseLost, "agent.heartbeat")));
            if let Err(error) = &result {
                if let Ok(mut slot) = heartbeat_local.error.lock() {
                    *slot = Some(error.clone());
                }
                heartbeat_local.cancel.cancel();
            }
            result
        });
        let mut segment = None;
        let result = AssertUnwindSafe(async {
            let saved = bindings.state.load(&bindings.scope, run_id).await?;
            let metadata = self.metadata_segment(&saved, context.clone()).await?;
            if expired {
                segment = Some(metadata);
                self.finish(
                    run_id,
                    PreparedOutcome {
                        result: OutcomeResult::Exhausted {
                            budget: BudgetKind::Elapsed,
                        },
                        output: vec![],
                        continuation: vec![],
                        unresolved_effects: vec![],
                        verification: None,
                    },
                    &budget,
                    segment.as_ref().expect("metadata segment"),
                    local,
                )
                .await
            } else {
                match self
                    .bind_segment(
                        &saved,
                        context.clone(),
                        Some(&lease),
                        ComponentBindPurpose::Execution,
                        Some(&budget),
                        local,
                    )
                    .await
                {
                    Ok(bound) => {
                        segment = Some(bound);
                        self.observe_pending(
                            run_id,
                            segment.as_ref().expect("bound segment"),
                            local,
                        )
                        .await;
                        crate::future::boxed(|| {
                            self.run_segment(
                                run_id,
                                prompt,
                                segment.as_ref().expect("bound segment"),
                                &budget,
                                &lease,
                                local,
                            )
                        })
                        .await
                    }
                    Err(error) => {
                        segment = Some(metadata);
                        if matches!(
                            error.code,
                            ErrorCode::LeaseLost
                                | ErrorCode::PersistenceUnavailable
                                | ErrorCode::RevisionConflict
                        ) {
                            return Err(error);
                        }
                        self.finish(
                            run_id,
                            PreparedOutcome {
                                result: match error.code {
                                    ErrorCode::Cancelled => OutcomeResult::Cancelled {
                                        reason: local
                                            .reason
                                            .lock()
                                            .map_err(|_| {
                                                fail(ErrorCode::InvalidContract, "agent.cancel")
                                            })?
                                            .as_ref()
                                            .map(ToString::to_string)
                                            .unwrap_or_else(|| "cancelled".into()),
                                    },
                                    ErrorCode::DeadlineExceeded | ErrorCode::BudgetExceeded => {
                                        OutcomeResult::Exhausted {
                                            budget: BudgetKind::Elapsed,
                                        }
                                    }
                                    _ => failed(&enum_name(&error.code)),
                                },
                                output: vec![],
                                continuation: vec![],
                                unresolved_effects: vec![],
                                verification: None,
                            },
                            &budget,
                            segment.as_ref().expect("metadata segment"),
                            local,
                        )
                        .await
                    }
                }
            }
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(fail(ErrorCode::InvalidContract, "agent.driver")));
        stop.cancel();
        let heartbeat_result = heartbeat
            .await
            .map_err(|_| fail(ErrorCode::LeaseLost, "agent.heartbeat"))?;
        let latest = bindings.state.load(&bindings.scope, run_id).await;
        if let Some(segment) = segment.as_ref() {
            if let Ok(saved) = &latest {
                if saved.snapshot.status.is_terminal() {
                    if bindings.components.is_some() && segment.owned.is_none() {
                        if expired {
                            self.cleanup_observers(saved, &context, local, vec![]).await;
                        } else if let Ok(mut slot) = local.release_error.lock() {
                            if slot.is_none() {
                                *slot = Some(fail(
                                    ErrorCode::ComponentUnavailable,
                                    "components.observers_not_bound",
                                ));
                            }
                        }
                    } else {
                        self.after_run(saved, segment, local).await;
                    }
                }
            }
            self.release_segment(segment, local).await;
        }
        if let Ok((_, now)) = budget.settlement_time(0) {
            let _ = bindings
                .state
                .release_lease(&bindings.scope, run_id, &lease, now)
                .await;
        }
        if latest.as_ref().is_ok_and(|saved| {
            saved.snapshot.status.is_terminal() || saved.snapshot.status == RunStatus::Waiting
        }) {
            return Ok(());
        }
        result.and(heartbeat_result)
    }

    async fn heartbeat(
        &self,
        run_id: &Id,
        mut lease: RunLease,
        budget: &RunBudget,
        stop: &CancellationToken,
    ) -> Result<(), ContractError> {
        let bindings = &self.inner.bindings;
        loop {
            let reading = bindings.clock.now()?;
            let next = reading
                .monotonic_ms
                .checked_add(bindings.settings.heartbeat_interval_ms)
                .ok_or_else(|| fail(ErrorCode::ClockUnavailable, "agent.heartbeat"))?;
            tokio::select! { biased;
                _ = stop.cancelled() => return Ok(()),
                result = bindings.clock.sleep_until(next) => result?,
            }
            let (_, now) = budget.settlement_time(0)?;
            let renewal = bindings.state.renew_lease(
                &bindings.scope,
                run_id,
                &lease,
                now,
                bindings.settings.lease_ttl_ms,
            );
            let remaining = lease
                .expires_at_ms
                .checked_sub(now)
                .and_then(|value| u64::try_from(value).ok())
                .filter(|value| *value > 0)
                .ok_or_else(|| fail(ErrorCode::LeaseLost, "agent.heartbeat"))?;
            let result = tokio::select! { biased;
                _ = stop.cancelled() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_millis(remaining)) => Err(fail(ErrorCode::LeaseLost, "agent.heartbeat")),
                result = renewal => result,
            };
            match result {
                Ok(current) => lease = current,
                Err(error) => return Err(error),
            }
        }
    }

    async fn run_segment(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        segment: &SegmentBindings,
        budget: &RunBudget,
        lease: &RunLease,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let context = &segment.context;
        let mut waiting = None;
        let mut saved = self
            .inner
            .bindings
            .state
            .load(budget.scope(), run_id)
            .await?;
        let recovering = saved
            .snapshot
            .recovery_receipts
            .last()
            .is_some_and(|receipt| receipt.accepted_revision == local.segment_start_revision);
        let mut recovery_error = None;
        if recovering {
            let uncertain: Vec<_> = saved
                .snapshot
                .tool_ledger
                .iter()
                .filter(|entry| {
                    matches!(
                        entry.state,
                        ToolCallState::Dispatching { .. } | ToolCallState::Unknown { .. }
                    )
                })
                .map(|entry| entry.call.call_id.clone())
                .collect();
            let round = self.tool_round(budget, segment).await?;
            for call_id in uncertain {
                match crate::future::boxed(|| round.reconcile_call(&call_id, context, budget)).await
                {
                    Ok(result) if result.effect == ToolEffect::Unknown => break,
                    Ok(_) => {}
                    Err(error) => {
                        recovery_error = Some(error);
                        break;
                    }
                }
            }
            saved = self
                .inner
                .bindings
                .state
                .load(budget.scope(), run_id)
                .await?;
        }
        let mut reuse_step = recovering
            && matches!(saved.snapshot.phase, RunPhase::Prepare | RunPhase::Model)
            && saved.snapshot.model_step_id.is_some();
        let mut pending_round = saved.snapshot.tool_ledger.iter().find(|entry| !matches!(&entry.state, ToolCallState::Settled { result } if result.status != ToolResultStatus::Unknown && result.effect != ToolEffect::Unknown)).map(|entry| entry.call.model_request_id.clone());
        let attempt = loop {
            if let Some(error) = recovery_error.take() {
                break Some(Err(error));
            }
            let current = self
                .inner
                .bindings
                .state
                .load(budget.scope(), run_id)
                .await?;
            if current.snapshot.candidate_ref.is_some() {
                match Box::pin(self.verify_candidate(segment, budget)).await {
                    Ok(super::verification::CandidateAction::Finish(candidate)) => {
                        return self
                            .finish(run_id, *candidate, budget, segment, local)
                            .await;
                    }
                    Ok(super::verification::CandidateAction::Repair) => continue,
                    Err(error) => break Some(Err(error)),
                }
            }
            if let Some(request_id) = pending_round.take() {
                let round = self.tool_round(budget, segment).await?;
                let result =
                    crate::future::boxed(|| round.execute(&request_id, context, budget)).await;
                self.remember_observer_error(local, round.observer_error());
                match result {
                    Ok(ToolRoundOutcome::Completed) => {}
                    Ok(outcome) => {
                        waiting = Some(self.tool_wait(outcome, budget).await?);
                        break None;
                    }
                    Err(error) => break Some(Err(error)),
                }
            }
            // Keep the nested model/verification path off the parent Tool loop stack.
            match Box::pin(self.generate(
                run_id,
                prompt.clone(),
                segment,
                budget,
                lease,
                std::mem::take(&mut reuse_step),
            ))
            .await
            {
                Ok(Guarded::Completed(ModelExchangeOutcome::Completed { response }))
                    if response.finish == ModelFinish::ToolCalls =>
                {
                    if let Err(error) = self.plan_tools(&response, &prompt, budget).await {
                        break Some(Err(error));
                    }
                    let round = self.tool_round(budget, segment).await?;
                    let result = crate::future::boxed(|| {
                        round.execute(&response.request_id, context, budget)
                    })
                    .await;
                    self.remember_observer_error(local, round.observer_error());
                    match result {
                        Ok(ToolRoundOutcome::Completed) => continue,
                        Ok(outcome) => {
                            waiting = Some(self.tool_wait(outcome, budget).await?);
                            break None;
                        }
                        Err(error) => break Some(Err(error)),
                    }
                }
                Ok(Guarded::Completed(ModelExchangeOutcome::Completed { response }))
                    if response.finish == ModelFinish::Stop
                        && response.tool_calls.is_empty()
                        && saved.snapshot.verification_plan_ref.is_some() =>
                {
                    if let Err(error) = Box::pin(self.candidate(&response, budget)).await {
                        break Some(Err(error));
                    }
                    continue;
                }
                result => break Some(result),
            }
        };
        if let Some(error) = local
            .error
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .clone()
        {
            return Err(error);
        }
        if let Some((wait, unresolved_effects)) = waiting {
            return self
                .finish(
                    run_id,
                    PreparedOutcome {
                        result: OutcomeResult::Waiting { wait },
                        output: vec![],
                        continuation: vec![],
                        unresolved_effects,
                        verification: None,
                    },
                    budget,
                    segment,
                    local,
                )
                .await;
        }
        let attempt = attempt.expect("non-waiting loop result");
        let mut continuation = vec![];
        let (result, output) = match attempt {
            Ok(Guarded::Completed(ModelExchangeOutcome::Completed { response }))
                if response.finish == ModelFinish::Stop && response.tool_calls.is_empty() =>
            {
                continuation = response.continuation;
                (
                    OutcomeResult::Succeeded {
                        completion_basis: CompletionBasis::TurnEnded,
                    },
                    vec![InputContent::Text {
                        text: response.text,
                    }],
                )
            }
            Ok(Guarded::Completed(ModelExchangeOutcome::Completed { response })) => (
                failed(if response.finish == ModelFinish::Refusal {
                    "model_refusal"
                } else {
                    "tool_execution_unsupported"
                }),
                vec![],
            ),
            Ok(Guarded::Completed(ModelExchangeOutcome::Failed { failure })) => (
                failed(&format!("model_{}", enum_name(&failure.kind))),
                if failure.partial_text().is_empty() {
                    vec![]
                } else {
                    vec![InputContent::Text {
                        text: failure.partial_text().to_owned(),
                    }]
                },
            ),
            Ok(Guarded::ApprovalRequired(_)) => (failed("approval_runtime_unsupported"), vec![]),
            Err(error)
                if matches!(
                    error.code,
                    ErrorCode::LeaseLost
                        | ErrorCode::RevisionConflict
                        | ErrorCode::PersistenceUnavailable
                        | ErrorCode::StateNotFound
                        | ErrorCode::ClockUnavailable
                        | ErrorCode::ClockRegression
                        | ErrorCode::InvalidTransition
                        | ErrorCode::InvalidSnapshot
                        | ErrorCode::InvalidEvent
                        | ErrorCode::RecordConflict
                ) =>
            {
                return Err(error);
            }
            Err(error) if error.code == ErrorCode::Cancelled => (
                OutcomeResult::Cancelled {
                    reason: local
                        .reason
                        .lock()
                        .map_err(|_| fail(ErrorCode::InvalidContract, "agent.cancel"))?
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "cancelled".into()),
                },
                vec![],
            ),
            Err(error) if error.code == ErrorCode::DeadlineExceeded => (
                OutcomeResult::Exhausted {
                    budget: BudgetKind::Elapsed,
                },
                vec![],
            ),
            Err(error) if error.code == ErrorCode::BudgetExceeded => {
                let kind = match error.path.as_str() {
                    "budget.model_calls" => BudgetKind::ModelCalls,
                    "budget.tool_attempts" => BudgetKind::ToolAttempts,
                    "budget.repair_attempts" => BudgetKind::RepairAttempts,
                    "budget.recovery_attempts" => BudgetKind::RecoveryAttempts,
                    _ => BudgetKind::Elapsed,
                };
                (OutcomeResult::Exhausted { budget: kind }, vec![])
            }
            Err(error) => (failed(&enum_name(&error.code)), vec![]),
        };
        self.finish(
            run_id,
            PreparedOutcome {
                result,
                output,
                continuation,
                unresolved_effects: vec![],
                verification: None,
            },
            budget,
            segment,
            local,
        )
        .await
    }

    async fn generate(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        segment: &SegmentBindings,
        budget: &RunBudget,
        lease: &RunLease,
        reuse_step: bool,
    ) -> Result<Guarded<ModelExchangeOutcome>, ContractError> {
        let context = &segment.context;
        let bindings = &self.inner.bindings;
        budget.check_boundary().await?;
        self.collect_sources(ContextTrigger::RunStart, None, segment, budget)
            .await?;
        let run_context = self.before_run(budget, segment).await?;
        let mut snapshot = bindings.state.load(&bindings.scope, run_id).await?.snapshot;
        let expected_revision = snapshot.revision;
        let step = match snapshot.model_step_id.as_ref().filter(|_| reuse_step) {
            Some(step) => step.clone(),
            None => bindings.ids.next_id()?,
        };
        if !reuse_step {
            let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
            snapshot.revision = snapshot
                .revision
                .checked_add(1)
                .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.prepare"))?;
            snapshot.phase = RunPhase::Prepare;
            snapshot.model_step_id = Some(step.clone());
            snapshot
                .source_states
                .retain(|state| state.trigger != ContextTrigger::BeforeModel);
            snapshot.usage.elapsed_ms = elapsed;
            snapshot.timing.last_observed_at_ms = now;
            bindings
                .state
                .commit(
                    &bindings.scope,
                    run_id,
                    CommitInput {
                        expected_revision,
                        lease: lease.clone(),
                        now_ms: now,
                        snapshot,
                        messages: vec![],
                        events: vec![],
                        records: vec![],
                    },
                )
                .await?;
        }
        let saved = bindings.state.load(&bindings.scope, run_id).await?;
        let router = bindings.router.snapshot();
        let rule = router
            .policy()
            .rules
            .iter()
            .find(|rule| {
                rule.model_binding == saved.snapshot.profile.profile().model_binding
                    && rule.purpose == ModelPurpose::Agent
            })
            .ok_or_else(|| fail(ErrorCode::ModelRouteDenied, "agent.routing"))?;
        self.collect_sources(
            ContextTrigger::BeforeModel,
            Some(step.clone()),
            segment,
            budget,
        )
        .await?;
        let (source_batch_refs, mut source_items) =
            self.source_context(&step, segment, budget).await?;
        if saved.snapshot.skill_plan_ref.is_some() {
            let skills = bindings
                .skills
                .as_ref()
                .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "agent.skills"))?;
            source_items.extend(
                skills
                    .context_items(&saved.snapshot, context, None, budget.call_deadline()?)
                    .await?,
            );
        }
        source_items.extend(run_context);
        let context_items = self
            .before_model(
                &step,
                saved.snapshot.request.input.clone(),
                source_items,
                segment,
                budget,
            )
            .await?;
        let verification_plan = self.verification_plan(&saved.snapshot).await?;
        let output = match verification_plan.schema {
            Some(schema) => ModelOutput::JsonSchema {
                schema: schema.schema,
            },
            None => ModelOutput::Text {},
        };
        let input = RoutedModelInput {
            model_step_id: step,
            routing: RouteRequest {
                model_binding: saved.snapshot.profile.profile().model_binding.clone(),
                purpose: ModelPurpose::Agent,
                required_capabilities: {
                    let mut required = std::collections::BTreeSet::from([Id::new("text")?]);
                    if !prompt.tools().is_empty() {
                        required.insert(Id::new("tool_calling")?);
                    }
                    if matches!(output, ModelOutput::JsonSchema { .. }) {
                        required.insert(Id::new("json_output")?);
                    }
                    required
                },
                input_tokens: 0,
                max_output_tokens: bindings.settings.max_output_tokens,
                options: saved.snapshot.request.model_options.clone(),
                scope: bindings.scope.clone(),
                allowed_bindings: std::iter::once(&rule.primary)
                    .chain(&rule.fallbacks)
                    .map(|binding| binding.id.clone())
                    .collect(),
                version_policy: rule.version_policy,
                previous_route: None,
                previous_failure: None,
            },
        };
        let projector = Projector {
            output,
            saved,
            prompt,
            settings: bindings.settings.clone(),
            bindings,
            budget,
            context_runtime: self.inner.context.clone(),
            context_items,
            sources: segment.sources.clone(),
            skills: bindings.skills.clone(),
            artifacts: bindings.artifacts.clone(),
            projected_artifacts: Mutex::new(vec![]),
            source_batch_refs,
        };
        bindings
            .model_exchange
            .generate_routed(
                bindings.router.as_ref(),
                &input,
                &projector,
                context,
                budget,
            )
            .await
    }

    async fn finish(
        &self,
        run_id: &Id,
        candidate: PreparedOutcome,
        budget: &RunBudget,
        segment: &SegmentBindings,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let PreparedOutcome {
            mut result,
            mut output,
            continuation,
            mut unresolved_effects,
            verification,
        } = candidate;
        let bindings = &self.inner.bindings;
        let lease = budget.lease();
        let mut saved = bindings.state.load(&bindings.scope, run_id).await?;
        if saved.snapshot.status.is_terminal() {
            return Ok(());
        }
        let unresolved: BTreeSet<_> = saved
            .snapshot
            .tool_ledger
            .iter()
            .filter_map(|entry| match &entry.state {
                ToolCallState::Unknown {
                    attempt_id,
                    idempotency_key,
                } => Some((attempt_id.clone(), idempotency_key.clone())),
                _ => None,
            })
            .collect();
        if !unresolved.is_empty() {
            let mut after = 0;
            loop {
                let page = bindings
                    .state
                    .read_events(&bindings.scope, run_id, after, MAX_EVENT_PAGE_SIZE)
                    .await?;
                for event in &page.events {
                    if let RunEventPayload::ToolUnresolved {
                        result_ref,
                        attempt_id,
                        idempotency_key,
                    } = &event.payload
                    {
                        if unresolved.contains(&(attempt_id.clone(), idempotency_key.clone()))
                            && !unresolved_effects.contains(result_ref)
                        {
                            unresolved_effects.push(result_ref.clone());
                        }
                    }
                }
                if !page.has_more {
                    break;
                }
                after = page.next_after_seq;
            }
        }
        if output.is_empty() && !matches!(result, OutcomeResult::Succeeded { .. }) {
            output = self.saved_partial_output(&saved.snapshot).await?;
        }
        // Finalization remains possible after cancellation/deadline, but only
        // under the stored lease. A stop during these reads also closes untouched
        // plans; it never invents a result for an uncertain dispatched operation.
        let mut cleaned = false;
        let (elapsed, now) = loop {
            let (_, check_at) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
            let current_lease = bindings
                .state
                .check_lease(&bindings.scope, run_id, lease, check_at)
                .await?;
            let (elapsed, now) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
            if now >= current_lease.expires_at_ms {
                return Err(fail(ErrorCode::LeaseLost, "agent.finish"));
            }
            if matches!(
                result,
                OutcomeResult::Succeeded { .. } | OutcomeResult::Waiting { .. }
            ) {
                if local.cancel.is_cancelled() {
                    result = OutcomeResult::Cancelled {
                        reason: local
                            .reason
                            .lock()
                            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.cancel"))?
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "cancelled".into()),
                    };
                } else if elapsed >= saved.snapshot.limits.max_elapsed_ms.get() {
                    result = OutcomeResult::Exhausted {
                        budget: BudgetKind::Elapsed,
                    };
                }
            }
            if !matches!(
                result,
                OutcomeResult::Succeeded { .. } | OutcomeResult::Waiting { .. }
            ) && saved.snapshot.tool_ledger.iter().any(|entry| {
                matches!(
                    entry.state,
                    ToolCallState::Planned {}
                        | ToolCallState::ApprovalPending { .. }
                        | ToolCallState::InputPending { .. }
                )
            }) {
                if cleaned {
                    return Err(fail(ErrorCode::InvalidTransition, "agent.pending_tools"));
                }
                self.settle_unstarted_tools(
                    &saved.snapshot,
                    segment,
                    budget,
                    matches!(result, OutcomeResult::Cancelled { .. }),
                    local,
                )
                .await?;
                saved = bindings.state.load(&bindings.scope, run_id).await?;
                cleaned = true;
                continue;
            }
            break (elapsed, now);
        };
        let mut snapshot = saved.snapshot;
        let expected_revision = snapshot.revision;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.finish"))?;
        snapshot.last_event_seq = snapshot
            .last_event_seq
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.event"))?;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        snapshot.status = result.status();
        snapshot.phase = if snapshot.status == RunStatus::Waiting {
            RunPhase::Waiting
        } else {
            RunPhase::Finish
        };
        snapshot.wait = if let OutcomeResult::Waiting { wait } = &result {
            Some(wait.clone())
        } else {
            None
        };
        if let OutcomeResult::Failed { failure } = &mut result {
            let verification_diagnostic =
                if let Some(reference) = snapshot.verification_records.last() {
                    let record: crate::verification::VerificationRecord =
                        self.read_verification(reference).await?;
                    (snapshot.candidate_ref.as_ref() == Some(&record.candidate_ref))
                        .then(|| reference.clone())
                } else {
                    None
                };
            failure.diagnostic_ref = verification_diagnostic.or_else(|| {
                snapshot
                    .model_ledger
                    .last()
                    .and_then(|entry| entry.response_ref.clone())
            });
        }
        let outcome = RunOutcome {
            result,
            output: output.clone(),
            artifacts: artifacts::produced(&snapshot),
            usage: snapshot.usage.clone(),
            checkpoint_revision: snapshot.revision,
            verification,
            unresolved_effects,
        };
        let record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(&outcome)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.outcome"))?,
        );
        let wait_record = snapshot
            .wait
            .as_ref()
            .map(|wait| {
                Ok::<_, ContractError>(ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(wait)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.wait"))?,
                ))
            })
            .transpose()?;
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: bindings.ids.next_id()?,
            scope: bindings.scope.clone(),
            run_id: run_id.clone(),
            session_id: snapshot.request.session_id.clone(),
            seq: snapshot
                .last_event_seq
                .try_into()
                .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.event"))?,
            timestamp_ms: now,
            payload: if let Some(wait_record) = &wait_record {
                RunEventPayload::RunWaiting {
                    wait_ref: wait_record.reference().clone(),
                }
            } else {
                RunEventPayload::RunFinished {
                    outcome_ref: record.reference().clone(),
                }
            },
        };
        let mut records = vec![record];
        records.extend(wait_record);
        let mut content: Vec<_> = output
            .into_iter()
            .map(|content| ContentBlock::Content { content })
            .collect();
        if snapshot.status == RunStatus::Succeeded {
            for continuation in continuation {
                let route = &snapshot
                    .model_ledger
                    .iter()
                    .rev()
                    .find(|entry| entry.purpose == ModelPurpose::Agent)
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.continuation"))?
                    .route;
                if continuation.route_digest() != &route.digest() {
                    return Err(fail(
                        ErrorCode::ModelContextIncompatible,
                        "agent.continuation",
                    ));
                }
                let record = ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(&continuation)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.continuation"))?,
                );
                content.push(ContentBlock::ProviderOpaque {
                    provider: route.provider.clone(),
                    route_digest: route.digest(),
                    data_ref: record.reference().clone(),
                });
                records.push(record);
            }
        }
        let messages = if content.is_empty() || snapshot.status != RunStatus::Succeeded {
            vec![]
        } else {
            vec![Message {
                message_id: bindings.ids.next_id()?,
                run_id: run_id.clone(),
                sequence: saved
                    .session
                    .transcript_revision
                    .checked_add(1)
                    .and_then(NonZeroU64::new)
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "message.sequence"))?,
                role: MessageRole::Assistant,
                content,
                origin: MessageOrigin::Model,
                visibility: Visibility::UserAndModel,
            }]
        };
        snapshot.outcome = Some(outcome);
        bindings
            .state
            .commit(
                &bindings.scope,
                run_id,
                CommitInput {
                    expected_revision,
                    lease: lease.clone(),
                    now_ms: now,
                    snapshot,
                    messages,
                    events: vec![event],
                    records,
                },
            )
            .await?;
        local.notify.notify_waiters();
        Ok(())
    }

    async fn saved_partial_output(
        &self,
        snapshot: &RunSnapshot,
    ) -> Result<Vec<InputContent>, ContractError> {
        let Some(step) = &snapshot.model_step_id else {
            return Ok(vec![]);
        };
        let Some(invocation) = snapshot.model_ledger.iter().rev().find(|invocation| {
            invocation.purpose == ModelPurpose::Agent
                && &invocation.model_step_id == step
                && invocation.run_id == snapshot.run_id
                && invocation.response_ref.is_some()
        }) else {
            return Ok(vec![]);
        };
        let reference = invocation
            .response_ref
            .as_ref()
            .expect("filtered response reference");
        let record = self
            .inner
            .bindings
            .state
            .read_record(&snapshot.scope, reference)
            .await?;
        if record.reference() != reference {
            return Err(fail(ErrorCode::InvalidSnapshot, "agent.partial_response"));
        }
        let response: StoredModelResponse = serde_json::from_value(record.value().clone())
            .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.partial_response"))?;
        if response.request_id != invocation.attempt_id
            || response.route_digest != invocation.route.digest()
        {
            return Err(fail(ErrorCode::InvalidSnapshot, "agent.partial_response"));
        }
        let text = match response.outcome {
            ModelExchangeOutcome::Completed { response } => response.text,
            ModelExchangeOutcome::Failed { failure } => failure.partial_text().to_owned(),
        };
        Ok(if text.is_empty() {
            vec![]
        } else {
            vec![InputContent::Text { text }]
        })
    }
}

pub(super) struct PreparedOutcome {
    pub result: OutcomeResult,
    pub output: Vec<InputContent>,
    pub continuation: Vec<OpaqueContinuation>,
    pub unresolved_effects: Vec<RecordRef>,
    pub verification: Option<VerificationSummary>,
}

struct Projector<'a> {
    output: ModelOutput,
    saved: StoredRun,
    prompt: PromptSnapshot,
    settings: AgentSettings,
    bindings: &'a AgentBindings,
    budget: &'a RunBudget,
    context_runtime: Arc<ContextRuntime>,
    context_items: Vec<ContextItem>,
    sources: Option<Arc<ContextSourceRuntime>>,
    source_batch_refs: Vec<RecordRef>,
    skills: Option<Arc<SkillRuntime>>,
    artifacts: Option<Arc<ArtifactRuntime>>,
    projected_artifacts: Mutex<Vec<ArtifactRef>>,
}
impl ModelRequestProjector for Projector<'_> {
    fn authorize_use<'a>(
        &'a self,
        selection: &'a RouteSelection,
        _input: &'a RoutedModelInput,
        context: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let deadline = context.deadline;
            let current = ExecutionContext::new(
                ExecutionContextData {
                    scope: context.scope.clone(),
                    principal_ref: context.principal_ref.clone(),
                    capability_grant_ref: context.capability_grant_ref.clone(),
                    trace_context: None,
                    system_inputs: None,
                },
                context.cancellation.clone(),
            );
            if let Some(sources) = &self.sources {
                sources
                    .authorize_use(
                        &self.saved.snapshot.run_id,
                        &self.source_batch_refs,
                        Some(&selection.route),
                        &current,
                        deadline,
                    )
                    .await?;
            }
            if self.saved.snapshot.skill_plan_ref.is_some() {
                let skills = self
                    .skills
                    .as_ref()
                    .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "agent.skills"))?;
                skills
                    .context_items(
                        &self.saved.snapshot,
                        &current,
                        Some(&selection.route),
                        deadline,
                    )
                    .await?;
            }
            let references = self
                .projected_artifacts
                .lock()
                .map_err(|_| fail(ErrorCode::InvalidContract, "agent.artifacts"))?
                .clone();
            if !references.is_empty() {
                let artifacts = self
                    .artifacts
                    .as_ref()
                    .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "agent.artifact_store"))?;
                for reference in &references {
                    artifacts.stat(reference, &current, Some(deadline)).await?;
                }
            }
            Ok(())
        })
    }
    fn project<'a>(
        &'a self,
        selection: &'a RouteSelection,
        input: &'a RoutedModelInput,
        context: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ProjectedModelRequest> {
        Box::pin(async move {
            if context.cancellation.is_cancelled() {
                return Err(fail(ErrorCode::Cancelled, "agent.projection"));
            }
            self.projected_artifacts
                .lock()
                .map_err(|_| fail(ErrorCode::InvalidContract, "agent.artifacts"))?
                .clear();
            self.authorize_use(selection, input, context).await?;
            let request_message = self
                .saved
                .messages
                .iter()
                .find(|message| {
                    message.run_id == self.saved.snapshot.run_id
                        && message.role == MessageRole::User
                })
                .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.request_message"))?;
            let seed = ProjectionInput {
                profile: &self.saved.snapshot.profile,
                scope: &context.scope,
                run_id: &self.saved.snapshot.run_id,
                model_step_id: &input.model_step_id,
                current_request: &self.saved.snapshot.request,
                current_request_message_id: &request_message.message_id,
                transcript: &self.saved.messages,
                context_items: &self.context_items,
                opaque_records: &[],
                expected_prompt_digest: &self.saved.session.prompt_snapshot.digest,
                request_id: input.model_step_id.clone(),
                purpose: input.routing.purpose,
                route: selection.route.clone(),
                output: self.output.clone(),
                max_output_tokens: self.settings.max_output_tokens,
                options: input.routing.options.clone(),
                response_limits: self.settings.response_limits.clone(),
                limits: self.settings.projection_limits,
            };
            let current = ExecutionContext::new(
                ExecutionContextData {
                    scope: context.scope.clone(),
                    principal_ref: context.principal_ref.clone(),
                    capability_grant_ref: context.capability_grant_ref.clone(),
                    trace_context: None,
                    system_inputs: None,
                },
                context.cancellation.clone(),
            );
            let prepared = Box::pin(self.context_runtime.prepare(
                &self.prompt,
                seed,
                crate::context_strategy::ContextServices {
                    bindings: self.bindings,
                    budget: self.budget,
                    context: &current,
                },
            ))
            .await?;
            let mut references = artifacts::selected(
                &prepared.projection.request,
                &self.saved,
                &prepared.artifacts,
            )?;
            for reference in prepared.artifacts {
                if !references.contains(&reference) {
                    references.push(reference);
                }
            }
            *self
                .projected_artifacts
                .lock()
                .map_err(|_| fail(ErrorCode::InvalidContract, "agent.artifacts"))? = references;
            Ok(ProjectedModelRequest {
                request: prepared.projection.request,
                input_tokens: prepared.input_tokens,
            })
        })
    }
}
pub(super) fn enum_name(value: &impl serde::Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "invalid_contract".into())
}
fn failed(code: &str) -> OutcomeResult {
    OutcomeResult::Failed {
        failure: Failure {
            code: Id::new(code).expect("nonempty static classification"),
            diagnostic_ref: None,
        },
    }
}
