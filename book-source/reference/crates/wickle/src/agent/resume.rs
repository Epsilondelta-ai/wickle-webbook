use super::*;
use std::panic::AssertUnwindSafe;

impl Agent {
    pub(super) async fn resume_command(
        &self,
        command: ResumeCommand,
        context: ExecutionContext,
    ) -> Result<Guarded<RunHandle>, ContractError> {
        self.check_scope(&context)?;
        if serde_json::to_vec(&command)
            .map_err(|_| fail(ErrorCode::InvalidJson, "agent.command"))?
            .len()
            > self.inner.bindings.settings.max_request_bytes
        {
            return Err(fail(ErrorCode::InvalidContract, "agent.command_size"));
        }
        if matches!(command.action, ResumeAction::Recover { .. }) {
            return self.recover_command(command, context).await;
        }
        let saved = self
            .resume_read(
                &context,
                self.inner
                    .bindings
                    .state
                    .load(&self.inner.bindings.scope, &command.run_id),
            )
            .await?;
        if let Guarded::ApprovalRequired(challenge) = self
            .authorize_resume(
                &command,
                &context,
                saved_binding_digest(&saved.snapshot, &command),
            )
            .await?
        {
            return Ok(Guarded::ApprovalRequired(challenge));
        }
        self.resume_inputs(&saved.snapshot, &context).await?;
        if let Some(receipt) = accepted(&saved.snapshot, &command)? {
            return Ok(Guarded::Completed(
                self.handle(command.run_id, receipt.accepted_revision)
                    .await?,
            ));
        }
        validate_wait(&saved.snapshot, &command)?;
        let execution_context = self.execution_context(&command.run_id, &context).await?;
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(self.inner.bindings.settings.start_timeout_ms);
        loop {
            match self.resume_owned(&command, &context).await {
                Ok((receipt, prompt, Some(lease), observations, segment_id)) => {
                    return Ok(Guarded::Completed(self.launch_segment(
                        command.run_id,
                        (segment_id, receipt.accepted_revision, receipt.expired),
                        prompt,
                        execution_context,
                        lease,
                        observations,
                    )?));
                }
                Ok((receipt, _, None, _, _)) => {
                    return Ok(Guarded::Completed(
                        self.handle(command.run_id, receipt.accepted_revision)
                            .await?,
                    ));
                }
                Err(error) => {
                    if !matches!(
                        error.code,
                        ErrorCode::LeaseBusy
                            | ErrorCode::RevisionConflict
                            | ErrorCode::InvalidTransition
                    ) {
                        return Err(error);
                    }
                    let latest = self
                        .resume_read(
                            &context,
                            self.inner
                                .bindings
                                .state
                                .load(&self.inner.bindings.scope, &command.run_id),
                        )
                        .await?;
                    if let Some(receipt) = accepted(&latest.snapshot, &command)? {
                        // A replay has no lease and must not launch another driver.
                        return Ok(Guarded::Completed(
                            self.handle(command.run_id, receipt.accepted_revision)
                                .await?,
                        ));
                    }
                    if error.code != ErrorCode::LeaseBusy {
                        return Err(error);
                    }
                    tokio::select! { biased;
                        _ = context.cancellation.cancelled() => return Err(fail(ErrorCode::Cancelled, "agent.resume")),
                        _ = tokio::time::sleep_until(deadline) => return Err(error),
                        _ = tokio::time::sleep(Duration::from_millis(self.inner.bindings.settings.observer_poll_ms.min(20))) => {},
                    }
                }
            }
        }
    }

    async fn resume_owned(
        &self,
        command: &ResumeCommand,
        context: &ExecutionContext,
    ) -> Result<
        (
            ResumeReceipt,
            PromptSnapshot,
            Option<RunLease>,
            Vec<(HookTarget, HookInput)>,
            Id,
        ),
        ContractError,
    > {
        let bindings = &self.inner.bindings;
        let saved = self
            .resume_read(
                context,
                bindings.state.load(&bindings.scope, &command.run_id),
            )
            .await?;
        let prompt = self.restore_resume_runtime(&saved, context).await?;
        if let Some(receipt) = accepted(&saved.snapshot, command)? {
            let handle = self
                .handle(command.run_id.clone(), receipt.accepted_revision)
                .await?;
            return Ok((receipt.clone(), prompt, None, vec![], handle.segment_id));
        }
        validate_wait(&saved.snapshot, command)?;
        self.resume_inputs(&saved.snapshot, context).await?;
        let wait = saved
            .snapshot
            .wait
            .as_ref()
            .expect("validated waiting snapshot");
        let mut clock =
            super::segment::PreparationClock::new(bindings.clock.clone(), &saved.snapshot)?;
        let (_, now) = clock.now()?;
        let expires_at_ms = saved
            .snapshot
            .timing
            .deadline_at_ms
            .min(wait.expires_at_ms.unwrap_or(i64::MAX));
        let expired = now >= expires_at_ms;
        let mut fixed_binding_digest = saved_binding_digest(&saved.snapshot, command);
        let candidate_review = matches!(
            wait.target,
            WaitTarget::Approval {
                target: ApprovalTarget::Candidate { .. }
            }
        );
        let prepared = if expired || candidate_review {
            None
        } else {
            let (call, bound) = self.resume_bound(&saved, context).await?;
            fixed_binding_digest = Some(bound.binding_digest().clone());
            if let Guarded::ApprovalRequired(_) = self
                .authorize_resume(command, context, fixed_binding_digest.clone())
                .await?
            {
                return Err(fail(ErrorCode::AccessDenied, "agent.resume_approval"));
            }
            let segment = self.metadata_segment(&saved, context.clone()).await?;
            let round = self.tool_round_for_snapshot(&saved, &segment).await?;
            match &command.action {
                ResumeAction::Approve { .. } => None,
                ResumeAction::Deny { .. } => Some(round.prepare_denial(
                    &saved,
                    &call.call_id,
                    &bound,
                    Id::new("approval_denied")?,
                    now,
                )?),
                ResumeAction::Input { answer, .. } => {
                    let WaitTarget::Input { request } = &wait.target else {
                        return Err(fail(ErrorCode::InvalidReference, "agent.input_wait"));
                    };
                    Some(round.prepare_input(&saved, request, &bound, answer.clone(), now)?)
                }
                ResumeAction::External { receipt_ref, .. } => {
                    let verifier =
                        bindings.external_receipt_verifier.as_ref().ok_or_else(|| {
                            fail(ErrorCode::CapabilityUnsupported, "agent.external_verifier")
                        })?;
                    let entry = saved
                        .snapshot
                        .tool_ledger
                        .iter()
                        .find(|entry| entry.call.call_id == call.call_id)
                        .expect("resolved call");
                    let ToolCallState::Unknown {
                        attempt_id,
                        idempotency_key,
                    } = &entry.state
                    else {
                        return Err(fail(ErrorCode::InvalidTransition, "agent.external_wait"));
                    };
                    self.authorize_receipt(receipt_ref, context).await?;
                    let record = self
                        .resume_read(
                            context,
                            bindings.state.read_record(&bindings.scope, receipt_ref),
                        )
                        .await?;
                    if record.reference() != receipt_ref {
                        return Err(fail(ErrorCode::InvalidSnapshot, "agent.receipt_reference"));
                    }
                    if serde_json::to_vec(record.value())
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.receipt"))?
                        .len()
                        > bindings.settings.max_request_bytes
                    {
                        return Err(fail(ErrorCode::InvalidArguments, "agent.receipt_size"));
                    }
                    let request = ExternalReceiptRequest {
                        call,
                        attempt_id: attempt_id.clone(),
                        idempotency_key: idempotency_key.clone(),
                        bound_input: bound.clone(),
                        receipt_ref: receipt_ref.clone(),
                        receipt: record.value().clone(),
                    };
                    self.authorize_receipt(receipt_ref, context).await?;
                    let (_, now) = clock.now()?;
                    let remaining = saved
                        .snapshot
                        .timing
                        .deadline_at_ms
                        .min(wait.expires_at_ms.unwrap_or(i64::MAX))
                        .saturating_sub(now)
                        .max(0) as u64;
                    let timeout = Duration::from_millis(
                        bindings
                            .settings
                            .start_timeout_ms
                            .min(bindings.settings.lease_ttl_ms / 2)
                            .min(remaining),
                    );
                    let cancellation = CancellationToken::new();
                    let _cancel = cancellation.clone().drop_guard();
                    let verification = ExternalReceiptContext {
                        scope: bindings.scope.clone(),
                        principal_ref: context.data.principal_ref.clone(),
                        capability_grant_ref: context.data.capability_grant_ref.clone(),
                        cancellation,
                        deadline: tokio::time::Instant::now() + timeout,
                    };
                    let mut verified = caller_read(context, Some(timeout), async {
                        AssertUnwindSafe(verifier.verify(&request, &verification))
                            .catch_unwind()
                            .await
                            .map_err(|_| {
                                fail(ErrorCode::InvalidContract, "agent.receipt_verifier")
                            })?
                            .map_err(|error| fail(error.code, "agent.receipt_verifier"))
                    })
                    .await?;
                    verification.cancellation.cancel();
                    self.authorize_receipt(receipt_ref, context).await?;
                    if let ToolExecutionOutcome::SucceededWithContent { content, .. } =
                        &verified.outcome
                    {
                        let checked = match &bindings.artifacts {
                            Some(artifacts) => caller_read(
                                context,
                                Some(timeout),
                                artifacts.validate_content(
                                    content,
                                    context,
                                    Some(verification.deadline),
                                ),
                            )
                            .await
                            .map(|_| ()),
                            None if content.iter().any(|item| {
                                matches!(
                                    item,
                                    InputContent::Artifact { .. } | InputContent::Evidence { .. }
                                )
                            }) =>
                            {
                                Err(fail(
                                    ErrorCode::ComponentUnavailable,
                                    "agent.artifact_store",
                                ))
                            }
                            None => Ok(()),
                        };
                        if let Err(error) = checked {
                            verified.outcome = ToolExecutionOutcome::Failed {
                                code: Id::new(super::driver::enum_name(&error.code))?,
                            };
                        }
                    }
                    Some(round.prepare_external(
                        &saved,
                        &request.call.call_id,
                        &bound,
                        verified,
                        now,
                    )?)
                }
                ResumeAction::Recover { .. } => {
                    return Err(fail(ErrorCode::CapabilityUnsupported, "agent.recovery"));
                }
            }
        };
        if let Guarded::ApprovalRequired(_) = self
            .authorize_resume(command, context, fixed_binding_digest)
            .await?
        {
            return Err(fail(ErrorCode::AccessDenied, "agent.resume_approval"));
        }
        if context.cancellation.is_cancelled() {
            return Err(fail(ErrorCode::Cancelled, "agent.resume"));
        }
        let old_outcome = saved
            .snapshot
            .outcome
            .as_ref()
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.wait_outcome"))?;
        let outcome_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(old_outcome)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.wait_outcome"))?,
        );
        let command_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(command)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.command"))?,
        );
        let mut receipt = ResumeReceipt {
            command: command.clone(),
            command_ref: command_record.reference().clone(),
            accepted_revision: saved
                .snapshot
                .revision
                .checked_add(1)
                .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.resume"))?,
            previous_segment_start_revision: segment_revision(&saved.snapshot),
            previous_outcome_ref: outcome_record.reference().clone(),
            previous_last_event_seq: saved.snapshot.last_event_seq,
            actor_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
            expired,
        };
        let mut records = vec![outcome_record, command_record];
        let mut messages = vec![];
        let mut events = vec![];
        let mut snapshot = saved.snapshot;
        let observation = prepared.as_ref().and_then(|prepared| {
            if let RunEventPayload::ToolSettled { result_ref } = &prepared.event.payload {
                Some((
                    HookTarget::AfterTool {
                        call_id: prepared.result.call_id.clone(),
                        result_ref: result_ref.clone(),
                    },
                    HookInput::tool_observed(&prepared.result.call_id, &prepared.result),
                ))
            } else {
                None
            }
        });
        if let Some(prepared) = prepared {
            apply_resolution(
                &mut snapshot,
                &mut messages,
                &mut events,
                &mut records,
                prepared,
            )?;
        }
        let (elapsed, now) = clock.now()?;
        receipt.expired = now >= expires_at_ms;
        snapshot.revision = receipt.accepted_revision;
        snapshot.status = RunStatus::Running;
        snapshot.phase = if candidate_review {
            RunPhase::Verify
        } else {
            RunPhase::Tool
        };
        snapshot.wait = None;
        snapshot.outcome = None;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        snapshot.last_event_seq = snapshot
            .last_event_seq
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::InvalidEvent, "agent.resume"))?;
        snapshot.resume_receipts.push(receipt.clone());
        events.push(RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: bindings.ids.next_id()?,
            scope: bindings.scope.clone(),
            run_id: command.run_id.clone(),
            session_id: snapshot.request.session_id.clone(),
            seq: snapshot
                .last_event_seq
                .try_into()
                .map_err(|_| fail(ErrorCode::InvalidEvent, "agent.resume"))?,
            timestamp_ms: now,
            payload: RunEventPayload::RunResumed {
                command_ref: receipt.command_ref.clone(),
            },
        });
        let started =
            bindings
                .state
                .begin_segment(
                    &bindings.scope,
                    BeginSegmentRequest {
                        run_id: command.run_id.clone(),
                        expected_revision: command.expected_revision,
                        segment_id: bindings.ids.next_id()?,
                        owner: bindings.ids.next_id()?,
                        now_ms: now,
                        lease_ttl_ms: bindings.settings.lease_ttl_ms.try_into().map_err(|_| {
                            fail(ErrorCode::InvalidConfiguration, "agent.lease_ttl")
                        })?,
                        start: SegmentStart::Resume(command.clone()),
                        transition: Some(SegmentTransition {
                            snapshot,
                            messages,
                            events,
                            records,
                        }),
                    },
                )
                .await?;
        let receipt = accepted(&started.state.snapshot, command)?
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.resume_accepted"))?
            .clone();
        Ok((
            receipt,
            prompt,
            started.lease,
            observation.into_iter().collect(),
            started.segment.segment_id,
        ))
    }

    pub(super) async fn authorize_resume(
        &self,
        command: &ResumeCommand,
        context: &ExecutionContext,
        binding_digest: Option<JsonDigest>,
    ) -> Result<Guarded<()>, ContractError> {
        let request = PolicyRequest {
            owner_scope: self.inner.bindings.scope.clone(),
            resource_id: command.run_id.clone(),
            action: PolicyAction::ResumeRun {
                command: Box::new(command.clone()),
                binding_digest,
            },
        };
        self.inner
            .bindings
            .policy
            .guard(&request, context, None, None, || async { Ok(()) })
            .await
    }
    async fn authorize_receipt(
        &self,
        reference: &RecordRef,
        context: &ExecutionContext,
    ) -> Result<(), ContractError> {
        let request = PolicyRequest {
            owner_scope: self.inner.bindings.scope.clone(),
            resource_id: reference.record_id.clone(),
            action: PolicyAction::ReadRecord {},
        };
        match self
            .inner
            .bindings
            .policy
            .guard(&request, context, None, None, || async { Ok(()) })
            .await?
        {
            Guarded::Completed(()) => Ok(()),
            Guarded::ApprovalRequired(_) => {
                Err(fail(ErrorCode::AccessDenied, "agent.receipt_read"))
            }
        }
    }
    pub(super) async fn resume_inputs(
        &self,
        snapshot: &RunSnapshot,
        context: &ExecutionContext,
    ) -> Result<(), ContractError> {
        if self.inner.profile.digest() != *snapshot.profile.profile_digest() {
            return Err(fail(ErrorCode::ProfileMismatch, "agent.resume_profile"));
        }
        if let Some(reference) = &snapshot.system_inputs {
            let record = self
                .resume_read(
                    context,
                    self.inner
                        .bindings
                        .state
                        .read_record(&snapshot.scope, &reference.snapshot_ref),
                )
                .await?;
            RunSystemInputs::from_value(record.value(), reference, &snapshot.scope)?
                .validate_resume(context.data.system_inputs.as_ref())?;
        } else if context
            .data
            .system_inputs
            .as_ref()
            .is_some_and(|inputs| !inputs.values().is_empty())
        {
            return Err(fail(ErrorCode::SystemInputsMismatch, "agent.system_inputs"));
        }
        Ok(())
    }
    pub(super) async fn restore_resume_runtime(
        &self,
        saved: &StoredRun,
        context: &ExecutionContext,
    ) -> Result<PromptSnapshot, ContractError> {
        let bindings = &self.inner.bindings;
        if saved.snapshot.interruption_plan_ref.is_some() {
            self.validate_interruption_binding(&saved.snapshot).await?;
        }
        let record = self
            .resume_read(
                context,
                bindings
                    .state
                    .read_record(&bindings.scope, &saved.session.prompt_snapshot),
            )
            .await?;
        let prompt = PromptSnapshot::restore(
            &serde_json::to_string(record.value())
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.prompt"))?,
            &saved.session.prompt_snapshot.digest,
            &saved.snapshot.profile,
            &bindings.scope,
        )?;
        let metadata = self.metadata_segment(saved, context.clone()).await?;
        let tools = metadata
            .tools
            .prompt_bindings(saved.snapshot.profile.profile())?;
        if tools.len() != prompt.tools().len()
            || tools.iter().zip(prompt.tools()).any(|(tool, pinned)| {
                tool.selection != pinned.selection
                    || tool.compiled.digest() != &pinned.compiled_digest
            })
        {
            return Err(fail(ErrorCode::ContextMismatch, "agent.pinned_tools"));
        }
        let expected = saved
            .snapshot
            .routing_snapshot_ref
            .as_ref()
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.routing"))?;
        let current =
            std::panic::catch_unwind(AssertUnwindSafe(|| bindings.router.snapshot().digest()))
                .map_err(|_| fail(ErrorCode::ModelRoutingMismatch, "agent.router"))?;
        if current != expected.digest {
            return Err(fail(
                ErrorCode::ModelRoutingMismatch,
                "agent.pinned_routing",
            ));
        }
        if let Some(assembly) = self.saved_assembly(saved).await? {
            let plan = HookRegistry::metadata(bindings.scope.clone(), assembly.hooks().to_vec())?
                .plan(saved.snapshot.profile.profile())?;
            if saved
                .snapshot
                .hook_plan_ref
                .as_ref()
                .map(|reference| &reference.digest)
                != Some(&plan.digest())
            {
                return Err(fail(ErrorCode::ContextMismatch, "agent.pinned_hooks"));
            }
        } else {
            match (&saved.snapshot.hook_plan_ref, &bindings.hooks) {
                (Some(reference), Some(hooks))
                    if hooks.plan(saved.snapshot.profile.profile())?.digest()
                        == reference.digest => {}
                (None, None) => {}
                (None, Some(hooks))
                    if hooks
                        .plan(saved.snapshot.profile.profile())?
                        .definitions()
                        .is_empty() => {}
                _ => return Err(fail(ErrorCode::ContextMismatch, "agent.pinned_hooks")),
            }
        }
        let expected_sources = if let Some(assembly) = self.saved_assembly(saved).await? {
            if assembly.sources().is_empty() {
                None
            } else {
                let estimator = bindings.context_token_estimator.as_ref().ok_or_else(|| {
                    fail(ErrorCode::InvalidConfiguration, "agent.source_estimator")
                })?;
                Some(
                    ContextSourceRegistry::metadata(
                        bindings.scope.clone(),
                        assembly.sources().to_vec(),
                    )?
                    .plan(saved.snapshot.profile.profile(), &estimator.version())?
                    .digest(),
                )
            }
        } else {
            bindings
                .context_sources
                .as_ref()
                .map(|sources| {
                    sources
                        .plan(saved.snapshot.profile.profile())
                        .map(|plan| plan.digest())
                })
                .transpose()?
        };
        if saved
            .snapshot
            .source_plan_ref
            .as_ref()
            .map(|reference| &reference.digest)
            != expected_sources.as_ref()
        {
            return Err(fail(ErrorCode::ContextMismatch, "agent.pinned_sources"));
        }
        match (&saved.snapshot.skill_plan_ref, &bindings.skills) {
            (Some(_), Some(skills)) => {
                let plan = skills.saved_plan(&saved.snapshot).await?;
                if plan.listings() != prompt.skills() {
                    return Err(fail(ErrorCode::ContextMismatch, "agent.pinned_skills"));
                }
            }
            (None, _) if saved.snapshot.profile.profile().skills.is_empty() => {}
            _ => return Err(fail(ErrorCode::ContextMismatch, "agent.pinned_skills")),
        }
        self.verification_plan(&saved.snapshot).await?;
        if let Some(reference) = &saved.snapshot.context_plan_ref {
            let record = bindings
                .state
                .read_record(&bindings.scope, reference)
                .await?;
            let plan = ContextPlan::restore(&record, &saved.snapshot.profile)?;
            if plan.digest()
                != self
                    .inner
                    .context
                    .plan(saved.snapshot.profile.profile(), &bindings.scope)?
                    .digest()
            {
                return Err(fail(ErrorCode::ContextMismatch, "agent.pinned_context"));
            }
        }
        Ok(prompt)
    }
    async fn resume_bound(
        &self,
        saved: &StoredRun,
        context: &ExecutionContext,
    ) -> Result<(ToolCall, BoundToolInput), ContractError> {
        let wait = saved
            .snapshot
            .wait
            .as_ref()
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.wait"))?;
        let call_id = match &wait.target {
            WaitTarget::Approval {
                target: ApprovalTarget::Tool { call_id, .. },
            }
            | WaitTarget::External { call_id, .. } => call_id,
            WaitTarget::Input { request } => &request.call_id,
            _ => return Err(fail(ErrorCode::CapabilityUnsupported, "agent.wait")),
        };
        let call = saved
            .snapshot
            .tool_ledger
            .iter()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.wait_call"))?
            .call
            .clone();
        let metadata = self.metadata_segment(saved, context.clone()).await?;
        let registered = metadata
            .tools
            .get(&call.tool_name)
            .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "agent.wait_tool"))?;
        let reference = call
            .bound_input_ref
            .as_ref()
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.wait_binding"))?;
        let record = self
            .resume_read(
                context,
                self.inner
                    .bindings
                    .state
                    .read_record(&saved.snapshot.scope, reference),
            )
            .await?;
        let bound = BoundToolInput::restore(
            &record,
            &registered.compiled,
            &saved.snapshot.scope,
            &saved.snapshot.run_id,
            &call,
            saved.snapshot.system_inputs.as_ref(),
        )?;
        if let WaitTarget::Approval {
            target: ApprovalTarget::Tool { binding_digest, .. },
        } = &wait.target
        {
            if binding_digest != bound.binding_digest() {
                return Err(fail(ErrorCode::InvalidSnapshot, "agent.wait_binding"));
            }
        }
        Ok((call, bound))
    }

    pub(super) async fn release_owned(&self, run_id: &Id, lease: &RunLease) {
        if let Ok(now) = self.inner.bindings.clock.now() {
            let _ = self
                .inner
                .bindings
                .state
                .release_lease(&self.inner.bindings.scope, run_id, lease, now.utc_ms)
                .await;
        }
    }
    pub(super) async fn resume_read<T>(
        &self,
        context: &ExecutionContext,
        future: impl std::future::Future<Output = Result<T, ContractError>>,
    ) -> Result<T, ContractError> {
        caller_read(
            context,
            Some(Duration::from_millis(
                self.inner.bindings.settings.start_timeout_ms,
            )),
            future,
        )
        .await
    }
    pub(super) fn launch_segment(
        &self,
        run_id: Id,
        segment: (Id, u64, bool),
        prompt: PromptSnapshot,
        context: ExecutionContext,
        lease: RunLease,
        observer_error: Vec<(HookTarget, HookInput)>,
    ) -> Result<RunHandle, ContractError> {
        let (segment_id, segment_start_revision, expired) = segment;
        let local = Arc::new(LocalRun::new(segment_start_revision));
        *local
            .pending_observations
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.observers"))? = observer_error;
        self.inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .insert(run_id.clone(), local.clone());
        let agent = self.clone();
        let driver_id = run_id.clone();
        let driver_local = local.clone();
        let mut data = context.data;
        data.system_inputs = None;
        let context = ExecutionContext::new(data, local.cancel.clone());
        tokio::spawn(async move {
            // Keep the large resumed driver out of this spawned closure's poll frame.
            let result = AssertUnwindSafe(crate::future::boxed(|| {
                agent.drive_leased(&driver_id, prompt, context, &driver_local, lease, expired)
            }))
            .catch_unwind()
            .await;
            let error = match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(fail(error.code, "agent.driver")),
                Err(_) => Some(fail(ErrorCode::InvalidContract, "agent.driver")),
            };
            let completed = error.is_none() && !agent.keep_local(&driver_local);
            if let Ok(mut slot) = driver_local.error.lock() {
                *slot = error;
            }
            driver_local.done.store(true, Ordering::Release);
            driver_local.notify.notify_waiters();
            if completed {
                if let Ok(mut runs) = agent.inner.runs.lock() {
                    if runs
                        .get(&driver_id)
                        .is_some_and(|current| Arc::ptr_eq(current, &driver_local))
                    {
                        runs.remove(&driver_id);
                    }
                }
            }
        });
        Ok(RunHandle {
            segment_id,
            agent: self.clone(),
            run_id,
            segment_start_revision,
            local: Some(local),
        })
    }
}

fn accepted<'a>(
    snapshot: &'a RunSnapshot,
    command: &ResumeCommand,
) -> Result<Option<&'a ResumeReceipt>, ContractError> {
    if snapshot
        .recovery_receipts
        .iter()
        .any(|receipt| receipt.command.command_id == command.command_id)
    {
        return Err(fail(ErrorCode::RequestConflict, "agent.resume_command"));
    }
    let receipt = snapshot
        .resume_receipts
        .iter()
        .find(|receipt| receipt.command.command_id == command.command_id);
    if receipt.is_some_and(|receipt| &receipt.command != command) {
        return Err(fail(ErrorCode::RequestConflict, "agent.resume_command"));
    }
    Ok(receipt)
}
fn saved_binding_digest(snapshot: &RunSnapshot, command: &ResumeCommand) -> Option<JsonDigest> {
    if let Some(receipt) = snapshot
        .resume_receipts
        .iter()
        .find(|receipt| receipt.command.command_id == command.command_id)
    {
        return match &receipt.command.action {
            ResumeAction::Approve {
                target: ApprovalTarget::Tool { binding_digest, .. },
                ..
            }
            | ResumeAction::Deny {
                target: ApprovalTarget::Tool { binding_digest, .. },
                ..
            } => Some(binding_digest.clone()),
            _ => None,
        };
    }
    match snapshot.wait.as_ref().map(|wait| &wait.target) {
        Some(WaitTarget::Approval {
            target: ApprovalTarget::Tool { binding_digest, .. },
        }) => Some(binding_digest.clone()),
        _ => None,
    }
}
fn validate_wait(snapshot: &RunSnapshot, command: &ResumeCommand) -> Result<(), ContractError> {
    if snapshot.status != RunStatus::Waiting {
        return Err(fail(ErrorCode::InvalidTransition, "agent.wait"));
    }
    if snapshot.revision != command.expected_revision {
        return Err(fail(ErrorCode::RevisionConflict, "agent.resume"));
    }
    let wait = snapshot
        .wait
        .as_ref()
        .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.wait"))?;
    let matched = match (&wait.target, &command.action) {
        (
            WaitTarget::Approval { target },
            ResumeAction::Approve {
                wait_id,
                target: supplied,
            }
            | ResumeAction::Deny {
                wait_id,
                target: supplied,
                ..
            },
        ) => wait_id == &wait.wait_id && supplied == target,
        (WaitTarget::Input { .. }, ResumeAction::Input { wait_id, .. })
        | (WaitTarget::External { .. }, ResumeAction::External { wait_id, .. }) => {
            wait_id == &wait.wait_id
        }
        _ => false,
    };
    if !matched {
        return Err(fail(ErrorCode::InvalidReference, "agent.wait_target"));
    }
    Ok(())
}
pub(super) fn apply_resolution(
    snapshot: &mut RunSnapshot,
    messages: &mut Vec<Message>,
    events: &mut Vec<RunEvent>,
    records: &mut Vec<ProtectedRecord>,
    prepared: PreparedToolResolution,
) -> Result<(), ContractError> {
    let entry = snapshot
        .tool_ledger
        .iter_mut()
        .find(|entry| entry.call.call_id == prepared.result.call_id)
        .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.resume_call"))?;
    entry.state = prepared.state;
    snapshot.last_event_seq = prepared.event.seq.get();
    messages.push(prepared.message);
    events.push(prepared.event);
    records.extend(prepared.records);
    Ok(())
}
