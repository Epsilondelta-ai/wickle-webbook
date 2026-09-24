use super::*;
use std::panic::AssertUnwindSafe;

impl Agent {
    pub(super) async fn admit(
        &self,
        request: RunRequest,
        context: ExecutionContext,
    ) -> Result<Guarded<RunHandle>, ContractError> {
        self.check_scope(&context)?;
        let bindings = &self.inner.bindings;
        let policy = PolicyRequest {
            owner_scope: bindings.scope.clone(),
            resource_id: request.request_id.clone(),
            action: PolicyAction::StartRun {},
        };
        if let Guarded::ApprovalRequired(challenge) = bindings
            .policy
            .guard(&policy, &context, None, None, || async { Ok(()) })
            .await?
        {
            return Ok(Guarded::ApprovalRequired(challenge));
        }
        let read_timeout = Some(Duration::from_millis(bindings.settings.start_timeout_ms));
        if let Some(saved) = caller_read(
            &context,
            read_timeout,
            bindings
                .state
                .find_request(&bindings.scope, &request.session_id, &request.request_id),
        )
        .await?
        {
            caller_read(
                &context,
                read_timeout,
                self.validate_replay(&request, &context, &saved),
            )
            .await?;
            return Ok(Guarded::Completed(
                self.latest_handle(saved.snapshot.run_id).await?,
            ));
        }
        if serde_json::to_vec(&request)
            .map_err(|_| fail(ErrorCode::InvalidJson, "agent.request"))?
            .len()
            > bindings.settings.max_request_bytes
        {
            return Err(fail(ErrorCode::InvalidContract, "agent.request_size"));
        }
        if request
            .input
            .iter()
            .any(|content| !matches!(content, InputContent::Text { .. }))
        {
            return Err(fail(ErrorCode::CapabilityUnsupported, "agent.request"));
        }
        self.validate_new_configuration()?;
        self.inner
            .verification
            .plan(&self.inner.profile, request.output_contract.as_ref())?;
        // Preparation may be cancelled or time out. Once durable admission begins,
        // this owned coordinator waits for its result even if the caller disconnects.
        let prepared = AssertUnwindSafe(self.prepare(request.clone(), &context)).catch_unwind();
        let (input, prompt) = tokio::select! { biased;
            _ = context.cancellation.cancelled() => return Err(fail(ErrorCode::Cancelled, "agent.admission")),
            _ = tokio::time::sleep(Duration::from_millis(bindings.settings.start_timeout_ms)) => return Err(fail(ErrorCode::DeadlineExceeded, "agent.admission")),
            result = prepared => result.map_err(|_| fail(ErrorCode::InvalidContract, "agent.preparation"))??,
        };
        // Current admission permission is checked again after metadata preparation.
        if let Guarded::ApprovalRequired(challenge) = bindings
            .policy
            .guard(&policy, &context, None, None, || async { Ok(()) })
            .await?
        {
            return Ok(Guarded::ApprovalRequired(challenge));
        }
        let candidate_id = input.snapshot.run_id.clone();
        let admission = match AssertUnwindSafe(bindings.state.admit(&bindings.scope, input))
            .catch_unwind()
            .await
        {
            Ok(result) => result,
            Err(_) => Err(fail(ErrorCode::InvalidContract, "agent.admission")),
        };
        let result = match admission {
            Ok(result) => result,
            Err(original) => {
                // A lost commit acknowledgement must not leave our admitted run
                // without a driver or create a second request on retry.
                match bindings
                    .state
                    .find_request(&bindings.scope, &request.session_id, &request.request_id)
                    .await
                {
                    Ok(Some(saved)) => {
                        self.validate_replay(&request, &context, &saved).await?;
                        AdmissionResult {
                            created: saved.snapshot.run_id == candidate_id,
                            state: saved,
                        }
                    }
                    _ => return Err(original),
                }
            }
        };
        if !result.created {
            self.validate_replay(&request, &context, &result.state)
                .await?;
            return Ok(Guarded::Completed(
                self.latest_handle(result.state.snapshot.run_id.clone())
                    .await?,
            ));
        }
        let run_id = result.state.snapshot.run_id;
        let local = Arc::new(LocalRun::new(0));
        self.inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .insert(run_id.clone(), local.clone());
        let agent = self.clone();
        let driver_id = run_id.clone();
        let driver_local = local.clone();
        let mut data = context.data;
        // Runtime tool values remain in protected storage. The model driver has
        // no reason to carry the admission map into model callbacks.
        data.system_inputs = None;
        let driver_context = ExecutionContext::new(data, local.cancel.clone());
        tokio::spawn(async move {
            let result = AssertUnwindSafe(crate::future::boxed(|| {
                agent.drive(&driver_id, prompt, driver_context, &driver_local)
            }))
            .catch_unwind()
            .await;
            let error = match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(fail(error.code, "agent.driver")),
                Err(_) => Some(fail(ErrorCode::InvalidContract, "agent.driver")),
            };
            let completed = error.is_none() && !agent.keep_local(&driver_local);
            if let Ok(mut saved) = driver_local.error.lock() {
                *saved = error;
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
        let segment_id = crate::state::initial_segment_id(&run_id)?;
        Ok(Guarded::Completed(RunHandle {
            segment_id,
            agent: self.clone(),
            run_id,
            segment_start_revision: 0,
            local: Some(local),
        }))
    }

    async fn validate_replay(
        &self,
        request: &RunRequest,
        context: &ExecutionContext,
        saved: &StoredRun,
    ) -> Result<(), ContractError> {
        match self
            .inner
            .bindings
            .state
            .read_execution(&self.inner.bindings.scope, &saved.snapshot.run_id)
            .await
        {
            Ok(history) => {
                if let Some(submitted) = history.submitted {
                    let candidate = self.capture_submission(request, context)?;
                    if !submitted
                        .matches_submission(&candidate, crate::JsonTextLimits::default())?
                    {
                        return Err(fail(ErrorCode::RequestConflict, "agent.submitted_request"));
                    }
                    return Ok(());
                }
            }
            Err(error) if error.code == ErrorCode::CapabilityUnsupported => {}
            Err(error) => return Err(error),
        }
        if self.inner.profile.digest() != *saved.snapshot.profile.profile_digest()
            || admission_digest(
                request,
                &saved.snapshot.profile,
                saved.snapshot.system_inputs.as_ref(),
            ) != saved.snapshot.request_digest
        {
            return Err(fail(ErrorCode::RequestConflict, "agent.request"));
        }
        if let Some(reference) = &saved.snapshot.system_inputs {
            let record = self
                .inner
                .bindings
                .state
                .read_record(&self.inner.bindings.scope, &reference.snapshot_ref)
                .await?;
            let values =
                RunSystemInputs::from_value(record.value(), reference, &saved.snapshot.scope)?;
            // start omission means empty input. Only resume may reuse saved values
            // through an omitted map, and this path handles start replay exclusively.
            let empty = SystemInputs::default();
            values.validate_resume(Some(context.data.system_inputs.as_ref().unwrap_or(&empty)))?;
        } else if context
            .data
            .system_inputs
            .as_ref()
            .is_some_and(|values| !values.values().is_empty())
        {
            return Err(fail(ErrorCode::SystemInputsMismatch, "agent.system_inputs"));
        }
        Ok(())
    }

    fn capture_submission(
        &self,
        request: &RunRequest,
        context: &ExecutionContext,
    ) -> Result<crate::RequestSnapshot, ContractError> {
        let request = serde_json::to_string(request)
            .map_err(|_| fail(ErrorCode::InvalidJson, "agent.request"))?;
        let system = context
            .data
            .system_inputs
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|_| fail(ErrorCode::InvalidJson, "agent.system_inputs"))?;
        crate::RequestSnapshot::capture(
            VersionedRef {
                id: self.inner.profile.agent_id.clone(),
                version: self.inner.profile.version.clone(),
            },
            &request,
            system.as_deref(),
            crate::JsonTextLimits::default(),
        )
    }

    async fn prepare(
        &self,
        request: RunRequest,
        context: &ExecutionContext,
    ) -> Result<(AdmissionInput, PromptSnapshot), ContractError> {
        let bindings = &self.inner.bindings;
        let submitted = self.capture_submission(&request, context)?;
        let routing = bindings.router.snapshot().clone();
        if routing.scope() != &bindings.scope {
            return Err(fail(ErrorCode::AccessDenied, "agent.router_scope"));
        }
        self.inner.context.validate_router(&routing)?;
        let profile = ProfileValidator::new(bindings.profile_resolver.as_ref())
            .validate(&self.inner.profile, &bindings.scope)
            .await?;
        let assembly = if let Some(runtime) = &bindings.components {
            let resolve_context = ComponentResolveContext {
                scope: bindings.scope.clone(),
                session_id: request.session_id.clone(),
                principal_ref: context.data.principal_ref.clone(),
                capability_grant_ref: context.data.capability_grant_ref.clone(),
                system_inputs: bindings.system_inputs.clone(),
                cancellation: context.cancellation.child_token(),
                deadline: tokio::time::Instant::now()
                    + Duration::from_millis(bindings.settings.start_timeout_ms),
            };
            let resolved = runtime.resolve(&profile, &resolve_context).await?;
            resolve_context.cancellation.cancel();
            if resolved.scope() != &bindings.scope
                || resolved.session_id() != &request.session_id
                || resolved.profile_resolution_digest() != profile.resolution_digest()
            {
                return Err(fail(ErrorCode::InvalidSnapshot, "agent.assembly"));
            }
            Some(resolved)
        } else {
            None
        };
        let tool_bindings = if let Some(assembly) = &assembly {
            ToolRegistry::metadata(bindings.scope.clone(), assembly.tools().to_vec())?
                .prompt_bindings(profile.profile())?
        } else {
            bindings
                .tools
                .as_ref()
                .map(|tools| tools.prompt_bindings(profile.profile()))
                .transpose()?
                .unwrap_or_default()
        };
        let skill_plan = if profile.profile().skills.is_empty() {
            None
        } else {
            Some(
                bindings
                    .skills
                    .as_ref()
                    .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "agent.skills"))?
                    .plan(&profile, &tool_bindings, bindings.profile_resolver.as_ref())
                    .await?,
            )
        };
        let skill_listings = skill_plan
            .as_ref()
            .map(SkillPlan::listings)
            .unwrap_or_default();
        let skill_record = skill_plan
            .as_ref()
            .map(|plan| {
                Ok::<_, ContractError>(ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(plan)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.skill_plan"))?,
                ))
            })
            .transpose()?;
        let session = match bindings
            .state
            .load_session(&bindings.scope, &request.session_id)
            .await
        {
            Ok(session) => Some(session),
            Err(error) if error.code == ErrorCode::StateNotFound => None,
            Err(error) => return Err(error),
        };
        let context_plan = self
            .inner
            .context
            .plan(profile.profile(), &bindings.scope)?;
        let context_revision_ref = session
            .as_ref()
            .and_then(|session| session.context_revision_ref.clone());
        if let Some(reference) = &context_revision_ref {
            self.inner
                .context
                .validate_session_plan(
                    reference,
                    &request.session_id,
                    &profile,
                    &context_plan,
                    bindings.state.as_ref(),
                )
                .await?;
        }
        let verification_plan = self
            .inner
            .verification
            .plan(profile.profile(), request.output_contract.as_ref())?;
        let verification_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(&verification_plan)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.verification_plan"))?,
        );
        let context_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(&context_plan)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.context_plan"))?,
        );
        let (prompt, prompt_record, sequence) = if let Some(session) = session {
            let record = bindings
                .state
                .read_record(&bindings.scope, &session.prompt_snapshot)
                .await?;
            let prompt = PromptSnapshot::restore(
                &serde_json::to_string(record.value())
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.prompt"))?,
                &record.reference().digest,
                &profile,
                &bindings.scope,
            )?;
            (
                prompt,
                record,
                session.transcript_revision.checked_add(1).ok_or_else(|| {
                    fail(ErrorCode::InvalidSnapshot, "session.transcript_revision")
                })?,
            )
        } else {
            let prompt = PromptSnapshot::create(
                &profile,
                bindings.host_instructions.clone(),
                None,
                tool_bindings.clone(),
                skill_listings.clone(),
            )?;
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(&prompt)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.prompt"))?,
            );
            (prompt, record, 1)
        };
        if prompt.skills() != skill_listings
            || prompt.tools().len() != tool_bindings.len()
            || prompt
                .tools()
                .iter()
                .zip(&tool_bindings)
                .any(|(pinned, binding)| {
                    pinned.selection != binding.selection
                        || pinned.compiled_digest != *binding.compiled.digest()
                        || pinned.descriptor_digest != *binding.compiled.descriptor_digest()
                        || pinned.model_tool != binding.compiled.to_model_tool()
                })
        {
            return Err(fail(ErrorCode::ContextMismatch, "agent.pinned_tools"));
        }
        let inputs = RunSystemInputs::capture(
            bindings.scope.clone(),
            context.data.system_inputs.clone(),
            &bindings.system_inputs,
        )?;
        let inputs_record = inputs.to_record(bindings.ids.next_id()?, 1);
        let inputs_ref = inputs.snapshot_ref(inputs_record.reference())?;
        let request_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(&request)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.request"))?,
        );
        let routing_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(&routing)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.routing"))?,
        );
        let run_id = bindings.ids.next_id()?;
        let hook_plan = if let Some(assembly) = &assembly {
            Some(
                HookRegistry::metadata(bindings.scope.clone(), assembly.hooks().to_vec())?
                    .plan(profile.profile())?,
            )
        } else {
            bindings
                .hooks
                .as_ref()
                .map(|hooks| hooks.plan(profile.profile()))
                .transpose()?
        };
        let hook_record = hook_plan
            .as_ref()
            .map(|plan| {
                Ok::<_, ContractError>(ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(plan)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.hooks"))?,
                ))
            })
            .transpose()?;
        let assembly_record = assembly
            .as_ref()
            .map(|assembly| {
                Ok::<_, ContractError>(ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(assembly)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.assembly"))?,
                ))
            })
            .transpose()?;
        let source_plan = if let Some(assembly) = &assembly {
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
                    .plan(profile.profile(), &estimator.version())?,
                )
            }
        } else {
            bindings
                .context_sources
                .as_ref()
                .map(|sources| sources.plan(profile.profile()))
                .transpose()?
        };
        let source_record = source_plan
            .as_ref()
            .map(|plan| {
                Ok::<_, ContractError>(ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(plan)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.sources"))?,
                ))
            })
            .transpose()?;
        let interruption_plan = InterruptionPlan::capture(
            bindings.interruption_policy.as_ref(),
            bindings.settings.interruption_timeout_ms,
        )?;
        let interruption_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(interruption_plan)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.interruption_plan"))?,
        );
        let now = bindings.clock.now()?.utc_ms;
        let snapshot = RunSnapshot {
            interruption_plan_ref: Some(interruption_record.reference().clone()),
            interruption_records: vec![],
            app_state: None,
            model_step_inputs: vec![],
            prepared_steps: vec![],
            active_prepared_step: None,
            schema_version: RunSnapshotSchemaVersion::V1,
            run_id: run_id.clone(),
            request_digest: admission_digest(&request, &profile, Some(&inputs_ref)),
            request: request.clone(),
            scope: bindings.scope.clone(),
            limits: profile.profile().limits.clone(),
            timing: RunTiming::new(now, profile.profile().limits.max_elapsed_ms.get())?,
            profile,
            status: RunStatus::Running,
            phase: RunPhase::Admission,
            model_step_id: None,
            usage: BudgetUsage::default(),
            reservations: vec![],
            model_ledger: vec![],
            tool_ledger: vec![],
            system_inputs: Some(inputs_ref),
            wait: None,
            outcome: None,
            assembly_ref: assembly_record
                .as_ref()
                .map(|record| record.reference().clone()),
            routing_snapshot_ref: Some(routing_record.reference().clone()),
            context_batches: vec![],
            source_states: vec![],
            source_plan_ref: source_record
                .as_ref()
                .map(|record| record.reference().clone()),
            skill_plan_ref: skill_record
                .as_ref()
                .map(|record| record.reference().clone()),
            context_plan_ref: Some(context_record.reference().clone()),
            context_revision_ref,
            context_decisions: vec![],
            verification_plan_ref: Some(verification_record.reference().clone()),
            candidate_ref: None,
            verification_records: vec![],
            revision: 0,
            resume_receipts: vec![],
            recovery_receipts: vec![],
            hook_plan_ref: hook_record
                .as_ref()
                .map(|record| record.reference().clone()),
            hook_applications: vec![],
            last_event_seq: 1,
        };
        let message = Message {
            source_model_request_id: None,
            message_id: bindings.ids.next_id()?,
            run_id: run_id.clone(),
            sequence: sequence
                .try_into()
                .map_err(|_| fail(ErrorCode::InvalidSnapshot, "message.sequence"))?,
            role: MessageRole::User,
            content: request
                .input
                .into_iter()
                .map(|content| ContentBlock::Content { content })
                .collect(),
            origin: MessageOrigin::User,
            visibility: Visibility::UserAndModel,
        };
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: bindings.ids.next_id()?,
            scope: bindings.scope.clone(),
            run_id,
            session_id: snapshot.request.session_id.clone(),
            seq: NonZeroU64::new(1).expect("initial sequence"),
            timestamp_ms: now,
            payload: RunEventPayload::RunStarted {
                request_ref: request_record.reference().clone(),
                profile_digest: snapshot.profile.profile_digest().clone(),
            },
        };
        Ok((
            AdmissionInput {
                execution_principal_ref: context.data.principal_ref.clone(),
                execution_grant_ref: context.data.capability_grant_ref.clone(),
                submitted: Some(submitted),
                snapshot,
                prompt_snapshot: prompt_record.reference().clone(),
                require_durable: bindings.settings.require_durable,
                messages: vec![message],
                events: vec![event],
                records: [
                    vec![
                        request_record,
                        prompt_record,
                        inputs_record,
                        routing_record,
                        interruption_record,
                    ],
                    hook_record.into_iter().collect(),
                    assembly_record.into_iter().collect(),
                    source_record.into_iter().collect(),
                    skill_record.into_iter().collect(),
                    vec![context_record, verification_record],
                ]
                .concat(),
            },
            prompt,
        ))
    }
}
