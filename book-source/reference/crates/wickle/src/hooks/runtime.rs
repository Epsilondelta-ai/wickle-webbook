use super::*;
use futures_util::FutureExt;
use std::{future::Future, panic::AssertUnwindSafe, time::Duration};

impl HookRuntime {
    /// Inject existing components. Transform persistence always uses the RunBudget's
    /// authoritative store; the injected store is used after the Run has ended.
    pub fn new(
        store: Arc<dyn StateStore>,
        policy: Arc<PolicyGate>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdSource>,
        registry: Arc<HookRegistry>,
    ) -> Self {
        Self {
            binding_set_id: None,
            store,
            policy,
            clock,
            ids,
            registry,
        }
    }
    /// Forward the current Host segment identity to scoped export wrappers.
    pub fn with_binding_set_id(mut self, binding_set_id: Id) -> Self {
        self.binding_set_id = Some(binding_set_id);
        self
    }
    /// Scope under which callbacks are registered.
    pub fn scope(&self) -> &Scope {
        self.registry.scope()
    }
    /// Pure construction of the exact selected plan.
    pub fn plan(&self, profile: &AgentProfile) -> Result<HookPlan, ContractError> {
        self.registry.plan(profile)
    }

    async fn pinned_plan(
        &self,
        snapshot: &RunSnapshot,
        store: &dyn StateStore,
    ) -> Result<HookPlan, ContractError> {
        if &snapshot.scope != self.scope() {
            return Err(hook_error(ErrorCode::AccessDenied, "hooks.scope"));
        }
        let expected = self.plan(snapshot.profile.profile())?;
        let reference = snapshot
            .hook_plan_ref
            .as_ref()
            .ok_or_else(|| hook_error(ErrorCode::InvalidSnapshot, "hooks.plan_ref"))?;
        let record = store.read_record(&snapshot.scope, reference).await?;
        if record.reference() != reference {
            return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.plan_record"));
        }
        let plan = HookPlan::restore(
            &serde_json::to_string(record.value())
                .map_err(|_| hook_error(ErrorCode::InvalidJson, "hooks.plan"))?,
            &snapshot.scope,
            &reference.digest,
        )?;
        if plan != expected {
            return Err(hook_error(ErrorCode::ProfileMismatch, "hooks.plan"));
        }
        Ok(plan)
    }

    /// Read and fold only an already persisted prefix; this never invokes a Hook.
    pub async fn saved_transform(
        &self,
        snapshot: &RunSnapshot,
        target: &HookTarget,
    ) -> Result<Option<HookTransform>, ContractError> {
        let plan = self.pinned_plan(snapshot, self.store.as_ref()).await?;
        let applications: Vec<_> = snapshot
            .hook_applications
            .iter()
            .filter(|application| &application.target == target)
            .cloned()
            .collect();
        let mut current = None;
        let mut deny = None;
        for application in &applications {
            let record = self
                .store
                .read_record(&snapshot.scope, &application.result_ref)
                .await?;
            let value = HookApplicationRecord::restore(
                &record,
                &plan,
                application,
                &snapshot.scope,
                &snapshot.run_id,
            )?;
            if current.is_none() {
                current = Some(value.input.clone());
            }
            if deny.is_some() {
                return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.denied_chain"));
            }
            deny = records::apply(current.as_mut().expect("first input"), &value)?;
        }
        Ok(current.map(|input| records::transformed(input, deny, applications)))
    }

    /// Reuse each saved application before executing the remaining selected chain.
    /// Invalid output always fails closed; only optional before_run callback
    /// failure is persisted as a warning and permits the next callback.
    pub async fn transform(
        &self,
        target: HookTarget,
        mut input: HookInput,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<HookTransform, ContractError> {
        self.check_scope(context, budget.scope())?;
        input.validate(&target)?;
        if matches!(
            target,
            HookTarget::AfterTool { .. } | HookTarget::AfterRun { .. }
        ) {
            return Err(hook_error(
                ErrorCode::InvalidContract,
                "hooks.transform_target",
            ));
        }
        budget.check_boundary().await?;
        let saved = bounded(
            context,
            Some(budget),
            budget.call_deadline()?,
            budget.store().load(budget.scope(), budget.run_id()),
        )
        .await?;
        let plan = bounded(
            context,
            Some(budget),
            budget.call_deadline()?,
            self.pinned_plan(&saved.snapshot, budget.store().as_ref()),
        )
        .await?;
        bounded(
            context,
            Some(budget),
            budget.call_deadline()?,
            self.validate_live_input(&saved, &target, &input, budget.store().as_ref()),
        )
        .await?;
        if let HookInput::BeforeTool {
            tool, model_inputs, ..
        } = &mut input
        {
            *model_inputs = crate::tool_schema::normalize_model_input_schema(
                &tool.model_input_schema,
                model_inputs,
            )?;
            // Replay the exact historical first input. Old records can predate
            // default-before-hook ordering and must not be silently rewritten.
            if let Some(first) = saved
                .snapshot
                .hook_applications
                .iter()
                .find(|application| application.target == target)
            {
                let record = bounded(
                    context,
                    Some(budget),
                    budget.call_deadline()?,
                    budget
                        .store()
                        .read_record(budget.scope(), &first.result_ref),
                )
                .await?;
                let first = HookApplicationRecord::restore(
                    &record,
                    &plan,
                    first,
                    budget.scope(),
                    budget.run_id(),
                )?;
                if let HookInput::BeforeTool {
                    model_inputs: original,
                    ..
                } = first.input
                {
                    *model_inputs = original;
                }
            }
        }
        let definitions: Vec<_> = plan
            .definitions()
            .iter()
            .enumerate()
            .filter(|(_, definition)| definition.position == target.position())
            .map(|(index, definition)| (definition.clone(), plan.selection(index).cloned()))
            .collect();
        let mut applications = Vec::new();
        let mut deny = None;
        for (definition, selection) in definitions {
            budget.check_boundary().await?;
            if context.cancellation.is_cancelled() {
                return Err(hook_error(ErrorCode::Cancelled, "hooks.transform"));
            }
            let current = bounded(
                context,
                Some(budget),
                budget.call_deadline()?,
                budget.store().load(budget.scope(), budget.run_id()),
            )
            .await?;
            if let Some(application) =
                current
                    .snapshot
                    .hook_applications
                    .iter()
                    .find(|application| {
                        application.target == target
                            && application.hook == definition.hook
                            && application.selection == selection
                    })
            {
                let record = bounded(
                    context,
                    Some(budget),
                    budget.call_deadline()?,
                    budget
                        .store()
                        .read_record(budget.scope(), &application.result_ref),
                )
                .await?;
                let value = HookApplicationRecord::restore(
                    &record,
                    &plan,
                    application,
                    budget.scope(),
                    budget.run_id(),
                )?;
                deny = records::apply(&mut input, &value)?;
                applications.push(application.clone());
                if deny.is_some() {
                    break;
                }
                continue;
            }
            let callback = self
                .invoke(
                    &definition,
                    selection.as_ref(),
                    &target,
                    &input,
                    budget.run_id(),
                    context,
                    Some(budget),
                    None,
                )
                .await?;
            let (output, failure) = match callback {
                Ok(output) => {
                    let output = records::normalize_output(&input, output)?;
                    records::validate_output(&definition, &input, &output)?;
                    (Some(output), None)
                }
                Err(code) if target == HookTarget::BeforeRun && !definition.required => {
                    (None, Some(code))
                }
                Err(_) => return Err(hook_error(ErrorCode::InvalidContract, "hooks.callback")),
            };
            let context_items = if let Some(HookOutput::Context { additions }) = &output {
                additions
                    .iter()
                    .map(|addition| {
                        records::stamped(
                            self.ids.next_id()?,
                            budget.scope(),
                            budget.run_id(),
                            &definition,
                            &target,
                            addition,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                vec![]
            };
            let value = HookApplicationRecord {
                selection: selection.clone(),
                scope: budget.scope().clone(),
                run_id: budget.run_id().clone(),
                hook: definition.hook.clone(),
                definition_digest: definition.digest(),
                target: target.clone(),
                input: input.clone(),
                output,
                context_items,
                failure,
            };
            let record = ProtectedRecord::new(
                self.ids.next_id()?,
                1,
                serde_json::to_value(&value)
                    .map_err(|_| hook_error(ErrorCode::InvalidJson, "hooks.application"))?,
            );
            let application = HookApplication {
                selection: selection.clone(),
                hook: definition.hook.clone(),
                definition_digest: definition.digest(),
                target: target.clone(),
                input_digest: input.digest(),
                result_ref: record.reference().clone(),
            };
            budget.check_boundary().await?;
            let mut snapshot = bounded(
                context,
                Some(budget),
                budget.call_deadline()?,
                budget.store().load(budget.scope(), budget.run_id()),
            )
            .await?
            .snapshot;
            if snapshot.hook_applications.iter().any(|prior| {
                prior.target == target
                    && prior.hook == definition.hook
                    && prior.selection == selection
            }) {
                return Err(hook_error(ErrorCode::RevisionConflict, "hooks.application"));
            }
            let expected_revision = snapshot.revision;
            let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
            snapshot.revision = snapshot
                .revision
                .checked_add(1)
                .ok_or_else(|| hook_error(ErrorCode::RevisionConflict, "hooks.revision"))?;
            snapshot.usage.elapsed_ms = elapsed;
            snapshot.timing.last_observed_at_ms = now;
            snapshot.hook_applications.push(application.clone());
            bounded(
                context,
                Some(budget),
                budget.call_deadline()?,
                budget.store().commit(
                    budget.scope(),
                    budget.run_id(),
                    CommitInput {
                        control_commands: vec![],
                        expected_revision,
                        lease: budget.lease().clone(),
                        now_ms: now,
                        snapshot,
                        messages: vec![],
                        events: vec![],
                        records: vec![record],
                    },
                ),
            )
            .await?;
            deny = records::apply(&mut input, &value)?;
            applications.push(application);
            if deny.is_some() {
                break;
            }
        }
        Ok(records::transformed(input, deny, applications))
    }

    /// Observe committed data with finite cleanup time independent of the expired
    /// Run budget. Stored reports suppress repeat delivery; no crash replay or
    /// exactly-once external effect guarantee is provided.
    pub async fn observe(
        &self,
        run_id: &Id,
        target: HookTarget,
        input: HookInput,
        context: &ExecutionContext,
    ) -> Result<(), ContractError> {
        self.check_scope(context, self.scope())?;
        input.validate(&target)?;
        if !matches!(
            target,
            HookTarget::AfterTool { .. } | HookTarget::AfterRun { .. }
        ) {
            return Err(hook_error(
                ErrorCode::InvalidContract,
                "hooks.observer_target",
            ));
        }
        // Terminal cleanup is independent of the execution token that may have
        // been cancelled to produce this outcome. Current Host policy still runs.
        let cleanup_context;
        let context = if matches!(target, HookTarget::AfterRun { .. }) {
            let mut data = context.data.clone();
            data.system_inputs = None;
            cleanup_context = ExecutionContext::new(data, CancellationToken::new());
            &cleanup_context
        } else {
            context
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let saved = bounded(
            context,
            None,
            deadline,
            self.store.load(self.scope(), run_id),
        )
        .await?;
        let plan = bounded(
            context,
            None,
            deadline,
            self.pinned_plan(&saved.snapshot, self.store.as_ref()),
        )
        .await?;
        bounded(
            context,
            None,
            deadline,
            self.validate_live_input(&saved, &target, &input, self.store.as_ref()),
        )
        .await?;
        let prior = bounded(
            context,
            None,
            deadline,
            self.store.read_hook_observations(self.scope(), run_id),
        )
        .await?;
        let observer_deadline = if matches!(target, HookTarget::AfterTool { .. }) {
            let remaining = saved
                .snapshot
                .timing
                .deadline_at_ms
                .saturating_sub(self.clock.now()?.utc_ms)
                .max(0) as u64;
            deadline.min(tokio::time::Instant::now() + Duration::from_millis(remaining))
        } else {
            deadline
        };
        for (index, definition) in plan
            .definitions()
            .iter()
            .enumerate()
            .filter(|(_, definition)| definition.position == target.position())
        {
            let selection = plan.selection(index);
            if prior.iter().any(|report| {
                report.hook == definition.hook
                    && report.selection.as_ref() == selection
                    && report.definition_digest == definition.digest()
                    && report.target == target
            }) {
                continue;
            }
            let observed = self
                .invoke(
                    definition,
                    selection,
                    &target,
                    &input,
                    run_id,
                    context,
                    None,
                    Some(observer_deadline),
                )
                .await;
            let status = match observed {
                Ok(Ok(output)) => match records::validate_output(definition, &input, &output) {
                    Ok(()) => HookObservationStatus::Completed,
                    Err(error) => HookObservationStatus::Failed {
                        code: code_id(error.code)?,
                    },
                },
                Ok(Err(code)) => HookObservationStatus::Failed { code },
                Err(error) => HookObservationStatus::Failed {
                    code: code_id(error.code)?,
                },
            };
            let report = HookObservation {
                selection: selection.cloned(),
                scope: self.scope().clone(),
                run_id: run_id.clone(),
                hook: definition.hook.clone(),
                definition_digest: definition.digest(),
                target: target.clone(),
                input_digest: input.digest(),
                status,
                timestamp_ms: self.clock.now()?.utc_ms,
            };
            // Report failures remain separate from the already committed result.
            // A fresh cleanup token allows recording that the caller cancelled.
            let mut data = context.data.clone();
            data.system_inputs = None;
            let cleanup = ExecutionContext::new(data, CancellationToken::new());
            bounded(
                &cleanup,
                None,
                tokio::time::Instant::now() + Duration::from_secs(30),
                self.store
                    .record_hook_observation(self.scope(), run_id, report),
            )
            .await?;
        }
        Ok(())
    }

    fn check_scope(&self, context: &ExecutionContext, scope: &Scope) -> Result<(), ContractError> {
        if self.scope() != scope || &context.data.scope != scope {
            return Err(hook_error(ErrorCode::AccessDenied, "hooks.scope"));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn invoke(
        &self,
        definition: &HookDefinition,
        selection: Option<&HookRef>,
        target: &HookTarget,
        input: &HookInput,
        run_id: &Id,
        context: &ExecutionContext,
        budget: Option<&RunBudget>,
        external_deadline: Option<tokio::time::Instant>,
    ) -> Result<Result<HookOutput, Id>, ContractError> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(definition.timeout_ms);
        let deadline = if let Some(budget) = budget {
            deadline.min(budget.call_deadline()?)
        } else {
            deadline
        };
        let deadline = external_deadline.map_or(deadline, |external| deadline.min(external));
        let cancellation = context.cancellation.child_token();
        let _cancel = cancellation.clone().drop_guard();
        let mut data = context.data.clone();
        data.system_inputs = None;
        let policy_context = ExecutionContext::new(data, cancellation.clone());
        let request = PolicyRequest {
            owner_scope: self.scope().clone(),
            resource_id: run_id.clone(),
            action: PolicyAction::InvokeHook {
                selection: selection.cloned(),
                hook: definition.hook.clone(),
                definition_digest: definition.digest(),
                target: target.clone(),
            },
        };
        let decision = bounded(
            &policy_context,
            budget,
            deadline,
            self.policy
                .check(&request, &policy_context, Some(deadline), None),
        )
        .await?;
        if decision != (PolicyDecision::Allow {}) {
            return Err(hook_error(ErrorCode::AccessDenied, "hooks.policy"));
        }
        if let Some(budget) = budget {
            budget.check_boundary().await?;
        }
        let entry = self
            .registry
            .get(&definition.hook, selection)
            .filter(|entry| entry.definition == *definition)
            .ok_or_else(|| hook_error(ErrorCode::ComponentUnavailable, "hooks.handler"))?;
        let hook_context = HookContext {
            selection: selection.cloned(),
            binding_set_id: self.binding_set_id.clone(),
            scope: self.scope().clone(),
            run_id: run_id.clone(),
            hook: definition.hook.clone(),
            target: target.clone(),
            principal_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
            cancellation: cancellation.clone(),
            deadline,
        };
        let operation = AssertUnwindSafe(async { entry.handler.call(input, &hook_context).await })
            .catch_unwind();
        let result = tokio::select! {biased;
            _=context.cancellation.cancelled()=>Err(hook_error(ErrorCode::Cancelled,"hooks.callback")),
            stopped=run_stopped(budget)=>Err(stopped),
            _=tokio::time::sleep_until(deadline)=>Ok(Err(code_id(ErrorCode::DeadlineExceeded)?)),
            result=operation=>Ok(match result{Ok(Ok(output))=>Ok(output),Ok(Err(error))=>Err(code_id(error.code)?),Err(_)=>Err(code_id(ErrorCode::InvalidContract)?)})
        };
        cancellation.cancel();
        result
    }

    async fn validate_live_input(
        &self,
        saved: &StoredRun,
        target: &HookTarget,
        input: &HookInput,
        store: &dyn StateStore,
    ) -> Result<(), ContractError> {
        let invalid = || hook_error(ErrorCode::InvalidSnapshot, "hooks.target_input");
        match (target, input) {
            (
                HookTarget::BeforeRun,
                HookInput::BeforeRun {
                    user_input,
                    context_items,
                },
            ) if user_input == &saved.snapshot.request.input && context_items.is_empty() => {}
            (
                HookTarget::BeforeModel { model_step_id },
                HookInput::BeforeModel {
                    user_input,
                    context_items,
                },
            ) if user_input == &saved.snapshot.request.input
                && saved.snapshot.model_step_id.as_ref() == Some(model_step_id) =>
            {
                let mut source_records = Vec::new();
                if let Some(reference) = &saved.snapshot.source_plan_ref {
                    source_records.push(store.read_record(&saved.snapshot.scope, reference).await?);
                }
                for reference in &saved.snapshot.context_batches {
                    source_records.push(store.read_record(&saved.snapshot.scope, reference).await?);
                }
                for reference in crate::skills::records::references(&saved.snapshot) {
                    source_records.push(store.read_record(&saved.snapshot.scope, reference).await?);
                }
                let mut expected = records::extension_context_for_step(
                    &saved.snapshot,
                    &source_records,
                    model_step_id,
                )?;
                for application in saved
                    .snapshot
                    .hook_applications
                    .iter()
                    .filter(|application| application.target == HookTarget::BeforeRun)
                {
                    let record = store
                        .read_record(&saved.snapshot.scope, &application.result_ref)
                        .await?;
                    let value: HookApplicationRecord =
                        serde_json::from_value(record.value().clone()).map_err(|_| invalid())?;
                    expected.extend(value.context_items);
                }
                if context_items != &expected {
                    return Err(invalid());
                }
            }
            (
                HookTarget::BeforeTool { call_id },
                HookInput::BeforeTool {
                    tool,
                    descriptor_digest,
                    compiled_digest,
                    original_model_inputs,
                    model_inputs,
                    ..
                },
            ) => {
                let call = &saved
                    .snapshot
                    .tool_ledger
                    .iter()
                    .find(|entry| &entry.call.call_id == call_id)
                    .ok_or_else(invalid)?
                    .call;
                if call.bound_input_ref.is_some()
                    || original_model_inputs != &call.model_inputs
                    || (model_inputs != original_model_inputs
                        && model_inputs
                            != &crate::tool_schema::normalize_model_input_schema(
                                &tool.model_input_schema,
                                original_model_inputs,
                            )?)
                    || call.descriptor_digest.as_ref() != Some(descriptor_digest)
                {
                    return Err(invalid());
                }
                let record = store
                    .read_record(&saved.snapshot.scope, &saved.session.prompt_snapshot)
                    .await?;
                let prompt = PromptSnapshot::restore(
                    &serde_json::to_string(record.value()).map_err(|_| invalid())?,
                    &saved.session.prompt_snapshot.digest,
                    &saved.snapshot.profile,
                    &saved.snapshot.scope,
                )?;
                if !prompt.tools().iter().any(|entry| {
                    entry.model_tool == *tool
                        && &entry.descriptor_digest == descriptor_digest
                        && &entry.compiled_digest == compiled_digest
                }) {
                    return Err(invalid());
                }
            }
            (
                HookTarget::AfterTool {
                    call_id,
                    result_ref,
                },
                HookInput::AfterTool { .. },
            ) => {
                let record = store.read_record(&saved.snapshot.scope, result_ref).await?;
                let result: ToolResult =
                    serde_json::from_value(record.value().clone()).map_err(|_| invalid())?;
                if record.reference() != result_ref
                    || &result.call_id != call_id
                    || &HookInput::tool_observed(call_id, &result) != input
                {
                    return Err(invalid());
                }
            }
            (
                HookTarget::AfterRun {
                    outcome_ref,
                    revision,
                },
                HookInput::AfterRun { .. },
            ) => {
                let record = store
                    .read_record(&saved.snapshot.scope, outcome_ref)
                    .await?;
                let outcome: RunOutcome =
                    serde_json::from_value(record.value().clone()).map_err(|_| invalid())?;
                if record.reference() != outcome_ref
                    || !saved.snapshot.status.is_terminal()
                    || saved.snapshot.revision != *revision
                    || saved.snapshot.outcome.as_ref() != Some(&outcome)
                    || &HookInput::run_observed(&outcome) != input
                {
                    return Err(invalid());
                }
            }
            _ => return Err(invalid()),
        }
        if matches!(
            target,
            HookTarget::AfterTool { .. } | HookTarget::AfterRun { .. }
        ) {
            let mut after = 0;
            let mut found = false;
            loop {
                let page = store
                    .read_events(&saved.snapshot.scope, &saved.snapshot.run_id, after, 256)
                    .await?;
                found |= page
                    .events
                    .iter()
                    .any(|event| match (target, &event.payload) {
                        (
                            HookTarget::AfterTool { result_ref, .. },
                            RunEventPayload::ToolSettled {
                                result_ref: reference,
                            }
                            | RunEventPayload::ToolUnresolved {
                                result_ref: reference,
                                ..
                            },
                        ) => result_ref == reference,
                        (
                            HookTarget::AfterRun { outcome_ref, .. },
                            RunEventPayload::RunFinished {
                                outcome_ref: reference,
                            },
                        ) => outcome_ref == reference,
                        _ => false,
                    });
                if found || !page.has_more {
                    break;
                }
                if page.next_after_seq <= after {
                    return Err(invalid());
                }
                after = page.next_after_seq;
            }
            if !found {
                return Err(invalid());
            }
        }
        Ok(())
    }
}

async fn run_stopped(budget: Option<&RunBudget>) -> ContractError {
    if let Some(budget) = budget {
        budget
            .wait_for_cancellation_or_deadline()
            .await
            .err()
            .unwrap_or_else(|| hook_error(ErrorCode::DeadlineExceeded, "hooks.run"))
    } else {
        std::future::pending().await
    }
}
async fn bounded<T>(
    context: &ExecutionContext,
    budget: Option<&RunBudget>,
    deadline: tokio::time::Instant,
    operation: impl Future<Output = Result<T, ContractError>>,
) -> Result<T, ContractError> {
    tokio::select! {biased;
        _=context.cancellation.cancelled()=>Err(hook_error(ErrorCode::Cancelled,"hooks.operation")),
        stopped=run_stopped(budget)=>Err(stopped),
        _=tokio::time::sleep_until(deadline)=>Err(hook_error(ErrorCode::DeadlineExceeded,"hooks.operation")),
        result=operation=>result,
    }
}
fn code_id(code: ErrorCode) -> Result<Id, ContractError> {
    Id::new(
        serde_json::to_value(code)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .unwrap_or_else(|| "invalid_contract".into()),
    )
}
