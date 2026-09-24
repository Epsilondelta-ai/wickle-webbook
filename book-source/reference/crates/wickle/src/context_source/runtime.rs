use super::*;
use futures_util::FutureExt;
use std::{future::Future, panic::AssertUnwindSafe, time::Duration};

impl ContextSourceRuntime {
    /// Capture the estimator version once; no provider callback is invoked.
    pub fn new(
        store: Arc<dyn StateStore>,
        policy: Arc<PolicyGate>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdSource>,
        registry: Arc<ContextSourceRegistry>,
        estimator: Arc<dyn ContextTokenEstimator>,
    ) -> Result<Self, ContractError> {
        let estimator_version = std::panic::catch_unwind(AssertUnwindSafe(|| estimator.version()))
            .map_err(|_| {
                source_error(
                    ErrorCode::InvalidContract,
                    "context_source.estimator_version",
                )
            })?;
        Ok(Self {
            store,
            policy,
            clock,
            ids,
            registry,
            estimator,
            estimator_version,
            binding_set_id: None,
        })
    }
    /// Pin the current scoped adapter segment for provider wrappers.
    pub fn with_binding_set_id(mut self, id: Id) -> Self {
        self.binding_set_id = Some(id);
        self
    }
    /// Exact registered namespace.
    pub fn scope(&self) -> &Scope {
        self.registry.scope()
    }
    /// Pure metadata plan using the previously cached estimator version.
    pub fn plan(&self, profile: &AgentProfile) -> Result<ContextSourcePlan, ContractError> {
        self.registry.plan(profile, &self.estimator_version)
    }
    async fn pinned_plan(
        &self,
        snapshot: &RunSnapshot,
        store: &dyn StateStore,
    ) -> Result<ContextSourcePlan, ContractError> {
        if &snapshot.scope != self.scope() {
            return Err(source_error(
                ErrorCode::AccessDenied,
                "context_source.scope",
            ));
        }
        let reference = snapshot.source_plan_ref.as_ref().ok_or_else(|| {
            source_error(ErrorCode::InvalidSnapshot, "context_source.plan_missing")
        })?;
        let record = store.read_record(&snapshot.scope, reference).await?;
        if record.reference() != reference {
            return Err(source_error(
                ErrorCode::InvalidSnapshot,
                "context_source.plan_record",
            ));
        }
        let plan = ContextSourcePlan::restore(
            &record.value().to_string(),
            self.scope(),
            &reference.digest,
        )?;
        if plan != self.plan(snapshot.profile.profile())? {
            return Err(source_error(
                ErrorCode::ProfileMismatch,
                "context_source.plan",
            ));
        }
        Ok(plan)
    }
    /// Collect selected sources in profile order, reusing the exact committed query.
    /// Every reply, including empty/unavailable, replaces its active slot atomically.
    pub async fn collect(
        &self,
        trigger: ContextTrigger,
        step: Option<Id>,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<Vec<ContextBatch>, ContractError> {
        self.check_scope(context)?;
        if budget.scope() != self.scope()
            || (trigger == ContextTrigger::BeforeModel) != step.is_some()
        {
            return Err(source_error(
                ErrorCode::InvalidContract,
                "context_source.trigger",
            ));
        }
        budget.check_boundary().await?;
        let saved = bounded(
            context,
            Some(budget),
            budget.call_deadline()?,
            budget.store().load(self.scope(), budget.run_id()),
        )
        .await?;
        if step.is_some() && saved.snapshot.model_step_id != step {
            return Err(source_error(
                ErrorCode::InvalidSnapshot,
                "context_source.step",
            ));
        }
        let plan = bounded(
            context,
            Some(budget),
            budget.call_deadline()?,
            self.pinned_plan(&saved.snapshot, budget.store().as_ref()),
        )
        .await?;
        let mut batches = Vec::new();
        for entry in plan
            .bindings()
            .iter()
            .filter(|entry| entry.binding.trigger == trigger)
        {
            let request = ContextRequest::for_run(&saved.snapshot, entry, step.clone())?;
            let current = bounded(
                context,
                Some(budget),
                budget.call_deadline()?,
                budget.store().load(self.scope(), budget.run_id()),
            )
            .await?;
            if let Some(state) = current.snapshot.source_states.iter().find(|state| {
                state.source == entry.binding.source
                    && state.trigger == trigger
                    && state.model_step_id == step
            }) {
                let record = bounded(
                    context,
                    Some(budget),
                    budget.call_deadline()?,
                    budget.store().read_record(self.scope(), &state.batch_ref),
                )
                .await?;
                let batch = ContextBatch::restore(&record, &plan, self.scope(), budget.run_id())?;
                if batch.request() != &request
                    || state.context_request_id != request.context_request_id
                {
                    return Err(source_error(
                        ErrorCode::InvalidSnapshot,
                        "context_source.saved_query",
                    ));
                }
                required_available(&batch)?;
                batches.push(batch);
                continue;
            }
            // A committed query cannot be queried again merely by dropping its slot.
            let mut history = Vec::new();
            for reference in &current.snapshot.context_batches {
                let record = bounded(
                    context,
                    Some(budget),
                    budget.call_deadline()?,
                    budget.store().read_record(self.scope(), reference),
                )
                .await?;
                let prior = ContextBatch::restore(&record, &plan, self.scope(), budget.run_id())?;
                if prior.request.context_request_id == request.context_request_id {
                    return Err(source_error(
                        ErrorCode::InvalidSnapshot,
                        "context_source.missing_slot",
                    ));
                }
                history.push(prior);
            }
            let result = self.provide(&request, context, budget).await?;
            budget.check_boundary().await?;
            let batch = ContextBatch::new(
                self.ids.next_id()?,
                request,
                result,
                self.clock.now()?.utc_ms,
                self.estimator_version.clone(),
                self.estimator.as_ref(),
                &history,
            )?;
            budget.check_boundary().await?;
            let mut snapshot = bounded(
                context,
                Some(budget),
                budget.call_deadline()?,
                budget.store().load(self.scope(), budget.run_id()),
            )
            .await?
            .snapshot;
            if snapshot
                .source_states
                .iter()
                .any(|state| state.context_request_id == batch.request.context_request_id)
            {
                return Err(source_error(
                    ErrorCode::RevisionConflict,
                    "context_source.query",
                ));
            }
            let expected_revision = snapshot.revision;
            let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
            snapshot.revision = snapshot.revision.checked_add(1).ok_or_else(|| {
                source_error(ErrorCode::RevisionConflict, "context_source.revision")
            })?;
            snapshot.usage.elapsed_ms = elapsed;
            snapshot.timing.last_observed_at_ms = now;
            snapshot.context_batches.push(batch.reference());
            snapshot
                .source_states
                .retain(|state| state.source != entry.binding.source || state.trigger != trigger);
            snapshot.source_states.push(SourceExecutionState {
                source: entry.binding.source.clone(),
                context_request_id: batch.request.context_request_id.clone(),
                trigger,
                model_step_id: step.clone(),
                batch_ref: batch.reference(),
            });
            bounded(
                context,
                Some(budget),
                budget.call_deadline()?,
                budget.store().commit(
                    self.scope(),
                    budget.run_id(),
                    CommitInput {
                        control_commands: vec![],
                        expected_revision,
                        lease: budget.lease().clone(),
                        now_ms: now,
                        snapshot,
                        messages: vec![],
                        events: vec![],
                        records: vec![batch.to_record()],
                    },
                ),
            )
            .await?;
            required_available(&batch)?;
            batches.push(batch);
        }
        Ok(batches)
    }
    /// Authorize the exact active batches for local preparation or a physical
    /// model dispatch. This never re-fetches or replaces provider items.
    pub async fn authorize_use(
        &self,
        run_id: &Id,
        batch_refs: &[RecordRef],
        route: Option<&ResolvedModelRoute>,
        context: &ExecutionContext,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<ContextItem>, ContractError> {
        self.check_scope(context)?;
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
        let active: Vec<_> = saved
            .snapshot
            .source_states
            .iter()
            .filter(|state| {
                state.trigger == ContextTrigger::RunStart
                    || state.model_step_id == saved.snapshot.model_step_id
            })
            .map(|state| &state.batch_ref)
            .collect();
        if active.len() != batch_refs.len()
            || active.iter().any(|reference| {
                batch_refs
                    .iter()
                    .filter(|candidate| *candidate == *reference)
                    .count()
                    != 1
            })
        {
            return Err(source_error(
                ErrorCode::InvalidSnapshot,
                "context_source.active_refs",
            ));
        }
        let mut items = vec![];
        for reference in batch_refs {
            let record = bounded(
                context,
                None,
                deadline,
                self.store.read_record(self.scope(), reference),
            )
            .await?;
            let batch = ContextBatch::restore(&record, &plan, self.scope(), run_id)?;
            if batch.request.session_id != saved.snapshot.request.session_id
                || batch.request.user_input != saved.snapshot.request.input
                || batch.request.model_step_id.is_some()
                    && batch.request.model_step_id != saved.snapshot.model_step_id
            {
                return Err(source_error(
                    ErrorCode::InvalidSnapshot,
                    "context_source.batch_use",
                ));
            }
            required_available(&batch)?;
            crate::future::boxed(|| {
                self.check_deletions(&batch, &saved.snapshot, &plan, context, deadline)
            })
            .await?;
            let remaining = saved
                .snapshot
                .timing
                .deadline_at_ms
                .saturating_sub(self.clock.now()?.utc_ms)
                .max(0) as u64;
            let use_deadline = deadline.min(
                tokio::time::Instant::now()
                    + Duration::from_millis(batch.request.binding.timeout_ms.get().min(remaining)),
            );
            let policy = PolicyRequest {
                owner_scope: self.scope().clone(),
                resource_id: run_id.clone(),
                action: PolicyAction::UseSourceContext {
                    source: batch.request.binding.source.clone(),
                    definition_digest: batch.request.definition.digest(),
                    batch_ref: reference.clone(),
                    route: route.cloned().map(Box::new),
                },
            };
            self.authorize(&policy, context, None, use_deadline).await?;
            // Empty/unavailable data has nothing to authorize at the unavailable
            // provider. Current Host source/use permission is still mandatory.
            if !batch.items.is_empty() {
                let entry = self
                    .registry
                    .get(&batch.request.binding.source)
                    .ok_or_else(|| {
                        source_error(ErrorCode::ComponentUnavailable, "context_source.provider")
                    })?;
                let cancellation = context.cancellation.child_token();
                let _cancel = cancellation.clone().drop_guard();
                let call =
                    self.call_context(&batch.request, context, cancellation.clone(), use_deadline);
                let request = ContextUseRequest {
                    consumer_run_id: run_id.clone(),
                    derived: false,
                    item_revisions: if let ContextResult::Ready { item_revisions, .. } =
                        &batch.result
                    {
                        item_revisions.clone()
                    } else {
                        Default::default()
                    },
                    batch_ref: reference.clone(),
                    request: batch.request.clone(),
                    items: batch.result.items().to_vec(),
                    source_revision: batch.result.source_revision().cloned(),
                    route: route.cloned(),
                };
                let checked = bounded(context, None, use_deadline, async {
                    AssertUnwindSafe(async { entry.source.authorize_use(&request, &call).await })
                        .catch_unwind()
                        .await
                        .map_err(|_| {
                            source_error(ErrorCode::InvalidContract, "context_source.authorize_use")
                        })?
                        .map_err(|error| source_error(error.code, "context_source.authorize_use"))
                })
                .await;
                cancellation.cancel();
                checked?;
                let estimated = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    self.estimator.estimate(batch.items())
                }))
                .map_err(|_| source_error(ErrorCode::InvalidContract, "context_source.estimator"))?
                .map_err(|error| source_error(error.code, "context_source.estimator"))?;
                if estimated != batch.estimated_tokens
                    || estimated > batch.request.binding.max_tokens.get()
                {
                    return Err(source_error(
                        ErrorCode::ContextBudgetExceeded,
                        "context_source.tokens",
                    ));
                }
            }
            if context.cancellation.is_cancelled() {
                return Err(source_error(ErrorCode::Cancelled, "context_source.use"));
            }
            if tokio::time::Instant::now() >= use_deadline {
                return Err(source_error(
                    ErrorCode::DeadlineExceeded,
                    "context_source.use",
                ));
            }
            if batch.fragments.is_empty() {
                items.extend(batch.items);
            } else {
                items.extend(select_context_fragments(&batch.fragments, self.scope())?);
            }
        }
        Ok(items)
    }
    // Deletion is source-native provenance, not a trigger-specific fragment
    // identity. Committed order also works for legacy batches without revisions.
    async fn check_deletions(
        &self,
        batch: &ContextBatch,
        observed: &RunSnapshot,
        plan: &ContextSourcePlan,
        context: &ExecutionContext,
        deadline: tokio::time::Instant,
    ) -> Result<(), ContractError> {
        if batch.result.items().is_empty() {
            return Ok(());
        }
        let start = if observed.run_id == batch.request.run_id {
            observed
                .context_batches
                .iter()
                .position(|reference| reference == &batch.reference())
                .ok_or_else(|| {
                    source_error(ErrorCode::InvalidSnapshot, "context_source.deletion_order")
                })?
                + 1
        } else {
            0
        };
        for reference in &observed.context_batches[start..] {
            let record = bounded(
                context,
                None,
                deadline,
                self.store.read_record(self.scope(), reference),
            )
            .await?;
            let later = ContextBatch::restore(&record, plan, self.scope(), &observed.run_id)?;
            if later.request.binding.source == batch.request.binding.source
                && later.request.definition == batch.request.definition
                && matches!(&later.result, ContextResult::Deleted { item_ids, .. }
                    if batch.result.items().iter().any(|item| item_ids.contains(&item.item_id)))
            {
                return Err(source_error(
                    ErrorCode::InvalidContext,
                    "context_source.withdrawn_lineage",
                ));
            }
        }
        Ok(())
    }
    fn check_scope(&self, context: &ExecutionContext) -> Result<(), ContractError> {
        if &context.data.scope != self.scope() {
            return Err(source_error(
                ErrorCode::AccessDenied,
                "context_source.scope",
            ));
        }
        Ok(())
    }
    fn call_context(
        &self,
        request: &ContextRequest,
        context: &ExecutionContext,
        cancellation: CancellationToken,
        deadline: tokio::time::Instant,
    ) -> ContextCallContext {
        ContextCallContext {
            scope: self.scope().clone(),
            run_id: request.run_id.clone(),
            session_id: request.session_id.clone(),
            source: request.binding.source.clone(),
            context_request_id: request.context_request_id.clone(),
            binding_set_id: self.binding_set_id.clone(),
            principal_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
            cancellation,
            deadline,
        }
    }
    async fn authorize(
        &self,
        request: &PolicyRequest,
        context: &ExecutionContext,
        budget: Option<&RunBudget>,
        deadline: tokio::time::Instant,
    ) -> Result<(), ContractError> {
        match bounded(
            context,
            budget,
            deadline,
            self.policy.check(request, context, Some(deadline), None),
        )
        .await?
        {
            PolicyDecision::Allow {} => Ok(()),
            PolicyDecision::Deny { .. } => Err(source_error(
                ErrorCode::AccessDenied,
                "context_source.policy",
            )),
            PolicyDecision::RequireApproval { .. } => Err(source_error(
                ErrorCode::ContextApprovalRequired,
                "context_source.policy",
            )),
        }
    }
    async fn provide(
        &self,
        request: &ContextRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<ContextResult, ContractError> {
        let deadline = (tokio::time::Instant::now()
            + Duration::from_millis(request.binding.timeout_ms.get()))
        .min(budget.call_deadline()?);
        let policy = PolicyRequest {
            owner_scope: self.scope().clone(),
            resource_id: request.run_id.clone(),
            action: PolicyAction::ProvideContext {
                source: request.binding.source.clone(),
                definition_digest: request.definition.digest(),
                context_request_id: request.context_request_id.clone(),
                trigger: request.binding.trigger,
                model_step_id: request.model_step_id.clone(),
                input_digest: request.input_digest(),
            },
        };
        self.authorize(&policy, context, Some(budget), deadline)
            .await?;
        budget.check_boundary().await?;
        let entry = self.registry.get(&request.binding.source).ok_or_else(|| {
            source_error(ErrorCode::ComponentUnavailable, "context_source.provider")
        })?;
        let cancellation = budget.cancellation().child_token();
        let _cancel = cancellation.clone().drop_guard();
        let call = self.call_context(request, context, cancellation.clone(), deadline);
        let callback =
            AssertUnwindSafe(async { entry.source.provide(request, &call).await }).catch_unwind();
        let result = tokio::select! {biased;
            _=context.cancellation.cancelled()=>Err(source_error(ErrorCode::Cancelled,"context_source.provide")),
            stopped=run_stopped(Some(budget))=>Err(stopped),
            _=tokio::time::sleep_until(deadline)=>Ok(unavailable("deadline_exceeded")?),
            result=callback=>match result{
                Ok(Ok(result))=>Ok(result),
                Ok(Err(error)) if error.code==ErrorCode::ContextSourceUnavailable=>Ok(unavailable("context_source_unavailable")?),
                Ok(Err(error))=>Err(source_error(error.code,"context_source.provide")),
                Err(_)=>Err(source_error(ErrorCode::InvalidContract,"context_source.provide")),
            },
        };
        cancellation.cancel();
        result
    }
}
fn required_available(batch: &ContextBatch) -> Result<(), ContractError> {
    if batch.request.binding.required && matches!(batch.result, ContextResult::Unavailable { .. }) {
        return Err(source_error(
            ErrorCode::ContextSourceUnavailable,
            "context_source.required",
        ));
    }
    Ok(())
}
fn unavailable(code: &str) -> Result<ContextResult, ContractError> {
    Ok(ContextResult::Unavailable {
        code: Id::new(code)?,
        source_revision: None,
        reported_usage: None,
    })
}
async fn run_stopped(budget: Option<&RunBudget>) -> ContractError {
    if let Some(budget) = budget {
        budget
            .wait_for_cancellation_or_deadline()
            .await
            .err()
            .unwrap_or_else(|| source_error(ErrorCode::DeadlineExceeded, "context_source.run"))
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
        _=context.cancellation.cancelled()=>Err(source_error(ErrorCode::Cancelled,"context_source.operation")),
        stopped=run_stopped(budget)=>Err(stopped),
        _=tokio::time::sleep_until(deadline)=>Err(source_error(ErrorCode::DeadlineExceeded,"context_source.operation")),
        result=operation=>result,
    }
}

impl ContextSourceRuntime {
    /// Resolve exact source dependencies of historical conversation, without fetching data.
    pub async fn lineage_for_messages(
        &self,
        session_id: &Id,
        messages: &[Message],
        context: &ExecutionContext,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<ContextLineage>, ContractError> {
        self.check_scope(context)?;
        let mut runs = std::collections::BTreeMap::new();
        let mut batches = Vec::new();
        for message in messages.iter().filter(|message| {
            matches!(
                message.visibility,
                Visibility::Model | Visibility::UserAndModel
            ) && matches!(message.origin, MessageOrigin::Model | MessageOrigin::Tool)
        }) {
            if runs.contains_key(&message.run_id) {
                continue;
            }
            let saved = bounded(
                context,
                None,
                deadline,
                self.store.load(self.scope(), &message.run_id),
            )
            .await?;
            if &saved.snapshot.request.session_id != session_id {
                return Err(source_error(
                    ErrorCode::AccessDenied,
                    "context_source.lineage_session",
                ));
            }
            if saved.snapshot.source_plan_ref.is_some() {
                let plan = bounded(
                    context,
                    None,
                    deadline,
                    self.pinned_plan(&saved.snapshot, self.store.as_ref()),
                )
                .await?;
                for reference in &saved.snapshot.context_batches {
                    let record = bounded(
                        context,
                        None,
                        deadline,
                        self.store.read_record(self.scope(), reference),
                    )
                    .await?;
                    batches.push(ContextBatch::restore(
                        &record,
                        &plan,
                        self.scope(),
                        &message.run_id,
                    )?);
                }
            }
            runs.insert(message.run_id.clone(), saved.snapshot);
        }
        crate::context_lineage::derive_lineage(
            messages,
            &runs.values().collect::<Vec<_>>(),
            &batches,
        )
    }
    /// Reauthorize original batch contents for a current consumer. Historical
    /// ownership and external versions are retained; no query or revision is invented.
    pub async fn authorize_lineage(
        &self,
        consumer_run_id: &Id,
        lineage: &[ContextLineage],
        route: Option<&ResolvedModelRoute>,
        context: &ExecutionContext,
        deadline: tokio::time::Instant,
    ) -> Result<(), ContractError> {
        self.check_scope(context)?;
        let consumer = bounded(
            context,
            None,
            deadline,
            self.store.load(self.scope(), consumer_run_id),
        )
        .await?;
        for dependency in lineage {
            let original = bounded(
                context,
                None,
                deadline,
                self.store.load(self.scope(), &dependency.run_id),
            )
            .await?;
            if original.snapshot.request.session_id != consumer.snapshot.request.session_id
                || !original
                    .snapshot
                    .context_batches
                    .contains(&dependency.batch_ref)
            {
                return Err(source_error(
                    ErrorCode::AccessDenied,
                    "context_source.lineage_owner",
                ));
            }
            let plan = bounded(
                context,
                None,
                deadline,
                self.pinned_plan(&original.snapshot, self.store.as_ref()),
            )
            .await?;
            let record = bounded(
                context,
                None,
                deadline,
                self.store.read_record(self.scope(), &dependency.batch_ref),
            )
            .await?;
            let batch = ContextBatch::restore(&record, &plan, self.scope(), &dependency.run_id)?;
            // User admission messages retain even failed Runs. Scan all later
            // observations in this session, including Runs with no model reply.
            let mut later_runs = Vec::new();
            let mut reached_origin = false;
            for message in &consumer.messages {
                reached_origin |= message.run_id == dependency.run_id;
                if reached_origin && !later_runs.contains(&message.run_id) {
                    later_runs.push(message.run_id.clone());
                }
            }
            if !later_runs.contains(&dependency.run_id) {
                later_runs.insert(0, dependency.run_id.clone());
            }
            if !later_runs.contains(consumer_run_id) {
                later_runs.push(consumer_run_id.clone());
            }
            for run_id in later_runs {
                let observed = bounded(
                    context,
                    None,
                    deadline,
                    self.store.load(self.scope(), &run_id),
                )
                .await?;
                if observed.snapshot.request.session_id != consumer.snapshot.request.session_id {
                    return Err(source_error(
                        ErrorCode::AccessDenied,
                        "context_source.lineage_session",
                    ));
                }
                if observed.snapshot.source_plan_ref.is_none() {
                    continue;
                }
                let observed_plan = bounded(
                    context,
                    None,
                    deadline,
                    self.pinned_plan(&observed.snapshot, self.store.as_ref()),
                )
                .await?;
                crate::future::boxed(|| {
                    self.check_deletions(
                        &batch,
                        &observed.snapshot,
                        &observed_plan,
                        context,
                        deadline,
                    )
                })
                .await?;
            }
            let use_deadline = deadline.min(
                tokio::time::Instant::now()
                    + Duration::from_millis(batch.request.binding.timeout_ms.get()),
            );
            self.authorize(
                &PolicyRequest {
                    owner_scope: self.scope().clone(),
                    resource_id: consumer_run_id.clone(),
                    action: PolicyAction::UseSourceContext {
                        source: batch.request.binding.source.clone(),
                        definition_digest: batch.request.definition.digest(),
                        batch_ref: dependency.batch_ref.clone(),
                        route: route.cloned().map(Box::new),
                    },
                },
                context,
                None,
                use_deadline,
            )
            .await?;
            if batch.items.is_empty() {
                continue;
            }
            let entry = self
                .registry
                .get(&batch.request.binding.source)
                .filter(|entry| entry.definition == batch.request.definition)
                .ok_or_else(|| {
                    source_error(
                        ErrorCode::ComponentUnavailable,
                        "context_source.lineage_provider",
                    )
                })?;
            let cancellation = context.cancellation.child_token();
            let _cancel = cancellation.clone().drop_guard();
            let mut call =
                self.call_context(&batch.request, context, cancellation.clone(), use_deadline);
            call.run_id = consumer_run_id.clone();
            let request = ContextUseRequest {
                consumer_run_id: consumer_run_id.clone(),
                derived: true,
                item_revisions: if let ContextResult::Ready { item_revisions, .. } = &batch.result {
                    item_revisions.clone()
                } else {
                    Default::default()
                },
                batch_ref: dependency.batch_ref.clone(),
                request: batch.request.clone(),
                items: batch.result.items().to_vec(),
                source_revision: batch.result.source_revision().cloned(),
                route: route.cloned(),
            };
            bounded(context, None, use_deadline, async {
                AssertUnwindSafe(async { entry.source.authorize_use(&request, &call).await })
                    .catch_unwind()
                    .await
                    .map_err(|_| {
                        source_error(ErrorCode::InvalidContract, "context_source.lineage_use")
                    })?
                    .map_err(|error| source_error(error.code, "context_source.lineage_use"))
            })
            .await?;
        }
        Ok(())
    }
}
