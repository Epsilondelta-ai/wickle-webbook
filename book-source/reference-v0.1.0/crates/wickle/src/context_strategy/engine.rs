use super::*;
use futures_util::FutureExt;
use std::{panic::AssertUnwindSafe, time::Duration};

pub(crate) struct ContextServices<'a> {
    pub bindings: &'a AgentBindings,
    pub budget: &'a RunBudget,
    pub context: &'a ExecutionContext,
}
pub(crate) struct PreparedContext {
    pub projection: ContextProjection,
    pub input_tokens: u64,
    pub artifacts: Vec<ArtifactRef>,
}
impl ContextRuntime {
    pub(super) async fn render(
        &self,
        prompt: &PromptSnapshot,
        seed: &ProjectionInput<'_>,
        view: &[Message],
        items: &[ContextItem],
        wide: bool,
        services: &ContextServices<'_>,
    ) -> Result<(ContextProjection, u64), ContractError> {
        let mut opaque = vec![];
        for message in view.iter().filter(|message| {
            matches!(
                message.visibility,
                Visibility::Model | Visibility::UserAndModel
            )
        }) {
            for block in &message.content {
                if let ContentBlock::ProviderOpaque {
                    provider,
                    route_digest,
                    data_ref,
                } = block
                {
                    if provider != &seed.route.provider || route_digest != &seed.route.digest() {
                        return Err(context_error(
                            ErrorCode::ModelContextIncompatible,
                            "context.opaque_route",
                        ));
                    }
                    let record = services
                        .bindings
                        .state
                        .read_record(&self.scope, data_ref)
                        .await?;
                    if record.reference() != data_ref {
                        return Err(context_error(
                            ErrorCode::ModelContextIncompatible,
                            "context.opaque_record",
                        ));
                    }
                    let continuation: OpaqueContinuation =
                        serde_json::from_value(record.value().clone()).map_err(|_| {
                            context_error(
                                ErrorCode::ModelContextIncompatible,
                                "context.opaque_record",
                            )
                        })?;
                    if continuation.route_digest() != route_digest {
                        return Err(context_error(
                            ErrorCode::ModelContextIncompatible,
                            "context.opaque_route",
                        ));
                    }
                    opaque.push(ScopedOpaque {
                        scope: self.scope.clone(),
                        reference: data_ref.clone(),
                        provider: provider.clone(),
                        continuation,
                    });
                }
            }
        }
        let mut input = seed.clone();
        input.limits.max_bytes = input
            .limits
            .max_bytes
            .min(input.response_limits.max_input_bytes);
        input.transcript = view;
        input.context_items = items;
        input.opaque_records = &opaque;
        if wide {
            input.limits = ProjectionLimits {
                max_bytes: self.limits.max_prepared_bytes,
                max_items: self.limits.max_prepared_bytes,
            };
            input.response_limits.max_input_bytes = self.limits.max_prepared_bytes;
        }
        let mut projection = ContextAssembler::new().project(prompt, input)?;
        let tokens = services
            .bindings
            .token_estimator
            .estimate(&projection.request)?;
        // Temporary preparation bounds are not counted as a compression gain.
        projection.request.limits = seed.response_limits.clone();
        Ok((projection, tokens))
    }
    pub(super) fn token_fit(
        &self,
        request: &ModelRequest,
        tokens: u64,
        services: &ContextServices<'_>,
    ) -> Result<bool, ContractError> {
        let catalog = services.bindings.router.snapshot().catalog();
        let binding = catalog
            .bindings
            .iter()
            .find(|binding| binding.binding == request.route.binding)
            .ok_or_else(|| context_error(ErrorCode::ModelNotRegistered, "context.route"))?;
        Ok(tokens
            <= binding
                .capabilities
                .context_window
                .get()
                .saturating_sub(request.max_output_tokens.get()))
    }
    pub(super) fn size(request: &ModelRequest) -> Result<u64, ContractError> {
        Ok(serde_json::to_vec(request)
            .map_err(|_| context_error(ErrorCode::InvalidJson, "context.request"))?
            .len() as u64)
    }
    async fn active(
        &self,
        saved: &StoredRun,
        services: &ContextServices<'_>,
    ) -> Result<(ContextPlan, Option<ContextRevision>), ContractError> {
        let Some(reference) = saved.snapshot.context_plan_ref.as_ref() else {
            if saved.snapshot.context_revision_ref.is_some()
                || !saved.snapshot.context_decisions.is_empty()
            {
                return Err(context_error(
                    ErrorCode::InvalidSnapshot,
                    "context.plan_missing",
                ));
            }
            return Ok((
                self.plan(saved.snapshot.profile.profile(), &self.scope)?,
                None,
            ));
        };
        let record = services
            .bindings
            .state
            .read_record(&self.scope, reference)
            .await?;
        let plan = ContextPlan::restore(&record, &saved.snapshot.profile)?;
        if plan != self.plan(saved.snapshot.profile.profile(), &self.scope)? {
            return Err(context_error(ErrorCode::ContextMismatch, "context.runtime"));
        }
        let revision = if let Some(reference) = &saved.snapshot.context_revision_ref {
            let record = services
                .bindings
                .state
                .read_record(&self.scope, reference)
                .await?;
            Some(ContextRevision::restore(
                &record,
                &plan,
                &self.scope,
                &saved.snapshot.request.session_id,
                &saved.messages,
            )?)
        } else {
            None
        };
        Ok((plan, revision))
    }
    pub(super) fn items(
        base: &[ContextItem],
        revision: Option<&ContextRevision>,
        plan: &ContextPlan,
    ) -> Result<Vec<ContextItem>, ContractError> {
        let mut items = base.to_vec();
        if let Some(revision) = revision {
            items.extend(revision.item(plan)?);
        }
        Ok(items)
    }
    pub(super) fn artifact_refs(
        projection: &ContextProjection,
        revision: Option<&ContextRevision>,
        plan: &ContextPlan,
    ) -> Result<Vec<ArtifactRef>, ContractError> {
        let mut references = vec![];
        if let Some(revision) = revision {
            for preview in &revision.previews {
                if projection
                    .selected_message_ids
                    .contains(&preview.message_id)
                    && !references.contains(&preview.preview.reference)
                {
                    references.push(preview.preview.reference.clone());
                }
            }
            if revision
                .item(plan)?
                .is_some_and(|item| projection.selected_context_ids.contains(&item.item_id))
            {
                for item in &revision.anchors {
                    if let InputContent::Artifact { reference } = item {
                        if !references.contains(reference) {
                            references.push(reference.clone());
                        }
                    }
                }
            }
        }
        Ok(references)
    }
    pub(crate) async fn prepare(
        &self,
        prompt: &PromptSnapshot,
        seed: ProjectionInput<'_>,
        services: ContextServices<'_>,
    ) -> Result<PreparedContext, ContractError> {
        services.budget.check_boundary().await?;
        let saved = services
            .bindings
            .state
            .load(&self.scope, services.budget.run_id())
            .await?;
        let (plan, previous) = self.active(&saved, &services).await?;
        let mut view = records::apply(&saved.messages, previous.as_ref())?;
        let mut items = Self::items(seed.context_items, previous.as_ref(), &plan)?;
        match self
            .render(prompt, &seed, &view, &items, false, &services)
            .boxed()
            .await
        {
            Ok((projection, input_tokens))
                if self.token_fit(&projection.request, input_tokens, &services)? =>
            {
                return Ok(PreparedContext {
                    artifacts: Self::artifact_refs(&projection, previous.as_ref(), &plan)?,
                    projection,
                    input_tokens,
                });
            }
            Ok(_) => {}
            Err(error) if error.code == ErrorCode::ContextBudgetExceeded => {}
            Err(error) => return Err(error),
        }
        if saved.snapshot.context_plan_ref.is_none() {
            return Err(context_error(
                ErrorCode::ContextBudgetExceeded,
                "context.legacy_without_plan",
            ));
        }
        let (before, before_tokens) = self
            .render(prompt, &seed, &view, &items, true, &services)
            .boxed()
            .await?;
        let before_bytes = Self::size(&before.request)?;
        let controls = ContextStrategyContext {
            principal_ref: services.context.data.principal_ref.clone(),
            capability_grant_ref: services.context.data.capability_grant_ref.clone(),
            cancellation: services.context.cancellation.child_token(),
            deadline: services
                .budget
                .call_deadline()?
                .min(tokio::time::Instant::now() + Duration::from_millis(self.limits.timeout_ms)),
        };
        let _cancel = controls.cancellation.clone().drop_guard();
        self.authorize(&saved, &seed.route, &controls, &services)
            .await?;
        let mut candidate = previous.clone().unwrap_or(ContextRevision {
            schema_version: "wickle.context-revision.v1".into(),
            scope: self.scope.clone(),
            session_id: saved.snapshot.request.session_id.clone(),
            run_id: saved.snapshot.run_id.clone(),
            model_step_id: seed.model_step_id.clone(),
            parent: saved.snapshot.context_revision_ref.clone(),
            plan_ref: saved
                .snapshot
                .context_plan_ref
                .clone()
                .expect("validated plan"),
            through_sequence: saved.session.transcript_revision,
            covered_message_ids: vec![],
            covered_digest: records::covered_digest(&saved.messages, &[]),
            summary: None,
            anchors: vec![],
            previews: vec![],
            before_bytes,
            after_bytes: before_bytes,
            before_tokens,
            after_tokens: before_tokens,
        });
        candidate.run_id = saved.snapshot.run_id.clone();
        candidate.model_step_id = seed.model_step_id.clone();
        candidate.parent = saved.snapshot.context_revision_ref.clone();
        candidate.plan_ref = saved
            .snapshot
            .context_plan_ref
            .clone()
            .expect("validated plan");
        candidate.through_sequence = saved.session.transcript_revision;
        candidate.before_bytes = before_bytes;
        candidate.before_tokens = before_tokens;
        self.preview(&saved, &mut candidate, &controls, &services)
            .await?;
        view = records::apply(&saved.messages, Some(&candidate))?;
        items = Self::items(seed.context_items, Some(&candidate), &plan)?;
        match self
            .render(prompt, &seed, &view, &items, false, &services)
            .boxed()
            .await
        {
            Ok((projection, tokens))
                if self.token_fit(&projection.request, tokens, &services)?
                    && Self::size(&projection.request)? < before_bytes
                    && tokens <= before_tokens =>
            {
                candidate.after_bytes = Self::size(&projection.request)?;
                candidate.after_tokens = tokens;
                self.authorize(&saved, &seed.route, &controls, &services)
                    .await?;
                self.commit(&saved, &candidate, None, &services).await?;
                return Ok(PreparedContext {
                    artifacts: Self::artifact_refs(&projection, Some(&candidate), &plan)?,
                    projection,
                    input_tokens: tokens,
                });
            }
            Ok(_) => {}
            Err(error) if error.code == ErrorCode::ContextBudgetExceeded => {}
            Err(error) => return Err(error),
        }
        self.compress(
            super::compression::Compression {
                prompt,
                seed: &seed,
                saved: &saved,
                plan: &plan,
                candidate,
                view: &view,
                controls: &controls,
            },
            &services,
        )
        .boxed()
        .await
    }
    pub(super) async fn authorize(
        &self,
        saved: &StoredRun,
        route: &ResolvedModelRoute,
        controls: &ContextStrategyContext,
        services: &ContextServices<'_>,
    ) -> Result<(), ContractError> {
        let request = PolicyRequest {
            owner_scope: self.scope.clone(),
            resource_id: saved.snapshot.run_id.clone(),
            action: PolicyAction::RewriteContext {
                strategy: self.definition.strategy.clone(),
                route: Box::new(route.clone()),
            },
        };
        match bounded(
            controls,
            services,
            services.bindings.policy.check(
                &request,
                services.context,
                Some(controls.deadline),
                None,
            ),
        )
        .await?
        {
            PolicyDecision::Allow {} => Ok(()),
            PolicyDecision::RequireApproval { .. } => Err(context_error(
                ErrorCode::ContextApprovalRequired,
                "context.policy",
            )),
            PolicyDecision::Deny { .. } => {
                Err(context_error(ErrorCode::AccessDenied, "context.policy"))
            }
        }
    }
}
pub(super) async fn bounded<T>(
    controls: &ContextStrategyContext,
    services: &ContextServices<'_>,
    future: impl std::future::Future<Output = Result<T, ContractError>>,
) -> Result<T, ContractError> {
    let result = tokio::select! {biased;
        _=controls.cancellation.cancelled()=>Err(context_error(ErrorCode::Cancelled,"context.operation")),
        stopped=services.budget.wait_for_cancellation_or_deadline()=>match stopped {Err(error)=>Err(error),Ok(())=>Err(context_error(ErrorCode::DeadlineExceeded,"context.operation"))},
        _=tokio::time::sleep_until(controls.deadline)=>Err(context_error(ErrorCode::DeadlineExceeded,"context.operation")),
        result=AssertUnwindSafe(future).catch_unwind()=>result.unwrap_or_else(|_|Err(context_error(ErrorCode::InvalidContract,"context.operation"))),
    };
    if result
        .as_ref()
        .is_err_and(|error| error.code == ErrorCode::DeadlineExceeded)
    {
        return Err(match services.budget.check_boundary().await {
            Err(error) => error,
            Ok(()) => context_error(
                ErrorCode::ContextCompactionFailed,
                "context.operation_timeout",
            ),
        });
    }
    result
}
