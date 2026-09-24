use super::*;
use std::panic::AssertUnwindSafe;

/// Owned per-segment registries. The facade's shared bindings never change.
pub(super) struct SegmentBindings {
    pub context: ExecutionContext,
    pub tools: Arc<ToolRegistry>,
    pub hooks: Option<Arc<HookRuntime>>,
    pub sources: Option<Arc<ContextSourceRuntime>>,
    pub owned: Option<BoundCapabilities>,
}
impl SegmentBindings {
    pub fn binding_set_id(&self) -> Option<&Id> {
        self.owned.as_ref().map(BoundCapabilities::binding_set_id)
    }
}

impl Agent {
    pub(super) fn direct_segment(
        &self,
        context: ExecutionContext,
    ) -> Result<SegmentBindings, ContractError> {
        Ok(SegmentBindings {
            context,
            tools: self
                .inner
                .bindings
                .tools
                .clone()
                .unwrap_or(Arc::new(ToolRegistry::new(
                    self.inner.bindings.scope.clone(),
                    vec![],
                )?)),
            hooks: self.inner.bindings.hooks.clone(),
            sources: self.inner.bindings.context_sources.clone(),
            owned: None,
        })
    }
    pub(super) async fn saved_assembly(
        &self,
        saved: &StoredRun,
    ) -> Result<Option<ResolvedAssembly>, ContractError> {
        let Some(reference) = &saved.snapshot.assembly_ref else {
            if self.inner.bindings.components.is_some() {
                return Err(fail(ErrorCode::InvalidSnapshot, "components.assembly"));
            }
            return Ok(None);
        };
        let bindings = &self.inner.bindings;
        let inputs = if let Some(reference) = &saved.snapshot.system_inputs {
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
        let record = bindings
            .state
            .read_record(&saved.snapshot.scope, reference)
            .await?;
        if record.reference() != reference {
            return Err(fail(ErrorCode::InvalidSnapshot, "components.assembly"));
        }
        let assembly = ResolvedAssembly::restore(
            &serde_json::to_string(record.value())
                .map_err(|_| fail(ErrorCode::InvalidJson, "components.assembly"))?,
            &saved.snapshot.profile,
            &inputs,
            &reference.digest,
        )?;
        if assembly.session_id() != &saved.snapshot.request.session_id {
            return Err(fail(ErrorCode::InvalidSnapshot, "components.session"));
        }
        Ok(Some(assembly))
    }
    pub(super) async fn metadata_segment(
        &self,
        saved: &StoredRun,
        context: ExecutionContext,
    ) -> Result<SegmentBindings, ContractError> {
        match caller_read(
            &context,
            Some(Duration::from_millis(
                self.inner.bindings.settings.start_timeout_ms,
            )),
            self.saved_assembly(saved),
        )
        .await?
        {
            Some(assembly) => Ok(SegmentBindings {
                context,
                tools: Arc::new(ToolRegistry::metadata(
                    saved.snapshot.scope.clone(),
                    assembly.tools().to_vec(),
                )?),
                hooks: None,
                sources: None,
                owned: None,
            }),
            None => self.direct_segment(context),
        }
    }
    pub(super) async fn bind_segment(
        &self,
        saved: &StoredRun,
        context: ExecutionContext,
        lease: Option<&RunLease>,
        purpose: ComponentBindPurpose,
        budget: Option<&RunBudget>,
        local: &LocalRun,
    ) -> Result<SegmentBindings, ContractError> {
        let Some(runtime) = &self.inner.bindings.components else {
            return self.direct_segment(context);
        };
        let assembly = caller_read(
            &context,
            Some(Duration::from_millis(
                self.inner.bindings.settings.start_timeout_ms,
            )),
            self.saved_assembly(saved),
        )
        .await?
        .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "components.assembly"))?;
        let bindings = &self.inner.bindings;
        if let Some(budget) = budget {
            budget.check_boundary().await?;
        }
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(bindings.settings.start_timeout_ms);
        let deadline = if let Some(budget) = budget {
            deadline.min(budget.call_deadline()?)
        } else {
            deadline
        };
        let bind_context = ComponentBindContext {
            scope: saved.snapshot.scope.clone(),
            run_id: saved.snapshot.run_id.clone(),
            session_id: saved.snapshot.request.session_id.clone(),
            binding_set_id: bindings.ids.next_id()?,
            principal_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
            lease: lease.cloned(),
            purpose,
            cancellation: context.cancellation.child_token(),
            deadline,
        };
        // The runtime retains staging ownership and rollback when this bounded
        // caller disconnects or drops its bind future.
        let _cancel = bind_context.cancellation.clone().drop_guard();
        let owned = caller_read(
            &context,
            Some(deadline.saturating_duration_since(tokio::time::Instant::now())),
            async {
                AssertUnwindSafe(runtime.bind(&assembly, &bind_context))
                    .catch_unwind()
                    .await
                    .map_err(|_| fail(ErrorCode::InvalidContract, "components.bind"))?
                    .map_err(|error| fail(error.code, "components.bind"))
            },
        )
        .await?;
        bind_context.cancellation.cancel();
        if owned.scope() != &saved.snapshot.scope
            || owned.run_id() != &saved.snapshot.run_id
            || owned.binding_set_id() != &bind_context.binding_set_id
        {
            self.release_bound(&owned, local).await;
            return Err(fail(ErrorCode::AccessDenied, "components.bound_identity"));
        }
        let contracts = (|| {
            let tools = owned
                .tools()
                .prompt_bindings(saved.snapshot.profile.profile())?;
            if tools.len() != assembly.tools().len()
                || tools
                    .iter()
                    .zip(assembly.tools())
                    .any(|(actual, expected)| {
                        actual.selection != expected.selection
                            || actual.compiled.digest() != expected.compiled.digest()
                            || actual.compiled.to_model_tool() != expected.compiled.to_model_tool()
                    })
            {
                return Err(fail(ErrorCode::ContextMismatch, "components.bound_tools"));
            }
            let expected =
                HookRegistry::metadata(saved.snapshot.scope.clone(), assembly.hooks().to_vec())?
                    .plan(saved.snapshot.profile.profile())?;
            if owned.hooks().plan(saved.snapshot.profile.profile())? != expected {
                return Err(fail(ErrorCode::ContextMismatch, "components.bound_hooks"));
            }
            Ok::<_, ContractError>(())
        })();
        if let Err(error) = contracts {
            self.release_bound(&owned, local).await;
            return Err(error);
        }
        let hook_runtime = HookRuntime::new(
            bindings.state.clone(),
            bindings.policy.clone(),
            bindings.clock.clone(),
            bindings.ids.clone(),
            owned.hooks().clone(),
        )
        .with_binding_set_id(owned.binding_set_id().clone());
        let sources = std::panic::catch_unwind(AssertUnwindSafe(|| -> Result<_, ContractError> {
            if assembly.sources().is_empty() {
                Ok(None)
            } else {
                let estimator = bindings
                    .context_token_estimator
                    .as_ref()
                    .ok_or_else(|| fail(ErrorCode::InvalidConfiguration, "agent.source_estimator"))?
                    .clone();
                let runtime = ContextSourceRuntime::new(
                    bindings.state.clone(),
                    bindings.policy.clone(),
                    bindings.clock.clone(),
                    bindings.ids.clone(),
                    owned.sources().clone(),
                    estimator,
                )?
                .with_binding_set_id(owned.binding_set_id().clone());
                let expected = saved
                    .snapshot
                    .source_plan_ref
                    .as_ref()
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "sources.plan"))?;
                if runtime.plan(saved.snapshot.profile.profile())?.digest() != expected.digest {
                    return Err(fail(ErrorCode::ContextMismatch, "components.bound_sources"));
                }
                Ok(Some(Arc::new(runtime)))
            }
        }))
        .unwrap_or_else(|_| {
            Err(fail(
                ErrorCode::InvalidContract,
                "components.source_runtime",
            ))
        });
        let sources = match sources {
            Ok(sources) => sources,
            Err(error) => {
                self.release_bound(&owned, local).await;
                return Err(error);
            }
        };
        Ok(SegmentBindings {
            context,
            tools: owned.tools().clone(),
            hooks: Some(Arc::new(hook_runtime)),
            sources,
            owned: Some(owned),
        })
    }
    pub(super) fn cleanup_deadline(
        &self,
        local: &LocalRun,
    ) -> Result<tokio::time::Instant, ContractError> {
        let mut deadline = local
            .cleanup_deadline
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "components.cleanup_deadline"))?;
        Ok(*deadline.get_or_insert_with(|| {
            tokio::time::Instant::now()
                + Duration::from_millis(self.inner.bindings.settings.cleanup_timeout_ms)
        }))
    }

    async fn release_capabilities(
        &self,
        owned: &BoundCapabilities,
    ) -> Result<ComponentReleaseReport, ContractError> {
        let context = ComponentReleaseContext {
            scope: owned.scope().clone(),
            run_id: owned.run_id().clone(),
            binding_set_id: owned.binding_set_id().clone(),
            cancellation: CancellationToken::new(),
            deadline: tokio::time::Instant::now()
                + Duration::from_millis(self.inner.bindings.settings.cleanup_timeout_ms),
        };
        let _cancel = context.cancellation.clone().drop_guard();
        tokio::time::timeout_at(
            context.deadline,
            AssertUnwindSafe(owned.release(&context)).catch_unwind(),
        )
        .await
        .map_err(|_| fail(ErrorCode::DeadlineExceeded, "components.release"))?
        .map_err(|_| fail(ErrorCode::InvalidContract, "components.release"))?
        .map_err(|error| fail(error.code, "components.release"))
    }
    pub(super) async fn release_segment(&self, segment: &SegmentBindings, local: &LocalRun) {
        let Some(owned) = &segment.owned else {
            return;
        };
        self.release_bound(owned, local).await;
    }
    async fn release_bound(&self, owned: &BoundCapabilities, local: &LocalRun) {
        match self.release_capabilities(owned).await {
            Ok(report) => {
                if let Ok(mut slot) = local.release_report.lock() {
                    match slot.as_mut() {
                        Some(previous) => previous.failures.extend(report.failures),
                        None => *slot = Some(report),
                    }
                }
            }
            Err(error) => {
                if let Ok(mut slot) = local.release_error.lock() {
                    *slot = Some(error);
                }
            }
        }
    }
    pub(super) fn keep_local(&self, local: &LocalRun) -> bool {
        local
            .observer_error
            .lock()
            .is_ok_and(|error| error.is_some())
            || local
                .release_error
                .lock()
                .is_ok_and(|error| error.is_some())
            || local.release_report.lock().is_ok_and(|report| {
                report
                    .as_ref()
                    .is_some_and(|report| !report.failures.is_empty())
            })
    }
    pub(super) async fn observe_pending(
        &self,
        run_id: &Id,
        segment: &SegmentBindings,
        local: &LocalRun,
    ) {
        let pending = local
            .pending_observations
            .lock()
            .map(|mut pending| std::mem::take(&mut *pending))
            .unwrap_or_default();
        for (target, input) in pending {
            self.remember_observer_error(
                local,
                self.after_tool(run_id, target, input, segment).await,
            );
        }
    }
    pub(super) async fn cleanup_observers(
        &self,
        saved: &StoredRun,
        context: &ExecutionContext,
        local: &LocalRun,
        observations: Vec<(HookTarget, HookInput)>,
    ) {
        let mut data = context.data.clone();
        data.system_inputs = None;
        let context = ExecutionContext::new(data, CancellationToken::new());
        if let Ok(mut pending) = local.pending_observations.lock() {
            pending.extend(observations);
        }
        let opened = self
            .bind_segment(
                saved,
                context,
                None,
                ComponentBindPurpose::ObserversOnly,
                None,
                local,
            )
            .await;
        let segment = match opened {
            Ok(segment) => segment,
            Err(error) => {
                if let Ok(mut slot) = local.release_error.lock() {
                    *slot = Some(fail(error.code, "components.observer_bind"));
                }
                return;
            }
        };
        let observed = AssertUnwindSafe(async {
            self.observe_pending(&saved.snapshot.run_id, &segment, local)
                .await;
            self.after_run(saved, &segment, local).await;
        })
        .catch_unwind()
        .await;
        if observed.is_err() {
            self.remember_observer_error(
                local,
                Some(fail(ErrorCode::InvalidContract, "components.observer")),
            );
        }
        self.release_segment(&segment, local).await;
    }
}
