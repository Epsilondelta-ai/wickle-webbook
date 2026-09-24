use futures_util::FutureExt;
use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use wickle::*;

pub(crate) struct SegmentLifetime {
    pub scope: Scope,
    pub run_id: Id,
    pub binding_set_id: Id,
    pub stopped: CancellationToken,
}
impl SegmentLifetime {
    fn check(&self, scope: &Scope, binding_set: Option<&Id>) -> Result<(), ContractError> {
        if scope != &self.scope || binding_set != Some(&self.binding_set_id) {
            return Err(ContractError::new(
                ErrorCode::AccessDenied,
                "adapter.segment",
            ));
        }
        if self.stopped.is_cancelled() {
            return Err(ContractError::new(
                ErrorCode::InvalidTransition,
                "adapter.released",
            ));
        }
        Ok(())
    }
}

pub(crate) struct ScopedTool {
    pub lifetime: Arc<SegmentLifetime>,
    pub executor: Arc<dyn ToolExecutor>,
}
impl ToolExecutor for ScopedTool {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.lifetime
                .check(&context.scope, context.binding_set_id.as_ref())?;
            if context.run_id != self.lifetime.run_id {
                return Err(ContractError::new(ErrorCode::AccessDenied, "adapter.run"));
            }
            let mut controlled = context.clone();
            controlled.cancellation = CancellationToken::new();
            let _cancel = controlled.cancellation.clone().drop_guard();
            tokio::select! { biased;
                _ = self.lifetime.stopped.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "adapter.released")),
                _ = context.cancellation.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "adapter.tool")),
                _ = tokio::time::sleep_until(context.deadline) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "adapter.tool")),
                result = self.executor.execute(args, &controlled) => result,
            }
        })
    }
    fn reconcile<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolReconciliation> {
        Box::pin(async move {
            self.lifetime
                .check(&context.scope, context.binding_set_id.as_ref())?;
            if context.run_id != self.lifetime.run_id {
                return Err(ContractError::new(ErrorCode::AccessDenied, "adapter.run"));
            }
            let mut controlled = context.clone();
            controlled.cancellation = CancellationToken::new();
            let _cancel = controlled.cancellation.clone().drop_guard();
            tokio::select! { biased;
                _ = self.lifetime.stopped.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "adapter.released")),
                _ = context.cancellation.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "adapter.reconcile")),
                _ = tokio::time::sleep_until(context.deadline) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "adapter.reconcile")),
                result = self.executor.reconcile(args, &controlled) => result,
            }
        })
    }
}

pub(crate) struct ScopedHook {
    pub lifetime: Arc<SegmentLifetime>,
    pub handler: Arc<dyn HookHandler>,
}
impl HookHandler for ScopedHook {
    fn call<'a>(
        &'a self,
        input: &'a HookInput,
        context: &'a HookContext,
    ) -> PortFuture<'a, HookOutput> {
        Box::pin(async move {
            self.lifetime
                .check(&context.scope, context.binding_set_id.as_ref())?;
            if context.run_id != self.lifetime.run_id {
                return Err(ContractError::new(ErrorCode::AccessDenied, "adapter.run"));
            }
            let mut controlled = context.clone();
            controlled.cancellation = CancellationToken::new();
            let _cancel = controlled.cancellation.clone().drop_guard();
            tokio::select! { biased;
                _ = self.lifetime.stopped.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "adapter.released")),
                _ = context.cancellation.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "adapter.hook")),
                _ = tokio::time::sleep_until(context.deadline) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "adapter.hook")),
                result = self.handler.call(input, &controlled) => result,
            }
        })
    }
}

pub(crate) struct ScopedSource {
    pub lifetime: Arc<SegmentLifetime>,
    pub selection: ContextSourceRef,
    pub definition: ContextSourceDefinition,
    pub source: Arc<dyn ContextSource>,
}
impl ScopedSource {
    fn check(
        &self,
        request: &ContextRequest,
        context: &ContextCallContext,
    ) -> Result<(), ContractError> {
        self.lifetime
            .check(&context.scope, context.binding_set_id.as_ref())?;
        if context.run_id != self.lifetime.run_id
            || request.run_id != context.run_id
            || request.scope != context.scope
            || request.session_id != context.session_id
            || context.source != self.selection
            || request.binding.source != self.selection
            || request.definition != self.definition
            || request.context_request_id != context.context_request_id
        {
            return Err(ContractError::new(
                ErrorCode::AccessDenied,
                "adapter.source",
            ));
        }
        Ok(())
    }
}
impl ContextSource for ScopedSource {
    fn provide<'a>(
        &'a self,
        request: &'a ContextRequest,
        context: &'a ContextCallContext,
    ) -> PortFuture<'a, ContextResult> {
        Box::pin(async move {
            self.check(request, context)?;
            let mut controlled = context.clone();
            controlled.cancellation = CancellationToken::new();
            let _cancel = controlled.cancellation.clone().drop_guard();
            tokio::select! { biased;
                _ = self.lifetime.stopped.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "adapter.released")),
                _ = context.cancellation.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "adapter.source")),
                _ = tokio::time::sleep_until(context.deadline) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "adapter.source")),
                result = self.source.provide(request, &controlled) => result,
            }
        })
    }
    fn authorize_use<'a>(
        &'a self,
        request: &'a ContextUseRequest,
        context: &'a ContextCallContext,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            if request.derived {
                self.lifetime
                    .check(&context.scope, context.binding_set_id.as_ref())?;
                if context.run_id != self.lifetime.run_id
                    || request.consumer_run_id != context.run_id
                    || request.request.scope != context.scope
                    || request.request.session_id != context.session_id
                    || context.source != self.selection
                    || request.request.binding.source != self.selection
                    || request.request.definition != self.definition
                    || request.request.context_request_id != context.context_request_id
                {
                    return Err(ContractError::new(
                        ErrorCode::AccessDenied,
                        "adapter.source_lineage",
                    ));
                }
            } else {
                if request.consumer_run_id != request.request.run_id {
                    return Err(ContractError::new(
                        ErrorCode::AccessDenied,
                        "adapter.source_consumer",
                    ));
                }
                self.check(&request.request, context)?;
            }
            let mut controlled = context.clone();
            controlled.cancellation = CancellationToken::new();
            let _cancel = controlled.cancellation.clone().drop_guard();
            tokio::select! { biased;
                _ = self.lifetime.stopped.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "adapter.released")),
                _ = context.cancellation.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "adapter.source")),
                _ = tokio::time::sleep_until(context.deadline) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "adapter.source")),
                result = self.source.authorize_use(request, &controlled) => result,
            }
        })
    }
}

struct ReleaseState {
    pending: Vec<(Id, Arc<dyn AdapterInstance>)>,
    report: ComponentReleaseReport,
}
pub(crate) struct ReleaseOwner {
    pub lifetime: Arc<SegmentLifetime>,
    close_timeout_ms: u64,
    state: Mutex<ReleaseState>,
}
impl ReleaseOwner {
    pub fn new(
        lifetime: Arc<SegmentLifetime>,
        instances: Vec<(Id, Arc<dyn AdapterInstance>)>,
        close_timeout_ms: u64,
    ) -> Self {
        Self {
            lifetime,
            close_timeout_ms,
            state: Mutex::new(ReleaseState {
                pending: instances,
                report: ComponentReleaseReport::default(),
            }),
        }
    }
}
impl ComponentRelease for ReleaseOwner {
    fn invalidate(&self) {
        self.lifetime.stopped.cancel();
    }

    fn release<'a>(
        &'a self,
        context: &'a ComponentReleaseContext,
    ) -> PortFuture<'a, ComponentReleaseReport> {
        Box::pin(async move {
            if context.scope != self.lifetime.scope
                || context.run_id != self.lifetime.run_id
                || context.binding_set_id != self.lifetime.binding_set_id
            {
                return Err(ContractError::new(
                    ErrorCode::AccessDenied,
                    "adapter.release_scope",
                ));
            }
            self.invalidate();
            let mut state = tokio::select! { biased;
                _ = context.cancellation.cancelled() => return Err(ContractError::new(ErrorCode::Cancelled, "adapter.release")),
                _ = tokio::time::sleep_until(context.deadline) => return Err(ContractError::new(ErrorCode::DeadlineExceeded, "adapter.release")),
                state = self.state.lock() => state,
            };
            while let Some((binding, instance)) = state.pending.last().cloned() {
                let deadline = (tokio::time::Instant::now()
                    + Duration::from_millis(self.close_timeout_ms))
                .min(context.deadline);
                let close = AdapterCloseContext {
                    scope: context.scope.clone(),
                    run_id: context.run_id.clone(),
                    binding_set_id: context.binding_set_id.clone(),
                    adapter_binding: binding.clone(),
                    cancellation: context.cancellation.child_token(),
                    deadline,
                };
                let _cancel = close.cancellation.clone().drop_guard();
                let operation =
                    AssertUnwindSafe(async { instance.close(&close).await }).catch_unwind();
                let result = tokio::select! { biased;
                    _ = context.cancellation.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "adapter.close")),
                    _ = tokio::time::sleep_until(deadline) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "adapter.close")),
                    result = operation => result.unwrap_or_else(|_| Err(ContractError::new(ErrorCode::InvalidContract, "adapter.close"))),
                };
                close.cancellation.cancel();
                state.pending.pop();
                if let Err(error) = result {
                    let code = serde_json::to_value(error.code)
                        .ok()
                        .and_then(|value| value.as_str().map(str::to_owned))
                        .unwrap_or_else(|| "invalid_contract".into());
                    state.report.failures.push(ComponentReleaseFailure {
                        adapter_binding: binding,
                        code: Id::new(code)?,
                    });
                }
            }
            Ok(state.report.clone())
        })
    }
}
