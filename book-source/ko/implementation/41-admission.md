# 41장 전체 구현과 변경 검사

[강의](../41-admission.md) · [전체 변경 패치](../solutions/41-admission.patch)

기준 `14ff40c42b8fd1e0da04cc70cb8de9bd872cee62`. 이 단계에서 바뀐 Rust·manifest·Python 파일의 전체 내용이다. 이전 버전과의 정확한 교체 위치·삭제는 patch를 따른다. 다음 장의 코드와 섞지 않는다.

## `crates/wickle/src/agent.rs`

```rust
use crate::*;
use futures_util::{FutureExt, stream};
use std::{
    collections::BTreeMap,
    fmt,
    num::NonZeroU64,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

mod admission;
mod artifacts;
mod components;
mod driver;
mod hooks;
mod persistence;
pub use persistence::{PersistenceFailure, UnconfirmedToolEffect};
mod recovery;
mod resume;
mod sources;
mod tools;
mod verification;
use components::SegmentBindings;

/// Host tokenizer or conservative estimator. This synchronous callback must not
/// perform I/O; returned tokens are estimates, not provider-reported usage.
pub trait ModelTokenEstimator: Send + Sync {
    /// Estimate the complete prepared request for its exact route.
    fn estimate(&self, request: &ModelRequest) -> Result<u64, ContractError>;
}

/// Finite runtime bounds, independent of the profile's total execution budgets.
#[derive(Debug, Clone)]
pub struct AgentSettings {
    /// Lease duration renewed by the detached driver.
    pub lease_ttl_ms: u64,
    /// Renewal interval; at most one third of the lease duration.
    pub heartbeat_interval_ms: u64,
    /// Maximum delay between durable observer polls.
    pub observer_poll_ms: u64,
    /// Maximum events read per page.
    pub event_page_size: usize,
    /// Deadline for admission preparation callbacks, before durable admission.
    pub start_timeout_ms: u64,
    /// Maximum serialized RunRequest bytes.
    pub max_request_bytes: usize,
    /// Reserved output-token limit for the initial text-model call.
    pub max_output_tokens: NonZeroU64,
    /// Model request and response bounds.
    pub response_limits: ModelResponseLimits,
    /// Context byte/item bounds, distinct from token estimates.
    pub projection_limits: ProjectionLimits,
    /// Per-tool callback and receipt limits; total attempts still use RunLimits.
    pub tool_execution_limits: ToolExecutionLimits,
    /// Require a durable StateStore at admission.
    pub require_durable: bool,
}
impl Default for AgentSettings {
    fn default() -> Self {
        Self {
            lease_ttl_ms: 30_000,
            heartbeat_interval_ms: 5_000,
            observer_poll_ms: 100,
            event_page_size: 64,
            start_timeout_ms: 30_000,
            max_request_bytes: 1_048_576,
            max_output_tokens: NonZeroU64::new(1024).expect("positive default"),
            response_limits: ModelResponseLimits {
                max_input_bytes: 1_048_576,
                max_response_bytes: 262_144,
                max_delta_bytes: 65_536,
                max_events: 4096,
                max_tool_calls: 16,
            },
            projection_limits: ProjectionLimits {
                max_bytes: 1_048_576,
                max_items: 1024,
            },
            tool_execution_limits: ToolExecutionLimits::default(),
            require_durable: false,
        }
    }
}
impl AgentSettings {
    /// Validate finite bounds without calling a runtime component.
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.lease_ttl_ms == 0
            || self.lease_ttl_ms > 86_400_000
            || self.heartbeat_interval_ms == 0
            || self.heartbeat_interval_ms > self.lease_ttl_ms / 3
            || self.observer_poll_ms == 0
            || self.observer_poll_ms > 60_000
            || self.start_timeout_ms == 0
            || self.start_timeout_ms > 86_400_000
            || self.event_page_size == 0
            || self.event_page_size > MAX_EVENT_PAGE_SIZE
            || self.max_request_bytes == 0
            || self.projection_limits.max_bytes == 0
            || self.projection_limits.max_items == 0
            || self.response_limits.max_input_bytes == 0
            || self.response_limits.max_response_bytes == 0
            || self.response_limits.max_delta_bytes == 0
            || self.response_limits.max_events == 0
            || self.tool_execution_limits.timeout_ms == 0
            || self.tool_execution_limits.timeout_ms > 86_400_000
            || self.tool_execution_limits.max_receipt_bytes == 0
        {
            return Err(fail(ErrorCode::InvalidConfiguration, "agent.settings"));
        }
        Ok(())
    }
}

/// Already-created Host components for one exact scope. Creating an Agent does
/// not invoke these ports, open connections, start tasks or read environment data.
pub struct AgentBindings {
    /// Fixed tenant/workspace/user namespace; validated against routing at start.
    pub scope: Scope,
    /// Durable or explicitly process-local state implementation.
    pub state: Arc<dyn StateStore>,
    /// Current authorization gate.
    pub policy: Arc<PolicyGate>,
    /// Approved profile metadata resolver, called only for new requests.
    pub profile_resolver: Arc<dyn ProfileResolver>,
    /// Configured model exchange with dispatcher and route inspector.
    pub model_exchange: Arc<ModelExchange>,
    /// Exact catalog and policy snapshot for newly admitted runs.
    pub router: Arc<dyn ModelRouter>,
    /// Trusted instructions pinned in the session prefix.
    pub host_instructions: Vec<String>,
    /// Registered system-input metadata; values arrive through ExecutionContext.
    pub system_inputs: SystemInputRegistry,
    /// Existing tool executors and compiled contracts, restricted to this scope.
    pub tools: Option<Arc<ToolRegistry>>,
    /// Optional read-only source for registered resolver-owned system inputs.
    pub system_input_resolver: Option<Arc<dyn SystemInputResolver>>,
    /// Optional read-only verifier for externally supplied effect receipts.
    pub external_receipt_verifier: Option<Arc<dyn ExternalReceiptVerifier>>,
    /// Optional scope-bound lifecycle runtime; selected definitions are pinned at admission.
    pub hooks: Option<Arc<HookRuntime>>,
    /// Optional component assembly/runtime. It owns all catalog and exported Tool/Hook/Source selections.
    /// Direct tools/hooks/context_sources cannot also be supplied when this is configured.
    pub components: Option<Arc<dyn ComponentRuntime>>,
    /// Directly supplied scoped context sources, used when components is None.
    pub context_sources: Option<Arc<ContextSourceRuntime>>,
    /// Versioned Host estimate for source items in component mode; never byte-as-token usage.
    pub context_token_estimator: Option<Arc<dyn ContextTokenEstimator>>,
    /// Exact Skill manifests and explicitly registered instruction loader.
    pub skills: Option<Arc<SkillRuntime>>,
    /// Scoped artifact access for Tool results and model-visible references.
    pub artifacts: Option<Arc<ArtifactRuntime>>,
    /// Optional bounded selector/compressor; omission uses bounded selection and previews only.
    pub context_runtime: Option<Arc<ContextRuntime>>,
    /// Output schemas and approved read-only verifiers.
    pub verification: Option<Arc<VerificationRuntime>>,
    /// Time source and timers.
    pub clock: Arc<dyn Clock>,
    /// New internal run/message/event identities, never business foreign keys.
    pub ids: Arc<dyn IdSource>,
    /// Route-specific token estimate callback.
    pub token_estimator: Arc<dyn ModelTokenEstimator>,
    /// Finite runtime limits.
    pub settings: AgentSettings,
}

/// Scope-bound Agent facade. Clone shares local driver ownership and observations.
#[derive(Clone)]
pub struct Agent {
    inner: Arc<Inner>,
}
struct Inner {
    profile: AgentProfile,
    bindings: AgentBindings,
    context: Arc<ContextRuntime>,
    verification: Arc<VerificationRuntime>,
    runs: Mutex<BTreeMap<Id, Arc<LocalRun>>>,
    observed: Arc<persistence::ObservedState>,
}
struct LocalRun {
    segment_start_revision: u64,
    cancel: CancellationToken,
    reason: Mutex<Option<Id>>,
    error: Mutex<Option<ContractError>>,
    observer_error: Mutex<Option<ContractError>>,
    release_report: Mutex<Option<ComponentReleaseReport>>,
    release_error: Mutex<Option<ContractError>>,
    pending_observations: Mutex<Vec<(HookTarget, HookInput)>>,
    done: AtomicBool,
    notify: Notify,
}
impl LocalRun {
    fn new(segment_start_revision: u64) -> Self {
        Self {
            segment_start_revision,
            cancel: CancellationToken::new(),
            reason: Mutex::new(None),
            error: Mutex::new(None),
            observer_error: Mutex::new(None),
            release_report: Mutex::new(None),
            release_error: Mutex::new(None),
            pending_observations: Mutex::new(vec![]),
            done: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }
}
impl fmt::Debug for Agent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Agent")
            .field("agent_id", &self.inner.profile.agent_id)
            .finish_non_exhaustive()
    }
}

/// Validate structural settings and binding scopes without invoking Host callbacks.
/// Profile requirements are resolved only when admitting a new request.
/// Tools, adapters, skills, context strategies, and verifiers use existing Host bindings.
/// Instruction asset loading and generic extension execution remain unsupported.
pub fn create_agent(
    profile: AgentProfile,
    mut bindings: AgentBindings,
) -> Result<Agent, ContractError> {
    profile.validate_structure()?;
    bindings.settings.validate()?;
    let context = match &bindings.context_runtime {
        Some(context) => context.clone(),
        None => Arc::new(ContextRuntime::bounded(bindings.scope.clone())?),
    };
    if context.scope() != &bindings.scope {
        return Err(fail(ErrorCode::AccessDenied, "agent.context_scope"));
    }
    let verification = match &bindings.verification {
        Some(runtime) => runtime.clone(),
        None => Arc::new(VerificationRuntime::text(bindings.scope.clone())?),
    };
    if verification.scope != bindings.scope {
        return Err(fail(ErrorCode::AccessDenied, "agent.verification_scope"));
    }
    if bindings
        .skills
        .as_ref()
        .is_some_and(|value| value.scope() != &bindings.scope)
        || bindings
            .tools
            .as_ref()
            .is_some_and(|value| value.scope() != &bindings.scope)
        || bindings
            .hooks
            .as_ref()
            .is_some_and(|value| value.scope() != &bindings.scope)
        || bindings
            .context_sources
            .as_ref()
            .is_some_and(|value| value.scope() != &bindings.scope)
    {
        return Err(fail(ErrorCode::AccessDenied, "agent.binding_scope"));
    }
    if bindings.components.is_some()
        && (bindings.tools.is_some()
            || bindings.hooks.is_some()
            || bindings.context_sources.is_some())
    {
        return Err(fail(
            ErrorCode::InvalidConfiguration,
            "agent.component_authority",
        ));
    }
    let observed = Arc::new(persistence::ObservedState::default());
    bindings.state = Arc::new(persistence::ObservedStore {
        inner: bindings.state.clone(),
        observed: observed.clone(),
    });
    Ok(Agent {
        inner: Arc::new(Inner {
            profile,
            bindings,
            context,
            verification,
            runs: Mutex::new(BTreeMap::new()),
            observed,
        }),
    })
}

/// A durable observer. Dropping this value or its streams does not cancel the driver.
#[derive(Clone)]
pub struct RunHandle {
    agent: Agent,
    run_id: Id,
    segment_start_revision: u64,
    local: Option<Arc<LocalRun>>,
}
impl fmt::Debug for RunHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunHandle")
            .field("run_id", &self.run_id)
            .finish_non_exhaustive()
    }
}

/// Result of an authorized cancellation request, separate from stored RunOutcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReceipt {
    /// Signalled this process's live driver. Cancellation is not yet committed.
    Requested,
    /// The saved run is already terminal; its outcome was not changed.
    AlreadyTerminal,
    /// No local driver is owned here. No remote cancellation was accepted or sent.
    NotLocal,
}

/// Protected observer reports and a local report-persistence failure, independent
/// of the saved execution outcome. An observer does not change business success.
#[derive(Debug)]
pub struct HookObservationView {
    /// Reports that the StateStore actually accepted.
    pub reports: Vec<HookObservation>,
    /// A local failure to persist an observer report, when this handle knows it.
    pub local_error: Option<ContractError>,
}

/// Local component cleanup information. It never replaces a stored RunOutcome.
#[derive(Debug)]
pub struct ComponentReleaseView {
    /// Completed release report for this handle's execution segment, when available.
    pub report: Option<ComponentReleaseReport>,
    /// Failure to finish the bounded release protocol, distinct from execution failure.
    pub local_error: Option<ContractError>,
}

impl Agent {
    /// Admit through an owned coordinator. Caller-future disconnection after polling
    /// does not abort durable admission or its detached driver.
    pub async fn start(
        &self,
        request: RunRequest,
        context: ExecutionContext,
    ) -> Result<Guarded<RunHandle>, ContractError> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| fail(ErrorCode::RuntimeUnavailable, "agent.runtime"))?;
        let agent = self.clone();
        runtime
            .spawn(async move { agent.admit(request, context).await })
            .await
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.coordinator"))?
    }
    /// Read minimal saved metadata under current permission.
    pub async fn get_run(
        &self,
        run_id: &Id,
        context: &ExecutionContext,
    ) -> Result<Guarded<RunView>, ContractError> {
        self.check_scope(context)?;
        let saved = caller_read(
            context,
            None,
            self.inner
                .bindings
                .state
                .load(&self.inner.bindings.scope, run_id),
        )
        .await?;
        self.inner
            .bindings
            .policy
            .run_view(&saved.snapshot, context, None)
            .await
    }
    /// Read protected saved state under the separate details permission.
    pub async fn get_run_details(
        &self,
        run_id: &Id,
        context: &ExecutionContext,
    ) -> Result<Guarded<RunSnapshot>, ContractError> {
        self.check_scope(context)?;
        let loaded = caller_read(
            context,
            None,
            self.inner
                .bindings
                .state
                .load(&self.inner.bindings.scope, run_id),
        )
        .await;
        let saved = match loaded {
            Ok(saved) => saved,
            Err(error) if error.code == ErrorCode::PersistenceUnavailable => {
                return self.persistence_error(run_id, context, error).await;
            }
            Err(error) => return Err(error),
        };
        self.inner
            .bindings
            .policy
            .run_details(&saved.snapshot, context, None)
            .await
    }
    /// Consume one authorized, fixed wait decision in an owned coordinator.
    /// A duplicate command returns its existing segment without restarting work.
    pub async fn resume(
        &self,
        command: ResumeCommand,
        context: ExecutionContext,
    ) -> Result<Guarded<RunHandle>, ContractError> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| fail(ErrorCode::RuntimeUnavailable, "agent.runtime"))?;
        self.check_scope(&context)?;
        let run_id = command.run_id.clone();
        let disclosure_context = context.clone();
        let agent = self.clone();
        let result = runtime
            .spawn(async move { agent.resume_command(command, context).await })
            .await
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.resume"))?;
        match result {
            Err(error) if error.code == ErrorCode::PersistenceUnavailable => {
                self.persistence_error(&run_id, &disclosure_context, error)
                    .await
            }
            other => other,
        }
    }
    async fn persistence_error<T>(
        &self,
        run_id: &Id,
        context: &ExecutionContext,
        error: ContractError,
    ) -> Result<Guarded<T>, ContractError> {
        self.check_scope(context)?;
        let request = PolicyRequest {
            owner_scope: self.inner.bindings.scope.clone(),
            resource_id: run_id.clone(),
            action: PolicyAction::ReadRunDetails {},
        };
        self.inner
            .bindings
            .policy
            .guard(&request, context, None, None, || async {
                Err(self.inner.observed.attach(run_id, error))
            })
            .await
    }
    fn check_scope(&self, context: &ExecutionContext) -> Result<(), ContractError> {
        if context.data.scope != self.inner.bindings.scope {
            return Err(fail(ErrorCode::AccessDenied, "scope"));
        }
        Ok(())
    }
    fn handle(&self, run_id: Id, segment_start_revision: u64) -> Result<RunHandle, ContractError> {
        let local = self
            .inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .get(&run_id)
            .filter(|local| local.segment_start_revision == segment_start_revision)
            .cloned();
        Ok(RunHandle {
            agent: self.clone(),
            run_id,
            segment_start_revision,
            local,
        })
    }
}

impl RunHandle {
    /// Inspect cleanup for this process's segment after current details authorization.
    pub async fn component_release(
        &self,
        context: &ExecutionContext,
    ) -> Result<Guarded<ComponentReleaseView>, ContractError> {
        self.agent.check_scope(context)?;
        let request = PolicyRequest {
            owner_scope: self.agent.inner.bindings.scope.clone(),
            resource_id: self.run_id.clone(),
            action: PolicyAction::ReadRunDetails {},
        };
        self.agent
            .inner
            .bindings
            .policy
            .guard(&request, context, None, None, || async {
                let local = self.current_local()?;
                let report = local
                    .as_ref()
                    .map(|local| {
                        local
                            .release_report
                            .lock()
                            .map(|report| report.clone())
                            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.release_state"))
                    })
                    .transpose()?
                    .flatten();
                let local_error = local
                    .as_ref()
                    .map(|local| {
                        local
                            .release_error
                            .lock()
                            .map(|error| error.clone())
                            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.release_state"))
                    })
                    .transpose()?
                    .flatten();
                Ok(ComponentReleaseView {
                    report,
                    local_error,
                })
            })
            .await
    }
    /// Read committed hook observations under current protected-details permission.
    pub async fn hook_observations(
        &self,
        context: &ExecutionContext,
    ) -> Result<Guarded<HookObservationView>, ContractError> {
        self.agent.check_scope(context)?;
        let bindings = &self.agent.inner.bindings;
        let request = PolicyRequest {
            owner_scope: bindings.scope.clone(),
            resource_id: self.run_id.clone(),
            action: PolicyAction::ReadRunDetails {},
        };
        if let Guarded::ApprovalRequired(challenge) = bindings
            .policy
            .guard(&request, context, None, None, || async { Ok(()) })
            .await?
        {
            return Ok(Guarded::ApprovalRequired(challenge));
        }
        let reports = caller_read(
            context,
            None,
            bindings
                .state
                .read_hook_observations(&bindings.scope, &self.run_id),
        )
        .await?;
        if reports
            .iter()
            .any(|report| report.scope != bindings.scope || report.run_id != self.run_id)
        {
            return Err(fail(
                ErrorCode::InvalidSnapshot,
                "agent.hook_observation_scope",
            ));
        }
        let local_error = self
            .current_local()?
            .map(|local| {
                local
                    .observer_error
                    .lock()
                    .map(|error| error.clone())
                    .map_err(|_| fail(ErrorCode::InvalidContract, "agent.observer_state"))
            })
            .transpose()?
            .flatten();
        bindings
            .policy
            .guard(&request, context, None, None, || async {
                Ok(HookObservationView {
                    reports,
                    local_error,
                })
            })
            .await
    }
    /// Stable saved run identity.
    pub fn run_id(&self) -> &Id {
        &self.run_id
    }
    /// Wait for an authorized saved outcome. Observer cancellation never cancels execution.
    pub async fn outcome(
        &self,
        context: &ExecutionContext,
    ) -> Result<Guarded<RunOutcome>, ContractError> {
        loop {
            let snapshot = match self.agent.get_run_details(&self.run_id, context).await? {
                Guarded::Completed(snapshot) => snapshot,
                Guarded::ApprovalRequired(challenge) => {
                    return Ok(Guarded::ApprovalRequired(challenge));
                }
            };
            if let Some(receipt) = snapshot.resume_receipts.iter().find(|receipt| {
                receipt.previous_segment_start_revision == self.segment_start_revision
            }) {
                let record = caller_read(
                    context,
                    None,
                    self.agent
                        .inner
                        .bindings
                        .state
                        .read_record(&snapshot.scope, &receipt.previous_outcome_ref),
                )
                .await?;
                if record.reference() != &receipt.previous_outcome_ref {
                    return Err(fail(ErrorCode::InvalidSnapshot, "agent.segment_reference"));
                }
                let outcome = serde_json::from_value(record.value().clone())
                    .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.segment_outcome"))?;
                let request = PolicyRequest {
                    owner_scope: snapshot.scope.clone(),
                    resource_id: self.run_id.clone(),
                    action: PolicyAction::ReadRunDetails {},
                };
                return self
                    .agent
                    .inner
                    .bindings
                    .policy
                    .guard(&request, context, None, None, || async { Ok(outcome) })
                    .await;
            }
            if snapshot.recovery_receipts.iter().any(|receipt| {
                receipt.previous_segment_start_revision == self.segment_start_revision
            }) {
                return Err(fail(ErrorCode::RevisionConflict, "agent.segment_recovered"));
            }
            if segment_revision(&snapshot) != self.segment_start_revision {
                return Err(fail(ErrorCode::InvalidSnapshot, "agent.segment"));
            }
            if let Some(outcome) = snapshot.outcome {
                return Ok(Guarded::Completed(outcome));
            }
            self.local_error()
                .map_err(|error| self.agent.inner.observed.attach(&self.run_id, error))?;
            self.wait(context).await?;
        }
    }
    /// Replay durable event metadata with fresh permission checks on every page
    /// and event. Polling has no channel backpressure on execution.
    pub fn events(
        &self,
        after_seq: u64,
        context: ExecutionContext,
    ) -> PortStream<'static, EventView> {
        let handle = self.clone();
        Box::pin(stream::try_unfold(
            (handle, context, after_seq, Vec::<RunEvent>::new()),
            |(handle, context, mut cursor, mut pending)| async move {
                loop {
                    if !pending.is_empty() {
                        let event = pending.remove(0);
                        let saved = caller_read(
                            &context,
                            None,
                            handle
                                .agent
                                .inner
                                .bindings
                                .state
                                .load(&handle.agent.inner.bindings.scope, &handle.run_id),
                        )
                        .await?;
                        if handle
                            .segment_end(&saved.snapshot)?
                            .is_some_and(|end| event.seq.get() > end)
                        {
                            return Ok(None);
                        }
                        let event = match handle
                            .agent
                            .inner
                            .bindings
                            .policy
                            .event_view(&event, &context, None)
                            .await?
                        {
                            Guarded::Completed(event) => event,
                            Guarded::ApprovalRequired(_) => {
                                return Err(fail(ErrorCode::AccessDenied, "agent.events.approval"));
                            }
                        };
                        cursor = event.seq.get();
                        return Ok(Some((event, (handle, context, cursor, pending))));
                    }
                    handle.agent.check_scope(&context)?;
                    let bindings = &handle.agent.inner.bindings;
                    let policy = PolicyRequest {
                        owner_scope: bindings.scope.clone(),
                        resource_id: handle.run_id.clone(),
                        action: PolicyAction::ReadEvents {},
                    };
                    match bindings
                        .policy
                        .guard(&policy, &context, None, None, || {
                            caller_read(
                                &context,
                                None,
                                bindings.state.read_events(
                                    &bindings.scope,
                                    &handle.run_id,
                                    cursor,
                                    bindings.settings.event_page_size,
                                ),
                            )
                        })
                        .await?
                    {
                        Guarded::ApprovalRequired(_) => {
                            return Err(fail(ErrorCode::AccessDenied, "agent.events.approval"));
                        }
                        Guarded::Completed(page) => {
                            pending = page.events;
                        }
                    }
                    if !pending.is_empty() {
                        continue;
                    }
                    let saved = caller_read(
                        &context,
                        None,
                        bindings.state.load(&bindings.scope, &handle.run_id),
                    )
                    .await?;
                    if let Some(end) = handle.segment_end(&saved.snapshot)? {
                        if cursor >= end {
                            return Ok(None);
                        }
                        continue;
                    }
                    handle.local_error()?;
                    handle.wait(&context).await?;
                }
            },
        ))
    }
    /// Signal only a locally owned driver after current CancelRun authorization.
    pub async fn cancel(
        &self,
        reason: Id,
        context: &ExecutionContext,
    ) -> Result<Guarded<CancelReceipt>, ContractError> {
        self.agent.check_scope(context)?;
        let bindings = &self.agent.inner.bindings;
        let saved = caller_read(
            context,
            None,
            bindings.state.load(&bindings.scope, &self.run_id),
        )
        .await?;
        let policy = PolicyRequest {
            owner_scope: saved.snapshot.scope,
            resource_id: self.run_id.clone(),
            action: PolicyAction::CancelRun {},
        };
        bindings
            .policy
            .guard(&policy, context, None, None, || async {
                if saved.snapshot.status.is_terminal() {
                    return Ok(CancelReceipt::AlreadyTerminal);
                }
                if saved.snapshot.status == RunStatus::Waiting {
                    return self
                        .agent
                        .cancel_waiting(self.run_id.clone(), reason, context.clone())
                        .await;
                }
                let current = self
                    .agent
                    .inner
                    .runs
                    .lock()
                    .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
                    .get(&self.run_id)
                    .cloned();
                if let Some(local) = current {
                    if !local.done.load(Ordering::Acquire) {
                        *local
                            .reason
                            .lock()
                            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.cancel"))? =
                            Some(reason);
                        local.cancel.cancel();
                        return Ok(CancelReceipt::Requested);
                    }
                }
                Ok(CancelReceipt::NotLocal)
            })
            .await
    }
    fn local_error(&self) -> Result<(), ContractError> {
        if let Some(local) = self.current_local()? {
            if local.done.load(Ordering::Acquire) {
                if let Some(error) = local
                    .error
                    .lock()
                    .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
                    .clone()
                {
                    return Err(error);
                }
            }
        }
        Ok(())
    }
    fn current_local(&self) -> Result<Option<Arc<LocalRun>>, ContractError> {
        Ok(self
            .agent
            .inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .get(&self.run_id)
            .filter(|local| local.segment_start_revision == self.segment_start_revision)
            .cloned()
            .or_else(|| self.local.clone()))
    }
    fn segment_end(&self, snapshot: &RunSnapshot) -> Result<Option<u64>, ContractError> {
        if let Some(receipt) = snapshot
            .resume_receipts
            .iter()
            .find(|receipt| receipt.previous_segment_start_revision == self.segment_start_revision)
        {
            return Ok(Some(receipt.previous_last_event_seq));
        }
        if let Some(receipt) = snapshot
            .recovery_receipts
            .iter()
            .find(|receipt| receipt.previous_segment_start_revision == self.segment_start_revision)
        {
            return Ok(Some(receipt.previous_last_event_seq));
        }
        if segment_revision(snapshot) != self.segment_start_revision {
            return Err(fail(ErrorCode::InvalidSnapshot, "agent.segment"));
        }
        Ok(
            (snapshot.status.is_terminal() || snapshot.status == RunStatus::Waiting)
                .then_some(snapshot.last_event_seq),
        )
    }
    async fn wait(&self, context: &ExecutionContext) -> Result<(), ContractError> {
        tokio::select! { biased;
            _ = context.cancellation.cancelled() => Err(fail(ErrorCode::Cancelled, "agent.observer")),
            _ = tokio::time::sleep(Duration::from_millis(self.agent.inner.bindings.settings.observer_poll_ms)) => Ok(()),
        }
    }
}

fn fail(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
fn segment_revision(snapshot: &RunSnapshot) -> u64 {
    snapshot
        .resume_receipts
        .last()
        .map_or(0, |receipt| receipt.accepted_revision)
        .max(
            snapshot
                .recovery_receipts
                .last()
                .map_or(0, |receipt| receipt.accepted_revision),
        )
}

// Only read-only caller operations use this helper. Cancelling a read drops its
// future without signalling the independent driver or cancelling a durable write.
async fn caller_read<T>(
    context: &ExecutionContext,
    timeout: Option<Duration>,
    future: impl std::future::Future<Output = Result<T, ContractError>>,
) -> Result<T, ContractError> {
    let deadline = async {
        match timeout {
            Some(timeout) => tokio::time::sleep(timeout).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::select! { biased;
        _ = context.cancellation.cancelled() => Err(fail(ErrorCode::Cancelled, "agent.read")),
        _ = deadline => Err(fail(ErrorCode::DeadlineExceeded, "agent.read")),
        result = future => result,
    }
}

impl Agent {
    pub(super) fn validate_new_configuration(&self) -> Result<(), ContractError> {
        let profile = &self.inner.profile;
        let bindings = &self.inner.bindings;
        let context = &self.inner.context;
        let verification = &self.inner.verification;
        if !matches!(profile.instructions, Instructions::Text(_))
            || (!profile.skills.is_empty() && bindings.skills.is_none())
            || (bindings.components.is_none()
                && (!profile.connectors.is_empty()
                    || profile.adapters.as_ref().is_some_and(|v| !v.is_empty())))
            || profile.extensions.as_ref().is_some_and(|v| !v.is_empty())
        {
            return Err(fail(ErrorCode::CapabilityUnsupported, "agent.profile"));
        }
        context.plan(profile, &bindings.scope)?;
        if verification.scope != bindings.scope {
            return Err(fail(ErrorCode::AccessDenied, "agent.verification_scope"));
        }
        verification.plan(profile, None)?;

        if bindings
            .skills
            .as_ref()
            .is_some_and(|skills| skills.scope() != &bindings.scope)
        {
            return Err(fail(ErrorCode::AccessDenied, "agent.skills_scope"));
        }
        if bindings.components.is_some()
            && (bindings.tools.is_some()
                || bindings.hooks.is_some()
                || bindings.context_sources.is_some())
        {
            return Err(fail(
                ErrorCode::InvalidConfiguration,
                "agent.component_authority",
            ));
        }
        if bindings.components.is_none() {
            match &bindings.context_sources {
                Some(sources) => {
                    if sources.scope() != &bindings.scope {
                        return Err(fail(ErrorCode::AccessDenied, "agent.sources_scope"));
                    }
                    sources.plan(profile)?;
                }
                None if profile
                    .context_sources
                    .as_ref()
                    .is_some_and(|sources| !sources.is_empty()) =>
                {
                    return Err(fail(ErrorCode::CapabilityUnsupported, "agent.sources"));
                }
                None => {}
            }
            match &bindings.hooks {
                Some(hooks) => {
                    if hooks.scope() != &bindings.scope {
                        return Err(fail(ErrorCode::AccessDenied, "agent.hooks_scope"));
                    }
                    hooks.plan(profile)?;
                }
                None if profile
                    .hooks
                    .as_ref()
                    .is_some_and(|hooks| !hooks.is_empty()) =>
                {
                    return Err(fail(ErrorCode::CapabilityUnsupported, "agent.hooks"));
                }
                None => {}
            }
            match &bindings.tools {
                Some(registry) => {
                    if registry.scope() != &bindings.scope {
                        return Err(fail(ErrorCode::AccessDenied, "agent.tools_scope"));
                    }
                    registry.prompt_bindings(profile)?;
                }
                None if !profile.tools.is_empty() => {
                    return Err(fail(ErrorCode::CapabilityUnsupported, "agent.tools"));
                }
                None => {}
            }
        }
        if bindings.components.is_some()
            && profile
                .context_sources
                .as_ref()
                .is_some_and(|sources| !sources.is_empty())
            && bindings.context_token_estimator.is_none()
        {
            return Err(fail(
                ErrorCode::InvalidConfiguration,
                "agent.source_estimator",
            ));
        }

        Ok(())
    }
}
```

## `crates/wickle/src/agent/admission.rs`

```rust
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
            let segment = segment_revision(&saved.snapshot);
            return Ok(Guarded::Completed(
                self.handle(saved.snapshot.run_id, segment)?,
            ));
        }
        if serde_json::to_vec(&request)
            .map_err(|_| fail(ErrorCode::InvalidJson, "agent.request"))?
            .len()
            > bindings.settings.max_request_bytes
        {
            return Err(fail(ErrorCode::InvalidContract, "agent.request_size"));
        }
        // The request contract exists before its purpose-specific routing integration.
        // Never silently accept an output cap that this driver cannot yet enforce.
        if request.max_output_tokens.is_some() {
            return Err(fail(
                ErrorCode::CapabilityUnsupported,
                "agent.request_output_cap",
            ));
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
            return Ok(Guarded::Completed(self.handle(
                result.state.snapshot.run_id.clone(),
                segment_revision(&result.state.snapshot),
            )?));
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
            let result =
                AssertUnwindSafe(agent.drive(&driver_id, prompt, driver_context, &driver_local))
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
        Ok(Guarded::Completed(RunHandle {
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
        let now = bindings.clock.now()?.utc_ms;
        let snapshot = RunSnapshot {
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
                submitted: Some(submitted),
                snapshot,
                prompt_snapshot: prompt_record.reference().clone(),
                require_durable: bindings.settings.require_durable,
                messages: vec![message],
                events: vec![event],
                records: [
                    vec![request_record, prompt_record, inputs_record, routing_record],
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
```

## `crates/wickle/src/context_strategy/runtime.rs`

```rust
use super::*;
use std::panic::AssertUnwindSafe;

impl ContextRewriteLimits {
    pub(super) fn validate(self) -> Result<(), ContractError> {
        if self.max_prepared_bytes == 0
            || self.max_prepared_bytes > 64 * 1024 * 1024
            || self.max_compactor_input_bytes == 0
            || self.max_compactor_input_bytes > self.max_prepared_bytes
            || self.max_summary_bytes == 0
            || self.max_summary_bytes > self.max_compactor_input_bytes
            || self.preview_above_bytes == 0
            || self.preview_above_bytes > self.max_prepared_bytes
            || self.max_previews > 128
            || self.max_compactions == 0
            || self.max_compactions > 64
            || self.timeout_ms == 0
            || self.timeout_ms > 86_400_000
        {
            return Err(context_error(
                ErrorCode::InvalidConfiguration,
                "context.limits",
            ));
        }
        Ok(())
    }
}
impl ContextRuntime {
    pub(crate) fn scope(&self) -> &Scope {
        &self.scope
    }

    pub(crate) fn validate_router(&self, routing: &RoutingSnapshot) -> Result<(), ContractError> {
        if routing.scope() != &self.scope {
            return Err(context_error(
                ErrorCode::AccessDenied,
                "context.routing_scope",
            ));
        }
        if let Some(ContextCompactor::Model(config)) = &self.compactor {
            if !routing.policy().rules.iter().any(|rule| {
                rule.model_binding == config.model_binding
                    && rule.purpose == ModelPurpose::Compaction
            }) {
                return Err(context_error(
                    ErrorCode::ModelRouteDenied,
                    "context.compaction_route",
                ));
            }
        }
        Ok(())
    }
    pub(crate) async fn validate_session_plan(
        &self,
        reference: &RecordRef,
        session: &Id,
        profile: &ResolvedProfile,
        current: &ContextPlan,
        state: &dyn StateStore,
    ) -> Result<(), ContractError> {
        let record = state.read_record(&self.scope, reference).await?;
        let revision: ContextRevision = serde_json::from_value(record.value().clone())
            .map_err(|_| context_error(ErrorCode::InvalidSnapshot, "context.revision"))?;
        if record.reference() != reference
            || revision.scope != self.scope
            || &revision.session_id != session
        {
            return Err(context_error(ErrorCode::InvalidSnapshot, "context.session"));
        }
        let record = state.read_record(&self.scope, &revision.plan_ref).await?;
        if ContextPlan::restore(&record, profile)?.digest() != current.digest() {
            return Err(context_error(
                ErrorCode::ContextMismatch,
                "context.session_plan",
            ));
        }
        Ok(())
    }
    /// Cache approved strategy metadata; this does not invoke selection, compression, or storage.
    pub fn new(
        scope: Scope,
        strategy: Arc<dyn ContextStrategy>,
        compactor: Option<ContextCompactor>,
        limits: ContextRewriteLimits,
    ) -> Result<Self, ContractError> {
        limits.validate()?;
        let definition = std::panic::catch_unwind(AssertUnwindSafe(|| strategy.definition()))
            .map_err(|_| context_error(ErrorCode::InvalidConfiguration, "context.strategy"))?;
        crate::tool_schema::compile_validator(&definition.config_schema)?;
        Ok(Self {
            scope,
            definition,
            strategy,
            compactor,
            limits,
        })
    }
    /// Default bounded selection and preview behavior without a compressor.
    pub fn bounded(scope: Scope) -> Result<Self, ContractError> {
        Self::new(
            scope,
            Arc::new(BoundedContextStrategy),
            None,
            ContextRewriteLimits::default(),
        )
    }
    /// Registered strategy metadata for a Host ProfileResolver when an explicit version is used.
    pub fn metadata(&self) -> ComponentMetadata {
        ComponentMetadata {
            reference: ComponentRef {
                kind: ComponentKind::ContextStrategy,
                id: self.definition.strategy.id.clone(),
                version: Some(self.definition.strategy.version.clone()),
            },
            contract_version: 1,
            manifest_digest: crate::serialization::data_digest(&self.definition),
            config_schema: self.definition.config_schema.clone(),
            dependencies: vec![],
            capabilities: Default::default(),
            required_capabilities: Default::default(),
            required_connections: Default::default(),
            model_name: None,
            hook_position: None,
            exports: vec![],
        }
    }
    pub(crate) fn plan(
        &self,
        profile: &AgentProfile,
        scope: &Scope,
    ) -> Result<ContextPlan, ContractError> {
        if scope != &self.scope {
            return Err(context_error(ErrorCode::AccessDenied, "context.scope"));
        }
        let plan = ContextPlan {
            schema_version: "wickle.context-plan.v1".into(),
            scope: scope.clone(),
            policy: profile.context_policy.clone(),
            strategy: self.definition.clone(),
            compactor: self.compactor.as_ref().map(|compactor| match compactor {
                ContextCompactor::Model(config) => CompactorIdentity::Model {
                    config: config.clone(),
                },
                ContextCompactor::Host { definition, .. } => CompactorIdentity::Host {
                    definition: definition.clone(),
                },
            }),
            limits: self.limits,
        };
        plan.validate(profile)?;
        Ok(plan)
    }
}
impl ContextPlan {
    /// Stable identity of the exact selector, compressor and limits.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
    pub(super) fn validate(&self, profile: &AgentProfile) -> Result<(), ContractError> {
        self.limits.validate()?;
        if self.schema_version != "wickle.context-plan.v1"
            || self.policy != profile.context_policy
            || self.policy.strategy != self.strategy.strategy.id
            || self.policy.version.as_ref().map_or(
                self.strategy.strategy.id.as_str() != "bounded"
                    || self.strategy.strategy.version.as_str() != "1",
                |version| version != &self.strategy.strategy.version,
            )
            || !crate::tool_schema::compile_validator(&self.strategy.config_schema)?.is_valid(
                &serde_json::to_value(self.policy.config.clone().unwrap_or_default())
                    .map_err(|_| context_error(ErrorCode::InvalidJson, "context.config"))?,
            )
        {
            return Err(context_error(
                ErrorCode::InvalidConfiguration,
                "context.plan",
            ));
        }
        Ok(())
    }
    /// Restore a protected plan against the owning profile and metadata version.
    pub fn restore(
        record: &ProtectedRecord,
        profile: &ResolvedProfile,
    ) -> Result<Self, ContractError> {
        let plan: Self = serde_json::from_value(record.value().clone())
            .map_err(|_| context_error(ErrorCode::InvalidSnapshot, "context.plan"))?;
        plan.validate(profile.profile())?;
        if &plan.scope != profile.scope() || plan.digest() != record.reference().digest {
            return Err(context_error(
                ErrorCode::InvalidSnapshot,
                "context.plan_identity",
            ));
        }
        if let Some(version) = &plan.policy.version {
            let metadata = ComponentMetadata {
                reference: ComponentRef {
                    kind: ComponentKind::ContextStrategy,
                    id: plan.policy.strategy.clone(),
                    version: Some(version.clone()),
                },
                contract_version: 1,
                manifest_digest: crate::serialization::data_digest(&plan.strategy),
                config_schema: plan.strategy.config_schema.clone(),
                dependencies: vec![],
                capabilities: Default::default(),
                required_capabilities: Default::default(),
                required_connections: Default::default(),
                model_name: None,
                hook_position: None,
                exports: vec![],
            };
            if !profile.components().iter().any(|component| {
                component.reference == metadata.reference
                    && component.definition_digest == crate::serialization::data_digest(&metadata)
            }) {
                return Err(context_error(
                    ErrorCode::ContextMismatch,
                    "context.strategy_metadata",
                ));
            }
        }
        Ok(plan)
    }
}
```

## `crates/wickle/src/execution_contracts.rs`

```rust
//! Persistable execution contracts. Transaction implementations and driver
//! integration use these records; declarations alone do not enable recovery.
use crate::{
    CanonicalizationVersion, ContractError, ErrorCode, Id, JsonDigest, JsonObject, JsonTextLimits,
    PortFuture, RecordRef, ResumeCommand, RunLease, RunOutcome, Scope, StoredRun, VersionedRef,
    canonicalize_json_text, versioned_digest_json,
};
use serde::{Deserialize, Serialize};
use std::{fmt, num::NonZeroU64};

/// Version of protected segment and prepared-step records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionRecordVersion {
    /// First execution-record contract; stored independently of legacy Run checkpoints.
    #[serde(rename = "wickle.execution-record.v1")]
    V1,
}

/// Original submitted data, kept separate from resolved defaults and runtime handles.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestSnapshot {
    schema_version: RequestSnapshotVersion,
    canonicalization: CanonicalizationVersion,
    /// Original submitted profile identity, not a newly resolved definition.
    pub profile_ref: VersionedRef,
    request_json: String,
    system_inputs_json: String,
    system_inputs_provided: bool,
    digest: JsonDigest,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum RequestSnapshotVersion {
    #[serde(rename = "wickle.request-snapshot.v1")]
    V1,
}
impl fmt::Debug for RequestSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestSnapshot")
            .field("canonicalization", &self.canonicalization)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}
impl RequestSnapshot {
    /// Capture validated caller JSON without resolving a catalog or binding.
    /// Start's absent system inputs become an empty object. Empty/omitted model options are normalized identically; other submitted
    /// fields retain their explicit presence. Request schema/authorization are the
    /// admission boundary's responsibility; capture alone does not admit a Run.
    pub fn capture(
        profile_ref: VersionedRef,
        request_json: &str,
        system_inputs_json: Option<&str>,
        limits: JsonTextLimits,
    ) -> Result<Self, ContractError> {
        let system_inputs_provided = system_inputs_json.is_some();
        let request = canonicalize_json_text(request_json, limits)?;
        let system = canonicalize_json_text(system_inputs_json.unwrap_or("{}"), limits)?;
        if request.first() != Some(&b'{') || system.first() != Some(&b'{') {
            return Err(ContractError::new(
                ErrorCode::InvalidContract,
                "request_snapshot.object",
            ));
        }
        let request_json = String::from_utf8(request)
            .map_err(|_| ContractError::new(ErrorCode::InvalidJson, "request_snapshot"))?;
        let system_inputs_json = String::from_utf8(system)
            .map_err(|_| ContractError::new(ErrorCode::InvalidJson, "request_snapshot"))?;
        let canonicalization = CanonicalizationVersion::WickleCanonicalJsonV1;
        let digest = Self::compute(
            &profile_ref,
            &request_json,
            &system_inputs_json,
            canonicalization,
            limits,
        )?;
        Ok(Self {
            schema_version: RequestSnapshotVersion::V1,
            canonicalization,
            profile_ref,
            request_json,
            system_inputs_json,
            system_inputs_provided,
            digest,
        })
    }
    fn compute(
        profile: &VersionedRef,
        request: &str,
        system: &str,
        version: CanonicalizationVersion,
        limits: JsonTextLimits,
    ) -> Result<JsonDigest, ContractError> {
        // Omitted and empty model option maps are the same submitted override.
        // RawValue keeps nested numeric tokens intact while removing only that key.
        let mut fields: std::collections::BTreeMap<String, Box<serde_json::value::RawValue>> =
            serde_json::from_str(request).map_err(|_| {
                ContractError::new(ErrorCode::InvalidJson, "request_snapshot.request")
            })?;
        if let Some(options) = fields.get("model_options") {
            if !options.get().starts_with('{') {
                return Err(ContractError::new(
                    ErrorCode::InvalidContract,
                    "request_snapshot.model_options",
                ));
            }
            if options.get() == "{}" {
                fields.remove("model_options");
            }
        }
        let request = serde_json::to_string(&fields)
            .map_err(|_| ContractError::new(ErrorCode::InvalidJson, "request_snapshot.request"))?;
        let profile = serde_json::to_string(profile)
            .map_err(|_| ContractError::new(ErrorCode::InvalidJson, "request_snapshot.profile"))?;
        let envelope = format!(
            "{{\"profile_ref\":{profile},\"request\":{request},\"system_inputs\":{system}}}"
        );
        versioned_digest_json(&envelope, version, limits)
    }
    /// Validate a stored record before comparison. Never trust a supplied digest.
    pub fn validate(&self, limits: JsonTextLimits) -> Result<(), ContractError> {
        let request = canonicalize_json_text(&self.request_json, limits)?;
        let system = canonicalize_json_text(&self.system_inputs_json, limits)?;
        if request.first() != Some(&b'{')
            || system.first() != Some(&b'{')
            || (!self.system_inputs_provided && system != b"{}")
            || Self::compute(
                &self.profile_ref,
                &self.request_json,
                &self.system_inputs_json,
                self.canonicalization,
                limits,
            )? != self.digest
        {
            return Err(ContractError::new(
                ErrorCode::InvalidSnapshot,
                "request_snapshot",
            ));
        }
        Ok(())
    }
    /// Compare another submission using this stored record's normalization and
    /// canonicalization rules, not the newer candidate's computed digest.
    pub fn matches_submission(
        &self,
        candidate: &Self,
        limits: JsonTextLimits,
    ) -> Result<bool, ContractError> {
        self.validate(limits)?;
        candidate.validate(limits)?;
        Ok(Self::compute(
            &candidate.profile_ref,
            &candidate.request_json,
            &candidate.system_inputs_json,
            self.canonicalization,
            limits,
        )? == self.digest)
    }
    /// Version that must be used when comparing a resubmission.
    pub fn canonicalization(&self) -> CanonicalizationVersion {
        self.canonicalization
    }
    /// Digest of the submitted profile, request and system input envelope.
    pub fn digest(&self) -> &JsonDigest {
        &self.digest
    }
    /// Privileged access to the submitted request JSON, retaining number lexemes.
    pub fn request_json(&self) -> &str {
        &self.request_json
    }
    /// Whether Start explicitly supplied system inputs. Omission still hashes as {}.
    pub fn system_inputs_provided(&self) -> bool {
        self.system_inputs_provided
    }
    /// Privileged access to protected system inputs. Never include in model context.
    pub fn system_inputs_json(&self) -> &str {
        &self.system_inputs_json
    }
}

/// Application-defined state, distinct from core execution status.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppState {
    /// Registered Host schema namespace.
    pub namespace: Id,
    /// Host-defined business status.
    pub status: Id,
    /// Validated against the registered Host schema before persistence.
    pub metadata: JsonObject,
}
impl fmt::Debug for AppState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppState")
            .field("namespace", &self.namespace)
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}
/// Why a segment stopped; protected causes cannot be overridden by app policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterruptionCause {
    /// Host is shutting down or handing ownership off.
    HostShutdown,
    /// Segment stop without a durable user cancellation.
    SegmentStopped,
    /// Explicit user cancellation.
    UserCancel,
    /// Run budget or deadline exhausted.
    BudgetExhausted,
    /// Ownership/fencing no longer valid.
    OwnershipLost,
    /// Snapshot cannot safely be recovered.
    RecoveryUnavailable,
}
/// Persisted evidence of a stopped execution segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InterruptionRecord {
    /// Segment whose execution stopped.
    pub segment_id: Id,
    /// Classified cause, not inferred from observer disconnection.
    pub cause: InterruptionCause,
    /// Last confirmed checkpoint revision.
    pub checkpoint_revision: u64,
    /// Whether a valid saved recovery path exists.
    pub recoverable: bool,
    /// Existing uncertain effects; policy must not remove these.
    pub unresolved_effects: Vec<RecordRef>,
}
/// Outcome for a particular segment. Resuming creates a different record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SegmentOutcome {
    /// Existing waiting or terminal outcome.
    Settled {
        /// Saved authoritative outcome.
        outcome: Box<RunOutcome>,
    },
    /// Recoverable stop, not a terminal Run outcome.
    Interrupted {
        /// Stop/recovery evidence.
        interruption: InterruptionRecord,
    },
}
/// Immutable identity and result of one accepted execution interval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSegment {
    /// Explicit protected-record schema version.
    pub schema_version: ExecutionRecordVersion,
    /// Original execution principal; a reviewer/control submitter cannot replace it.
    pub execution_principal_ref: Id,
    /// Run that owns this segment.
    pub run_id: Id,
    /// Never reused when the Run is resumed.
    pub segment_id: Id,
    /// Revision at acceptance.
    pub accepted_revision: u64,
    /// Present after waiting, interruption or terminal settlement.
    pub outcome: Option<SegmentOutcome>,
    /// App state snapshot at settlement; never defines core status.
    pub app_state: Option<AppState>,
}
/// Frozen prepared-model inputs. References belong to the same authorized scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedStepRecord {
    /// Explicit protected-record schema version.
    pub schema_version: ExecutionRecordVersion,
    /// Agent, verification or compaction invocation budget/policy purpose.
    pub purpose: crate::ModelPurpose,
    /// Protected compiled-provider contract records for this exact projection.
    pub compiled_tools: Vec<RecordRef>,
    /// Logical model step identity.
    pub model_step_id: Id,
    /// Changes when the model input changes, not on identical transport retries.
    pub projection_revision: NonZeroU64,
    /// Pinned route, effective options and their provenance.
    pub model_configuration: RecordRef,
    /// Pinned canonical tool set and compiled provider contracts.
    pub tool_set: RecordRef,
    /// Persisted assembled prompt/input projection.
    pub context_projection: RecordRef,
    /// Compiler and assembler contract identity.
    pub assembler: VersionedRef,
}
/// A durable control request. Submission is not a completed state transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlAction {
    /// Permanently cancel a Run, preserving known/unknown effects.
    Cancel {
        /// Auditable reason identifier.
        reason: Id,
    },
    /// Stop the active segment without declaring Run success.
    Stop {
        /// Why the Host stopped execution.
        cause: InterruptionCause,
    },
    /// Explicitly process a Run deadline that has elapsed.
    Expire,
}
/// Deduplicated control command, authenticated by the Host before submission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlCommand {
    /// Stable command identity within a Run.
    pub command_id: Id,
    /// Requesting principal, distinct from the original execution principal.
    pub principal_ref: Id,
    /// Authorized action.
    pub action: ControlAction,
}
/// Why a transaction creates or claims a segment.
#[derive(Debug, Clone)]
pub enum SegmentStart {
    /// Claim the segment already identified by admission.
    Initial,
    /// Consume a matching approval/input/recovery command.
    Resume(ResumeCommand),
    /// Consume an already persisted control command, without model/tool execution.
    Control(Id),
}
/// Inputs for atomic command consumption, lease acquisition and segment creation.
#[derive(Clone)]
pub struct BeginSegmentRequest {
    /// Proposed validated checkpoint/events for resume or control. Initial claim has None.
    pub transition: Option<SegmentTransition>,
    /// Owning Run.
    pub run_id: Id,
    /// CAS revision before this transaction.
    pub expected_revision: u64,
    /// Proposed new segment ID (initial claims use admission's ID).
    pub segment_id: Id,
    /// New execution owner.
    pub owner: Id,
    /// Trusted clock value.
    pub now_ms: i64,
    /// Bounded positive ownership TTL.
    pub lease_ttl_ms: NonZeroU64,
    /// Command or initial admission claim.
    pub start: SegmentStart,
}
/// Result of atomic segment acceptance; replay must not launch another driver.
#[derive(Clone)]
pub struct BeginSegmentResult {
    /// Saved execution state.
    pub state: StoredRun,
    /// Accepted or previously accepted segment.
    pub segment: ExecutionSegment,
    /// Only a new owner receives a lease. Replays have None.
    pub lease: Option<RunLease>,
}
/// Acknowledgment of durable command acceptance, not execution completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlReceipt {
    /// Owning Run.
    pub run_id: Id,
    /// Deduplicated command.
    pub command_id: Id,
    /// Segment that processed it, if already consumed.
    pub processed_segment_id: Option<Id>,
}
/// Atomic operations that storage implementations must supply before the new
/// segment driver can be enabled. No default read/then/write emulation is safe.
/// StateStore integration requires the same transaction as its snapshot/events.
pub trait ExecutionTransactions: Send + Sync {
    /// Read protected segment/command history without mutation; Host authorizes access.
    fn read_execution<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
    ) -> PortFuture<'a, ExecutionHistory>;

    /// Atomically deduplicate/consume the command, check CAS and allocate ownership.
    fn begin_segment<'a>(
        &'a self,
        scope: &'a Scope,
        request: BeginSegmentRequest,
    ) -> PortFuture<'a, BeginSegmentResult>;
    /// Atomically persist a command ID and payload; conflicting reuse is rejected.
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        command: ControlCommand,
    ) -> PortFuture<'a, ControlReceipt>;
}

/// Allowed callback proposals. None may declare success or overwrite effect evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterruptionAction {
    /// Use the core's cause-specific default.
    UseDefault,
    /// Save a recoverable interrupted segment when the core permits it.
    Pause,
    /// Cancel when the cause permits this transition.
    Cancel,
    /// Fail when the cause permits this transition.
    Fail,
}
/// Limited immutable data exposed to the application interruption policy.
#[derive(Debug, Clone)]
pub struct InterruptionInfo {
    /// Resource scope; this does not grant authorization.
    pub scope: Scope,
    /// Stopped Run identity.
    pub run_id: Id,
    /// Fixed interruption evidence.
    pub interruption: InterruptionRecord,
    /// Current app state without mutable access to the Run.
    pub app_state: Option<AppState>,
}
/// Policy result; core validation and persistence remain mandatory.
#[derive(Debug, Clone)]
pub struct InterruptionDecision {
    /// Suggested transition.
    pub action: InterruptionAction,
    /// Optional Host-schema-validated business state.
    pub app_state: Option<AppState>,
}
/// A versioned application policy with no state-store or tool execution handle.
pub trait InterruptionPolicy: Send + Sync {
    /// Immutable identity pinned at admission.
    fn identity(&self) -> VersionedRef;
    /// Cooperatively compute a proposal. The driver owns timeout and fallback.
    fn decide<'a>(&'a self, info: &'a InterruptionInfo) -> PortFuture<'a, InterruptionDecision>;
}

impl ExecutionSegment {
    /// Check structural settlement invariants before persistence; authorization,
    /// Host app-state schema and atomic ledger transitions remain store/driver checks.
    pub fn validate(&self) -> Result<(), ContractError> {
        let invalid = || ContractError::new(ErrorCode::InvalidSnapshot, "execution_segment");
        match &self.outcome {
            Some(SegmentOutcome::Settled { outcome }) => {
                outcome.validate()?;
                if outcome.checkpoint_revision < self.accepted_revision {
                    return Err(invalid());
                }
            }
            Some(SegmentOutcome::Interrupted { interruption })
                if interruption.segment_id != self.segment_id
                    || interruption.checkpoint_revision < self.accepted_revision
                    || !interruption.recoverable
                    || !matches!(
                        interruption.cause,
                        InterruptionCause::HostShutdown | InterruptionCause::SegmentStopped
                    ) =>
            {
                return Err(invalid());
            }
            Some(SegmentOutcome::Interrupted { .. }) => {}
            None => {}
        }
        Ok(())
    }
}

/// A proposed checkpoint delta; the transaction supplies its own validated lease.
#[derive(Clone)]
pub struct SegmentTransition {
    /// Next checkpoint at expected_revision + 1.
    pub snapshot: crate::RunSnapshot,
    /// New transcript messages.
    pub messages: Vec<crate::Message>,
    /// Matching durable events.
    pub events: Vec<crate::RunEvent>,
    /// Immutable records referenced by the new checkpoint/events.
    pub records: Vec<crate::ProtectedRecord>,
}
/// Persisted control command and its optional processing result.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredControlCommand {
    /// Original authenticated command payload.
    pub command: ControlCommand,
    /// Segment that consumed it; absent means pending, not failed.
    pub processed_segment_id: Option<Id>,
}
/// Command deduplication evidence bound to an accepted segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedSegmentCommand {
    /// Stable command identity.
    pub command_id: Id,
    /// Original normalized command payload digest.
    pub payload_digest: JsonDigest,
    /// Segment created by this command.
    pub segment_id: Id,
}
/// Protected execution history stored atomically with a Run's checkpoint/events.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionHistory {
    /// Owning Run.
    pub run_id: Id,
    /// Authenticated original execution actor.
    pub execution_principal_ref: Id,
    /// Submitted request evidence, separate from effective configuration.
    pub submitted: Option<RequestSnapshot>,
    /// Initial segment has been claimed at least once. Retrying a claim is not recovery.
    pub initial_claimed: bool,
    /// Ordered immutable past segments and the current segment.
    pub segments: Vec<ExecutionSegment>,
    /// Accepted resume/control command identities.
    pub accepted_commands: Vec<AcceptedSegmentCommand>,
    /// Pending and processed durable controls.
    pub controls: Vec<StoredControlCommand>,
}
impl fmt::Debug for ExecutionHistory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionHistory")
            .field("run_id", &self.run_id)
            .field("segment_count", &self.segments.len())
            .finish_non_exhaustive()
    }
}
```

## `crates/wickle/src/state.rs`

```rust
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroU64,
    sync::{Mutex, MutexGuard},
};

use serde::de::DeserializeOwned;
use serde_json::Value;

mod checkpoint;
mod context_state;
mod execution;
mod hook_state;
mod reconciliation_state;
mod recovery_state;
mod skill_state;
mod source_state;
mod verification_state;
pub use checkpoint::{STATE_STORE_CHECKPOINT_VERSION, StateStoreCheckpoint};
use hook_state::{validate_hook_observation, validate_hook_snapshot, validate_hook_transition};
use source_state::{validate_source_snapshot, validate_source_transition};

use crate::{
    ApprovalTarget, BudgetUsage, ContentBlock, ContractError, ErrorCode, Id, Message,
    ModelAttemptState, ModelExchangeOutcome, ModelFinish, ModelInvocationRecord, OutcomeResult,
    PortFuture, RecordRef, ResumeAction, ResumeCommand, RunEvent, RunEventPayload, RunPhase,
    RunSnapshot, RunStatus, Scope, SessionSchemaVersion, SessionSnapshot, StoredModelResponse,
    ToolCall, ToolCallState, ToolResult, VerificationSummary, WaitState, WaitTarget,
    admission_digest, canonical_digest,
};

/// Guarantees offered by a state-store implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateStoreCapabilities {
    /// Records survive process termination.
    pub durable: bool,
    /// Execution leases coordinate independent processes.
    pub cross_process_leases: bool,
    /// Committed events can be replayed in sequence order.
    pub event_replay: bool,
}

/// Immutable, scope-owned data stored with its referencing state and events.
/// Access requires Host authorization; Debug never prints the payload.
#[derive(Clone, PartialEq)]
pub struct ProtectedRecord {
    reference: RecordRef,
    value: Value,
}

impl ProtectedRecord {
    /// Compute the reference digest from owned data. A revision is immutable.
    pub fn new(record_id: Id, revision: u64, value: Value) -> Self {
        Self {
            reference: RecordRef {
                record_id,
                revision,
                digest: canonical_digest(&value),
            },
            value,
        }
    }

    /// Exact immutable record identity, without its payload.
    pub fn reference(&self) -> &RecordRef {
        &self.reference
    }

    /// Explicit privileged access, never an automatic public/model projection.
    pub fn value(&self) -> &Value {
        &self.value
    }
}

impl fmt::Debug for ProtectedRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtectedRecord")
            .field("reference", &self.reference)
            .field("value", &"<redacted>")
            .finish()
    }
}

/// Initial records accepted atomically for a newly admitted run.
#[derive(Clone)]
pub struct AdmissionInput {
    /// Authenticated original execution principal, not the latest reviewer.
    pub execution_principal_ref: Id,
    /// Original submitted inputs when supplied by the versioned admission path.
    pub submitted: Option<crate::RequestSnapshot>,
    /// Running/admission checkpoint at revision zero.
    pub snapshot: RunSnapshot,
    /// Session-pinned prompt record; reused unchanged by subsequent runs.
    pub prompt_snapshot: RecordRef,
    /// New messages, numbered consecutively across the session.
    pub messages: Vec<Message>,
    /// One run.started event at sequence one, referencing the accepted request.
    pub events: Vec<RunEvent>,
    /// New immutable records, available to references in this transaction.
    pub records: Vec<ProtectedRecord>,
    /// Reject implementations that cannot preserve state across process termination.
    pub require_durable: bool,
}

/// An owned protected checkpoint and its complete session transcript.
/// Use PolicyGate views to select data for less privileged callers.
#[derive(Clone, PartialEq)]
pub struct StoredRun {
    /// Current run checkpoint and protected record references.
    pub snapshot: RunSnapshot,
    /// Current session metadata, including its active run.
    pub session: SessionSnapshot,
    /// Append-only session transcript, including messages from earlier runs.
    pub messages: Vec<Message>,
}

impl fmt::Debug for StoredRun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredRun")
            .field("run_id", &self.snapshot.run_id)
            .field("revision", &self.snapshot.revision)
            .field("message_count", &self.messages.len())
            .finish_non_exhaustive()
    }
}

/// Admission reports whether it created a run or found the original request.
#[derive(Debug, Clone, PartialEq)]
pub struct AdmissionResult {
    /// False for identical request replay; candidate records are not applied.
    pub created: bool,
    /// Existing or newly admitted run, with its original pinned data.
    pub state: StoredRun,
}

/// Store-issued lease identity. Possession is not Host authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLease {
    /// Exact resource namespace.
    pub scope: Scope,
    /// Run owned by this lease.
    pub run_id: Id,
    /// Worker identity supplied by trusted runtime code.
    pub owner: Id,
    /// Increasing generation retained across expiration and release.
    pub fencing_token: u64,
    /// Expiration reported when issued. Validation uses the store's current expiry,
    /// so renewal does not invalidate copies of the same owner/fencing generation.
    pub expires_at_ms: i64,
}

/// A complete candidate checkpoint and append-only data for one atomic commit.
#[derive(Clone)]
pub struct CommitInput {
    /// Compare-and-swap revision of the currently saved checkpoint.
    pub expected_revision: u64,
    /// Current unexpired execution lease.
    pub lease: RunLease,
    /// Trusted current UTC milliseconds, also used to reject expired leases.
    pub now_ms: i64,
    /// Next checkpoint, at expected_revision + 1.
    pub snapshot: RunSnapshot,
    /// New messages, continuing the session sequence.
    pub messages: Vec<Message>,
    /// New events, continuing the run sequence.
    pub events: Vec<RunEvent>,
    /// Immutable records to insert in the same transaction.
    pub records: Vec<ProtectedRecord>,
}

/// A bounded, ordered page of protected durable events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventPage {
    /// Events strictly after the supplied cursor.
    pub events: Vec<RunEvent>,
    /// Cursor for the next page, unchanged for an empty page.
    pub next_after_seq: u64,
    /// More events were available when this page was read.
    pub has_more: bool,
    /// Oldest retained sequence; None when no events are stored.
    pub first_available_seq: Option<NonZeroU64>,
    /// Latest committed sequence when this page was read.
    pub last_available_seq: u64,
}

/// Largest event page accepted by the reference store.
pub const MAX_EVENT_PAGE_SIZE: usize = 1_000;

/// Trusted core storage port. Scope isolation is enforced by the store itself.
/// The facade separately applies current PolicyGate authorization. No raw load or
/// record reference grants permission to publish the returned data.
pub trait StateStore: crate::ExecutionTransactions + Send + Sync {
    /// Describe storage and coordination guarantees.
    fn capabilities(&self) -> StateStoreCapabilities;
    /// Find the original request before re-resolving current profile or routing metadata.
    /// Missing scope/request returns None. Atomic admission remains the final deduplication boundary.
    fn find_request<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
        request_id: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>>;
    /// Atomically deduplicate a request and reserve its session's active-run slot.
    fn admit<'a>(
        &'a self,
        scope: &'a Scope,
        input: AdmissionInput,
    ) -> PortFuture<'a, AdmissionResult>;
    /// Load owned state and the complete session transcript.
    fn load<'a>(&'a self, scope: &'a Scope, run_id: &'a Id) -> PortFuture<'a, StoredRun>;
    /// Read session-pinned metadata without changing its active run.
    fn load_session<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
    ) -> PortFuture<'a, SessionSnapshot>;
    /// Validate owner/generation against the current stored expiry without renewing.
    /// Return the latest lease metadata, including any concurrent heartbeat renewal.
    fn check_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, RunLease>;
    /// Acquire a new generation after any previous lease has expired or been released.
    fn acquire_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        owner: &'a Id,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease>;
    /// Renew an unexpired generation; an expired lease cannot be revived.
    fn renew_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease>;
    /// Release only the currently owned unexpired generation.
    fn release_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, ()>;
    /// Validate and commit state, transcript, records and events atomically.
    fn commit<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun>;
    /// Replay a bounded page. Retention gaps must not silently skip missing events.
    fn read_events<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        after_seq: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage>;
    /// Read an exact scope-owned immutable record after separate Host authorization.
    fn read_record<'a>(
        &'a self,
        scope: &'a Scope,
        reference: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord>;
    /// Append a report about an already committed result without changing its
    /// outcome, snapshot revision, session ownership, or durable event sequence.
    fn record_hook_observation<'a>(
        &'a self,
        _scope: &'a Scope,
        _run_id: &'a Id,
        _report: crate::HookObservation,
    ) -> PortFuture<'a, ()> {
        Box::pin(async {
            Err(error(
                ErrorCode::CapabilityUnsupported,
                "store.hook_observations",
            ))
        })
    }
    /// Read protected lifecycle observation reports after Host authorization.
    fn read_hook_observations<'a>(
        &'a self,
        _scope: &'a Scope,
        _run_id: &'a Id,
    ) -> PortFuture<'a, Vec<crate::HookObservation>> {
        Box::pin(async {
            Err(error(
                ErrorCode::CapabilityUnsupported,
                "store.hook_observations",
            ))
        })
    }
}

type ScopeKey = (Id, Id, Option<Id>);
type RecordKey = (Id, u64);

#[derive(Clone, Default)]
struct ScopeState {
    sessions: BTreeMap<Id, SessionState>,
    runs: BTreeMap<Id, RunState>,
    requests: BTreeMap<(Id, Id), Id>,
    records: BTreeMap<RecordKey, ProtectedRecord>,
    event_ids: BTreeSet<Id>,
    message_ids: BTreeSet<Id>,
    hook_observations: BTreeMap<Id, Vec<crate::HookObservation>>,
    executions: BTreeMap<Id, crate::ExecutionHistory>,
    legacy_runs: BTreeSet<Id>,
}

#[derive(Clone)]
struct SessionState {
    snapshot: SessionSnapshot,
    messages: Vec<Message>,
}

#[derive(Clone)]
struct RunState {
    snapshot: RunSnapshot,
    events: Vec<RunEvent>,
    lease: Option<RunLease>,
    last_fencing_token: u64,
}

/// Process-local reference store. It retains all committed data for its lifetime.
/// A single short critical section validates and applies each transaction; no
/// external calls or awaits occur while the lock is held. It provides neither
/// process-restart durability nor coordination between separate processes.
#[derive(Default)]
pub struct MemoryStateStore {
    scopes: Mutex<BTreeMap<ScopeKey, ScopeState>>,
}

impl MemoryStateStore {
    /// Construct an empty store without creating a runtime or doing I/O.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<MutexGuard<'_, BTreeMap<ScopeKey, ScopeState>>, ContractError> {
        self.scopes
            .lock()
            .map_err(|_| error(ErrorCode::PersistenceUnavailable, "state_store"))
    }
}

impl StateStore for MemoryStateStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        StateStoreCapabilities {
            durable: false,
            cross_process_leases: false,
            event_replay: true,
        }
    }

    fn find_request<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
        request_id: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        Box::pin(async move {
            let scopes = self.lock()?;
            let Some(state) = scopes.get(&scope_key(scope)) else {
                return Ok(None);
            };
            state
                .requests
                .get(&(session_id.clone(), request_id.clone()))
                .map(|run_id| stored_run(state, run_id))
                .transpose()
        })
    }

    fn admit<'a>(
        &'a self,
        scope: &'a Scope,
        input: AdmissionInput,
    ) -> PortFuture<'a, AdmissionResult> {
        Box::pin(async move {
            check_scope(scope, &input.snapshot.scope)?;
            if scope != input.snapshot.profile.scope() {
                return Err(error(ErrorCode::InvalidSnapshot, "request_scope"));
            }
            let mut scopes = self.lock()?;
            let empty = ScopeState::default();
            let state = scopes.get(&scope_key(scope)).unwrap_or(&empty);
            let request_key = (
                input.snapshot.request.session_id.clone(),
                input.snapshot.request.request_id.clone(),
            );
            if let Some(run_id) = state.requests.get(&request_key) {
                let previous = stored_run(state, run_id)?;
                let same = match state
                    .executions
                    .get(run_id)
                    .and_then(|h| h.submitted.as_ref())
                {
                    Some(stored) => match &input.submitted {
                        Some(candidate) => stored
                            .matches_submission(candidate, crate::JsonTextLimits::default())?,
                        None => false,
                    },
                    None => {
                        previous.snapshot.request_digest
                            == admission_digest(
                                &input.snapshot.request,
                                &input.snapshot.profile,
                                input.snapshot.system_inputs.as_ref(),
                            )
                    }
                };
                if !same {
                    return Err(error(ErrorCode::RequestConflict, "request"));
                }
                return Ok(AdmissionResult {
                    created: false,
                    state: previous,
                });
            }
            if input.require_durable {
                return Err(error(ErrorCode::CapabilityUnsupported, "store.durable"));
            }
            if input.snapshot.request_digest
                != admission_digest(
                    &input.snapshot.request,
                    &input.snapshot.profile,
                    input.snapshot.system_inputs.as_ref(),
                )
            {
                return Err(error(ErrorCode::InvalidSnapshot, "request_digest"));
            }
            if state.runs.iter().any(|(id, run)| {
                !run.snapshot.status.is_terminal() && !state.executions.contains_key(id)
            }) {
                return Err(error(
                    ErrorCode::CapabilityUnsupported,
                    "execution.legacy_drain_required",
                ));
            }
            input.snapshot.validate()?;
            if input.snapshot.revision != 0
                || input.snapshot.status != RunStatus::Running
                || input.snapshot.phase != RunPhase::Admission
                || !input.snapshot.model_ledger.is_empty()
                || !input.snapshot.tool_ledger.is_empty()
                || !input.snapshot.reservations.is_empty()
                || !input.snapshot.resume_receipts.is_empty()
                || !input.snapshot.recovery_receipts.is_empty()
                || !input.snapshot.hook_applications.is_empty()
                || !input.snapshot.context_batches.is_empty()
                || !input.snapshot.source_states.is_empty()
                || input.snapshot.usage != BudgetUsage::default()
            {
                return Err(error(ErrorCode::InvalidSnapshot, "admission"));
            }
            if state.runs.contains_key(&input.snapshot.run_id) {
                return Err(error(ErrorCode::RunConflict, "run_id"));
            }
            let session_id = &input.snapshot.request.session_id;
            let previous_session = state.sessions.get(session_id);
            if let Some(session) = previous_session {
                if session.snapshot.profile_digest != *input.snapshot.profile.profile_digest()
                    || session.snapshot.prompt_snapshot != input.prompt_snapshot
                {
                    return Err(error(ErrorCode::ProfileMismatch, "session.profile"));
                }
                if session.snapshot.active_run_id.is_some() {
                    return Err(error(ErrorCode::SessionBusy, "session"));
                }
            }
            let additions = validate_records(state, &input.records)?;
            record_value(state, &additions, &input.prompt_snapshot)?;
            validate_snapshot_refs(state, &additions, &input.snapshot)?;
            validate_events(
                state,
                &additions,
                &input.snapshot,
                0,
                &input.events,
                true,
                &input.messages,
            )?;
            let previous_sequence = previous_session.map_or(0, |s| s.snapshot.transcript_revision);
            if input.snapshot.context_revision_ref.as_ref()
                != previous_session
                    .and_then(|session| session.snapshot.context_revision_ref.as_ref())
                || !input.snapshot.context_decisions.is_empty()
            {
                return Err(error(ErrorCode::InvalidSnapshot, "context.admission"));
            }
            let transcript_revision = validate_messages(
                state,
                &additions,
                &input.snapshot.run_id,
                previous_sequence,
                &input.messages,
            )?;
            let mut messages = previous_session.map_or_else(Vec::new, |s| s.messages.clone());
            messages.extend(input.messages);
            let session = SessionSnapshot {
                schema_version: SessionSchemaVersion::V1,
                session_id: session_id.clone(),
                scope: scope.clone(),
                profile_digest: input.snapshot.profile.profile_digest().clone(),
                prompt_snapshot: input.prompt_snapshot,
                transcript_revision,
                context_revision_ref: input.snapshot.context_revision_ref.clone(),
                active_run_id: Some(input.snapshot.run_id.clone()),
            };
            let result = StoredRun {
                snapshot: input.snapshot.clone(),
                session: session.clone(),
                messages: messages.clone(),
            };
            let execution = execution::initial_history(
                &input.snapshot,
                &input.execution_principal_ref,
                input.submitted.as_ref(),
            )?;
            // All fallible checks precede these mutations.
            let state = scopes.entry(scope_key(scope)).or_default();
            state
                .executions
                .insert(input.snapshot.run_id.clone(), execution);
            state.records.extend(additions);
            state
                .message_ids
                .extend(messages.iter().map(|m| m.message_id.clone()));
            state
                .event_ids
                .extend(input.events.iter().map(|e| e.event_id.clone()));
            state
                .requests
                .insert(request_key, input.snapshot.run_id.clone());
            state.sessions.insert(
                session.session_id.clone(),
                SessionState {
                    snapshot: session,
                    messages,
                },
            );
            state.runs.insert(
                input.snapshot.run_id.clone(),
                RunState {
                    snapshot: input.snapshot,
                    events: input.events,
                    lease: None,
                    last_fencing_token: 0,
                },
            );
            Ok(AdmissionResult {
                created: true,
                state: result,
            })
        })
    }

    fn load<'a>(&'a self, scope: &'a Scope, run_id: &'a Id) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            let scopes = self.lock()?;
            stored_run(namespace(&scopes, scope)?, run_id)
        })
    }

    fn load_session<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
    ) -> PortFuture<'a, SessionSnapshot> {
        Box::pin(async move {
            let scopes = self.lock()?;
            namespace(&scopes, scope)?
                .sessions
                .get(session_id)
                .map(|session| session.snapshot.clone())
                .ok_or_else(not_found)
        })
    }

    fn check_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, RunLease> {
        Box::pin(async move {
            let scopes = self.lock()?;
            let run = namespace(&scopes, scope)?
                .runs
                .get(run_id)
                .ok_or_else(not_found)?;
            validate_lease(run, scope, run_id, lease, now_ms)?;
            Ok(run.lease.as_ref().expect("validated lease").clone())
        })
    }

    fn acquire_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        owner: &'a Id,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease> {
        Box::pin(async move { self.acquire_lease_now(scope, run_id, owner, now_ms, ttl_ms) })
    }

    fn renew_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease> {
        Box::pin(async move {
            let expires_at_ms = lease_expiry(now_ms, ttl_ms)?;
            let mut scopes = self.lock()?;
            let run = run_mut(&mut scopes, scope, run_id)?;
            validate_lease(run, scope, run_id, lease, now_ms)?;
            let renewed = RunLease {
                expires_at_ms,
                ..lease.clone()
            };
            run.lease = Some(renewed.clone());
            Ok(renewed)
        })
    }

    fn release_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let mut scopes = self.lock()?;
            let run = run_mut(&mut scopes, scope, run_id)?;
            validate_lease(run, scope, run_id, lease, now_ms)?;
            run.lease = None;
            Ok(())
        })
    }

    fn commit<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move { self.commit_now(scope, run_id, input, None) })
    }

    fn read_events<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        after_seq: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage> {
        Box::pin(async move {
            if limit == 0 || limit > MAX_EVENT_PAGE_SIZE {
                return Err(error(ErrorCode::InvalidContract, "events.limit"));
            }
            let scopes = self.lock()?;
            let run = namespace(&scopes, scope)?
                .runs
                .get(run_id)
                .ok_or_else(not_found)?;
            let mut available = run.events.iter().filter(|e| e.seq.get() > after_seq);
            let events: Vec<_> = available.by_ref().take(limit).cloned().collect();
            Ok(EventPage {
                next_after_seq: events.last().map_or(after_seq, |e| e.seq.get()),
                has_more: available.next().is_some(),
                first_available_seq: run.events.first().map(|e| e.seq),
                last_available_seq: run.snapshot.last_event_seq,
                events,
            })
        })
    }

    fn read_record<'a>(
        &'a self,
        scope: &'a Scope,
        reference: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        Box::pin(async move {
            let scopes = self.lock()?;
            let record = namespace(&scopes, scope)?
                .records
                .get(&record_key(reference))
                .ok_or_else(not_found)?;
            if record.reference != *reference {
                return Err(error(ErrorCode::RecordConflict, "record.reference"));
            }
            Ok(record.clone())
        })
    }
    fn record_hook_observation<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        report: crate::HookObservation,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let mut scopes = self.lock()?;
            let state = scopes.get_mut(&scope_key(scope)).ok_or_else(not_found)?;
            validate_hook_observation(state, scope, run_id, &report)?;
            let reports = state.hook_observations.entry(run_id.clone()).or_default();
            if let Some(existing) = reports.iter().find(|existing| {
                existing.hook == report.hook
                    && existing.selection == report.selection
                    && existing.target == report.target
            }) {
                return if existing == &report {
                    Ok(())
                } else {
                    Err(error(ErrorCode::RecordConflict, "hooks.observation"))
                };
            }
            reports.push(report);
            Ok(())
        })
    }
    fn read_hook_observations<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
    ) -> PortFuture<'a, Vec<crate::HookObservation>> {
        Box::pin(async move {
            let scopes = self.lock()?;
            let state = namespace(&scopes, scope)?;
            if !state.runs.contains_key(run_id) {
                return Err(not_found());
            }
            Ok(state
                .hook_observations
                .get(run_id)
                .cloned()
                .unwrap_or_default())
        })
    }
}

fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}

fn not_found() -> ContractError {
    error(ErrorCode::StateNotFound, "state")
}

fn scope_key(scope: &Scope) -> ScopeKey {
    (
        scope.tenant_id.clone(),
        scope.workspace_id.clone(),
        scope.user_id.clone(),
    )
}

fn record_key(reference: &RecordRef) -> RecordKey {
    (reference.record_id.clone(), reference.revision)
}

fn check_scope(expected: &Scope, actual: &Scope) -> Result<(), ContractError> {
    if expected != actual {
        Err(error(ErrorCode::AccessDenied, "scope"))
    } else {
        Ok(())
    }
}

fn namespace<'a>(
    scopes: &'a BTreeMap<ScopeKey, ScopeState>,
    scope: &Scope,
) -> Result<&'a ScopeState, ContractError> {
    scopes.get(&scope_key(scope)).ok_or_else(not_found)
}

fn run_mut<'a>(
    scopes: &'a mut BTreeMap<ScopeKey, ScopeState>,
    scope: &Scope,
    run_id: &Id,
) -> Result<&'a mut RunState, ContractError> {
    scopes
        .get_mut(&scope_key(scope))
        .and_then(|state| state.runs.get_mut(run_id))
        .ok_or_else(not_found)
}

fn stored_run(state: &ScopeState, run_id: &Id) -> Result<StoredRun, ContractError> {
    let run = state.runs.get(run_id).ok_or_else(not_found)?;
    let session = state
        .sessions
        .get(&run.snapshot.request.session_id)
        .ok_or_else(not_found)?;
    Ok(StoredRun {
        snapshot: run.snapshot.clone(),
        session: session.snapshot.clone(),
        messages: session.messages.clone(),
    })
}

fn lease_expiry(now_ms: i64, ttl_ms: u64) -> Result<i64, ContractError> {
    let ttl = i64::try_from(ttl_ms)
        .ok()
        .filter(|ttl| *ttl > 0)
        .ok_or_else(|| error(ErrorCode::InvalidContract, "lease.ttl_ms"))?;
    now_ms
        .checked_add(ttl)
        .ok_or_else(|| error(ErrorCode::InvalidContract, "lease.expires_at_ms"))
}

fn validate_lease(
    run: &RunState,
    scope: &Scope,
    run_id: &Id,
    provided: &RunLease,
    now_ms: i64,
) -> Result<(), ContractError> {
    if &provided.scope != scope
        || &provided.run_id != run_id
        || !run.lease.as_ref().is_some_and(|stored| {
            stored.owner == provided.owner
                && stored.fencing_token == provided.fencing_token
                && now_ms < stored.expires_at_ms
        })
    {
        return Err(error(ErrorCode::LeaseLost, "lease"));
    }
    Ok(())
}

fn validate_records(
    state: &ScopeState,
    records: &[ProtectedRecord],
) -> Result<BTreeMap<RecordKey, ProtectedRecord>, ContractError> {
    let mut additions = BTreeMap::new();
    for record in records {
        let key = record_key(&record.reference);
        if state
            .records
            .get(&key)
            .or_else(|| additions.get(&key))
            .is_some_and(|existing| existing != record)
        {
            return Err(error(ErrorCode::RecordConflict, "records"));
        }
        additions.insert(key, record.clone());
    }
    Ok(additions)
}

fn record_value<'a>(
    state: &'a ScopeState,
    additions: &'a BTreeMap<RecordKey, ProtectedRecord>,
    reference: &RecordRef,
) -> Result<&'a Value, ContractError> {
    let record = additions
        .get(&record_key(reference))
        .or_else(|| state.records.get(&record_key(reference)))
        .ok_or_else(not_found)?;
    if &record.reference != reference {
        return Err(error(ErrorCode::RecordConflict, "record.reference"));
    }
    Ok(&record.value)
}

fn validate_snapshot_refs(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    recovery_state::validate(state, additions, snapshot)?;
    validate_source_snapshot(state, additions, snapshot)?;
    skill_state::validate_skill_snapshot(state, additions, snapshot)?;
    context_state::validate_snapshot(state, additions, snapshot)?;
    verification_state::validate(state, additions, snapshot)?;
    validate_hook_snapshot(state, additions, snapshot)?;
    let mut references = Vec::new();
    for receipt in &snapshot.resume_receipts {
        let command: ResumeCommand = event_record(state, additions, &receipt.command_ref)?;
        let outcome: crate::RunOutcome =
            event_record(state, additions, &receipt.previous_outcome_ref)?;
        let crate::OutcomeResult::Waiting { wait } = &outcome.result else {
            return Err(error(ErrorCode::InvalidSnapshot, "resume_receipts.outcome"));
        };
        outcome.validate()?;
        if command != receipt.command
            || outcome.checkpoint_revision != command.expected_revision
            || !action_matches_wait(wait, &command.action)
        {
            return Err(error(ErrorCode::InvalidSnapshot, "resume_receipts.records"));
        }
        for reference in &outcome.unresolved_effects {
            record_value(state, additions, reference)?;
        }
    }
    if let Some(reference) = &snapshot.routing_snapshot_ref {
        let value = record_value(state, additions, reference)?;
        let routing = crate::RoutingSnapshot::restore(
            &serde_json::to_string(value)
                .map_err(|_| error(ErrorCode::InvalidSnapshot, "routing"))?,
            &snapshot.scope,
            &reference.digest,
        )?;
        for invocation in &snapshot.model_ledger {
            routing.validate_route(&invocation.route)?;
            // The saved step identifies the logical binding independently of the physical route.
            // Auxiliary stages may use their own purpose-specific rule.
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct SavedStep {
                schema_version: String,
                run_id: Id,
                input: crate::RoutedModelInput,
            }
            let key = (
                Id::new(format!(
                    "model-step-{}",
                    crate::canonical_digest(&serde_json::json!([
                        snapshot.run_id,
                        invocation.model_step_id
                    ]))
                ))?,
                1,
            );
            let value = additions
                .get(&key)
                .map(ProtectedRecord::value)
                .or_else(|| state.records.get(&key).map(|record| &record.value))
                .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "routing.step_input"))?;
            let step: SavedStep = serde_json::from_value(value.clone())
                .map_err(|_| error(ErrorCode::InvalidSnapshot, "routing.step_input"))?;
            if step.schema_version != "wickle.model-step.v1"
                || step.run_id != snapshot.run_id
                || step.input.model_step_id != invocation.model_step_id
                || step.input.routing.scope != snapshot.scope
                || step.input.routing.purpose != invocation.purpose
                || (invocation.purpose == crate::ModelPurpose::Agent
                    && step.input.routing.model_binding != snapshot.profile.profile().model_binding)
            {
                return Err(error(ErrorCode::InvalidSnapshot, "routing.step_identity"));
            }
            let rule = routing
                .policy()
                .rules
                .iter()
                .find(|rule| {
                    rule.model_binding == step.input.routing.model_binding
                        && rule.purpose == invocation.purpose
                })
                .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "routing.invocation"))?;
            if invocation.inspection_ref.is_none()
                || !(rule.primary == invocation.route.binding
                    || rule.fallbacks.contains(&invocation.route.binding))
            {
                return Err(error(ErrorCode::InvalidSnapshot, "routing.invocation"));
            }
            let reference = invocation
                .inspection_ref
                .as_ref()
                .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "routing.inspection"))?;
            let observation: crate::ModelRouteObservation =
                serde_json::from_value(record_value(state, additions, reference)?.clone())
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "routing.inspection"))?;
            let require_pinned = invocation.route.version_semantics
                == crate::VersionSemantics::Pinned
                || rule.version_policy == crate::VersionPolicy::RequirePinned;
            observation.validate(
                &invocation.route,
                if require_pinned {
                    crate::VersionPolicy::RequirePinned
                } else {
                    crate::VersionPolicy::AllowMutable
                },
            )?;
        }
    }
    let run_inputs = snapshot
        .system_inputs
        .as_ref()
        .map(|inputs| {
            crate::RunSystemInputs::from_value(
                record_value(state, additions, &inputs.snapshot_ref)?,
                inputs,
                &snapshot.scope,
            )
            .map_err(|_| error(ErrorCode::InvalidSnapshot, "system_inputs"))
        })
        .transpose()?;
    for invocation in &snapshot.model_ledger {
        if let Some(reference) = invocation
            .inspection_ref
            .as_ref()
            .filter(|_| snapshot.routing_snapshot_ref.is_none())
        {
            let observation: crate::ModelRouteObservation =
                serde_json::from_value(record_value(state, additions, reference)?.clone())
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "model.inspection"))?;
            observation.validate(&invocation.route, crate::VersionPolicy::AllowMutable)?;
        }
        if let Some(reference) = &invocation.response_ref {
            validate_model_response(state, additions, invocation, reference)?;
        }
    }
    if let Some(reference) = &snapshot.assembly_ref {
        let registry = crate::SystemInputRegistry::new(
            run_inputs
                .as_ref()
                .map(|inputs| inputs.definitions().values().cloned().collect())
                .unwrap_or_default(),
        )?;
        let value = record_value(state, additions, reference)?;
        let assembly = crate::ResolvedAssembly::restore(
            &serde_json::to_string(value)
                .map_err(|_| error(ErrorCode::InvalidSnapshot, "assembly"))?,
            &snapshot.profile,
            &registry,
            &reference.digest,
        )?;
        if assembly.session_id() != &snapshot.request.session_id {
            return Err(error(ErrorCode::InvalidSnapshot, "assembly.session"));
        }
        if let Some(session) = state.sessions.get(&snapshot.request.session_id) {
            let value = record_value(state, additions, &session.snapshot.prompt_snapshot)?;
            let prompt = crate::PromptSnapshot::restore(
                &serde_json::to_string(value)
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "assembly.prompt"))?,
                &session.snapshot.prompt_snapshot.digest,
                &snapshot.profile,
                &snapshot.scope,
            )?;
            if prompt.tools().len() != assembly.tools().len()
                || prompt
                    .tools()
                    .iter()
                    .zip(assembly.tools())
                    .any(|(pinned, binding)| {
                        pinned.selection != binding.selection
                            || &pinned.compiled_digest != binding.compiled.digest()
                            || pinned.model_tool != binding.compiled.to_model_tool()
                    })
            {
                return Err(error(ErrorCode::InvalidSnapshot, "assembly.prompt_tools"));
            }
        }
    }
    references.extend(&snapshot.context_batches);
    references.extend(snapshot.source_states.iter().map(|s| &s.batch_ref));
    for entry in &snapshot.tool_ledger {
        if let Some(reference) = &entry.call.bound_input_ref {
            crate::input_binding::validate_bound_record(
                record_value(state, additions, reference)?,
                snapshot,
                &entry.call,
                run_inputs.as_ref(),
            )
            .map_err(|_| error(ErrorCode::InvalidSnapshot, "bound_input"))?;
            let bound = record_value(state, additions, reference)?;
            let transform_ref = bound
                .get("data")
                .and_then(|data| data.get("transformation_ref"))
                .map(|value| {
                    serde_json::from_value::<RecordRef>(value.clone()).map_err(|_| {
                        error(ErrorCode::InvalidSnapshot, "bound_input.transformation_ref")
                    })
                })
                .transpose()?;
            let transformed = transform_ref
                .as_ref()
                .map(|reference| record_value(state, additions, reference))
                .transpose()?;
            crate::input_binding::validate_bound_transformation(
                bound,
                snapshot,
                &entry.call,
                transformed,
            )?;
        }
    }
    for entry in &snapshot.tool_ledger {
        if let ToolCallState::Settled { result } = &entry.state {
            references.extend(tool_result_refs(result));
        }
    }
    if let Some(wait) = &snapshot.wait {
        if let WaitTarget::Approval {
            target: ApprovalTarget::Candidate { candidate_ref, .. },
        } = &wait.target
        {
            references.push(candidate_ref);
        }
    }
    if let Some(outcome) = &snapshot.outcome {
        references.extend(&outcome.unresolved_effects);
        if let Some(verification) = &outcome.verification {
            references.extend(&verification.evidence);
        }
        if let OutcomeResult::Failed { failure } = &outcome.result {
            references.extend(failure.diagnostic_ref.iter());
        }
    }
    for reference in references {
        record_value(state, additions, reference)?;
    }
    Ok(())
}

fn validate_model_response(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    invocation: &ModelInvocationRecord,
    reference: &RecordRef,
) -> Result<(), ContractError> {
    let invalid = || error(ErrorCode::InvalidSnapshot, "model_ledger.response_ref");
    let saved: StoredModelResponse =
        serde_json::from_value(record_value(state, additions, reference)?.clone())
            .map_err(|_| invalid())?;
    let route_digest = invocation.route.digest();
    if saved.request_id != invocation.attempt_id || saved.route_digest != route_digest {
        return Err(invalid());
    }
    let metadata = match (&invocation.state, &saved.outcome) {
        (ModelAttemptState::Completed {}, ModelExchangeOutcome::Completed { response }) => {
            let mut call_ids = BTreeSet::new();
            if response.request_id != invocation.attempt_id
                || response.route_digest != route_digest
                || response
                    .continuation
                    .iter()
                    .any(|continuation| continuation.route_digest() != &route_digest)
                || response.finish == ModelFinish::Length
                || (response.finish == ModelFinish::ToolCalls) != !response.tool_calls.is_empty()
                || response
                    .tool_calls
                    .iter()
                    .any(|call| !call_ids.insert(&call.provider_call_id))
            {
                return Err(invalid());
            }
            &response.metadata
        }
        (ModelAttemptState::Failed { kind }, ModelExchangeOutcome::Failed { failure })
            if *kind == failure.kind =>
        {
            &failure.metadata
        }
        _ => return Err(invalid()),
    };
    if metadata.provider_request_id != invocation.provider_request_id
        || metadata.reported_model_id != invocation.reported_model_id
        || metadata.reported_model_version != invocation.reported_model_version
        || metadata.usage != invocation.usage
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_messages(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    run_id: &Id,
    last_sequence: u64,
    messages: &[Message],
) -> Result<u64, ContractError> {
    let mut sequence = last_sequence;
    let mut seen = BTreeSet::new();
    for message in messages {
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::InvalidMessage, "messages.sequence"))?;
        if &message.run_id != run_id
            || message.sequence.get() != sequence
            || state.message_ids.contains(&message.message_id)
            || !seen.insert(&message.message_id)
        {
            return Err(error(ErrorCode::InvalidMessage, "messages"));
        }
        for content in &message.content {
            if matches!(content, ContentBlock::ToolResultCorrection { .. }) {
                let mut history: Vec<_> = state
                    .sessions
                    .values()
                    .flat_map(|session| &session.messages)
                    .filter(|prior| prior.run_id == *run_id && prior.sequence <= message.sequence)
                    .cloned()
                    .collect();
                for addition in messages
                    .iter()
                    .filter(|addition| addition.sequence <= message.sequence)
                {
                    if !history
                        .iter()
                        .any(|prior| prior.message_id == addition.message_id)
                    {
                        history.push(addition.clone());
                    }
                }
                history.sort_by_key(|item| item.sequence);
                crate::message::tool_corrections(&history)?;
            }
            let references = match content {
                ContentBlock::Content { .. } => Vec::new(),
                ContentBlock::ToolCall { call } => call.bound_input_ref.iter().collect(),
                ContentBlock::ToolResult { result }
                | ContentBlock::ToolResultCorrection { result, .. } => tool_result_refs(result),
                ContentBlock::ProviderOpaque { data_ref, .. } => vec![data_ref],
            };
            for reference in references {
                record_value(state, additions, reference)?;
            }
        }
    }
    Ok(sequence)
}

fn validate_tool_pair(
    state: &ScopeState,
    snapshot: &RunSnapshot,
    additions: &[Message],
    result: &ToolResult,
) -> Result<(), ContractError> {
    let existing = state
        .sessions
        .get(&snapshot.request.session_id)
        .map_or(&[][..], |session| session.messages.as_slice());
    let entry = snapshot
        .tool_ledger
        .iter()
        .find(|entry| entry.call.call_id == result.call_id)
        .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.result_call"))?;
    let paired = existing.iter().chain(additions).any(|message| {
        message.message_id == result.call_message_id
            && message.run_id == snapshot.run_id
            && message.role == crate::MessageRole::Assistant
            && message.origin == crate::MessageOrigin::Model
            && message.content.iter().any(|content| {
                let ContentBlock::ToolCall { call } = content else {
                    return false;
                };
                let mut original = call.clone();
                if original.bound_input_ref.is_none() {
                    original.bound_input_ref = entry.call.bound_input_ref.clone();
                }
                original == entry.call
            })
    });
    if !paired {
        return Err(error(ErrorCode::InvalidSnapshot, "tool.result_message"));
    }
    Ok(())
}

fn tool_result_refs(result: &ToolResult) -> Vec<&RecordRef> {
    result
        .effect_receipt_ref
        .iter()
        .chain(
            result
                .error
                .iter()
                .flat_map(|error| error.diagnostic_ref.iter()),
        )
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn validate_events(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    previous_seq: u64,
    events: &[RunEvent],
    admission: bool,
    messages: &[Message],
) -> Result<(), ContractError> {
    let mut sequence = previous_seq;
    let mut seen = BTreeSet::new();
    let mut started = 0;
    let mut finished = 0;
    let mut resumed = 0;
    let reconciled = reconciliation_state::corrections(
        state,
        additions,
        snapshot,
        &events.iter().collect::<Vec<_>>(),
        &messages.iter().collect::<Vec<_>>(),
    )?;
    for message in messages {
        for content in &message.content {
            if let ContentBlock::ToolResultCorrection { result, .. } = content {
                if reconciled.contains(&message.message_id) {
                    continue;
                }
                let previous = state.runs.get(&snapshot.run_id).ok_or_else(not_found)?;
                if !matches!(previous.snapshot.wait.as_ref().map(|wait| &wait.target), Some(WaitTarget::External { call_id, .. }) if call_id == &result.call_id)
                    || !snapshot.resume_receipts.last().is_some_and(|receipt| {
                        receipt.accepted_revision == snapshot.revision
                            && matches!(receipt.command.action, ResumeAction::External { .. })
                    })
                    || !events.iter().any(|event| match &event.payload {
                        RunEventPayload::ToolSettled { result_ref } => {
                            event_record::<ToolResult>(state, additions, result_ref)
                                .is_ok_and(|saved| saved == *result)
                        }
                        _ => false,
                    })
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.tool_correction"));
                }
            }
        }
    }
    for event in events {
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::InvalidEvent, "events.seq"))?;
        if event.scope != snapshot.scope
            || event.run_id != snapshot.run_id
            || event.session_id != snapshot.request.session_id
            || event.seq.get() != sequence
            || state.event_ids.contains(&event.event_id)
            || !seen.insert(&event.event_id)
        {
            return Err(error(ErrorCode::InvalidEvent, "events"));
        }
        let reference = match &event.payload {
            RunEventPayload::RunRecovered {
                recovery_receipt_ref,
            } => {
                recovery_state::event(state, additions, snapshot, event, recovery_receipt_ref)?;
                recovery_receipt_ref
            }
            RunEventPayload::ToolReconciled { reconciliation_ref } => reconciliation_ref,
            RunEventPayload::ContextRewritten { revision_ref } => {
                let revision = context_state::revision(state, additions, revision_ref, snapshot)?;
                if snapshot.context_revision_ref.as_ref() != Some(revision_ref)
                    || revision.run_id != snapshot.run_id
                    || Some(&revision.model_step_id) != snapshot.model_step_id.as_ref()
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.context"));
                }
                revision_ref
            }
            RunEventPayload::RunStarted {
                request_ref,
                profile_digest,
            } => {
                started += 1;
                if !admission
                    || profile_digest != snapshot.profile.profile_digest()
                    || record_value(state, additions, request_ref)?
                        != &serde_json::to_value(&snapshot.request)
                            .map_err(|_| error(ErrorCode::InvalidContract, "request"))?
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_started"));
                }
                request_ref
            }
            RunEventPayload::RunFinished { outcome_ref } => {
                finished += 1;
                let outcome = snapshot
                    .outcome
                    .as_ref()
                    .filter(|_| snapshot.status.is_terminal())
                    .ok_or_else(|| error(ErrorCode::InvalidEvent, "events.run_finished"))?;
                if record_value(state, additions, outcome_ref)?
                    != &serde_json::to_value(outcome)
                        .map_err(|_| error(ErrorCode::InvalidContract, "outcome"))?
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_finished"));
                }
                outcome_ref
            }
            RunEventPayload::ToolPlanned { call_ref } => {
                let call: ToolCall = event_record(state, additions, call_ref)?;
                if !snapshot.tool_ledger.iter().any(|entry| entry.call == call) {
                    return Err(error(ErrorCode::InvalidEvent, "events.tool_planned"));
                }
                call_ref
            }
            RunEventPayload::ToolSettled { result_ref } => {
                let result: ToolResult = event_record(state, additions, result_ref)?;
                if !snapshot.tool_ledger.iter().any(|entry| {
                    matches!(
                        &entry.state, ToolCallState::Settled { result: saved } if *saved == result
                    )
                }) {
                    return Err(error(ErrorCode::InvalidEvent, "events.tool_settled"));
                }
                if state.runs.get(&snapshot.run_id).is_some_and(|previous| previous.snapshot.tool_ledger.iter()
                    .any(|entry| entry.call.call_id == result.call_id && matches!(entry.state, ToolCallState::Unknown { .. })))
                    && !messages.iter().flat_map(|message| &message.content)
                        .any(|content| matches!(content, ContentBlock::ToolResultCorrection { result: corrected, .. } if corrected == &result))
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.tool_correction_missing"));
                }
                validate_tool_pair(state, snapshot, messages, &result)?;
                result_ref
            }
            RunEventPayload::ToolUnresolved {
                result_ref,
                attempt_id,
                idempotency_key,
            } => {
                let result: ToolResult = event_record(state, additions, result_ref)?;
                if result.status != crate::ToolResultStatus::Unknown || result.effect != crate::ToolEffect::Unknown
                    || !snapshot.tool_ledger.iter().any(|entry| entry.call.call_id == result.call_id
                        && matches!(&entry.state, ToolCallState::Unknown { attempt_id: saved, idempotency_key: key }
                            if saved == attempt_id && key == idempotency_key))
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.tool_unresolved"));
                }
                validate_tool_pair(state, snapshot, messages, &result)?;
                for reference in tool_result_refs(&result) {
                    record_value(state, additions, reference)?;
                }
                result_ref
            }
            RunEventPayload::VerificationCompleted { verification_ref } => {
                verification_state::event(state, additions, snapshot, verification_ref)?;
                verification_ref
            }
            RunEventPayload::RunWaiting { wait_ref } => {
                let wait: WaitState = event_record(state, additions, wait_ref)?;
                if snapshot.status != RunStatus::Waiting || snapshot.wait.as_ref() != Some(&wait) {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_waiting"));
                }
                wait_ref
            }
            RunEventPayload::RunResumed { command_ref } => {
                resumed += 1;
                let command: ResumeCommand = event_record(state, additions, command_ref)?;
                let previous = state.runs.get(&snapshot.run_id).ok_or_else(not_found)?;
                if snapshot.status != RunStatus::Running
                    || command.run_id != snapshot.run_id
                    || command.expected_revision != previous.snapshot.revision
                    || !resume_target_matches(&previous.snapshot, &command.action)
                    || !snapshot.resume_receipts.last().is_some_and(|receipt| {
                        receipt.command_ref == *command_ref
                            && receipt.command == command
                            && receipt.accepted_revision == snapshot.revision
                    })
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_resumed"));
                }
                let receipt = snapshot
                    .resume_receipts
                    .last()
                    .expect("receipt checked above");
                let prior: crate::RunOutcome =
                    event_record(state, additions, &receipt.previous_outcome_ref)?;
                if previous.snapshot.outcome.as_ref() != Some(&prior)
                    || event.timestamp_ms != snapshot.timing.last_observed_at_ms
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.resume_outcome"));
                }
                match &command.action {
                    ResumeAction::External { receipt_ref, .. } => {
                        record_value(state, additions, receipt_ref)?;
                    }
                    ResumeAction::Recover { recovery_ref } => {
                        record_value(state, additions, recovery_ref)?;
                    }
                    _ => {}
                }
                command_ref
            }
            RunEventPayload::ModelRouteSelected {
                invocation_ref,
                route_digest,
            } => {
                let invocation: ModelInvocationRecord =
                    event_record(state, additions, invocation_ref)?;
                if invocation.route.digest() != *route_digest
                    || !snapshot.model_ledger.contains(&invocation)
                {
                    return Err(error(
                        ErrorCode::InvalidEvent,
                        "events.model_route_selected",
                    ));
                }
                invocation_ref
            }
        };
        record_value(state, additions, reference)?;
    }
    if sequence != snapshot.last_event_seq
        || (admission && (started != 1 || events.len() != 1))
        || (snapshot.status.is_terminal() && finished != 1)
        || (!admission
            && resumed
                != snapshot.resume_receipts.len().saturating_sub(
                    state
                        .runs
                        .get(&snapshot.run_id)
                        .ok_or_else(not_found)?
                        .snapshot
                        .resume_receipts
                        .len(),
                ))
    {
        return Err(error(ErrorCode::InvalidEvent, "events"));
    }
    if !admission {
        let previous = &state
            .runs
            .get(&snapshot.run_id)
            .ok_or_else(not_found)?
            .snapshot;
        for old in &previous.tool_ledger {
            let Some(new) = snapshot
                .tool_ledger
                .iter()
                .find(|entry| entry.call.call_id == old.call.call_id)
            else {
                continue;
            };
            if !matches!(old.state, ToolCallState::Unknown { .. })
                || !matches!(new.state, ToolCallState::Settled { .. })
            {
                continue;
            }
            if messages.iter().any(|message|reconciled.contains(&message.message_id)&&matches!(message.content.as_slice(),[ContentBlock::ToolResultCorrection{result,..}] if result.call_id==old.call.call_id)){continue;}
            if !snapshot.resume_receipts.last().is_some_and(|receipt| {
                    receipt.accepted_revision == snapshot.revision
                        && matches!(receipt.command.action, ResumeAction::External { .. })
                        && matches!(previous.wait.as_ref().map(|wait| &wait.target), Some(WaitTarget::External { call_id, .. }) if call_id == &old.call.call_id)
                }) || !messages.iter().any(|message| matches!(message.content.as_slice(),
                    [ContentBlock::ToolResultCorrection { result, .. }] if result.call_id == old.call.call_id))
            { return Err(error(ErrorCode::InvalidEvent, "events.tool_correction_missing")); }
        }
        if snapshot
            .resume_receipts
            .last()
            .is_some_and(|receipt| receipt.accepted_revision == snapshot.revision)
        {
            let target = match &previous.wait.as_ref().ok_or_else(not_found)?.target {
                WaitTarget::Input { request } => Some((&request.call_id, Some(request))),
                WaitTarget::Approval {
                    target: ApprovalTarget::Tool { call_id, .. },
                } => Some((call_id, None)),
                _ => None,
            };
            if let Some((call_id, input)) = target {
                let old = previous
                    .tool_ledger
                    .iter()
                    .find(|entry| &entry.call.call_id == call_id)
                    .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "resume.call"))?;
                if input.is_some_and(|request| !matches!(&old.state, ToolCallState::InputPending { request: pending, .. } if pending == request))
                    || (input.is_none() && !matches!(old.state, ToolCallState::Planned {} | ToolCallState::ApprovalPending { .. }))
                { return Err(error(ErrorCode::InvalidTransition, "resume.call_state")); }
            }
        }
    }
    Ok(())
}

fn event_record<T: DeserializeOwned>(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    reference: &RecordRef,
) -> Result<T, ContractError> {
    serde_json::from_value(record_value(state, additions, reference)?.clone())
        .map_err(|_| error(ErrorCode::InvalidEvent, "events.record"))
}

/// Replay the causal facts shared by live commits and durable checkpoint restore.
/// A receipt does not by itself authorize rewriting a result: its preceding wait,
/// intervening settlement, and transcript observation must identify the same call.
fn validate_resume_history(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    events: &[&RunEvent],
    messages: &[&Message],
) -> Result<(), ContractError> {
    let invalid = || error(ErrorCode::InvalidSnapshot, "resume.history");
    let resumed: Vec<_> = events
        .iter()
        .copied()
        .filter(|event| matches!(event.payload, RunEventPayload::RunResumed { .. }))
        .collect();
    if resumed.len() != snapshot.resume_receipts.len() {
        return Err(invalid());
    }
    let mut previous_resume_seq = 0;
    let mut corrected_calls = BTreeSet::new();
    let mut authorized_corrections =
        reconciliation_state::corrections(state, additions, snapshot, events, messages)?;
    for message in messages
        .iter()
        .filter(|message| authorized_corrections.contains(&message.message_id))
    {
        if let [ContentBlock::ToolResultCorrection { result, .. }] = message.content.as_slice() {
            corrected_calls.insert(result.call_id.clone());
        }
    }
    for (event, receipt) in resumed.into_iter().zip(&snapshot.resume_receipts) {
        let RunEventPayload::RunResumed { command_ref } = &event.payload else {
            unreachable!()
        };
        if command_ref != &receipt.command_ref
            || event.seq.get() <= receipt.previous_last_event_seq
            || receipt.previous_last_event_seq <= previous_resume_seq
            || event.timestamp_ms > snapshot.timing.last_observed_at_ms
        {
            return Err(invalid());
        }
        previous_resume_seq = event.seq.get();
        let prior: crate::RunOutcome =
            event_record(state, additions, &receipt.previous_outcome_ref)?;
        let OutcomeResult::Waiting { wait } = &prior.result else {
            return Err(invalid());
        };
        let waiting_event = events
            .iter()
            .find(|event| event.seq.get() == receipt.previous_last_event_seq)
            .ok_or_else(invalid)?;
        let RunEventPayload::RunWaiting { wait_ref } = &waiting_event.payload else {
            return Err(invalid());
        };
        let saved_wait: WaitState = event_record(state, additions, wait_ref)?;
        let expired = event.timestamp_ms >= snapshot.timing.deadline_at_ms
            || wait
                .expires_at_ms
                .is_some_and(|deadline| event.timestamp_ms >= deadline);
        if &saved_wait != wait
            || receipt.expired != expired
            || waiting_event.timestamp_ms > event.timestamp_ms
        {
            return Err(invalid());
        }
        let between: Vec<_> = events
            .iter()
            .copied()
            .filter(|candidate| {
                candidate.seq.get() > receipt.previous_last_event_seq && candidate.seq < event.seq
            })
            .collect();
        // Approval records permission only; execution belongs to the following
        // segment. Candidate verification has its separate runtime contract.
        if matches!(receipt.command.action, ResumeAction::Approve { .. })
            || matches!(
                wait.target,
                WaitTarget::Approval {
                    target: ApprovalTarget::Candidate { .. }
                }
            )
        {
            if !between.is_empty() {
                return Err(invalid());
            }
            if let WaitTarget::Approval {
                target:
                    ApprovalTarget::Tool {
                        call_id,
                        binding_digest,
                    },
            } = &wait.target
            {
                validate_resume_binding(state, additions, snapshot, call_id, binding_digest)?;
            }
            continue;
        }
        if receipt.expired && between.is_empty() {
            continue;
        }
        let [settled] = between.as_slice() else {
            return Err(invalid());
        };
        let RunEventPayload::ToolSettled { result_ref } = &settled.payload else {
            return Err(invalid());
        };
        let result: ToolResult = event_record(state, additions, result_ref)?;
        if !snapshot.tool_ledger.iter().any(|entry| {
            matches!(&entry.state,
            ToolCallState::Settled { result: current } if current == &result)
        }) {
            return Err(invalid());
        }
        match (&receipt.command.action, &wait.target) {
            (ResumeAction::Input { answer, .. }, WaitTarget::Input { request }) => {
                if result.call_id != request.call_id
                    || result.status != crate::ToolResultStatus::Succeeded
                    || result.effect != crate::ToolEffect::NotApplied
                    || result.content
                        != [crate::InputContent::Json {
                            value: answer.clone(),
                        }]
                    || result.error.is_some()
                    || result.effect_receipt_ref.is_some()
                {
                    return Err(invalid());
                }
                validate_resume_result_message(snapshot, messages, &result)?;
            }
            (
                ResumeAction::Deny { .. },
                WaitTarget::Approval {
                    target:
                        ApprovalTarget::Tool {
                            call_id,
                            binding_digest,
                        },
                },
            ) => {
                validate_resume_binding(state, additions, snapshot, call_id, binding_digest)?;
                if &result.call_id != call_id
                    || result.status != crate::ToolResultStatus::Denied
                    || result.effect != crate::ToolEffect::NotApplied
                    || !result.content.is_empty()
                    || result.effect_receipt_ref.is_some()
                {
                    return Err(invalid());
                }
                validate_resume_result_message(snapshot, messages, &result)?;
            }
            (
                ResumeAction::External { receipt_ref, .. },
                WaitTarget::External {
                    call_id,
                    effect_key,
                },
            ) => {
                record_value(state, additions, receipt_ref)?;
                if &result.call_id != call_id
                    || result.effect == crate::ToolEffect::Unknown
                    || result.status == crate::ToolResultStatus::Unknown
                {
                    return Err(invalid());
                }
                let unknown_event = events.iter().rev().find(|candidate| {
                    candidate.seq.get() < receipt.previous_last_event_seq
                        && matches!(&candidate.payload, RunEventPayload::ToolUnresolved { result_ref, idempotency_key, .. }
                            if idempotency_key == effect_key && event_record::<ToolResult>(state, additions, result_ref)
                                .is_ok_and(|unknown| &unknown.call_id == call_id))
                }).ok_or_else(invalid)?;
                let RunEventPayload::ToolUnresolved { result_ref, .. } = &unknown_event.payload
                else {
                    unreachable!()
                };
                let unknown: ToolResult = event_record(state, additions, result_ref)?;
                let digest =
                    canonical_digest(&serde_json::to_value(&unknown).map_err(|_| invalid())?);
                let matching: Vec<_> = messages.iter().filter(|message| {
                    message.run_id == snapshot.run_id && matches!(message.content.as_slice(),
                        [ContentBlock::ToolResultCorrection { previous_message_id, previous_result_digest, result: corrected }]
                        if corrected == &result && previous_result_digest == &digest
                            && messages.iter().any(|prior| prior.message_id == *previous_message_id
                                && prior.run_id == snapshot.run_id && matches!(prior.content.as_slice(),
                                    [ContentBlock::ToolResult { result: previous }] if previous == &unknown)))
                }).collect();
                let [correction] = matching.as_slice() else {
                    return Err(invalid());
                };
                if !authorized_corrections.insert(correction.message_id.clone())
                    || !corrected_calls.insert(call_id.clone())
                {
                    return Err(invalid());
                }
            }
            _ => return Err(invalid()),
        }
    }
    for message in messages
        .iter()
        .filter(|message| message.run_id == snapshot.run_id)
    {
        if message
            .content
            .iter()
            .any(|content| matches!(content, ContentBlock::ToolResultCorrection { .. }))
            && !authorized_corrections.contains(&message.message_id)
        {
            return Err(invalid());
        }
    }
    for event in events {
        if let RunEventPayload::ToolUnresolved { result_ref, .. } = &event.payload {
            let unknown: ToolResult = event_record(state, additions, result_ref)?;
            if snapshot.tool_ledger.iter().any(|entry| {
                entry.call.call_id == unknown.call_id
                    && matches!(entry.state, ToolCallState::Settled { .. })
            }) && !corrected_calls.contains(&unknown.call_id)
            {
                return Err(invalid());
            }
        }
    }
    Ok(())
}

fn validate_resume_binding(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    call_id: &Id,
    binding_digest: &crate::JsonDigest,
) -> Result<(), ContractError> {
    let invalid = || error(ErrorCode::InvalidSnapshot, "resume.binding");
    let reference = snapshot
        .tool_ledger
        .iter()
        .find(|entry| &entry.call.call_id == call_id)
        .and_then(|entry| entry.call.bound_input_ref.as_ref())
        .ok_or_else(invalid)?;
    // validate_snapshot_refs already validates this typed protected binding.
    if record_value(state, additions, reference)?.get("binding_digest")
        != Some(&serde_json::to_value(binding_digest).map_err(|_| invalid())?)
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_resume_result_message(
    snapshot: &RunSnapshot,
    messages: &[&Message],
    result: &ToolResult,
) -> Result<(), ContractError> {
    let count = messages.iter().filter(|message| message.run_id == snapshot.run_id
        && message.role == crate::MessageRole::Tool && message.origin == crate::MessageOrigin::Tool
        && matches!(message.content.as_slice(), [ContentBlock::ToolResult { result: saved }] if saved == result)).count();
    if count != 1 {
        return Err(error(ErrorCode::InvalidSnapshot, "resume.result_message"));
    }
    Ok(())
}

fn resume_target_matches(previous: &RunSnapshot, action: &ResumeAction) -> bool {
    if let ResumeAction::Recover { .. } = action {
        return previous.status == RunStatus::Running;
    }
    previous
        .wait
        .as_ref()
        .is_some_and(|wait| action_matches_wait(wait, action))
}

fn action_matches_wait(wait: &WaitState, action: &ResumeAction) -> bool {
    match action {
        ResumeAction::Recover { .. } => false,
        ResumeAction::Approve { wait_id, target }
        | ResumeAction::Deny {
            wait_id, target, ..
        } => {
            &wait.wait_id == wait_id
                && matches!(&wait.target, WaitTarget::Approval { target: saved } if saved == target)
        }
        ResumeAction::Input { wait_id, .. } => {
            &wait.wait_id == wait_id && matches!(wait.target, WaitTarget::Input { .. })
        }
        ResumeAction::External { wait_id, .. } => {
            &wait.wait_id == wait_id && matches!(wait.target, WaitTarget::External { .. })
        }
    }
}

fn validate_transition(previous: &RunSnapshot, next: &RunSnapshot) -> Result<(), ContractError> {
    if previous.status.is_terminal() {
        return Err(error(ErrorCode::InvalidTransition, "run.status"));
    }
    next.validate()?;
    validate_hook_transition(previous, next)?;
    validate_source_transition(previous, next)?;
    if previous.skill_plan_ref != next.skill_plan_ref {
        return Err(error(ErrorCode::InvalidTransition, "skills.immutable_plan"));
    }
    crate::budget::validate_budget_transition(previous, next)?;
    if !next.resume_receipts.starts_with(&previous.resume_receipts)
        || next.resume_receipts.len() > previous.resume_receipts.len() + 1
    {
        return Err(error(ErrorCode::InvalidTransition, "resume_receipts"));
    }
    let resumed = next.resume_receipts.len() != previous.resume_receipts.len();
    if resumed {
        let receipt = next.resume_receipts.last().expect("new receipt");
        if previous.status != RunStatus::Waiting
            || next.status != RunStatus::Running
            || next.outcome.is_some()
            || next.wait.is_some()
            || receipt.accepted_revision != next.revision
            || receipt.command.expected_revision != previous.revision
            || receipt.previous_last_event_seq != previous.last_event_seq
            || !resume_target_matches(previous, &receipt.command.action)
        {
            return Err(error(
                ErrorCode::InvalidTransition,
                "resume_receipts.acceptance",
            ));
        }
    } else if previous.status == RunStatus::Waiting && next.status == RunStatus::Running {
        return Err(error(
            ErrorCode::InvalidTransition,
            "resume_receipts.missing",
        ));
    }
    if previous.run_id != next.run_id
        || previous.request != next.request
        || previous.request_digest != next.request_digest
        || previous.scope != next.scope
        || previous.system_inputs != next.system_inputs
        || previous.limits != next.limits
        || next.revision
            != previous
                .revision
                .checked_add(1)
                .ok_or_else(|| error(ErrorCode::RevisionConflict, "revision"))?
        || previous.assembly_ref != next.assembly_ref
        || (previous.routing_snapshot_ref.is_some()
            && previous.routing_snapshot_ref != next.routing_snapshot_ref)
        || (previous.routing_snapshot_ref.is_none()
            && next.routing_snapshot_ref.is_some()
            && previous.usage.model_calls != 0)
    {
        return Err(error(
            ErrorCode::InvalidTransition,
            "snapshot.immutable_fields",
        ));
    }
    if previous.profile != next.profile {
        return Err(error(ErrorCode::ProfileMismatch, "snapshot.profile"));
    }
    if previous.tool_ledger.len() > next.tool_ledger.len() {
        return Err(error(ErrorCode::InvalidTransition, "tool_ledger"));
    }
    for (old, new) in previous.tool_ledger.iter().zip(&next.tool_ledger) {
        let mut call = old.call.clone();
        if call.bound_input_ref.is_none() {
            call.bound_input_ref = new.call.bound_input_ref.clone();
        }
        if call != new.call
            || (matches!(old.state, ToolCallState::Settled { .. }) && old != new)
            || (matches!(
                old.state,
                ToolCallState::Dispatching { .. }
                    | ToolCallState::Unknown { .. }
                    | ToolCallState::ApprovalPending { .. }
                    | ToolCallState::InputPending { .. }
            ) && matches!(new.state, ToolCallState::Planned { .. }))
            || (matches!(old.state, ToolCallState::Unknown { .. })
                && matches!(new.state, ToolCallState::ApprovalPending { .. }))
            || (matches!(old.state, ToolCallState::ApprovalPending { .. })
                && matches!(new.state, ToolCallState::Unknown { .. }))
            || (matches!(old.state, ToolCallState::InputPending { .. })
                && !matches!(
                    new.state,
                    ToolCallState::InputPending { .. } | ToolCallState::Settled { .. }
                ))
            || (matches!(new.state, ToolCallState::InputPending { .. })
                && !matches!(
                    old.state,
                    ToolCallState::Dispatching { .. } | ToolCallState::InputPending { .. }
                ))
        {
            return Err(error(ErrorCode::InvalidTransition, "tool_ledger"));
        }
        if let (
            ToolCallState::Dispatching {
                attempt_id: old_attempt,
                idempotency_key: old_key,
            }
            | ToolCallState::Unknown {
                attempt_id: old_attempt,
                idempotency_key: old_key,
            }
            | ToolCallState::ApprovalPending {
                attempt_id: old_attempt,
                idempotency_key: old_key,
            }
            | ToolCallState::InputPending {
                attempt_id: old_attempt,
                idempotency_key: old_key,
                ..
            },
            ToolCallState::Dispatching {
                attempt_id: new_attempt,
                idempotency_key: new_key,
            }
            | ToolCallState::Unknown {
                attempt_id: new_attempt,
                idempotency_key: new_key,
            }
            | ToolCallState::ApprovalPending {
                attempt_id: new_attempt,
                idempotency_key: new_key,
            }
            | ToolCallState::InputPending {
                attempt_id: new_attempt,
                idempotency_key: new_key,
                ..
            },
        ) = (&old.state, &new.state)
        {
            let retry = matches!(
                old.state,
                ToolCallState::Unknown { .. } | ToolCallState::ApprovalPending { .. }
            ) && matches!(new.state, ToolCallState::Dispatching { .. });
            if old_key != new_key || (!retry && old_attempt != new_attempt) {
                return Err(error(ErrorCode::InvalidTransition, "tool_ledger.attempt"));
            }
        }
        if let (
            ToolCallState::InputPending {
                request: before, ..
            },
            ToolCallState::InputPending { request: after, .. },
        ) = (&old.state, &new.state)
        {
            if before != after {
                return Err(error(
                    ErrorCode::InvalidTransition,
                    "tool_ledger.input_request",
                ));
            }
        }
    }
    if previous.model_ledger.len() > next.model_ledger.len()
        || previous
            .model_ledger
            .iter()
            .zip(&next.model_ledger)
            .any(|(old, new)| {
                old.run_id != new.run_id
                    || old.model_step_id != new.model_step_id
                    || old.attempt_id != new.attempt_id
                    || old.purpose != new.purpose
                    || old.route != new.route
                    || old.selection_reason != new.selection_reason
                    || old.request_digest != new.request_digest
                    || old.inspection_ref != new.inspection_ref
                    || (matches!(
                        old.state,
                        ModelAttemptState::Completed {}
                            | ModelAttemptState::Failed { .. }
                            | ModelAttemptState::Interrupted { .. }
                    ) && old != new)
                    || (matches!(old.state, ModelAttemptState::Unknown {})
                        && matches!(new.state, ModelAttemptState::Reserved {}))
            })
    {
        return Err(error(ErrorCode::InvalidTransition, "model_ledger"));
    }
    let old = &previous.usage;
    let new = &next.usage;
    if new.model_calls < old.model_calls
        || new.tool_attempts < old.tool_attempts
        || new.repair_attempts < old.repair_attempts
        || new.recovery_attempts < old.recovery_attempts
        || new.elapsed_ms < old.elapsed_ms
    {
        return Err(error(ErrorCode::InvalidTransition, "usage"));
    }
    Ok(())
}

impl MemoryStateStore {
    fn acquire_lease_now(
        &self,
        scope: &Scope,
        run_id: &Id,
        owner: &Id,
        now_ms: i64,
        ttl_ms: u64,
    ) -> Result<RunLease, ContractError> {
        let expires_at_ms = lease_expiry(now_ms, ttl_ms)?;
        let mut scopes = self.lock()?;
        let run = run_mut(&mut scopes, scope, run_id)?;
        if run.snapshot.status.is_terminal() {
            return Err(error(ErrorCode::InvalidTransition, "run.status"));
        }
        if run.lease.as_ref().is_some_and(|l| l.expires_at_ms > now_ms) {
            return Err(error(ErrorCode::LeaseBusy, "lease"));
        }
        let fencing_token = run
            .last_fencing_token
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::InvalidContract, "lease.fencing_token"))?;
        let lease = RunLease {
            scope: scope.clone(),
            run_id: run_id.clone(),
            owner: owner.clone(),
            fencing_token,
            expires_at_ms,
        };
        run.last_fencing_token = fencing_token;
        run.lease = Some(lease.clone());
        Ok(lease)
    }
}

impl MemoryStateStore {
    fn commit_now(
        &self,
        scope: &Scope,
        run_id: &Id,
        input: CommitInput,
        segment_override: Option<&Id>,
    ) -> Result<StoredRun, ContractError> {
        check_scope(scope, &input.snapshot.scope)?;
        let mut scopes = self.lock()?;
        let state = namespace(&scopes, scope)?;
        let run = state.runs.get(run_id).ok_or_else(not_found)?;
        validate_lease(run, scope, run_id, &input.lease, input.now_ms)?;
        if run.snapshot.revision != input.expected_revision {
            return Err(error(ErrorCode::RevisionConflict, "revision"));
        }
        validate_transition(&run.snapshot, &input.snapshot)?;
        recovery_state::transition(&run.snapshot, &input.snapshot, &input.events)?;
        if input.events.iter().any(|event| {
            matches!(
                event.payload,
                RunEventPayload::RunResumed { .. } | RunEventPayload::RunRecovered { .. }
            ) && event.timestamp_ms > input.now_ms
        }) {
            return Err(error(ErrorCode::InvalidEvent, "events.resume_time"));
        }
        if let Some(receipt) = input
            .snapshot
            .resume_receipts
            .last()
            .filter(|receipt| receipt.accepted_revision == input.snapshot.revision)
        {
            let expired = input.now_ms >= run.snapshot.timing.deadline_at_ms
                || run
                    .snapshot
                    .wait
                    .as_ref()
                    .and_then(|wait| wait.expires_at_ms)
                    .is_some_and(|deadline| input.now_ms >= deadline);
            // A durable adapter may advance the lease-check time after
            // queue/lock delay. Crossing expiry must not turn a stale
            // on-time decision into an accepted approval.
            if receipt.expired != expired {
                return Err(error(
                    ErrorCode::DeadlineExceeded,
                    "resume.acceptance_expiry",
                ));
            }
        }
        if let Some(receipt) = input
            .snapshot
            .recovery_receipts
            .last()
            .filter(|receipt| receipt.accepted_revision == input.snapshot.revision)
        {
            if receipt.expired != (input.now_ms >= run.snapshot.timing.deadline_at_ms) {
                return Err(error(
                    ErrorCode::DeadlineExceeded,
                    "recovery.acceptance_expiry",
                ));
            }
        }
        let additions = validate_records(state, &input.records)?;
        validate_snapshot_refs(state, &additions, &input.snapshot)?;
        context_state::validate_update(&run.snapshot, &input.snapshot, &input.events)?;
        verification_state::transition(
            state,
            &additions,
            &run.snapshot,
            &input.snapshot,
            &input.messages,
            &input.events,
        )?;
        validate_events(
            state,
            &additions,
            &input.snapshot,
            run.snapshot.last_event_seq,
            &input.events,
            false,
            &input.messages,
        )?;
        let session = state
            .sessions
            .get(&run.snapshot.request.session_id)
            .ok_or_else(not_found)?;
        if session.snapshot.active_run_id.as_ref() != Some(run_id) {
            return Err(error(ErrorCode::InvalidTransition, "session.active_run_id"));
        }
        let transcript_revision = validate_messages(
            state,
            &additions,
            run_id,
            session.snapshot.transcript_revision,
            &input.messages,
        )?;
        let mut session_snapshot = session.snapshot.clone();
        let history: Vec<_> = run.events.iter().chain(&input.events).collect();
        let transcript: Vec<_> = session.messages.iter().chain(&input.messages).collect();
        validate_resume_history(state, &additions, &input.snapshot, &history, &transcript)?;
        session_snapshot.transcript_revision = transcript_revision;
        session_snapshot.context_revision_ref = input.snapshot.context_revision_ref.clone();
        if input.snapshot.status.is_terminal() {
            session_snapshot.active_run_id = None;
        }
        let mut messages = session.messages.clone();
        messages.extend(input.messages);
        let result = StoredRun {
            snapshot: input.snapshot.clone(),
            session: session_snapshot.clone(),
            messages: messages.clone(),
        };
        let execution = state
            .executions
            .get(run_id)
            .map(|history| {
                execution::advance_history(
                    history,
                    &run.snapshot,
                    &input.snapshot,
                    segment_override,
                )
            })
            .transpose()?;
        let state = scopes
            .get_mut(&scope_key(scope))
            .expect("validated namespace");
        if let Some(execution) = execution {
            state.executions.insert(run_id.clone(), execution);
        }

        state.records.extend(additions);
        state
            .message_ids
            .extend(messages.iter().map(|m| m.message_id.clone()));
        state
            .event_ids
            .extend(input.events.iter().map(|e| e.event_id.clone()));
        state.sessions.insert(
            session_snapshot.session_id.clone(),
            SessionState {
                snapshot: session_snapshot,
                messages,
            },
        );
        let run = state.runs.get_mut(run_id).expect("validated run");
        run.snapshot = input.snapshot;
        run.events.extend(input.events);
        if run.snapshot.status.is_terminal() {
            run.lease = None;
        }
        Ok(result)
    }
}
```

## `crates/wickle/tests/agent_runtime.rs`

```rust
//! Agent lifecycle, saved outcomes, request identity, and detached execution.

#[path = "support/agent.rs"]
#[allow(dead_code)]
mod support;
use futures_util::StreamExt;
use std::sync::atomic::Ordering;
use support::*;
use wickle::*;

#[test]
fn starting_without_a_tokio_runtime_returns_a_typed_error_before_callbacks() {
    use std::{future::Future, task::Context};
    let fixture = Fixture::new(Response::Text, false);
    let agent = fixture.agent();
    let future = agent.start(request("request"), context());
    let mut future = std::pin::pin!(future);
    let waker = futures_util::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let result = future.as_mut().poll(&mut context);
    assert!(matches!(
        result,
        std::task::Poll::Ready(Err(ContractError {
            code: ErrorCode::RuntimeUnavailable,
            ..
        }))
    ));
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropping_a_polled_start_future_does_not_abort_its_owned_admission_or_driver() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    {
        let start = agent.start(request("request"), context());
        tokio::pin!(start);
        assert!(futures_util::poll!(start.as_mut()).is_pending());
    }
    fixture.model.entered.notified().await;
    let saved = fixture
        .store
        .find_request(&scope(), &id("session"), &id("request"))
        .await
        .unwrap()
        .unwrap();
    fixture.model.release.add_permits(1);
    let handle = fixture.started(&agent, "request").await;
    assert_eq!(handle.run_id(), &saved.snapshot.run_id);
    assert_eq!(
        completed(handle.outcome(&context()).await.unwrap())
            .result
            .status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn construction_does_not_resolve_metadata_authorize_generate_estimate_or_allocate_ids() {
    let fixture = Fixture::new(Response::Text, false);
    let _agent = fixture.agent();
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.policy.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.router.queries.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.router.snapshots.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.inspector.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.estimator.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.ids.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_text_turn_finishes_with_the_stored_outcome_as_authority() {
    let fixture = Fixture::new(Response::Text, false);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "candidate answer".into()
        }]
    );
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert_eq!(saved.snapshot.outcome, Some(outcome.clone()));
    assert_eq!(saved.snapshot.revision, outcome.checkpoint_revision);
    assert_eq!(saved.session.active_run_id, None);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let events = fixture
        .store
        .read_events(&scope(), handle.run_id(), 0, 100)
        .await
        .unwrap();
    assert!(matches!(
        events.events.last().unwrap().payload,
        RunEventPayload::RunFinished { .. }
    ));
    let view = completed(agent.get_run(handle.run_id(), &context()).await.unwrap());
    assert_eq!(view.status, RunStatus::Succeeded);
}

#[tokio::test]
async fn dropping_the_handle_outcome_waiter_and_event_stream_does_not_cancel_the_driver() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let run_id = handle.run_id().clone();
    {
        let context = context();
        let outcome = handle.outcome(&context);
        tokio::pin!(outcome);
        assert!(futures_util::poll!(outcome.as_mut()).is_pending());
    }
    {
        let mut events = handle.events(0, context());
        let first = events.next().await.unwrap().unwrap();
        assert_eq!(first.run_id, run_id);
    }
    drop(handle);
    fixture.model.release.add_permits(1);
    let replay = completed(agent.start(request("request"), context()).await.unwrap());
    let outcome = completed(replay.outcome(&context()).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn duplicate_requests_reuse_the_run_before_new_metadata_resolution_and_changed_options_conflict()
 {
    let fixture = Fixture::new(Response::Text, false);
    let agent = fixture.agent();
    let first = fixture.started(&agent, "request").await;
    completed(first.outcome(&context()).await.unwrap());
    let resolutions = fixture.catalog.calls.load(Ordering::SeqCst);
    fixture.catalog.revision.store(99, Ordering::SeqCst);
    let duplicate = fixture.started(&agent, "request").await;
    assert_eq!(duplicate.run_id(), first.run_id());
    completed(duplicate.outcome(&context()).await.unwrap());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), resolutions);
    let mut changed = request("request");
    changed
        .model_options
        .insert("effort".into(), serde_json::json!("high"));
    assert_eq!(
        agent.start(changed, context()).await.unwrap_err().code,
        ErrorCode::RequestConflict
    );
}

#[tokio::test]
async fn a_second_request_is_busy_until_the_active_run_finishes() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let first = fixture.started(&agent, "first").await;
    fixture.model.entered.notified().await;
    assert_eq!(
        agent
            .start(request("second"), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::SessionBusy
    );
    fixture.model.release.add_permits(1);
    completed(first.outcome(&context()).await.unwrap());
    let original = fixture
        .store
        .load(&scope(), first.run_id())
        .await
        .unwrap()
        .session
        .prompt_snapshot;
    let second = fixture.started(&agent, "second").await;
    fixture.model.release.add_permits(1);
    completed(second.outcome(&context()).await.unwrap());
    assert_ne!(first.run_id(), second.run_id());
    assert_eq!(
        fixture
            .store
            .load(&scope(), second.run_id())
            .await
            .unwrap()
            .session
            .prompt_snapshot,
        original
    );
    let requests = fixture.model.requests.lock().unwrap();
    let systems = |request: &ModelRequest| {
        request
            .messages
            .iter()
            .filter(|message| message.role == ModelRole::System)
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(systems(&requests[0]), systems(&requests[1]));
}

#[tokio::test]
async fn foreign_scope_and_current_read_or_cancel_denials_do_not_control_an_existing_run() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let mut foreign = context();
    foreign.data.scope.tenant_id = id("foreign");
    assert!(agent.get_run(handle.run_id(), &foreign).await.is_err());
    assert!(handle.cancel(id("cancel"), &foreign).await.is_err());
    fixture.policy.deny.store(1, Ordering::SeqCst);
    assert!(agent.get_run(handle.run_id(), &context()).await.is_err());
    assert!(handle.outcome(&context()).await.is_err());
    fixture.policy.deny.store(2, Ordering::SeqCst);
    assert!(handle.cancel(id("cancel"), &context()).await.is_err());
    fixture.policy.deny.store(0, Ordering::SeqCst);
    fixture.model.release.add_permits(1);
    assert_eq!(
        completed(handle.outcome(&context()).await.unwrap())
            .result
            .status(),
        RunStatus::Succeeded
    );
}

#[tokio::test]
async fn cancelling_a_running_request_preserves_its_reserved_attempt_and_saves_cancellation() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let receipt = completed(
        handle
            .cancel(id("user_cancelled"), &context())
            .await
            .unwrap(),
    );
    assert_eq!(receipt, CancelReceipt::Requested);
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Cancelled);
    assert_eq!(outcome.usage.model_calls, 1);
    let saved = fixture
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap()
        .snapshot;
    assert_eq!(saved.status, RunStatus::Cancelled);
    assert_eq!(saved.reservations.len(), 1);
    assert_eq!(
        completed(handle.cancel(id("again"), &context()).await.unwrap()),
        CancelReceipt::AlreadyTerminal
    );
}

#[tokio::test]
async fn classified_model_failure_is_saved_with_its_partial_output_instead_of_success() {
    let fixture = Fixture::new(Response::TransportFailure, false);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Failed);
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "candidate answer".into()
        }]
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .outcome,
        Some(outcome)
    );
}

#[tokio::test]
async fn unsupported_profile_modes_are_rejected_before_resolution_or_model_calls() {
    let fixture = Fixture::new(Response::Text, false);
    let mut verified = profile();
    verified.completion_policy = CompletionPolicy::Verified {
        verifier_ref: reference("verifier"),
    };
    assert!(
        create_agent(verified, fixture.bindings())
            .unwrap()
            .start(request("request"), context())
            .await
            .is_err()
    );
    let mut tools = profile();
    tools.tools = vec![ToolBindingRef::Catalog(CatalogToolRef {
        tool_id: id("search"),
        version: id("1"),
        bindings: None,
        config: None,
    })];
    assert!(
        create_agent(tools, fixture.bindings())
            .unwrap()
            .start(request("request"), context())
            .await
            .is_err()
    );
    let mut hooks = profile();
    hooks.hooks = Some(vec![HookRef::Catalog(CatalogHookRef {
        hook_id: id("hook"),
        version: id("1"),
        position: HookPosition::BeforeModel,
    })]);
    assert_eq!(
        create_agent(hooks, fixture.bindings())
            .unwrap()
            .start(request("request"), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
    let mut sources = profile();
    sources.context_sources = Some(vec![ContextSourceBinding {
        source: ContextSourceRef::Catalog(CatalogSourceRef {
            source_id: id("source"),
            version: id("1"),
        }),
        trigger: ContextTrigger::RunStart,
        required: true,
        timeout_ms: 1000.try_into().unwrap(),
        max_items: 1.try_into().unwrap(),
        max_bytes: 1024.try_into().unwrap(),
        max_tokens: 128.try_into().unwrap(),
    }]);
    assert_eq!(
        create_agent(sources, fixture.bindings())
            .unwrap()
            .start(request("request"), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_changed_profile_cannot_replace_a_completed_sessions_pinned_prompt() {
    let fixture = Fixture::new(Response::Text, false);
    let agent = fixture.agent();
    let first = fixture.started(&agent, "first").await;
    completed(first.outcome(&context()).await.unwrap());
    let mut changed = profile();
    changed.version = id("2.0.0");
    let other = create_agent(changed, fixture.bindings()).unwrap();
    assert!(other.start(request("second"), context()).await.is_err());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert!(
        fixture
            .store
            .find_request(&scope(), &id("session"), &id("second"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn cancellation_receipt_is_not_a_terminal_outcome_until_the_final_commit_succeeds() {
    let fixture = Fixture::new(Response::Text, true);
    let store = std::sync::Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::Pause,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    assert_eq!(
        completed(handle.cancel(id("cancel"), &context()).await.unwrap()),
        CancelReceipt::Requested
    );
    store.final_entered.notified().await;
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert_eq!(saved.snapshot.status, RunStatus::Running);
    assert!(saved.snapshot.outcome.is_none());
    assert!(
        !fixture
            .store
            .read_events(&scope(), handle.run_id(), 0, 100)
            .await
            .unwrap()
            .events
            .iter()
            .any(|event| matches!(event.payload, RunEventPayload::RunFinished { .. }))
    );
    store.release.add_permits(1);
    assert_eq!(
        completed(handle.outcome(&context()).await.unwrap())
            .result
            .status(),
        RunStatus::Cancelled
    );
}

#[tokio::test]
async fn failed_final_storage_never_reports_a_successful_outcome_or_finished_event() {
    let fixture = Fixture::new(Response::Text, false);
    let store = std::sync::Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::Reject,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    assert_eq!(
        handle.outcome(&context()).await.unwrap_err().code,
        ErrorCode::PersistenceUnavailable
    );
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert!(saved.snapshot.outcome.is_none());
    assert!(!saved.snapshot.status.is_terminal());
    assert!(
        !fixture
            .store
            .read_events(&scope(), handle.run_id(), 0, 100)
            .await
            .unwrap()
            .events
            .iter()
            .any(|event| matches!(event.payload, RunEventPayload::RunFinished { .. }))
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_lost_final_commit_ack_is_resolved_from_stored_success_without_reexecuting_the_model() {
    let fixture = Fixture::new(Response::Text, false);
    let store = std::sync::Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::LoseAcknowledgement,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .outcome,
        Some(outcome)
    );
    let duplicate = fixture.started(&agent, "request").await;
    assert_eq!(duplicate.run_id(), handle.run_id());
    completed(duplicate.outcome(&context()).await.unwrap());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.final_attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn heartbeat_keeps_a_long_running_model_attempt_owned_beyond_the_original_lease() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    for _ in 0..15 {
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
    }
    let now = fixture.clock.now().unwrap().utc_ms;
    assert_eq!(
        fixture
            .store
            .acquire_lease(&scope(), handle.run_id(), &id("competitor"), now, 1000)
            .await
            .unwrap_err()
            .code,
        ErrorCode::LeaseBusy
    );
    fixture.model.release.add_permits(1);
    assert_eq!(
        completed(handle.outcome(&context()).await.unwrap())
            .result
            .status(),
        RunStatus::Succeeded
    );
}

#[tokio::test(start_paused = true)]
async fn deadline_exhaustion_stops_an_incomplete_stream_and_preserves_the_attempt() {
    let fixture = Fixture::new(Response::WaitAfterText, false);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    );
    assert_eq!(outcome.usage.model_calls, 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .status,
        RunStatus::Exhausted
    );
}

#[tokio::test]
async fn impossible_token_estimates_fail_before_model_dispatch() {
    let fixture = Fixture::new(Response::Text, false);
    fixture.estimator.tokens.store(8192, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_ne!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(outcome.usage.model_calls, 0);
}

#[tokio::test]
async fn buffered_events_recheck_current_permission_and_observer_cancellation_before_delivery() {
    for cancel in [false, true] {
        let fixture = Fixture::new(Response::Text, true);
        let agent = fixture.agent();
        let handle = fixture.started(&agent, "request").await;
        fixture.model.entered.notified().await;
        let observer = context();
        let mut events = handle.events(0, observer.clone());
        let first = events.next().await.unwrap().unwrap();
        assert_eq!(first.event_type, "run.started");
        if cancel {
            observer.cancellation.cancel();
        } else {
            fixture.policy.deny.store(1, Ordering::SeqCst);
        }
        let second = events
            .next()
            .await
            .expect("observer receives a denial, not an event");
        assert_eq!(
            second.unwrap_err().code,
            if cancel {
                ErrorCode::Cancelled
            } else {
                ErrorCode::AccessDenied
            }
        );
        fixture.policy.deny.store(0, Ordering::SeqCst);
        fixture.model.release.add_permits(1);
        assert_eq!(
            completed(handle.outcome(&context()).await.unwrap())
                .result
                .status(),
            RunStatus::Succeeded
        );
    }
}

#[tokio::test]
async fn an_event_committed_between_empty_page_and_terminal_read_is_still_delivered() {
    let fixture = Fixture::new(Response::Text, true);
    let store = std::sync::Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::PauseEmptyEventPage,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let before = fixture
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap()
        .snapshot
        .last_event_seq;
    let mut events = handle.events(before, context());
    let next = tokio::spawn(async move { events.next().await });
    store.empty_page_entered.notified().await;
    fixture.model.release.add_permits(1);
    completed(handle.outcome(&context()).await.unwrap());
    let terminal = fixture
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap()
        .snapshot;
    assert!(terminal.last_event_seq > before);
    store.empty_page_release.add_permits(1);
    let delivered = next
        .await
        .unwrap()
        .expect("final durable event must not be lost")
        .unwrap();
    assert_eq!(delivered.event_type, "run.finished");
    assert_eq!(delivered.seq.get(), terminal.last_event_seq);
}

#[tokio::test]
async fn concurrent_duplicate_starts_share_one_run_and_one_model_attempt() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let (first, second) = tokio::join!(
        agent.start(request("request"), context()),
        agent.start(request("request"), context())
    );
    let first = completed(first.unwrap());
    let second = completed(second.unwrap());
    assert_eq!(first.run_id(), second.run_id());
    fixture.model.entered.notified().await;
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    fixture.model.release.add_permits(1);
    let observer = context();
    let (first_outcome, second_outcome) =
        tokio::join!(first.outcome(&observer), second.outcome(&observer));
    assert_eq!(
        completed(first_outcome.unwrap()),
        completed(second_outcome.unwrap())
    );
}

#[tokio::test]
async fn adapter_panic_fails_and_repeated_unknown_tools_exhaust_without_tool_dispatch() {
    for (response, expected_status, expected_calls) in [
        (Response::Panic, RunStatus::Failed, 1),
        (Response::Tool, RunStatus::Exhausted, 4),
    ] {
        let fixture = Fixture::new(response, false);
        let agent = fixture.agent();
        let handle = fixture.started(&agent, "request").await;
        let outcome = completed(handle.outcome(&context()).await.unwrap());
        assert_eq!(outcome.result.status(), expected_status);
        assert_eq!(outcome.usage.model_calls, expected_calls);
        assert_eq!(outcome.usage.tool_attempts, 0);
        assert_eq!(
            fixture.model.calls.load(Ordering::SeqCst),
            expected_calls as usize
        );
        assert!(
            fixture
                .store
                .load(&scope(), handle.run_id())
                .await
                .unwrap()
                .session
                .active_run_id
                .is_none()
        );
    }
}

#[tokio::test]
async fn a_start_policy_denial_admits_no_run_and_calls_no_resolver_or_model() {
    let fixture = Fixture::new(Response::Text, false);
    fixture.policy.deny.store(3, Ordering::SeqCst);
    let agent = fixture.agent();
    assert_eq!(
        agent
            .start(request("request"), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert!(
        fixture
            .store
            .find_request(&scope(), &id("session"), &id("request"))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_later_run_replays_the_exact_committed_continuation_once_on_the_original_route() {
    let fixture = Fixture::new(Response::WithContinuation, false);
    let agent = fixture.agent();
    let first = fixture.started(&agent, "first").await;
    completed(first.outcome(&context()).await.unwrap());
    let saved = fixture.store.load(&scope(), first.run_id()).await.unwrap();
    let opaque: Vec<_> = saved
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            ContentBlock::ProviderOpaque {
                provider,
                route_digest,
                data_ref,
            } => Some((provider, route_digest, data_ref)),
            _ => None,
        })
        .collect();
    assert_eq!(opaque.len(), 1);
    assert_eq!(opaque[0].0, &id("fixture"));
    let record = fixture
        .store
        .read_record(&scope(), opaque[0].2)
        .await
        .unwrap();
    let expected: OpaqueContinuation = serde_json::from_value(record.value().clone()).unwrap();
    assert_eq!(
        expected.data(),
        &serde_json::json!({"signature":"fixture-signature"})
    );
    assert_eq!(expected.route_digest(), opaque[0].1);
    let second = fixture.started(&agent, "second").await;
    completed(second.outcome(&context()).await.unwrap());
    let requests = fixture.model.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let replayed: Vec<_> = requests[1]
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            ModelContent::Opaque { continuation } => Some(continuation),
            _ => None,
        })
        .collect();
    assert_eq!(replayed, vec![&expected]);
    assert_eq!(replayed[0].route_digest(), &requests[1].route.digest());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn changing_provider_for_the_same_session_does_not_forward_or_silently_discard_opaque_state()
{
    let fixture = Fixture::new(Response::WithContinuation, false);
    let agent = fixture.agent();
    let first = fixture.started(&agent, "first").await;
    completed(first.outcome(&context()).await.unwrap());
    let mut other = Fixture::new(Response::Text, false);
    other.store = fixture.store.clone();
    other.ids = fixture.ids.clone();
    other.clock = fixture.clock.clone();
    other.router = std::sync::Arc::new(Router::for_provider("different-provider"));
    let selected = other
        .router
        .snapshot
        .route_for_binding(&reference("route"))
        .unwrap();
    let mut port = Model::new(Response::Text, false);
    port.port_binding = ModelPortBinding {
        provider: selected.provider.clone(),
        adapter: selected.adapter.clone(),
        connection_ref: selected.connection_ref.clone(),
    };
    other.model = std::sync::Arc::new(port);
    let other_agent = other.agent();
    let second = other.started(&other_agent, "second").await;
    let outcome = completed(second.outcome(&context()).await.unwrap());
    assert!(
        matches!(outcome.result,OutcomeResult::Failed{failure} if failure.code==id("model_context_incompatible"))
    );
    assert_eq!(other.model.calls.load(Ordering::SeqCst), 0);
    assert!(other.model.requests.lock().unwrap().is_empty());
    // A clean session proves the second provider configuration is usable: the
    // earlier rejection is caused by incompatible continuation, not its binding.
    let mut clean = request("clean-request");
    clean.session_id = id("clean-session");
    let clean = completed(other_agent.start(clean, context()).await.unwrap());
    assert_eq!(
        completed(clean.outcome(&context()).await.unwrap())
            .result
            .status(),
        RunStatus::Succeeded
    );
    assert_eq!(other.model.calls.load(Ordering::SeqCst), 1);
    assert!(
        !other.model.requests.lock().unwrap()[0]
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .any(|content| matches!(content, ModelContent::Opaque { .. }))
    );
}

#[tokio::test]
async fn start_replay_treats_omitted_system_inputs_as_empty_instead_of_reusing_saved_values() {
    let fixture = Fixture::new(Response::Text, false);
    let mut bindings = fixture.bindings();
    bindings.system_inputs = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: serde_json::json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    let agent = create_agent(profile(), bindings).unwrap();
    let mut supplied = context();
    supplied.data.system_inputs = Some(SystemInputs::new(JsonObject::from([(
        "workspace_id".into(),
        serde_json::json!("11111111-1111-4111-8111-111111111111"),
    )])));
    let first = completed(
        agent
            .start(request("with-inputs"), supplied.clone())
            .await
            .unwrap(),
    );
    completed(first.outcome(&context()).await.unwrap());
    let before = fixture.catalog.calls.load(Ordering::SeqCst);
    let omitted = agent
        .start(request("with-inputs"), context())
        .await
        .unwrap_err();
    assert!(matches!(
        omitted.code,
        ErrorCode::SystemInputsMismatch | ErrorCode::RequestConflict
    ));
    let same = completed(agent.start(request("with-inputs"), supplied).await.unwrap());
    assert_eq!(same.run_id(), first.run_id());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), before);

    let mut empty_request = request("empty-inputs");
    empty_request.session_id = id("empty-session");
    let empty = completed(agent.start(empty_request.clone(), context()).await.unwrap());
    completed(empty.outcome(&context()).await.unwrap());
    let mut explicit_empty = context();
    explicit_empty.data.system_inputs = Some(SystemInputs::default());
    let same_empty = completed(agent.start(empty_request, explicit_empty).await.unwrap());
    assert_eq!(same_empty.run_id(), empty.run_id());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn observer_cancellation_interrupts_pending_store_reads_without_cancelling_the_driver() {
    for operation in 0..3 {
        let fixture = Fixture::new(Response::Text, true);
        let store = std::sync::Arc::new(FinalCommitStore::new(
            fixture.store.clone(),
            FinalCommitMode::PassThrough,
        ));
        let mut bindings = fixture.bindings();
        bindings.state = store.clone();
        let agent = create_agent(profile(), bindings).unwrap();
        let handle = fixture.started(&agent, "request").await;
        fixture.model.entered.notified().await;
        let observer = context();
        store
            .block_read
            .store(if operation == 1 { 2 } else { 1 }, Ordering::SeqCst);
        let waiting_handle = handle.clone();
        let waiting_agent = agent.clone();
        let waiting_context = observer.clone();
        let waiting = tokio::spawn(async move {
            match operation {
                0 => waiting_handle.outcome(&waiting_context).await.map(|_| ()),
                1 => waiting_handle
                    .events(0, waiting_context)
                    .next()
                    .await
                    .expect("observer must report cancellation")
                    .map(|_| ()),
                _ => waiting_agent
                    .get_run(waiting_handle.run_id(), &waiting_context)
                    .await
                    .map(|_| ()),
            }
        });
        store.read_entered.notified().await;
        observer.cancellation.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .expect("cancel must interrupt the pending store read")
            .unwrap();
        assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
        fixture.model.release.add_permits(1);
        assert_eq!(
            completed(handle.outcome(&context()).await.unwrap())
                .result
                .status(),
            RunStatus::Succeeded
        );
        assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn initial_request_lookup_observes_cancellation_and_start_timeout_before_admission() {
    for cancel in [true, false] {
        let fixture = Fixture::new(Response::Text, false);
        let store = std::sync::Arc::new(FinalCommitStore::new(
            fixture.store.clone(),
            FinalCommitMode::PassThrough,
        ));
        store.block_read.store(3, Ordering::SeqCst);
        let mut bindings = fixture.bindings();
        bindings.state = store.clone();
        bindings.settings.start_timeout_ms = 30;
        let agent = create_agent(profile(), bindings).unwrap();
        let caller = context();
        let task_context = caller.clone();
        let start =
            tokio::spawn(async move { agent.start(request("request"), task_context).await });
        store.read_entered.notified().await;
        if cancel {
            caller.cancellation.cancel();
        }
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), start)
            .await
            .expect("request lookup must be bounded by start control")
            .unwrap();
        assert_eq!(
            result.unwrap_err().code,
            if cancel {
                ErrorCode::Cancelled
            } else {
                ErrorCode::DeadlineExceeded
            }
        );
        assert!(
            fixture
                .store
                .find_request(&scope(), &id("session"), &id("request"))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn exhausting_a_retry_budget_preserves_the_partial_response_already_saved_for_this_step() {
    let fixture = Fixture::new(Response::TransportFailure, false);
    let mut profile = profile();
    profile.limits.max_model_calls = 1.try_into().unwrap();
    profile.limits.max_recovery_attempts = 1;
    let gate = std::sync::Arc::new(
        PolicyGate::new(fixture.policy.clone(), std::time::Duration::from_secs(1)).unwrap(),
    );
    let exchange = ModelExchange::new(fixture.model.clone(), gate)
        .with_route_inspector(fixture.inspector.clone(), std::time::Duration::from_secs(1))
        .unwrap()
        .with_retry_policy(ModelRetryPolicy {
            max_retries: 1,
            backoff_ms: 0,
        });
    let mut bindings = fixture.bindings();
    bindings.model_exchange = std::sync::Arc::new(exchange);
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::ModelCalls
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "candidate answer".into()
        }]
    );
    assert_eq!(outcome.usage.model_calls, 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let saved = fixture
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap()
        .snapshot;
    assert_eq!(saved.outcome, Some(outcome));
    assert!(saved.model_ledger[0].response_ref.is_some());
}

#[tokio::test]
async fn another_session_finishes_while_the_first_run_waits_on_model_io() {
    use std::sync::{Arc, atomic::AtomicUsize};
    use std::time::Duration;
    struct Lanes {
        slow: Arc<support::Model>,
        fast: Arc<support::Model>,
        calls: AtomicUsize,
    }
    impl ModelPort for Lanes {
        fn binding(&self) -> ModelPortBinding {
            self.slow.binding()
        }
        fn generate<'a>(
            &'a self,
            request: &'a ModelRequest,
            context: &'a ModelCallContext,
        ) -> PortStream<'a, ModelEvent> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.slow.generate(request, context)
            } else {
                self.fast.generate(request, context)
            }
        }
    }
    let fixture = Fixture::new(Response::Text, false);
    let lanes = Arc::new(Lanes {
        slow: Arc::new(support::Model::new(Response::Text, true)),
        fast: Arc::new(support::Model::new(Response::Text, false)),
        calls: AtomicUsize::new(0),
    });
    let mut bindings = fixture.bindings();
    bindings.model_exchange = Arc::new(
        ModelExchange::new(lanes.clone(), bindings.policy.clone())
            .with_route_inspector(fixture.inspector.clone(), Duration::from_secs(1))
            .unwrap(),
    );
    let agent = create_agent(profile(), bindings).unwrap();
    let baseline = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    let mut slow_request = request("slow");
    slow_request.session_id = id("slow-session");
    let slow = completed(agent.start(slow_request, context()).await.unwrap());
    tokio::time::timeout(Duration::from_secs(2), lanes.slow.entered.notified())
        .await
        .unwrap();
    assert_eq!(lanes.slow.release.available_permits(), 0);
    let mut fast_request = request("fast");
    fast_request.session_id = id("fast-session");
    let fast = completed(agent.start(fast_request, context()).await.unwrap());
    let result = tokio::time::timeout(Duration::from_secs(2), fast.outcome(&context()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed(result).result.status(), RunStatus::Succeeded);
    assert_eq!(
        fixture
            .store
            .load(&scope(), slow.run_id())
            .await
            .unwrap()
            .snapshot
            .status,
        RunStatus::Running
    );
    assert_eq!(lanes.slow.release.available_permits(), 0);
    lanes.slow.release.add_permits(1);
    let result = tokio::time::timeout(Duration::from_secs(2), slow.outcome(&context()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed(result).result.status(), RunStatus::Succeeded);
    assert_eq!(lanes.calls.load(Ordering::SeqCst), 2);
    assert!(
        fixture
            .store
            .load_session(&scope(), &id("slow-session"))
            .await
            .unwrap()
            .active_run_id
            .is_none()
    );
    assert!(
        fixture
            .store
            .load_session(&scope(), &id("fast-session"))
            .await
            .unwrap()
            .active_run_id
            .is_none()
    );
    drop(slow);
    drop(fast);
    drop(agent);
    tokio::time::timeout(Duration::from_secs(2), async {
        while tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks()
            > baseline
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn saved_submission_survives_catalog_outage_but_not_revoked_authorization() {
    struct Offline(std::sync::atomic::AtomicUsize);
    impl ProfileResolver for Offline {
        fn resolve<'a>(
            &'a self,
            _: &'a ComponentRef,
            _: &'a Scope,
        ) -> PortFuture<'a, ComponentMetadata> {
            Box::pin(async move {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err(ContractError::new(
                    ErrorCode::ComponentUnavailable,
                    "offline.catalog",
                ))
            })
        }
    }
    let fixture = Fixture::new(Response::Text, false);
    let first_agent = fixture.agent();
    let first = fixture.started(&first_agent, "original").await;
    completed(first.outcome(&context()).await.unwrap());
    let offline = std::sync::Arc::new(Offline(std::sync::atomic::AtomicUsize::new(0)));
    let mut bindings = fixture.bindings();
    bindings.profile_resolver = offline.clone();
    let restarted = create_agent(profile(), bindings).unwrap();
    let replay = completed(
        restarted
            .start(request("original"), context())
            .await
            .unwrap(),
    );
    assert_eq!(replay.run_id(), first.run_id());
    assert_eq!(offline.0.load(Ordering::SeqCst), 0);
    fixture.policy.deny.store(3, Ordering::SeqCst);
    assert_eq!(
        restarted
            .start(request("original"), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(offline.0.load(Ordering::SeqCst), 0);
    fixture.policy.deny.store(0, Ordering::SeqCst);
    assert_eq!(
        restarted
            .start(request("new-offline"), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    assert!(offline.0.load(Ordering::SeqCst) > 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}
```

## `crates/wickle/tests/execution_contracts.rs`

```rust
//! Execution-record invariants at the persistence boundary.
use serde_json::json;
use wickle::*;
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn profile() -> VersionedRef {
    VersionedRef {
        id: id("agent"),
        version: id("1"),
    }
}
#[test]
fn submitted_snapshot_preserves_exact_numbers_and_normalizes_only_empty_overrides() {
    let a = RequestSnapshot::capture(
        profile(),
        r#"{"model_options":{},"input":184467440737095516160001}"#,
        None,
        JsonTextLimits::default(),
    )
    .unwrap();
    let b = RequestSnapshot::capture(
        profile(),
        r#"{"input":184467440737095516160001}"#,
        Some("{}"),
        JsonTextLimits::default(),
    )
    .unwrap();
    assert_eq!(a.digest(), b.digest());
    assert!(!a.system_inputs_provided());
    assert!(b.system_inputs_provided());
    let restored: RequestSnapshot =
        serde_json::from_str(&serde_json::to_string(&a).unwrap()).unwrap();
    restored.validate(JsonTextLimits::default()).unwrap();
    assert_eq!(
        restored.request_json(),
        r#"{"input":184467440737095516160001,"model_options":{}}"#
    );
    let other = RequestSnapshot::capture(
        profile(),
        r#"{"input":184467440737095516160002}"#,
        None,
        JsonTextLimits::default(),
    )
    .unwrap();
    assert_ne!(a.digest(), other.digest());
    assert!(
        RequestSnapshot::capture(
            profile(),
            r#"{"model_options":null}"#,
            None,
            JsonTextLimits::default()
        )
        .is_err()
    );
    assert!(
        RequestSnapshot::capture(profile(), "{}", Some("null"), JsonTextLimits::default()).is_err()
    );
}
#[test]
fn snapshots_reject_tampering_and_debug_omits_protected_values() {
    let a = RequestSnapshot::capture(
        profile(),
        "{}",
        Some(r#"{"workspace_id":"private-workspace-uuid"}"#),
        JsonTextLimits::default(),
    )
    .unwrap();
    assert!(!format!("{a:?}").contains("private-workspace-uuid"));
    let mut changed = serde_json::to_value(a).unwrap();
    changed["system_inputs_json"] = json!("{}");
    let b: RequestSnapshot = serde_json::from_value(changed.clone()).unwrap();
    assert!(b.validate(JsonTextLimits::default()).is_err());
    changed["schema_version"] = json!("future");
    assert!(serde_json::from_value::<RequestSnapshot>(changed).is_err());
}
#[test]
fn interrupted_segments_require_matching_recoverable_evidence() {
    let mut segment = ExecutionSegment {
        schema_version: ExecutionRecordVersion::V1,
        execution_principal_ref: id("original-user"),
        run_id: id("run"),
        segment_id: id("segment"),
        accepted_revision: 4,
        app_state: None,
        outcome: Some(SegmentOutcome::Interrupted {
            interruption: InterruptionRecord {
                segment_id: id("segment"),
                cause: InterruptionCause::HostShutdown,
                checkpoint_revision: 5,
                recoverable: true,
                unresolved_effects: vec![],
            },
        }),
    };
    segment.validate().unwrap();
    let saved = serde_json::to_string(&segment).unwrap();
    let replay: ExecutionSegment = serde_json::from_str(&saved).unwrap();
    replay.validate().unwrap();
    assert_eq!(segment, replay);
    if let Some(SegmentOutcome::Interrupted { interruption }) = &mut segment.outcome {
        interruption.cause = InterruptionCause::UserCancel;
    }
    assert!(segment.validate().is_err());
    if let Some(SegmentOutcome::Interrupted { interruption }) = &mut segment.outcome {
        interruption.cause = InterruptionCause::HostShutdown;
        interruption.segment_id = id("other");
    }
    assert!(segment.validate().is_err());
    assert!(!RunStatus::Interrupted.is_terminal());
}
#[test]
fn output_cap_preserves_omission_and_rejects_null_zero_or_fraction() {
    let base = json!({"request_id":"r","session_id":"s","input":[],"trigger":{"kind":"user"}});
    let original = RunRequest::from_json(&base.to_string()).unwrap();
    assert!(original.max_output_tokens.is_none());
    assert!(
        serde_json::to_value(original)
            .unwrap()
            .get("max_output_tokens")
            .is_none()
    );
    for cap in [json!(null), json!(0), json!(-1), json!(1.5)] {
        let mut input = base.clone();
        input["max_output_tokens"] = cap;
        assert!(RunRequest::from_json(&input.to_string()).is_err());
    }
    let mut input = base;
    input["max_output_tokens"] = json!(42);
    assert_eq!(
        RunRequest::from_json(&input.to_string())
            .unwrap()
            .max_output_tokens
            .unwrap()
            .get(),
        42
    );
}

#[test]
fn replay_uses_stored_canonicalization_instead_of_candidate_digest() {
    let original = RequestSnapshot::capture(
        profile(),
        r#"{"model_options":{"temperature":1e3}}"#,
        None,
        JsonTextLimits::default(),
    )
    .unwrap();
    let envelope = format!(
        "{{\"profile_ref\":{},\"request\":{},\"system_inputs\":{{}}}}",
        serde_json::to_string(&profile()).unwrap(),
        original.request_json()
    );
    let mut stored = serde_json::to_value(&original).unwrap();
    stored["canonicalization"] = json!("sorted-json-v1");
    stored["digest"] = json!(
        versioned_digest_json(
            &envelope,
            CanonicalizationVersion::SortedJsonV1,
            JsonTextLimits::default()
        )
        .unwrap()
    );
    let stored: RequestSnapshot = serde_json::from_value(stored).unwrap();
    let candidate = RequestSnapshot::capture(
        profile(),
        r#"{"model_options":{"temperature":1000.0}}"#,
        Some("{}"),
        JsonTextLimits::default(),
    )
    .unwrap();
    assert_ne!(stored.digest(), candidate.digest());
    assert!(
        stored
            .matches_submission(&candidate, JsonTextLimits::default())
            .unwrap()
    );
    assert!(
        !original
            .matches_submission(&candidate, JsonTextLimits::default())
            .unwrap()
    );
}
```

## `crates/wickle/tests/state.rs`

```rust
//! Atomic admission, persistence, leases, and scope isolation of the memory store.

use serde_json::json;
use std::{collections::BTreeSet, sync::Arc};
use wickle::*;

mod support;
use support::*;

#[tokio::test]
async fn request_lookup_uses_scope_and_session_and_returns_current_state_after_restore() {
    let store = MemoryStateStore::new();
    assert!(
        store
            .find_request(&scope(), &id("first"), &id("request"))
            .await
            .unwrap()
            .is_none()
    );
    let first = store
        .admit(
            &scope(),
            admission("run-a", "request", "first", "input", "1").await,
        )
        .await
        .unwrap()
        .state;
    store
        .admit(
            &scope(),
            admission("run-b", "request", "second", "input", "1").await,
        )
        .await
        .unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run-a"), &id("worker"), 0, 1000)
        .await
        .unwrap();
    let finished = store
        .commit(&scope(), &id("run-a"), finished(&first.snapshot, lease, 1))
        .await
        .unwrap();
    assert_eq!(
        store
            .find_request(&scope(), &id("first"), &id("request"))
            .await
            .unwrap(),
        Some(finished)
    );
    let checkpoint = store.export_checkpoint(&scope()).unwrap();
    let restored = MemoryStateStore::from_checkpoint(
        StateStoreCheckpoint::from_json(
            &serde_json::to_string(&checkpoint).unwrap(),
            &scope(),
            &checkpoint.digest(),
        )
        .unwrap(),
    );
    let first = restored
        .find_request(&scope(), &id("first"), &id("request"))
        .await
        .unwrap()
        .unwrap();
    let second = restored
        .find_request(&scope(), &id("second"), &id("request"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.snapshot.status, RunStatus::Succeeded);
    assert_eq!(second.snapshot.run_id, id("run-b"));
    let foreign = Scope {
        workspace_id: id("foreign"),
        ..scope()
    };
    assert!(
        restored
            .find_request(&foreign, &id("first"), &id("request"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        restored
            .find_request(&scope(), &id("missing"), &id("request"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn admitted_model_options_are_fixed_for_replay_and_later_commits() {
    fn with_effort(mut input: AdmissionInput, effort: &str) -> AdmissionInput {
        input.snapshot.request.model_options =
            JsonObject::from([("reasoning_effort".into(), json!(effort))]);
        input.snapshot.request_digest =
            admission_digest(&input.snapshot.request, &input.snapshot.profile, None);
        let RunEventPayload::RunStarted { request_ref, .. } = &mut input.events[0].payload else {
            unreachable!()
        };
        let record = ProtectedRecord::new(
            request_ref.record_id.clone(),
            request_ref.revision,
            serde_json::to_value(&input.snapshot.request).unwrap(),
        );
        let old_ref = request_ref.clone();
        *request_ref = record.reference().clone();
        *input
            .records
            .iter_mut()
            .find(|record| record.reference() == &old_ref)
            .unwrap() = record;
        input
    }
    let store = MemoryStateStore::new();
    let first = with_effort(
        admission("run", "request", "session", "input", "1").await,
        "high",
    );
    let expected_options = first.snapshot.request.model_options.clone();
    let original = store.admit(&scope(), first).await.unwrap().state;
    let replay = with_effort(
        admission("replacement", "request", "session", "input", "2").await,
        "high",
    );
    let replay = store.admit(&scope(), replay).await.unwrap();
    assert!(!replay.created);
    assert_eq!(
        replay.state.snapshot.request.model_options,
        expected_options
    );
    let changed = with_effort(
        admission("replacement", "request", "session", "input", "2").await,
        "low",
    );
    assert_ne!(
        changed.snapshot.request_digest,
        original.snapshot.request_digest
    );
    assert_eq!(
        store.admit(&scope(), changed).await.unwrap_err().code,
        ErrorCode::RequestConflict
    );

    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("worker"), 1, 100)
        .await
        .unwrap();
    let mut change = prepared(&original.snapshot, lease, 2);
    change
        .snapshot
        .request
        .model_options
        .insert("reasoning_effort".into(), json!("low"));
    change.snapshot.request_digest =
        admission_digest(&change.snapshot.request, &change.snapshot.profile, None);
    assert_eq!(
        store
            .commit(&scope(), &id("run"), change)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidTransition
    );
    let saved = store.load(&scope(), &id("run")).await.unwrap().snapshot;
    let restored = RunSnapshot::from_json(&serde_json::to_string(&saved).unwrap()).unwrap();
    assert_eq!(restored.request.model_options, expected_options);
    assert_eq!(restored.request_digest, original.snapshot.request_digest);
}

#[tokio::test]
async fn identical_retries_return_the_original_run_without_replacing_resolved_metadata() {
    let store = MemoryStateStore::new();
    let first = admission("run-a", "request", "session", "input", "1").await;
    let receipt = store.admit(&scope(), first.clone()).await.unwrap();
    assert!(receipt.created);
    let retry = admission("run-b", "request", "session", "input", "2").await;
    let replay = store.admit(&scope(), retry).await.unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state.snapshot.run_id, id("run-a"));
    assert_eq!(
        replay.state.snapshot.profile.resolution_digest(),
        first.snapshot.profile.resolution_digest()
    );
    assert_eq!(replay.state.messages.len(), 1);
    let changed = admission("run-c", "request", "session", "different input", "1").await;
    assert!(store.admit(&scope(), changed).await.is_err());
    assert_eq!(
        store
            .load(&scope(), &id("run-a"))
            .await
            .unwrap()
            .snapshot
            .revision,
        0
    );
    let events = store
        .read_events(&scope(), &id("run-a"), 0, 100)
        .await
        .unwrap();
    assert_eq!(events.events.len(), 1);
}

#[tokio::test]
async fn concurrent_duplicate_admission_creates_exactly_one_run() {
    let store = Arc::new(MemoryStateStore::new());
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut handles = vec![];
    for n in 0..8 {
        let input = admission(&format!("run-{n}"), "request", "session", "input", "1").await;
        let store = store.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            store.admit(&scope(), input).await.unwrap()
        }));
    }
    let mut created = 0;
    let mut ids = BTreeSet::new();
    for h in handles {
        let result = h.await.unwrap();
        created += usize::from(result.created);
        ids.insert(result.state.snapshot.run_id);
    }
    assert_eq!(created, 1);
    assert_eq!(ids.len(), 1);
}

#[tokio::test]
async fn distinct_concurrent_requests_create_only_one_active_run_in_the_session() {
    let store = Arc::new(MemoryStateStore::new());
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut handles = Vec::new();
    for n in 0..8 {
        let input = admission(
            &format!("run-{n}"),
            &format!("request-{n}"),
            "session",
            "input",
            "1",
        )
        .await;
        let store = store.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            store.admit(&scope(), input).await
        }));
    }
    let mut accepted = Vec::new();
    let mut rejected = 0;
    for handle in handles {
        match handle.await.unwrap() {
            Ok(result) => accepted.push(result.state.snapshot.run_id),
            Err(_) => rejected += 1,
        }
    }
    assert_eq!(accepted.len(), 1);
    assert_eq!(rejected, 7);
    assert_eq!(
        store
            .load_session(&scope(), &id("session"))
            .await
            .unwrap()
            .active_run_id
            .as_ref(),
        accepted.first()
    );
}

#[tokio::test]
async fn waiting_keeps_the_session_busy_even_after_the_worker_releases_its_lease() {
    let store = MemoryStateStore::new();
    store
        .admit(
            &scope(),
            admission("run", "request", "session", "input", "1").await,
        )
        .await
        .unwrap();
    let state = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut update = prepared(&state.snapshot, lease.clone(), 101);
    let wait = WaitState {
        wait_id: id("wait"),
        target: WaitTarget::Input {
            request: InputRequest {
                input_request_id: id("input-request"),
                call_id: id("input-call"),
                question: "Choose a source".into(),
                schema_ref: None,
            },
        },
        expires_at_ms: Some(1000),
    };
    let record = ProtectedRecord::new(id("wait-record"), 1, serde_json::to_value(&wait).unwrap());
    update.snapshot.status = RunStatus::Waiting;
    update.snapshot.phase = RunPhase::Waiting;
    update.snapshot.wait = Some(wait);
    update.snapshot.last_event_seq = 2;
    update.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        2,
        RunEventPayload::RunWaiting {
            wait_ref: record.reference().clone(),
        },
    ));
    update.records.push(record);
    store.commit(&scope(), &id("run"), update).await.unwrap();
    store
        .release_lease(&scope(), &id("run"), &lease, 102)
        .await
        .unwrap();
    assert!(
        store
            .admit(
                &scope(),
                admission("other", "other-request", "session", "other", "1").await
            )
            .await
            .is_err()
    );
    let replay = store
        .admit(
            &scope(),
            admission("replacement", "request", "session", "input", "2").await,
        )
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state.snapshot.status, RunStatus::Waiting);
}

#[tokio::test]
async fn competing_requests_cannot_share_an_active_session_and_terminal_commit_releases_it() {
    let store = MemoryStateStore::new();
    let first = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), first).await.unwrap();
    assert!(
        store
            .admit(
                &scope(),
                admission("other", "other-request", "session", "other", "1").await
            )
            .await
            .is_err()
    );
    let state = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 50)
        .await
        .unwrap();
    store
        .commit(&scope(), &id("run"), finished(&state.snapshot, lease, 101))
        .await
        .unwrap();
    assert!(
        store
            .load_session(&scope(), &id("session"))
            .await
            .unwrap()
            .active_run_id
            .is_none()
    );
    let mut second = admission("other", "other-request", "session", "other", "1").await;
    second.messages[0].sequence = 2.try_into().unwrap();
    assert!(store.admit(&scope(), second).await.unwrap().created);
    assert_eq!(
        store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .status,
        RunStatus::Succeeded
    );
    let replay = store
        .admit(
            &scope(),
            admission("replacement", "request", "session", "input", "2").await,
        )
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state.snapshot.run_id, id("run"));
}

#[tokio::test]
async fn lease_expiry_fencing_and_revision_conflicts_are_independent() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), input).await.unwrap();
    let first = store
        .acquire_lease(&scope(), &id("run"), &id("owner-a"), 100, 10)
        .await
        .unwrap();
    assert!(
        store
            .acquire_lease(&scope(), &id("run"), &id("owner-b"), 109, 10)
            .await
            .is_err()
    );
    assert!(
        store
            .renew_lease(&scope(), &id("run"), &first, 110, 10)
            .await
            .is_err()
    );
    let second = store
        .acquire_lease(&scope(), &id("run"), &id("owner-b"), 110, 10)
        .await
        .unwrap();
    assert!(second.fencing_token > first.fencing_token);
    let state = store.load(&scope(), &id("run")).await.unwrap();
    assert!(
        store
            .commit(
                &scope(),
                &id("run"),
                prepared(&state.snapshot, first.clone(), 111)
            )
            .await
            .is_err()
    );
    let update = prepared(&state.snapshot, second.clone(), 111);
    store
        .commit(&scope(), &id("run"), update.clone())
        .await
        .unwrap();
    assert!(store.commit(&scope(), &id("run"), update).await.is_err());
    assert!(
        store
            .renew_lease(&scope(), &id("run"), &first, 111, 20)
            .await
            .is_err()
    );
    let renewed = store
        .renew_lease(&scope(), &id("run"), &second, 119, 20)
        .await
        .unwrap();
    assert_eq!(renewed.fencing_token, second.fencing_token);
    let current = store.load(&scope(), &id("run")).await.unwrap();
    // Heartbeat renews expiry without invalidating the driver's same-generation copy.
    store
        .commit(
            &scope(),
            &id("run"),
            prepared(&current.snapshot, second.clone(), 125),
        )
        .await
        .unwrap();
    store
        .release_lease(&scope(), &id("run"), &renewed, 130)
        .await
        .unwrap();
    let third = store
        .acquire_lease(&scope(), &id("run"), &id("owner-c"), 130, 20)
        .await
        .unwrap();
    assert!(third.fencing_token > renewed.fencing_token);
    let current = store.load(&scope(), &id("run")).await.unwrap();
    let mut forged = third.clone();
    forged.expires_at_ms = i64::MAX;
    assert_eq!(
        store
            .commit(
                &scope(),
                &id("run"),
                prepared(&current.snapshot, forged, 150)
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::LeaseLost
    );
}

#[tokio::test]
async fn an_event_cannot_announce_a_wait_absent_from_the_committed_snapshot() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    let wrong_payload = input.records[0].reference().clone();
    store.admit(&scope(), input).await.unwrap();
    let before = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut update = prepared(&before.snapshot, lease, 101);
    update.snapshot.last_event_seq = 2;
    update.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        2,
        RunEventPayload::RunWaiting {
            wait_ref: wrong_payload,
        },
    ));
    assert_eq!(
        store
            .commit(&scope(), &id("run"), update)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidEvent
    );
    assert_eq!(store.load(&scope(), &id("run")).await.unwrap(), before);
    assert_eq!(
        store
            .read_events(&scope(), &id("run"), 0, 100)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
}

#[tokio::test]
async fn uncertain_tool_effects_keep_the_original_attempt_and_idempotency_key() {
    struct StationaryClock;
    impl Clock for StationaryClock {
        fn now(&self) -> Result<ClockReading, ContractError> {
            Ok(ClockReading {
                utc_ms: 102,
                monotonic_ms: 0,
            })
        }
        fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
            Box::pin(std::future::pending())
        }
    }
    struct AllowPolicy;
    impl PolicyPort for AllowPolicy {
        fn authorize<'a>(
            &'a self,
            _: &'a PolicyRequest,
            _: PolicyContext<'a>,
        ) -> PortFuture<'a, PolicyDecision> {
            Box::pin(async { Ok(PolicyDecision::Allow {}) })
        }
    }
    let store = Arc::new(MemoryStateStore::new());
    let registry = Arc::new(SystemInputRegistry::default());
    let compiled = SchemaCompiler::new().compile(ToolDescriptor {
        tool: VersionedRef { id: id("tool"), version: id("1") }, name: id("tool"), description: "Write a record".into(),
        input_schema: json!({"type":"object","properties":{},"required":[],"additionalProperties":false}), agent_parameters: vec![], system_bindings: None,
        output_schema: json!(true), side_effect: ToolSideEffect::Write, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: true, max_output_bytes: 1024.try_into().unwrap(),
    }, &registry).unwrap();
    let mut input = admission("run", "request", "session", "input", "1").await;
    let mut profile = input.snapshot.profile.profile().clone();
    profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
        tool_id: id("tool"),
        version: id("1"),
        bindings: None,
        config: None,
    }));
    input.snapshot.profile = ProfileValidator::new(&Catalog { revision: "1" })
        .validate(&profile, &scope())
        .await
        .unwrap();
    input.snapshot.request_digest =
        admission_digest(&input.snapshot.request, &input.snapshot.profile, None);
    if let RunEventPayload::RunStarted { profile_digest, .. } = &mut input.events[0].payload {
        *profile_digest = input.snapshot.profile.profile_digest().clone();
    }
    store.admit(&scope(), input).await.unwrap();
    let before = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut plan = prepared(&before.snapshot, lease.clone(), 101);
    let call = ToolCall {
        call_id: id("call"),
        model_request_id: id("model-request"),
        provider_call_id: id("provider-call"),
        tool_name: id("tool"),
        model_inputs: Default::default(),
        descriptor_digest: Some(compiled.descriptor_digest().clone()),
        bound_input_ref: None,
    };
    let call_record =
        ProtectedRecord::new(id("planned-call"), 1, serde_json::to_value(&call).unwrap());
    plan.snapshot.phase = RunPhase::Tool;
    plan.snapshot.last_event_seq = 2;
    plan.snapshot.tool_ledger.push(ToolLedgerEntry {
        call,
        state: ToolCallState::Planned {},
    });
    plan.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        2,
        RunEventPayload::ToolPlanned {
            call_ref: call_record.reference().clone(),
        },
    ));
    plan.records.push(call_record);
    store.commit(&scope(), &id("run"), plan).await.unwrap();
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope(),
            principal_ref: id("caller"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    );
    let budget = RunBudget::attach(
        store.clone(),
        Arc::new(StationaryClock),
        Arc::new(RandomIdSource),
        scope(),
        id("run"),
        lease.clone(),
        context.cancellation.clone(),
    )
    .await
    .unwrap();
    let binder = InputBinder::new(
        registry,
        None,
        Arc::new(
            PolicyGate::new(Arc::new(AllowPolicy), std::time::Duration::from_secs(1)).unwrap(),
        ),
        Arc::new(RandomIdSource),
    );
    binder
        .bind(&compiled, &id("call"), &context, &budget)
        .await
        .unwrap();
    let reservation = budget
        .reserve(ReservationKind::Tool {
            call_id: id("call"),
        })
        .await
        .unwrap();
    let planned = store.load(&scope(), &id("run")).await.unwrap();
    let mut dispatch = prepared(&planned.snapshot, lease.clone(), 102);
    dispatch.snapshot.phase = RunPhase::Tool;
    dispatch.snapshot.tool_ledger[0].state = ToolCallState::Dispatching {
        attempt_id: reservation.attempt_id.clone(),
        idempotency_key: id("effect-key"),
    };
    let dispatched = store.commit(&scope(), &id("run"), dispatch).await.unwrap();
    let mut lost = prepared(&dispatched.snapshot, lease.clone(), 103);
    lost.snapshot.phase = RunPhase::Tool;
    lost.snapshot.tool_ledger[0].state = ToolCallState::Unknown {
        attempt_id: id("attempt-b"),
        idempotency_key: id("different-key"),
    };
    lost.snapshot.reservations.push(AttemptReservation {
        attempt_id: id("attempt-b"),
        kind: ReservationKind::Tool {
            call_id: id("call"),
        },
        reserved_at_ms: lost.snapshot.timing.last_observed_at_ms,
    });
    lost.snapshot.usage.tool_attempts += 1;
    let rejected = store.commit(&scope(), &id("run"), lost).await.unwrap_err();
    assert_eq!(rejected.code, ErrorCode::InvalidTransition, "{rejected:?}");
    let mut lost = prepared(&dispatched.snapshot, lease, 103);
    lost.snapshot.phase = RunPhase::Tool;
    lost.snapshot.tool_ledger[0].state = ToolCallState::Unknown {
        attempt_id: reservation.attempt_id.clone(),
        idempotency_key: id("effect-key"),
    };
    let saved = store.commit(&scope(), &id("run"), lost).await.unwrap();
    assert!(
        matches!(&saved.snapshot.tool_ledger[0].state, ToolCallState::Unknown { attempt_id, idempotency_key } if attempt_id == &reservation.attempt_id && idempotency_key == &id("effect-key"))
    );
}

#[tokio::test]
async fn invalid_multi_event_commit_does_not_partially_publish_records_state_or_messages() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), input).await.unwrap();
    let before = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut update = prepared(&before.snapshot, lease, 101);
    let wait = WaitState {
        wait_id: id("new-wait"),
        target: WaitTarget::Input {
            request: InputRequest {
                input_request_id: id("input-request"),
                call_id: id("input-call"),
                question: "Choose a source".into(),
                schema_ref: None,
            },
        },
        expires_at_ms: None,
    };
    let record = ProtectedRecord::new(id("new-record"), 1, serde_json::to_value(&wait).unwrap());
    let reference = record.reference().clone();
    update.records.push(record);
    update.events = vec![
        event(
            &id("run"),
            &id("session"),
            &scope(),
            2,
            RunEventPayload::RunWaiting {
                wait_ref: reference.clone(),
            },
        ),
        event(
            &id("run"),
            &id("session"),
            &scope(),
            2,
            RunEventPayload::RunWaiting {
                wait_ref: reference.clone(),
            },
        ),
    ];
    update.snapshot.last_event_seq = 2;
    update.snapshot.status = RunStatus::Waiting;
    update.snapshot.phase = RunPhase::Waiting;
    update.snapshot.wait = Some(wait);
    let mut message = before.messages[0].clone();
    message.message_id = id("new-message");
    message.sequence = 2.try_into().unwrap();
    update.messages.push(message);
    assert!(store.commit(&scope(), &id("run"), update).await.is_err());
    let after = store.load(&scope(), &id("run")).await.unwrap();
    assert_eq!(after.snapshot, before.snapshot);
    assert_eq!(after.messages, before.messages);
    assert_eq!(after.session, before.session);
    assert!(store.read_record(&scope(), &reference).await.is_err());
    assert_eq!(
        store
            .read_events(&scope(), &id("run"), 0, 100)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
}

#[tokio::test]
async fn commits_cannot_replace_request_or_resolved_profile_and_reads_return_owned_snapshots() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), input).await.unwrap();
    let before = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut update = prepared(&before.snapshot, lease.clone(), 101);
    update.snapshot.request.input = vec![InputContent::Text {
        text: "replacement".into(),
    }];
    update.snapshot.request_digest =
        admission_digest(&update.snapshot.request, &update.snapshot.profile, None);
    assert!(store.commit(&scope(), &id("run"), update).await.is_err());
    let replacement = admission("run", "request", "session", "input", "2").await;
    let mut update = prepared(&before.snapshot, lease, 101);
    update.snapshot.profile = replacement.snapshot.profile;
    assert!(store.commit(&scope(), &id("run"), update).await.is_err());
    let mut copy = store.load(&scope(), &id("run")).await.unwrap();
    copy.messages.clear();
    copy.snapshot.request.input.clear();
    let after = store.load(&scope(), &id("run")).await.unwrap();
    assert_eq!(after.snapshot, before.snapshot);
    assert_eq!(after.messages, before.messages);
}

#[tokio::test]
async fn every_store_surface_is_scoped_and_memory_does_not_claim_durability() {
    let store = MemoryStateStore::new();
    let capabilities = store.capabilities();
    assert!(
        !capabilities.durable && !capabilities.cross_process_leases && capabilities.event_replay
    );
    let mut durable = admission(
        "durable",
        "durable-request",
        "durable-session",
        "input",
        "1",
    )
    .await;
    durable.require_durable = true;
    assert!(store.admit(&scope(), durable).await.is_err());
    let input = admission("run", "request", "session", "input", "1").await;
    let record = input.records[0].reference().clone();
    store.admit(&scope(), input).await.unwrap();
    let snapshot = store.load(&scope(), &id("run")).await.unwrap().snapshot;
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    for foreign in [
        Scope {
            tenant_id: id("other"),
            ..scope()
        },
        Scope {
            workspace_id: id("other"),
            ..scope()
        },
        Scope {
            user_id: Some(id("other")),
            ..scope()
        },
    ] {
        assert!(store.load(&foreign, &id("run")).await.is_err());
        assert!(store.load_session(&foreign, &id("session")).await.is_err());
        assert!(
            store
                .read_events(&foreign, &id("run"), 0, 100)
                .await
                .is_err()
        );
        assert!(store.read_record(&foreign, &record).await.is_err());
        assert!(
            store
                .acquire_lease(&foreign, &id("run"), &id("owner"), 101, 10)
                .await
                .is_err()
        );
        assert!(
            store
                .renew_lease(&foreign, &id("run"), &lease, 101, 10)
                .await
                .is_err()
        );
        assert!(
            store
                .commit(
                    &foreign,
                    &id("run"),
                    prepared(&snapshot, lease.clone(), 101)
                )
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn event_pages_are_exclusive_ordered_replayable_and_preserved_after_completion() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), input).await.unwrap();
    let snapshot = store.load(&scope(), &id("run")).await.unwrap().snapshot;
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    store
        .commit(&scope(), &id("run"), finished(&snapshot, lease, 101))
        .await
        .unwrap();
    let first = store.read_events(&scope(), &id("run"), 0, 1).await.unwrap();
    assert_eq!(first.events.len(), 1);
    assert!(first.has_more);
    assert_eq!(first.next_after_seq, 1);
    let second = store
        .read_events(&scope(), &id("run"), first.next_after_seq, 1)
        .await
        .unwrap();
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.events[0].seq.get(), 2);
    assert!(!second.has_more);
    assert_eq!(
        store
            .read_events(&scope(), &id("run"), 1, 100)
            .await
            .unwrap()
            .events,
        second.events
    );
    assert!(
        store
            .read_events(&scope(), &id("run"), 2, 100)
            .await
            .unwrap()
            .events
            .is_empty()
    );
    assert!(
        store
            .acquire_lease(&scope(), &id("run"), &id("owner"), 102, 100)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn recovery_acceptance_requires_the_exact_running_checkpoint_and_its_event() {
    for mode in ["valid", "missing-event", "changed-source"] {
        let store = MemoryStateStore::new();
        let saved = store
            .admit(
                &scope(),
                admission("run", "request", "session", "input", "1").await,
            )
            .await
            .unwrap()
            .state;
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("recovery-worker"), 0, 1000)
            .await
            .unwrap();
        let mut source = saved.snapshot.clone();
        if mode == "changed-source" {
            source.phase = RunPhase::Tool;
        }
        let source = source.recovery_record(id("source-checkpoint")).unwrap();
        let command = ResumeCommand {
            run_id: id("run"),
            expected_revision: saved.snapshot.revision,
            command_id: id("recover-once"),
            action: ResumeAction::Recover {
                recovery_ref: source.reference().clone(),
            },
        };
        let command_record = ProtectedRecord::new(
            id("recovery-command"),
            1,
            serde_json::to_value(&command).unwrap(),
        );
        let receipt = RecoveryReceipt {
            command,
            command_ref: command_record.reference().clone(),
            source_snapshot_ref: source.reference().clone(),
            accepted_revision: saved.snapshot.revision + 1,
            previous_segment_start_revision: 0,
            previous_last_event_seq: saved.snapshot.last_event_seq,
            actor_ref: id("operator"),
            capability_grant_ref: id("grant"),
            expired: false,
            recovery_attempt_id: Some(id("recovery-budget")),
        };
        let record = ProtectedRecord::new(
            id("recovery-receipt"),
            1,
            serde_json::to_value(&receipt).unwrap(),
        );
        let mut update = prepared(&saved.snapshot, lease, 0);
        update.snapshot.recovery_receipts.push(receipt);
        update.snapshot.usage.recovery_attempts += 1;
        update.snapshot.reservations.push(AttemptReservation {
            attempt_id: id("recovery-budget"),
            kind: ReservationKind::Recovery {},
            reserved_at_ms: 0,
        });
        if mode != "missing-event" {
            update.snapshot.last_event_seq += 1;
            update.events.push(event(
                &id("run"),
                &id("session"),
                &scope(),
                update.snapshot.last_event_seq,
                RunEventPayload::RunRecovered {
                    recovery_receipt_ref: record.reference().clone(),
                },
            ));
        }
        for event in &mut update.events {
            event.timestamp_ms = 0;
        }
        update.records = vec![source, command_record, record];
        let result = store.commit(&scope(), &id("run"), update).await;
        if mode == "valid" {
            let result = result.unwrap();
            assert_eq!(result.snapshot.status, RunStatus::Running);
            assert!(result.snapshot.outcome.is_none());
            let checkpoint = store.export_checkpoint(&scope()).unwrap();
            let restored = StateStoreCheckpoint::from_json(
                &serde_json::to_string(&checkpoint).unwrap(),
                &scope(),
                &checkpoint.digest(),
            )
            .unwrap();
            assert_eq!(
                MemoryStateStore::from_checkpoint(restored)
                    .load(&scope(), &id("run"))
                    .await
                    .unwrap(),
                result
            );
        } else {
            assert!(result.is_err(), "{mode}");
            assert_eq!(store.load(&scope(), &id("run")).await.unwrap(), saved);
        }
    }
}

#[tokio::test]
async fn admission_race_compares_stored_submission_before_candidate_configuration() {
    let store = MemoryStateStore::new();
    assert!(
        store
            .find_request(&scope(), &id("session"), &id("request"))
            .await
            .unwrap()
            .is_none()
    );
    let mut first = admission("winner", "request", "session", "same", "1").await;
    let profile = first.snapshot.profile.profile();
    first.submitted = Some(
        RequestSnapshot::capture(
            VersionedRef {
                id: profile.agent_id.clone(),
                version: profile.version.clone(),
            },
            &serde_json::to_string(&first.snapshot.request).unwrap(),
            None,
            JsonTextLimits::default(),
        )
        .unwrap(),
    );
    let original = first.submitted.clone();
    let winner = store.admit(&scope(), first).await.unwrap();
    let mut loser = admission("loser", "request", "session", "same", "99").await;
    loser.submitted = original;
    loser.snapshot.request_digest = canonical_digest(&json!("changed-current-configuration"));
    let replay = store.admit(&scope(), loser.clone()).await.unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state, winner.state);
    let changed = RequestSnapshot::capture(
        VersionedRef {
            id: id("assistant"),
            version: id("1.0.0"),
        },
        &serde_json::to_string(
            &admission("unused", "request", "session", "different", "1")
                .await
                .snapshot
                .request,
        )
        .unwrap(),
        None,
        JsonTextLimits::default(),
    )
    .unwrap();
    loser.submitted = Some(changed);
    assert_eq!(
        store.admit(&scope(), loser).await.unwrap_err().code,
        ErrorCode::RequestConflict
    );
}
```

## `crates/wickle/tests/verification.rs`

```rust
//! Candidate validation, repair, review waits, and durable verification evidence.
#[path = "support/agent.rs"]
#[allow(dead_code)]
mod support;
use futures_util::{TryStreamExt, stream};
use serde_json::json;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::*;
use wickle::*;

struct Metadata;
impl ProfileResolver for Metadata {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(reference.version.clone().unwrap_or(id("1"))),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!(reference)),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: Default::default(),
                required_capabilities: Default::default(),
                required_connections: Default::default(),
                model_name: None,
                hook_position: None,
                exports: vec![],
            })
        })
    }
}
struct Answers {
    texts: Mutex<VecDeque<String>>,
    calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
}
impl ModelPort for Answers {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: id("fixture"),
            adapter: reference("adapter"),
            connection_ref: reference("connection"),
        }
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        let text = self
            .texts
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra model call");
        Box::pin(stream::iter([
            Ok(ModelEvent::TextDelta { text }),
            Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::Stop,
                metadata: Default::default(),
                continuation: vec![],
            }),
        ]))
    }
}
struct Checks {
    decisions: Mutex<VecDeque<Result<VerificationDecision, ContractError>>>,
    calls: AtomicUsize,
}
impl Verifier for Checks {
    fn definition(&self) -> VerifierDefinition {
        VerifierDefinition {
            verifier_ref: reference("quality"),
            criteria_ref: reference("criteria"),
            configuration: Default::default(),
            criteria: "Validate the supplied candidate against the reference fixture.".into(),
        }
    }
    fn verify<'a>(
        &'a self,
        _: &'a VerificationInput,
        _: &'a VerifierContext<'a>,
    ) -> PortFuture<'a, VerificationDecision> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.decisions
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected repeated verifier invocation")
        })
    }
}
fn setup(
    texts: &[&str],
    decisions: Vec<Result<VerificationDecision, ContractError>>,
) -> (
    Fixture,
    AgentProfile,
    AgentBindings,
    Arc<Answers>,
    Arc<Checks>,
) {
    let fixture = Fixture::new(Response::Text, false);
    let mut bindings = fixture.bindings();
    bindings.profile_resolver = Arc::new(Metadata);
    let model = Arc::new(Answers {
        texts: Mutex::new(texts.iter().map(|text| (*text).to_owned()).collect()),
        calls: AtomicUsize::new(0),
        requests: Mutex::new(vec![]),
    });
    bindings.model_exchange = Arc::new(
        ModelExchange::new(model.clone(), bindings.policy.clone())
            .with_route_inspector(fixture.inspector.clone(), Duration::from_secs(1))
            .unwrap(),
    );
    let checks = Arc::new(Checks {
        decisions: Mutex::new(decisions.into()),
        calls: AtomicUsize::new(0),
    });
    bindings.verification = Some(Arc::new(
        VerificationRuntime::new(
            scope(),
            vec![],
            vec![checks.clone()],
            VerificationLimits::default(),
        )
        .unwrap(),
    ));
    let mut profile = profile();
    profile.completion_policy = CompletionPolicy::Verified {
        verifier_ref: reference("quality"),
    };
    profile.limits.max_repair_attempts = 2;
    (fixture, profile, bindings, model, checks)
}
#[tokio::test]
async fn pass_pins_candidate_criteria_and_evidence_and_replays_without_verifying_again() {
    let (fixture, profile, bindings, model, checks) =
        setup(&["checked result"], vec![Ok(VerificationDecision::Pass {})]);
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::Verified
        }
    );
    assert_eq!(
        outcome.verification.as_ref().unwrap().verdict,
        VerificationVerdict::Pass
    );
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    let summary = outcome.verification.as_ref().unwrap();
    assert_eq!(summary.criteria_ref, reference("criteria"));
    assert_eq!(
        summary.evidence,
        vec![saved.snapshot.candidate_ref.clone().unwrap()]
    );
    let events: Vec<_> = handle.events(0, context()).try_collect().await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "verification.completed")
            .count(),
        1
    );
    let replay = fixture.started(&agent, "request").await;
    assert_eq!(
        completed(replay.outcome(&context()).await.unwrap()),
        outcome
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(checks.calls.load(Ordering::SeqCst), 1);
    let checkpoint = fixture.store.export_checkpoint(&scope()).unwrap();
    let bytes = serde_json::to_vec(&checkpoint).unwrap();
    let restored = StateStoreCheckpoint::from_json(
        &String::from_utf8(bytes).unwrap(),
        &scope(),
        &checkpoint.digest(),
    )
    .unwrap();
    let fresh = MemoryStateStore::from_checkpoint(restored);
    assert_eq!(fresh.load(&scope(), handle.run_id()).await.unwrap(), saved);
}
#[tokio::test]
async fn repair_retains_the_candidate_and_verification_provenance_then_accepts_only_the_new_result()
{
    let (fixture, profile, bindings, model, checks) = setup(
        &["incomplete", "complete"],
        vec![
            Ok(VerificationDecision::Revise {
                feedback: "Include the missing evidence.".into(),
            }),
            Ok(VerificationDecision::Pass {}),
        ],
    );
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "complete".into()
        }]
    );
    assert_eq!(outcome.usage.repair_attempts, 1);
    assert_eq!(outcome.usage.model_calls, 2);
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    let feedback: Vec<_> = saved
        .messages
        .iter()
        .filter(|message| message.origin == MessageOrigin::Verification)
        .collect();
    assert_eq!(feedback.len(), 1);
    assert_eq!(feedback[0].visibility, Visibility::Model);
    assert_eq!(
        saved
            .messages
            .iter()
            .filter(|message| message.origin == MessageOrigin::User)
            .count(),
        1
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(checks.calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn repair_budget_prevents_another_model_call_and_preserves_the_partial_candidate() {
    let (fixture, mut profile, bindings, model, checks) = setup(
        &["incomplete"],
        vec![Ok(VerificationDecision::Revise {
            feedback: "Missing evidence.".into(),
        })],
    );
    profile.limits.max_repair_attempts = 0;
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::RepairAttempts
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "incomplete".into()
        }]
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(checks.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn quality_rejection_and_verifier_transport_failure_are_distinct() {
    for (decision, expected) in [
        (
            Ok(VerificationDecision::Fail {
                reason: "Evidence contradicts the conclusion.".into(),
            }),
            "verification_failed",
        ),
        (
            Err(ContractError::new(
                ErrorCode::ComponentUnavailable,
                "synthetic.transport",
            )),
            "verification_unavailable",
        ),
    ] {
        let (fixture, profile, bindings, model, _) = setup(&["candidate"], vec![decision]);
        let agent = create_agent(profile, bindings).unwrap();
        let handle = fixture.started(&agent, "request").await;
        let outcome = completed(handle.outcome(&context()).await.unwrap());
        let OutcomeResult::Failed { failure } = outcome.result else {
            panic!("expected failure")
        };
        assert_eq!(failure.code, id(expected));
        let diagnostic = fixture
            .store
            .read_record(&scope(), failure.diagnostic_ref.as_ref().unwrap())
            .await
            .unwrap();
        if expected == "verification_failed" {
            assert_eq!(
                diagnostic.value()["decision"]["reason"],
                json!("Evidence contradicts the conclusion.")
            );
        } else {
            assert_eq!(
                diagnostic.value()["error"]["code"],
                json!("component_unavailable")
            );
        }
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        if expected == "verification_failed" {
            assert_eq!(
                outcome.verification.unwrap().verdict,
                VerificationVerdict::Fail
            );
        } else {
            assert!(outcome.verification.is_none());
        }
    }
}
#[tokio::test]
async fn review_approval_is_bound_to_the_candidate_and_never_calls_the_model_or_verifier_again() {
    let (fixture, profile, bindings, model, checks) = setup(
        &["review candidate"],
        vec![Ok(VerificationDecision::Wait {
            reason: "Review this evidence.".into(),
            expires_at_ms: None,
        })],
    );
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let waiting = completed(handle.outcome(&context()).await.unwrap());
    let OutcomeResult::Waiting { wait } = waiting.result else {
        panic!("expected review wait")
    };
    let WaitTarget::Approval { target } = wait.target else {
        panic!("expected candidate approval")
    };
    assert_eq!(
        waiting.verification.unwrap().verdict,
        VerificationVerdict::Wait
    );
    let command = ResumeCommand {
        run_id: handle.run_id().clone(),
        expected_revision: waiting.checkpoint_revision,
        command_id: id("approve-review"),
        action: ResumeAction::Approve {
            wait_id: wait.wait_id,
            target,
        },
    };
    let resumed = completed(agent.resume(command.clone(), context()).await.unwrap());
    let outcome = completed(resumed.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::Verified
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "review candidate".into()
        }]
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(checks.calls.load(Ordering::SeqCst), 1);
    let replay = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        completed(replay.outcome(&context()).await.unwrap()),
        outcome
    );
}

fn configure_router(bindings: &mut AgentBindings, json_output: bool, verification: bool) {
    let old = bindings.router.snapshot();
    let mut catalog = old.catalog().clone();
    let mut policy = old.policy().clone();
    if json_output {
        catalog.models[0]
            .capabilities
            .features
            .insert(id("json_output"));
        catalog.bindings[0]
            .capabilities
            .features
            .insert(id("json_output"));
        catalog.bindings[0].evidence.clear();
        let digest = catalog.bindings[0]
            .contract_digest(&catalog.models[0])
            .unwrap();
        catalog.bindings[0].evidence.push(ModelValidationEvidence {
            kind: ModelValidationKind::ContractTest,
            binding_digest: digest,
            checked_at_ms: 1000,
            evidence_ref: id("updated-fixture"),
            passed: true,
        });
    }
    if verification {
        let mut rule = policy.rules[0].clone();
        rule.purpose = ModelPurpose::Verification;
        policy.rules.push(rule);
    }
    bindings.router = Arc::new(Router {
        snapshot: RoutingSnapshot::new(catalog, policy).unwrap(),
        queries: AtomicUsize::new(0),
        snapshots: AtomicUsize::new(0),
    });
}
#[tokio::test]
async fn json_format_and_deterministic_quality_checks_repair_different_failures() {
    let (fixture, mut profile, mut bindings, model, checks) =
        setup(&["not-json", r#"{"amount":5}"#, r#"{"amount":11}"#], vec![]);
    configure_router(&mut bindings, true, false);
    profile.output_contract = OutputContract::JsonSchema {
        schema_ref: reference("shape"),
    };
    let format = json!({"type":"object","properties":{"amount":{"type":"integer"}},"required":["amount"],"additionalProperties":false});
    let quality = json!({"type":"object","properties":{"amount":{"type":"integer","minimum":10}},"required":["amount"],"additionalProperties":false});
    bindings.verification = Some(Arc::new(
        VerificationRuntime::new(
            scope(),
            vec![OutputSchemaDefinition {
                schema_ref: reference("shape"),
                schema: format.clone(),
            }],
            vec![Arc::new(
                SchemaVerifier::new(checks.definition(), quality).unwrap(),
            )],
            VerificationLimits::default(),
        )
        .unwrap(),
    ));
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::Verified
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Json {
            value: json!({"amount":11})
        }]
    );
    assert_eq!(outcome.usage.model_calls, 3);
    assert_eq!(outcome.usage.repair_attempts, 2);
    assert_eq!(
        model.requests.lock().unwrap()[0].output,
        ModelOutput::JsonSchema { schema: format }
    );
}
struct ModelReview {
    binding: Id,
}
impl Verifier for ModelReview {
    fn definition(&self) -> VerifierDefinition {
        VerifierDefinition {
            verifier_ref: reference("quality"),
            criteria_ref: reference("model-criteria"),
            configuration: serde_json::from_value(json!({"model_binding":self.binding})).unwrap(),
            criteria: "Ask the configured reviewer to check the candidate.".into(),
        }
    }
    fn verify<'a>(
        &'a self,
        input: &'a VerificationInput,
        context: &'a VerifierContext<'a>,
    ) -> PortFuture<'a, VerificationDecision> {
        Box::pin(async move {
            let text = context
                .models
                .generate(VerificationModelRequest {
                    stage: id("review"),
                    model_binding: self.binding.clone(),
                    messages: vec![ModelMessage {
                        role: ModelRole::User,
                        content: vec![ModelContent::Json {
                            value: json!({"candidate":input.candidate.output}),
                        }],
                    }],
                    options: None,
                    max_output_tokens: 128.try_into().unwrap(),
                })
                .await?;
            serde_json::from_value(parse_json(&text)?)
                .map_err(|_| ContractError::new(ErrorCode::InvalidContract, "review.response"))
        })
    }
}
#[tokio::test]
async fn model_review_shares_budget_and_does_not_replace_the_agent_step() {
    for capacity in [1, 2] {
        let (fixture, mut profile, mut bindings, model, _) =
            setup(&["candidate", r#"{"verdict":"pass"}"#], vec![]);
        configure_router(&mut bindings, false, true);
        profile.limits.max_model_calls = capacity.try_into().unwrap();
        bindings.verification = Some(Arc::new(
            VerificationRuntime::new(
                scope(),
                vec![],
                vec![Arc::new(ModelReview {
                    binding: id("primary"),
                })],
                VerificationLimits::default(),
            )
            .unwrap(),
        ));
        let agent = create_agent(profile, bindings).unwrap();
        let handle = fixture.started(&agent, "request").await;
        let outcome = completed(handle.outcome(&context()).await.unwrap());
        if capacity == 1 {
            assert_eq!(
                outcome.result,
                OutcomeResult::Exhausted {
                    budget: BudgetKind::ModelCalls
                }
            );
        } else {
            assert_eq!(
                outcome.result,
                OutcomeResult::Succeeded {
                    completion_basis: CompletionBasis::Verified
                }
            );
        }
        assert_eq!(model.calls.load(Ordering::SeqCst), capacity as usize);
        let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
        assert_eq!(
            saved.snapshot.model_step_id.as_ref(),
            Some(&saved.snapshot.model_ledger[0].model_step_id)
        );
        assert_eq!(outcome.usage.model_calls, capacity);
        if capacity == 2 {
            assert_eq!(
                saved.snapshot.model_ledger[1].purpose,
                ModelPurpose::Verification
            );
            assert_ne!(
                saved.snapshot.model_ledger[0].model_step_id,
                saved.snapshot.model_ledger[1].model_step_id
            );
        }
    }
}

struct PausedCheck {
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
impl Verifier for PausedCheck {
    fn definition(&self) -> VerifierDefinition {
        VerifierDefinition {
            verifier_ref: reference("quality"),
            criteria_ref: reference("criteria"),
            criteria: "Wait for controlled review completion.".into(),
            configuration: Default::default(),
        }
    }
    fn verify<'a>(
        &'a self,
        _: &'a VerificationInput,
        _: &'a VerifierContext<'a>,
    ) -> PortFuture<'a, VerificationDecision> {
        Box::pin(async move {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
            Ok(VerificationDecision::Pass {})
        })
    }
}
#[tokio::test]
async fn cancellation_and_timeout_cannot_adopt_a_late_verifier_pass() {
    for cancelled in [true, false] {
        let (fixture, profile, mut bindings, model, _) = setup(&["candidate"], vec![]);
        let check = Arc::new(PausedCheck {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        });
        bindings.verification = Some(Arc::new(
            VerificationRuntime::new(
                scope(),
                vec![],
                vec![check.clone()],
                VerificationLimits {
                    timeout_ms: if cancelled { 1000 } else { 20 },
                    ..Default::default()
                },
            )
            .unwrap(),
        ));
        let agent = create_agent(profile, bindings).unwrap();
        let handle = fixture.started(&agent, "request").await;
        check.entered.notified().await;
        if cancelled {
            completed(handle.cancel(id("stop-review"), &context()).await.unwrap());
        }
        let outcome = completed(handle.outcome(&context()).await.unwrap());
        check.release.add_permits(1);
        if cancelled {
            assert_eq!(outcome.result.status(), RunStatus::Cancelled);
        } else {
            assert!(
                matches!(outcome.result,OutcomeResult::Failed{ref failure} if failure.code==id("verification_unavailable"))
            );
        }
        assert!(outcome.verification.is_none());
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            completed(handle.outcome(&context()).await.unwrap()),
            outcome
        );
    }
}
#[tokio::test]
async fn decision_commit_failure_keeps_the_candidate_and_ack_loss_does_not_repeat_verification() {
    for lose_ack in [false, true] {
        let (fixture, profile, mut bindings, model, checks) =
            setup(&["candidate"], vec![Ok(VerificationDecision::Pass {})]);
        bindings.state = Arc::new(FinalCommitStore::new(
            fixture.store.clone(),
            if lose_ack {
                FinalCommitMode::LoseVerificationAcknowledgement
            } else {
                FinalCommitMode::RejectVerification
            },
        ));
        let agent = create_agent(profile, bindings).unwrap();
        let handle = fixture.started(&agent, "request").await;
        let outcome = handle.outcome(&context()).await;
        let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
        if lose_ack {
            assert_eq!(
                completed(outcome.unwrap()).result.status(),
                RunStatus::Succeeded
            );
            assert_eq!(saved.snapshot.verification_records.len(), 1);
        } else {
            assert_eq!(outcome.unwrap_err().code, ErrorCode::PersistenceUnavailable);
            assert!(saved.snapshot.candidate_ref.is_some());
            assert!(saved.snapshot.verification_records.is_empty());
            assert_eq!(saved.snapshot.status, RunStatus::Running);
        }
        assert_eq!(checks.calls.load(Ordering::SeqCst), 1);
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn denied_review_finishes_without_reexecuting_the_candidate() {
    let (fixture, profile, bindings, model, checks) = setup(
        &["candidate"],
        vec![Ok(VerificationDecision::Wait {
            reason: "Human evidence review.".into(),
            expires_at_ms: None,
        })],
    );
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let waiting = completed(handle.outcome(&context()).await.unwrap());
    let OutcomeResult::Waiting { wait } = waiting.result else {
        panic!()
    };
    let WaitTarget::Approval { target } = wait.target else {
        panic!()
    };
    let command = ResumeCommand {
        run_id: handle.run_id().clone(),
        expected_revision: waiting.checkpoint_revision,
        command_id: id("deny-review"),
        action: ResumeAction::Deny {
            wait_id: wait.wait_id,
            target,
            reason: "Evidence rejected.".into(),
        },
    };
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    let outcome = completed(resumed.outcome(&context()).await.unwrap());
    assert!(
        matches!(outcome.result,OutcomeResult::Failed{ref failure} if failure.code==id("verification_failed"))
    );
    assert_eq!(
        outcome.verification.unwrap().verdict,
        VerificationVerdict::Fail
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(checks.calls.load(Ordering::SeqCst), 1);
}

fn replace_reference(
    value: &mut serde_json::Value,
    old: &serde_json::Value,
    new: &serde_json::Value,
) {
    if value == old {
        *value = new.clone();
        return;
    }
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                replace_reference(value, old, new)
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values_mut() {
                replace_reference(value, old, new)
            }
        }
        _ => {}
    }
}
fn rehash_records(image: &mut serde_json::Value) {
    for _ in 0..image["records"].as_array().unwrap().len() * 2 {
        let changed = image["records"]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|record| {
                let digest = serde_json::to_value(canonical_digest(&record["value"])).unwrap();
                if record["reference"]["digest"] == digest {
                    None
                } else {
                    let old = record["reference"].clone();
                    let mut new = old.clone();
                    new["digest"] = digest;
                    Some((old, new))
                }
            });
        let Some((old, new)) = changed else {
            return;
        };
        replace_reference(image, &old, &new);
    }
    panic!("record graph did not converge")
}
#[tokio::test]
async fn recalculating_record_hashes_cannot_replace_the_verified_model_candidate() {
    let (fixture, profile, bindings, _, _) = setup(
        &["original candidate"],
        vec![Ok(VerificationDecision::Pass {})],
    );
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    completed(handle.outcome(&context()).await.unwrap());
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    let checkpoint = fixture.store.export_checkpoint(&scope()).unwrap();
    let mut image = serde_json::to_value(checkpoint).unwrap();
    let reference = saved.snapshot.candidate_ref.unwrap();
    let target = image["records"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|record| record["reference"]["record_id"] == json!(reference.record_id))
        .unwrap();
    target["value"]["output"][0]["text"] = json!("forged candidate");
    rehash_records(&mut image);
    let result =
        StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(&image));
    assert!(result.is_err());
}
#[tokio::test]
async fn review_resume_rejects_changed_criteria_and_wrong_candidate_before_any_new_calls() {
    let (fixture, profile, bindings, model, checks) = setup(
        &["candidate"],
        vec![Ok(VerificationDecision::Wait {
            reason: "Review criteria.".into(),
            expires_at_ms: None,
        })],
    );
    let agent = create_agent(profile.clone(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let waiting = completed(handle.outcome(&context()).await.unwrap());
    let OutcomeResult::Waiting { wait } = waiting.result else {
        panic!()
    };
    let WaitTarget::Approval { target } = wait.target else {
        panic!()
    };
    let command = ResumeCommand {
        run_id: handle.run_id().clone(),
        expected_revision: waiting.checkpoint_revision,
        command_id: id("review"),
        action: ResumeAction::Approve {
            wait_id: wait.wait_id,
            target,
        },
    };
    let mut wrong = command.clone();
    if let ResumeAction::Approve {
        target: ApprovalTarget::Candidate { candidate_ref, .. },
        ..
    } = &mut wrong.action
    {
        candidate_ref.record_id = id("another-candidate");
    }
    assert!(agent.resume(wrong, context()).await.is_err());
    let mut bindings = fixture.bindings();
    bindings.profile_resolver = Arc::new(Metadata);
    let mut definition = checks.definition();
    definition.configuration.insert("minimum".into(), json!(4));
    let verifier = SchemaVerifier::new(definition, json!({"type":"object"})).unwrap();
    bindings.verification = Some(Arc::new(
        VerificationRuntime::new(
            scope(),
            vec![],
            vec![Arc::new(verifier)],
            VerificationLimits::default(),
        )
        .unwrap(),
    ));
    let changed = create_agent(profile, bindings).unwrap();
    assert_eq!(
        changed.resume(command, context()).await.unwrap_err().code,
        ErrorCode::ContextMismatch
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(checks.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .status,
        RunStatus::Waiting
    );
}

#[tokio::test]
async fn a_verifier_decision_cannot_commit_without_its_required_event() {
    let (fixture, profile, mut bindings, _, _) =
        setup(&["candidate"], vec![Ok(VerificationDecision::Pass {})]);
    bindings.state = Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::OmitVerificationEvent,
    ));
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let error = handle.outcome(&context()).await.unwrap_err();
    assert!(matches!(
        error.code,
        ErrorCode::InvalidEvent | ErrorCode::InvalidSnapshot
    ));
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert!(saved.snapshot.verification_records.is_empty());
    assert_ne!(saved.snapshot.status, RunStatus::Succeeded);
}

#[tokio::test]
async fn restored_feedback_cannot_be_promoted_to_a_new_user_request() {
    let (fixture, profile, bindings, _, _) = setup(
        &["first", "second"],
        vec![
            Ok(VerificationDecision::Revise {
                feedback: "Add evidence.".into(),
            }),
            Ok(VerificationDecision::Pass {}),
        ],
    );
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    completed(handle.outcome(&context()).await.unwrap());
    let mut image =
        serde_json::to_value(fixture.store.export_checkpoint(&scope()).unwrap()).unwrap();
    fn promote(value: &mut serde_json::Value) -> bool {
        match value {
            serde_json::Value::Object(map) => {
                if map.get("origin") == Some(&json!("verification"))
                    && map.contains_key("message_id")
                {
                    map.insert("origin".into(), json!("user"));
                    return true;
                }
                map.values_mut().any(promote)
            }
            serde_json::Value::Array(values) => values.iter_mut().any(promote),
            _ => false,
        }
    }
    assert!(promote(&mut image));
    assert!(
        StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(&image))
            .is_err()
    );
}

#[tokio::test]
async fn a_verifier_can_use_its_own_explicit_logical_model_binding() {
    let (fixture, profile, mut bindings, model, _) =
        setup(&["candidate", r#"{"verdict":"pass"}"#], vec![]);
    let mut policy = bindings.router.snapshot().policy().clone();
    let mut review = policy.rules[0].clone();
    review.model_binding = id("review-model");
    review.purpose = ModelPurpose::Verification;
    policy.rules.push(review);
    bindings.router = Arc::new(Router {
        snapshot: RoutingSnapshot::new(bindings.router.snapshot().catalog().clone(), policy)
            .unwrap(),
        queries: AtomicUsize::new(0),
        snapshots: AtomicUsize::new(0),
    });
    bindings.verification = Some(Arc::new(
        VerificationRuntime::new(
            scope(),
            vec![],
            vec![Arc::new(ModelReview {
                binding: id("review-model"),
            })],
            VerificationLimits::default(),
        )
        .unwrap(),
    ));
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::Verified
        }
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn replay_precedes_current_verifier_lookup_and_new_request_limits() {
    let (fixture, profile, bindings, model, checks) =
        setup(&["checked"], vec![Ok(VerificationDecision::Pass {})]);
    let agent = create_agent(profile.clone(), bindings).unwrap();
    let first = fixture.started(&agent, "stored").await;
    let original = completed(first.outcome(&context()).await.unwrap());
    let mut changed = fixture.bindings();
    changed.settings.max_request_bytes = 1;
    let restarted = create_agent(profile.clone(), changed).unwrap();
    let replay = completed(restarted.start(request("stored"), context()).await.unwrap());
    assert_eq!(replay.run_id(), first.run_id());
    assert_eq!(
        completed(replay.outcome(&context()).await.unwrap()),
        original
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(checks.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        restarted
            .start(request("new-too-large"), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidContract
    );
    let missing_verifier = create_agent(profile, fixture.bindings()).unwrap();
    assert_eq!(
        missing_verifier
            .start(request("new-no-verifier"), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    assert!(
        fixture
            .store
            .find_request(&scope(), &id("session"), &id("new-no-verifier"))
            .await
            .unwrap()
            .is_none()
    );
}
```

## `tests/support/agent_consumer.rs`

```rust
// Synthetic adapters and metadata inspector: no provider network calls are made.
// The fixed clock supports deterministic accounting; this does not test timeouts.
use futures_util::{TryStreamExt, stream};
use serde_json::json;
use std::collections::BTreeSet;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;
use wickle_model_router::{ModelDispatcherEntry, PolicyModelRouter, RegistryModelDispatcher};
use wickle_state_sqlite::SqliteStateStore;

fn id(s: &str) -> Id {
    Id::new(s).expect("example identifiers")
}
struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        r: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id("revision-1")),
                    ..r.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("registered model")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: None,
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

fn reference(name: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id("1"),
    }
}
fn routing_snapshot(scope: &Scope) -> Result<RoutingSnapshot, ContractError> {
    let mut models = vec![];
    let mut bindings = vec![];
    for name in ["first", "second"] {
        let capabilities = ModelCapabilities {
            revision: id("capabilities"),
            features: BTreeSet::from([id("text")]),
            options_schema: json!({"type":"object","properties":{"reasoning_effort":{"enum":["high"]}},"additionalProperties":false}),
            context_window: 4096.try_into().unwrap(),
            max_output_tokens: 512.try_into().unwrap(),
        };
        let model = ModelDefinition {
            model_key: id(name),
            family: id("example"),
            provider: id(name),
            model_id: id("example-model"),
            model_version: id("release-1"),
            version_semantics: VersionSemantics::Pinned,
            lifecycle: ModelLifecycle::Active,
            capabilities: capabilities.clone(),
            evidence: vec![],
        };
        let mut binding = ModelBinding {
            binding: reference(name),
            model: model.reference(),
            requested_model: model.model_id.clone(),
            adapter: reference("adapter"),
            connection_ref: reference(&format!("{name}-account")),
            target: JsonObject::new(),
            target_schema: json!({"type":"object","additionalProperties":false}),
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("v1"),
            },
            deployment_revision: None,
            version_semantics: VersionSemantics::Pinned,
            capabilities,
            support: ModelSupportStatus::ContractTested,
            evidence: vec![],
        };
        binding.evidence.push(ModelValidationEvidence {
            kind: ModelValidationKind::ContractTest,
            binding_digest: binding.contract_digest(&model)?,
            checked_at_ms: 1000,
            evidence_ref: id("synthetic-fixture"),
            passed: true,
        });
        models.push(model);
        bindings.push(binding);
    }
    RoutingSnapshot::new(
        ModelCatalogSnapshot {
            revision: id("catalog"),
            scope: scope.clone(),
            models,
            bindings,
            aliases: vec![],
        },
        RoutingPolicy {
            revision: id("policy"),
            scope: scope.clone(),
            rules: vec![RoutingRule {
                model_binding: id("primary"),
                purpose: ModelPurpose::Agent,
                primary: reference("first"),
                fallbacks: vec![reference("second")],
                fallback_on: vec![ModelFailureKind::RateLimited],
                version_policy: VersionPolicy::RequirePinned,
                min_support: ModelSupportStatus::ContractTested,
            }],
        },
    )
}
struct TemporaryDatabase(std::path::PathBuf);
impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct ExampleClock;
impl Clock for ExampleClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: 1000,
            monotonic_ms: 1000,
        })
    }
    fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}
struct ExamplePolicy;
impl PolicyPort for ExamplePolicy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::InvokeModel { route, .. } = &request.action {
                if route.connection_ref.id == id(&format!("{}-account", route.provider)) {
                    return Ok(PolicyDecision::Allow {});
                }
            }
            Ok(
                if matches!(request.action, PolicyAction::InvokeModel { .. }) {
                    PolicyDecision::Deny {
                        reason: id("unknown-account"),
                    }
                } else {
                    PolicyDecision::Allow {}
                },
            )
        })
    }
}
struct ExampleInspector;
// This echo is a fixture only. A real inspector must read authoritative provider
// metadata instead of presenting requested values as independently observed facts.
impl ModelRouteInspector for ExampleInspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        _: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: ModelRouteAvailability::Available,
                model_id: Some(route.model_id.clone()),
                model_version: Some(route.model_version.clone()),
                deployment_revision: None,
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id("synthetic-metadata-check"),
            })
        })
    }
}
struct ExampleModel {
    route: ResolvedModelRoute,
    calls: AtomicUsize,
    fail: bool,
}
impl ModelPort for ExampleModel {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: self.route.provider.clone(),
            adapter: self.route.adapter.clone(),
            connection_ref: self.route.connection_ref.clone(),
        }
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        assert_eq!(
            request.options.get("reasoning_effort"),
            Some(&json!("high"))
        );
        assert_eq!(request.route, self.route);
        self.calls.fetch_add(1, Ordering::SeqCst);
        let events = if self.fail {
            vec![Ok(ModelEvent::ResponseError {
                kind: ModelFailureKind::RateLimited,
                metadata: ModelResponseMetadata::default(),
            })]
        } else {
            vec![
                Ok(ModelEvent::TextDelta {
                    text: "second provider result".into(),
                }),
                Ok(ModelEvent::ResponseCompleted {
                    finish: ModelFinish::Stop,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![],
                }),
            ]
        };
        Box::pin(stream::iter(events))
    }
}
struct Estimate;
impl ModelTokenEstimator for Estimate {
    fn estimate(&self, request: &ModelRequest) -> Result<u64, ContractError> {
        // Conservative test estimate; it is not provider-measured token usage.
        serde_json::to_vec(request)
            .map(|bytes| bytes.len() as u64)
            .map_err(|_| ContractError::new(ErrorCode::InvalidContext, "example.estimate"))
    }
}
fn completed<T>(value: Guarded<T>) -> Result<T, Box<dyn std::error::Error>> {
    match value {
        Guarded::Completed(value) => Ok(value),
        Guarded::ApprovalRequired(_) => Err("unexpected approval".into()),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let temporary = TemporaryDatabase(
        std::env::temp_dir().join(format!("wickle-agent-{}", RandomIdSource.next_id()?)),
    );
    std::fs::create_dir(&temporary.0)?;
    let database = temporary.0.join("state.sqlite3");
    let store = Arc::new(SqliteStateStore::open(&database)?);
    let snapshot = routing_snapshot(&scope)?;
    let first = Arc::new(ExampleModel {
        route: snapshot.route_for_binding(&reference("first"))?,
        calls: AtomicUsize::new(0),
        fail: true,
    });
    let second = Arc::new(ExampleModel {
        route: snapshot.route_for_binding(&reference("second"))?,
        calls: AtomicUsize::new(0),
        fail: false,
    });
    let policy = Arc::new(PolicyGate::new(
        Arc::new(ExamplePolicy),
        Duration::from_secs(1),
    )?);
    let exchange = Arc::new(
        ModelExchange::with_dispatcher(
            Arc::new(RegistryModelDispatcher::new(vec![
                ModelDispatcherEntry {
                    scope: scope.clone(),
                    port: first.clone(),
                },
                ModelDispatcherEntry {
                    scope: scope.clone(),
                    port: second.clone(),
                },
            ])?),
            policy.clone(),
        )
        .with_route_inspector(Arc::new(ExampleInspector), Duration::from_secs(1))?,
    );
    let profile = AgentProfile::from_json(
        r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
        "name":"Assistant","description":"Agent consumer","instructions":{"text":"Use supplied information"},
        "model_binding":"primary","tools":[],"skills":[],"connectors":[],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":4,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":2,"max_elapsed_ms":10000}
    }"#,
    )?;
    let agent = create_agent(
        profile,
        AgentBindings {
            scope: scope.clone(),
            state: store.clone(),
            policy,
            profile_resolver: Arc::new(Catalog),
            model_exchange: exchange,
            router: Arc::new(PolicyModelRouter::new(snapshot)?),
            host_instructions: vec!["Preserve the requested output.".into()],
            system_inputs: SystemInputRegistry::new(vec![])?,
            clock: Arc::new(ExampleClock),
            ids: Arc::new(RandomIdSource),
            tools: None,
            system_input_resolver: None, external_receipt_verifier: None, components: None, context_sources: None, context_token_estimator: None, context_runtime: None, verification: None, skills: None, artifacts: None, hooks: None,
            token_estimator: Arc::new(Estimate),
            settings: AgentSettings {
                max_output_tokens: 128.try_into()?,
                require_durable: true,
                ..AgentSettings::default()
            },
        },
    )?;
    assert_eq!(first.calls.load(Ordering::SeqCst), 0);
    assert_eq!(second.calls.load(Ordering::SeqCst), 0);
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id("actor"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    );
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Retrieve the available result".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::from([("reasoning_effort".into(), json!("high"))]),
        max_output_tokens: None,
        output_contract: None,
    };
    let handle = completed(agent.start(request.clone(), context.clone()).await?)?;
    let run_id = handle.run_id().clone();
    let mut events = handle.events(0, context.clone());
    use futures_util::StreamExt;
    let started = events.next().await.ok_or("missing admission event")??;
    assert_eq!(started.event_type, "run.started");
    drop(events);
    let outcome = completed(handle.outcome(&context).await?)?;
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "second provider result".into()
        }]
    );
    assert_eq!(outcome.usage.model_calls, 2);
    assert_eq!(outcome.usage.recovery_attempts, 1);
    let replay = completed(agent.start(request.clone(), context.clone()).await?)?;
    assert_eq!(replay.run_id(), &run_id);
    assert_eq!(completed(replay.outcome(&context).await?)?, outcome);
    let submitted = store.read_execution(&scope, &run_id).await?.submitted.ok_or("submitted identity missing")?;
    submitted.validate(JsonTextLimits::default())?;
    let mut changed = request;
    changed.model_options.insert("reasoning_effort".into(), json!("changed"));
    assert_eq!(agent.start(changed, context.clone()).await.unwrap_err().code, ErrorCode::RequestConflict);
    assert_eq!(store.read_execution(&scope, &run_id).await?.submitted.as_ref().map(|s|s.digest()), Some(submitted.digest()));

    assert_eq!(first.calls.load(Ordering::SeqCst), 1);
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    let view = completed(agent.get_run(&run_id, &context).await?)?;
    assert_eq!(view.status, RunStatus::Succeeded);
    let events: Vec<_> = handle
        .events(started.seq.get(), context.clone())
        .try_collect()
        .await?;
    assert_eq!(
        events.last().ok_or("missing finish event")?.event_type,
        "run.finished"
    );
    drop(store);
    let restored = SqliteStateStore::open(&database)?
        .load(&scope, &run_id)
        .await?;
    assert_eq!(restored.snapshot.outcome, Some(outcome));
    assert!(restored.session.active_run_id.is_none());
    println!(
        "agent consumer: pure construction, detached execution after observer drop, fallback under shared budgets, stored outcome and event replay, duplicate request without new model calls, SQLite reopen"
    );
    Ok(())
}
```

## `tests/support/gather_consumer.rs`

```rust
// Independent retrieval/aggregation Host with replaceable memory and graph sources.
#[allow(dead_code)]
mod host {
    include!("source_consumer.rs");
    use serde_json::Value;
    use std::sync::Mutex;
    const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";
    fn ready(
        request: &ContextRequest,
        native: &str,
        origin: ContextOrigin,
        value: serde_json::Value,
    ) -> ContextResult {
        ContextResult::Ready {
            items: vec![ContextItem::new(
                id("row-1"),
                origin,
                reference(native),
                request.scope.clone(),
                vec![InputContent::Json { value }],
                ContextLifetime::Run {
                    run_id: request.run_id.clone(),
                },
                ContextPriority::Required,
            )],
            source_revision: Some(id("dataset-1")),
            reported_usage: None,
        }
    }
    fn authorize(
        request: &ContextUseRequest,
        context: &ContextCallContext,
        native: &str,
    ) -> Result<(), ContractError> {
        if request.request.scope != context.scope
            || request
                .items
                .iter()
                .any(|item| item.item_id != id("row-1") || item.source_ref != reference(native))
        {
            return Err(ContractError::new(
                ErrorCode::AccessDenied,
                "business.source",
            ));
        }
        Ok(())
    }
    // The two providers deliberately use different backing representations.
    struct MemoryA {
        period: String,
        calls: Arc<AtomicUsize>,
    }
    impl ContextSource for MemoryA {
        fn provide<'a>(
            &'a self,
            request: &'a ContextRequest,
            _: &'a ContextCallContext,
        ) -> PortFuture<'a, ContextResult> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(ready(
                    request,
                    "memory-a",
                    ContextOrigin::Memory,
                    json!({"period":self.period}),
                ))
            })
        }
        fn authorize_use<'a>(
            &'a self,
            request: &'a ContextUseRequest,
            context: &'a ContextCallContext,
        ) -> PortFuture<'a, ()> {
            Box::pin(async move { authorize(request, context, "memory-a") })
        }
    }
    struct MemoryB {
        records: std::collections::BTreeMap<String, String>,
        calls: Arc<AtomicUsize>,
    }
    impl ContextSource for MemoryB {
        fn provide<'a>(
            &'a self,
            request: &'a ContextRequest,
            _: &'a ContextCallContext,
        ) -> PortFuture<'a, ContextResult> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let period = self.records.get("preferred.period").ok_or_else(|| {
                    ContractError::new(ErrorCode::ComponentUnavailable, "memory.record")
                })?;
                Ok(ready(
                    request,
                    "memory-b",
                    ContextOrigin::Memory,
                    json!({"period":period}),
                ))
            })
        }
        fn authorize_use<'a>(
            &'a self,
            request: &'a ContextUseRequest,
            context: &'a ContextCallContext,
        ) -> PortFuture<'a, ()> {
            Box::pin(async move { authorize(request, context, "memory-b") })
        }
    }
    struct Graph {
        calls: Arc<AtomicUsize>,
    }
    impl ContextSource for Graph {
        fn provide<'a>(
            &'a self,
            request: &'a ContextRequest,
            _: &'a ContextCallContext,
        ) -> PortFuture<'a, ContextResult> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(ready(
                    request,
                    "graph",
                    ContextOrigin::Retrieval,
                    json!({"periods":["quarter","month"],"relationship":"period_has_records"}),
                ))
            })
        }
        fn authorize_use<'a>(
            &'a self,
            request: &'a ContextUseRequest,
            context: &'a ContextCallContext,
        ) -> PortFuture<'a, ()> {
            Box::pin(async move { authorize(request, context, "graph") })
        }
    }
    struct GatherCatalog;
    impl ProfileResolver for GatherCatalog {
        fn resolve<'a>(
            &'a self,
            reference: &'a ComponentRef,
            scope: &'a Scope,
        ) -> PortFuture<'a, ComponentMetadata> {
            Box::pin(async move {
                if reference.kind == ComponentKind::Tool && reference.id != id("lookup") {
                    return Err(ContractError::new(
                        ErrorCode::ComponentUnavailable,
                        "profile.tool",
                    ));
                }
                let mut metadata = Catalog.resolve(reference, scope).await?;
                if reference.kind == ComponentKind::Tool {
                    metadata.model_name = Some(id("lookup"));
                }
                Ok(metadata)
            })
        }
    }
    struct GatherPolicy;
    impl PolicyPort for GatherPolicy {
        fn authorize<'a>(
            &'a self,
            request: &'a PolicyRequest,
            _: PolicyContext<'a>,
        ) -> PortFuture<'a, PolicyDecision> {
            Box::pin(async move {
                Ok(
                    if matches!(&request.action,PolicyAction::ExecuteTool{input} if input.execution_args().get("workspace_id")!=Some(&json!(WORKSPACE)))
                    {
                        PolicyDecision::Deny {
                            reason: id("foreign-workspace"),
                        }
                    } else {
                        PolicyDecision::Allow {}
                    },
                )
            })
        }
    }
    struct Lookup {
        scope: Scope,
        calls: AtomicUsize,
        seen: Mutex<Vec<JsonObject>>,
    }
    impl ToolExecutor for Lookup {
        fn execute<'a>(
            &'a self,
            args: &'a JsonObject,
            context: &'a ToolExecutionContext,
        ) -> PortFuture<'a, ToolExecutionResult> {
            Box::pin(async move {
                assert_eq!(context.scope, self.scope);
                assert_eq!(args["workspace_id"], WORKSPACE);
                assert_eq!(args.len(), 3);
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.seen.lock().unwrap().push(args.clone());
                let period = args["query"].as_str().unwrap();
                let limit = args["limit"].as_u64().unwrap() as usize;
                let amounts = match period {
                    "quarter" => vec![20, 22, 999],
                    "month" => vec![40, 60, 999],
                    _ => {
                        return Err(ContractError::new(
                            ErrorCode::InvalidContract,
                            "lookup.query",
                        ));
                    }
                };
                let rows: Vec<_> = amounts
                    .into_iter()
                    .take(limit)
                    .map(|amount| json!({"amount":amount}))
                    .collect();
                let total: i64 = rows.iter().map(|row| row["amount"].as_i64().unwrap()).sum();
                let evidence = EvidenceRef {
                    source_id: id("records"),
                    version: id("1"),
                    location: id(period),
                    content_hash: id(canonical_digest(&json!(rows)).as_str()),
                    quote: None,
                };
                Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Succeeded {
                        value: json!({"rows":rows,"total":total,"evidence":evidence}),
                    },
                    effect: ToolEffect::NotApplied,
                    receipt: None,
                })
            })
        }
    }
    struct GatherModel {
        period: &'static str,
        calls: AtomicUsize,
    }
    impl ModelPort for GatherModel {
        fn binding(&self) -> ModelPortBinding {
            ModelPortBinding {
                provider: id("synthetic"),
                adapter: reference("synthetic-adapter"),
                connection_ref: reference("synthetic-connection"),
            }
        }
        fn generate<'a>(
            &'a self,
            request: &'a ModelRequest,
            _: &'a ModelCallContext,
        ) -> PortStream<'a, ModelEvent> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(call < 2);
            let contexts: Vec<_> = request
                .messages
                .iter()
                .flat_map(|message| {
                    message
                        .content
                        .iter()
                        .filter_map(move |content| match content {
                            ModelContent::Json { value } if value["kind"] == "context_data" => {
                                assert_eq!(message.role, ModelRole::User);
                                Some(value)
                            }
                            _ => None,
                        })
                })
                .collect();
            assert_eq!(contexts.len(), 2);
            assert_ne!(contexts[0]["item_id"], contexts[1]["item_id"]);
            let memory = contexts
                .iter()
                .find(|value| value["origin"] == "memory")
                .unwrap();
            let graph = contexts
                .iter()
                .find(|value| value["origin"] == "retrieval")
                .unwrap();
            let period = memory["content"][0]["value"]["period"].as_str().unwrap();
            assert_eq!(period, self.period);
            assert!(
                graph["content"][0]["value"]["periods"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(period))
            );
            assert_eq!(request.tools.len(), 1);
            let properties = request.tools[0].model_input_schema["properties"]
                .as_object()
                .unwrap();
            assert_eq!(properties.len(), 2);
            assert!(properties.contains_key("query") && properties.contains_key("limit"));
            let events = if call == 0 {
                vec![
                    Ok(ModelEvent::ToolArgumentsDelta {
                        index: 0,
                        provider_call_id: Some("lookup-rows".into()),
                        name: Some("lookup".into()),
                        delta: json!({"query":period,"limit":2}).to_string(),
                    }),
                    Ok(ModelEvent::ResponseCompleted {
                        finish: ModelFinish::ToolCalls,
                        metadata: Default::default(),
                        continuation: vec![],
                    }),
                ]
            } else {
                let result = request
                    .messages
                    .iter()
                    .flat_map(|m| &m.content)
                    .find_map(|v| match v {
                        ModelContent::ToolResult {
                            provider_call_id,
                            content,
                        } if provider_call_id == &id("lookup-rows") => Some(content),
                        _ => None,
                    })
                    .unwrap();
                assert_eq!(result["status"], "succeeded");
                let value = &result["content"][0]["value"];
                let evidence: EvidenceRef =
                    serde_json::from_value(value["evidence"].clone()).unwrap();
                assert_eq!(evidence.location, id(period));
                assert_eq!(
                    evidence.content_hash.as_str(),
                    canonical_digest(&value["rows"]).as_str()
                );
                vec![
                    Ok(ModelEvent::TextDelta {
                        text: json!({"period":period,"total":value["total"],"evidence":evidence})
                            .to_string(),
                    }),
                    Ok(ModelEvent::ResponseCompleted {
                        finish: ModelFinish::Stop,
                        metadata: Default::default(),
                        continuation: vec![],
                    }),
                ]
            };
            Box::pin(stream::iter(events))
        }
    }
    fn gather_routing(scope: &Scope) -> Result<RoutingSnapshot, ContractError> {
        let capabilities = ModelCapabilities {
            revision: id("features"),
            features: BTreeSet::from([id("text"), id("tool_calling")]),
            options_schema: json!({"type":"object","additionalProperties":false}),
            context_window: 4096.try_into().unwrap(),
            max_output_tokens: 512.try_into().unwrap(),
        };
        let model = ModelDefinition {
            model_key: id("synthetic"),
            family: id("synthetic"),
            provider: id("synthetic"),
            model_id: id("fixture-model"),
            model_version: id("release-1"),
            version_semantics: VersionSemantics::Pinned,
            lifecycle: ModelLifecycle::Active,
            capabilities: capabilities.clone(),
            evidence: vec![],
        };
        let mut binding = ModelBinding {
            binding: reference("primary"),
            model: model.reference(),
            requested_model: model.model_id.clone(),
            adapter: reference("synthetic-adapter"),
            connection_ref: reference("synthetic-connection"),
            target: JsonObject::new(),
            target_schema: json!({"type":"object","additionalProperties":false}),
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("v1"),
            },
            deployment_revision: None,
            version_semantics: VersionSemantics::Pinned,
            capabilities,
            support: ModelSupportStatus::ContractTested,
            evidence: vec![],
        };
        binding.evidence.push(ModelValidationEvidence {
            kind: ModelValidationKind::ContractTest,
            binding_digest: binding.contract_digest(&model)?,
            checked_at_ms: 1000,
            evidence_ref: id("synthetic-contract-test"),
            passed: true,
        });
        RoutingSnapshot::new(
            ModelCatalogSnapshot {
                revision: id("catalog-1"),
                scope: scope.clone(),
                models: vec![model],
                bindings: vec![binding],
                aliases: vec![],
            },
            RoutingPolicy {
                revision: id("policy-1"),
                scope: scope.clone(),
                rules: vec![RoutingRule {
                    model_binding: id("primary"),
                    purpose: ModelPurpose::Agent,
                    primary: reference("primary"),
                    fallbacks: vec![],
                    fallback_on: vec![],
                    version_policy: VersionPolicy::RequirePinned,
                    min_support: ModelSupportStatus::ContractTested,
                }],
            },
        )
    }

    struct Scenario {
        scope: Scope,
        store: Arc<SqliteStateStore>,
        memory: Arc<dyn ContextSource>,
        memory_id: &'static str,
        graph: Arc<Graph>,
        model: Arc<GatherModel>,
        tool: Arc<Lookup>,
    }
    impl Scenario {
        fn build(&self, profile: AgentProfile) -> Result<Agent, ContractError> {
            let policy = Arc::new(PolicyGate::new(
                Arc::new(GatherPolicy),
                Duration::from_secs(5),
            )?);
            let clock = Arc::new(SystemClock::new());
            let ids = Arc::new(RandomIdSource);
            let sources = Arc::new(ContextSourceRuntime::new(
                self.store.clone(),
                policy.clone(),
                clock.clone(),
                ids.clone(),
                Arc::new(ContextSourceRegistry::new(
                    self.scope.clone(),
                    vec![
                        ContextSourceRegistration {
                            selection: ContextSourceRef::Catalog(CatalogSourceRef {
                                source_id: id(self.memory_id),
                                version: id("1"),
                            }),
                            definition: ContextSourceDefinition {
                                source: reference(self.memory_id),
                                origin: ContextOrigin::Memory,
                                contract_version: 1,
                            },
                            source: self.memory.clone(),
                        },
                        ContextSourceRegistration {
                            selection: ContextSourceRef::Catalog(CatalogSourceRef {
                                source_id: id("graph"),
                                version: id("1"),
                            }),
                            definition: ContextSourceDefinition {
                                source: reference("graph"),
                                origin: ContextOrigin::Retrieval,
                                contract_version: 1,
                            },
                            source: self.graph.clone(),
                        },
                    ],
                )?),
                Arc::new(SourceEstimate),
            )?);
            let inputs = SystemInputRegistry::new(vec![
                SystemInputDefinition {
                    key: id("workspace_id"),
                    version: id("1"),
                    value_schema: json!({"type":"string","format":"uuid"}),
                    source: SystemInputSource::Run {},
                },
                SystemInputDefinition {
                    key: id("private_note"),
                    version: id("1"),
                    value_schema: json!({"type":"string"}),
                    source: SystemInputSource::Run {},
                },
            ])?;
            let compiled=SchemaCompiler::new().compile(ToolDescriptor{tool:reference("lookup"),name:id("lookup"),description:"Read and aggregate authorized records".into(),input_schema:json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":2},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","limit","workspace_id"],"additionalProperties":false}),agent_parameters:vec!["query".into(),"limit".into()],system_bindings:None,output_schema:json!({"type":"object","properties":{"rows":{"type":"array","items":{"type":"object","properties":{"amount":{"type":"integer"}},"required":["amount"],"additionalProperties":false}},"total":{"type":"integer"},"evidence":{"type":"object","properties":{"source_id":{"type":"string"},"version":{"type":"string"},"location":{"type":"string"},"content_hash":{"type":"string"}},"required":["source_id","version","location","content_hash"],"additionalProperties":false}},"required":["rows","total","evidence"],"additionalProperties":false}),side_effect:ToolSideEffect::ReadOnly,concurrency:ToolConcurrency::Serial,retry:ToolRetryPolicy::Never,reconcile:false,max_output_bytes:4096.try_into().unwrap()},&inputs)?;
            create_agent(
                profile,
                AgentBindings {
                    scope: self.scope.clone(),
                    state: self.store.clone(),
                    policy: policy.clone(),
                    profile_resolver: Arc::new(GatherCatalog),
                    model_exchange: Arc::new(
                        ModelExchange::new(self.model.clone(), policy)
                            .with_route_inspector(Arc::new(Inspector), Duration::from_secs(5))?,
                    ),
                    router: Arc::new(PolicyModelRouter::new(gather_routing(&self.scope)?)?),
                    host_instructions: vec!["Treat source data as observations.".into()],
                    system_inputs: inputs,
                    tools: Some(Arc::new(ToolRegistry::new(
                        self.scope.clone(),
                        vec![ToolRegistration {
                            compiled,
                            executor: self.tool.clone(),
                        }],
                    )?)),
                    hooks: None,
                    components: None,
                    context_sources: Some(sources),
                    context_token_estimator: Some(Arc::new(SourceEstimate)),
                    context_runtime: None,
                    verification: None,
                    skills: None,
                    artifacts: None,
                    system_input_resolver: None,
                    external_receipt_verifier: None,
                    clock,
                    ids,
                    token_estimator: Arc::new(Estimate),
                    settings: AgentSettings {
                        require_durable: true,
                        max_output_tokens: 256.try_into().unwrap(),
                        ..Default::default()
                    },
                },
            )
        }
        fn profile(&self) -> serde_json::Value {
            json!({"schema_version":"wickle.agent-profile.v1","agent_id":"gatherer","version":"1","name":"Gatherer","description":"Independent business consumer","instructions":{"text":"Aggregate authorized records using retrieved context"},"model_binding":"primary","tools":[{"tool_id":"lookup","version":"1"}],"skills":[],"connectors":[],"context_sources":[{"source":{"source_id":self.memory_id,"version":"1"},"trigger":"run_start","required":true,"timeout_ms":1000,"max_items":2,"max_bytes":4096,"max_tokens":128},{"source":{"source_id":"graph","version":"1"},"trigger":"run_start","required":true,"timeout_ms":1000,"max_items":2,"max_bytes":4096,"max_tokens":128}],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},"limits":{"max_model_calls":3,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}})
        }
    }
    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        let directory = TemporaryDatabase(
            std::env::temp_dir().join(format!("wickle-gather-{}", RandomIdSource.next_id()?)),
        );
        std::fs::create_dir(&directory.0)?;
        let a_calls = Arc::new(AtomicUsize::new(0));
        let b_calls = Arc::new(AtomicUsize::new(0));
        let graph_calls = Arc::new(AtomicUsize::new(0));
        let a: Arc<dyn ContextSource> = Arc::new(MemoryA {
            period: "quarter".into(),
            calls: a_calls.clone(),
        });
        let b: Arc<dyn ContextSource> = Arc::new(MemoryB {
            records: std::collections::BTreeMap::from([(
                "preferred.period".into(),
                "month".into(),
            )]),
            calls: b_calls.clone(),
        });
        let graph = Arc::new(Graph {
            calls: graph_calls.clone(),
        });
        for (memory, memory_id, period, total) in [
            (a, "memory-a", "quarter", 42),
            (b, "memory-b", "month", 100),
        ] {
            let scope = Scope {
                tenant_id: id("tenant"),
                workspace_id: id("workspace"),
                user_id: None,
            };
            let scenario = Scenario {
                scope: scope.clone(),
                store: Arc::new(SqliteStateStore::open(
                    directory.0.join(format!("{memory_id}.sqlite3")),
                )?),
                memory,
                memory_id,
                graph: graph.clone(),
                model: Arc::new(GatherModel {
                    period,
                    calls: AtomicUsize::new(0),
                }),
                tool: Arc::new(Lookup {
                    scope: scope.clone(),
                    calls: AtomicUsize::new(0),
                    seen: Mutex::new(vec![]),
                }),
            };
            let profile_path = directory.0.join(format!("{memory_id}.profile.json"));
            std::fs::write(&profile_path, scenario.profile().to_string())?;
            let profile = AgentProfile::from_json(&std::fs::read_to_string(&profile_path)?)?;
            ProfileValidator::new(&GatherCatalog)
                .validate(&profile, &scope)
                .await?;
            let agent = scenario.build(profile)?;
            let execution = ExecutionContext::new(
                ExecutionContextData {
                    scope: scope.clone(),
                    principal_ref: id("reader"),
                    capability_grant_ref: id("read-grant"),
                    trace_context: None,
                    system_inputs: Some(SystemInputs::new(JsonObject::from([
                        ("workspace_id".into(), json!(WORKSPACE)),
                        ("private_note".into(), json!("not a tool input")),
                    ]))),
                },
                Default::default(),
            );
            let handle = completed(agent.start(request("gather"), execution.clone()).await?)?;
            let outcome = completed(handle.outcome(&execution).await?)?;
            assert_eq!(outcome.result.status(), RunStatus::Succeeded);
            let InputContent::Text { text } = &outcome.output[0] else {
                return Err("expected aggregate output".into());
            };
            let value: Value = serde_json::from_str(text)?;
            assert_eq!(value["total"], total);
            assert_eq!(value["period"], period);
            let evidence: EvidenceRef = serde_json::from_value(value["evidence"].clone())?;
            assert_eq!(evidence.source_id, id("records"));
            assert_eq!(evidence.location, id(period));
            assert_eq!(scenario.model.calls.load(Ordering::SeqCst), 2);
            assert_eq!(scenario.tool.calls.load(Ordering::SeqCst), 1);
            assert_eq!(scenario.tool.seen.lock().unwrap()[0]["query"], period);
            let mut unknown = scenario.profile();
            unknown["tools"][0]["tool_id"] = json!("unregistered");
            let invalid_profile = AgentProfile::from_json(&unknown.to_string())?;
            assert!(
                ProfileValidator::new(&GatherCatalog)
                    .validate(&invalid_profile, &scope)
                    .await
                    .is_err()
            );
            let error = scenario
                .build(invalid_profile)?
                .start(request("unregistered"), execution.clone())
                .await
                .err()
                .ok_or("unregistered tool execution was accepted")?;
            assert_eq!(error.code, ErrorCode::ComponentUnavailable);
            let mut escalation = scenario.profile();
            escalation["capability_grant_ref"] = json!("administrator");
            assert!(AgentProfile::from_json(&escalation.to_string()).is_err());
            let mut foreign = execution.clone();
            foreign.data.scope.workspace_id = id("foreign");
            assert!(agent.start(request("foreign"), foreign).await.is_err());
            assert_eq!(scenario.model.calls.load(Ordering::SeqCst), 2);
            assert_eq!(scenario.tool.calls.load(Ordering::SeqCst), 1);
        }
        assert_eq!(a_calls.load(Ordering::SeqCst), 1);
        assert_eq!(b_calls.load(Ordering::SeqCst), 1);
        assert_eq!(graph_calls.load(Ordering::SeqCst), 2);
        println!(
            "gather consumer: distinct memory A/B implementations plus graph retrieval; scoped query/limit Tool loop with hidden UUID; evidence used in final aggregation; external profiles and rejected escalation passed"
        );
        Ok(())
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    host::run().await
}
```
