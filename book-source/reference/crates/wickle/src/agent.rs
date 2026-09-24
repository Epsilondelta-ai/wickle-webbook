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
mod control;
mod driver;
mod hooks;
mod inspection;
mod interruption;
mod persistence;
pub use persistence::{PersistenceFailure, UnconfirmedToolEffect};
mod recovery;
mod resume;
mod segment;
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
    /// Upper bound for cooperative interruption callbacks; default one second.
    pub interruption_timeout_ms: u64,
    /// Separate bounded cleanup window; default five seconds, not new work time.
    pub cleanup_timeout_ms: u64,
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
            interruption_timeout_ms: 1000,
            cleanup_timeout_ms: 5000,
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
        if self.interruption_timeout_ms == 0
            || self.cleanup_timeout_ms == 0
            || self.interruption_timeout_ms > 86_400_000
            || self.cleanup_timeout_ms > 86_400_000
            || self.lease_ttl_ms == 0
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
    /// Optional versioned stop policy and business-state schema; omission uses the core default.
    pub interruption_policy: Option<InterruptionPolicyBinding>,
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
    stop_cause: Mutex<Option<InterruptionCause>>,
    cleanup_deadline: Mutex<Option<tokio::time::Instant>>,
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
            stop_cause: Mutex::new(None),
            cleanup_deadline: Mutex::new(None),
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
    segment_id: Id,
    agent: Agent,
    run_id: Id,
    segment_start_revision: u64,
    local: Option<Arc<LocalRun>>,
}
impl fmt::Debug for RunHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunHandle")
            .field("run_id", &self.run_id)
            .field("segment_id", &self.segment_id)
            .finish_non_exhaustive()
    }
}

/// Result of an authorized cancellation request, separate from stored RunOutcome.
pub type CancelReceipt = ControlReceipt;

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
            .run_view(
                &saved.snapshot,
                self.inner.bindings.clock.as_ref(),
                context,
                None,
            )
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
        // Build the owned coordinator outside the caller's poll stack before
        // handing it to Tokio; recovery can nest several large state futures.
        let result = runtime
            .spawn(crate::future::boxed(|| async move {
                agent.resume_command(command, context).await
            }))
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
    async fn handle(
        &self,
        run_id: Id,
        segment_start_revision: u64,
    ) -> Result<RunHandle, ContractError> {
        let history = self
            .inner
            .bindings
            .state
            .read_execution(&self.inner.bindings.scope, &run_id)
            .await?;
        let segment = history
            .segments
            .iter()
            .find(|segment| segment.accepted_revision == segment_start_revision)
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.segment"))?;
        self.segment_handle(run_id, segment_start_revision, segment.segment_id.clone())
    }
    async fn latest_handle(&self, run_id: Id) -> Result<RunHandle, ContractError> {
        let bindings = &self.inner.bindings;
        let history = match bindings
            .state
            .read_execution(&bindings.scope, &run_id)
            .await
        {
            Ok(history) => history,
            Err(error) if error.code == ErrorCode::CapabilityUnsupported => {
                let saved = bindings.state.load(&bindings.scope, &run_id).await?;
                if !saved.snapshot.status.is_terminal() {
                    return Err(error);
                }
                // A deterministic read-only view ID does not invent execution ownership.
                let segment_id = Id::new(format!(
                    "legacy-view:{}",
                    crate::serialization::data_digest(&(&run_id, saved.snapshot.revision))
                ))?;
                return self.segment_handle(run_id, segment_revision(&saved.snapshot), segment_id);
            }
            Err(error) => return Err(error),
        };
        let segment = history
            .segments
            .last()
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.segment"))?;
        self.segment_handle(
            run_id,
            segment.accepted_revision,
            segment.segment_id.clone(),
        )
    }
    fn segment_handle(
        &self,
        run_id: Id,
        segment_start_revision: u64,
        segment_id: Id,
    ) -> Result<RunHandle, ContractError> {
        let local = self
            .inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .get(&run_id)
            .filter(|local| local.segment_start_revision == segment_start_revision)
            .cloned();
        Ok(RunHandle {
            segment_id,
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
    /// Immutable execution interval identity. Resuming creates a different handle.
    pub fn segment_id(&self) -> &Id {
        &self.segment_id
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
            let history = caller_read(
                context,
                None,
                self.agent
                    .inner
                    .bindings
                    .state
                    .read_execution(&snapshot.scope, &self.run_id),
            )
            .await;
            let outcome = match history {
                Ok(history) => {
                    let segment = history
                        .segments
                        .iter()
                        .find(|segment| {
                            segment.segment_id == self.segment_id
                                && segment.accepted_revision == self.segment_start_revision
                        })
                        .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.segment"))?;
                    match &segment.outcome {
                        Some(SegmentOutcome::Settled { outcome }) => Some(outcome.as_ref().clone()),
                        Some(SegmentOutcome::Interrupted { interruption }) => {
                            let reference =
                                segment.source_snapshot_ref.as_ref().ok_or_else(|| {
                                    fail(
                                        ErrorCode::ComponentUnavailable,
                                        "agent.legacy_segment_source",
                                    )
                                })?;
                            let record = caller_read(
                                context,
                                None,
                                self.agent
                                    .inner
                                    .bindings
                                    .state
                                    .read_record(&snapshot.scope, reference),
                            )
                            .await?;
                            if record.reference() != reference {
                                return Err(fail(
                                    ErrorCode::InvalidSnapshot,
                                    "agent.segment_source",
                                ));
                            }
                            let source: RunSnapshot =
                                serde_json::from_value(record.value().clone()).map_err(|_| {
                                    fail(ErrorCode::InvalidSnapshot, "agent.segment_source")
                                })?;
                            Some(RunOutcome {
                                result: OutcomeResult::Interrupted {
                                    interruption: interruption.clone(),
                                },
                                output: vec![],
                                artifacts: vec![],
                                usage: source.usage,
                                checkpoint_revision: source.revision,
                                verification: None,
                                unresolved_effects: interruption.unresolved_effects.clone(),
                                app_state: source.app_state,
                            })
                        }
                        None => None,
                    }
                }
                Err(error)
                    if error.code == ErrorCode::CapabilityUnsupported
                        && snapshot.status.is_terminal() =>
                {
                    snapshot.outcome.clone()
                }
                Err(error) => return Err(error),
            };
            if let Some(outcome) = outcome {
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
                            .segment_end(&saved.snapshot, &context)
                            .await?
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
                    if let Some(end) = handle.segment_end(&saved.snapshot, &context).await? {
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
    /// Cooperatively stop this execution interval without cancelling the Run.
    /// NotLocal does not submit a remote command; the Host must deliver it to its Worker.
    pub async fn stop_execution(
        &self,
        cause: InterruptionCause,
        context: &ExecutionContext,
    ) -> Result<Guarded<ExecutionStopReceipt>, ContractError> {
        if !matches!(
            cause,
            InterruptionCause::HostShutdown | InterruptionCause::SegmentStopped
        ) {
            return Err(fail(ErrorCode::InvalidContract, "interruption.stop_cause"));
        }
        self.agent.check_scope(context)?;
        let bindings = &self.agent.inner.bindings;
        let request = PolicyRequest {
            owner_scope: bindings.scope.clone(),
            resource_id: self.run_id.clone(),
            action: PolicyAction::StopExecution { cause },
        };
        bindings
            .policy
            .guard(&request, context, None, None, || async {
                let saved = caller_read(
                    context,
                    None,
                    bindings.state.load(&bindings.scope, &self.run_id),
                )
                .await?;
                if saved.snapshot.status != RunStatus::Running
                    || segment_revision(&saved.snapshot) != self.segment_start_revision
                {
                    return Ok(ExecutionStopReceipt::AlreadySettled);
                }
                if saved.snapshot.interruption_plan_ref.is_none() {
                    return Err(fail(
                        ErrorCode::ComponentUnavailable,
                        "interruption.legacy_plan",
                    ));
                }
                let Some(local) = self.current_local()? else {
                    return Ok(ExecutionStopReceipt::NotLocal);
                };
                if local.done.load(Ordering::Acquire) {
                    return Ok(ExecutionStopReceipt::NotLocal);
                }
                let mut stop = local
                    .stop_cause
                    .lock()
                    .map_err(|_| fail(ErrorCode::InvalidContract, "interruption.stop_state"))?;
                if stop.is_none() {
                    *stop = Some(cause);
                }
                local.cancel.cancel();
                Ok(ExecutionStopReceipt::Requested)
            })
            .await
    }
    /// Submit a durable cancellation under current authority. Use
    /// Agent::submit_control_command when the caller supplies an idempotency key.
    pub async fn cancel(
        &self,
        reason: Id,
        context: &ExecutionContext,
    ) -> Result<Guarded<CancelReceipt>, ContractError> {
        self.agent
            .submit_control_command(
                self.run_id.clone(),
                ControlCommand {
                    command_id: self.agent.inner.bindings.ids.next_id()?,
                    principal_ref: context.data.principal_ref.clone(),
                    action: ControlAction::Cancel { reason },
                },
                context.clone(),
            )
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
    async fn segment_end(
        &self,
        snapshot: &RunSnapshot,
        context: &ExecutionContext,
    ) -> Result<Option<u64>, ContractError> {
        let history = caller_read(
            context,
            None,
            self.agent
                .inner
                .bindings
                .state
                .read_execution(&snapshot.scope, &self.run_id),
        )
        .await;
        match history {
            Ok(history) => {
                let segment = history
                    .segments
                    .iter()
                    .find(|segment| segment.accepted_revision == self.segment_start_revision)
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.segment"))?;
                if let Some(last) = segment.last_event_seq {
                    return Ok(Some(last));
                }
            }
            Err(error) if error.code == ErrorCode::CapabilityUnsupported => {}
            Err(error) => return Err(error),
        }
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
        Ok((snapshot.status.is_terminal()
            || matches!(snapshot.status, RunStatus::Waiting | RunStatus::Interrupted))
        .then_some(snapshot.last_event_seq))
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
