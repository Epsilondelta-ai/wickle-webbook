# 49장 전체 구현과 변경 검사

[강의](../49-inspection.md) · [전체 변경 패치](../solutions/49-inspection.patch)

기준 `c2af068d489db44aa5fddaf628b1f99c1f2ed4e0`. 이 단계에서 바뀐 Rust·manifest·Python 파일의 전체 내용이다. 이전 버전과의 정확한 교체 위치·삭제는 patch를 따른다. 다음 장의 코드와 섞지 않는다.

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
```

## `crates/wickle/src/agent/inspection.rs`

```rust
use super::*;
use crate::inspection::{StoredInspection, compose};
use serde::de::DeserializeOwned;

impl Agent {
    /// Inspect saved composition under current permission. This performs only
    /// policy checks and storage reads; it never prepares or executes a step.
    pub async fn inspect_step(
        &self,
        run_id: &Id,
        step: StepRef,
        context: &ExecutionContext,
        options: InspectionOptions,
    ) -> Result<Guarded<CompositionReport>, ContractError> {
        self.check_scope(context)?;
        let timeout = Duration::from_millis(self.inner.bindings.settings.start_timeout_ms);
        let deadline = tokio::time::Instant::now() + timeout;
        let mut request = PolicyRequest {
            owner_scope: self.inner.bindings.scope.clone(),
            resource_id: run_id.clone(),
            action: PolicyAction::InspectStep {
                step: step.clone(),
                options,
                context_fragments: vec![],
            },
        };
        if let Guarded::ApprovalRequired(challenge) = self
            .inner
            .bindings
            .policy
            .guard(&request, context, Some(deadline), None, || async { Ok(()) })
            .await?
        {
            return Ok(Guarded::ApprovalRequired(challenge));
        }
        let (report, context_fragments) = caller_read(
            context,
            Some(timeout),
            crate::future::boxed(|| self.inspect_records(run_id, step, context, options, deadline)),
        )
        .await?;
        if let PolicyAction::InspectStep {
            context_fragments: included,
            ..
        } = &mut request.action
        {
            *included = context_fragments;
        }
        self.inner
            .bindings
            .policy
            .guard(&request, context, Some(deadline), None, || async {
                Ok(report)
            })
            .await
    }

    async fn inspect_records(
        &self,
        run_id: &Id,
        step: StepRef,
        context: &ExecutionContext,
        options: InspectionOptions,
        deadline: tokio::time::Instant,
    ) -> Result<(CompositionReport, Vec<InspectionFragmentRef>), ContractError> {
        let bindings = &self.inner.bindings;
        let saved = match bindings.state.load(&bindings.scope, run_id).await {
            Ok(saved) => saved,
            Err(error) if missing_reason(error.code).is_some() => {
                return Ok((unavailable(run_id, step, error.code, "run", None), vec![]));
            }
            Err(error) => return Err(error),
        };
        if saved.snapshot.scope != bindings.scope || saved.snapshot.run_id != *run_id {
            return Err(fail(ErrorCode::InvalidSnapshot, "inspection.run"));
        }
        let mut lookup_gaps = vec![];
        let mut selected = None;
        for reference in &saved.snapshot.prepared_steps {
            if matches!(&step, StepRef::Prepared { record_id } if *record_id != reference.record_id)
            {
                continue;
            }
            let root: Option<PreparedStepRecord> = self
                .inspection_record(reference, "preparation", &mut lookup_gaps)
                .await?;
            let Some(root) = root else { continue };
            if root.scope != bindings.scope
                || root.run_id != *run_id
                || root.profile_digest != *saved.snapshot.profile.profile_digest()
            {
                return Err(fail(ErrorCode::InvalidSnapshot, "inspection.preparation"));
            }
            let matched = match &step {
                StepRef::Prepared { record_id } => *record_id == reference.record_id,
                StepRef::Logical {
                    model_step_id,
                    purpose,
                    projection_revision,
                } => {
                    root.model_step_id == *model_step_id
                        && root.purpose == *purpose
                        && root.projection_revision == *projection_revision
                }
            };
            if matched {
                selected = Some((reference.clone(), root));
                break;
            }
        }
        let Some((reference, root)) = selected else {
            let status = if lookup_gaps.is_empty() {
                InspectionStatus::NotFound
            } else if matches!(step, StepRef::Prepared { .. })
                && lookup_gaps.iter().any(|gap| gap.reason == "expired")
            {
                InspectionStatus::Expired
            } else if matches!(step, StepRef::Prepared { .. }) {
                InspectionStatus::NotFound
            } else {
                InspectionStatus::Partial
            };
            return Ok((
                CompositionReport {
                    schema_version: "wickle.composition-report.v1",
                    run_id: run_id.clone(),
                    step,
                    status,
                    composition: None,
                    unresolved: lookup_gaps,
                },
                vec![],
            ));
        };
        let mut unresolved = vec![UnresolvedInspectionField {
            field: "estimator".into(),
            record_id: None,
            reason: "not_recorded".into(),
        }];
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct ConfigurationRecord {
            route: ResolvedModelRoute,
            configuration: ModelConfiguration,
        }
        let config: Option<ConfigurationRecord> = self
            .inspection_record(
                &root.model_configuration,
                "model_configuration",
                &mut unresolved,
            )
            .await?;
        let projection: Option<PreparedModelProjection> = self
            .inspection_record(&root.context_projection, "projection", &mut unresolved)
            .await?;
        if projection.as_ref().is_some_and(|projection| {
            projection.scope != bindings.scope
                || projection.run_id != *run_id
                || projection.request.request_id != root.model_step_id
                || projection.request.purpose != root.purpose
                || projection.fingerprint() != root.projection_fingerprint
        }) {
            return Err(fail(ErrorCode::InvalidSnapshot, "inspection.projection"));
        }
        if let (Some(config), Some(projection)) = (&config, &projection) {
            if config.route != projection.request.route
                || config.configuration.effective != projection.request.options
                || config.configuration.max_output_tokens != projection.request.max_output_tokens
            {
                return Err(fail(ErrorCode::InvalidSnapshot, "inspection.configuration"));
            }
        }
        let route = config
            .as_ref()
            .map(|record| record.route.clone())
            .or_else(|| {
                projection
                    .as_ref()
                    .map(|projection| projection.request.route.clone())
            });
        let tool_set: Option<ResolvedToolSet> = self
            .inspection_record(&root.tool_set, "tool_set", &mut unresolved)
            .await?;
        let mut tools = vec![];
        if let Some(tool_set) = tool_set {
            if tool_set.scope != bindings.scope
                || tool_set.run_id != *run_id
                || tool_set.entries.len() != root.compiled_tools.len()
            {
                return Err(fail(ErrorCode::InvalidSnapshot, "inspection.tool_set"));
            }
            for (entry, reference) in tool_set.entries.into_iter().zip(&root.compiled_tools) {
                let value = self
                    .inspection_record::<serde_json::Value>(
                        reference,
                        "tool_compilation",
                        &mut unresolved,
                    )
                    .await?;
                let compiled = value.map(CompiledToolContract::inspection).transpose()?;
                if let Some(compiled) = &compiled {
                    if compiled.tool != entry.manifest.tool
                        || compiled.canonical_name != entry.manifest.model_tool.name
                        || compiled.canonical_schema_digest != entry.manifest.model_schema_digest
                        || route.as_ref().is_some_and(|route| {
                            compiled.target.provider != route.provider
                                || compiled.target.api_contract != route.api_contract
                                || compiled.target.capability_revision != route.capability_revision
                        })
                    {
                        return Err(fail(
                            ErrorCode::InvalidSnapshot,
                            "inspection.tool_compilation",
                        ));
                    }
                }
                tools.push((entry.manifest, compiled));
            }
        }
        let mut attempts = vec![];
        for invocation in &saved.snapshot.model_ledger {
            if invocation.prepared_step_ref.as_ref() != Some(&reference) {
                continue;
            }
            if invocation.model_step_id != root.model_step_id
                || invocation.purpose != root.purpose
                || invocation.run_id != *run_id
            {
                return Err(fail(ErrorCode::InvalidSnapshot, "inspection.invocation"));
            }
            let mut observed = matches!(invocation.state, ModelAttemptState::Completed {})
                || invocation.provider_request_id.is_some()
                || invocation.reported_model_id.is_some()
                || invocation.reported_model_version.is_some()
                || invocation
                    .usage
                    .as_ref()
                    .is_some_and(|usage| usage.measurement == UsageMeasurement::Reported);
            if let Some(response_ref) = &invocation.response_ref {
                let response: Option<StoredModelResponse> = self
                    .inspection_record(response_ref, "response", &mut unresolved)
                    .await?;
                if let Some(response) = response {
                    if response.request_id != invocation.attempt_id
                        || response.route_digest != invocation.route.digest()
                    {
                        return Err(fail(ErrorCode::InvalidSnapshot, "inspection.response"));
                    }
                    observed |= match response.outcome {
                        ModelExchangeOutcome::Completed { .. } => true,
                        ModelExchangeOutcome::Failed { failure } => {
                            !failure.partial_text().is_empty()
                                || failure.metadata.provider_request_id.is_some()
                                || failure.metadata.reported_model_id.is_some()
                                || failure.metadata.reported_model_version.is_some()
                                || failure.metadata.usage.as_ref().is_some_and(|usage| {
                                    usage.measurement == UsageMeasurement::Reported
                                })
                        }
                    };
                }
            }
            attempts.push((invocation.clone(), observed));
        }
        let mut disclosure = BTreeMap::new();
        let mut context_fragments = vec![];
        let mut displayed_bytes = 0usize;
        if let Some(projection) = &projection {
            for fragment in &projection.provenance.fragments {
                if fragment.identity.scope != bindings.scope {
                    return Err(fail(
                        ErrorCode::InvalidSnapshot,
                        "inspection.fragment_scope",
                    ));
                }
                let display = match &fragment.value {
                    FragmentValue::Item { item } if options.include_context_content => {
                        let request = PolicyRequest {
                            owner_scope: bindings.scope.clone(),
                            resource_id: run_id.clone(),
                            action: PolicyAction::InspectContextFragment {
                                identity: Box::new(fragment.identity.clone()),
                                core_revision: fragment.core_revision,
                                content_digest: fragment.content_digest.clone(),
                            },
                        };
                        match bindings
                            .policy
                            .check(&request, context, Some(deadline), None)
                            .await?
                        {
                            PolicyDecision::Allow {} => {
                                let bytes = serde_json::to_vec(&item.content)
                                    .map_err(|_| {
                                        fail(ErrorCode::InvalidSnapshot, "inspection.fragment")
                                    })?
                                    .len();
                                if bytes > 65_536usize.saturating_sub(displayed_bytes) {
                                    ContextDisclosure::Redacted {
                                        reason: "display_limit".into(),
                                    }
                                } else {
                                    displayed_bytes += bytes;
                                    ContextDisclosure::Included {
                                        content: item.content.clone(),
                                    }
                                }
                            }
                            PolicyDecision::Deny { .. } => ContextDisclosure::Redacted {
                                reason: "access_denied".into(),
                            },
                            PolicyDecision::RequireApproval { .. } => ContextDisclosure::Redacted {
                                reason: "approval_required".into(),
                            },
                        }
                    }
                    FragmentValue::Item { .. } => ContextDisclosure::Redacted {
                        reason: "not_requested".into(),
                    },
                    _ => ContextDisclosure::NotApplicable,
                };
                if matches!(display, ContextDisclosure::Included { .. }) {
                    context_fragments.push(InspectionFragmentRef {
                        identity: fragment.identity.clone(),
                        core_revision: fragment.core_revision,
                        content_digest: fragment.content_digest.clone(),
                    });
                }
                disclosure.insert(crate::inspection::disclosure_key(fragment), display);
            }
        }
        Ok((
            compose(
                step,
                StoredInspection {
                    reference,
                    root,
                    route,
                    configuration: config.map(|record| record.configuration),
                    tools,
                    projection,
                    attempts,
                    disclosure,
                    limits: saved.snapshot.limits,
                    status: saved.snapshot.status,
                    revision: saved.snapshot.revision,
                    outcome: saved.snapshot.outcome,
                    unresolved,
                },
            ),
            context_fragments,
        ))
    }

    async fn inspection_record<T: DeserializeOwned>(
        &self,
        reference: &RecordRef,
        field: &str,
        unresolved: &mut Vec<UnresolvedInspectionField>,
    ) -> Result<Option<T>, ContractError> {
        match self
            .inner
            .bindings
            .state
            .read_record(&self.inner.bindings.scope, reference)
            .await
        {
            Ok(record) => {
                if record.reference() != reference {
                    return Err(fail(
                        ErrorCode::InvalidSnapshot,
                        "inspection.record_identity",
                    ));
                }
                serde_json::from_value(record.value().clone())
                    .map(Some)
                    .map_err(|_| fail(ErrorCode::InvalidSnapshot, "inspection.record"))
            }
            Err(error) if missing_reason(error.code).is_some() => {
                unresolved.push(UnresolvedInspectionField {
                    field: field.into(),
                    record_id: Some(reference.record_id.clone()),
                    reason: missing_reason(error.code)
                        .expect("matched missing code")
                        .into(),
                });
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
}
fn missing_reason(code: ErrorCode) -> Option<&'static str> {
    match code {
        ErrorCode::StateNotFound => Some("not_found"),
        ErrorCode::RecordExpired => Some("expired"),
        _ => None,
    }
}
fn unavailable(
    run_id: &Id,
    step: StepRef,
    code: ErrorCode,
    field: &str,
    record_id: Option<Id>,
) -> CompositionReport {
    CompositionReport {
        schema_version: "wickle.composition-report.v1",
        run_id: run_id.clone(),
        step,
        status: if code == ErrorCode::RecordExpired {
            InspectionStatus::Expired
        } else {
            InspectionStatus::NotFound
        },
        composition: None,
        unresolved: vec![UnresolvedInspectionField {
            field: field.into(),
            record_id,
            reason: missing_reason(code).unwrap_or("not_found").into(),
        }],
    }
}
```

## `crates/wickle/src/error.rs`

```rust
use serde::{Deserialize, Serialize};

/// Stable categories for contract and profile validation failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorCode {
    /// The verifier could not complete its check; this is not a quality rejection.
    VerificationUnavailable,
    /// Current policy or exact owner scope denies access.
    AccessDenied,
    /// Artifact identity, content, metadata, or size violates its immutable contract.
    InvalidArtifact,
    /// Artifact policy requires an explicit Host approval before storage access.
    ArtifactApprovalRequired,
    /// A Skill manifest, body, dependency or loader result violates its pinned contract.
    InvalidSkill,
    /// Context selection would split a complete group or remove protected data.
    InvalidContextSelection,
    /// A compressor did not produce a smaller usable context projection.
    ContextCompactionNoReduction,
    /// A model compressor failed or returned an unsupported completion.
    ContextCompactionFailed,
    /// Current Skill policy requires explicit Host approval before loading or use.
    SkillApprovalRequired,
    /// The trusted policy failed or panicked; no permission was granted.
    PolicyUnavailable,
    /// The call's finite deadline elapsed.
    DeadlineExceeded,
    /// The current operation was cancelled.
    Cancelled,
    /// The Host has not supplied the required asynchronous runtime.
    RuntimeUnavailable,
    /// A configured call, repair, or recovery budget has no remaining capacity.
    BudgetExceeded,
    /// A required time reading or timer could not be obtained.
    ClockUnavailable,
    /// A monotonic reading regressed or a resumed UTC clock predates saved progress.
    ClockRegression,
    /// The Host identifier source could not generate an internal execution identifier.
    IdGenerationFailed,
    /// Input is not unambiguous, finite JSON.
    InvalidJson,
    /// Input does not match a data contract.
    InvalidContract,
    /// The requested model, version, binding or alias is not registered.
    ModelNotRegistered,
    /// An exact model/API/adapter/target binding or its evidence is inconsistent.
    ModelBindingInvalid,
    /// The required feature or declared capability limit is unsupported.
    ModelCapabilityUnsupported,
    /// Provider options do not satisfy the model and exact-binding contracts.
    ModelOptionUnsupported,
    /// The model or deployment is not verified immutable under the requested policy.
    ModelVersionUnpinned,
    /// Current observed model or deployment metadata differs from the pinned route.
    ModelVersionDrift,
    /// A bounded target inspection failed or could not establish availability.
    ModelInspectionUnavailable,
    /// A prior physical attempt has no known result and needs explicit recovery.
    ModelAttemptUnresolved,
    /// The selected release is retired or otherwise unavailable.
    ModelUnavailable,
    /// Catalog scope-independent revision, identity or serialized integrity differs.
    ModelCatalogMismatch,
    /// Static routing configuration has duplicate, missing or unsupported constraints.
    ModelRoutingInvalid,
    /// Saved routing metadata, request identity or selected route differs.
    ModelRoutingMismatch,
    /// Static routing constraints forbid this target or fallback reason.
    ModelRouteDenied,
    /// No eligible candidate remains in the finite permitted fallback list.
    ModelRoutesExhausted,
    /// Required support evidence is absent, failed, or of an insufficient kind.
    ModelSupportInsufficient,
    /// Estimated input plus reserved output exceeds this model/binding context budget.
    ModelContextIncompatible,
    /// Tool exposure, binding metadata, or a registered input schema is inconsistent.
    InvalidToolInputContract,
    /// The compiler cannot safely project this input schema or reference form.
    UnsupportedInputProjection,
    /// Model-owned or assembled tool arguments do not satisfy their input contract.
    InvalidArguments,
    /// An external receipt did not establish the tool effect; its saved wait remains.
    ToolEffectUnresolved,
    /// A supplied system value does not satisfy its registered input contract.
    SystemInputInvalid,
    /// A required registered system value is absent; the model must not invent it.
    SystemInputMissing,
    /// A read-only system-value resolver is unavailable or failed safely.
    SystemInputUnavailable,
    /// Supplied/resumed values or pinned input metadata differ from the saved snapshot.
    SystemInputsMismatch,
    /// Lookup permission requires separate Host approval before a target is known.
    SystemInputApprovalRequired,
    /// Resolver-count or serialized input-size bounds were exceeded.
    InputBindingLimitExceeded,
    /// Context identity, provenance structure, or call/result protocol is invalid.
    InvalidContext,
    /// Context scope, pinned assets, or protected-record identity does not match.
    ContextMismatch,
    /// Required context cannot fit the explicit byte or item bounds without truncation.
    ContextBudgetExceeded,
    /// A required context source is explicitly unavailable.
    ContextSourceUnavailable,
    /// Context access requires approval through a separate interactive operation.
    ContextApprovalRequired,
    /// The document format is not supported.
    UnsupportedSchemaVersion,
    /// A reference or binding is missing or inconsistent.
    InvalidReference,
    /// A required component or exact version is unavailable.
    ComponentUnavailable,
    /// A component uses an unsupported metadata contract.
    UnsupportedContractVersion,
    /// Selected components do not supply a required capability.
    CapabilityUnsupported,
    /// A configuration does not satisfy its registered schema.
    InvalidConfiguration,
    /// A registered schema is invalid or requires unsupported resolution.
    InvalidSchema,
    /// A profile differs from the profile pinned to an existing execution.
    ProfileMismatch,
    /// Stored data violates checkpoint invariants.
    InvalidSnapshot,
    /// The requested run, session, or protected record is absent in this exact scope.
    StateNotFound,
    /// A protected record was deliberately expired by the storage retention policy.
    RecordExpired,
    /// An existing request identity was reused with different logical input.
    RequestConflict,
    /// The session already has a running or waiting run.
    SessionBusy,
    /// A proposed run identifier already belongs to another request in this scope.
    RunConflict,
    /// The compare-and-swap revision no longer matches saved state.
    RevisionConflict,
    /// Another unexpired execution lease already owns the run.
    LeaseBusy,
    /// The execution lease expired or no longer matches its owner and generation.
    LeaseLost,
    /// A candidate change violates immutable data or state-transition rules.
    InvalidTransition,
    /// An event has a duplicate identity, invalid sequence, or inconsistent references.
    InvalidEvent,
    /// A message has a duplicate identity, invalid sequence, or wrong owning run.
    InvalidMessage,
    /// Immutable record content or a requested reference digest conflicts.
    RecordConflict,
    /// Authoritative storage is unavailable; no successful commit is implied.
    PersistenceUnavailable,
}

/// A validation error that does not retain submitted values or credentials.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code:?} at {path}")]
pub struct ContractError {
    /// Machine-readable failure category.
    pub code: ErrorCode,
    /// Contract field or reference location, without submitted values.
    pub path: String,
    /// Authorized run diagnostics when durable storage is unavailable.
    pub persistence: Option<Box<crate::PersistenceFailure>>,
}

impl ContractError {
    /// Construct an error using a safe contract location.
    pub fn new(code: ErrorCode, path: impl Into<String>) -> Self {
        Self {
            code,
            path: path.into(),
            persistence: None,
        }
    }
}
```

## `crates/wickle/src/inspection.rs`

```rust
//! Read-only composition reports derived from stored evidence.
use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, num::NonZeroU64};

/// Identify persisted preparation without supplying runtime objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum StepRef {
    /// Exact logical input revision and purpose.
    Logical {
        /// Logical model step.
        model_step_id: Id,
        /// Agent, verification, or compaction.
        purpose: ModelPurpose,
        /// Frozen projection revision.
        projection_revision: NonZeroU64,
    },
    /// A preparation record belonging to this Run.
    Prepared {
        /// Record ID, resolved against the Run's saved reference list.
        record_id: Id,
    },
}
/// Display options, never execution or credential-disclosure permission.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectionOptions {
    /// Request source fragment content; each item still needs current permission.
    #[serde(default)]
    pub include_context_content: bool,
}
/// Exact saved content set covered by the final diagnostic authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectionFragmentRef {
    /// Scoped producer and native fragment identity.
    pub identity: FragmentIdentity,
    /// Exact core observation.
    pub core_revision: NonZeroU64,
    /// Original content identity.
    pub content_digest: JsonDigest,
}
/// Whether persisted evidence was available, without recreating it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectionStatus {
    /// Selected preparation and its required records were available.
    Found,
    /// Some referenced evidence is absent or explicitly expired.
    Partial,
    /// No matching preparation or record exists.
    NotFound,
    /// The store explicitly classified the selected record as expired.
    Expired,
}
/// Why one report field cannot be populated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnresolvedInspectionField {
    /// Safe field/category name, not a storage error payload.
    pub field: String,
    /// Missing record identity when known.
    pub record_id: Option<Id>,
    /// `not_found`, `expired`, or `not_recorded`.
    pub reason: String,
}
/// Facts a stored record supports; none asserts business success.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectionEvidence {
    /// A model input was prepared and saved.
    Prepared,
    /// A physical call budget was reserved, not proof of transmission.
    DispatchReserved,
    /// A response record was observed and saved.
    ResponseObserved,
    /// No saved response proves whether transmission occurred.
    TransmissionUnknown,
}
/// Public report, with protected runtime inputs excluded by construction.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CompositionReport {
    /// Stable report encoding.
    pub schema_version: &'static str,
    /// Authorized Run identity.
    pub run_id: Id,
    /// Original selector.
    pub step: StepRef,
    /// Lookup/retention result.
    pub status: InspectionStatus,
    /// Available saved preparation, never a newly generated projection.
    pub composition: Option<StepComposition>,
    /// Explicit gaps; unknown values are not guessed.
    pub unresolved: Vec<UnresolvedInspectionField>,
}
/// Composition of one exact prepared input.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StepComposition {
    /// Selected preparation record identity.
    pub prepared_record_id: Id,
    /// Logical model step.
    pub model_step_id: Id,
    /// Invocation purpose.
    pub purpose: ModelPurpose,
    /// Exact frozen revision.
    pub projection_revision: NonZeroU64,
    /// Original unredacted execution fingerprint; never recomputed from this DTO.
    pub fingerprint: JsonDigest,
    /// Pinned assembler identity.
    pub assembler: VersionedRef,
    /// Saved reason for this projection revision.
    pub change_reason: Id,
    /// Preparation evidence, independent of physical transmission.
    pub evidence: Vec<InspectionEvidence>,
    /// Safe model and option metadata.
    pub model: Option<ModelComposition>,
    /// Model-owned schemas; hidden execution contracts are excluded.
    pub tools: Vec<ToolComposition>,
    /// Fragments in their original saved order.
    pub fragments: Vec<FragmentComposition>,
    /// Context selection evidence, distinct from display redaction.
    pub selection: Option<SelectionComposition>,
    /// Physical reservations and observed response facts.
    pub attempts: Vec<AttemptComposition>,
    /// Saved input estimate, not actual provider usage.
    pub estimated_input_tokens: Option<u64>,
    /// Estimator identity, if it was recorded; older preparations have none.
    pub estimator: Option<VersionedRef>,
    /// Immutable Run limits.
    pub run_limits: RunLimits,
    /// Current saved Run status; inspection does not decide an outcome.
    pub recorded_run_status: RunStatus,
    /// Snapshot revision at which this Run status was read.
    pub recorded_run_revision: u64,
    /// Saved Run outcome metadata, without business output or inferred success.
    pub recorded_run_outcome: Option<OutcomeComposition>,
    /// Paths hidden only for display, not removed from the saved projection.
    pub redacted_paths: Vec<String>,
}
/// Authoritative Run outcome metadata associated with the inspected snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OutcomeComposition {
    /// Saved status, not an inspector verdict.
    pub status: RunStatus,
    /// Exact settlement revision.
    pub checkpoint_revision: u64,
    /// Present only when the saved Run succeeded; turn-ended is not verified business success.
    pub completion_basis: Option<CompletionBasis>,
}
/// Model metadata with connection references and target configuration omitted.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ModelComposition {
    /// Selected provider.
    pub provider: Id,
    /// Requested model/alias.
    pub requested_model: Id,
    /// Resolved model name.
    pub model_id: Id,
    /// Opaque model release.
    pub model_version: Id,
    /// Whether the target is pinned or mutable.
    pub version_semantics: VersionSemantics,
    /// Adapter implementation identity.
    pub adapter: VersionedRef,
    /// Operation and API version.
    pub api_contract: ApiContract,
    /// Capability metadata revision.
    pub capability_revision: Id,
    /// Original route identity, without exposing its protected fields.
    pub route_digest: JsonDigest,
    /// Requested/effective inference options and origins, with sensitive fields redacted.
    pub configuration: Option<ModelConfiguration>,
}
/// Safe schema and compiler evidence for an advertised Tool.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ToolComposition {
    /// Exact catalog Tool version.
    pub tool: VersionedRef,
    /// Model-facing canonical name.
    pub canonical_name: Id,
    /// Canonical model-only schema, with display redaction.
    pub canonical_schema: Value,
    /// Original canonical schema identity.
    pub canonical_schema_digest: JsonDigest,
    /// Saved provider name, when its compilation record is available.
    pub provider_name: Option<Id>,
    /// Provider model-only schema, with display redaction.
    pub provider_schema: Option<Value>,
    /// Pinned compiler identity.
    pub compiler: Option<VersionedRef>,
    /// Saved compilation identity.
    pub compiled_digest: Option<JsonDigest>,
    /// Identity of the saved argument decoding plan; not execution arguments.
    pub decode_plan_digest: Option<JsonDigest>,
    /// Constraint explanation metadata; text can contain schema annotations and is omitted.
    pub constraints: Vec<ConstraintComposition>,
    /// Saved native/context/core enforcement classifications.
    pub enforcement: Vec<ToolConstraintEnforcement>,
}
/// Source and identity of trusted compiler-generated constraint text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConstraintComposition {
    /// Fragment identity.
    pub fragment_id: Id,
    /// Original explanation digest.
    pub digest: JsonDigest,
    /// Compiler that generated the text.
    pub source: VersionedRef,
}
/// Display disclosure, independent of inclusion in model input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContextDisclosure {
    /// Explicitly authorized content, bounded to a finite report size.
    Included {
        /// Source context only; never model messages or system execution inputs.
        content: Vec<InputContent>,
    },
    /// Content withheld from the display.
    Redacted {
        /// `not_requested`, `access_denied`, `approval_required`, or `display_limit`.
        reason: String,
    },
    /// Selection/tombstone metadata has no source text to disclose.
    NotApplicable,
}
/// Stable observation metadata; source revision can legitimately be unknown.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FragmentComposition {
    /// Producer identity; scope and Host execution values are not copied here.
    pub producer_id: Id,
    /// Native fragment identity.
    pub fragment_id: Id,
    /// Core-issued revision.
    pub core_revision: NonZeroU64,
    /// Original observation digest.
    pub content_digest: JsonDigest,
    /// Opaque external revision, if known.
    pub source_revision: Option<Id>,
    /// Origin classification.
    pub origin: ContextOrigin,
    /// Zero-based order in the saved preparation.
    pub order: usize,
    /// `included`, `excluded`, `tombstone`, `selection`, or `unresolved`.
    pub selection: String,
    /// Saved reason, or `not_recorded` when the preparation lacks the detail.
    pub selection_reason: String,
    /// Separately authorized display data.
    pub disclosure: ContextDisclosure,
}
/// Selection lists refer to saved inputs, not redaction decisions.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SelectionComposition {
    /// Transcript boundary used for preparation.
    pub through_sequence: u64,
    /// Selected original messages, without contents or opaque continuations.
    pub included_messages: Vec<Id>,
    /// Dropped messages; historical records do not record individual reasons.
    pub excluded_messages: Vec<Id>,
    /// Selected context item identities.
    pub included_context: Vec<Id>,
    /// Dropped context identities.
    pub excluded_context: Vec<Id>,
}
/// Reservation/response evidence for a physical model attempt.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AttemptComposition {
    /// Physical attempt identity.
    pub attempt_id: Id,
    /// Saved ledger state.
    pub state: ModelAttemptState,
    /// A response or local collector-failure record was saved; not proof of remote response.
    pub result_recorded: bool,
    /// Reservation alone always leaves transmission unconfirmed.
    pub evidence: Vec<InspectionEvidence>,
    /// Model identity actually reported, never filled from the route.
    pub reported_model_id: Option<Id>,
    /// Version actually reported.
    pub reported_model_version: Option<Id>,
    /// Actual reported usage, if observed.
    pub usage: Option<ModelUsage>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SavedToolInspection {
    pub tool: VersionedRef,
    pub canonical_name: Id,
    pub canonical_schema_digest: JsonDigest,
    pub compiler: VersionedRef,
    pub target: ProviderToolTarget,
    pub wire_tool: ModelTool,
    pub decode_plan_digest: JsonDigest,
    pub digest: JsonDigest,
    pub fragments: Vec<ToolConstraintFragment>,
    pub enforcement: Vec<ToolConstraintEnforcement>,
}
#[derive(Serialize, Deserialize)]
pub(crate) struct StoredInspection {
    pub reference: RecordRef,
    pub root: PreparedStepRecord,
    pub route: Option<ResolvedModelRoute>,
    pub configuration: Option<ModelConfiguration>,
    pub tools: Vec<(PinnedPromptTool, Option<SavedToolInspection>)>,
    pub projection: Option<PreparedModelProjection>,
    pub attempts: Vec<(ModelInvocationRecord, bool)>,
    pub disclosure: BTreeMap<String, ContextDisclosure>,
    pub limits: RunLimits,
    pub status: RunStatus,
    pub revision: u64,
    pub outcome: Option<RunOutcome>,
    pub unresolved: Vec<UnresolvedInspectionField>,
}

/// Pure conversion: no ports, clocks, policy callbacks or mutable runtime objects.
pub(crate) fn compose(step: StepRef, saved: StoredInspection) -> CompositionReport {
    let mut redacted_paths = vec![];
    let model = saved.route.map(|route| {
        let configuration = saved.configuration.map(|mut config| {
            for (name, options) in [
                ("requested", &mut config.requested),
                ("effective", &mut config.effective),
            ] {
                for (key, value) in options.iter_mut() {
                    let path = format!("model.configuration.{name}.{key}");
                    if sensitive(key) {
                        *value = Value::Null;
                        redacted_paths.push(path);
                    } else {
                        scrub(value, &path, &mut redacted_paths);
                    }
                }
            }
            config
        });
        ModelComposition {
            route_digest: route.digest(),
            provider: route.provider,
            requested_model: route.requested_model,
            model_id: route.model_id,
            model_version: route.model_version,
            version_semantics: route.version_semantics,
            adapter: route.adapter,
            api_contract: route.api_contract,
            capability_revision: route.capability_revision,
            configuration,
        }
    });
    let tools = saved
        .tools
        .into_iter()
        .enumerate()
        .map(|(index, (manifest, compiled))| {
            let mut canonical_schema = manifest.model_tool.model_input_schema;
            scrub(
                &mut canonical_schema,
                &format!("tools.{index}.canonical_schema"),
                &mut redacted_paths,
            );
            let mut report = ToolComposition {
                tool: manifest.tool,
                canonical_name: manifest.model_tool.name,
                canonical_schema,
                canonical_schema_digest: manifest.model_schema_digest,
                provider_name: None,
                provider_schema: None,
                compiler: None,
                compiled_digest: None,
                decode_plan_digest: None,
                constraints: vec![],
                enforcement: vec![],
            };
            if let Some(compiled) = compiled {
                let mut schema = compiled.wire_tool.model_input_schema;
                scrub(
                    &mut schema,
                    &format!("tools.{index}.provider_schema"),
                    &mut redacted_paths,
                );
                report.provider_name = Some(compiled.wire_tool.name);
                report.provider_schema = Some(schema);
                report.constraints = compiled
                    .fragments
                    .into_iter()
                    .map(|fragment| ConstraintComposition {
                        fragment_id: fragment.id,
                        digest: fragment.digest,
                        source: compiled.compiler.clone(),
                    })
                    .collect();
                report.compiler = Some(compiled.compiler);
                report.compiled_digest = Some(compiled.digest);
                report.decode_plan_digest = Some(compiled.decode_plan_digest);
                report.enforcement = compiled.enforcement;
            }
            report
        })
        .collect();
    let fragments = saved
        .projection
        .as_ref()
        .map(|projection| {
            projection
                .provenance
                .fragments
                .iter()
                .enumerate()
                .map(|(order, fragment)| {
                    let (selection, reason) = match &fragment.value {
                        FragmentValue::Item { item }
                            if projection
                                .provenance
                                .selected_context_ids
                                .contains(&item.item_id) =>
                        {
                            ("included".into(), "selected".into())
                        }
                        FragmentValue::Item { item }
                            if projection
                                .provenance
                                .dropped_context_ids
                                .contains(&item.item_id) =>
                        {
                            ("excluded".into(), "not_recorded".into())
                        }
                        FragmentValue::Item { .. } => ("unresolved".into(), "not_recorded".into()),
                        FragmentValue::Tombstone { reason } => {
                            ("tombstone".into(), reason.to_string())
                        }
                        FragmentValue::Selection { status, .. } => {
                            ("selection".into(), status.to_string())
                        }
                    };
                    FragmentComposition {
                        producer_id: fragment.identity.producer_id.clone(),
                        fragment_id: fragment.identity.fragment_id.clone(),
                        core_revision: fragment.core_revision,
                        content_digest: fragment.content_digest.clone(),
                        source_revision: fragment.source_revision.clone(),
                        origin: fragment.origin,
                        order,
                        selection,
                        selection_reason: reason,
                        disclosure: saved
                            .disclosure
                            .get(&disclosure_key(fragment))
                            .cloned()
                            .unwrap_or_else(|| ContextDisclosure::Redacted {
                                reason: "not_requested".into(),
                            }),
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let selection = saved
        .projection
        .as_ref()
        .map(|projection| SelectionComposition {
            through_sequence: projection.provenance.through_sequence,
            included_messages: projection.provenance.selected_message_ids.clone(),
            excluded_messages: projection.provenance.dropped_message_ids.clone(),
            included_context: projection.provenance.selected_context_ids.clone(),
            excluded_context: projection.provenance.dropped_context_ids.clone(),
        });
    let attempts = saved
        .attempts
        .into_iter()
        .map(|(attempt, response_observed)| AttemptComposition {
            evidence: vec![
                InspectionEvidence::DispatchReserved,
                if response_observed {
                    InspectionEvidence::ResponseObserved
                } else {
                    InspectionEvidence::TransmissionUnknown
                },
            ],
            result_recorded: attempt.response_ref.is_some(),
            attempt_id: attempt.attempt_id,
            state: attempt.state,
            reported_model_id: attempt.reported_model_id,
            reported_model_version: attempt.reported_model_version,
            usage: attempt.usage,
        })
        .collect();
    let status = if saved
        .unresolved
        .iter()
        .any(|field| field.reason != "not_recorded")
    {
        InspectionStatus::Partial
    } else {
        InspectionStatus::Found
    };
    CompositionReport {
        schema_version: "wickle.composition-report.v1",
        run_id: saved.root.run_id.clone(),
        step,
        status,
        composition: Some(StepComposition {
            prepared_record_id: saved.reference.record_id,
            model_step_id: saved.root.model_step_id,
            purpose: saved.root.purpose,
            projection_revision: saved.root.projection_revision,
            fingerprint: saved.root.projection_fingerprint,
            assembler: saved.root.assembler,
            change_reason: saved.root.change_reason,
            evidence: vec![InspectionEvidence::Prepared],
            model,
            tools,
            fragments,
            selection,
            attempts,
            estimated_input_tokens: saved
                .projection
                .as_ref()
                .map(|projection| projection.input_tokens),
            estimator: None,
            run_limits: saved.limits,
            recorded_run_status: saved.status,
            recorded_run_revision: saved.revision,
            recorded_run_outcome: saved.outcome.map(|outcome| OutcomeComposition {
                status: outcome.result.status(),
                checkpoint_revision: outcome.checkpoint_revision,
                completion_basis: match outcome.result {
                    OutcomeResult::Succeeded { completion_basis } => Some(completion_basis),
                    _ => None,
                },
            }),
            redacted_paths,
        }),
        unresolved: saved.unresolved,
    }
}
fn sensitive(key: &str) -> bool {
    let normalized: String = key
        .chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect();
    [
        "password",
        "secret",
        "apikey",
        "credential",
        "authorization",
        "accesstoken",
        "refreshtoken",
        "connection",
        "endpoint",
        "executionargs",
        "systeminputs",
        "opaque",
    ]
    .iter()
    .any(|word| normalized.contains(word))
}
fn scrub(value: &mut Value, path: &str, redacted: &mut Vec<String>) {
    match value {
        Value::Object(values) => {
            for (key, value) in values {
                let child = format!("{path}.{key}");
                if sensitive(key)
                    || matches!(
                        key.as_str(),
                        "description" | "examples" | "default" | "$comment"
                    )
                {
                    *value = Value::Null;
                    redacted.push(child);
                } else {
                    scrub(value, &child, redacted);
                }
            }
        }
        Value::Array(values) => {
            for (index, value) in values.iter_mut().enumerate() {
                scrub(value, &format!("{path}.{index}"), redacted);
            }
        }
        _ => {}
    }
}

pub(crate) fn disclosure_key(fragment: &ContextFragment) -> String {
    format!(
        "{}:{}:{}",
        fragment.identity.key(),
        fragment.core_revision,
        fragment.content_digest
    )
}
```

## `crates/wickle/src/lib.rs`

```rust
//! Wickle, an agent engine for Rust applications.
//!
//! Profiles, scoped metadata resolution, and versioned execution data contracts.
//! The agent driver runs model/tool loops with scoped ports, separate system
//! inputs, persisted attempt accounting, and explicit effect outcomes.
//!
//! Runtime objects stay in Host code. Only documented data contracts are
//! serialized; successful decoding does not authenticate a caller.
//!
//! Internal modules are not extension points; use the root exports.
//! ```compile_fail
//! use wickle::serialization::canonical_digest;
//! ```

mod agent;
mod artifacts;
mod budget;
mod canonical;
mod execution_contracts;
pub use execution_contracts::{
    AcceptedSegmentCommand, AppState, BeginSegmentRequest, BeginSegmentResult, ControlAction,
    ControlCommand, ControlReceipt, ExecutionHistory, ExecutionRecordVersion, ExecutionSegment,
    ExecutionTransactions, InterruptionAction, InterruptionCause, InterruptionDecision,
    InterruptionInfo, InterruptionPolicy, InterruptionRecord, PreparedStepRecord, RequestSnapshot,
    SegmentOutcome, SegmentStart, SegmentTransition, StoredControlCommand,
};
mod clock;
mod component_runtime;
mod context;
mod context_projection;
mod context_source;
mod context_strategy;
mod error;
mod hooks;
mod input_binding;
mod message;
mod model;
mod model_options;
pub use model_options::{
    ModelConfiguration, ModelOptionSource, merge_model_options, validate_inference_options,
};
mod model_catalog;
mod model_dispatch;
mod model_execution;
mod model_protocol;
mod model_routing;
mod policy;
mod profile;
mod resolution;
mod run;
mod serialization;
pub use canonical::{
    CanonicalizationVersion, JsonTextLimits, canonicalize_json_text, versioned_digest_json,
};
mod inspection;
mod skills;
mod state;
mod tool_execution;
mod tool_schema;
mod views;
pub use inspection::{
    AttemptComposition, CompositionReport, ConstraintComposition, ContextDisclosure,
    FragmentComposition, InspectionEvidence, InspectionFragmentRef, InspectionOptions,
    InspectionStatus, ModelComposition, OutcomeComposition, SelectionComposition, StepComposition,
    StepRef, ToolComposition, UnresolvedInspectionField,
};

pub use agent::{
    Agent, AgentBindings, AgentSettings, CancelReceipt, ComponentReleaseView, HookObservationView,
    ModelTokenEstimator, PersistenceFailure, RunHandle, UnconfirmedToolEffect, create_agent,
};
pub use artifacts::{
    ArtifactCallContext, ArtifactData, ArtifactInput, ArtifactLimits, ArtifactMetadata,
    ArtifactPreview, ArtifactRuntime, ArtifactStore, MemoryArtifactStore,
};
pub use budget::{AttemptReservation, ReservationKind, RunBudget, RunTiming};
pub use clock::{Clock, ClockReading, IdSource, RandomIdSource, SystemClock};
pub use component_runtime::{
    AdapterBindingState, AdapterCloseContext, AdapterDefinition, AdapterExportDefinition,
    AdapterExportInstance, AdapterFactory, AdapterInitContext, AdapterInstance, BoundCapabilities,
    ComponentBindContext, ComponentBindPurpose, ComponentRelease, ComponentReleaseContext,
    ComponentReleaseFailure, ComponentReleaseReport, ComponentResolveContext, ComponentRuntime,
    ResolvedAdapterBinding, ResolvedAssembly, ResolvedConnection, ResolvedHookBinding,
    ResolvedToolBinding,
};
pub use context_projection::{
    CONTEXT_ASSEMBLER_VERSION, ContextAssembler, ContextItem, ContextLifetime, ContextOrigin,
    ContextPriority, ContextProjection, InstructionAssetContent, PinnedPromptTool, ProjectionInput,
    ProjectionLimits, PromptSnapshot, PromptToolBinding, ScopedOpaque, SkillManifest,
};
pub use context_source::{
    ContextBatch, ContextCallContext, ContextRequest, ContextResult, ContextSource,
    ContextSourceDefinition, ContextSourcePlan, ContextSourceRegistration, ContextSourceRegistry,
    ContextSourceRuntime, ContextSourceUsage, ContextTokenEstimator, ContextUseRequest,
    PlannedContextSource, ResolvedSourceBinding,
};
pub use context_strategy::{
    BoundedContextStrategy, CompactionRequest, ContextCompactor, ContextDecision, ContextPlan,
    ContextPreview, ContextRevision, ContextRewriteLimits, ContextRuntime, ContextSegment,
    ContextSelectionInput, ContextStrategy, ContextStrategyContext, ContextStrategyDefinition,
    HostContextCompactor, ModelCompactorConfig,
};
pub use hooks::{
    HookApplication, HookApplicationRecord, HookContext, HookContextAddition, HookDefinition,
    HookHandler, HookInput, HookObservation, HookObservationStatus, HookOutput, HookPlan,
    HookRegistration, HookRegistry, HookRuntime, HookTarget, HookTransform,
};
pub use input_binding::{
    BoundSystemInput, BoundToolInput, InputBinder, InputBindingLimits, ResolvedSystemInput,
    RunSystemInputs, SystemInputResolveContext, SystemInputResolveRequest, SystemInputResolver,
    ToolBindingResult,
};
pub use model_catalog::{
    CatalogRequirements, ModelAlias, ModelBinding, ModelCapabilities, ModelCatalog,
    ModelCatalogSnapshot, ModelDefinition, ModelDefinitionRef, ModelEvidence, ModelLifecycle,
    ModelSupportStatus, ModelValidationEvidence, ModelValidationKind, ResolvedCatalogBinding,
};
pub use model_protocol::{
    ModelCallContext, ModelContent, ModelEvent, ModelFinish, ModelMessage, ModelOutput, ModelPort,
    ModelPortBinding, ModelProtocolError, ModelProtocolErrorCode, ModelRequest, ModelResponse,
    ModelResponseLimits, ModelResponseMetadata, ModelRole, ModelTool, OpaqueContinuation,
    ProposedToolCall, ToolCallValidation, collect_model_response,
};
pub use model_routing::{
    MAX_ROUTE_FALLBACKS, MAX_ROUTING_RULES, ModelRouter, ROUTING_SNAPSHOT_VERSION, RouteSelection,
    RouteSelectionReason, RoutingPolicy, RoutingRule, RoutingSnapshot,
};
pub use policy::{
    ApprovalChallenge, Guarded, PolicyAction, PolicyContext, PolicyDecision, PolicyGate,
    PolicyPort, PolicyRequest, ToolApproval, ToolPolicyInput,
};
pub use skills::{
    LoadedSkill, PlannedSkill, SkillBindings, SkillCallContext, SkillDefinition, SkillLimits,
    SkillPlan, SkillResolver, SkillRuntime,
};
pub use state::{
    AdmissionInput, AdmissionResult, CommitInput, EventPage, MAX_EVENT_PAGE_SIZE, MemoryStateStore,
    ProtectedRecord, RunLease, STATE_STORE_CHECKPOINT_VERSION, StateStore, StateStoreCapabilities,
    StateStoreCheckpoint, StoredRun,
};
pub use tool_execution::{
    ExternalReceiptContext, ExternalReceiptRequest, ExternalReceiptVerifier,
    PreparedToolResolution, SerialToolRound, ToolEffect, ToolExecutionContext, ToolExecutionLimits,
    ToolExecutionOutcome, ToolExecutionResult, ToolExecutor, ToolRegistration, ToolRegistry,
    ToolRoundOutcome,
};
pub use tool_schema::{
    CompiledTool, SchemaCompiler, SystemInputDefinition, SystemInputRegistry, SystemInputSource,
    TOOL_SCHEMA_COMPILER_VERSION, ToolConcurrency, ToolDescriptor, ToolRetryPolicy, ToolSideEffect,
};
pub use views::{ArtifactView, EventView, RunView};

pub use context::{
    ExecutionContext, ExecutionContextData, PortFuture, PortStream, Scope, SystemInputs,
};
pub use error::{ContractError, ErrorCode};
pub use message::{
    ArtifactRef, ContentBlock, EvidenceRef, Failure, InputContent, Message, MessageOrigin,
    MessageRole, ProviderToolArguments, RecordRef, ToolCall, ToolResult, ToolResultStatus,
    Visibility,
};
pub use model::{
    ApiContract, ModelAttemptState, ModelFailureKind, ModelInvocationRecord, ModelPurpose,
    ModelUsage, ResolvedModelRoute, RouteRequest, UsageMeasurement, VersionPolicy,
    VersionSemantics,
};
pub use model_dispatch::{
    ModelDispatcher, ModelInspectionContext, ModelRouteAvailability, ModelRouteInspector,
    ModelRouteObservation,
};
pub use model_execution::{
    ModelExchange, ModelExchangeOutcome, ModelProjectionContext, ModelRequestProjector,
    ModelRetryPolicy, ProjectedModelRequest, RoutedModelInput, StoredModelResponse,
};
pub use profile::{
    AdapterBindingRef, AgentProfile, CatalogHookRef, CatalogSourceRef, CatalogToolRef,
    CompletionPolicy, ConnectorBindingRef, ContextPolicy, ContextSourceBinding, ContextSourceRef,
    ContextTrigger, ExportRef, HookPosition, HookRef, InstructionAsset, InstructionText,
    Instructions, OutputContract, PROFILE_SCHEMA_VERSION, ProfileSchemaVersion, RunLimits,
    SkillRef, ToolBindingRef, VersionedRef,
};
pub use resolution::{
    ComponentKind, ComponentMetadata, ComponentRef, ExportKind, ExportMetadata, ProfileResolver,
    ProfileValidator, ResolvedComponent, ResolvedProfile,
};
pub use run::{
    ApprovalTarget, BudgetKind, BudgetUsage, CompletionBasis, EphemeralEvent, InputRequest,
    OutcomeResult, RUN_EVENT_SCHEMA_VERSION, RUN_SNAPSHOT_SCHEMA_VERSION, ResumeAction,
    ResumeCommand, ResumeReceipt, RunEvent, RunEventPayload, RunEventSchemaVersion, RunOutcome,
    RunPhase, RunRequest, RunSnapshot, RunSnapshotSchemaVersion, RunStatus, RunTrigger,
    SessionSchemaVersion, SessionSnapshot, SourceExecutionState, SystemInputSnapshotRef,
    ToolCallState, ToolLedgerEntry, VerificationSummary, VerificationVerdict, WaitState,
    WaitTarget, admission_digest,
};
pub use serialization::{
    Id, JsonDigest, JsonObject, canonical_digest, canonical_digest_json, parse_json,
};

mod verification;
pub use verification::{
    OutputSchemaDefinition, VerificationCandidate, VerificationDecision, VerificationInput,
    VerificationLimits, VerificationModel, VerificationModelRequest, VerificationPlan,
    VerificationRuntime, Verifier, VerifierContext, VerifierDefinition,
};

pub use verification::SchemaVerifier;

mod future;

pub use tool_execution::ToolReconciliation;

mod recovery;
pub use recovery::RecoveryReceipt;

mod provider_tool_schema;
pub use provider_tool_schema::{
    ArgumentDecodePlan, ArgumentFieldMapping, ArgumentValueEncoding, CompiledToolContract,
    NativeToolSchemaCompiler, ProviderToolProjection, ProviderToolSchemaCompiler,
    ProviderToolSchemaLimits, ProviderToolTarget, ToolConstraintEnforcement,
    ToolConstraintFragment,
};

mod context_fragment;
pub use context_fragment::{
    CONTEXT_FRAGMENT_ASSEMBLER, ContextFragment, FragmentIdentity, FragmentOwner, FragmentValue,
    select_context_fragments,
};

mod context_lineage;
pub use context_lineage::ContextLineage;

mod prepared_step;
pub use prepared_step::{
    PreparedModelProjection, ProjectionProvenance, ResolvedToolSet, ResolvedToolSetEntry,
};

mod interruption;
pub use interruption::{
    AppStateSchema, ExecutionStopReceipt, InterruptionDecisionRecord, InterruptionPlan,
    InterruptionPolicyBinding,
};
```

## `crates/wickle/src/policy.rs`

```rust
use std::{fmt, future::Future, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{
    ContractError, ErrorCode, ExecutionContext, Id, JsonDigest, JsonObject, ModelPurpose,
    PortFuture, Scope, VersionedRef, serialization::data_digest,
};

/// Final, bound tool inputs visible to the trusted policy implementation.
/// Serialized values require protected storage and must not enter model/UI logs.
#[derive(Clone, PartialEq, Serialize)]
pub struct ToolPolicyInput {
    /// Core call identity.
    pub call_id: Id,
    /// Exact tool identity and version.
    pub tool: VersionedRef,
    /// Pinned descriptor identity.
    pub descriptor_digest: JsonDigest,
    /// Binding identity computed by the trusted input binder.
    pub binding_digest: JsonDigest,
    execution_args: JsonObject,
    #[serde(skip_serializing_if = "Option::is_none")]
    approval: Option<ToolApproval>,
    #[serde(skip_serializing_if = "Option::is_none")]
    selection: Option<crate::ToolBindingRef>,
}

impl ToolPolicyInput {
    /// Own the binder's final arguments. The gate never invents missing IDs.
    pub fn new(
        call_id: Id,
        tool: VersionedRef,
        descriptor_digest: JsonDigest,
        binding_digest: JsonDigest,
        execution_args: JsonObject,
    ) -> Self {
        Self {
            call_id,
            tool,
            descriptor_digest,
            binding_digest,
            execution_args,
            approval: None,
            selection: None,
        }
    }
    /// Inspect the full arguments to check actual target existence and ownership.
    pub fn execution_args(&self) -> &JsonObject {
        &self.execution_args
    }
    /// A recorded approval of this exact binding. The current policy still decides
    /// whether the actor may execute; this evidence never overrides a Deny.
    pub fn approval(&self) -> Option<&ToolApproval> {
        self.approval.as_ref()
    }
    /// Original selected catalog tool or adapter binding/export. A model alias
    /// alone never identifies the authorized external connection.
    pub fn selection(&self) -> Option<&crate::ToolBindingRef> {
        self.selection.as_ref()
    }
    pub(crate) fn with_selection(mut self, selection: crate::ToolBindingRef) -> Self {
        self.selection = Some(selection);
        self
    }
    pub(crate) fn with_approval(mut self, receipt: &crate::ResumeReceipt) -> Self {
        self.approval = Some(ToolApproval {
            command_id: receipt.command.command_id.clone(),
            command_ref: receipt.command_ref.clone(),
            accepted_revision: receipt.accepted_revision,
            actor_ref: receipt.actor_ref.clone(),
            capability_grant_ref: receipt.capability_grant_ref.clone(),
        });
        self
    }
}

/// Core-validated evidence that an authenticated actor approved a fixed tool binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ToolApproval {
    command_id: Id,
    command_ref: crate::RecordRef,
    accepted_revision: u64,
    actor_ref: Id,
    capability_grant_ref: Id,
}
impl ToolApproval {
    /// Accepted command identity.
    pub fn command_id(&self) -> &Id {
        &self.command_id
    }
    /// Protected command record, for authorized auditing.
    pub fn command_ref(&self) -> &crate::RecordRef {
        &self.command_ref
    }
    /// Revision at which approval was committed.
    pub fn accepted_revision(&self) -> u64 {
        self.accepted_revision
    }
    /// Authenticated approver.
    pub fn actor_ref(&self) -> &Id {
        &self.actor_ref
    }
    /// Host grant checked when approval was accepted.
    pub fn capability_grant_ref(&self) -> &Id {
        &self.capability_grant_ref
    }
}

impl fmt::Debug for ToolPolicyInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolPolicyInput")
            .field("call_id", &self.call_id)
            .field("tool", &self.tool)
            .field("descriptor_digest", &self.descriptor_digest)
            .field("binding_digest", &self.binding_digest)
            .field("execution_args", &"<redacted>")
            .finish()
    }
}

/// Operation being authorized; data access and protected-detail access differ.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum PolicyAction {
    /// Admit a new run.
    StartRun {},
    /// Read minimal run metadata.
    ReadRun {},
    /// Read the protected checkpoint, separately from the public view.
    ReadRunDetails {},
    /// Inspect one saved model preparation without executing it.
    InspectStep {
        /// Caller-selected saved identity.
        step: crate::StepRef,
        /// Requested display scope; this never grants secret-input access.
        options: crate::InspectionOptions,
        /// Final raw-content set. Allow must authorize the whole set against one current ACL view.
        /// Empty for the initial metadata/intent check.
        context_fragments: Vec<crate::InspectionFragmentRef>,
    },
    /// Read content of one exact source fragment for diagnostic display.
    InspectContextFragment {
        /// Scoped original producer and fragment identity.
        identity: Box<crate::FragmentIdentity>,
        /// Core observation revision.
        core_revision: std::num::NonZeroU64,
        /// Exact saved content identity.
        content_digest: JsonDigest,
    },
    /// Resume a recorded wait or interruption.
    ResumeRun {
        /// Exact command, so authorization distinguishes approving, denying,
        /// answering and supplying an external receipt.
        command: Box<crate::ResumeCommand>,
        /// Fixed tool binding when the saved wait belongs to a tool.
        binding_digest: Option<JsonDigest>,
    },
    /// Request cancellation.
    CancelRun {},
    /// Explicitly settle an elapsed Run deadline.
    ExpireRun {},
    /// Authorize a Worker to process an already authenticated durable control.
    ProcessControl {
        /// Immutable command being processed, including its original submitter.
        command: Box<crate::ControlCommand>,
    },
    /// Stop one execution interval without granting authority to cancel the Run.
    StopExecution {
        /// Host shutdown or explicit segment stop; protected causes are core-owned.
        cause: crate::InterruptionCause,
    },
    /// Read artifact data/metadata.
    ReadArtifact {},
    /// Write an artifact in the owning scope.
    WriteArtifact {},
    /// Load or use an exact Skill manifest under current access and model destination policy.
    ReadSkill {
        /// Native Skill identity and version.
        skill: VersionedRef,
        /// Complete immutable manifest identity.
        manifest_digest: JsonDigest,
        /// None for local loading/preparation, otherwise the model destination.
        route: Option<Box<crate::ResolvedModelRoute>>,
    },
    /// Read minimal event metadata.
    ReadEvents {},
    /// Read a protected record referenced by an event or checkpoint.
    ReadRecord {},
    /// Use scoped data in model context.
    UseContext {},
    /// Evaluate a fixed candidate under current authorization.
    VerifyCandidate {
        /// Protected candidate identity.
        candidate_ref: crate::RecordRef,
        /// Exact selected verifier, absent for local output validation.
        verifier_ref: Option<crate::VersionedRef>,
    },
    /// Rewrite only an authorized conversation projection, never the original transcript.
    RewriteContext {
        /// Exact read-only strategy identity.
        strategy: VersionedRef,
        /// Model destination for the resulting context.
        route: Box<crate::ResolvedModelRoute>,
    },
    /// Read one explicitly selected automatic context source.
    ProvideContext {
        /// Catalog or adapter export selection.
        source: crate::ContextSourceRef,
        /// Pinned source contract.
        definition_digest: JsonDigest,
        /// Logical lookup identity, reused after persistence.
        context_request_id: Id,
        /// Collection point.
        trigger: crate::ContextTrigger,
        /// Logical model step for step-scoped lookups.
        model_step_id: Option<Id>,
        /// Identity of the scoped query and lookup settings.
        input_digest: JsonDigest,
    },
    /// Recheck a saved source batch before local transformation or model transmission.
    UseSourceContext {
        /// Exact original source selection.
        source: crate::ContextSourceRef,
        /// Pinned source contract.
        definition_digest: JsonDigest,
        /// Saved batch whose data and derived context are being used.
        batch_ref: crate::RecordRef,
        /// None for local preparation, otherwise the exact model destination.
        route: Option<Box<crate::ResolvedModelRoute>>,
    },
    /// Invoke one selected lifecycle hook under its pinned definition and target.
    InvokeHook {
        /// Exact selected hook version.
        hook: VersionedRef,
        /// Original adapter binding/export; absent for catalog hooks.
        selection: Option<crate::HookRef>,
        /// Immutable execution definition.
        definition_digest: JsonDigest,
        /// Exact lifecycle invocation scope within the Run.
        target: crate::HookTarget,
    },
    /// Resolve approved component metadata before admission, without opening a connection.
    ResolveComponents {
        /// Identity of the profile and metadata being assembled.
        profile_resolution_digest: JsonDigest,
    },
    /// Open one adapter for a scoped execution or observer segment.
    BindAdapter {
        /// Profile-local binding, independent of exported model aliases.
        binding_id: Id,
        /// Exact registered adapter implementation version.
        adapter: VersionedRef,
        /// Full pinned definition including export contracts.
        definition_digest: JsonDigest,
        /// Named Host account/connection revisions, without credentials.
        connections: std::collections::BTreeMap<Id, VersionedRef>,
        /// Fresh scope-bound execution segment identity.
        binding_set_id: Id,
        /// Whether business tools or only observers may be activated.
        purpose: crate::ComponentBindPurpose,
    },
    /// Read one registered system key before the final target value is known.
    ResolveSystemInput {
        /// Exact tool requesting the lookup.
        tool: VersionedRef,
        /// Original adapter export selection, absent for catalog tools.
        selection: Option<crate::ToolBindingRef>,
        /// Logical call whose binding is being prepared.
        call_id: Id,
        /// Pinned tool descriptor identity.
        descriptor_digest: JsonDigest,
        /// Compiled input contract identity.
        compiled_digest: JsonDigest,
        /// Exact registry key, never a path expression.
        key: Id,
        /// Pinned system-input definition revision.
        definition_version: Id,
        /// Exact read-only resolver implementation.
        resolver_ref: VersionedRef,
    },
    /// Send input to a selected model route.
    InvokeModel {
        /// Exact provider, target, model and connection metadata for current authorization.
        route: Box<crate::ResolvedModelRoute>,
        /// Purpose being authorized.
        purpose: ModelPurpose,
    },
    /// Read the external outcome of a previously dispatched attempt.
    ReconcileTool {
        /// Frozen inputs and selected external binding.
        input: ToolPolicyInput,
        /// Original charged attempt, not a new execution.
        attempt_id: Id,
        /// Original external idempotency identity.
        idempotency_key: Id,
    },
    /// Dispatch one tool using final validated inputs.
    ExecuteTool {
        /// Final inputs, including system-owned parameters.
        input: ToolPolicyInput,
    },
}

/// An operation on an authoritative resource identity.
/// Obtain owner_scope from trusted stored metadata, not a caller's claimed scope.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PolicyRequest {
    /// Stored owner scope; user_id=None is not a wildcard.
    pub owner_scope: Scope,
    /// Run, artifact, event stream, record, context, or tool resource identity.
    pub resource_id: Id,
    /// Exact proposed action.
    pub action: PolicyAction,
}

impl PolicyRequest {
    /// Identity of the full proposed action and owning scope, including tool inputs.
    /// It excludes the approving principal so a new authorized reviewer can act.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }
}

/// Current authenticated policy context, without the run's whole system-input map.
pub struct PolicyContext<'a> {
    /// Current authenticated resource scope.
    pub scope: &'a Scope,
    /// Current principal, distinct from resource scope and original tool inputs.
    pub principal_ref: &'a Id,
    /// Current grant reference; the Host checks membership and revocation.
    pub capability_grant_ref: &'a Id,
    /// Cooperative cancellation signal.
    pub cancellation: &'a CancellationToken,
    /// Effective policy deadline on the monotonic clock.
    pub deadline: Instant,
}

/// Host authorization decision. A reason is an informational code, not a grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
pub enum PolicyDecision {
    /// This exact action is currently allowed.
    Allow {},
    /// The action is denied.
    Deny {
        /// Safe reason code, without bound values or SDK error text.
        reason: Id,
    },
    /// The action requires an approval flow before it can be performed.
    RequireApproval {
        /// Safe reason code.
        reason: Id,
    },
}

impl PolicyDecision {
    /// Intersect a Host decision with a restriction; an allow never removes a denial
    /// or an approval requirement. Existing Host reasons take precedence.
    pub fn restrict(self, restriction: Self) -> Self {
        match (self, restriction) {
            (denied @ Self::Deny { .. }, _) | (_, denied @ Self::Deny { .. }) => denied,
            (approval @ Self::RequireApproval { .. }, _)
            | (_, approval @ Self::RequireApproval { .. }) => approval,
            _ => Self::Allow {},
        }
    }
}

/// Trusted Host policy. Implement actual resource/membership/FK checks here.
pub trait PolicyPort: Send + Sync {
    /// Check the current grant against the exact bound action without performing it.
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision>;
}

/// An approval request bound to an exact action, not a reusable permission token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApprovalChallenge {
    /// Scope whose resource will be affected.
    pub scope: Scope,
    /// Resource identity.
    pub resource_id: Id,
    /// Digest includes final tool input, descriptor/version, and scope.
    pub request_digest: JsonDigest,
    /// Safe reason code for the Host's approval UI.
    pub reason: Id,
}

/// Result of a guarded operation. Approval-required never invokes the operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Guarded<T> {
    /// Operation completed after a current authorization check.
    Completed(T),
    /// No operation was invoked; the Host/runtime must handle this approval request.
    ApprovalRequired(ApprovalChallenge),
}

/// Current authorization with exact scope matching, timeout, and cancellation.
/// This does not authenticate caller-supplied JSON or provide a sandbox for Host code.
pub struct PolicyGate {
    policy: Arc<dyn PolicyPort>,
    timeout: Duration,
}

impl PolicyGate {
    /// Configure a finite, positive policy timeout without creating a runtime.
    pub fn new(policy: Arc<dyn PolicyPort>, timeout: Duration) -> Result<Self, ContractError> {
        if timeout.is_zero() || Instant::now().checked_add(timeout).is_none() {
            return Err(ContractError::new(
                ErrorCode::InvalidContract,
                "policy.timeout",
            ));
        }
        Ok(Self { policy, timeout })
    }

    /// Check the current Host decision. Every call rechecks policy; permits are not cached.
    pub async fn check(
        &self,
        request: &PolicyRequest,
        context: &ExecutionContext,
        deadline: Option<Instant>,
        restriction: Option<PolicyDecision>,
    ) -> Result<PolicyDecision, ContractError> {
        if request.owner_scope != context.data.scope {
            return Err(ContractError::new(ErrorCode::AccessDenied, "scope"));
        }
        if context.cancellation.is_cancelled() {
            return Err(ContractError::new(ErrorCode::Cancelled, "policy"));
        }
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(ContractError::new(ErrorCode::RuntimeUnavailable, "policy"));
        }
        let now = Instant::now();
        let policy_deadline = now
            .checked_add(self.timeout)
            .ok_or_else(|| ContractError::new(ErrorCode::InvalidContract, "policy.timeout"))?;
        let effective = deadline.map_or(policy_deadline, |d| d.min(policy_deadline));
        if effective <= now {
            return Err(ContractError::new(ErrorCode::DeadlineExceeded, "policy"));
        }
        let decision = AssertUnwindSafe(async {
            tokio::select! {
                biased;
                _ = context.cancellation.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "policy")),
                _ = tokio::time::sleep_until(effective) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "policy")),
                result = self.policy.authorize(request, PolicyContext {
                    scope: &context.data.scope, principal_ref: &context.data.principal_ref,
                    capability_grant_ref: &context.data.capability_grant_ref,
                    cancellation: &context.cancellation, deadline: effective,
                }) => result.map_err(|_| ContractError::new(ErrorCode::PolicyUnavailable, "policy")),
            }
        }).catch_unwind().await.map_err(|_| ContractError::new(ErrorCode::PolicyUnavailable, "policy"))??;
        if context.cancellation.is_cancelled() {
            return Err(ContractError::new(ErrorCode::Cancelled, "policy"));
        }
        if Instant::now() >= effective {
            return Err(ContractError::new(ErrorCode::DeadlineExceeded, "policy"));
        }
        Ok(match restriction {
            Some(other) => decision.restrict(other),
            None => decision,
        })
    }

    /// Invoke a closure only after current policy allows it. Future construction is
    /// also delayed until authorization. The operation owns its I/O cancellation and
    /// effect reconciliation; dropping a future is not treated as external rollback.
    pub async fn guard<T, F, Fut>(
        &self,
        request: &PolicyRequest,
        context: &ExecutionContext,
        deadline: Option<Instant>,
        restriction: Option<PolicyDecision>,
        operation: F,
    ) -> Result<Guarded<T>, ContractError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, ContractError>>,
    {
        match self.check(request, context, deadline, restriction).await? {
            PolicyDecision::Allow {} => operation().await.map(Guarded::Completed),
            PolicyDecision::Deny { .. } => {
                Err(ContractError::new(ErrorCode::AccessDenied, "policy"))
            }
            PolicyDecision::RequireApproval { reason } => {
                Ok(Guarded::ApprovalRequired(ApprovalChallenge {
                    scope: request.owner_scope.clone(),
                    resource_id: request.resource_id.clone(),
                    request_digest: request.digest(),
                    reason,
                }))
            }
        }
    }
}
```

## `crates/wickle/src/provider_tool_schema.rs`

```rust
//! Pure, bounded provider projection of an already separated Tool input contract.
use crate::{
    ApiContract, CompiledTool, ContractError, ErrorCode, Id, JsonDigest, JsonObject, ModelTool,
    VersionedRef, canonical_digest, parse_json, serialization::data_digest,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, fmt};

/// Exact provider protocol and capability revision used for compilation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderToolTarget {
    /// Provider namespace, including deployment-specific provider adapters.
    pub provider: Id,
    /// Exact operation and API version.
    pub api_contract: ApiContract,
    /// Pinned target capability revision.
    pub capability_revision: Id,
}
/// Finite bounds on compilation, persisted projection and incoming arguments.
#[derive(Debug, Clone, Copy)]
pub struct ProviderToolSchemaLimits {
    /// Maximum serialized canonical or wire schema/tool bytes.
    pub max_schema_bytes: usize,
    /// Maximum schema nesting before traversal or serialization.
    pub max_schema_depth: usize,
    /// Maximum total serialized compiled contract bytes, including explanations.
    pub max_contract_bytes: usize,
    /// Maximum provider argument bytes before parsing.
    pub max_argument_bytes: usize,
}
impl Default for ProviderToolSchemaLimits {
    fn default() -> Self {
        Self {
            max_schema_bytes: 65_536,
            max_schema_depth: 64,
            max_contract_bytes: 262_144,
            max_argument_bytes: 65_536,
        }
    }
}
/// Reversible representation of a single model-owned field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArgumentValueEncoding {
    /// Preserve the JSON value, including explicit null.
    Identity {},
    /// Encode omission separately from null using an object envelope.
    Presence {
        /// Boolean discriminator: false means omitted, true means supplied.
        present_key: String,
        /// Required value member; must be null when present is false.
        value_key: String,
    },
}
/// One-to-one mapping from a wire property to an exposed canonical property.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArgumentFieldMapping {
    /// Property emitted by the provider.
    pub wire_name: String,
    /// Original model-owned property, never a system-owned property.
    pub canonical_name: String,
    /// Value and omission restoration rule.
    pub encoding: ArgumentValueEncoding,
}
/// Stored codec. It never guesses that null means omission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArgumentDecodePlan {
    /// Property names and values are unchanged.
    Identity {},
    /// Explicit complete field mapping; unknown wire properties are errors.
    Fields {
        /// Ordered mappings with unique wire and canonical names.
        fields: Vec<ArgumentFieldMapping>,
    },
}
/// Compiler output before the core stamps original identity and explanations.
#[derive(Debug, Clone)]
pub struct ProviderToolProjection {
    /// The exact Tool definition submitted to the provider.
    pub wire_tool: ModelTool,
    /// Reversible normalization back into original model-owned arguments.
    pub decode_plan: ArgumentDecodePlan,
}
/// Pure trusted adapter extension. Input contains only the model-visible Tool;
/// hidden definitions, system values, credentials and runtime handles are absent.
pub trait ProviderToolSchemaCompiler: Send + Sync {
    /// Immutable implementation identity; change its version when output changes.
    fn reference(&self) -> VersionedRef;
    /// Preserve native constraints where supported. Unsupported representation
    /// must use a relaxed schema plus a reversible codec, never delete the Tool.
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError>;
}
/// Compiler for protocols that accept the original model-visible JSON Schema.
#[derive(Debug, Clone, Copy, Default)]
pub struct NativeToolSchemaCompiler;
impl ProviderToolSchemaCompiler for NativeToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new("wickle-native-tool-schema").expect("static id"),
            version: Id::new("1").expect("static version"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
        _: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        Ok(ProviderToolProjection {
            wire_tool: tool.clone(),
            decode_plan: ArgumentDecodePlan::Identity {},
        })
    }
}
/// Deterministic trusted explanation associated with this exact Tool projection.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolConstraintFragment {
    /// Stable content-addressed identity, ordered in the compiled contract.
    pub id: Id,
    /// Only canonical model-visible schema and codec instructions.
    pub text: String,
    /// Digest of the exact explanation text.
    pub digest: JsonDigest,
}
impl fmt::Debug for ToolConstraintFragment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolConstraintFragment")
            .field("id", &self.id)
            .field("digest", &self.digest)
            .finish()
    }
}
/// Where an original schema node is enforced. Core validation is always required.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolConstraintEnforcement {
    /// JSON pointer into the canonical model schema; empty denotes the whole schema.
    pub canonical_pointer: String,
    /// Confirmed native under an identical schema and identity codec. False is
    /// conservative: a relaxed wire schema can still enforce part of this node.
    pub provider_native: bool,
    /// Included in the canonical constraint explanation.
    pub context_text: bool,
    /// Original validation must occur after decoding, before execution.
    pub core: bool,
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContractData {
    schema_version: String,
    tool: VersionedRef,
    canonical_name: Id,
    descriptor_digest: JsonDigest,
    canonical_schema_digest: JsonDigest,
    compiler: VersionedRef,
    target: ProviderToolTarget,
    wire_tool: ModelTool,
    decode_plan: ArgumentDecodePlan,
    fragments: Vec<ToolConstraintFragment>,
    enforcement: Vec<ToolConstraintEnforcement>,
}
/// Immutable route-specific contract. Serialize only to protected storage; submit
/// wire_tool and constraint_fragments to the model, not the whole record.
#[derive(Clone, Serialize)]
pub struct CompiledToolContract {
    data: ContractData,
    digest: JsonDigest,
}
impl fmt::Debug for CompiledToolContract {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompiledToolContract")
            .field("tool", &self.data.tool)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}
impl CompiledToolContract {
    /// Compile only after canonical ownership separation and validate finite output.
    pub fn compile(
        tool: &CompiledTool,
        target: ProviderToolTarget,
        compiler: &dyn ProviderToolSchemaCompiler,
        limits: ProviderToolSchemaLimits,
    ) -> Result<Self, ContractError> {
        check_limits(limits)?;
        let visible = tool.to_model_tool();
        depth_bound(&visible.model_input_schema, limits.max_schema_depth)?;
        bounded(&visible, limits.max_schema_bytes)?;
        let reference = compiler.reference();
        let projection = compiler.compile(&visible, &target)?;
        if compiler.reference() != reference {
            return Err(invalid("provider_tool.compiler_revision"));
        }
        Self::build(tool, target, reference, projection, limits)
    }
    fn build(
        tool: &CompiledTool,
        target: ProviderToolTarget,
        compiler: VersionedRef,
        projection: ProviderToolProjection,
        limits: ProviderToolSchemaLimits,
    ) -> Result<Self, ContractError> {
        check_limits(limits)?;
        depth_bound(tool.model_input_schema(), limits.max_schema_depth)?;
        depth_bound(
            &projection.wire_tool.model_input_schema,
            limits.max_schema_depth,
        )?;
        bounded(&projection.wire_tool, limits.max_schema_bytes)?;
        let schema = &projection.wire_tool.model_input_schema;
        if !valid_name(projection.wire_tool.name.as_str())
            || schema.get("type") != Some(&json!("object"))
            || schema.get("additionalProperties") != Some(&json!(false))
        {
            return Err(invalid("provider_tool.wire_boundary"));
        }
        crate::tool_schema::compile_validator(schema)?;
        validate_codec(tool.model_input_schema(), schema, &projection.decode_plan)?;
        let identity = matches!(projection.decode_plan, ArgumentDecodePlan::Identity {});
        let explained = schema != tool.model_input_schema() || !identity;
        let fragments = if explained {
            let text = format!(
                "Tool {}: arguments must satisfy this canonical JSON Schema after decoding: {}\nDecode representation: {}. Field mappings restore wire_name to canonical_name. For a presence envelope, both members are required: true marks a supplied value (including explicit null); false with a null value placeholder means omission. Preserve omission and explicit null as distinct values.",
                projection.wire_tool.name,
                serde_json::to_string(tool.model_input_schema())
                    .map_err(|_| invalid("provider_tool.schema"))?,
                serde_json::to_string(&projection.decode_plan)
                    .map_err(|_| invalid("provider_tool.codec"))?
            );
            let digest = data_digest(&text);
            vec![ToolConstraintFragment {
                id: Id::new(format!(
                    "tool-constraints-{}",
                    canonical_digest(&json!(text))
                ))?,
                text,
                digest,
            }]
        } else {
            vec![]
        };
        let mut enforcement = Vec::new();
        collect_enforcement(
            tool.model_input_schema(),
            schema,
            "",
            identity && schema == tool.model_input_schema(),
            explained,
            &mut enforcement,
        );
        let data = ContractData {
            schema_version: "wickle.provider-tool-contract.v1".into(),
            tool: tool.descriptor().tool.clone(),
            canonical_name: tool.descriptor().name.clone(),
            descriptor_digest: tool.descriptor_digest().clone(),
            canonical_schema_digest: tool.model_schema_digest().clone(),
            compiler,
            target,
            wire_tool: projection.wire_tool,
            decode_plan: projection.decode_plan,
            fragments,
            enforcement,
        };
        let result = Self {
            digest: data_digest(&data),
            data,
        };
        bounded(&result, limits.max_contract_bytes)?;
        Ok(result)
    }
    /// Restore against the trusted original Tool, destination and expected digest.
    /// This uses the saved codec and never invokes a newer compiler implementation.
    pub fn restore(
        text: &str,
        tool: &CompiledTool,
        target: &ProviderToolTarget,
        expected: &JsonDigest,
        limits: ProviderToolSchemaLimits,
    ) -> Result<Self, ContractError> {
        check_limits(limits)?;
        if text.len() > limits.max_contract_bytes {
            return Err(invalid("provider_tool.size"));
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Saved {
            data: ContractData,
            digest: JsonDigest,
        }
        let saved: Saved = serde_json::from_value(parse_json(text)?)
            .map_err(|_| invalid("provider_tool.record"))?;
        if &saved.digest != expected
            || data_digest(&saved.data) != *expected
            || &saved.data.target != target
        {
            return Err(invalid("provider_tool.identity"));
        }
        let rebuilt = Self::build(
            tool,
            target.clone(),
            saved.data.compiler.clone(),
            ProviderToolProjection {
                wire_tool: saved.data.wire_tool.clone(),
                decode_plan: saved.data.decode_plan.clone(),
            },
            limits,
        )?;
        if rebuilt.data != saved.data || rebuilt.digest != *expected {
            return Err(invalid("provider_tool.identity"));
        }
        Ok(rebuilt)
    }
    pub(crate) fn inspection(
        value: Value,
    ) -> Result<crate::inspection::SavedToolInspection, ContractError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Saved {
            data: ContractData,
            digest: JsonDigest,
        }
        let saved: Saved =
            serde_json::from_value(value).map_err(|_| invalid("provider_tool.record"))?;
        if saved.data.schema_version != "wickle.provider-tool-contract.v1"
            || data_digest(&saved.data) != saved.digest
        {
            return Err(invalid("provider_tool.identity"));
        }
        Ok(crate::inspection::SavedToolInspection {
            tool: saved.data.tool,
            canonical_name: saved.data.canonical_name,
            canonical_schema_digest: saved.data.canonical_schema_digest,
            compiler: saved.data.compiler,
            target: saved.data.target,
            wire_tool: saved.data.wire_tool,
            decode_plan_digest: data_digest(&saved.data.decode_plan),
            digest: saved.digest,
            fragments: saved.data.fragments,
            enforcement: saved.data.enforcement,
        })
    }
    /// Original model-facing Tool name to restore after provider name mapping.
    pub fn canonical_name(&self) -> &Id {
        &self.data.canonical_name
    }
    /// Original registered Tool identity.
    pub fn tool(&self) -> &VersionedRef {
        &self.data.tool
    }
    /// Frozen provider-facing Tool, excluding hidden input metadata.
    pub fn wire_tool(&self) -> &ModelTool {
        &self.data.wire_tool
    }
    /// Ordered trusted fragments that must accompany the Tool definition.
    pub fn constraint_fragments(&self) -> &[ToolConstraintFragment] {
        &self.data.fragments
    }
    /// Exact original constraint locations and enforcement mechanisms.
    pub fn enforcement(&self) -> &[ToolConstraintEnforcement] {
        &self.data.enforcement
    }
    /// Pinned compiler identity and version.
    pub fn compiler(&self) -> &VersionedRef {
        &self.data.compiler
    }
    /// Exact destination protocol/capability revision.
    pub fn target(&self) -> &ProviderToolTarget {
        &self.data.target
    }
    /// Protected compilation identity.
    pub fn digest(&self) -> &JsonDigest {
        &self.digest
    }
    /// Encode canonical historical model arguments for this exact provider
    /// representation. System inputs never belong in this map.
    pub fn encode_arguments(&self, input: &JsonObject) -> Result<JsonObject, ContractError> {
        match &self.data.decode_plan {
            ArgumentDecodePlan::Identity {} => Ok(input.clone()),
            ArgumentDecodePlan::Fields { fields } => {
                if input
                    .keys()
                    .any(|key| !fields.iter().any(|field| &field.canonical_name == key))
                {
                    return Err(arguments());
                }
                let mut output = JsonObject::new();
                for field in fields {
                    let value = input.get(&field.canonical_name);
                    match &field.encoding {
                        ArgumentValueEncoding::Identity {} => {
                            if let Some(value) = value {
                                output.insert(field.wire_name.clone(), value.clone());
                            }
                        }
                        ArgumentValueEncoding::Presence {
                            present_key,
                            value_key,
                        } => {
                            let envelope = serde_json::Map::from_iter([
                                (present_key.clone(), Value::Bool(value.is_some())),
                                (value_key.clone(), value.cloned().unwrap_or(Value::Null)),
                            ]);
                            output.insert(field.wire_name.clone(), Value::Object(envelope));
                        }
                    }
                }
                Ok(output)
            }
        }
    }
    /// Restore model-owned names and values. Validation/defaults/system binding
    /// are separate boundaries; this does not authorize or execute the Tool.
    pub fn decode_arguments(
        &self,
        raw: &str,
        limits: ProviderToolSchemaLimits,
    ) -> Result<JsonObject, ContractError> {
        check_limits(limits)?;
        let object = parse_provider_arguments(raw, limits.max_argument_bytes)?;
        match &self.data.decode_plan {
            ArgumentDecodePlan::Identity {} => Ok(object.clone()),
            ArgumentDecodePlan::Fields { fields } => {
                let mut result = JsonObject::new();
                for (name, value) in &object {
                    let mapping = fields
                        .iter()
                        .find(|field| &field.wire_name == name)
                        .ok_or_else(arguments)?;
                    let restored = match &mapping.encoding {
                        ArgumentValueEncoding::Identity {} => Some(value.clone()),
                        ArgumentValueEncoding::Presence {
                            present_key,
                            value_key,
                        } => {
                            let envelope = value.as_object().ok_or_else(arguments)?;
                            match envelope.get(present_key).and_then(Value::as_bool) {
                                Some(false)
                                    if envelope.len() == 2
                                        && envelope.get(value_key) == Some(&Value::Null) =>
                                {
                                    None
                                }
                                Some(true) if envelope.len() == 2 => {
                                    Some(envelope.get(value_key).ok_or_else(arguments)?.clone())
                                }
                                _ => return Err(arguments()),
                            }
                        }
                    };
                    if let Some(value) = restored {
                        result.insert(mapping.canonical_name.clone(), value);
                    }
                }
                Ok(result)
            }
        }
    }
}
fn validate_codec(
    canonical: &Value,
    wire: &Value,
    plan: &ArgumentDecodePlan,
) -> Result<(), ContractError> {
    let canonical = canonical
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("provider_tool.canonical_properties"))?;
    let wire = wire
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("provider_tool.wire_properties"))?;
    match plan {
        ArgumentDecodePlan::Identity {} if canonical.keys().eq(wire.keys()) => Ok(()),
        ArgumentDecodePlan::Fields { fields } => {
            let mut from = BTreeSet::new();
            let mut to = BTreeSet::new();
            for field in fields {
                if !wire.contains_key(&field.wire_name)
                    || !canonical.contains_key(&field.canonical_name)
                    || !from.insert(&field.wire_name)
                    || !to.insert(&field.canonical_name)
                {
                    return Err(invalid("provider_tool.codec_mapping"));
                }
                if let ArgumentValueEncoding::Presence {
                    present_key,
                    value_key,
                } = &field.encoding
                {
                    if present_key.is_empty() || value_key.is_empty() || present_key == value_key {
                        return Err(invalid("provider_tool.presence_keys"));
                    }
                }
            }
            if from.len() != wire.len() || to.len() != canonical.len() {
                return Err(invalid("provider_tool.codec_coverage"));
            }
            Ok(())
        }
        _ => Err(invalid("provider_tool.codec_mapping")),
    }
}
fn collect_enforcement(
    canonical: &Value,
    wire: &Value,
    pointer: &str,
    identity: bool,
    text: bool,
    output: &mut Vec<ToolConstraintEnforcement>,
) {
    output.push(ToolConstraintEnforcement {
        canonical_pointer: pointer.into(),
        provider_native: identity && wire.pointer(pointer) == Some(canonical),
        context_text: text,
        core: true,
    });
    let Some(map) = canonical.as_object() else {
        return;
    };
    for (key, value) in map {
        if matches!(
            key.as_str(),
            "title"
                | "description"
                | "default"
                | "examples"
                | "$comment"
                | "$schema"
                | "$id"
                | "deprecated"
                | "readOnly"
                | "writeOnly"
        ) {
            continue;
        }
        let path = format!("{pointer}/{}", key.replace('~', "~0").replace('/', "~1"));
        match key.as_str() {
            "properties" | "$defs" | "definitions" | "dependentSchemas" | "patternProperties" => {
                if let Some(children) = value.as_object() {
                    for (name, child) in children {
                        collect_enforcement(
                            child,
                            wire,
                            &format!("{path}/{}", name.replace('~', "~0").replace('/', "~1")),
                            identity,
                            text,
                            output,
                        );
                    }
                }
            }
            "allOf" | "anyOf" | "oneOf" | "prefixItems" => {
                if let Some(children) = value.as_array() {
                    for (index, child) in children.iter().enumerate() {
                        collect_enforcement(
                            child,
                            wire,
                            &format!("{path}/{index}"),
                            identity,
                            text,
                            output,
                        );
                    }
                }
                output.push(ToolConstraintEnforcement {
                    canonical_pointer: path.clone(),
                    provider_native: identity && wire.pointer(&path) == Some(value),
                    context_text: text,
                    core: true,
                });
            }
            "items"
            | "additionalProperties"
            | "unevaluatedProperties"
            | "unevaluatedItems"
            | "contains"
            | "not"
            | "if"
            | "then"
            | "else"
            | "propertyNames" => collect_enforcement(value, wire, &path, identity, text, output),
            _ => output.push(ToolConstraintEnforcement {
                canonical_pointer: path.clone(),
                provider_native: identity && wire.pointer(&path) == Some(value),
                context_text: text,
                core: true,
            }),
        }
    }
}

fn bounded(value: &impl Serialize, max: usize) -> Result<(), ContractError> {
    if serde_json::to_vec(value)
        .map_err(|_| invalid("provider_tool.json"))?
        .len()
        > max
    {
        return Err(invalid("provider_tool.size"));
    }
    Ok(())
}
fn check_limits(limits: ProviderToolSchemaLimits) -> Result<(), ContractError> {
    if limits.max_schema_depth == 0
        || limits.max_schema_depth > 128
        || limits.max_schema_bytes == 0
        || limits.max_contract_bytes == 0
        || limits.max_argument_bytes == 0
    {
        return Err(invalid("provider_tool.limits"));
    }
    Ok(())
}
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn invalid(path: &str) -> ContractError {
    ContractError::new(ErrorCode::UnsupportedInputProjection, path)
}
fn arguments() -> ContractError {
    ContractError::new(ErrorCode::InvalidArguments, "provider_tool.arguments")
}

fn depth_bound(schema: &Value, max: usize) -> Result<(), ContractError> {
    let mut pending = vec![(schema, 0)];
    while let Some((value, depth)) = pending.pop() {
        if depth > max {
            return Err(invalid("provider_tool.depth"));
        }
        match value {
            Value::Object(map) => pending.extend(map.values().map(|value| (value, depth + 1))),
            Value::Array(array) => pending.extend(array.iter().map(|value| (value, depth + 1))),
            _ => {}
        }
    }
    Ok(())
}

// The legacy Value parser must remain unchanged for old digests. This new codec
// refuses values it cannot represent, rather than silently rounding model input.
fn numbers_preserved(raw: &serde_json::value::RawValue, parsed: &Value) -> bool {
    use serde_json::value::RawValue;
    match raw.get().as_bytes()[0] {
        b'{' => {
            let Ok(object) =
                serde_json::from_str::<std::collections::BTreeMap<String, &RawValue>>(raw.get())
            else {
                return false;
            };
            object.into_iter().all(|(key, raw)| {
                parsed
                    .get(&key)
                    .is_some_and(|value| numbers_preserved(raw, value))
            })
        }
        b'[' => {
            let Ok(array) = serde_json::from_str::<Vec<&RawValue>>(raw.get()) else {
                return false;
            };
            array.into_iter().enumerate().all(|(index, raw)| {
                parsed
                    .get(index)
                    .is_some_and(|value| numbers_preserved(raw, value))
            })
        }
        b'-' | b'0'..=b'9' => parsed.as_number().is_some_and(|number| {
            normalized_decimal(raw.get())
                .is_some_and(|original| Some(original) == normalized_decimal(&number.to_string()))
        }),
        _ => true,
    }
}
fn normalized_decimal(text: &str) -> Option<(bool, String, i128)> {
    let negative = text.starts_with('-');
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let (mantissa, exponent) = unsigned.split_once(['e', 'E']).unwrap_or((unsigned, "0"));
    let fraction = mantissa
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Some((false, "0".into(), 0));
    }
    let trimmed = digits.trim_end_matches('0');
    let exponent = exponent
        .parse::<i128>()
        .ok()?
        .checked_sub(fraction as i128)?
        .checked_add((digits.len() - trimmed.len()) as i128)?;
    Some((negative, trimmed.into(), exponent))
}

pub(crate) fn parse_provider_arguments(
    raw: &str,
    max_bytes: usize,
) -> Result<JsonObject, ContractError> {
    if raw.len() > max_bytes {
        return Err(arguments());
    }
    let value = parse_json(raw).map_err(|_| arguments())?;
    let original: &serde_json::value::RawValue =
        serde_json::from_str(raw).map_err(|_| arguments())?;
    if !numbers_preserved(original, &value) {
        return Err(ContractError::new(
            ErrorCode::InvalidArguments,
            "provider_tool.numeric_precision",
        ));
    }
    Ok(value
        .as_object()
        .ok_or_else(arguments)?
        .clone()
        .into_iter()
        .collect())
}
```

## `crates/wickle/tests/agent_inspection.rs`

```rust
//! Stored diagnostics disclose evidence without preparing or executing another step.
#[path = "support/agent.rs"]
#[allow(dead_code)]
mod agent_support;
#[path = "support/agent_hooks.rs"]
#[allow(dead_code)]
mod hooks_support;
#[path = "support/agent_resume.rs"]
#[allow(dead_code)]
mod resume_support;
#[path = "support/context_sources.rs"]
#[allow(dead_code, unused_imports)]
mod source_support;
use agent_support::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;

struct InspectionPolicy {
    mode: AtomicUsize,
    content: AtomicUsize,
    fragment_checks: AtomicUsize,
    pause_fragment: AtomicUsize,
    fragment_entered: tokio::sync::Notify,
    fragment_release: tokio::sync::Semaphore,
}
impl Default for InspectionPolicy {
    fn default() -> Self {
        Self {
            mode: AtomicUsize::new(0),
            content: AtomicUsize::new(0),
            fragment_checks: AtomicUsize::new(0),
            pause_fragment: AtomicUsize::new(0),
            fragment_entered: tokio::sync::Notify::new(),
            fragment_release: tokio::sync::Semaphore::new(0),
        }
    }
}
impl PolicyPort for InspectionPolicy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            let mode = match &request.action {
                PolicyAction::InspectStep {
                    context_fragments, ..
                } => {
                    let mode = self.mode.load(Ordering::SeqCst);
                    if mode == 0 && !context_fragments.is_empty() {
                        self.content.load(Ordering::SeqCst)
                    } else {
                        mode
                    }
                }
                PolicyAction::InspectContextFragment { .. } => {
                    let number = self.fragment_checks.fetch_add(1, Ordering::SeqCst) + 1;
                    if number == self.pause_fragment.load(Ordering::SeqCst) {
                        self.fragment_entered.notify_one();
                        self.fragment_release.acquire().await.unwrap().forget();
                    }
                    self.content.load(Ordering::SeqCst)
                }
                _ => 0,
            };
            Ok(match mode {
                1 => PolicyDecision::Deny {
                    reason: id("revoked"),
                },
                2 => PolicyDecision::RequireApproval {
                    reason: id("review"),
                },
                _ => PolicyDecision::Allow {},
            })
        })
    }
}
struct NoExecution;
impl ModelPort for NoExecution {
    fn binding(&self) -> ModelPortBinding {
        panic!("inspection must not resolve a model")
    }
    fn generate<'a>(
        &'a self,
        _: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        panic!("inspection must not invoke a model")
    }
    fn tool_schema_compiler(&self) -> Arc<dyn ProviderToolSchemaCompiler> {
        panic!("inspection must not compile Tool schemas")
    }
}
impl ModelTokenEstimator for NoExecution {
    fn estimate(&self, _: &ModelRequest) -> Result<u64, ContractError> {
        panic!("inspection must not estimate again")
    }
}
impl ComponentRuntime for NoExecution {
    fn resolve<'a>(
        &'a self,
        _: &'a ResolvedProfile,
        _: &'a ComponentResolveContext,
    ) -> PortFuture<'a, ResolvedAssembly> {
        panic!("inspection must not resolve adapter factories")
    }
    fn bind<'a>(
        &'a self,
        _: &'a ResolvedAssembly,
        _: &'a ComponentBindContext,
    ) -> PortFuture<'a, BoundCapabilities> {
        panic!("inspection must not open adapter factories")
    }
}
fn reader(
    profile: AgentProfile,
    mut bindings: AgentBindings,
    policy: Arc<InspectionPolicy>,
) -> Agent {
    bindings.policy = Arc::new(PolicyGate::new(policy, Duration::from_secs(2)).unwrap());
    bindings.model_exchange = Arc::new(ModelExchange::new(
        Arc::new(NoExecution),
        bindings.policy.clone(),
    ));
    bindings.token_estimator = Arc::new(NoExecution);
    if bindings.tools.is_none() && bindings.context_sources.is_none() {
        bindings.components = Some(Arc::new(NoExecution));
    }
    create_agent(profile, bindings).unwrap()
}
async fn reference(store: &dyn StateStore, run: &Id) -> (StepRef, PreparedStepRecord) {
    let saved = store.load(&scope(), run).await.unwrap();
    let record = store
        .read_record(&scope(), saved.snapshot.prepared_steps.last().unwrap())
        .await
        .unwrap();
    (
        StepRef::Prepared {
            record_id: record.reference().record_id.clone(),
        },
        serde_json::from_value(record.value().clone()).unwrap(),
    )
}
async fn report(agent: &Agent, run: &Id, step: StepRef, raw: bool) -> CompositionReport {
    completed(
        agent
            .inspect_step(
                run,
                step,
                &context(),
                InspectionOptions {
                    include_context_content: raw,
                },
            )
            .await
            .unwrap(),
    )
}

#[tokio::test]
async fn inspection_is_reproducible_preserves_fingerprints_and_has_no_execution_or_storage_writes()
{
    let fixture = Fixture::new(Response::Text, false);
    let mut profile = profile();
    profile.model_options = [("effort".into(), serde_json::json!("low"))].into();
    let running = create_agent(profile.clone(), fixture.bindings()).unwrap();
    let mut input = request("inspection");
    input.model_options = [("effort".into(), serde_json::json!("high"))].into();
    let handle = completed(running.start(input, context()).await.unwrap());
    completed(handle.outcome(&context()).await.unwrap());
    let (step, root) = reference(fixture.store.as_ref(), handle.run_id()).await;
    let reader = reader(
        profile,
        fixture.bindings(),
        Arc::new(InspectionPolicy::default()),
    );
    let before = fixture.store.export_checkpoint(&scope()).unwrap();
    let counters = (
        fixture.model.calls.load(Ordering::SeqCst),
        fixture.catalog.calls.load(Ordering::SeqCst),
        fixture.router.queries.load(Ordering::SeqCst),
    );
    let first = report(&reader, handle.run_id(), step.clone(), false).await;
    let second = report(
        &reader,
        handle.run_id(),
        StepRef::Logical {
            model_step_id: root.model_step_id.clone(),
            purpose: root.purpose,
            projection_revision: root.projection_revision,
        },
        false,
    )
    .await;
    assert_eq!(first.composition, second.composition);
    assert_eq!(first.status, InspectionStatus::Found);
    let composition = first.composition.as_ref().unwrap();
    assert_eq!(composition.fingerprint, root.projection_fingerprint);
    assert_eq!(composition.evidence, vec![InspectionEvidence::Prepared]);
    assert_eq!(
        composition
            .recorded_run_outcome
            .as_ref()
            .unwrap()
            .completion_basis,
        Some(CompletionBasis::TurnEnded)
    );
    assert_eq!(composition.attempts.len(), 1);
    assert!(
        composition.attempts[0]
            .evidence
            .contains(&InspectionEvidence::ResponseObserved)
    );
    let options = composition
        .model
        .as_ref()
        .unwrap()
        .configuration
        .as_ref()
        .unwrap();
    assert_eq!(
        options.effective.get("effort"),
        Some(&serde_json::json!("high"))
    );
    assert_eq!(options.sources.get("effort"), Some(&ModelOptionSource::Run));
    assert!(composition.estimated_input_tokens.unwrap() > 0);
    assert!(composition.estimator.is_none());
    assert!(
        first
            .unresolved
            .iter()
            .any(|field| field.field == "estimator" && field.reason == "not_recorded")
    );
    assert_eq!(
        fixture.store.export_checkpoint(&scope()).unwrap().digest(),
        before.digest()
    );
    assert_eq!(
        (
            fixture.model.calls.load(Ordering::SeqCst),
            fixture.catalog.calls.load(Ordering::SeqCst),
            fixture.router.queries.load(Ordering::SeqCst)
        ),
        counters
    );
}

#[tokio::test]
async fn tool_report_contains_model_schemas_but_no_bound_uuid_values_or_connection_details() {
    let mut fixture = resume_support::Fixture::new(resume_support::Mode::Approval);
    let mut registrations = vec![];
    for name in ["before", "target", "after"] {
        let registered = fixture.registry.get(&id(name)).unwrap();
        let mut descriptor = registered.compiled.descriptor().clone();
        if name == "target" {
            descriptor.description = "private-description-sentinel".into();
            descriptor.input_schema["properties"]["query"]["default"] =
                serde_json::json!("private-default-sentinel");
        }
        registrations.push(ToolRegistration {
            compiled: SchemaCompiler::new()
                .compile(descriptor, &fixture.inputs)
                .unwrap(),
            executor: registered.executor.clone(),
        });
    }
    fixture.registry = Arc::new(ToolRegistry::new(scope(), registrations).unwrap());
    let handle = fixture.started(&fixture.agent()).await;
    fixture.outcome(&handle).await;
    let (step, _) = reference(fixture.base.store.as_ref(), handle.run_id()).await;
    let agent = reader(
        fixture.profile.clone(),
        fixture.bindings(),
        Arc::new(InspectionPolicy::default()),
    );
    let calls = fixture.resolver.calls.load(Ordering::SeqCst);
    let result = report(&agent, handle.run_id(), step, true).await;
    let composition = result.composition.as_ref().unwrap();
    assert_eq!(composition.tools.len(), 3);
    let target = composition
        .tools
        .iter()
        .find(|tool| tool.canonical_name == id("target"))
        .unwrap();
    assert!(target.canonical_schema["properties"].get("query").is_some());
    assert!(
        target.canonical_schema["properties"]
            .get("workspace_id")
            .is_none()
    );
    assert!(target.provider_schema.is_some());
    assert!(target.decode_plan_digest.is_some());
    assert!(
        composition
            .redacted_paths
            .iter()
            .any(|path| path.ends_with("query.default"))
    );
    assert!(target.canonical_schema["properties"]["query"]["default"].is_null());
    let encoded = serde_json::to_string(&result).unwrap();
    for private in [
        resume_support::WORKSPACE,
        resume_support::RECORD,
        "execution_args",
        "system_bindings",
        "connection_ref",
        "private-description-sentinel",
        "private-default-sentinel",
    ] {
        assert!(!encoded.contains(private), "disclosed {private}");
    }
    assert_eq!(fixture.resolver.calls.load(Ordering::SeqCst), calls);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn preparation_only_and_an_unconfirmed_reserved_attempt_have_distinct_evidence() {
    for reserved in [false, true] {
        let fixture = Fixture::new(Response::Text, false);
        let mut bindings = fixture.bindings();
        if reserved {
            bindings.state = Arc::new(FinalCommitStore::new(
                fixture.store.clone(),
                FinalCommitMode::RejectModelResult,
            ));
        } else {
            fixture.policy.deny.store(10, Ordering::SeqCst);
        }
        let owner = create_agent(profile(), bindings).unwrap();
        let handle = fixture.started(&owner, "evidence").await;
        let _ = handle.outcome(&context()).await;
        let (step, _) = reference(fixture.store.as_ref(), handle.run_id()).await;
        let agent = reader(
            profile(),
            fixture.bindings(),
            Arc::new(InspectionPolicy::default()),
        );
        let result = report(&agent, handle.run_id(), step, false).await;
        let composition = result.composition.unwrap();
        assert_eq!(composition.evidence, vec![InspectionEvidence::Prepared]);
        assert_eq!(composition.attempts.len(), usize::from(reserved));
        if reserved {
            assert_eq!(
                composition.attempts[0].evidence,
                vec![
                    InspectionEvidence::DispatchReserved,
                    InspectionEvidence::TransmissionUnknown
                ]
            );
            assert_ne!(composition.recorded_run_status, RunStatus::Succeeded);
        }
        assert_eq!(
            fixture.model.calls.load(Ordering::SeqCst),
            usize::from(reserved)
        );
    }
}

#[tokio::test]
async fn missing_and_expired_records_remain_structured_gaps_without_repreparation() {
    let fixture = Fixture::new(Response::Text, false);
    let handle = fixture.started(&fixture.agent(), "retention").await;
    completed(handle.outcome(&context()).await.unwrap());
    let (step, root) = reference(fixture.store.as_ref(), handle.run_id()).await;
    let StepRef::Prepared { record_id } = &step else {
        unreachable!()
    };
    let store = Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::PassThrough,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    let agent = reader(profile(), bindings, Arc::new(InspectionPolicy::default()));
    for (code, status) in [
        (ErrorCode::StateNotFound, InspectionStatus::NotFound),
        (ErrorCode::RecordExpired, InspectionStatus::Expired),
    ] {
        *store.record_fault.lock().unwrap() = Some((record_id.clone(), code));
        let result = report(&agent, handle.run_id(), step.clone(), false).await;
        assert_eq!(result.status, status);
        assert!(result.composition.is_none());
    }
    *store.record_fault.lock().unwrap() =
        Some((root.context_projection.record_id, ErrorCode::RecordExpired));
    let partial = report(&agent, handle.run_id(), step, false).await;
    assert_eq!(partial.status, InspectionStatus::Partial);
    assert!(
        partial
            .composition
            .unwrap()
            .estimated_input_tokens
            .is_none()
    );
    assert!(
        partial
            .unresolved
            .iter()
            .any(|field| field.field == "projection" && field.reason == "expired")
    );
    *store.record_fault.lock().unwrap() = None;
    assert_eq!(
        report(
            &agent,
            handle.run_id(),
            StepRef::Prepared {
                record_id: id("unknown-record")
            },
            false
        )
        .await
        .status,
        InspectionStatus::NotFound
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn approval_and_denial_do_not_read_the_store_or_create_a_wait_and_late_revocation_is_respected()
 {
    let fixture = Fixture::new(Response::Text, false);
    let handle = fixture.started(&fixture.agent(), "authorization").await;
    completed(handle.outcome(&context()).await.unwrap());
    let (step, _) = reference(fixture.store.as_ref(), handle.run_id()).await;
    let store = Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::PassThrough,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    let policy = Arc::new(InspectionPolicy::default());
    let agent = reader(profile(), bindings, policy.clone());
    let before = fixture.store.export_checkpoint(&scope()).unwrap();
    policy.mode.store(2, Ordering::SeqCst);
    assert!(matches!(
        agent
            .inspect_step(
                handle.run_id(),
                step.clone(),
                &context(),
                Default::default()
            )
            .await
            .unwrap(),
        Guarded::ApprovalRequired(_)
    ));
    policy.mode.store(1, Ordering::SeqCst);
    assert_eq!(
        agent
            .inspect_step(
                handle.run_id(),
                step.clone(),
                &context(),
                Default::default()
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(store.load_calls.load(Ordering::SeqCst), 0);
    policy.mode.store(0, Ordering::SeqCst);
    store.block_read.store(5, Ordering::SeqCst);
    let run = handle.run_id().clone();
    let querying = tokio::spawn(async move {
        agent
            .inspect_step(&run, step, &context(), Default::default())
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), store.read_entered.notified())
        .await
        .unwrap();
    policy.mode.store(1, Ordering::SeqCst);
    store.release.add_permits(1);
    assert_eq!(
        querying.await.unwrap().unwrap_err().code,
        ErrorCode::AccessDenied
    );
    assert_eq!(
        fixture.store.export_checkpoint(&scope()).unwrap().digest(),
        before.digest()
    );
}

#[tokio::test]
async fn fragment_content_requires_opt_in_and_current_permission_without_source_callbacks() {
    let mut fixture = source_support::Fixture::new();
    let source = fixture.add(
        "search",
        ContextTrigger::RunStart,
        true,
        vec![source_support::Reply::Ready],
    );
    let handle = fixture.start(&fixture.agent()).await;
    fixture.outcome(&handle).await;
    let (step, root) = reference(fixture.store.inner.as_ref(), handle.run_id()).await;
    let policy = Arc::new(InspectionPolicy::default());
    let agent = reader(fixture.profile(), fixture.agent_bindings(), policy.clone());
    let calls = (
        source.calls.load(Ordering::SeqCst),
        source.uses.load(Ordering::SeqCst),
        fixture.estimator.calls.load(Ordering::SeqCst),
        fixture.model.inner.calls.load(Ordering::SeqCst),
    );
    let hidden = report(&agent, handle.run_id(), step.clone(), false).await;
    assert!(hidden.composition.as_ref().unwrap().fragments.iter().any(|fragment| matches!(&fragment.disclosure, ContextDisclosure::Redacted { reason } if reason == "not_requested")));
    let shown = report(&agent, handle.run_id(), step.clone(), true).await;
    assert!(
        shown
            .composition
            .as_ref()
            .unwrap()
            .fragments
            .iter()
            .any(|fragment| matches!(fragment.disclosure, ContextDisclosure::Included { .. }))
    );
    policy.content.store(1, Ordering::SeqCst);
    let denied = report(&agent, handle.run_id(), step, true).await;
    assert!(
        denied
            .composition
            .as_ref()
            .unwrap()
            .fragments
            .iter()
            .all(|fragment| !matches!(fragment.disclosure, ContextDisclosure::Included { .. }))
    );
    assert_eq!(
        denied.composition.unwrap().fingerprint,
        root.projection_fingerprint
    );
    assert_eq!(
        (
            source.calls.load(Ordering::SeqCst),
            source.uses.load(Ordering::SeqCst),
            fixture.estimator.calls.load(Ordering::SeqCst),
            fixture.model.inner.calls.load(Ordering::SeqCst)
        ),
        calls
    );
}

struct EmptyResponse {
    binding: ModelPortBinding,
    empty: bool,
}
impl ModelPort for EmptyResponse {
    fn binding(&self) -> ModelPortBinding {
        self.binding.clone()
    }
    fn generate<'a>(
        &'a self,
        _: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        if self.empty {
            Box::pin(futures_util::stream::empty())
        } else {
            Box::pin(futures_util::stream::once(async {
                Err(ContractError::new(
                    ErrorCode::ModelUnavailable,
                    "local.transport",
                ))
            }))
        }
    }
}
#[tokio::test]
async fn local_empty_stream_or_transport_failure_is_not_proof_of_a_provider_response() {
    for empty in [false, true] {
        let fixture = Fixture::new(Response::TransportFailure, false);
        let mut bindings = fixture.bindings();
        bindings.model_exchange = Arc::new(
            ModelExchange::new(
                Arc::new(EmptyResponse {
                    binding: fixture.model.binding(),
                    empty,
                }),
                bindings.policy.clone(),
            )
            .with_route_inspector(fixture.inspector.clone(), Duration::from_secs(1))
            .unwrap(),
        );
        let owner = create_agent(profile(), bindings).unwrap();
        let handle = fixture.started(&owner, "local-failure").await;
        assert_eq!(
            completed(handle.outcome(&context()).await.unwrap())
                .result
                .status(),
            RunStatus::Failed
        );
        let (step, _) = reference(fixture.store.as_ref(), handle.run_id()).await;
        let reader = reader(
            profile(),
            fixture.bindings(),
            Arc::new(InspectionPolicy::default()),
        );
        let report = report(&reader, handle.run_id(), step, false).await;
        let attempt = &report.composition.unwrap().attempts[0];
        assert!(attempt.result_recorded);
        assert!(
            attempt
                .evidence
                .contains(&InspectionEvidence::TransmissionUnknown)
        );
        assert!(
            !attempt
                .evidence
                .contains(&InspectionEvidence::ResponseObserved)
        );
    }
}

#[tokio::test]
async fn raw_fragment_set_is_reauthorized_after_later_fragment_checks_revoke_earlier_access() {
    let mut fixture = source_support::Fixture::new();
    fixture.add(
        "first",
        ContextTrigger::RunStart,
        true,
        vec![source_support::Reply::Ready],
    );
    fixture.add(
        "second",
        ContextTrigger::RunStart,
        true,
        vec![source_support::Reply::Ready],
    );
    let handle = fixture.start(&fixture.agent()).await;
    fixture.outcome(&handle).await;
    let (step, _) = reference(fixture.store.inner.as_ref(), handle.run_id()).await;
    let policy = Arc::new(InspectionPolicy::default());
    policy.pause_fragment.store(2, Ordering::SeqCst);
    let agent = reader(fixture.profile(), fixture.agent_bindings(), policy.clone());
    let run = handle.run_id().clone();
    let pending = tokio::spawn(async move {
        agent
            .inspect_step(
                &run,
                step,
                &context(),
                InspectionOptions {
                    include_context_content: true,
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), policy.fragment_entered.notified())
        .await
        .unwrap();
    policy.content.store(1, Ordering::SeqCst);
    policy.fragment_release.add_permits(1);
    assert_eq!(
        pending.await.unwrap().unwrap_err().code,
        ErrorCode::AccessDenied
    );
}

#[tokio::test]
async fn a_step_must_belong_to_the_requested_run_and_scope_and_opaque_data_is_never_disclosed() {
    let fixture = Fixture::new(Response::WithContinuation, false);
    let owner = fixture.agent();
    let first = fixture.started(&owner, "first").await;
    completed(first.outcome(&context()).await.unwrap());
    let (step, _) = reference(fixture.store.as_ref(), first.run_id()).await;
    let first_saved = fixture.store.load(&scope(), first.run_id()).await.unwrap();
    let response_ref = first_saved.snapshot.model_ledger[0]
        .response_ref
        .as_ref()
        .unwrap();
    let protected = fixture
        .store
        .read_record(&scope(), response_ref)
        .await
        .unwrap();
    assert!(protected.value().to_string().contains("fixture-signature"));
    let second = fixture.started(&owner, "second").await;
    completed(second.outcome(&context()).await.unwrap());
    let agent = reader(
        profile(),
        fixture.bindings(),
        Arc::new(InspectionPolicy::default()),
    );
    let visible = report(&agent, first.run_id(), step.clone(), true).await;
    assert!(
        !serde_json::to_string(&visible)
            .unwrap()
            .contains("fixture-signature")
    );
    assert_eq!(
        report(&agent, second.run_id(), step.clone(), false)
            .await
            .status,
        InspectionStatus::NotFound
    );
    let mut foreign = context();
    foreign.data.scope.workspace_id = id("foreign");
    assert_eq!(
        agent
            .inspect_step(first.run_id(), step, &foreign, Default::default())
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}
```

## `crates/wickle/tests/support/agent.rs`

```rust
//! Deterministic Host components for agent runtime lifecycle tests.

use futures_util::{StreamExt, stream};
use serde_json::json;
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};
use wickle::*;

pub fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
pub fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    }
}
pub fn reference(name: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id("1"),
    }
}
pub fn profile() -> AgentProfile {
    AgentProfile::from_json(r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
        "name":"Assistant","description":"Runtime fixture","instructions":{"text":"Use supplied records"},
        "model_binding":"primary","tools":[],"skills":[],"connectors":[],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":4,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
    }"#).unwrap()
}
pub fn request(name: &str) -> RunRequest {
    RunRequest {
        request_id: id(name),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Retrieve the requested information".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        max_output_tokens: None,
        output_contract: None,
    }
}
pub fn context() -> ExecutionContext {
    ExecutionContext::new(
        ExecutionContextData {
            scope: scope(),
            principal_ref: id("caller"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    )
}
pub fn completed<T>(result: Guarded<T>) -> T {
    match result {
        Guarded::Completed(value) => value,
        Guarded::ApprovalRequired(_) => panic!("unexpected approval"),
    }
}

pub struct TestClock {
    origin: tokio::time::Instant,
}
impl TestClock {
    pub fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
        }
    }
}
impl Clock for TestClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        let elapsed = self.origin.elapsed().as_millis() as u64;
        Ok(ClockReading {
            utc_ms: 1000 + elapsed as i64,
            monotonic_ms: elapsed,
        })
    }
    fn sleep_until<'a>(&'a self, deadline: u64) -> PortFuture<'a, ()> {
        Box::pin(async move {
            tokio::time::sleep_until(self.origin + Duration::from_millis(deadline)).await;
            Ok(())
        })
    }
}
#[derive(Default)]
pub struct Ids(pub AtomicUsize);
impl IdSource for Ids {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id(&format!("id-{}", self.0.fetch_add(1, Ordering::SeqCst))))
    }
}

#[derive(Default)]
pub struct Catalog {
    pub calls: AtomicUsize,
    pub revision: AtomicUsize,
}
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id(&format!(
                        "revision-{}",
                        self.revision.load(Ordering::SeqCst)
                    ))),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!(self.revision.load(Ordering::SeqCst))),
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

#[derive(Default)]
pub struct Policy {
    pub calls: AtomicUsize,
    pub deny: AtomicUsize,
    pub model_checks: AtomicUsize,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let deny = match self.deny.load(Ordering::SeqCst) {
                1 => matches!(
                    request.action,
                    PolicyAction::ReadRun {}
                        | PolicyAction::ReadRunDetails {}
                        | PolicyAction::ReadEvents {}
                ),
                2 => matches!(request.action, PolicyAction::CancelRun {}),
                3 => matches!(request.action, PolicyAction::StartRun {}),
                4 => matches!(request.action, PolicyAction::ResumeRun { .. }),
                10 => {
                    matches!(request.action, PolicyAction::InvokeModel { .. })
                        && self.model_checks.fetch_add(1, Ordering::SeqCst) > 0
                }
                _ => false,
            };
            Ok(if deny {
                PolicyDecision::Deny {
                    reason: id("denied"),
                }
            } else {
                PolicyDecision::Allow {}
            })
        })
    }
}

pub struct Router {
    pub snapshot: RoutingSnapshot,
    pub queries: AtomicUsize,
    pub snapshots: AtomicUsize,
}
impl Router {
    pub fn new() -> Self {
        Self::for_provider("fixture")
    }
    pub fn for_provider(provider: &str) -> Self {
        let capabilities = ModelCapabilities {
            revision: id("capabilities"),
            features: [id("text")].into_iter().collect(),
            options_schema: json!({"type":"object","properties":{"effort":{"enum":["low","high"]}},"additionalProperties":false}),
            context_window: 8192.try_into().unwrap(),
            max_output_tokens: 2048.try_into().unwrap(),
        };
        let model = ModelDefinition {
            model_key: id("model"),
            family: id("fixture"),
            provider: id(provider),
            model_id: id("fixture-model"),
            model_version: id("release"),
            version_semantics: VersionSemantics::Pinned,
            lifecycle: ModelLifecycle::Active,
            capabilities: capabilities.clone(),
            evidence: vec![],
        };
        let mut binding = ModelBinding {
            default_options: Default::default(),
            binding: reference("route"),
            model: model.reference(),
            requested_model: model.model_id.clone(),
            adapter: reference("adapter"),
            connection_ref: reference("connection"),
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
            binding_digest: binding.contract_digest(&model).unwrap(),
            checked_at_ms: 1000,
            evidence_ref: id("fixture-proof"),
            passed: true,
        });
        let snapshot = RoutingSnapshot::new(
            ModelCatalogSnapshot {
                revision: id("catalog"),
                scope: scope(),
                models: vec![model],
                bindings: vec![binding],
                aliases: vec![],
            },
            RoutingPolicy {
                revision: id("policy"),
                scope: scope(),
                rules: vec![RoutingRule {
                    model_binding: id("primary"),
                    purpose: ModelPurpose::Agent,
                    primary: reference("route"),
                    fallbacks: vec![],
                    fallback_on: vec![],
                    version_policy: VersionPolicy::RequirePinned,
                    min_support: ModelSupportStatus::ContractTested,
                }],
            },
        )
        .unwrap();
        Self {
            snapshot,
            queries: AtomicUsize::new(0),
            snapshots: AtomicUsize::new(0),
        }
    }
}
impl ModelRouter for Router {
    fn snapshot(&self) -> &RoutingSnapshot {
        self.snapshots.fetch_add(1, Ordering::SeqCst);
        &self.snapshot
    }
    fn resolve<'a>(&'a self, request: &'a RouteRequest) -> PortFuture<'a, RouteSelection> {
        Box::pin(async move {
            self.queries.fetch_add(1, Ordering::SeqCst);
            let selected = RouteSelection {
                route: self.snapshot.route_for_binding(&reference("route"))?,
                reason: if request.previous_route.is_some() {
                    RouteSelectionReason::Reuse
                } else {
                    RouteSelectionReason::Initial
                },
                candidate_index: 0,
                routing_snapshot_digest: self.snapshot.digest(),
                request_digest: request.digest(),
            };
            self.snapshot.validate_selection(request, &selected)?;
            Ok(selected)
        })
    }
}

pub struct Inspector {
    pub calls: AtomicUsize,
}
impl ModelRouteInspector for Inspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        _: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: ModelRouteAvailability::Available,
                model_id: Some(id("fixture-model")),
                model_version: Some(id("release")),
                deployment_revision: None,
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id("synthetic-inspection"),
            })
        })
    }
}
pub struct Estimator {
    pub calls: AtomicUsize,
    pub tokens: AtomicUsize,
}
impl ModelTokenEstimator for Estimator {
    fn estimate(&self, _: &ModelRequest) -> Result<u64, ContractError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.tokens.load(Ordering::SeqCst) as u64)
    }
}

#[derive(Clone, Copy)]
pub enum Response {
    Text,
    WithContinuation,
    TransportFailure,
    Truncated,
    WaitAfterText,
    Panic,
    Tool,
}
pub struct Model {
    pub calls: AtomicUsize,
    pub entered: Notify,
    pub release: Semaphore,
    pub response: Response,
    pub requests: Mutex<Vec<ModelRequest>>,
    pub gated: bool,
    pub port_binding: ModelPortBinding,
}
impl Model {
    pub fn new(response: Response, gated: bool) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Semaphore::new(0),
            response,
            requests: Mutex::new(vec![]),
            gated,
            port_binding: ModelPortBinding {
                provider: id("fixture"),
                adapter: reference("adapter"),
                connection_ref: reference("connection"),
            },
        }
    }
}
impl ModelPort for Model {
    fn binding(&self) -> ModelPortBinding {
        self.port_binding.clone()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        self.entered.notify_one();
        Box::pin(
            stream::once(async move {
                if self.gated {
                    self.release.acquire().await.unwrap().forget();
                }
                if matches!(self.response, Response::Panic) {
                    panic!("synthetic adapter panic");
                }
                let mut events = vec![Ok(ModelEvent::TextDelta {
                    text: "candidate answer".into(),
                })];
                match self.response {
                    Response::Text | Response::Panic | Response::WithContinuation => {
                        events.push(Ok(ModelEvent::ResponseCompleted {
                            finish: ModelFinish::Stop,
                            metadata: ModelResponseMetadata::default(),
                            continuation: if matches!(self.response, Response::WithContinuation) {
                                vec![OpaqueContinuation::new(
                                    &request.route,
                                    json!({"signature":"fixture-signature"}),
                                )]
                            } else {
                                vec![]
                            },
                        }))
                    }
                    Response::TransportFailure => events.push(Ok(ModelEvent::ResponseError {
                        kind: ModelFailureKind::Transport,
                        metadata: ModelResponseMetadata::default(),
                    })),
                    Response::Truncated => events.push(Ok(ModelEvent::ResponseCompleted {
                        finish: ModelFinish::Length,
                        metadata: ModelResponseMetadata::default(),
                        continuation: vec![],
                    })),
                    Response::Tool => {
                        events.push(Ok(ModelEvent::ToolArgumentsDelta {
                            index: 0,
                            provider_call_id: Some("call".into()),
                            name: Some("unregistered".into()),
                            delta: "{}".into(),
                        }));
                        events.push(Ok(ModelEvent::ResponseCompleted {
                            finish: ModelFinish::ToolCalls,
                            metadata: ModelResponseMetadata::default(),
                            continuation: vec![],
                        }));
                    }
                    Response::WaitAfterText => {}
                }
                let trailing = if matches!(self.response, Response::WaitAfterText) {
                    stream::pending().boxed()
                } else {
                    stream::empty().boxed()
                };
                stream::iter(events).chain(trailing)
            })
            .flatten(),
        )
    }
}

pub struct Fixture {
    pub store: Arc<MemoryStateStore>,
    pub policy: Arc<Policy>,
    pub catalog: Arc<Catalog>,
    pub router: Arc<Router>,
    pub inspector: Arc<Inspector>,
    pub estimator: Arc<Estimator>,
    pub model: Arc<Model>,
    pub clock: Arc<TestClock>,
    pub ids: Arc<Ids>,
}

#[derive(Clone, Copy)]
pub enum FinalCommitMode {
    RejectModelResult,
    RejectRecoveryLease,
    RejectRecoveryAcceptance,
    LoseRecoveryAcknowledgement,
    RejectCandidate,
    OmitVerificationEvent,
    OmitInterruptionEvent,
    RejectVerification,
    LoseVerificationAcknowledgement,
    PassThrough,
    Reject,
    LoseAcknowledgement,
    Pause,
    PauseEmptyEventPage,
    RejectContext,
    LoseContextAcknowledgement,
}
pub struct FinalCommitStore {
    pub inner: Arc<MemoryStateStore>,
    pub mode: FinalCommitMode,
    pub final_entered: Notify,
    pub release: Semaphore,
    pub final_attempts: AtomicUsize,
    pub release_calls: AtomicUsize,
    pub context_attempts: AtomicUsize,
    pub empty_page_entered: Notify,
    pub empty_page_release: Semaphore,
    paused_empty_page: AtomicBool,
    pub block_read: AtomicUsize,
    pub read_entered: Notify,
    pub record_fault: Mutex<Option<(Id, ErrorCode)>>,
    pub load_calls: AtomicUsize,
}
impl FinalCommitStore {
    pub fn new(inner: Arc<MemoryStateStore>, mode: FinalCommitMode) -> Self {
        Self {
            inner,
            mode,
            final_entered: Notify::new(),
            release: Semaphore::new(0),
            final_attempts: AtomicUsize::new(0),
            release_calls: AtomicUsize::new(0),
            context_attempts: AtomicUsize::new(0),
            empty_page_entered: Notify::new(),
            empty_page_release: Semaphore::new(0),
            paused_empty_page: AtomicBool::new(false),
            block_read: AtomicUsize::new(0),
            read_entered: Notify::new(),
            record_fault: Mutex::new(None),
            load_calls: AtomicUsize::new(0),
        }
    }
}
impl StateStore for FinalCommitStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        s: &'a Scope,
        session: &'a Id,
        request: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        Box::pin(async move {
            if self
                .block_read
                .compare_exchange(3, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.read_entered.notify_one();
                std::future::pending::<()>().await;
            }
            self.inner.find_request(s, session, request).await
        })
    }
    fn admit<'a>(&'a self, s: &'a Scope, input: AdmissionInput) -> PortFuture<'a, AdmissionResult> {
        self.inner.admit(s, input)
    }
    fn load<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            self.load_calls.fetch_add(1, Ordering::SeqCst);
            if self.block_read.load(Ordering::SeqCst) == 4 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "load.offline",
                ));
            }

            if self
                .block_read
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.read_entered.notify_one();
                std::future::pending::<()>().await;
            }
            self.inner.load(s, r).await
        })
    }
    fn load_session<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, SessionSnapshot> {
        self.inner.load_session(s, r)
    }
    fn check_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.check_lease(s, r, l, n)
    }
    fn acquire_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        o: &'a Id,
        n: i64,
        t: u64,
    ) -> PortFuture<'a, RunLease> {
        if matches!(self.mode, FinalCommitMode::RejectRecoveryLease) {
            return Box::pin(async {
                Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "lease.offline",
                ))
            });
        }
        self.inner.acquire_lease(s, r, o, n, t)
    }
    fn renew_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
        t: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.renew_lease(s, r, l, n, t)
    }
    fn release_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, ()> {
        self.release_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.release_lease(s, r, l, n)
    }
    fn read_events<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        after: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage> {
        Box::pin(async move {
            if self
                .block_read
                .compare_exchange(2, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.read_entered.notify_one();
                std::future::pending::<()>().await;
            }
            let page = self.inner.read_events(s, r, after, limit).await?;
            if matches!(self.mode, FinalCommitMode::PauseEmptyEventPage)
                && page.events.is_empty()
                && !self.paused_empty_page.swap(true, Ordering::SeqCst)
            {
                self.empty_page_entered.notify_one();
                self.empty_page_release.acquire().await.unwrap().forget();
            }
            Ok(page)
        })
    }
    fn read_record<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        Box::pin(async move {
            if let Some((id, code)) = self.record_fault.lock().unwrap().as_ref() {
                if *id == r.record_id {
                    return Err(ContractError::new(*code, "record.retention"));
                }
            }
            let record = self.inner.read_record(s, r).await?;
            if self
                .block_read
                .compare_exchange(5, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.read_entered.notify_one();
                self.release.acquire().await.unwrap().forget();
            }
            Ok(record)
        })
    }
    fn commit<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            let mut input = input;
            if matches!(self.mode, FinalCommitMode::OmitInterruptionEvent)
                && input.snapshot.status == RunStatus::Interrupted
            {
                input.events.retain(|event| {
                    !matches!(event.payload, RunEventPayload::RunInterrupted { .. })
                });
                input.snapshot.last_event_seq -= 1;
            }
            if matches!(self.mode, FinalCommitMode::RejectModelResult)
                && input
                    .snapshot
                    .model_ledger
                    .iter()
                    .any(|entry| entry.response_ref.is_some())
            {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "model.result.commit",
                ));
            }
            if matches!(self.mode, FinalCommitMode::RejectCandidate)
                && self
                    .inner
                    .load(s, r)
                    .await?
                    .snapshot
                    .candidate_ref
                    .is_none()
                && input.snapshot.candidate_ref.is_some()
            {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "candidate.commit",
                ));
            }
            if matches!(self.mode, FinalCommitMode::OmitVerificationEvent) {
                let before = input.events.len();
                input.events.retain(|event| {
                    !matches!(event.payload, RunEventPayload::VerificationCompleted { .. })
                });
                input.snapshot.last_event_seq -= (before - input.events.len()) as u64;
            }
            if matches!(
                self.mode,
                FinalCommitMode::RejectContext | FinalCommitMode::LoseContextAcknowledgement
            ) && self.inner.load(s, r).await?.snapshot.context_revision_ref
                != input.snapshot.context_revision_ref
            {
                self.context_attempts.fetch_add(1, Ordering::SeqCst);
                if matches!(self.mode, FinalCommitMode::LoseContextAcknowledgement) {
                    self.inner.commit(s, r, input).await?;
                }
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "context.commit",
                ));
            }
            if matches!(
                self.mode,
                FinalCommitMode::RejectVerification
                    | FinalCommitMode::LoseVerificationAcknowledgement
            ) && self.inner.load(s, r).await?.snapshot.verification_records
                != input.snapshot.verification_records
            {
                if matches!(self.mode, FinalCommitMode::LoseVerificationAcknowledgement) {
                    self.inner.commit(s, r, input).await?;
                }
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "verification.commit",
                ));
            }
            if !input.snapshot.status.is_terminal()
                && input.snapshot.status != RunStatus::Interrupted
            {
                return self.inner.commit(s, r, input).await;
            }
            self.final_attempts.fetch_add(1, Ordering::SeqCst);
            self.final_entered.notify_one();
            match self.mode {
                FinalCommitMode::Reject => Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "final.commit",
                )),
                FinalCommitMode::LoseAcknowledgement => {
                    self.inner.commit(s, r, input).await?;
                    Err(ContractError::new(
                        ErrorCode::PersistenceUnavailable,
                        "final.ack",
                    ))
                }
                FinalCommitMode::Pause => {
                    self.release.acquire().await.unwrap().forget();
                    self.inner.commit(s, r, input).await
                }
                FinalCommitMode::PauseEmptyEventPage
                | FinalCommitMode::LoseRecoveryAcknowledgement
                | FinalCommitMode::RejectRecoveryLease
                | FinalCommitMode::RejectRecoveryAcceptance
                | FinalCommitMode::RejectModelResult
                | FinalCommitMode::RejectCandidate
                | FinalCommitMode::OmitVerificationEvent
                | FinalCommitMode::OmitInterruptionEvent
                | FinalCommitMode::RejectVerification
                | FinalCommitMode::LoseVerificationAcknowledgement
                | FinalCommitMode::PassThrough
                | FinalCommitMode::RejectContext
                | FinalCommitMode::LoseContextAcknowledgement => {
                    self.inner.commit(s, r, input).await
                }
            }
        })
    }
}
impl Fixture {
    pub fn new(response: Response, gated: bool) -> Self {
        Self {
            store: Arc::new(MemoryStateStore::new()),
            policy: Arc::new(Policy::default()),
            catalog: Arc::new(Catalog::default()),
            router: Arc::new(Router::new()),
            inspector: Arc::new(Inspector {
                calls: AtomicUsize::new(0),
            }),
            estimator: Arc::new(Estimator {
                calls: AtomicUsize::new(0),
                tokens: AtomicUsize::new(32),
            }),
            model: Arc::new(Model::new(response, gated)),
            clock: Arc::new(TestClock::new()),
            ids: Arc::new(Ids::default()),
        }
    }
    pub fn bindings(&self) -> AgentBindings {
        let gate = Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap());
        AgentBindings {
            interruption_policy: None,
            scope: scope(),
            state: self.store.clone(),
            policy: gate.clone(),
            profile_resolver: self.catalog.clone(),
            model_exchange: Arc::new(
                ModelExchange::new(self.model.clone(), gate)
                    .with_route_inspector(self.inspector.clone(), Duration::from_secs(1))
                    .unwrap(),
            ),
            router: self.router.clone(),
            host_instructions: vec!["Trusted host rules".into()],
            system_inputs: SystemInputRegistry::new(vec![]).unwrap(),
            clock: self.clock.clone(),
            ids: self.ids.clone(),
            tools: None,
            system_input_resolver: None,
            external_receipt_verifier: None,
            components: None,
            context_sources: None,
            context_token_estimator: None,
            context_runtime: None,
            verification: None,
            skills: None,
            artifacts: None,
            hooks: None,
            token_estimator: self.estimator.clone(),
            settings: AgentSettings {
                observer_poll_ms: 1,
                heartbeat_interval_ms: 100,
                lease_ttl_ms: 1000,
                max_output_tokens: 128.try_into().unwrap(),
                ..AgentSettings::default()
            },
        }
    }
    pub fn agent(&self) -> Agent {
        create_agent(profile(), self.bindings()).unwrap()
    }
    pub async fn started(&self, agent: &Agent, name: &str) -> RunHandle {
        completed(agent.start(request(name), context()).await.unwrap())
    }
}

impl wickle::ExecutionTransactions for FinalCommitStore {
    fn read_execution<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
    ) -> wickle::PortFuture<'a, wickle::ExecutionHistory> {
        self.inner.read_execution(scope, run_id)
    }
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
        command: wickle::ControlCommand,
    ) -> wickle::PortFuture<'a, wickle::ControlReceipt> {
        self.inner.submit_control_command(scope, run_id, command)
    }
    fn begin_segment<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        request: wickle::BeginSegmentRequest,
    ) -> wickle::PortFuture<'a, wickle::BeginSegmentResult> {
        Box::pin(async move {
            let recovery = matches!(&request.start, SegmentStart::Resume(command) if matches!(command.action, ResumeAction::Recover { .. }));
            if recovery
                && matches!(
                    self.mode,
                    FinalCommitMode::RejectRecoveryLease
                        | FinalCommitMode::RejectRecoveryAcceptance
                )
            {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "segment.offline",
                ));
            }
            let result = self.inner.begin_segment(scope, request).await?;
            if recovery && matches!(self.mode, FinalCommitMode::LoseRecoveryAcknowledgement) {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "segment.ack",
                ));
            }
            Ok(result)
        })
    }
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
        atomic::{AtomicBool, AtomicUsize, Ordering},
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
            default_options: Default::default(),
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
    hold: AtomicBool,
    entered: tokio::sync::Notify,
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
        if self.hold.load(Ordering::SeqCst) {
            self.entered.notify_one();
            return Box::pin(stream::pending());
        }
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
        hold: AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
        route: snapshot.route_for_binding(&reference("first"))?,
        calls: AtomicUsize::new(0),
        fail: true,
    });
    let second = Arc::new(ExampleModel {
        hold: AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
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
            interruption_policy: None,
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
    let mut changed = request.clone();
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
    // Exercise a cooperative stop against the packaged library and SQLite store.
    first.hold.store(true, Ordering::SeqCst);
    let stopped_request = RunRequest {
        request_id: id("maintenance-request"),
        session_id: id("maintenance-session"),
        ..request
    };
    let stopped = completed(agent.start(stopped_request.clone(), context.clone()).await?)?;
    tokio::time::timeout(Duration::from_secs(5), first.entered.notified()).await?;
    assert_eq!(completed(stopped.stop_execution(InterruptionCause::HostShutdown, &context).await?)?, ExecutionStopReceipt::Requested);
    let stopped_outcome = completed(tokio::time::timeout(Duration::from_secs(5), stopped.outcome(&context)).await??)?;
    assert!(matches!(&stopped_outcome.result, OutcomeResult::Interrupted { interruption }
        if interruption.cause == InterruptionCause::HostShutdown && interruption.recoverable));
    let reopened = SqliteStateStore::open(&database)?;
    let saved = reopened.load(&scope, stopped.run_id()).await?;
    assert_eq!(saved.snapshot.outcome, Some(stopped_outcome.clone()));
    assert_eq!(saved.session.active_run_id.as_ref(), Some(stopped.run_id()));
    let replay = completed(agent.start(stopped_request.clone(), context.clone()).await?)?;
    assert_eq!(completed(replay.outcome(&context).await?)?, stopped_outcome);
    assert_eq!(first.calls.load(Ordering::SeqCst), 2);
    let cancellation = ControlCommand { command_id: id("cancel-maintenance"), principal_ref: context.data.principal_ref.clone(), action: ControlAction::Cancel { reason: id("withdrawn") } };
    let receipt = completed(agent.submit_control_command(stopped.run_id().clone(), cancellation.clone(), context.clone()).await?)?;
    assert!(receipt.processed_segment_id.is_some());
    assert_eq!(completed(stopped.outcome(&context).await?)?, stopped_outcome);
    let latest = completed(agent.start(stopped_request, context.clone()).await?)?;
    assert_eq!(Some(latest.segment_id()), receipt.processed_segment_id.as_ref());
    assert_eq!(completed(latest.outcome(&context).await?)?.result.status(), RunStatus::Cancelled);
    let before_read = SqliteStateStore::open(&database)?.load(&scope, stopped.run_id()).await?;
    assert_eq!(completed(agent.get_control_receipt(stopped.run_id(), &cancellation.command_id, &context).await?)?, receipt);
    assert_eq!(SqliteStateStore::open(&database)?.load(&scope, stopped.run_id()).await?, before_read);
    assert_eq!(completed(agent.submit_control_command(stopped.run_id().clone(), cancellation, context.clone()).await?)?, receipt);
    assert_eq!(first.calls.load(Ordering::SeqCst), 2);
    let prepared_id = restored.snapshot.prepared_steps.last().ok_or("missing saved preparation")?.record_id.clone();
    let inspection = completed(agent.inspect_step(&run_id, StepRef::Prepared { record_id: prepared_id }, &context, InspectionOptions::default()).await?)?;
    assert_eq!(inspection.status, InspectionStatus::Found);
    let composition = inspection.composition.as_ref().ok_or("missing composition")?;
    assert_eq!(composition.recorded_run_revision, restored.snapshot.revision);
    assert_eq!(composition.recorded_run_outcome.as_ref().and_then(|outcome| outcome.completion_basis), Some(CompletionBasis::TurnEnded));
    assert!(composition.attempts.iter().any(|attempt| attempt.evidence.contains(&InspectionEvidence::ResponseObserved)));
    assert_eq!(composition.model.as_ref().and_then(|model| model.configuration.as_ref()).and_then(|configuration| configuration.effective.get("reasoning_effort")), Some(&json!("high")));
    let encoded = serde_json::to_value(&inspection)?;
    assert!(encoded["composition"]["model"].get("connection_ref").is_none());
    assert!(encoded["composition"]["model"].get("target").is_none());
    assert_eq!(SqliteStateStore::open(&database)?.load(&scope, &run_id).await?, restored);
    assert_eq!(first.calls.load(Ordering::SeqCst), 2);
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    println!(
        "agent consumer: pure construction, detached execution after observer drop, fallback under shared budgets, stored outcome and event replay, duplicate request without new model calls, SQLite reopen, recoverable host stop, durable replay and read-only stored composition inspection"
    );
    Ok(())
}
```
