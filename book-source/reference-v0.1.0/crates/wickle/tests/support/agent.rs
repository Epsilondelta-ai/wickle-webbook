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
    pub context_attempts: AtomicUsize,
    pub empty_page_entered: Notify,
    pub empty_page_release: Semaphore,
    paused_empty_page: AtomicBool,
    pub block_read: AtomicUsize,
    pub read_entered: Notify,
}
impl FinalCommitStore {
    pub fn new(inner: Arc<MemoryStateStore>, mode: FinalCommitMode) -> Self {
        Self {
            inner,
            mode,
            final_entered: Notify::new(),
            release: Semaphore::new(0),
            final_attempts: AtomicUsize::new(0),
            context_attempts: AtomicUsize::new(0),
            empty_page_entered: Notify::new(),
            empty_page_release: Semaphore::new(0),
            paused_empty_page: AtomicBool::new(false),
            block_read: AtomicUsize::new(0),
            read_entered: Notify::new(),
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
        self.inner.read_record(s, r)
    }
    fn commit<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            let mut input = input;
            if matches!(self.mode, FinalCommitMode::RejectRecoveryAcceptance)
                && input
                    .events
                    .iter()
                    .any(|event| matches!(event.payload, RunEventPayload::RunRecovered { .. }))
            {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "recovery.offline",
                ));
            }

            if matches!(self.mode, FinalCommitMode::LoseRecoveryAcknowledgement)
                && input
                    .events
                    .iter()
                    .any(|event| matches!(event.payload, RunEventPayload::RunRecovered { .. }))
            {
                self.inner.commit(s, r, input).await?;
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "recovery.ack",
                ));
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
            if !input.snapshot.status.is_terminal() {
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
