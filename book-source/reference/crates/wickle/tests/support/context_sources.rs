//! Deterministic sources and model attempts expose saved-batch and current-ACL behavior.

use super::{hooks_support, resume_support};
use futures_util::stream;
pub use hooks_support::{completed, context, gate, id, object, reference, request, scope};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};
use wickle::*;

#[derive(Clone, Copy)]
pub enum Reply {
    Ready,
    ReadySame,
    ReadyVersion,
    Deleted,
    Empty,
    Unavailable,
    WrongScope,
    WrongOrigin,
    WrongLifetime,
    WrongDigest,
    TooMany,
    TooLarge,
    Pending,
    Paused,
    Error,
    ReadyEmpty,
}

pub struct Estimator {
    pub tokens: AtomicUsize,
    pub calls: AtomicUsize,
}
impl ContextTokenEstimator for Estimator {
    fn version(&self) -> VersionedRef {
        reference("fixture-context-estimate")
    }
    fn estimate(&self, items: &[ContextItem]) -> Result<u64, ContractError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(if items.is_empty() {
            0
        } else {
            self.tokens.load(Ordering::SeqCst) as u64
        })
    }
}
pub struct Source {
    pub definition: ContextSourceDefinition,
    pub selection: ContextSourceRef,
    pub replies: Mutex<VecDeque<Reply>>,
    pub calls: AtomicUsize,
    pub uses: AtomicUsize,
    pub use_failure: Mutex<Option<(usize, ErrorCode)>>,
    pub revoked: Arc<AtomicBool>,
    pub requests: Mutex<Vec<ContextRequest>>,
    pub use_requests: Mutex<Vec<ContextUseRequest>>,
    pub order: Arc<Mutex<Vec<Id>>>,
    pub entered: Notify,
    pub release: Semaphore,
}
impl ContextSource for Source {
    fn provide<'a>(
        &'a self,
        request: &'a ContextRequest,
        context: &'a ContextCallContext,
    ) -> PortFuture<'a, ContextResult> {
        Box::pin(async move {
            let generation = self.calls.fetch_add(1, Ordering::SeqCst);
            self.requests.lock().unwrap().push(request.clone());
            self.order
                .lock()
                .unwrap()
                .push(self.definition.source.id.clone());
            assert_eq!(request.scope, scope());
            assert_eq!(context.scope, scope());
            assert_eq!(request.definition, self.definition);
            assert_eq!(request.binding.source, self.selection);
            assert_eq!(context.source, self.selection);
            assert_eq!(context.context_request_id, request.context_request_id);
            assert_eq!(
                request.user_input,
                super::agent_support::request("request").input
            );
            self.entered.notify_one();
            let reply = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("only planned source queries may execute");
            match reply {
                Reply::Pending => return std::future::pending().await,
                Reply::Paused => {
                    self.release.acquire().await.unwrap().forget();
                }
                Reply::Error => {
                    return Err(ContractError::new(
                        ErrorCode::InvalidContract,
                        "source.transport",
                    ));
                }
                Reply::Deleted => {
                    return Ok(ContextResult::Deleted {
                        item_ids: vec![id("shared")],
                        source_revision: Some(id(&format!("revision-{generation}"))),
                        reported_usage: None,
                    });
                }
                Reply::Empty => {
                    return Ok(ContextResult::Empty {
                        source_revision: Some(id(&format!("revision-{generation}"))),
                        reported_usage: None,
                    });
                }
                Reply::Unavailable => {
                    return Ok(ContextResult::Unavailable {
                        code: id("backend_unavailable"),
                        source_revision: Some(id(&format!("revision-{generation}"))),
                        reported_usage: None,
                    });
                }
                _ => {}
            }
            let count = if matches!(reply, Reply::TooMany) {
                3
            } else if matches!(reply, Reply::ReadyEmpty) {
                0
            } else {
                1
            };
            let mut items = vec![];
            for index in 0..count {
                let mut owner = request.scope.clone();
                if matches!(reply, Reply::WrongScope) {
                    owner.tenant_id = id("other-tenant");
                }
                let origin = if matches!(reply, Reply::WrongOrigin) {
                    ContextOrigin::Memory
                } else {
                    self.definition.origin
                };
                let lifetime = if matches!(reply, Reply::WrongLifetime) {
                    ContextLifetime::Session {
                        session_id: request.session_id.clone(),
                    }
                } else {
                    match request.binding.trigger {
                        ContextTrigger::RunStart => ContextLifetime::Run {
                            run_id: request.run_id.clone(),
                        },
                        ContextTrigger::BeforeModel => ContextLifetime::Step {
                            run_id: request.run_id.clone(),
                            model_step_id: request.model_step_id.clone().unwrap(),
                        },
                    }
                };
                let value = if matches!(reply, Reply::TooLarge) {
                    json!({"source":self.definition.source.id,"payload":"x".repeat(5000)})
                } else {
                    json!({"source":self.definition.source.id,"generation":if matches!(reply, Reply::ReadySame) { 0 } else { generation }})
                };
                let mut item = ContextItem::new(
                    if count == 1 {
                        id("shared")
                    } else {
                        id(&format!("item-{index}"))
                    },
                    origin,
                    self.definition.source.clone(),
                    owner,
                    vec![InputContent::Json { value }],
                    lifetime,
                    ContextPriority::Required,
                );
                if matches!(reply, Reply::WrongDigest) {
                    item.digest = canonical_digest(&json!("not-the-item"));
                }
                items.push(item);
            }
            Ok(ContextResult::Ready {
                item_revisions: if matches!(reply, Reply::ReadyVersion) {
                    [(id("shared"), id("document-r1"))].into()
                } else {
                    Default::default()
                },
                items,
                source_revision: Some(id(&format!("revision-{generation}"))),
                reported_usage: Some(ContextSourceUsage {
                    requests: Some(1),
                    tokens: Some(1_000_000),
                }),
            })
        })
    }
    fn authorize_use<'a>(
        &'a self,
        request: &'a ContextUseRequest,
        context: &'a ContextCallContext,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let check = self.uses.fetch_add(1, Ordering::SeqCst) + 1;
            self.use_requests.lock().unwrap().push(request.clone());
            assert_eq!(request.request.binding.source, self.selection);
            assert_eq!(context.source, self.selection);
            assert_eq!(context.scope, scope());
            assert_eq!(request.request.scope, scope());
            assert!(
                request
                    .items
                    .iter()
                    .all(|item| item.item_id == id("shared"))
            );
            if let Some((at, code)) = *self.use_failure.lock().unwrap() {
                if check == at {
                    return Err(ContractError::new(code, "source.current_acl"));
                }
            }
            if self.revoked.load(Ordering::SeqCst) {
                Err(ContractError::new(
                    ErrorCode::AccessDenied,
                    "source.current_acl",
                ))
            } else {
                Ok(())
            }
        })
    }
}
pub struct Model {
    pub inner: Arc<resume_support::Model>,
    pub physical_calls: AtomicUsize,
    pub fail_first: AtomicBool,
    pub revoke_on_failure: AtomicBool,
    pub revoked: Arc<AtomicBool>,
    pub requests: Mutex<Vec<ModelRequest>>,
}
impl ModelPort for Model {
    fn binding(&self) -> ModelPortBinding {
        self.inner.binding()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let attempt = self.physical_calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        if attempt == 0 && self.fail_first.load(Ordering::SeqCst) {
            if self.revoke_on_failure.load(Ordering::SeqCst) {
                self.revoked.store(true, Ordering::SeqCst);
            }
            Box::pin(stream::iter([Ok(ModelEvent::ResponseError {
                kind: ModelFailureKind::Transport,
                metadata: Default::default(),
            })]))
        } else {
            self.inner.generate(request, context)
        }
    }
}
pub struct Fixture {
    pub base: hooks_support::Fixture,
    pub sources: Vec<Arc<Source>>,
    pub bindings: Vec<ContextSourceBinding>,
    pub estimator: Arc<Estimator>,
    pub model: Arc<Model>,
    pub revoked: Arc<AtomicBool>,
    pub order: Arc<Mutex<Vec<Id>>>,
    pub copy_hook: Option<Arc<CopyHook>>,
    pub policy: Arc<Policy>,
    pub store: Arc<SourceStore>,
}
impl Fixture {
    pub fn new() -> Self {
        let base = hooks_support::Fixture::new();
        let revoked = Arc::new(AtomicBool::new(false));
        let model = Arc::new(Model {
            inner: base.base.model.clone(),
            physical_calls: AtomicUsize::new(0),
            fail_first: AtomicBool::new(false),
            revoke_on_failure: AtomicBool::new(false),
            revoked: revoked.clone(),
            requests: Mutex::new(vec![]),
        });
        let policy = Arc::new(Policy {
            inner: base.policy.clone(),
            deny_provide: AtomicBool::new(false),
            deny_use: AtomicBool::new(false),
        });
        let store = Arc::new(SourceStore {
            inner: base.base.base.store.clone(),
            failure: AtomicUsize::new(0),
            preparation_failure: AtomicUsize::new(0),
        });
        Self {
            base,
            sources: vec![],
            bindings: vec![],
            estimator: Arc::new(Estimator {
                tokens: AtomicUsize::new(16),
                calls: AtomicUsize::new(0),
            }),
            model,
            revoked,
            order: Arc::new(Mutex::new(vec![])),
            copy_hook: None,
            policy,
            store,
        }
    }
    pub fn add(
        &mut self,
        name: &str,
        trigger: ContextTrigger,
        required: bool,
        replies: Vec<Reply>,
    ) -> Arc<Source> {
        let selection = ContextSourceRef::Catalog(CatalogSourceRef {
            source_id: id(name),
            version: id("1"),
        });
        let source = Arc::new(Source {
            definition: ContextSourceDefinition {
                source: reference(name),
                origin: ContextOrigin::Retrieval,
                contract_version: 1,
            },
            selection: selection.clone(),
            replies: Mutex::new(replies.into()),
            calls: AtomicUsize::new(0),
            uses: AtomicUsize::new(0),
            use_failure: Mutex::new(None),
            revoked: self.revoked.clone(),
            requests: Mutex::new(vec![]),
            use_requests: Mutex::new(vec![]),
            order: self.order.clone(),
            entered: Notify::new(),
            release: Semaphore::new(0),
        });
        self.bindings.push(ContextSourceBinding {
            source: selection,
            trigger,
            required,
            timeout_ms: 5000.try_into().unwrap(),
            max_items: 2.try_into().unwrap(),
            max_bytes: 4096.try_into().unwrap(),
            max_tokens: 100.try_into().unwrap(),
        });
        self.sources.push(source.clone());
        source
    }
    pub fn profile(&self) -> AgentProfile {
        let mut profile = self.base.profile();
        profile.context_sources = Some(self.bindings.clone());
        profile
    }
    pub fn enable_copy_hook(&mut self) -> Arc<CopyHook> {
        self.base.add(
            "copied-source",
            HookPosition::BeforeModel,
            hooks_support::Behavior::Context,
            0,
            true,
        );
        let hook = Arc::new(CopyHook {
            calls: AtomicUsize::new(0),
            seen: Mutex::new(vec![]),
        });
        self.copy_hook = Some(hook.clone());
        hook
    }
    pub fn agent_bindings(&self) -> AgentBindings {
        let mut bindings = self.base.bindings();
        bindings.state = self.store.clone();
        bindings.policy =
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(5)).unwrap());
        bindings.model_exchange = Arc::new(
            ModelExchange::new(self.model.clone(), bindings.policy.clone())
                .with_route_inspector(
                    self.base.base.base.inspector.clone(),
                    Duration::from_secs(5),
                )
                .unwrap()
                .with_retry_policy(ModelRetryPolicy {
                    max_retries: 1,
                    backoff_ms: 0,
                }),
        );
        let registry = Arc::new(
            ContextSourceRegistry::new(
                scope(),
                self.sources
                    .iter()
                    .map(|source| ContextSourceRegistration {
                        selection: source.selection.clone(),
                        definition: source.definition.clone(),
                        source: source.clone(),
                    })
                    .collect(),
            )
            .unwrap(),
        );
        bindings.context_sources = Some(Arc::new(
            ContextSourceRuntime::new(
                bindings.state.clone(),
                bindings.policy.clone(),
                bindings.clock.clone(),
                bindings.ids.clone(),
                registry,
                self.estimator.clone(),
            )
            .unwrap(),
        ));
        bindings.context_token_estimator = Some(self.estimator.clone());
        if let Some(copy) = &self.copy_hook {
            let registry = HookRegistry::new(
                scope(),
                self.base
                    .hooks
                    .iter()
                    .map(|hook| HookRegistration {
                        definition: hook.definition.clone(),
                        handler: if hook.definition.hook.id == id("copied-source") {
                            copy.clone() as Arc<dyn HookHandler>
                        } else {
                            hook.clone()
                        },
                    })
                    .collect(),
            )
            .unwrap();
            bindings.hooks = Some(Arc::new(HookRuntime::new(
                bindings.state.clone(),
                bindings.policy.clone(),
                bindings.clock.clone(),
                bindings.ids.clone(),
                Arc::new(registry),
            )));
        }
        bindings
    }
    pub fn agent(&self) -> Agent {
        create_agent(self.profile(), self.agent_bindings()).unwrap()
    }
    pub async fn start(&self, agent: &Agent) -> RunHandle {
        self.base.started(agent).await
    }
    pub async fn outcome(&self, handle: &RunHandle) -> RunOutcome {
        self.base.outcome(handle).await
    }
    pub async fn saved(&self, handle: &RunHandle) -> StoredRun {
        self.base.saved(handle).await
    }
}
pub struct Policy {
    inner: Arc<hooks_support::Policy>,
    pub deny_provide: AtomicBool,
    pub deny_use: AtomicBool,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if (matches!(request.action, PolicyAction::ProvideContext { .. })
                && self.deny_provide.load(Ordering::SeqCst))
                || (matches!(request.action, PolicyAction::UseSourceContext { .. })
                    && self.deny_use.load(Ordering::SeqCst))
            {
                return Ok(PolicyDecision::Deny {
                    reason: id("source_policy_revoked"),
                });
            }
            self.inner.authorize(request, context).await
        })
    }
}

/// Inject faults only into a newly committed source batch; all other state behavior is real.
pub struct SourceStore {
    pub inner: Arc<MemoryStateStore>,
    pub failure: AtomicUsize,
    pub preparation_failure: AtomicUsize,
}
impl StateStore for SourceStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        s: &'a Scope,
        session: &'a Id,
        r: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        self.inner.find_request(s, session, r)
    }
    fn admit<'a>(&'a self, s: &'a Scope, input: AdmissionInput) -> PortFuture<'a, AdmissionResult> {
        self.inner.admit(s, input)
    }
    fn load<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, StoredRun> {
        self.inner.load(s, r)
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
        a: u64,
        n: usize,
    ) -> PortFuture<'a, EventPage> {
        self.inner.read_events(s, r, a, n)
    }
    fn read_record<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        self.inner.read_record(s, r)
    }
    fn record_hook_observation<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        report: HookObservation,
    ) -> PortFuture<'a, ()> {
        self.inner.record_hook_observation(s, r, report)
    }
    fn read_hook_observations<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
    ) -> PortFuture<'a, Vec<HookObservation>> {
        self.inner.read_hook_observations(s, r)
    }
    fn commit<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            let saved = self.inner.load(s, r).await?;
            if input.snapshot.prepared_steps.len() > saved.snapshot.prepared_steps.len() {
                match self.preparation_failure.swap(0, Ordering::SeqCst) {
                    1 => {
                        return Err(ContractError::new(
                            ErrorCode::PersistenceUnavailable,
                            "prepared.commit",
                        ));
                    }
                    2 => {
                        self.inner.commit(s, r, input).await?;
                        return Err(ContractError::new(
                            ErrorCode::PersistenceUnavailable,
                            "prepared.ack",
                        ));
                    }
                    _ => {}
                }
            }
            if input.snapshot.context_batches.len() > saved.snapshot.context_batches.len() {
                match self.failure.swap(0, Ordering::SeqCst) {
                    1 => {
                        return Err(ContractError::new(
                            ErrorCode::PersistenceUnavailable,
                            "source.commit",
                        ));
                    }
                    2 => {
                        self.inner.commit(s, r, input).await?;
                        return Err(ContractError::new(
                            ErrorCode::PersistenceUnavailable,
                            "source.ack",
                        ));
                    }
                    _ => {}
                }
            }
            self.inner.commit(s, r, input).await
        })
    }
}
pub struct CopyHook {
    pub calls: AtomicUsize,
    pub seen: Mutex<Vec<Vec<ContextItem>>>,
}
impl HookHandler for CopyHook {
    fn call<'a>(&'a self, input: &'a HookInput, _: &'a HookContext) -> PortFuture<'a, HookOutput> {
        Box::pin(async move {
            let HookInput::BeforeModel { context_items, .. } = input else {
                return Err(ContractError::new(
                    ErrorCode::InvalidContract,
                    "hook.position",
                ));
            };
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.seen.lock().unwrap().push(context_items.clone());
            Ok(HookOutput::Context {
                additions: context_items
                    .iter()
                    .map(|item| HookContextAddition {
                        content: item.content.clone(),
                        priority: ContextPriority::Required,
                    })
                    .collect(),
            })
        })
    }
}

pub fn projected_items(request: &ModelRequest) -> Vec<&Value> {
    request
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
        .collect()
}

/// Two exact, usable routes sharing the fixture port; only an explicit matching
/// failure permits the second candidate. Counters expose accidental fallback.
pub struct FallbackRouter {
    snapshot: RoutingSnapshot,
    pub fallbacks: AtomicUsize,
}
impl FallbackRouter {
    pub fn new(original: &RoutingSnapshot, cause: ModelFailureKind) -> Self {
        let mut catalog = original.catalog().clone();
        let mut policy = original.policy().clone();
        let mut fallback = catalog.bindings[0].clone();
        fallback.binding = reference("fallback-route");
        let digest = fallback.contract_digest(&catalog.models[0]).unwrap();
        for evidence in &mut fallback.evidence {
            evidence.binding_digest = digest.clone();
        }
        policy.rules[0].fallbacks = vec![fallback.binding.clone()];
        policy.rules[0].fallback_on = vec![cause];
        catalog.bindings.push(fallback);
        Self {
            snapshot: RoutingSnapshot::new(catalog, policy).unwrap(),
            fallbacks: AtomicUsize::new(0),
        }
    }
}
impl ModelRouter for FallbackRouter {
    fn snapshot(&self) -> &RoutingSnapshot {
        &self.snapshot
    }
    fn resolve<'a>(&'a self, request: &'a RouteRequest) -> PortFuture<'a, RouteSelection> {
        Box::pin(async move {
            let rule = &self.snapshot.policy().rules[0];
            let (binding, reason, candidate_index) = if let Some(failure) = request.previous_failure
            {
                self.fallbacks.fetch_add(1, Ordering::SeqCst);
                (
                    &rule.fallbacks[0],
                    RouteSelectionReason::Fallback { failure },
                    1,
                )
            } else if request
                .previous_route
                .as_ref()
                .is_some_and(|route| route.binding == rule.fallbacks[0])
            {
                (&rule.fallbacks[0], RouteSelectionReason::Reuse, 1)
            } else {
                (
                    &rule.primary,
                    if request.previous_route.is_some() {
                        RouteSelectionReason::Reuse
                    } else {
                        RouteSelectionReason::Initial
                    },
                    0,
                )
            };
            let selection = RouteSelection {
                route: self.snapshot.route_for_binding(binding)?,
                reason,
                candidate_index,
                routing_snapshot_digest: self.snapshot.digest(),
                request_digest: request.digest(),
            };
            self.snapshot.validate_selection(request, &selection)?;
            Ok(selection)
        })
    }
}

impl wickle::ExecutionTransactions for SourceStore {
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
        self.inner.begin_segment(scope, request)
    }
}
