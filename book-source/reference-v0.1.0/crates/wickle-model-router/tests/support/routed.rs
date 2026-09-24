use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::stream;
use serde_json::json;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use wickle::*;
use wickle_model_router::{ModelDispatcherEntry, PolicyModelRouter, RegistryModelDispatcher};

use crate::core::{self, id, scope};

pub fn reference(name: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id("1"),
    }
}
pub fn options() -> JsonObject {
    [("effort".into(), json!("high"))].into_iter().collect()
}

pub fn routing_snapshot() -> RoutingSnapshot {
    let mut models = Vec::new();
    let mut bindings = Vec::new();
    for (name, provider) in [("primary", "provider-a"), ("fallback", "provider-b")] {
        let capabilities = ModelCapabilities {
            revision: id("capabilities"),
            features: [id("text")].into_iter().collect(),
            options_schema: json!({"type":"object","properties":{"effort":{"enum":["high","low"]}},"required":[],"additionalProperties":false}),
            context_window: 4096.try_into().unwrap(),
            max_output_tokens: 1024.try_into().unwrap(),
        };
        let model = ModelDefinition {
            model_key: id(name),
            family: id("fixture-family"),
            provider: id(provider),
            model_id: id(&format!("{name}-model")),
            model_version: id(&format!("{name}-release")),
            version_semantics: VersionSemantics::Pinned,
            lifecycle: ModelLifecycle::Active,
            capabilities: capabilities.clone(),
            evidence: vec![ModelEvidence {
                source_ref: id("fixture-manifest"),
                observed_at_ms: 1,
            }],
        };
        let mut binding = ModelBinding {
            binding: reference(name),
            model: model.reference(),
            requested_model: model.model_id.clone(),
            adapter: reference("adapter"),
            connection_ref: reference(&format!("{name}-account")),
            target: [("region".into(), json!(format!("{name}-region")))]
                .into_iter()
                .collect(),
            target_schema: json!({"type":"object","properties":{"region":{"type":"string"}},"required":["region"],"additionalProperties":false}),
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("protocol"),
            },
            deployment_revision: Some(id(&format!("{name}-deployment"))),
            version_semantics: VersionSemantics::Pinned,
            capabilities,
            support: ModelSupportStatus::ContractTested,
            evidence: vec![],
        };
        binding.evidence.push(ModelValidationEvidence {
            kind: ModelValidationKind::ContractTest,
            binding_digest: binding.contract_digest(&model).unwrap(),
            checked_at_ms: 1,
            evidence_ref: id("fixture-contract"),
            passed: true,
        });
        models.push(model);
        bindings.push(binding);
    }
    let catalog = ModelCatalogSnapshot {
        revision: id("catalog-1"),
        scope: scope(),
        models,
        bindings,
        aliases: vec![],
    };
    let rules = [
        ModelPurpose::Agent,
        ModelPurpose::Verification,
        ModelPurpose::Compaction,
    ]
    .into_iter()
    .map(|purpose| RoutingRule {
        model_binding: id("primary"),
        purpose,
        primary: reference("primary"),
        fallbacks: vec![reference("fallback")],
        fallback_on: vec![
            ModelFailureKind::RateLimited,
            ModelFailureKind::Transport,
            ModelFailureKind::Unavailable,
            ModelFailureKind::VersionDrift,
        ],
        version_policy: VersionPolicy::RequirePinned,
        min_support: ModelSupportStatus::ContractTested,
    })
    .collect();
    RoutingSnapshot::new(
        catalog,
        RoutingPolicy {
            revision: id("policy-1"),
            scope: scope(),
            rules,
        },
    )
    .unwrap()
}

pub struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: 0,
            monotonic_ms: 0,
        })
    }
    fn sleep_until<'a>(&'a self, deadline: u64) -> PortFuture<'a, ()> {
        if deadline == 0 {
            Box::pin(async { Ok(()) })
        } else {
            Box::pin(std::future::pending())
        }
    }
}
#[derive(Default)]
pub struct Ids(AtomicUsize);
impl IdSource for Ids {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id(&format!(
            "attempt-{}",
            self.0.fetch_add(1, Ordering::SeqCst)
        )))
    }
}

#[derive(Clone, Copy)]
pub enum Reply {
    Complete,
    Fail(ModelFailureKind),
    Pending,
}

pub struct Model {
    binding: ModelPortBinding,
    replies: Mutex<VecDeque<Reply>>,
    pub calls: Mutex<Vec<ModelRequest>>,
    pub entered: Notify,
}
impl Model {
    fn new(binding: &ModelBinding, replies: Vec<Reply>, snapshot: &RoutingSnapshot) -> Self {
        let model = snapshot
            .catalog()
            .models
            .iter()
            .find(|model| model.reference() == binding.model)
            .unwrap();
        Self {
            binding: ModelPortBinding {
                provider: model.provider.clone(),
                adapter: binding.adapter.clone(),
                connection_ref: binding.connection_ref.clone(),
            },
            replies: Mutex::new(replies.into()),
            calls: Mutex::new(vec![]),
            entered: Notify::new(),
        }
    }
}
impl ModelPort for Model {
    fn binding(&self) -> ModelPortBinding {
        self.binding.clone()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        assert!(
            self.binding.matches_route(&request.route),
            "provider/account binding mismatch"
        );
        assert_eq!(request.request_id, context.attempt_id);
        assert_eq!(context.scope, scope());
        self.calls.lock().unwrap().push(request.clone());
        self.entered.notify_one();
        match self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra physical model request")
        {
            Reply::Complete => Box::pin(stream::iter([
                Ok(ModelEvent::TextDelta {
                    text: "Completed response".into(),
                }),
                Ok(ModelEvent::ResponseCompleted {
                    finish: ModelFinish::Stop,
                    metadata: ModelResponseMetadata {
                        provider_request_id: Some(id("actual-provider-request")),
                        reported_model_id: Some(id("actually-reported-model")),
                        reported_model_version: None,
                        usage: None,
                    },
                    continuation: vec![],
                }),
            ])),
            Reply::Fail(kind) => Box::pin(stream::iter([
                Ok(ModelEvent::TextDelta {
                    text: "Partial response".into(),
                }),
                Ok(ModelEvent::ResponseError {
                    kind,
                    metadata: ModelResponseMetadata::default(),
                }),
            ])),
            Reply::Pending => Box::pin(stream::pending()),
        }
    }
}

#[derive(Default)]
pub struct Policy {
    pub denied_provider: Mutex<Option<Id>>,
    pub calls: Mutex<Vec<(ResolvedModelRoute, ModelPurpose)>>,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::InvokeModel { route, purpose } = &request.action {
                self.calls
                    .lock()
                    .unwrap()
                    .push((route.as_ref().clone(), *purpose));
                if self.denied_provider.lock().unwrap().as_ref() == Some(&route.provider) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("destination-denied"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}

#[derive(Clone, Copy, Default)]
pub enum Inspection {
    #[default]
    Healthy,
    DriftPrimary,
    UnavailablePrimary,
    UnavailableAll,
    Pending,
    Panics,
}
#[derive(Default)]
pub struct Inspector {
    pub mode: Mutex<Inspection>,
    pub calls: Mutex<Vec<ResolvedModelRoute>>,
    pub tokens: Mutex<Vec<CancellationToken>>,
    pub entered: Notify,
}
impl ModelRouteInspector for Inspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        context: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            assert_eq!(context.scope, scope());
            self.calls.lock().unwrap().push(route.clone());
            self.tokens
                .lock()
                .unwrap()
                .push(context.cancellation.clone());
            self.entered.notify_one();
            let mode = *self.mode.lock().unwrap();
            match mode {
                Inspection::Pending => std::future::pending::<()>().await,
                Inspection::Panics => panic!("fixture inspector panic"),
                _ => {}
            }
            let primary = route.binding.id == id("primary");
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: if matches!(mode, Inspection::UnavailableAll)
                    || (primary && matches!(mode, Inspection::UnavailablePrimary))
                {
                    ModelRouteAvailability::Unavailable
                } else {
                    ModelRouteAvailability::Available
                },
                model_id: Some(route.model_id.clone()),
                model_version: Some(if primary && matches!(mode, Inspection::DriftPrimary) {
                    id("changed-release")
                } else {
                    route.model_version.clone()
                }),
                deployment_revision: route.deployment_revision.clone(),
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id(&format!("inspection-{}", route.binding.id)),
            })
        })
    }
}

#[derive(Clone, Copy, Default)]
pub enum Projection {
    #[default]
    Valid,
    WrongRoute,
    WrongOptions,
    TooManyTokens,
    OldOpaque,
    DifferentContent,
    UsesTools,
    UsesJson,
}
#[derive(Default)]
pub struct Projector {
    pub mode: Mutex<Projection>,
    pub calls: Mutex<Vec<ResolvedModelRoute>>,
}
impl ModelRequestProjector for Projector {
    fn project<'a>(
        &'a self,
        selection: &'a RouteSelection,
        input: &'a RoutedModelInput,
        _: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ProjectedModelRequest> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(selection.route.clone());
            let mode = *self.mode.lock().unwrap();
            let mut request = ModelRequest {
                request_id: input.model_step_id.clone(),
                purpose: input.routing.purpose,
                route: selection.route.clone(),
                messages: vec![ModelMessage {
                    role: ModelRole::User,
                    content: vec![ModelContent::Text {
                        text: if matches!(mode, Projection::DifferentContent) {
                            "Changed required context"
                        } else {
                            "Preserved required context"
                        }
                        .into(),
                    }],
                }],
                tools: vec![],
                output: ModelOutput::Text {},
                max_output_tokens: input.routing.max_output_tokens,
                options: input.routing.options.clone(),
                limits: ModelResponseLimits {
                    max_input_bytes: 16384,
                    max_response_bytes: 4096,
                    max_delta_bytes: 1024,
                    max_events: 8,
                    max_tool_calls: 0,
                },
            };
            match mode {
                Projection::WrongRoute => {
                    request.route.connection_ref = reference("different-account")
                }
                Projection::WrongOptions => {
                    request.options.insert("effort".into(), json!("low"));
                }
                Projection::OldOpaque => {
                    let old = ResolvedModelRoute {
                        provider: id("old-provider"),
                        ..selection.route.clone()
                    };
                    request.messages.push(ModelMessage {
                        role: ModelRole::Assistant,
                        content: vec![ModelContent::Opaque {
                            continuation: OpaqueContinuation::new(
                                &old,
                                json!({"private_replay":"old"}),
                            ),
                        }],
                    });
                }
                Projection::UsesTools => {
                    request.tools = vec![ModelTool {
                        name: id("search"),
                        description: "Search records".into(),
                        model_input_schema: json!({"type":"object","properties":{},"required":[],"additionalProperties":false}),
                    }];
                }
                Projection::UsesJson => {
                    request.output = ModelOutput::JsonSchema {
                        schema: json!({"type":"object"}),
                    };
                }
                _ => {}
            }
            Ok(ProjectedModelRequest {
                request,
                input_tokens: if matches!(mode, Projection::TooManyTokens) {
                    4096
                } else {
                    100
                },
            })
        })
    }
}

pub struct Fixture {
    pub store: Arc<MemoryStateStore>,
    pub lease: RunLease,
    pub ids: Arc<Ids>,
    pub snapshot: RoutingSnapshot,
    pub router: PolicyModelRouter,
    pub first: Arc<Model>,
    pub second: Arc<Model>,
    pub policy: Arc<Policy>,
    pub inspector: Arc<Inspector>,
    pub projector: Projector,
}
impl Fixture {
    pub async fn new(first: Vec<Reply>, second: Vec<Reply>) -> Self {
        Self::with_limits(first, second, 8, 4).await
    }
    pub async fn with_limits(
        first: Vec<Reply>,
        second: Vec<Reply>,
        model_calls: u64,
        recoveries: u64,
    ) -> Self {
        let store = Arc::new(MemoryStateStore::new());
        let mut input =
            core::admission("run", "request", "session", "Preserved request", "1").await;
        let mut profile = serde_json::to_value(input.snapshot.profile.profile()).unwrap();
        profile["limits"]["max_model_calls"] = json!(model_calls);
        profile["limits"]["max_recovery_attempts"] = json!(recoveries);
        input.snapshot.profile = ProfileValidator::new(&core::Catalog { revision: "1" })
            .validate(
                &AgentProfile::from_json(&profile.to_string()).unwrap(),
                &scope(),
            )
            .await
            .unwrap();
        input.snapshot.limits = input.snapshot.profile.profile().limits.clone();
        input.snapshot.request.model_options = options();
        input.snapshot.request_digest =
            admission_digest(&input.snapshot.request, &input.snapshot.profile, None);
        let RunEventPayload::RunStarted {
            request_ref,
            profile_digest,
        } = &mut input.events[0].payload
        else {
            unreachable!()
        };
        *profile_digest = input.snapshot.profile.profile_digest().clone();
        let record = ProtectedRecord::new(
            request_ref.record_id.clone(),
            request_ref.revision,
            serde_json::to_value(&input.snapshot.request).unwrap(),
        );
        let previous = request_ref.clone();
        *request_ref = record.reference().clone();
        *input
            .records
            .iter_mut()
            .find(|record| record.reference() == &previous)
            .unwrap() = record;
        store.admit(&scope(), input).await.unwrap();
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("worker"), 0, 20_000)
            .await
            .unwrap();
        let snapshot = routing_snapshot();
        let first = Arc::new(Model::new(
            &snapshot.catalog().bindings[0],
            first,
            &snapshot,
        ));
        let second = Arc::new(Model::new(
            &snapshot.catalog().bindings[1],
            second,
            &snapshot,
        ));
        let router = PolicyModelRouter::new(snapshot.clone()).unwrap();
        Self {
            store,
            lease,
            ids: Arc::new(Ids::default()),
            snapshot,
            router,
            first,
            second,
            policy: Arc::new(Policy::default()),
            inspector: Arc::new(Inspector::default()),
            projector: Projector::default(),
        }
    }
    pub fn input(&self, step: &str) -> RoutedModelInput {
        RoutedModelInput {
            model_step_id: id(step),
            routing: RouteRequest {
                model_binding: id("primary"),
                purpose: ModelPurpose::Agent,
                required_capabilities: [id("text")].into_iter().collect(),
                input_tokens: 1,
                max_output_tokens: 64.try_into().unwrap(),
                options: options(),
                scope: scope(),
                allowed_bindings: vec![id("primary"), id("fallback")],
                version_policy: VersionPolicy::RequirePinned,
                previous_route: None,
                previous_failure: None,
            },
        }
    }
    pub fn enable_feature(&mut self, feature: &str) {
        let mut catalog = self.snapshot.catalog().clone();
        for model in &mut catalog.models {
            model.capabilities.features.insert(id(feature));
        }
        for binding in &mut catalog.bindings {
            binding.capabilities.features.insert(id(feature));
            let model = catalog
                .models
                .iter()
                .find(|model| model.reference() == binding.model)
                .unwrap();
            binding.evidence[0].binding_digest = binding.contract_digest(model).unwrap();
        }
        self.snapshot = RoutingSnapshot::new(catalog, self.snapshot.policy().clone()).unwrap();
        self.router = PolicyModelRouter::new(self.snapshot.clone()).unwrap();
    }
    pub fn context(&self) -> ExecutionContext {
        ExecutionContext::new(
            ExecutionContextData {
                scope: scope(),
                principal_ref: id("actor"),
                capability_grant_ref: id("grant"),
                trace_context: None,
                system_inputs: None,
            },
            CancellationToken::new(),
        )
    }
    pub async fn budget(&self) -> RunBudget {
        RunBudget::attach(
            self.store.clone(),
            Arc::new(FixedClock),
            self.ids.clone(),
            scope(),
            id("run"),
            self.lease.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap()
    }
    pub fn exchange(&self, retries: u32) -> ModelExchange {
        let dispatcher = RegistryModelDispatcher::new(vec![
            ModelDispatcherEntry {
                scope: scope(),
                port: self.first.clone(),
            },
            ModelDispatcherEntry {
                scope: scope(),
                port: self.second.clone(),
            },
        ])
        .unwrap();
        ModelExchange::with_dispatcher(
            Arc::new(dispatcher),
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap()),
        )
        .with_route_inspector(self.inspector.clone(), Duration::from_secs(1))
        .unwrap()
        .with_retry_policy(ModelRetryPolicy {
            max_retries: retries,
            backoff_ms: 0,
        })
    }
    pub async fn saved(&self) -> RunSnapshot {
        self.store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
    }
    pub fn call_counts(&self) -> (usize, usize) {
        (
            self.first.calls.lock().unwrap().len(),
            self.second.calls.lock().unwrap().len(),
        )
    }
}

pub fn completed(outcome: Guarded<ModelExchangeOutcome>) -> ModelResponse {
    let Guarded::Completed(ModelExchangeOutcome::Completed { response }) = outcome else {
        panic!("expected complete response")
    };
    response
}

pub async fn seed_tool(fixture: &Fixture, state: ToolCallState) {
    let saved = fixture.saved().await;
    let call = ToolCall {
        call_id: id("unsettled-call"),
        model_request_id: id("earlier-attempt"),
        provider_call_id: id("earlier-call"),
        tool_name: id("tool"),
        model_inputs: JsonObject::new(),
        descriptor_digest: Some(canonical_digest(&json!("descriptor"))),
        bound_input_ref: None,
    };
    let mut change = core::prepared(&saved, fixture.lease.clone(), 0);
    change
        .snapshot
        .tool_ledger
        .push(ToolLedgerEntry { call, state });
    fixture
        .store
        .commit(&scope(), &id("run"), change)
        .await
        .unwrap();
}

pub fn unknown_tool_result() -> ToolCallState {
    ToolCallState::Settled {
        result: ToolResult {
            call_id: id("unsettled-call"),
            call_message_id: id("earlier-message"),
            status: ToolResultStatus::Unknown,
            effect: ToolEffect::Unknown,
            content: vec![],
            effect_receipt_ref: None,
            skill_ref: None,
            error: None,
        },
    }
}
