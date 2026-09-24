# 16장 전체 Rust 구현과 테스트

[강의로](../16-tools.md) · [전체 변경 패치](../solutions/16-tools.patch)

기준 `84ad84936e4974d81bd731c6f4bedd445310b8db`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle-model-router/tests/support/routed.rs`

```rust
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
            error: None,
        },
    }
}
```

## `crates/wickle-state-sqlite/tests/support/mod.rs`

```rust
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::stream;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use wickle::*;

use crate::core::{self, admission, event, id, scope};

pub struct Database {
    directory: PathBuf,
}
impl Database {
    pub fn new() -> Self {
        let unique = RandomIdSource.next_id().unwrap();
        let directory = std::env::temp_dir().join(format!("wickle-sqlite-test-{unique}"));
        std::fs::create_dir(&directory).unwrap();
        Self { directory }
    }
    pub fn path(&self) -> PathBuf {
        self.directory.join("state.sqlite")
    }
    pub fn file(&self, name: &str) -> PathBuf {
        self.directory.join(name)
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

pub async fn durable_admission(run: &str, request: &str, session: &str) -> AdmissionInput {
    let mut input = admission(run, request, session, "Stored request", "1").await;
    input.require_durable = true;
    input
}

#[derive(Default)]
struct FixedIds(AtomicUsize);
impl IdSource for FixedIds {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id(&format!(
            "attempt-{}",
            self.0.fetch_add(1, Ordering::SeqCst)
        )))
    }
}
struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: 0,
            monotonic_ms: 0,
        })
    }
    fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}
struct Allow;
impl PolicyPort for Allow {
    fn authorize<'a>(
        &'a self,
        _: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async { Ok(PolicyDecision::Allow {}) })
    }
}
fn reference(name: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id("1"),
    }
}
struct Model;
impl ModelPort for Model {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: id("fixture-provider"),
            adapter: reference("fixture-adapter"),
            connection_ref: reference("fixture-connection"),
        }
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        assert_eq!(request.request_id, context.attempt_id);
        Box::pin(stream::iter([
            Ok(ModelEvent::ToolArgumentsDelta {
                index: 0,
                provider_call_id: Some("provider-call".into()),
                name: Some("search".into()),
                delta: r#"{"query":"reports"}"#.into(),
            }),
            Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::ToolCalls,
                metadata: ModelResponseMetadata {
                    provider_request_id: Some(id("reported-request")),
                    reported_model_id: Some(id("reported-model")),
                    reported_model_version: None,
                    usage: Some(ModelUsage {
                        measurement: UsageMeasurement::Reported,
                        input_tokens: Some(7),
                        output_tokens: Some(3),
                    }),
                },
                continuation: vec![],
            }),
        ]))
    }
}

/// Use real model/binder boundaries to create historical records whose state later changes.
pub async fn populate_protected_run(store: Arc<dyn StateStore>) -> Value {
    let registry = Arc::new(
        SystemInputRegistry::new(vec![SystemInputDefinition {
            key: id("workspace_id"),
            version: id("definition-1"),
            value_schema: json!({"type":"string","format":"uuid"}),
            source: SystemInputSource::Run {},
        }])
        .unwrap(),
    );
    let compiled = SchemaCompiler::new().compile(ToolDescriptor {
        tool: reference("search"), name: id("search"), description: "Search reports".into(),
        input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","default":10},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters: vec!["query".into(), "limit".into()], system_bindings: None,
        output_schema: json!({"type":"string"}), side_effect: ToolSideEffect::ReadOnly,
        concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false,
        max_output_bytes: 1024.try_into().unwrap(),
    }, &registry).unwrap();
    let mut input = durable_admission("run", "request", "session").await;
    let mut profile = serde_json::to_value(input.snapshot.profile.profile()).unwrap();
    profile["tools"] = json!([{"tool_id":"search","version":"1"}]);
    input.snapshot.profile = ProfileValidator::new(&core::Catalog { revision: "1" })
        .validate(
            &AgentProfile::from_json(&profile.to_string()).unwrap(),
            &scope(),
        )
        .await
        .unwrap();
    let values = SystemInputs::new(
        [(
            "workspace_id".into(),
            json!("11111111-1111-4111-8111-111111111111"),
        )]
        .into_iter()
        .collect(),
    );
    let captured = RunSystemInputs::capture(scope(), Some(values), &registry).unwrap();
    let run_record = captured.to_record(id("run-inputs"), 7);
    input.snapshot.system_inputs = Some(captured.snapshot_ref(run_record.reference()).unwrap());
    input.snapshot.request_digest = admission_digest(
        &input.snapshot.request,
        &input.snapshot.profile,
        input.snapshot.system_inputs.as_ref(),
    );
    if let RunEventPayload::RunStarted { profile_digest, .. } = &mut input.events[0].payload {
        *profile_digest = input.snapshot.profile.profile_digest().clone();
    }
    input.records.push(run_record);
    store.admit(&scope(), input).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("worker"), 0, 20_000)
        .await
        .unwrap();
    let cancellation = CancellationToken::new();
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope(),
            principal_ref: id("principal"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        cancellation.clone(),
    );
    let ids = Arc::new(FixedIds::default());
    let budget = RunBudget::attach(
        store.clone(),
        Arc::new(FixedClock),
        ids.clone(),
        scope(),
        id("run"),
        lease.clone(),
        cancellation,
    )
    .await
    .unwrap();
    let policy = Arc::new(PolicyGate::new(Arc::new(Allow), Duration::from_secs(1)).unwrap());
    let binding = Model.binding();
    let request = ModelRequest {
        options: JsonObject::new(),
        request_id: id("step"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference("primary"),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id("requested-model"),
            model_id: id("resolved-model"),
            model_version: id("resolved-release"),
            version_semantics: VersionSemantics::Pinned,
            provider: binding.provider,
            target: JsonObject::new(),
            deployment_revision: None,
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("v1"),
            },
            adapter: binding.adapter,
            capability_revision: id("capability-1"),
            connection_ref: binding.connection_ref,
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find reports".into(),
            }],
        }],
        tools: vec![compiled.to_model_tool()],
        output: ModelOutput::Text {},
        max_output_tokens: 64.try_into().unwrap(),
        limits: ModelResponseLimits {
            max_input_bytes: 16384,
            max_response_bytes: 4096,
            max_delta_bytes: 1024,
            max_events: 4,
            max_tool_calls: 1,
        },
    };
    let Guarded::Completed(ModelExchangeOutcome::Completed { response }) =
        ModelExchange::new(Arc::new(Model), policy.clone())
            .generate(&request, &context, &budget)
            .await
            .unwrap()
    else {
        panic!("fixture model was not completed");
    };
    let proposal = &response.tool_calls[0];
    let call = ToolCall {
        call_id: id("call"),
        model_request_id: response.request_id.clone(),
        provider_call_id: proposal.provider_call_id.clone(),
        tool_name: proposal.name.clone(),
        model_inputs: proposal.model_inputs.clone(),
        descriptor_digest: Some(compiled.descriptor_digest().clone()),
        bound_input_ref: None,
    };
    let planned = ProtectedRecord::new(id("planned-call"), 1, serde_json::to_value(&call).unwrap());
    let saved = store.load(&scope(), &id("run")).await.unwrap();
    let mut update = core::prepared(&saved.snapshot, lease.clone(), 0);
    update.snapshot.phase = RunPhase::Tool;
    update.snapshot.tool_ledger.push(ToolLedgerEntry {
        call: call.clone(),
        state: ToolCallState::Planned {},
    });
    update.snapshot.last_event_seq += 1;
    update.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        update.snapshot.last_event_seq,
        RunEventPayload::ToolPlanned {
            call_ref: planned.reference().clone(),
        },
    ));
    update.messages.push(Message {
        message_id: id("call-message"),
        run_id: id("run"),
        sequence: 2.try_into().unwrap(),
        role: MessageRole::Assistant,
        origin: MessageOrigin::Model,
        visibility: Visibility::UserAndModel,
        content: vec![ContentBlock::ToolCall { call: call.clone() }],
    });
    update.records.push(planned);
    store.commit(&scope(), &id("run"), update).await.unwrap();
    let bound = InputBinder::new(registry, None, policy, ids)
        .bind(&compiled, &id("call"), &context, &budget)
        .await
        .unwrap();
    let attempt = budget
        .reserve(ReservationKind::Tool {
            call_id: id("call"),
        })
        .await
        .unwrap();
    let saved = store.load(&scope(), &id("run")).await.unwrap();
    let mut dispatch = core::prepared(&saved.snapshot, lease.clone(), 0);
    dispatch.snapshot.phase = RunPhase::Tool;
    dispatch.snapshot.tool_ledger[0].state = ToolCallState::Dispatching {
        attempt_id: attempt.attempt_id,
        idempotency_key: id("effect-key"),
    };
    store.commit(&scope(), &id("run"), dispatch).await.unwrap();
    let receipt = ProtectedRecord::new(id("receipt"), 1, json!({"fixture_effect":"completed"}));
    let tool_result = ToolResult {
        call_id: id("call"),
        call_message_id: id("call-message"),
        status: ToolResultStatus::Succeeded,
        effect: ToolEffect::Applied,
        content: vec![InputContent::Text {
            text: "Observed reports".into(),
        }],
        effect_receipt_ref: Some(receipt.reference().clone()),
        error: None,
    };
    let result_record = ProtectedRecord::new(
        id("tool-result"),
        1,
        serde_json::to_value(&tool_result).unwrap(),
    );
    let saved = store.load(&scope(), &id("run")).await.unwrap();
    let mut settled = core::prepared(&saved.snapshot, lease.clone(), 0);
    settled.snapshot.tool_ledger[0].state = ToolCallState::Settled {
        result: tool_result.clone(),
    };
    settled.snapshot.last_event_seq += 1;
    settled.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        settled.snapshot.last_event_seq,
        RunEventPayload::ToolSettled {
            result_ref: result_record.reference().clone(),
        },
    ));
    settled.messages.push(Message {
        message_id: id("result-message"),
        run_id: id("run"),
        sequence: 3.try_into().unwrap(),
        role: MessageRole::Tool,
        origin: MessageOrigin::Tool,
        visibility: Visibility::UserAndModel,
        content: vec![ContentBlock::ToolResult {
            result: tool_result,
        }],
    });
    settled.records.extend([receipt.clone(), result_record]);
    store.commit(&scope(), &id("run"), settled).await.unwrap();
    let saved = store.load(&scope(), &id("run")).await.unwrap();
    store
        .commit(
            &scope(),
            &id("run"),
            core::finished(&saved.snapshot, lease, 0),
        )
        .await
        .unwrap();
    let saved = store.load(&scope(), &id("run")).await.unwrap();
    let references = [
        saved
            .snapshot
            .system_inputs
            .as_ref()
            .unwrap()
            .snapshot_ref
            .clone(),
        bound.reference,
        saved.snapshot.model_ledger[0].response_ref.clone().unwrap(),
        receipt.reference().clone(),
    ];
    let mut records = Vec::new();
    for reference in references {
        let record = store.read_record(&scope(), &reference).await.unwrap();
        records.push(json!({"reference":reference,"value":record.value()}));
    }
    json!({"snapshot":saved.snapshot,"session":saved.session,"messages":saved.messages,"records":records})
}
```

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
mod driver;
mod tools;

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
    runs: Mutex<BTreeMap<Id, Arc<LocalRun>>>,
}
struct LocalRun {
    cancel: CancellationToken,
    reason: Mutex<Option<Id>>,
    error: Mutex<Option<ContractError>>,
    done: AtomicBool,
    notify: Notify,
}
impl LocalRun {
    fn new() -> Self {
        Self {
            cancel: CancellationToken::new(),
            reason: Mutex::new(None),
            error: Mutex::new(None),
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

/// Validate the initial text/turn-end runtime without invoking any Host callback.
/// Catalog tools use already-created executors. Asset loaders, adapter exports,
/// verifiers and extension execution require their separate runtime bindings.
pub fn create_agent(
    profile: AgentProfile,
    bindings: AgentBindings,
) -> Result<Agent, ContractError> {
    profile.validate_structure()?;
    bindings.settings.validate()?;
    if !matches!(profile.instructions, Instructions::Text(_))
        || !matches!(profile.output_contract, OutputContract::Text {})
        || !matches!(profile.completion_policy, CompletionPolicy::TurnEnd {})
        || !profile.skills.is_empty()
        || !profile.connectors.is_empty()
        || profile.adapters.as_ref().is_some_and(|v| !v.is_empty())
        || profile.hooks.as_ref().is_some_and(|v| !v.is_empty())
        || profile
            .context_sources
            .as_ref()
            .is_some_and(|v| !v.is_empty())
        || profile.extensions.as_ref().is_some_and(|v| !v.is_empty())
        || profile.context_policy.strategy.as_str() != "bounded"
    {
        return Err(fail(ErrorCode::CapabilityUnsupported, "agent.profile"));
    }
    match &bindings.tools {
        Some(registry) => {
            if registry.scope() != &bindings.scope {
                return Err(fail(ErrorCode::AccessDenied, "agent.tools_scope"));
            }
            registry.prompt_bindings(&profile)?;
        }
        None if !profile.tools.is_empty() => {
            return Err(fail(ErrorCode::CapabilityUnsupported, "agent.tools"));
        }
        None => {}
    }
    Ok(Agent {
        inner: Arc::new(Inner {
            profile,
            bindings,
            runs: Mutex::new(BTreeMap::new()),
        }),
    })
}

/// A durable observer. Dropping this value or its streams does not cancel the driver.
#[derive(Clone)]
pub struct RunHandle {
    agent: Agent,
    run_id: Id,
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
            .run_details(&saved.snapshot, context, None)
            .await
    }
    /// Command consumption is unsupported until the explicit waiting/resume runtime is connected.
    pub async fn resume(
        &self,
        _command: ResumeCommand,
        context: ExecutionContext,
    ) -> Result<Guarded<RunHandle>, ContractError> {
        self.check_scope(&context)?;
        Err(fail(ErrorCode::CapabilityUnsupported, "agent.resume"))
    }
    fn check_scope(&self, context: &ExecutionContext) -> Result<(), ContractError> {
        if context.data.scope != self.inner.bindings.scope {
            return Err(fail(ErrorCode::AccessDenied, "scope"));
        }
        Ok(())
    }
    fn handle(&self, run_id: Id) -> Result<RunHandle, ContractError> {
        let local = self
            .inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .get(&run_id)
            .cloned();
        Ok(RunHandle {
            agent: self.clone(),
            run_id,
            local,
        })
    }
}

impl RunHandle {
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
            if let Some(outcome) = snapshot.outcome {
                return Ok(Guarded::Completed(outcome));
            }
            self.local_error()?;
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
                    if saved.snapshot.status.is_terminal()
                        || saved.snapshot.status == RunStatus::Waiting
                    {
                        if cursor >= saved.snapshot.last_event_seq {
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
                if let Some(local) = self.current_local()? {
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
        if let Some(local) = &self.local {
            return Ok(Some(local.clone()));
        }
        Ok(self
            .agent
            .inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .get(&self.run_id)
            .cloned())
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
        if serde_json::to_vec(&request)
            .map_err(|_| fail(ErrorCode::InvalidJson, "agent.request"))?
            .len()
            > bindings.settings.max_request_bytes
        {
            return Err(fail(ErrorCode::InvalidContract, "agent.request_size"));
        }
        if request
            .output_contract
            .as_ref()
            .is_some_and(|value| !matches!(value, OutputContract::Text {}))
            || request
                .input
                .iter()
                .any(|content| !matches!(content, InputContent::Text { .. }))
        {
            return Err(fail(ErrorCode::CapabilityUnsupported, "agent.request"));
        }
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
            return Ok(Guarded::Completed(self.handle(saved.snapshot.run_id)?));
        }
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
            return Ok(Guarded::Completed(
                self.handle(result.state.snapshot.run_id)?,
            ));
        }
        let run_id = result.state.snapshot.run_id;
        let local = Arc::new(LocalRun::new());
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
            let completed = error.is_none();
            if let Ok(mut saved) = driver_local.error.lock() {
                *saved = error;
            }
            driver_local.done.store(true, Ordering::Release);
            driver_local.notify.notify_waiters();
            if completed {
                if let Ok(mut runs) = agent.inner.runs.lock() {
                    runs.remove(&driver_id);
                }
            }
        });
        Ok(Guarded::Completed(RunHandle {
            agent: self.clone(),
            run_id,
            local: Some(local),
        }))
    }

    async fn validate_replay(
        &self,
        request: &RunRequest,
        context: &ExecutionContext,
        saved: &StoredRun,
    ) -> Result<(), ContractError> {
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

    async fn prepare(
        &self,
        request: RunRequest,
        context: &ExecutionContext,
    ) -> Result<(AdmissionInput, PromptSnapshot), ContractError> {
        let bindings = &self.inner.bindings;
        let routing = bindings.router.snapshot().clone();
        if routing.scope() != &bindings.scope {
            return Err(fail(ErrorCode::AccessDenied, "agent.router_scope"));
        }
        let profile = ProfileValidator::new(bindings.profile_resolver.as_ref())
            .validate(&self.inner.profile, &bindings.scope)
            .await?;
        let tool_bindings = bindings
            .tools
            .as_ref()
            .map(|tools| tools.prompt_bindings(profile.profile()))
            .transpose()?
            .unwrap_or_default();
        let session = match bindings
            .state
            .load_session(&bindings.scope, &request.session_id)
            .await
        {
            Ok(session) => Some(session),
            Err(error) if error.code == ErrorCode::StateNotFound => None,
            Err(error) => return Err(error),
        };
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
                vec![],
            )?;
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(&prompt)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.prompt"))?,
            );
            (prompt, record, 1)
        };
        if prompt.tools().len() != tool_bindings.len()
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
            assembly_ref: None,
            routing_snapshot_ref: Some(routing_record.reference().clone()),
            context_batches: vec![],
            source_states: vec![],
            revision: 0,
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
                snapshot,
                prompt_snapshot: prompt_record.reference().clone(),
                require_durable: bindings.settings.require_durable,
                messages: vec![message],
                events: vec![event],
                records: vec![request_record, prompt_record, inputs_record, routing_record],
            },
            prompt,
        ))
    }
}
```

## `crates/wickle/src/agent/driver.rs`

```rust
use super::*;
use std::panic::AssertUnwindSafe;

impl Agent {
    pub(super) async fn drive(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        context: ExecutionContext,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let bindings = &self.inner.bindings;
        let now = bindings.clock.now()?.utc_ms;
        let lease = bindings
            .state
            .acquire_lease(
                &bindings.scope,
                run_id,
                &bindings.ids.next_id()?,
                now,
                bindings.settings.lease_ttl_ms,
            )
            .await?;
        let budget = Arc::new(
            RunBudget::attach(
                bindings.state.clone(),
                bindings.clock.clone(),
                bindings.ids.clone(),
                bindings.scope.clone(),
                run_id.clone(),
                lease.clone(),
                local.cancel.clone(),
            )
            .await?,
        );
        let stop = CancellationToken::new();
        let heartbeat_agent = self.clone();
        let heartbeat_budget = budget.clone();
        let heartbeat_lease = lease.clone();
        let heartbeat_id = run_id.clone();
        let heartbeat_stop = stop.clone();
        let heartbeat_local = local.clone();
        let heartbeat = tokio::spawn(async move {
            let result = AssertUnwindSafe(heartbeat_agent.heartbeat(
                &heartbeat_id,
                heartbeat_lease,
                &heartbeat_budget,
                &heartbeat_stop,
            ))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(fail(ErrorCode::LeaseLost, "agent.heartbeat")));
            if let Err(error) = &result {
                if let Ok(mut slot) = heartbeat_local.error.lock() {
                    *slot = Some(error.clone());
                }
                heartbeat_local.cancel.cancel();
            }
            result
        });
        let result =
            AssertUnwindSafe(self.run_segment(run_id, prompt, &context, &budget, &lease, local))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| Err(fail(ErrorCode::InvalidContract, "agent.driver")));
        stop.cancel();
        let heartbeat_result = heartbeat
            .await
            .map_err(|_| fail(ErrorCode::LeaseLost, "agent.heartbeat"))?;
        // Stored completion is authoritative even if an acknowledgement or the
        // final heartbeat was lost after the terminal transaction succeeded.
        if let Ok(saved) = bindings.state.load(&bindings.scope, run_id).await {
            if saved.snapshot.status.is_terminal() {
                return Ok(());
            }
        }
        if let Ok((_, now)) = budget.settlement_time(0) {
            let _ = bindings
                .state
                .release_lease(&bindings.scope, run_id, &lease, now)
                .await;
        }
        result.and(heartbeat_result)
    }

    async fn heartbeat(
        &self,
        run_id: &Id,
        mut lease: RunLease,
        budget: &RunBudget,
        stop: &CancellationToken,
    ) -> Result<(), ContractError> {
        let bindings = &self.inner.bindings;
        loop {
            let reading = bindings.clock.now()?;
            let next = reading
                .monotonic_ms
                .checked_add(bindings.settings.heartbeat_interval_ms)
                .ok_or_else(|| fail(ErrorCode::ClockUnavailable, "agent.heartbeat"))?;
            tokio::select! { biased;
                _ = stop.cancelled() => return Ok(()),
                result = bindings.clock.sleep_until(next) => result?,
            }
            let (_, now) = budget.settlement_time(0)?;
            let renewal = bindings.state.renew_lease(
                &bindings.scope,
                run_id,
                &lease,
                now,
                bindings.settings.lease_ttl_ms,
            );
            let remaining = lease
                .expires_at_ms
                .checked_sub(now)
                .and_then(|value| u64::try_from(value).ok())
                .filter(|value| *value > 0)
                .ok_or_else(|| fail(ErrorCode::LeaseLost, "agent.heartbeat"))?;
            let result = tokio::select! { biased;
                _ = stop.cancelled() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_millis(remaining)) => Err(fail(ErrorCode::LeaseLost, "agent.heartbeat")),
                result = renewal => result,
            };
            match result {
                Ok(current) => lease = current,
                Err(error) => return Err(error),
            }
        }
    }

    async fn run_segment(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        context: &ExecutionContext,
        budget: &RunBudget,
        lease: &RunLease,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let mut waiting = None;
        let attempt = loop {
            match self
                .generate(run_id, prompt.clone(), context, budget, lease)
                .await
            {
                Ok(Guarded::Completed(ModelExchangeOutcome::Completed { response }))
                    if response.finish == ModelFinish::ToolCalls =>
                {
                    if let Err(error) = self.plan_tools(&response, &prompt, budget).await {
                        break Some(Err(error));
                    }
                    let round = self.tool_round()?;
                    match round.execute(&response.request_id, context, budget).await {
                        Ok(ToolRoundOutcome::Completed) => continue,
                        Ok(outcome) => {
                            waiting = Some(self.tool_wait(outcome, budget).await?);
                            break None;
                        }
                        Err(error) => break Some(Err(error)),
                    }
                }
                result => break Some(result),
            }
        };
        if let Some(error) = local
            .error
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .clone()
        {
            return Err(error);
        }
        if let Some((wait, unresolved_effects)) = waiting {
            return self
                .finish(
                    run_id,
                    PreparedOutcome {
                        result: OutcomeResult::Waiting { wait },
                        output: vec![],
                        continuation: vec![],
                        unresolved_effects,
                    },
                    budget,
                    context,
                    local,
                )
                .await;
        }
        let attempt = attempt.expect("non-waiting loop result");
        let mut continuation = vec![];
        let (result, output) = match attempt {
            Ok(Guarded::Completed(ModelExchangeOutcome::Completed { response }))
                if response.finish == ModelFinish::Stop && response.tool_calls.is_empty() =>
            {
                continuation = response.continuation;
                (
                    OutcomeResult::Succeeded {
                        completion_basis: CompletionBasis::TurnEnded,
                    },
                    vec![InputContent::Text {
                        text: response.text,
                    }],
                )
            }
            Ok(Guarded::Completed(ModelExchangeOutcome::Completed { response })) => (
                failed(if response.finish == ModelFinish::Refusal {
                    "model_refusal"
                } else {
                    "tool_execution_unsupported"
                }),
                vec![],
            ),
            Ok(Guarded::Completed(ModelExchangeOutcome::Failed { failure })) => (
                failed(&format!("model_{}", enum_name(&failure.kind))),
                if failure.partial_text().is_empty() {
                    vec![]
                } else {
                    vec![InputContent::Text {
                        text: failure.partial_text().to_owned(),
                    }]
                },
            ),
            Ok(Guarded::ApprovalRequired(_)) => (failed("approval_runtime_unsupported"), vec![]),
            Err(error)
                if matches!(
                    error.code,
                    ErrorCode::LeaseLost
                        | ErrorCode::RevisionConflict
                        | ErrorCode::PersistenceUnavailable
                        | ErrorCode::StateNotFound
                        | ErrorCode::ClockUnavailable
                        | ErrorCode::ClockRegression
                        | ErrorCode::InvalidTransition
                        | ErrorCode::InvalidSnapshot
                        | ErrorCode::InvalidEvent
                        | ErrorCode::RecordConflict
                ) =>
            {
                return Err(error);
            }
            Err(error) if error.code == ErrorCode::Cancelled => (
                OutcomeResult::Cancelled {
                    reason: local
                        .reason
                        .lock()
                        .map_err(|_| fail(ErrorCode::InvalidContract, "agent.cancel"))?
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "cancelled".into()),
                },
                vec![],
            ),
            Err(error) if error.code == ErrorCode::DeadlineExceeded => (
                OutcomeResult::Exhausted {
                    budget: BudgetKind::Elapsed,
                },
                vec![],
            ),
            Err(error) if error.code == ErrorCode::BudgetExceeded => {
                let kind = match error.path.as_str() {
                    "budget.model_calls" => BudgetKind::ModelCalls,
                    "budget.tool_attempts" => BudgetKind::ToolAttempts,
                    "budget.repair_attempts" => BudgetKind::RepairAttempts,
                    "budget.recovery_attempts" => BudgetKind::RecoveryAttempts,
                    _ => BudgetKind::Elapsed,
                };
                (OutcomeResult::Exhausted { budget: kind }, vec![])
            }
            Err(error) => (failed(&enum_name(&error.code)), vec![]),
        };
        self.finish(
            run_id,
            PreparedOutcome {
                result,
                output,
                continuation,
                unresolved_effects: vec![],
            },
            budget,
            context,
            local,
        )
        .await
    }

    async fn generate(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        context: &ExecutionContext,
        budget: &RunBudget,
        lease: &RunLease,
    ) -> Result<Guarded<ModelExchangeOutcome>, ContractError> {
        let bindings = &self.inner.bindings;
        budget.check_boundary().await?;
        let mut snapshot = bindings.state.load(&bindings.scope, run_id).await?.snapshot;
        let expected_revision = snapshot.revision;
        let step = bindings.ids.next_id()?;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.prepare"))?;
        snapshot.phase = RunPhase::Prepare;
        snapshot.model_step_id = Some(step.clone());
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        let saved = bindings
            .state
            .commit(
                &bindings.scope,
                run_id,
                CommitInput {
                    expected_revision,
                    lease: lease.clone(),
                    now_ms: now,
                    snapshot,
                    messages: vec![],
                    events: vec![],
                    records: vec![],
                },
            )
            .await?;
        let router = bindings.router.snapshot();
        let rule = router
            .policy()
            .rules
            .iter()
            .find(|rule| {
                rule.model_binding == saved.snapshot.profile.profile().model_binding
                    && rule.purpose == ModelPurpose::Agent
            })
            .ok_or_else(|| fail(ErrorCode::ModelRouteDenied, "agent.routing"))?;
        let input = RoutedModelInput {
            model_step_id: step,
            routing: RouteRequest {
                model_binding: saved.snapshot.profile.profile().model_binding.clone(),
                purpose: ModelPurpose::Agent,
                required_capabilities: if prompt.tools().is_empty() {
                    std::collections::BTreeSet::from([Id::new("text")?])
                } else {
                    std::collections::BTreeSet::from([Id::new("text")?, Id::new("tool_calling")?])
                },
                input_tokens: 0,
                max_output_tokens: bindings.settings.max_output_tokens,
                options: saved.snapshot.request.model_options.clone(),
                scope: bindings.scope.clone(),
                allowed_bindings: std::iter::once(&rule.primary)
                    .chain(&rule.fallbacks)
                    .map(|binding| binding.id.clone())
                    .collect(),
                version_policy: rule.version_policy,
                previous_route: None,
                previous_failure: None,
            },
        };
        let projector = Projector {
            saved,
            prompt,
            settings: bindings.settings.clone(),
            estimator: bindings.token_estimator.clone(),
            state: bindings.state.clone(),
        };
        bindings
            .model_exchange
            .generate_routed(
                bindings.router.as_ref(),
                &input,
                &projector,
                context,
                budget,
            )
            .await
    }

    async fn finish(
        &self,
        run_id: &Id,
        candidate: PreparedOutcome,
        budget: &RunBudget,
        context: &ExecutionContext,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let PreparedOutcome {
            mut result,
            mut output,
            continuation,
            unresolved_effects,
        } = candidate;
        let bindings = &self.inner.bindings;
        let lease = budget.lease();
        let mut saved = bindings.state.load(&bindings.scope, run_id).await?;
        if saved.snapshot.status.is_terminal() {
            return Ok(());
        }
        if output.is_empty() && !matches!(result, OutcomeResult::Succeeded { .. }) {
            output = self.saved_partial_output(&saved.snapshot).await?;
        }
        // Finalization remains possible after cancellation/deadline, but only
        // under the stored lease. A stop during these reads also closes untouched
        // plans; it never invents a result for an uncertain dispatched operation.
        let mut cleaned = false;
        let (elapsed, now) = loop {
            let (_, check_at) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
            let current_lease = bindings
                .state
                .check_lease(&bindings.scope, run_id, lease, check_at)
                .await?;
            let (elapsed, now) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
            if now >= current_lease.expires_at_ms {
                return Err(fail(ErrorCode::LeaseLost, "agent.finish"));
            }
            if matches!(
                result,
                OutcomeResult::Succeeded { .. } | OutcomeResult::Waiting { .. }
            ) {
                if local.cancel.is_cancelled() {
                    result = OutcomeResult::Cancelled {
                        reason: local
                            .reason
                            .lock()
                            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.cancel"))?
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "cancelled".into()),
                    };
                } else if elapsed >= saved.snapshot.limits.max_elapsed_ms.get() {
                    result = OutcomeResult::Exhausted {
                        budget: BudgetKind::Elapsed,
                    };
                }
            }
            if !matches!(
                result,
                OutcomeResult::Succeeded { .. } | OutcomeResult::Waiting { .. }
            ) && saved.snapshot.tool_ledger.iter().any(|entry| {
                matches!(
                    entry.state,
                    ToolCallState::Planned {} | ToolCallState::ApprovalPending { .. }
                )
            }) {
                if cleaned {
                    return Err(fail(ErrorCode::InvalidTransition, "agent.pending_tools"));
                }
                self.settle_unstarted_tools(
                    &saved.snapshot,
                    context,
                    budget,
                    matches!(result, OutcomeResult::Cancelled { .. }),
                )
                .await?;
                saved = bindings.state.load(&bindings.scope, run_id).await?;
                cleaned = true;
                continue;
            }
            break (elapsed, now);
        };
        let mut snapshot = saved.snapshot;
        let expected_revision = snapshot.revision;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.finish"))?;
        snapshot.last_event_seq = snapshot
            .last_event_seq
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.event"))?;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        snapshot.status = result.status();
        snapshot.phase = if snapshot.status == RunStatus::Waiting {
            RunPhase::Waiting
        } else {
            RunPhase::Finish
        };
        snapshot.wait = if let OutcomeResult::Waiting { wait } = &result {
            Some(wait.clone())
        } else {
            None
        };
        if let OutcomeResult::Failed { failure } = &mut result {
            failure.diagnostic_ref = snapshot
                .model_ledger
                .last()
                .and_then(|entry| entry.response_ref.clone());
        }
        let outcome = RunOutcome {
            result,
            output: output.clone(),
            artifacts: vec![],
            usage: snapshot.usage.clone(),
            checkpoint_revision: snapshot.revision,
            verification: None,
            unresolved_effects,
        };
        let record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(&outcome)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.outcome"))?,
        );
        let wait_record = snapshot
            .wait
            .as_ref()
            .map(|wait| {
                Ok::<_, ContractError>(ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(wait)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.wait"))?,
                ))
            })
            .transpose()?;
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: bindings.ids.next_id()?,
            scope: bindings.scope.clone(),
            run_id: run_id.clone(),
            session_id: snapshot.request.session_id.clone(),
            seq: snapshot
                .last_event_seq
                .try_into()
                .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.event"))?,
            timestamp_ms: now,
            payload: if let Some(wait_record) = &wait_record {
                RunEventPayload::RunWaiting {
                    wait_ref: wait_record.reference().clone(),
                }
            } else {
                RunEventPayload::RunFinished {
                    outcome_ref: record.reference().clone(),
                }
            },
        };
        let mut records = vec![record];
        records.extend(wait_record);
        let mut content: Vec<_> = output
            .into_iter()
            .map(|content| ContentBlock::Content { content })
            .collect();
        if snapshot.status == RunStatus::Succeeded {
            for continuation in continuation {
                let route = &snapshot
                    .model_ledger
                    .last()
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.continuation"))?
                    .route;
                if continuation.route_digest() != &route.digest() {
                    return Err(fail(
                        ErrorCode::ModelContextIncompatible,
                        "agent.continuation",
                    ));
                }
                let record = ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(&continuation)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.continuation"))?,
                );
                content.push(ContentBlock::ProviderOpaque {
                    provider: route.provider.clone(),
                    route_digest: route.digest(),
                    data_ref: record.reference().clone(),
                });
                records.push(record);
            }
        }
        let messages = if content.is_empty() || snapshot.status != RunStatus::Succeeded {
            vec![]
        } else {
            vec![Message {
                message_id: bindings.ids.next_id()?,
                run_id: run_id.clone(),
                sequence: saved
                    .session
                    .transcript_revision
                    .checked_add(1)
                    .and_then(NonZeroU64::new)
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "message.sequence"))?,
                role: MessageRole::Assistant,
                content,
                origin: MessageOrigin::Model,
                visibility: Visibility::UserAndModel,
            }]
        };
        snapshot.outcome = Some(outcome);
        bindings
            .state
            .commit(
                &bindings.scope,
                run_id,
                CommitInput {
                    expected_revision,
                    lease: lease.clone(),
                    now_ms: now,
                    snapshot,
                    messages,
                    events: vec![event],
                    records,
                },
            )
            .await?;
        local.notify.notify_waiters();
        Ok(())
    }

    async fn saved_partial_output(
        &self,
        snapshot: &RunSnapshot,
    ) -> Result<Vec<InputContent>, ContractError> {
        let Some(step) = &snapshot.model_step_id else {
            return Ok(vec![]);
        };
        let Some(invocation) = snapshot.model_ledger.iter().rev().find(|invocation| {
            &invocation.model_step_id == step
                && invocation.run_id == snapshot.run_id
                && invocation.response_ref.is_some()
        }) else {
            return Ok(vec![]);
        };
        let reference = invocation
            .response_ref
            .as_ref()
            .expect("filtered response reference");
        let record = self
            .inner
            .bindings
            .state
            .read_record(&snapshot.scope, reference)
            .await?;
        if record.reference() != reference {
            return Err(fail(ErrorCode::InvalidSnapshot, "agent.partial_response"));
        }
        let response: StoredModelResponse = serde_json::from_value(record.value().clone())
            .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.partial_response"))?;
        if response.request_id != invocation.attempt_id
            || response.route_digest != invocation.route.digest()
        {
            return Err(fail(ErrorCode::InvalidSnapshot, "agent.partial_response"));
        }
        let text = match response.outcome {
            ModelExchangeOutcome::Completed { response } => response.text,
            ModelExchangeOutcome::Failed { failure } => failure.partial_text().to_owned(),
        };
        Ok(if text.is_empty() {
            vec![]
        } else {
            vec![InputContent::Text { text }]
        })
    }
}

struct PreparedOutcome {
    result: OutcomeResult,
    output: Vec<InputContent>,
    continuation: Vec<OpaqueContinuation>,
    unresolved_effects: Vec<RecordRef>,
}

struct Projector {
    saved: StoredRun,
    prompt: PromptSnapshot,
    settings: AgentSettings,
    estimator: Arc<dyn ModelTokenEstimator>,
    state: Arc<dyn StateStore>,
}
impl ModelRequestProjector for Projector {
    fn project<'a>(
        &'a self,
        selection: &'a RouteSelection,
        input: &'a RoutedModelInput,
        context: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ProjectedModelRequest> {
        Box::pin(async move {
            if context.cancellation.is_cancelled() {
                return Err(fail(ErrorCode::Cancelled, "agent.projection"));
            }
            let request_message = self
                .saved
                .messages
                .iter()
                .find(|message| {
                    message.run_id == self.saved.snapshot.run_id
                        && message.role == MessageRole::User
                })
                .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.request_message"))?;
            let mut opaque_records: Vec<ScopedOpaque> = vec![];
            for message in &self.saved.messages {
                if !matches!(
                    message.visibility,
                    Visibility::Model | Visibility::UserAndModel
                ) {
                    continue;
                }
                for content in &message.content {
                    if let ContentBlock::ProviderOpaque {
                        provider,
                        route_digest,
                        data_ref,
                    } = content
                    {
                        if provider != &selection.route.provider
                            || route_digest != &selection.route.digest()
                        {
                            return Err(fail(
                                ErrorCode::ModelContextIncompatible,
                                "agent.opaque_route",
                            ));
                        }
                        if opaque_records
                            .iter()
                            .any(|record| &record.reference == data_ref)
                        {
                            continue;
                        }
                        let record = self.state.read_record(&context.scope, data_ref).await?;
                        if record.reference() != data_ref {
                            return Err(fail(
                                ErrorCode::ModelContextIncompatible,
                                "agent.opaque_record",
                            ));
                        }
                        let continuation: OpaqueContinuation =
                            serde_json::from_value(record.value().clone()).map_err(|_| {
                                fail(ErrorCode::ModelContextIncompatible, "agent.opaque_record")
                            })?;
                        if continuation.route_digest() != route_digest
                            || canonical_digest(record.value()) != data_ref.digest
                        {
                            return Err(fail(
                                ErrorCode::ModelContextIncompatible,
                                "agent.opaque_record",
                            ));
                        }
                        opaque_records.push(ScopedOpaque {
                            scope: context.scope.clone(),
                            reference: data_ref.clone(),
                            provider: provider.clone(),
                            continuation,
                        });
                    }
                }
            }
            let projection = ContextAssembler::new().project(
                &self.prompt,
                ProjectionInput {
                    profile: &self.saved.snapshot.profile,
                    scope: &context.scope,
                    run_id: &self.saved.snapshot.run_id,
                    model_step_id: &input.model_step_id,
                    current_request: &self.saved.snapshot.request,
                    current_request_message_id: &request_message.message_id,
                    transcript: &self.saved.messages,
                    context_items: &[],
                    opaque_records: &opaque_records,
                    expected_prompt_digest: &self.saved.session.prompt_snapshot.digest,
                    request_id: input.model_step_id.clone(),
                    purpose: input.routing.purpose,
                    route: selection.route.clone(),
                    output: ModelOutput::Text {},
                    max_output_tokens: self.settings.max_output_tokens,
                    options: input.routing.options.clone(),
                    response_limits: self.settings.response_limits.clone(),
                    limits: self.settings.projection_limits,
                },
            )?;
            let input_tokens = self.estimator.estimate(&projection.request)?;
            Ok(ProjectedModelRequest {
                request: projection.request,
                input_tokens,
            })
        })
    }
}
fn enum_name(value: &impl serde::Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "invalid_contract".into())
}
fn failed(code: &str) -> OutcomeResult {
    OutcomeResult::Failed {
        failure: Failure {
            code: Id::new(code).expect("nonempty static classification"),
            diagnostic_ref: None,
        },
    }
}
```

## `crates/wickle/src/agent/tools.rs`

```rust
use super::*;

impl Agent {
    pub(super) fn tool_round(&self) -> Result<SerialToolRound, ContractError> {
        let bindings = &self.inner.bindings;
        let registry = match &bindings.tools {
            Some(registry) => registry.clone(),
            None => Arc::new(ToolRegistry::new(bindings.scope.clone(), vec![])?),
        };
        let binder = Arc::new(InputBinder::new(
            Arc::new(bindings.system_inputs.clone()),
            bindings.system_input_resolver.clone(),
            bindings.policy.clone(),
            bindings.ids.clone(),
        ));
        SerialToolRound::new(
            registry,
            binder,
            bindings.policy.clone(),
            bindings.ids.clone(),
        )
        .with_limits(bindings.settings.tool_execution_limits)
    }

    /// Commit the original complete model plan before any resolver or tool runs.
    pub(super) async fn plan_tools(
        &self,
        response: &ModelResponse,
        prompt: &PromptSnapshot,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        budget.check_boundary().await?;
        let bindings = &self.inner.bindings;
        let saved = bindings.state.load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        if response.finish != ModelFinish::ToolCalls
            || response.tool_calls.is_empty()
            || snapshot
                .tool_ledger
                .iter()
                .any(|entry| entry.call.model_request_id == response.request_id)
        {
            return Err(fail(ErrorCode::InvalidTransition, "agent.tool_plan"));
        }
        let invocation = snapshot
            .model_ledger
            .iter()
            .find(|invocation| {
                invocation.attempt_id == response.request_id
                    && matches!(invocation.state, ModelAttemptState::Completed {})
            })
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.tool_response"))?;
        if invocation.route.digest() != response.route_digest {
            return Err(fail(ErrorCode::ModelRoutingMismatch, "agent.tool_response"));
        }
        let provider = invocation.route.provider.clone();
        let route_digest = invocation.route.digest();
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let mut content = Vec::new();
        if !response.text.is_empty() {
            content.push(ContentBlock::Content {
                content: InputContent::Text {
                    text: response.text.clone(),
                },
            });
        }
        let mut records = vec![];
        let mut events = vec![];
        for proposed in &response.tool_calls {
            let descriptor_digest = prompt
                .tools()
                .iter()
                .find(|tool| tool.model_tool.name == proposed.name)
                .map(|tool| tool.descriptor_digest.clone());
            let call = ToolCall {
                call_id: bindings.ids.next_id()?,
                model_request_id: response.request_id.clone(),
                provider_call_id: proposed.provider_call_id.clone(),
                tool_name: proposed.name.clone(),
                model_inputs: proposed.model_inputs.clone(),
                descriptor_digest,
                bound_input_ref: None,
            };
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(&call)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.tool_plan"))?,
            );
            snapshot.last_event_seq = snapshot
                .last_event_seq
                .checked_add(1)
                .ok_or_else(|| fail(ErrorCode::InvalidEvent, "agent.tool_plan"))?;
            events.push(RunEvent {
                schema_version: RunEventSchemaVersion::V1,
                event_id: bindings.ids.next_id()?,
                scope: budget.scope().clone(),
                run_id: budget.run_id().clone(),
                session_id: snapshot.request.session_id.clone(),
                seq: snapshot
                    .last_event_seq
                    .try_into()
                    .map_err(|_| fail(ErrorCode::InvalidEvent, "agent.tool_plan"))?,
                timestamp_ms: now,
                payload: RunEventPayload::ToolPlanned {
                    call_ref: record.reference().clone(),
                },
            });
            records.push(record);
            content.push(ContentBlock::ToolCall { call: call.clone() });
            snapshot.tool_ledger.push(ToolLedgerEntry {
                call,
                state: ToolCallState::Planned {},
            });
        }
        for continuation in &response.continuation {
            if continuation.route_digest() != &route_digest {
                return Err(fail(
                    ErrorCode::ModelContextIncompatible,
                    "agent.continuation",
                ));
            }
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(continuation)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.continuation"))?,
            );
            content.push(ContentBlock::ProviderOpaque {
                provider: provider.clone(),
                route_digest: route_digest.clone(),
                data_ref: record.reference().clone(),
            });
            records.push(record);
        }
        let message = Message {
            message_id: bindings.ids.next_id()?,
            run_id: budget.run_id().clone(),
            sequence: saved
                .session
                .transcript_revision
                .checked_add(1)
                .and_then(NonZeroU64::new)
                .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.tool_plan"))?,
            role: MessageRole::Assistant,
            content,
            origin: MessageOrigin::Model,
            visibility: Visibility::UserAndModel,
        };
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.tool_plan"))?;
        snapshot.phase = RunPhase::Tool;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        bindings
            .state
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages: vec![message],
                    events,
                    records,
                },
            )
            .await?;
        Ok(())
    }

    pub(super) async fn tool_wait(
        &self,
        outcome: ToolRoundOutcome,
        budget: &RunBudget,
    ) -> Result<(WaitState, Vec<RecordRef>), ContractError> {
        let bindings = &self.inner.bindings;
        let (target, unresolved) = match outcome {
            ToolRoundOutcome::ApprovalRequired {
                call_id,
                binding_digest,
                ..
            } => (
                WaitTarget::Approval {
                    target: ApprovalTarget::Tool {
                        call_id,
                        binding_digest,
                    },
                },
                vec![],
            ),
            ToolRoundOutcome::Unresolved {
                call_id,
                result_ref,
            } => {
                let saved = bindings.state.load(budget.scope(), budget.run_id()).await?;
                let entry = saved
                    .snapshot
                    .tool_ledger
                    .iter()
                    .find(|entry| entry.call.call_id == call_id)
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.unresolved_tool"))?;
                let ToolCallState::Unknown {
                    idempotency_key, ..
                } = &entry.state
                else {
                    return Err(fail(ErrorCode::InvalidTransition, "agent.unresolved_tool"));
                };
                (
                    WaitTarget::External {
                        call_id,
                        effect_key: idempotency_key.clone(),
                    },
                    vec![result_ref],
                )
            }
            ToolRoundOutcome::Completed => {
                return Err(fail(ErrorCode::InvalidTransition, "agent.tool_wait"));
            }
        };
        Ok((
            WaitState {
                wait_id: bindings.ids.next_id()?,
                target,
                expires_at_ms: Some(
                    bindings
                        .state
                        .load(budget.scope(), budget.run_id())
                        .await?
                        .snapshot
                        .timing
                        .deadline_at_ms,
                ),
            },
            unresolved,
        ))
    }

    pub(super) async fn settle_unstarted_tools(
        &self,
        snapshot: &RunSnapshot,
        context: &ExecutionContext,
        budget: &RunBudget,
        cancelled: bool,
    ) -> Result<(), ContractError> {
        let requests: std::collections::BTreeSet<_> = snapshot
            .tool_ledger
            .iter()
            .filter(|entry| {
                matches!(
                    entry.state,
                    ToolCallState::Planned {} | ToolCallState::ApprovalPending { .. }
                )
            })
            .map(|entry| entry.call.model_request_id.clone())
            .collect();
        if requests.is_empty() {
            return Ok(());
        }
        let round = self.tool_round()?;
        for request in requests {
            round
                .settle_unstarted(
                    &request,
                    if cancelled {
                        ToolResultStatus::Cancelled
                    } else {
                        ToolResultStatus::Failed
                    },
                    Id::new(if cancelled {
                        "cancelled"
                    } else {
                        "run_stopped"
                    })?,
                    context,
                    budget,
                )
                .await?;
        }
        Ok(())
    }
}
```

## `crates/wickle/src/context_projection.rs`

```rust
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, io,
    num::NonZeroU64,
};

use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Value, json};

use crate::{
    AgentProfile, CompiledTool, ComponentKind, ContentBlock, ContractError, ErrorCode, Id,
    InputContent, Instructions, JsonDigest, JsonObject, Message, MessageOrigin, MessageRole,
    ModelContent, ModelMessage, ModelOutput, ModelPurpose, ModelRequest, ModelResponseLimits,
    ModelRole, ModelTool, OpaqueContinuation, RecordRef, ResolvedComponent, ResolvedModelRoute,
    ResolvedProfile, RunRequest, Scope, ToolBindingRef, ToolResultStatus, VersionedRef, Visibility,
    parse_json, serialization::data_digest,
};

/// Version of the session prefix and byte-bounded projection contract.
pub const CONTEXT_ASSEMBLER_VERSION: &str = "wickle.context-assembler.v1";

/// Instruction data already resolved and authorized by the Host; no loader is invoked here.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstructionAssetContent {
    /// Exact instruction asset selected by the profile.
    pub asset: VersionedRef,
    /// Complete text to pin; it is never silently truncated.
    pub text: String,
}

impl fmt::Debug for InstructionAssetContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InstructionAssetContent")
            .field("asset", &self.asset)
            .finish_non_exhaustive()
    }
}

/// A trusted assembly's mapping from a selected profile reference to its compiled tool.
/// The later adapter factory must attest that an export actually supplies this descriptor.
#[derive(Debug, Clone)]
pub struct PromptToolBinding {
    /// Exact selected catalog reference or adapter export, including alias/configuration.
    pub selection: ToolBindingRef,
    /// Validated immutable input split; its full schema is not copied into the prefix.
    pub compiled: CompiledTool,
}

/// Initial skill listing metadata, deliberately separate from skill body loading.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillManifest {
    /// Exact selected skill identity and version.
    pub skill: VersionedRef,
    /// Short public listing name.
    pub name: String,
    /// Public purpose description, not an automatically executed instruction body.
    pub description: String,
    /// Trusted catalog manifest identity pinned with this listing.
    pub manifest_digest: JsonDigest,
}

impl fmt::Debug for SkillManifest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SkillManifest")
            .field("skill", &self.skill)
            .field("manifest_digest", &self.manifest_digest)
            .finish_non_exhaustive()
    }
}

/// Model-facing part of a tool pinned into the session prefix.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedPromptTool {
    /// Exact profile selection, retaining alias and binding identity.
    pub selection: ToolBindingRef,
    /// Exact underlying tool descriptor identity.
    pub tool: VersionedRef,
    /// Compiler contract used for input projection.
    pub compiler_version: String,
    /// Full compiled input-contract digest, without its hidden schemas or values.
    pub compiled_digest: JsonDigest,
    /// Original descriptor identity used by stored core ToolCall records.
    pub descriptor_digest: JsonDigest,
    /// Identity of the derived model-input schema.
    pub model_schema_digest: JsonDigest,
    /// Only the model-visible tool schema and public description.
    pub model_tool: ModelTool,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PromptData {
    assembler_version: String,
    scope: Scope,
    profile: AgentProfile,
    initial_resolution_digest: JsonDigest,
    pinned_components: Vec<ResolvedComponent>,
    host_instructions: Vec<String>,
    profile_asset: Option<InstructionAssetContent>,
    tools: Vec<PinnedPromptTool>,
    skills: Vec<SkillManifest>,
}

/// Owned session prefix. It can be serialized for protected storage but cannot be
/// deserialized without verifying a trusted expected digest, scope and profile.
/// Its digest equals the digest of the serialized value stored by ProtectedRecord.
#[derive(Clone)]
pub struct PromptSnapshot {
    data: PromptData,
    digest: JsonDigest,
}

impl Serialize for PromptSnapshot {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.data.serialize(serializer)
    }
}
impl fmt::Debug for PromptSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PromptSnapshot")
            .field("digest", &self.digest)
            .field("tool_count", &self.data.tools.len())
            .field("skill_count", &self.data.skills.len())
            .finish_non_exhaustive()
    }
}

impl PromptSnapshot {
    /// Pin already-authorized assets in profile order. This does not create adapter
    /// factories, fetch instructions, or load skill bodies. Profile text cannot
    /// delete or replace the independently owned Host message. Actual instruction
    /// adherence within a provider's system channel still requires evaluation;
    /// execution permissions are enforced separately by PolicyGate.
    pub fn create(
        profile: &ResolvedProfile,
        host_instructions: Vec<String>,
        profile_asset: Option<InstructionAssetContent>,
        mut tools: Vec<PromptToolBinding>,
        mut skills: Vec<SkillManifest>,
    ) -> Result<Self, ContractError> {
        if tools.len() != profile.profile().tools.len()
            || skills.len() != profile.profile().skills.len()
        {
            return Err(invalid("prompt.selections"));
        }
        let mut pinned_tools = Vec::new();
        for selection in &profile.profile().tools {
            let index = tools
                .iter()
                .position(|binding| &binding.selection == selection)
                .ok_or_else(|| invalid("prompt.tools"))?;
            let binding = tools.remove(index);
            let mut model_tool = binding.compiled.to_model_tool();
            if let ToolBindingRef::Export(export) = selection {
                if let Some(alias) = &export.alias {
                    model_tool.name = alias.clone();
                }
            }
            pinned_tools.push(PinnedPromptTool {
                selection: selection.clone(),
                tool: binding.compiled.descriptor().tool.clone(),
                compiler_version: binding.compiled.compiler_version().into(),
                compiled_digest: binding.compiled.digest().clone(),
                descriptor_digest: binding.compiled.descriptor_digest().clone(),
                model_schema_digest: binding.compiled.model_schema_digest().clone(),
                model_tool,
            });
        }
        let mut pinned_skills = Vec::new();
        for selection in &profile.profile().skills {
            let index = skills
                .iter()
                .position(|manifest| {
                    manifest.skill.id == selection.skill_id
                        && manifest.skill.version == selection.version
                })
                .ok_or_else(|| invalid("prompt.skills"))?;
            pinned_skills.push(skills.remove(index));
        }
        let data = PromptData {
            assembler_version: CONTEXT_ASSEMBLER_VERSION.into(),
            scope: profile.scope().clone(),
            profile: profile.profile().clone(),
            initial_resolution_digest: profile.resolution_digest().clone(),
            pinned_components: non_model_components(profile),
            host_instructions,
            profile_asset,
            tools: pinned_tools,
            skills: pinned_skills,
        };
        let snapshot = Self {
            digest: data_digest(&data),
            data,
        };
        snapshot.validate_data()?;
        Ok(snapshot)
    }

    /// Canonical identity of the exact protected serialized prefix.
    pub fn digest(&self) -> JsonDigest {
        self.digest.clone()
    }
    /// Read pinned public tool metadata and identities, without hidden input schemas.
    pub fn tools(&self) -> &[PinnedPromptTool] {
        &self.data.tools
    }
    /// Read the original selected skill listings, without fetching newer versions.
    pub fn skills(&self) -> &[SkillManifest] {
        &self.data.skills
    }
    /// Read the authenticated scope in which this prefix was pinned.
    pub fn scope(&self) -> &Scope {
        &self.data.scope
    }

    /// Restore a protected record using its trusted digest and the current run's
    /// resolved profile. A new run may resolve a different model binding only.
    /// Resume must continue to use the original run's profile and selected route;
    /// this method is not an authorization to replace either during a run.
    pub fn restore(
        input: &str,
        expected_digest: &JsonDigest,
        profile: &ResolvedProfile,
        scope: &Scope,
    ) -> Result<Self, ContractError> {
        let data: PromptData =
            serde_json::from_value(parse_json(input).map_err(|_| invalid("prompt"))?)
                .map_err(|_| invalid("prompt"))?;
        let snapshot = Self {
            digest: data_digest(&data),
            data,
        };
        snapshot.validate_for(profile, scope, expected_digest)?;
        Ok(snapshot)
    }

    /// Require the stored prefix identity, scope, profile, and all non-model assets.
    pub fn validate_for(
        &self,
        profile: &ResolvedProfile,
        scope: &Scope,
        expected_digest: &JsonDigest,
    ) -> Result<(), ContractError> {
        if &self.digest != expected_digest
            || &self.data.scope != scope
            || profile.scope() != scope
            || self.data.profile.digest() != *profile.profile_digest()
            || self.data.pinned_components != non_model_components(profile)
        {
            return Err(mismatch("prompt"));
        }
        self.validate_data()
    }

    fn validate_data(&self) -> Result<(), ContractError> {
        if self.data.assembler_version != CONTEXT_ASSEMBLER_VERSION
            || data_digest(&self.data) != self.digest
        {
            return Err(mismatch("prompt.version"));
        }
        match (&self.data.profile.instructions, &self.data.profile_asset) {
            (Instructions::Text(_), None) => {}
            (Instructions::Asset(reference), Some(asset)) if reference.asset_ref == asset.asset => {
            }
            _ => return Err(mismatch("prompt.instructions")),
        }
        if self.data.tools.len() != self.data.profile.tools.len()
            || self.data.skills.len() != self.data.profile.skills.len()
        {
            return Err(mismatch("prompt.selections"));
        }
        let mut names = BTreeSet::new();
        for (selection, tool) in self.data.profile.tools.iter().zip(&self.data.tools) {
            if selection != &tool.selection
                || !names.insert(&tool.model_tool.name)
                || crate::canonical_digest(&tool.model_tool.model_input_schema)
                    != tool.model_schema_digest
            {
                return Err(mismatch("prompt.tools"));
            }
            match selection {
                ToolBindingRef::Catalog(reference) => {
                    if tool.tool.id != reference.tool_id || tool.tool.version != reference.version {
                        return Err(mismatch("prompt.tools"));
                    }
                }
                ToolBindingRef::Export(export) => {
                    let adapter = self
                        .data
                        .profile
                        .adapters
                        .as_ref()
                        .and_then(|adapters| {
                            adapters
                                .iter()
                                .find(|adapter| adapter.binding_id == export.adapter_binding)
                        })
                        .ok_or_else(|| mismatch("prompt.export"))?;
                    if !self.data.pinned_components.iter().any(|component| {
                        component.reference.kind == ComponentKind::Adapter
                            && component.reference.id == adapter.adapter_id
                            && component.reference.version.as_ref() == Some(&adapter.version)
                    }) || export
                        .alias
                        .as_ref()
                        .is_some_and(|alias| alias != &tool.model_tool.name)
                    {
                        return Err(mismatch("prompt.export"));
                    }
                }
            }
        }
        for (selected, manifest) in self.data.profile.skills.iter().zip(&self.data.skills) {
            if selected.skill_id != manifest.skill.id || selected.version != manifest.skill.version
            {
                return Err(mismatch("prompt.skills"));
            }
        }
        Ok(())
    }

    fn prefix(&self) -> Vec<ModelMessage> {
        let profile_text = match &self.data.profile.instructions {
            Instructions::Text(instructions) => instructions.text.clone(),
            Instructions::Asset(_) => self
                .data
                .profile_asset
                .as_ref()
                .expect("validated instruction asset")
                .text
                .clone(),
        };
        let mut messages = vec![
            ModelMessage {
                role: ModelRole::System,
                content: self
                    .data
                    .host_instructions
                    .iter()
                    .map(|text| ModelContent::Text { text: text.clone() })
                    .collect(),
            },
            ModelMessage {
                role: ModelRole::System,
                content: vec![ModelContent::Text { text: profile_text }],
            },
        ];
        if !self.data.skills.is_empty() {
            messages.push(ModelMessage {
                role: ModelRole::User,
                content: vec![ModelContent::Json {
                    value: json!({"kind":"available_skills", "skills":self.data.skills}),
                }],
            });
        }
        messages
    }
}

fn non_model_components(profile: &ResolvedProfile) -> Vec<ResolvedComponent> {
    profile
        .components()
        .iter()
        .filter(|component| component.reference.kind != ComponentKind::ModelBinding)
        .cloned()
        .collect()
}

/// Source classification of already-authorized context data. None grants system authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextOrigin {
    /// Additional user-provided context, distinct from the preserved original request.
    User,
    /// Data associated with a selected pinned skill; no loader runs here.
    Skill,
    /// Data associated with a selected tool.
    Tool,
    /// External retrieved data, not trusted instructions.
    Retrieval,
    /// Recalled memory, not a policy grant.
    Memory,
    /// Verification feedback, not a Host instruction replacement.
    Verification,
}

/// Scope of context lifetime. An item outside its lifetime is explicitly omitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextLifetime {
    /// Context valid throughout one session.
    Session {
        /// Owning session.
        session_id: Id,
    },
    /// Context valid during one run.
    Run {
        /// Owning run.
        run_id: Id,
    },
    /// Context valid only for one logical model step.
    Step {
        /// Owning run.
        run_id: Id,
        /// Logical step, preserved across physical retries.
        model_step_id: Id,
    },
}

/// Selection importance, independent of source authority and provider role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextPriority {
    /// Fail if this active item cannot fit in full.
    Required,
    /// Include whole if remaining bounds permit it.
    Optional,
}

/// Data with explicit source, scope, integrity and lifetime. Constructing this DTO
/// does not authenticate provenance; callers must authorize sources before supply.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextItem {
    /// Stable source item identity.
    pub item_id: Id,
    /// Claimed source classification, retained in a data envelope.
    pub origin: ContextOrigin,
    /// Exact source/asset identity and version.
    pub source_ref: VersionedRef,
    /// Authenticated source scope supplied by the Host.
    pub scope: Scope,
    /// Explicitly selected content, not a system map or raw protected record.
    pub content: Vec<InputContent>,
    /// Digest of all other fields, checked again at projection.
    pub digest: JsonDigest,
    /// Session/run/step applicability.
    pub lifetime: ContextLifetime,
    /// Required versus optional selection, without elevated instruction authority.
    pub priority_class: ContextPriority,
}

impl ContextItem {
    /// Own supplied data and compute its source/lifetime/content identity.
    pub fn new(
        item_id: Id,
        origin: ContextOrigin,
        source_ref: VersionedRef,
        scope: Scope,
        content: Vec<InputContent>,
        lifetime: ContextLifetime,
        priority_class: ContextPriority,
    ) -> Self {
        let digest = data_digest(&(
            &item_id,
            origin,
            &source_ref,
            &scope,
            &content,
            &lifetime,
            priority_class,
        ));
        Self {
            item_id,
            origin,
            source_ref,
            scope,
            content,
            digest,
            lifetime,
            priority_class,
        }
    }
    fn valid_digest(&self) -> bool {
        self.digest
            == data_digest(&(
                &self.item_id,
                self.origin,
                &self.source_ref,
                &self.scope,
                &self.content,
                &self.lifetime,
                self.priority_class,
            ))
    }
}
impl fmt::Debug for ContextItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContextItem")
            .field("item_id", &self.item_id)
            .field("origin", &self.origin)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

/// Already-authorized typed provider replay data, not a generic JSON record loader.
#[derive(Debug, Clone)]
pub struct ScopedOpaque {
    /// Scope from which the protected record was read.
    pub scope: Scope,
    /// Exact reference whose digest covers the serialized OpaqueContinuation.
    pub reference: RecordRef,
    /// Provider that owns the record.
    pub provider: Id,
    /// Typed continuation with an exact route identity.
    pub continuation: OpaqueContinuation,
}

/// Finite projection size, separate from model token context capacity and usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionLimits {
    /// Maximum serialized final ModelRequest bytes, including schemas and metadata.
    pub max_bytes: usize,
    /// Maximum projected content blocks plus model tool definitions.
    pub max_items: usize,
}

/// Read-only projection inputs. Transcript must come from a trusted, scoped session
/// store; Message alone cannot authenticate its owner or prove history completeness.
pub struct ProjectionInput<'a> {
    /// Original resolved profile of this run; never re-resolve it during resume.
    pub profile: &'a ResolvedProfile,
    /// Authenticated execution scope.
    pub scope: &'a Scope,
    /// Current owning run.
    pub run_id: &'a Id,
    /// Logical step, identical to request_id before physical invocation allocation.
    pub model_step_id: &'a Id,
    /// Original persisted run request.
    pub current_request: &'a RunRequest,
    /// Exact stored user message containing that request, to prevent duplication.
    pub current_request_message_id: &'a Id,
    /// Owned-store history borrowed without mutation, including the current user message.
    pub transcript: &'a [Message],
    /// Already-authorized context items; no external source is queried here.
    pub context_items: &'a [ContextItem],
    /// Already-authorized opaque records with typed scope/provider/route metadata.
    pub opaque_records: &'a [ScopedOpaque],
    /// Trusted digest from the session's pinned prompt record.
    pub expected_prompt_digest: &'a JsonDigest,
    /// Logical step identity; ModelExchange later assigns a separate physical request ID.
    pub request_id: Id,
    /// Accounting purpose of this invocation.
    pub purpose: ModelPurpose,
    /// Already-selected immutable model route.
    pub route: ResolvedModelRoute,
    /// Already-resolved requested output mode.
    pub output: ModelOutput,
    /// Provider output-token request, not an estimate of input bytes.
    pub max_output_tokens: NonZeroU64,
    /// Host-owned logical options preserved in the final ModelRequest, outside prompt content.
    /// The selected catalog schemas and adapter define supported keys and wire mapping.
    pub options: JsonObject,
    /// Provider request/response decoding bounds.
    pub response_limits: ModelResponseLimits,
    /// Byte/item projection bounds, not a tokenizer or model context-window check.
    pub limits: ProjectionLimits,
}

/// Separate model projection and explicit selection provenance. No original messages change.
#[derive(Debug)]
pub struct ContextProjection {
    /// Complete prepared model request.
    pub request: ModelRequest,
    /// Original message identities represented in the model request.
    pub selected_message_ids: Vec<Id>,
    /// Original message identities omitted by visibility or whole-run selection.
    pub dropped_message_ids: Vec<Id>,
    /// Active supplied context items included in full.
    pub selected_context_ids: Vec<Id>,
    /// Context items omitted by lifetime or optional-item bounds.
    pub dropped_context_ids: Vec<Id>,
    /// Identity of the unchanged session prefix.
    pub prompt_digest: JsonDigest,
}

/// Prefix reuse and conservative selection without retrieval, loading or compaction.
#[derive(Debug, Clone, Copy, Default)]
pub struct ContextAssembler;

struct RunGroup {
    run_id: Id,
    messages: Vec<(Id, ModelMessage)>,
    has_tool_round: bool,
    has_unknown: bool,
}

impl ContextAssembler {
    /// Construct an assembler without doing I/O.
    pub fn new() -> Self {
        Self
    }

    /// Preserve the fixed prefix and all model-visible current-run messages. Older
    /// complete runs and optional items are added newest first without splitting
    /// tool rounds. The latest visible tool round and runs with unknown tool results
    /// are mandatory. Any unfinished round or oversized required input fails.
    /// Byte bounds do not claim to estimate or enforce provider token context size.
    pub fn project(
        &self,
        snapshot: &PromptSnapshot,
        input: ProjectionInput<'_>,
    ) -> Result<ContextProjection, ContractError> {
        snapshot.validate_for(input.profile, input.scope, input.expected_prompt_digest)?;
        if input.request_id != *input.model_step_id
            || input.limits.max_bytes == 0
            || input.limits.max_items == 0
        {
            return Err(invalid("projection.identity_or_limits"));
        }
        validate_current_request(&input)?;
        let groups = project_transcript(snapshot, &input)?;
        let current = groups
            .iter()
            .position(|group| &group.run_id == input.run_id)
            .ok_or_else(|| invalid("projection.current_run"))?;
        if current + 1 != groups.len() {
            return Err(invalid("projection.incomplete_round"));
        }
        let mut selected_groups = BTreeSet::from([current]);
        if let Some(index) = groups.iter().rposition(|group| group.has_tool_round) {
            selected_groups.insert(index);
        }
        selected_groups.extend(
            groups
                .iter()
                .enumerate()
                .filter(|(_, group)| group.has_unknown)
                .map(|(index, _)| index),
        );
        let mut context = Vec::new();
        let mut active = Vec::new();
        let mut seen_context = BTreeSet::new();
        for item in input.context_items {
            if !seen_context.insert(&item.item_id) || !item.valid_digest() {
                return Err(invalid("context_item.digest"));
            }
            if &item.scope != input.scope {
                return Err(mismatch("context_item.scope"));
            }
            if item.origin == ContextOrigin::Skill
                && !snapshot
                    .data
                    .skills
                    .iter()
                    .any(|manifest| manifest.skill == item.source_ref)
            {
                return Err(mismatch("context_item.skill"));
            }
            if item.origin == ContextOrigin::Tool
                && !snapshot
                    .data
                    .tools
                    .iter()
                    .any(|tool| tool.tool == item.source_ref)
            {
                return Err(mismatch("context_item.tool"));
            }
            let applicable = match &item.lifetime {
                ContextLifetime::Session { session_id } => {
                    session_id == &input.current_request.session_id
                }
                ContextLifetime::Run { run_id } => run_id == input.run_id,
                ContextLifetime::Step {
                    run_id,
                    model_step_id,
                } => run_id == input.run_id && model_step_id == input.model_step_id,
            };
            let message = if applicable {
                Some(ModelMessage {
                    role: ModelRole::User,
                    content: vec![ModelContent::Json {
                        value: json!({
                            "kind":"context_data", "item_id":item.item_id, "origin":item.origin,
                            "source_ref":item.source_ref,
                            "content":item.content.iter().map(|content| safe_value(content, input.scope)).collect::<Result<Vec<_>,_>>()?
                        }),
                    }],
                })
            } else {
                None
            };
            active.push(applicable);
            context.push(message);
        }
        let mut selected_context: BTreeSet<usize> = input
            .context_items
            .iter()
            .enumerate()
            .filter(|(index, item)| {
                active[*index] && item.priority_class == ContextPriority::Required
            })
            .map(|(index, _)| index)
            .collect();
        let make_request = |selected_groups: &BTreeSet<usize>,
                            selected_context: &BTreeSet<usize>| {
            let mut messages = snapshot.prefix();
            for index in selected_groups {
                messages.extend(
                    groups[*index]
                        .messages
                        .iter()
                        .map(|(_, message)| message.clone()),
                );
            }
            for index in selected_context {
                messages.push(context[*index].as_ref().expect("active context").clone());
            }
            ModelRequest {
                request_id: input.request_id.clone(),
                purpose: input.purpose,
                route: input.route.clone(),
                messages,
                tools: snapshot
                    .data
                    .tools
                    .iter()
                    .map(|tool| tool.model_tool.clone())
                    .collect(),
                output: input.output.clone(),
                max_output_tokens: input.max_output_tokens,
                options: input.options.clone(),
                limits: input.response_limits.clone(),
            }
        };
        if !fits(
            &make_request(&selected_groups, &selected_context),
            &input.limits,
        ) {
            return Err(budget());
        }
        for index in (0..current).rev() {
            if selected_groups.contains(&index) {
                continue;
            }
            selected_groups.insert(index);
            if !fits(
                &make_request(&selected_groups, &selected_context),
                &input.limits,
            ) {
                selected_groups.remove(&index);
            }
        }
        for index in (0..input.context_items.len()).rev() {
            if !active[index]
                || input.context_items[index].priority_class == ContextPriority::Required
            {
                continue;
            }
            selected_context.insert(index);
            if !fits(
                &make_request(&selected_groups, &selected_context),
                &input.limits,
            ) {
                selected_context.remove(&index);
            }
        }
        let request = make_request(&selected_groups, &selected_context);
        request
            .validate()
            .map_err(|_| invalid("projection.model_request"))?;
        let selected_message_ids: Vec<_> = selected_groups
            .iter()
            .flat_map(|index| groups[*index].messages.iter().map(|(id, _)| id.clone()))
            .collect();
        let selected_ids: BTreeSet<_> = selected_message_ids.iter().collect();
        Ok(ContextProjection {
            request,
            dropped_message_ids: input
                .transcript
                .iter()
                .filter(|message| !selected_ids.contains(&message.message_id))
                .map(|message| message.message_id.clone())
                .collect(),
            selected_message_ids,
            selected_context_ids: selected_context
                .iter()
                .map(|index| input.context_items[*index].item_id.clone())
                .collect(),
            dropped_context_ids: input
                .context_items
                .iter()
                .enumerate()
                .filter(|(index, _)| !selected_context.contains(index))
                .map(|(_, item)| item.item_id.clone())
                .collect(),
            prompt_digest: snapshot.digest(),
        })
    }
}

fn validate_current_request(input: &ProjectionInput<'_>) -> Result<(), ContractError> {
    let message = input
        .transcript
        .iter()
        .find(|message| &message.message_id == input.current_request_message_id)
        .ok_or_else(|| invalid("projection.current_request"))?;
    if &message.run_id != input.run_id
        || message.role != MessageRole::User
        || message.origin != MessageOrigin::User
        || !visible(message)
    {
        return Err(invalid("projection.current_request"));
    }
    let contents: Option<Vec<_>> = message
        .content
        .iter()
        .map(|block| match block {
            ContentBlock::Content { content } => Some(content),
            _ => None,
        })
        .collect();
    if contents.as_deref()
        != Some(
            input
                .current_request
                .input
                .iter()
                .collect::<Vec<_>>()
                .as_slice(),
        )
    {
        return Err(mismatch("projection.current_request"));
    }
    Ok(())
}

struct PendingCall {
    message_id: Id,
    provider_call_id: Id,
    visible: bool,
    known: bool,
}

fn project_transcript(
    snapshot: &PromptSnapshot,
    input: &ProjectionInput<'_>,
) -> Result<Vec<RunGroup>, ContractError> {
    let mut groups = Vec::new();
    let mut seen_messages = BTreeSet::new();
    let mut seen_runs = BTreeSet::new();
    let mut previous_sequence = 0;
    let mut cursor = 0;
    while cursor < input.transcript.len() {
        let run_id = input.transcript[cursor].run_id.clone();
        if !seen_runs.insert(run_id.clone()) {
            return Err(invalid("transcript.run_order"));
        }
        let end = input.transcript[cursor..]
            .iter()
            .position(|message| message.run_id != run_id)
            .map_or(input.transcript.len(), |offset| cursor + offset);
        let mut projected = Vec::new();
        let mut has_tool_round = false;
        let mut has_unknown = false;
        let mut pending: BTreeMap<Id, PendingCall> = BTreeMap::new();
        let mut seen_calls = BTreeSet::new();
        for message in &input.transcript[cursor..end] {
            if message.sequence.get() <= previous_sequence
                || !seen_messages.insert(&message.message_id)
            {
                return Err(invalid("transcript.order"));
            }
            previous_sequence = message.sequence.get();
            let is_visible = visible(message);
            if is_visible {
                match message.role {
                    MessageRole::System => return Err(invalid("transcript.system_role")),
                    MessageRole::Assistant if message.origin != MessageOrigin::Model => {
                        return Err(invalid("transcript.origin"));
                    }
                    MessageRole::Tool if message.origin != MessageOrigin::Tool => {
                        return Err(invalid("transcript.origin"));
                    }
                    MessageRole::User
                        if matches!(
                            message.origin,
                            MessageOrigin::Host
                                | MessageOrigin::Profile
                                | MessageOrigin::Model
                                | MessageOrigin::Tool
                        ) =>
                    {
                        return Err(invalid("transcript.origin"));
                    }
                    _ => {}
                }
                if !pending.is_empty() && message.role != MessageRole::Tool {
                    return Err(invalid("transcript.incomplete_round"));
                }
            }
            let mut content = Vec::new();
            for block in &message.content {
                match block {
                    ContentBlock::ToolCall { call } => {
                        has_tool_round |= is_visible;
                        if message.role != MessageRole::Assistant
                            || message.origin != MessageOrigin::Model
                            || !seen_calls.insert(&call.call_id)
                        {
                            return Err(invalid("transcript.tool_call"));
                        }
                        let tool = snapshot
                            .data
                            .tools
                            .iter()
                            .find(|tool| tool.model_tool.name == call.tool_name);
                        if tool.is_some_and(|tool| {
                            Some(&tool.descriptor_digest) != call.descriptor_digest.as_ref()
                        }) {
                            return Err(mismatch("transcript.descriptor"));
                        }
                        pending.insert(
                            call.call_id.clone(),
                            PendingCall {
                                message_id: message.message_id.clone(),
                                provider_call_id: call.provider_call_id.clone(),
                                visible: is_visible,
                                known: tool.is_some(),
                            },
                        );
                        if is_visible {
                            content.push(ModelContent::ToolCall {
                                provider_call_id: call.provider_call_id.clone(),
                                name: call.tool_name.clone(),
                                arguments: call.model_inputs.clone(),
                            });
                        }
                    }
                    ContentBlock::ToolResult { result } => {
                        if message.role != MessageRole::Tool
                            || message.origin != MessageOrigin::Tool
                        {
                            return Err(invalid("transcript.tool_result"));
                        }
                        let call = pending
                            .remove(&result.call_id)
                            .ok_or_else(|| invalid("transcript.tool_result"))?;
                        if call.message_id != result.call_message_id
                            || call.visible != is_visible
                            || (!call.known && result.status == ToolResultStatus::Succeeded)
                        {
                            return Err(invalid("transcript.tool_pair"));
                        }
                        if result.status == ToolResultStatus::Unknown
                            || result.effect == crate::ToolEffect::Unknown
                        {
                            if !is_visible {
                                return Err(invalid("transcript.hidden_unknown_effect"));
                            }
                            has_unknown = true;
                        }
                        if is_visible {
                            let values = result
                                .content
                                .iter()
                                .map(|item| safe_value(item, input.scope))
                                .collect::<Result<Vec<_>, _>>()?;
                            let mut value = json!({"status":result.status,"effect":result.effect,"content":values});
                            if let Some(failure) = &result.error {
                                value["error"] = json!({"code":failure.code});
                            }
                            content.push(ModelContent::ToolResult {
                                provider_call_id: call.provider_call_id,
                                content: value,
                            });
                        }
                    }
                    ContentBlock::Content { content: item } if is_visible => {
                        if message.role == MessageRole::Tool {
                            return Err(invalid("transcript.tool_result"));
                        }
                        content.push(safe_content(item, input.scope)?);
                    }
                    ContentBlock::ProviderOpaque {
                        provider,
                        route_digest,
                        data_ref,
                    } if is_visible => {
                        if message.role != MessageRole::Assistant
                            || message.origin != MessageOrigin::Model
                            || provider != &input.route.provider
                            || route_digest != &input.route.digest()
                        {
                            return Err(mismatch("transcript.opaque_route"));
                        }
                        let records: Vec<_> = input
                            .opaque_records
                            .iter()
                            .filter(|record| &record.reference == data_ref)
                            .collect();
                        if records.len() != 1 {
                            return Err(mismatch("transcript.opaque_record"));
                        }
                        let record = records[0];
                        if &record.scope != input.scope
                            || &record.provider != provider
                            || record.continuation.route_digest() != route_digest
                            || data_digest(&record.continuation) != data_ref.digest
                        {
                            return Err(mismatch("transcript.opaque_record"));
                        }
                        content.push(ModelContent::Opaque {
                            continuation: record.continuation.clone(),
                        });
                    }
                    _ => {}
                }
            }
            if is_visible && !content.is_empty() {
                let role = match message.role {
                    MessageRole::User => ModelRole::User,
                    MessageRole::Assistant => ModelRole::Assistant,
                    MessageRole::Tool => ModelRole::Tool,
                    MessageRole::System => unreachable!("visible System rejected"),
                };
                if message.role == MessageRole::User && message.origin != MessageOrigin::User {
                    let values = content
                        .iter()
                        .map(|content| {
                            serde_json::to_value(content).expect("model content serialization")
                        })
                        .collect::<Vec<_>>();
                    content = vec![ModelContent::Json {
                        value: json!({"kind":"transcript_data", "origin":message.origin,
                        "source_message_id":message.message_id, "content":values}),
                    }];
                }
                projected.push((message.message_id.clone(), ModelMessage { role, content }));
            }
        }
        if !pending.is_empty() {
            return Err(invalid("transcript.incomplete_round"));
        }
        groups.push(RunGroup {
            run_id,
            messages: projected,
            has_tool_round,
            has_unknown,
        });
        cursor = end;
    }
    Ok(groups)
}

fn visible(message: &Message) -> bool {
    matches!(
        message.visibility,
        Visibility::Model | Visibility::UserAndModel
    )
}

fn safe_content(content: &InputContent, scope: &Scope) -> Result<ModelContent, ContractError> {
    match content {
        InputContent::Text { text } => Ok(ModelContent::Text { text: text.clone() }),
        InputContent::Json { value } => Ok(ModelContent::Json {
            value: value.clone(),
        }),
        _ => Ok(ModelContent::Json {
            value: safe_value(content, scope)?,
        }),
    }
}

fn safe_value(content: &InputContent, scope: &Scope) -> Result<Value, ContractError> {
    Ok(match content {
        InputContent::Text { text } => json!({"type":"text","text":text}),
        InputContent::Json { value } => json!({"type":"json","value":value}),
        InputContent::Artifact { reference } => {
            if &reference.scope != scope {
                return Err(mismatch("context.artifact_scope"));
            }
            json!({"type":"artifact","artifact_id":reference.artifact_id,"media_type":reference.media_type,
                "size_bytes":reference.size_bytes,"content_hash":reference.content_hash})
        }
        InputContent::Evidence { reference } => {
            let mut value = json!({"type":"evidence","source_id":reference.source_id,"version":reference.version,
                "location":reference.location,"content_hash":reference.content_hash});
            if let Some(quote) = &reference.quote {
                value["quote"] = json!(quote);
            }
            value
        }
    })
}

struct ByteCounter {
    written: usize,
    limit: usize,
}
impl io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.written = self
            .written
            .checked_add(bytes.len())
            .filter(|size| *size <= self.limit)
            .ok_or_else(|| io::Error::other("projection limit exceeded"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn fits(request: &ModelRequest, limits: &ProjectionLimits) -> bool {
    let count = request
        .messages
        .iter()
        .try_fold(request.tools.len(), |count, message| {
            count.checked_add(message.content.len())
        });
    if count.is_none_or(|count| count > limits.max_items) {
        return false;
    }
    serde_json::to_writer(
        &mut ByteCounter {
            written: 0,
            limit: limits.max_bytes.min(request.limits.max_input_bytes),
        },
        request,
    )
    .is_ok()
}
fn invalid(path: &str) -> ContractError {
    ContractError::new(ErrorCode::InvalidContext, path)
}
fn mismatch(path: &str) -> ContractError {
    ContractError::new(ErrorCode::ContextMismatch, path)
}
fn budget() -> ContractError {
    ContractError::new(
        ErrorCode::ContextBudgetExceeded,
        "projection.required_input",
    )
}
```

## `crates/wickle/src/input_binding.rs`

```rust
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    io,
    panic::AssertUnwindSafe,
    sync::Arc,
};

use futures_util::FutureExt;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::{
    CommitInput, CompiledTool, ContractError, ErrorCode, ExecutionContext, ExecutionContextData,
    Id, IdSource, JsonDigest, JsonObject, PolicyAction, PolicyDecision, PolicyGate, PolicyRequest,
    PortFuture, ProtectedRecord, RecordRef, RunBudget, RunSnapshot, Scope, SystemInputDefinition,
    SystemInputRegistry, SystemInputSnapshotRef, SystemInputSource, SystemInputs, ToolBindingRef,
    ToolCall, ToolCallState, ToolPolicyInput, VersionedRef, serialization::data_digest,
    tool_schema::compile_validator,
};

const RUN_INPUT_VERSION: &str = "wickle.run-system-inputs.v1";
const BOUND_INPUT_VERSION: &str = "wickle.bound-tool-input.v1";

/// Finite resolver and input-size bounds. They are independent of model token budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputBindingLimits {
    /// Maximum distinct resolver keys read for one new call; zero disables resolver reads.
    pub max_resolver_calls: usize,
    /// Maximum serialized bytes in one resolved or run-supplied value.
    pub max_value_bytes: usize,
    /// Maximum protected run-input or bound-input record size.
    pub max_bound_bytes: usize,
}
impl Default for InputBindingLimits {
    fn default() -> Self {
        Self {
            max_resolver_calls: 64,
            max_value_bytes: 65_536,
            max_bound_bytes: 1_048_576,
        }
    }
}
impl InputBindingLimits {
    fn validate(self) -> Result<(), ContractError> {
        if self.max_value_bytes == 0 || self.max_bound_bytes == 0 {
            return Err(error(ErrorCode::InvalidContract, "input_binding.limits"));
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunInputData {
    schema_version: String,
    scope: Scope,
    values: SystemInputs,
    definitions: BTreeMap<Id, SystemInputDefinition>,
}

/// Owned admission-time values and definition metadata. No resolver executes during
/// capture, and a missing value is not replaced by a schema default or generated ID.
#[derive(Clone)]
pub struct RunSystemInputs {
    data: RunInputData,
}

impl Serialize for RunSystemInputs {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.data.serialize(serializer)
    }
}
impl fmt::Debug for RunSystemInputs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunSystemInputs")
            .field("value_count", &self.data.values.values().len())
            .field("definition_count", &self.data.definitions.len())
            .finish_non_exhaustive()
    }
}

impl RunSystemInputs {
    /// Validate supplied keys/types and freeze owned values with the default finite bounds.
    pub fn capture(
        scope: Scope,
        supplied: Option<SystemInputs>,
        registry: &SystemInputRegistry,
    ) -> Result<Self, ContractError> {
        Self::capture_with_limits(scope, supplied, registry, InputBindingLimits::default())
    }
    /// Capture using explicit finite size bounds. Missing registered keys are allowed.
    pub fn capture_with_limits(
        scope: Scope,
        supplied: Option<SystemInputs>,
        registry: &SystemInputRegistry,
        limits: InputBindingLimits,
    ) -> Result<Self, ContractError> {
        limits.validate()?;
        let snapshot = Self {
            data: RunInputData {
                schema_version: RUN_INPUT_VERSION.into(),
                scope,
                values: supplied.unwrap_or_default(),
                definitions: registry.definitions().clone(),
            },
        };
        snapshot.validate_data()?;
        check_size(&snapshot, limits.max_bound_bytes)?;
        for value in snapshot.values().values() {
            check_size(value, limits.max_value_bytes)?;
        }
        Ok(snapshot)
    }
    /// Explicit access for the trusted binder; never automatic model projection.
    pub fn values(&self) -> &JsonObject {
        self.data.values.values()
    }
    /// Definition revisions and schemas pinned at admission.
    pub fn definitions(&self) -> &BTreeMap<Id, SystemInputDefinition> {
        &self.data.definitions
    }
    /// Exact owning scope of these values.
    pub fn scope(&self) -> &Scope {
        &self.data.scope
    }
    /// Digest of the complete protected serialized snapshot.
    pub fn digest(&self) -> JsonDigest {
        data_digest(&self.data)
    }
    /// Create the immutable record to include in the admission transaction.
    pub fn to_record(&self, record_id: Id, revision: u64) -> ProtectedRecord {
        ProtectedRecord::new(
            record_id,
            revision,
            serde_json::to_value(self).expect("serializable input data"),
        )
    }
    /// Create the run checkpoint reference after verifying its protected record identity.
    pub fn snapshot_ref(
        &self,
        record: &RecordRef,
    ) -> Result<SystemInputSnapshotRef, ContractError> {
        if record.digest != self.digest() {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "system_inputs.record",
            ));
        }
        Ok(SystemInputSnapshotRef {
            snapshot_ref: record.clone(),
            values_digest: data_digest(self.values()),
            definition_versions: self
                .definitions()
                .iter()
                .map(|(key, definition)| (key.clone(), definition.version.clone()))
                .collect(),
        })
    }
    /// Restore exact stored data and verify every pinned definition against the registry.
    /// Additional unrelated registry keys do not replace or enlarge the saved snapshot.
    pub fn restore(
        record: &ProtectedRecord,
        reference: &SystemInputSnapshotRef,
        scope: &Scope,
        registry: &SystemInputRegistry,
    ) -> Result<Self, ContractError> {
        if record.reference() != &reference.snapshot_ref {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "system_inputs.record",
            ));
        }
        let snapshot = Self::from_value(record.value(), reference, scope)?;
        if snapshot
            .definitions()
            .iter()
            .any(|(key, definition)| registry.get(key) != Some(definition))
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "system_inputs.definitions",
            ));
        }
        Ok(snapshot)
    }
    /// Omission reuses saved values. Any supplied map, including an empty map, must match.
    pub fn validate_resume(&self, supplied: Option<&SystemInputs>) -> Result<(), ContractError> {
        if supplied.is_some_and(|values| data_digest(values.values()) != data_digest(self.values()))
        {
            return Err(error(ErrorCode::SystemInputsMismatch, "system_inputs"));
        }
        Ok(())
    }
    fn validate_data(&self) -> Result<(), ContractError> {
        if self.data.schema_version != RUN_INPUT_VERSION
            || self
                .definitions()
                .iter()
                .any(|(key, definition)| key != &definition.key)
        {
            return Err(error(
                ErrorCode::SystemInputInvalid,
                "system_inputs.snapshot",
            ));
        }
        SystemInputRegistry::new(self.definitions().values().cloned().collect())
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "system_inputs.definitions"))?;
        for (key, value) in self.values() {
            let key = Id::new(key.clone())
                .map_err(|_| error(ErrorCode::SystemInputInvalid, "system_inputs.key"))?;
            let definition = self
                .definitions()
                .get(&key)
                .ok_or_else(|| error(ErrorCode::SystemInputInvalid, "system_inputs.key"))?;
            if !matches!(definition.source, SystemInputSource::Run {}) {
                return Err(error(ErrorCode::SystemInputInvalid, "system_inputs.source"));
            }
            validate_value(definition, value)?;
        }
        Ok(())
    }
    pub(crate) fn from_value(
        value: &Value,
        reference: &SystemInputSnapshotRef,
        scope: &Scope,
    ) -> Result<Self, ContractError> {
        let data: RunInputData = serde_json::from_value(value.clone())
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "system_inputs.snapshot"))?;
        let snapshot = Self { data };
        snapshot.validate_data()?;
        if snapshot.scope() != scope
            || snapshot.snapshot_ref(&reference.snapshot_ref)? != *reference
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "system_inputs.snapshot",
            ));
        }
        Ok(snapshot)
    }
}

/// One exact read-only resolver lookup, without other system values or credentials.
#[derive(Clone)]
pub struct SystemInputResolveRequest {
    /// Registered key being requested.
    pub key: Id,
    /// Pinned value-definition revision.
    pub definition_version: Id,
    /// Exact resolver implementation selected by the definition.
    pub resolver_ref: VersionedRef,
    /// Normalized model-owned arguments only, including declared top-level defaults.
    pub model_inputs: JsonObject,
}
impl fmt::Debug for SystemInputResolveRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SystemInputResolveRequest")
            .field("key", &self.key)
            .field("definition_version", &self.definition_version)
            .finish_non_exhaustive()
    }
}

/// Current actor and execution bounds supplied to a trusted read-only resolver.
#[derive(Debug, Clone)]
pub struct SystemInputResolveContext {
    /// Authenticated scope, not a value extracted from the model's arguments.
    pub scope: Scope,
    /// Current principal; it does not rewrite the run's original system-input values.
    pub principal_ref: Id,
    /// Current capability grant, checked by policy and the resolver's own backend.
    pub capability_grant_ref: Id,
    /// Current owning run.
    pub run_id: Id,
    /// Original logical call identity.
    pub call_id: Id,
    /// Deadline for this lookup.
    pub deadline: tokio::time::Instant,
    /// Child cancellation signal linked to both execution and caller cancellation.
    pub cancellation: CancellationToken,
}

/// Data and source revision returned by a resolver, or recorded from a run snapshot.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedSystemInput {
    /// Supplied JSON value; explicit null is different from an absent result.
    pub value: Value,
    /// Source data revision, not a newly invented foreign key.
    pub revision: Id,
}
impl fmt::Debug for ResolvedSystemInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ResolvedSystemInput(<redacted>)")
    }
}

/// Trusted read-only lookup port. It must honor scope, principal, deadline and
/// cancellation, and must not hide business writes or create missing foreign keys.
pub trait SystemInputResolver: Send + Sync {
    /// Read one exact registered key; None means absent, not JSON null.
    fn resolve<'a>(
        &'a self,
        request: &'a SystemInputResolveRequest,
        context: &'a SystemInputResolveContext,
    ) -> PortFuture<'a, Option<ResolvedSystemInput>>;
}

/// One hidden parameter's fixed source, revision and optional value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundSystemInput {
    /// Registry key, which may differ from the handler parameter name.
    pub key: Id,
    /// Value-definition version pinned by the compiler and admission snapshot.
    pub definition_version: Id,
    /// Run snapshot or exact resolver implementation.
    pub source: SystemInputSource,
    /// None is absence; Some with value:null is an explicitly supplied null.
    pub resolved: Option<ResolvedSystemInput>,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundInputData {
    schema_version: String,
    scope: Scope,
    run_id: Id,
    call_id: Id,
    tool: VersionedRef,
    descriptor_digest: JsonDigest,
    compiled_digest: JsonDigest,
    compiler_version: String,
    original_model_inputs: JsonObject,
    normalized_model_inputs: JsonObject,
    run_inputs_ref: Option<SystemInputSnapshotRef>,
    system_inputs: BTreeMap<String, BoundSystemInput>,
    execution_args: JsonObject,
}

/// Immutable execution inputs. Serialization is only for protected storage/policy,
/// never a replacement for the original model ToolCall or its transcript message.
#[derive(Clone, Serialize)]
pub struct BoundToolInput {
    data: BoundInputData,
    binding_digest: JsonDigest,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundInputRecord {
    data: BoundInputData,
    binding_digest: JsonDigest,
}

impl fmt::Debug for BoundToolInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundToolInput")
            .field("call_id", &self.data.call_id)
            .field("tool", &self.data.tool)
            .field("binding_digest", &self.binding_digest)
            .finish_non_exhaustive()
    }
}

impl BoundToolInput {
    /// Exact owning resource scope.
    pub fn scope(&self) -> &Scope {
        &self.data.scope
    }
    /// Owning run identity.
    pub fn run_id(&self) -> &Id {
        &self.data.run_id
    }
    /// Stable logical call identity.
    pub fn call_id(&self) -> &Id {
        &self.data.call_id
    }
    /// Exact registered tool identity.
    pub fn tool(&self) -> &VersionedRef {
        &self.data.tool
    }
    /// Original descriptor digest.
    pub fn descriptor_digest(&self) -> &JsonDigest {
        &self.data.descriptor_digest
    }
    /// Compiler, schema and selected system-definition identity.
    pub fn compiled_digest(&self) -> &JsonDigest {
        &self.data.compiled_digest
    }
    /// Pinned compiler contract version.
    pub fn compiler_version(&self) -> &str {
        &self.data.compiler_version
    }
    /// Unmodified arguments originally recorded for the model call.
    pub fn original_model_inputs(&self) -> &JsonObject {
        &self.data.original_model_inputs
    }
    /// Original model arguments plus declared optional top-level defaults.
    pub fn normalized_model_inputs(&self) -> &JsonObject {
        &self.data.normalized_model_inputs
    }
    /// Only hidden parameters needed by this tool, with fixed absence/value metadata.
    pub fn system_inputs(&self) -> &BTreeMap<String, BoundSystemInput> {
        &self.data.system_inputs
    }
    /// Full handler arguments; privileged access, never automatic model echo.
    pub fn execution_args(&self) -> &JsonObject {
        &self.data.execution_args
    }
    /// Digest over exact inputs, tool/compiler identity, source revisions, scope and call.
    pub fn binding_digest(&self) -> &JsonDigest {
        &self.binding_digest
    }
    /// Build the existing final-value policy input without introducing a new ownership port.
    pub fn policy_input(&self) -> ToolPolicyInput {
        ToolPolicyInput::new(
            self.data.call_id.clone(),
            self.data.tool.clone(),
            self.data.descriptor_digest.clone(),
            self.binding_digest.clone(),
            self.data.execution_args.clone(),
        )
    }
    /// Exact action checked for allow/deny/approval after all values are fixed.
    pub fn policy_request(&self) -> PolicyRequest {
        PolicyRequest {
            owner_scope: self.data.scope.clone(),
            resource_id: self.data.run_id.clone(),
            action: PolicyAction::ExecuteTool {
                input: self.policy_input(),
            },
        }
    }
    /// Restore protected inputs using the saved ledger call's exact record reference
    /// and the currently supplied compiled contract.
    pub fn restore(
        record: &ProtectedRecord,
        compiled: &CompiledTool,
        scope: &Scope,
        run_id: &Id,
        call: &ToolCall,
        run_inputs_ref: Option<&SystemInputSnapshotRef>,
    ) -> Result<Self, ContractError> {
        let bound = Self::from_value(record.value())?;
        if call.bound_input_ref.as_ref() != Some(record.reference())
            || data_digest(&bound) != record.reference().digest
        {
            return Err(error(ErrorCode::SystemInputsMismatch, "bound_input.record"));
        }
        bound.validate_identity(scope, run_id, call, run_inputs_ref)?;
        bound.validate_compiled(compiled)?;
        Ok(bound)
    }
    fn from_value(value: &Value) -> Result<Self, ContractError> {
        let record: BoundInputRecord = serde_json::from_value(value.clone())
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "bound_input"))?;
        let bound = Self {
            data: record.data,
            binding_digest: record.binding_digest,
        };
        if bound.data.schema_version != BOUND_INPUT_VERSION
            || data_digest(&bound.data) != bound.binding_digest
            || data_digest(&bound) != crate::canonical_digest(value)
        {
            return Err(error(ErrorCode::SystemInputsMismatch, "bound_input.digest"));
        }
        let mut execution = bound.data.normalized_model_inputs.clone();
        if bound
            .data
            .original_model_inputs
            .iter()
            .any(|(key, value)| execution.get(key) != Some(value))
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.model_inputs",
            ));
        }
        let mut sources: BTreeMap<&Id, &BoundSystemInput> = BTreeMap::new();
        for (parameter, input) in &bound.data.system_inputs {
            if execution.contains_key(parameter) {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.ownership",
                ));
            }
            if sources
                .insert(&input.key, input)
                .is_some_and(|previous| previous != input)
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.sources",
                ));
            }
            if let Some(resolved) = &input.resolved {
                execution.insert(parameter.clone(), resolved.value.clone());
            }
        }
        if execution != bound.data.execution_args {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.execution_args",
            ));
        }
        Ok(bound)
    }
    fn validate_identity(
        &self,
        scope: &Scope,
        run_id: &Id,
        call: &ToolCall,
        run_inputs_ref: Option<&SystemInputSnapshotRef>,
    ) -> Result<(), ContractError> {
        if self.scope() != scope
            || self.run_id() != run_id
            || self.call_id() != &call.call_id
            || Some(self.descriptor_digest()) != call.descriptor_digest.as_ref()
            || self.original_model_inputs() != &call.model_inputs
            || self.data.run_inputs_ref.as_ref() != run_inputs_ref
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.identity",
            ));
        }
        Ok(())
    }
    fn validate_compiled(&self, compiled: &CompiledTool) -> Result<(), ContractError> {
        if self.compiled_digest() != compiled.digest()
            || self.compiler_version() != compiled.compiler_version()
            || self.tool() != &compiled.descriptor().tool
            || self.descriptor_digest() != compiled.descriptor_digest()
            || self.system_inputs().len() != compiled.system_bindings().len()
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.compiled",
            ));
        }
        compiled.validate_model_inputs(self.original_model_inputs())?;
        if normalize_model_inputs(compiled, self.original_model_inputs())?
            != *self.normalized_model_inputs()
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.normalization",
            ));
        }
        for (parameter, definition) in compiled.system_bindings() {
            let input = self
                .system_inputs()
                .get(parameter)
                .ok_or_else(|| error(ErrorCode::SystemInputsMismatch, "bound_input.parameters"))?;
            if input.key != definition.key
                || input.definition_version != definition.version
                || input.source != definition.source
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.definitions",
                ));
            }
            if let Some(value) = &input.resolved {
                validate_value(definition, &value.value)?;
            }
        }
        compiled
            .validate_execution_inputs(self.execution_args())
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "tool.execution_inputs"))
    }
}

/// A saved candidate and the decision observed at binding time, not a reusable
/// dispatch permit. The executor must recheck current policy/budgets before I/O.
#[derive(Debug)]
pub struct ToolBindingResult {
    /// Owned immutable protected input.
    pub input: BoundToolInput,
    /// Record stored atomically with the call's bound_input_ref.
    pub reference: RecordRef,
    /// Allow or require_approval. Deny is returned as an error without saving a new candidate.
    pub decision: PolicyDecision,
}

/// Default normalization, registered system-value lookup and immutable candidate persistence.
/// This version starts from the original model input. Hook transformations require
/// a separate recorded path and never overwrite the original ToolCall.
pub struct InputBinder {
    registry: Arc<SystemInputRegistry>,
    resolver: Option<Arc<dyn SystemInputResolver>>,
    policy: Arc<PolicyGate>,
    ids: Arc<dyn IdSource>,
    limits: InputBindingLimits,
}

impl InputBinder {
    /// Wire trusted metadata, optional read-only resolver, policy, and internal record IDs.
    pub fn new(
        registry: Arc<SystemInputRegistry>,
        resolver: Option<Arc<dyn SystemInputResolver>>,
        policy: Arc<PolicyGate>,
        ids: Arc<dyn IdSource>,
    ) -> Self {
        Self {
            registry,
            resolver,
            policy,
            ids,
            limits: InputBindingLimits::default(),
        }
    }
    /// Set finite lookup/value/candidate bounds. Zero lookups disables resolver sources.
    pub fn with_limits(mut self, limits: InputBindingLimits) -> Result<Self, ContractError> {
        limits.validate()?;
        self.limits = limits;
        Ok(self)
    }

    /// Reuse an existing saved binding, or bind and atomically save a new candidate.
    /// Every path checks current policy; an existing call never re-queries its resolver.
    pub async fn bind(
        &self,
        compiled: &CompiledTool,
        call_id: &Id,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<ToolBindingResult, ContractError> {
        boundary(context, budget).await?;
        let saved = bounded(
            context,
            budget,
            budget.store().load(budget.scope(), budget.run_id()),
        )
        .await?;
        let call = saved
            .snapshot
            .tool_ledger
            .iter()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool_call"))?
            .call
            .clone();
        check_selection(&saved.snapshot, compiled, &call)?;
        let run_inputs = match &saved.snapshot.system_inputs {
            Some(reference) => {
                boundary(context, budget).await?;
                let record = bounded(
                    context,
                    budget,
                    budget
                        .store()
                        .read_record(budget.scope(), &reference.snapshot_ref),
                )
                .await?;
                let inputs =
                    RunSystemInputs::restore(&record, reference, budget.scope(), &self.registry)?;
                inputs.validate_resume(context.data.system_inputs.as_ref())?;
                Some(inputs)
            }
            None => {
                if context
                    .data
                    .system_inputs
                    .as_ref()
                    .is_some_and(|values| !values.values().is_empty())
                {
                    return Err(error(ErrorCode::SystemInputsMismatch, "system_inputs"));
                }
                if !compiled.system_bindings().is_empty() {
                    return Err(error(
                        ErrorCode::SystemInputMissing,
                        "system_inputs.snapshot",
                    ));
                }
                None
            }
        };
        for definition in compiled.system_bindings().values() {
            if self.registry.get(&definition.key) != Some(definition)
                || run_inputs
                    .as_ref()
                    .and_then(|inputs| inputs.definitions().get(&definition.key))
                    != Some(definition)
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "system_inputs.definitions",
                ));
            }
        }
        if let Some(reference) = &call.bound_input_ref {
            boundary(context, budget).await?;
            let record = bounded(
                context,
                budget,
                budget.store().read_record(budget.scope(), reference),
            )
            .await?;
            let input = BoundToolInput::restore(
                &record,
                compiled,
                budget.scope(),
                budget.run_id(),
                &call,
                saved.snapshot.system_inputs.as_ref(),
            )?;
            validate_bound_record(record.value(), &saved.snapshot, &call, run_inputs.as_ref())?;
            check_size(&input, self.limits.max_bound_bytes)?;
            for value in input
                .system_inputs()
                .values()
                .filter_map(|input| input.resolved.as_ref())
            {
                check_size(&value.value, self.limits.max_value_bytes)?;
            }
            let decision = self
                .authorize(&input.policy_request(), context, budget, false)
                .await?;
            boundary(context, budget).await?;
            return Ok(ToolBindingResult {
                input,
                reference: reference.clone(),
                decision,
            });
        }
        if !matches!(
            saved
                .snapshot
                .tool_ledger
                .iter()
                .find(|entry| &entry.call.call_id == call_id)
                .expect("found call")
                .state,
            ToolCallState::Planned {}
        ) {
            return Err(error(ErrorCode::InvalidTransition, "tool_call.state"));
        }
        let normalized = normalize_model_inputs(compiled, &call.model_inputs)?;
        check_size(&normalized, self.limits.max_bound_bytes)?;
        let mut execution_args = normalized.clone();
        let mut system_inputs = BTreeMap::new();
        let mut values: BTreeMap<Id, Option<ResolvedSystemInput>> = BTreeMap::new();
        let mut resolver_calls = 0;
        for (parameter, definition) in compiled.system_bindings() {
            let resolved = if let Some(cached) = values.get(&definition.key) {
                cached.clone()
            } else {
                let value = match &definition.source {
                    SystemInputSource::Run {} => run_inputs
                        .as_ref()
                        .and_then(|inputs| inputs.values().get(definition.key.as_str()))
                        .cloned()
                        .map(|value| ResolvedSystemInput {
                            value,
                            revision: Id::new(
                                saved
                                    .snapshot
                                    .system_inputs
                                    .as_ref()
                                    .expect("required run snapshot")
                                    .snapshot_ref
                                    .revision
                                    .to_string(),
                            )
                            .expect("numeric revision"),
                        }),
                    SystemInputSource::Resolver { resolver_ref } => {
                        if resolver_calls >= self.limits.max_resolver_calls {
                            return Err(limit_error());
                        }
                        let request = PolicyRequest {
                            owner_scope: budget.scope().clone(),
                            resource_id: budget.run_id().clone(),
                            action: PolicyAction::ResolveSystemInput {
                                tool: compiled.descriptor().tool.clone(),
                                call_id: call_id.clone(),
                                descriptor_digest: compiled.descriptor_digest().clone(),
                                compiled_digest: compiled.digest().clone(),
                                key: definition.key.clone(),
                                definition_version: definition.version.clone(),
                                resolver_ref: resolver_ref.clone(),
                            },
                        };
                        self.authorize(&request, context, budget, true).await?;
                        let resolver = self.resolver.as_ref().ok_or_else(|| {
                            error(ErrorCode::SystemInputUnavailable, "system_input.resolver")
                        })?;
                        boundary(context, budget).await?;
                        let request = SystemInputResolveRequest {
                            key: definition.key.clone(),
                            definition_version: definition.version.clone(),
                            resolver_ref: resolver_ref.clone(),
                            model_inputs: normalized.clone(),
                        };
                        let child = budget.cancellation().child_token();
                        let lookup_context = SystemInputResolveContext {
                            scope: budget.scope().clone(),
                            principal_ref: context.data.principal_ref.clone(),
                            capability_grant_ref: context.data.capability_grant_ref.clone(),
                            run_id: budget.run_id().clone(),
                            call_id: call_id.clone(),
                            deadline: budget.call_deadline()?,
                            cancellation: child.clone(),
                        };
                        let lookup = AssertUnwindSafe(async {
                            resolver.resolve(&request, &lookup_context).await
                        })
                        .catch_unwind();
                        tokio::pin!(lookup);
                        let guard = child.drop_guard();
                        resolver_calls += 1;
                        let answer = bounded(context, budget, async {
                            lookup
                                .await
                                .map_err(|_| {
                                    error(
                                        ErrorCode::SystemInputUnavailable,
                                        "system_input.resolver",
                                    )
                                })?
                                .map_err(|_| {
                                    error(
                                        ErrorCode::SystemInputUnavailable,
                                        "system_input.resolver",
                                    )
                                })
                        })
                        .await;
                        drop(guard);
                        let answer = answer?;
                        boundary(context, budget).await?;
                        answer
                    }
                };
                if let Some(resolved) = &value {
                    check_size(&resolved.value, self.limits.max_value_bytes)?;
                    validate_value(definition, &resolved.value)?;
                }
                values.insert(definition.key.clone(), value.clone());
                value
            };
            if let Some(value) = &resolved {
                execution_args.insert(parameter.clone(), value.value.clone());
            } else if required_parameter(compiled, parameter) {
                return Err(error(
                    ErrorCode::SystemInputMissing,
                    &system_input_path(&definition.key),
                ));
            }
            system_inputs.insert(
                parameter.clone(),
                BoundSystemInput {
                    key: definition.key.clone(),
                    definition_version: definition.version.clone(),
                    source: definition.source.clone(),
                    resolved,
                },
            );
        }
        compiled
            .validate_execution_inputs(&execution_args)
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "tool.execution_inputs"))?;
        let data = BoundInputData {
            schema_version: BOUND_INPUT_VERSION.into(),
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            call_id: call_id.clone(),
            tool: compiled.descriptor().tool.clone(),
            descriptor_digest: compiled.descriptor_digest().clone(),
            compiled_digest: compiled.digest().clone(),
            compiler_version: compiled.compiler_version().into(),
            original_model_inputs: call.model_inputs.clone(),
            normalized_model_inputs: normalized,
            run_inputs_ref: saved.snapshot.system_inputs.clone(),
            system_inputs,
            execution_args,
        };
        let input = BoundToolInput {
            binding_digest: data_digest(&data),
            data,
        };
        check_size(&input, self.limits.max_bound_bytes)?;
        let decision = self
            .authorize(&input.policy_request(), context, budget, false)
            .await?;
        boundary(context, budget).await?;
        let record = ProtectedRecord::new(
            self.ids.next_id()?,
            1,
            serde_json::to_value(&input).expect("bound input serialization"),
        );
        let reference = record.reference().clone();
        let mut next = saved.snapshot;
        let expected_revision = next.revision;
        let (elapsed, now_ms) = budget.settlement_time(next.usage.elapsed_ms)?;
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::RevisionConflict, "revision"))?;
        next.usage.elapsed_ms = elapsed;
        next.timing.last_observed_at_ms = now_ms;
        next.tool_ledger
            .iter_mut()
            .find(|entry| &entry.call.call_id == call_id)
            .expect("found call")
            .call
            .bound_input_ref = Some(reference.clone());
        bounded(
            context,
            budget,
            budget.store().commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms,
                    snapshot: next,
                    messages: Vec::new(),
                    events: Vec::new(),
                    records: vec![record],
                },
            ),
        )
        .await?;
        boundary(context, budget).await?;
        Ok(ToolBindingResult {
            input,
            reference,
            decision,
        })
    }

    async fn authorize(
        &self,
        request: &PolicyRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
        lookup: bool,
    ) -> Result<PolicyDecision, ContractError> {
        boundary(context, budget).await?;
        let deadline = budget.call_deadline()?;
        let child = budget.cancellation().child_token();
        let policy_context = ExecutionContext::new(
            ExecutionContextData {
                scope: context.data.scope.clone(),
                principal_ref: context.data.principal_ref.clone(),
                capability_grant_ref: context.data.capability_grant_ref.clone(),
                trace_context: None,
                system_inputs: None,
            },
            child.clone(),
        );
        let check = self
            .policy
            .check(request, &policy_context, Some(deadline), None);
        tokio::pin!(check);
        let guard = child.drop_guard();
        let result = bounded(context, budget, &mut check).await;
        drop(guard);
        let decision = result?;
        boundary(context, budget).await?;
        match decision {
            PolicyDecision::Deny { .. } => Err(error(ErrorCode::AccessDenied, "policy")),
            PolicyDecision::RequireApproval { .. } if lookup => Err(error(
                ErrorCode::SystemInputApprovalRequired,
                "system_input.lookup",
            )),
            decision => Ok(decision),
        }
    }
}

fn check_selection(
    snapshot: &RunSnapshot,
    compiled: &CompiledTool,
    call: &ToolCall,
) -> Result<(), ContractError> {
    if call.descriptor_digest.as_ref() != Some(compiled.descriptor_digest())
        || !snapshot
            .profile
            .profile()
            .tools
            .iter()
            .any(|selection| match selection {
                ToolBindingRef::Catalog(reference) => {
                    reference.tool_id == compiled.descriptor().tool.id
                        && reference.version == compiled.descriptor().tool.version
                        && call.tool_name == compiled.descriptor().name
                }
                ToolBindingRef::Export(export) => {
                    export.alias.as_ref().unwrap_or(&compiled.descriptor().name) == &call.tool_name
                        && snapshot
                            .profile
                            .profile()
                            .adapters
                            .as_ref()
                            .is_some_and(|adapters| {
                                adapters
                                    .iter()
                                    .any(|adapter| adapter.binding_id == export.adapter_binding)
                            })
                }
            })
    {
        return Err(error(
            ErrorCode::InvalidToolInputContract,
            "tool_call.descriptor",
        ));
    }
    Ok(())
}

fn normalize_model_inputs(
    compiled: &CompiledTool,
    original: &JsonObject,
) -> Result<JsonObject, ContractError> {
    compiled.validate_model_inputs(original)?;
    let mut normalized = original.clone();
    let properties = compiled
        .model_input_schema()
        .get("properties")
        .and_then(Value::as_object)
        .expect("compiled properties");
    for parameter in &compiled.descriptor().agent_parameters {
        if normalized.contains_key(parameter) || required_parameter(compiled, parameter) {
            continue;
        }
        let mut schema = &properties[parameter];
        let mut seen = BTreeSet::new();
        loop {
            if let Some(default) = schema.get("default") {
                normalized.insert(parameter.clone(), default.clone());
                break;
            }
            let Some(reference) = schema.get("$ref").and_then(Value::as_str) else {
                break;
            };
            if !seen.insert(reference) {
                break;
            }
            let pointer = reference
                .strip_prefix('#')
                .ok_or_else(|| error(ErrorCode::InvalidToolInputContract, "model_defaults"))?;
            schema = compiled
                .model_input_schema()
                .pointer(pointer)
                .ok_or_else(|| error(ErrorCode::InvalidToolInputContract, "model_defaults"))?;
        }
    }
    compiled.validate_model_inputs(&normalized)?;
    Ok(normalized)
}
fn required_parameter(compiled: &CompiledTool, parameter: &str) -> bool {
    compiled
        .input_schema()
        .get("required")
        .and_then(Value::as_array)
        .is_some_and(|required| required.iter().any(|name| name.as_str() == Some(parameter)))
}
fn validate_value(definition: &SystemInputDefinition, value: &Value) -> Result<(), ContractError> {
    let validator = compile_validator(&definition.value_schema).map_err(|_| {
        error(
            ErrorCode::SystemInputInvalid,
            &system_input_path(&definition.key),
        )
    })?;
    if !validator.is_valid(value) {
        return Err(error(
            ErrorCode::SystemInputInvalid,
            &system_input_path(&definition.key),
        ));
    }
    Ok(())
}

fn system_input_path(key: &Id) -> String {
    // Only registered metadata is named; JSON escaping prevents control characters
    // or punctuation from being interpreted as a path or leaking a supplied value.
    format!(
        "system_inputs[{}]",
        serde_json::to_string(key.as_str()).expect("serializable key")
    )
}

async fn boundary(context: &ExecutionContext, budget: &RunBudget) -> Result<(), ContractError> {
    if &context.data.scope != budget.scope() {
        return Err(error(ErrorCode::AccessDenied, "scope"));
    }
    if context.cancellation.is_cancelled() {
        return Err(error(ErrorCode::Cancelled, "input_binding"));
    }
    bounded(context, budget, budget.check_boundary()).await
}
async fn bounded<T>(
    context: &ExecutionContext,
    budget: &RunBudget,
    future: impl Future<Output = Result<T, ContractError>>,
) -> Result<T, ContractError> {
    if context.cancellation.is_cancelled() {
        return Err(error(ErrorCode::Cancelled, "input_binding"));
    }
    tokio::select! {
        biased;
        _ = context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "input_binding")),
        stopped = budget.wait_for_cancellation_or_deadline() => { stopped?; Err(error(ErrorCode::DeadlineExceeded, "input_binding")) },
        result = future => {
            if context.cancellation.is_cancelled() || budget.cancellation().is_cancelled() { return Err(error(ErrorCode::Cancelled, "input_binding")); }
            budget.call_deadline()?;
            result
        }
    }
}

pub(crate) fn validate_bound_record(
    value: &Value,
    snapshot: &RunSnapshot,
    call: &ToolCall,
    run_inputs: Option<&RunSystemInputs>,
) -> Result<(), ContractError> {
    let input = BoundToolInput::from_value(value)?;
    input.validate_identity(
        &snapshot.scope,
        &snapshot.run_id,
        call,
        snapshot.system_inputs.as_ref(),
    )?;
    for bound in input.system_inputs().values() {
        let data = run_inputs
            .ok_or_else(|| error(ErrorCode::SystemInputsMismatch, "bound_input.run_snapshot"))?;
        let definition = data
            .definitions()
            .get(&bound.key)
            .ok_or_else(|| error(ErrorCode::SystemInputsMismatch, "bound_input.definition"))?;
        if definition.version != bound.definition_version || definition.source != bound.source {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.definition",
            ));
        }
        if let Some(value) = &bound.resolved {
            validate_value(definition, &value.value)?;
        }
        if matches!(bound.source, SystemInputSource::Run {}) {
            let expected = data.values().get(bound.key.as_str());
            if bound.resolved.as_ref().map(|resolved| &resolved.value) != expected
                || bound.resolved.as_ref().is_some_and(|resolved| {
                    resolved.revision.as_str()
                        != snapshot
                            .system_inputs
                            .as_ref()
                            .expect("snapshot supplied")
                            .snapshot_ref
                            .revision
                            .to_string()
                })
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.run_value",
                ));
            }
        }
    }
    Ok(())
}

struct ByteCounter {
    total: usize,
    limit: usize,
}
impl io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.total = self
            .total
            .checked_add(bytes.len())
            .filter(|size| *size <= self.limit)
            .ok_or_else(|| io::Error::other("input size limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn check_size(value: &impl Serialize, limit: usize) -> Result<(), ContractError> {
    serde_json::to_writer(&mut ByteCounter { total: 0, limit }, value).map_err(|_| limit_error())
}
fn limit_error() -> ContractError {
    error(ErrorCode::InputBindingLimitExceeded, "input_binding.limits")
}
fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
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
mod budget;
mod clock;
mod context;
mod context_projection;
mod error;
mod input_binding;
mod message;
mod model;
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
mod state;
mod tool_execution;
mod tool_schema;
mod views;

pub use agent::{
    Agent, AgentBindings, AgentSettings, CancelReceipt, ModelTokenEstimator, RunHandle,
    create_agent,
};
pub use budget::{AttemptReservation, ReservationKind, RunBudget, RunTiming};
pub use clock::{Clock, ClockReading, IdSource, RandomIdSource, SystemClock};
pub use context_projection::{
    CONTEXT_ASSEMBLER_VERSION, ContextAssembler, ContextItem, ContextLifetime, ContextOrigin,
    ContextPriority, ContextProjection, InstructionAssetContent, PinnedPromptTool, ProjectionInput,
    ProjectionLimits, PromptSnapshot, PromptToolBinding, ScopedOpaque, SkillManifest,
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
    PolicyPort, PolicyRequest, ToolPolicyInput,
};
pub use state::{
    AdmissionInput, AdmissionResult, CommitInput, EventPage, MAX_EVENT_PAGE_SIZE, MemoryStateStore,
    ProtectedRecord, RunLease, STATE_STORE_CHECKPOINT_VERSION, StateStore, StateStoreCapabilities,
    StateStoreCheckpoint, StoredRun,
};
pub use tool_execution::{
    SerialToolRound, ToolEffect, ToolExecutionContext, ToolExecutionLimits, ToolExecutionOutcome,
    ToolExecutionResult, ToolExecutor, ToolRegistration, ToolRegistry, ToolRoundOutcome,
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
    MessageRole, RecordRef, ToolCall, ToolResult, ToolResultStatus, Visibility,
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
    ResumeCommand, RunEvent, RunEventPayload, RunEventSchemaVersion, RunOutcome, RunPhase,
    RunRequest, RunSnapshot, RunSnapshotSchemaVersion, RunStatus, RunTrigger, SessionSchemaVersion,
    SessionSnapshot, SourceExecutionState, SystemInputSnapshotRef, ToolCallState, ToolLedgerEntry,
    VerificationSummary, VerificationVerdict, WaitState, WaitTarget, admission_digest,
};
pub use serialization::{
    Id, JsonDigest, JsonObject, canonical_digest, canonical_digest_json, parse_json,
};
```

## `crates/wickle/src/message.rs`

```rust
use crate::{Id, JsonDigest, JsonObject, Scope, serialization::optional};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

/// An authorized reference to immutable stored data, not the referenced payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordRef {
    /// Store-specific record identifier.
    pub record_id: Id,
    /// Exact stored revision.
    pub revision: u64,
    /// Digest of the referenced contract data.
    pub digest: JsonDigest,
}

/// Artifact metadata. Reading bytes still requires current scope authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    /// Artifact identifier.
    pub artifact_id: Id,
    /// Scope that owns the artifact.
    pub scope: Scope,
    /// Media type of the stored bytes.
    pub media_type: Id,
    /// Original byte length.
    pub size_bytes: u64,
    /// Store-defined content hash, separate from JSON contract digests.
    pub content_hash: Id,
}

/// Provenance for a source passage or fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceRef {
    /// Source identifier.
    pub source_id: Id,
    /// Exact source version/revision.
    pub version: Id,
    /// Source-specific passage location.
    pub location: Id,
    /// Original source content hash.
    pub content_hash: Id,
    /// Optional quoted passage.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub quote: Option<String>,
}

/// User-supplied or final-output content; cannot inject tool calls or provider state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum InputContent {
    /// Text content.
    Text {
        /// Text body.
        text: String,
    },
    /// JSON data, not executable objects.
    Json {
        /// JSON value; explicit JSON null is valid content.
        value: serde_json::Value,
    },
    /// Artifact metadata.
    Artifact {
        /// Artifact reference.
        reference: ArtifactRef,
    },
    /// Evidence metadata.
    Evidence {
        /// Source reference.
        reference: EvidenceRef,
    },
}

/// Model-owned tool arguments and provenance, separate from system execution inputs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolCall {
    /// Core call identifier.
    pub call_id: Id,
    /// Original model request identifier.
    pub model_request_id: Id,
    /// Provider-local call identifier, scoped by model_request_id.
    pub provider_call_id: Id,
    /// Model-facing tool name.
    pub tool_name: Id,
    /// Original model-supplied inputs, never replaced with execution_args.
    pub model_inputs: JsonObject,
    /// Pinned descriptor identity. None means the name was unregistered when planned.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub descriptor_digest: Option<JsonDigest>,
    /// Protected bound-input record, once binding succeeds.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub bound_input_ref: Option<RecordRef>,
}

/// Outcome of one tool dispatch or a pre-dispatch denial.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolResultStatus {
    /// Tool completed successfully.
    Succeeded,
    /// Tool failed with a classified error.
    Failed,
    /// Policy or validation denied execution.
    Denied,
    /// Execution was cancelled.
    Cancelled,
    /// External effect status is not known.
    Unknown,
}

/// A safe structured failure; raw SDK errors belong in protected Host diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Failure {
    /// Registered error/reason code.
    pub code: Id,
    /// Optional protected diagnostic reference.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub diagnostic_ref: Option<RecordRef>,
}

/// Tool observation explicitly paired with its call message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolResult {
    /// Core call identifier.
    pub call_id: Id,
    /// Message containing the matching call.
    pub call_message_id: Id,
    /// Explicit execution status.
    pub status: ToolResultStatus,
    /// Confirmed external effect, separate from validation of the returned value.
    /// Missing legacy metadata is unknown, never evidence that a write did not occur.
    #[serde(default)]
    pub effect: crate::ToolEffect,
    /// Bounded model-visible observations or references.
    pub content: Vec<InputContent>,
    /// Protected receipt, retained even if output processing fails.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub effect_receipt_ref: Option<RecordRef>,
    /// Classified failure, when applicable.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub error: Option<Failure>,
}

/// Transcript content. Provider replay data is a scoped, protected reference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContentBlock {
    /// Ordinary displayable content.
    Content {
        /// Text, JSON, artifact, or evidence.
        content: InputContent,
    },
    /// A complete model tool call.
    ToolCall {
        /// Call and model-only arguments.
        call: ToolCall,
    },
    /// Paired tool observation.
    ToolResult {
        /// Observation and receipt reference.
        result: ToolResult,
    },
    /// Opaque continuation data bound to one provider/route.
    ProviderOpaque {
        /// Registered provider key.
        provider: Id,
        /// Route that can interpret the protected block.
        route_digest: JsonDigest,
        /// Protected replay-data record.
        data_ref: RecordRef,
    },
}

/// Logical message role before provider-specific projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    /// Host/profile instruction role.
    System,
    /// User input.
    User,
    /// Model output.
    Assistant,
    /// Tool observation.
    Tool,
}

/// Provenance of content; a wire role does not grant authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageOrigin {
    /// Trusted Host instructions.
    Host,
    /// Pinned profile instructions.
    Profile,
    /// User input.
    User,
    /// Model response.
    Model,
    /// Loaded skill data.
    Skill,
    /// Tool observation.
    Tool,
    /// Retrieved external data.
    Retrieval,
    /// Retrieved memory.
    Memory,
    /// Verifier feedback.
    Verification,
    /// Synthetic recovery bookkeeping.
    Recovery,
}

/// Intended projection surfaces; authorization is still enforced at use time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    /// Protected execution data only.
    Internal,
    /// Model projection only.
    Model,
    /// User presentation only.
    User,
    /// Both model projection and user presentation.
    UserAndModel,
}

/// An original transcript record, not a provider request or UI event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    /// Unique message identity.
    pub message_id: Id,
    /// Owning run.
    pub run_id: Id,
    /// Monotonic stored message sequence.
    pub sequence: NonZeroU64,
    /// Logical role.
    pub role: MessageRole,
    /// Original content blocks.
    pub content: Vec<ContentBlock>,
    /// Source provenance.
    pub origin: MessageOrigin,
    /// Intended projections.
    pub visibility: Visibility,
}
```

## `crates/wickle/src/model_execution/routed.rs`

```rust
use super::*;
use crate::{
    ModelInspectionContext, ModelPurpose, ModelRouter, PortFuture, ResolvedModelRoute,
    RouteRequest, RouteSelection, RouteSelectionReason, RoutingSnapshot, ToolCallState,
    VersionPolicy,
};
use tokio_util::sync::CancellationToken;

/// One logical model step. Its physical retries receive separate attempt IDs.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RoutedModelInput {
    /// Stable step identifier chosen by the driver and retained during recovery.
    pub model_step_id: Id,
    /// Host requirements; saved history supplies the previous route and failure.
    pub routing: RouteRequest,
}

/// Current Host context for a bounded route-specific projection.
pub struct ModelProjectionContext {
    /// Exact authenticated namespace.
    pub scope: crate::Scope,
    /// Current principal for any separately authorized context reads.
    pub principal_ref: Id,
    /// Current Host capability grant.
    pub capability_grant_ref: Id,
    /// Cancelled when projection completes, fails, or its caller stops.
    pub cancellation: CancellationToken,
    /// Finite deadline inherited from the Run.
    pub deadline: tokio::time::Instant,
}

/// A fully prepared request and its route-specific input-token estimate.
#[derive(Debug, Clone)]
pub struct ProjectedModelRequest {
    /// Exact selected route, purpose, logical step, options and output budget.
    pub request: ModelRequest,
    /// Host/tokenizer estimate for this final projection, not a byte count.
    pub input_tokens: u64,
}

/// Trusted Host projection port. It preserves required context and builds a fresh
/// request for the exact selected route; it must not invoke a model or run a Tool.
pub trait ModelRequestProjector: Send + Sync {
    /// Project immutable transcript/context into the selected provider's contract.
    fn project<'a>(
        &'a self,
        selection: &'a RouteSelection,
        input: &'a RoutedModelInput,
        context: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ProjectedModelRequest>;
}

impl ModelExchange {
    /// Resolve, project, inspect, and execute a logical model step with finite
    /// explicit fallback. The same Run budgets account for every recovery and
    /// physical call. This does not advance an agent loop or execute Tools.
    ///
    /// The catalog/policy snapshot is pinned before the first physical attempt.
    /// Completed responses for this step are reused after current authorization
    /// and request-identity checks. Unresolved attempts require explicit recovery;
    /// this method never silently resends them. Projection is trusted Host code:
    /// it must preserve required context when changing providers.
    pub async fn generate_routed(
        &self,
        router: &dyn ModelRouter,
        input: &RoutedModelInput,
        projector: &dyn ModelRequestProjector,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<Guarded<ModelExchangeOutcome>, ContractError> {
        if self.inspector.is_none() {
            return Err(failure(ErrorCode::InvalidConfiguration, "model.inspector"));
        }
        if input.routing.scope != context.data.scope || &input.routing.scope != budget.scope() {
            return Err(failure(ErrorCode::AccessDenied, "routing.scope"));
        }
        if input.routing.previous_route.is_some() || input.routing.previous_failure.is_some() {
            return Err(failure(
                ErrorCode::ModelRoutingInvalid,
                "routing.history_is_stored",
            ));
        }
        budget.check_boundary().await?;
        let pinned = router.snapshot().clone();
        self.pin_routing(&pinned, context, budget).await?;
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        if input.routing.model_binding != saved.snapshot.profile.profile().model_binding {
            return Err(failure(
                ErrorCode::ModelRouteDenied,
                "routing.profile_binding",
            ));
        }
        if input.routing.purpose == ModelPurpose::Agent
            && input.routing.options != saved.snapshot.request.model_options
        {
            return Err(failure(ErrorCode::RequestConflict, "routing.model_options"));
        }
        if saved.snapshot.tool_ledger.iter().any(|entry| !matches!(&entry.state,
            ToolCallState::Settled { result } if result.status != crate::ToolResultStatus::Unknown && result.effect != crate::ToolEffect::Unknown
        )) {
            return Err(failure(ErrorCode::InvalidTransition, "routing.unsettled_tools"));
        }
        self.pin_step_input(input, context, budget).await?;
        if saved.snapshot.model_ledger.iter().any(|attempt| {
            matches!(
                attempt.state,
                ModelAttemptState::Reserved {} | ModelAttemptState::Unknown {}
            )
        }) {
            return Err(failure(
                ErrorCode::ModelAttemptUnresolved,
                "routing.attempt",
            ));
        }
        let previous = saved
            .snapshot
            .model_ledger
            .iter()
            .rev()
            .find(|attempt| attempt.model_step_id == input.model_step_id)
            .cloned();
        let mut routing = input.routing.clone();
        let mut replay = None;
        if let Some(previous) = previous {
            if previous.purpose != routing.purpose {
                return Err(failure(
                    ErrorCode::ModelRoutingMismatch,
                    "routing.step_purpose",
                ));
            }
            routing.previous_route = Some(previous.route.clone());
            match previous.state {
                ModelAttemptState::Completed {} => replay = Some(previous),
                ModelAttemptState::Failed { kind } => routing.previous_failure = Some(kind),
                ModelAttemptState::Reserved {} | ModelAttemptState::Unknown {} => {
                    return Err(failure(
                        ErrorCode::ModelAttemptUnresolved,
                        "routing.attempt",
                    ));
                }
            }
        }
        // A custom router must also advance monotonically through the pinned list.
        let mut previous_index = None;
        for _ in 0..=crate::MAX_ROUTE_FALLBACKS {
            budget.check_boundary().await?;
            if context.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let selection = external(
                async { router.resolve(&routing).await },
                context,
                budget,
                ErrorCode::ModelRoutingInvalid,
            )
            .await?;
            if router.snapshot().digest() != pinned.digest() {
                return Err(failure(ErrorCode::ModelRoutingMismatch, "routing.snapshot"));
            }
            pinned.validate_selection(&routing, &selection)?;
            if previous_index.is_some_and(|index| selection.candidate_index <= index) {
                return Err(failure(
                    ErrorCode::ModelRoutingMismatch,
                    "routing.fallback_order",
                ));
            }
            if matches!(selection.reason, RouteSelectionReason::Fallback { .. }) {
                budget.reserve(ReservationKind::Recovery {}).await?;
            }
            // Authorize the exact destination before a Host projection or metadata lookup.
            if let Guarded::ApprovalRequired(challenge) = self
                .authorize_route(&selection.route, routing.purpose, context, budget)
                .await?
            {
                return Ok(Guarded::ApprovalRequired(challenge));
            }
            let projection_context = ModelProjectionContext {
                scope: budget.scope().clone(),
                principal_ref: context.data.principal_ref.clone(),
                capability_grant_ref: context.data.capability_grant_ref.clone(),
                cancellation: budget.cancellation().child_token(),
                deadline: budget.call_deadline()?,
            };
            let _cancel = projection_context.cancellation.clone().drop_guard();
            let prepared = external(
                async {
                    projector
                        .project(&selection, input, &projection_context)
                        .await
                },
                context,
                budget,
                ErrorCode::ModelContextIncompatible,
            )
            .await?;
            projection_context.cancellation.cancel();
            validate_projection(&prepared.request, input, &selection)?;
            // Validate the final route-specific estimate, not the earlier candidate estimate.
            let mut final_requirements = routing.clone();
            final_requirements.input_tokens = prepared.input_tokens;
            final_requirements
                .required_capabilities
                .extend(prepared.request.required_capabilities());
            final_requirements.previous_route = Some(selection.route.clone());
            final_requirements.previous_failure = None;
            let mut final_selection = selection.clone();
            final_selection.reason = RouteSelectionReason::Reuse;
            final_selection.request_digest = final_requirements.digest();
            pinned.validate_selection(&final_requirements, &final_selection)?;
            if let Some(previous) = replay.take() {
                let mut physical = prepared.request.clone();
                physical.request_id = previous.attempt_id;
                if physical.digest() != previous.request_digest {
                    return Err(failure(
                        ErrorCode::RequestConflict,
                        "routing.replay_projection",
                    ));
                }
                if let Guarded::ApprovalRequired(challenge) =
                    self.authorize(&prepared.request, context, budget).await?
                {
                    return Ok(Guarded::ApprovalRequired(challenge));
                }
                let reference = previous
                    .response_ref
                    .ok_or_else(|| failure(ErrorCode::InvalidSnapshot, "routing.response"))?;
                let record = budget
                    .store()
                    .read_record(budget.scope(), &reference)
                    .await?;
                let response: StoredModelResponse = serde_json::from_value(record.value().clone())
                    .map_err(|_| failure(ErrorCode::InvalidSnapshot, "routing.response"))?;
                budget.check_boundary().await?;
                if context.cancellation.is_cancelled() {
                    return Err(cancelled());
                }
                return Ok(Guarded::Completed(response.outcome));
            }
            let rule = pinned
                .policy()
                .rules
                .iter()
                .find(|rule| {
                    rule.model_binding == routing.model_binding && rule.purpose == routing.purpose
                })
                .ok_or_else(|| failure(ErrorCode::ModelRouteDenied, "routing.rule"))?;
            let version_policy = if selection.route.version_semantics
                == crate::VersionSemantics::Pinned
                || rule.version_policy == VersionPolicy::RequirePinned
                || routing.version_policy == VersionPolicy::RequirePinned
            {
                VersionPolicy::RequirePinned
            } else {
                VersionPolicy::AllowMutable
            };
            let result = self
                .generate_inner(
                    &prepared.request,
                    context,
                    budget,
                    Some((&selection, version_policy)),
                )
                .await;
            let cause = match result {
                Ok(Guarded::Completed(ModelExchangeOutcome::Failed { failure: ref error })) => {
                    error.kind
                }
                Err(ref error) if error.code == ErrorCode::ModelVersionDrift => {
                    ModelFailureKind::VersionDrift
                }
                Err(ref error) if error.code == ErrorCode::ModelUnavailable => {
                    ModelFailureKind::Unavailable
                }
                other => return other,
            };
            if !rule.fallback_on.contains(&cause) {
                return result;
            }
            previous_index = Some(selection.candidate_index);
            routing.previous_route = Some(selection.route);
            routing.previous_failure = Some(cause);
        }
        Err(failure(
            ErrorCode::ModelRoutesExhausted,
            "routing.candidates",
        ))
    }

    async fn pin_routing(
        &self,
        routing: &RoutingSnapshot,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        if routing.scope() != budget.scope() {
            return Err(failure(ErrorCode::AccessDenied, "routing.scope"));
        }
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        if let Some(reference) = &saved.snapshot.routing_snapshot_ref {
            let record = budget
                .store()
                .read_record(budget.scope(), reference)
                .await?;
            let restored = RoutingSnapshot::restore(
                &serde_json::to_string(record.value()).map_err(|_| revision_error())?,
                budget.scope(),
                &reference.digest,
            )?;
            if restored.digest() != routing.digest() {
                return Err(failure(
                    ErrorCode::ModelRoutingMismatch,
                    "routing.pinned_snapshot",
                ));
            }
            return Ok(());
        }
        if saved.snapshot.usage.model_calls != 0 {
            return Err(failure(
                ErrorCode::ModelRoutingMismatch,
                "routing.already_started",
            ));
        }
        if context.cancellation.is_cancelled() {
            return Err(cancelled());
        }
        budget.check_boundary().await?;
        let record = ProtectedRecord::new(
            Id::new(format!("model-routing-{}", budget.run_id()))?,
            1,
            serde_json::to_value(routing).map_err(|_| revision_error())?,
        );
        let mut snapshot = saved.snapshot;
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(revision_error)?;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        snapshot.routing_snapshot_ref = Some(record.reference().clone());
        budget
            .store()
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages: vec![],
                    events: vec![],
                    records: vec![record],
                },
            )
            .await?;
        budget.check_boundary().await?;
        Ok(())
    }

    async fn pin_step_input(
        &self,
        input: &RoutedModelInput,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let key =
            crate::canonical_digest(&serde_json::json!([budget.run_id(), input.model_step_id]));
        let record = ProtectedRecord::new(
            Id::new(format!("model-step-{key}"))?,
            1,
            serde_json::json!({"schema_version":"wickle.model-step.v1", "run_id":budget.run_id(), "input":input}),
        );
        match budget
            .store()
            .read_record(budget.scope(), record.reference())
            .await
        {
            Ok(_) => return Ok(()),
            Err(error) if error.code == ErrorCode::StateNotFound => {}
            Err(error) if error.code == ErrorCode::RecordConflict => {
                return Err(failure(ErrorCode::RequestConflict, "routing.step_input"));
            }
            Err(error) => return Err(error),
        }
        budget.check_boundary().await?;
        if context.cancellation.is_cancelled() {
            return Err(cancelled());
        }
        let mut snapshot = budget
            .store()
            .load(budget.scope(), budget.run_id())
            .await?
            .snapshot;
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(revision_error)?;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        budget
            .store()
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages: vec![],
                    events: vec![],
                    records: vec![record],
                },
            )
            .await?;
        budget.check_boundary().await?;
        Ok(())
    }

    pub(super) async fn inspect_route(
        &self,
        request: &ModelRequest,
        version_policy: VersionPolicy,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<crate::ModelRouteObservation, ContractError> {
        let (inspector, timeout) = self
            .inspector
            .as_ref()
            .ok_or_else(|| failure(ErrorCode::InvalidConfiguration, "model.inspector"))?;
        budget.check_boundary().await?;
        let deadline = tokio::time::Instant::now()
            .checked_add(*timeout)
            .ok_or_else(|| failure(ErrorCode::InvalidConfiguration, "model.inspection_timeout"))?
            .min(budget.call_deadline()?);
        let inspection = ModelInspectionContext {
            scope: budget.scope().clone(),
            principal_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
            cancellation: budget.cancellation().child_token(),
            deadline,
        };
        let _cancel = inspection.cancellation.clone().drop_guard();
        let observation = tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => return Err(failure(ErrorCode::ModelInspectionUnavailable, "model.inspection_timeout")),
            result = external(async { inspector.inspect(&request.route, &inspection).await }, context, budget, ErrorCode::ModelInspectionUnavailable) => result?,
        };
        inspection.cancellation.cancel();
        observation.validate(&request.route, version_policy)?;
        budget.check_boundary().await?;
        if context.cancellation.is_cancelled() {
            return Err(cancelled());
        }
        Ok(observation)
    }

    async fn authorize_route(
        &self,
        route: &ResolvedModelRoute,
        purpose: ModelPurpose,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<Guarded<()>, ContractError> {
        let request = PolicyRequest {
            owner_scope: budget.scope().clone(),
            resource_id: budget.run_id().clone(),
            action: PolicyAction::InvokeModel {
                route: Box::new(route.clone()),
                purpose,
            },
        };
        external(
            self.policy.guard(
                &request,
                context,
                Some(budget.call_deadline()?),
                None,
                || async { Ok(()) },
            ),
            context,
            budget,
            ErrorCode::PolicyUnavailable,
        )
        .await
    }
}

fn validate_projection(
    request: &ModelRequest,
    input: &RoutedModelInput,
    selection: &RouteSelection,
) -> Result<(), ContractError> {
    if request.request_id != input.model_step_id
        || request.route != selection.route
        || request.purpose != input.routing.purpose
        || request.options != input.routing.options
        || request.max_output_tokens != input.routing.max_output_tokens
    {
        return Err(failure(
            ErrorCode::ModelContextIncompatible,
            "routing.projection",
        ));
    }
    request.validate()
}

async fn external<T>(
    future: impl std::future::Future<Output = Result<T, ContractError>>,
    context: &ExecutionContext,
    budget: &RunBudget,
    code: ErrorCode,
) -> Result<T, ContractError> {
    tokio::select! {
        biased;
        _ = context.cancellation.cancelled() => Err(cancelled()),
        stopped = budget.wait_for_cancellation_or_deadline() => match stopped {
            Err(error) => Err(error), Ok(()) => Err(failure(ErrorCode::DeadlineExceeded, "model.routing")),
        },
        result = AssertUnwindSafe(future).catch_unwind() => result.map_err(|_| failure(code, "model.routing_callback"))?.map_err(|error| failure(error.code, "model.routing_callback")),
    }
}

fn failure(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}

pub(super) fn reason_code(reason: RouteSelectionReason) -> &'static str {
    match reason {
        RouteSelectionReason::Initial => "initial_route",
        RouteSelectionReason::Reuse => "saved_route_reuse",
        RouteSelectionReason::Fallback { failure } => match failure {
            ModelFailureKind::Timeout => "fallback_timeout",
            ModelFailureKind::RateLimited => "fallback_rate_limited",
            ModelFailureKind::Transport => "fallback_transport",
            ModelFailureKind::Protocol => "fallback_protocol",
            ModelFailureKind::ContextOverflow => "fallback_context_overflow",
            ModelFailureKind::Authentication => "fallback_authentication",
            ModelFailureKind::Unsupported => "fallback_unsupported",
            ModelFailureKind::Unavailable => "fallback_unavailable",
            ModelFailureKind::VersionDrift => "fallback_version_drift",
        },
    }
}
```

## `crates/wickle/src/run.rs`

```rust
use crate::{
    ArtifactRef, AttemptReservation, CompletionPolicy, ContractError, ErrorCode, Failure, Id,
    InputContent, JsonDigest, JsonObject, ModelAttemptState, ModelInvocationRecord, RecordRef,
    ReservationKind, ResolvedProfile, RunLimits, RunTiming, Scope, ToolCall, ToolResult,
    VersionedRef,
    serialization::{data_digest, decode, optional},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
};

/// Current run checkpoint format, independent of profile and event formats.
pub const RUN_SNAPSHOT_SCHEMA_VERSION: &str = "wickle.run-snapshot.v1";
/// Current durable event format.
pub const RUN_EVENT_SCHEMA_VERSION: &str = "wickle.run-event.v1";

/// Supported run checkpoint versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunSnapshotSchemaVersion {
    /// First checkpoint format.
    #[serde(rename = "wickle.run-snapshot.v1")]
    V1,
}

/// Supported session checkpoint versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionSchemaVersion {
    /// First session format.
    #[serde(rename = "wickle.session-snapshot.v1")]
    V1,
}

/// Supported durable event versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunEventSchemaVersion {
    /// First durable event format.
    #[serde(rename = "wickle.run-event.v1")]
    V1,
}

/// Why a Host submitted a run. Trigger data does not authenticate its sender.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunTrigger {
    /// Direct user request.
    User {},
    /// An event verified by the Host.
    Event {
        /// Host event identity.
        source_id: Id,
    },
    /// A schedule occurrence computed by the Host.
    Schedule {
        /// Occurrence identity, not a cron expression for the core to run.
        source_id: Id,
    },
    /// Child execution requested by a Host orchestration layer.
    Child {
        /// Parent run identity. Execution capability is checked separately.
        parent_run_id: Id,
    },
}

/// Caller request data; trusted execution context is supplied separately.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunRequest {
    /// Host-generated idempotency identity within scope and session.
    pub request_id: Id,
    /// Session whose pinned profile will be used.
    pub session_id: Id,
    /// User data, without injected tool calls or provider continuation state.
    pub input: Vec<InputContent>,
    /// Verified trigger provenance.
    pub trigger: RunTrigger,
    /// Logical model options authorized by the Host and pinned with the admitted request.
    /// Catalog schemas define supported keys; credentials and raw provider bodies do not belong here.
    #[serde(default, skip_serializing_if = "JsonObject::is_empty")]
    pub model_options: JsonObject,
    /// Optional output override; Host policy must authorize its use.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub output_contract: Option<crate::OutputContract>,
}

impl RunRequest {
    /// Decode caller data without granting authority or creating a run.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        decode(input, None)
    }
}

/// Exact request for human input, bound to the originating call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputRequest {
    /// Stable input request identity.
    pub input_request_id: Id,
    /// Call that must receive the answer.
    pub call_id: Id,
    /// Question shown by the Host.
    pub question: String,
    /// Optional exact schema for the answer.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub schema_ref: Option<VersionedRef>,
}

/// Exact operation or candidate to which approval applies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApprovalTarget {
    /// Tool approval binds the final execution input digest.
    Tool {
        /// Core call identity.
        call_id: Id,
        /// Digest that includes system-owned inputs.
        binding_digest: JsonDigest,
    },
    /// Review of a fixed candidate.
    Candidate {
        /// Stored candidate identity.
        candidate_ref: RecordRef,
        /// Exact verifier definition.
        verifier_ref: VersionedRef,
    },
}

/// Typed reason a run waits without making additional model calls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitTarget {
    /// Explicit approval of fixed data.
    Approval {
        /// Approval target.
        target: ApprovalTarget,
    },
    /// Answer to a recorded input request.
    Input {
        /// Input request.
        request: InputRequest,
    },
    /// Confirmation of an uncertain external effect.
    External {
        /// Call with uncertain effect.
        call_id: Id,
        /// Stable external idempotency/reconciliation key.
        effect_key: Id,
    },
}

/// Saved wait identity, target, and optional expiry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitState {
    /// Unique wait identity used to reject stale answers.
    pub wait_id: Id,
    /// Data or effect being awaited.
    pub target: WaitTarget,
    /// UTC milliseconds since Unix epoch; the run deadline still applies.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub expires_at_ms: Option<i64>,
}

/// A specific answer or recovery request; none of these grant execution permission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResumeAction {
    /// Accept a fixed approval target.
    Approve {
        /// Matching wait identity.
        wait_id: Id,
        /// Exact target presented for approval.
        target: ApprovalTarget,
    },
    /// Reject a fixed approval target.
    Deny {
        /// Matching wait identity.
        wait_id: Id,
        /// Exact target presented for approval.
        target: ApprovalTarget,
        /// User-supplied rejection reason.
        reason: String,
    },
    /// Supply data for a recorded input request.
    Input {
        /// Matching wait identity.
        wait_id: Id,
        /// Answer data, validated against the saved request by the resume handler.
        answer: serde_json::Value,
    },
    /// Supply a protected receipt for an external effect.
    External {
        /// Matching wait identity.
        wait_id: Id,
        /// Evidence to be verified by the authorized handler.
        receipt_ref: RecordRef,
    },
    /// Resume an interrupted nonterminal execution.
    Recover {
        /// Host-verified recovery evidence.
        recovery_ref: RecordRef,
    },
}

/// Idempotent resume command; state/policy enforcement is performed by the driver.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeCommand {
    /// Run to resume.
    pub run_id: Id,
    /// Revision the caller observed.
    pub expected_revision: u64,
    /// Deduplicates retries of the same decision.
    pub command_id: Id,
    /// Typed decision or recovery evidence.
    pub action: ResumeAction,
}

impl ResumeCommand {
    /// Decode an unambiguous command without executing or authorizing it.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        decode(input, None)
    }
}

/// Public run status categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Actively processing.
    Running,
    /// Persisted wait.
    Waiting,
    /// Completion policy satisfied.
    Succeeded,
    /// Unrecoverable failure.
    Failed,
    /// Explicit cancellation completed.
    Cancelled,
    /// A finite execution budget was exhausted.
    Exhausted,
}

impl RunStatus {
    /// Whether this status cannot be resumed as the same run.
    pub fn is_terminal(self) -> bool {
        !matches!(self, Self::Running | Self::Waiting)
    }
}

/// Driver phases; transition execution belongs to the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPhase {
    /// Admission validation and initial storage.
    Admission,
    /// Context and request preparation.
    Prepare,
    /// One model invocation.
    Model,
    /// Tool round processing.
    Tool,
    /// Output and completion checks.
    Verify,
    /// Saved wait.
    Waiting,
    /// Terminal outcome committed.
    Finish,
}

/// Stored budget consumption; usage measurement and reservation happen elsewhere.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetUsage {
    /// Reserved physical model attempts.
    pub model_calls: u64,
    /// Reserved physical tool attempts.
    pub tool_attempts: u64,
    /// Candidate repair attempts.
    pub repair_attempts: u64,
    /// Execution recovery attempts.
    pub recovery_attempts: u64,
    /// Elapsed milliseconds including waits.
    pub elapsed_ms: u64,
}

/// The budget that stopped an execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetKind {
    /// Model attempts.
    ModelCalls,
    /// Tool attempts.
    ToolAttempts,
    /// Repairs.
    RepairAttempts,
    /// Recoveries.
    RecoveryAttempts,
    /// Elapsed wall time.
    Elapsed,
}

/// What supports a successful outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionBasis {
    /// The model ended its turn; external business success is not asserted.
    TurnEnded,
    /// A pinned verifier accepted the candidate.
    Verified,
}

/// Recorded verifier classification, separate from transport errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationVerdict {
    /// Candidate accepted.
    Pass,
    /// Candidate needs revision.
    Revise,
    /// Human review required.
    Wait,
    /// Candidate rejected.
    Fail,
}

/// Evidence supporting the recorded verifier decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationSummary {
    /// Verifier actually used.
    pub verifier_ref: VersionedRef,
    /// Exact evaluation criteria.
    pub criteria_ref: VersionedRef,
    /// Decision classification.
    pub verdict: VerificationVerdict,
    /// Protected evidence records.
    pub evidence: Vec<RecordRef>,
}

/// Outcome-specific data. Success always names its completion basis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutcomeResult {
    /// Saved waiting outcome for the current execution segment.
    Waiting {
        /// Wait data.
        wait: WaitState,
    },
    /// Completion policy satisfied.
    Succeeded {
        /// Why completion was accepted.
        completion_basis: CompletionBasis,
    },
    /// Execution failed.
    Failed {
        /// Classified failure.
        failure: Failure,
    },
    /// Cancellation completed; existing effects remain in their records.
    Cancelled {
        /// Cancellation reason.
        reason: String,
    },
    /// Execution budget exhausted.
    Exhausted {
        /// Exhausted budget.
        budget: BudgetKind,
    },
}

impl OutcomeResult {
    /// Public status of this outcome.
    pub fn status(&self) -> RunStatus {
        match self {
            Self::Waiting { .. } => RunStatus::Waiting,
            Self::Succeeded { .. } => RunStatus::Succeeded,
            Self::Failed { .. } => RunStatus::Failed,
            Self::Cancelled { .. } => RunStatus::Cancelled,
            Self::Exhausted { .. } => RunStatus::Exhausted,
        }
    }
}

/// Stored outcome; it is the authority for completion, not an event or text delta.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunOutcome {
    /// Outcome classification and required status-specific data.
    pub result: OutcomeResult,
    /// Final or partial output.
    pub output: Vec<InputContent>,
    /// Produced artifact metadata.
    pub artifacts: Vec<ArtifactRef>,
    /// Consumption recorded at this checkpoint.
    pub usage: BudgetUsage,
    /// Exact checkpoint revision.
    pub checkpoint_revision: u64,
    /// Optional verifier evidence; mandatory for verified success.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub verification: Option<VerificationSummary>,
    /// Effects that must not be blindly repeated.
    pub unresolved_effects: Vec<RecordRef>,
}

impl RunOutcome {
    /// Check required evidence for a verified success.
    pub fn validate(&self) -> Result<(), ContractError> {
        if matches!(self.result, OutcomeResult::Succeeded { .. })
            && !self.unresolved_effects.is_empty()
        {
            return Err(ContractError::new(
                ErrorCode::InvalidContract,
                "outcome.unresolved_effects",
            ));
        }
        if matches!(
            self.result,
            OutcomeResult::Succeeded {
                completion_basis: CompletionBasis::Verified
            }
        ) && !self
            .verification
            .as_ref()
            .is_some_and(|v| v.verdict == VerificationVerdict::Pass)
        {
            return Err(ContractError::new(
                ErrorCode::InvalidContract,
                "outcome.verification",
            ));
        }
        Ok(())
    }
}

/// State of one planned tool call; this does not execute state transitions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolCallState {
    /// Plan is saved and no dispatch is recorded.
    Planned {},
    /// Dispatch was reserved and may have happened.
    Dispatching {
        /// Physical attempt identity.
        attempt_id: Id,
        /// Stable key for external deduplication/reconciliation.
        idempotency_key: Id,
    },
    /// A charged attempt was stopped for approval before entering the executor.
    ApprovalPending {
        /// Reservation that remains charged even though execution did not start.
        attempt_id: Id,
        /// Frozen effect key reused if execution is later authorized.
        idempotency_key: Id,
    },
    /// Result was recorded.
    Settled {
        /// Paired tool result.
        result: ToolResult,
    },
    /// Effect is unknown after interruption.
    Unknown {
        /// Uncertain attempt identity.
        attempt_id: Id,
        /// Original external effect key.
        idempotency_key: Id,
    },
}

/// Planned model arguments and the corresponding dispatch/result state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolLedgerEntry {
    /// Original call and protected bound-input reference.
    pub call: ToolCall,
    /// Dispatch/result state.
    pub state: ToolCallState,
}

/// Protected system-input storage reference and versions used in request identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemInputSnapshotRef {
    /// Protected storage location; excluded from logical request identity.
    pub snapshot_ref: RecordRef,
    /// Digest of the validated owned values, including an explicit empty map.
    pub values_digest: JsonDigest,
    /// Exact registered input-definition versions.
    pub definition_versions: BTreeMap<Id, Id>,
}

/// Digest of logical start input, independent of a storage record's location.
/// A missing system-input map is the empty map for start; resume preserves the
/// separate missing/empty distinction in ExecutionContextData.
pub fn admission_digest(
    request: &RunRequest,
    profile: &ResolvedProfile,
    system_inputs: Option<&SystemInputSnapshotRef>,
) -> JsonDigest {
    let empty_digest = crate::canonical_digest(&serde_json::json!({}));
    let empty_versions = BTreeMap::new();
    let (values, versions) = system_inputs
        .map(|s| (&s.values_digest, &s.definition_versions))
        .unwrap_or((&empty_digest, &empty_versions));
    data_digest(&(request, profile.profile_digest(), values, versions))
}

/// Session metadata pinned across requests. A store enforces the active-run rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSnapshot {
    /// Session document version.
    pub schema_version: SessionSchemaVersion,
    /// Session identity.
    pub session_id: Id,
    /// Owning scope.
    pub scope: Scope,
    /// Pinned profile identity.
    pub profile_digest: JsonDigest,
    /// Pinned prompt data reference.
    pub prompt_snapshot: RecordRef,
    /// Current transcript revision.
    pub transcript_revision: u64,
    /// One active running/waiting run, or omission when none exists.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub active_run_id: Option<Id>,
}

/// Saved context-source execution position for retry and resume reuse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceExecutionState {
    /// Exact source selection, including adapter binding when applicable.
    pub source: crate::ContextSourceRef,
    /// Stable context request identity.
    pub context_request_id: Id,
    /// Collection trigger.
    pub trigger: crate::ContextTrigger,
    /// Required for a before_model collection.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub model_step_id: Option<Id>,
    /// Committed context batch, including empty/unavailable results.
    pub batch_ref: RecordRef,
}

/// Run checkpoint DTO. Use `from_json` or `validate` at the storage boundary.
/// Protected inputs are references, not automatically exposed execution arguments.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSnapshot {
    /// Checkpoint document version.
    pub schema_version: RunSnapshotSchemaVersion,
    /// Run identity.
    pub run_id: Id,
    /// Original caller request.
    pub request: RunRequest,
    /// Logical input digest used for deduplication.
    pub request_digest: JsonDigest,
    /// Scope used for storage, policy, tools, and resume.
    pub scope: Scope,
    /// Immutable profile and resolved definition identities.
    pub profile: ResolvedProfile,
    /// Current execution status.
    pub status: RunStatus,
    /// Current driver phase.
    pub phase: RunPhase,
    /// Current logical model step, if allocated.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub model_step_id: Option<Id>,
    /// Effective limits, no greater than profile limits.
    pub limits: RunLimits,
    /// Saved usage/reservations.
    pub usage: BudgetUsage,
    /// Original admission/deadline and persisted monotonic elapsed-time anchor.
    pub timing: RunTiming,
    /// Append-only charged attempt reservations, preserved across errors and resume.
    pub reservations: Vec<AttemptReservation>,
    /// Physical model attempt records.
    pub model_ledger: Vec<ModelInvocationRecord>,
    /// Saved tool plans and states.
    pub tool_ledger: Vec<ToolLedgerEntry>,
    /// Pinned, protected system values and their contract revisions.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub system_inputs: Option<SystemInputSnapshotRef>,
    /// Saved wait data, only while waiting.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub wait: Option<WaitState>,
    /// Last waiting or terminal outcome.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub outcome: Option<RunOutcome>,
    /// Pinned assembly metadata, without process-local handler objects.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub assembly_ref: Option<RecordRef>,
    /// Immutable catalog and routing policy used by this run's model calls.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub routing_snapshot_ref: Option<RecordRef>,
    /// Committed context batches.
    pub context_batches: Vec<RecordRef>,
    /// Saved collection positions.
    pub source_states: Vec<SourceExecutionState>,
    /// Compare-and-swap revision.
    pub revision: u64,
    /// Last durable event sequence; ephemeral deltas do not consume it.
    pub last_event_seq: u64,
}

impl RunSnapshot {
    /// Decode a known checkpoint version and verify static consistency.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        let snapshot: Self = decode(input, Some(RUN_SNAPSHOT_SCHEMA_VERSION))?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Check static checkpoint invariants without performing recovery or authorization.
    pub fn validate(&self) -> Result<(), ContractError> {
        let invalid = |path| ContractError::new(ErrorCode::InvalidSnapshot, path);
        crate::budget::validate_budget(self)?;
        if &self.scope != self.profile.scope()
            || self.request_digest
                != admission_digest(&self.request, &self.profile, self.system_inputs.as_ref())
        {
            return Err(invalid("request_digest"));
        }
        let requested = &self.profile.profile().limits;
        if self.limits.max_model_calls > requested.max_model_calls
            || self.limits.max_tool_attempts > requested.max_tool_attempts
            || self.limits.max_repair_attempts > requested.max_repair_attempts
            || self.limits.max_recovery_attempts > requested.max_recovery_attempts
            || self.limits.max_elapsed_ms > requested.max_elapsed_ms
        {
            return Err(invalid("limits"));
        }
        match self.status {
            RunStatus::Running
                if matches!(self.phase, RunPhase::Waiting | RunPhase::Finish)
                    || self.wait.is_some()
                    || self.outcome.is_some() =>
            {
                return Err(invalid("status"));
            }
            RunStatus::Waiting if self.phase != RunPhase::Waiting || self.wait.is_none() => {
                return Err(invalid("wait"));
            }
            s if s.is_terminal()
                && (self.phase != RunPhase::Finish
                    || self.wait.is_some()
                    || self.outcome.is_none()) =>
            {
                return Err(invalid("outcome"));
            }
            _ => {}
        }
        if let Some(outcome) = &self.outcome {
            outcome.validate()?;
            if outcome.result.status() != self.status
                || outcome.checkpoint_revision != self.revision
                || outcome.usage != self.usage
            {
                return Err(invalid("outcome"));
            }
            if let OutcomeResult::Waiting { wait } = &outcome.result {
                if self.wait.as_ref() != Some(wait) {
                    return Err(invalid("outcome.wait"));
                }
            }
            if let OutcomeResult::Succeeded { completion_basis } = &outcome.result {
                match (&self.profile.profile().completion_policy, completion_basis) {
                    (CompletionPolicy::TurnEnd {}, CompletionBasis::TurnEnded) => {}
                    (CompletionPolicy::Verified { verifier_ref }, CompletionBasis::Verified)
                        if outcome
                            .verification
                            .as_ref()
                            .is_some_and(|v| &v.verifier_ref == verifier_ref) => {}
                    _ => return Err(invalid("outcome.completion_basis")),
                }
            }
        }
        let mut calls = BTreeSet::new();
        for entry in &self.tool_ledger {
            if !calls.insert(&entry.call.call_id) {
                return Err(invalid("tool_ledger.call_id"));
            }
            let unregistered_safe = match &entry.state {
                ToolCallState::Planned {} => true,
                ToolCallState::Settled { result } => {
                    result.effect == crate::ToolEffect::NotApplied
                        && matches!(
                            result.status,
                            crate::ToolResultStatus::Failed
                                | crate::ToolResultStatus::Denied
                                | crate::ToolResultStatus::Cancelled
                        )
                }
                _ => false,
            };
            if entry.call.descriptor_digest.is_none()
                && (entry.call.bound_input_ref.is_some() || !unregistered_safe)
            {
                return Err(invalid("tool_ledger.unregistered"));
            }
            match &entry.state {
                ToolCallState::Dispatching { attempt_id, .. } | ToolCallState::Unknown { attempt_id, .. } | ToolCallState::ApprovalPending { attempt_id, .. }
                    if !self.reservations.iter().any(|reservation| &reservation.attempt_id == attempt_id
                        && matches!(&reservation.kind, ReservationKind::Tool { call_id } if call_id == &entry.call.call_id)) =>
                {
                    return Err(invalid("tool_ledger.reservation"));
                }
                ToolCallState::Dispatching { .. } | ToolCallState::Unknown { .. } | ToolCallState::ApprovalPending { .. }
                    if entry.call.bound_input_ref.is_none() =>
                {
                    return Err(invalid("tool_ledger.bound_input_ref"));
                }
                ToolCallState::Settled { result } if result.call_id != entry.call.call_id => {
                    return Err(invalid("tool_ledger.result.call_id"));
                }
                _ => {}
            }
            if self.status == RunStatus::Succeeded
                && !matches!(&entry.state, ToolCallState::Settled { result } if result.status != crate::ToolResultStatus::Unknown && result.effect != crate::ToolEffect::Unknown)
            {
                return Err(invalid("tool_ledger.unsettled"));
            }
        }
        let mut attempts = BTreeSet::new();
        let model_reservations: BTreeMap<_, _> = self
            .reservations
            .iter()
            .filter_map(|reservation| match reservation.kind {
                ReservationKind::Model { purpose } => Some((&reservation.attempt_id, purpose)),
                _ => None,
            })
            .collect();
        for invocation in &self.model_ledger {
            if invocation.run_id != self.run_id || !attempts.insert(&invocation.attempt_id) {
                return Err(invalid("model_ledger.attempt_id"));
            }
            if model_reservations.get(&invocation.attempt_id) != Some(&invocation.purpose) {
                return Err(invalid("model_ledger.reservation"));
            }
            let settled = matches!(
                invocation.state,
                ModelAttemptState::Completed {} | ModelAttemptState::Failed { .. }
            );
            if settled != invocation.response_ref.is_some() {
                return Err(invalid("model_ledger.response_ref"));
            }
        }
        if self
            .source_states
            .iter()
            .any(|s| (s.trigger == crate::ContextTrigger::BeforeModel) != s.model_step_id.is_some())
        {
            return Err(invalid("source_states.model_step_id"));
        }
        Ok(())
    }
}

/// Durable facts reference stored records rather than copying protected inputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum RunEventPayload {
    /// Admission committed.
    #[serde(rename = "run.started")]
    RunStarted {
        /// Accepted request reference.
        request_ref: RecordRef,
        /// Pinned profile identity.
        profile_digest: JsonDigest,
    },
    /// Tool plan committed.
    #[serde(rename = "tool.planned")]
    ToolPlanned {
        /// Protected call record.
        call_ref: RecordRef,
    },
    /// Tool result committed.
    #[serde(rename = "tool.settled")]
    ToolSettled {
        /// Protected result record.
        result_ref: RecordRef,
    },
    /// A recorded result cannot establish whether an external operation applied.
    #[serde(rename = "tool.unresolved")]
    ToolUnresolved {
        /// Protected paired Unknown result.
        result_ref: RecordRef,
        /// Uncertain physical attempt whose reservation remains charged.
        attempt_id: Id,
        /// Original external effect key, retained for reconciliation.
        idempotency_key: Id,
    },
    /// Verifier decision committed.
    #[serde(rename = "verification.completed")]
    VerificationCompleted {
        /// Recorded verification evidence.
        verification_ref: RecordRef,
    },
    /// Wait committed.
    #[serde(rename = "run.waiting")]
    RunWaiting {
        /// Recorded wait.
        wait_ref: RecordRef,
    },
    /// Resume command consumed.
    #[serde(rename = "run.resumed")]
    RunResumed {
        /// Consumed command record.
        command_ref: RecordRef,
    },
    /// Terminal outcome committed.
    #[serde(rename = "run.finished")]
    RunFinished {
        /// Authoritative outcome record.
        outcome_ref: RecordRef,
    },
    /// Model route and invocation identity committed.
    #[serde(rename = "model.route_selected")]
    ModelRouteSelected {
        /// Invocation record.
        invocation_ref: RecordRef,
        /// Selected route identity.
        route_digest: JsonDigest,
    },
}

/// A durable event committed atomically with authoritative state by the store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunEvent {
    /// Independent event wire version.
    pub schema_version: RunEventSchemaVersion,
    /// Stable event identity for deduplication.
    pub event_id: Id,
    /// Owning scope.
    pub scope: Scope,
    /// Owning run.
    pub run_id: Id,
    /// Owning session.
    pub session_id: Id,
    /// Positive durable event sequence.
    pub seq: NonZeroU64,
    /// UTC milliseconds since Unix epoch.
    pub timestamp_ms: i64,
    /// Typed event data with authorized record references.
    pub payload: RunEventPayload,
}

impl RunEvent {
    /// Decode a known event format without replaying or dispatching it.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        decode(input, Some(RUN_EVENT_SCHEMA_VERSION))
    }
}

/// Non-durable presentation hints; these carry no durable sequence or completion claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum EphemeralEvent {
    /// Candidate text from an incomplete model response.
    CandidateTextDelta {
        /// Owning run.
        run_id: Id,
        /// Physical model attempt.
        attempt_id: Id,
        /// Candidate text, not a committed final answer.
        text: String,
    },
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
pub use checkpoint::{STATE_STORE_CHECKPOINT_VERSION, StateStoreCheckpoint};

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
pub trait StateStore: Send + Sync {
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
            if input.require_durable {
                return Err(error(ErrorCode::CapabilityUnsupported, "store.durable"));
            }
            check_scope(scope, &input.snapshot.scope)?;
            if scope != input.snapshot.profile.scope()
                || input.snapshot.request_digest
                    != admission_digest(
                        &input.snapshot.request,
                        &input.snapshot.profile,
                        input.snapshot.system_inputs.as_ref(),
                    )
            {
                return Err(error(ErrorCode::InvalidSnapshot, "request_digest"));
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
                if previous.snapshot.request_digest != input.snapshot.request_digest {
                    return Err(error(ErrorCode::RequestConflict, "request"));
                }
                return Ok(AdmissionResult {
                    created: false,
                    state: previous,
                });
            }
            input.snapshot.validate()?;
            if input.snapshot.revision != 0
                || input.snapshot.status != RunStatus::Running
                || input.snapshot.phase != RunPhase::Admission
                || !input.snapshot.model_ledger.is_empty()
                || !input.snapshot.tool_ledger.is_empty()
                || !input.snapshot.reservations.is_empty()
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
                active_run_id: Some(input.snapshot.run_id.clone()),
            };
            let result = StoredRun {
                snapshot: input.snapshot.clone(),
                session: session.clone(),
                messages: messages.clone(),
            };
            // All fallible checks precede these mutations.
            let state = scopes.entry(scope_key(scope)).or_default();
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
        Box::pin(async move {
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
        })
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
        Box::pin(async move {
            check_scope(scope, &input.snapshot.scope)?;
            let mut scopes = self.lock()?;
            let state = namespace(&scopes, scope)?;
            let run = state.runs.get(run_id).ok_or_else(not_found)?;
            validate_lease(run, scope, run_id, &input.lease, input.now_ms)?;
            if run.snapshot.revision != input.expected_revision {
                return Err(error(ErrorCode::RevisionConflict, "revision"));
            }
            validate_transition(&run.snapshot, &input.snapshot)?;
            let additions = validate_records(state, &input.records)?;
            validate_snapshot_refs(state, &additions, &input.snapshot)?;
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
            session_snapshot.transcript_revision = transcript_revision;
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
            let state = scopes
                .get_mut(&scope_key(scope))
                .expect("validated namespace");
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
        })
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
    let mut references = Vec::new();
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
            if invocation.inspection_ref.is_none()
                || !routing.policy().rules.iter().any(|rule| {
                    rule.model_binding == snapshot.profile.profile().model_binding
                        && rule.purpose == invocation.purpose
                        && (rule.primary == invocation.route.binding
                            || rule.fallbacks.contains(&invocation.route.binding))
                })
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
                || routing.policy().rules.iter().any(|rule| {
                    rule.model_binding == snapshot.profile.profile().model_binding
                        && rule.purpose == invocation.purpose
                        && rule.version_policy == crate::VersionPolicy::RequirePinned
                });
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
    references.extend(snapshot.assembly_ref.iter());
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
            let references = match content {
                ContentBlock::Content { .. } => Vec::new(),
                ContentBlock::ToolCall { call } => call.bound_input_ref.iter().collect(),
                ContentBlock::ToolResult { result } => tool_result_refs(result),
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
                // Additional verification history needs an explicit checkpoint contract.
                // A standalone event cannot substitute for the saved verification record.
                let verification: VerificationSummary =
                    event_record(state, additions, verification_ref)?;
                if snapshot
                    .outcome
                    .as_ref()
                    .and_then(|outcome| outcome.verification.as_ref())
                    != Some(&verification)
                {
                    return Err(error(
                        ErrorCode::InvalidEvent,
                        "events.verification_completed",
                    ));
                }
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
                let command: ResumeCommand = event_record(state, additions, command_ref)?;
                let previous = state.runs.get(&snapshot.run_id).ok_or_else(not_found)?;
                if snapshot.status != RunStatus::Running
                    || command.run_id != snapshot.run_id
                    || command.expected_revision != previous.snapshot.revision
                    || !resume_target_matches(&previous.snapshot, &command.action)
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_resumed"));
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
    {
        return Err(error(ErrorCode::InvalidEvent, "events"));
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

fn resume_target_matches(previous: &RunSnapshot, action: &ResumeAction) -> bool {
    match action {
        ResumeAction::Recover { .. } => previous.status == RunStatus::Running,
        ResumeAction::Approve { wait_id, target }
        | ResumeAction::Deny {
            wait_id, target, ..
        } => previous.wait.as_ref().is_some_and(|wait| {
            &wait.wait_id == wait_id
                && matches!(&wait.target, WaitTarget::Approval { target: saved } if saved == target)
        }),
        ResumeAction::Input { wait_id, .. } => previous.wait.as_ref().is_some_and(|wait| {
            &wait.wait_id == wait_id && matches!(wait.target, WaitTarget::Input { .. })
        }),
        ResumeAction::External { wait_id, .. } => previous.wait.as_ref().is_some_and(|wait| {
            &wait.wait_id == wait_id && matches!(wait.target, WaitTarget::External { .. })
        }),
    }
}

fn validate_transition(previous: &RunSnapshot, next: &RunSnapshot) -> Result<(), ContractError> {
    if previous.status.is_terminal() {
        return Err(error(ErrorCode::InvalidTransition, "run.status"));
    }
    next.validate()?;
    crate::budget::validate_budget_transition(previous, next)?;
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
        || (previous.assembly_ref.is_some() && previous.assembly_ref != next.assembly_ref)
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
            ) && matches!(new.state, ToolCallState::Planned { .. }))
            || (matches!(old.state, ToolCallState::Unknown { .. })
                && matches!(new.state, ToolCallState::ApprovalPending { .. }))
            || (matches!(old.state, ToolCallState::ApprovalPending { .. })
                && matches!(new.state, ToolCallState::Unknown { .. }))
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
                        ModelAttemptState::Completed {} | ModelAttemptState::Failed { .. }
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
```

## `crates/wickle/src/state/checkpoint.rs`

```rust
use super::*;
use crate::{JsonDigest, RunOutcome, RunRequest, serialization::data_digest};
use serde::{Deserialize, Serialize, Serializer};

/// Version of the protected, scope-local memory-store checkpoint format.
pub const STATE_STORE_CHECKPOINT_VERSION: &str = "wickle.state-store.v1";

/// An owned, validated scope graph. Explicit serialization contains protected
/// transcript and input data and is intended only for authorized storage adapters.
/// No caller can mutate its state or deserialize it without full validation.
#[derive(Clone)]
pub struct StateStoreCheckpoint {
    scope: Scope,
    state: ScopeState,
}

impl fmt::Debug for StateStoreCheckpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateStoreCheckpoint")
            .field("session_count", &self.state.sessions.len())
            .field("run_count", &self.state.runs.len())
            .field("record_count", &self.state.records.len())
            .finish_non_exhaustive()
    }
}

#[derive(Serialize)]
struct CheckpointView<'a> {
    schema_version: &'static str,
    scope: &'a Scope,
    sessions: Vec<SessionView<'a>>,
    runs: Vec<RunView<'a>>,
    records: Vec<RecordView<'a>>,
}
#[derive(Serialize)]
struct SessionView<'a> {
    snapshot: &'a SessionSnapshot,
    messages: &'a [Message],
}
#[derive(Serialize)]
struct RunView<'a> {
    snapshot: &'a RunSnapshot,
    events: &'a [RunEvent],
    lease: Option<LeaseData>,
    last_fencing_token: u64,
}
#[derive(Serialize)]
struct RecordView<'a> {
    reference: &'a RecordRef,
    value: &'a Value,
}

impl Serialize for StateStoreCheckpoint {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        CheckpointView {
            schema_version: STATE_STORE_CHECKPOINT_VERSION,
            scope: &self.scope,
            sessions: self
                .state
                .sessions
                .values()
                .map(|session| SessionView {
                    snapshot: &session.snapshot,
                    messages: &session.messages,
                })
                .collect(),
            runs: self
                .state
                .runs
                .values()
                .map(|run| RunView {
                    snapshot: &run.snapshot,
                    events: &run.events,
                    lease: run.lease.as_ref().map(LeaseData::from),
                    last_fencing_token: run.last_fencing_token,
                })
                .collect(),
            records: self
                .state
                .records
                .values()
                .map(|record| RecordView {
                    reference: record.reference(),
                    value: record.value(),
                })
                .collect(),
        }
        .serialize(serializer)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointData {
    schema_version: String,
    scope: Scope,
    sessions: Vec<SessionData>,
    runs: Vec<RunData>,
    records: Vec<RecordData>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionData {
    snapshot: SessionSnapshot,
    messages: Vec<Message>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunData {
    snapshot: RunSnapshot,
    events: Vec<RunEvent>,
    lease: Option<LeaseData>,
    last_fencing_token: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordData {
    reference: RecordRef,
    value: Value,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaseData {
    scope: Scope,
    run_id: Id,
    owner: Id,
    fencing_token: u64,
    expires_at_ms: i64,
}
impl From<&RunLease> for LeaseData {
    fn from(lease: &RunLease) -> Self {
        Self {
            scope: lease.scope.clone(),
            run_id: lease.run_id.clone(),
            owner: lease.owner.clone(),
            fencing_token: lease.fencing_token,
            expires_at_ms: lease.expires_at_ms,
        }
    }
}
impl From<LeaseData> for RunLease {
    fn from(lease: LeaseData) -> Self {
        Self {
            scope: lease.scope,
            run_id: lease.run_id,
            owner: lease.owner,
            fencing_token: lease.fencing_token,
            expires_at_ms: lease.expires_at_ms,
        }
    }
}

impl StateStoreCheckpoint {
    /// Exact namespace covered by the protected checkpoint.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Canonical identity of the serialized scope graph, excluding derived indexes.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }
    /// Parse a known version and validate scope, trusted digest, current state,
    /// historical typed records and derived indexes. Collection order is the stable
    /// key order produced by export; malformed or noncanonical images are rejected.
    pub fn from_json(
        input: &str,
        scope: &Scope,
        expected_digest: &JsonDigest,
    ) -> Result<Self, ContractError> {
        let value = crate::parse_json(input)?;
        if value.get("schema_version").and_then(Value::as_str)
            != Some(STATE_STORE_CHECKPOINT_VERSION)
        {
            return Err(error(
                ErrorCode::UnsupportedSchemaVersion,
                "checkpoint.schema_version",
            ));
        }
        if canonical_digest(&value) != *expected_digest {
            return Err(invalid("checkpoint.digest"));
        }
        let data: CheckpointData =
            serde_json::from_value(value).map_err(|_| invalid("checkpoint"))?;
        if &data.scope != scope {
            return Err(error(ErrorCode::AccessDenied, "checkpoint.scope"));
        }
        let checkpoint = restore_graph(data)?;
        if checkpoint.digest() != *expected_digest {
            return Err(invalid("checkpoint.canonical_form"));
        }
        Ok(checkpoint)
    }
}

impl MemoryStateStore {
    /// Copy only the requested namespace without performing I/O or exposing live
    /// mutable references. Unknown namespaces return StateNotFound.
    pub fn export_checkpoint(&self, scope: &Scope) -> Result<StateStoreCheckpoint, ContractError> {
        let scopes = self.lock()?;
        Ok(StateStoreCheckpoint {
            scope: scope.clone(),
            state: namespace(&scopes, scope)?.clone(),
        })
    }
    /// Move an already validated private checkpoint into a new process-local store.
    /// This does not perform a second graph validation or claim durable capabilities.
    pub fn from_checkpoint(checkpoint: StateStoreCheckpoint) -> Self {
        Self {
            scopes: Mutex::new(BTreeMap::from([(
                scope_key(&checkpoint.scope),
                checkpoint.state,
            )])),
        }
    }
}

fn restore_graph(data: CheckpointData) -> Result<StateStoreCheckpoint, ContractError> {
    if data.schema_version != STATE_STORE_CHECKPOINT_VERSION {
        return Err(invalid("checkpoint.schema_version"));
    }
    let mut state = ScopeState::default();
    for record in data.records {
        if canonical_digest(&record.value) != record.reference.digest {
            return Err(invalid("checkpoint.record_digest"));
        }
        let key = record_key(&record.reference);
        if state
            .records
            .insert(
                key,
                ProtectedRecord {
                    reference: record.reference,
                    value: record.value,
                },
            )
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_record"));
        }
    }
    for session in data.sessions {
        if session.snapshot.scope != data.scope {
            return Err(invalid("checkpoint.session_scope"));
        }
        if state
            .sessions
            .insert(
                session.snapshot.session_id.clone(),
                SessionState {
                    snapshot: session.snapshot,
                    messages: session.messages,
                },
            )
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_session"));
        }
    }
    for run in data.runs {
        if run.snapshot.scope != data.scope {
            return Err(invalid("checkpoint.run_scope"));
        }
        run.snapshot.validate()?;
        let session = state
            .sessions
            .get(&run.snapshot.request.session_id)
            .ok_or_else(|| invalid("checkpoint.run_session"))?;
        if session.snapshot.profile_digest != *run.snapshot.profile.profile_digest() {
            return Err(invalid("checkpoint.session_profile"));
        }
        if run.snapshot.revision > 0 && run.last_fencing_token == 0 {
            return Err(invalid("checkpoint.fencing_generation"));
        }
        if let Some(lease) = &run.lease {
            if lease.scope != data.scope
                || lease.run_id != run.snapshot.run_id
                || lease.fencing_token == 0
                || lease.fencing_token != run.last_fencing_token
                || run.snapshot.status.is_terminal()
            {
                return Err(invalid("checkpoint.lease"));
            }
        }
        if run.snapshot.revision == 0
            && (run.snapshot.status != RunStatus::Running
                || run.snapshot.phase != RunPhase::Admission
                || run.snapshot.usage != BudgetUsage::default()
                || !run.snapshot.reservations.is_empty()
                || !run.snapshot.model_ledger.is_empty()
                || !run.snapshot.tool_ledger.is_empty()
                || run.events.len() != 1)
        {
            return Err(invalid("checkpoint.admission"));
        }
        let request = (
            run.snapshot.request.session_id.clone(),
            run.snapshot.request.request_id.clone(),
        );
        if state
            .requests
            .insert(request, run.snapshot.run_id.clone())
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_request"));
        }
        let run_id = run.snapshot.run_id.clone();
        if state
            .runs
            .insert(
                run_id,
                RunState {
                    snapshot: run.snapshot,
                    events: run.events,
                    lease: run.lease.map(Into::into),
                    last_fencing_token: run.last_fencing_token,
                },
            )
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_run"));
        }
    }
    let empty = BTreeMap::new();
    let mut message_ids = BTreeSet::new();
    for session in state.sessions.values() {
        record_value(&state, &empty, &session.snapshot.prompt_snapshot)?;
        let active: Vec<_> = state
            .runs
            .values()
            .filter(|run| {
                run.snapshot.request.session_id == session.snapshot.session_id
                    && !run.snapshot.status.is_terminal()
            })
            .collect();
        if active.len() > 1
            || active.first().map(|run| &run.snapshot.run_id)
                != session.snapshot.active_run_id.as_ref()
        {
            return Err(invalid("checkpoint.active_run"));
        }
        if !state
            .runs
            .values()
            .any(|run| run.snapshot.request.session_id == session.snapshot.session_id)
        {
            return Err(invalid("checkpoint.orphan_session"));
        }
        let mut sequence = 0;
        let mut seen_runs = BTreeSet::new();
        let mut previous_run = None;
        for message in &session.messages {
            let run = state
                .runs
                .get(&message.run_id)
                .ok_or_else(|| invalid("checkpoint.message_run"))?;
            if run.snapshot.request.session_id != session.snapshot.session_id
                || !message_ids.insert(message.message_id.clone())
            {
                return Err(invalid("checkpoint.message_identity"));
            }
            if previous_run != Some(&message.run_id) {
                if !seen_runs.insert(&message.run_id) {
                    return Err(invalid("checkpoint.message_run_order"));
                }
                previous_run = Some(&message.run_id);
            }
            sequence = validate_messages(
                &state,
                &empty,
                &message.run_id,
                sequence,
                std::slice::from_ref(message),
            )?;
        }
        if sequence != session.snapshot.transcript_revision {
            return Err(invalid("checkpoint.transcript_revision"));
        }
        if let Some(active_run) = &session.snapshot.active_run_id {
            if seen_runs.contains(active_run) && previous_run != Some(active_run) {
                return Err(invalid("checkpoint.active_run_order"));
            }
        }
    }
    let mut event_ids = BTreeSet::new();
    for run in state.runs.values() {
        validate_snapshot_refs(&state, &empty, &run.snapshot)?;
        validate_history(&state, run, &mut event_ids)?;
    }
    state.message_ids = message_ids;
    state.event_ids = event_ids;
    Ok(StateStoreCheckpoint {
        scope: data.scope,
        state,
    })
}

fn validate_history(
    state: &ScopeState,
    run: &RunState,
    event_ids: &mut BTreeSet<Id>,
) -> Result<(), ContractError> {
    let empty = BTreeMap::new();
    let mut sequence = 0_u64;
    let mut started = 0;
    let mut finished = 0;
    let mut unresolved_keys = BTreeMap::new();
    for event in &run.events {
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| invalid("checkpoint.event_sequence"))?;
        if event.scope != run.snapshot.scope
            || event.run_id != run.snapshot.run_id
            || event.session_id != run.snapshot.request.session_id
            || event.seq.get() != sequence
            || !event_ids.insert(event.event_id.clone())
        {
            return Err(invalid("checkpoint.event_identity"));
        }
        match &event.payload {
            RunEventPayload::RunStarted {
                request_ref,
                profile_digest,
            } => {
                started += 1;
                let request: RunRequest = event_record(state, &empty, request_ref)?;
                if sequence != 1
                    || request != run.snapshot.request
                    || profile_digest != run.snapshot.profile.profile_digest()
                {
                    return Err(invalid("checkpoint.run_started"));
                }
            }
            RunEventPayload::RunFinished { outcome_ref } => {
                finished += 1;
                let outcome: RunOutcome = event_record(state, &empty, outcome_ref)?;
                if !run.snapshot.status.is_terminal()
                    || run.snapshot.outcome.as_ref() != Some(&outcome)
                {
                    return Err(invalid("checkpoint.run_finished"));
                }
            }
            RunEventPayload::ToolPlanned { call_ref } => {
                let mut call: ToolCall = event_record(state, &empty, call_ref)?;
                let current = run
                    .snapshot
                    .tool_ledger
                    .iter()
                    .find(|entry| entry.call.call_id == call.call_id)
                    .ok_or_else(|| invalid("checkpoint.tool_planned"))?;
                if call.bound_input_ref.is_none() {
                    call.bound_input_ref = current.call.bound_input_ref.clone();
                }
                if call != current.call {
                    return Err(invalid("checkpoint.tool_planned"));
                }
            }
            RunEventPayload::ToolSettled { result_ref } => {
                let result: ToolResult = event_record(state, &empty, result_ref)?;
                if !run.snapshot.tool_ledger.iter().any(|entry| matches!(&entry.state, ToolCallState::Settled { result: current } if current == &result)) {
                    return Err(invalid("checkpoint.tool_settled"));
                }
                validate_tool_pair(state, &run.snapshot, &[], &result)?;
                for reference in tool_result_refs(&result) {
                    record_value(state, &empty, reference)?;
                }
            }
            RunEventPayload::ToolUnresolved {
                result_ref,
                attempt_id,
                idempotency_key,
            } => {
                let result: ToolResult = event_record(state, &empty, result_ref)?;
                if result.status != crate::ToolResultStatus::Unknown || result.effect != crate::ToolEffect::Unknown
                    || !run.snapshot.tool_ledger.iter().any(|entry| entry.call.call_id == result.call_id)
                    || !run.snapshot.reservations.iter().any(|reservation| &reservation.attempt_id == attempt_id
                        && matches!(&reservation.kind, crate::ReservationKind::Tool { call_id } if call_id == &result.call_id))
                {
                    return Err(invalid("checkpoint.tool_unresolved"));
                }
                let entry = run
                    .snapshot
                    .tool_ledger
                    .iter()
                    .find(|entry| entry.call.call_id == result.call_id)
                    .expect("call membership checked above");
                let current_key = match &entry.state {
                    ToolCallState::Dispatching {
                        idempotency_key, ..
                    }
                    | ToolCallState::ApprovalPending {
                        idempotency_key, ..
                    }
                    | ToolCallState::Unknown {
                        idempotency_key, ..
                    } => Some(idempotency_key),
                    _ => None,
                };
                if current_key.is_some_and(|key| key != idempotency_key)
                    || unresolved_keys
                        .insert(result.call_id.clone(), idempotency_key)
                        .is_some_and(|key| key != idempotency_key)
                {
                    return Err(invalid("checkpoint.tool_unresolved_key"));
                }
                validate_tool_pair(state, &run.snapshot, &[], &result)?;
                for reference in tool_result_refs(&result) {
                    record_value(state, &empty, reference)?;
                }
            }
            RunEventPayload::VerificationCompleted { verification_ref } => {
                let verification: VerificationSummary =
                    event_record(state, &empty, verification_ref)?;
                for reference in &verification.evidence {
                    record_value(state, &empty, reference)?;
                }
            }
            RunEventPayload::RunWaiting { wait_ref } => {
                let wait: WaitState = event_record(state, &empty, wait_ref)?;
                if let WaitTarget::Approval {
                    target: ApprovalTarget::Candidate { candidate_ref, .. },
                } = &wait.target
                {
                    record_value(state, &empty, candidate_ref)?;
                }
            }
            RunEventPayload::RunResumed { command_ref } => {
                let command: ResumeCommand = event_record(state, &empty, command_ref)?;
                if command.run_id != run.snapshot.run_id
                    || command.expected_revision >= run.snapshot.revision
                {
                    return Err(invalid("checkpoint.run_resumed"));
                }
                let reference = match &command.action {
                    ResumeAction::External { receipt_ref, .. } => Some(receipt_ref),
                    ResumeAction::Recover { recovery_ref } => Some(recovery_ref),
                    ResumeAction::Approve {
                        target: ApprovalTarget::Candidate { candidate_ref, .. },
                        ..
                    }
                    | ResumeAction::Deny {
                        target: ApprovalTarget::Candidate { candidate_ref, .. },
                        ..
                    } => Some(candidate_ref),
                    _ => None,
                };
                if let Some(reference) = reference {
                    record_value(state, &empty, reference)?;
                }
            }
            RunEventPayload::ModelRouteSelected {
                invocation_ref,
                route_digest,
            } => {
                let old: ModelInvocationRecord = event_record(state, &empty, invocation_ref)?;
                let current = run
                    .snapshot
                    .model_ledger
                    .iter()
                    .find(|current| current.attempt_id == old.attempt_id)
                    .ok_or_else(|| invalid("checkpoint.model_route"))?;
                if old.run_id != current.run_id
                    || old.model_step_id != current.model_step_id
                    || old.purpose != current.purpose
                    || old.route != current.route
                    || old.selection_reason != current.selection_reason
                    || old.request_digest != current.request_digest
                    || old.inspection_ref != current.inspection_ref
                    || old.route.digest() != *route_digest
                    || (matches!(
                        old.state,
                        ModelAttemptState::Completed {} | ModelAttemptState::Failed { .. }
                    ) && &old != current)
                    || (matches!(old.state, ModelAttemptState::Unknown {})
                        && matches!(current.state, ModelAttemptState::Reserved {}))
                {
                    return Err(invalid("checkpoint.model_route"));
                }
                if let Some(reference) = &old.response_ref {
                    validate_model_response(state, &empty, &old, reference)?;
                }
            }
        }
    }
    if started != 1
        || sequence != run.snapshot.last_event_seq
        || finished != usize::from(run.snapshot.status.is_terminal())
    {
        return Err(invalid("checkpoint.events"));
    }
    Ok(())
}

fn invalid(path: &str) -> ContractError {
    error(ErrorCode::InvalidSnapshot, path)
}
```

## `crates/wickle/src/tool_execution.rs`

```rust
use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, fmt, sync::Arc};
use tokio_util::sync::CancellationToken;

mod round;

/// Identity and controls for one physical tool call. Credentials and unrelated
/// system inputs remain in the executor's Host-owned binding.
#[derive(Debug, Clone)]
pub struct ToolExecutionContext {
    /// Logical call whose plan and bound input were already saved.
    pub call_id: Id,
    /// Charged physical attempt, already recorded before execution.
    pub attempt_id: Id,
    /// Stable external deduplication identity across recovery of this call.
    pub idempotency_key: Id,
    /// Exact authorized namespace.
    pub scope: Scope,
    /// Current authenticated actor.
    pub principal_ref: Id,
    /// Current Host authorization grant.
    pub capability_grant_ref: Id,
    /// Cancelled when the attempt stops, including timeout or caller cancellation.
    pub cancellation: CancellationToken,
    /// Finite execution deadline.
    pub deadline: tokio::time::Instant,
}

/// Effect information attested by the trusted executor, independent of output validation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolEffect {
    /// The executor confirms no external business write occurred.
    NotApplied,
    /// An external business write is confirmed; its receipt must be retained.
    Applied,
    /// Whether an external business write occurred could not be established.
    #[default]
    Unknown,
}

/// A handler's safe result; it cannot replace core call identities or ledger state.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolExecutionOutcome {
    /// Returned value to check against the pinned output schema.
    Succeeded {
        /// Raw returned JSON; only a validated, bounded value becomes model content.
        value: Value,
    },
    /// Classified handler failure, independent of whether a write happened.
    Failed {
        /// Safe registered failure code, without SDK error messages or payloads.
        code: Id,
    },
}

/// Explicit completion and effect receipt. Serialize only for protected storage.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolExecutionResult {
    /// Returned value or safe failure classification.
    pub outcome: ToolExecutionOutcome,
    /// Observed external effect status.
    pub effect: ToolEffect,
    /// Original effect receipt, required for a confirmed Applied result.
    pub receipt: Option<Value>,
}
impl fmt::Debug for ToolExecutionOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Succeeded { .. } => "ToolExecutionOutcome::Succeeded(<protected>)",
            Self::Failed { .. } => "ToolExecutionOutcome::Failed(<classified>)",
        })
    }
}
impl fmt::Debug for ToolExecutionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolExecutionResult")
            .field("effect", &self.effect)
            .field("has_receipt", &self.receipt.is_some())
            .finish_non_exhaustive()
    }
}

/// Exactly one physical execution. Implementations must not hide retry loops or
/// spawn untracked operations; effect uncertainty must be reported honestly.
pub trait ToolExecutor: Send + Sync {
    /// Execute only the final policy-approved arguments, not the original model
    /// map, full system-input snapshot, or caller-supplied tool identities.
    fn execute<'a>(
        &'a self,
        execution_args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult>;
}

/// A trusted Host associates one compiled contract with an existing executor.
/// Factory-level code/manifest attestation is separate from this registration.
#[derive(Clone)]
pub struct ToolRegistration {
    /// Exact descriptor and model-input projection.
    pub compiled: CompiledTool,
    /// Existing scoped executor; construction and credentials remain in Host code.
    pub executor: Arc<dyn ToolExecutor>,
}
impl fmt::Debug for ToolRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolRegistration")
            .field("compiled", &self.compiled)
            .finish_non_exhaustive()
    }
}

/// Scope-bound, immutable mapping of exact tool contracts to existing executors.
#[derive(Debug)]
pub struct ToolRegistry {
    scope: Scope,
    entries: BTreeMap<Id, ToolRegistration>,
}
impl ToolRegistry {
    /// Register without invoking handlers; duplicate names and exact tool identities fail.
    pub fn new(scope: Scope, entries: Vec<ToolRegistration>) -> Result<Self, ContractError> {
        let mut registered = BTreeMap::new();
        for entry in entries {
            if registered.values().any(|prior: &ToolRegistration| {
                prior.compiled.descriptor().tool == entry.compiled.descriptor().tool
            }) || registered
                .insert(entry.compiled.descriptor().name.clone(), entry)
                .is_some()
            {
                return Err(error(
                    ErrorCode::InvalidToolInputContract,
                    "tools.duplicate",
                ));
            }
        }
        Ok(Self {
            scope,
            entries: registered,
        })
    }
    /// Exact namespace under which handlers were registered.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Inspect an exact portable name without executing it or resolving an alias.
    pub fn get(&self, name: &Id) -> Option<&ToolRegistration> {
        self.entries.get(name)
    }
    /// Return only profile-selected contracts in profile order. Adapter exports
    /// require their separate runtime factory and are not implicitly opened here.
    pub fn prompt_bindings(
        &self,
        profile: &AgentProfile,
    ) -> Result<Vec<PromptToolBinding>, ContractError> {
        profile
            .tools
            .iter()
            .map(|selection| {
                let ToolBindingRef::Catalog(reference) = selection else {
                    return Err(error(ErrorCode::CapabilityUnsupported, "tools.export"));
                };
                let entry = self
                    .entries
                    .values()
                    .find(|entry| {
                        entry.compiled.descriptor().tool.id == reference.tool_id
                            && entry.compiled.descriptor().tool.version == reference.version
                    })
                    .ok_or_else(|| error(ErrorCode::ComponentUnavailable, "tools.selection"))?;
                Ok(PromptToolBinding {
                    selection: selection.clone(),
                    compiled: entry.compiled.clone(),
                })
            })
            .collect()
    }
}

/// Per-attempt bounds. The Run still owns total attempts, recovery and elapsed time.
#[derive(Debug, Clone, Copy)]
pub struct ToolExecutionLimits {
    /// Maximum elapsed time for one executor callback.
    pub timeout_ms: u64,
    /// Maximum raw effect-receipt size accepted from a handler.
    pub max_receipt_bytes: usize,
}
impl Default for ToolExecutionLimits {
    fn default() -> Self {
        Self {
            timeout_ms: 30_000,
            max_receipt_bytes: 65_536,
        }
    }
}

/// Whether the complete saved round is safe to follow with another model step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolRoundOutcome {
    /// Every planned call has a settled result and no unknown external effect remains.
    Completed,
    /// A fixed bound candidate requires the separate approval runtime.
    ApprovalRequired {
        /// Call whose immutable candidate was saved.
        call_id: Id,
        /// Current safe policy reason.
        reason: Id,
        /// Exact saved candidate; approval cannot rebind its system inputs.
        bound_input_ref: RecordRef,
        /// Identity of the final model-and-system argument binding.
        binding_digest: JsonDigest,
    },
    /// A prior or current attempt requires explicit effect reconciliation.
    Unresolved {
        /// Call that prevents further tool and model dispatch.
        call_id: Id,
        /// Protected uncertainty observation committed with the matching event.
        result_ref: RecordRef,
    },
}

/// Serial execution of a previously committed model tool round.
pub struct SerialToolRound {
    registry: Arc<ToolRegistry>,
    binder: Arc<InputBinder>,
    policy: Arc<PolicyGate>,
    ids: Arc<dyn IdSource>,
    limits: ToolExecutionLimits,
}
impl SerialToolRound {
    /// Inject existing bindings; no tool is run or looked up externally here.
    pub fn new(
        registry: Arc<ToolRegistry>,
        binder: Arc<InputBinder>,
        policy: Arc<PolicyGate>,
        ids: Arc<dyn IdSource>,
    ) -> Self {
        Self {
            registry,
            binder,
            policy,
            ids,
            limits: ToolExecutionLimits::default(),
        }
    }
    /// Require finite nonzero timeout and receipt limits.
    pub fn with_limits(mut self, limits: ToolExecutionLimits) -> Result<Self, ContractError> {
        if limits.timeout_ms == 0 || limits.timeout_ms > 86_400_000 || limits.max_receipt_bytes == 0
        {
            return Err(error(ErrorCode::InvalidConfiguration, "tools.limits"));
        }
        self.limits = limits;
        Ok(self)
    }
}
fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
```

## `crates/wickle/src/tool_execution/round.rs`

```rust
use super::*;
use futures_util::FutureExt;
use std::{panic::AssertUnwindSafe, time::Duration};

impl SerialToolRound {
    /// Execute only calls from one committed physical model response, in saved order.
    /// Existing settled results are reused, and uncertain attempts are never retried.
    pub async fn execute(
        &self,
        model_request_id: &Id,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<ToolRoundOutcome, ContractError> {
        self.scope(context, budget)?;
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        if let Some(entry) = saved.snapshot.tool_ledger.iter().find(|entry| {
            matches!(entry.state, ToolCallState::Unknown { .. } | ToolCallState::Dispatching { .. })
                || matches!(&entry.state, ToolCallState::Settled { result } if result.status == ToolResultStatus::Unknown || result.effect == ToolEffect::Unknown)
        }) {
            return self.existing_uncertainty(entry, budget).await;
        }
        let call_ids: Vec<_> = saved
            .snapshot
            .tool_ledger
            .iter()
            .filter(|entry| &entry.call.model_request_id == model_request_id)
            .map(|entry| entry.call.call_id.clone())
            .collect();
        for call_id in call_ids {
            self.boundary(context, budget).await?;
            let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
            let entry = saved
                .snapshot
                .tool_ledger
                .iter()
                .find(|entry| entry.call.call_id == call_id)
                .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
            match &entry.state {
                ToolCallState::Settled { result }
                    if result.status != ToolResultStatus::Unknown
                        && result.effect != ToolEffect::Unknown =>
                {
                    continue;
                }
                ToolCallState::Planned {} | ToolCallState::ApprovalPending { .. } => {}
                _ => return self.existing_uncertainty(entry, budget).await,
            }
            let call = entry.call.clone();
            let pending_key = if let ToolCallState::ApprovalPending {
                idempotency_key, ..
            } = &entry.state
            {
                Some(idempotency_key.clone())
            } else {
                None
            };
            let call_message_id = call_message(&saved, &call)?;
            let registered = self.registry.get(&call.tool_name);
            let Some(registered) = registered.filter(|entry| {
                call.descriptor_digest.as_ref() == Some(entry.compiled.descriptor_digest())
            }) else {
                self.reject(
                    &call,
                    call_message_id,
                    ToolResultStatus::Failed,
                    "unknown_tool",
                    budget,
                )
                .await?;
                continue;
            };
            if registered
                .compiled
                .validate_model_inputs(&call.model_inputs)
                .is_err()
            {
                self.reject(
                    &call,
                    call_message_id,
                    ToolResultStatus::Failed,
                    "invalid_arguments",
                    budget,
                )
                .await?;
                continue;
            }
            let bound = match self
                .binder
                .bind(&registered.compiled, &call_id, context, budget)
                .await
            {
                Ok(bound) => bound,
                Err(error) if control_or_storage(error.code) => return Err(error),
                Err(error) => {
                    let status = if error.code == ErrorCode::AccessDenied {
                        ToolResultStatus::Denied
                    } else {
                        ToolResultStatus::Failed
                    };
                    self.reject(
                        &call,
                        call_message_id,
                        status,
                        &code_name(error.code),
                        budget,
                    )
                    .await?;
                    continue;
                }
            };
            if let PolicyDecision::RequireApproval { reason } = bound.decision {
                return Ok(ToolRoundOutcome::ApprovalRequired {
                    call_id,
                    reason,
                    bound_input_ref: bound.reference,
                    binding_digest: bound.input.binding_digest().clone(),
                });
            }
            match self.authorize(&bound.input, context, budget).await? {
                PolicyDecision::RequireApproval { reason } => {
                    return Ok(ToolRoundOutcome::ApprovalRequired {
                        call_id,
                        reason,
                        bound_input_ref: bound.reference,
                        binding_digest: bound.input.binding_digest().clone(),
                    });
                }
                PolicyDecision::Deny { .. } => {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Denied,
                        "access_denied",
                        budget,
                    )
                    .await?;
                    continue;
                }
                PolicyDecision::Allow {} => {}
            }
            let reservation = budget
                .reserve(ReservationKind::Tool {
                    call_id: call_id.clone(),
                })
                .await?;
            let key = if let Some(key) = pending_key {
                key
            } else {
                Id::new(format!(
                    "tool-effect-{}",
                    canonical_digest(&serde_json::json!([
                        budget.scope(),
                        budget.run_id(),
                        call_id,
                        bound.input.binding_digest()
                    ]))
                ))?
            };
            self.dispatching(&call_id, &reservation.attempt_id, &key, budget)
                .await?;
            let gate = async {
                self.boundary(context, budget).await?;
                let decision = self.authorize(&bound.input, context, budget).await?;
                self.boundary(context, budget).await?;
                Ok::<_, ContractError>(decision)
            }
            .await;
            match gate {
                Ok(PolicyDecision::Allow {}) => {}
                Ok(PolicyDecision::RequireApproval { reason }) => {
                    self.approval_pending(&call_id, &reservation.attempt_id, &key, budget)
                        .await?;
                    return Ok(ToolRoundOutcome::ApprovalRequired {
                        call_id,
                        reason,
                        bound_input_ref: bound.reference,
                        binding_digest: bound.input.binding_digest().clone(),
                    });
                }
                Ok(PolicyDecision::Deny { .. }) => {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Denied,
                        "access_denied",
                        budget,
                    )
                    .await?;
                    continue;
                }
                Err(error)
                    if matches!(
                        error.code,
                        ErrorCode::Cancelled | ErrorCode::DeadlineExceeded
                    ) =>
                {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Cancelled,
                        &code_name(error.code),
                        budget,
                    )
                    .await?;
                    return Err(error);
                }
                Err(error) if control_or_storage(error.code) => return Err(error),
                Err(error) => {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Denied,
                        &code_name(error.code),
                        budget,
                    )
                    .await?;
                    continue;
                }
            }
            let cancellation = budget.cancellation().child_token();
            let _cancel = cancellation.clone().drop_guard();
            let run_deadline = match budget.call_deadline() {
                Ok(deadline) => deadline,
                Err(error) if error.code == ErrorCode::DeadlineExceeded => {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Cancelled,
                        "deadline_exceeded",
                        budget,
                    )
                    .await?;
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
            let deadline = tokio::time::Instant::now()
                .checked_add(Duration::from_millis(self.limits.timeout_ms))
                .ok_or_else(|| error(ErrorCode::InvalidConfiguration, "tool.timeout"))?
                .min(run_deadline);
            let execution = ToolExecutionContext {
                call_id: call_id.clone(),
                attempt_id: reservation.attempt_id.clone(),
                idempotency_key: key.clone(),
                scope: budget.scope().clone(),
                principal_ref: context.data.principal_ref.clone(),
                capability_grant_ref: context.data.capability_grant_ref.clone(),
                cancellation,
                deadline,
            };
            let mut entered = false;
            let result = {
                let operation = AssertUnwindSafe(async {
                    entered = true;
                    registered
                        .executor
                        .execute(bound.input.execution_args(), &execution)
                        .await
                })
                .catch_unwind();
                tokio::select! { biased;
                    _ = context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "tool.execution")),
                    _ = tokio::time::sleep_until(deadline) => Err(error(ErrorCode::DeadlineExceeded, "tool.execution")),
                    stopped = budget.wait_for_cancellation_or_deadline() => match stopped { Err(error) => Err(error), Ok(()) => Err(error(ErrorCode::DeadlineExceeded, "tool.execution")) },
                    result = operation => result.unwrap_or_else(|_| Err(error(ErrorCode::InvalidContract, "tool.executor"))),
                }
            };
            execution.cancellation.cancel();
            let completion = match result {
                Ok(result) => result,
                Err(error) => ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Failed {
                        code: Id::new(code_name(error.code))?,
                    },
                    effect: if entered
                        && registered.compiled.descriptor().side_effect != ToolSideEffect::ReadOnly
                    {
                        ToolEffect::Unknown
                    } else {
                        ToolEffect::NotApplied
                    },
                    receipt: None,
                },
            };
            let (result, records) = self.validate_output(
                &call,
                call_message_id,
                &execution,
                &registered.compiled,
                completion,
            )?;
            let unresolved = result.effect == ToolEffect::Unknown;
            let state = if unresolved {
                ToolCallState::Unknown {
                    attempt_id: execution.attempt_id.clone(),
                    idempotency_key: key.clone(),
                }
            } else {
                ToolCallState::Settled {
                    result: result.clone(),
                }
            };
            let result_ref = self
                .settle(&call_id, state, result, records, budget)
                .await?;
            if unresolved {
                return Ok(ToolRoundOutcome::Unresolved {
                    call_id,
                    result_ref,
                });
            }
        }
        Ok(ToolRoundOutcome::Completed)
    }

    /// Close only never-dispatched plans when a driver ends a segment because of
    /// cancellation or exhaustion. Unknown and already-dispatched calls are untouched.
    pub async fn settle_unstarted(
        &self,
        model_request_id: &Id,
        status: ToolResultStatus,
        code: Id,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        self.scope(context, budget)?;
        if !matches!(
            status,
            ToolResultStatus::Failed | ToolResultStatus::Denied | ToolResultStatus::Cancelled
        ) {
            return Err(error(ErrorCode::InvalidContract, "tool.settlement"));
        }
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        for entry in &saved.snapshot.tool_ledger {
            if &entry.call.model_request_id == model_request_id
                && matches!(
                    entry.state,
                    ToolCallState::Planned {} | ToolCallState::ApprovalPending { .. }
                )
            {
                self.reject(
                    &entry.call,
                    call_message(&saved, &entry.call)?,
                    status,
                    code.as_str(),
                    budget,
                )
                .await?;
            }
        }
        Ok(())
    }

    fn scope(&self, context: &ExecutionContext, budget: &RunBudget) -> Result<(), ContractError> {
        if &context.data.scope != budget.scope() || self.registry.scope() != budget.scope() {
            return Err(error(ErrorCode::AccessDenied, "scope"));
        }
        Ok(())
    }
    async fn boundary(
        &self,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        self.scope(context, budget)?;
        if context.cancellation.is_cancelled() {
            return Err(error(ErrorCode::Cancelled, "tool"));
        }
        budget.check_boundary().await
    }
    async fn authorize(
        &self,
        input: &BoundToolInput,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<PolicyDecision, ContractError> {
        let request = input.policy_request();
        tokio::select! { biased;
            _ = context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "tool.policy")),
            stopped = budget.wait_for_cancellation_or_deadline() => match stopped { Err(error) => Err(error), Ok(()) => Err(error(ErrorCode::DeadlineExceeded, "tool.policy")) },
            result = self.policy.check(&request, context, Some(budget.call_deadline()?), None) => result,
        }
    }
    async fn dispatching(
        &self,
        call_id: &Id,
        attempt_id: &Id,
        key: &Id,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let entry = snapshot
            .tool_ledger
            .iter_mut()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
        if !matches!(
            entry.state,
            ToolCallState::Planned {} | ToolCallState::ApprovalPending { .. }
        ) || entry.call.bound_input_ref.is_none()
        {
            return Err(error(ErrorCode::InvalidTransition, "tool.dispatch"));
        }
        if let ToolCallState::ApprovalPending {
            idempotency_key, ..
        } = &entry.state
        {
            if idempotency_key != key {
                return Err(error(ErrorCode::InvalidTransition, "tool.idempotency_key"));
            }
        }
        entry.state = ToolCallState::Dispatching {
            attempt_id: attempt_id.clone(),
            idempotency_key: key.clone(),
        };
        self.commit(snapshot, vec![], vec![], vec![], budget).await
    }
    async fn approval_pending(
        &self,
        call_id: &Id,
        attempt_id: &Id,
        key: &Id,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let entry = snapshot
            .tool_ledger
            .iter_mut()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
        if !matches!(&entry.state, ToolCallState::Dispatching { attempt_id: current, idempotency_key } if current == attempt_id && idempotency_key == key)
        {
            return Err(error(ErrorCode::InvalidTransition, "tool.approval"));
        }
        entry.state = ToolCallState::ApprovalPending {
            attempt_id: attempt_id.clone(),
            idempotency_key: key.clone(),
        };
        self.commit(snapshot, vec![], vec![], vec![], budget).await
    }
    async fn reject(
        &self,
        call: &ToolCall,
        call_message_id: Id,
        status: ToolResultStatus,
        code: &str,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let result = ToolResult {
            call_id: call.call_id.clone(),
            call_message_id,
            status,
            effect: ToolEffect::NotApplied,
            content: vec![],
            effect_receipt_ref: None,
            error: Some(Failure {
                code: Id::new(code)?,
                diagnostic_ref: None,
            }),
        };
        self.settle(
            &call.call_id,
            ToolCallState::Settled {
                result: result.clone(),
            },
            result,
            vec![],
            budget,
        )
        .await?;
        Ok(())
    }

    fn validate_output(
        &self,
        call: &ToolCall,
        call_message_id: Id,
        execution: &ToolExecutionContext,
        compiled: &CompiledTool,
        completion: ToolExecutionResult,
    ) -> Result<(ToolResult, Vec<ProtectedRecord>), ContractError> {
        let effect = completion.effect;
        let receipt_bytes = completion
            .receipt
            .as_ref()
            .map(|receipt| serde_json::to_vec(receipt).map(|bytes| bytes.len()))
            .transpose()
            .map_err(|_| error(ErrorCode::InvalidJson, "tool.receipt"))?;
        let receipt_oversized =
            receipt_bytes.is_some_and(|size| size > self.limits.max_receipt_bytes);
        let raw_receipt = if receipt_oversized {
            serde_json::json!({"omitted":true,"bytes":receipt_bytes,"digest":canonical_digest(completion.receipt.as_ref().expect("oversized receipt"))})
        } else {
            completion.receipt.clone().unwrap_or(Value::Null)
        };
        let mut status = ToolResultStatus::Succeeded;
        let mut code = None;
        let mut content = vec![];
        let raw_output = match &completion.outcome {
            ToolExecutionOutcome::Succeeded { value } => {
                let bytes = serde_json::to_vec(value)
                    .map_err(|_| error(ErrorCode::InvalidJson, "tool.output"))?
                    .len();
                if bytes as u64 > compiled.descriptor().max_output_bytes.get() {
                    status = ToolResultStatus::Failed;
                    code = Some(Id::new("tool_output_too_large")?);
                    serde_json::json!({"omitted":true,"bytes":bytes,"digest":canonical_digest(value)})
                } else {
                    if !crate::tool_schema::compile_validator(&compiled.descriptor().output_schema)?
                        .is_valid(value)
                    {
                        status = ToolResultStatus::Failed;
                        code = Some(Id::new("invalid_tool_output")?);
                    } else {
                        content.push(InputContent::Json {
                            value: value.clone(),
                        });
                    }
                    value.clone()
                }
            }
            ToolExecutionOutcome::Failed { code: failure } => {
                status = if failure.as_str() == "cancelled" {
                    ToolResultStatus::Cancelled
                } else {
                    ToolResultStatus::Failed
                };
                code = Some(failure.clone());
                Value::Null
            }
        };
        if effect == ToolEffect::Unknown {
            status = ToolResultStatus::Unknown;
            code = Some(Id::new("tool_effect_unknown")?);
            content.clear();
        } else if receipt_oversized {
            status = ToolResultStatus::Failed;
            code = Some(Id::new("effect_receipt_too_large")?);
            content.clear();
        } else if effect == ToolEffect::Applied && completion.receipt.is_none() {
            status = ToolResultStatus::Failed;
            code = Some(Id::new("effect_receipt_missing")?);
            content.clear();
        } else if effect == ToolEffect::Applied
            && compiled.descriptor().side_effect == ToolSideEffect::ReadOnly
        {
            status = ToolResultStatus::Failed;
            code = Some(Id::new("tool_effect_contract")?);
            content.clear();
        }
        let record = ProtectedRecord::new(
            self.ids.next_id()?,
            1,
            serde_json::json!({
                "scope":execution.scope,"call_id":execution.call_id,"attempt_id":execution.attempt_id,"idempotency_key":execution.idempotency_key,
                "effect":effect,"receipt":raw_receipt,"receipt_omitted":receipt_oversized,"output":raw_output,"error_code":code,
            }),
        );
        let reference = record.reference().clone();
        Ok((
            ToolResult {
                call_id: call.call_id.clone(),
                call_message_id,
                status,
                effect,
                content,
                effect_receipt_ref: (effect != ToolEffect::NotApplied
                    || completion.receipt.is_some())
                .then(|| reference.clone()),
                error: code.map(|code| Failure {
                    code,
                    diagnostic_ref: Some(reference),
                }),
            },
            vec![record],
        ))
    }

    async fn settle(
        &self,
        call_id: &Id,
        state: ToolCallState,
        result: ToolResult,
        mut records: Vec<ProtectedRecord>,
        budget: &RunBudget,
    ) -> Result<RecordRef, ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let entry = snapshot
            .tool_ledger
            .iter_mut()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
        if matches!(
            entry.state,
            ToolCallState::Settled { .. } | ToolCallState::Unknown { .. }
        ) {
            return Err(error(ErrorCode::InvalidTransition, "tool.settlement"));
        }
        entry.state = state.clone();
        let record = ProtectedRecord::new(
            self.ids.next_id()?,
            1,
            serde_json::to_value(&result)
                .map_err(|_| error(ErrorCode::InvalidJson, "tool.result"))?,
        );
        let reference = record.reference().clone();
        records.push(record);
        snapshot.last_event_seq = snapshot
            .last_event_seq
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::InvalidEvent, "tool.event"))?;
        let (_, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: self.ids.next_id()?,
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            session_id: snapshot.request.session_id.clone(),
            seq: snapshot
                .last_event_seq
                .try_into()
                .map_err(|_| error(ErrorCode::InvalidEvent, "tool.event"))?,
            timestamp_ms: now,
            payload: match state {
                ToolCallState::Unknown {
                    attempt_id,
                    idempotency_key,
                } => RunEventPayload::ToolUnresolved {
                    result_ref: reference.clone(),
                    attempt_id,
                    idempotency_key,
                },
                _ => RunEventPayload::ToolSettled {
                    result_ref: reference.clone(),
                },
            },
        };
        let message = Message {
            message_id: self.ids.next_id()?,
            run_id: budget.run_id().clone(),
            sequence: saved
                .session
                .transcript_revision
                .checked_add(1)
                .and_then(std::num::NonZeroU64::new)
                .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.message"))?,
            role: MessageRole::Tool,
            content: vec![ContentBlock::ToolResult { result }],
            origin: MessageOrigin::Tool,
            visibility: Visibility::UserAndModel,
        };
        self.commit(snapshot, vec![message], vec![event], records, budget)
            .await?;
        Ok(reference)
    }
    async fn commit(
        &self,
        mut snapshot: RunSnapshot,
        messages: Vec<Message>,
        events: Vec<RunEvent>,
        records: Vec<ProtectedRecord>,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let expected_revision = snapshot.revision;
        let (_, check_at) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let lease = budget
            .store()
            .check_lease(budget.scope(), budget.run_id(), budget.lease(), check_at)
            .await?;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        if now >= lease.expires_at_ms {
            return Err(error(ErrorCode::LeaseLost, "tool.lease"));
        }
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::RevisionConflict, "tool.revision"))?;
        snapshot.phase = RunPhase::Tool;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        budget
            .store()
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages,
                    events,
                    records,
                },
            )
            .await?;
        Ok(())
    }
    async fn existing_uncertainty(
        &self,
        entry: &ToolLedgerEntry,
        budget: &RunBudget,
    ) -> Result<ToolRoundOutcome, ContractError> {
        let ToolCallState::Unknown {
            attempt_id,
            idempotency_key,
        } = &entry.state
        else {
            return Err(error(
                ErrorCode::InvalidTransition,
                "tool.unresolved_dispatch",
            ));
        };
        let mut after = 0;
        loop {
            let page = budget
                .store()
                .read_events(budget.scope(), budget.run_id(), after, MAX_EVENT_PAGE_SIZE)
                .await?;
            for event in &page.events {
                if let RunEventPayload::ToolUnresolved {
                    result_ref,
                    attempt_id: saved_attempt,
                    idempotency_key: saved_key,
                } = &event.payload
                {
                    if saved_attempt == attempt_id && saved_key == idempotency_key {
                        return Ok(ToolRoundOutcome::Unresolved {
                            call_id: entry.call.call_id.clone(),
                            result_ref: result_ref.clone(),
                        });
                    }
                }
            }
            if !page.has_more {
                return Err(error(ErrorCode::InvalidSnapshot, "tool.unresolved_result"));
            }
            after = page.next_after_seq;
        }
    }
}

fn call_message(saved: &StoredRun, call: &ToolCall) -> Result<Id, ContractError> {
    let messages: Vec<_> = saved.messages.iter().filter(|message| message.run_id == saved.snapshot.run_id && message.role == MessageRole::Assistant && message.content.iter().any(|content| matches!(content, ContentBlock::ToolCall { call: candidate } if candidate.call_id == call.call_id && candidate.model_request_id == call.model_request_id && candidate.provider_call_id == call.provider_call_id && candidate.tool_name == call.tool_name && candidate.model_inputs == call.model_inputs && candidate.descriptor_digest == call.descriptor_digest))).collect();
    if messages.len() != 1 {
        return Err(error(ErrorCode::InvalidSnapshot, "tool.call_message"));
    }
    Ok(messages[0].message_id.clone())
}
fn code_name(code: ErrorCode) -> String {
    serde_json::to_value(code)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "invalid_contract".into())
}
fn control_or_storage(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::Cancelled
            | ErrorCode::DeadlineExceeded
            | ErrorCode::BudgetExceeded
            | ErrorCode::LeaseLost
            | ErrorCode::RevisionConflict
            | ErrorCode::PersistenceUnavailable
            | ErrorCode::StateNotFound
            | ErrorCode::ClockUnavailable
            | ErrorCode::ClockRegression
    )
}
```

## `crates/wickle/src/views.rs`

```rust
use crate::{
    ArtifactRef, BudgetUsage, ContractError, ExecutionContext, Guarded, Id, PolicyAction,
    PolicyGate, PolicyRequest, RunEvent, RunEventPayload, RunPhase, RunSnapshot, RunStatus,
};
use serde::Serialize;
use std::num::NonZeroU64;
use tokio::time::Instant;

/// Minimal run metadata. Protected records and outcome internals are not included.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunView {
    /// Run identity.
    pub run_id: Id,
    /// Session identity.
    pub session_id: Id,
    /// Current status.
    pub status: RunStatus,
    /// Current phase.
    pub phase: RunPhase,
    /// Snapshot revision.
    pub revision: u64,
    /// Saved usage, not inferred provider consumption.
    pub usage: BudgetUsage,
}

/// Authorized artifact metadata without an embedded storage location or payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactView {
    /// Artifact identity.
    pub artifact_id: Id,
    /// Stored media type.
    pub media_type: Id,
    /// Original byte size.
    pub size_bytes: u64,
    /// Store-defined content hash.
    pub content_hash: Id,
}

/// Event metadata without protected record references from its payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EventView {
    /// Event identity.
    pub event_id: Id,
    /// Owning run.
    pub run_id: Id,
    /// Owning session.
    pub session_id: Id,
    /// Durable sequence.
    pub seq: NonZeroU64,
    /// UTC milliseconds since Unix epoch.
    pub timestamp_ms: i64,
    /// Durable event kind.
    pub event_type: &'static str,
}

impl PolicyGate {
    /// Authorize using the stored run scope and select public fields explicitly.
    pub async fn run_view(
        &self,
        snapshot: &RunSnapshot,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<Guarded<RunView>, ContractError> {
        let request = PolicyRequest {
            owner_scope: snapshot.scope.clone(),
            resource_id: snapshot.run_id.clone(),
            action: PolicyAction::ReadRun {},
        };
        self.guard(&request, context, deadline, None, || async {
            Ok(RunView {
                run_id: snapshot.run_id.clone(),
                session_id: snapshot.request.session_id.clone(),
                status: snapshot.status,
                phase: snapshot.phase,
                revision: snapshot.revision,
                usage: snapshot.usage.clone(),
            })
        })
        .await
    }

    /// Read a full protected checkpoint only through the distinct details action.
    pub async fn run_details(
        &self,
        snapshot: &RunSnapshot,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<Guarded<RunSnapshot>, ContractError> {
        let request = PolicyRequest {
            owner_scope: snapshot.scope.clone(),
            resource_id: snapshot.run_id.clone(),
            action: PolicyAction::ReadRunDetails {},
        };
        self.guard(&request, context, deadline, None, || async {
            Ok(snapshot.clone())
        })
        .await
    }

    /// Authorize against authoritative artifact metadata before selecting its view.
    pub async fn artifact_view(
        &self,
        artifact: &ArtifactRef,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<Guarded<ArtifactView>, ContractError> {
        let request = PolicyRequest {
            owner_scope: artifact.scope.clone(),
            resource_id: artifact.artifact_id.clone(),
            action: PolicyAction::ReadArtifact {},
        };
        self.guard(&request, context, deadline, None, || async {
            Ok(ArtifactView {
                artifact_id: artifact.artifact_id.clone(),
                media_type: artifact.media_type.clone(),
                size_bytes: artifact.size_bytes,
                content_hash: artifact.content_hash.clone(),
            })
        })
        .await
    }

    /// Authorize a stored event without publishing its protected payload references.
    pub async fn event_view(
        &self,
        event: &RunEvent,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<Guarded<EventView>, ContractError> {
        let request = PolicyRequest {
            owner_scope: event.scope.clone(),
            resource_id: event.run_id.clone(),
            action: PolicyAction::ReadEvents {},
        };
        self.guard(&request, context, deadline, None, || async {
            let event_type = match &event.payload {
                RunEventPayload::RunStarted { .. } => "run.started",
                RunEventPayload::ToolPlanned { .. } => "tool.planned",
                RunEventPayload::ToolSettled { .. } => "tool.settled",
                RunEventPayload::ToolUnresolved { .. } => "tool.unresolved",
                RunEventPayload::VerificationCompleted { .. } => "verification.completed",
                RunEventPayload::RunWaiting { .. } => "run.waiting",
                RunEventPayload::RunResumed { .. } => "run.resumed",
                RunEventPayload::RunFinished { .. } => "run.finished",
                RunEventPayload::ModelRouteSelected { .. } => "model.route_selected",
            };
            Ok(EventView {
                event_id: event.event_id.clone(),
                run_id: event.run_id.clone(),
                session_id: event.session_id.clone(),
                seq: event.seq,
                timestamp_ms: event.timestamp_ms,
                event_type,
            })
        })
        .await
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
async fn unsupported_profile_modes_are_rejected_before_any_callbacks_or_model_calls() {
    let fixture = Fixture::new(Response::Text, false);
    let mut verified = profile();
    verified.completion_policy = CompletionPolicy::Verified {
        verifier_ref: reference("verifier"),
    };
    assert!(create_agent(verified, fixture.bindings()).is_err());
    let mut tools = profile();
    tools.tools = vec![ToolBindingRef::Catalog(CatalogToolRef {
        tool_id: id("search"),
        version: id("1"),
        bindings: None,
        config: None,
    })];
    assert!(create_agent(tools, fixture.bindings()).is_err());
    let mut hooks = profile();
    hooks.hooks = Some(vec![HookRef::Catalog(CatalogHookRef {
        hook_id: id("hook"),
        version: id("1"),
        position: HookPosition::BeforeModel,
    })]);
    assert_eq!(
        create_agent(hooks, fixture.bindings()).unwrap_err().code,
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
        create_agent(sources, fixture.bindings()).unwrap_err().code,
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
```

## `crates/wickle/tests/agent_tool_loop.rs`

```rust
//! Agent-level model/tool loops preserve binding boundaries, waits, and effect outcomes.

#[path = "support/agent.rs"]
#[allow(dead_code)]
mod agent_support;
use agent_support::{completed, context, id, profile, reference, request, scope};
use futures_util::stream;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use wickle::*;

const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";
fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id("1")),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("tool-loop-catalog")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| reference.id.clone()),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}
#[derive(Default)]
struct Policy {
    mode: AtomicUsize,
    tool_checks: AtomicUsize,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::ExecuteTool { .. } = &request.action {
                let check = self.tool_checks.fetch_add(1, Ordering::SeqCst) + 1;
                match self.mode.load(Ordering::SeqCst) {
                    1 => {
                        return Ok(PolicyDecision::Deny {
                            reason: id("denied"),
                        });
                    }
                    2 => {
                        return Ok(PolicyDecision::RequireApproval {
                            reason: id("review"),
                        });
                    }
                    3 if check >= 3 => {
                        return Ok(PolicyDecision::RequireApproval {
                            reason: id("late_review"),
                        });
                    }
                    _ => {}
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}

struct Model {
    plans: Vec<(&'static str, JsonObject)>,
    calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
}
impl ModelPort for Model {
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
        let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        let events = if attempt == 0 {
            let mut events: Vec<_> = self
                .plans
                .iter()
                .enumerate()
                .map(|(index, (name, arguments))| {
                    Ok(ModelEvent::ToolArgumentsDelta {
                        index: index as u32,
                        provider_call_id: Some(format!("provider-{index}")),
                        name: Some((*name).into()),
                        delta: serde_json::to_string(arguments).unwrap(),
                    })
                })
                .collect();
            events.push(Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::ToolCalls,
                metadata: ModelResponseMetadata::default(),
                continuation: vec![],
            }));
            events
        } else {
            vec![
                Ok(ModelEvent::TextDelta {
                    text: "All observations processed".into(),
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
#[derive(Clone, Copy)]
enum Behavior {
    Success,
    InvalidOutput,
    Unknown,
    Pending,
}
struct Tool {
    name: &'static str,
    behavior: Behavior,
    effect: ToolEffect,
    calls: AtomicUsize,
    applied: AtomicUsize,
    arguments: Mutex<Vec<JsonObject>>,
    order: Arc<Mutex<Vec<&'static str>>>,
    entered: Notify,
}
impl ToolExecutor for Tool {
    fn execute<'a>(
        &'a self,
        arguments: &'a JsonObject,
        _: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.arguments.lock().unwrap().push(arguments.clone());
            self.order.lock().unwrap().push(self.name);
            if self.effect == ToolEffect::Applied {
                self.applied.fetch_add(1, Ordering::SeqCst);
            }
            self.entered.notify_one();
            match self.behavior {
            Behavior::Pending=>std::future::pending().await,
            Behavior::Unknown=>Ok(ToolExecutionResult{outcome:ToolExecutionOutcome::Failed{code:id("lost_response")},effect:ToolEffect::Unknown,receipt:None}),
            Behavior::Success|Behavior::InvalidOutput=>Ok(ToolExecutionResult{
                outcome:ToolExecutionOutcome::Succeeded{value:if matches!(self.behavior,Behavior::InvalidOutput){json!(42)}else{json!(format!("{} observation",self.name))}},effect:self.effect,
                receipt:(self.effect==ToolEffect::Applied).then(||json!({"private_receipt":"only-for-storage","target":arguments["workspace_id"]})),
            }),
        }
        })
    }
}

struct Fixture {
    base: agent_support::Fixture,
    model: Arc<Model>,
    policy: Arc<Policy>,
    tools: Vec<Arc<Tool>>,
    registry: Arc<ToolRegistry>,
    inputs: SystemInputRegistry,
    profile: AgentProfile,
    order: Arc<Mutex<Vec<&'static str>>>,
}
impl Fixture {
    fn new(plans: Vec<(&'static str, JsonObject)>, write_behavior: Behavior) -> Self {
        let base = agent_support::Fixture::new(agent_support::Response::Text, false);
        let inputs = SystemInputRegistry::new(vec![SystemInputDefinition {
            key: id("workspace_id"),
            version: id("1"),
            value_schema: json!({"type":"string","format":"uuid"}),
            source: SystemInputSource::Run {},
        }])
        .unwrap();
        let order = Arc::new(Mutex::new(vec![]));
        let mut tools = vec![];
        let mut registrations = vec![];
        let mut profile = profile();
        profile.limits.max_tool_attempts = 4;
        for (name, effect, behavior) in [
            ("read", ToolSideEffect::ReadOnly, Behavior::Success),
            ("write", ToolSideEffect::Write, write_behavior),
        ] {
            let compiled=SchemaCompiler::new().compile(ToolDescriptor{tool:reference(name),name:id(name),description:format!("{name} records"),input_schema:json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"default":10},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),agent_parameters:vec!["query".into(),"limit".into()],system_bindings:None,output_schema:json!({"type":"string"}),side_effect:effect,concurrency:ToolConcurrency::Serial,retry:ToolRetryPolicy::Never,reconcile:false,max_output_bytes:4096.try_into().unwrap()},&inputs).unwrap();
            let executor = Arc::new(Tool {
                name,
                behavior,
                effect: if effect == ToolSideEffect::ReadOnly {
                    ToolEffect::NotApplied
                } else {
                    ToolEffect::Applied
                },
                calls: AtomicUsize::new(0),
                applied: AtomicUsize::new(0),
                arguments: Mutex::new(vec![]),
                order: order.clone(),
                entered: Notify::new(),
            });
            registrations.push(ToolRegistration {
                compiled,
                executor: executor.clone(),
            });
            tools.push(executor);
            profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
                tool_id: id(name),
                version: id("1"),
                bindings: None,
                config: None,
            }));
        }
        Self {
            base,
            model: Arc::new(Model {
                plans,
                calls: AtomicUsize::new(0),
                requests: Mutex::new(vec![]),
            }),
            policy: Arc::new(Policy::default()),
            tools,
            registry: Arc::new(ToolRegistry::new(scope(), registrations).unwrap()),
            inputs,
            profile,
            order,
        }
    }
    fn agent(&self) -> Agent {
        let mut bindings = self.base.bindings();
        let mut router = agent_support::Router::new();
        let mut catalog = router.snapshot.catalog().clone();
        catalog.models[0]
            .capabilities
            .features
            .insert(id("tool_calling"));
        catalog.bindings[0]
            .capabilities
            .features
            .insert(id("tool_calling"));
        catalog.bindings[0].evidence[0].binding_digest = catalog.bindings[0]
            .contract_digest(&catalog.models[0])
            .unwrap();
        router.snapshot = RoutingSnapshot::new(catalog, router.snapshot.policy().clone()).unwrap();
        let policy =
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap());
        bindings.router = Arc::new(router);
        bindings.profile_resolver = Arc::new(Catalog);
        bindings.policy = policy.clone();
        bindings.model_exchange = Arc::new(
            ModelExchange::new(self.model.clone(), policy)
                .with_route_inspector(self.base.inspector.clone(), Duration::from_secs(1))
                .unwrap(),
        );
        bindings.tools = Some(self.registry.clone());
        bindings.system_inputs = self.inputs.clone();
        bindings.system_input_resolver = None;
        bindings.settings.tool_execution_limits = ToolExecutionLimits {
            timeout_ms: 30,
            max_receipt_bytes: 4096,
        };
        create_agent(self.profile.clone(), bindings).unwrap()
    }
    async fn start(&self, agent: &Agent) -> RunHandle {
        let mut context = context();
        context.data.system_inputs =
            Some(SystemInputs::new(object(json!({"workspace_id":WORKSPACE}))));
        completed(agent.start(request("request"), context).await.unwrap())
    }
    async fn outcome(&self, handle: &RunHandle) -> RunOutcome {
        completed(handle.outcome(&context()).await.unwrap())
    }
}

fn default_plans() -> Vec<(&'static str, JsonObject)> {
    vec![
        ("read", object(json!({"query":"first"}))),
        ("write", object(json!({"query":"second","limit":2}))),
    ]
}
fn observations(request: &ModelRequest) -> Vec<(&Id, &Value)> {
    request
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            ModelContent::ToolResult {
                provider_call_id,
                content,
            } => Some((provider_call_id, content)),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn an_agent_executes_two_calls_then_receives_only_the_safe_observations_and_original_arguments()
 {
    let fixture = Fixture::new(default_plans(), Behavior::Success);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(*fixture.order.lock().unwrap(), vec!["read", "write"]);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        fixture.tools[0].arguments.lock().unwrap()[0],
        object(json!({"query":"first","limit":10,"workspace_id":WORKSPACE}))
    );
    assert_eq!(
        fixture.tools[1].arguments.lock().unwrap()[0],
        object(json!({"query":"second","limit":2,"workspace_id":WORKSPACE}))
    );
    let requests = fixture.model.requests.lock().unwrap();
    for tool in &requests[0].tools {
        assert_eq!(
            tool.model_input_schema["properties"]
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>(),
            ["query".to_owned(), "limit".to_owned()]
                .into_iter()
                .collect()
        );
    }
    let calls: Vec<_> = requests[1]
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            ModelContent::ToolCall {
                provider_call_id,
                name,
                arguments,
            } => Some((provider_call_id, name, arguments)),
            _ => None,
        })
        .collect();
    assert_eq!(
        calls,
        vec![
            (
                &id("provider-0"),
                &id("read"),
                &object(json!({"query":"first"}))
            ),
            (
                &id("provider-1"),
                &id("write"),
                &object(json!({"query":"second","limit":2}))
            )
        ]
    );
    assert_eq!(
        observations(&requests[1]),
        vec![
            (
                &id("provider-0"),
                &json!({"status":"succeeded","effect":"not_applied","content":[{"type":"json","value":"read observation"}]})
            ),
            (
                &id("provider-1"),
                &json!({"status":"succeeded","effect":"applied","content":[{"type":"json","value":"write observation"}]})
            )
        ]
    );
}

#[tokio::test]
async fn unknown_invalid_and_denied_calls_return_errors_to_the_model_without_executing() {
    for case in 0..3 {
        let plan = match case {
            0 => ("unregistered", object(json!({"query":"x"}))),
            1 => (
                "read",
                object(json!({"query":"x","workspace_id":WORKSPACE})),
            ),
            _ => ("read", object(json!({"query":"x"}))),
        };
        let fixture = Fixture::new(vec![plan], Behavior::Success);
        if case == 2 {
            fixture.policy.mode.store(1, Ordering::SeqCst);
        }
        let agent = fixture.agent();
        let handle = fixture.start(&agent).await;
        let outcome = fixture.outcome(&handle).await;
        assert_eq!(outcome.result.status(), RunStatus::Succeeded);
        assert_eq!(outcome.usage.tool_attempts, 0);
        assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
        assert!(fixture.order.lock().unwrap().is_empty());
        let requests = fixture.model.requests.lock().unwrap();
        let results = observations(&requests[1]);
        assert_eq!(results.len(), 1);
        assert_ne!(results[0].1["status"], json!("succeeded"));
        assert_eq!(results[0].1["effect"], json!("not_applied"));
        assert!(results[0].1.get("error").is_some());
    }
}

#[tokio::test]
async fn an_applied_write_with_invalid_output_reaches_the_model_as_failure_and_is_not_replayed() {
    let fixture = Fixture::new(
        vec![("write", object(json!({"query":"x"})))],
        Behavior::InvalidOutput,
    );
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[0].state else {
        panic!("write result missing")
    };
    assert_eq!(result.status, ToolResultStatus::Failed);
    assert_eq!(result.effect, ToolEffect::Applied);
    let record = fixture
        .base
        .store
        .read_record(&scope(), result.effect_receipt_ref.as_ref().unwrap())
        .await
        .unwrap();
    assert_eq!(
        record.value()["receipt"],
        json!({"private_receipt":"only-for-storage","target":WORKSPACE})
    );
    {
        let requests = fixture.model.requests.lock().unwrap();
        assert_eq!(observations(&requests[1])[0].1["effect"], json!("applied"));
        assert_eq!(observations(&requests[1])[0].1["status"], json!("failed"));
    }
    let replay = fixture.start(&agent).await;
    assert_eq!(replay.run_id(), handle.run_id());
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn approval_waits_with_a_fixed_binding_before_later_tools_or_model_calls() {
    let fixture = Fixture::new(default_plans(), Behavior::Success);
    fixture.policy.mode.store(2, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(outcome.result.status(), RunStatus::Waiting);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert!(fixture.order.lock().unwrap().is_empty());
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    assert!(saved.snapshot.tool_ledger[0].call.bound_input_ref.is_some());
    assert!(saved.snapshot.tool_ledger[1].call.bound_input_ref.is_none());
    let wait = saved.snapshot.wait.as_ref().unwrap();
    assert!(matches!(wait.target, WaitTarget::Approval { .. }));
    assert_eq!(saved.session.active_run_id.as_ref(), Some(handle.run_id()));
}

#[tokio::test]
async fn approval_required_after_dispatch_reservation_becomes_a_fixed_agent_wait() {
    let fixture = Fixture::new(default_plans(), Behavior::Success);
    fixture.policy.mode.store(3, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(outcome.result.status(), RunStatus::Waiting);
    assert_eq!(fixture.policy.tool_checks.load(Ordering::SeqCst), 3);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert!(fixture.order.lock().unwrap().is_empty());
    assert!(
        fixture
            .tools
            .iter()
            .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
    );
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    let entry = &saved.snapshot.tool_ledger[0];
    let ToolCallState::ApprovalPending { attempt_id, .. } = &entry.state else {
        panic!("an unexecuted reserved call must remain pending approval")
    };
    let bound_ref = entry.call.bound_input_ref.as_ref().unwrap();
    let record = fixture
        .base
        .store
        .read_record(&scope(), bound_ref)
        .await
        .unwrap();
    let compiled = &fixture.registry.get(&id("read")).unwrap().compiled;
    let bound = BoundToolInput::restore(
        &record,
        compiled,
        &scope(),
        handle.run_id(),
        &entry.call,
        saved.snapshot.system_inputs.as_ref(),
    )
    .unwrap();
    assert_eq!(
        bound.execution_args(),
        &object(json!({"query":"first","limit":10,"workspace_id":WORKSPACE}))
    );
    assert_eq!(
        saved.snapshot.wait.as_ref().unwrap().target,
        WaitTarget::Approval {
            target: ApprovalTarget::Tool {
                call_id: entry.call.call_id.clone(),
                binding_digest: bound.binding_digest().clone(),
            },
        }
    );
    assert_eq!(saved.snapshot.usage.tool_attempts, 1);
    let reservations: Vec<_> = saved
        .snapshot
        .reservations
        .iter()
        .filter(|reservation| matches!(reservation.kind, ReservationKind::Tool { .. }))
        .collect();
    assert_eq!(reservations.len(), 1);
    assert_eq!(&reservations[0].attempt_id, attempt_id);
    assert_eq!(
        reservations[0].kind,
        ReservationKind::Tool {
            call_id: entry.call.call_id.clone()
        }
    );
    assert!(matches!(
        saved.snapshot.tool_ledger[1].state,
        ToolCallState::Planned {}
    ));
    assert!(saved.snapshot.tool_ledger[1].call.bound_input_ref.is_none());
    assert_eq!(saved.session.active_run_id.as_ref(), Some(handle.run_id()));
}

#[tokio::test]
async fn uncertain_write_waits_and_does_not_run_the_later_tool_or_next_model() {
    let fixture = Fixture::new(
        vec![
            ("write", object(json!({"query":"x"}))),
            ("read", object(json!({"query":"y"}))),
        ],
        Behavior::Unknown,
    );
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(outcome.result.status(), RunStatus::Waiting);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[0].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    assert!(matches!(
        saved.snapshot.tool_ledger[0].state,
        ToolCallState::Unknown { .. }
    ));
    assert!(matches!(
        saved.snapshot.wait.as_ref().unwrap().target,
        WaitTarget::External { .. }
    ));
    assert!(!outcome.unresolved_effects.is_empty());
}

#[tokio::test]
async fn using_the_last_model_slot_still_executes_its_saved_tool_plan_before_exhaustion() {
    let mut fixture = Fixture::new(default_plans(), Behavior::Success);
    fixture.profile.limits.max_model_calls = 1.try_into().unwrap();
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::ModelCalls
        }
    );
    assert_eq!(outcome.usage.tool_attempts, 2);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert!(fixture.base.store.load(&scope(),handle.run_id()).await.unwrap().snapshot.tool_ledger.iter().all(|entry|matches!(&entry.state,ToolCallState::Settled{result} if result.status==ToolResultStatus::Succeeded)));
}

#[tokio::test]
async fn tool_budget_exhaustion_settles_the_unstarted_plan_without_a_second_model_call() {
    let mut fixture = Fixture::new(default_plans(), Behavior::Success);
    fixture.profile.limits.max_tool_attempts = 1;
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::ToolAttempts
        }
    );
    assert_eq!(fixture.tools[0].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[1].state else {
        panic!("unstarted call remains orphaned")
    };
    assert_eq!(result.effect, ToolEffect::NotApplied);
    assert_ne!(result.status, ToolResultStatus::Succeeded);
}

#[tokio::test]
async fn cancelling_an_entered_write_retains_its_unknown_effect_and_closes_unstarted_calls() {
    let fixture = Fixture::new(
        vec![
            ("write", object(json!({"query":"x"}))),
            ("read", object(json!({"query":"y"}))),
        ],
        Behavior::Pending,
    );
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    tokio::time::timeout(Duration::from_secs(5), fixture.tools[1].entered.notified())
        .await
        .expect("write must enter before cancellation is requested");
    assert_eq!(
        completed(handle.cancel(id("stop"), &context()).await.unwrap()),
        CancelReceipt::Requested
    );
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(outcome.result.status(), RunStatus::Cancelled);
    assert!(!outcome.unresolved_effects.is_empty());
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    assert!(matches!(
        saved.snapshot.tool_ledger[0].state,
        ToolCallState::Unknown { .. }
    ));
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[1].state else {
        panic!("unstarted call not closed")
    };
    assert_eq!(result.effect, ToolEffect::NotApplied);
    assert_eq!(fixture.tools[0].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn the_run_deadline_keeps_an_entered_write_unknown_and_closes_the_remaining_plan() {
    let mut fixture = Fixture::new(
        vec![
            ("write", object(json!({"query":"x"}))),
            ("read", object(json!({"query":"y"}))),
        ],
        Behavior::Pending,
    );
    fixture.profile.limits.max_elapsed_ms = 20.try_into().unwrap();
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    );
    assert!(!outcome.unresolved_effects.is_empty());
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    assert!(matches!(
        saved.snapshot.tool_ledger[0].state,
        ToolCallState::Unknown { .. }
    ));
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[1].state else {
        panic!("unstarted call not closed")
    };
    assert_eq!(result.effect, ToolEffect::NotApplied);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[0].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}
```

## `crates/wickle/tests/budget.rs`

```rust
//! Reservations and stop boundaries exercised against a real state store.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Barrier, Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use wickle::*;

#[allow(dead_code)]
mod support;
use support::{admission, id, scope};

#[derive(Default)]
struct FakeClock {
    reading: Mutex<(i64, u64)>,
    changed: Notify,
}
impl FakeClock {
    fn set(&self, utc_ms: i64, monotonic_ms: u64) {
        *self.reading.lock().unwrap() = (utc_ms, monotonic_ms);
        self.changed.notify_waiters();
    }
}
impl Clock for FakeClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        let (utc_ms, monotonic_ms) = *self.reading.lock().unwrap();
        Ok(ClockReading {
            utc_ms,
            monotonic_ms,
        })
    }
    fn sleep_until<'a>(&'a self, deadline: u64) -> PortFuture<'a, ()> {
        Box::pin(async move {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.now()?.monotonic_ms >= deadline {
                    return Ok(());
                }
                changed.await;
            }
        })
    }
}
#[derive(Default)]
struct FixedIds(AtomicUsize);
impl IdSource for FixedIds {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id(&format!(
            "attempt-{}",
            self.0.fetch_add(1, Ordering::SeqCst)
        )))
    }
}
struct RepeatedId;
impl IdSource for RepeatedId {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id("same-attempt"))
    }
}

enum CommitControl {
    Race(Barrier),
    Pause { entered: Notify, release: Semaphore },
    Fail,
    ExpireDuringLeaseRead(Arc<FakeClock>),
}
struct ControlledStore {
    inner: Arc<MemoryStateStore>,
    control: CommitControl,
}
impl StateStore for ControlledStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
        request_id: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        self.inner.find_request(scope, session_id, request_id)
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
        lease: &'a RunLease,
        now: i64,
    ) -> PortFuture<'a, RunLease> {
        Box::pin(async move {
            let current = self.inner.check_lease(s, r, lease, now).await?;
            if let CommitControl::ExpireDuringLeaseRead(clock) = &self.control {
                clock.set(current.expires_at_ms, current.expires_at_ms as u64);
            }
            Ok(current)
        })
    }
    fn acquire_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        owner: &'a Id,
        now: i64,
        ttl: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.acquire_lease(s, r, owner, now, ttl)
    }
    fn renew_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        lease: &'a RunLease,
        now: i64,
        ttl: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.renew_lease(s, r, lease, now, ttl)
    }
    fn release_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        lease: &'a RunLease,
        now: i64,
    ) -> PortFuture<'a, ()> {
        self.inner.release_lease(s, r, lease, now)
    }
    fn read_events<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        after: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage> {
        self.inner.read_events(s, r, after, limit)
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
            match &self.control {
                CommitControl::Race(barrier) => {
                    barrier.wait().await;
                }
                CommitControl::Fail => {
                    return Err(ContractError::new(
                        ErrorCode::PersistenceUnavailable,
                        "injected.commit",
                    ));
                }
                CommitControl::Pause { .. } | CommitControl::ExpireDuringLeaseRead(_) => {}
            }
            let saved = self.inner.commit(s, r, input).await?;
            if let CommitControl::Pause { entered, release } = &self.control {
                entered.notify_one();
                release.acquire().await.unwrap().forget();
            }
            Ok(saved)
        })
    }
}
struct Fixture {
    store: Arc<MemoryStateStore>,
    clock: Arc<FakeClock>,
    ids: Arc<dyn IdSource>,
    lease: RunLease,
    cancellation: CancellationToken,
}
impl Fixture {
    async fn new(models: u64, tools: u64, repair: u64, recovery: u64) -> Self {
        let store = Arc::new(MemoryStateStore::new());
        let mut input = admission("run", "request", "session", "Read evidence", "1").await;
        input.snapshot.limits.max_model_calls = models.try_into().unwrap();
        input.snapshot.limits.max_tool_attempts = tools;
        input.snapshot.limits.max_repair_attempts = repair;
        input.snapshot.limits.max_recovery_attempts = recovery;
        input.snapshot.limits.max_elapsed_ms = 100.try_into().unwrap();
        input.snapshot.timing = RunTiming::new(0, 100).unwrap();
        store.admit(&scope(), input).await.unwrap();
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("worker"), 0, 200)
            .await
            .unwrap();
        Self {
            store,
            clock: Arc::new(FakeClock::default()),
            ids: Arc::new(FixedIds::default()),
            lease,
            cancellation: CancellationToken::new(),
        }
    }
    async fn budget(&self, store: Arc<dyn StateStore>) -> RunBudget {
        RunBudget::attach(
            store,
            self.clock.clone(),
            self.ids.clone(),
            scope(),
            id("run"),
            self.lease.clone(),
            self.cancellation.clone(),
        )
        .await
        .unwrap()
    }
    async fn saved(&self) -> RunSnapshot {
        self.store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
    }
}
fn model(purpose: ModelPurpose) -> ReservationKind {
    ReservationKind::Model { purpose }
}
fn counted(counter: &AtomicUsize) -> std::future::Ready<Result<(), ContractError>> {
    counter.fetch_add(1, Ordering::SeqCst);
    std::future::ready(Ok(()))
}

#[tokio::test]
async fn model_purposes_share_a_limit_and_do_not_consume_the_tool_budget() {
    let fixture = Fixture::new(3, 1, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    let calls = AtomicUsize::new(0);
    for purpose in [
        ModelPurpose::Agent,
        ModelPurpose::Verification,
        ModelPurpose::Compaction,
    ] {
        budget
            .execute(model(purpose), |_| counted(&calls))
            .await
            .unwrap();
    }
    let error = budget
        .execute(model(ModelPurpose::Agent), |_| counted(&calls))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::BudgetExceeded);
    budget
        .execute(
            ReservationKind::Tool {
                call_id: id("planned-call"),
            },
            |_| counted(&calls),
        )
        .await
        .unwrap();
    for kind in [
        ReservationKind::Repair {},
        ReservationKind::Recovery {},
        ReservationKind::Tool {
            call_id: id("another-call"),
        },
    ] {
        assert_eq!(
            budget
                .execute(kind, |_| counted(&calls))
                .await
                .unwrap_err()
                .code,
            ErrorCode::BudgetExceeded
        );
    }
    let saved = fixture.saved().await;
    assert_eq!(
        (
            calls.load(Ordering::SeqCst),
            saved.usage.model_calls,
            saved.usage.tool_attempts
        ),
        (4, 3, 1)
    );
    assert_eq!(saved.reservations.len(), 4);
}

#[tokio::test]
async fn the_last_model_slot_can_only_authorize_one_competing_dispatch() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let store = Arc::new(ControlledStore {
        inner: fixture.store.clone(),
        control: CommitControl::Race(Barrier::new(2)),
    });
    let left = fixture.budget(store.clone()).await;
    let right = fixture.budget(store).await;
    let calls = AtomicUsize::new(0);
    let (left, right) = tokio::join!(
        left.execute(model(ModelPurpose::Agent), |_| counted(&calls)),
        right.execute(model(ModelPurpose::Verification), |_| counted(&calls)),
    );
    let results = [left, right];
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results.into_iter().find_map(Result::err).unwrap().code,
        ErrorCode::RevisionConflict
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let saved = fixture.saved().await;
    assert_eq!((saved.usage.model_calls, saved.reservations.len()), (1, 1));
}

#[tokio::test]
async fn failed_persistence_never_constructs_the_call_or_charges_the_store() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let store = Arc::new(ControlledStore {
        inner: fixture.store.clone(),
        control: CommitControl::Fail,
    });
    let budget = fixture.budget(store).await;
    let calls = AtomicUsize::new(0);
    assert_eq!(
        budget
            .execute(model(ModelPurpose::Agent), |_| counted(&calls))
            .await
            .unwrap_err()
            .code,
        ErrorCode::PersistenceUnavailable
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.saved().await.usage.model_calls, 0);
}

#[tokio::test]
async fn a_stop_during_reservation_blocks_dispatch_without_refunding_the_saved_attempt() {
    for stop in [
        ErrorCode::Cancelled,
        ErrorCode::DeadlineExceeded,
        ErrorCode::LeaseLost,
    ] {
        let fixture = Fixture::new(2, 0, 0, 0).await;
        let store = Arc::new(ControlledStore {
            inner: fixture.store.clone(),
            control: CommitControl::Pause {
                entered: Notify::new(),
                release: Semaphore::new(0),
            },
        });
        let budget = fixture.budget(store.clone()).await;
        let calls = AtomicUsize::new(0);
        let execute = budget.execute(model(ModelPurpose::Agent), |_| counted(&calls));
        let interrupt = async {
            let CommitControl::Pause { entered, release } = &store.control else {
                unreachable!()
            };
            entered.notified().await;
            match stop {
                ErrorCode::Cancelled => fixture.cancellation.cancel(),
                ErrorCode::DeadlineExceeded => fixture.clock.set(100, 100),
                ErrorCode::LeaseLost => {
                    fixture
                        .store
                        .release_lease(&scope(), &id("run"), &fixture.lease, 0)
                        .await
                        .unwrap();
                    fixture
                        .store
                        .acquire_lease(&scope(), &id("run"), &id("replacement"), 0, 200)
                        .await
                        .unwrap();
                }
                _ => unreachable!(),
            }
            release.add_permits(1);
        };
        let (result, ()) = tokio::join!(execute, interrupt);
        assert_eq!(result.unwrap_err().code, stop);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let saved = fixture.saved().await;
        assert_eq!((saved.usage.model_calls, saved.reservations.len()), (1, 1));
    }
}

#[tokio::test]
async fn an_unknown_attempt_stays_charged_after_reattach_and_retry_gets_a_new_id() {
    let fixture = Fixture::new(2, 0, 1, 1).await;
    let first = fixture.budget(fixture.store.clone()).await;
    first
        .execute(model(ModelPurpose::Agent), |_| async {
            Err::<(), _>(ContractError::new(
                ErrorCode::PersistenceUnavailable,
                "response.unknown",
            ))
        })
        .await
        .unwrap_err();
    fixture.clock.set(10, 10);
    let resumed = fixture.budget(fixture.store.clone()).await;
    resumed
        .execute(model(ModelPurpose::Agent), |_| async { Ok(()) })
        .await
        .unwrap();
    resumed.reserve(ReservationKind::Repair {}).await.unwrap();
    resumed.reserve(ReservationKind::Recovery {}).await.unwrap();
    let saved = fixture.saved().await;
    assert_eq!(saved.usage.model_calls, 2);
    assert_eq!(saved.usage.repair_attempts, 1);
    assert_eq!(saved.usage.recovery_attempts, 1);
    assert_ne!(
        saved.reservations[0].attempt_id,
        saved.reservations[1].attempt_id
    );
    assert_eq!(saved.reservations[0].reserved_at_ms, 0);
    assert_eq!(saved.reservations[1].reserved_at_ms, 10);
    assert_eq!(saved.usage.elapsed_ms, 10);
    assert_eq!(saved.timing.deadline_at_ms, 100);
}

#[tokio::test]
async fn reused_attempt_identity_cannot_authorize_a_second_physical_call() {
    let mut fixture = Fixture::new(2, 0, 0, 0).await;
    fixture.ids = Arc::new(RepeatedId);
    let budget = fixture.budget(fixture.store.clone()).await;
    let calls = AtomicUsize::new(0);
    budget
        .execute(model(ModelPurpose::Agent), |_| counted(&calls))
        .await
        .unwrap();
    assert_eq!(
        budget
            .execute(model(ModelPurpose::Agent), |_| counted(&calls))
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidSnapshot
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.saved().await.usage.model_calls, 1);
}

#[tokio::test]
async fn active_elapsed_uses_monotonic_time_and_reattach_counts_offline_time() {
    let fixture = Fixture::new(3, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    fixture.clock.set(50_000, 10);
    budget.reserve(model(ModelPurpose::Agent)).await.unwrap();
    fixture.clock.set(-50_000, 20);
    budget
        .reserve(model(ModelPurpose::Verification))
        .await
        .unwrap();
    assert_eq!(fixture.saved().await.usage.elapsed_ms, 20);
    let regression = RunBudget::attach(
        fixture.store.clone(),
        fixture.clock.clone(),
        fixture.ids.clone(),
        scope(),
        id("run"),
        fixture.lease.clone(),
        fixture.cancellation.clone(),
    )
    .await;
    assert!(matches!(
        regression,
        Err(ContractError {
            code: ErrorCode::ClockRegression,
            ..
        })
    ));
    fixture.clock.set(95, 40);
    let resumed = fixture.budget(fixture.store.clone()).await;
    assert_eq!(resumed.elapsed_ms().unwrap(), 95);
    fixture.clock.set(100, 45);
    let calls = AtomicUsize::new(0);
    assert_eq!(
        resumed
            .execute(model(ModelPurpose::Compaction), |_| counted(&calls))
            .await
            .unwrap_err()
            .code,
        ErrorCode::DeadlineExceeded
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.saved().await.usage.model_calls, 2);
}

#[tokio::test]
async fn waiting_observation_consumes_time_but_no_model_attempts() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let snapshot = fixture.saved().await;
    let mut commit = support::prepared(&snapshot, fixture.lease.clone(), 0);
    commit.snapshot.status = RunStatus::Waiting;
    commit.snapshot.phase = RunPhase::Waiting;
    commit.snapshot.wait = Some(WaitState {
        wait_id: id("wait"),
        target: WaitTarget::Approval {
            target: ApprovalTarget::Tool {
                call_id: id("call"),
                binding_digest: canonical_digest(&serde_json::json!({})),
            },
        },
        expires_at_ms: None,
    });
    // A wait target needs a matching planned tool in the saved checkpoint.
    commit.snapshot.tool_ledger.push(ToolLedgerEntry {
        call: ToolCall {
            call_id: id("call"),
            model_request_id: id("model"),
            provider_call_id: id("provider-call"),
            tool_name: id("read"),
            model_inputs: JsonObject::new(),
            descriptor_digest: Some(canonical_digest(&serde_json::json!({}))),
            bound_input_ref: None,
        },
        state: ToolCallState::Planned {},
    });
    fixture
        .store
        .commit(&scope(), &id("run"), commit)
        .await
        .unwrap();
    let waiting = fixture.budget(fixture.store.clone()).await;
    let observer = waiting.wait_for_cancellation_or_deadline();
    tokio::pin!(observer);
    assert!(futures_util::poll!(observer.as_mut()).is_pending());
    fixture.clock.set(100, 100);
    assert_eq!(
        observer.await.unwrap_err().code,
        ErrorCode::DeadlineExceeded
    );
    assert_eq!(fixture.saved().await.usage.model_calls, 0);
    let resumed = fixture.budget(fixture.store.clone()).await;
    assert_eq!(resumed.elapsed_ms().unwrap(), 100);
}

#[tokio::test]
async fn cancellation_after_an_effect_does_not_undo_it_or_refund_the_attempt() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    let effects = AtomicUsize::new(0);
    let error = budget
        .execute(model(ModelPurpose::Agent), |_| async {
            effects.fetch_add(1, Ordering::SeqCst);
            fixture.cancellation.cancel();
            Ok(())
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Cancelled);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.saved().await.usage.model_calls, 1);
    assert_eq!(
        budget
            .execute(model(ModelPurpose::Agent), |_| counted(&effects))
            .await
            .unwrap_err()
            .code,
        ErrorCode::Cancelled
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dropping_a_waiter_does_not_cancel_execution_and_a_clock_timer_wakes_at_deadline() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    let mut observer = Box::pin(budget.wait_for_cancellation_or_deadline());
    assert!(futures_util::poll!(observer.as_mut()).is_pending());
    drop(observer);
    budget
        .execute(model(ModelPurpose::Agent), |_| async { Ok(()) })
        .await
        .unwrap();
    assert!(!fixture.cancellation.is_cancelled());
    let clock = SystemClock::new();
    let reading = clock.now().unwrap();
    clock.sleep_until(reading.monotonic_ms + 1).await.unwrap();
    assert!(clock.now().unwrap().monotonic_ms > reading.monotonic_ms);
}

#[tokio::test]
async fn an_in_flight_operation_is_interrupted_when_the_original_deadline_arrives() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    let calls = AtomicUsize::new(0);
    let execution = budget.execute(model(ModelPurpose::Agent), |_| {
        calls.fetch_add(1, Ordering::SeqCst);
        std::future::pending::<Result<(), ContractError>>()
    });
    tokio::pin!(execution);
    assert!(futures_util::poll!(execution.as_mut()).is_pending());
    fixture.clock.set(100, 100);
    assert_eq!(
        execution.await.unwrap_err().code,
        ErrorCode::DeadlineExceeded
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.saved().await.usage.model_calls, 1);
}

#[tokio::test]
async fn lease_expiration_while_checking_storage_blocks_the_next_call() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    fixture
        .store
        .renew_lease(&scope(), &id("run"), &fixture.lease, 0, 10)
        .await
        .unwrap();
    let store = Arc::new(ControlledStore {
        inner: fixture.store.clone(),
        control: CommitControl::ExpireDuringLeaseRead(fixture.clock.clone()),
    });
    let budget = fixture.budget(store).await;
    let calls = AtomicUsize::new(0);
    assert_eq!(
        budget
            .execute(model(ModelPurpose::Agent), |_| counted(&calls))
            .await
            .unwrap_err()
            .code,
        ErrorCode::LeaseLost
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.saved().await.usage.model_calls, 1);
}

#[tokio::test]
async fn a_monotonic_clock_regression_cannot_extend_a_live_segment() {
    let fixture = Fixture::new(2, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    fixture.clock.set(20, 20);
    budget.reserve(model(ModelPurpose::Agent)).await.unwrap();
    fixture.clock.set(30, 10);
    let calls = AtomicUsize::new(0);
    assert_eq!(
        budget
            .execute(model(ModelPurpose::Agent), |_| counted(&calls))
            .await
            .unwrap_err()
            .code,
        ErrorCode::ClockRegression
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.saved().await.usage.model_calls, 1);
}

#[tokio::test]
async fn saved_attempts_and_original_deadline_cannot_be_refunded_or_replaced() {
    let fixture = Fixture::new(2, 1, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    budget.reserve(model(ModelPurpose::Agent)).await.unwrap();
    let before = fixture.saved().await;
    let mut refund = support::prepared(&before, fixture.lease.clone(), 0);
    refund.snapshot.reservations.clear();
    refund.snapshot.usage.model_calls = 0;
    assert_eq!(
        fixture
            .store
            .commit(&scope(), &id("run"), refund)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidTransition
    );
    let mut reset = support::prepared(&before, fixture.lease.clone(), 0);
    reset.snapshot.timing = RunTiming::new(1, 100).unwrap();
    reset.snapshot.reservations[0].reserved_at_ms = 1;
    assert_eq!(
        fixture
            .store
            .commit(&scope(), &id("run"), reset)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidTransition
    );
    let mut extra = support::prepared(&before, fixture.lease.clone(), 0);
    extra.snapshot.usage.model_calls += 1;
    assert_eq!(
        fixture
            .store
            .commit(&scope(), &id("run"), extra)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidSnapshot
    );
    assert_eq!(fixture.saved().await, before);
}

#[tokio::test]
async fn restoring_a_snapshot_rejects_missing_reservations_and_forged_counts() {
    let fixture = Fixture::new(2, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    budget.reserve(model(ModelPurpose::Agent)).await.unwrap();
    let snapshot = fixture.saved().await;
    let mut missing = snapshot.clone();
    missing.reservations.clear();
    let mut forged = snapshot.clone();
    forged.usage.model_calls = 2;
    let mut duplicated = snapshot.clone();
    duplicated
        .reservations
        .push(snapshot.reservations[0].clone());
    duplicated.usage.model_calls = 2;
    for invalid in [missing, forged, duplicated] {
        assert_eq!(
            RunSnapshot::from_json(&serde_json::to_string(&invalid).unwrap())
                .unwrap_err()
                .code,
            ErrorCode::InvalidSnapshot
        );
    }
    assert_eq!(
        RunSnapshot::from_json(&serde_json::to_string(&snapshot).unwrap()).unwrap(),
        snapshot
    );
}
```

## `crates/wickle/tests/context_projection.rs`

```rust
//! Pinned prompts, provenance, protected-input boundaries, and atomic context selection.

use serde_json::{Value, json};
use std::collections::BTreeSet;
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn versioned(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
fn record(value: &str) -> RecordRef {
    RecordRef {
        record_id: id(value),
        revision: 1,
        digest: canonical_digest(&json!(value)),
    }
}
fn compiled_tool(name: &str) -> CompiledTool {
    let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    SchemaCompiler::new().compile(ToolDescriptor {
        tool:versioned(name), name:id(name), description:format!("{name} records"),
        input_schema:json!({"type":"object","properties":{"query":{"type":"string"},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters:vec!["query".into()], system_bindings:None,
        output_schema:json!({"type":"string"}), side_effect:ToolSideEffect::ReadOnly,
        concurrency:ToolConcurrency::Serial, retry:ToolRetryPolicy::Never, reconcile:false,
        max_output_bytes:4096.try_into().unwrap(),
    }, &registry).unwrap()
}

struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(reference.version.clone().unwrap_or_else(|| id("1"))),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(
                    &json!({"id":reference.id,"version":reference.version}),
                ),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| reference.id.clone()),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

fn route() -> ResolvedModelRoute {
    ResolvedModelRoute {
        binding: versioned("model"),
        catalog_revision: id("catalog"),
        routing_policy_revision: id("policy"),
        requested_model: id("model"),
        model_id: id("model"),
        model_version: id("release"),
        version_semantics: VersionSemantics::Pinned,
        provider: id("provider"),
        target: JsonObject::new(),
        deployment_revision: None,
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        adapter: versioned("adapter"),
        capability_revision: id("capabilities"),
        connection_ref: versioned("connection"),
    }
}

struct Fixture {
    scope: Scope,
    profile: ResolvedProfile,
    prompt: PromptSnapshot,
    prompt_digest: JsonDigest,
    tools: Vec<CompiledTool>,
    skill: SkillManifest,
    request: RunRequest,
    run_id: Id,
    step_id: Id,
    request_message_id: Id,
}

impl Fixture {
    async fn new() -> Self {
        let scope = Scope {
            tenant_id: id("tenant"),
            workspace_id: id("workspace"),
            user_id: None,
        };
        let profile=AgentProfile::from_json(r#"{
            "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
            "name":"Assistant","description":"Context fixture","instructions":{"text":"Profile instructions"},
            "model_binding":"model","tools":[{"tool_id":"search","version":"1"},{"tool_id":"read","version":"1"}],
            "skills":[{"skill_id":"analysis","version":"1"}],"connectors":[],
            "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
            "limits":{"max_model_calls":4,"max_tool_attempts":4,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
        }"#).unwrap();
        let profile = ProfileValidator::new(&Catalog)
            .validate(&profile, &scope)
            .await
            .unwrap();
        let tools = vec![compiled_tool("search"), compiled_tool("read")];
        let skill = SkillManifest {
            skill: versioned("analysis"),
            name: "Analysis".into(),
            description: "Analyze evidence".into(),
            manifest_digest: canonical_digest(&json!("analysis manifest")),
        };
        let bindings = profile
            .profile()
            .tools
            .iter()
            .cloned()
            .zip(tools.iter().cloned())
            .map(|(selection, compiled)| PromptToolBinding {
                selection,
                compiled,
            })
            .collect();
        let prompt = PromptSnapshot::create(
            &profile,
            vec!["Host rule A".into(), "Host rule B".into()],
            None,
            bindings,
            vec![skill.clone()],
        )
        .unwrap();
        let prompt_digest = prompt.digest();
        Self {
            scope,
            profile,
            prompt,
            prompt_digest,
            tools,
            skill,
            request: RunRequest {
                request_id: id("user-request"),
                session_id: id("session"),
                input: vec![InputContent::Text {
                    text: "Current requested work".into(),
                }],
                trigger: RunTrigger::User {},
                model_options: JsonObject::new(),
                output_contract: None,
            },
            run_id: id("current-run"),
            step_id: id("step"),
            request_message_id: id("current-message"),
        }
    }
    fn current_message(&self, sequence: u64) -> Message {
        Message {
            message_id: self.request_message_id.clone(),
            run_id: self.run_id.clone(),
            sequence: sequence.try_into().unwrap(),
            role: MessageRole::User,
            content: self
                .request
                .input
                .iter()
                .cloned()
                .map(|content| ContentBlock::Content { content })
                .collect(),
            origin: MessageOrigin::User,
            visibility: Visibility::UserAndModel,
        }
    }
    fn input<'a>(
        &'a self,
        transcript: &'a [Message],
        items: &'a [ContextItem],
        opaque: &'a [ScopedOpaque],
    ) -> ProjectionInput<'a> {
        ProjectionInput {
            profile: &self.profile,
            scope: &self.scope,
            run_id: &self.run_id,
            model_step_id: &self.step_id,
            current_request: &self.request,
            current_request_message_id: &self.request_message_id,
            transcript,
            context_items: items,
            opaque_records: opaque,
            expected_prompt_digest: &self.prompt_digest,
            request_id: self.step_id.clone(),
            purpose: ModelPurpose::Agent,
            route: route(),
            output: ModelOutput::Text {},
            max_output_tokens: 128.try_into().unwrap(),
            options: JsonObject::new(),
            response_limits: ModelResponseLimits {
                max_input_bytes: 100_000,
                max_response_bytes: 4096,
                max_delta_bytes: 1024,
                max_events: 32,
                max_tool_calls: 4,
            },
            limits: ProjectionLimits {
                max_bytes: 100_000,
                max_items: 100,
            },
        }
    }
    fn item(&self, name: &str, origin: ContextOrigin, priority: ContextPriority) -> ContextItem {
        ContextItem::new(
            id(name),
            origin,
            versioned(name),
            self.scope.clone(),
            vec![InputContent::Text {
                text: format!("data for {name}"),
            }],
            ContextLifetime::Run {
                run_id: self.run_id.clone(),
            },
            priority,
        )
    }
}

fn message(
    run: &str,
    sequence: u64,
    role: MessageRole,
    origin: MessageOrigin,
    content: Vec<ContentBlock>,
) -> Message {
    Message {
        message_id: id(&format!("message-{sequence}")),
        run_id: id(run),
        sequence: sequence.try_into().unwrap(),
        role,
        origin,
        content,
        visibility: Visibility::UserAndModel,
    }
}

#[tokio::test]
async fn host_model_options_reach_the_port_without_becoming_prompt_content() {
    use std::{sync::Mutex, time::Duration};
    struct ObserveOptions(Mutex<Option<JsonObject>>);
    impl ModelPort for ObserveOptions {
        fn binding(&self) -> ModelPortBinding {
            let route = route();
            ModelPortBinding {
                provider: route.provider,
                adapter: route.adapter,
                connection_ref: route.connection_ref,
            }
        }
        fn generate<'a>(
            &'a self,
            request: &'a ModelRequest,
            _: &'a ModelCallContext,
        ) -> PortStream<'a, ModelEvent> {
            *self.0.lock().unwrap() = Some(request.options.clone());
            Box::pin(futures_util::stream::iter([Ok(
                ModelEvent::ResponseCompleted {
                    finish: ModelFinish::Stop,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![],
                },
            )]))
        }
    }
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let assembler = ContextAssembler::new();
    let baseline = assembler
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let options = object(json!({"reasoning_effort":"high", "provider_mode":{"budget":32}}));
    let mut input = fixture.input(&transcript, &[], &[]);
    input.options = options.clone();
    let projected = assembler.project(&fixture.prompt, input).unwrap();
    assert_eq!(projected.request.messages, baseline.request.messages);
    assert_eq!(projected.request.tools, baseline.request.tools);
    assert_ne!(projected.request.digest(), baseline.request.digest());
    let port = ObserveOptions(Mutex::new(None));
    let call = ModelCallContext {
        attempt_id: id("attempt"),
        run_id: fixture.run_id.clone(),
        scope: fixture.scope.clone(),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(1),
    };
    collect_model_response(&projected.request, port.generate(&projected.request, &call))
        .await
        .unwrap();
    assert_eq!(*port.0.lock().unwrap(), Some(options.clone()));
    let mut limited = fixture.input(&transcript, &[], &[]);
    limited.options = options;
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len();
    assert_eq!(
        assembler
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}
fn text(value: &str) -> ContentBlock {
    ContentBlock::Content {
        content: InputContent::Text { text: value.into() },
    }
}
fn round(run: &str, sequence: u64, label: &str, body: &str, tool: &CompiledTool) -> Vec<Message> {
    let call_message = id(&format!("message-{sequence}"));
    let call = ToolCall {
        call_id: id(label),
        model_request_id: id(&format!("request-{label}")),
        provider_call_id: id(&format!("provider-{label}")),
        tool_name: id("search"),
        model_inputs: object(json!({"query":label})),
        descriptor_digest: Some(tool.descriptor_digest().clone()),
        bound_input_ref: Some(record("bound-private-input")),
    };
    let result = ToolResult {
        call_id: call.call_id.clone(),
        call_message_id: call_message,
        status: ToolResultStatus::Failed,
        effect: ToolEffect::NotApplied,
        content: vec![InputContent::Text { text: body.into() }],
        effect_receipt_ref: Some(record("private-effect-receipt")),
        error: Some(Failure {
            code: id("unavailable"),
            diagnostic_ref: Some(record("private-diagnostic")),
        }),
    };
    vec![
        message(
            run,
            sequence,
            MessageRole::Assistant,
            MessageOrigin::Model,
            vec![ContentBlock::ToolCall { call }],
        ),
        message(
            run,
            sequence + 1,
            MessageRole::Tool,
            MessageOrigin::Tool,
            vec![ContentBlock::ToolResult { result }],
        ),
    ]
}

#[tokio::test]
async fn host_profile_and_skill_prefixes_are_pinned_and_tools_use_only_compiled_model_schemas() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert_eq!(projected.prompt_digest, fixture.prompt_digest);
    assert_eq!(
        projected.request.messages[0],
        ModelMessage {
            role: ModelRole::System,
            content: vec![
                ModelContent::Text {
                    text: "Host rule A".into()
                },
                ModelContent::Text {
                    text: "Host rule B".into()
                }
            ]
        }
    );
    assert_eq!(
        projected.request.messages[1],
        ModelMessage {
            role: ModelRole::System,
            content: vec![ModelContent::Text {
                text: "Profile instructions".into()
            }]
        }
    );
    assert_eq!(
        projected.request.tools,
        fixture
            .tools
            .iter()
            .map(CompiledTool::to_model_tool)
            .collect::<Vec<_>>()
    );
    for tool in &projected.request.tools {
        assert!(
            tool.model_input_schema["properties"]
                .get("workspace_id")
                .is_none()
        );
    }
    assert_eq!(
        projected.selected_message_ids,
        vec![fixture.request_message_id.clone()]
    );
    projected.request.validate().unwrap();
    let restored = PromptSnapshot::restore(
        &serde_json::to_string(&fixture.prompt).unwrap(),
        &fixture.prompt_digest,
        &fixture.profile,
        &fixture.scope,
    )
    .unwrap();
    let repeated = ContextAssembler::new()
        .project(&restored, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert_eq!(projected.request, repeated.request);
}

#[tokio::test]
async fn current_request_is_exactly_the_persisted_message_and_is_never_silently_replaced() {
    let fixture = Fixture::new().await;
    let current = fixture.current_message(1);
    for transcript in [
        vec![],
        vec![Message {
            content: vec![text("A different request")],
            ..current.clone()
        }],
        vec![Message {
            visibility: Visibility::Internal,
            ..current.clone()
        }],
        vec![Message {
            run_id: id("other-run"),
            ..current.clone()
        }],
    ] {
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
    let transcript = vec![current];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert_eq!(
        projected.request.messages.last().unwrap(),
        &ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Current requested work".into()
            }]
        }
    );
    assert_eq!(
        projected
            .selected_message_ids
            .iter()
            .filter(|message_id| *message_id == &fixture.request_message_id)
            .count(),
        1
    );
}

#[tokio::test]
async fn tool_projection_keeps_model_arguments_and_public_observations_without_execution_records() {
    let fixture = Fixture::new().await;
    let mut transcript = vec![fixture.current_message(1)];
    transcript.extend(round(
        "current-run",
        2,
        "call",
        "public observation",
        &fixture.tools[0],
    ));
    let mut internal = message(
        "current-run",
        4,
        MessageRole::User,
        MessageOrigin::Recovery,
        vec![ContentBlock::Content {
            content: InputContent::Json {
                value: json!({"system_inputs":{"workspace_id":"11111111-1111-4111-8111-111111111111"},"execution_args":{"query":"call","workspace_id":"11111111-1111-4111-8111-111111111111"},"raw_diagnostic":"private"}),
            },
        }],
    );
    internal.visibility = Visibility::Internal;
    transcript.push(internal);
    let original = transcript.clone();
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let contents: Vec<_> = projected
        .request
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .collect();
    let call = contents
        .iter()
        .find_map(|content| match content {
            ModelContent::ToolCall {
                provider_call_id,
                name,
                arguments,
            } => Some((provider_call_id, name, arguments)),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        call,
        (
            &id("provider-call"),
            &id("search"),
            &object(json!({"query":"call"}))
        )
    );
    let result = contents
        .iter()
        .find_map(|content| match content {
            ModelContent::ToolResult {
                provider_call_id,
                content,
            } => Some((provider_call_id, content)),
            _ => None,
        })
        .unwrap();
    assert_eq!(result.0, &id("provider-call"));
    assert_eq!(
        result.1,
        &json!({"status":"failed","effect":"not_applied","content":[{"type":"text","text":"public observation"}],"error":{"code":"unavailable"}})
    );
    assert_eq!(projected.request.messages.len(), 6);
    assert_eq!(
        projected.selected_message_ids,
        vec![
            fixture.request_message_id.clone(),
            id("message-2"),
            id("message-3")
        ]
    );
    assert_eq!(transcript, original);
    projected.request.validate().unwrap();
}

#[tokio::test]
async fn data_context_never_adds_system_authority_or_replaces_the_pinned_prefix() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let items = vec![
        fixture.item(
            "retrieval",
            ContextOrigin::Retrieval,
            ContextPriority::Required,
        ),
        fixture.item("memory", ContextOrigin::Memory, ContextPriority::Required),
        fixture.item(
            "verification",
            ContextOrigin::Verification,
            ContextPriority::Required,
        ),
    ];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &items, &[]))
        .unwrap();
    assert_eq!(projected.selected_context_ids.len(), 3);
    let system = |request: ModelRequest| {
        request
            .messages
            .into_iter()
            .filter(|message| message.role == ModelRole::System)
            .collect::<Vec<_>>()
    };
    assert_eq!(system(projected.request), system(baseline.request));
    let mut forged = serde_json::to_value(&items[0]).unwrap();
    forged["origin"] = json!("host");
    assert!(serde_json::from_value::<ContextItem>(forged).is_err());
}

#[tokio::test]
async fn context_items_validate_scope_and_digest_and_exclude_other_run_or_step_lifetimes() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let original = fixture.item(
        "source",
        ContextOrigin::Retrieval,
        ContextPriority::Required,
    );
    let mut wrong_scope = original.clone();
    wrong_scope.scope.tenant_id = id("other-tenant");
    let mut wrong_run = original.clone();
    wrong_run.lifetime = ContextLifetime::Run {
        run_id: id("other-run"),
    };
    let mut wrong_step = original.clone();
    wrong_step.lifetime = ContextLifetime::Step {
        run_id: fixture.run_id.clone(),
        model_step_id: id("other-step"),
    };
    let mut changed_content = original;
    changed_content.content = vec![InputContent::Text {
        text: "changed data".into(),
    }];
    let with_valid_digest = |item: ContextItem| {
        ContextItem::new(
            item.item_id,
            item.origin,
            item.source_ref,
            item.scope,
            item.content,
            item.lifetime,
            item.priority_class,
        )
    };
    for item in [with_valid_digest(wrong_scope), changed_content] {
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[item], &[]))
                .is_err()
        );
    }
    for item in [with_valid_digest(wrong_run), with_valid_digest(wrong_step)] {
        let items = vec![item];
        let projected = ContextAssembler::new()
            .project(&fixture.prompt, fixture.input(&transcript, &items, &[]))
            .unwrap();
        assert!(projected.selected_context_ids.is_empty());
        assert_eq!(projected.dropped_context_ids, vec![id("source")]);
    }
}

#[tokio::test]
async fn a_current_tool_round_cannot_be_incomplete_or_have_an_unpaired_result() {
    let fixture = Fixture::new().await;
    let complete = round("current-run", 2, "call", "observation", &fixture.tools[0]);
    for transcript in [
        vec![fixture.current_message(1), complete[0].clone()],
        vec![fixture.current_message(1), complete[1].clone()],
        vec![
            fixture.current_message(1),
            complete[0].clone(),
            message(
                "current-run",
                3,
                MessageRole::User,
                MessageOrigin::User,
                vec![text("interleaved")],
            ),
            Message {
                sequence: 4.try_into().unwrap(),
                ..complete[1].clone()
            },
        ],
    ] {
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
}

#[tokio::test]
async fn raw_transcript_system_roles_cannot_extend_or_replace_the_pinned_prefix() {
    let fixture = Fixture::new().await;
    for origin in [
        MessageOrigin::Host,
        MessageOrigin::Profile,
        MessageOrigin::Retrieval,
        MessageOrigin::Memory,
    ] {
        let transcript = vec![
            fixture.current_message(1),
            message(
                "current-run",
                2,
                MessageRole::System,
                origin,
                vec![text("untrusted transcript instructions")],
            ),
        ];
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
}

#[tokio::test]
async fn visibility_filtering_and_message_references_cannot_separate_a_tool_call_from_its_result() {
    let fixture = Fixture::new().await;
    for hidden in [0, 1] {
        let mut messages = round("current-run", 2, "call", "observation", &fixture.tools[0]);
        messages[hidden].visibility = Visibility::Internal;
        let mut transcript = vec![fixture.current_message(1)];
        transcript.extend(messages);
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
    let mut messages = round("current-run", 2, "call", "observation", &fixture.tools[0]);
    let ContentBlock::ToolResult { result } = &mut messages[1].content[0] else {
        unreachable!()
    };
    result.call_message_id = id("some-other-call-message");
    let mut transcript = vec![fixture.current_message(1)];
    transcript.extend(messages);
    assert!(
        ContextAssembler::new()
            .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
            .is_err()
    );
}

#[tokio::test]
async fn bounded_selection_discards_an_older_run_as_a_whole_and_retains_the_latest_complete_round()
{
    let fixture = Fixture::new().await;
    let large = "old ".repeat(512);
    let mut transcript = vec![message(
        "old-run",
        1,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Old work")],
    )];
    transcript.extend(round("old-run", 2, "old-call", &large, &fixture.tools[0]));
    transcript.push(message(
        "old-run",
        4,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Old answer")],
    ));
    transcript.push(message(
        "recent-run",
        5,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Recent work")],
    ));
    transcript.extend(round(
        "recent-run",
        6,
        "recent-call",
        "Recent observation",
        &fixture.tools[0],
    ));
    transcript.push(message(
        "recent-run",
        8,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Recent answer")],
    ));
    transcript.push(fixture.current_message(9));
    let original = transcript.clone();
    let full = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let full_bytes = serde_json::to_vec(&full.request).unwrap().len();
    let mut bounded = fixture.input(&transcript, &[], &[]);
    bounded.limits.max_bytes = full_bytes - large.len() / 2;
    let byte_limit = bounded.limits.max_bytes;
    let selected = ContextAssembler::new()
        .project(&fixture.prompt, bounded)
        .unwrap();
    assert_eq!(
        selected.selected_message_ids,
        vec![
            id("message-5"),
            id("message-6"),
            id("message-7"),
            id("message-8"),
            fixture.request_message_id.clone()
        ]
    );
    assert_eq!(
        selected.dropped_message_ids,
        vec![
            id("message-1"),
            id("message-2"),
            id("message-3"),
            id("message-4")
        ]
    );
    assert!(serde_json::to_vec(&selected.request).unwrap().len() <= byte_limit);
    selected.request.validate().unwrap();
    assert_eq!(transcript, original);
}

#[tokio::test]
async fn required_current_work_and_prefix_report_budget_exhaustion_instead_of_truncation() {
    let fixture = Fixture::new().await;
    let current = vec![fixture.current_message(1)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&current, &[], &[]))
        .unwrap();
    let count = baseline
        .request
        .messages
        .iter()
        .map(|message| message.content.len())
        .sum::<usize>()
        + baseline.request.tools.len();
    let mut item_limited = fixture.input(&current, &[], &[]);
    item_limited.limits.max_items = count - 1;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, item_limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
    let mut input_limited = fixture.input(&current, &[], &[]);
    input_limited.response_limits.max_input_bytes = 1;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, input_limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
    let mut transcript = current;
    transcript.extend(round(
        "current-run",
        2,
        "current-call",
        &"data ".repeat(400),
        &fixture.tools[0],
    ));
    let mut bounded = fixture.input(&transcript, &[], &[]);
    bounded.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 64;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, bounded)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn optional_context_can_be_removed_but_required_context_cannot_be_silently_dropped() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let mut optional = fixture.item(
        "optional",
        ContextOrigin::Retrieval,
        ContextPriority::Optional,
    );
    optional = ContextItem::new(
        optional.item_id,
        optional.origin,
        optional.source_ref,
        optional.scope,
        vec![InputContent::Text {
            text: "reference data ".repeat(1000),
        }],
        optional.lifetime,
        optional.priority_class,
    );
    let items = vec![optional.clone()];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let mut limited = fixture.input(&transcript, &items, &[]);
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 128;
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, limited)
        .unwrap();
    assert!(projected.selected_context_ids.is_empty());
    assert_eq!(projected.dropped_context_ids, vec![id("optional")]);
    let required = ContextItem::new(
        optional.item_id,
        optional.origin,
        optional.source_ref,
        optional.scope,
        optional.content,
        optional.lifetime,
        ContextPriority::Required,
    );
    let required_items = vec![required];
    let mut limited = fixture.input(&transcript, &required_items, &[]);
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 128;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn pinned_snapshot_rejects_other_scope_profile_and_recomputed_replacement_contents() {
    let fixture = Fixture::new().await;
    let mut foreign = fixture.scope.clone();
    foreign.tenant_id = id("other-tenant");
    assert!(
        fixture
            .prompt
            .validate_for(&fixture.profile, &foreign, &fixture.prompt_digest)
            .is_err()
    );
    let mut changed_profile = fixture.profile.profile().clone();
    changed_profile.version = id("2.0.0");
    let changed_profile = ProfileValidator::new(&Catalog)
        .validate(&changed_profile, &fixture.scope)
        .await
        .unwrap();
    assert!(
        fixture
            .prompt
            .validate_for(&changed_profile, &fixture.scope, &fixture.prompt_digest)
            .is_err()
    );
    let bindings = fixture
        .profile
        .profile()
        .tools
        .iter()
        .cloned()
        .zip(fixture.tools.iter().cloned())
        .map(|(selection, compiled)| PromptToolBinding {
            selection,
            compiled,
        })
        .collect();
    let replacement = PromptSnapshot::create(
        &fixture.profile,
        vec!["Replacement Host policy".into()],
        None,
        bindings,
        vec![fixture.skill.clone()],
    )
    .unwrap();
    assert!(
        replacement
            .validate_for(&fixture.profile, &fixture.scope, &fixture.prompt_digest)
            .is_err()
    );
    assert!(
        PromptSnapshot::restore(
            &serde_json::to_string(&replacement).unwrap(),
            &fixture.prompt_digest,
            &fixture.profile,
            &fixture.scope
        )
        .is_err()
    );
}

#[tokio::test]
async fn prompt_creation_rejects_unselected_tools_and_changed_skill_versions() {
    let fixture = Fixture::new().await;
    let mut extra = fixture
        .profile
        .profile()
        .tools
        .iter()
        .cloned()
        .zip(fixture.tools.iter().cloned())
        .map(|(selection, compiled)| PromptToolBinding {
            selection,
            compiled,
        })
        .collect::<Vec<_>>();
    extra.push(PromptToolBinding {
        selection: ToolBindingRef::Catalog(CatalogToolRef {
            tool_id: id("unselected"),
            version: id("1"),
            bindings: None,
            config: None,
        }),
        compiled: compiled_tool("unselected"),
    });
    assert!(
        PromptSnapshot::create(
            &fixture.profile,
            vec!["Host rule".into()],
            None,
            extra,
            vec![fixture.skill.clone()]
        )
        .is_err()
    );
    let bindings = fixture
        .profile
        .profile()
        .tools
        .iter()
        .cloned()
        .zip(fixture.tools.iter().cloned())
        .map(|(selection, compiled)| PromptToolBinding {
            selection,
            compiled,
        })
        .collect();
    let mut changed = fixture.skill.clone();
    changed.skill.version = id("2");
    assert!(
        PromptSnapshot::create(
            &fixture.profile,
            vec!["Host rule".into()],
            None,
            bindings,
            vec![changed]
        )
        .is_err()
    );
}

#[tokio::test]
async fn opaque_replay_requires_matching_protected_record_scope_provider_and_route() {
    let fixture = Fixture::new().await;
    let continuation =
        OpaqueContinuation::new(&route(), json!({"signature":"provider continuation"}));
    let reference = RecordRef {
        record_id: id("opaque"),
        revision: 1,
        digest: canonical_digest(&serde_json::to_value(&continuation).unwrap()),
    };
    let mut transcript = vec![fixture.current_message(1)];
    transcript.push(message(
        "current-run",
        2,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![ContentBlock::ProviderOpaque {
            provider: id("provider"),
            route_digest: route().digest(),
            data_ref: reference.clone(),
        }],
    ));
    let records = vec![ScopedOpaque {
        scope: fixture.scope.clone(),
        reference: reference.clone(),
        provider: id("provider"),
        continuation: continuation.clone(),
    }];
    let accepted = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &records))
        .unwrap();
    assert!(accepted.request.messages.iter().flat_map(|message|&message.content).any(|content|matches!(content,ModelContent::Opaque{continuation:found} if found==&continuation)));
    let mut changed = fixture.input(&transcript, &[], &records);
    changed.route.connection_ref.version = id("different-connection");
    assert!(
        ContextAssembler::new()
            .project(&fixture.prompt, changed)
            .is_err()
    );
    let mut wrong_scope = records.clone();
    wrong_scope[0].scope.tenant_id = id("other-tenant");
    assert!(
        ContextAssembler::new()
            .project(
                &fixture.prompt,
                fixture.input(&transcript, &[], &wrong_scope)
            )
            .is_err()
    );
    let mut wrong_provider = records.clone();
    wrong_provider[0].provider = id("other-provider");
    assert!(
        ContextAssembler::new()
            .project(
                &fixture.prompt,
                fixture.input(&transcript, &[], &wrong_provider)
            )
            .is_err()
    );
    let mut wrong_record = records;
    wrong_record[0].reference = record("different-record");
    assert!(
        ContextAssembler::new()
            .project(
                &fixture.prompt,
                fixture.input(&transcript, &[], &wrong_record)
            )
            .is_err()
    );
}

#[tokio::test]
async fn the_latest_tool_round_from_an_earlier_run_is_required_context() {
    let fixture = Fixture::new().await;
    let current = vec![fixture.current_message(5)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&current, &[], &[]))
        .unwrap();
    let mut transcript = vec![message(
        "previous-run",
        1,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Previous work")],
    )];
    transcript.extend(round(
        "previous-run",
        2,
        "previous-call",
        &"data ".repeat(400),
        &fixture.tools[0],
    ));
    transcript.push(message(
        "previous-run",
        4,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Previous answer")],
    ));
    transcript.extend(current);
    let unbounded = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert!(unbounded.selected_message_ids.contains(&id("message-2")));
    assert!(unbounded.selected_message_ids.contains(&id("message-3")));
    let mut limited = fixture.input(&transcript, &[], &[]);
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 64;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn an_older_unknown_effect_is_not_dropped_when_a_newer_round_exists() {
    let fixture = Fixture::new().await;
    let large = "unknown effect ".repeat(128);
    let mut transcript = vec![message(
        "unknown-run",
        1,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Earlier work")],
    )];
    let mut uncertain = round("unknown-run", 2, "unknown-call", &large, &fixture.tools[0]);
    let ContentBlock::ToolResult { result } = &mut uncertain[1].content[0] else {
        unreachable!()
    };
    result.status = ToolResultStatus::Unknown;
    result.effect = ToolEffect::Unknown;
    transcript.extend(uncertain);
    transcript.push(message(
        "unknown-run",
        4,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Effect not confirmed")],
    ));
    transcript.push(message(
        "recent-run",
        5,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Recent work")],
    ));
    transcript.extend(round(
        "recent-run",
        6,
        "recent-call",
        "Recent observation",
        &fixture.tools[0],
    ));
    transcript.push(message(
        "recent-run",
        8,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Recent answer")],
    ));
    transcript.push(fixture.current_message(9));
    let full = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert!(full.selected_message_ids.contains(&id("message-3")));
    let mut limited = fixture.input(&transcript, &[], &[]);
    limited.limits.max_bytes = serde_json::to_vec(&full.request).unwrap().len() - large.len() / 2;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn loaded_skill_context_uses_its_pinned_version_without_gaining_system_authority() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let loaded = ContextItem::new(
        id("loaded-skill"),
        ContextOrigin::Skill,
        fixture.skill.skill.clone(),
        fixture.scope.clone(),
        vec![InputContent::Text {
            text: "Loaded task instructions".into(),
        }],
        ContextLifetime::Run {
            run_id: fixture.run_id.clone(),
        },
        ContextPriority::Required,
    );
    let items = vec![loaded.clone()];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &items, &[]))
        .unwrap();
    assert_eq!(projected.selected_context_ids, vec![id("loaded-skill")]);
    let system = |request: ModelRequest| {
        request
            .messages
            .into_iter()
            .filter(|message| message.role == ModelRole::System)
            .collect::<Vec<_>>()
    };
    assert_eq!(system(projected.request), system(baseline.request));
    let changed = ContextItem::new(
        loaded.item_id,
        loaded.origin,
        VersionedRef {
            id: loaded.source_ref.id,
            version: id("2"),
        },
        loaded.scope,
        loaded.content,
        loaded.lifetime,
        loaded.priority_class,
    );
    assert!(
        ContextAssembler::new()
            .project(&fixture.prompt, fixture.input(&transcript, &[changed], &[]))
            .is_err()
    );
}
```

## `crates/wickle/tests/contracts.rs`

```rust
//! Behavioral checks for validation, persisted contracts, and profile identity.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn digest(value: &str) -> JsonDigest {
    canonical_digest(&json!(value))
}
fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    }
}
fn reference(kind: ComponentKind, name: &str) -> ComponentRef {
    ComponentRef {
        kind,
        id: id(name),
        version: if kind == ComponentKind::ModelBinding || kind == ComponentKind::Extension {
            None
        } else {
            Some(id("1.0.0"))
        },
    }
}

fn profile_value() -> Value {
    json!({
        "schema_version": "wickle.agent-profile.v1", "agent_id": "research", "version": "1.0.0",
        "name": "Research assistant", "description": "Find information with sources",
        "instructions": {"text": "Use available evidence."}, "model_binding": "primary",
        "tools": [{"tool_id": "documents.search", "version": "1.0.0", "bindings": {"main": "knowledge"}, "config": {"limit": 5}}],
        "skills": [], "connectors": [{"binding_id": "knowledge", "connector_id": "document-store", "version": "1.0.0"}],
        "context_policy": {"strategy": "bounded"}, "output_contract": {"type": "text"},
        "limits": {"max_model_calls": 8, "max_tool_attempts": 12, "max_repair_attempts": 0, "max_recovery_attempts": 2, "max_elapsed_ms": 30000}
    })
}
fn profile() -> AgentProfile {
    AgentProfile::from_json(&profile_value().to_string()).unwrap()
}

struct Catalog(BTreeMap<ComponentRef, ComponentMetadata>);

impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        requested_scope: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            if requested_scope != &scope() {
                return Err(ContractError::new(ErrorCode::ComponentUnavailable, "scope"));
            }
            self.0
                .get(reference)
                .cloned()
                .ok_or_else(|| ContractError::new(ErrorCode::ComponentUnavailable, "reference"))
        })
    }
}

fn metadata(key: &ComponentRef) -> ComponentMetadata {
    let mut resolved = key.clone();
    resolved.version = Some(id("1.0.0"));
    ComponentMetadata {
        reference: resolved,
        contract_version: 1,
        manifest_digest: digest("manifest"),
        config_schema: json!({"type":"object","additionalProperties":false}),
        dependencies: vec![],
        capabilities: BTreeSet::new(),
        required_capabilities: BTreeSet::new(),
        required_connections: BTreeSet::new(),
        model_name: None,
        hook_position: None,
        exports: vec![],
    }
}

fn catalog() -> Catalog {
    let mut definitions = BTreeMap::new();
    let model = reference(ComponentKind::ModelBinding, "primary");
    let mut model_meta = metadata(&model);
    model_meta.capabilities.insert(id("model.tool_calling"));
    definitions.insert(model, model_meta);
    let connector = reference(ComponentKind::Connector, "document-store");
    definitions.insert(connector.clone(), metadata(&connector));
    let tool = reference(ComponentKind::Tool, "documents.search");
    let mut tool_meta = metadata(&tool);
    tool_meta.config_schema = json!({"type":"object","properties":{"limit":{"type":"integer","minimum":1,"maximum":50}},"required":["limit"],"additionalProperties":false});
    tool_meta.required_connections.insert(id("main"));
    tool_meta
        .required_capabilities
        .insert(id("model.tool_calling"));
    tool_meta.capabilities.insert(id("documents.search"));
    tool_meta.model_name = Some(id("documents_search"));
    definitions.insert(tool, tool_meta);
    Catalog(definitions)
}

async fn resolved() -> ResolvedProfile {
    ProfileValidator::new(&catalog())
        .validate(&profile(), &scope())
        .await
        .unwrap()
}

#[test]
fn digest_matches_independent_sha256_vectors_and_sorts_nested_objects() {
    assert_eq!(
        canonical_digest_json("{}").unwrap().as_str(),
        "sorted-json-v1:sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
    );
    let a = r#"{"z":1,"a":{"x":[3,1],"b":2}}"#;
    let b = r#"{ "a": {"b":2,"x":[3,1]}, "z":1 }"#;
    let actual = canonical_digest_json(a).unwrap();
    assert_eq!(
        actual.as_str(),
        "sorted-json-v1:sha256:b2db09df32697403c319dfb8cd57f51a8400eb9943753795ccbb34d1383f01a2"
    );
    assert_eq!(actual, canonical_digest_json(b).unwrap());
    for changed in [
        r#"{"z":1,"a":{"x":[1,3],"b":2}}"#,
        r#"{"z":2,"a":{"x":[3,1],"b":2}}"#,
    ] {
        assert_ne!(actual, canonical_digest_json(changed).unwrap());
    }
    assert_ne!(
        canonical_digest_json("1").unwrap(),
        canonical_digest_json("1.0").unwrap()
    );
    assert_ne!(
        canonical_digest_json("0").unwrap(),
        canonical_digest_json("-0.0").unwrap()
    );
}

#[test]
fn ambiguous_and_non_json_input_is_rejected_instead_of_being_normalized() {
    for input in [
        "NaN",
        "Infinity",
        "-Infinity",
        "1e400",
        "undefined",
        r#"{"a":1,"a":2}"#,
        r#"{"nested":{"a":1,"a":2}}"#,
        "{} {}",
    ] {
        assert_eq!(
            canonical_digest_json(input).unwrap_err().code,
            ErrorCode::InvalidJson,
            "{input}"
        );
    }
}

#[test]
fn profile_rejects_unknown_fields_runtime_objects_invalid_limits_and_null_options() {
    let invalid = [
        ("api_key", json!("credential-value")),
        ("runtime_bindings", json!({"model":"client"})),
        ("sdk_client", json!({})),
        ("adapters", Value::Null),
        ("hooks", Value::Null),
        ("context_sources", Value::Null),
        ("extensions", Value::Null),
        (
            "instructions",
            json!({"text":"a","module_path":"untrusted-code"}),
        ),
        ("completion_policy", json!({"mode":"verified"})),
        (
            "completion_policy",
            json!({"mode":"turn_end","verifier_ref":{"id":"extra","version":"1"}}),
        ),
        (
            "output_contract",
            json!({"type":"text","schema_ref":{"id":"extra","version":"1"}}),
        ),
        (
            "tools",
            json!([{"tool_id":"documents.search","version":"1.0.0","adapter_binding":"mixed","export_id":"search"}]),
        ),
    ];
    for (key, value) in invalid {
        let mut input = profile_value();
        input[key] = value;
        assert_eq!(
            AgentProfile::from_json(&input.to_string())
                .expect_err(&format!("accepted invalid field: {key}"))
                .code,
            ErrorCode::InvalidContract,
            "{key}"
        );
    }
    for value in [json!(0), json!(-1), json!(1.5), Value::Null] {
        let mut input = profile_value();
        input["limits"]["max_model_calls"] = value;
        assert_eq!(
            AgentProfile::from_json(&input.to_string())
                .unwrap_err()
                .code,
            ErrorCode::InvalidContract
        );
    }
    for field in ["schema_version", "tools", "model_binding", "limits"] {
        let mut input = profile_value();
        input.as_object_mut().unwrap().remove(field);
        assert!(
            AgentProfile::from_json(&input.to_string()).is_err(),
            "missing {field}"
        );
    }
    let mut input = profile_value();
    input["schema_version"] = json!("wickle.agent-profile.v99");
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedSchemaVersion
    );
}

#[test]
fn optional_fields_preserve_presence_and_zero_means_disabled() {
    let absent = profile();
    assert_eq!(absent.completion_policy, CompletionPolicy::TurnEnd {});
    assert_eq!(absent.limits.max_repair_attempts, 0);
    let mut input = profile_value();
    input["adapters"] = json!([]);
    let empty = AgentProfile::from_json(&input.to_string()).unwrap();
    assert!(absent.adapters.is_none());
    assert_eq!(empty.adapters, Some(vec![]));
    assert_ne!(absent.digest(), empty.digest());
    assert_eq!(
        AgentProfile::from_json(&serde_json::to_string(&empty).unwrap()).unwrap(),
        empty
    );
}

#[test]
fn local_binding_errors_are_rejected_before_metadata_resolution() {
    let mut input = profile_value();
    input["tools"][0]["bindings"]["main"] = json!("missing");
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
    let mut input = profile_value();
    let duplicate = input["connectors"][0].clone();
    input["connectors"].as_array_mut().unwrap().push(duplicate);
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .path,
        "connectors.binding_id"
    );
    let mut input = profile_value();
    input["tools"] = json!([{"adapter_binding":"missing","export_id":"search"}]);
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .path,
        "adapter_binding"
    );
    let mut input = profile_value();
    input["context_policy"] = json!({"strategy":"custom"});
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .path,
        "context_policy.version"
    );
}

#[tokio::test]
async fn resolver_accepts_registered_components_and_freezes_their_full_definition_identity() {
    let profile = profile();
    let catalog = catalog();
    let pinned = ProfileValidator::new(&catalog)
        .validate(&profile, &scope())
        .await
        .unwrap();
    assert_eq!(pinned.components().len(), 3);
    assert!(
        pinned
            .components()
            .iter()
            .all(|c| c.reference.version.as_ref() == Some(&id("1.0.0")))
    );
    let restored: ResolvedProfile =
        serde_json::from_str(&serde_json::to_string(&pinned).unwrap()).unwrap();
    restored.ensure_matches(&profile, &scope()).unwrap();
    restored.ensure_same_resolution(&pinned).unwrap();
    let mut changed_catalog = catalog;
    changed_catalog
        .0
        .get_mut(&reference(ComponentKind::ModelBinding, "primary"))
        .unwrap()
        .manifest_digest = digest("new definition");
    let newer = ProfileValidator::new(&changed_catalog)
        .validate(&profile, &scope())
        .await
        .unwrap();
    assert_eq!(
        pinned.ensure_same_resolution(&newer).unwrap_err().code,
        ErrorCode::ProfileMismatch
    );
}

#[tokio::test]
async fn unavailable_dependencies_wrong_versions_and_missing_capabilities_fail_resolution() {
    let p = profile();
    let mut missing = catalog();
    missing
        .0
        .remove(&reference(ComponentKind::Tool, "documents.search"));
    assert_eq!(
        ProfileValidator::new(&missing)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    let mut wrong = catalog();
    wrong
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .reference
        .version = Some(id("2.0.0"));
    assert_eq!(
        ProfileValidator::new(&wrong)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    let mut unsupported = catalog();
    unsupported
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .contract_version = 2;
    assert_eq!(
        ProfileValidator::new(&unsupported)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedContractVersion
    );
    let mut no_capability = catalog();
    no_capability
        .0
        .get_mut(&reference(ComponentKind::ModelBinding, "primary"))
        .unwrap()
        .capabilities
        .clear();
    assert_eq!(
        ProfileValidator::new(&no_capability)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
    let mut no_dependency = catalog();
    no_dependency
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .dependencies
        .push(reference(ComponentKind::Tool, "skills.load"));
    assert_eq!(
        ProfileValidator::new(&no_dependency)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    let mut no_connection = p.clone();
    if let ToolBindingRef::Catalog(tool) = &mut no_connection.tools[0] {
        tool.bindings = None;
    }
    assert_eq!(
        ProfileValidator::new(&catalog())
            .validate(&no_connection, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
}

#[tokio::test]
async fn registered_configuration_schema_rejects_wrong_types_ranges_and_credential_fields() {
    for config in [
        json!({"limit":0}),
        json!({"limit":"5"}),
        json!({"limit":51}),
        json!({"limit":5,"api_key":"credential-value"}),
    ] {
        let mut input = profile_value();
        input["tools"][0]["config"] = config;
        let p = AgentProfile::from_json(&input.to_string()).unwrap();
        let error = ProfileValidator::new(&catalog())
            .validate(&p, &scope())
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidConfiguration);
        assert!(!error.to_string().contains("credential-value"));
        assert!(!format!("{error:?}").contains("credential-value"));
    }
}

#[tokio::test]
async fn external_schema_references_fail_but_literal_reference_data_is_not_executed() {
    for schema in [
        json!({"$ref":"https://unavailable.invalid/schema"}),
        json!({"properties":{"limit":{"$ref":"file:///tmp/schema"}}}),
        json!({"$dynamicRef":"#anchor"}),
        json!({"type":"integer"}),
    ] {
        let mut c = catalog();
        c.0.get_mut(&reference(ComponentKind::Tool, "documents.search"))
            .unwrap()
            .config_schema = schema;
        let error = ProfileValidator::new(&c)
            .validate(&profile(), &scope())
            .await
            .unwrap_err();
        assert!(matches!(
            error.code,
            ErrorCode::InvalidSchema | ErrorCode::InvalidConfiguration
        ));
    }
    let mut c = catalog();
    let meta =
        c.0.get_mut(&reference(ComponentKind::Tool, "documents.search"))
            .unwrap();
    meta.config_schema = json!({"$defs":{"limit":{"type":"integer","minimum":1}},"type":"object","properties":{"limit":{"$ref":"#/$defs/limit"}},"required":["limit"],"additionalProperties":false,"default":{"$ref":"https://example.invalid/literal-data"}});
    ProfileValidator::new(&c)
        .validate(&profile(), &scope())
        .await
        .unwrap();
}

#[tokio::test]
async fn extensions_require_registered_namespaces_and_valid_data() {
    let mut input = profile_value();
    input["extensions"] = json!({"bad":{}});
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
    input["extensions"] = json!({"example.settings":{"enabled":true}});
    let p = AgentProfile::from_json(&input.to_string()).unwrap();
    assert_eq!(
        ProfileValidator::new(&catalog())
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    let mut c = catalog();
    let key = reference(ComponentKind::Extension, "example.settings");
    let mut definition = metadata(&key);
    definition.config_schema = json!({"type":"object","properties":{"enabled":{"type":"boolean"}},"additionalProperties":false});
    c.0.insert(key, definition);
    ProfileValidator::new(&c)
        .validate(&p, &scope())
        .await
        .unwrap();
    input["extensions"]["example.settings"]["enabled"] = json!(1);
    assert_eq!(
        ProfileValidator::new(&c)
            .validate(
                &AgentProfile::from_json(&input.to_string()).unwrap(),
                &scope()
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidConfiguration
    );
}

#[tokio::test]
async fn registered_formats_are_asserted_instead_of_treated_as_annotations() {
    let mut catalog = catalog();
    catalog
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .config_schema = json!({
        "type": "object", "properties": {"example_uuid": {"type": "string", "format": "uuid"}},
        "required": ["example_uuid"], "additionalProperties": false
    });
    let mut input = profile_value();
    input["tools"][0]["config"] = json!({"example_uuid": "not-a-uuid"});
    let invalid = AgentProfile::from_json(&input.to_string()).unwrap();
    assert_eq!(
        ProfileValidator::new(&catalog)
            .validate(&invalid, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidConfiguration
    );
    input["tools"][0]["config"] = json!({"example_uuid": "123e4567-e89b-12d3-a456-426614174000"});
    ProfileValidator::new(&catalog)
        .validate(
            &AgentProfile::from_json(&input.to_string()).unwrap(),
            &scope(),
        )
        .await
        .unwrap();
}

fn adapter_profile() -> (AgentProfile, Catalog) {
    let mut value = profile_value();
    value["tools"] =
        json!([{"adapter_binding":"documents","export_id":"search","alias":"search_documents"}]);
    value["adapters"] = json!([{"binding_id":"documents","adapter_id":"document-tools","version":"1.0.0","connections":{"main":"knowledge"}}]);
    let mut c = catalog();
    let key = reference(ComponentKind::Adapter, "document-tools");
    let mut definition = metadata(&key);
    definition.required_connections.insert(id("main"));
    definition.exports.push(ExportMetadata {
        export_id: id("search"),
        kind: ExportKind::Tool,
        contract_version: 1,
        model_name: Some(id("documents_search")),
        hook_position: None,
        capabilities: BTreeSet::from([id("documents.search")]),
        required_capabilities: BTreeSet::from([id("model.tool_calling")]),
    });
    definition.exports.push(ExportMetadata {
        export_id: id("unused"),
        kind: ExportKind::Tool,
        contract_version: 1,
        model_name: Some(id("unused")),
        hook_position: None,
        capabilities: BTreeSet::from([id("unused.capability")]),
        required_capabilities: BTreeSet::new(),
    });
    c.0.insert(key, definition);
    (AgentProfile::from_json(&value.to_string()).unwrap(), c)
}

#[tokio::test]
async fn adapter_exports_must_exist_match_kind_and_be_selected_to_supply_capabilities() {
    let (p, c) = adapter_profile();
    ProfileValidator::new(&c)
        .validate(&p, &scope())
        .await
        .unwrap();
    let mut wrong_kind = catalog();
    let (_, mut definitions) = adapter_profile();
    let key = reference(ComponentKind::Adapter, "document-tools");
    let mut definition = definitions.0.remove(&key).unwrap();
    definition.exports[0].kind = ExportKind::ContextSource;
    wrong_kind.0.insert(key.clone(), definition);
    assert_eq!(
        ProfileValidator::new(&wrong_kind)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
    let mut missing = p.clone();
    if let ToolBindingRef::Export(export) = &mut missing.tools[0] {
        export.export_id = id("missing");
    }
    assert_eq!(
        ProfileValidator::new(&c)
            .validate(&missing, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
    let mut needs_unselected = c;
    needs_unselected
        .0
        .get_mut(&key)
        .unwrap()
        .required_capabilities
        .insert(id("unused.capability"));
    assert_eq!(
        ProfileValidator::new(&needs_unselected)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
    let mut duplicate = p.clone();
    duplicate.tools.push(duplicate.tools[0].clone());
    assert_eq!(
        ProfileValidator::new(&adapter_profile().1)
            .validate(&duplicate, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
}

#[tokio::test]
async fn saved_profiles_reject_changed_instructions_versions_scope_and_tampered_serialization() {
    let pinned = resolved().await;
    let p = profile();
    let mut changed = p.clone();
    changed.version = id("2.0.0");
    assert_eq!(
        pinned.ensure_matches(&changed, &scope()).unwrap_err().code,
        ErrorCode::ProfileMismatch
    );
    let mut changed = p.clone();
    changed.instructions = Instructions::Text(InstructionText {
        text: "Changed behavior".into(),
    });
    assert_eq!(
        pinned.ensure_matches(&changed, &scope()).unwrap_err().code,
        ErrorCode::ProfileMismatch
    );
    let mut other_scope = scope();
    other_scope.tenant_id = id("other");
    assert_eq!(
        pinned.ensure_matches(&p, &other_scope).unwrap_err().code,
        ErrorCode::ProfileMismatch
    );
    let mut stored = serde_json::to_value(&pinned).unwrap();
    stored["profile"]["version"] = json!("new");
    assert!(serde_json::from_value::<ResolvedProfile>(stored).is_err());
}

#[test]
fn system_inputs_preserve_absent_empty_and_owned_values_without_debug_leakage() {
    let mut input = json!({"scope":{"tenant_id":"t","workspace_id":"w"},"principal_ref":"p","capability_grant_ref":"g"});
    let absent = ExecutionContextData::from_json(&input.to_string()).unwrap();
    assert!(absent.system_inputs.is_none());
    input["system_inputs"] = json!({});
    let empty = ExecutionContextData::from_json(&input.to_string()).unwrap();
    assert!(empty.system_inputs.as_ref().unwrap().values().is_empty());
    input["system_inputs"] = Value::Null;
    assert!(ExecutionContextData::from_json(&input.to_string()).is_err());
    input["system_inputs"] = json!({"workspace_id":"private-workspace-value"});
    let stored = ExecutionContextData::from_json(&input.to_string()).unwrap();
    input["system_inputs"]["workspace_id"] = json!("mutated");
    assert_eq!(
        stored.system_inputs.as_ref().unwrap().values()["workspace_id"],
        json!("private-workspace-value")
    );
    assert!(!format!("{stored:?}").contains("private-workspace-value"));
    let roundtrip =
        ExecutionContextData::from_json(&serde_json::to_string(&stored).unwrap()).unwrap();
    assert_eq!(roundtrip, stored);
}

fn record(name: &str) -> RecordRef {
    RecordRef {
        record_id: id(name),
        revision: 1,
        digest: digest(name),
    }
}
fn request() -> RunRequest {
    RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Find supporting evidence".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    }
}

#[test]
fn absent_model_options_preserve_existing_request_encodings_and_digests() {
    let mut legacy = serde_json::to_value(request()).unwrap();
    legacy.as_object_mut().unwrap().remove("model_options");
    let restored = RunRequest::from_json(&legacy.to_string()).unwrap();
    assert!(restored.model_options.is_empty());
    assert_eq!(
        canonical_digest(&serde_json::to_value(restored).unwrap()),
        canonical_digest(&legacy)
    );
    legacy["model_options"] = json!(null);
    assert!(RunRequest::from_json(&legacy.to_string()).is_err());
}

async fn checkpoint() -> RunSnapshot {
    let p = resolved().await;
    let request = request();
    let system_inputs = Some(SystemInputSnapshotRef {
        snapshot_ref: record("protected-inputs"),
        values_digest: digest("owned inputs"),
        definition_versions: BTreeMap::from([(id("workspace_id"), id("1"))]),
    });
    let wait = WaitState {
        wait_id: id("approval"),
        target: WaitTarget::Approval {
            target: ApprovalTarget::Tool {
                call_id: id("call"),
                binding_digest: digest("bound args"),
            },
        },
        expires_at_ms: Some(100000),
    };
    RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id("run"),
        request_digest: admission_digest(&request, &p, system_inputs.as_ref()),
        request,
        scope: scope(),
        timing: RunTiming::new(0, p.profile().limits.max_elapsed_ms.get()).unwrap(),
        reservations: vec![AttemptReservation {
            attempt_id: id("first-model-attempt"),
            kind: ReservationKind::Model {
                purpose: ModelPurpose::Agent,
            },
            reserved_at_ms: 0,
        }],
        limits: p.profile().limits.clone(),
        profile: p,
        status: RunStatus::Waiting,
        phase: RunPhase::Waiting,
        model_step_id: Some(id("step")),
        usage: BudgetUsage {
            model_calls: 1,
            ..BudgetUsage::default()
        },
        model_ledger: vec![],
        tool_ledger: vec![ToolLedgerEntry {
            call: ToolCall {
                call_id: id("call"),
                model_request_id: id("model-request"),
                provider_call_id: id("provider-call"),
                tool_name: id("documents_search"),
                model_inputs: BTreeMap::from([("query".into(), json!("evidence"))]),
                descriptor_digest: Some(digest("descriptor")),
                bound_input_ref: Some(record("bound-inputs")),
            },
            state: ToolCallState::Planned {},
        }],
        system_inputs,
        wait: Some(wait),
        outcome: None,
        assembly_ref: Some(record("assembly")),
        routing_snapshot_ref: None,
        context_batches: vec![record("context-batch")],
        source_states: vec![],
        revision: 4,
        last_event_seq: 7,
    }
}

#[tokio::test]
async fn approval_checkpoint_roundtrip_preserves_the_target_and_deduplication_identity() {
    let snapshot = checkpoint().await;
    snapshot.validate().unwrap();
    let restored = RunSnapshot::from_json(&serde_json::to_string(&snapshot).unwrap()).unwrap();
    assert_eq!(restored, snapshot);
    let target = match &restored.wait.as_ref().unwrap().target {
        WaitTarget::Approval { target } => target.clone(),
        _ => unreachable!(),
    };
    let command = ResumeCommand {
        run_id: restored.run_id.clone(),
        expected_revision: restored.revision,
        command_id: id("decision"),
        action: ResumeAction::Approve {
            wait_id: restored.wait.as_ref().unwrap().wait_id.clone(),
            target,
        },
    };
    assert_eq!(
        ResumeCommand::from_json(&serde_json::to_string(&command).unwrap()).unwrap(),
        command
    );
    let mut relocated = snapshot.system_inputs.clone().unwrap();
    relocated.snapshot_ref = record("new-storage-location");
    assert_eq!(
        snapshot.request_digest,
        admission_digest(&snapshot.request, &snapshot.profile, Some(&relocated))
    );
    relocated.values_digest = digest("different inputs");
    assert_ne!(
        snapshot.request_digest,
        admission_digest(&snapshot.request, &snapshot.profile, Some(&relocated))
    );
    relocated = snapshot.system_inputs.clone().unwrap();
    relocated
        .definition_versions
        .insert(id("workspace_id"), id("2"));
    assert_ne!(
        snapshot.request_digest,
        admission_digest(&snapshot.request, &snapshot.profile, Some(&relocated))
    );
    let empty = SystemInputSnapshotRef {
        snapshot_ref: record("empty-inputs"),
        values_digest: canonical_digest(&json!({})),
        definition_versions: BTreeMap::new(),
    };
    assert_eq!(
        admission_digest(&snapshot.request, &snapshot.profile, None),
        admission_digest(&snapshot.request, &snapshot.profile, Some(&empty))
    );
}

#[tokio::test]
async fn catalog_and_export_names_cannot_create_ambiguous_tool_routing() {
    let (mut profile, mut catalog) = adapter_profile();
    profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
        tool_id: id("documents.search"),
        version: id("1.0.0"),
        bindings: Some(BTreeMap::from([(id("main"), id("knowledge"))])),
        config: Some(BTreeMap::from([("limit".into(), json!(5))])),
    }));
    catalog
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .model_name = Some(id("search_documents"));
    assert_eq!(
        ProfileValidator::new(&catalog)
            .validate(&profile, &scope())
            .await
            .unwrap_err()
            .path,
        "tools.model_name"
    );
}

#[tokio::test]
async fn checkpoint_rejects_inconsistent_state_budget_inputs_and_dispatch_records() {
    let valid = checkpoint().await;
    let mut malformed = valid.clone();
    malformed.wait = None;
    assert_eq!(
        malformed.validate().unwrap_err().code,
        ErrorCode::InvalidSnapshot
    );
    let mut malformed = valid.clone();
    malformed.limits.max_tool_attempts += 1;
    assert_eq!(malformed.validate().unwrap_err().path, "limits");
    let mut malformed = valid.clone();
    malformed.request.request_id = id("changed");
    assert_eq!(malformed.validate().unwrap_err().path, "request_digest");
    let mut malformed = valid.clone();
    malformed.tool_ledger[0].call.bound_input_ref = None;
    malformed.tool_ledger[0].state = ToolCallState::Dispatching {
        attempt_id: id("attempt"),
        idempotency_key: id("effect"),
    };
    malformed.reservations.push(AttemptReservation {
        attempt_id: id("attempt"),
        kind: ReservationKind::Tool {
            call_id: id("call"),
        },
        reserved_at_ms: 0,
    });
    malformed.usage.tool_attempts += 1;
    assert_eq!(
        malformed.validate().unwrap_err().path,
        "tool_ledger.bound_input_ref"
    );
    let mut malformed = valid.clone();
    malformed.tool_ledger.push(malformed.tool_ledger[0].clone());
    assert_eq!(
        malformed.validate().unwrap_err().path,
        "tool_ledger.call_id"
    );
    let mut stored = serde_json::to_value(&valid).unwrap();
    stored["schema_version"] = json!("wickle.run-snapshot.v2");
    assert_eq!(
        RunSnapshot::from_json(&stored.to_string())
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedSchemaVersion
    );
}

#[tokio::test]
async fn dispatched_and_approval_pending_tools_require_their_own_saved_reservation() {
    for state in [
        ToolCallState::Dispatching {
            attempt_id: id("attempt"),
            idempotency_key: id("effect"),
        },
        ToolCallState::ApprovalPending {
            attempt_id: id("attempt"),
            idempotency_key: id("effect"),
        },
        ToolCallState::Unknown {
            attempt_id: id("attempt"),
            idempotency_key: id("effect"),
        },
    ] {
        let mut snapshot = checkpoint().await;
        snapshot.tool_ledger[0].state = state;
        assert_eq!(
            snapshot.validate().unwrap_err().path,
            "tool_ledger.reservation"
        );
        snapshot.reservations.push(AttemptReservation {
            attempt_id: id("attempt"),
            kind: ReservationKind::Tool {
                call_id: id("different-call"),
            },
            reserved_at_ms: 0,
        });
        snapshot.usage.tool_attempts += 1;
        assert_eq!(
            snapshot.validate().unwrap_err().path,
            "tool_ledger.reservation"
        );
        snapshot.reservations.last_mut().unwrap().kind = ReservationKind::Tool {
            call_id: id("call"),
        };
        snapshot.validate().unwrap();
        snapshot.tool_ledger[0].call.descriptor_digest = None;
        assert_eq!(
            snapshot.validate().unwrap_err().path,
            "tool_ledger.unregistered"
        );
    }
}

#[tokio::test]
async fn an_unregistered_tool_can_only_be_planned_or_settled_without_an_effect() {
    let mut snapshot = checkpoint().await;
    snapshot.tool_ledger[0].call.descriptor_digest = None;
    snapshot.tool_ledger[0].call.bound_input_ref = None;
    snapshot.validate().unwrap();
    let result = ToolResult {
        call_id: id("call"),
        call_message_id: id("original-assistant"),
        status: ToolResultStatus::Failed,
        effect: ToolEffect::NotApplied,
        content: vec![],
        error: None,
        effect_receipt_ref: None,
    };
    snapshot.tool_ledger[0].state = ToolCallState::Settled {
        result: result.clone(),
    };
    snapshot.validate().unwrap();
    for effect in [ToolEffect::Applied, ToolEffect::Unknown] {
        snapshot.tool_ledger[0].state = ToolCallState::Settled {
            result: ToolResult {
                effect,
                ..result.clone()
            },
        };
        assert_eq!(
            snapshot.validate().unwrap_err().path,
            "tool_ledger.unregistered"
        );
    }
    snapshot.tool_ledger[0].state = ToolCallState::Settled {
        result: ToolResult {
            status: ToolResultStatus::Succeeded,
            ..result
        },
    };
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "tool_ledger.unregistered"
    );
}

#[tokio::test]
async fn success_requires_a_matching_completion_basis_and_verified_success_requires_evidence() {
    let mut snapshot = checkpoint().await;
    snapshot.status = RunStatus::Succeeded;
    snapshot.phase = RunPhase::Finish;
    snapshot.wait = None;
    snapshot.outcome = Some(RunOutcome {
        result: OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded,
        },
        output: vec![InputContent::Text {
            text: "Candidate answer".into(),
        }],
        artifacts: vec![],
        usage: snapshot.usage.clone(),
        checkpoint_revision: snapshot.revision,
        verification: None,
        unresolved_effects: vec![],
    });
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "tool_ledger.unsettled"
    );
    snapshot.tool_ledger[0].state = ToolCallState::Settled {
        result: ToolResult {
            call_id: id("call"),
            call_message_id: id("call-message"),
            status: ToolResultStatus::Succeeded,
            effect: ToolEffect::NotApplied,
            content: vec![InputContent::Text {
                text: "Evidence found".into(),
            }],
            effect_receipt_ref: None,
            error: None,
        },
    };
    snapshot.validate().unwrap();
    snapshot
        .outcome
        .as_mut()
        .unwrap()
        .unresolved_effects
        .push(record("unknown-effect"));
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "outcome.unresolved_effects"
    );
    snapshot
        .outcome
        .as_mut()
        .unwrap()
        .unresolved_effects
        .clear();
    snapshot.outcome.as_mut().unwrap().result = OutcomeResult::Succeeded {
        completion_basis: CompletionBasis::Verified,
    };
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "outcome.verification"
    );
    snapshot.outcome.as_mut().unwrap().verification = Some(VerificationSummary {
        verifier_ref: VersionedRef {
            id: id("verifier"),
            version: id("1"),
        },
        criteria_ref: VersionedRef {
            id: id("criteria"),
            version: id("1"),
        },
        verdict: VerificationVerdict::Pass,
        evidence: vec![record("evidence")],
    });
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "outcome.completion_basis"
    );
}

#[test]
fn event_and_input_contracts_reject_unsupported_versions_and_execution_injection() {
    let event = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("event"),
        scope: scope(),
        run_id: id("run"),
        session_id: id("session"),
        seq: 1.try_into().unwrap(),
        timestamp_ms: 1000,
        payload: RunEventPayload::RunFinished {
            outcome_ref: record("outcome"),
        },
    };
    assert_eq!(
        RunEvent::from_json(&serde_json::to_string(&event).unwrap()).unwrap(),
        event
    );
    let mut value = serde_json::to_value(&event).unwrap();
    value["schema_version"] = json!("wickle.run-event.v2");
    assert_eq!(
        RunEvent::from_json(&value.to_string()).unwrap_err().code,
        ErrorCode::UnsupportedSchemaVersion
    );
    let mut value = serde_json::to_value(&event).unwrap();
    value["seq"] = json!(0);
    assert!(RunEvent::from_json(&value.to_string()).is_err());
    let mut value = serde_json::to_value(request()).unwrap();
    value["input"] = json!([{"type":"tool_call","call":{"tool_name":"unapproved"}}]);
    assert!(RunRequest::from_json(&value.to_string()).is_err());
    let mut value = serde_json::to_value(request()).unwrap();
    value["trigger"] = json!({"kind":"user","source_id":"forged"});
    assert!(RunRequest::from_json(&value.to_string()).is_err());
    assert!(
        serde_json::from_value::<ModelAttemptState>(json!({"state":"completed","kind":"timeout"}))
            .is_err()
    );
    assert!(
        serde_json::from_value::<ToolCallState>(
            json!({"state":"planned","idempotency_key":"unexpected"})
        )
        .is_err()
    );
}

#[test]
fn route_roundtrip_keeps_model_api_deployment_and_adapter_versions_distinct() {
    let route = ResolvedModelRoute {
        binding: VersionedRef {
            id: id("binding"),
            version: id("binding-revision"),
        },
        catalog_revision: id("catalog-1"),
        routing_policy_revision: id("policy-1"),
        requested_model: id("requested-alias"),
        model_id: id("model-family"),
        model_version: id("release-A"),
        version_semantics: VersionSemantics::MutableDeployment,
        provider: id("custom-provider"),
        target: BTreeMap::from([("deployment".into(), json!("deployment-name"))]),
        deployment_revision: Some(id("deployment-revision")),
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("api-contract-version"),
        },
        adapter: VersionedRef {
            id: id("adapter"),
            version: id("adapter-version"),
        },
        capability_revision: id("capabilities-1"),
        connection_ref: VersionedRef {
            id: id("connection"),
            version: id("connection-revision"),
        },
    };
    let restored: ResolvedModelRoute =
        serde_json::from_str(&serde_json::to_string(&route).unwrap()).unwrap();
    assert_eq!(restored, route);
    let original = route.digest();
    let mut changed = route.clone();
    changed.model_version = id("release-B");
    assert_ne!(changed.digest(), original);
    changed = route.clone();
    changed.api_contract.version = id("different-api-version");
    assert_ne!(changed.digest(), original);
    changed = route.clone();
    changed.deployment_revision = Some(id("new-deployment-revision"));
    assert_ne!(changed.digest(), original);
    let record = ModelInvocationRecord {
        run_id: id("run"),
        model_step_id: id("step"),
        attempt_id: id("attempt"),
        purpose: ModelPurpose::Agent,
        route,
        selection_reason: id("policy-default"),
        request_digest: digest("request"),
        state: ModelAttemptState::Completed {},
        inspection_ref: None,
        response_ref: None,
        provider_request_id: None,
        reported_model_id: None,
        reported_model_version: None,
        usage: None,
    };
    let restored: ModelInvocationRecord =
        serde_json::from_str(&serde_json::to_string(&record).unwrap()).unwrap();
    assert_eq!(restored.reported_model_version, None);
    assert_eq!(restored.usage, None);
}
```

## `crates/wickle/tests/input_binding.rs`

```rust
//! Frozen system inputs, scoped resolution, defaults, and binding persistence boundaries.

use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use wickle::*;

#[allow(dead_code)]
mod support;
use support::{id, scope};

const OWNED: &str = "11111111-1111-4111-8111-111111111111";
const OTHER: &str = "22222222-2222-4222-8222-222222222222";
fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
fn inputs(value: Value) -> SystemInputs {
    SystemInputs::new(object(value))
}
fn versioned(name: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id("1"),
    }
}
fn definition(key: &str, schema: Value) -> SystemInputDefinition {
    SystemInputDefinition {
        key: id(key),
        version: id("1"),
        value_schema: schema,
        source: SystemInputSource::Run {},
    }
}
fn registry() -> SystemInputRegistry {
    SystemInputRegistry::new(vec![
        definition("workspace_id", json!({"type":"string","format":"uuid"})),
        definition("query", json!({"type":"string"})),
        definition("user_id", json!({"type":"string"})),
    ])
    .unwrap()
}
fn descriptor() -> ToolDescriptor {
    ToolDescriptor {
        tool: versioned("search"),
        name: id("search"),
        description: "Search records".into(),
        input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"default":10},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters: vec!["query".into(), "limit".into()],
        system_bindings: None,
        output_schema: json!({"type":"string"}),
        side_effect: ToolSideEffect::ReadOnly,
        concurrency: ToolConcurrency::Serial,
        retry: ToolRetryPolicy::Never,
        reconcile: false,
        max_output_bytes: 4096.try_into().unwrap(),
    }
}

#[derive(Default)]
struct FakeClock {
    reading: Mutex<(i64, u64)>,
    changed: Notify,
}
impl FakeClock {
    fn advance(&self, millis: u64) {
        *self.reading.lock().unwrap() = (millis as i64, millis);
        self.changed.notify_waiters();
    }
}
impl Clock for FakeClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        let (utc_ms, monotonic_ms) = *self.reading.lock().unwrap();
        Ok(ClockReading {
            utc_ms,
            monotonic_ms,
        })
    }
    fn sleep_until<'a>(&'a self, deadline: u64) -> PortFuture<'a, ()> {
        Box::pin(async move {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.now()?.monotonic_ms >= deadline {
                    return Ok(());
                }
                changed.await;
            }
        })
    }
}
#[derive(Default)]
struct Ids(AtomicUsize);
impl IdSource for Ids {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id(&format!(
            "binding-{}",
            self.0.fetch_add(1, Ordering::SeqCst)
        )))
    }
}
struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id("1")),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("catalog")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: Default::default(),
                required_capabilities: Default::default(),
                required_connections: Default::default(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| reference.id.clone()),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

#[derive(Default)]
struct Policy {
    // 0 allow; 1 target ownership; 2 deny lookup; 3 approve lookup; 4 deny execution; 5 approve execution.
    mode: AtomicUsize,
    lookups: AtomicUsize,
    executions: Mutex<Vec<JsonObject>>,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            match &request.action {
                PolicyAction::ResolveSystemInput { .. } => {
                    self.lookups.fetch_add(1, Ordering::SeqCst);
                    match self.mode.load(Ordering::SeqCst) {
                        2 => {
                            return Ok(PolicyDecision::Deny {
                                reason: id("lookup_denied"),
                            });
                        }
                        3 => {
                            return Ok(PolicyDecision::RequireApproval {
                                reason: id("lookup_approval"),
                            });
                        }
                        _ => {}
                    }
                }
                PolicyAction::ExecuteTool { input } => {
                    self.executions
                        .lock()
                        .unwrap()
                        .push(input.execution_args().clone());
                    match self.mode.load(Ordering::SeqCst) {
                        1 => {
                            // The Host resource catalog associates each real UUID with an owner.
                            let target = input
                                .execution_args()
                                .get("workspace_id")
                                .and_then(Value::as_str);
                            let owner = match target {
                                Some(OWNED) => Some(scope()),
                                Some(OTHER) => Some(Scope {
                                    tenant_id: id("foreign-tenant"),
                                    ..scope()
                                }),
                                _ => None,
                            };
                            if owner.as_ref() != Some(context.scope) {
                                return Ok(PolicyDecision::Deny {
                                    reason: id("target_not_owned"),
                                });
                            }
                        }
                        4 => {
                            return Ok(PolicyDecision::Deny {
                                reason: id("revoked"),
                            });
                        }
                        5 => {
                            return Ok(PolicyDecision::RequireApproval {
                                reason: id("execution_approval"),
                            });
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}

struct Resolver {
    value: Mutex<Option<ResolvedSystemInput>>,
    calls: AtomicUsize,
    requests: Mutex<Vec<SystemInputResolveRequest>>,
    cancel: Mutex<Option<CancellationToken>>,
    advance: Mutex<Option<Arc<FakeClock>>>,
}
impl Resolver {
    fn new(value: Value) -> Self {
        Self {
            value: Mutex::new(Some(ResolvedSystemInput {
                value,
                revision: id("revision-1"),
            })),
            calls: AtomicUsize::new(0),
            requests: Mutex::new(vec![]),
            cancel: Mutex::new(None),
            advance: Mutex::new(None),
        }
    }
    fn set(&self, value: Value, revision: &str) {
        *self.value.lock().unwrap() = Some(ResolvedSystemInput {
            value,
            revision: id(revision),
        });
    }
}
impl SystemInputResolver for Resolver {
    fn resolve<'a>(
        &'a self,
        request: &'a SystemInputResolveRequest,
        context: &'a SystemInputResolveContext,
    ) -> PortFuture<'a, Option<ResolvedSystemInput>> {
        Box::pin(async move {
            assert_eq!(context.scope, scope());
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.requests.lock().unwrap().push(request.clone());
            if let Some(token) = self.cancel.lock().unwrap().as_ref() {
                token.cancel();
            }
            if let Some(clock) = self.advance.lock().unwrap().as_ref() {
                clock.advance(10_000);
            }
            Ok(self.value.lock().unwrap().clone())
        })
    }
}
fn resolver_registry() -> SystemInputRegistry {
    SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("current_workspace"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Resolver {
            resolver_ref: versioned("workspace_lookup"),
        },
    }])
    .unwrap()
}
fn resolver_descriptor() -> ToolDescriptor {
    let mut tool = descriptor();
    tool.system_bindings = Some(BTreeMap::from([(
        "workspace_id".into(),
        id("current_workspace"),
    )]));
    tool
}

struct Fixture {
    store: Arc<MemoryStateStore>,
    clock: Arc<FakeClock>,
    ids: Arc<Ids>,
    lease: RunLease,
    context: ExecutionContext,
    registry: Arc<SystemInputRegistry>,
    tool: CompiledTool,
    policy: Arc<Policy>,
}
impl Fixture {
    async fn new(
        tool: ToolDescriptor,
        registry: SystemInputRegistry,
        supplied: Option<SystemInputs>,
    ) -> Self {
        let registry = Arc::new(registry);
        let tool = SchemaCompiler::new().compile(tool, &registry).unwrap();
        let fixed = RunSystemInputs::capture(scope(), supplied.clone(), &registry).unwrap();
        let fixed_record = fixed.to_record(id("run-inputs"), 7);
        let fixed_ref = fixed.snapshot_ref(fixed_record.reference()).unwrap();
        let mut input = support::admission("run", "request", "session", "Read evidence", "1").await;
        let mut profile = input.snapshot.profile.profile().clone();
        profile.tools = vec![ToolBindingRef::Catalog(CatalogToolRef {
            tool_id: tool.descriptor().tool.id.clone(),
            version: tool.descriptor().tool.version.clone(),
            bindings: None,
            config: None,
        })];
        input.snapshot.profile = ProfileValidator::new(&Catalog)
            .validate(&profile, &scope())
            .await
            .unwrap();
        input.snapshot.system_inputs = Some(fixed_ref);
        input.snapshot.request_digest = admission_digest(
            &input.snapshot.request,
            &input.snapshot.profile,
            input.snapshot.system_inputs.as_ref(),
        );
        let RunEventPayload::RunStarted { profile_digest, .. } = &mut input.events[0].payload
        else {
            unreachable!()
        };
        *profile_digest = input.snapshot.profile.profile_digest().clone();
        input.records.push(fixed_record);
        let store = Arc::new(MemoryStateStore::new());
        store.admit(&scope(), input).await.unwrap();
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("worker"), 0, 20_000)
            .await
            .unwrap();
        let context = ExecutionContext::new(
            ExecutionContextData {
                scope: scope(),
                principal_ref: id("user"),
                capability_grant_ref: id("grant"),
                trace_context: None,
                system_inputs: supplied,
            },
            CancellationToken::new(),
        );
        Self {
            store,
            clock: Arc::new(FakeClock::default()),
            ids: Arc::new(Ids::default()),
            lease,
            context,
            registry,
            tool,
            policy: Arc::new(Policy::default()),
        }
    }
    async fn plan(&self, call_id: &str, model_inputs: JsonObject) -> ToolCall {
        let saved = self.store.load(&scope(), &id("run")).await.unwrap();
        let call = ToolCall {
            call_id: id(call_id),
            model_request_id: id(&format!("request-{call_id}")),
            provider_call_id: id(&format!("provider-{call_id}")),
            tool_name: self.tool.descriptor().name.clone(),
            model_inputs,
            descriptor_digest: Some(self.tool.descriptor_digest().clone()),
            bound_input_ref: None,
        };
        let record = ProtectedRecord::new(
            id(&format!("planned-{call_id}")),
            1,
            serde_json::to_value(&call).unwrap(),
        );
        let mut update = support::prepared(
            &saved.snapshot,
            self.lease.clone(),
            self.clock.now().unwrap().utc_ms,
        );
        update.snapshot.phase = RunPhase::Tool;
        update.snapshot.tool_ledger.push(ToolLedgerEntry {
            call: call.clone(),
            state: ToolCallState::Planned {},
        });
        update.snapshot.last_event_seq += 1;
        update.events = vec![support::event(
            &id("run"),
            &id("session"),
            &scope(),
            update.snapshot.last_event_seq,
            RunEventPayload::ToolPlanned {
                call_ref: record.reference().clone(),
            },
        )];
        update.messages = vec![Message {
            message_id: id(&format!("call-message-{call_id}")),
            run_id: id("run"),
            sequence: (saved.session.transcript_revision + 1).try_into().unwrap(),
            role: MessageRole::Assistant,
            origin: MessageOrigin::Model,
            visibility: Visibility::UserAndModel,
            content: vec![ContentBlock::ToolCall { call: call.clone() }],
        }];
        update.records = vec![record];
        self.store
            .commit(&scope(), &id("run"), update)
            .await
            .unwrap();
        call
    }
    async fn budget(&self, store: Arc<dyn StateStore>) -> RunBudget {
        RunBudget::attach(
            store,
            self.clock.clone(),
            self.ids.clone(),
            scope(),
            id("run"),
            self.lease.clone(),
            self.context.cancellation.clone(),
        )
        .await
        .unwrap()
    }
    fn binder(&self, resolver: Option<Arc<dyn SystemInputResolver>>) -> InputBinder {
        InputBinder::new(
            self.registry.clone(),
            resolver,
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap()),
            self.ids.clone(),
        )
    }
    async fn bind(
        &self,
        call_id: &str,
        resolver: Option<Arc<dyn SystemInputResolver>>,
    ) -> Result<ToolBindingResult, ContractError> {
        self.binder(resolver)
            .bind(
                &self.tool,
                &id(call_id),
                &self.context,
                &self.budget(self.store.clone()).await,
            )
            .await
    }
}

#[test]
fn run_capture_rejects_unregistered_keys_invalid_values_and_resolver_source_conflicts() {
    let invalid_value = RunSystemInputs::capture(
        scope(),
        Some(inputs(json!({"workspace_id":"private-invalid-value"}))),
        &registry(),
    )
    .unwrap_err();
    assert_eq!(invalid_value.path, r#"system_inputs["workspace_id"]"#);
    for value in [
        json!({"unregistered":"x"}),
        json!({"workspace_id":"not-a-uuid"}),
        json!({"workspace_id":null}),
    ] {
        assert!(RunSystemInputs::capture(scope(), Some(inputs(value)), &registry()).is_err());
    }
    assert!(
        RunSystemInputs::capture(
            scope(),
            Some(inputs(json!({"current_workspace":OWNED}))),
            &resolver_registry()
        )
        .is_err()
    );
}

#[test]
fn restored_run_inputs_distinguish_omission_from_empty_or_changed_resume_values() {
    let registry = registry();
    let supplied = inputs(json!({"workspace_id":OWNED}));
    let fixed = RunSystemInputs::capture(scope(), Some(supplied.clone()), &registry).unwrap();
    let record = fixed.to_record(id("snapshot"), 1);
    let reference = fixed.snapshot_ref(record.reference()).unwrap();
    let restored = RunSystemInputs::restore(&record, &reference, &scope(), &registry).unwrap();
    restored.validate_resume(None).unwrap();
    restored.validate_resume(Some(&supplied)).unwrap();
    assert!(
        restored
            .validate_resume(Some(&SystemInputs::default()))
            .is_err()
    );
    assert!(
        restored
            .validate_resume(Some(&inputs(json!({"workspace_id":OTHER}))))
            .is_err()
    );
    assert_eq!(restored.values(), &object(json!({"workspace_id":OWNED})));
    let other_scope = Scope {
        tenant_id: id("foreign"),
        ..scope()
    };
    assert!(RunSystemInputs::restore(&record, &reference, &other_scope, &registry).is_err());
    let mut changed_values = record.value().clone();
    *changed_values
        .get_mut("values")
        .unwrap()
        .get_mut("workspace_id")
        .unwrap() = json!(OTHER);
    let wrong_record = ProtectedRecord::new(id("snapshot"), 1, changed_values);
    assert!(RunSystemInputs::restore(&wrong_record, &reference, &scope(), &registry).is_err());
    let mut changed_definitions = fixed.definitions().values().cloned().collect::<Vec<_>>();
    changed_definitions
        .iter_mut()
        .find(|definition| definition.key == id("workspace_id"))
        .unwrap()
        .version = id("2");
    let changed_registry = SystemInputRegistry::new(changed_definitions).unwrap();
    assert!(RunSystemInputs::restore(&record, &reference, &scope(), &changed_registry).is_err());
}

#[tokio::test]
async fn binding_keeps_original_and_defaulted_model_inputs_separate_from_system_arguments() {
    let fixture=Fixture::new(descriptor(),registry(),Some(inputs(json!({"workspace_id":OWNED,"query":"system query must not overwrite","user_id":"extra-registered-value"})))).await;
    let original = object(json!({"query":"model query"}));
    fixture.plan("call", original.clone()).await;
    let bound = fixture.bind("call", None).await.unwrap();
    assert_eq!(bound.input.original_model_inputs(), &original);
    assert_eq!(
        bound.input.normalized_model_inputs(),
        &object(json!({"query":"model query","limit":10}))
    );
    assert_eq!(
        bound.input.execution_args(),
        &object(json!({"query":"model query","limit":10,"workspace_id":OWNED}))
    );
    assert_eq!(bound.decision, PolicyDecision::Allow {});
    assert_eq!(
        bound.input.system_inputs()["workspace_id"].definition_version,
        id("1")
    );
    assert_eq!(
        bound.input.system_inputs()["workspace_id"]
            .resolved
            .as_ref()
            .unwrap()
            .revision,
        id("7")
    );
    let saved = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    assert_eq!(saved.tool_ledger[0].call.model_inputs, original);
    assert_eq!(
        saved.tool_ledger[0].call.bound_input_ref.as_ref(),
        Some(&bound.reference)
    );
    assert_eq!(saved.usage.tool_attempts, 0);
    assert_eq!(
        fixture.policy.executions.lock().unwrap().as_slice(),
        &[object(
            json!({"query":"model query","limit":10,"workspace_id":OWNED})
        )]
    );
}

#[tokio::test]
async fn a_model_supplied_hidden_uuid_is_rejected_even_when_it_matches_the_saved_value() {
    let fixture = Fixture::new(
        descriptor(),
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture
        .plan("call", object(json!({"query":"x","workspace_id":OWNED})))
        .await;
    let before = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    assert!(fixture.bind("call", None).await.is_err());
    assert!(fixture.policy.executions.lock().unwrap().is_empty());
    assert_eq!(
        fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot,
        before
    );
}

#[tokio::test]
async fn a_missing_required_system_value_is_not_filled_from_defaults_or_generated_ids() {
    let mut tool = descriptor();
    tool.input_schema["properties"]["workspace_id"]["default"] = json!(OWNED);
    tool.system_bindings = Some(BTreeMap::from([(
        "workspace_id".into(),
        id("active_workspace_id"),
    )]));
    let registry = SystemInputRegistry::new(vec![definition(
        "active_workspace_id",
        json!({"type":"string","format":"uuid"}),
    )])
    .unwrap();
    let fixture = Fixture::new(tool, registry, None).await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let error = fixture.bind("call", None).await.unwrap_err();
    assert_eq!(error.code, ErrorCode::SystemInputMissing);
    assert_eq!(error.path, r#"system_inputs["active_workspace_id"]"#);
    let saved = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    assert!(saved.tool_ledger[0].call.bound_input_ref.is_none());
    assert!(fixture.policy.executions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn optional_missing_system_inputs_are_omitted_and_explicit_null_follows_the_schema() {
    for supplied in [None, Some(inputs(json!({"workspace_id":null})))] {
        let mut tool = descriptor();
        tool.input_schema["required"] = json!(["query"]);
        tool.input_schema["properties"]["workspace_id"]["type"] = json!(["string", "null"]);
        let registry = SystemInputRegistry::new(vec![definition(
            "workspace_id",
            json!({"type":["string","null"],"format":"uuid"}),
        )])
        .unwrap();
        let is_null = supplied.is_some();
        let fixture = Fixture::new(tool, registry, supplied).await;
        fixture.plan("call", object(json!({"query":"x"}))).await;
        let bound = fixture.bind("call", None).await.unwrap();
        if is_null {
            assert_eq!(
                bound.input.execution_args().get("workspace_id"),
                Some(&Value::Null)
            );
        } else {
            assert!(!bound.input.execution_args().contains_key("workspace_id"));
        }
    }
}

#[tokio::test]
async fn model_defaults_follow_local_references_but_do_not_invent_nested_fields() {
    let mut tool = descriptor();
    tool.input_schema["properties"]["limit"] = json!({"$ref":"#/$defs/Limit"});
    tool.input_schema["$defs"] = json!({"Limit":{"type":"integer","minimum":1,"default":7}});
    tool.input_schema["properties"]["query"] = json!({"type":"object","properties":{"sort":{"type":"string","default":"descending"}},"additionalProperties":false});
    let fixture = Fixture::new(
        tool,
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture.plan("call", object(json!({"query":{}}))).await;
    let bound = fixture.bind("call", None).await.unwrap();
    assert_eq!(
        bound.input.original_model_inputs(),
        &object(json!({"query":{}}))
    );
    assert_eq!(
        bound.input.normalized_model_inputs(),
        &object(json!({"query":{},"limit":7}))
    );
}

#[tokio::test]
async fn a_valid_foreign_uuid_is_denied_using_the_actual_bound_target() {
    for target in [OWNED, OTHER] {
        let fixture = Fixture::new(
            descriptor(),
            registry(),
            Some(inputs(json!({"workspace_id":target}))),
        )
        .await;
        fixture.policy.mode.store(1, Ordering::SeqCst);
        fixture.plan("call", object(json!({"query":"x"}))).await;
        let bound = fixture.bind("call", None).await;
        assert_eq!(bound.is_ok(), target == OWNED);
        assert_eq!(
            fixture.policy.executions.lock().unwrap()[0]["workspace_id"],
            json!(target)
        );
        let saved = fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot;
        assert_eq!(
            saved.tool_ledger[0].call.bound_input_ref.is_some(),
            target == OWNED
        );
    }
}

#[tokio::test]
async fn one_resolver_key_is_resolved_once_per_binding_and_cached_calls_keep_the_old_target() {
    let mut tool = resolver_descriptor();
    tool.input_schema["properties"]["owner_workspace_id"] =
        json!({"type":"string","format":"uuid"});
    tool.input_schema["required"] = json!(["query", "workspace_id", "owner_workspace_id"]);
    tool.system_bindings
        .as_mut()
        .unwrap()
        .insert("owner_workspace_id".into(), id("current_workspace"));
    let fixture = Fixture::new(tool, resolver_registry(), None).await;
    fixture.plan("first", object(json!({"query":"x"}))).await;
    let resolver = Arc::new(Resolver::new(json!(OWNED)));
    let first = fixture.bind("first", Some(resolver.clone())).await.unwrap();
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(first.input.execution_args()["workspace_id"], json!(OWNED));
    assert_eq!(
        first.input.system_inputs()["workspace_id"]
            .resolved
            .as_ref()
            .unwrap()
            .revision,
        id("revision-1")
    );
    assert_eq!(
        first.input.execution_args()["owner_workspace_id"],
        json!(OWNED)
    );
    resolver.set(json!(OTHER), "revision-2");
    let cached = fixture.bind("first", Some(resolver.clone())).await.unwrap();
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(cached.reference, first.reference);
    assert_eq!(cached.input.binding_digest(), first.input.binding_digest());
    assert_eq!(cached.input.execution_args()["workspace_id"], json!(OWNED));
    fixture.plan("second", object(json!({"query":"x"}))).await;
    let second = fixture
        .bind("second", Some(resolver.clone()))
        .await
        .unwrap();
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 2);
    assert_eq!(second.input.execution_args()["workspace_id"], json!(OTHER));
    assert_eq!(
        second.input.system_inputs()["workspace_id"]
            .resolved
            .as_ref()
            .unwrap()
            .revision,
        id("revision-2")
    );
    assert_ne!(second.input.binding_digest(), first.input.binding_digest());
}

#[tokio::test]
async fn cached_bindings_still_recheck_current_permission_without_resolving_again() {
    let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let resolver = Arc::new(Resolver::new(json!(OWNED)));
    let first = fixture.bind("call", Some(resolver.clone())).await.unwrap();
    fixture.policy.mode.store(4, Ordering::SeqCst);
    assert!(fixture.bind("call", Some(resolver.clone())).await.is_err());
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.policy.executions.lock().unwrap().len(), 2);
    assert_eq!(
        fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .tool_ledger[0]
            .call
            .bound_input_ref
            .as_ref(),
        Some(&first.reference)
    );
}

#[tokio::test]
async fn lookup_denial_or_approval_blocks_resolution_while_execution_approval_saves_a_fixed_candidate()
 {
    for mode in [2, 3, 5] {
        let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
        fixture.plan("call", object(json!({"query":"x"}))).await;
        fixture.policy.mode.store(mode, Ordering::SeqCst);
        let resolver = Arc::new(Resolver::new(json!(OWNED)));
        let result = fixture.bind("call", Some(resolver.clone())).await;
        let saved = fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot;
        if mode == 5 {
            let binding = result.unwrap();
            assert!(matches!(
                binding.decision,
                PolicyDecision::RequireApproval { .. }
            ));
            assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
            assert!(saved.tool_ledger[0].call.bound_input_ref.is_some());
        } else {
            assert!(result.is_err());
            assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
            assert!(saved.tool_ledger[0].call.bound_input_ref.is_none());
        }
    }
}

#[tokio::test]
async fn cancellation_deadline_and_lease_loss_block_new_resolver_calls() {
    for stop in [
        ErrorCode::Cancelled,
        ErrorCode::DeadlineExceeded,
        ErrorCode::LeaseLost,
    ] {
        let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
        fixture.plan("call", object(json!({"query":"x"}))).await;
        let budget = fixture.budget(fixture.store.clone()).await;
        match stop {
            ErrorCode::Cancelled => fixture.context.cancellation.cancel(),
            ErrorCode::DeadlineExceeded => fixture.clock.advance(10_000),
            ErrorCode::LeaseLost => fixture
                .store
                .release_lease(&scope(), &id("run"), &fixture.lease, 0)
                .await
                .unwrap(),
            _ => unreachable!(),
        }
        let resolver = Arc::new(Resolver::new(json!(OWNED)));
        let result = fixture
            .binder(Some(resolver.clone()))
            .bind(&fixture.tool, &id("call"), &fixture.context, &budget)
            .await;
        assert_eq!(result.unwrap_err().code, stop);
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
        assert!(
            fixture
                .store
                .load(&scope(), &id("run"))
                .await
                .unwrap()
                .snapshot
                .tool_ledger[0]
                .call
                .bound_input_ref
                .is_none()
        );
    }
}

#[tokio::test]
async fn a_stop_during_resolution_prevents_binding_persistence_and_final_authorization() {
    for cancel in [true, false] {
        let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
        fixture.plan("call", object(json!({"query":"x"}))).await;
        let resolver = Arc::new(Resolver::new(json!(OWNED)));
        if cancel {
            *resolver.cancel.lock().unwrap() = Some(fixture.context.cancellation.clone());
        } else {
            *resolver.advance.lock().unwrap() = Some(fixture.clock.clone());
        }
        let result = fixture.bind("call", Some(resolver.clone())).await;
        assert!(result.is_err());
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
        assert!(fixture.policy.executions.lock().unwrap().is_empty());
        assert!(
            fixture
                .store
                .load(&scope(), &id("run"))
                .await
                .unwrap()
                .snapshot
                .tool_ledger[0]
                .call
                .bound_input_ref
                .is_none()
        );
    }
}

struct FailingCommitStore {
    inner: Arc<MemoryStateStore>,
    commits: AtomicUsize,
    persist_first: bool,
}
impl StateStore for FailingCommitStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
        request_id: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        self.inner.find_request(scope, session_id, request_id)
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
    fn check_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.check_lease(s, r, l, n)
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
    fn commit<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            self.commits.fetch_add(1, Ordering::SeqCst);
            if self.persist_first {
                self.inner.commit(scope, run_id, input).await?;
            }
            Err(ContractError::new(
                ErrorCode::PersistenceUnavailable,
                "binding.commit",
            ))
        })
    }
}

#[tokio::test]
async fn failed_storage_does_not_return_a_ready_binding_or_change_the_planned_call() {
    let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let original = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    let store = Arc::new(FailingCommitStore {
        inner: fixture.store.clone(),
        commits: AtomicUsize::new(0),
        persist_first: false,
    });
    let resolver = Arc::new(Resolver::new(json!(OWNED)));
    let result = fixture
        .binder(Some(resolver.clone()))
        .bind(
            &fixture.tool,
            &id("call"),
            &fixture.context,
            &fixture.budget(store.clone()).await,
        )
        .await;
    assert_eq!(result.unwrap_err().code, ErrorCode::PersistenceUnavailable);
    assert_eq!(store.commits.load(Ordering::SeqCst), 1);
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot,
        original
    );
}

#[tokio::test]
async fn a_lost_commit_ack_reuses_the_saved_binding_without_resolving_the_new_value() {
    let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let store = Arc::new(FailingCommitStore {
        inner: fixture.store.clone(),
        commits: AtomicUsize::new(0),
        persist_first: true,
    });
    let resolver = Arc::new(Resolver::new(json!(OWNED)));
    let first = fixture
        .binder(Some(resolver.clone()))
        .bind(
            &fixture.tool,
            &id("call"),
            &fixture.context,
            &fixture.budget(store.clone()).await,
        )
        .await;
    assert_eq!(first.unwrap_err().code, ErrorCode::PersistenceUnavailable);
    let saved = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    let call = &saved.tool_ledger[0].call;
    let reference = call
        .bound_input_ref
        .clone()
        .expect("commit applied despite the lost acknowledgement");
    let record = fixture
        .store
        .read_record(&scope(), &reference)
        .await
        .unwrap();
    let committed = BoundToolInput::restore(
        &record,
        &fixture.tool,
        &scope(),
        &id("run"),
        call,
        saved.system_inputs.as_ref(),
    )
    .unwrap();
    assert_eq!(committed.execution_args()["workspace_id"], json!(OWNED));
    assert_eq!(
        committed.system_inputs()["workspace_id"]
            .resolved
            .as_ref()
            .unwrap()
            .revision,
        id("revision-1")
    );

    resolver.set(json!(OTHER), "revision-2");
    let retried = fixture.bind("call", Some(resolver.clone())).await.unwrap();
    assert_eq!(retried.reference, reference);
    assert_eq!(retried.input.binding_digest(), committed.binding_digest());
    assert_eq!(retried.input.execution_args()["workspace_id"], json!(OWNED));
    assert_eq!(
        retried.input.system_inputs()["workspace_id"]
            .resolved
            .as_ref()
            .unwrap()
            .revision,
        id("revision-1")
    );
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.commits.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .revision,
        saved.revision
    );
}

#[tokio::test]
async fn restored_bound_inputs_require_the_original_record_call_scope_and_run() {
    let fixture = Fixture::new(
        descriptor(),
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let bound = fixture.bind("call", None).await.unwrap();
    let record = fixture
        .store
        .read_record(&scope(), &bound.reference)
        .await
        .unwrap();
    let snapshot = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    let call = &snapshot.tool_ledger[0].call;
    let restored = BoundToolInput::restore(
        &record,
        &fixture.tool,
        &scope(),
        &id("run"),
        call,
        snapshot.system_inputs.as_ref(),
    )
    .unwrap();
    assert_eq!(restored.execution_args(), bound.input.execution_args());
    for (record_id, revision) in [
        (id("another-record"), record.reference().revision),
        (
            record.reference().record_id.clone(),
            record.reference().revision + 1,
        ),
    ] {
        let relocated = ProtectedRecord::new(record_id, revision, record.value().clone());
        assert!(
            BoundToolInput::restore(
                &relocated,
                &fixture.tool,
                &scope(),
                &id("run"),
                call,
                snapshot.system_inputs.as_ref()
            )
            .is_err()
        );
    }
    let foreign = Scope {
        tenant_id: id("foreign"),
        ..scope()
    };
    assert!(
        BoundToolInput::restore(
            &record,
            &fixture.tool,
            &foreign,
            &id("run"),
            call,
            snapshot.system_inputs.as_ref()
        )
        .is_err()
    );
    assert!(
        BoundToolInput::restore(
            &record,
            &fixture.tool,
            &scope(),
            &id("another-run"),
            call,
            snapshot.system_inputs.as_ref()
        )
        .is_err()
    );
    let mut changed_call = call.clone();
    changed_call.model_inputs = object(json!({"query":"changed"}));
    assert!(
        BoundToolInput::restore(
            &record,
            &fixture.tool,
            &scope(),
            &id("run"),
            &changed_call,
            snapshot.system_inputs.as_ref()
        )
        .is_err()
    );
    let mut changed_values = record.value().clone();
    let data = changed_values.get_mut("data").unwrap();
    *data
        .get_mut("execution_args")
        .unwrap()
        .get_mut("workspace_id")
        .unwrap() = json!(OTHER);
    *data
        .get_mut("system_inputs")
        .unwrap()
        .get_mut("workspace_id")
        .unwrap()
        .get_mut("resolved")
        .unwrap()
        .get_mut("value")
        .unwrap() = json!(OTHER);
    let tampered = ProtectedRecord::new(
        record.reference().record_id.clone(),
        record.reference().revision,
        changed_values,
    );
    assert!(
        BoundToolInput::restore(
            &tampered,
            &fixture.tool,
            &scope(),
            &id("run"),
            call,
            snapshot.system_inputs.as_ref()
        )
        .is_err()
    );
    let mut changed_reference = snapshot.system_inputs.clone().unwrap();
    changed_reference.values_digest = canonical_digest(&json!({"workspace_id":OTHER}));
    assert!(
        BoundToolInput::restore(
            &record,
            &fixture.tool,
            &scope(),
            &id("run"),
            call,
            Some(&changed_reference)
        )
        .is_err()
    );
}

#[tokio::test]
async fn only_direct_optional_defaults_are_applied_not_conditional_or_required_model_defaults() {
    let mut required_default = descriptor();
    required_default.input_schema["required"] = json!(["query", "limit", "workspace_id"]);
    let fixture = Fixture::new(
        required_default,
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    assert!(fixture.bind("call", None).await.is_err());

    let mut conditional = descriptor();
    conditional.agent_parameters.push("workspace_id".into());
    conditional.input_schema["properties"]["limit"]
        .as_object_mut()
        .unwrap()
        .remove("default");
    conditional.input_schema["if"] = json!({"properties":{"query":{"const":"strict"}}});
    conditional.input_schema["then"] = json!({"properties":{"limit":{"default":99}}});
    let fixture = Fixture::new(conditional, SystemInputRegistry::new(vec![]).unwrap(), None).await;
    let original = object(json!({"query":"strict","workspace_id":OWNED}));
    fixture.plan("call", original.clone()).await;
    let bound = fixture.bind("call", None).await.unwrap();
    assert_eq!(bound.input.normalized_model_inputs(), &original);
}

#[tokio::test]
async fn an_explicit_nullable_model_value_is_not_replaced_by_its_default() {
    let mut tool = descriptor();
    tool.input_schema["properties"]["limit"]["type"] = json!(["integer", "null"]);
    let fixture = Fixture::new(
        tool,
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture
        .plan("call", object(json!({"query":"x","limit":null})))
        .await;
    let bound = fixture.bind("call", None).await.unwrap();
    assert_eq!(bound.input.original_model_inputs()["limit"], Value::Null);
    assert_eq!(bound.input.normalized_model_inputs()["limit"], Value::Null);
    assert_eq!(bound.input.execution_args()["limit"], Value::Null);
}

#[tokio::test]
async fn zero_resolver_capacity_and_small_value_bounds_stop_before_unsafe_progress() {
    let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let resolver = Arc::new(Resolver::new(json!(OWNED)));
    let binder = fixture
        .binder(Some(resolver.clone()))
        .with_limits(InputBindingLimits {
            max_resolver_calls: 0,
            ..InputBindingLimits::default()
        })
        .unwrap();
    let budget = fixture.budget(fixture.store.clone()).await;
    assert!(
        binder
            .bind(&fixture.tool, &id("call"), &fixture.context, &budget)
            .await
            .is_err()
    );
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);

    let fixture = Fixture::new(
        descriptor(),
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let binder = fixture
        .binder(None)
        .with_limits(InputBindingLimits {
            max_value_bytes: 4,
            ..InputBindingLimits::default()
        })
        .unwrap();
    assert!(
        binder
            .bind(
                &fixture.tool,
                &id("call"),
                &fixture.context,
                &fixture.budget(fixture.store.clone()).await
            )
            .await
            .is_err()
    );
    assert!(fixture.policy.executions.lock().unwrap().is_empty());
    assert!(
        fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .tool_ledger[0]
            .call
            .bound_input_ref
            .is_none()
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
    PassThrough,
    Reject,
    LoseAcknowledgement,
    Pause,
    PauseEmptyEventPage,
}
pub struct FinalCommitStore {
    pub inner: Arc<MemoryStateStore>,
    pub mode: FinalCommitMode,
    pub final_entered: Notify,
    pub release: Semaphore,
    pub final_attempts: AtomicUsize,
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
                FinalCommitMode::PauseEmptyEventPage | FinalCommitMode::PassThrough => {
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
```

## `crates/wickle/tests/support/tool_execution.rs`

```rust
//! Stored tool plans and observable executor effects for tool-round contract tests.

use super::support::{admission, event, id, prepared, scope};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use wickle::*;

pub const OWNED: &str = "11111111-1111-4111-8111-111111111111";
pub const FOREIGN: &str = "22222222-2222-4222-8222-222222222222";
pub fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
pub fn input_registry() -> SystemInputRegistry {
    SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap()
}
fn compiled(name: &str, effect: ToolSideEffect, registry: &SystemInputRegistry) -> CompiledTool {
    SchemaCompiler::new().compile(ToolDescriptor{tool:reference(name),name:id(name),description:format!("{name} records"),input_schema:json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"default":10},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),agent_parameters:vec!["query".into(),"limit".into()],system_bindings:None,output_schema:json!({"type":"string"}),side_effect:effect,concurrency:ToolConcurrency::Serial,retry:ToolRetryPolicy::Never,reconcile:false,max_output_bytes:4096.try_into().unwrap()},registry).unwrap()
}
struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id("1")),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("fixture")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| reference.id.clone()),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}
pub struct ClockSource {
    origin: tokio::time::Instant,
}
impl ClockSource {
    fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
        }
    }
}
impl Clock for ClockSource {
    fn now(&self) -> Result<ClockReading, ContractError> {
        let elapsed = self.origin.elapsed().as_millis() as u64;
        Ok(ClockReading {
            utc_ms: elapsed as i64,
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
pub struct Ids(AtomicUsize);
impl IdSource for Ids {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id(&format!(
            "tool-record-{}",
            self.0.fetch_add(1, Ordering::SeqCst)
        )))
    }
}
#[derive(Default)]
pub struct Policy {
    pub denied: Mutex<Option<Id>>,
    pub approval: Mutex<Option<Id>>,
    pub calls: Mutex<Vec<ToolPolicyInput>>,
    pub reject_foreign: AtomicUsize,
    pub revoke_on_check: AtomicUsize,
    pub approve_on_check: AtomicUsize,
    pub store: Option<Arc<MemoryStateStore>>,
    pub before_approval: Mutex<Option<ToolLedgerEntry>>,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::ExecuteTool { input } = &request.action {
                let count = {
                    let mut calls = self.calls.lock().unwrap();
                    calls.push(input.clone());
                    calls.len()
                };
                if self.denied.lock().unwrap().as_ref() == Some(&input.call_id)
                    || (self.reject_foreign.load(Ordering::SeqCst) > 0
                        && input.execution_args().get("workspace_id") != Some(&json!(OWNED)))
                    || (self.revoke_on_check.load(Ordering::SeqCst) > 0
                        && count >= self.revoke_on_check.load(Ordering::SeqCst))
                {
                    return Ok(PolicyDecision::Deny {
                        reason: id("not_owned_or_revoked"),
                    });
                }
                if self.approve_on_check.load(Ordering::SeqCst) > 0
                    && count >= self.approve_on_check.load(Ordering::SeqCst)
                {
                    let saved = self
                        .store
                        .as_ref()
                        .unwrap()
                        .load(&request.owner_scope, &request.resource_id)
                        .await?;
                    *self.before_approval.lock().unwrap() = Some(
                        saved
                            .snapshot
                            .tool_ledger
                            .into_iter()
                            .find(|entry| entry.call.call_id == input.call_id)
                            .unwrap(),
                    );
                    return Ok(PolicyDecision::RequireApproval {
                        reason: id("late_review"),
                    });
                }
                if self.approval.lock().unwrap().as_ref() == Some(&input.call_id) {
                    return Ok(PolicyDecision::RequireApproval {
                        reason: id("review_required"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}
#[derive(Clone, Copy)]
pub enum Action {
    Success,
    WrongOutput,
    Pending,
    Panic,
    Error,
    DeclaredFailure,
    DeclaredUnknown,
    Cancel,
}
#[derive(Debug, Clone, PartialEq)]
pub struct Invocation {
    pub call_id: Id,
    pub attempt_id: Id,
    pub idempotency_key: Id,
    pub args: JsonObject,
}
pub struct Executor {
    pub action: Action,
    pub side_effect: ToolSideEffect,
    pub calls: AtomicUsize,
    pub applied: AtomicUsize,
    pub observed: Mutex<Vec<Invocation>>,
    pub order: Arc<Mutex<Vec<Id>>>,
    pub store: Arc<MemoryStateStore>,
    pub cancel: Mutex<Option<CancellationToken>>,
    pub entered: Notify,
}
impl ToolExecutor for Executor {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.order.lock().unwrap().push(context.call_id.clone());
            self.observed.lock().unwrap().push(Invocation {
                call_id: context.call_id.clone(),
                attempt_id: context.attempt_id.clone(),
                idempotency_key: context.idempotency_key.clone(),
                args: args.clone(),
            });
            let saved = self.store.load(&scope(), &id("run")).await?;
            let entry = saved
                .snapshot
                .tool_ledger
                .iter()
                .find(|entry| entry.call.call_id == context.call_id)
                .unwrap();
            assert!(
                matches!(&entry.state,ToolCallState::Dispatching{attempt_id,idempotency_key} if attempt_id==&context.attempt_id&&idempotency_key==&context.idempotency_key)
            );
            assert!(entry.call.bound_input_ref.is_some());
            assert_eq!(context.scope, scope());
            if self.side_effect != ToolSideEffect::ReadOnly {
                self.applied.fetch_add(1, Ordering::SeqCst);
            }
            self.entered.notify_one();
            let effect = if self.side_effect == ToolSideEffect::ReadOnly {
                ToolEffect::NotApplied
            } else {
                ToolEffect::Applied
            };
            let receipt = (effect == ToolEffect::Applied)
                .then(|| json!({"effect_id":"external-effect","value":args["query"]}));
            match self.action {
                Action::Success => Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Succeeded {
                        value: json!("observed result"),
                    },
                    effect,
                    receipt,
                }),
                Action::WrongOutput => Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Succeeded { value: json!(42) },
                    effect,
                    receipt,
                }),
                Action::DeclaredFailure => Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Failed {
                        code: id("tool_failed"),
                    },
                    effect,
                    receipt,
                }),
                Action::DeclaredUnknown => Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Failed {
                        code: id("unknown_effect"),
                    },
                    effect: ToolEffect::Unknown,
                    receipt,
                }),
                Action::Pending => std::future::pending().await,
                Action::Panic => panic!("synthetic executor panic after entry"),
                Action::Error => Err(ContractError::new(
                    ErrorCode::InvalidContract,
                    "synthetic.transport",
                )),
                Action::Cancel => {
                    self.cancel.lock().unwrap().as_ref().unwrap().cancel();
                    std::future::pending().await
                }
            }
        })
    }
}

pub struct Fixture {
    pub store: Arc<MemoryStateStore>,
    pub policy: Arc<Policy>,
    pub clock: Arc<ClockSource>,
    pub ids: Arc<Ids>,
    pub context: ExecutionContext,
    pub registry: Arc<ToolRegistry>,
    pub input_registry: Arc<SystemInputRegistry>,
    pub compiled: Vec<CompiledTool>,
    pub executors: Vec<Arc<Executor>>,
    pub order: Arc<Mutex<Vec<Id>>>,
    pub lease: RunLease,
}
impl Fixture {
    pub async fn new(tools: &[(&str, ToolSideEffect, Action)], system_value: Option<&str>) -> Self {
        let store = Arc::new(MemoryStateStore::new());
        let input_registry = Arc::new(input_registry());
        let order = Arc::new(Mutex::new(vec![]));
        let mut compiled_tools = vec![];
        let mut executors = vec![];
        let mut registrations = vec![];
        for (name, side_effect, action) in tools {
            let compiled = compiled(name, *side_effect, &input_registry);
            let executor = Arc::new(Executor {
                action: *action,
                side_effect: *side_effect,
                calls: AtomicUsize::new(0),
                applied: AtomicUsize::new(0),
                observed: Mutex::new(vec![]),
                order: order.clone(),
                store: store.clone(),
                cancel: Mutex::new(None),
                entered: Notify::new(),
            });
            registrations.push(ToolRegistration {
                compiled: compiled.clone(),
                executor: executor.clone(),
            });
            compiled_tools.push(compiled);
            executors.push(executor);
        }
        let registry = Arc::new(ToolRegistry::new(scope(), registrations).unwrap());
        let supplied =
            system_value.map(|value| SystemInputs::new(object(json!({"workspace_id":value}))));
        let fixed = RunSystemInputs::capture(scope(), supplied.clone(), &input_registry).unwrap();
        let record = fixed.to_record(id("run-inputs"), 7);
        let input_ref = fixed.snapshot_ref(record.reference()).unwrap();
        let mut input = admission("run", "request", "session", "Read evidence", "1").await;
        let mut profile = input.snapshot.profile.profile().clone();
        profile.tools = compiled_tools
            .iter()
            .map(|tool| {
                ToolBindingRef::Catalog(CatalogToolRef {
                    tool_id: tool.descriptor().tool.id.clone(),
                    version: tool.descriptor().tool.version.clone(),
                    bindings: None,
                    config: None,
                })
            })
            .collect();
        profile.limits.max_tool_attempts = 8;
        input.snapshot.profile = ProfileValidator::new(&Catalog)
            .validate(&profile, &scope())
            .await
            .unwrap();
        input.snapshot.limits = profile.limits.clone();
        input.snapshot.system_inputs = Some(input_ref);
        input.snapshot.request_digest = admission_digest(
            &input.snapshot.request,
            &input.snapshot.profile,
            input.snapshot.system_inputs.as_ref(),
        );
        let RunEventPayload::RunStarted { profile_digest, .. } = &mut input.events[0].payload
        else {
            unreachable!()
        };
        *profile_digest = input.snapshot.profile.profile_digest().clone();
        input.records.push(record);
        store.admit(&scope(), input).await.unwrap();
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("worker"), 0, 20000)
            .await
            .unwrap();
        let context = ExecutionContext::new(
            ExecutionContextData {
                scope: scope(),
                principal_ref: id("caller"),
                capability_grant_ref: id("grant"),
                trace_context: None,
                system_inputs: supplied,
            },
            Default::default(),
        );
        Self {
            policy: Arc::new(Policy {
                store: Some(store.clone()),
                ..Policy::default()
            }),
            store,
            clock: Arc::new(ClockSource::new()),
            ids: Arc::new(Ids::default()),
            context,
            registry,
            input_registry,
            compiled: compiled_tools,
            executors,
            order,
            lease,
        }
    }
    pub async fn plan(&self, calls: &[(&str, &str, JsonObject)]) {
        let saved = self.store.load(&scope(), &id("run")).await.unwrap();
        let mut update = prepared(
            &saved.snapshot,
            self.lease.clone(),
            self.clock.now().unwrap().utc_ms,
        );
        update.snapshot.phase = RunPhase::Tool;
        let mut content = vec![];
        for (call_id, name, args) in calls {
            let descriptor_digest = self
                .compiled
                .iter()
                .find(|tool| tool.descriptor().name.as_str() == *name)
                .map(|tool| tool.descriptor_digest().clone());
            let call = ToolCall {
                call_id: id(call_id),
                model_request_id: id("model-request"),
                provider_call_id: id(&format!("provider-{call_id}")),
                tool_name: id(name),
                model_inputs: args.clone(),
                descriptor_digest,
                bound_input_ref: None,
            };
            let record = ProtectedRecord::new(
                id(&format!("plan-{call_id}")),
                1,
                serde_json::to_value(&call).unwrap(),
            );
            update.snapshot.tool_ledger.push(ToolLedgerEntry {
                call: call.clone(),
                state: ToolCallState::Planned {},
            });
            update.snapshot.last_event_seq += 1;
            update.events.push(event(
                &id("run"),
                &id("session"),
                &scope(),
                update.snapshot.last_event_seq,
                RunEventPayload::ToolPlanned {
                    call_ref: record.reference().clone(),
                },
            ));
            update.records.push(record);
            content.push(ContentBlock::ToolCall { call });
        }
        update.messages.push(Message {
            message_id: id("call-message"),
            run_id: id("run"),
            sequence: (saved.session.transcript_revision + 1).try_into().unwrap(),
            role: MessageRole::Assistant,
            origin: MessageOrigin::Model,
            visibility: Visibility::UserAndModel,
            content,
        });
        self.store
            .commit(&scope(), &id("run"), update)
            .await
            .unwrap();
    }
    pub async fn budget(&self, store: Arc<dyn StateStore>) -> RunBudget {
        RunBudget::attach(
            store,
            self.clock.clone(),
            self.ids.clone(),
            scope(),
            id("run"),
            self.lease.clone(),
            self.context.cancellation.clone(),
        )
        .await
        .unwrap()
    }
    pub fn round(&self) -> SerialToolRound {
        let policy =
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap());
        let binder = Arc::new(InputBinder::new(
            self.input_registry.clone(),
            None,
            policy.clone(),
            self.ids.clone(),
        ));
        SerialToolRound::new(self.registry.clone(), binder, policy, self.ids.clone())
    }
    pub async fn execute(&self) -> Result<ToolRoundOutcome, ContractError> {
        self.round()
            .execute(
                &id("model-request"),
                &self.context,
                &self.budget(self.store.clone()).await,
            )
            .await
    }
    pub async fn saved(&self) -> StoredRun {
        self.store.load(&scope(), &id("run")).await.unwrap()
    }
}

#[derive(Clone, Copy)]
pub enum FailStage {
    Binding,
    Reservation,
    Dispatch,
    Result,
}
pub struct FaultStore {
    pub inner: Arc<MemoryStateStore>,
    pub stage: FailStage,
    pub lose_ack: bool,
    pub failures: AtomicUsize,
}
impl StateStore for FaultStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        s: &'a Scope,
        session: &'a Id,
        request: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        self.inner.find_request(s, session, request)
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
        after: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage> {
        self.inner.read_events(s, r, after, limit)
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
            let previous = self.inner.load(s, r).await?.snapshot;
            let should_fail = match self.stage {
                FailStage::Binding => {
                    previous.tool_ledger[0].call.bound_input_ref.is_none()
                        && input.snapshot.tool_ledger[0].call.bound_input_ref.is_some()
                }
                FailStage::Reservation => {
                    input.snapshot.usage.tool_attempts > previous.usage.tool_attempts
                }
                FailStage::Dispatch => {
                    matches!(
                        input.snapshot.tool_ledger[0].state,
                        ToolCallState::Dispatching { .. }
                    ) && !matches!(
                        previous.tool_ledger[0].state,
                        ToolCallState::Dispatching { .. }
                    )
                }
                FailStage::Result => {
                    matches!(
                        input.snapshot.tool_ledger[0].state,
                        ToolCallState::Settled { .. } | ToolCallState::Unknown { .. }
                    ) && matches!(
                        previous.tool_ledger[0].state,
                        ToolCallState::Dispatching { .. }
                    )
                }
            };
            if should_fail {
                self.failures.fetch_add(1, Ordering::SeqCst);
                if self.lose_ack {
                    self.inner.commit(s, r, input).await?;
                }
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "tool.transaction",
                ));
            }
            self.inner.commit(s, r, input).await
        })
    }
}
```

## `crates/wickle/tests/tool_execution.rs`

```rust
//! Serial tool execution validates and persists each boundary before external effects.

#[allow(dead_code)]
mod support;
#[path = "support/tool_execution.rs"]
#[allow(dead_code)]
mod tool_support;
use serde_json::json;
use std::sync::atomic::Ordering;
use support::{id, scope};
use tool_support::*;
use wickle::*;

#[tokio::test]
async fn stored_calls_execute_in_order_using_only_the_frozen_handler_arguments() {
    let fixture = Fixture::new(
        &[
            ("read", ToolSideEffect::ReadOnly, Action::Success),
            ("write", ToolSideEffect::Write, Action::Success),
        ],
        Some(OWNED),
    )
    .await;
    fixture
        .plan(&[
            ("first", "read", object(json!({"query":"first"}))),
            (
                "second",
                "write",
                object(json!({"query":"second","limit":2})),
            ),
        ])
        .await;
    assert!(matches!(
        fixture.execute().await.unwrap(),
        ToolRoundOutcome::Completed
    ));
    assert_eq!(
        *fixture.order.lock().unwrap(),
        vec![id("first"), id("second")]
    );
    assert_eq!(
        fixture.executors[0].observed.lock().unwrap()[0].args,
        object(json!({"query":"first","limit":10,"workspace_id":OWNED}))
    );
    assert_eq!(
        fixture.executors[1].observed.lock().unwrap()[0].args,
        object(json!({"query":"second","limit":2,"workspace_id":OWNED}))
    );
    let saved = fixture.saved().await;
    assert_eq!(saved.snapshot.usage.tool_attempts, 2);
    assert_eq!(
        saved.snapshot.tool_ledger[0].call.model_inputs,
        object(json!({"query":"first"}))
    );
    let results: Vec<_> = saved
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            ContentBlock::ToolResult { result } => Some(result),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].call_id, id("first"));
    assert_eq!(results[1].call_id, id("second"));
    for result in results {
        assert_eq!(result.call_message_id, id("call-message"));
        assert_eq!(result.status, ToolResultStatus::Succeeded);
    }
    assert!(matches!(
        fixture.execute().await.unwrap(),
        ToolRoundOutcome::Completed
    ));
    assert_eq!(fixture.executors[0].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.executors[1].calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalid_model_arguments_and_unknown_tools_settle_without_executor_dispatch() {
    let fixture = Fixture::new(
        &[("read", ToolSideEffect::ReadOnly, Action::Success)],
        Some(OWNED),
    )
    .await;
    fixture
        .plan(&[
            (
                "hidden",
                "read",
                object(json!({"query":"x","workspace_id":OWNED})),
            ),
            ("unknown", "unregistered", object(json!({"query":"x"}))),
            ("valid", "read", object(json!({"query":"good"}))),
        ])
        .await;
    assert!(matches!(
        fixture.execute().await.unwrap(),
        ToolRoundOutcome::Completed
    ));
    assert_eq!(fixture.executors[0].calls.load(Ordering::SeqCst), 1);
    assert_eq!(*fixture.order.lock().unwrap(), vec![id("valid")]);
    let saved = fixture.saved().await;
    assert_eq!(saved.snapshot.usage.tool_attempts, 1);
    assert!(
        saved.snapshot.tool_ledger[1]
            .call
            .descriptor_digest
            .is_none()
    );
    for entry in &saved.snapshot.tool_ledger[..2] {
        let ToolCallState::Settled { result } = &entry.state else {
            panic!("rejected call remains unsettled")
        };
        assert_ne!(result.status, ToolResultStatus::Succeeded);
        assert_eq!(result.effect, ToolEffect::NotApplied);
        assert!(entry.call.bound_input_ref.is_none());
    }
}

#[tokio::test]
async fn missing_system_inputs_and_valid_foreign_targets_never_reach_the_executor() {
    for value in [None, Some(FOREIGN)] {
        let fixture =
            Fixture::new(&[("write", ToolSideEffect::Write, Action::Success)], value).await;
        fixture.policy.reject_foreign.store(1, Ordering::SeqCst);
        fixture
            .plan(&[("call", "write", object(json!({"query":"x"})))])
            .await;
        let _outcome = fixture.execute().await;
        assert_eq!(fixture.executors[0].calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.executors[0].applied.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.saved().await.snapshot.usage.tool_attempts, 0);
    }
}

#[tokio::test]
async fn policy_is_checked_again_after_binding_and_can_revoke_the_final_call() {
    for check in [2, 3] {
        let fixture = Fixture::new(
            &[("write", ToolSideEffect::Write, Action::Success)],
            Some(OWNED),
        )
        .await;
        fixture
            .plan(&[("call", "write", object(json!({"query":"x"})))])
            .await;
        fixture
            .policy
            .revoke_on_check
            .store(check, Ordering::SeqCst);
        assert!(matches!(
            fixture.execute().await.unwrap(),
            ToolRoundOutcome::Completed
        ));
        assert_eq!(fixture.policy.calls.lock().unwrap().len(), check);
        assert_eq!(fixture.executors[0].calls.load(Ordering::SeqCst), 0);
        let saved = fixture.saved().await;
        assert!(saved.snapshot.tool_ledger[0].call.bound_input_ref.is_some());
        assert_eq!(saved.snapshot.usage.tool_attempts, u64::from(check == 3));
        let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[0].state else {
            panic!("denied call must settle without an effect")
        };
        assert_eq!(result.status, ToolResultStatus::Denied);
        assert_eq!(result.effect, ToolEffect::NotApplied);
    }
}

#[tokio::test]
async fn applied_write_with_invalid_output_keeps_its_receipt_and_is_never_reexecuted() {
    let fixture = Fixture::new(
        &[("write", ToolSideEffect::Write, Action::WrongOutput)],
        Some(OWNED),
    )
    .await;
    fixture
        .plan(&[("call", "write", object(json!({"query":"x"})))])
        .await;
    assert!(matches!(
        fixture.execute().await.unwrap(),
        ToolRoundOutcome::Completed
    ));
    let saved = fixture.saved().await;
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[0].state else {
        panic!("result not settled")
    };
    assert_eq!(result.status, ToolResultStatus::Failed);
    assert_eq!(result.effect, ToolEffect::Applied);
    let receipt = result
        .effect_receipt_ref
        .as_ref()
        .expect("applied effect receipt is retained");
    let record = fixture.store.read_record(&scope(), receipt).await.unwrap();
    assert_eq!(
        &record.value()["receipt"],
        &json!({"effect_id":"external-effect","value":"x"})
    );
    assert!(matches!(
        fixture.execute().await.unwrap(),
        ToolRoundOutcome::Completed
    ));
    assert_eq!(fixture.executors[0].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.executors[0].applied.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn an_approval_candidate_is_persisted_and_stops_later_calls_without_dispatch() {
    let fixture = Fixture::new(
        &[
            ("write", ToolSideEffect::Write, Action::Success),
            ("read", ToolSideEffect::ReadOnly, Action::Success),
        ],
        Some(OWNED),
    )
    .await;
    fixture
        .plan(&[
            ("review", "write", object(json!({"query":"x"}))),
            ("later", "read", object(json!({"query":"y"}))),
        ])
        .await;
    *fixture.policy.approval.lock().unwrap() = Some(id("review"));
    let outcome = fixture.execute().await.unwrap();
    assert!(
        matches!(outcome,ToolRoundOutcome::ApprovalRequired{call_id,..} if call_id==id("review"))
    );
    let saved = fixture.saved().await;
    assert!(saved.snapshot.tool_ledger[0].call.bound_input_ref.is_some());
    assert!(saved.snapshot.tool_ledger[1].call.bound_input_ref.is_none());
    assert_eq!(saved.snapshot.usage.tool_attempts, 0);
    assert!(fixture.order.lock().unwrap().is_empty());
}

#[tokio::test]
async fn late_approval_preserves_the_reserved_attempt_and_stops_before_executor_entry() {
    let fixture = Fixture::new(
        &[
            ("write", ToolSideEffect::Write, Action::Success),
            ("read", ToolSideEffect::ReadOnly, Action::Success),
        ],
        Some(OWNED),
    )
    .await;
    fixture
        .plan(&[
            ("review", "write", object(json!({"query":"x"}))),
            ("later", "read", object(json!({"query":"y"}))),
        ])
        .await;
    // Binding and pre-reservation authorization allow; only the stored dispatch gate changes.
    fixture.policy.approve_on_check.store(3, Ordering::SeqCst);
    let ToolRoundOutcome::ApprovalRequired {
        call_id,
        bound_input_ref,
        binding_digest,
        ..
    } = fixture.execute().await.unwrap()
    else {
        panic!("late approval must pause the round")
    };
    assert_eq!(fixture.policy.calls.lock().unwrap().len(), 3);
    let before = fixture
        .policy
        .before_approval
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    let ToolCallState::Dispatching {
        attempt_id,
        idempotency_key,
    } = before.state
    else {
        panic!("the approval policy must run after the dispatch reservation is stored")
    };
    let saved = fixture.saved().await;
    let entry = &saved.snapshot.tool_ledger[0];
    assert_eq!(entry.call, before.call);
    assert_eq!(
        entry.state,
        ToolCallState::ApprovalPending {
            attempt_id: attempt_id.clone(),
            idempotency_key,
        }
    );
    assert_eq!(call_id, entry.call.call_id);
    assert_eq!(entry.call.bound_input_ref.as_ref(), Some(&bound_input_ref));
    let record = fixture
        .store
        .read_record(&scope(), &bound_input_ref)
        .await
        .unwrap();
    let bound = BoundToolInput::restore(
        &record,
        &fixture.compiled[0],
        &scope(),
        &id("run"),
        &entry.call,
        saved.snapshot.system_inputs.as_ref(),
    )
    .unwrap();
    assert_eq!(bound.binding_digest(), &binding_digest);
    assert_eq!(bound.original_model_inputs(), &object(json!({"query":"x"})));
    assert_eq!(
        bound.execution_args(),
        &object(json!({"query":"x","limit":10,"workspace_id":OWNED}))
    );
    assert_eq!(saved.snapshot.usage.tool_attempts, 1);
    let reservations: Vec<_> = saved
        .snapshot
        .reservations
        .iter()
        .filter(|reservation| matches!(reservation.kind, ReservationKind::Tool { .. }))
        .collect();
    assert_eq!(reservations.len(), 1);
    assert_eq!(reservations[0].attempt_id, attempt_id);
    assert_eq!(reservations[0].kind, ReservationKind::Tool { call_id });
    assert!(matches!(
        saved.snapshot.tool_ledger[1].state,
        ToolCallState::Planned {}
    ));
    assert!(saved.snapshot.tool_ledger[1].call.bound_input_ref.is_none());
    assert!(fixture.order.lock().unwrap().is_empty());
    assert!(
        fixture
            .executors
            .iter()
            .all(|executor| executor.calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test(start_paused = true)]
async fn a_write_timeout_preserves_unknown_attempt_identity_and_stops_following_tools() {
    let fixture = Fixture::new(
        &[
            ("write", ToolSideEffect::Write, Action::Pending),
            ("read", ToolSideEffect::ReadOnly, Action::Success),
        ],
        Some(OWNED),
    )
    .await;
    fixture
        .plan(&[
            ("uncertain", "write", object(json!({"query":"x"}))),
            ("later", "read", object(json!({"query":"y"}))),
        ])
        .await;
    let round = fixture
        .round()
        .with_limits(ToolExecutionLimits {
            timeout_ms: 20,
            max_receipt_bytes: 4096,
        })
        .unwrap();
    let outcome = round
        .execute(
            &id("model-request"),
            &fixture.context,
            &fixture.budget(fixture.store.clone()).await,
        )
        .await
        .unwrap();
    assert!(matches!(outcome,ToolRoundOutcome::Unresolved{call_id,..} if call_id==id("uncertain")));
    let saved = fixture.saved().await;
    let ToolCallState::Unknown {
        attempt_id,
        idempotency_key,
    } = &saved.snapshot.tool_ledger[0].state
    else {
        panic!("lost write must remain unknown")
    };
    let invocation = fixture.executors[0].observed.lock().unwrap()[0].clone();
    assert_eq!(attempt_id, &invocation.attempt_id);
    assert_eq!(idempotency_key, &invocation.idempotency_key);
    assert_eq!(fixture.executors[0].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.executors[1].calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        fixture.execute().await.unwrap(),
        ToolRoundOutcome::Unresolved { .. }
    ));
    assert_eq!(fixture.executors[0].calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn write_panic_transport_error_or_explicit_unknown_never_claims_no_effect() {
    for action in [Action::Panic, Action::Error, Action::DeclaredUnknown] {
        let fixture = Fixture::new(
            &[
                ("write", ToolSideEffect::Write, action),
                ("read", ToolSideEffect::ReadOnly, Action::Success),
            ],
            Some(OWNED),
        )
        .await;
        fixture
            .plan(&[
                ("uncertain", "write", object(json!({"query":"x"}))),
                ("later", "read", object(json!({"query":"y"}))),
            ])
            .await;
        assert!(matches!(
            fixture.execute().await.unwrap(),
            ToolRoundOutcome::Unresolved { .. }
        ));
        let saved = fixture.saved().await;
        assert!(matches!(
            saved.snapshot.tool_ledger[0].state,
            ToolCallState::Unknown { .. }
        ));
        let result = saved
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .find_map(|content| match content {
                ContentBlock::ToolResult { result } if result.call_id == id("uncertain") => {
                    Some(result)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(result.effect, ToolEffect::Unknown);
        assert_eq!(fixture.executors[0].applied.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.executors[1].calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn checkpoint_restore_rejects_a_changed_unknown_effect_key() {
    let fixture = Fixture::new(
        &[("write", ToolSideEffect::Write, Action::DeclaredUnknown)],
        Some(OWNED),
    )
    .await;
    fixture
        .plan(&[("uncertain", "write", object(json!({"query":"x"})))])
        .await;
    fixture.execute().await.unwrap();
    let checkpoint = fixture.store.export_checkpoint(&scope()).unwrap();
    let original = serde_json::to_value(&checkpoint).unwrap();
    let restored =
        StateStoreCheckpoint::from_json(&original.to_string(), &scope(), &checkpoint.digest())
            .unwrap();
    let restored = MemoryStateStore::from_checkpoint(restored);
    assert_eq!(
        restored.load(&scope(), &id("run")).await.unwrap(),
        fixture.saved().await
    );

    let mut changed = original;
    let event = changed["runs"][0]["events"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|event| event["payload"]["type"] == "tool.unresolved")
        .unwrap();
    event["payload"]["idempotency_key"] = json!("different-effect-key");
    // Recomputing the container checksum must not bypass cross-record validation.
    let error = StateStoreCheckpoint::from_json(
        &changed.to_string(),
        &scope(),
        &canonical_digest(&changed),
    )
    .unwrap_err();
    assert_eq!(error.path, "checkpoint.tool_unresolved_key");
}

#[tokio::test]
async fn read_only_transport_failure_has_no_effect_and_does_not_block_the_next_read() {
    let fixture = Fixture::new(
        &[
            ("read", ToolSideEffect::ReadOnly, Action::Error),
            ("next", ToolSideEffect::ReadOnly, Action::Success),
        ],
        Some(OWNED),
    )
    .await;
    fixture
        .plan(&[
            ("failed", "read", object(json!({"query":"x"}))),
            ("next", "next", object(json!({"query":"y"}))),
        ])
        .await;
    assert!(matches!(
        fixture.execute().await.unwrap(),
        ToolRoundOutcome::Completed
    ));
    let saved = fixture.saved().await;
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[0].state else {
        panic!("read failure should settle")
    };
    assert_eq!(result.effect, ToolEffect::NotApplied);
    assert_eq!(fixture.executors[1].calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_binding_reservation_or_dispatch_persistence_prevents_executor_entry() {
    for stage in [
        FailStage::Binding,
        FailStage::Reservation,
        FailStage::Dispatch,
    ] {
        let fixture = Fixture::new(
            &[("write", ToolSideEffect::Write, Action::Success)],
            Some(OWNED),
        )
        .await;
        fixture
            .plan(&[("call", "write", object(json!({"query":"x"})))])
            .await;
        let store = std::sync::Arc::new(FaultStore {
            inner: fixture.store.clone(),
            stage,
            lose_ack: false,
            failures: std::sync::atomic::AtomicUsize::new(0),
        });
        let result = fixture
            .round()
            .execute(
                &id("model-request"),
                &fixture.context,
                &fixture.budget(store.clone()).await,
            )
            .await;
        assert_eq!(result.unwrap_err().code, ErrorCode::PersistenceUnavailable);
        assert_eq!(store.failures.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.executors[0].calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.executors[0].applied.load(Ordering::SeqCst), 0);
        let saved = fixture.saved().await;
        assert!(matches!(
            saved.snapshot.tool_ledger[0].state,
            ToolCallState::Planned {}
        ));
    }
}

#[tokio::test]
async fn failed_result_storage_keeps_dispatch_uncertainty_and_blocks_later_effects() {
    let fixture = Fixture::new(
        &[
            ("write", ToolSideEffect::Write, Action::Success),
            ("read", ToolSideEffect::ReadOnly, Action::Success),
        ],
        Some(OWNED),
    )
    .await;
    fixture
        .plan(&[
            ("written", "write", object(json!({"query":"x"}))),
            ("later", "read", object(json!({"query":"y"}))),
        ])
        .await;
    let store = std::sync::Arc::new(FaultStore {
        inner: fixture.store.clone(),
        stage: FailStage::Result,
        lose_ack: false,
        failures: std::sync::atomic::AtomicUsize::new(0),
    });
    let result = fixture
        .round()
        .execute(
            &id("model-request"),
            &fixture.context,
            &fixture.budget(store.clone()).await,
        )
        .await;
    assert_eq!(result.unwrap_err().code, ErrorCode::PersistenceUnavailable);
    assert_eq!(fixture.executors[0].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.executors[1].calls.load(Ordering::SeqCst), 0);
    let saved = fixture.saved().await;
    assert!(matches!(
        saved.snapshot.tool_ledger[0].state,
        ToolCallState::Dispatching { .. }
    ));
    assert!(
        !fixture
            .store
            .read_events(&scope(), &id("run"), 0, 100)
            .await
            .unwrap()
            .events
            .iter()
            .any(|event| matches!(event.payload, RunEventPayload::ToolSettled { .. }))
    );
}

#[tokio::test]
async fn result_commit_acknowledgement_loss_does_not_repeat_an_applied_write() {
    let fixture = Fixture::new(
        &[("write", ToolSideEffect::Write, Action::Success)],
        Some(OWNED),
    )
    .await;
    fixture
        .plan(&[("call", "write", object(json!({"query":"x"})))])
        .await;
    let store = std::sync::Arc::new(FaultStore {
        inner: fixture.store.clone(),
        stage: FailStage::Result,
        lose_ack: true,
        failures: std::sync::atomic::AtomicUsize::new(0),
    });
    let _first = fixture
        .round()
        .execute(
            &id("model-request"),
            &fixture.context,
            &fixture.budget(store.clone()).await,
        )
        .await;
    assert_eq!(store.failures.load(Ordering::SeqCst), 1);
    let saved = fixture.saved().await;
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[0].state else {
        panic!("committed result must survive its lost acknowledgement")
    };
    assert_eq!(result.effect, ToolEffect::Applied);
    assert!(matches!(
        fixture.execute().await.unwrap(),
        ToolRoundOutcome::Completed
    ));
    assert_eq!(fixture.executors[0].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.executors[0].applied.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn scope_or_lease_mismatch_prevents_all_executor_calls() {
    let fixture = Fixture::new(
        &[("write", ToolSideEffect::Write, Action::Success)],
        Some(OWNED),
    )
    .await;
    fixture
        .plan(&[("call", "write", object(json!({"query":"x"})))])
        .await;
    let budget = fixture.budget(fixture.store.clone()).await;
    let mut foreign = fixture.context.clone();
    foreign.data.scope.tenant_id = id("foreign");
    assert!(
        fixture
            .round()
            .execute(&id("model-request"), &foreign, &budget)
            .await
            .is_err()
    );
    fixture
        .store
        .release_lease(
            &scope(),
            &id("run"),
            &fixture.lease,
            fixture.clock.now().unwrap().utc_ms,
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .round()
            .execute(&id("model-request"), &fixture.context, &budget)
            .await
            .unwrap_err()
            .code,
        ErrorCode::LeaseLost
    );
    assert_eq!(fixture.executors[0].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.saved().await.snapshot.usage.tool_attempts, 0);
}

#[tokio::test]
async fn cancellation_after_executor_entry_retains_an_unknown_write_and_stops_the_round() {
    let fixture = Fixture::new(
        &[
            ("write", ToolSideEffect::Write, Action::Cancel),
            ("read", ToolSideEffect::ReadOnly, Action::Success),
        ],
        Some(OWNED),
    )
    .await;
    *fixture.executors[0].cancel.lock().unwrap() = Some(fixture.context.cancellation.clone());
    fixture
        .plan(&[
            ("uncertain", "write", object(json!({"query":"x"}))),
            ("later", "read", object(json!({"query":"y"}))),
        ])
        .await;
    assert!(
        matches!(fixture.execute().await.unwrap(),ToolRoundOutcome::Unresolved{call_id,..} if call_id==id("uncertain"))
    );
    let saved = fixture.saved().await;
    assert!(matches!(
        saved.snapshot.tool_ledger[0].state,
        ToolCallState::Unknown { .. }
    ));
    assert_eq!(fixture.executors[0].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.executors[1].calls.load(Ordering::SeqCst), 0);
    let result = saved
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|content| match content {
            ContentBlock::ToolResult { result } if result.call_id == id("uncertain") => {
                Some(result)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(result.effect, ToolEffect::Unknown);
    assert_eq!(saved.snapshot.usage.tool_attempts, 1);
}

#[tokio::test]
async fn cancellation_before_the_round_starts_never_reserves_or_enters_an_executor() {
    let fixture = Fixture::new(
        &[("write", ToolSideEffect::Write, Action::Success)],
        Some(OWNED),
    )
    .await;
    fixture
        .plan(&[("call", "write", object(json!({"query":"x"})))])
        .await;
    fixture.context.cancellation.cancel();
    assert_eq!(
        fixture.execute().await.unwrap_err().code,
        ErrorCode::Cancelled
    );
    assert_eq!(fixture.executors[0].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.saved().await.snapshot.usage.tool_attempts, 0);
}

#[tokio::test]
async fn registry_requires_unique_names_and_the_exact_selected_tool_version() {
    let fixture = Fixture::new(
        &[("read", ToolSideEffect::ReadOnly, Action::Success)],
        Some(OWNED),
    )
    .await;
    let entry = ToolRegistration {
        compiled: fixture.compiled[0].clone(),
        executor: fixture.executors[0].clone(),
    };
    assert!(ToolRegistry::new(scope(), vec![entry.clone(), entry]).is_err());
    let saved = fixture.saved().await;
    let mut selected = saved.snapshot.profile.profile().clone();
    let ToolBindingRef::Catalog(reference) = &mut selected.tools[0] else {
        unreachable!()
    };
    reference.version = id("not-installed");
    assert!(fixture.registry.prompt_bindings(&selected).is_err());
    assert_eq!(fixture.executors[0].calls.load(Ordering::SeqCst), 0);
    let ordinary = fixture
        .registry
        .prompt_bindings(saved.snapshot.profile.profile())
        .unwrap();
    assert_eq!(ordinary.len(), 1);
    assert_eq!(ordinary[0].compiled.digest(), fixture.compiled[0].digest());
}

#[tokio::test]
async fn a_saved_failed_result_with_unknown_effect_still_blocks_following_tools() {
    let fixture = Fixture::new(
        &[
            ("write", ToolSideEffect::Write, Action::Success),
            ("read", ToolSideEffect::ReadOnly, Action::Success),
        ],
        Some(OWNED),
    )
    .await;
    fixture
        .plan(&[
            ("historical", "write", object(json!({"query":"x"}))),
            ("later", "read", object(json!({"query":"y"}))),
        ])
        .await;
    let saved = fixture.saved().await;
    // The stored effect field remains authoritative even in a checkpoint whose
    // failed status does not use the newer dedicated Unknown classification.
    let result = ToolResult {
        call_id: id("historical"),
        call_message_id: id("call-message"),
        status: ToolResultStatus::Failed,
        effect: ToolEffect::Unknown,
        content: vec![],
        effect_receipt_ref: None,
        error: Some(Failure {
            code: id("historical_unknown"),
            diagnostic_ref: None,
        }),
    };
    let record = ProtectedRecord::new(
        id("historical-result"),
        1,
        serde_json::to_value(&result).unwrap(),
    );
    let mut update = support::prepared(
        &saved.snapshot,
        fixture.lease.clone(),
        fixture.clock.now().unwrap().utc_ms,
    );
    update.snapshot.tool_ledger[0].state = ToolCallState::Settled {
        result: result.clone(),
    };
    update.snapshot.last_event_seq += 1;
    update.events.push(support::event(
        &id("run"),
        &id("session"),
        &scope(),
        update.snapshot.last_event_seq,
        RunEventPayload::ToolSettled {
            result_ref: record.reference().clone(),
        },
    ));
    update.messages.push(Message {
        message_id: id("historical-result-message"),
        run_id: id("run"),
        sequence: (saved.session.transcript_revision + 1).try_into().unwrap(),
        role: MessageRole::Tool,
        origin: MessageOrigin::Tool,
        visibility: Visibility::UserAndModel,
        content: vec![ContentBlock::ToolResult { result }],
    });
    update.records.push(record);
    fixture
        .store
        .commit(&scope(), &id("run"), update)
        .await
        .unwrap();
    assert!(!matches!(
        fixture.execute().await,
        Ok(ToolRoundOutcome::Completed)
    ));
    assert_eq!(fixture.executors[0].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.executors[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.saved().await.snapshot.usage.tool_attempts, 0);
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
            system_input_resolver: None,
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
    let replay = completed(agent.start(request, context.clone()).await?)?;
    assert_eq!(replay.run_id(), &run_id);
    assert_eq!(completed(replay.outcome(&context).await?)?, outcome);
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

## `tests/support/input_binding_consumer.rs`

```rust
use serde_json::json;
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;

const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";
const REPORT_A: &str = "22222222-2222-4222-8222-222222222222";
const REPORT_B: &str = "33333333-3333-4333-8333-333333333333";
fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
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
                    version: r.version.clone().or_else(|| Some(id("1"))),
                    ..r.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!(r.id)),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (r.kind == ComponentKind::Tool).then(|| r.id.clone()),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}
struct OwnedTargets;
impl PolicyPort for OwnedTargets {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::ExecuteTool { input } = &request.action {
                let args = input.execution_args();
                let owned = match input.tool.id.as_str() {
                    "search" => {
                        args.get("workspace_id").and_then(|v| v.as_str()) == Some(WORKSPACE)
                    }
                    "read_report" => matches!(
                        args.get("report_id").and_then(|v| v.as_str()),
                        Some(REPORT_A | REPORT_B)
                    ),
                    _ => false,
                };
                if !owned {
                    return Ok(PolicyDecision::Deny {
                        reason: id("target_not_owned"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}
struct CurrentReport {
    value: Mutex<ResolvedSystemInput>,
    calls: AtomicUsize,
}
impl SystemInputResolver for CurrentReport {
    fn resolve<'a>(
        &'a self,
        request: &'a SystemInputResolveRequest,
        _: &'a SystemInputResolveContext,
    ) -> PortFuture<'a, Option<ResolvedSystemInput>> {
        assert_eq!(request.key, id("current_report_id"));
        self.calls.fetch_add(1, Ordering::SeqCst);
        let value = self.value.lock().unwrap().clone();
        Box::pin(async move { Ok(Some(value)) })
    }
}
fn tool(
    name: &str,
    input_schema: serde_json::Value,
    agent_parameters: Vec<String>,
) -> ToolDescriptor {
    ToolDescriptor {
        tool: reference(name),
        name: id(name),
        description: "Read authorized data".into(),
        input_schema,
        agent_parameters,
        system_bindings: None,
        output_schema: json!({"type":"string"}),
        side_effect: ToolSideEffect::ReadOnly,
        concurrency: ToolConcurrency::Serial,
        retry: ToolRetryPolicy::Never,
        reconcile: false,
        max_output_bytes: 4096.try_into().unwrap(),
    }
}
async fn plan(
    store: &MemoryStateStore,
    scope: &Scope,
    run: &Id,
    lease: &RunLease,
    clock: &SystemClock,
    call_id: &str,
    compiled: &CompiledTool,
    model_inputs: JsonObject,
) -> Result<(), ContractError> {
    let saved = store.load(scope, run).await?;
    let mut snapshot = saved.snapshot;
    let expected_revision = snapshot.revision;
    snapshot.revision += 1;
    snapshot.phase = RunPhase::Tool;
    let call = ToolCall {
        call_id: id(call_id),
        model_request_id: id("model-request"),
        provider_call_id: id(&format!("provider-{call_id}")),
        tool_name: compiled.descriptor().name.clone(),
        model_inputs,
        descriptor_digest: Some(compiled.descriptor_digest().clone()),
        bound_input_ref: None,
    };
    snapshot.tool_ledger.push(ToolLedgerEntry {
        call,
        state: ToolCallState::Planned {},
    });
    store
        .commit(
            scope,
            run,
            CommitInput {
                expected_revision,
                lease: lease.clone(),
                now_ms: clock.now()?.utc_ms,
                snapshot,
                messages: vec![],
                events: vec![],
                records: vec![],
            },
        )
        .await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let registry = Arc::new(SystemInputRegistry::new(vec![
        SystemInputDefinition {
            key: id("workspace_id"),
            version: id("1"),
            value_schema: json!({"type":"string","format":"uuid"}),
            source: SystemInputSource::Run {},
        },
        SystemInputDefinition {
            key: id("unused_key"),
            version: id("1"),
            value_schema: json!({"type":"string"}),
            source: SystemInputSource::Run {},
        },
        SystemInputDefinition {
            key: id("current_report_id"),
            version: id("1"),
            value_schema: json!({"type":"string","format":"uuid"}),
            source: SystemInputSource::Resolver {
                resolver_ref: reference("current-report"),
            },
        },
    ])?);
    let search = SchemaCompiler::new().compile(tool("search", json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","default":10},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}), vec!["query".into(),"limit".into()]), &registry)?;
    let mut report = tool(
        "read_report",
        json!({"type":"object","properties":{"report_id":{"type":"string","format":"uuid"}},"required":["report_id"],"additionalProperties":false}),
        vec![],
    );
    report.system_bindings = Some(std::collections::BTreeMap::from([(
        "report_id".into(),
        id("current_report_id"),
    )]));
    let report = SchemaCompiler::new().compile(report, &registry)?;
    let values = SystemInputs::new(JsonObject::from([
        ("workspace_id".into(), json!(WORKSPACE)),
        ("unused_key".into(), json!("not a tool argument")),
    ]));
    let captured = RunSystemInputs::capture(scope.clone(), Some(values.clone()), &registry)?;
    let input_record = captured.to_record(id("run-inputs"), 1);
    let input_ref = captured.snapshot_ref(input_record.reference())?;
    let profile = AgentProfile::from_json(
        r#"{
      "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1",
      "name":"Assistant","description":"Binding example","instructions":{"text":"Use available evidence"},"model_binding":"primary",
      "tools":[{"tool_id":"search","version":"1"},{"tool_id":"read_report","version":"1"}],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
      "limits":{"max_model_calls":4,"max_tool_attempts":4,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}
    }"#,
    )?;
    let profile = ProfileValidator::new(&Catalog)
        .validate(&profile, &scope)
        .await?;
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Read recent results".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    };
    let request_record =
        ProtectedRecord::new(id("request-data"), 1, serde_json::to_value(&request)?);
    let prompt = ProtectedRecord::new(
        id("prompt"),
        1,
        json!({"instructions":"Use available evidence"}),
    );
    let clock = Arc::new(SystemClock::new());
    let now = clock.now()?.utc_ms;
    let run = id("run");
    let snapshot = RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: run.clone(),
        request_digest: admission_digest(&request, &profile, Some(&input_ref)),
        request: request.clone(),
        scope: scope.clone(),
        limits: profile.profile().limits.clone(),
        profile,
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        usage: BudgetUsage::default(),
        timing: RunTiming::new(now, 30000)?,
        reservations: vec![],
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: Some(input_ref.clone()),
        wait: None,
        outcome: None,
        assembly_ref: None,
        routing_snapshot_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    let started = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("started"),
        scope: scope.clone(),
        run_id: run.clone(),
        session_id: request.session_id.clone(),
        seq: 1.try_into()?,
        timestamp_ms: now,
        payload: RunEventPayload::RunStarted {
            request_ref: request_record.reference().clone(),
            profile_digest: snapshot.profile.profile_digest().clone(),
        },
    };
    let store = Arc::new(MemoryStateStore::new());
    store
        .admit(
            &scope,
            AdmissionInput {
                snapshot,
                prompt_snapshot: prompt.reference().clone(),
                messages: vec![Message {
                    message_id: id("user-message"),
                    run_id: run.clone(),
                    sequence: 1.try_into()?,
                    role: MessageRole::User,
                    content: vec![ContentBlock::Content {
                        content: request.input[0].clone(),
                    }],
                    origin: MessageOrigin::User,
                    visibility: Visibility::UserAndModel,
                }],
                events: vec![started],
                records: vec![request_record, prompt, input_record.clone()],
                require_durable: false,
            },
        )
        .await?;
    let lease = store
        .acquire_lease(&scope, &run, &id("worker"), now, 30000)
        .await?;
    let mut context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id("caller"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: Some(values),
        },
        Default::default(),
    );
    let budget = RunBudget::attach(
        store.clone(),
        clock.clone(),
        Arc::new(RandomIdSource),
        scope.clone(),
        run.clone(),
        lease.clone(),
        context.cancellation.clone(),
    )
    .await?;
    let resolver = Arc::new(CurrentReport {
        value: Mutex::new(ResolvedSystemInput {
            value: json!(REPORT_A),
            revision: id("revision-1"),
        }),
        calls: AtomicUsize::new(0),
    });
    let binder = InputBinder::new(
        registry.clone(),
        Some(resolver.clone()),
        Arc::new(PolicyGate::new(
            Arc::new(OwnedTargets),
            Duration::from_secs(1),
        )?),
        Arc::new(RandomIdSource),
    );
    plan(
        &store,
        &scope,
        &run,
        &lease,
        &clock,
        "search-call",
        &search,
        JsonObject::from([("query".into(), json!("recent results"))]),
    )
    .await?;
    let search_result = binder
        .bind(&search, &id("search-call"), &context, &budget)
        .await?;
    assert_eq!(
        serde_json::to_value(search_result.input.execution_args())?,
        json!({"query":"recent results","limit":10,"workspace_id":WORKSPACE})
    );
    assert_eq!(
        serde_json::to_value(search_result.input.original_model_inputs())?,
        json!({"query":"recent results"})
    );
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
    context.data.system_inputs = None;
    plan(
        &store,
        &scope,
        &run,
        &lease,
        &clock,
        "report-first",
        &report,
        JsonObject::new(),
    )
    .await?;
    let first = binder
        .bind(&report, &id("report-first"), &context, &budget)
        .await?;
    *resolver.value.lock().unwrap() = ResolvedSystemInput {
        value: json!(REPORT_B),
        revision: id("revision-2"),
    };
    let cached = binder
        .bind(&report, &id("report-first"), &context, &budget)
        .await?;
    assert_eq!(cached.reference, first.reference);
    assert_eq!(cached.input.execution_args()["report_id"], json!(REPORT_A));
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    plan(
        &store,
        &scope,
        &run,
        &lease,
        &clock,
        "report-next",
        &report,
        JsonObject::new(),
    )
    .await?;
    let next = binder
        .bind(&report, &id("report-next"), &context, &budget)
        .await?;
    assert_eq!(next.input.execution_args()["report_id"], json!(REPORT_B));
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 2);
    let restored = RunSystemInputs::restore(&input_record, &input_ref, &scope, &registry)?;
    restored.validate_resume(None)?;
    assert!(
        restored
            .validate_resume(Some(&SystemInputs::default()))
            .is_err()
    );
    println!(
        "input binding consumer: model query + default limit + Host workspace; unused key omitted; cached target fixed; new call resolves the new report; omitted resume inputs reuse the snapshot"
    );
    Ok(())
}
```

## `tests/support/tool_loop_consumer.rs`

```rust
// Synthetic model, tool, and metadata inspector. No provider network or business
// database calls occur. SQLite is real; the assertions exercise the public Agent API.
use futures_util::{TryStreamExt, stream};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;
use wickle_model_router::PolicyModelRouter;
use wickle_state_sqlite::SqliteStateStore;

const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";
fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .expect("object fixture")
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
fn completed<T>(value: Guarded<T>) -> Result<T, Box<dyn std::error::Error>> {
    match value {
        Guarded::Completed(value) => Ok(value),
        Guarded::ApprovalRequired(_) => Err("unexpected approval".into()),
    }
}

struct Catalog {
    calls: AtomicUsize,
}
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(
                        reference
                            .version
                            .clone()
                            .unwrap_or_else(|| id("model-binding-revision")),
                    ),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("synthetic registered component")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| id("search")),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}
struct Policy;
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::ExecuteTool { input } = &request.action {
                if input.execution_args().get("workspace_id") != Some(&json!(WORKSPACE)) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("foreign-workspace"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}
struct Inspector;
impl ModelRouteInspector for Inspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        _: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        // Fixture echo only: production inspectors must fetch authoritative metadata.
        Box::pin(async move {
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: ModelRouteAvailability::Available,
                model_id: Some(route.model_id.clone()),
                model_version: Some(route.model_version.clone()),
                deployment_revision: None,
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id("synthetic-inspection"),
            })
        })
    }
}
struct Estimate;
impl ModelTokenEstimator for Estimate {
    fn estimate(&self, _: &ModelRequest) -> Result<u64, ContractError> {
        // Deliberately synthetic estimate for this fixed fixture, not a tokenizer.
        Ok(512)
    }
}

fn routing(scope: &Scope) -> Result<RoutingSnapshot, ContractError> {
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

struct Model {
    route: ResolvedModelRoute,
    calls: AtomicUsize,
}
impl ModelPort for Model {
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
        assert_eq!(request.route, self.route);
        assert_eq!(request.tools.len(), 1);
        assert_eq!(request.tools[0].name, id("search"));
        assert_eq!(
            request.tools[0].model_input_schema["properties"],
            json!({"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"default":2}})
        );
        // A concrete protected UUID exists in this execution; its absence is a data-boundary check.
        assert!(!serde_json::to_string(request).unwrap().contains(WORKSPACE));
        let events = match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => vec![
                Ok(ModelEvent::ToolArgumentsDelta {
                    index: 0,
                    provider_call_id: Some("search-alpha".into()),
                    name: Some("search".into()),
                    delta: r#"{"query":"alpha"}"#.into(),
                }),
                Ok(ModelEvent::ToolArgumentsDelta {
                    index: 1,
                    provider_call_id: Some("search-beta".into()),
                    name: Some("search".into()),
                    delta: r#"{"query":"beta","limit":3}"#.into(),
                }),
                Ok(ModelEvent::ResponseCompleted {
                    finish: ModelFinish::ToolCalls,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![],
                }),
            ],
            1 => {
                let calls: Vec<_> = request
                    .messages
                    .iter()
                    .flat_map(|message| &message.content)
                    .filter_map(|content| {
                        if let ModelContent::ToolCall {
                            provider_call_id,
                            arguments,
                            ..
                        } = content
                        {
                            Some((provider_call_id.clone(), arguments.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();
                assert_eq!(
                    calls,
                    vec![
                        (id("search-alpha"), object(json!({"query":"alpha"}))),
                        (id("search-beta"), object(json!({"query":"beta","limit":3})))
                    ]
                );
                let results: Vec<_> = request
                    .messages
                    .iter()
                    .flat_map(|message| &message.content)
                    .filter_map(|content| {
                        if let ModelContent::ToolResult {
                            provider_call_id,
                            content,
                        } = content
                        {
                            Some((provider_call_id.clone(), content.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();
                assert_eq!(results.len(), 2);
                for (index, query) in ["alpha", "beta"].iter().enumerate() {
                    assert_eq!(results[index].0, calls[index].0);
                    assert_eq!(
                        results[index].1,
                        json!({"status":"succeeded","effect":"not_applied","content":[{"type":"json","value":{"query":query,"count":index+2}}]})
                    );
                }
                vec![
                    Ok(ModelEvent::TextDelta {
                        text: "Alpha has 2 results; beta has 3.".into(),
                    }),
                    Ok(ModelEvent::ResponseCompleted {
                        finish: ModelFinish::Stop,
                        metadata: ModelResponseMetadata::default(),
                        continuation: vec![],
                    }),
                ]
            }
            _ => panic!("duplicate start must not invoke the model again"),
        };
        Box::pin(stream::iter(events))
    }
}
struct Search {
    arguments: Mutex<Vec<JsonObject>>,
}
impl ToolExecutor for Search {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        assert_eq!(args.get("workspace_id"), Some(&json!(WORKSPACE)));
        assert_eq!(context.scope.workspace_id, id("workspace"));
        assert_eq!(context.principal_ref, id("actor"));
        let mut arguments = self.arguments.lock().unwrap();
        let index = arguments.len();
        assert_eq!(
            args,
            &object(if index == 0 {
                json!({"query":"alpha","limit":2,"workspace_id":WORKSPACE})
            } else {
                json!({"query":"beta","limit":3,"workspace_id":WORKSPACE})
            })
        );
        arguments.push(args.clone());
        drop(arguments);
        Box::pin(async move {
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: json!({"query":args["query"],"count":args["limit"]}),
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}
fn registry(
    scope: &Scope,
    search: Arc<Search>,
) -> Result<(SystemInputRegistry, ToolRegistry), ContractError> {
    let inputs = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])?;
    let compiled = SchemaCompiler::new().compile(ToolDescriptor { tool: reference("search"), name: id("search"), description: "Search authorized records".into(),
        input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"default":2},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters: vec!["query".into(),"limit".into()], system_bindings: None,
        output_schema: json!({"type":"object","properties":{"query":{"type":"string"},"count":{"type":"integer"}},"required":["query","count"],"additionalProperties":false}),
        side_effect: ToolSideEffect::ReadOnly, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false, max_output_bytes: 1024.try_into().unwrap() }, &inputs)?;
    Ok((
        inputs,
        ToolRegistry::new(
            scope.clone(),
            vec![ToolRegistration {
                compiled,
                executor: search,
            }],
        )?,
    ))
}
struct TemporaryDatabase(std::path::PathBuf);
impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
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
        std::env::temp_dir().join(format!("wickle-tool-loop-{}", RandomIdSource.next_id()?)),
    );
    std::fs::create_dir(&temporary.0)?;
    let database = temporary.0.join("state.sqlite3");
    let routing = routing(&scope)?;
    let model = Arc::new(Model {
        route: routing.route_for_binding(&reference("primary"))?,
        calls: AtomicUsize::new(0),
    });
    let search = Arc::new(Search {
        arguments: Mutex::new(vec![]),
    });
    let catalog = Arc::new(Catalog {
        calls: AtomicUsize::new(0),
    });
    let policy = Arc::new(PolicyGate::new(Arc::new(Policy), Duration::from_secs(1))?);
    let exchange = Arc::new(
        ModelExchange::new(model.clone(), policy.clone())
            .with_route_inspector(Arc::new(Inspector), Duration::from_secs(1))?,
    );
    let router = Arc::new(PolicyModelRouter::new(routing)?);
    let profile = AgentProfile::from_json(
        r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1",
        "name":"Assistant","description":"Synthetic tool loop consumer","instructions":{"text":"Search then summarize the observations"},
        "model_binding":"primary","tools":[{"tool_id":"search","version":"1"}],"skills":[],"connectors":[],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":4,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}
    }"#,
    )?;
    let make_agent = |store: Arc<SqliteStateStore>| -> Result<Agent, ContractError> {
        let (system_inputs, tools) = registry(&scope, search.clone())?;
        create_agent(
            profile.clone(),
            AgentBindings {
                scope: scope.clone(),
                state: store,
                policy: policy.clone(),
                profile_resolver: catalog.clone(),
                model_exchange: exchange.clone(),
                router: router.clone(),
                host_instructions: vec!["Use only the authorized workspace.".into()],
                system_inputs,
                tools: Some(Arc::new(tools)),
                system_input_resolver: None,
                clock: Arc::new(SystemClock::new()),
                ids: Arc::new(RandomIdSource),
                token_estimator: Arc::new(Estimate),
                settings: AgentSettings {
                    require_durable: true,
                    max_output_tokens: 128.try_into().unwrap(),
                    ..AgentSettings::default()
                },
            },
        )
    };
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id("actor"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: Some(SystemInputs::new(object(json!({"workspace_id":WORKSPACE})))),
        },
        Default::default(),
    );
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Compare alpha and beta".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    };
    let store = Arc::new(SqliteStateStore::open(&database)?);
    let agent = make_agent(store.clone())?;
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert!(search.arguments.lock().unwrap().is_empty());
    let handle = completed(agent.start(request.clone(), context.clone()).await?)?;
    let run_id = handle.run_id().clone();
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
            text: "Alpha has 2 results; beta has 3.".into()
        }]
    );
    assert_eq!(
        (outcome.usage.model_calls, outcome.usage.tool_attempts),
        (2, 2)
    );
    let events: Vec<_> = handle.events(0, context.clone()).try_collect().await?;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "tool.planned")
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "tool.settled")
            .count(),
        2
    );
    assert_eq!(
        events.last().ok_or("missing terminal event")?.event_type,
        "run.finished"
    );
    let saved = store.load(&scope, &run_id).await?;
    assert_eq!(
        saved.snapshot.tool_ledger[0].call.model_inputs,
        object(json!({"query":"alpha"}))
    );
    for entry in &saved.snapshot.tool_ledger {
        assert!(entry.call.bound_input_ref.is_some());
        assert!(
            matches!(&entry.state, ToolCallState::Settled { result } if result.status == ToolResultStatus::Succeeded && result.effect == ToolEffect::NotApplied)
        );
    }
    drop(handle);
    drop(agent);
    drop(store);

    let reopened = Arc::new(SqliteStateStore::open(&database)?);
    let restored = reopened.load(&scope, &run_id).await?;
    assert_eq!(restored.snapshot.outcome, Some(outcome.clone()));
    assert_eq!(restored.snapshot.tool_ledger, saved.snapshot.tool_ledger);
    assert!(restored.session.active_run_id.is_none());
    let resolver_calls = catalog.calls.load(Ordering::SeqCst);
    let replay_agent = make_agent(reopened)?;
    let replay = completed(replay_agent.start(request, context.clone()).await?)?;
    assert_eq!(replay.run_id(), &run_id);
    assert_eq!(completed(replay.outcome(&context).await?)?, outcome);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(search.arguments.lock().unwrap().len(), 2);
    assert_eq!(catalog.calls.load(Ordering::SeqCst), resolver_calls);
    println!(
        "tool loop consumer: two serial calls with system UUID binding and model-only arguments; final model response; SQLite reopen and request replay without additional model, tool, or resolver calls"
    );
    Ok(())
}
```
