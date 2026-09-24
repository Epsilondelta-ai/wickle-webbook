//! Policy, persisted accounting, and recovery around a single-call model port.
use futures_util::{StreamExt, stream};
use serde_json::json;
use std::{
    collections::VecDeque,
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
use support::{admission, id, scope};

fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn request(provider: &str) -> ModelRequest {
    ModelRequest {
        request_id: id("logical-step"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference(provider),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id("model"),
            model_id: id("model"),
            model_version: id("release"),
            version_semantics: VersionSemantics::Pinned,
            provider: id(provider),
            target: JsonObject::new(),
            deployment_revision: None,
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("v1"),
            },
            adapter: reference(&format!("{provider}-adapter")),
            capability_revision: id("capabilities"),
            connection_ref: reference(&format!("{provider}-connection")),
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find evidence".into(),
            }],
        }],
        tools: vec![],
        output: ModelOutput::Text {},
        max_output_tokens: 32.try_into().unwrap(),
        options: JsonObject::new(),
        limits: ModelResponseLimits {
            max_input_bytes: 8192,
            max_response_bytes: 4096,
            max_delta_bytes: 1024,
            max_events: 32,
            max_tool_calls: 0,
        },
    }
}

struct Policy {
    calls: AtomicUsize,
    deny_at: AtomicUsize,
    approval_at: AtomicUsize,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            deny_at: AtomicUsize::new(usize::MAX),
            approval_at: AtomicUsize::new(usize::MAX),
        }
    }
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        _: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        let count = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(async move {
            Ok(if count >= self.deny_at.load(Ordering::SeqCst) {
                PolicyDecision::Deny {
                    reason: id("revoked"),
                }
            } else if count >= self.approval_at.load(Ordering::SeqCst) {
                PolicyDecision::RequireApproval {
                    reason: id("review"),
                }
            } else {
                PolicyDecision::Allow {}
            })
        })
    }
}

enum Reply {
    Complete,
    Tool(String),
    Fail(ModelFailureKind),
    IncompleteTool,
    Pending,
}
struct ScriptedModel {
    store: Arc<MemoryStateStore>,
    binding: ModelPortBinding,
    replies: Mutex<VecDeque<Reply>>,
    observed: Mutex<Vec<(ModelRequest, Id, CancellationToken)>>,
    entered: Notify,
}
impl ModelPort for ScriptedModel {
    fn binding(&self) -> ModelPortBinding {
        self.binding.clone()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        self.observed.lock().unwrap().push((
            request.clone(),
            context.attempt_id.clone(),
            context.cancellation.clone(),
        ));
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("no hidden extra physical call");
        let start = stream::once(async move {
            let saved = self
                .store
                .load(&context.scope, &context.run_id)
                .await
                .unwrap();
            let attempt = saved
                .snapshot
                .model_ledger
                .iter()
                .find(|entry| entry.attempt_id == context.attempt_id)
                .unwrap();
            assert!(matches!(attempt.state, ModelAttemptState::Reserved {}));
            assert_eq!(attempt.request_digest, request.digest());
            assert_eq!(attempt.route, request.route);
            assert!(
                saved
                    .snapshot
                    .reservations
                    .iter()
                    .any(|entry| entry.attempt_id == context.attempt_id)
            );
            self.entered.notify_one();
            Ok(ModelEvent::TextDelta {
                text: "candidate".into(),
            })
        });
        let tail: PortStream<'a, ModelEvent> = match reply {
            Reply::Complete => Box::pin(stream::iter([Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::Stop,
                metadata: ModelResponseMetadata {
                    provider_request_id: Some(id("provider-response")),
                    usage: Some(ModelUsage {
                        measurement: UsageMeasurement::Reported,
                        input_tokens: Some(3),
                        output_tokens: Some(2),
                    }),
                    ..ModelResponseMetadata::default()
                },
                continuation: vec![],
            })])),
            Reply::Fail(kind) => Box::pin(stream::iter([Ok(ModelEvent::ResponseError {
                kind,
                metadata: ModelResponseMetadata::default(),
            })])),
            Reply::IncompleteTool => Box::pin(stream::iter([Ok(ModelEvent::ToolArgumentsDelta {
                index: 0,
                provider_call_id: Some("tool".into()),
                name: Some("search".into()),
                delta: "{\"query\":".into(),
            })])),
            Reply::Tool(arguments) => Box::pin(stream::iter([
                Ok(ModelEvent::ToolArgumentsDelta {
                    index: 0,
                    provider_call_id: Some("provider-tool".into()),
                    name: Some("wire_search".into()),
                    delta: arguments,
                }),
                Ok(ModelEvent::ResponseCompleted {
                    finish: ModelFinish::ToolCalls,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![],
                }),
            ])),
            Reply::Pending => Box::pin(stream::pending()),
        };
        Box::pin(start.chain(tail))
    }
}
struct Fixture {
    store: Arc<MemoryStateStore>,
    budget: RunBudget,
    lease: RunLease,
    context: ExecutionContext,
    policy: Arc<Policy>,
}
impl Fixture {
    async fn new(models: u64, recovery: u64) -> Self {
        Self::with_records(models, recovery, Arc::new(RandomIdSource), vec![]).await
    }
    async fn with_records(
        models: u64,
        recovery: u64,
        ids: Arc<dyn IdSource>,
        records: Vec<ProtectedRecord>,
    ) -> Self {
        let clock = Arc::new(SystemClock::new());
        let now = clock.now().unwrap().utc_ms;
        let store = Arc::new(MemoryStateStore::new());
        let mut input = admission("run", "request", "session", "Find evidence", "1").await;
        input.snapshot.limits.max_model_calls = models.try_into().unwrap();
        input.snapshot.limits.max_recovery_attempts = recovery;
        input.snapshot.timing =
            RunTiming::new(now, input.snapshot.limits.max_elapsed_ms.get()).unwrap();
        input.events[0].timestamp_ms = now;
        input.records.extend(records);
        store.admit(&scope(), input).await.unwrap();
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("worker"), now, 20_000)
            .await
            .unwrap();
        let context = ExecutionContext::new(
            ExecutionContextData {
                scope: scope(),
                principal_ref: id("caller"),
                capability_grant_ref: id("grant"),
                trace_context: None,
                system_inputs: Some(SystemInputs::new(JsonObject::from([(
                    "database_fk".into(),
                    json!("host-only-value"),
                )]))),
            },
            CancellationToken::new(),
        );
        let budget = RunBudget::attach(
            store.clone(),
            clock,
            ids,
            scope(),
            id("run"),
            lease.clone(),
            context.cancellation.clone(),
        )
        .await
        .unwrap();
        Self {
            store,
            budget,
            lease,
            context,
            policy: Arc::new(Policy::default()),
        }
    }
    fn model(&self, provider: &str, replies: Vec<Reply>) -> Arc<ScriptedModel> {
        let route = request(provider).route;
        Arc::new(ScriptedModel {
            store: self.store.clone(),
            binding: ModelPortBinding {
                provider: route.provider,
                adapter: route.adapter,
                connection_ref: route.connection_ref,
            },
            replies: Mutex::new(replies.into()),
            observed: Mutex::new(vec![]),
            entered: Notify::new(),
        })
    }
    fn exchange(&self, model: Arc<dyn ModelPort>, retries: u32) -> ModelExchange {
        ModelExchange::new(
            model,
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap()),
        )
        .with_retry_policy(ModelRetryPolicy {
            max_retries: retries,
            backoff_ms: 0,
        })
    }
    async fn saved(&self) -> RunSnapshot {
        self.store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
    }
}

#[tokio::test]
async fn each_retry_has_its_own_saved_attempt_and_rechecks_policy() {
    let fixture = Fixture::new(3, 1).await;
    let model = fixture.model(
        "first",
        vec![Reply::Fail(ModelFailureKind::RateLimited), Reply::Complete],
    );
    let exchange = fixture.exchange(model.clone(), 2);
    let mut original = request("first");
    original.options = JsonObject::from([("reasoning_effort".into(), json!("high"))]);
    let result = exchange
        .generate(&original, &fixture.context, &fixture.budget)
        .await
        .unwrap();
    let Guarded::Completed(ModelExchangeOutcome::Completed { response }) = result else {
        panic!("expected completed response")
    };
    let saved = fixture.saved().await;
    assert_eq!(
        (saved.usage.model_calls, saved.usage.recovery_attempts),
        (2, 1)
    );
    assert_eq!(saved.model_ledger.len(), 2);
    assert_eq!(
        saved.model_ledger[0].state,
        ModelAttemptState::Failed {
            kind: ModelFailureKind::RateLimited
        }
    );
    assert_eq!(saved.model_ledger[1].state, ModelAttemptState::Completed {});
    assert_eq!(saved.model_ledger[1].reported_model_id, None);
    assert_eq!(saved.model_ledger[1].reported_model_version, None);
    assert_eq!(
        saved.model_ledger[1].usage.as_ref().unwrap().output_tokens,
        Some(2)
    );
    assert_eq!(response.request_id, saved.model_ledger[1].attempt_id);
    {
        let observed = model.observed.lock().unwrap();
        assert_eq!(observed.len(), 2);
        assert_ne!(observed[0].1, observed[1].1);
        for (request, attempt, _) in observed.iter() {
            assert_eq!(&request.request_id, attempt);
            assert_eq!(request.route, original.route);
            assert_eq!(request.messages, original.messages);
            assert_eq!(request.options, original.options);
            // A real Host value exists but no model request surface automatically copies it.
            assert!(
                !serde_json::to_string(request)
                    .unwrap()
                    .contains("host-only-value")
            );
        }
    }
    assert!(
        saved
            .model_ledger
            .iter()
            .all(|entry| entry.model_step_id == original.request_id)
    );
    assert_eq!(fixture.policy.calls.load(Ordering::SeqCst), 4);
    let events = fixture
        .store
        .read_events(&scope(), &id("run"), 0, 10)
        .await
        .unwrap();
    assert_eq!(events.events.len(), 3);
}

#[tokio::test]
async fn exhausted_recovery_and_model_budgets_each_stop_new_requests() {
    for (models, recovery) in [(8, 1), (1, 1)] {
        let fixture = Fixture::new(models, recovery).await;
        let model = fixture.model(
            "first",
            vec![
                Reply::Fail(ModelFailureKind::Transport),
                Reply::Fail(ModelFailureKind::Transport),
            ],
        );
        let exchange = fixture.exchange(model.clone(), 100);
        assert_eq!(
            exchange
                .generate(&request("first"), &fixture.context, &fixture.budget)
                .await
                .unwrap_err()
                .code,
            ErrorCode::BudgetExceeded
        );
        let expected = if models == 1 { 1 } else { 2 };
        assert_eq!(model.observed.lock().unwrap().len(), expected);
        assert_eq!(fixture.saved().await.usage.model_calls, expected as u64);
    }
}

#[tokio::test]
async fn authentication_capability_and_unchanged_context_overflow_are_not_retried() {
    for kind in [
        ModelFailureKind::Authentication,
        ModelFailureKind::Unsupported,
        ModelFailureKind::ContextOverflow,
    ] {
        let fixture = Fixture::new(4, 1).await;
        let model = fixture.model("first", vec![Reply::Fail(kind)]);
        let exchange = fixture.exchange(model.clone(), 3);
        let Guarded::Completed(ModelExchangeOutcome::Failed { failure }) = exchange
            .generate(&request("first"), &fixture.context, &fixture.budget)
            .await
            .unwrap()
        else {
            panic!("expected classified failure")
        };
        assert_eq!(failure.kind, kind);
        assert_eq!(model.observed.lock().unwrap().len(), 1);
        assert_eq!(fixture.saved().await.usage.recovery_attempts, 0);
    }
}

#[tokio::test]
async fn default_retry_is_disabled_and_partial_tools_never_become_a_complete_plan() {
    let fixture = Fixture::new(4, 1).await;
    let model = fixture.model("first", vec![Reply::IncompleteTool]);
    let exchange = fixture.exchange(model.clone(), 0);
    let mut request = request("first");
    request.limits.max_tool_calls = 1;
    let Guarded::Completed(ModelExchangeOutcome::Failed { failure }) = exchange
        .generate(&request, &fixture.context, &fixture.budget)
        .await
        .unwrap()
    else {
        panic!("expected incomplete response rejection")
    };
    assert_eq!(failure.kind, ModelFailureKind::Protocol);
    assert_eq!(failure.partial_text(), "candidate");
    let saved = fixture.saved().await;
    assert!(saved.tool_ledger.is_empty());
    assert_eq!(saved.usage.tool_attempts, 0);
    assert_eq!(saved.usage.recovery_attempts, 0);
    assert_eq!(model.observed.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn initial_denial_and_revocation_after_reservation_both_prevent_adapter_entry() {
    for deny_at in [1, 2, 3] {
        let fixture = Fixture::new(4, 1).await;
        fixture.policy.deny_at.store(deny_at, Ordering::SeqCst);
        let model = fixture.model("first", vec![Reply::Fail(ModelFailureKind::RateLimited)]);
        let exchange = fixture.exchange(model.clone(), 1);
        assert_eq!(
            exchange
                .generate(&request("first"), &fixture.context, &fixture.budget)
                .await
                .unwrap_err()
                .code,
            ErrorCode::AccessDenied
        );
        assert_eq!(
            model.observed.lock().unwrap().len(),
            usize::from(deny_at == 3)
        );
        assert_eq!(
            fixture.saved().await.usage.model_calls,
            u64::from(deny_at >= 2)
        );
    }
}

#[tokio::test]
async fn approval_does_not_dispatch_and_unreserved_approval_consumes_no_budget() {
    for approval_at in [1, 2] {
        let fixture = Fixture::new(4, 1).await;
        fixture
            .policy
            .approval_at
            .store(approval_at, Ordering::SeqCst);
        let model = fixture.model("first", vec![]);
        let exchange = fixture.exchange(model.clone(), 0);
        assert!(matches!(
            exchange
                .generate(&request("first"), &fixture.context, &fixture.budget)
                .await
                .unwrap(),
            Guarded::ApprovalRequired(_)
        ));
        assert!(model.observed.lock().unwrap().is_empty());
        assert_eq!(
            fixture.saved().await.usage.model_calls,
            u64::from(approval_at == 2)
        );
    }
}

#[tokio::test]
async fn wrong_connection_scope_or_opaque_route_is_rejected_before_reservation() {
    let fixture = Fixture::new(4, 1).await;
    let model = fixture.model("second", vec![]);
    let exchange = fixture.exchange(model.clone(), 0);
    assert!(
        exchange
            .generate(&request("first"), &fixture.context, &fixture.budget)
            .await
            .is_err()
    );
    let mut foreign_context = fixture.context.clone();
    foreign_context.data.scope.workspace_id = id("foreign");
    assert_eq!(
        exchange
            .generate(&request("second"), &foreign_context, &fixture.budget)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    let mut changed = request("second");
    changed.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![ModelContent::Opaque {
            continuation: OpaqueContinuation::new(
                &request("first").route,
                json!({"signature":"first-private"}),
            ),
        }],
    });
    assert!(
        exchange
            .generate(&changed, &fixture.context, &fixture.budget)
            .await
            .is_err()
    );
    assert!(model.observed.lock().unwrap().is_empty());
    assert_eq!(fixture.saved().await.usage.model_calls, 0);
}

#[tokio::test]
async fn cancellation_drops_the_adapter_signal_and_retains_an_unknown_charged_attempt() {
    let fixture = Fixture::new(4, 1).await;
    let model = fixture.model("first", vec![Reply::Pending]);
    let exchange = fixture.exchange(model.clone(), 2);
    let request = request("first");
    let execution = exchange.generate(&request, &fixture.context, &fixture.budget);
    let cancel = async {
        model.entered.notified().await;
        fixture.context.cancellation.cancel();
    };
    let (result, ()) = tokio::join!(execution, cancel);
    assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
    let saved = fixture.saved().await;
    assert_eq!(saved.usage.model_calls, 1);
    assert_eq!(saved.usage.recovery_attempts, 0);
    assert_eq!(saved.model_ledger[0].state, ModelAttemptState::Unknown {});
    assert!(model.observed.lock().unwrap()[0].2.is_cancelled());
}

#[tokio::test]
async fn exhausted_recovery_retains_the_last_partial_failure_in_protected_storage() {
    let fixture = Fixture::new(4, 0).await;
    let model = fixture.model("first", vec![Reply::Fail(ModelFailureKind::Transport)]);
    let exchange = fixture.exchange(model.clone(), 1);
    assert_eq!(
        exchange
            .generate(&request("first"), &fixture.context, &fixture.budget)
            .await
            .unwrap_err()
            .code,
        ErrorCode::BudgetExceeded
    );
    let saved = fixture.saved().await;
    let record = fixture
        .store
        .read_record(
            &scope(),
            saved.model_ledger[0].response_ref.as_ref().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(record.value()["outcome"]["result"], "failed");
    assert_eq!(record.value()["outcome"]["failure"]["kind"], "transport");
    assert_eq!(
        record.value()["outcome"]["failure"]["partial_text"],
        "candidate"
    );
    assert_eq!(model.observed.lock().unwrap().len(), 1);
    let mut foreign = scope();
    foreign.workspace_id = id("other");
    assert!(
        fixture
            .store
            .read_record(&foreign, record.reference())
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn caller_cancellation_interrupts_backoff_even_with_an_independent_budget_token() {
    let mut fixture = Fixture::new(4, 1).await;
    fixture.context.cancellation = CancellationToken::new();
    let model = fixture.model("first", vec![Reply::Fail(ModelFailureKind::Transport)]);
    let exchange = ModelExchange::new(
        model.clone(),
        Arc::new(PolicyGate::new(fixture.policy.clone(), Duration::from_secs(1)).unwrap()),
    )
    .with_retry_policy(ModelRetryPolicy {
        max_retries: 1,
        backoff_ms: 1000,
    });
    let request = request("first");
    let cancel = async {
        loop {
            if fixture.saved().await.usage.recovery_attempts == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        fixture.context.cancellation.cancel();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_millis(5), async {
        tokio::join!(
            exchange.generate(&request, &fixture.context, &fixture.budget),
            cancel
        )
    })
    .await
    .expect("caller cancellation must not wait for the one-second backoff");
    assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
    assert_eq!(model.observed.lock().unwrap().len(), 1);
}

struct PendingPolicy {
    entered: Notify,
}
impl PolicyPort for PendingPolicy {
    fn authorize<'a>(
        &'a self,
        _: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test(start_paused = true)]
async fn budget_cancellation_interrupts_policy_even_with_an_independent_caller_token() {
    let mut fixture = Fixture::new(4, 1).await;
    let budget_cancellation = fixture.context.cancellation.clone();
    fixture.context.cancellation = CancellationToken::new();
    let policy = Arc::new(PendingPolicy {
        entered: Notify::new(),
    });
    let model = fixture.model("first", vec![]);
    let exchange = ModelExchange::new(
        model.clone(),
        Arc::new(PolicyGate::new(policy.clone(), Duration::from_secs(1)).unwrap()),
    );
    let request = request("first");
    let cancel = async {
        policy.entered.notified().await;
        budget_cancellation.cancel();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_millis(5), async {
        tokio::join!(
            exchange.generate(&request, &fixture.context, &fixture.budget),
            cancel
        )
    })
    .await
    .expect("budget cancellation must not wait for the policy timeout");
    assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
    assert!(model.observed.lock().unwrap().is_empty());
    assert_eq!(fixture.saved().await.usage.model_calls, 0);
}

struct FixedAttempt;
impl IdSource for FixedAttempt {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id("fixed-attempt"))
    }
}

#[tokio::test]
async fn failed_invocation_or_response_persistence_never_causes_an_untracked_retry() {
    for (record_id, expected_calls) in [
        ("model-invocation-fixed-attempt", 0),
        ("model-response-fixed-attempt", 1),
    ] {
        let existing = ProtectedRecord::new(id(record_id), 1, json!({"original":"immutable"}));
        let fixture =
            Fixture::with_records(4, 1, Arc::new(FixedAttempt), vec![existing.clone()]).await;
        let model = fixture.model("first", vec![Reply::Complete]);
        let exchange = fixture.exchange(model.clone(), 5);
        assert_eq!(
            exchange
                .generate(&request("first"), &fixture.context, &fixture.budget)
                .await
                .unwrap_err()
                .code,
            ErrorCode::RecordConflict
        );
        assert_eq!(model.observed.lock().unwrap().len(), expected_calls);
        let saved = fixture.saved().await;
        assert_eq!(saved.usage.model_calls, expected_calls as u64);
        assert_eq!(saved.reservations.len(), expected_calls);
        assert_eq!(saved.usage.recovery_attempts, 0);
        assert_eq!(saved.model_ledger.len(), expected_calls);
        if expected_calls == 1 {
            assert_eq!(saved.model_ledger[0].state, ModelAttemptState::Reserved {});
            assert_eq!(saved.model_ledger[0].response_ref, None);
        }
        assert_eq!(
            fixture
                .store
                .read_record(&scope(), existing.reference())
                .await
                .unwrap()
                .value(),
            existing.value()
        );
    }
}

#[tokio::test]
async fn a_completed_ledger_entry_requires_the_exact_typed_response_and_reservation() {
    let fixture = Fixture::new(4, 1).await;
    fixture.policy.approval_at.store(2, Ordering::SeqCst);
    let model = fixture.model("first", vec![]);
    let exchange = fixture.exchange(model, 0);
    exchange
        .generate(&request("first"), &fixture.context, &fixture.budget)
        .await
        .unwrap();
    let before = fixture.saved().await;
    let entry = &before.model_ledger[0];
    let metadata = ModelResponseMetadata::default();
    let body = StoredModelResponse {
        request_id: entry.attempt_id.clone(),
        route_digest: entry.route.digest(),
        outcome: ModelExchangeOutcome::Completed {
            response: ModelResponse {
                request_id: entry.attempt_id.clone(),
                route_digest: entry.route.digest(),
                text: "Completed candidate".into(),
                tool_calls: vec![],
                finish: ModelFinish::Stop,
                metadata,
                continuation: vec![],
            },
        },
    };
    let valid = serde_json::to_value(&body).unwrap();
    let mut wrong_attempt = valid.clone();
    wrong_attempt["request_id"] = json!("another-attempt");
    let mut wrong_route = valid.clone();
    wrong_route["route_digest"] = serde_json::to_value(request("second").route.digest()).unwrap();
    let mut wrong_metadata = valid.clone();
    wrong_metadata["outcome"]["response"]["metadata"]["reported_model_id"] =
        json!("unreported-model");
    let mut wrong_finish = valid.clone();
    wrong_finish["outcome"]["response"]["finish"] = json!("tool_calls");
    for (index, value) in [
        json!({"unrelated":"record"}),
        wrong_attempt,
        wrong_route,
        wrong_metadata,
        wrong_finish,
    ]
    .into_iter()
    .enumerate()
    {
        let record = ProtectedRecord::new(id(&format!("invalid-{index}")), 1, value);
        let mut commit = support::prepared(
            &before,
            fixture.lease.clone(),
            before.timing.last_observed_at_ms,
        );
        commit.snapshot.model_ledger[0].state = ModelAttemptState::Completed {};
        commit.snapshot.model_ledger[0].response_ref = Some(record.reference().clone());
        commit.records.push(record);
        assert_eq!(
            fixture
                .store
                .commit(&scope(), &id("run"), commit)
                .await
                .unwrap_err()
                .code,
            ErrorCode::InvalidSnapshot
        );
        assert_eq!(fixture.saved().await, before);
    }
    let mut missing = before.clone();
    missing.model_ledger[0].state = ModelAttemptState::Completed {};
    assert!(missing.validate().is_err());
    let mut unreserved = before.clone();
    unreserved.model_ledger[0].attempt_id = id("unreserved");
    assert!(unreserved.validate().is_err());
    let record = ProtectedRecord::new(id("valid-response"), 1, valid);
    let mut commit = support::prepared(
        &before,
        fixture.lease.clone(),
        before.timing.last_observed_at_ms,
    );
    commit.snapshot.model_ledger[0].state = ModelAttemptState::Completed {};
    commit.snapshot.model_ledger[0].response_ref = Some(record.reference().clone());
    commit.records.push(record.clone());
    let saved = fixture
        .store
        .commit(&scope(), &id("run"), commit)
        .await
        .unwrap();
    assert_eq!(
        saved.snapshot.model_ledger[0].response_ref.as_ref(),
        Some(record.reference())
    );
}

struct RenamedArguments;
impl ProviderToolSchemaCompiler for RenamedArguments {
    fn reference(&self) -> VersionedRef {
        reference("renamed")
    }
    fn compile(
        &self,
        tool: &ModelTool,
        _: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        Ok(ProviderToolProjection {
            wire_tool: ModelTool {
                name: id("wire_search"),
                description: tool.description.clone(),
                model_input_schema: json!({"type":"object","properties":{"q":{"type":"string"},"n":{"type":"object","properties":{"present":{"type":"boolean"},"value":{"type":["integer","null"]}},"required":["present","value"],"additionalProperties":false}},"required":["q","n"],"additionalProperties":false}),
            },
            decode_plan: ArgumentDecodePlan::Fields {
                fields: vec![
                    ArgumentFieldMapping {
                        wire_name: "q".into(),
                        canonical_name: "query".into(),
                        encoding: ArgumentValueEncoding::Identity {},
                    },
                    ArgumentFieldMapping {
                        wire_name: "n".into(),
                        canonical_name: "limit".into(),
                        encoding: ArgumentValueEncoding::Presence {
                            present_key: "present".into(),
                            value_key: "value".into(),
                        },
                    },
                ],
            },
        })
    }
}
#[tokio::test]
async fn saved_provider_codec_restores_canonical_arguments_before_required_defaults() {
    let registry = SystemInputRegistry::new(vec![]).unwrap();
    let canonical = SchemaCompiler::new().compile(ToolDescriptor::from_json(r#"{
        "tool":{"id":"search","version":"1"},"name":"search","description":"Search",
        "input_schema":{"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","default":10}},"required":["query","limit"],"additionalProperties":false},
        "agent_parameters":["query","limit"],"output_schema":{"type":"string"},"max_output_bytes":100
    }"#).unwrap(),&registry).unwrap();
    let mut request = request("first");
    let target = ProviderToolTarget::for_route(&request.route);
    let contract = CompiledToolContract::compile(
        &canonical,
        target,
        &RenamedArguments,
        ProviderToolSchemaLimits::default(),
    )
    .unwrap();
    let record = ProtectedRecord::new(
        id("compiled-provider-tool"),
        1,
        serde_json::to_value(&contract).unwrap(),
    );
    let contract_ref = record.reference().clone();
    let fixture = Fixture::with_records(1, 0, Arc::new(RandomIdSource), vec![record]).await;
    request.tools = vec![contract.wire_tool().clone()];
    request.limits.max_tool_calls = 1;
    let raw = r#"{ "q": "evidence", "n": {"present":false,"value":null} }"#;
    let model = fixture.model("first", vec![Reply::Tool(raw.into())]);
    let response = fixture
        .exchange(model, 0)
        .generate(&request, &fixture.context, &fixture.budget)
        .await
        .unwrap();
    let Guarded::Completed(ModelExchangeOutcome::Completed { response }) = response else {
        panic!("expected complete Tool proposal")
    };
    let proposal = &response.tool_calls[0];
    assert_eq!(proposal.raw_arguments.as_deref(), Some(raw));
    let mut call = ToolCall {
        provider_arguments: Some(ProviderToolArguments {
            name: proposal.name.clone(),
            raw: raw.into(),
            compiled_contract_ref: Some(contract_ref),
        }),
        call_id: id("call"),
        model_request_id: response.request_id,
        provider_call_id: proposal.provider_call_id.clone(),
        tool_name: id("search"),
        model_inputs: contract
            .decode_arguments(raw, ProviderToolSchemaLimits::default())
            .unwrap(),
        descriptor_digest: Some(canonical.descriptor_digest().clone()),
        bound_input_ref: None,
    };
    let binder = InputBinder::new(
        Arc::new(registry),
        None,
        Arc::new(PolicyGate::new(fixture.policy.clone(), Duration::from_secs(1)).unwrap()),
        Arc::new(RandomIdSource),
    );
    let restored = binder
        .prepare_model_inputs(&canonical, &call, &fixture.context, &fixture.budget)
        .await
        .unwrap();
    assert_eq!(
        restored,
        JsonObject::from([
            ("query".into(), json!("evidence")),
            ("limit".into(), json!(10))
        ])
    );
    assert!(!call.model_inputs.contains_key("limit"));
    call.model_inputs.insert("query".into(), json!("changed"));
    assert_eq!(
        binder
            .prepare_model_inputs(&canonical, &call, &fixture.context, &fixture.budget)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidArguments
    );
}
