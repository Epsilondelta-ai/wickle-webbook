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
        provider_arguments: None,
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
        source_model_request_id: None,
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
        skill_ref: None,
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
        source_model_request_id: None,
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
