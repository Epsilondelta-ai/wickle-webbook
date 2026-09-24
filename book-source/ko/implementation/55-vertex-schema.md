# 55장 전체 구현과 변경 검사

[강의](../55-vertex-schema.md) · [전체 변경 패치](../solutions/55-vertex-schema.patch)

기준 `63c5ec65b1c44b6293bfe49e8afaefff32e83c23`. 이 단계에서 바뀐 Rust·manifest·Python 파일의 전체 내용이다. 이전 버전과의 정확한 교체 위치·삭제는 patch를 따른다. 다음 장의 코드와 섞지 않는다.

## `crates/wickle-model-vertex/src/model.rs`

```rust
use crate::{VertexAudience, VertexConnection, error};
use futures_util::stream;
use reqwest::Response;
use std::collections::VecDeque;
use wickle::*;
use wickle_model_gemini::protocol::{GenerateContentDecoder, encode_vertex_request};
use wickle_model_responses::SseDecoder;

/// One Vertex streamGenerateContent POST per invocation, streamed into Wickle model events.
/// The adapter does not retry, run Tool handlers, or load environment variables.
#[derive(Debug, Clone)]
pub struct VertexModel {
    connection: VertexConnection,
}
impl VertexModel {
    /// Bind an already configured connection without making a network request.
    pub fn new(connection: VertexConnection) -> Self {
        Self { connection }
    }
}
impl ModelPort for VertexModel {
    fn tool_schema_compiler(&self) -> std::sync::Arc<dyn ProviderToolSchemaCompiler> {
        use wickle_model_gemini::protocol::{FunctionSchemaFormat, GeminiToolSchemaCompiler};
        std::sync::Arc::new(GeminiToolSchemaCompiler::new(
            FunctionSchemaFormat::JsonSchema,
        ))
    }
    fn binding(&self) -> ModelPortBinding {
        self.connection.binding()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let state = State {
            connection: &self.connection,
            request,
            context,
            response: None,
            decoder: GenerateContentDecoder::for_vertex(request),
            framing: SseDecoder::new(
                self.connection.0.options.max_transport_bytes,
                self.connection.0.options.max_event_bytes,
                self.connection.0.options.max_protocol_events,
            ),
            queue: VecDeque::new(),
            started: false,
            finished: false,
        };
        Box::pin(stream::unfold(state, |mut state| async move {
            loop {
                if !state.finished && state.context.cancellation.is_cancelled() {
                    state.queue.clear();
                    state.fail(error(ErrorCode::Cancelled, "stream"));
                }
                if !state.finished && tokio::time::Instant::now() >= state.context.deadline {
                    state.queue.clear();
                    state.fail(error(ErrorCode::DeadlineExceeded, "stream"));
                }
                if let Some(event) = state.queue.pop_front() {
                    return Some((event, state));
                }
                if state.finished {
                    return None;
                }
                if !state.started {
                    state.started = true;
                    if let Err(failure) = state.start().await {
                        state.fail(failure);
                    }
                    continue;
                }
                let result = {
                    let response = state
                        .response
                        .as_mut()
                        .expect("started response or finished state");
                    tokio::select! { biased;
                        _ = state.context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "stream")),
                        _ = tokio::time::sleep_until(state.context.deadline) => Err(error(ErrorCode::DeadlineExceeded, "stream")),
                        chunk = response.chunk() => chunk.map_err(transport_error),
                    }
                };
                match result {
                    Ok(Some(bytes)) => match state.framing.push(&bytes) {
                        Ok(events) => {
                            for event in events {
                                match state.decoder.event(event) {
                                    Ok(events) => state.queue.extend(events.into_iter().map(Ok)),
                                    Err(error) => {
                                        state.fail(error);
                                        break;
                                    }
                                }
                            }
                        }
                        Err(error) => state.fail(error),
                    },
                    Ok(None) => match state.framing.finish().and_then(|_| state.decoder.finish()) {
                        Ok(event) => {
                            state.queue.push_back(Ok(event));
                            state.finished = true;
                            state.response = None;
                        }
                        Err(error) => state.fail(error),
                    },
                    Err(error) => state.fail(error),
                }
            }
        }))
    }
}
struct State<'a> {
    connection: &'a VertexConnection,
    request: &'a ModelRequest,
    context: &'a ModelCallContext,
    response: Option<Response>,
    decoder: GenerateContentDecoder<'a>,
    framing: SseDecoder,
    queue: VecDeque<Result<ModelEvent, ContractError>>,
    started: bool,
    finished: bool,
}
impl State<'_> {
    async fn start(&mut self) -> Result<(), ContractError> {
        self.connection
            .validate(&self.request.route, &self.context.scope)?;
        if self.context.attempt_id != self.request.request_id {
            return Err(error(ErrorCode::RequestConflict, "attempt"));
        }
        self.request.validate()?;
        let body = encode_vertex_request(self.request)?;
        if body.len() > self.request.limits.max_input_bytes {
            return Err(error(ErrorCode::ModelCapabilityUnsupported, "request_size"));
        }
        let name = crate::connection::model_name(self.request.route.model_id.as_str())?;
        let mut url = self
            .connection
            .0
            .base
            .join(&format!(
                "v1/projects/{}/locations/{}/publishers/google/models/{}:streamGenerateContent",
                self.connection.0.options.project, self.connection.0.options.location, name
            ))
            .map_err(|_| error(ErrorCode::InvalidConfiguration, "generate_url"))?;
        url.query_pairs_mut().append_pair("alt", "sse");
        let headers = match self
            .connection
            .headers(
                VertexAudience::Inference,
                &self.context.cancellation,
                self.context.deadline,
            )
            .await
        {
            Ok(value) => value,
            Err(failure) if failure.code == ErrorCode::AccessDenied => {
                self.queue.push_back(Ok(ModelEvent::ResponseError {
                    kind: ModelFailureKind::Authentication,
                    metadata: self.decoder.metadata.clone(),
                }));
                self.finished = true;
                return Ok(());
            }
            Err(failure) => return Err(failure),
        };
        let operation = self
            .connection
            .0
            .client
            .post(url)
            .headers(headers)
            .header("accept", "text/event-stream")
            .header("accept-encoding", "identity")
            .body(body)
            .send();
        let response = tokio::select! { biased;
            _ = self.context.cancellation.cancelled() => return Err(error(ErrorCode::Cancelled, "request")),
            _ = tokio::time::sleep_until(self.context.deadline) => return Err(error(ErrorCode::DeadlineExceeded, "request")),
            result = operation => result.map_err(transport_error)?,
        };
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let kind = match status {
                401 | 403 => ModelFailureKind::Authentication,
                404 => ModelFailureKind::Unavailable,
                408 | 504 => ModelFailureKind::Timeout,
                429 => ModelFailureKind::RateLimited,
                500..=599 => ModelFailureKind::Transport,
                _ => ModelFailureKind::Unsupported,
            };
            self.queue.push_back(Ok(ModelEvent::ResponseError {
                kind,
                metadata: self.decoder.metadata.clone(),
            }));
            self.finished = true;
            return Ok(());
        }
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim);
        if !content_type.is_some_and(|value| value.eq_ignore_ascii_case("text/event-stream")) {
            return Err(error(ErrorCode::InvalidContract, "content_type"));
        }
        if response
            .content_length()
            .is_some_and(|bytes| bytes > self.connection.0.options.max_transport_bytes as u64)
        {
            return Err(error(ErrorCode::InvalidContract, "response_size"));
        }
        self.response = Some(response);
        Ok(())
    }
    fn fail(&mut self, failure: ContractError) {
        self.finished = true;
        self.response = None;
        if matches!(
            failure.code,
            ErrorCode::Cancelled | ErrorCode::AccessDenied | ErrorCode::RequestConflict
        ) {
            self.queue.push_back(Err(failure));
            return;
        }
        let kind = match failure.code {
            ErrorCode::DeadlineExceeded => ModelFailureKind::Timeout,
            ErrorCode::ModelUnavailable => ModelFailureKind::Transport,
            ErrorCode::ModelOptionUnsupported
            | ErrorCode::ModelCapabilityUnsupported
            | ErrorCode::CapabilityUnsupported
            | ErrorCode::ModelBindingInvalid
            | ErrorCode::InvalidConfiguration => ModelFailureKind::Unsupported,
            _ => ModelFailureKind::Protocol,
        };
        self.queue.push_back(Ok(ModelEvent::ResponseError {
            kind,
            metadata: self.decoder.metadata.clone(),
        }));
    }
}
fn transport_error(error_value: reqwest::Error) -> ContractError {
    error(
        if error_value.is_timeout() {
            ErrorCode::DeadlineExceeded
        } else {
            ErrorCode::ModelUnavailable
        },
        "transport",
    )
}
```

## `crates/wickle-model-vertex/tests/agent_contract.rs`

```rust
//! Actual HTTP/SSE through the core: provider projection, repair and system binding.
#[path = "../../wickle/tests/support/agent.rs"]
#[allow(dead_code)]
mod core_host;
#[allow(dead_code)]
mod support;
use core_host::{completed, id, reference, scope};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use wickle::*;
use wickle_model_vertex::*;
const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";

struct Catalog(core_host::Catalog);
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        scope: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            let mut metadata = self.0.resolve(reference, scope).await?;
            if reference.kind == ComponentKind::Tool {
                metadata.model_name = Some(reference.id.clone());
            }
            if let Some(version) = &reference.version {
                metadata.reference.version = Some(version.clone());
            }
            Ok(metadata)
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
        Box::pin(async move {
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: ModelRouteAvailability::Available,
                model_id: Some(route.model_id.clone()),
                model_version: Some(route.model_version.clone()),
                deployment_revision: None,
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id("local-http-fixture"),
            })
        })
    }
}
struct Capture(Mutex<Vec<JsonObject>>);
impl ToolExecutor for Capture {
    fn execute<'a>(
        &'a self,
        call: &'a JsonObject,
        _: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.0.lock().unwrap().push(call.clone());
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: json!("observed"),
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}
fn call_reply(index: usize, arguments: &str) -> support::Reply {
    let placeholder = json!({"replace":"arguments"});
    let part = json!({"functionCall":{"name":"lookup","args":placeholder,"willContinue":false,"partialArgs":[]},"thoughtSignature":format!("signed-{index}")});
    let data = vec![
        json!({"responseId":format!("response-{index}"),"modelVersion":"fixture-release","candidates":[{"index":0,"content":{"role":"model","parts":[part]},"finishReason":"STOP"}]}),
    ];
    let mut reply = support::reply(&data);
    reply.body = String::from_utf8(reply.body)
        .unwrap()
        .replace(&placeholder.to_string(), arguments)
        .into_bytes();
    reply
}
#[tokio::test]
async fn compiled_vertex_contract_repairs_arguments_and_retries_only_through_the_core() {
    let invalid_constraint =
        json!({"query":"latest","limit":7,"note":null,"filter":{"category":"finance"}}).to_string();
    let invalid_json =
        json!({"query":"latest","limit":9,"note":null,"filter":"invalid-shape"}).to_string();
    let valid =
        json!({"query":"latest","limit":9,"note":null,"filter":{"category":"finance"}}).to_string();
    let malformed = "[]";
    let rounded = r#"{"query":"latest","limit":0.12345678901234567890123456789}"#;
    let server = support::Server::new(vec![
        support::Reply::json(503, json!({"error":{"code":503}})),
        call_reply(0, malformed),
        call_reply(4, rounded),
        call_reply(1, &invalid_constraint),
        call_reply(2, &invalid_json),
        call_reply(3, &valid),
        support::reply(&support::events("complete")),
    ])
    .await;
    let connection = VertexConnection::new(
        scope(),
        reference("account"),
        support::token(),
        support::options(&server),
    )
    .unwrap();
    let fixture = core_host::Fixture::new(core_host::Response::Text, false);
    let mut catalog = fixture.router.snapshot.catalog().clone();
    catalog.models[0].provider = id("google-vertex");
    catalog.models[0].model_id = id(support::MODEL);
    catalog.models[0].model_version = id("fixture-release");
    catalog.models[0]
        .capabilities
        .features
        .insert(id("tool_calling"));
    catalog.models[0].capabilities.options_schema = json!({"type":"object","properties":{"thinking_level":{"enum":["low","medium","high"]}},"additionalProperties":false});
    catalog.bindings[0]
        .default_options
        .insert("thinking_level".into(), json!("high"));
    catalog.bindings[0].model = catalog.models[0].reference();
    catalog.bindings[0].requested_model = id(support::MODEL);
    catalog.bindings[0].adapter = connection.binding().adapter;
    catalog.bindings[0].connection_ref = connection.binding().connection_ref;
    catalog.bindings[0].api_contract = connection.api_contract();
    catalog.bindings[0].target = connection.target().clone();
    let properties: serde_json::Map<String, Value> = connection
        .target()
        .iter()
        .map(|(key, value)| (key.clone(), json!({"const":value})))
        .collect();
    let required: Vec<_> = connection.target().keys().cloned().collect();
    catalog.bindings[0].target_schema = json!({"type":"object","properties":properties,"required":required,"additionalProperties":false});
    catalog.bindings[0].capabilities = catalog.models[0].capabilities.clone();
    catalog.bindings[0].evidence[0].binding_digest = catalog.bindings[0]
        .contract_digest(&catalog.models[0])
        .unwrap();
    let snapshot = RoutingSnapshot::new(catalog, fixture.router.snapshot.policy().clone()).unwrap();
    let mut bindings = fixture.bindings();
    bindings.profile_resolver = Arc::new(Catalog(core_host::Catalog::default()));
    bindings.router = Arc::new(core_host::Router {
        snapshot,
        queries: AtomicUsize::new(0),
        snapshots: AtomicUsize::new(0),
    });
    bindings.model_exchange = Arc::new(
        ModelExchange::new(
            Arc::new(VertexModel::new(connection)),
            bindings.policy.clone(),
        )
        .with_route_inspector(Arc::new(Inspector), Duration::from_secs(1))
        .unwrap()
        .with_retry_policy(ModelRetryPolicy {
            max_retries: 1,
            backoff_ms: 0,
        }),
    );
    bindings.system_inputs = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    let tool = SchemaCompiler::new().compile(ToolDescriptor {
        tool: reference("lookup"), name: id("lookup"), description: "Read scoped data".into(),
        input_schema: json!({"type":"object","properties":{
            "query":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":10,"default":7},"note":{"type":["string","null"]},
            "filter":{"type":"object","properties":{"category":{"type":"string"},"term":{"type":"string"}},"required":["category"],"additionalProperties":false},
            "workspace_id":{"type":"string","format":"uuid"}
        },"required":["query","workspace_id"],"additionalProperties":false,"if":{"properties":{"query":{"const":"latest"}}},"then":{"properties":{"limit":{"minimum":8}}}}),
        agent_parameters: vec!["query".into(),"limit".into(),"note".into(),"filter".into()], system_bindings: None,
        output_schema: json!({"type":"string"}), side_effect: ToolSideEffect::ReadOnly, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false, max_output_bytes: 4096.try_into().unwrap(),
    }, &bindings.system_inputs).unwrap();
    let wire_definition = tool.to_model_tool();
    let capture = Arc::new(Capture(Mutex::new(vec![])));
    bindings.tools = Some(Arc::new(
        ToolRegistry::new(
            scope(),
            vec![ToolRegistration {
                compiled: tool,
                executor: capture.clone(),
            }],
        )
        .unwrap(),
    ));
    let mut profile = core_host::profile();
    profile.limits.max_model_calls = 8.try_into().unwrap();
    profile.limits.max_recovery_attempts = 1;
    profile
        .model_options
        .insert("thinking_level".into(), json!("low"));
    profile.limits.max_tool_attempts = 2;
    profile.limits.max_repair_attempts = 4;
    profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
        tool_id: id("lookup"),
        version: id("1"),
        bindings: None,
        config: None,
    }));
    let agent = create_agent(profile, bindings).unwrap();
    let mut context = core_host::context();
    context.data.system_inputs = Some(SystemInputs::new(
        [("workspace_id".into(), json!(WORKSPACE))].into(),
    ));
    let mut request = core_host::request("contract");
    request
        .model_options
        .insert("thinking_level".into(), json!("medium"));
    let handle = completed(agent.start(request, context.clone()).await.unwrap());
    let outcome = completed(
        tokio::time::timeout(Duration::from_secs(10), handle.outcome(&context))
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        outcome.result.status(),
        RunStatus::Succeeded,
        "{:?}",
        outcome.result
    );
    assert_eq!(outcome.usage.repair_attempts, 4);
    assert_eq!(outcome.usage.recovery_attempts, 1);
    assert_eq!(outcome.usage.model_calls, 7);
    assert_eq!(outcome.usage.tool_attempts, 1);
    assert_eq!(
        capture.0.lock().unwrap().as_slice(),
        &[JsonObject::from([
            ("query".into(), json!("latest")),
            ("limit".into(), json!(9)),
            ("note".into(), Value::Null),
            ("filter".into(), json!({"category":"finance"})),
            ("workspace_id".into(), json!(WORKSPACE))
        ])]
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 7);
    assert_eq!(requests[0].body, requests[1].body);
    for request in requests.iter() {
        let declaration = &request.body["tools"][0]["functionDeclarations"][0];
        let schema = &declaration["parametersJsonSchema"];
        assert_eq!(schema["required"], json!(["query"]));
        assert!(schema["properties"].get("workspace_id").is_none());
        assert!(
            request
                .path
                .contains("/v1/projects/test-project/locations/global/publishers/google/models/")
        );
        assert!(
            request
                .headers
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-token")
        );
        assert!(
            request
                .headers
                .to_ascii_lowercase()
                .contains("x-goog-user-project: billing-project")
        );
        assert_eq!(
            request.body["generationConfig"]["thinkingConfig"]["thinkingLevel"],
            "MEDIUM"
        );
        assert_eq!(
            request.body["toolConfig"]["functionCallingConfig"]["streamFunctionCallArguments"],
            false
        );
        assert!(!request.body.to_string().contains(WORKSPACE));
    }
    // The provider-owned signed parts are replayed without replacing bad inputs.
    let contents = requests[6].body["contents"].as_array().unwrap();
    let parts: Vec<_> = contents
        .iter()
        .filter(|item| item["role"] == "model")
        .flat_map(|item| item["parts"].as_array().unwrap())
        .collect();
    assert_eq!(parts.len(), 5);
    assert_eq!(parts[0]["functionCall"]["args"], json!([]));
    assert_eq!(parts[0]["thoughtSignature"], "signed-0");
    assert_eq!(parts[0]["functionCall"]["willContinue"], false);
    assert_eq!(parts[0]["functionCall"]["partialArgs"], json!([]));
    assert_eq!(
        parts[4]["functionCall"]["args"],
        parse_json(&valid).unwrap()
    );
    assert!(requests[6].raw_body.contains(rounded));
    assert!(wire_definition.model_input_schema.get("if").is_some());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
}
```
