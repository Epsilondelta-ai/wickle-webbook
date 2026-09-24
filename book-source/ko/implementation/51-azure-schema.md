# 51장 전체 구현과 변경 검사

[강의](../51-azure-schema.md) · [전체 변경 패치](../solutions/51-azure-schema.patch)

기준 `c81215a7a4b4872366b9491ed3cb0751e4610820`. 이 단계에서 바뀐 Rust·manifest·Python 파일의 전체 내용이다. 이전 버전과의 정확한 교체 위치·삭제는 patch를 따른다. 다음 장의 코드와 섞지 않는다.

## `crates/wickle-model-azure-openai/src/model.rs`

```rust
use crate::{AzureAudience, AzureCredentialContext, AzureOpenAiConnection, auth, error};
use futures_util::stream;
use reqwest::Response;
use serde_json::Value;
use std::collections::VecDeque;
use wickle::*;
use wickle_model_responses::{ResponsesDecoder, SseDecoder, encode_request};

/// One Azure OpenAI Responses POST per invocation, streamed into Wickle model events.
/// The adapter does not retry, run Tool handlers, or load environment variables.
#[derive(Debug, Clone)]
pub struct AzureOpenAiModel {
    connection: AzureOpenAiConnection,
}
impl AzureOpenAiModel {
    /// Bind an already configured connection without making a network request.
    pub fn new(connection: AzureOpenAiConnection) -> Self {
        Self { connection }
    }
}
impl ModelPort for AzureOpenAiModel {
    fn tool_schema_compiler(&self) -> std::sync::Arc<dyn ProviderToolSchemaCompiler> {
        std::sync::Arc::new(wickle_model_responses::AzureResponsesToolSchemaCompiler)
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
            decoder: ResponsesDecoder::new(request, None),
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
    connection: &'a AzureOpenAiConnection,
    request: &'a ModelRequest,
    context: &'a ModelCallContext,
    response: Option<Response>,
    decoder: ResponsesDecoder<'a>,
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
        let mut value = encode_request(self.request)?;
        // The wire selects a deployment; the core route retains its underlying
        // model/release and the original route-bound opaque continuation.
        value["model"] = serde_json::json!(self.connection.0.options.deployment);
        let body =
            serde_json::to_vec(&value).map_err(|_| error(ErrorCode::InvalidJson, "request"))?;
        if body.len() > self.request.limits.max_input_bytes {
            return Err(error(ErrorCode::ModelCapabilityUnsupported, "request_size"));
        }
        let url = self
            .connection
            .0
            .base
            .join("responses")
            .map_err(|_| error(ErrorCode::InvalidConfiguration, "responses_url"))?;
        let headers = match auth::authorize(
            self.connection.0.credentials.as_ref(),
            AzureCredentialContext {
                scope: &self.context.scope,
                audience: AzureAudience::Inference,
                cancellation: &self.context.cancellation,
                deadline: self.context.deadline,
            },
        )
        .await
        {
            Ok(headers) => headers,
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
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
            .header("accept-encoding", "identity")
            .body(body)
            .send();
        let mut response = tokio::select! { biased;
            _ = self.context.cancellation.cancelled() => return Err(error(ErrorCode::Cancelled, "request")),
            _ = tokio::time::sleep_until(self.context.deadline) => return Err(error(ErrorCode::DeadlineExceeded, "request")),
            result = operation => result.map_err(transport_error)?,
        };
        self.decoder.metadata.provider_request_id = response
            .headers()
            .get("apim-request-id")
            .or_else(|| response.headers().get("x-request-id"))
            .map(|value| {
                value
                    .to_str()
                    .map_err(|_| error(ErrorCode::InvalidContract, "request_id"))
                    .and_then(Id::new)
            })
            .transpose()?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let mut kind = match status {
                401 | 403 => ModelFailureKind::Authentication,
                404 => ModelFailureKind::Unavailable,
                408 | 504 => ModelFailureKind::Timeout,
                429 => ModelFailureKind::RateLimited,
                500..=599 => ModelFailureKind::Transport,
                _ => ModelFailureKind::Unsupported,
            };
            if status == 400 {
                let mut bytes = vec![];
                loop {
                    let chunk = tokio::select! { biased;
                        _ = self.context.cancellation.cancelled() => return Err(error(ErrorCode::Cancelled, "error_body")),
                        _ = tokio::time::sleep_until(self.context.deadline) => return Err(error(ErrorCode::DeadlineExceeded, "error_body")),
                        chunk = response.chunk() => chunk.map_err(transport_error)?,
                    };
                    let Some(chunk) = chunk else {
                        break;
                    };
                    if bytes.len().saturating_add(chunk.len())
                        > 65_536.min(self.connection.0.options.max_transport_bytes)
                    {
                        break;
                    }
                    bytes.extend_from_slice(&chunk);
                }
                if let Ok(value) = std::str::from_utf8(&bytes)
                    .map_err(|_| ())
                    .and_then(|text| parse_json(text).map_err(|_| ()))
                {
                    if value.pointer("/error/code").and_then(Value::as_str)
                        == Some("context_length_exceeded")
                    {
                        kind = ModelFailureKind::ContextOverflow;
                    }
                }
            }
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

## `crates/wickle-model-azure-openai/tests/agent_contract.rs`

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
use wickle_model_azure_openai::*;
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
fn call_events(index: usize, arguments: &str) -> Vec<Value> {
    let response = format!("resp_{index}");
    let item = format!("fc_{index}");
    let call = format!("call_{index}");
    let output = json!({"id":item,"type":"function_call","call_id":call,"name":"lookup","arguments":arguments,"status":"completed"});
    vec![
        json!({"type":"response.created","response":{"id":response,"model":"model","status":"in_progress"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":item,"type":"function_call","call_id":call,"name":"lookup","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":0,"item_id":item,"delta":arguments}),
        json!({"type":"response.function_call_arguments.done","output_index":0,"item_id":item,"name":"lookup","arguments":arguments}),
        json!({"type":"response.output_item.done","output_index":0,"item":output}),
        json!({"type":"response.completed","response":{"id":response,"model":"model","status":"completed","output":[output]}}),
    ]
}
#[tokio::test]
async fn unsupported_constraints_and_invalid_json_text_repair_before_system_binding_or_execution() {
    exercise(false).await;
}
#[tokio::test]
async fn packed_argument_objects_repair_and_replay_before_one_scoped_execution() {
    exercise(true).await;
}
async fn exercise(packed: bool) {
    let mut invalid_constraint = json!({"query":"latest","limit":{"present":true,"value":7},"note":{"present":true,"value":null},"filter":"[ {\"category\": \"finance\"} ]"}).to_string();
    let mut invalid_json = json!({"query":"latest","limit":{"present":true,"value":9},"note":{"present":true,"value":null},"filter":"[not json"}).to_string();
    let mut valid = json!({"query":"latest","limit":{"present":true,"value":9},"note":{"present":true,"value":null},"filter":"[ {\"category\": \"finance\"} ]"}).to_string();
    if packed {
        invalid_constraint = json!({"arguments":json!({"query":"latest","limit":7,"note":null,"filter":{"category":"finance"}}).to_string()}).to_string();
        invalid_json = json!({"arguments":"[not json"}).to_string();
        valid = json!({"arguments":json!({"query":"latest","limit":9,"note":null,"filter":{"category":"finance"}}).to_string()}).to_string();
    }
    let malformed = "{not json";
    let rounded = r#"{"query":"latest","limit":0.12345678901234567890123456789}"#;
    let server = support::Server::new(vec![
        support::Reply::sse(&call_events(0, malformed)),
        support::Reply::sse(&call_events(4, rounded)),
        support::Reply::sse(&call_events(1, &invalid_constraint)),
        support::Reply::sse(&call_events(2, &invalid_json)),
        support::Reply::sse(&call_events(3, &valid)),
        support::Reply::sse(&support::events("model", "complete")),
    ])
    .await;
    let connection = AzureOpenAiConnection::new(
        scope(),
        reference("account"),
        Arc::new(AzureCredential::ApiKey("fixture-key-not-a-secret".into())),
        AzureOpenAiOptions::new(server.base.trim_end_matches("v1/"), "test-deployment"),
    )
    .unwrap();
    let fixture = core_host::Fixture::new(core_host::Response::Text, false);
    let mut catalog = fixture.router.snapshot.catalog().clone();
    catalog.models[0].provider = id("azure-openai");
    catalog.models[0].model_id = id("model");
    catalog.models[0]
        .capabilities
        .features
        .insert(id("tool_calling"));
    catalog.bindings[0].model = catalog.models[0].reference();
    catalog.bindings[0].requested_model = id("model");
    catalog.bindings[0].adapter = connection.binding().adapter;
    catalog.bindings[0].connection_ref = connection.binding().connection_ref;
    catalog.bindings[0].api_contract = AzureOpenAiConnection::api_contract();
    catalog.bindings[0].target = connection.target().clone();
    catalog.bindings[0].target_schema = json!({"type":"object","properties":{"endpoint":{"type":"string"},"deployment":{"type":"string"}},"required":["endpoint","deployment"],"additionalProperties":false});
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
            Arc::new(AzureOpenAiModel::new(connection)),
            bindings.policy.clone(),
        )
        .with_route_inspector(Arc::new(Inspector), Duration::from_secs(1))
        .unwrap(),
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
    let tool = if packed {
        let mut descriptor = tool.descriptor().clone();
        for index in 0..97 {
            let name = format!("optional{index}");
            descriptor.input_schema["properties"][&name] = json!({"type":"string"});
            descriptor.agent_parameters.push(name);
        }
        SchemaCompiler::new()
            .compile(descriptor, &bindings.system_inputs)
            .unwrap()
    } else {
        tool
    };
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
    profile.limits.max_model_calls = 7.try_into().unwrap();
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
    let handle = completed(
        agent
            .start(core_host::request("contract"), context.clone())
            .await
            .unwrap(),
    );
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
    assert_eq!(requests.len(), 6);
    for request in requests.iter() {
        assert_eq!(request.body["tools"][0]["strict"], true);
        if packed {
            assert_eq!(
                request.body["tools"][0]["parameters"]["required"],
                json!(["arguments"])
            );
        }
        assert_eq!(request.body["model"], "test-deployment");
        assert_eq!(request.body["parallel_tool_calls"], false);
        assert!(request.path.starts_with("/openai/v1/responses"));
        assert!(!request.body.to_string().contains(WORKSPACE));
    }
    let replayed: Vec<_> = requests[5].body["input"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["type"] == "function_call")
        .map(|item| item["arguments"].as_str().unwrap())
        .collect();
    assert_eq!(
        replayed,
        vec![
            malformed,
            rounded,
            invalid_constraint.as_str(),
            invalid_json.as_str(),
            valid.as_str()
        ]
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
}
```

## `crates/wickle-model-azure-openai/tests/responses.rs`

```rust
//! Azure inference/authentication and deployment metadata transport contracts.
mod support;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::*;
use wickle::*;
use wickle_model_azure_openai::*;

struct Tokens(AtomicUsize);
impl AzureCredentialProvider for Tokens {
    fn credential<'a>(
        &'a self,
        context: &'a AzureCredentialContext<'a>,
    ) -> PortFuture<'a, AzureCredential> {
        Box::pin(async move {
            assert_eq!(context.scope, &scope());
            assert_eq!(context.audience, AzureAudience::Inference);
            let number = self.0.fetch_add(1, Ordering::SeqCst);
            Ok(AzureCredential::EntraToken(format!("token-{number}")))
        })
    }
}

#[tokio::test]
async fn deployment_mapping_preserves_model_identity_and_refreshes_entra_per_request() {
    for entra in [false, true] {
        let mut reply = Reply::sse(&events("base-model", "42"));
        reply.headers = vec![("apim-request-id", "azure-request".into())];
        let server = Server::new(vec![reply, Reply::sse(&events("base-model", "43"))]).await;
        let tokens = Arc::new(Tokens(AtomicUsize::new(0)));
        let credentials: Arc<dyn AzureCredentialProvider> = if entra {
            tokens.clone()
        } else {
            Arc::new(AzureCredential::ApiKey("fixture-key".into()))
        };
        let connection = AzureOpenAiConnection::new(
            scope(),
            reference("account"),
            credentials,
            options(&server),
        )
        .unwrap();
        let model = AzureOpenAiModel::new(connection.clone());
        let mut request = request(&connection, "base-model");
        let original = request.route.clone();
        for answer in ["42", "43"] {
            let response =
                collect_model_response(&request, model.generate(&request, &context(&request)))
                    .await
                    .unwrap();
            assert_eq!(response.text, answer);
            assert_eq!(response.metadata.reported_model_id, Some(id("base-model")));
            assert!(response.metadata.reported_model_version.is_none());
            assert_eq!(response.metadata.usage.unwrap().output_tokens, Some(7));
            if answer == "42" {
                assert_eq!(
                    response.metadata.provider_request_id,
                    Some(id("azure-request"))
                );
            }
            request.request_id = id("second-attempt");
        }
        assert_eq!(request.route, original);
        assert_eq!(tokens.0.load(Ordering::SeqCst), if entra { 2 } else { 0 });
        let calls = server.requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        for (index, call) in calls.iter().enumerate() {
            assert_eq!(call.method, "POST");
            assert_eq!(call.path, "/openai/v1/responses");
            assert_eq!(call.body["model"], "finance-deployment");
            assert_eq!(call.body["reasoning"]["effort"], "medium");
            assert_eq!(call.body["store"], false);
            let headers = call.headers.to_ascii_lowercase();
            if entra {
                assert!(headers.contains(&format!("authorization: bearer token-{index}")));
                assert!(!headers.contains("api-key:"));
            } else {
                assert!(headers.contains("api-key: fixture-key"));
                assert!(!headers.contains("authorization:"));
            }
            assert!(!call.body.to_string().contains("hidden-workspace"));
            assert!(!call.body.to_string().contains("fixture-key"));
        }
    }
}

#[tokio::test]
async fn wrong_scope_route_api_and_credentials_never_make_an_http_request() {
    for case in [
        "scope",
        "target",
        "provider",
        "api",
        "options",
        "credential",
    ] {
        let server = Server::new(vec![]).await;
        let tokens = Arc::new(Tokens(AtomicUsize::new(0)));
        let credentials: Arc<dyn AzureCredentialProvider> = if case == "credential" {
            Arc::new(AzureCredential::ApiKey("bad\nkey".into()))
        } else {
            tokens.clone()
        };
        let connection = AzureOpenAiConnection::new(
            scope(),
            reference("account"),
            credentials,
            options(&server),
        )
        .unwrap();
        let model = AzureOpenAiModel::new(connection.clone());
        let mut request = request(&connection, "base-model");
        let mut context = context(&request);
        match case {
            "scope" => context.scope.workspace_id = id("another-workspace"),
            "target" => {
                request
                    .route
                    .target
                    .insert("deployment".into(), json!("another-deployment"));
            }
            "provider" => request.route.provider = id("openai"),
            "api" => request.route.api_contract.version = id("2025-04-01-preview"),
            "options" => {
                request.options.insert("model".into(), json!("injected"));
            }
            _ => {}
        }
        let failure = collect_model_response(&request, model.generate(&request, &context))
            .await
            .unwrap_err();
        if case == "credential" {
            assert_eq!(failure.kind, ModelFailureKind::Authentication);
        }
        assert!(server.requests.lock().unwrap().is_empty());
        assert_eq!(tokens.0.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn azure_tools_and_json_output_keep_original_route_bound_replay() {
    let call = json!({"id":"fc","type":"function_call","call_id":"call-1","name":"lookup","arguments":"{\"query\":\"figures\"}","status":"completed"});
    let first = vec![
        json!({"type":"response.created","response":{"id":"resp_1","model":"base-model","status":"in_progress"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"fc","type":"function_call","call_id":"call-1","name":"lookup","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":0,"item_id":"fc","delta":"{\"query\":\"figures\"}"}),
        json!({"type":"response.completed","response":{"id":"resp_1","model":"base-model","status":"completed","output":[call]}}),
    ];
    let server = Server::new(vec![
        Reply::sse(&first),
        Reply::sse(&events("base-model", "{\"answer\":42}")),
    ])
    .await;
    let connection = connection(&server);
    let model = AzureOpenAiModel::new(connection.clone());
    let mut request = request(&connection, "base-model");
    request.tools = vec![ModelTool {
        name: id("lookup"),
        description: "Read figures".into(),
        model_input_schema: json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}),
    }];
    let first = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(first.finish, ModelFinish::ToolCalls);
    assert_eq!(first.tool_calls[0].model_inputs["query"], "figures");
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![
            ModelContent::ToolCall {
                provider_call_id: id("call-1"),
                name: id("lookup"),
                arguments: JsonObject::from([("query".into(), json!("figures"))]),
            },
            ModelContent::Opaque {
                continuation: first.continuation[0].clone(),
            },
        ],
    });
    request.messages.push(ModelMessage {
        role: ModelRole::Tool,
        content: vec![ModelContent::ToolResult {
            provider_call_id: id("call-1"),
            content: json!({"answer":42}),
        }],
    });
    request.output = ModelOutput::JsonSchema {
        schema: json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}),
    };
    let response = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(parse_json(&response.text).unwrap(), json!({"answer":42}));
    let calls = server.requests.lock().unwrap();
    let input = calls[1].body["input"].as_array().unwrap();
    assert_eq!(
        input
            .iter()
            .filter(|value| value["type"] == "function_call")
            .count(),
        1
    );
    assert_eq!(calls[1].body["model"], "finance-deployment");
    assert_eq!(calls[1].body["text"]["format"]["strict"], true);
    assert_eq!(calls[0].body["tools"][0]["strict"], true);
}

fn inspection_context() -> ModelInspectionContext {
    ModelInspectionContext {
        scope: scope(),
        principal_ref: id("user"),
        capability_grant_ref: id("grant"),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(3),
    }
}
fn deployment(version: &str) -> Value {
    json!({"id":format!("{ACCOUNT}/deployments/finance-deployment"),"name":"finance-deployment","type":"Microsoft.CognitiveServices/accounts/deployments","properties":{"model":{"format":"OpenAI","name":"base-model","version":version},"provisioningState":"Succeeded","versionUpgradeOption":"OnceNewDefaultVersionAvailable"}})
}
fn inspector(
    connection: AzureOpenAiConnection,
    server: &Server,
    credentials: AzureCredential,
) -> AzureOpenAiInspector {
    AzureOpenAiInspector::new(
        connection,
        Arc::new(credentials),
        AzureInspectionOptions {
            endpoint: origin(server),
            ..Default::default()
        },
    )
    .unwrap()
}

#[tokio::test]
async fn arm_inspection_detects_model_upgrade_without_claiming_deployment_immutability() {
    let inference = Server::new(vec![]).await;
    let management = Server::new(vec![
        Reply::json(200, deployment("release")),
        Reply::json(200, deployment("next-release")),
    ])
    .await;
    let connection = connection(&inference);
    let mut request = request(&connection, "base-model");
    let inspector = inspector(
        connection,
        &management,
        AzureCredential::EntraToken("management-token".into()),
    );
    let first = inspector
        .inspect(&request.route, &inspection_context())
        .await
        .unwrap();
    assert_eq!(first.model_version, Some(id("release")));
    assert_eq!(first.version_semantics, VersionSemantics::MutableDeployment);
    first
        .validate(&request.route, VersionPolicy::AllowMutable)
        .unwrap();
    assert_eq!(
        first
            .validate(&request.route, VersionPolicy::RequirePinned)
            .unwrap_err()
            .code,
        ErrorCode::ModelVersionUnpinned
    );
    request.route.deployment_revision = first.deployment_revision.clone();
    let changed = inspector
        .inspect(&request.route, &inspection_context())
        .await
        .unwrap();
    assert_eq!(changed.model_version, Some(id("next-release")));
    assert_ne!(changed.deployment_revision, first.deployment_revision);
    assert_eq!(
        changed
            .validate(&request.route, VersionPolicy::AllowMutable)
            .unwrap_err()
            .code,
        ErrorCode::ModelVersionDrift
    );
    assert!(inference.requests.lock().unwrap().is_empty());
    for call in management.requests.lock().unwrap().iter() {
        assert_eq!(call.method, "GET");
        assert_eq!(
            call.path,
            format!("{ACCOUNT}/deployments/finance-deployment?api-version=2025-06-01")
        );
        assert!(
            call.headers
                .to_ascii_lowercase()
                .contains("authorization: bearer management-token")
        );
        assert!(!call.headers.contains("fixture-key"));
    }
}

#[tokio::test]
async fn metadata_unavailability_mismatch_and_wrong_audience_do_not_invent_a_version() {
    for case in [
        "missing",
        "wrong-target",
        "version-missing",
        "provisioning",
        "api-key",
    ] {
        let mut body = deployment("release");
        if case == "wrong-target" {
            body["id"] = json!(format!("{ACCOUNT}/deployments/other"));
        }
        if case == "version-missing" {
            body["properties"]["model"]
                .as_object_mut()
                .unwrap()
                .remove("version");
        }
        if case == "provisioning" {
            body["properties"]["provisioningState"] = json!("Updating");
        }
        let server = Server::new(vec![Reply::json(
            if case == "missing" { 404 } else { 200 },
            body,
        )])
        .await;
        let connection = connection(&server);
        let request = request(&connection, "base-model");
        let credential = if case == "api-key" {
            AzureCredential::ApiKey("data-plane-key".into())
        } else {
            AzureCredential::EntraToken("management-token".into())
        };
        let result = inspector(connection, &server, credential)
            .inspect(&request.route, &inspection_context())
            .await;
        match case {
            "missing" => {
                let value = result.unwrap();
                assert_eq!(value.availability, ModelRouteAvailability::Unavailable);
                assert!(value.model_version.is_none());
            }
            "provisioning" => assert_eq!(
                result.unwrap().availability,
                ModelRouteAvailability::Unknown
            ),
            _ => assert!(result.is_err(), "{case}"),
        }
        if case == "api-key" {
            assert!(server.requests.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn azure_errors_and_redirects_do_not_retry_or_forward_credentials() {
    let redirected = Server::new(vec![]).await;
    for (status, expected) in [
        (401, ModelFailureKind::Authentication),
        (429, ModelFailureKind::RateLimited),
        (503, ModelFailureKind::Transport),
        (307, ModelFailureKind::Unsupported),
    ] {
        let mut reply = Reply::json(status, json!({"error":{"message":"private diagnostics"}}));
        if status == 307 {
            reply
                .headers
                .push(("location", format!("{}responses", redirected.base)));
        }
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let request = request(&connection, "base-model");
        let model = AzureOpenAiModel::new(connection);
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, expected);
        assert!(!format!("{failure:?}").contains("private diagnostics"));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
    assert!(redirected.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancelled_or_truncated_streams_never_become_a_completed_response() {
    for cancel in [false, true] {
        let mut data = events("base-model", "partial");
        data.truncate(3);
        let mut reply = Reply::sse(&data);
        reply.stall = cancel;
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let request = request(&connection, "base-model");
        let model = AzureOpenAiModel::new(connection);
        let context = context(&request);
        let mut stream = model.generate(&request, &context);
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ModelEvent::TextDelta { .. }
        ));
        server.entered.notified().await;
        if cancel {
            context.cancellation.cancel();
        }
        let terminal = stream.next().await.unwrap();
        if cancel {
            assert_eq!(terminal.unwrap_err().code, ErrorCode::Cancelled);
        } else {
            assert!(matches!(
                terminal.unwrap(),
                ModelEvent::ResponseError {
                    kind: ModelFailureKind::Protocol,
                    ..
                }
            ));
        }
        assert!(stream.next().await.is_none());
        tokio::time::timeout(Duration::from_secs(1), server.closed.notified())
            .await
            .unwrap();
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

struct PendingCredential(tokio::sync::Notify);
impl AzureCredentialProvider for PendingCredential {
    fn credential<'a>(
        &'a self,
        _: &'a AzureCredentialContext<'a>,
    ) -> PortFuture<'a, AzureCredential> {
        Box::pin(async move {
            self.0.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_and_deadline_bound_host_token_refresh_before_http() {
    for cancel in [false, true] {
        let server = Server::new(vec![]).await;
        let credentials = Arc::new(PendingCredential(tokio::sync::Notify::new()));
        let connection = AzureOpenAiConnection::new(
            scope(),
            reference("account"),
            credentials.clone(),
            options(&server),
        )
        .unwrap();
        let model = AzureOpenAiModel::new(connection.clone());
        let request = request(&connection, "base-model");
        let mut context = context(&request);
        context.deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let mut stream = model.generate(&request, &context);
        let mut next = Box::pin(stream.next());
        tokio::select! {
            _ = credentials.0.notified() => {},
            value = &mut next => panic!("refresh completed unexpectedly: {value:?}"),
        }
        if cancel {
            context.cancellation.cancel();
        } else {
            tokio::time::advance(Duration::from_secs(2)).await;
        }
        let value = next.await.unwrap();
        if cancel {
            assert_eq!(value.unwrap_err().code, ErrorCode::Cancelled);
        } else {
            assert!(matches!(
                value.unwrap(),
                ModelEvent::ResponseError {
                    kind: ModelFailureKind::Timeout,
                    ..
                }
            ));
        }
        assert!(stream.next().await.is_none());
        assert!(server.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn arm_scope_and_target_rejection_precedes_credential_lookup() {
    for wrong_scope in [true, false] {
        let server = Server::new(vec![]).await;
        let connection = connection(&server);
        let mut request = request(&connection, "base-model");
        let credentials = Arc::new(Tokens(AtomicUsize::new(0)));
        let inspector = AzureOpenAiInspector::new(
            connection,
            credentials.clone(),
            AzureInspectionOptions {
                endpoint: origin(&server),
                ..Default::default()
            },
        )
        .unwrap();
        let mut context = inspection_context();
        if wrong_scope {
            context.scope.workspace_id = id("different-workspace");
        } else {
            request
                .route
                .target
                .insert("deployment".into(), json!("different-deployment"));
        }
        assert!(inspector.inspect(&request.route, &context).await.is_err());
        assert_eq!(credentials.0.load(Ordering::SeqCst), 0);
        assert!(server.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn two_documented_releases_keep_deployment_and_model_identity_distinct() {
    let releases = ["gpt-6-astra", "gpt-5.6-sol"];
    let server = Server::new(
        releases
            .iter()
            .map(|release| Reply::sse(&events(release, release)))
            .collect(),
    )
    .await;
    let mut bindings = vec![];
    for (index, release) in releases.iter().enumerate() {
        let mut options = options(&server);
        options.deployment = format!("deployment-{index}");
        let connection = AzureOpenAiConnection::new(
            scope(),
            reference(&format!("account-{index}")),
            Arc::new(AzureCredential::ApiKey("fixture-key".into())),
            options,
        )
        .unwrap();
        let mut request = request(&connection, release);
        request.route.model_version = id(if index == 0 {
            "2026-09-03"
        } else {
            "2026-07-09"
        });
        bindings.push((AzureOpenAiModel::new(connection), request));
    }
    for ((model, request), release) in bindings.iter().zip(releases) {
        let result = collect_model_response(request, model.generate(request, &context(request)))
            .await
            .unwrap();
        assert_eq!(result.text, release);
        assert_eq!(result.metadata.reported_model_id, Some(id(release)));
        assert!(result.metadata.reported_model_version.is_none());
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (index, request) in requests.iter().enumerate() {
        assert_eq!(request.body["model"], format!("deployment-{index}"));
        assert_eq!(request.path, "/openai/v1/responses");
    }
}

fn compiled_tool(schema: Value) -> CompiledTool {
    let parameters = schema["properties"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    SchemaCompiler::new()
        .compile(
            ToolDescriptor {
                tool: reference("lookup"),
                name: id("lookup"),
                description: "Read data".into(),
                input_schema: schema,
                agent_parameters: parameters,
                system_bindings: None,
                output_schema: json!({"type":"string"}),
                side_effect: ToolSideEffect::ReadOnly,
                concurrency: ToolConcurrency::Serial,
                retry: ToolRetryPolicy::Never,
                reconcile: false,
                max_output_bytes: 4096.try_into().unwrap(),
            },
            &SystemInputRegistry::new(vec![]).unwrap(),
        )
        .unwrap()
}

#[tokio::test]
async fn azure_projection_respects_its_native_limits_and_preserves_validation_and_release_identity()
{
    let server = Server::new(vec![Reply::sse(&events("base-model", "done"))]).await;
    let connection = connection(&server);
    let model = AzureOpenAiModel::new(connection.clone());
    let mut request = request(&connection, "base-model");
    let original = compiled_tool(
        json!({"type":"object","properties":{"query":{"type":"string","pattern":"^[a-z]+$","minLength":2}},"required":["query"],"additionalProperties":false}),
    );
    let target = ProviderToolTarget::for_route(&request.route);
    let contract = CompiledToolContract::compile(
        &original,
        target.clone(),
        model.tool_schema_compiler().as_ref(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        contract.wire_tool().model_input_schema["properties"]["query"],
        json!({"type":"string"})
    );
    let decoded = contract
        .decode_arguments(r#"{"query":"INVALID"}"#, Default::default())
        .unwrap();
    assert!(original.validate_model_inputs(&decoded).is_err());
    assert!(
        contract
            .enforcement()
            .iter()
            .any(
                |entry| entry.canonical_pointer == "/properties/query/pattern"
                    && entry.core
                    && entry.context_text
                    && !entry.provider_native
            )
    );
    let saved = serde_json::to_string(&contract).unwrap();
    CompiledToolContract::restore(
        &saved,
        &original,
        &target,
        contract.digest(),
        Default::default(),
    )
    .unwrap();
    request.route.model_version = id("new-release");
    assert!(
        CompiledToolContract::restore(
            &saved,
            &original,
            &ProviderToolTarget::for_route(&request.route),
            contract.digest(),
            Default::default()
        )
        .is_err()
    );
    request.route.model_version = id("release");
    request.tools = vec![contract.wire_tool().clone()];
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let calls = server.requests.lock().unwrap();
    assert_eq!(calls[0].body["tools"][0]["strict"], true);
    assert_eq!(
        calls[0].body["tools"][0]["parameters"],
        contract.wire_tool().model_input_schema
    );
    assert_eq!(calls[0].body["model"], "finance-deployment");
}

#[tokio::test]
async fn azure_compacts_wide_and_deep_values_without_narrowing_the_canonical_shape() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let model = AzureOpenAiModel::new(connection.clone());
    let target = ProviderToolTarget::for_route(&request(&connection, "base-model").route);
    let properties: serde_json::Map<String, Value> = (0..100)
        .map(|i| (format!("field{i}"), json!({"type":"string"})))
        .collect();
    let required: Vec<_> = properties.keys().cloned().collect();
    let wide = json!({"type":"object","properties":properties,"required":required,"additionalProperties":false});
    let wide_value: serde_json::Map<String, Value> = (0..100)
        .map(|i| (format!("field{i}"), json!("value")))
        .collect();
    let mut deep =
        json!({"type":"object","properties":{},"required":[],"additionalProperties":false});
    let mut deep_value = json!({});
    for _ in 0..5 {
        deep = json!({"type":"object","properties":{"child":deep},"required":["child"],"additionalProperties":false});
        deep_value = json!({"child":deep_value});
    }
    for (schema, value) in [(wide, Value::Object(wide_value)), (deep, deep_value)] {
        let original = compiled_tool(
            json!({"type":"object","properties":{"query":schema},"required":["query"],"additionalProperties":false}),
        );
        let azure = CompiledToolContract::compile(
            &original,
            target.clone(),
            model.tool_schema_compiler().as_ref(),
            Default::default(),
        )
        .unwrap();
        let openai = CompiledToolContract::compile(
            &original,
            target.clone(),
            &wickle_model_responses::ResponsesToolSchemaCompiler,
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            openai.wire_tool().model_input_schema,
            *original.model_input_schema()
        );
        assert_eq!(
            azure.wire_tool().model_input_schema["properties"]["query"],
            json!({"type":"string"})
        );
        assert_ne!(azure.compiler(), openai.compiler());
        let canonical = JsonObject::from([("query".into(), value)]);
        let encoded = azure.encode_arguments(&canonical).unwrap();
        let decoded = azure
            .decode_arguments(
                &serde_json::to_string(&encoded).unwrap(),
                Default::default(),
            )
            .unwrap();
        assert_eq!(decoded, canonical);
        original.validate_model_inputs(&decoded).unwrap();
        let invalid = azure
            .decode_arguments(r#"{"query":"42"}"#, Default::default())
            .unwrap();
        assert!(original.validate_model_inputs(&invalid).is_err());
    }
}

#[tokio::test]
async fn top_level_property_limit_uses_one_json_object_without_exposing_system_fields() {
    let server = Server::new(vec![
        Reply::sse(&events("base-model", "done")),
        Reply::sse(&events("base-model", "done")),
    ])
    .await;
    let connection = connection(&server);
    let model = AzureOpenAiModel::new(connection.clone());
    for count in [100, 101] {
        let properties: serde_json::Map<String, Value> = (0..count)
            .map(|i| (format!("field{i}"), json!({"type":["string","null"]})))
            .collect();
        let mut required: Vec<_> = properties.keys().cloned().collect();
        // Omission/null preservation is checked on the packed case.
        if count == 101 {
            required.retain(|name| name != "field0");
        }
        let original = compiled_tool(
            json!({"type":"object","properties":properties,"required":required,"additionalProperties":false}),
        );
        let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
            key: id("workspace_id"),
            version: id("1"),
            value_schema: json!({"type":"string","format":"uuid"}),
            source: SystemInputSource::Run {},
        }])
        .unwrap();
        let mut descriptor = original.descriptor().clone();
        descriptor.input_schema["properties"]["workspace_id"] =
            json!({"type":"string","format":"uuid","description":"hidden-system-schema"});
        descriptor.input_schema["required"]
            .as_array_mut()
            .unwrap()
            .push(json!("workspace_id"));
        let original = SchemaCompiler::new()
            .compile(descriptor, &registry)
            .unwrap();
        let mut request = request(&connection, "base-model");
        let target = ProviderToolTarget::for_route(&request.route);
        let contract = CompiledToolContract::compile(
            &original,
            target.clone(),
            model.tool_schema_compiler().as_ref(),
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            contract.wire_tool().model_input_schema["properties"]
                .as_object()
                .unwrap()
                .len(),
            if count == 100 { 100 } else { 1 }
        );
        let canonical: JsonObject = (0..count)
            .filter(|i| count == 100 || *i != 0)
            .map(|i| {
                (
                    format!("field{i}"),
                    if i == 1 { Value::Null } else { json!("value") },
                )
            })
            .collect();
        let encoded = contract.encode_arguments(&canonical).unwrap();
        let decoded = contract
            .decode_arguments(
                &serde_json::to_string(&encoded).unwrap(),
                Default::default(),
            )
            .unwrap();
        assert_eq!(decoded, canonical);
        original.validate_model_inputs(&decoded).unwrap();
        let saved = serde_json::to_string(&contract).unwrap();
        let restored = CompiledToolContract::restore(
            &saved,
            &original,
            &target,
            contract.digest(),
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            restored
                .decode_arguments(
                    &serde_json::to_string(&encoded).unwrap(),
                    Default::default()
                )
                .unwrap(),
            canonical
        );
        for fragment in contract.constraint_fragments() {
            assert!(!fragment.text.contains("hidden-system-schema"));
        }
        if count == 101 {
            for invalid in [
                json!({"arguments":"{"}),
                json!({"arguments":"[]"}),
                json!({"arguments":{}}),
                json!({"arguments":"{}","extra":1}),
                json!({}),
                json!({"arguments":"{\"field1\":1,\"field1\":2}"}),
                json!({"arguments":"{\"field1\":0.1234567890123456789012345}"}),
            ] {
                assert!(
                    contract
                        .decode_arguments(&invalid.to_string(), Default::default())
                        .is_err()
                );
            }
            let invalid = contract
                .decode_arguments(r#"{"arguments":"{}"}"#, Default::default())
                .unwrap();
            assert!(original.validate_model_inputs(&invalid).is_err());
            let hidden = contract
                .decode_arguments(
                    r#"{"arguments":"{\"workspace_id\":\"invented\"}"}"#,
                    Default::default(),
                )
                .unwrap();
            assert!(original.validate_model_inputs(&hidden).is_err());
        }
        request.tools = vec![contract.wire_tool().clone()];
        collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
    }
    for request in server.requests.lock().unwrap().iter() {
        assert_eq!(request.body["tools"][0]["strict"], true);
    }
}

#[tokio::test]
async fn empty_object_depth_boundaries_keep_openai_compiler_revision_compatible() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let target = ProviderToolTarget::for_route(&request(&connection, "base-model").route);
    for levels in [5, 6, 11] {
        let mut schema =
            json!({"type":"object","properties":{},"required":[],"additionalProperties":false});
        for _ in 1..levels {
            schema = json!({"type":"object","properties":{"child":schema},"required":["child"],"additionalProperties":false});
        }
        let original = compiled_tool(schema);
        let openai = CompiledToolContract::compile(
            &original,
            target.clone(),
            &wickle_model_responses::ResponsesToolSchemaCompiler,
            Default::default(),
        )
        .unwrap();
        assert_eq!(openai.compiler().version, id("1"));
        assert_eq!(
            openai.wire_tool().model_input_schema,
            *original.model_input_schema()
        );
        let azure = CompiledToolContract::compile(
            &original,
            target.clone(),
            &wickle_model_responses::AzureResponsesToolSchemaCompiler,
            Default::default(),
        )
        .unwrap();
        if levels == 5 {
            assert_eq!(
                azure.wire_tool().model_input_schema,
                *original.model_input_schema()
            );
        } else {
            assert_eq!(
                azure.wire_tool().model_input_schema["properties"]["child"],
                json!({"type":"string"})
            );
        }
    }
}
```

## `crates/wickle-model-responses/src/codec.rs`

```rust
use serde_json::{Value, json};
use wickle::*;

pub(crate) const REPLAY_KIND: &str = "wickle.openai.responses.v1";
pub(crate) const XAI_REPLAY_KIND: &str = "wickle.xai.responses.v1";
fn failure(code: ErrorCode) -> ContractError {
    crate::error(code, "codec")
}

/// Encode a Responses request, preserving exact route-bound replay and model schemas.
pub fn encode_request(request: &ModelRequest) -> Result<Value, ContractError> {
    encode(request, false)
}
/// Encode xAI's Responses dialect without relaxing OpenAI/Azure validation.
pub fn encode_xai_request(request: &ModelRequest) -> Result<Value, ContractError> {
    if request.tools.len() > 350 || request.options.contains_key("verbosity") {
        return Err(failure(ErrorCode::ModelCapabilityUnsupported));
    }
    if let Some(effort) = request.options.get("reasoning_effort") {
        let effort = effort
            .as_str()
            .ok_or_else(|| failure(ErrorCode::ModelOptionUnsupported))?;
        if !matches!(effort, "none" | "low" | "medium" | "high" | "xhigh")
            || (matches!(request.route.model_id.as_str(), "grok-4.6" | "grok-4.5")
                && effort == "none")
        {
            return Err(failure(ErrorCode::ModelOptionUnsupported));
        }
    }
    encode(request, true)
}
fn encode(request: &ModelRequest, xai: bool) -> Result<Value, ContractError> {
    request.validate()?;
    if request.options.keys().any(|key| {
        !["reasoning_effort", "temperature", "top_p", "verbosity"].contains(&key.as_str())
    }) {
        return Err(failure(ErrorCode::ModelOptionUnsupported));
    }
    let mut input = Vec::new();
    for message in &request.messages {
        let opaque: Vec<_> = message
            .content
            .iter()
            .filter_map(|content| match content {
                ModelContent::Opaque { continuation } => Some(continuation),
                _ => None,
            })
            .collect();
        if !opaque.is_empty() {
            if opaque.len() != 1 || message.role != ModelRole::Assistant {
                return Err(failure(ErrorCode::ModelContextIncompatible));
            }
            let replay = opaque[0].data();
            let object = replay
                .as_object()
                .ok_or_else(|| failure(ErrorCode::ModelContextIncompatible))?;
            if object.len() != 2
                || object.get("kind")
                    != Some(&json!(if xai { XAI_REPLAY_KIND } else { REPLAY_KIND }))
            {
                return Err(failure(ErrorCode::ModelContextIncompatible));
            }
            let items = replay
                .get("items")
                .and_then(Value::as_array)
                .ok_or_else(|| failure(ErrorCode::ModelContextIncompatible))?;
            let decoded = inspect_output_items(items, xai)?;
            let mut text = String::new();
            let mut calls = Vec::new();
            for content in &message.content {
                match content {
                    ModelContent::Text { text: value } => text.push_str(value),
                    ModelContent::ToolCall {
                        provider_call_id,
                        name,
                        arguments,
                    } => calls.push((provider_call_id.as_str(), name.as_str(), arguments)),
                    ModelContent::Opaque { .. } => {}
                    _ => return Err(failure(ErrorCode::ModelContextIncompatible)),
                }
            }
            if text != decoded.text || calls.len() != decoded.calls.len() {
                return Err(failure(ErrorCode::ModelContextIncompatible));
            }
            for ((call_id, name, arguments), original) in calls.iter().zip(&decoded.calls) {
                if *call_id != original.call_id
                    || *name != original.name
                    || match parse_provider_arguments(
                        &original.arguments,
                        request.limits.max_input_bytes,
                    ) {
                        Ok(original) => **arguments != original,
                        // The core retains an empty canonical placeholder for an
                        // unparseable proposal. Replay the original opaque text so
                        // its paired Tool error can reach the next model turn.
                        Err(_) => !arguments.is_empty(),
                    }
                {
                    return Err(failure(ErrorCode::ModelContextIncompatible));
                }
            }
            // Replay the provider items once, in their original order. Do not also
            // append their normalized text and calls, which would duplicate them.
            input.extend(items.iter().cloned());
            continue;
        }
        let role = match message.role {
            ModelRole::System => "system",
            ModelRole::User => "user",
            ModelRole::Assistant => "assistant",
            ModelRole::Tool => "tool",
        };
        let mut parts = Vec::new();
        for content in &message.content {
            if message.role == ModelRole::Tool
                && !matches!(content, ModelContent::ToolResult { .. })
            {
                return Err(failure(ErrorCode::ModelContextIncompatible));
            }
            match content {
                ModelContent::Text { text } => parts.push(text.clone()),
                ModelContent::Json { value } => {
                    parts.push(
                        serde_json::to_string(value)
                            .map_err(|_| failure(ErrorCode::InvalidContract))?,
                    );
                }
                ModelContent::ToolCall {
                    provider_call_id,
                    name,
                    arguments,
                } => {
                    append_text(&mut input, role, &mut parts);
                    input.push(json!({"type":"function_call","call_id":provider_call_id,"name":name,"arguments":serde_json::to_string(arguments).map_err(|_| failure(ErrorCode::InvalidContract))?}));
                }
                ModelContent::ToolResult {
                    provider_call_id,
                    content,
                } => {
                    append_text(&mut input, role, &mut parts);
                    input.push(json!({"type":"function_call_output","call_id":provider_call_id,"output":serde_json::to_string(content).map_err(|_| failure(ErrorCode::InvalidContract))?}));
                }
                ModelContent::Opaque { .. } => unreachable!("opaque handled before normalization"),
            }
        }
        append_text(&mut input, role, &mut parts);
    }
    let tools: Vec<_> = request.tools.iter().map(|tool| json!({"type":"function","name":tool.name,"description":tool.description,"parameters":tool.model_input_schema,"strict":!xai && match request.route.provider.as_str() { "openai" => crate::schema::strict_schema_supported(&tool.model_input_schema, request.route.model_id.as_str().starts_with("ft:")), "azure-openai" => crate::schema::azure_strict_schema_supported(&tool.model_input_schema), _ => false }})).collect();
    let mut payload = json!({"model":request.route.model_id,"input":input,"max_output_tokens":request.max_output_tokens.get(),"store":false,"stream":true,"truncation":"disabled","include":["reasoning.encrypted_content"]});
    if !tools.is_empty() {
        payload["tools"] = json!(tools);
        payload["parallel_tool_calls"] = json!(false);
        payload["tool_choice"] = json!("auto");
    }
    if let Some(effort) = request.options.get("reasoning_effort") {
        let effort = effort
            .as_str()
            .filter(|effort| {
                ["none", "minimal", "low", "medium", "high", "xhigh", "max"].contains(effort)
            })
            .ok_or_else(|| failure(ErrorCode::ModelOptionUnsupported))?;
        payload["reasoning"] = json!({"effort":effort});
    }
    for (key, maximum) in [("temperature", 2.0), ("top_p", 1.0)] {
        if let Some(value) = request.options.get(key) {
            if !value
                .as_f64()
                .is_some_and(|number| number.is_finite() && number >= 0.0 && number <= maximum)
            {
                return Err(failure(ErrorCode::ModelOptionUnsupported));
            }
            payload[key] = value.clone();
        }
    }
    if let ModelOutput::JsonSchema { schema } = &request.output {
        validate_output_schema(schema)?;
        payload["text"] = json!({"format":{"type":"json_schema","name":"agent_output","strict":true,"schema":schema}});
    }
    if let Some(value) = request.options.get("verbosity") {
        if !matches!(value.as_str(), Some("low" | "medium" | "high")) {
            return Err(failure(ErrorCode::ModelOptionUnsupported));
        }
        if payload.get("text").is_none() {
            payload["text"] = json!({});
        }
        payload["text"]["verbosity"] = value.clone();
    }
    Ok(payload)
}

fn append_text(input: &mut Vec<Value>, role: &str, parts: &mut Vec<String>) {
    if !parts.is_empty() {
        input.push(json!({"role":role,"content":parts.join("\n")}));
        parts.clear();
    }
}

pub(crate) struct OutputCall {
    pub index: u32,
    pub call_id: String,
    pub name: String,
    pub arguments: String,
}
pub(crate) struct OutputItems {
    pub text: String,
    pub calls: Vec<OutputCall>,
    pub refused: bool,
}
pub(crate) fn inspect_output_items(
    items: &[Value],
    xai: bool,
) -> Result<OutputItems, ContractError> {
    let mut output = OutputItems {
        text: String::new(),
        calls: vec![],
        refused: false,
    };
    let mut call_ids = std::collections::BTreeSet::new();
    for (index, item) in items.iter().enumerate() {
        let object = item
            .as_object()
            .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
        if object.get("id").is_some_and(|id| {
            id.as_str()
                .is_none_or(|id| id.is_empty() && !(xai && item["type"] == "reasoning"))
        }) || object
            .get("status")
            .is_some_and(|status| status != "completed")
        {
            return Err(failure(ErrorCode::InvalidContract));
        }
        let allowed: &[&str] = match item.get("type").and_then(Value::as_str) {
            Some("reasoning") => {
                let summary = item
                    .get("summary")
                    .and_then(Value::as_array)
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                if item
                    .get("encrypted_content")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
                    || summary.iter().any(|part| {
                        part.get("type") != Some(&json!("summary_text"))
                            || part.get("text").and_then(Value::as_str).is_none()
                    })
                {
                    return Err(failure(ErrorCode::ModelContextIncompatible));
                }
                if let Some(content) = item.get("content") {
                    let content = content
                        .as_array()
                        .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                    if content.iter().any(|part| {
                        part.get("type") != Some(&json!("reasoning_text"))
                            || part.get("text").and_then(Value::as_str).is_none()
                            || part.as_object().is_none_or(|part| {
                                part.keys()
                                    .any(|key| !matches!(key.as_str(), "type" | "text"))
                            })
                    }) {
                        return Err(failure(ErrorCode::InvalidContract));
                    }
                }
                // Preserve reasoning content inside route-bound opaque replay only.
                &[
                    "type",
                    "id",
                    "status",
                    "summary",
                    "encrypted_content",
                    "content",
                ]
            }
            Some("message") if item.get("role") == Some(&json!("assistant")) => {
                if item.get("phase").is_some_and(|phase| {
                    !phase.is_null()
                        && !matches!(phase.as_str(), Some("commentary" | "final_answer"))
                }) {
                    return Err(failure(ErrorCode::InvalidContract));
                }
                for part in item
                    .get("content")
                    .and_then(Value::as_array)
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?
                {
                    let text = match part.get("type").and_then(Value::as_str) {
                        Some("output_text") => part.get("text").and_then(Value::as_str),
                        Some("refusal") => {
                            output.refused = true;
                            part.get("refusal").and_then(Value::as_str)
                        }
                        _ => return Err(failure(ErrorCode::InvalidContract)),
                    }
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                    output.text.push_str(text);
                }
                &["type", "id", "status", "role", "content", "phase"]
            }
            Some("function_call") => {
                let call_id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                let arguments = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                if call_id.is_empty()
                    || call_id.len() > 256
                    || call_id.chars().any(|c| c.is_whitespace() || c.is_control())
                    || name.is_empty()
                    || name.len() > 64
                    || !name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                    || !call_ids.insert(call_id)
                {
                    return Err(failure(ErrorCode::InvalidContract));
                }
                // The provider envelope is valid even when proposed arguments are not.
                // Keep their exact bytes for the core's validation/repair loop.
                output.calls.push(OutputCall {
                    index: index
                        .try_into()
                        .map_err(|_| failure(ErrorCode::InvalidContract))?,
                    call_id: call_id.into(),
                    name: name.into(),
                    arguments: arguments.into(),
                });
                &["type", "id", "status", "call_id", "name", "arguments"]
            }
            _ => return Err(failure(ErrorCode::CapabilityUnsupported)),
        };
        if object.keys().any(|key| !allowed.contains(&key.as_str())) {
            return Err(failure(ErrorCode::CapabilityUnsupported));
        }
    }
    if output.refused && !output.calls.is_empty() {
        return Err(failure(ErrorCode::InvalidContract));
    }
    Ok(output)
}

pub(crate) fn fragments(text: &str, maximum: usize) -> Result<Vec<String>, ContractError> {
    let mut remaining = text;
    let mut chunks = Vec::new();
    while !remaining.is_empty() {
        let mut boundary = maximum.min(remaining.len());
        while boundary > 0 && !remaining.is_char_boundary(boundary) {
            boundary -= 1;
        }
        if boundary == 0 {
            return Err(failure(ErrorCode::InvalidContract));
        }
        chunks.push(remaining[..boundary].into());
        remaining = &remaining[boundary..];
    }
    Ok(chunks)
}

fn validate_output_schema(schema: &Value) -> Result<(), ContractError> {
    if schema.get("type") != Some(&json!("object")) || schema.get("anyOf").is_some() {
        return Err(failure(ErrorCode::ModelCapabilityUnsupported));
    }
    fn node(schema: &Value, depth: usize) -> Result<(), ContractError> {
        let object = schema
            .as_object()
            .ok_or_else(|| failure(ErrorCode::ModelCapabilityUnsupported))?;
        if depth > 10
            || [
                "allOf",
                "oneOf",
                "not",
                "dependentRequired",
                "dependentSchemas",
                "if",
                "then",
                "else",
                "patternProperties",
                "propertyNames",
                "unevaluatedProperties",
                "prefixItems",
                "contains",
                "uniqueItems",
            ]
            .iter()
            .any(|key| object.contains_key(*key))
        {
            return Err(failure(ErrorCode::ModelCapabilityUnsupported));
        }
        if schema
            .get("$ref")
            .is_some_and(|value| value.as_str().is_none_or(|value| !value.starts_with('#')))
        {
            return Err(failure(ErrorCode::ModelCapabilityUnsupported));
        }
        let is_object = schema.get("type").is_some_and(|kind| {
            kind == "object"
                || kind
                    .as_array()
                    .is_some_and(|types| types.iter().any(|kind| kind == "object"))
        });
        if is_object {
            let properties = schema
                .get("properties")
                .and_then(Value::as_object)
                .ok_or_else(|| failure(ErrorCode::ModelCapabilityUnsupported))?;
            let required = schema
                .get("required")
                .and_then(Value::as_array)
                .ok_or_else(|| failure(ErrorCode::ModelCapabilityUnsupported))?;
            let names: std::collections::BTreeSet<_> =
                required.iter().filter_map(Value::as_str).collect();
            if schema.get("additionalProperties") != Some(&json!(false))
                || required.len() != names.len()
                || names.len() != properties.len()
                || properties.keys().any(|key| !names.contains(key.as_str()))
            {
                return Err(failure(ErrorCode::ModelCapabilityUnsupported));
            }
        }
        for key in ["properties", "$defs"] {
            if let Some(values) = object.get(key) {
                for child in values
                    .as_object()
                    .ok_or_else(|| failure(ErrorCode::ModelCapabilityUnsupported))?
                    .values()
                {
                    node(child, depth + 1)?;
                }
            }
        }
        if let Some(items) = object.get("items") {
            node(items, depth + 1)?;
        }
        if let Some(items) = object.get("anyOf") {
            for child in items
                .as_array()
                .ok_or_else(|| failure(ErrorCode::ModelCapabilityUnsupported))?
            {
                node(child, depth + 1)?;
            }
        }
        Ok(())
    }
    node(schema, 0)
}
```

## `crates/wickle-model-responses/src/lib.rs`

```rust
//! Shared Responses wire codecs. Provider authentication, endpoints, model
//! selection, and capability policy belong to the calling adapter and Host.
#![forbid(unsafe_code)]
mod codec;
mod response;
mod schema;
mod sse;
pub use codec::{encode_request, encode_xai_request};
pub use response::Decoder as ResponsesDecoder;
pub use schema::{AzureResponsesToolSchemaCompiler, ResponsesToolSchemaCompiler};
pub use sse::{Decoder as SseDecoder, Event as SseEvent};
use wickle::{ContractError, ErrorCode};
fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("responses.{location}"))
}
```

## `crates/wickle-model-responses/src/schema.rs`

```rust
//! Versioned strict projection. Original constraints are preserved by the core.
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;
use wickle::*;

/// Responses-compatible Tool schemas with reversible omission/value handling.
#[derive(Debug, Clone, Copy, Default)]
pub struct ResponsesToolSchemaCompiler;
impl ProviderToolSchemaCompiler for ResponsesToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new("wickle-responses-tool-schema").expect("constant"),
            version: Id::new("1").expect("constant"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        let restricted = target
            .model
            .as_ref()
            .is_none_or(|model| model.id.as_str().starts_with("ft:"));
        compile(tool, target, SchemaPolicy::openai(restricted))
    }
}

/// Azure Responses projection using its documented schema subset and limits.
#[derive(Debug, Clone, Copy, Default)]
pub struct AzureResponsesToolSchemaCompiler;
impl ProviderToolSchemaCompiler for AzureResponsesToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new("wickle-azure-responses-tool-schema").expect("constant"),
            version: Id::new("1").expect("constant"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        if target.api_contract.operation.as_str() != "responses" {
            return Err(invalid("operation"));
        }
        let policy = SchemaPolicy::azure();
        let properties = tool
            .model_input_schema
            .get("properties")
            .and_then(Value::as_object);
        if properties.is_some_and(|properties| properties.len() > policy.max_properties) {
            let mut wire_tool = tool.clone();
            wire_tool.model_input_schema = object_schema(Map::from_iter([(
                "arguments".into(),
                json!({"type":"string"}),
            )]));
            return Ok(ProviderToolProjection {
                wire_tool,
                decode_plan: ArgumentDecodePlan::JsonObjectText {
                    wire_name: "arguments".into(),
                },
            });
        }
        compile(tool, target, policy)
    }
}
#[derive(Clone, Copy)]
struct SchemaPolicy {
    restricted_constraints: bool,
    max_depth: usize,
    max_properties: usize,
    max_object_depth: usize,
}
impl SchemaPolicy {
    fn openai(restricted_constraints: bool) -> Self {
        Self {
            restricted_constraints,
            max_depth: 10,
            max_properties: 5000,
            max_object_depth: 10,
        }
    }
    fn azure() -> Self {
        Self {
            restricted_constraints: true,
            max_depth: 5,
            max_properties: 100,
            max_object_depth: 4,
        }
    }
}
fn compile(
    tool: &ModelTool,
    target: &ProviderToolTarget,
    policy: SchemaPolicy,
) -> Result<ProviderToolProjection, ContractError> {
    if target.api_contract.operation.as_str() != "responses" {
        return Err(invalid("operation"));
    }
    let fine_tuned = policy.restricted_constraints;
    if supported_with(&tool.model_input_schema, policy) {
        return Ok(ProviderToolProjection {
            wire_tool: tool.clone(),
            decode_plan: ArgumentDecodePlan::Identity {},
        });
    }
    let root = &tool.model_input_schema;
    let properties = root
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("properties"))?;
    let required: BTreeSet<_> = root
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    let mut wire = Map::new();
    let mut fields = vec![];
    for (name, schema) in properties {
        let optional = !required.contains(name.as_str());
        let native = lower(schema, root, fine_tuned, 1, &mut BTreeSet::new());
        let (schema, encoding) = match native {
            Some(schema) if optional => (
                json!({"type":"object","properties":{"present":{"type":"boolean"},"value":{"anyOf":[schema,{"type":"null"}]}},"required":["present","value"],"additionalProperties":false}),
                ArgumentValueEncoding::Presence {
                    present_key: "present".into(),
                    value_key: "value".into(),
                },
            ),
            Some(schema) => (schema, ArgumentValueEncoding::Identity {}),
            None => (
                json_text_schema(optional),
                ArgumentValueEncoding::JsonText { optional },
            ),
        };
        wire.insert(name.clone(), schema);
        fields.push(ArgumentFieldMapping {
            wire_name: name.clone(),
            canonical_name: name.clone(),
            encoding,
        });
    }
    let mut projected = tool.clone();
    projected.model_input_schema = object_schema(wire);
    // Representation overhead must not exceed either provider limits or
    // the core's original per-schema byte bound. Compact the largest
    // remaining native field, preserving every canonical field and rule.
    while !supported_with(&projected.model_input_schema, policy)
        || serde_json::to_vec(&projected)
            .map_err(|_| invalid("json"))?
            .len()
            > ProviderToolSchemaLimits::default().max_schema_bytes
    {
        let candidate = fields
            .iter()
            .enumerate()
            .filter(|(_, field)| !matches!(field.encoding, ArgumentValueEncoding::JsonText { .. }))
            .max_by_key(|(_, field)| {
                projected.model_input_schema["properties"][&field.wire_name]
                    .to_string()
                    .len()
            })
            .map(|(index, _)| index)
            .ok_or_else(|| invalid("limits"))?;
        let field = &mut fields[candidate];
        let optional = !required.contains(field.canonical_name.as_str());
        projected.model_input_schema["properties"][&field.wire_name] = json_text_schema(optional);
        field.encoding = ArgumentValueEncoding::JsonText { optional };
    }
    Ok(ProviderToolProjection {
        wire_tool: projected,
        decode_plan: ArgumentDecodePlan::Fields { fields },
    })
}

fn object_schema(properties: Map<String, Value>) -> Value {
    let required: Vec<_> = properties.keys().cloned().collect();
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}
fn json_text_schema(_optional: bool) -> Value {
    // The frozen constraint fragment explains the encoding once for the Tool;
    // repeating it in every property needlessly consumes the schema byte budget.
    json!({"type":"string"})
}

fn lower(
    schema: &Value,
    root: &Value,
    fine_tuned: bool,
    depth: usize,
    visiting: &mut BTreeSet<String>,
) -> Option<Value> {
    if depth > 8 {
        return None;
    }
    let node = schema.as_object()?;
    if let Some(reference) = node.get("$ref") {
        let reference = reference.as_str()?;
        if node
            .keys()
            .any(|key| !matches!(key.as_str(), "$ref" | "description" | "title" | "$comment"))
            || !reference.starts_with('#')
            || !visiting.insert(reference.into())
        {
            return None;
        }
        let target = root.pointer(&reference[1..])?;
        let result = lower(target, root, fine_tuned, depth + 1, visiting);
        visiting.remove(reference);
        return result;
    }
    if let Some(alternatives) = node.get("anyOf").or_else(|| node.get("oneOf")) {
        // Keeping only a union would lose sibling intersections. Use JSON text
        // for mixed forms rather than accidentally narrowing their values.
        if node.keys().any(|key| {
            !matches!(
                key.as_str(),
                "anyOf" | "oneOf" | "description" | "title" | "$comment"
            )
        }) {
            return None;
        }
        let branches: Vec<_> = alternatives
            .as_array()?
            .iter()
            .map(|branch| lower(branch, root, fine_tuned, depth + 1, visiting))
            .collect::<Option<_>>()?;
        return Some(json!({"anyOf":branches}));
    }
    let kind = node.get("type")?;
    let types = schema_types(kind)?;
    let mut result = Map::new();
    result.insert("type".into(), kind.clone());
    if types.contains(&"object") {
        if node.contains_key("patternProperties")
            || node.get("additionalProperties") != Some(&Value::Bool(false))
        {
            return None;
        }
        let properties = node.get("properties")?.as_object()?;
        if !all_required(node, properties) {
            return None;
        }
        let mut projected = Map::new();
        for (name, child) in properties {
            projected.insert(
                name.clone(),
                lower(child, root, fine_tuned, depth + 1, visiting)?,
            );
        }
        result.insert("properties".into(), Value::Object(projected));
        result.insert(
            "required".into(),
            node.get("required").cloned().unwrap_or_else(|| json!([])),
        );
        result.insert("additionalProperties".into(), Value::Bool(false));
    }
    if types.contains(&"array") {
        if node.contains_key("prefixItems") || node.get("items").is_some_and(Value::is_array) {
            return None;
        }
        result.insert(
            "items".into(),
            lower(node.get("items")?, root, fine_tuned, depth + 1, visiting)?,
        );
    }
    if let Some(value) = node.get("enum") {
        if value.as_array().is_some_and(|values| {
            !values.is_empty()
                && values
                    .iter()
                    .all(|value| !value.is_object() && !value.is_array())
        }) {
            result.insert("enum".into(), value.clone());
        }
    } else if let Some(value) = node.get("const") {
        if !value.is_object() && !value.is_array() {
            result.insert("enum".into(), json!([value]));
        }
    }
    if !fine_tuned {
        for key in [
            "pattern",
            "minimum",
            "maximum",
            "exclusiveMinimum",
            "exclusiveMaximum",
            "multipleOf",
            "minItems",
            "maxItems",
        ] {
            if let Some(value) = node.get(key) {
                result.insert(key.into(), value.clone());
            }
        }
        if let Some(value) = node
            .get("format")
            .filter(|value| value.as_str().is_some_and(known_format))
        {
            result.insert("format".into(), value.clone());
        }
    }
    if let Some(description) = node.get("description").filter(|value| value.is_string()) {
        result.insert("description".into(), description.clone());
    }
    Some(Value::Object(result))
}
fn schema_types(value: &Value) -> Option<Vec<&str>> {
    let types = if let Some(kind) = value.as_str() {
        vec![kind]
    } else {
        let values = value.as_array()?;
        if values.len() != 2 || !values.iter().any(|kind| kind == "null") {
            return None;
        }
        values
            .iter()
            .map(Value::as_str)
            .collect::<Option<Vec<_>>>()?
    };
    types
        .iter()
        .all(|kind| {
            matches!(
                *kind,
                "string" | "number" | "integer" | "boolean" | "object" | "array" | "null"
            )
        })
        .then_some(types)
}
fn all_required(node: &Map<String, Value>, properties: &Map<String, Value>) -> bool {
    let Some(required) = node.get("required").and_then(Value::as_array) else {
        return properties.is_empty();
    };
    let names: BTreeSet<_> = required.iter().filter_map(Value::as_str).collect();
    names.len() == required.len()
        && names.len() == properties.len()
        && properties.keys().all(|name| names.contains(name.as_str()))
}
fn known_format(value: &str) -> bool {
    matches!(
        value,
        "date-time"
            | "time"
            | "date"
            | "duration"
            | "email"
            | "hostname"
            | "ipv4"
            | "ipv6"
            | "uuid"
    )
}
#[derive(Default)]
struct Bounds {
    properties: usize,
    enums: usize,
    characters: usize,
}
/// Check only; this never rewrites the already compiled wire schema.
pub(crate) fn strict_schema_supported(schema: &Value, fine_tuned: bool) -> bool {
    supported_with(schema, SchemaPolicy::openai(fine_tuned))
}
pub(crate) fn azure_strict_schema_supported(schema: &Value) -> bool {
    supported_with(schema, SchemaPolicy::azure())
}
fn supported_with(schema: &Value, policy: SchemaPolicy) -> bool {
    schema.get("type") == Some(&json!("object"))
        && schema.get("anyOf").is_none()
        && strict_node(schema, schema, policy, 0, &mut Bounds::default())
}
fn strict_node(
    schema: &Value,
    root: &Value,
    policy: SchemaPolicy,
    depth: usize,
    bounds: &mut Bounds,
) -> bool {
    let Some(node) = schema.as_object() else {
        return false;
    };
    if depth > policy.max_depth
        || node.keys().any(|key| {
            !matches!(
                key.as_str(),
                "type"
                    | "properties"
                    | "required"
                    | "additionalProperties"
                    | "items"
                    | "enum"
                    | "anyOf"
                    | "$defs"
                    | "$ref"
                    | "description"
                    | "title"
                    | "pattern"
                    | "format"
                    | "minimum"
                    | "maximum"
                    | "exclusiveMinimum"
                    | "exclusiveMaximum"
                    | "multipleOf"
                    | "minItems"
                    | "maxItems"
            )
        })
    {
        return false;
    }
    if policy.restricted_constraints
        && [
            "pattern",
            "format",
            "minimum",
            "maximum",
            "exclusiveMinimum",
            "exclusiveMaximum",
            "multipleOf",
            "minItems",
            "maxItems",
        ]
        .iter()
        .any(|key| node.contains_key(*key))
    {
        return false;
    }
    if node
        .get("format")
        .is_some_and(|value| !value.as_str().is_some_and(known_format))
    {
        return false;
    }
    if let Some(reference) = node.get("$ref") {
        if reference.as_str().is_none_or(|reference| {
            !reference.starts_with('#') || root.pointer(&reference[1..]).is_none()
        }) {
            return false;
        }
    }
    let types = match node.get("type") {
        Some(value) => match schema_types(value) {
            Some(types) => types,
            None => return false,
        },
        None if node.contains_key("anyOf") || node.contains_key("$ref") => vec![],
        None => return false,
    };
    if types.contains(&"object") {
        if depth > policy.max_object_depth {
            return false;
        }
        let Some(properties) = node.get("properties").and_then(Value::as_object) else {
            return false;
        };
        if node.get("additionalProperties") != Some(&Value::Bool(false))
            || !node.contains_key("required")
            || !all_required(node, properties)
        {
            return false;
        }
    }
    if types.contains(&"array") && !node.contains_key("items") {
        return false;
    }
    if let Some(values) = node.get("enum") {
        let Some(values) = values.as_array() else {
            return false;
        };
        if values.is_empty()
            || values
                .iter()
                .any(|value| value.is_array() || value.is_object())
        {
            return false;
        }
        bounds.enums = bounds.enums.saturating_add(values.len());
        let characters: usize = values
            .iter()
            .filter_map(Value::as_str)
            .map(|value| value.chars().count())
            .sum();
        if values.len() > 250 && characters > 15_000 {
            return false;
        }
        bounds.characters = bounds.characters.saturating_add(characters);
    }
    for key in ["properties", "$defs"] {
        if let Some(children) = node.get(key) {
            let Some(children) = children.as_object() else {
                return false;
            };
            if key == "properties" {
                bounds.properties = bounds.properties.saturating_add(children.len());
            }
            bounds.characters = bounds.characters.saturating_add(
                children
                    .keys()
                    .map(|name| name.chars().count())
                    .sum::<usize>(),
            );
            for child in children.values() {
                if !strict_node(child, root, policy, depth + 1, bounds) {
                    return false;
                }
            }
        }
    }
    if let Some(items) = node.get("items") {
        if !strict_node(items, root, policy, depth + 1, bounds) {
            return false;
        }
    }
    if let Some(branches) = node.get("anyOf") {
        let Some(branches) = branches.as_array() else {
            return false;
        };
        if branches.is_empty()
            || branches
                .iter()
                .any(|branch| !strict_node(branch, root, policy, depth + 1, bounds))
        {
            return false;
        }
    }
    bounds.properties <= policy.max_properties
        && bounds.enums <= 1000
        && bounds.characters <= 120_000
}
fn invalid(path: &str) -> ContractError {
    ContractError::new(
        ErrorCode::UnsupportedInputProjection,
        format!("responses.schema.{path}"),
    )
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
    /// Exact model/release, absent only in older or manually unqualified contracts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<VersionedRef>,
    /// Provider namespace, including deployment-specific provider adapters.
    pub provider: Id,
    /// Exact operation and API version.
    pub api_contract: ApiContract,
    /// Pinned target capability revision.
    pub capability_revision: Id,
}
impl ProviderToolTarget {
    /// Capture the selected route without connection metadata or credentials.
    pub fn for_route(route: &crate::ResolvedModelRoute) -> Self {
        Self {
            model: Some(VersionedRef {
                id: route.model_id.clone(),
                version: route.model_version.clone(),
            }),
            provider: route.provider.clone(),
            api_contract: route.api_contract.clone(),
            capability_revision: route.capability_revision.clone(),
        }
    }
    fn matches_saved(&self, saved: &Self) -> bool {
        self.provider == saved.provider
            && self.api_contract == saved.api_contract
            && self.capability_revision == saved.capability_revision
            && saved
                .model
                .as_ref()
                .is_none_or(|model| self.model.as_ref() == Some(model))
    }
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
    /// A JSON document encoded as a string. Optional values use [] for omission
    /// and `[value]` for a supplied value, keeping explicit null distinct.
    JsonText {
        /// Whether the string represents an optional zero-or-one value array.
        optional: bool,
    },
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
    /// Entire canonical argument object encoded as one JSON string property.
    JsonObjectText {
        /// The sole wire property; its decoded value must be an object.
        wire_name: String,
    },
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
            let mut text = format!(
                "Tool {}: arguments must satisfy this canonical JSON Schema after decoding: {}\nDecode representation: {}. Field mappings restore wire_name to canonical_name. For a presence envelope, both members are required: true marks a supplied value (including explicit null); false with a null value placeholder means omission. Preserve omission and explicit null as distinct values.",
                projection.wire_tool.name,
                serde_json::to_string(tool.model_input_schema())
                    .map_err(|_| invalid("provider_tool.schema"))?,
                serde_json::to_string(&projection.decode_plan)
                    .map_err(|_| invalid("provider_tool.codec"))?
            );
            if matches!(&projection.decode_plan, ArgumentDecodePlan::Fields { fields } if fields.iter().any(|field| matches!(field.encoding, ArgumentValueEncoding::JsonText { .. })))
            {
                text.push_str(" For json_text, the wire value is a JSON string parsed by the core. With optional=false it encodes the canonical value itself. With optional=true it must encode [] for omission or [value] for a supplied value, including [null] for explicit null. Nested optional properties remain absent inside that JSON document; do not replace absence with null.");
            }
            if matches!(
                &projection.decode_plan,
                ArgumentDecodePlan::JsonObjectText { .. }
            ) {
                text.push_str(" For json_object_text, send exactly the named wire property as a JSON string containing the entire canonical argument object. Preserve absent properties and explicit null values inside it; do not include system-owned fields.");
            }
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
            || !target.matches_saved(&saved.data.target)
        {
            return Err(invalid("provider_tool.identity"));
        }
        let rebuilt = Self::build(
            tool,
            saved.data.target.clone(),
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
            ArgumentDecodePlan::JsonObjectText { wire_name } => Ok(JsonObject::from([(
                wire_name.clone(),
                Value::String(serde_json::to_string(input).map_err(|_| arguments())?),
            )])),
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
                        ArgumentValueEncoding::JsonText { optional } => {
                            if *optional || value.is_some() {
                                let encoded = if *optional {
                                    serde_json::to_string(&value.into_iter().collect::<Vec<_>>())
                                } else {
                                    serde_json::to_string(value.expect("present value"))
                                }
                                .map_err(|_| arguments())?;
                                output.insert(field.wire_name.clone(), Value::String(encoded));
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
            ArgumentDecodePlan::JsonObjectText { wire_name } => {
                if object.len() != 1 {
                    return Err(arguments());
                }
                let text = object
                    .get(wire_name)
                    .and_then(Value::as_str)
                    .ok_or_else(arguments)?;
                parse_provider_arguments(text, limits.max_argument_bytes)
            }
            ArgumentDecodePlan::Fields { fields } => {
                let mut result = JsonObject::new();
                for (name, value) in &object {
                    let mapping = fields
                        .iter()
                        .find(|field| &field.wire_name == name)
                        .ok_or_else(arguments)?;
                    let restored = match &mapping.encoding {
                        ArgumentValueEncoding::Identity {} => Some(value.clone()),
                        ArgumentValueEncoding::JsonText { optional } => {
                            let text = value.as_str().ok_or_else(arguments)?;
                            let parsed = parse_provider_value(text, limits.max_argument_bytes)?;
                            if *optional {
                                let values = parsed.as_array().ok_or_else(arguments)?;
                                match values.len() {
                                    0 => None,
                                    1 => Some(values[0].clone()),
                                    _ => return Err(arguments()),
                                }
                            } else {
                                Some(parsed)
                            }
                        }
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
        ArgumentDecodePlan::JsonObjectText { wire_name }
            if wire.len() == 1
                && wire.get(wire_name).and_then(|value| value.get("type"))
                    == Some(&json!("string")) =>
        {
            Ok(())
        }
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

/// Parse model-owned provider arguments without silently rounding number tokens.
pub fn parse_provider_arguments(raw: &str, max_bytes: usize) -> Result<JsonObject, ContractError> {
    Ok(parse_provider_value(raw, max_bytes)?
        .as_object()
        .ok_or_else(arguments)?
        .clone()
        .into_iter()
        .collect())
}
fn parse_provider_value(raw: &str, max_bytes: usize) -> Result<Value, ContractError> {
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
    Ok(value)
}
```
