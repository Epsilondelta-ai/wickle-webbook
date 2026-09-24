# 53장 전체 구현과 변경 검사

[강의](../53-bedrock-repair.md) · [전체 변경 패치](../solutions/53-bedrock-repair.patch)

기준 `103d361c7de3e902f434e8a450029f7b39bb3105`. 이 단계에서 바뀐 Rust·manifest·Python 파일의 전체 내용이다. 이전 버전과의 정확한 교체 위치·삭제는 patch를 따른다. 다음 장의 코드와 섞지 않는다.

## `crates/wickle-model-bedrock/src/model.rs`

```rust
use crate::{
    BedrockAudience, BedrockConnection, BedrockCredentialContext, BedrockEndpoint,
    BedrockOperation, auth, error, framing::Framing,
};
use futures_util::stream;
use reqwest::Response;
use serde_json::Value;
use std::collections::VecDeque;
use wickle::*;
use wickle_model_anthropic::protocol::{MessagesDecoder, encode_request};

/// One Bedrock Claude POST per invocation, streamed into Wickle model events.
/// The adapter does not retry, run Tool handlers, or load environment variables.
#[derive(Debug, Clone)]
pub struct BedrockModel {
    connection: BedrockConnection,
}
impl BedrockModel {
    /// Bind an already configured connection without making a network request.
    pub fn new(connection: BedrockConnection) -> Self {
        Self { connection }
    }
}
impl ModelPort for BedrockModel {
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
            decoder: MessagesDecoder::new(request, None),
            framing: Framing::new(&self.connection.0.options),
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
    connection: &'a BedrockConnection,
    request: &'a ModelRequest,
    context: &'a ModelCallContext,
    response: Option<Response>,
    decoder: MessagesDecoder<'a>,
    framing: Framing,
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
        if self.connection.0.options.endpoint == BedrockEndpoint::Mantle
            && matches!(self.request.output, ModelOutput::JsonSchema { .. })
        {
            return Err(error(
                ErrorCode::ModelCapabilityUnsupported,
                "mantle_json_output",
            ));
        }
        let mut value = encode_request(self.request)?;
        match self.connection.0.options.operation {
            BedrockOperation::Messages => {
                value["model"] = serde_json::json!(self.connection.0.options.selector.value())
            }
            BedrockOperation::InvokeStream => {
                let object = value.as_object_mut().expect("encoded object");
                object.remove("model");
                object.remove("stream");
                object.insert(
                    "anthropic_version".into(),
                    serde_json::json!("bedrock-2023-05-31"),
                );
            }
        }
        let body =
            serde_json::to_vec(&value).map_err(|_| error(ErrorCode::InvalidJson, "request"))?;
        if body.len() > self.request.limits.max_input_bytes {
            return Err(error(ErrorCode::ModelCapabilityUnsupported, "request_size"));
        }
        let url = self.connection.0.url.clone();
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "content-type",
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        headers.insert(
            "accept",
            reqwest::header::HeaderValue::from_static(self.content_type()),
        );
        headers.insert(
            "accept-encoding",
            reqwest::header::HeaderValue::from_static("identity"),
        );
        if self.connection.0.options.operation == BedrockOperation::Messages {
            headers.insert(
                "anthropic-version",
                reqwest::header::HeaderValue::from_static("2023-06-01"),
            );
        }
        let headers = match auth::authorize(
            self.connection.0.credentials.as_ref(),
            BedrockCredentialContext {
                scope: &self.context.scope,
                audience: BedrockAudience::Inference,
                region: &self.connection.0.options.region,
                cancellation: &self.context.cancellation,
                deadline: self.context.deadline,
            },
            auth::SigningRequest {
                method: "POST",
                url: &url,
                body: &body,
                service: self.connection.signing_service(),
                clock: self.connection.0.clock.as_ref(),
                headers,
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
            .body(body)
            .send();
        let mut response = tokio::select! { biased;
            _ = self.context.cancellation.cancelled() => return Err(error(ErrorCode::Cancelled, "request")),
            _ = tokio::time::sleep_until(self.context.deadline) => return Err(error(ErrorCode::DeadlineExceeded, "request")),
            result = operation => result.map_err(transport_error)?,
        };
        self.decoder.metadata.provider_request_id = response
            .headers()
            .get("request-id")
            .or_else(|| response.headers().get("x-amzn-requestid"))
            .or_else(|| response.headers().get("x-amz-request-id"))
            .map(|value| {
                value
                    .to_str()
                    .map_err(|_| error(ErrorCode::InvalidContract, "request_id"))
                    .and_then(Id::new)
            })
            .transpose()?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let error_type = response
                .headers()
                .get("x-amzn-errortype")
                .and_then(|value| value.to_str().ok())
                .map(aws_error_name)
                .map(str::to_owned);
            let mut kind = match status {
                401 | 403 => ModelFailureKind::Authentication,
                404 => ModelFailureKind::Unavailable,
                413 => ModelFailureKind::ContextOverflow,
                408 | 504 => ModelFailureKind::Timeout,
                429 => ModelFailureKind::RateLimited,
                500..=599 => ModelFailureKind::Transport,
                _ => ModelFailureKind::Unsupported,
            };
            // Only the named stream error is documented as retryable at 424.
            // Generic ModelErrorException shares that status and stays unchanged.
            if status == 424 && error_type.as_deref() == Some("ModelStreamErrorException") {
                kind = ModelFailureKind::Transport;
            }
            if status == 400 || (status == 424 && error_type.is_none()) {
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
                    if status == 424
                        && value
                            .get("code")
                            .or_else(|| value.get("__type"))
                            .and_then(Value::as_str)
                            .map(aws_error_name)
                            == Some("ModelStreamErrorException")
                    {
                        kind = ModelFailureKind::Transport;
                    }
                    if status == 400
                        && value.pointer("/error/code").and_then(Value::as_str)
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
        if !content_type.is_some_and(|value| value.eq_ignore_ascii_case(self.content_type())) {
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
    fn content_type(&self) -> &'static str {
        match self.connection.0.options.operation {
            BedrockOperation::Messages => "text/event-stream",
            BedrockOperation::InvokeStream => "application/vnd.amazon.eventstream",
        }
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

// AWS restJson1 error names may include a colon suffix and namespace prefix.
fn aws_error_name(value: &str) -> &str {
    let value = value.split_once(':').map_or(value, |(name, _)| name);
    value.split_once('#').map_or(value, |(_, name)| name)
}
```

## `crates/wickle-model-bedrock/tests/agent_contract.rs`

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
use wickle_model_bedrock::*;
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
    let mut data = support::events("claude-opus-5", "");
    data.truncate(4);
    data[0]["message"]["id"] = json!(format!("msg_{index}"));
    data.extend([
        json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":format!("call_{index}"),"name":"lookup","input":{}}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":arguments}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":20}}),
        json!({"type":"message_stop"}),
    ]);
    data
}
#[tokio::test]
async fn native_and_binary_transports_repair_before_scoped_execution_with_core_owned_retry() {
    for (operation, endpoint) in [
        (BedrockOperation::Messages, BedrockEndpoint::Runtime),
        (BedrockOperation::Messages, BedrockEndpoint::Mantle),
        (BedrockOperation::InvokeStream, BedrockEndpoint::Runtime),
    ] {
        exercise(operation, endpoint).await;
    }
}
async fn exercise(operation: BedrockOperation, endpoint: BedrockEndpoint) {
    let invalid_constraint =
        json!({"query":"latest","limit":7,"note":null,"filter":{"category":"finance"}}).to_string();
    let invalid_json =
        json!({"query":"latest","limit":9,"note":null,"filter":"invalid-shape"}).to_string();
    let valid =
        json!({"query":"latest","limit":9,"note":null,"filter":{"category":"finance"}}).to_string();
    let malformed = "{not json";
    let rounded = r#"{"query":"latest","limit":0.12345678901234567890123456789}"#;
    let mut transport_failure = support::Reply::json(
        424,
        json!({"__type":"ModelStreamErrorException","message":"private failure detail"}),
    );
    transport_failure
        .headers
        .push(("x-amzn-requestid", "failed-attempt".into()));
    let server = support::Server::new(vec![
        transport_failure,
        support::reply(operation, &call_events(0, malformed)),
        support::reply(operation, &call_events(4, rounded)),
        support::reply(operation, &call_events(1, &invalid_constraint)),
        support::reply(operation, &call_events(2, &invalid_json)),
        support::reply(operation, &call_events(3, &valid)),
        support::reply(operation, &support::events("claude-opus-5", "complete")),
    ])
    .await;
    let mut options = support::options(&server);
    options.operation = operation;
    options.endpoint = endpoint;
    let connection = BedrockConnection::new(
        scope(),
        reference("account"),
        support::credentials(),
        options,
    )
    .unwrap()
    .with_clock(Arc::new(support::FixedClock));
    let fixture = core_host::Fixture::new(core_host::Response::Text, false);
    let mut catalog = fixture.router.snapshot.catalog().clone();
    catalog.models[0].provider = id("aws-bedrock");
    catalog.models[0].model_id = id(support::MODEL);
    catalog.models[0]
        .capabilities
        .features
        .insert(id("tool_calling"));
    catalog.models[0].capabilities.options_schema = json!({"type":"object","properties":{"effort":{"enum":["low","medium","high"]}},"additionalProperties":false});
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
    catalog.bindings[0]
        .default_options
        .insert("effort".into(), json!("high"));
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
            Arc::new(BedrockModel::new(connection)),
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
    let expected_schema = tool.model_input_schema().clone();
    assert!(expected_schema["properties"].get("workspace_id").is_none());
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
    profile.model_options.insert("effort".into(), json!("low"));
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
        .insert("effort".into(), json!("medium"));
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
        assert_eq!(request.body["tools"][0]["input_schema"], expected_schema);
        assert_eq!(
            request.body["tools"][0]["input_schema"]["required"],
            json!(["query"])
        );
        assert_eq!(request.body["output_config"]["effort"], "medium");
        let headers = request.headers.to_ascii_lowercase();
        assert!(headers.contains(if endpoint == BedrockEndpoint::Mantle {
            "/us-east-1/bedrock-mantle/aws4_request"
        } else {
            "/us-east-1/bedrock/aws4_request"
        }));
        if operation == BedrockOperation::InvokeStream {
            assert!(request.path.ends_with("/invoke-with-response-stream"));
            assert_eq!(request.body["anthropic_version"], "bedrock-2023-05-31");
            assert!(request.body.get("model").is_none());
            assert!(request.body.get("stream").is_none());
        } else {
            assert_eq!(request.path, "/anthropic/v1/messages");
            assert_eq!(request.body["model"], support::MODEL);
        }
        assert!(!request.body.to_string().contains(WORKSPACE));
    }
    let content: Vec<_> = requests[6].body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|message| message["content"].as_array().unwrap())
        .collect();
    let replayed: Vec<_> = content
        .iter()
        .filter(|item| item["type"] == "tool_use")
        .map(|item| item["input"].clone())
        .collect();
    assert_eq!(
        replayed,
        vec![
            json!({"INVALID_JSON":malformed}),
            json!({"INVALID_JSON":rounded}),
            parse_json(&invalid_constraint).unwrap(),
            parse_json(&invalid_json).unwrap(),
            parse_json(&valid).unwrap()
        ]
    );
    let failures: Vec<_> = content
        .iter()
        .filter(|item| item["type"] == "tool_result" && item["is_error"] == true)
        .collect();
    assert_eq!(failures.len(), 2);
    for (result, raw) in failures.into_iter().zip([malformed, rounded]) {
        assert_eq!(
            parse_json(result["content"].as_str().unwrap()).unwrap()["INVALID_JSON"],
            raw
        );
    }
    let thinking: Vec<_> = content
        .iter()
        .filter(|item| item["type"] == "thinking")
        .collect();
    assert_eq!(thinking.len(), 5);
    assert!(
        thinking
            .iter()
            .all(|block| block["signature"] == "signature-fixture")
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
}
```

## `crates/wickle-model-bedrock/tests/bedrock.rs`

```rust
//! AWS signing, endpoint, binary/SSE framing and metadata contracts without AWS calls.
mod support;
use aws_smithy_types::event_stream::{Header, HeaderValue, Message};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use support::*;
use wickle::*;
use wickle_model_bedrock::*;

fn configured(
    server: &Server,
    operation: BedrockOperation,
    endpoint: BedrockEndpoint,
    bearer: bool,
) -> BedrockConnection {
    let mut options = options(server);
    options.operation = operation;
    options.endpoint = endpoint;
    let credential = if bearer {
        Arc::new(BedrockCredential::Bearer("test-bearer".into()))
            as Arc<dyn BedrockCredentialProvider>
    } else {
        credentials()
    };
    BedrockConnection::new(scope(), reference("account"), credential, options)
        .unwrap()
        .with_clock(Arc::new(FixedClock))
}

#[tokio::test]
async fn each_wire_operation_preserves_model_and_uses_its_own_auth_contract() {
    for (operation, endpoint, bearer) in [
        (BedrockOperation::Messages, BedrockEndpoint::Runtime, true),
        (BedrockOperation::Messages, BedrockEndpoint::Mantle, false),
        (
            BedrockOperation::InvokeStream,
            BedrockEndpoint::Runtime,
            false,
        ),
    ] {
        let server = Server::new(vec![reply(operation, &events(MODEL, "42"))]).await;
        let connection = configured(&server, operation, endpoint, bearer);
        let request = request(&connection, MODEL);
        let model = BedrockModel::new(connection);
        let response =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap();
        assert_eq!(response.text, "42");
        assert_eq!(
            response.metadata.provider_request_id,
            Some(id("aws-request"))
        );
        assert_eq!(response.metadata.reported_model_id, Some(id(MODEL)));
        assert_eq!(response.metadata.usage.unwrap().output_tokens, Some(15));
        assert_eq!(
            response.continuation[0].data()["blocks"][0]["signature"],
            "signature-fixture"
        );
        let calls = server.requests.lock().unwrap();
        assert_eq!(calls.len(), 1);
        let call = &calls[0];
        let headers = call.headers.to_ascii_lowercase();
        assert_eq!(call.method, "POST");
        if bearer {
            assert!(headers.contains("authorization: bearer test-bearer"));
            assert!(!headers.contains("x-amz-security-token"));
        } else {
            assert!(headers.contains("x-amz-security-token: test-session"));
            assert!(headers.contains(if endpoint == BedrockEndpoint::Mantle {
                "/us-east-1/bedrock-mantle/aws4_request"
            } else {
                "/us-east-1/bedrock/aws4_request"
            }));
            assert!(!headers.contains("test-secret"));
        }
        match operation {
            BedrockOperation::Messages => {
                assert_eq!(call.path, "/anthropic/v1/messages");
                assert_eq!(call.body["model"], MODEL);
                assert_eq!(call.body["stream"], true);
                assert!(call.body.get("anthropic_version").is_none());
                assert!(headers.contains("anthropic-version: 2023-06-01"));
            }
            BedrockOperation::InvokeStream => {
                assert_eq!(
                    call.path,
                    format!("/model/{MODEL}/invoke-with-response-stream")
                );
                assert_eq!(call.body["anthropic_version"], "bedrock-2023-05-31");
                assert!(call.body.get("model").is_none());
                assert!(call.body.get("stream").is_none());
            }
        }
        assert_eq!(call.body["output_config"]["effort"], "medium");
        assert!(!call.body.to_string().contains("hidden-workspace"));
    }
}

#[tokio::test]
async fn corrupted_truncated_or_oversized_aws_frames_never_complete() {
    for case in ["crc", "truncated", "length", "count"] {
        let mut response = reply(BedrockOperation::InvokeStream, &events(MODEL, "answer"));
        match case {
            "crc" => response.body[12] ^= 1,
            "truncated" => {
                response.body.pop();
            }
            "length" => response.body[..4].copy_from_slice(&u32::MAX.to_be_bytes()),
            _ => {}
        }
        let server = Server::new(vec![response]).await;
        let mut options = options(&server);
        options.operation = BedrockOperation::InvokeStream;
        if case == "count" {
            options.max_protocol_events = 1;
        }
        let connection =
            BedrockConnection::new(scope(), reference("account"), credentials(), options)
                .unwrap()
                .with_clock(Arc::new(FixedClock));
        let request = request(&connection, MODEL);
        let model = BedrockModel::new(connection);
        assert!(
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .is_err(),
            "{case}"
        );
    }
}

#[tokio::test]
async fn scope_contract_unsupported_features_and_expired_credentials_stop_before_http() {
    for case in ["scope", "route", "api", "json", "expired"] {
        let server = Server::new(vec![]).await;
        let mut options = options(&server);
        options.endpoint = BedrockEndpoint::Mantle;
        let credential: Arc<dyn BedrockCredentialProvider> = if case == "expired" {
            Arc::new(BedrockCredential::Aws(AwsCredentials::new(
                "AKIDEXAMPLE",
                "secret",
                None,
                Some(std::time::UNIX_EPOCH),
                "fixture",
            )))
        } else {
            credentials()
        };
        let connection = BedrockConnection::new(scope(), reference("account"), credential, options)
            .unwrap()
            .with_clock(Arc::new(FixedClock));
        let mut request = request(&connection, MODEL);
        let mut context = context(&request);
        let model = BedrockModel::new(connection);
        match case {
            "scope" => context.scope.workspace_id = id("other"),
            "route" => {
                request
                    .route
                    .target
                    .insert("region".into(), json!("us-west-2"));
            }
            "api" => request.route.api_contract.version = id("other"),
            "json" => {
                request.output = ModelOutput::JsonSchema {
                    schema: json!({"type":"object","properties":{},"additionalProperties":false}),
                }
            }
            _ => {}
        }
        let failure = collect_model_response(&request, model.generate(&request, &context))
            .await
            .unwrap_err();
        if case == "expired" {
            assert_eq!(failure.kind, ModelFailureKind::Authentication);
        }
        assert!(server.requests.lock().unwrap().is_empty());
    }
}

fn foundation() -> Value {
    json!({"modelDetails":{"modelId":MODEL,"modelArn":format!("arn:aws:bedrock:us-east-1::foundation-model/{MODEL}"),"providerName":"Anthropic","responseStreamingSupported":true,"modelLifecycle":{"status":"ACTIVE"}}})
}
fn profile() -> Value {
    json!({"inferenceProfileId":"profile","inferenceProfileArn":"arn:aws:bedrock:us-east-1:123456789012:inference-profile/profile","status":"ACTIVE","models":[{"modelArn":format!("arn:aws:bedrock:us-east-1::foundation-model/{MODEL}")},{"modelArn":format!("arn:aws:bedrock:us-west-2::foundation-model/{MODEL}")}],"updatedAt":"2025-01-01T00:00:00Z"})
}
fn inspect_context() -> ModelInspectionContext {
    ModelInspectionContext {
        scope: scope(),
        principal_ref: id("user"),
        capability_grant_ref: id("grant"),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(3),
    }
}
fn snapshot() -> BedrockSnapshot {
    BedrockSnapshot {
        model_id: id(MODEL),
        model_version: id("release"),
        evidence_ref: id("documented-claude-release"),
    }
}

#[tokio::test]
async fn foundation_metadata_needs_its_own_iam_access_and_actual_release_evidence() {
    let server = Server::new(vec![
        Reply::json(200, foundation()),
        Reply::json(200, foundation()),
    ])
    .await;
    let connection = connection(&server);
    let request = request(&connection, MODEL);
    let unknown = BedrockInspector::new(connection.clone(), credentials(), vec![])
        .unwrap()
        .inspect(&request.route, &inspect_context())
        .await
        .unwrap();
    assert!(unknown.model_version.is_none());
    assert_eq!(unknown.version_semantics, VersionSemantics::Unverified);
    let known = BedrockInspector::new(connection.clone(), credentials(), vec![snapshot()])
        .unwrap()
        .inspect(&request.route, &inspect_context())
        .await
        .unwrap();
    known
        .validate(&request.route, VersionPolicy::RequirePinned)
        .unwrap();
    let denied = BedrockInspector::new(
        connection,
        Arc::new(BedrockCredential::Bearer("inference-only".into())),
        vec![snapshot()],
    )
    .unwrap()
    .inspect(&request.route, &inspect_context())
    .await
    .unwrap_err();
    assert_eq!(denied.code, ErrorCode::AccessDenied);
    let calls = server.requests.lock().unwrap();
    assert_eq!(calls.len(), 2);
    for call in calls.iter() {
        assert_eq!(call.method, "GET");
        assert_eq!(call.path, format!("/foundation-models/{MODEL}"));
        assert!(call.headers.contains("/us-east-1/bedrock/aws4_request"));
        assert!(call.body.is_null());
    }
}

#[tokio::test]
async fn profile_destinations_and_revision_are_checked_separately_from_origin_region() {
    for mode in ["allowed", "destination", "model-drift"] {
        let mut changed = profile();
        if mode == "model-drift" {
            changed["models"][1]["modelArn"] =
                json!("arn:aws:bedrock:us-west-2::foundation-model/anthropic.other-model");
        } else {
            changed["updatedAt"] = json!("2025-02-01T00:00:00Z");
        }
        let server =
            Server::new(vec![Reply::json(200, profile()), Reply::json(200, changed)]).await;
        let mut options = options(&server);
        options.selector = BedrockSelector::InferenceProfile("profile".into());
        options.allowed_destination_regions = if mode == "destination" {
            vec!["us-east-1".into()]
        } else {
            vec!["us-east-1".into(), "us-west-2".into()]
        };
        let connection =
            BedrockConnection::new(scope(), reference("account"), credentials(), options)
                .unwrap()
                .with_clock(Arc::new(FixedClock));
        let mut request = request(&connection, MODEL);
        request.route.version_semantics = VersionSemantics::MutableDeployment;
        let inspector = BedrockInspector::new(connection, credentials(), vec![snapshot()]).unwrap();
        let first = inspector.inspect(&request.route, &inspect_context()).await;
        if mode == "destination" {
            assert_eq!(first.unwrap_err().code, ErrorCode::AccessDenied);
            continue;
        }
        let first = first.unwrap();
        first
            .validate(&request.route, VersionPolicy::AllowMutable)
            .unwrap();
        assert_eq!(first.version_semantics, VersionSemantics::MutableDeployment);
        request.route.deployment_revision = first.deployment_revision;
        let next = inspector.inspect(&request.route, &inspect_context()).await;
        if mode == "model-drift" {
            assert_eq!(next.unwrap_err().code, ErrorCode::ModelVersionDrift);
        } else {
            assert_eq!(
                next.unwrap()
                    .validate(&request.route, VersionPolicy::AllowMutable)
                    .unwrap_err()
                    .code,
                ErrorCode::ModelVersionDrift
            );
        }
        assert_eq!(
            server.requests.lock().unwrap()[0].path,
            "/inference-profiles/profile"
        );
    }
}

#[tokio::test]
async fn aws_exception_frames_and_http_failures_do_not_retry_or_expose_private_messages() {
    for binary in [false, true] {
        let mut response = Reply::json(429, json!({"message":"private details"}));
        if binary {
            let msg = Message::new(br#"{"message":"private details"}"#.to_vec())
                .add_header(Header::new(
                    ":message-type",
                    HeaderValue::String("exception".into()),
                ))
                .add_header(Header::new(
                    ":exception-type",
                    HeaderValue::String("throttlingException".into()),
                ));
            response.status = 200;
            response.content_type = "application/vnd.amazon.eventstream";
            response.body.clear();
            aws_smithy_eventstream::frame::write_message_to(&msg, &mut response.body).unwrap();
        }
        let server = Server::new(vec![response]).await;
        let connection = configured(
            &server,
            BedrockOperation::InvokeStream,
            BedrockEndpoint::Runtime,
            false,
        );
        let request = request(&connection, MODEL);
        let model = BedrockModel::new(connection);
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, ModelFailureKind::RateLimited);
        assert!(!format!("{failure:?}").contains("private details"));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn cancellation_and_deadline_close_both_stream_types_without_retry() {
    for operation in [BedrockOperation::Messages, BedrockOperation::InvokeStream] {
        for cancel in [true, false] {
            let mut data = events(MODEL, "partial");
            data.truncate(6);
            let mut response = reply(operation, &data);
            response.stall = true;
            let server = Server::new(vec![response]).await;
            let connection = configured(&server, operation, BedrockEndpoint::Runtime, false);
            let request = request(&connection, MODEL);
            let model = BedrockModel::new(connection);
            let mut context = context(&request);
            context.deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            let mut stream = model.generate(&request, &context);
            assert!(matches!(
                stream.next().await.unwrap().unwrap(),
                ModelEvent::TextDelta { .. }
            ));
            server.entered.notified().await;
            if cancel {
                context.cancellation.cancel();
            }
            let next = stream.next().await.unwrap();
            if cancel {
                assert_eq!(next.unwrap_err().code, ErrorCode::Cancelled);
            } else {
                assert!(matches!(
                    next.unwrap(),
                    ModelEvent::ResponseError {
                        kind: ModelFailureKind::Timeout,
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
}

#[tokio::test]
async fn redirects_never_forward_signed_credentials() {
    let destination = Server::new(vec![]).await;
    let mut response = Reply::json(307, json!({}));
    response
        .headers
        .push(("location", destination.base.clone()));
    let server = Server::new(vec![response]).await;
    let connection = connection(&server);
    let request = request(&connection, MODEL);
    let model = BedrockModel::new(connection);
    assert!(
        collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .is_err()
    );
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    assert!(destination.requests.lock().unwrap().is_empty());
}

fn tool_events() -> Vec<Value> {
    let mut data = events(MODEL, "");
    data.truncate(4);
    data.extend([
        json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"lookup","input":{},"caller":{"type":"direct"}}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"query\":"}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"\"alpha\"}"}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":20}}),
        json!({"type":"message_stop"}),
    ]);
    data
}
fn tool(request: &mut ModelRequest) {
    request.tools = vec![ModelTool {
        name: id("lookup"),
        description: "Read a record".into(),
        model_input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer"}},"required":["query"],"additionalProperties":false}),
    }];
}

#[tokio::test]
async fn profile_arn_and_signed_tool_continuation_survive_both_wire_operations() {
    let arn = "arn:aws:bedrock:us-east-1:123456789012:inference-profile/profile";
    for operation in [BedrockOperation::Messages, BedrockOperation::InvokeStream] {
        let server = Server::new(vec![
            reply(operation, &tool_events()),
            reply(operation, &events(MODEL, "42")),
        ])
        .await;
        let mut options = options(&server);
        options.operation = operation;
        options.selector = BedrockSelector::InferenceProfile(arn.into());
        let connection =
            BedrockConnection::new(scope(), reference("account"), credentials(), options)
                .unwrap()
                .with_clock(Arc::new(FixedClock));
        let mut request = request(&connection, MODEL);
        tool(&mut request);
        let model = BedrockModel::new(connection);
        let first = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(first.finish, ModelFinish::ToolCalls);
        assert_eq!(first.tool_calls[0].model_inputs["query"], "alpha");
        request.messages.push(ModelMessage {
            role: ModelRole::Assistant,
            content: vec![
                ModelContent::ToolCall {
                    provider_call_id: id("toolu_1"),
                    name: id("lookup"),
                    arguments: JsonObject::from([("query".into(), json!("alpha"))]),
                },
                ModelContent::Opaque {
                    continuation: first.continuation[0].clone(),
                },
            ],
        });
        request.messages.push(ModelMessage {
            role: ModelRole::Tool,
            content: vec![ModelContent::ToolResult {
                provider_call_id: id("toolu_1"),
                content: json!({"answer":42}),
            }],
        });
        request.request_id = id("next-attempt");
        let second = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(second.text, "42");
        let calls = server.requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        let body = &calls[1].body;
        let blocks = body["messages"][1]["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["signature"], "signature-fixture");
        assert_eq!(blocks[1]["id"], "toolu_1");
        assert_eq!(blocks[1]["input"]["query"], "alpha");
        assert_eq!(body["messages"][2]["content"][0]["tool_use_id"], "toolu_1");
        match operation {
            BedrockOperation::Messages => assert_eq!(body["model"], arn),
            BedrockOperation::InvokeStream => assert_eq!(
                calls[1].path,
                "/model/arn:aws:bedrock:us-east-1:123456789012:inference-profile%2Fprofile/invoke-with-response-stream"
            ),
        }
    }
}

struct AdvancingCredentials {
    clock: Arc<std::sync::atomic::AtomicI64>,
    expired: bool,
}
struct AdvancingClock(Arc<std::sync::atomic::AtomicI64>);
impl Clock for AdvancingClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: self.0.load(std::sync::atomic::Ordering::SeqCst),
            monotonic_ms: 0,
        })
    }
    fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}
impl BedrockCredentialProvider for AdvancingCredentials {
    fn credential<'a>(
        &'a self,
        _: &'a BedrockCredentialContext<'a>,
    ) -> PortFuture<'a, BedrockCredential> {
        Box::pin(async move {
            self.clock
                .store(1735689610000, std::sync::atomic::Ordering::SeqCst);
            Ok(BedrockCredential::Aws(AwsCredentials::new(
                "AKIDEXAMPLE",
                "secret",
                None,
                self.expired
                    .then_some(std::time::UNIX_EPOCH + Duration::from_secs(1735689605)),
                "fixture",
            )))
        })
    }
}
#[tokio::test]
async fn credential_expiry_and_signature_use_time_after_refresh() {
    for expired in [true, false] {
        let server = Server::new(vec![reply(
            BedrockOperation::Messages,
            &events(MODEL, "42"),
        )])
        .await;
        let clock = Arc::new(std::sync::atomic::AtomicI64::new(1735689600000));
        let credentials = Arc::new(AdvancingCredentials {
            clock: clock.clone(),
            expired,
        });
        let connection =
            BedrockConnection::new(scope(), reference("account"), credentials, options(&server))
                .unwrap()
                .with_clock(Arc::new(AdvancingClock(clock)));
        let request = request(&connection, MODEL);
        let model = BedrockModel::new(connection);
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        let calls = server.requests.lock().unwrap();
        if expired {
            assert_eq!(result.unwrap_err().kind, ModelFailureKind::Authentication);
            assert!(calls.is_empty());
        } else {
            assert_eq!(result.unwrap().text, "42");
            assert!(
                calls[0]
                    .headers
                    .to_ascii_lowercase()
                    .contains("x-amz-date: 20250101t000010z")
            );
        }
    }
}
struct PendingCredentials(tokio::sync::Notify);
impl BedrockCredentialProvider for PendingCredentials {
    fn credential<'a>(
        &'a self,
        _: &'a BedrockCredentialContext<'a>,
    ) -> PortFuture<'a, BedrockCredential> {
        Box::pin(async move {
            self.0.notify_one();
            std::future::pending().await
        })
    }
}
#[tokio::test]
async fn credential_lookup_obeys_cancellation_and_deadline_before_http() {
    for cancel in [true, false] {
        let server = Server::new(vec![]).await;
        let credentials = Arc::new(PendingCredentials(tokio::sync::Notify::new()));
        let connection = BedrockConnection::new(
            scope(),
            reference("account"),
            credentials.clone(),
            options(&server),
        )
        .unwrap();
        let request = request(&connection, MODEL);
        let model = BedrockModel::new(connection);
        let mut context = context(&request);
        context.deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let result = async { model.generate(&request, &context).next().await.unwrap() };
        let trigger = async {
            credentials.0.notified().await;
            if cancel {
                context.cancellation.cancel();
            }
        };
        let (result, _) = tokio::join!(result, trigger);
        if cancel {
            assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
        } else {
            assert!(matches!(
                result.unwrap(),
                ModelEvent::ResponseError {
                    kind: ModelFailureKind::Timeout,
                    ..
                }
            ));
        }
        assert!(server.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn two_documented_releases_keep_independent_selectors() {
    let releases = ["anthropic.claude-opus-5", "anthropic.claude-opus-4-8"];
    for endpoint in [BedrockEndpoint::Runtime, BedrockEndpoint::Mantle] {
        let server = Server::new(
            releases
                .iter()
                .map(|release| reply(BedrockOperation::Messages, &events(release, release)))
                .collect(),
        )
        .await;
        let mut bindings = vec![];
        for (index, release) in releases.iter().enumerate() {
            let mut options = options(&server);
            options.selector = BedrockSelector::Foundation((*release).into());
            options.operation = BedrockOperation::Messages;
            options.endpoint = endpoint;
            let connection = BedrockConnection::new(
                scope(),
                reference(&format!("account-{index}")),
                Arc::new(BedrockCredential::Bearer("fixture-bearer".into())),
                options,
            )
            .unwrap()
            .with_clock(Arc::new(FixedClock));
            let mut request = request(&connection, release);
            request.route.model_version = id(&format!("fixture-revision-{index}"));
            bindings.push((BedrockModel::new(connection), request));
        }
        for ((model, request), release) in bindings.iter().zip(releases) {
            let result =
                collect_model_response(request, model.generate(request, &context(request)))
                    .await
                    .unwrap();
            assert_eq!(result.text, release);
            assert_eq!(result.metadata.reported_model_id, Some(id(release)));
            assert!(result.metadata.reported_model_version.is_none());
        }
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        for (request, release) in requests.iter().zip(releases) {
            assert_eq!(request.body["model"], release);
            assert_eq!(request.path, "/anthropic/v1/messages");
        }
    }
}

#[tokio::test]
async fn named_http_stream_errors_are_recoverable_without_classifying_every_424_as_transient() {
    for (header, body, expected) in [
        (
            Some("ModelStreamErrorException"),
            json!({}),
            ModelFailureKind::Transport,
        ),
        (
            Some("aws.bedrock#ModelStreamErrorException:legacy"),
            json!({}),
            ModelFailureKind::Transport,
        ),
        (
            None,
            json!({"__type":"aws.bedrock#ModelStreamErrorException"}),
            ModelFailureKind::Transport,
        ),
        (
            None,
            json!({"code":"ModelStreamErrorException:legacy"}),
            ModelFailureKind::Transport,
        ),
        (
            Some("ModelErrorException"),
            json!({"__type":"ModelStreamErrorException"}),
            ModelFailureKind::Unsupported,
        ),
        (
            None,
            json!({"code":"ModelErrorException"}),
            ModelFailureKind::Unsupported,
        ),
        (None, json!({}), ModelFailureKind::Unsupported),
        (
            None,
            json!({"code":"UnknownException","message":"ModelStreamErrorException"}),
            ModelFailureKind::Unsupported,
        ),
    ] {
        let mut response = Reply::json(424, body);
        if let Some(header) = header {
            response.headers.push(("x-amzn-errortype", header.into()));
        }
        let server = Server::new(vec![response]).await;
        let connection = configured(
            &server,
            BedrockOperation::InvokeStream,
            BedrockEndpoint::Runtime,
            false,
        );
        let request = request(&connection, MODEL);
        let model = BedrockModel::new(connection);
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, expected, "header {header:?}");
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn error_classification_does_not_wait_for_named_bodies_and_bounds_unknown_bodies() {
    for case in ["header-stall", "body-deadline", "body-limit"] {
        let mut response = Reply::json(
            424,
            json!({"__type":"ModelStreamErrorException","message":"x".repeat(10000)}),
        );
        response.stall = case != "body-limit";
        if case == "header-stall" {
            response
                .headers
                .push(("x-amzn-errortype", "ModelStreamErrorException".into()));
        }
        let server = Server::new(vec![response]).await;
        let mut options = options(&server);
        options.operation = BedrockOperation::InvokeStream;
        if case == "body-limit" {
            options.max_transport_bytes = 64;
            options.max_event_bytes = 64;
        }
        let connection =
            BedrockConnection::new(scope(), reference("account"), credentials(), options)
                .unwrap()
                .with_clock(Arc::new(FixedClock));
        let request = request(&connection, MODEL);
        let model = BedrockModel::new(connection);
        let mut context = context(&request);
        if case == "body-deadline" {
            context.deadline = tokio::time::Instant::now() + Duration::from_millis(50);
        }
        let failure = tokio::time::timeout(
            Duration::from_secs(1),
            collect_model_response(&request, model.generate(&request, &context)),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(
            failure.kind,
            match case {
                "header-stall" => ModelFailureKind::Transport,
                "body-deadline" => ModelFailureKind::Timeout,
                _ => ModelFailureKind::Unsupported,
            }
        );
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}
```

## `crates/wickle-model-bedrock/tests/support/mod.rs`

```rust
use aws_smithy_types::event_stream::{Header, HeaderValue, Message};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use wickle::*;
use wickle_model_bedrock::*;

pub fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
pub fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
pub fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("hidden-workspace"),
        user_id: None,
    }
}
pub const MODEL: &str = "anthropic.claude-opus-5";
pub fn origin(server: &Server) -> String {
    server.base.trim_end_matches("v1/").into()
}
pub struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: 1735689600000,
            monotonic_ms: 0,
        })
    }
    fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}
pub fn credentials() -> std::sync::Arc<dyn BedrockCredentialProvider> {
    std::sync::Arc::new(BedrockCredential::Aws(AwsCredentials::new(
        "AKIDEXAMPLE",
        "test-secret",
        Some("test-session".into()),
        None,
        "fixture",
    )))
}
pub fn options(server: &Server) -> BedrockOptions {
    let mut options = BedrockOptions::new("us-east-1", BedrockSelector::Foundation(MODEL.into()));
    options.endpoint_url = Some(origin(server));
    options.metadata_url = Some(origin(server));
    options
}
pub fn connection(server: &Server) -> BedrockConnection {
    BedrockConnection::new(
        scope(),
        reference("account"),
        credentials(),
        options(server),
    )
    .unwrap()
    .with_clock(std::sync::Arc::new(FixedClock))
}
pub fn request(connection: &BedrockConnection, model: &str) -> ModelRequest {
    let binding = connection.binding();
    ModelRequest {
        request_id: id("attempt"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference("primary"),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id(model),
            model_id: id(model),
            model_version: id("release"),
            version_semantics: VersionSemantics::Pinned,
            provider: binding.provider,
            target: connection.target().clone(),
            deployment_revision: None,
            api_contract: connection.api_contract(),
            adapter: binding.adapter,
            capability_revision: id("capabilities"),
            connection_ref: binding.connection_ref,
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find the requested figures".into(),
            }],
        }],
        tools: vec![],
        output: ModelOutput::Text {},
        max_output_tokens: 128.try_into().unwrap(),
        options: JsonObject::from([("effort".into(), json!("medium"))]),
        limits: ModelResponseLimits {
            max_input_bytes: 32_768,
            max_response_bytes: 32_768,
            max_delta_bytes: 128,
            max_events: 256,
            max_tool_calls: 4,
        },
    }
}
pub fn context(request: &ModelRequest) -> ModelCallContext {
    ModelCallContext {
        attempt_id: request.request_id.clone(),
        run_id: id("run"),
        scope: scope(),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(5),
    }
}
pub fn events(model: &str, text: &str) -> Vec<Value> {
    vec![
        json!({"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":model,"content":[],"stop_reason":null,"usage":{"input_tokens":25,"output_tokens":1}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signature-fixture"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":text}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":15}}),
        json!({"type":"message_stop"}),
    ]
}

#[path = "../../../../tests/support/model_http.rs"]
mod http;
pub use http::*;

pub fn frame(value: &Value) -> Vec<u8> {
    let payload =
        serde_json::to_vec(&json!({"bytes":STANDARD.encode(serde_json::to_vec(value).unwrap())}))
            .unwrap();
    let message = Message::new(payload)
        .add_header(Header::new(
            ":message-type",
            HeaderValue::String("event".into()),
        ))
        .add_header(Header::new(
            ":event-type",
            HeaderValue::String("chunk".into()),
        ))
        .add_header(Header::new(
            ":content-type",
            HeaderValue::String("application/json".into()),
        ));
    let mut bytes = vec![];
    aws_smithy_eventstream::frame::write_message_to(&message, &mut bytes).unwrap();
    bytes
}
pub fn reply(operation: BedrockOperation, data: &[Value]) -> Reply {
    let mut reply = Reply::sse(data);
    reply.headers = vec![("x-amzn-requestid", "aws-request".into())];
    if operation == BedrockOperation::InvokeStream {
        reply.content_type = "application/vnd.amazon.eventstream";
        reply.body = data.iter().flat_map(frame).collect();
        reply.chunk = 3;
    }
    reply
}
```

## 문맥을 위한 기존 파일: `crates/wickle-model-bedrock/src/framing.rs`

이 파일은 이 checkpoint에서 새로 수정된 파일은 아니지만, 강의의 실행 경계를 읽기 위해 당시 전체 코드를 함께 제공한다. 실제 변경 위치는 patch를 따른다.

```rust
use crate::{BedrockOperation, BedrockOptions, error};
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::BytesMut;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use wickle::*;
use wickle_model_responses::{SseDecoder, SseEvent};

pub(crate) enum Framing {
    Sse(SseDecoder),
    Aws(AwsFrames),
}
impl Framing {
    pub fn new(options: &BedrockOptions) -> Self {
        match options.operation {
            BedrockOperation::Messages => Self::Sse(SseDecoder::new(
                options.max_transport_bytes,
                options.max_event_bytes,
                options.max_protocol_events,
            )),
            BedrockOperation::InvokeStream => Self::Aws(AwsFrames {
                buffer: BytesMut::new(),
                bytes: 0,
                frames: 0,
                max_bytes: options.max_transport_bytes,
                max_frame: options.max_event_bytes,
                max_frames: options.max_protocol_events,
            }),
        }
    }
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseEvent>, ContractError> {
        match self {
            Self::Sse(value) => value.push(bytes),
            Self::Aws(value) => value.push(bytes),
        }
    }
    pub fn finish(&self) -> Result<(), ContractError> {
        match self {
            Self::Sse(value) => value.finish(),
            Self::Aws(value) => {
                if value.buffer.is_empty() {
                    Ok(())
                } else {
                    Err(invalid())
                }
            }
        }
    }
}
pub(crate) struct AwsFrames {
    buffer: BytesMut,
    bytes: usize,
    frames: usize,
    max_bytes: usize,
    max_frame: usize,
    max_frames: usize,
}
impl AwsFrames {
    fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseEvent>, ContractError> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|n| *n <= self.max_bytes)
            .ok_or_else(invalid)?;
        self.buffer.extend_from_slice(bytes);
        let mut events = vec![];
        while self.buffer.len() >= 4 {
            let len =
                u32::from_be_bytes(self.buffer[..4].try_into().map_err(|_| invalid())?) as usize;
            if len < 16 || len > self.max_frame {
                return Err(invalid());
            }
            if self.buffer.len() < len {
                break;
            }
            self.frames = self
                .frames
                .checked_add(1)
                .filter(|n| *n <= self.max_frames)
                .ok_or_else(invalid)?;
            let frame = self.buffer.split_to(len).freeze();
            // The AWS library validates both prelude and message CRCs and headers.
            let message = aws_smithy_eventstream::frame::read_message_from(&frame[..])
                .map_err(|_| invalid())?;
            let mut headers = BTreeMap::new();
            for header in message.headers() {
                let name = header.name().as_str();
                if headers.contains_key(name) {
                    return Err(invalid());
                }
                headers.insert(name, header.value());
            }
            let text = |key| {
                headers
                    .get(key)
                    .and_then(|v| v.as_string().ok())
                    .map(|v| v.as_str())
            };
            match text(":message-type") {
                Some("event") if text(":event-type") == Some("chunk") => {
                    if text(":content-type").is_some_and(|v| v != "application/json") {
                        return Err(invalid());
                    }
                    let body = std::str::from_utf8(message.payload()).map_err(|_| invalid())?;
                    let body = parse_json(body)?;
                    let encoded = body
                        .get("bytes")
                        .and_then(Value::as_str)
                        .ok_or_else(invalid)?;
                    let decoded = STANDARD.decode(encoded).map_err(|_| invalid())?;
                    let data = String::from_utf8(decoded).map_err(|_| invalid())?;
                    // The Messages decoder owns JSON/block/terminal validation.
                    events.push(SseEvent { name: None, data });
                }
                Some("exception" | "error") => {
                    let kind = match text(":exception-type").or_else(|| text(":error-code")) {
                        Some("throttlingException") => "rate_limit_error",
                        Some("modelTimeoutException") => {
                            return Err(error(ErrorCode::DeadlineExceeded, "model_timeout"));
                        }
                        Some(
                            "internalServerException"
                            | "modelStreamErrorException"
                            | "serviceUnavailableException",
                        ) => "api_error",
                        Some("validationException") => "invalid_request_error",
                        Some("accessDeniedException") => "permission_error",
                        _ => return Err(invalid()),
                    };
                    events.push(SseEvent {
                        name: Some("error".into()),
                        data: json!({"type":"error","error":{"type":kind}}).to_string(),
                    });
                }
                _ => return Err(invalid()),
            }
        }
        Ok(events)
    }
}
fn invalid() -> ContractError {
    error(ErrorCode::InvalidContract, "aws_event_stream")
}
```
