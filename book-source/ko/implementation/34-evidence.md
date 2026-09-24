# 34장 전체 Rust 구현과 테스트

[강의로](../34-evidence.md) · [전체 변경 패치](../solutions/34-evidence.patch)

기준 `5a35b0e4cf484463005bb6a7a7eb4b396af3fc0f`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle-model-anthropic/tests/messages.rs`

```rust
//! Anthropic HTTP/SSE contracts, signed replay, cancellation and metadata.
mod support;
use futures_util::StreamExt;
use serde_json::{Value, json};
use support::*;
use wickle::*;
use wickle_model_anthropic::*;

#[tokio::test]
async fn thinking_before_text_and_cumulative_usage_are_not_misread_as_visible_output() {
    let mut data = events("claude-opus-5", "42");
    data.insert(2,json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"private reasoning"}}));
    let mut reply = Reply::sse(&data);
    reply.headers = vec![("request-id", "req-claude".into())];
    let server = Server::new(vec![reply]).await;
    let connection = AnthropicConnection::new(
        scope(),
        reference("account"),
        "fixture-key",
        AnthropicOptions {
            base_url: server.base.trim_end_matches("v1/").into(),
            workspace_id: Some("wrkspc_fixture".into()),
            ..Default::default()
        },
    )
    .unwrap();
    let request = request(&connection, "claude-opus-5");
    let model = AnthropicModel::new(connection);
    let response = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(response.text, "42");
    assert_eq!(response.finish, ModelFinish::Stop);
    assert_eq!(
        response.metadata.provider_request_id,
        Some(id("req-claude"))
    );
    let usage = response.metadata.usage.unwrap();
    assert_eq!(usage.input_tokens, Some(25));
    assert_eq!(usage.output_tokens, Some(15));
    assert!(response.metadata.reported_model_version.is_none());
    assert_eq!(
        response.continuation[0].data()["blocks"][0]["thinking"],
        "private reasoning"
    );
    assert_eq!(
        response.continuation[0].data()["blocks"][0]["signature"],
        "signature-fixture"
    );
    let calls = server.requests.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let call = &calls[0];
    assert_eq!(call.method, "POST");
    assert_eq!(call.path, "/v1/messages");
    let headers = call.headers.to_ascii_lowercase();
    assert!(headers.contains("authorization: bearer fixture-key"));
    assert!(headers.contains("anthropic-version: 2023-06-01"));
    assert!(headers.contains("anthropic-workspace-id: wrkspc_fixture"));
    assert_eq!(call.body["model"], "claude-opus-5");
    assert_eq!(call.body["output_config"]["effort"], "medium");
    assert!(call.body.get("thinking").is_none());
    assert!(call.body.get("fallbacks").is_none());
    assert!(!call.body.to_string().contains("hidden-workspace"));
    assert!(!call.body.to_string().contains("fixture-key"));
}

fn tool_events() -> Vec<Value> {
    let mut data = events("claude-opus-5", "");
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
async fn signed_empty_thinking_and_tool_inputs_are_replayed_once_with_results() {
    let server = Server::new(vec![
        Reply::sse(&tool_events()),
        Reply::sse(&events("claude-opus-5", "{\"answer\":42}")),
    ])
    .await;
    let connection = connection(&server);
    let model = AnthropicModel::new(connection.clone());
    let mut request = request(&connection, "claude-opus-5");
    tool(&mut request);
    let response = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(response.tool_calls.len(), 1);
    assert_eq!(response.tool_calls[0].model_inputs["query"], "alpha");
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![
            ModelContent::ToolCall {
                provider_call_id: id("toolu_1"),
                name: id("lookup"),
                arguments: JsonObject::from([("query".into(), json!("alpha"))]),
            },
            ModelContent::Opaque {
                continuation: response.continuation[0].clone(),
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
    request.output = ModelOutput::JsonSchema {
        schema: json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}),
    };
    request.request_id = id("next-attempt");
    let response = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(parse_json(&response.text).unwrap(), json!({"answer":42}));
    {
        let calls = server.requests.lock().unwrap();
        let body = &calls[1].body;
        assert_eq!(body["messages"][1]["content"].as_array().unwrap().len(), 2);
        assert_eq!(
            body["messages"][1]["content"][0],
            json!({"type":"thinking","thinking":"","signature":"signature-fixture"})
        );
        assert_eq!(body["messages"][1]["content"][1]["type"], "tool_use");
        assert_eq!(
            body["messages"][1]["content"][1]["caller"],
            json!({"type":"direct"})
        );
        assert_eq!(body["messages"][2]["role"], "user");
        assert_eq!(body["messages"][2]["content"][0]["tool_use_id"], "toolu_1");
        assert_eq!(body["output_config"]["format"]["type"], "json_schema");
        assert_eq!(
            body["tools"][0]["input_schema"]["required"],
            json!(["query"])
        );
    }
    if let ModelContent::ToolCall { arguments, .. } = &mut request.messages[1].content[0] {
        arguments.insert("query".into(), json!("tampered"));
    }
    assert!(
        collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .is_err()
    );
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn invalid_scope_options_and_model_thinking_combinations_stop_before_http() {
    for case in [
        "scope", "target", "attempt", "binding", "api", "option", "effort", "manual", "disabled",
        "prefill", "schema",
    ] {
        let server = Server::new(vec![]).await;
        let connection = connection(&server);
        let model = AnthropicModel::new(connection.clone());
        let mut request = request(&connection, "claude-opus-5");
        let mut context = context(&request);
        match case {
            "scope" => context.scope.workspace_id = id("other"),
            "attempt" => context.attempt_id = id("other-attempt"),
            "binding" => request.route.connection_ref = reference("other-account"),
            "api" => request.route.api_contract.version = id("unsupported"),
            "target" => {
                request
                    .route
                    .target
                    .insert("base_url".into(), json!("https://wrong.example/"));
            }
            "option" => {
                request.options.insert("fallbacks".into(), json!("default"));
            }
            "effort" => {
                request.options.insert("effort".into(), json!("adaptive"));
            }
            "manual" => {
                request.max_output_tokens = 4096.try_into().unwrap();
                request
                    .options
                    .insert("thinking_mode".into(), json!("enabled"));
                request
                    .options
                    .insert("thinking_budget_tokens".into(), json!(1024));
            }
            "disabled" => {
                request
                    .options
                    .insert("thinking_mode".into(), json!("disabled"));
                request.options.insert("effort".into(), json!("max"));
            }
            "prefill" => request.messages.push(ModelMessage {
                role: ModelRole::Assistant,
                content: vec![ModelContent::Text {
                    text: "prefix".into(),
                }],
            }),
            "schema" => {
                request.output = ModelOutput::JsonSchema {
                    schema: json!({"type":"object","properties":{"n":{"type":"integer","minimum":1}},"additionalProperties":false}),
                }
            }
            _ => unreachable!(),
        }
        assert!(
            collect_model_response(&request, model.generate(&request, &context))
                .await
                .is_err(),
            "{case}"
        );
        assert!(server.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn truncated_unsigned_misordered_and_conflicting_streams_are_not_successes() {
    for case in [
        "truncated",
        "unsigned",
        "duplicate-start",
        "wrong-index",
        "open-block",
        "usage",
        "double-stop",
        "native-tool",
        "partial-json",
    ] {
        let mut data = if case == "partial-json" {
            tool_events()
        } else {
            events("claude-opus-5", "answer")
        };
        match case {
            "truncated" => {
                data.pop();
            }
            "unsigned" => {
                data.remove(2);
            }
            "duplicate-start" => data.insert(1, data[0].clone()),
            "wrong-index" => data[5]["index"] = json!(0),
            "open-block" => {
                data.remove(6);
            }
            "usage" => data[7]["usage"]["output_tokens"] = json!(0),
            "double-stop" => data.push(json!({"type":"message_stop"})),
            "native-tool" => data[4]["content_block"]["type"] = json!("server_tool_use"),
            "partial-json" => data[6]["delta"]["partial_json"] = json!("\"alpha\""),
            _ => unreachable!(),
        }
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let model = AnthropicModel::new(connection.clone());
        let mut request = request(&connection, "claude-opus-5");
        tool(&mut request);
        assert!(
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .is_err(),
            "{case}"
        );
    }
}

#[tokio::test]
async fn model_metadata_uses_anthropic_type_and_registered_snapshot_facts() {
    let server = Server::new(vec![
        Reply::json(
            200,
            json!({"id":"claude-opus-5","type":"model","display_name":"Claude Opus 5"}),
        ),
        Reply::json(200, json!({"id":"claude-opus-5","type":"model"})),
    ])
    .await;
    let connection = connection(&server);
    let request = request(&connection, "claude-opus-5");
    let context = ModelInspectionContext {
        scope: scope(),
        principal_ref: id("user"),
        capability_grant_ref: id("grant"),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(3),
    };
    let unknown = AnthropicInspector::new(connection.clone(), vec![])
        .unwrap()
        .inspect(&request.route, &context)
        .await
        .unwrap();
    assert!(unknown.model_version.is_none());
    assert_eq!(unknown.version_semantics, VersionSemantics::Unverified);
    let known = AnthropicInspector::new(
        connection,
        vec![AnthropicSnapshot {
            model_id: id("claude-opus-5"),
            model_version: id("release"),
            evidence_ref: id("documented-release"),
        }],
    )
    .unwrap()
    .inspect(&request.route, &context)
    .await
    .unwrap();
    known
        .validate(&request.route, VersionPolicy::RequirePinned)
        .unwrap();
    for call in server.requests.lock().unwrap().iter() {
        assert_eq!(call.method, "GET");
        assert_eq!(call.path, "/v1/models/claude-opus-5");
    }
}

#[tokio::test]
async fn cancellation_and_deadline_close_streams_without_retry() {
    for cancel in [true, false] {
        let mut data = events("claude-opus-5", "partial");
        data.truncate(6);
        let mut reply = Reply::sse(&data);
        reply.stall = true;
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let model = AnthropicModel::new(connection.clone());
        let request = request(&connection, "claude-opus-5");
        let mut context = context(&request);
        context.deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(300);
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
        tokio::time::timeout(std::time::Duration::from_secs(1), server.closed.notified())
            .await
            .unwrap();
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn redacted_thinking_limits_and_error_modes_preserve_safe_completion() {
    for case in [
        "redacted",
        "length",
        "refusal",
        "pause",
        "limit",
        "native-caller",
    ] {
        let mut data = if case == "native-caller" {
            tool_events()
        } else {
            events("claude-opus-5", "answer")
        };
        match case {
            "redacted" => {
                data[1]["content_block"] =
                    json!({"type":"redacted_thinking","data":"opaque-redacted"});
                data.remove(2);
            }
            "length" => data[7]["delta"]["stop_reason"] = json!("max_tokens"),
            "refusal" => data[7]["delta"]["stop_reason"] = json!("refusal"),
            "pause" => data[7]["delta"]["stop_reason"] = json!("pause_turn"),
            "native-caller" => {
                data[4]["content_block"]["caller"] =
                    json!({"type":"code_execution_20260120","tool_id":"srvtoolu_1"})
            }
            _ => {}
        }
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let model = AnthropicModel::new(connection.clone());
        let mut request = request(&connection, "claude-opus-5");
        tool(&mut request);
        if case == "limit" {
            request.limits.max_response_bytes = 8;
        }
        let events: Vec<_> = model.generate(&request, &context(&request)).collect().await;
        let terminal = events.last().unwrap().as_ref().unwrap();
        match case {
            "redacted" => {
                let ModelEvent::ResponseCompleted { continuation, .. } = terminal else {
                    panic!("no redacted completion")
                };
                assert_eq!(
                    continuation[0].data()["blocks"][0],
                    json!({"type":"redacted_thinking","data":"opaque-redacted"})
                );
            }
            "length" => assert!(
                matches!(terminal,ModelEvent::ResponseCompleted{finish:ModelFinish::Length,continuation,..} if continuation.is_empty())
            ),
            "refusal" => assert!(
                matches!(terminal,ModelEvent::ResponseCompleted{finish:ModelFinish::Refusal,continuation,..} if continuation.is_empty())
            ),
            _ => assert!(
                matches!(terminal, ModelEvent::ResponseError { .. }),
                "{case}"
            ),
        }
    }
}

#[tokio::test]
async fn provider_http_and_stream_errors_are_not_retried_or_leaked() {
    let redirect = Server::new(vec![]).await;
    for (status, expected) in [
        (401, ModelFailureKind::Authentication),
        (429, ModelFailureKind::RateLimited),
        (529, ModelFailureKind::Transport),
        (307, ModelFailureKind::Unsupported),
        (200, ModelFailureKind::Transport),
    ] {
        let mut reply = if status == 200 {
            Reply::sse(&[
                json!({"type":"error","error":{"type":"overloaded_error","message":"provider private detail"}}),
            ])
        } else {
            Reply::json(
                status,
                json!({"error":{"type":"api_error","message":"provider private detail"}}),
            )
        };
        if status == 307 {
            reply.headers.push(("location", redirect.base.clone()));
        }
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let request = request(&connection, "claude-opus-5");
        let model = AnthropicModel::new(connection);
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, expected);
        assert!(!format!("{failure:?}").contains("provider private detail"));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
    assert!(redirect.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn unicode_fragments_reserve_exactly_one_terminal_event() {
    for (delta_limit, event_limit, valid) in [(3, 3, true), (3, 2, false), (2, 3, false)] {
        let server = Server::new(vec![Reply::sse(&events("claude-opus-5", "한글"))]).await;
        let connection = connection(&server);
        let model = AnthropicModel::new(connection.clone());
        let mut request = request(&connection, "claude-opus-5");
        request.limits.max_delta_bytes = delta_limit;
        request.limits.max_events = event_limit;
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        if valid {
            assert_eq!(result.unwrap().text, "한글");
        } else {
            assert!(result.is_err());
        }
    }
}

#[tokio::test]
async fn inspection_missing_drift_cancellation_and_size_limits_are_reported() {
    for case in ["missing", "drift", "large", "cancel"] {
        let mut body =
            json!({"type":"model","id":if case=="drift"{"other-model"}else{"claude-opus-5"}});
        if case == "large" {
            body["unused"] = json!("x".repeat(70000));
        }
        let mut reply = Reply::json(if case == "missing" { 404 } else { 200 }, body);
        reply.stall = case == "cancel";
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let request = request(&connection, "claude-opus-5");
        let inspector = AnthropicInspector::new(connection, vec![]).unwrap();
        let context = ModelInspectionContext {
            scope: scope(),
            principal_ref: id("user"),
            capability_grant_ref: id("grant"),
            cancellation: Default::default(),
            deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(3),
        };
        let mut pending = Box::pin(inspector.inspect(&request.route, &context));
        if case == "cancel" {
            tokio::select! { _=server.entered.notified()=>{}, result=&mut pending=>panic!("inspection completed early: {result:?}") }
            context.cancellation.cancel();
        }
        let result = pending.await;
        match case {
            "missing" => {
                let observation = result.unwrap();
                assert_eq!(
                    observation.availability,
                    ModelRouteAvailability::Unavailable
                );
                assert!(observation.model_version.is_none());
            }
            "drift" => assert_eq!(result.unwrap_err().code, ErrorCode::ModelVersionDrift),
            "cancel" => {
                assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
                tokio::time::timeout(std::time::Duration::from_secs(1), server.closed.notified())
                    .await
                    .unwrap();
            }
            _ => assert_eq!(
                result.unwrap_err().code,
                ErrorCode::ModelInspectionUnavailable
            ),
        }
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn two_documented_release_ids_coexist_without_replacing_connection_state() {
    let releases = ["claude-opus-5", "claude-opus-4-8"];
    let server = Server::new(
        releases
            .iter()
            .map(|release| Reply::sse(&events(release, release)))
            .collect(),
    )
    .await;
    let connection = connection(&server);
    let model = AnthropicModel::new(connection.clone());
    for (index, release) in releases.iter().enumerate() {
        let mut request = request(&connection, release);
        request.route.binding = reference(&format!("binding-{index}"));
        request.route.model_version = id(&format!("fixture-revision-{index}"));
        let result = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(result.text, *release);
        assert_eq!(result.metadata.reported_model_id, Some(id(release)));
        assert!(result.metadata.reported_model_version.is_none());
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (request, release) in requests.iter().zip(releases) {
        assert_eq!(request.body["model"], release);
    }
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
    assert_eq!(calls[0].body["tools"][0]["strict"], false);
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
```

## `crates/wickle-model-bedrock/tests/bedrock.rs`

```rust
//! AWS signing, endpoint, binary/SSE framing and metadata contracts without AWS calls.
mod support;
use aws_smithy_types::event_stream::{Header, HeaderValue, Message};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use support::*;
use wickle::*;
use wickle_model_bedrock::*;

fn frame(value: &Value) -> Vec<u8> {
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
fn reply(operation: BedrockOperation, data: &[Value]) -> Reply {
    let mut reply = Reply::sse(data);
    reply.headers = vec![("x-amzn-requestid", "aws-request".into())];
    if operation == BedrockOperation::InvokeStream {
        reply.content_type = "application/vnd.amazon.eventstream";
        reply.body = data.iter().flat_map(frame).collect();
        reply.chunk = 3;
    }
    reply
}
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
```

## `crates/wickle-model-gemini/src/codec.rs`

```rust
use crate::{connection::model_name, error};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use wickle::*;

pub(crate) const KIND: &str = "wickle.gemini.generate_content.v1";
pub(crate) fn invalid() -> ContractError {
    error(ErrorCode::ModelContextIncompatible, "content")
}
/// Function declaration schema representation supported by the selected endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FunctionSchemaFormat {
    /// Stable Gemini API OpenAPI Schema fields, without additionalProperties.
    OpenApi,
    /// JSON Schema function declarations, supported by Gemini v1beta.
    JsonSchema,
}
/// Encode only authorized content and explicitly supported logical options.
pub fn encode_request(
    request: &ModelRequest,
    format: FunctionSchemaFormat,
) -> Result<Value, ContractError> {
    encode(request, format, false)
}
/// Encode Vertex v1 using JSON function schemas and its current text response format.
pub fn encode_vertex_request(request: &ModelRequest) -> Result<Value, ContractError> {
    let mut body = encode(request, FunctionSchemaFormat::JsonSchema, true)?;
    if !request.tools.is_empty() {
        body["toolConfig"] = json!({"functionCallingConfig":{"streamFunctionCallArguments":false}});
    }
    if let ModelOutput::JsonSchema { schema } = &request.output {
        let config = body["generationConfig"]
            .as_object_mut()
            .ok_or_else(invalid)?;
        config.remove("responseMimeType");
        config.remove("responseJsonSchema");
        config.insert(
            "responseFormat".into(),
            json!([{"text":{"mimeType":"APPLICATION_JSON","schema":schema}}]),
        );
    }
    Ok(body)
}
fn encode(
    request: &ModelRequest,
    format: FunctionSchemaFormat,
    vertex: bool,
) -> Result<Value, ContractError> {
    request.validate()?;
    let mut contents: Vec<Value> = vec![];
    let mut system = vec![];
    let mut calls: BTreeMap<String, (String, Option<String>)> = BTreeMap::new();
    let mut call_order: Vec<String> = vec![];
    let mut messages = request.messages.iter().peekable();
    while let Some(message) = messages.next() {
        let mut ordered: Vec<_> = message.content.iter().collect();
        if message.role == ModelRole::Assistant {
            call_order.clear();
        }
        if message.role == ModelRole::Tool {
            // Calls without wire IDs are associated by position. Results may arrive
            // in completion order, including separate contiguous Tool messages.
            while messages.peek().is_some_and(|m| m.role == ModelRole::Tool) {
                ordered.extend(messages.next().expect("peeked message").content.iter());
            }
            ordered.sort_by_key(|part| match part {
                ModelContent::ToolResult {
                    provider_call_id, ..
                } => call_order
                    .iter()
                    .position(|id| id == provider_call_id.as_str())
                    .unwrap_or(usize::MAX),
                _ => usize::MAX,
            });
        }
        let opaque: Vec<_> = message
            .content
            .iter()
            .filter_map(|p| {
                if let ModelContent::Opaque { continuation } = p {
                    Some(continuation)
                } else {
                    None
                }
            })
            .collect();
        let parts = if opaque.is_empty() {
            let mut parts = vec![];
            for part in ordered {
                parts.push(match part {
                    ModelContent::Text{text} if message.role!=ModelRole::Tool=>json!({"text":text}),
                    ModelContent::Json{value} if message.role!=ModelRole::Tool=>json!({"text":value.to_string()}),
                    ModelContent::ToolCall{provider_call_id,name,arguments} if message.role==ModelRole::Assistant=>{
                        call_order.push(provider_call_id.to_string());
                        calls.insert(provider_call_id.to_string(),(name.to_string(),Some(provider_call_id.to_string())));
                        json!({"functionCall":{"id":provider_call_id,"name":name,"args":arguments}})
                    },
                    ModelContent::ToolResult{provider_call_id,content} if message.role==ModelRole::Tool=>{
                        let (name,wire_id)=calls.get(provider_call_id.as_str()).ok_or_else(invalid)?;
                        let mut value=json!({"name":name,"response":if content.is_object(){content.clone()}else{json!({"result":content})}});
                        if let Some(id)=wire_id {value["id"]=json!(id);}
                        json!({"functionResponse":value})
                    },
                    _=>return Err(invalid()),
                });
            }
            parts
        } else {
            if opaque.len() != 1
                || message.role != ModelRole::Assistant
                || opaque[0].route_digest() != &request.route.digest()
            {
                return Err(invalid());
            }
            let data = opaque[0].data();
            if data["kind"] != KIND || data.as_object().is_none_or(|v| v.len() != 3) {
                return Err(invalid());
            }
            let parts = data["parts"].as_array().ok_or_else(invalid)?;
            let ids: Vec<String> =
                serde_json::from_value(data["call_ids"].clone()).map_err(|_| invalid())?;
            let mut text = String::new();
            let mut decoded = vec![];
            for part in parts {
                let item = inspect_part(part, vertex)?;
                text.push_str(&item.text);
                if let Some(call) = item.call {
                    decoded.push(call);
                }
            }
            let visible: String = message
                .content
                .iter()
                .filter_map(|p| {
                    if let ModelContent::Text { text } = p {
                        Some(text.as_str())
                    } else {
                        None
                    }
                })
                .collect();
            let projected: Vec<_> = message
                .content
                .iter()
                .filter_map(|p| {
                    if let ModelContent::ToolCall {
                        provider_call_id,
                        name,
                        arguments,
                    } = p
                    {
                        Some((provider_call_id, name, arguments))
                    } else {
                        None
                    }
                })
                .collect();
            if text != visible
                || decoded.len() != projected.len()
                || ids.len() != decoded.len()
                || message.content.iter().any(|p| {
                    !matches!(
                        p,
                        ModelContent::Text { .. }
                            | ModelContent::ToolCall { .. }
                            | ModelContent::Opaque { .. }
                    )
                })
            {
                return Err(invalid());
            }
            for ((call, (id, name, args)), local) in decoded.iter().zip(projected).zip(&ids) {
                if id.as_str() != local
                    || name.as_str() != call.name
                    || serde_json::to_value(args).map_err(|_| invalid())? != call.args
                    || call.id.as_ref().is_some_and(|wire| wire != local)
                {
                    return Err(invalid());
                }
                call_order.push(local.clone());
                calls.insert(local.clone(), (call.name.clone(), call.id.clone()));
            }
            parts.clone()
        };
        if message.role == ModelRole::System {
            if !contents.is_empty() {
                return Err(invalid());
            }
            system.extend(parts);
        } else {
            let role = if message.role == ModelRole::Assistant {
                "model"
            } else {
                "user"
            };
            if let Some(last) = contents.last_mut().filter(|v| v["role"] == role) {
                last["parts"]
                    .as_array_mut()
                    .ok_or_else(invalid)?
                    .extend(parts);
            } else {
                contents.push(json!({"role":role,"parts":parts}));
            }
        }
    }
    if contents.last().is_some_and(|v| v["role"] == "model") {
        return Err(invalid());
    }
    let mut config = json!({"candidateCount":1,"maxOutputTokens":request.max_output_tokens});
    if request.max_output_tokens.get() > i32::MAX as u64 {
        return Err(error(
            ErrorCode::ModelOptionUnsupported,
            "max_output_tokens",
        ));
    }
    let mut thinking = serde_json::Map::new();
    for (key, value) in &request.options {
        match key.as_str() {
            "thinking_level" => {
                let level = value
                    .as_str()
                    .filter(|s| matches!(*s, "minimal" | "low" | "medium" | "high"))
                    .ok_or_else(|| error(ErrorCode::ModelOptionUnsupported, "thinking_level"))?;
                if matches!(
                    model_name(request.route.model_id.as_str())?,
                    "gemini-3.7-flash" | "gemini-3.8-flash"
                ) && level == "minimal"
                {
                    return Err(error(ErrorCode::ModelOptionUnsupported, "thinking_level"));
                }
                thinking.insert("thinkingLevel".into(), json!(level.to_ascii_uppercase()));
            }
            "thinking_budget_tokens" => {
                let n = value
                    .as_i64()
                    .filter(|n| *n >= -1 && *n <= i32::MAX as i64)
                    .ok_or_else(|| error(ErrorCode::ModelOptionUnsupported, "thinking_budget"))?;
                thinking.insert("thinkingBudget".into(), json!(n));
            }
            "temperature" | "top_p" => {
                let max = if key == "temperature" { 2.0 } else { 1.0 };
                let n = value
                    .as_f64()
                    .filter(|n| *n >= 0.0 && *n <= max)
                    .ok_or_else(|| error(ErrorCode::ModelOptionUnsupported, "sampling"))?;
                config[if key == "temperature" {
                    "temperature"
                } else {
                    "topP"
                }] = json!(n);
            }
            _ => return Err(error(ErrorCode::ModelOptionUnsupported, "options")),
        }
    }
    if thinking.len() > 1 {
        return Err(error(
            ErrorCode::ModelOptionUnsupported,
            "thinking_combination",
        ));
    }
    if !thinking.is_empty() {
        config["thinkingConfig"] = Value::Object(thinking);
    }
    if let ModelOutput::JsonSchema { schema } = &request.output {
        schema_value(schema, FunctionSchemaFormat::JsonSchema)?;
        config["responseMimeType"] = json!("application/json");
        config["responseJsonSchema"] = schema.clone();
    }
    let mut body = json!({"contents":contents,"generationConfig":config});
    if !system.is_empty() {
        body["systemInstruction"] = json!({"parts":system});
    }
    if !request.tools.is_empty() {
        let declarations = request
            .tools
            .iter()
            .map(|tool| {
                if tool.name.as_str().len() > 128
                    || !tool.name.as_str().bytes().all(|b| {
                        b.is_ascii_alphanumeric() || matches!(b, b'_' | b':' | b'.' | b'-')
                    })
                {
                    return Err(error(
                        ErrorCode::ModelCapabilityUnsupported,
                        "function_name",
                    ));
                }
                let schema = schema_value(&tool.model_input_schema, format)?;
                let mut value = json!({"name":tool.name,"description":tool.description});
                value[match format {
                    FunctionSchemaFormat::OpenApi => "parameters",
                    FunctionSchemaFormat::JsonSchema => "parametersJsonSchema",
                }] = schema;
                Ok(value)
            })
            .collect::<Result<Vec<_>, ContractError>>()?;
        body["tools"] = json!([{"functionDeclarations":declarations}]);
    }
    Ok(body)
}
fn schema_value(schema: &Value, format: FunctionSchemaFormat) -> Result<Value, ContractError> {
    let object = schema
        .as_object()
        .ok_or_else(|| error(ErrorCode::ModelCapabilityUnsupported, "schema"))?;
    if format == FunctionSchemaFormat::OpenApi
        && object.get("enum").is_some_and(|v| {
            object.get("type") != Some(&json!("string"))
                || !v.as_array().is_some_and(|a| a.iter().all(Value::is_string))
        })
    {
        return Err(error(ErrorCode::ModelCapabilityUnsupported, "schema_enum"));
    }
    let mut output = serde_json::Map::new();
    for (key, value) in object {
        let translated = match key.as_str() {
            "properties" => Value::Object(
                value
                    .as_object()
                    .ok_or_else(invalid)?
                    .iter()
                    .map(|(name, sub)| Ok((name.clone(), schema_value(sub, format)?)))
                    .collect::<Result<_, ContractError>>()?,
            ),
            "items" => schema_value(value, format)?,
            "anyOf" => Value::Array(
                value
                    .as_array()
                    .ok_or_else(invalid)?
                    .iter()
                    .map(|v| schema_value(v, format))
                    .collect::<Result<_, _>>()?,
            ),
            "additionalProperties" if format == FunctionSchemaFormat::JsonSchema => {
                if value.is_boolean() {
                    value.clone()
                } else {
                    schema_value(value, format)?
                }
            }
            "type" if format == FunctionSchemaFormat::OpenApi => {
                let t = value
                    .as_str()
                    .filter(|s| {
                        matches!(
                            *s,
                            "object" | "array" | "string" | "integer" | "number" | "boolean"
                        )
                    })
                    .ok_or_else(|| error(ErrorCode::ModelCapabilityUnsupported, "schema_type"))?;
                json!(t.to_ascii_uppercase())
            }
            "type" | "required" | "enum" | "description" | "title" | "minimum" | "maximum"
            | "minItems" | "maxItems" | "format" => value.clone(),
            _ => {
                return Err(error(
                    ErrorCode::ModelCapabilityUnsupported,
                    "schema_keyword",
                ));
            }
        };
        output.insert(key.clone(), translated);
    }
    Ok(Value::Object(output))
}
pub(crate) struct Call {
    pub id: Option<String>,
    pub name: String,
    pub args: Value,
}
pub(crate) struct Part {
    pub text: String,
    pub call: Option<Call>,
}
pub(crate) fn inspect_part(part: &Value, vertex: bool) -> Result<Part, ContractError> {
    let object = part.as_object().ok_or_else(invalid)?;
    if object.keys().any(|k| {
        !matches!(
            k.as_str(),
            "text" | "thought" | "thoughtSignature" | "functionCall"
        )
    }) {
        return Err(error(ErrorCode::CapabilityUnsupported, "part"));
    }
    if part.get("thought").is_some_and(|v| !v.is_boolean())
        || part
            .get("thoughtSignature")
            .is_some_and(|v| v.as_str().is_none_or(str::is_empty))
    {
        return Err(invalid());
    }
    let mut result = Part {
        text: String::new(),
        call: None,
    };
    if let Some(call) = part.get("functionCall") {
        if part.get("text").is_some() || part.get("thought") == Some(&json!(true)) {
            return Err(invalid());
        }
        let call = call.as_object().ok_or_else(invalid)?;
        if call.keys().any(|k| {
            !matches!(k.as_str(), "name" | "args" | "id")
                && !(vertex && matches!(k.as_str(), "willContinue" | "partialArgs"))
        }) || call.get("willContinue").is_some_and(|v| v != &json!(false))
            || call
                .get("partialArgs")
                .is_some_and(|v| v.as_array().is_none_or(|v| !v.is_empty()))
        {
            return Err(error(
                ErrorCode::CapabilityUnsupported,
                "partial_function_call",
            ));
        }
        let name = call
            .get("name")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(invalid)?;
        let id = call
            .get("id")
            .map(|v| {
                v.as_str()
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .ok_or_else(invalid)
            })
            .transpose()?;
        let args = call.get("args").cloned().unwrap_or_else(|| json!({}));
        if !args.is_object() {
            return Err(invalid());
        }
        result.call = Some(Call {
            id,
            name: name.into(),
            args,
        });
    } else if let Some(text) = part.get("text") {
        let text = text.as_str().ok_or_else(invalid)?;
        if part.get("thought") != Some(&json!(true)) {
            result.text = text.into();
        }
    } else if !object.contains_key("thoughtSignature") {
        return Err(invalid());
    }
    Ok(result)
}
```

## `crates/wickle-model-gemini/tests/generate.rs`

```rust
//! Gemini REST/SSE contracts, signed replay and API-version-specific schemas.
mod support;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::time::Duration;
use support::*;
use wickle::*;
use wickle_model_gemini::*;

fn tool(request: &mut ModelRequest) {
    request.tools = vec![ModelTool {
        name: id("lookup"),
        description: "Look up an authorized record".into(),
        model_input_schema: json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}),
    }];
}
fn tool_events() -> Vec<Value> {
    vec![
        json!({"responseId":"tools","modelVersion":"fixture-release","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"name":"lookup","args":{"query":"alpha"}},"thoughtSignature":"signed-call"},{"functionCall":{"name":"lookup","args":{"query":"beta"},"id":"provider-call"}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":20,"candidatesTokenCount":5,"thoughtsTokenCount":7,"totalTokenCount":32}}),
    ]
}

#[tokio::test]
async fn explicit_versions_and_resource_prefix_preserve_wire_and_usage() {
    for version in ["v1", "v1beta"] {
        let server = Server::new(vec![reply(&events("결과 42"))]).await;
        let connection = GeminiConnection::new(
            scope(),
            reference("account"),
            "fixture-key",
            GeminiOptions {
                base_url: server.base.trim_end_matches("v1/").into(),
                api_version: version.into(),
                ..Default::default()
            },
        )
        .unwrap();
        let mut request = request(&connection, &format!("models/{MODEL}"));
        request.messages.insert(
            0,
            ModelMessage {
                role: ModelRole::System,
                content: vec![ModelContent::Text {
                    text: "Use authorized records".into(),
                }],
            },
        );
        let model = GeminiModel::new(connection);
        let response =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap();
        assert_eq!(response.text, "결과 42");
        assert_eq!(
            response.metadata.provider_request_id,
            Some(id("response-fixture"))
        );
        assert_eq!(
            response.metadata.reported_model_version,
            Some(id("fixture-release"))
        );
        assert!(response.metadata.reported_model_id.is_none());
        assert_eq!(response.metadata.usage.unwrap().output_tokens, Some(12));
        assert_eq!(
            response.continuation[0].data()["parts"][0]["thoughtSignature"],
            "signed-thought"
        );
        let calls = server.requests.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].method, "POST");
        assert_eq!(
            calls[0].path,
            format!("/{version}/models/{MODEL}:streamGenerateContent?alt=sse")
        );
        assert!(
            calls[0]
                .headers
                .to_ascii_lowercase()
                .contains("x-goog-api-key: fixture-key")
        );
        assert!(!calls[0].path.contains("fixture-key"));
        let body = &calls[0].body;
        assert_eq!(
            body["generationConfig"]["thinkingConfig"]["thinkingLevel"],
            "MEDIUM"
        );
        assert_eq!(
            body["systemInstruction"]["parts"][0]["text"],
            "Use authorized records"
        );
        assert_eq!(body["generationConfig"]["candidateCount"], 1);
        assert!(!body.to_string().contains("hidden-workspace"));
    }
}
#[tokio::test]
async fn parallel_calls_replay_signatures_without_inventing_wire_ids() {
    let server = Server::new(vec![reply(&tool_events()), reply(&events("42"))]).await;
    let connection = connection(&server);
    let mut request = request(&connection, MODEL);
    tool(&mut request);
    let model = GeminiModel::new(connection);
    let first = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(first.finish, ModelFinish::ToolCalls);
    assert_eq!(first.tool_calls.len(), 2);
    assert_ne!(
        first.tool_calls[0].provider_call_id,
        first.tool_calls[1].provider_call_id
    );
    let mut content: Vec<_> = first
        .tool_calls
        .iter()
        .map(|call| ModelContent::ToolCall {
            provider_call_id: call.provider_call_id.clone(),
            name: call.name.clone(),
            arguments: call.model_inputs.clone(),
        })
        .collect();
    content.push(ModelContent::Opaque {
        continuation: first.continuation[0].clone(),
    });
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content,
    });
    request.messages.push(ModelMessage {
        role: ModelRole::Tool,
        content: first
            .tool_calls
            .iter()
            .map(|call| ModelContent::ToolResult {
                provider_call_id: call.provider_call_id.clone(),
                content: json!({"answer":42}),
            })
            .collect(),
    });
    request.request_id = id("next-attempt");
    request.output = ModelOutput::JsonSchema {
        schema: json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}),
    };
    let response = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(response.text, "42");
    {
        let calls = server.requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        let body = &calls[1].body;
        assert_eq!(
            body["contents"][1]["parts"],
            tool_events()[0]["candidates"][0]["content"]["parts"]
        );
        assert!(
            body["contents"][2]["parts"][0]["functionResponse"]
                .get("id")
                .is_none()
        );
        assert_eq!(
            body["contents"][2]["parts"][1]["functionResponse"]["id"],
            "provider-call"
        );
        assert_eq!(
            body["contents"][2]["parts"][0]["functionResponse"]["name"],
            "lookup"
        );
        assert_eq!(
            body["tools"][0]["functionDeclarations"][0]["parametersJsonSchema"]["additionalProperties"],
            false
        );
        assert_eq!(
            body["generationConfig"]["responseMimeType"],
            "application/json"
        );
    }
    if let ModelContent::ToolCall { arguments, .. } = &mut request.messages[1].content[0] {
        arguments.insert("query".into(), json!("changed"));
    }
    assert!(
        collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .is_err()
    );
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn stable_schema_cannot_silently_drop_closed_object_constraints() {
    let server = Server::new(vec![]).await;
    let connection = GeminiConnection::new(
        scope(),
        reference("account"),
        "fixture",
        GeminiOptions {
            base_url: server.base.trim_end_matches("v1/").into(),
            ..Default::default()
        },
    )
    .unwrap();
    let mut request = request(&connection, MODEL);
    tool(&mut request);
    let model = GeminiModel::new(connection);
    let failure = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap_err();
    assert_eq!(failure.kind, ModelFailureKind::Unsupported);
    assert!(server.requests.lock().unwrap().is_empty());
    request.tools[0]
        .model_input_schema
        .as_object_mut()
        .unwrap()
        .remove("additionalProperties");
    request.tools[0].model_input_schema["properties"]["query"]["enum"] = json!(["alpha", "beta"]);
    let encoded =
        protocol::encode_request(&request, protocol::FunctionSchemaFormat::OpenApi).unwrap();
    assert_eq!(
        encoded["tools"][0]["functionDeclarations"][0]["parameters"]["type"],
        "OBJECT"
    );
    assert_eq!(
        encoded["tools"][0]["functionDeclarations"][0]["parameters"]["properties"]["query"]["type"],
        "STRING"
    );
    request.tools[0].model_input_schema =
        json!({"type":"object","properties":{"choice":{"type":"integer","enum":[1,2]}}});
    let failure = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap_err();
    assert_eq!(failure.kind, ModelFailureKind::Unsupported);
    assert!(server.requests.lock().unwrap().is_empty());
}
#[tokio::test]
async fn unsupported_options_scope_and_route_fail_before_network() {
    for case in [
        "minimal", "combined", "option", "scope", "api", "model", "schema",
    ] {
        let server = Server::new(vec![]).await;
        let connection = connection(&server);
        let mut request = request(&connection, MODEL);
        let model = GeminiModel::new(connection);
        let mut context = context(&request);
        match case {
            "minimal" => {
                request
                    .options
                    .insert("thinking_level".into(), json!("minimal"));
            }
            "combined" => {
                request
                    .options
                    .insert("thinking_budget_tokens".into(), json!(100));
            }
            "option" => {
                request.options.insert("native_search".into(), json!(true));
            }
            "scope" => context.scope.workspace_id = id("other"),
            "api" => request.route.api_contract.version = id("v2"),
            "model" => request.route.model_id = id("models/models/invalid"),
            _ => {
                request.output = ModelOutput::JsonSchema {
                    schema: json!({"type":"object","properties":{},"additionalProperties":false,"not":{}}),
                }
            }
        }
        assert!(
            collect_model_response(&request, model.generate(&request, &context))
                .await
                .is_err(),
            "{case}"
        );
        assert!(server.requests.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn malformed_incomplete_or_ambiguous_streams_never_complete() {
    for case in [
        "eof",
        "sse",
        "duplicate",
        "candidates",
        "signature",
        "native",
        "version",
        "usage",
        "limit",
    ] {
        let mut data = events("answer");
        match case {
            "eof" => {
                data[1]["candidates"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("finishReason");
            }
            "duplicate" => {
                data = tool_events();
                data[0]["candidates"][0]["content"]["parts"][0]["functionCall"]["id"] =
                    json!("provider-call");
            }
            "candidates" => {
                data[0]["candidates"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"index":1}));
            }
            "signature" => {
                data[0]["candidates"][0]["content"]["parts"][0]["thoughtSignature"] = json!(42)
            }
            "native" => {
                data[0]["candidates"][0]["content"]["parts"] =
                    json!([{"executableCode":{"code":"private"}}])
            }
            "version" => data[1]["modelVersion"] = json!("different"),
            "usage" => data[1]["usageMetadata"]["totalTokenCount"] = json!(3),
            _ => {}
        }
        let mut response = reply(&data);
        if case == "sse" {
            response.body.pop();
            response.body.pop();
        }
        let server = Server::new(vec![response]).await;
        let connection = connection(&server);
        let mut request = request(&connection, MODEL);
        tool(&mut request);
        let model = GeminiModel::new(connection);
        if case == "limit" {
            request.limits.max_response_bytes = 8;
        }
        assert!(
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .is_err(),
            "{case}"
        );
    }
}
#[tokio::test]
async fn refusal_and_length_are_not_successful_continuations() {
    for reason in ["SAFETY", "MAX_TOKENS", "prompt"] {
        let mut data = events("partial");
        data[1]["candidates"][0]["finishReason"] = json!(reason);
        if reason == "prompt" {
            data = vec![json!({"promptFeedback":{"blockReason":"SAFETY"}})];
        }
        let server = Server::new(vec![reply(&data)]).await;
        let connection = connection(&server);
        let request = request(&connection, MODEL);
        let model = GeminiModel::new(connection);
        let events: Vec<_> = model.generate(&request, &context(&request)).collect().await;
        let ModelEvent::ResponseCompleted {
            finish,
            continuation,
            ..
        } = events.last().unwrap().as_ref().unwrap()
        else {
            panic!("missing terminal")
        };
        assert_eq!(
            *finish,
            if reason == "MAX_TOKENS" {
                ModelFinish::Length
            } else {
                ModelFinish::Refusal
            }
        );
        assert!(continuation.is_empty());
    }
}
#[tokio::test]
async fn cancellation_deadline_and_redirect_do_not_leave_requests_running() {
    for cancel in [true, false] {
        let mut data = events("partial");
        data[1]["candidates"][0]
            .as_object_mut()
            .unwrap()
            .remove("finishReason");
        let mut response = reply(&data);
        response.stall = true;
        let server = Server::new(vec![response]).await;
        let connection = connection(&server);
        let request = request(&connection, MODEL);
        let model = GeminiModel::new(connection);
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
    let destination = Server::new(vec![]).await;
    let mut response = Reply::json(307, json!({}));
    response
        .headers
        .push(("location", destination.base.clone()));
    let server = Server::new(vec![response]).await;
    let connection = connection(&server);
    let request = request(&connection, MODEL);
    let model = GeminiModel::new(connection);
    assert!(
        collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .is_err()
    );
    assert!(destination.requests.lock().unwrap().is_empty());
}
fn metadata() -> Value {
    json!({"name":format!("models/{MODEL}"),"version":"001","supportedGenerationMethods":["generateContent"]})
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
fn snapshot() -> GeminiSnapshot {
    GeminiSnapshot {
        model_id: id(MODEL),
        model_version: id("fixture-release"),
        metadata_version: id("001"),
        evidence_ref: id("documented-snapshot"),
    }
}
#[tokio::test]
async fn metadata_observations_require_actual_release_evidence_and_detect_drift() {
    let mut changed = metadata();
    changed["version"] = json!("002");
    let server = Server::new(vec![
        Reply::json(200, metadata()),
        Reply::json(200, metadata()),
        Reply::json(200, changed),
        Reply::json(404, json!({})),
    ])
    .await;
    let connection = connection(&server);
    let request = request(&connection, MODEL);
    let unknown = GeminiInspector::new(connection.clone(), vec![])
        .unwrap()
        .inspect(&request.route, &inspect_context())
        .await
        .unwrap();
    assert_eq!(unknown.version_semantics, VersionSemantics::Unverified);
    assert!(unknown.model_version.is_none());
    let inspector = GeminiInspector::new(connection, vec![snapshot()]).unwrap();
    let known = inspector
        .inspect(&request.route, &inspect_context())
        .await
        .unwrap();
    known
        .validate(&request.route, VersionPolicy::RequirePinned)
        .unwrap();
    assert_eq!(
        inspector
            .inspect(&request.route, &inspect_context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ModelVersionDrift
    );
    assert_eq!(
        inspector
            .inspect(&request.route, &inspect_context())
            .await
            .unwrap()
            .availability,
        ModelRouteAvailability::Unavailable
    );
    assert_eq!(
        server.requests.lock().unwrap()[0].path,
        format!("/v1beta/models/{MODEL}")
    );
}

#[tokio::test]
async fn http_failures_are_classified_without_retry_or_private_error_text() {
    for (status, kind) in [
        (401, ModelFailureKind::Authentication),
        (404, ModelFailureKind::Unavailable),
        (429, ModelFailureKind::RateLimited),
        (503, ModelFailureKind::Transport),
    ] {
        let server = Server::new(vec![Reply::json(
            status,
            json!({"error":{"message":"private provider details"}}),
        )])
        .await;
        let connection = connection(&server);
        let request = request(&connection, MODEL);
        let model = GeminiModel::new(connection);
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, kind);
        assert!(!format!("{failure:?}").contains("private provider details"));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}
#[tokio::test]
async fn omitted_usage_is_unknown_and_late_usage_is_cumulative() {
    for case in ["none", "late", "partial"] {
        let late = case == "late";
        let mut data = events("answer");
        data[1].as_object_mut().unwrap().remove("usageMetadata");
        data[0].as_object_mut().unwrap().remove("modelVersion");
        if late {
            data.push(json!({"usageMetadata":{"promptTokenCount":20,"candidatesTokenCount":5,"thoughtsTokenCount":7,"totalTokenCount":32}}));
        }
        if case == "partial" {
            data.push(json!({"usageMetadata":{"candidatesTokenCount":5}}));
        }
        let server = Server::new(vec![reply(&data)]).await;
        let connection = connection(&server);
        let request = request(&connection, MODEL);
        let model = GeminiModel::new(connection);
        let response =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap();
        assert!(response.metadata.reported_model_version.is_none());
        if late {
            assert_eq!(response.metadata.usage.unwrap().output_tokens, Some(12));
        } else if case == "partial" {
            assert!(response.metadata.usage.unwrap().output_tokens.is_none());
        } else {
            assert!(response.metadata.usage.is_none());
        }
    }
}

#[tokio::test]
async fn idless_parallel_results_are_matched_in_original_call_order() {
    let mut data = tool_events();
    data[0]["candidates"][0]["content"]["parts"][1]["functionCall"]
        .as_object_mut()
        .unwrap()
        .remove("id");
    let server = Server::new(vec![reply(&data), reply(&events("done"))]).await;
    let connection = connection(&server);
    let mut request = request(&connection, MODEL);
    tool(&mut request);
    let model = GeminiModel::new(connection);
    let first = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let mut content: Vec<_> = first
        .tool_calls
        .iter()
        .map(|c| ModelContent::ToolCall {
            provider_call_id: c.provider_call_id.clone(),
            name: c.name.clone(),
            arguments: c.model_inputs.clone(),
        })
        .collect();
    content.push(ModelContent::Opaque {
        continuation: first.continuation[0].clone(),
    });
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content,
    });
    for (index, answer) in [(1, "beta result"), (0, "alpha result")] {
        request.messages.push(ModelMessage {
            role: ModelRole::Tool,
            content: vec![ModelContent::ToolResult {
                provider_call_id: first.tool_calls[index].provider_call_id.clone(),
                content: json!({"answer":answer}),
            }],
        });
    }
    request.request_id = id("next-attempt");
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let calls = server.requests.lock().unwrap();
    let parts = calls[1].body["contents"][2]["parts"].as_array().unwrap();
    assert_eq!(
        parts[0]["functionResponse"]["response"]["answer"],
        "alpha result"
    );
    assert_eq!(
        parts[1]["functionResponse"]["response"]["answer"],
        "beta result"
    );
    assert!(
        parts
            .iter()
            .all(|p| p["functionResponse"].get("id").is_none())
    );
}

#[tokio::test]
async fn minimal_thinking_is_rejected_for_both_flash_releases_before_network() {
    for release in ["gemini-3.7-flash", "gemini-3.8-flash"] {
        let server = Server::new(vec![reply(&events("would otherwise succeed"))]).await;
        let connection = connection(&server);
        let mut request = request(&connection, release);
        request
            .options
            .insert("thinking_level".into(), json!("minimal"));
        let model = GeminiModel::new(connection);
        let error = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .expect_err("unsupported option must fail before dispatch");
        assert_eq!(error.kind, ModelFailureKind::Unsupported);
        assert!(server.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn two_documented_release_ids_keep_path_and_reported_revision_separate() {
    let releases = ["gemini-3.8-flash", "gemini-3.7-flash"];
    let replies = releases
        .iter()
        .enumerate()
        .map(|(index, release)| {
            let mut data = events(release);
            data[0]["modelVersion"] = json!(format!("fixture-revision-{index}"));
            reply(&data)
        })
        .collect();
    let server = Server::new(replies).await;
    let connection = connection(&server);
    let model = GeminiModel::new(connection.clone());
    for (index, release) in releases.iter().enumerate() {
        let mut request = request(&connection, release);
        request.route.binding = reference(&format!("binding-{index}"));
        request.route.model_version = id(&format!("fixture-revision-{index}"));
        let result = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(result.text, *release);
        assert_eq!(
            result.metadata.reported_model_version,
            Some(id(&format!("fixture-revision-{index}")))
        );
        assert!(result.metadata.reported_model_id.is_none());
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (request, release) in requests.iter().zip(releases) {
        assert!(
            request
                .path
                .contains(&format!("/models/{release}:streamGenerateContent"))
        );
    }
}
```

## `crates/wickle-model-openai/tests/responses.rs`

```rust
//! Real HTTP/SSE boundaries, scoped credentials, version evidence, and lossless replay.
#[allow(dead_code)]
mod support;
use futures_util::StreamExt;
use serde_json::{Value, json};
use support::*;
use wickle::*;
use wickle_model_openai::*;

#[tokio::test]
async fn streaming_versions_options_and_reported_usage_do_not_leak_host_context() {
    let server = Server::new(vec![
        Reply::sse(&events("model-first", "안녕")),
        Reply::sse(&events("model-second", "second")),
    ])
    .await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    for (name, effort, text) in [
        ("model-first", "low", "안녕"),
        ("model-second", "high", "second"),
    ] {
        let mut request = request(&connection, name);
        request.route.model_version = id(name);
        request
            .options
            .insert("reasoning_effort".into(), json!(effort));
        let context = context(&request);
        let result = collect_model_response(&request, model.generate(&request, &context))
            .await
            .unwrap();
        assert_eq!(result.text, text);
        assert_eq!(result.finish, ModelFinish::Stop);
        assert_eq!(result.metadata.reported_model_id, Some(id(name)));
        assert!(result.metadata.reported_model_version.is_none());
        assert_eq!(
            result.metadata.usage,
            Some(ModelUsage {
                measurement: UsageMeasurement::Reported,
                input_tokens: Some(11),
                output_tokens: Some(7)
            })
        );
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (index, effort) in ["low", "high"].into_iter().enumerate() {
        assert_eq!(requests[index].path, "/v1/responses");
        assert_eq!(requests[index].method, "POST");
        assert_eq!(requests[index].body["reasoning"]["effort"], effort);
        assert_eq!(requests[index].body["stream"], true);
        assert_eq!(requests[index].body["store"], false);
        assert_eq!(requests[index].body["truncation"], "disabled");
        let encoded = requests[index].body.to_string();
        assert!(!encoded.contains("hidden-workspace"));
        assert!(!encoded.contains("fixture-key-not-a-secret"));
        assert!(
            requests[index]
                .headers
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-key-not-a-secret")
        );
    }
    assert!(!format!("{connection:?} {model:?}").contains("fixture-key-not-a-secret"));
}

#[tokio::test]
async fn structured_output_preserves_schema_and_rejects_unsupported_contracts_before_http() {
    let schema = json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false});
    let server = Server::new(vec![Reply::sse(&events("model", r#"{"answer":42}"#))]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    request.output = ModelOutput::JsonSchema {
        schema: schema.clone(),
    };
    request.options.insert("verbosity".into(), json!("low"));
    let result = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(parse_json(&result.text).unwrap(), json!({"answer":42}));
    let body = server.requests.lock().unwrap()[0].body.clone();
    assert_eq!(body["text"]["format"]["schema"], schema);
    assert_eq!(body["text"]["format"]["strict"], true);
    assert_eq!(body["text"]["verbosity"], "low");
    request.output = ModelOutput::JsonSchema {
        schema: json!({"type":"object","properties":{"optional":{"type":"string"}},"additionalProperties":false}),
    };
    let failure = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap_err();
    assert_eq!(failure.kind, ModelFailureKind::Unsupported);
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

fn function_events() -> Vec<Value> {
    let reasoning = json!({"id":"rs_1","type":"reasoning","summary":[],"encrypted_content":"ciphertext-fixture"});
    let call = json!({"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"query\":\"figures\"}","status":"completed"});
    vec![
        json!({"type":"response.created","response":{"id":"resp_1","model":"model","status":"in_progress"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"rs_1","type":"reasoning","summary":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":reasoning}),
        json!({"type":"response.output_item.added","output_index":1,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":"fc_1","delta":"{\"query\":"}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":"fc_1","delta":"\"figures\"}"}),
        json!({"type":"response.function_call_arguments.done","output_index":1,"item_id":"fc_1","arguments":"{\"query\":\"figures\"}"}),
        json!({"type":"response.output_item.done","output_index":1,"item":call}),
        json!({"type":"response.completed","response":{"id":"resp_1","model":"model","status":"completed","output":[reasoning,call]}}),
    ]
}
fn with_tool(request: &mut ModelRequest) {
    request.tools = vec![ModelTool {
        name: id("lookup"),
        description: "Read figures".into(),
        model_input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer"}},"required":["query"],"additionalProperties":false}),
    }];
}
#[tokio::test]
async fn function_fragments_and_full_reasoning_replay_preserve_original_order_once() {
    let server = Server::new(vec![
        Reply::sse(&function_events()),
        Reply::sse(&events("model", "received result")),
    ])
    .await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    with_tool(&mut request);
    let first = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(first.finish, ModelFinish::ToolCalls);
    assert_eq!(first.tool_calls.len(), 1);
    assert!(first.metadata.usage.is_none());
    assert_eq!(first.continuation.len(), 1);
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![
            ModelContent::ToolCall {
                provider_call_id: id("call_1"),
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
            provider_call_id: id("call_1"),
            content: json!({"result":73}),
        }],
    });
    request.request_id = id("second-attempt");
    let result = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(result.text, "received result");
    {
        let calls = server.requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        let input = calls[1].body["input"].as_array().unwrap();
        assert_eq!(
            input
                .iter()
                .filter(|item| item["type"] == "function_call")
                .count(),
            1
        );
        assert_eq!(input[1]["type"], "reasoning");
        assert_eq!(input[1]["encrypted_content"], "ciphertext-fixture");
        assert_eq!(input[2]["call_id"], "call_1");
        assert_eq!(input[3]["type"], "function_call_output");
        assert_eq!(
            parse_json(input[3]["output"].as_str().unwrap()).unwrap(),
            json!({"result":73})
        );
        assert_eq!(calls[0].body["tools"][0]["strict"], false);
        assert_eq!(
            calls[0].body["tools"][0]["parameters"]["required"],
            json!(["query"])
        );
    }
    if let ModelContent::ToolCall { arguments, .. } = &mut request.messages[1].content[0] {
        arguments.insert("query".into(), json!("changed"));
    }
    assert!(
        collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .is_err()
    );
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn truncated_conflicting_and_length_limited_streams_never_return_executable_calls() {
    for mode in ["truncated", "conflict", "length", "duplicate-terminal"] {
        let mut data = function_events();
        match mode {
            "truncated" => {
                data.truncate(5);
            }
            "conflict" => {
                data[5]["item_id"] = json!("another-item");
            }
            "length" => {
                data.truncate(5);
                data.push(json!({"type":"response.incomplete","response":{"id":"resp_1","model":"model","status":"incomplete","output":[],"incomplete_details":{"reason":"max_output_tokens"}}}));
            }
            _ => {
                data.push(data.last().unwrap().clone());
            }
        }
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let mut request = request(&connection, "model");
        with_tool(&mut request);
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        assert!(result.is_err(), "{mode} must not yield a complete call");
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn unknown_options_wrong_scope_and_wrong_target_never_reach_the_server() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    for field in [
        "store",
        "input",
        "tools",
        "background",
        "previous_response_id",
    ] {
        let mut request = request(&connection, "model");
        request.options.insert(field.into(), json!(true));
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, ModelFailureKind::Unsupported);
    }
    let mut request = request(&connection, "model");
    let mut caller = context(&request);
    caller.scope.workspace_id = id("other");
    assert_eq!(
        model
            .generate(&request, &caller)
            .next()
            .await
            .unwrap()
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    caller = context(&request);
    request
        .route
        .target
        .insert("project_id".into(), json!("different"));
    assert!(
        collect_model_response(&request, model.generate(&request, &caller))
            .await
            .is_err()
    );
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn provider_errors_and_redirects_are_not_retried_or_exposed_as_text() {
    for (status, expected) in [
        (401, ModelFailureKind::Authentication),
        (404, ModelFailureKind::Unavailable),
        (429, ModelFailureKind::RateLimited),
        (503, ModelFailureKind::Transport),
        (302, ModelFailureKind::Unsupported),
    ] {
        let redirect = Server::new(vec![Reply::sse(&events("model", "unexpected redirect"))]).await;
        let mut reply = Reply::json(
            status,
            json!({"error":{"message":"private diagnostic","code":"fixture"}}),
        );
        if status == 302 {
            reply
                .headers
                .push(("location", format!("{}responses", redirect.base)));
        }
        let server = Server::new(vec![
            reply,
            Reply::sse(&events("model", "unexpected retry")),
        ])
        .await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let request = request(&connection, "model");
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, expected);
        assert!(failure.partial_text().is_empty());
        assert_eq!(server.requests.lock().unwrap().len(), 1);
        assert!(redirect.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn cancellation_and_deadline_drop_an_unfinished_stream() {
    for cancel in [true, false] {
        let mut reply = Reply::sse(&events("model", "partial")[..3]);
        reply.stall = true;
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let request = request(&connection, "model");
        let mut caller = context(&request);
        if !cancel {
            caller.deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(100);
        }
        let mut stream = model.generate(&request, &caller);
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ModelEvent::TextDelta { .. }
        ));
        if cancel {
            caller.cancellation.cancel();
        }
        let next = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap();
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
        tokio::time::timeout(std::time::Duration::from_secs(1), server.closed.notified())
            .await
            .unwrap();
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn model_inspection_uses_registered_snapshot_facts_not_requested_versions_or_name_patterns() {
    let server = Server::new(vec![
        Reply::json(200, json!({"id":"model-2026-01-01","object":"model"})),
        Reply::json(200, json!({"id":"model-2026-01-01","object":"model"})),
    ])
    .await;
    let connection = connection(&server);
    let request = request(&connection, "model-2026-01-01");
    let context = ModelInspectionContext {
        scope: scope(),
        principal_ref: id("user"),
        capability_grant_ref: id("grant"),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(3),
    };
    let unknown = OpenAiInspector::new(connection.clone(), vec![])
        .unwrap()
        .inspect(&request.route, &context)
        .await
        .unwrap();
    assert!(unknown.model_version.is_none());
    assert_eq!(unknown.version_semantics, VersionSemantics::Unverified);
    assert!(
        unknown
            .validate(&request.route, VersionPolicy::RequirePinned)
            .is_err()
    );
    let known = OpenAiInspector::new(
        connection,
        vec![OpenAiSnapshot {
            model_id: id("model-2026-01-01"),
            model_version: id("actual-release"),
            evidence_ref: id("documented-snapshot"),
        }],
    )
    .unwrap()
    .inspect(&request.route, &context)
    .await
    .unwrap();
    assert_eq!(known.model_version, Some(id("actual-release")));
    assert_eq!(known.version_semantics, VersionSemantics::Pinned);
    assert_eq!(
        known
            .validate(&request.route, VersionPolicy::RequirePinned)
            .unwrap_err()
            .code,
        ErrorCode::ModelVersionDrift
    );
    let calls = server.requests.lock().unwrap();
    assert!(
        calls
            .iter()
            .all(|call| call.method == "GET" && call.path == "/v1/models/model-2026-01-01")
    );
}

#[tokio::test]
async fn refusal_cannot_be_relabelled_as_success_by_a_conflicting_terminal() {
    for valid in [false, true] {
        let mut data = events("model", "Unable to help");
        data[2]["type"] = json!("response.refusal.delta");
        data[3]["type"] = json!("response.refusal.done");
        data[3].as_object_mut().unwrap().remove("text");
        data[3]["refusal"] = json!("Unable to help");
        if valid {
            let part = json!({"type":"refusal","refusal":"Unable to help"});
            data[4]["item"]["content"] = json!([part]);
            data[5]["response"]["output"][0]["content"] = json!([part]);
        }
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let request = request(&connection, "model");
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        if valid {
            assert_eq!(result.unwrap().finish, ModelFinish::Refusal);
        } else {
            assert!(result.is_err());
        }
    }
}

#[tokio::test]
async fn interleaved_function_deltas_keep_independent_identity_and_arguments() {
    let mut data = vec![
        json!({"type":"response.created","response":{"id":"resp_1","model":"model","status":"in_progress"}}),
    ];
    let calls:Vec<_>=["one","two"].into_iter().enumerate().map(|(index,query)|json!({"id":format!("fc_{index}"),"type":"function_call","call_id":format!("call_{index}"),"name":"lookup","arguments":format!("{{\"query\":\"{query}\"}}"),"status":"completed"})).collect();
    for (index, call) in calls.iter().enumerate() {
        let mut item = call.clone();
        item["arguments"] = json!("");
        item.as_object_mut().unwrap().remove("status");
        data.push(json!({"type":"response.output_item.added","output_index":index,"item":item}));
    }
    for (index, delta) in [
        (0, "{\"query\":"),
        (1, "{\"query\":"),
        (1, "\"two\"}"),
        (0, "\"one\"}"),
    ] {
        data.push(json!({"type":"response.function_call_arguments.delta","output_index":index,"item_id":format!("fc_{index}"),"delta":delta}));
    }
    for (index, call) in calls.iter().enumerate() {
        data.push(json!({"type":"response.function_call_arguments.done","output_index":index,"item_id":format!("fc_{index}"),"arguments":call["arguments"]}));
        data.push(json!({"type":"response.output_item.done","output_index":index,"item":call}));
    }
    data.push(json!({"type":"response.completed","response":{"id":"resp_1","model":"model","status":"completed","output":calls}}));
    let server = Server::new(vec![Reply::sse(&data)]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    with_tool(&mut request);
    let result = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(result.tool_calls.len(), 2);
    assert_eq!(result.tool_calls[0].model_inputs["query"], "one");
    assert_eq!(result.tool_calls[1].model_inputs["query"], "two");
    assert_ne!(
        result.tool_calls[0].provider_call_id,
        result.tool_calls[1].provider_call_id
    );
}

#[tokio::test]
async fn stream_and_payload_limits_reject_oversized_or_reassigned_data_without_a_success() {
    for case in ["raw", "payload", "events", "identity", "sequence", "usage"] {
        let mut data = events("model", "candidate");
        if case == "identity" {
            data[5]["response"]["id"] = json!("other-response");
        }
        if case == "usage" {
            data[5]["response"]["usage"]["output_tokens"] = json!("7");
        }
        let mut reply = Reply::sse(&data);
        if case == "sequence" {
            let text = String::from_utf8(reply.body).unwrap();
            reply.body = text
                .replacen("\"sequence_number\":2", "\"sequence_number\":1", 1)
                .into_bytes();
        }
        let server = Server::new(vec![reply]).await;
        let connection = if case == "raw" {
            OpenAiConnection::new(
                scope(),
                reference("account"),
                "fixture-key-not-a-secret",
                OpenAiOptions {
                    base_url: server.base.clone(),
                    max_transport_bytes: 64,
                    max_event_bytes: 64,
                    ..Default::default()
                },
            )
            .unwrap()
        } else {
            connection(&server)
        };
        let model = OpenAiModel::new(connection.clone());
        let mut request = request(&connection, "model");
        if case == "payload" {
            request.limits.max_response_bytes = 4;
        }
        if case == "events" {
            request.limits.max_events = 1;
        }
        assert!(
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .is_err(),
            "{case}"
        );
    }
}

#[tokio::test]
async fn protocol_metadata_does_not_consume_the_normalized_model_event_allowance() {
    let server = Server::new(vec![Reply::sse(&events("model", "one delta"))]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    request.limits.max_events = 2;
    let result = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(result.text, "one delta");
}

#[tokio::test]
async fn final_content_cannot_move_between_messages_or_parts() {
    for mode in ["valid", "message", "part", "kind"] {
        let mut data = events("model", "A");
        data.truncate(4);
        let mut first = text_item("A");
        first["phase"] = json!("commentary");
        let mut second = text_item("B");
        second["id"] = json!("msg_2");
        second["phase"] = json!("final_answer");
        data.push(json!({"type":"response.output_item.added","output_index":1,"item":{"id":"msg_2","type":"message","role":"assistant","content":[]}}));
        data.push(json!({"type":"response.output_text.delta","output_index":1,"item_id":"msg_2","content_index":0,"delta":"B"}));
        match mode {
            "message" => {
                first["content"][0]["text"] = json!("AB");
                second["content"] = json!([]);
            }
            "part" => {
                first["content"] =
                    json!([{"type":"output_text","text":""},{"type":"output_text","text":"A"}]);
            }
            "kind" => {
                first["content"] = json!([{"type":"refusal","refusal":"A"}]);
            }
            _ => {}
        }
        data.push(json!({"type":"response.completed","response":{"id":"resp_1","model":"model","status":"completed","output":[first,second]}}));
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let request = request(&connection, "model");
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        if mode == "valid" {
            assert_eq!(result.unwrap().text, "AB");
        } else {
            assert!(result.is_err(), "accepted {mode} reassignment");
        }
    }
}

#[tokio::test]
async fn two_documented_release_ids_coexist_without_replacing_connection_state() {
    let releases = ["gpt-6-astra", "gpt-5.6-sol"];
    let server = Server::new(
        releases
            .iter()
            .map(|release| Reply::sse(&events(release, release)))
            .collect(),
    )
    .await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    for (index, release) in releases.iter().enumerate() {
        let mut request = request(&connection, release);
        request.route.binding = reference(&format!("binding-{index}"));
        request.route.model_version = id(&format!("fixture-revision-{index}"));
        let result = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(result.text, *release);
        assert_eq!(result.metadata.reported_model_id, Some(id(release)));
        assert!(result.metadata.reported_model_version.is_none());
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (request, release) in requests.iter().zip(releases) {
        assert_eq!(request.body["model"], release);
    }
}
```

## `crates/wickle-model-vertex/tests/vertex.rs`

```rust
//! Vertex resource, OAuth, complete function-call and publisher metadata contracts.
mod support;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, UNIX_EPOCH},
};
use support::*;
use wickle::*;
use wickle_model_vertex::*;
fn tool(request: &mut ModelRequest) {
    request.tools = vec![ModelTool {
        name: id("lookup"),
        description: "Read an authorized record".into(),
        model_input_schema: json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}),
    }];
}
fn tool_events() -> Vec<Value> {
    vec![
        json!({"modelVersion":"fixture-release","responseId":"vertex-response","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"name":"lookup","args":{"query":"alpha"},"willContinue":false,"partialArgs":[]},"thoughtSignature":"signed-vertex"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":20,"candidatesTokenCount":5,"thoughtsTokenCount":7,"totalTokenCount":32}}),
    ]
}
#[test]
fn endpoint_families_follow_their_documented_hostnames() {
    for (location, expected) in [
        ("global", "https://aiplatform.googleapis.com/"),
        (
            "us-central1",
            "https://us-central1-aiplatform.googleapis.com/",
        ),
        ("us", "https://aiplatform.us.rep.googleapis.com/"),
        ("eu", "https://aiplatform.eu.rep.googleapis.com/"),
    ] {
        let mut options = VertexOptions::new("test-project");
        options.location = location.into();
        let connection =
            VertexConnection::new(scope(), reference("account"), token(), options).unwrap();
        assert_eq!(connection.target()["endpoint"], expected);
        assert_eq!(connection.target()["location"], location);
    }
}
#[tokio::test]
async fn complete_signed_tools_and_current_output_format_use_the_scoped_resource() {
    let server = Server::new(vec![
        reply(&tool_events()),
        reply(&events("{\"answer\":42}")),
    ])
    .await;
    let connection = connection(&server);
    let mut request = support::request(&connection, MODEL);
    tool(&mut request);
    let model = VertexModel::new(connection);
    let first = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(first.finish, ModelFinish::ToolCalls);
    assert_eq!(first.tool_calls[0].model_inputs["query"], "alpha");
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![
            ModelContent::ToolCall {
                provider_call_id: first.tool_calls[0].provider_call_id.clone(),
                name: id("lookup"),
                arguments: first.tool_calls[0].model_inputs.clone(),
            },
            ModelContent::Opaque {
                continuation: first.continuation[0].clone(),
            },
        ],
    });
    request.messages.push(ModelMessage {
        role: ModelRole::Tool,
        content: vec![ModelContent::ToolResult {
            provider_call_id: first.tool_calls[0].provider_call_id.clone(),
            content: json!({"answer":42}),
        }],
    });
    request.request_id = id("next-attempt");
    request.output = ModelOutput::JsonSchema {
        schema: json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}),
    };
    let response = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(parse_json(&response.text).unwrap(), json!({"answer":42}));
    assert_eq!(response.metadata.usage.unwrap().output_tokens, Some(12));
    let calls = server.requests.lock().unwrap();
    assert_eq!(calls.len(), 2);
    for call in calls.iter() {
        assert_eq!(call.method, "POST");
        assert_eq!(
            call.path,
            format!(
                "/v1/projects/test-project/locations/global/publishers/google/models/{MODEL}:streamGenerateContent?alt=sse"
            )
        );
        assert!(
            call.headers
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-token")
        );
        assert!(
            call.headers
                .to_ascii_lowercase()
                .contains("x-goog-user-project: billing-project")
        );
        assert!(!call.headers.to_ascii_lowercase().contains("x-goog-api-key"));
        assert!(!call.body.to_string().contains("hidden-workspace"));
    }
    let body = &calls[1].body;
    assert_eq!(
        body["contents"][1]["parts"],
        tool_events()[0]["candidates"][0]["content"]["parts"]
    );
    assert!(
        body["contents"][2]["parts"][0]["functionResponse"]
            .get("id")
            .is_none()
    );
    assert_eq!(
        body["tools"][0]["functionDeclarations"][0]["parametersJsonSchema"]["additionalProperties"],
        false
    );
    assert_eq!(
        body["toolConfig"]["functionCallingConfig"]["streamFunctionCallArguments"],
        false
    );
    assert_eq!(
        body["generationConfig"]["responseFormat"][0]["text"]["mimeType"],
        "APPLICATION_JSON"
    );
    assert!(body["generationConfig"].get("responseJsonSchema").is_none());
    assert!(body["generationConfig"].get("responseMimeType").is_none());
}
#[tokio::test]
async fn partial_functions_and_foreign_resources_cannot_dispatch() {
    for partial in [true, false] {
        let mut data = tool_events();
        if partial {
            data[0]["candidates"][0]["content"]["parts"][0]["functionCall"]["willContinue"] =
                json!(true);
        } else {
            data[0]["candidates"][0]["content"]["parts"][0]["functionCall"]["partialArgs"] =
                json!([{"jsonPath":"$.query","stringValue":"a"}]);
        }
        let server = Server::new(vec![reply(&data)]).await;
        let connection = connection(&server);
        let mut request = support::request(&connection, MODEL);
        tool(&mut request);
        let model = VertexModel::new(connection);
        assert!(
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .is_err()
        );
    }
    for key in [
        "project",
        "location",
        "quota_project",
        "endpoint",
        "scope",
        "api",
    ] {
        let server = Server::new(vec![]).await;
        let connection = connection(&server);
        let mut request = support::request(&connection, MODEL);
        let mut context = context(&request);
        let model = VertexModel::new(connection);
        if key == "scope" {
            context.scope.workspace_id = id("foreign");
        } else if key == "api" {
            request.route.api_contract.version = id("v1beta1");
        } else {
            request.route.target.insert(key.into(), json!("foreign"));
        }
        assert!(
            collect_model_response(&request, model.generate(&request, &context))
                .await
                .is_err()
        );
        assert!(server.requests.lock().unwrap().is_empty());
    }
}
struct TestClock(Arc<AtomicI64>);
impl Clock for TestClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: self.0.load(Ordering::SeqCst),
            monotonic_ms: 0,
        })
    }
    fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}
struct Refresh {
    clock: Arc<AtomicI64>,
    expired: bool,
}
impl VertexTokenProvider for Refresh {
    fn token<'a>(&'a self, context: &'a VertexTokenContext<'a>) -> PortFuture<'a, VertexToken> {
        Box::pin(async move {
            assert_eq!(context.project, "test-project");
            assert_eq!(context.scope, &scope());
            self.clock.store(10000, Ordering::SeqCst);
            VertexToken::new(
                "refreshed-token",
                Some(UNIX_EPOCH + Duration::from_secs(if self.expired { 5 } else { 20 })),
            )
        })
    }
}
#[tokio::test]
async fn expiry_is_checked_after_token_refresh() {
    for expired in [true, false] {
        let server = Server::new(vec![reply(&events("42"))]).await;
        let clock = Arc::new(AtomicI64::new(0));
        let connection = VertexConnection::new(
            scope(),
            reference("account"),
            Arc::new(Refresh {
                clock: clock.clone(),
                expired,
            }),
            options(&server),
        )
        .unwrap()
        .with_clock(Arc::new(TestClock(clock)));
        let request = support::request(&connection, MODEL);
        let model = VertexModel::new(connection);
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
                    .contains("authorization: bearer refreshed-token")
            );
        }
    }
}
struct Pending(tokio::sync::Notify);
impl VertexTokenProvider for Pending {
    fn token<'a>(&'a self, _: &'a VertexTokenContext<'a>) -> PortFuture<'a, VertexToken> {
        Box::pin(async move {
            self.0.notify_one();
            std::future::pending().await
        })
    }
}
#[tokio::test]
async fn credential_lookup_and_streaming_obey_cancellation_and_deadlines() {
    for stage in ["token", "stream"] {
        for cancel in [true, false] {
            let mut data = events("partial");
            data[1]["candidates"][0]
                .as_object_mut()
                .unwrap()
                .remove("finishReason");
            let mut response = reply(&data);
            response.stall = true;
            let server = Server::new(vec![response]).await;
            let pending = Arc::new(Pending(tokio::sync::Notify::new()));
            let credentials: Arc<dyn VertexTokenProvider> = if stage == "token" {
                pending.clone()
            } else {
                token()
            };
            let connection =
                VertexConnection::new(scope(), reference("account"), credentials, options(&server))
                    .unwrap();
            let request = support::request(&connection, MODEL);
            let model = VertexModel::new(connection);
            let mut context = context(&request);
            context.deadline = tokio::time::Instant::now() + Duration::from_secs(1);
            let mut stream = model.generate(&request, &context);
            if stage == "stream" {
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
            } else {
                let trigger = async {
                    pending.0.notified().await;
                    if cancel {
                        context.cancellation.cancel();
                    }
                };
                let (next, _) = tokio::join!(stream.next(), trigger);
                let next = next.unwrap();
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
                assert!(server.requests.lock().unwrap().is_empty());
            }
        }
    }
}
fn metadata() -> Value {
    json!({"name":format!("publishers/google/models/{MODEL}"),"versionId":"7","versionState":"VERSION_STATE_STABLE"})
}
fn snapshot() -> VertexSnapshot {
    VertexSnapshot {
        model_id: id(MODEL),
        model_version: id("fixture-release"),
        publisher_version_id: id("7"),
        supported_locations: vec!["global".into(), "us".into(), "eu".into()],
        evidence_ref: id("documented-release"),
    }
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
#[tokio::test]
async fn publisher_artifacts_do_not_invent_release_or_location_support() {
    let mut changed = metadata();
    changed["versionId"] = json!("8");
    let server = Server::new(vec![
        Reply::json(200, metadata()),
        Reply::json(200, metadata()),
        Reply::json(200, changed),
        Reply::json(404, json!({})),
    ])
    .await;
    let connection = connection(&server);
    let request = support::request(&connection, MODEL);
    let unknown = VertexInspector::new(connection.clone(), vec![])
        .unwrap()
        .inspect(&request.route, &inspection_context())
        .await
        .unwrap();
    assert!(unknown.model_version.is_none());
    assert_eq!(unknown.availability, ModelRouteAvailability::Unknown);
    assert_eq!(unknown.version_semantics, VersionSemantics::Unverified);
    let inspector = VertexInspector::new(connection, vec![snapshot()]).unwrap();
    inspector
        .inspect(&request.route, &inspection_context())
        .await
        .unwrap()
        .validate(&request.route, VersionPolicy::RequirePinned)
        .unwrap();
    assert_eq!(
        inspector
            .inspect(&request.route, &inspection_context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ModelVersionDrift
    );
    assert_eq!(
        inspector
            .inspect(&request.route, &inspection_context())
            .await
            .unwrap()
            .availability,
        ModelRouteAvailability::Unavailable
    );
    {
        let calls = server.requests.lock().unwrap();
        assert_eq!(calls[0].method, "GET");
        assert_eq!(
            calls[0].path,
            format!("/v1/publishers/google/models/{MODEL}?view=PUBLISHER_MODEL_VERSION_VIEW_BASIC")
        );
    }

    let server = Server::new(vec![Reply::json(200, metadata())]).await;
    let mut options = options(&server);
    options.location = "us-central1".into();
    let connection =
        VertexConnection::new(scope(), reference("account"), token(), options).unwrap();
    let request = support::request(&connection, MODEL);
    assert_eq!(
        VertexInspector::new(connection, vec![snapshot()])
            .unwrap()
            .inspect(&request.route, &inspection_context())
            .await
            .unwrap()
            .availability,
        ModelRouteAvailability::Unavailable
    );
}

#[tokio::test]
async fn error_status_and_redirect_never_retry_or_forward_tokens() {
    let destination = Server::new(vec![]).await;
    for (status, kind) in [
        (307, ModelFailureKind::Unsupported),
        (403, ModelFailureKind::Authentication),
        (429, ModelFailureKind::RateLimited),
        (503, ModelFailureKind::Transport),
    ] {
        let mut response = Reply::json(status, json!({"error":{"message":"private detail"}}));
        response
            .headers
            .push(("location", destination.base.clone()));
        let server = Server::new(vec![response]).await;
        let connection = connection(&server);
        let request = support::request(&connection, MODEL);
        let model = VertexModel::new(connection);
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, kind);
        assert!(!format!("{failure:?}").contains("private detail"));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
    assert!(destination.requests.lock().unwrap().is_empty());
}

struct MetadataPending(tokio::sync::Notify);
impl VertexTokenProvider for MetadataPending {
    fn token<'a>(&'a self, context: &'a VertexTokenContext<'a>) -> PortFuture<'a, VertexToken> {
        Box::pin(async move {
            assert_eq!(context.audience, VertexAudience::Metadata);
            self.0.notify_one();
            std::future::pending().await
        })
    }
}
#[tokio::test]
async fn metadata_token_and_http_waits_obey_cancellation_and_deadline() {
    for stage in ["token", "http"] {
        for cancel in [true, false] {
            let mut response = Reply::json(200, metadata());
            response.stall = true;
            let server = Server::new(vec![response]).await;
            let pending = Arc::new(MetadataPending(tokio::sync::Notify::new()));
            let tokens: Arc<dyn VertexTokenProvider> = if stage == "token" {
                pending.clone()
            } else {
                token()
            };
            let connection =
                VertexConnection::new(scope(), reference("account"), tokens, options(&server))
                    .unwrap();
            let request = support::request(&connection, MODEL);
            let inspector = VertexInspector::new(connection, vec![snapshot()]).unwrap();
            let mut context = inspection_context();
            context.deadline = tokio::time::Instant::now() + Duration::from_secs(1);
            let trigger = async {
                if stage == "token" {
                    pending.0.notified().await;
                } else {
                    server.entered.notified().await;
                }
                if cancel {
                    context.cancellation.cancel();
                }
            };
            let (result, _) = tokio::join!(inspector.inspect(&request.route, &context), trigger);
            assert_eq!(
                result.unwrap_err().code,
                if cancel {
                    ErrorCode::Cancelled
                } else {
                    ErrorCode::DeadlineExceeded
                }
            );
            if stage == "token" {
                assert!(server.requests.lock().unwrap().is_empty());
            } else {
                tokio::time::timeout(Duration::from_secs(1), server.closed.notified())
                    .await
                    .unwrap();
                assert_eq!(server.requests.lock().unwrap().len(), 1);
            }
        }
    }
}

#[tokio::test]
async fn minimal_thinking_is_rejected_for_both_flash_releases_before_network() {
    for release in ["gemini-3.7-flash", "gemini-3.8-flash"] {
        let server = Server::new(vec![reply(&events("would otherwise succeed"))]).await;
        let connection = connection(&server);
        let mut request = request(&connection, release);
        request
            .options
            .insert("thinking_level".into(), json!("minimal"));
        let model = VertexModel::new(connection);
        let error = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .expect_err("unsupported option must fail before dispatch");
        assert_eq!(error.kind, ModelFailureKind::Unsupported);
        assert!(server.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn two_documented_release_ids_keep_path_and_reported_revision_separate() {
    let releases = ["gemini-3.8-flash", "gemini-3.7-flash"];
    let replies = releases
        .iter()
        .enumerate()
        .map(|(index, release)| {
            let mut data = events(release);
            data[0]["modelVersion"] = json!(format!("fixture-revision-{index}"));
            reply(&data)
        })
        .collect();
    let server = Server::new(replies).await;
    let connection = connection(&server);
    let model = VertexModel::new(connection.clone());
    for (index, release) in releases.iter().enumerate() {
        let mut request = request(&connection, release);
        request.route.binding = reference(&format!("binding-{index}"));
        request.route.model_version = id(&format!("fixture-revision-{index}"));
        let result = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(result.text, *release);
        assert_eq!(
            result.metadata.reported_model_version,
            Some(id(&format!("fixture-revision-{index}")))
        );
        assert!(result.metadata.reported_model_id.is_none());
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (request, release) in requests.iter().zip(releases) {
        assert!(
            request
                .path
                .contains(&format!("/models/{release}:streamGenerateContent"))
        );
    }
}
```

## `crates/wickle-model-xai/tests/responses.rs`

```rust
//! xAI-specific Responses contracts and shared-dialect isolation.
mod support;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::time::Duration;
use support::*;
use wickle::*;
use wickle_model_responses::{ResponsesDecoder, SseEvent};
use wickle_model_xai::*;

fn function_events() -> Vec<Value> {
    let reasoning =
        json!({"id":"","type":"reasoning","summary":[],"encrypted_content":"ciphertext-fixture"});
    let call = json!({"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"query\":\"figures\"}","status":"completed"});
    vec![
        json!({"type":"response.created","response":{"id":"resp_1","model":MODEL,"status":"in_progress"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"","type":"reasoning","summary":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":reasoning}),
        json!({"type":"response.output_item.added","output_index":1,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":"fc_1","delta":"{\"query\":"}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":"fc_1","delta":"\"figures\"}"}),
        json!({"type":"response.function_call_arguments.done","output_index":1,"item_id":"fc_1","arguments":"{\"query\":\"figures\"}"}),
        json!({"type":"response.output_item.done","output_index":1,"item":call}),
        json!({"type":"response.completed","response":{"id":"resp_1","model":MODEL,"status":"completed","output":[reasoning,call]}}),
    ]
}
fn with_tool(request: &mut ModelRequest) {
    request.tools = vec![ModelTool {
        name: id("lookup"),
        description: "Read figures".into(),
        model_input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer"}},"required":["query"],"additionalProperties":false}),
    }];
}
#[tokio::test]
async fn explicit_stateless_contract_and_effort_do_not_leak_host_context() {
    let server = Server::new(vec![
        Reply::sse(&events(MODEL, "answer")),
        Reply::sse(&events(MODEL, "answer")),
    ])
    .await;
    let connection = connection(&server);
    let model = XaiModel::new(connection.clone());
    for effort in ["high", "xhigh"] {
        let mut request = request(&connection, MODEL);
        request
            .options
            .insert("reasoning_effort".into(), json!(effort));
        let response =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap();
        assert_eq!(response.text, "answer");
        assert_eq!(response.metadata.reported_model_id, Some(id(MODEL)));
        assert!(response.metadata.reported_model_version.is_none());
        assert_eq!(response.metadata.usage.unwrap().output_tokens, Some(7));
    }
    let calls = server.requests.lock().unwrap();
    assert_eq!(calls.len(), 2);
    for (call, effort) in calls.iter().zip(["high", "xhigh"]) {
        assert_eq!(call.method, "POST");
        assert_eq!(call.path, "/v1/responses");
        assert!(
            call.headers
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-key-not-a-secret")
        );
        assert_eq!(call.body["store"], false);
        assert_eq!(call.body["include"], json!(["reasoning.encrypted_content"]));
        assert_eq!(call.body["reasoning"]["effort"], effort);
        assert!(!call.body.to_string().contains("hidden-workspace"));
        assert!(call.body.get("previous_response_id").is_none());
    }
}
#[tokio::test]
async fn empty_or_missing_reasoning_ids_are_replayed_without_relaxing_other_dialects() {
    for missing in [false, true] {
        let mut data = function_events();
        if missing {
            data[1]["item"].as_object_mut().unwrap().remove("id");
            data[2]["item"].as_object_mut().unwrap().remove("id");
            data[8]["response"]["output"][0]
                .as_object_mut()
                .unwrap()
                .remove("id");
        }
        let server = Server::new(vec![
            Reply::sse(&data),
            Reply::sse(&events(MODEL, "received")),
        ])
        .await;
        let connection = connection(&server);
        let model = XaiModel::new(connection.clone());
        let mut request = request(&connection, MODEL);
        with_tool(&mut request);
        let mut strict = ResponsesDecoder::new(&request, None);
        assert!(data.iter().any(|value| {
            strict
                .event(SseEvent {
                    name: None,
                    data: value.to_string(),
                })
                .is_err()
        }));
        let first = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(first.finish, ModelFinish::ToolCalls);
        assert_eq!(first.tool_calls[0].provider_call_id, id("call_1"));
        request.messages.push(ModelMessage {
            role: ModelRole::Assistant,
            content: vec![
                ModelContent::ToolCall {
                    provider_call_id: id("call_1"),
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
                provider_call_id: id("call_1"),
                content: json!({"result":73}),
            }],
        });
        request.request_id = id("second-attempt");
        assert!(wickle_model_responses::encode_request(&request).is_err());
        let response =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap();
        assert_eq!(response.text, "received");
        {
            let calls = server.requests.lock().unwrap();
            let input = calls[1].body["input"].as_array().unwrap();
            assert_eq!(input[1], data[8]["response"]["output"][0]);
            assert_eq!(input[2]["call_id"], "call_1");
            assert_eq!(input[3]["type"], "function_call_output");
            assert_eq!(
                parse_json(input[3]["output"].as_str().unwrap()).unwrap(),
                json!({"result":73})
            );
            assert_eq!(
                input
                    .iter()
                    .filter(|i| i["type"] == "function_call")
                    .count(),
                1
            );
        }
        if let ModelContent::ToolCall { arguments, .. } = &mut request.messages[1].content[0] {
            arguments.insert("query".into(), json!("changed"));
        }
        assert!(
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .is_err()
        );
        assert_eq!(server.requests.lock().unwrap().len(), 2);
    }
}
#[tokio::test]
async fn legacy_usage_includes_reasoning_without_fabricating_missing_counts() {
    for (usage, input, output, valid) in [
        (
            json!({"prompt_tokens":32,"completion_tokens":9,"completion_tokens_details":{"reasoning_tokens":110},"total_tokens":151}),
            Some(32),
            Some(119),
            true,
        ),
        (
            json!({"input_tokens":32,"output_tokens":119,"total_tokens":151}),
            Some(32),
            Some(119),
            true,
        ),
        (
            json!({"prompt_tokens":32,"completion_tokens":9,"completion_tokens_details":{"reasoning_tokens":110}}),
            Some(32),
            None,
            true,
        ),
        (json!({}), None, None, true),
        (
            json!({"input_tokens":32,"prompt_tokens":33,"output_tokens":9}),
            None,
            None,
            false,
        ),
        (
            json!({"input_tokens":32,"output_tokens":9,"total_tokens":151}),
            None,
            None,
            false,
        ),
        (
            json!({"prompt_tokens":32,"completion_tokens":999,"total_tokens":151}),
            None,
            None,
            false,
        ),
    ] {
        let mut data = events(MODEL, "answer");
        data[5]["response"]["usage"] = usage;
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let request = request(&connection, MODEL);
        let model = XaiModel::new(connection);
        let response =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        if valid {
            let usage = response.unwrap().metadata.usage.unwrap();
            assert_eq!(usage.input_tokens, input);
            assert_eq!(usage.output_tokens, output);
        } else {
            assert!(response.is_err());
        }
    }
}
#[tokio::test]
async fn unsupported_options_scope_schema_and_service_tool_limit_fail_before_http() {
    for case in [
        "none",
        "minimal",
        "max",
        "verbosity",
        "scope",
        "api",
        "target",
        "tools",
        "schema",
    ] {
        let server = Server::new(vec![]).await;
        let connection = connection(&server);
        let mut request = request(&connection, MODEL);
        let mut context = context(&request);
        let model = XaiModel::new(connection);
        match case {
            "none" | "minimal" | "max" => {
                request
                    .options
                    .insert("reasoning_effort".into(), json!(case));
            }
            "verbosity" => {
                request.options.insert("verbosity".into(), json!("low"));
            }
            "scope" => context.scope.workspace_id = id("foreign"),
            "api" => request.route.api_contract.version = id("v2"),
            "target" => {
                request
                    .route
                    .target
                    .insert("base_url".into(), json!("https://other.example/v1/"));
            }
            "tools" => {
                request.limits.max_input_bytes = 1_000_000;
                request.tools=(0..351).map(|n|ModelTool{name:id(&format!("tool-{n}")),description:"Read".into(),model_input_schema:json!({"type":"object","properties":{},"additionalProperties":false})}).collect();
                request.validate().unwrap();
            }
            _ => {
                request.output = ModelOutput::JsonSchema {
                    schema: json!({"type":"object","properties":{"optional":{"type":"string"}},"additionalProperties":false}),
                }
            }
        }
        assert!(
            collect_model_response(&request, model.generate(&request, &context))
                .await
                .is_err(),
            "{case}"
        );
        assert!(server.requests.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn absent_executable_ids_ciphertext_or_terminals_never_produce_calls() {
    for case in [
        "item-id",
        "call-id",
        "ciphertext",
        "eof",
        "native",
        "conflict",
    ] {
        let mut data = function_events();
        match case {
            "item-id" => data[3]["item"]["id"] = json!(""),
            "call-id" => data[3]["item"]["call_id"] = json!(""),
            "ciphertext" => {
                data[8]["response"]["output"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("encrypted_content");
            }
            "eof" => {
                data.pop();
            }
            "native" => data[3]["item"]["type"] = json!("web_search_call"),
            _ => data[8]["response"]["output"][1]["arguments"] = json!("{}"),
        }
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let mut request = request(&connection, MODEL);
        with_tool(&mut request);
        let model = XaiModel::new(connection);
        assert!(
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .is_err(),
            "{case}"
        );
    }
}
#[tokio::test]
async fn native_json_output_and_actual_model_reporting_remain_explicit() {
    let server = Server::new(vec![Reply::sse(&events(
        "replacement-model",
        "{\"answer\":42}",
    ))])
    .await;
    let connection = connection(&server);
    let mut request = request(&connection, MODEL);
    let schema = json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false});
    request.output = ModelOutput::JsonSchema {
        schema: schema.clone(),
    };
    let model = XaiModel::new(connection);
    let response = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(
        response.metadata.reported_model_id,
        Some(id("replacement-model"))
    );
    assert_eq!(request.route.model_id, id(MODEL));
    assert!(response.metadata.reported_model_version.is_none());
    let calls = server.requests.lock().unwrap();
    assert_eq!(calls[0].body["text"]["format"]["schema"], schema);
    assert_eq!(calls[0].body["text"]["format"]["strict"], true);
}
#[tokio::test]
async fn cancellation_deadline_errors_and_redirects_close_without_retry() {
    for cancel in [true, false] {
        let mut data = events(MODEL, "partial");
        data.truncate(3);
        let mut reply = Reply::sse(&data);
        reply.stall = true;
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let request = request(&connection, MODEL);
        let model = XaiModel::new(connection);
        let mut context = context(&request);
        context.deadline = tokio::time::Instant::now() + Duration::from_secs(1);
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
    let destination = Server::new(vec![]).await;
    for (status, kind) in [
        (307, ModelFailureKind::Unsupported),
        (401, ModelFailureKind::Authentication),
        (429, ModelFailureKind::RateLimited),
        (503, ModelFailureKind::Transport),
    ] {
        let mut reply = Reply::json(status, json!({"error":{"message":"private detail"}}));
        reply.headers.push(("location", destination.base.clone()));
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let request = request(&connection, MODEL);
        let model = XaiModel::new(connection);
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, kind);
        assert!(!format!("{failure:?}").contains("private detail"));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
    assert!(destination.requests.lock().unwrap().is_empty());
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
#[tokio::test]
async fn models_metadata_does_not_infer_immutable_versions_from_names() {
    let metadata = json!({"object":"model","id":MODEL,"created":12345});
    let server = Server::new(vec![
        Reply::json(200, metadata.clone()),
        Reply::json(200, metadata),
        Reply::json(200, json!({"object":"model","id":"replacement"})),
        Reply::json(404, json!({})),
    ])
    .await;
    let connection = connection(&server);
    let request = request(&connection, MODEL);
    let unknown = XaiInspector::new(connection.clone(), vec![])
        .unwrap()
        .inspect(&request.route, &inspection_context())
        .await
        .unwrap();
    assert_eq!(unknown.version_semantics, VersionSemantics::Unverified);
    assert!(unknown.model_version.is_none());
    let inspector = XaiInspector::new(
        connection,
        vec![XaiSnapshot {
            model_id: id(MODEL),
            model_version: id("release"),
            evidence_ref: id("explicit-immutable-evidence"),
        }],
    )
    .unwrap();
    inspector
        .inspect(&request.route, &inspection_context())
        .await
        .unwrap()
        .validate(&request.route, VersionPolicy::RequirePinned)
        .unwrap();
    assert_eq!(
        inspector
            .inspect(&request.route, &inspection_context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ModelVersionDrift
    );
    assert_eq!(
        inspector
            .inspect(&request.route, &inspection_context())
            .await
            .unwrap()
            .availability,
        ModelRouteAvailability::Unavailable
    );
    let calls = server.requests.lock().unwrap();
    assert_eq!(calls[0].method, "GET");
    assert_eq!(calls[0].path, format!("/v1/models/{MODEL}"));
}

#[tokio::test]
async fn metadata_cancellation_scope_and_size_limits_are_independent_of_inference() {
    for case in ["cancel", "deadline", "scope", "large"] {
        let mut response = Reply::json(200, json!({"object":"model","id":MODEL}));
        response.stall = matches!(case, "cancel" | "deadline");
        if case == "large" {
            response = Reply::json(
                200,
                json!({"object":"model","id":MODEL,"padding":"x".repeat(70_000)}),
            );
        }
        let server = Server::new(vec![response]).await;
        let connection = connection(&server);
        let request = request(&connection, MODEL);
        let inspector = XaiInspector::new(connection, vec![]).unwrap();
        let mut context = inspection_context();
        context.deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        if case == "scope" {
            context.scope.workspace_id = id("foreign");
        }
        let trigger = async {
            if case == "cancel" {
                server.entered.notified().await;
                context.cancellation.cancel();
            }
        };
        let (result, _) = tokio::join!(inspector.inspect(&request.route, &context), trigger);
        assert_eq!(
            result.unwrap_err().code,
            match case {
                "cancel" => ErrorCode::Cancelled,
                "deadline" => ErrorCode::DeadlineExceeded,
                "scope" => ErrorCode::AccessDenied,
                _ => ErrorCode::ModelInspectionUnavailable,
            }
        );
        if case == "scope" {
            assert!(server.requests.lock().unwrap().is_empty());
        } else if matches!(case, "cancel" | "deadline") {
            tokio::time::timeout(Duration::from_secs(1), server.closed.notified())
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn reasoning_exceptions_never_allow_invalid_executable_identities() {
    let mut cases = vec![(None, None, true)];
    for field in ["id", "call_id"] {
        for value in [None, Some(Value::Null), Some(json!(42)), Some(json!(""))] {
            cases.push((Some(field), value, false));
        }
    }
    for (field, value, valid) in cases {
        let mut call = json!({"type":"function_call","id":"fc_1","call_id":"call_1","name":"lookup","arguments":"{\"query\":\"figures\"}","status":"completed"});
        if let Some(field) = field {
            if let Some(value) = value {
                call[field] = value;
            } else {
                call.as_object_mut().unwrap().remove(field);
            }
        }
        let data = vec![
            json!({"type":"response.created","response":{"id":"r","model":MODEL,"status":"in_progress"}}),
            json!({"type":"response.output_item.added","output_index":0,"item":call}),
            json!({"type":"response.output_item.done","output_index":0,"item":call}),
            json!({"type":"response.completed","response":{"id":"r","model":MODEL,"status":"completed","output":[call]}}),
        ];
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let mut request = request(&connection, MODEL);
        with_tool(&mut request);
        let model = XaiModel::new(connection);
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        if valid {
            assert_eq!(result.unwrap().finish, ModelFinish::ToolCalls);
        } else {
            assert!(result.is_err(), "{field:?}");
        }
    }
}

#[tokio::test]
async fn two_documented_release_ids_coexist_without_replacing_connection_state() {
    let releases = ["grok-4.6", "grok-4.5"];
    let server = Server::new(
        releases
            .iter()
            .map(|release| Reply::sse(&events(release, release)))
            .collect(),
    )
    .await;
    let connection = connection(&server);
    let model = XaiModel::new(connection.clone());
    for (index, release) in releases.iter().enumerate() {
        let mut request = request(&connection, release);
        request.route.binding = reference(&format!("binding-{index}"));
        request.route.model_version = id(&format!("fixture-revision-{index}"));
        let result = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(result.text, *release);
        assert_eq!(result.metadata.reported_model_id, Some(id(release)));
        assert!(result.metadata.reported_model_version.is_none());
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (request, release) in requests.iter().zip(releases) {
        assert_eq!(request.body["model"], release);
    }
}
```

## `tests/support/version_matrix_consumer.rs`

```rust
// Synthetic catalog evidence and capabilities; no provider availability is claimed.
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use wickle::*;
use wickle_model_router::ImmutableModelCatalog;

fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}

fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}

fn definition(provider: &str, version: &str, wire: &str, features: &[&str]) -> ModelDefinition {
    ModelDefinition {
        model_key: id("example-model"),
        family: id("example-family"),
        provider: id(provider),
        model_id: id(wire),
        model_version: id(version),
        version_semantics: VersionSemantics::Pinned,
        lifecycle: ModelLifecycle::Active,
        capabilities: ModelCapabilities {
            revision: id(version),
            features: features.iter().map(|value| id(value)).collect(),
            options_schema: json!({
                "type":"object", "properties":{"fixture_option":{"const":version}}, "additionalProperties":false
            }),
            context_window: 8192.try_into().unwrap(),
            max_output_tokens: 2048.try_into().unwrap(),
        },
        evidence: vec![ModelEvidence {
            source_ref: id("example-provider-manifest"),
            observed_at_ms: 1000,
        }],
    }
}

fn definition_ref(model: &ModelDefinition) -> ModelDefinitionRef {
    ModelDefinitionRef {
        provider: model.provider.clone(),
        model_key: model.model_key.clone(),
        model_version: model.model_version.clone(),
    }
}

fn binding(model: &ModelDefinition, name: &str) -> Result<ModelBinding, ContractError> {
    let mut binding = ModelBinding {
        binding: reference(name),
        model: definition_ref(model),
        requested_model: model.model_id.clone(),
        adapter: reference("example-adapter"),
        connection_ref: reference(name),
        target: BTreeMap::from([("region".into(), json!("example-region"))]),
        target_schema: json!({
            "type":"object", "properties":{"region":{"const":"example-region"}},
            "required":["region"], "additionalProperties":false
        }),
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("api-contract-1"),
        },
        deployment_revision: Some(id(name)),
        version_semantics: VersionSemantics::Pinned,
        capabilities: model.capabilities.clone(),
        support: ModelSupportStatus::ContractTested,
        evidence: vec![],
    };
    binding.evidence.push(ModelValidationEvidence {
        kind: ModelValidationKind::ContractTest,
        binding_digest: binding.contract_digest(model)?,
        checked_at_ms: 1001,
        evidence_ref: id("example-contract-fixture-result"),
        passed: true,
    });
    Ok(binding)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let specs = [
        ("openai", "gpt-6-astra", "gpt-5.6-sol"),
        ("azure-openai", "gpt-6-astra", "gpt-5.6-sol"),
        ("anthropic", "claude-opus-5", "claude-opus-4-8"),
        (
            "aws-bedrock",
            "anthropic.claude-opus-5",
            "anthropic.claude-opus-4-8",
        ),
        ("google-gemini", "gemini-3.8-flash", "gemini-3.7-flash"),
        ("google-vertex", "gemini-3.8-flash", "gemini-3.7-flash"),
        ("xai", "grok-4.6", "grok-4.5"),
    ];
    let mut snapshot = ModelCatalogSnapshot {
        revision: id("catalog-1"),
        scope: scope.clone(),
        models: vec![],
        bindings: vec![],
        aliases: vec![],
    };
    for (provider, first, second) in specs {
        let a = definition(provider, "fixture-r1", first, &["text"]);
        let b = definition(provider, "fixture-r2", second, &["text", "tool_calling"]);
        snapshot.aliases.push(ModelAlias {
            provider: id(provider),
            alias: id("preferred"),
            target: definition_ref(&a),
        });
        snapshot
            .bindings
            .push(binding(&a, &format!("{provider}-a"))?);
        snapshot
            .bindings
            .push(binding(&b, &format!("{provider}-b"))?);
        snapshot.models.extend([a, b]);
    }
    let catalog = ImmutableModelCatalog::new(snapshot.clone())?;
    let saved = serde_json::to_string(catalog.snapshot())?;
    let requirements = CatalogRequirements {
        features: BTreeSet::from([id("tool_calling")]),
        options: JsonObject::new(),
        input_tokens: 1024,
        max_output_tokens: 256.try_into().unwrap(),
        version_policy: VersionPolicy::RequirePinned,
        min_support: ModelSupportStatus::ContractTested,
    };
    for (provider, first, second) in specs {
        let a = catalog
            .get_binding(
                &scope,
                &id("catalog-1"),
                &reference(&format!("{provider}-a")),
            )
            .await?;
        let b = catalog
            .get_binding(
                &scope,
                &id("catalog-1"),
                &reference(&format!("{provider}-b")),
            )
            .await?;
        assert_eq!(a.model.provider, id(provider));
        assert_eq!(b.model.provider, id(provider));
        assert_eq!(a.model.model_id, id(first));
        assert_eq!(b.model.model_id, id(second));
        assert_eq!(a.model.model_key, b.model.model_key);
        assert_ne!(a.model.model_version, b.model.model_version);
        assert!(a.validate(&requirements).is_err());
        b.validate(&requirements)?;
        let wrong = JsonObject::from([("fixture_option".into(), json!("fixture-r2"))]);
        assert!(a.binding.capabilities.validate_options(&wrong).is_err());
        b.binding.capabilities.validate_options(&wrong)?;
        let mut live = requirements.clone();
        live.min_support = ModelSupportStatus::LiveVerified;
        assert!(b.validate(&live).is_err());
        let mut upgraded = snapshot.clone();
        upgraded.revision = id("catalog-2");
        upgraded
            .aliases
            .iter_mut()
            .find(|alias| alias.provider == id(provider))
            .unwrap()
            .target = definition_ref(&b.model);
        let new = ImmutableModelCatalog::new(upgraded)?;
        for (other, _, _) in specs {
            let target = new
                .resolve_alias(&scope, &id("catalog-2"), &id(other), &id("preferred"))
                .await?;
            assert_eq!(
                target.model_version,
                id(if other == provider {
                    "fixture-r2"
                } else {
                    "fixture-r1"
                })
            );
        }
        let restored = ImmutableModelCatalog::restore(&saved, &catalog.digest())?;
        assert_eq!(
            restored
                .resolve_alias(&scope, &id("catalog-1"), &id(provider), &id("preferred"))
                .await?
                .model_version,
            id("fixture-r1")
        );
        assert!(
            restored
                .get_binding(
                    &scope,
                    &id("catalog-2"),
                    &reference(&format!("{provider}-a"))
                )
                .await
                .is_err()
        );
    }
    let mut forged = snapshot.clone();
    forged.bindings[0].support = ModelSupportStatus::LiveVerified;
    assert!(ImmutableModelCatalog::new(forged).is_err());
    let mut planned = snapshot.clone();
    planned.bindings[1].support = ModelSupportStatus::Planned;
    planned.bindings[1].evidence.clear();
    let planned = ImmutableModelCatalog::new(planned)?;
    assert!(
        planned
            .get_binding(&scope, &id("catalog-1"), &reference("openai-b"))
            .await?
            .validate(&requirements)
            .is_err()
    );
    let mut retired = snapshot.clone();
    retired.models[1].lifecycle = ModelLifecycle::Retired;
    retired.bindings[1] = binding(&retired.models[1], "openai-b")?;
    let retired = ImmutableModelCatalog::new(retired)?;
    assert!(
        retired
            .get_binding(&scope, &id("catalog-1"), &reference("openai-b"))
            .await?
            .validate(&requirements)
            .is_err()
    );
    let mut changed: serde_json::Value = serde_json::from_str(&saved)?;
    changed["bindings"][0]["target"]["region"] = json!("different-region");
    assert!(ImmutableModelCatalog::restore(&changed.to_string(), &catalog.digest()).is_err());
    let mut foreign = scope.clone();
    foreign.workspace_id = id("foreign");
    assert!(
        catalog
            .get_binding(&foreign, &id("catalog-1"), &reference("openai-a"))
            .await
            .is_err()
    );
    println!(
        "version matrix: 7 provider namespaces / 14 coexisting catalog identities; capability differences, scoped alias upgrades, immutable restoration, target drift and planned/live/retired gates passed (synthetic evidence, no provider calls)"
    );
    Ok(())
}
```
