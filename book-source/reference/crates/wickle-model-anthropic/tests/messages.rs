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
    assert_eq!(
        response.continuation[0].data()["kind"],
        "wickle.anthropic.messages.v1"
    );
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
    ] {
        let mut data = events("claude-opus-5", "answer");
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

fn invalid_tool_reply(raw: &str, initial: bool) -> Reply {
    let mut data = tool_events();
    data[5]["delta"]["partial_json"] = json!(raw);
    data.remove(6);
    if initial {
        data[4]["content_block"]["input"] = json!({"query":"initial"});
        data.remove(5);
    }
    let mut reply = Reply::sse(&data);
    if initial {
        reply.body = String::from_utf8(reply.body)
            .unwrap()
            .replace(
                r#""input":{"query":"initial"}"#,
                &format!("\"input\":{raw}"),
            )
            .into_bytes();
    }
    reply
}
#[tokio::test]
async fn complete_invalid_arguments_preserve_raw_evidence_and_signed_thinking_for_repair() {
    for (raw, initial) in [
        (r#"{"query":"unfinished""#, false),
        (r#"{"query":0.12345678901234567890123456789}"#, false),
        (r#"{"query":0.12345678901234567890123456789}"#, true),
        (r#"{"query":"one","query":"two"}"#, false),
        (r#"{"query":"one","query":"two"}"#, true),
        (r#"{"query":1e400}"#, true),
        (r#"["not an object"]"#, true),
        (r#"["not an object"]"#, false),
    ] {
        let server = Server::new(vec![
            invalid_tool_reply(raw, initial),
            Reply::sse(&events("claude-opus-5", "repair acknowledged")),
        ])
        .await;
        let connection = connection(&server);
        let model = AnthropicModel::new(connection.clone());
        let mut request = request(&connection, "claude-opus-5");
        tool(&mut request);
        let first = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(first.finish, ModelFinish::ToolCalls);
        assert_eq!(first.tool_calls[0].raw_arguments.as_deref(), Some(raw));
        assert!(first.tool_calls[0].model_inputs.is_empty());
        assert_eq!(
            first.tool_calls[0].validation,
            ToolCallValidation::InvalidArguments
        );
        assert_eq!(
            first.continuation[0].data()["kind"],
            "wickle.anthropic.messages.v2"
        );
        request.messages.push(ModelMessage {
            role: ModelRole::Assistant,
            content: vec![
                ModelContent::ToolCall {
                    provider_call_id: id("toolu_1"),
                    name: id("lookup"),
                    arguments: JsonObject::new(),
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
                content: json!({"status":"failed","error":{"code":"invalid_arguments"}}),
            }],
        });
        request.request_id = id("repair");
        let second = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(second.text, "repair acknowledged");
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let body = &requests[1].body;
        assert_eq!(
            body["messages"][1]["content"][0],
            json!({"type":"thinking","thinking":"","signature":"signature-fixture"})
        );
        assert_eq!(
            body["messages"][1]["content"][1]["input"],
            json!({"INVALID_JSON":raw})
        );
        let result = &body["messages"][2]["content"][0];
        assert_eq!(result["is_error"], true);
        assert_eq!(
            parse_json(result["content"].as_str().unwrap()).unwrap()["INVALID_JSON"],
            raw
        );
        drop(requests);
        for mode in ["valid-raw", "unknown-call", "changed-input", "extra-field"] {
            let mut changed = first.continuation[0].data().clone();
            match mode {
                "valid-raw" => changed["invalid_arguments"]["toolu_1"] = json!("{}"),
                "unknown-call" => changed["invalid_arguments"]["other"] = json!("{"),
                "changed-input" => changed["blocks"][1]["input"] = json!({"query":"tampered"}),
                _ => changed["extra"] = json!(true),
            }
            request.messages[1].content[1] = ModelContent::Opaque {
                continuation: OpaqueContinuation::new(&request.route, changed),
            };
            assert!(
                wickle_model_anthropic::protocol::encode_request(&request).is_err(),
                "{mode}"
            );
        }
        request.messages[1].content[1] = ModelContent::Opaque {
            continuation: first.continuation[0].clone(),
        };
        if let ModelContent::ToolCall { arguments, .. } = &mut request.messages[1].content[0] {
            arguments.insert("query".into(), json!("tampered"));
        }
        assert!(wickle_model_anthropic::protocol::encode_request(&request).is_err());
        assert_eq!(server.requests.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn length_limited_tool_input_never_becomes_a_repairable_completed_proposal() {
    let mut data = tool_events();
    data[6]["delta"]["partial_json"] = json!("\"cut off");
    data[8]["delta"]["stop_reason"] = json!("max_tokens");
    let server = Server::new(vec![Reply::sse(&data)]).await;
    let connection = connection(&server);
    let model = AnthropicModel::new(connection.clone());
    let mut request = request(&connection, "claude-opus-5");
    tool(&mut request);
    let result =
        collect_model_response(&request, model.generate(&request, &context(&request))).await;
    assert!(result.is_err());
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn opus_five_point_five_keeps_adaptive_thinking_and_rejects_disabled_or_manual_modes() {
    let server = Server::new(vec![Reply::sse(&events("claude-opus-5-5", "done"))]).await;
    let connection = connection(&server);
    let model = AnthropicModel::new(connection.clone());
    let mut request = request(&connection, "claude-opus-5-5");
    request.max_output_tokens = 4096.try_into().unwrap();
    for mode in ["disabled", "enabled"] {
        request.options.insert("thinking_mode".into(), json!(mode));
        if mode == "enabled" {
            request
                .options
                .insert("thinking_budget_tokens".into(), json!(1024));
        }
        assert!(
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .is_err()
        );
        assert!(server.requests.lock().unwrap().is_empty());
    }
    request.options.remove("thinking_budget_tokens");
    request
        .options
        .insert("thinking_mode".into(), json!("adaptive"));
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].body["thinking"], json!({"type":"adaptive"}));
    assert_eq!(requests[0].body["output_config"]["effort"], "medium");
}

#[tokio::test]
async fn initial_input_exemption_never_hides_duplicate_or_malformed_envelope_fields() {
    for mode in ["duplicate-index", "duplicate-input", "malformed-outer"] {
        let mut reply = invalid_tool_reply(r#"{"query":1e400}"#, true);
        let body = String::from_utf8(reply.body).unwrap();
        reply.body = match mode {
            "duplicate-index" => body.replacen("\"index\":1", "\"index\":1,\"index\":2", 1),
            "duplicate-input" => body.replacen("\"input\":", "\"input\":{},\"input\":", 1),
            _ => body.replacen("\"index\":1", "\"index\":1,\"unexpected\":", 1),
        }
        .into_bytes();
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let model = AnthropicModel::new(connection.clone());
        let mut request = request(&connection, "claude-opus-5");
        tool(&mut request);
        assert!(
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .is_err(),
            "{mode}"
        );
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn mixed_valid_and_invalid_calls_keep_each_result_and_error_marker_with_its_call() {
    let mut data = tool_events();
    data[5]["delta"]["partial_json"] = json!("{");
    data.remove(6);
    data.truncate(7);
    data.extend([
        json!({"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_2","name":"lookup","input":{}}}),
        json!({"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"query\":\"beta\"}"}}),
        json!({"type":"content_block_stop","index":2}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":20}}),
        json!({"type":"message_stop"}),
    ]);
    let server = Server::new(vec![
        Reply::sse(&data),
        Reply::sse(&events("claude-opus-5", "done")),
    ])
    .await;
    let connection = connection(&server);
    let model = AnthropicModel::new(connection.clone());
    let mut request = request(&connection, "claude-opus-5");
    tool(&mut request);
    let first = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(first.tool_calls.len(), 2);
    assert_eq!(
        first.tool_calls[0].validation,
        ToolCallValidation::InvalidArguments
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
        content: vec![
            ModelContent::ToolResult {
                provider_call_id: id("toolu_1"),
                content: json!({"error":"invalid_arguments"}),
            },
            ModelContent::ToolResult {
                provider_call_id: id("toolu_2"),
                content: json!({"answer":42}),
            },
        ],
    });
    request.request_id = id("second");
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let requests = server.requests.lock().unwrap();
    let body = &requests[1].body;
    assert_eq!(
        body["messages"][1]["content"][1]["input"],
        json!({"INVALID_JSON":"{"})
    );
    assert_eq!(
        body["messages"][1]["content"][2]["input"],
        json!({"query":"beta"})
    );
    let results = &body["messages"][2]["content"];
    assert_eq!(results[0]["tool_use_id"], "toolu_1");
    assert_eq!(results[0]["is_error"], true);
    assert_eq!(results[1]["tool_use_id"], "toolu_2");
    assert!(results[1].get("is_error").is_none());
    assert_eq!(
        parse_json(results[1]["content"].as_str().unwrap()).unwrap(),
        json!({"answer":42})
    );
}
