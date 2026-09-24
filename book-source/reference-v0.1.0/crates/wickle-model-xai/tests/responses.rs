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
