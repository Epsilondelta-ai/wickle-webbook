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

fn compiled_input(schema: Value) -> CompiledTool {
    let properties = schema["properties"]
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
                agent_parameters: properties,
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
async fn compiled_optional_and_nested_inputs_use_strict_wire_and_restore_the_original_contract() {
    let server = Server::new(vec![Reply::sse(&events("model", "done"))]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    let original = compiled_input(json!({"type":"object","properties":{
        "query":{"type":"string","minLength":2,"pattern":"^[a-z]+$"},
        "note":{"type":["string","null"]},
        "filter":{"type":"object","properties":{"category":{"type":"string"},"term":{"type":"string"}},"required":["category"],"additionalProperties":false}
    },"required":["query"],"additionalProperties":false,"if":{"properties":{"query":{"const":"latest"}}},"then":{"required":["note"]}}));
    let contract = CompiledToolContract::compile(
        &original,
        ProviderToolTarget::for_route(&request.route),
        model.tool_schema_compiler().as_ref(),
        ProviderToolSchemaLimits::default(),
    )
    .unwrap();
    request.tools = vec![contract.wire_tool().clone()];
    for fragment in contract.constraint_fragments() {
        request.messages[0].content.push(ModelContent::Text {
            text: fragment.text.clone(),
        });
    }
    let canonical = JsonObject::from([
        ("query".into(), json!("latest")),
        ("note".into(), Value::Null),
        ("filter".into(), json!({"category":"finance"})),
    ]);
    let wire = contract.encode_arguments(&canonical).unwrap();
    assert_eq!(wire["note"], json!({"present":true,"value":null}));
    assert_eq!(
        parse_json(wire["filter"].as_str().unwrap()).unwrap(),
        json!([{"category":"finance"}])
    );
    assert_eq!(
        contract
            .decode_arguments(
                &serde_json::to_string(&wire).unwrap(),
                ProviderToolSchemaLimits::default()
            )
            .unwrap(),
        canonical
    );
    let omitted = JsonObject::from([("query".into(), json!("other"))]);
    let encoded = contract.encode_arguments(&omitted).unwrap();
    assert_eq!(
        contract
            .decode_arguments(
                &serde_json::to_string(&encoded).unwrap(),
                ProviderToolSchemaLimits::default()
            )
            .unwrap(),
        omitted
    );
    assert!(
        contract
            .enforcement()
            .iter()
            .any(|constraint| constraint.canonical_pointer == "/if"
                && constraint.context_text
                && !constraint.provider_native)
    );
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let body = &server.requests.lock().unwrap()[0].body;
    assert_eq!(body["tools"][0]["strict"], true);
    assert_eq!(
        body["tools"][0]["parameters"],
        contract.wire_tool().model_input_schema
    );
    assert_eq!(
        body["tools"][0]["parameters"]["required"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        body["tools"][0]["parameters"]["properties"]["query"]["pattern"],
        "^[a-z]+$"
    );
    assert!(
        body["tools"][0]["parameters"]["properties"]["query"]
            .get("minLength")
            .is_none()
    );
}

#[tokio::test]
async fn model_qualified_compilation_respects_fine_tuned_native_constraint_limits() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let original = compiled_input(
        json!({"type":"object","properties":{"query":{"type":"string","pattern":"^[a-z]+$"}},"required":["query"],"additionalProperties":false}),
    );
    let mut route = request(&connection, "gpt-6-astra").route;
    let normal = CompiledToolContract::compile(
        &original,
        ProviderToolTarget::for_route(&route),
        model.tool_schema_compiler().as_ref(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        normal.wire_tool().model_input_schema,
        *original.model_input_schema()
    );
    route.model_id = id("ft:base:fixture:release");
    let tuned = CompiledToolContract::compile(
        &original,
        ProviderToolTarget::for_route(&route),
        model.tool_schema_compiler().as_ref(),
        Default::default(),
    )
    .unwrap();
    assert!(
        tuned.wire_tool().model_input_schema["properties"]["query"]
            .get("pattern")
            .is_none()
    );
    assert!(
        tuned
            .enforcement()
            .iter()
            .any(
                |constraint| constraint.canonical_pointer == "/properties/query/pattern"
                    && constraint.core
                    && constraint.context_text
            )
    );
    assert_ne!(normal.digest(), tuned.digest());
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn oversized_native_enum_uses_reversible_text_without_losing_original_validation() {
    let server = Server::new(vec![Reply::sse(&events("model", "done"))]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    let values: Vec<_> = (0..1001).map(|index| format!("choice-{index}")).collect();
    let original = compiled_input(
        json!({"type":"object","properties":{"query":{"type":"string","enum":values}},"required":["query"],"additionalProperties":false}),
    );
    let contract = CompiledToolContract::compile(
        &original,
        ProviderToolTarget::for_route(&request.route),
        model.tool_schema_compiler().as_ref(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        contract.wire_tool().model_input_schema["properties"]["query"],
        json!({"type":"string"})
    );
    for (value, valid) in [("choice-1000", true), ("invented-choice", false)] {
        let canonical = JsonObject::from([("query".into(), json!(value))]);
        let encoded = contract.encode_arguments(&canonical).unwrap();
        let restored = contract
            .decode_arguments(
                &serde_json::to_string(&encoded).unwrap(),
                Default::default(),
            )
            .unwrap();
        assert_eq!(restored, canonical);
        assert_eq!(original.validate_model_inputs(&restored).is_ok(), valid);
    }
    request.tools = vec![contract.wire_tool().clone()];
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(
        server.requests.lock().unwrap()[0].body["tools"][0]["strict"],
        true
    );
}

#[tokio::test]
async fn shared_reference_work_budget_falls_back_without_losing_canonical_values() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let request = request(&connection, "model");
    let mut defs = serde_json::Map::new();
    defs.insert("level3".into(),json!({"type":"string","minLength":1,"description":"A leaf selected from a shared reference graph"}));
    let mut value = json!("selected");
    for level in (0..3).rev() {
        let properties: serde_json::Map<String, Value> = (0..5)
            .map(|index| {
                (
                    format!("branch{index}"),
                    json!({"$ref":format!("#/$defs/level{}",level+1)}),
                )
            })
            .collect();
        let required: Vec<_> = properties.keys().cloned().collect();
        defs.insert(format!("level{level}"),json!({"type":"object","properties":properties,"required":required,"additionalProperties":false}));
        value = Value::Object(
            (0..5)
                .map(|index| (format!("branch{index}"), value.clone()))
                .collect(),
        );
    }
    let canonical = compiled_input(
        json!({"type":"object","properties":{"a":{"$ref":"#/$defs/level0"},"b":{"$ref":"#/$defs/level0"}},"required":["a","b"],"additionalProperties":false,"if":{"required":["a"]},"then":{"required":["b"]},"$defs":defs}),
    );
    let contract = CompiledToolContract::compile(
        &canonical,
        ProviderToolTarget::for_route(&request.route),
        &wickle_model_responses::ResponsesToolSchemaCompiler,
        Default::default(),
    )
    .unwrap();
    // Each field alone fits native limits. Expansion work across both fields must
    // share one budget rather than reset at each reference or top-level property.
    assert_eq!(
        contract.wire_tool().model_input_schema["properties"]["a"]["type"],
        "object"
    );
    assert_eq!(
        contract.wire_tool().model_input_schema["properties"]["b"]["type"],
        "string"
    );
    let values = JsonObject::from([("a".into(), value.clone()), ("b".into(), value)]);
    canonical.validate_model_inputs(&values).unwrap();
    let encoded = contract.encode_arguments(&values).unwrap();
    assert!(encoded["b"].is_string());
    assert_eq!(
        contract
            .decode_arguments(
                &serde_json::to_string(&encoded).unwrap(),
                Default::default()
            )
            .unwrap(),
        values
    );
    let mut invalid = values;
    invalid.get_mut("b").unwrap()["branch0"]["branch0"]["branch0"] = json!(7);
    let encoded = contract.encode_arguments(&invalid).unwrap();
    let decoded = contract
        .decode_arguments(
            &serde_json::to_string(&encoded).unwrap(),
            Default::default(),
        )
        .unwrap();
    assert!(canonical.validate_model_inputs(&decoded).is_err());
}
