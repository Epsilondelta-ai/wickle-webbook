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
