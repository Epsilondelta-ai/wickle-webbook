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
async fn stable_serialization_relaxes_closure_but_direct_collection_still_rejects_extra_arguments()
{
    let mut data = tool_events();
    data[0]["candidates"][0]["content"]["parts"][0]["functionCall"]["args"]["extra"] = json!(true);
    let server = Server::new(vec![reply(&data)]).await;
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
    let response = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(
        response.tool_calls[0].validation,
        ToolCallValidation::InvalidArguments
    );
    assert!(server.requests.lock().unwrap()[0].body["tools"][0]["functionDeclarations"][0]["parameters"].get("additionalProperties").is_none());
    request.tools[0].model_input_schema["properties"]["query"]["enum"] = json!(["alpha", "beta"]);
    let encoded: Value = serde_json::from_slice(
        &protocol::encode_request(&request, protocol::FunctionSchemaFormat::OpenApi).unwrap(),
    )
    .unwrap();
    assert_eq!(
        encoded["tools"][0]["functionDeclarations"][0]["parameters"]["type"],
        "OBJECT"
    );
    assert_eq!(
        encoded["tools"][0]["functionDeclarations"][0]["parameters"]["properties"]["query"]["enum"],
        json!(["alpha", "beta"])
    );
    // Direct callers bypassing the compiler must still provide a supported representation.
    request.tools[0].model_input_schema =
        json!({"type":"object","properties":{"choice":{"type":"integer","enum":[1,2]}}});
    assert!(protocol::encode_request(&request, protocol::FunctionSchemaFormat::OpenApi).is_err());
    assert_eq!(server.requests.lock().unwrap().len(), 1);
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

fn compiled_input(schema: Value) -> CompiledTool {
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
                description: "Read an authorized record".into(),
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
async fn compiled_dialects_preserve_native_constraints_and_keep_relaxed_rules_authoritative() {
    for version in ["v1", "v1beta"] {
        let server = Server::new(vec![reply(&events("done"))]).await;
        let connection = GeminiConnection::new(
            scope(),
            reference("account"),
            "fixture",
            GeminiOptions {
                base_url: server.base.trim_end_matches("v1/").into(),
                api_version: version.into(),
                ..Default::default()
            },
        )
        .unwrap();
        let model = GeminiModel::new(connection.clone());
        let mut request = request(&connection, MODEL);
        let original = compiled_input(json!({"type":"object","properties":{
            "query":{"type":"string","pattern":"^[a-z]+$","minLength":2},
            "limit":{"type":"integer","minimum":1,"maximum":10,"default":1},
            "note":{"type":["string","null"],"enum":["brief",null]},
            "choice":{"type":"integer","enum":[1,2]}
        },"required":["query"],"additionalProperties":false,"if":{"properties":{"query":{"const":"latest"}}},"then":{"properties":{"limit":{"minimum":8}}}}));
        let contract = CompiledToolContract::compile(
            &original,
            ProviderToolTarget::for_route(&request.route),
            model.tool_schema_compiler().as_ref(),
            Default::default(),
        )
        .unwrap();
        assert!(
            contract
                .enforcement()
                .iter()
                .any(|rule| rule.canonical_pointer == "/additionalProperties"
                    && rule.core
                    && rule.context_text
                    && !rule.provider_native)
        );
        assert!(
            contract
                .enforcement()
                .iter()
                .any(|rule| rule.canonical_pointer == "/if" && rule.core && rule.context_text)
        );
        for canonical in [
            JsonObject::from([("query".into(), json!("alpha"))]),
            JsonObject::from([
                ("query".into(), json!("alpha")),
                ("note".into(), Value::Null),
            ]),
        ] {
            let wire = contract.encode_arguments(&canonical).unwrap();
            assert_eq!(
                contract
                    .decode_arguments(&serde_json::to_string(&wire).unwrap(), Default::default())
                    .unwrap(),
                canonical
            );
        }
        let invalid = contract
            .decode_arguments(r#"{"query":"latest","limit":7}"#, Default::default())
            .unwrap();
        assert!(original.validate_model_inputs(&invalid).is_err());
        let canonical = JsonObject::from([
            ("query".into(), json!("latest")),
            ("limit".into(), json!(8)),
            ("note".into(), Value::Null),
            ("choice".into(), json!(2)),
        ]);
        original.validate_model_inputs(&canonical).unwrap();
        request.tools = vec![contract.wire_tool().clone()];
        for fragment in contract.constraint_fragments() {
            request.messages[0].content.push(ModelContent::Text {
                text: fragment.text.clone(),
            });
        }
        collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        let requests = server.requests.lock().unwrap();
        let declaration = &requests[0].body["tools"][0]["functionDeclarations"][0];
        let wire = &declaration[if version == "v1" {
            "parameters"
        } else {
            "parametersJsonSchema"
        }];
        assert_eq!(wire["properties"]["query"]["pattern"], "^[a-z]+$");
        assert_eq!(
            wire["properties"]["query"]["minLength"],
            if version == "v1" {
                json!("2")
            } else {
                json!(2)
            }
        );
        assert_eq!(wire["properties"]["limit"]["minimum"], 1);
        assert_eq!(wire["properties"]["limit"]["maximum"], 10);
        let note = &wire["properties"]["note"]["anyOf"];
        assert_eq!(note[0]["enum"], json!(["brief"]));
        assert_eq!(
            note[1]["type"],
            if version == "v1" {
                json!("NULL")
            } else {
                json!("null")
            }
        );
        if version == "v1" {
            assert!(wire.get("additionalProperties").is_none());
            assert_eq!(wire["properties"]["choice"]["type"], "INTEGER");
            assert!(wire["properties"]["choice"].get("enum").is_none());
            assert!(declaration.get("parametersJsonSchema").is_none());
        } else {
            assert_eq!(wire["additionalProperties"], false);
            assert_eq!(wire["properties"]["choice"]["enum"], json!([1, 2]));
            assert!(declaration.get("parameters").is_none());
        }
    }
}

#[tokio::test]
async fn openapi_free_form_values_use_json_text_and_parameterless_tools_omit_parameters() {
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
    let model = GeminiModel::new(connection.clone());
    let request = request(&connection, MODEL);
    let original = compiled_input(
        json!({"type":"object","properties":{"metadata":{"type":"object","additionalProperties":true},"tuple":{"type":"array","prefixItems":[{"type":"string"}],"items":false}},"required":[],"additionalProperties":false}),
    );
    let contract = CompiledToolContract::compile(
        &original,
        ProviderToolTarget::for_route(&request.route),
        model.tool_schema_compiler().as_ref(),
        Default::default(),
    )
    .unwrap();
    for value in [
        JsonObject::new(),
        JsonObject::from([
            ("metadata".into(), json!({"key":"value"})),
            ("tuple".into(), json!(["first"])),
        ]),
    ] {
        let encoded = contract.encode_arguments(&value).unwrap();
        assert_eq!(
            contract
                .decode_arguments(
                    &serde_json::to_string(&encoded).unwrap(),
                    Default::default()
                )
                .unwrap(),
            value
        );
    }
    let original = compiled_input(
        json!({"type":"object","properties":{},"required":[],"additionalProperties":false}),
    );
    let contract = CompiledToolContract::compile(
        &original,
        ProviderToolTarget::for_route(&request.route),
        model.tool_schema_compiler().as_ref(),
        Default::default(),
    )
    .unwrap();
    let mut request = request;
    request.tools = vec![contract.wire_tool().clone()];
    let body: Value = serde_json::from_slice(
        &protocol::encode_request(&request, protocol::FunctionSchemaFormat::OpenApi).unwrap(),
    )
    .unwrap();
    assert!(
        body["tools"][0]["functionDeclarations"][0]
            .get("parameters")
            .is_none()
    );
    assert!(
        original
            .validate_model_inputs(&JsonObject::from([("extra".into(), json!(true))]))
            .is_err()
    );
}

fn raw_content_part(body: &str, content: usize, part: usize) -> String {
    type Raw<'a> = std::collections::BTreeMap<String, &'a serde_json::value::RawValue>;
    let body: Raw<'_> = serde_json::from_str(body).unwrap();
    let contents: Vec<&serde_json::value::RawValue> =
        serde_json::from_str(body["contents"].get()).unwrap();
    let content: Raw<'_> = serde_json::from_str(contents[content].get()).unwrap();
    let parts: Vec<&serde_json::value::RawValue> =
        serde_json::from_str(content["parts"].get()).unwrap();
    parts[part].get().into()
}

#[tokio::test]
async fn rejected_numeric_tokens_and_signed_parts_survive_exact_request_byte_replay() {
    let original = r#"{"functionCall": {"name":"lookup","args":{"query":0.12345678901234567890123456789}},"thoughtSignature":"signed-call"}"#;
    let mut data = tool_events();
    data[0]["candidates"][0]["content"]["parts"][1]["functionCall"]["args"]["query"] = json!(1);
    let part = data[0]["candidates"][0]["content"]["parts"][0].to_string();
    let mut response = reply(&data);
    response.body = String::from_utf8(response.body)
        .unwrap()
        .replace(&part, original)
        .into_bytes();
    let server = Server::new(vec![response, reply(&events("done"))]).await;
    let connection = connection(&server);
    let model = GeminiModel::new(connection.clone());
    let mut request = request(&connection, MODEL);
    tool(&mut request);
    request.tools[0].model_input_schema["properties"]["query"] = json!({"type":"number"});
    let first = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(
        first.tool_calls[0].raw_arguments.as_deref(),
        Some(r#"{"query":0.12345678901234567890123456789}"#)
    );
    assert_eq!(
        first.tool_calls[0].validation,
        ToolCallValidation::InvalidArguments
    );
    assert_eq!(first.tool_calls[1].validation, ToolCallValidation::Valid);
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
                content: json!({"error":"repair input"}),
            })
            .collect(),
    });
    request.request_id = id("next");
    // Prepared requests cross this protected JSON boundary before retry/recovery.
    let stored = serde_json::to_vec(&request).unwrap();
    let mut request: ModelRequest = serde_json::from_slice(&stored).unwrap();
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let calls = server.requests.lock().unwrap();
    assert_eq!(raw_content_part(&calls[1].raw_body, 1, 0), original);
    assert!(
        calls[1].body["contents"][2]["parts"][0]["functionResponse"]
            .get("id")
            .is_none()
    );
    assert_eq!(
        calls[1].body["contents"][2]["parts"][1]["functionResponse"]["id"],
        "provider-call"
    );
    drop(calls);
    for mode in ["signature", "valid-raw", "extra-index", "extra-key"] {
        let mut data = first.continuation[0].data().clone();
        match mode {
            "signature" => data["parts"][0]["thoughtSignature"] = json!("tampered"),
            "valid-raw" => {
                data["raw_parts"]["0"] = json!(
                    r#"{"functionCall":{"name":"lookup","args":{}},"thoughtSignature":"signed-call"}"#
                )
            }
            "extra-index" => data["raw_parts"]["99"] = json!(original),
            _ => data["extra"] = json!(true),
        }
        let last = request.messages[1].content.len() - 1;
        request.messages[1].content[last] = ModelContent::Opaque {
            continuation: OpaqueContinuation::new(&request.route, data),
        };
        assert!(
            protocol::encode_request(&request, protocol::FunctionSchemaFormat::JsonSchema).is_err(),
            "{mode}"
        );
    }
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn root_constraints_and_reference_siblings_survive_projection_without_overwriting_intersections()
 {
    let server = Server::new(vec![reply(&events("done"))]).await;
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
    let model = GeminiModel::new(connection.clone());
    let mut request = request(&connection, MODEL);
    let original = compiled_input(json!({"type":"object","properties":{
        "query":{"$ref":"#/$defs/text","maxLength":20},
        "limit":{"$ref":"#/$defs/number","minimum":3,"maximum":8},
        "conflict":{"$ref":"#/$defs/text","pattern":"z$"}
    },"required":["query"],"additionalProperties":false,"minProperties":2,"maxProperties":3,"title":"Bounded inputs","$defs":{"text":{"type":"string","pattern":"^[a-z]+$"},"number":{"type":"integer","minimum":1,"maximum":10}}}));
    let contract = CompiledToolContract::compile(
        &original,
        ProviderToolTarget::for_route(&request.route),
        model.tool_schema_compiler().as_ref(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        contract.wire_tool().model_input_schema["properties"]["query"]["pattern"],
        "^[a-z]+$"
    );
    assert_eq!(
        contract.wire_tool().model_input_schema["properties"]["query"]["maxLength"],
        20
    );
    assert_eq!(
        contract.wire_tool().model_input_schema["properties"]["limit"]["minimum"],
        3
    );
    assert_eq!(
        contract.wire_tool().model_input_schema["properties"]["limit"]["maximum"],
        8
    );
    let canonical = JsonObject::from([
        ("query".into(), json!("alpha")),
        ("conflict".into(), json!("az")),
    ]);
    let encoded = contract.encode_arguments(&canonical).unwrap();
    assert_eq!(encoded["conflict"], json!("\"az\""));
    assert_eq!(
        contract
            .decode_arguments(
                &serde_json::to_string(&encoded).unwrap(),
                Default::default()
            )
            .unwrap(),
        canonical
    );
    assert!(
        original
            .validate_model_inputs(&JsonObject::from([("query".into(), json!("alpha"))]))
            .is_err()
    );
    request.tools = vec![contract.wire_tool().clone()];
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let requests = server.requests.lock().unwrap();
    let schema = &requests[0].body["tools"][0]["functionDeclarations"][0]["parameters"];
    assert_eq!(schema["minProperties"], "2");
    assert_eq!(schema["maxProperties"], "3");
}

#[tokio::test]
async fn compiler_rejects_a_dialect_that_does_not_match_the_saved_destination() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let request = request(&connection, MODEL);
    let tool = ModelTool {
        name: id("lookup"),
        description: "Lookup".into(),
        model_input_schema: json!({"type":"object","properties":{},"required":[],"additionalProperties":false}),
    };
    let mut target = ProviderToolTarget::for_route(&request.route);
    let openapi = protocol::GeminiToolSchemaCompiler::new(protocol::FunctionSchemaFormat::OpenApi);
    let json = protocol::GeminiToolSchemaCompiler::new(protocol::FunctionSchemaFormat::JsonSchema);
    assert!(openapi.compile(&tool, &target).is_err());
    json.compile(&tool, &target).unwrap();
    target.api_contract.version = id("v1");
    openapi.compile(&tool, &target).unwrap();
    assert!(json.compile(&tool, &target).is_err());
    target.provider = id("google-vertex");
    json.compile(&tool, &target).unwrap();
    assert!(openapi.compile(&tool, &target).is_err());
}

#[tokio::test]
async fn compact_branching_references_exhaust_a_shared_budget_and_fall_back_without_expanding() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let request = request(&connection, MODEL);
    let mut defs = serde_json::Map::new();
    defs.insert("level7".into(), json!({"type":"string"}));
    for level in (0..7).rev() {
        let props: serde_json::Map<String, Value> = (0..4)
            .map(|branch| {
                (
                    format!("branch{branch}"),
                    json!({"$ref":format!("#/$defs/level{}",level+1)}),
                )
            })
            .collect();
        let required: Vec<_> = props.keys().cloned().collect();
        defs.insert(format!("level{level}"),json!({"type":"object","properties":props,"required":required,"additionalProperties":false}));
    }
    let tool = ModelTool {
        name: id("lookup"),
        description: "Lookup".into(),
        model_input_schema: json!({"type":"object","properties":{"query":{"$ref":"#/$defs/level0"}},"required":["query"],"additionalProperties":false,"$defs":defs}),
    };
    let compiler =
        protocol::GeminiToolSchemaCompiler::new(protocol::FunctionSchemaFormat::JsonSchema);
    let projection = compiler
        .compile(&tool, &ProviderToolTarget::for_route(&request.route))
        .unwrap();
    assert_eq!(
        projection.wire_tool.model_input_schema["properties"]["query"],
        json!({"type":"string"})
    );
    let ArgumentDecodePlan::Fields { fields } = projection.decode_plan else {
        panic!("missing field codec")
    };
    assert!(matches!(
        fields[0].encoding,
        ArgumentValueEncoding::JsonText { optional: false }
    ));
}
