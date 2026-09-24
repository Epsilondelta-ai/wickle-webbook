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
