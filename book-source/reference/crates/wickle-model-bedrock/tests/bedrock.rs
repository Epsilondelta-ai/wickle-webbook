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
