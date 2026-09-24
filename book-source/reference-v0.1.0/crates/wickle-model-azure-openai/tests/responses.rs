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
