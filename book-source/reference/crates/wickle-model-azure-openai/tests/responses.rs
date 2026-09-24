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
    assert_eq!(calls[0].body["tools"][0]["strict"], true);
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

fn compiled_tool(schema: Value) -> CompiledTool {
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
                description: "Read data".into(),
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
async fn azure_projection_respects_its_native_limits_and_preserves_validation_and_release_identity()
{
    let server = Server::new(vec![Reply::sse(&events("base-model", "done"))]).await;
    let connection = connection(&server);
    let model = AzureOpenAiModel::new(connection.clone());
    let mut request = request(&connection, "base-model");
    let original = compiled_tool(
        json!({"type":"object","properties":{"query":{"type":"string","pattern":"^[a-z]+$","minLength":2}},"required":["query"],"additionalProperties":false}),
    );
    let target = ProviderToolTarget::for_route(&request.route);
    let contract = CompiledToolContract::compile(
        &original,
        target.clone(),
        model.tool_schema_compiler().as_ref(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        contract.wire_tool().model_input_schema["properties"]["query"],
        json!({"type":"string"})
    );
    let decoded = contract
        .decode_arguments(r#"{"query":"INVALID"}"#, Default::default())
        .unwrap();
    assert!(original.validate_model_inputs(&decoded).is_err());
    assert!(
        contract
            .enforcement()
            .iter()
            .any(
                |entry| entry.canonical_pointer == "/properties/query/pattern"
                    && entry.core
                    && entry.context_text
                    && !entry.provider_native
            )
    );
    let saved = serde_json::to_string(&contract).unwrap();
    CompiledToolContract::restore(
        &saved,
        &original,
        &target,
        contract.digest(),
        Default::default(),
    )
    .unwrap();
    request.route.model_version = id("new-release");
    assert!(
        CompiledToolContract::restore(
            &saved,
            &original,
            &ProviderToolTarget::for_route(&request.route),
            contract.digest(),
            Default::default()
        )
        .is_err()
    );
    request.route.model_version = id("release");
    request.tools = vec![contract.wire_tool().clone()];
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let calls = server.requests.lock().unwrap();
    assert_eq!(calls[0].body["tools"][0]["strict"], true);
    assert_eq!(
        calls[0].body["tools"][0]["parameters"],
        contract.wire_tool().model_input_schema
    );
    assert_eq!(calls[0].body["model"], "finance-deployment");
}

#[tokio::test]
async fn azure_compacts_wide_and_deep_values_without_narrowing_the_canonical_shape() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let model = AzureOpenAiModel::new(connection.clone());
    let target = ProviderToolTarget::for_route(&request(&connection, "base-model").route);
    let properties: serde_json::Map<String, Value> = (0..100)
        .map(|i| (format!("field{i}"), json!({"type":"string"})))
        .collect();
    let required: Vec<_> = properties.keys().cloned().collect();
    let wide = json!({"type":"object","properties":properties,"required":required,"additionalProperties":false});
    let wide_value: serde_json::Map<String, Value> = (0..100)
        .map(|i| (format!("field{i}"), json!("value")))
        .collect();
    let mut deep =
        json!({"type":"object","properties":{},"required":[],"additionalProperties":false});
    let mut deep_value = json!({});
    for _ in 0..5 {
        deep = json!({"type":"object","properties":{"child":deep},"required":["child"],"additionalProperties":false});
        deep_value = json!({"child":deep_value});
    }
    for (schema, value) in [(wide, Value::Object(wide_value)), (deep, deep_value)] {
        let original = compiled_tool(
            json!({"type":"object","properties":{"query":schema},"required":["query"],"additionalProperties":false}),
        );
        let azure = CompiledToolContract::compile(
            &original,
            target.clone(),
            model.tool_schema_compiler().as_ref(),
            Default::default(),
        )
        .unwrap();
        let openai = CompiledToolContract::compile(
            &original,
            target.clone(),
            &wickle_model_responses::ResponsesToolSchemaCompiler,
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            openai.wire_tool().model_input_schema,
            *original.model_input_schema()
        );
        assert_eq!(
            azure.wire_tool().model_input_schema["properties"]["query"],
            json!({"type":"string"})
        );
        assert_ne!(azure.compiler(), openai.compiler());
        let canonical = JsonObject::from([("query".into(), value)]);
        let encoded = azure.encode_arguments(&canonical).unwrap();
        let decoded = azure
            .decode_arguments(
                &serde_json::to_string(&encoded).unwrap(),
                Default::default(),
            )
            .unwrap();
        assert_eq!(decoded, canonical);
        original.validate_model_inputs(&decoded).unwrap();
        let invalid = azure
            .decode_arguments(r#"{"query":"42"}"#, Default::default())
            .unwrap();
        assert!(original.validate_model_inputs(&invalid).is_err());
    }
}

#[tokio::test]
async fn top_level_property_limit_uses_one_json_object_without_exposing_system_fields() {
    let server = Server::new(vec![
        Reply::sse(&events("base-model", "done")),
        Reply::sse(&events("base-model", "done")),
    ])
    .await;
    let connection = connection(&server);
    let model = AzureOpenAiModel::new(connection.clone());
    for count in [100, 101] {
        let properties: serde_json::Map<String, Value> = (0..count)
            .map(|i| (format!("field{i}"), json!({"type":["string","null"]})))
            .collect();
        let mut required: Vec<_> = properties.keys().cloned().collect();
        // Omission/null preservation is checked on the packed case.
        if count == 101 {
            required.retain(|name| name != "field0");
        }
        let original = compiled_tool(
            json!({"type":"object","properties":properties,"required":required,"additionalProperties":false}),
        );
        let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
            key: id("workspace_id"),
            version: id("1"),
            value_schema: json!({"type":"string","format":"uuid"}),
            source: SystemInputSource::Run {},
        }])
        .unwrap();
        let mut descriptor = original.descriptor().clone();
        descriptor.input_schema["properties"]["workspace_id"] =
            json!({"type":"string","format":"uuid","description":"hidden-system-schema"});
        descriptor.input_schema["required"]
            .as_array_mut()
            .unwrap()
            .push(json!("workspace_id"));
        let original = SchemaCompiler::new()
            .compile(descriptor, &registry)
            .unwrap();
        let mut request = request(&connection, "base-model");
        let target = ProviderToolTarget::for_route(&request.route);
        let contract = CompiledToolContract::compile(
            &original,
            target.clone(),
            model.tool_schema_compiler().as_ref(),
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            contract.wire_tool().model_input_schema["properties"]
                .as_object()
                .unwrap()
                .len(),
            if count == 100 { 100 } else { 1 }
        );
        let canonical: JsonObject = (0..count)
            .filter(|i| count == 100 || *i != 0)
            .map(|i| {
                (
                    format!("field{i}"),
                    if i == 1 { Value::Null } else { json!("value") },
                )
            })
            .collect();
        let encoded = contract.encode_arguments(&canonical).unwrap();
        let decoded = contract
            .decode_arguments(
                &serde_json::to_string(&encoded).unwrap(),
                Default::default(),
            )
            .unwrap();
        assert_eq!(decoded, canonical);
        original.validate_model_inputs(&decoded).unwrap();
        let saved = serde_json::to_string(&contract).unwrap();
        let restored = CompiledToolContract::restore(
            &saved,
            &original,
            &target,
            contract.digest(),
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            restored
                .decode_arguments(
                    &serde_json::to_string(&encoded).unwrap(),
                    Default::default()
                )
                .unwrap(),
            canonical
        );
        for fragment in contract.constraint_fragments() {
            assert!(!fragment.text.contains("hidden-system-schema"));
        }
        if count == 101 {
            for invalid in [
                json!({"arguments":"{"}),
                json!({"arguments":"[]"}),
                json!({"arguments":{}}),
                json!({"arguments":"{}","extra":1}),
                json!({}),
                json!({"arguments":"{\"field1\":1,\"field1\":2}"}),
                json!({"arguments":"{\"field1\":0.1234567890123456789012345}"}),
            ] {
                assert!(
                    contract
                        .decode_arguments(&invalid.to_string(), Default::default())
                        .is_err()
                );
            }
            let invalid = contract
                .decode_arguments(r#"{"arguments":"{}"}"#, Default::default())
                .unwrap();
            assert!(original.validate_model_inputs(&invalid).is_err());
            let hidden = contract
                .decode_arguments(
                    r#"{"arguments":"{\"workspace_id\":\"invented\"}"}"#,
                    Default::default(),
                )
                .unwrap();
            assert!(original.validate_model_inputs(&hidden).is_err());
        }
        request.tools = vec![contract.wire_tool().clone()];
        collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
    }
    for request in server.requests.lock().unwrap().iter() {
        assert_eq!(request.body["tools"][0]["strict"], true);
    }
}

#[tokio::test]
async fn empty_object_depth_boundaries_preserve_native_schema_with_bounded_compiler() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let target = ProviderToolTarget::for_route(&request(&connection, "base-model").route);
    for levels in [5, 6, 11] {
        let mut schema =
            json!({"type":"object","properties":{},"required":[],"additionalProperties":false});
        for _ in 1..levels {
            schema = json!({"type":"object","properties":{"child":schema},"required":["child"],"additionalProperties":false});
        }
        let original = compiled_tool(schema);
        let openai = CompiledToolContract::compile(
            &original,
            target.clone(),
            &wickle_model_responses::ResponsesToolSchemaCompiler,
            Default::default(),
        )
        .unwrap();
        assert_eq!(openai.compiler().version, id("2"));
        assert_eq!(
            openai.wire_tool().model_input_schema,
            *original.model_input_schema()
        );
        let azure = CompiledToolContract::compile(
            &original,
            target.clone(),
            &wickle_model_responses::AzureResponsesToolSchemaCompiler,
            Default::default(),
        )
        .unwrap();
        if levels == 5 {
            assert_eq!(
                azure.wire_tool().model_input_schema,
                *original.model_input_schema()
            );
        } else {
            assert_eq!(
                azure.wire_tool().model_input_schema["properties"]["child"],
                json!({"type":"string"})
            );
        }
    }
}
