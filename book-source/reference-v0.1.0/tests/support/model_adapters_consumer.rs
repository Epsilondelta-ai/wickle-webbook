// Cross-provider dispatcher/authorization conformance using extracted adapter packages.
use futures_util::StreamExt;
use std::{net::TcpListener, sync::Arc, time::Duration};
use wickle::*;
use wickle_model_router::{ModelDispatcherEntry, RegistryModelDispatcher};
fn id(value: &str) -> Id {
    Id::new(value).expect("fixture identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn request(
    binding: ModelPortBinding,
    target: JsonObject,
    api_contract: ApiContract,
) -> ModelRequest {
    ModelRequest {
        request_id: id("attempt"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference("selected"),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id("fixture-model"),
            model_id: id("fixture-model"),
            model_version: id("fixture-release"),
            version_semantics: VersionSemantics::Unverified,
            provider: binding.provider,
            target,
            deployment_revision: None,
            api_contract,
            adapter: binding.adapter,
            capability_revision: id("caps"),
            connection_ref: binding.connection_ref,
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Read the authorized fixture".into(),
            }],
        }],
        tools: vec![],
        output: ModelOutput::Text {},
        max_output_tokens: 128.try_into().unwrap(),
        options: JsonObject::new(),
        limits: ModelResponseLimits {
            max_input_bytes: 32768,
            max_response_bytes: 32768,
            max_delta_bytes: 128,
            max_events: 64,
            max_tool_calls: 0,
        },
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let origin = format!("http://{}/", listener.local_addr()?);
    let v1 = format!("{origin}v1/");
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let mut providers: Vec<(Arc<dyn ModelPort>, ModelRequest)> = vec![];
    macro_rules! add {
        ($connection:expr,$model:path,$api:expr) => {{
            let connection = $connection?;
            let request = request(
                connection.binding(),
                connection.target().clone(),
                ($api)(&connection),
            );
            request.validate()?;
            let port: Arc<dyn ModelPort> = Arc::new(<$model>::new(connection));
            providers.push((port, request));
        }};
    }
    add!(
        wickle_model_openai::OpenAiConnection::new(
            scope.clone(),
            reference("shared"),
            "fixture-key",
            wickle_model_openai::OpenAiOptions {
                base_url: v1.clone(),
                ..Default::default()
            }
        ),
        wickle_model_openai::OpenAiModel,
        |_: &wickle_model_openai::OpenAiConnection| {
            wickle_model_openai::OpenAiConnection::api_contract()
        }
    );
    add!(
        wickle_model_azure_openai::AzureOpenAiConnection::new(
            scope.clone(),
            reference("shared"),
            Arc::new(wickle_model_azure_openai::AzureCredential::ApiKey(
                "fixture-key".into()
            )),
            wickle_model_azure_openai::AzureOpenAiOptions::new(&origin, "fixture-deployment")
        ),
        wickle_model_azure_openai::AzureOpenAiModel,
        |_: &wickle_model_azure_openai::AzureOpenAiConnection| {
            wickle_model_azure_openai::AzureOpenAiConnection::api_contract()
        }
    );
    add!(
        wickle_model_anthropic::AnthropicConnection::new(
            scope.clone(),
            reference("shared"),
            "fixture-key",
            wickle_model_anthropic::AnthropicOptions {
                base_url: origin.clone(),
                ..Default::default()
            }
        ),
        wickle_model_anthropic::AnthropicModel,
        |_: &wickle_model_anthropic::AnthropicConnection| {
            wickle_model_anthropic::AnthropicConnection::api_contract()
        }
    );
    let mut bedrock = wickle_model_bedrock::BedrockOptions::new(
        "us-east-1",
        wickle_model_bedrock::BedrockSelector::Foundation("fixture-model".into()),
    );
    bedrock.endpoint_url = Some(origin.clone());
    bedrock.metadata_url = Some(origin.clone());
    add!(
        wickle_model_bedrock::BedrockConnection::new(
            scope.clone(),
            reference("shared"),
            Arc::new(wickle_model_bedrock::BedrockCredential::Bearer(
                "fixture-key".into()
            )),
            bedrock
        ),
        wickle_model_bedrock::BedrockModel,
        |c: &wickle_model_bedrock::BedrockConnection| c.api_contract()
    );
    add!(
        wickle_model_gemini::GeminiConnection::new(
            scope.clone(),
            reference("shared"),
            "fixture-key",
            wickle_model_gemini::GeminiOptions {
                base_url: origin.clone(),
                api_version: "v1beta".into(),
                ..Default::default()
            }
        ),
        wickle_model_gemini::GeminiModel,
        |c: &wickle_model_gemini::GeminiConnection| c.api_contract()
    );
    let mut vertex = wickle_model_vertex::VertexOptions::new("fixture-project");
    vertex.endpoint_url = Some(origin.clone());
    vertex.metadata_url = Some(origin.clone());
    add!(
        wickle_model_vertex::VertexConnection::new(
            scope.clone(),
            reference("shared"),
            Arc::new(wickle_model_vertex::VertexToken::new(
                "fixture-token",
                None
            )?),
            vertex
        ),
        wickle_model_vertex::VertexModel,
        |c: &wickle_model_vertex::VertexConnection| c.api_contract()
    );
    add!(
        wickle_model_xai::XaiConnection::new(
            scope.clone(),
            reference("shared"),
            "fixture-key",
            wickle_model_xai::XaiOptions {
                base_url: v1,
                ..Default::default()
            }
        ),
        wickle_model_xai::XaiModel,
        |c: &wickle_model_xai::XaiConnection| c.api_contract()
    );
    let dispatcher = RegistryModelDispatcher::new(
        providers
            .iter()
            .map(|(port, _)| ModelDispatcherEntry {
                scope: scope.clone(),
                port: port.clone(),
            })
            .collect(),
    )?;
    for (expected, request) in &providers {
        let port = dispatcher.resolve(&scope, &request.route)?;
        assert!(Arc::ptr_eq(&port, expected));
        let mut foreign = scope.clone();
        foreign.workspace_id = id("foreign");
        assert_eq!(
            dispatcher
                .resolve(&foreign, &request.route)
                .err()
                .unwrap()
                .code,
            ErrorCode::ModelBindingInvalid
        );
        let mut missing = request.route.clone();
        missing.connection_ref.version = id("missing");
        assert_eq!(
            dispatcher.resolve(&scope, &missing).err().unwrap().code,
            ErrorCode::ModelBindingInvalid
        );
        let context = ModelCallContext {
            attempt_id: request.request_id.clone(),
            run_id: id("run"),
            scope: foreign,
            cancellation: Default::default(),
            deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        };
        let result = port.generate(request, &context).next().await.unwrap();
        assert_eq!(result.unwrap_err().code, ErrorCode::AccessDenied);
        let mut wrong = request.clone();
        wrong.route.api_contract.version = id("unsupported");
        let context = ModelCallContext {
            scope: scope.clone(),
            ..context
        };
        let result = port.generate(&wrong, &context).next().await.unwrap()?;
        assert!(matches!(
            result,
            ModelEvent::ResponseError {
                kind: ModelFailureKind::Unsupported,
                ..
            }
        ));
    }
    assert!(matches!(listener.accept(),Err(error) if error.kind()==std::io::ErrorKind::WouldBlock));
    println!(
        "Model adapter conformance: all seven concrete providers share one dispatcher without binding collisions; foreign scopes, missing revisions and incompatible API contracts fail before network I/O (extracted packages, no provider calls)"
    );
    Ok(())
}
