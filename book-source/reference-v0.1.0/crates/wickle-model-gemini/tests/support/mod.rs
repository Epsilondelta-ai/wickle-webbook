use serde_json::{Value, json};
use wickle::*;
use wickle_model_gemini::*;

pub fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
pub fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
pub fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("hidden-workspace"),
        user_id: None,
    }
}
pub fn connection(server: &Server) -> GeminiConnection {
    GeminiConnection::new(
        scope(),
        reference("account"),
        "fixture-key-not-a-secret",
        GeminiOptions {
            base_url: server.base.trim_end_matches("v1/").into(),
            api_version: "v1beta".into(),
            ..Default::default()
        },
    )
    .unwrap()
}
pub fn request(connection: &GeminiConnection, model: &str) -> ModelRequest {
    let binding = connection.binding();
    ModelRequest {
        request_id: id("attempt"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference("primary"),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id(model),
            model_id: id(model),
            model_version: id("fixture-release"),
            version_semantics: VersionSemantics::Pinned,
            provider: binding.provider,
            target: connection.target().clone(),
            deployment_revision: None,
            api_contract: connection.api_contract(),
            adapter: binding.adapter,
            capability_revision: id("capabilities"),
            connection_ref: binding.connection_ref,
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find the requested figures".into(),
            }],
        }],
        tools: vec![],
        output: ModelOutput::Text {},
        max_output_tokens: 128.try_into().unwrap(),
        options: JsonObject::from([("thinking_level".into(), json!("medium"))]),
        limits: ModelResponseLimits {
            max_input_bytes: 32_768,
            max_response_bytes: 32_768,
            max_delta_bytes: 128,
            max_events: 256,
            max_tool_calls: 4,
        },
    }
}
pub fn context(request: &ModelRequest) -> ModelCallContext {
    ModelCallContext {
        attempt_id: request.request_id.clone(),
        run_id: id("run"),
        scope: scope(),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(5),
    }
}
pub const MODEL: &str = "gemini-3.8-flash";
pub fn events(text: &str) -> Vec<Value> {
    vec![
        json!({"responseId":"response-fixture","modelVersion":"fixture-release","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"private thought","thought":true,"thoughtSignature":"signed-thought"}]}}]}),
        json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":text,"thoughtSignature":"signed-answer"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":20,"candidatesTokenCount":5,"thoughtsTokenCount":7,"totalTokenCount":32}}),
    ]
}
pub fn reply(events: &[Value]) -> Reply {
    let mut reply = Reply::sse(&[]);
    reply.body = events
        .iter()
        .flat_map(|value| format!("data: {value}\n\n").into_bytes())
        .collect();
    reply.chunk = 3;
    reply
}

#[path = "../../../../tests/support/model_http.rs"]
mod http;
pub use http::*;
