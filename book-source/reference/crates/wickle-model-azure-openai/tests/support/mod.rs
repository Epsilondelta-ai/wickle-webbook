use serde_json::{Value, json};
use std::sync::Arc;
use wickle::*;
use wickle_model_azure_openai::*;

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
pub const ACCOUNT: &str = "/subscriptions/sub/resourceGroups/group/providers/Microsoft.CognitiveServices/accounts/account";
pub fn origin(server: &Server) -> String {
    server.base.trim_end_matches("v1/").into()
}
pub fn options(server: &Server) -> AzureOpenAiOptions {
    let mut options = AzureOpenAiOptions::new(origin(server), "finance-deployment");
    options.resource_id = Some(ACCOUNT.into());
    options
}
pub fn connection(server: &Server) -> AzureOpenAiConnection {
    AzureOpenAiConnection::new(
        scope(),
        reference("account"),
        Arc::new(AzureCredential::ApiKey("fixture-key".into())),
        options(server),
    )
    .unwrap()
}
pub fn request(connection: &AzureOpenAiConnection, model: &str) -> ModelRequest {
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
            model_version: id("release"),
            version_semantics: VersionSemantics::MutableDeployment,
            provider: binding.provider,
            target: connection.target().clone(),
            deployment_revision: None,
            api_contract: AzureOpenAiConnection::api_contract(),
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
        options: JsonObject::from([("reasoning_effort".into(), json!("medium"))]),
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
pub fn text_item(text: &str) -> Value {
    json!({"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":text,"annotations":[]}]})
}
pub fn events(model: &str, text: &str) -> Vec<Value> {
    let item = text_item(text);
    vec![
        json!({"type":"response.created","response":{"id":"resp_1","model":model,"status":"in_progress","usage":null}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","status":"in_progress","content":[]}}),
        json!({"type":"response.output_text.delta","output_index":0,"item_id":"msg_1","content_index":0,"delta":text}),
        json!({"type":"response.output_text.done","output_index":0,"item_id":"msg_1","content_index":0,"text":text}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        json!({"type":"response.completed","response":{"id":"resp_1","model":model,"status":"completed","output":[item],"usage":{"input_tokens":11,"output_tokens":7}}}),
    ]
}

#[path = "../../../../tests/support/model_http.rs"]
mod http;
pub use http::*;
