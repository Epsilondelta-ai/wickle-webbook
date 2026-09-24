use aws_smithy_types::event_stream::{Header, HeaderValue, Message};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use wickle::*;
use wickle_model_bedrock::*;

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
pub const MODEL: &str = "anthropic.claude-opus-5";
pub fn origin(server: &Server) -> String {
    server.base.trim_end_matches("v1/").into()
}
pub struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: 1735689600000,
            monotonic_ms: 0,
        })
    }
    fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}
pub fn credentials() -> std::sync::Arc<dyn BedrockCredentialProvider> {
    std::sync::Arc::new(BedrockCredential::Aws(AwsCredentials::new(
        "AKIDEXAMPLE",
        "test-secret",
        Some("test-session".into()),
        None,
        "fixture",
    )))
}
pub fn options(server: &Server) -> BedrockOptions {
    let mut options = BedrockOptions::new("us-east-1", BedrockSelector::Foundation(MODEL.into()));
    options.endpoint_url = Some(origin(server));
    options.metadata_url = Some(origin(server));
    options
}
pub fn connection(server: &Server) -> BedrockConnection {
    BedrockConnection::new(
        scope(),
        reference("account"),
        credentials(),
        options(server),
    )
    .unwrap()
    .with_clock(std::sync::Arc::new(FixedClock))
}
pub fn request(connection: &BedrockConnection, model: &str) -> ModelRequest {
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
        options: JsonObject::from([("effort".into(), json!("medium"))]),
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
pub fn events(model: &str, text: &str) -> Vec<Value> {
    vec![
        json!({"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":model,"content":[],"stop_reason":null,"usage":{"input_tokens":25,"output_tokens":1}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signature-fixture"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":text}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":15}}),
        json!({"type":"message_stop"}),
    ]
}

#[path = "../../../../tests/support/model_http.rs"]
mod http;
pub use http::*;

pub fn frame(value: &Value) -> Vec<u8> {
    let payload =
        serde_json::to_vec(&json!({"bytes":STANDARD.encode(serde_json::to_vec(value).unwrap())}))
            .unwrap();
    let message = Message::new(payload)
        .add_header(Header::new(
            ":message-type",
            HeaderValue::String("event".into()),
        ))
        .add_header(Header::new(
            ":event-type",
            HeaderValue::String("chunk".into()),
        ))
        .add_header(Header::new(
            ":content-type",
            HeaderValue::String("application/json".into()),
        ));
    let mut bytes = vec![];
    aws_smithy_eventstream::frame::write_message_to(&message, &mut bytes).unwrap();
    bytes
}
pub fn reply(operation: BedrockOperation, data: &[Value]) -> Reply {
    let mut reply = Reply::sse(data);
    reply.headers = vec![("x-amzn-requestid", "aws-request".into())];
    if operation == BedrockOperation::InvokeStream {
        reply.content_type = "application/vnd.amazon.eventstream";
        reply.body = data.iter().flat_map(frame).collect();
        reply.chunk = 3;
    }
    reply
}
