// Real loopback HTTP/SSE against the extracted Anthropic adapter package; no provider call.
use serde_json::json;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use wickle::*;
use wickle_model_anthropic::*;
fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}/", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = vec![];
        let (header_end, length) = loop {
            let mut chunk = [0; 4096];
            let size = socket.read(&mut chunk).await.unwrap();
            assert!(size > 0);
            bytes.extend_from_slice(&chunk[..size]);
            assert!(bytes.len() < 65536);
            if let Some(end) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                let headers = std::str::from_utf8(&bytes[..end])
                    .unwrap()
                    .to_ascii_lowercase();
                let length = headers
                    .lines()
                    .find_map(|s| s.strip_prefix("content-length:"))
                    .unwrap()
                    .trim()
                    .parse::<usize>()
                    .unwrap();
                break (end + 4, length);
            }
        };
        while bytes.len() < header_end + length {
            let mut chunk = [0; 4096];
            let size = socket.read(&mut chunk).await.unwrap();
            assert!(size > 0);
            bytes.extend_from_slice(&chunk[..size]);
        }
        let headers = std::str::from_utf8(&bytes[..header_end])
            .unwrap()
            .to_ascii_lowercase();
        assert!(headers.starts_with("post /v1/messages "));
        assert!(headers.contains("anthropic-workspace-id: wrkspc_fixture"));
        let body =
            parse_json(std::str::from_utf8(&bytes[header_end..header_end + length]).unwrap())
                .unwrap();
        assert_eq!(body["model"], "fixture-model");
        assert_eq!(body["stream"], true);
        assert!(body.get("fallbacks").is_none());
        assert_eq!(body["output_config"]["effort"], "medium");
        assert_eq!(body["output_config"]["format"]["type"], "json_schema");
        assert!(!body.to_string().contains("private-workspace"));
        assert!(!body.to_string().contains("fixture-key"));
        let events = vec![
            json!({"type":"message_start","message":{"id":"message","type":"message","role":"assistant","model":"fixture-model","content":[],"usage":{"input_tokens":12,"output_tokens":1}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signed-fixture"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"{\"answer\":42}"}}),
            json!({"type":"content_block_stop","index":1}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}),
            json!({"type":"message_stop"}),
        ];
        let wire: String = events
            .into_iter()
            .map(|value| format!("data: {value}\n\n"))
            .collect();
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",wire.len()).as_bytes()).await.unwrap();
        for chunk in wire.as_bytes().chunks(5) {
            socket.write_all(chunk).await.unwrap();
            tokio::task::yield_now().await;
        }
        socket.shutdown().await.unwrap();
    });
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("private-workspace"),
        user_id: None,
    };
    let connection = AnthropicConnection::new(
        scope.clone(),
        reference("account"),
        "fixture-key",
        AnthropicOptions {
            base_url: base,
            workspace_id: Some("wrkspc_fixture".into()),
            ..Default::default()
        },
    )?;
    let binding = connection.binding();
    let model = AnthropicModel::new(connection.clone());
    let request = ModelRequest {
        request_id: id("attempt"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference("model"),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id("fixture-model"),
            model_id: id("fixture-model"),
            model_version: id("release"),
            version_semantics: VersionSemantics::Unverified,
            provider: binding.provider,
            target: connection.target().clone(),
            deployment_revision: None,
            api_contract: AnthropicConnection::api_contract(),
            adapter: binding.adapter,
            capability_revision: id("caps"),
            connection_ref: binding.connection_ref,
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Return the fixture answer.".into(),
            }],
        }],
        tools: vec![],
        output: ModelOutput::JsonSchema {
            schema: json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}),
        },
        max_output_tokens: 128.try_into()?,
        options: JsonObject::from([("effort".into(), json!("medium"))]),
        limits: ModelResponseLimits {
            max_input_bytes: 32768,
            max_response_bytes: 32768,
            max_delta_bytes: 128,
            max_events: 64,
            max_tool_calls: 0,
        },
    };
    let context = ModelCallContext {
        attempt_id: request.request_id.clone(),
        run_id: id("run"),
        scope,
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(10),
    };
    let response = collect_model_response(&request, model.generate(&request, &context)).await?;
    assert_eq!(parse_json(&response.text)?, json!({"answer":42}));
    assert_eq!(response.finish, ModelFinish::Stop);
    assert_eq!(
        response.metadata.reported_model_id,
        Some(id("fixture-model"))
    );
    assert!(response.metadata.reported_model_version.is_none());
    assert_eq!(
        response
            .metadata
            .usage
            .as_ref()
            .and_then(|u| u.output_tokens),
        Some(5)
    );
    assert_eq!(
        response.continuation[0].data()["blocks"][0]["signature"],
        "signed-fixture"
    );
    server.await?;
    println!(
        "Anthropic consumer: extracted adapter performs one HTTP/SSE request, preserves workspace identity, native output options, signed thinking and cumulative usage, decodes JSON and reported usage, and excludes Host context from the wire (local fixture, no provider network)"
    );
    Ok(())
}
