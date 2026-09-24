# 50장 전체 구현과 변경 검사

[강의](../50-openai-schema.md) · [전체 변경 패치](../solutions/50-openai-schema.patch)

기준 `27d6456eabd95776fd128e7fb52a83e5e355384c`. 이 단계에서 바뀐 Rust·manifest·Python 파일의 전체 내용이다. 이전 버전과의 정확한 교체 위치·삭제는 patch를 따른다. 다음 장의 코드와 섞지 않는다.

## `crates/wickle-mcp/tests/stdio.rs`

```rust
//! Real stdio protocol, input ownership, bounds and effect contracts.
#[path = "support/binding.rs"]
mod binding;
mod support;
use binding::bound_arguments;
use serde_json::json;
use std::time::Duration;
use support::*;
use wickle::*;
use wickle_mcp::*;
#[tokio::test]
async fn reviewed_snapshot_keeps_original_names_versions_and_explicit_input_ownership() {
    let dir = Directory::new();
    let client = connect(&dir, "normal", McpLimits::default()).await;
    let snapshot = snapshot(&client).await;
    assert_eq!(
        snapshot.tool_names().collect::<Vec<_>>(),
        vec!["db.query", "db.write"]
    );
    assert_eq!(
        snapshot.raw_tool("db.query").unwrap()["_meta"]["version"],
        "1"
    );
    assert_eq!(snapshot.server_info()["version"], "1");
    let text = serde_json::to_string(&snapshot).unwrap();
    let restored =
        McpSnapshot::restore(&text, &scope(), &reference("account"), &snapshot.digest()).unwrap();
    assert_eq!(restored.digest(), snapshot.digest());
    let mut foreign = scope();
    foreign.workspace_id = id("foreign");
    assert!(
        McpSnapshot::restore(&text, &foreign, &reference("account"), &snapshot.digest()).is_err()
    );
    let compiled = compiled(&snapshot, "db.query", ToolSideEffect::ReadOnly);
    assert!(
        compiled.model_input_schema()["properties"]
            .get("workspace_id")
            .is_none()
    );
    assert!(
        compiled.model_input_schema()["properties"]
            .get("query")
            .is_some()
    );
    assert!(compiled.validate_model_inputs(&args()).is_err());
    let bound = bound_arguments(&compiled).await.unwrap();
    let executor = client.bind_tool(&snapshot, "db.query", compiled).unwrap();
    let result = executor.execute(&bound, &context()).await.unwrap();
    assert_eq!(result.effect, ToolEffect::NotApplied);
    assert!(
        matches!(result.outcome,ToolExecutionOutcome::Succeeded{value} if value==json!({"answer":42}))
    );
    let records = records(&dir);
    let first = &records[0];
    assert!(first["home"].is_null());
    assert_eq!(first["allowed"], "yes");
    let call = records.iter().find(|v| v.get("call").is_some()).unwrap();
    assert_eq!(call["call"], "db.query");
    assert_eq!(call["args"]["workspace_id"], WORKSPACE);
    assert_eq!(call["args"].as_object().unwrap().len(), 3);
    let pid = latest_pid(&dir);
    close(&client).await;
    close(&client).await;
    assert!(!process_alive(pid));
    assert!(
        client
            .discover(
                &scope(),
                &Default::default(),
                tokio::time::Instant::now() + Duration::from_secs(1)
            )
            .await
            .is_err()
    );
}
#[tokio::test]
async fn selected_descriptor_drift_blocks_dispatch_and_new_tools_do_not_activate() {
    for mode in ["drift", "added"] {
        let dir = Directory::new();
        let client = connect(&dir, mode, McpLimits::default()).await;
        let snapshot = snapshot(&client).await;
        let compiled = compiled(&snapshot, "db.query", ToolSideEffect::ReadOnly);
        let executor = client
            .bind_tool(&snapshot, "db.query", compiled.clone())
            .unwrap();
        let result = executor.execute(&args(), &context()).await.unwrap();
        if mode == "drift" {
            assert!(
                matches!(result.outcome,ToolExecutionOutcome::Failed{code} if code==id("mcp.descriptor_drift"))
            );
            assert_eq!(result.effect, ToolEffect::NotApplied);
            assert_eq!(call_count(&dir), 0);
        } else {
            assert!(matches!(
                result.outcome,
                ToolExecutionOutcome::Succeeded { .. }
            ));
            assert!(client.bind_tool(&snapshot, "db.new", compiled).is_err());
            assert_eq!(call_count(&dir), 1);
        }
        close(&client).await;
    }
}
#[tokio::test]
async fn completed_or_lost_writes_are_unknown_without_host_attestation_and_never_retried() {
    for mode in ["normal", "exit_write", "hang_write"] {
        let dir = Directory::new();
        let client = connect(&dir, mode, McpLimits::default()).await;
        let snapshot = snapshot(&client).await;
        let compiled = compiled(&snapshot, "db.write", ToolSideEffect::Write);
        let executor = client.bind_tool(&snapshot, "db.write", compiled).unwrap();
        let mut context = context();
        context.deadline = tokio::time::Instant::now() + Duration::from_millis(300);
        let result = executor.execute(&args(), &context).await.unwrap();
        assert_eq!(result.effect, ToolEffect::Unknown);
        assert!(result.receipt.is_none());
        assert_eq!(call_count(&dir), 1);
        if mode != "normal" {
            assert_eq!(
                std::fs::read_to_string(dir.0.join("calls.jsonl.effect")).unwrap(),
                "applied\n"
            );
            let result = executor.execute(&args(), &context).await.unwrap();
            assert!(matches!(
                result.outcome,
                ToolExecutionOutcome::Failed { .. }
            ));
            assert_eq!(call_count(&dir), 1);
        }
        close(&client).await;
    }
}
#[tokio::test]
async fn metadata_change_during_read_only_call_does_not_claim_no_effect() {
    let dir = Directory::new();
    let client = connect(&dir, "notify", McpLimits::default()).await;
    let snapshot = snapshot(&client).await;
    let executor = client
        .bind_tool(
            &snapshot,
            "db.query",
            compiled(&snapshot, "db.query", ToolSideEffect::ReadOnly),
        )
        .unwrap();
    let result = executor.execute(&args(), &context()).await.unwrap();
    assert_eq!(result.effect, ToolEffect::Unknown);
    assert!(
        matches!(result.outcome,ToolExecutionOutcome::Failed{code} if code==id("mcp.descriptor_changed_during_call"))
    );
    close(&client).await;
}
#[tokio::test]
async fn protocol_size_duplicate_json_and_remote_errors_are_not_successes() {
    for mode in ["duplicate", "oversized", "tool_error"] {
        let dir = Directory::new();
        let client = connect(
            &dir,
            mode,
            McpLimits {
                max_frame_bytes: 4096,
                ..Default::default()
            },
        )
        .await;
        let snapshot = snapshot(&client).await;
        let executor = client
            .bind_tool(
                &snapshot,
                "db.query",
                compiled(&snapshot, "db.query", ToolSideEffect::ReadOnly),
            )
            .unwrap();
        let result = executor.execute(&args(), &context()).await.unwrap();
        assert!(!format!("{result:?}").contains("private remote error"));
        assert!(
            matches!(result.outcome,ToolExecutionOutcome::Failed{code} if code==id(if mode=="tool_error"{"mcp.remote_error"}else{"mcp.call_failed"}))
        );
        assert_eq!(result.effect, ToolEffect::NotApplied);
        close(&client).await;
    }
}
#[tokio::test]
async fn initialization_version_timeout_and_repeated_cursors_fail_with_cleanup() {
    for mode in ["wrong_version", "hang_init"] {
        let dir = Directory::new();
        let result = McpClient::connect(
            scope(),
            reference("account"),
            command(&dir, mode),
            McpLimits::default(),
            &Default::default(),
            tokio::time::Instant::now() + Duration::from_millis(300),
        )
        .await;
        assert!(result.is_err());
    }
    let dir = Directory::new();
    let client = connect(&dir, "cursor", McpLimits::default()).await;
    assert!(
        client
            .discover(
                &scope(),
                &Default::default(),
                tokio::time::Instant::now() + Duration::from_secs(1)
            )
            .await
            .is_err()
    );
    close(&client).await;
}

#[tokio::test]
async fn server_sampling_requests_do_not_gain_model_execution_capability() {
    let dir = Directory::new();
    let client = connect(&dir, "callback", McpLimits::default()).await;
    let _snapshot = snapshot(&client).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let values = std::fs::read_to_string(dir.0.join("calls.jsonl")).unwrap_or_default();
        if let Some(response) = values
            .lines()
            .filter_map(|v| parse_json(v).ok())
            .find_map(|v| v.get("client_response").cloned())
        {
            assert_eq!(response["id"], "server-sample");
            assert_eq!(response["error"]["code"], -32601);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "sampling request was not rejected"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let init = records(&dir)
        .into_iter()
        .find(|v| v["method"] == "initialize")
        .unwrap();
    assert!(init["params"]["capabilities"].get("sampling").is_none());
    assert_eq!(call_count(&dir), 0);
    close(&client).await;
}

#[tokio::test]
async fn duplicate_response_cannot_replace_reviewed_raw_metadata() {
    let dir = Directory::new();
    let client = connect(&dir, "duplicate_list", McpLimits::default()).await;
    let snapshot = snapshot(&client).await;
    assert_eq!(
        snapshot.raw_tool("db.query").unwrap()["_meta"]["version"],
        "1"
    );
    close(&client).await;
}

#[tokio::test]
async fn abandoning_execute_terminates_in_flight_write_and_prevents_reuse() {
    let dir = Directory::new();
    let client = connect(&dir, "hang_write", McpLimits::default()).await;
    let snapshot = snapshot(&client).await;
    let executor = client
        .bind_tool(
            &snapshot,
            "db.write",
            compiled(&snapshot, "db.write", ToolSideEffect::Write),
        )
        .unwrap();
    let inputs = args();
    let context = context();
    {
        let execution = executor.execute(&inputs, &context);
        tokio::pin!(execution);
        tokio::select! {
            result = &mut execution => panic!("hanging write completed: {result:?}"),
            _ = async {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
                // File creation precedes the write; wait for the effect acknowledgement
                // before dropping the future and terminating the child.
                while std::fs::read_to_string(dir.0.join("calls.jsonl.effect")).ok().as_deref() != Some("applied\n") {
                    assert!(tokio::time::Instant::now() < deadline);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            } => {}
        }
        // Drop the future without cancelling its token, like an outer timeout.
    }
    assert!(!context.cancellation.is_cancelled());
    let pid = latest_pid(&dir);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while process_alive(pid) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "abandoned operation retained its child"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let result = executor.execute(&inputs, &context).await.unwrap();
    assert!(matches!(
        result.outcome,
        ToolExecutionOutcome::Failed { .. }
    ));
    assert_eq!(call_count(&dir), 1);
    assert_eq!(
        std::fs::read_to_string(dir.0.join("calls.jsonl.effect")).unwrap(),
        "applied\n"
    );
    close(&client).await;
}

#[tokio::test]
async fn bounded_discovery_and_notification_flood_fail_closed() {
    for (mode, limits) in [
        (
            "normal",
            McpLimits {
                max_tools: 1,
                ..Default::default()
            },
        ),
        (
            "pages",
            McpLimits {
                max_pages: 2,
                ..Default::default()
            },
        ),
    ] {
        let dir = Directory::new();
        let client = connect(&dir, mode, limits).await;
        assert!(
            client
                .discover(&scope(), &Default::default(), context().deadline)
                .await
                .is_err()
        );
        close(&client).await;
        assert!(!process_alive(latest_pid(&dir)));
    }
    let dir = Directory::new();
    let client = connect(
        &dir,
        "flood",
        McpLimits {
            max_messages: 8,
            ..Default::default()
        },
    )
    .await;
    let snapshot = snapshot(&client).await;
    let executor = client
        .bind_tool(
            &snapshot,
            "db.query",
            compiled(&snapshot, "db.query", ToolSideEffect::ReadOnly),
        )
        .unwrap();
    assert!(matches!(
        executor.execute(&args(), &context()).await.unwrap().outcome,
        ToolExecutionOutcome::Failed { .. }
    ));
    close(&client).await;
}

#[tokio::test]
async fn content_projection_and_output_limits_are_enforced() {
    for mode in ["text_metadata", "image", "missing_structured", "normal"] {
        let dir = Directory::new();
        let client = connect(&dir, mode, McpLimits::default()).await;
        let snapshot = snapshot(&client).await;
        let mut approval = McpToolApproval::new(
            id("search"),
            id("search"),
            vec!["query".into(), "limit".into()],
        );
        approval.side_effect = ToolSideEffect::ReadOnly;
        if mode == "normal" {
            approval.max_output_bytes = 1.try_into().unwrap();
        }
        let tool = SchemaCompiler::new()
            .compile(
                snapshot.descriptor("db.query", approval).unwrap(),
                &registry(),
            )
            .unwrap();
        let executor = client.bind_tool(&snapshot, "db.query", tool).unwrap();
        let result = executor.execute(&args(), &context()).await.unwrap();
        if mode == "text_metadata" {
            assert!(
                matches!(result.outcome, ToolExecutionOutcome::Succeeded { value } if value == json!({"content":[{"type":"text","text":"found"}]}))
            );
        } else {
            let expected = match mode {
                "image" => "mcp.unsupported_content",
                "missing_structured" => "mcp.missing_structured_output",
                _ => "mcp.output_limit",
            };
            assert!(
                matches!(result.outcome, ToolExecutionOutcome::Failed { code } if code == id(expected))
            );
        }
        close(&client).await;
    }
}

#[tokio::test]
async fn multipage_snapshot_keeps_schema_and_oversized_input_never_dispatches() {
    let dir = Directory::new();
    let client = connect(
        &dir,
        "multipage",
        McpLimits {
            max_frame_bytes: 4096,
            ..Default::default()
        },
    )
    .await;
    let snapshot = snapshot(&client).await;
    assert_eq!(
        snapshot.tool_names().collect::<Vec<_>>(),
        ["db.query", "db.write"]
    );
    let restored = McpSnapshot::restore(
        &serde_json::to_string(&snapshot).unwrap(),
        &scope(),
        &reference("account"),
        &snapshot.digest(),
    )
    .unwrap();
    let compiled = compiled(&restored, "db.query", ToolSideEffect::ReadOnly);
    assert_eq!(
        compiled.descriptor().input_schema["properties"]["workspace_id"]["format"],
        "uuid"
    );
    let mut inputs = args();
    inputs.insert("workspace_id".into(), json!("invalid-uuid"));
    assert!(compiled.validate_execution_inputs(&inputs).is_err());
    let executor = client.bind_tool(&snapshot, "db.query", compiled).unwrap();
    inputs = args();
    inputs.insert("query".into(), json!("x".repeat(5000)));
    let result = executor.execute(&inputs, &context()).await.unwrap();
    assert_eq!(result.effect, ToolEffect::NotApplied);
    assert!(
        matches!(result.outcome, ToolExecutionOutcome::Failed { code } if code == id("mcp.input_limit"))
    );
    assert_eq!(call_count(&dir), 0);
    close(&client).await;
}
```

## `crates/wickle-model-openai/src/model.rs`

```rust
use crate::{OpenAiConnection, error};
use futures_util::stream;
use reqwest::Response;
use serde_json::Value;
use std::collections::VecDeque;
use wickle::*;
use wickle_model_responses::{ResponsesDecoder, SseDecoder, encode_request};

/// One OpenAI Responses POST per invocation, streamed into Wickle model events.
/// The adapter does not retry, run Tool handlers, or load environment variables.
#[derive(Debug, Clone)]
pub struct OpenAiModel {
    connection: OpenAiConnection,
}
impl OpenAiModel {
    /// Bind an already configured connection without making a network request.
    pub fn new(connection: OpenAiConnection) -> Self {
        Self { connection }
    }
}
impl ModelPort for OpenAiModel {
    fn tool_schema_compiler(&self) -> std::sync::Arc<dyn ProviderToolSchemaCompiler> {
        std::sync::Arc::new(wickle_model_responses::ResponsesToolSchemaCompiler)
    }
    fn binding(&self) -> ModelPortBinding {
        self.connection.binding()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let state = State {
            connection: &self.connection,
            request,
            context,
            response: None,
            decoder: ResponsesDecoder::new(request, None),
            framing: SseDecoder::new(
                self.connection.0.options.max_transport_bytes,
                self.connection.0.options.max_event_bytes,
                self.connection.0.options.max_protocol_events,
            ),
            queue: VecDeque::new(),
            started: false,
            finished: false,
        };
        Box::pin(stream::unfold(state, |mut state| async move {
            loop {
                if !state.finished && state.context.cancellation.is_cancelled() {
                    state.queue.clear();
                    state.fail(error(ErrorCode::Cancelled, "stream"));
                }
                if !state.finished && tokio::time::Instant::now() >= state.context.deadline {
                    state.queue.clear();
                    state.fail(error(ErrorCode::DeadlineExceeded, "stream"));
                }
                if let Some(event) = state.queue.pop_front() {
                    return Some((event, state));
                }
                if state.finished {
                    return None;
                }
                if !state.started {
                    state.started = true;
                    if let Err(failure) = state.start().await {
                        state.fail(failure);
                    }
                    continue;
                }
                let result = {
                    let response = state
                        .response
                        .as_mut()
                        .expect("started response or finished state");
                    tokio::select! { biased;
                        _ = state.context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "stream")),
                        _ = tokio::time::sleep_until(state.context.deadline) => Err(error(ErrorCode::DeadlineExceeded, "stream")),
                        chunk = response.chunk() => chunk.map_err(transport_error),
                    }
                };
                match result {
                    Ok(Some(bytes)) => match state.framing.push(&bytes) {
                        Ok(events) => {
                            for event in events {
                                match state.decoder.event(event) {
                                    Ok(events) => state.queue.extend(events.into_iter().map(Ok)),
                                    Err(error) => {
                                        state.fail(error);
                                        break;
                                    }
                                }
                            }
                        }
                        Err(error) => state.fail(error),
                    },
                    Ok(None) => match state.framing.finish().and_then(|_| state.decoder.finish()) {
                        Ok(event) => {
                            state.queue.push_back(Ok(event));
                            state.finished = true;
                            state.response = None;
                        }
                        Err(error) => state.fail(error),
                    },
                    Err(error) => state.fail(error),
                }
            }
        }))
    }
}
struct State<'a> {
    connection: &'a OpenAiConnection,
    request: &'a ModelRequest,
    context: &'a ModelCallContext,
    response: Option<Response>,
    decoder: ResponsesDecoder<'a>,
    framing: SseDecoder,
    queue: VecDeque<Result<ModelEvent, ContractError>>,
    started: bool,
    finished: bool,
}
impl State<'_> {
    async fn start(&mut self) -> Result<(), ContractError> {
        self.connection
            .validate(&self.request.route, &self.context.scope)?;
        if self.context.attempt_id != self.request.request_id {
            return Err(error(ErrorCode::RequestConflict, "attempt"));
        }
        self.request.validate()?;
        let value = encode_request(self.request)?;
        let body =
            serde_json::to_vec(&value).map_err(|_| error(ErrorCode::InvalidJson, "request"))?;
        if body.len() > self.request.limits.max_input_bytes {
            return Err(error(ErrorCode::ModelCapabilityUnsupported, "request_size"));
        }
        let url = self
            .connection
            .0
            .base
            .join("responses")
            .map_err(|_| error(ErrorCode::InvalidConfiguration, "responses_url"))?;
        let operation = self
            .connection
            .0
            .client
            .post(url)
            .header("accept", "text/event-stream")
            .header("accept-encoding", "identity")
            .body(body)
            .send();
        let mut response = tokio::select! { biased;
            _ = self.context.cancellation.cancelled() => return Err(error(ErrorCode::Cancelled, "request")),
            _ = tokio::time::sleep_until(self.context.deadline) => return Err(error(ErrorCode::DeadlineExceeded, "request")),
            result = operation => result.map_err(transport_error)?,
        };
        self.decoder.metadata.provider_request_id = response
            .headers()
            .get("x-request-id")
            .map(|value| {
                value
                    .to_str()
                    .map_err(|_| error(ErrorCode::InvalidContract, "request_id"))
                    .and_then(Id::new)
            })
            .transpose()?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let mut kind = match status {
                401 | 403 => ModelFailureKind::Authentication,
                404 => ModelFailureKind::Unavailable,
                408 | 504 => ModelFailureKind::Timeout,
                429 => ModelFailureKind::RateLimited,
                500..=599 => ModelFailureKind::Transport,
                _ => ModelFailureKind::Unsupported,
            };
            if status == 400 {
                let mut bytes = vec![];
                loop {
                    let chunk = tokio::select! { biased;
                        _ = self.context.cancellation.cancelled() => return Err(error(ErrorCode::Cancelled, "error_body")),
                        _ = tokio::time::sleep_until(self.context.deadline) => return Err(error(ErrorCode::DeadlineExceeded, "error_body")),
                        chunk = response.chunk() => chunk.map_err(transport_error)?,
                    };
                    let Some(chunk) = chunk else {
                        break;
                    };
                    if bytes.len().saturating_add(chunk.len())
                        > 65_536.min(self.connection.0.options.max_transport_bytes)
                    {
                        break;
                    }
                    bytes.extend_from_slice(&chunk);
                }
                if let Ok(value) = std::str::from_utf8(&bytes)
                    .map_err(|_| ())
                    .and_then(|text| parse_json(text).map_err(|_| ()))
                {
                    if value.pointer("/error/code").and_then(Value::as_str)
                        == Some("context_length_exceeded")
                    {
                        kind = ModelFailureKind::ContextOverflow;
                    }
                }
            }
            self.queue.push_back(Ok(ModelEvent::ResponseError {
                kind,
                metadata: self.decoder.metadata.clone(),
            }));
            self.finished = true;
            return Ok(());
        }
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim);
        if !content_type.is_some_and(|value| value.eq_ignore_ascii_case("text/event-stream")) {
            return Err(error(ErrorCode::InvalidContract, "content_type"));
        }
        if response
            .content_length()
            .is_some_and(|bytes| bytes > self.connection.0.options.max_transport_bytes as u64)
        {
            return Err(error(ErrorCode::InvalidContract, "response_size"));
        }
        self.response = Some(response);
        Ok(())
    }
    fn fail(&mut self, failure: ContractError) {
        self.finished = true;
        self.response = None;
        if matches!(
            failure.code,
            ErrorCode::Cancelled | ErrorCode::AccessDenied | ErrorCode::RequestConflict
        ) {
            self.queue.push_back(Err(failure));
            return;
        }
        let kind = match failure.code {
            ErrorCode::DeadlineExceeded => ModelFailureKind::Timeout,
            ErrorCode::ModelUnavailable => ModelFailureKind::Transport,
            ErrorCode::ModelOptionUnsupported
            | ErrorCode::ModelCapabilityUnsupported
            | ErrorCode::CapabilityUnsupported
            | ErrorCode::ModelBindingInvalid
            | ErrorCode::InvalidConfiguration => ModelFailureKind::Unsupported,
            _ => ModelFailureKind::Protocol,
        };
        self.queue.push_back(Ok(ModelEvent::ResponseError {
            kind,
            metadata: self.decoder.metadata.clone(),
        }));
    }
}
fn transport_error(error_value: reqwest::Error) -> ContractError {
    error(
        if error_value.is_timeout() {
            ErrorCode::DeadlineExceeded
        } else {
            ErrorCode::ModelUnavailable
        },
        "transport",
    )
}
```

## `crates/wickle-model-openai/tests/agent_contract.rs`

```rust
//! Actual HTTP/SSE through the core: provider projection, repair and system binding.
#[path = "../../wickle/tests/support/agent.rs"]
#[allow(dead_code)]
mod core_host;
#[allow(dead_code)]
mod support;
use core_host::{completed, id, reference, scope};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use wickle::*;
use wickle_model_openai::*;
const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";

struct Catalog(core_host::Catalog);
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        scope: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            let mut metadata = self.0.resolve(reference, scope).await?;
            if reference.kind == ComponentKind::Tool {
                metadata.model_name = Some(reference.id.clone());
            }
            if let Some(version) = &reference.version {
                metadata.reference.version = Some(version.clone());
            }
            Ok(metadata)
        })
    }
}
struct Inspector;
impl ModelRouteInspector for Inspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        _: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: ModelRouteAvailability::Available,
                model_id: Some(route.model_id.clone()),
                model_version: Some(route.model_version.clone()),
                deployment_revision: None,
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id("local-http-fixture"),
            })
        })
    }
}
struct Capture(Mutex<Vec<JsonObject>>);
impl ToolExecutor for Capture {
    fn execute<'a>(
        &'a self,
        call: &'a JsonObject,
        _: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.0.lock().unwrap().push(call.clone());
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: json!("observed"),
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}
fn call_events(index: usize, arguments: &str) -> Vec<Value> {
    let response = format!("resp_{index}");
    let item = format!("fc_{index}");
    let call = format!("call_{index}");
    let output = json!({"id":item,"type":"function_call","call_id":call,"name":"lookup","arguments":arguments,"status":"completed"});
    vec![
        json!({"type":"response.created","response":{"id":response,"model":"model","status":"in_progress"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":item,"type":"function_call","call_id":call,"name":"lookup","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":0,"item_id":item,"delta":arguments}),
        json!({"type":"response.function_call_arguments.done","output_index":0,"item_id":item,"name":"lookup","arguments":arguments}),
        json!({"type":"response.output_item.done","output_index":0,"item":output}),
        json!({"type":"response.completed","response":{"id":response,"model":"model","status":"completed","output":[output]}}),
    ]
}
#[tokio::test]
async fn unsupported_constraints_and_invalid_json_text_repair_before_system_binding_or_execution() {
    let invalid_constraint = json!({"query":"latest","limit":{"present":true,"value":7},"note":{"present":true,"value":null},"filter":"[ {\"category\": \"finance\"} ]"}).to_string();
    let invalid_json = json!({"query":"latest","limit":{"present":true,"value":9},"note":{"present":true,"value":null},"filter":"[not json"}).to_string();
    let valid = json!({"query":"latest","limit":{"present":true,"value":9},"note":{"present":true,"value":null},"filter":"[ {\"category\": \"finance\"} ]"}).to_string();
    let malformed = "{not json";
    let rounded = r#"{"query":"latest","limit":0.12345678901234567890123456789}"#;
    let server = support::Server::new(vec![
        support::Reply::sse(&call_events(0, malformed)),
        support::Reply::sse(&call_events(4, rounded)),
        support::Reply::sse(&call_events(1, &invalid_constraint)),
        support::Reply::sse(&call_events(2, &invalid_json)),
        support::Reply::sse(&call_events(3, &valid)),
        support::Reply::sse(&support::events("model", "complete")),
    ])
    .await;
    let connection = OpenAiConnection::new(
        scope(),
        reference("account"),
        "fixture-key-not-a-secret",
        OpenAiOptions {
            base_url: server.base.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    let fixture = core_host::Fixture::new(core_host::Response::Text, false);
    let mut catalog = fixture.router.snapshot.catalog().clone();
    catalog.models[0].provider = id("openai");
    catalog.models[0].model_id = id("model");
    catalog.models[0]
        .capabilities
        .features
        .insert(id("tool_calling"));
    catalog.bindings[0].model = catalog.models[0].reference();
    catalog.bindings[0].requested_model = id("model");
    catalog.bindings[0].adapter = connection.binding().adapter;
    catalog.bindings[0].connection_ref = connection.binding().connection_ref;
    catalog.bindings[0].api_contract = OpenAiConnection::api_contract();
    catalog.bindings[0].target = connection.target().clone();
    catalog.bindings[0].target_schema = json!({"type":"object","properties":{"base_url":{"type":"string"}},"required":["base_url"],"additionalProperties":false});
    catalog.bindings[0].capabilities = catalog.models[0].capabilities.clone();
    catalog.bindings[0].evidence[0].binding_digest = catalog.bindings[0]
        .contract_digest(&catalog.models[0])
        .unwrap();
    let snapshot = RoutingSnapshot::new(catalog, fixture.router.snapshot.policy().clone()).unwrap();
    let mut bindings = fixture.bindings();
    bindings.profile_resolver = Arc::new(Catalog(core_host::Catalog::default()));
    bindings.router = Arc::new(core_host::Router {
        snapshot,
        queries: AtomicUsize::new(0),
        snapshots: AtomicUsize::new(0),
    });
    bindings.model_exchange = Arc::new(
        ModelExchange::new(
            Arc::new(OpenAiModel::new(connection)),
            bindings.policy.clone(),
        )
        .with_route_inspector(Arc::new(Inspector), Duration::from_secs(1))
        .unwrap(),
    );
    bindings.system_inputs = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    let tool = SchemaCompiler::new().compile(ToolDescriptor {
        tool: reference("lookup"), name: id("lookup"), description: "Read scoped data".into(),
        input_schema: json!({"type":"object","properties":{
            "query":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":10,"default":7},"note":{"type":["string","null"]},
            "filter":{"type":"object","properties":{"category":{"type":"string"},"term":{"type":"string"}},"required":["category"],"additionalProperties":false},
            "workspace_id":{"type":"string","format":"uuid"}
        },"required":["query","workspace_id"],"additionalProperties":false,"if":{"properties":{"query":{"const":"latest"}}},"then":{"properties":{"limit":{"minimum":8}}}}),
        agent_parameters: vec!["query".into(),"limit".into(),"note".into(),"filter".into()], system_bindings: None,
        output_schema: json!({"type":"string"}), side_effect: ToolSideEffect::ReadOnly, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false, max_output_bytes: 4096.try_into().unwrap(),
    }, &bindings.system_inputs).unwrap();
    let capture = Arc::new(Capture(Mutex::new(vec![])));
    bindings.tools = Some(Arc::new(
        ToolRegistry::new(
            scope(),
            vec![ToolRegistration {
                compiled: tool,
                executor: capture.clone(),
            }],
        )
        .unwrap(),
    ));
    let mut profile = core_host::profile();
    profile.limits.max_model_calls = 7.try_into().unwrap();
    profile.limits.max_tool_attempts = 2;
    profile.limits.max_repair_attempts = 4;
    profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
        tool_id: id("lookup"),
        version: id("1"),
        bindings: None,
        config: None,
    }));
    let agent = create_agent(profile, bindings).unwrap();
    let mut context = core_host::context();
    context.data.system_inputs = Some(SystemInputs::new(
        [("workspace_id".into(), json!(WORKSPACE))].into(),
    ));
    let handle = completed(
        agent
            .start(core_host::request("contract"), context.clone())
            .await
            .unwrap(),
    );
    let outcome = completed(
        tokio::time::timeout(Duration::from_secs(10), handle.outcome(&context))
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        outcome.result.status(),
        RunStatus::Succeeded,
        "{:?}",
        outcome.result
    );
    assert_eq!(outcome.usage.repair_attempts, 4);
    assert_eq!(outcome.usage.tool_attempts, 1);
    assert_eq!(
        capture.0.lock().unwrap().as_slice(),
        &[JsonObject::from([
            ("query".into(), json!("latest")),
            ("limit".into(), json!(9)),
            ("note".into(), Value::Null),
            ("filter".into(), json!({"category":"finance"})),
            ("workspace_id".into(), json!(WORKSPACE))
        ])]
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 6);
    for request in requests.iter() {
        assert_eq!(request.body["tools"][0]["strict"], true);
        assert!(!request.body.to_string().contains(WORKSPACE));
    }
    let replayed: Vec<_> = requests[5].body["input"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["type"] == "function_call")
        .map(|item| item["arguments"].as_str().unwrap())
        .collect();
    assert_eq!(
        replayed,
        vec![
            malformed,
            rounded,
            invalid_constraint.as_str(),
            invalid_json.as_str(),
            valid.as_str()
        ]
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
}
```

## `crates/wickle-model-openai/tests/responses.rs`

```rust
//! Real HTTP/SSE boundaries, scoped credentials, version evidence, and lossless replay.
#[allow(dead_code)]
mod support;
use futures_util::StreamExt;
use serde_json::{Value, json};
use support::*;
use wickle::*;
use wickle_model_openai::*;

#[tokio::test]
async fn streaming_versions_options_and_reported_usage_do_not_leak_host_context() {
    let server = Server::new(vec![
        Reply::sse(&events("model-first", "안녕")),
        Reply::sse(&events("model-second", "second")),
    ])
    .await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    for (name, effort, text) in [
        ("model-first", "low", "안녕"),
        ("model-second", "high", "second"),
    ] {
        let mut request = request(&connection, name);
        request.route.model_version = id(name);
        request
            .options
            .insert("reasoning_effort".into(), json!(effort));
        let context = context(&request);
        let result = collect_model_response(&request, model.generate(&request, &context))
            .await
            .unwrap();
        assert_eq!(result.text, text);
        assert_eq!(result.finish, ModelFinish::Stop);
        assert_eq!(result.metadata.reported_model_id, Some(id(name)));
        assert!(result.metadata.reported_model_version.is_none());
        assert_eq!(
            result.metadata.usage,
            Some(ModelUsage {
                measurement: UsageMeasurement::Reported,
                input_tokens: Some(11),
                output_tokens: Some(7)
            })
        );
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (index, effort) in ["low", "high"].into_iter().enumerate() {
        assert_eq!(requests[index].path, "/v1/responses");
        assert_eq!(requests[index].method, "POST");
        assert_eq!(requests[index].body["reasoning"]["effort"], effort);
        assert_eq!(requests[index].body["stream"], true);
        assert_eq!(requests[index].body["store"], false);
        assert_eq!(requests[index].body["truncation"], "disabled");
        let encoded = requests[index].body.to_string();
        assert!(!encoded.contains("hidden-workspace"));
        assert!(!encoded.contains("fixture-key-not-a-secret"));
        assert!(
            requests[index]
                .headers
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-key-not-a-secret")
        );
    }
    assert!(!format!("{connection:?} {model:?}").contains("fixture-key-not-a-secret"));
}

#[tokio::test]
async fn structured_output_preserves_schema_and_rejects_unsupported_contracts_before_http() {
    let schema = json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false});
    let server = Server::new(vec![Reply::sse(&events("model", r#"{"answer":42}"#))]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    request.output = ModelOutput::JsonSchema {
        schema: schema.clone(),
    };
    request.options.insert("verbosity".into(), json!("low"));
    let result = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(parse_json(&result.text).unwrap(), json!({"answer":42}));
    let body = server.requests.lock().unwrap()[0].body.clone();
    assert_eq!(body["text"]["format"]["schema"], schema);
    assert_eq!(body["text"]["format"]["strict"], true);
    assert_eq!(body["text"]["verbosity"], "low");
    request.output = ModelOutput::JsonSchema {
        schema: json!({"type":"object","properties":{"optional":{"type":"string"}},"additionalProperties":false}),
    };
    let failure = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap_err();
    assert_eq!(failure.kind, ModelFailureKind::Unsupported);
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

fn function_events() -> Vec<Value> {
    let reasoning = json!({"id":"rs_1","type":"reasoning","summary":[],"encrypted_content":"ciphertext-fixture"});
    let call = json!({"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"query\":\"figures\"}","status":"completed"});
    vec![
        json!({"type":"response.created","response":{"id":"resp_1","model":"model","status":"in_progress"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"rs_1","type":"reasoning","summary":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":reasoning}),
        json!({"type":"response.output_item.added","output_index":1,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":"fc_1","delta":"{\"query\":"}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":"fc_1","delta":"\"figures\"}"}),
        json!({"type":"response.function_call_arguments.done","output_index":1,"item_id":"fc_1","arguments":"{\"query\":\"figures\"}"}),
        json!({"type":"response.output_item.done","output_index":1,"item":call}),
        json!({"type":"response.completed","response":{"id":"resp_1","model":"model","status":"completed","output":[reasoning,call]}}),
    ]
}
fn with_tool(request: &mut ModelRequest) {
    request.tools = vec![ModelTool {
        name: id("lookup"),
        description: "Read figures".into(),
        model_input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer"}},"required":["query"],"additionalProperties":false}),
    }];
}
#[tokio::test]
async fn function_fragments_and_full_reasoning_replay_preserve_original_order_once() {
    let server = Server::new(vec![
        Reply::sse(&function_events()),
        Reply::sse(&events("model", "received result")),
    ])
    .await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    with_tool(&mut request);
    let first = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(first.finish, ModelFinish::ToolCalls);
    assert_eq!(first.tool_calls.len(), 1);
    assert!(first.metadata.usage.is_none());
    assert_eq!(first.continuation.len(), 1);
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![
            ModelContent::ToolCall {
                provider_call_id: id("call_1"),
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
            provider_call_id: id("call_1"),
            content: json!({"result":73}),
        }],
    });
    request.request_id = id("second-attempt");
    let result = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(result.text, "received result");
    {
        let calls = server.requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        let input = calls[1].body["input"].as_array().unwrap();
        assert_eq!(
            input
                .iter()
                .filter(|item| item["type"] == "function_call")
                .count(),
            1
        );
        assert_eq!(input[1]["type"], "reasoning");
        assert_eq!(input[1]["encrypted_content"], "ciphertext-fixture");
        assert_eq!(input[2]["call_id"], "call_1");
        assert_eq!(input[3]["type"], "function_call_output");
        assert_eq!(
            parse_json(input[3]["output"].as_str().unwrap()).unwrap(),
            json!({"result":73})
        );
        assert_eq!(calls[0].body["tools"][0]["strict"], false);
        assert_eq!(
            calls[0].body["tools"][0]["parameters"]["required"],
            json!(["query"])
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
async fn truncated_conflicting_and_length_limited_streams_never_return_executable_calls() {
    for mode in ["truncated", "conflict", "length", "duplicate-terminal"] {
        let mut data = function_events();
        match mode {
            "truncated" => {
                data.truncate(5);
            }
            "conflict" => {
                data[5]["item_id"] = json!("another-item");
            }
            "length" => {
                data.truncate(5);
                data.push(json!({"type":"response.incomplete","response":{"id":"resp_1","model":"model","status":"incomplete","output":[],"incomplete_details":{"reason":"max_output_tokens"}}}));
            }
            _ => {
                data.push(data.last().unwrap().clone());
            }
        }
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let mut request = request(&connection, "model");
        with_tool(&mut request);
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        assert!(result.is_err(), "{mode} must not yield a complete call");
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn unknown_options_wrong_scope_and_wrong_target_never_reach_the_server() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    for field in [
        "store",
        "input",
        "tools",
        "background",
        "previous_response_id",
    ] {
        let mut request = request(&connection, "model");
        request.options.insert(field.into(), json!(true));
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, ModelFailureKind::Unsupported);
    }
    let mut request = request(&connection, "model");
    let mut caller = context(&request);
    caller.scope.workspace_id = id("other");
    assert_eq!(
        model
            .generate(&request, &caller)
            .next()
            .await
            .unwrap()
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    caller = context(&request);
    request
        .route
        .target
        .insert("project_id".into(), json!("different"));
    assert!(
        collect_model_response(&request, model.generate(&request, &caller))
            .await
            .is_err()
    );
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn provider_errors_and_redirects_are_not_retried_or_exposed_as_text() {
    for (status, expected) in [
        (401, ModelFailureKind::Authentication),
        (404, ModelFailureKind::Unavailable),
        (429, ModelFailureKind::RateLimited),
        (503, ModelFailureKind::Transport),
        (302, ModelFailureKind::Unsupported),
    ] {
        let redirect = Server::new(vec![Reply::sse(&events("model", "unexpected redirect"))]).await;
        let mut reply = Reply::json(
            status,
            json!({"error":{"message":"private diagnostic","code":"fixture"}}),
        );
        if status == 302 {
            reply
                .headers
                .push(("location", format!("{}responses", redirect.base)));
        }
        let server = Server::new(vec![
            reply,
            Reply::sse(&events("model", "unexpected retry")),
        ])
        .await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let request = request(&connection, "model");
        let failure =
            collect_model_response(&request, model.generate(&request, &context(&request)))
                .await
                .unwrap_err();
        assert_eq!(failure.kind, expected);
        assert!(failure.partial_text().is_empty());
        assert_eq!(server.requests.lock().unwrap().len(), 1);
        assert!(redirect.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn cancellation_and_deadline_drop_an_unfinished_stream() {
    for cancel in [true, false] {
        let mut reply = Reply::sse(&events("model", "partial")[..3]);
        reply.stall = true;
        let server = Server::new(vec![reply]).await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let request = request(&connection, "model");
        let mut caller = context(&request);
        if !cancel {
            caller.deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(100);
        }
        let mut stream = model.generate(&request, &caller);
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ModelEvent::TextDelta { .. }
        ));
        if cancel {
            caller.cancellation.cancel();
        }
        let next = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap();
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
        tokio::time::timeout(std::time::Duration::from_secs(1), server.closed.notified())
            .await
            .unwrap();
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn model_inspection_uses_registered_snapshot_facts_not_requested_versions_or_name_patterns() {
    let server = Server::new(vec![
        Reply::json(200, json!({"id":"model-2026-01-01","object":"model"})),
        Reply::json(200, json!({"id":"model-2026-01-01","object":"model"})),
    ])
    .await;
    let connection = connection(&server);
    let request = request(&connection, "model-2026-01-01");
    let context = ModelInspectionContext {
        scope: scope(),
        principal_ref: id("user"),
        capability_grant_ref: id("grant"),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(3),
    };
    let unknown = OpenAiInspector::new(connection.clone(), vec![])
        .unwrap()
        .inspect(&request.route, &context)
        .await
        .unwrap();
    assert!(unknown.model_version.is_none());
    assert_eq!(unknown.version_semantics, VersionSemantics::Unverified);
    assert!(
        unknown
            .validate(&request.route, VersionPolicy::RequirePinned)
            .is_err()
    );
    let known = OpenAiInspector::new(
        connection,
        vec![OpenAiSnapshot {
            model_id: id("model-2026-01-01"),
            model_version: id("actual-release"),
            evidence_ref: id("documented-snapshot"),
        }],
    )
    .unwrap()
    .inspect(&request.route, &context)
    .await
    .unwrap();
    assert_eq!(known.model_version, Some(id("actual-release")));
    assert_eq!(known.version_semantics, VersionSemantics::Pinned);
    assert_eq!(
        known
            .validate(&request.route, VersionPolicy::RequirePinned)
            .unwrap_err()
            .code,
        ErrorCode::ModelVersionDrift
    );
    let calls = server.requests.lock().unwrap();
    assert!(
        calls
            .iter()
            .all(|call| call.method == "GET" && call.path == "/v1/models/model-2026-01-01")
    );
}

#[tokio::test]
async fn refusal_cannot_be_relabelled_as_success_by_a_conflicting_terminal() {
    for valid in [false, true] {
        let mut data = events("model", "Unable to help");
        data[2]["type"] = json!("response.refusal.delta");
        data[3]["type"] = json!("response.refusal.done");
        data[3].as_object_mut().unwrap().remove("text");
        data[3]["refusal"] = json!("Unable to help");
        if valid {
            let part = json!({"type":"refusal","refusal":"Unable to help"});
            data[4]["item"]["content"] = json!([part]);
            data[5]["response"]["output"][0]["content"] = json!([part]);
        }
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let request = request(&connection, "model");
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        if valid {
            assert_eq!(result.unwrap().finish, ModelFinish::Refusal);
        } else {
            assert!(result.is_err());
        }
    }
}

#[tokio::test]
async fn interleaved_function_deltas_keep_independent_identity_and_arguments() {
    let mut data = vec![
        json!({"type":"response.created","response":{"id":"resp_1","model":"model","status":"in_progress"}}),
    ];
    let calls:Vec<_>=["one","two"].into_iter().enumerate().map(|(index,query)|json!({"id":format!("fc_{index}"),"type":"function_call","call_id":format!("call_{index}"),"name":"lookup","arguments":format!("{{\"query\":\"{query}\"}}"),"status":"completed"})).collect();
    for (index, call) in calls.iter().enumerate() {
        let mut item = call.clone();
        item["arguments"] = json!("");
        item.as_object_mut().unwrap().remove("status");
        data.push(json!({"type":"response.output_item.added","output_index":index,"item":item}));
    }
    for (index, delta) in [
        (0, "{\"query\":"),
        (1, "{\"query\":"),
        (1, "\"two\"}"),
        (0, "\"one\"}"),
    ] {
        data.push(json!({"type":"response.function_call_arguments.delta","output_index":index,"item_id":format!("fc_{index}"),"delta":delta}));
    }
    for (index, call) in calls.iter().enumerate() {
        data.push(json!({"type":"response.function_call_arguments.done","output_index":index,"item_id":format!("fc_{index}"),"arguments":call["arguments"]}));
        data.push(json!({"type":"response.output_item.done","output_index":index,"item":call}));
    }
    data.push(json!({"type":"response.completed","response":{"id":"resp_1","model":"model","status":"completed","output":calls}}));
    let server = Server::new(vec![Reply::sse(&data)]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    with_tool(&mut request);
    let result = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(result.tool_calls.len(), 2);
    assert_eq!(result.tool_calls[0].model_inputs["query"], "one");
    assert_eq!(result.tool_calls[1].model_inputs["query"], "two");
    assert_ne!(
        result.tool_calls[0].provider_call_id,
        result.tool_calls[1].provider_call_id
    );
}

#[tokio::test]
async fn stream_and_payload_limits_reject_oversized_or_reassigned_data_without_a_success() {
    for case in ["raw", "payload", "events", "identity", "sequence", "usage"] {
        let mut data = events("model", "candidate");
        if case == "identity" {
            data[5]["response"]["id"] = json!("other-response");
        }
        if case == "usage" {
            data[5]["response"]["usage"]["output_tokens"] = json!("7");
        }
        let mut reply = Reply::sse(&data);
        if case == "sequence" {
            let text = String::from_utf8(reply.body).unwrap();
            reply.body = text
                .replacen("\"sequence_number\":2", "\"sequence_number\":1", 1)
                .into_bytes();
        }
        let server = Server::new(vec![reply]).await;
        let connection = if case == "raw" {
            OpenAiConnection::new(
                scope(),
                reference("account"),
                "fixture-key-not-a-secret",
                OpenAiOptions {
                    base_url: server.base.clone(),
                    max_transport_bytes: 64,
                    max_event_bytes: 64,
                    ..Default::default()
                },
            )
            .unwrap()
        } else {
            connection(&server)
        };
        let model = OpenAiModel::new(connection.clone());
        let mut request = request(&connection, "model");
        if case == "payload" {
            request.limits.max_response_bytes = 4;
        }
        if case == "events" {
            request.limits.max_events = 1;
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
async fn protocol_metadata_does_not_consume_the_normalized_model_event_allowance() {
    let server = Server::new(vec![Reply::sse(&events("model", "one delta"))]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    request.limits.max_events = 2;
    let result = collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(result.text, "one delta");
}

#[tokio::test]
async fn final_content_cannot_move_between_messages_or_parts() {
    for mode in ["valid", "message", "part", "kind"] {
        let mut data = events("model", "A");
        data.truncate(4);
        let mut first = text_item("A");
        first["phase"] = json!("commentary");
        let mut second = text_item("B");
        second["id"] = json!("msg_2");
        second["phase"] = json!("final_answer");
        data.push(json!({"type":"response.output_item.added","output_index":1,"item":{"id":"msg_2","type":"message","role":"assistant","content":[]}}));
        data.push(json!({"type":"response.output_text.delta","output_index":1,"item_id":"msg_2","content_index":0,"delta":"B"}));
        match mode {
            "message" => {
                first["content"][0]["text"] = json!("AB");
                second["content"] = json!([]);
            }
            "part" => {
                first["content"] =
                    json!([{"type":"output_text","text":""},{"type":"output_text","text":"A"}]);
            }
            "kind" => {
                first["content"] = json!([{"type":"refusal","refusal":"A"}]);
            }
            _ => {}
        }
        data.push(json!({"type":"response.completed","response":{"id":"resp_1","model":"model","status":"completed","output":[first,second]}}));
        let server = Server::new(vec![Reply::sse(&data)]).await;
        let connection = connection(&server);
        let model = OpenAiModel::new(connection.clone());
        let request = request(&connection, "model");
        let result =
            collect_model_response(&request, model.generate(&request, &context(&request))).await;
        if mode == "valid" {
            assert_eq!(result.unwrap().text, "AB");
        } else {
            assert!(result.is_err(), "accepted {mode} reassignment");
        }
    }
}

#[tokio::test]
async fn two_documented_release_ids_coexist_without_replacing_connection_state() {
    let releases = ["gpt-6-astra", "gpt-5.6-sol"];
    let server = Server::new(
        releases
            .iter()
            .map(|release| Reply::sse(&events(release, release)))
            .collect(),
    )
    .await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    for (index, release) in releases.iter().enumerate() {
        let mut request = request(&connection, release);
        request.route.binding = reference(&format!("binding-{index}"));
        request.route.model_version = id(&format!("fixture-revision-{index}"));
        let result = collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .unwrap();
        assert_eq!(result.text, *release);
        assert_eq!(result.metadata.reported_model_id, Some(id(release)));
        assert!(result.metadata.reported_model_version.is_none());
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (request, release) in requests.iter().zip(releases) {
        assert_eq!(request.body["model"], release);
    }
}

fn compiled_input(schema: Value) -> CompiledTool {
    let properties = schema["properties"]
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
                agent_parameters: properties,
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
async fn compiled_optional_and_nested_inputs_use_strict_wire_and_restore_the_original_contract() {
    let server = Server::new(vec![Reply::sse(&events("model", "done"))]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    let original = compiled_input(json!({"type":"object","properties":{
        "query":{"type":"string","minLength":2,"pattern":"^[a-z]+$"},
        "note":{"type":["string","null"]},
        "filter":{"type":"object","properties":{"category":{"type":"string"},"term":{"type":"string"}},"required":["category"],"additionalProperties":false}
    },"required":["query"],"additionalProperties":false,"if":{"properties":{"query":{"const":"latest"}}},"then":{"required":["note"]}}));
    let contract = CompiledToolContract::compile(
        &original,
        ProviderToolTarget::for_route(&request.route),
        model.tool_schema_compiler().as_ref(),
        ProviderToolSchemaLimits::default(),
    )
    .unwrap();
    request.tools = vec![contract.wire_tool().clone()];
    for fragment in contract.constraint_fragments() {
        request.messages[0].content.push(ModelContent::Text {
            text: fragment.text.clone(),
        });
    }
    let canonical = JsonObject::from([
        ("query".into(), json!("latest")),
        ("note".into(), Value::Null),
        ("filter".into(), json!({"category":"finance"})),
    ]);
    let wire = contract.encode_arguments(&canonical).unwrap();
    assert_eq!(wire["note"], json!({"present":true,"value":null}));
    assert_eq!(
        parse_json(wire["filter"].as_str().unwrap()).unwrap(),
        json!([{"category":"finance"}])
    );
    assert_eq!(
        contract
            .decode_arguments(
                &serde_json::to_string(&wire).unwrap(),
                ProviderToolSchemaLimits::default()
            )
            .unwrap(),
        canonical
    );
    let omitted = JsonObject::from([("query".into(), json!("other"))]);
    let encoded = contract.encode_arguments(&omitted).unwrap();
    assert_eq!(
        contract
            .decode_arguments(
                &serde_json::to_string(&encoded).unwrap(),
                ProviderToolSchemaLimits::default()
            )
            .unwrap(),
        omitted
    );
    assert!(
        contract
            .enforcement()
            .iter()
            .any(|constraint| constraint.canonical_pointer == "/if"
                && constraint.context_text
                && !constraint.provider_native)
    );
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let body = &server.requests.lock().unwrap()[0].body;
    assert_eq!(body["tools"][0]["strict"], true);
    assert_eq!(
        body["tools"][0]["parameters"],
        contract.wire_tool().model_input_schema
    );
    assert_eq!(
        body["tools"][0]["parameters"]["required"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        body["tools"][0]["parameters"]["properties"]["query"]["pattern"],
        "^[a-z]+$"
    );
    assert!(
        body["tools"][0]["parameters"]["properties"]["query"]
            .get("minLength")
            .is_none()
    );
}

#[tokio::test]
async fn model_qualified_compilation_respects_fine_tuned_native_constraint_limits() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let original = compiled_input(
        json!({"type":"object","properties":{"query":{"type":"string","pattern":"^[a-z]+$"}},"required":["query"],"additionalProperties":false}),
    );
    let mut route = request(&connection, "gpt-6-astra").route;
    let normal = CompiledToolContract::compile(
        &original,
        ProviderToolTarget::for_route(&route),
        model.tool_schema_compiler().as_ref(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        normal.wire_tool().model_input_schema,
        *original.model_input_schema()
    );
    route.model_id = id("ft:base:fixture:release");
    let tuned = CompiledToolContract::compile(
        &original,
        ProviderToolTarget::for_route(&route),
        model.tool_schema_compiler().as_ref(),
        Default::default(),
    )
    .unwrap();
    assert!(
        tuned.wire_tool().model_input_schema["properties"]["query"]
            .get("pattern")
            .is_none()
    );
    assert!(
        tuned
            .enforcement()
            .iter()
            .any(
                |constraint| constraint.canonical_pointer == "/properties/query/pattern"
                    && constraint.core
                    && constraint.context_text
            )
    );
    assert_ne!(normal.digest(), tuned.digest());
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn oversized_native_enum_uses_reversible_text_without_losing_original_validation() {
    let server = Server::new(vec![Reply::sse(&events("model", "done"))]).await;
    let connection = connection(&server);
    let model = OpenAiModel::new(connection.clone());
    let mut request = request(&connection, "model");
    let values: Vec<_> = (0..1001).map(|index| format!("choice-{index}")).collect();
    let original = compiled_input(
        json!({"type":"object","properties":{"query":{"type":"string","enum":values}},"required":["query"],"additionalProperties":false}),
    );
    let contract = CompiledToolContract::compile(
        &original,
        ProviderToolTarget::for_route(&request.route),
        model.tool_schema_compiler().as_ref(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        contract.wire_tool().model_input_schema["properties"]["query"],
        json!({"type":"string"})
    );
    for (value, valid) in [("choice-1000", true), ("invented-choice", false)] {
        let canonical = JsonObject::from([("query".into(), json!(value))]);
        let encoded = contract.encode_arguments(&canonical).unwrap();
        let restored = contract
            .decode_arguments(
                &serde_json::to_string(&encoded).unwrap(),
                Default::default(),
            )
            .unwrap();
        assert_eq!(restored, canonical);
        assert_eq!(original.validate_model_inputs(&restored).is_ok(), valid);
    }
    request.tools = vec![contract.wire_tool().clone()];
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    assert_eq!(
        server.requests.lock().unwrap()[0].body["tools"][0]["strict"],
        true
    );
}
```

## `crates/wickle-model-responses/src/codec.rs`

```rust
use serde_json::{Value, json};
use wickle::*;

pub(crate) const REPLAY_KIND: &str = "wickle.openai.responses.v1";
pub(crate) const XAI_REPLAY_KIND: &str = "wickle.xai.responses.v1";
fn failure(code: ErrorCode) -> ContractError {
    crate::error(code, "codec")
}

/// Encode a Responses request, preserving exact route-bound replay and model schemas.
pub fn encode_request(request: &ModelRequest) -> Result<Value, ContractError> {
    encode(request, false)
}
/// Encode xAI's Responses dialect without relaxing OpenAI/Azure validation.
pub fn encode_xai_request(request: &ModelRequest) -> Result<Value, ContractError> {
    if request.tools.len() > 350 || request.options.contains_key("verbosity") {
        return Err(failure(ErrorCode::ModelCapabilityUnsupported));
    }
    if let Some(effort) = request.options.get("reasoning_effort") {
        let effort = effort
            .as_str()
            .ok_or_else(|| failure(ErrorCode::ModelOptionUnsupported))?;
        if !matches!(effort, "none" | "low" | "medium" | "high" | "xhigh")
            || (matches!(request.route.model_id.as_str(), "grok-4.6" | "grok-4.5")
                && effort == "none")
        {
            return Err(failure(ErrorCode::ModelOptionUnsupported));
        }
    }
    encode(request, true)
}
fn encode(request: &ModelRequest, xai: bool) -> Result<Value, ContractError> {
    request.validate()?;
    if request.options.keys().any(|key| {
        !["reasoning_effort", "temperature", "top_p", "verbosity"].contains(&key.as_str())
    }) {
        return Err(failure(ErrorCode::ModelOptionUnsupported));
    }
    let mut input = Vec::new();
    for message in &request.messages {
        let opaque: Vec<_> = message
            .content
            .iter()
            .filter_map(|content| match content {
                ModelContent::Opaque { continuation } => Some(continuation),
                _ => None,
            })
            .collect();
        if !opaque.is_empty() {
            if opaque.len() != 1 || message.role != ModelRole::Assistant {
                return Err(failure(ErrorCode::ModelContextIncompatible));
            }
            let replay = opaque[0].data();
            let object = replay
                .as_object()
                .ok_or_else(|| failure(ErrorCode::ModelContextIncompatible))?;
            if object.len() != 2
                || object.get("kind")
                    != Some(&json!(if xai { XAI_REPLAY_KIND } else { REPLAY_KIND }))
            {
                return Err(failure(ErrorCode::ModelContextIncompatible));
            }
            let items = replay
                .get("items")
                .and_then(Value::as_array)
                .ok_or_else(|| failure(ErrorCode::ModelContextIncompatible))?;
            let decoded = inspect_output_items(items, xai)?;
            let mut text = String::new();
            let mut calls = Vec::new();
            for content in &message.content {
                match content {
                    ModelContent::Text { text: value } => text.push_str(value),
                    ModelContent::ToolCall {
                        provider_call_id,
                        name,
                        arguments,
                    } => calls.push((provider_call_id.as_str(), name.as_str(), arguments)),
                    ModelContent::Opaque { .. } => {}
                    _ => return Err(failure(ErrorCode::ModelContextIncompatible)),
                }
            }
            if text != decoded.text || calls.len() != decoded.calls.len() {
                return Err(failure(ErrorCode::ModelContextIncompatible));
            }
            for ((call_id, name, arguments), original) in calls.iter().zip(&decoded.calls) {
                if *call_id != original.call_id
                    || *name != original.name
                    || match parse_provider_arguments(
                        &original.arguments,
                        request.limits.max_input_bytes,
                    ) {
                        Ok(original) => **arguments != original,
                        // The core retains an empty canonical placeholder for an
                        // unparseable proposal. Replay the original opaque text so
                        // its paired Tool error can reach the next model turn.
                        Err(_) => !arguments.is_empty(),
                    }
                {
                    return Err(failure(ErrorCode::ModelContextIncompatible));
                }
            }
            // Replay the provider items once, in their original order. Do not also
            // append their normalized text and calls, which would duplicate them.
            input.extend(items.iter().cloned());
            continue;
        }
        let role = match message.role {
            ModelRole::System => "system",
            ModelRole::User => "user",
            ModelRole::Assistant => "assistant",
            ModelRole::Tool => "tool",
        };
        let mut parts = Vec::new();
        for content in &message.content {
            if message.role == ModelRole::Tool
                && !matches!(content, ModelContent::ToolResult { .. })
            {
                return Err(failure(ErrorCode::ModelContextIncompatible));
            }
            match content {
                ModelContent::Text { text } => parts.push(text.clone()),
                ModelContent::Json { value } => {
                    parts.push(
                        serde_json::to_string(value)
                            .map_err(|_| failure(ErrorCode::InvalidContract))?,
                    );
                }
                ModelContent::ToolCall {
                    provider_call_id,
                    name,
                    arguments,
                } => {
                    append_text(&mut input, role, &mut parts);
                    input.push(json!({"type":"function_call","call_id":provider_call_id,"name":name,"arguments":serde_json::to_string(arguments).map_err(|_| failure(ErrorCode::InvalidContract))?}));
                }
                ModelContent::ToolResult {
                    provider_call_id,
                    content,
                } => {
                    append_text(&mut input, role, &mut parts);
                    input.push(json!({"type":"function_call_output","call_id":provider_call_id,"output":serde_json::to_string(content).map_err(|_| failure(ErrorCode::InvalidContract))?}));
                }
                ModelContent::Opaque { .. } => unreachable!("opaque handled before normalization"),
            }
        }
        append_text(&mut input, role, &mut parts);
    }
    let tools: Vec<_> = request.tools.iter().map(|tool| json!({"type":"function","name":tool.name,"description":tool.description,"parameters":tool.model_input_schema,"strict":!xai && request.route.provider.as_str() == "openai" && crate::schema::strict_schema_supported(&tool.model_input_schema, request.route.model_id.as_str().starts_with("ft:"))})).collect();
    let mut payload = json!({"model":request.route.model_id,"input":input,"max_output_tokens":request.max_output_tokens.get(),"store":false,"stream":true,"truncation":"disabled","include":["reasoning.encrypted_content"]});
    if !tools.is_empty() {
        payload["tools"] = json!(tools);
        payload["parallel_tool_calls"] = json!(false);
        payload["tool_choice"] = json!("auto");
    }
    if let Some(effort) = request.options.get("reasoning_effort") {
        let effort = effort
            .as_str()
            .filter(|effort| {
                ["none", "minimal", "low", "medium", "high", "xhigh", "max"].contains(effort)
            })
            .ok_or_else(|| failure(ErrorCode::ModelOptionUnsupported))?;
        payload["reasoning"] = json!({"effort":effort});
    }
    for (key, maximum) in [("temperature", 2.0), ("top_p", 1.0)] {
        if let Some(value) = request.options.get(key) {
            if !value
                .as_f64()
                .is_some_and(|number| number.is_finite() && number >= 0.0 && number <= maximum)
            {
                return Err(failure(ErrorCode::ModelOptionUnsupported));
            }
            payload[key] = value.clone();
        }
    }
    if let ModelOutput::JsonSchema { schema } = &request.output {
        validate_output_schema(schema)?;
        payload["text"] = json!({"format":{"type":"json_schema","name":"agent_output","strict":true,"schema":schema}});
    }
    if let Some(value) = request.options.get("verbosity") {
        if !matches!(value.as_str(), Some("low" | "medium" | "high")) {
            return Err(failure(ErrorCode::ModelOptionUnsupported));
        }
        if payload.get("text").is_none() {
            payload["text"] = json!({});
        }
        payload["text"]["verbosity"] = value.clone();
    }
    Ok(payload)
}

fn append_text(input: &mut Vec<Value>, role: &str, parts: &mut Vec<String>) {
    if !parts.is_empty() {
        input.push(json!({"role":role,"content":parts.join("\n")}));
        parts.clear();
    }
}

pub(crate) struct OutputCall {
    pub index: u32,
    pub call_id: String,
    pub name: String,
    pub arguments: String,
}
pub(crate) struct OutputItems {
    pub text: String,
    pub calls: Vec<OutputCall>,
    pub refused: bool,
}
pub(crate) fn inspect_output_items(
    items: &[Value],
    xai: bool,
) -> Result<OutputItems, ContractError> {
    let mut output = OutputItems {
        text: String::new(),
        calls: vec![],
        refused: false,
    };
    let mut call_ids = std::collections::BTreeSet::new();
    for (index, item) in items.iter().enumerate() {
        let object = item
            .as_object()
            .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
        if object.get("id").is_some_and(|id| {
            id.as_str()
                .is_none_or(|id| id.is_empty() && !(xai && item["type"] == "reasoning"))
        }) || object
            .get("status")
            .is_some_and(|status| status != "completed")
        {
            return Err(failure(ErrorCode::InvalidContract));
        }
        let allowed: &[&str] = match item.get("type").and_then(Value::as_str) {
            Some("reasoning") => {
                let summary = item
                    .get("summary")
                    .and_then(Value::as_array)
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                if item
                    .get("encrypted_content")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
                    || summary.iter().any(|part| {
                        part.get("type") != Some(&json!("summary_text"))
                            || part.get("text").and_then(Value::as_str).is_none()
                    })
                {
                    return Err(failure(ErrorCode::ModelContextIncompatible));
                }
                if let Some(content) = item.get("content") {
                    let content = content
                        .as_array()
                        .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                    if content.iter().any(|part| {
                        part.get("type") != Some(&json!("reasoning_text"))
                            || part.get("text").and_then(Value::as_str).is_none()
                            || part.as_object().is_none_or(|part| {
                                part.keys()
                                    .any(|key| !matches!(key.as_str(), "type" | "text"))
                            })
                    }) {
                        return Err(failure(ErrorCode::InvalidContract));
                    }
                }
                // Preserve reasoning content inside route-bound opaque replay only.
                &[
                    "type",
                    "id",
                    "status",
                    "summary",
                    "encrypted_content",
                    "content",
                ]
            }
            Some("message") if item.get("role") == Some(&json!("assistant")) => {
                if item.get("phase").is_some_and(|phase| {
                    !phase.is_null()
                        && !matches!(phase.as_str(), Some("commentary" | "final_answer"))
                }) {
                    return Err(failure(ErrorCode::InvalidContract));
                }
                for part in item
                    .get("content")
                    .and_then(Value::as_array)
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?
                {
                    let text = match part.get("type").and_then(Value::as_str) {
                        Some("output_text") => part.get("text").and_then(Value::as_str),
                        Some("refusal") => {
                            output.refused = true;
                            part.get("refusal").and_then(Value::as_str)
                        }
                        _ => return Err(failure(ErrorCode::InvalidContract)),
                    }
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                    output.text.push_str(text);
                }
                &["type", "id", "status", "role", "content", "phase"]
            }
            Some("function_call") => {
                let call_id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                let arguments = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .ok_or_else(|| failure(ErrorCode::InvalidContract))?;
                if call_id.is_empty()
                    || call_id.len() > 256
                    || call_id.chars().any(|c| c.is_whitespace() || c.is_control())
                    || name.is_empty()
                    || name.len() > 64
                    || !name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                    || !call_ids.insert(call_id)
                {
                    return Err(failure(ErrorCode::InvalidContract));
                }
                // The provider envelope is valid even when proposed arguments are not.
                // Keep their exact bytes for the core's validation/repair loop.
                output.calls.push(OutputCall {
                    index: index
                        .try_into()
                        .map_err(|_| failure(ErrorCode::InvalidContract))?,
                    call_id: call_id.into(),
                    name: name.into(),
                    arguments: arguments.into(),
                });
                &["type", "id", "status", "call_id", "name", "arguments"]
            }
            _ => return Err(failure(ErrorCode::CapabilityUnsupported)),
        };
        if object.keys().any(|key| !allowed.contains(&key.as_str())) {
            return Err(failure(ErrorCode::CapabilityUnsupported));
        }
    }
    if output.refused && !output.calls.is_empty() {
        return Err(failure(ErrorCode::InvalidContract));
    }
    Ok(output)
}

pub(crate) fn fragments(text: &str, maximum: usize) -> Result<Vec<String>, ContractError> {
    let mut remaining = text;
    let mut chunks = Vec::new();
    while !remaining.is_empty() {
        let mut boundary = maximum.min(remaining.len());
        while boundary > 0 && !remaining.is_char_boundary(boundary) {
            boundary -= 1;
        }
        if boundary == 0 {
            return Err(failure(ErrorCode::InvalidContract));
        }
        chunks.push(remaining[..boundary].into());
        remaining = &remaining[boundary..];
    }
    Ok(chunks)
}

fn validate_output_schema(schema: &Value) -> Result<(), ContractError> {
    if schema.get("type") != Some(&json!("object")) || schema.get("anyOf").is_some() {
        return Err(failure(ErrorCode::ModelCapabilityUnsupported));
    }
    fn node(schema: &Value, depth: usize) -> Result<(), ContractError> {
        let object = schema
            .as_object()
            .ok_or_else(|| failure(ErrorCode::ModelCapabilityUnsupported))?;
        if depth > 10
            || [
                "allOf",
                "oneOf",
                "not",
                "dependentRequired",
                "dependentSchemas",
                "if",
                "then",
                "else",
                "patternProperties",
                "propertyNames",
                "unevaluatedProperties",
                "prefixItems",
                "contains",
                "uniqueItems",
            ]
            .iter()
            .any(|key| object.contains_key(*key))
        {
            return Err(failure(ErrorCode::ModelCapabilityUnsupported));
        }
        if schema
            .get("$ref")
            .is_some_and(|value| value.as_str().is_none_or(|value| !value.starts_with('#')))
        {
            return Err(failure(ErrorCode::ModelCapabilityUnsupported));
        }
        let is_object = schema.get("type").is_some_and(|kind| {
            kind == "object"
                || kind
                    .as_array()
                    .is_some_and(|types| types.iter().any(|kind| kind == "object"))
        });
        if is_object {
            let properties = schema
                .get("properties")
                .and_then(Value::as_object)
                .ok_or_else(|| failure(ErrorCode::ModelCapabilityUnsupported))?;
            let required = schema
                .get("required")
                .and_then(Value::as_array)
                .ok_or_else(|| failure(ErrorCode::ModelCapabilityUnsupported))?;
            let names: std::collections::BTreeSet<_> =
                required.iter().filter_map(Value::as_str).collect();
            if schema.get("additionalProperties") != Some(&json!(false))
                || required.len() != names.len()
                || names.len() != properties.len()
                || properties.keys().any(|key| !names.contains(key.as_str()))
            {
                return Err(failure(ErrorCode::ModelCapabilityUnsupported));
            }
        }
        for key in ["properties", "$defs"] {
            if let Some(values) = object.get(key) {
                for child in values
                    .as_object()
                    .ok_or_else(|| failure(ErrorCode::ModelCapabilityUnsupported))?
                    .values()
                {
                    node(child, depth + 1)?;
                }
            }
        }
        if let Some(items) = object.get("items") {
            node(items, depth + 1)?;
        }
        if let Some(items) = object.get("anyOf") {
            for child in items
                .as_array()
                .ok_or_else(|| failure(ErrorCode::ModelCapabilityUnsupported))?
            {
                node(child, depth + 1)?;
            }
        }
        Ok(())
    }
    node(schema, 0)
}
```

## `crates/wickle-model-responses/src/lib.rs`

```rust
//! Shared Responses wire codecs. Provider authentication, endpoints, model
//! selection, and capability policy belong to the calling adapter and Host.
#![forbid(unsafe_code)]
mod codec;
mod response;
mod schema;
mod sse;
pub use codec::{encode_request, encode_xai_request};
pub use response::Decoder as ResponsesDecoder;
pub use schema::ResponsesToolSchemaCompiler;
pub use sse::{Decoder as SseDecoder, Event as SseEvent};
use wickle::{ContractError, ErrorCode};
fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("responses.{location}"))
}
```

## `crates/wickle-model-responses/src/schema.rs`

```rust
//! Versioned strict projection. Original constraints are preserved by the core.
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;
use wickle::*;

/// Responses-compatible Tool schemas with reversible omission/value handling.
#[derive(Debug, Clone, Copy, Default)]
pub struct ResponsesToolSchemaCompiler;
impl ProviderToolSchemaCompiler for ResponsesToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new("wickle-responses-tool-schema").expect("constant"),
            version: Id::new("1").expect("constant"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        if target.api_contract.operation.as_str() != "responses" {
            return Err(invalid("operation"));
        }
        // Unqualified manual targets use the common fine-tuned subset. The
        // normal core path supplies the exact selected model/release.
        let fine_tuned = target
            .model
            .as_ref()
            .is_none_or(|model| model.id.as_str().starts_with("ft:"));
        if strict_schema_supported(&tool.model_input_schema, fine_tuned) {
            return Ok(ProviderToolProjection {
                wire_tool: tool.clone(),
                decode_plan: ArgumentDecodePlan::Identity {},
            });
        }
        let root = &tool.model_input_schema;
        let properties = root
            .get("properties")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("properties"))?;
        let required: BTreeSet<_> = root
            .get("required")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        let mut wire = Map::new();
        let mut fields = vec![];
        for (name, schema) in properties {
            let optional = !required.contains(name.as_str());
            let native = lower(schema, root, fine_tuned, 1, &mut BTreeSet::new());
            let (schema, encoding) = match native {
                Some(schema) if optional => (
                    json!({"type":"object","properties":{"present":{"type":"boolean"},"value":{"anyOf":[schema,{"type":"null"}]}},"required":["present","value"],"additionalProperties":false}),
                    ArgumentValueEncoding::Presence {
                        present_key: "present".into(),
                        value_key: "value".into(),
                    },
                ),
                Some(schema) => (schema, ArgumentValueEncoding::Identity {}),
                None => (
                    json_text_schema(optional),
                    ArgumentValueEncoding::JsonText { optional },
                ),
            };
            wire.insert(name.clone(), schema);
            fields.push(ArgumentFieldMapping {
                wire_name: name.clone(),
                canonical_name: name.clone(),
                encoding,
            });
        }
        let mut projected = tool.clone();
        projected.model_input_schema = object_schema(wire);
        // Representation overhead must not exceed either provider limits or
        // the core's original per-schema byte bound. Compact the largest
        // remaining native field, preserving every canonical field and rule.
        while !strict_schema_supported(&projected.model_input_schema, fine_tuned)
            || serde_json::to_vec(&projected)
                .map_err(|_| invalid("json"))?
                .len()
                > ProviderToolSchemaLimits::default().max_schema_bytes
        {
            let candidate = fields
                .iter()
                .enumerate()
                .filter(|(_, field)| {
                    !matches!(field.encoding, ArgumentValueEncoding::JsonText { .. })
                })
                .max_by_key(|(_, field)| {
                    projected.model_input_schema["properties"][&field.wire_name]
                        .to_string()
                        .len()
                })
                .map(|(index, _)| index)
                .ok_or_else(|| invalid("limits"))?;
            let field = &mut fields[candidate];
            let optional = !required.contains(field.canonical_name.as_str());
            projected.model_input_schema["properties"][&field.wire_name] =
                json_text_schema(optional);
            field.encoding = ArgumentValueEncoding::JsonText { optional };
        }
        Ok(ProviderToolProjection {
            wire_tool: projected,
            decode_plan: ArgumentDecodePlan::Fields { fields },
        })
    }
}
fn object_schema(properties: Map<String, Value>) -> Value {
    let required: Vec<_> = properties.keys().cloned().collect();
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}
fn json_text_schema(_optional: bool) -> Value {
    // The frozen constraint fragment explains the encoding once for the Tool;
    // repeating it in every property needlessly consumes the schema byte budget.
    json!({"type":"string"})
}

fn lower(
    schema: &Value,
    root: &Value,
    fine_tuned: bool,
    depth: usize,
    visiting: &mut BTreeSet<String>,
) -> Option<Value> {
    if depth > 8 {
        return None;
    }
    let node = schema.as_object()?;
    if let Some(reference) = node.get("$ref") {
        let reference = reference.as_str()?;
        if node
            .keys()
            .any(|key| !matches!(key.as_str(), "$ref" | "description" | "title" | "$comment"))
            || !reference.starts_with('#')
            || !visiting.insert(reference.into())
        {
            return None;
        }
        let target = root.pointer(&reference[1..])?;
        let result = lower(target, root, fine_tuned, depth + 1, visiting);
        visiting.remove(reference);
        return result;
    }
    if let Some(alternatives) = node.get("anyOf").or_else(|| node.get("oneOf")) {
        // Keeping only a union would lose sibling intersections. Use JSON text
        // for mixed forms rather than accidentally narrowing their values.
        if node.keys().any(|key| {
            !matches!(
                key.as_str(),
                "anyOf" | "oneOf" | "description" | "title" | "$comment"
            )
        }) {
            return None;
        }
        let branches: Vec<_> = alternatives
            .as_array()?
            .iter()
            .map(|branch| lower(branch, root, fine_tuned, depth + 1, visiting))
            .collect::<Option<_>>()?;
        return Some(json!({"anyOf":branches}));
    }
    let kind = node.get("type")?;
    let types = schema_types(kind)?;
    let mut result = Map::new();
    result.insert("type".into(), kind.clone());
    if types.contains(&"object") {
        if node.contains_key("patternProperties")
            || node.get("additionalProperties") != Some(&Value::Bool(false))
        {
            return None;
        }
        let properties = node.get("properties")?.as_object()?;
        if !all_required(node, properties) {
            return None;
        }
        let mut projected = Map::new();
        for (name, child) in properties {
            projected.insert(
                name.clone(),
                lower(child, root, fine_tuned, depth + 1, visiting)?,
            );
        }
        result.insert("properties".into(), Value::Object(projected));
        result.insert(
            "required".into(),
            node.get("required").cloned().unwrap_or_else(|| json!([])),
        );
        result.insert("additionalProperties".into(), Value::Bool(false));
    }
    if types.contains(&"array") {
        if node.contains_key("prefixItems") || node.get("items").is_some_and(Value::is_array) {
            return None;
        }
        result.insert(
            "items".into(),
            lower(node.get("items")?, root, fine_tuned, depth + 1, visiting)?,
        );
    }
    if let Some(value) = node.get("enum") {
        if value.as_array().is_some_and(|values| {
            !values.is_empty()
                && values
                    .iter()
                    .all(|value| !value.is_object() && !value.is_array())
        }) {
            result.insert("enum".into(), value.clone());
        }
    } else if let Some(value) = node.get("const") {
        if !value.is_object() && !value.is_array() {
            result.insert("enum".into(), json!([value]));
        }
    }
    if !fine_tuned {
        for key in [
            "pattern",
            "minimum",
            "maximum",
            "exclusiveMinimum",
            "exclusiveMaximum",
            "multipleOf",
            "minItems",
            "maxItems",
        ] {
            if let Some(value) = node.get(key) {
                result.insert(key.into(), value.clone());
            }
        }
        if let Some(value) = node
            .get("format")
            .filter(|value| value.as_str().is_some_and(known_format))
        {
            result.insert("format".into(), value.clone());
        }
    }
    if let Some(description) = node.get("description").filter(|value| value.is_string()) {
        result.insert("description".into(), description.clone());
    }
    Some(Value::Object(result))
}
fn schema_types(value: &Value) -> Option<Vec<&str>> {
    let types = if let Some(kind) = value.as_str() {
        vec![kind]
    } else {
        let values = value.as_array()?;
        if values.len() != 2 || !values.iter().any(|kind| kind == "null") {
            return None;
        }
        values
            .iter()
            .map(Value::as_str)
            .collect::<Option<Vec<_>>>()?
    };
    types
        .iter()
        .all(|kind| {
            matches!(
                *kind,
                "string" | "number" | "integer" | "boolean" | "object" | "array" | "null"
            )
        })
        .then_some(types)
}
fn all_required(node: &Map<String, Value>, properties: &Map<String, Value>) -> bool {
    let Some(required) = node.get("required").and_then(Value::as_array) else {
        return properties.is_empty();
    };
    let names: BTreeSet<_> = required.iter().filter_map(Value::as_str).collect();
    names.len() == required.len()
        && names.len() == properties.len()
        && properties.keys().all(|name| names.contains(name.as_str()))
}
fn known_format(value: &str) -> bool {
    matches!(
        value,
        "date-time"
            | "time"
            | "date"
            | "duration"
            | "email"
            | "hostname"
            | "ipv4"
            | "ipv6"
            | "uuid"
    )
}
#[derive(Default)]
struct Bounds {
    properties: usize,
    enums: usize,
    characters: usize,
}
/// Check only; this never rewrites the already compiled wire schema.
pub(crate) fn strict_schema_supported(schema: &Value, fine_tuned: bool) -> bool {
    schema.get("type") == Some(&json!("object"))
        && schema.get("anyOf").is_none()
        && strict_node(schema, schema, fine_tuned, 0, &mut Bounds::default())
}
fn strict_node(
    schema: &Value,
    root: &Value,
    fine_tuned: bool,
    depth: usize,
    bounds: &mut Bounds,
) -> bool {
    let Some(node) = schema.as_object() else {
        return false;
    };
    if depth > 10
        || node.keys().any(|key| {
            !matches!(
                key.as_str(),
                "type"
                    | "properties"
                    | "required"
                    | "additionalProperties"
                    | "items"
                    | "enum"
                    | "anyOf"
                    | "$defs"
                    | "$ref"
                    | "description"
                    | "title"
                    | "pattern"
                    | "format"
                    | "minimum"
                    | "maximum"
                    | "exclusiveMinimum"
                    | "exclusiveMaximum"
                    | "multipleOf"
                    | "minItems"
                    | "maxItems"
            )
        })
    {
        return false;
    }
    if fine_tuned
        && [
            "pattern",
            "format",
            "minimum",
            "maximum",
            "exclusiveMinimum",
            "exclusiveMaximum",
            "multipleOf",
            "minItems",
            "maxItems",
        ]
        .iter()
        .any(|key| node.contains_key(*key))
    {
        return false;
    }
    if node
        .get("format")
        .is_some_and(|value| !value.as_str().is_some_and(known_format))
    {
        return false;
    }
    if let Some(reference) = node.get("$ref") {
        if reference.as_str().is_none_or(|reference| {
            !reference.starts_with('#') || root.pointer(&reference[1..]).is_none()
        }) {
            return false;
        }
    }
    let types = match node.get("type") {
        Some(value) => match schema_types(value) {
            Some(types) => types,
            None => return false,
        },
        None if node.contains_key("anyOf") || node.contains_key("$ref") => vec![],
        None => return false,
    };
    if types.contains(&"object") {
        let Some(properties) = node.get("properties").and_then(Value::as_object) else {
            return false;
        };
        if node.get("additionalProperties") != Some(&Value::Bool(false))
            || !node.contains_key("required")
            || !all_required(node, properties)
        {
            return false;
        }
    }
    if types.contains(&"array") && !node.contains_key("items") {
        return false;
    }
    if let Some(values) = node.get("enum") {
        let Some(values) = values.as_array() else {
            return false;
        };
        if values.is_empty()
            || values
                .iter()
                .any(|value| value.is_array() || value.is_object())
        {
            return false;
        }
        bounds.enums = bounds.enums.saturating_add(values.len());
        let characters: usize = values
            .iter()
            .filter_map(Value::as_str)
            .map(|value| value.chars().count())
            .sum();
        if values.len() > 250 && characters > 15_000 {
            return false;
        }
        bounds.characters = bounds.characters.saturating_add(characters);
    }
    for key in ["properties", "$defs"] {
        if let Some(children) = node.get(key) {
            let Some(children) = children.as_object() else {
                return false;
            };
            if key == "properties" {
                bounds.properties = bounds.properties.saturating_add(children.len());
            }
            bounds.characters = bounds.characters.saturating_add(
                children
                    .keys()
                    .map(|name| name.chars().count())
                    .sum::<usize>(),
            );
            for child in children.values() {
                if !strict_node(child, root, fine_tuned, depth + 1, bounds) {
                    return false;
                }
            }
        }
    }
    if let Some(items) = node.get("items") {
        if !strict_node(items, root, fine_tuned, depth + 1, bounds) {
            return false;
        }
    }
    if let Some(branches) = node.get("anyOf") {
        let Some(branches) = branches.as_array() else {
            return false;
        };
        if branches.is_empty()
            || branches
                .iter()
                .any(|branch| !strict_node(branch, root, fine_tuned, depth + 1, bounds))
        {
            return false;
        }
    }
    bounds.properties <= 5000 && bounds.enums <= 1000 && bounds.characters <= 120_000
}
fn invalid(path: &str) -> ContractError {
    ContractError::new(
        ErrorCode::UnsupportedInputProjection,
        format!("responses.schema.{path}"),
    )
}
```

## `crates/wickle-model-router/tests/support/routed.rs`

```rust
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::stream;
use serde_json::json;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use wickle::*;
use wickle_model_router::{ModelDispatcherEntry, PolicyModelRouter, RegistryModelDispatcher};

use crate::core::{self, id, scope};

fn search_binding() -> PromptToolBinding {
    let descriptor = ToolDescriptor::from_json(&json!({
        "tool":{"id":"search","version":"1"},"name":"search","description":"Search records",
        "input_schema":{"type":"object","properties":{},"required":[],"additionalProperties":false},
        "agent_parameters":[],"output_schema":{},"max_output_bytes":1024
    }).to_string()).unwrap();
    PromptToolBinding {
        selection: serde_json::from_value(json!({"tool_id":"search","version":"1"})).unwrap(),
        compiled: SchemaCompiler::new()
            .compile(descriptor, &SystemInputRegistry::default())
            .unwrap(),
    }
}
fn search_manifest(binding: &PromptToolBinding) -> PinnedPromptTool {
    PinnedPromptTool {
        selection: binding.selection.clone(),
        tool: binding.compiled.descriptor().tool.clone(),
        compiler_version: binding.compiled.compiler_version().into(),
        compiled_digest: binding.compiled.digest().clone(),
        descriptor_digest: binding.compiled.descriptor_digest().clone(),
        model_schema_digest: binding.compiled.model_schema_digest().clone(),
        model_tool: binding.compiled.to_model_tool(),
    }
}
pub fn reference(name: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id("1"),
    }
}
pub fn options() -> JsonObject {
    [("effort".into(), json!("high"))].into_iter().collect()
}

pub fn routing_snapshot() -> RoutingSnapshot {
    let mut models = Vec::new();
    let mut bindings = Vec::new();
    for (name, provider) in [("primary", "provider-a"), ("fallback", "provider-b")] {
        let capabilities = ModelCapabilities {
            revision: id("capabilities"),
            features: [id("text")].into_iter().collect(),
            options_schema: json!({"type":"object","properties":{"effort":{"enum":["high","low"]}},"required":[],"additionalProperties":false}),
            context_window: 4096.try_into().unwrap(),
            max_output_tokens: 1024.try_into().unwrap(),
        };
        let model = ModelDefinition {
            model_key: id(name),
            family: id("fixture-family"),
            provider: id(provider),
            model_id: id(&format!("{name}-model")),
            model_version: id(&format!("{name}-release")),
            version_semantics: VersionSemantics::Pinned,
            lifecycle: ModelLifecycle::Active,
            capabilities: capabilities.clone(),
            evidence: vec![ModelEvidence {
                source_ref: id("fixture-manifest"),
                observed_at_ms: 1,
            }],
        };
        let mut binding = ModelBinding {
            default_options: Default::default(),
            binding: reference(name),
            model: model.reference(),
            requested_model: model.model_id.clone(),
            adapter: reference("adapter"),
            connection_ref: reference(&format!("{name}-account")),
            target: [("region".into(), json!(format!("{name}-region")))]
                .into_iter()
                .collect(),
            target_schema: json!({"type":"object","properties":{"region":{"type":"string"}},"required":["region"],"additionalProperties":false}),
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("protocol"),
            },
            deployment_revision: Some(id(&format!("{name}-deployment"))),
            version_semantics: VersionSemantics::Pinned,
            capabilities,
            support: ModelSupportStatus::ContractTested,
            evidence: vec![],
        };
        binding.evidence.push(ModelValidationEvidence {
            kind: ModelValidationKind::ContractTest,
            binding_digest: binding.contract_digest(&model).unwrap(),
            checked_at_ms: 1,
            evidence_ref: id("fixture-contract"),
            passed: true,
        });
        models.push(model);
        bindings.push(binding);
    }
    let catalog = ModelCatalogSnapshot {
        revision: id("catalog-1"),
        scope: scope(),
        models,
        bindings,
        aliases: vec![],
    };
    let rules = [
        ModelPurpose::Agent,
        ModelPurpose::Verification,
        ModelPurpose::Compaction,
    ]
    .into_iter()
    .map(|purpose| RoutingRule {
        model_binding: id("primary"),
        purpose,
        primary: reference("primary"),
        fallbacks: vec![reference("fallback")],
        fallback_on: vec![
            ModelFailureKind::RateLimited,
            ModelFailureKind::Transport,
            ModelFailureKind::Unavailable,
            ModelFailureKind::VersionDrift,
        ],
        version_policy: VersionPolicy::RequirePinned,
        min_support: ModelSupportStatus::ContractTested,
    })
    .collect();
    RoutingSnapshot::new(
        catalog,
        RoutingPolicy {
            revision: id("policy-1"),
            scope: scope(),
            rules,
        },
    )
    .unwrap()
}

pub struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: 0,
            monotonic_ms: 0,
        })
    }
    fn sleep_until<'a>(&'a self, deadline: u64) -> PortFuture<'a, ()> {
        if deadline == 0 {
            Box::pin(async { Ok(()) })
        } else {
            Box::pin(std::future::pending())
        }
    }
}
#[derive(Default)]
pub struct Ids(AtomicUsize);
impl IdSource for Ids {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id(&format!(
            "attempt-{}",
            self.0.fetch_add(1, Ordering::SeqCst)
        )))
    }
}

#[derive(Clone, Copy)]
pub enum Reply {
    Complete,
    Fail(ModelFailureKind),
    Pending,
}

pub struct Model {
    pub compiler_lookups: AtomicUsize,
    pub panic_compiler: AtomicBool,
    binding: ModelPortBinding,
    replies: Mutex<VecDeque<Reply>>,
    pub calls: Mutex<Vec<ModelRequest>>,
    pub entered: Notify,
}
impl Model {
    fn new(binding: &ModelBinding, replies: Vec<Reply>, snapshot: &RoutingSnapshot) -> Self {
        let model = snapshot
            .catalog()
            .models
            .iter()
            .find(|model| model.reference() == binding.model)
            .unwrap();
        Self {
            compiler_lookups: AtomicUsize::new(0),
            panic_compiler: AtomicBool::new(false),
            binding: ModelPortBinding {
                provider: model.provider.clone(),
                adapter: binding.adapter.clone(),
                connection_ref: binding.connection_ref.clone(),
            },
            replies: Mutex::new(replies.into()),
            calls: Mutex::new(vec![]),
            entered: Notify::new(),
        }
    }
}
impl ModelPort for Model {
    fn tool_schema_compiler(&self) -> Arc<dyn ProviderToolSchemaCompiler> {
        self.compiler_lookups.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.panic_compiler.load(Ordering::SeqCst),
            "compiler must not be consulted for replay"
        );
        Arc::new(NativeToolSchemaCompiler)
    }
    fn binding(&self) -> ModelPortBinding {
        self.binding.clone()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        assert!(
            self.binding.matches_route(&request.route),
            "provider/account binding mismatch"
        );
        assert_eq!(request.request_id, context.attempt_id);
        assert_eq!(context.scope, scope());
        self.calls.lock().unwrap().push(request.clone());
        self.entered.notify_one();
        match self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra physical model request")
        {
            Reply::Complete => Box::pin(stream::iter([
                Ok(ModelEvent::TextDelta {
                    text: "Completed response".into(),
                }),
                Ok(ModelEvent::ResponseCompleted {
                    finish: ModelFinish::Stop,
                    metadata: ModelResponseMetadata {
                        provider_request_id: Some(id("actual-provider-request")),
                        reported_model_id: Some(id("actually-reported-model")),
                        reported_model_version: None,
                        usage: None,
                    },
                    continuation: vec![],
                }),
            ])),
            Reply::Fail(kind) => Box::pin(stream::iter([
                Ok(ModelEvent::TextDelta {
                    text: "Partial response".into(),
                }),
                Ok(ModelEvent::ResponseError {
                    kind,
                    metadata: ModelResponseMetadata::default(),
                }),
            ])),
            Reply::Pending => Box::pin(stream::pending()),
        }
    }
}

#[derive(Default)]
pub struct Policy {
    pub denied_provider: Mutex<Option<Id>>,
    pub calls: Mutex<Vec<(ResolvedModelRoute, ModelPurpose)>>,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::InvokeModel { route, purpose } = &request.action {
                self.calls
                    .lock()
                    .unwrap()
                    .push((route.as_ref().clone(), *purpose));
                if self.denied_provider.lock().unwrap().as_ref() == Some(&route.provider) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("destination-denied"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}

#[derive(Clone, Copy, Default)]
pub enum Inspection {
    #[default]
    Healthy,
    DriftPrimary,
    UnavailablePrimary,
    UnavailableAll,
    Pending,
    PendingFallback,
    Panics,
}
#[derive(Default)]
pub struct Inspector {
    pub mode: Mutex<Inspection>,
    pub calls: Mutex<Vec<ResolvedModelRoute>>,
    pub tokens: Mutex<Vec<CancellationToken>>,
    pub entered: Notify,
}
impl ModelRouteInspector for Inspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        context: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            assert_eq!(context.scope, scope());
            self.calls.lock().unwrap().push(route.clone());
            self.tokens
                .lock()
                .unwrap()
                .push(context.cancellation.clone());
            self.entered.notify_one();
            let mode = *self.mode.lock().unwrap();
            match mode {
                Inspection::Pending => std::future::pending::<()>().await,
                Inspection::PendingFallback if route.binding.id == id("fallback") => {
                    std::future::pending::<()>().await
                }
                Inspection::Panics => panic!("fixture inspector panic"),
                _ => {}
            }
            let primary = route.binding.id == id("primary");
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: if matches!(mode, Inspection::UnavailableAll)
                    || (primary && matches!(mode, Inspection::UnavailablePrimary))
                {
                    ModelRouteAvailability::Unavailable
                } else {
                    ModelRouteAvailability::Available
                },
                model_id: Some(route.model_id.clone()),
                model_version: Some(if primary && matches!(mode, Inspection::DriftPrimary) {
                    id("changed-release")
                } else {
                    route.model_version.clone()
                }),
                deployment_revision: route.deployment_revision.clone(),
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id(&format!("inspection-{}", route.binding.id)),
            })
        })
    }
}

#[derive(Clone, Copy, Default)]
pub enum Projection {
    #[default]
    Valid,
    WrongRoute,
    WrongOptions,
    TooManyTokens,
    OldOpaque,
    DifferentContent,
    UsesTools,
    UsesJson,
}
#[derive(Default)]
pub struct Projector {
    pub mode: Mutex<Projection>,
    pub calls: Mutex<Vec<ResolvedModelRoute>>,
}
impl ModelRequestProjector for Projector {
    fn project<'a>(
        &'a self,
        selection: &'a RouteSelection,
        input: &'a RoutedModelInput,
        context: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ProjectedModelRequest> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(selection.route.clone());
            let mode = *self.mode.lock().unwrap();
            let mut request = ModelRequest {
                request_id: input.model_step_id.clone(),
                purpose: input.routing.purpose,
                route: selection.route.clone(),
                messages: vec![ModelMessage {
                    role: ModelRole::User,
                    content: vec![ModelContent::Text {
                        text: if matches!(mode, Projection::DifferentContent) {
                            "Changed required context"
                        } else {
                            "Preserved required context"
                        }
                        .into(),
                    }],
                }],
                tools: vec![],
                output: ModelOutput::Text {},
                max_output_tokens: context.configuration.max_output_tokens,
                options: context.configuration.effective.clone(),
                limits: ModelResponseLimits {
                    max_input_bytes: 16384,
                    max_response_bytes: 4096,
                    max_delta_bytes: 1024,
                    max_events: 8,
                    max_tool_calls: 0,
                },
            };
            match mode {
                Projection::WrongRoute => {
                    request.route.connection_ref = reference("different-account")
                }
                Projection::WrongOptions => {
                    request.options.insert("effort".into(), json!("low"));
                }
                Projection::OldOpaque => {
                    let old = ResolvedModelRoute {
                        provider: id("old-provider"),
                        ..selection.route.clone()
                    };
                    request.messages.push(ModelMessage {
                        role: ModelRole::Assistant,
                        content: vec![ModelContent::Opaque {
                            continuation: OpaqueContinuation::new(
                                &old,
                                json!({"private_replay":"old"}),
                            ),
                        }],
                    });
                }
                Projection::UsesTools => {
                    request.tools = vec![ModelTool {
                        name: id("search"),
                        description: "Search records".into(),
                        model_input_schema: json!({"type":"object","properties":{},"required":[],"additionalProperties":false}),
                    }];
                }
                Projection::UsesJson => {
                    request.output = ModelOutput::JsonSchema {
                        schema: json!({"type":"object"}),
                    };
                }
                _ => {}
            }
            let mut tool_set = vec![];
            let mut compiled_tools = vec![];
            if matches!(mode, Projection::UsesTools) {
                let binding = search_binding();
                tool_set.push(ResolvedToolSetEntry::new(
                    search_manifest(&binding),
                    &binding.compiled,
                )?);
                compiled_tools.push(CompiledToolContract::compile(
                    &binding.compiled,
                    ProviderToolTarget::for_route(&selection.route),
                    context
                        .tool_schema_compiler
                        .as_deref()
                        .expect("new projection compiler"),
                    ProviderToolSchemaLimits::default(),
                )?);
            }
            Ok(ProjectedModelRequest {
                tool_set,
                compiled_tools,
                provenance: Default::default(),
                request,
                input_tokens: if matches!(mode, Projection::TooManyTokens) {
                    4096
                } else {
                    100
                },
            })
        })
    }
}

pub struct Fixture {
    pub store: Arc<MemoryStateStore>,
    pub lease: RunLease,
    pub ids: Arc<Ids>,
    pub snapshot: RoutingSnapshot,
    pub router: PolicyModelRouter,
    pub first: Arc<Model>,
    pub second: Arc<Model>,
    pub policy: Arc<Policy>,
    pub inspector: Arc<Inspector>,
    pub projector: Projector,
}
impl Fixture {
    pub async fn new(first: Vec<Reply>, second: Vec<Reply>) -> Self {
        Self::with_limits(first, second, 8, 4).await
    }
    pub async fn with_limits(
        first: Vec<Reply>,
        second: Vec<Reply>,
        model_calls: u64,
        recoveries: u64,
    ) -> Self {
        let store = Arc::new(MemoryStateStore::new());
        let mut input =
            core::admission("run", "request", "session", "Preserved request", "1").await;
        let mut profile = serde_json::to_value(input.snapshot.profile.profile()).unwrap();
        profile["tools"] = json!([{"tool_id":"search","version":"1"}]);
        profile["limits"]["max_model_calls"] = json!(model_calls);
        profile["limits"]["max_recovery_attempts"] = json!(recoveries);
        input.snapshot.profile = ProfileValidator::new(&core::Catalog { revision: "1" })
            .validate(
                &AgentProfile::from_json(&profile.to_string()).unwrap(),
                &scope(),
            )
            .await
            .unwrap();
        input.snapshot.limits = input.snapshot.profile.profile().limits.clone();
        input.snapshot.request.model_options = options();
        input.snapshot.request_digest =
            admission_digest(&input.snapshot.request, &input.snapshot.profile, None);
        let RunEventPayload::RunStarted {
            request_ref,
            profile_digest,
        } = &mut input.events[0].payload
        else {
            unreachable!()
        };
        *profile_digest = input.snapshot.profile.profile_digest().clone();
        let record = ProtectedRecord::new(
            request_ref.record_id.clone(),
            request_ref.revision,
            serde_json::to_value(&input.snapshot.request).unwrap(),
        );
        let previous = request_ref.clone();
        *request_ref = record.reference().clone();
        *input
            .records
            .iter_mut()
            .find(|record| record.reference() == &previous)
            .unwrap() = record;
        let prompt = PromptSnapshot::create(
            &input.snapshot.profile,
            vec![],
            None,
            vec![search_binding()],
            vec![],
        )
        .unwrap();
        let prompt_record = ProtectedRecord::new(
            input.prompt_snapshot.record_id.clone(),
            1,
            serde_json::to_value(prompt).unwrap(),
        );
        let old_prompt = input.prompt_snapshot.clone();
        input.prompt_snapshot = prompt_record.reference().clone();
        *input
            .records
            .iter_mut()
            .find(|record| record.reference() == &old_prompt)
            .unwrap() = prompt_record;
        store.admit(&scope(), input).await.unwrap();
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("worker"), 0, 20_000)
            .await
            .unwrap();
        let snapshot = routing_snapshot();
        let first = Arc::new(Model::new(
            &snapshot.catalog().bindings[0],
            first,
            &snapshot,
        ));
        let second = Arc::new(Model::new(
            &snapshot.catalog().bindings[1],
            second,
            &snapshot,
        ));
        let router = PolicyModelRouter::new(snapshot.clone()).unwrap();
        Self {
            store,
            lease,
            ids: Arc::new(Ids::default()),
            snapshot,
            router,
            first,
            second,
            policy: Arc::new(Policy::default()),
            inspector: Arc::new(Inspector::default()),
            projector: Projector::default(),
        }
    }
    pub fn input(&self, step: &str) -> RoutedModelInput {
        RoutedModelInput {
            model_step_id: id(step),
            routing: RouteRequest {
                model_binding: id("primary"),
                purpose: ModelPurpose::Agent,
                required_capabilities: [id("text")].into_iter().collect(),
                input_tokens: 1,
                max_output_tokens: 64.try_into().unwrap(),
                options: options(),
                scope: scope(),
                allowed_bindings: vec![id("primary"), id("fallback")],
                version_policy: VersionPolicy::RequirePinned,
                previous_route: None,
                previous_failure: None,
            },
        }
    }
    pub fn enable_feature(&mut self, feature: &str) {
        let mut catalog = self.snapshot.catalog().clone();
        for model in &mut catalog.models {
            model.capabilities.features.insert(id(feature));
        }
        for binding in &mut catalog.bindings {
            binding.capabilities.features.insert(id(feature));
            let model = catalog
                .models
                .iter()
                .find(|model| model.reference() == binding.model)
                .unwrap();
            binding.evidence[0].binding_digest = binding.contract_digest(model).unwrap();
        }
        self.snapshot = RoutingSnapshot::new(catalog, self.snapshot.policy().clone()).unwrap();
        self.router = PolicyModelRouter::new(self.snapshot.clone()).unwrap();
    }
    pub fn context(&self) -> ExecutionContext {
        ExecutionContext::new(
            ExecutionContextData {
                scope: scope(),
                principal_ref: id("actor"),
                capability_grant_ref: id("grant"),
                trace_context: None,
                system_inputs: None,
            },
            CancellationToken::new(),
        )
    }
    pub async fn budget(&self) -> RunBudget {
        RunBudget::attach(
            self.store.clone(),
            Arc::new(FixedClock),
            self.ids.clone(),
            scope(),
            id("run"),
            self.lease.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap()
    }
    pub fn exchange(&self, retries: u32) -> ModelExchange {
        let dispatcher = RegistryModelDispatcher::new(vec![
            ModelDispatcherEntry {
                scope: scope(),
                port: self.first.clone(),
            },
            ModelDispatcherEntry {
                scope: scope(),
                port: self.second.clone(),
            },
        ])
        .unwrap();
        ModelExchange::with_dispatcher(
            Arc::new(dispatcher),
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap()),
        )
        .with_route_inspector(self.inspector.clone(), Duration::from_secs(1))
        .unwrap()
        .with_retry_policy(ModelRetryPolicy {
            max_retries: retries,
            backoff_ms: 0,
        })
    }
    pub async fn saved(&self) -> RunSnapshot {
        self.store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
    }
    pub fn call_counts(&self) -> (usize, usize) {
        (
            self.first.calls.lock().unwrap().len(),
            self.second.calls.lock().unwrap().len(),
        )
    }
}

pub fn completed(outcome: Guarded<ModelExchangeOutcome>) -> ModelResponse {
    let Guarded::Completed(ModelExchangeOutcome::Completed { response }) = outcome else {
        panic!("expected complete response")
    };
    response
}

pub async fn seed_tool(fixture: &Fixture, state: ToolCallState) {
    let saved = fixture.saved().await;
    let call = ToolCall {
        provider_arguments: None,
        call_id: id("unsettled-call"),
        model_request_id: id("earlier-attempt"),
        provider_call_id: id("earlier-call"),
        tool_name: id("tool"),
        model_inputs: JsonObject::new(),
        descriptor_digest: Some(canonical_digest(&json!("descriptor"))),
        bound_input_ref: None,
    };
    let mut change = core::prepared(&saved, fixture.lease.clone(), 0);
    change
        .snapshot
        .tool_ledger
        .push(ToolLedgerEntry { call, state });
    fixture
        .store
        .commit(&scope(), &id("run"), change)
        .await
        .unwrap();
}

pub fn unknown_tool_result() -> ToolCallState {
    ToolCallState::Settled {
        result: ToolResult {
            call_id: id("unsettled-call"),
            call_message_id: id("earlier-message"),
            status: ToolResultStatus::Unknown,
            effect: ToolEffect::Unknown,
            content: vec![],
            effect_receipt_ref: None,
            skill_ref: None,
            error: None,
        },
    }
}
```

## `crates/wickle/src/agent/driver.rs`

```rust
use super::*;
use std::{collections::BTreeSet, panic::AssertUnwindSafe};

impl Agent {
    pub(super) async fn drive(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        context: ExecutionContext,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let bindings = &self.inner.bindings;
        let now = bindings.clock.now()?.utc_ms;
        let lease = bindings
            .state
            .acquire_lease(
                &bindings.scope,
                run_id,
                &bindings.ids.next_id()?,
                now,
                bindings.settings.lease_ttl_ms,
            )
            .await?;
        crate::future::boxed(|| self.drive_leased(run_id, prompt, context, local, lease, false))
            .await
    }

    pub(super) async fn drive_leased(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        context: ExecutionContext,
        local: &Arc<LocalRun>,
        lease: RunLease,
        expired: bool,
    ) -> Result<(), ContractError> {
        let bindings = &self.inner.bindings;
        let budget = match RunBudget::attach(
            bindings.state.clone(),
            bindings.clock.clone(),
            bindings.ids.clone(),
            bindings.scope.clone(),
            run_id.clone(),
            lease.clone(),
            local.cancel.clone(),
        )
        .await
        {
            Ok(budget) => Arc::new(budget),
            Err(error) => {
                self.release_owned(run_id, &lease).await;
                return Err(error);
            }
        };
        let stop = CancellationToken::new();
        let heartbeat_agent = self.clone();
        let heartbeat_budget = budget.clone();
        let heartbeat_lease = lease.clone();
        let heartbeat_id = run_id.clone();
        let heartbeat_stop = stop.clone();
        let heartbeat_local = local.clone();
        let heartbeat = tokio::spawn(async move {
            let result = AssertUnwindSafe(heartbeat_agent.heartbeat(
                &heartbeat_id,
                heartbeat_lease,
                &heartbeat_budget,
                &heartbeat_stop,
                &heartbeat_local,
            ))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(fail(ErrorCode::LeaseLost, "agent.heartbeat")));
            if let Err(error) = &result {
                if let Ok(mut slot) = heartbeat_local.error.lock() {
                    *slot = Some(error.clone());
                }
                heartbeat_local.cancel.cancel();
            }
            result
        });
        let mut segment = None;
        let result = {
            let work = AssertUnwindSafe(async {
                let saved = bindings.state.load(&bindings.scope, run_id).await?;
                let metadata = self.metadata_segment(&saved, context.clone()).await?;
                if expired {
                    segment = Some(metadata);
                    self.finish(
                        run_id,
                        PreparedOutcome {
                            result: OutcomeResult::Exhausted {
                                budget: BudgetKind::Elapsed,
                            },
                            output: vec![],
                            continuation: vec![],
                            unresolved_effects: vec![],
                            verification: None,
                        },
                        &budget,
                        segment.as_ref().expect("metadata segment"),
                        local,
                    )
                    .await
                } else {
                    match self
                        .bind_segment(
                            &saved,
                            context.clone(),
                            Some(&lease),
                            ComponentBindPurpose::Execution,
                            Some(&budget),
                            local,
                        )
                        .await
                    {
                        Ok(bound) => {
                            segment = Some(bound);
                            self.observe_pending(
                                run_id,
                                segment.as_ref().expect("bound segment"),
                                local,
                            )
                            .await;
                            crate::future::boxed(|| {
                                self.run_segment(
                                    run_id,
                                    prompt,
                                    segment.as_ref().expect("bound segment"),
                                    &budget,
                                    &lease,
                                    local,
                                )
                            })
                            .await
                        }
                        Err(error) => {
                            segment = Some(metadata);
                            if matches!(
                                error.code,
                                ErrorCode::LeaseLost
                                    | ErrorCode::PersistenceUnavailable
                                    | ErrorCode::RevisionConflict
                            ) {
                                return Err(error);
                            }
                            self.finish(
                                run_id,
                                PreparedOutcome {
                                    result: match error.code {
                                        ErrorCode::Cancelled => OutcomeResult::Cancelled {
                                            reason: local
                                                .reason
                                                .lock()
                                                .map_err(|_| {
                                                    fail(ErrorCode::InvalidContract, "agent.cancel")
                                                })?
                                                .as_ref()
                                                .map(ToString::to_string)
                                                .unwrap_or_else(|| "cancelled".into()),
                                        },
                                        ErrorCode::DeadlineExceeded | ErrorCode::BudgetExceeded => {
                                            OutcomeResult::Exhausted {
                                                budget: BudgetKind::Elapsed,
                                            }
                                        }
                                        _ => failed(&enum_name(&error.code)),
                                    },
                                    output: vec![],
                                    continuation: vec![],
                                    unresolved_effects: vec![],
                                    verification: None,
                                },
                                &budget,
                                segment.as_ref().expect("metadata segment"),
                                local,
                            )
                            .await
                        }
                    }
                }
            })
            .catch_unwind();
            tokio::pin!(work);
            tokio::select! { biased;
                result = &mut work => result.unwrap_or_else(|_| Err(fail(ErrorCode::InvalidContract, "agent.driver"))),
                _ = budget.wait_for_cancellation_or_deadline() => {
                    let deadline = self.cleanup_deadline(local)?;
                    match tokio::time::timeout_at(deadline, &mut work).await {
                        Ok(result) => result.unwrap_or_else(|_| Err(fail(ErrorCode::InvalidContract, "agent.driver"))),
                        Err(_) => Err(fail(ErrorCode::PersistenceUnavailable, "agent.stop_cleanup_timeout")),
                    }
                }
            }
        };
        stop.cancel();
        let heartbeat_result = heartbeat
            .await
            .map_err(|_| fail(ErrorCode::LeaseLost, "agent.heartbeat"))?;
        let cleanup_deadline = self.cleanup_deadline(local)?;
        let ownership_lost = matches!(&result, Err(error) if error.code == ErrorCode::LeaseLost)
            || matches!(&heartbeat_result, Err(error) if error.code == ErrorCode::LeaseLost);
        let latest = match tokio::time::timeout_at(
            cleanup_deadline,
            bindings.state.load(&bindings.scope, run_id),
        )
        .await
        {
            Ok(value) => value,
            Err(_) => Err(fail(
                ErrorCode::PersistenceUnavailable,
                "agent.cleanup_read",
            )),
        };
        // Release the fenced lease before adapter cleanup. Known ownership loss
        // never authorizes a release or an after-run callback from the old owner.
        // Terminal commits already release the lease atomically in the store.
        let terminal_committed = latest
            .as_ref()
            .is_ok_and(|saved| saved.snapshot.status.is_terminal());
        if !ownership_lost && !terminal_committed {
            if let Ok((_, now)) = budget.settlement_time(0) {
                let released = tokio::time::timeout_at(
                    cleanup_deadline,
                    bindings
                        .state
                        .release_lease(&bindings.scope, run_id, &lease, now),
                )
                .await;
                let failure = match released {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(error),
                    Err(_) => Some(fail(ErrorCode::DeadlineExceeded, "agent.lease_release")),
                };
                if let Some(error) = failure {
                    if let Ok(mut slot) = local.release_error.lock() {
                        *slot = Some(error);
                    }
                }
            }
        }
        if let Some(segment) = segment.as_ref() {
            let cleanup = async {
                if !ownership_lost {
                    if let Ok(saved) = &latest {
                        if saved.snapshot.status.is_terminal() {
                            if bindings.components.is_some() && segment.owned.is_none() {
                                if expired {
                                    self.cleanup_observers(saved, &context, local, vec![]).await;
                                } else if let Ok(mut slot) = local.release_error.lock() {
                                    if slot.is_none() {
                                        *slot = Some(fail(
                                            ErrorCode::ComponentUnavailable,
                                            "components.observers_not_bound",
                                        ));
                                    }
                                }
                            } else {
                                self.after_run(saved, segment, local).await;
                            }
                        }
                    }
                }
                self.release_segment(segment, local).await;
            };
            if tokio::time::timeout_at(cleanup_deadline, cleanup)
                .await
                .is_err()
            {
                if let Ok(mut slot) = local.release_error.lock() {
                    *slot = Some(fail(ErrorCode::DeadlineExceeded, "components.cleanup"));
                }
            }
        }
        if ownership_lost {
            return Err(fail(ErrorCode::LeaseLost, "agent.ownership_lost"));
        }
        if latest.as_ref().is_ok_and(|saved| {
            saved.snapshot.status.is_terminal()
                || matches!(
                    saved.snapshot.status,
                    RunStatus::Waiting | RunStatus::Interrupted
                )
        }) {
            return Ok(());
        }
        result.and(heartbeat_result)
    }

    async fn heartbeat(
        &self,
        run_id: &Id,
        mut lease: RunLease,
        budget: &RunBudget,
        stop: &CancellationToken,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let bindings = &self.inner.bindings;
        loop {
            let reading = bindings.clock.now()?;
            let next = reading
                .monotonic_ms
                .checked_add(bindings.settings.heartbeat_interval_ms)
                .ok_or_else(|| fail(ErrorCode::ClockUnavailable, "agent.heartbeat"))?;
            tokio::select! { biased;
                _ = stop.cancelled() => return Ok(()),
                result = bindings.clock.sleep_until(next) => result?,
            }
            let (_, now) = budget.settlement_time(0)?;
            let renewal = bindings.state.renew_lease(
                &bindings.scope,
                run_id,
                &lease,
                now,
                bindings.settings.lease_ttl_ms,
            );
            let remaining = lease
                .expires_at_ms
                .checked_sub(now)
                .and_then(|value| u64::try_from(value).ok())
                .filter(|value| *value > 0)
                .ok_or_else(|| fail(ErrorCode::LeaseLost, "agent.heartbeat"))?;
            let result = tokio::select! { biased;
                _ = stop.cancelled() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_millis(remaining)) => Err(fail(ErrorCode::LeaseLost, "agent.heartbeat")),
                result = renewal => result,
            };
            match result {
                Ok(current) => lease = current,
                Err(error) => return Err(error),
            }
            self.signal_pending_control(run_id, local, Some(now))
                .await?;
        }
    }

    async fn run_segment(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        segment: &SegmentBindings,
        budget: &RunBudget,
        lease: &RunLease,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let context = &segment.context;
        let mut waiting = None;
        let mut saved = self
            .inner
            .bindings
            .state
            .load(budget.scope(), run_id)
            .await?;
        let recovering = saved
            .snapshot
            .recovery_receipts
            .last()
            .is_some_and(|receipt| receipt.accepted_revision == local.segment_start_revision);
        let mut recovery_error = None;
        if recovering {
            let uncertain: Vec<_> = saved
                .snapshot
                .tool_ledger
                .iter()
                .filter(|entry| {
                    matches!(
                        entry.state,
                        ToolCallState::Dispatching { .. } | ToolCallState::Unknown { .. }
                    )
                })
                .map(|entry| entry.call.call_id.clone())
                .collect();
            let round = self.tool_round(budget, segment).await?;
            for call_id in uncertain {
                match crate::future::boxed(|| round.reconcile_call(&call_id, context, budget)).await
                {
                    Ok(result) if result.effect == ToolEffect::Unknown => break,
                    Ok(_) => {}
                    Err(error) => {
                        recovery_error = Some(error);
                        break;
                    }
                }
            }
            saved = self
                .inner
                .bindings
                .state
                .load(budget.scope(), run_id)
                .await?;
        }
        let mut reuse_step = recovering
            && matches!(saved.snapshot.phase, RunPhase::Prepare | RunPhase::Model)
            && saved.snapshot.model_step_id.is_some();
        let mut pending_round = saved.snapshot.tool_ledger.iter().find(|entry| !matches!(&entry.state, ToolCallState::Settled { result } if result.status != ToolResultStatus::Unknown && result.effect != ToolEffect::Unknown)).map(|entry| entry.call.model_request_id.clone());
        let attempt = loop {
            if let Some(error) = recovery_error.take() {
                break Some(Err(error));
            }
            let current = self
                .inner
                .bindings
                .state
                .load(budget.scope(), run_id)
                .await?;
            if current.snapshot.candidate_ref.is_some() {
                match crate::future::boxed(|| self.verify_candidate(segment, budget)).await {
                    Ok(super::verification::CandidateAction::Finish(candidate)) => {
                        return self
                            .finish(run_id, *candidate, budget, segment, local)
                            .await;
                    }
                    Ok(super::verification::CandidateAction::Repair) => continue,
                    Err(error) => break Some(Err(error)),
                }
            }
            if let Some(request_id) = pending_round.take() {
                let round = self.tool_round(budget, segment).await?;
                let result =
                    crate::future::boxed(|| round.execute(&request_id, context, budget)).await;
                self.remember_observer_error(local, round.observer_error());
                match result {
                    Ok(ToolRoundOutcome::Completed) => {}
                    Ok(outcome) => {
                        waiting = Some(self.tool_wait(outcome, budget).await?);
                        break None;
                    }
                    Err(error) => break Some(Err(error)),
                }
            }
            if let Some(request_id) = current
                .snapshot
                .tool_ledger
                .last()
                .map(|entry| entry.call.model_request_id.clone())
            {
                if let Err(error) = self.reserve_tool_repair(&request_id, budget).await {
                    break Some(Err(error));
                }
            }
            // Keep the nested model/verification path off the parent Tool loop stack.
            match crate::future::boxed(|| {
                self.generate(
                    run_id,
                    prompt.clone(),
                    segment,
                    budget,
                    lease,
                    std::mem::take(&mut reuse_step),
                )
            })
            .await
            {
                Ok(Guarded::Completed(ModelExchangeOutcome::Completed { response }))
                    if response.finish == ModelFinish::ToolCalls =>
                {
                    if let Err(error) = self.plan_tools(&response, &prompt, budget).await {
                        break Some(Err(error));
                    }
                    let round = self.tool_round(budget, segment).await?;
                    let result = crate::future::boxed(|| {
                        round.execute(&response.request_id, context, budget)
                    })
                    .await;
                    self.remember_observer_error(local, round.observer_error());
                    match result {
                        Ok(ToolRoundOutcome::Completed) => continue,
                        Ok(outcome) => {
                            waiting = Some(self.tool_wait(outcome, budget).await?);
                            break None;
                        }
                        Err(error) => break Some(Err(error)),
                    }
                }
                Ok(Guarded::Completed(ModelExchangeOutcome::Completed { response }))
                    if response.finish == ModelFinish::Stop
                        && response.tool_calls.is_empty()
                        && saved.snapshot.verification_plan_ref.is_some() =>
                {
                    if let Err(error) =
                        crate::future::boxed(|| self.candidate(&response, budget)).await
                    {
                        break Some(Err(error));
                    }
                    continue;
                }
                result => break Some(result),
            }
        };
        if let Some(error) = local
            .error
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .clone()
        {
            return Err(error);
        }
        if let Some((wait, unresolved_effects)) = waiting {
            return self
                .finish(
                    run_id,
                    PreparedOutcome {
                        result: OutcomeResult::Waiting { wait },
                        output: vec![],
                        continuation: vec![],
                        unresolved_effects,
                        verification: None,
                    },
                    budget,
                    segment,
                    local,
                )
                .await;
        }
        let attempt = attempt.expect("non-waiting loop result");
        let mut continuation = vec![];
        let (result, output) = match attempt {
            Ok(Guarded::Completed(ModelExchangeOutcome::Completed { response }))
                if response.finish == ModelFinish::Stop && response.tool_calls.is_empty() =>
            {
                continuation = response.continuation;
                (
                    OutcomeResult::Succeeded {
                        completion_basis: CompletionBasis::TurnEnded,
                    },
                    vec![InputContent::Text {
                        text: response.text,
                    }],
                )
            }
            Ok(Guarded::Completed(ModelExchangeOutcome::Completed { response })) => (
                failed(if response.finish == ModelFinish::Refusal {
                    "model_refusal"
                } else {
                    "tool_execution_unsupported"
                }),
                vec![],
            ),
            Ok(Guarded::Completed(ModelExchangeOutcome::Failed { failure })) => (
                failed(&format!("model_{}", enum_name(&failure.kind))),
                if failure.partial_text().is_empty() {
                    vec![]
                } else {
                    vec![InputContent::Text {
                        text: failure.partial_text().to_owned(),
                    }]
                },
            ),
            Ok(Guarded::ApprovalRequired(_)) => (failed("approval_runtime_unsupported"), vec![]),
            Err(error)
                if matches!(
                    error.code,
                    ErrorCode::LeaseLost
                        | ErrorCode::RevisionConflict
                        | ErrorCode::PersistenceUnavailable
                        | ErrorCode::StateNotFound
                        | ErrorCode::ClockUnavailable
                        | ErrorCode::ClockRegression
                        | ErrorCode::InvalidTransition
                        | ErrorCode::InvalidSnapshot
                        | ErrorCode::InvalidEvent
                        | ErrorCode::RecordConflict
                ) =>
            {
                return Err(error);
            }
            Err(error) if error.code == ErrorCode::Cancelled => (
                OutcomeResult::Cancelled {
                    reason: local
                        .reason
                        .lock()
                        .map_err(|_| fail(ErrorCode::InvalidContract, "agent.cancel"))?
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "cancelled".into()),
                },
                vec![],
            ),
            Err(error) if error.code == ErrorCode::DeadlineExceeded => (
                OutcomeResult::Exhausted {
                    budget: BudgetKind::Elapsed,
                },
                vec![],
            ),
            Err(error) if error.code == ErrorCode::BudgetExceeded => {
                let kind = match error.path.as_str() {
                    "budget.model_calls" => BudgetKind::ModelCalls,
                    "budget.tool_attempts" => BudgetKind::ToolAttempts,
                    "budget.repair_attempts" => BudgetKind::RepairAttempts,
                    "budget.recovery_attempts" => BudgetKind::RecoveryAttempts,
                    _ => BudgetKind::Elapsed,
                };
                (OutcomeResult::Exhausted { budget: kind }, vec![])
            }
            Err(error) => (failed(&enum_name(&error.code)), vec![]),
        };
        self.finish(
            run_id,
            PreparedOutcome {
                result,
                output,
                continuation,
                unresolved_effects: vec![],
                verification: None,
            },
            budget,
            segment,
            local,
        )
        .await
    }

    async fn generate(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        segment: &SegmentBindings,
        budget: &RunBudget,
        lease: &RunLease,
        reuse_step: bool,
    ) -> Result<Guarded<ModelExchangeOutcome>, ContractError> {
        let context = &segment.context;
        let bindings = &self.inner.bindings;
        budget.check_boundary().await?;
        self.collect_sources(ContextTrigger::RunStart, None, segment, budget)
            .await?;
        let run_context = self.before_run(budget, segment).await?;
        let mut snapshot = bindings.state.load(&bindings.scope, run_id).await?.snapshot;
        let expected_revision = snapshot.revision;
        let step = match snapshot.model_step_id.as_ref().filter(|_| reuse_step) {
            Some(step) => step.clone(),
            None => bindings.ids.next_id()?,
        };
        if !reuse_step {
            let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
            snapshot.revision = snapshot
                .revision
                .checked_add(1)
                .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.prepare"))?;
            snapshot.phase = RunPhase::Prepare;
            snapshot.model_step_id = Some(step.clone());
            snapshot.active_prepared_step = None;
            snapshot
                .source_states
                .retain(|state| state.trigger != ContextTrigger::BeforeModel);
            snapshot.usage.elapsed_ms = elapsed;
            snapshot.timing.last_observed_at_ms = now;
            bindings
                .state
                .commit(
                    &bindings.scope,
                    run_id,
                    CommitInput {
                        control_commands: vec![],
                        expected_revision,
                        lease: lease.clone(),
                        now_ms: now,
                        snapshot,
                        messages: vec![],
                        events: vec![],
                        records: vec![],
                    },
                )
                .await?;
        }
        let saved = bindings.state.load(&bindings.scope, run_id).await?;
        let router = bindings.router.snapshot();
        let rule = router
            .policy()
            .rules
            .iter()
            .find(|rule| {
                rule.model_binding == saved.snapshot.profile.profile().model_binding
                    && rule.purpose == ModelPurpose::Agent
            })
            .ok_or_else(|| fail(ErrorCode::ModelRouteDenied, "agent.routing"))?;
        self.collect_sources(
            ContextTrigger::BeforeModel,
            Some(step.clone()),
            segment,
            budget,
        )
        .await?;
        let (source_batch_refs, mut source_items) =
            self.source_context(&step, segment, budget).await?;
        if saved.snapshot.skill_plan_ref.is_some() {
            let skills = bindings
                .skills
                .as_ref()
                .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "agent.skills"))?;
            source_items.extend(
                skills
                    .context_items(&saved.snapshot, context, None, budget.call_deadline()?)
                    .await?,
            );
        }
        source_items.extend(run_context);
        let context_items = self
            .before_model(
                &step,
                saved.snapshot.request.input.clone(),
                source_items,
                segment,
                budget,
            )
            .await?;
        let verification_plan = self.verification_plan(&saved.snapshot).await?;
        let output = match verification_plan.schema {
            Some(schema) => ModelOutput::JsonSchema {
                schema: schema.schema,
            },
            None => ModelOutput::Text {},
        };
        let input = RoutedModelInput {
            model_step_id: step,
            routing: RouteRequest {
                model_binding: saved.snapshot.profile.profile().model_binding.clone(),
                purpose: ModelPurpose::Agent,
                required_capabilities: {
                    let mut required = std::collections::BTreeSet::from([Id::new("text")?]);
                    if !prompt.tools().is_empty() {
                        required.insert(Id::new("tool_calling")?);
                    }
                    if matches!(output, ModelOutput::JsonSchema { .. }) {
                        required.insert(Id::new("json_output")?);
                    }
                    required
                },
                input_tokens: 0,
                max_output_tokens: crate::model_options::output_cap(
                    &saved.snapshot,
                    bindings.settings.max_output_tokens,
                ),
                options: crate::model_options::agent_options(&saved.snapshot),
                scope: bindings.scope.clone(),
                allowed_bindings: std::iter::once(&rule.primary)
                    .chain(&rule.fallbacks)
                    .map(|binding| binding.id.clone())
                    .collect(),
                version_policy: rule.version_policy,
                previous_route: None,
                previous_failure: None,
            },
        };
        let projector = Projector {
            tools: segment.tools.clone(),
            output,
            saved,
            prompt,
            settings: bindings.settings.clone(),
            bindings,
            budget,
            context_runtime: self.inner.context.clone(),
            context_items,
            sources: segment.sources.clone(),
            skills: bindings.skills.clone(),
            artifacts: bindings.artifacts.clone(),
            projected_artifacts: Mutex::new(vec![]),
            projected_lineage: Mutex::new(vec![]),
            source_batch_refs,
        };
        bindings
            .model_exchange
            .generate_routed(
                bindings.router.as_ref(),
                &input,
                &projector,
                context,
                budget,
            )
            .await
    }

    async fn finish(
        &self,
        run_id: &Id,
        candidate: PreparedOutcome,
        budget: &RunBudget,
        segment: &SegmentBindings,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let deadline = self.cleanup_deadline(local)?;
        tokio::time::timeout_at(
            deadline,
            crate::future::boxed(|| self.finish_inner(run_id, candidate, budget, segment, local)),
        )
        .await
        .map_err(|_| {
            fail(
                ErrorCode::PersistenceUnavailable,
                "agent.finalization_timeout",
            )
        })?
    }
    async fn finish_inner(
        &self,
        run_id: &Id,
        candidate: PreparedOutcome,
        budget: &RunBudget,
        segment: &SegmentBindings,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let PreparedOutcome {
            mut result,
            mut output,
            continuation,
            mut unresolved_effects,
            verification,
        } = candidate;
        let bindings = &self.inner.bindings;
        let lease = budget.lease();
        let mut saved = bindings.state.load(&bindings.scope, run_id).await?;
        if saved.snapshot.status.is_terminal() {
            return Ok(());
        }
        let unresolved: BTreeSet<_> = saved
            .snapshot
            .tool_ledger
            .iter()
            .filter_map(|entry| match &entry.state {
                ToolCallState::Unknown {
                    attempt_id,
                    idempotency_key,
                } => Some((attempt_id.clone(), idempotency_key.clone())),
                _ => None,
            })
            .collect();
        if !unresolved.is_empty() {
            let mut after = 0;
            loop {
                let page = bindings
                    .state
                    .read_events(&bindings.scope, run_id, after, MAX_EVENT_PAGE_SIZE)
                    .await?;
                for event in &page.events {
                    if let RunEventPayload::ToolUnresolved {
                        result_ref,
                        attempt_id,
                        idempotency_key,
                    } = &event.payload
                    {
                        if unresolved.contains(&(attempt_id.clone(), idempotency_key.clone()))
                            && !unresolved_effects.contains(result_ref)
                        {
                            unresolved_effects.push(result_ref.clone());
                        }
                    }
                }
                if !page.has_more {
                    break;
                }
                after = page.next_after_seq;
            }
        }
        if output.is_empty() && !matches!(result, OutcomeResult::Succeeded { .. }) {
            output = self.saved_partial_output(&saved.snapshot).await?;
        }
        // Finalization remains possible after cancellation/deadline, but only
        // under the stored lease. A stop during these reads also closes untouched
        // plans; it never invents a result for an uncertain dispatched operation.
        let mut cleaned = false;
        let mut interruption_decision: Option<InterruptionDecisionRecord> = None;
        let mut consumed_control = None;
        let (elapsed, now) = loop {
            tokio::task::yield_now().await;
            let (_, check_at) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
            let current_lease = bindings
                .state
                .check_lease(&bindings.scope, run_id, lease, check_at)
                .await?;
            let (elapsed, now) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
            if now >= current_lease.expires_at_ms {
                return Err(fail(ErrorCode::LeaseLost, "agent.finish"));
            }
            if let Some(command) = self
                .signal_pending_control(run_id, local, Some(now))
                .await?
            {
                consumed_control = Some(command.command_id);
            }
            if let Some(error) = local
                .error
                .lock()
                .map_err(|_| fail(ErrorCode::InvalidContract, "control.error"))?
                .as_ref()
            {
                return Err(error.clone());
            }
            let cancel_reason = local
                .reason
                .lock()
                .map_err(|_| fail(ErrorCode::InvalidContract, "agent.cancel"))?
                .clone();
            if let Some(reason) = &cancel_reason {
                result = OutcomeResult::Cancelled {
                    reason: reason.to_string(),
                };
            } else if elapsed >= saved.snapshot.limits.max_elapsed_ms.get() {
                result = OutcomeResult::Exhausted {
                    budget: BudgetKind::Elapsed,
                };
            } else if interruption_decision.is_none()
                && local.cancel.is_cancelled()
                && !matches!(
                    result,
                    OutcomeResult::Exhausted { .. } | OutcomeResult::Interrupted { .. }
                )
            {
                result = OutcomeResult::Cancelled {
                    reason: "execution_stopped".into(),
                };
            }
            let cause = match &result {
                // An individual wait expiry is not exhaustion of the Run's
                // original deadline and does not invoke interruption policy.
                OutcomeResult::Exhausted {
                    budget: BudgetKind::Elapsed,
                } if elapsed < saved.snapshot.limits.max_elapsed_ms.get() => None,
                OutcomeResult::Exhausted { .. } => Some(InterruptionCause::BudgetExhausted),
                OutcomeResult::Cancelled { .. } if cancel_reason.is_some() => {
                    Some(InterruptionCause::UserCancel)
                }
                OutcomeResult::Cancelled { .. } => Some(
                    local
                        .stop_cause
                        .lock()
                        .map_err(|_| fail(ErrorCode::InvalidContract, "interruption.stop_state"))?
                        .unwrap_or(InterruptionCause::SegmentStopped),
                ),
                _ => None,
            };
            if let Some(decision) = &mut interruption_decision {
                if let Some(cause) = cause.filter(|cause| {
                    matches!(
                        cause,
                        InterruptionCause::UserCancel | InterruptionCause::BudgetExhausted
                    )
                }) {
                    if decision.interruption.cause != cause {
                        decision.interruption.cause = cause;
                        decision.interruption.recoverable = false;
                        decision.action = InterruptionAction::UseDefault;
                        decision.callback_error = Some(Id::new("protected_cause_changed")?);
                    }
                }
            } else if let Some(cause) =
                cause.filter(|_| saved.snapshot.interruption_plan_ref.is_some())
            {
                let decision = crate::future::boxed(|| {
                    self.interruption_decision(&saved, cause, &unresolved_effects)
                })
                .await?;
                result = super::interruption::interruption_result(&decision, &result);
                interruption_decision = Some(decision);
                continue; // Recheck time, ownership and protected causes after the callback.
            }
            if !matches!(
                result,
                OutcomeResult::Succeeded { .. }
                    | OutcomeResult::Waiting { .. }
                    | OutcomeResult::Interrupted { .. }
            ) && saved.snapshot.tool_ledger.iter().any(|entry| {
                matches!(
                    entry.state,
                    ToolCallState::Planned {}
                        | ToolCallState::ApprovalPending { .. }
                        | ToolCallState::InputPending { .. }
                )
            }) {
                if cleaned {
                    return Err(fail(ErrorCode::InvalidTransition, "agent.pending_tools"));
                }
                self.settle_unstarted_tools(
                    &saved.snapshot,
                    segment,
                    budget,
                    matches!(result, OutcomeResult::Cancelled { .. }),
                    local,
                )
                .await?;
                saved = bindings.state.load(&bindings.scope, run_id).await?;
                cleaned = true;
                continue;
            }
            break (elapsed, now);
        };
        let mut snapshot = saved.snapshot;
        let expected_revision = snapshot.revision;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.finish"))?;
        snapshot.last_event_seq = snapshot
            .last_event_seq
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.event"))?;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        snapshot.status = result.status();
        snapshot.phase = match snapshot.status {
            RunStatus::Waiting => RunPhase::Waiting,
            RunStatus::Interrupted => snapshot.phase,
            _ => RunPhase::Finish,
        };
        snapshot.wait = if let OutcomeResult::Waiting { wait } = &result {
            Some(wait.clone())
        } else {
            None
        };
        if let OutcomeResult::Failed { failure } = &mut result {
            let verification_diagnostic =
                if let Some(reference) = snapshot.verification_records.last() {
                    let record: crate::verification::VerificationRecord =
                        self.read_verification(reference).await?;
                    (snapshot.candidate_ref.as_ref() == Some(&record.candidate_ref))
                        .then(|| reference.clone())
                } else {
                    None
                };
            failure.diagnostic_ref = verification_diagnostic.or_else(|| {
                snapshot
                    .model_ledger
                    .last()
                    .and_then(|entry| entry.response_ref.clone())
            });
        }
        let interruption_record = if let Some(mut decision) = interruption_decision {
            decision.interruption.checkpoint_revision = expected_revision;
            decision.interruption.recoverable = matches!(result, OutcomeResult::Interrupted { .. });
            if let OutcomeResult::Interrupted { interruption } = &mut result {
                *interruption = decision.interruption.clone();
            }
            snapshot.app_state = decision.app_state.clone();
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(decision)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "interruption.decision"))?,
            );
            snapshot
                .interruption_records
                .push(record.reference().clone());
            Some(record)
        } else {
            None
        };
        let outcome = RunOutcome {
            app_state: snapshot.app_state.clone(),
            result,
            output: output.clone(),
            artifacts: artifacts::produced(&snapshot),
            usage: snapshot.usage.clone(),
            checkpoint_revision: snapshot.revision,
            verification,
            unresolved_effects,
        };
        let record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(&outcome)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.outcome"))?,
        );
        let wait_record = snapshot
            .wait
            .as_ref()
            .map(|wait| {
                Ok::<_, ContractError>(ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(wait)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.wait"))?,
                ))
            })
            .transpose()?;
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: bindings.ids.next_id()?,
            scope: bindings.scope.clone(),
            run_id: run_id.clone(),
            session_id: snapshot.request.session_id.clone(),
            seq: snapshot
                .last_event_seq
                .try_into()
                .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.event"))?,
            timestamp_ms: now,
            payload: if snapshot.status == RunStatus::Interrupted {
                RunEventPayload::RunInterrupted {
                    outcome_ref: record.reference().clone(),
                    decision_ref: interruption_record
                        .as_ref()
                        .ok_or_else(|| {
                            fail(ErrorCode::InvalidSnapshot, "interruption.decision_missing")
                        })?
                        .reference()
                        .clone(),
                }
            } else if let Some(wait_record) = &wait_record {
                RunEventPayload::RunWaiting {
                    outcome_ref: Some(record.reference().clone()),
                    wait_ref: wait_record.reference().clone(),
                }
            } else {
                RunEventPayload::RunFinished {
                    outcome_ref: record.reference().clone(),
                }
            },
        };
        let mut records = vec![record];
        records.extend(interruption_record);
        records.extend(wait_record);
        let mut content: Vec<_> = output
            .into_iter()
            .map(|content| ContentBlock::Content { content })
            .collect();
        if snapshot.status == RunStatus::Succeeded {
            for continuation in continuation {
                let route = &snapshot
                    .model_ledger
                    .iter()
                    .rev()
                    .find(|entry| entry.purpose == ModelPurpose::Agent)
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.continuation"))?
                    .route;
                if continuation.route_digest() != &route.digest() {
                    return Err(fail(
                        ErrorCode::ModelContextIncompatible,
                        "agent.continuation",
                    ));
                }
                let record = ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(&continuation)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.continuation"))?,
                );
                content.push(ContentBlock::ProviderOpaque {
                    provider: route.provider.clone(),
                    route_digest: route.digest(),
                    data_ref: record.reference().clone(),
                });
                records.push(record);
            }
        }
        let messages = if content.is_empty() || snapshot.status != RunStatus::Succeeded {
            vec![]
        } else {
            vec![Message {
                source_model_request_id: snapshot
                    .model_ledger
                    .iter()
                    .rev()
                    .find(|entry| {
                        entry.purpose == ModelPurpose::Agent
                            && matches!(entry.state, ModelAttemptState::Completed {})
                    })
                    .map(|entry| entry.attempt_id.clone()),
                message_id: bindings.ids.next_id()?,
                run_id: run_id.clone(),
                sequence: saved
                    .session
                    .transcript_revision
                    .checked_add(1)
                    .and_then(NonZeroU64::new)
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "message.sequence"))?,
                role: MessageRole::Assistant,
                content,
                origin: MessageOrigin::Model,
                visibility: Visibility::UserAndModel,
            }]
        };
        snapshot.outcome = Some(outcome);
        bindings
            .state
            .commit(
                &bindings.scope,
                run_id,
                CommitInput {
                    control_commands: consumed_control.into_iter().collect(),
                    expected_revision,
                    lease: lease.clone(),
                    now_ms: now,
                    snapshot,
                    messages,
                    events: vec![event],
                    records,
                },
            )
            .await?;
        local.notify.notify_waiters();
        Ok(())
    }

    async fn saved_partial_output(
        &self,
        snapshot: &RunSnapshot,
    ) -> Result<Vec<InputContent>, ContractError> {
        let Some(step) = &snapshot.model_step_id else {
            return Ok(vec![]);
        };
        let Some(invocation) = snapshot.model_ledger.iter().rev().find(|invocation| {
            invocation.purpose == ModelPurpose::Agent
                && &invocation.model_step_id == step
                && invocation.run_id == snapshot.run_id
                && invocation.response_ref.is_some()
        }) else {
            return Ok(vec![]);
        };
        let reference = invocation
            .response_ref
            .as_ref()
            .expect("filtered response reference");
        let record = self
            .inner
            .bindings
            .state
            .read_record(&snapshot.scope, reference)
            .await?;
        if record.reference() != reference {
            return Err(fail(ErrorCode::InvalidSnapshot, "agent.partial_response"));
        }
        let response: StoredModelResponse = serde_json::from_value(record.value().clone())
            .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.partial_response"))?;
        if response.request_id != invocation.attempt_id
            || response.route_digest != invocation.route.digest()
        {
            return Err(fail(ErrorCode::InvalidSnapshot, "agent.partial_response"));
        }
        let text = match response.outcome {
            ModelExchangeOutcome::Completed { response } => response.text,
            ModelExchangeOutcome::Failed { failure } => failure.partial_text().to_owned(),
        };
        Ok(if text.is_empty() {
            vec![]
        } else {
            vec![InputContent::Text { text }]
        })
    }
}

pub(super) struct PreparedOutcome {
    pub result: OutcomeResult,
    pub output: Vec<InputContent>,
    pub continuation: Vec<OpaqueContinuation>,
    pub unresolved_effects: Vec<RecordRef>,
    pub verification: Option<VerificationSummary>,
}

struct Projector<'a> {
    tools: Arc<ToolRegistry>,
    output: ModelOutput,
    saved: StoredRun,
    prompt: PromptSnapshot,
    settings: AgentSettings,
    bindings: &'a AgentBindings,
    budget: &'a RunBudget,
    context_runtime: Arc<ContextRuntime>,
    context_items: Vec<ContextItem>,
    sources: Option<Arc<ContextSourceRuntime>>,
    source_batch_refs: Vec<RecordRef>,
    skills: Option<Arc<SkillRuntime>>,
    artifacts: Option<Arc<ArtifactRuntime>>,
    projected_artifacts: Mutex<Vec<ArtifactRef>>,
    projected_lineage: Mutex<Vec<ContextLineage>>,
}
impl ModelRequestProjector for Projector<'_> {
    fn authorize_prepared<'a>(
        &'a self,
        prepared: &'a PreparedModelProjection,
        selection: &'a RouteSelection,
        input: &'a RoutedModelInput,
        context: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            *self
                .projected_lineage
                .lock()
                .map_err(|_| fail(ErrorCode::InvalidContract, "agent.lineage"))? =
                prepared.provenance.source_lineage.clone();
            let mut known = prepared.provenance.artifacts.clone();
            let mut current = self.saved.snapshot.context_revision_ref.clone();
            let mut seen = std::collections::BTreeSet::new();
            while let Some(reference) = current {
                if !seen.insert((reference.record_id.clone(), reference.revision)) {
                    return Err(fail(ErrorCode::InvalidSnapshot, "agent.context_cycle"));
                }
                let record = self
                    .bindings
                    .state
                    .read_record(&context.scope, &reference)
                    .await?;
                let revision: ContextRevision = serde_json::from_value(record.value().clone())
                    .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.context_revision"))?;
                known.extend(crate::prepared_step::revision_artifacts(&revision));
                current = revision.parent;
            }
            let mut artifacts = artifacts::selected(&prepared.request, &self.saved, &known)?;
            for reference in &prepared.provenance.artifacts {
                if !artifacts.contains(reference) {
                    artifacts.push(reference.clone());
                }
            }
            *self
                .projected_artifacts
                .lock()
                .map_err(|_| fail(ErrorCode::InvalidContract, "agent.artifacts"))? = artifacts;
            self.authorize_use(selection, input, context).await
        })
    }

    fn authorize_use<'a>(
        &'a self,
        selection: &'a RouteSelection,
        _input: &'a RoutedModelInput,
        context: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let deadline = context.deadline;
            let current = ExecutionContext::new(
                ExecutionContextData {
                    scope: context.scope.clone(),
                    principal_ref: context.principal_ref.clone(),
                    capability_grant_ref: context.capability_grant_ref.clone(),
                    trace_context: None,
                    system_inputs: None,
                },
                context.cancellation.clone(),
            );
            if let Some(sources) = &self.sources {
                sources
                    .authorize_use(
                        &self.saved.snapshot.run_id,
                        &self.source_batch_refs,
                        Some(&selection.route),
                        &current,
                        deadline,
                    )
                    .await?;
            }
            let lineage = self
                .projected_lineage
                .lock()
                .map_err(|_| fail(ErrorCode::InvalidContract, "agent.lineage"))?
                .clone();
            if !lineage.is_empty() {
                let sources = self
                    .sources
                    .as_ref()
                    .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "agent.lineage_source"))?;
                crate::future::boxed(|| {
                    sources.authorize_lineage(
                        &self.saved.snapshot.run_id,
                        &lineage,
                        Some(&selection.route),
                        &current,
                        deadline,
                    )
                })
                .await?;
            }
            if self.saved.snapshot.skill_plan_ref.is_some() {
                let skills = self
                    .skills
                    .as_ref()
                    .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "agent.skills"))?;
                skills
                    .context_items(
                        &self.saved.snapshot,
                        &current,
                        Some(&selection.route),
                        deadline,
                    )
                    .await?;
            }
            let references = self
                .projected_artifacts
                .lock()
                .map_err(|_| fail(ErrorCode::InvalidContract, "agent.artifacts"))?
                .clone();
            if !references.is_empty() {
                let artifacts = self
                    .artifacts
                    .as_ref()
                    .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "agent.artifact_store"))?;
                for reference in &references {
                    artifacts.stat(reference, &current, Some(deadline)).await?;
                }
            }
            Ok(())
        })
    }
    fn project<'a>(
        &'a self,
        selection: &'a RouteSelection,
        input: &'a RoutedModelInput,
        context: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ProjectedModelRequest> {
        Box::pin(async move {
            if context.cancellation.is_cancelled() {
                return Err(fail(ErrorCode::Cancelled, "agent.projection"));
            }
            self.projected_artifacts
                .lock()
                .map_err(|_| fail(ErrorCode::InvalidContract, "agent.artifacts"))?
                .clear();
            self.authorize_use(selection, input, context).await?;
            let request_message = self
                .saved
                .messages
                .iter()
                .find(|message| {
                    message.run_id == self.saved.snapshot.run_id
                        && message.role == MessageRole::User
                })
                .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.request_message"))?;
            let target = ProviderToolTarget::for_route(&selection.route);
            let mut tool_set = vec![];
            let mut compiled_tools = vec![];
            for manifest in self.prompt.tools() {
                let tool = &self
                    .tools
                    .get(&manifest.model_tool.name)
                    .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "agent.prepared_tool"))?
                    .compiled;
                tool_set.push(ResolvedToolSetEntry::new(manifest.clone(), tool)?);
                compiled_tools.push(CompiledToolContract::compile(
                    tool,
                    target.clone(),
                    context.tool_schema_compiler.as_deref().ok_or_else(|| {
                        fail(ErrorCode::InvalidConfiguration, "agent.schema_compiler")
                    })?,
                    ProviderToolSchemaLimits::default(),
                )?);
            }
            let seed = ProjectionInput {
                tool_contracts: &compiled_tools,
                profile: &self.saved.snapshot.profile,
                scope: &context.scope,
                run_id: &self.saved.snapshot.run_id,
                model_step_id: &input.model_step_id,
                current_request: &self.saved.snapshot.request,
                current_request_message_id: &request_message.message_id,
                transcript: &self.saved.messages,
                context_items: &self.context_items,
                opaque_records: &[],
                expected_prompt_digest: &self.saved.session.prompt_snapshot.digest,
                request_id: input.model_step_id.clone(),
                purpose: input.routing.purpose,
                route: selection.route.clone(),
                output: self.output.clone(),
                max_output_tokens: context.configuration.max_output_tokens,
                options: context.configuration.effective.clone(),
                response_limits: self.settings.response_limits.clone(),
                limits: self.settings.projection_limits,
            };
            let current = ExecutionContext::new(
                ExecutionContextData {
                    scope: context.scope.clone(),
                    principal_ref: context.principal_ref.clone(),
                    capability_grant_ref: context.capability_grant_ref.clone(),
                    trace_context: None,
                    system_inputs: None,
                },
                context.cancellation.clone(),
            );
            // Preparation can nest another routed model call for compaction.
            // Construct its future outside this projector's poll stack frame.
            let prepared = crate::future::boxed(|| {
                self.context_runtime.prepare(
                    &self.prompt,
                    seed,
                    crate::context_strategy::ContextServices {
                        sources: self.sources.as_deref(),
                        bindings: self.bindings,
                        budget: self.budget,
                        context: &current,
                    },
                )
            })
            .await?;
            *self
                .projected_lineage
                .lock()
                .map_err(|_| fail(ErrorCode::InvalidContract, "agent.lineage"))? =
                prepared.lineage.clone();
            let mut references = artifacts::selected(
                &prepared.projection.request,
                &self.saved,
                &prepared.artifacts,
            )?;
            for reference in prepared.artifacts {
                if !references.contains(&reference) {
                    references.push(reference);
                }
            }
            *self
                .projected_artifacts
                .lock()
                .map_err(|_| fail(ErrorCode::InvalidContract, "agent.artifacts"))? = references;
            let mut fragments = vec![];
            for reference in &self.source_batch_refs {
                let record = self
                    .bindings
                    .state
                    .read_record(&context.scope, reference)
                    .await?;
                if let Some(value) = record.value().get("fragments") {
                    fragments.extend(
                        serde_json::from_value::<Vec<ContextFragment>>(value.clone()).map_err(
                            |_| fail(ErrorCode::InvalidSnapshot, "agent.prepared_fragments"),
                        )?,
                    );
                }
            }
            let provenance = ProjectionProvenance {
                context_revision_ref: None, // Stamped from the committed snapshot by ModelExchange.
                through_sequence: self.saved.session.transcript_revision,
                source_batches: self.source_batch_refs.clone(),
                source_lineage: prepared.lineage,
                artifacts: self
                    .projected_artifacts
                    .lock()
                    .map_err(|_| fail(ErrorCode::InvalidContract, "agent.artifacts"))?
                    .clone(),
                fragments,
                selected_message_ids: prepared.projection.selected_message_ids,
                dropped_message_ids: prepared.projection.dropped_message_ids,
                selected_context_ids: prepared.projection.selected_context_ids,
                dropped_context_ids: prepared.projection.dropped_context_ids,
            };
            Ok(ProjectedModelRequest {
                tool_set,
                compiled_tools,
                provenance,
                request: prepared.projection.request,
                input_tokens: prepared.input_tokens,
            })
        })
    }
}
pub(super) fn enum_name(value: &impl serde::Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "invalid_contract".into())
}
pub(super) fn failed(code: &str) -> OutcomeResult {
    OutcomeResult::Failed {
        failure: Failure {
            code: Id::new(code).expect("nonempty static classification"),
            diagnostic_ref: None,
        },
    }
}
```

## `crates/wickle/src/agent/tools.rs`

```rust
use super::*;

impl Agent {
    /// Charge at most once per rejected Tool round, including recovery after reservation.
    pub(super) async fn reserve_tool_repair(
        &self,
        request_id: &Id,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let saved = self
            .inner
            .bindings
            .state
            .load(budget.scope(), budget.run_id())
            .await?;
        let invalid = saved.snapshot.tool_ledger.iter().any(|entry| {
            &entry.call.model_request_id == request_id && crate::budget::needs_tool_repair(entry)
        });
        let reserved = saved.snapshot.reservations.iter().any(|reservation| matches!(&reservation.kind, ReservationKind::ToolRepair { model_request_id } if model_request_id == request_id));
        if invalid && !reserved {
            budget
                .reserve(ReservationKind::ToolRepair {
                    model_request_id: request_id.clone(),
                })
                .await?;
        }
        Ok(())
    }

    pub(super) async fn tool_round(
        &self,
        budget: &RunBudget,
        segment: &SegmentBindings,
    ) -> Result<SerialToolRound, ContractError> {
        let saved = self
            .inner
            .bindings
            .state
            .load(budget.scope(), budget.run_id())
            .await?;
        self.tool_round_for_snapshot(&saved, segment).await
    }

    pub(super) async fn tool_round_for_snapshot(
        &self,
        saved: &StoredRun,
        segment: &SegmentBindings,
    ) -> Result<SerialToolRound, ContractError> {
        let bindings = &self.inner.bindings;
        let registry = segment.tools.clone();
        let definitions = if let Some(reference) = &saved.snapshot.system_inputs {
            let record = bindings
                .state
                .read_record(&saved.snapshot.scope, &reference.snapshot_ref)
                .await?;
            let inputs =
                RunSystemInputs::from_value(record.value(), reference, &saved.snapshot.scope)?;
            SystemInputRegistry::new(inputs.definitions().values().cloned().collect())?
        } else {
            bindings.system_inputs.clone()
        };
        let binder = Arc::new(InputBinder::new(
            Arc::new(definitions),
            bindings.system_input_resolver.clone(),
            bindings.policy.clone(),
            bindings.ids.clone(),
        ));
        let mut round = SerialToolRound::new(
            registry,
            binder,
            bindings.policy.clone(),
            bindings.ids.clone(),
        )
        .with_limits(bindings.settings.tool_execution_limits)?;
        if let Some(hooks) = &segment.hooks {
            round = round.with_hooks(hooks.clone());
        }
        if let Some(artifacts) = &bindings.artifacts {
            round = round.with_artifacts(artifacts.clone());
        }
        if saved.snapshot.skill_plan_ref.is_some() {
            let skills = bindings
                .skills
                .as_ref()
                .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "agent.skills"))?;
            round = round.with_skill_plan(skills.saved_plan(&saved.snapshot).await?);
        }
        if let Some(binding_set_id) = segment.binding_set_id() {
            round = round.with_binding_set_id(binding_set_id.clone());
        }
        Ok(round)
    }

    /// Commit the original complete model plan before any resolver or tool runs.
    pub(super) async fn plan_tools(
        &self,
        response: &ModelResponse,
        prompt: &PromptSnapshot,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        budget.check_boundary().await?;
        let bindings = &self.inner.bindings;
        let saved = bindings.state.load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        if response.finish != ModelFinish::ToolCalls
            || response.tool_calls.is_empty()
            || snapshot
                .tool_ledger
                .iter()
                .any(|entry| entry.call.model_request_id == response.request_id)
        {
            return Err(fail(ErrorCode::InvalidTransition, "agent.tool_plan"));
        }
        let invocation = snapshot
            .model_ledger
            .iter()
            .find(|invocation| {
                invocation.attempt_id == response.request_id
                    && matches!(invocation.state, ModelAttemptState::Completed {})
            })
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.tool_response"))?;
        if invocation.route.digest() != response.route_digest {
            return Err(fail(ErrorCode::ModelRoutingMismatch, "agent.tool_response"));
        }
        let mut prepared_tools = Vec::new();
        if let Some(reference) = &invocation.prepared_step_ref {
            let root_record = bindings
                .state
                .read_record(budget.scope(), reference)
                .await?;
            let root: PreparedStepRecord = serde_json::from_value(root_record.value().clone())
                .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.prepared_step"))?;
            let tool_record = bindings
                .state
                .read_record(budget.scope(), &root.tool_set)
                .await?;
            let tool_set: ResolvedToolSet = serde_json::from_value(tool_record.value().clone())
                .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.prepared_tool_set"))?;
            tool_set.validate_shape()?;
            if tool_set.entries.len() != root.compiled_tools.len() {
                return Err(fail(ErrorCode::InvalidSnapshot, "agent.prepared_tool_set"));
            }
            let target = ProviderToolTarget::for_route(&invocation.route);
            for (entry, reference) in tool_set.entries.into_iter().zip(root.compiled_tools) {
                if !prompt.tools().contains(&entry.manifest) {
                    return Err(fail(ErrorCode::InvalidSnapshot, "agent.prepared_manifest"));
                }
                let tool = entry.restore_tool()?;
                let record = bindings
                    .state
                    .read_record(budget.scope(), &reference)
                    .await?;
                let digest: JsonDigest =
                    serde_json::from_value(record.value()["digest"].clone())
                        .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.prepared_contract"))?;
                let contract = CompiledToolContract::restore(
                    &record.value().to_string(),
                    &tool,
                    &target,
                    &digest,
                    ProviderToolSchemaLimits::default(),
                )?;
                prepared_tools.push((entry.manifest, contract, reference));
            }
        }
        let provider = invocation.route.provider.clone();
        let route_digest = invocation.route.digest();
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let mut content = Vec::new();
        if !response.text.is_empty() {
            content.push(ContentBlock::Content {
                content: InputContent::Text {
                    text: response.text.clone(),
                },
            });
        }
        let mut records = vec![];
        let mut events = vec![];
        for proposed in &response.tool_calls {
            let prepared = prepared_tools
                .iter()
                .find(|(_, contract, _)| contract.wire_tool().name == proposed.name);
            let (name, model_inputs, descriptor_digest, contract_ref) =
                if let Some((manifest, contract, reference)) = prepared {
                    let raw = proposed.raw_arguments.clone().unwrap_or_else(|| {
                        serde_json::to_string(&proposed.model_inputs).expect("model arguments")
                    });
                    let decoded = match contract
                        .decode_arguments(&raw, ProviderToolSchemaLimits::default())
                    {
                        Ok(value) => value,
                        Err(error) if error.code == ErrorCode::InvalidArguments => {
                            JsonObject::new()
                        }
                        Err(error) => return Err(error),
                    };
                    (
                        contract.canonical_name().clone(),
                        decoded,
                        Some(manifest.descriptor_digest.clone()),
                        Some(reference.clone()),
                    )
                } else {
                    let digest = if invocation.prepared_step_ref.is_none() {
                        prompt
                            .tools()
                            .iter()
                            .find(|tool| tool.model_tool.name == proposed.name)
                            .map(|tool| tool.descriptor_digest.clone())
                    } else {
                        None
                    };
                    (
                        proposed.name.clone(),
                        proposed.model_inputs.clone(),
                        digest,
                        None,
                    )
                };
            let call = ToolCall {
                provider_arguments: proposed.raw_arguments.as_ref().map(|raw| {
                    ProviderToolArguments {
                        name: proposed.name.clone(),
                        raw: raw.clone(),
                        compiled_contract_ref: contract_ref,
                    }
                }),
                call_id: bindings.ids.next_id()?,
                model_request_id: response.request_id.clone(),
                provider_call_id: proposed.provider_call_id.clone(),
                tool_name: name,
                model_inputs,
                descriptor_digest,
                bound_input_ref: None,
            };
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(&call)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.tool_plan"))?,
            );
            snapshot.last_event_seq = snapshot
                .last_event_seq
                .checked_add(1)
                .ok_or_else(|| fail(ErrorCode::InvalidEvent, "agent.tool_plan"))?;
            events.push(RunEvent {
                schema_version: RunEventSchemaVersion::V1,
                event_id: bindings.ids.next_id()?,
                scope: budget.scope().clone(),
                run_id: budget.run_id().clone(),
                session_id: snapshot.request.session_id.clone(),
                seq: snapshot
                    .last_event_seq
                    .try_into()
                    .map_err(|_| fail(ErrorCode::InvalidEvent, "agent.tool_plan"))?,
                timestamp_ms: now,
                payload: RunEventPayload::ToolPlanned {
                    call_ref: record.reference().clone(),
                },
            });
            records.push(record);
            content.push(ContentBlock::ToolCall { call: call.clone() });
            snapshot.tool_ledger.push(ToolLedgerEntry {
                call,
                state: ToolCallState::Planned {},
            });
        }
        for continuation in &response.continuation {
            if continuation.route_digest() != &route_digest {
                return Err(fail(
                    ErrorCode::ModelContextIncompatible,
                    "agent.continuation",
                ));
            }
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(continuation)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.continuation"))?,
            );
            content.push(ContentBlock::ProviderOpaque {
                provider: provider.clone(),
                route_digest: route_digest.clone(),
                data_ref: record.reference().clone(),
            });
            records.push(record);
        }
        let message = Message {
            source_model_request_id: Some(response.request_id.clone()),
            message_id: bindings.ids.next_id()?,
            run_id: budget.run_id().clone(),
            sequence: saved
                .session
                .transcript_revision
                .checked_add(1)
                .and_then(NonZeroU64::new)
                .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.tool_plan"))?,
            role: MessageRole::Assistant,
            content,
            origin: MessageOrigin::Model,
            visibility: Visibility::UserAndModel,
        };
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.tool_plan"))?;
        snapshot.phase = RunPhase::Tool;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        bindings
            .state
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    control_commands: vec![],
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages: vec![message],
                    events,
                    records,
                },
            )
            .await?;
        Ok(())
    }

    pub(super) async fn tool_wait(
        &self,
        outcome: ToolRoundOutcome,
        budget: &RunBudget,
    ) -> Result<(WaitState, Vec<RecordRef>), ContractError> {
        let bindings = &self.inner.bindings;
        let (target, unresolved) = match outcome {
            ToolRoundOutcome::ApprovalRequired {
                call_id,
                binding_digest,
                ..
            } => (
                WaitTarget::Approval {
                    target: ApprovalTarget::Tool {
                        call_id,
                        binding_digest,
                    },
                },
                vec![],
            ),
            ToolRoundOutcome::Unresolved {
                call_id,
                result_ref,
            } => {
                let saved = bindings.state.load(budget.scope(), budget.run_id()).await?;
                let entry = saved
                    .snapshot
                    .tool_ledger
                    .iter()
                    .find(|entry| entry.call.call_id == call_id)
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.unresolved_tool"))?;
                let ToolCallState::Unknown {
                    idempotency_key, ..
                } = &entry.state
                else {
                    return Err(fail(ErrorCode::InvalidTransition, "agent.unresolved_tool"));
                };
                (
                    WaitTarget::External {
                        call_id,
                        effect_key: idempotency_key.clone(),
                    },
                    vec![result_ref],
                )
            }
            ToolRoundOutcome::InputRequired { request } => (WaitTarget::Input { request }, vec![]),
            ToolRoundOutcome::Completed => {
                return Err(fail(ErrorCode::InvalidTransition, "agent.tool_wait"));
            }
        };
        Ok((
            WaitState {
                wait_id: bindings.ids.next_id()?,
                target,
                expires_at_ms: Some(
                    bindings
                        .state
                        .load(budget.scope(), budget.run_id())
                        .await?
                        .snapshot
                        .timing
                        .deadline_at_ms,
                ),
            },
            unresolved,
        ))
    }

    pub(super) async fn settle_unstarted_tools(
        &self,
        snapshot: &RunSnapshot,
        segment: &SegmentBindings,
        budget: &RunBudget,
        cancelled: bool,
        local: &LocalRun,
    ) -> Result<(), ContractError> {
        let requests: std::collections::BTreeSet<_> = snapshot
            .tool_ledger
            .iter()
            .filter(|entry| {
                matches!(
                    entry.state,
                    ToolCallState::Planned {}
                        | ToolCallState::ApprovalPending { .. }
                        | ToolCallState::InputPending { .. }
                )
            })
            .map(|entry| entry.call.model_request_id.clone())
            .collect();
        if requests.is_empty() {
            return Ok(());
        }
        let round = self.tool_round(budget, segment).await?;
        for request in requests {
            round
                .settle_unstarted(
                    &request,
                    if cancelled {
                        ToolResultStatus::Cancelled
                    } else {
                        ToolResultStatus::Failed
                    },
                    Id::new(if cancelled {
                        "cancelled"
                    } else {
                        "run_stopped"
                    })?,
                    &segment.context,
                    budget,
                )
                .await?;
            self.remember_observer_error(local, round.observer_error());
        }
        Ok(())
    }
}
```

## `crates/wickle/src/context_projection.rs`

```rust
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, io,
    num::NonZeroU64,
};

use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Value, json};

use crate::{
    AgentProfile, CompiledTool, ComponentKind, ContentBlock, ContractError, ErrorCode, Id,
    InputContent, Instructions, JsonDigest, JsonObject, Message, MessageOrigin, MessageRole,
    ModelContent, ModelMessage, ModelOutput, ModelPurpose, ModelRequest, ModelResponseLimits,
    ModelRole, ModelTool, OpaqueContinuation, RecordRef, ResolvedComponent, ResolvedModelRoute,
    ResolvedProfile, RunRequest, Scope, ToolBindingRef, ToolResultStatus, VersionedRef, Visibility,
    parse_json, serialization::data_digest,
};

/// Version of the session prefix and byte-bounded projection contract.
pub const CONTEXT_ASSEMBLER_VERSION: &str = "wickle.context-assembler.v1";

/// Instruction data already resolved and authorized by the Host; no loader is invoked here.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstructionAssetContent {
    /// Exact instruction asset selected by the profile.
    pub asset: VersionedRef,
    /// Complete text to pin; it is never silently truncated.
    pub text: String,
}

impl fmt::Debug for InstructionAssetContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InstructionAssetContent")
            .field("asset", &self.asset)
            .finish_non_exhaustive()
    }
}

/// A trusted assembly's mapping from a selected profile reference to its compiled tool.
/// The later adapter factory must attest that an export actually supplies this descriptor.
#[derive(Debug, Clone)]
pub struct PromptToolBinding {
    /// Exact selected catalog reference or adapter export, including alias/configuration.
    pub selection: ToolBindingRef,
    /// Validated immutable input split; its full schema is not copied into the prefix.
    pub compiled: CompiledTool,
}

/// Initial skill listing metadata, deliberately separate from skill body loading.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillManifest {
    /// Exact selected skill identity and version.
    pub skill: VersionedRef,
    /// Short public listing name.
    pub name: String,
    /// Public purpose description, not an automatically executed instruction body.
    pub description: String,
    /// Trusted catalog manifest identity pinned with this listing.
    pub manifest_digest: JsonDigest,
}

impl fmt::Debug for SkillManifest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SkillManifest")
            .field("skill", &self.skill)
            .field("manifest_digest", &self.manifest_digest)
            .finish_non_exhaustive()
    }
}

/// Model-facing part of a tool pinned into the session prefix.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedPromptTool {
    /// Exact profile selection, retaining alias and binding identity.
    pub selection: ToolBindingRef,
    /// Exact underlying tool descriptor identity.
    pub tool: VersionedRef,
    /// Compiler contract used for input projection.
    pub compiler_version: String,
    /// Full compiled input-contract digest, without its hidden schemas or values.
    pub compiled_digest: JsonDigest,
    /// Original descriptor identity used by stored core ToolCall records.
    pub descriptor_digest: JsonDigest,
    /// Identity of the derived model-input schema.
    pub model_schema_digest: JsonDigest,
    /// Only the model-visible tool schema and public description.
    pub model_tool: ModelTool,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PromptData {
    assembler_version: String,
    scope: Scope,
    profile: AgentProfile,
    initial_resolution_digest: JsonDigest,
    pinned_components: Vec<ResolvedComponent>,
    host_instructions: Vec<String>,
    profile_asset: Option<InstructionAssetContent>,
    tools: Vec<PinnedPromptTool>,
    skills: Vec<SkillManifest>,
}

/// Owned session prefix. It can be serialized for protected storage but cannot be
/// deserialized without verifying a trusted expected digest, scope and profile.
/// Its digest equals the digest of the serialized value stored by ProtectedRecord.
#[derive(Clone)]
pub struct PromptSnapshot {
    data: PromptData,
    digest: JsonDigest,
}

impl Serialize for PromptSnapshot {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.data.serialize(serializer)
    }
}
impl fmt::Debug for PromptSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PromptSnapshot")
            .field("digest", &self.digest)
            .field("tool_count", &self.data.tools.len())
            .field("skill_count", &self.data.skills.len())
            .finish_non_exhaustive()
    }
}

impl PromptSnapshot {
    /// Pin already-authorized assets in profile order. This does not create adapter
    /// factories, fetch instructions, or load skill bodies. Profile text cannot
    /// delete or replace the independently owned Host message. Actual instruction
    /// adherence within a provider's system channel still requires evaluation;
    /// execution permissions are enforced separately by PolicyGate.
    pub fn create(
        profile: &ResolvedProfile,
        host_instructions: Vec<String>,
        profile_asset: Option<InstructionAssetContent>,
        mut tools: Vec<PromptToolBinding>,
        mut skills: Vec<SkillManifest>,
    ) -> Result<Self, ContractError> {
        if tools.len() != profile.profile().tools.len()
            || skills.len() != profile.profile().skills.len()
        {
            return Err(invalid("prompt.selections"));
        }
        let mut pinned_tools = Vec::new();
        for selection in &profile.profile().tools {
            let index = tools
                .iter()
                .position(|binding| &binding.selection == selection)
                .ok_or_else(|| invalid("prompt.tools"))?;
            let binding = tools.remove(index);
            let mut model_tool = binding.compiled.to_model_tool();
            if let ToolBindingRef::Export(export) = selection {
                if let Some(alias) = &export.alias {
                    model_tool.name = alias.clone();
                }
            }
            pinned_tools.push(PinnedPromptTool {
                selection: selection.clone(),
                tool: binding.compiled.descriptor().tool.clone(),
                compiler_version: binding.compiled.compiler_version().into(),
                compiled_digest: binding.compiled.digest().clone(),
                descriptor_digest: binding.compiled.descriptor_digest().clone(),
                model_schema_digest: binding.compiled.model_schema_digest().clone(),
                model_tool,
            });
        }
        let mut pinned_skills = Vec::new();
        for selection in &profile.profile().skills {
            let index = skills
                .iter()
                .position(|manifest| {
                    manifest.skill.id == selection.skill_id
                        && manifest.skill.version == selection.version
                })
                .ok_or_else(|| invalid("prompt.skills"))?;
            pinned_skills.push(skills.remove(index));
        }
        let data = PromptData {
            assembler_version: CONTEXT_ASSEMBLER_VERSION.into(),
            scope: profile.scope().clone(),
            profile: profile.profile().clone(),
            initial_resolution_digest: profile.resolution_digest().clone(),
            pinned_components: non_model_components(profile),
            host_instructions,
            profile_asset,
            tools: pinned_tools,
            skills: pinned_skills,
        };
        let snapshot = Self {
            digest: data_digest(&data),
            data,
        };
        snapshot.validate_data()?;
        Ok(snapshot)
    }

    /// Canonical identity of the exact protected serialized prefix.
    pub fn digest(&self) -> JsonDigest {
        self.digest.clone()
    }
    /// Read pinned public tool metadata and identities, without hidden input schemas.
    pub fn tools(&self) -> &[PinnedPromptTool] {
        &self.data.tools
    }
    /// Read the original selected skill listings, without fetching newer versions.
    pub fn skills(&self) -> &[SkillManifest] {
        &self.data.skills
    }
    /// Read the authenticated scope in which this prefix was pinned.
    pub fn scope(&self) -> &Scope {
        &self.data.scope
    }

    /// Restore a protected record using its trusted digest and the current run's
    /// resolved profile. A new run may resolve a different model binding only.
    /// Resume must continue to use the original run's profile and selected route;
    /// this method is not an authorization to replace either during a run.
    pub fn restore(
        input: &str,
        expected_digest: &JsonDigest,
        profile: &ResolvedProfile,
        scope: &Scope,
    ) -> Result<Self, ContractError> {
        let data: PromptData =
            serde_json::from_value(parse_json(input).map_err(|_| invalid("prompt"))?)
                .map_err(|_| invalid("prompt"))?;
        let snapshot = Self {
            digest: data_digest(&data),
            data,
        };
        snapshot.validate_for(profile, scope, expected_digest)?;
        Ok(snapshot)
    }

    /// Require the stored prefix identity, scope, profile, and all non-model assets.
    pub fn validate_for(
        &self,
        profile: &ResolvedProfile,
        scope: &Scope,
        expected_digest: &JsonDigest,
    ) -> Result<(), ContractError> {
        if &self.digest != expected_digest
            || &self.data.scope != scope
            || profile.scope() != scope
            || self.data.profile.digest() != *profile.profile_digest()
            || self.data.pinned_components != non_model_components(profile)
        {
            return Err(mismatch("prompt"));
        }
        self.validate_data()
    }

    fn validate_data(&self) -> Result<(), ContractError> {
        if self.data.assembler_version != CONTEXT_ASSEMBLER_VERSION
            || data_digest(&self.data) != self.digest
        {
            return Err(mismatch("prompt.version"));
        }
        match (&self.data.profile.instructions, &self.data.profile_asset) {
            (Instructions::Text(_), None) => {}
            (Instructions::Asset(reference), Some(asset)) if reference.asset_ref == asset.asset => {
            }
            _ => return Err(mismatch("prompt.instructions")),
        }
        if self.data.tools.len() != self.data.profile.tools.len()
            || self.data.skills.len() != self.data.profile.skills.len()
        {
            return Err(mismatch("prompt.selections"));
        }
        let mut names = BTreeSet::new();
        for (selection, tool) in self.data.profile.tools.iter().zip(&self.data.tools) {
            if selection != &tool.selection
                || !names.insert(&tool.model_tool.name)
                || crate::canonical_digest(&tool.model_tool.model_input_schema)
                    != tool.model_schema_digest
            {
                return Err(mismatch("prompt.tools"));
            }
            match selection {
                ToolBindingRef::Catalog(reference) => {
                    if tool.tool.id != reference.tool_id || tool.tool.version != reference.version {
                        return Err(mismatch("prompt.tools"));
                    }
                }
                ToolBindingRef::Export(export) => {
                    let adapter = self
                        .data
                        .profile
                        .adapters
                        .as_ref()
                        .and_then(|adapters| {
                            adapters
                                .iter()
                                .find(|adapter| adapter.binding_id == export.adapter_binding)
                        })
                        .ok_or_else(|| mismatch("prompt.export"))?;
                    if !self.data.pinned_components.iter().any(|component| {
                        component.reference.kind == ComponentKind::Adapter
                            && component.reference.id == adapter.adapter_id
                            && component.reference.version.as_ref() == Some(&adapter.version)
                    }) || export
                        .alias
                        .as_ref()
                        .is_some_and(|alias| alias != &tool.model_tool.name)
                    {
                        return Err(mismatch("prompt.export"));
                    }
                }
            }
        }
        for (selected, manifest) in self.data.profile.skills.iter().zip(&self.data.skills) {
            if selected.skill_id != manifest.skill.id || selected.version != manifest.skill.version
            {
                return Err(mismatch("prompt.skills"));
            }
        }
        Ok(())
    }

    fn prefix(&self) -> Vec<ModelMessage> {
        let profile_text = match &self.data.profile.instructions {
            Instructions::Text(instructions) => instructions.text.clone(),
            Instructions::Asset(_) => self
                .data
                .profile_asset
                .as_ref()
                .expect("validated instruction asset")
                .text
                .clone(),
        };
        let mut messages = vec![
            ModelMessage {
                role: ModelRole::System,
                content: self
                    .data
                    .host_instructions
                    .iter()
                    .map(|text| ModelContent::Text { text: text.clone() })
                    .collect(),
            },
            ModelMessage {
                role: ModelRole::System,
                content: vec![ModelContent::Text { text: profile_text }],
            },
        ];
        if !self.data.skills.is_empty() {
            messages.push(ModelMessage {
                role: ModelRole::User,
                content: vec![ModelContent::Json {
                    value: json!({"kind":"available_skills", "skills":self.data.skills}),
                }],
            });
        }
        messages
    }
}

fn non_model_components(profile: &ResolvedProfile) -> Vec<ResolvedComponent> {
    profile
        .components()
        .iter()
        .filter(|component| component.reference.kind != ComponentKind::ModelBinding)
        .cloned()
        .collect()
}

/// Source classification of already-authorized context data. None grants system authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextOrigin {
    /// Validated summary of stored conversation, never a System instruction.
    Compaction,
    /// Additional user-provided context, distinct from the preserved original request.
    User,
    /// Data associated with a selected pinned skill; no loader runs here.
    Skill,
    /// Data associated with a selected tool.
    Tool,
    /// External retrieved data, not trusted instructions.
    Retrieval,
    /// Recalled memory, not a policy grant.
    Memory,
    /// Verification feedback, not a Host instruction replacement.
    Verification,
    /// Bounded data added by a selected lifecycle hook; it carries no system authority.
    Hook,
}

/// Scope of context lifetime. An item outside its lifetime is explicitly omitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextLifetime {
    /// Context valid throughout one session.
    Session {
        /// Owning session.
        session_id: Id,
    },
    /// Context valid during one run.
    Run {
        /// Owning run.
        run_id: Id,
    },
    /// Context valid only for one logical model step.
    Step {
        /// Owning run.
        run_id: Id,
        /// Logical step, preserved across physical retries.
        model_step_id: Id,
    },
}

/// Selection importance, independent of source authority and provider role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextPriority {
    /// Fail if this active item cannot fit in full.
    Required,
    /// Include whole if remaining bounds permit it.
    Optional,
}

/// Data with explicit source, scope, integrity and lifetime. Constructing this DTO
/// does not authenticate provenance; callers must authorize sources before supply.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextItem {
    /// Stable source item identity.
    pub item_id: Id,
    /// Claimed source classification, retained in a data envelope.
    pub origin: ContextOrigin,
    /// Exact source/asset identity and version.
    pub source_ref: VersionedRef,
    /// Authenticated source scope supplied by the Host.
    pub scope: Scope,
    /// Explicitly selected content, not a system map or raw protected record.
    pub content: Vec<InputContent>,
    /// Digest of all other fields, checked again at projection.
    pub digest: JsonDigest,
    /// Session/run/step applicability.
    pub lifetime: ContextLifetime,
    /// Required versus optional selection, without elevated instruction authority.
    pub priority_class: ContextPriority,
}

impl ContextItem {
    /// Own supplied data and compute its source/lifetime/content identity.
    pub fn new(
        item_id: Id,
        origin: ContextOrigin,
        source_ref: VersionedRef,
        scope: Scope,
        content: Vec<InputContent>,
        lifetime: ContextLifetime,
        priority_class: ContextPriority,
    ) -> Self {
        let digest = data_digest(&(
            &item_id,
            origin,
            &source_ref,
            &scope,
            &content,
            &lifetime,
            priority_class,
        ));
        Self {
            item_id,
            origin,
            source_ref,
            scope,
            content,
            digest,
            lifetime,
            priority_class,
        }
    }
    pub(crate) fn valid_digest(&self) -> bool {
        self.digest
            == data_digest(&(
                &self.item_id,
                self.origin,
                &self.source_ref,
                &self.scope,
                &self.content,
                &self.lifetime,
                self.priority_class,
            ))
    }
}
impl fmt::Debug for ContextItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContextItem")
            .field("item_id", &self.item_id)
            .field("origin", &self.origin)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

/// Already-authorized typed provider replay data, not a generic JSON record loader.
#[derive(Debug, Clone)]
pub struct ScopedOpaque {
    /// Scope from which the protected record was read.
    pub scope: Scope,
    /// Exact reference whose digest covers the serialized OpaqueContinuation.
    pub reference: RecordRef,
    /// Provider that owns the record.
    pub provider: Id,
    /// Typed continuation with an exact route identity.
    pub continuation: OpaqueContinuation,
}

/// Finite projection size, separate from model token context capacity and usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionLimits {
    /// Maximum serialized final ModelRequest bytes, including schemas and metadata.
    pub max_bytes: usize,
    /// Maximum projected content blocks plus model tool definitions.
    pub max_items: usize,
}

/// Read-only projection inputs. Transcript must come from a trusted, scoped session
/// store; Message alone cannot authenticate its owner or prove history completeness.
#[derive(Clone)]
pub struct ProjectionInput<'a> {
    /// Validated provider Tool contracts; empty retains native projection for direct callers.
    pub tool_contracts: &'a [crate::CompiledToolContract],
    /// Original resolved profile of this run; never re-resolve it during resume.
    pub profile: &'a ResolvedProfile,
    /// Authenticated execution scope.
    pub scope: &'a Scope,
    /// Current owning run.
    pub run_id: &'a Id,
    /// Logical step, identical to request_id before physical invocation allocation.
    pub model_step_id: &'a Id,
    /// Original persisted run request.
    pub current_request: &'a RunRequest,
    /// Exact stored user message containing that request, to prevent duplication.
    pub current_request_message_id: &'a Id,
    /// Owned-store history borrowed without mutation, including the current user message.
    pub transcript: &'a [Message],
    /// Already-authorized context items; no external source is queried here.
    pub context_items: &'a [ContextItem],
    /// Already-authorized opaque records with typed scope/provider/route metadata.
    pub opaque_records: &'a [ScopedOpaque],
    /// Trusted digest from the session's pinned prompt record.
    pub expected_prompt_digest: &'a JsonDigest,
    /// Logical step identity; ModelExchange later assigns a separate physical request ID.
    pub request_id: Id,
    /// Accounting purpose of this invocation.
    pub purpose: ModelPurpose,
    /// Already-selected immutable model route.
    pub route: ResolvedModelRoute,
    /// Already-resolved requested output mode.
    pub output: ModelOutput,
    /// Provider output-token request, not an estimate of input bytes.
    pub max_output_tokens: NonZeroU64,
    /// Host-owned logical options preserved in the final ModelRequest, outside prompt content.
    /// The selected catalog schemas and adapter define supported keys and wire mapping.
    pub options: JsonObject,
    /// Provider request/response decoding bounds.
    pub response_limits: ModelResponseLimits,
    /// Byte/item projection bounds, not a tokenizer or model context-window check.
    pub limits: ProjectionLimits,
}

/// Separate model projection and explicit selection provenance. No original messages change.
#[derive(Debug)]
pub struct ContextProjection {
    /// Complete prepared model request.
    pub request: ModelRequest,
    /// Original message identities represented in the model request.
    pub selected_message_ids: Vec<Id>,
    /// Original message identities omitted by visibility or whole-run selection.
    pub dropped_message_ids: Vec<Id>,
    /// Active supplied context items included in full.
    pub selected_context_ids: Vec<Id>,
    /// Context items omitted by lifetime or optional-item bounds.
    pub dropped_context_ids: Vec<Id>,
    /// Identity of the unchanged session prefix.
    pub prompt_digest: JsonDigest,
}

/// Prefix reuse and conservative selection without retrieval, loading or compaction.
#[derive(Debug, Clone, Copy, Default)]
pub struct ContextAssembler;

struct RunGroup {
    run_id: Id,
    messages: Vec<(Id, ModelMessage)>,
    has_tool_round: bool,
    has_unknown: bool,
}

impl ContextAssembler {
    /// Construct an assembler without doing I/O.
    pub fn new() -> Self {
        Self
    }

    /// Preserve the fixed prefix and all model-visible current-run messages. Older
    /// complete runs and optional items are added newest first without splitting
    /// tool rounds. The latest visible tool round and runs with unknown tool results
    /// are mandatory. Any unfinished round or oversized required input fails.
    /// Byte bounds do not claim to estimate or enforce provider token context size.
    pub fn project(
        &self,
        snapshot: &PromptSnapshot,
        input: ProjectionInput<'_>,
    ) -> Result<ContextProjection, ContractError> {
        snapshot.validate_for(input.profile, input.scope, input.expected_prompt_digest)?;
        if input.request_id != *input.model_step_id
            || input.limits.max_bytes == 0
            || input.limits.max_items == 0
        {
            return Err(invalid("projection.identity_or_limits"));
        }
        validate_current_request(&input)?;
        let groups = project_transcript(snapshot, &input)?;
        if !input.tool_contracts.is_empty()
            && (input.tool_contracts.len() != snapshot.tools().len()
                || input
                    .tool_contracts
                    .iter()
                    .zip(snapshot.tools())
                    .any(|(contract, original)| {
                        contract.canonical_name() != &original.model_tool.name
                            || contract.tool() != &original.tool
                            || contract.target().provider != input.route.provider
                            || contract.target().api_contract != input.route.api_contract
                            || contract.target().capability_revision
                                != input.route.capability_revision
                    }))
        {
            return Err(mismatch("projection.tool_contracts"));
        }
        let current = groups
            .iter()
            .position(|group| &group.run_id == input.run_id)
            .ok_or_else(|| invalid("projection.current_run"))?;
        if current + 1 != groups.len() {
            return Err(invalid("projection.incomplete_round"));
        }
        let mut selected_groups = BTreeSet::from([current]);
        if let Some(index) = groups.iter().rposition(|group| group.has_tool_round) {
            selected_groups.insert(index);
        }
        selected_groups.extend(
            groups
                .iter()
                .enumerate()
                .filter(|(_, group)| group.has_unknown)
                .map(|(index, _)| index),
        );
        let mut context = Vec::new();
        let mut active = Vec::new();
        let mut seen_context = BTreeSet::new();
        for item in input.context_items {
            if !seen_context.insert(&item.item_id) || !item.valid_digest() {
                return Err(invalid("context_item.digest"));
            }
            if &item.scope != input.scope {
                return Err(mismatch("context_item.scope"));
            }
            if item.origin == ContextOrigin::Skill
                && !snapshot
                    .data
                    .skills
                    .iter()
                    .any(|manifest| manifest.skill == item.source_ref)
            {
                return Err(mismatch("context_item.skill"));
            }
            if item.origin == ContextOrigin::Tool
                && !snapshot
                    .data
                    .tools
                    .iter()
                    .any(|tool| tool.tool == item.source_ref)
            {
                return Err(mismatch("context_item.tool"));
            }
            let applicable = match &item.lifetime {
                ContextLifetime::Session { session_id } => {
                    session_id == &input.current_request.session_id
                }
                ContextLifetime::Run { run_id } => run_id == input.run_id,
                ContextLifetime::Step {
                    run_id,
                    model_step_id,
                } => run_id == input.run_id && model_step_id == input.model_step_id,
            };
            let message = if applicable {
                Some(ModelMessage {
                    role: ModelRole::User,
                    content: vec![ModelContent::Json {
                        value: json!({
                            "kind":"context_data", "item_id":item.item_id, "origin":item.origin,
                            "source_ref":item.source_ref,
                            "content":item.content.iter().map(|content| safe_value(content, input.scope)).collect::<Result<Vec<_>,_>>()?
                        }),
                    }],
                })
            } else {
                None
            };
            active.push(applicable);
            context.push(message);
        }
        let mut selected_context: BTreeSet<usize> = input
            .context_items
            .iter()
            .enumerate()
            .filter(|(index, item)| {
                active[*index] && item.priority_class == ContextPriority::Required
            })
            .map(|(index, _)| index)
            .collect();
        let make_request = |selected_groups: &BTreeSet<usize>,
                            selected_context: &BTreeSet<usize>| {
            let mut messages = snapshot.prefix();
            for contract in input.tool_contracts {
                for fragment in contract.constraint_fragments() {
                    messages.push(ModelMessage {
                        role: ModelRole::System,
                        content: vec![ModelContent::Text {
                            text: fragment.text.clone(),
                        }],
                    });
                }
            }
            // Historical summaries precede the retained conversation and current request.
            for index in selected_context {
                if input.context_items[*index].origin == ContextOrigin::Compaction {
                    messages.push(context[*index].as_ref().expect("active summary").clone());
                }
            }
            for index in selected_groups {
                messages.extend(
                    groups[*index]
                        .messages
                        .iter()
                        .map(|(_, message)| message.clone()),
                );
            }
            for index in selected_context {
                if input.context_items[*index].origin != ContextOrigin::Compaction {
                    messages.push(context[*index].as_ref().expect("active context").clone());
                }
            }
            ModelRequest {
                request_id: input.request_id.clone(),
                purpose: input.purpose,
                route: input.route.clone(),
                messages,
                tools: if input.tool_contracts.is_empty() {
                    snapshot
                        .tools()
                        .iter()
                        .map(|tool| tool.model_tool.clone())
                        .collect()
                } else {
                    input
                        .tool_contracts
                        .iter()
                        .map(|contract| contract.wire_tool().clone())
                        .collect()
                },
                output: input.output.clone(),
                max_output_tokens: input.max_output_tokens,
                options: input.options.clone(),
                limits: input.response_limits.clone(),
            }
        };
        if !fits(
            &make_request(&selected_groups, &selected_context),
            &input.limits,
        ) {
            return Err(budget());
        }
        for index in (0..current).rev() {
            if selected_groups.contains(&index) {
                continue;
            }
            selected_groups.insert(index);
            if !fits(
                &make_request(&selected_groups, &selected_context),
                &input.limits,
            ) {
                selected_groups.remove(&index);
            }
        }
        for index in (0..input.context_items.len()).rev() {
            if !active[index]
                || input.context_items[index].priority_class == ContextPriority::Required
            {
                continue;
            }
            selected_context.insert(index);
            if !fits(
                &make_request(&selected_groups, &selected_context),
                &input.limits,
            ) {
                selected_context.remove(&index);
            }
        }
        let request = make_request(&selected_groups, &selected_context);
        request
            .validate()
            .map_err(|_| invalid("projection.model_request"))?;
        let selected_message_ids: Vec<_> = selected_groups
            .iter()
            .flat_map(|index| groups[*index].messages.iter().map(|(id, _)| id.clone()))
            .collect();
        let selected_ids: BTreeSet<_> = selected_message_ids.iter().collect();
        Ok(ContextProjection {
            request,
            dropped_message_ids: input
                .transcript
                .iter()
                .filter(|message| !selected_ids.contains(&message.message_id))
                .map(|message| message.message_id.clone())
                .collect(),
            selected_message_ids,
            selected_context_ids: selected_context
                .iter()
                .map(|index| input.context_items[*index].item_id.clone())
                .collect(),
            dropped_context_ids: input
                .context_items
                .iter()
                .enumerate()
                .filter(|(index, _)| !selected_context.contains(index))
                .map(|(_, item)| item.item_id.clone())
                .collect(),
            prompt_digest: snapshot.digest(),
        })
    }
}

fn validate_current_request(input: &ProjectionInput<'_>) -> Result<(), ContractError> {
    let message = input
        .transcript
        .iter()
        .find(|message| &message.message_id == input.current_request_message_id)
        .ok_or_else(|| invalid("projection.current_request"))?;
    if &message.run_id != input.run_id
        || message.role != MessageRole::User
        || message.origin != MessageOrigin::User
        || !visible(message)
    {
        return Err(invalid("projection.current_request"));
    }
    let contents: Option<Vec<_>> = message
        .content
        .iter()
        .map(|block| match block {
            ContentBlock::Content { content } => Some(content),
            _ => None,
        })
        .collect();
    if contents.as_deref()
        != Some(
            input
                .current_request
                .input
                .iter()
                .collect::<Vec<_>>()
                .as_slice(),
        )
    {
        return Err(mismatch("projection.current_request"));
    }
    Ok(())
}

struct PendingCall {
    message_id: Id,
    provider_call_id: Id,
    visible: bool,
    known: bool,
}

fn project_transcript(
    snapshot: &PromptSnapshot,
    input: &ProjectionInput<'_>,
) -> Result<Vec<RunGroup>, ContractError> {
    let corrections = crate::message::tool_corrections(input.transcript)?;
    let mut groups = Vec::new();
    let mut seen_messages = BTreeSet::new();
    let mut seen_runs = BTreeSet::new();
    let mut previous_sequence = 0;
    let mut cursor = 0;
    while cursor < input.transcript.len() {
        let run_id = input.transcript[cursor].run_id.clone();
        if !seen_runs.insert(run_id.clone()) {
            return Err(invalid("transcript.run_order"));
        }
        let end = input.transcript[cursor..]
            .iter()
            .position(|message| message.run_id != run_id)
            .map_or(input.transcript.len(), |offset| cursor + offset);
        let mut projected = Vec::new();
        let mut has_tool_round = false;
        let mut has_unknown = false;
        let mut pending: BTreeMap<Id, PendingCall> = BTreeMap::new();
        let mut seen_calls = BTreeSet::new();
        for message in &input.transcript[cursor..end] {
            if message.sequence.get() <= previous_sequence
                || !seen_messages.insert(&message.message_id)
            {
                return Err(invalid("transcript.order"));
            }
            previous_sequence = message.sequence.get();
            let is_visible = visible(message);
            if is_visible {
                match message.role {
                    MessageRole::System => return Err(invalid("transcript.system_role")),
                    MessageRole::Assistant if message.origin != MessageOrigin::Model => {
                        return Err(invalid("transcript.origin"));
                    }
                    MessageRole::Tool if message.origin != MessageOrigin::Tool => {
                        return Err(invalid("transcript.origin"));
                    }
                    MessageRole::User
                        if matches!(
                            message.origin,
                            MessageOrigin::Host
                                | MessageOrigin::Profile
                                | MessageOrigin::Model
                                | MessageOrigin::Tool
                        ) =>
                    {
                        return Err(invalid("transcript.origin"));
                    }
                    _ => {}
                }
                if !pending.is_empty() && message.role != MessageRole::Tool {
                    return Err(invalid("transcript.incomplete_round"));
                }
            }
            let mut content = Vec::new();
            for block in &message.content {
                match block {
                    ContentBlock::ToolCall { call } => {
                        has_tool_round |= is_visible;
                        if message.role != MessageRole::Assistant
                            || message.origin != MessageOrigin::Model
                            || !seen_calls.insert(&call.call_id)
                        {
                            return Err(invalid("transcript.tool_call"));
                        }
                        let tool = snapshot
                            .data
                            .tools
                            .iter()
                            .find(|tool| tool.model_tool.name == call.tool_name)
                            .filter(|_| call.descriptor_digest.is_some());
                        if tool.is_some_and(|tool| {
                            Some(&tool.descriptor_digest) != call.descriptor_digest.as_ref()
                        }) {
                            return Err(mismatch("transcript.descriptor"));
                        }
                        pending.insert(
                            call.call_id.clone(),
                            PendingCall {
                                message_id: message.message_id.clone(),
                                provider_call_id: call.provider_call_id.clone(),
                                visible: is_visible,
                                known: tool.is_some(),
                            },
                        );
                        if is_visible {
                            let contract = if tool.is_some() {
                                input
                                    .tool_contracts
                                    .iter()
                                    .find(|contract| contract.canonical_name() == &call.tool_name)
                            } else {
                                None
                            };
                            // Opaque provider replay is immutable: retain its original wire
                            // names and argument strings/values, including rejected proposals.
                            let original = call.provider_arguments.as_ref().filter(|_| {
                                message
                                    .content
                                    .iter()
                                    .any(|item| matches!(item, ContentBlock::ProviderOpaque { .. }))
                            });
                            content.push(ModelContent::ToolCall {
                                provider_call_id: call.provider_call_id.clone(),
                                name: original.map_or_else(
                                    || {
                                        contract.map_or_else(
                                            || call.tool_name.clone(),
                                            |contract| contract.wire_tool().name.clone(),
                                        )
                                    },
                                    |original| original.name.clone(),
                                ),
                                arguments: if let Some(original) = original {
                                    crate::parse_provider_arguments(
                                        &original.raw,
                                        input.response_limits.max_input_bytes,
                                    )
                                    .unwrap_or_default()
                                } else {
                                    match contract {
                                        Some(contract) => {
                                            contract.encode_arguments(&call.model_inputs)?
                                        }
                                        None => call.model_inputs.clone(),
                                    }
                                },
                            });
                        }
                    }
                    ContentBlock::ToolResult { result } => {
                        let result = corrections
                            .get(&message.message_id)
                            .map_or(result, |(_, result)| result);
                        if message.role != MessageRole::Tool
                            || message.origin != MessageOrigin::Tool
                        {
                            return Err(invalid("transcript.tool_result"));
                        }
                        let call = pending
                            .remove(&result.call_id)
                            .ok_or_else(|| invalid("transcript.tool_result"))?;
                        if call.message_id != result.call_message_id
                            || call.visible != is_visible
                            || (!call.known && result.status == ToolResultStatus::Succeeded)
                        {
                            return Err(invalid("transcript.tool_pair"));
                        }
                        if result.status == ToolResultStatus::Unknown
                            || result.effect == crate::ToolEffect::Unknown
                        {
                            if !is_visible {
                                return Err(invalid("transcript.hidden_unknown_effect"));
                            }
                            has_unknown = true;
                        }
                        if is_visible {
                            let values = result
                                .content
                                .iter()
                                .map(|item| safe_value(item, input.scope))
                                .collect::<Result<Vec<_>, _>>()?;
                            let mut value = json!({"status":result.status,"effect":result.effect,"content":values});
                            if let Some(failure) = &result.error {
                                value["error"] = json!({"code":failure.code});
                            }
                            content.push(ModelContent::ToolResult {
                                provider_call_id: call.provider_call_id,
                                content: value,
                            });
                        }
                    }
                    ContentBlock::Content { content: item } if is_visible => {
                        if message.role == MessageRole::Tool {
                            return Err(invalid("transcript.tool_result"));
                        }
                        content.push(safe_content(item, input.scope)?);
                    }
                    ContentBlock::ProviderOpaque {
                        provider,
                        route_digest,
                        data_ref,
                    } if is_visible => {
                        if message.role != MessageRole::Assistant
                            || message.origin != MessageOrigin::Model
                            || provider != &input.route.provider
                            || route_digest != &input.route.digest()
                        {
                            return Err(mismatch("transcript.opaque_route"));
                        }
                        let records: Vec<_> = input
                            .opaque_records
                            .iter()
                            .filter(|record| &record.reference == data_ref)
                            .collect();
                        if records.len() != 1 {
                            return Err(mismatch("transcript.opaque_record"));
                        }
                        let record = records[0];
                        if &record.scope != input.scope
                            || &record.provider != provider
                            || record.continuation.route_digest() != route_digest
                            || data_digest(&record.continuation) != data_ref.digest
                        {
                            return Err(mismatch("transcript.opaque_record"));
                        }
                        content.push(ModelContent::Opaque {
                            continuation: record.continuation.clone(),
                        });
                    }
                    _ => {}
                }
            }
            if is_visible && !content.is_empty() {
                let role = match message.role {
                    MessageRole::User => ModelRole::User,
                    MessageRole::Assistant => ModelRole::Assistant,
                    MessageRole::Tool => ModelRole::Tool,
                    MessageRole::System => unreachable!("visible System rejected"),
                };
                if message.role == MessageRole::User && message.origin != MessageOrigin::User {
                    let values = content
                        .iter()
                        .map(|content| {
                            serde_json::to_value(content).expect("model content serialization")
                        })
                        .collect::<Vec<_>>();
                    content = vec![ModelContent::Json {
                        value: json!({"kind":"transcript_data", "origin":message.origin,
                        "source_message_id":message.message_id, "content":values}),
                    }];
                }
                let source_id = corrections
                    .get(&message.message_id)
                    .map_or(&message.message_id, |(source_id, _)| source_id);
                projected.push((source_id.clone(), ModelMessage { role, content }));
            }
        }
        if !pending.is_empty() {
            return Err(invalid("transcript.incomplete_round"));
        }
        groups.push(RunGroup {
            run_id,
            messages: projected,
            has_tool_round,
            has_unknown,
        });
        cursor = end;
    }
    Ok(groups)
}

fn visible(message: &Message) -> bool {
    matches!(
        message.visibility,
        Visibility::Model | Visibility::UserAndModel
    )
}

fn safe_content(content: &InputContent, scope: &Scope) -> Result<ModelContent, ContractError> {
    match content {
        InputContent::Text { text } => Ok(ModelContent::Text { text: text.clone() }),
        InputContent::Json { value } => Ok(ModelContent::Json {
            value: value.clone(),
        }),
        _ => Ok(ModelContent::Json {
            value: safe_value(content, scope)?,
        }),
    }
}

pub(crate) fn safe_value(content: &InputContent, scope: &Scope) -> Result<Value, ContractError> {
    Ok(match content {
        InputContent::Text { text } => json!({"type":"text","text":text}),
        InputContent::Json { value } => json!({"type":"json","value":value}),
        InputContent::Artifact { reference } => {
            if &reference.scope != scope {
                return Err(mismatch("context.artifact_scope"));
            }
            json!({"type":"artifact","artifact_id":reference.artifact_id,"media_type":reference.media_type,
                "size_bytes":reference.size_bytes,"content_hash":reference.content_hash})
        }
        InputContent::Evidence { reference } => {
            let mut value = json!({"type":"evidence","source_id":reference.source_id,"version":reference.version,
                "location":reference.location,"content_hash":reference.content_hash});
            if let Some(quote) = &reference.quote {
                value["quote"] = json!(quote);
            }
            value
        }
    })
}

struct ByteCounter {
    written: usize,
    limit: usize,
}
impl io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.written = self
            .written
            .checked_add(bytes.len())
            .filter(|size| *size <= self.limit)
            .ok_or_else(|| io::Error::other("projection limit exceeded"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn fits(request: &ModelRequest, limits: &ProjectionLimits) -> bool {
    let count = request
        .messages
        .iter()
        .try_fold(request.tools.len(), |count, message| {
            count.checked_add(message.content.len())
        });
    if count.is_none_or(|count| count > limits.max_items) {
        return false;
    }
    serde_json::to_writer(
        &mut ByteCounter {
            written: 0,
            limit: limits.max_bytes.min(request.limits.max_input_bytes),
        },
        request,
    )
    .is_ok()
}
fn invalid(path: &str) -> ContractError {
    ContractError::new(ErrorCode::InvalidContext, path)
}
fn mismatch(path: &str) -> ContractError {
    ContractError::new(ErrorCode::ContextMismatch, path)
}
fn budget() -> ContractError {
    ContractError::new(
        ErrorCode::ContextBudgetExceeded,
        "projection.required_input",
    )
}
```

## `crates/wickle/src/input_binding.rs`

```rust
use std::{collections::BTreeMap, fmt, future::Future, io, panic::AssertUnwindSafe, sync::Arc};

use futures_util::FutureExt;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::{
    CommitInput, CompiledTool, ContractError, ErrorCode, ExecutionContext, ExecutionContextData,
    Id, IdSource, JsonDigest, JsonObject, PolicyAction, PolicyDecision, PolicyGate, PolicyRequest,
    PortFuture, ProtectedRecord, RecordRef, RunBudget, RunSnapshot, Scope, SystemInputDefinition,
    SystemInputRegistry, SystemInputSnapshotRef, SystemInputSource, SystemInputs, ToolBindingRef,
    ToolCall, ToolCallState, ToolPolicyInput, VersionedRef, serialization::data_digest,
    tool_schema::compile_validator,
};

const RUN_INPUT_VERSION: &str = "wickle.run-system-inputs.v1";
const BOUND_INPUT_VERSION: &str = "wickle.bound-tool-input.v1";

/// Finite resolver and input-size bounds. They are independent of model token budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputBindingLimits {
    /// Maximum distinct resolver keys read for one new call; zero disables resolver reads.
    pub max_resolver_calls: usize,
    /// Maximum serialized bytes in one resolved or run-supplied value.
    pub max_value_bytes: usize,
    /// Maximum protected run-input or bound-input record size.
    pub max_bound_bytes: usize,
}
impl Default for InputBindingLimits {
    fn default() -> Self {
        Self {
            max_resolver_calls: 64,
            max_value_bytes: 65_536,
            max_bound_bytes: 1_048_576,
        }
    }
}
impl InputBindingLimits {
    fn validate(self) -> Result<(), ContractError> {
        if self.max_value_bytes == 0 || self.max_bound_bytes == 0 {
            return Err(error(ErrorCode::InvalidContract, "input_binding.limits"));
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunInputData {
    schema_version: String,
    scope: Scope,
    values: SystemInputs,
    definitions: BTreeMap<Id, SystemInputDefinition>,
}

/// Owned admission-time values and definition metadata. No resolver executes during
/// capture, and a missing value is not replaced by a schema default or generated ID.
#[derive(Clone)]
pub struct RunSystemInputs {
    data: RunInputData,
}

impl Serialize for RunSystemInputs {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.data.serialize(serializer)
    }
}
impl fmt::Debug for RunSystemInputs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunSystemInputs")
            .field("value_count", &self.data.values.values().len())
            .field("definition_count", &self.data.definitions.len())
            .finish_non_exhaustive()
    }
}

impl RunSystemInputs {
    /// Validate supplied keys/types and freeze owned values with the default finite bounds.
    pub fn capture(
        scope: Scope,
        supplied: Option<SystemInputs>,
        registry: &SystemInputRegistry,
    ) -> Result<Self, ContractError> {
        Self::capture_with_limits(scope, supplied, registry, InputBindingLimits::default())
    }
    /// Capture using explicit finite size bounds. Missing registered keys are allowed.
    pub fn capture_with_limits(
        scope: Scope,
        supplied: Option<SystemInputs>,
        registry: &SystemInputRegistry,
        limits: InputBindingLimits,
    ) -> Result<Self, ContractError> {
        limits.validate()?;
        let snapshot = Self {
            data: RunInputData {
                schema_version: RUN_INPUT_VERSION.into(),
                scope,
                values: supplied.unwrap_or_default(),
                definitions: registry.definitions().clone(),
            },
        };
        snapshot.validate_data()?;
        check_size(&snapshot, limits.max_bound_bytes)?;
        for value in snapshot.values().values() {
            check_size(value, limits.max_value_bytes)?;
        }
        Ok(snapshot)
    }
    /// Explicit access for the trusted binder; never automatic model projection.
    pub fn values(&self) -> &JsonObject {
        self.data.values.values()
    }
    /// Definition revisions and schemas pinned at admission.
    pub fn definitions(&self) -> &BTreeMap<Id, SystemInputDefinition> {
        &self.data.definitions
    }
    /// Exact owning scope of these values.
    pub fn scope(&self) -> &Scope {
        &self.data.scope
    }
    /// Digest of the complete protected serialized snapshot.
    pub fn digest(&self) -> JsonDigest {
        data_digest(&self.data)
    }
    /// Create the immutable record to include in the admission transaction.
    pub fn to_record(&self, record_id: Id, revision: u64) -> ProtectedRecord {
        ProtectedRecord::new(
            record_id,
            revision,
            serde_json::to_value(self).expect("serializable input data"),
        )
    }
    /// Create the run checkpoint reference after verifying its protected record identity.
    pub fn snapshot_ref(
        &self,
        record: &RecordRef,
    ) -> Result<SystemInputSnapshotRef, ContractError> {
        if record.digest != self.digest() {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "system_inputs.record",
            ));
        }
        Ok(SystemInputSnapshotRef {
            snapshot_ref: record.clone(),
            values_digest: data_digest(self.values()),
            definition_versions: self
                .definitions()
                .iter()
                .map(|(key, definition)| (key.clone(), definition.version.clone()))
                .collect(),
        })
    }
    /// Restore exact stored data and verify every pinned definition against the registry.
    /// Additional unrelated registry keys do not replace or enlarge the saved snapshot.
    pub fn restore(
        record: &ProtectedRecord,
        reference: &SystemInputSnapshotRef,
        scope: &Scope,
        registry: &SystemInputRegistry,
    ) -> Result<Self, ContractError> {
        if record.reference() != &reference.snapshot_ref {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "system_inputs.record",
            ));
        }
        let snapshot = Self::from_value(record.value(), reference, scope)?;
        if snapshot
            .definitions()
            .iter()
            .any(|(key, definition)| registry.get(key) != Some(definition))
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "system_inputs.definitions",
            ));
        }
        Ok(snapshot)
    }
    /// Omission reuses saved values. Any supplied map, including an empty map, must match.
    pub fn validate_resume(&self, supplied: Option<&SystemInputs>) -> Result<(), ContractError> {
        if supplied.is_some_and(|values| data_digest(values.values()) != data_digest(self.values()))
        {
            return Err(error(ErrorCode::SystemInputsMismatch, "system_inputs"));
        }
        Ok(())
    }
    fn validate_data(&self) -> Result<(), ContractError> {
        if self.data.schema_version != RUN_INPUT_VERSION
            || self
                .definitions()
                .iter()
                .any(|(key, definition)| key != &definition.key)
        {
            return Err(error(
                ErrorCode::SystemInputInvalid,
                "system_inputs.snapshot",
            ));
        }
        SystemInputRegistry::new(self.definitions().values().cloned().collect())
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "system_inputs.definitions"))?;
        for (key, value) in self.values() {
            let key = Id::new(key.clone())
                .map_err(|_| error(ErrorCode::SystemInputInvalid, "system_inputs.key"))?;
            let definition = self
                .definitions()
                .get(&key)
                .ok_or_else(|| error(ErrorCode::SystemInputInvalid, "system_inputs.key"))?;
            if !matches!(definition.source, SystemInputSource::Run {}) {
                return Err(error(ErrorCode::SystemInputInvalid, "system_inputs.source"));
            }
            validate_value(definition, value)?;
        }
        Ok(())
    }
    pub(crate) fn from_value(
        value: &Value,
        reference: &SystemInputSnapshotRef,
        scope: &Scope,
    ) -> Result<Self, ContractError> {
        let data: RunInputData = serde_json::from_value(value.clone())
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "system_inputs.snapshot"))?;
        let snapshot = Self { data };
        snapshot.validate_data()?;
        if snapshot.scope() != scope
            || snapshot.snapshot_ref(&reference.snapshot_ref)? != *reference
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "system_inputs.snapshot",
            ));
        }
        Ok(snapshot)
    }
}

/// One exact read-only resolver lookup, without other system values or credentials.
#[derive(Clone)]
pub struct SystemInputResolveRequest {
    /// Original selected adapter export; absent for a directly registered catalog tool.
    pub selection: Option<ToolBindingRef>,
    /// Registered key being requested.
    pub key: Id,
    /// Pinned value-definition revision.
    pub definition_version: Id,
    /// Exact resolver implementation selected by the definition.
    pub resolver_ref: VersionedRef,
    /// Normalized model-owned arguments only, including declared top-level defaults.
    pub model_inputs: JsonObject,
}
impl fmt::Debug for SystemInputResolveRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SystemInputResolveRequest")
            .field("key", &self.key)
            .field("definition_version", &self.definition_version)
            .finish_non_exhaustive()
    }
}

/// Current actor and execution bounds supplied to a trusted read-only resolver.
#[derive(Debug, Clone)]
pub struct SystemInputResolveContext {
    /// Authenticated scope, not a value extracted from the model's arguments.
    pub scope: Scope,
    /// Current principal; it does not rewrite the run's original system-input values.
    pub principal_ref: Id,
    /// Current capability grant, checked by policy and the resolver's own backend.
    pub capability_grant_ref: Id,
    /// Current owning run.
    pub run_id: Id,
    /// Original logical call identity.
    pub call_id: Id,
    /// Deadline for this lookup.
    pub deadline: tokio::time::Instant,
    /// Child cancellation signal linked to both execution and caller cancellation.
    pub cancellation: CancellationToken,
}

/// Data and source revision returned by a resolver, or recorded from a run snapshot.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedSystemInput {
    /// Supplied JSON value; explicit null is different from an absent result.
    pub value: Value,
    /// Source data revision, not a newly invented foreign key.
    pub revision: Id,
}
impl fmt::Debug for ResolvedSystemInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ResolvedSystemInput(<redacted>)")
    }
}

/// Trusted read-only lookup port. It must honor scope, principal, deadline and
/// cancellation, and must not hide business writes or create missing foreign keys.
pub trait SystemInputResolver: Send + Sync {
    /// Read one exact registered key; None means absent, not JSON null.
    fn resolve<'a>(
        &'a self,
        request: &'a SystemInputResolveRequest,
        context: &'a SystemInputResolveContext,
    ) -> PortFuture<'a, Option<ResolvedSystemInput>>;
}

/// One hidden parameter's fixed source, revision and optional value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundSystemInput {
    /// Registry key, which may differ from the handler parameter name.
    pub key: Id,
    /// Value-definition version pinned by the compiler and admission snapshot.
    pub definition_version: Id,
    /// Run snapshot or exact resolver implementation.
    pub source: SystemInputSource,
    /// None is absence; Some with value:null is an explicitly supplied null.
    pub resolved: Option<ResolvedSystemInput>,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundInputData {
    schema_version: String,
    scope: Scope,
    run_id: Id,
    call_id: Id,
    tool: VersionedRef,
    #[serde(
        default,
        deserialize_with = "crate::serialization::optional",
        skip_serializing_if = "Option::is_none"
    )]
    selection: Option<ToolBindingRef>,
    descriptor_digest: JsonDigest,
    compiled_digest: JsonDigest,
    compiler_version: String,
    original_model_inputs: JsonObject,
    #[serde(
        default,
        deserialize_with = "crate::serialization::optional",
        skip_serializing_if = "Option::is_none"
    )]
    effective_model_inputs: Option<JsonObject>,
    #[serde(
        default,
        deserialize_with = "crate::serialization::optional",
        skip_serializing_if = "Option::is_none"
    )]
    transformation_ref: Option<RecordRef>,
    normalized_model_inputs: JsonObject,
    run_inputs_ref: Option<SystemInputSnapshotRef>,
    system_inputs: BTreeMap<String, BoundSystemInput>,
    execution_args: JsonObject,
}

/// Immutable execution inputs. Serialization is only for protected storage/policy,
/// never a replacement for the original model ToolCall or its transcript message.
#[derive(Clone, Serialize)]
pub struct BoundToolInput {
    data: BoundInputData,
    binding_digest: JsonDigest,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundInputRecord {
    data: BoundInputData,
    binding_digest: JsonDigest,
}

impl fmt::Debug for BoundToolInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundToolInput")
            .field("call_id", &self.data.call_id)
            .field("tool", &self.data.tool)
            .field("binding_digest", &self.binding_digest)
            .finish_non_exhaustive()
    }
}

impl BoundToolInput {
    /// Exact owning resource scope.
    pub fn scope(&self) -> &Scope {
        &self.data.scope
    }
    /// Owning run identity.
    pub fn run_id(&self) -> &Id {
        &self.data.run_id
    }
    /// Stable logical call identity.
    pub fn call_id(&self) -> &Id {
        &self.data.call_id
    }
    /// Exact registered tool identity.
    pub fn tool(&self) -> &VersionedRef {
        &self.data.tool
    }
    /// Original export selection when this call is supplied by a scoped adapter.
    pub fn selection(&self) -> Option<&ToolBindingRef> {
        self.data.selection.as_ref()
    }
    /// Original descriptor digest.
    pub fn descriptor_digest(&self) -> &JsonDigest {
        &self.data.descriptor_digest
    }
    /// Compiler, schema and selected system-definition identity.
    pub fn compiled_digest(&self) -> &JsonDigest {
        &self.data.compiled_digest
    }
    /// Pinned compiler contract version.
    pub fn compiler_version(&self) -> &str {
        &self.data.compiler_version
    }
    /// Unmodified arguments originally recorded for the model call.
    pub fn original_model_inputs(&self) -> &JsonObject {
        &self.data.original_model_inputs
    }
    /// Validated hook-transformed arguments, or the unchanged original arguments.
    pub fn effective_model_inputs(&self) -> &JsonObject {
        self.data
            .effective_model_inputs
            .as_ref()
            .unwrap_or(&self.data.original_model_inputs)
    }
    /// Exact saved final transformation record, when hooks transformed this call.
    pub fn transformation_ref(&self) -> Option<&RecordRef> {
        self.data.transformation_ref.as_ref()
    }
    /// Effective model arguments plus declared top-level defaults.
    pub fn normalized_model_inputs(&self) -> &JsonObject {
        &self.data.normalized_model_inputs
    }
    /// Only hidden parameters needed by this tool, with fixed absence/value metadata.
    pub fn system_inputs(&self) -> &BTreeMap<String, BoundSystemInput> {
        &self.data.system_inputs
    }
    /// Full handler arguments; privileged access, never automatic model echo.
    pub fn execution_args(&self) -> &JsonObject {
        &self.data.execution_args
    }
    /// Digest over exact inputs, tool/compiler identity, source revisions, scope and call.
    pub fn binding_digest(&self) -> &JsonDigest {
        &self.binding_digest
    }
    /// Build the existing final-value policy input without introducing a new ownership port.
    pub fn policy_input(&self) -> ToolPolicyInput {
        let input = ToolPolicyInput::new(
            self.data.call_id.clone(),
            self.data.tool.clone(),
            self.data.descriptor_digest.clone(),
            self.binding_digest.clone(),
            self.data.execution_args.clone(),
        );
        match &self.data.selection {
            Some(selection) => input.with_selection(selection.clone()),
            None => input,
        }
    }
    /// Exact action checked for allow/deny/approval after all values are fixed.
    pub fn policy_request(&self) -> PolicyRequest {
        PolicyRequest {
            owner_scope: self.data.scope.clone(),
            resource_id: self.data.run_id.clone(),
            action: PolicyAction::ExecuteTool {
                input: self.policy_input(),
            },
        }
    }
    pub(crate) fn policy_request_for_run(&self, snapshot: &crate::RunSnapshot) -> PolicyRequest {
        let mut input = self.policy_input();
        if snapshot.scope == self.data.scope && snapshot.run_id == self.data.run_id {
            let receipt = snapshot.resume_receipts.iter().rev().find(|receipt| {
                matches!(&receipt.command.action,
                    crate::ResumeAction::Approve { target: crate::ApprovalTarget::Tool { call_id, binding_digest }, .. }
                    | crate::ResumeAction::Deny { target: crate::ApprovalTarget::Tool { call_id, binding_digest }, .. }
                    if call_id == &self.data.call_id && binding_digest == &self.binding_digest)
            });
            if let Some(receipt) = receipt.filter(|receipt| {
                !receipt.expired
                    && matches!(receipt.command.action, crate::ResumeAction::Approve { .. })
            }) {
                input = input.with_approval(receipt);
            }
        }
        PolicyRequest {
            owner_scope: self.data.scope.clone(),
            resource_id: self.data.run_id.clone(),
            action: PolicyAction::ExecuteTool { input },
        }
    }
    /// Restore protected inputs using the saved ledger call's exact record reference
    /// and the currently supplied compiled contract.
    pub fn restore(
        record: &ProtectedRecord,
        compiled: &CompiledTool,
        scope: &Scope,
        run_id: &Id,
        call: &ToolCall,
        run_inputs_ref: Option<&SystemInputSnapshotRef>,
    ) -> Result<Self, ContractError> {
        let bound = Self::from_value(record.value())?;
        if call.bound_input_ref.as_ref() != Some(record.reference())
            || data_digest(&bound) != record.reference().digest
        {
            return Err(error(ErrorCode::SystemInputsMismatch, "bound_input.record"));
        }
        bound.validate_identity(scope, run_id, call, run_inputs_ref)?;
        bound.validate_compiled(compiled)?;
        Ok(bound)
    }
    fn from_value(value: &Value) -> Result<Self, ContractError> {
        let record: BoundInputRecord = serde_json::from_value(value.clone())
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "bound_input"))?;
        let bound = Self {
            data: record.data,
            binding_digest: record.binding_digest,
        };
        if bound.data.schema_version != BOUND_INPUT_VERSION
            || data_digest(&bound.data) != bound.binding_digest
            || data_digest(&bound) != crate::canonical_digest(value)
        {
            return Err(error(ErrorCode::SystemInputsMismatch, "bound_input.digest"));
        }
        let mut execution = bound.data.normalized_model_inputs.clone();
        if bound.data.effective_model_inputs.is_some() != bound.data.transformation_ref.is_some() {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.transformation",
            ));
        }
        if bound
            .effective_model_inputs()
            .iter()
            .any(|(key, value)| execution.get(key) != Some(value))
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.model_inputs",
            ));
        }
        let mut sources: BTreeMap<&Id, &BoundSystemInput> = BTreeMap::new();
        for (parameter, input) in &bound.data.system_inputs {
            if execution.contains_key(parameter) {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.ownership",
                ));
            }
            if sources
                .insert(&input.key, input)
                .is_some_and(|previous| previous != input)
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.sources",
                ));
            }
            if let Some(resolved) = &input.resolved {
                execution.insert(parameter.clone(), resolved.value.clone());
            }
        }
        if execution != bound.data.execution_args {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.execution_args",
            ));
        }
        Ok(bound)
    }
    fn validate_identity(
        &self,
        scope: &Scope,
        run_id: &Id,
        call: &ToolCall,
        run_inputs_ref: Option<&SystemInputSnapshotRef>,
    ) -> Result<(), ContractError> {
        if self.scope() != scope
            || self.run_id() != run_id
            || self.call_id() != &call.call_id
            || Some(self.descriptor_digest()) != call.descriptor_digest.as_ref()
            || self.original_model_inputs() != &call.model_inputs
            || self.data.run_inputs_ref.as_ref() != run_inputs_ref
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.identity",
            ));
        }
        Ok(())
    }
    fn validate_compiled(&self, compiled: &CompiledTool) -> Result<(), ContractError> {
        if self.compiled_digest() != compiled.digest()
            || self.compiler_version() != compiled.compiler_version()
            || self.tool() != &compiled.descriptor().tool
            || self.descriptor_digest() != compiled.descriptor_digest()
            || self.system_inputs().len() != compiled.system_bindings().len()
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.compiled",
            ));
        }
        compiled.normalize_model_inputs(self.original_model_inputs())?;
        compiled.normalize_model_inputs(self.effective_model_inputs())?;
        if normalize_model_inputs(compiled, self.effective_model_inputs())?
            != *self.normalized_model_inputs()
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.normalization",
            ));
        }
        for (parameter, definition) in compiled.system_bindings() {
            let input = self
                .system_inputs()
                .get(parameter)
                .ok_or_else(|| error(ErrorCode::SystemInputsMismatch, "bound_input.parameters"))?;
            if input.key != definition.key
                || input.definition_version != definition.version
                || input.source != definition.source
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.definitions",
                ));
            }
            if let Some(value) = &input.resolved {
                validate_value(definition, &value.value)?;
            }
        }
        compiled
            .validate_execution_inputs(self.execution_args())
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "tool.execution_inputs"))
    }
}

/// A saved candidate and the decision observed at binding time, not a reusable
/// dispatch permit. The executor must recheck current policy/budgets before I/O.
#[derive(Debug)]
pub struct ToolBindingResult {
    /// Owned immutable protected input.
    pub input: BoundToolInput,
    /// Record stored atomically with the call's bound_input_ref.
    pub reference: RecordRef,
    /// Allow or require_approval. Deny is returned as an error without saving a new candidate.
    pub decision: PolicyDecision,
}

/// Default normalization, registered system-value lookup and immutable candidate persistence.
/// This version starts from the original model input. Hook transformations require
/// a separate recorded path and never overwrite the original ToolCall.
pub struct InputBinder {
    registry: Arc<SystemInputRegistry>,
    resolver: Option<Arc<dyn SystemInputResolver>>,
    policy: Arc<PolicyGate>,
    ids: Arc<dyn IdSource>,
    limits: InputBindingLimits,
}

impl InputBinder {
    /// Restore and verify provider arguments before defaults, Hooks or system resolution.
    /// This performs only protected store reads; it does not invoke a Tool/resolver.
    pub async fn prepare_model_inputs(
        &self,
        compiled: &CompiledTool,
        call: &ToolCall,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<JsonObject, ContractError> {
        if &context.data.scope != budget.scope() {
            return Err(error(ErrorCode::AccessDenied, "tool.scope"));
        }
        if call.descriptor_digest.as_ref() != Some(compiled.descriptor_digest()) {
            return Err(error(
                ErrorCode::InvalidToolInputContract,
                "tool.descriptor",
            ));
        }
        boundary(context, budget).await?;
        if let Some(provider) = &call.provider_arguments {
            let decoded = if let Some(reference) = &provider.compiled_contract_ref {
                let saved = bounded(
                    context,
                    budget,
                    budget.store().load(budget.scope(), budget.run_id()),
                )
                .await?;
                let invocation = saved
                    .snapshot
                    .model_ledger
                    .iter()
                    .find(|invocation| invocation.attempt_id == call.model_request_id)
                    .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.provider_invocation"))?;
                let target = crate::ProviderToolTarget::for_route(&invocation.route);
                let record = bounded(
                    context,
                    budget,
                    budget.store().read_record(budget.scope(), reference),
                )
                .await?;
                let digest =
                    serde_json::from_value(record.value().get("digest").cloned().ok_or_else(
                        || error(ErrorCode::InvalidSnapshot, "tool.provider_contract"),
                    )?)
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "tool.provider_contract"))?;
                let limits = crate::ProviderToolSchemaLimits {
                    max_argument_bytes: self.limits.max_bound_bytes,
                    ..Default::default()
                };
                let contract = crate::CompiledToolContract::restore(
                    &serde_json::to_string(record.value())
                        .map_err(|_| error(ErrorCode::InvalidJson, "tool.provider_contract"))?,
                    compiled,
                    &target,
                    &digest,
                    limits,
                )?;
                if contract.wire_tool().name != provider.name {
                    return Err(error(ErrorCode::InvalidArguments, "tool.provider_name"));
                }
                contract.decode_arguments(&provider.raw, limits)?
            } else {
                if provider.name != call.tool_name {
                    return Err(error(ErrorCode::InvalidArguments, "tool.provider_name"));
                }
                crate::provider_tool_schema::parse_provider_arguments(
                    &provider.raw,
                    self.limits.max_bound_bytes,
                )?
            };
            if decoded != call.model_inputs {
                return Err(error(
                    ErrorCode::InvalidArguments,
                    "tool.canonical_arguments",
                ));
            }
        }
        compiled.normalize_model_inputs(&call.model_inputs)
    }

    /// Wire trusted metadata, optional read-only resolver, policy, and internal record IDs.
    pub fn new(
        registry: Arc<SystemInputRegistry>,
        resolver: Option<Arc<dyn SystemInputResolver>>,
        policy: Arc<PolicyGate>,
        ids: Arc<dyn IdSource>,
    ) -> Self {
        Self {
            registry,
            resolver,
            policy,
            ids,
            limits: InputBindingLimits::default(),
        }
    }
    /// Set finite lookup/value/candidate bounds. Zero lookups disables resolver sources.
    pub fn with_limits(mut self, limits: InputBindingLimits) -> Result<Self, ContractError> {
        limits.validate()?;
        self.limits = limits;
        Ok(self)
    }

    /// Reuse an existing saved binding, or bind and atomically save a new candidate.
    /// Every path checks current policy; an existing call never re-queries its resolver.
    pub async fn bind(
        &self,
        compiled: &CompiledTool,
        call_id: &Id,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<ToolBindingResult, ContractError> {
        boundary(context, budget).await?;
        let saved = bounded(
            context,
            budget,
            budget.store().load(budget.scope(), budget.run_id()),
        )
        .await?;
        let call = saved
            .snapshot
            .tool_ledger
            .iter()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool_call"))?
            .call
            .clone();
        check_selection(&saved.snapshot, compiled, &call)?;
        crate::future::boxed(|| self.prepare_model_inputs(compiled, &call, context, budget))
            .await?;
        let selection = resolved_tool_selection(&saved.snapshot, compiled, context, budget).await?;
        let run_inputs = match &saved.snapshot.system_inputs {
            Some(reference) => {
                boundary(context, budget).await?;
                let record = bounded(
                    context,
                    budget,
                    budget
                        .store()
                        .read_record(budget.scope(), &reference.snapshot_ref),
                )
                .await?;
                let inputs =
                    RunSystemInputs::restore(&record, reference, budget.scope(), &self.registry)?;
                inputs.validate_resume(context.data.system_inputs.as_ref())?;
                Some(inputs)
            }
            None => {
                if context
                    .data
                    .system_inputs
                    .as_ref()
                    .is_some_and(|values| !values.values().is_empty())
                {
                    return Err(error(ErrorCode::SystemInputsMismatch, "system_inputs"));
                }
                if !compiled.system_bindings().is_empty() {
                    return Err(error(
                        ErrorCode::SystemInputMissing,
                        "system_inputs.snapshot",
                    ));
                }
                None
            }
        };
        for definition in compiled.system_bindings().values() {
            if self.registry.get(&definition.key) != Some(definition)
                || run_inputs
                    .as_ref()
                    .and_then(|inputs| inputs.definitions().get(&definition.key))
                    != Some(definition)
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "system_inputs.definitions",
                ));
            }
        }
        let transformed =
            saved_tool_transform(&saved.snapshot, compiled, &call, context, budget).await?;
        if let Some(reference) = &call.bound_input_ref {
            boundary(context, budget).await?;
            let record = bounded(
                context,
                budget,
                budget.store().read_record(budget.scope(), reference),
            )
            .await?;
            let input = BoundToolInput::restore(
                &record,
                compiled,
                budget.scope(),
                budget.run_id(),
                &call,
                saved.snapshot.system_inputs.as_ref(),
            )?;
            validate_bound_record(record.value(), &saved.snapshot, &call, run_inputs.as_ref())?;
            if input.selection() != selection.as_ref() {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.selection",
                ));
            }
            if input.transformation_ref() != transformed.as_ref().map(|(_, reference)| reference)
                || input.effective_model_inputs()
                    != transformed
                        .as_ref()
                        .map_or(&call.model_inputs, |(inputs, _)| inputs)
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.transformation",
                ));
            }
            check_size(&input, self.limits.max_bound_bytes)?;
            for value in input
                .system_inputs()
                .values()
                .filter_map(|input| input.resolved.as_ref())
            {
                check_size(&value.value, self.limits.max_value_bytes)?;
            }
            let decision = self
                .authorize(
                    &input.policy_request_for_run(&saved.snapshot),
                    context,
                    budget,
                    false,
                )
                .await?;
            boundary(context, budget).await?;
            return Ok(ToolBindingResult {
                input,
                reference: reference.clone(),
                decision,
            });
        }
        if !matches!(
            saved
                .snapshot
                .tool_ledger
                .iter()
                .find(|entry| &entry.call.call_id == call_id)
                .expect("found call")
                .state,
            ToolCallState::Planned {}
        ) {
            return Err(error(ErrorCode::InvalidTransition, "tool_call.state"));
        }
        let effective = transformed
            .as_ref()
            .map_or(&call.model_inputs, |(inputs, _)| inputs);
        let normalized = normalize_model_inputs(compiled, effective)?;
        check_size(&normalized, self.limits.max_bound_bytes)?;
        let mut execution_args = normalized.clone();
        let mut system_inputs = BTreeMap::new();
        let mut values: BTreeMap<Id, Option<ResolvedSystemInput>> = BTreeMap::new();
        let mut resolver_calls = 0;
        for (parameter, definition) in compiled.system_bindings() {
            let resolved = if let Some(cached) = values.get(&definition.key) {
                cached.clone()
            } else {
                let value = match &definition.source {
                    SystemInputSource::Run {} => run_inputs
                        .as_ref()
                        .and_then(|inputs| inputs.values().get(definition.key.as_str()))
                        .cloned()
                        .map(|value| ResolvedSystemInput {
                            value,
                            revision: Id::new(
                                saved
                                    .snapshot
                                    .system_inputs
                                    .as_ref()
                                    .expect("required run snapshot")
                                    .snapshot_ref
                                    .revision
                                    .to_string(),
                            )
                            .expect("numeric revision"),
                        }),
                    SystemInputSource::Resolver { resolver_ref } => {
                        if resolver_calls >= self.limits.max_resolver_calls {
                            return Err(limit_error());
                        }
                        let request = PolicyRequest {
                            owner_scope: budget.scope().clone(),
                            resource_id: budget.run_id().clone(),
                            action: PolicyAction::ResolveSystemInput {
                                selection: selection.clone(),
                                tool: compiled.descriptor().tool.clone(),
                                call_id: call_id.clone(),
                                descriptor_digest: compiled.descriptor_digest().clone(),
                                compiled_digest: compiled.digest().clone(),
                                key: definition.key.clone(),
                                definition_version: definition.version.clone(),
                                resolver_ref: resolver_ref.clone(),
                            },
                        };
                        self.authorize(&request, context, budget, true).await?;
                        let resolver = self.resolver.as_ref().ok_or_else(|| {
                            error(ErrorCode::SystemInputUnavailable, "system_input.resolver")
                        })?;
                        boundary(context, budget).await?;
                        let request = SystemInputResolveRequest {
                            selection: selection.clone(),
                            key: definition.key.clone(),
                            definition_version: definition.version.clone(),
                            resolver_ref: resolver_ref.clone(),
                            model_inputs: normalized.clone(),
                        };
                        let child = budget.cancellation().child_token();
                        let lookup_context = SystemInputResolveContext {
                            scope: budget.scope().clone(),
                            principal_ref: context.data.principal_ref.clone(),
                            capability_grant_ref: context.data.capability_grant_ref.clone(),
                            run_id: budget.run_id().clone(),
                            call_id: call_id.clone(),
                            deadline: budget.call_deadline()?,
                            cancellation: child.clone(),
                        };
                        let lookup = AssertUnwindSafe(async {
                            resolver.resolve(&request, &lookup_context).await
                        })
                        .catch_unwind();
                        tokio::pin!(lookup);
                        let guard = child.drop_guard();
                        resolver_calls += 1;
                        let answer = bounded(context, budget, async {
                            lookup
                                .await
                                .map_err(|_| {
                                    error(
                                        ErrorCode::SystemInputUnavailable,
                                        "system_input.resolver",
                                    )
                                })?
                                .map_err(|_| {
                                    error(
                                        ErrorCode::SystemInputUnavailable,
                                        "system_input.resolver",
                                    )
                                })
                        })
                        .await;
                        drop(guard);
                        let answer = answer?;
                        boundary(context, budget).await?;
                        answer
                    }
                };
                if let Some(resolved) = &value {
                    check_size(&resolved.value, self.limits.max_value_bytes)?;
                    validate_value(definition, &resolved.value)?;
                }
                values.insert(definition.key.clone(), value.clone());
                value
            };
            if let Some(value) = &resolved {
                execution_args.insert(parameter.clone(), value.value.clone());
            } else if required_parameter(compiled, parameter) {
                return Err(error(
                    ErrorCode::SystemInputMissing,
                    &system_input_path(&definition.key),
                ));
            }
            system_inputs.insert(
                parameter.clone(),
                BoundSystemInput {
                    key: definition.key.clone(),
                    definition_version: definition.version.clone(),
                    source: definition.source.clone(),
                    resolved,
                },
            );
        }
        compiled
            .validate_execution_inputs(&execution_args)
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "tool.execution_inputs"))?;
        let data = BoundInputData {
            schema_version: BOUND_INPUT_VERSION.into(),
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            call_id: call_id.clone(),
            tool: compiled.descriptor().tool.clone(),
            selection,
            descriptor_digest: compiled.descriptor_digest().clone(),
            compiled_digest: compiled.digest().clone(),
            compiler_version: compiled.compiler_version().into(),
            original_model_inputs: call.model_inputs.clone(),
            effective_model_inputs: transformed.as_ref().map(|(inputs, _)| inputs.clone()),
            transformation_ref: transformed.map(|(_, reference)| reference),
            normalized_model_inputs: normalized,
            run_inputs_ref: saved.snapshot.system_inputs.clone(),
            system_inputs,
            execution_args,
        };
        let input = BoundToolInput {
            binding_digest: data_digest(&data),
            data,
        };
        check_size(&input, self.limits.max_bound_bytes)?;
        let decision = self
            .authorize(
                &input.policy_request_for_run(&saved.snapshot),
                context,
                budget,
                false,
            )
            .await?;
        boundary(context, budget).await?;
        let record = ProtectedRecord::new(
            self.ids.next_id()?,
            1,
            serde_json::to_value(&input).expect("bound input serialization"),
        );
        let reference = record.reference().clone();
        let mut next = saved.snapshot;
        let expected_revision = next.revision;
        let (elapsed, now_ms) = budget.settlement_time(next.usage.elapsed_ms)?;
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::RevisionConflict, "revision"))?;
        next.usage.elapsed_ms = elapsed;
        next.timing.last_observed_at_ms = now_ms;
        next.tool_ledger
            .iter_mut()
            .find(|entry| &entry.call.call_id == call_id)
            .expect("found call")
            .call
            .bound_input_ref = Some(reference.clone());
        bounded(
            context,
            budget,
            budget.store().commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    control_commands: vec![],
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms,
                    snapshot: next,
                    messages: Vec::new(),
                    events: Vec::new(),
                    records: vec![record],
                },
            ),
        )
        .await?;
        boundary(context, budget).await?;
        Ok(ToolBindingResult {
            input,
            reference,
            decision,
        })
    }

    async fn authorize(
        &self,
        request: &PolicyRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
        lookup: bool,
    ) -> Result<PolicyDecision, ContractError> {
        boundary(context, budget).await?;
        let deadline = budget.call_deadline()?;
        let child = budget.cancellation().child_token();
        let policy_context = ExecutionContext::new(
            ExecutionContextData {
                scope: context.data.scope.clone(),
                principal_ref: context.data.principal_ref.clone(),
                capability_grant_ref: context.data.capability_grant_ref.clone(),
                trace_context: None,
                system_inputs: None,
            },
            child.clone(),
        );
        let check = self
            .policy
            .check(request, &policy_context, Some(deadline), None);
        tokio::pin!(check);
        let guard = child.drop_guard();
        let result = bounded(context, budget, &mut check).await;
        drop(guard);
        let decision = result?;
        boundary(context, budget).await?;
        match decision {
            PolicyDecision::Deny { .. } => Err(error(ErrorCode::AccessDenied, "policy")),
            PolicyDecision::RequireApproval { .. } if lookup => Err(error(
                ErrorCode::SystemInputApprovalRequired,
                "system_input.lookup",
            )),
            decision => Ok(decision),
        }
    }
}

fn check_selection(
    snapshot: &RunSnapshot,
    compiled: &CompiledTool,
    call: &ToolCall,
) -> Result<(), ContractError> {
    if call.descriptor_digest.as_ref() != Some(compiled.descriptor_digest())
        || !snapshot
            .profile
            .profile()
            .tools
            .iter()
            .any(|selection| match selection {
                ToolBindingRef::Catalog(reference) => {
                    reference.tool_id == compiled.descriptor().tool.id
                        && reference.version == compiled.descriptor().tool.version
                        && call.tool_name == compiled.descriptor().name
                }
                ToolBindingRef::Export(export) => {
                    export.alias.as_ref().unwrap_or(&compiled.descriptor().name) == &call.tool_name
                        && snapshot
                            .profile
                            .profile()
                            .adapters
                            .as_ref()
                            .is_some_and(|adapters| {
                                adapters
                                    .iter()
                                    .any(|adapter| adapter.binding_id == export.adapter_binding)
                            })
                }
            })
    {
        return Err(error(
            ErrorCode::InvalidToolInputContract,
            "tool_call.descriptor",
        ));
    }
    Ok(())
}

fn normalize_model_inputs(
    compiled: &CompiledTool,
    original: &JsonObject,
) -> Result<JsonObject, ContractError> {
    compiled.normalize_model_inputs(original)
}
fn required_parameter(compiled: &CompiledTool, parameter: &str) -> bool {
    compiled
        .input_schema()
        .get("required")
        .and_then(Value::as_array)
        .is_some_and(|required| required.iter().any(|name| name.as_str() == Some(parameter)))
}
fn validate_value(definition: &SystemInputDefinition, value: &Value) -> Result<(), ContractError> {
    let validator = compile_validator(&definition.value_schema).map_err(|_| {
        error(
            ErrorCode::SystemInputInvalid,
            &system_input_path(&definition.key),
        )
    })?;
    if !validator.is_valid(value) {
        return Err(error(
            ErrorCode::SystemInputInvalid,
            &system_input_path(&definition.key),
        ));
    }
    Ok(())
}

fn system_input_path(key: &Id) -> String {
    // Only registered metadata is named; JSON escaping prevents control characters
    // or punctuation from being interpreted as a path or leaking a supplied value.
    format!(
        "system_inputs[{}]",
        serde_json::to_string(key.as_str()).expect("serializable key")
    )
}

async fn boundary(context: &ExecutionContext, budget: &RunBudget) -> Result<(), ContractError> {
    if &context.data.scope != budget.scope() {
        return Err(error(ErrorCode::AccessDenied, "scope"));
    }
    if context.cancellation.is_cancelled() {
        return Err(error(ErrorCode::Cancelled, "input_binding"));
    }
    bounded(context, budget, budget.check_boundary()).await
}
async fn bounded<T>(
    context: &ExecutionContext,
    budget: &RunBudget,
    future: impl Future<Output = Result<T, ContractError>>,
) -> Result<T, ContractError> {
    if context.cancellation.is_cancelled() {
        return Err(error(ErrorCode::Cancelled, "input_binding"));
    }
    tokio::select! {
        biased;
        _ = context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "input_binding")),
        stopped = budget.wait_for_cancellation_or_deadline() => { stopped?; Err(error(ErrorCode::DeadlineExceeded, "input_binding")) },
        result = future => {
            if context.cancellation.is_cancelled() || budget.cancellation().is_cancelled() { return Err(error(ErrorCode::Cancelled, "input_binding")); }
            budget.call_deadline()?;
            result
        }
    }
}

async fn resolved_tool_selection(
    snapshot: &RunSnapshot,
    compiled: &CompiledTool,
    context: &ExecutionContext,
    budget: &RunBudget,
) -> Result<Option<ToolBindingRef>, ContractError> {
    let Some(reference) = &snapshot.assembly_ref else {
        if snapshot
            .profile
            .profile()
            .tools
            .iter()
            .any(|selection| matches!(selection, ToolBindingRef::Export(_)))
        {
            return Err(error(ErrorCode::InvalidSnapshot, "bound_input.assembly"));
        }
        return Ok(None);
    };
    let inputs = if let Some(reference) = &snapshot.system_inputs {
        let record = bounded(
            context,
            budget,
            budget
                .store()
                .read_record(budget.scope(), &reference.snapshot_ref),
        )
        .await?;
        let input = RunSystemInputs::from_value(record.value(), reference, budget.scope())?;
        SystemInputRegistry::new(input.definitions().values().cloned().collect())?
    } else {
        SystemInputRegistry::default()
    };
    let record = bounded(
        context,
        budget,
        budget.store().read_record(budget.scope(), reference),
    )
    .await?;
    let assembly = crate::ResolvedAssembly::restore(
        &serde_json::to_string(record.value())
            .map_err(|_| error(ErrorCode::InvalidJson, "bound_input.assembly"))?,
        &snapshot.profile,
        &inputs,
        &reference.digest,
    )?;
    let matches: Vec<_> = assembly
        .tools()
        .iter()
        .filter(|binding| {
            binding.compiled.digest() == compiled.digest()
                && binding.compiled.descriptor().name == compiled.descriptor().name
        })
        .collect();
    if matches.len() != 1 {
        return Err(error(
            ErrorCode::SystemInputsMismatch,
            "bound_input.selection",
        ));
    }
    Ok(match &matches[0].selection {
        selection @ ToolBindingRef::Export(_) => Some(selection.clone()),
        _ => None,
    })
}

async fn saved_tool_transform(
    snapshot: &RunSnapshot,
    compiled: &CompiledTool,
    call: &ToolCall,
    context: &ExecutionContext,
    budget: &RunBudget,
) -> Result<Option<(JsonObject, RecordRef)>, ContractError> {
    let Some(reference) = &snapshot.hook_plan_ref else {
        return Ok(None);
    };
    let record = bounded(
        context,
        budget,
        budget.store().read_record(budget.scope(), reference),
    )
    .await?;
    let plan = crate::HookPlan::restore(
        &serde_json::to_string(record.value())
            .map_err(|_| error(ErrorCode::InvalidJson, "hooks.plan"))?,
        budget.scope(),
        &reference.digest,
    )?;
    let definitions: Vec<_> = plan
        .definitions()
        .iter()
        .filter(|definition| definition.position == crate::HookPosition::BeforeTool)
        .collect();
    if definitions.is_empty() {
        return Ok(None);
    }
    let target = crate::HookTarget::BeforeTool {
        call_id: call.call_id.clone(),
    };
    let applications: Vec<_> = snapshot
        .hook_applications
        .iter()
        .filter(|application| application.target == target)
        .collect();
    if applications.len() != definitions.len() {
        return Err(error(
            ErrorCode::InvalidTransition,
            "hooks.before_tool_missing",
        ));
    }
    let mut inputs = call.model_inputs.clone();
    for (index, (definition, application)) in definitions.iter().zip(&applications).enumerate() {
        if definition.hook != application.hook {
            return Err(error(ErrorCode::InvalidSnapshot, "hooks.order"));
        }
        let record = bounded(
            context,
            budget,
            budget
                .store()
                .read_record(budget.scope(), &application.result_ref),
        )
        .await?;
        let record = crate::HookApplicationRecord::restore(
            &record,
            &plan,
            application,
            budget.scope(),
            budget.run_id(),
        )?;
        let crate::HookInput::BeforeTool {
            tool,
            descriptor_digest,
            compiled_digest,
            original_model_inputs,
            model_inputs,
        } = &record.input
        else {
            return Err(error(ErrorCode::InvalidSnapshot, "hooks.tool_input"));
        };
        if index == 0 && model_inputs == &compiled.normalize_model_inputs(&call.model_inputs)? {
            inputs = model_inputs.clone();
        }
        if tool != &compiled.to_model_tool()
            || descriptor_digest != compiled.descriptor_digest()
            || compiled_digest != compiled.digest()
            || original_model_inputs != &call.model_inputs
            || model_inputs != &inputs
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "hooks.tool_identity",
            ));
        }
        let Some(crate::HookOutput::Tool { model_inputs, deny }) = record.output else {
            return Err(error(ErrorCode::InvalidSnapshot, "hooks.tool_output"));
        };
        if deny.is_some() {
            return Err(error(ErrorCode::AccessDenied, "hooks.tool_denied"));
        }
        compiled.normalize_model_inputs(&model_inputs)?;
        inputs = model_inputs;
    }
    Ok(Some((
        inputs,
        applications
            .last()
            .expect("nonempty definitions")
            .result_ref
            .clone(),
    )))
}

/// Validate the exact saved transformation that a bound candidate claims to use.
pub(crate) fn validate_bound_transformation(
    value: &Value,
    snapshot: &RunSnapshot,
    call: &ToolCall,
    transformation: Option<&Value>,
) -> Result<(), ContractError> {
    let bound = BoundToolInput::from_value(value)?;
    let target = crate::HookTarget::BeforeTool {
        call_id: call.call_id.clone(),
    };
    let application = snapshot
        .hook_applications
        .iter()
        .rev()
        .find(|application| application.target == target);
    if bound.transformation_ref() != application.map(|application| &application.result_ref) {
        return Err(error(
            ErrorCode::SystemInputsMismatch,
            "bound_input.transform_reference",
        ));
    }
    match (bound.transformation_ref(), transformation) {
        (None, None) => Ok(()),
        (Some(reference), Some(value)) => {
            if crate::canonical_digest(value) != reference.digest {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.transform_digest",
                ));
            }
            let record: crate::HookApplicationRecord = serde_json::from_value(value.clone())
                .map_err(|_| error(ErrorCode::InvalidSnapshot, "bound_input.transform_record"))?;
            let crate::HookInput::BeforeTool {
                descriptor_digest,
                compiled_digest,
                original_model_inputs,
                ..
            } = record.input
            else {
                return Err(error(
                    ErrorCode::InvalidSnapshot,
                    "bound_input.transform_input",
                ));
            };
            if record.scope != snapshot.scope
                || record.run_id != snapshot.run_id
                || record.target != target
                || &descriptor_digest != bound.descriptor_digest()
                || &compiled_digest != bound.compiled_digest()
                || original_model_inputs != call.model_inputs
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.transform_identity",
                ));
            }
            match record.output {
                Some(crate::HookOutput::Tool {
                    model_inputs,
                    deny: None,
                }) if &model_inputs == bound.effective_model_inputs() => Ok(()),
                _ => Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.transform_output",
                )),
            }
        }
        _ => Err(error(
            ErrorCode::SystemInputsMismatch,
            "bound_input.transform_record",
        )),
    }
}

pub(crate) fn validate_bound_record(
    value: &Value,
    snapshot: &RunSnapshot,
    call: &ToolCall,
    run_inputs: Option<&RunSystemInputs>,
) -> Result<(), ContractError> {
    let input = BoundToolInput::from_value(value)?;
    input.validate_identity(
        &snapshot.scope,
        &snapshot.run_id,
        call,
        snapshot.system_inputs.as_ref(),
    )?;
    if input
        .selection()
        .is_some_and(|selection| !snapshot.profile.profile().tools.contains(selection))
    {
        return Err(error(
            ErrorCode::SystemInputsMismatch,
            "bound_input.selection",
        ));
    }
    for bound in input.system_inputs().values() {
        let data = run_inputs
            .ok_or_else(|| error(ErrorCode::SystemInputsMismatch, "bound_input.run_snapshot"))?;
        let definition = data
            .definitions()
            .get(&bound.key)
            .ok_or_else(|| error(ErrorCode::SystemInputsMismatch, "bound_input.definition"))?;
        if definition.version != bound.definition_version || definition.source != bound.source {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.definition",
            ));
        }
        if let Some(value) = &bound.resolved {
            validate_value(definition, &value.value)?;
        }
        if matches!(bound.source, SystemInputSource::Run {}) {
            let expected = data.values().get(bound.key.as_str());
            if bound.resolved.as_ref().map(|resolved| &resolved.value) != expected
                || bound.resolved.as_ref().is_some_and(|resolved| {
                    resolved.revision.as_str()
                        != snapshot
                            .system_inputs
                            .as_ref()
                            .expect("snapshot supplied")
                            .snapshot_ref
                            .revision
                            .to_string()
                })
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.run_value",
                ));
            }
        }
    }
    Ok(())
}

struct ByteCounter {
    total: usize,
    limit: usize,
}
impl io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.total = self
            .total
            .checked_add(bytes.len())
            .filter(|size| *size <= self.limit)
            .ok_or_else(|| io::Error::other("input size limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn check_size(value: &impl Serialize, limit: usize) -> Result<(), ContractError> {
    serde_json::to_writer(&mut ByteCounter { total: 0, limit }, value).map_err(|_| limit_error())
}
fn limit_error() -> ContractError {
    error(ErrorCode::InputBindingLimitExceeded, "input_binding.limits")
}
fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
```

## `crates/wickle/src/lib.rs`

```rust
//! Wickle, an agent engine for Rust applications.
//!
//! Profiles, scoped metadata resolution, and versioned execution data contracts.
//! The agent driver runs model/tool loops with scoped ports, separate system
//! inputs, persisted attempt accounting, and explicit effect outcomes.
//!
//! Runtime objects stay in Host code. Only documented data contracts are
//! serialized; successful decoding does not authenticate a caller.
//!
//! Internal modules are not extension points; use the root exports.
//! ```compile_fail
//! use wickle::serialization::canonical_digest;
//! ```

mod agent;
mod artifacts;
mod budget;
mod canonical;
mod execution_contracts;
pub use execution_contracts::{
    AcceptedSegmentCommand, AppState, BeginSegmentRequest, BeginSegmentResult, ControlAction,
    ControlCommand, ControlReceipt, ExecutionHistory, ExecutionRecordVersion, ExecutionSegment,
    ExecutionTransactions, InterruptionAction, InterruptionCause, InterruptionDecision,
    InterruptionInfo, InterruptionPolicy, InterruptionRecord, PreparedStepRecord, RequestSnapshot,
    SegmentOutcome, SegmentStart, SegmentTransition, StoredControlCommand,
};
mod clock;
mod component_runtime;
mod context;
mod context_projection;
mod context_source;
mod context_strategy;
mod error;
mod hooks;
mod input_binding;
mod message;
mod model;
mod model_options;
pub use model_options::{
    ModelConfiguration, ModelOptionSource, merge_model_options, validate_inference_options,
};
mod model_catalog;
mod model_dispatch;
mod model_execution;
mod model_protocol;
mod model_routing;
mod policy;
mod profile;
mod resolution;
mod run;
mod serialization;
pub use canonical::{
    CanonicalizationVersion, JsonTextLimits, canonicalize_json_text, versioned_digest_json,
};
mod inspection;
mod skills;
mod state;
mod tool_execution;
mod tool_schema;
mod views;
pub use inspection::{
    AttemptComposition, CompositionReport, ConstraintComposition, ContextDisclosure,
    FragmentComposition, InspectionEvidence, InspectionFragmentRef, InspectionOptions,
    InspectionStatus, ModelComposition, OutcomeComposition, SelectionComposition, StepComposition,
    StepRef, ToolComposition, UnresolvedInspectionField,
};

pub use agent::{
    Agent, AgentBindings, AgentSettings, CancelReceipt, ComponentReleaseView, HookObservationView,
    ModelTokenEstimator, PersistenceFailure, RunHandle, UnconfirmedToolEffect, create_agent,
};
pub use artifacts::{
    ArtifactCallContext, ArtifactData, ArtifactInput, ArtifactLimits, ArtifactMetadata,
    ArtifactPreview, ArtifactRuntime, ArtifactStore, MemoryArtifactStore,
};
pub use budget::{AttemptReservation, ReservationKind, RunBudget, RunTiming};
pub use clock::{Clock, ClockReading, IdSource, RandomIdSource, SystemClock};
pub use component_runtime::{
    AdapterBindingState, AdapterCloseContext, AdapterDefinition, AdapterExportDefinition,
    AdapterExportInstance, AdapterFactory, AdapterInitContext, AdapterInstance, BoundCapabilities,
    ComponentBindContext, ComponentBindPurpose, ComponentRelease, ComponentReleaseContext,
    ComponentReleaseFailure, ComponentReleaseReport, ComponentResolveContext, ComponentRuntime,
    ResolvedAdapterBinding, ResolvedAssembly, ResolvedConnection, ResolvedHookBinding,
    ResolvedToolBinding,
};
pub use context_projection::{
    CONTEXT_ASSEMBLER_VERSION, ContextAssembler, ContextItem, ContextLifetime, ContextOrigin,
    ContextPriority, ContextProjection, InstructionAssetContent, PinnedPromptTool, ProjectionInput,
    ProjectionLimits, PromptSnapshot, PromptToolBinding, ScopedOpaque, SkillManifest,
};
pub use context_source::{
    ContextBatch, ContextCallContext, ContextRequest, ContextResult, ContextSource,
    ContextSourceDefinition, ContextSourcePlan, ContextSourceRegistration, ContextSourceRegistry,
    ContextSourceRuntime, ContextSourceUsage, ContextTokenEstimator, ContextUseRequest,
    PlannedContextSource, ResolvedSourceBinding,
};
pub use context_strategy::{
    BoundedContextStrategy, CompactionRequest, ContextCompactor, ContextDecision, ContextPlan,
    ContextPreview, ContextRevision, ContextRewriteLimits, ContextRuntime, ContextSegment,
    ContextSelectionInput, ContextStrategy, ContextStrategyContext, ContextStrategyDefinition,
    HostContextCompactor, ModelCompactorConfig,
};
pub use hooks::{
    HookApplication, HookApplicationRecord, HookContext, HookContextAddition, HookDefinition,
    HookHandler, HookInput, HookObservation, HookObservationStatus, HookOutput, HookPlan,
    HookRegistration, HookRegistry, HookRuntime, HookTarget, HookTransform,
};
pub use input_binding::{
    BoundSystemInput, BoundToolInput, InputBinder, InputBindingLimits, ResolvedSystemInput,
    RunSystemInputs, SystemInputResolveContext, SystemInputResolveRequest, SystemInputResolver,
    ToolBindingResult,
};
pub use model_catalog::{
    CatalogRequirements, ModelAlias, ModelBinding, ModelCapabilities, ModelCatalog,
    ModelCatalogSnapshot, ModelDefinition, ModelDefinitionRef, ModelEvidence, ModelLifecycle,
    ModelSupportStatus, ModelValidationEvidence, ModelValidationKind, ResolvedCatalogBinding,
};
pub use model_protocol::{
    ModelCallContext, ModelContent, ModelEvent, ModelFinish, ModelMessage, ModelOutput, ModelPort,
    ModelPortBinding, ModelProtocolError, ModelProtocolErrorCode, ModelRequest, ModelResponse,
    ModelResponseLimits, ModelResponseMetadata, ModelRole, ModelTool, OpaqueContinuation,
    ProposedToolCall, ToolCallValidation, collect_model_response,
};
pub use model_routing::{
    MAX_ROUTE_FALLBACKS, MAX_ROUTING_RULES, ModelRouter, ROUTING_SNAPSHOT_VERSION, RouteSelection,
    RouteSelectionReason, RoutingPolicy, RoutingRule, RoutingSnapshot,
};
pub use policy::{
    ApprovalChallenge, Guarded, PolicyAction, PolicyContext, PolicyDecision, PolicyGate,
    PolicyPort, PolicyRequest, ToolApproval, ToolPolicyInput,
};
pub use skills::{
    LoadedSkill, PlannedSkill, SkillBindings, SkillCallContext, SkillDefinition, SkillLimits,
    SkillPlan, SkillResolver, SkillRuntime,
};
pub use state::{
    AdmissionInput, AdmissionResult, CommitInput, EventPage, MAX_EVENT_PAGE_SIZE, MemoryStateStore,
    ProtectedRecord, RunLease, STATE_STORE_CHECKPOINT_VERSION, StateStore, StateStoreCapabilities,
    StateStoreCheckpoint, StoredRun,
};
pub use tool_execution::{
    ExternalReceiptContext, ExternalReceiptRequest, ExternalReceiptVerifier,
    PreparedToolResolution, SerialToolRound, ToolEffect, ToolExecutionContext, ToolExecutionLimits,
    ToolExecutionOutcome, ToolExecutionResult, ToolExecutor, ToolRegistration, ToolRegistry,
    ToolRoundOutcome,
};
pub use tool_schema::{
    CompiledTool, SchemaCompiler, SystemInputDefinition, SystemInputRegistry, SystemInputSource,
    TOOL_SCHEMA_COMPILER_VERSION, ToolConcurrency, ToolDescriptor, ToolRetryPolicy, ToolSideEffect,
};
pub use views::{ArtifactView, EventView, RunView};

pub use context::{
    ExecutionContext, ExecutionContextData, PortFuture, PortStream, Scope, SystemInputs,
};
pub use error::{ContractError, ErrorCode};
pub use message::{
    ArtifactRef, ContentBlock, EvidenceRef, Failure, InputContent, Message, MessageOrigin,
    MessageRole, ProviderToolArguments, RecordRef, ToolCall, ToolResult, ToolResultStatus,
    Visibility,
};
pub use model::{
    ApiContract, ModelAttemptState, ModelFailureKind, ModelInvocationRecord, ModelPurpose,
    ModelUsage, ResolvedModelRoute, RouteRequest, UsageMeasurement, VersionPolicy,
    VersionSemantics,
};
pub use model_dispatch::{
    ModelDispatcher, ModelInspectionContext, ModelRouteAvailability, ModelRouteInspector,
    ModelRouteObservation,
};
pub use model_execution::{
    ModelExchange, ModelExchangeOutcome, ModelProjectionContext, ModelRequestProjector,
    ModelRetryPolicy, ProjectedModelRequest, RoutedModelInput, StoredModelResponse,
};
pub use profile::{
    AdapterBindingRef, AgentProfile, CatalogHookRef, CatalogSourceRef, CatalogToolRef,
    CompletionPolicy, ConnectorBindingRef, ContextPolicy, ContextSourceBinding, ContextSourceRef,
    ContextTrigger, ExportRef, HookPosition, HookRef, InstructionAsset, InstructionText,
    Instructions, OutputContract, PROFILE_SCHEMA_VERSION, ProfileSchemaVersion, RunLimits,
    SkillRef, ToolBindingRef, VersionedRef,
};
pub use resolution::{
    ComponentKind, ComponentMetadata, ComponentRef, ExportKind, ExportMetadata, ProfileResolver,
    ProfileValidator, ResolvedComponent, ResolvedProfile,
};
pub use run::{
    ApprovalTarget, BudgetKind, BudgetUsage, CompletionBasis, EphemeralEvent, InputRequest,
    OutcomeResult, RUN_EVENT_SCHEMA_VERSION, RUN_SNAPSHOT_SCHEMA_VERSION, ResumeAction,
    ResumeCommand, ResumeReceipt, RunEvent, RunEventPayload, RunEventSchemaVersion, RunOutcome,
    RunPhase, RunRequest, RunSnapshot, RunSnapshotSchemaVersion, RunStatus, RunTrigger,
    SessionSchemaVersion, SessionSnapshot, SourceExecutionState, SystemInputSnapshotRef,
    ToolCallState, ToolLedgerEntry, VerificationSummary, VerificationVerdict, WaitState,
    WaitTarget, admission_digest,
};
pub use serialization::{
    Id, JsonDigest, JsonObject, canonical_digest, canonical_digest_json, parse_json,
};

mod verification;
pub use verification::{
    OutputSchemaDefinition, VerificationCandidate, VerificationDecision, VerificationInput,
    VerificationLimits, VerificationModel, VerificationModelRequest, VerificationPlan,
    VerificationRuntime, Verifier, VerifierContext, VerifierDefinition,
};

pub use verification::SchemaVerifier;

mod future;

pub use tool_execution::ToolReconciliation;

mod recovery;
pub use recovery::RecoveryReceipt;

mod provider_tool_schema;
pub use provider_tool_schema::{
    ArgumentDecodePlan, ArgumentFieldMapping, ArgumentValueEncoding, CompiledToolContract,
    NativeToolSchemaCompiler, ProviderToolProjection, ProviderToolSchemaCompiler,
    ProviderToolSchemaLimits, ProviderToolTarget, ToolConstraintEnforcement,
    ToolConstraintFragment, parse_provider_arguments,
};

mod context_fragment;
pub use context_fragment::{
    CONTEXT_FRAGMENT_ASSEMBLER, ContextFragment, FragmentIdentity, FragmentOwner, FragmentValue,
    select_context_fragments,
};

mod context_lineage;
pub use context_lineage::ContextLineage;

mod prepared_step;
pub use prepared_step::{
    PreparedModelProjection, ProjectionProvenance, ResolvedToolSet, ResolvedToolSetEntry,
};

mod interruption;
pub use interruption::{
    AppStateSchema, ExecutionStopReceipt, InterruptionDecisionRecord, InterruptionPlan,
    InterruptionPolicyBinding,
};
```

## `crates/wickle/src/model_execution/prepared.rs`

```rust
use super::*;
use crate::*;

pub(super) struct StoredPreparation {
    pub reference: RecordRef,
    pub projection: PreparedModelProjection,
}
impl ModelExchange {
    pub(super) async fn load_preparation(
        &self,
        input: &RoutedModelInput,
        route: &ResolvedModelRoute,
        configuration: &ModelConfiguration,
        budget: &RunBudget,
    ) -> Result<Option<StoredPreparation>, ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        for reference in saved.snapshot.prepared_steps.iter().rev() {
            let record = budget
                .store()
                .read_record(budget.scope(), reference)
                .await?;
            let root: PreparedStepRecord =
                serde_json::from_value(record.value().clone()).map_err(|_| revision_error())?;
            if root.model_step_id != input.model_step_id || root.purpose != input.routing.purpose {
                continue;
            }
            let configuration_record = budget
                .store()
                .read_record(budget.scope(), &root.model_configuration)
                .await?;
            let pinned_route: ResolvedModelRoute =
                serde_json::from_value(configuration_record.value()["route"].clone())
                    .map_err(|_| revision_error())?;
            if &pinned_route != route {
                continue;
            }
            let pinned: ModelConfiguration =
                serde_json::from_value(configuration_record.value()["configuration"].clone())
                    .map_err(|_| revision_error())?;
            if &pinned != configuration {
                return Err(ContractError::new(
                    ErrorCode::RequestConflict,
                    "prepared.configuration",
                ));
            }
            let projection_record = budget
                .store()
                .read_record(budget.scope(), &root.context_projection)
                .await?;
            let projection: PreparedModelProjection =
                serde_json::from_value(projection_record.value().clone())
                    .map_err(|_| revision_error())?;
            if root.scope != *budget.scope()
                || root.run_id != *budget.run_id()
                || root.profile_digest != *saved.snapshot.profile.profile_digest()
                || root.assembly_ref != saved.snapshot.assembly_ref
                || projection.scope != root.scope
                || projection.run_id != root.run_id
                || projection.request.request_id != root.model_step_id
                || projection.request.route != *route
                || projection.request.purpose != root.purpose
                || projection.fingerprint() != root.projection_fingerprint
            {
                return Err(ContractError::new(
                    ErrorCode::InvalidSnapshot,
                    "prepared.identity",
                ));
            }
            projection.request.validate()?;
            return Ok(Some(StoredPreparation {
                reference: reference.clone(),
                projection,
            }));
        }
        Ok(None)
    }
    pub(super) async fn save_preparation(
        &self,
        mut prepared: ProjectedModelRequest,
        configuration: &ModelConfiguration,
        selection: &RouteSelection,
        budget: &RunBudget,
    ) -> Result<StoredPreparation, ContractError> {
        budget.check_boundary().await?;
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        prepared.provenance.context_revision_ref = saved.snapshot.context_revision_ref.clone();
        let step = prepared.request.request_id.clone();
        let mut step_input = None;
        for reference in &saved.snapshot.model_step_inputs {
            let record = budget
                .store()
                .read_record(budget.scope(), reference)
                .await?;
            if record.value()["input"]["model_step_id"] == serde_json::json!(step) {
                prepared.provenance.through_sequence = record.value()["through_sequence"]
                    .as_u64()
                    .ok_or_else(revision_error)?;
                step_input = Some(reference.clone());
                break;
            }
        }
        let step_input = step_input.ok_or_else(revision_error)?;
        let mut revision = 1u64;
        for reference in &saved.snapshot.prepared_steps {
            let record = budget
                .store()
                .read_record(budget.scope(), reference)
                .await?;
            let prior: PreparedStepRecord =
                serde_json::from_value(record.value().clone()).map_err(|_| revision_error())?;
            if prior.model_step_id == step {
                revision = prior
                    .projection_revision
                    .get()
                    .checked_add(1)
                    .ok_or_else(revision_error)?;
            }
        }
        let key = crate::canonical_digest(&serde_json::json!([budget.run_id(), step, revision]));
        let child = |name: &str, value| -> Result<ProtectedRecord, ContractError> {
            Ok(ProtectedRecord::new(
                Id::new(format!("prepared-{key}-{name}"))?,
                1,
                value,
            ))
        };
        let tool_set = ResolvedToolSet {
            schema_version: "wickle.resolved-tool-set.v1".into(),
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            entries: prepared.tool_set,
        };
        tool_set.validate_shape()?;
        if prepared.compiled_tools.len() != tool_set.entries.len()
            || prepared.compiled_tools.len() != prepared.request.tools.len()
        {
            return Err(ContractError::new(
                ErrorCode::InvalidContract,
                "prepared.tool_count",
            ));
        }
        let target = ProviderToolTarget::for_route(&selection.route);
        let mut records = Vec::new();
        let mut contracts = Vec::new();
        for (index, ((entry, contract), wire)) in tool_set
            .entries
            .iter()
            .zip(&prepared.compiled_tools)
            .zip(&prepared.request.tools)
            .enumerate()
        {
            let tool = entry.restore_tool()?;
            let restored = CompiledToolContract::restore(
                &serde_json::to_string(contract).map_err(|_| revision_error())?,
                &tool,
                &target,
                contract.digest(),
                ProviderToolSchemaLimits::default(),
            )?;
            if restored.wire_tool() != wire {
                return Err(ContractError::new(
                    ErrorCode::InvalidContract,
                    "prepared.wire_tool",
                ));
            }
            let record = child(
                &format!("tool-{index}"),
                serde_json::to_value(contract).map_err(|_| revision_error())?,
            )?;
            contracts.push(record.reference().clone());
            records.push(record);
        }
        let tools = child(
            "tool-set",
            serde_json::to_value(tool_set).map_err(|_| revision_error())?,
        )?;
        let configuration_record = child(
            "configuration",
            serde_json::json!({"route":selection.route,"configuration":configuration}),
        )?;
        let projection = PreparedModelProjection {
            schema_version: "wickle.prepared-projection.v1".into(),
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            request: prepared.request,
            input_tokens: prepared.input_tokens,
            provenance: prepared.provenance,
        };
        let projection_record = child(
            "projection",
            serde_json::to_value(&projection).map_err(|_| revision_error())?,
        )?;
        let root = PreparedStepRecord {
            step_input,
            schema_version: ExecutionRecordVersion::V1,
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            profile_digest: saved.snapshot.profile.profile_digest().clone(),
            assembly_ref: saved.snapshot.assembly_ref.clone(),
            projection_fingerprint: projection.fingerprint(),
            change_reason: Id::new(routed::reason_code(selection.reason))?,
            purpose: projection.request.purpose,
            compiled_tools: contracts,
            model_step_id: step,
            projection_revision: revision.try_into().map_err(|_| revision_error())?,
            model_configuration: configuration_record.reference().clone(),
            tool_set: tools.reference().clone(),
            context_projection: projection_record.reference().clone(),
            assembler: VersionedRef {
                id: Id::new("wickle-prepared-model")?,
                version: Id::new("1")?,
            },
        };
        let record = child(
            "step",
            serde_json::to_value(&root).map_err(|_| revision_error())?,
        )?;
        let reference = record.reference().clone();
        records.extend([tools, configuration_record, projection_record, record]);
        let mut snapshot = saved.snapshot;
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(revision_error)?;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        snapshot.prepared_steps.push(reference.clone());
        if root.purpose == ModelPurpose::Agent {
            snapshot.model_step_id = Some(root.model_step_id.clone());
            snapshot.active_prepared_step = Some(reference.clone());
        }
        budget
            .store()
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    control_commands: vec![],
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    records,
                    messages: vec![],
                    events: vec![],
                },
            )
            .await?;
        budget.check_boundary().await?;
        Ok(StoredPreparation {
            reference,
            projection,
        })
    }
}
```

## `crates/wickle/src/provider_tool_schema.rs`

```rust
//! Pure, bounded provider projection of an already separated Tool input contract.
use crate::{
    ApiContract, CompiledTool, ContractError, ErrorCode, Id, JsonDigest, JsonObject, ModelTool,
    VersionedRef, canonical_digest, parse_json, serialization::data_digest,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, fmt};

/// Exact provider protocol and capability revision used for compilation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderToolTarget {
    /// Exact model/release, absent only in older or manually unqualified contracts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<VersionedRef>,
    /// Provider namespace, including deployment-specific provider adapters.
    pub provider: Id,
    /// Exact operation and API version.
    pub api_contract: ApiContract,
    /// Pinned target capability revision.
    pub capability_revision: Id,
}
impl ProviderToolTarget {
    /// Capture the selected route without connection metadata or credentials.
    pub fn for_route(route: &crate::ResolvedModelRoute) -> Self {
        Self {
            model: Some(VersionedRef {
                id: route.model_id.clone(),
                version: route.model_version.clone(),
            }),
            provider: route.provider.clone(),
            api_contract: route.api_contract.clone(),
            capability_revision: route.capability_revision.clone(),
        }
    }
    fn matches_saved(&self, saved: &Self) -> bool {
        self.provider == saved.provider
            && self.api_contract == saved.api_contract
            && self.capability_revision == saved.capability_revision
            && saved
                .model
                .as_ref()
                .is_none_or(|model| self.model.as_ref() == Some(model))
    }
}
/// Finite bounds on compilation, persisted projection and incoming arguments.
#[derive(Debug, Clone, Copy)]
pub struct ProviderToolSchemaLimits {
    /// Maximum serialized canonical or wire schema/tool bytes.
    pub max_schema_bytes: usize,
    /// Maximum schema nesting before traversal or serialization.
    pub max_schema_depth: usize,
    /// Maximum total serialized compiled contract bytes, including explanations.
    pub max_contract_bytes: usize,
    /// Maximum provider argument bytes before parsing.
    pub max_argument_bytes: usize,
}
impl Default for ProviderToolSchemaLimits {
    fn default() -> Self {
        Self {
            max_schema_bytes: 65_536,
            max_schema_depth: 64,
            max_contract_bytes: 262_144,
            max_argument_bytes: 65_536,
        }
    }
}
/// Reversible representation of a single model-owned field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArgumentValueEncoding {
    /// Preserve the JSON value, including explicit null.
    Identity {},
    /// A JSON document encoded as a string. Optional values use [] for omission
    /// and `[value]` for a supplied value, keeping explicit null distinct.
    JsonText {
        /// Whether the string represents an optional zero-or-one value array.
        optional: bool,
    },
    /// Encode omission separately from null using an object envelope.
    Presence {
        /// Boolean discriminator: false means omitted, true means supplied.
        present_key: String,
        /// Required value member; must be null when present is false.
        value_key: String,
    },
}
/// One-to-one mapping from a wire property to an exposed canonical property.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArgumentFieldMapping {
    /// Property emitted by the provider.
    pub wire_name: String,
    /// Original model-owned property, never a system-owned property.
    pub canonical_name: String,
    /// Value and omission restoration rule.
    pub encoding: ArgumentValueEncoding,
}
/// Stored codec. It never guesses that null means omission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArgumentDecodePlan {
    /// Property names and values are unchanged.
    Identity {},
    /// Explicit complete field mapping; unknown wire properties are errors.
    Fields {
        /// Ordered mappings with unique wire and canonical names.
        fields: Vec<ArgumentFieldMapping>,
    },
}
/// Compiler output before the core stamps original identity and explanations.
#[derive(Debug, Clone)]
pub struct ProviderToolProjection {
    /// The exact Tool definition submitted to the provider.
    pub wire_tool: ModelTool,
    /// Reversible normalization back into original model-owned arguments.
    pub decode_plan: ArgumentDecodePlan,
}
/// Pure trusted adapter extension. Input contains only the model-visible Tool;
/// hidden definitions, system values, credentials and runtime handles are absent.
pub trait ProviderToolSchemaCompiler: Send + Sync {
    /// Immutable implementation identity; change its version when output changes.
    fn reference(&self) -> VersionedRef;
    /// Preserve native constraints where supported. Unsupported representation
    /// must use a relaxed schema plus a reversible codec, never delete the Tool.
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError>;
}
/// Compiler for protocols that accept the original model-visible JSON Schema.
#[derive(Debug, Clone, Copy, Default)]
pub struct NativeToolSchemaCompiler;
impl ProviderToolSchemaCompiler for NativeToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new("wickle-native-tool-schema").expect("static id"),
            version: Id::new("1").expect("static version"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
        _: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        Ok(ProviderToolProjection {
            wire_tool: tool.clone(),
            decode_plan: ArgumentDecodePlan::Identity {},
        })
    }
}
/// Deterministic trusted explanation associated with this exact Tool projection.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolConstraintFragment {
    /// Stable content-addressed identity, ordered in the compiled contract.
    pub id: Id,
    /// Only canonical model-visible schema and codec instructions.
    pub text: String,
    /// Digest of the exact explanation text.
    pub digest: JsonDigest,
}
impl fmt::Debug for ToolConstraintFragment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolConstraintFragment")
            .field("id", &self.id)
            .field("digest", &self.digest)
            .finish()
    }
}
/// Where an original schema node is enforced. Core validation is always required.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolConstraintEnforcement {
    /// JSON pointer into the canonical model schema; empty denotes the whole schema.
    pub canonical_pointer: String,
    /// Confirmed native under an identical schema and identity codec. False is
    /// conservative: a relaxed wire schema can still enforce part of this node.
    pub provider_native: bool,
    /// Included in the canonical constraint explanation.
    pub context_text: bool,
    /// Original validation must occur after decoding, before execution.
    pub core: bool,
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContractData {
    schema_version: String,
    tool: VersionedRef,
    canonical_name: Id,
    descriptor_digest: JsonDigest,
    canonical_schema_digest: JsonDigest,
    compiler: VersionedRef,
    target: ProviderToolTarget,
    wire_tool: ModelTool,
    decode_plan: ArgumentDecodePlan,
    fragments: Vec<ToolConstraintFragment>,
    enforcement: Vec<ToolConstraintEnforcement>,
}
/// Immutable route-specific contract. Serialize only to protected storage; submit
/// wire_tool and constraint_fragments to the model, not the whole record.
#[derive(Clone, Serialize)]
pub struct CompiledToolContract {
    data: ContractData,
    digest: JsonDigest,
}
impl fmt::Debug for CompiledToolContract {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompiledToolContract")
            .field("tool", &self.data.tool)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}
impl CompiledToolContract {
    /// Compile only after canonical ownership separation and validate finite output.
    pub fn compile(
        tool: &CompiledTool,
        target: ProviderToolTarget,
        compiler: &dyn ProviderToolSchemaCompiler,
        limits: ProviderToolSchemaLimits,
    ) -> Result<Self, ContractError> {
        check_limits(limits)?;
        let visible = tool.to_model_tool();
        depth_bound(&visible.model_input_schema, limits.max_schema_depth)?;
        bounded(&visible, limits.max_schema_bytes)?;
        let reference = compiler.reference();
        let projection = compiler.compile(&visible, &target)?;
        if compiler.reference() != reference {
            return Err(invalid("provider_tool.compiler_revision"));
        }
        Self::build(tool, target, reference, projection, limits)
    }
    fn build(
        tool: &CompiledTool,
        target: ProviderToolTarget,
        compiler: VersionedRef,
        projection: ProviderToolProjection,
        limits: ProviderToolSchemaLimits,
    ) -> Result<Self, ContractError> {
        check_limits(limits)?;
        depth_bound(tool.model_input_schema(), limits.max_schema_depth)?;
        depth_bound(
            &projection.wire_tool.model_input_schema,
            limits.max_schema_depth,
        )?;
        bounded(&projection.wire_tool, limits.max_schema_bytes)?;
        let schema = &projection.wire_tool.model_input_schema;
        if !valid_name(projection.wire_tool.name.as_str())
            || schema.get("type") != Some(&json!("object"))
            || schema.get("additionalProperties") != Some(&json!(false))
        {
            return Err(invalid("provider_tool.wire_boundary"));
        }
        crate::tool_schema::compile_validator(schema)?;
        validate_codec(tool.model_input_schema(), schema, &projection.decode_plan)?;
        let identity = matches!(projection.decode_plan, ArgumentDecodePlan::Identity {});
        let explained = schema != tool.model_input_schema() || !identity;
        let fragments = if explained {
            let mut text = format!(
                "Tool {}: arguments must satisfy this canonical JSON Schema after decoding: {}\nDecode representation: {}. Field mappings restore wire_name to canonical_name. For a presence envelope, both members are required: true marks a supplied value (including explicit null); false with a null value placeholder means omission. Preserve omission and explicit null as distinct values.",
                projection.wire_tool.name,
                serde_json::to_string(tool.model_input_schema())
                    .map_err(|_| invalid("provider_tool.schema"))?,
                serde_json::to_string(&projection.decode_plan)
                    .map_err(|_| invalid("provider_tool.codec"))?
            );
            if matches!(&projection.decode_plan, ArgumentDecodePlan::Fields { fields } if fields.iter().any(|field| matches!(field.encoding, ArgumentValueEncoding::JsonText { .. })))
            {
                text.push_str(" For json_text, the wire value is a JSON string parsed by the core. With optional=false it encodes the canonical value itself. With optional=true it must encode [] for omission or [value] for a supplied value, including [null] for explicit null. Nested optional properties remain absent inside that JSON document; do not replace absence with null.");
            }
            let digest = data_digest(&text);
            vec![ToolConstraintFragment {
                id: Id::new(format!(
                    "tool-constraints-{}",
                    canonical_digest(&json!(text))
                ))?,
                text,
                digest,
            }]
        } else {
            vec![]
        };
        let mut enforcement = Vec::new();
        collect_enforcement(
            tool.model_input_schema(),
            schema,
            "",
            identity && schema == tool.model_input_schema(),
            explained,
            &mut enforcement,
        );
        let data = ContractData {
            schema_version: "wickle.provider-tool-contract.v1".into(),
            tool: tool.descriptor().tool.clone(),
            canonical_name: tool.descriptor().name.clone(),
            descriptor_digest: tool.descriptor_digest().clone(),
            canonical_schema_digest: tool.model_schema_digest().clone(),
            compiler,
            target,
            wire_tool: projection.wire_tool,
            decode_plan: projection.decode_plan,
            fragments,
            enforcement,
        };
        let result = Self {
            digest: data_digest(&data),
            data,
        };
        bounded(&result, limits.max_contract_bytes)?;
        Ok(result)
    }
    /// Restore against the trusted original Tool, destination and expected digest.
    /// This uses the saved codec and never invokes a newer compiler implementation.
    pub fn restore(
        text: &str,
        tool: &CompiledTool,
        target: &ProviderToolTarget,
        expected: &JsonDigest,
        limits: ProviderToolSchemaLimits,
    ) -> Result<Self, ContractError> {
        check_limits(limits)?;
        if text.len() > limits.max_contract_bytes {
            return Err(invalid("provider_tool.size"));
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Saved {
            data: ContractData,
            digest: JsonDigest,
        }
        let saved: Saved = serde_json::from_value(parse_json(text)?)
            .map_err(|_| invalid("provider_tool.record"))?;
        if &saved.digest != expected
            || data_digest(&saved.data) != *expected
            || !target.matches_saved(&saved.data.target)
        {
            return Err(invalid("provider_tool.identity"));
        }
        let rebuilt = Self::build(
            tool,
            saved.data.target.clone(),
            saved.data.compiler.clone(),
            ProviderToolProjection {
                wire_tool: saved.data.wire_tool.clone(),
                decode_plan: saved.data.decode_plan.clone(),
            },
            limits,
        )?;
        if rebuilt.data != saved.data || rebuilt.digest != *expected {
            return Err(invalid("provider_tool.identity"));
        }
        Ok(rebuilt)
    }
    pub(crate) fn inspection(
        value: Value,
    ) -> Result<crate::inspection::SavedToolInspection, ContractError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Saved {
            data: ContractData,
            digest: JsonDigest,
        }
        let saved: Saved =
            serde_json::from_value(value).map_err(|_| invalid("provider_tool.record"))?;
        if saved.data.schema_version != "wickle.provider-tool-contract.v1"
            || data_digest(&saved.data) != saved.digest
        {
            return Err(invalid("provider_tool.identity"));
        }
        Ok(crate::inspection::SavedToolInspection {
            tool: saved.data.tool,
            canonical_name: saved.data.canonical_name,
            canonical_schema_digest: saved.data.canonical_schema_digest,
            compiler: saved.data.compiler,
            target: saved.data.target,
            wire_tool: saved.data.wire_tool,
            decode_plan_digest: data_digest(&saved.data.decode_plan),
            digest: saved.digest,
            fragments: saved.data.fragments,
            enforcement: saved.data.enforcement,
        })
    }
    /// Original model-facing Tool name to restore after provider name mapping.
    pub fn canonical_name(&self) -> &Id {
        &self.data.canonical_name
    }
    /// Original registered Tool identity.
    pub fn tool(&self) -> &VersionedRef {
        &self.data.tool
    }
    /// Frozen provider-facing Tool, excluding hidden input metadata.
    pub fn wire_tool(&self) -> &ModelTool {
        &self.data.wire_tool
    }
    /// Ordered trusted fragments that must accompany the Tool definition.
    pub fn constraint_fragments(&self) -> &[ToolConstraintFragment] {
        &self.data.fragments
    }
    /// Exact original constraint locations and enforcement mechanisms.
    pub fn enforcement(&self) -> &[ToolConstraintEnforcement] {
        &self.data.enforcement
    }
    /// Pinned compiler identity and version.
    pub fn compiler(&self) -> &VersionedRef {
        &self.data.compiler
    }
    /// Exact destination protocol/capability revision.
    pub fn target(&self) -> &ProviderToolTarget {
        &self.data.target
    }
    /// Protected compilation identity.
    pub fn digest(&self) -> &JsonDigest {
        &self.digest
    }
    /// Encode canonical historical model arguments for this exact provider
    /// representation. System inputs never belong in this map.
    pub fn encode_arguments(&self, input: &JsonObject) -> Result<JsonObject, ContractError> {
        match &self.data.decode_plan {
            ArgumentDecodePlan::Identity {} => Ok(input.clone()),
            ArgumentDecodePlan::Fields { fields } => {
                if input
                    .keys()
                    .any(|key| !fields.iter().any(|field| &field.canonical_name == key))
                {
                    return Err(arguments());
                }
                let mut output = JsonObject::new();
                for field in fields {
                    let value = input.get(&field.canonical_name);
                    match &field.encoding {
                        ArgumentValueEncoding::Identity {} => {
                            if let Some(value) = value {
                                output.insert(field.wire_name.clone(), value.clone());
                            }
                        }
                        ArgumentValueEncoding::JsonText { optional } => {
                            if *optional || value.is_some() {
                                let encoded = if *optional {
                                    serde_json::to_string(&value.into_iter().collect::<Vec<_>>())
                                } else {
                                    serde_json::to_string(value.expect("present value"))
                                }
                                .map_err(|_| arguments())?;
                                output.insert(field.wire_name.clone(), Value::String(encoded));
                            }
                        }
                        ArgumentValueEncoding::Presence {
                            present_key,
                            value_key,
                        } => {
                            let envelope = serde_json::Map::from_iter([
                                (present_key.clone(), Value::Bool(value.is_some())),
                                (value_key.clone(), value.cloned().unwrap_or(Value::Null)),
                            ]);
                            output.insert(field.wire_name.clone(), Value::Object(envelope));
                        }
                    }
                }
                Ok(output)
            }
        }
    }
    /// Restore model-owned names and values. Validation/defaults/system binding
    /// are separate boundaries; this does not authorize or execute the Tool.
    pub fn decode_arguments(
        &self,
        raw: &str,
        limits: ProviderToolSchemaLimits,
    ) -> Result<JsonObject, ContractError> {
        check_limits(limits)?;
        let object = parse_provider_arguments(raw, limits.max_argument_bytes)?;
        match &self.data.decode_plan {
            ArgumentDecodePlan::Identity {} => Ok(object.clone()),
            ArgumentDecodePlan::Fields { fields } => {
                let mut result = JsonObject::new();
                for (name, value) in &object {
                    let mapping = fields
                        .iter()
                        .find(|field| &field.wire_name == name)
                        .ok_or_else(arguments)?;
                    let restored = match &mapping.encoding {
                        ArgumentValueEncoding::Identity {} => Some(value.clone()),
                        ArgumentValueEncoding::JsonText { optional } => {
                            let text = value.as_str().ok_or_else(arguments)?;
                            let parsed = parse_provider_value(text, limits.max_argument_bytes)?;
                            if *optional {
                                let values = parsed.as_array().ok_or_else(arguments)?;
                                match values.len() {
                                    0 => None,
                                    1 => Some(values[0].clone()),
                                    _ => return Err(arguments()),
                                }
                            } else {
                                Some(parsed)
                            }
                        }
                        ArgumentValueEncoding::Presence {
                            present_key,
                            value_key,
                        } => {
                            let envelope = value.as_object().ok_or_else(arguments)?;
                            match envelope.get(present_key).and_then(Value::as_bool) {
                                Some(false)
                                    if envelope.len() == 2
                                        && envelope.get(value_key) == Some(&Value::Null) =>
                                {
                                    None
                                }
                                Some(true) if envelope.len() == 2 => {
                                    Some(envelope.get(value_key).ok_or_else(arguments)?.clone())
                                }
                                _ => return Err(arguments()),
                            }
                        }
                    };
                    if let Some(value) = restored {
                        result.insert(mapping.canonical_name.clone(), value);
                    }
                }
                Ok(result)
            }
        }
    }
}
fn validate_codec(
    canonical: &Value,
    wire: &Value,
    plan: &ArgumentDecodePlan,
) -> Result<(), ContractError> {
    let canonical = canonical
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("provider_tool.canonical_properties"))?;
    let wire = wire
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("provider_tool.wire_properties"))?;
    match plan {
        ArgumentDecodePlan::Identity {} if canonical.keys().eq(wire.keys()) => Ok(()),
        ArgumentDecodePlan::Fields { fields } => {
            let mut from = BTreeSet::new();
            let mut to = BTreeSet::new();
            for field in fields {
                if !wire.contains_key(&field.wire_name)
                    || !canonical.contains_key(&field.canonical_name)
                    || !from.insert(&field.wire_name)
                    || !to.insert(&field.canonical_name)
                {
                    return Err(invalid("provider_tool.codec_mapping"));
                }
                if let ArgumentValueEncoding::Presence {
                    present_key,
                    value_key,
                } = &field.encoding
                {
                    if present_key.is_empty() || value_key.is_empty() || present_key == value_key {
                        return Err(invalid("provider_tool.presence_keys"));
                    }
                }
            }
            if from.len() != wire.len() || to.len() != canonical.len() {
                return Err(invalid("provider_tool.codec_coverage"));
            }
            Ok(())
        }
        _ => Err(invalid("provider_tool.codec_mapping")),
    }
}
fn collect_enforcement(
    canonical: &Value,
    wire: &Value,
    pointer: &str,
    identity: bool,
    text: bool,
    output: &mut Vec<ToolConstraintEnforcement>,
) {
    output.push(ToolConstraintEnforcement {
        canonical_pointer: pointer.into(),
        provider_native: identity && wire.pointer(pointer) == Some(canonical),
        context_text: text,
        core: true,
    });
    let Some(map) = canonical.as_object() else {
        return;
    };
    for (key, value) in map {
        if matches!(
            key.as_str(),
            "title"
                | "description"
                | "default"
                | "examples"
                | "$comment"
                | "$schema"
                | "$id"
                | "deprecated"
                | "readOnly"
                | "writeOnly"
        ) {
            continue;
        }
        let path = format!("{pointer}/{}", key.replace('~', "~0").replace('/', "~1"));
        match key.as_str() {
            "properties" | "$defs" | "definitions" | "dependentSchemas" | "patternProperties" => {
                if let Some(children) = value.as_object() {
                    for (name, child) in children {
                        collect_enforcement(
                            child,
                            wire,
                            &format!("{path}/{}", name.replace('~', "~0").replace('/', "~1")),
                            identity,
                            text,
                            output,
                        );
                    }
                }
            }
            "allOf" | "anyOf" | "oneOf" | "prefixItems" => {
                if let Some(children) = value.as_array() {
                    for (index, child) in children.iter().enumerate() {
                        collect_enforcement(
                            child,
                            wire,
                            &format!("{path}/{index}"),
                            identity,
                            text,
                            output,
                        );
                    }
                }
                output.push(ToolConstraintEnforcement {
                    canonical_pointer: path.clone(),
                    provider_native: identity && wire.pointer(&path) == Some(value),
                    context_text: text,
                    core: true,
                });
            }
            "items"
            | "additionalProperties"
            | "unevaluatedProperties"
            | "unevaluatedItems"
            | "contains"
            | "not"
            | "if"
            | "then"
            | "else"
            | "propertyNames" => collect_enforcement(value, wire, &path, identity, text, output),
            _ => output.push(ToolConstraintEnforcement {
                canonical_pointer: path.clone(),
                provider_native: identity && wire.pointer(&path) == Some(value),
                context_text: text,
                core: true,
            }),
        }
    }
}

fn bounded(value: &impl Serialize, max: usize) -> Result<(), ContractError> {
    if serde_json::to_vec(value)
        .map_err(|_| invalid("provider_tool.json"))?
        .len()
        > max
    {
        return Err(invalid("provider_tool.size"));
    }
    Ok(())
}
fn check_limits(limits: ProviderToolSchemaLimits) -> Result<(), ContractError> {
    if limits.max_schema_depth == 0
        || limits.max_schema_depth > 128
        || limits.max_schema_bytes == 0
        || limits.max_contract_bytes == 0
        || limits.max_argument_bytes == 0
    {
        return Err(invalid("provider_tool.limits"));
    }
    Ok(())
}
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn invalid(path: &str) -> ContractError {
    ContractError::new(ErrorCode::UnsupportedInputProjection, path)
}
fn arguments() -> ContractError {
    ContractError::new(ErrorCode::InvalidArguments, "provider_tool.arguments")
}

fn depth_bound(schema: &Value, max: usize) -> Result<(), ContractError> {
    let mut pending = vec![(schema, 0)];
    while let Some((value, depth)) = pending.pop() {
        if depth > max {
            return Err(invalid("provider_tool.depth"));
        }
        match value {
            Value::Object(map) => pending.extend(map.values().map(|value| (value, depth + 1))),
            Value::Array(array) => pending.extend(array.iter().map(|value| (value, depth + 1))),
            _ => {}
        }
    }
    Ok(())
}

// The legacy Value parser must remain unchanged for old digests. This new codec
// refuses values it cannot represent, rather than silently rounding model input.
fn numbers_preserved(raw: &serde_json::value::RawValue, parsed: &Value) -> bool {
    use serde_json::value::RawValue;
    match raw.get().as_bytes()[0] {
        b'{' => {
            let Ok(object) =
                serde_json::from_str::<std::collections::BTreeMap<String, &RawValue>>(raw.get())
            else {
                return false;
            };
            object.into_iter().all(|(key, raw)| {
                parsed
                    .get(&key)
                    .is_some_and(|value| numbers_preserved(raw, value))
            })
        }
        b'[' => {
            let Ok(array) = serde_json::from_str::<Vec<&RawValue>>(raw.get()) else {
                return false;
            };
            array.into_iter().enumerate().all(|(index, raw)| {
                parsed
                    .get(index)
                    .is_some_and(|value| numbers_preserved(raw, value))
            })
        }
        b'-' | b'0'..=b'9' => parsed.as_number().is_some_and(|number| {
            normalized_decimal(raw.get())
                .is_some_and(|original| Some(original) == normalized_decimal(&number.to_string()))
        }),
        _ => true,
    }
}
fn normalized_decimal(text: &str) -> Option<(bool, String, i128)> {
    let negative = text.starts_with('-');
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let (mantissa, exponent) = unsigned.split_once(['e', 'E']).unwrap_or((unsigned, "0"));
    let fraction = mantissa
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Some((false, "0".into(), 0));
    }
    let trimmed = digits.trim_end_matches('0');
    let exponent = exponent
        .parse::<i128>()
        .ok()?
        .checked_sub(fraction as i128)?
        .checked_add((digits.len() - trimmed.len()) as i128)?;
    Some((negative, trimmed.into(), exponent))
}

/// Parse model-owned provider arguments without silently rounding number tokens.
pub fn parse_provider_arguments(raw: &str, max_bytes: usize) -> Result<JsonObject, ContractError> {
    Ok(parse_provider_value(raw, max_bytes)?
        .as_object()
        .ok_or_else(arguments)?
        .clone()
        .into_iter()
        .collect())
}
fn parse_provider_value(raw: &str, max_bytes: usize) -> Result<Value, ContractError> {
    if raw.len() > max_bytes {
        return Err(arguments());
    }
    let value = parse_json(raw).map_err(|_| arguments())?;
    let original: &serde_json::value::RawValue =
        serde_json::from_str(raw).map_err(|_| arguments())?;
    if !numbers_preserved(original, &value) {
        return Err(ContractError::new(
            ErrorCode::InvalidArguments,
            "provider_tool.numeric_precision",
        ));
    }
    Ok(value)
}
```

## `crates/wickle/src/state/prepared_state.rs`

```rust
use super::*;
use crate::*;

fn read<T: DeserializeOwned>(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    reference: &RecordRef,
) -> Result<T, ContractError> {
    serde_json::from_value(record_value(state, additions, reference)?.clone())
        .map_err(|_| invalid("prepared.record"))
}
fn invalid(path: &str) -> ContractError {
    error(ErrorCode::InvalidSnapshot, path)
}

pub(super) fn validate(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    let mut step_ids = BTreeSet::new();
    for reference in &snapshot.model_step_inputs {
        let value = record_value(state, additions, reference)?;
        let input: RoutedModelInput = serde_json::from_value(value["input"].clone())
            .map_err(|_| invalid("prepared.step_input"))?;
        let session = state
            .sessions
            .get(&snapshot.request.session_id)
            .ok_or_else(not_found)?;
        if value["schema_version"] != "wickle.model-step.v2"
            || value["run_id"] != serde_json::json!(snapshot.run_id)
            || value["through_sequence"]
                .as_u64()
                .is_none_or(|sequence| sequence > session.snapshot.transcript_revision)
            || input.routing.scope != snapshot.scope
            || !step_ids.insert(input.model_step_id)
        {
            return Err(invalid("prepared.step_input"));
        }
    }
    // These caches live only for this validation call. Immutable contracts are
    // compiled once; every repeated serialized value is still checked exactly.
    let mut tool_cache = BTreeMap::new();
    let mut contract_cache: BTreeMap<(String, String), (CompiledToolContract, Value)> =
        BTreeMap::new();
    let routing = if snapshot.prepared_steps.is_empty() {
        None
    } else {
        let reference = snapshot
            .routing_snapshot_ref
            .as_ref()
            .ok_or_else(|| invalid("prepared.routing"))?;
        Some(RoutingSnapshot::restore(
            &record_value(state, additions, reference)?.to_string(),
            &snapshot.scope,
            &reference.digest,
        )?)
    };
    let mut prompt_cache = None;
    let mut revisions = BTreeMap::new();
    let mut roots = BTreeMap::new();
    let mut unique = BTreeSet::new();
    for reference in &snapshot.prepared_steps {
        if !unique.insert(record_key(reference)) {
            return Err(invalid("prepared.duplicate"));
        }
        let root: PreparedStepRecord = read(state, additions, reference)?;
        let expected = revisions.entry(root.model_step_id.clone()).or_insert(0u64);
        *expected = expected
            .checked_add(1)
            .ok_or_else(|| invalid("prepared.revision"))?;
        if root.scope != snapshot.scope
            || root.run_id != snapshot.run_id
            || root.profile_digest != *snapshot.profile.profile_digest()
            || root.assembly_ref != snapshot.assembly_ref
            || root.projection_revision.get() != *expected
            || root.assembler
                != (VersionedRef {
                    id: Id::new("wickle-prepared-model")?,
                    version: Id::new("1")?,
                })
        {
            return Err(invalid("prepared.identity"));
        }
        let projection: PreparedModelProjection = read(state, additions, &root.context_projection)?;
        let tools: ResolvedToolSet = read(state, additions, &root.tool_set)?;
        tools.validate_shape()?;
        let configuration_value = record_value(state, additions, &root.model_configuration)?;
        let route: ResolvedModelRoute =
            serde_json::from_value(configuration_value["route"].clone())
                .map_err(|_| invalid("prepared.route"))?;
        let configuration: ModelConfiguration =
            serde_json::from_value(configuration_value["configuration"].clone())
                .map_err(|_| invalid("prepared.configuration"))?;
        if projection.schema_version != "wickle.prepared-projection.v1"
            || projection.scope != snapshot.scope
            || projection.run_id != snapshot.run_id
            || projection.request.request_id != root.model_step_id
            || projection.request.purpose != root.purpose
            || projection.request.route != route
            || projection.request.options != configuration.effective
            || projection.request.max_output_tokens != configuration.max_output_tokens
            || projection.fingerprint() != root.projection_fingerprint
            || tools.scope != snapshot.scope
            || tools.run_id != snapshot.run_id
            || tools.entries.len() != projection.request.tools.len()
            || tools.entries.len() != root.compiled_tools.len()
        {
            return Err(invalid("prepared.projection"));
        }
        projection.request.validate()?;
        let routing = routing.as_ref().expect("nonempty preparation history");
        routing.validate_route(&route)?;
        if !snapshot.model_step_inputs.contains(&root.step_input) {
            return Err(invalid("prepared.step_reference"));
        }
        let step_value = record_value(state, additions, &root.step_input)?;
        let step: RoutedModelInput = serde_json::from_value(step_value["input"].clone())
            .map_err(|_| invalid("prepared.step"))?;
        if step_value["schema_version"] != "wickle.model-step.v2"
            || step_value["through_sequence"].as_u64()
                != Some(projection.provenance.through_sequence)
            || step_value["run_id"] != serde_json::json!(snapshot.run_id)
            || step.model_step_id != root.model_step_id
            || step.routing.scope != snapshot.scope
            || step.routing.purpose != root.purpose
        {
            return Err(invalid("prepared.step"));
        }
        let expected_configuration = routing.model_configuration(
            &route,
            &step.routing.options,
            &crate::model_options::requested_sources(snapshot, root.purpose, &step.routing.options),
            step.routing.max_output_tokens,
        )?;
        if configuration != expected_configuration {
            return Err(invalid("prepared.configuration"));
        }
        let session = state
            .sessions
            .get(&snapshot.request.session_id)
            .ok_or_else(not_found)?;
        if !tools.entries.is_empty() && prompt_cache.is_none() {
            prompt_cache = Some(PromptSnapshot::restore(
                &record_value(state, additions, &session.snapshot.prompt_snapshot)?.to_string(),
                &session.snapshot.prompt_snapshot.digest,
                &snapshot.profile,
                &snapshot.scope,
            )?);
        }
        let manifests = prompt_cache.as_ref().map_or(&[][..], PromptSnapshot::tools);
        if root.purpose != ModelPurpose::Agent && !tools.entries.is_empty() {
            return Err(invalid("prepared.auxiliary_tools"));
        }
        if projection.provenance.through_sequence > session.snapshot.transcript_revision {
            return Err(invalid("prepared.transcript_boundary"));
        }
        let messages: Vec<_> = session
            .messages
            .iter()
            .filter(|message| message.sequence.get() <= projection.provenance.through_sequence)
            .cloned()
            .collect();
        let expected_lineage =
            context_state::source_lineage(state, additions, snapshot, &messages)?;
        if (root.purpose == ModelPurpose::Agent
            && expected_lineage != projection.provenance.source_lineage)
            || expected_lineage
                .iter()
                .any(|dependency| !projection.provenance.source_lineage.contains(dependency))
        {
            return Err(invalid("prepared.lineage_missing"));
        }
        let mut extra = vec![];
        let mut current = snapshot.context_revision_ref.clone();
        let mut context_refs = Vec::new();
        while let Some(reference) = current {
            if context_refs.contains(&reference) {
                return Err(invalid("prepared.context_cycle"));
            }
            context_refs.push(reference.clone());
            let revision: ContextRevision = read(state, additions, &reference)?;
            if revision.scope != snapshot.scope
                || revision.session_id != snapshot.request.session_id
            {
                return Err(invalid("prepared.context_scope"));
            }
            extra.extend(crate::prepared_step::revision_artifacts(&revision));
            current = revision.parent;
        }
        if projection
            .provenance
            .context_revision_ref
            .as_ref()
            .is_some_and(|reference| !context_refs.contains(reference))
        {
            return Err(invalid("prepared.context_reference"));
        }
        let expected_artifacts = crate::prepared_step::projected_artifacts(
            &projection.request,
            &messages,
            &extra,
            &snapshot.scope,
        )?;
        if expected_artifacts
            .iter()
            .any(|reference| !projection.provenance.artifacts.contains(reference))
            || projection
                .provenance
                .artifacts
                .iter()
                .any(|reference| reference.scope != snapshot.scope)
        {
            return Err(invalid("prepared.artifacts_missing"));
        }
        let mut manifests = manifests.iter();
        let target = ProviderToolTarget::for_route(&route);
        let mut contracts = vec![];
        for ((entry, reference), wire) in tools
            .entries
            .iter()
            .zip(&root.compiled_tools)
            .zip(&projection.request.tools)
        {
            if !manifests.any(|manifest| manifest == &entry.manifest) {
                return Err(invalid("prepared.tool_manifest"));
            }
            let tool = entry.restore_cached(&mut tool_cache)?;
            let value = record_value(state, additions, reference)?;
            let digest: JsonDigest = serde_json::from_value(value["digest"].clone())
                .map_err(|_| invalid("prepared.contract_digest"))?;
            let cache_key = (
                digest.to_string(),
                entry.manifest.compiled_digest.to_string(),
            );
            let contract = if let Some((contract, expected)) = contract_cache.get(&cache_key) {
                if expected != value || contract.target() != &target {
                    return Err(invalid("prepared.contract_cache_identity"));
                }
                contract.clone()
            } else {
                let contract = CompiledToolContract::restore(
                    &value.to_string(),
                    &tool,
                    &target,
                    &digest,
                    ProviderToolSchemaLimits::default(),
                )?;
                contract_cache.insert(cache_key, (contract.clone(), value.clone()));
                contract
            };
            if contract.wire_tool() != wire {
                return Err(invalid("prepared.wire_tool"));
            }
            contracts.push(contract);
        }
        if root.purpose == ModelPurpose::Agent {
            let mut expected_sources = vec![];
            for binding in snapshot.profile.profile().context_sources.iter().flatten() {
                let mut matching = vec![];
                for reference in &snapshot.context_batches {
                    let value = record_value(state, additions, reference)?;
                    let request: ContextRequest = serde_json::from_value(value["request"].clone())
                        .map_err(|_| invalid("prepared.source_request"))?;
                    if request.binding == *binding
                        && (binding.trigger == ContextTrigger::RunStart
                            || request.model_step_id.as_ref() == Some(&root.model_step_id))
                    {
                        matching.push(reference.clone());
                    }
                }
                if matching.len() != 1 {
                    return Err(invalid("prepared.source_slot"));
                }
                expected_sources.push(matching.remove(0));
            }
            if expected_sources != projection.provenance.source_batches {
                return Err(invalid("prepared.source_selection"));
            }
        }
        let mut fragments = vec![];
        for reference in &projection.provenance.source_batches {
            if !snapshot.context_batches.contains(reference) {
                return Err(invalid("prepared.source_batch"));
            }
            if let Some(value) = record_value(state, additions, reference)?.get("fragments") {
                fragments.extend(
                    serde_json::from_value::<Vec<ContextFragment>>(value.clone())
                        .map_err(|_| invalid("prepared.fragments"))?,
                );
            }
        }
        if fragments != projection.provenance.fragments {
            return Err(invalid("prepared.fragments"));
        }
        for fragment in &fragments {
            fragment.validate()?;
        }
        for dependency in &projection.provenance.source_lineage {
            let original = if dependency.run_id == snapshot.run_id {
                snapshot
            } else {
                &state
                    .runs
                    .get(&dependency.run_id)
                    .ok_or_else(not_found)?
                    .snapshot
            };
            if original.request.session_id != snapshot.request.session_id
                || !original.context_batches.contains(&dependency.batch_ref)
            {
                return Err(invalid("prepared.lineage"));
            }
            record_value(state, additions, &dependency.batch_ref)?;
        }
        roots.insert(
            record_key(reference),
            (root, projection, configuration, tools, contracts),
        );
    }
    if let Some(reference) = &snapshot.active_prepared_step {
        let (root, ..) = roots
            .get(&record_key(reference))
            .ok_or_else(|| invalid("prepared.active"))?;
        if root.purpose != ModelPurpose::Agent
            || snapshot.model_step_id.as_ref() != Some(&root.model_step_id)
            || snapshot.prepared_steps.iter().rev().find(|reference| {
                roots
                    .get(&record_key(reference))
                    .is_some_and(|(root, ..)| root.purpose == ModelPurpose::Agent)
            }) != Some(reference)
        {
            return Err(invalid("prepared.active"));
        }
    }
    for invocation in &snapshot.model_ledger {
        let Some(reference) = &invocation.prepared_step_ref else {
            continue;
        };
        if !snapshot.prepared_steps.contains(reference) {
            return Err(invalid("prepared.invocation_reference"));
        }
        let (root, projection, configuration, _, _) = roots
            .get(&record_key(reference))
            .ok_or_else(|| invalid("prepared.invocation"))?;
        let mut physical = projection.request.clone();
        physical.request_id = invocation.attempt_id.clone();
        if invocation.model_step_id != root.model_step_id
            || invocation.purpose != root.purpose
            || invocation.route != projection.request.route
            || invocation.configuration.as_ref() != Some(configuration)
            || invocation.request_digest != physical.digest()
        {
            return Err(invalid("prepared.invocation"));
        }
    }
    for ledger in &snapshot.tool_ledger {
        let call = &ledger.call;
        let Some(invocation) = snapshot
            .model_ledger
            .iter()
            .find(|invocation| invocation.attempt_id == call.model_request_id)
        else {
            continue;
        };
        let Some(reference) = &invocation.prepared_step_ref else {
            continue;
        };
        let (root, _, _, tools, contracts) = roots
            .get(&record_key(reference))
            .ok_or_else(|| invalid("prepared.call"))?;
        let arguments = call
            .provider_arguments
            .as_ref()
            .ok_or_else(|| invalid("prepared.call_provenance"))?;
        let response: StoredModelResponse = read(
            state,
            additions,
            invocation
                .response_ref
                .as_ref()
                .ok_or_else(|| invalid("prepared.call_response"))?,
        )?;
        let ModelExchangeOutcome::Completed { response } = response.outcome else {
            return Err(invalid("prepared.call_response"));
        };
        if !response.tool_calls.iter().any(|proposed| {
            proposed.provider_call_id == call.provider_call_id
                && proposed.name == arguments.name
                && proposed.raw_arguments.as_ref() == Some(&arguments.raw)
        }) {
            return Err(invalid("prepared.call_response"));
        }
        if let Some((index, contract)) = contracts
            .iter()
            .enumerate()
            .find(|(_, contract)| contract.wire_tool().name == arguments.name)
        {
            let decoded = match contract
                .decode_arguments(&arguments.raw, ProviderToolSchemaLimits::default())
            {
                Ok(value) => value,
                Err(error) if error.code == ErrorCode::InvalidArguments => JsonObject::new(),
                Err(error) => return Err(error),
            };
            if call.tool_name != *contract.canonical_name()
                || call.model_inputs != decoded
                || call.descriptor_digest.as_ref()
                    != Some(&tools.entries[index].manifest.descriptor_digest)
                || arguments.compiled_contract_ref.as_ref() != Some(&root.compiled_tools[index])
            {
                return Err(invalid("prepared.call_contract"));
            }
        } else if call.descriptor_digest.is_some()
            || arguments.compiled_contract_ref.is_some()
            || call.tool_name != arguments.name
        {
            return Err(invalid("prepared.unadvertised_tool"));
        }
    }
    Ok(())
}
```

## `crates/wickle/tests/model_execution.rs`

```rust
//! Policy, persisted accounting, and recovery around a single-call model port.
use futures_util::{StreamExt, stream};
use serde_json::json;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use wickle::*;

#[allow(dead_code)]
mod support;
use support::{admission, id, scope};

fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn request(provider: &str) -> ModelRequest {
    ModelRequest {
        request_id: id("logical-step"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference(provider),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id("model"),
            model_id: id("model"),
            model_version: id("release"),
            version_semantics: VersionSemantics::Pinned,
            provider: id(provider),
            target: JsonObject::new(),
            deployment_revision: None,
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("v1"),
            },
            adapter: reference(&format!("{provider}-adapter")),
            capability_revision: id("capabilities"),
            connection_ref: reference(&format!("{provider}-connection")),
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find evidence".into(),
            }],
        }],
        tools: vec![],
        output: ModelOutput::Text {},
        max_output_tokens: 32.try_into().unwrap(),
        options: JsonObject::new(),
        limits: ModelResponseLimits {
            max_input_bytes: 8192,
            max_response_bytes: 4096,
            max_delta_bytes: 1024,
            max_events: 32,
            max_tool_calls: 0,
        },
    }
}

struct Policy {
    calls: AtomicUsize,
    deny_at: AtomicUsize,
    approval_at: AtomicUsize,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            deny_at: AtomicUsize::new(usize::MAX),
            approval_at: AtomicUsize::new(usize::MAX),
        }
    }
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        _: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        let count = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(async move {
            Ok(if count >= self.deny_at.load(Ordering::SeqCst) {
                PolicyDecision::Deny {
                    reason: id("revoked"),
                }
            } else if count >= self.approval_at.load(Ordering::SeqCst) {
                PolicyDecision::RequireApproval {
                    reason: id("review"),
                }
            } else {
                PolicyDecision::Allow {}
            })
        })
    }
}

enum Reply {
    Complete,
    Tool(String),
    Fail(ModelFailureKind),
    IncompleteTool,
    Pending,
}
struct ScriptedModel {
    store: Arc<MemoryStateStore>,
    binding: ModelPortBinding,
    replies: Mutex<VecDeque<Reply>>,
    observed: Mutex<Vec<(ModelRequest, Id, CancellationToken)>>,
    entered: Notify,
}
impl ModelPort for ScriptedModel {
    fn binding(&self) -> ModelPortBinding {
        self.binding.clone()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        self.observed.lock().unwrap().push((
            request.clone(),
            context.attempt_id.clone(),
            context.cancellation.clone(),
        ));
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("no hidden extra physical call");
        let start = stream::once(async move {
            let saved = self
                .store
                .load(&context.scope, &context.run_id)
                .await
                .unwrap();
            let attempt = saved
                .snapshot
                .model_ledger
                .iter()
                .find(|entry| entry.attempt_id == context.attempt_id)
                .unwrap();
            assert!(matches!(attempt.state, ModelAttemptState::Reserved {}));
            assert_eq!(attempt.request_digest, request.digest());
            assert_eq!(attempt.route, request.route);
            assert!(
                saved
                    .snapshot
                    .reservations
                    .iter()
                    .any(|entry| entry.attempt_id == context.attempt_id)
            );
            self.entered.notify_one();
            Ok(ModelEvent::TextDelta {
                text: "candidate".into(),
            })
        });
        let tail: PortStream<'a, ModelEvent> = match reply {
            Reply::Complete => Box::pin(stream::iter([Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::Stop,
                metadata: ModelResponseMetadata {
                    provider_request_id: Some(id("provider-response")),
                    usage: Some(ModelUsage {
                        measurement: UsageMeasurement::Reported,
                        input_tokens: Some(3),
                        output_tokens: Some(2),
                    }),
                    ..ModelResponseMetadata::default()
                },
                continuation: vec![],
            })])),
            Reply::Fail(kind) => Box::pin(stream::iter([Ok(ModelEvent::ResponseError {
                kind,
                metadata: ModelResponseMetadata::default(),
            })])),
            Reply::IncompleteTool => Box::pin(stream::iter([Ok(ModelEvent::ToolArgumentsDelta {
                index: 0,
                provider_call_id: Some("tool".into()),
                name: Some("search".into()),
                delta: "{\"query\":".into(),
            })])),
            Reply::Tool(arguments) => Box::pin(stream::iter([
                Ok(ModelEvent::ToolArgumentsDelta {
                    index: 0,
                    provider_call_id: Some("provider-tool".into()),
                    name: Some("wire_search".into()),
                    delta: arguments,
                }),
                Ok(ModelEvent::ResponseCompleted {
                    finish: ModelFinish::ToolCalls,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![],
                }),
            ])),
            Reply::Pending => Box::pin(stream::pending()),
        };
        Box::pin(start.chain(tail))
    }
}
struct Fixture {
    store: Arc<MemoryStateStore>,
    budget: RunBudget,
    lease: RunLease,
    context: ExecutionContext,
    policy: Arc<Policy>,
}
impl Fixture {
    async fn new(models: u64, recovery: u64) -> Self {
        Self::with_records(models, recovery, Arc::new(RandomIdSource), vec![]).await
    }
    async fn with_records(
        models: u64,
        recovery: u64,
        ids: Arc<dyn IdSource>,
        records: Vec<ProtectedRecord>,
    ) -> Self {
        let clock = Arc::new(SystemClock::new());
        let now = clock.now().unwrap().utc_ms;
        let store = Arc::new(MemoryStateStore::new());
        let mut input = admission("run", "request", "session", "Find evidence", "1").await;
        input.snapshot.limits.max_model_calls = models.try_into().unwrap();
        input.snapshot.limits.max_recovery_attempts = recovery;
        input.snapshot.timing =
            RunTiming::new(now, input.snapshot.limits.max_elapsed_ms.get()).unwrap();
        input.events[0].timestamp_ms = now;
        input.records.extend(records);
        store.admit(&scope(), input).await.unwrap();
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("worker"), now, 20_000)
            .await
            .unwrap();
        let context = ExecutionContext::new(
            ExecutionContextData {
                scope: scope(),
                principal_ref: id("caller"),
                capability_grant_ref: id("grant"),
                trace_context: None,
                system_inputs: Some(SystemInputs::new(JsonObject::from([(
                    "database_fk".into(),
                    json!("host-only-value"),
                )]))),
            },
            CancellationToken::new(),
        );
        let budget = RunBudget::attach(
            store.clone(),
            clock,
            ids,
            scope(),
            id("run"),
            lease.clone(),
            context.cancellation.clone(),
        )
        .await
        .unwrap();
        Self {
            store,
            budget,
            lease,
            context,
            policy: Arc::new(Policy::default()),
        }
    }
    fn model(&self, provider: &str, replies: Vec<Reply>) -> Arc<ScriptedModel> {
        let route = request(provider).route;
        Arc::new(ScriptedModel {
            store: self.store.clone(),
            binding: ModelPortBinding {
                provider: route.provider,
                adapter: route.adapter,
                connection_ref: route.connection_ref,
            },
            replies: Mutex::new(replies.into()),
            observed: Mutex::new(vec![]),
            entered: Notify::new(),
        })
    }
    fn exchange(&self, model: Arc<dyn ModelPort>, retries: u32) -> ModelExchange {
        ModelExchange::new(
            model,
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap()),
        )
        .with_retry_policy(ModelRetryPolicy {
            max_retries: retries,
            backoff_ms: 0,
        })
    }
    async fn saved(&self) -> RunSnapshot {
        self.store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
    }
}

#[tokio::test]
async fn each_retry_has_its_own_saved_attempt_and_rechecks_policy() {
    let fixture = Fixture::new(3, 1).await;
    let model = fixture.model(
        "first",
        vec![Reply::Fail(ModelFailureKind::RateLimited), Reply::Complete],
    );
    let exchange = fixture.exchange(model.clone(), 2);
    let mut original = request("first");
    original.options = JsonObject::from([("reasoning_effort".into(), json!("high"))]);
    let result = exchange
        .generate(&original, &fixture.context, &fixture.budget)
        .await
        .unwrap();
    let Guarded::Completed(ModelExchangeOutcome::Completed { response }) = result else {
        panic!("expected completed response")
    };
    let saved = fixture.saved().await;
    assert_eq!(
        (saved.usage.model_calls, saved.usage.recovery_attempts),
        (2, 1)
    );
    assert_eq!(saved.model_ledger.len(), 2);
    assert_eq!(
        saved.model_ledger[0].state,
        ModelAttemptState::Failed {
            kind: ModelFailureKind::RateLimited
        }
    );
    assert_eq!(saved.model_ledger[1].state, ModelAttemptState::Completed {});
    assert_eq!(saved.model_ledger[1].reported_model_id, None);
    assert_eq!(saved.model_ledger[1].reported_model_version, None);
    assert_eq!(
        saved.model_ledger[1].usage.as_ref().unwrap().output_tokens,
        Some(2)
    );
    assert_eq!(response.request_id, saved.model_ledger[1].attempt_id);
    {
        let observed = model.observed.lock().unwrap();
        assert_eq!(observed.len(), 2);
        assert_ne!(observed[0].1, observed[1].1);
        for (request, attempt, _) in observed.iter() {
            assert_eq!(&request.request_id, attempt);
            assert_eq!(request.route, original.route);
            assert_eq!(request.messages, original.messages);
            assert_eq!(request.options, original.options);
            // A real Host value exists but no model request surface automatically copies it.
            assert!(
                !serde_json::to_string(request)
                    .unwrap()
                    .contains("host-only-value")
            );
        }
    }
    assert!(
        saved
            .model_ledger
            .iter()
            .all(|entry| entry.model_step_id == original.request_id)
    );
    assert_eq!(fixture.policy.calls.load(Ordering::SeqCst), 4);
    let events = fixture
        .store
        .read_events(&scope(), &id("run"), 0, 10)
        .await
        .unwrap();
    assert_eq!(events.events.len(), 3);
}

#[tokio::test]
async fn exhausted_recovery_and_model_budgets_each_stop_new_requests() {
    for (models, recovery) in [(8, 1), (1, 1)] {
        let fixture = Fixture::new(models, recovery).await;
        let model = fixture.model(
            "first",
            vec![
                Reply::Fail(ModelFailureKind::Transport),
                Reply::Fail(ModelFailureKind::Transport),
            ],
        );
        let exchange = fixture.exchange(model.clone(), 100);
        assert_eq!(
            exchange
                .generate(&request("first"), &fixture.context, &fixture.budget)
                .await
                .unwrap_err()
                .code,
            ErrorCode::BudgetExceeded
        );
        let expected = if models == 1 { 1 } else { 2 };
        assert_eq!(model.observed.lock().unwrap().len(), expected);
        assert_eq!(fixture.saved().await.usage.model_calls, expected as u64);
    }
}

#[tokio::test]
async fn authentication_capability_and_unchanged_context_overflow_are_not_retried() {
    for kind in [
        ModelFailureKind::Authentication,
        ModelFailureKind::Unsupported,
        ModelFailureKind::ContextOverflow,
    ] {
        let fixture = Fixture::new(4, 1).await;
        let model = fixture.model("first", vec![Reply::Fail(kind)]);
        let exchange = fixture.exchange(model.clone(), 3);
        let Guarded::Completed(ModelExchangeOutcome::Failed { failure }) = exchange
            .generate(&request("first"), &fixture.context, &fixture.budget)
            .await
            .unwrap()
        else {
            panic!("expected classified failure")
        };
        assert_eq!(failure.kind, kind);
        assert_eq!(model.observed.lock().unwrap().len(), 1);
        assert_eq!(fixture.saved().await.usage.recovery_attempts, 0);
    }
}

#[tokio::test]
async fn default_retry_is_disabled_and_partial_tools_never_become_a_complete_plan() {
    let fixture = Fixture::new(4, 1).await;
    let model = fixture.model("first", vec![Reply::IncompleteTool]);
    let exchange = fixture.exchange(model.clone(), 0);
    let mut request = request("first");
    request.limits.max_tool_calls = 1;
    let Guarded::Completed(ModelExchangeOutcome::Failed { failure }) = exchange
        .generate(&request, &fixture.context, &fixture.budget)
        .await
        .unwrap()
    else {
        panic!("expected incomplete response rejection")
    };
    assert_eq!(failure.kind, ModelFailureKind::Protocol);
    assert_eq!(failure.partial_text(), "candidate");
    let saved = fixture.saved().await;
    assert!(saved.tool_ledger.is_empty());
    assert_eq!(saved.usage.tool_attempts, 0);
    assert_eq!(saved.usage.recovery_attempts, 0);
    assert_eq!(model.observed.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn initial_denial_and_revocation_after_reservation_both_prevent_adapter_entry() {
    for deny_at in [1, 2, 3] {
        let fixture = Fixture::new(4, 1).await;
        fixture.policy.deny_at.store(deny_at, Ordering::SeqCst);
        let model = fixture.model("first", vec![Reply::Fail(ModelFailureKind::RateLimited)]);
        let exchange = fixture.exchange(model.clone(), 1);
        assert_eq!(
            exchange
                .generate(&request("first"), &fixture.context, &fixture.budget)
                .await
                .unwrap_err()
                .code,
            ErrorCode::AccessDenied
        );
        assert_eq!(
            model.observed.lock().unwrap().len(),
            usize::from(deny_at == 3)
        );
        assert_eq!(
            fixture.saved().await.usage.model_calls,
            u64::from(deny_at >= 2)
        );
    }
}

#[tokio::test]
async fn approval_does_not_dispatch_and_unreserved_approval_consumes_no_budget() {
    for approval_at in [1, 2] {
        let fixture = Fixture::new(4, 1).await;
        fixture
            .policy
            .approval_at
            .store(approval_at, Ordering::SeqCst);
        let model = fixture.model("first", vec![]);
        let exchange = fixture.exchange(model.clone(), 0);
        assert!(matches!(
            exchange
                .generate(&request("first"), &fixture.context, &fixture.budget)
                .await
                .unwrap(),
            Guarded::ApprovalRequired(_)
        ));
        assert!(model.observed.lock().unwrap().is_empty());
        assert_eq!(
            fixture.saved().await.usage.model_calls,
            u64::from(approval_at == 2)
        );
    }
}

#[tokio::test]
async fn wrong_connection_scope_or_opaque_route_is_rejected_before_reservation() {
    let fixture = Fixture::new(4, 1).await;
    let model = fixture.model("second", vec![]);
    let exchange = fixture.exchange(model.clone(), 0);
    assert!(
        exchange
            .generate(&request("first"), &fixture.context, &fixture.budget)
            .await
            .is_err()
    );
    let mut foreign_context = fixture.context.clone();
    foreign_context.data.scope.workspace_id = id("foreign");
    assert_eq!(
        exchange
            .generate(&request("second"), &foreign_context, &fixture.budget)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    let mut changed = request("second");
    changed.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![ModelContent::Opaque {
            continuation: OpaqueContinuation::new(
                &request("first").route,
                json!({"signature":"first-private"}),
            ),
        }],
    });
    assert!(
        exchange
            .generate(&changed, &fixture.context, &fixture.budget)
            .await
            .is_err()
    );
    assert!(model.observed.lock().unwrap().is_empty());
    assert_eq!(fixture.saved().await.usage.model_calls, 0);
}

#[tokio::test]
async fn cancellation_drops_the_adapter_signal_and_retains_an_unknown_charged_attempt() {
    let fixture = Fixture::new(4, 1).await;
    let model = fixture.model("first", vec![Reply::Pending]);
    let exchange = fixture.exchange(model.clone(), 2);
    let request = request("first");
    let execution = exchange.generate(&request, &fixture.context, &fixture.budget);
    let cancel = async {
        model.entered.notified().await;
        fixture.context.cancellation.cancel();
    };
    let (result, ()) = tokio::join!(execution, cancel);
    assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
    let saved = fixture.saved().await;
    assert_eq!(saved.usage.model_calls, 1);
    assert_eq!(saved.usage.recovery_attempts, 0);
    assert_eq!(saved.model_ledger[0].state, ModelAttemptState::Unknown {});
    assert!(model.observed.lock().unwrap()[0].2.is_cancelled());
}

#[tokio::test]
async fn exhausted_recovery_retains_the_last_partial_failure_in_protected_storage() {
    let fixture = Fixture::new(4, 0).await;
    let model = fixture.model("first", vec![Reply::Fail(ModelFailureKind::Transport)]);
    let exchange = fixture.exchange(model.clone(), 1);
    assert_eq!(
        exchange
            .generate(&request("first"), &fixture.context, &fixture.budget)
            .await
            .unwrap_err()
            .code,
        ErrorCode::BudgetExceeded
    );
    let saved = fixture.saved().await;
    let record = fixture
        .store
        .read_record(
            &scope(),
            saved.model_ledger[0].response_ref.as_ref().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(record.value()["outcome"]["result"], "failed");
    assert_eq!(record.value()["outcome"]["failure"]["kind"], "transport");
    assert_eq!(
        record.value()["outcome"]["failure"]["partial_text"],
        "candidate"
    );
    assert_eq!(model.observed.lock().unwrap().len(), 1);
    let mut foreign = scope();
    foreign.workspace_id = id("other");
    assert!(
        fixture
            .store
            .read_record(&foreign, record.reference())
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn caller_cancellation_interrupts_backoff_even_with_an_independent_budget_token() {
    let mut fixture = Fixture::new(4, 1).await;
    fixture.context.cancellation = CancellationToken::new();
    let model = fixture.model("first", vec![Reply::Fail(ModelFailureKind::Transport)]);
    let exchange = ModelExchange::new(
        model.clone(),
        Arc::new(PolicyGate::new(fixture.policy.clone(), Duration::from_secs(1)).unwrap()),
    )
    .with_retry_policy(ModelRetryPolicy {
        max_retries: 1,
        backoff_ms: 1000,
    });
    let request = request("first");
    let cancel = async {
        loop {
            if fixture.saved().await.usage.recovery_attempts == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        fixture.context.cancellation.cancel();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_millis(5), async {
        tokio::join!(
            exchange.generate(&request, &fixture.context, &fixture.budget),
            cancel
        )
    })
    .await
    .expect("caller cancellation must not wait for the one-second backoff");
    assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
    assert_eq!(model.observed.lock().unwrap().len(), 1);
}

struct PendingPolicy {
    entered: Notify,
}
impl PolicyPort for PendingPolicy {
    fn authorize<'a>(
        &'a self,
        _: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test(start_paused = true)]
async fn budget_cancellation_interrupts_policy_even_with_an_independent_caller_token() {
    let mut fixture = Fixture::new(4, 1).await;
    let budget_cancellation = fixture.context.cancellation.clone();
    fixture.context.cancellation = CancellationToken::new();
    let policy = Arc::new(PendingPolicy {
        entered: Notify::new(),
    });
    let model = fixture.model("first", vec![]);
    let exchange = ModelExchange::new(
        model.clone(),
        Arc::new(PolicyGate::new(policy.clone(), Duration::from_secs(1)).unwrap()),
    );
    let request = request("first");
    let cancel = async {
        policy.entered.notified().await;
        budget_cancellation.cancel();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_millis(5), async {
        tokio::join!(
            exchange.generate(&request, &fixture.context, &fixture.budget),
            cancel
        )
    })
    .await
    .expect("budget cancellation must not wait for the policy timeout");
    assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
    assert!(model.observed.lock().unwrap().is_empty());
    assert_eq!(fixture.saved().await.usage.model_calls, 0);
}

struct FixedAttempt;
impl IdSource for FixedAttempt {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id("fixed-attempt"))
    }
}

#[tokio::test]
async fn failed_invocation_or_response_persistence_never_causes_an_untracked_retry() {
    for (record_id, expected_calls) in [
        ("model-invocation-fixed-attempt", 0),
        ("model-response-fixed-attempt", 1),
    ] {
        let existing = ProtectedRecord::new(id(record_id), 1, json!({"original":"immutable"}));
        let fixture =
            Fixture::with_records(4, 1, Arc::new(FixedAttempt), vec![existing.clone()]).await;
        let model = fixture.model("first", vec![Reply::Complete]);
        let exchange = fixture.exchange(model.clone(), 5);
        assert_eq!(
            exchange
                .generate(&request("first"), &fixture.context, &fixture.budget)
                .await
                .unwrap_err()
                .code,
            ErrorCode::RecordConflict
        );
        assert_eq!(model.observed.lock().unwrap().len(), expected_calls);
        let saved = fixture.saved().await;
        assert_eq!(saved.usage.model_calls, expected_calls as u64);
        assert_eq!(saved.reservations.len(), expected_calls);
        assert_eq!(saved.usage.recovery_attempts, 0);
        assert_eq!(saved.model_ledger.len(), expected_calls);
        if expected_calls == 1 {
            assert_eq!(saved.model_ledger[0].state, ModelAttemptState::Reserved {});
            assert_eq!(saved.model_ledger[0].response_ref, None);
        }
        assert_eq!(
            fixture
                .store
                .read_record(&scope(), existing.reference())
                .await
                .unwrap()
                .value(),
            existing.value()
        );
    }
}

#[tokio::test]
async fn a_completed_ledger_entry_requires_the_exact_typed_response_and_reservation() {
    let fixture = Fixture::new(4, 1).await;
    fixture.policy.approval_at.store(2, Ordering::SeqCst);
    let model = fixture.model("first", vec![]);
    let exchange = fixture.exchange(model, 0);
    exchange
        .generate(&request("first"), &fixture.context, &fixture.budget)
        .await
        .unwrap();
    let before = fixture.saved().await;
    let entry = &before.model_ledger[0];
    let metadata = ModelResponseMetadata::default();
    let body = StoredModelResponse {
        request_id: entry.attempt_id.clone(),
        route_digest: entry.route.digest(),
        outcome: ModelExchangeOutcome::Completed {
            response: ModelResponse {
                request_id: entry.attempt_id.clone(),
                route_digest: entry.route.digest(),
                text: "Completed candidate".into(),
                tool_calls: vec![],
                finish: ModelFinish::Stop,
                metadata,
                continuation: vec![],
            },
        },
    };
    let valid = serde_json::to_value(&body).unwrap();
    let mut wrong_attempt = valid.clone();
    wrong_attempt["request_id"] = json!("another-attempt");
    let mut wrong_route = valid.clone();
    wrong_route["route_digest"] = serde_json::to_value(request("second").route.digest()).unwrap();
    let mut wrong_metadata = valid.clone();
    wrong_metadata["outcome"]["response"]["metadata"]["reported_model_id"] =
        json!("unreported-model");
    let mut wrong_finish = valid.clone();
    wrong_finish["outcome"]["response"]["finish"] = json!("tool_calls");
    for (index, value) in [
        json!({"unrelated":"record"}),
        wrong_attempt,
        wrong_route,
        wrong_metadata,
        wrong_finish,
    ]
    .into_iter()
    .enumerate()
    {
        let record = ProtectedRecord::new(id(&format!("invalid-{index}")), 1, value);
        let mut commit = support::prepared(
            &before,
            fixture.lease.clone(),
            before.timing.last_observed_at_ms,
        );
        commit.snapshot.model_ledger[0].state = ModelAttemptState::Completed {};
        commit.snapshot.model_ledger[0].response_ref = Some(record.reference().clone());
        commit.records.push(record);
        assert_eq!(
            fixture
                .store
                .commit(&scope(), &id("run"), commit)
                .await
                .unwrap_err()
                .code,
            ErrorCode::InvalidSnapshot
        );
        assert_eq!(fixture.saved().await, before);
    }
    let mut missing = before.clone();
    missing.model_ledger[0].state = ModelAttemptState::Completed {};
    assert!(missing.validate().is_err());
    let mut unreserved = before.clone();
    unreserved.model_ledger[0].attempt_id = id("unreserved");
    assert!(unreserved.validate().is_err());
    let record = ProtectedRecord::new(id("valid-response"), 1, valid);
    let mut commit = support::prepared(
        &before,
        fixture.lease.clone(),
        before.timing.last_observed_at_ms,
    );
    commit.snapshot.model_ledger[0].state = ModelAttemptState::Completed {};
    commit.snapshot.model_ledger[0].response_ref = Some(record.reference().clone());
    commit.records.push(record.clone());
    let saved = fixture
        .store
        .commit(&scope(), &id("run"), commit)
        .await
        .unwrap();
    assert_eq!(
        saved.snapshot.model_ledger[0].response_ref.as_ref(),
        Some(record.reference())
    );
}

struct RenamedArguments;
impl ProviderToolSchemaCompiler for RenamedArguments {
    fn reference(&self) -> VersionedRef {
        reference("renamed")
    }
    fn compile(
        &self,
        tool: &ModelTool,
        _: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        Ok(ProviderToolProjection {
            wire_tool: ModelTool {
                name: id("wire_search"),
                description: tool.description.clone(),
                model_input_schema: json!({"type":"object","properties":{"q":{"type":"string"},"n":{"type":"object","properties":{"present":{"type":"boolean"},"value":{"type":["integer","null"]}},"required":["present","value"],"additionalProperties":false}},"required":["q","n"],"additionalProperties":false}),
            },
            decode_plan: ArgumentDecodePlan::Fields {
                fields: vec![
                    ArgumentFieldMapping {
                        wire_name: "q".into(),
                        canonical_name: "query".into(),
                        encoding: ArgumentValueEncoding::Identity {},
                    },
                    ArgumentFieldMapping {
                        wire_name: "n".into(),
                        canonical_name: "limit".into(),
                        encoding: ArgumentValueEncoding::Presence {
                            present_key: "present".into(),
                            value_key: "value".into(),
                        },
                    },
                ],
            },
        })
    }
}
#[tokio::test]
async fn saved_provider_codec_restores_canonical_arguments_before_required_defaults() {
    let registry = SystemInputRegistry::new(vec![]).unwrap();
    let canonical = SchemaCompiler::new().compile(ToolDescriptor::from_json(r#"{
        "tool":{"id":"search","version":"1"},"name":"search","description":"Search",
        "input_schema":{"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","default":10}},"required":["query","limit"],"additionalProperties":false},
        "agent_parameters":["query","limit"],"output_schema":{"type":"string"},"max_output_bytes":100
    }"#).unwrap(),&registry).unwrap();
    let mut request = request("first");
    let target = ProviderToolTarget::for_route(&request.route);
    let contract = CompiledToolContract::compile(
        &canonical,
        target,
        &RenamedArguments,
        ProviderToolSchemaLimits::default(),
    )
    .unwrap();
    let record = ProtectedRecord::new(
        id("compiled-provider-tool"),
        1,
        serde_json::to_value(&contract).unwrap(),
    );
    let contract_ref = record.reference().clone();
    let fixture = Fixture::with_records(1, 0, Arc::new(RandomIdSource), vec![record]).await;
    request.tools = vec![contract.wire_tool().clone()];
    request.limits.max_tool_calls = 1;
    let raw = r#"{ "q": "evidence", "n": {"present":false,"value":null} }"#;
    let model = fixture.model("first", vec![Reply::Tool(raw.into())]);
    let response = fixture
        .exchange(model, 0)
        .generate(&request, &fixture.context, &fixture.budget)
        .await
        .unwrap();
    let Guarded::Completed(ModelExchangeOutcome::Completed { response }) = response else {
        panic!("expected complete Tool proposal")
    };
    let proposal = &response.tool_calls[0];
    assert_eq!(proposal.raw_arguments.as_deref(), Some(raw));
    let mut call = ToolCall {
        provider_arguments: Some(ProviderToolArguments {
            name: proposal.name.clone(),
            raw: raw.into(),
            compiled_contract_ref: Some(contract_ref),
        }),
        call_id: id("call"),
        model_request_id: response.request_id,
        provider_call_id: proposal.provider_call_id.clone(),
        tool_name: id("search"),
        model_inputs: contract
            .decode_arguments(raw, ProviderToolSchemaLimits::default())
            .unwrap(),
        descriptor_digest: Some(canonical.descriptor_digest().clone()),
        bound_input_ref: None,
    };
    let binder = InputBinder::new(
        Arc::new(registry),
        None,
        Arc::new(PolicyGate::new(fixture.policy.clone(), Duration::from_secs(1)).unwrap()),
        Arc::new(RandomIdSource),
    );
    let restored = binder
        .prepare_model_inputs(&canonical, &call, &fixture.context, &fixture.budget)
        .await
        .unwrap();
    assert_eq!(
        restored,
        JsonObject::from([
            ("query".into(), json!("evidence")),
            ("limit".into(), json!(10))
        ])
    );
    assert!(!call.model_inputs.contains_key("limit"));
    call.model_inputs.insert("query".into(), json!("changed"));
    assert_eq!(
        binder
            .prepare_model_inputs(&canonical, &call, &fixture.context, &fixture.budget)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidArguments
    );
}
```

## `crates/wickle/tests/provider_tool_schema.rs`

```rust
//! Provider projection retains original constraints and reversible presence semantics.
use serde_json::{Value, json};
use wickle::*;
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn reference(s: &str) -> VersionedRef {
    VersionedRef {
        id: id(s),
        version: id("1"),
    }
}
fn target() -> ProviderToolTarget {
    ProviderToolTarget {
        model: None,
        provider: id("example"),
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("1"),
        },
        capability_revision: id("caps-1"),
    }
}
fn tool() -> CompiledTool {
    let descriptor = ToolDescriptor::from_json(r##"{
      "tool":{"id":"search","version":"1"},"name":"search","description":"Search",
      "input_schema":{"type":"object","properties":{"query":{"$ref":"#/$defs/Query"},"note":{"type":["string","null"]},"workspace_id":{"type":"string","const":"private-workspace"}},"required":["query","workspace_id"],"additionalProperties":false,
      "$defs":{"Query":{"type":"string","minLength":2},"Private":{"const":"hidden-secret"}}},
      "agent_parameters":["query","note"],"output_schema":{"type":"string"},"max_output_bytes":100
    }"##).unwrap();
    let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    SchemaCompiler::new()
        .compile(descriptor, &registry)
        .unwrap()
}
struct Restricted;
impl ProviderToolSchemaCompiler for Restricted {
    fn reference(&self) -> VersionedRef {
        reference("restricted")
    }
    fn compile(
        &self,
        tool: &ModelTool,
        _: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        // The compiler receives only the model contract, never the full input schema.
        assert!(
            tool.model_input_schema["properties"]
                .get("workspace_id")
                .is_none()
        );
        assert!(tool.model_input_schema["$defs"].get("Private").is_none());
        Ok(ProviderToolProjection {
            wire_tool: ModelTool {
                name: id("wire_search"),
                description: tool.description.clone(),
                model_input_schema: json!({"type":"object","properties":{"q":{"type":"string"},"n":{"type":"object","properties":{"present":{"type":"boolean"},"value":{"type":["string","null"]}},"required":["present","value"],"additionalProperties":false}},"required":["q","n"],"additionalProperties":false}),
            },
            decode_plan: ArgumentDecodePlan::Fields {
                fields: vec![
                    ArgumentFieldMapping {
                        wire_name: "q".into(),
                        canonical_name: "query".into(),
                        encoding: ArgumentValueEncoding::Identity {},
                    },
                    ArgumentFieldMapping {
                        wire_name: "n".into(),
                        canonical_name: "note".into(),
                        encoding: ArgumentValueEncoding::Presence {
                            present_key: "present".into(),
                            value_key: "value".into(),
                        },
                    },
                ],
            },
        })
    }
}
#[test]
fn relaxed_schema_keeps_constraints_and_distinguishes_omission_null_and_values() {
    let tool = tool();
    let limits = ProviderToolSchemaLimits::default();
    let compiled = CompiledToolContract::compile(&tool, target(), &Restricted, limits).unwrap();
    assert_eq!(compiled.canonical_name(), &id("search"));
    assert_eq!(compiled.wire_tool().name, id("wire_search"));
    let omitted = compiled
        .decode_arguments(r#"{"q":"ok","n":{"present":false,"value":null}}"#, limits)
        .unwrap();
    assert!(!omitted.contains_key("note"));
    let null = compiled
        .decode_arguments(r#"{"q":"ok","n":{"present":true,"value":null}}"#, limits)
        .unwrap();
    assert_eq!(null["note"], Value::Null);
    tool.validate_model_inputs(&null).unwrap();
    let invalid = compiled
        .decode_arguments(r#"{"q":"x","n":{"present":false,"value":null}}"#, limits)
        .unwrap();
    assert!(tool.validate_model_inputs(&invalid).is_err());
    for raw in [
        r#"{"q":"ok","n":{"present":false,"value":"unexpected"}}"#,
        r#"{"q":"ok","n":{"present":false}}"#,
        r#"{"q":"ok","n":{"present":true}}"#,
        r#"{"q":"ok","n":null}"#,
        r#"{"q":"ok","workspace_id":"forged"}"#,
        r#"{"q":"a","q":"b"}"#,
    ] {
        assert_eq!(
            compiled.decode_arguments(raw, limits).unwrap_err().code,
            ErrorCode::InvalidArguments
        );
    }
    assert!(
        compiled
            .enforcement()
            .iter()
            .all(|item| item.core && item.context_text)
    );
    let fragment = &compiled.constraint_fragments()[0];
    // Confidentiality check of all model-visible surfaces, not prompt-quality testing.
    let visible = format!(
        "{}{}",
        serde_json::to_string(compiled.wire_tool()).unwrap(),
        fragment.text
    );
    for secret in ["workspace_id", "private-workspace", "hidden-secret"] {
        assert!(!visible.contains(secret));
    }
    assert_eq!(
        fragment.digest,
        versioned_digest_json(
            &serde_json::to_string(&fragment.text).unwrap(),
            CanonicalizationVersion::SortedJsonV1,
            JsonTextLimits::default()
        )
        .unwrap()
    );
}
#[test]
fn native_projection_restores_exactly_and_rejects_changed_destination_or_projection() {
    let tool = tool();
    let limits = ProviderToolSchemaLimits::default();
    let compiled =
        CompiledToolContract::compile(&tool, target(), &NativeToolSchemaCompiler, limits).unwrap();
    assert_eq!(compiled.wire_tool(), &tool.to_model_tool());
    assert!(compiled.constraint_fragments().is_empty());
    let text = serde_json::to_string(&compiled).unwrap();
    CompiledToolContract::restore(&text, &tool, &target(), compiled.digest(), limits).unwrap();
    let mut changed = target();
    changed.capability_revision = id("caps-2");
    assert!(
        CompiledToolContract::restore(&text, &tool, &changed, compiled.digest(), limits).is_err()
    );
    let mut record: Value = serde_json::from_str(&text).unwrap();
    record["data"]["wire_tool"]["description"] = json!("modified");
    assert!(
        CompiledToolContract::restore(
            &record.to_string(),
            &tool,
            &target(),
            compiled.digest(),
            limits
        )
        .is_err()
    );
    assert!(
        CompiledToolContract::compile(
            &tool,
            target(),
            &Restricted,
            ProviderToolSchemaLimits {
                max_contract_bytes: 128,
                ..limits
            }
        )
        .is_err()
    );
    assert!(
        CompiledToolContract::compile(
            &tool,
            target(),
            &Restricted,
            ProviderToolSchemaLimits {
                max_schema_depth: 1,
                ..limits
            }
        )
        .is_err()
    );
}

struct Broken(usize);
impl ProviderToolSchemaCompiler for Broken {
    fn reference(&self) -> VersionedRef {
        reference("broken")
    }
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        let mut projected = Restricted.compile(tool, target)?;
        let ArgumentDecodePlan::Fields { fields } = &mut projected.decode_plan else {
            unreachable!()
        };
        match self.0 {
            0 => fields[0].canonical_name = "workspace_id".into(),
            1 => fields[1].wire_name = fields[0].wire_name.clone(),
            2 => {
                fields.pop();
            }
            3 => {
                fields[1].encoding = ArgumentValueEncoding::Presence {
                    present_key: "value".into(),
                    value_key: "value".into(),
                }
            }
            4 => projected.wire_tool.model_input_schema["additionalProperties"] = json!(true),
            _ => projected.wire_tool.name = id("invalid.name"),
        }
        Ok(projected)
    }
}
#[test]
fn compiler_output_cannot_change_ownership_drop_fields_or_ambiguate_presence() {
    for case in 0..6 {
        assert!(
            CompiledToolContract::compile(
                &tool(),
                target(),
                &Broken(case),
                ProviderToolSchemaLimits::default()
            )
            .is_err()
        );
    }
}

#[test]
fn codecs_never_silently_round_numeric_arguments() {
    let mut descriptor = tool().descriptor().clone();
    descriptor.input_schema["properties"]["note"] = json!({"type":"number"});
    let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    let tool = SchemaCompiler::new()
        .compile(descriptor, &registry)
        .unwrap();
    let limits = ProviderToolSchemaLimits::default();
    let compiled =
        CompiledToolContract::compile(&tool, target(), &NativeToolSchemaCompiler, limits).unwrap();
    for number in [
        "18446744073709551617",
        "0.12345678901234567890123456789",
        "1e-999",
    ] {
        assert!(
            compiled
                .decode_arguments(&format!(r#"{{"query":"ok","note":{number}}}"#), limits)
                .is_err(),
            "changed numeric value: {number}"
        );
    }
    for number in ["18446744073709551615", "0.1", "1e2", "100.00", "-0.0"] {
        let decoded = compiled
            .decode_arguments(&format!(r#"{{"query":"ok","note":{number}}}"#), limits)
            .unwrap();
        tool.validate_model_inputs(&decoded).unwrap();
    }
}

struct JsonTextCompiler;
impl ProviderToolSchemaCompiler for JsonTextCompiler {
    fn reference(&self) -> VersionedRef {
        reference("json-text")
    }
    fn compile(
        &self,
        tool: &ModelTool,
        _: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        Ok(ProviderToolProjection {
            wire_tool: ModelTool {
                name: tool.name.clone(),
                description: tool.description.clone(),
                model_input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"note":{"type":"string"}},"required":["query","note"],"additionalProperties":false}),
            },
            decode_plan: ArgumentDecodePlan::Fields {
                fields: vec![
                    ArgumentFieldMapping {
                        wire_name: "query".into(),
                        canonical_name: "query".into(),
                        encoding: ArgumentValueEncoding::JsonText { optional: false },
                    },
                    ArgumentFieldMapping {
                        wire_name: "note".into(),
                        canonical_name: "note".into(),
                        encoding: ArgumentValueEncoding::JsonText { optional: true },
                    },
                ],
            },
        })
    }
}
#[test]
fn json_text_values_preserve_omission_null_and_numeric_precision() {
    let tool = tool();
    let limits = ProviderToolSchemaLimits::default();
    let contract =
        CompiledToolContract::compile(&tool, target(), &JsonTextCompiler, limits).unwrap();
    let missing = JsonObject::from([("query".into(), json!("facts"))]);
    let encoded = contract.encode_arguments(&missing).unwrap();
    assert_eq!(encoded["note"], json!("[]"));
    assert_eq!(
        contract
            .decode_arguments(&serde_json::to_string(&encoded).unwrap(), limits)
            .unwrap(),
        missing
    );
    let supplied = JsonObject::from([
        ("query".into(), json!("facts")),
        ("note".into(), Value::Null),
    ]);
    let encoded = contract.encode_arguments(&supplied).unwrap();
    assert_eq!(encoded["note"], json!("[null]"));
    assert_eq!(
        contract
            .decode_arguments(&serde_json::to_string(&encoded).unwrap(), limits)
            .unwrap(),
        supplied
    );
    for value in ["null", "[null,null]", "[", "[1.000000000000000001]"] {
        let raw = json!({"query":"\"facts\"","note":value}).to_string();
        assert_eq!(
            contract.decode_arguments(&raw, limits).unwrap_err().code,
            ErrorCode::InvalidArguments
        );
    }
    let record = serde_json::to_string(&contract).unwrap();
    let restored =
        CompiledToolContract::restore(&record, &tool, &target(), contract.digest(), limits)
            .unwrap();
    assert_eq!(
        restored
            .decode_arguments(&serde_json::to_string(&encoded).unwrap(), limits)
            .unwrap(),
        supplied
    );
}
#[test]
fn qualified_models_are_pinned_without_rewriting_an_older_unqualified_contract() {
    let tool = tool();
    let limits = ProviderToolSchemaLimits::default();
    let legacy =
        CompiledToolContract::compile(&tool, target(), &NativeToolSchemaCompiler, limits).unwrap();
    let mut current = target();
    current.model = Some(reference("model-snapshot"));
    let restored = CompiledToolContract::restore(
        &serde_json::to_string(&legacy).unwrap(),
        &tool,
        &current,
        legacy.digest(),
        limits,
    )
    .unwrap();
    assert!(restored.target().model.is_none());
    assert_eq!(restored.digest(), legacy.digest());
    let modern =
        CompiledToolContract::compile(&tool, current.clone(), &NativeToolSchemaCompiler, limits)
            .unwrap();
    current.model.as_mut().unwrap().version = id("different-release");
    assert!(
        CompiledToolContract::restore(
            &serde_json::to_string(&modern).unwrap(),
            &tool,
            &current,
            modern.digest(),
            limits
        )
        .is_err()
    );
}
```

## `tests/support/tool_schema_consumer.rs`

```rust
use futures_util::stream;
use serde_json::json;
use std::collections::BTreeMap;
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}

fn registry(revision: &str) -> Result<SystemInputRegistry, ContractError> {
    SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("active_workspace_id"),
        version: id(revision),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
}

fn model_request(tool: ModelTool) -> ModelRequest {
    ModelRequest {
        request_id: id("model-request"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference("local-model"),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id("example-model"),
            model_id: id("example-model"),
            model_version: id("1"),
            version_semantics: VersionSemantics::Pinned,
            provider: id("example-provider"),
            target: JsonObject::new(),
            deployment_revision: None,
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("1"),
            },
            adapter: reference("example-adapter"),
            capability_revision: id("capabilities"),
            connection_ref: reference("connection"),
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find recent reports".into(),
            }],
        }],
        tools: vec![tool],
        output: ModelOutput::Text {},
        max_output_tokens: 256.try_into().unwrap(),
        options: JsonObject::new(),
        limits: ModelResponseLimits {
            max_input_bytes: 16_384,
            max_response_bytes: 4096,
            max_delta_bytes: 1024,
            max_events: 8,
            max_tool_calls: 1,
        },
    }
}

async fn propose(
    request: &ModelRequest,
    inputs: &JsonObject,
) -> Result<ModelResponse, ModelProtocolError> {
    collect_model_response(
        request,
        Box::pin(stream::iter([
            Ok(ModelEvent::ToolArgumentsDelta {
                index: 0,
                provider_call_id: Some("call".into()),
                name: Some("search_reports".into()),
                delta: serde_json::to_string(inputs).unwrap(),
            }),
            Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::ToolCalls,
                metadata: ModelResponseMetadata::default(),
                continuation: vec![],
            }),
        ])),
    )
    .await
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptor = ToolDescriptor::from_json(
        r##"{
      "tool":{"id":"report-search","version":"1"},
      "name":"search_reports","description":"Search reports in the current workspace",
      "input_schema":{
        "type":"object",
        "properties":{
          "query":{"$ref":"#/$defs/Query"},
          "limit":{"type":"integer","minimum":1,"default":10},
          "workspace_id":{"$ref":"#/$defs/WorkspaceId"}
        },
        "required":["query","workspace_id"],"additionalProperties":false,
        "examples":[{"query":"private example","workspace_id":"f7fba7f5-f8e7-4f44-b885-45d0688e9f33"}],
        "$defs":{
          "Query":{"type":"string","minLength":1},
          "WorkspaceId":{"type":"string","format":"uuid"},
          "Unused":{"type":"string","description":"unrelated internal schema"}
        }
      },
      "agent_parameters":["query","limit"],
      "system_bindings":{"workspace_id":"active_workspace_id"},
      "output_schema":{"type":"array","items":{"type":"string"}},
      "side_effect":"read_only","concurrency":"serial","retry":"never","reconcile":false,
      "max_output_bytes":4096
    }"##,
    )?;
    let registry = registry("1")?;
    let compiler = SchemaCompiler::new();
    let compiled = compiler.compile(descriptor, &registry)?;
    let schema = compiled.model_input_schema();
    assert_eq!(schema["required"], json!(["query"]));
    assert_eq!(schema["additionalProperties"], false);
    assert!(schema["properties"].get("workspace_id").is_none());
    assert!(schema.get("examples").is_none());
    assert!(schema["$defs"].get("WorkspaceId").is_none());
    assert!(schema["$defs"].get("Unused").is_none());
    assert!(schema["$defs"].get("Query").is_some());
    assert_eq!(
        compiled.system_bindings()["workspace_id"].key,
        id("active_workspace_id")
    );
    let model_inputs = BTreeMap::from([("query".into(), json!("recent results"))]);
    compiled.validate_model_inputs(&model_inputs)?;
    // Validation alone does not apply defaults; the binder owns that operation.
    let mut full = model_inputs.clone();
    full.insert(
        "workspace_id".into(),
        json!("f7fba7f5-f8e7-4f44-b885-45d0688e9f33"),
    );
    assert!(compiled.validate_model_inputs(&full).is_err());
    compiled.validate_execution_inputs(&full)?;
    let mut invalid_full = full.clone();
    invalid_full.insert("workspace_id".into(), json!("an-invented-hash"));
    assert!(compiled.validate_execution_inputs(&invalid_full).is_err());
    assert!(compiled.validate_execution_inputs(&model_inputs).is_err());
    let target = ProviderToolTarget {
        model: None,
        provider: id("example-provider"),
        api_contract: ApiContract { operation: id("messages"), version: id("1") },
        capability_revision: id("capabilities"),
    };
    let limits = ProviderToolSchemaLimits::default();
    let projected = CompiledToolContract::compile(&compiled, target.clone(), &NativeToolSchemaCompiler, limits)?;
    let persisted = serde_json::to_string(&projected)?;
    let reopened = CompiledToolContract::restore(&persisted, &compiled, &target, projected.digest(), limits)?;
    let decoded = reopened.decode_arguments(&serde_json::to_string(&model_inputs)?, limits)?;
    compiled.validate_model_inputs(&decoded)?;
    assert_eq!(decoded, model_inputs);
    assert_eq!(reopened.canonical_name(), &id("search_reports"));
    let request = model_request(compiled.to_model_tool());
    assert_eq!(
        propose(&request, &model_inputs).await?.tool_calls[0].validation,
        ToolCallValidation::Valid
    );
    assert_eq!(
        propose(&request, &full).await?.tool_calls[0].validation,
        ToolCallValidation::InvalidArguments
    );
    let saved = serde_json::to_string(&compiled)?;
    let restored = compiler.restore(&saved, &registry, compiled.digest())?;
    assert_eq!(restored.digest(), compiled.digest());
    assert_eq!(restored.model_input_schema(), compiled.model_input_schema());
    let changed_registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("active_workspace_id"),
        version: id("2"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])?;
    assert!(
        compiler
            .restore(&saved, &changed_registry, compiled.digest())
            .is_err()
    );
    println!(
        "tool schema consumer: query/limit exposed; hidden schema omitted; hidden input rejected by the model boundary; full UUID schema checked; compiled identity preserved and changed registry rejected"
    );
    Ok(())
}
```
