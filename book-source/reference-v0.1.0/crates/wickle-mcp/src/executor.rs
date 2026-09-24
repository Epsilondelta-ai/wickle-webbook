use crate::{McpClient, McpSnapshot, error};
use serde_json::{Value, json};
use std::sync::atomic::Ordering;
use wickle::*;
/// One approved remote Tool. It sends only already-bound execution arguments.
#[derive(Clone)]
pub struct McpToolExecutor {
    pub(crate) client: McpClient,
    pub(crate) snapshot: McpSnapshot,
    pub(crate) remote: String,
    pub(crate) compiled: CompiledTool,
    pub(crate) segment: Option<(Id, Id)>,
}
impl McpClient {
    /// Bind an explicitly reviewed/compiled descriptor. This never chooses exposure
    /// or permissions from remote annotations and never activates another Tool.
    pub fn bind_tool(
        &self,
        snapshot: &McpSnapshot,
        remote: &str,
        compiled: CompiledTool,
    ) -> Result<McpToolExecutor, ContractError> {
        if snapshot.scope() != self.scope() || snapshot.connection_ref() != self.connection_ref() {
            return Err(error(ErrorCode::AccessDenied, "snapshot_scope"));
        }
        snapshot.validate_descriptor(remote, compiled.descriptor())?;
        Ok(McpToolExecutor {
            client: self.clone(),
            snapshot: snapshot.clone(),
            remote: remote.into(),
            compiled,
            segment: None,
        })
    }
}
fn failed(code: &str, effect: ToolEffect) -> ToolExecutionResult {
    ToolExecutionResult {
        outcome: ToolExecutionOutcome::Failed {
            code: Id::new(code).expect("static code"),
        },
        effect,
        receipt: None,
    }
}
impl ToolExecutor for McpToolExecutor {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            if context.scope != *self.client.scope()
                || self.segment.as_ref().is_some_and(|(run, segment)| {
                    run != &context.run_id || context.binding_set_id.as_ref() != Some(segment)
                })
            {
                return Err(error(ErrorCode::AccessDenied, "execution_scope"));
            }
            if self.compiled.validate_execution_inputs(args).is_err() {
                return Ok(failed("mcp.invalid_arguments", ToolEffect::NotApplied));
            }
            let request = json!({"jsonrpc":"2.0","id":u32::MAX,"method":"tools/call","params":{"name":self.remote,"arguments":args}});
            if serde_json::to_vec(&request)
                .map_err(|_| error(ErrorCode::InvalidJson, "arguments"))?
                .len()
                > self.client.0.limits.max_frame_bytes
            {
                return Ok(failed("mcp.input_limit", ToolEffect::NotApplied));
            }
            let _guard = match self
                .client
                .lock(&context.cancellation, context.deadline)
                .await
            {
                Ok(guard) => guard,
                Err(_) => {
                    return Ok(failed(
                        "mcp.before_dispatch_cancelled",
                        ToolEffect::NotApplied,
                    ));
                }
            };
            let (current, revision) = match self
                .client
                .discover_locked(&context.cancellation, context.deadline)
                .await
            {
                Ok(value) => value,
                Err(_) => {
                    let _ = self
                        .client
                        .close(tokio::time::Instant::now() + self.client.0.limits.close_timeout)
                        .await;
                    return Ok(failed("mcp.discovery_failed", ToolEffect::NotApplied));
                }
            };
            if !self.snapshot.matches_tool(&current, &self.remote)
                || self.client.0.changes.load(Ordering::SeqCst) != revision
            {
                return Ok(failed("mcp.descriptor_drift", ToolEffect::NotApplied));
            }
            if context.cancellation.is_cancelled()
                || tokio::time::Instant::now() >= context.deadline
            {
                return Ok(failed(
                    "mcp.before_dispatch_cancelled",
                    ToolEffect::NotApplied,
                ));
            }
            let effect = if self.compiled.descriptor().side_effect == ToolSideEffect::ReadOnly {
                ToolEffect::NotApplied
            } else {
                ToolEffect::Unknown
            };
            let result = match self
                .client
                .call_locked(&self.remote, args, &context.cancellation, context.deadline)
                .await
            {
                Ok(result) => result,
                Err(_) => {
                    let _ = self
                        .client
                        .close(tokio::time::Instant::now() + self.client.0.limits.close_timeout)
                        .await;
                    return Ok(failed(
                        "mcp.call_failed",
                        if self.client.0.changes.load(Ordering::SeqCst) != revision {
                            ToolEffect::Unknown
                        } else {
                            effect
                        },
                    ));
                }
            };
            if self.client.0.changes.load(Ordering::SeqCst) != revision {
                return Ok(failed(
                    "mcp.descriptor_changed_during_call",
                    ToolEffect::Unknown,
                ));
            }
            if result.is_error == Some(true) {
                return Ok(failed("mcp.remote_error", effect));
            }
            let value = if self
                .snapshot
                .raw_tool(&self.remote)
                .and_then(|v| v.get("outputSchema"))
                .is_some_and(|v| !v.is_null())
            {
                match result.structured_content {
                    Some(value) => value,
                    None => return Ok(failed("mcp.missing_structured_output", effect)),
                }
            } else {
                let content = serde_json::to_value(result.content)
                    .map_err(|_| error(ErrorCode::InvalidContract, "content"))?;
                let Some(items) = content.as_array() else {
                    return Ok(failed("mcp.invalid_content", effect));
                };
                if items.iter().any(|v| {
                    v.get("type") != Some(&json!("text"))
                        || v.get("text").and_then(Value::as_str).is_none()
                }) {
                    return Ok(failed("mcp.unsupported_content", effect));
                }
                json!({"content":items.iter().map(|v|json!({"type":"text","text":v["text"]})).collect::<Vec<_>>()})
            };
            if serde_json::to_vec(&value)
                .map_err(|_| error(ErrorCode::InvalidJson, "output"))?
                .len() as u64
                > self.compiled.descriptor().max_output_bytes.get()
            {
                return Ok(failed("mcp.output_limit", effect));
            }
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded { value },
                effect,
                receipt: None,
            })
        })
    }
}
