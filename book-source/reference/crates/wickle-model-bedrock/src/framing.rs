use crate::{BedrockOperation, BedrockOptions, error};
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::BytesMut;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use wickle::*;
use wickle_model_responses::{SseDecoder, SseEvent};

pub(crate) enum Framing {
    Sse(SseDecoder),
    Aws(AwsFrames),
}
impl Framing {
    pub fn new(options: &BedrockOptions) -> Self {
        match options.operation {
            BedrockOperation::Messages => Self::Sse(SseDecoder::new(
                options.max_transport_bytes,
                options.max_event_bytes,
                options.max_protocol_events,
            )),
            BedrockOperation::InvokeStream => Self::Aws(AwsFrames {
                buffer: BytesMut::new(),
                bytes: 0,
                frames: 0,
                max_bytes: options.max_transport_bytes,
                max_frame: options.max_event_bytes,
                max_frames: options.max_protocol_events,
            }),
        }
    }
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseEvent>, ContractError> {
        match self {
            Self::Sse(value) => value.push(bytes),
            Self::Aws(value) => value.push(bytes),
        }
    }
    pub fn finish(&self) -> Result<(), ContractError> {
        match self {
            Self::Sse(value) => value.finish(),
            Self::Aws(value) => {
                if value.buffer.is_empty() {
                    Ok(())
                } else {
                    Err(invalid())
                }
            }
        }
    }
}
pub(crate) struct AwsFrames {
    buffer: BytesMut,
    bytes: usize,
    frames: usize,
    max_bytes: usize,
    max_frame: usize,
    max_frames: usize,
}
impl AwsFrames {
    fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseEvent>, ContractError> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|n| *n <= self.max_bytes)
            .ok_or_else(invalid)?;
        self.buffer.extend_from_slice(bytes);
        let mut events = vec![];
        while self.buffer.len() >= 4 {
            let len =
                u32::from_be_bytes(self.buffer[..4].try_into().map_err(|_| invalid())?) as usize;
            if len < 16 || len > self.max_frame {
                return Err(invalid());
            }
            if self.buffer.len() < len {
                break;
            }
            self.frames = self
                .frames
                .checked_add(1)
                .filter(|n| *n <= self.max_frames)
                .ok_or_else(invalid)?;
            let frame = self.buffer.split_to(len).freeze();
            // The AWS library validates both prelude and message CRCs and headers.
            let message = aws_smithy_eventstream::frame::read_message_from(&frame[..])
                .map_err(|_| invalid())?;
            let mut headers = BTreeMap::new();
            for header in message.headers() {
                let name = header.name().as_str();
                if headers.contains_key(name) {
                    return Err(invalid());
                }
                headers.insert(name, header.value());
            }
            let text = |key| {
                headers
                    .get(key)
                    .and_then(|v| v.as_string().ok())
                    .map(|v| v.as_str())
            };
            match text(":message-type") {
                Some("event") if text(":event-type") == Some("chunk") => {
                    if text(":content-type").is_some_and(|v| v != "application/json") {
                        return Err(invalid());
                    }
                    let body = std::str::from_utf8(message.payload()).map_err(|_| invalid())?;
                    let body = parse_json(body)?;
                    let encoded = body
                        .get("bytes")
                        .and_then(Value::as_str)
                        .ok_or_else(invalid)?;
                    let decoded = STANDARD.decode(encoded).map_err(|_| invalid())?;
                    let data = String::from_utf8(decoded).map_err(|_| invalid())?;
                    // The Messages decoder owns JSON/block/terminal validation.
                    events.push(SseEvent { name: None, data });
                }
                Some("exception" | "error") => {
                    let kind = match text(":exception-type").or_else(|| text(":error-code")) {
                        Some("throttlingException") => "rate_limit_error",
                        Some("modelTimeoutException") => {
                            return Err(error(ErrorCode::DeadlineExceeded, "model_timeout"));
                        }
                        Some(
                            "internalServerException"
                            | "modelStreamErrorException"
                            | "serviceUnavailableException",
                        ) => "api_error",
                        Some("validationException") => "invalid_request_error",
                        Some("accessDeniedException") => "permission_error",
                        _ => return Err(invalid()),
                    };
                    events.push(SseEvent {
                        name: Some("error".into()),
                        data: json!({"type":"error","error":{"type":kind}}).to_string(),
                    });
                }
                _ => return Err(invalid()),
            }
        }
        Ok(events)
    }
}
fn invalid() -> ContractError {
    error(ErrorCode::InvalidContract, "aws_event_stream")
}
