use crate::{
    BedrockAudience, BedrockConnection, BedrockCredentialContext, BedrockEndpoint,
    BedrockOperation, auth, error, framing::Framing,
};
use futures_util::stream;
use reqwest::Response;
use serde_json::Value;
use std::collections::VecDeque;
use wickle::*;
use wickle_model_anthropic::protocol::{MessagesDecoder, encode_request};

/// One Bedrock Claude POST per invocation, streamed into Wickle model events.
/// The adapter does not retry, run Tool handlers, or load environment variables.
#[derive(Debug, Clone)]
pub struct BedrockModel {
    connection: BedrockConnection,
}
impl BedrockModel {
    /// Bind an already configured connection without making a network request.
    pub fn new(connection: BedrockConnection) -> Self {
        Self { connection }
    }
}
impl ModelPort for BedrockModel {
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
            decoder: MessagesDecoder::new(request, None),
            framing: Framing::new(&self.connection.0.options),
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
    connection: &'a BedrockConnection,
    request: &'a ModelRequest,
    context: &'a ModelCallContext,
    response: Option<Response>,
    decoder: MessagesDecoder<'a>,
    framing: Framing,
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
        if self.connection.0.options.endpoint == BedrockEndpoint::Mantle
            && matches!(self.request.output, ModelOutput::JsonSchema { .. })
        {
            return Err(error(
                ErrorCode::ModelCapabilityUnsupported,
                "mantle_json_output",
            ));
        }
        let mut value = encode_request(self.request)?;
        match self.connection.0.options.operation {
            BedrockOperation::Messages => {
                value["model"] = serde_json::json!(self.connection.0.options.selector.value())
            }
            BedrockOperation::InvokeStream => {
                let object = value.as_object_mut().expect("encoded object");
                object.remove("model");
                object.remove("stream");
                object.insert(
                    "anthropic_version".into(),
                    serde_json::json!("bedrock-2023-05-31"),
                );
            }
        }
        let body =
            serde_json::to_vec(&value).map_err(|_| error(ErrorCode::InvalidJson, "request"))?;
        if body.len() > self.request.limits.max_input_bytes {
            return Err(error(ErrorCode::ModelCapabilityUnsupported, "request_size"));
        }
        let url = self.connection.0.url.clone();
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "content-type",
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        headers.insert(
            "accept",
            reqwest::header::HeaderValue::from_static(self.content_type()),
        );
        headers.insert(
            "accept-encoding",
            reqwest::header::HeaderValue::from_static("identity"),
        );
        if self.connection.0.options.operation == BedrockOperation::Messages {
            headers.insert(
                "anthropic-version",
                reqwest::header::HeaderValue::from_static("2023-06-01"),
            );
        }
        let headers = match auth::authorize(
            self.connection.0.credentials.as_ref(),
            BedrockCredentialContext {
                scope: &self.context.scope,
                audience: BedrockAudience::Inference,
                region: &self.connection.0.options.region,
                cancellation: &self.context.cancellation,
                deadline: self.context.deadline,
            },
            auth::SigningRequest {
                method: "POST",
                url: &url,
                body: &body,
                service: self.connection.signing_service(),
                clock: self.connection.0.clock.as_ref(),
                headers,
            },
        )
        .await
        {
            Ok(headers) => headers,
            Err(failure) if failure.code == ErrorCode::AccessDenied => {
                self.queue.push_back(Ok(ModelEvent::ResponseError {
                    kind: ModelFailureKind::Authentication,
                    metadata: self.decoder.metadata.clone(),
                }));
                self.finished = true;
                return Ok(());
            }
            Err(failure) => return Err(failure),
        };
        let operation = self
            .connection
            .0
            .client
            .post(url)
            .headers(headers)
            .body(body)
            .send();
        let mut response = tokio::select! { biased;
            _ = self.context.cancellation.cancelled() => return Err(error(ErrorCode::Cancelled, "request")),
            _ = tokio::time::sleep_until(self.context.deadline) => return Err(error(ErrorCode::DeadlineExceeded, "request")),
            result = operation => result.map_err(transport_error)?,
        };
        self.decoder.metadata.provider_request_id = response
            .headers()
            .get("request-id")
            .or_else(|| response.headers().get("x-amzn-requestid"))
            .or_else(|| response.headers().get("x-amz-request-id"))
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
                413 => ModelFailureKind::ContextOverflow,
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
        if !content_type.is_some_and(|value| value.eq_ignore_ascii_case(self.content_type())) {
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
    fn content_type(&self) -> &'static str {
        match self.connection.0.options.operation {
            BedrockOperation::Messages => "text/event-stream",
            BedrockOperation::InvokeStream => "application/vnd.amazon.eventstream",
        }
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
