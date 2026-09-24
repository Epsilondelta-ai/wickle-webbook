use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, io,
    num::NonZeroU64,
};

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::{
    ContractError, ErrorCode, Id, JsonDigest, JsonObject, ModelFailureKind, ModelPurpose,
    ModelUsage, PortStream, ResolvedModelRoute, Scope, VersionedRef, parse_json,
    serialization::data_digest,
};

/// Adapter-owned provider, implementation, and credential-binding identities.
/// Actual credentials remain in the adapter instance, never in a model request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelPortBinding {
    /// Registered service key, distinct for direct and hosted provider paths.
    pub provider: Id,
    /// Exact adapter implementation identity.
    pub adapter: VersionedRef,
    /// Host-owned credential/connection binding revision.
    pub connection_ref: VersionedRef,
}

impl ModelPortBinding {
    /// Require all adapter identities to match the immutable selected route.
    pub fn matches_route(&self, route: &ResolvedModelRoute) -> bool {
        self.provider == route.provider
            && self.adapter == route.adapter
            && self.connection_ref == route.connection_ref
    }
}

/// One physical model request's runtime context, without credentials or system inputs.
#[derive(Debug, Clone)]
pub struct ModelCallContext {
    /// Budget reservation and physical invocation identity.
    pub attempt_id: Id,
    /// Owning execution.
    pub run_id: Id,
    /// Authenticated data/execution scope supplied by the Host.
    pub scope: Scope,
    /// Cooperative cancellation signal.
    pub cancellation: CancellationToken,
    /// Effective deadline for this physical invocation.
    pub deadline: tokio::time::Instant,
}

/// A single physical provider invocation, without an internal agent loop or retry.
/// Adapters disable hidden SDK retries and provider-native tool execution. They
/// enforce wire/body limits while decoding, report safe typed errors, and end the
/// stream after the one request. Credentials are obtained through their binding.
pub trait ModelPort: Send + Sync {
    /// Identities of the adapter and connection actually used by this instance.
    fn binding(&self) -> ModelPortBinding;
    /// Generate one response. Partial argument deltas are never execution commands.
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent>;
}

/// Roles in an already-authorized model projection, separate from stored messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelRole {
    /// Trusted instructions selected by the core's projection layer.
    System,
    /// User data selected for this invocation.
    User,
    /// Prior model content and proposed calls.
    Assistant,
    /// Observations paired with prior proposed calls.
    Tool,
}

/// Provider continuation data pinned to an exact route, including its versions.
/// Explicit serialization is for protected storage/provider replay only.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpaqueContinuation {
    route_digest: JsonDigest,
    data: Value,
}

impl OpaqueContinuation {
    /// Bind provider-replay data to the route that produced it.
    pub fn new(route: &ResolvedModelRoute, data: Value) -> Self {
        Self {
            route_digest: route.digest(),
            data,
        }
    }
    /// Exact route identity required before replaying these bytes.
    pub fn route_digest(&self) -> &JsonDigest {
        &self.route_digest
    }
    /// Explicit privileged access for the matching provider adapter.
    pub fn data(&self) -> &Value {
        &self.data
    }
}

impl fmt::Debug for OpaqueContinuation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpaqueContinuation")
            .field("route_digest", &self.route_digest)
            .field("data", &"<redacted>")
            .finish()
    }
}

/// Provider-facing content selected explicitly by the core projection layer.
/// No variant contains ExecutionContext, complete system inputs, or storage records.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelContent {
    /// Authorized text, including instructions or observations as indicated by role.
    Text {
        /// Text sent to the selected model.
        text: String,
    },
    /// Explicitly selected JSON data; it conveys no authority by itself.
    Json {
        /// Model-visible JSON.
        value: Value,
    },
    /// A prior model-proposed call, containing only model-owned arguments.
    ToolCall {
        /// Provider/projection call identity, paired with its observation.
        provider_call_id: Id,
        /// Normalized model-facing tool name.
        name: Id,
        /// Model-owned arguments, never merged system execution arguments.
        arguments: JsonObject,
    },
    /// A limited observation without receipts or hidden input maps.
    ToolResult {
        /// Matching call in the preceding assistant tool round.
        provider_call_id: Id,
        /// Explicitly selected model-visible observation.
        content: Value,
    },
    /// Provider-replay data permitted only on its original exact route.
    Opaque {
        /// Protected continuation selected for provider replay.
        continuation: OpaqueContinuation,
    },
}

impl fmt::Debug for ModelContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Text { .. } => "text",
            Self::Json { .. } => "json",
            Self::ToolCall { .. } => "tool_call",
            Self::ToolResult { .. } => "tool_result",
            Self::Opaque { .. } => "opaque",
        };
        f.debug_struct("ModelContent")
            .field("type", &kind)
            .finish_non_exhaustive()
    }
}

/// A projected message, not an original transcript record or client-submitted role.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelMessage {
    /// Role selected after validating provenance and allowed instruction placement.
    pub role: ModelRole,
    /// Model-visible content only.
    pub content: Vec<ModelContent>,
}

/// Only the compiled model-facing portion of a registered tool definition.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelTool {
    /// Portable normalized ASCII name: letters, digits, underscore or hyphen, 1..64 bytes.
    pub name: Id,
    /// Model-facing description; hidden input descriptions are excluded by the compiler.
    pub description: String,
    /// Derived input schema, without system-owned fields or their definitions/examples.
    pub model_input_schema: Value,
}

impl fmt::Debug for ModelTool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelTool")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// Provider output-mode request. Final candidate verification belongs to the driver.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelOutput {
    /// Ordinary text output.
    Text {},
    /// Structured output with an already-resolved, model-visible schema.
    JsonSchema {
        /// Resolved output schema, without storage lookup references.
        schema: Value,
    },
}

impl fmt::Debug for ModelOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Text {} => "ModelOutput::Text",
            Self::JsonSchema { .. } => "ModelOutput::JsonSchema(<redacted>)",
        })
    }
}

/// Finite decoding bounds, independent of token usage and total run budgets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelResponseLimits {
    /// Maximum serialized logical request size, including projection and schemas.
    pub max_input_bytes: usize,
    /// Cumulative UTF-8 response payload bytes, including call metadata and opaque data.
    pub max_response_bytes: usize,
    /// Maximum bytes in one text or tool-argument fragment.
    pub max_delta_bytes: usize,
    /// Maximum stream events, including empty fragments and terminal events.
    pub max_events: usize,
    /// Maximum distinct proposed calls; zero prohibits tool-call output.
    pub max_tool_calls: usize,
}

/// Immutable-for-invocation logical request containing only a prepared projection.
/// Explicit serialization is for protected storage/transport codecs, not public logs.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRequest {
    /// Request identity, scoped to this physical provider invocation.
    pub request_id: Id,
    /// Agent, verification, or compaction accounting purpose.
    pub purpose: ModelPurpose,
    /// Exact selected provider/model/API/connection revisions.
    pub route: ResolvedModelRoute,
    /// Authorized projection; never the raw transcript or ExecutionContext.
    pub messages: Vec<ModelMessage>,
    /// Derived model-facing tool definitions only.
    pub tools: Vec<ModelTool>,
    /// Requested provider output format, distinct from final outcome verification.
    pub output: ModelOutput,
    /// Finite provider output-token request.
    pub max_output_tokens: NonZeroU64,
    /// Host-owned logical options; the Host/router must validate selected catalog schemas.
    /// Adapters explicitly map supported keys to their API; this is not a raw wire-body merge.
    #[serde(default, skip_serializing_if = "JsonObject::is_empty")]
    pub options: JsonObject,
    /// Input and response decoding limits selected for this route.
    pub limits: ModelResponseLimits,
}

impl fmt::Debug for ModelRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelRequest")
            .field("request_id", &self.request_id)
            .field("purpose", &self.purpose)
            .field("route_digest", &self.route.digest())
            .field("message_count", &self.messages.len())
            .field("tool_count", &self.tools.len())
            .finish_non_exhaustive()
    }
}

impl ModelRequest {
    /// Capabilities implied by the prepared request: `text`, plus `tool_calling`
    /// for tool schemas and `json_output` for a structured output contract.
    /// Host requirements may add further registered features.
    pub fn required_capabilities(&self) -> std::collections::BTreeSet<Id> {
        let mut features = std::collections::BTreeSet::from([
            Id::new("text").expect("static capability identifier")
        ]);
        if !self.tools.is_empty() {
            features.insert(Id::new("tool_calling").expect("static capability identifier"));
        }
        if matches!(self.output, ModelOutput::JsonSchema { .. }) {
            features.insert(Id::new("json_output").expect("static capability identifier"));
        }
        features
    }

    /// Hash the complete prepared request, without introducing runtime credentials.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }

    /// Check finite bounds, projected protocol, schemas and continuation route identity.
    /// Host policy and actual adapter-binding checks are separate execution boundaries.
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.limits.max_input_bytes == 0
            || self.limits.max_response_bytes == 0
            || self.limits.max_delta_bytes == 0
            || self.limits.max_events == 0
        {
            return Err(invalid_request("model_request.limits"));
        }
        json_size(self, self.limits.max_input_bytes)
            .map_err(|_| invalid_request("model_request.input_size"))?;
        let mut names = BTreeSet::new();
        for tool in &self.tools {
            if !valid_name(tool.name.as_str())
                || !names.insert(&tool.name)
                || tool.model_input_schema.get("type").and_then(Value::as_str) != Some("object")
            {
                return Err(invalid_request("model_request.tools"));
            }
            compile_schema(&tool.model_input_schema)?;
        }
        if let ModelOutput::JsonSchema { schema } = &self.output {
            compile_schema(schema)?;
        }
        let mut pending_calls = BTreeSet::new();
        for message in &self.messages {
            if !pending_calls.is_empty() && message.role != ModelRole::Tool {
                return Err(invalid_request("model_request.tool_results"));
            }
            let mut round_calls = BTreeSet::new();
            for content in &message.content {
                match content {
                    ModelContent::ToolCall {
                        provider_call_id,
                        name,
                        ..
                    } => {
                        if message.role != ModelRole::Assistant
                            || !valid_call_id(provider_call_id.as_str())
                            || !valid_name(name.as_str())
                            || !round_calls.insert(provider_call_id.clone())
                        {
                            return Err(invalid_request("model_request.tool_calls"));
                        }
                    }
                    ModelContent::ToolResult {
                        provider_call_id, ..
                    } => {
                        if message.role != ModelRole::Tool
                            || !pending_calls.remove(provider_call_id)
                        {
                            return Err(invalid_request("model_request.tool_results"));
                        }
                    }
                    ModelContent::Opaque { continuation } => {
                        if message.role != ModelRole::Assistant
                            || continuation.route_digest() != &self.route.digest()
                        {
                            return Err(invalid_request("model_request.continuation"));
                        }
                    }
                    ModelContent::Text { .. } | ModelContent::Json { .. } => {
                        if message.role == ModelRole::Tool {
                            return Err(invalid_request("model_request.tool_results"));
                        }
                    }
                }
            }
            pending_calls.extend(round_calls);
        }
        if !pending_calls.is_empty() {
            return Err(invalid_request("model_request.tool_results"));
        }
        Ok(())
    }
}

/// Provider finish classification, normalized independently of provider strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelFinish {
    /// A complete answer without proposed tool calls.
    Stop,
    /// A complete response proposing one or more calls.
    ToolCalls,
    /// Truncated output. Assembly returns an error and no executable call plan.
    Length,
    /// Explicit refusal without a tool plan; not a successful business outcome.
    Refusal,
}

/// Facts actually reported by the provider; omitted identifiers and usage stay unknown.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelResponseMetadata {
    /// Provider correlation identity, if reported.
    pub provider_request_id: Option<Id>,
    /// Actual response model, not copied from the requested route as a guess.
    pub reported_model_id: Option<Id>,
    /// Actual reported release/version, if supplied by the provider.
    pub reported_model_version: Option<Id>,
    /// Reported or explicitly estimated token usage, never implicit zeroes.
    pub usage: Option<ModelUsage>,
}

impl fmt::Debug for ModelResponseMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelResponseMetadata")
            .field("usage", &self.usage)
            .finish_non_exhaustive()
    }
}

/// Untrusted provider response fragments. None is a core ToolCall or dispatch permit.
#[derive(Clone, PartialEq)]
pub enum ModelEvent {
    /// Candidate text fragment.
    TextDelta {
        /// UTF-8 text, subject to per-fragment and aggregate bounds.
        text: String,
    },
    /// Fragment of one proposed call. Identity/name may arrive in an earlier fragment.
    ToolArgumentsDelta {
        /// Stable provider output index, allowing interleaved call fragments.
        index: u32,
        /// Complete provider identity when available; conflicting replacements fail.
        provider_call_id: Option<String>,
        /// Complete normalized name when available; conflicting replacements fail.
        name: Option<String>,
        /// JSON argument fragment, never executed before full response validation.
        delta: String,
    },
    /// Complete logical response; the adapter must subsequently end this request stream.
    ResponseCompleted {
        /// Normalized finish classification.
        finish: ModelFinish,
        /// Actually reported response metadata.
        metadata: ModelResponseMetadata,
        /// Protected continuation data bound to this request's route.
        continuation: Vec<OpaqueContinuation>,
    },
    /// Safe classified provider failure, without raw SDK error strings.
    ResponseError {
        /// Classification used by the core's bounded recovery policy.
        kind: ModelFailureKind,
        /// Facts actually reported before failure.
        metadata: ModelResponseMetadata,
    },
}

impl fmt::Debug for ModelEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::TextDelta { .. } => "text_delta",
            Self::ToolArgumentsDelta { .. } => "tool_arguments_delta",
            Self::ResponseCompleted { .. } => "response_completed",
            Self::ResponseError { .. } => "response_error",
        };
        f.debug_struct("ModelEvent")
            .field("type", &kind)
            .finish_non_exhaustive()
    }
}

/// Protocol validation only; even Valid proposals require the later tool boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallValidation {
    /// Known model-facing tool and conforming model-owned arguments.
    Valid,
    /// Not among the tools advertised for this request; execution is prohibited.
    UnknownTool,
    /// Model-owned arguments fail the advertised schema; execution is prohibited.
    InvalidArguments,
}

/// A complete provider proposal, awaiting core call-ID allocation and protected planning.
/// It cannot be passed as a core ToolCall without explicit later materialization.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposedToolCall {
    /// Provider-local identity, scoped by the ModelResponse request_id.
    pub provider_call_id: Id,
    /// Normalized model-facing name.
    pub name: Id,
    /// Parsed model-owned JSON object; no system parameters have been injected.
    pub model_inputs: JsonObject,
    /// Preliminary schema/availability result, not policy or tool authorization.
    pub validation: ToolCallValidation,
}

impl fmt::Debug for ProposedToolCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProposedToolCall")
            .field("name", &self.name)
            .field("validation", &self.validation)
            .finish_non_exhaustive()
    }
}

/// A complete protocol response, distinct from a verified agent outcome or persisted plan.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelResponse {
    /// Request that scopes provider call identifiers.
    pub request_id: Id,
    /// Exact route that produced this response and continuation data.
    pub route_digest: JsonDigest,
    /// Complete candidate text; final output verification remains separate.
    pub text: String,
    /// Complete proposals, including structured rejection reasons for invalid tools.
    pub tool_calls: Vec<ProposedToolCall>,
    /// Complete stop/tool/refusal classification.
    pub finish: ModelFinish,
    /// Provider-reported facts.
    pub metadata: ModelResponseMetadata,
    /// Protected provider-replay data.
    pub continuation: Vec<OpaqueContinuation>,
}

impl fmt::Debug for ModelResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelResponse")
            .field("request_id", &self.request_id)
            .field("finish", &self.finish)
            .field("tool_count", &self.tool_calls.len())
            .finish_non_exhaustive()
    }
}

/// Safe, stable assembly/recovery reasons without submitted fragments or SDK messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelProtocolErrorCode {
    /// Invalid projection, schema, or request bounds.
    InvalidRequest,
    /// The stream failed before a complete response was received.
    StreamFailure,
    /// EOF arrived without a terminal response.
    MissingCompletion,
    /// Another terminal or any event followed completion.
    UnexpectedEvent,
    /// Arguments were malformed, ambiguous, or not a JSON object.
    InvalidArguments,
    /// A provider call identity was missing, malformed, or replaced.
    InvalidCallId,
    /// A model-facing name was missing, malformed, or replaced.
    InvalidToolName,
    /// Different proposed calls reused the same provider-local identity.
    DuplicateCallId,
    /// The normalized finish classification conflicts with the actual proposed calls.
    FinishMismatch,
    /// A response event, byte, fragment or tool count exceeded its finite bound.
    ResponseLimitExceeded,
    /// Continuation data belongs to a different route or version.
    RouteMismatch,
    /// The adapter reported a typed provider failure.
    ProviderFailure,
    /// Output was truncated, even if individual argument fragments looked complete.
    OutputTruncated,
}

/// A failed response may expose bounded candidate text explicitly, never partial calls.
/// Serialization is for protected failure records, never automatic public logging.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelProtocolError {
    /// Recovery classification; the core decides whether another paid attempt is allowed.
    pub kind: ModelFailureKind,
    /// Safe protocol/provider reason.
    pub code: ModelProtocolErrorCode,
    /// Metadata actually observed; missing values remain unknown.
    pub metadata: Box<ModelResponseMetadata>,
    partial_text: String,
}

impl ModelProtocolError {
    /// Construct a sanitized failure, without embedding an arbitrary SDK error.
    pub fn new(
        kind: ModelFailureKind,
        code: ModelProtocolErrorCode,
        metadata: ModelResponseMetadata,
    ) -> Self {
        Self {
            kind,
            code,
            metadata: Box::new(metadata),
            partial_text: String::new(),
        }
    }
    /// Explicit access to bounded, unverified text; it is not a completed answer.
    pub fn partial_text(&self) -> &str {
        &self.partial_text
    }
}

impl fmt::Debug for ModelProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelProtocolError")
            .field("kind", &self.kind)
            .field("code", &self.code)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for ModelProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {:?}", self.kind, self.code)
    }
}
impl std::error::Error for ModelProtocolError {}

#[derive(Default)]
struct PartialCall {
    provider_call_id: Option<String>,
    name: Option<String>,
    arguments: String,
}

struct ResponseAssembly {
    text: String,
    calls: BTreeMap<u32, PartialCall>,
    bytes: usize,
    events: usize,
    terminal: Option<(ModelFinish, ModelResponseMetadata, Vec<OpaqueContinuation>)>,
}

impl ResponseAssembly {
    fn error(&self, code: ModelProtocolErrorCode) -> ModelProtocolError {
        let mut error = ModelProtocolError::new(
            ModelFailureKind::Protocol,
            code,
            self.terminal
                .as_ref()
                .map_or_else(ModelResponseMetadata::default, |(_, metadata, _)| {
                    metadata.clone()
                }),
        );
        error.partial_text = self.text.clone();
        error
    }
    fn add_bytes(&mut self, count: usize, maximum: usize) -> Result<(), ModelProtocolError> {
        let total = self
            .bytes
            .checked_add(count)
            .filter(|n| *n <= maximum)
            .ok_or_else(|| self.error(ModelProtocolErrorCode::ResponseLimitExceeded))?;
        self.bytes = total;
        Ok(())
    }
}

/// Collect one logical response through EOF. No partial call escapes on failure.
/// The caller must bound time/cancellation around collection: a stream that never
/// yields cannot be stopped by event/byte limits alone. No tools are executed here.
pub async fn collect_model_response(
    request: &ModelRequest,
    mut stream: PortStream<'_, ModelEvent>,
) -> Result<ModelResponse, ModelProtocolError> {
    request.validate().map_err(|_| {
        ModelProtocolError::new(
            ModelFailureKind::Protocol,
            ModelProtocolErrorCode::InvalidRequest,
            ModelResponseMetadata::default(),
        )
    })?;
    let mut assembly = ResponseAssembly {
        text: String::new(),
        calls: BTreeMap::new(),
        bytes: 0,
        events: 0,
        terminal: None,
    };
    while let Some(next) = stream.next().await {
        if assembly.terminal.is_some() {
            return Err(assembly.error(ModelProtocolErrorCode::UnexpectedEvent));
        }
        assembly.events = assembly
            .events
            .checked_add(1)
            .filter(|count| *count <= request.limits.max_events)
            .ok_or_else(|| assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded))?;
        let event = next.map_err(|_| {
            let mut error = assembly.error(ModelProtocolErrorCode::StreamFailure);
            error.kind = ModelFailureKind::Transport;
            error
        })?;
        match event {
            ModelEvent::TextDelta { text } => {
                if text.len() > request.limits.max_delta_bytes {
                    return Err(assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded));
                }
                assembly.add_bytes(text.len(), request.limits.max_response_bytes)?;
                assembly.text.push_str(&text);
            }
            ModelEvent::ToolArgumentsDelta {
                index,
                provider_call_id,
                name,
                delta,
            } => {
                if delta.len() > request.limits.max_delta_bytes
                    || (!assembly.calls.contains_key(&index)
                        && assembly.calls.len() >= request.limits.max_tool_calls)
                {
                    return Err(assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded));
                }
                if provider_call_id
                    .as_ref()
                    .is_some_and(|id| !valid_call_id(id))
                {
                    return Err(assembly.error(ModelProtocolErrorCode::InvalidCallId));
                }
                if name.as_ref().is_some_and(|name| !valid_name(name)) {
                    return Err(assembly.error(ModelProtocolErrorCode::InvalidToolName));
                }
                let bytes = delta
                    .len()
                    .saturating_add(provider_call_id.as_ref().map_or(0, String::len))
                    .saturating_add(name.as_ref().map_or(0, String::len));
                assembly.add_bytes(bytes, request.limits.max_response_bytes)?;
                if let Some(existing) = assembly.calls.get(&index) {
                    if existing
                        .provider_call_id
                        .as_ref()
                        .zip(provider_call_id.as_ref())
                        .is_some_and(|(old, new)| old != new)
                    {
                        return Err(assembly.error(ModelProtocolErrorCode::InvalidCallId));
                    }
                    if existing
                        .name
                        .as_ref()
                        .zip(name.as_ref())
                        .is_some_and(|(old, new)| old != new)
                    {
                        return Err(assembly.error(ModelProtocolErrorCode::InvalidToolName));
                    }
                }
                let call = assembly.calls.entry(index).or_default();
                if let Some(id) = provider_call_id {
                    call.provider_call_id = Some(id);
                }
                if let Some(name) = name {
                    call.name = Some(name);
                }
                call.arguments.push_str(&delta);
            }
            ModelEvent::ResponseCompleted {
                finish,
                metadata,
                continuation,
            } => {
                let mut bytes = metadata_bytes(&metadata)
                    .ok_or_else(|| assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded))?;
                for item in &continuation {
                    if item.route_digest() != &request.route.digest() {
                        return Err(assembly.error(ModelProtocolErrorCode::RouteMismatch));
                    }
                    let size = json_size(item.data(), request.limits.max_response_bytes).map_err(
                        |_| assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded),
                    )?;
                    bytes = bytes.checked_add(size).ok_or_else(|| {
                        assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded)
                    })?;
                }
                assembly.add_bytes(bytes, request.limits.max_response_bytes)?;
                assembly.terminal = Some((finish, metadata, continuation));
            }
            ModelEvent::ResponseError { kind, metadata } => {
                let bytes = metadata_bytes(&metadata)
                    .ok_or_else(|| assembly.error(ModelProtocolErrorCode::ResponseLimitExceeded))?;
                assembly.add_bytes(bytes, request.limits.max_response_bytes)?;
                let mut error = ModelProtocolError::new(
                    kind,
                    ModelProtocolErrorCode::ProviderFailure,
                    metadata,
                );
                error.partial_text = assembly.text;
                return Err(error);
            }
        }
    }
    let (finish, metadata, continuation) = assembly
        .terminal
        .as_ref()
        .ok_or_else(|| assembly.error(ModelProtocolErrorCode::MissingCompletion))?;
    if *finish == ModelFinish::Length {
        return Err(assembly.error(ModelProtocolErrorCode::OutputTruncated));
    }
    if (*finish == ModelFinish::ToolCalls) != !assembly.calls.is_empty() {
        return Err(assembly.error(ModelProtocolErrorCode::FinishMismatch));
    }
    let mut tool_calls = Vec::new();
    let mut seen_ids = BTreeSet::new();
    for call in assembly.calls.values() {
        let provider_call_id = call
            .provider_call_id
            .as_ref()
            .ok_or_else(|| assembly.error(ModelProtocolErrorCode::InvalidCallId))?;
        if !seen_ids.insert(provider_call_id) {
            return Err(assembly.error(ModelProtocolErrorCode::DuplicateCallId));
        }
        let name = call
            .name
            .as_ref()
            .ok_or_else(|| assembly.error(ModelProtocolErrorCode::InvalidToolName))?;
        let value = parse_json(&call.arguments)
            .map_err(|_| assembly.error(ModelProtocolErrorCode::InvalidArguments))?;
        let object = value
            .as_object()
            .ok_or_else(|| assembly.error(ModelProtocolErrorCode::InvalidArguments))?;
        let validation = match request.tools.iter().find(|tool| tool.name.as_str() == name) {
            None => ToolCallValidation::UnknownTool,
            Some(tool) => {
                let validator = compile_schema(&tool.model_input_schema)
                    .map_err(|_| assembly.error(ModelProtocolErrorCode::InvalidRequest))?;
                if validator.is_valid(&value) {
                    ToolCallValidation::Valid
                } else {
                    ToolCallValidation::InvalidArguments
                }
            }
        };
        tool_calls.push(ProposedToolCall {
            provider_call_id: Id::new(provider_call_id.clone())
                .map_err(|_| assembly.error(ModelProtocolErrorCode::InvalidCallId))?,
            name: Id::new(name.clone())
                .map_err(|_| assembly.error(ModelProtocolErrorCode::InvalidToolName))?,
            model_inputs: object
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            validation,
        });
    }
    Ok(ModelResponse {
        request_id: request.request_id.clone(),
        route_digest: request.route.digest(),
        text: assembly.text,
        tool_calls,
        finish: *finish,
        metadata: metadata.clone(),
        continuation: continuation.clone(),
    })
}

fn invalid_request(path: &str) -> ContractError {
    ContractError::new(ErrorCode::InvalidContract, path)
}

fn valid_call_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value.chars().any(|c| c.is_whitespace() || c.is_control())
}
fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

struct NoSchemaRetrieval;
impl jsonschema::Retrieve for NoSchemaRetrieval {
    fn retrieve(
        &self,
        _: &jsonschema::Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err("external schema retrieval is disabled".into())
    }
}
fn compile_schema(schema: &Value) -> Result<jsonschema::Validator, ContractError> {
    jsonschema::draft202012::options()
        .should_validate_formats(true)
        .with_retriever(NoSchemaRetrieval)
        .build(schema)
        .map_err(|_| invalid_request("model_request.schema"))
}

struct ByteCounter {
    count: usize,
    maximum: usize,
}
impl io::Write for ByteCounter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.count = self
            .count
            .checked_add(buffer.len())
            .filter(|count| *count <= self.maximum)
            .ok_or_else(|| io::Error::other("serialized value exceeds its limit"))?;
        Ok(buffer.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn json_size(value: &impl Serialize, maximum: usize) -> Result<usize, ()> {
    let mut counter = ByteCounter { count: 0, maximum };
    serde_json::to_writer(&mut counter, value).map_err(|_| ())?;
    Ok(counter.count)
}
fn metadata_bytes(metadata: &ModelResponseMetadata) -> Option<usize> {
    [
        &metadata.provider_request_id,
        &metadata.reported_model_id,
        &metadata.reported_model_version,
    ]
    .into_iter()
    .flatten()
    .try_fold(0_usize, |size, id| size.checked_add(id.as_str().len()))
}
