//! Validated context selection and cumulative, separately stored projections.

use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt, sync::Arc};
use tokio_util::sync::CancellationToken;

mod compression;
mod engine;
mod model_compactor;
mod operations;
pub(crate) mod records;
mod runtime;
pub(crate) use engine::ContextServices;

/// Versioned contract for a read-only context selector.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextStrategyDefinition {
    /// Exact algorithm identity.
    pub strategy: VersionedRef,
    /// Schema for nonsecret profile context configuration.
    pub config_schema: Value,
}
/// One complete eligible conversation segment. Its IDs cannot be split on adoption.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSegment {
    /// Original messages in order, never including protected user/control messages.
    pub message_ids: Vec<Id>,
    /// Bounded model-visible data, excluding opaque replay and private receipts.
    pub content: Value,
}
/// Read-only selection input; it exposes no store, mutable Run, or credentials.
#[derive(Clone)]
pub struct ContextSelectionInput {
    /// Owning namespace.
    pub scope: Scope,
    /// Current Run.
    pub run_id: Id,
    /// Fixed nonsecret context configuration.
    pub config: JsonObject,
    /// Complete eligible segments, oldest first.
    pub segments: Vec<ContextSegment>,
    /// Whether an existing cumulative summary can also be reduced.
    pub has_summary: bool,
    /// Maximum serialized selected data for a compactor request.
    pub max_input_bytes: usize,
}
impl fmt::Debug for ContextSelectionInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ContextSelectionInput(<protected>)")
    }
}
/// Bounded current identity for selection and external pure compression callbacks.
#[derive(Debug, Clone)]
pub struct ContextStrategyContext {
    /// Current authenticated actor.
    pub principal_ref: Id,
    /// Current Host grant.
    pub capability_grant_ref: Id,
    /// Cooperative cancellation.
    pub cancellation: CancellationToken,
    /// Finite effective deadline.
    pub deadline: tokio::time::Instant,
}
/// Select complete eligible groups; the core validates every returned identity.
pub trait ContextStrategy: Send + Sync {
    /// Pure metadata, cached when a ContextRuntime is constructed.
    fn definition(&self) -> ContextStrategyDefinition;
    /// Return original message IDs to summarize, without changing their contents.
    fn select<'a>(
        &'a self,
        input: &'a ContextSelectionInput,
        context: &'a ContextStrategyContext,
    ) -> PortFuture<'a, Vec<Id>>;
}
/// Select the oldest complete eligible segments within the candidate byte limit.
#[derive(Default)]
pub struct BoundedContextStrategy;
impl ContextStrategy for BoundedContextStrategy {
    fn definition(&self) -> ContextStrategyDefinition {
        ContextStrategyDefinition {
            strategy: VersionedRef {
                id: Id::new("bounded").expect("constant ID"),
                version: Id::new("1").expect("constant ID"),
            },
            config_schema: serde_json::json!({"type":"object","additionalProperties":false}),
        }
    }
    fn select<'a>(
        &'a self,
        input: &'a ContextSelectionInput,
        _: &'a ContextStrategyContext,
    ) -> PortFuture<'a, Vec<Id>> {
        Box::pin(async move {
            let mut bytes = 0usize;
            let mut ids = vec![];
            for segment in &input.segments {
                let size = serde_json::to_vec(&segment.content)
                    .map_err(|_| context_error(ErrorCode::InvalidJson, "context.segment"))?
                    .len();
                if bytes.saturating_add(size) > input.max_input_bytes {
                    break;
                }
                bytes += size;
                ids.extend(segment.message_ids.clone());
            }
            Ok(ids)
        })
    }
}
/// Exact source data supplied to a compressor. The result is only summary text.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionRequest {
    /// Stable identity over source data, not a transient snapshot revision.
    pub request_id: Id,
    /// Owning namespace.
    pub scope: Scope,
    /// Current Run.
    pub run_id: Id,
    /// Original current request and constraints, retained separately by the core.
    pub current_input: Vec<InputContent>,
    /// Previous cumulative summary, if any.
    pub previous_summary: Option<String>,
    /// Complete selected model-visible segments.
    pub segments: Vec<ContextSegment>,
}
impl fmt::Debug for CompactionRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CompactionRequest(<protected>)")
    }
}
/// A pure Host compressor. Model-backed compression uses the Model variant instead.
pub trait HostContextCompactor: Send + Sync {
    /// Return a complete bounded summary; no hidden model requests or business writes.
    fn compact<'a>(
        &'a self,
        request: &'a CompactionRequest,
        context: &'a ContextStrategyContext,
    ) -> PortFuture<'a, String>;
}
/// Routing settings for a core-owned, budgeted model compression request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCompactorConfig {
    /// Logical binding with an explicit Compaction routing rule.
    pub model_binding: Id,
    /// Optional Host override; None inherits the admitted Run's model options.
    pub options: Option<JsonObject>,
    /// Reserved output tokens for the summary.
    pub max_output_tokens: std::num::NonZeroU64,
}
/// An approved model route or pure Host compressor, never an arbitrary executable path.
#[derive(Clone)]
pub enum ContextCompactor {
    /// Runs through the Agent's ModelExchange/Router and the same Run budget.
    Model(ModelCompactorConfig),
    /// Read-only, non-model algorithm owned by the Host.
    Host {
        /// Immutable implementation/configuration identity.
        definition: VersionedRef,
        /// Already-created approved implementation.
        compressor: Arc<dyn HostContextCompactor>,
    },
}
/// Finite preparation, preview, compression and callback bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextRewriteLimits {
    /// Upper bound for an intermediate local model projection.
    pub max_prepared_bytes: usize,
    /// Maximum source data sent to a compressor.
    pub max_compactor_input_bytes: usize,
    /// Maximum UTF-8 summary bytes; oversize results fail rather than truncate.
    pub max_summary_bytes: usize,
    /// Preview large text/JSON Tool observations above this threshold.
    pub preview_above_bytes: usize,
    /// Maximum new previews per preparation.
    pub max_previews: usize,
    /// Maximum compression decisions per Run.
    pub max_compactions: u64,
    /// Finite selection/Host compression timeout.
    pub timeout_ms: u64,
}
impl Default for ContextRewriteLimits {
    fn default() -> Self {
        Self {
            max_prepared_bytes: 16 * 1024 * 1024,
            max_compactor_input_bytes: 1024 * 1024,
            max_summary_bytes: 16 * 1024,
            preview_above_bytes: 16 * 1024,
            max_previews: 16,
            max_compactions: 8,
            timeout_ms: 30_000,
        }
    }
}
/// Core-owned configuration and validation around an approved selector/compressor.
pub struct ContextRuntime {
    scope: Scope,
    definition: ContextStrategyDefinition,
    strategy: Arc<dyn ContextStrategy>,
    compactor: Option<ContextCompactor>,
    limits: ContextRewriteLimits,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CompactorIdentity {
    Model { config: ModelCompactorConfig },
    Host { definition: VersionedRef },
}
/// Immutable configuration selected for one admitted Run.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextPlan {
    pub(crate) schema_version: String,
    pub(crate) scope: Scope,
    pub(crate) policy: ContextPolicy,
    pub(crate) strategy: ContextStrategyDefinition,
    pub(crate) compactor: Option<CompactorIdentity>,
    pub(crate) limits: ContextRewriteLimits,
}
/// One separately stored preview of an original text/JSON Tool observation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextPreview {
    /// Original Tool observation message.
    pub message_id: Id,
    /// Original call whose observation owns this content index.
    pub tool_call_id: Id,
    /// Original index in ToolResult.content.
    pub content_index: usize,
    /// Digest of the exact original InputContent.
    pub original_digest: JsonDigest,
    /// Original bytes and bounded display text, never a replacement in StateStore.
    pub preview: ArtifactPreview,
}
/// A cumulative, validated model-context revision, separate from original messages.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextRevision {
    pub(crate) schema_version: String,
    pub(crate) scope: Scope,
    pub(crate) session_id: Id,
    pub(crate) run_id: Id,
    pub(crate) model_step_id: Id,
    pub(crate) parent: Option<RecordRef>,
    pub(crate) plan_ref: RecordRef,
    pub(crate) through_sequence: u64,
    pub(crate) covered_message_ids: Vec<Id>,
    pub(crate) covered_digest: JsonDigest,
    pub(crate) summary: Option<String>,
    pub(crate) anchors: Vec<InputContent>,
    pub(crate) previews: Vec<ContextPreview>,
    pub(crate) before_bytes: u64,
    pub(crate) after_bytes: u64,
    pub(crate) before_tokens: u64,
    pub(crate) after_tokens: u64,
}
/// Durable rejection/application evidence for one logical compression input.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextDecision {
    pub(crate) schema_version: String,
    pub(crate) scope: Scope,
    pub(crate) run_id: Id,
    pub(crate) model_step_id: Id,
    pub(crate) request_id: Id,
    pub(crate) request_digest: JsonDigest,
    pub(crate) request: CompactionRequest,
    pub(crate) source_revision_ref: Option<RecordRef>,
    pub(crate) through_sequence: u64,
    pub(crate) previews: Vec<ContextPreview>,
    pub(crate) revision_ref: Option<RecordRef>,
    pub(crate) failure: Option<ErrorCode>,
}
fn context_error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
impl fmt::Debug for ContextRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ContextRevision(<protected>)")
    }
}
impl fmt::Debug for ContextPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ContextPlan(<protected>)")
    }
}
impl fmt::Debug for ContextDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ContextDecision(<protected>)")
    }
}
