//! Bounded context collection, immutable batches and current source authorization.

use crate::*;
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};
use tokio_util::sync::CancellationToken;

mod records;
mod runtime;

/// Immutable contract for a read-only source of retrieved data or memory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSourceDefinition {
    /// Native source identity and exact version.
    pub source: VersionedRef,
    /// Only Retrieval and Memory are valid automatic-source origins.
    pub origin: ContextOrigin,
    /// Supported source contract version, currently 1.
    pub contract_version: u32,
}
impl ContextSourceDefinition {
    /// Complete definition identity.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
    /// Reject privileged origins or unknown contracts.
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.contract_version != 1 {
            return Err(source_error(
                ErrorCode::UnsupportedContractVersion,
                "context_source.definition",
            ));
        }
        if !matches!(
            self.origin,
            ContextOrigin::Retrieval | ContextOrigin::Memory
        ) {
            return Err(source_error(
                ErrorCode::InvalidContract,
                "context_source.origin",
            ));
        }
        Ok(())
    }
}
/// A source selection and immutable definition, resolved without opening it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedSourceBinding {
    /// Original source, trigger, limits and failure requirement.
    pub binding: ContextSourceBinding,
    /// Exact read-only source contract.
    pub definition: ContextSourceDefinition,
    /// Catalog metadata; exports retain this in their adapter definition.
    pub metadata: Option<ComponentMetadata>,
}
/// Host-owned token estimation, independent of untrusted provider usage reports.
pub trait ContextTokenEstimator: Send + Sync {
    /// Immutable algorithm/tokenizer identity; cached when the runtime is built.
    fn version(&self) -> VersionedRef;
    /// Estimate the actual validated, namespaced context data, separately from bytes.
    fn estimate(&self, items: &[ContextItem]) -> Result<u64, ContractError>;
}
/// Provider-reported usage, retained as reported rather than charged as trusted
/// model tokens. None means unreported, not zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSourceUsage {
    /// Provider-reported service requests.
    pub requests: Option<u64>,
    /// Provider-reported service tokens, if applicable.
    pub tokens: Option<u64>,
}
/// Read-only source reply. Empty and unavailable never carry stale items.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextResult {
    /// One or more original, locally identified items.
    Ready {
        /// Items whose original claims must validate before namespacing.
        items: Vec<ContextItem>,
        /// Optional source data revision.
        source_revision: Option<Id>,
        /// Optional reported external-service usage.
        reported_usage: Option<ContextSourceUsage>,
    },
    /// A successful query returned no items.
    Empty {
        /// Optional source data revision.
        source_revision: Option<Id>,
        /// Optional reported external-service usage.
        reported_usage: Option<ContextSourceUsage>,
    },
    /// Explicit operational unavailability; this is not a permission override.
    Unavailable {
        /// Safe classified service failure code.
        code: Id,
        /// Optional observed source revision.
        source_revision: Option<Id>,
        /// Optional reported external-service usage.
        reported_usage: Option<ContextSourceUsage>,
    },
}
impl fmt::Debug for ContextResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ContextResult(<protected>)")
    }
}
impl ContextResult {
    /// Original local items used for source-side authorization checks.
    pub fn items(&self) -> &[ContextItem] {
        if let Self::Ready { items, .. } = self {
            items
        } else {
            &[]
        }
    }
    /// Optional source revision; absence is not inferred from a requested version.
    pub fn source_revision(&self) -> Option<&Id> {
        match self {
            Self::Ready {
                source_revision, ..
            }
            | Self::Empty {
                source_revision, ..
            }
            | Self::Unavailable {
                source_revision, ..
            } => source_revision.as_ref(),
        }
    }
}
/// Stable logical query identity; transport retry never invents a new source query.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextRequest {
    /// Deterministic identity over the fixed Run, source, trigger and input.
    pub context_request_id: Id,
    /// Exact namespace.
    pub scope: Scope,
    /// Owning Run.
    pub run_id: Id,
    /// Owning session.
    pub session_id: Id,
    /// Original source selection and finite bounds.
    pub binding: ContextSourceBinding,
    /// Pinned source version and allowed origin.
    pub definition: ContextSourceDefinition,
    /// Present exactly for before_model queries.
    pub model_step_id: Option<Id>,
    /// Original user content only; never the whole transcript or SystemInputs.
    pub user_input: Vec<InputContent>,
}
impl fmt::Debug for ContextRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContextRequest")
            .field("context_request_id", &self.context_request_id)
            .finish_non_exhaustive()
    }
}
impl ContextRequest {
    /// Digest of the original query data.
    pub fn input_digest(&self) -> JsonDigest {
        crate::serialization::data_digest(&self.user_input)
    }
}
/// Current identity and controls, independently renewed for every use check.
#[derive(Debug, Clone)]
pub struct ContextCallContext {
    /// Scope checked before the callback.
    pub scope: Scope,
    /// Owning Run.
    pub run_id: Id,
    /// Owning session.
    pub session_id: Id,
    /// Logical source selection, including adapter binding when applicable.
    pub source: ContextSourceRef,
    /// Original logical query.
    pub context_request_id: Id,
    /// Current process-local adapter segment, absent for direct sources.
    pub binding_set_id: Option<Id>,
    /// Current authenticated actor.
    pub principal_ref: Id,
    /// Current Host authorization grant.
    pub capability_grant_ref: Id,
    /// Cooperative callback cancellation.
    pub cancellation: CancellationToken,
    /// Effective finite callback deadline.
    pub deadline: tokio::time::Instant,
}
/// Original source-local identities and content for current source ACL checks.
#[derive(Clone)]
pub struct ContextUseRequest {
    /// Exact immutable batch used in this projection.
    pub batch_ref: RecordRef,
    /// Original query identity and pinned source contract.
    pub request: ContextRequest,
    /// Original provider items, not the derived core namespace.
    pub items: Vec<ContextItem>,
    /// Optional provider data revision.
    pub source_revision: Option<Id>,
    /// Exact model destination; None is a local preparation/use check.
    pub route: Option<ResolvedModelRoute>,
}
impl fmt::Debug for ContextUseRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ContextUseRequest(<protected>)")
    }
}
/// A trusted read-only provider. It must not hide writes, subscriptions or retries.
pub trait ContextSource: Send + Sync {
    /// Fetch once for a logical source query; partial late replies are not adopted.
    fn provide<'a>(
        &'a self,
        request: &'a ContextRequest,
        context: &'a ContextCallContext,
    ) -> PortFuture<'a, ContextResult>;
    /// Check current source-side ownership/ACL of the exact cached data. Cannot
    /// replace items or implicitly fetch a new revision.
    fn authorize_use<'a>(
        &'a self,
        request: &'a ContextUseRequest,
        context: &'a ContextCallContext,
    ) -> PortFuture<'a, ()>;
}
/// Existing source implementation associated with one actual selection.
#[derive(Clone)]
pub struct ContextSourceRegistration {
    /// Native catalog or adapter export identity.
    pub selection: ContextSourceRef,
    /// Immutable source contract.
    pub definition: ContextSourceDefinition,
    /// Existing scoped read-only provider.
    pub source: Arc<dyn ContextSource>,
}
/// One source's profile bounds and pinned definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedContextSource {
    /// Exact profile binding and limits.
    pub binding: ContextSourceBinding,
    /// Immutable provider definition.
    pub definition: ContextSourceDefinition,
}
/// Immutable scope-local source registry; creating it invokes no source callback.
pub struct ContextSourceRegistry {
    scope: Scope,
    entries: Vec<ContextSourceRegistration>,
}
/// Admission-pinned source order, limits, definitions and estimator algorithm.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSourcePlan {
    scope: Scope,
    bindings: Vec<PlannedContextSource>,
    estimator_version: VersionedRef,
}
/// Protected logical source result, including original provenance and the derived
/// model-facing namespace. Only validated batches may be used or restored.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextBatch {
    schema_version: String,
    batch_id: Id,
    request: ContextRequest,
    result: ContextResult,
    items: Vec<ContextItem>,
    collected_at_ms: i64,
    estimated_tokens: u64,
    estimator_version: VersionedRef,
}
impl fmt::Debug for ContextBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContextBatch")
            .field("batch_id", &self.batch_id)
            .field("item_count", &self.items.len())
            .finish_non_exhaustive()
    }
}
/// Core-owned collection, storage and current data-use authorization.
pub struct ContextSourceRuntime {
    store: Arc<dyn StateStore>,
    policy: Arc<PolicyGate>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
    registry: Arc<ContextSourceRegistry>,
    estimator: Arc<dyn ContextTokenEstimator>,
    estimator_version: VersionedRef,
    binding_set_id: Option<Id>,
}
fn source_error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
