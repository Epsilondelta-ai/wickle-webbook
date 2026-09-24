//! Scoped immutable artifacts and bounded, currently authorized reads.

use crate::*;
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fmt,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Immutable artifact identity and Host-attested original source revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactMetadata {
    /// Exact owning namespace, byte hash, media type, and size.
    pub reference: ArtifactRef,
    /// Source revision asserted by the trusted publisher, never an implicit latest alias.
    pub source: Option<VersionedRef>,
}
/// Bytes and publication metadata. Contents must not enter ordinary logs.
#[derive(Clone)]
pub struct ArtifactInput {
    /// MIME type of the original bytes.
    pub media_type: Id,
    /// Complete original bytes, never a preview substituted for the source.
    pub bytes: Vec<u8>,
    /// Optional original source and revision attested by the Host publisher.
    pub source: Option<VersionedRef>,
}
impl fmt::Debug for ArtifactInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ArtifactInput(<protected>)")
    }
}
/// Complete data returned by a store after a bounded read.
#[derive(Clone)]
pub struct ArtifactData {
    /// Authoritative metadata for these bytes.
    pub metadata: ArtifactMetadata,
    /// Complete original bytes.
    pub bytes: Vec<u8>,
}
impl fmt::Debug for ArtifactData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ArtifactData(<protected>)")
    }
}
/// Current principal and cancellation controls for one storage operation.
#[derive(Debug, Clone)]
pub struct ArtifactCallContext {
    /// Authenticated owning namespace.
    pub scope: Scope,
    /// Current actor.
    pub principal_ref: Id,
    /// Current Host grant.
    pub capability_grant_ref: Id,
    /// Cooperative operation cancellation.
    pub cancellation: CancellationToken,
    /// Finite operation deadline.
    pub deadline: Instant,
}
/// Immutable storage port. Retention, deletion, and persistence belong to the Host.
pub trait ArtifactStore: Send + Sync {
    /// Publish under a Host-generated ID. Reusing an ID with different data must fail.
    fn put<'a>(
        &'a self,
        artifact_id: &'a Id,
        input: &'a ArtifactInput,
        context: &'a ArtifactCallContext,
    ) -> PortFuture<'a, ArtifactMetadata>;
    /// Look up the exact reference, never silently return a newer revision.
    fn stat<'a>(
        &'a self,
        reference: &'a ArtifactRef,
        context: &'a ArtifactCallContext,
    ) -> PortFuture<'a, ArtifactMetadata>;
    /// Return complete bytes only if they fit the explicit byte limit.
    fn get<'a>(
        &'a self,
        reference: &'a ArtifactRef,
        max_bytes: u64,
        context: &'a ArtifactCallContext,
    ) -> PortFuture<'a, ArtifactData>;
}
/// Finite storage and display bounds; bytes are not model-token estimates.
#[derive(Debug, Clone, Copy)]
pub struct ArtifactLimits {
    /// Largest accepted complete artifact.
    pub max_bytes: u64,
    /// Largest UTF-8 preview, in bytes.
    pub max_preview_bytes: usize,
    /// Maximum time for one authorized operation.
    pub timeout_ms: u64,
}
impl Default for ArtifactLimits {
    fn default() -> Self {
        Self {
            max_bytes: 16 * 1024 * 1024,
            max_preview_bytes: 4096,
            timeout_ms: 30_000,
        }
    }
}
impl ArtifactLimits {
    fn validate(self) -> Result<(), ContractError> {
        if self.max_bytes == 0
            || self.max_bytes > 64 * 1024 * 1024
            || self.max_preview_bytes == 0
            || self.max_preview_bytes > 64 * 1024
            || self.max_preview_bytes as u64 > self.max_bytes
            || self.timeout_ms == 0
            || self.timeout_ms > 86_400_000
        {
            return Err(err(ErrorCode::InvalidConfiguration, "artifact.limits"));
        }
        Ok(())
    }
}
/// A display preview, explicitly separate from the immutable original.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactPreview {
    /// Exact original artifact metadata.
    pub reference: ArtifactRef,
    /// None for binary media. UTF-8 text previews end on a character boundary.
    pub text: Option<String>,
    /// Whether some original bytes are absent from this preview.
    pub truncated: bool,
}
/// Core validation and policy wrapper around an existing Host store.
pub struct ArtifactRuntime {
    store: Arc<dyn ArtifactStore>,
    policy: Arc<PolicyGate>,
    ids: Arc<dyn IdSource>,
    limits: ArtifactLimits,
}
impl ArtifactRuntime {
    /// Validate typed observations against current storage metadata and source evidence.
    /// Evidence must be accompanied by its immutable artifact reference.
    pub async fn validate_content(
        &self,
        content: &[InputContent],
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<Vec<ArtifactRef>, ContractError> {
        let mut references = vec![];
        let mut metadata = vec![];
        for item in content {
            if let InputContent::Artifact { reference } = item {
                metadata.push(self.stat(reference, context, deadline).await?);
                if !references.contains(reference) {
                    references.push(reference.clone());
                }
            }
        }
        for item in content {
            if let InputContent::Evidence {
                reference: evidence,
            } = item
            {
                let mut matched = false;
                for reference in references.iter().filter(|reference| {
                    reference.content_hash == evidence.content_hash
                        && metadata.iter().any(|metadata| {
                            metadata.reference == **reference
                                && metadata.source.as_ref().is_some_and(|source| {
                                    source.id == evidence.source_id
                                        && source.version == evidence.version
                                })
                        })
                }) {
                    let actual = self
                        .evidence(
                            reference,
                            evidence.location.clone(),
                            evidence.quote.clone(),
                            context,
                            deadline,
                        )
                        .await?;
                    if actual == *evidence {
                        matched = true;
                        break;
                    }
                }
                if !matched {
                    return Err(err(ErrorCode::InvalidArtifact, "artifact.evidence"));
                }
            }
        }
        Ok(references)
    }
    /// Construct without invoking storage, identity, or policy callbacks.
    pub fn new(
        store: Arc<dyn ArtifactStore>,
        policy: Arc<PolicyGate>,
        ids: Arc<dyn IdSource>,
        limits: ArtifactLimits,
    ) -> Result<Self, ContractError> {
        limits.validate()?;
        Ok(Self {
            store,
            policy,
            ids,
            limits,
        })
    }
    fn controls(
        &self,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> ArtifactCallContext {
        let limit = Instant::now() + Duration::from_millis(self.limits.timeout_ms);
        ArtifactCallContext {
            scope: context.data.scope.clone(),
            principal_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
            cancellation: context.cancellation.child_token(),
            deadline: deadline.map_or(limit, |d| d.min(limit)),
        }
    }
    async fn authorize(
        &self,
        id: &Id,
        write: bool,
        context: &ExecutionContext,
        controls: &ArtifactCallContext,
    ) -> Result<(), ContractError> {
        let request = PolicyRequest {
            owner_scope: controls.scope.clone(),
            resource_id: id.clone(),
            action: if write {
                PolicyAction::WriteArtifact {}
            } else {
                PolicyAction::ReadArtifact {}
            },
        };
        match self
            .policy
            .check(&request, context, Some(controls.deadline), None)
            .await?
        {
            PolicyDecision::Allow {} => Ok(()),
            PolicyDecision::RequireApproval { .. } => {
                Err(err(ErrorCode::ArtifactApprovalRequired, "artifact.policy"))
            }
            PolicyDecision::Deny { .. } => Err(err(ErrorCode::AccessDenied, "artifact.policy")),
        }
    }
    /// Store complete bytes under a new system-generated immutable identity.
    pub async fn put(
        &self,
        input: ArtifactInput,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<ArtifactMetadata, ContractError> {
        validate_input(&input, self.limits.max_bytes)?;
        let id = self.ids.next_id()?;
        self.put_named(id, input, context, deadline).await
    }
    /// Core-generated immutable identity for idempotent context-preview publication.
    pub(crate) async fn put_named(
        &self,
        id: Id,
        input: ArtifactInput,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<ArtifactMetadata, ContractError> {
        validate_input(&input, self.limits.max_bytes)?;
        let controls = self.controls(context, deadline);
        let _cancel = controls.cancellation.clone().drop_guard();
        self.authorize(&id, true, context, &controls).await?;
        let expected = metadata(&id, &input, &controls.scope)?;
        let actual = bounded(&controls, async {
            self.store.put(&id, &input, &controls).await
        })
        .await?;
        if actual != expected {
            return Err(err(ErrorCode::InvalidArtifact, "artifact.put_metadata"));
        }
        Ok(actual)
    }
    /// Read authoritative metadata after checking the current namespace and policy.
    pub async fn stat(
        &self,
        reference: &ArtifactRef,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<ArtifactMetadata, ContractError> {
        check_scope(reference, &context.data.scope)?;
        validate_reference(reference, reference, self.limits.max_bytes)?;
        let controls = self.controls(context, deadline);
        let _cancel = controls.cancellation.clone().drop_guard();
        self.authorize(&reference.artifact_id, false, context, &controls)
            .await?;
        let actual = bounded(&controls, async {
            self.store.stat(reference, &controls).await
        })
        .await?;
        validate_reference(reference, &actual.reference, self.limits.max_bytes)?;
        self.authorize(&reference.artifact_id, false, context, &controls)
            .await?;
        Ok(actual)
    }
    /// Read complete bytes and verify the original size, hash, media type and scope.
    pub async fn get(
        &self,
        reference: &ArtifactRef,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<ArtifactData, ContractError> {
        check_scope(reference, &context.data.scope)?;
        validate_reference(reference, reference, self.limits.max_bytes)?;
        let controls = self.controls(context, deadline);
        let _cancel = controls.cancellation.clone().drop_guard();
        self.authorize(&reference.artifact_id, false, context, &controls)
            .await?;
        let data = bounded(&controls, async {
            self.store
                .get(reference, self.limits.max_bytes, &controls)
                .await
        })
        .await?;
        validate_reference(reference, &data.metadata.reference, self.limits.max_bytes)?;
        if data.bytes.len() as u64 != reference.size_bytes
            || content_hash(&data.bytes)? != reference.content_hash
        {
            return Err(err(ErrorCode::InvalidArtifact, "artifact.content"));
        }
        self.authorize(&reference.artifact_id, false, context, &controls)
            .await?;
        Ok(data)
    }
    /// Produce a bounded UTF-8 display preview; this never changes stored bytes.
    pub async fn preview(
        &self,
        reference: &ArtifactRef,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<ArtifactPreview, ContractError> {
        let data = self.get(reference, context, deadline).await?;
        let textual = reference.media_type.as_str().starts_with("text/")
            || reference.media_type.as_str() == "application/json";
        let text = if textual {
            let text = std::str::from_utf8(&data.bytes)
                .map_err(|_| err(ErrorCode::InvalidArtifact, "artifact.utf8"))?;
            let mut end = self.limits.max_preview_bytes.min(text.len());
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            Some(text[..end].to_owned())
        } else {
            None
        };
        let truncated = text
            .as_ref()
            .map_or(!data.bytes.is_empty(), |text| text.len() < data.bytes.len());
        Ok(ArtifactPreview {
            reference: reference.clone(),
            text,
            truncated,
        })
    }
    /// Create evidence from the stored Host-attested source revision and actual bytes.
    /// The Host is responsible for the semantic meaning of a source-specific location.
    pub async fn evidence(
        &self,
        reference: &ArtifactRef,
        location: Id,
        quote: Option<String>,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<EvidenceRef, ContractError> {
        let data = self.get(reference, context, deadline).await?;
        let source = data
            .metadata
            .source
            .ok_or_else(|| err(ErrorCode::InvalidArtifact, "artifact.source"))?;
        if let Some(quote) = &quote {
            if quote.is_empty()
                || quote.len() > self.limits.max_preview_bytes
                || !std::str::from_utf8(&data.bytes).is_ok_and(|body| body.contains(quote))
            {
                return Err(err(ErrorCode::InvalidArtifact, "artifact.quote"));
            }
        }
        Ok(EvidenceRef {
            source_id: source.id,
            version: source.version,
            location,
            content_hash: reference.content_hash.clone(),
            quote,
        })
    }
}
/// In-process reference implementation. It does not claim durable storage.
#[derive(Default)]
pub struct MemoryArtifactStore {
    entries: Mutex<BTreeMap<ArtifactKey, ArtifactData>>,
}
type ArtifactKey = (Id, Id, Option<Id>, Id);
impl ArtifactStore for MemoryArtifactStore {
    fn put<'a>(
        &'a self,
        id: &'a Id,
        input: &'a ArtifactInput,
        context: &'a ArtifactCallContext,
    ) -> PortFuture<'a, ArtifactMetadata> {
        Box::pin(async move {
            check_controls(context)?;
            validate_input(input, 64 * 1024 * 1024)?;
            let metadata = metadata(id, input, &context.scope)?;
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| err(ErrorCode::PersistenceUnavailable, "artifact.store"))?;
            let key = key(&context.scope, id);
            if let Some(prior) = entries.get(&key) {
                if prior.metadata != metadata || prior.bytes != input.bytes {
                    return Err(err(ErrorCode::RequestConflict, "artifact.immutable"));
                }
                return Ok(prior.metadata.clone());
            }
            entries.insert(
                key,
                ArtifactData {
                    metadata: metadata.clone(),
                    bytes: input.bytes.clone(),
                },
            );
            Ok(metadata)
        })
    }
    fn stat<'a>(
        &'a self,
        reference: &'a ArtifactRef,
        context: &'a ArtifactCallContext,
    ) -> PortFuture<'a, ArtifactMetadata> {
        Box::pin(async move {
            check_controls(context)?;
            check_scope(reference, &context.scope)?;
            let entries = self
                .entries
                .lock()
                .map_err(|_| err(ErrorCode::PersistenceUnavailable, "artifact.store"))?;
            let data = entries
                .get(&key(&context.scope, &reference.artifact_id))
                .ok_or_else(|| err(ErrorCode::StateNotFound, "artifact.id"))?;
            validate_reference(reference, &data.metadata.reference, 64 * 1024 * 1024)?;
            Ok(data.metadata.clone())
        })
    }
    fn get<'a>(
        &'a self,
        reference: &'a ArtifactRef,
        max_bytes: u64,
        context: &'a ArtifactCallContext,
    ) -> PortFuture<'a, ArtifactData> {
        Box::pin(async move {
            check_controls(context)?;
            check_scope(reference, &context.scope)?;
            let entries = self
                .entries
                .lock()
                .map_err(|_| err(ErrorCode::PersistenceUnavailable, "artifact.store"))?;
            let data = entries
                .get(&key(&context.scope, &reference.artifact_id))
                .ok_or_else(|| err(ErrorCode::StateNotFound, "artifact.id"))?;
            validate_reference(reference, &data.metadata.reference, max_bytes)?;
            Ok(data.clone())
        })
    }
}
pub(crate) fn content_hash(bytes: &[u8]) -> Result<Id, ContractError> {
    let hex: String = Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Id::new(format!("sha256:{hex}"))
}
fn validate_input(input: &ArtifactInput, max_bytes: u64) -> Result<(), ContractError> {
    if input.bytes.len() as u64 > max_bytes || !input.media_type.as_str().contains('/') {
        return Err(err(ErrorCode::InvalidArtifact, "artifact.input"));
    }
    Ok(())
}
fn metadata(
    id: &Id,
    input: &ArtifactInput,
    scope: &Scope,
) -> Result<ArtifactMetadata, ContractError> {
    Ok(ArtifactMetadata {
        reference: ArtifactRef {
            artifact_id: id.clone(),
            scope: scope.clone(),
            media_type: input.media_type.clone(),
            size_bytes: input.bytes.len() as u64,
            content_hash: content_hash(&input.bytes)?,
        },
        source: input.source.clone(),
    })
}
fn validate_reference(
    expected: &ArtifactRef,
    actual: &ArtifactRef,
    max_bytes: u64,
) -> Result<(), ContractError> {
    if expected != actual
        || actual.size_bytes > max_bytes
        || !actual.media_type.as_str().contains('/')
    {
        return Err(err(ErrorCode::InvalidArtifact, "artifact.reference"));
    }
    Ok(())
}
fn key(scope: &Scope, id: &Id) -> (Id, Id, Option<Id>, Id) {
    (
        scope.tenant_id.clone(),
        scope.workspace_id.clone(),
        scope.user_id.clone(),
        id.clone(),
    )
}
fn check_scope(reference: &ArtifactRef, scope: &Scope) -> Result<(), ContractError> {
    if &reference.scope != scope {
        return Err(err(ErrorCode::AccessDenied, "artifact.scope"));
    }
    Ok(())
}
fn check_controls(context: &ArtifactCallContext) -> Result<(), ContractError> {
    if context.cancellation.is_cancelled() {
        return Err(err(ErrorCode::Cancelled, "artifact.operation"));
    }
    if Instant::now() >= context.deadline {
        return Err(err(ErrorCode::DeadlineExceeded, "artifact.operation"));
    }
    Ok(())
}
async fn bounded<T>(
    context: &ArtifactCallContext,
    future: impl std::future::Future<Output = Result<T, ContractError>>,
) -> Result<T, ContractError> {
    let result = tokio::select! { biased;
        _ = context.cancellation.cancelled() => Err(err(ErrorCode::Cancelled, "artifact.operation")),
        _ = tokio::time::sleep_until(context.deadline) => Err(err(ErrorCode::DeadlineExceeded, "artifact.operation")),
        result = AssertUnwindSafe(future).catch_unwind() => result.unwrap_or_else(|_| Err(err(ErrorCode::InvalidArtifact, "artifact.operation"))),
    };
    check_controls(context)?;
    result.map_err(|error| err(error.code, "artifact.operation"))
}
fn err(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
