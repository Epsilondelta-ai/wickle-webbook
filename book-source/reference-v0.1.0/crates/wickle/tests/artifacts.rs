//! Original bytes, immutable provenance and current authorization through real storage calls.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Notify;
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn context() -> ExecutionContext {
    ExecutionContext::new(
        ExecutionContextData {
            scope: Scope {
                tenant_id: id("tenant"),
                workspace_id: id("workspace"),
                user_id: None,
            },
            principal_ref: id("member"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    )
}
fn input(text: &str, version: &str) -> ArtifactInput {
    ArtifactInput {
        media_type: id("text/plain"),
        bytes: text.as_bytes().to_vec(),
        source: Some(VersionedRef {
            id: id("report"),
            version: id(version),
        }),
    }
}
struct Policy(Arc<AtomicBool>);
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        _: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            Ok(if self.0.load(Ordering::SeqCst) {
                PolicyDecision::Deny {
                    reason: id("revoked"),
                }
            } else {
                PolicyDecision::Allow {}
            })
        })
    }
}
struct Store {
    memory: MemoryArtifactStore,
    deny: Arc<AtomicBool>,
    revoke: AtomicBool,
    corrupt: AtomicBool,
    pending: AtomicBool,
    reads: AtomicUsize,
    entered: Notify,
}
impl ArtifactStore for Store {
    fn put<'a>(
        &'a self,
        id: &'a Id,
        input: &'a ArtifactInput,
        ctx: &'a ArtifactCallContext,
    ) -> PortFuture<'a, ArtifactMetadata> {
        self.memory.put(id, input, ctx)
    }
    fn stat<'a>(
        &'a self,
        reference: &'a ArtifactRef,
        ctx: &'a ArtifactCallContext,
    ) -> PortFuture<'a, ArtifactMetadata> {
        self.memory.stat(reference, ctx)
    }
    fn get<'a>(
        &'a self,
        reference: &'a ArtifactRef,
        limit: u64,
        ctx: &'a ArtifactCallContext,
    ) -> PortFuture<'a, ArtifactData> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            if self.pending.load(Ordering::SeqCst) {
                return std::future::pending().await;
            }
            let mut data = self.memory.get(reference, limit, ctx).await?;
            if self.revoke.load(Ordering::SeqCst) {
                self.deny.store(true, Ordering::SeqCst);
            }
            if self.corrupt.load(Ordering::SeqCst) {
                data.bytes[0] ^= 1;
            }
            Ok(data)
        })
    }
}
fn fixture() -> (Arc<ArtifactRuntime>, Arc<Store>, ExecutionContext) {
    let deny = Arc::new(AtomicBool::new(false));
    let store = Arc::new(Store {
        memory: Default::default(),
        deny: deny.clone(),
        revoke: AtomicBool::new(false),
        corrupt: AtomicBool::new(false),
        pending: AtomicBool::new(false),
        reads: AtomicUsize::new(0),
        entered: Notify::new(),
    });
    let policy = Arc::new(PolicyGate::new(Arc::new(Policy(deny)), Duration::from_secs(1)).unwrap());
    let runtime = Arc::new(
        ArtifactRuntime::new(
            store.clone(),
            policy,
            Arc::new(RandomIdSource),
            ArtifactLimits {
                max_bytes: 1024,
                max_preview_bytes: 5,
                timeout_ms: 1000,
            },
        )
        .unwrap(),
    );
    (runtime, store, context())
}

#[tokio::test]
async fn previews_preserve_complete_utf8_originals_and_binary_data_is_not_fabricated_text() {
    let (runtime, _, ctx) = fixture();
    let stored = runtime
        .put(input("가나다🙂", "1"), &ctx, None)
        .await
        .unwrap();
    let preview = runtime
        .preview(&stored.reference, &ctx, None)
        .await
        .unwrap();
    assert_eq!(preview.text.as_deref(), Some("가"));
    assert!(preview.truncated);
    assert_eq!(
        runtime
            .get(&stored.reference, &ctx, None)
            .await
            .unwrap()
            .bytes,
        "가나다🙂".as_bytes()
    );
    let binary = runtime
        .put(
            ArtifactInput {
                media_type: id("application/octet-stream"),
                bytes: vec![0, 255, 128],
                source: None,
            },
            &ctx,
            None,
        )
        .await
        .unwrap();
    let preview = runtime
        .preview(&binary.reference, &ctx, None)
        .await
        .unwrap();
    assert_eq!(preview.text, None);
    assert!(preview.truncated);
    assert_eq!(
        runtime
            .get(&binary.reference, &ctx, None)
            .await
            .unwrap()
            .bytes,
        vec![0, 255, 128]
    );
}

#[tokio::test]
async fn source_updates_do_not_replace_old_bytes_or_evidence_and_reusing_an_id_is_immutable() {
    let (runtime, store, ctx) = fixture();
    let old = runtime
        .put(input("old report", "1"), &ctx, None)
        .await
        .unwrap();
    let proof = runtime
        .evidence(&old.reference, id("line-1"), Some("old".into()), &ctx, None)
        .await
        .unwrap();
    let new = runtime
        .put(input("new report", "2"), &ctx, None)
        .await
        .unwrap();
    assert_ne!(old.reference.artifact_id, new.reference.artifact_id);
    assert_ne!(old.reference.content_hash, new.reference.content_hash);
    assert_eq!(proof.version, id("1"));
    assert_eq!(proof.content_hash, old.reference.content_hash);
    assert_eq!(
        runtime.get(&old.reference, &ctx, None).await.unwrap().bytes,
        b"old report"
    );
    assert!(
        runtime
            .evidence(&old.reference, id("line-1"), Some("new".into()), &ctx, None)
            .await
            .is_err()
    );
    let controls = ArtifactCallContext {
        scope: ctx.data.scope.clone(),
        principal_ref: id("member"),
        capability_grant_ref: id("grant"),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(1),
    };
    let changed = store
        .put(
            &old.reference.artifact_id,
            &input("old report", "2"),
            &controls,
        )
        .await
        .unwrap_err();
    assert_eq!(changed.code, ErrorCode::RequestConflict);
    assert_eq!(runtime.stat(&old.reference, &ctx, None).await.unwrap(), old);
}

#[tokio::test]
async fn foreign_scope_forged_metadata_and_corrupted_content_cannot_return_an_artifact() {
    let (runtime, store, ctx) = fixture();
    let stored = runtime
        .put(input("private report", "1"), &ctx, None)
        .await
        .unwrap();
    let mut other = ctx.clone();
    other.data.scope.workspace_id = id("other");
    assert_eq!(
        runtime
            .get(&stored.reference, &other, None)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
    let mut forged = stored.reference.clone();
    forged.scope = other.data.scope.clone();
    assert_eq!(
        runtime.get(&forged, &other, None).await.unwrap_err().code,
        ErrorCode::StateNotFound
    );
    let mut changed = stored.reference.clone();
    changed.media_type = id("application/json");
    assert!(runtime.get(&changed, &ctx, None).await.is_err());
    store.corrupt.store(true, Ordering::SeqCst);
    assert_eq!(
        runtime
            .get(&stored.reference, &ctx, None)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidArtifact
    );
}

#[tokio::test]
async fn permission_revocation_during_a_read_blocks_its_result_and_later_reads() {
    let (runtime, store, ctx) = fixture();
    let stored = runtime
        .put(input("private report", "1"), &ctx, None)
        .await
        .unwrap();
    store.revoke.store(true, Ordering::SeqCst);
    assert_eq!(
        runtime
            .get(&stored.reference, &ctx, None)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
    assert_eq!(
        runtime
            .get(&stored.reference, &ctx, None)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn oversized_writes_and_cancelled_or_expired_reads_do_not_return_partial_data() {
    let (runtime, store, ctx) = fixture();
    assert!(
        runtime
            .put(input(&"x".repeat(1025), "1"), &ctx, None)
            .await
            .is_err()
    );
    let stored = runtime
        .put(input("private report", "1"), &ctx, None)
        .await
        .unwrap();
    store.pending.store(true, Ordering::SeqCst);
    let cancel = ctx.cancellation.clone();
    let task = tokio::spawn({
        let runtime = runtime.clone();
        let reference = stored.reference.clone();
        let ctx = ctx.clone();
        async move { runtime.get(&reference, &ctx, None).await }
    });
    store.entered.notified().await;
    cancel.cancel();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .code,
        ErrorCode::Cancelled
    );
    let fresh = context();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(10);
    assert_eq!(
        runtime
            .get(&stored.reference, &fresh, Some(deadline))
            .await
            .unwrap_err()
            .code,
        ErrorCode::DeadlineExceeded
    );
}
