//! Atomic execution ownership, commands, rollback and checkpoint validation.
use serde_json::json;
use std::sync::Arc;
use wickle::*;
#[allow(dead_code)]
mod support;
use support::*;
#[path = "support/execution_store.rs"]
mod suite;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_store_atomically_claims_segments_and_consumes_controls() {
    suite::atomic_execution_contract(Arc::new(MemoryStateStore::new())).await;
}
#[tokio::test]
async fn checkpoint_preserves_history_and_rejects_corrupted_execution_identity() {
    let store = MemoryStateStore::new();
    store
        .admit(
            &scope(),
            admission("run", "request", "session", "input", "1").await,
        )
        .await
        .unwrap();
    let checkpoint = store.export_checkpoint(&scope()).unwrap();
    let encoded = serde_json::to_string(&checkpoint).unwrap();
    let restored = MemoryStateStore::from_checkpoint(
        StateStoreCheckpoint::from_json(&encoded, &scope(), &checkpoint.digest()).unwrap(),
    );
    assert_eq!(
        restored.read_execution(&scope(), &id("run")).await.unwrap(),
        store.read_execution(&scope(), &id("run")).await.unwrap()
    );
    let mut corrupt: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    corrupt["executions"][0]["segments"][0]["execution_principal_ref"] = json!("replacement");
    assert!(
        StateStoreCheckpoint::from_json(
            &corrupt.to_string(),
            &scope(),
            &canonical_digest(&corrupt)
        )
        .is_err()
    );
    // An authentic legacy-format graph remains readable, but has no invented actor/segment evidence.
    let mut legacy: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    legacy["schema_version"] = json!("wickle.state-store.v1");
    legacy.as_object_mut().unwrap().remove("executions");
    let restored = MemoryStateStore::from_checkpoint(
        StateStoreCheckpoint::from_json(&legacy.to_string(), &scope(), &canonical_digest(&legacy))
            .unwrap(),
    );
    assert_eq!(
        restored
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .run_id,
        id("run")
    );
    assert_eq!(
        restored
            .read_execution(&scope(), &id("run"))
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
    assert_eq!(
        restored
            .admit(
                &scope(),
                admission("other", "other", "other", "input", "1").await
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
}
#[tokio::test]
async fn recovery_acceptance_and_lease_are_one_transaction() {
    suite::atomic_recovery_contract(Arc::new(MemoryStateStore::new())).await;
}

#[tokio::test]
async fn version_two_rejects_missing_active_history_even_when_other_histories_remain() {
    let store = MemoryStateStore::new();
    for run in ["first", "second"] {
        store
            .admit(&scope(), admission(run, run, run, "input", "1").await)
            .await
            .unwrap();
    }
    let mut image = serde_json::to_value(store.export_checkpoint(&scope()).unwrap()).unwrap();
    image["executions"].as_array_mut().unwrap().remove(0);
    assert!(
        StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(&image))
            .is_err()
    );
}
#[tokio::test]
async fn restored_receipts_must_match_the_original_recovery_command_and_segment() {
    let store = Arc::new(MemoryStateStore::new());
    suite::atomic_recovery_contract(store.clone()).await;
    let image = serde_json::to_value(store.export_checkpoint(&scope()).unwrap()).unwrap();
    for kind in ["missing", "digest", "segment"] {
        let mut changed = image.clone();
        match kind {
            "missing" => changed["executions"][0]["accepted_commands"] = json!([]),
            "digest" => {
                changed["executions"][0]["accepted_commands"][0]["payload_digest"] =
                    json!(canonical_digest(&json!(false)))
            }
            _ => {
                changed["executions"][0]["accepted_commands"][0]["segment_id"] =
                    changed["executions"][0]["segments"][0]["segment_id"].clone()
            }
        }
        assert!(
            StateStoreCheckpoint::from_json(
                &changed.to_string(),
                &scope(),
                &canonical_digest(&changed)
            )
            .is_err(),
            "{kind}"
        );
    }
}

#[tokio::test]
async fn version_two_cannot_silently_reclassify_new_terminal_runs_as_legacy() {
    let store = Arc::new(MemoryStateStore::new());
    suite::atomic_execution_contract(store.clone()).await;
    store
        .admit(
            &scope(),
            admission("other", "other", "other", "input", "1").await,
        )
        .await
        .unwrap();
    let mut image = serde_json::to_value(store.export_checkpoint(&scope()).unwrap()).unwrap();
    let histories = image["executions"].as_array_mut().unwrap();
    let index = histories
        .iter()
        .position(|v| v["run_id"] == "atomic-run")
        .unwrap();
    histories.remove(index);
    assert!(
        StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(&image))
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_conflicting_submissions_keep_only_the_winning_request_snapshot() {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
    suite::conflicting_submissions_race([store.clone(), store]).await;
}
