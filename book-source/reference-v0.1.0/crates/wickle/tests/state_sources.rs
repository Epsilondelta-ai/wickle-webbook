//! Real source generations seed corruption/transition tests; original provider
//! replies and their cryptographic records stay intact in the rollback cases.

#[path = "support/agent.rs"]
#[allow(dead_code)]
mod agent_support;
#[path = "support/agent_hooks.rs"]
#[allow(dead_code)]
mod hooks_support;
#[path = "support/agent_resume.rs"]
#[allow(dead_code)]
mod resume_support;
#[path = "support/context_sources.rs"]
#[allow(dead_code, unused_imports)]
mod support;

use serde_json::{Value, json};
use std::sync::atomic::Ordering;
use support::*;
use wickle::*;

fn restore(image: &Value) -> Result<MemoryStateStore, ContractError> {
    StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(image))
        .map(MemoryStateStore::from_checkpoint)
}
fn first_slot(image: &Value) -> SourceExecutionState {
    let reference = &image["runs"][0]["snapshot"]["context_batches"][0];
    let value = &image["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|record| &record["reference"] == reference)
        .unwrap()["value"];
    let request: ContextRequest = serde_json::from_value(value["request"].clone()).unwrap();
    SourceExecutionState {
        source: request.binding.source,
        trigger: request.binding.trigger,
        model_step_id: request.model_step_id,
        context_request_id: request.context_request_id,
        batch_ref: serde_json::from_value(reference.clone()).unwrap(),
    }
}

#[tokio::test]
async fn restoring_current_generation_cannot_reactivate_ready_data_before_a_later_empty_reply() {
    for reply in [Reply::Empty, Reply::Unavailable] {
        let mut fixture = Fixture::new();
        fixture.add(
            "records",
            ContextTrigger::BeforeModel,
            false,
            vec![Reply::Ready, reply],
        );
        let agent = fixture.agent();
        let handle = fixture.start(&agent).await;
        assert_eq!(
            fixture.outcome(&handle).await.result.status(),
            RunStatus::Succeeded
        );
        let image =
            serde_json::to_value(fixture.store.inner.export_checkpoint(&scope()).unwrap()).unwrap();
        let saved = fixture.saved(&handle).await;
        assert_eq!(
            restore(&image)
                .unwrap()
                .load(&scope(), handle.run_id())
                .await
                .unwrap(),
            saved
        );
        assert_eq!(saved.snapshot.context_batches.len(), 2);
        let old = first_slot(&image);
        assert_ne!(old.model_step_id, saved.snapshot.model_step_id);
        let mut changed = image.clone();
        changed["runs"][0]["snapshot"]["model_step_id"] =
            serde_json::to_value(&old.model_step_id).unwrap();
        changed["runs"][0]["snapshot"]["source_states"] = json!([old]);
        assert!(
            restore(&changed).is_err(),
            "old Ready generation was reactivated after a later empty/unavailable batch"
        );
        let mut erased = image.clone();
        erased["runs"][0]["snapshot"]
            .as_object_mut()
            .unwrap()
            .remove("model_step_id");
        erased["runs"][0]["snapshot"]["source_states"] = json!([]);
        assert!(
            restore(&erased).is_err(),
            "completed current source generation was erased"
        );
    }
}

#[tokio::test]
async fn a_batch_without_a_current_or_recorded_model_step_is_not_valid_history() {
    let mut fixture = Fixture::new();
    fixture.add(
        "records",
        ContextTrigger::BeforeModel,
        true,
        vec![Reply::Ready],
    );
    fixture.revoked.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Failed
    );
    assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), 0);
    let image =
        serde_json::to_value(fixture.store.inner.export_checkpoint(&scope()).unwrap()).unwrap();
    restore(&image).unwrap();
    let mut changed = image;
    changed["runs"][0]["snapshot"]["model_step_id"] = json!("unrelated-next-step");
    changed["runs"][0]["snapshot"]["source_states"] = json!([]);
    assert!(
        restore(&changed).is_err(),
        "orphan source generation was accepted as historical context"
    );
}

#[tokio::test]
async fn a_live_commit_cannot_roll_the_current_source_slot_back_to_an_earlier_step() {
    let mut fixture = Fixture::new();
    fixture.add(
        "records",
        ContextTrigger::BeforeModel,
        false,
        vec![Reply::Ready, Reply::Empty],
    );
    fixture.model.inner.hold_final.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    gate(&fixture.model.inner.final_entered).await;
    let saved = fixture.saved(&handle).await;
    let image =
        serde_json::to_value(fixture.store.inner.export_checkpoint(&scope()).unwrap()).unwrap();
    let old = first_slot(&image);
    let lease = &image["runs"][0]["lease"];
    let lease = RunLease {
        scope: scope(),
        run_id: handle.run_id().clone(),
        owner: serde_json::from_value(lease["owner"].clone()).unwrap(),
        fencing_token: lease["fencing_token"].as_u64().unwrap(),
        expires_at_ms: lease["expires_at_ms"].as_i64().unwrap(),
    };
    let isolated = restore(&image).unwrap();
    let mut snapshot = saved.snapshot.clone();
    snapshot.revision += 1;
    snapshot.model_step_id = old.model_step_id.clone();
    snapshot.source_states = vec![old];
    let changed = isolated
        .commit(
            &scope(),
            handle.run_id(),
            CommitInput {
                expected_revision: saved.snapshot.revision,
                lease,
                now_ms: snapshot.timing.last_observed_at_ms,
                snapshot,
                messages: vec![],
                events: vec![],
                records: vec![],
            },
        )
        .await;
    fixture.model.inner.final_release.add_permits(1);
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert!(changed.is_err(), "live source slot rollback was committed");
    assert_eq!(
        isolated.load(&scope(), handle.run_id()).await.unwrap(),
        saved
    );
}

#[tokio::test]
async fn a_delayed_query_result_cannot_be_committed_after_another_generation_is_prepared() {
    let mut fixture = Fixture::new();
    let source = fixture.add(
        "records",
        ContextTrigger::BeforeModel,
        false,
        vec![Reply::Paused, Reply::Empty],
    );
    fixture.model.inner.hold_final.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    gate(&source.entered).await;
    let before = fixture.saved(&handle).await;
    let before_image =
        serde_json::to_value(fixture.store.inner.export_checkpoint(&scope()).unwrap()).unwrap();
    assert!(before.snapshot.context_batches.is_empty());
    source.release.add_permits(1);
    gate(&fixture.model.inner.final_entered).await;
    let image =
        serde_json::to_value(fixture.store.inner.export_checkpoint(&scope()).unwrap()).unwrap();
    let slot = first_slot(&image);
    assert_eq!(slot.model_step_id, before.snapshot.model_step_id);
    let record = image["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|record| record["reference"] == serde_json::to_value(&slot.batch_ref).unwrap())
        .unwrap();
    let record = ProtectedRecord::new(
        slot.batch_ref.record_id.clone(),
        slot.batch_ref.revision,
        record["value"].clone(),
    );
    let raw = &before_image["runs"][0]["lease"];
    let lease = RunLease {
        scope: scope(),
        run_id: handle.run_id().clone(),
        owner: serde_json::from_value(raw["owner"].clone()).unwrap(),
        fencing_token: raw["fencing_token"].as_u64().unwrap(),
        expires_at_ms: raw["expires_at_ms"].as_i64().unwrap(),
    };
    let mut collected = before.snapshot.clone();
    collected.revision += 1;
    collected.context_batches.push(slot.batch_ref.clone());
    collected.source_states.push(slot);
    let candidate = CommitInput {
        expected_revision: before.snapshot.revision,
        lease: lease.clone(),
        now_ms: collected.timing.last_observed_at_ms,
        snapshot: collected,
        messages: vec![],
        events: vec![],
        records: vec![record],
    };
    // The unchanged actual query result is a valid commit for its original generation.
    let positive = restore(&before_image).unwrap();
    positive
        .commit(&scope(), handle.run_id(), candidate.clone())
        .await
        .unwrap();
    let isolated = restore(&before_image).unwrap();
    let mut advanced = before.snapshot.clone();
    advanced.revision += 1;
    advanced.model_step_id = Some(id("new-query-generation"));
    isolated
        .commit(
            &scope(),
            handle.run_id(),
            CommitInput {
                expected_revision: before.snapshot.revision,
                lease,
                now_ms: advanced.timing.last_observed_at_ms,
                snapshot: advanced.clone(),
                messages: vec![],
                events: vec![],
                records: vec![],
            },
        )
        .await
        .unwrap();
    let before_late = isolated.load(&scope(), handle.run_id()).await.unwrap();
    let mut late = candidate;
    late.expected_revision = advanced.revision;
    late.snapshot.revision = advanced.revision + 1;
    let result = isolated.commit(&scope(), handle.run_id(), late).await;
    fixture.model.inner.final_release.add_permits(1);
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert!(
        result.is_err(),
        "a delayed earlier query replaced the newly prepared generation"
    );
    assert_eq!(
        isolated.load(&scope(), handle.run_id()).await.unwrap(),
        before_late
    );
}
