//! Hook reports and transformations remain tied to committed data after restoration.
#[path = "support/agent.rs"]
#[allow(dead_code)]
mod agent_support;
#[path = "support/agent_resume.rs"]
#[allow(dead_code)]
mod resume_support;
#[path = "support/agent_hooks.rs"]
#[allow(dead_code, unused_imports)]
mod support;
use serde_json::{Value, json};
use support::*;
use wickle::*;

fn restore(value: &Value) -> Result<MemoryStateStore, ContractError> {
    StateStoreCheckpoint::from_json(&value.to_string(), &scope(), &canonical_digest(value))
        .map(MemoryStateStore::from_checkpoint)
}

#[tokio::test]
async fn hook_reports_append_after_completion_without_mutating_the_run_or_event_endpoint() {
    let mut fixture = Fixture::new();
    fixture.add(
        "tool-observer",
        HookPosition::AfterTool,
        Behavior::Observe,
        0,
        true,
    );
    fixture.add(
        "run-observer",
        HookPosition::AfterRun,
        Behavior::Observe,
        0,
        true,
    );
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    let view = observations(&handle, 4).await;
    assert!(view.local_error.is_none());
    assert_eq!(view.reports.len(), 4);
    let store = &fixture.base.base.store;
    let before = store.load(&scope(), handle.run_id()).await.unwrap();
    let events = store
        .read_events(&scope(), handle.run_id(), 0, 100)
        .await
        .unwrap();
    let checkpoint = store.export_checkpoint(&scope()).unwrap();
    let image = serde_json::to_value(&checkpoint).unwrap();
    let restored = restore(&image).unwrap();
    assert_eq!(
        restored
            .read_hook_observations(&scope(), handle.run_id())
            .await
            .unwrap(),
        view.reports
    );
    let report = view
        .reports
        .iter()
        .find(|report| matches!(report.target, HookTarget::AfterRun { .. }))
        .unwrap();
    store
        .record_hook_observation(&scope(), handle.run_id(), report.clone())
        .await
        .unwrap();
    assert_eq!(
        store.export_checkpoint(&scope()).unwrap().digest(),
        checkpoint.digest()
    );
    for fault in 0..5 {
        let mut changed = report.clone();
        match fault {
            0 => changed.scope.workspace_id = id("other-workspace"),
            1 => changed.definition_digest = canonical_digest(&json!("different hook")),
            2 => changed.input_digest = canonical_digest(&json!("different outcome")),
            3 => changed.target = HookTarget::BeforeRun,
            4 => {
                changed.status = HookObservationStatus::Failed {
                    code: id("changed-report"),
                }
            }
            _ => unreachable!(),
        }
        assert!(
            store
                .record_hook_observation(&scope(), handle.run_id(), changed)
                .await
                .is_err(),
            "report fault {fault}"
        );
    }
    assert_eq!(store.load(&scope(), handle.run_id()).await.unwrap(), before);
    assert_eq!(
        store
            .read_events(&scope(), handle.run_id(), 0, 100)
            .await
            .unwrap(),
        events
    );
    let mut duplicate = image.clone();
    let report = duplicate["hook_observations"][0].clone();
    duplicate["hook_observations"]
        .as_array_mut()
        .unwrap()
        .push(report);
    assert!(restore(&duplicate).is_err());
    let mut changed = image;
    changed["hook_observations"][0]["input_digest"] =
        serde_json::to_value(canonical_digest(&json!("forged"))).unwrap();
    assert!(restore(&changed).is_err());
}

fn replace_reference(value: &mut Value, old: &Value, new: &Value) {
    if value == old {
        *value = new.clone();
        return;
    }
    match value {
        Value::Array(items) => items
            .iter_mut()
            .for_each(|item| replace_reference(item, old, new)),
        Value::Object(fields) => fields
            .values_mut()
            .for_each(|item| replace_reference(item, old, new)),
        _ => {}
    }
}

#[tokio::test]
async fn recomputed_checksums_cannot_replace_hook_context_provenance_or_remove_required_applications()
 {
    let mut fixture = Fixture::new();
    fixture.add(
        "context",
        HookPosition::BeforeRun,
        Behavior::Context,
        0,
        true,
    );
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    let saved = fixture.saved(&handle).await;
    let image =
        serde_json::to_value(fixture.base.base.store.export_checkpoint(&scope()).unwrap()).unwrap();
    restore(&image).unwrap();
    let reference = serde_json::to_value(&saved.snapshot.hook_applications[0].result_ref).unwrap();
    let mut changed = image.clone();
    let record = changed["records"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|record| record["reference"] == reference)
        .unwrap();
    let original: ContextItem =
        serde_json::from_value(record["value"]["context_items"][0].clone()).unwrap();
    let forged = ContextItem::new(
        original.item_id,
        ContextOrigin::Memory,
        reference_id(),
        original.scope,
        original.content,
        original.lifetime,
        original.priority_class,
    );
    record["value"]["context_items"][0] = serde_json::to_value(forged).unwrap();
    let mut new_reference = reference.clone();
    new_reference["digest"] = serde_json::to_value(canonical_digest(&record["value"])).unwrap();
    replace_reference(&mut changed, &reference, &new_reference);
    assert!(restore(&changed).is_err());
    let mut missing = image;
    missing["runs"][0]["snapshot"]["hook_applications"] = json!([]);
    assert!(restore(&missing).is_err());
}

fn reference_id() -> VersionedRef {
    VersionedRef {
        id: id("foreign-source"),
        version: id("1"),
    }
}
