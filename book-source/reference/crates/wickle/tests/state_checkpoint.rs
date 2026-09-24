//! Restoration checks exercise saved state and historical facts, not implementation text.

mod support;

use serde_json::{Value, json};
use support::{admission, event, finished, id, prepared, scope};
use wickle::*;

fn restore_json(value: &Value, owner: &Scope) -> Result<MemoryStateStore, ContractError> {
    let digest = canonical_digest(value);
    let checkpoint = StateStoreCheckpoint::from_json(&value.to_string(), owner, &digest)?;
    Ok(MemoryStateStore::from_checkpoint(checkpoint))
}

#[tokio::test]
async fn exporting_one_namespace_excludes_another_scope_with_the_same_identifiers() {
    let store = MemoryStateStore::new();
    store
        .admit(
            &scope(),
            admission("run", "request", "session", "first scope", "1").await,
        )
        .await
        .unwrap();
    let mut other = scope();
    other.tenant_id = id("other-tenant");
    let mut input = admission(
        "run",
        "request",
        "session",
        "second scope private data",
        "1",
    )
    .await;
    input.snapshot.profile = ProfileValidator::new(&support::Catalog { revision: "1" })
        .validate(input.snapshot.profile.profile(), &other)
        .await
        .unwrap();
    input.snapshot.scope = other.clone();
    input.snapshot.request_digest =
        admission_digest(&input.snapshot.request, &input.snapshot.profile, None);
    for event in &mut input.events {
        event.scope = other.clone();
    }
    store.admit(&other, input).await.unwrap();
    let checkpoint = store.export_checkpoint(&scope()).unwrap();
    let serialized = serde_json::to_string(&checkpoint).unwrap();
    assert!(!serialized.contains("second scope private data"));
    let restored = MemoryStateStore::from_checkpoint(checkpoint);
    assert_eq!(
        restored
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .request
            .input,
        vec![InputContent::Text {
            text: "first scope".into()
        }]
    );
    assert_eq!(
        restored.load(&other, &id("run")).await.unwrap_err().code,
        ErrorCode::StateNotFound
    );
    assert!(store.load(&other, &id("run")).await.is_ok());
}

#[tokio::test]
async fn checkpoint_preserves_request_identity_private_records_and_lease_generation() {
    let store = MemoryStateStore::new();
    let request = admission("run", "request", "session", "private user input", "1").await;
    store.admit(&scope(), request.clone()).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("worker"), 100, 10)
        .await
        .unwrap();
    let snapshot = store.load(&scope(), &id("run")).await.unwrap().snapshot;
    let record = ProtectedRecord::new(
        id("orphan-private-record"),
        7,
        json!({"secret":"protected checkpoint value"}),
    );
    let record_ref = record.reference().clone();
    let mut change = prepared(&snapshot, lease.clone(), 101);
    change.records.push(record);
    store.commit(&scope(), &id("run"), change).await.unwrap();
    let before = store.load(&scope(), &id("run")).await.unwrap();
    let checkpoint = store.export_checkpoint(&scope()).unwrap();
    let encoded = serde_json::to_string(&checkpoint).unwrap();
    assert_eq!(
        canonical_digest(&serde_json::from_str(&encoded).unwrap()),
        checkpoint.digest()
    );
    assert!(!format!("{checkpoint:?}").contains("protected checkpoint value"));
    assert!(!format!("{checkpoint:?}").contains("private user input"));
    let decoded =
        StateStoreCheckpoint::from_json(&encoded, &scope(), &checkpoint.digest()).unwrap();
    let restored = MemoryStateStore::from_checkpoint(decoded);
    assert_eq!(restored.load(&scope(), &id("run")).await.unwrap(), before);
    assert_eq!(
        restored
            .read_record(&scope(), &record_ref)
            .await
            .unwrap()
            .value(),
        &json!({"secret":"protected checkpoint value"})
    );
    assert!(!restored.admit(&scope(), request).await.unwrap().created);
    assert_eq!(
        restored
            .check_lease(&scope(), &id("run"), &lease, 109)
            .await
            .unwrap(),
        lease
    );
    assert_eq!(
        restored
            .check_lease(&scope(), &id("run"), &lease, 110)
            .await
            .unwrap_err()
            .code,
        ErrorCode::LeaseLost
    );
    let replacement = restored
        .acquire_lease(&scope(), &id("run"), &id("new-worker"), 110, 20)
        .await
        .unwrap();
    assert!(replacement.fencing_token > lease.fencing_token);
    assert_eq!(
        restored
            .commit(&scope(), &id("run"), prepared(&before.snapshot, lease, 111))
            .await
            .unwrap_err()
            .code,
        ErrorCode::LeaseLost
    );
    assert!(!restored.capabilities().durable);
    assert!(!restored.capabilities().cross_process_leases);
}

#[tokio::test]
async fn released_leases_keep_their_fencing_counter_after_restoration() {
    let store = MemoryStateStore::new();
    store
        .admit(
            &scope(),
            admission("run", "request", "session", "input", "1").await,
        )
        .await
        .unwrap();
    let first = store
        .acquire_lease(&scope(), &id("run"), &id("worker"), 1, 100)
        .await
        .unwrap();
    store
        .release_lease(&scope(), &id("run"), &first, 2)
        .await
        .unwrap();
    let restored = MemoryStateStore::from_checkpoint(store.export_checkpoint(&scope()).unwrap());
    assert!(
        restored
            .check_lease(&scope(), &id("run"), &first, 3)
            .await
            .is_err()
    );
    let second = restored
        .acquire_lease(&scope(), &id("run"), &id("worker"), 3, 100)
        .await
        .unwrap();
    assert!(second.fencing_token > first.fencing_token);
}

#[tokio::test]
async fn historical_wait_and_resume_survive_a_finished_run_and_a_new_session_run() {
    let store = MemoryStateStore::new();
    store
        .admit(
            &scope(),
            admission("run-a", "request-a", "session", "first input", "1").await,
        )
        .await
        .unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run-a"), &id("worker"), 100, 100)
        .await
        .unwrap();
    let snapshot = store.load(&scope(), &id("run-a")).await.unwrap().snapshot;
    let candidate = ProtectedRecord::new(id("candidate"), 1, json!({"source":"saved selection"}));
    let target = ApprovalTarget::Candidate {
        candidate_ref: candidate.reference().clone(),
        verifier_ref: VersionedRef {
            id: id("verifier"),
            version: id("1"),
        },
    };
    let wait = WaitState {
        wait_id: id("wait"),
        target: WaitTarget::Approval {
            target: target.clone(),
        },
        expires_at_ms: Some(180),
    };
    let wait_record = ProtectedRecord::new(id("old-wait"), 1, serde_json::to_value(&wait).unwrap());
    let mut waiting = prepared(&snapshot, lease.clone(), 101);
    waiting.snapshot.status = RunStatus::Waiting;
    waiting.snapshot.phase = RunPhase::Waiting;
    waiting.snapshot.wait = Some(wait);
    waiting.snapshot.outcome = Some(RunOutcome {
        app_state: None,
        result: OutcomeResult::Waiting {
            wait: waiting.snapshot.wait.clone().unwrap(),
        },
        output: vec![],
        artifacts: vec![],
        usage: waiting.snapshot.usage.clone(),
        checkpoint_revision: waiting.snapshot.revision,
        verification: None,
        unresolved_effects: vec![],
    });
    let prior_outcome = ProtectedRecord::new(
        id("old-outcome"),
        1,
        serde_json::to_value(waiting.snapshot.outcome.as_ref().unwrap()).unwrap(),
    );
    let prior_outcome_ref = prior_outcome.reference().clone();
    waiting.records.push(prior_outcome);
    waiting.records.push(candidate);
    waiting.snapshot.last_event_seq += 1;
    waiting.events.push(event(
        &id("run-a"),
        &id("session"),
        &scope(),
        2,
        RunEventPayload::RunWaiting {
            outcome_ref: Some(prior_outcome_ref.clone()),
            wait_ref: wait_record.reference().clone(),
        },
    ));
    waiting.records.push(wait_record);
    waiting.events[0].timestamp_ms = 101;
    let waiting = store.commit(&scope(), &id("run-a"), waiting).await.unwrap();
    let command = ResumeCommand {
        run_id: id("run-a"),
        expected_revision: waiting.snapshot.revision,
        command_id: id("answer"),
        action: ResumeAction::Approve {
            wait_id: id("wait"),
            target,
        },
    };
    let command_record = ProtectedRecord::new(
        id("old-command"),
        1,
        serde_json::to_value(&command).unwrap(),
    );
    let mut resumed = prepared(&waiting.snapshot, lease.clone(), 102);
    resumed.snapshot.status = RunStatus::Running;
    resumed.snapshot.wait = None;
    resumed.snapshot.outcome = None;
    resumed.snapshot.timing.last_observed_at_ms = 102;
    resumed.snapshot.usage.elapsed_ms = (102 - resumed.snapshot.timing.started_at_ms) as u64;
    resumed.snapshot.resume_receipts.push(ResumeReceipt {
        command: command.clone(),
        command_ref: command_record.reference().clone(),
        accepted_revision: resumed.snapshot.revision,
        previous_segment_start_revision: 0,
        previous_outcome_ref: prior_outcome_ref,
        previous_last_event_seq: waiting.snapshot.last_event_seq,
        actor_ref: id("reviewer"),
        capability_grant_ref: id("reviewer-grant"),
        expired: false,
    });
    resumed.snapshot.last_event_seq += 1;
    resumed.events.push(event(
        &id("run-a"),
        &id("session"),
        &scope(),
        3,
        RunEventPayload::RunResumed {
            command_ref: command_record.reference().clone(),
        },
    ));
    resumed.events[0].timestamp_ms = 102;
    resumed.records.push(command_record);
    let resumed = store.commit(&scope(), &id("run-a"), resumed).await.unwrap();
    store
        .commit(
            &scope(),
            &id("run-a"),
            finished(&resumed.snapshot, lease, 103),
        )
        .await
        .unwrap();
    let mut next = admission("run-b", "request-b", "session", "next input", "2").await;
    next.messages[0].sequence = 2.try_into().unwrap();
    store.admit(&scope(), next).await.unwrap();
    let image = serde_json::to_value(store.export_checkpoint(&scope()).unwrap()).unwrap();
    let restored = restore_json(&image, &scope()).unwrap();
    let old = restored.load(&scope(), &id("run-a")).await.unwrap();
    assert_eq!(old.snapshot.status, RunStatus::Succeeded);
    assert!(old.snapshot.wait.is_none());
    assert_eq!(old.session.active_run_id, Some(id("run-b")));
    assert_eq!(old.messages.len(), 2);
    let events = restored
        .read_events(&scope(), &id("run-a"), 0, 10)
        .await
        .unwrap();
    assert_eq!(events.events.len(), 4);
    assert!(matches!(
        events.events[1].payload,
        RunEventPayload::RunWaiting { .. }
    ));
    assert!(matches!(
        events.events[2].payload,
        RunEventPayload::RunResumed { .. }
    ));

    for fault in 0..3 {
        let mut corrupted = image.clone();
        let receipts = &mut corrupted["runs"][0]["snapshot"]["resume_receipts"];
        match fault {
            0 => receipts[0]["command"]["command_id"] = json!("changed command"),
            1 => receipts[0]["previous_segment_start_revision"] = json!(999),
            2 => {
                let duplicate = receipts[0].clone();
                receipts.as_array_mut().unwrap().push(duplicate);
            }
            _ => unreachable!(),
        }
        assert!(
            restore_json(&corrupted, &scope()).is_err(),
            "resume receipt fault {fault}"
        );
    }

    // An active run cannot precede a later completed run in the same transcript.
    let mut reversed = image;
    let messages = reversed["sessions"][0]["messages"].as_array_mut().unwrap();
    messages.swap(0, 1);
    messages[0]["sequence"] = json!(1);
    messages[1]["sequence"] = json!(2);
    assert_eq!(
        restore_json(&reversed, &scope()).err().unwrap().code,
        ErrorCode::InvalidSnapshot
    );
}

#[tokio::test]
async fn structural_corruption_is_rejected_even_with_a_recomputed_outer_digest() {
    let store = MemoryStateStore::new();
    store
        .admit(
            &scope(),
            admission("run", "request", "session", "input", "1").await,
        )
        .await
        .unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("worker"), 100, 20)
        .await
        .unwrap();
    let original = serde_json::to_value(store.export_checkpoint(&scope()).unwrap()).unwrap();
    let mut mutations = Vec::new();
    for collection in ["sessions", "runs", "records"] {
        let mut value = original.clone();
        let duplicate = value[collection][0].clone();
        value[collection].as_array_mut().unwrap().push(duplicate);
        mutations.push(value);
    }
    let paths = [
        (
            "/runs/0/snapshot/scope/workspace_id",
            json!("different-workspace"),
        ),
        (
            "/sessions/0/snapshot/scope/tenant_id",
            json!("different-tenant"),
        ),
        ("/sessions/0/snapshot/active_run_id", json!("missing-run")),
        ("/sessions/0/snapshot/transcript_revision", json!(2)),
        ("/sessions/0/messages/0/run_id", json!("missing-run")),
        ("/runs/0/events/0/seq", json!(2)),
        ("/runs/0/events/0/run_id", json!("missing-run")),
        (
            "/runs/0/lease/fencing_token",
            json!(lease.fencing_token + 1),
        ),
        ("/runs/0/last_fencing_token", json!(0)),
        (
            "/records/0/value",
            json!({"changed":"without updating record digest"}),
        ),
    ];
    for (path, replacement) in paths {
        let mut value = original.clone();
        *value.pointer_mut(path).unwrap() = replacement;
        mutations.push(value);
    }
    for mutation in mutations {
        assert!(restore_json(&mutation, &scope()).is_err());
    }
    let restored = restore_json(&original, &scope()).unwrap();
    assert_eq!(
        restored
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .run_id,
        id("run")
    );
}

#[tokio::test]
async fn version_scope_and_trusted_digest_are_independent_restore_guards() {
    let store = MemoryStateStore::new();
    store
        .admit(
            &scope(),
            admission("run", "request", "session", "input", "1").await,
        )
        .await
        .unwrap();
    let checkpoint = store.export_checkpoint(&scope()).unwrap();
    let json = serde_json::to_string(&checkpoint).unwrap();
    let mut foreign = scope();
    foreign.user_id = Some(id("another-user"));
    assert_eq!(
        StateStoreCheckpoint::from_json(&json, &foreign, &checkpoint.digest())
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert!(store.export_checkpoint(&foreign).is_err());
    assert!(
        StateStoreCheckpoint::from_json(&json, &scope(), &canonical_digest(&json!("wrong digest")))
            .is_err()
    );
    let mut unknown: Value = serde_json::from_str(&json).unwrap();
    unknown["schema_version"] = json!("wickle.state-store.v99");
    assert_eq!(
        StateStoreCheckpoint::from_json(
            &unknown.to_string(),
            &scope(),
            &canonical_digest(&unknown)
        )
        .unwrap_err()
        .code,
        ErrorCode::UnsupportedSchemaVersion
    );
}
