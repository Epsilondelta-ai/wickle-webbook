//! Real-file transactions, process isolation, and restoration of protected execution state.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use rusqlite::Connection;
use serde_json::{Value, json};
use wickle::*;
use wickle_state_sqlite::SqliteStateStore;

#[allow(dead_code)]
#[path = "../../wickle/tests/support/mod.rs"]
mod core;
mod support;
#[path = "support/workers.rs"]
mod workers;

use core::{event, finished, id, prepared, scope};
use support::{Database, durable_admission};

#[tokio::test]
async fn durable_admission_replays_the_original_run_and_releases_a_finished_session_after_reopen() {
    let database = Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    assert!(store.capabilities().durable);
    let first = store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap();
    assert!(first.created);
    drop(store);
    let store = SqliteStateStore::open(database.path()).unwrap();
    let replay = store
        .admit(
            &scope(),
            durable_admission("replacement", "request", "session").await,
        )
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state, first.state);
    assert_eq!(
        store
            .find_request(&scope(), &id("session"), &id("request"))
            .await
            .unwrap(),
        Some(first.state.clone())
    );
    assert!(
        store
            .find_request(
                &Scope {
                    workspace_id: id("foreign"),
                    ..scope()
                },
                &id("session"),
                &id("request")
            )
            .await
            .unwrap()
            .is_none()
    );
    let mut changed = durable_admission("changed", "request", "session").await;
    changed.snapshot.request.input = vec![InputContent::Text {
        text: "Different input".into(),
    }];
    changed.snapshot.request_digest =
        admission_digest(&changed.snapshot.request, &changed.snapshot.profile, None);
    assert_eq!(
        store.admit(&scope(), changed).await.unwrap_err().code,
        ErrorCode::RequestConflict
    );
    assert_eq!(
        store
            .admit(
                &scope(),
                durable_admission("other", "other", "session").await
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::SessionBusy
    );
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 0, 10_000)
        .await
        .unwrap();
    store
        .commit(
            &scope(),
            &id("run"),
            finished(&first.state.snapshot, lease, 1),
        )
        .await
        .unwrap();
    drop(store);
    let store = SqliteStateStore::open(database.path()).unwrap();
    let mut second = durable_admission("second", "second", "session").await;
    assert_eq!(
        store
            .find_request(&scope(), &id("session"), &id("request"))
            .await
            .unwrap()
            .unwrap()
            .snapshot
            .status,
        RunStatus::Succeeded
    );
    second.messages[0].sequence = 2.try_into().unwrap();
    assert!(store.admit(&scope(), second).await.unwrap().created);
    assert_eq!(
        store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .status,
        RunStatus::Succeeded
    );
    let replay = store
        .admit(
            &scope(),
            durable_admission("another", "request", "session").await,
        )
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state.snapshot.run_id, id("run"));
}

#[tokio::test]
async fn failed_transactions_leave_no_records_or_events_and_stale_revisions_cannot_commit() {
    let database = Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    let initial = store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap()
        .state;
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 0, 10_000)
        .await
        .unwrap();
    let mut invalid = finished(&initial.snapshot, lease.clone(), 1);
    let unpublished = ProtectedRecord::new(id("unpublished"), 1, json!({"value":"candidate"}));
    invalid.records.push(unpublished.clone());
    invalid.events[0].seq = 9.try_into().unwrap();
    assert!(store.commit(&scope(), &id("run"), invalid).await.is_err());
    assert_eq!(store.load(&scope(), &id("run")).await.unwrap(), initial);
    assert!(
        store
            .read_record(&scope(), unpublished.reference())
            .await
            .is_err()
    );
    assert_eq!(
        store
            .read_events(&scope(), &id("run"), 0, 10)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
    let update = prepared(&initial.snapshot, lease.clone(), 2);
    let saved = store
        .commit(&scope(), &id("run"), update.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .commit(&scope(), &id("run"), update)
            .await
            .unwrap_err()
            .code,
        ErrorCode::RevisionConflict
    );
    let mut final_update = finished(&saved.snapshot, lease, 3);
    final_update.records.push(unpublished.clone());
    store
        .commit(&scope(), &id("run"), final_update)
        .await
        .unwrap();
    drop(store);
    let store = SqliteStateStore::open(database.path()).unwrap();
    assert_eq!(
        store
            .read_record(&scope(), unpublished.reference())
            .await
            .unwrap(),
        unpublished
    );
    let page = store.read_events(&scope(), &id("run"), 0, 1).await.unwrap();
    assert!(page.has_more);
    assert_eq!(page.next_after_seq, 1);
    let next = store
        .read_events(&scope(), &id("run"), page.next_after_seq, 1)
        .await
        .unwrap();
    assert!(!next.has_more);
    assert_eq!(next.next_after_seq, 2);
    assert!(matches!(
        next.events[0].payload,
        RunEventPayload::RunFinished { .. }
    ));
    for foreign in [
        Scope {
            tenant_id: id("foreign"),
            ..scope()
        },
        Scope {
            workspace_id: id("foreign-workspace"),
            ..scope()
        },
        Scope {
            user_id: Some(id("foreign-user")),
            ..scope()
        },
    ] {
        assert!(store.load(&foreign, &id("run")).await.is_err());
        assert!(store.load_session(&foreign, &id("session")).await.is_err());
        assert!(
            store
                .read_record(&foreign, unpublished.reference())
                .await
                .is_err()
        );
        assert!(
            store
                .read_events(&foreign, &id("run"), 0, 10)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn renewed_leases_and_released_fence_counters_survive_separate_connections_and_reopen() {
    let database = Database::new();
    let first = SqliteStateStore::open(database.path()).unwrap();
    first
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap();
    let lease = first
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 10_000)
        .await
        .unwrap();
    let renewed = first
        .renew_lease(&scope(), &id("run"), &lease, 105, 20_000)
        .await
        .unwrap();
    drop(first);
    let next = SqliteStateStore::open(database.path()).unwrap();
    let after_original_expiry = lease.expires_at_ms + 100;
    assert_eq!(
        next.check_lease(&scope(), &id("run"), &lease, after_original_expiry)
            .await
            .unwrap()
            .expires_at_ms,
        renewed.expires_at_ms
    );
    assert_eq!(
        next.acquire_lease(
            &scope(),
            &id("run"),
            &id("new-owner"),
            after_original_expiry + 1,
            10_000
        )
        .await
        .unwrap_err()
        .code,
        ErrorCode::LeaseBusy
    );
    let now = renewed.expires_at_ms + 1;
    let takeover = next
        .acquire_lease(&scope(), &id("run"), &id("owner"), now, 10_000)
        .await
        .unwrap();
    assert!(takeover.fencing_token > lease.fencing_token);
    assert_eq!(
        next.check_lease(&scope(), &id("run"), &lease, now + 1)
            .await
            .unwrap_err()
            .code,
        ErrorCode::LeaseLost
    );
    next.release_lease(&scope(), &id("run"), &takeover, now + 2)
        .await
        .unwrap();
    drop(next);
    let reopened = SqliteStateStore::open(database.path()).unwrap();
    let after_release = reopened
        .acquire_lease(&scope(), &id("run"), &id("owner"), now + 3, 10_000)
        .await
        .unwrap();
    assert!(after_release.fencing_token > takeover.fencing_token);
    assert_eq!(
        reopened
            .renew_lease(&scope(), &id("run"), &takeover, now + 4, 10_000)
            .await
            .unwrap_err()
            .code,
        ErrorCode::LeaseLost
    );
}

#[tokio::test]
async fn historical_wait_and_resume_events_remain_valid_after_terminal_reopen() {
    let database = Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    let saved = store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap()
        .state;
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 0, 10_000)
        .await
        .unwrap();
    // This low-level store test preserves generic candidate-review history;
    // actual tool approvals are exercised by the Agent resume consumer.
    let candidate = ProtectedRecord::new(id("candidate"), 1, json!({"selection":"saved"}));
    let target = ApprovalTarget::Candidate {
        candidate_ref: candidate.reference().clone(),
        verifier_ref: VersionedRef {
            id: id("reviewer"),
            version: id("1"),
        },
    };
    let wait = WaitState {
        wait_id: id("wait"),
        target: WaitTarget::Approval {
            target: target.clone(),
        },
        expires_at_ms: None,
    };
    let record = ProtectedRecord::new(id("wait-record"), 1, serde_json::to_value(&wait).unwrap());
    let mut update = prepared(&saved.snapshot, lease.clone(), 1);
    update.snapshot.status = RunStatus::Waiting;
    update.snapshot.phase = RunPhase::Waiting;
    update.snapshot.wait = Some(wait);
    update.snapshot.outcome = Some(RunOutcome {
        app_state: None,
        result: OutcomeResult::Waiting {
            wait: update.snapshot.wait.clone().unwrap(),
        },
        output: vec![],
        artifacts: vec![],
        usage: update.snapshot.usage.clone(),
        checkpoint_revision: update.snapshot.revision,
        verification: None,
        unresolved_effects: vec![],
    });
    let prior_outcome = ProtectedRecord::new(
        id("prior-outcome"),
        1,
        serde_json::to_value(update.snapshot.outcome.as_ref().unwrap()).unwrap(),
    );
    let prior_outcome_ref = prior_outcome.reference().clone();
    update.records.extend([candidate, prior_outcome]);
    update.snapshot.last_event_seq += 1;
    update.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        2,
        RunEventPayload::RunWaiting {
            outcome_ref: Some(prior_outcome_ref.clone()),
            wait_ref: record.reference().clone(),
        },
    ));
    update.events[0].timestamp_ms = 1;
    update.records.push(record);
    store.commit(&scope(), &id("run"), update).await.unwrap();
    store
        .release_lease(&scope(), &id("run"), &lease, 2)
        .await
        .unwrap();
    drop(store);
    let store = SqliteStateStore::open(database.path()).unwrap();
    assert_eq!(
        store
            .admit(
                &scope(),
                durable_admission("other", "other", "session").await
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::SessionBusy
    );
    let saved = store.load(&scope(), &id("run")).await.unwrap();
    assert_eq!(saved.snapshot.status, RunStatus::Waiting);
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("new-owner"), 3, 10_000)
        .await
        .unwrap();
    let command = ResumeCommand {
        run_id: id("run"),
        expected_revision: saved.snapshot.revision,
        command_id: id("resume"),
        action: ResumeAction::Approve {
            wait_id: id("wait"),
            target,
        },
    };
    let record = ProtectedRecord::new(
        id("resume-record"),
        1,
        serde_json::to_value(&command).unwrap(),
    );
    let mut update = prepared(&saved.snapshot, lease.clone(), 4);
    update.snapshot.status = RunStatus::Running;
    update.snapshot.wait = None;
    update.snapshot.outcome = None;
    update.snapshot.timing.last_observed_at_ms = 4;
    update.snapshot.usage.elapsed_ms = 4;
    update.snapshot.resume_receipts.push(ResumeReceipt {
        command: command.clone(),
        command_ref: record.reference().clone(),
        accepted_revision: update.snapshot.revision,
        expired: false,
        previous_segment_start_revision: 0,
        previous_outcome_ref: prior_outcome_ref,
        previous_last_event_seq: saved.snapshot.last_event_seq,
        actor_ref: id("reviewer"),
        capability_grant_ref: id("reviewer-grant"),
    });
    update.snapshot.last_event_seq += 1;
    update.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        3,
        RunEventPayload::RunResumed {
            command_ref: record.reference().clone(),
        },
    ));
    update.records.push(record);
    update.events[0].timestamp_ms = 4;
    let resumed = store.commit(&scope(), &id("run"), update).await.unwrap();
    store
        .commit(&scope(), &id("run"), finished(&resumed.snapshot, lease, 5))
        .await
        .unwrap();
    drop(store);
    let reopened = SqliteStateStore::open(database.path()).unwrap();
    assert_eq!(
        reopened
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .status,
        RunStatus::Succeeded
    );
    assert_eq!(
        reopened
            .read_events(&scope(), &id("run"), 0, 10)
            .await
            .unwrap()
            .events
            .len(),
        4
    );
}

#[tokio::test]
async fn independent_processes_deduplicate_admission_and_compete_for_an_active_session() {
    for duplicate in [true, false] {
        let database = Database::new();
        drop(SqliteStateStore::open(database.path()).unwrap());
        let mut first =
            workers::Worker::spawn(&database, "first", "admit", json!({"request":"request"}));
        let mut second = workers::Worker::spawn(
            &database,
            "second",
            "admit",
            json!({"request":if duplicate { "request" } else { "other-request" }}),
        );
        workers::wait(&first.ready);
        workers::wait(&second.ready);
        workers::signal(&database.file("gate"));
        let results = [first.finish(), second.finish()];
        assert_eq!(
            results
                .iter()
                .filter(|result| result["created"] == true)
                .count(),
            1
        );
        if duplicate {
            assert_eq!(results[0]["run"], results[1]["run"]);
            assert_eq!(
                results
                    .iter()
                    .filter(|result| result["created"] == false)
                    .count(),
                1
            );
        } else {
            assert_eq!(
                results
                    .iter()
                    .filter(|result| result["error"] == "SessionBusy")
                    .count(),
                1
            );
        }
    }
}

#[tokio::test]
async fn an_old_process_cannot_write_after_another_process_takes_over_its_lease() {
    let database = Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap();
    let mut first = workers::Worker::spawn(&database, "first", "lease_hold", json!({}));
    let mut second = workers::Worker::spawn(&database, "second", "lease_hold", json!({}));
    workers::wait(&first.ready);
    workers::wait(&second.ready);
    workers::signal(&database.file("gate"));
    workers::wait(&first.result);
    workers::wait(&second.result);
    let initial = [workers::read(&first.result), workers::read(&second.result)];
    assert_eq!(
        initial
            .iter()
            .filter(|result| result["error"] == "LeaseBusy")
            .count(),
        1
    );
    let winner = initial
        .iter()
        .position(|result| result["fence"].is_u64())
        .unwrap();
    let acquired = &initial[winner];
    let fence = acquired["fence"].as_u64().unwrap();
    let mut replacement = workers::Worker::spawn(
        &database,
        "replacement",
        "takeover",
        json!({"now":acquired["expires_at_ms"].as_i64().unwrap() + 1,"owner":if winner == 0 {"first"} else {"second"}}),
    );
    let result = replacement.finish();
    assert!(result["fence"].as_u64().unwrap() > fence);
    assert_eq!(result["revision"], 1);
    workers::signal(&database.file("resume"));
    let completed = [first.finish(), second.finish()];
    let rejected = completed
        .iter()
        .find(|result| result["stale_error"] == "LeaseLost")
        .unwrap();
    assert_eq!(rejected["observed_revision"], 1);
    assert_eq!(
        store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .revision,
        1
    );
}

#[tokio::test]
async fn committed_protected_state_recovers_after_process_exit_without_destructors() {
    let database = Database::new();
    let mut worker = workers::Worker::spawn(&database, "committer", "commit_exit", json!({}));
    let expected = worker.finish();
    let mut wal_path = database.path().into_os_string();
    wal_path.push("-wal");
    let wal = std::fs::metadata(std::path::PathBuf::from(wal_path))
        .expect("the exited worker must leave its committed WAL for recovery");
    assert!(wal.len() > 0, "the committed WAL must not be empty");
    let reopened = SqliteStateStore::open(database.path()).unwrap();
    let actual = reopened.load(&scope(), &id("run")).await.unwrap();
    assert_eq!(
        serde_json::to_value(&actual.snapshot).unwrap(),
        expected["snapshot"]
    );
    assert_eq!(
        serde_json::to_value(&actual.session).unwrap(),
        expected["session"]
    );
    assert_eq!(
        serde_json::to_value(&actual.messages).unwrap(),
        expected["messages"]
    );
    assert_eq!(actual.snapshot.usage.model_calls, 1);
    assert_eq!(actual.snapshot.usage.tool_attempts, 1);
    assert_eq!(actual.snapshot.reservations.len(), 2);
    assert_eq!(
        actual
            .snapshot
            .system_inputs
            .as_ref()
            .unwrap()
            .snapshot_ref
            .revision,
        7
    );
    assert!(
        actual.snapshot.model_ledger[0]
            .reported_model_version
            .is_none()
    );
    for record in expected["records"].as_array().unwrap() {
        let reference: RecordRef = serde_json::from_value(record["reference"].clone()).unwrap();
        assert_eq!(
            reopened
                .read_record(&scope(), &reference)
                .await
                .unwrap()
                .value(),
            &record["value"]
        );
    }
    let events = reopened
        .read_events(&scope(), &id("run"), 0, 100)
        .await
        .unwrap();
    assert_eq!(events.events.len(), 5);
    assert_eq!(events.last_available_seq, 5);
    assert!(actual.session.active_run_id.is_none());
}

#[tokio::test]
async fn a_busy_writer_times_out_and_killing_an_uncommitted_writer_rolls_back_its_image() {
    let database = Database::new();
    let store =
        SqliteStateStore::open_with_busy_timeout(database.path(), Duration::from_millis(20))
            .unwrap();
    let initial = store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap()
        .state;
    let lock = Connection::open(database.path()).unwrap();
    lock.execute_batch("BEGIN IMMEDIATE").unwrap();
    let started = Instant::now();
    assert_eq!(
        store
            .acquire_lease(&scope(), &id("run"), &id("owner"), 0, 100)
            .await
            .unwrap_err()
            .code,
        ErrorCode::PersistenceUnavailable
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "busy timeout was not bounded"
    );
    lock.execute_batch("ROLLBACK").unwrap();
    let mut worker = workers::Worker::spawn(&database, "uncommitted", "uncommitted", json!({}));
    workers::wait(&worker.ready);
    // A WAL reader sees the previous committed image while the writer is uncommitted.
    assert_eq!(store.load(&scope(), &id("run")).await.unwrap(), initial);
    worker.terminate();
    let reopened = SqliteStateStore::open(database.path()).unwrap();
    assert_eq!(reopened.load(&scope(), &id("run")).await.unwrap(), initial);
    assert_eq!(
        reopened
            .read_events(&scope(), &id("run"), 0, 10)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn time_waiting_for_a_write_lock_cannot_revive_a_lease() {
    for renew in [false, true] {
        let database = Database::new();
        let store = Arc::new(
            SqliteStateStore::open_with_busy_timeout(database.path(), Duration::from_secs(2))
                .unwrap(),
        );
        let saved = store
            .admit(
                &scope(),
                durable_admission("run", "request", "session").await,
            )
            .await
            .unwrap()
            .state;
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 40)
            .await
            .unwrap();
        let request_time = lease.expires_at_ms - 40;
        let connection = Connection::open(database.path()).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let task = {
            let store = store.clone();
            let entered = entered.clone();
            tokio::spawn(async move {
                entered.notify_one();
                if renew {
                    store
                        .renew_lease(&scope(), &id("run"), &lease, request_time, 10_000)
                        .await
                        .map(|_| ())
                } else {
                    store
                        .commit(
                            &scope(),
                            &id("run"),
                            prepared(&saved.snapshot, lease, request_time),
                        )
                        .await
                        .map(|_| ())
                }
            })
        };
        entered.notified().await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        connection.execute_batch("ROLLBACK").unwrap();
        assert_eq!(task.await.unwrap().unwrap_err().code, ErrorCode::LeaseLost);
        assert_eq!(
            store
                .load(&scope(), &id("run"))
                .await
                .unwrap()
                .snapshot
                .revision,
            0
        );
    }
}

fn rewrite_checkpoint(database: &Database, change: impl FnOnce(&mut Value)) {
    let connection = Connection::open(database.path()).unwrap();
    let encoded: String = connection
        .query_row(
            "SELECT checkpoint_json FROM wickle_scope_checkpoints",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut value: Value = serde_json::from_str(&encoded).unwrap();
    change(&mut value);
    let checksum = canonical_digest(&value);
    assert_eq!(
        connection
            .execute(
                "UPDATE wickle_scope_checkpoints SET checkpoint_json=?1,checksum=?2",
                rusqlite::params![value.to_string(), checksum.as_str()]
            )
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn unknown_database_versions_and_corrupted_scope_images_are_rejected() {
    // This is a user table, not one of SQLite's reserved sqlite_* internal objects.
    let foreign = Database::new();
    let connection = Connection::open(foreign.path()).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE sqliteuser(value INTEGER); INSERT INTO sqliteuser VALUES (42);",
        )
        .unwrap();
    assert!(SqliteStateStore::open(foreign.path()).is_err());
    assert_eq!(
        connection
            .query_row("SELECT value FROM sqliteuser", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        42
    );

    for (statement, expected) in [
        (
            "PRAGMA user_version=999",
            ErrorCode::UnsupportedSchemaVersion,
        ),
        ("PRAGMA application_id=1", ErrorCode::InvalidContract),
        (
            "UPDATE wickle_metadata SET schema_version=999",
            ErrorCode::UnsupportedSchemaVersion,
        ),
    ] {
        let database = Database::new();
        drop(SqliteStateStore::open(database.path()).unwrap());
        Connection::open(database.path())
            .unwrap()
            .execute_batch(statement)
            .unwrap();
        assert_eq!(
            SqliteStateStore::open(database.path()).unwrap_err().code,
            expected
        );
    }
    let changed_schema = Database::new();
    let store = SqliteStateStore::open(changed_schema.path()).unwrap();
    store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap();
    drop(store);
    Connection::open(changed_schema.path())
        .unwrap()
        .execute_batch(
            "BEGIN IMMEDIATE;
         ALTER TABLE wickle_scope_checkpoints RENAME TO old_scope_checkpoints;
         CREATE TABLE wickle_scope_checkpoints (
             scope_key TEXT NOT NULL, checkpoint_json TEXT NOT NULL, checksum TEXT NOT NULL
         ) STRICT;
         INSERT INTO wickle_scope_checkpoints SELECT * FROM old_scope_checkpoints;
         DROP TABLE old_scope_checkpoints;
         COMMIT;",
        )
        .unwrap();
    assert!(
        SqliteStateStore::open(changed_schema.path()).is_err(),
        "a schema without its uniqueness constraint was accepted"
    );
    for fault in [
        "version",
        "scope",
        "duplicate_run",
        "duplicate_record",
        "duplicate_message",
        "duplicate_event",
        "record_body",
        "missing_record",
        "active_slot",
        "fence",
        "checksum",
    ] {
        let database = Database::new();
        let store = SqliteStateStore::open(database.path()).unwrap();
        store
            .admit(
                &scope(),
                durable_admission("run", "request", "session").await,
            )
            .await
            .unwrap();
        store
            .acquire_lease(&scope(), &id("run"), &id("owner"), 0, 10_000)
            .await
            .unwrap();
        if fault == "duplicate_event" {
            store
                .admit(
                    &scope(),
                    durable_admission("other", "other-request", "other-session").await,
                )
                .await
                .unwrap();
        }
        drop(store);
        rewrite_checkpoint(&database, |image| match fault {
            "version" => image["schema_version"] = json!("wickle.state-store.v999"),
            "scope" => image["scope"]["tenant_id"] = json!("foreign"),
            "duplicate_run" => {
                let copy = image["runs"][0].clone();
                image["runs"].as_array_mut().unwrap().push(copy);
            }
            "duplicate_record" => {
                let copy = image["records"][0].clone();
                image["records"].as_array_mut().unwrap().push(copy);
            }
            "duplicate_message" => {
                let mut copy = image["sessions"][0]["messages"][0].clone();
                copy["sequence"] = json!(2);
                image["sessions"][0]["messages"]
                    .as_array_mut()
                    .unwrap()
                    .push(copy);
                image["sessions"][0]["snapshot"]["transcript_revision"] = json!(2);
            }
            "duplicate_event" => {
                image["runs"][1]["events"][0]["event_id"] =
                    image["runs"][0]["events"][0]["event_id"].clone()
            }
            "record_body" => {
                let prompt = image["records"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|record| record["reference"]["record_id"] == "prompt-session")
                    .unwrap();
                prompt["value"]["instructions"] = json!("Changed instructions");
            }
            "missing_record" => image["records"]
                .as_array_mut()
                .unwrap()
                .retain(|record| record["reference"]["record_id"] != "prompt-session"),
            "active_slot" => {
                image["sessions"][0]["snapshot"]
                    .as_object_mut()
                    .unwrap()
                    .remove("active_run_id");
            }
            "fence" => image["runs"][0]["last_fencing_token"] = json!(0),
            "checksum" => {}
            _ => unreachable!(),
        });
        if fault == "checksum" {
            let wrong = canonical_digest(&json!("different image"));
            Connection::open(database.path())
                .unwrap()
                .execute(
                    "UPDATE wickle_scope_checkpoints SET checksum=?1",
                    [wrong.as_str()],
                )
                .unwrap();
        }
        let error = match SqliteStateStore::open(database.path()) {
            Err(error) => error,
            Ok(store) => store
                .load(&scope(), &id("run"))
                .await
                .expect_err("corrupt checkpoint was accepted"),
        };
        if fault == "version" {
            assert_eq!(error.code, ErrorCode::UnsupportedSchemaVersion);
        }
    }
}

#[tokio::test]
async fn an_open_handle_does_not_silently_replace_a_deleted_or_different_database() {
    let database = Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap();
    std::fs::remove_file(database.path()).unwrap();
    assert!(store.load(&scope(), &id("run")).await.is_err());
    assert!(
        !database.path().exists(),
        "a read recreated the deleted database"
    );
    let replacement = SqliteStateStore::open(database.path()).unwrap();
    replacement
        .admit(
            &scope(),
            durable_admission("replacement", "replacement", "session").await,
        )
        .await
        .unwrap();
    assert!(
        store.load(&scope(), &id("replacement")).await.is_err(),
        "old handle accepted a different store identity"
    );
    assert!(replacement.load(&scope(), &id("replacement")).await.is_ok());
}

#[test]
#[ignore = "Subprocess fixture; only parent process tests supply its required input"]
fn process_worker() {
    workers::run();
}

#[tokio::test]
async fn warmed_checkpoints_observe_external_updates_and_revalidate_changed_bytes() {
    let database = Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    let initial = store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap()
        .state;
    let cloned = store.clone();
    assert_eq!(cloned.load(&scope(), &id("run")).await.unwrap(), initial);
    let other = SqliteStateStore::open(database.path()).unwrap();
    other
        .admit(
            &scope(),
            durable_admission("other", "request-2", "session-2").await,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .find_request(&scope(), &id("session-2"), &id("request-2"))
            .await
            .unwrap()
            .unwrap()
            .snapshot
            .run_id,
        id("other")
    );
    let connection = Connection::open(database.path()).unwrap();
    let (original, checksum): (String, String) = connection
        .query_row(
            "SELECT checkpoint_json,checksum FROM wickle_scope_checkpoints",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let mut corrupted: Value = serde_json::from_str(&original).unwrap();
    corrupted["schema_version"] = json!("unknown-checkpoint-format");
    // Even a correctly recomputed outer digest cannot skip full validation.
    connection
        .execute(
            "UPDATE wickle_scope_checkpoints SET checkpoint_json=?1,checksum=?2",
            rusqlite::params![corrupted.to_string(), canonical_digest(&corrupted).as_str()],
        )
        .unwrap();
    assert!(store.load(&scope(), &id("run")).await.is_err());
    assert!(cloned.load(&scope(), &id("run")).await.is_err());
    connection
        .execute(
            "UPDATE wickle_scope_checkpoints SET checkpoint_json=?1,checksum=?2",
            rusqlite::params![original, checksum],
        )
        .unwrap();
    assert_eq!(store.load(&scope(), &id("run")).await.unwrap(), initial);
    let mut foreign = scope();
    foreign.workspace_id = id("foreign");
    assert!(store.load(&foreign, &id("run")).await.is_err());
}

#[tokio::test]
async fn a_failed_sql_write_never_publishes_mutated_cached_lease_state() {
    let database = Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    let initial = store
        .admit(
            &scope(),
            durable_admission("run", "request", "session").await,
        )
        .await
        .unwrap()
        .state;
    assert_eq!(store.load(&scope(), &id("run")).await.unwrap(), initial);
    let connection = Connection::open(database.path()).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_write BEFORE UPDATE ON wickle_scope_checkpoints BEGIN SELECT RAISE(ABORT,'injected commit failure'); END;").unwrap();
    assert!(
        store
            .acquire_lease(&scope(), &id("run"), &id("failed-owner"), 0, 10_000)
            .await
            .is_err()
    );
    connection
        .execute_batch("DROP TRIGGER reject_write;")
        .unwrap();
    // An uncommitted lease must not survive under the previous row's cache key.
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("next-owner"), 1, 10_000)
        .await
        .unwrap();
    store
        .check_lease(&scope(), &id("run"), &lease, 2)
        .await
        .unwrap();
    assert_eq!(store.load(&scope(), &id("run")).await.unwrap(), initial);
}

#[tokio::test]
async fn killing_an_uncommitted_legacy_upgrade_preserves_old_data_and_allows_one_later_upgrade() {
    let database = Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    let initial = store
        .admit(
            &scope(),
            core::admission("legacy", "legacy-request", "legacy-session", "old", "1").await,
        )
        .await
        .unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("legacy"), &id("owner"), 0, 1000)
        .await
        .unwrap();
    let terminal = store
        .commit(
            &scope(),
            &id("legacy"),
            finished(&initial.state.snapshot, lease, 1),
        )
        .await
        .unwrap();
    drop(store);
    let connection = Connection::open(database.path()).unwrap();
    let text: String = connection
        .query_row(
            "SELECT checkpoint_json FROM wickle_scope_checkpoints",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut legacy: Value = serde_json::from_str(&text).unwrap();
    legacy["schema_version"] = json!("wickle.state-store.v1");
    legacy.as_object_mut().unwrap().remove("executions");
    connection
        .execute(
            "UPDATE wickle_scope_checkpoints SET checkpoint_json=?1,checksum=?2",
            rusqlite::params![legacy.to_string(), canonical_digest(&legacy).as_str()],
        )
        .unwrap();
    // The real shared state engine builds the upgrade image. The subprocess
    // pauses its SQLite image write before COMMIT, without a production test hook.
    let memory = MemoryStateStore::from_checkpoint(
        StateStoreCheckpoint::from_json(&legacy.to_string(), &scope(), &canonical_digest(&legacy))
            .unwrap(),
    );
    let input = core::admission("new", "new-request", "new-session", "new", "1").await;
    memory.admit(&scope(), input.clone()).await.unwrap();
    let upgraded = serde_json::to_value(memory.export_checkpoint(&scope()).unwrap()).unwrap();
    assert_eq!(upgraded["schema_version"], "wickle.state-store.v2");
    let old_reader = SqliteStateStore::open(database.path()).unwrap();
    let mut worker = workers::Worker::spawn(
        &database,
        "upgrade",
        "uncommitted-upgrade",
        json!({"image":upgraded}),
    );
    workers::wait(&worker.ready);
    assert_eq!(
        old_reader.load(&scope(), &id("legacy")).await.unwrap(),
        terminal
    );
    worker.terminate();
    drop(old_reader);
    let after_kill: String = connection
        .query_row(
            "SELECT checkpoint_json FROM wickle_scope_checkpoints",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(after_kill, legacy.to_string());
    let reopened = SqliteStateStore::open(database.path()).unwrap();
    let accepted = reopened.admit(&scope(), input.clone()).await.unwrap();
    assert!(accepted.created);
    let replay = reopened.admit(&scope(), input).await.unwrap();
    assert!(!replay.created);
    assert_eq!(accepted.state, replay.state);
    assert_eq!(
        reopened.load(&scope(), &id("legacy")).await.unwrap(),
        terminal
    );
    let after_commit: String = connection
        .query_row(
            "SELECT checkpoint_json FROM wickle_scope_checkpoints",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&after_commit).unwrap(),
        upgraded
    );
}
