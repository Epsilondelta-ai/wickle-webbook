# 17장 전체 Rust 구현과 테스트

[강의로](../17-resume.md) · [전체 변경 패치](../solutions/17-resume.patch)

기준 `d33573ed8be1f811f75a6dc116ea908a69917cd8`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle-state-sqlite/tests/state_store.rs`

```rust
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
```

## `crates/wickle/src/agent.rs`

```rust
use crate::*;
use futures_util::{FutureExt, stream};
use std::{
    collections::BTreeMap,
    fmt,
    num::NonZeroU64,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

mod admission;
mod driver;
mod resume;
mod tools;

/// Host tokenizer or conservative estimator. This synchronous callback must not
/// perform I/O; returned tokens are estimates, not provider-reported usage.
pub trait ModelTokenEstimator: Send + Sync {
    /// Estimate the complete prepared request for its exact route.
    fn estimate(&self, request: &ModelRequest) -> Result<u64, ContractError>;
}

/// Finite runtime bounds, independent of the profile's total execution budgets.
#[derive(Debug, Clone)]
pub struct AgentSettings {
    /// Lease duration renewed by the detached driver.
    pub lease_ttl_ms: u64,
    /// Renewal interval; at most one third of the lease duration.
    pub heartbeat_interval_ms: u64,
    /// Maximum delay between durable observer polls.
    pub observer_poll_ms: u64,
    /// Maximum events read per page.
    pub event_page_size: usize,
    /// Deadline for admission preparation callbacks, before durable admission.
    pub start_timeout_ms: u64,
    /// Maximum serialized RunRequest bytes.
    pub max_request_bytes: usize,
    /// Reserved output-token limit for the initial text-model call.
    pub max_output_tokens: NonZeroU64,
    /// Model request and response bounds.
    pub response_limits: ModelResponseLimits,
    /// Context byte/item bounds, distinct from token estimates.
    pub projection_limits: ProjectionLimits,
    /// Per-tool callback and receipt limits; total attempts still use RunLimits.
    pub tool_execution_limits: ToolExecutionLimits,
    /// Require a durable StateStore at admission.
    pub require_durable: bool,
}
impl Default for AgentSettings {
    fn default() -> Self {
        Self {
            lease_ttl_ms: 30_000,
            heartbeat_interval_ms: 5_000,
            observer_poll_ms: 100,
            event_page_size: 64,
            start_timeout_ms: 30_000,
            max_request_bytes: 1_048_576,
            max_output_tokens: NonZeroU64::new(1024).expect("positive default"),
            response_limits: ModelResponseLimits {
                max_input_bytes: 1_048_576,
                max_response_bytes: 262_144,
                max_delta_bytes: 65_536,
                max_events: 4096,
                max_tool_calls: 16,
            },
            projection_limits: ProjectionLimits {
                max_bytes: 1_048_576,
                max_items: 1024,
            },
            tool_execution_limits: ToolExecutionLimits::default(),
            require_durable: false,
        }
    }
}
impl AgentSettings {
    /// Validate finite bounds without calling a runtime component.
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.lease_ttl_ms == 0
            || self.lease_ttl_ms > 86_400_000
            || self.heartbeat_interval_ms == 0
            || self.heartbeat_interval_ms > self.lease_ttl_ms / 3
            || self.observer_poll_ms == 0
            || self.observer_poll_ms > 60_000
            || self.start_timeout_ms == 0
            || self.start_timeout_ms > 86_400_000
            || self.event_page_size == 0
            || self.event_page_size > MAX_EVENT_PAGE_SIZE
            || self.max_request_bytes == 0
            || self.projection_limits.max_bytes == 0
            || self.projection_limits.max_items == 0
            || self.response_limits.max_input_bytes == 0
            || self.response_limits.max_response_bytes == 0
            || self.response_limits.max_delta_bytes == 0
            || self.response_limits.max_events == 0
            || self.tool_execution_limits.timeout_ms == 0
            || self.tool_execution_limits.timeout_ms > 86_400_000
            || self.tool_execution_limits.max_receipt_bytes == 0
        {
            return Err(fail(ErrorCode::InvalidConfiguration, "agent.settings"));
        }
        Ok(())
    }
}

/// Already-created Host components for one exact scope. Creating an Agent does
/// not invoke these ports, open connections, start tasks or read environment data.
pub struct AgentBindings {
    /// Fixed tenant/workspace/user namespace; validated against routing at start.
    pub scope: Scope,
    /// Durable or explicitly process-local state implementation.
    pub state: Arc<dyn StateStore>,
    /// Current authorization gate.
    pub policy: Arc<PolicyGate>,
    /// Approved profile metadata resolver, called only for new requests.
    pub profile_resolver: Arc<dyn ProfileResolver>,
    /// Configured model exchange with dispatcher and route inspector.
    pub model_exchange: Arc<ModelExchange>,
    /// Exact catalog and policy snapshot for newly admitted runs.
    pub router: Arc<dyn ModelRouter>,
    /// Trusted instructions pinned in the session prefix.
    pub host_instructions: Vec<String>,
    /// Registered system-input metadata; values arrive through ExecutionContext.
    pub system_inputs: SystemInputRegistry,
    /// Existing tool executors and compiled contracts, restricted to this scope.
    pub tools: Option<Arc<ToolRegistry>>,
    /// Optional read-only source for registered resolver-owned system inputs.
    pub system_input_resolver: Option<Arc<dyn SystemInputResolver>>,
    /// Optional read-only verifier for externally supplied effect receipts.
    pub external_receipt_verifier: Option<Arc<dyn ExternalReceiptVerifier>>,
    /// Time source and timers.
    pub clock: Arc<dyn Clock>,
    /// New internal run/message/event identities, never business foreign keys.
    pub ids: Arc<dyn IdSource>,
    /// Route-specific token estimate callback.
    pub token_estimator: Arc<dyn ModelTokenEstimator>,
    /// Finite runtime limits.
    pub settings: AgentSettings,
}

/// Scope-bound Agent facade. Clone shares local driver ownership and observations.
#[derive(Clone)]
pub struct Agent {
    inner: Arc<Inner>,
}
struct Inner {
    profile: AgentProfile,
    bindings: AgentBindings,
    runs: Mutex<BTreeMap<Id, Arc<LocalRun>>>,
}
struct LocalRun {
    segment_start_revision: u64,
    cancel: CancellationToken,
    reason: Mutex<Option<Id>>,
    error: Mutex<Option<ContractError>>,
    done: AtomicBool,
    notify: Notify,
}
impl LocalRun {
    fn new(segment_start_revision: u64) -> Self {
        Self {
            segment_start_revision,
            cancel: CancellationToken::new(),
            reason: Mutex::new(None),
            error: Mutex::new(None),
            done: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }
}
impl fmt::Debug for Agent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Agent")
            .field("agent_id", &self.inner.profile.agent_id)
            .finish_non_exhaustive()
    }
}

/// Validate the initial text/turn-end runtime without invoking any Host callback.
/// Catalog tools use already-created executors. Asset loaders, adapter exports,
/// verifiers and extension execution require their separate runtime bindings.
pub fn create_agent(
    profile: AgentProfile,
    bindings: AgentBindings,
) -> Result<Agent, ContractError> {
    profile.validate_structure()?;
    bindings.settings.validate()?;
    if !matches!(profile.instructions, Instructions::Text(_))
        || !matches!(profile.output_contract, OutputContract::Text {})
        || !matches!(profile.completion_policy, CompletionPolicy::TurnEnd {})
        || !profile.skills.is_empty()
        || !profile.connectors.is_empty()
        || profile.adapters.as_ref().is_some_and(|v| !v.is_empty())
        || profile.hooks.as_ref().is_some_and(|v| !v.is_empty())
        || profile
            .context_sources
            .as_ref()
            .is_some_and(|v| !v.is_empty())
        || profile.extensions.as_ref().is_some_and(|v| !v.is_empty())
        || profile.context_policy.strategy.as_str() != "bounded"
    {
        return Err(fail(ErrorCode::CapabilityUnsupported, "agent.profile"));
    }
    match &bindings.tools {
        Some(registry) => {
            if registry.scope() != &bindings.scope {
                return Err(fail(ErrorCode::AccessDenied, "agent.tools_scope"));
            }
            registry.prompt_bindings(&profile)?;
        }
        None if !profile.tools.is_empty() => {
            return Err(fail(ErrorCode::CapabilityUnsupported, "agent.tools"));
        }
        None => {}
    }
    Ok(Agent {
        inner: Arc::new(Inner {
            profile,
            bindings,
            runs: Mutex::new(BTreeMap::new()),
        }),
    })
}

/// A durable observer. Dropping this value or its streams does not cancel the driver.
#[derive(Clone)]
pub struct RunHandle {
    agent: Agent,
    run_id: Id,
    segment_start_revision: u64,
    local: Option<Arc<LocalRun>>,
}
impl fmt::Debug for RunHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunHandle")
            .field("run_id", &self.run_id)
            .finish_non_exhaustive()
    }
}

/// Result of an authorized cancellation request, separate from stored RunOutcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReceipt {
    /// Signalled this process's live driver. Cancellation is not yet committed.
    Requested,
    /// The saved run is already terminal; its outcome was not changed.
    AlreadyTerminal,
    /// No local driver is owned here. No remote cancellation was accepted or sent.
    NotLocal,
}

impl Agent {
    /// Admit through an owned coordinator. Caller-future disconnection after polling
    /// does not abort durable admission or its detached driver.
    pub async fn start(
        &self,
        request: RunRequest,
        context: ExecutionContext,
    ) -> Result<Guarded<RunHandle>, ContractError> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| fail(ErrorCode::RuntimeUnavailable, "agent.runtime"))?;
        let agent = self.clone();
        runtime
            .spawn(async move { agent.admit(request, context).await })
            .await
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.coordinator"))?
    }
    /// Read minimal saved metadata under current permission.
    pub async fn get_run(
        &self,
        run_id: &Id,
        context: &ExecutionContext,
    ) -> Result<Guarded<RunView>, ContractError> {
        self.check_scope(context)?;
        let saved = caller_read(
            context,
            None,
            self.inner
                .bindings
                .state
                .load(&self.inner.bindings.scope, run_id),
        )
        .await?;
        self.inner
            .bindings
            .policy
            .run_view(&saved.snapshot, context, None)
            .await
    }
    /// Read protected saved state under the separate details permission.
    pub async fn get_run_details(
        &self,
        run_id: &Id,
        context: &ExecutionContext,
    ) -> Result<Guarded<RunSnapshot>, ContractError> {
        self.check_scope(context)?;
        let saved = caller_read(
            context,
            None,
            self.inner
                .bindings
                .state
                .load(&self.inner.bindings.scope, run_id),
        )
        .await?;
        self.inner
            .bindings
            .policy
            .run_details(&saved.snapshot, context, None)
            .await
    }
    /// Consume one authorized, fixed wait decision in an owned coordinator.
    /// A duplicate command returns its existing segment without restarting work.
    pub async fn resume(
        &self,
        command: ResumeCommand,
        context: ExecutionContext,
    ) -> Result<Guarded<RunHandle>, ContractError> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| fail(ErrorCode::RuntimeUnavailable, "agent.runtime"))?;
        let agent = self.clone();
        runtime
            .spawn(async move { agent.resume_command(command, context).await })
            .await
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.resume"))?
    }
    fn check_scope(&self, context: &ExecutionContext) -> Result<(), ContractError> {
        if context.data.scope != self.inner.bindings.scope {
            return Err(fail(ErrorCode::AccessDenied, "scope"));
        }
        Ok(())
    }
    fn handle(&self, run_id: Id, segment_start_revision: u64) -> Result<RunHandle, ContractError> {
        let local = self
            .inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .get(&run_id)
            .filter(|local| local.segment_start_revision == segment_start_revision)
            .cloned();
        Ok(RunHandle {
            agent: self.clone(),
            run_id,
            segment_start_revision,
            local,
        })
    }
}

impl RunHandle {
    /// Stable saved run identity.
    pub fn run_id(&self) -> &Id {
        &self.run_id
    }
    /// Wait for an authorized saved outcome. Observer cancellation never cancels execution.
    pub async fn outcome(
        &self,
        context: &ExecutionContext,
    ) -> Result<Guarded<RunOutcome>, ContractError> {
        loop {
            let snapshot = match self.agent.get_run_details(&self.run_id, context).await? {
                Guarded::Completed(snapshot) => snapshot,
                Guarded::ApprovalRequired(challenge) => {
                    return Ok(Guarded::ApprovalRequired(challenge));
                }
            };
            if let Some(receipt) = snapshot.resume_receipts.iter().find(|receipt| {
                receipt.previous_segment_start_revision == self.segment_start_revision
            }) {
                let record = caller_read(
                    context,
                    None,
                    self.agent
                        .inner
                        .bindings
                        .state
                        .read_record(&snapshot.scope, &receipt.previous_outcome_ref),
                )
                .await?;
                if record.reference() != &receipt.previous_outcome_ref {
                    return Err(fail(ErrorCode::InvalidSnapshot, "agent.segment_reference"));
                }
                let outcome = serde_json::from_value(record.value().clone())
                    .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.segment_outcome"))?;
                let request = PolicyRequest {
                    owner_scope: snapshot.scope.clone(),
                    resource_id: self.run_id.clone(),
                    action: PolicyAction::ReadRunDetails {},
                };
                return self
                    .agent
                    .inner
                    .bindings
                    .policy
                    .guard(&request, context, None, None, || async { Ok(outcome) })
                    .await;
            }
            if segment_revision(&snapshot) != self.segment_start_revision {
                return Err(fail(ErrorCode::InvalidSnapshot, "agent.segment"));
            }
            if let Some(outcome) = snapshot.outcome {
                return Ok(Guarded::Completed(outcome));
            }
            self.local_error()?;
            self.wait(context).await?;
        }
    }
    /// Replay durable event metadata with fresh permission checks on every page
    /// and event. Polling has no channel backpressure on execution.
    pub fn events(
        &self,
        after_seq: u64,
        context: ExecutionContext,
    ) -> PortStream<'static, EventView> {
        let handle = self.clone();
        Box::pin(stream::try_unfold(
            (handle, context, after_seq, Vec::<RunEvent>::new()),
            |(handle, context, mut cursor, mut pending)| async move {
                loop {
                    if !pending.is_empty() {
                        let event = pending.remove(0);
                        let saved = caller_read(
                            &context,
                            None,
                            handle
                                .agent
                                .inner
                                .bindings
                                .state
                                .load(&handle.agent.inner.bindings.scope, &handle.run_id),
                        )
                        .await?;
                        if handle
                            .segment_end(&saved.snapshot)?
                            .is_some_and(|end| event.seq.get() > end)
                        {
                            return Ok(None);
                        }
                        let event = match handle
                            .agent
                            .inner
                            .bindings
                            .policy
                            .event_view(&event, &context, None)
                            .await?
                        {
                            Guarded::Completed(event) => event,
                            Guarded::ApprovalRequired(_) => {
                                return Err(fail(ErrorCode::AccessDenied, "agent.events.approval"));
                            }
                        };
                        cursor = event.seq.get();
                        return Ok(Some((event, (handle, context, cursor, pending))));
                    }
                    handle.agent.check_scope(&context)?;
                    let bindings = &handle.agent.inner.bindings;
                    let policy = PolicyRequest {
                        owner_scope: bindings.scope.clone(),
                        resource_id: handle.run_id.clone(),
                        action: PolicyAction::ReadEvents {},
                    };
                    match bindings
                        .policy
                        .guard(&policy, &context, None, None, || {
                            caller_read(
                                &context,
                                None,
                                bindings.state.read_events(
                                    &bindings.scope,
                                    &handle.run_id,
                                    cursor,
                                    bindings.settings.event_page_size,
                                ),
                            )
                        })
                        .await?
                    {
                        Guarded::ApprovalRequired(_) => {
                            return Err(fail(ErrorCode::AccessDenied, "agent.events.approval"));
                        }
                        Guarded::Completed(page) => {
                            pending = page.events;
                        }
                    }
                    if !pending.is_empty() {
                        continue;
                    }
                    let saved = caller_read(
                        &context,
                        None,
                        bindings.state.load(&bindings.scope, &handle.run_id),
                    )
                    .await?;
                    if let Some(end) = handle.segment_end(&saved.snapshot)? {
                        if cursor >= end {
                            return Ok(None);
                        }
                        continue;
                    }
                    handle.local_error()?;
                    handle.wait(&context).await?;
                }
            },
        ))
    }
    /// Signal only a locally owned driver after current CancelRun authorization.
    pub async fn cancel(
        &self,
        reason: Id,
        context: &ExecutionContext,
    ) -> Result<Guarded<CancelReceipt>, ContractError> {
        self.agent.check_scope(context)?;
        let bindings = &self.agent.inner.bindings;
        let saved = caller_read(
            context,
            None,
            bindings.state.load(&bindings.scope, &self.run_id),
        )
        .await?;
        let policy = PolicyRequest {
            owner_scope: saved.snapshot.scope,
            resource_id: self.run_id.clone(),
            action: PolicyAction::CancelRun {},
        };
        bindings
            .policy
            .guard(&policy, context, None, None, || async {
                if saved.snapshot.status.is_terminal() {
                    return Ok(CancelReceipt::AlreadyTerminal);
                }
                if saved.snapshot.status == RunStatus::Waiting {
                    return self
                        .agent
                        .cancel_waiting(self.run_id.clone(), reason, context.clone())
                        .await;
                }
                let current = self
                    .agent
                    .inner
                    .runs
                    .lock()
                    .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
                    .get(&self.run_id)
                    .cloned();
                if let Some(local) = current {
                    if !local.done.load(Ordering::Acquire) {
                        *local
                            .reason
                            .lock()
                            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.cancel"))? =
                            Some(reason);
                        local.cancel.cancel();
                        return Ok(CancelReceipt::Requested);
                    }
                }
                Ok(CancelReceipt::NotLocal)
            })
            .await
    }
    fn local_error(&self) -> Result<(), ContractError> {
        if let Some(local) = self.current_local()? {
            if local.done.load(Ordering::Acquire) {
                if let Some(error) = local
                    .error
                    .lock()
                    .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
                    .clone()
                {
                    return Err(error);
                }
            }
        }
        Ok(())
    }
    fn current_local(&self) -> Result<Option<Arc<LocalRun>>, ContractError> {
        if let Some(local) = &self.local {
            return Ok(Some(local.clone()));
        }
        Ok(self
            .agent
            .inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .get(&self.run_id)
            .filter(|local| local.segment_start_revision == self.segment_start_revision)
            .cloned())
    }
    fn segment_end(&self, snapshot: &RunSnapshot) -> Result<Option<u64>, ContractError> {
        if let Some(receipt) = snapshot
            .resume_receipts
            .iter()
            .find(|receipt| receipt.previous_segment_start_revision == self.segment_start_revision)
        {
            return Ok(Some(receipt.previous_last_event_seq));
        }
        if segment_revision(snapshot) != self.segment_start_revision {
            return Err(fail(ErrorCode::InvalidSnapshot, "agent.segment"));
        }
        Ok(
            (snapshot.status.is_terminal() || snapshot.status == RunStatus::Waiting)
                .then_some(snapshot.last_event_seq),
        )
    }
    async fn wait(&self, context: &ExecutionContext) -> Result<(), ContractError> {
        tokio::select! { biased;
            _ = context.cancellation.cancelled() => Err(fail(ErrorCode::Cancelled, "agent.observer")),
            _ = tokio::time::sleep(Duration::from_millis(self.agent.inner.bindings.settings.observer_poll_ms)) => Ok(()),
        }
    }
}

fn fail(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
fn segment_revision(snapshot: &RunSnapshot) -> u64 {
    snapshot
        .resume_receipts
        .last()
        .map_or(0, |receipt| receipt.accepted_revision)
}

// Only read-only caller operations use this helper. Cancelling a read drops its
// future without signalling the independent driver or cancelling a durable write.
async fn caller_read<T>(
    context: &ExecutionContext,
    timeout: Option<Duration>,
    future: impl std::future::Future<Output = Result<T, ContractError>>,
) -> Result<T, ContractError> {
    let deadline = async {
        match timeout {
            Some(timeout) => tokio::time::sleep(timeout).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::select! { biased;
        _ = context.cancellation.cancelled() => Err(fail(ErrorCode::Cancelled, "agent.read")),
        _ = deadline => Err(fail(ErrorCode::DeadlineExceeded, "agent.read")),
        result = future => result,
    }
}
```

## `crates/wickle/src/agent/admission.rs`

```rust
use super::*;
use std::panic::AssertUnwindSafe;

impl Agent {
    pub(super) async fn admit(
        &self,
        request: RunRequest,
        context: ExecutionContext,
    ) -> Result<Guarded<RunHandle>, ContractError> {
        self.check_scope(&context)?;
        let bindings = &self.inner.bindings;
        if serde_json::to_vec(&request)
            .map_err(|_| fail(ErrorCode::InvalidJson, "agent.request"))?
            .len()
            > bindings.settings.max_request_bytes
        {
            return Err(fail(ErrorCode::InvalidContract, "agent.request_size"));
        }
        if request
            .output_contract
            .as_ref()
            .is_some_and(|value| !matches!(value, OutputContract::Text {}))
            || request
                .input
                .iter()
                .any(|content| !matches!(content, InputContent::Text { .. }))
        {
            return Err(fail(ErrorCode::CapabilityUnsupported, "agent.request"));
        }
        let policy = PolicyRequest {
            owner_scope: bindings.scope.clone(),
            resource_id: request.request_id.clone(),
            action: PolicyAction::StartRun {},
        };
        if let Guarded::ApprovalRequired(challenge) = bindings
            .policy
            .guard(&policy, &context, None, None, || async { Ok(()) })
            .await?
        {
            return Ok(Guarded::ApprovalRequired(challenge));
        }
        let read_timeout = Some(Duration::from_millis(bindings.settings.start_timeout_ms));
        if let Some(saved) = caller_read(
            &context,
            read_timeout,
            bindings
                .state
                .find_request(&bindings.scope, &request.session_id, &request.request_id),
        )
        .await?
        {
            caller_read(
                &context,
                read_timeout,
                self.validate_replay(&request, &context, &saved),
            )
            .await?;
            let segment = segment_revision(&saved.snapshot);
            return Ok(Guarded::Completed(
                self.handle(saved.snapshot.run_id, segment)?,
            ));
        }
        // Preparation may be cancelled or time out. Once durable admission begins,
        // this owned coordinator waits for its result even if the caller disconnects.
        let prepared = AssertUnwindSafe(self.prepare(request.clone(), &context)).catch_unwind();
        let (input, prompt) = tokio::select! { biased;
            _ = context.cancellation.cancelled() => return Err(fail(ErrorCode::Cancelled, "agent.admission")),
            _ = tokio::time::sleep(Duration::from_millis(bindings.settings.start_timeout_ms)) => return Err(fail(ErrorCode::DeadlineExceeded, "agent.admission")),
            result = prepared => result.map_err(|_| fail(ErrorCode::InvalidContract, "agent.preparation"))??,
        };
        // Current admission permission is checked again after metadata preparation.
        if let Guarded::ApprovalRequired(challenge) = bindings
            .policy
            .guard(&policy, &context, None, None, || async { Ok(()) })
            .await?
        {
            return Ok(Guarded::ApprovalRequired(challenge));
        }
        let candidate_id = input.snapshot.run_id.clone();
        let admission = match AssertUnwindSafe(bindings.state.admit(&bindings.scope, input))
            .catch_unwind()
            .await
        {
            Ok(result) => result,
            Err(_) => Err(fail(ErrorCode::InvalidContract, "agent.admission")),
        };
        let result = match admission {
            Ok(result) => result,
            Err(original) => {
                // A lost commit acknowledgement must not leave our admitted run
                // without a driver or create a second request on retry.
                match bindings
                    .state
                    .find_request(&bindings.scope, &request.session_id, &request.request_id)
                    .await
                {
                    Ok(Some(saved)) => {
                        self.validate_replay(&request, &context, &saved).await?;
                        AdmissionResult {
                            created: saved.snapshot.run_id == candidate_id,
                            state: saved,
                        }
                    }
                    _ => return Err(original),
                }
            }
        };
        if !result.created {
            self.validate_replay(&request, &context, &result.state)
                .await?;
            return Ok(Guarded::Completed(self.handle(
                result.state.snapshot.run_id.clone(),
                segment_revision(&result.state.snapshot),
            )?));
        }
        let run_id = result.state.snapshot.run_id;
        let local = Arc::new(LocalRun::new(0));
        self.inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .insert(run_id.clone(), local.clone());
        let agent = self.clone();
        let driver_id = run_id.clone();
        let driver_local = local.clone();
        let mut data = context.data;
        // Runtime tool values remain in protected storage. The model driver has
        // no reason to carry the admission map into model callbacks.
        data.system_inputs = None;
        let driver_context = ExecutionContext::new(data, local.cancel.clone());
        tokio::spawn(async move {
            let result =
                AssertUnwindSafe(agent.drive(&driver_id, prompt, driver_context, &driver_local))
                    .catch_unwind()
                    .await;
            let error = match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(fail(error.code, "agent.driver")),
                Err(_) => Some(fail(ErrorCode::InvalidContract, "agent.driver")),
            };
            let completed = error.is_none();
            if let Ok(mut saved) = driver_local.error.lock() {
                *saved = error;
            }
            driver_local.done.store(true, Ordering::Release);
            driver_local.notify.notify_waiters();
            if completed {
                if let Ok(mut runs) = agent.inner.runs.lock() {
                    if runs
                        .get(&driver_id)
                        .is_some_and(|current| Arc::ptr_eq(current, &driver_local))
                    {
                        runs.remove(&driver_id);
                    }
                }
            }
        });
        Ok(Guarded::Completed(RunHandle {
            agent: self.clone(),
            run_id,
            segment_start_revision: 0,
            local: Some(local),
        }))
    }

    async fn validate_replay(
        &self,
        request: &RunRequest,
        context: &ExecutionContext,
        saved: &StoredRun,
    ) -> Result<(), ContractError> {
        if self.inner.profile.digest() != *saved.snapshot.profile.profile_digest()
            || admission_digest(
                request,
                &saved.snapshot.profile,
                saved.snapshot.system_inputs.as_ref(),
            ) != saved.snapshot.request_digest
        {
            return Err(fail(ErrorCode::RequestConflict, "agent.request"));
        }
        if let Some(reference) = &saved.snapshot.system_inputs {
            let record = self
                .inner
                .bindings
                .state
                .read_record(&self.inner.bindings.scope, &reference.snapshot_ref)
                .await?;
            let values =
                RunSystemInputs::from_value(record.value(), reference, &saved.snapshot.scope)?;
            // start omission means empty input. Only resume may reuse saved values
            // through an omitted map, and this path handles start replay exclusively.
            let empty = SystemInputs::default();
            values.validate_resume(Some(context.data.system_inputs.as_ref().unwrap_or(&empty)))?;
        } else if context
            .data
            .system_inputs
            .as_ref()
            .is_some_and(|values| !values.values().is_empty())
        {
            return Err(fail(ErrorCode::SystemInputsMismatch, "agent.system_inputs"));
        }
        Ok(())
    }

    async fn prepare(
        &self,
        request: RunRequest,
        context: &ExecutionContext,
    ) -> Result<(AdmissionInput, PromptSnapshot), ContractError> {
        let bindings = &self.inner.bindings;
        let routing = bindings.router.snapshot().clone();
        if routing.scope() != &bindings.scope {
            return Err(fail(ErrorCode::AccessDenied, "agent.router_scope"));
        }
        let profile = ProfileValidator::new(bindings.profile_resolver.as_ref())
            .validate(&self.inner.profile, &bindings.scope)
            .await?;
        let tool_bindings = bindings
            .tools
            .as_ref()
            .map(|tools| tools.prompt_bindings(profile.profile()))
            .transpose()?
            .unwrap_or_default();
        let session = match bindings
            .state
            .load_session(&bindings.scope, &request.session_id)
            .await
        {
            Ok(session) => Some(session),
            Err(error) if error.code == ErrorCode::StateNotFound => None,
            Err(error) => return Err(error),
        };
        let (prompt, prompt_record, sequence) = if let Some(session) = session {
            let record = bindings
                .state
                .read_record(&bindings.scope, &session.prompt_snapshot)
                .await?;
            let prompt = PromptSnapshot::restore(
                &serde_json::to_string(record.value())
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.prompt"))?,
                &record.reference().digest,
                &profile,
                &bindings.scope,
            )?;
            (
                prompt,
                record,
                session.transcript_revision.checked_add(1).ok_or_else(|| {
                    fail(ErrorCode::InvalidSnapshot, "session.transcript_revision")
                })?,
            )
        } else {
            let prompt = PromptSnapshot::create(
                &profile,
                bindings.host_instructions.clone(),
                None,
                tool_bindings.clone(),
                vec![],
            )?;
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(&prompt)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.prompt"))?,
            );
            (prompt, record, 1)
        };
        if prompt.tools().len() != tool_bindings.len()
            || prompt
                .tools()
                .iter()
                .zip(&tool_bindings)
                .any(|(pinned, binding)| {
                    pinned.selection != binding.selection
                        || pinned.compiled_digest != *binding.compiled.digest()
                        || pinned.descriptor_digest != *binding.compiled.descriptor_digest()
                        || pinned.model_tool != binding.compiled.to_model_tool()
                })
        {
            return Err(fail(ErrorCode::ContextMismatch, "agent.pinned_tools"));
        }
        let inputs = RunSystemInputs::capture(
            bindings.scope.clone(),
            context.data.system_inputs.clone(),
            &bindings.system_inputs,
        )?;
        let inputs_record = inputs.to_record(bindings.ids.next_id()?, 1);
        let inputs_ref = inputs.snapshot_ref(inputs_record.reference())?;
        let request_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(&request)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.request"))?,
        );
        let routing_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(&routing)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.routing"))?,
        );
        let run_id = bindings.ids.next_id()?;
        let now = bindings.clock.now()?.utc_ms;
        let snapshot = RunSnapshot {
            schema_version: RunSnapshotSchemaVersion::V1,
            run_id: run_id.clone(),
            request_digest: admission_digest(&request, &profile, Some(&inputs_ref)),
            request: request.clone(),
            scope: bindings.scope.clone(),
            limits: profile.profile().limits.clone(),
            timing: RunTiming::new(now, profile.profile().limits.max_elapsed_ms.get())?,
            profile,
            status: RunStatus::Running,
            phase: RunPhase::Admission,
            model_step_id: None,
            usage: BudgetUsage::default(),
            reservations: vec![],
            model_ledger: vec![],
            tool_ledger: vec![],
            system_inputs: Some(inputs_ref),
            wait: None,
            outcome: None,
            assembly_ref: None,
            routing_snapshot_ref: Some(routing_record.reference().clone()),
            context_batches: vec![],
            source_states: vec![],
            revision: 0,
            resume_receipts: vec![],
            last_event_seq: 1,
        };
        let message = Message {
            message_id: bindings.ids.next_id()?,
            run_id: run_id.clone(),
            sequence: sequence
                .try_into()
                .map_err(|_| fail(ErrorCode::InvalidSnapshot, "message.sequence"))?,
            role: MessageRole::User,
            content: request
                .input
                .into_iter()
                .map(|content| ContentBlock::Content { content })
                .collect(),
            origin: MessageOrigin::User,
            visibility: Visibility::UserAndModel,
        };
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: bindings.ids.next_id()?,
            scope: bindings.scope.clone(),
            run_id,
            session_id: snapshot.request.session_id.clone(),
            seq: NonZeroU64::new(1).expect("initial sequence"),
            timestamp_ms: now,
            payload: RunEventPayload::RunStarted {
                request_ref: request_record.reference().clone(),
                profile_digest: snapshot.profile.profile_digest().clone(),
            },
        };
        Ok((
            AdmissionInput {
                snapshot,
                prompt_snapshot: prompt_record.reference().clone(),
                require_durable: bindings.settings.require_durable,
                messages: vec![message],
                events: vec![event],
                records: vec![request_record, prompt_record, inputs_record, routing_record],
            },
            prompt,
        ))
    }
}
```

## `crates/wickle/src/agent/driver.rs`

```rust
use super::*;
use std::panic::AssertUnwindSafe;

impl Agent {
    pub(super) async fn drive(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        context: ExecutionContext,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let bindings = &self.inner.bindings;
        let now = bindings.clock.now()?.utc_ms;
        let lease = bindings
            .state
            .acquire_lease(
                &bindings.scope,
                run_id,
                &bindings.ids.next_id()?,
                now,
                bindings.settings.lease_ttl_ms,
            )
            .await?;
        self.drive_leased(run_id, prompt, context, local, lease, false)
            .await
    }

    pub(super) async fn drive_leased(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        context: ExecutionContext,
        local: &Arc<LocalRun>,
        lease: RunLease,
        expired: bool,
    ) -> Result<(), ContractError> {
        let bindings = &self.inner.bindings;
        let budget = Arc::new(
            RunBudget::attach(
                bindings.state.clone(),
                bindings.clock.clone(),
                bindings.ids.clone(),
                bindings.scope.clone(),
                run_id.clone(),
                lease.clone(),
                local.cancel.clone(),
            )
            .await?,
        );
        let stop = CancellationToken::new();
        let heartbeat_agent = self.clone();
        let heartbeat_budget = budget.clone();
        let heartbeat_lease = lease.clone();
        let heartbeat_id = run_id.clone();
        let heartbeat_stop = stop.clone();
        let heartbeat_local = local.clone();
        let heartbeat = tokio::spawn(async move {
            let result = AssertUnwindSafe(heartbeat_agent.heartbeat(
                &heartbeat_id,
                heartbeat_lease,
                &heartbeat_budget,
                &heartbeat_stop,
            ))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(fail(ErrorCode::LeaseLost, "agent.heartbeat")));
            if let Err(error) = &result {
                if let Ok(mut slot) = heartbeat_local.error.lock() {
                    *slot = Some(error.clone());
                }
                heartbeat_local.cancel.cancel();
            }
            result
        });
        let result = AssertUnwindSafe(async {
            if expired {
                self.finish(
                    run_id,
                    PreparedOutcome {
                        result: OutcomeResult::Exhausted {
                            budget: BudgetKind::Elapsed,
                        },
                        output: vec![],
                        continuation: vec![],
                        unresolved_effects: vec![],
                    },
                    &budget,
                    &context,
                    local,
                )
                .await
            } else {
                self.run_segment(run_id, prompt, &context, &budget, &lease, local)
                    .await
            }
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(fail(ErrorCode::InvalidContract, "agent.driver")));
        stop.cancel();
        let heartbeat_result = heartbeat
            .await
            .map_err(|_| fail(ErrorCode::LeaseLost, "agent.heartbeat"))?;
        // Stored completion is authoritative even if an acknowledgement or the
        // final heartbeat was lost after the terminal transaction succeeded.
        if let Ok(saved) = bindings.state.load(&bindings.scope, run_id).await {
            if saved.snapshot.status.is_terminal() {
                return Ok(());
            }
        }
        if let Ok((_, now)) = budget.settlement_time(0) {
            let _ = bindings
                .state
                .release_lease(&bindings.scope, run_id, &lease, now)
                .await;
        }
        result.and(heartbeat_result)
    }

    async fn heartbeat(
        &self,
        run_id: &Id,
        mut lease: RunLease,
        budget: &RunBudget,
        stop: &CancellationToken,
    ) -> Result<(), ContractError> {
        let bindings = &self.inner.bindings;
        loop {
            let reading = bindings.clock.now()?;
            let next = reading
                .monotonic_ms
                .checked_add(bindings.settings.heartbeat_interval_ms)
                .ok_or_else(|| fail(ErrorCode::ClockUnavailable, "agent.heartbeat"))?;
            tokio::select! { biased;
                _ = stop.cancelled() => return Ok(()),
                result = bindings.clock.sleep_until(next) => result?,
            }
            let (_, now) = budget.settlement_time(0)?;
            let renewal = bindings.state.renew_lease(
                &bindings.scope,
                run_id,
                &lease,
                now,
                bindings.settings.lease_ttl_ms,
            );
            let remaining = lease
                .expires_at_ms
                .checked_sub(now)
                .and_then(|value| u64::try_from(value).ok())
                .filter(|value| *value > 0)
                .ok_or_else(|| fail(ErrorCode::LeaseLost, "agent.heartbeat"))?;
            let result = tokio::select! { biased;
                _ = stop.cancelled() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_millis(remaining)) => Err(fail(ErrorCode::LeaseLost, "agent.heartbeat")),
                result = renewal => result,
            };
            match result {
                Ok(current) => lease = current,
                Err(error) => return Err(error),
            }
        }
    }

    async fn run_segment(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        context: &ExecutionContext,
        budget: &RunBudget,
        lease: &RunLease,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let mut waiting = None;
        let saved = self
            .inner
            .bindings
            .state
            .load(budget.scope(), run_id)
            .await?;
        let mut pending_round = saved.snapshot.tool_ledger.iter().find(|entry| !matches!(&entry.state, ToolCallState::Settled { result } if result.status != ToolResultStatus::Unknown && result.effect != ToolEffect::Unknown)).map(|entry| entry.call.model_request_id.clone());
        let attempt = loop {
            if let Some(request_id) = pending_round.take() {
                match self
                    .tool_round(budget)
                    .await?
                    .execute(&request_id, context, budget)
                    .await
                {
                    Ok(ToolRoundOutcome::Completed) => {}
                    Ok(outcome) => {
                        waiting = Some(self.tool_wait(outcome, budget).await?);
                        break None;
                    }
                    Err(error) => break Some(Err(error)),
                }
            }
            match self
                .generate(run_id, prompt.clone(), context, budget, lease)
                .await
            {
                Ok(Guarded::Completed(ModelExchangeOutcome::Completed { response }))
                    if response.finish == ModelFinish::ToolCalls =>
                {
                    if let Err(error) = self.plan_tools(&response, &prompt, budget).await {
                        break Some(Err(error));
                    }
                    let round = self.tool_round(budget).await?;
                    match round.execute(&response.request_id, context, budget).await {
                        Ok(ToolRoundOutcome::Completed) => continue,
                        Ok(outcome) => {
                            waiting = Some(self.tool_wait(outcome, budget).await?);
                            break None;
                        }
                        Err(error) => break Some(Err(error)),
                    }
                }
                result => break Some(result),
            }
        };
        if let Some(error) = local
            .error
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .clone()
        {
            return Err(error);
        }
        if let Some((wait, unresolved_effects)) = waiting {
            return self
                .finish(
                    run_id,
                    PreparedOutcome {
                        result: OutcomeResult::Waiting { wait },
                        output: vec![],
                        continuation: vec![],
                        unresolved_effects,
                    },
                    budget,
                    context,
                    local,
                )
                .await;
        }
        let attempt = attempt.expect("non-waiting loop result");
        let mut continuation = vec![];
        let (result, output) = match attempt {
            Ok(Guarded::Completed(ModelExchangeOutcome::Completed { response }))
                if response.finish == ModelFinish::Stop && response.tool_calls.is_empty() =>
            {
                continuation = response.continuation;
                (
                    OutcomeResult::Succeeded {
                        completion_basis: CompletionBasis::TurnEnded,
                    },
                    vec![InputContent::Text {
                        text: response.text,
                    }],
                )
            }
            Ok(Guarded::Completed(ModelExchangeOutcome::Completed { response })) => (
                failed(if response.finish == ModelFinish::Refusal {
                    "model_refusal"
                } else {
                    "tool_execution_unsupported"
                }),
                vec![],
            ),
            Ok(Guarded::Completed(ModelExchangeOutcome::Failed { failure })) => (
                failed(&format!("model_{}", enum_name(&failure.kind))),
                if failure.partial_text().is_empty() {
                    vec![]
                } else {
                    vec![InputContent::Text {
                        text: failure.partial_text().to_owned(),
                    }]
                },
            ),
            Ok(Guarded::ApprovalRequired(_)) => (failed("approval_runtime_unsupported"), vec![]),
            Err(error)
                if matches!(
                    error.code,
                    ErrorCode::LeaseLost
                        | ErrorCode::RevisionConflict
                        | ErrorCode::PersistenceUnavailable
                        | ErrorCode::StateNotFound
                        | ErrorCode::ClockUnavailable
                        | ErrorCode::ClockRegression
                        | ErrorCode::InvalidTransition
                        | ErrorCode::InvalidSnapshot
                        | ErrorCode::InvalidEvent
                        | ErrorCode::RecordConflict
                ) =>
            {
                return Err(error);
            }
            Err(error) if error.code == ErrorCode::Cancelled => (
                OutcomeResult::Cancelled {
                    reason: local
                        .reason
                        .lock()
                        .map_err(|_| fail(ErrorCode::InvalidContract, "agent.cancel"))?
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "cancelled".into()),
                },
                vec![],
            ),
            Err(error) if error.code == ErrorCode::DeadlineExceeded => (
                OutcomeResult::Exhausted {
                    budget: BudgetKind::Elapsed,
                },
                vec![],
            ),
            Err(error) if error.code == ErrorCode::BudgetExceeded => {
                let kind = match error.path.as_str() {
                    "budget.model_calls" => BudgetKind::ModelCalls,
                    "budget.tool_attempts" => BudgetKind::ToolAttempts,
                    "budget.repair_attempts" => BudgetKind::RepairAttempts,
                    "budget.recovery_attempts" => BudgetKind::RecoveryAttempts,
                    _ => BudgetKind::Elapsed,
                };
                (OutcomeResult::Exhausted { budget: kind }, vec![])
            }
            Err(error) => (failed(&enum_name(&error.code)), vec![]),
        };
        self.finish(
            run_id,
            PreparedOutcome {
                result,
                output,
                continuation,
                unresolved_effects: vec![],
            },
            budget,
            context,
            local,
        )
        .await
    }

    async fn generate(
        &self,
        run_id: &Id,
        prompt: PromptSnapshot,
        context: &ExecutionContext,
        budget: &RunBudget,
        lease: &RunLease,
    ) -> Result<Guarded<ModelExchangeOutcome>, ContractError> {
        let bindings = &self.inner.bindings;
        budget.check_boundary().await?;
        let mut snapshot = bindings.state.load(&bindings.scope, run_id).await?.snapshot;
        let expected_revision = snapshot.revision;
        let step = bindings.ids.next_id()?;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.prepare"))?;
        snapshot.phase = RunPhase::Prepare;
        snapshot.model_step_id = Some(step.clone());
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        let saved = bindings
            .state
            .commit(
                &bindings.scope,
                run_id,
                CommitInput {
                    expected_revision,
                    lease: lease.clone(),
                    now_ms: now,
                    snapshot,
                    messages: vec![],
                    events: vec![],
                    records: vec![],
                },
            )
            .await?;
        let router = bindings.router.snapshot();
        let rule = router
            .policy()
            .rules
            .iter()
            .find(|rule| {
                rule.model_binding == saved.snapshot.profile.profile().model_binding
                    && rule.purpose == ModelPurpose::Agent
            })
            .ok_or_else(|| fail(ErrorCode::ModelRouteDenied, "agent.routing"))?;
        let input = RoutedModelInput {
            model_step_id: step,
            routing: RouteRequest {
                model_binding: saved.snapshot.profile.profile().model_binding.clone(),
                purpose: ModelPurpose::Agent,
                required_capabilities: if prompt.tools().is_empty() {
                    std::collections::BTreeSet::from([Id::new("text")?])
                } else {
                    std::collections::BTreeSet::from([Id::new("text")?, Id::new("tool_calling")?])
                },
                input_tokens: 0,
                max_output_tokens: bindings.settings.max_output_tokens,
                options: saved.snapshot.request.model_options.clone(),
                scope: bindings.scope.clone(),
                allowed_bindings: std::iter::once(&rule.primary)
                    .chain(&rule.fallbacks)
                    .map(|binding| binding.id.clone())
                    .collect(),
                version_policy: rule.version_policy,
                previous_route: None,
                previous_failure: None,
            },
        };
        let projector = Projector {
            saved,
            prompt,
            settings: bindings.settings.clone(),
            estimator: bindings.token_estimator.clone(),
            state: bindings.state.clone(),
        };
        bindings
            .model_exchange
            .generate_routed(
                bindings.router.as_ref(),
                &input,
                &projector,
                context,
                budget,
            )
            .await
    }

    async fn finish(
        &self,
        run_id: &Id,
        candidate: PreparedOutcome,
        budget: &RunBudget,
        context: &ExecutionContext,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let PreparedOutcome {
            mut result,
            mut output,
            continuation,
            mut unresolved_effects,
        } = candidate;
        let bindings = &self.inner.bindings;
        let lease = budget.lease();
        let mut saved = bindings.state.load(&bindings.scope, run_id).await?;
        if saved.snapshot.status.is_terminal() {
            return Ok(());
        }
        if unresolved_effects.is_empty() && saved.snapshot.tool_ledger.iter().any(|entry| matches!(entry.state, ToolCallState::Unknown { .. }) || matches!(&entry.state, ToolCallState::Settled { result } if result.effect == ToolEffect::Unknown)) {
            if let Some(receipt) = saved.snapshot.resume_receipts.last() {
                let record = bindings.state.read_record(&bindings.scope, &receipt.previous_outcome_ref).await?;
                let previous: RunOutcome = serde_json::from_value(record.value().clone()).map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.previous_outcome"))?;
                unresolved_effects = previous.unresolved_effects;
            }
        }
        if output.is_empty() && !matches!(result, OutcomeResult::Succeeded { .. }) {
            output = self.saved_partial_output(&saved.snapshot).await?;
        }
        // Finalization remains possible after cancellation/deadline, but only
        // under the stored lease. A stop during these reads also closes untouched
        // plans; it never invents a result for an uncertain dispatched operation.
        let mut cleaned = false;
        let (elapsed, now) = loop {
            let (_, check_at) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
            let current_lease = bindings
                .state
                .check_lease(&bindings.scope, run_id, lease, check_at)
                .await?;
            let (elapsed, now) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
            if now >= current_lease.expires_at_ms {
                return Err(fail(ErrorCode::LeaseLost, "agent.finish"));
            }
            if matches!(
                result,
                OutcomeResult::Succeeded { .. } | OutcomeResult::Waiting { .. }
            ) {
                if local.cancel.is_cancelled() {
                    result = OutcomeResult::Cancelled {
                        reason: local
                            .reason
                            .lock()
                            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.cancel"))?
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "cancelled".into()),
                    };
                } else if elapsed >= saved.snapshot.limits.max_elapsed_ms.get() {
                    result = OutcomeResult::Exhausted {
                        budget: BudgetKind::Elapsed,
                    };
                }
            }
            if !matches!(
                result,
                OutcomeResult::Succeeded { .. } | OutcomeResult::Waiting { .. }
            ) && saved.snapshot.tool_ledger.iter().any(|entry| {
                matches!(
                    entry.state,
                    ToolCallState::Planned {}
                        | ToolCallState::ApprovalPending { .. }
                        | ToolCallState::InputPending { .. }
                )
            }) {
                if cleaned {
                    return Err(fail(ErrorCode::InvalidTransition, "agent.pending_tools"));
                }
                self.settle_unstarted_tools(
                    &saved.snapshot,
                    context,
                    budget,
                    matches!(result, OutcomeResult::Cancelled { .. }),
                )
                .await?;
                saved = bindings.state.load(&bindings.scope, run_id).await?;
                cleaned = true;
                continue;
            }
            break (elapsed, now);
        };
        let mut snapshot = saved.snapshot;
        let expected_revision = snapshot.revision;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.finish"))?;
        snapshot.last_event_seq = snapshot
            .last_event_seq
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.event"))?;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        snapshot.status = result.status();
        snapshot.phase = if snapshot.status == RunStatus::Waiting {
            RunPhase::Waiting
        } else {
            RunPhase::Finish
        };
        snapshot.wait = if let OutcomeResult::Waiting { wait } = &result {
            Some(wait.clone())
        } else {
            None
        };
        if let OutcomeResult::Failed { failure } = &mut result {
            failure.diagnostic_ref = snapshot
                .model_ledger
                .last()
                .and_then(|entry| entry.response_ref.clone());
        }
        let outcome = RunOutcome {
            result,
            output: output.clone(),
            artifacts: vec![],
            usage: snapshot.usage.clone(),
            checkpoint_revision: snapshot.revision,
            verification: None,
            unresolved_effects,
        };
        let record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(&outcome)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.outcome"))?,
        );
        let wait_record = snapshot
            .wait
            .as_ref()
            .map(|wait| {
                Ok::<_, ContractError>(ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(wait)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.wait"))?,
                ))
            })
            .transpose()?;
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: bindings.ids.next_id()?,
            scope: bindings.scope.clone(),
            run_id: run_id.clone(),
            session_id: snapshot.request.session_id.clone(),
            seq: snapshot
                .last_event_seq
                .try_into()
                .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.event"))?,
            timestamp_ms: now,
            payload: if let Some(wait_record) = &wait_record {
                RunEventPayload::RunWaiting {
                    wait_ref: wait_record.reference().clone(),
                }
            } else {
                RunEventPayload::RunFinished {
                    outcome_ref: record.reference().clone(),
                }
            },
        };
        let mut records = vec![record];
        records.extend(wait_record);
        let mut content: Vec<_> = output
            .into_iter()
            .map(|content| ContentBlock::Content { content })
            .collect();
        if snapshot.status == RunStatus::Succeeded {
            for continuation in continuation {
                let route = &snapshot
                    .model_ledger
                    .last()
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.continuation"))?
                    .route;
                if continuation.route_digest() != &route.digest() {
                    return Err(fail(
                        ErrorCode::ModelContextIncompatible,
                        "agent.continuation",
                    ));
                }
                let record = ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(&continuation)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.continuation"))?,
                );
                content.push(ContentBlock::ProviderOpaque {
                    provider: route.provider.clone(),
                    route_digest: route.digest(),
                    data_ref: record.reference().clone(),
                });
                records.push(record);
            }
        }
        let messages = if content.is_empty() || snapshot.status != RunStatus::Succeeded {
            vec![]
        } else {
            vec![Message {
                message_id: bindings.ids.next_id()?,
                run_id: run_id.clone(),
                sequence: saved
                    .session
                    .transcript_revision
                    .checked_add(1)
                    .and_then(NonZeroU64::new)
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "message.sequence"))?,
                role: MessageRole::Assistant,
                content,
                origin: MessageOrigin::Model,
                visibility: Visibility::UserAndModel,
            }]
        };
        snapshot.outcome = Some(outcome);
        bindings
            .state
            .commit(
                &bindings.scope,
                run_id,
                CommitInput {
                    expected_revision,
                    lease: lease.clone(),
                    now_ms: now,
                    snapshot,
                    messages,
                    events: vec![event],
                    records,
                },
            )
            .await?;
        local.notify.notify_waiters();
        Ok(())
    }

    async fn saved_partial_output(
        &self,
        snapshot: &RunSnapshot,
    ) -> Result<Vec<InputContent>, ContractError> {
        let Some(step) = &snapshot.model_step_id else {
            return Ok(vec![]);
        };
        let Some(invocation) = snapshot.model_ledger.iter().rev().find(|invocation| {
            &invocation.model_step_id == step
                && invocation.run_id == snapshot.run_id
                && invocation.response_ref.is_some()
        }) else {
            return Ok(vec![]);
        };
        let reference = invocation
            .response_ref
            .as_ref()
            .expect("filtered response reference");
        let record = self
            .inner
            .bindings
            .state
            .read_record(&snapshot.scope, reference)
            .await?;
        if record.reference() != reference {
            return Err(fail(ErrorCode::InvalidSnapshot, "agent.partial_response"));
        }
        let response: StoredModelResponse = serde_json::from_value(record.value().clone())
            .map_err(|_| fail(ErrorCode::InvalidSnapshot, "agent.partial_response"))?;
        if response.request_id != invocation.attempt_id
            || response.route_digest != invocation.route.digest()
        {
            return Err(fail(ErrorCode::InvalidSnapshot, "agent.partial_response"));
        }
        let text = match response.outcome {
            ModelExchangeOutcome::Completed { response } => response.text,
            ModelExchangeOutcome::Failed { failure } => failure.partial_text().to_owned(),
        };
        Ok(if text.is_empty() {
            vec![]
        } else {
            vec![InputContent::Text { text }]
        })
    }
}

struct PreparedOutcome {
    result: OutcomeResult,
    output: Vec<InputContent>,
    continuation: Vec<OpaqueContinuation>,
    unresolved_effects: Vec<RecordRef>,
}

struct Projector {
    saved: StoredRun,
    prompt: PromptSnapshot,
    settings: AgentSettings,
    estimator: Arc<dyn ModelTokenEstimator>,
    state: Arc<dyn StateStore>,
}
impl ModelRequestProjector for Projector {
    fn project<'a>(
        &'a self,
        selection: &'a RouteSelection,
        input: &'a RoutedModelInput,
        context: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ProjectedModelRequest> {
        Box::pin(async move {
            if context.cancellation.is_cancelled() {
                return Err(fail(ErrorCode::Cancelled, "agent.projection"));
            }
            let request_message = self
                .saved
                .messages
                .iter()
                .find(|message| {
                    message.run_id == self.saved.snapshot.run_id
                        && message.role == MessageRole::User
                })
                .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.request_message"))?;
            let mut opaque_records: Vec<ScopedOpaque> = vec![];
            for message in &self.saved.messages {
                if !matches!(
                    message.visibility,
                    Visibility::Model | Visibility::UserAndModel
                ) {
                    continue;
                }
                for content in &message.content {
                    if let ContentBlock::ProviderOpaque {
                        provider,
                        route_digest,
                        data_ref,
                    } = content
                    {
                        if provider != &selection.route.provider
                            || route_digest != &selection.route.digest()
                        {
                            return Err(fail(
                                ErrorCode::ModelContextIncompatible,
                                "agent.opaque_route",
                            ));
                        }
                        if opaque_records
                            .iter()
                            .any(|record| &record.reference == data_ref)
                        {
                            continue;
                        }
                        let record = self.state.read_record(&context.scope, data_ref).await?;
                        if record.reference() != data_ref {
                            return Err(fail(
                                ErrorCode::ModelContextIncompatible,
                                "agent.opaque_record",
                            ));
                        }
                        let continuation: OpaqueContinuation =
                            serde_json::from_value(record.value().clone()).map_err(|_| {
                                fail(ErrorCode::ModelContextIncompatible, "agent.opaque_record")
                            })?;
                        if continuation.route_digest() != route_digest
                            || canonical_digest(record.value()) != data_ref.digest
                        {
                            return Err(fail(
                                ErrorCode::ModelContextIncompatible,
                                "agent.opaque_record",
                            ));
                        }
                        opaque_records.push(ScopedOpaque {
                            scope: context.scope.clone(),
                            reference: data_ref.clone(),
                            provider: provider.clone(),
                            continuation,
                        });
                    }
                }
            }
            let projection = ContextAssembler::new().project(
                &self.prompt,
                ProjectionInput {
                    profile: &self.saved.snapshot.profile,
                    scope: &context.scope,
                    run_id: &self.saved.snapshot.run_id,
                    model_step_id: &input.model_step_id,
                    current_request: &self.saved.snapshot.request,
                    current_request_message_id: &request_message.message_id,
                    transcript: &self.saved.messages,
                    context_items: &[],
                    opaque_records: &opaque_records,
                    expected_prompt_digest: &self.saved.session.prompt_snapshot.digest,
                    request_id: input.model_step_id.clone(),
                    purpose: input.routing.purpose,
                    route: selection.route.clone(),
                    output: ModelOutput::Text {},
                    max_output_tokens: self.settings.max_output_tokens,
                    options: input.routing.options.clone(),
                    response_limits: self.settings.response_limits.clone(),
                    limits: self.settings.projection_limits,
                },
            )?;
            let input_tokens = self.estimator.estimate(&projection.request)?;
            Ok(ProjectedModelRequest {
                request: projection.request,
                input_tokens,
            })
        })
    }
}
fn enum_name(value: &impl serde::Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "invalid_contract".into())
}
fn failed(code: &str) -> OutcomeResult {
    OutcomeResult::Failed {
        failure: Failure {
            code: Id::new(code).expect("nonempty static classification"),
            diagnostic_ref: None,
        },
    }
}
```

## `crates/wickle/src/agent/resume.rs`

```rust
use super::*;
use std::panic::AssertUnwindSafe;

impl Agent {
    pub(super) async fn resume_command(
        &self,
        command: ResumeCommand,
        context: ExecutionContext,
    ) -> Result<Guarded<RunHandle>, ContractError> {
        self.check_scope(&context)?;
        if matches!(
            command.action,
            ResumeAction::Recover { .. }
                | ResumeAction::Approve {
                    target: ApprovalTarget::Candidate { .. },
                    ..
                }
                | ResumeAction::Deny {
                    target: ApprovalTarget::Candidate { .. },
                    ..
                }
        ) {
            return Err(fail(
                ErrorCode::CapabilityUnsupported,
                "agent.resume_action",
            ));
        }
        if serde_json::to_vec(&command)
            .map_err(|_| fail(ErrorCode::InvalidJson, "agent.command"))?
            .len()
            > self.inner.bindings.settings.max_request_bytes
        {
            return Err(fail(ErrorCode::InvalidContract, "agent.command_size"));
        }
        let saved = self
            .resume_read(
                &context,
                self.inner
                    .bindings
                    .state
                    .load(&self.inner.bindings.scope, &command.run_id),
            )
            .await?;
        if let Guarded::ApprovalRequired(challenge) = self
            .authorize_resume(
                &command,
                &context,
                saved_binding_digest(&saved.snapshot, &command),
            )
            .await?
        {
            return Ok(Guarded::ApprovalRequired(challenge));
        }
        self.resume_inputs(&saved.snapshot, &context).await?;
        if let Some(receipt) = accepted(&saved.snapshot, &command)? {
            return Ok(Guarded::Completed(
                self.handle(command.run_id, receipt.accepted_revision)?,
            ));
        }
        validate_wait(&saved.snapshot, &command)?;
        let lease = match self.waiting_lease(&command.run_id, &context).await {
            Ok(lease) => lease,
            Err(error) => {
                let latest = self
                    .resume_read(
                        &context,
                        self.inner
                            .bindings
                            .state
                            .load(&self.inner.bindings.scope, &command.run_id),
                    )
                    .await?;
                if let Some(receipt) = accepted(&latest.snapshot, &command)? {
                    return Ok(Guarded::Completed(
                        self.handle(command.run_id, receipt.accepted_revision)?,
                    ));
                }
                return Err(error);
            }
        };
        let result = self.resume_owned(&command, &context, &lease).await;
        match result {
            Ok((receipt, prompt, true)) => Ok(Guarded::Completed(self.launch_resumed(
                command.run_id,
                receipt.accepted_revision,
                prompt,
                context,
                lease,
                receipt.expired,
            )?)),
            Ok((receipt, _, false)) => {
                self.release_owned(&command.run_id, &lease).await;
                Ok(Guarded::Completed(
                    self.handle(command.run_id, receipt.accepted_revision)?,
                ))
            }
            Err(error) => {
                self.release_owned(&command.run_id, &lease).await;
                Err(error)
            }
        }
    }

    async fn resume_owned(
        &self,
        command: &ResumeCommand,
        context: &ExecutionContext,
        lease: &RunLease,
    ) -> Result<(ResumeReceipt, PromptSnapshot, bool), ContractError> {
        let bindings = &self.inner.bindings;
        let saved = self
            .resume_read(
                context,
                bindings.state.load(&bindings.scope, &command.run_id),
            )
            .await?;
        let prompt = self.restore_resume_runtime(&saved, context).await?;
        if let Some(receipt) = accepted(&saved.snapshot, command)? {
            return Ok((receipt.clone(), prompt, false));
        }
        validate_wait(&saved.snapshot, command)?;
        self.resume_inputs(&saved.snapshot, context).await?;
        let wait = saved
            .snapshot
            .wait
            .as_ref()
            .expect("validated waiting snapshot");
        let budget = RunBudget::attach(
            bindings.state.clone(),
            bindings.clock.clone(),
            bindings.ids.clone(),
            bindings.scope.clone(),
            command.run_id.clone(),
            lease.clone(),
            CancellationToken::new(),
        )
        .await?;
        let (_, now) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
        let expires_at_ms = saved
            .snapshot
            .timing
            .deadline_at_ms
            .min(wait.expires_at_ms.unwrap_or(i64::MAX));
        let expired = now >= expires_at_ms;
        let mut fixed_binding_digest = saved_binding_digest(&saved.snapshot, command);
        let prepared = if expired {
            None
        } else {
            let (call, bound) = self.resume_bound(&saved, context).await?;
            fixed_binding_digest = Some(bound.binding_digest().clone());
            if let Guarded::ApprovalRequired(_) = self
                .authorize_resume(command, context, fixed_binding_digest.clone())
                .await?
            {
                return Err(fail(ErrorCode::AccessDenied, "agent.resume_approval"));
            }
            let round = self.tool_round(&budget).await?;
            match &command.action {
                ResumeAction::Approve { .. } => None,
                ResumeAction::Deny { .. } => Some(round.prepare_denial(
                    &saved,
                    &call.call_id,
                    &bound,
                    Id::new("approval_denied")?,
                    now,
                )?),
                ResumeAction::Input { answer, .. } => {
                    let WaitTarget::Input { request } = &wait.target else {
                        return Err(fail(ErrorCode::InvalidReference, "agent.input_wait"));
                    };
                    Some(round.prepare_input(&saved, request, &bound, answer.clone(), now)?)
                }
                ResumeAction::External { receipt_ref, .. } => {
                    let verifier =
                        bindings.external_receipt_verifier.as_ref().ok_or_else(|| {
                            fail(ErrorCode::CapabilityUnsupported, "agent.external_verifier")
                        })?;
                    let entry = saved
                        .snapshot
                        .tool_ledger
                        .iter()
                        .find(|entry| entry.call.call_id == call.call_id)
                        .expect("resolved call");
                    let ToolCallState::Unknown {
                        attempt_id,
                        idempotency_key,
                    } = &entry.state
                    else {
                        return Err(fail(ErrorCode::InvalidTransition, "agent.external_wait"));
                    };
                    self.authorize_receipt(receipt_ref, context).await?;
                    let record = self
                        .resume_read(
                            context,
                            bindings.state.read_record(&bindings.scope, receipt_ref),
                        )
                        .await?;
                    if record.reference() != receipt_ref {
                        return Err(fail(ErrorCode::InvalidSnapshot, "agent.receipt_reference"));
                    }
                    if serde_json::to_vec(record.value())
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.receipt"))?
                        .len()
                        > bindings.settings.max_request_bytes
                    {
                        return Err(fail(ErrorCode::InvalidArguments, "agent.receipt_size"));
                    }
                    let request = ExternalReceiptRequest {
                        call,
                        attempt_id: attempt_id.clone(),
                        idempotency_key: idempotency_key.clone(),
                        bound_input: bound.clone(),
                        receipt_ref: receipt_ref.clone(),
                        receipt: record.value().clone(),
                    };
                    self.authorize_receipt(receipt_ref, context).await?;
                    let (_, now) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
                    let current_lease = bindings
                        .state
                        .check_lease(&bindings.scope, &command.run_id, lease, now)
                        .await?;
                    let (_, now) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
                    if now >= current_lease.expires_at_ms {
                        return Err(fail(ErrorCode::LeaseLost, "agent.external_verifier"));
                    }
                    let remaining = saved
                        .snapshot
                        .timing
                        .deadline_at_ms
                        .min(wait.expires_at_ms.unwrap_or(i64::MAX))
                        .min(current_lease.expires_at_ms)
                        .saturating_sub(now)
                        .max(0) as u64;
                    let timeout = Duration::from_millis(
                        bindings
                            .settings
                            .start_timeout_ms
                            .min(bindings.settings.lease_ttl_ms / 2)
                            .min(remaining),
                    );
                    let cancellation = CancellationToken::new();
                    let _cancel = cancellation.clone().drop_guard();
                    let verification = ExternalReceiptContext {
                        scope: bindings.scope.clone(),
                        principal_ref: context.data.principal_ref.clone(),
                        capability_grant_ref: context.data.capability_grant_ref.clone(),
                        cancellation,
                        deadline: tokio::time::Instant::now() + timeout,
                    };
                    let verified = caller_read(context, Some(timeout), async {
                        AssertUnwindSafe(verifier.verify(&request, &verification))
                            .catch_unwind()
                            .await
                            .map_err(|_| {
                                fail(ErrorCode::InvalidContract, "agent.receipt_verifier")
                            })?
                            .map_err(|error| fail(error.code, "agent.receipt_verifier"))
                    })
                    .await?;
                    verification.cancellation.cancel();
                    self.authorize_receipt(receipt_ref, context).await?;
                    Some(round.prepare_external(
                        &saved,
                        &request.call.call_id,
                        &bound,
                        verified,
                        now,
                    )?)
                }
                ResumeAction::Recover { .. } => {
                    return Err(fail(ErrorCode::CapabilityUnsupported, "agent.recovery"));
                }
            }
        };
        if let Guarded::ApprovalRequired(_) = self
            .authorize_resume(command, context, fixed_binding_digest)
            .await?
        {
            return Err(fail(ErrorCode::AccessDenied, "agent.resume_approval"));
        }
        if context.cancellation.is_cancelled() {
            return Err(fail(ErrorCode::Cancelled, "agent.resume"));
        }
        let old_outcome = saved
            .snapshot
            .outcome
            .as_ref()
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.wait_outcome"))?;
        let outcome_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(old_outcome)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.wait_outcome"))?,
        );
        let command_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(command)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.command"))?,
        );
        let mut receipt = ResumeReceipt {
            command: command.clone(),
            command_ref: command_record.reference().clone(),
            accepted_revision: saved
                .snapshot
                .revision
                .checked_add(1)
                .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.resume"))?,
            previous_segment_start_revision: segment_revision(&saved.snapshot),
            previous_outcome_ref: outcome_record.reference().clone(),
            previous_last_event_seq: saved.snapshot.last_event_seq,
            actor_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
            expired,
        };
        let mut records = vec![outcome_record, command_record];
        let mut messages = vec![];
        let mut events = vec![];
        let mut snapshot = saved.snapshot;
        if let Some(prepared) = prepared {
            apply_resolution(
                &mut snapshot,
                &mut messages,
                &mut events,
                &mut records,
                prepared,
            )?;
        }
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let current_lease = bindings
            .state
            .check_lease(&bindings.scope, &command.run_id, lease, now)
            .await?;
        let (elapsed, now) = budget.settlement_time(elapsed)?;
        if now >= current_lease.expires_at_ms {
            return Err(fail(ErrorCode::LeaseLost, "agent.resume"));
        }
        receipt.expired = now >= expires_at_ms;
        snapshot.revision = receipt.accepted_revision;
        snapshot.status = RunStatus::Running;
        snapshot.phase = RunPhase::Tool;
        snapshot.wait = None;
        snapshot.outcome = None;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        snapshot.last_event_seq = snapshot
            .last_event_seq
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::InvalidEvent, "agent.resume"))?;
        snapshot.resume_receipts.push(receipt.clone());
        events.push(RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: bindings.ids.next_id()?,
            scope: bindings.scope.clone(),
            run_id: command.run_id.clone(),
            session_id: snapshot.request.session_id.clone(),
            seq: snapshot
                .last_event_seq
                .try_into()
                .map_err(|_| fail(ErrorCode::InvalidEvent, "agent.resume"))?,
            timestamp_ms: now,
            payload: RunEventPayload::RunResumed {
                command_ref: receipt.command_ref.clone(),
            },
        });
        let commit = bindings
            .state
            .commit(
                &bindings.scope,
                &command.run_id,
                CommitInput {
                    expected_revision: command.expected_revision,
                    lease: lease.clone(),
                    now_ms: now,
                    snapshot,
                    messages,
                    events,
                    records,
                },
            )
            .await;
        if let Err(error) = commit {
            let latest = bindings
                .state
                .load(&bindings.scope, &command.run_id)
                .await?;
            if accepted(&latest.snapshot, command)?.is_some_and(|saved| saved == &receipt) {
                return Ok((receipt, prompt, true));
            }
            return Err(error);
        }
        Ok((receipt, prompt, true))
    }

    async fn authorize_resume(
        &self,
        command: &ResumeCommand,
        context: &ExecutionContext,
        binding_digest: Option<JsonDigest>,
    ) -> Result<Guarded<()>, ContractError> {
        let request = PolicyRequest {
            owner_scope: self.inner.bindings.scope.clone(),
            resource_id: command.run_id.clone(),
            action: PolicyAction::ResumeRun {
                command: Box::new(command.clone()),
                binding_digest,
            },
        };
        self.inner
            .bindings
            .policy
            .guard(&request, context, None, None, || async { Ok(()) })
            .await
    }
    async fn authorize_receipt(
        &self,
        reference: &RecordRef,
        context: &ExecutionContext,
    ) -> Result<(), ContractError> {
        let request = PolicyRequest {
            owner_scope: self.inner.bindings.scope.clone(),
            resource_id: reference.record_id.clone(),
            action: PolicyAction::ReadRecord {},
        };
        match self
            .inner
            .bindings
            .policy
            .guard(&request, context, None, None, || async { Ok(()) })
            .await?
        {
            Guarded::Completed(()) => Ok(()),
            Guarded::ApprovalRequired(_) => {
                Err(fail(ErrorCode::AccessDenied, "agent.receipt_read"))
            }
        }
    }
    async fn resume_inputs(
        &self,
        snapshot: &RunSnapshot,
        context: &ExecutionContext,
    ) -> Result<(), ContractError> {
        if self.inner.profile.digest() != *snapshot.profile.profile_digest() {
            return Err(fail(ErrorCode::ProfileMismatch, "agent.resume_profile"));
        }
        if let Some(reference) = &snapshot.system_inputs {
            let record = self
                .resume_read(
                    context,
                    self.inner
                        .bindings
                        .state
                        .read_record(&snapshot.scope, &reference.snapshot_ref),
                )
                .await?;
            RunSystemInputs::from_value(record.value(), reference, &snapshot.scope)?
                .validate_resume(context.data.system_inputs.as_ref())?;
        } else if context
            .data
            .system_inputs
            .as_ref()
            .is_some_and(|inputs| !inputs.values().is_empty())
        {
            return Err(fail(ErrorCode::SystemInputsMismatch, "agent.system_inputs"));
        }
        Ok(())
    }
    async fn restore_resume_runtime(
        &self,
        saved: &StoredRun,
        context: &ExecutionContext,
    ) -> Result<PromptSnapshot, ContractError> {
        let bindings = &self.inner.bindings;
        let record = self
            .resume_read(
                context,
                bindings
                    .state
                    .read_record(&bindings.scope, &saved.session.prompt_snapshot),
            )
            .await?;
        let prompt = PromptSnapshot::restore(
            &serde_json::to_string(record.value())
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.prompt"))?,
            &saved.session.prompt_snapshot.digest,
            &saved.snapshot.profile,
            &bindings.scope,
        )?;
        let tools = bindings
            .tools
            .as_ref()
            .map(|tools| tools.prompt_bindings(saved.snapshot.profile.profile()))
            .transpose()?
            .unwrap_or_default();
        if tools.len() != prompt.tools().len()
            || tools.iter().zip(prompt.tools()).any(|(tool, pinned)| {
                tool.selection != pinned.selection
                    || tool.compiled.digest() != &pinned.compiled_digest
            })
        {
            return Err(fail(ErrorCode::ContextMismatch, "agent.pinned_tools"));
        }
        let expected = saved
            .snapshot
            .routing_snapshot_ref
            .as_ref()
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.routing"))?;
        let current =
            std::panic::catch_unwind(AssertUnwindSafe(|| bindings.router.snapshot().digest()))
                .map_err(|_| fail(ErrorCode::ModelRoutingMismatch, "agent.router"))?;
        if current != expected.digest {
            return Err(fail(
                ErrorCode::ModelRoutingMismatch,
                "agent.pinned_routing",
            ));
        }
        Ok(prompt)
    }
    async fn resume_bound(
        &self,
        saved: &StoredRun,
        context: &ExecutionContext,
    ) -> Result<(ToolCall, BoundToolInput), ContractError> {
        let wait = saved
            .snapshot
            .wait
            .as_ref()
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.wait"))?;
        let call_id = match &wait.target {
            WaitTarget::Approval {
                target: ApprovalTarget::Tool { call_id, .. },
            }
            | WaitTarget::External { call_id, .. } => call_id,
            WaitTarget::Input { request } => &request.call_id,
            _ => return Err(fail(ErrorCode::CapabilityUnsupported, "agent.wait")),
        };
        let call = saved
            .snapshot
            .tool_ledger
            .iter()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.wait_call"))?
            .call
            .clone();
        let registered = self
            .inner
            .bindings
            .tools
            .as_ref()
            .and_then(|tools| tools.get(&call.tool_name))
            .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "agent.wait_tool"))?;
        let reference = call
            .bound_input_ref
            .as_ref()
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.wait_binding"))?;
        let record = self
            .resume_read(
                context,
                self.inner
                    .bindings
                    .state
                    .read_record(&saved.snapshot.scope, reference),
            )
            .await?;
        let bound = BoundToolInput::restore(
            &record,
            &registered.compiled,
            &saved.snapshot.scope,
            &saved.snapshot.run_id,
            &call,
            saved.snapshot.system_inputs.as_ref(),
        )?;
        if let WaitTarget::Approval {
            target: ApprovalTarget::Tool { binding_digest, .. },
        } = &wait.target
        {
            if binding_digest != bound.binding_digest() {
                return Err(fail(ErrorCode::InvalidSnapshot, "agent.wait_binding"));
            }
        }
        Ok((call, bound))
    }

    async fn waiting_lease(
        &self,
        run_id: &Id,
        context: &ExecutionContext,
    ) -> Result<RunLease, ContractError> {
        let bindings = &self.inner.bindings;
        let owner = bindings.ids.next_id()?;
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(bindings.settings.start_timeout_ms);
        loop {
            if context.cancellation.is_cancelled() {
                return Err(fail(ErrorCode::Cancelled, "agent.resume"));
            }
            let saved = self
                .resume_read(context, bindings.state.load(&bindings.scope, run_id))
                .await?;
            if saved.snapshot.status != RunStatus::Waiting {
                return Err(fail(ErrorCode::InvalidTransition, "agent.wait"));
            }
            match bindings
                .state
                .acquire_lease(
                    &bindings.scope,
                    run_id,
                    &owner,
                    bindings.clock.now()?.utc_ms,
                    bindings.settings.lease_ttl_ms,
                )
                .await
            {
                Ok(lease) => return Ok(lease),
                Err(error) if error.code == ErrorCode::LeaseBusy => {}
                Err(error) => return Err(error),
            }
            tokio::select! { biased;
                _ = context.cancellation.cancelled() => return Err(fail(ErrorCode::Cancelled, "agent.resume")),
                _ = tokio::time::sleep_until(deadline) => return Err(fail(ErrorCode::LeaseBusy, "agent.wait_handoff")),
                _ = tokio::time::sleep(Duration::from_millis(bindings.settings.observer_poll_ms.min(20))) => {},
            }
        }
    }
    async fn release_owned(&self, run_id: &Id, lease: &RunLease) {
        if let Ok(now) = self.inner.bindings.clock.now() {
            let _ = self
                .inner
                .bindings
                .state
                .release_lease(&self.inner.bindings.scope, run_id, lease, now.utc_ms)
                .await;
        }
    }
    async fn resume_read<T>(
        &self,
        context: &ExecutionContext,
        future: impl std::future::Future<Output = Result<T, ContractError>>,
    ) -> Result<T, ContractError> {
        caller_read(
            context,
            Some(Duration::from_millis(
                self.inner.bindings.settings.start_timeout_ms,
            )),
            future,
        )
        .await
    }
    fn launch_resumed(
        &self,
        run_id: Id,
        segment_start_revision: u64,
        prompt: PromptSnapshot,
        context: ExecutionContext,
        lease: RunLease,
        expired: bool,
    ) -> Result<RunHandle, ContractError> {
        let local = Arc::new(LocalRun::new(segment_start_revision));
        self.inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .insert(run_id.clone(), local.clone());
        let agent = self.clone();
        let driver_id = run_id.clone();
        let driver_local = local.clone();
        let mut data = context.data;
        data.system_inputs = None;
        let context = ExecutionContext::new(data, local.cancel.clone());
        tokio::spawn(async move {
            let result = AssertUnwindSafe(agent.drive_leased(
                &driver_id,
                prompt,
                context,
                &driver_local,
                lease,
                expired,
            ))
            .catch_unwind()
            .await;
            let error = match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(fail(error.code, "agent.driver")),
                Err(_) => Some(fail(ErrorCode::InvalidContract, "agent.driver")),
            };
            let completed = error.is_none();
            if let Ok(mut slot) = driver_local.error.lock() {
                *slot = error;
            }
            driver_local.done.store(true, Ordering::Release);
            driver_local.notify.notify_waiters();
            if completed {
                if let Ok(mut runs) = agent.inner.runs.lock() {
                    if runs
                        .get(&driver_id)
                        .is_some_and(|current| Arc::ptr_eq(current, &driver_local))
                    {
                        runs.remove(&driver_id);
                    }
                }
            }
        });
        Ok(RunHandle {
            agent: self.clone(),
            run_id,
            segment_start_revision,
            local: Some(local),
        })
    }

    pub(super) async fn cancel_waiting(
        &self,
        run_id: Id,
        reason: Id,
        context: ExecutionContext,
    ) -> Result<CancelReceipt, ContractError> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| fail(ErrorCode::RuntimeUnavailable, "agent.runtime"))?;
        let agent = self.clone();
        runtime
            .spawn(async move { agent.cancel_waiting_owned(run_id, reason, context).await })
            .await
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.cancel"))?
    }
    async fn cancel_waiting_owned(
        &self,
        run_id: Id,
        reason: Id,
        context: ExecutionContext,
    ) -> Result<CancelReceipt, ContractError> {
        let bindings = &self.inner.bindings;
        let lease = match self.waiting_lease(&run_id, &context).await {
            Ok(lease) => lease,
            Err(error) => {
                if bindings
                    .state
                    .load(&bindings.scope, &run_id)
                    .await?
                    .snapshot
                    .status
                    .is_terminal()
                {
                    return Ok(CancelReceipt::AlreadyTerminal);
                }
                return Err(error);
            }
        };
        let result = async {
            let mut saved = self
                .resume_read(&context, bindings.state.load(&bindings.scope, &run_id))
                .await?;
            if saved.snapshot.status.is_terminal() {
                return Ok(CancelReceipt::AlreadyTerminal);
            }
            if saved.snapshot.status != RunStatus::Waiting {
                return Err(fail(ErrorCode::InvalidTransition, "agent.cancel_wait"));
            }
            let policy = PolicyRequest {
                owner_scope: bindings.scope.clone(),
                resource_id: run_id.clone(),
                action: PolicyAction::CancelRun {},
            };
            if let Guarded::ApprovalRequired(_) = bindings
                .policy
                .guard(&policy, &context, None, None, || async { Ok(()) })
                .await?
            {
                return Err(fail(ErrorCode::AccessDenied, "agent.cancel_wait"));
            }
            let budget = RunBudget::attach(
                bindings.state.clone(),
                bindings.clock.clone(),
                bindings.ids.clone(),
                bindings.scope.clone(),
                run_id.clone(),
                lease.clone(),
                CancellationToken::new(),
            )
            .await?;
            let round = self.tool_round(&budget).await?;
            let expected_revision = saved.snapshot.revision;
            let mut messages = vec![];
            let mut events = vec![];
            let mut records = vec![];
            let calls: Vec<_> = saved
                .snapshot
                .tool_ledger
                .iter()
                .filter(|entry| {
                    matches!(
                        entry.state,
                        ToolCallState::Planned {}
                            | ToolCallState::ApprovalPending { .. }
                            | ToolCallState::InputPending { .. }
                    )
                })
                .map(|entry| entry.call.call_id.clone())
                .collect();
            for call in calls {
                let (_, now) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
                let prepared = round.prepare_unstarted(
                    &saved,
                    &call,
                    ToolResultStatus::Cancelled,
                    Id::new("cancelled")?,
                    now,
                )?;
                saved.session.transcript_revision += 1;
                saved.messages.push(prepared.message.clone());
                apply_resolution(
                    &mut saved.snapshot,
                    &mut messages,
                    &mut events,
                    &mut records,
                    prepared,
                )?;
            }
            let (elapsed, now) = budget.settlement_time(saved.snapshot.usage.elapsed_ms)?;
            let mut snapshot = saved.snapshot;
            snapshot.revision = snapshot
                .revision
                .checked_add(1)
                .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.cancel_wait"))?;
            snapshot.status = RunStatus::Cancelled;
            snapshot.phase = RunPhase::Finish;
            snapshot.wait = None;
            snapshot.usage.elapsed_ms = elapsed;
            snapshot.timing.last_observed_at_ms = now;
            let previous = snapshot
                .outcome
                .take()
                .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.wait_outcome"))?;
            let outcome = RunOutcome {
                result: OutcomeResult::Cancelled {
                    reason: reason.to_string(),
                },
                output: previous.output,
                artifacts: previous.artifacts,
                usage: snapshot.usage.clone(),
                checkpoint_revision: snapshot.revision,
                verification: None,
                unresolved_effects: previous.unresolved_effects,
            };
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(&outcome)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.cancel_wait"))?,
            );
            snapshot.last_event_seq = snapshot
                .last_event_seq
                .checked_add(1)
                .ok_or_else(|| fail(ErrorCode::InvalidEvent, "agent.cancel_wait"))?;
            events.push(RunEvent {
                schema_version: RunEventSchemaVersion::V1,
                event_id: bindings.ids.next_id()?,
                scope: bindings.scope.clone(),
                run_id: run_id.clone(),
                session_id: snapshot.request.session_id.clone(),
                seq: snapshot
                    .last_event_seq
                    .try_into()
                    .map_err(|_| fail(ErrorCode::InvalidEvent, "agent.cancel_wait"))?,
                timestamp_ms: now,
                payload: RunEventPayload::RunFinished {
                    outcome_ref: record.reference().clone(),
                },
            });
            records.push(record);
            snapshot.outcome = Some(outcome);
            let commit = bindings
                .state
                .commit(
                    &bindings.scope,
                    &run_id,
                    CommitInput {
                        expected_revision,
                        lease: lease.clone(),
                        now_ms: now,
                        snapshot,
                        messages,
                        events,
                        records,
                    },
                )
                .await;
            if let Err(error) = commit {
                if bindings
                    .state
                    .load(&bindings.scope, &run_id)
                    .await?
                    .snapshot
                    .status
                    == RunStatus::Cancelled
                {
                    return Ok(CancelReceipt::Requested);
                }
                return Err(error);
            }
            Ok(CancelReceipt::Requested)
        }
        .await;
        self.release_owned(&run_id, &lease).await;
        result
    }
}

fn accepted<'a>(
    snapshot: &'a RunSnapshot,
    command: &ResumeCommand,
) -> Result<Option<&'a ResumeReceipt>, ContractError> {
    let receipt = snapshot
        .resume_receipts
        .iter()
        .find(|receipt| receipt.command.command_id == command.command_id);
    if receipt.is_some_and(|receipt| &receipt.command != command) {
        return Err(fail(ErrorCode::RequestConflict, "agent.resume_command"));
    }
    Ok(receipt)
}
fn saved_binding_digest(snapshot: &RunSnapshot, command: &ResumeCommand) -> Option<JsonDigest> {
    if let Some(receipt) = snapshot
        .resume_receipts
        .iter()
        .find(|receipt| receipt.command.command_id == command.command_id)
    {
        return match &receipt.command.action {
            ResumeAction::Approve {
                target: ApprovalTarget::Tool { binding_digest, .. },
                ..
            }
            | ResumeAction::Deny {
                target: ApprovalTarget::Tool { binding_digest, .. },
                ..
            } => Some(binding_digest.clone()),
            _ => None,
        };
    }
    match snapshot.wait.as_ref().map(|wait| &wait.target) {
        Some(WaitTarget::Approval {
            target: ApprovalTarget::Tool { binding_digest, .. },
        }) => Some(binding_digest.clone()),
        _ => None,
    }
}
fn validate_wait(snapshot: &RunSnapshot, command: &ResumeCommand) -> Result<(), ContractError> {
    if snapshot.status != RunStatus::Waiting {
        return Err(fail(ErrorCode::InvalidTransition, "agent.wait"));
    }
    if snapshot.revision != command.expected_revision {
        return Err(fail(ErrorCode::RevisionConflict, "agent.resume"));
    }
    let wait = snapshot
        .wait
        .as_ref()
        .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.wait"))?;
    let matched = match (&wait.target, &command.action) {
        (
            WaitTarget::Approval { target },
            ResumeAction::Approve {
                wait_id,
                target: supplied,
            }
            | ResumeAction::Deny {
                wait_id,
                target: supplied,
                ..
            },
        ) => wait_id == &wait.wait_id && supplied == target,
        (WaitTarget::Input { .. }, ResumeAction::Input { wait_id, .. })
        | (WaitTarget::External { .. }, ResumeAction::External { wait_id, .. }) => {
            wait_id == &wait.wait_id
        }
        _ => false,
    };
    if !matched {
        return Err(fail(ErrorCode::InvalidReference, "agent.wait_target"));
    }
    Ok(())
}
fn apply_resolution(
    snapshot: &mut RunSnapshot,
    messages: &mut Vec<Message>,
    events: &mut Vec<RunEvent>,
    records: &mut Vec<ProtectedRecord>,
    prepared: PreparedToolResolution,
) -> Result<(), ContractError> {
    let entry = snapshot
        .tool_ledger
        .iter_mut()
        .find(|entry| entry.call.call_id == prepared.result.call_id)
        .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.resume_call"))?;
    entry.state = prepared.state;
    snapshot.last_event_seq = prepared.event.seq.get();
    messages.push(prepared.message);
    events.push(prepared.event);
    records.extend(prepared.records);
    Ok(())
}
```

## `crates/wickle/src/agent/tools.rs`

```rust
use super::*;

impl Agent {
    pub(super) async fn tool_round(
        &self,
        budget: &RunBudget,
    ) -> Result<SerialToolRound, ContractError> {
        let bindings = &self.inner.bindings;
        let registry = match &bindings.tools {
            Some(registry) => registry.clone(),
            None => Arc::new(ToolRegistry::new(bindings.scope.clone(), vec![])?),
        };
        let saved = bindings.state.load(budget.scope(), budget.run_id()).await?;
        let definitions = if let Some(reference) = &saved.snapshot.system_inputs {
            let record = bindings
                .state
                .read_record(budget.scope(), &reference.snapshot_ref)
                .await?;
            let inputs = RunSystemInputs::from_value(record.value(), reference, budget.scope())?;
            SystemInputRegistry::new(inputs.definitions().values().cloned().collect())?
        } else {
            bindings.system_inputs.clone()
        };
        let binder = Arc::new(InputBinder::new(
            Arc::new(definitions),
            bindings.system_input_resolver.clone(),
            bindings.policy.clone(),
            bindings.ids.clone(),
        ));
        SerialToolRound::new(
            registry,
            binder,
            bindings.policy.clone(),
            bindings.ids.clone(),
        )
        .with_limits(bindings.settings.tool_execution_limits)
    }

    /// Commit the original complete model plan before any resolver or tool runs.
    pub(super) async fn plan_tools(
        &self,
        response: &ModelResponse,
        prompt: &PromptSnapshot,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        budget.check_boundary().await?;
        let bindings = &self.inner.bindings;
        let saved = bindings.state.load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        if response.finish != ModelFinish::ToolCalls
            || response.tool_calls.is_empty()
            || snapshot
                .tool_ledger
                .iter()
                .any(|entry| entry.call.model_request_id == response.request_id)
        {
            return Err(fail(ErrorCode::InvalidTransition, "agent.tool_plan"));
        }
        let invocation = snapshot
            .model_ledger
            .iter()
            .find(|invocation| {
                invocation.attempt_id == response.request_id
                    && matches!(invocation.state, ModelAttemptState::Completed {})
            })
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.tool_response"))?;
        if invocation.route.digest() != response.route_digest {
            return Err(fail(ErrorCode::ModelRoutingMismatch, "agent.tool_response"));
        }
        let provider = invocation.route.provider.clone();
        let route_digest = invocation.route.digest();
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let mut content = Vec::new();
        if !response.text.is_empty() {
            content.push(ContentBlock::Content {
                content: InputContent::Text {
                    text: response.text.clone(),
                },
            });
        }
        let mut records = vec![];
        let mut events = vec![];
        for proposed in &response.tool_calls {
            let descriptor_digest = prompt
                .tools()
                .iter()
                .find(|tool| tool.model_tool.name == proposed.name)
                .map(|tool| tool.descriptor_digest.clone());
            let call = ToolCall {
                call_id: bindings.ids.next_id()?,
                model_request_id: response.request_id.clone(),
                provider_call_id: proposed.provider_call_id.clone(),
                tool_name: proposed.name.clone(),
                model_inputs: proposed.model_inputs.clone(),
                descriptor_digest,
                bound_input_ref: None,
            };
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(&call)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.tool_plan"))?,
            );
            snapshot.last_event_seq = snapshot
                .last_event_seq
                .checked_add(1)
                .ok_or_else(|| fail(ErrorCode::InvalidEvent, "agent.tool_plan"))?;
            events.push(RunEvent {
                schema_version: RunEventSchemaVersion::V1,
                event_id: bindings.ids.next_id()?,
                scope: budget.scope().clone(),
                run_id: budget.run_id().clone(),
                session_id: snapshot.request.session_id.clone(),
                seq: snapshot
                    .last_event_seq
                    .try_into()
                    .map_err(|_| fail(ErrorCode::InvalidEvent, "agent.tool_plan"))?,
                timestamp_ms: now,
                payload: RunEventPayload::ToolPlanned {
                    call_ref: record.reference().clone(),
                },
            });
            records.push(record);
            content.push(ContentBlock::ToolCall { call: call.clone() });
            snapshot.tool_ledger.push(ToolLedgerEntry {
                call,
                state: ToolCallState::Planned {},
            });
        }
        for continuation in &response.continuation {
            if continuation.route_digest() != &route_digest {
                return Err(fail(
                    ErrorCode::ModelContextIncompatible,
                    "agent.continuation",
                ));
            }
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(continuation)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.continuation"))?,
            );
            content.push(ContentBlock::ProviderOpaque {
                provider: provider.clone(),
                route_digest: route_digest.clone(),
                data_ref: record.reference().clone(),
            });
            records.push(record);
        }
        let message = Message {
            message_id: bindings.ids.next_id()?,
            run_id: budget.run_id().clone(),
            sequence: saved
                .session
                .transcript_revision
                .checked_add(1)
                .and_then(NonZeroU64::new)
                .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.tool_plan"))?,
            role: MessageRole::Assistant,
            content,
            origin: MessageOrigin::Model,
            visibility: Visibility::UserAndModel,
        };
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| fail(ErrorCode::RevisionConflict, "agent.tool_plan"))?;
        snapshot.phase = RunPhase::Tool;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        bindings
            .state
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages: vec![message],
                    events,
                    records,
                },
            )
            .await?;
        Ok(())
    }

    pub(super) async fn tool_wait(
        &self,
        outcome: ToolRoundOutcome,
        budget: &RunBudget,
    ) -> Result<(WaitState, Vec<RecordRef>), ContractError> {
        let bindings = &self.inner.bindings;
        let (target, unresolved) = match outcome {
            ToolRoundOutcome::ApprovalRequired {
                call_id,
                binding_digest,
                ..
            } => (
                WaitTarget::Approval {
                    target: ApprovalTarget::Tool {
                        call_id,
                        binding_digest,
                    },
                },
                vec![],
            ),
            ToolRoundOutcome::Unresolved {
                call_id,
                result_ref,
            } => {
                let saved = bindings.state.load(budget.scope(), budget.run_id()).await?;
                let entry = saved
                    .snapshot
                    .tool_ledger
                    .iter()
                    .find(|entry| entry.call.call_id == call_id)
                    .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "agent.unresolved_tool"))?;
                let ToolCallState::Unknown {
                    idempotency_key, ..
                } = &entry.state
                else {
                    return Err(fail(ErrorCode::InvalidTransition, "agent.unresolved_tool"));
                };
                (
                    WaitTarget::External {
                        call_id,
                        effect_key: idempotency_key.clone(),
                    },
                    vec![result_ref],
                )
            }
            ToolRoundOutcome::InputRequired { request } => (WaitTarget::Input { request }, vec![]),
            ToolRoundOutcome::Completed => {
                return Err(fail(ErrorCode::InvalidTransition, "agent.tool_wait"));
            }
        };
        Ok((
            WaitState {
                wait_id: bindings.ids.next_id()?,
                target,
                expires_at_ms: Some(
                    bindings
                        .state
                        .load(budget.scope(), budget.run_id())
                        .await?
                        .snapshot
                        .timing
                        .deadline_at_ms,
                ),
            },
            unresolved,
        ))
    }

    pub(super) async fn settle_unstarted_tools(
        &self,
        snapshot: &RunSnapshot,
        context: &ExecutionContext,
        budget: &RunBudget,
        cancelled: bool,
    ) -> Result<(), ContractError> {
        let requests: std::collections::BTreeSet<_> = snapshot
            .tool_ledger
            .iter()
            .filter(|entry| {
                matches!(
                    entry.state,
                    ToolCallState::Planned {}
                        | ToolCallState::ApprovalPending { .. }
                        | ToolCallState::InputPending { .. }
                )
            })
            .map(|entry| entry.call.model_request_id.clone())
            .collect();
        if requests.is_empty() {
            return Ok(());
        }
        let round = self.tool_round(budget).await?;
        for request in requests {
            round
                .settle_unstarted(
                    &request,
                    if cancelled {
                        ToolResultStatus::Cancelled
                    } else {
                        ToolResultStatus::Failed
                    },
                    Id::new(if cancelled {
                        "cancelled"
                    } else {
                        "run_stopped"
                    })?,
                    context,
                    budget,
                )
                .await?;
        }
        Ok(())
    }
}
```

## `crates/wickle/src/context_projection.rs`

```rust
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, io,
    num::NonZeroU64,
};

use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Value, json};

use crate::{
    AgentProfile, CompiledTool, ComponentKind, ContentBlock, ContractError, ErrorCode, Id,
    InputContent, Instructions, JsonDigest, JsonObject, Message, MessageOrigin, MessageRole,
    ModelContent, ModelMessage, ModelOutput, ModelPurpose, ModelRequest, ModelResponseLimits,
    ModelRole, ModelTool, OpaqueContinuation, RecordRef, ResolvedComponent, ResolvedModelRoute,
    ResolvedProfile, RunRequest, Scope, ToolBindingRef, ToolResultStatus, VersionedRef, Visibility,
    parse_json, serialization::data_digest,
};

/// Version of the session prefix and byte-bounded projection contract.
pub const CONTEXT_ASSEMBLER_VERSION: &str = "wickle.context-assembler.v1";

/// Instruction data already resolved and authorized by the Host; no loader is invoked here.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstructionAssetContent {
    /// Exact instruction asset selected by the profile.
    pub asset: VersionedRef,
    /// Complete text to pin; it is never silently truncated.
    pub text: String,
}

impl fmt::Debug for InstructionAssetContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InstructionAssetContent")
            .field("asset", &self.asset)
            .finish_non_exhaustive()
    }
}

/// A trusted assembly's mapping from a selected profile reference to its compiled tool.
/// The later adapter factory must attest that an export actually supplies this descriptor.
#[derive(Debug, Clone)]
pub struct PromptToolBinding {
    /// Exact selected catalog reference or adapter export, including alias/configuration.
    pub selection: ToolBindingRef,
    /// Validated immutable input split; its full schema is not copied into the prefix.
    pub compiled: CompiledTool,
}

/// Initial skill listing metadata, deliberately separate from skill body loading.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillManifest {
    /// Exact selected skill identity and version.
    pub skill: VersionedRef,
    /// Short public listing name.
    pub name: String,
    /// Public purpose description, not an automatically executed instruction body.
    pub description: String,
    /// Trusted catalog manifest identity pinned with this listing.
    pub manifest_digest: JsonDigest,
}

impl fmt::Debug for SkillManifest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SkillManifest")
            .field("skill", &self.skill)
            .field("manifest_digest", &self.manifest_digest)
            .finish_non_exhaustive()
    }
}

/// Model-facing part of a tool pinned into the session prefix.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedPromptTool {
    /// Exact profile selection, retaining alias and binding identity.
    pub selection: ToolBindingRef,
    /// Exact underlying tool descriptor identity.
    pub tool: VersionedRef,
    /// Compiler contract used for input projection.
    pub compiler_version: String,
    /// Full compiled input-contract digest, without its hidden schemas or values.
    pub compiled_digest: JsonDigest,
    /// Original descriptor identity used by stored core ToolCall records.
    pub descriptor_digest: JsonDigest,
    /// Identity of the derived model-input schema.
    pub model_schema_digest: JsonDigest,
    /// Only the model-visible tool schema and public description.
    pub model_tool: ModelTool,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PromptData {
    assembler_version: String,
    scope: Scope,
    profile: AgentProfile,
    initial_resolution_digest: JsonDigest,
    pinned_components: Vec<ResolvedComponent>,
    host_instructions: Vec<String>,
    profile_asset: Option<InstructionAssetContent>,
    tools: Vec<PinnedPromptTool>,
    skills: Vec<SkillManifest>,
}

/// Owned session prefix. It can be serialized for protected storage but cannot be
/// deserialized without verifying a trusted expected digest, scope and profile.
/// Its digest equals the digest of the serialized value stored by ProtectedRecord.
#[derive(Clone)]
pub struct PromptSnapshot {
    data: PromptData,
    digest: JsonDigest,
}

impl Serialize for PromptSnapshot {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.data.serialize(serializer)
    }
}
impl fmt::Debug for PromptSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PromptSnapshot")
            .field("digest", &self.digest)
            .field("tool_count", &self.data.tools.len())
            .field("skill_count", &self.data.skills.len())
            .finish_non_exhaustive()
    }
}

impl PromptSnapshot {
    /// Pin already-authorized assets in profile order. This does not create adapter
    /// factories, fetch instructions, or load skill bodies. Profile text cannot
    /// delete or replace the independently owned Host message. Actual instruction
    /// adherence within a provider's system channel still requires evaluation;
    /// execution permissions are enforced separately by PolicyGate.
    pub fn create(
        profile: &ResolvedProfile,
        host_instructions: Vec<String>,
        profile_asset: Option<InstructionAssetContent>,
        mut tools: Vec<PromptToolBinding>,
        mut skills: Vec<SkillManifest>,
    ) -> Result<Self, ContractError> {
        if tools.len() != profile.profile().tools.len()
            || skills.len() != profile.profile().skills.len()
        {
            return Err(invalid("prompt.selections"));
        }
        let mut pinned_tools = Vec::new();
        for selection in &profile.profile().tools {
            let index = tools
                .iter()
                .position(|binding| &binding.selection == selection)
                .ok_or_else(|| invalid("prompt.tools"))?;
            let binding = tools.remove(index);
            let mut model_tool = binding.compiled.to_model_tool();
            if let ToolBindingRef::Export(export) = selection {
                if let Some(alias) = &export.alias {
                    model_tool.name = alias.clone();
                }
            }
            pinned_tools.push(PinnedPromptTool {
                selection: selection.clone(),
                tool: binding.compiled.descriptor().tool.clone(),
                compiler_version: binding.compiled.compiler_version().into(),
                compiled_digest: binding.compiled.digest().clone(),
                descriptor_digest: binding.compiled.descriptor_digest().clone(),
                model_schema_digest: binding.compiled.model_schema_digest().clone(),
                model_tool,
            });
        }
        let mut pinned_skills = Vec::new();
        for selection in &profile.profile().skills {
            let index = skills
                .iter()
                .position(|manifest| {
                    manifest.skill.id == selection.skill_id
                        && manifest.skill.version == selection.version
                })
                .ok_or_else(|| invalid("prompt.skills"))?;
            pinned_skills.push(skills.remove(index));
        }
        let data = PromptData {
            assembler_version: CONTEXT_ASSEMBLER_VERSION.into(),
            scope: profile.scope().clone(),
            profile: profile.profile().clone(),
            initial_resolution_digest: profile.resolution_digest().clone(),
            pinned_components: non_model_components(profile),
            host_instructions,
            profile_asset,
            tools: pinned_tools,
            skills: pinned_skills,
        };
        let snapshot = Self {
            digest: data_digest(&data),
            data,
        };
        snapshot.validate_data()?;
        Ok(snapshot)
    }

    /// Canonical identity of the exact protected serialized prefix.
    pub fn digest(&self) -> JsonDigest {
        self.digest.clone()
    }
    /// Read pinned public tool metadata and identities, without hidden input schemas.
    pub fn tools(&self) -> &[PinnedPromptTool] {
        &self.data.tools
    }
    /// Read the original selected skill listings, without fetching newer versions.
    pub fn skills(&self) -> &[SkillManifest] {
        &self.data.skills
    }
    /// Read the authenticated scope in which this prefix was pinned.
    pub fn scope(&self) -> &Scope {
        &self.data.scope
    }

    /// Restore a protected record using its trusted digest and the current run's
    /// resolved profile. A new run may resolve a different model binding only.
    /// Resume must continue to use the original run's profile and selected route;
    /// this method is not an authorization to replace either during a run.
    pub fn restore(
        input: &str,
        expected_digest: &JsonDigest,
        profile: &ResolvedProfile,
        scope: &Scope,
    ) -> Result<Self, ContractError> {
        let data: PromptData =
            serde_json::from_value(parse_json(input).map_err(|_| invalid("prompt"))?)
                .map_err(|_| invalid("prompt"))?;
        let snapshot = Self {
            digest: data_digest(&data),
            data,
        };
        snapshot.validate_for(profile, scope, expected_digest)?;
        Ok(snapshot)
    }

    /// Require the stored prefix identity, scope, profile, and all non-model assets.
    pub fn validate_for(
        &self,
        profile: &ResolvedProfile,
        scope: &Scope,
        expected_digest: &JsonDigest,
    ) -> Result<(), ContractError> {
        if &self.digest != expected_digest
            || &self.data.scope != scope
            || profile.scope() != scope
            || self.data.profile.digest() != *profile.profile_digest()
            || self.data.pinned_components != non_model_components(profile)
        {
            return Err(mismatch("prompt"));
        }
        self.validate_data()
    }

    fn validate_data(&self) -> Result<(), ContractError> {
        if self.data.assembler_version != CONTEXT_ASSEMBLER_VERSION
            || data_digest(&self.data) != self.digest
        {
            return Err(mismatch("prompt.version"));
        }
        match (&self.data.profile.instructions, &self.data.profile_asset) {
            (Instructions::Text(_), None) => {}
            (Instructions::Asset(reference), Some(asset)) if reference.asset_ref == asset.asset => {
            }
            _ => return Err(mismatch("prompt.instructions")),
        }
        if self.data.tools.len() != self.data.profile.tools.len()
            || self.data.skills.len() != self.data.profile.skills.len()
        {
            return Err(mismatch("prompt.selections"));
        }
        let mut names = BTreeSet::new();
        for (selection, tool) in self.data.profile.tools.iter().zip(&self.data.tools) {
            if selection != &tool.selection
                || !names.insert(&tool.model_tool.name)
                || crate::canonical_digest(&tool.model_tool.model_input_schema)
                    != tool.model_schema_digest
            {
                return Err(mismatch("prompt.tools"));
            }
            match selection {
                ToolBindingRef::Catalog(reference) => {
                    if tool.tool.id != reference.tool_id || tool.tool.version != reference.version {
                        return Err(mismatch("prompt.tools"));
                    }
                }
                ToolBindingRef::Export(export) => {
                    let adapter = self
                        .data
                        .profile
                        .adapters
                        .as_ref()
                        .and_then(|adapters| {
                            adapters
                                .iter()
                                .find(|adapter| adapter.binding_id == export.adapter_binding)
                        })
                        .ok_or_else(|| mismatch("prompt.export"))?;
                    if !self.data.pinned_components.iter().any(|component| {
                        component.reference.kind == ComponentKind::Adapter
                            && component.reference.id == adapter.adapter_id
                            && component.reference.version.as_ref() == Some(&adapter.version)
                    }) || export
                        .alias
                        .as_ref()
                        .is_some_and(|alias| alias != &tool.model_tool.name)
                    {
                        return Err(mismatch("prompt.export"));
                    }
                }
            }
        }
        for (selected, manifest) in self.data.profile.skills.iter().zip(&self.data.skills) {
            if selected.skill_id != manifest.skill.id || selected.version != manifest.skill.version
            {
                return Err(mismatch("prompt.skills"));
            }
        }
        Ok(())
    }

    fn prefix(&self) -> Vec<ModelMessage> {
        let profile_text = match &self.data.profile.instructions {
            Instructions::Text(instructions) => instructions.text.clone(),
            Instructions::Asset(_) => self
                .data
                .profile_asset
                .as_ref()
                .expect("validated instruction asset")
                .text
                .clone(),
        };
        let mut messages = vec![
            ModelMessage {
                role: ModelRole::System,
                content: self
                    .data
                    .host_instructions
                    .iter()
                    .map(|text| ModelContent::Text { text: text.clone() })
                    .collect(),
            },
            ModelMessage {
                role: ModelRole::System,
                content: vec![ModelContent::Text { text: profile_text }],
            },
        ];
        if !self.data.skills.is_empty() {
            messages.push(ModelMessage {
                role: ModelRole::User,
                content: vec![ModelContent::Json {
                    value: json!({"kind":"available_skills", "skills":self.data.skills}),
                }],
            });
        }
        messages
    }
}

fn non_model_components(profile: &ResolvedProfile) -> Vec<ResolvedComponent> {
    profile
        .components()
        .iter()
        .filter(|component| component.reference.kind != ComponentKind::ModelBinding)
        .cloned()
        .collect()
}

/// Source classification of already-authorized context data. None grants system authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextOrigin {
    /// Additional user-provided context, distinct from the preserved original request.
    User,
    /// Data associated with a selected pinned skill; no loader runs here.
    Skill,
    /// Data associated with a selected tool.
    Tool,
    /// External retrieved data, not trusted instructions.
    Retrieval,
    /// Recalled memory, not a policy grant.
    Memory,
    /// Verification feedback, not a Host instruction replacement.
    Verification,
}

/// Scope of context lifetime. An item outside its lifetime is explicitly omitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextLifetime {
    /// Context valid throughout one session.
    Session {
        /// Owning session.
        session_id: Id,
    },
    /// Context valid during one run.
    Run {
        /// Owning run.
        run_id: Id,
    },
    /// Context valid only for one logical model step.
    Step {
        /// Owning run.
        run_id: Id,
        /// Logical step, preserved across physical retries.
        model_step_id: Id,
    },
}

/// Selection importance, independent of source authority and provider role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextPriority {
    /// Fail if this active item cannot fit in full.
    Required,
    /// Include whole if remaining bounds permit it.
    Optional,
}

/// Data with explicit source, scope, integrity and lifetime. Constructing this DTO
/// does not authenticate provenance; callers must authorize sources before supply.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextItem {
    /// Stable source item identity.
    pub item_id: Id,
    /// Claimed source classification, retained in a data envelope.
    pub origin: ContextOrigin,
    /// Exact source/asset identity and version.
    pub source_ref: VersionedRef,
    /// Authenticated source scope supplied by the Host.
    pub scope: Scope,
    /// Explicitly selected content, not a system map or raw protected record.
    pub content: Vec<InputContent>,
    /// Digest of all other fields, checked again at projection.
    pub digest: JsonDigest,
    /// Session/run/step applicability.
    pub lifetime: ContextLifetime,
    /// Required versus optional selection, without elevated instruction authority.
    pub priority_class: ContextPriority,
}

impl ContextItem {
    /// Own supplied data and compute its source/lifetime/content identity.
    pub fn new(
        item_id: Id,
        origin: ContextOrigin,
        source_ref: VersionedRef,
        scope: Scope,
        content: Vec<InputContent>,
        lifetime: ContextLifetime,
        priority_class: ContextPriority,
    ) -> Self {
        let digest = data_digest(&(
            &item_id,
            origin,
            &source_ref,
            &scope,
            &content,
            &lifetime,
            priority_class,
        ));
        Self {
            item_id,
            origin,
            source_ref,
            scope,
            content,
            digest,
            lifetime,
            priority_class,
        }
    }
    fn valid_digest(&self) -> bool {
        self.digest
            == data_digest(&(
                &self.item_id,
                self.origin,
                &self.source_ref,
                &self.scope,
                &self.content,
                &self.lifetime,
                self.priority_class,
            ))
    }
}
impl fmt::Debug for ContextItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContextItem")
            .field("item_id", &self.item_id)
            .field("origin", &self.origin)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

/// Already-authorized typed provider replay data, not a generic JSON record loader.
#[derive(Debug, Clone)]
pub struct ScopedOpaque {
    /// Scope from which the protected record was read.
    pub scope: Scope,
    /// Exact reference whose digest covers the serialized OpaqueContinuation.
    pub reference: RecordRef,
    /// Provider that owns the record.
    pub provider: Id,
    /// Typed continuation with an exact route identity.
    pub continuation: OpaqueContinuation,
}

/// Finite projection size, separate from model token context capacity and usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionLimits {
    /// Maximum serialized final ModelRequest bytes, including schemas and metadata.
    pub max_bytes: usize,
    /// Maximum projected content blocks plus model tool definitions.
    pub max_items: usize,
}

/// Read-only projection inputs. Transcript must come from a trusted, scoped session
/// store; Message alone cannot authenticate its owner or prove history completeness.
pub struct ProjectionInput<'a> {
    /// Original resolved profile of this run; never re-resolve it during resume.
    pub profile: &'a ResolvedProfile,
    /// Authenticated execution scope.
    pub scope: &'a Scope,
    /// Current owning run.
    pub run_id: &'a Id,
    /// Logical step, identical to request_id before physical invocation allocation.
    pub model_step_id: &'a Id,
    /// Original persisted run request.
    pub current_request: &'a RunRequest,
    /// Exact stored user message containing that request, to prevent duplication.
    pub current_request_message_id: &'a Id,
    /// Owned-store history borrowed without mutation, including the current user message.
    pub transcript: &'a [Message],
    /// Already-authorized context items; no external source is queried here.
    pub context_items: &'a [ContextItem],
    /// Already-authorized opaque records with typed scope/provider/route metadata.
    pub opaque_records: &'a [ScopedOpaque],
    /// Trusted digest from the session's pinned prompt record.
    pub expected_prompt_digest: &'a JsonDigest,
    /// Logical step identity; ModelExchange later assigns a separate physical request ID.
    pub request_id: Id,
    /// Accounting purpose of this invocation.
    pub purpose: ModelPurpose,
    /// Already-selected immutable model route.
    pub route: ResolvedModelRoute,
    /// Already-resolved requested output mode.
    pub output: ModelOutput,
    /// Provider output-token request, not an estimate of input bytes.
    pub max_output_tokens: NonZeroU64,
    /// Host-owned logical options preserved in the final ModelRequest, outside prompt content.
    /// The selected catalog schemas and adapter define supported keys and wire mapping.
    pub options: JsonObject,
    /// Provider request/response decoding bounds.
    pub response_limits: ModelResponseLimits,
    /// Byte/item projection bounds, not a tokenizer or model context-window check.
    pub limits: ProjectionLimits,
}

/// Separate model projection and explicit selection provenance. No original messages change.
#[derive(Debug)]
pub struct ContextProjection {
    /// Complete prepared model request.
    pub request: ModelRequest,
    /// Original message identities represented in the model request.
    pub selected_message_ids: Vec<Id>,
    /// Original message identities omitted by visibility or whole-run selection.
    pub dropped_message_ids: Vec<Id>,
    /// Active supplied context items included in full.
    pub selected_context_ids: Vec<Id>,
    /// Context items omitted by lifetime or optional-item bounds.
    pub dropped_context_ids: Vec<Id>,
    /// Identity of the unchanged session prefix.
    pub prompt_digest: JsonDigest,
}

/// Prefix reuse and conservative selection without retrieval, loading or compaction.
#[derive(Debug, Clone, Copy, Default)]
pub struct ContextAssembler;

struct RunGroup {
    run_id: Id,
    messages: Vec<(Id, ModelMessage)>,
    has_tool_round: bool,
    has_unknown: bool,
}

impl ContextAssembler {
    /// Construct an assembler without doing I/O.
    pub fn new() -> Self {
        Self
    }

    /// Preserve the fixed prefix and all model-visible current-run messages. Older
    /// complete runs and optional items are added newest first without splitting
    /// tool rounds. The latest visible tool round and runs with unknown tool results
    /// are mandatory. Any unfinished round or oversized required input fails.
    /// Byte bounds do not claim to estimate or enforce provider token context size.
    pub fn project(
        &self,
        snapshot: &PromptSnapshot,
        input: ProjectionInput<'_>,
    ) -> Result<ContextProjection, ContractError> {
        snapshot.validate_for(input.profile, input.scope, input.expected_prompt_digest)?;
        if input.request_id != *input.model_step_id
            || input.limits.max_bytes == 0
            || input.limits.max_items == 0
        {
            return Err(invalid("projection.identity_or_limits"));
        }
        validate_current_request(&input)?;
        let groups = project_transcript(snapshot, &input)?;
        let current = groups
            .iter()
            .position(|group| &group.run_id == input.run_id)
            .ok_or_else(|| invalid("projection.current_run"))?;
        if current + 1 != groups.len() {
            return Err(invalid("projection.incomplete_round"));
        }
        let mut selected_groups = BTreeSet::from([current]);
        if let Some(index) = groups.iter().rposition(|group| group.has_tool_round) {
            selected_groups.insert(index);
        }
        selected_groups.extend(
            groups
                .iter()
                .enumerate()
                .filter(|(_, group)| group.has_unknown)
                .map(|(index, _)| index),
        );
        let mut context = Vec::new();
        let mut active = Vec::new();
        let mut seen_context = BTreeSet::new();
        for item in input.context_items {
            if !seen_context.insert(&item.item_id) || !item.valid_digest() {
                return Err(invalid("context_item.digest"));
            }
            if &item.scope != input.scope {
                return Err(mismatch("context_item.scope"));
            }
            if item.origin == ContextOrigin::Skill
                && !snapshot
                    .data
                    .skills
                    .iter()
                    .any(|manifest| manifest.skill == item.source_ref)
            {
                return Err(mismatch("context_item.skill"));
            }
            if item.origin == ContextOrigin::Tool
                && !snapshot
                    .data
                    .tools
                    .iter()
                    .any(|tool| tool.tool == item.source_ref)
            {
                return Err(mismatch("context_item.tool"));
            }
            let applicable = match &item.lifetime {
                ContextLifetime::Session { session_id } => {
                    session_id == &input.current_request.session_id
                }
                ContextLifetime::Run { run_id } => run_id == input.run_id,
                ContextLifetime::Step {
                    run_id,
                    model_step_id,
                } => run_id == input.run_id && model_step_id == input.model_step_id,
            };
            let message = if applicable {
                Some(ModelMessage {
                    role: ModelRole::User,
                    content: vec![ModelContent::Json {
                        value: json!({
                            "kind":"context_data", "item_id":item.item_id, "origin":item.origin,
                            "source_ref":item.source_ref,
                            "content":item.content.iter().map(|content| safe_value(content, input.scope)).collect::<Result<Vec<_>,_>>()?
                        }),
                    }],
                })
            } else {
                None
            };
            active.push(applicable);
            context.push(message);
        }
        let mut selected_context: BTreeSet<usize> = input
            .context_items
            .iter()
            .enumerate()
            .filter(|(index, item)| {
                active[*index] && item.priority_class == ContextPriority::Required
            })
            .map(|(index, _)| index)
            .collect();
        let make_request = |selected_groups: &BTreeSet<usize>,
                            selected_context: &BTreeSet<usize>| {
            let mut messages = snapshot.prefix();
            for index in selected_groups {
                messages.extend(
                    groups[*index]
                        .messages
                        .iter()
                        .map(|(_, message)| message.clone()),
                );
            }
            for index in selected_context {
                messages.push(context[*index].as_ref().expect("active context").clone());
            }
            ModelRequest {
                request_id: input.request_id.clone(),
                purpose: input.purpose,
                route: input.route.clone(),
                messages,
                tools: snapshot
                    .data
                    .tools
                    .iter()
                    .map(|tool| tool.model_tool.clone())
                    .collect(),
                output: input.output.clone(),
                max_output_tokens: input.max_output_tokens,
                options: input.options.clone(),
                limits: input.response_limits.clone(),
            }
        };
        if !fits(
            &make_request(&selected_groups, &selected_context),
            &input.limits,
        ) {
            return Err(budget());
        }
        for index in (0..current).rev() {
            if selected_groups.contains(&index) {
                continue;
            }
            selected_groups.insert(index);
            if !fits(
                &make_request(&selected_groups, &selected_context),
                &input.limits,
            ) {
                selected_groups.remove(&index);
            }
        }
        for index in (0..input.context_items.len()).rev() {
            if !active[index]
                || input.context_items[index].priority_class == ContextPriority::Required
            {
                continue;
            }
            selected_context.insert(index);
            if !fits(
                &make_request(&selected_groups, &selected_context),
                &input.limits,
            ) {
                selected_context.remove(&index);
            }
        }
        let request = make_request(&selected_groups, &selected_context);
        request
            .validate()
            .map_err(|_| invalid("projection.model_request"))?;
        let selected_message_ids: Vec<_> = selected_groups
            .iter()
            .flat_map(|index| groups[*index].messages.iter().map(|(id, _)| id.clone()))
            .collect();
        let selected_ids: BTreeSet<_> = selected_message_ids.iter().collect();
        Ok(ContextProjection {
            request,
            dropped_message_ids: input
                .transcript
                .iter()
                .filter(|message| !selected_ids.contains(&message.message_id))
                .map(|message| message.message_id.clone())
                .collect(),
            selected_message_ids,
            selected_context_ids: selected_context
                .iter()
                .map(|index| input.context_items[*index].item_id.clone())
                .collect(),
            dropped_context_ids: input
                .context_items
                .iter()
                .enumerate()
                .filter(|(index, _)| !selected_context.contains(index))
                .map(|(_, item)| item.item_id.clone())
                .collect(),
            prompt_digest: snapshot.digest(),
        })
    }
}

fn validate_current_request(input: &ProjectionInput<'_>) -> Result<(), ContractError> {
    let message = input
        .transcript
        .iter()
        .find(|message| &message.message_id == input.current_request_message_id)
        .ok_or_else(|| invalid("projection.current_request"))?;
    if &message.run_id != input.run_id
        || message.role != MessageRole::User
        || message.origin != MessageOrigin::User
        || !visible(message)
    {
        return Err(invalid("projection.current_request"));
    }
    let contents: Option<Vec<_>> = message
        .content
        .iter()
        .map(|block| match block {
            ContentBlock::Content { content } => Some(content),
            _ => None,
        })
        .collect();
    if contents.as_deref()
        != Some(
            input
                .current_request
                .input
                .iter()
                .collect::<Vec<_>>()
                .as_slice(),
        )
    {
        return Err(mismatch("projection.current_request"));
    }
    Ok(())
}

struct PendingCall {
    message_id: Id,
    provider_call_id: Id,
    visible: bool,
    known: bool,
}

fn project_transcript(
    snapshot: &PromptSnapshot,
    input: &ProjectionInput<'_>,
) -> Result<Vec<RunGroup>, ContractError> {
    let corrections = crate::message::tool_corrections(input.transcript)?;
    let mut groups = Vec::new();
    let mut seen_messages = BTreeSet::new();
    let mut seen_runs = BTreeSet::new();
    let mut previous_sequence = 0;
    let mut cursor = 0;
    while cursor < input.transcript.len() {
        let run_id = input.transcript[cursor].run_id.clone();
        if !seen_runs.insert(run_id.clone()) {
            return Err(invalid("transcript.run_order"));
        }
        let end = input.transcript[cursor..]
            .iter()
            .position(|message| message.run_id != run_id)
            .map_or(input.transcript.len(), |offset| cursor + offset);
        let mut projected = Vec::new();
        let mut has_tool_round = false;
        let mut has_unknown = false;
        let mut pending: BTreeMap<Id, PendingCall> = BTreeMap::new();
        let mut seen_calls = BTreeSet::new();
        for message in &input.transcript[cursor..end] {
            if message.sequence.get() <= previous_sequence
                || !seen_messages.insert(&message.message_id)
            {
                return Err(invalid("transcript.order"));
            }
            previous_sequence = message.sequence.get();
            let is_visible = visible(message);
            if is_visible {
                match message.role {
                    MessageRole::System => return Err(invalid("transcript.system_role")),
                    MessageRole::Assistant if message.origin != MessageOrigin::Model => {
                        return Err(invalid("transcript.origin"));
                    }
                    MessageRole::Tool if message.origin != MessageOrigin::Tool => {
                        return Err(invalid("transcript.origin"));
                    }
                    MessageRole::User
                        if matches!(
                            message.origin,
                            MessageOrigin::Host
                                | MessageOrigin::Profile
                                | MessageOrigin::Model
                                | MessageOrigin::Tool
                        ) =>
                    {
                        return Err(invalid("transcript.origin"));
                    }
                    _ => {}
                }
                if !pending.is_empty() && message.role != MessageRole::Tool {
                    return Err(invalid("transcript.incomplete_round"));
                }
            }
            let mut content = Vec::new();
            for block in &message.content {
                match block {
                    ContentBlock::ToolCall { call } => {
                        has_tool_round |= is_visible;
                        if message.role != MessageRole::Assistant
                            || message.origin != MessageOrigin::Model
                            || !seen_calls.insert(&call.call_id)
                        {
                            return Err(invalid("transcript.tool_call"));
                        }
                        let tool = snapshot
                            .data
                            .tools
                            .iter()
                            .find(|tool| tool.model_tool.name == call.tool_name);
                        if tool.is_some_and(|tool| {
                            Some(&tool.descriptor_digest) != call.descriptor_digest.as_ref()
                        }) {
                            return Err(mismatch("transcript.descriptor"));
                        }
                        pending.insert(
                            call.call_id.clone(),
                            PendingCall {
                                message_id: message.message_id.clone(),
                                provider_call_id: call.provider_call_id.clone(),
                                visible: is_visible,
                                known: tool.is_some(),
                            },
                        );
                        if is_visible {
                            content.push(ModelContent::ToolCall {
                                provider_call_id: call.provider_call_id.clone(),
                                name: call.tool_name.clone(),
                                arguments: call.model_inputs.clone(),
                            });
                        }
                    }
                    ContentBlock::ToolResult { result } => {
                        let result = corrections
                            .get(&message.message_id)
                            .map_or(result, |(_, result)| result);
                        if message.role != MessageRole::Tool
                            || message.origin != MessageOrigin::Tool
                        {
                            return Err(invalid("transcript.tool_result"));
                        }
                        let call = pending
                            .remove(&result.call_id)
                            .ok_or_else(|| invalid("transcript.tool_result"))?;
                        if call.message_id != result.call_message_id
                            || call.visible != is_visible
                            || (!call.known && result.status == ToolResultStatus::Succeeded)
                        {
                            return Err(invalid("transcript.tool_pair"));
                        }
                        if result.status == ToolResultStatus::Unknown
                            || result.effect == crate::ToolEffect::Unknown
                        {
                            if !is_visible {
                                return Err(invalid("transcript.hidden_unknown_effect"));
                            }
                            has_unknown = true;
                        }
                        if is_visible {
                            let values = result
                                .content
                                .iter()
                                .map(|item| safe_value(item, input.scope))
                                .collect::<Result<Vec<_>, _>>()?;
                            let mut value = json!({"status":result.status,"effect":result.effect,"content":values});
                            if let Some(failure) = &result.error {
                                value["error"] = json!({"code":failure.code});
                            }
                            content.push(ModelContent::ToolResult {
                                provider_call_id: call.provider_call_id,
                                content: value,
                            });
                        }
                    }
                    ContentBlock::Content { content: item } if is_visible => {
                        if message.role == MessageRole::Tool {
                            return Err(invalid("transcript.tool_result"));
                        }
                        content.push(safe_content(item, input.scope)?);
                    }
                    ContentBlock::ProviderOpaque {
                        provider,
                        route_digest,
                        data_ref,
                    } if is_visible => {
                        if message.role != MessageRole::Assistant
                            || message.origin != MessageOrigin::Model
                            || provider != &input.route.provider
                            || route_digest != &input.route.digest()
                        {
                            return Err(mismatch("transcript.opaque_route"));
                        }
                        let records: Vec<_> = input
                            .opaque_records
                            .iter()
                            .filter(|record| &record.reference == data_ref)
                            .collect();
                        if records.len() != 1 {
                            return Err(mismatch("transcript.opaque_record"));
                        }
                        let record = records[0];
                        if &record.scope != input.scope
                            || &record.provider != provider
                            || record.continuation.route_digest() != route_digest
                            || data_digest(&record.continuation) != data_ref.digest
                        {
                            return Err(mismatch("transcript.opaque_record"));
                        }
                        content.push(ModelContent::Opaque {
                            continuation: record.continuation.clone(),
                        });
                    }
                    _ => {}
                }
            }
            if is_visible && !content.is_empty() {
                let role = match message.role {
                    MessageRole::User => ModelRole::User,
                    MessageRole::Assistant => ModelRole::Assistant,
                    MessageRole::Tool => ModelRole::Tool,
                    MessageRole::System => unreachable!("visible System rejected"),
                };
                if message.role == MessageRole::User && message.origin != MessageOrigin::User {
                    let values = content
                        .iter()
                        .map(|content| {
                            serde_json::to_value(content).expect("model content serialization")
                        })
                        .collect::<Vec<_>>();
                    content = vec![ModelContent::Json {
                        value: json!({"kind":"transcript_data", "origin":message.origin,
                        "source_message_id":message.message_id, "content":values}),
                    }];
                }
                let source_id = corrections
                    .get(&message.message_id)
                    .map_or(&message.message_id, |(source_id, _)| source_id);
                projected.push((source_id.clone(), ModelMessage { role, content }));
            }
        }
        if !pending.is_empty() {
            return Err(invalid("transcript.incomplete_round"));
        }
        groups.push(RunGroup {
            run_id,
            messages: projected,
            has_tool_round,
            has_unknown,
        });
        cursor = end;
    }
    Ok(groups)
}

fn visible(message: &Message) -> bool {
    matches!(
        message.visibility,
        Visibility::Model | Visibility::UserAndModel
    )
}

fn safe_content(content: &InputContent, scope: &Scope) -> Result<ModelContent, ContractError> {
    match content {
        InputContent::Text { text } => Ok(ModelContent::Text { text: text.clone() }),
        InputContent::Json { value } => Ok(ModelContent::Json {
            value: value.clone(),
        }),
        _ => Ok(ModelContent::Json {
            value: safe_value(content, scope)?,
        }),
    }
}

fn safe_value(content: &InputContent, scope: &Scope) -> Result<Value, ContractError> {
    Ok(match content {
        InputContent::Text { text } => json!({"type":"text","text":text}),
        InputContent::Json { value } => json!({"type":"json","value":value}),
        InputContent::Artifact { reference } => {
            if &reference.scope != scope {
                return Err(mismatch("context.artifact_scope"));
            }
            json!({"type":"artifact","artifact_id":reference.artifact_id,"media_type":reference.media_type,
                "size_bytes":reference.size_bytes,"content_hash":reference.content_hash})
        }
        InputContent::Evidence { reference } => {
            let mut value = json!({"type":"evidence","source_id":reference.source_id,"version":reference.version,
                "location":reference.location,"content_hash":reference.content_hash});
            if let Some(quote) = &reference.quote {
                value["quote"] = json!(quote);
            }
            value
        }
    })
}

struct ByteCounter {
    written: usize,
    limit: usize,
}
impl io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.written = self
            .written
            .checked_add(bytes.len())
            .filter(|size| *size <= self.limit)
            .ok_or_else(|| io::Error::other("projection limit exceeded"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn fits(request: &ModelRequest, limits: &ProjectionLimits) -> bool {
    let count = request
        .messages
        .iter()
        .try_fold(request.tools.len(), |count, message| {
            count.checked_add(message.content.len())
        });
    if count.is_none_or(|count| count > limits.max_items) {
        return false;
    }
    serde_json::to_writer(
        &mut ByteCounter {
            written: 0,
            limit: limits.max_bytes.min(request.limits.max_input_bytes),
        },
        request,
    )
    .is_ok()
}
fn invalid(path: &str) -> ContractError {
    ContractError::new(ErrorCode::InvalidContext, path)
}
fn mismatch(path: &str) -> ContractError {
    ContractError::new(ErrorCode::ContextMismatch, path)
}
fn budget() -> ContractError {
    ContractError::new(
        ErrorCode::ContextBudgetExceeded,
        "projection.required_input",
    )
}
```

## `crates/wickle/src/error.rs`

```rust
use serde::{Deserialize, Serialize};

/// Stable categories for contract and profile validation failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorCode {
    /// Current policy or exact owner scope denies access.
    AccessDenied,
    /// The trusted policy failed or panicked; no permission was granted.
    PolicyUnavailable,
    /// The call's finite deadline elapsed.
    DeadlineExceeded,
    /// The current operation was cancelled.
    Cancelled,
    /// The Host has not supplied the required asynchronous runtime.
    RuntimeUnavailable,
    /// A configured call, repair, or recovery budget has no remaining capacity.
    BudgetExceeded,
    /// A required time reading or timer could not be obtained.
    ClockUnavailable,
    /// A monotonic reading regressed or a resumed UTC clock predates saved progress.
    ClockRegression,
    /// The Host identifier source could not generate an internal execution identifier.
    IdGenerationFailed,
    /// Input is not unambiguous, finite JSON.
    InvalidJson,
    /// Input does not match a data contract.
    InvalidContract,
    /// The requested model, version, binding or alias is not registered.
    ModelNotRegistered,
    /// An exact model/API/adapter/target binding or its evidence is inconsistent.
    ModelBindingInvalid,
    /// The required feature or declared capability limit is unsupported.
    ModelCapabilityUnsupported,
    /// Provider options do not satisfy the model and exact-binding contracts.
    ModelOptionUnsupported,
    /// The model or deployment is not verified immutable under the requested policy.
    ModelVersionUnpinned,
    /// Current observed model or deployment metadata differs from the pinned route.
    ModelVersionDrift,
    /// A bounded target inspection failed or could not establish availability.
    ModelInspectionUnavailable,
    /// A prior physical attempt has no known result and needs explicit recovery.
    ModelAttemptUnresolved,
    /// The selected release is retired or otherwise unavailable.
    ModelUnavailable,
    /// Catalog scope-independent revision, identity or serialized integrity differs.
    ModelCatalogMismatch,
    /// Static routing configuration has duplicate, missing or unsupported constraints.
    ModelRoutingInvalid,
    /// Saved routing metadata, request identity or selected route differs.
    ModelRoutingMismatch,
    /// Static routing constraints forbid this target or fallback reason.
    ModelRouteDenied,
    /// No eligible candidate remains in the finite permitted fallback list.
    ModelRoutesExhausted,
    /// Required support evidence is absent, failed, or of an insufficient kind.
    ModelSupportInsufficient,
    /// Estimated input plus reserved output exceeds this model/binding context budget.
    ModelContextIncompatible,
    /// Tool exposure, binding metadata, or a registered input schema is inconsistent.
    InvalidToolInputContract,
    /// The compiler cannot safely project this input schema or reference form.
    UnsupportedInputProjection,
    /// Model-owned or assembled tool arguments do not satisfy their input contract.
    InvalidArguments,
    /// An external receipt did not establish the tool effect; its saved wait remains.
    ToolEffectUnresolved,
    /// A supplied system value does not satisfy its registered input contract.
    SystemInputInvalid,
    /// A required registered system value is absent; the model must not invent it.
    SystemInputMissing,
    /// A read-only system-value resolver is unavailable or failed safely.
    SystemInputUnavailable,
    /// Supplied/resumed values or pinned input metadata differ from the saved snapshot.
    SystemInputsMismatch,
    /// Lookup permission requires separate Host approval before a target is known.
    SystemInputApprovalRequired,
    /// Resolver-count or serialized input-size bounds were exceeded.
    InputBindingLimitExceeded,
    /// Context identity, provenance structure, or call/result protocol is invalid.
    InvalidContext,
    /// Context scope, pinned assets, or protected-record identity does not match.
    ContextMismatch,
    /// Required context cannot fit the explicit byte or item bounds without truncation.
    ContextBudgetExceeded,
    /// The document format is not supported.
    UnsupportedSchemaVersion,
    /// A reference or binding is missing or inconsistent.
    InvalidReference,
    /// A required component or exact version is unavailable.
    ComponentUnavailable,
    /// A component uses an unsupported metadata contract.
    UnsupportedContractVersion,
    /// Selected components do not supply a required capability.
    CapabilityUnsupported,
    /// A configuration does not satisfy its registered schema.
    InvalidConfiguration,
    /// A registered schema is invalid or requires unsupported resolution.
    InvalidSchema,
    /// A profile differs from the profile pinned to an existing execution.
    ProfileMismatch,
    /// Stored data violates checkpoint invariants.
    InvalidSnapshot,
    /// The requested run, session, or protected record is absent in this exact scope.
    StateNotFound,
    /// An existing request identity was reused with different logical input.
    RequestConflict,
    /// The session already has a running or waiting run.
    SessionBusy,
    /// A proposed run identifier already belongs to another request in this scope.
    RunConflict,
    /// The compare-and-swap revision no longer matches saved state.
    RevisionConflict,
    /// Another unexpired execution lease already owns the run.
    LeaseBusy,
    /// The execution lease expired or no longer matches its owner and generation.
    LeaseLost,
    /// A candidate change violates immutable data or state-transition rules.
    InvalidTransition,
    /// An event has a duplicate identity, invalid sequence, or inconsistent references.
    InvalidEvent,
    /// A message has a duplicate identity, invalid sequence, or wrong owning run.
    InvalidMessage,
    /// Immutable record content or a requested reference digest conflicts.
    RecordConflict,
    /// Authoritative storage is unavailable; no successful commit is implied.
    PersistenceUnavailable,
}

/// A validation error that does not retain submitted values or credentials.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code:?} at {path}")]
pub struct ContractError {
    /// Machine-readable failure category.
    pub code: ErrorCode,
    /// Contract field or reference location, without submitted values.
    pub path: String,
}

impl ContractError {
    /// Construct an error using a safe contract location.
    pub fn new(code: ErrorCode, path: impl Into<String>) -> Self {
        Self {
            code,
            path: path.into(),
        }
    }
}
```

## `crates/wickle/src/input_binding.rs`

```rust
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    io,
    panic::AssertUnwindSafe,
    sync::Arc,
};

use futures_util::FutureExt;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::{
    CommitInput, CompiledTool, ContractError, ErrorCode, ExecutionContext, ExecutionContextData,
    Id, IdSource, JsonDigest, JsonObject, PolicyAction, PolicyDecision, PolicyGate, PolicyRequest,
    PortFuture, ProtectedRecord, RecordRef, RunBudget, RunSnapshot, Scope, SystemInputDefinition,
    SystemInputRegistry, SystemInputSnapshotRef, SystemInputSource, SystemInputs, ToolBindingRef,
    ToolCall, ToolCallState, ToolPolicyInput, VersionedRef, serialization::data_digest,
    tool_schema::compile_validator,
};

const RUN_INPUT_VERSION: &str = "wickle.run-system-inputs.v1";
const BOUND_INPUT_VERSION: &str = "wickle.bound-tool-input.v1";

/// Finite resolver and input-size bounds. They are independent of model token budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputBindingLimits {
    /// Maximum distinct resolver keys read for one new call; zero disables resolver reads.
    pub max_resolver_calls: usize,
    /// Maximum serialized bytes in one resolved or run-supplied value.
    pub max_value_bytes: usize,
    /// Maximum protected run-input or bound-input record size.
    pub max_bound_bytes: usize,
}
impl Default for InputBindingLimits {
    fn default() -> Self {
        Self {
            max_resolver_calls: 64,
            max_value_bytes: 65_536,
            max_bound_bytes: 1_048_576,
        }
    }
}
impl InputBindingLimits {
    fn validate(self) -> Result<(), ContractError> {
        if self.max_value_bytes == 0 || self.max_bound_bytes == 0 {
            return Err(error(ErrorCode::InvalidContract, "input_binding.limits"));
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunInputData {
    schema_version: String,
    scope: Scope,
    values: SystemInputs,
    definitions: BTreeMap<Id, SystemInputDefinition>,
}

/// Owned admission-time values and definition metadata. No resolver executes during
/// capture, and a missing value is not replaced by a schema default or generated ID.
#[derive(Clone)]
pub struct RunSystemInputs {
    data: RunInputData,
}

impl Serialize for RunSystemInputs {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.data.serialize(serializer)
    }
}
impl fmt::Debug for RunSystemInputs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunSystemInputs")
            .field("value_count", &self.data.values.values().len())
            .field("definition_count", &self.data.definitions.len())
            .finish_non_exhaustive()
    }
}

impl RunSystemInputs {
    /// Validate supplied keys/types and freeze owned values with the default finite bounds.
    pub fn capture(
        scope: Scope,
        supplied: Option<SystemInputs>,
        registry: &SystemInputRegistry,
    ) -> Result<Self, ContractError> {
        Self::capture_with_limits(scope, supplied, registry, InputBindingLimits::default())
    }
    /// Capture using explicit finite size bounds. Missing registered keys are allowed.
    pub fn capture_with_limits(
        scope: Scope,
        supplied: Option<SystemInputs>,
        registry: &SystemInputRegistry,
        limits: InputBindingLimits,
    ) -> Result<Self, ContractError> {
        limits.validate()?;
        let snapshot = Self {
            data: RunInputData {
                schema_version: RUN_INPUT_VERSION.into(),
                scope,
                values: supplied.unwrap_or_default(),
                definitions: registry.definitions().clone(),
            },
        };
        snapshot.validate_data()?;
        check_size(&snapshot, limits.max_bound_bytes)?;
        for value in snapshot.values().values() {
            check_size(value, limits.max_value_bytes)?;
        }
        Ok(snapshot)
    }
    /// Explicit access for the trusted binder; never automatic model projection.
    pub fn values(&self) -> &JsonObject {
        self.data.values.values()
    }
    /// Definition revisions and schemas pinned at admission.
    pub fn definitions(&self) -> &BTreeMap<Id, SystemInputDefinition> {
        &self.data.definitions
    }
    /// Exact owning scope of these values.
    pub fn scope(&self) -> &Scope {
        &self.data.scope
    }
    /// Digest of the complete protected serialized snapshot.
    pub fn digest(&self) -> JsonDigest {
        data_digest(&self.data)
    }
    /// Create the immutable record to include in the admission transaction.
    pub fn to_record(&self, record_id: Id, revision: u64) -> ProtectedRecord {
        ProtectedRecord::new(
            record_id,
            revision,
            serde_json::to_value(self).expect("serializable input data"),
        )
    }
    /// Create the run checkpoint reference after verifying its protected record identity.
    pub fn snapshot_ref(
        &self,
        record: &RecordRef,
    ) -> Result<SystemInputSnapshotRef, ContractError> {
        if record.digest != self.digest() {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "system_inputs.record",
            ));
        }
        Ok(SystemInputSnapshotRef {
            snapshot_ref: record.clone(),
            values_digest: data_digest(self.values()),
            definition_versions: self
                .definitions()
                .iter()
                .map(|(key, definition)| (key.clone(), definition.version.clone()))
                .collect(),
        })
    }
    /// Restore exact stored data and verify every pinned definition against the registry.
    /// Additional unrelated registry keys do not replace or enlarge the saved snapshot.
    pub fn restore(
        record: &ProtectedRecord,
        reference: &SystemInputSnapshotRef,
        scope: &Scope,
        registry: &SystemInputRegistry,
    ) -> Result<Self, ContractError> {
        if record.reference() != &reference.snapshot_ref {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "system_inputs.record",
            ));
        }
        let snapshot = Self::from_value(record.value(), reference, scope)?;
        if snapshot
            .definitions()
            .iter()
            .any(|(key, definition)| registry.get(key) != Some(definition))
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "system_inputs.definitions",
            ));
        }
        Ok(snapshot)
    }
    /// Omission reuses saved values. Any supplied map, including an empty map, must match.
    pub fn validate_resume(&self, supplied: Option<&SystemInputs>) -> Result<(), ContractError> {
        if supplied.is_some_and(|values| data_digest(values.values()) != data_digest(self.values()))
        {
            return Err(error(ErrorCode::SystemInputsMismatch, "system_inputs"));
        }
        Ok(())
    }
    fn validate_data(&self) -> Result<(), ContractError> {
        if self.data.schema_version != RUN_INPUT_VERSION
            || self
                .definitions()
                .iter()
                .any(|(key, definition)| key != &definition.key)
        {
            return Err(error(
                ErrorCode::SystemInputInvalid,
                "system_inputs.snapshot",
            ));
        }
        SystemInputRegistry::new(self.definitions().values().cloned().collect())
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "system_inputs.definitions"))?;
        for (key, value) in self.values() {
            let key = Id::new(key.clone())
                .map_err(|_| error(ErrorCode::SystemInputInvalid, "system_inputs.key"))?;
            let definition = self
                .definitions()
                .get(&key)
                .ok_or_else(|| error(ErrorCode::SystemInputInvalid, "system_inputs.key"))?;
            if !matches!(definition.source, SystemInputSource::Run {}) {
                return Err(error(ErrorCode::SystemInputInvalid, "system_inputs.source"));
            }
            validate_value(definition, value)?;
        }
        Ok(())
    }
    pub(crate) fn from_value(
        value: &Value,
        reference: &SystemInputSnapshotRef,
        scope: &Scope,
    ) -> Result<Self, ContractError> {
        let data: RunInputData = serde_json::from_value(value.clone())
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "system_inputs.snapshot"))?;
        let snapshot = Self { data };
        snapshot.validate_data()?;
        if snapshot.scope() != scope
            || snapshot.snapshot_ref(&reference.snapshot_ref)? != *reference
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "system_inputs.snapshot",
            ));
        }
        Ok(snapshot)
    }
}

/// One exact read-only resolver lookup, without other system values or credentials.
#[derive(Clone)]
pub struct SystemInputResolveRequest {
    /// Registered key being requested.
    pub key: Id,
    /// Pinned value-definition revision.
    pub definition_version: Id,
    /// Exact resolver implementation selected by the definition.
    pub resolver_ref: VersionedRef,
    /// Normalized model-owned arguments only, including declared top-level defaults.
    pub model_inputs: JsonObject,
}
impl fmt::Debug for SystemInputResolveRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SystemInputResolveRequest")
            .field("key", &self.key)
            .field("definition_version", &self.definition_version)
            .finish_non_exhaustive()
    }
}

/// Current actor and execution bounds supplied to a trusted read-only resolver.
#[derive(Debug, Clone)]
pub struct SystemInputResolveContext {
    /// Authenticated scope, not a value extracted from the model's arguments.
    pub scope: Scope,
    /// Current principal; it does not rewrite the run's original system-input values.
    pub principal_ref: Id,
    /// Current capability grant, checked by policy and the resolver's own backend.
    pub capability_grant_ref: Id,
    /// Current owning run.
    pub run_id: Id,
    /// Original logical call identity.
    pub call_id: Id,
    /// Deadline for this lookup.
    pub deadline: tokio::time::Instant,
    /// Child cancellation signal linked to both execution and caller cancellation.
    pub cancellation: CancellationToken,
}

/// Data and source revision returned by a resolver, or recorded from a run snapshot.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedSystemInput {
    /// Supplied JSON value; explicit null is different from an absent result.
    pub value: Value,
    /// Source data revision, not a newly invented foreign key.
    pub revision: Id,
}
impl fmt::Debug for ResolvedSystemInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ResolvedSystemInput(<redacted>)")
    }
}

/// Trusted read-only lookup port. It must honor scope, principal, deadline and
/// cancellation, and must not hide business writes or create missing foreign keys.
pub trait SystemInputResolver: Send + Sync {
    /// Read one exact registered key; None means absent, not JSON null.
    fn resolve<'a>(
        &'a self,
        request: &'a SystemInputResolveRequest,
        context: &'a SystemInputResolveContext,
    ) -> PortFuture<'a, Option<ResolvedSystemInput>>;
}

/// One hidden parameter's fixed source, revision and optional value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundSystemInput {
    /// Registry key, which may differ from the handler parameter name.
    pub key: Id,
    /// Value-definition version pinned by the compiler and admission snapshot.
    pub definition_version: Id,
    /// Run snapshot or exact resolver implementation.
    pub source: SystemInputSource,
    /// None is absence; Some with value:null is an explicitly supplied null.
    pub resolved: Option<ResolvedSystemInput>,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundInputData {
    schema_version: String,
    scope: Scope,
    run_id: Id,
    call_id: Id,
    tool: VersionedRef,
    descriptor_digest: JsonDigest,
    compiled_digest: JsonDigest,
    compiler_version: String,
    original_model_inputs: JsonObject,
    normalized_model_inputs: JsonObject,
    run_inputs_ref: Option<SystemInputSnapshotRef>,
    system_inputs: BTreeMap<String, BoundSystemInput>,
    execution_args: JsonObject,
}

/// Immutable execution inputs. Serialization is only for protected storage/policy,
/// never a replacement for the original model ToolCall or its transcript message.
#[derive(Clone, Serialize)]
pub struct BoundToolInput {
    data: BoundInputData,
    binding_digest: JsonDigest,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundInputRecord {
    data: BoundInputData,
    binding_digest: JsonDigest,
}

impl fmt::Debug for BoundToolInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundToolInput")
            .field("call_id", &self.data.call_id)
            .field("tool", &self.data.tool)
            .field("binding_digest", &self.binding_digest)
            .finish_non_exhaustive()
    }
}

impl BoundToolInput {
    /// Exact owning resource scope.
    pub fn scope(&self) -> &Scope {
        &self.data.scope
    }
    /// Owning run identity.
    pub fn run_id(&self) -> &Id {
        &self.data.run_id
    }
    /// Stable logical call identity.
    pub fn call_id(&self) -> &Id {
        &self.data.call_id
    }
    /// Exact registered tool identity.
    pub fn tool(&self) -> &VersionedRef {
        &self.data.tool
    }
    /// Original descriptor digest.
    pub fn descriptor_digest(&self) -> &JsonDigest {
        &self.data.descriptor_digest
    }
    /// Compiler, schema and selected system-definition identity.
    pub fn compiled_digest(&self) -> &JsonDigest {
        &self.data.compiled_digest
    }
    /// Pinned compiler contract version.
    pub fn compiler_version(&self) -> &str {
        &self.data.compiler_version
    }
    /// Unmodified arguments originally recorded for the model call.
    pub fn original_model_inputs(&self) -> &JsonObject {
        &self.data.original_model_inputs
    }
    /// Original model arguments plus declared optional top-level defaults.
    pub fn normalized_model_inputs(&self) -> &JsonObject {
        &self.data.normalized_model_inputs
    }
    /// Only hidden parameters needed by this tool, with fixed absence/value metadata.
    pub fn system_inputs(&self) -> &BTreeMap<String, BoundSystemInput> {
        &self.data.system_inputs
    }
    /// Full handler arguments; privileged access, never automatic model echo.
    pub fn execution_args(&self) -> &JsonObject {
        &self.data.execution_args
    }
    /// Digest over exact inputs, tool/compiler identity, source revisions, scope and call.
    pub fn binding_digest(&self) -> &JsonDigest {
        &self.binding_digest
    }
    /// Build the existing final-value policy input without introducing a new ownership port.
    pub fn policy_input(&self) -> ToolPolicyInput {
        ToolPolicyInput::new(
            self.data.call_id.clone(),
            self.data.tool.clone(),
            self.data.descriptor_digest.clone(),
            self.binding_digest.clone(),
            self.data.execution_args.clone(),
        )
    }
    /// Exact action checked for allow/deny/approval after all values are fixed.
    pub fn policy_request(&self) -> PolicyRequest {
        PolicyRequest {
            owner_scope: self.data.scope.clone(),
            resource_id: self.data.run_id.clone(),
            action: PolicyAction::ExecuteTool {
                input: self.policy_input(),
            },
        }
    }
    pub(crate) fn policy_request_for_run(&self, snapshot: &crate::RunSnapshot) -> PolicyRequest {
        let mut input = self.policy_input();
        if snapshot.scope == self.data.scope && snapshot.run_id == self.data.run_id {
            let receipt = snapshot.resume_receipts.iter().rev().find(|receipt| {
                matches!(&receipt.command.action,
                    crate::ResumeAction::Approve { target: crate::ApprovalTarget::Tool { call_id, binding_digest }, .. }
                    | crate::ResumeAction::Deny { target: crate::ApprovalTarget::Tool { call_id, binding_digest }, .. }
                    if call_id == &self.data.call_id && binding_digest == &self.binding_digest)
            });
            if let Some(receipt) = receipt.filter(|receipt| {
                !receipt.expired
                    && matches!(receipt.command.action, crate::ResumeAction::Approve { .. })
            }) {
                input = input.with_approval(receipt);
            }
        }
        PolicyRequest {
            owner_scope: self.data.scope.clone(),
            resource_id: self.data.run_id.clone(),
            action: PolicyAction::ExecuteTool { input },
        }
    }
    /// Restore protected inputs using the saved ledger call's exact record reference
    /// and the currently supplied compiled contract.
    pub fn restore(
        record: &ProtectedRecord,
        compiled: &CompiledTool,
        scope: &Scope,
        run_id: &Id,
        call: &ToolCall,
        run_inputs_ref: Option<&SystemInputSnapshotRef>,
    ) -> Result<Self, ContractError> {
        let bound = Self::from_value(record.value())?;
        if call.bound_input_ref.as_ref() != Some(record.reference())
            || data_digest(&bound) != record.reference().digest
        {
            return Err(error(ErrorCode::SystemInputsMismatch, "bound_input.record"));
        }
        bound.validate_identity(scope, run_id, call, run_inputs_ref)?;
        bound.validate_compiled(compiled)?;
        Ok(bound)
    }
    fn from_value(value: &Value) -> Result<Self, ContractError> {
        let record: BoundInputRecord = serde_json::from_value(value.clone())
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "bound_input"))?;
        let bound = Self {
            data: record.data,
            binding_digest: record.binding_digest,
        };
        if bound.data.schema_version != BOUND_INPUT_VERSION
            || data_digest(&bound.data) != bound.binding_digest
            || data_digest(&bound) != crate::canonical_digest(value)
        {
            return Err(error(ErrorCode::SystemInputsMismatch, "bound_input.digest"));
        }
        let mut execution = bound.data.normalized_model_inputs.clone();
        if bound
            .data
            .original_model_inputs
            .iter()
            .any(|(key, value)| execution.get(key) != Some(value))
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.model_inputs",
            ));
        }
        let mut sources: BTreeMap<&Id, &BoundSystemInput> = BTreeMap::new();
        for (parameter, input) in &bound.data.system_inputs {
            if execution.contains_key(parameter) {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.ownership",
                ));
            }
            if sources
                .insert(&input.key, input)
                .is_some_and(|previous| previous != input)
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.sources",
                ));
            }
            if let Some(resolved) = &input.resolved {
                execution.insert(parameter.clone(), resolved.value.clone());
            }
        }
        if execution != bound.data.execution_args {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.execution_args",
            ));
        }
        Ok(bound)
    }
    fn validate_identity(
        &self,
        scope: &Scope,
        run_id: &Id,
        call: &ToolCall,
        run_inputs_ref: Option<&SystemInputSnapshotRef>,
    ) -> Result<(), ContractError> {
        if self.scope() != scope
            || self.run_id() != run_id
            || self.call_id() != &call.call_id
            || Some(self.descriptor_digest()) != call.descriptor_digest.as_ref()
            || self.original_model_inputs() != &call.model_inputs
            || self.data.run_inputs_ref.as_ref() != run_inputs_ref
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.identity",
            ));
        }
        Ok(())
    }
    fn validate_compiled(&self, compiled: &CompiledTool) -> Result<(), ContractError> {
        if self.compiled_digest() != compiled.digest()
            || self.compiler_version() != compiled.compiler_version()
            || self.tool() != &compiled.descriptor().tool
            || self.descriptor_digest() != compiled.descriptor_digest()
            || self.system_inputs().len() != compiled.system_bindings().len()
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.compiled",
            ));
        }
        compiled.validate_model_inputs(self.original_model_inputs())?;
        if normalize_model_inputs(compiled, self.original_model_inputs())?
            != *self.normalized_model_inputs()
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.normalization",
            ));
        }
        for (parameter, definition) in compiled.system_bindings() {
            let input = self
                .system_inputs()
                .get(parameter)
                .ok_or_else(|| error(ErrorCode::SystemInputsMismatch, "bound_input.parameters"))?;
            if input.key != definition.key
                || input.definition_version != definition.version
                || input.source != definition.source
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.definitions",
                ));
            }
            if let Some(value) = &input.resolved {
                validate_value(definition, &value.value)?;
            }
        }
        compiled
            .validate_execution_inputs(self.execution_args())
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "tool.execution_inputs"))
    }
}

/// A saved candidate and the decision observed at binding time, not a reusable
/// dispatch permit. The executor must recheck current policy/budgets before I/O.
#[derive(Debug)]
pub struct ToolBindingResult {
    /// Owned immutable protected input.
    pub input: BoundToolInput,
    /// Record stored atomically with the call's bound_input_ref.
    pub reference: RecordRef,
    /// Allow or require_approval. Deny is returned as an error without saving a new candidate.
    pub decision: PolicyDecision,
}

/// Default normalization, registered system-value lookup and immutable candidate persistence.
/// This version starts from the original model input. Hook transformations require
/// a separate recorded path and never overwrite the original ToolCall.
pub struct InputBinder {
    registry: Arc<SystemInputRegistry>,
    resolver: Option<Arc<dyn SystemInputResolver>>,
    policy: Arc<PolicyGate>,
    ids: Arc<dyn IdSource>,
    limits: InputBindingLimits,
}

impl InputBinder {
    /// Wire trusted metadata, optional read-only resolver, policy, and internal record IDs.
    pub fn new(
        registry: Arc<SystemInputRegistry>,
        resolver: Option<Arc<dyn SystemInputResolver>>,
        policy: Arc<PolicyGate>,
        ids: Arc<dyn IdSource>,
    ) -> Self {
        Self {
            registry,
            resolver,
            policy,
            ids,
            limits: InputBindingLimits::default(),
        }
    }
    /// Set finite lookup/value/candidate bounds. Zero lookups disables resolver sources.
    pub fn with_limits(mut self, limits: InputBindingLimits) -> Result<Self, ContractError> {
        limits.validate()?;
        self.limits = limits;
        Ok(self)
    }

    /// Reuse an existing saved binding, or bind and atomically save a new candidate.
    /// Every path checks current policy; an existing call never re-queries its resolver.
    pub async fn bind(
        &self,
        compiled: &CompiledTool,
        call_id: &Id,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<ToolBindingResult, ContractError> {
        boundary(context, budget).await?;
        let saved = bounded(
            context,
            budget,
            budget.store().load(budget.scope(), budget.run_id()),
        )
        .await?;
        let call = saved
            .snapshot
            .tool_ledger
            .iter()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool_call"))?
            .call
            .clone();
        check_selection(&saved.snapshot, compiled, &call)?;
        let run_inputs = match &saved.snapshot.system_inputs {
            Some(reference) => {
                boundary(context, budget).await?;
                let record = bounded(
                    context,
                    budget,
                    budget
                        .store()
                        .read_record(budget.scope(), &reference.snapshot_ref),
                )
                .await?;
                let inputs =
                    RunSystemInputs::restore(&record, reference, budget.scope(), &self.registry)?;
                inputs.validate_resume(context.data.system_inputs.as_ref())?;
                Some(inputs)
            }
            None => {
                if context
                    .data
                    .system_inputs
                    .as_ref()
                    .is_some_and(|values| !values.values().is_empty())
                {
                    return Err(error(ErrorCode::SystemInputsMismatch, "system_inputs"));
                }
                if !compiled.system_bindings().is_empty() {
                    return Err(error(
                        ErrorCode::SystemInputMissing,
                        "system_inputs.snapshot",
                    ));
                }
                None
            }
        };
        for definition in compiled.system_bindings().values() {
            if self.registry.get(&definition.key) != Some(definition)
                || run_inputs
                    .as_ref()
                    .and_then(|inputs| inputs.definitions().get(&definition.key))
                    != Some(definition)
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "system_inputs.definitions",
                ));
            }
        }
        if let Some(reference) = &call.bound_input_ref {
            boundary(context, budget).await?;
            let record = bounded(
                context,
                budget,
                budget.store().read_record(budget.scope(), reference),
            )
            .await?;
            let input = BoundToolInput::restore(
                &record,
                compiled,
                budget.scope(),
                budget.run_id(),
                &call,
                saved.snapshot.system_inputs.as_ref(),
            )?;
            validate_bound_record(record.value(), &saved.snapshot, &call, run_inputs.as_ref())?;
            check_size(&input, self.limits.max_bound_bytes)?;
            for value in input
                .system_inputs()
                .values()
                .filter_map(|input| input.resolved.as_ref())
            {
                check_size(&value.value, self.limits.max_value_bytes)?;
            }
            let decision = self
                .authorize(
                    &input.policy_request_for_run(&saved.snapshot),
                    context,
                    budget,
                    false,
                )
                .await?;
            boundary(context, budget).await?;
            return Ok(ToolBindingResult {
                input,
                reference: reference.clone(),
                decision,
            });
        }
        if !matches!(
            saved
                .snapshot
                .tool_ledger
                .iter()
                .find(|entry| &entry.call.call_id == call_id)
                .expect("found call")
                .state,
            ToolCallState::Planned {}
        ) {
            return Err(error(ErrorCode::InvalidTransition, "tool_call.state"));
        }
        let normalized = normalize_model_inputs(compiled, &call.model_inputs)?;
        check_size(&normalized, self.limits.max_bound_bytes)?;
        let mut execution_args = normalized.clone();
        let mut system_inputs = BTreeMap::new();
        let mut values: BTreeMap<Id, Option<ResolvedSystemInput>> = BTreeMap::new();
        let mut resolver_calls = 0;
        for (parameter, definition) in compiled.system_bindings() {
            let resolved = if let Some(cached) = values.get(&definition.key) {
                cached.clone()
            } else {
                let value = match &definition.source {
                    SystemInputSource::Run {} => run_inputs
                        .as_ref()
                        .and_then(|inputs| inputs.values().get(definition.key.as_str()))
                        .cloned()
                        .map(|value| ResolvedSystemInput {
                            value,
                            revision: Id::new(
                                saved
                                    .snapshot
                                    .system_inputs
                                    .as_ref()
                                    .expect("required run snapshot")
                                    .snapshot_ref
                                    .revision
                                    .to_string(),
                            )
                            .expect("numeric revision"),
                        }),
                    SystemInputSource::Resolver { resolver_ref } => {
                        if resolver_calls >= self.limits.max_resolver_calls {
                            return Err(limit_error());
                        }
                        let request = PolicyRequest {
                            owner_scope: budget.scope().clone(),
                            resource_id: budget.run_id().clone(),
                            action: PolicyAction::ResolveSystemInput {
                                tool: compiled.descriptor().tool.clone(),
                                call_id: call_id.clone(),
                                descriptor_digest: compiled.descriptor_digest().clone(),
                                compiled_digest: compiled.digest().clone(),
                                key: definition.key.clone(),
                                definition_version: definition.version.clone(),
                                resolver_ref: resolver_ref.clone(),
                            },
                        };
                        self.authorize(&request, context, budget, true).await?;
                        let resolver = self.resolver.as_ref().ok_or_else(|| {
                            error(ErrorCode::SystemInputUnavailable, "system_input.resolver")
                        })?;
                        boundary(context, budget).await?;
                        let request = SystemInputResolveRequest {
                            key: definition.key.clone(),
                            definition_version: definition.version.clone(),
                            resolver_ref: resolver_ref.clone(),
                            model_inputs: normalized.clone(),
                        };
                        let child = budget.cancellation().child_token();
                        let lookup_context = SystemInputResolveContext {
                            scope: budget.scope().clone(),
                            principal_ref: context.data.principal_ref.clone(),
                            capability_grant_ref: context.data.capability_grant_ref.clone(),
                            run_id: budget.run_id().clone(),
                            call_id: call_id.clone(),
                            deadline: budget.call_deadline()?,
                            cancellation: child.clone(),
                        };
                        let lookup = AssertUnwindSafe(async {
                            resolver.resolve(&request, &lookup_context).await
                        })
                        .catch_unwind();
                        tokio::pin!(lookup);
                        let guard = child.drop_guard();
                        resolver_calls += 1;
                        let answer = bounded(context, budget, async {
                            lookup
                                .await
                                .map_err(|_| {
                                    error(
                                        ErrorCode::SystemInputUnavailable,
                                        "system_input.resolver",
                                    )
                                })?
                                .map_err(|_| {
                                    error(
                                        ErrorCode::SystemInputUnavailable,
                                        "system_input.resolver",
                                    )
                                })
                        })
                        .await;
                        drop(guard);
                        let answer = answer?;
                        boundary(context, budget).await?;
                        answer
                    }
                };
                if let Some(resolved) = &value {
                    check_size(&resolved.value, self.limits.max_value_bytes)?;
                    validate_value(definition, &resolved.value)?;
                }
                values.insert(definition.key.clone(), value.clone());
                value
            };
            if let Some(value) = &resolved {
                execution_args.insert(parameter.clone(), value.value.clone());
            } else if required_parameter(compiled, parameter) {
                return Err(error(
                    ErrorCode::SystemInputMissing,
                    &system_input_path(&definition.key),
                ));
            }
            system_inputs.insert(
                parameter.clone(),
                BoundSystemInput {
                    key: definition.key.clone(),
                    definition_version: definition.version.clone(),
                    source: definition.source.clone(),
                    resolved,
                },
            );
        }
        compiled
            .validate_execution_inputs(&execution_args)
            .map_err(|_| error(ErrorCode::SystemInputInvalid, "tool.execution_inputs"))?;
        let data = BoundInputData {
            schema_version: BOUND_INPUT_VERSION.into(),
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            call_id: call_id.clone(),
            tool: compiled.descriptor().tool.clone(),
            descriptor_digest: compiled.descriptor_digest().clone(),
            compiled_digest: compiled.digest().clone(),
            compiler_version: compiled.compiler_version().into(),
            original_model_inputs: call.model_inputs.clone(),
            normalized_model_inputs: normalized,
            run_inputs_ref: saved.snapshot.system_inputs.clone(),
            system_inputs,
            execution_args,
        };
        let input = BoundToolInput {
            binding_digest: data_digest(&data),
            data,
        };
        check_size(&input, self.limits.max_bound_bytes)?;
        let decision = self
            .authorize(
                &input.policy_request_for_run(&saved.snapshot),
                context,
                budget,
                false,
            )
            .await?;
        boundary(context, budget).await?;
        let record = ProtectedRecord::new(
            self.ids.next_id()?,
            1,
            serde_json::to_value(&input).expect("bound input serialization"),
        );
        let reference = record.reference().clone();
        let mut next = saved.snapshot;
        let expected_revision = next.revision;
        let (elapsed, now_ms) = budget.settlement_time(next.usage.elapsed_ms)?;
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::RevisionConflict, "revision"))?;
        next.usage.elapsed_ms = elapsed;
        next.timing.last_observed_at_ms = now_ms;
        next.tool_ledger
            .iter_mut()
            .find(|entry| &entry.call.call_id == call_id)
            .expect("found call")
            .call
            .bound_input_ref = Some(reference.clone());
        bounded(
            context,
            budget,
            budget.store().commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms,
                    snapshot: next,
                    messages: Vec::new(),
                    events: Vec::new(),
                    records: vec![record],
                },
            ),
        )
        .await?;
        boundary(context, budget).await?;
        Ok(ToolBindingResult {
            input,
            reference,
            decision,
        })
    }

    async fn authorize(
        &self,
        request: &PolicyRequest,
        context: &ExecutionContext,
        budget: &RunBudget,
        lookup: bool,
    ) -> Result<PolicyDecision, ContractError> {
        boundary(context, budget).await?;
        let deadline = budget.call_deadline()?;
        let child = budget.cancellation().child_token();
        let policy_context = ExecutionContext::new(
            ExecutionContextData {
                scope: context.data.scope.clone(),
                principal_ref: context.data.principal_ref.clone(),
                capability_grant_ref: context.data.capability_grant_ref.clone(),
                trace_context: None,
                system_inputs: None,
            },
            child.clone(),
        );
        let check = self
            .policy
            .check(request, &policy_context, Some(deadline), None);
        tokio::pin!(check);
        let guard = child.drop_guard();
        let result = bounded(context, budget, &mut check).await;
        drop(guard);
        let decision = result?;
        boundary(context, budget).await?;
        match decision {
            PolicyDecision::Deny { .. } => Err(error(ErrorCode::AccessDenied, "policy")),
            PolicyDecision::RequireApproval { .. } if lookup => Err(error(
                ErrorCode::SystemInputApprovalRequired,
                "system_input.lookup",
            )),
            decision => Ok(decision),
        }
    }
}

fn check_selection(
    snapshot: &RunSnapshot,
    compiled: &CompiledTool,
    call: &ToolCall,
) -> Result<(), ContractError> {
    if call.descriptor_digest.as_ref() != Some(compiled.descriptor_digest())
        || !snapshot
            .profile
            .profile()
            .tools
            .iter()
            .any(|selection| match selection {
                ToolBindingRef::Catalog(reference) => {
                    reference.tool_id == compiled.descriptor().tool.id
                        && reference.version == compiled.descriptor().tool.version
                        && call.tool_name == compiled.descriptor().name
                }
                ToolBindingRef::Export(export) => {
                    export.alias.as_ref().unwrap_or(&compiled.descriptor().name) == &call.tool_name
                        && snapshot
                            .profile
                            .profile()
                            .adapters
                            .as_ref()
                            .is_some_and(|adapters| {
                                adapters
                                    .iter()
                                    .any(|adapter| adapter.binding_id == export.adapter_binding)
                            })
                }
            })
    {
        return Err(error(
            ErrorCode::InvalidToolInputContract,
            "tool_call.descriptor",
        ));
    }
    Ok(())
}

fn normalize_model_inputs(
    compiled: &CompiledTool,
    original: &JsonObject,
) -> Result<JsonObject, ContractError> {
    compiled.validate_model_inputs(original)?;
    let mut normalized = original.clone();
    let properties = compiled
        .model_input_schema()
        .get("properties")
        .and_then(Value::as_object)
        .expect("compiled properties");
    for parameter in &compiled.descriptor().agent_parameters {
        if normalized.contains_key(parameter) || required_parameter(compiled, parameter) {
            continue;
        }
        let mut schema = &properties[parameter];
        let mut seen = BTreeSet::new();
        loop {
            if let Some(default) = schema.get("default") {
                normalized.insert(parameter.clone(), default.clone());
                break;
            }
            let Some(reference) = schema.get("$ref").and_then(Value::as_str) else {
                break;
            };
            if !seen.insert(reference) {
                break;
            }
            let pointer = reference
                .strip_prefix('#')
                .ok_or_else(|| error(ErrorCode::InvalidToolInputContract, "model_defaults"))?;
            schema = compiled
                .model_input_schema()
                .pointer(pointer)
                .ok_or_else(|| error(ErrorCode::InvalidToolInputContract, "model_defaults"))?;
        }
    }
    compiled.validate_model_inputs(&normalized)?;
    Ok(normalized)
}
fn required_parameter(compiled: &CompiledTool, parameter: &str) -> bool {
    compiled
        .input_schema()
        .get("required")
        .and_then(Value::as_array)
        .is_some_and(|required| required.iter().any(|name| name.as_str() == Some(parameter)))
}
fn validate_value(definition: &SystemInputDefinition, value: &Value) -> Result<(), ContractError> {
    let validator = compile_validator(&definition.value_schema).map_err(|_| {
        error(
            ErrorCode::SystemInputInvalid,
            &system_input_path(&definition.key),
        )
    })?;
    if !validator.is_valid(value) {
        return Err(error(
            ErrorCode::SystemInputInvalid,
            &system_input_path(&definition.key),
        ));
    }
    Ok(())
}

fn system_input_path(key: &Id) -> String {
    // Only registered metadata is named; JSON escaping prevents control characters
    // or punctuation from being interpreted as a path or leaking a supplied value.
    format!(
        "system_inputs[{}]",
        serde_json::to_string(key.as_str()).expect("serializable key")
    )
}

async fn boundary(context: &ExecutionContext, budget: &RunBudget) -> Result<(), ContractError> {
    if &context.data.scope != budget.scope() {
        return Err(error(ErrorCode::AccessDenied, "scope"));
    }
    if context.cancellation.is_cancelled() {
        return Err(error(ErrorCode::Cancelled, "input_binding"));
    }
    bounded(context, budget, budget.check_boundary()).await
}
async fn bounded<T>(
    context: &ExecutionContext,
    budget: &RunBudget,
    future: impl Future<Output = Result<T, ContractError>>,
) -> Result<T, ContractError> {
    if context.cancellation.is_cancelled() {
        return Err(error(ErrorCode::Cancelled, "input_binding"));
    }
    tokio::select! {
        biased;
        _ = context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "input_binding")),
        stopped = budget.wait_for_cancellation_or_deadline() => { stopped?; Err(error(ErrorCode::DeadlineExceeded, "input_binding")) },
        result = future => {
            if context.cancellation.is_cancelled() || budget.cancellation().is_cancelled() { return Err(error(ErrorCode::Cancelled, "input_binding")); }
            budget.call_deadline()?;
            result
        }
    }
}

pub(crate) fn validate_bound_record(
    value: &Value,
    snapshot: &RunSnapshot,
    call: &ToolCall,
    run_inputs: Option<&RunSystemInputs>,
) -> Result<(), ContractError> {
    let input = BoundToolInput::from_value(value)?;
    input.validate_identity(
        &snapshot.scope,
        &snapshot.run_id,
        call,
        snapshot.system_inputs.as_ref(),
    )?;
    for bound in input.system_inputs().values() {
        let data = run_inputs
            .ok_or_else(|| error(ErrorCode::SystemInputsMismatch, "bound_input.run_snapshot"))?;
        let definition = data
            .definitions()
            .get(&bound.key)
            .ok_or_else(|| error(ErrorCode::SystemInputsMismatch, "bound_input.definition"))?;
        if definition.version != bound.definition_version || definition.source != bound.source {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.definition",
            ));
        }
        if let Some(value) = &bound.resolved {
            validate_value(definition, &value.value)?;
        }
        if matches!(bound.source, SystemInputSource::Run {}) {
            let expected = data.values().get(bound.key.as_str());
            if bound.resolved.as_ref().map(|resolved| &resolved.value) != expected
                || bound.resolved.as_ref().is_some_and(|resolved| {
                    resolved.revision.as_str()
                        != snapshot
                            .system_inputs
                            .as_ref()
                            .expect("snapshot supplied")
                            .snapshot_ref
                            .revision
                            .to_string()
                })
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.run_value",
                ));
            }
        }
    }
    Ok(())
}

struct ByteCounter {
    total: usize,
    limit: usize,
}
impl io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.total = self
            .total
            .checked_add(bytes.len())
            .filter(|size| *size <= self.limit)
            .ok_or_else(|| io::Error::other("input size limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn check_size(value: &impl Serialize, limit: usize) -> Result<(), ContractError> {
    serde_json::to_writer(&mut ByteCounter { total: 0, limit }, value).map_err(|_| limit_error())
}
fn limit_error() -> ContractError {
    error(ErrorCode::InputBindingLimitExceeded, "input_binding.limits")
}
fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
```

## `crates/wickle/src/lib.rs`

```rust
//! Wickle, an agent engine for Rust applications.
//!
//! Profiles, scoped metadata resolution, and versioned execution data contracts.
//! The agent driver runs model/tool loops with scoped ports, separate system
//! inputs, persisted attempt accounting, and explicit effect outcomes.
//!
//! Runtime objects stay in Host code. Only documented data contracts are
//! serialized; successful decoding does not authenticate a caller.
//!
//! Internal modules are not extension points; use the root exports.
//! ```compile_fail
//! use wickle::serialization::canonical_digest;
//! ```

mod agent;
mod budget;
mod clock;
mod context;
mod context_projection;
mod error;
mod input_binding;
mod message;
mod model;
mod model_catalog;
mod model_dispatch;
mod model_execution;
mod model_protocol;
mod model_routing;
mod policy;
mod profile;
mod resolution;
mod run;
mod serialization;
mod state;
mod tool_execution;
mod tool_schema;
mod views;

pub use agent::{
    Agent, AgentBindings, AgentSettings, CancelReceipt, ModelTokenEstimator, RunHandle,
    create_agent,
};
pub use budget::{AttemptReservation, ReservationKind, RunBudget, RunTiming};
pub use clock::{Clock, ClockReading, IdSource, RandomIdSource, SystemClock};
pub use context_projection::{
    CONTEXT_ASSEMBLER_VERSION, ContextAssembler, ContextItem, ContextLifetime, ContextOrigin,
    ContextPriority, ContextProjection, InstructionAssetContent, PinnedPromptTool, ProjectionInput,
    ProjectionLimits, PromptSnapshot, PromptToolBinding, ScopedOpaque, SkillManifest,
};
pub use input_binding::{
    BoundSystemInput, BoundToolInput, InputBinder, InputBindingLimits, ResolvedSystemInput,
    RunSystemInputs, SystemInputResolveContext, SystemInputResolveRequest, SystemInputResolver,
    ToolBindingResult,
};
pub use model_catalog::{
    CatalogRequirements, ModelAlias, ModelBinding, ModelCapabilities, ModelCatalog,
    ModelCatalogSnapshot, ModelDefinition, ModelDefinitionRef, ModelEvidence, ModelLifecycle,
    ModelSupportStatus, ModelValidationEvidence, ModelValidationKind, ResolvedCatalogBinding,
};
pub use model_protocol::{
    ModelCallContext, ModelContent, ModelEvent, ModelFinish, ModelMessage, ModelOutput, ModelPort,
    ModelPortBinding, ModelProtocolError, ModelProtocolErrorCode, ModelRequest, ModelResponse,
    ModelResponseLimits, ModelResponseMetadata, ModelRole, ModelTool, OpaqueContinuation,
    ProposedToolCall, ToolCallValidation, collect_model_response,
};
pub use model_routing::{
    MAX_ROUTE_FALLBACKS, MAX_ROUTING_RULES, ModelRouter, ROUTING_SNAPSHOT_VERSION, RouteSelection,
    RouteSelectionReason, RoutingPolicy, RoutingRule, RoutingSnapshot,
};
pub use policy::{
    ApprovalChallenge, Guarded, PolicyAction, PolicyContext, PolicyDecision, PolicyGate,
    PolicyPort, PolicyRequest, ToolApproval, ToolPolicyInput,
};
pub use state::{
    AdmissionInput, AdmissionResult, CommitInput, EventPage, MAX_EVENT_PAGE_SIZE, MemoryStateStore,
    ProtectedRecord, RunLease, STATE_STORE_CHECKPOINT_VERSION, StateStore, StateStoreCapabilities,
    StateStoreCheckpoint, StoredRun,
};
pub use tool_execution::{
    ExternalReceiptContext, ExternalReceiptRequest, ExternalReceiptVerifier,
    PreparedToolResolution, SerialToolRound, ToolEffect, ToolExecutionContext, ToolExecutionLimits,
    ToolExecutionOutcome, ToolExecutionResult, ToolExecutor, ToolRegistration, ToolRegistry,
    ToolRoundOutcome,
};
pub use tool_schema::{
    CompiledTool, SchemaCompiler, SystemInputDefinition, SystemInputRegistry, SystemInputSource,
    TOOL_SCHEMA_COMPILER_VERSION, ToolConcurrency, ToolDescriptor, ToolRetryPolicy, ToolSideEffect,
};
pub use views::{ArtifactView, EventView, RunView};

pub use context::{
    ExecutionContext, ExecutionContextData, PortFuture, PortStream, Scope, SystemInputs,
};
pub use error::{ContractError, ErrorCode};
pub use message::{
    ArtifactRef, ContentBlock, EvidenceRef, Failure, InputContent, Message, MessageOrigin,
    MessageRole, RecordRef, ToolCall, ToolResult, ToolResultStatus, Visibility,
};
pub use model::{
    ApiContract, ModelAttemptState, ModelFailureKind, ModelInvocationRecord, ModelPurpose,
    ModelUsage, ResolvedModelRoute, RouteRequest, UsageMeasurement, VersionPolicy,
    VersionSemantics,
};
pub use model_dispatch::{
    ModelDispatcher, ModelInspectionContext, ModelRouteAvailability, ModelRouteInspector,
    ModelRouteObservation,
};
pub use model_execution::{
    ModelExchange, ModelExchangeOutcome, ModelProjectionContext, ModelRequestProjector,
    ModelRetryPolicy, ProjectedModelRequest, RoutedModelInput, StoredModelResponse,
};
pub use profile::{
    AdapterBindingRef, AgentProfile, CatalogHookRef, CatalogSourceRef, CatalogToolRef,
    CompletionPolicy, ConnectorBindingRef, ContextPolicy, ContextSourceBinding, ContextSourceRef,
    ContextTrigger, ExportRef, HookPosition, HookRef, InstructionAsset, InstructionText,
    Instructions, OutputContract, PROFILE_SCHEMA_VERSION, ProfileSchemaVersion, RunLimits,
    SkillRef, ToolBindingRef, VersionedRef,
};
pub use resolution::{
    ComponentKind, ComponentMetadata, ComponentRef, ExportKind, ExportMetadata, ProfileResolver,
    ProfileValidator, ResolvedComponent, ResolvedProfile,
};
pub use run::{
    ApprovalTarget, BudgetKind, BudgetUsage, CompletionBasis, EphemeralEvent, InputRequest,
    OutcomeResult, RUN_EVENT_SCHEMA_VERSION, RUN_SNAPSHOT_SCHEMA_VERSION, ResumeAction,
    ResumeCommand, ResumeReceipt, RunEvent, RunEventPayload, RunEventSchemaVersion, RunOutcome,
    RunPhase, RunRequest, RunSnapshot, RunSnapshotSchemaVersion, RunStatus, RunTrigger,
    SessionSchemaVersion, SessionSnapshot, SourceExecutionState, SystemInputSnapshotRef,
    ToolCallState, ToolLedgerEntry, VerificationSummary, VerificationVerdict, WaitState,
    WaitTarget, admission_digest,
};
pub use serialization::{
    Id, JsonDigest, JsonObject, canonical_digest, canonical_digest_json, parse_json,
};
```

## `crates/wickle/src/message.rs`

```rust
use crate::{Id, JsonDigest, JsonObject, Scope, serialization::optional};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

/// An authorized reference to immutable stored data, not the referenced payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordRef {
    /// Store-specific record identifier.
    pub record_id: Id,
    /// Exact stored revision.
    pub revision: u64,
    /// Digest of the referenced contract data.
    pub digest: JsonDigest,
}

/// Artifact metadata. Reading bytes still requires current scope authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    /// Artifact identifier.
    pub artifact_id: Id,
    /// Scope that owns the artifact.
    pub scope: Scope,
    /// Media type of the stored bytes.
    pub media_type: Id,
    /// Original byte length.
    pub size_bytes: u64,
    /// Store-defined content hash, separate from JSON contract digests.
    pub content_hash: Id,
}

/// Provenance for a source passage or fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceRef {
    /// Source identifier.
    pub source_id: Id,
    /// Exact source version/revision.
    pub version: Id,
    /// Source-specific passage location.
    pub location: Id,
    /// Original source content hash.
    pub content_hash: Id,
    /// Optional quoted passage.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub quote: Option<String>,
}

/// User-supplied or final-output content; cannot inject tool calls or provider state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum InputContent {
    /// Text content.
    Text {
        /// Text body.
        text: String,
    },
    /// JSON data, not executable objects.
    Json {
        /// JSON value; explicit JSON null is valid content.
        value: serde_json::Value,
    },
    /// Artifact metadata.
    Artifact {
        /// Artifact reference.
        reference: ArtifactRef,
    },
    /// Evidence metadata.
    Evidence {
        /// Source reference.
        reference: EvidenceRef,
    },
}

/// Model-owned tool arguments and provenance, separate from system execution inputs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolCall {
    /// Core call identifier.
    pub call_id: Id,
    /// Original model request identifier.
    pub model_request_id: Id,
    /// Provider-local call identifier, scoped by model_request_id.
    pub provider_call_id: Id,
    /// Model-facing tool name.
    pub tool_name: Id,
    /// Original model-supplied inputs, never replaced with execution_args.
    pub model_inputs: JsonObject,
    /// Pinned descriptor identity. None means the name was unregistered when planned.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub descriptor_digest: Option<JsonDigest>,
    /// Protected bound-input record, once binding succeeds.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub bound_input_ref: Option<RecordRef>,
}

/// Outcome of one tool dispatch or a pre-dispatch denial.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolResultStatus {
    /// Tool completed successfully.
    Succeeded,
    /// Tool failed with a classified error.
    Failed,
    /// Policy or validation denied execution.
    Denied,
    /// Execution was cancelled.
    Cancelled,
    /// External effect status is not known.
    Unknown,
}

/// A safe structured failure; raw SDK errors belong in protected Host diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Failure {
    /// Registered error/reason code.
    pub code: Id,
    /// Optional protected diagnostic reference.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub diagnostic_ref: Option<RecordRef>,
}

/// Tool observation explicitly paired with its call message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolResult {
    /// Core call identifier.
    pub call_id: Id,
    /// Message containing the matching call.
    pub call_message_id: Id,
    /// Explicit execution status.
    pub status: ToolResultStatus,
    /// Confirmed external effect, separate from validation of the returned value.
    /// Missing legacy metadata is unknown, never evidence that a write did not occur.
    #[serde(default)]
    pub effect: crate::ToolEffect,
    /// Bounded model-visible observations or references.
    pub content: Vec<InputContent>,
    /// Protected receipt, retained even if output processing fails.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub effect_receipt_ref: Option<RecordRef>,
    /// Classified failure, when applicable.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub error: Option<Failure>,
}

/// Transcript content. Provider replay data is a scoped, protected reference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContentBlock {
    /// Ordinary displayable content.
    Content {
        /// Text, JSON, artifact, or evidence.
        content: InputContent,
    },
    /// A complete model tool call.
    ToolCall {
        /// Call and model-only arguments.
        call: ToolCall,
    },
    /// Paired tool observation.
    ToolResult {
        /// Observation and receipt reference.
        result: ToolResult,
    },
    /// Explicit resolution of an earlier Unknown observation. The original
    /// message stays immutable; only its model projection is superseded.
    ToolResultCorrection {
        /// Earlier Tool message containing the unresolved observation.
        previous_message_id: Id,
        /// Canonical digest of that exact earlier ToolResult.
        previous_result_digest: JsonDigest,
        /// Confirmed result for the same call and original call message.
        result: ToolResult,
    },
    /// Opaque continuation data bound to one provider/route.
    ProviderOpaque {
        /// Registered provider key.
        provider: Id,
        /// Route that can interpret the protected block.
        route_digest: JsonDigest,
        /// Protected replay-data record.
        data_ref: RecordRef,
    },
}

/// Validate explicit corrections while retaining the original append-only history.
pub(crate) fn tool_corrections(
    messages: &[Message],
) -> Result<std::collections::BTreeMap<Id, (Id, ToolResult)>, crate::ContractError> {
    let invalid = || {
        crate::ContractError::new(
            crate::ErrorCode::InvalidContext,
            "transcript.tool_correction",
        )
    };
    let mut corrections = std::collections::BTreeMap::new();
    for (index, message) in messages.iter().enumerate() {
        for content in &message.content {
            let ContentBlock::ToolResultCorrection {
                previous_message_id,
                previous_result_digest,
                result,
            } = content
            else {
                continue;
            };
            if message.role != MessageRole::Tool
                || message.origin != MessageOrigin::Tool
                || message.content.len() != 1
            {
                return Err(invalid());
            }
            let prior_index = messages[..index]
                .iter()
                .position(|prior| &prior.message_id == previous_message_id)
                .ok_or_else(invalid)?;
            let prior = &messages[prior_index];
            let [ContentBlock::ToolResult { result: previous }] = prior.content.as_slice() else {
                return Err(invalid());
            };
            if prior.run_id != message.run_id
                || prior.sequence >= message.sequence
                || prior.role != MessageRole::Tool
                || prior.origin != MessageOrigin::Tool
                || prior.visibility != message.visibility
                || previous.call_id != result.call_id
                || previous.call_message_id != result.call_message_id
                || previous.effect != crate::ToolEffect::Unknown
                || previous.status != ToolResultStatus::Unknown
                || result.effect == crate::ToolEffect::Unknown
                || result.status == ToolResultStatus::Unknown
                || &crate::canonical_digest(&serde_json::to_value(previous).map_err(|_| invalid())?)
                    != previous_result_digest
                || messages[prior_index + 1..index].iter().any(|intervening| {
                    intervening.run_id != message.run_id || intervening.role != MessageRole::Tool
                })
                || corrections
                    .insert(
                        previous_message_id.clone(),
                        (message.message_id.clone(), result.clone()),
                    )
                    .is_some()
            {
                return Err(invalid());
            }
        }
    }
    Ok(corrections)
}

/// Logical message role before provider-specific projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    /// Host/profile instruction role.
    System,
    /// User input.
    User,
    /// Model output.
    Assistant,
    /// Tool observation.
    Tool,
}

/// Provenance of content; a wire role does not grant authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageOrigin {
    /// Trusted Host instructions.
    Host,
    /// Pinned profile instructions.
    Profile,
    /// User input.
    User,
    /// Model response.
    Model,
    /// Loaded skill data.
    Skill,
    /// Tool observation.
    Tool,
    /// Retrieved external data.
    Retrieval,
    /// Retrieved memory.
    Memory,
    /// Verifier feedback.
    Verification,
    /// Synthetic recovery bookkeeping.
    Recovery,
}

/// Intended projection surfaces; authorization is still enforced at use time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    /// Protected execution data only.
    Internal,
    /// Model projection only.
    Model,
    /// User presentation only.
    User,
    /// Both model projection and user presentation.
    UserAndModel,
}

/// An original transcript record, not a provider request or UI event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    /// Unique message identity.
    pub message_id: Id,
    /// Owning run.
    pub run_id: Id,
    /// Monotonic stored message sequence.
    pub sequence: NonZeroU64,
    /// Logical role.
    pub role: MessageRole,
    /// Original content blocks.
    pub content: Vec<ContentBlock>,
    /// Source provenance.
    pub origin: MessageOrigin,
    /// Intended projections.
    pub visibility: Visibility,
}
```

## `crates/wickle/src/policy.rs`

```rust
use std::{fmt, future::Future, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{
    ContractError, ErrorCode, ExecutionContext, Id, JsonDigest, JsonObject, ModelPurpose,
    PortFuture, Scope, VersionedRef, serialization::data_digest,
};

/// Final, bound tool inputs visible to the trusted policy implementation.
/// Serialized values require protected storage and must not enter model/UI logs.
#[derive(Clone, PartialEq, Serialize)]
pub struct ToolPolicyInput {
    /// Core call identity.
    pub call_id: Id,
    /// Exact tool identity and version.
    pub tool: VersionedRef,
    /// Pinned descriptor identity.
    pub descriptor_digest: JsonDigest,
    /// Binding identity computed by the trusted input binder.
    pub binding_digest: JsonDigest,
    execution_args: JsonObject,
    #[serde(skip_serializing_if = "Option::is_none")]
    approval: Option<ToolApproval>,
}

impl ToolPolicyInput {
    /// Own the binder's final arguments. The gate never invents missing IDs.
    pub fn new(
        call_id: Id,
        tool: VersionedRef,
        descriptor_digest: JsonDigest,
        binding_digest: JsonDigest,
        execution_args: JsonObject,
    ) -> Self {
        Self {
            call_id,
            tool,
            descriptor_digest,
            binding_digest,
            execution_args,
            approval: None,
        }
    }
    /// Inspect the full arguments to check actual target existence and ownership.
    pub fn execution_args(&self) -> &JsonObject {
        &self.execution_args
    }
    /// A recorded approval of this exact binding. The current policy still decides
    /// whether the actor may execute; this evidence never overrides a Deny.
    pub fn approval(&self) -> Option<&ToolApproval> {
        self.approval.as_ref()
    }
    pub(crate) fn with_approval(mut self, receipt: &crate::ResumeReceipt) -> Self {
        self.approval = Some(ToolApproval {
            command_id: receipt.command.command_id.clone(),
            command_ref: receipt.command_ref.clone(),
            accepted_revision: receipt.accepted_revision,
            actor_ref: receipt.actor_ref.clone(),
            capability_grant_ref: receipt.capability_grant_ref.clone(),
        });
        self
    }
}

/// Core-validated evidence that an authenticated actor approved a fixed tool binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ToolApproval {
    command_id: Id,
    command_ref: crate::RecordRef,
    accepted_revision: u64,
    actor_ref: Id,
    capability_grant_ref: Id,
}
impl ToolApproval {
    /// Accepted command identity.
    pub fn command_id(&self) -> &Id {
        &self.command_id
    }
    /// Protected command record, for authorized auditing.
    pub fn command_ref(&self) -> &crate::RecordRef {
        &self.command_ref
    }
    /// Revision at which approval was committed.
    pub fn accepted_revision(&self) -> u64 {
        self.accepted_revision
    }
    /// Authenticated approver.
    pub fn actor_ref(&self) -> &Id {
        &self.actor_ref
    }
    /// Host grant checked when approval was accepted.
    pub fn capability_grant_ref(&self) -> &Id {
        &self.capability_grant_ref
    }
}

impl fmt::Debug for ToolPolicyInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolPolicyInput")
            .field("call_id", &self.call_id)
            .field("tool", &self.tool)
            .field("descriptor_digest", &self.descriptor_digest)
            .field("binding_digest", &self.binding_digest)
            .field("execution_args", &"<redacted>")
            .finish()
    }
}

/// Operation being authorized; data access and protected-detail access differ.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum PolicyAction {
    /// Admit a new run.
    StartRun {},
    /// Read minimal run metadata.
    ReadRun {},
    /// Read the protected checkpoint, separately from the public view.
    ReadRunDetails {},
    /// Resume a recorded wait or interruption.
    ResumeRun {
        /// Exact command, so authorization distinguishes approving, denying,
        /// answering and supplying an external receipt.
        command: Box<crate::ResumeCommand>,
        /// Fixed tool binding when the saved wait belongs to a tool.
        binding_digest: Option<JsonDigest>,
    },
    /// Request cancellation.
    CancelRun {},
    /// Read artifact data/metadata.
    ReadArtifact {},
    /// Write an artifact in the owning scope.
    WriteArtifact {},
    /// Read minimal event metadata.
    ReadEvents {},
    /// Read a protected record referenced by an event or checkpoint.
    ReadRecord {},
    /// Use scoped data in model context.
    UseContext {},
    /// Read one registered system key before the final target value is known.
    ResolveSystemInput {
        /// Exact tool requesting the lookup.
        tool: VersionedRef,
        /// Logical call whose binding is being prepared.
        call_id: Id,
        /// Pinned tool descriptor identity.
        descriptor_digest: JsonDigest,
        /// Compiled input contract identity.
        compiled_digest: JsonDigest,
        /// Exact registry key, never a path expression.
        key: Id,
        /// Pinned system-input definition revision.
        definition_version: Id,
        /// Exact read-only resolver implementation.
        resolver_ref: VersionedRef,
    },
    /// Send input to a selected model route.
    InvokeModel {
        /// Exact provider, target, model and connection metadata for current authorization.
        route: Box<crate::ResolvedModelRoute>,
        /// Purpose being authorized.
        purpose: ModelPurpose,
    },
    /// Dispatch one tool using final validated inputs.
    ExecuteTool {
        /// Final inputs, including system-owned parameters.
        input: ToolPolicyInput,
    },
}

/// An operation on an authoritative resource identity.
/// Obtain owner_scope from trusted stored metadata, not a caller's claimed scope.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PolicyRequest {
    /// Stored owner scope; user_id=None is not a wildcard.
    pub owner_scope: Scope,
    /// Run, artifact, event stream, record, context, or tool resource identity.
    pub resource_id: Id,
    /// Exact proposed action.
    pub action: PolicyAction,
}

impl PolicyRequest {
    /// Identity of the full proposed action and owning scope, including tool inputs.
    /// It excludes the approving principal so a new authorized reviewer can act.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }
}

/// Current authenticated policy context, without the run's whole system-input map.
pub struct PolicyContext<'a> {
    /// Current authenticated resource scope.
    pub scope: &'a Scope,
    /// Current principal, distinct from resource scope and original tool inputs.
    pub principal_ref: &'a Id,
    /// Current grant reference; the Host checks membership and revocation.
    pub capability_grant_ref: &'a Id,
    /// Cooperative cancellation signal.
    pub cancellation: &'a CancellationToken,
    /// Effective policy deadline on the monotonic clock.
    pub deadline: Instant,
}

/// Host authorization decision. A reason is an informational code, not a grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
pub enum PolicyDecision {
    /// This exact action is currently allowed.
    Allow {},
    /// The action is denied.
    Deny {
        /// Safe reason code, without bound values or SDK error text.
        reason: Id,
    },
    /// The action requires an approval flow before it can be performed.
    RequireApproval {
        /// Safe reason code.
        reason: Id,
    },
}

impl PolicyDecision {
    /// Intersect a Host decision with a restriction; an allow never removes a denial
    /// or an approval requirement. Existing Host reasons take precedence.
    pub fn restrict(self, restriction: Self) -> Self {
        match (self, restriction) {
            (denied @ Self::Deny { .. }, _) | (_, denied @ Self::Deny { .. }) => denied,
            (approval @ Self::RequireApproval { .. }, _)
            | (_, approval @ Self::RequireApproval { .. }) => approval,
            _ => Self::Allow {},
        }
    }
}

/// Trusted Host policy. Implement actual resource/membership/FK checks here.
pub trait PolicyPort: Send + Sync {
    /// Check the current grant against the exact bound action without performing it.
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision>;
}

/// An approval request bound to an exact action, not a reusable permission token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApprovalChallenge {
    /// Scope whose resource will be affected.
    pub scope: Scope,
    /// Resource identity.
    pub resource_id: Id,
    /// Digest includes final tool input, descriptor/version, and scope.
    pub request_digest: JsonDigest,
    /// Safe reason code for the Host's approval UI.
    pub reason: Id,
}

/// Result of a guarded operation. Approval-required never invokes the operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Guarded<T> {
    /// Operation completed after a current authorization check.
    Completed(T),
    /// No operation was invoked; the Host/runtime must handle this approval request.
    ApprovalRequired(ApprovalChallenge),
}

/// Current authorization with exact scope matching, timeout, and cancellation.
/// This does not authenticate caller-supplied JSON or provide a sandbox for Host code.
pub struct PolicyGate {
    policy: Arc<dyn PolicyPort>,
    timeout: Duration,
}

impl PolicyGate {
    /// Configure a finite, positive policy timeout without creating a runtime.
    pub fn new(policy: Arc<dyn PolicyPort>, timeout: Duration) -> Result<Self, ContractError> {
        if timeout.is_zero() || Instant::now().checked_add(timeout).is_none() {
            return Err(ContractError::new(
                ErrorCode::InvalidContract,
                "policy.timeout",
            ));
        }
        Ok(Self { policy, timeout })
    }

    /// Check the current Host decision. Every call rechecks policy; permits are not cached.
    pub async fn check(
        &self,
        request: &PolicyRequest,
        context: &ExecutionContext,
        deadline: Option<Instant>,
        restriction: Option<PolicyDecision>,
    ) -> Result<PolicyDecision, ContractError> {
        if request.owner_scope != context.data.scope {
            return Err(ContractError::new(ErrorCode::AccessDenied, "scope"));
        }
        if context.cancellation.is_cancelled() {
            return Err(ContractError::new(ErrorCode::Cancelled, "policy"));
        }
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(ContractError::new(ErrorCode::RuntimeUnavailable, "policy"));
        }
        let now = Instant::now();
        let policy_deadline = now
            .checked_add(self.timeout)
            .ok_or_else(|| ContractError::new(ErrorCode::InvalidContract, "policy.timeout"))?;
        let effective = deadline.map_or(policy_deadline, |d| d.min(policy_deadline));
        if effective <= now {
            return Err(ContractError::new(ErrorCode::DeadlineExceeded, "policy"));
        }
        let decision = AssertUnwindSafe(async {
            tokio::select! {
                biased;
                _ = context.cancellation.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "policy")),
                _ = tokio::time::sleep_until(effective) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "policy")),
                result = self.policy.authorize(request, PolicyContext {
                    scope: &context.data.scope, principal_ref: &context.data.principal_ref,
                    capability_grant_ref: &context.data.capability_grant_ref,
                    cancellation: &context.cancellation, deadline: effective,
                }) => result.map_err(|_| ContractError::new(ErrorCode::PolicyUnavailable, "policy")),
            }
        }).catch_unwind().await.map_err(|_| ContractError::new(ErrorCode::PolicyUnavailable, "policy"))??;
        if context.cancellation.is_cancelled() {
            return Err(ContractError::new(ErrorCode::Cancelled, "policy"));
        }
        if Instant::now() >= effective {
            return Err(ContractError::new(ErrorCode::DeadlineExceeded, "policy"));
        }
        Ok(match restriction {
            Some(other) => decision.restrict(other),
            None => decision,
        })
    }

    /// Invoke a closure only after current policy allows it. Future construction is
    /// also delayed until authorization. The operation owns its I/O cancellation and
    /// effect reconciliation; dropping a future is not treated as external rollback.
    pub async fn guard<T, F, Fut>(
        &self,
        request: &PolicyRequest,
        context: &ExecutionContext,
        deadline: Option<Instant>,
        restriction: Option<PolicyDecision>,
        operation: F,
    ) -> Result<Guarded<T>, ContractError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, ContractError>>,
    {
        match self.check(request, context, deadline, restriction).await? {
            PolicyDecision::Allow {} => operation().await.map(Guarded::Completed),
            PolicyDecision::Deny { .. } => {
                Err(ContractError::new(ErrorCode::AccessDenied, "policy"))
            }
            PolicyDecision::RequireApproval { reason } => {
                Ok(Guarded::ApprovalRequired(ApprovalChallenge {
                    scope: request.owner_scope.clone(),
                    resource_id: request.resource_id.clone(),
                    request_digest: request.digest(),
                    reason,
                }))
            }
        }
    }
}
```

## `crates/wickle/src/run.rs`

```rust
use crate::{
    ArtifactRef, AttemptReservation, CompletionPolicy, ContractError, ErrorCode, Failure, Id,
    InputContent, JsonDigest, JsonObject, ModelAttemptState, ModelInvocationRecord, RecordRef,
    ReservationKind, ResolvedProfile, RunLimits, RunTiming, Scope, ToolCall, ToolResult,
    VersionedRef,
    serialization::{data_digest, decode, optional},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
};

/// Current run checkpoint format, independent of profile and event formats.
pub const RUN_SNAPSHOT_SCHEMA_VERSION: &str = "wickle.run-snapshot.v1";
/// Current durable event format.
pub const RUN_EVENT_SCHEMA_VERSION: &str = "wickle.run-event.v1";

/// Supported run checkpoint versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunSnapshotSchemaVersion {
    /// First checkpoint format.
    #[serde(rename = "wickle.run-snapshot.v1")]
    V1,
}

/// Supported session checkpoint versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionSchemaVersion {
    /// First session format.
    #[serde(rename = "wickle.session-snapshot.v1")]
    V1,
}

/// Supported durable event versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunEventSchemaVersion {
    /// First durable event format.
    #[serde(rename = "wickle.run-event.v1")]
    V1,
}

/// Why a Host submitted a run. Trigger data does not authenticate its sender.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunTrigger {
    /// Direct user request.
    User {},
    /// An event verified by the Host.
    Event {
        /// Host event identity.
        source_id: Id,
    },
    /// A schedule occurrence computed by the Host.
    Schedule {
        /// Occurrence identity, not a cron expression for the core to run.
        source_id: Id,
    },
    /// Child execution requested by a Host orchestration layer.
    Child {
        /// Parent run identity. Execution capability is checked separately.
        parent_run_id: Id,
    },
}

/// Caller request data; trusted execution context is supplied separately.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunRequest {
    /// Host-generated idempotency identity within scope and session.
    pub request_id: Id,
    /// Session whose pinned profile will be used.
    pub session_id: Id,
    /// User data, without injected tool calls or provider continuation state.
    pub input: Vec<InputContent>,
    /// Verified trigger provenance.
    pub trigger: RunTrigger,
    /// Logical model options authorized by the Host and pinned with the admitted request.
    /// Catalog schemas define supported keys; credentials and raw provider bodies do not belong here.
    #[serde(default, skip_serializing_if = "JsonObject::is_empty")]
    pub model_options: JsonObject,
    /// Optional output override; Host policy must authorize its use.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub output_contract: Option<crate::OutputContract>,
}

impl RunRequest {
    /// Decode caller data without granting authority or creating a run.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        decode(input, None)
    }
}

/// Exact request for human input, bound to the originating call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputRequest {
    /// Stable input request identity.
    pub input_request_id: Id,
    /// Call that must receive the answer.
    pub call_id: Id,
    /// Question shown by the Host.
    pub question: String,
    /// Optional exact schema for the answer.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub schema_ref: Option<VersionedRef>,
}

/// Exact operation or candidate to which approval applies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApprovalTarget {
    /// Tool approval binds the final execution input digest.
    Tool {
        /// Core call identity.
        call_id: Id,
        /// Digest that includes system-owned inputs.
        binding_digest: JsonDigest,
    },
    /// Review of a fixed candidate.
    Candidate {
        /// Stored candidate identity.
        candidate_ref: RecordRef,
        /// Exact verifier definition.
        verifier_ref: VersionedRef,
    },
}

/// Typed reason a run waits without making additional model calls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitTarget {
    /// Explicit approval of fixed data.
    Approval {
        /// Approval target.
        target: ApprovalTarget,
    },
    /// Answer to a recorded input request.
    Input {
        /// Input request.
        request: InputRequest,
    },
    /// Confirmation of an uncertain external effect.
    External {
        /// Call with uncertain effect.
        call_id: Id,
        /// Stable external idempotency/reconciliation key.
        effect_key: Id,
    },
}

/// Saved wait identity, target, and optional expiry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitState {
    /// Unique wait identity used to reject stale answers.
    pub wait_id: Id,
    /// Data or effect being awaited.
    pub target: WaitTarget,
    /// UTC milliseconds since Unix epoch; the run deadline still applies.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub expires_at_ms: Option<i64>,
}

/// A specific answer or recovery request; none of these grant execution permission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResumeAction {
    /// Accept a fixed approval target.
    Approve {
        /// Matching wait identity.
        wait_id: Id,
        /// Exact target presented for approval.
        target: ApprovalTarget,
    },
    /// Reject a fixed approval target.
    Deny {
        /// Matching wait identity.
        wait_id: Id,
        /// Exact target presented for approval.
        target: ApprovalTarget,
        /// User-supplied rejection reason.
        reason: String,
    },
    /// Supply data for a recorded input request.
    Input {
        /// Matching wait identity.
        wait_id: Id,
        /// Answer data, validated against the saved request by the resume handler.
        answer: serde_json::Value,
    },
    /// Supply a protected receipt for an external effect.
    External {
        /// Matching wait identity.
        wait_id: Id,
        /// Evidence to be verified by the authorized handler.
        receipt_ref: RecordRef,
    },
    /// Resume an interrupted nonterminal execution.
    Recover {
        /// Host-verified recovery evidence.
        recovery_ref: RecordRef,
    },
}

/// Idempotent resume command; state/policy enforcement is performed by the driver.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeCommand {
    /// Run to resume.
    pub run_id: Id,
    /// Revision the caller observed.
    pub expected_revision: u64,
    /// Deduplicates retries of the same decision.
    pub command_id: Id,
    /// Typed decision or recovery evidence.
    pub action: ResumeAction,
}

impl ResumeCommand {
    /// Decode an unambiguous command without executing or authorizing it.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        decode(input, None)
    }
}

/// Durable acceptance of one resume command and the segment it continued.
/// Values and references are protected run data, not public event payloads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeReceipt {
    /// Exact command, including the decision or answer used for deduplication.
    pub command: ResumeCommand,
    /// Protected immutable copy used by the resumed event.
    pub command_ref: RecordRef,
    /// Revision at which this command was accepted and its new segment began.
    pub accepted_revision: u64,
    /// Whether the original wait or Run deadline had already elapsed at acceptance.
    /// An expired approval is not execution authority.
    #[serde(default)]
    pub expired: bool,
    /// Start revision of the preceding segment; zero identifies the initial segment.
    pub previous_segment_start_revision: u64,
    /// Original saved Waiting outcome returned by handles for the preceding segment.
    pub previous_outcome_ref: RecordRef,
    /// Last durable event in the preceding segment.
    pub previous_last_event_seq: u64,
    /// Authenticated actor who authorized this command.
    pub actor_ref: Id,
    /// Current Host grant used when accepting the command.
    pub capability_grant_ref: Id,
}

/// Public run status categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Actively processing.
    Running,
    /// Persisted wait.
    Waiting,
    /// Completion policy satisfied.
    Succeeded,
    /// Unrecoverable failure.
    Failed,
    /// Explicit cancellation completed.
    Cancelled,
    /// A finite execution budget was exhausted.
    Exhausted,
}

impl RunStatus {
    /// Whether this status cannot be resumed as the same run.
    pub fn is_terminal(self) -> bool {
        !matches!(self, Self::Running | Self::Waiting)
    }
}

/// Driver phases; transition execution belongs to the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPhase {
    /// Admission validation and initial storage.
    Admission,
    /// Context and request preparation.
    Prepare,
    /// One model invocation.
    Model,
    /// Tool round processing.
    Tool,
    /// Output and completion checks.
    Verify,
    /// Saved wait.
    Waiting,
    /// Terminal outcome committed.
    Finish,
}

/// Stored budget consumption; usage measurement and reservation happen elsewhere.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetUsage {
    /// Reserved physical model attempts.
    pub model_calls: u64,
    /// Reserved physical tool attempts.
    pub tool_attempts: u64,
    /// Candidate repair attempts.
    pub repair_attempts: u64,
    /// Execution recovery attempts.
    pub recovery_attempts: u64,
    /// Elapsed milliseconds including waits.
    pub elapsed_ms: u64,
}

/// The budget that stopped an execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetKind {
    /// Model attempts.
    ModelCalls,
    /// Tool attempts.
    ToolAttempts,
    /// Repairs.
    RepairAttempts,
    /// Recoveries.
    RecoveryAttempts,
    /// Elapsed wall time.
    Elapsed,
}

/// What supports a successful outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionBasis {
    /// The model ended its turn; external business success is not asserted.
    TurnEnded,
    /// A pinned verifier accepted the candidate.
    Verified,
}

/// Recorded verifier classification, separate from transport errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationVerdict {
    /// Candidate accepted.
    Pass,
    /// Candidate needs revision.
    Revise,
    /// Human review required.
    Wait,
    /// Candidate rejected.
    Fail,
}

/// Evidence supporting the recorded verifier decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationSummary {
    /// Verifier actually used.
    pub verifier_ref: VersionedRef,
    /// Exact evaluation criteria.
    pub criteria_ref: VersionedRef,
    /// Decision classification.
    pub verdict: VerificationVerdict,
    /// Protected evidence records.
    pub evidence: Vec<RecordRef>,
}

/// Outcome-specific data. Success always names its completion basis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutcomeResult {
    /// Saved waiting outcome for the current execution segment.
    Waiting {
        /// Wait data.
        wait: WaitState,
    },
    /// Completion policy satisfied.
    Succeeded {
        /// Why completion was accepted.
        completion_basis: CompletionBasis,
    },
    /// Execution failed.
    Failed {
        /// Classified failure.
        failure: Failure,
    },
    /// Cancellation completed; existing effects remain in their records.
    Cancelled {
        /// Cancellation reason.
        reason: String,
    },
    /// Execution budget exhausted.
    Exhausted {
        /// Exhausted budget.
        budget: BudgetKind,
    },
}

impl OutcomeResult {
    /// Public status of this outcome.
    pub fn status(&self) -> RunStatus {
        match self {
            Self::Waiting { .. } => RunStatus::Waiting,
            Self::Succeeded { .. } => RunStatus::Succeeded,
            Self::Failed { .. } => RunStatus::Failed,
            Self::Cancelled { .. } => RunStatus::Cancelled,
            Self::Exhausted { .. } => RunStatus::Exhausted,
        }
    }
}

/// Stored outcome; it is the authority for completion, not an event or text delta.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunOutcome {
    /// Outcome classification and required status-specific data.
    pub result: OutcomeResult,
    /// Final or partial output.
    pub output: Vec<InputContent>,
    /// Produced artifact metadata.
    pub artifacts: Vec<ArtifactRef>,
    /// Consumption recorded at this checkpoint.
    pub usage: BudgetUsage,
    /// Exact checkpoint revision.
    pub checkpoint_revision: u64,
    /// Optional verifier evidence; mandatory for verified success.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub verification: Option<VerificationSummary>,
    /// Effects that must not be blindly repeated.
    pub unresolved_effects: Vec<RecordRef>,
}

impl RunOutcome {
    /// Check required evidence for a verified success.
    pub fn validate(&self) -> Result<(), ContractError> {
        if matches!(self.result, OutcomeResult::Succeeded { .. })
            && !self.unresolved_effects.is_empty()
        {
            return Err(ContractError::new(
                ErrorCode::InvalidContract,
                "outcome.unresolved_effects",
            ));
        }
        if matches!(
            self.result,
            OutcomeResult::Succeeded {
                completion_basis: CompletionBasis::Verified
            }
        ) && !self
            .verification
            .as_ref()
            .is_some_and(|v| v.verdict == VerificationVerdict::Pass)
        {
            return Err(ContractError::new(
                ErrorCode::InvalidContract,
                "outcome.verification",
            ));
        }
        Ok(())
    }
}

/// State of one planned tool call; this does not execute state transitions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolCallState {
    /// Plan is saved and no dispatch is recorded.
    Planned {},
    /// Dispatch was reserved and may have happened.
    Dispatching {
        /// Physical attempt identity.
        attempt_id: Id,
        /// Stable key for external deduplication/reconciliation.
        idempotency_key: Id,
    },
    /// A charged attempt was stopped for approval before entering the executor.
    ApprovalPending {
        /// Reservation that remains charged even though execution did not start.
        attempt_id: Id,
        /// Frozen effect key reused if execution is later authorized.
        idempotency_key: Id,
    },
    /// A no-effect input request is awaiting an answer instead of reentering its executor.
    InputPending {
        /// Charged attempt that requested the input.
        attempt_id: Id,
        /// Original effect identity, retained while the call is incomplete.
        idempotency_key: Id,
        /// Core-generated question tied to this call. Its exact compiled output
        /// schema validates the answer when schema_ref is absent.
        request: InputRequest,
    },
    /// Result was recorded.
    Settled {
        /// Paired tool result.
        result: ToolResult,
    },
    /// Effect is unknown after interruption.
    Unknown {
        /// Uncertain attempt identity.
        attempt_id: Id,
        /// Original external effect key.
        idempotency_key: Id,
    },
}

/// Planned model arguments and the corresponding dispatch/result state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolLedgerEntry {
    /// Original call and protected bound-input reference.
    pub call: ToolCall,
    /// Dispatch/result state.
    pub state: ToolCallState,
}

/// Protected system-input storage reference and versions used in request identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemInputSnapshotRef {
    /// Protected storage location; excluded from logical request identity.
    pub snapshot_ref: RecordRef,
    /// Digest of the validated owned values, including an explicit empty map.
    pub values_digest: JsonDigest,
    /// Exact registered input-definition versions.
    pub definition_versions: BTreeMap<Id, Id>,
}

/// Digest of logical start input, independent of a storage record's location.
/// A missing system-input map is the empty map for start; resume preserves the
/// separate missing/empty distinction in ExecutionContextData.
pub fn admission_digest(
    request: &RunRequest,
    profile: &ResolvedProfile,
    system_inputs: Option<&SystemInputSnapshotRef>,
) -> JsonDigest {
    let empty_digest = crate::canonical_digest(&serde_json::json!({}));
    let empty_versions = BTreeMap::new();
    let (values, versions) = system_inputs
        .map(|s| (&s.values_digest, &s.definition_versions))
        .unwrap_or((&empty_digest, &empty_versions));
    data_digest(&(request, profile.profile_digest(), values, versions))
}

/// Session metadata pinned across requests. A store enforces the active-run rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSnapshot {
    /// Session document version.
    pub schema_version: SessionSchemaVersion,
    /// Session identity.
    pub session_id: Id,
    /// Owning scope.
    pub scope: Scope,
    /// Pinned profile identity.
    pub profile_digest: JsonDigest,
    /// Pinned prompt data reference.
    pub prompt_snapshot: RecordRef,
    /// Current transcript revision.
    pub transcript_revision: u64,
    /// One active running/waiting run, or omission when none exists.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub active_run_id: Option<Id>,
}

/// Saved context-source execution position for retry and resume reuse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceExecutionState {
    /// Exact source selection, including adapter binding when applicable.
    pub source: crate::ContextSourceRef,
    /// Stable context request identity.
    pub context_request_id: Id,
    /// Collection trigger.
    pub trigger: crate::ContextTrigger,
    /// Required for a before_model collection.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub model_step_id: Option<Id>,
    /// Committed context batch, including empty/unavailable results.
    pub batch_ref: RecordRef,
}

/// Run checkpoint DTO. Use `from_json` or `validate` at the storage boundary.
/// Protected inputs are references, not automatically exposed execution arguments.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSnapshot {
    /// Checkpoint document version.
    pub schema_version: RunSnapshotSchemaVersion,
    /// Run identity.
    pub run_id: Id,
    /// Original caller request.
    pub request: RunRequest,
    /// Logical input digest used for deduplication.
    pub request_digest: JsonDigest,
    /// Scope used for storage, policy, tools, and resume.
    pub scope: Scope,
    /// Immutable profile and resolved definition identities.
    pub profile: ResolvedProfile,
    /// Current execution status.
    pub status: RunStatus,
    /// Current driver phase.
    pub phase: RunPhase,
    /// Current logical model step, if allocated.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub model_step_id: Option<Id>,
    /// Effective limits, no greater than profile limits.
    pub limits: RunLimits,
    /// Saved usage/reservations.
    pub usage: BudgetUsage,
    /// Original admission/deadline and persisted monotonic elapsed-time anchor.
    pub timing: RunTiming,
    /// Append-only charged attempt reservations, preserved across errors and resume.
    pub reservations: Vec<AttemptReservation>,
    /// Append-only resume acceptances and prior segment outcomes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resume_receipts: Vec<ResumeReceipt>,
    /// Physical model attempt records.
    pub model_ledger: Vec<ModelInvocationRecord>,
    /// Saved tool plans and states.
    pub tool_ledger: Vec<ToolLedgerEntry>,
    /// Pinned, protected system values and their contract revisions.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub system_inputs: Option<SystemInputSnapshotRef>,
    /// Saved wait data, only while waiting.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub wait: Option<WaitState>,
    /// Last waiting or terminal outcome.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub outcome: Option<RunOutcome>,
    /// Pinned assembly metadata, without process-local handler objects.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub assembly_ref: Option<RecordRef>,
    /// Immutable catalog and routing policy used by this run's model calls.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub routing_snapshot_ref: Option<RecordRef>,
    /// Committed context batches.
    pub context_batches: Vec<RecordRef>,
    /// Saved collection positions.
    pub source_states: Vec<SourceExecutionState>,
    /// Compare-and-swap revision.
    pub revision: u64,
    /// Last durable event sequence; ephemeral deltas do not consume it.
    pub last_event_seq: u64,
}

impl RunSnapshot {
    /// Decode a known checkpoint version and verify static consistency.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        let snapshot: Self = decode(input, Some(RUN_SNAPSHOT_SCHEMA_VERSION))?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Check static checkpoint invariants without performing recovery or authorization.
    pub fn validate(&self) -> Result<(), ContractError> {
        let invalid = |path| ContractError::new(ErrorCode::InvalidSnapshot, path);
        crate::budget::validate_budget(self)?;
        if &self.scope != self.profile.scope()
            || self.request_digest
                != admission_digest(&self.request, &self.profile, self.system_inputs.as_ref())
        {
            return Err(invalid("request_digest"));
        }
        let requested = &self.profile.profile().limits;
        if self.limits.max_model_calls > requested.max_model_calls
            || self.limits.max_tool_attempts > requested.max_tool_attempts
            || self.limits.max_repair_attempts > requested.max_repair_attempts
            || self.limits.max_recovery_attempts > requested.max_recovery_attempts
            || self.limits.max_elapsed_ms > requested.max_elapsed_ms
        {
            return Err(invalid("limits"));
        }
        match self.status {
            RunStatus::Running
                if matches!(self.phase, RunPhase::Waiting | RunPhase::Finish)
                    || self.wait.is_some()
                    || self.outcome.is_some() =>
            {
                return Err(invalid("status"));
            }
            RunStatus::Waiting if self.phase != RunPhase::Waiting || self.wait.is_none() => {
                return Err(invalid("wait"));
            }
            s if s.is_terminal()
                && (self.phase != RunPhase::Finish
                    || self.wait.is_some()
                    || self.outcome.is_none()) =>
            {
                return Err(invalid("outcome"));
            }
            _ => {}
        }
        if let Some(outcome) = &self.outcome {
            outcome.validate()?;
            if outcome.result.status() != self.status
                || outcome.checkpoint_revision != self.revision
                || outcome.usage != self.usage
            {
                return Err(invalid("outcome"));
            }
            if let OutcomeResult::Waiting { wait } = &outcome.result {
                if self.wait.as_ref() != Some(wait) {
                    return Err(invalid("outcome.wait"));
                }
            }
            if let OutcomeResult::Succeeded { completion_basis } = &outcome.result {
                match (&self.profile.profile().completion_policy, completion_basis) {
                    (CompletionPolicy::TurnEnd {}, CompletionBasis::TurnEnded) => {}
                    (CompletionPolicy::Verified { verifier_ref }, CompletionBasis::Verified)
                        if outcome
                            .verification
                            .as_ref()
                            .is_some_and(|v| &v.verifier_ref == verifier_ref) => {}
                    _ => return Err(invalid("outcome.completion_basis")),
                }
            }
        }
        let mut commands = BTreeSet::new();
        let mut segment = 0;
        for receipt in &self.resume_receipts {
            if receipt.command.run_id != self.run_id
                || !commands.insert(&receipt.command.command_id)
                || receipt.accepted_revision > self.revision
                || receipt.command.expected_revision.checked_add(1)
                    != Some(receipt.accepted_revision)
                || receipt.previous_segment_start_revision != segment
                || receipt.command.expected_revision < segment
                || receipt.previous_last_event_seq > self.last_event_seq
            {
                return Err(invalid("resume_receipts"));
            }
            segment = receipt.accepted_revision;
        }
        let mut calls = BTreeSet::new();
        for entry in &self.tool_ledger {
            if !calls.insert(&entry.call.call_id) {
                return Err(invalid("tool_ledger.call_id"));
            }
            let unregistered_safe = match &entry.state {
                ToolCallState::Planned {} => true,
                ToolCallState::Settled { result } => {
                    result.effect == crate::ToolEffect::NotApplied
                        && matches!(
                            result.status,
                            crate::ToolResultStatus::Failed
                                | crate::ToolResultStatus::Denied
                                | crate::ToolResultStatus::Cancelled
                        )
                }
                _ => false,
            };
            if entry.call.descriptor_digest.is_none()
                && (entry.call.bound_input_ref.is_some() || !unregistered_safe)
            {
                return Err(invalid("tool_ledger.unregistered"));
            }
            match &entry.state {
                ToolCallState::Dispatching { attempt_id, .. } | ToolCallState::Unknown { attempt_id, .. } | ToolCallState::ApprovalPending { attempt_id, .. } | ToolCallState::InputPending { attempt_id, .. }
                    if !self.reservations.iter().any(|reservation| &reservation.attempt_id == attempt_id
                        && matches!(&reservation.kind, ReservationKind::Tool { call_id } if call_id == &entry.call.call_id)) =>
                {
                    return Err(invalid("tool_ledger.reservation"));
                }
                ToolCallState::Dispatching { .. } | ToolCallState::Unknown { .. } | ToolCallState::ApprovalPending { .. } | ToolCallState::InputPending { .. }
                    if entry.call.bound_input_ref.is_none() =>
                {
                    return Err(invalid("tool_ledger.bound_input_ref"));
                }
                ToolCallState::Settled { result } if result.call_id != entry.call.call_id => {
                    return Err(invalid("tool_ledger.result.call_id"));
                }
                ToolCallState::InputPending { request, .. }
                    if request.call_id != entry.call.call_id || request.question.trim().is_empty() =>
                {
                    return Err(invalid("tool_ledger.input_request"));
                }
                _ => {}
            }
            if self.status == RunStatus::Succeeded
                && !matches!(&entry.state, ToolCallState::Settled { result } if result.status != crate::ToolResultStatus::Unknown && result.effect != crate::ToolEffect::Unknown)
            {
                return Err(invalid("tool_ledger.unsettled"));
            }
        }
        let mut attempts = BTreeSet::new();
        let model_reservations: BTreeMap<_, _> = self
            .reservations
            .iter()
            .filter_map(|reservation| match reservation.kind {
                ReservationKind::Model { purpose } => Some((&reservation.attempt_id, purpose)),
                _ => None,
            })
            .collect();
        for invocation in &self.model_ledger {
            if invocation.run_id != self.run_id || !attempts.insert(&invocation.attempt_id) {
                return Err(invalid("model_ledger.attempt_id"));
            }
            if model_reservations.get(&invocation.attempt_id) != Some(&invocation.purpose) {
                return Err(invalid("model_ledger.reservation"));
            }
            let settled = matches!(
                invocation.state,
                ModelAttemptState::Completed {} | ModelAttemptState::Failed { .. }
            );
            if settled != invocation.response_ref.is_some() {
                return Err(invalid("model_ledger.response_ref"));
            }
        }
        if self
            .source_states
            .iter()
            .any(|s| (s.trigger == crate::ContextTrigger::BeforeModel) != s.model_step_id.is_some())
        {
            return Err(invalid("source_states.model_step_id"));
        }
        Ok(())
    }
}

/// Durable facts reference stored records rather than copying protected inputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum RunEventPayload {
    /// Admission committed.
    #[serde(rename = "run.started")]
    RunStarted {
        /// Accepted request reference.
        request_ref: RecordRef,
        /// Pinned profile identity.
        profile_digest: JsonDigest,
    },
    /// Tool plan committed.
    #[serde(rename = "tool.planned")]
    ToolPlanned {
        /// Protected call record.
        call_ref: RecordRef,
    },
    /// Tool result committed.
    #[serde(rename = "tool.settled")]
    ToolSettled {
        /// Protected result record.
        result_ref: RecordRef,
    },
    /// A recorded result cannot establish whether an external operation applied.
    #[serde(rename = "tool.unresolved")]
    ToolUnresolved {
        /// Protected paired Unknown result.
        result_ref: RecordRef,
        /// Uncertain physical attempt whose reservation remains charged.
        attempt_id: Id,
        /// Original external effect key, retained for reconciliation.
        idempotency_key: Id,
    },
    /// Verifier decision committed.
    #[serde(rename = "verification.completed")]
    VerificationCompleted {
        /// Recorded verification evidence.
        verification_ref: RecordRef,
    },
    /// Wait committed.
    #[serde(rename = "run.waiting")]
    RunWaiting {
        /// Recorded wait.
        wait_ref: RecordRef,
    },
    /// Resume command consumed.
    #[serde(rename = "run.resumed")]
    RunResumed {
        /// Consumed command record.
        command_ref: RecordRef,
    },
    /// Terminal outcome committed.
    #[serde(rename = "run.finished")]
    RunFinished {
        /// Authoritative outcome record.
        outcome_ref: RecordRef,
    },
    /// Model route and invocation identity committed.
    #[serde(rename = "model.route_selected")]
    ModelRouteSelected {
        /// Invocation record.
        invocation_ref: RecordRef,
        /// Selected route identity.
        route_digest: JsonDigest,
    },
}

/// A durable event committed atomically with authoritative state by the store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunEvent {
    /// Independent event wire version.
    pub schema_version: RunEventSchemaVersion,
    /// Stable event identity for deduplication.
    pub event_id: Id,
    /// Owning scope.
    pub scope: Scope,
    /// Owning run.
    pub run_id: Id,
    /// Owning session.
    pub session_id: Id,
    /// Positive durable event sequence.
    pub seq: NonZeroU64,
    /// UTC milliseconds since Unix epoch.
    pub timestamp_ms: i64,
    /// Typed event data with authorized record references.
    pub payload: RunEventPayload,
}

impl RunEvent {
    /// Decode a known event format without replaying or dispatching it.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        decode(input, Some(RUN_EVENT_SCHEMA_VERSION))
    }
}

/// Non-durable presentation hints; these carry no durable sequence or completion claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum EphemeralEvent {
    /// Candidate text from an incomplete model response.
    CandidateTextDelta {
        /// Owning run.
        run_id: Id,
        /// Physical model attempt.
        attempt_id: Id,
        /// Candidate text, not a committed final answer.
        text: String,
    },
}
```

## `crates/wickle/src/state.rs`

```rust
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroU64,
    sync::{Mutex, MutexGuard},
};

use serde::de::DeserializeOwned;
use serde_json::Value;

mod checkpoint;
pub use checkpoint::{STATE_STORE_CHECKPOINT_VERSION, StateStoreCheckpoint};

use crate::{
    ApprovalTarget, BudgetUsage, ContentBlock, ContractError, ErrorCode, Id, Message,
    ModelAttemptState, ModelExchangeOutcome, ModelFinish, ModelInvocationRecord, OutcomeResult,
    PortFuture, RecordRef, ResumeAction, ResumeCommand, RunEvent, RunEventPayload, RunPhase,
    RunSnapshot, RunStatus, Scope, SessionSchemaVersion, SessionSnapshot, StoredModelResponse,
    ToolCall, ToolCallState, ToolResult, VerificationSummary, WaitState, WaitTarget,
    admission_digest, canonical_digest,
};

/// Guarantees offered by a state-store implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateStoreCapabilities {
    /// Records survive process termination.
    pub durable: bool,
    /// Execution leases coordinate independent processes.
    pub cross_process_leases: bool,
    /// Committed events can be replayed in sequence order.
    pub event_replay: bool,
}

/// Immutable, scope-owned data stored with its referencing state and events.
/// Access requires Host authorization; Debug never prints the payload.
#[derive(Clone, PartialEq)]
pub struct ProtectedRecord {
    reference: RecordRef,
    value: Value,
}

impl ProtectedRecord {
    /// Compute the reference digest from owned data. A revision is immutable.
    pub fn new(record_id: Id, revision: u64, value: Value) -> Self {
        Self {
            reference: RecordRef {
                record_id,
                revision,
                digest: canonical_digest(&value),
            },
            value,
        }
    }

    /// Exact immutable record identity, without its payload.
    pub fn reference(&self) -> &RecordRef {
        &self.reference
    }

    /// Explicit privileged access, never an automatic public/model projection.
    pub fn value(&self) -> &Value {
        &self.value
    }
}

impl fmt::Debug for ProtectedRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtectedRecord")
            .field("reference", &self.reference)
            .field("value", &"<redacted>")
            .finish()
    }
}

/// Initial records accepted atomically for a newly admitted run.
#[derive(Clone)]
pub struct AdmissionInput {
    /// Running/admission checkpoint at revision zero.
    pub snapshot: RunSnapshot,
    /// Session-pinned prompt record; reused unchanged by subsequent runs.
    pub prompt_snapshot: RecordRef,
    /// New messages, numbered consecutively across the session.
    pub messages: Vec<Message>,
    /// One run.started event at sequence one, referencing the accepted request.
    pub events: Vec<RunEvent>,
    /// New immutable records, available to references in this transaction.
    pub records: Vec<ProtectedRecord>,
    /// Reject implementations that cannot preserve state across process termination.
    pub require_durable: bool,
}

/// An owned protected checkpoint and its complete session transcript.
/// Use PolicyGate views to select data for less privileged callers.
#[derive(Clone, PartialEq)]
pub struct StoredRun {
    /// Current run checkpoint and protected record references.
    pub snapshot: RunSnapshot,
    /// Current session metadata, including its active run.
    pub session: SessionSnapshot,
    /// Append-only session transcript, including messages from earlier runs.
    pub messages: Vec<Message>,
}

impl fmt::Debug for StoredRun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredRun")
            .field("run_id", &self.snapshot.run_id)
            .field("revision", &self.snapshot.revision)
            .field("message_count", &self.messages.len())
            .finish_non_exhaustive()
    }
}

/// Admission reports whether it created a run or found the original request.
#[derive(Debug, Clone, PartialEq)]
pub struct AdmissionResult {
    /// False for identical request replay; candidate records are not applied.
    pub created: bool,
    /// Existing or newly admitted run, with its original pinned data.
    pub state: StoredRun,
}

/// Store-issued lease identity. Possession is not Host authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLease {
    /// Exact resource namespace.
    pub scope: Scope,
    /// Run owned by this lease.
    pub run_id: Id,
    /// Worker identity supplied by trusted runtime code.
    pub owner: Id,
    /// Increasing generation retained across expiration and release.
    pub fencing_token: u64,
    /// Expiration reported when issued. Validation uses the store's current expiry,
    /// so renewal does not invalidate copies of the same owner/fencing generation.
    pub expires_at_ms: i64,
}

/// A complete candidate checkpoint and append-only data for one atomic commit.
#[derive(Clone)]
pub struct CommitInput {
    /// Compare-and-swap revision of the currently saved checkpoint.
    pub expected_revision: u64,
    /// Current unexpired execution lease.
    pub lease: RunLease,
    /// Trusted current UTC milliseconds, also used to reject expired leases.
    pub now_ms: i64,
    /// Next checkpoint, at expected_revision + 1.
    pub snapshot: RunSnapshot,
    /// New messages, continuing the session sequence.
    pub messages: Vec<Message>,
    /// New events, continuing the run sequence.
    pub events: Vec<RunEvent>,
    /// Immutable records to insert in the same transaction.
    pub records: Vec<ProtectedRecord>,
}

/// A bounded, ordered page of protected durable events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventPage {
    /// Events strictly after the supplied cursor.
    pub events: Vec<RunEvent>,
    /// Cursor for the next page, unchanged for an empty page.
    pub next_after_seq: u64,
    /// More events were available when this page was read.
    pub has_more: bool,
    /// Oldest retained sequence; None when no events are stored.
    pub first_available_seq: Option<NonZeroU64>,
    /// Latest committed sequence when this page was read.
    pub last_available_seq: u64,
}

/// Largest event page accepted by the reference store.
pub const MAX_EVENT_PAGE_SIZE: usize = 1_000;

/// Trusted core storage port. Scope isolation is enforced by the store itself.
/// The facade separately applies current PolicyGate authorization. No raw load or
/// record reference grants permission to publish the returned data.
pub trait StateStore: Send + Sync {
    /// Describe storage and coordination guarantees.
    fn capabilities(&self) -> StateStoreCapabilities;
    /// Find the original request before re-resolving current profile or routing metadata.
    /// Missing scope/request returns None. Atomic admission remains the final deduplication boundary.
    fn find_request<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
        request_id: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>>;
    /// Atomically deduplicate a request and reserve its session's active-run slot.
    fn admit<'a>(
        &'a self,
        scope: &'a Scope,
        input: AdmissionInput,
    ) -> PortFuture<'a, AdmissionResult>;
    /// Load owned state and the complete session transcript.
    fn load<'a>(&'a self, scope: &'a Scope, run_id: &'a Id) -> PortFuture<'a, StoredRun>;
    /// Read session-pinned metadata without changing its active run.
    fn load_session<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
    ) -> PortFuture<'a, SessionSnapshot>;
    /// Validate owner/generation against the current stored expiry without renewing.
    /// Return the latest lease metadata, including any concurrent heartbeat renewal.
    fn check_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, RunLease>;
    /// Acquire a new generation after any previous lease has expired or been released.
    fn acquire_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        owner: &'a Id,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease>;
    /// Renew an unexpired generation; an expired lease cannot be revived.
    fn renew_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease>;
    /// Release only the currently owned unexpired generation.
    fn release_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, ()>;
    /// Validate and commit state, transcript, records and events atomically.
    fn commit<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun>;
    /// Replay a bounded page. Retention gaps must not silently skip missing events.
    fn read_events<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        after_seq: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage>;
    /// Read an exact scope-owned immutable record after separate Host authorization.
    fn read_record<'a>(
        &'a self,
        scope: &'a Scope,
        reference: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord>;
}

type ScopeKey = (Id, Id, Option<Id>);
type RecordKey = (Id, u64);

#[derive(Clone, Default)]
struct ScopeState {
    sessions: BTreeMap<Id, SessionState>,
    runs: BTreeMap<Id, RunState>,
    requests: BTreeMap<(Id, Id), Id>,
    records: BTreeMap<RecordKey, ProtectedRecord>,
    event_ids: BTreeSet<Id>,
    message_ids: BTreeSet<Id>,
}

#[derive(Clone)]
struct SessionState {
    snapshot: SessionSnapshot,
    messages: Vec<Message>,
}

#[derive(Clone)]
struct RunState {
    snapshot: RunSnapshot,
    events: Vec<RunEvent>,
    lease: Option<RunLease>,
    last_fencing_token: u64,
}

/// Process-local reference store. It retains all committed data for its lifetime.
/// A single short critical section validates and applies each transaction; no
/// external calls or awaits occur while the lock is held. It provides neither
/// process-restart durability nor coordination between separate processes.
#[derive(Default)]
pub struct MemoryStateStore {
    scopes: Mutex<BTreeMap<ScopeKey, ScopeState>>,
}

impl MemoryStateStore {
    /// Construct an empty store without creating a runtime or doing I/O.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<MutexGuard<'_, BTreeMap<ScopeKey, ScopeState>>, ContractError> {
        self.scopes
            .lock()
            .map_err(|_| error(ErrorCode::PersistenceUnavailable, "state_store"))
    }
}

impl StateStore for MemoryStateStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        StateStoreCapabilities {
            durable: false,
            cross_process_leases: false,
            event_replay: true,
        }
    }

    fn find_request<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
        request_id: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        Box::pin(async move {
            let scopes = self.lock()?;
            let Some(state) = scopes.get(&scope_key(scope)) else {
                return Ok(None);
            };
            state
                .requests
                .get(&(session_id.clone(), request_id.clone()))
                .map(|run_id| stored_run(state, run_id))
                .transpose()
        })
    }

    fn admit<'a>(
        &'a self,
        scope: &'a Scope,
        input: AdmissionInput,
    ) -> PortFuture<'a, AdmissionResult> {
        Box::pin(async move {
            if input.require_durable {
                return Err(error(ErrorCode::CapabilityUnsupported, "store.durable"));
            }
            check_scope(scope, &input.snapshot.scope)?;
            if scope != input.snapshot.profile.scope()
                || input.snapshot.request_digest
                    != admission_digest(
                        &input.snapshot.request,
                        &input.snapshot.profile,
                        input.snapshot.system_inputs.as_ref(),
                    )
            {
                return Err(error(ErrorCode::InvalidSnapshot, "request_digest"));
            }
            let mut scopes = self.lock()?;
            let empty = ScopeState::default();
            let state = scopes.get(&scope_key(scope)).unwrap_or(&empty);
            let request_key = (
                input.snapshot.request.session_id.clone(),
                input.snapshot.request.request_id.clone(),
            );
            if let Some(run_id) = state.requests.get(&request_key) {
                let previous = stored_run(state, run_id)?;
                if previous.snapshot.request_digest != input.snapshot.request_digest {
                    return Err(error(ErrorCode::RequestConflict, "request"));
                }
                return Ok(AdmissionResult {
                    created: false,
                    state: previous,
                });
            }
            input.snapshot.validate()?;
            if input.snapshot.revision != 0
                || input.snapshot.status != RunStatus::Running
                || input.snapshot.phase != RunPhase::Admission
                || !input.snapshot.model_ledger.is_empty()
                || !input.snapshot.tool_ledger.is_empty()
                || !input.snapshot.reservations.is_empty()
                || !input.snapshot.resume_receipts.is_empty()
                || input.snapshot.usage != BudgetUsage::default()
            {
                return Err(error(ErrorCode::InvalidSnapshot, "admission"));
            }
            if state.runs.contains_key(&input.snapshot.run_id) {
                return Err(error(ErrorCode::RunConflict, "run_id"));
            }
            let session_id = &input.snapshot.request.session_id;
            let previous_session = state.sessions.get(session_id);
            if let Some(session) = previous_session {
                if session.snapshot.profile_digest != *input.snapshot.profile.profile_digest()
                    || session.snapshot.prompt_snapshot != input.prompt_snapshot
                {
                    return Err(error(ErrorCode::ProfileMismatch, "session.profile"));
                }
                if session.snapshot.active_run_id.is_some() {
                    return Err(error(ErrorCode::SessionBusy, "session"));
                }
            }
            let additions = validate_records(state, &input.records)?;
            record_value(state, &additions, &input.prompt_snapshot)?;
            validate_snapshot_refs(state, &additions, &input.snapshot)?;
            validate_events(
                state,
                &additions,
                &input.snapshot,
                0,
                &input.events,
                true,
                &input.messages,
            )?;
            let previous_sequence = previous_session.map_or(0, |s| s.snapshot.transcript_revision);
            let transcript_revision = validate_messages(
                state,
                &additions,
                &input.snapshot.run_id,
                previous_sequence,
                &input.messages,
            )?;
            let mut messages = previous_session.map_or_else(Vec::new, |s| s.messages.clone());
            messages.extend(input.messages);
            let session = SessionSnapshot {
                schema_version: SessionSchemaVersion::V1,
                session_id: session_id.clone(),
                scope: scope.clone(),
                profile_digest: input.snapshot.profile.profile_digest().clone(),
                prompt_snapshot: input.prompt_snapshot,
                transcript_revision,
                active_run_id: Some(input.snapshot.run_id.clone()),
            };
            let result = StoredRun {
                snapshot: input.snapshot.clone(),
                session: session.clone(),
                messages: messages.clone(),
            };
            // All fallible checks precede these mutations.
            let state = scopes.entry(scope_key(scope)).or_default();
            state.records.extend(additions);
            state
                .message_ids
                .extend(messages.iter().map(|m| m.message_id.clone()));
            state
                .event_ids
                .extend(input.events.iter().map(|e| e.event_id.clone()));
            state
                .requests
                .insert(request_key, input.snapshot.run_id.clone());
            state.sessions.insert(
                session.session_id.clone(),
                SessionState {
                    snapshot: session,
                    messages,
                },
            );
            state.runs.insert(
                input.snapshot.run_id.clone(),
                RunState {
                    snapshot: input.snapshot,
                    events: input.events,
                    lease: None,
                    last_fencing_token: 0,
                },
            );
            Ok(AdmissionResult {
                created: true,
                state: result,
            })
        })
    }

    fn load<'a>(&'a self, scope: &'a Scope, run_id: &'a Id) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            let scopes = self.lock()?;
            stored_run(namespace(&scopes, scope)?, run_id)
        })
    }

    fn load_session<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
    ) -> PortFuture<'a, SessionSnapshot> {
        Box::pin(async move {
            let scopes = self.lock()?;
            namespace(&scopes, scope)?
                .sessions
                .get(session_id)
                .map(|session| session.snapshot.clone())
                .ok_or_else(not_found)
        })
    }

    fn check_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, RunLease> {
        Box::pin(async move {
            let scopes = self.lock()?;
            let run = namespace(&scopes, scope)?
                .runs
                .get(run_id)
                .ok_or_else(not_found)?;
            validate_lease(run, scope, run_id, lease, now_ms)?;
            Ok(run.lease.as_ref().expect("validated lease").clone())
        })
    }

    fn acquire_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        owner: &'a Id,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease> {
        Box::pin(async move {
            let expires_at_ms = lease_expiry(now_ms, ttl_ms)?;
            let mut scopes = self.lock()?;
            let run = run_mut(&mut scopes, scope, run_id)?;
            if run.snapshot.status.is_terminal() {
                return Err(error(ErrorCode::InvalidTransition, "run.status"));
            }
            if run.lease.as_ref().is_some_and(|l| l.expires_at_ms > now_ms) {
                return Err(error(ErrorCode::LeaseBusy, "lease"));
            }
            let fencing_token = run
                .last_fencing_token
                .checked_add(1)
                .ok_or_else(|| error(ErrorCode::InvalidContract, "lease.fencing_token"))?;
            let lease = RunLease {
                scope: scope.clone(),
                run_id: run_id.clone(),
                owner: owner.clone(),
                fencing_token,
                expires_at_ms,
            };
            run.last_fencing_token = fencing_token;
            run.lease = Some(lease.clone());
            Ok(lease)
        })
    }

    fn renew_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease> {
        Box::pin(async move {
            let expires_at_ms = lease_expiry(now_ms, ttl_ms)?;
            let mut scopes = self.lock()?;
            let run = run_mut(&mut scopes, scope, run_id)?;
            validate_lease(run, scope, run_id, lease, now_ms)?;
            let renewed = RunLease {
                expires_at_ms,
                ..lease.clone()
            };
            run.lease = Some(renewed.clone());
            Ok(renewed)
        })
    }

    fn release_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let mut scopes = self.lock()?;
            let run = run_mut(&mut scopes, scope, run_id)?;
            validate_lease(run, scope, run_id, lease, now_ms)?;
            run.lease = None;
            Ok(())
        })
    }

    fn commit<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            check_scope(scope, &input.snapshot.scope)?;
            let mut scopes = self.lock()?;
            let state = namespace(&scopes, scope)?;
            let run = state.runs.get(run_id).ok_or_else(not_found)?;
            validate_lease(run, scope, run_id, &input.lease, input.now_ms)?;
            if run.snapshot.revision != input.expected_revision {
                return Err(error(ErrorCode::RevisionConflict, "revision"));
            }
            validate_transition(&run.snapshot, &input.snapshot)?;
            if input.events.iter().any(|event| {
                matches!(event.payload, RunEventPayload::RunResumed { .. })
                    && event.timestamp_ms > input.now_ms
            }) {
                return Err(error(ErrorCode::InvalidEvent, "events.resume_time"));
            }
            if let Some(receipt) = input
                .snapshot
                .resume_receipts
                .last()
                .filter(|receipt| receipt.accepted_revision == input.snapshot.revision)
            {
                let expired = input.now_ms >= run.snapshot.timing.deadline_at_ms
                    || run
                        .snapshot
                        .wait
                        .as_ref()
                        .and_then(|wait| wait.expires_at_ms)
                        .is_some_and(|deadline| input.now_ms >= deadline);
                // A durable adapter may advance the lease-check time after
                // queue/lock delay. Crossing expiry must not turn a stale
                // on-time decision into an accepted approval.
                if receipt.expired != expired {
                    return Err(error(
                        ErrorCode::DeadlineExceeded,
                        "resume.acceptance_expiry",
                    ));
                }
            }
            let additions = validate_records(state, &input.records)?;
            validate_snapshot_refs(state, &additions, &input.snapshot)?;
            validate_events(
                state,
                &additions,
                &input.snapshot,
                run.snapshot.last_event_seq,
                &input.events,
                false,
                &input.messages,
            )?;
            let session = state
                .sessions
                .get(&run.snapshot.request.session_id)
                .ok_or_else(not_found)?;
            if session.snapshot.active_run_id.as_ref() != Some(run_id) {
                return Err(error(ErrorCode::InvalidTransition, "session.active_run_id"));
            }
            let transcript_revision = validate_messages(
                state,
                &additions,
                run_id,
                session.snapshot.transcript_revision,
                &input.messages,
            )?;
            let mut session_snapshot = session.snapshot.clone();
            let history: Vec<_> = run.events.iter().chain(&input.events).collect();
            let transcript: Vec<_> = session.messages.iter().chain(&input.messages).collect();
            validate_resume_history(state, &additions, &input.snapshot, &history, &transcript)?;
            session_snapshot.transcript_revision = transcript_revision;
            if input.snapshot.status.is_terminal() {
                session_snapshot.active_run_id = None;
            }
            let mut messages = session.messages.clone();
            messages.extend(input.messages);
            let result = StoredRun {
                snapshot: input.snapshot.clone(),
                session: session_snapshot.clone(),
                messages: messages.clone(),
            };
            let state = scopes
                .get_mut(&scope_key(scope))
                .expect("validated namespace");
            state.records.extend(additions);
            state
                .message_ids
                .extend(messages.iter().map(|m| m.message_id.clone()));
            state
                .event_ids
                .extend(input.events.iter().map(|e| e.event_id.clone()));
            state.sessions.insert(
                session_snapshot.session_id.clone(),
                SessionState {
                    snapshot: session_snapshot,
                    messages,
                },
            );
            let run = state.runs.get_mut(run_id).expect("validated run");
            run.snapshot = input.snapshot;
            run.events.extend(input.events);
            if run.snapshot.status.is_terminal() {
                run.lease = None;
            }
            Ok(result)
        })
    }

    fn read_events<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        after_seq: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage> {
        Box::pin(async move {
            if limit == 0 || limit > MAX_EVENT_PAGE_SIZE {
                return Err(error(ErrorCode::InvalidContract, "events.limit"));
            }
            let scopes = self.lock()?;
            let run = namespace(&scopes, scope)?
                .runs
                .get(run_id)
                .ok_or_else(not_found)?;
            let mut available = run.events.iter().filter(|e| e.seq.get() > after_seq);
            let events: Vec<_> = available.by_ref().take(limit).cloned().collect();
            Ok(EventPage {
                next_after_seq: events.last().map_or(after_seq, |e| e.seq.get()),
                has_more: available.next().is_some(),
                first_available_seq: run.events.first().map(|e| e.seq),
                last_available_seq: run.snapshot.last_event_seq,
                events,
            })
        })
    }

    fn read_record<'a>(
        &'a self,
        scope: &'a Scope,
        reference: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        Box::pin(async move {
            let scopes = self.lock()?;
            let record = namespace(&scopes, scope)?
                .records
                .get(&record_key(reference))
                .ok_or_else(not_found)?;
            if record.reference != *reference {
                return Err(error(ErrorCode::RecordConflict, "record.reference"));
            }
            Ok(record.clone())
        })
    }
}

fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}

fn not_found() -> ContractError {
    error(ErrorCode::StateNotFound, "state")
}

fn scope_key(scope: &Scope) -> ScopeKey {
    (
        scope.tenant_id.clone(),
        scope.workspace_id.clone(),
        scope.user_id.clone(),
    )
}

fn record_key(reference: &RecordRef) -> RecordKey {
    (reference.record_id.clone(), reference.revision)
}

fn check_scope(expected: &Scope, actual: &Scope) -> Result<(), ContractError> {
    if expected != actual {
        Err(error(ErrorCode::AccessDenied, "scope"))
    } else {
        Ok(())
    }
}

fn namespace<'a>(
    scopes: &'a BTreeMap<ScopeKey, ScopeState>,
    scope: &Scope,
) -> Result<&'a ScopeState, ContractError> {
    scopes.get(&scope_key(scope)).ok_or_else(not_found)
}

fn run_mut<'a>(
    scopes: &'a mut BTreeMap<ScopeKey, ScopeState>,
    scope: &Scope,
    run_id: &Id,
) -> Result<&'a mut RunState, ContractError> {
    scopes
        .get_mut(&scope_key(scope))
        .and_then(|state| state.runs.get_mut(run_id))
        .ok_or_else(not_found)
}

fn stored_run(state: &ScopeState, run_id: &Id) -> Result<StoredRun, ContractError> {
    let run = state.runs.get(run_id).ok_or_else(not_found)?;
    let session = state
        .sessions
        .get(&run.snapshot.request.session_id)
        .ok_or_else(not_found)?;
    Ok(StoredRun {
        snapshot: run.snapshot.clone(),
        session: session.snapshot.clone(),
        messages: session.messages.clone(),
    })
}

fn lease_expiry(now_ms: i64, ttl_ms: u64) -> Result<i64, ContractError> {
    let ttl = i64::try_from(ttl_ms)
        .ok()
        .filter(|ttl| *ttl > 0)
        .ok_or_else(|| error(ErrorCode::InvalidContract, "lease.ttl_ms"))?;
    now_ms
        .checked_add(ttl)
        .ok_or_else(|| error(ErrorCode::InvalidContract, "lease.expires_at_ms"))
}

fn validate_lease(
    run: &RunState,
    scope: &Scope,
    run_id: &Id,
    provided: &RunLease,
    now_ms: i64,
) -> Result<(), ContractError> {
    if &provided.scope != scope
        || &provided.run_id != run_id
        || !run.lease.as_ref().is_some_and(|stored| {
            stored.owner == provided.owner
                && stored.fencing_token == provided.fencing_token
                && now_ms < stored.expires_at_ms
        })
    {
        return Err(error(ErrorCode::LeaseLost, "lease"));
    }
    Ok(())
}

fn validate_records(
    state: &ScopeState,
    records: &[ProtectedRecord],
) -> Result<BTreeMap<RecordKey, ProtectedRecord>, ContractError> {
    let mut additions = BTreeMap::new();
    for record in records {
        let key = record_key(&record.reference);
        if state
            .records
            .get(&key)
            .or_else(|| additions.get(&key))
            .is_some_and(|existing| existing != record)
        {
            return Err(error(ErrorCode::RecordConflict, "records"));
        }
        additions.insert(key, record.clone());
    }
    Ok(additions)
}

fn record_value<'a>(
    state: &'a ScopeState,
    additions: &'a BTreeMap<RecordKey, ProtectedRecord>,
    reference: &RecordRef,
) -> Result<&'a Value, ContractError> {
    let record = additions
        .get(&record_key(reference))
        .or_else(|| state.records.get(&record_key(reference)))
        .ok_or_else(not_found)?;
    if &record.reference != reference {
        return Err(error(ErrorCode::RecordConflict, "record.reference"));
    }
    Ok(&record.value)
}

fn validate_snapshot_refs(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    let mut references = Vec::new();
    for receipt in &snapshot.resume_receipts {
        let command: ResumeCommand = event_record(state, additions, &receipt.command_ref)?;
        let outcome: crate::RunOutcome =
            event_record(state, additions, &receipt.previous_outcome_ref)?;
        let crate::OutcomeResult::Waiting { wait } = &outcome.result else {
            return Err(error(ErrorCode::InvalidSnapshot, "resume_receipts.outcome"));
        };
        outcome.validate()?;
        if command != receipt.command
            || outcome.checkpoint_revision != command.expected_revision
            || !action_matches_wait(wait, &command.action)
        {
            return Err(error(ErrorCode::InvalidSnapshot, "resume_receipts.records"));
        }
        for reference in &outcome.unresolved_effects {
            record_value(state, additions, reference)?;
        }
    }
    if let Some(reference) = &snapshot.routing_snapshot_ref {
        let value = record_value(state, additions, reference)?;
        let routing = crate::RoutingSnapshot::restore(
            &serde_json::to_string(value)
                .map_err(|_| error(ErrorCode::InvalidSnapshot, "routing"))?,
            &snapshot.scope,
            &reference.digest,
        )?;
        for invocation in &snapshot.model_ledger {
            routing.validate_route(&invocation.route)?;
            if invocation.inspection_ref.is_none()
                || !routing.policy().rules.iter().any(|rule| {
                    rule.model_binding == snapshot.profile.profile().model_binding
                        && rule.purpose == invocation.purpose
                        && (rule.primary == invocation.route.binding
                            || rule.fallbacks.contains(&invocation.route.binding))
                })
            {
                return Err(error(ErrorCode::InvalidSnapshot, "routing.invocation"));
            }
            let reference = invocation
                .inspection_ref
                .as_ref()
                .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "routing.inspection"))?;
            let observation: crate::ModelRouteObservation =
                serde_json::from_value(record_value(state, additions, reference)?.clone())
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "routing.inspection"))?;
            let require_pinned = invocation.route.version_semantics
                == crate::VersionSemantics::Pinned
                || routing.policy().rules.iter().any(|rule| {
                    rule.model_binding == snapshot.profile.profile().model_binding
                        && rule.purpose == invocation.purpose
                        && rule.version_policy == crate::VersionPolicy::RequirePinned
                });
            observation.validate(
                &invocation.route,
                if require_pinned {
                    crate::VersionPolicy::RequirePinned
                } else {
                    crate::VersionPolicy::AllowMutable
                },
            )?;
        }
    }
    let run_inputs = snapshot
        .system_inputs
        .as_ref()
        .map(|inputs| {
            crate::RunSystemInputs::from_value(
                record_value(state, additions, &inputs.snapshot_ref)?,
                inputs,
                &snapshot.scope,
            )
            .map_err(|_| error(ErrorCode::InvalidSnapshot, "system_inputs"))
        })
        .transpose()?;
    for invocation in &snapshot.model_ledger {
        if let Some(reference) = invocation
            .inspection_ref
            .as_ref()
            .filter(|_| snapshot.routing_snapshot_ref.is_none())
        {
            let observation: crate::ModelRouteObservation =
                serde_json::from_value(record_value(state, additions, reference)?.clone())
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "model.inspection"))?;
            observation.validate(&invocation.route, crate::VersionPolicy::AllowMutable)?;
        }
        if let Some(reference) = &invocation.response_ref {
            validate_model_response(state, additions, invocation, reference)?;
        }
    }
    references.extend(snapshot.assembly_ref.iter());
    references.extend(&snapshot.context_batches);
    references.extend(snapshot.source_states.iter().map(|s| &s.batch_ref));
    for entry in &snapshot.tool_ledger {
        if let Some(reference) = &entry.call.bound_input_ref {
            crate::input_binding::validate_bound_record(
                record_value(state, additions, reference)?,
                snapshot,
                &entry.call,
                run_inputs.as_ref(),
            )
            .map_err(|_| error(ErrorCode::InvalidSnapshot, "bound_input"))?;
        }
    }
    for entry in &snapshot.tool_ledger {
        if let ToolCallState::Settled { result } = &entry.state {
            references.extend(tool_result_refs(result));
        }
    }
    if let Some(wait) = &snapshot.wait {
        if let WaitTarget::Approval {
            target: ApprovalTarget::Candidate { candidate_ref, .. },
        } = &wait.target
        {
            references.push(candidate_ref);
        }
    }
    if let Some(outcome) = &snapshot.outcome {
        references.extend(&outcome.unresolved_effects);
        if let Some(verification) = &outcome.verification {
            references.extend(&verification.evidence);
        }
        if let OutcomeResult::Failed { failure } = &outcome.result {
            references.extend(failure.diagnostic_ref.iter());
        }
    }
    for reference in references {
        record_value(state, additions, reference)?;
    }
    Ok(())
}

fn validate_model_response(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    invocation: &ModelInvocationRecord,
    reference: &RecordRef,
) -> Result<(), ContractError> {
    let invalid = || error(ErrorCode::InvalidSnapshot, "model_ledger.response_ref");
    let saved: StoredModelResponse =
        serde_json::from_value(record_value(state, additions, reference)?.clone())
            .map_err(|_| invalid())?;
    let route_digest = invocation.route.digest();
    if saved.request_id != invocation.attempt_id || saved.route_digest != route_digest {
        return Err(invalid());
    }
    let metadata = match (&invocation.state, &saved.outcome) {
        (ModelAttemptState::Completed {}, ModelExchangeOutcome::Completed { response }) => {
            let mut call_ids = BTreeSet::new();
            if response.request_id != invocation.attempt_id
                || response.route_digest != route_digest
                || response
                    .continuation
                    .iter()
                    .any(|continuation| continuation.route_digest() != &route_digest)
                || response.finish == ModelFinish::Length
                || (response.finish == ModelFinish::ToolCalls) != !response.tool_calls.is_empty()
                || response
                    .tool_calls
                    .iter()
                    .any(|call| !call_ids.insert(&call.provider_call_id))
            {
                return Err(invalid());
            }
            &response.metadata
        }
        (ModelAttemptState::Failed { kind }, ModelExchangeOutcome::Failed { failure })
            if *kind == failure.kind =>
        {
            &failure.metadata
        }
        _ => return Err(invalid()),
    };
    if metadata.provider_request_id != invocation.provider_request_id
        || metadata.reported_model_id != invocation.reported_model_id
        || metadata.reported_model_version != invocation.reported_model_version
        || metadata.usage != invocation.usage
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_messages(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    run_id: &Id,
    last_sequence: u64,
    messages: &[Message],
) -> Result<u64, ContractError> {
    let mut sequence = last_sequence;
    let mut seen = BTreeSet::new();
    for message in messages {
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::InvalidMessage, "messages.sequence"))?;
        if &message.run_id != run_id
            || message.sequence.get() != sequence
            || state.message_ids.contains(&message.message_id)
            || !seen.insert(&message.message_id)
        {
            return Err(error(ErrorCode::InvalidMessage, "messages"));
        }
        for content in &message.content {
            if matches!(content, ContentBlock::ToolResultCorrection { .. }) {
                let mut history: Vec<_> = state
                    .sessions
                    .values()
                    .flat_map(|session| &session.messages)
                    .filter(|prior| prior.run_id == *run_id && prior.sequence <= message.sequence)
                    .cloned()
                    .collect();
                for addition in messages
                    .iter()
                    .filter(|addition| addition.sequence <= message.sequence)
                {
                    if !history
                        .iter()
                        .any(|prior| prior.message_id == addition.message_id)
                    {
                        history.push(addition.clone());
                    }
                }
                history.sort_by_key(|item| item.sequence);
                crate::message::tool_corrections(&history)?;
            }
            let references = match content {
                ContentBlock::Content { .. } => Vec::new(),
                ContentBlock::ToolCall { call } => call.bound_input_ref.iter().collect(),
                ContentBlock::ToolResult { result }
                | ContentBlock::ToolResultCorrection { result, .. } => tool_result_refs(result),
                ContentBlock::ProviderOpaque { data_ref, .. } => vec![data_ref],
            };
            for reference in references {
                record_value(state, additions, reference)?;
            }
        }
    }
    Ok(sequence)
}

fn validate_tool_pair(
    state: &ScopeState,
    snapshot: &RunSnapshot,
    additions: &[Message],
    result: &ToolResult,
) -> Result<(), ContractError> {
    let existing = state
        .sessions
        .get(&snapshot.request.session_id)
        .map_or(&[][..], |session| session.messages.as_slice());
    let entry = snapshot
        .tool_ledger
        .iter()
        .find(|entry| entry.call.call_id == result.call_id)
        .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.result_call"))?;
    let paired = existing.iter().chain(additions).any(|message| {
        message.message_id == result.call_message_id
            && message.run_id == snapshot.run_id
            && message.role == crate::MessageRole::Assistant
            && message.origin == crate::MessageOrigin::Model
            && message.content.iter().any(|content| {
                let ContentBlock::ToolCall { call } = content else {
                    return false;
                };
                let mut original = call.clone();
                if original.bound_input_ref.is_none() {
                    original.bound_input_ref = entry.call.bound_input_ref.clone();
                }
                original == entry.call
            })
    });
    if !paired {
        return Err(error(ErrorCode::InvalidSnapshot, "tool.result_message"));
    }
    Ok(())
}

fn tool_result_refs(result: &ToolResult) -> Vec<&RecordRef> {
    result
        .effect_receipt_ref
        .iter()
        .chain(
            result
                .error
                .iter()
                .flat_map(|error| error.diagnostic_ref.iter()),
        )
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn validate_events(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    previous_seq: u64,
    events: &[RunEvent],
    admission: bool,
    messages: &[Message],
) -> Result<(), ContractError> {
    let mut sequence = previous_seq;
    let mut seen = BTreeSet::new();
    let mut started = 0;
    let mut finished = 0;
    let mut resumed = 0;
    for message in messages {
        for content in &message.content {
            if let ContentBlock::ToolResultCorrection { result, .. } = content {
                let previous = state.runs.get(&snapshot.run_id).ok_or_else(not_found)?;
                if !matches!(previous.snapshot.wait.as_ref().map(|wait| &wait.target), Some(WaitTarget::External { call_id, .. }) if call_id == &result.call_id)
                    || !snapshot.resume_receipts.last().is_some_and(|receipt| {
                        receipt.accepted_revision == snapshot.revision
                            && matches!(receipt.command.action, ResumeAction::External { .. })
                    })
                    || !events.iter().any(|event| match &event.payload {
                        RunEventPayload::ToolSettled { result_ref } => {
                            event_record::<ToolResult>(state, additions, result_ref)
                                .is_ok_and(|saved| saved == *result)
                        }
                        _ => false,
                    })
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.tool_correction"));
                }
            }
        }
    }
    for event in events {
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::InvalidEvent, "events.seq"))?;
        if event.scope != snapshot.scope
            || event.run_id != snapshot.run_id
            || event.session_id != snapshot.request.session_id
            || event.seq.get() != sequence
            || state.event_ids.contains(&event.event_id)
            || !seen.insert(&event.event_id)
        {
            return Err(error(ErrorCode::InvalidEvent, "events"));
        }
        let reference = match &event.payload {
            RunEventPayload::RunStarted {
                request_ref,
                profile_digest,
            } => {
                started += 1;
                if !admission
                    || profile_digest != snapshot.profile.profile_digest()
                    || record_value(state, additions, request_ref)?
                        != &serde_json::to_value(&snapshot.request)
                            .map_err(|_| error(ErrorCode::InvalidContract, "request"))?
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_started"));
                }
                request_ref
            }
            RunEventPayload::RunFinished { outcome_ref } => {
                finished += 1;
                let outcome = snapshot
                    .outcome
                    .as_ref()
                    .filter(|_| snapshot.status.is_terminal())
                    .ok_or_else(|| error(ErrorCode::InvalidEvent, "events.run_finished"))?;
                if record_value(state, additions, outcome_ref)?
                    != &serde_json::to_value(outcome)
                        .map_err(|_| error(ErrorCode::InvalidContract, "outcome"))?
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_finished"));
                }
                outcome_ref
            }
            RunEventPayload::ToolPlanned { call_ref } => {
                let call: ToolCall = event_record(state, additions, call_ref)?;
                if !snapshot.tool_ledger.iter().any(|entry| entry.call == call) {
                    return Err(error(ErrorCode::InvalidEvent, "events.tool_planned"));
                }
                call_ref
            }
            RunEventPayload::ToolSettled { result_ref } => {
                let result: ToolResult = event_record(state, additions, result_ref)?;
                if !snapshot.tool_ledger.iter().any(|entry| {
                    matches!(
                        &entry.state, ToolCallState::Settled { result: saved } if *saved == result
                    )
                }) {
                    return Err(error(ErrorCode::InvalidEvent, "events.tool_settled"));
                }
                if state.runs.get(&snapshot.run_id).is_some_and(|previous| previous.snapshot.tool_ledger.iter()
                    .any(|entry| entry.call.call_id == result.call_id && matches!(entry.state, ToolCallState::Unknown { .. })))
                    && !messages.iter().flat_map(|message| &message.content)
                        .any(|content| matches!(content, ContentBlock::ToolResultCorrection { result: corrected, .. } if corrected == &result))
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.tool_correction_missing"));
                }
                validate_tool_pair(state, snapshot, messages, &result)?;
                result_ref
            }
            RunEventPayload::ToolUnresolved {
                result_ref,
                attempt_id,
                idempotency_key,
            } => {
                let result: ToolResult = event_record(state, additions, result_ref)?;
                if result.status != crate::ToolResultStatus::Unknown || result.effect != crate::ToolEffect::Unknown
                    || !snapshot.tool_ledger.iter().any(|entry| entry.call.call_id == result.call_id
                        && matches!(&entry.state, ToolCallState::Unknown { attempt_id: saved, idempotency_key: key }
                            if saved == attempt_id && key == idempotency_key))
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.tool_unresolved"));
                }
                validate_tool_pair(state, snapshot, messages, &result)?;
                for reference in tool_result_refs(&result) {
                    record_value(state, additions, reference)?;
                }
                result_ref
            }
            RunEventPayload::VerificationCompleted { verification_ref } => {
                // Additional verification history needs an explicit checkpoint contract.
                // A standalone event cannot substitute for the saved verification record.
                let verification: VerificationSummary =
                    event_record(state, additions, verification_ref)?;
                if snapshot
                    .outcome
                    .as_ref()
                    .and_then(|outcome| outcome.verification.as_ref())
                    != Some(&verification)
                {
                    return Err(error(
                        ErrorCode::InvalidEvent,
                        "events.verification_completed",
                    ));
                }
                verification_ref
            }
            RunEventPayload::RunWaiting { wait_ref } => {
                let wait: WaitState = event_record(state, additions, wait_ref)?;
                if snapshot.status != RunStatus::Waiting || snapshot.wait.as_ref() != Some(&wait) {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_waiting"));
                }
                wait_ref
            }
            RunEventPayload::RunResumed { command_ref } => {
                resumed += 1;
                let command: ResumeCommand = event_record(state, additions, command_ref)?;
                let previous = state.runs.get(&snapshot.run_id).ok_or_else(not_found)?;
                if snapshot.status != RunStatus::Running
                    || command.run_id != snapshot.run_id
                    || command.expected_revision != previous.snapshot.revision
                    || !resume_target_matches(&previous.snapshot, &command.action)
                    || !snapshot.resume_receipts.last().is_some_and(|receipt| {
                        receipt.command_ref == *command_ref
                            && receipt.command == command
                            && receipt.accepted_revision == snapshot.revision
                    })
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_resumed"));
                }
                let receipt = snapshot
                    .resume_receipts
                    .last()
                    .expect("receipt checked above");
                let prior: crate::RunOutcome =
                    event_record(state, additions, &receipt.previous_outcome_ref)?;
                if previous.snapshot.outcome.as_ref() != Some(&prior)
                    || event.timestamp_ms != snapshot.timing.last_observed_at_ms
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.resume_outcome"));
                }
                match &command.action {
                    ResumeAction::External { receipt_ref, .. } => {
                        record_value(state, additions, receipt_ref)?;
                    }
                    ResumeAction::Recover { recovery_ref } => {
                        record_value(state, additions, recovery_ref)?;
                    }
                    _ => {}
                }
                command_ref
            }
            RunEventPayload::ModelRouteSelected {
                invocation_ref,
                route_digest,
            } => {
                let invocation: ModelInvocationRecord =
                    event_record(state, additions, invocation_ref)?;
                if invocation.route.digest() != *route_digest
                    || !snapshot.model_ledger.contains(&invocation)
                {
                    return Err(error(
                        ErrorCode::InvalidEvent,
                        "events.model_route_selected",
                    ));
                }
                invocation_ref
            }
        };
        record_value(state, additions, reference)?;
    }
    if sequence != snapshot.last_event_seq
        || (admission && (started != 1 || events.len() != 1))
        || (snapshot.status.is_terminal() && finished != 1)
        || (!admission
            && resumed
                != snapshot.resume_receipts.len().saturating_sub(
                    state
                        .runs
                        .get(&snapshot.run_id)
                        .ok_or_else(not_found)?
                        .snapshot
                        .resume_receipts
                        .len(),
                ))
    {
        return Err(error(ErrorCode::InvalidEvent, "events"));
    }
    if !admission {
        let previous = &state
            .runs
            .get(&snapshot.run_id)
            .ok_or_else(not_found)?
            .snapshot;
        for old in &previous.tool_ledger {
            let Some(new) = snapshot
                .tool_ledger
                .iter()
                .find(|entry| entry.call.call_id == old.call.call_id)
            else {
                continue;
            };
            if !matches!(old.state, ToolCallState::Unknown { .. })
                || !matches!(new.state, ToolCallState::Settled { .. })
            {
                continue;
            }
            if !snapshot.resume_receipts.last().is_some_and(|receipt| {
                    receipt.accepted_revision == snapshot.revision
                        && matches!(receipt.command.action, ResumeAction::External { .. })
                        && matches!(previous.wait.as_ref().map(|wait| &wait.target), Some(WaitTarget::External { call_id, .. }) if call_id == &old.call.call_id)
                }) || !messages.iter().any(|message| matches!(message.content.as_slice(),
                    [ContentBlock::ToolResultCorrection { result, .. }] if result.call_id == old.call.call_id))
            { return Err(error(ErrorCode::InvalidEvent, "events.tool_correction_missing")); }
        }
        if snapshot
            .resume_receipts
            .last()
            .is_some_and(|receipt| receipt.accepted_revision == snapshot.revision)
        {
            let target = match &previous.wait.as_ref().ok_or_else(not_found)?.target {
                WaitTarget::Input { request } => Some((&request.call_id, Some(request))),
                WaitTarget::Approval {
                    target: ApprovalTarget::Tool { call_id, .. },
                } => Some((call_id, None)),
                _ => None,
            };
            if let Some((call_id, input)) = target {
                let old = previous
                    .tool_ledger
                    .iter()
                    .find(|entry| &entry.call.call_id == call_id)
                    .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "resume.call"))?;
                if input.is_some_and(|request| !matches!(&old.state, ToolCallState::InputPending { request: pending, .. } if pending == request))
                    || (input.is_none() && !matches!(old.state, ToolCallState::Planned {} | ToolCallState::ApprovalPending { .. }))
                { return Err(error(ErrorCode::InvalidTransition, "resume.call_state")); }
            }
        }
    }
    Ok(())
}

fn event_record<T: DeserializeOwned>(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    reference: &RecordRef,
) -> Result<T, ContractError> {
    serde_json::from_value(record_value(state, additions, reference)?.clone())
        .map_err(|_| error(ErrorCode::InvalidEvent, "events.record"))
}

/// Replay the causal facts shared by live commits and durable checkpoint restore.
/// A receipt does not by itself authorize rewriting a result: its preceding wait,
/// intervening settlement, and transcript observation must identify the same call.
fn validate_resume_history(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    events: &[&RunEvent],
    messages: &[&Message],
) -> Result<(), ContractError> {
    let invalid = || error(ErrorCode::InvalidSnapshot, "resume.history");
    let resumed: Vec<_> = events
        .iter()
        .copied()
        .filter(|event| matches!(event.payload, RunEventPayload::RunResumed { .. }))
        .collect();
    if resumed.len() != snapshot.resume_receipts.len() {
        return Err(invalid());
    }
    let mut previous_resume_seq = 0;
    let mut corrected_calls = BTreeSet::new();
    let mut authorized_corrections = BTreeSet::new();
    for (event, receipt) in resumed.into_iter().zip(&snapshot.resume_receipts) {
        let RunEventPayload::RunResumed { command_ref } = &event.payload else {
            unreachable!()
        };
        if command_ref != &receipt.command_ref
            || event.seq.get() <= receipt.previous_last_event_seq
            || receipt.previous_last_event_seq <= previous_resume_seq
            || event.timestamp_ms > snapshot.timing.last_observed_at_ms
        {
            return Err(invalid());
        }
        previous_resume_seq = event.seq.get();
        let prior: crate::RunOutcome =
            event_record(state, additions, &receipt.previous_outcome_ref)?;
        let OutcomeResult::Waiting { wait } = &prior.result else {
            return Err(invalid());
        };
        let waiting_event = events
            .iter()
            .find(|event| event.seq.get() == receipt.previous_last_event_seq)
            .ok_or_else(invalid)?;
        let RunEventPayload::RunWaiting { wait_ref } = &waiting_event.payload else {
            return Err(invalid());
        };
        let saved_wait: WaitState = event_record(state, additions, wait_ref)?;
        let expired = event.timestamp_ms >= snapshot.timing.deadline_at_ms
            || wait
                .expires_at_ms
                .is_some_and(|deadline| event.timestamp_ms >= deadline);
        if &saved_wait != wait
            || receipt.expired != expired
            || waiting_event.timestamp_ms > event.timestamp_ms
        {
            return Err(invalid());
        }
        let between: Vec<_> = events
            .iter()
            .copied()
            .filter(|candidate| {
                candidate.seq.get() > receipt.previous_last_event_seq && candidate.seq < event.seq
            })
            .collect();
        // Approval records permission only; execution belongs to the following
        // segment. Candidate verification has its separate runtime contract.
        if matches!(receipt.command.action, ResumeAction::Approve { .. })
            || matches!(
                wait.target,
                WaitTarget::Approval {
                    target: ApprovalTarget::Candidate { .. }
                }
            )
        {
            if !between.is_empty() {
                return Err(invalid());
            }
            if let WaitTarget::Approval {
                target:
                    ApprovalTarget::Tool {
                        call_id,
                        binding_digest,
                    },
            } = &wait.target
            {
                validate_resume_binding(state, additions, snapshot, call_id, binding_digest)?;
            }
            continue;
        }
        if receipt.expired && between.is_empty() {
            continue;
        }
        let [settled] = between.as_slice() else {
            return Err(invalid());
        };
        let RunEventPayload::ToolSettled { result_ref } = &settled.payload else {
            return Err(invalid());
        };
        let result: ToolResult = event_record(state, additions, result_ref)?;
        if !snapshot.tool_ledger.iter().any(|entry| {
            matches!(&entry.state,
            ToolCallState::Settled { result: current } if current == &result)
        }) {
            return Err(invalid());
        }
        match (&receipt.command.action, &wait.target) {
            (ResumeAction::Input { answer, .. }, WaitTarget::Input { request }) => {
                if result.call_id != request.call_id
                    || result.status != crate::ToolResultStatus::Succeeded
                    || result.effect != crate::ToolEffect::NotApplied
                    || result.content
                        != [crate::InputContent::Json {
                            value: answer.clone(),
                        }]
                    || result.error.is_some()
                    || result.effect_receipt_ref.is_some()
                {
                    return Err(invalid());
                }
                validate_resume_result_message(snapshot, messages, &result)?;
            }
            (
                ResumeAction::Deny { .. },
                WaitTarget::Approval {
                    target:
                        ApprovalTarget::Tool {
                            call_id,
                            binding_digest,
                        },
                },
            ) => {
                validate_resume_binding(state, additions, snapshot, call_id, binding_digest)?;
                if &result.call_id != call_id
                    || result.status != crate::ToolResultStatus::Denied
                    || result.effect != crate::ToolEffect::NotApplied
                    || !result.content.is_empty()
                    || result.effect_receipt_ref.is_some()
                {
                    return Err(invalid());
                }
                validate_resume_result_message(snapshot, messages, &result)?;
            }
            (
                ResumeAction::External { receipt_ref, .. },
                WaitTarget::External {
                    call_id,
                    effect_key,
                },
            ) => {
                record_value(state, additions, receipt_ref)?;
                if &result.call_id != call_id
                    || result.effect == crate::ToolEffect::Unknown
                    || result.status == crate::ToolResultStatus::Unknown
                {
                    return Err(invalid());
                }
                let unknown_event = events.iter().rev().find(|candidate| {
                    candidate.seq.get() < receipt.previous_last_event_seq
                        && matches!(&candidate.payload, RunEventPayload::ToolUnresolved { result_ref, idempotency_key, .. }
                            if idempotency_key == effect_key && event_record::<ToolResult>(state, additions, result_ref)
                                .is_ok_and(|unknown| &unknown.call_id == call_id))
                }).ok_or_else(invalid)?;
                let RunEventPayload::ToolUnresolved { result_ref, .. } = &unknown_event.payload
                else {
                    unreachable!()
                };
                let unknown: ToolResult = event_record(state, additions, result_ref)?;
                let digest =
                    canonical_digest(&serde_json::to_value(&unknown).map_err(|_| invalid())?);
                let matching: Vec<_> = messages.iter().filter(|message| {
                    message.run_id == snapshot.run_id && matches!(message.content.as_slice(),
                        [ContentBlock::ToolResultCorrection { previous_message_id, previous_result_digest, result: corrected }]
                        if corrected == &result && previous_result_digest == &digest
                            && messages.iter().any(|prior| prior.message_id == *previous_message_id
                                && prior.run_id == snapshot.run_id && matches!(prior.content.as_slice(),
                                    [ContentBlock::ToolResult { result: previous }] if previous == &unknown)))
                }).collect();
                let [correction] = matching.as_slice() else {
                    return Err(invalid());
                };
                if !authorized_corrections.insert(correction.message_id.clone())
                    || !corrected_calls.insert(call_id.clone())
                {
                    return Err(invalid());
                }
            }
            _ => return Err(invalid()),
        }
    }
    for message in messages
        .iter()
        .filter(|message| message.run_id == snapshot.run_id)
    {
        if message
            .content
            .iter()
            .any(|content| matches!(content, ContentBlock::ToolResultCorrection { .. }))
            && !authorized_corrections.contains(&message.message_id)
        {
            return Err(invalid());
        }
    }
    for event in events {
        if let RunEventPayload::ToolUnresolved { result_ref, .. } = &event.payload {
            let unknown: ToolResult = event_record(state, additions, result_ref)?;
            if snapshot.tool_ledger.iter().any(|entry| {
                entry.call.call_id == unknown.call_id
                    && matches!(entry.state, ToolCallState::Settled { .. })
            }) && !corrected_calls.contains(&unknown.call_id)
            {
                return Err(invalid());
            }
        }
    }
    Ok(())
}

fn validate_resume_binding(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    call_id: &Id,
    binding_digest: &crate::JsonDigest,
) -> Result<(), ContractError> {
    let invalid = || error(ErrorCode::InvalidSnapshot, "resume.binding");
    let reference = snapshot
        .tool_ledger
        .iter()
        .find(|entry| &entry.call.call_id == call_id)
        .and_then(|entry| entry.call.bound_input_ref.as_ref())
        .ok_or_else(invalid)?;
    // validate_snapshot_refs already validates this typed protected binding.
    if record_value(state, additions, reference)?.get("binding_digest")
        != Some(&serde_json::to_value(binding_digest).map_err(|_| invalid())?)
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_resume_result_message(
    snapshot: &RunSnapshot,
    messages: &[&Message],
    result: &ToolResult,
) -> Result<(), ContractError> {
    let count = messages.iter().filter(|message| message.run_id == snapshot.run_id
        && message.role == crate::MessageRole::Tool && message.origin == crate::MessageOrigin::Tool
        && matches!(message.content.as_slice(), [ContentBlock::ToolResult { result: saved }] if saved == result)).count();
    if count != 1 {
        return Err(error(ErrorCode::InvalidSnapshot, "resume.result_message"));
    }
    Ok(())
}

fn resume_target_matches(previous: &RunSnapshot, action: &ResumeAction) -> bool {
    if let ResumeAction::Recover { .. } = action {
        return previous.status == RunStatus::Running;
    }
    previous
        .wait
        .as_ref()
        .is_some_and(|wait| action_matches_wait(wait, action))
}

fn action_matches_wait(wait: &WaitState, action: &ResumeAction) -> bool {
    match action {
        ResumeAction::Recover { .. } => false,
        ResumeAction::Approve { wait_id, target }
        | ResumeAction::Deny {
            wait_id, target, ..
        } => {
            &wait.wait_id == wait_id
                && matches!(&wait.target, WaitTarget::Approval { target: saved } if saved == target)
        }
        ResumeAction::Input { wait_id, .. } => {
            &wait.wait_id == wait_id && matches!(wait.target, WaitTarget::Input { .. })
        }
        ResumeAction::External { wait_id, .. } => {
            &wait.wait_id == wait_id && matches!(wait.target, WaitTarget::External { .. })
        }
    }
}

fn validate_transition(previous: &RunSnapshot, next: &RunSnapshot) -> Result<(), ContractError> {
    if previous.status.is_terminal() {
        return Err(error(ErrorCode::InvalidTransition, "run.status"));
    }
    next.validate()?;
    crate::budget::validate_budget_transition(previous, next)?;
    if !next.resume_receipts.starts_with(&previous.resume_receipts)
        || next.resume_receipts.len() > previous.resume_receipts.len() + 1
    {
        return Err(error(ErrorCode::InvalidTransition, "resume_receipts"));
    }
    let resumed = next.resume_receipts.len() != previous.resume_receipts.len();
    if resumed {
        let receipt = next.resume_receipts.last().expect("new receipt");
        if previous.status != RunStatus::Waiting
            || next.status != RunStatus::Running
            || next.outcome.is_some()
            || next.wait.is_some()
            || receipt.accepted_revision != next.revision
            || receipt.command.expected_revision != previous.revision
            || receipt.previous_last_event_seq != previous.last_event_seq
            || !resume_target_matches(previous, &receipt.command.action)
        {
            return Err(error(
                ErrorCode::InvalidTransition,
                "resume_receipts.acceptance",
            ));
        }
    } else if previous.status == RunStatus::Waiting && next.status == RunStatus::Running {
        return Err(error(
            ErrorCode::InvalidTransition,
            "resume_receipts.missing",
        ));
    }
    if previous.run_id != next.run_id
        || previous.request != next.request
        || previous.request_digest != next.request_digest
        || previous.scope != next.scope
        || previous.system_inputs != next.system_inputs
        || previous.limits != next.limits
        || next.revision
            != previous
                .revision
                .checked_add(1)
                .ok_or_else(|| error(ErrorCode::RevisionConflict, "revision"))?
        || (previous.assembly_ref.is_some() && previous.assembly_ref != next.assembly_ref)
        || (previous.routing_snapshot_ref.is_some()
            && previous.routing_snapshot_ref != next.routing_snapshot_ref)
        || (previous.routing_snapshot_ref.is_none()
            && next.routing_snapshot_ref.is_some()
            && previous.usage.model_calls != 0)
    {
        return Err(error(
            ErrorCode::InvalidTransition,
            "snapshot.immutable_fields",
        ));
    }
    if previous.profile != next.profile {
        return Err(error(ErrorCode::ProfileMismatch, "snapshot.profile"));
    }
    if previous.tool_ledger.len() > next.tool_ledger.len() {
        return Err(error(ErrorCode::InvalidTransition, "tool_ledger"));
    }
    for (old, new) in previous.tool_ledger.iter().zip(&next.tool_ledger) {
        let mut call = old.call.clone();
        if call.bound_input_ref.is_none() {
            call.bound_input_ref = new.call.bound_input_ref.clone();
        }
        if call != new.call
            || (matches!(old.state, ToolCallState::Settled { .. }) && old != new)
            || (matches!(
                old.state,
                ToolCallState::Dispatching { .. }
                    | ToolCallState::Unknown { .. }
                    | ToolCallState::ApprovalPending { .. }
                    | ToolCallState::InputPending { .. }
            ) && matches!(new.state, ToolCallState::Planned { .. }))
            || (matches!(old.state, ToolCallState::Unknown { .. })
                && matches!(new.state, ToolCallState::ApprovalPending { .. }))
            || (matches!(old.state, ToolCallState::ApprovalPending { .. })
                && matches!(new.state, ToolCallState::Unknown { .. }))
            || (matches!(old.state, ToolCallState::InputPending { .. })
                && !matches!(
                    new.state,
                    ToolCallState::InputPending { .. } | ToolCallState::Settled { .. }
                ))
            || (matches!(new.state, ToolCallState::InputPending { .. })
                && !matches!(
                    old.state,
                    ToolCallState::Dispatching { .. } | ToolCallState::InputPending { .. }
                ))
        {
            return Err(error(ErrorCode::InvalidTransition, "tool_ledger"));
        }
        if let (
            ToolCallState::Dispatching {
                attempt_id: old_attempt,
                idempotency_key: old_key,
            }
            | ToolCallState::Unknown {
                attempt_id: old_attempt,
                idempotency_key: old_key,
            }
            | ToolCallState::ApprovalPending {
                attempt_id: old_attempt,
                idempotency_key: old_key,
            }
            | ToolCallState::InputPending {
                attempt_id: old_attempt,
                idempotency_key: old_key,
                ..
            },
            ToolCallState::Dispatching {
                attempt_id: new_attempt,
                idempotency_key: new_key,
            }
            | ToolCallState::Unknown {
                attempt_id: new_attempt,
                idempotency_key: new_key,
            }
            | ToolCallState::ApprovalPending {
                attempt_id: new_attempt,
                idempotency_key: new_key,
            }
            | ToolCallState::InputPending {
                attempt_id: new_attempt,
                idempotency_key: new_key,
                ..
            },
        ) = (&old.state, &new.state)
        {
            let retry = matches!(
                old.state,
                ToolCallState::Unknown { .. } | ToolCallState::ApprovalPending { .. }
            ) && matches!(new.state, ToolCallState::Dispatching { .. });
            if old_key != new_key || (!retry && old_attempt != new_attempt) {
                return Err(error(ErrorCode::InvalidTransition, "tool_ledger.attempt"));
            }
        }
        if let (
            ToolCallState::InputPending {
                request: before, ..
            },
            ToolCallState::InputPending { request: after, .. },
        ) = (&old.state, &new.state)
        {
            if before != after {
                return Err(error(
                    ErrorCode::InvalidTransition,
                    "tool_ledger.input_request",
                ));
            }
        }
    }
    if previous.model_ledger.len() > next.model_ledger.len()
        || previous
            .model_ledger
            .iter()
            .zip(&next.model_ledger)
            .any(|(old, new)| {
                old.run_id != new.run_id
                    || old.model_step_id != new.model_step_id
                    || old.attempt_id != new.attempt_id
                    || old.purpose != new.purpose
                    || old.route != new.route
                    || old.selection_reason != new.selection_reason
                    || old.request_digest != new.request_digest
                    || old.inspection_ref != new.inspection_ref
                    || (matches!(
                        old.state,
                        ModelAttemptState::Completed {} | ModelAttemptState::Failed { .. }
                    ) && old != new)
                    || (matches!(old.state, ModelAttemptState::Unknown {})
                        && matches!(new.state, ModelAttemptState::Reserved {}))
            })
    {
        return Err(error(ErrorCode::InvalidTransition, "model_ledger"));
    }
    let old = &previous.usage;
    let new = &next.usage;
    if new.model_calls < old.model_calls
        || new.tool_attempts < old.tool_attempts
        || new.repair_attempts < old.repair_attempts
        || new.recovery_attempts < old.recovery_attempts
        || new.elapsed_ms < old.elapsed_ms
    {
        return Err(error(ErrorCode::InvalidTransition, "usage"));
    }
    Ok(())
}
```

## `crates/wickle/src/state/checkpoint.rs`

```rust
use super::*;
use crate::{JsonDigest, RunOutcome, RunRequest, serialization::data_digest};
use serde::{Deserialize, Serialize, Serializer};

/// Version of the protected, scope-local memory-store checkpoint format.
pub const STATE_STORE_CHECKPOINT_VERSION: &str = "wickle.state-store.v1";

/// An owned, validated scope graph. Explicit serialization contains protected
/// transcript and input data and is intended only for authorized storage adapters.
/// No caller can mutate its state or deserialize it without full validation.
#[derive(Clone)]
pub struct StateStoreCheckpoint {
    scope: Scope,
    state: ScopeState,
}

impl fmt::Debug for StateStoreCheckpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateStoreCheckpoint")
            .field("session_count", &self.state.sessions.len())
            .field("run_count", &self.state.runs.len())
            .field("record_count", &self.state.records.len())
            .finish_non_exhaustive()
    }
}

#[derive(Serialize)]
struct CheckpointView<'a> {
    schema_version: &'static str,
    scope: &'a Scope,
    sessions: Vec<SessionView<'a>>,
    runs: Vec<RunView<'a>>,
    records: Vec<RecordView<'a>>,
}
#[derive(Serialize)]
struct SessionView<'a> {
    snapshot: &'a SessionSnapshot,
    messages: &'a [Message],
}
#[derive(Serialize)]
struct RunView<'a> {
    snapshot: &'a RunSnapshot,
    events: &'a [RunEvent],
    lease: Option<LeaseData>,
    last_fencing_token: u64,
}
#[derive(Serialize)]
struct RecordView<'a> {
    reference: &'a RecordRef,
    value: &'a Value,
}

impl Serialize for StateStoreCheckpoint {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        CheckpointView {
            schema_version: STATE_STORE_CHECKPOINT_VERSION,
            scope: &self.scope,
            sessions: self
                .state
                .sessions
                .values()
                .map(|session| SessionView {
                    snapshot: &session.snapshot,
                    messages: &session.messages,
                })
                .collect(),
            runs: self
                .state
                .runs
                .values()
                .map(|run| RunView {
                    snapshot: &run.snapshot,
                    events: &run.events,
                    lease: run.lease.as_ref().map(LeaseData::from),
                    last_fencing_token: run.last_fencing_token,
                })
                .collect(),
            records: self
                .state
                .records
                .values()
                .map(|record| RecordView {
                    reference: record.reference(),
                    value: record.value(),
                })
                .collect(),
        }
        .serialize(serializer)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointData {
    schema_version: String,
    scope: Scope,
    sessions: Vec<SessionData>,
    runs: Vec<RunData>,
    records: Vec<RecordData>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionData {
    snapshot: SessionSnapshot,
    messages: Vec<Message>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunData {
    snapshot: RunSnapshot,
    events: Vec<RunEvent>,
    lease: Option<LeaseData>,
    last_fencing_token: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordData {
    reference: RecordRef,
    value: Value,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaseData {
    scope: Scope,
    run_id: Id,
    owner: Id,
    fencing_token: u64,
    expires_at_ms: i64,
}
impl From<&RunLease> for LeaseData {
    fn from(lease: &RunLease) -> Self {
        Self {
            scope: lease.scope.clone(),
            run_id: lease.run_id.clone(),
            owner: lease.owner.clone(),
            fencing_token: lease.fencing_token,
            expires_at_ms: lease.expires_at_ms,
        }
    }
}
impl From<LeaseData> for RunLease {
    fn from(lease: LeaseData) -> Self {
        Self {
            scope: lease.scope,
            run_id: lease.run_id,
            owner: lease.owner,
            fencing_token: lease.fencing_token,
            expires_at_ms: lease.expires_at_ms,
        }
    }
}

impl StateStoreCheckpoint {
    /// Exact namespace covered by the protected checkpoint.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Canonical identity of the serialized scope graph, excluding derived indexes.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }
    /// Parse a known version and validate scope, trusted digest, current state,
    /// historical typed records and derived indexes. Collection order is the stable
    /// key order produced by export; malformed or noncanonical images are rejected.
    pub fn from_json(
        input: &str,
        scope: &Scope,
        expected_digest: &JsonDigest,
    ) -> Result<Self, ContractError> {
        let value = crate::parse_json(input)?;
        if value.get("schema_version").and_then(Value::as_str)
            != Some(STATE_STORE_CHECKPOINT_VERSION)
        {
            return Err(error(
                ErrorCode::UnsupportedSchemaVersion,
                "checkpoint.schema_version",
            ));
        }
        if canonical_digest(&value) != *expected_digest {
            return Err(invalid("checkpoint.digest"));
        }
        let data: CheckpointData =
            serde_json::from_value(value).map_err(|_| invalid("checkpoint"))?;
        if &data.scope != scope {
            return Err(error(ErrorCode::AccessDenied, "checkpoint.scope"));
        }
        let checkpoint = restore_graph(data)?;
        if checkpoint.digest() != *expected_digest {
            return Err(invalid("checkpoint.canonical_form"));
        }
        Ok(checkpoint)
    }
}

impl MemoryStateStore {
    /// Copy only the requested namespace without performing I/O or exposing live
    /// mutable references. Unknown namespaces return StateNotFound.
    pub fn export_checkpoint(&self, scope: &Scope) -> Result<StateStoreCheckpoint, ContractError> {
        let scopes = self.lock()?;
        Ok(StateStoreCheckpoint {
            scope: scope.clone(),
            state: namespace(&scopes, scope)?.clone(),
        })
    }
    /// Move an already validated private checkpoint into a new process-local store.
    /// This does not perform a second graph validation or claim durable capabilities.
    pub fn from_checkpoint(checkpoint: StateStoreCheckpoint) -> Self {
        Self {
            scopes: Mutex::new(BTreeMap::from([(
                scope_key(&checkpoint.scope),
                checkpoint.state,
            )])),
        }
    }
}

fn restore_graph(data: CheckpointData) -> Result<StateStoreCheckpoint, ContractError> {
    if data.schema_version != STATE_STORE_CHECKPOINT_VERSION {
        return Err(invalid("checkpoint.schema_version"));
    }
    let mut state = ScopeState::default();
    for record in data.records {
        if canonical_digest(&record.value) != record.reference.digest {
            return Err(invalid("checkpoint.record_digest"));
        }
        let key = record_key(&record.reference);
        if state
            .records
            .insert(
                key,
                ProtectedRecord {
                    reference: record.reference,
                    value: record.value,
                },
            )
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_record"));
        }
    }
    for session in data.sessions {
        if session.snapshot.scope != data.scope {
            return Err(invalid("checkpoint.session_scope"));
        }
        if state
            .sessions
            .insert(
                session.snapshot.session_id.clone(),
                SessionState {
                    snapshot: session.snapshot,
                    messages: session.messages,
                },
            )
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_session"));
        }
    }
    for run in data.runs {
        if run.snapshot.scope != data.scope {
            return Err(invalid("checkpoint.run_scope"));
        }
        run.snapshot.validate()?;
        let session = state
            .sessions
            .get(&run.snapshot.request.session_id)
            .ok_or_else(|| invalid("checkpoint.run_session"))?;
        if session.snapshot.profile_digest != *run.snapshot.profile.profile_digest() {
            return Err(invalid("checkpoint.session_profile"));
        }
        if run.snapshot.revision > 0 && run.last_fencing_token == 0 {
            return Err(invalid("checkpoint.fencing_generation"));
        }
        if let Some(lease) = &run.lease {
            if lease.scope != data.scope
                || lease.run_id != run.snapshot.run_id
                || lease.fencing_token == 0
                || lease.fencing_token != run.last_fencing_token
                || run.snapshot.status.is_terminal()
            {
                return Err(invalid("checkpoint.lease"));
            }
        }
        if run.snapshot.revision == 0
            && (run.snapshot.status != RunStatus::Running
                || run.snapshot.phase != RunPhase::Admission
                || run.snapshot.usage != BudgetUsage::default()
                || !run.snapshot.reservations.is_empty()
                || !run.snapshot.model_ledger.is_empty()
                || !run.snapshot.tool_ledger.is_empty()
                || run.events.len() != 1)
        {
            return Err(invalid("checkpoint.admission"));
        }
        let request = (
            run.snapshot.request.session_id.clone(),
            run.snapshot.request.request_id.clone(),
        );
        if state
            .requests
            .insert(request, run.snapshot.run_id.clone())
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_request"));
        }
        let run_id = run.snapshot.run_id.clone();
        if state
            .runs
            .insert(
                run_id,
                RunState {
                    snapshot: run.snapshot,
                    events: run.events,
                    lease: run.lease.map(Into::into),
                    last_fencing_token: run.last_fencing_token,
                },
            )
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_run"));
        }
    }
    let empty = BTreeMap::new();
    let mut message_ids = BTreeSet::new();
    for session in state.sessions.values() {
        record_value(&state, &empty, &session.snapshot.prompt_snapshot)?;
        let active: Vec<_> = state
            .runs
            .values()
            .filter(|run| {
                run.snapshot.request.session_id == session.snapshot.session_id
                    && !run.snapshot.status.is_terminal()
            })
            .collect();
        if active.len() > 1
            || active.first().map(|run| &run.snapshot.run_id)
                != session.snapshot.active_run_id.as_ref()
        {
            return Err(invalid("checkpoint.active_run"));
        }
        if !state
            .runs
            .values()
            .any(|run| run.snapshot.request.session_id == session.snapshot.session_id)
        {
            return Err(invalid("checkpoint.orphan_session"));
        }
        let mut sequence = 0;
        let mut seen_runs = BTreeSet::new();
        let mut previous_run = None;
        for message in &session.messages {
            let run = state
                .runs
                .get(&message.run_id)
                .ok_or_else(|| invalid("checkpoint.message_run"))?;
            if run.snapshot.request.session_id != session.snapshot.session_id
                || !message_ids.insert(message.message_id.clone())
            {
                return Err(invalid("checkpoint.message_identity"));
            }
            if previous_run != Some(&message.run_id) {
                if !seen_runs.insert(&message.run_id) {
                    return Err(invalid("checkpoint.message_run_order"));
                }
                previous_run = Some(&message.run_id);
            }
            sequence = validate_messages(
                &state,
                &empty,
                &message.run_id,
                sequence,
                std::slice::from_ref(message),
            )?;
        }
        if sequence != session.snapshot.transcript_revision {
            return Err(invalid("checkpoint.transcript_revision"));
        }
        if let Some(active_run) = &session.snapshot.active_run_id {
            if seen_runs.contains(active_run) && previous_run != Some(active_run) {
                return Err(invalid("checkpoint.active_run_order"));
            }
        }
    }
    let mut event_ids = BTreeSet::new();
    for run in state.runs.values() {
        validate_snapshot_refs(&state, &empty, &run.snapshot)?;
        validate_history(&state, run, &mut event_ids)?;
    }
    state.message_ids = message_ids;
    state.event_ids = event_ids;
    Ok(StateStoreCheckpoint {
        scope: data.scope,
        state,
    })
}

fn validate_history(
    state: &ScopeState,
    run: &RunState,
    event_ids: &mut BTreeSet<Id>,
) -> Result<(), ContractError> {
    let empty = BTreeMap::new();
    let mut sequence = 0_u64;
    let mut started = 0;
    let mut finished = 0;
    let mut resumed = 0;
    let mut unresolved_keys = BTreeMap::new();
    for event in &run.events {
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| invalid("checkpoint.event_sequence"))?;
        if event.scope != run.snapshot.scope
            || event.run_id != run.snapshot.run_id
            || event.session_id != run.snapshot.request.session_id
            || event.seq.get() != sequence
            || !event_ids.insert(event.event_id.clone())
        {
            return Err(invalid("checkpoint.event_identity"));
        }
        match &event.payload {
            RunEventPayload::RunStarted {
                request_ref,
                profile_digest,
            } => {
                started += 1;
                let request: RunRequest = event_record(state, &empty, request_ref)?;
                if sequence != 1
                    || request != run.snapshot.request
                    || profile_digest != run.snapshot.profile.profile_digest()
                {
                    return Err(invalid("checkpoint.run_started"));
                }
            }
            RunEventPayload::RunFinished { outcome_ref } => {
                finished += 1;
                let outcome: RunOutcome = event_record(state, &empty, outcome_ref)?;
                if !run.snapshot.status.is_terminal()
                    || run.snapshot.outcome.as_ref() != Some(&outcome)
                {
                    return Err(invalid("checkpoint.run_finished"));
                }
            }
            RunEventPayload::ToolPlanned { call_ref } => {
                let mut call: ToolCall = event_record(state, &empty, call_ref)?;
                let current = run
                    .snapshot
                    .tool_ledger
                    .iter()
                    .find(|entry| entry.call.call_id == call.call_id)
                    .ok_or_else(|| invalid("checkpoint.tool_planned"))?;
                if call.bound_input_ref.is_none() {
                    call.bound_input_ref = current.call.bound_input_ref.clone();
                }
                if call != current.call {
                    return Err(invalid("checkpoint.tool_planned"));
                }
            }
            RunEventPayload::ToolSettled { result_ref } => {
                let result: ToolResult = event_record(state, &empty, result_ref)?;
                if !run.snapshot.tool_ledger.iter().any(|entry| matches!(&entry.state, ToolCallState::Settled { result: current } if current == &result)) {
                    return Err(invalid("checkpoint.tool_settled"));
                }
                validate_tool_pair(state, &run.snapshot, &[], &result)?;
                for reference in tool_result_refs(&result) {
                    record_value(state, &empty, reference)?;
                }
            }
            RunEventPayload::ToolUnresolved {
                result_ref,
                attempt_id,
                idempotency_key,
            } => {
                let result: ToolResult = event_record(state, &empty, result_ref)?;
                if result.status != crate::ToolResultStatus::Unknown || result.effect != crate::ToolEffect::Unknown
                    || !run.snapshot.tool_ledger.iter().any(|entry| entry.call.call_id == result.call_id)
                    || !run.snapshot.reservations.iter().any(|reservation| &reservation.attempt_id == attempt_id
                        && matches!(&reservation.kind, crate::ReservationKind::Tool { call_id } if call_id == &result.call_id))
                {
                    return Err(invalid("checkpoint.tool_unresolved"));
                }
                let entry = run
                    .snapshot
                    .tool_ledger
                    .iter()
                    .find(|entry| entry.call.call_id == result.call_id)
                    .expect("call membership checked above");
                let current_key = match &entry.state {
                    ToolCallState::Dispatching {
                        idempotency_key, ..
                    }
                    | ToolCallState::ApprovalPending {
                        idempotency_key, ..
                    }
                    | ToolCallState::Unknown {
                        idempotency_key, ..
                    } => Some(idempotency_key),
                    _ => None,
                };
                if current_key.is_some_and(|key| key != idempotency_key)
                    || unresolved_keys
                        .insert(result.call_id.clone(), idempotency_key)
                        .is_some_and(|key| key != idempotency_key)
                {
                    return Err(invalid("checkpoint.tool_unresolved_key"));
                }
                validate_tool_pair(state, &run.snapshot, &[], &result)?;
                for reference in tool_result_refs(&result) {
                    record_value(state, &empty, reference)?;
                }
            }
            RunEventPayload::VerificationCompleted { verification_ref } => {
                let verification: VerificationSummary =
                    event_record(state, &empty, verification_ref)?;
                for reference in &verification.evidence {
                    record_value(state, &empty, reference)?;
                }
            }
            RunEventPayload::RunWaiting { wait_ref } => {
                let wait: WaitState = event_record(state, &empty, wait_ref)?;
                if let WaitTarget::Approval {
                    target: ApprovalTarget::Candidate { candidate_ref, .. },
                } = &wait.target
                {
                    record_value(state, &empty, candidate_ref)?;
                }
            }
            RunEventPayload::RunResumed { command_ref } => {
                resumed += 1;
                let command: ResumeCommand = event_record(state, &empty, command_ref)?;
                if command.run_id != run.snapshot.run_id
                    || command.expected_revision >= run.snapshot.revision
                    || !run.snapshot.resume_receipts.iter().any(|receipt| {
                        receipt.command_ref == *command_ref
                            && receipt.command == command
                            && event.seq.get() > receipt.previous_last_event_seq
                    })
                {
                    return Err(invalid("checkpoint.run_resumed"));
                }
                let reference = match &command.action {
                    ResumeAction::External { receipt_ref, .. } => Some(receipt_ref),
                    ResumeAction::Recover { recovery_ref } => Some(recovery_ref),
                    ResumeAction::Approve {
                        target: ApprovalTarget::Candidate { candidate_ref, .. },
                        ..
                    }
                    | ResumeAction::Deny {
                        target: ApprovalTarget::Candidate { candidate_ref, .. },
                        ..
                    } => Some(candidate_ref),
                    _ => None,
                };
                if let Some(reference) = reference {
                    record_value(state, &empty, reference)?;
                }
            }
            RunEventPayload::ModelRouteSelected {
                invocation_ref,
                route_digest,
            } => {
                let old: ModelInvocationRecord = event_record(state, &empty, invocation_ref)?;
                let current = run
                    .snapshot
                    .model_ledger
                    .iter()
                    .find(|current| current.attempt_id == old.attempt_id)
                    .ok_or_else(|| invalid("checkpoint.model_route"))?;
                if old.run_id != current.run_id
                    || old.model_step_id != current.model_step_id
                    || old.purpose != current.purpose
                    || old.route != current.route
                    || old.selection_reason != current.selection_reason
                    || old.request_digest != current.request_digest
                    || old.inspection_ref != current.inspection_ref
                    || old.route.digest() != *route_digest
                    || (matches!(
                        old.state,
                        ModelAttemptState::Completed {} | ModelAttemptState::Failed { .. }
                    ) && &old != current)
                    || (matches!(old.state, ModelAttemptState::Unknown {})
                        && matches!(current.state, ModelAttemptState::Reserved {}))
                {
                    return Err(invalid("checkpoint.model_route"));
                }
                if let Some(reference) = &old.response_ref {
                    validate_model_response(state, &empty, &old, reference)?;
                }
            }
        }
    }
    if started != 1
        || sequence != run.snapshot.last_event_seq
        || finished != usize::from(run.snapshot.status.is_terminal())
        || resumed != run.snapshot.resume_receipts.len()
    {
        return Err(invalid("checkpoint.events"));
    }
    let events: Vec<_> = run.events.iter().collect();
    let messages: Vec<_> = state
        .sessions
        .get(&run.snapshot.request.session_id)
        .ok_or_else(|| invalid("checkpoint.session"))?
        .messages
        .iter()
        .collect();
    validate_resume_history(state, &empty, &run.snapshot, &events, &messages)?;
    Ok(())
}

fn invalid(path: &str) -> ContractError {
    error(ErrorCode::InvalidSnapshot, path)
}
```

## `crates/wickle/src/tool_execution.rs`

```rust
use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, fmt, sync::Arc};
use tokio_util::sync::CancellationToken;

mod resume;
mod round;

/// Identity and controls for one physical tool call. Credentials and unrelated
/// system inputs remain in the executor's Host-owned binding.
#[derive(Debug, Clone)]
pub struct ToolExecutionContext {
    /// Logical call whose plan and bound input were already saved.
    pub call_id: Id,
    /// Charged physical attempt, already recorded before execution.
    pub attempt_id: Id,
    /// Stable external deduplication identity across recovery of this call.
    pub idempotency_key: Id,
    /// Exact authorized namespace.
    pub scope: Scope,
    /// Current authenticated actor.
    pub principal_ref: Id,
    /// Current Host authorization grant.
    pub capability_grant_ref: Id,
    /// Cancelled when the attempt stops, including timeout or caller cancellation.
    pub cancellation: CancellationToken,
    /// Finite execution deadline.
    pub deadline: tokio::time::Instant,
}

/// Effect information attested by the trusted executor, independent of output validation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolEffect {
    /// The executor confirms no external business write occurred.
    NotApplied,
    /// An external business write is confirmed; its receipt must be retained.
    Applied,
    /// Whether an external business write occurred could not be established.
    #[default]
    Unknown,
}

/// A handler's safe result; it cannot replace core call identities or ledger state.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolExecutionOutcome {
    /// Returned value to check against the pinned output schema.
    Succeeded {
        /// Raw returned JSON; only a validated, bounded value becomes model content.
        value: Value,
    },
    /// Classified handler failure, independent of whether a write happened.
    Failed {
        /// Safe registered failure code, without SDK error messages or payloads.
        code: Id,
    },
    /// Ask the Host for the value that will complete this call, without rerunning
    /// the executor. Requires NotApplied and no receipt. The pinned output schema
    /// validates the answer; this does not suspend and resume handler code.
    InputRequired {
        /// Bounded question displayed to the authorized caller.
        question: String,
    },
}

/// Explicit completion and effect receipt. Serialize only for protected storage.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolExecutionResult {
    /// Returned value or safe failure classification.
    pub outcome: ToolExecutionOutcome,
    /// Observed external effect status.
    pub effect: ToolEffect,
    /// Original effect receipt, required for a confirmed Applied result.
    pub receipt: Option<Value>,
}
impl fmt::Debug for ToolExecutionOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Succeeded { .. } => "ToolExecutionOutcome::Succeeded(<protected>)",
            Self::Failed { .. } => "ToolExecutionOutcome::Failed(<classified>)",
            Self::InputRequired { .. } => "ToolExecutionOutcome::InputRequired(<protected>)",
        })
    }
}
impl fmt::Debug for ToolExecutionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolExecutionResult")
            .field("effect", &self.effect)
            .field("has_receipt", &self.receipt.is_some())
            .finish_non_exhaustive()
    }
}

/// Exactly one physical execution. Implementations must not hide retry loops or
/// spawn untracked operations; effect uncertainty must be reported honestly.
pub trait ToolExecutor: Send + Sync {
    /// Execute only the final policy-approved arguments, not the original model
    /// map, full system-input snapshot, or caller-supplied tool identities.
    fn execute<'a>(
        &'a self,
        execution_args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult>;
}

/// Exact saved effect and protected evidence presented to a trusted Host verifier.
/// The Host must authenticate the receipt and its ownership, not merely compare
/// caller-supplied IDs. This value must not be sent to a model or ordinary logs.
#[derive(Clone)]
pub struct ExternalReceiptRequest {
    /// Original saved logical call, including its immutable input reference.
    pub call: ToolCall,
    /// Original uncertain physical attempt; no new execution is requested.
    pub attempt_id: Id,
    /// Original external deduplication identity.
    pub idempotency_key: Id,
    /// Restored, policy-authorized final arguments for that same call.
    pub bound_input: BoundToolInput,
    /// Exact record authorized and retrieved by the Agent before verification.
    pub receipt_ref: RecordRef,
    /// Protected record contents, not an untrusted substitute for the reference.
    pub receipt: Value,
}
impl fmt::Debug for ExternalReceiptRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ExternalReceiptRequest(<protected>)")
    }
}

/// Current authorization and finite controls for a read-only receipt inspection.
#[derive(Debug, Clone)]
pub struct ExternalReceiptContext {
    /// Exact namespace of the waiting run and receipt.
    pub scope: Scope,
    /// Current authenticated actor.
    pub principal_ref: Id,
    /// Current Host authorization grant.
    pub capability_grant_ref: Id,
    /// Cancelled when verification stops or times out.
    pub cancellation: CancellationToken,
    /// Finite callback deadline.
    pub deadline: tokio::time::Instant,
}

/// Read-only Host attestation of a previously uncertain effect. It must not
/// execute or retry the business operation. Unknown leaves the wait unresolved.
pub trait ExternalReceiptVerifier: Send + Sync {
    /// Return an authenticated result for the original call and frozen target.
    fn verify<'a>(
        &'a self,
        request: &'a ExternalReceiptRequest,
        context: &'a ExternalReceiptContext,
    ) -> PortFuture<'a, ToolExecutionResult>;
}

/// Prepared settlement for one authorized resume command. No state is committed
/// here: the Agent saves this alongside command consumption and RunResumed in one
/// transaction. Protected values deliberately have no ordinary Debug output.
pub struct PreparedToolResolution {
    /// Final observation for the original logical call.
    pub result: ToolResult,
    /// Replacement ledger state for that call.
    pub state: ToolCallState,
    /// Paired result, or explicit correction of the old Unknown observation.
    pub message: Message,
    /// Immutable result and diagnostic/effect records needed by the settlement.
    pub records: Vec<ProtectedRecord>,
    /// Next ToolSettled event; the Agent sequences RunResumed after it.
    pub event: RunEvent,
}
impl fmt::Debug for PreparedToolResolution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PreparedToolResolution(<protected>)")
    }
}

struct AttemptIdentity<'a> {
    scope: &'a Scope,
    attempt_id: &'a Id,
    idempotency_key: &'a Id,
}

/// A trusted Host associates one compiled contract with an existing executor.
/// Factory-level code/manifest attestation is separate from this registration.
#[derive(Clone)]
pub struct ToolRegistration {
    /// Exact descriptor and model-input projection.
    pub compiled: CompiledTool,
    /// Existing scoped executor; construction and credentials remain in Host code.
    pub executor: Arc<dyn ToolExecutor>,
}
impl fmt::Debug for ToolRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolRegistration")
            .field("compiled", &self.compiled)
            .finish_non_exhaustive()
    }
}

/// Scope-bound, immutable mapping of exact tool contracts to existing executors.
#[derive(Debug)]
pub struct ToolRegistry {
    scope: Scope,
    entries: BTreeMap<Id, ToolRegistration>,
}
impl ToolRegistry {
    /// Register without invoking handlers; duplicate names and exact tool identities fail.
    pub fn new(scope: Scope, entries: Vec<ToolRegistration>) -> Result<Self, ContractError> {
        let mut registered = BTreeMap::new();
        for entry in entries {
            if registered.values().any(|prior: &ToolRegistration| {
                prior.compiled.descriptor().tool == entry.compiled.descriptor().tool
            }) || registered
                .insert(entry.compiled.descriptor().name.clone(), entry)
                .is_some()
            {
                return Err(error(
                    ErrorCode::InvalidToolInputContract,
                    "tools.duplicate",
                ));
            }
        }
        Ok(Self {
            scope,
            entries: registered,
        })
    }
    /// Exact namespace under which handlers were registered.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Inspect an exact portable name without executing it or resolving an alias.
    pub fn get(&self, name: &Id) -> Option<&ToolRegistration> {
        self.entries.get(name)
    }
    /// Return only profile-selected contracts in profile order. Adapter exports
    /// require their separate runtime factory and are not implicitly opened here.
    pub fn prompt_bindings(
        &self,
        profile: &AgentProfile,
    ) -> Result<Vec<PromptToolBinding>, ContractError> {
        profile
            .tools
            .iter()
            .map(|selection| {
                let ToolBindingRef::Catalog(reference) = selection else {
                    return Err(error(ErrorCode::CapabilityUnsupported, "tools.export"));
                };
                let entry = self
                    .entries
                    .values()
                    .find(|entry| {
                        entry.compiled.descriptor().tool.id == reference.tool_id
                            && entry.compiled.descriptor().tool.version == reference.version
                    })
                    .ok_or_else(|| error(ErrorCode::ComponentUnavailable, "tools.selection"))?;
                Ok(PromptToolBinding {
                    selection: selection.clone(),
                    compiled: entry.compiled.clone(),
                })
            })
            .collect()
    }
}

/// Per-attempt bounds. The Run still owns total attempts, recovery and elapsed time.
#[derive(Debug, Clone, Copy)]
pub struct ToolExecutionLimits {
    /// Maximum elapsed time for one executor callback.
    pub timeout_ms: u64,
    /// Maximum raw effect-receipt size accepted from a handler.
    pub max_receipt_bytes: usize,
}
impl Default for ToolExecutionLimits {
    fn default() -> Self {
        Self {
            timeout_ms: 30_000,
            max_receipt_bytes: 65_536,
        }
    }
}

/// Whether the complete saved round is safe to follow with another model step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolRoundOutcome {
    /// Every planned call has a settled result and no unknown external effect remains.
    Completed,
    /// A fixed bound candidate requires the separate approval runtime.
    ApprovalRequired {
        /// Call whose immutable candidate was saved.
        call_id: Id,
        /// Current safe policy reason.
        reason: Id,
        /// Exact saved candidate; approval cannot rebind its system inputs.
        bound_input_ref: RecordRef,
        /// Identity of the final model-and-system argument binding.
        binding_digest: JsonDigest,
    },
    /// A no-effect input tool has saved its question and original attempt.
    InputRequired {
        /// Stable request answered through an authorized resume command.
        request: InputRequest,
    },
    /// A prior or current attempt requires explicit effect reconciliation.
    Unresolved {
        /// Call that prevents further tool and model dispatch.
        call_id: Id,
        /// Protected uncertainty observation committed with the matching event.
        result_ref: RecordRef,
    },
}

/// Serial execution of a previously committed model tool round.
pub struct SerialToolRound {
    registry: Arc<ToolRegistry>,
    binder: Arc<InputBinder>,
    policy: Arc<PolicyGate>,
    ids: Arc<dyn IdSource>,
    limits: ToolExecutionLimits,
}
impl SerialToolRound {
    /// Inject existing bindings; no tool is run or looked up externally here.
    pub fn new(
        registry: Arc<ToolRegistry>,
        binder: Arc<InputBinder>,
        policy: Arc<PolicyGate>,
        ids: Arc<dyn IdSource>,
    ) -> Self {
        Self {
            registry,
            binder,
            policy,
            ids,
            limits: ToolExecutionLimits::default(),
        }
    }
    /// Require finite nonzero timeout and receipt limits.
    pub fn with_limits(mut self, limits: ToolExecutionLimits) -> Result<Self, ContractError> {
        if limits.timeout_ms == 0 || limits.timeout_ms > 86_400_000 || limits.max_receipt_bytes == 0
        {
            return Err(error(ErrorCode::InvalidConfiguration, "tools.limits"));
        }
        self.limits = limits;
        Ok(self)
    }
}
fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
```

## `crates/wickle/src/tool_execution/resume.rs`

```rust
use super::*;

impl SerialToolRound {
    /// Prepare a denied observation for an exact saved approval candidate. The
    /// Agent must authorize and atomically consume the matching Deny command.
    pub fn prepare_denial(
        &self,
        saved: &StoredRun,
        call_id: &Id,
        bound: &BoundToolInput,
        code: Id,
        now_ms: i64,
    ) -> Result<PreparedToolResolution, ContractError> {
        let (entry, _) = self.resolution_call(saved, call_id, bound)?;
        if !matches!(
            entry.state,
            ToolCallState::Planned {} | ToolCallState::ApprovalPending { .. }
        ) || !matches!(saved.snapshot.wait.as_ref().map(|wait| &wait.target),
                Some(WaitTarget::Approval { target: ApprovalTarget::Tool { call_id: target, binding_digest } })
                    if target == call_id && binding_digest == bound.binding_digest())
        {
            return Err(error(ErrorCode::InvalidTransition, "tool.denial"));
        }
        self.prepare_unstarted(saved, call_id, ToolResultStatus::Denied, code, now_ms)
    }

    /// Prepare cancellation or rejection of a saved no-effect call, including an
    /// unknown tool name whose binding was never created. The Agent authorizes
    /// the operation and commits all settlements with the terminal outcome; an
    /// uncertain or potentially dispatched call cannot use this path.
    pub fn prepare_unstarted(
        &self,
        saved: &StoredRun,
        call_id: &Id,
        status: ToolResultStatus,
        code: Id,
        now_ms: i64,
    ) -> Result<PreparedToolResolution, ContractError> {
        if saved.snapshot.scope != *self.registry.scope() {
            return Err(error(ErrorCode::AccessDenied, "tool.resume_scope"));
        }
        let entry = saved
            .snapshot
            .tool_ledger
            .iter()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool.resume_call"))?;
        if saved.snapshot.status != RunStatus::Waiting
            || !matches!(
                status,
                ToolResultStatus::Failed | ToolResultStatus::Denied | ToolResultStatus::Cancelled
            )
            || !matches!(
                entry.state,
                ToolCallState::Planned {}
                    | ToolCallState::ApprovalPending { .. }
                    | ToolCallState::InputPending { .. }
            )
        {
            return Err(error(ErrorCode::InvalidTransition, "tool.unstarted"));
        }
        let result = ToolResult {
            call_id: call_id.clone(),
            call_message_id: round::call_message(saved, &entry.call)?,
            status,
            effect: ToolEffect::NotApplied,
            content: vec![],
            effect_receipt_ref: None,
            error: Some(Failure {
                code,
                diagnostic_ref: None,
            }),
        };
        self.prepared_resolution(saved, result, vec![], None, now_ms)
    }

    /// Validate an answer against the original compiled output schema and prepare
    /// the original call's completion. Invalid answers do not consume the wait;
    /// neither the input resolver nor the tool executor is invoked.
    pub fn prepare_input(
        &self,
        saved: &StoredRun,
        request: &InputRequest,
        bound: &BoundToolInput,
        answer: Value,
        now_ms: i64,
    ) -> Result<PreparedToolResolution, ContractError> {
        let (entry, compiled) = self.resolution_call(saved, &request.call_id, bound)?;
        let ToolCallState::InputPending {
            attempt_id,
            idempotency_key,
            request: pending,
        } = &entry.state
        else {
            return Err(error(ErrorCode::InvalidTransition, "tool.input"));
        };
        if pending != request
            || request.schema_ref.is_some()
            || !matches!(saved.snapshot.wait.as_ref().map(|wait| &wait.target),
                Some(WaitTarget::Input { request: waiting }) if waiting == request)
        {
            return Err(error(ErrorCode::InvalidReference, "tool.input_request"));
        }
        let bytes = serde_json::to_vec(&answer)
            .map_err(|_| error(ErrorCode::InvalidJson, "tool.input_answer"))?
            .len();
        if bytes as u64 > compiled.descriptor().max_output_bytes.get()
            || !crate::tool_schema::compile_validator(&compiled.descriptor().output_schema)?
                .is_valid(&answer)
        {
            return Err(error(ErrorCode::InvalidArguments, "tool.input_answer"));
        }
        let (result, records) = self.validate_output(
            &entry.call,
            round::call_message(saved, &entry.call)?,
            AttemptIdentity {
                scope: &saved.snapshot.scope,
                attempt_id,
                idempotency_key,
            },
            compiled,
            ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded { value: answer },
                effect: ToolEffect::NotApplied,
                receipt: None,
            },
        )?;
        self.prepared_resolution(saved, result, records, None, now_ms)
    }

    /// Prepare a correction from a trusted, authorized read-only verification.
    /// Unknown cannot consume the wait. Output errors preserve confirmed effects
    /// and receipt evidence, and never trigger a new business execution.
    pub fn prepare_external(
        &self,
        saved: &StoredRun,
        call_id: &Id,
        bound: &BoundToolInput,
        verified: ToolExecutionResult,
        now_ms: i64,
    ) -> Result<PreparedToolResolution, ContractError> {
        let (entry, compiled) = self.resolution_call(saved, call_id, bound)?;
        let ToolCallState::Unknown {
            attempt_id,
            idempotency_key,
        } = &entry.state
        else {
            return Err(error(ErrorCode::InvalidTransition, "tool.external"));
        };
        if !matches!(saved.snapshot.wait.as_ref().map(|wait| &wait.target),
            Some(WaitTarget::External { call_id: target, effect_key })
                if target == call_id && effect_key == idempotency_key)
        {
            return Err(error(ErrorCode::InvalidReference, "tool.external_wait"));
        }
        if verified.effect == ToolEffect::Unknown {
            return Err(error(ErrorCode::ToolEffectUnresolved, "tool.external"));
        }
        let call_message_id = round::call_message(saved, &entry.call)?;
        let mut previous = saved.messages.iter().flat_map(|message| {
            message.content.iter().filter_map(|content| match content {
                ContentBlock::ToolResult { result }
                    if message.run_id == saved.snapshot.run_id
                        && message.role == MessageRole::Tool
                        && message.origin == MessageOrigin::Tool
                        && result.call_id == *call_id
                        && result.call_message_id == call_message_id
                        && result.status == ToolResultStatus::Unknown
                        && result.effect == ToolEffect::Unknown =>
                {
                    Some((message.message_id.clone(), result))
                }
                _ => None,
            })
        });
        let (previous_message_id, previous_result) = previous
            .next()
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.unknown_result"))?;
        if previous.next().is_some() {
            return Err(error(ErrorCode::InvalidSnapshot, "tool.unknown_result"));
        }
        let previous_result_digest = canonical_digest(
            &serde_json::to_value(previous_result)
                .map_err(|_| error(ErrorCode::InvalidJson, "tool.unknown_result"))?,
        );
        let (result, records) = self.validate_output(
            &entry.call,
            call_message_id,
            AttemptIdentity {
                scope: &saved.snapshot.scope,
                attempt_id,
                idempotency_key,
            },
            compiled,
            verified,
        )?;
        self.prepared_resolution(
            saved,
            result,
            records,
            Some((previous_message_id, previous_result_digest)),
            now_ms,
        )
    }

    fn resolution_call<'a>(
        &'a self,
        saved: &'a StoredRun,
        call_id: &Id,
        bound: &BoundToolInput,
    ) -> Result<(&'a ToolLedgerEntry, &'a CompiledTool), ContractError> {
        if saved.snapshot.scope != *self.registry.scope() {
            return Err(error(ErrorCode::AccessDenied, "tool.resume_scope"));
        }
        if saved.snapshot.status != RunStatus::Waiting {
            return Err(error(ErrorCode::InvalidTransition, "tool.resume_status"));
        }
        let entry = saved
            .snapshot
            .tool_ledger
            .iter()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool.resume_call"))?;
        let compiled = &self
            .registry
            .get(&entry.call.tool_name)
            .ok_or_else(|| error(ErrorCode::ComponentUnavailable, "tool.resume_contract"))?
            .compiled;
        // Reconstruct only to verify the exact immutable record reference. This
        // neither substitutes for the Agent's scoped retrieval nor performs I/O.
        let reference = entry
            .call
            .bound_input_ref
            .as_ref()
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool.bound_input"))?;
        let record = ProtectedRecord::new(
            reference.record_id.clone(),
            reference.revision,
            serde_json::to_value(bound)
                .map_err(|_| error(ErrorCode::InvalidJson, "tool.bound_input"))?,
        );
        BoundToolInput::restore(
            &record,
            compiled,
            &saved.snapshot.scope,
            &saved.snapshot.run_id,
            &entry.call,
            saved.snapshot.system_inputs.as_ref(),
        )?;
        Ok((entry, compiled))
    }

    fn prepared_resolution(
        &self,
        saved: &StoredRun,
        result: ToolResult,
        mut records: Vec<ProtectedRecord>,
        correction: Option<(Id, JsonDigest)>,
        now_ms: i64,
    ) -> Result<PreparedToolResolution, ContractError> {
        if now_ms < saved.snapshot.timing.last_observed_at_ms {
            return Err(error(ErrorCode::ClockRegression, "tool.resume_time"));
        }
        let record = ProtectedRecord::new(
            self.ids.next_id()?,
            1,
            serde_json::to_value(&result)
                .map_err(|_| error(ErrorCode::InvalidJson, "tool.result"))?,
        );
        let result_ref = record.reference().clone();
        records.push(record);
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: self.ids.next_id()?,
            scope: saved.snapshot.scope.clone(),
            run_id: saved.snapshot.run_id.clone(),
            session_id: saved.snapshot.request.session_id.clone(),
            seq: saved
                .snapshot
                .last_event_seq
                .checked_add(1)
                .and_then(std::num::NonZeroU64::new)
                .ok_or_else(|| error(ErrorCode::InvalidEvent, "tool.resume_event"))?,
            timestamp_ms: now_ms,
            payload: RunEventPayload::ToolSettled { result_ref },
        };
        let content = if let Some((previous_message_id, previous_result_digest)) = correction {
            ContentBlock::ToolResultCorrection {
                previous_message_id,
                previous_result_digest,
                result: result.clone(),
            }
        } else {
            ContentBlock::ToolResult {
                result: result.clone(),
            }
        };
        let message = Message {
            message_id: self.ids.next_id()?,
            run_id: saved.snapshot.run_id.clone(),
            sequence: saved
                .session
                .transcript_revision
                .checked_add(1)
                .and_then(std::num::NonZeroU64::new)
                .ok_or_else(|| error(ErrorCode::InvalidMessage, "tool.resume_message"))?,
            role: MessageRole::Tool,
            content: vec![content],
            origin: MessageOrigin::Tool,
            visibility: Visibility::UserAndModel,
        };
        Ok(PreparedToolResolution {
            state: ToolCallState::Settled {
                result: result.clone(),
            },
            result,
            message,
            records,
            event,
        })
    }
}
```

## `crates/wickle/src/tool_execution/round.rs`

```rust
use super::*;
use futures_util::FutureExt;
use std::{panic::AssertUnwindSafe, time::Duration};

impl SerialToolRound {
    /// Execute only calls from one committed physical model response, in saved order.
    /// Existing settled results are reused, and uncertain attempts are never retried.
    pub async fn execute(
        &self,
        model_request_id: &Id,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<ToolRoundOutcome, ContractError> {
        self.scope(context, budget)?;
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        if let Some(entry) = saved.snapshot.tool_ledger.iter().find(|entry| {
            matches!(entry.state, ToolCallState::Unknown { .. } | ToolCallState::Dispatching { .. })
                || matches!(&entry.state, ToolCallState::Settled { result } if result.status == ToolResultStatus::Unknown || result.effect == ToolEffect::Unknown)
        }) {
            return self.existing_uncertainty(entry, budget).await;
        }
        if let Some(request) = saved.snapshot.tool_ledger.iter().find_map(|entry| {
            if let ToolCallState::InputPending { request, .. } = &entry.state {
                Some(request.clone())
            } else {
                None
            }
        }) {
            return Ok(ToolRoundOutcome::InputRequired { request });
        }
        let call_ids: Vec<_> = saved
            .snapshot
            .tool_ledger
            .iter()
            .filter(|entry| &entry.call.model_request_id == model_request_id)
            .map(|entry| entry.call.call_id.clone())
            .collect();
        for call_id in call_ids {
            self.boundary(context, budget).await?;
            let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
            let entry = saved
                .snapshot
                .tool_ledger
                .iter()
                .find(|entry| entry.call.call_id == call_id)
                .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
            match &entry.state {
                ToolCallState::Settled { result }
                    if result.status != ToolResultStatus::Unknown
                        && result.effect != ToolEffect::Unknown =>
                {
                    continue;
                }
                ToolCallState::Planned {} | ToolCallState::ApprovalPending { .. } => {}
                ToolCallState::InputPending { request, .. } => {
                    return Ok(ToolRoundOutcome::InputRequired {
                        request: request.clone(),
                    });
                }
                _ => return self.existing_uncertainty(entry, budget).await,
            }
            let call = entry.call.clone();
            let pending_key = if let ToolCallState::ApprovalPending {
                idempotency_key, ..
            } = &entry.state
            {
                Some(idempotency_key.clone())
            } else {
                None
            };
            let call_message_id = call_message(&saved, &call)?;
            let registered = self.registry.get(&call.tool_name);
            let Some(registered) = registered.filter(|entry| {
                call.descriptor_digest.as_ref() == Some(entry.compiled.descriptor_digest())
            }) else {
                self.reject(
                    &call,
                    call_message_id,
                    ToolResultStatus::Failed,
                    "unknown_tool",
                    budget,
                )
                .await?;
                continue;
            };
            if registered
                .compiled
                .validate_model_inputs(&call.model_inputs)
                .is_err()
            {
                self.reject(
                    &call,
                    call_message_id,
                    ToolResultStatus::Failed,
                    "invalid_arguments",
                    budget,
                )
                .await?;
                continue;
            }
            let bound = match self
                .binder
                .bind(&registered.compiled, &call_id, context, budget)
                .await
            {
                Ok(bound) => bound,
                Err(error) if control_or_storage(error.code) => return Err(error),
                Err(error) => {
                    let status = if error.code == ErrorCode::AccessDenied {
                        ToolResultStatus::Denied
                    } else {
                        ToolResultStatus::Failed
                    };
                    self.reject(
                        &call,
                        call_message_id,
                        status,
                        &code_name(error.code),
                        budget,
                    )
                    .await?;
                    continue;
                }
            };
            if let PolicyDecision::RequireApproval { reason } = bound.decision {
                return Ok(ToolRoundOutcome::ApprovalRequired {
                    call_id,
                    reason,
                    bound_input_ref: bound.reference,
                    binding_digest: bound.input.binding_digest().clone(),
                });
            }
            match self.authorize(&bound.input, context, budget).await? {
                PolicyDecision::RequireApproval { reason } => {
                    return Ok(ToolRoundOutcome::ApprovalRequired {
                        call_id,
                        reason,
                        bound_input_ref: bound.reference,
                        binding_digest: bound.input.binding_digest().clone(),
                    });
                }
                PolicyDecision::Deny { .. } => {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Denied,
                        "access_denied",
                        budget,
                    )
                    .await?;
                    continue;
                }
                PolicyDecision::Allow {} => {}
            }
            let reservation = budget
                .reserve(ReservationKind::Tool {
                    call_id: call_id.clone(),
                })
                .await?;
            let key = if let Some(key) = pending_key {
                key
            } else {
                Id::new(format!(
                    "tool-effect-{}",
                    canonical_digest(&serde_json::json!([
                        budget.scope(),
                        budget.run_id(),
                        call_id,
                        bound.input.binding_digest()
                    ]))
                ))?
            };
            self.dispatching(&call_id, &reservation.attempt_id, &key, budget)
                .await?;
            let gate = async {
                self.boundary(context, budget).await?;
                let decision = self.authorize(&bound.input, context, budget).await?;
                self.boundary(context, budget).await?;
                Ok::<_, ContractError>(decision)
            }
            .await;
            match gate {
                Ok(PolicyDecision::Allow {}) => {}
                Ok(PolicyDecision::RequireApproval { reason }) => {
                    self.approval_pending(&call_id, &reservation.attempt_id, &key, budget)
                        .await?;
                    return Ok(ToolRoundOutcome::ApprovalRequired {
                        call_id,
                        reason,
                        bound_input_ref: bound.reference,
                        binding_digest: bound.input.binding_digest().clone(),
                    });
                }
                Ok(PolicyDecision::Deny { .. }) => {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Denied,
                        "access_denied",
                        budget,
                    )
                    .await?;
                    continue;
                }
                Err(error)
                    if matches!(
                        error.code,
                        ErrorCode::Cancelled | ErrorCode::DeadlineExceeded
                    ) =>
                {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Cancelled,
                        &code_name(error.code),
                        budget,
                    )
                    .await?;
                    return Err(error);
                }
                Err(error) if control_or_storage(error.code) => return Err(error),
                Err(error) => {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Denied,
                        &code_name(error.code),
                        budget,
                    )
                    .await?;
                    continue;
                }
            }
            let cancellation = budget.cancellation().child_token();
            let _cancel = cancellation.clone().drop_guard();
            let run_deadline = match budget.call_deadline() {
                Ok(deadline) => deadline,
                Err(error) if error.code == ErrorCode::DeadlineExceeded => {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Cancelled,
                        "deadline_exceeded",
                        budget,
                    )
                    .await?;
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
            let deadline = tokio::time::Instant::now()
                .checked_add(Duration::from_millis(self.limits.timeout_ms))
                .ok_or_else(|| error(ErrorCode::InvalidConfiguration, "tool.timeout"))?
                .min(run_deadline);
            let execution = ToolExecutionContext {
                call_id: call_id.clone(),
                attempt_id: reservation.attempt_id.clone(),
                idempotency_key: key.clone(),
                scope: budget.scope().clone(),
                principal_ref: context.data.principal_ref.clone(),
                capability_grant_ref: context.data.capability_grant_ref.clone(),
                cancellation,
                deadline,
            };
            let mut entered = false;
            let result = {
                let operation = AssertUnwindSafe(async {
                    entered = true;
                    registered
                        .executor
                        .execute(bound.input.execution_args(), &execution)
                        .await
                })
                .catch_unwind();
                tokio::select! { biased;
                    _ = context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "tool.execution")),
                    _ = tokio::time::sleep_until(deadline) => Err(error(ErrorCode::DeadlineExceeded, "tool.execution")),
                    stopped = budget.wait_for_cancellation_or_deadline() => match stopped { Err(error) => Err(error), Ok(()) => Err(error(ErrorCode::DeadlineExceeded, "tool.execution")) },
                    result = operation => result.unwrap_or_else(|_| Err(error(ErrorCode::InvalidContract, "tool.executor"))),
                }
            };
            execution.cancellation.cancel();
            let completion = match result {
                Ok(result) => result,
                Err(error) => ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Failed {
                        code: Id::new(code_name(error.code))?,
                    },
                    effect: if entered
                        && registered.compiled.descriptor().side_effect != ToolSideEffect::ReadOnly
                    {
                        ToolEffect::Unknown
                    } else {
                        ToolEffect::NotApplied
                    },
                    receipt: None,
                },
            };
            if let ToolExecutionOutcome::InputRequired { question } = &completion.outcome {
                let question_bytes = serde_json::to_vec(question)
                    .map_err(|_| error(ErrorCode::InvalidJson, "tool.input_question"))?
                    .len();
                if completion.effect == ToolEffect::NotApplied
                    && completion.receipt.is_none()
                    && !question.trim().is_empty()
                    && question_bytes as u64
                        <= registered.compiled.descriptor().max_output_bytes.get()
                {
                    let request = InputRequest {
                        input_request_id: self.ids.next_id()?,
                        call_id: call_id.clone(),
                        question: question.clone(),
                        schema_ref: None,
                    };
                    self.input_pending(&execution, &request, budget).await?;
                    return Ok(ToolRoundOutcome::InputRequired { request });
                }
            }
            let (result, records) = self.validate_output(
                &call,
                call_message_id,
                AttemptIdentity {
                    scope: &execution.scope,
                    attempt_id: &execution.attempt_id,
                    idempotency_key: &execution.idempotency_key,
                },
                &registered.compiled,
                completion,
            )?;
            let unresolved = result.effect == ToolEffect::Unknown;
            let state = if unresolved {
                ToolCallState::Unknown {
                    attempt_id: execution.attempt_id.clone(),
                    idempotency_key: key.clone(),
                }
            } else {
                ToolCallState::Settled {
                    result: result.clone(),
                }
            };
            let result_ref = self
                .settle(&call_id, state, result, records, budget)
                .await?;
            if unresolved {
                return Ok(ToolRoundOutcome::Unresolved {
                    call_id,
                    result_ref,
                });
            }
        }
        Ok(ToolRoundOutcome::Completed)
    }

    /// Close unstarted plans and input requests confirmed to have no effect when
    /// a segment ends. Unknown and potentially dispatched calls are untouched.
    pub async fn settle_unstarted(
        &self,
        model_request_id: &Id,
        status: ToolResultStatus,
        code: Id,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        self.scope(context, budget)?;
        if !matches!(
            status,
            ToolResultStatus::Failed | ToolResultStatus::Denied | ToolResultStatus::Cancelled
        ) {
            return Err(error(ErrorCode::InvalidContract, "tool.settlement"));
        }
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        for entry in &saved.snapshot.tool_ledger {
            if &entry.call.model_request_id == model_request_id
                && matches!(
                    entry.state,
                    ToolCallState::Planned {}
                        | ToolCallState::ApprovalPending { .. }
                        | ToolCallState::InputPending { .. }
                )
            {
                self.reject(
                    &entry.call,
                    call_message(&saved, &entry.call)?,
                    status,
                    code.as_str(),
                    budget,
                )
                .await?;
            }
        }
        Ok(())
    }

    fn scope(&self, context: &ExecutionContext, budget: &RunBudget) -> Result<(), ContractError> {
        if &context.data.scope != budget.scope() || self.registry.scope() != budget.scope() {
            return Err(error(ErrorCode::AccessDenied, "scope"));
        }
        Ok(())
    }
    async fn boundary(
        &self,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        self.scope(context, budget)?;
        if context.cancellation.is_cancelled() {
            return Err(error(ErrorCode::Cancelled, "tool"));
        }
        budget.check_boundary().await
    }
    async fn authorize(
        &self,
        input: &BoundToolInput,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<PolicyDecision, ContractError> {
        let saved = tokio::select! { biased;
            _ = context.cancellation.cancelled() => return Err(error(ErrorCode::Cancelled, "tool.policy")),
            stopped = budget.wait_for_cancellation_or_deadline() => return match stopped { Err(error) => Err(error), Ok(()) => Err(error(ErrorCode::DeadlineExceeded, "tool.policy")) },
            result = budget.store().load(budget.scope(), budget.run_id()) => result?,
        };
        let request = input.policy_request_for_run(&saved.snapshot);
        tokio::select! { biased;
            _ = context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "tool.policy")),
            stopped = budget.wait_for_cancellation_or_deadline() => match stopped { Err(error) => Err(error), Ok(()) => Err(error(ErrorCode::DeadlineExceeded, "tool.policy")) },
            result = self.policy.check(&request, context, Some(budget.call_deadline()?), None) => result,
        }
    }
    async fn dispatching(
        &self,
        call_id: &Id,
        attempt_id: &Id,
        key: &Id,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let entry = snapshot
            .tool_ledger
            .iter_mut()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
        if !matches!(
            entry.state,
            ToolCallState::Planned {} | ToolCallState::ApprovalPending { .. }
        ) || entry.call.bound_input_ref.is_none()
        {
            return Err(error(ErrorCode::InvalidTransition, "tool.dispatch"));
        }
        if let ToolCallState::ApprovalPending {
            idempotency_key, ..
        } = &entry.state
        {
            if idempotency_key != key {
                return Err(error(ErrorCode::InvalidTransition, "tool.idempotency_key"));
            }
        }
        entry.state = ToolCallState::Dispatching {
            attempt_id: attempt_id.clone(),
            idempotency_key: key.clone(),
        };
        self.commit(snapshot, vec![], vec![], vec![], budget).await
    }
    async fn approval_pending(
        &self,
        call_id: &Id,
        attempt_id: &Id,
        key: &Id,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let entry = snapshot
            .tool_ledger
            .iter_mut()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
        if !matches!(&entry.state, ToolCallState::Dispatching { attempt_id: current, idempotency_key } if current == attempt_id && idempotency_key == key)
        {
            return Err(error(ErrorCode::InvalidTransition, "tool.approval"));
        }
        entry.state = ToolCallState::ApprovalPending {
            attempt_id: attempt_id.clone(),
            idempotency_key: key.clone(),
        };
        self.commit(snapshot, vec![], vec![], vec![], budget).await
    }
    async fn input_pending(
        &self,
        execution: &ToolExecutionContext,
        request: &InputRequest,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let entry = snapshot
            .tool_ledger
            .iter_mut()
            .find(|entry| entry.call.call_id == execution.call_id)
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
        if !matches!(&entry.state, ToolCallState::Dispatching { attempt_id, idempotency_key }
            if attempt_id == &execution.attempt_id && idempotency_key == &execution.idempotency_key)
        {
            return Err(error(ErrorCode::InvalidTransition, "tool.input_pending"));
        }
        entry.state = ToolCallState::InputPending {
            attempt_id: execution.attempt_id.clone(),
            idempotency_key: execution.idempotency_key.clone(),
            request: request.clone(),
        };
        self.commit(snapshot, vec![], vec![], vec![], budget).await
    }
    async fn reject(
        &self,
        call: &ToolCall,
        call_message_id: Id,
        status: ToolResultStatus,
        code: &str,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let result = ToolResult {
            call_id: call.call_id.clone(),
            call_message_id,
            status,
            effect: ToolEffect::NotApplied,
            content: vec![],
            effect_receipt_ref: None,
            error: Some(Failure {
                code: Id::new(code)?,
                diagnostic_ref: None,
            }),
        };
        self.settle(
            &call.call_id,
            ToolCallState::Settled {
                result: result.clone(),
            },
            result,
            vec![],
            budget,
        )
        .await?;
        Ok(())
    }

    pub(super) fn validate_output(
        &self,
        call: &ToolCall,
        call_message_id: Id,
        attempt: AttemptIdentity<'_>,
        compiled: &CompiledTool,
        completion: ToolExecutionResult,
    ) -> Result<(ToolResult, Vec<ProtectedRecord>), ContractError> {
        let effect = completion.effect;
        let receipt_bytes = completion
            .receipt
            .as_ref()
            .map(|receipt| serde_json::to_vec(receipt).map(|bytes| bytes.len()))
            .transpose()
            .map_err(|_| error(ErrorCode::InvalidJson, "tool.receipt"))?;
        let receipt_oversized =
            receipt_bytes.is_some_and(|size| size > self.limits.max_receipt_bytes);
        let raw_receipt = if receipt_oversized {
            serde_json::json!({"omitted":true,"bytes":receipt_bytes,"digest":canonical_digest(completion.receipt.as_ref().expect("oversized receipt"))})
        } else {
            completion.receipt.clone().unwrap_or(Value::Null)
        };
        let mut status = ToolResultStatus::Succeeded;
        let mut code = None;
        let mut content = vec![];
        let raw_output = match &completion.outcome {
            ToolExecutionOutcome::Succeeded { value } => {
                let bytes = serde_json::to_vec(value)
                    .map_err(|_| error(ErrorCode::InvalidJson, "tool.output"))?
                    .len();
                if bytes as u64 > compiled.descriptor().max_output_bytes.get() {
                    status = ToolResultStatus::Failed;
                    code = Some(Id::new("tool_output_too_large")?);
                    serde_json::json!({"omitted":true,"bytes":bytes,"digest":canonical_digest(value)})
                } else {
                    if !crate::tool_schema::compile_validator(&compiled.descriptor().output_schema)?
                        .is_valid(value)
                    {
                        status = ToolResultStatus::Failed;
                        code = Some(Id::new("invalid_tool_output")?);
                    } else {
                        content.push(InputContent::Json {
                            value: value.clone(),
                        });
                    }
                    value.clone()
                }
            }
            ToolExecutionOutcome::Failed { code: failure } => {
                status = if failure.as_str() == "cancelled" {
                    ToolResultStatus::Cancelled
                } else {
                    ToolResultStatus::Failed
                };
                code = Some(failure.clone());
                Value::Null
            }
            ToolExecutionOutcome::InputRequired { .. } => {
                // Only a bounded no-effect request is accepted before this path.
                // Invalid requests retain any reported effect and receipt.
                status = ToolResultStatus::Failed;
                code = Some(Id::new("invalid_input_request")?);
                Value::Null
            }
        };
        if effect == ToolEffect::Unknown {
            status = ToolResultStatus::Unknown;
            code = Some(Id::new("tool_effect_unknown")?);
            content.clear();
        } else if receipt_oversized {
            status = ToolResultStatus::Failed;
            code = Some(Id::new("effect_receipt_too_large")?);
            content.clear();
        } else if effect == ToolEffect::Applied && completion.receipt.is_none() {
            status = ToolResultStatus::Failed;
            code = Some(Id::new("effect_receipt_missing")?);
            content.clear();
        } else if effect == ToolEffect::Applied
            && compiled.descriptor().side_effect == ToolSideEffect::ReadOnly
        {
            status = ToolResultStatus::Failed;
            code = Some(Id::new("tool_effect_contract")?);
            content.clear();
        }
        let record = ProtectedRecord::new(
            self.ids.next_id()?,
            1,
            serde_json::json!({
                "scope":attempt.scope,"call_id":call.call_id,"attempt_id":attempt.attempt_id,"idempotency_key":attempt.idempotency_key,
                "effect":effect,"receipt":raw_receipt,"receipt_omitted":receipt_oversized,"output":raw_output,"error_code":code,
            }),
        );
        let reference = record.reference().clone();
        Ok((
            ToolResult {
                call_id: call.call_id.clone(),
                call_message_id,
                status,
                effect,
                content,
                effect_receipt_ref: (effect != ToolEffect::NotApplied
                    || completion.receipt.is_some())
                .then(|| reference.clone()),
                error: code.map(|code| Failure {
                    code,
                    diagnostic_ref: Some(reference),
                }),
            },
            vec![record],
        ))
    }

    async fn settle(
        &self,
        call_id: &Id,
        state: ToolCallState,
        result: ToolResult,
        mut records: Vec<ProtectedRecord>,
        budget: &RunBudget,
    ) -> Result<RecordRef, ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let entry = snapshot
            .tool_ledger
            .iter_mut()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
        if matches!(
            entry.state,
            ToolCallState::Settled { .. } | ToolCallState::Unknown { .. }
        ) {
            return Err(error(ErrorCode::InvalidTransition, "tool.settlement"));
        }
        entry.state = state.clone();
        let record = ProtectedRecord::new(
            self.ids.next_id()?,
            1,
            serde_json::to_value(&result)
                .map_err(|_| error(ErrorCode::InvalidJson, "tool.result"))?,
        );
        let reference = record.reference().clone();
        records.push(record);
        snapshot.last_event_seq = snapshot
            .last_event_seq
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::InvalidEvent, "tool.event"))?;
        let (_, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: self.ids.next_id()?,
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            session_id: snapshot.request.session_id.clone(),
            seq: snapshot
                .last_event_seq
                .try_into()
                .map_err(|_| error(ErrorCode::InvalidEvent, "tool.event"))?,
            timestamp_ms: now,
            payload: match state {
                ToolCallState::Unknown {
                    attempt_id,
                    idempotency_key,
                } => RunEventPayload::ToolUnresolved {
                    result_ref: reference.clone(),
                    attempt_id,
                    idempotency_key,
                },
                _ => RunEventPayload::ToolSettled {
                    result_ref: reference.clone(),
                },
            },
        };
        let message = Message {
            message_id: self.ids.next_id()?,
            run_id: budget.run_id().clone(),
            sequence: saved
                .session
                .transcript_revision
                .checked_add(1)
                .and_then(std::num::NonZeroU64::new)
                .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.message"))?,
            role: MessageRole::Tool,
            content: vec![ContentBlock::ToolResult { result }],
            origin: MessageOrigin::Tool,
            visibility: Visibility::UserAndModel,
        };
        self.commit(snapshot, vec![message], vec![event], records, budget)
            .await?;
        Ok(reference)
    }
    async fn commit(
        &self,
        mut snapshot: RunSnapshot,
        messages: Vec<Message>,
        events: Vec<RunEvent>,
        records: Vec<ProtectedRecord>,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let expected_revision = snapshot.revision;
        let (_, check_at) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let lease = budget
            .store()
            .check_lease(budget.scope(), budget.run_id(), budget.lease(), check_at)
            .await?;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        if now >= lease.expires_at_ms {
            return Err(error(ErrorCode::LeaseLost, "tool.lease"));
        }
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::RevisionConflict, "tool.revision"))?;
        snapshot.phase = RunPhase::Tool;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        budget
            .store()
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages,
                    events,
                    records,
                },
            )
            .await?;
        Ok(())
    }
    async fn existing_uncertainty(
        &self,
        entry: &ToolLedgerEntry,
        budget: &RunBudget,
    ) -> Result<ToolRoundOutcome, ContractError> {
        let ToolCallState::Unknown {
            attempt_id,
            idempotency_key,
        } = &entry.state
        else {
            return Err(error(
                ErrorCode::InvalidTransition,
                "tool.unresolved_dispatch",
            ));
        };
        let mut after = 0;
        loop {
            let page = budget
                .store()
                .read_events(budget.scope(), budget.run_id(), after, MAX_EVENT_PAGE_SIZE)
                .await?;
            for event in &page.events {
                if let RunEventPayload::ToolUnresolved {
                    result_ref,
                    attempt_id: saved_attempt,
                    idempotency_key: saved_key,
                } = &event.payload
                {
                    if saved_attempt == attempt_id && saved_key == idempotency_key {
                        return Ok(ToolRoundOutcome::Unresolved {
                            call_id: entry.call.call_id.clone(),
                            result_ref: result_ref.clone(),
                        });
                    }
                }
            }
            if !page.has_more {
                return Err(error(ErrorCode::InvalidSnapshot, "tool.unresolved_result"));
            }
            after = page.next_after_seq;
        }
    }
}

pub(super) fn call_message(saved: &StoredRun, call: &ToolCall) -> Result<Id, ContractError> {
    let messages: Vec<_> = saved.messages.iter().filter(|message| message.run_id == saved.snapshot.run_id && message.role == MessageRole::Assistant && message.content.iter().any(|content| matches!(content, ContentBlock::ToolCall { call: candidate } if candidate.call_id == call.call_id && candidate.model_request_id == call.model_request_id && candidate.provider_call_id == call.provider_call_id && candidate.tool_name == call.tool_name && candidate.model_inputs == call.model_inputs && candidate.descriptor_digest == call.descriptor_digest))).collect();
    if messages.len() != 1 {
        return Err(error(ErrorCode::InvalidSnapshot, "tool.call_message"));
    }
    Ok(messages[0].message_id.clone())
}
fn code_name(code: ErrorCode) -> String {
    serde_json::to_value(code)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "invalid_contract".into())
}
fn control_or_storage(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::Cancelled
            | ErrorCode::DeadlineExceeded
            | ErrorCode::BudgetExceeded
            | ErrorCode::LeaseLost
            | ErrorCode::RevisionConflict
            | ErrorCode::PersistenceUnavailable
            | ErrorCode::StateNotFound
            | ErrorCode::ClockUnavailable
            | ErrorCode::ClockRegression
    )
}
```

## `crates/wickle/tests/agent_resume.rs`

```rust
//! Waiting segments resume through durable commands without repeating saved tool effects.

#[path = "support/agent.rs"]
#[allow(dead_code)]
mod agent_support;
#[path = "support/agent_resume.rs"]
#[allow(dead_code)]
mod support;
use futures_util::{StreamExt, TryStreamExt};
use serde_json::json;
use std::{sync::atomic::Ordering, time::Duration};
use support::*;
use wickle::*;

#[tokio::test]
async fn approval_continues_the_same_run_with_frozen_inputs_and_a_distinct_segment_handle() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    let waiting = fixture.outcome(&original).await;
    assert_eq!(waiting.result.status(), RunStatus::Waiting);
    let saved_wait = fixture.saved(&original).await;
    assert_eq!(*fixture.order.lock().unwrap(), vec!["before"]);
    assert_eq!(fixture.resolver.calls.load(Ordering::SeqCst), 1);
    let command = fixture.approve(&original, "approve").await;
    *fixture.resolver.value.lock().unwrap() = ResolvedSystemInput {
        value: json!(CHANGED_RECORD),
        revision: id("record-revision-2"),
    };
    let mut approver = context();
    approver.data.principal_ref = id("reviewer");
    approver.data.capability_grant_ref = id("reviewer-grant");
    let resumed = completed(
        agent
            .resume(command.clone(), approver.clone())
            .await
            .unwrap(),
    );
    let result = fixture.outcome(&resumed).await;
    assert_eq!(resumed.run_id(), original.run_id());
    assert_eq!(result.result.status(), RunStatus::Succeeded);
    assert!(result.checkpoint_revision > waiting.checkpoint_revision);
    assert_eq!(
        *fixture.order.lock().unwrap(),
        vec!["before", "target", "after"]
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    {
        let observed = fixture.tools[1].seen.lock().unwrap();
        assert_eq!(
            observed[0].arguments,
            object(json!({"query":"target","workspace_id":WORKSPACE,"record_id":RECORD}))
        );
        assert_eq!(
            observed[0].context.principal_ref,
            approver.data.principal_ref
        );
        assert_eq!(
            observed[0].context.capability_grant_ref,
            approver.data.capability_grant_ref
        );
    }
    let saved = fixture.saved(&resumed).await;
    assert_eq!(saved.snapshot.request, saved_wait.snapshot.request);
    assert_eq!(
        saved.snapshot.system_inputs,
        saved_wait.snapshot.system_inputs
    );
    assert_eq!(
        saved.session.prompt_snapshot,
        saved_wait.session.prompt_snapshot
    );
    assert_eq!(
        saved.snapshot.tool_ledger[1].call,
        saved_wait.snapshot.tool_ledger[1].call
    );
    let receipt = &saved.snapshot.resume_receipts[0];
    assert_eq!(saved.snapshot.resume_receipts.len(), 1);
    assert_eq!(receipt.command, command);
    assert_eq!(receipt.actor_ref, approver.data.principal_ref);
    assert_eq!(
        receipt.previous_last_event_seq,
        saved_wait.snapshot.last_event_seq
    );
    assert_eq!(receipt.previous_segment_start_revision, 0);
    let prior = fixture
        .base
        .store
        .read_record(&scope(), &receipt.previous_outcome_ref)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_value::<RunOutcome>(prior.value().clone()).unwrap(),
        waiting
    );
    assert_eq!(fixture.outcome(&original).await, waiting);
    let old_events: Vec<_> = original.events(0, context()).try_collect().await.unwrap();
    assert_eq!(
        old_events.last().unwrap().seq.get(),
        saved_wait.snapshot.last_event_seq
    );
    let remaining: Vec<_> = resumed
        .events(saved_wait.snapshot.last_event_seq, context())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        remaining[0].seq.get(),
        saved_wait.snapshot.last_event_seq + 1
    );
    assert_eq!(
        remaining.last().unwrap().seq.get(),
        saved.snapshot.last_event_seq
    );
    let events = fixture
        .base
        .store
        .read_events(&scope(), original.run_id(), 0, 100)
        .await
        .unwrap();
    assert!(
        events
            .events
            .windows(2)
            .all(|pair| pair[1].seq.get() == pair[0].seq.get() + 1)
    );
    assert_eq!(
        events
            .events
            .iter()
            .filter(|event| matches!(event.payload, RunEventPayload::RunResumed { .. }))
            .count(),
        1
    );
    assert!(
        fixture
            .policy
            .tool_checks
            .lock()
            .unwrap()
            .iter()
            .any(|(input, principal)| input
                .approval()
                .is_some_and(|approval| approval.command_id() == &command.command_id
                    && approval.actor_ref() == &approver.data.principal_ref)
                && principal == &approver.data.principal_ref)
    );
}

#[tokio::test(start_paused = true)]
async fn waiting_consumes_no_new_calls_and_late_approval_keeps_its_charged_reservation() {
    let fixture = Fixture::new(Mode::LateApproval);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    let waiting = fixture.outcome(&original).await;
    let saved = fixture.saved(&original).await;
    let ToolCallState::ApprovalPending {
        attempt_id,
        idempotency_key,
    } = &saved.snapshot.tool_ledger[1].state
    else {
        panic!("expected reserved approval")
    };
    assert_eq!(waiting.usage.tool_attempts, 2); // One completed read and one unentered reservation.
    tokio::time::advance(Duration::from_millis(500)).await;
    assert_eq!(fixture.saved(&original).await.snapshot.usage, waiting.usage);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    let resumed = completed(
        agent
            .resume(fixture.approve(&original, "approve-late").await, context())
            .await
            .unwrap(),
    );
    let result = fixture.outcome(&resumed).await;
    assert_eq!(result.result.status(), RunStatus::Succeeded);
    assert_eq!(result.usage.tool_attempts, 4);
    let target = fixture.tools[1].seen.lock().unwrap();
    assert_ne!(&target[0].context.attempt_id, attempt_id);
    assert_eq!(&target[0].context.idempotency_key, idempotency_key);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn repeated_commands_reuse_acceptance_after_completion_and_changed_decisions_conflict() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let command = fixture.approve(&original, "approval-command").await;
    let resumed = completed(agent.resume(command.clone(), context()).await.unwrap());
    let outcome = fixture.outcome(&resumed).await;
    let before = fixture.saved(&resumed).await;
    let replay = completed(agent.resume(command.clone(), context()).await.unwrap());
    assert_eq!(fixture.outcome(&replay).await, outcome);
    assert_eq!(fixture.saved(&replay).await.snapshot, before.snapshot);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
    let mut conflicting = command.clone();
    let ResumeAction::Approve { wait_id, target } = command.action else {
        unreachable!()
    };
    conflicting.action = ResumeAction::Deny {
        wait_id,
        target,
        reason: "declined".into(),
    };
    assert_eq!(
        agent.resume(conflicting, context()).await.unwrap_err().code,
        ErrorCode::RequestConflict
    );
    let mut fresh = before.snapshot.resume_receipts[0].command.clone();
    fresh.command_id = id("new-command-after-terminal");
    fresh.expected_revision = before.snapshot.revision;
    assert!(agent.resume(fresh, context()).await.is_err());
    assert_eq!(fixture.saved(&replay).await.snapshot, before.snapshot);
}

#[tokio::test]
async fn explicit_system_input_changes_and_wrong_wait_binding_scope_or_revision_do_not_consume_commands()
 {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    fixture.outcome(&handle).await;
    let saved = fixture.saved(&handle).await;
    let command = fixture.approve(&handle, "approval").await;
    for values in [
        JsonObject::new(),
        object(json!({"workspace_id":CHANGED_RECORD})),
    ] {
        let mut caller = context();
        caller.data.system_inputs = Some(SystemInputs::new(values));
        assert_eq!(
            agent
                .resume(command.clone(), caller)
                .await
                .unwrap_err()
                .code,
            ErrorCode::SystemInputsMismatch
        );
    }
    let mut wrong_scope = context();
    wrong_scope.data.scope.tenant_id = id("foreign");
    assert_eq!(
        agent
            .resume(command.clone(), wrong_scope)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    let mut stale = command.clone();
    stale.expected_revision -= 1;
    assert!(agent.resume(stale, context()).await.is_err());
    let mut wrong_wait = command.clone();
    if let ResumeAction::Approve { wait_id, .. } = &mut wrong_wait.action {
        *wait_id = id("other-wait");
    }
    assert!(agent.resume(wrong_wait, context()).await.is_err());
    let mut wrong_binding = command.clone();
    if let ResumeAction::Approve {
        target: ApprovalTarget::Tool { binding_digest, .. },
        ..
    } = &mut wrong_binding.action
    {
        *binding_digest = canonical_digest(&json!("other-target"));
    }
    assert!(agent.resume(wrong_binding, context()).await.is_err());
    assert_eq!(fixture.saved(&handle).await.snapshot, saved.snapshot);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    let mut same_values = context();
    same_values.data.system_inputs =
        Some(SystemInputs::new(object(json!({"workspace_id":WORKSPACE}))));
    let resumed = completed(agent.resume(command, same_values).await.unwrap());
    assert_eq!(
        fixture.outcome(&resumed).await.result.status(),
        RunStatus::Succeeded
    );
}

#[tokio::test]
async fn a_recorded_approval_never_overrides_current_resume_or_target_denial() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    fixture.outcome(&handle).await;
    let command = fixture.approve(&handle, "approve").await;
    fixture.policy.deny_resume.store(true, Ordering::SeqCst);
    assert_eq!(
        agent
            .resume(command.clone(), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert!(
        fixture
            .saved(&handle)
            .await
            .snapshot
            .resume_receipts
            .is_empty()
    );
    fixture.policy.deny_resume.store(false, Ordering::SeqCst);
    fixture
        .policy
        .require_resume_approval
        .store(true, Ordering::SeqCst);
    assert!(matches!(
        agent.resume(command.clone(), context()).await.unwrap(),
        Guarded::ApprovalRequired(_)
    ));
    assert!(
        fixture
            .saved(&handle)
            .await
            .snapshot
            .resume_receipts
            .is_empty()
    );
    fixture
        .policy
        .require_resume_approval
        .store(false, Ordering::SeqCst);
    fixture.policy.deny_execute.store(true, Ordering::SeqCst);
    let resumed = completed(agent.resume(command.clone(), context()).await.unwrap());
    fixture.outcome(&resumed).await;
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 0);
    let saved = fixture.saved(&resumed).await;
    assert!(
        matches!(&saved.snapshot.tool_ledger[1].state, ToolCallState::Settled { result } if result.status == ToolResultStatus::Denied && result.effect == ToolEffect::NotApplied)
    );
    fixture.policy.deny_resume.store(true, Ordering::SeqCst);
    assert_eq!(
        agent.resume(command, context()).await.unwrap_err().code,
        ErrorCode::AccessDenied
    );
}

#[tokio::test]
async fn denying_the_fixed_candidate_closes_only_that_call_and_preserves_prior_results() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    fixture.outcome(&handle).await;
    let mut command = fixture.approve(&handle, "deny").await;
    let ResumeAction::Approve { wait_id, target } = command.action else {
        unreachable!()
    };
    command.action = ResumeAction::Deny {
        wait_id,
        target,
        reason: "Target declined by reviewer".into(),
    };
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&resumed).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(*fixture.order.lock().unwrap(), vec!["before", "after"]);
    let saved = fixture.saved(&resumed).await;
    assert!(
        matches!(&saved.snapshot.tool_ledger[1].state, ToolCallState::Settled { result } if result.status == ToolResultStatus::Denied && result.effect == ToolEffect::NotApplied)
    );
    assert_eq!(fixture.tools[0].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn competing_identical_commands_consume_once_even_when_an_earlier_caller_is_paused() {
    let fixture = Fixture::new(Mode::Approval);
    let first = fixture.agent();
    let second = fixture.agent();
    let original = fixture.started(&first).await;
    fixture.outcome(&original).await;
    let command = fixture.approve(&original, "shared-approval").await;
    fixture
        .policy
        .pause_next_resume
        .store(true, Ordering::SeqCst);
    let delayed = tokio::spawn({
        let command = command.clone();
        async move { first.resume(command, context()).await }
    });
    gate(&fixture.policy.resume_entered).await;
    let winner = completed(second.resume(command.clone(), context()).await.unwrap());
    let result = fixture.outcome(&winner).await;
    fixture.policy.resume_release.add_permits(1);
    let replay = completed(delayed.await.unwrap().unwrap());
    assert_eq!(fixture.outcome(&replay).await, result);
    let saved = fixture.saved(&replay).await;
    assert_eq!(saved.snapshot.resume_receipts.len(), 1);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn dropping_resumed_observers_does_not_cancel_the_accepted_execution() {
    let fixture = Fixture::new(Mode::Approval);
    fixture.model.hold_final.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    let waiting = fixture.outcome(&original).await;
    let command = fixture.approve(&original, "resume-owned").await;
    let resumed = completed(agent.resume(command.clone(), context()).await.unwrap());
    gate(&fixture.model.final_entered).await;
    let observer = context();
    let mut outcome_future = Box::pin(resumed.outcome(&observer));
    assert!(futures_util::poll!(outcome_future.as_mut()).is_pending());
    drop(outcome_future);
    let mut events = resumed.events(0, context());
    events.next().await.unwrap().unwrap();
    drop(events);
    drop(resumed);
    fixture.model.final_release.add_permits(1);
    let replay = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&replay).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.outcome(&original).await, waiting);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn input_answers_use_the_saved_output_schema_and_do_not_reenter_the_question_tool() {
    let fixture = Fixture::new(Mode::Input);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    let waiting = fixture.outcome(&original).await;
    assert_eq!(waiting.result.status(), RunStatus::Waiting);
    let before = fixture.saved(&original).await;
    let wait = before.snapshot.wait.as_ref().unwrap();
    assert!(matches!(wait.target, WaitTarget::Input { .. }));
    let mut command = ResumeCommand {
        run_id: original.run_id().clone(),
        expected_revision: before.snapshot.revision,
        command_id: id("answer"),
        action: ResumeAction::Input {
            wait_id: wait.wait_id.clone(),
            answer: json!({"selection":"annual"}),
        },
    };
    for answer in [
        json!(42),
        json!({}),
        json!({"selection":"unknown"}),
        json!({"selection":"annual","workspace_id":WORKSPACE}),
    ] {
        let mut invalid = command.clone();
        if let ResumeAction::Input { answer: value, .. } = &mut invalid.action {
            *value = answer;
        }
        assert!(agent.resume(invalid, context()).await.is_err());
        assert_eq!(fixture.saved(&original).await.snapshot, before.snapshot);
    }
    command.command_id = id("valid-answer");
    let resumed = completed(agent.resume(command.clone(), context()).await.unwrap());
    let result = fixture.outcome(&resumed).await;
    assert_eq!(result.result.status(), RunStatus::Succeeded);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 0);
    assert_eq!(
        *fixture.order.lock().unwrap(),
        vec!["before", "target", "after"]
    );
    let saved = fixture.saved(&resumed).await;
    assert!(
        matches!(&saved.snapshot.tool_ledger[1].state, ToolCallState::Settled { result } if result.effect == ToolEffect::NotApplied && result.content == vec![InputContent::Json { value: json!({"selection":"annual"}) }])
    );
    let requests = fixture.model.requests.lock().unwrap();
    let target_results: Vec<_> = requests[1]
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            ModelContent::ToolResult {
                provider_call_id,
                content,
            } if provider_call_id == &id("provider-1") => Some(content),
            _ => None,
        })
        .collect();
    assert_eq!(
        target_results,
        vec![
            &json!({"status":"succeeded","effect":"not_applied","content":[{"type":"json","value":{"selection":"annual"}}]})
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn waiting_time_counts_toward_the_original_run_deadline_without_new_dispatch() {
    let mut fixture = Fixture::new(Mode::Approval);
    fixture.profile.limits.max_elapsed_ms = 100.try_into().unwrap();
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    let waiting = fixture.outcome(&original).await;
    let command = fixture.approve(&original, "expired-approval").await;
    tokio::time::advance(Duration::from_millis(101)).await;
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    let result = fixture.outcome(&resumed).await;
    assert_eq!(
        result.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    );
    assert_eq!(result.usage.model_calls, waiting.usage.model_calls);
    assert_eq!(result.usage.tool_attempts, waiting.usage.tool_attempts);
    assert!(result.usage.elapsed_ms >= 100);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancelling_a_saved_wait_without_a_local_driver_prevents_new_resume_commands() {
    let fixture = Fixture::new(Mode::Approval);
    let original_agent = fixture.agent();
    let original = fixture.started(&original_agent).await;
    fixture.outcome(&original).await;
    let command = fixture.approve(&original, "approve-too-late").await;
    drop(original_agent);
    let reopened = fixture.agent();
    let mut execution = context();
    execution.data.system_inputs =
        Some(SystemInputs::new(object(json!({"workspace_id":WORKSPACE}))));
    let handle = completed(reopened.start(request("request"), execution).await.unwrap());
    fixture.policy.deny_cancel.store(true, Ordering::SeqCst);
    assert_eq!(
        handle
            .cancel(id("not-authorized"), &context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(
        fixture.saved(&handle).await.snapshot.status,
        RunStatus::Waiting
    );
    fixture.policy.deny_cancel.store(false, Ordering::SeqCst);
    let receipt = completed(handle.cancel(id("stop-waiting"), &context()).await.unwrap());
    assert_ne!(receipt, CancelReceipt::NotLocal);
    let result = fixture.outcome(&handle).await;
    assert_eq!(result.result.status(), RunStatus::Cancelled);
    assert!(reopened.resume(command, context()).await.is_err());
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_command_commit_leaves_the_wait_intact_and_a_retry_executes_once() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let before = fixture.saved(&original).await;
    let command = fixture.approve(&original, "retry-commit").await;
    fixture.store.mode.store(1, Ordering::SeqCst);
    assert_eq!(
        agent
            .resume(command.clone(), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::PersistenceUnavailable
    );
    assert_eq!(fixture.saved(&original).await.snapshot, before.snapshot);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&resumed).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.saved(&resumed).await.snapshot.resume_receipts.len(),
        1
    );
}

#[tokio::test]
async fn command_commit_ack_loss_is_recovered_without_consuming_or_executing_twice() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let command = fixture.approve(&original, "lost-ack").await;
    fixture.store.mode.store(2, Ordering::SeqCst);
    let initial = agent.resume(command.clone(), context()).await;
    if let Err(error) = initial {
        assert_eq!(error.code, ErrorCode::PersistenceUnavailable);
    }
    let replay = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&replay).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.store.consumed_commits.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.saved(&replay).await.snapshot.resume_receipts.len(),
        1
    );
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn dropping_a_polled_resume_future_does_not_abandon_its_command_transaction() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let command = fixture.approve(&original, "detached-resume").await;
    fixture.store.mode.store(3, Ordering::SeqCst);
    let mut resuming = Box::pin(agent.resume(command.clone(), context()));
    assert!(futures_util::poll!(resuming.as_mut()).is_pending());
    gate(&fixture.store.entered).await;
    drop(resuming);
    fixture.store.release.add_permits(1);
    let replay = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&replay).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.saved(&replay).await.snapshot.resume_receipts.len(),
        1
    );
}

#[tokio::test]
async fn a_cancelled_wait_rejects_an_approval_still_paused_at_its_current_policy_check() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let command = fixture.approve(&original, "losing-approval").await;
    fixture
        .policy
        .pause_next_resume
        .store(true, Ordering::SeqCst);
    let pending = tokio::spawn(async move { agent.resume(command, context()).await });
    gate(&fixture.policy.resume_entered).await;
    completed(
        original
            .cancel(id("cancel-before-approval"), &context())
            .await
            .unwrap(),
    );
    assert_eq!(
        fixture.outcome(&original).await.result.status(),
        RunStatus::Cancelled
    );
    fixture.policy.resume_release.add_permits(1);
    assert!(pending.await.unwrap().is_err());
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert!(
        fixture
            .saved(&original)
            .await
            .snapshot
            .resume_receipts
            .is_empty()
    );
}

async fn external_command(
    fixture: &Fixture,
    handle: &RunHandle,
    reference: RecordRef,
) -> ResumeCommand {
    let saved = fixture.saved(handle).await;
    let wait = saved.snapshot.wait.unwrap();
    assert!(matches!(wait.target, WaitTarget::External { .. }));
    ResumeCommand {
        run_id: handle.run_id().clone(),
        expected_revision: saved.snapshot.revision,
        command_id: id("external-result"),
        action: ResumeAction::External {
            wait_id: wait.wait_id,
            receipt_ref: reference,
        },
    }
}

#[tokio::test]
async fn external_receipts_require_current_access_and_host_verification_before_effect_resolution() {
    let fixture = Fixture::new(Mode::External);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    let waiting = fixture.outcome(&original).await;
    assert_eq!(waiting.result.status(), RunStatus::Waiting);
    let saved = fixture.saved(&original).await;
    let valid =
        external_command(&fixture, &original, fixture.store.proof.reference().clone()).await;
    fixture
        .policy
        .deny_receipt_read
        .store(true, Ordering::SeqCst);
    assert_eq!(
        agent
            .resume(valid.clone(), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(fixture.verifier.calls.load(Ordering::SeqCst), 0);
    fixture
        .policy
        .deny_receipt_read
        .store(false, Ordering::SeqCst);
    let mut wrong_ref = fixture.store.proof.reference().clone();
    wrong_ref.digest = canonical_digest(&json!("wrong-record"));
    assert!(
        agent
            .resume(
                external_command(&fixture, &original, wrong_ref).await,
                context()
            )
            .await
            .is_err()
    );
    assert_eq!(fixture.verifier.calls.load(Ordering::SeqCst), 0);
    let forged = external_command(
        &fixture,
        &original,
        fixture.store.forged.reference().clone(),
    )
    .await;
    assert_eq!(
        agent.resume(forged, context()).await.unwrap_err().code,
        ErrorCode::AccessDenied
    );
    assert_eq!(fixture.saved(&original).await.snapshot, saved.snapshot);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let mut reviewer = context();
    reviewer.data.principal_ref = id("effect-reviewer");
    let resumed = completed(agent.resume(valid.clone(), reviewer).await.unwrap());
    let outcome = fixture.outcome(&resumed).await;
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert!(outcome.unresolved_effects.is_empty());
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.outcome(&original).await, waiting);
    let requests = fixture.model.requests.lock().unwrap();
    let target_results: Vec<_> = requests[1]
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            ModelContent::ToolResult {
                provider_call_id,
                content,
            } if provider_call_id == &id("provider-1") => Some(content),
            _ => None,
        })
        .collect();
    assert_eq!(
        target_results,
        vec![
            &json!({"status":"succeeded","effect":"applied","content":[{"type":"json","value":"verified target"}]})
        ]
    );
}

#[tokio::test]
async fn an_external_verdict_that_remains_unknown_keeps_the_wait_and_command_unconsumed() {
    let fixture = Fixture::new(Mode::External);
    fixture.verifier.mode.store(1, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let before = fixture.saved(&original).await;
    let command =
        external_command(&fixture, &original, fixture.store.proof.reference().clone()).await;
    assert_eq!(
        agent
            .resume(command.clone(), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ToolEffectUnresolved
    );
    assert_eq!(fixture.saved(&original).await.snapshot, before.snapshot);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    fixture.verifier.mode.store(0, Ordering::SeqCst);
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&resumed).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_verified_write_with_invalid_output_keeps_the_applied_effect_and_original_receipt() {
    let fixture = Fixture::new(Mode::External);
    fixture.verifier.mode.store(2, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let command =
        external_command(&fixture, &original, fixture.store.proof.reference().clone()).await;
    let resumed = completed(agent.resume(command.clone(), context()).await.unwrap());
    fixture.outcome(&resumed).await;
    let saved = fixture.saved(&resumed).await;
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[1].state else {
        panic!("expected reconciled effect")
    };
    assert_eq!(result.status, ToolResultStatus::Failed);
    assert_eq!(result.effect, ToolEffect::Applied);
    let receipt = fixture
        .base
        .store
        .read_record(&scope(), result.effect_receipt_ref.as_ref().unwrap())
        .await
        .unwrap();
    assert_eq!(&receipt.value()["receipt"], fixture.store.proof.value());
    let replay = completed(agent.resume(command, context()).await.unwrap());
    fixture.outcome(&replay).await;
    assert_eq!(fixture.verifier.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_competing_denial_consumes_the_wait_before_a_paused_approval() {
    let fixture = Fixture::new(Mode::Approval);
    let first = fixture.agent();
    let second = fixture.agent();
    let original = fixture.started(&first).await;
    fixture.outcome(&original).await;
    let approval = fixture.approve(&original, "losing-approval").await;
    let mut denial = approval.clone();
    denial.command_id = id("winning-denial");
    let ResumeAction::Approve { wait_id, target } = denial.action else {
        unreachable!()
    };
    denial.action = ResumeAction::Deny {
        wait_id,
        target,
        reason: "Do not apply".into(),
    };
    fixture
        .policy
        .pause_next_resume
        .store(true, Ordering::SeqCst);
    let delayed = tokio::spawn(async move { first.resume(approval, context()).await });
    gate(&fixture.policy.resume_entered).await;
    let winner = completed(second.resume(denial.clone(), context()).await.unwrap());
    fixture.outcome(&winner).await;
    fixture.policy.resume_release.add_permits(1);
    assert!(delayed.await.unwrap().is_err());
    let saved = fixture.saved(&winner).await;
    assert_eq!(saved.snapshot.resume_receipts.len(), 1);
    assert_eq!(saved.snapshot.resume_receipts[0].command, denial);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(*fixture.order.lock().unwrap(), vec!["before", "after"]);
}

#[tokio::test]
async fn resume_rejects_replacement_profiles_and_changed_answer_contracts() {
    let fixture = Fixture::new(Mode::Input);
    let original_agent = fixture.agent();
    let handle = fixture.started(&original_agent).await;
    fixture.outcome(&handle).await;
    let saved = fixture.saved(&handle).await;
    let wait = saved.snapshot.wait.as_ref().unwrap();
    let mut command = ResumeCommand {
        run_id: handle.run_id().clone(),
        expected_revision: saved.snapshot.revision,
        command_id: id("answer-pinned"),
        action: ResumeAction::Input {
            wait_id: wait.wait_id.clone(),
            answer: json!(42),
        },
    };
    let mut changed_profile = fixture.profile.clone();
    changed_profile.version = id("2");
    let changed_agent = create_agent(changed_profile, fixture.bindings()).unwrap();
    let mut valid_answer = command.clone();
    if let ResumeAction::Input { answer, .. } = &mut valid_answer.action {
        *answer = json!({"selection":"annual"});
    }
    assert!(changed_agent.resume(valid_answer, context()).await.is_err());
    let mut bindings = fixture.bindings();
    let mut registrations = vec![];
    for name in ["before", "target", "after"] {
        let existing = fixture.registry.get(&id(name)).unwrap();
        let mut descriptor = existing.compiled.descriptor().clone();
        if name == "target" {
            descriptor.output_schema = json!({"type":"integer"});
        }
        registrations.push(ToolRegistration {
            compiled: SchemaCompiler::new()
                .compile(descriptor, &fixture.inputs)
                .unwrap(),
            executor: existing.executor.clone(),
        });
    }
    bindings.tools = Some(std::sync::Arc::new(
        ToolRegistry::new(scope(), registrations).unwrap(),
    ));
    let changed_contract = create_agent(fixture.profile.clone(), bindings).unwrap();
    assert!(
        changed_contract
            .resume(command.clone(), context())
            .await
            .is_err()
    );
    assert_eq!(fixture.saved(&handle).await.snapshot, saved.snapshot);
    if let ResumeAction::Input { answer, .. } = &mut command.action {
        *answer = json!({"selection":"quarterly"});
    }
    let resumed = completed(original_agent.resume(command, context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&resumed).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn expiry_and_cancellation_of_an_external_wait_preserve_the_uncertain_effect_without_verifying_it()
 {
    for expire in [false, true] {
        let mut fixture = Fixture::new(Mode::External);
        fixture.profile.limits.max_elapsed_ms = 100.try_into().unwrap();
        let agent = fixture.agent();
        let original = fixture.started(&agent).await;
        let waiting = fixture.outcome(&original).await;
        assert!(!waiting.unresolved_effects.is_empty());
        let outcome = if expire {
            let command =
                external_command(&fixture, &original, fixture.store.proof.reference().clone())
                    .await;
            tokio::time::advance(Duration::from_millis(101)).await;
            let resumed = completed(agent.resume(command, context()).await.unwrap());
            fixture.outcome(&resumed).await
        } else {
            completed(
                original
                    .cancel(id("stop-external-wait"), &context())
                    .await
                    .unwrap(),
            );
            fixture.outcome(&original).await
        };
        assert_eq!(
            outcome.result.status(),
            if expire {
                RunStatus::Exhausted
            } else {
                RunStatus::Cancelled
            }
        );
        assert_eq!(outcome.unresolved_effects, waiting.unresolved_effects);
        assert_eq!(outcome.usage.tool_attempts, waiting.usage.tool_attempts);
        assert_eq!(fixture.verifier.calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn a_verified_not_applied_result_settles_the_call_without_retrying_the_business_operation() {
    let fixture = Fixture::new(Mode::External);
    fixture.tools[1].apply_write.store(false, Ordering::SeqCst);
    fixture.verifier.mode.store(3, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let command =
        external_command(&fixture, &original, fixture.store.proof.reference().clone()).await;
    let resumed = completed(agent.resume(command.clone(), context()).await.unwrap());
    let outcome = fixture.outcome(&resumed).await;
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert!(outcome.unresolved_effects.is_empty());
    let saved = fixture.saved(&resumed).await;
    assert!(matches!(&saved.snapshot.tool_ledger[1].state,
        ToolCallState::Settled { result } if result.status == ToolResultStatus::Failed && result.effect == ToolEffect::NotApplied));
    let replay = completed(agent.resume(command, context()).await.unwrap());
    fixture.outcome(&replay).await;
    assert_eq!(fixture.verifier.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn an_expired_individual_wait_cannot_execute_even_while_the_run_has_time_remaining() {
    let fixture = Fixture::new(Mode::Approval);
    fixture.store.wait_timeout_ms.store(20, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    let waiting = fixture.outcome(&original).await;
    let saved = fixture.saved(&original).await;
    let expires = saved.snapshot.wait.as_ref().unwrap().expires_at_ms.unwrap();
    assert!(expires < saved.snapshot.timing.deadline_at_ms);
    let command = fixture.approve(&original, "expired-short-wait").await;
    tokio::time::advance(Duration::from_millis(30)).await;
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    let outcome = fixture.outcome(&resumed).await;
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    );
    assert!(outcome.usage.elapsed_ms < fixture.profile.limits.max_elapsed_ms.get());
    assert_eq!(outcome.usage.tool_attempts, waiting.usage.tool_attempts);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert!(fixture.saved(&resumed).await.snapshot.resume_receipts[0].expired);
}

#[tokio::test(start_paused = true)]
async fn an_approval_accepted_before_wait_expiry_keeps_running_after_that_old_deadline() {
    let fixture = Fixture::new(Mode::Approval);
    fixture.store.wait_timeout_ms.store(100, Ordering::SeqCst);
    fixture.model.hold_final.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    let waiting = fixture.outcome(&original).await;
    let command = fixture.approve(&original, "accepted-before-expiry").await;
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    gate(&fixture.model.final_entered).await;
    assert!(!fixture.saved(&resumed).await.snapshot.resume_receipts[0].expired);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_millis(150)).await;
    fixture.model.final_release.add_permits(1);
    let outcome = fixture.outcome(&resumed).await;
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(fixture.outcome(&original).await, waiting);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn historical_segment_outcomes_recheck_permission_after_the_protected_record_read() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let command = fixture.approve(&original, "approval").await;
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    fixture.outcome(&resumed).await;
    let saved = fixture.saved(&resumed).await;
    let previous_ref = saved.snapshot.resume_receipts[0]
        .previous_outcome_ref
        .clone();
    *fixture.store.pause_record.lock().unwrap() = Some(previous_ref);
    let checks_before = fixture.policy.details_checks.load(Ordering::SeqCst);
    let observer = tokio::spawn(async move { original.outcome(&context()).await });
    gate(&fixture.store.record_entered).await;
    // The first snapshot permission was granted; only the subsequent protected read is suspended.
    assert!(fixture.policy.details_checks.load(Ordering::SeqCst) > checks_before);
    fixture.policy.deny_details.store(true, Ordering::SeqCst);
    fixture.store.record_release.add_permits(1);
    assert_eq!(
        observer.await.unwrap().unwrap_err().code,
        ErrorCode::AccessDenied
    );
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}
```

## `crates/wickle/tests/context_projection.rs`

```rust
//! Pinned prompts, provenance, protected-input boundaries, and atomic context selection.

use serde_json::{Value, json};
use std::collections::BTreeSet;
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn versioned(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
fn record(value: &str) -> RecordRef {
    RecordRef {
        record_id: id(value),
        revision: 1,
        digest: canonical_digest(&json!(value)),
    }
}
fn compiled_tool(name: &str) -> CompiledTool {
    let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    SchemaCompiler::new().compile(ToolDescriptor {
        tool:versioned(name), name:id(name), description:format!("{name} records"),
        input_schema:json!({"type":"object","properties":{"query":{"type":"string"},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters:vec!["query".into()], system_bindings:None,
        output_schema:json!({"type":"string"}), side_effect:ToolSideEffect::ReadOnly,
        concurrency:ToolConcurrency::Serial, retry:ToolRetryPolicy::Never, reconcile:false,
        max_output_bytes:4096.try_into().unwrap(),
    }, &registry).unwrap()
}

struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(reference.version.clone().unwrap_or_else(|| id("1"))),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(
                    &json!({"id":reference.id,"version":reference.version}),
                ),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| reference.id.clone()),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

fn route() -> ResolvedModelRoute {
    ResolvedModelRoute {
        binding: versioned("model"),
        catalog_revision: id("catalog"),
        routing_policy_revision: id("policy"),
        requested_model: id("model"),
        model_id: id("model"),
        model_version: id("release"),
        version_semantics: VersionSemantics::Pinned,
        provider: id("provider"),
        target: JsonObject::new(),
        deployment_revision: None,
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        adapter: versioned("adapter"),
        capability_revision: id("capabilities"),
        connection_ref: versioned("connection"),
    }
}

struct Fixture {
    scope: Scope,
    profile: ResolvedProfile,
    prompt: PromptSnapshot,
    prompt_digest: JsonDigest,
    tools: Vec<CompiledTool>,
    skill: SkillManifest,
    request: RunRequest,
    run_id: Id,
    step_id: Id,
    request_message_id: Id,
}

impl Fixture {
    async fn new() -> Self {
        let scope = Scope {
            tenant_id: id("tenant"),
            workspace_id: id("workspace"),
            user_id: None,
        };
        let profile=AgentProfile::from_json(r#"{
            "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
            "name":"Assistant","description":"Context fixture","instructions":{"text":"Profile instructions"},
            "model_binding":"model","tools":[{"tool_id":"search","version":"1"},{"tool_id":"read","version":"1"}],
            "skills":[{"skill_id":"analysis","version":"1"}],"connectors":[],
            "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
            "limits":{"max_model_calls":4,"max_tool_attempts":4,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
        }"#).unwrap();
        let profile = ProfileValidator::new(&Catalog)
            .validate(&profile, &scope)
            .await
            .unwrap();
        let tools = vec![compiled_tool("search"), compiled_tool("read")];
        let skill = SkillManifest {
            skill: versioned("analysis"),
            name: "Analysis".into(),
            description: "Analyze evidence".into(),
            manifest_digest: canonical_digest(&json!("analysis manifest")),
        };
        let bindings = profile
            .profile()
            .tools
            .iter()
            .cloned()
            .zip(tools.iter().cloned())
            .map(|(selection, compiled)| PromptToolBinding {
                selection,
                compiled,
            })
            .collect();
        let prompt = PromptSnapshot::create(
            &profile,
            vec!["Host rule A".into(), "Host rule B".into()],
            None,
            bindings,
            vec![skill.clone()],
        )
        .unwrap();
        let prompt_digest = prompt.digest();
        Self {
            scope,
            profile,
            prompt,
            prompt_digest,
            tools,
            skill,
            request: RunRequest {
                request_id: id("user-request"),
                session_id: id("session"),
                input: vec![InputContent::Text {
                    text: "Current requested work".into(),
                }],
                trigger: RunTrigger::User {},
                model_options: JsonObject::new(),
                output_contract: None,
            },
            run_id: id("current-run"),
            step_id: id("step"),
            request_message_id: id("current-message"),
        }
    }
    fn current_message(&self, sequence: u64) -> Message {
        Message {
            message_id: self.request_message_id.clone(),
            run_id: self.run_id.clone(),
            sequence: sequence.try_into().unwrap(),
            role: MessageRole::User,
            content: self
                .request
                .input
                .iter()
                .cloned()
                .map(|content| ContentBlock::Content { content })
                .collect(),
            origin: MessageOrigin::User,
            visibility: Visibility::UserAndModel,
        }
    }
    fn input<'a>(
        &'a self,
        transcript: &'a [Message],
        items: &'a [ContextItem],
        opaque: &'a [ScopedOpaque],
    ) -> ProjectionInput<'a> {
        ProjectionInput {
            profile: &self.profile,
            scope: &self.scope,
            run_id: &self.run_id,
            model_step_id: &self.step_id,
            current_request: &self.request,
            current_request_message_id: &self.request_message_id,
            transcript,
            context_items: items,
            opaque_records: opaque,
            expected_prompt_digest: &self.prompt_digest,
            request_id: self.step_id.clone(),
            purpose: ModelPurpose::Agent,
            route: route(),
            output: ModelOutput::Text {},
            max_output_tokens: 128.try_into().unwrap(),
            options: JsonObject::new(),
            response_limits: ModelResponseLimits {
                max_input_bytes: 100_000,
                max_response_bytes: 4096,
                max_delta_bytes: 1024,
                max_events: 32,
                max_tool_calls: 4,
            },
            limits: ProjectionLimits {
                max_bytes: 100_000,
                max_items: 100,
            },
        }
    }
    fn item(&self, name: &str, origin: ContextOrigin, priority: ContextPriority) -> ContextItem {
        ContextItem::new(
            id(name),
            origin,
            versioned(name),
            self.scope.clone(),
            vec![InputContent::Text {
                text: format!("data for {name}"),
            }],
            ContextLifetime::Run {
                run_id: self.run_id.clone(),
            },
            priority,
        )
    }
}

fn message(
    run: &str,
    sequence: u64,
    role: MessageRole,
    origin: MessageOrigin,
    content: Vec<ContentBlock>,
) -> Message {
    Message {
        message_id: id(&format!("message-{sequence}")),
        run_id: id(run),
        sequence: sequence.try_into().unwrap(),
        role,
        origin,
        content,
        visibility: Visibility::UserAndModel,
    }
}

#[tokio::test]
async fn host_model_options_reach_the_port_without_becoming_prompt_content() {
    use std::{sync::Mutex, time::Duration};
    struct ObserveOptions(Mutex<Option<JsonObject>>);
    impl ModelPort for ObserveOptions {
        fn binding(&self) -> ModelPortBinding {
            let route = route();
            ModelPortBinding {
                provider: route.provider,
                adapter: route.adapter,
                connection_ref: route.connection_ref,
            }
        }
        fn generate<'a>(
            &'a self,
            request: &'a ModelRequest,
            _: &'a ModelCallContext,
        ) -> PortStream<'a, ModelEvent> {
            *self.0.lock().unwrap() = Some(request.options.clone());
            Box::pin(futures_util::stream::iter([Ok(
                ModelEvent::ResponseCompleted {
                    finish: ModelFinish::Stop,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![],
                },
            )]))
        }
    }
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let assembler = ContextAssembler::new();
    let baseline = assembler
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let options = object(json!({"reasoning_effort":"high", "provider_mode":{"budget":32}}));
    let mut input = fixture.input(&transcript, &[], &[]);
    input.options = options.clone();
    let projected = assembler.project(&fixture.prompt, input).unwrap();
    assert_eq!(projected.request.messages, baseline.request.messages);
    assert_eq!(projected.request.tools, baseline.request.tools);
    assert_ne!(projected.request.digest(), baseline.request.digest());
    let port = ObserveOptions(Mutex::new(None));
    let call = ModelCallContext {
        attempt_id: id("attempt"),
        run_id: fixture.run_id.clone(),
        scope: fixture.scope.clone(),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(1),
    };
    collect_model_response(&projected.request, port.generate(&projected.request, &call))
        .await
        .unwrap();
    assert_eq!(*port.0.lock().unwrap(), Some(options.clone()));
    let mut limited = fixture.input(&transcript, &[], &[]);
    limited.options = options;
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len();
    assert_eq!(
        assembler
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}
fn text(value: &str) -> ContentBlock {
    ContentBlock::Content {
        content: InputContent::Text { text: value.into() },
    }
}
fn round(run: &str, sequence: u64, label: &str, body: &str, tool: &CompiledTool) -> Vec<Message> {
    let call_message = id(&format!("message-{sequence}"));
    let call = ToolCall {
        call_id: id(label),
        model_request_id: id(&format!("request-{label}")),
        provider_call_id: id(&format!("provider-{label}")),
        tool_name: id("search"),
        model_inputs: object(json!({"query":label})),
        descriptor_digest: Some(tool.descriptor_digest().clone()),
        bound_input_ref: Some(record("bound-private-input")),
    };
    let result = ToolResult {
        call_id: call.call_id.clone(),
        call_message_id: call_message,
        status: ToolResultStatus::Failed,
        effect: ToolEffect::NotApplied,
        content: vec![InputContent::Text { text: body.into() }],
        effect_receipt_ref: Some(record("private-effect-receipt")),
        error: Some(Failure {
            code: id("unavailable"),
            diagnostic_ref: Some(record("private-diagnostic")),
        }),
    };
    vec![
        message(
            run,
            sequence,
            MessageRole::Assistant,
            MessageOrigin::Model,
            vec![ContentBlock::ToolCall { call }],
        ),
        message(
            run,
            sequence + 1,
            MessageRole::Tool,
            MessageOrigin::Tool,
            vec![ContentBlock::ToolResult { result }],
        ),
    ]
}

#[tokio::test]
async fn host_profile_and_skill_prefixes_are_pinned_and_tools_use_only_compiled_model_schemas() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert_eq!(projected.prompt_digest, fixture.prompt_digest);
    assert_eq!(
        projected.request.messages[0],
        ModelMessage {
            role: ModelRole::System,
            content: vec![
                ModelContent::Text {
                    text: "Host rule A".into()
                },
                ModelContent::Text {
                    text: "Host rule B".into()
                }
            ]
        }
    );
    assert_eq!(
        projected.request.messages[1],
        ModelMessage {
            role: ModelRole::System,
            content: vec![ModelContent::Text {
                text: "Profile instructions".into()
            }]
        }
    );
    assert_eq!(
        projected.request.tools,
        fixture
            .tools
            .iter()
            .map(CompiledTool::to_model_tool)
            .collect::<Vec<_>>()
    );
    for tool in &projected.request.tools {
        assert!(
            tool.model_input_schema["properties"]
                .get("workspace_id")
                .is_none()
        );
    }
    assert_eq!(
        projected.selected_message_ids,
        vec![fixture.request_message_id.clone()]
    );
    projected.request.validate().unwrap();
    let restored = PromptSnapshot::restore(
        &serde_json::to_string(&fixture.prompt).unwrap(),
        &fixture.prompt_digest,
        &fixture.profile,
        &fixture.scope,
    )
    .unwrap();
    let repeated = ContextAssembler::new()
        .project(&restored, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert_eq!(projected.request, repeated.request);
}

#[tokio::test]
async fn current_request_is_exactly_the_persisted_message_and_is_never_silently_replaced() {
    let fixture = Fixture::new().await;
    let current = fixture.current_message(1);
    for transcript in [
        vec![],
        vec![Message {
            content: vec![text("A different request")],
            ..current.clone()
        }],
        vec![Message {
            visibility: Visibility::Internal,
            ..current.clone()
        }],
        vec![Message {
            run_id: id("other-run"),
            ..current.clone()
        }],
    ] {
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
    let transcript = vec![current];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert_eq!(
        projected.request.messages.last().unwrap(),
        &ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Current requested work".into()
            }]
        }
    );
    assert_eq!(
        projected
            .selected_message_ids
            .iter()
            .filter(|message_id| *message_id == &fixture.request_message_id)
            .count(),
        1
    );
}

#[tokio::test]
async fn tool_projection_keeps_model_arguments_and_public_observations_without_execution_records() {
    let fixture = Fixture::new().await;
    let mut transcript = vec![fixture.current_message(1)];
    transcript.extend(round(
        "current-run",
        2,
        "call",
        "public observation",
        &fixture.tools[0],
    ));
    let mut internal = message(
        "current-run",
        4,
        MessageRole::User,
        MessageOrigin::Recovery,
        vec![ContentBlock::Content {
            content: InputContent::Json {
                value: json!({"system_inputs":{"workspace_id":"11111111-1111-4111-8111-111111111111"},"execution_args":{"query":"call","workspace_id":"11111111-1111-4111-8111-111111111111"},"raw_diagnostic":"private"}),
            },
        }],
    );
    internal.visibility = Visibility::Internal;
    transcript.push(internal);
    let original = transcript.clone();
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let contents: Vec<_> = projected
        .request
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .collect();
    let call = contents
        .iter()
        .find_map(|content| match content {
            ModelContent::ToolCall {
                provider_call_id,
                name,
                arguments,
            } => Some((provider_call_id, name, arguments)),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        call,
        (
            &id("provider-call"),
            &id("search"),
            &object(json!({"query":"call"}))
        )
    );
    let result = contents
        .iter()
        .find_map(|content| match content {
            ModelContent::ToolResult {
                provider_call_id,
                content,
            } => Some((provider_call_id, content)),
            _ => None,
        })
        .unwrap();
    assert_eq!(result.0, &id("provider-call"));
    assert_eq!(
        result.1,
        &json!({"status":"failed","effect":"not_applied","content":[{"type":"text","text":"public observation"}],"error":{"code":"unavailable"}})
    );
    assert_eq!(projected.request.messages.len(), 6);
    assert_eq!(
        projected.selected_message_ids,
        vec![
            fixture.request_message_id.clone(),
            id("message-2"),
            id("message-3")
        ]
    );
    assert_eq!(transcript, original);
    projected.request.validate().unwrap();
}

#[tokio::test]
async fn explicit_unknown_correction_projects_one_confirmed_result_without_changing_history() {
    let fixture = Fixture::new().await;
    let mut transcript = vec![fixture.current_message(1)];
    transcript.extend(round(
        "current-run",
        2,
        "call",
        "uncertain",
        &fixture.tools[0],
    ));
    let ContentBlock::ToolResult { result } = &mut transcript[2].content[0] else {
        unreachable!()
    };
    result.status = ToolResultStatus::Unknown;
    result.effect = ToolEffect::Unknown;
    let prior_digest = canonical_digest(&serde_json::to_value(&*result).unwrap());
    let confirmed = ToolResult {
        status: ToolResultStatus::Succeeded,
        effect: ToolEffect::Applied,
        content: vec![InputContent::Text {
            text: "confirmed receipt".into(),
        }],
        error: None,
        ..result.clone()
    };
    transcript.push(message(
        "current-run",
        4,
        MessageRole::Tool,
        MessageOrigin::Tool,
        vec![ContentBlock::ToolResultCorrection {
            previous_message_id: id("message-3"),
            previous_result_digest: prior_digest,
            result: confirmed,
        }],
    ));
    let original = transcript.clone();
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let results: Vec<_> = projected
        .request
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| {
            if let ModelContent::ToolResult {
                provider_call_id,
                content,
            } = content
            {
                Some((provider_call_id, content))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        results,
        vec![(
            &id("provider-call"),
            &json!({"status":"succeeded","effect":"applied","content":[{"type":"text","text":"confirmed receipt"}]})
        )]
    );
    assert!(projected.selected_message_ids.contains(&id("message-4")));
    assert!(!projected.selected_message_ids.contains(&id("message-3")));
    assert_eq!(transcript, original);
    projected.request.validate().unwrap();

    for fault in 0..7 {
        let mut changed = transcript.clone();
        match fault {
            0 => {
                let ContentBlock::ToolResultCorrection {
                    previous_result_digest,
                    ..
                } = &mut changed[3].content[0]
                else {
                    unreachable!()
                };
                *previous_result_digest = canonical_digest(&json!("different"));
            }
            1 => {
                let ContentBlock::ToolResultCorrection { result, .. } = &mut changed[3].content[0]
                else {
                    unreachable!()
                };
                result.call_id = id("foreign-call");
            }
            2 => changed[3].origin = MessageOrigin::User,
            3 => {
                let ContentBlock::ToolResult { result } = &mut changed[2].content[0] else {
                    unreachable!()
                };
                result.status = ToolResultStatus::Succeeded;
                result.effect = ToolEffect::Applied;
                let digest = canonical_digest(&serde_json::to_value(&*result).unwrap());
                let ContentBlock::ToolResultCorrection {
                    previous_result_digest,
                    ..
                } = &mut changed[3].content[0]
                else {
                    unreachable!()
                };
                *previous_result_digest = digest;
            }
            4 => {
                let mut duplicate = changed[3].clone();
                duplicate.message_id = id("second-correction");
                duplicate.sequence = 5.try_into().unwrap();
                changed.push(duplicate);
            }
            5 => {
                changed[3].sequence = 5.try_into().unwrap();
                changed.insert(
                    3,
                    message(
                        "current-run",
                        4,
                        MessageRole::Assistant,
                        MessageOrigin::Model,
                        vec![text("premature continuation")],
                    ),
                );
            }
            6 => {
                let ContentBlock::ToolResult { result } = &mut changed[2].content[0] else {
                    unreachable!()
                };
                result.effect = ToolEffect::Applied;
                let digest = canonical_digest(&serde_json::to_value(&*result).unwrap());
                let ContentBlock::ToolResultCorrection {
                    previous_result_digest,
                    result,
                    ..
                } = &mut changed[3].content[0]
                else {
                    unreachable!()
                };
                *previous_result_digest = digest;
                result.effect = ToolEffect::NotApplied;
            }
            _ => unreachable!(),
        }
        let error = ContextAssembler::new()
            .project(&fixture.prompt, fixture.input(&changed, &[], &[]))
            .unwrap_err();
        assert_eq!(error.path, "transcript.tool_correction", "fault {fault}");
    }
}

#[tokio::test]
async fn data_context_never_adds_system_authority_or_replaces_the_pinned_prefix() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let items = vec![
        fixture.item(
            "retrieval",
            ContextOrigin::Retrieval,
            ContextPriority::Required,
        ),
        fixture.item("memory", ContextOrigin::Memory, ContextPriority::Required),
        fixture.item(
            "verification",
            ContextOrigin::Verification,
            ContextPriority::Required,
        ),
    ];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &items, &[]))
        .unwrap();
    assert_eq!(projected.selected_context_ids.len(), 3);
    let system = |request: ModelRequest| {
        request
            .messages
            .into_iter()
            .filter(|message| message.role == ModelRole::System)
            .collect::<Vec<_>>()
    };
    assert_eq!(system(projected.request), system(baseline.request));
    let mut forged = serde_json::to_value(&items[0]).unwrap();
    forged["origin"] = json!("host");
    assert!(serde_json::from_value::<ContextItem>(forged).is_err());
}

#[tokio::test]
async fn context_items_validate_scope_and_digest_and_exclude_other_run_or_step_lifetimes() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let original = fixture.item(
        "source",
        ContextOrigin::Retrieval,
        ContextPriority::Required,
    );
    let mut wrong_scope = original.clone();
    wrong_scope.scope.tenant_id = id("other-tenant");
    let mut wrong_run = original.clone();
    wrong_run.lifetime = ContextLifetime::Run {
        run_id: id("other-run"),
    };
    let mut wrong_step = original.clone();
    wrong_step.lifetime = ContextLifetime::Step {
        run_id: fixture.run_id.clone(),
        model_step_id: id("other-step"),
    };
    let mut changed_content = original;
    changed_content.content = vec![InputContent::Text {
        text: "changed data".into(),
    }];
    let with_valid_digest = |item: ContextItem| {
        ContextItem::new(
            item.item_id,
            item.origin,
            item.source_ref,
            item.scope,
            item.content,
            item.lifetime,
            item.priority_class,
        )
    };
    for item in [with_valid_digest(wrong_scope), changed_content] {
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[item], &[]))
                .is_err()
        );
    }
    for item in [with_valid_digest(wrong_run), with_valid_digest(wrong_step)] {
        let items = vec![item];
        let projected = ContextAssembler::new()
            .project(&fixture.prompt, fixture.input(&transcript, &items, &[]))
            .unwrap();
        assert!(projected.selected_context_ids.is_empty());
        assert_eq!(projected.dropped_context_ids, vec![id("source")]);
    }
}

#[tokio::test]
async fn a_current_tool_round_cannot_be_incomplete_or_have_an_unpaired_result() {
    let fixture = Fixture::new().await;
    let complete = round("current-run", 2, "call", "observation", &fixture.tools[0]);
    for transcript in [
        vec![fixture.current_message(1), complete[0].clone()],
        vec![fixture.current_message(1), complete[1].clone()],
        vec![
            fixture.current_message(1),
            complete[0].clone(),
            message(
                "current-run",
                3,
                MessageRole::User,
                MessageOrigin::User,
                vec![text("interleaved")],
            ),
            Message {
                sequence: 4.try_into().unwrap(),
                ..complete[1].clone()
            },
        ],
    ] {
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
}

#[tokio::test]
async fn raw_transcript_system_roles_cannot_extend_or_replace_the_pinned_prefix() {
    let fixture = Fixture::new().await;
    for origin in [
        MessageOrigin::Host,
        MessageOrigin::Profile,
        MessageOrigin::Retrieval,
        MessageOrigin::Memory,
    ] {
        let transcript = vec![
            fixture.current_message(1),
            message(
                "current-run",
                2,
                MessageRole::System,
                origin,
                vec![text("untrusted transcript instructions")],
            ),
        ];
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
}

#[tokio::test]
async fn visibility_filtering_and_message_references_cannot_separate_a_tool_call_from_its_result() {
    let fixture = Fixture::new().await;
    for hidden in [0, 1] {
        let mut messages = round("current-run", 2, "call", "observation", &fixture.tools[0]);
        messages[hidden].visibility = Visibility::Internal;
        let mut transcript = vec![fixture.current_message(1)];
        transcript.extend(messages);
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
    let mut messages = round("current-run", 2, "call", "observation", &fixture.tools[0]);
    let ContentBlock::ToolResult { result } = &mut messages[1].content[0] else {
        unreachable!()
    };
    result.call_message_id = id("some-other-call-message");
    let mut transcript = vec![fixture.current_message(1)];
    transcript.extend(messages);
    assert!(
        ContextAssembler::new()
            .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
            .is_err()
    );
}

#[tokio::test]
async fn bounded_selection_discards_an_older_run_as_a_whole_and_retains_the_latest_complete_round()
{
    let fixture = Fixture::new().await;
    let large = "old ".repeat(512);
    let mut transcript = vec![message(
        "old-run",
        1,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Old work")],
    )];
    transcript.extend(round("old-run", 2, "old-call", &large, &fixture.tools[0]));
    transcript.push(message(
        "old-run",
        4,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Old answer")],
    ));
    transcript.push(message(
        "recent-run",
        5,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Recent work")],
    ));
    transcript.extend(round(
        "recent-run",
        6,
        "recent-call",
        "Recent observation",
        &fixture.tools[0],
    ));
    transcript.push(message(
        "recent-run",
        8,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Recent answer")],
    ));
    transcript.push(fixture.current_message(9));
    let original = transcript.clone();
    let full = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let full_bytes = serde_json::to_vec(&full.request).unwrap().len();
    let mut bounded = fixture.input(&transcript, &[], &[]);
    bounded.limits.max_bytes = full_bytes - large.len() / 2;
    let byte_limit = bounded.limits.max_bytes;
    let selected = ContextAssembler::new()
        .project(&fixture.prompt, bounded)
        .unwrap();
    assert_eq!(
        selected.selected_message_ids,
        vec![
            id("message-5"),
            id("message-6"),
            id("message-7"),
            id("message-8"),
            fixture.request_message_id.clone()
        ]
    );
    assert_eq!(
        selected.dropped_message_ids,
        vec![
            id("message-1"),
            id("message-2"),
            id("message-3"),
            id("message-4")
        ]
    );
    assert!(serde_json::to_vec(&selected.request).unwrap().len() <= byte_limit);
    selected.request.validate().unwrap();
    assert_eq!(transcript, original);
}

#[tokio::test]
async fn required_current_work_and_prefix_report_budget_exhaustion_instead_of_truncation() {
    let fixture = Fixture::new().await;
    let current = vec![fixture.current_message(1)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&current, &[], &[]))
        .unwrap();
    let count = baseline
        .request
        .messages
        .iter()
        .map(|message| message.content.len())
        .sum::<usize>()
        + baseline.request.tools.len();
    let mut item_limited = fixture.input(&current, &[], &[]);
    item_limited.limits.max_items = count - 1;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, item_limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
    let mut input_limited = fixture.input(&current, &[], &[]);
    input_limited.response_limits.max_input_bytes = 1;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, input_limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
    let mut transcript = current;
    transcript.extend(round(
        "current-run",
        2,
        "current-call",
        &"data ".repeat(400),
        &fixture.tools[0],
    ));
    let mut bounded = fixture.input(&transcript, &[], &[]);
    bounded.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 64;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, bounded)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn optional_context_can_be_removed_but_required_context_cannot_be_silently_dropped() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let mut optional = fixture.item(
        "optional",
        ContextOrigin::Retrieval,
        ContextPriority::Optional,
    );
    optional = ContextItem::new(
        optional.item_id,
        optional.origin,
        optional.source_ref,
        optional.scope,
        vec![InputContent::Text {
            text: "reference data ".repeat(1000),
        }],
        optional.lifetime,
        optional.priority_class,
    );
    let items = vec![optional.clone()];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let mut limited = fixture.input(&transcript, &items, &[]);
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 128;
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, limited)
        .unwrap();
    assert!(projected.selected_context_ids.is_empty());
    assert_eq!(projected.dropped_context_ids, vec![id("optional")]);
    let required = ContextItem::new(
        optional.item_id,
        optional.origin,
        optional.source_ref,
        optional.scope,
        optional.content,
        optional.lifetime,
        ContextPriority::Required,
    );
    let required_items = vec![required];
    let mut limited = fixture.input(&transcript, &required_items, &[]);
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 128;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn pinned_snapshot_rejects_other_scope_profile_and_recomputed_replacement_contents() {
    let fixture = Fixture::new().await;
    let mut foreign = fixture.scope.clone();
    foreign.tenant_id = id("other-tenant");
    assert!(
        fixture
            .prompt
            .validate_for(&fixture.profile, &foreign, &fixture.prompt_digest)
            .is_err()
    );
    let mut changed_profile = fixture.profile.profile().clone();
    changed_profile.version = id("2.0.0");
    let changed_profile = ProfileValidator::new(&Catalog)
        .validate(&changed_profile, &fixture.scope)
        .await
        .unwrap();
    assert!(
        fixture
            .prompt
            .validate_for(&changed_profile, &fixture.scope, &fixture.prompt_digest)
            .is_err()
    );
    let bindings = fixture
        .profile
        .profile()
        .tools
        .iter()
        .cloned()
        .zip(fixture.tools.iter().cloned())
        .map(|(selection, compiled)| PromptToolBinding {
            selection,
            compiled,
        })
        .collect();
    let replacement = PromptSnapshot::create(
        &fixture.profile,
        vec!["Replacement Host policy".into()],
        None,
        bindings,
        vec![fixture.skill.clone()],
    )
    .unwrap();
    assert!(
        replacement
            .validate_for(&fixture.profile, &fixture.scope, &fixture.prompt_digest)
            .is_err()
    );
    assert!(
        PromptSnapshot::restore(
            &serde_json::to_string(&replacement).unwrap(),
            &fixture.prompt_digest,
            &fixture.profile,
            &fixture.scope
        )
        .is_err()
    );
}

#[tokio::test]
async fn prompt_creation_rejects_unselected_tools_and_changed_skill_versions() {
    let fixture = Fixture::new().await;
    let mut extra = fixture
        .profile
        .profile()
        .tools
        .iter()
        .cloned()
        .zip(fixture.tools.iter().cloned())
        .map(|(selection, compiled)| PromptToolBinding {
            selection,
            compiled,
        })
        .collect::<Vec<_>>();
    extra.push(PromptToolBinding {
        selection: ToolBindingRef::Catalog(CatalogToolRef {
            tool_id: id("unselected"),
            version: id("1"),
            bindings: None,
            config: None,
        }),
        compiled: compiled_tool("unselected"),
    });
    assert!(
        PromptSnapshot::create(
            &fixture.profile,
            vec!["Host rule".into()],
            None,
            extra,
            vec![fixture.skill.clone()]
        )
        .is_err()
    );
    let bindings = fixture
        .profile
        .profile()
        .tools
        .iter()
        .cloned()
        .zip(fixture.tools.iter().cloned())
        .map(|(selection, compiled)| PromptToolBinding {
            selection,
            compiled,
        })
        .collect();
    let mut changed = fixture.skill.clone();
    changed.skill.version = id("2");
    assert!(
        PromptSnapshot::create(
            &fixture.profile,
            vec!["Host rule".into()],
            None,
            bindings,
            vec![changed]
        )
        .is_err()
    );
}

#[tokio::test]
async fn opaque_replay_requires_matching_protected_record_scope_provider_and_route() {
    let fixture = Fixture::new().await;
    let continuation =
        OpaqueContinuation::new(&route(), json!({"signature":"provider continuation"}));
    let reference = RecordRef {
        record_id: id("opaque"),
        revision: 1,
        digest: canonical_digest(&serde_json::to_value(&continuation).unwrap()),
    };
    let mut transcript = vec![fixture.current_message(1)];
    transcript.push(message(
        "current-run",
        2,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![ContentBlock::ProviderOpaque {
            provider: id("provider"),
            route_digest: route().digest(),
            data_ref: reference.clone(),
        }],
    ));
    let records = vec![ScopedOpaque {
        scope: fixture.scope.clone(),
        reference: reference.clone(),
        provider: id("provider"),
        continuation: continuation.clone(),
    }];
    let accepted = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &records))
        .unwrap();
    assert!(accepted.request.messages.iter().flat_map(|message|&message.content).any(|content|matches!(content,ModelContent::Opaque{continuation:found} if found==&continuation)));
    let mut changed = fixture.input(&transcript, &[], &records);
    changed.route.connection_ref.version = id("different-connection");
    assert!(
        ContextAssembler::new()
            .project(&fixture.prompt, changed)
            .is_err()
    );
    let mut wrong_scope = records.clone();
    wrong_scope[0].scope.tenant_id = id("other-tenant");
    assert!(
        ContextAssembler::new()
            .project(
                &fixture.prompt,
                fixture.input(&transcript, &[], &wrong_scope)
            )
            .is_err()
    );
    let mut wrong_provider = records.clone();
    wrong_provider[0].provider = id("other-provider");
    assert!(
        ContextAssembler::new()
            .project(
                &fixture.prompt,
                fixture.input(&transcript, &[], &wrong_provider)
            )
            .is_err()
    );
    let mut wrong_record = records;
    wrong_record[0].reference = record("different-record");
    assert!(
        ContextAssembler::new()
            .project(
                &fixture.prompt,
                fixture.input(&transcript, &[], &wrong_record)
            )
            .is_err()
    );
}

#[tokio::test]
async fn the_latest_tool_round_from_an_earlier_run_is_required_context() {
    let fixture = Fixture::new().await;
    let current = vec![fixture.current_message(5)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&current, &[], &[]))
        .unwrap();
    let mut transcript = vec![message(
        "previous-run",
        1,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Previous work")],
    )];
    transcript.extend(round(
        "previous-run",
        2,
        "previous-call",
        &"data ".repeat(400),
        &fixture.tools[0],
    ));
    transcript.push(message(
        "previous-run",
        4,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Previous answer")],
    ));
    transcript.extend(current);
    let unbounded = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert!(unbounded.selected_message_ids.contains(&id("message-2")));
    assert!(unbounded.selected_message_ids.contains(&id("message-3")));
    let mut limited = fixture.input(&transcript, &[], &[]);
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 64;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn an_older_unknown_effect_is_not_dropped_when_a_newer_round_exists() {
    let fixture = Fixture::new().await;
    let large = "unknown effect ".repeat(128);
    let mut transcript = vec![message(
        "unknown-run",
        1,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Earlier work")],
    )];
    let mut uncertain = round("unknown-run", 2, "unknown-call", &large, &fixture.tools[0]);
    let ContentBlock::ToolResult { result } = &mut uncertain[1].content[0] else {
        unreachable!()
    };
    result.status = ToolResultStatus::Unknown;
    result.effect = ToolEffect::Unknown;
    transcript.extend(uncertain);
    transcript.push(message(
        "unknown-run",
        4,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Effect not confirmed")],
    ));
    transcript.push(message(
        "recent-run",
        5,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Recent work")],
    ));
    transcript.extend(round(
        "recent-run",
        6,
        "recent-call",
        "Recent observation",
        &fixture.tools[0],
    ));
    transcript.push(message(
        "recent-run",
        8,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Recent answer")],
    ));
    transcript.push(fixture.current_message(9));
    let full = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert!(full.selected_message_ids.contains(&id("message-3")));
    let mut limited = fixture.input(&transcript, &[], &[]);
    limited.limits.max_bytes = serde_json::to_vec(&full.request).unwrap().len() - large.len() / 2;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn loaded_skill_context_uses_its_pinned_version_without_gaining_system_authority() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let loaded = ContextItem::new(
        id("loaded-skill"),
        ContextOrigin::Skill,
        fixture.skill.skill.clone(),
        fixture.scope.clone(),
        vec![InputContent::Text {
            text: "Loaded task instructions".into(),
        }],
        ContextLifetime::Run {
            run_id: fixture.run_id.clone(),
        },
        ContextPriority::Required,
    );
    let items = vec![loaded.clone()];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &items, &[]))
        .unwrap();
    assert_eq!(projected.selected_context_ids, vec![id("loaded-skill")]);
    let system = |request: ModelRequest| {
        request
            .messages
            .into_iter()
            .filter(|message| message.role == ModelRole::System)
            .collect::<Vec<_>>()
    };
    assert_eq!(system(projected.request), system(baseline.request));
    let changed = ContextItem::new(
        loaded.item_id,
        loaded.origin,
        VersionedRef {
            id: loaded.source_ref.id,
            version: id("2"),
        },
        loaded.scope,
        loaded.content,
        loaded.lifetime,
        loaded.priority_class,
    );
    assert!(
        ContextAssembler::new()
            .project(&fixture.prompt, fixture.input(&transcript, &[changed], &[]))
            .is_err()
    );
}
```

## `crates/wickle/tests/contracts.rs`

```rust
//! Behavioral checks for validation, persisted contracts, and profile identity.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn digest(value: &str) -> JsonDigest {
    canonical_digest(&json!(value))
}
fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    }
}
fn reference(kind: ComponentKind, name: &str) -> ComponentRef {
    ComponentRef {
        kind,
        id: id(name),
        version: if kind == ComponentKind::ModelBinding || kind == ComponentKind::Extension {
            None
        } else {
            Some(id("1.0.0"))
        },
    }
}

fn profile_value() -> Value {
    json!({
        "schema_version": "wickle.agent-profile.v1", "agent_id": "research", "version": "1.0.0",
        "name": "Research assistant", "description": "Find information with sources",
        "instructions": {"text": "Use available evidence."}, "model_binding": "primary",
        "tools": [{"tool_id": "documents.search", "version": "1.0.0", "bindings": {"main": "knowledge"}, "config": {"limit": 5}}],
        "skills": [], "connectors": [{"binding_id": "knowledge", "connector_id": "document-store", "version": "1.0.0"}],
        "context_policy": {"strategy": "bounded"}, "output_contract": {"type": "text"},
        "limits": {"max_model_calls": 8, "max_tool_attempts": 12, "max_repair_attempts": 0, "max_recovery_attempts": 2, "max_elapsed_ms": 30000}
    })
}
fn profile() -> AgentProfile {
    AgentProfile::from_json(&profile_value().to_string()).unwrap()
}

struct Catalog(BTreeMap<ComponentRef, ComponentMetadata>);

impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        requested_scope: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            if requested_scope != &scope() {
                return Err(ContractError::new(ErrorCode::ComponentUnavailable, "scope"));
            }
            self.0
                .get(reference)
                .cloned()
                .ok_or_else(|| ContractError::new(ErrorCode::ComponentUnavailable, "reference"))
        })
    }
}

fn metadata(key: &ComponentRef) -> ComponentMetadata {
    let mut resolved = key.clone();
    resolved.version = Some(id("1.0.0"));
    ComponentMetadata {
        reference: resolved,
        contract_version: 1,
        manifest_digest: digest("manifest"),
        config_schema: json!({"type":"object","additionalProperties":false}),
        dependencies: vec![],
        capabilities: BTreeSet::new(),
        required_capabilities: BTreeSet::new(),
        required_connections: BTreeSet::new(),
        model_name: None,
        hook_position: None,
        exports: vec![],
    }
}

fn catalog() -> Catalog {
    let mut definitions = BTreeMap::new();
    let model = reference(ComponentKind::ModelBinding, "primary");
    let mut model_meta = metadata(&model);
    model_meta.capabilities.insert(id("model.tool_calling"));
    definitions.insert(model, model_meta);
    let connector = reference(ComponentKind::Connector, "document-store");
    definitions.insert(connector.clone(), metadata(&connector));
    let tool = reference(ComponentKind::Tool, "documents.search");
    let mut tool_meta = metadata(&tool);
    tool_meta.config_schema = json!({"type":"object","properties":{"limit":{"type":"integer","minimum":1,"maximum":50}},"required":["limit"],"additionalProperties":false});
    tool_meta.required_connections.insert(id("main"));
    tool_meta
        .required_capabilities
        .insert(id("model.tool_calling"));
    tool_meta.capabilities.insert(id("documents.search"));
    tool_meta.model_name = Some(id("documents_search"));
    definitions.insert(tool, tool_meta);
    Catalog(definitions)
}

async fn resolved() -> ResolvedProfile {
    ProfileValidator::new(&catalog())
        .validate(&profile(), &scope())
        .await
        .unwrap()
}

#[test]
fn digest_matches_independent_sha256_vectors_and_sorts_nested_objects() {
    assert_eq!(
        canonical_digest_json("{}").unwrap().as_str(),
        "sorted-json-v1:sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
    );
    let a = r#"{"z":1,"a":{"x":[3,1],"b":2}}"#;
    let b = r#"{ "a": {"b":2,"x":[3,1]}, "z":1 }"#;
    let actual = canonical_digest_json(a).unwrap();
    assert_eq!(
        actual.as_str(),
        "sorted-json-v1:sha256:b2db09df32697403c319dfb8cd57f51a8400eb9943753795ccbb34d1383f01a2"
    );
    assert_eq!(actual, canonical_digest_json(b).unwrap());
    for changed in [
        r#"{"z":1,"a":{"x":[1,3],"b":2}}"#,
        r#"{"z":2,"a":{"x":[3,1],"b":2}}"#,
    ] {
        assert_ne!(actual, canonical_digest_json(changed).unwrap());
    }
    assert_ne!(
        canonical_digest_json("1").unwrap(),
        canonical_digest_json("1.0").unwrap()
    );
    assert_ne!(
        canonical_digest_json("0").unwrap(),
        canonical_digest_json("-0.0").unwrap()
    );
}

#[test]
fn ambiguous_and_non_json_input_is_rejected_instead_of_being_normalized() {
    for input in [
        "NaN",
        "Infinity",
        "-Infinity",
        "1e400",
        "undefined",
        r#"{"a":1,"a":2}"#,
        r#"{"nested":{"a":1,"a":2}}"#,
        "{} {}",
    ] {
        assert_eq!(
            canonical_digest_json(input).unwrap_err().code,
            ErrorCode::InvalidJson,
            "{input}"
        );
    }
}

#[test]
fn profile_rejects_unknown_fields_runtime_objects_invalid_limits_and_null_options() {
    let invalid = [
        ("api_key", json!("credential-value")),
        ("runtime_bindings", json!({"model":"client"})),
        ("sdk_client", json!({})),
        ("adapters", Value::Null),
        ("hooks", Value::Null),
        ("context_sources", Value::Null),
        ("extensions", Value::Null),
        (
            "instructions",
            json!({"text":"a","module_path":"untrusted-code"}),
        ),
        ("completion_policy", json!({"mode":"verified"})),
        (
            "completion_policy",
            json!({"mode":"turn_end","verifier_ref":{"id":"extra","version":"1"}}),
        ),
        (
            "output_contract",
            json!({"type":"text","schema_ref":{"id":"extra","version":"1"}}),
        ),
        (
            "tools",
            json!([{"tool_id":"documents.search","version":"1.0.0","adapter_binding":"mixed","export_id":"search"}]),
        ),
    ];
    for (key, value) in invalid {
        let mut input = profile_value();
        input[key] = value;
        assert_eq!(
            AgentProfile::from_json(&input.to_string())
                .expect_err(&format!("accepted invalid field: {key}"))
                .code,
            ErrorCode::InvalidContract,
            "{key}"
        );
    }
    for value in [json!(0), json!(-1), json!(1.5), Value::Null] {
        let mut input = profile_value();
        input["limits"]["max_model_calls"] = value;
        assert_eq!(
            AgentProfile::from_json(&input.to_string())
                .unwrap_err()
                .code,
            ErrorCode::InvalidContract
        );
    }
    for field in ["schema_version", "tools", "model_binding", "limits"] {
        let mut input = profile_value();
        input.as_object_mut().unwrap().remove(field);
        assert!(
            AgentProfile::from_json(&input.to_string()).is_err(),
            "missing {field}"
        );
    }
    let mut input = profile_value();
    input["schema_version"] = json!("wickle.agent-profile.v99");
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedSchemaVersion
    );
}

#[test]
fn optional_fields_preserve_presence_and_zero_means_disabled() {
    let absent = profile();
    assert_eq!(absent.completion_policy, CompletionPolicy::TurnEnd {});
    assert_eq!(absent.limits.max_repair_attempts, 0);
    let mut input = profile_value();
    input["adapters"] = json!([]);
    let empty = AgentProfile::from_json(&input.to_string()).unwrap();
    assert!(absent.adapters.is_none());
    assert_eq!(empty.adapters, Some(vec![]));
    assert_ne!(absent.digest(), empty.digest());
    assert_eq!(
        AgentProfile::from_json(&serde_json::to_string(&empty).unwrap()).unwrap(),
        empty
    );
}

#[test]
fn local_binding_errors_are_rejected_before_metadata_resolution() {
    let mut input = profile_value();
    input["tools"][0]["bindings"]["main"] = json!("missing");
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
    let mut input = profile_value();
    let duplicate = input["connectors"][0].clone();
    input["connectors"].as_array_mut().unwrap().push(duplicate);
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .path,
        "connectors.binding_id"
    );
    let mut input = profile_value();
    input["tools"] = json!([{"adapter_binding":"missing","export_id":"search"}]);
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .path,
        "adapter_binding"
    );
    let mut input = profile_value();
    input["context_policy"] = json!({"strategy":"custom"});
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .path,
        "context_policy.version"
    );
}

#[tokio::test]
async fn resolver_accepts_registered_components_and_freezes_their_full_definition_identity() {
    let profile = profile();
    let catalog = catalog();
    let pinned = ProfileValidator::new(&catalog)
        .validate(&profile, &scope())
        .await
        .unwrap();
    assert_eq!(pinned.components().len(), 3);
    assert!(
        pinned
            .components()
            .iter()
            .all(|c| c.reference.version.as_ref() == Some(&id("1.0.0")))
    );
    let restored: ResolvedProfile =
        serde_json::from_str(&serde_json::to_string(&pinned).unwrap()).unwrap();
    restored.ensure_matches(&profile, &scope()).unwrap();
    restored.ensure_same_resolution(&pinned).unwrap();
    let mut changed_catalog = catalog;
    changed_catalog
        .0
        .get_mut(&reference(ComponentKind::ModelBinding, "primary"))
        .unwrap()
        .manifest_digest = digest("new definition");
    let newer = ProfileValidator::new(&changed_catalog)
        .validate(&profile, &scope())
        .await
        .unwrap();
    assert_eq!(
        pinned.ensure_same_resolution(&newer).unwrap_err().code,
        ErrorCode::ProfileMismatch
    );
}

#[tokio::test]
async fn unavailable_dependencies_wrong_versions_and_missing_capabilities_fail_resolution() {
    let p = profile();
    let mut missing = catalog();
    missing
        .0
        .remove(&reference(ComponentKind::Tool, "documents.search"));
    assert_eq!(
        ProfileValidator::new(&missing)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    let mut wrong = catalog();
    wrong
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .reference
        .version = Some(id("2.0.0"));
    assert_eq!(
        ProfileValidator::new(&wrong)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    let mut unsupported = catalog();
    unsupported
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .contract_version = 2;
    assert_eq!(
        ProfileValidator::new(&unsupported)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedContractVersion
    );
    let mut no_capability = catalog();
    no_capability
        .0
        .get_mut(&reference(ComponentKind::ModelBinding, "primary"))
        .unwrap()
        .capabilities
        .clear();
    assert_eq!(
        ProfileValidator::new(&no_capability)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
    let mut no_dependency = catalog();
    no_dependency
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .dependencies
        .push(reference(ComponentKind::Tool, "skills.load"));
    assert_eq!(
        ProfileValidator::new(&no_dependency)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    let mut no_connection = p.clone();
    if let ToolBindingRef::Catalog(tool) = &mut no_connection.tools[0] {
        tool.bindings = None;
    }
    assert_eq!(
        ProfileValidator::new(&catalog())
            .validate(&no_connection, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
}

#[tokio::test]
async fn registered_configuration_schema_rejects_wrong_types_ranges_and_credential_fields() {
    for config in [
        json!({"limit":0}),
        json!({"limit":"5"}),
        json!({"limit":51}),
        json!({"limit":5,"api_key":"credential-value"}),
    ] {
        let mut input = profile_value();
        input["tools"][0]["config"] = config;
        let p = AgentProfile::from_json(&input.to_string()).unwrap();
        let error = ProfileValidator::new(&catalog())
            .validate(&p, &scope())
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidConfiguration);
        assert!(!error.to_string().contains("credential-value"));
        assert!(!format!("{error:?}").contains("credential-value"));
    }
}

#[tokio::test]
async fn external_schema_references_fail_but_literal_reference_data_is_not_executed() {
    for schema in [
        json!({"$ref":"https://unavailable.invalid/schema"}),
        json!({"properties":{"limit":{"$ref":"file:///tmp/schema"}}}),
        json!({"$dynamicRef":"#anchor"}),
        json!({"type":"integer"}),
    ] {
        let mut c = catalog();
        c.0.get_mut(&reference(ComponentKind::Tool, "documents.search"))
            .unwrap()
            .config_schema = schema;
        let error = ProfileValidator::new(&c)
            .validate(&profile(), &scope())
            .await
            .unwrap_err();
        assert!(matches!(
            error.code,
            ErrorCode::InvalidSchema | ErrorCode::InvalidConfiguration
        ));
    }
    let mut c = catalog();
    let meta =
        c.0.get_mut(&reference(ComponentKind::Tool, "documents.search"))
            .unwrap();
    meta.config_schema = json!({"$defs":{"limit":{"type":"integer","minimum":1}},"type":"object","properties":{"limit":{"$ref":"#/$defs/limit"}},"required":["limit"],"additionalProperties":false,"default":{"$ref":"https://example.invalid/literal-data"}});
    ProfileValidator::new(&c)
        .validate(&profile(), &scope())
        .await
        .unwrap();
}

#[tokio::test]
async fn extensions_require_registered_namespaces_and_valid_data() {
    let mut input = profile_value();
    input["extensions"] = json!({"bad":{}});
    assert_eq!(
        AgentProfile::from_json(&input.to_string())
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
    input["extensions"] = json!({"example.settings":{"enabled":true}});
    let p = AgentProfile::from_json(&input.to_string()).unwrap();
    assert_eq!(
        ProfileValidator::new(&catalog())
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    let mut c = catalog();
    let key = reference(ComponentKind::Extension, "example.settings");
    let mut definition = metadata(&key);
    definition.config_schema = json!({"type":"object","properties":{"enabled":{"type":"boolean"}},"additionalProperties":false});
    c.0.insert(key, definition);
    ProfileValidator::new(&c)
        .validate(&p, &scope())
        .await
        .unwrap();
    input["extensions"]["example.settings"]["enabled"] = json!(1);
    assert_eq!(
        ProfileValidator::new(&c)
            .validate(
                &AgentProfile::from_json(&input.to_string()).unwrap(),
                &scope()
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidConfiguration
    );
}

#[tokio::test]
async fn registered_formats_are_asserted_instead_of_treated_as_annotations() {
    let mut catalog = catalog();
    catalog
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .config_schema = json!({
        "type": "object", "properties": {"example_uuid": {"type": "string", "format": "uuid"}},
        "required": ["example_uuid"], "additionalProperties": false
    });
    let mut input = profile_value();
    input["tools"][0]["config"] = json!({"example_uuid": "not-a-uuid"});
    let invalid = AgentProfile::from_json(&input.to_string()).unwrap();
    assert_eq!(
        ProfileValidator::new(&catalog)
            .validate(&invalid, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidConfiguration
    );
    input["tools"][0]["config"] = json!({"example_uuid": "123e4567-e89b-12d3-a456-426614174000"});
    ProfileValidator::new(&catalog)
        .validate(
            &AgentProfile::from_json(&input.to_string()).unwrap(),
            &scope(),
        )
        .await
        .unwrap();
}

fn adapter_profile() -> (AgentProfile, Catalog) {
    let mut value = profile_value();
    value["tools"] =
        json!([{"adapter_binding":"documents","export_id":"search","alias":"search_documents"}]);
    value["adapters"] = json!([{"binding_id":"documents","adapter_id":"document-tools","version":"1.0.0","connections":{"main":"knowledge"}}]);
    let mut c = catalog();
    let key = reference(ComponentKind::Adapter, "document-tools");
    let mut definition = metadata(&key);
    definition.required_connections.insert(id("main"));
    definition.exports.push(ExportMetadata {
        export_id: id("search"),
        kind: ExportKind::Tool,
        contract_version: 1,
        model_name: Some(id("documents_search")),
        hook_position: None,
        capabilities: BTreeSet::from([id("documents.search")]),
        required_capabilities: BTreeSet::from([id("model.tool_calling")]),
    });
    definition.exports.push(ExportMetadata {
        export_id: id("unused"),
        kind: ExportKind::Tool,
        contract_version: 1,
        model_name: Some(id("unused")),
        hook_position: None,
        capabilities: BTreeSet::from([id("unused.capability")]),
        required_capabilities: BTreeSet::new(),
    });
    c.0.insert(key, definition);
    (AgentProfile::from_json(&value.to_string()).unwrap(), c)
}

#[tokio::test]
async fn adapter_exports_must_exist_match_kind_and_be_selected_to_supply_capabilities() {
    let (p, c) = adapter_profile();
    ProfileValidator::new(&c)
        .validate(&p, &scope())
        .await
        .unwrap();
    let mut wrong_kind = catalog();
    let (_, mut definitions) = adapter_profile();
    let key = reference(ComponentKind::Adapter, "document-tools");
    let mut definition = definitions.0.remove(&key).unwrap();
    definition.exports[0].kind = ExportKind::ContextSource;
    wrong_kind.0.insert(key.clone(), definition);
    assert_eq!(
        ProfileValidator::new(&wrong_kind)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
    let mut missing = p.clone();
    if let ToolBindingRef::Export(export) = &mut missing.tools[0] {
        export.export_id = id("missing");
    }
    assert_eq!(
        ProfileValidator::new(&c)
            .validate(&missing, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
    let mut needs_unselected = c;
    needs_unselected
        .0
        .get_mut(&key)
        .unwrap()
        .required_capabilities
        .insert(id("unused.capability"));
    assert_eq!(
        ProfileValidator::new(&needs_unselected)
            .validate(&p, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
    let mut duplicate = p.clone();
    duplicate.tools.push(duplicate.tools[0].clone());
    assert_eq!(
        ProfileValidator::new(&adapter_profile().1)
            .validate(&duplicate, &scope())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidReference
    );
}

#[tokio::test]
async fn saved_profiles_reject_changed_instructions_versions_scope_and_tampered_serialization() {
    let pinned = resolved().await;
    let p = profile();
    let mut changed = p.clone();
    changed.version = id("2.0.0");
    assert_eq!(
        pinned.ensure_matches(&changed, &scope()).unwrap_err().code,
        ErrorCode::ProfileMismatch
    );
    let mut changed = p.clone();
    changed.instructions = Instructions::Text(InstructionText {
        text: "Changed behavior".into(),
    });
    assert_eq!(
        pinned.ensure_matches(&changed, &scope()).unwrap_err().code,
        ErrorCode::ProfileMismatch
    );
    let mut other_scope = scope();
    other_scope.tenant_id = id("other");
    assert_eq!(
        pinned.ensure_matches(&p, &other_scope).unwrap_err().code,
        ErrorCode::ProfileMismatch
    );
    let mut stored = serde_json::to_value(&pinned).unwrap();
    stored["profile"]["version"] = json!("new");
    assert!(serde_json::from_value::<ResolvedProfile>(stored).is_err());
}

#[test]
fn system_inputs_preserve_absent_empty_and_owned_values_without_debug_leakage() {
    let mut input = json!({"scope":{"tenant_id":"t","workspace_id":"w"},"principal_ref":"p","capability_grant_ref":"g"});
    let absent = ExecutionContextData::from_json(&input.to_string()).unwrap();
    assert!(absent.system_inputs.is_none());
    input["system_inputs"] = json!({});
    let empty = ExecutionContextData::from_json(&input.to_string()).unwrap();
    assert!(empty.system_inputs.as_ref().unwrap().values().is_empty());
    input["system_inputs"] = Value::Null;
    assert!(ExecutionContextData::from_json(&input.to_string()).is_err());
    input["system_inputs"] = json!({"workspace_id":"private-workspace-value"});
    let stored = ExecutionContextData::from_json(&input.to_string()).unwrap();
    input["system_inputs"]["workspace_id"] = json!("mutated");
    assert_eq!(
        stored.system_inputs.as_ref().unwrap().values()["workspace_id"],
        json!("private-workspace-value")
    );
    assert!(!format!("{stored:?}").contains("private-workspace-value"));
    let roundtrip =
        ExecutionContextData::from_json(&serde_json::to_string(&stored).unwrap()).unwrap();
    assert_eq!(roundtrip, stored);
}

fn record(name: &str) -> RecordRef {
    RecordRef {
        record_id: id(name),
        revision: 1,
        digest: digest(name),
    }
}
fn request() -> RunRequest {
    RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Find supporting evidence".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    }
}

#[test]
fn absent_model_options_preserve_existing_request_encodings_and_digests() {
    let mut legacy = serde_json::to_value(request()).unwrap();
    legacy.as_object_mut().unwrap().remove("model_options");
    let restored = RunRequest::from_json(&legacy.to_string()).unwrap();
    assert!(restored.model_options.is_empty());
    assert_eq!(
        canonical_digest(&serde_json::to_value(restored).unwrap()),
        canonical_digest(&legacy)
    );
    legacy["model_options"] = json!(null);
    assert!(RunRequest::from_json(&legacy.to_string()).is_err());
}

async fn checkpoint() -> RunSnapshot {
    let p = resolved().await;
    let request = request();
    let system_inputs = Some(SystemInputSnapshotRef {
        snapshot_ref: record("protected-inputs"),
        values_digest: digest("owned inputs"),
        definition_versions: BTreeMap::from([(id("workspace_id"), id("1"))]),
    });
    let wait = WaitState {
        wait_id: id("approval"),
        target: WaitTarget::Approval {
            target: ApprovalTarget::Tool {
                call_id: id("call"),
                binding_digest: digest("bound args"),
            },
        },
        expires_at_ms: Some(100000),
    };
    RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id("run"),
        request_digest: admission_digest(&request, &p, system_inputs.as_ref()),
        request,
        scope: scope(),
        timing: RunTiming::new(0, p.profile().limits.max_elapsed_ms.get()).unwrap(),
        resume_receipts: vec![],
        reservations: vec![AttemptReservation {
            attempt_id: id("first-model-attempt"),
            kind: ReservationKind::Model {
                purpose: ModelPurpose::Agent,
            },
            reserved_at_ms: 0,
        }],
        limits: p.profile().limits.clone(),
        profile: p,
        status: RunStatus::Waiting,
        phase: RunPhase::Waiting,
        model_step_id: Some(id("step")),
        usage: BudgetUsage {
            model_calls: 1,
            ..BudgetUsage::default()
        },
        model_ledger: vec![],
        tool_ledger: vec![ToolLedgerEntry {
            call: ToolCall {
                call_id: id("call"),
                model_request_id: id("model-request"),
                provider_call_id: id("provider-call"),
                tool_name: id("documents_search"),
                model_inputs: BTreeMap::from([("query".into(), json!("evidence"))]),
                descriptor_digest: Some(digest("descriptor")),
                bound_input_ref: Some(record("bound-inputs")),
            },
            state: ToolCallState::Planned {},
        }],
        system_inputs,
        wait: Some(wait),
        outcome: None,
        assembly_ref: Some(record("assembly")),
        routing_snapshot_ref: None,
        context_batches: vec![record("context-batch")],
        source_states: vec![],
        revision: 4,
        last_event_seq: 7,
    }
}

#[tokio::test]
async fn approval_checkpoint_roundtrip_preserves_the_target_and_deduplication_identity() {
    let snapshot = checkpoint().await;
    snapshot.validate().unwrap();
    let restored = RunSnapshot::from_json(&serde_json::to_string(&snapshot).unwrap()).unwrap();
    assert_eq!(restored, snapshot);
    let target = match &restored.wait.as_ref().unwrap().target {
        WaitTarget::Approval { target } => target.clone(),
        _ => unreachable!(),
    };
    let command = ResumeCommand {
        run_id: restored.run_id.clone(),
        expected_revision: restored.revision,
        command_id: id("decision"),
        action: ResumeAction::Approve {
            wait_id: restored.wait.as_ref().unwrap().wait_id.clone(),
            target,
        },
    };
    assert_eq!(
        ResumeCommand::from_json(&serde_json::to_string(&command).unwrap()).unwrap(),
        command
    );
    let mut relocated = snapshot.system_inputs.clone().unwrap();
    relocated.snapshot_ref = record("new-storage-location");
    assert_eq!(
        snapshot.request_digest,
        admission_digest(&snapshot.request, &snapshot.profile, Some(&relocated))
    );
    relocated.values_digest = digest("different inputs");
    assert_ne!(
        snapshot.request_digest,
        admission_digest(&snapshot.request, &snapshot.profile, Some(&relocated))
    );
    relocated = snapshot.system_inputs.clone().unwrap();
    relocated
        .definition_versions
        .insert(id("workspace_id"), id("2"));
    assert_ne!(
        snapshot.request_digest,
        admission_digest(&snapshot.request, &snapshot.profile, Some(&relocated))
    );
    let empty = SystemInputSnapshotRef {
        snapshot_ref: record("empty-inputs"),
        values_digest: canonical_digest(&json!({})),
        definition_versions: BTreeMap::new(),
    };
    assert_eq!(
        admission_digest(&snapshot.request, &snapshot.profile, None),
        admission_digest(&snapshot.request, &snapshot.profile, Some(&empty))
    );
}

#[tokio::test]
async fn catalog_and_export_names_cannot_create_ambiguous_tool_routing() {
    let (mut profile, mut catalog) = adapter_profile();
    profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
        tool_id: id("documents.search"),
        version: id("1.0.0"),
        bindings: Some(BTreeMap::from([(id("main"), id("knowledge"))])),
        config: Some(BTreeMap::from([("limit".into(), json!(5))])),
    }));
    catalog
        .0
        .get_mut(&reference(ComponentKind::Tool, "documents.search"))
        .unwrap()
        .model_name = Some(id("search_documents"));
    assert_eq!(
        ProfileValidator::new(&catalog)
            .validate(&profile, &scope())
            .await
            .unwrap_err()
            .path,
        "tools.model_name"
    );
}

#[tokio::test]
async fn checkpoint_rejects_inconsistent_state_budget_inputs_and_dispatch_records() {
    let valid = checkpoint().await;
    let mut malformed = valid.clone();
    malformed.wait = None;
    assert_eq!(
        malformed.validate().unwrap_err().code,
        ErrorCode::InvalidSnapshot
    );
    let mut malformed = valid.clone();
    malformed.limits.max_tool_attempts += 1;
    assert_eq!(malformed.validate().unwrap_err().path, "limits");
    let mut malformed = valid.clone();
    malformed.request.request_id = id("changed");
    assert_eq!(malformed.validate().unwrap_err().path, "request_digest");
    let mut malformed = valid.clone();
    malformed.tool_ledger[0].call.bound_input_ref = None;
    malformed.tool_ledger[0].state = ToolCallState::Dispatching {
        attempt_id: id("attempt"),
        idempotency_key: id("effect"),
    };
    malformed.reservations.push(AttemptReservation {
        attempt_id: id("attempt"),
        kind: ReservationKind::Tool {
            call_id: id("call"),
        },
        reserved_at_ms: 0,
    });
    malformed.usage.tool_attempts += 1;
    assert_eq!(
        malformed.validate().unwrap_err().path,
        "tool_ledger.bound_input_ref"
    );
    let mut malformed = valid.clone();
    malformed.tool_ledger.push(malformed.tool_ledger[0].clone());
    assert_eq!(
        malformed.validate().unwrap_err().path,
        "tool_ledger.call_id"
    );
    let mut stored = serde_json::to_value(&valid).unwrap();
    stored["schema_version"] = json!("wickle.run-snapshot.v2");
    assert_eq!(
        RunSnapshot::from_json(&stored.to_string())
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedSchemaVersion
    );
}

#[tokio::test]
async fn dispatched_and_approval_pending_tools_require_their_own_saved_reservation() {
    for state in [
        ToolCallState::Dispatching {
            attempt_id: id("attempt"),
            idempotency_key: id("effect"),
        },
        ToolCallState::ApprovalPending {
            attempt_id: id("attempt"),
            idempotency_key: id("effect"),
        },
        ToolCallState::Unknown {
            attempt_id: id("attempt"),
            idempotency_key: id("effect"),
        },
    ] {
        let mut snapshot = checkpoint().await;
        snapshot.tool_ledger[0].state = state;
        assert_eq!(
            snapshot.validate().unwrap_err().path,
            "tool_ledger.reservation"
        );
        snapshot.reservations.push(AttemptReservation {
            attempt_id: id("attempt"),
            kind: ReservationKind::Tool {
                call_id: id("different-call"),
            },
            reserved_at_ms: 0,
        });
        snapshot.usage.tool_attempts += 1;
        assert_eq!(
            snapshot.validate().unwrap_err().path,
            "tool_ledger.reservation"
        );
        snapshot.reservations.last_mut().unwrap().kind = ReservationKind::Tool {
            call_id: id("call"),
        };
        snapshot.validate().unwrap();
        snapshot.tool_ledger[0].call.descriptor_digest = None;
        assert_eq!(
            snapshot.validate().unwrap_err().path,
            "tool_ledger.unregistered"
        );
    }
}

#[tokio::test]
async fn an_unregistered_tool_can_only_be_planned_or_settled_without_an_effect() {
    let mut snapshot = checkpoint().await;
    snapshot.tool_ledger[0].call.descriptor_digest = None;
    snapshot.tool_ledger[0].call.bound_input_ref = None;
    snapshot.validate().unwrap();
    let result = ToolResult {
        call_id: id("call"),
        call_message_id: id("original-assistant"),
        status: ToolResultStatus::Failed,
        effect: ToolEffect::NotApplied,
        content: vec![],
        error: None,
        effect_receipt_ref: None,
    };
    snapshot.tool_ledger[0].state = ToolCallState::Settled {
        result: result.clone(),
    };
    snapshot.validate().unwrap();
    for effect in [ToolEffect::Applied, ToolEffect::Unknown] {
        snapshot.tool_ledger[0].state = ToolCallState::Settled {
            result: ToolResult {
                effect,
                ..result.clone()
            },
        };
        assert_eq!(
            snapshot.validate().unwrap_err().path,
            "tool_ledger.unregistered"
        );
    }
    snapshot.tool_ledger[0].state = ToolCallState::Settled {
        result: ToolResult {
            status: ToolResultStatus::Succeeded,
            ..result
        },
    };
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "tool_ledger.unregistered"
    );
}

#[tokio::test]
async fn success_requires_a_matching_completion_basis_and_verified_success_requires_evidence() {
    let mut snapshot = checkpoint().await;
    snapshot.status = RunStatus::Succeeded;
    snapshot.phase = RunPhase::Finish;
    snapshot.wait = None;
    snapshot.outcome = Some(RunOutcome {
        result: OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded,
        },
        output: vec![InputContent::Text {
            text: "Candidate answer".into(),
        }],
        artifacts: vec![],
        usage: snapshot.usage.clone(),
        checkpoint_revision: snapshot.revision,
        verification: None,
        unresolved_effects: vec![],
    });
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "tool_ledger.unsettled"
    );
    snapshot.tool_ledger[0].state = ToolCallState::Settled {
        result: ToolResult {
            call_id: id("call"),
            call_message_id: id("call-message"),
            status: ToolResultStatus::Succeeded,
            effect: ToolEffect::NotApplied,
            content: vec![InputContent::Text {
                text: "Evidence found".into(),
            }],
            effect_receipt_ref: None,
            error: None,
        },
    };
    snapshot.validate().unwrap();
    snapshot
        .outcome
        .as_mut()
        .unwrap()
        .unresolved_effects
        .push(record("unknown-effect"));
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "outcome.unresolved_effects"
    );
    snapshot
        .outcome
        .as_mut()
        .unwrap()
        .unresolved_effects
        .clear();
    snapshot.outcome.as_mut().unwrap().result = OutcomeResult::Succeeded {
        completion_basis: CompletionBasis::Verified,
    };
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "outcome.verification"
    );
    snapshot.outcome.as_mut().unwrap().verification = Some(VerificationSummary {
        verifier_ref: VersionedRef {
            id: id("verifier"),
            version: id("1"),
        },
        criteria_ref: VersionedRef {
            id: id("criteria"),
            version: id("1"),
        },
        verdict: VerificationVerdict::Pass,
        evidence: vec![record("evidence")],
    });
    assert_eq!(
        snapshot.validate().unwrap_err().path,
        "outcome.completion_basis"
    );
}

#[test]
fn event_and_input_contracts_reject_unsupported_versions_and_execution_injection() {
    let event = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("event"),
        scope: scope(),
        run_id: id("run"),
        session_id: id("session"),
        seq: 1.try_into().unwrap(),
        timestamp_ms: 1000,
        payload: RunEventPayload::RunFinished {
            outcome_ref: record("outcome"),
        },
    };
    assert_eq!(
        RunEvent::from_json(&serde_json::to_string(&event).unwrap()).unwrap(),
        event
    );
    let mut value = serde_json::to_value(&event).unwrap();
    value["schema_version"] = json!("wickle.run-event.v2");
    assert_eq!(
        RunEvent::from_json(&value.to_string()).unwrap_err().code,
        ErrorCode::UnsupportedSchemaVersion
    );
    let mut value = serde_json::to_value(&event).unwrap();
    value["seq"] = json!(0);
    assert!(RunEvent::from_json(&value.to_string()).is_err());
    let mut value = serde_json::to_value(request()).unwrap();
    value["input"] = json!([{"type":"tool_call","call":{"tool_name":"unapproved"}}]);
    assert!(RunRequest::from_json(&value.to_string()).is_err());
    let mut value = serde_json::to_value(request()).unwrap();
    value["trigger"] = json!({"kind":"user","source_id":"forged"});
    assert!(RunRequest::from_json(&value.to_string()).is_err());
    assert!(
        serde_json::from_value::<ModelAttemptState>(json!({"state":"completed","kind":"timeout"}))
            .is_err()
    );
    assert!(
        serde_json::from_value::<ToolCallState>(
            json!({"state":"planned","idempotency_key":"unexpected"})
        )
        .is_err()
    );
}

#[test]
fn route_roundtrip_keeps_model_api_deployment_and_adapter_versions_distinct() {
    let route = ResolvedModelRoute {
        binding: VersionedRef {
            id: id("binding"),
            version: id("binding-revision"),
        },
        catalog_revision: id("catalog-1"),
        routing_policy_revision: id("policy-1"),
        requested_model: id("requested-alias"),
        model_id: id("model-family"),
        model_version: id("release-A"),
        version_semantics: VersionSemantics::MutableDeployment,
        provider: id("custom-provider"),
        target: BTreeMap::from([("deployment".into(), json!("deployment-name"))]),
        deployment_revision: Some(id("deployment-revision")),
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("api-contract-version"),
        },
        adapter: VersionedRef {
            id: id("adapter"),
            version: id("adapter-version"),
        },
        capability_revision: id("capabilities-1"),
        connection_ref: VersionedRef {
            id: id("connection"),
            version: id("connection-revision"),
        },
    };
    let restored: ResolvedModelRoute =
        serde_json::from_str(&serde_json::to_string(&route).unwrap()).unwrap();
    assert_eq!(restored, route);
    let original = route.digest();
    let mut changed = route.clone();
    changed.model_version = id("release-B");
    assert_ne!(changed.digest(), original);
    changed = route.clone();
    changed.api_contract.version = id("different-api-version");
    assert_ne!(changed.digest(), original);
    changed = route.clone();
    changed.deployment_revision = Some(id("new-deployment-revision"));
    assert_ne!(changed.digest(), original);
    let record = ModelInvocationRecord {
        run_id: id("run"),
        model_step_id: id("step"),
        attempt_id: id("attempt"),
        purpose: ModelPurpose::Agent,
        route,
        selection_reason: id("policy-default"),
        request_digest: digest("request"),
        state: ModelAttemptState::Completed {},
        inspection_ref: None,
        response_ref: None,
        provider_request_id: None,
        reported_model_id: None,
        reported_model_version: None,
        usage: None,
    };
    let restored: ModelInvocationRecord =
        serde_json::from_str(&serde_json::to_string(&record).unwrap()).unwrap();
    assert_eq!(restored.reported_model_version, None);
    assert_eq!(restored.usage, None);
}
```

## `crates/wickle/tests/policy.rs`

```rust
//! Authorization behavior with injected Host policies and protected stored records.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::pending,
    num::NonZeroU64,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use serde_json::{Value, json};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn digest(value: &str) -> JsonDigest {
    canonical_digest(&json!(value))
}

fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    }
}

fn context(owner: Scope) -> ExecutionContext {
    ExecutionContext::new(
        ExecutionContextData {
            scope: owner,
            principal_ref: id("originator"),
            capability_grant_ref: id("member-grant"),
            trace_context: None,
            system_inputs: Some(SystemInputs::new(BTreeMap::from([(
                "private_host_value".into(),
                json!("protected-host-value"),
            )]))),
        },
        CancellationToken::new(),
    )
}

fn request(action: PolicyAction) -> PolicyRequest {
    PolicyRequest {
        owner_scope: scope(),
        resource_id: id("run"),
        action,
    }
}

#[derive(Clone)]
enum Behavior {
    Decision(PolicyDecision),
    Error,
    PanicBeforeFuture,
    PanicInFuture,
    Pending,
    CancelThenAllow,
}

#[derive(Clone)]
struct Observed {
    request: PolicyRequest,
    scope: Scope,
    principal: Id,
    grant: Id,
}

struct HostPolicy {
    behavior: Mutex<Behavior>,
    calls: AtomicUsize,
    observed: Mutex<Vec<Observed>>,
}

impl HostPolicy {
    fn new(behavior: Behavior) -> Arc<Self> {
        Arc::new(Self {
            behavior: Mutex::new(behavior),
            calls: AtomicUsize::new(0),
            observed: Mutex::new(Vec::new()),
        })
    }

    fn gate(self: &Arc<Self>) -> PolicyGate {
        PolicyGate::new(self.clone(), Duration::from_millis(50)).unwrap()
    }
}

impl PolicyPort for HostPolicy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.observed.lock().unwrap().push(Observed {
            request: request.clone(),
            scope: context.scope.clone(),
            principal: context.principal_ref.clone(),
            grant: context.capability_grant_ref.clone(),
        });
        let behavior = self.behavior.lock().unwrap().clone();
        if matches!(behavior, Behavior::PanicBeforeFuture) {
            panic!("policy callback panicked before returning a future");
        }
        Box::pin(async move {
            match behavior {
                Behavior::Decision(decision) => Ok(decision),
                Behavior::Error => Err(ContractError::new(
                    ErrorCode::InvalidContract,
                    "private-policy-diagnostic",
                )),
                Behavior::PanicInFuture => panic!("policy future panicked"),
                Behavior::Pending => pending().await,
                Behavior::CancelThenAllow => {
                    context.cancellation.cancel();
                    Ok(PolicyDecision::Allow {})
                }
                Behavior::PanicBeforeFuture => unreachable!(),
            }
        })
    }
}

#[derive(Default)]
struct OperationCounts {
    constructed: AtomicUsize,
    executed: AtomicUsize,
}

impl OperationCounts {
    async fn guarded(
        &self,
        gate: &PolicyGate,
        request: &PolicyRequest,
        context: &ExecutionContext,
        deadline: Option<Instant>,
        restriction: Option<PolicyDecision>,
    ) -> Result<Guarded<u32>, ContractError> {
        gate.guard(request, context, deadline, restriction, || {
            self.constructed.fetch_add(1, Ordering::SeqCst);
            async {
                self.executed.fetch_add(1, Ordering::SeqCst);
                Ok(42)
            }
        })
        .await
    }

    fn assert_calls(&self, expected: usize) {
        assert_eq!(self.constructed.load(Ordering::SeqCst), expected);
        assert_eq!(self.executed.load(Ordering::SeqCst), expected);
    }
}

#[tokio::test]
async fn every_control_boundary_requires_exact_tenant_workspace_and_user_scope() {
    let policy = HostPolicy::new(Behavior::Decision(PolicyDecision::Allow {}));
    let gate = policy.gate();
    let actions = [
        PolicyAction::ReadRun {},
        PolicyAction::ReadRunDetails {},
        PolicyAction::ReadArtifact {},
        PolicyAction::ReadEvents {},
        PolicyAction::ResumeRun {
            command: Box::new(ResumeCommand {
                run_id: id("run"),
                expected_revision: 0,
                command_id: id("resume-command"),
                action: ResumeAction::Recover {
                    recovery_ref: record("recovery"),
                },
            }),
            binding_digest: None,
        },
        PolicyAction::CancelRun {},
    ];
    let calls = OperationCounts::default();
    for owner_user in [None, Some(id("owner"))] {
        let mut owner = scope();
        owner.user_id = owner_user;
        let mut foreign_tenant = owner.clone();
        foreign_tenant.tenant_id = id("other-tenant");
        let mut foreign_workspace = owner.clone();
        foreign_workspace.workspace_id = id("other-workspace");
        let mut foreign_user = owner.clone();
        foreign_user.user_id = Some(id("other-user"));
        let mut different_presence = owner.clone();
        different_presence.user_id = if owner.user_id.is_some() {
            None
        } else {
            Some(id("owner"))
        };
        for action in &actions {
            let mut request = request(action.clone());
            request.owner_scope = owner.clone();
            for wrong_scope in [
                &foreign_tenant,
                &foreign_workspace,
                &foreign_user,
                &different_presence,
            ] {
                let error = calls
                    .guarded(&gate, &request, &context(wrong_scope.clone()), None, None)
                    .await
                    .unwrap_err();
                assert_eq!(error.code, ErrorCode::AccessDenied);
            }
        }
    }
    calls.assert_calls(0);
    assert_eq!(policy.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        calls
            .guarded(
                &gate,
                &request(PolicyAction::ReadRun {}),
                &context(scope()),
                None,
                None,
            )
            .await
            .unwrap(),
        Guarded::Completed(42)
    );
    calls.assert_calls(1);
    assert_eq!(policy.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn denial_errors_panics_timeout_and_cancellation_do_not_construct_operations() {
    let cases = [
        (
            Behavior::Decision(PolicyDecision::Deny {
                reason: id("membership_revoked"),
            }),
            ErrorCode::AccessDenied,
        ),
        (Behavior::Error, ErrorCode::PolicyUnavailable),
        (Behavior::PanicBeforeFuture, ErrorCode::PolicyUnavailable),
        (Behavior::PanicInFuture, ErrorCode::PolicyUnavailable),
        (Behavior::Pending, ErrorCode::DeadlineExceeded),
        (Behavior::CancelThenAllow, ErrorCode::Cancelled),
    ];
    for (behavior, expected) in cases {
        let policy = HostPolicy::new(behavior);
        let calls = OperationCounts::default();
        let error = calls
            .guarded(
                &policy.gate(),
                &request(PolicyAction::CancelRun {}),
                &context(scope()),
                None,
                Some(PolicyDecision::Allow {}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, expected);
        assert_eq!(error.path, "policy");
        calls.assert_calls(0);
        assert_eq!(policy.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn preexisting_cancellation_or_deadline_prevents_even_policy_entry() {
    let policy = HostPolicy::new(Behavior::Decision(PolicyDecision::Allow {}));
    let gate = policy.gate();
    let request = request(PolicyAction::StartRun {});
    let cancelled = context(scope());
    cancelled.cancellation.cancel();
    let calls = OperationCounts::default();
    assert_eq!(
        calls
            .guarded(&gate, &request, &cancelled, None, None)
            .await
            .unwrap_err()
            .code,
        ErrorCode::Cancelled
    );
    assert_eq!(
        calls
            .guarded(
                &gate,
                &request,
                &context(scope()),
                Some(Instant::now()),
                None,
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::DeadlineExceeded
    );
    calls.assert_calls(0);
    assert_eq!(policy.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn caller_deadline_bounds_a_pending_policy_before_its_configured_timeout() {
    let policy = HostPolicy::new(Behavior::Pending);
    let gate = policy.gate();
    let start = Instant::now();
    let calls = OperationCounts::default();
    let error = calls
        .guarded(
            &gate,
            &request(PolicyAction::ReadEvents {}),
            &context(scope()),
            Some(start + Duration::from_millis(5)),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::DeadlineExceeded);
    assert!(start.elapsed() < Duration::from_millis(50));
    calls.assert_calls(0);
}

#[tokio::test]
async fn cancellation_while_policy_is_pending_stops_before_dispatch() {
    let policy = HostPolicy::new(Behavior::Pending);
    let gate = policy.gate();
    let context = context(scope());
    let calls = OperationCounts::default();
    let request = request(PolicyAction::ReadArtifact {});
    let cancellation = async {
        tokio::task::yield_now().await;
        context.cancellation.cancel();
    };
    let (result, ()) = tokio::join!(
        calls.guarded(&gate, &request, &context, None, None),
        cancellation,
    );
    assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
    calls.assert_calls(0);
    assert_eq!(policy.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn every_access_rechecks_the_current_grant_after_revocation() {
    let policy = HostPolicy::new(Behavior::Decision(PolicyDecision::Allow {}));
    let gate = policy.gate();
    let context = context(scope());
    let request = request(PolicyAction::ReadRun {});
    let calls = OperationCounts::default();
    assert_eq!(
        calls
            .guarded(&gate, &request, &context, None, None)
            .await
            .unwrap(),
        Guarded::Completed(42)
    );
    *policy.behavior.lock().unwrap() = Behavior::Decision(PolicyDecision::Deny {
        reason: id("membership_revoked"),
    });
    assert_eq!(
        calls
            .guarded(&gate, &request, &context, None, None)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    calls.assert_calls(1);
    assert_eq!(policy.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn restrictions_preserve_host_denial_and_approval_and_can_only_reduce_access() {
    let allow = PolicyDecision::Allow {};
    let deny = PolicyDecision::Deny {
        reason: id("host_denied"),
    };
    let approval = PolicyDecision::RequireApproval {
        reason: id("host_review"),
    };
    let other_deny = PolicyDecision::Deny {
        reason: id("hook_denied"),
    };
    let other_approval = PolicyDecision::RequireApproval {
        reason: id("hook_review"),
    };
    let cases = [
        (deny.clone(), allow.clone(), deny.clone()),
        (deny.clone(), other_deny, deny.clone()),
        (deny.clone(), approval.clone(), deny.clone()),
        (approval.clone(), allow.clone(), approval.clone()),
        (approval.clone(), other_approval, approval.clone()),
        (approval.clone(), deny.clone(), deny.clone()),
        (allow.clone(), deny.clone(), deny),
        (allow, approval.clone(), approval),
    ];
    for (host, restriction, expected) in cases {
        let gate = HostPolicy::new(Behavior::Decision(host)).gate();
        let request = request(PolicyAction::ReadRun {});
        let context = context(scope());
        assert_eq!(
            gate.check(&request, &context, None, Some(restriction.clone()))
                .await
                .unwrap(),
            expected
        );
        let calls = OperationCounts::default();
        let actual = calls
            .guarded(&gate, &request, &context, None, Some(restriction))
            .await;
        match expected {
            PolicyDecision::Deny { .. } => {
                assert_eq!(actual.unwrap_err().code, ErrorCode::AccessDenied);
            }
            PolicyDecision::RequireApproval { reason } => match actual.unwrap() {
                Guarded::ApprovalRequired(challenge) => assert_eq!(challenge.reason, reason),
                Guarded::Completed(_) => panic!("approval requirement was bypassed"),
            },
            PolicyDecision::Allow {} => unreachable!(),
        }
        calls.assert_calls(0);
    }
}

fn tool_request(document_id: &str) -> PolicyRequest {
    request(PolicyAction::ExecuteTool {
        input: ToolPolicyInput::new(
            id("call"),
            VersionedRef {
                id: id("documents.read"),
                version: id("1.0.0"),
            },
            digest("descriptor"),
            digest("binding"),
            BTreeMap::from([
                ("document_id".into(), json!(document_id)),
                ("query".into(), json!("revenue")),
            ]),
        ),
    })
}

struct ResourcePolicy(BTreeMap<String, Scope>);

impl PolicyPort for ResourcePolicy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            let PolicyAction::ExecuteTool { input } = &request.action else {
                return Ok(PolicyDecision::Deny {
                    reason: id("unsupported_action"),
                });
            };
            let owner = input
                .execution_args()
                .get("document_id")
                .and_then(Value::as_str)
                .and_then(|key| self.0.get(key));
            Ok(if owner == Some(context.scope) {
                PolicyDecision::Allow {}
            } else {
                PolicyDecision::Deny {
                    reason: id("target_unavailable"),
                }
            })
        })
    }
}

#[tokio::test]
async fn actual_bound_target_must_exist_and_belong_to_scope_before_business_operation() {
    let owned = "bc005010-d3e8-4cb8-b1fd-f6ff02c90ca6";
    let foreign = "c11cf1bb-47a2-455c-a6f8-6d7e217cd195";
    let missing = "d3a805bd-a7ea-4224-a39c-511097c43af8";
    let mut foreign_scope = scope();
    foreign_scope.tenant_id = id("other-tenant");
    let gate = PolicyGate::new(
        Arc::new(ResourcePolicy(BTreeMap::from([
            (owned.into(), scope()),
            (foreign.into(), foreign_scope),
        ]))),
        Duration::from_secs(1),
    )
    .unwrap();
    let calls = OperationCounts::default();
    let mut context = context(scope());
    context.data.system_inputs = Some(SystemInputs::new(BTreeMap::from([(
        "document_id".into(),
        json!(owned),
    )])));
    for target in [foreign, missing] {
        assert_eq!(
            calls
                .guarded(&gate, &tool_request(target), &context, None, None)
                .await
                .unwrap_err()
                .code,
            ErrorCode::AccessDenied
        );
    }
    calls.assert_calls(0);
    assert_eq!(
        calls
            .guarded(&gate, &tool_request(owned), &context, None, None)
            .await
            .unwrap(),
        Guarded::Completed(42)
    );
    calls.assert_calls(1);
}

#[tokio::test]
async fn approval_keeps_the_bound_action_while_authenticating_a_different_reviewer() {
    let policy = HostPolicy::new(Behavior::Decision(PolicyDecision::RequireApproval {
        reason: id("review_required"),
    }));
    let gate = policy.gate();
    let request = tool_request("protected-original-target");
    let original = context(scope());
    let mut reviewer = context(scope());
    reviewer.data.principal_ref = id("reviewer");
    reviewer.data.capability_grant_ref = id("review-grant");
    reviewer.data.system_inputs = Some(SystemInputs::new(BTreeMap::from([(
        "document_id".into(),
        json!("replacement-target"),
    )])));
    let calls = OperationCounts::default();
    let mut challenges = Vec::new();
    for context in [&original, &reviewer] {
        match calls
            .guarded(
                &gate,
                &request,
                context,
                None,
                Some(PolicyDecision::Allow {}),
            )
            .await
            .unwrap()
        {
            Guarded::ApprovalRequired(challenge) => challenges.push(challenge),
            Guarded::Completed(_) => panic!("approval unexpectedly executed the operation"),
        }
    }
    calls.assert_calls(0);
    assert_eq!(challenges[0], challenges[1]);
    assert_eq!(challenges[0].scope, request.owner_scope);
    assert_eq!(challenges[0].request_digest, request.digest());
    let observed = policy.observed.lock().unwrap();
    assert_eq!(observed[0].request, request);
    assert_eq!(observed[1].request, request);
    assert_eq!(observed[0].scope, request.owner_scope);
    assert_eq!(observed[1].scope, request.owner_scope);
    assert_eq!(observed[0].principal, original.data.principal_ref);
    assert_eq!(observed[1].principal, reviewer.data.principal_ref);
    assert_eq!(observed[1].grant, reviewer.data.capability_grant_ref);
    assert!(!format!("{request:?}").contains("protected-original-target"));
    assert!(matches!(
        request.action,
        PolicyAction::ExecuteTool { ref input }
            if input.execution_args()["document_id"] == json!("protected-original-target")
    ));
}

#[tokio::test]
async fn approval_identity_changes_for_arguments_versions_descriptors_bindings_and_scope() {
    let gate = HostPolicy::new(Behavior::Decision(PolicyDecision::RequireApproval {
        reason: id("review_required"),
    }))
    .gate();
    let original = tool_request("original-target");
    let mut variants = vec![tool_request("changed-target")];
    let mut changed_version = original.clone();
    if let PolicyAction::ExecuteTool { input } = &mut changed_version.action {
        input.tool.version = id("2.0.0");
    }
    variants.push(changed_version);
    let mut changed_descriptor = original.clone();
    if let PolicyAction::ExecuteTool { input } = &mut changed_descriptor.action {
        input.descriptor_digest = digest("new descriptor");
    }
    variants.push(changed_descriptor);
    let mut changed_binding = original.clone();
    if let PolicyAction::ExecuteTool { input } = &mut changed_binding.action {
        input.binding_digest = digest("new binding");
    }
    variants.push(changed_binding);
    let mut changed_scope = original.clone();
    changed_scope.owner_scope.workspace_id = id("another-workspace");
    variants.push(changed_scope);
    let calls = OperationCounts::default();
    for request in variants {
        let challenge = calls
            .guarded(
                &gate,
                &request,
                &context(request.owner_scope.clone()),
                None,
                None,
            )
            .await
            .unwrap();
        let Guarded::ApprovalRequired(challenge) = challenge else {
            panic!("changed action was executed without approval");
        };
        assert_ne!(challenge.request_digest, original.digest());
        assert_eq!(challenge.request_digest, request.digest());
        assert_eq!(challenge.scope, request.owner_scope);
    }
    calls.assert_calls(0);
}

struct ModelCatalog;

impl ProfileResolver for ModelCatalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            if reference.kind != ComponentKind::ModelBinding || reference.id != id("primary") {
                return Err(ContractError::new(
                    ErrorCode::ComponentUnavailable,
                    "reference",
                ));
            }
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id("1.0.0")),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: digest("model manifest"),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: None,
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

fn record(name: &str) -> RecordRef {
    RecordRef {
        record_id: id(name),
        revision: 1,
        digest: digest(name),
    }
}

async fn snapshot() -> RunSnapshot {
    let profile = AgentProfile::from_json(
        &json!({
            "schema_version":"wickle.agent-profile.v1", "agent_id":"research", "version":"1.0.0",
            "name":"Research", "description":"Summarize documents", "instructions":{"text":"private-profile-instructions"},
            "model_binding":"primary", "tools":[], "skills":[], "connectors":[],
            "context_policy":{"strategy":"bounded"}, "output_contract":{"type":"text"},
            "limits":{"max_model_calls":4,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
        })
        .to_string(),
    )
    .unwrap();
    let profile = ProfileValidator::new(&ModelCatalog)
        .validate(&profile, &scope())
        .await
        .unwrap();
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "private-user-input".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    };
    let system_inputs = Some(SystemInputSnapshotRef {
        snapshot_ref: record("protected-system-inputs"),
        values_digest: digest("private-system-map"),
        definition_versions: BTreeMap::from([(id("workspace_id"), id("1"))]),
    });
    let usage = BudgetUsage {
        model_calls: 1,
        ..BudgetUsage::default()
    };
    RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id("run"),
        request_digest: admission_digest(&request, &profile, system_inputs.as_ref()),
        request,
        scope: scope(),
        timing: RunTiming::new(0, profile.profile().limits.max_elapsed_ms.get()).unwrap(),
        resume_receipts: vec![],
        reservations: vec![AttemptReservation {
            attempt_id: id("first-model-attempt"),
            kind: ReservationKind::Model {
                purpose: ModelPurpose::Agent,
            },
            reserved_at_ms: 0,
        }],
        limits: profile.profile().limits.clone(),
        profile,
        status: RunStatus::Failed,
        phase: RunPhase::Finish,
        model_step_id: None,
        usage: usage.clone(),
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs,
        wait: None,
        outcome: Some(RunOutcome {
            result: OutcomeResult::Failed {
                failure: Failure {
                    code: id("provider_unavailable"),
                    diagnostic_ref: Some(record("protected-diagnostic")),
                },
            },
            output: vec![InputContent::Text {
                text: "private-partial-output".into(),
            }],
            artifacts: vec![],
            usage,
            checkpoint_revision: 3,
            verification: None,
            unresolved_effects: vec![],
        }),
        assembly_ref: Some(record("protected-assembly")),
        routing_snapshot_ref: None,
        context_batches: vec![record("protected-context-batch")],
        source_states: vec![],
        revision: 3,
        last_event_seq: 2,
    }
}

fn artifact() -> ArtifactRef {
    ArtifactRef {
        artifact_id: id("artifact"),
        scope: scope(),
        media_type: id("text/plain"),
        size_bytes: 7,
        content_hash: id("sha256-abcdef"),
    }
}

fn event() -> RunEvent {
    RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("event"),
        scope: scope(),
        run_id: id("run"),
        session_id: id("session"),
        seq: NonZeroU64::new(2).unwrap(),
        timestamp_ms: 1000,
        payload: RunEventPayload::RunFinished {
            outcome_ref: record("protected-outcome"),
        },
    }
}

#[tokio::test]
async fn authorized_minimal_views_serialize_only_public_metadata_and_use_distinct_actions() {
    let policy = HostPolicy::new(Behavior::Decision(PolicyDecision::Allow {}));
    let gate = policy.gate();
    let context = context(scope());
    let snapshot = snapshot().await;
    snapshot.validate().unwrap();
    let Guarded::Completed(run) = gate.run_view(&snapshot, &context, None).await.unwrap() else {
        panic!("expected authorized public run view");
    };
    assert_eq!(
        serde_json::to_value(run).unwrap(),
        json!({"run_id":"run","session_id":"session","status":"failed","phase":"finish","revision":3,
            "usage":{"model_calls":1,"tool_attempts":0,"repair_attempts":0,"recovery_attempts":0,"elapsed_ms":0}})
    );
    let Guarded::Completed(artifact) = gate
        .artifact_view(&artifact(), &context, None)
        .await
        .unwrap()
    else {
        panic!("expected authorized artifact metadata");
    };
    assert_eq!(
        serde_json::to_value(artifact).unwrap(),
        json!({"artifact_id":"artifact","media_type":"text/plain","size_bytes":7,"content_hash":"sha256-abcdef"})
    );
    let Guarded::Completed(event) = gate.event_view(&event(), &context, None).await.unwrap() else {
        panic!("expected authorized event metadata");
    };
    assert_eq!(
        serde_json::to_value(event).unwrap(),
        json!({"event_id":"event","run_id":"run","session_id":"session","seq":2,"timestamp_ms":1000,"event_type":"run.finished"})
    );
    let observed = policy.observed.lock().unwrap();
    assert_eq!(observed[0].request.action, PolicyAction::ReadRun {});
    assert_eq!(observed[1].request.action, PolicyAction::ReadArtifact {});
    assert_eq!(observed[1].request.resource_id, id("artifact"));
    assert_eq!(observed[2].request.action, PolicyAction::ReadEvents {});
    assert_eq!(observed[2].request.resource_id, id("run"));
}

#[tokio::test]
async fn public_and_protected_views_reject_claimed_scopes_different_from_stored_owners() {
    let policy = HostPolicy::new(Behavior::Decision(PolicyDecision::Allow {}));
    let gate = policy.gate();
    let snapshot = snapshot().await;
    let mut wrong_scopes = vec![scope(); 3];
    wrong_scopes[0].tenant_id = id("other-tenant");
    wrong_scopes[1].workspace_id = id("other-workspace");
    wrong_scopes[2].user_id = Some(id("other-user"));
    for wrong_scope in wrong_scopes {
        let context = context(wrong_scope);
        assert_eq!(
            gate.run_view(&snapshot, &context, None)
                .await
                .unwrap_err()
                .code,
            ErrorCode::AccessDenied
        );
        assert_eq!(
            gate.run_details(&snapshot, &context, None)
                .await
                .unwrap_err()
                .code,
            ErrorCode::AccessDenied
        );
        assert_eq!(
            gate.artifact_view(&artifact(), &context, None)
                .await
                .unwrap_err()
                .code,
            ErrorCode::AccessDenied
        );
        assert_eq!(
            gate.event_view(&event(), &context, None)
                .await
                .unwrap_err()
                .code,
            ErrorCode::AccessDenied
        );
    }
    assert_eq!(policy.calls.load(Ordering::SeqCst), 0);
}

struct PublicOnlyPolicy;

impl PolicyPort for PublicOnlyPolicy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            Ok(match request.action {
                PolicyAction::ReadRun {} | PolicyAction::ReadEvents {} => PolicyDecision::Allow {},
                _ => PolicyDecision::Deny {
                    reason: id("detail_access_not_granted"),
                },
            })
        })
    }
}

#[tokio::test]
async fn public_read_permission_does_not_grant_access_to_protected_run_details() {
    let gate = PolicyGate::new(Arc::new(PublicOnlyPolicy), Duration::from_secs(1)).unwrap();
    let context = context(scope());
    let snapshot = snapshot().await;
    assert!(matches!(
        gate.run_view(&snapshot, &context, None).await.unwrap(),
        Guarded::Completed(_)
    ));
    assert!(matches!(
        gate.event_view(&event(), &context, None).await.unwrap(),
        Guarded::Completed(_)
    ));
    assert_eq!(
        gate.run_details(&snapshot, &context, None)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    let policy = HostPolicy::new(Behavior::Decision(PolicyDecision::Allow {}));
    let Guarded::Completed(details) = policy
        .gate()
        .run_details(&snapshot, &context, None)
        .await
        .unwrap()
    else {
        panic!("explicit detail permission did not grant the protected view");
    };
    assert_eq!(details, snapshot);
    assert_eq!(
        policy.observed.lock().unwrap()[0].request.action,
        PolicyAction::ReadRunDetails {}
    );
}
```

## `crates/wickle/tests/state_checkpoint.rs`

```rust
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
```

## `crates/wickle/tests/state_resume_invariants.rs`

```rust
//! Replay and transaction checks use actual waiting Agent executions as their seed.

#[path = "support/agent.rs"]
#[allow(dead_code)]
mod agent_support;
#[path = "support/agent_resume.rs"]
#[allow(dead_code)]
mod support;

use futures_util::stream;
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use support::*;
use wickle::*;

fn restore(image: &Value) -> Result<MemoryStateStore, ContractError> {
    StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(image))
        .map(MemoryStateStore::from_checkpoint)
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

fn replace_record(image: &mut Value, reference: &Value, value: Value) {
    let record = image["records"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|record| &record["reference"] == reference)
        .unwrap();
    record["value"] = value.clone();
    let mut updated = reference.clone();
    updated["digest"] = serde_json::to_value(canonical_digest(&value)).unwrap();
    replace_reference(image, reference, &updated);
}

struct TwoQuestions(AtomicUsize);
impl ModelPort for TwoQuestions {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: id("fixture"),
            adapter: reference("adapter"),
            connection_ref: reference("connection"),
        }
    }
    fn generate<'a>(
        &'a self,
        _: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let call = self.0.fetch_add(1, Ordering::SeqCst);
        let (event, finish) = if call < 2 {
            (
                ModelEvent::ToolArgumentsDelta {
                    index: 0,
                    provider_call_id: Some(format!("question-{call}")),
                    name: Some("target".into()),
                    delta: json!({"query":"source"}).to_string(),
                },
                ModelFinish::ToolCalls,
            )
        } else {
            (
                ModelEvent::TextDelta {
                    text: "Both source choices are saved.".into(),
                },
                ModelFinish::Stop,
            )
        };
        Box::pin(stream::iter(vec![
            Ok(event),
            Ok(ModelEvent::ResponseCompleted {
                finish,
                metadata: ModelResponseMetadata::default(),
                continuation: vec![],
            }),
        ]))
    }
}

#[tokio::test]
async fn restored_resume_receipts_have_distinct_events_and_the_exact_preceding_wait() {
    let fixture = Fixture::new(Mode::Input);
    let mut bindings = fixture.bindings();
    let model = Arc::new(TwoQuestions(AtomicUsize::new(0)));
    bindings.model_exchange = Arc::new(
        ModelExchange::new(model.clone(), bindings.policy.clone())
            .with_route_inspector(
                fixture.base.inspector.clone(),
                std::time::Duration::from_secs(1),
            )
            .unwrap(),
    );
    let agent = create_agent(fixture.profile.clone(), bindings).unwrap();
    let mut handle = fixture.started(&agent).await;
    for (number, selection) in ["annual", "quarterly"].into_iter().enumerate() {
        assert_eq!(
            fixture.outcome(&handle).await.result.status(),
            RunStatus::Waiting
        );
        let saved = fixture.saved(&handle).await;
        let command = ResumeCommand {
            run_id: handle.run_id().clone(),
            expected_revision: saved.snapshot.revision,
            command_id: id(&format!("answer-{number}")),
            action: ResumeAction::Input {
                wait_id: saved.snapshot.wait.unwrap().wait_id,
                answer: json!({"selection":selection}),
            },
        };
        handle = completed(agent.resume(command, context()).await.unwrap());
    }
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(model.0.load(Ordering::SeqCst), 3);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 2);
    let image =
        serde_json::to_value(fixture.base.store.export_checkpoint(&scope()).unwrap()).unwrap();
    let restored = restore(&image).unwrap();
    assert_eq!(
        restored.load(&scope(), handle.run_id()).await.unwrap(),
        fixture.saved(&handle).await
    );
    for fault in 0..4 {
        let mut corrupt = image.clone();
        match fault {
            0 => {
                let events = corrupt["runs"][0]["events"].as_array_mut().unwrap();
                let indices: Vec<_> = events
                    .iter()
                    .enumerate()
                    .filter_map(|(i, event)| {
                        (event["payload"]["type"] == "run.resumed").then_some(i)
                    })
                    .collect();
                assert_eq!(indices.len(), 2);
                events[indices[1]]["payload"]["command_ref"] =
                    events[indices[0]]["payload"]["command_ref"].clone();
            }
            1 => {
                corrupt["runs"][0]["snapshot"]["resume_receipts"][0]["previous_last_event_seq"] =
                    json!(1)
            }
            2 => {
                let receipt = &mut corrupt["runs"][0]["snapshot"]["resume_receipts"][0];
                receipt["command"]["action"]["answer"] = json!({"selection":"quarterly"});
                let command = receipt["command"].clone();
                let reference = receipt["command_ref"].clone();
                replace_record(&mut corrupt, &reference, command);
            }
            3 => corrupt["runs"][0]["snapshot"]["resume_receipts"][0]["expired"] = json!(true),
            _ => unreachable!(),
        }
        assert!(
            restore(&corrupt).is_err(),
            "resume history fault {fault} was accepted"
        );
    }
}

#[tokio::test]
async fn an_external_correction_cannot_outlive_its_authorized_settlement_history() {
    let fixture = Fixture::new(Mode::External);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    assert_eq!(
        fixture.outcome(&original).await.result.status(),
        RunStatus::Waiting
    );
    let before = fixture.saved(&original).await;
    let command = ResumeCommand {
        run_id: original.run_id().clone(),
        expected_revision: before.snapshot.revision,
        command_id: id("external-proof"),
        action: ResumeAction::External {
            wait_id: before.snapshot.wait.unwrap().wait_id,
            receipt_ref: fixture.store.proof.reference().clone(),
        },
    };
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&resumed).await.result.status(),
        RunStatus::Succeeded
    );
    let image =
        serde_json::to_value(fixture.base.store.export_checkpoint(&scope()).unwrap()).unwrap();
    restore(&image).unwrap();
    let mut corrupt = image.clone();
    let messages = corrupt["sessions"][0]["messages"].as_array_mut().unwrap();
    let before = messages.len();
    messages.retain(|message| message["content"][0]["type"] != "tool_result_correction");
    assert_eq!(messages.len(), before - 1);
    for (index, message) in messages.iter_mut().enumerate() {
        message["sequence"] = json!(index + 1);
    }
    let count = messages.len();
    corrupt["sessions"][0]["snapshot"]["transcript_revision"] = json!(count);
    assert!(
        restore(&corrupt).is_err(),
        "settled effect restored without its correction"
    );
}

async fn replay_acceptance(before: &Value, after: &Value) -> (MemoryStateStore, CommitInput) {
    let store = restore(before).unwrap();
    let run_id: Id =
        serde_json::from_value(before["runs"][0]["snapshot"]["run_id"].clone()).unwrap();
    let saved = store.load(&scope(), &run_id).await.unwrap();
    let final_snapshot = RunSnapshot::from_json(&after["runs"][0]["snapshot"].to_string()).unwrap();
    let receipt = final_snapshot.resume_receipts[0].clone();
    let all_events: Vec<RunEvent> =
        serde_json::from_value(after["runs"][0]["events"].clone()).unwrap();
    let resumed = all_events
        .iter()
        .find(|event| {
            matches!(&event.payload,
        RunEventPayload::RunResumed { command_ref } if command_ref == &receipt.command_ref)
        })
        .unwrap();
    let events: Vec<_> = all_events
        .iter()
        .filter(|event| event.seq.get() > saved.snapshot.last_event_seq && event.seq <= resumed.seq)
        .cloned()
        .collect();
    assert_eq!(
        events.len(),
        2,
        "one settlement and one acceptance are expected"
    );
    let now_ms = resumed.timestamp_ms;
    let raw_lease = &before["runs"][0]["lease"];
    let lease = if raw_lease.is_null() {
        store
            .acquire_lease(&scope(), &run_id, &id("transaction-review"), now_ms, 60_000)
            .await
            .unwrap()
    } else {
        RunLease {
            scope: scope(),
            run_id: run_id.clone(),
            owner: serde_json::from_value(raw_lease["owner"].clone()).unwrap(),
            fencing_token: raw_lease["fencing_token"].as_u64().unwrap(),
            expires_at_ms: raw_lease["expires_at_ms"].as_i64().unwrap(),
        }
    };
    let mut snapshot = saved.snapshot.clone();
    snapshot.revision = receipt.accepted_revision;
    snapshot.status = RunStatus::Running;
    snapshot.phase = RunPhase::Tool;
    snapshot.wait = None;
    snapshot.outcome = None;
    snapshot.last_event_seq = resumed.seq.get();
    snapshot.usage.elapsed_ms = (now_ms - snapshot.timing.started_at_ms) as u64;
    snapshot.timing.last_observed_at_ms = now_ms;
    snapshot.resume_receipts.push(receipt);
    snapshot.tool_ledger[1].state = final_snapshot.tool_ledger[1].state.clone();
    let all_messages: Vec<Message> =
        serde_json::from_value(after["sessions"][0]["messages"].clone()).unwrap();
    let messages = all_messages
        .into_iter()
        .filter(|message| message.sequence.get() == saved.session.transcript_revision + 1)
        .collect();
    let records = after["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|record| {
            let reference: RecordRef = serde_json::from_value(record["reference"].clone()).unwrap();
            ProtectedRecord::new(
                reference.record_id,
                reference.revision,
                record["value"].clone(),
            )
        })
        .collect();
    (
        store,
        CommitInput {
            expected_revision: saved.snapshot.revision,
            lease,
            now_ms,
            snapshot,
            messages,
            events,
            records,
        },
    )
}

#[tokio::test]
async fn accepting_a_command_requires_its_target_settlement_in_the_same_transaction() {
    for (case, mode) in [Mode::Input, Mode::Approval, Mode::External]
        .into_iter()
        .enumerate()
    {
        let fixture = Fixture::new(mode);
        let agent = fixture.agent();
        let original = fixture.started(&agent).await;
        assert_eq!(
            fixture.outcome(&original).await.result.status(),
            RunStatus::Waiting
        );
        let saved = fixture.saved(&original).await;
        let before =
            serde_json::to_value(fixture.base.store.export_checkpoint(&scope()).unwrap()).unwrap();
        let wait = saved.snapshot.wait.clone().unwrap();
        let action = match wait.target {
            WaitTarget::Input { .. } => ResumeAction::Input {
                wait_id: wait.wait_id,
                answer: json!({"selection":"annual"}),
            },
            WaitTarget::Approval { target } => ResumeAction::Deny {
                wait_id: wait.wait_id,
                target,
                reason: "Declined by reviewer".into(),
            },
            WaitTarget::External { .. } => ResumeAction::External {
                wait_id: wait.wait_id,
                receipt_ref: fixture.store.proof.reference().clone(),
            },
        };
        let command = ResumeCommand {
            run_id: original.run_id().clone(),
            expected_revision: saved.snapshot.revision,
            command_id: id("acceptance"),
            action,
        };
        let resumed = completed(agent.resume(command, context()).await.unwrap());
        assert_eq!(
            fixture.outcome(&resumed).await.result.status(),
            RunStatus::Succeeded
        );
        let after =
            serde_json::to_value(fixture.base.store.export_checkpoint(&scope()).unwrap()).unwrap();
        let (positive_store, positive) = replay_acceptance(&before, &after).await;
        positive_store
            .commit(&scope(), original.run_id(), positive)
            .await
            .unwrap();
        let (delayed_store, mut delayed) = replay_acceptance(&before, &after).await;
        delayed.now_ms += 5;
        delayed_store
            .commit(&scope(), original.run_id(), delayed)
            .await
            .unwrap();

        for fault in 0..2 {
            let (store, mut candidate) = replay_acceptance(&before, &after).await;
            if fault == 0 {
                candidate.events.remove(0);
                candidate.events[0].seq = (saved.snapshot.last_event_seq + 1).try_into().unwrap();
                candidate.snapshot.last_event_seq -= 1;
                candidate.messages.clear();
            } else if let ResumeAction::Input { answer, .. } =
                &mut candidate.snapshot.resume_receipts[0].command.action
            {
                *answer = json!({"selection":"quarterly"});
                let receipt = &mut candidate.snapshot.resume_receipts[0];
                let record = candidate
                    .records
                    .iter_mut()
                    .find(|record| record.reference() == &receipt.command_ref)
                    .unwrap();
                *record = ProtectedRecord::new(
                    record.reference().record_id.clone(),
                    record.reference().revision,
                    serde_json::to_value(&receipt.command).unwrap(),
                );
                receipt.command_ref = record.reference().clone();
                let RunEventPayload::RunResumed { command_ref } = &mut candidate.events[1].payload
                else {
                    unreachable!()
                };
                *command_ref = receipt.command_ref.clone();
            } else {
                // Keep the old unresolved/pending target but supply the accepted
                // command and a perfectly well-formed settlement event for it.
                candidate.snapshot.tool_ledger[1].state =
                    saved.snapshot.tool_ledger[1].state.clone();
            }
            let initial = store.load(&scope(), original.run_id()).await.unwrap();
            assert!(
                store
                    .commit(&scope(), original.run_id(), candidate)
                    .await
                    .is_err(),
                "accepted malformed transaction {case}/{fault}"
            );
            assert_eq!(
                store.load(&scope(), original.run_id()).await.unwrap(),
                initial
            );
        }
        let (expired_store, mut stale) = replay_acceptance(&before, &after).await;
        let deadline = stale.snapshot.timing.deadline_at_ms;
        stale.lease = expired_store
            .renew_lease(
                &scope(),
                original.run_id(),
                &stale.lease,
                stale.now_ms,
                (deadline - stale.now_ms + 1_000) as u64,
            )
            .await
            .unwrap();
        stale.now_ms = deadline;
        let initial = expired_store
            .load(&scope(), original.run_id())
            .await
            .unwrap();
        assert_eq!(
            expired_store
                .commit(&scope(), original.run_id(), stale)
                .await
                .unwrap_err()
                .code,
            ErrorCode::DeadlineExceeded,
            "queue delay must not accept an expired decision"
        );
        assert_eq!(
            expired_store
                .load(&scope(), original.run_id())
                .await
                .unwrap(),
            initial
        );
    }
}
```

## `crates/wickle/tests/support/agent.rs`

```rust
//! Deterministic Host components for agent runtime lifecycle tests.

use futures_util::{StreamExt, stream};
use serde_json::json;
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};
use wickle::*;

pub fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
pub fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    }
}
pub fn reference(name: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id("1"),
    }
}
pub fn profile() -> AgentProfile {
    AgentProfile::from_json(r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
        "name":"Assistant","description":"Runtime fixture","instructions":{"text":"Use supplied records"},
        "model_binding":"primary","tools":[],"skills":[],"connectors":[],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":4,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
    }"#).unwrap()
}
pub fn request(name: &str) -> RunRequest {
    RunRequest {
        request_id: id(name),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Retrieve the requested information".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    }
}
pub fn context() -> ExecutionContext {
    ExecutionContext::new(
        ExecutionContextData {
            scope: scope(),
            principal_ref: id("caller"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    )
}
pub fn completed<T>(result: Guarded<T>) -> T {
    match result {
        Guarded::Completed(value) => value,
        Guarded::ApprovalRequired(_) => panic!("unexpected approval"),
    }
}

pub struct TestClock {
    origin: tokio::time::Instant,
}
impl TestClock {
    pub fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
        }
    }
}
impl Clock for TestClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        let elapsed = self.origin.elapsed().as_millis() as u64;
        Ok(ClockReading {
            utc_ms: 1000 + elapsed as i64,
            monotonic_ms: elapsed,
        })
    }
    fn sleep_until<'a>(&'a self, deadline: u64) -> PortFuture<'a, ()> {
        Box::pin(async move {
            tokio::time::sleep_until(self.origin + Duration::from_millis(deadline)).await;
            Ok(())
        })
    }
}
#[derive(Default)]
pub struct Ids(pub AtomicUsize);
impl IdSource for Ids {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id(&format!("id-{}", self.0.fetch_add(1, Ordering::SeqCst))))
    }
}

#[derive(Default)]
pub struct Catalog {
    pub calls: AtomicUsize,
    pub revision: AtomicUsize,
}
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id(&format!(
                        "revision-{}",
                        self.revision.load(Ordering::SeqCst)
                    ))),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!(self.revision.load(Ordering::SeqCst))),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: None,
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

#[derive(Default)]
pub struct Policy {
    pub calls: AtomicUsize,
    pub deny: AtomicUsize,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let deny = match self.deny.load(Ordering::SeqCst) {
                1 => matches!(
                    request.action,
                    PolicyAction::ReadRun {}
                        | PolicyAction::ReadRunDetails {}
                        | PolicyAction::ReadEvents {}
                ),
                2 => matches!(request.action, PolicyAction::CancelRun {}),
                3 => matches!(request.action, PolicyAction::StartRun {}),
                _ => false,
            };
            Ok(if deny {
                PolicyDecision::Deny {
                    reason: id("denied"),
                }
            } else {
                PolicyDecision::Allow {}
            })
        })
    }
}

pub struct Router {
    pub snapshot: RoutingSnapshot,
    pub queries: AtomicUsize,
    pub snapshots: AtomicUsize,
}
impl Router {
    pub fn new() -> Self {
        Self::for_provider("fixture")
    }
    pub fn for_provider(provider: &str) -> Self {
        let capabilities = ModelCapabilities {
            revision: id("capabilities"),
            features: [id("text")].into_iter().collect(),
            options_schema: json!({"type":"object","properties":{"effort":{"enum":["low","high"]}},"additionalProperties":false}),
            context_window: 8192.try_into().unwrap(),
            max_output_tokens: 2048.try_into().unwrap(),
        };
        let model = ModelDefinition {
            model_key: id("model"),
            family: id("fixture"),
            provider: id(provider),
            model_id: id("fixture-model"),
            model_version: id("release"),
            version_semantics: VersionSemantics::Pinned,
            lifecycle: ModelLifecycle::Active,
            capabilities: capabilities.clone(),
            evidence: vec![],
        };
        let mut binding = ModelBinding {
            binding: reference("route"),
            model: model.reference(),
            requested_model: model.model_id.clone(),
            adapter: reference("adapter"),
            connection_ref: reference("connection"),
            target: JsonObject::new(),
            target_schema: json!({"type":"object","additionalProperties":false}),
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("v1"),
            },
            deployment_revision: None,
            version_semantics: VersionSemantics::Pinned,
            capabilities,
            support: ModelSupportStatus::ContractTested,
            evidence: vec![],
        };
        binding.evidence.push(ModelValidationEvidence {
            kind: ModelValidationKind::ContractTest,
            binding_digest: binding.contract_digest(&model).unwrap(),
            checked_at_ms: 1000,
            evidence_ref: id("fixture-proof"),
            passed: true,
        });
        let snapshot = RoutingSnapshot::new(
            ModelCatalogSnapshot {
                revision: id("catalog"),
                scope: scope(),
                models: vec![model],
                bindings: vec![binding],
                aliases: vec![],
            },
            RoutingPolicy {
                revision: id("policy"),
                scope: scope(),
                rules: vec![RoutingRule {
                    model_binding: id("primary"),
                    purpose: ModelPurpose::Agent,
                    primary: reference("route"),
                    fallbacks: vec![],
                    fallback_on: vec![],
                    version_policy: VersionPolicy::RequirePinned,
                    min_support: ModelSupportStatus::ContractTested,
                }],
            },
        )
        .unwrap();
        Self {
            snapshot,
            queries: AtomicUsize::new(0),
            snapshots: AtomicUsize::new(0),
        }
    }
}
impl ModelRouter for Router {
    fn snapshot(&self) -> &RoutingSnapshot {
        self.snapshots.fetch_add(1, Ordering::SeqCst);
        &self.snapshot
    }
    fn resolve<'a>(&'a self, request: &'a RouteRequest) -> PortFuture<'a, RouteSelection> {
        Box::pin(async move {
            self.queries.fetch_add(1, Ordering::SeqCst);
            let selected = RouteSelection {
                route: self.snapshot.route_for_binding(&reference("route"))?,
                reason: if request.previous_route.is_some() {
                    RouteSelectionReason::Reuse
                } else {
                    RouteSelectionReason::Initial
                },
                candidate_index: 0,
                routing_snapshot_digest: self.snapshot.digest(),
                request_digest: request.digest(),
            };
            self.snapshot.validate_selection(request, &selected)?;
            Ok(selected)
        })
    }
}

pub struct Inspector {
    pub calls: AtomicUsize,
}
impl ModelRouteInspector for Inspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        _: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: ModelRouteAvailability::Available,
                model_id: Some(id("fixture-model")),
                model_version: Some(id("release")),
                deployment_revision: None,
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id("synthetic-inspection"),
            })
        })
    }
}
pub struct Estimator {
    pub calls: AtomicUsize,
    pub tokens: AtomicUsize,
}
impl ModelTokenEstimator for Estimator {
    fn estimate(&self, _: &ModelRequest) -> Result<u64, ContractError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.tokens.load(Ordering::SeqCst) as u64)
    }
}

#[derive(Clone, Copy)]
pub enum Response {
    Text,
    WithContinuation,
    TransportFailure,
    Truncated,
    WaitAfterText,
    Panic,
    Tool,
}
pub struct Model {
    pub calls: AtomicUsize,
    pub entered: Notify,
    pub release: Semaphore,
    pub response: Response,
    pub requests: Mutex<Vec<ModelRequest>>,
    pub gated: bool,
    pub port_binding: ModelPortBinding,
}
impl Model {
    pub fn new(response: Response, gated: bool) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Semaphore::new(0),
            response,
            requests: Mutex::new(vec![]),
            gated,
            port_binding: ModelPortBinding {
                provider: id("fixture"),
                adapter: reference("adapter"),
                connection_ref: reference("connection"),
            },
        }
    }
}
impl ModelPort for Model {
    fn binding(&self) -> ModelPortBinding {
        self.port_binding.clone()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        self.entered.notify_one();
        Box::pin(
            stream::once(async move {
                if self.gated {
                    self.release.acquire().await.unwrap().forget();
                }
                if matches!(self.response, Response::Panic) {
                    panic!("synthetic adapter panic");
                }
                let mut events = vec![Ok(ModelEvent::TextDelta {
                    text: "candidate answer".into(),
                })];
                match self.response {
                    Response::Text | Response::Panic | Response::WithContinuation => {
                        events.push(Ok(ModelEvent::ResponseCompleted {
                            finish: ModelFinish::Stop,
                            metadata: ModelResponseMetadata::default(),
                            continuation: if matches!(self.response, Response::WithContinuation) {
                                vec![OpaqueContinuation::new(
                                    &request.route,
                                    json!({"signature":"fixture-signature"}),
                                )]
                            } else {
                                vec![]
                            },
                        }))
                    }
                    Response::TransportFailure => events.push(Ok(ModelEvent::ResponseError {
                        kind: ModelFailureKind::Transport,
                        metadata: ModelResponseMetadata::default(),
                    })),
                    Response::Truncated => events.push(Ok(ModelEvent::ResponseCompleted {
                        finish: ModelFinish::Length,
                        metadata: ModelResponseMetadata::default(),
                        continuation: vec![],
                    })),
                    Response::Tool => {
                        events.push(Ok(ModelEvent::ToolArgumentsDelta {
                            index: 0,
                            provider_call_id: Some("call".into()),
                            name: Some("unregistered".into()),
                            delta: "{}".into(),
                        }));
                        events.push(Ok(ModelEvent::ResponseCompleted {
                            finish: ModelFinish::ToolCalls,
                            metadata: ModelResponseMetadata::default(),
                            continuation: vec![],
                        }));
                    }
                    Response::WaitAfterText => {}
                }
                let trailing = if matches!(self.response, Response::WaitAfterText) {
                    stream::pending().boxed()
                } else {
                    stream::empty().boxed()
                };
                stream::iter(events).chain(trailing)
            })
            .flatten(),
        )
    }
}

pub struct Fixture {
    pub store: Arc<MemoryStateStore>,
    pub policy: Arc<Policy>,
    pub catalog: Arc<Catalog>,
    pub router: Arc<Router>,
    pub inspector: Arc<Inspector>,
    pub estimator: Arc<Estimator>,
    pub model: Arc<Model>,
    pub clock: Arc<TestClock>,
    pub ids: Arc<Ids>,
}

#[derive(Clone, Copy)]
pub enum FinalCommitMode {
    PassThrough,
    Reject,
    LoseAcknowledgement,
    Pause,
    PauseEmptyEventPage,
}
pub struct FinalCommitStore {
    pub inner: Arc<MemoryStateStore>,
    pub mode: FinalCommitMode,
    pub final_entered: Notify,
    pub release: Semaphore,
    pub final_attempts: AtomicUsize,
    pub empty_page_entered: Notify,
    pub empty_page_release: Semaphore,
    paused_empty_page: AtomicBool,
    pub block_read: AtomicUsize,
    pub read_entered: Notify,
}
impl FinalCommitStore {
    pub fn new(inner: Arc<MemoryStateStore>, mode: FinalCommitMode) -> Self {
        Self {
            inner,
            mode,
            final_entered: Notify::new(),
            release: Semaphore::new(0),
            final_attempts: AtomicUsize::new(0),
            empty_page_entered: Notify::new(),
            empty_page_release: Semaphore::new(0),
            paused_empty_page: AtomicBool::new(false),
            block_read: AtomicUsize::new(0),
            read_entered: Notify::new(),
        }
    }
}
impl StateStore for FinalCommitStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        s: &'a Scope,
        session: &'a Id,
        request: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        Box::pin(async move {
            if self
                .block_read
                .compare_exchange(3, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.read_entered.notify_one();
                std::future::pending::<()>().await;
            }
            self.inner.find_request(s, session, request).await
        })
    }
    fn admit<'a>(&'a self, s: &'a Scope, input: AdmissionInput) -> PortFuture<'a, AdmissionResult> {
        self.inner.admit(s, input)
    }
    fn load<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            if self
                .block_read
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.read_entered.notify_one();
                std::future::pending::<()>().await;
            }
            self.inner.load(s, r).await
        })
    }
    fn load_session<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, SessionSnapshot> {
        self.inner.load_session(s, r)
    }
    fn check_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.check_lease(s, r, l, n)
    }
    fn acquire_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        o: &'a Id,
        n: i64,
        t: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.acquire_lease(s, r, o, n, t)
    }
    fn renew_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
        t: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.renew_lease(s, r, l, n, t)
    }
    fn release_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, ()> {
        self.inner.release_lease(s, r, l, n)
    }
    fn read_events<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        after: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage> {
        Box::pin(async move {
            if self
                .block_read
                .compare_exchange(2, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.read_entered.notify_one();
                std::future::pending::<()>().await;
            }
            let page = self.inner.read_events(s, r, after, limit).await?;
            if matches!(self.mode, FinalCommitMode::PauseEmptyEventPage)
                && page.events.is_empty()
                && !self.paused_empty_page.swap(true, Ordering::SeqCst)
            {
                self.empty_page_entered.notify_one();
                self.empty_page_release.acquire().await.unwrap().forget();
            }
            Ok(page)
        })
    }
    fn read_record<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        self.inner.read_record(s, r)
    }
    fn commit<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            if !input.snapshot.status.is_terminal() {
                return self.inner.commit(s, r, input).await;
            }
            self.final_attempts.fetch_add(1, Ordering::SeqCst);
            self.final_entered.notify_one();
            match self.mode {
                FinalCommitMode::Reject => Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "final.commit",
                )),
                FinalCommitMode::LoseAcknowledgement => {
                    self.inner.commit(s, r, input).await?;
                    Err(ContractError::new(
                        ErrorCode::PersistenceUnavailable,
                        "final.ack",
                    ))
                }
                FinalCommitMode::Pause => {
                    self.release.acquire().await.unwrap().forget();
                    self.inner.commit(s, r, input).await
                }
                FinalCommitMode::PauseEmptyEventPage | FinalCommitMode::PassThrough => {
                    self.inner.commit(s, r, input).await
                }
            }
        })
    }
}
impl Fixture {
    pub fn new(response: Response, gated: bool) -> Self {
        Self {
            store: Arc::new(MemoryStateStore::new()),
            policy: Arc::new(Policy::default()),
            catalog: Arc::new(Catalog::default()),
            router: Arc::new(Router::new()),
            inspector: Arc::new(Inspector {
                calls: AtomicUsize::new(0),
            }),
            estimator: Arc::new(Estimator {
                calls: AtomicUsize::new(0),
                tokens: AtomicUsize::new(32),
            }),
            model: Arc::new(Model::new(response, gated)),
            clock: Arc::new(TestClock::new()),
            ids: Arc::new(Ids::default()),
        }
    }
    pub fn bindings(&self) -> AgentBindings {
        let gate = Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap());
        AgentBindings {
            scope: scope(),
            state: self.store.clone(),
            policy: gate.clone(),
            profile_resolver: self.catalog.clone(),
            model_exchange: Arc::new(
                ModelExchange::new(self.model.clone(), gate)
                    .with_route_inspector(self.inspector.clone(), Duration::from_secs(1))
                    .unwrap(),
            ),
            router: self.router.clone(),
            host_instructions: vec!["Trusted host rules".into()],
            system_inputs: SystemInputRegistry::new(vec![]).unwrap(),
            clock: self.clock.clone(),
            ids: self.ids.clone(),
            tools: None,
            system_input_resolver: None,
            external_receipt_verifier: None,
            token_estimator: self.estimator.clone(),
            settings: AgentSettings {
                observer_poll_ms: 1,
                heartbeat_interval_ms: 100,
                lease_ttl_ms: 1000,
                max_output_tokens: 128.try_into().unwrap(),
                ..AgentSettings::default()
            },
        }
    }
    pub fn agent(&self) -> Agent {
        create_agent(profile(), self.bindings()).unwrap()
    }
    pub async fn started(&self, agent: &Agent, name: &str) -> RunHandle {
        completed(agent.start(request(name), context()).await.unwrap())
    }
}
```

## `crates/wickle/tests/support/agent_resume.rs`

```rust
//! Observable model, policy, resolver, and tool behavior across saved execution segments.

use super::agent_support;
pub use agent_support::{completed, context, id, reference, request, scope};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};
use wickle::*;

pub const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";
pub const RECORD: &str = "22222222-2222-4222-8222-222222222222";
pub const CHANGED_RECORD: &str = "33333333-3333-4333-8333-333333333333";
pub fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id("1")),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("resume-tools")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| reference.id.clone()),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Approval,
    LateApproval,
    Input,
    External,
}

pub struct Policy {
    pub mode: Mode,
    pub deny_resume: AtomicBool,
    pub deny_execute: AtomicBool,
    pub deny_cancel: AtomicBool,
    pub deny_receipt_read: AtomicBool,
    pub deny_details: AtomicBool,
    pub details_checks: AtomicUsize,
    pub require_resume_approval: AtomicBool,
    pub target_checks: AtomicUsize,
    pub resume_checks: Mutex<Vec<(ResumeCommand, Id)>>,
    pub tool_checks: Mutex<Vec<(ToolPolicyInput, Id)>>,
    pub pause_next_resume: AtomicBool,
    pub resume_entered: Notify,
    pub resume_release: Semaphore,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if matches!(request.action, PolicyAction::ReadRunDetails {}) {
                self.details_checks.fetch_add(1, Ordering::SeqCst);
                if self.deny_details.load(Ordering::SeqCst) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("details_revoked"),
                    });
                }
            }
            if let PolicyAction::ResumeRun { command, .. } = &request.action {
                self.resume_checks
                    .lock()
                    .unwrap()
                    .push(((**command).clone(), context.principal_ref.clone()));
                if self.pause_next_resume.swap(false, Ordering::SeqCst) {
                    self.resume_entered.notify_one();
                    self.resume_release.acquire().await.unwrap().forget();
                }
                if self.deny_resume.load(Ordering::SeqCst) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("resume_revoked"),
                    });
                }
                if self.require_resume_approval.load(Ordering::SeqCst) {
                    return Ok(PolicyDecision::RequireApproval {
                        reason: id("resume_review"),
                    });
                }
            }
            if matches!(request.action, PolicyAction::CancelRun {})
                && self.deny_cancel.load(Ordering::SeqCst)
            {
                return Ok(PolicyDecision::Deny {
                    reason: id("cancel_revoked"),
                });
            }
            if matches!(request.action, PolicyAction::ReadRecord { .. })
                && self.deny_receipt_read.load(Ordering::SeqCst)
            {
                return Ok(PolicyDecision::Deny {
                    reason: id("receipt_revoked"),
                });
            }
            if let PolicyAction::ExecuteTool { input } = &request.action {
                self.tool_checks
                    .lock()
                    .unwrap()
                    .push((input.clone(), context.principal_ref.clone()));
                if input.tool.id == id("target") {
                    let check = self.target_checks.fetch_add(1, Ordering::SeqCst) + 1;
                    if self.deny_execute.load(Ordering::SeqCst)
                        || input.execution_args().get("workspace_id") != Some(&json!(WORKSPACE))
                    {
                        return Ok(PolicyDecision::Deny {
                            reason: id("target_revoked"),
                        });
                    }
                    if input.approval().is_none()
                        && (self.mode == Mode::Approval
                            || (self.mode == Mode::LateApproval && check >= 3))
                    {
                        return Ok(PolicyDecision::RequireApproval {
                            reason: id("target_review"),
                        });
                    }
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}

pub struct Resolver {
    pub calls: AtomicUsize,
    pub value: Mutex<ResolvedSystemInput>,
}
impl SystemInputResolver for Resolver {
    fn resolve<'a>(
        &'a self,
        request: &'a SystemInputResolveRequest,
        context: &'a SystemInputResolveContext,
    ) -> PortFuture<'a, Option<ResolvedSystemInput>> {
        Box::pin(async move {
            assert_eq!(request.key, id("record_id"));
            assert_eq!(context.scope, scope());
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(self.value.lock().unwrap().clone()))
        })
    }
}

pub struct Model {
    pub calls: AtomicUsize,
    pub requests: Mutex<Vec<ModelRequest>>,
    pub hold_final: AtomicBool,
    pub final_entered: Notify,
    pub final_release: Semaphore,
}
impl ModelPort for Model {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: id("fixture"),
            adapter: reference("adapter"),
            connection_ref: reference("connection"),
        }
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        let completion = |finish| {
            Ok(ModelEvent::ResponseCompleted {
                finish,
                metadata: ModelResponseMetadata::default(),
                continuation: vec![],
            })
        };
        if attempt == 0 {
            let mut events: Vec<_> = ["before", "target", "after"]
                .iter()
                .enumerate()
                .map(|(index, name)| {
                    Ok(ModelEvent::ToolArgumentsDelta {
                        index: index as u32,
                        provider_call_id: Some(format!("provider-{index}")),
                        name: Some((*name).into()),
                        delta: serde_json::to_string(&json!({"query":name})).unwrap(),
                    })
                })
                .collect();
            events.push(completion(ModelFinish::ToolCalls));
            Box::pin(stream::iter(events))
        } else {
            Box::pin(
                stream::once(async move {
                    if self.hold_final.load(Ordering::SeqCst) {
                        self.final_entered.notify_one();
                        self.final_release.acquire().await.unwrap().forget();
                    }
                    Ok(ModelEvent::TextDelta {
                        text: "Recorded observations processed".into(),
                    })
                })
                .chain(stream::iter([completion(ModelFinish::Stop)])),
            )
        }
    }
}

pub struct Invocation {
    pub arguments: JsonObject,
    pub context: ToolExecutionContext,
}
pub struct Tool {
    pub name: &'static str,
    pub mode: Mode,
    pub calls: AtomicUsize,
    pub applied: AtomicUsize,
    pub apply_write: AtomicBool,
    pub seen: Mutex<Vec<Invocation>>,
    pub order: Arc<Mutex<Vec<&'static str>>>,
}
impl ToolExecutor for Tool {
    fn execute<'a>(
        &'a self,
        arguments: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.seen.lock().unwrap().push(Invocation {
                arguments: arguments.clone(),
                context: context.clone(),
            });
            self.order.lock().unwrap().push(self.name);
            if self.name != "target" {
                return Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Succeeded {
                        value: json!(self.name),
                    },
                    effect: ToolEffect::NotApplied,
                    receipt: None,
                });
            }
            if self.mode == Mode::Input {
                return Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::InputRequired {
                        question: "Which report should be selected?".into(),
                    },
                    effect: ToolEffect::NotApplied,
                    receipt: None,
                });
            }
            if self.apply_write.load(Ordering::SeqCst) {
                self.applied.fetch_add(1, Ordering::SeqCst);
            }
            if self.mode == Mode::External {
                return Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Failed {
                        code: id("response_lost"),
                    },
                    effect: ToolEffect::Unknown,
                    receipt: None,
                });
            }
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: json!("target"),
                },
                effect: ToolEffect::Applied,
                receipt: Some(
                    json!({"effect_id":"stored-effect","record_id":arguments["record_id"]}),
                ),
            })
        })
    }
}

pub struct Fixture {
    pub base: agent_support::Fixture,
    pub model: Arc<Model>,
    pub policy: Arc<Policy>,
    pub resolver: Arc<Resolver>,
    pub tools: Vec<Arc<Tool>>,
    pub registry: Arc<ToolRegistry>,
    pub inputs: SystemInputRegistry,
    pub profile: AgentProfile,
    pub order: Arc<Mutex<Vec<&'static str>>>,
    pub store: Arc<CommandStore>,
    pub verifier: Arc<Verifier>,
}
impl Fixture {
    pub fn new(mode: Mode) -> Self {
        let base = agent_support::Fixture::new(agent_support::Response::Text, false);
        let inputs = SystemInputRegistry::new(vec![
            SystemInputDefinition {
                key: id("workspace_id"),
                version: id("1"),
                value_schema: json!({"type":"string","format":"uuid"}),
                source: SystemInputSource::Run {},
            },
            SystemInputDefinition {
                key: id("record_id"),
                version: id("1"),
                value_schema: json!({"type":"string","format":"uuid"}),
                source: SystemInputSource::Resolver {
                    resolver_ref: reference("records"),
                },
            },
        ])
        .unwrap();
        let mut profile = agent_support::profile();
        profile.limits.max_tool_attempts = 6;
        let order = Arc::new(Mutex::new(vec![]));
        let mut tools = vec![];
        let mut registrations = vec![];
        for name in ["before", "target", "after"] {
            let target = name == "target";
            let compiled = SchemaCompiler::new().compile(ToolDescriptor {
                tool: reference(name), name: id(name), description: format!("Observe {name}"),
                input_schema: if target { json!({"type":"object","properties":{"query":{"type":"string"},"workspace_id":{"type":"string","format":"uuid"},"record_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id","record_id"],"additionalProperties":false}) }
                    else { json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}) },
                agent_parameters: vec!["query".into()], system_bindings: None,
                output_schema: if target && mode == Mode::Input { json!({"type":"object","properties":{"selection":{"type":"string","enum":["annual","quarterly"]}},"required":["selection"],"additionalProperties":false}) } else { json!({"type":"string"}) },
                side_effect: if target && mode != Mode::Input { ToolSideEffect::Write } else { ToolSideEffect::ReadOnly },
                concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: mode == Mode::External,
                max_output_bytes: 4096.try_into().unwrap(),
            }, &inputs).unwrap();
            let tool = Arc::new(Tool {
                name,
                mode,
                calls: AtomicUsize::new(0),
                applied: AtomicUsize::new(0),
                apply_write: AtomicBool::new(true),
                seen: Mutex::new(vec![]),
                order: order.clone(),
            });
            registrations.push(ToolRegistration {
                compiled,
                executor: tool.clone(),
            });
            tools.push(tool);
            profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
                tool_id: id(name),
                version: id("1"),
                bindings: None,
                config: None,
            }));
        }
        let store = Arc::new(CommandStore {
            inner: base.store.clone(),
            mode: AtomicUsize::new(0),
            consumed_commits: AtomicUsize::new(0),
            wait_timeout_ms: AtomicUsize::new(0),
            pause_record: Mutex::new(None),
            record_entered: Notify::new(),
            record_release: Semaphore::new(0),
            proof: ProtectedRecord::new(
                id("external-proof"),
                1,
                json!({"source":"trusted-fixture-service","proof":"verified-write"}),
            ),
            forged: ProtectedRecord::new(
                id("forged-proof"),
                1,
                json!({"source":"caller","proof":"verified-write"}),
            ),
            entered: Notify::new(),
            release: Semaphore::new(0),
        });
        let verifier = Arc::new(Verifier {
            target: tools[1].clone(),
            calls: AtomicUsize::new(0),
            mode: AtomicUsize::new(0),
            seen: Mutex::new(vec![]),
        });
        Self {
            base,
            profile,
            order,
            model: Arc::new(Model {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(vec![]),
                hold_final: AtomicBool::new(false),
                final_entered: Notify::new(),
                final_release: Semaphore::new(0),
            }),
            policy: Arc::new(Policy {
                mode,
                deny_resume: AtomicBool::new(false),
                deny_execute: AtomicBool::new(false),
                deny_cancel: AtomicBool::new(false),
                deny_receipt_read: AtomicBool::new(false),
                deny_details: AtomicBool::new(false),
                details_checks: AtomicUsize::new(0),
                require_resume_approval: AtomicBool::new(false),
                target_checks: AtomicUsize::new(0),
                resume_checks: Mutex::new(vec![]),
                tool_checks: Mutex::new(vec![]),
                pause_next_resume: AtomicBool::new(false),
                resume_entered: Notify::new(),
                resume_release: Semaphore::new(0),
            }),
            resolver: Arc::new(Resolver {
                calls: AtomicUsize::new(0),
                value: Mutex::new(ResolvedSystemInput {
                    value: json!(RECORD),
                    revision: id("record-revision-1"),
                }),
            }),
            tools,
            registry: Arc::new(ToolRegistry::new(scope(), registrations).unwrap()),
            inputs,
            store,
            verifier,
        }
    }
    pub fn bindings(&self) -> AgentBindings {
        let mut bindings = self.base.bindings();
        let mut router = agent_support::Router::new();
        let mut catalog = router.snapshot.catalog().clone();
        catalog.models[0]
            .capabilities
            .features
            .insert(id("tool_calling"));
        catalog.bindings[0]
            .capabilities
            .features
            .insert(id("tool_calling"));
        catalog.bindings[0].evidence[0].binding_digest = catalog.bindings[0]
            .contract_digest(&catalog.models[0])
            .unwrap();
        router.snapshot = RoutingSnapshot::new(catalog, router.snapshot.policy().clone()).unwrap();
        let policy =
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(5)).unwrap());
        bindings.router = Arc::new(router);
        bindings.profile_resolver = Arc::new(Catalog);
        bindings.policy = policy.clone();
        bindings.model_exchange = Arc::new(
            ModelExchange::new(self.model.clone(), policy)
                .with_route_inspector(self.base.inspector.clone(), Duration::from_secs(1))
                .unwrap(),
        );
        bindings.tools = Some(self.registry.clone());
        bindings.system_inputs = self.inputs.clone();
        bindings.system_input_resolver = Some(self.resolver.clone());
        bindings.state = self.store.clone();
        bindings.external_receipt_verifier = Some(self.verifier.clone());
        bindings
    }
    pub fn agent(&self) -> Agent {
        create_agent(self.profile.clone(), self.bindings()).unwrap()
    }
    pub async fn started(&self, agent: &Agent) -> RunHandle {
        let mut execution = context();
        execution.data.system_inputs =
            Some(SystemInputs::new(object(json!({"workspace_id":WORKSPACE}))));
        completed(agent.start(request("request"), execution).await.unwrap())
    }
    pub async fn outcome(&self, handle: &RunHandle) -> RunOutcome {
        completed(
            tokio::time::timeout(Duration::from_secs(5), handle.outcome(&context()))
                .await
                .expect("segment outcome must resolve")
                .unwrap(),
        )
    }
    pub async fn saved(&self, handle: &RunHandle) -> StoredRun {
        self.base
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
    }
    pub async fn approve(&self, handle: &RunHandle, command_id: &str) -> ResumeCommand {
        let saved = self.saved(handle).await;
        let wait = saved.snapshot.wait.unwrap();
        let WaitTarget::Approval { target } = wait.target else {
            panic!("expected approval wait")
        };
        ResumeCommand {
            run_id: handle.run_id().clone(),
            expected_revision: saved.snapshot.revision,
            command_id: id(command_id),
            action: ResumeAction::Approve {
                wait_id: wait.wait_id,
                target,
            },
        }
    }
}

pub async fn gate(entered: &Notify) {
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("observable operation must enter");
}

pub struct Verifier {
    pub target: Arc<Tool>,
    pub calls: AtomicUsize,
    pub mode: AtomicUsize,
    pub seen: Mutex<Vec<ExternalReceiptRequest>>,
}
impl ExternalReceiptVerifier for Verifier {
    fn verify<'a>(
        &'a self,
        request: &'a ExternalReceiptRequest,
        context: &'a ExternalReceiptContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.seen.lock().unwrap().push(request.clone());
            let execution = self.target.seen.lock().unwrap()[0].context.clone();
            assert_eq!(request.call.call_id, execution.call_id);
            assert_eq!(request.attempt_id, execution.attempt_id);
            assert_eq!(request.idempotency_key, execution.idempotency_key);
            assert_eq!(context.scope, execution.scope);
            assert_eq!(
                request.bound_input.execution_args()["record_id"],
                json!(RECORD)
            );
            if request.receipt
                != json!({"source":"trusted-fixture-service","proof":"verified-write"})
            {
                return Err(ContractError::new(
                    ErrorCode::AccessDenied,
                    "receipt.signature",
                ));
            }
            if self.mode.load(Ordering::SeqCst) == 1 {
                return Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Failed {
                        code: id("still_unknown"),
                    },
                    effect: ToolEffect::Unknown,
                    receipt: None,
                });
            }
            if self.mode.load(Ordering::SeqCst) == 3 {
                assert_eq!(self.target.applied.load(Ordering::SeqCst), 0);
                return Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Failed {
                        code: id("not_applied"),
                    },
                    effect: ToolEffect::NotApplied,
                    receipt: Some(request.receipt.clone()),
                });
            }
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: if self.mode.load(Ordering::SeqCst) == 2 {
                        json!(42)
                    } else {
                        json!("verified target")
                    },
                },
                effect: ToolEffect::Applied,
                receipt: Some(request.receipt.clone()),
            })
        })
    }
}

/// Fail only resume-command consumption, independently of result or heartbeat writes.
pub struct CommandStore {
    pub inner: Arc<MemoryStateStore>,
    pub mode: AtomicUsize,
    pub consumed_commits: AtomicUsize,
    pub wait_timeout_ms: AtomicUsize,
    pub pause_record: Mutex<Option<RecordRef>>,
    pub record_entered: Notify,
    pub record_release: Semaphore,
    pub proof: ProtectedRecord,
    pub forged: ProtectedRecord,
    pub entered: Notify,
    pub release: Semaphore,
}
impl StateStore for CommandStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        s: &'a Scope,
        session: &'a Id,
        r: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        self.inner.find_request(s, session, r)
    }
    fn admit<'a>(
        &'a self,
        s: &'a Scope,
        mut input: AdmissionInput,
    ) -> PortFuture<'a, AdmissionResult> {
        input
            .records
            .extend([self.proof.clone(), self.forged.clone()]);
        self.inner.admit(s, input)
    }
    fn load<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, StoredRun> {
        self.inner.load(s, r)
    }
    fn load_session<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, SessionSnapshot> {
        self.inner.load_session(s, r)
    }
    fn check_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.check_lease(s, r, l, n)
    }
    fn acquire_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        o: &'a Id,
        n: i64,
        t: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.acquire_lease(s, r, o, n, t)
    }
    fn renew_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
        t: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.renew_lease(s, r, l, n, t)
    }
    fn release_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, ()> {
        self.inner.release_lease(s, r, l, n)
    }
    fn read_events<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        after: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage> {
        self.inner.read_events(s, r, after, limit)
    }
    fn read_record<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        Box::pin(async move {
            let pause = {
                let mut reference = self.pause_record.lock().unwrap();
                if reference.as_ref() == Some(r) {
                    reference.take();
                    true
                } else {
                    false
                }
            };
            if pause {
                self.record_entered.notify_one();
                self.record_release.acquire().await.unwrap().forget();
            }
            self.inner.read_record(s, r).await
        })
    }
    fn commit<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        mut input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            let timeout = self.wait_timeout_ms.load(Ordering::SeqCst);
            if timeout > 0
                && input
                    .events
                    .iter()
                    .any(|event| matches!(event.payload, RunEventPayload::RunWaiting { .. }))
            {
                // A Host can constrain an individual wait without shortening the Run budget.
                // Rewrite the newly created wait and its paired protected outcome atomically.
                let old_wait = serde_json::to_value(input.snapshot.wait.as_ref().unwrap()).unwrap();
                let old_outcome =
                    serde_json::to_value(input.snapshot.outcome.as_ref().unwrap()).unwrap();
                let wait = input.snapshot.wait.as_mut().unwrap();
                wait.expires_at_ms =
                    Some(input.snapshot.timing.last_observed_at_ms + timeout as i64);
                if let OutcomeResult::Waiting { wait: saved_wait } =
                    &mut input.snapshot.outcome.as_mut().unwrap().result
                {
                    *saved_wait = wait.clone();
                }
                let new_wait = serde_json::to_value(wait).unwrap();
                let new_outcome =
                    serde_json::to_value(input.snapshot.outcome.as_ref().unwrap()).unwrap();
                let mut wait_ref = None;
                for record in &mut input.records {
                    let value = if record.value() == &old_wait {
                        Some(new_wait.clone())
                    } else if record.value() == &old_outcome {
                        Some(new_outcome.clone())
                    } else {
                        None
                    };
                    if let Some(value) = value {
                        let is_wait = record.value() == &old_wait;
                        *record = ProtectedRecord::new(
                            record.reference().record_id.clone(),
                            record.reference().revision,
                            value,
                        );
                        if is_wait {
                            wait_ref = Some(record.reference().clone());
                        }
                    }
                }
                for event in &mut input.events {
                    if let RunEventPayload::RunWaiting {
                        wait_ref: reference,
                    } = &mut event.payload
                    {
                        *reference = wait_ref.clone().expect("paired wait record must exist");
                    }
                }
            }
            if !input
                .events
                .iter()
                .any(|event| matches!(event.payload, RunEventPayload::RunResumed { .. }))
            {
                return self.inner.commit(s, r, input).await;
            }
            self.consumed_commits.fetch_add(1, Ordering::SeqCst);
            match self.mode.swap(0, Ordering::SeqCst) {
                1 => Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "resume.commit",
                )),
                2 => {
                    self.inner.commit(s, r, input).await?;
                    Err(ContractError::new(
                        ErrorCode::PersistenceUnavailable,
                        "resume.ack",
                    ))
                }
                3 => {
                    self.entered.notify_one();
                    self.release.acquire().await.unwrap().forget();
                    self.inner.commit(s, r, input).await
                }
                _ => self.inner.commit(s, r, input).await,
            }
        })
    }
}
```

## `crates/wickle/tests/support/mod.rs`

```rust
//! Shared realistic run fixtures for storage and execution boundary tests.

use serde_json::json;
use std::collections::BTreeSet;
use wickle::*;

pub fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
pub fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    }
}

pub struct Catalog {
    pub revision: &'static str,
}
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id(self.revision)),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!(self.revision)),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| reference.id.clone()),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

pub fn event(
    run: &Id,
    session: &Id,
    owner: &Scope,
    seq: u64,
    payload: RunEventPayload,
) -> RunEvent {
    RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id(&format!("event-{run}-{seq}")),
        scope: owner.clone(),
        run_id: run.clone(),
        session_id: session.clone(),
        seq: seq.try_into().unwrap(),
        timestamp_ms: 1000 + seq as i64,
        payload,
    }
}

pub async fn admission(
    run: &str,
    request_id: &str,
    session: &str,
    text: &str,
    revision: &'static str,
) -> AdmissionInput {
    let owner = scope();
    let profile=AgentProfile::from_json(&json!({
        "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
        "name":"Assistant","description":"State contract example","instructions":{"text":"Use evidence"},
        "model_binding":"primary","tools":[],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":8,"max_tool_attempts":4,"max_repair_attempts":1,"max_recovery_attempts":1,"max_elapsed_ms":10000}
    }).to_string()).unwrap();
    let profile = ProfileValidator::new(&Catalog { revision })
        .validate(&profile, &owner)
        .await
        .unwrap();
    let request = RunRequest {
        request_id: id(request_id),
        session_id: id(session),
        input: vec![InputContent::Text { text: text.into() }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    };
    let request_record = ProtectedRecord::new(
        id(&format!("request-{run}")),
        1,
        serde_json::to_value(&request).unwrap(),
    );
    let prompt_record = ProtectedRecord::new(
        id(&format!("prompt-{session}")),
        1,
        json!({"instructions":"Use evidence"}),
    );
    let snapshot = RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id(run),
        request_digest: admission_digest(&request, &profile, None),
        request: request.clone(),
        scope: owner.clone(),
        timing: RunTiming::new(0, profile.profile().limits.max_elapsed_ms.get()).unwrap(),
        reservations: vec![],
        resume_receipts: vec![],
        limits: profile.profile().limits.clone(),
        profile,
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        usage: BudgetUsage::default(),
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: None,
        wait: None,
        outcome: None,
        assembly_ref: None,
        routing_snapshot_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    let started = event(
        &snapshot.run_id,
        &request.session_id,
        &owner,
        1,
        RunEventPayload::RunStarted {
            request_ref: request_record.reference().clone(),
            profile_digest: snapshot.profile.profile_digest().clone(),
        },
    );
    let message = Message {
        message_id: id(&format!("message-{run}")),
        run_id: snapshot.run_id.clone(),
        sequence: 1.try_into().unwrap(),
        role: MessageRole::User,
        content: vec![ContentBlock::Content {
            content: InputContent::Text { text: text.into() },
        }],
        origin: MessageOrigin::User,
        visibility: Visibility::UserAndModel,
    };
    AdmissionInput {
        snapshot,
        prompt_snapshot: prompt_record.reference().clone(),
        messages: vec![message],
        events: vec![started],
        records: vec![request_record, prompt_record],
        require_durable: false,
    }
}

pub fn prepared(snapshot: &RunSnapshot, lease: RunLease, now: i64) -> CommitInput {
    let mut next = snapshot.clone();
    next.revision += 1;
    next.phase = RunPhase::Prepare;
    CommitInput {
        expected_revision: snapshot.revision,
        lease,
        now_ms: now,
        snapshot: next,
        messages: vec![],
        events: vec![],
        records: vec![],
    }
}

pub fn finished(snapshot: &RunSnapshot, lease: RunLease, now: i64) -> CommitInput {
    let mut next = snapshot.clone();
    next.revision += 1;
    next.last_event_seq += 1;
    next.status = RunStatus::Succeeded;
    next.phase = RunPhase::Finish;
    let outcome = RunOutcome {
        result: OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded,
        },
        output: vec![InputContent::Text {
            text: "Completed".into(),
        }],
        artifacts: vec![],
        usage: next.usage.clone(),
        checkpoint_revision: next.revision,
        verification: None,
        unresolved_effects: vec![],
    };
    let record = ProtectedRecord::new(
        id(&format!("outcome-{}", next.run_id)),
        1,
        serde_json::to_value(&outcome).unwrap(),
    );
    next.outcome = Some(outcome);
    let finished = event(
        &next.run_id,
        &next.request.session_id,
        &next.scope,
        next.last_event_seq,
        RunEventPayload::RunFinished {
            outcome_ref: record.reference().clone(),
        },
    );
    CommitInput {
        expected_revision: snapshot.revision,
        lease,
        now_ms: now,
        snapshot: next,
        messages: vec![],
        events: vec![finished],
        records: vec![record],
    }
}
```

## `tests/support/agent_consumer.rs`

```rust
// Synthetic adapters and metadata inspector: no provider network calls are made.
// The fixed clock supports deterministic accounting; this does not test timeouts.
use futures_util::{TryStreamExt, stream};
use serde_json::json;
use std::collections::BTreeSet;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;
use wickle_model_router::{ModelDispatcherEntry, PolicyModelRouter, RegistryModelDispatcher};
use wickle_state_sqlite::SqliteStateStore;

fn id(s: &str) -> Id {
    Id::new(s).expect("example identifiers")
}
struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        r: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id("revision-1")),
                    ..r.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("registered model")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: None,
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

fn reference(name: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id("1"),
    }
}
fn routing_snapshot(scope: &Scope) -> Result<RoutingSnapshot, ContractError> {
    let mut models = vec![];
    let mut bindings = vec![];
    for name in ["first", "second"] {
        let capabilities = ModelCapabilities {
            revision: id("capabilities"),
            features: BTreeSet::from([id("text")]),
            options_schema: json!({"type":"object","properties":{"reasoning_effort":{"enum":["high"]}},"additionalProperties":false}),
            context_window: 4096.try_into().unwrap(),
            max_output_tokens: 512.try_into().unwrap(),
        };
        let model = ModelDefinition {
            model_key: id(name),
            family: id("example"),
            provider: id(name),
            model_id: id("example-model"),
            model_version: id("release-1"),
            version_semantics: VersionSemantics::Pinned,
            lifecycle: ModelLifecycle::Active,
            capabilities: capabilities.clone(),
            evidence: vec![],
        };
        let mut binding = ModelBinding {
            binding: reference(name),
            model: model.reference(),
            requested_model: model.model_id.clone(),
            adapter: reference("adapter"),
            connection_ref: reference(&format!("{name}-account")),
            target: JsonObject::new(),
            target_schema: json!({"type":"object","additionalProperties":false}),
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("v1"),
            },
            deployment_revision: None,
            version_semantics: VersionSemantics::Pinned,
            capabilities,
            support: ModelSupportStatus::ContractTested,
            evidence: vec![],
        };
        binding.evidence.push(ModelValidationEvidence {
            kind: ModelValidationKind::ContractTest,
            binding_digest: binding.contract_digest(&model)?,
            checked_at_ms: 1000,
            evidence_ref: id("synthetic-fixture"),
            passed: true,
        });
        models.push(model);
        bindings.push(binding);
    }
    RoutingSnapshot::new(
        ModelCatalogSnapshot {
            revision: id("catalog"),
            scope: scope.clone(),
            models,
            bindings,
            aliases: vec![],
        },
        RoutingPolicy {
            revision: id("policy"),
            scope: scope.clone(),
            rules: vec![RoutingRule {
                model_binding: id("primary"),
                purpose: ModelPurpose::Agent,
                primary: reference("first"),
                fallbacks: vec![reference("second")],
                fallback_on: vec![ModelFailureKind::RateLimited],
                version_policy: VersionPolicy::RequirePinned,
                min_support: ModelSupportStatus::ContractTested,
            }],
        },
    )
}
struct TemporaryDatabase(std::path::PathBuf);
impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct ExampleClock;
impl Clock for ExampleClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: 1000,
            monotonic_ms: 1000,
        })
    }
    fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}
struct ExamplePolicy;
impl PolicyPort for ExamplePolicy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::InvokeModel { route, .. } = &request.action {
                if route.connection_ref.id == id(&format!("{}-account", route.provider)) {
                    return Ok(PolicyDecision::Allow {});
                }
            }
            Ok(
                if matches!(request.action, PolicyAction::InvokeModel { .. }) {
                    PolicyDecision::Deny {
                        reason: id("unknown-account"),
                    }
                } else {
                    PolicyDecision::Allow {}
                },
            )
        })
    }
}
struct ExampleInspector;
// This echo is a fixture only. A real inspector must read authoritative provider
// metadata instead of presenting requested values as independently observed facts.
impl ModelRouteInspector for ExampleInspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        _: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: ModelRouteAvailability::Available,
                model_id: Some(route.model_id.clone()),
                model_version: Some(route.model_version.clone()),
                deployment_revision: None,
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id("synthetic-metadata-check"),
            })
        })
    }
}
struct ExampleModel {
    route: ResolvedModelRoute,
    calls: AtomicUsize,
    fail: bool,
}
impl ModelPort for ExampleModel {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: self.route.provider.clone(),
            adapter: self.route.adapter.clone(),
            connection_ref: self.route.connection_ref.clone(),
        }
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        assert_eq!(
            request.options.get("reasoning_effort"),
            Some(&json!("high"))
        );
        assert_eq!(request.route, self.route);
        self.calls.fetch_add(1, Ordering::SeqCst);
        let events = if self.fail {
            vec![Ok(ModelEvent::ResponseError {
                kind: ModelFailureKind::RateLimited,
                metadata: ModelResponseMetadata::default(),
            })]
        } else {
            vec![
                Ok(ModelEvent::TextDelta {
                    text: "second provider result".into(),
                }),
                Ok(ModelEvent::ResponseCompleted {
                    finish: ModelFinish::Stop,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![],
                }),
            ]
        };
        Box::pin(stream::iter(events))
    }
}
struct Estimate;
impl ModelTokenEstimator for Estimate {
    fn estimate(&self, request: &ModelRequest) -> Result<u64, ContractError> {
        // Conservative test estimate; it is not provider-measured token usage.
        serde_json::to_vec(request)
            .map(|bytes| bytes.len() as u64)
            .map_err(|_| ContractError::new(ErrorCode::InvalidContext, "example.estimate"))
    }
}
fn completed<T>(value: Guarded<T>) -> Result<T, Box<dyn std::error::Error>> {
    match value {
        Guarded::Completed(value) => Ok(value),
        Guarded::ApprovalRequired(_) => Err("unexpected approval".into()),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let temporary = TemporaryDatabase(
        std::env::temp_dir().join(format!("wickle-agent-{}", RandomIdSource.next_id()?)),
    );
    std::fs::create_dir(&temporary.0)?;
    let database = temporary.0.join("state.sqlite3");
    let store = Arc::new(SqliteStateStore::open(&database)?);
    let snapshot = routing_snapshot(&scope)?;
    let first = Arc::new(ExampleModel {
        route: snapshot.route_for_binding(&reference("first"))?,
        calls: AtomicUsize::new(0),
        fail: true,
    });
    let second = Arc::new(ExampleModel {
        route: snapshot.route_for_binding(&reference("second"))?,
        calls: AtomicUsize::new(0),
        fail: false,
    });
    let policy = Arc::new(PolicyGate::new(
        Arc::new(ExamplePolicy),
        Duration::from_secs(1),
    )?);
    let exchange = Arc::new(
        ModelExchange::with_dispatcher(
            Arc::new(RegistryModelDispatcher::new(vec![
                ModelDispatcherEntry {
                    scope: scope.clone(),
                    port: first.clone(),
                },
                ModelDispatcherEntry {
                    scope: scope.clone(),
                    port: second.clone(),
                },
            ])?),
            policy.clone(),
        )
        .with_route_inspector(Arc::new(ExampleInspector), Duration::from_secs(1))?,
    );
    let profile = AgentProfile::from_json(
        r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
        "name":"Assistant","description":"Agent consumer","instructions":{"text":"Use supplied information"},
        "model_binding":"primary","tools":[],"skills":[],"connectors":[],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":4,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":2,"max_elapsed_ms":10000}
    }"#,
    )?;
    let agent = create_agent(
        profile,
        AgentBindings {
            scope: scope.clone(),
            state: store.clone(),
            policy,
            profile_resolver: Arc::new(Catalog),
            model_exchange: exchange,
            router: Arc::new(PolicyModelRouter::new(snapshot)?),
            host_instructions: vec!["Preserve the requested output.".into()],
            system_inputs: SystemInputRegistry::new(vec![])?,
            clock: Arc::new(ExampleClock),
            ids: Arc::new(RandomIdSource),
            tools: None,
            system_input_resolver: None, external_receipt_verifier: None,
            token_estimator: Arc::new(Estimate),
            settings: AgentSettings {
                max_output_tokens: 128.try_into()?,
                require_durable: true,
                ..AgentSettings::default()
            },
        },
    )?;
    assert_eq!(first.calls.load(Ordering::SeqCst), 0);
    assert_eq!(second.calls.load(Ordering::SeqCst), 0);
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id("actor"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    );
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Retrieve the available result".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::from([("reasoning_effort".into(), json!("high"))]),
        output_contract: None,
    };
    let handle = completed(agent.start(request.clone(), context.clone()).await?)?;
    let run_id = handle.run_id().clone();
    let mut events = handle.events(0, context.clone());
    use futures_util::StreamExt;
    let started = events.next().await.ok_or("missing admission event")??;
    assert_eq!(started.event_type, "run.started");
    drop(events);
    let outcome = completed(handle.outcome(&context).await?)?;
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "second provider result".into()
        }]
    );
    assert_eq!(outcome.usage.model_calls, 2);
    assert_eq!(outcome.usage.recovery_attempts, 1);
    let replay = completed(agent.start(request, context.clone()).await?)?;
    assert_eq!(replay.run_id(), &run_id);
    assert_eq!(completed(replay.outcome(&context).await?)?, outcome);
    assert_eq!(first.calls.load(Ordering::SeqCst), 1);
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    let view = completed(agent.get_run(&run_id, &context).await?)?;
    assert_eq!(view.status, RunStatus::Succeeded);
    let events: Vec<_> = handle
        .events(started.seq.get(), context.clone())
        .try_collect()
        .await?;
    assert_eq!(
        events.last().ok_or("missing finish event")?.event_type,
        "run.finished"
    );
    drop(store);
    let restored = SqliteStateStore::open(&database)?
        .load(&scope, &run_id)
        .await?;
    assert_eq!(restored.snapshot.outcome, Some(outcome));
    assert!(restored.session.active_run_id.is_none());
    println!(
        "agent consumer: pure construction, detached execution after observer drop, fallback under shared budgets, stored outcome and event replay, duplicate request without new model calls, SQLite reopen"
    );
    Ok(())
}
```

## `tests/support/budget_consumer.rs`

```rust
use serde_json::json;
use std::{cell::Cell, collections::BTreeSet, sync::Arc};
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}

struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id("1")),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("example model binding")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: None,
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

fn context(scope: &Scope) -> ExecutionContext {
    ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id("caller"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    )
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let profile = AgentProfile::from_json(
        r#"{
          "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
          "name":"Assistant","description":"Budget example","instructions":{"text":"Use evidence"},
          "model_binding":"primary","tools":[],"skills":[],"connectors":[],
          "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
          "limits":{"max_model_calls":1,"max_tool_attempts":3,"max_repair_attempts":0,
                    "max_recovery_attempts":1,"max_elapsed_ms":30000}
        }"#,
    )?;
    let profile = ProfileValidator::new(&Catalog)
        .validate(&profile, &scope)
        .await?;
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Count available records".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    };
    let request_record =
        ProtectedRecord::new(id("request-record"), 1, serde_json::to_value(&request)?);
    let prompt = ProtectedRecord::new(id("prompt"), 1, json!({"text":"Use evidence"}));
    let clock = Arc::new(SystemClock::new());
    let started_at = clock.now()?.utc_ms;
    let run_id = id("run");
    let snapshot = RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: run_id.clone(),
        request_digest: admission_digest(&request, &profile, None),
        request: request.clone(),
        scope: scope.clone(),
        limits: profile.profile().limits.clone(),
        profile,
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        usage: BudgetUsage::default(),
        timing: RunTiming::new(started_at, 30000)?,
        reservations: vec![],
        resume_receipts: vec![],
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: None,
        wait: None,
        outcome: None,
        assembly_ref: None,
        routing_snapshot_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    let started = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("started"),
        scope: scope.clone(),
        run_id: run_id.clone(),
        session_id: request.session_id.clone(),
        seq: 1.try_into()?,
        timestamp_ms: started_at,
        payload: RunEventPayload::RunStarted {
            request_ref: request_record.reference().clone(),
            profile_digest: snapshot.profile.profile_digest().clone(),
        },
    };
    let store = Arc::new(MemoryStateStore::new());
    store
        .admit(
            &scope,
            AdmissionInput {
                snapshot,
                prompt_snapshot: prompt.reference().clone(),
                messages: vec![Message {
                    message_id: id("user-message"),
                    run_id: run_id.clone(),
                    sequence: 1.try_into()?,
                    role: MessageRole::User,
                    content: vec![ContentBlock::Content {
                        content: request.input[0].clone(),
                    }],
                    origin: MessageOrigin::User,
                    visibility: Visibility::UserAndModel,
                }],
                events: vec![started],
                records: vec![request_record, prompt],
                require_durable: false,
            },
        )
        .await?;
    let lease = store
        .acquire_lease(&scope, &run_id, &id("worker"), clock.now()?.utc_ms, 30000)
        .await?;
    let execution = context(&scope);
    let budget = RunBudget::attach(
        store.clone(),
        clock.clone(),
        Arc::new(RandomIdSource),
        scope.clone(),
        run_id.clone(),
        lease.clone(),
        execution.cancellation.clone(),
    )
    .await?;
    let dispatches = Cell::new(0);
    let model_attempt = budget
        .execute(
            ReservationKind::Model {
                purpose: ModelPurpose::Agent,
            },
            |reservation| {
                dispatches.set(dispatches.get() + 1);
                async move { Ok(reservation.attempt_id) }
            },
        )
        .await?;
    let refused = budget
        .execute(
            ReservationKind::Model {
                purpose: ModelPurpose::Compaction,
            },
            |_| {
                dispatches.set(dispatches.get() + 1);
                async { Ok(()) }
            },
        )
        .await;
    assert_eq!(refused.unwrap_err().code, ErrorCode::BudgetExceeded);
    assert_eq!(dispatches.get(), 1);

    // This example exercises reservation boundaries; a full driver owns policy,
    // tool schemas, actual provider/tool dispatch, and result settlement.
    let count = budget
        .execute(
            ReservationKind::Tool {
                call_id: id("count-records"),
            },
            |_| {
                dispatches.set(dispatches.get() + 1);
                async { Ok(["first", "second"].len()) }
            },
        )
        .await?;
    assert_eq!(count, 2);
    assert_eq!(dispatches.get(), 2);

    // Save a reservation without reporting an execution result, then detach.
    let unsettled = budget
        .reserve(ReservationKind::Tool {
            call_id: id("inspect-record"),
        })
        .await?;
    execution.cancellation.cancel();
    let cancelled = budget
        .execute(
            ReservationKind::Tool {
                call_id: id("cancelled-call"),
            },
            |_| {
                dispatches.set(dispatches.get() + 1);
                async { Ok(()) }
            },
        )
        .await;
    assert_eq!(cancelled.unwrap_err().code, ErrorCode::Cancelled);
    assert_eq!(dispatches.get(), 2);
    drop(budget);

    let restored = store.load(&scope, &run_id).await?.snapshot;
    let restored = RunSnapshot::from_json(&serde_json::to_string(&restored)?)?;
    assert_eq!(restored.usage.model_calls, 1);
    assert_eq!(restored.usage.tool_attempts, 2);
    assert_eq!(restored.reservations.len(), 3);
    assert!(restored.reservations.contains(&unsettled));
    assert!(
        restored
            .reservations
            .iter()
            .any(|saved| saved.attempt_id == model_attempt)
    );
    let resumed = RunBudget::attach(
        store.clone(),
        clock,
        Arc::new(RandomIdSource),
        scope.clone(),
        run_id.clone(),
        lease,
        context(&scope).cancellation,
    )
    .await?;
    let before = store.load(&scope, &run_id).await?.snapshot.reservations;
    assert_eq!(
        resumed
            .reserve(ReservationKind::Model {
                purpose: ModelPurpose::Verification,
            })
            .await
            .unwrap_err()
            .code,
        ErrorCode::BudgetExceeded
    );
    assert_eq!(
        store.load(&scope, &run_id).await?.snapshot.reservations,
        before
    );
    println!(
        "budget consumer: model limit preserved; tool budget independent; cancellation dispatched 0 additional calls; unsettled reservation retained after reattach"
    );
    Ok(())
}
```

## `tests/support/context_consumer.rs`

```rust
use serde_json::json;
use std::collections::BTreeSet;
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: reference.version.clone().or_else(|| Some(id("1"))),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!(reference.id)),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| id("search")),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}
fn compiled_tool() -> Result<CompiledTool, ContractError> {
    let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string"}),
        source: SystemInputSource::Run {},
    }])?;
    SchemaCompiler::new().compile(ToolDescriptor {
        tool: reference("search"), name: id("search"), description: "Search available evidence".into(),
        input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"workspace_id":{"type":"string","description":"Internal workspace key"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters: vec!["query".into()], system_bindings: None, output_schema: json!({"type":"array"}), side_effect: ToolSideEffect::ReadOnly, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false, max_output_bytes: 4096.try_into().unwrap(),
    }, &registry)
}
fn route() -> ResolvedModelRoute {
    ResolvedModelRoute {
        binding: reference("primary"),
        catalog_revision: id("catalog"),
        routing_policy_revision: id("policy"),
        requested_model: id("example"),
        model_id: id("example"),
        model_version: id("1"),
        version_semantics: VersionSemantics::Pinned,
        provider: id("example"),
        target: JsonObject::new(),
        deployment_revision: None,
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("1"),
        },
        adapter: reference("example"),
        capability_revision: id("capabilities"),
        connection_ref: reference("connection"),
    }
}
fn request(run: &str, text: &str) -> RunRequest {
    RunRequest {
        request_id: id(&format!("request-{run}")),
        session_id: id("session"),
        input: vec![InputContent::Text { text: text.into() }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::from([("reasoning_effort".into(), json!("high"))]),
        output_contract: None,
    }
}
fn admission(
    profile: &ResolvedProfile,
    prompt: &ProtectedRecord,
    run: &str,
    first_sequence: u64,
    started_at_ms: i64,
    text: &str,
) -> AdmissionInput {
    let request = request(run, text);
    let request_record = ProtectedRecord::new(
        id(&format!("input-{run}")),
        1,
        serde_json::to_value(&request).unwrap(),
    );
    let scope = profile.scope().clone();
    let snapshot = RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id(run),
        request_digest: admission_digest(&request, profile, None),
        request: request.clone(),
        scope: scope.clone(),
        profile: profile.clone(),
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        limits: profile.profile().limits.clone(),
        usage: BudgetUsage::default(),
        timing: RunTiming::new(started_at_ms, 10000).unwrap(),
        reservations: vec![],
        resume_receipts: vec![],
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: None,
        wait: None,
        outcome: None,
        assembly_ref: None,
        routing_snapshot_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    AdmissionInput {
        snapshot,
        prompt_snapshot: prompt.reference().clone(),
        require_durable: false,
        messages: vec![Message {
            message_id: id(&format!("user-{run}")),
            run_id: id(run),
            sequence: first_sequence.try_into().unwrap(),
            role: MessageRole::User,
            content: request
                .input
                .into_iter()
                .map(|content| ContentBlock::Content { content })
                .collect(),
            origin: MessageOrigin::User,
            visibility: Visibility::UserAndModel,
        }],
        events: vec![RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: id(&format!("start-{run}")),
            scope,
            run_id: id(run),
            session_id: id("session"),
            seq: 1.try_into().unwrap(),
            timestamp_ms: started_at_ms,
            payload: RunEventPayload::RunStarted {
                request_ref: request_record.reference().clone(),
                profile_digest: profile.profile_digest().clone(),
            },
        }],
        records: vec![request_record, prompt.clone()],
    }
}
fn project(
    prompt: &PromptSnapshot,
    stored: &StoredRun,
) -> Result<ContextProjection, ContractError> {
    let step = id(&format!("step-{}", stored.snapshot.run_id));
    ContextAssembler::new().project(
        prompt,
        ProjectionInput {
            profile: &stored.snapshot.profile,
            scope: &stored.snapshot.scope,
            run_id: &stored.snapshot.run_id,
            model_step_id: &step,
            current_request: &stored.snapshot.request,
            current_request_message_id: &id(&format!("user-{}", stored.snapshot.run_id)),
            transcript: &stored.messages,
            context_items: &[],
            opaque_records: &[],
            expected_prompt_digest: &stored.session.prompt_snapshot.digest,
            request_id: step.clone(),
            purpose: ModelPurpose::Agent,
            route: route(),
            output: ModelOutput::Text {},
            max_output_tokens: 128.try_into().unwrap(),
            options: stored.snapshot.request.model_options.clone(),
            response_limits: ModelResponseLimits {
                max_input_bytes: 32_768,
                max_response_bytes: 4096,
                max_delta_bytes: 1024,
                max_events: 16,
                max_tool_calls: 1,
            },
            limits: ProjectionLimits {
                max_bytes: 32_768,
                max_items: 30,
            },
        },
    )
}

fn projected_user_occurrences(projection: &ContextProjection, text: &str) -> usize {
    projection
        .request
        .messages
        .iter()
        .filter(|message| message.role == ModelRole::User)
        .flat_map(|message| &message.content)
        .filter(|content| matches!(content, ModelContent::Text { text: value } if value == text))
        .count()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let profile = AgentProfile::from_json(
        r#"{
      "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1",
      "name":"Assistant","description":"Context example","instructions":{"text":"Summarize available evidence"},
      "model_binding":"primary","tools":[{"tool_id":"search","version":"1"}],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
      "limits":{"max_model_calls":4,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
    }"#,
    )?;
    let profile = ProfileValidator::new(&Catalog)
        .validate(&profile, &scope)
        .await?;
    let tool = compiled_tool()?;
    let prompt = PromptSnapshot::create(
        &profile,
        vec!["Only report actions supported by supplied observations.".into()],
        None,
        vec![PromptToolBinding {
            selection: profile.profile().tools[0].clone(),
            compiled: tool.clone(),
        }],
        vec![],
    )?;
    let prompt_record = ProtectedRecord::new(id("prompt"), 1, serde_json::to_value(&prompt)?);
    assert_eq!(prompt.digest(), prompt_record.reference().digest);
    let store = MemoryStateStore::new();
    let mut first_input = admission(
        &profile,
        &prompt_record,
        "first",
        1,
        1000,
        "Review the available evidence",
    );
    first_input.messages.push(Message {
        message_id: id("private-state"),
        run_id: id("first"),
        sequence: 2.try_into()?,
        role: MessageRole::System,
        content: vec![ContentBlock::Content {
            content: InputContent::Json {
                value: json!({"workspace_id":"host-only-database-value"}),
            },
        }],
        origin: MessageOrigin::Host,
        visibility: Visibility::Internal,
    });
    let first = store.admit(&scope, first_input).await?.state;
    let first_projection = project(&prompt, &first)?;
    assert_eq!(first_projection.request.options, first.snapshot.request.model_options);
    assert_eq!(first_projection.request.messages[0].role, ModelRole::System);
    assert_eq!(
        projected_user_occurrences(&first_projection, "Review the available evidence"),
        1
    );
    assert!(
        first_projection.request.tools[0].model_input_schema["properties"]
            .get("workspace_id")
            .is_none()
    );
    assert!(
        !serde_json::to_string(&first_projection.request)?.contains("host-only-database-value")
    );
    assert_eq!(
        first_projection
            .selected_message_ids
            .iter()
            .filter(|message| **message == id("user-first"))
            .count(),
        1
    );

    // Finish this demonstration run without claiming that a model or tool executed.
    let lease = store
        .acquire_lease(&scope, &id("first"), &id("worker"), 1000, 1000)
        .await?;
    let mut snapshot = first.snapshot.clone();
    snapshot.revision = 1;
    snapshot.last_event_seq = 2;
    snapshot.status = RunStatus::Cancelled;
    snapshot.phase = RunPhase::Finish;
    snapshot.usage.elapsed_ms = 1;
    snapshot.timing.last_observed_at_ms = 1001;
    let outcome = RunOutcome {
        result: OutcomeResult::Cancelled {
            reason: "Demonstration complete".into(),
        },
        output: vec![],
        artifacts: vec![],
        usage: snapshot.usage.clone(),
        checkpoint_revision: 1,
        verification: None,
        unresolved_effects: vec![],
    };
    let outcome_record =
        ProtectedRecord::new(id("outcome-first"), 1, serde_json::to_value(&outcome)?);
    snapshot.outcome = Some(outcome);
    store
        .commit(
            &scope,
            &id("first"),
            CommitInput {
                expected_revision: 0,
                lease,
                now_ms: 1001,
                snapshot,
                messages: vec![],
                records: vec![outcome_record.clone()],
                events: vec![RunEvent {
                    schema_version: RunEventSchemaVersion::V1,
                    event_id: id("finish-first"),
                    scope: scope.clone(),
                    run_id: id("first"),
                    session_id: id("session"),
                    seq: 2.try_into()?,
                    timestamp_ms: 1001,
                    payload: RunEventPayload::RunFinished {
                        outcome_ref: outcome_record.reference().clone(),
                    },
                }],
            },
        )
        .await?;

    let changed = PromptSnapshot::create(
        &profile,
        vec!["Changed operating policy".into()],
        None,
        vec![PromptToolBinding {
            selection: profile.profile().tools[0].clone(),
            compiled: tool,
        }],
        vec![],
    )?;
    let changed_record =
        ProtectedRecord::new(id("changed-prompt"), 1, serde_json::to_value(&changed)?);
    assert!(
        store
            .admit(
                &scope,
                admission(&profile, &changed_record, "wrong", 3, 1002, "Continue")
            )
            .await
            .is_err()
    );
    let second = store
        .admit(
            &scope,
            admission(
                &profile,
                &prompt_record,
                "second",
                3,
                1002,
                "Now give a concise summary",
            ),
        )
        .await?
        .state;
    let saved_prompt = store
        .read_record(&scope, &second.session.prompt_snapshot)
        .await?;
    let restored = PromptSnapshot::restore(
        &serde_json::to_string(saved_prompt.value())?,
        &second.session.prompt_snapshot.digest,
        &second.snapshot.profile,
        &scope,
    )?;
    let second_projection = project(&restored, &second)?;
    assert_eq!(
        projected_user_occurrences(&second_projection, "Now give a concise summary"),
        1
    );
    assert_eq!(
        first_projection.prompt_digest,
        second_projection.prompt_digest
    );
    assert_eq!(
        first_projection.request.tools,
        second_projection.request.tools
    );
    assert_eq!(
        second_projection
            .selected_message_ids
            .iter()
            .filter(|message| **message == id("user-second"))
            .count(),
        1
    );
    assert!(
        !serde_json::to_string(&second_projection.request)?.contains("host-only-database-value")
    );
    println!(
        "context consumer: two stored runs share the pinned prompt/tool schema; changed prompt refused; current request appears once; internal execution data excluded"
    );
    Ok(())
}
```

## `tests/support/input_binding_consumer.rs`

```rust
use serde_json::json;
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;

const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";
const REPORT_A: &str = "22222222-2222-4222-8222-222222222222";
const REPORT_B: &str = "33333333-3333-4333-8333-333333333333";
fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}

struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        r: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: r.version.clone().or_else(|| Some(id("1"))),
                    ..r.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!(r.id)),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (r.kind == ComponentKind::Tool).then(|| r.id.clone()),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}
struct OwnedTargets;
impl PolicyPort for OwnedTargets {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::ExecuteTool { input } = &request.action {
                let args = input.execution_args();
                let owned = match input.tool.id.as_str() {
                    "search" => {
                        args.get("workspace_id").and_then(|v| v.as_str()) == Some(WORKSPACE)
                    }
                    "read_report" => matches!(
                        args.get("report_id").and_then(|v| v.as_str()),
                        Some(REPORT_A | REPORT_B)
                    ),
                    _ => false,
                };
                if !owned {
                    return Ok(PolicyDecision::Deny {
                        reason: id("target_not_owned"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}
struct CurrentReport {
    value: Mutex<ResolvedSystemInput>,
    calls: AtomicUsize,
}
impl SystemInputResolver for CurrentReport {
    fn resolve<'a>(
        &'a self,
        request: &'a SystemInputResolveRequest,
        _: &'a SystemInputResolveContext,
    ) -> PortFuture<'a, Option<ResolvedSystemInput>> {
        assert_eq!(request.key, id("current_report_id"));
        self.calls.fetch_add(1, Ordering::SeqCst);
        let value = self.value.lock().unwrap().clone();
        Box::pin(async move { Ok(Some(value)) })
    }
}
fn tool(
    name: &str,
    input_schema: serde_json::Value,
    agent_parameters: Vec<String>,
) -> ToolDescriptor {
    ToolDescriptor {
        tool: reference(name),
        name: id(name),
        description: "Read authorized data".into(),
        input_schema,
        agent_parameters,
        system_bindings: None,
        output_schema: json!({"type":"string"}),
        side_effect: ToolSideEffect::ReadOnly,
        concurrency: ToolConcurrency::Serial,
        retry: ToolRetryPolicy::Never,
        reconcile: false,
        max_output_bytes: 4096.try_into().unwrap(),
    }
}
async fn plan(
    store: &MemoryStateStore,
    scope: &Scope,
    run: &Id,
    lease: &RunLease,
    clock: &SystemClock,
    call_id: &str,
    compiled: &CompiledTool,
    model_inputs: JsonObject,
) -> Result<(), ContractError> {
    let saved = store.load(scope, run).await?;
    let mut snapshot = saved.snapshot;
    let expected_revision = snapshot.revision;
    snapshot.revision += 1;
    snapshot.phase = RunPhase::Tool;
    let call = ToolCall {
        call_id: id(call_id),
        model_request_id: id("model-request"),
        provider_call_id: id(&format!("provider-{call_id}")),
        tool_name: compiled.descriptor().name.clone(),
        model_inputs,
        descriptor_digest: Some(compiled.descriptor_digest().clone()),
        bound_input_ref: None,
    };
    snapshot.tool_ledger.push(ToolLedgerEntry {
        call,
        state: ToolCallState::Planned {},
    });
    store
        .commit(
            scope,
            run,
            CommitInput {
                expected_revision,
                lease: lease.clone(),
                now_ms: clock.now()?.utc_ms,
                snapshot,
                messages: vec![],
                events: vec![],
                records: vec![],
            },
        )
        .await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let registry = Arc::new(SystemInputRegistry::new(vec![
        SystemInputDefinition {
            key: id("workspace_id"),
            version: id("1"),
            value_schema: json!({"type":"string","format":"uuid"}),
            source: SystemInputSource::Run {},
        },
        SystemInputDefinition {
            key: id("unused_key"),
            version: id("1"),
            value_schema: json!({"type":"string"}),
            source: SystemInputSource::Run {},
        },
        SystemInputDefinition {
            key: id("current_report_id"),
            version: id("1"),
            value_schema: json!({"type":"string","format":"uuid"}),
            source: SystemInputSource::Resolver {
                resolver_ref: reference("current-report"),
            },
        },
    ])?);
    let search = SchemaCompiler::new().compile(tool("search", json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","default":10},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}), vec!["query".into(),"limit".into()]), &registry)?;
    let mut report = tool(
        "read_report",
        json!({"type":"object","properties":{"report_id":{"type":"string","format":"uuid"}},"required":["report_id"],"additionalProperties":false}),
        vec![],
    );
    report.system_bindings = Some(std::collections::BTreeMap::from([(
        "report_id".into(),
        id("current_report_id"),
    )]));
    let report = SchemaCompiler::new().compile(report, &registry)?;
    let values = SystemInputs::new(JsonObject::from([
        ("workspace_id".into(), json!(WORKSPACE)),
        ("unused_key".into(), json!("not a tool argument")),
    ]));
    let captured = RunSystemInputs::capture(scope.clone(), Some(values.clone()), &registry)?;
    let input_record = captured.to_record(id("run-inputs"), 1);
    let input_ref = captured.snapshot_ref(input_record.reference())?;
    let profile = AgentProfile::from_json(
        r#"{
      "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1",
      "name":"Assistant","description":"Binding example","instructions":{"text":"Use available evidence"},"model_binding":"primary",
      "tools":[{"tool_id":"search","version":"1"},{"tool_id":"read_report","version":"1"}],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
      "limits":{"max_model_calls":4,"max_tool_attempts":4,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}
    }"#,
    )?;
    let profile = ProfileValidator::new(&Catalog)
        .validate(&profile, &scope)
        .await?;
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Read recent results".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    };
    let request_record =
        ProtectedRecord::new(id("request-data"), 1, serde_json::to_value(&request)?);
    let prompt = ProtectedRecord::new(
        id("prompt"),
        1,
        json!({"instructions":"Use available evidence"}),
    );
    let clock = Arc::new(SystemClock::new());
    let now = clock.now()?.utc_ms;
    let run = id("run");
    let snapshot = RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: run.clone(),
        request_digest: admission_digest(&request, &profile, Some(&input_ref)),
        request: request.clone(),
        scope: scope.clone(),
        limits: profile.profile().limits.clone(),
        profile,
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        usage: BudgetUsage::default(),
        timing: RunTiming::new(now, 30000)?,
        reservations: vec![],
        resume_receipts: vec![],
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: Some(input_ref.clone()),
        wait: None,
        outcome: None,
        assembly_ref: None,
        routing_snapshot_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    let started = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("started"),
        scope: scope.clone(),
        run_id: run.clone(),
        session_id: request.session_id.clone(),
        seq: 1.try_into()?,
        timestamp_ms: now,
        payload: RunEventPayload::RunStarted {
            request_ref: request_record.reference().clone(),
            profile_digest: snapshot.profile.profile_digest().clone(),
        },
    };
    let store = Arc::new(MemoryStateStore::new());
    store
        .admit(
            &scope,
            AdmissionInput {
                snapshot,
                prompt_snapshot: prompt.reference().clone(),
                messages: vec![Message {
                    message_id: id("user-message"),
                    run_id: run.clone(),
                    sequence: 1.try_into()?,
                    role: MessageRole::User,
                    content: vec![ContentBlock::Content {
                        content: request.input[0].clone(),
                    }],
                    origin: MessageOrigin::User,
                    visibility: Visibility::UserAndModel,
                }],
                events: vec![started],
                records: vec![request_record, prompt, input_record.clone()],
                require_durable: false,
            },
        )
        .await?;
    let lease = store
        .acquire_lease(&scope, &run, &id("worker"), now, 30000)
        .await?;
    let mut context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id("caller"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: Some(values),
        },
        Default::default(),
    );
    let budget = RunBudget::attach(
        store.clone(),
        clock.clone(),
        Arc::new(RandomIdSource),
        scope.clone(),
        run.clone(),
        lease.clone(),
        context.cancellation.clone(),
    )
    .await?;
    let resolver = Arc::new(CurrentReport {
        value: Mutex::new(ResolvedSystemInput {
            value: json!(REPORT_A),
            revision: id("revision-1"),
        }),
        calls: AtomicUsize::new(0),
    });
    let binder = InputBinder::new(
        registry.clone(),
        Some(resolver.clone()),
        Arc::new(PolicyGate::new(
            Arc::new(OwnedTargets),
            Duration::from_secs(1),
        )?),
        Arc::new(RandomIdSource),
    );
    plan(
        &store,
        &scope,
        &run,
        &lease,
        &clock,
        "search-call",
        &search,
        JsonObject::from([("query".into(), json!("recent results"))]),
    )
    .await?;
    let search_result = binder
        .bind(&search, &id("search-call"), &context, &budget)
        .await?;
    assert_eq!(
        serde_json::to_value(search_result.input.execution_args())?,
        json!({"query":"recent results","limit":10,"workspace_id":WORKSPACE})
    );
    assert_eq!(
        serde_json::to_value(search_result.input.original_model_inputs())?,
        json!({"query":"recent results"})
    );
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
    context.data.system_inputs = None;
    plan(
        &store,
        &scope,
        &run,
        &lease,
        &clock,
        "report-first",
        &report,
        JsonObject::new(),
    )
    .await?;
    let first = binder
        .bind(&report, &id("report-first"), &context, &budget)
        .await?;
    *resolver.value.lock().unwrap() = ResolvedSystemInput {
        value: json!(REPORT_B),
        revision: id("revision-2"),
    };
    let cached = binder
        .bind(&report, &id("report-first"), &context, &budget)
        .await?;
    assert_eq!(cached.reference, first.reference);
    assert_eq!(cached.input.execution_args()["report_id"], json!(REPORT_A));
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    plan(
        &store,
        &scope,
        &run,
        &lease,
        &clock,
        "report-next",
        &report,
        JsonObject::new(),
    )
    .await?;
    let next = binder
        .bind(&report, &id("report-next"), &context, &budget)
        .await?;
    assert_eq!(next.input.execution_args()["report_id"], json!(REPORT_B));
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 2);
    let restored = RunSystemInputs::restore(&input_record, &input_ref, &scope, &registry)?;
    restored.validate_resume(None)?;
    assert!(
        restored
            .validate_resume(Some(&SystemInputs::default()))
            .is_err()
    );
    println!(
        "input binding consumer: model query + default limit + Host workspace; unused key omitted; cached target fixed; new call resolves the new report; omitted resume inputs reuse the snapshot"
    );
    Ok(())
}
```

## `tests/support/resume_consumer.rs`

```rust
// Synthetic model, tool, resolver, and metadata inspector; no provider network or
// business database calls. Real SQLite persists an approval wait across Host instances.
use futures_util::{TryStreamExt, stream};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;
use wickle_model_router::PolicyModelRouter;
use wickle_state_sqlite::SqliteStateStore;

const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";
fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .expect("object fixture")
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
fn completed<T>(value: Guarded<T>) -> Result<T, Box<dyn std::error::Error>> {
    match value {
        Guarded::Completed(value) => Ok(value),
        Guarded::ApprovalRequired(_) => Err("unexpected approval".into()),
    }
}

struct Catalog {
    calls: AtomicUsize,
}
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(
                        reference
                            .version
                            .clone()
                            .unwrap_or_else(|| id("model-binding-revision")),
                    ),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("synthetic registered component")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| id("write")),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}
struct Policy;
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::ExecuteTool { input } = &request.action {
                if input.execution_args().get("workspace_id") != Some(&json!(WORKSPACE)) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("foreign-workspace"),
                    });
                }
                let approved = input.approval().is_some_and(|approval| {
                    approval.actor_ref() == &id("reviewer")
                        && approval.capability_grant_ref() == &id("reviewer-grant")
                        && context.principal_ref == &id("reviewer")
                });
                if !approved {
                    return Ok(PolicyDecision::RequireApproval {
                        reason: id("write-review"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}
struct Inspector;
impl ModelRouteInspector for Inspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        _: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        // Fixture echo only: production inspectors must fetch authoritative metadata.
        Box::pin(async move {
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: ModelRouteAvailability::Available,
                model_id: Some(route.model_id.clone()),
                model_version: Some(route.model_version.clone()),
                deployment_revision: None,
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id("synthetic-inspection"),
            })
        })
    }
}
struct Estimate;
impl ModelTokenEstimator for Estimate {
    fn estimate(&self, _: &ModelRequest) -> Result<u64, ContractError> {
        // Deliberately synthetic estimate for this fixed fixture, not a tokenizer.
        Ok(512)
    }
}

fn routing(scope: &Scope) -> Result<RoutingSnapshot, ContractError> {
    let capabilities = ModelCapabilities {
        revision: id("features"),
        features: BTreeSet::from([id("text"), id("tool_calling")]),
        options_schema: json!({"type":"object","additionalProperties":false}),
        context_window: 4096.try_into().unwrap(),
        max_output_tokens: 512.try_into().unwrap(),
    };
    let model = ModelDefinition {
        model_key: id("synthetic"),
        family: id("synthetic"),
        provider: id("synthetic"),
        model_id: id("fixture-model"),
        model_version: id("release-1"),
        version_semantics: VersionSemantics::Pinned,
        lifecycle: ModelLifecycle::Active,
        capabilities: capabilities.clone(),
        evidence: vec![],
    };
    let mut binding = ModelBinding {
        binding: reference("primary"),
        model: model.reference(),
        requested_model: model.model_id.clone(),
        adapter: reference("synthetic-adapter"),
        connection_ref: reference("synthetic-connection"),
        target: JsonObject::new(),
        target_schema: json!({"type":"object","additionalProperties":false}),
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        deployment_revision: None,
        version_semantics: VersionSemantics::Pinned,
        capabilities,
        support: ModelSupportStatus::ContractTested,
        evidence: vec![],
    };
    binding.evidence.push(ModelValidationEvidence {
        kind: ModelValidationKind::ContractTest,
        binding_digest: binding.contract_digest(&model)?,
        checked_at_ms: 1000,
        evidence_ref: id("synthetic-contract-test"),
        passed: true,
    });
    RoutingSnapshot::new(
        ModelCatalogSnapshot {
            revision: id("catalog-1"),
            scope: scope.clone(),
            models: vec![model],
            bindings: vec![binding],
            aliases: vec![],
        },
        RoutingPolicy {
            revision: id("policy-1"),
            scope: scope.clone(),
            rules: vec![RoutingRule {
                model_binding: id("primary"),
                purpose: ModelPurpose::Agent,
                primary: reference("primary"),
                fallbacks: vec![],
                fallback_on: vec![],
                version_policy: VersionPolicy::RequirePinned,
                min_support: ModelSupportStatus::ContractTested,
            }],
        },
    )
}

const RECORD: &str = "22222222-2222-4222-8222-222222222222";
const NEW_RECORD: &str = "33333333-3333-4333-8333-333333333333";

struct Model {
    route: ResolvedModelRoute,
    propose: bool,
    calls: AtomicUsize,
}
impl ModelPort for Model {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: self.route.provider.clone(),
            adapter: self.route.adapter.clone(),
            connection_ref: self.route.connection_ref.clone(),
        }
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        assert_eq!(
            self.calls.fetch_add(1, Ordering::SeqCst),
            0,
            "each segment must call its model once"
        );
        assert_eq!(request.route, self.route);
        assert_eq!(request.tools.len(), 1);
        assert_eq!(request.tools[0].name, id("write"));
        assert_eq!(
            request.tools[0].model_input_schema["properties"],
            json!({"query":{"type":"string"}})
        );
        let finish;
        let mut events;
        if self.propose {
            finish = ModelFinish::ToolCalls;
            events = vec![Ok(ModelEvent::ToolArgumentsDelta {
                index: 0,
                provider_call_id: Some("write-report".into()),
                name: Some("write".into()),
                delta: r#"{"query":"report"}"#.into(),
            })];
        } else {
            let calls: Vec<_> = request
                .messages
                .iter()
                .flat_map(|message| &message.content)
                .filter_map(|content| match content {
                    ModelContent::ToolCall {
                        provider_call_id,
                        arguments,
                        ..
                    } => Some((provider_call_id.clone(), arguments.clone())),
                    _ => None,
                })
                .collect();
            assert_eq!(
                calls,
                vec![(id("write-report"), object(json!({"query":"report"})))]
            );
            let observations: Vec<_> = request
                .messages
                .iter()
                .flat_map(|message| &message.content)
                .filter_map(|content| match content {
                    ModelContent::ToolResult {
                        provider_call_id,
                        content,
                    } => Some((provider_call_id.clone(), content.clone())),
                    _ => None,
                })
                .collect();
            assert_eq!(
                observations,
                vec![(
                    id("write-report"),
                    json!({"status":"succeeded","effect":"applied","content":[{"type":"json","value":"written"}]})
                )]
            );
            finish = ModelFinish::Stop;
            events = vec![Ok(ModelEvent::TextDelta {
                text: "Report written.".into(),
            })];
        }
        events.push(Ok(ModelEvent::ResponseCompleted {
            finish,
            metadata: ModelResponseMetadata::default(),
            continuation: vec![],
        }));
        Box::pin(stream::iter(events))
    }
}
struct Resolver {
    value: &'static str,
    revision: &'static str,
    calls: AtomicUsize,
}
impl SystemInputResolver for Resolver {
    fn resolve<'a>(
        &'a self,
        request: &'a SystemInputResolveRequest,
        context: &'a SystemInputResolveContext,
    ) -> PortFuture<'a, Option<ResolvedSystemInput>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.key, id("record_id"));
        assert_eq!(context.principal_ref, id("requester"));
        Box::pin(async move {
            Ok(Some(ResolvedSystemInput {
                value: json!(self.value),
                revision: id(self.revision),
            }))
        })
    }
}
struct Writer {
    calls: AtomicUsize,
    seen: Mutex<Vec<(JsonObject, Id)>>,
}
impl ToolExecutor for Writer {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        assert_eq!(
            self.calls.fetch_add(1, Ordering::SeqCst),
            0,
            "the saved write may execute once"
        );
        assert_eq!(
            args,
            &object(json!({"query":"report","workspace_id":WORKSPACE,"record_id":RECORD}))
        );
        assert_eq!(context.principal_ref, id("reviewer"));
        assert_eq!(context.capability_grant_ref, id("reviewer-grant"));
        self.seen
            .lock()
            .unwrap()
            .push((args.clone(), context.call_id.clone()));
        Box::pin(async move {
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: json!("written"),
                },
                effect: ToolEffect::Applied,
                receipt: Some(json!({"effect_id":"synthetic-write","record_id":args["record_id"]})),
            })
        })
    }
}
fn registry(
    scope: &Scope,
    writer: Arc<Writer>,
) -> Result<(SystemInputRegistry, ToolRegistry), ContractError> {
    let inputs = SystemInputRegistry::new(vec![
        SystemInputDefinition {
            key: id("workspace_id"),
            version: id("1"),
            value_schema: json!({"type":"string","format":"uuid"}),
            source: SystemInputSource::Run {},
        },
        SystemInputDefinition {
            key: id("record_id"),
            version: id("1"),
            value_schema: json!({"type":"string","format":"uuid"}),
            source: SystemInputSource::Resolver {
                resolver_ref: reference("records"),
            },
        },
    ])?;
    let compiled = SchemaCompiler::new().compile(ToolDescriptor {
        tool: reference("write"), name: id("write"), description: "Write an authorized report".into(),
        input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"workspace_id":{"type":"string","format":"uuid"},"record_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id","record_id"],"additionalProperties":false}),
        agent_parameters: vec!["query".into()], system_bindings: None, output_schema: json!({"type":"string"}),
        side_effect: ToolSideEffect::Write, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false,
        max_output_bytes: 1024.try_into().unwrap(),
    }, &inputs)?;
    Ok((
        inputs,
        ToolRegistry::new(
            scope.clone(),
            vec![ToolRegistration {
                compiled,
                executor: writer,
            }],
        )?,
    ))
}
fn agent(
    scope: &Scope,
    store: Arc<SqliteStateStore>,
    model: Arc<Model>,
    writer: Arc<Writer>,
    resolver: Arc<Resolver>,
    catalog: Arc<Catalog>,
) -> Result<Agent, ContractError> {
    let (system_inputs, tools) = registry(scope, writer)?;
    let policy = Arc::new(PolicyGate::new(Arc::new(Policy), Duration::from_secs(1))?);
    let profile = AgentProfile::from_json(
        r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"writer","version":"1",
        "name":"Writer","description":"Synthetic approval resume consumer","instructions":{"text":"Write the report after authorization"},
        "model_binding":"primary","tools":[{"tool_id":"write","version":"1"}],"skills":[],"connectors":[],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":3,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}
    }"#,
    )?;
    create_agent(
        profile,
        AgentBindings {
            scope: scope.clone(),
            state: store,
            policy: policy.clone(),
            profile_resolver: catalog,
            model_exchange: Arc::new(
                ModelExchange::new(model, policy)
                    .with_route_inspector(Arc::new(Inspector), Duration::from_secs(1))?,
            ),
            router: Arc::new(PolicyModelRouter::new(routing(scope)?)?),
            host_instructions: vec!["Use only authorized inputs.".into()],
            system_inputs,
            tools: Some(Arc::new(tools)),
            system_input_resolver: Some(resolver),
            external_receipt_verifier: None,
            clock: Arc::new(SystemClock::new()),
            ids: Arc::new(RandomIdSource),
            token_estimator: Arc::new(Estimate),
            settings: AgentSettings {
                require_durable: true,
                max_output_tokens: 128.try_into().unwrap(),
                ..AgentSettings::default()
            },
        },
    )
}
fn context(scope: &Scope, reviewer: bool) -> ExecutionContext {
    ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id(if reviewer { "reviewer" } else { "requester" }),
            capability_grant_ref: id(if reviewer {
                "reviewer-grant"
            } else {
                "requester-grant"
            }),
            trace_context: None,
            system_inputs: if reviewer {
                None
            } else {
                Some(SystemInputs::new(object(json!({"workspace_id":WORKSPACE}))))
            },
        },
        Default::default(),
    )
}
struct TemporaryDatabase(std::path::PathBuf);
impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let temporary = TemporaryDatabase(
        std::env::temp_dir().join(format!("wickle-resume-{}", RandomIdSource.next_id()?)),
    );
    std::fs::create_dir(&temporary.0)?;
    let database = temporary.0.join("state.sqlite3");
    let store = Arc::new(SqliteStateStore::open(&database)?);
    let model = Arc::new(Model {
        route: routing(&scope)?.route_for_binding(&reference("primary"))?,
        propose: true,
        calls: AtomicUsize::new(0),
    });
    let writer = Arc::new(Writer {
        calls: AtomicUsize::new(0),
        seen: Mutex::new(vec![]),
    });
    let resolver = Arc::new(Resolver {
        value: RECORD,
        revision: "record-A",
        calls: AtomicUsize::new(0),
    });
    let catalog = Arc::new(Catalog {
        calls: AtomicUsize::new(0),
    });
    let initial_agent = agent(
        &scope,
        store.clone(),
        model.clone(),
        writer.clone(),
        resolver.clone(),
        catalog.clone(),
    )?;
    let caller = context(&scope, false);
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Write the report".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    };
    let handle = completed(initial_agent.start(request, caller.clone()).await?)?;
    let waiting = completed(handle.outcome(&caller).await?)?;
    assert_eq!(waiting.result.status(), RunStatus::Waiting);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(writer.calls.load(Ordering::SeqCst), 0);
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    let run_id = handle.run_id().clone();
    let saved = store.load(&scope, &run_id).await?;
    let wait = saved.snapshot.wait.clone().ok_or("missing wait")?;
    let WaitTarget::Approval { target } = wait.target else {
        return Err("expected tool approval".into());
    };
    let command = ResumeCommand {
        run_id: run_id.clone(),
        expected_revision: saved.snapshot.revision,
        command_id: id("approve-report"),
        action: ResumeAction::Approve {
            wait_id: wait.wait_id,
            target,
        },
    };
    let bound_ref = saved.snapshot.tool_ledger[0]
        .call
        .bound_input_ref
        .clone()
        .ok_or("missing binding")?;
    let bound_record = store.read_record(&scope, &bound_ref).await?;
    let (_, tools) = registry(&scope, writer.clone())?;
    let bound = BoundToolInput::restore(
        &bound_record,
        &tools.get(&id("write")).ok_or("tool missing")?.compiled,
        &scope,
        &run_id,
        &saved.snapshot.tool_ledger[0].call,
        saved.snapshot.system_inputs.as_ref(),
    )?;
    assert_eq!(
        bound.execution_args(),
        &object(json!({"query":"report","workspace_id":WORKSPACE,"record_id":RECORD}))
    );
    assert_eq!(
        bound.system_inputs()["record_id"]
            .resolved
            .as_ref()
            .ok_or("record missing")?
            .revision,
        id("record-A")
    );
    let events: Vec<_> = handle.events(0, caller.clone()).try_collect().await?;
    assert_eq!(
        events.last().ok_or("missing wait event")?.event_type,
        "run.waiting"
    );
    let old_store = Arc::downgrade(&store);
    let old_model = Arc::downgrade(&model);
    let old_resolver = Arc::downgrade(&resolver);
    drop(tools);
    drop(handle);
    drop(initial_agent);
    drop(store);
    drop(model);
    drop(writer);
    drop(resolver);
    drop(catalog);
    // A saved wait ends its driver; verify no previous Host instance remains alive.
    tokio::time::timeout(Duration::from_secs(5), async {
        while old_store.upgrade().is_some()
            || old_model.upgrade().is_some()
            || old_resolver.upgrade().is_some()
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;

    let reopened = Arc::new(SqliteStateStore::open(&database)?);
    let restored = reopened.load(&scope, &run_id).await?;
    assert_eq!(restored.snapshot, saved.snapshot);
    assert_eq!(
        restored.session.prompt_snapshot,
        saved.session.prompt_snapshot
    );
    let model = Arc::new(Model {
        route: routing(&scope)?.route_for_binding(&reference("primary"))?,
        propose: false,
        calls: AtomicUsize::new(0),
    });
    let writer = Arc::new(Writer {
        calls: AtomicUsize::new(0),
        seen: Mutex::new(vec![]),
    });
    let resolver = Arc::new(Resolver {
        value: NEW_RECORD,
        revision: "record-B",
        calls: AtomicUsize::new(0),
    });
    let catalog = Arc::new(Catalog {
        calls: AtomicUsize::new(0),
    });
    let resumed_agent = agent(
        &scope,
        reopened.clone(),
        model.clone(),
        writer.clone(),
        resolver.clone(),
        catalog.clone(),
    )?;
    let reviewer = context(&scope, true);
    let resumed = completed(
        resumed_agent
            .resume(command.clone(), reviewer.clone())
            .await?,
    )?;
    assert_eq!(resumed.run_id(), &run_id);
    let outcome = completed(resumed.outcome(&reviewer).await?)?;
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "Report written.".into()
        }]
    );
    assert_eq!(
        (outcome.usage.model_calls, outcome.usage.tool_attempts),
        (2, 1)
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(writer.calls.load(Ordering::SeqCst), 1);
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
    assert_eq!(catalog.calls.load(Ordering::SeqCst), 0);
    let finished = reopened.load(&scope, &run_id).await?;
    assert_eq!(
        finished.snapshot.tool_ledger[0]
            .call
            .bound_input_ref
            .as_ref(),
        Some(&bound_ref)
    );
    assert_eq!(
        finished.snapshot.system_inputs,
        saved.snapshot.system_inputs
    );
    assert_eq!(
        finished.snapshot.routing_snapshot_ref,
        saved.snapshot.routing_snapshot_ref
    );
    assert_eq!(finished.snapshot.resume_receipts.len(), 1);
    let acceptance = &finished.snapshot.resume_receipts[0];
    assert_eq!(acceptance.command, command);
    assert_eq!(acceptance.actor_ref, id("reviewer"));
    assert_eq!(
        acceptance.previous_last_event_seq,
        saved.snapshot.last_event_seq
    );
    let previous = reopened
        .read_record(&scope, &acceptance.previous_outcome_ref)
        .await?;
    assert_eq!(
        serde_json::from_value::<RunOutcome>(previous.value().clone())?,
        waiting
    );
    let continued: Vec<_> = resumed
        .events(saved.snapshot.last_event_seq, reviewer.clone())
        .try_collect()
        .await?;
    assert_eq!(continued[0].seq.get(), saved.snapshot.last_event_seq + 1);
    assert_eq!(continued[0].event_type, "run.resumed");
    assert_eq!(
        continued.last().ok_or("missing finish event")?.event_type,
        "run.finished"
    );
    let before_replay = (
        model.calls.load(Ordering::SeqCst),
        writer.calls.load(Ordering::SeqCst),
        resolver.calls.load(Ordering::SeqCst),
        catalog.calls.load(Ordering::SeqCst),
    );
    let replay = completed(resumed_agent.resume(command, reviewer.clone()).await?)?;
    assert_eq!(completed(replay.outcome(&reviewer).await?)?, outcome);
    assert_eq!(
        (
            model.calls.load(Ordering::SeqCst),
            writer.calls.load(Ordering::SeqCst),
            resolver.calls.load(Ordering::SeqCst),
            catalog.calls.load(Ordering::SeqCst)
        ),
        before_replay
    );
    assert_eq!(
        reopened.load(&scope, &run_id).await?.snapshot,
        finished.snapshot
    );
    println!(
        "resume consumer: real SQLite wait/reopen; same Run and frozen inputs; new reviewer; one write; contiguous events; duplicate command adds no model, tool, resolver, or metadata calls (synthetic Host ports, no provider network)"
    );
    Ok(())
}
```

## `tests/support/routing_consumer.rs`

```rust
// Synthetic adapters and metadata inspector: no provider network calls are made.
// The fixed clock supports deterministic accounting; this does not test timeouts.
use futures_util::stream;
use serde_json::json;
use std::collections::BTreeSet;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;
use wickle_model_router::{ModelDispatcherEntry, PolicyModelRouter, RegistryModelDispatcher};
use wickle_state_sqlite::SqliteStateStore;

fn id(s: &str) -> Id {
    Id::new(s).expect("example identifiers")
}
struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        r: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id("revision-1")),
                    ..r.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("registered model")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: None,
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

fn reference(name: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id("1"),
    }
}
fn routing_snapshot(scope: &Scope) -> Result<RoutingSnapshot, ContractError> {
    let mut models = vec![];
    let mut bindings = vec![];
    for name in ["first", "second"] {
        let capabilities = ModelCapabilities {
            revision: id("capabilities"),
            features: BTreeSet::from([id("text")]),
            options_schema: json!({"type":"object","properties":{"reasoning_effort":{"enum":["high"]}},"additionalProperties":false}),
            context_window: 4096.try_into().unwrap(),
            max_output_tokens: 512.try_into().unwrap(),
        };
        let model = ModelDefinition {
            model_key: id(name),
            family: id("example"),
            provider: id(name),
            model_id: id("example-model"),
            model_version: id("release-1"),
            version_semantics: VersionSemantics::Pinned,
            lifecycle: ModelLifecycle::Active,
            capabilities: capabilities.clone(),
            evidence: vec![],
        };
        let mut binding = ModelBinding {
            binding: reference(name),
            model: model.reference(),
            requested_model: model.model_id.clone(),
            adapter: reference("adapter"),
            connection_ref: reference(&format!("{name}-account")),
            target: JsonObject::new(),
            target_schema: json!({"type":"object","additionalProperties":false}),
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("v1"),
            },
            deployment_revision: None,
            version_semantics: VersionSemantics::Pinned,
            capabilities,
            support: ModelSupportStatus::ContractTested,
            evidence: vec![],
        };
        binding.evidence.push(ModelValidationEvidence {
            kind: ModelValidationKind::ContractTest,
            binding_digest: binding.contract_digest(&model)?,
            checked_at_ms: 1000,
            evidence_ref: id("synthetic-fixture"),
            passed: true,
        });
        models.push(model);
        bindings.push(binding);
    }
    RoutingSnapshot::new(
        ModelCatalogSnapshot {
            revision: id("catalog"),
            scope: scope.clone(),
            models,
            bindings,
            aliases: vec![],
        },
        RoutingPolicy {
            revision: id("policy"),
            scope: scope.clone(),
            rules: vec![RoutingRule {
                model_binding: id("primary"),
                purpose: ModelPurpose::Agent,
                primary: reference("first"),
                fallbacks: vec![reference("second")],
                fallback_on: vec![ModelFailureKind::RateLimited],
                version_policy: VersionPolicy::RequirePinned,
                min_support: ModelSupportStatus::ContractTested,
            }],
        },
    )
}
struct TemporaryDatabase(std::path::PathBuf);
impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct ExampleClock;
impl Clock for ExampleClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: 1000,
            monotonic_ms: 1000,
        })
    }
    fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}
struct ExamplePolicy;
impl PolicyPort for ExamplePolicy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::InvokeModel { route, .. } = &request.action {
                if route.connection_ref.id == id(&format!("{}-account", route.provider)) {
                    return Ok(PolicyDecision::Allow {});
                }
            }
            Ok(PolicyDecision::Deny {
                reason: id("unknown-account"),
            })
        })
    }
}
struct ExampleInspector;
// This echo is a fixture only. A real inspector must read authoritative provider
// metadata instead of presenting requested values as independently observed facts.
impl ModelRouteInspector for ExampleInspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        _: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: ModelRouteAvailability::Available,
                model_id: Some(route.model_id.clone()),
                model_version: Some(route.model_version.clone()),
                deployment_revision: None,
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id("synthetic-metadata-check"),
            })
        })
    }
}
struct ExampleModel {
    route: ResolvedModelRoute,
    calls: AtomicUsize,
    fail: bool,
}
impl ModelPort for ExampleModel {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: self.route.provider.clone(),
            adapter: self.route.adapter.clone(),
            connection_ref: self.route.connection_ref.clone(),
        }
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        assert_eq!(
            request.options.get("reasoning_effort"),
            Some(&json!("high"))
        );
        assert_eq!(request.route, self.route);
        self.calls.fetch_add(1, Ordering::SeqCst);
        let events = if self.fail {
            vec![Ok(ModelEvent::ResponseError {
                kind: ModelFailureKind::RateLimited,
                metadata: ModelResponseMetadata::default(),
            })]
        } else {
            vec![
                Ok(ModelEvent::TextDelta {
                    text: "second provider result".into(),
                }),
                Ok(ModelEvent::ResponseCompleted {
                    finish: ModelFinish::Stop,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![],
                }),
            ]
        };
        Box::pin(stream::iter(events))
    }
}
struct ExampleProjector;
impl ModelRequestProjector for ExampleProjector {
    fn project<'a>(
        &'a self,
        selection: &'a RouteSelection,
        input: &'a RoutedModelInput,
        _: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ProjectedModelRequest> {
        Box::pin(async move {
            Ok(ProjectedModelRequest {
                input_tokens: 32,
                request: ModelRequest {
                    request_id: input.model_step_id.clone(),
                    purpose: input.routing.purpose,
                    route: selection.route.clone(),
                    messages: vec![ModelMessage {
                        role: ModelRole::User,
                        content: vec![ModelContent::Text {
                            text: "Inspect stored state".into(),
                        }],
                    }],
                    tools: vec![],
                    output: ModelOutput::Text {},
                    max_output_tokens: input.routing.max_output_tokens,
                    options: input.routing.options.clone(),
                    limits: ModelResponseLimits {
                        max_input_bytes: 8192,
                        max_response_bytes: 4096,
                        max_delta_bytes: 1024,
                        max_events: 8,
                        max_tool_calls: 0,
                    },
                },
            })
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let profile = AgentProfile::from_json(
        r#"{
      "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
      "name":"Assistant","description":"Storage example","instructions":{"text":"Use evidence"},
      "model_binding":"primary","tools":[],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
      "limits":{"max_model_calls":4,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":2,"max_elapsed_ms":10000}
    }"#,
    )?;
    let profile = ProfileValidator::new(&Catalog)
        .validate(&profile, &scope)
        .await?;
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Inspect stored state".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::from([("reasoning_effort".into(), json!("high"))]),
        output_contract: None,
    };
    let request_record =
        ProtectedRecord::new(id("request-record"), 1, serde_json::to_value(&request)?);
    let prompt = ProtectedRecord::new(id("prompt"), 1, json!({"text":"Use evidence"}));
    let snapshot = RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id("run"),
        request_digest: admission_digest(&request, &profile, None),
        request: request.clone(),
        scope: scope.clone(),
        timing: RunTiming::new(1000, profile.profile().limits.max_elapsed_ms.get()).unwrap(),
        reservations: vec![],
        resume_receipts: vec![],
        limits: profile.profile().limits.clone(),
        profile,
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        usage: BudgetUsage::default(),
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: None,
        wait: None,
        outcome: None,
        assembly_ref: None,
        routing_snapshot_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    let started = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("started"),
        scope: scope.clone(),
        run_id: id("run"),
        session_id: id("session"),
        seq: 1.try_into()?,
        timestamp_ms: 1000,
        payload: RunEventPayload::RunStarted {
            request_ref: request_record.reference().clone(),
            profile_digest: snapshot.profile.profile_digest().clone(),
        },
    };
    let message = Message {
        message_id: id("user-message"),
        run_id: id("run"),
        sequence: 1.try_into()?,
        role: MessageRole::User,
        content: vec![ContentBlock::Content {
            content: request.input[0].clone(),
        }],
        origin: MessageOrigin::User,
        visibility: Visibility::UserAndModel,
    };
    let input = AdmissionInput {
        snapshot,
        prompt_snapshot: prompt.reference().clone(),
        messages: vec![message],
        events: vec![started],
        records: vec![request_record, prompt],
        require_durable: true,
    };
    let temporary = TemporaryDatabase(
        std::env::temp_dir().join(format!("wickle-routing-{}", RandomIdSource.next_id()?)),
    );
    std::fs::create_dir(&temporary.0)?;
    let database = temporary.0.join("state.sqlite3");
    let store = Arc::new(SqliteStateStore::open(&database)?);
    store.admit(&scope, input).await?;
    let lease = store
        .acquire_lease(&scope, &id("run"), &id("worker"), 1000, 10_000)
        .await?;
    let budget = RunBudget::attach(
        store.clone(),
        Arc::new(ExampleClock),
        Arc::new(RandomIdSource),
        scope.clone(),
        id("run"),
        lease.clone(),
        Default::default(),
    )
    .await?;
    let snapshot = routing_snapshot(&scope)?;
    let router = PolicyModelRouter::new(snapshot.clone())?;
    let first = Arc::new(ExampleModel {
        route: snapshot.route_for_binding(&reference("first"))?,
        calls: AtomicUsize::new(0),
        fail: true,
    });
    let second = Arc::new(ExampleModel {
        route: snapshot.route_for_binding(&reference("second"))?,
        calls: AtomicUsize::new(0),
        fail: false,
    });
    let exchange = ModelExchange::with_dispatcher(
        Arc::new(RegistryModelDispatcher::new(vec![
            ModelDispatcherEntry {
                scope: scope.clone(),
                port: first.clone(),
            },
            ModelDispatcherEntry {
                scope: scope.clone(),
                port: second.clone(),
            },
        ])?),
        Arc::new(PolicyGate::new(
            Arc::new(ExamplePolicy),
            Duration::from_secs(1),
        )?),
    )
    .with_route_inspector(Arc::new(ExampleInspector), Duration::from_secs(1))?;
    let input = RoutedModelInput {
        model_step_id: id("step"),
        routing: RouteRequest {
            model_binding: id("primary"),
            purpose: ModelPurpose::Agent,
            required_capabilities: BTreeSet::from([id("text")]),
            input_tokens: 32,
            max_output_tokens: 128.try_into()?,
            options: JsonObject::from([("reasoning_effort".into(), json!("high"))]),
            scope: scope.clone(),
            allowed_bindings: vec![id("first"), id("second")],
            version_policy: VersionPolicy::RequirePinned,
            previous_route: None,
            previous_failure: None,
        },
    };
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id("actor"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    );
    let first_result = exchange
        .generate_routed(&router, &input, &ExampleProjector, &context, &budget)
        .await?;
    assert!(
        matches!(&first_result, Guarded::Completed(ModelExchangeOutcome::Completed { response }) if response.text == "second provider result")
    );
    let saved = store.load(&scope, &id("run")).await?;
    assert_eq!(saved.snapshot.usage.model_calls, 2);
    assert_eq!(saved.snapshot.usage.recovery_attempts, 1);
    assert_eq!(
        saved.snapshot.model_ledger[1].selection_reason,
        id("fallback_rate_limited")
    );
    for invocation in &saved.snapshot.model_ledger {
        assert!(invocation.reported_model_version.is_none());
        let reference = invocation
            .inspection_ref
            .as_ref()
            .ok_or("inspection record missing")?;
        let record = store.read_record(&scope, reference).await?;
        let observation: ModelRouteObservation = serde_json::from_value(record.value().clone())?;
        observation.validate(&invocation.route, VersionPolicy::RequirePinned)?;
    }
    // Reopen persisted routing, observation, and step-input records from SQLite.
    drop(budget);
    drop(store);
    let restored = Arc::new(SqliteStateStore::open(&database)?);
    let resumed_budget = RunBudget::attach(
        restored.clone(),
        Arc::new(ExampleClock),
        Arc::new(RandomIdSource),
        scope.clone(),
        id("run"),
        lease,
        Default::default(),
    )
    .await?;
    let second_result = exchange
        .generate_routed(
            &router,
            &input,
            &ExampleProjector,
            &context,
            &resumed_budget,
        )
        .await?;
    assert!(
        matches!(&second_result, Guarded::Completed(ModelExchangeOutcome::Completed { response }) if response.text == "second provider result")
    );
    assert_eq!(first.calls.load(Ordering::SeqCst), 1);
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        restored.load(&scope, &id("run")).await?.snapshot.revision,
        saved.snapshot.revision
    );
    println!(
        "routing consumer: separate accounts selected; rate-limit fallback charged two model calls and one recovery; effort preserved; complete step reused after SQLite reopen with zero new calls"
    );
    Ok(())
}
```

## `tests/support/sqlite_consumer.rs`

```rust
use serde_json::json;
use std::collections::BTreeSet;
use wickle::*;
use wickle_state_sqlite::SqliteStateStore;

struct TemporaryStore(std::path::PathBuf);
impl TemporaryStore {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "wickle-sqlite-consumer-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir(&directory)?;
        Ok(Self(directory))
    }
}
impl Drop for TemporaryStore {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn id(s: &str) -> Id {
    Id::new(s).expect("example identifiers")
}
struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        r: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id("revision-1")),
                    ..r.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("registered model")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: None,
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    if std::env::args().nth(1).as_deref() == Some("verify") {
        let database = std::env::args_os()
            .nth(2)
            .ok_or("database argument missing")?;
        let parent: u32 = std::env::args()
            .nth(3)
            .ok_or("parent process missing")?
            .parse()?;
        assert_ne!(parent, std::process::id());
        let store = SqliteStateStore::open(database)?;
        let restored = store.load(&scope, &id("run")).await?;
        assert_eq!(restored.snapshot.status, RunStatus::Succeeded);
        assert_eq!(restored.snapshot.revision, 1);
        assert_eq!(
            restored.snapshot.request.model_options.get("reasoning_effort"),
            Some(&json!("high"))
        );
        assert!(restored.session.active_run_id.is_none());
        let outcome = restored
            .snapshot
            .outcome
            .as_ref()
            .ok_or("outcome missing")?;
        assert_eq!(
            outcome.output,
            vec![InputContent::Text {
                text: "Stored result".into()
            }]
        );
        let reference = ProtectedRecord::new(id("outcome"), 1, serde_json::to_value(outcome)?);
        assert_eq!(
            store
                .read_record(&scope, reference.reference())
                .await?
                .value(),
            reference.value()
        );
        let events = store.read_events(&scope, &id("run"), 0, 10).await?;
        assert_eq!(events.events.len(), 2);
        assert_eq!(events.last_available_seq, 2);
        println!("SQLite child: reopened completed run, outcome record, and two committed events");
        return Ok(());
    }
    let temporary = TemporaryStore::new()?;
    let database = temporary.0.join("state.sqlite3");
    let profile = AgentProfile::from_json(
        r#"{
      "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
      "name":"Assistant","description":"Storage example","instructions":{"text":"Use evidence"},
      "model_binding":"primary","tools":[],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
      "limits":{"max_model_calls":4,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
    }"#,
    )?;
    let profile = ProfileValidator::new(&Catalog)
        .validate(&profile, &scope)
        .await?;
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Inspect stored state".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: std::collections::BTreeMap::from([
            ("reasoning_effort".into(), json!("high")),
        ]),
        output_contract: None,
    };
    let request_record =
        ProtectedRecord::new(id("request-record"), 1, serde_json::to_value(&request)?);
    let prompt = ProtectedRecord::new(id("prompt"), 1, json!({"text":"Use evidence"}));
    let snapshot = RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id("run"),
        request_digest: admission_digest(&request, &profile, None),
        request: request.clone(),
        scope: scope.clone(),
        timing: RunTiming::new(1000, profile.profile().limits.max_elapsed_ms.get()).unwrap(),
        reservations: vec![],
        resume_receipts: vec![],
        limits: profile.profile().limits.clone(),
        profile,
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        usage: BudgetUsage::default(),
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: None,
        wait: None,
        outcome: None,
        assembly_ref: None,
        routing_snapshot_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    let started = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("started"),
        scope: scope.clone(),
        run_id: id("run"),
        session_id: id("session"),
        seq: 1.try_into()?,
        timestamp_ms: 1000,
        payload: RunEventPayload::RunStarted {
            request_ref: request_record.reference().clone(),
            profile_digest: snapshot.profile.profile_digest().clone(),
        },
    };
    let message = Message {
        message_id: id("user-message"),
        run_id: id("run"),
        sequence: 1.try_into()?,
        role: MessageRole::User,
        content: vec![ContentBlock::Content {
            content: request.input[0].clone(),
        }],
        origin: MessageOrigin::User,
        visibility: Visibility::UserAndModel,
    };
    let input = AdmissionInput {
        snapshot,
        prompt_snapshot: prompt.reference().clone(),
        messages: vec![message],
        events: vec![started],
        records: vec![request_record, prompt],
        require_durable: true,
    };
    let store = SqliteStateStore::open(&database)?;
    assert!(store.capabilities().durable);
    assert!(store.capabilities().cross_process_leases);
    let first = store.admit(&scope, input.clone()).await?;
    let replay = store.admit(&scope, input).await?;
    assert!(first.created);
    assert!(!replay.created);
    assert_eq!(first.state, replay.state);
    println!(
        "admission: created={}, retry_created={}, run={}",
        first.created, replay.created, replay.state.snapshot.run_id
    );
    let lease = store
        .acquire_lease(&scope, &id("run"), &id("worker"), 1000, 10_000)
        .await?;
    let mut next = first.state.snapshot.clone();
    next.revision = 1;
    next.last_event_seq = 2;
    next.status = RunStatus::Succeeded;
    next.phase = RunPhase::Finish;
    let outcome = RunOutcome {
        result: OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded,
        },
        output: vec![InputContent::Text {
            text: "Stored result".into(),
        }],
        artifacts: vec![],
        usage: next.usage.clone(),
        checkpoint_revision: 1,
        verification: None,
        unresolved_effects: vec![],
    };
    let outcome_record = ProtectedRecord::new(id("outcome"), 1, serde_json::to_value(&outcome)?);
    next.outcome = Some(outcome);
    let event = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("finished"),
        scope: scope.clone(),
        run_id: id("run"),
        session_id: id("session"),
        seq: 2.try_into()?,
        timestamp_ms: 1001,
        payload: RunEventPayload::RunFinished {
            outcome_ref: outcome_record.reference().clone(),
        },
    };
    let result = store
        .commit(
            &scope,
            &id("run"),
            CommitInput {
                expected_revision: 0,
                lease,
                now_ms: 1001,
                snapshot: next,
                messages: vec![],
                events: vec![event],
                records: vec![outcome_record],
            },
        )
        .await?;
    println!(
        "committed: revision={}, status={:?}, active_run={:?}",
        result.snapshot.revision, result.snapshot.status, result.session.active_run_id
    );
    let events = store.read_events(&scope, &id("run"), 0, 10).await?;
    println!(
        "event replay: count={}, last_seq={}",
        events.events.len(),
        events.last_available_seq
    );
    let foreign = Scope {
        tenant_id: id("another-tenant"),
        ..scope.clone()
    };
    let rejected = store.load(&foreign, &id("run")).await;
    assert!(matches!(&rejected, Err(error) if error.code == ErrorCode::StateNotFound));
    println!(
        "foreign scope: {}",
        match rejected {
            Err(error) => format!("{:?}", error.code),
            Ok(_) => "UNEXPECTED ACCESS".into(),
        }
    );
    drop(store);
    let status = std::process::Command::new(std::env::current_exe()?)
        .arg("verify")
        .arg(&database)
        .arg(std::process::id().to_string())
        .status()?;
    if !status.success() {
        return Err("independent SQLite reader failed".into());
    }
    println!(
        "SQLite consumer: atomic admission/commit, scope isolation, and independent process restoration passed"
    );
    Ok(())
}
```

## `tests/support/state_consumer.rs`

```rust
use serde_json::json;
use std::collections::BTreeSet;
use wickle::*;

fn id(s: &str) -> Id {
    Id::new(s).expect("example identifiers")
}
struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        r: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id("revision-1")),
                    ..r.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("registered model")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: None,
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let profile = AgentProfile::from_json(
        r#"{
      "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
      "name":"Assistant","description":"Storage example","instructions":{"text":"Use evidence"},
      "model_binding":"primary","tools":[],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
      "limits":{"max_model_calls":4,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
    }"#,
    )?;
    let profile = ProfileValidator::new(&Catalog)
        .validate(&profile, &scope)
        .await?;
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Inspect stored state".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    };
    let request_record =
        ProtectedRecord::new(id("request-record"), 1, serde_json::to_value(&request)?);
    let prompt = ProtectedRecord::new(id("prompt"), 1, json!({"text":"Use evidence"}));
    let snapshot = RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id("run"),
        request_digest: admission_digest(&request, &profile, None),
        request: request.clone(),
        scope: scope.clone(),
        timing: RunTiming::new(1000, profile.profile().limits.max_elapsed_ms.get()).unwrap(),
        reservations: vec![],
        resume_receipts: vec![],
        limits: profile.profile().limits.clone(),
        profile,
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        usage: BudgetUsage::default(),
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: None,
        wait: None,
        outcome: None,
        assembly_ref: None,
        routing_snapshot_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    let started = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("started"),
        scope: scope.clone(),
        run_id: id("run"),
        session_id: id("session"),
        seq: 1.try_into()?,
        timestamp_ms: 1000,
        payload: RunEventPayload::RunStarted {
            request_ref: request_record.reference().clone(),
            profile_digest: snapshot.profile.profile_digest().clone(),
        },
    };
    let message = Message {
        message_id: id("user-message"),
        run_id: id("run"),
        sequence: 1.try_into()?,
        role: MessageRole::User,
        content: vec![ContentBlock::Content {
            content: request.input[0].clone(),
        }],
        origin: MessageOrigin::User,
        visibility: Visibility::UserAndModel,
    };
    let input = AdmissionInput {
        snapshot,
        prompt_snapshot: prompt.reference().clone(),
        messages: vec![message],
        events: vec![started],
        records: vec![request_record, prompt],
        require_durable: false,
    };
    let store = MemoryStateStore::new();
    let first = store.admit(&scope, input.clone()).await?;
    let replay = store.admit(&scope, input).await?;
    println!(
        "admission: created={}, retry_created={}, run={}",
        first.created, replay.created, replay.state.snapshot.run_id
    );
    let lease = store
        .acquire_lease(&scope, &id("run"), &id("worker"), 1000, 100)
        .await?;
    let mut next = first.state.snapshot.clone();
    next.revision = 1;
    next.last_event_seq = 2;
    next.status = RunStatus::Succeeded;
    next.phase = RunPhase::Finish;
    let outcome = RunOutcome {
        result: OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded,
        },
        output: vec![InputContent::Text {
            text: "Stored result".into(),
        }],
        artifacts: vec![],
        usage: next.usage.clone(),
        checkpoint_revision: 1,
        verification: None,
        unresolved_effects: vec![],
    };
    let outcome_record = ProtectedRecord::new(id("outcome"), 1, serde_json::to_value(&outcome)?);
    next.outcome = Some(outcome);
    let event = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("finished"),
        scope: scope.clone(),
        run_id: id("run"),
        session_id: id("session"),
        seq: 2.try_into()?,
        timestamp_ms: 1001,
        payload: RunEventPayload::RunFinished {
            outcome_ref: outcome_record.reference().clone(),
        },
    };
    let result = store
        .commit(
            &scope,
            &id("run"),
            CommitInput {
                expected_revision: 0,
                lease,
                now_ms: 1001,
                snapshot: next,
                messages: vec![],
                events: vec![event],
                records: vec![outcome_record],
            },
        )
        .await?;
    println!(
        "committed: revision={}, status={:?}, active_run={:?}",
        result.snapshot.revision, result.snapshot.status, result.session.active_run_id
    );
    let events = store.read_events(&scope, &id("run"), 0, 10).await?;
    println!(
        "event replay: count={}, last_seq={}",
        events.events.len(),
        events.last_available_seq
    );
    let foreign = Scope {
        tenant_id: id("another-tenant"),
        ..scope
    };
    let rejected = store.load(&foreign, &id("run")).await;
    println!(
        "foreign scope: {}",
        match rejected {
            Err(error) => format!("{:?}", error.code),
            Ok(_) => "UNEXPECTED ACCESS".into(),
        }
    );
    Ok(())
}
```

## `tests/support/tool_loop_consumer.rs`

```rust
// Synthetic model, tool, and metadata inspector. No provider network or business
// database calls occur. SQLite is real; the assertions exercise the public Agent API.
use futures_util::{TryStreamExt, stream};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;
use wickle_model_router::PolicyModelRouter;
use wickle_state_sqlite::SqliteStateStore;

const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";
fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .expect("object fixture")
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
fn completed<T>(value: Guarded<T>) -> Result<T, Box<dyn std::error::Error>> {
    match value {
        Guarded::Completed(value) => Ok(value),
        Guarded::ApprovalRequired(_) => Err("unexpected approval".into()),
    }
}

struct Catalog {
    calls: AtomicUsize,
}
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(
                        reference
                            .version
                            .clone()
                            .unwrap_or_else(|| id("model-binding-revision")),
                    ),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("synthetic registered component")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| id("search")),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}
struct Policy;
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::ExecuteTool { input } = &request.action {
                if input.execution_args().get("workspace_id") != Some(&json!(WORKSPACE)) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("foreign-workspace"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}
struct Inspector;
impl ModelRouteInspector for Inspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        _: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        // Fixture echo only: production inspectors must fetch authoritative metadata.
        Box::pin(async move {
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: ModelRouteAvailability::Available,
                model_id: Some(route.model_id.clone()),
                model_version: Some(route.model_version.clone()),
                deployment_revision: None,
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id("synthetic-inspection"),
            })
        })
    }
}
struct Estimate;
impl ModelTokenEstimator for Estimate {
    fn estimate(&self, _: &ModelRequest) -> Result<u64, ContractError> {
        // Deliberately synthetic estimate for this fixed fixture, not a tokenizer.
        Ok(512)
    }
}

fn routing(scope: &Scope) -> Result<RoutingSnapshot, ContractError> {
    let capabilities = ModelCapabilities {
        revision: id("features"),
        features: BTreeSet::from([id("text"), id("tool_calling")]),
        options_schema: json!({"type":"object","additionalProperties":false}),
        context_window: 4096.try_into().unwrap(),
        max_output_tokens: 512.try_into().unwrap(),
    };
    let model = ModelDefinition {
        model_key: id("synthetic"),
        family: id("synthetic"),
        provider: id("synthetic"),
        model_id: id("fixture-model"),
        model_version: id("release-1"),
        version_semantics: VersionSemantics::Pinned,
        lifecycle: ModelLifecycle::Active,
        capabilities: capabilities.clone(),
        evidence: vec![],
    };
    let mut binding = ModelBinding {
        binding: reference("primary"),
        model: model.reference(),
        requested_model: model.model_id.clone(),
        adapter: reference("synthetic-adapter"),
        connection_ref: reference("synthetic-connection"),
        target: JsonObject::new(),
        target_schema: json!({"type":"object","additionalProperties":false}),
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        deployment_revision: None,
        version_semantics: VersionSemantics::Pinned,
        capabilities,
        support: ModelSupportStatus::ContractTested,
        evidence: vec![],
    };
    binding.evidence.push(ModelValidationEvidence {
        kind: ModelValidationKind::ContractTest,
        binding_digest: binding.contract_digest(&model)?,
        checked_at_ms: 1000,
        evidence_ref: id("synthetic-contract-test"),
        passed: true,
    });
    RoutingSnapshot::new(
        ModelCatalogSnapshot {
            revision: id("catalog-1"),
            scope: scope.clone(),
            models: vec![model],
            bindings: vec![binding],
            aliases: vec![],
        },
        RoutingPolicy {
            revision: id("policy-1"),
            scope: scope.clone(),
            rules: vec![RoutingRule {
                model_binding: id("primary"),
                purpose: ModelPurpose::Agent,
                primary: reference("primary"),
                fallbacks: vec![],
                fallback_on: vec![],
                version_policy: VersionPolicy::RequirePinned,
                min_support: ModelSupportStatus::ContractTested,
            }],
        },
    )
}

struct Model {
    route: ResolvedModelRoute,
    calls: AtomicUsize,
}
impl ModelPort for Model {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: self.route.provider.clone(),
            adapter: self.route.adapter.clone(),
            connection_ref: self.route.connection_ref.clone(),
        }
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        assert_eq!(request.route, self.route);
        assert_eq!(request.tools.len(), 1);
        assert_eq!(request.tools[0].name, id("search"));
        assert_eq!(
            request.tools[0].model_input_schema["properties"],
            json!({"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"default":2}})
        );
        // A concrete protected UUID exists in this execution; its absence is a data-boundary check.
        assert!(!serde_json::to_string(request).unwrap().contains(WORKSPACE));
        let events = match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => vec![
                Ok(ModelEvent::ToolArgumentsDelta {
                    index: 0,
                    provider_call_id: Some("search-alpha".into()),
                    name: Some("search".into()),
                    delta: r#"{"query":"alpha"}"#.into(),
                }),
                Ok(ModelEvent::ToolArgumentsDelta {
                    index: 1,
                    provider_call_id: Some("search-beta".into()),
                    name: Some("search".into()),
                    delta: r#"{"query":"beta","limit":3}"#.into(),
                }),
                Ok(ModelEvent::ResponseCompleted {
                    finish: ModelFinish::ToolCalls,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![],
                }),
            ],
            1 => {
                let calls: Vec<_> = request
                    .messages
                    .iter()
                    .flat_map(|message| &message.content)
                    .filter_map(|content| {
                        if let ModelContent::ToolCall {
                            provider_call_id,
                            arguments,
                            ..
                        } = content
                        {
                            Some((provider_call_id.clone(), arguments.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();
                assert_eq!(
                    calls,
                    vec![
                        (id("search-alpha"), object(json!({"query":"alpha"}))),
                        (id("search-beta"), object(json!({"query":"beta","limit":3})))
                    ]
                );
                let results: Vec<_> = request
                    .messages
                    .iter()
                    .flat_map(|message| &message.content)
                    .filter_map(|content| {
                        if let ModelContent::ToolResult {
                            provider_call_id,
                            content,
                        } = content
                        {
                            Some((provider_call_id.clone(), content.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();
                assert_eq!(results.len(), 2);
                for (index, query) in ["alpha", "beta"].iter().enumerate() {
                    assert_eq!(results[index].0, calls[index].0);
                    assert_eq!(
                        results[index].1,
                        json!({"status":"succeeded","effect":"not_applied","content":[{"type":"json","value":{"query":query,"count":index+2}}]})
                    );
                }
                vec![
                    Ok(ModelEvent::TextDelta {
                        text: "Alpha has 2 results; beta has 3.".into(),
                    }),
                    Ok(ModelEvent::ResponseCompleted {
                        finish: ModelFinish::Stop,
                        metadata: ModelResponseMetadata::default(),
                        continuation: vec![],
                    }),
                ]
            }
            _ => panic!("duplicate start must not invoke the model again"),
        };
        Box::pin(stream::iter(events))
    }
}
struct Search {
    arguments: Mutex<Vec<JsonObject>>,
}
impl ToolExecutor for Search {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        assert_eq!(args.get("workspace_id"), Some(&json!(WORKSPACE)));
        assert_eq!(context.scope.workspace_id, id("workspace"));
        assert_eq!(context.principal_ref, id("actor"));
        let mut arguments = self.arguments.lock().unwrap();
        let index = arguments.len();
        assert_eq!(
            args,
            &object(if index == 0 {
                json!({"query":"alpha","limit":2,"workspace_id":WORKSPACE})
            } else {
                json!({"query":"beta","limit":3,"workspace_id":WORKSPACE})
            })
        );
        arguments.push(args.clone());
        drop(arguments);
        Box::pin(async move {
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: json!({"query":args["query"],"count":args["limit"]}),
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}
fn registry(
    scope: &Scope,
    search: Arc<Search>,
) -> Result<(SystemInputRegistry, ToolRegistry), ContractError> {
    let inputs = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])?;
    let compiled = SchemaCompiler::new().compile(ToolDescriptor { tool: reference("search"), name: id("search"), description: "Search authorized records".into(),
        input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"default":2},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters: vec!["query".into(),"limit".into()], system_bindings: None,
        output_schema: json!({"type":"object","properties":{"query":{"type":"string"},"count":{"type":"integer"}},"required":["query","count"],"additionalProperties":false}),
        side_effect: ToolSideEffect::ReadOnly, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false, max_output_bytes: 1024.try_into().unwrap() }, &inputs)?;
    Ok((
        inputs,
        ToolRegistry::new(
            scope.clone(),
            vec![ToolRegistration {
                compiled,
                executor: search,
            }],
        )?,
    ))
}
struct TemporaryDatabase(std::path::PathBuf);
impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let temporary = TemporaryDatabase(
        std::env::temp_dir().join(format!("wickle-tool-loop-{}", RandomIdSource.next_id()?)),
    );
    std::fs::create_dir(&temporary.0)?;
    let database = temporary.0.join("state.sqlite3");
    let routing = routing(&scope)?;
    let model = Arc::new(Model {
        route: routing.route_for_binding(&reference("primary"))?,
        calls: AtomicUsize::new(0),
    });
    let search = Arc::new(Search {
        arguments: Mutex::new(vec![]),
    });
    let catalog = Arc::new(Catalog {
        calls: AtomicUsize::new(0),
    });
    let policy = Arc::new(PolicyGate::new(Arc::new(Policy), Duration::from_secs(1))?);
    let exchange = Arc::new(
        ModelExchange::new(model.clone(), policy.clone())
            .with_route_inspector(Arc::new(Inspector), Duration::from_secs(1))?,
    );
    let router = Arc::new(PolicyModelRouter::new(routing)?);
    let profile = AgentProfile::from_json(
        r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1",
        "name":"Assistant","description":"Synthetic tool loop consumer","instructions":{"text":"Search then summarize the observations"},
        "model_binding":"primary","tools":[{"tool_id":"search","version":"1"}],"skills":[],"connectors":[],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":4,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}
    }"#,
    )?;
    let make_agent = |store: Arc<SqliteStateStore>| -> Result<Agent, ContractError> {
        let (system_inputs, tools) = registry(&scope, search.clone())?;
        create_agent(
            profile.clone(),
            AgentBindings {
                scope: scope.clone(),
                state: store,
                policy: policy.clone(),
                profile_resolver: catalog.clone(),
                model_exchange: exchange.clone(),
                router: router.clone(),
                host_instructions: vec!["Use only the authorized workspace.".into()],
                system_inputs,
                tools: Some(Arc::new(tools)),
                system_input_resolver: None, external_receipt_verifier: None,
                clock: Arc::new(SystemClock::new()),
                ids: Arc::new(RandomIdSource),
                token_estimator: Arc::new(Estimate),
                settings: AgentSettings {
                    require_durable: true,
                    max_output_tokens: 128.try_into().unwrap(),
                    ..AgentSettings::default()
                },
            },
        )
    };
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id("actor"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: Some(SystemInputs::new(object(json!({"workspace_id":WORKSPACE})))),
        },
        Default::default(),
    );
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Compare alpha and beta".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    };
    let store = Arc::new(SqliteStateStore::open(&database)?);
    let agent = make_agent(store.clone())?;
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert!(search.arguments.lock().unwrap().is_empty());
    let handle = completed(agent.start(request.clone(), context.clone()).await?)?;
    let run_id = handle.run_id().clone();
    let outcome = completed(handle.outcome(&context).await?)?;
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "Alpha has 2 results; beta has 3.".into()
        }]
    );
    assert_eq!(
        (outcome.usage.model_calls, outcome.usage.tool_attempts),
        (2, 2)
    );
    let events: Vec<_> = handle.events(0, context.clone()).try_collect().await?;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "tool.planned")
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "tool.settled")
            .count(),
        2
    );
    assert_eq!(
        events.last().ok_or("missing terminal event")?.event_type,
        "run.finished"
    );
    let saved = store.load(&scope, &run_id).await?;
    assert_eq!(
        saved.snapshot.tool_ledger[0].call.model_inputs,
        object(json!({"query":"alpha"}))
    );
    for entry in &saved.snapshot.tool_ledger {
        assert!(entry.call.bound_input_ref.is_some());
        assert!(
            matches!(&entry.state, ToolCallState::Settled { result } if result.status == ToolResultStatus::Succeeded && result.effect == ToolEffect::NotApplied)
        );
    }
    drop(handle);
    drop(agent);
    drop(store);

    let reopened = Arc::new(SqliteStateStore::open(&database)?);
    let restored = reopened.load(&scope, &run_id).await?;
    assert_eq!(restored.snapshot.outcome, Some(outcome.clone()));
    assert_eq!(restored.snapshot.tool_ledger, saved.snapshot.tool_ledger);
    assert!(restored.session.active_run_id.is_none());
    let resolver_calls = catalog.calls.load(Ordering::SeqCst);
    let replay_agent = make_agent(reopened)?;
    let replay = completed(replay_agent.start(request, context.clone()).await?)?;
    assert_eq!(replay.run_id(), &run_id);
    assert_eq!(completed(replay.outcome(&context).await?)?, outcome);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(search.arguments.lock().unwrap().len(), 2);
    assert_eq!(catalog.calls.load(Ordering::SeqCst), resolver_calls);
    println!(
        "tool loop consumer: two serial calls with system UUID binding and model-only arguments; final model response; SQLite reopen and request replay without additional model, tool, or resolver calls"
    );
    Ok(())
}
```
