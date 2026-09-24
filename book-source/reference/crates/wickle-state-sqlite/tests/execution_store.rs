//! Same atomic execution contract through real SQLite transactions and reopen.
use std::sync::Arc;
use wickle::*;
use wickle_state_sqlite::SqliteStateStore;
#[allow(dead_code)]
#[path = "../../wickle/tests/support/mod.rs"]
mod core;
use core::*;
#[path = "../../wickle/tests/support/execution_store.rs"]
mod suite;
#[allow(dead_code)]
mod support;
#[tokio::test]
async fn sqlite_execution_transactions_survive_reopen() {
    let database = support::Database::new();
    let store = Arc::new(SqliteStateStore::open(database.path()).unwrap());
    suite::atomic_execution_contract(store.clone()).await;
    let expected = store
        .read_execution(&scope(), &id("atomic-run"))
        .await
        .unwrap();
    drop(store);
    let reopened = SqliteStateStore::open(database.path()).unwrap();
    assert_eq!(
        reopened
            .read_execution(&scope(), &id("atomic-run"))
            .await
            .unwrap(),
        expected
    );
}
#[tokio::test]
async fn sqlite_recovery_rolls_back_and_replays_the_same_segment() {
    let database = support::Database::new();
    let store = Arc::new(SqliteStateStore::open(database.path()).unwrap());
    suite::atomic_recovery_contract(store.clone()).await;
    let history = store
        .read_execution(&scope(), &id("recover-run"))
        .await
        .unwrap();
    drop(store);
    assert_eq!(
        SqliteStateStore::open(database.path())
            .unwrap()
            .read_execution(&scope(), &id("recover-run"))
            .await
            .unwrap(),
        history
    );
}

#[tokio::test]
async fn legacy_terminal_rows_are_read_without_rewrite_then_upgrade_on_new_admission() {
    let database = support::Database::new();
    let store = SqliteStateStore::open(database.path()).unwrap();
    let first = store
        .admit(
            &scope(),
            admission("legacy", "legacy-request", "legacy-session", "input", "1").await,
        )
        .await
        .unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("legacy"), &id("owner"), 0, 10000)
        .await
        .unwrap();
    let terminal = store
        .commit(
            &scope(),
            &id("legacy"),
            finished(&first.state.snapshot, lease, 1),
        )
        .await
        .unwrap();
    drop(store);
    let conn = rusqlite::Connection::open(database.path()).unwrap();
    let raw: String = conn
        .query_row(
            "SELECT checkpoint_json FROM wickle_scope_checkpoints",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut legacy: serde_json::Value = serde_json::from_str(&raw).unwrap();
    legacy["schema_version"] = serde_json::json!("wickle.state-store.v1");
    legacy.as_object_mut().unwrap().remove("executions");
    conn.execute(
        "UPDATE wickle_scope_checkpoints SET checkpoint_json=?1,checksum=?2",
        rusqlite::params![legacy.to_string(), canonical_digest(&legacy).as_str()],
    )
    .unwrap();
    let store = SqliteStateStore::open(database.path()).unwrap();
    assert_eq!(store.load(&scope(), &id("legacy")).await.unwrap(), terminal);
    let after_read: String = conn
        .query_row(
            "SELECT checkpoint_json FROM wickle_scope_checkpoints",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(after_read, legacy.to_string());
    store
        .admit(
            &scope(),
            admission("new", "new-request", "new-session", "input", "1").await,
        )
        .await
        .unwrap();
    let after_write: String = conn
        .query_row(
            "SELECT checkpoint_json FROM wickle_scope_checkpoints",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&after_write).unwrap()["schema_version"],
        "wickle.state-store.v2"
    );
    assert_eq!(store.load(&scope(), &id("legacy")).await.unwrap(), terminal);
    assert!(store.read_execution(&scope(), &id("legacy")).await.is_err());
    assert!(store.read_execution(&scope(), &id("new")).await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_conflicting_submissions_keep_only_the_winning_sqlite_snapshot() {
    let database = support::Database::new();
    suite::conflicting_submissions_race([
        Arc::new(SqliteStateStore::open(database.path()).unwrap()),
        Arc::new(SqliteStateStore::open(database.path()).unwrap()),
    ])
    .await;
    let reopened = SqliteStateStore::open(database.path()).unwrap();
    let saved = reopened
        .find_request(&scope(), &id("race-session"), &id("same-key"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        reopened
            .load_session(&scope(), &id("race-session"))
            .await
            .unwrap()
            .active_run_id,
        Some(saved.snapshot.run_id)
    );
}
