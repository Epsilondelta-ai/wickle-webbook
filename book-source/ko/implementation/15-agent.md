# 15장 전체 Rust 구현과 테스트

[강의로](../15-agent.md) · [전체 변경 패치](../solutions/15-agent.patch)

기준 `0d9b104a078e144b78dc3dc5cdd57a06135c982a`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle-state-sqlite/src/lib.rs`

```rust
//! Durable SQLite storage for Wickle's validated state-store checkpoints.
//!
//! Each operation uses a database transaction and delegates state transitions to
//! the core memory store. Asynchronous operations run on Tokio's blocking pool.
//! Dropping a waiting future does not roll back an already running transaction.

#![forbid(unsafe_code)]

use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use std::{
    collections::BTreeMap,
    fmt,
    fs::OpenOptions,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tokio::runtime::Handle;
use wickle::{
    AdmissionInput, AdmissionResult, CommitInput, ContractError, ErrorCode, EventPage, Id,
    IdSource, JsonDigest, MemoryStateStore, PortFuture, ProtectedRecord, RandomIdSource, RecordRef,
    RunLease, Scope, SessionSnapshot, StateStore, StateStoreCapabilities, StateStoreCheckpoint,
    StoredRun,
};

const APPLICATION_ID: i64 = 0x574B4C45;
const SCHEMA_VERSION: i64 = 1;
const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_BUSY_TIMEOUT: Duration = Duration::from_secs(30);
const MIN_SQLITE_VERSION: i32 = 3_053_002;
const METADATA_SCHEMA: &str = "CREATE TABLE wickle_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL,
    store_id TEXT NOT NULL
) STRICT";
const CHECKPOINT_SCHEMA: &str = "CREATE TABLE wickle_scope_checkpoints (
    scope_key TEXT PRIMARY KEY NOT NULL,
    checkpoint_json TEXT NOT NULL,
    checksum TEXT NOT NULL
) STRICT";

/// SQLite-backed checkpoints for an explicitly selected disk file.
///
/// Every connection uses WAL and synchronous=FULL. Writes serialize through
/// BEGIN IMMEDIATE, then reuse the core's admission, revision and fencing checks.
/// Reads use a consistent deferred transaction. A successful write is returned
/// only after SQLite commits. The Host must provide a filesystem suitable for
/// SQLite WAL and retain the database file and its related WAL state.
/// Lease operations advance the supplied logical time by monotonic time spent
/// waiting for the blocking pool, database lock and checkpoint reconstruction.
///
/// Operations use Tokio's blocking pool. Cancelling or dropping their waiting
/// future can lose the acknowledgement of a committed operation; it does not
/// guarantee rollback. Recover by reading state or replaying the same request.
#[derive(Clone)]
pub struct SqliteStateStore {
    path: PathBuf,
    busy_timeout: Duration,
    store_id: String,
}

impl fmt::Debug for SqliteStateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteStateStore")
            .field("busy_timeout", &self.busy_timeout)
            .finish_non_exhaustive()
    }
}

impl SqliteStateStore {
    /// Open or initialize a dedicated disk database with a five-second busy timeout.
    ///
    /// This is synchronous initialization; call it before starting asynchronous
    /// work, or place it in the Host's blocking task. No runtime is created.
    /// New files use owner-only permissions on Unix. Existing permissions are
    /// retained. Empty paths, SQLite URI paths and :memory: are rejected.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ContractError> {
        Self::open_with_busy_timeout(path, DEFAULT_BUSY_TIMEOUT)
    }

    /// Open with an explicit bounded SQLite lock wait, from zero to thirty seconds.
    /// Zero requests immediate failure on lock contention. SQLite measures this
    /// setting in milliseconds. Other database and filesystem work is not timed by
    /// this option, and queued/running blocking tasks can outlive their waiter.
    pub fn open_with_busy_timeout(
        path: impl AsRef<Path>,
        busy_timeout: Duration,
    ) -> Result<Self, ContractError> {
        if busy_timeout > MAX_BUSY_TIMEOUT {
            return Err(error(ErrorCode::InvalidContract, "sqlite.busy_timeout"));
        }
        check_engine()?;
        let path = prepare_file(path.as_ref())?;
        let mut connection = connect(&path, busy_timeout)?;
        // Inspect before changing journal settings in an unrelated database.
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(storage_error)?;
        database_identity(&transaction)?;
        transaction.commit().map_err(storage_error)?;
        let mode: String = connection
            .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
            .map_err(storage_error)?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(error(
                ErrorCode::PersistenceUnavailable,
                "sqlite.journal_mode",
            ));
        }
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(storage_error)?;
        check_durability(&connection)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let store_id = match database_identity(&transaction)? {
            Some(identity) => identity,
            None => {
                let store_id = RandomIdSource.next_id()?.to_string();
                transaction
                    .execute_batch(&format!("{METADATA_SCHEMA}; {CHECKPOINT_SCHEMA};"))
                    .map_err(storage_error)?;
                transaction.execute("INSERT INTO wickle_metadata(singleton,schema_version,store_id) VALUES (1,?1,?2)",params![SCHEMA_VERSION,store_id]).map_err(storage_error)?;
                transaction
                    .pragma_update(None, "application_id", APPLICATION_ID)
                    .map_err(storage_error)?;
                transaction
                    .pragma_update(None, "user_version", SCHEMA_VERSION)
                    .map_err(storage_error)?;
                store_id
            }
        };
        transaction.commit().map_err(storage_error)?;
        check_durability(&connection)?;
        Ok(Self {
            path,
            busy_timeout,
            store_id,
        })
    }

    fn transact<'a, T, F>(
        &'a self,
        scope: &'a Scope,
        write: bool,
        operation: F,
    ) -> PortFuture<'a, T>
    where
        T: Send + 'static,
        F: FnOnce(&MemoryStateStore, &Scope, &Handle) -> Result<T, ContractError> + Send + 'static,
    {
        let store = self.clone();
        let scope = scope.clone();
        Box::pin(async move {
            let runtime = Handle::try_current()
                .map_err(|_| error(ErrorCode::RuntimeUnavailable, "sqlite.runtime"))?;
            tokio::task::spawn_blocking(move || {
                store.transaction(&scope, write, &runtime, operation)
            })
            .await
            .map_err(|_| error(ErrorCode::PersistenceUnavailable, "sqlite.worker"))?
        })
    }

    fn transaction<T, F>(
        &self,
        scope: &Scope,
        write: bool,
        runtime: &Handle,
        operation: F,
    ) -> Result<T, ContractError>
    where
        F: FnOnce(&MemoryStateStore, &Scope, &Handle) -> Result<T, ContractError>,
    {
        // No CREATE flag and no initializer here: a missing/replaced file is not
        // silently converted into a new store during an operation.
        let mut connection = connect(&self.path, self.busy_timeout)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(storage_error)?;
        check_durability(&connection)?;
        let behavior = if write {
            TransactionBehavior::Immediate
        } else {
            TransactionBehavior::Deferred
        };
        let transaction = connection
            .transaction_with_behavior(behavior)
            .map_err(storage_error)?;
        if database_identity(&transaction)?.as_deref() != Some(self.store_id.as_str()) {
            return Err(error(ErrorCode::InvalidSnapshot, "sqlite.store_identity"));
        }
        let key = scope_key(scope)?;
        let row: Option<(String, String)> = transaction
            .query_row(
                "SELECT checkpoint_json,checksum FROM wickle_scope_checkpoints WHERE scope_key=?1",
                params![key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(storage_error)?;
        let state = match row {
            None => MemoryStateStore::new(),
            Some((json, checksum)) => {
                let digest = JsonDigest::try_from(checksum)
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "sqlite.checksum"))?;
                let checkpoint = StateStoreCheckpoint::from_json(&json, scope, &digest)?;
                MemoryStateStore::from_checkpoint(checkpoint)
            }
        };
        let result = operation(&state, scope, runtime)?;
        if write {
            let checkpoint = state.export_checkpoint(scope)?;
            let json = serde_json::to_string(&checkpoint).map_err(storage_error)?;
            let digest = checkpoint.digest();
            transaction.execute(
                "INSERT INTO wickle_scope_checkpoints(scope_key,checkpoint_json,checksum) VALUES (?1,?2,?3)
                 ON CONFLICT(scope_key) DO UPDATE SET checkpoint_json=excluded.checkpoint_json,checksum=excluded.checksum",
                params![key,json,digest.as_str()],
            ).map_err(storage_error)?;
        }
        transaction.commit().map_err(storage_error)?;
        Ok(result)
    }
}

impl StateStore for SqliteStateStore {
    fn find_request<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
        request_id: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        let session_id = session_id.clone();
        let request_id = request_id.clone();
        self.transact(scope, false, move |state, scope, runtime| {
            runtime.block_on(state.find_request(scope, &session_id, &request_id))
        })
    }

    fn capabilities(&self) -> StateStoreCapabilities {
        StateStoreCapabilities {
            durable: true,
            cross_process_leases: true,
            event_replay: true,
        }
    }
    fn admit<'a>(
        &'a self,
        scope: &'a Scope,
        mut input: AdmissionInput,
    ) -> PortFuture<'a, AdmissionResult> {
        // The outer SQLite transaction supplies durability. The core memory
        // implementation still enforces every other admission invariant.
        input.require_durable = false;
        self.transact(scope, true, move |state, scope, runtime| {
            runtime.block_on(state.admit(scope, input))
        })
    }
    fn load<'a>(&'a self, scope: &'a Scope, run_id: &'a Id) -> PortFuture<'a, StoredRun> {
        let run_id = run_id.clone();
        self.transact(scope, false, move |state, scope, runtime| {
            runtime.block_on(state.load(scope, &run_id))
        })
    }
    fn load_session<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
    ) -> PortFuture<'a, SessionSnapshot> {
        let session_id = session_id.clone();
        self.transact(scope, false, move |state, scope, runtime| {
            runtime.block_on(state.load_session(scope, &session_id))
        })
    }
    fn check_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, RunLease> {
        let entered = Instant::now();
        let run_id = run_id.clone();
        let lease = lease.clone();
        self.transact(scope, false, move |state, scope, runtime| {
            runtime.block_on(state.check_lease(
                scope,
                &run_id,
                &lease,
                effective_now(now_ms, entered)?,
            ))
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
        let entered = Instant::now();
        let run_id = run_id.clone();
        let owner = owner.clone();
        self.transact(scope, true, move |state, scope, runtime| {
            runtime.block_on(state.acquire_lease(
                scope,
                &run_id,
                &owner,
                effective_now(now_ms, entered)?,
                ttl_ms,
            ))
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
        let entered = Instant::now();
        let run_id = run_id.clone();
        let lease = lease.clone();
        self.transact(scope, true, move |state, scope, runtime| {
            runtime.block_on(state.renew_lease(
                scope,
                &run_id,
                &lease,
                effective_now(now_ms, entered)?,
                ttl_ms,
            ))
        })
    }
    fn release_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, ()> {
        let entered = Instant::now();
        let run_id = run_id.clone();
        let lease = lease.clone();
        self.transact(scope, true, move |state, scope, runtime| {
            runtime.block_on(state.release_lease(
                scope,
                &run_id,
                &lease,
                effective_now(now_ms, entered)?,
            ))
        })
    }
    fn commit<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        mut input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        let entered = Instant::now();
        let run_id = run_id.clone();
        self.transact(scope, true, move |state, scope, runtime| {
            input.now_ms = effective_now(input.now_ms, entered)?;
            runtime.block_on(state.commit(scope, &run_id, input))
        })
    }
    fn read_events<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        after_seq: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage> {
        let run_id = run_id.clone();
        self.transact(scope, false, move |state, scope, runtime| {
            runtime.block_on(state.read_events(scope, &run_id, after_seq, limit))
        })
    }
    fn read_record<'a>(
        &'a self,
        scope: &'a Scope,
        reference: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        let reference = reference.clone();
        self.transact(scope, false, move |state, scope, runtime| {
            runtime.block_on(state.read_record(scope, &reference))
        })
    }
}

fn check_engine() -> Result<(), ContractError> {
    if rusqlite::version_number() < MIN_SQLITE_VERSION {
        return Err(error(
            ErrorCode::UnsupportedContractVersion,
            "sqlite.engine_version",
        ));
    }
    Ok(())
}

fn prepare_file(path: &Path) -> Result<PathBuf, ContractError> {
    if path.as_os_str().is_empty()
        || path == Path::new(":memory:")
        || path.to_string_lossy().starts_with("file:")
    {
        return Err(error(ErrorCode::InvalidContract, "sqlite.path"));
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(file) => drop(file),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(storage_error(error)),
    }
    let path = std::fs::canonicalize(path).map_err(storage_error)?;
    if !std::fs::metadata(&path).map_err(storage_error)?.is_file() {
        return Err(error(ErrorCode::InvalidContract, "sqlite.path"));
    }
    Ok(path)
}

fn connect(path: &Path, busy_timeout: Duration) -> Result<Connection, ContractError> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(storage_error)?;
    connection
        .busy_timeout(busy_timeout)
        .map_err(storage_error)?;
    Ok(connection)
}

fn check_durability(connection: &Connection) -> Result<(), ContractError> {
    let mode: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .map_err(storage_error)?;
    let synchronous: i64 = connection
        .pragma_query_value(None, "synchronous", |row| row.get(0))
        .map_err(storage_error)?;
    if !mode.eq_ignore_ascii_case("wal") || synchronous != 2 {
        return Err(error(
            ErrorCode::PersistenceUnavailable,
            "sqlite.durability",
        ));
    }
    Ok(())
}

fn database_identity(connection: &Connection) -> Result<Option<String>, ContractError> {
    let application: i64 = connection
        .pragma_query_value(None, "application_id", |row| row.get(0))
        .map_err(storage_error)?;
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(storage_error)?;
    let mut statement = connection
        .prepare(
            "SELECT type,name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY type,name",
        )
        .map_err(storage_error)?;
    let objects = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                normalized_schema(&row.get::<_, String>(2)?),
            ))
        })
        .map_err(storage_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(storage_error)?;
    if application == 0 && version == 0 && objects.is_empty() {
        return Ok(None);
    }
    if application != APPLICATION_ID {
        return Err(error(ErrorCode::InvalidContract, "sqlite.application_id"));
    }
    if version != SCHEMA_VERSION {
        return Err(error(
            ErrorCode::UnsupportedSchemaVersion,
            "sqlite.schema_version",
        ));
    }
    let expected = vec![
        (
            "table".to_owned(),
            "wickle_metadata".to_owned(),
            normalized_schema(METADATA_SCHEMA),
        ),
        (
            "table".to_owned(),
            "wickle_scope_checkpoints".to_owned(),
            normalized_schema(CHECKPOINT_SCHEMA),
        ),
    ];
    if objects != expected {
        return Err(error(ErrorCode::UnsupportedSchemaVersion, "sqlite.schema"));
    }
    let mut statement = connection
        .prepare("SELECT singleton,schema_version,store_id FROM wickle_metadata")
        .map_err(storage_error)?;
    let metadata = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(storage_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(storage_error)?;
    if metadata.len() != 1 || metadata[0].0 != 1 {
        return Err(error(ErrorCode::InvalidSnapshot, "sqlite.metadata"));
    }
    if metadata[0].1 != SCHEMA_VERSION {
        return Err(error(
            ErrorCode::UnsupportedSchemaVersion,
            "sqlite.schema_version",
        ));
    }
    let store_id = &metadata[0].2;
    if store_id.len() != 32 || !store_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(error(ErrorCode::InvalidSnapshot, "sqlite.store_identity"));
    }
    Ok(Some(store_id.clone()))
}

fn scope_key(scope: &Scope) -> Result<String, ContractError> {
    let ordered: BTreeMap<String, serde_json::Value> =
        serde_json::from_value(serde_json::to_value(scope).map_err(storage_error)?)
            .map_err(storage_error)?;
    serde_json::to_string(&ordered).map_err(storage_error)
}
fn normalized_schema(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_ascii_whitespace())
        .map(|character| character.to_ascii_lowercase())
        .collect()
}
fn effective_now(now_ms: i64, entered: Instant) -> Result<i64, ContractError> {
    i64::try_from(entered.elapsed().as_millis())
        .ok()
        .and_then(|elapsed| now_ms.checked_add(elapsed))
        .ok_or_else(|| error(ErrorCode::ClockUnavailable, "sqlite.monotonic_time"))
}
fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
fn storage_error<E>(_: E) -> ContractError {
    error(ErrorCode::PersistenceUnavailable, "sqlite.storage")
}
```

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
    let wait = WaitState {
        wait_id: id("wait"),
        target: WaitTarget::Input {
            request: InputRequest {
                input_request_id: id("question"),
                call_id: id("input-call"),
                question: "Choose input".into(),
                schema_ref: None,
            },
        },
        expires_at_ms: None,
    };
    let record = ProtectedRecord::new(id("wait-record"), 1, serde_json::to_value(&wait).unwrap());
    let mut update = prepared(&saved.snapshot, lease.clone(), 1);
    update.snapshot.status = RunStatus::Waiting;
    update.snapshot.phase = RunPhase::Waiting;
    update.snapshot.wait = Some(wait);
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
        action: ResumeAction::Input {
            wait_id: id("wait"),
            answer: json!("selected"),
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
    cancel: CancellationToken,
    reason: Mutex<Option<Id>>,
    error: Mutex<Option<ContractError>>,
    done: AtomicBool,
    notify: Notify,
}
impl LocalRun {
    fn new() -> Self {
        Self {
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
/// Tools, asset loaders, verifiers and extension execution require later runtime bindings.
pub fn create_agent(
    profile: AgentProfile,
    bindings: AgentBindings,
) -> Result<Agent, ContractError> {
    profile.validate_structure()?;
    bindings.settings.validate()?;
    if !matches!(profile.instructions, Instructions::Text(_))
        || !matches!(profile.output_contract, OutputContract::Text {})
        || !matches!(profile.completion_policy, CompletionPolicy::TurnEnd {})
        || !profile.tools.is_empty()
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
    /// Command consumption is unsupported until the explicit waiting/resume runtime is connected.
    pub async fn resume(
        &self,
        _command: ResumeCommand,
        context: ExecutionContext,
    ) -> Result<Guarded<RunHandle>, ContractError> {
        self.check_scope(&context)?;
        Err(fail(ErrorCode::CapabilityUnsupported, "agent.resume"))
    }
    fn check_scope(&self, context: &ExecutionContext) -> Result<(), ContractError> {
        if context.data.scope != self.inner.bindings.scope {
            return Err(fail(ErrorCode::AccessDenied, "scope"));
        }
        Ok(())
    }
    fn handle(&self, run_id: Id) -> Result<RunHandle, ContractError> {
        let local = self
            .inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .get(&run_id)
            .cloned();
        Ok(RunHandle {
            agent: self.clone(),
            run_id,
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
                    if saved.snapshot.status.is_terminal() {
                        if cursor >= saved.snapshot.last_event_seq {
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
                if let Some(local) = self.current_local()? {
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
            .cloned())
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
            return Ok(Guarded::Completed(self.handle(saved.snapshot.run_id)?));
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
            return Ok(Guarded::Completed(
                self.handle(result.state.snapshot.run_id)?,
            ));
        }
        let run_id = result.state.snapshot.run_id;
        let local = Arc::new(LocalRun::new());
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
                    runs.remove(&driver_id);
                }
            }
        });
        Ok(Guarded::Completed(RunHandle {
            agent: self.clone(),
            run_id,
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
                vec![],
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
        let result =
            AssertUnwindSafe(self.run_segment(run_id, prompt, &context, &budget, &lease, local))
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
        let attempt = self.generate(run_id, prompt, context, budget, lease).await;
        if let Some(error) = local
            .error
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .clone()
        {
            return Err(error);
        }
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
            },
            budget,
            lease,
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
                required_capabilities: std::collections::BTreeSet::from([Id::new("text")?]),
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
        lease: &RunLease,
        local: &Arc<LocalRun>,
    ) -> Result<(), ContractError> {
        let PreparedOutcome {
            mut result,
            mut output,
            continuation,
        } = candidate;
        let bindings = &self.inner.bindings;
        let saved = bindings.state.load(&bindings.scope, run_id).await?;
        let mut snapshot = saved.snapshot;
        if snapshot.status.is_terminal() {
            return Ok(());
        }
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        bindings
            .state
            .check_lease(&bindings.scope, run_id, lease, now)
            .await?;
        if local.cancel.is_cancelled() && matches!(result, OutcomeResult::Succeeded { .. }) {
            result = OutcomeResult::Cancelled {
                reason: local
                    .reason
                    .lock()
                    .map_err(|_| fail(ErrorCode::InvalidContract, "agent.cancel"))?
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "cancelled".into()),
            };
            output.clear();
        }
        if elapsed >= snapshot.limits.max_elapsed_ms.get()
            && matches!(result, OutcomeResult::Succeeded { .. })
        {
            result = OutcomeResult::Exhausted {
                budget: BudgetKind::Elapsed,
            };
            output.clear();
        }
        if output.is_empty() && !matches!(result, OutcomeResult::Succeeded { .. }) {
            output = self.saved_partial_output(&snapshot).await?;
        }
        // Protected response reads may have waited. Refresh settlement time and
        // the stored lease before the final transaction.
        let (_, check_at) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let current_lease = bindings
            .state
            .check_lease(&bindings.scope, run_id, lease, check_at)
            .await?;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        if now >= current_lease.expires_at_ms {
            return Err(fail(ErrorCode::LeaseLost, "agent.finish"));
        }
        if matches!(result, OutcomeResult::Succeeded { .. }) {
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
            } else if elapsed >= snapshot.limits.max_elapsed_ms.get() {
                result = OutcomeResult::Exhausted {
                    budget: BudgetKind::Elapsed,
                };
            }
        }
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
        snapshot.phase = RunPhase::Finish;
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
            unresolved_effects: vec![],
        };
        let record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(&outcome)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.outcome"))?,
        );
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
            payload: RunEventPayload::RunFinished {
                outcome_ref: record.reference().clone(),
            },
        };
        let mut records = vec![record];
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

## `crates/wickle/src/lib.rs`

```rust
//! Wickle, an agent engine for Rust applications.
//!
//! Profiles, scoped metadata resolution, and versioned execution data contracts.
//! Model calls use scoped ports and persisted attempt accounting. Tool dispatch
//! and the agent driver are not implemented yet.
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
    PolicyPort, PolicyRequest, ToolPolicyInput,
};
pub use state::{
    AdmissionInput, AdmissionResult, CommitInput, EventPage, MAX_EVENT_PAGE_SIZE, MemoryStateStore,
    ProtectedRecord, RunLease, STATE_STORE_CHECKPOINT_VERSION, StateStore, StateStoreCapabilities,
    StateStoreCheckpoint, StoredRun,
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
    ResumeCommand, RunEvent, RunEventPayload, RunEventSchemaVersion, RunOutcome, RunPhase,
    RunRequest, RunSnapshot, RunSnapshotSchemaVersion, RunStatus, RunTrigger, SessionSchemaVersion,
    SessionSnapshot, SourceExecutionState, SystemInputSnapshotRef, ToolCallState, ToolLedgerEntry,
    VerificationSummary, VerificationVerdict, WaitState, WaitTarget, admission_digest,
};
pub use serialization::{
    Id, JsonDigest, JsonObject, canonical_digest, canonical_digest_json, parse_json,
};
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
            validate_events(state, &additions, &input.snapshot, 0, &input.events, true)?;
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
            let additions = validate_records(state, &input.records)?;
            validate_snapshot_refs(state, &additions, &input.snapshot)?;
            validate_events(
                state,
                &additions,
                &input.snapshot,
                run.snapshot.last_event_seq,
                &input.events,
                false,
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
            let references = match content {
                ContentBlock::Content { .. } => Vec::new(),
                ContentBlock::ToolCall { call } => call.bound_input_ref.iter().collect(),
                ContentBlock::ToolResult { result } => tool_result_refs(result),
                ContentBlock::ProviderOpaque { data_ref, .. } => vec![data_ref],
            };
            for reference in references {
                record_value(state, additions, reference)?;
            }
        }
    }
    Ok(sequence)
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

fn validate_events(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
    previous_seq: u64,
    events: &[RunEvent],
    admission: bool,
) -> Result<(), ContractError> {
    let mut sequence = previous_seq;
    let mut seen = BTreeSet::new();
    let mut started = 0;
    let mut finished = 0;
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
                let command: ResumeCommand = event_record(state, additions, command_ref)?;
                let previous = state.runs.get(&snapshot.run_id).ok_or_else(not_found)?;
                if snapshot.status != RunStatus::Running
                    || command.run_id != snapshot.run_id
                    || command.expected_revision != previous.snapshot.revision
                    || !resume_target_matches(&previous.snapshot, &command.action)
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.run_resumed"));
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
    {
        return Err(error(ErrorCode::InvalidEvent, "events"));
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

fn resume_target_matches(previous: &RunSnapshot, action: &ResumeAction) -> bool {
    match action {
        ResumeAction::Recover { .. } => previous.status == RunStatus::Running,
        ResumeAction::Approve { wait_id, target }
        | ResumeAction::Deny {
            wait_id, target, ..
        } => previous.wait.as_ref().is_some_and(|wait| {
            &wait.wait_id == wait_id
                && matches!(&wait.target, WaitTarget::Approval { target: saved } if saved == target)
        }),
        ResumeAction::Input { wait_id, .. } => previous.wait.as_ref().is_some_and(|wait| {
            &wait.wait_id == wait_id && matches!(wait.target, WaitTarget::Input { .. })
        }),
        ResumeAction::External { wait_id, .. } => previous.wait.as_ref().is_some_and(|wait| {
            &wait.wait_id == wait_id && matches!(wait.target, WaitTarget::External { .. })
        }),
    }
}

fn validate_transition(previous: &RunSnapshot, next: &RunSnapshot) -> Result<(), ContractError> {
    if previous.status.is_terminal() {
        return Err(error(ErrorCode::InvalidTransition, "run.status"));
    }
    next.validate()?;
    crate::budget::validate_budget_transition(previous, next)?;
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
                ToolCallState::Dispatching { .. } | ToolCallState::Unknown { .. }
            ) && matches!(new.state, ToolCallState::Planned { .. }))
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
            },
            ToolCallState::Dispatching {
                attempt_id: new_attempt,
                idempotency_key: new_key,
            }
            | ToolCallState::Unknown {
                attempt_id: new_attempt,
                idempotency_key: new_key,
            },
        ) = (&old.state, &new.state)
        {
            let retry = matches!(old.state, ToolCallState::Unknown { .. })
                && matches!(new.state, ToolCallState::Dispatching { .. });
            if old_key != new_key || (!retry && old_attempt != new_attempt) {
                return Err(error(ErrorCode::InvalidTransition, "tool_ledger.attempt"));
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

## `crates/wickle/tests/agent_runtime.rs`

```rust
//! Agent lifecycle, saved outcomes, request identity, and detached execution.

#[path = "support/agent.rs"]
#[allow(dead_code)]
mod support;
use futures_util::StreamExt;
use std::sync::atomic::Ordering;
use support::*;
use wickle::*;

#[test]
fn starting_without_a_tokio_runtime_returns_a_typed_error_before_callbacks() {
    use std::{future::Future, task::Context};
    let fixture = Fixture::new(Response::Text, false);
    let agent = fixture.agent();
    let future = agent.start(request("request"), context());
    let mut future = std::pin::pin!(future);
    let waker = futures_util::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let result = future.as_mut().poll(&mut context);
    assert!(matches!(
        result,
        std::task::Poll::Ready(Err(ContractError {
            code: ErrorCode::RuntimeUnavailable,
            ..
        }))
    ));
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropping_a_polled_start_future_does_not_abort_its_owned_admission_or_driver() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    {
        let start = agent.start(request("request"), context());
        tokio::pin!(start);
        assert!(futures_util::poll!(start.as_mut()).is_pending());
    }
    fixture.model.entered.notified().await;
    let saved = fixture
        .store
        .find_request(&scope(), &id("session"), &id("request"))
        .await
        .unwrap()
        .unwrap();
    fixture.model.release.add_permits(1);
    let handle = fixture.started(&agent, "request").await;
    assert_eq!(handle.run_id(), &saved.snapshot.run_id);
    assert_eq!(
        completed(handle.outcome(&context()).await.unwrap())
            .result
            .status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn construction_does_not_resolve_metadata_authorize_generate_estimate_or_allocate_ids() {
    let fixture = Fixture::new(Response::Text, false);
    let _agent = fixture.agent();
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.policy.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.router.queries.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.router.snapshots.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.inspector.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.estimator.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.ids.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_text_turn_finishes_with_the_stored_outcome_as_authority() {
    let fixture = Fixture::new(Response::Text, false);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "candidate answer".into()
        }]
    );
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert_eq!(saved.snapshot.outcome, Some(outcome.clone()));
    assert_eq!(saved.snapshot.revision, outcome.checkpoint_revision);
    assert_eq!(saved.session.active_run_id, None);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let events = fixture
        .store
        .read_events(&scope(), handle.run_id(), 0, 100)
        .await
        .unwrap();
    assert!(matches!(
        events.events.last().unwrap().payload,
        RunEventPayload::RunFinished { .. }
    ));
    let view = completed(agent.get_run(handle.run_id(), &context()).await.unwrap());
    assert_eq!(view.status, RunStatus::Succeeded);
}

#[tokio::test]
async fn dropping_the_handle_outcome_waiter_and_event_stream_does_not_cancel_the_driver() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let run_id = handle.run_id().clone();
    {
        let context = context();
        let outcome = handle.outcome(&context);
        tokio::pin!(outcome);
        assert!(futures_util::poll!(outcome.as_mut()).is_pending());
    }
    {
        let mut events = handle.events(0, context());
        let first = events.next().await.unwrap().unwrap();
        assert_eq!(first.run_id, run_id);
    }
    drop(handle);
    fixture.model.release.add_permits(1);
    let replay = completed(agent.start(request("request"), context()).await.unwrap());
    let outcome = completed(replay.outcome(&context()).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn duplicate_requests_reuse_the_run_before_new_metadata_resolution_and_changed_options_conflict()
 {
    let fixture = Fixture::new(Response::Text, false);
    let agent = fixture.agent();
    let first = fixture.started(&agent, "request").await;
    completed(first.outcome(&context()).await.unwrap());
    let resolutions = fixture.catalog.calls.load(Ordering::SeqCst);
    fixture.catalog.revision.store(99, Ordering::SeqCst);
    let duplicate = fixture.started(&agent, "request").await;
    assert_eq!(duplicate.run_id(), first.run_id());
    completed(duplicate.outcome(&context()).await.unwrap());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), resolutions);
    let mut changed = request("request");
    changed
        .model_options
        .insert("effort".into(), serde_json::json!("high"));
    assert_eq!(
        agent.start(changed, context()).await.unwrap_err().code,
        ErrorCode::RequestConflict
    );
}

#[tokio::test]
async fn a_second_request_is_busy_until_the_active_run_finishes() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let first = fixture.started(&agent, "first").await;
    fixture.model.entered.notified().await;
    assert_eq!(
        agent
            .start(request("second"), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::SessionBusy
    );
    fixture.model.release.add_permits(1);
    completed(first.outcome(&context()).await.unwrap());
    let original = fixture
        .store
        .load(&scope(), first.run_id())
        .await
        .unwrap()
        .session
        .prompt_snapshot;
    let second = fixture.started(&agent, "second").await;
    fixture.model.release.add_permits(1);
    completed(second.outcome(&context()).await.unwrap());
    assert_ne!(first.run_id(), second.run_id());
    assert_eq!(
        fixture
            .store
            .load(&scope(), second.run_id())
            .await
            .unwrap()
            .session
            .prompt_snapshot,
        original
    );
    let requests = fixture.model.requests.lock().unwrap();
    let systems = |request: &ModelRequest| {
        request
            .messages
            .iter()
            .filter(|message| message.role == ModelRole::System)
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(systems(&requests[0]), systems(&requests[1]));
}

#[tokio::test]
async fn foreign_scope_and_current_read_or_cancel_denials_do_not_control_an_existing_run() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let mut foreign = context();
    foreign.data.scope.tenant_id = id("foreign");
    assert!(agent.get_run(handle.run_id(), &foreign).await.is_err());
    assert!(handle.cancel(id("cancel"), &foreign).await.is_err());
    fixture.policy.deny.store(1, Ordering::SeqCst);
    assert!(agent.get_run(handle.run_id(), &context()).await.is_err());
    assert!(handle.outcome(&context()).await.is_err());
    fixture.policy.deny.store(2, Ordering::SeqCst);
    assert!(handle.cancel(id("cancel"), &context()).await.is_err());
    fixture.policy.deny.store(0, Ordering::SeqCst);
    fixture.model.release.add_permits(1);
    assert_eq!(
        completed(handle.outcome(&context()).await.unwrap())
            .result
            .status(),
        RunStatus::Succeeded
    );
}

#[tokio::test]
async fn cancelling_a_running_request_preserves_its_reserved_attempt_and_saves_cancellation() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let receipt = completed(
        handle
            .cancel(id("user_cancelled"), &context())
            .await
            .unwrap(),
    );
    assert_eq!(receipt, CancelReceipt::Requested);
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Cancelled);
    assert_eq!(outcome.usage.model_calls, 1);
    let saved = fixture
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap()
        .snapshot;
    assert_eq!(saved.status, RunStatus::Cancelled);
    assert_eq!(saved.reservations.len(), 1);
    assert_eq!(
        completed(handle.cancel(id("again"), &context()).await.unwrap()),
        CancelReceipt::AlreadyTerminal
    );
}

#[tokio::test]
async fn classified_model_failure_is_saved_with_its_partial_output_instead_of_success() {
    let fixture = Fixture::new(Response::TransportFailure, false);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Failed);
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "candidate answer".into()
        }]
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .outcome,
        Some(outcome)
    );
}

#[tokio::test]
async fn unsupported_profile_modes_are_rejected_before_any_callbacks_or_model_calls() {
    let fixture = Fixture::new(Response::Text, false);
    let mut verified = profile();
    verified.completion_policy = CompletionPolicy::Verified {
        verifier_ref: reference("verifier"),
    };
    assert!(create_agent(verified, fixture.bindings()).is_err());
    let mut tools = profile();
    tools.tools = vec![ToolBindingRef::Catalog(CatalogToolRef {
        tool_id: id("search"),
        version: id("1"),
        bindings: None,
        config: None,
    })];
    assert!(create_agent(tools, fixture.bindings()).is_err());
    let mut hooks = profile();
    hooks.hooks = Some(vec![HookRef::Catalog(CatalogHookRef {
        hook_id: id("hook"),
        version: id("1"),
        position: HookPosition::BeforeModel,
    })]);
    assert_eq!(
        create_agent(hooks, fixture.bindings()).unwrap_err().code,
        ErrorCode::CapabilityUnsupported
    );
    let mut sources = profile();
    sources.context_sources = Some(vec![ContextSourceBinding {
        source: ContextSourceRef::Catalog(CatalogSourceRef {
            source_id: id("source"),
            version: id("1"),
        }),
        trigger: ContextTrigger::RunStart,
        required: true,
        timeout_ms: 1000.try_into().unwrap(),
        max_items: 1.try_into().unwrap(),
        max_bytes: 1024.try_into().unwrap(),
        max_tokens: 128.try_into().unwrap(),
    }]);
    assert_eq!(
        create_agent(sources, fixture.bindings()).unwrap_err().code,
        ErrorCode::CapabilityUnsupported
    );
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_changed_profile_cannot_replace_a_completed_sessions_pinned_prompt() {
    let fixture = Fixture::new(Response::Text, false);
    let agent = fixture.agent();
    let first = fixture.started(&agent, "first").await;
    completed(first.outcome(&context()).await.unwrap());
    let mut changed = profile();
    changed.version = id("2.0.0");
    let other = create_agent(changed, fixture.bindings()).unwrap();
    assert!(other.start(request("second"), context()).await.is_err());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert!(
        fixture
            .store
            .find_request(&scope(), &id("session"), &id("second"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn cancellation_receipt_is_not_a_terminal_outcome_until_the_final_commit_succeeds() {
    let fixture = Fixture::new(Response::Text, true);
    let store = std::sync::Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::Pause,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    assert_eq!(
        completed(handle.cancel(id("cancel"), &context()).await.unwrap()),
        CancelReceipt::Requested
    );
    store.final_entered.notified().await;
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert_eq!(saved.snapshot.status, RunStatus::Running);
    assert!(saved.snapshot.outcome.is_none());
    assert!(
        !fixture
            .store
            .read_events(&scope(), handle.run_id(), 0, 100)
            .await
            .unwrap()
            .events
            .iter()
            .any(|event| matches!(event.payload, RunEventPayload::RunFinished { .. }))
    );
    store.release.add_permits(1);
    assert_eq!(
        completed(handle.outcome(&context()).await.unwrap())
            .result
            .status(),
        RunStatus::Cancelled
    );
}

#[tokio::test]
async fn failed_final_storage_never_reports_a_successful_outcome_or_finished_event() {
    let fixture = Fixture::new(Response::Text, false);
    let store = std::sync::Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::Reject,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    assert_eq!(
        handle.outcome(&context()).await.unwrap_err().code,
        ErrorCode::PersistenceUnavailable
    );
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert!(saved.snapshot.outcome.is_none());
    assert!(!saved.snapshot.status.is_terminal());
    assert!(
        !fixture
            .store
            .read_events(&scope(), handle.run_id(), 0, 100)
            .await
            .unwrap()
            .events
            .iter()
            .any(|event| matches!(event.payload, RunEventPayload::RunFinished { .. }))
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_lost_final_commit_ack_is_resolved_from_stored_success_without_reexecuting_the_model() {
    let fixture = Fixture::new(Response::Text, false);
    let store = std::sync::Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::LoseAcknowledgement,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .outcome,
        Some(outcome)
    );
    let duplicate = fixture.started(&agent, "request").await;
    assert_eq!(duplicate.run_id(), handle.run_id());
    completed(duplicate.outcome(&context()).await.unwrap());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.final_attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn heartbeat_keeps_a_long_running_model_attempt_owned_beyond_the_original_lease() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    for _ in 0..15 {
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
    }
    let now = fixture.clock.now().unwrap().utc_ms;
    assert_eq!(
        fixture
            .store
            .acquire_lease(&scope(), handle.run_id(), &id("competitor"), now, 1000)
            .await
            .unwrap_err()
            .code,
        ErrorCode::LeaseBusy
    );
    fixture.model.release.add_permits(1);
    assert_eq!(
        completed(handle.outcome(&context()).await.unwrap())
            .result
            .status(),
        RunStatus::Succeeded
    );
}

#[tokio::test(start_paused = true)]
async fn deadline_exhaustion_stops_an_incomplete_stream_and_preserves_the_attempt() {
    let fixture = Fixture::new(Response::WaitAfterText, false);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    );
    assert_eq!(outcome.usage.model_calls, 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .status,
        RunStatus::Exhausted
    );
}

#[tokio::test]
async fn impossible_token_estimates_fail_before_model_dispatch() {
    let fixture = Fixture::new(Response::Text, false);
    fixture.estimator.tokens.store(8192, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_ne!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(outcome.usage.model_calls, 0);
}

#[tokio::test]
async fn buffered_events_recheck_current_permission_and_observer_cancellation_before_delivery() {
    for cancel in [false, true] {
        let fixture = Fixture::new(Response::Text, true);
        let agent = fixture.agent();
        let handle = fixture.started(&agent, "request").await;
        fixture.model.entered.notified().await;
        let observer = context();
        let mut events = handle.events(0, observer.clone());
        let first = events.next().await.unwrap().unwrap();
        assert_eq!(first.event_type, "run.started");
        if cancel {
            observer.cancellation.cancel();
        } else {
            fixture.policy.deny.store(1, Ordering::SeqCst);
        }
        let second = events
            .next()
            .await
            .expect("observer receives a denial, not an event");
        assert_eq!(
            second.unwrap_err().code,
            if cancel {
                ErrorCode::Cancelled
            } else {
                ErrorCode::AccessDenied
            }
        );
        fixture.policy.deny.store(0, Ordering::SeqCst);
        fixture.model.release.add_permits(1);
        assert_eq!(
            completed(handle.outcome(&context()).await.unwrap())
                .result
                .status(),
            RunStatus::Succeeded
        );
    }
}

#[tokio::test]
async fn an_event_committed_between_empty_page_and_terminal_read_is_still_delivered() {
    let fixture = Fixture::new(Response::Text, true);
    let store = std::sync::Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::PauseEmptyEventPage,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let before = fixture
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap()
        .snapshot
        .last_event_seq;
    let mut events = handle.events(before, context());
    let next = tokio::spawn(async move { events.next().await });
    store.empty_page_entered.notified().await;
    fixture.model.release.add_permits(1);
    completed(handle.outcome(&context()).await.unwrap());
    let terminal = fixture
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap()
        .snapshot;
    assert!(terminal.last_event_seq > before);
    store.empty_page_release.add_permits(1);
    let delivered = next
        .await
        .unwrap()
        .expect("final durable event must not be lost")
        .unwrap();
    assert_eq!(delivered.event_type, "run.finished");
    assert_eq!(delivered.seq.get(), terminal.last_event_seq);
}

#[tokio::test]
async fn concurrent_duplicate_starts_share_one_run_and_one_model_attempt() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let (first, second) = tokio::join!(
        agent.start(request("request"), context()),
        agent.start(request("request"), context())
    );
    let first = completed(first.unwrap());
    let second = completed(second.unwrap());
    assert_eq!(first.run_id(), second.run_id());
    fixture.model.entered.notified().await;
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    fixture.model.release.add_permits(1);
    let observer = context();
    let (first_outcome, second_outcome) =
        tokio::join!(first.outcome(&observer), second.outcome(&observer));
    assert_eq!(
        completed(first_outcome.unwrap()),
        completed(second_outcome.unwrap())
    );
}

#[tokio::test]
async fn adapter_panic_and_unexpected_tool_proposals_are_saved_as_failure_without_an_extra_attempt()
{
    for response in [Response::Panic, Response::Tool] {
        let fixture = Fixture::new(response, false);
        let agent = fixture.agent();
        let handle = fixture.started(&agent, "request").await;
        let outcome = completed(handle.outcome(&context()).await.unwrap());
        assert_eq!(outcome.result.status(), RunStatus::Failed);
        assert_eq!(outcome.usage.model_calls, 1);
        assert_eq!(outcome.usage.tool_attempts, 0);
        assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
        assert!(
            fixture
                .store
                .load(&scope(), handle.run_id())
                .await
                .unwrap()
                .session
                .active_run_id
                .is_none()
        );
    }
}

#[tokio::test]
async fn a_start_policy_denial_admits_no_run_and_calls_no_resolver_or_model() {
    let fixture = Fixture::new(Response::Text, false);
    fixture.policy.deny.store(3, Ordering::SeqCst);
    let agent = fixture.agent();
    assert_eq!(
        agent
            .start(request("request"), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert!(
        fixture
            .store
            .find_request(&scope(), &id("session"), &id("request"))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_later_run_replays_the_exact_committed_continuation_once_on_the_original_route() {
    let fixture = Fixture::new(Response::WithContinuation, false);
    let agent = fixture.agent();
    let first = fixture.started(&agent, "first").await;
    completed(first.outcome(&context()).await.unwrap());
    let saved = fixture.store.load(&scope(), first.run_id()).await.unwrap();
    let opaque: Vec<_> = saved
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            ContentBlock::ProviderOpaque {
                provider,
                route_digest,
                data_ref,
            } => Some((provider, route_digest, data_ref)),
            _ => None,
        })
        .collect();
    assert_eq!(opaque.len(), 1);
    assert_eq!(opaque[0].0, &id("fixture"));
    let record = fixture
        .store
        .read_record(&scope(), opaque[0].2)
        .await
        .unwrap();
    let expected: OpaqueContinuation = serde_json::from_value(record.value().clone()).unwrap();
    assert_eq!(
        expected.data(),
        &serde_json::json!({"signature":"fixture-signature"})
    );
    assert_eq!(expected.route_digest(), opaque[0].1);
    let second = fixture.started(&agent, "second").await;
    completed(second.outcome(&context()).await.unwrap());
    let requests = fixture.model.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let replayed: Vec<_> = requests[1]
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            ModelContent::Opaque { continuation } => Some(continuation),
            _ => None,
        })
        .collect();
    assert_eq!(replayed, vec![&expected]);
    assert_eq!(replayed[0].route_digest(), &requests[1].route.digest());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn changing_provider_for_the_same_session_does_not_forward_or_silently_discard_opaque_state()
{
    let fixture = Fixture::new(Response::WithContinuation, false);
    let agent = fixture.agent();
    let first = fixture.started(&agent, "first").await;
    completed(first.outcome(&context()).await.unwrap());
    let mut other = Fixture::new(Response::Text, false);
    other.store = fixture.store.clone();
    other.ids = fixture.ids.clone();
    other.clock = fixture.clock.clone();
    other.router = std::sync::Arc::new(Router::for_provider("different-provider"));
    let selected = other
        .router
        .snapshot
        .route_for_binding(&reference("route"))
        .unwrap();
    let mut port = Model::new(Response::Text, false);
    port.port_binding = ModelPortBinding {
        provider: selected.provider.clone(),
        adapter: selected.adapter.clone(),
        connection_ref: selected.connection_ref.clone(),
    };
    other.model = std::sync::Arc::new(port);
    let other_agent = other.agent();
    let second = other.started(&other_agent, "second").await;
    let outcome = completed(second.outcome(&context()).await.unwrap());
    assert!(
        matches!(outcome.result,OutcomeResult::Failed{failure} if failure.code==id("model_context_incompatible"))
    );
    assert_eq!(other.model.calls.load(Ordering::SeqCst), 0);
    assert!(other.model.requests.lock().unwrap().is_empty());
    // A clean session proves the second provider configuration is usable: the
    // earlier rejection is caused by incompatible continuation, not its binding.
    let mut clean = request("clean-request");
    clean.session_id = id("clean-session");
    let clean = completed(other_agent.start(clean, context()).await.unwrap());
    assert_eq!(
        completed(clean.outcome(&context()).await.unwrap())
            .result
            .status(),
        RunStatus::Succeeded
    );
    assert_eq!(other.model.calls.load(Ordering::SeqCst), 1);
    assert!(
        !other.model.requests.lock().unwrap()[0]
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .any(|content| matches!(content, ModelContent::Opaque { .. }))
    );
}

#[tokio::test]
async fn start_replay_treats_omitted_system_inputs_as_empty_instead_of_reusing_saved_values() {
    let fixture = Fixture::new(Response::Text, false);
    let mut bindings = fixture.bindings();
    bindings.system_inputs = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: serde_json::json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    let agent = create_agent(profile(), bindings).unwrap();
    let mut supplied = context();
    supplied.data.system_inputs = Some(SystemInputs::new(JsonObject::from([(
        "workspace_id".into(),
        serde_json::json!("11111111-1111-4111-8111-111111111111"),
    )])));
    let first = completed(
        agent
            .start(request("with-inputs"), supplied.clone())
            .await
            .unwrap(),
    );
    completed(first.outcome(&context()).await.unwrap());
    let before = fixture.catalog.calls.load(Ordering::SeqCst);
    let omitted = agent
        .start(request("with-inputs"), context())
        .await
        .unwrap_err();
    assert!(matches!(
        omitted.code,
        ErrorCode::SystemInputsMismatch | ErrorCode::RequestConflict
    ));
    let same = completed(agent.start(request("with-inputs"), supplied).await.unwrap());
    assert_eq!(same.run_id(), first.run_id());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), before);

    let mut empty_request = request("empty-inputs");
    empty_request.session_id = id("empty-session");
    let empty = completed(agent.start(empty_request.clone(), context()).await.unwrap());
    completed(empty.outcome(&context()).await.unwrap());
    let mut explicit_empty = context();
    explicit_empty.data.system_inputs = Some(SystemInputs::default());
    let same_empty = completed(agent.start(empty_request, explicit_empty).await.unwrap());
    assert_eq!(same_empty.run_id(), empty.run_id());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn observer_cancellation_interrupts_pending_store_reads_without_cancelling_the_driver() {
    for operation in 0..3 {
        let fixture = Fixture::new(Response::Text, true);
        let store = std::sync::Arc::new(FinalCommitStore::new(
            fixture.store.clone(),
            FinalCommitMode::PassThrough,
        ));
        let mut bindings = fixture.bindings();
        bindings.state = store.clone();
        let agent = create_agent(profile(), bindings).unwrap();
        let handle = fixture.started(&agent, "request").await;
        fixture.model.entered.notified().await;
        let observer = context();
        store
            .block_read
            .store(if operation == 1 { 2 } else { 1 }, Ordering::SeqCst);
        let waiting_handle = handle.clone();
        let waiting_agent = agent.clone();
        let waiting_context = observer.clone();
        let waiting = tokio::spawn(async move {
            match operation {
                0 => waiting_handle.outcome(&waiting_context).await.map(|_| ()),
                1 => waiting_handle
                    .events(0, waiting_context)
                    .next()
                    .await
                    .expect("observer must report cancellation")
                    .map(|_| ()),
                _ => waiting_agent
                    .get_run(waiting_handle.run_id(), &waiting_context)
                    .await
                    .map(|_| ()),
            }
        });
        store.read_entered.notified().await;
        observer.cancellation.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .expect("cancel must interrupt the pending store read")
            .unwrap();
        assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
        fixture.model.release.add_permits(1);
        assert_eq!(
            completed(handle.outcome(&context()).await.unwrap())
                .result
                .status(),
            RunStatus::Succeeded
        );
        assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn initial_request_lookup_observes_cancellation_and_start_timeout_before_admission() {
    for cancel in [true, false] {
        let fixture = Fixture::new(Response::Text, false);
        let store = std::sync::Arc::new(FinalCommitStore::new(
            fixture.store.clone(),
            FinalCommitMode::PassThrough,
        ));
        store.block_read.store(3, Ordering::SeqCst);
        let mut bindings = fixture.bindings();
        bindings.state = store.clone();
        bindings.settings.start_timeout_ms = 30;
        let agent = create_agent(profile(), bindings).unwrap();
        let caller = context();
        let task_context = caller.clone();
        let start =
            tokio::spawn(async move { agent.start(request("request"), task_context).await });
        store.read_entered.notified().await;
        if cancel {
            caller.cancellation.cancel();
        }
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), start)
            .await
            .expect("request lookup must be bounded by start control")
            .unwrap();
        assert_eq!(
            result.unwrap_err().code,
            if cancel {
                ErrorCode::Cancelled
            } else {
                ErrorCode::DeadlineExceeded
            }
        );
        assert!(
            fixture
                .store
                .find_request(&scope(), &id("session"), &id("request"))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn exhausting_a_retry_budget_preserves_the_partial_response_already_saved_for_this_step() {
    let fixture = Fixture::new(Response::TransportFailure, false);
    let mut profile = profile();
    profile.limits.max_model_calls = 1.try_into().unwrap();
    profile.limits.max_recovery_attempts = 1;
    let gate = std::sync::Arc::new(
        PolicyGate::new(fixture.policy.clone(), std::time::Duration::from_secs(1)).unwrap(),
    );
    let exchange = ModelExchange::new(fixture.model.clone(), gate)
        .with_route_inspector(fixture.inspector.clone(), std::time::Duration::from_secs(1))
        .unwrap()
        .with_retry_policy(ModelRetryPolicy {
            max_retries: 1,
            backoff_ms: 0,
        });
    let mut bindings = fixture.bindings();
    bindings.model_exchange = std::sync::Arc::new(exchange);
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::ModelCalls
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "candidate answer".into()
        }]
    );
    assert_eq!(outcome.usage.model_calls, 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let saved = fixture
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap()
        .snapshot;
    assert_eq!(saved.outcome, Some(outcome));
    assert!(saved.model_ledger[0].response_ref.is_some());
}
```

## `crates/wickle/tests/budget.rs`

```rust
//! Reservations and stop boundaries exercised against a real state store.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Barrier, Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use wickle::*;

#[allow(dead_code)]
mod support;
use support::{admission, id, scope};

#[derive(Default)]
struct FakeClock {
    reading: Mutex<(i64, u64)>,
    changed: Notify,
}
impl FakeClock {
    fn set(&self, utc_ms: i64, monotonic_ms: u64) {
        *self.reading.lock().unwrap() = (utc_ms, monotonic_ms);
        self.changed.notify_waiters();
    }
}
impl Clock for FakeClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        let (utc_ms, monotonic_ms) = *self.reading.lock().unwrap();
        Ok(ClockReading {
            utc_ms,
            monotonic_ms,
        })
    }
    fn sleep_until<'a>(&'a self, deadline: u64) -> PortFuture<'a, ()> {
        Box::pin(async move {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.now()?.monotonic_ms >= deadline {
                    return Ok(());
                }
                changed.await;
            }
        })
    }
}
#[derive(Default)]
struct FixedIds(AtomicUsize);
impl IdSource for FixedIds {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id(&format!(
            "attempt-{}",
            self.0.fetch_add(1, Ordering::SeqCst)
        )))
    }
}
struct RepeatedId;
impl IdSource for RepeatedId {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id("same-attempt"))
    }
}

enum CommitControl {
    Race(Barrier),
    Pause { entered: Notify, release: Semaphore },
    Fail,
    ExpireDuringLeaseRead(Arc<FakeClock>),
}
struct ControlledStore {
    inner: Arc<MemoryStateStore>,
    control: CommitControl,
}
impl StateStore for ControlledStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
        request_id: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        self.inner.find_request(scope, session_id, request_id)
    }
    fn admit<'a>(&'a self, s: &'a Scope, input: AdmissionInput) -> PortFuture<'a, AdmissionResult> {
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
        lease: &'a RunLease,
        now: i64,
    ) -> PortFuture<'a, RunLease> {
        Box::pin(async move {
            let current = self.inner.check_lease(s, r, lease, now).await?;
            if let CommitControl::ExpireDuringLeaseRead(clock) = &self.control {
                clock.set(current.expires_at_ms, current.expires_at_ms as u64);
            }
            Ok(current)
        })
    }
    fn acquire_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        owner: &'a Id,
        now: i64,
        ttl: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.acquire_lease(s, r, owner, now, ttl)
    }
    fn renew_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        lease: &'a RunLease,
        now: i64,
        ttl: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.renew_lease(s, r, lease, now, ttl)
    }
    fn release_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        lease: &'a RunLease,
        now: i64,
    ) -> PortFuture<'a, ()> {
        self.inner.release_lease(s, r, lease, now)
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
        self.inner.read_record(s, r)
    }
    fn commit<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            match &self.control {
                CommitControl::Race(barrier) => {
                    barrier.wait().await;
                }
                CommitControl::Fail => {
                    return Err(ContractError::new(
                        ErrorCode::PersistenceUnavailable,
                        "injected.commit",
                    ));
                }
                CommitControl::Pause { .. } | CommitControl::ExpireDuringLeaseRead(_) => {}
            }
            let saved = self.inner.commit(s, r, input).await?;
            if let CommitControl::Pause { entered, release } = &self.control {
                entered.notify_one();
                release.acquire().await.unwrap().forget();
            }
            Ok(saved)
        })
    }
}
struct Fixture {
    store: Arc<MemoryStateStore>,
    clock: Arc<FakeClock>,
    ids: Arc<dyn IdSource>,
    lease: RunLease,
    cancellation: CancellationToken,
}
impl Fixture {
    async fn new(models: u64, tools: u64, repair: u64, recovery: u64) -> Self {
        let store = Arc::new(MemoryStateStore::new());
        let mut input = admission("run", "request", "session", "Read evidence", "1").await;
        input.snapshot.limits.max_model_calls = models.try_into().unwrap();
        input.snapshot.limits.max_tool_attempts = tools;
        input.snapshot.limits.max_repair_attempts = repair;
        input.snapshot.limits.max_recovery_attempts = recovery;
        input.snapshot.limits.max_elapsed_ms = 100.try_into().unwrap();
        input.snapshot.timing = RunTiming::new(0, 100).unwrap();
        store.admit(&scope(), input).await.unwrap();
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("worker"), 0, 200)
            .await
            .unwrap();
        Self {
            store,
            clock: Arc::new(FakeClock::default()),
            ids: Arc::new(FixedIds::default()),
            lease,
            cancellation: CancellationToken::new(),
        }
    }
    async fn budget(&self, store: Arc<dyn StateStore>) -> RunBudget {
        RunBudget::attach(
            store,
            self.clock.clone(),
            self.ids.clone(),
            scope(),
            id("run"),
            self.lease.clone(),
            self.cancellation.clone(),
        )
        .await
        .unwrap()
    }
    async fn saved(&self) -> RunSnapshot {
        self.store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
    }
}
fn model(purpose: ModelPurpose) -> ReservationKind {
    ReservationKind::Model { purpose }
}
fn counted(counter: &AtomicUsize) -> std::future::Ready<Result<(), ContractError>> {
    counter.fetch_add(1, Ordering::SeqCst);
    std::future::ready(Ok(()))
}

#[tokio::test]
async fn model_purposes_share_a_limit_and_do_not_consume_the_tool_budget() {
    let fixture = Fixture::new(3, 1, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    let calls = AtomicUsize::new(0);
    for purpose in [
        ModelPurpose::Agent,
        ModelPurpose::Verification,
        ModelPurpose::Compaction,
    ] {
        budget
            .execute(model(purpose), |_| counted(&calls))
            .await
            .unwrap();
    }
    let error = budget
        .execute(model(ModelPurpose::Agent), |_| counted(&calls))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::BudgetExceeded);
    budget
        .execute(
            ReservationKind::Tool {
                call_id: id("planned-call"),
            },
            |_| counted(&calls),
        )
        .await
        .unwrap();
    for kind in [
        ReservationKind::Repair {},
        ReservationKind::Recovery {},
        ReservationKind::Tool {
            call_id: id("another-call"),
        },
    ] {
        assert_eq!(
            budget
                .execute(kind, |_| counted(&calls))
                .await
                .unwrap_err()
                .code,
            ErrorCode::BudgetExceeded
        );
    }
    let saved = fixture.saved().await;
    assert_eq!(
        (
            calls.load(Ordering::SeqCst),
            saved.usage.model_calls,
            saved.usage.tool_attempts
        ),
        (4, 3, 1)
    );
    assert_eq!(saved.reservations.len(), 4);
}

#[tokio::test]
async fn the_last_model_slot_can_only_authorize_one_competing_dispatch() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let store = Arc::new(ControlledStore {
        inner: fixture.store.clone(),
        control: CommitControl::Race(Barrier::new(2)),
    });
    let left = fixture.budget(store.clone()).await;
    let right = fixture.budget(store).await;
    let calls = AtomicUsize::new(0);
    let (left, right) = tokio::join!(
        left.execute(model(ModelPurpose::Agent), |_| counted(&calls)),
        right.execute(model(ModelPurpose::Verification), |_| counted(&calls)),
    );
    let results = [left, right];
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results.into_iter().find_map(Result::err).unwrap().code,
        ErrorCode::RevisionConflict
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let saved = fixture.saved().await;
    assert_eq!((saved.usage.model_calls, saved.reservations.len()), (1, 1));
}

#[tokio::test]
async fn failed_persistence_never_constructs_the_call_or_charges_the_store() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let store = Arc::new(ControlledStore {
        inner: fixture.store.clone(),
        control: CommitControl::Fail,
    });
    let budget = fixture.budget(store).await;
    let calls = AtomicUsize::new(0);
    assert_eq!(
        budget
            .execute(model(ModelPurpose::Agent), |_| counted(&calls))
            .await
            .unwrap_err()
            .code,
        ErrorCode::PersistenceUnavailable
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.saved().await.usage.model_calls, 0);
}

#[tokio::test]
async fn a_stop_during_reservation_blocks_dispatch_without_refunding_the_saved_attempt() {
    for stop in [
        ErrorCode::Cancelled,
        ErrorCode::DeadlineExceeded,
        ErrorCode::LeaseLost,
    ] {
        let fixture = Fixture::new(2, 0, 0, 0).await;
        let store = Arc::new(ControlledStore {
            inner: fixture.store.clone(),
            control: CommitControl::Pause {
                entered: Notify::new(),
                release: Semaphore::new(0),
            },
        });
        let budget = fixture.budget(store.clone()).await;
        let calls = AtomicUsize::new(0);
        let execute = budget.execute(model(ModelPurpose::Agent), |_| counted(&calls));
        let interrupt = async {
            let CommitControl::Pause { entered, release } = &store.control else {
                unreachable!()
            };
            entered.notified().await;
            match stop {
                ErrorCode::Cancelled => fixture.cancellation.cancel(),
                ErrorCode::DeadlineExceeded => fixture.clock.set(100, 100),
                ErrorCode::LeaseLost => {
                    fixture
                        .store
                        .release_lease(&scope(), &id("run"), &fixture.lease, 0)
                        .await
                        .unwrap();
                    fixture
                        .store
                        .acquire_lease(&scope(), &id("run"), &id("replacement"), 0, 200)
                        .await
                        .unwrap();
                }
                _ => unreachable!(),
            }
            release.add_permits(1);
        };
        let (result, ()) = tokio::join!(execute, interrupt);
        assert_eq!(result.unwrap_err().code, stop);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let saved = fixture.saved().await;
        assert_eq!((saved.usage.model_calls, saved.reservations.len()), (1, 1));
    }
}

#[tokio::test]
async fn an_unknown_attempt_stays_charged_after_reattach_and_retry_gets_a_new_id() {
    let fixture = Fixture::new(2, 0, 1, 1).await;
    let first = fixture.budget(fixture.store.clone()).await;
    first
        .execute(model(ModelPurpose::Agent), |_| async {
            Err::<(), _>(ContractError::new(
                ErrorCode::PersistenceUnavailable,
                "response.unknown",
            ))
        })
        .await
        .unwrap_err();
    fixture.clock.set(10, 10);
    let resumed = fixture.budget(fixture.store.clone()).await;
    resumed
        .execute(model(ModelPurpose::Agent), |_| async { Ok(()) })
        .await
        .unwrap();
    resumed.reserve(ReservationKind::Repair {}).await.unwrap();
    resumed.reserve(ReservationKind::Recovery {}).await.unwrap();
    let saved = fixture.saved().await;
    assert_eq!(saved.usage.model_calls, 2);
    assert_eq!(saved.usage.repair_attempts, 1);
    assert_eq!(saved.usage.recovery_attempts, 1);
    assert_ne!(
        saved.reservations[0].attempt_id,
        saved.reservations[1].attempt_id
    );
    assert_eq!(saved.reservations[0].reserved_at_ms, 0);
    assert_eq!(saved.reservations[1].reserved_at_ms, 10);
    assert_eq!(saved.usage.elapsed_ms, 10);
    assert_eq!(saved.timing.deadline_at_ms, 100);
}

#[tokio::test]
async fn reused_attempt_identity_cannot_authorize_a_second_physical_call() {
    let mut fixture = Fixture::new(2, 0, 0, 0).await;
    fixture.ids = Arc::new(RepeatedId);
    let budget = fixture.budget(fixture.store.clone()).await;
    let calls = AtomicUsize::new(0);
    budget
        .execute(model(ModelPurpose::Agent), |_| counted(&calls))
        .await
        .unwrap();
    assert_eq!(
        budget
            .execute(model(ModelPurpose::Agent), |_| counted(&calls))
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidSnapshot
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.saved().await.usage.model_calls, 1);
}

#[tokio::test]
async fn active_elapsed_uses_monotonic_time_and_reattach_counts_offline_time() {
    let fixture = Fixture::new(3, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    fixture.clock.set(50_000, 10);
    budget.reserve(model(ModelPurpose::Agent)).await.unwrap();
    fixture.clock.set(-50_000, 20);
    budget
        .reserve(model(ModelPurpose::Verification))
        .await
        .unwrap();
    assert_eq!(fixture.saved().await.usage.elapsed_ms, 20);
    let regression = RunBudget::attach(
        fixture.store.clone(),
        fixture.clock.clone(),
        fixture.ids.clone(),
        scope(),
        id("run"),
        fixture.lease.clone(),
        fixture.cancellation.clone(),
    )
    .await;
    assert!(matches!(
        regression,
        Err(ContractError {
            code: ErrorCode::ClockRegression,
            ..
        })
    ));
    fixture.clock.set(95, 40);
    let resumed = fixture.budget(fixture.store.clone()).await;
    assert_eq!(resumed.elapsed_ms().unwrap(), 95);
    fixture.clock.set(100, 45);
    let calls = AtomicUsize::new(0);
    assert_eq!(
        resumed
            .execute(model(ModelPurpose::Compaction), |_| counted(&calls))
            .await
            .unwrap_err()
            .code,
        ErrorCode::DeadlineExceeded
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.saved().await.usage.model_calls, 2);
}

#[tokio::test]
async fn waiting_observation_consumes_time_but_no_model_attempts() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let snapshot = fixture.saved().await;
    let mut commit = support::prepared(&snapshot, fixture.lease.clone(), 0);
    commit.snapshot.status = RunStatus::Waiting;
    commit.snapshot.phase = RunPhase::Waiting;
    commit.snapshot.wait = Some(WaitState {
        wait_id: id("wait"),
        target: WaitTarget::Approval {
            target: ApprovalTarget::Tool {
                call_id: id("call"),
                binding_digest: canonical_digest(&serde_json::json!({})),
            },
        },
        expires_at_ms: None,
    });
    // A wait target needs a matching planned tool in the saved checkpoint.
    commit.snapshot.tool_ledger.push(ToolLedgerEntry {
        call: ToolCall {
            call_id: id("call"),
            model_request_id: id("model"),
            provider_call_id: id("provider-call"),
            tool_name: id("read"),
            model_inputs: JsonObject::new(),
            descriptor_digest: canonical_digest(&serde_json::json!({})),
            bound_input_ref: None,
        },
        state: ToolCallState::Planned {},
    });
    fixture
        .store
        .commit(&scope(), &id("run"), commit)
        .await
        .unwrap();
    let waiting = fixture.budget(fixture.store.clone()).await;
    let observer = waiting.wait_for_cancellation_or_deadline();
    tokio::pin!(observer);
    assert!(futures_util::poll!(observer.as_mut()).is_pending());
    fixture.clock.set(100, 100);
    assert_eq!(
        observer.await.unwrap_err().code,
        ErrorCode::DeadlineExceeded
    );
    assert_eq!(fixture.saved().await.usage.model_calls, 0);
    let resumed = fixture.budget(fixture.store.clone()).await;
    assert_eq!(resumed.elapsed_ms().unwrap(), 100);
}

#[tokio::test]
async fn cancellation_after_an_effect_does_not_undo_it_or_refund_the_attempt() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    let effects = AtomicUsize::new(0);
    let error = budget
        .execute(model(ModelPurpose::Agent), |_| async {
            effects.fetch_add(1, Ordering::SeqCst);
            fixture.cancellation.cancel();
            Ok(())
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Cancelled);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.saved().await.usage.model_calls, 1);
    assert_eq!(
        budget
            .execute(model(ModelPurpose::Agent), |_| counted(&effects))
            .await
            .unwrap_err()
            .code,
        ErrorCode::Cancelled
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dropping_a_waiter_does_not_cancel_execution_and_a_clock_timer_wakes_at_deadline() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    let mut observer = Box::pin(budget.wait_for_cancellation_or_deadline());
    assert!(futures_util::poll!(observer.as_mut()).is_pending());
    drop(observer);
    budget
        .execute(model(ModelPurpose::Agent), |_| async { Ok(()) })
        .await
        .unwrap();
    assert!(!fixture.cancellation.is_cancelled());
    let clock = SystemClock::new();
    let reading = clock.now().unwrap();
    clock.sleep_until(reading.monotonic_ms + 1).await.unwrap();
    assert!(clock.now().unwrap().monotonic_ms > reading.monotonic_ms);
}

#[tokio::test]
async fn an_in_flight_operation_is_interrupted_when_the_original_deadline_arrives() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    let calls = AtomicUsize::new(0);
    let execution = budget.execute(model(ModelPurpose::Agent), |_| {
        calls.fetch_add(1, Ordering::SeqCst);
        std::future::pending::<Result<(), ContractError>>()
    });
    tokio::pin!(execution);
    assert!(futures_util::poll!(execution.as_mut()).is_pending());
    fixture.clock.set(100, 100);
    assert_eq!(
        execution.await.unwrap_err().code,
        ErrorCode::DeadlineExceeded
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.saved().await.usage.model_calls, 1);
}

#[tokio::test]
async fn lease_expiration_while_checking_storage_blocks_the_next_call() {
    let fixture = Fixture::new(1, 0, 0, 0).await;
    fixture
        .store
        .renew_lease(&scope(), &id("run"), &fixture.lease, 0, 10)
        .await
        .unwrap();
    let store = Arc::new(ControlledStore {
        inner: fixture.store.clone(),
        control: CommitControl::ExpireDuringLeaseRead(fixture.clock.clone()),
    });
    let budget = fixture.budget(store).await;
    let calls = AtomicUsize::new(0);
    assert_eq!(
        budget
            .execute(model(ModelPurpose::Agent), |_| counted(&calls))
            .await
            .unwrap_err()
            .code,
        ErrorCode::LeaseLost
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.saved().await.usage.model_calls, 1);
}

#[tokio::test]
async fn a_monotonic_clock_regression_cannot_extend_a_live_segment() {
    let fixture = Fixture::new(2, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    fixture.clock.set(20, 20);
    budget.reserve(model(ModelPurpose::Agent)).await.unwrap();
    fixture.clock.set(30, 10);
    let calls = AtomicUsize::new(0);
    assert_eq!(
        budget
            .execute(model(ModelPurpose::Agent), |_| counted(&calls))
            .await
            .unwrap_err()
            .code,
        ErrorCode::ClockRegression
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.saved().await.usage.model_calls, 1);
}

#[tokio::test]
async fn saved_attempts_and_original_deadline_cannot_be_refunded_or_replaced() {
    let fixture = Fixture::new(2, 1, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    budget.reserve(model(ModelPurpose::Agent)).await.unwrap();
    let before = fixture.saved().await;
    let mut refund = support::prepared(&before, fixture.lease.clone(), 0);
    refund.snapshot.reservations.clear();
    refund.snapshot.usage.model_calls = 0;
    assert_eq!(
        fixture
            .store
            .commit(&scope(), &id("run"), refund)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidTransition
    );
    let mut reset = support::prepared(&before, fixture.lease.clone(), 0);
    reset.snapshot.timing = RunTiming::new(1, 100).unwrap();
    reset.snapshot.reservations[0].reserved_at_ms = 1;
    assert_eq!(
        fixture
            .store
            .commit(&scope(), &id("run"), reset)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidTransition
    );
    let mut extra = support::prepared(&before, fixture.lease.clone(), 0);
    extra.snapshot.usage.model_calls += 1;
    assert_eq!(
        fixture
            .store
            .commit(&scope(), &id("run"), extra)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidSnapshot
    );
    assert_eq!(fixture.saved().await, before);
}

#[tokio::test]
async fn restoring_a_snapshot_rejects_missing_reservations_and_forged_counts() {
    let fixture = Fixture::new(2, 0, 0, 0).await;
    let budget = fixture.budget(fixture.store.clone()).await;
    budget.reserve(model(ModelPurpose::Agent)).await.unwrap();
    let snapshot = fixture.saved().await;
    let mut missing = snapshot.clone();
    missing.reservations.clear();
    let mut forged = snapshot.clone();
    forged.usage.model_calls = 2;
    let mut duplicated = snapshot.clone();
    duplicated
        .reservations
        .push(snapshot.reservations[0].clone());
    duplicated.usage.model_calls = 2;
    for invalid in [missing, forged, duplicated] {
        assert_eq!(
            RunSnapshot::from_json(&serde_json::to_string(&invalid).unwrap())
                .unwrap_err()
                .code,
            ErrorCode::InvalidSnapshot
        );
    }
    assert_eq!(
        RunSnapshot::from_json(&serde_json::to_string(&snapshot).unwrap()).unwrap(),
        snapshot
    );
}
```

## `crates/wickle/tests/input_binding.rs`

```rust
//! Frozen system inputs, scoped resolution, defaults, and binding persistence boundaries.

use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use wickle::*;

#[allow(dead_code)]
mod support;
use support::{id, scope};

const OWNED: &str = "11111111-1111-4111-8111-111111111111";
const OTHER: &str = "22222222-2222-4222-8222-222222222222";
fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
fn inputs(value: Value) -> SystemInputs {
    SystemInputs::new(object(value))
}
fn versioned(name: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id("1"),
    }
}
fn definition(key: &str, schema: Value) -> SystemInputDefinition {
    SystemInputDefinition {
        key: id(key),
        version: id("1"),
        value_schema: schema,
        source: SystemInputSource::Run {},
    }
}
fn registry() -> SystemInputRegistry {
    SystemInputRegistry::new(vec![
        definition("workspace_id", json!({"type":"string","format":"uuid"})),
        definition("query", json!({"type":"string"})),
        definition("user_id", json!({"type":"string"})),
    ])
    .unwrap()
}
fn descriptor() -> ToolDescriptor {
    ToolDescriptor {
        tool: versioned("search"),
        name: id("search"),
        description: "Search records".into(),
        input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"default":10},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters: vec!["query".into(), "limit".into()],
        system_bindings: None,
        output_schema: json!({"type":"string"}),
        side_effect: ToolSideEffect::ReadOnly,
        concurrency: ToolConcurrency::Serial,
        retry: ToolRetryPolicy::Never,
        reconcile: false,
        max_output_bytes: 4096.try_into().unwrap(),
    }
}

#[derive(Default)]
struct FakeClock {
    reading: Mutex<(i64, u64)>,
    changed: Notify,
}
impl FakeClock {
    fn advance(&self, millis: u64) {
        *self.reading.lock().unwrap() = (millis as i64, millis);
        self.changed.notify_waiters();
    }
}
impl Clock for FakeClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        let (utc_ms, monotonic_ms) = *self.reading.lock().unwrap();
        Ok(ClockReading {
            utc_ms,
            monotonic_ms,
        })
    }
    fn sleep_until<'a>(&'a self, deadline: u64) -> PortFuture<'a, ()> {
        Box::pin(async move {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.now()?.monotonic_ms >= deadline {
                    return Ok(());
                }
                changed.await;
            }
        })
    }
}
#[derive(Default)]
struct Ids(AtomicUsize);
impl IdSource for Ids {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id(&format!(
            "binding-{}",
            self.0.fetch_add(1, Ordering::SeqCst)
        )))
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
                    version: Some(id("1")),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("catalog")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: Default::default(),
                required_capabilities: Default::default(),
                required_connections: Default::default(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| reference.id.clone()),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

#[derive(Default)]
struct Policy {
    // 0 allow; 1 target ownership; 2 deny lookup; 3 approve lookup; 4 deny execution; 5 approve execution.
    mode: AtomicUsize,
    lookups: AtomicUsize,
    executions: Mutex<Vec<JsonObject>>,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            match &request.action {
                PolicyAction::ResolveSystemInput { .. } => {
                    self.lookups.fetch_add(1, Ordering::SeqCst);
                    match self.mode.load(Ordering::SeqCst) {
                        2 => {
                            return Ok(PolicyDecision::Deny {
                                reason: id("lookup_denied"),
                            });
                        }
                        3 => {
                            return Ok(PolicyDecision::RequireApproval {
                                reason: id("lookup_approval"),
                            });
                        }
                        _ => {}
                    }
                }
                PolicyAction::ExecuteTool { input } => {
                    self.executions
                        .lock()
                        .unwrap()
                        .push(input.execution_args().clone());
                    match self.mode.load(Ordering::SeqCst) {
                        1 => {
                            // The Host resource catalog associates each real UUID with an owner.
                            let target = input
                                .execution_args()
                                .get("workspace_id")
                                .and_then(Value::as_str);
                            let owner = match target {
                                Some(OWNED) => Some(scope()),
                                Some(OTHER) => Some(Scope {
                                    tenant_id: id("foreign-tenant"),
                                    ..scope()
                                }),
                                _ => None,
                            };
                            if owner.as_ref() != Some(context.scope) {
                                return Ok(PolicyDecision::Deny {
                                    reason: id("target_not_owned"),
                                });
                            }
                        }
                        4 => {
                            return Ok(PolicyDecision::Deny {
                                reason: id("revoked"),
                            });
                        }
                        5 => {
                            return Ok(PolicyDecision::RequireApproval {
                                reason: id("execution_approval"),
                            });
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}

struct Resolver {
    value: Mutex<Option<ResolvedSystemInput>>,
    calls: AtomicUsize,
    requests: Mutex<Vec<SystemInputResolveRequest>>,
    cancel: Mutex<Option<CancellationToken>>,
    advance: Mutex<Option<Arc<FakeClock>>>,
}
impl Resolver {
    fn new(value: Value) -> Self {
        Self {
            value: Mutex::new(Some(ResolvedSystemInput {
                value,
                revision: id("revision-1"),
            })),
            calls: AtomicUsize::new(0),
            requests: Mutex::new(vec![]),
            cancel: Mutex::new(None),
            advance: Mutex::new(None),
        }
    }
    fn set(&self, value: Value, revision: &str) {
        *self.value.lock().unwrap() = Some(ResolvedSystemInput {
            value,
            revision: id(revision),
        });
    }
}
impl SystemInputResolver for Resolver {
    fn resolve<'a>(
        &'a self,
        request: &'a SystemInputResolveRequest,
        context: &'a SystemInputResolveContext,
    ) -> PortFuture<'a, Option<ResolvedSystemInput>> {
        Box::pin(async move {
            assert_eq!(context.scope, scope());
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.requests.lock().unwrap().push(request.clone());
            if let Some(token) = self.cancel.lock().unwrap().as_ref() {
                token.cancel();
            }
            if let Some(clock) = self.advance.lock().unwrap().as_ref() {
                clock.advance(10_000);
            }
            Ok(self.value.lock().unwrap().clone())
        })
    }
}
fn resolver_registry() -> SystemInputRegistry {
    SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("current_workspace"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Resolver {
            resolver_ref: versioned("workspace_lookup"),
        },
    }])
    .unwrap()
}
fn resolver_descriptor() -> ToolDescriptor {
    let mut tool = descriptor();
    tool.system_bindings = Some(BTreeMap::from([(
        "workspace_id".into(),
        id("current_workspace"),
    )]));
    tool
}

struct Fixture {
    store: Arc<MemoryStateStore>,
    clock: Arc<FakeClock>,
    ids: Arc<Ids>,
    lease: RunLease,
    context: ExecutionContext,
    registry: Arc<SystemInputRegistry>,
    tool: CompiledTool,
    policy: Arc<Policy>,
}
impl Fixture {
    async fn new(
        tool: ToolDescriptor,
        registry: SystemInputRegistry,
        supplied: Option<SystemInputs>,
    ) -> Self {
        let registry = Arc::new(registry);
        let tool = SchemaCompiler::new().compile(tool, &registry).unwrap();
        let fixed = RunSystemInputs::capture(scope(), supplied.clone(), &registry).unwrap();
        let fixed_record = fixed.to_record(id("run-inputs"), 7);
        let fixed_ref = fixed.snapshot_ref(fixed_record.reference()).unwrap();
        let mut input = support::admission("run", "request", "session", "Read evidence", "1").await;
        let mut profile = input.snapshot.profile.profile().clone();
        profile.tools = vec![ToolBindingRef::Catalog(CatalogToolRef {
            tool_id: tool.descriptor().tool.id.clone(),
            version: tool.descriptor().tool.version.clone(),
            bindings: None,
            config: None,
        })];
        input.snapshot.profile = ProfileValidator::new(&Catalog)
            .validate(&profile, &scope())
            .await
            .unwrap();
        input.snapshot.system_inputs = Some(fixed_ref);
        input.snapshot.request_digest = admission_digest(
            &input.snapshot.request,
            &input.snapshot.profile,
            input.snapshot.system_inputs.as_ref(),
        );
        let RunEventPayload::RunStarted { profile_digest, .. } = &mut input.events[0].payload
        else {
            unreachable!()
        };
        *profile_digest = input.snapshot.profile.profile_digest().clone();
        input.records.push(fixed_record);
        let store = Arc::new(MemoryStateStore::new());
        store.admit(&scope(), input).await.unwrap();
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("worker"), 0, 20_000)
            .await
            .unwrap();
        let context = ExecutionContext::new(
            ExecutionContextData {
                scope: scope(),
                principal_ref: id("user"),
                capability_grant_ref: id("grant"),
                trace_context: None,
                system_inputs: supplied,
            },
            CancellationToken::new(),
        );
        Self {
            store,
            clock: Arc::new(FakeClock::default()),
            ids: Arc::new(Ids::default()),
            lease,
            context,
            registry,
            tool,
            policy: Arc::new(Policy::default()),
        }
    }
    async fn plan(&self, call_id: &str, model_inputs: JsonObject) -> ToolCall {
        let saved = self.store.load(&scope(), &id("run")).await.unwrap();
        let call = ToolCall {
            call_id: id(call_id),
            model_request_id: id(&format!("request-{call_id}")),
            provider_call_id: id(&format!("provider-{call_id}")),
            tool_name: self.tool.descriptor().name.clone(),
            model_inputs,
            descriptor_digest: self.tool.descriptor_digest().clone(),
            bound_input_ref: None,
        };
        let record = ProtectedRecord::new(
            id(&format!("planned-{call_id}")),
            1,
            serde_json::to_value(&call).unwrap(),
        );
        let mut update = support::prepared(
            &saved.snapshot,
            self.lease.clone(),
            self.clock.now().unwrap().utc_ms,
        );
        update.snapshot.phase = RunPhase::Tool;
        update.snapshot.tool_ledger.push(ToolLedgerEntry {
            call: call.clone(),
            state: ToolCallState::Planned {},
        });
        update.snapshot.last_event_seq += 1;
        update.events = vec![support::event(
            &id("run"),
            &id("session"),
            &scope(),
            update.snapshot.last_event_seq,
            RunEventPayload::ToolPlanned {
                call_ref: record.reference().clone(),
            },
        )];
        update.messages = vec![Message {
            message_id: id(&format!("call-message-{call_id}")),
            run_id: id("run"),
            sequence: (saved.session.transcript_revision + 1).try_into().unwrap(),
            role: MessageRole::Assistant,
            origin: MessageOrigin::Model,
            visibility: Visibility::UserAndModel,
            content: vec![ContentBlock::ToolCall { call: call.clone() }],
        }];
        update.records = vec![record];
        self.store
            .commit(&scope(), &id("run"), update)
            .await
            .unwrap();
        call
    }
    async fn budget(&self, store: Arc<dyn StateStore>) -> RunBudget {
        RunBudget::attach(
            store,
            self.clock.clone(),
            self.ids.clone(),
            scope(),
            id("run"),
            self.lease.clone(),
            self.context.cancellation.clone(),
        )
        .await
        .unwrap()
    }
    fn binder(&self, resolver: Option<Arc<dyn SystemInputResolver>>) -> InputBinder {
        InputBinder::new(
            self.registry.clone(),
            resolver,
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap()),
            self.ids.clone(),
        )
    }
    async fn bind(
        &self,
        call_id: &str,
        resolver: Option<Arc<dyn SystemInputResolver>>,
    ) -> Result<ToolBindingResult, ContractError> {
        self.binder(resolver)
            .bind(
                &self.tool,
                &id(call_id),
                &self.context,
                &self.budget(self.store.clone()).await,
            )
            .await
    }
}

#[test]
fn run_capture_rejects_unregistered_keys_invalid_values_and_resolver_source_conflicts() {
    let invalid_value = RunSystemInputs::capture(
        scope(),
        Some(inputs(json!({"workspace_id":"private-invalid-value"}))),
        &registry(),
    )
    .unwrap_err();
    assert_eq!(invalid_value.path, r#"system_inputs["workspace_id"]"#);
    for value in [
        json!({"unregistered":"x"}),
        json!({"workspace_id":"not-a-uuid"}),
        json!({"workspace_id":null}),
    ] {
        assert!(RunSystemInputs::capture(scope(), Some(inputs(value)), &registry()).is_err());
    }
    assert!(
        RunSystemInputs::capture(
            scope(),
            Some(inputs(json!({"current_workspace":OWNED}))),
            &resolver_registry()
        )
        .is_err()
    );
}

#[test]
fn restored_run_inputs_distinguish_omission_from_empty_or_changed_resume_values() {
    let registry = registry();
    let supplied = inputs(json!({"workspace_id":OWNED}));
    let fixed = RunSystemInputs::capture(scope(), Some(supplied.clone()), &registry).unwrap();
    let record = fixed.to_record(id("snapshot"), 1);
    let reference = fixed.snapshot_ref(record.reference()).unwrap();
    let restored = RunSystemInputs::restore(&record, &reference, &scope(), &registry).unwrap();
    restored.validate_resume(None).unwrap();
    restored.validate_resume(Some(&supplied)).unwrap();
    assert!(
        restored
            .validate_resume(Some(&SystemInputs::default()))
            .is_err()
    );
    assert!(
        restored
            .validate_resume(Some(&inputs(json!({"workspace_id":OTHER}))))
            .is_err()
    );
    assert_eq!(restored.values(), &object(json!({"workspace_id":OWNED})));
    let other_scope = Scope {
        tenant_id: id("foreign"),
        ..scope()
    };
    assert!(RunSystemInputs::restore(&record, &reference, &other_scope, &registry).is_err());
    let mut changed_values = record.value().clone();
    *changed_values
        .get_mut("values")
        .unwrap()
        .get_mut("workspace_id")
        .unwrap() = json!(OTHER);
    let wrong_record = ProtectedRecord::new(id("snapshot"), 1, changed_values);
    assert!(RunSystemInputs::restore(&wrong_record, &reference, &scope(), &registry).is_err());
    let mut changed_definitions = fixed.definitions().values().cloned().collect::<Vec<_>>();
    changed_definitions
        .iter_mut()
        .find(|definition| definition.key == id("workspace_id"))
        .unwrap()
        .version = id("2");
    let changed_registry = SystemInputRegistry::new(changed_definitions).unwrap();
    assert!(RunSystemInputs::restore(&record, &reference, &scope(), &changed_registry).is_err());
}

#[tokio::test]
async fn binding_keeps_original_and_defaulted_model_inputs_separate_from_system_arguments() {
    let fixture=Fixture::new(descriptor(),registry(),Some(inputs(json!({"workspace_id":OWNED,"query":"system query must not overwrite","user_id":"extra-registered-value"})))).await;
    let original = object(json!({"query":"model query"}));
    fixture.plan("call", original.clone()).await;
    let bound = fixture.bind("call", None).await.unwrap();
    assert_eq!(bound.input.original_model_inputs(), &original);
    assert_eq!(
        bound.input.normalized_model_inputs(),
        &object(json!({"query":"model query","limit":10}))
    );
    assert_eq!(
        bound.input.execution_args(),
        &object(json!({"query":"model query","limit":10,"workspace_id":OWNED}))
    );
    assert_eq!(bound.decision, PolicyDecision::Allow {});
    assert_eq!(
        bound.input.system_inputs()["workspace_id"].definition_version,
        id("1")
    );
    assert_eq!(
        bound.input.system_inputs()["workspace_id"]
            .resolved
            .as_ref()
            .unwrap()
            .revision,
        id("7")
    );
    let saved = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    assert_eq!(saved.tool_ledger[0].call.model_inputs, original);
    assert_eq!(
        saved.tool_ledger[0].call.bound_input_ref.as_ref(),
        Some(&bound.reference)
    );
    assert_eq!(saved.usage.tool_attempts, 0);
    assert_eq!(
        fixture.policy.executions.lock().unwrap().as_slice(),
        &[object(
            json!({"query":"model query","limit":10,"workspace_id":OWNED})
        )]
    );
}

#[tokio::test]
async fn a_model_supplied_hidden_uuid_is_rejected_even_when_it_matches_the_saved_value() {
    let fixture = Fixture::new(
        descriptor(),
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture
        .plan("call", object(json!({"query":"x","workspace_id":OWNED})))
        .await;
    let before = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    assert!(fixture.bind("call", None).await.is_err());
    assert!(fixture.policy.executions.lock().unwrap().is_empty());
    assert_eq!(
        fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot,
        before
    );
}

#[tokio::test]
async fn a_missing_required_system_value_is_not_filled_from_defaults_or_generated_ids() {
    let mut tool = descriptor();
    tool.input_schema["properties"]["workspace_id"]["default"] = json!(OWNED);
    tool.system_bindings = Some(BTreeMap::from([(
        "workspace_id".into(),
        id("active_workspace_id"),
    )]));
    let registry = SystemInputRegistry::new(vec![definition(
        "active_workspace_id",
        json!({"type":"string","format":"uuid"}),
    )])
    .unwrap();
    let fixture = Fixture::new(tool, registry, None).await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let error = fixture.bind("call", None).await.unwrap_err();
    assert_eq!(error.code, ErrorCode::SystemInputMissing);
    assert_eq!(error.path, r#"system_inputs["active_workspace_id"]"#);
    let saved = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    assert!(saved.tool_ledger[0].call.bound_input_ref.is_none());
    assert!(fixture.policy.executions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn optional_missing_system_inputs_are_omitted_and_explicit_null_follows_the_schema() {
    for supplied in [None, Some(inputs(json!({"workspace_id":null})))] {
        let mut tool = descriptor();
        tool.input_schema["required"] = json!(["query"]);
        tool.input_schema["properties"]["workspace_id"]["type"] = json!(["string", "null"]);
        let registry = SystemInputRegistry::new(vec![definition(
            "workspace_id",
            json!({"type":["string","null"],"format":"uuid"}),
        )])
        .unwrap();
        let is_null = supplied.is_some();
        let fixture = Fixture::new(tool, registry, supplied).await;
        fixture.plan("call", object(json!({"query":"x"}))).await;
        let bound = fixture.bind("call", None).await.unwrap();
        if is_null {
            assert_eq!(
                bound.input.execution_args().get("workspace_id"),
                Some(&Value::Null)
            );
        } else {
            assert!(!bound.input.execution_args().contains_key("workspace_id"));
        }
    }
}

#[tokio::test]
async fn model_defaults_follow_local_references_but_do_not_invent_nested_fields() {
    let mut tool = descriptor();
    tool.input_schema["properties"]["limit"] = json!({"$ref":"#/$defs/Limit"});
    tool.input_schema["$defs"] = json!({"Limit":{"type":"integer","minimum":1,"default":7}});
    tool.input_schema["properties"]["query"] = json!({"type":"object","properties":{"sort":{"type":"string","default":"descending"}},"additionalProperties":false});
    let fixture = Fixture::new(
        tool,
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture.plan("call", object(json!({"query":{}}))).await;
    let bound = fixture.bind("call", None).await.unwrap();
    assert_eq!(
        bound.input.original_model_inputs(),
        &object(json!({"query":{}}))
    );
    assert_eq!(
        bound.input.normalized_model_inputs(),
        &object(json!({"query":{},"limit":7}))
    );
}

#[tokio::test]
async fn a_valid_foreign_uuid_is_denied_using_the_actual_bound_target() {
    for target in [OWNED, OTHER] {
        let fixture = Fixture::new(
            descriptor(),
            registry(),
            Some(inputs(json!({"workspace_id":target}))),
        )
        .await;
        fixture.policy.mode.store(1, Ordering::SeqCst);
        fixture.plan("call", object(json!({"query":"x"}))).await;
        let bound = fixture.bind("call", None).await;
        assert_eq!(bound.is_ok(), target == OWNED);
        assert_eq!(
            fixture.policy.executions.lock().unwrap()[0]["workspace_id"],
            json!(target)
        );
        let saved = fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot;
        assert_eq!(
            saved.tool_ledger[0].call.bound_input_ref.is_some(),
            target == OWNED
        );
    }
}

#[tokio::test]
async fn one_resolver_key_is_resolved_once_per_binding_and_cached_calls_keep_the_old_target() {
    let mut tool = resolver_descriptor();
    tool.input_schema["properties"]["owner_workspace_id"] =
        json!({"type":"string","format":"uuid"});
    tool.input_schema["required"] = json!(["query", "workspace_id", "owner_workspace_id"]);
    tool.system_bindings
        .as_mut()
        .unwrap()
        .insert("owner_workspace_id".into(), id("current_workspace"));
    let fixture = Fixture::new(tool, resolver_registry(), None).await;
    fixture.plan("first", object(json!({"query":"x"}))).await;
    let resolver = Arc::new(Resolver::new(json!(OWNED)));
    let first = fixture.bind("first", Some(resolver.clone())).await.unwrap();
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(first.input.execution_args()["workspace_id"], json!(OWNED));
    assert_eq!(
        first.input.system_inputs()["workspace_id"]
            .resolved
            .as_ref()
            .unwrap()
            .revision,
        id("revision-1")
    );
    assert_eq!(
        first.input.execution_args()["owner_workspace_id"],
        json!(OWNED)
    );
    resolver.set(json!(OTHER), "revision-2");
    let cached = fixture.bind("first", Some(resolver.clone())).await.unwrap();
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(cached.reference, first.reference);
    assert_eq!(cached.input.binding_digest(), first.input.binding_digest());
    assert_eq!(cached.input.execution_args()["workspace_id"], json!(OWNED));
    fixture.plan("second", object(json!({"query":"x"}))).await;
    let second = fixture
        .bind("second", Some(resolver.clone()))
        .await
        .unwrap();
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 2);
    assert_eq!(second.input.execution_args()["workspace_id"], json!(OTHER));
    assert_eq!(
        second.input.system_inputs()["workspace_id"]
            .resolved
            .as_ref()
            .unwrap()
            .revision,
        id("revision-2")
    );
    assert_ne!(second.input.binding_digest(), first.input.binding_digest());
}

#[tokio::test]
async fn cached_bindings_still_recheck_current_permission_without_resolving_again() {
    let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let resolver = Arc::new(Resolver::new(json!(OWNED)));
    let first = fixture.bind("call", Some(resolver.clone())).await.unwrap();
    fixture.policy.mode.store(4, Ordering::SeqCst);
    assert!(fixture.bind("call", Some(resolver.clone())).await.is_err());
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.policy.executions.lock().unwrap().len(), 2);
    assert_eq!(
        fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .tool_ledger[0]
            .call
            .bound_input_ref
            .as_ref(),
        Some(&first.reference)
    );
}

#[tokio::test]
async fn lookup_denial_or_approval_blocks_resolution_while_execution_approval_saves_a_fixed_candidate()
 {
    for mode in [2, 3, 5] {
        let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
        fixture.plan("call", object(json!({"query":"x"}))).await;
        fixture.policy.mode.store(mode, Ordering::SeqCst);
        let resolver = Arc::new(Resolver::new(json!(OWNED)));
        let result = fixture.bind("call", Some(resolver.clone())).await;
        let saved = fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot;
        if mode == 5 {
            let binding = result.unwrap();
            assert!(matches!(
                binding.decision,
                PolicyDecision::RequireApproval { .. }
            ));
            assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
            assert!(saved.tool_ledger[0].call.bound_input_ref.is_some());
        } else {
            assert!(result.is_err());
            assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
            assert!(saved.tool_ledger[0].call.bound_input_ref.is_none());
        }
    }
}

#[tokio::test]
async fn cancellation_deadline_and_lease_loss_block_new_resolver_calls() {
    for stop in [
        ErrorCode::Cancelled,
        ErrorCode::DeadlineExceeded,
        ErrorCode::LeaseLost,
    ] {
        let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
        fixture.plan("call", object(json!({"query":"x"}))).await;
        let budget = fixture.budget(fixture.store.clone()).await;
        match stop {
            ErrorCode::Cancelled => fixture.context.cancellation.cancel(),
            ErrorCode::DeadlineExceeded => fixture.clock.advance(10_000),
            ErrorCode::LeaseLost => fixture
                .store
                .release_lease(&scope(), &id("run"), &fixture.lease, 0)
                .await
                .unwrap(),
            _ => unreachable!(),
        }
        let resolver = Arc::new(Resolver::new(json!(OWNED)));
        let result = fixture
            .binder(Some(resolver.clone()))
            .bind(&fixture.tool, &id("call"), &fixture.context, &budget)
            .await;
        assert_eq!(result.unwrap_err().code, stop);
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
        assert!(
            fixture
                .store
                .load(&scope(), &id("run"))
                .await
                .unwrap()
                .snapshot
                .tool_ledger[0]
                .call
                .bound_input_ref
                .is_none()
        );
    }
}

#[tokio::test]
async fn a_stop_during_resolution_prevents_binding_persistence_and_final_authorization() {
    for cancel in [true, false] {
        let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
        fixture.plan("call", object(json!({"query":"x"}))).await;
        let resolver = Arc::new(Resolver::new(json!(OWNED)));
        if cancel {
            *resolver.cancel.lock().unwrap() = Some(fixture.context.cancellation.clone());
        } else {
            *resolver.advance.lock().unwrap() = Some(fixture.clock.clone());
        }
        let result = fixture.bind("call", Some(resolver.clone())).await;
        assert!(result.is_err());
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
        assert!(fixture.policy.executions.lock().unwrap().is_empty());
        assert!(
            fixture
                .store
                .load(&scope(), &id("run"))
                .await
                .unwrap()
                .snapshot
                .tool_ledger[0]
                .call
                .bound_input_ref
                .is_none()
        );
    }
}

struct FailingCommitStore {
    inner: Arc<MemoryStateStore>,
    commits: AtomicUsize,
    persist_first: bool,
}
impl StateStore for FailingCommitStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
        request_id: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        self.inner.find_request(scope, session_id, request_id)
    }
    fn admit<'a>(&'a self, s: &'a Scope, input: AdmissionInput) -> PortFuture<'a, AdmissionResult> {
        self.inner.admit(s, input)
    }
    fn load<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, StoredRun> {
        self.inner.load(s, r)
    }
    fn load_session<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, SessionSnapshot> {
        self.inner.load_session(s, r)
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
    fn check_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.check_lease(s, r, l, n)
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
        a: u64,
        n: usize,
    ) -> PortFuture<'a, EventPage> {
        self.inner.read_events(s, r, a, n)
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
        scope: &'a Scope,
        run_id: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            self.commits.fetch_add(1, Ordering::SeqCst);
            if self.persist_first {
                self.inner.commit(scope, run_id, input).await?;
            }
            Err(ContractError::new(
                ErrorCode::PersistenceUnavailable,
                "binding.commit",
            ))
        })
    }
}

#[tokio::test]
async fn failed_storage_does_not_return_a_ready_binding_or_change_the_planned_call() {
    let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let original = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    let store = Arc::new(FailingCommitStore {
        inner: fixture.store.clone(),
        commits: AtomicUsize::new(0),
        persist_first: false,
    });
    let resolver = Arc::new(Resolver::new(json!(OWNED)));
    let result = fixture
        .binder(Some(resolver.clone()))
        .bind(
            &fixture.tool,
            &id("call"),
            &fixture.context,
            &fixture.budget(store.clone()).await,
        )
        .await;
    assert_eq!(result.unwrap_err().code, ErrorCode::PersistenceUnavailable);
    assert_eq!(store.commits.load(Ordering::SeqCst), 1);
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot,
        original
    );
}

#[tokio::test]
async fn a_lost_commit_ack_reuses_the_saved_binding_without_resolving_the_new_value() {
    let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let store = Arc::new(FailingCommitStore {
        inner: fixture.store.clone(),
        commits: AtomicUsize::new(0),
        persist_first: true,
    });
    let resolver = Arc::new(Resolver::new(json!(OWNED)));
    let first = fixture
        .binder(Some(resolver.clone()))
        .bind(
            &fixture.tool,
            &id("call"),
            &fixture.context,
            &fixture.budget(store.clone()).await,
        )
        .await;
    assert_eq!(first.unwrap_err().code, ErrorCode::PersistenceUnavailable);
    let saved = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    let call = &saved.tool_ledger[0].call;
    let reference = call
        .bound_input_ref
        .clone()
        .expect("commit applied despite the lost acknowledgement");
    let record = fixture
        .store
        .read_record(&scope(), &reference)
        .await
        .unwrap();
    let committed = BoundToolInput::restore(
        &record,
        &fixture.tool,
        &scope(),
        &id("run"),
        call,
        saved.system_inputs.as_ref(),
    )
    .unwrap();
    assert_eq!(committed.execution_args()["workspace_id"], json!(OWNED));
    assert_eq!(
        committed.system_inputs()["workspace_id"]
            .resolved
            .as_ref()
            .unwrap()
            .revision,
        id("revision-1")
    );

    resolver.set(json!(OTHER), "revision-2");
    let retried = fixture.bind("call", Some(resolver.clone())).await.unwrap();
    assert_eq!(retried.reference, reference);
    assert_eq!(retried.input.binding_digest(), committed.binding_digest());
    assert_eq!(retried.input.execution_args()["workspace_id"], json!(OWNED));
    assert_eq!(
        retried.input.system_inputs()["workspace_id"]
            .resolved
            .as_ref()
            .unwrap()
            .revision,
        id("revision-1")
    );
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.commits.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .revision,
        saved.revision
    );
}

#[tokio::test]
async fn restored_bound_inputs_require_the_original_record_call_scope_and_run() {
    let fixture = Fixture::new(
        descriptor(),
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let bound = fixture.bind("call", None).await.unwrap();
    let record = fixture
        .store
        .read_record(&scope(), &bound.reference)
        .await
        .unwrap();
    let snapshot = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    let call = &snapshot.tool_ledger[0].call;
    let restored = BoundToolInput::restore(
        &record,
        &fixture.tool,
        &scope(),
        &id("run"),
        call,
        snapshot.system_inputs.as_ref(),
    )
    .unwrap();
    assert_eq!(restored.execution_args(), bound.input.execution_args());
    for (record_id, revision) in [
        (id("another-record"), record.reference().revision),
        (
            record.reference().record_id.clone(),
            record.reference().revision + 1,
        ),
    ] {
        let relocated = ProtectedRecord::new(record_id, revision, record.value().clone());
        assert!(
            BoundToolInput::restore(
                &relocated,
                &fixture.tool,
                &scope(),
                &id("run"),
                call,
                snapshot.system_inputs.as_ref()
            )
            .is_err()
        );
    }
    let foreign = Scope {
        tenant_id: id("foreign"),
        ..scope()
    };
    assert!(
        BoundToolInput::restore(
            &record,
            &fixture.tool,
            &foreign,
            &id("run"),
            call,
            snapshot.system_inputs.as_ref()
        )
        .is_err()
    );
    assert!(
        BoundToolInput::restore(
            &record,
            &fixture.tool,
            &scope(),
            &id("another-run"),
            call,
            snapshot.system_inputs.as_ref()
        )
        .is_err()
    );
    let mut changed_call = call.clone();
    changed_call.model_inputs = object(json!({"query":"changed"}));
    assert!(
        BoundToolInput::restore(
            &record,
            &fixture.tool,
            &scope(),
            &id("run"),
            &changed_call,
            snapshot.system_inputs.as_ref()
        )
        .is_err()
    );
    let mut changed_values = record.value().clone();
    let data = changed_values.get_mut("data").unwrap();
    *data
        .get_mut("execution_args")
        .unwrap()
        .get_mut("workspace_id")
        .unwrap() = json!(OTHER);
    *data
        .get_mut("system_inputs")
        .unwrap()
        .get_mut("workspace_id")
        .unwrap()
        .get_mut("resolved")
        .unwrap()
        .get_mut("value")
        .unwrap() = json!(OTHER);
    let tampered = ProtectedRecord::new(
        record.reference().record_id.clone(),
        record.reference().revision,
        changed_values,
    );
    assert!(
        BoundToolInput::restore(
            &tampered,
            &fixture.tool,
            &scope(),
            &id("run"),
            call,
            snapshot.system_inputs.as_ref()
        )
        .is_err()
    );
    let mut changed_reference = snapshot.system_inputs.clone().unwrap();
    changed_reference.values_digest = canonical_digest(&json!({"workspace_id":OTHER}));
    assert!(
        BoundToolInput::restore(
            &record,
            &fixture.tool,
            &scope(),
            &id("run"),
            call,
            Some(&changed_reference)
        )
        .is_err()
    );
}

#[tokio::test]
async fn only_direct_optional_defaults_are_applied_not_conditional_or_required_model_defaults() {
    let mut required_default = descriptor();
    required_default.input_schema["required"] = json!(["query", "limit", "workspace_id"]);
    let fixture = Fixture::new(
        required_default,
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    assert!(fixture.bind("call", None).await.is_err());

    let mut conditional = descriptor();
    conditional.agent_parameters.push("workspace_id".into());
    conditional.input_schema["properties"]["limit"]
        .as_object_mut()
        .unwrap()
        .remove("default");
    conditional.input_schema["if"] = json!({"properties":{"query":{"const":"strict"}}});
    conditional.input_schema["then"] = json!({"properties":{"limit":{"default":99}}});
    let fixture = Fixture::new(conditional, SystemInputRegistry::new(vec![]).unwrap(), None).await;
    let original = object(json!({"query":"strict","workspace_id":OWNED}));
    fixture.plan("call", original.clone()).await;
    let bound = fixture.bind("call", None).await.unwrap();
    assert_eq!(bound.input.normalized_model_inputs(), &original);
}

#[tokio::test]
async fn an_explicit_nullable_model_value_is_not_replaced_by_its_default() {
    let mut tool = descriptor();
    tool.input_schema["properties"]["limit"]["type"] = json!(["integer", "null"]);
    let fixture = Fixture::new(
        tool,
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture
        .plan("call", object(json!({"query":"x","limit":null})))
        .await;
    let bound = fixture.bind("call", None).await.unwrap();
    assert_eq!(bound.input.original_model_inputs()["limit"], Value::Null);
    assert_eq!(bound.input.normalized_model_inputs()["limit"], Value::Null);
    assert_eq!(bound.input.execution_args()["limit"], Value::Null);
}

#[tokio::test]
async fn zero_resolver_capacity_and_small_value_bounds_stop_before_unsafe_progress() {
    let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let resolver = Arc::new(Resolver::new(json!(OWNED)));
    let binder = fixture
        .binder(Some(resolver.clone()))
        .with_limits(InputBindingLimits {
            max_resolver_calls: 0,
            ..InputBindingLimits::default()
        })
        .unwrap();
    let budget = fixture.budget(fixture.store.clone()).await;
    assert!(
        binder
            .bind(&fixture.tool, &id("call"), &fixture.context, &budget)
            .await
            .is_err()
    );
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);

    let fixture = Fixture::new(
        descriptor(),
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let binder = fixture
        .binder(None)
        .with_limits(InputBindingLimits {
            max_value_bytes: 4,
            ..InputBindingLimits::default()
        })
        .unwrap();
    assert!(
        binder
            .bind(
                &fixture.tool,
                &id("call"),
                &fixture.context,
                &fixture.budget(fixture.store.clone()).await
            )
            .await
            .is_err()
    );
    assert!(fixture.policy.executions.lock().unwrap().is_empty());
    assert!(
        fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .tool_ledger[0]
            .call
            .bound_input_ref
            .is_none()
    );
}
```

## `crates/wickle/tests/state.rs`

```rust
//! Atomic admission, persistence, leases, and scope isolation of the memory store.

use serde_json::json;
use std::{collections::BTreeSet, sync::Arc};
use wickle::*;

mod support;
use support::*;

#[tokio::test]
async fn request_lookup_uses_scope_and_session_and_returns_current_state_after_restore() {
    let store = MemoryStateStore::new();
    assert!(
        store
            .find_request(&scope(), &id("first"), &id("request"))
            .await
            .unwrap()
            .is_none()
    );
    let first = store
        .admit(
            &scope(),
            admission("run-a", "request", "first", "input", "1").await,
        )
        .await
        .unwrap()
        .state;
    store
        .admit(
            &scope(),
            admission("run-b", "request", "second", "input", "1").await,
        )
        .await
        .unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run-a"), &id("worker"), 0, 1000)
        .await
        .unwrap();
    let finished = store
        .commit(&scope(), &id("run-a"), finished(&first.snapshot, lease, 1))
        .await
        .unwrap();
    assert_eq!(
        store
            .find_request(&scope(), &id("first"), &id("request"))
            .await
            .unwrap(),
        Some(finished)
    );
    let checkpoint = store.export_checkpoint(&scope()).unwrap();
    let restored = MemoryStateStore::from_checkpoint(
        StateStoreCheckpoint::from_json(
            &serde_json::to_string(&checkpoint).unwrap(),
            &scope(),
            &checkpoint.digest(),
        )
        .unwrap(),
    );
    let first = restored
        .find_request(&scope(), &id("first"), &id("request"))
        .await
        .unwrap()
        .unwrap();
    let second = restored
        .find_request(&scope(), &id("second"), &id("request"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.snapshot.status, RunStatus::Succeeded);
    assert_eq!(second.snapshot.run_id, id("run-b"));
    let foreign = Scope {
        workspace_id: id("foreign"),
        ..scope()
    };
    assert!(
        restored
            .find_request(&foreign, &id("first"), &id("request"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        restored
            .find_request(&scope(), &id("missing"), &id("request"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn admitted_model_options_are_fixed_for_replay_and_later_commits() {
    fn with_effort(mut input: AdmissionInput, effort: &str) -> AdmissionInput {
        input.snapshot.request.model_options =
            JsonObject::from([("reasoning_effort".into(), json!(effort))]);
        input.snapshot.request_digest =
            admission_digest(&input.snapshot.request, &input.snapshot.profile, None);
        let RunEventPayload::RunStarted { request_ref, .. } = &mut input.events[0].payload else {
            unreachable!()
        };
        let record = ProtectedRecord::new(
            request_ref.record_id.clone(),
            request_ref.revision,
            serde_json::to_value(&input.snapshot.request).unwrap(),
        );
        let old_ref = request_ref.clone();
        *request_ref = record.reference().clone();
        *input
            .records
            .iter_mut()
            .find(|record| record.reference() == &old_ref)
            .unwrap() = record;
        input
    }
    let store = MemoryStateStore::new();
    let first = with_effort(
        admission("run", "request", "session", "input", "1").await,
        "high",
    );
    let expected_options = first.snapshot.request.model_options.clone();
    let original = store.admit(&scope(), first).await.unwrap().state;
    let replay = with_effort(
        admission("replacement", "request", "session", "input", "2").await,
        "high",
    );
    let replay = store.admit(&scope(), replay).await.unwrap();
    assert!(!replay.created);
    assert_eq!(
        replay.state.snapshot.request.model_options,
        expected_options
    );
    let changed = with_effort(
        admission("replacement", "request", "session", "input", "2").await,
        "low",
    );
    assert_ne!(
        changed.snapshot.request_digest,
        original.snapshot.request_digest
    );
    assert_eq!(
        store.admit(&scope(), changed).await.unwrap_err().code,
        ErrorCode::RequestConflict
    );

    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("worker"), 1, 100)
        .await
        .unwrap();
    let mut change = prepared(&original.snapshot, lease, 2);
    change
        .snapshot
        .request
        .model_options
        .insert("reasoning_effort".into(), json!("low"));
    change.snapshot.request_digest =
        admission_digest(&change.snapshot.request, &change.snapshot.profile, None);
    assert_eq!(
        store
            .commit(&scope(), &id("run"), change)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidTransition
    );
    let saved = store.load(&scope(), &id("run")).await.unwrap().snapshot;
    let restored = RunSnapshot::from_json(&serde_json::to_string(&saved).unwrap()).unwrap();
    assert_eq!(restored.request.model_options, expected_options);
    assert_eq!(restored.request_digest, original.snapshot.request_digest);
}

#[tokio::test]
async fn identical_retries_return_the_original_run_without_replacing_resolved_metadata() {
    let store = MemoryStateStore::new();
    let first = admission("run-a", "request", "session", "input", "1").await;
    let receipt = store.admit(&scope(), first.clone()).await.unwrap();
    assert!(receipt.created);
    let retry = admission("run-b", "request", "session", "input", "2").await;
    let replay = store.admit(&scope(), retry).await.unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state.snapshot.run_id, id("run-a"));
    assert_eq!(
        replay.state.snapshot.profile.resolution_digest(),
        first.snapshot.profile.resolution_digest()
    );
    assert_eq!(replay.state.messages.len(), 1);
    let changed = admission("run-c", "request", "session", "different input", "1").await;
    assert!(store.admit(&scope(), changed).await.is_err());
    assert_eq!(
        store
            .load(&scope(), &id("run-a"))
            .await
            .unwrap()
            .snapshot
            .revision,
        0
    );
    let events = store
        .read_events(&scope(), &id("run-a"), 0, 100)
        .await
        .unwrap();
    assert_eq!(events.events.len(), 1);
}

#[tokio::test]
async fn concurrent_duplicate_admission_creates_exactly_one_run() {
    let store = Arc::new(MemoryStateStore::new());
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut handles = vec![];
    for n in 0..8 {
        let input = admission(&format!("run-{n}"), "request", "session", "input", "1").await;
        let store = store.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            store.admit(&scope(), input).await.unwrap()
        }));
    }
    let mut created = 0;
    let mut ids = BTreeSet::new();
    for h in handles {
        let result = h.await.unwrap();
        created += usize::from(result.created);
        ids.insert(result.state.snapshot.run_id);
    }
    assert_eq!(created, 1);
    assert_eq!(ids.len(), 1);
}

#[tokio::test]
async fn distinct_concurrent_requests_create_only_one_active_run_in_the_session() {
    let store = Arc::new(MemoryStateStore::new());
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut handles = Vec::new();
    for n in 0..8 {
        let input = admission(
            &format!("run-{n}"),
            &format!("request-{n}"),
            "session",
            "input",
            "1",
        )
        .await;
        let store = store.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            store.admit(&scope(), input).await
        }));
    }
    let mut accepted = Vec::new();
    let mut rejected = 0;
    for handle in handles {
        match handle.await.unwrap() {
            Ok(result) => accepted.push(result.state.snapshot.run_id),
            Err(_) => rejected += 1,
        }
    }
    assert_eq!(accepted.len(), 1);
    assert_eq!(rejected, 7);
    assert_eq!(
        store
            .load_session(&scope(), &id("session"))
            .await
            .unwrap()
            .active_run_id
            .as_ref(),
        accepted.first()
    );
}

#[tokio::test]
async fn waiting_keeps_the_session_busy_even_after_the_worker_releases_its_lease() {
    let store = MemoryStateStore::new();
    store
        .admit(
            &scope(),
            admission("run", "request", "session", "input", "1").await,
        )
        .await
        .unwrap();
    let state = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut update = prepared(&state.snapshot, lease.clone(), 101);
    let wait = WaitState {
        wait_id: id("wait"),
        target: WaitTarget::Input {
            request: InputRequest {
                input_request_id: id("input-request"),
                call_id: id("input-call"),
                question: "Choose a source".into(),
                schema_ref: None,
            },
        },
        expires_at_ms: Some(1000),
    };
    let record = ProtectedRecord::new(id("wait-record"), 1, serde_json::to_value(&wait).unwrap());
    update.snapshot.status = RunStatus::Waiting;
    update.snapshot.phase = RunPhase::Waiting;
    update.snapshot.wait = Some(wait);
    update.snapshot.last_event_seq = 2;
    update.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        2,
        RunEventPayload::RunWaiting {
            wait_ref: record.reference().clone(),
        },
    ));
    update.records.push(record);
    store.commit(&scope(), &id("run"), update).await.unwrap();
    store
        .release_lease(&scope(), &id("run"), &lease, 102)
        .await
        .unwrap();
    assert!(
        store
            .admit(
                &scope(),
                admission("other", "other-request", "session", "other", "1").await
            )
            .await
            .is_err()
    );
    let replay = store
        .admit(
            &scope(),
            admission("replacement", "request", "session", "input", "2").await,
        )
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state.snapshot.status, RunStatus::Waiting);
}

#[tokio::test]
async fn competing_requests_cannot_share_an_active_session_and_terminal_commit_releases_it() {
    let store = MemoryStateStore::new();
    let first = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), first).await.unwrap();
    assert!(
        store
            .admit(
                &scope(),
                admission("other", "other-request", "session", "other", "1").await
            )
            .await
            .is_err()
    );
    let state = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 50)
        .await
        .unwrap();
    store
        .commit(&scope(), &id("run"), finished(&state.snapshot, lease, 101))
        .await
        .unwrap();
    assert!(
        store
            .load_session(&scope(), &id("session"))
            .await
            .unwrap()
            .active_run_id
            .is_none()
    );
    let mut second = admission("other", "other-request", "session", "other", "1").await;
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
            admission("replacement", "request", "session", "input", "2").await,
        )
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state.snapshot.run_id, id("run"));
}

#[tokio::test]
async fn lease_expiry_fencing_and_revision_conflicts_are_independent() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), input).await.unwrap();
    let first = store
        .acquire_lease(&scope(), &id("run"), &id("owner-a"), 100, 10)
        .await
        .unwrap();
    assert!(
        store
            .acquire_lease(&scope(), &id("run"), &id("owner-b"), 109, 10)
            .await
            .is_err()
    );
    assert!(
        store
            .renew_lease(&scope(), &id("run"), &first, 110, 10)
            .await
            .is_err()
    );
    let second = store
        .acquire_lease(&scope(), &id("run"), &id("owner-b"), 110, 10)
        .await
        .unwrap();
    assert!(second.fencing_token > first.fencing_token);
    let state = store.load(&scope(), &id("run")).await.unwrap();
    assert!(
        store
            .commit(
                &scope(),
                &id("run"),
                prepared(&state.snapshot, first.clone(), 111)
            )
            .await
            .is_err()
    );
    let update = prepared(&state.snapshot, second.clone(), 111);
    store
        .commit(&scope(), &id("run"), update.clone())
        .await
        .unwrap();
    assert!(store.commit(&scope(), &id("run"), update).await.is_err());
    assert!(
        store
            .renew_lease(&scope(), &id("run"), &first, 111, 20)
            .await
            .is_err()
    );
    let renewed = store
        .renew_lease(&scope(), &id("run"), &second, 119, 20)
        .await
        .unwrap();
    assert_eq!(renewed.fencing_token, second.fencing_token);
    let current = store.load(&scope(), &id("run")).await.unwrap();
    // Heartbeat renews expiry without invalidating the driver's same-generation copy.
    store
        .commit(
            &scope(),
            &id("run"),
            prepared(&current.snapshot, second.clone(), 125),
        )
        .await
        .unwrap();
    store
        .release_lease(&scope(), &id("run"), &renewed, 130)
        .await
        .unwrap();
    let third = store
        .acquire_lease(&scope(), &id("run"), &id("owner-c"), 130, 20)
        .await
        .unwrap();
    assert!(third.fencing_token > renewed.fencing_token);
    let current = store.load(&scope(), &id("run")).await.unwrap();
    let mut forged = third.clone();
    forged.expires_at_ms = i64::MAX;
    assert_eq!(
        store
            .commit(
                &scope(),
                &id("run"),
                prepared(&current.snapshot, forged, 150)
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::LeaseLost
    );
}

#[tokio::test]
async fn an_event_cannot_announce_a_wait_absent_from_the_committed_snapshot() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    let wrong_payload = input.records[0].reference().clone();
    store.admit(&scope(), input).await.unwrap();
    let before = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut update = prepared(&before.snapshot, lease, 101);
    update.snapshot.last_event_seq = 2;
    update.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        2,
        RunEventPayload::RunWaiting {
            wait_ref: wrong_payload,
        },
    ));
    assert_eq!(
        store
            .commit(&scope(), &id("run"), update)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidEvent
    );
    assert_eq!(store.load(&scope(), &id("run")).await.unwrap(), before);
    assert_eq!(
        store
            .read_events(&scope(), &id("run"), 0, 100)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
}

#[tokio::test]
async fn uncertain_tool_effects_keep_the_original_attempt_and_idempotency_key() {
    struct StationaryClock;
    impl Clock for StationaryClock {
        fn now(&self) -> Result<ClockReading, ContractError> {
            Ok(ClockReading {
                utc_ms: 102,
                monotonic_ms: 0,
            })
        }
        fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
            Box::pin(std::future::pending())
        }
    }
    struct AllowPolicy;
    impl PolicyPort for AllowPolicy {
        fn authorize<'a>(
            &'a self,
            _: &'a PolicyRequest,
            _: PolicyContext<'a>,
        ) -> PortFuture<'a, PolicyDecision> {
            Box::pin(async { Ok(PolicyDecision::Allow {}) })
        }
    }
    let store = Arc::new(MemoryStateStore::new());
    let registry = Arc::new(SystemInputRegistry::default());
    let compiled = SchemaCompiler::new().compile(ToolDescriptor {
        tool: VersionedRef { id: id("tool"), version: id("1") }, name: id("tool"), description: "Write a record".into(),
        input_schema: json!({"type":"object","properties":{},"required":[],"additionalProperties":false}), agent_parameters: vec![], system_bindings: None,
        output_schema: json!(true), side_effect: ToolSideEffect::Write, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: true, max_output_bytes: 1024.try_into().unwrap(),
    }, &registry).unwrap();
    let mut input = admission("run", "request", "session", "input", "1").await;
    let mut profile = input.snapshot.profile.profile().clone();
    profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
        tool_id: id("tool"),
        version: id("1"),
        bindings: None,
        config: None,
    }));
    input.snapshot.profile = ProfileValidator::new(&Catalog { revision: "1" })
        .validate(&profile, &scope())
        .await
        .unwrap();
    input.snapshot.request_digest =
        admission_digest(&input.snapshot.request, &input.snapshot.profile, None);
    if let RunEventPayload::RunStarted { profile_digest, .. } = &mut input.events[0].payload {
        *profile_digest = input.snapshot.profile.profile_digest().clone();
    }
    store.admit(&scope(), input).await.unwrap();
    let before = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut plan = prepared(&before.snapshot, lease.clone(), 101);
    let call = ToolCall {
        call_id: id("call"),
        model_request_id: id("model-request"),
        provider_call_id: id("provider-call"),
        tool_name: id("tool"),
        model_inputs: Default::default(),
        descriptor_digest: compiled.descriptor_digest().clone(),
        bound_input_ref: None,
    };
    let call_record =
        ProtectedRecord::new(id("planned-call"), 1, serde_json::to_value(&call).unwrap());
    plan.snapshot.phase = RunPhase::Tool;
    plan.snapshot.last_event_seq = 2;
    plan.snapshot.tool_ledger.push(ToolLedgerEntry {
        call,
        state: ToolCallState::Planned {},
    });
    plan.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        2,
        RunEventPayload::ToolPlanned {
            call_ref: call_record.reference().clone(),
        },
    ));
    plan.records.push(call_record);
    store.commit(&scope(), &id("run"), plan).await.unwrap();
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope(),
            principal_ref: id("caller"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    );
    let budget = RunBudget::attach(
        store.clone(),
        Arc::new(StationaryClock),
        Arc::new(RandomIdSource),
        scope(),
        id("run"),
        lease.clone(),
        context.cancellation.clone(),
    )
    .await
    .unwrap();
    let binder = InputBinder::new(
        registry,
        None,
        Arc::new(
            PolicyGate::new(Arc::new(AllowPolicy), std::time::Duration::from_secs(1)).unwrap(),
        ),
        Arc::new(RandomIdSource),
    );
    binder
        .bind(&compiled, &id("call"), &context, &budget)
        .await
        .unwrap();
    let reservation = budget
        .reserve(ReservationKind::Tool {
            call_id: id("call"),
        })
        .await
        .unwrap();
    let planned = store.load(&scope(), &id("run")).await.unwrap();
    let mut dispatch = prepared(&planned.snapshot, lease.clone(), 102);
    dispatch.snapshot.phase = RunPhase::Tool;
    dispatch.snapshot.tool_ledger[0].state = ToolCallState::Dispatching {
        attempt_id: reservation.attempt_id.clone(),
        idempotency_key: id("effect-key"),
    };
    let dispatched = store.commit(&scope(), &id("run"), dispatch).await.unwrap();
    let mut lost = prepared(&dispatched.snapshot, lease.clone(), 103);
    lost.snapshot.phase = RunPhase::Tool;
    lost.snapshot.tool_ledger[0].state = ToolCallState::Unknown {
        attempt_id: id("attempt-b"),
        idempotency_key: id("different-key"),
    };
    assert_eq!(
        store
            .commit(&scope(), &id("run"), lost)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidTransition
    );
    let mut lost = prepared(&dispatched.snapshot, lease, 103);
    lost.snapshot.phase = RunPhase::Tool;
    lost.snapshot.tool_ledger[0].state = ToolCallState::Unknown {
        attempt_id: reservation.attempt_id.clone(),
        idempotency_key: id("effect-key"),
    };
    let saved = store.commit(&scope(), &id("run"), lost).await.unwrap();
    assert!(
        matches!(&saved.snapshot.tool_ledger[0].state, ToolCallState::Unknown { attempt_id, idempotency_key } if attempt_id == &reservation.attempt_id && idempotency_key == &id("effect-key"))
    );
}

#[tokio::test]
async fn invalid_multi_event_commit_does_not_partially_publish_records_state_or_messages() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), input).await.unwrap();
    let before = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut update = prepared(&before.snapshot, lease, 101);
    let wait = WaitState {
        wait_id: id("new-wait"),
        target: WaitTarget::Input {
            request: InputRequest {
                input_request_id: id("input-request"),
                call_id: id("input-call"),
                question: "Choose a source".into(),
                schema_ref: None,
            },
        },
        expires_at_ms: None,
    };
    let record = ProtectedRecord::new(id("new-record"), 1, serde_json::to_value(&wait).unwrap());
    let reference = record.reference().clone();
    update.records.push(record);
    update.events = vec![
        event(
            &id("run"),
            &id("session"),
            &scope(),
            2,
            RunEventPayload::RunWaiting {
                wait_ref: reference.clone(),
            },
        ),
        event(
            &id("run"),
            &id("session"),
            &scope(),
            2,
            RunEventPayload::RunWaiting {
                wait_ref: reference.clone(),
            },
        ),
    ];
    update.snapshot.last_event_seq = 2;
    update.snapshot.status = RunStatus::Waiting;
    update.snapshot.phase = RunPhase::Waiting;
    update.snapshot.wait = Some(wait);
    let mut message = before.messages[0].clone();
    message.message_id = id("new-message");
    message.sequence = 2.try_into().unwrap();
    update.messages.push(message);
    assert!(store.commit(&scope(), &id("run"), update).await.is_err());
    let after = store.load(&scope(), &id("run")).await.unwrap();
    assert_eq!(after.snapshot, before.snapshot);
    assert_eq!(after.messages, before.messages);
    assert_eq!(after.session, before.session);
    assert!(store.read_record(&scope(), &reference).await.is_err());
    assert_eq!(
        store
            .read_events(&scope(), &id("run"), 0, 100)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
}

#[tokio::test]
async fn commits_cannot_replace_request_or_resolved_profile_and_reads_return_owned_snapshots() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), input).await.unwrap();
    let before = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut update = prepared(&before.snapshot, lease.clone(), 101);
    update.snapshot.request.input = vec![InputContent::Text {
        text: "replacement".into(),
    }];
    update.snapshot.request_digest =
        admission_digest(&update.snapshot.request, &update.snapshot.profile, None);
    assert!(store.commit(&scope(), &id("run"), update).await.is_err());
    let replacement = admission("run", "request", "session", "input", "2").await;
    let mut update = prepared(&before.snapshot, lease, 101);
    update.snapshot.profile = replacement.snapshot.profile;
    assert!(store.commit(&scope(), &id("run"), update).await.is_err());
    let mut copy = store.load(&scope(), &id("run")).await.unwrap();
    copy.messages.clear();
    copy.snapshot.request.input.clear();
    let after = store.load(&scope(), &id("run")).await.unwrap();
    assert_eq!(after.snapshot, before.snapshot);
    assert_eq!(after.messages, before.messages);
}

#[tokio::test]
async fn every_store_surface_is_scoped_and_memory_does_not_claim_durability() {
    let store = MemoryStateStore::new();
    let capabilities = store.capabilities();
    assert!(
        !capabilities.durable && !capabilities.cross_process_leases && capabilities.event_replay
    );
    let mut durable = admission(
        "durable",
        "durable-request",
        "durable-session",
        "input",
        "1",
    )
    .await;
    durable.require_durable = true;
    assert!(store.admit(&scope(), durable).await.is_err());
    let input = admission("run", "request", "session", "input", "1").await;
    let record = input.records[0].reference().clone();
    store.admit(&scope(), input).await.unwrap();
    let snapshot = store.load(&scope(), &id("run")).await.unwrap().snapshot;
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    for foreign in [
        Scope {
            tenant_id: id("other"),
            ..scope()
        },
        Scope {
            workspace_id: id("other"),
            ..scope()
        },
        Scope {
            user_id: Some(id("other")),
            ..scope()
        },
    ] {
        assert!(store.load(&foreign, &id("run")).await.is_err());
        assert!(store.load_session(&foreign, &id("session")).await.is_err());
        assert!(
            store
                .read_events(&foreign, &id("run"), 0, 100)
                .await
                .is_err()
        );
        assert!(store.read_record(&foreign, &record).await.is_err());
        assert!(
            store
                .acquire_lease(&foreign, &id("run"), &id("owner"), 101, 10)
                .await
                .is_err()
        );
        assert!(
            store
                .renew_lease(&foreign, &id("run"), &lease, 101, 10)
                .await
                .is_err()
        );
        assert!(
            store
                .commit(
                    &foreign,
                    &id("run"),
                    prepared(&snapshot, lease.clone(), 101)
                )
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn event_pages_are_exclusive_ordered_replayable_and_preserved_after_completion() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), input).await.unwrap();
    let snapshot = store.load(&scope(), &id("run")).await.unwrap().snapshot;
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    store
        .commit(&scope(), &id("run"), finished(&snapshot, lease, 101))
        .await
        .unwrap();
    let first = store.read_events(&scope(), &id("run"), 0, 1).await.unwrap();
    assert_eq!(first.events.len(), 1);
    assert!(first.has_more);
    assert_eq!(first.next_after_seq, 1);
    let second = store
        .read_events(&scope(), &id("run"), first.next_after_seq, 1)
        .await
        .unwrap();
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.events[0].seq.get(), 2);
    assert!(!second.has_more);
    assert_eq!(
        store
            .read_events(&scope(), &id("run"), 1, 100)
            .await
            .unwrap()
            .events,
        second.events
    );
    assert!(
        store
            .read_events(&scope(), &id("run"), 2, 100)
            .await
            .unwrap()
            .events
            .is_empty()
    );
    assert!(
        store
            .acquire_lease(&scope(), &id("run"), &id("owner"), 102, 100)
            .await
            .is_err()
    );
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
