# 13장 전체 Rust 구현과 테스트

[강의로](../13-sqlite.md) · [전체 변경 패치](../solutions/13-sqlite.patch)

기준 `757fc97f7ccceadbd697e4f32056a92cdf0f95df`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

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

## `crates/wickle-state-sqlite/tests/support/mod.rs`

```rust
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::stream;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use wickle::*;

use crate::core::{self, admission, event, id, scope};

pub struct Database {
    directory: PathBuf,
}
impl Database {
    pub fn new() -> Self {
        let unique = RandomIdSource.next_id().unwrap();
        let directory = std::env::temp_dir().join(format!("wickle-sqlite-test-{unique}"));
        std::fs::create_dir(&directory).unwrap();
        Self { directory }
    }
    pub fn path(&self) -> PathBuf {
        self.directory.join("state.sqlite")
    }
    pub fn file(&self, name: &str) -> PathBuf {
        self.directory.join(name)
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

pub async fn durable_admission(run: &str, request: &str, session: &str) -> AdmissionInput {
    let mut input = admission(run, request, session, "Stored request", "1").await;
    input.require_durable = true;
    input
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
struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: 0,
            monotonic_ms: 0,
        })
    }
    fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}
struct Allow;
impl PolicyPort for Allow {
    fn authorize<'a>(
        &'a self,
        _: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async { Ok(PolicyDecision::Allow {}) })
    }
}
fn reference(name: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id("1"),
    }
}
struct Model;
impl ModelPort for Model {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: id("fixture-provider"),
            adapter: reference("fixture-adapter"),
            connection_ref: reference("fixture-connection"),
        }
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        assert_eq!(request.request_id, context.attempt_id);
        Box::pin(stream::iter([
            Ok(ModelEvent::ToolArgumentsDelta {
                index: 0,
                provider_call_id: Some("provider-call".into()),
                name: Some("search".into()),
                delta: r#"{"query":"reports"}"#.into(),
            }),
            Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::ToolCalls,
                metadata: ModelResponseMetadata {
                    provider_request_id: Some(id("reported-request")),
                    reported_model_id: Some(id("reported-model")),
                    reported_model_version: None,
                    usage: Some(ModelUsage {
                        measurement: UsageMeasurement::Reported,
                        input_tokens: Some(7),
                        output_tokens: Some(3),
                    }),
                },
                continuation: vec![],
            }),
        ]))
    }
}

/// Use real model/binder boundaries to create historical records whose state later changes.
pub async fn populate_protected_run(store: Arc<dyn StateStore>) -> Value {
    let registry = Arc::new(
        SystemInputRegistry::new(vec![SystemInputDefinition {
            key: id("workspace_id"),
            version: id("definition-1"),
            value_schema: json!({"type":"string","format":"uuid"}),
            source: SystemInputSource::Run {},
        }])
        .unwrap(),
    );
    let compiled = SchemaCompiler::new().compile(ToolDescriptor {
        tool: reference("search"), name: id("search"), description: "Search reports".into(),
        input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","default":10},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters: vec!["query".into(), "limit".into()], system_bindings: None,
        output_schema: json!({"type":"string"}), side_effect: ToolSideEffect::ReadOnly,
        concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false,
        max_output_bytes: 1024.try_into().unwrap(),
    }, &registry).unwrap();
    let mut input = durable_admission("run", "request", "session").await;
    let mut profile = serde_json::to_value(input.snapshot.profile.profile()).unwrap();
    profile["tools"] = json!([{"tool_id":"search","version":"1"}]);
    input.snapshot.profile = ProfileValidator::new(&core::Catalog { revision: "1" })
        .validate(
            &AgentProfile::from_json(&profile.to_string()).unwrap(),
            &scope(),
        )
        .await
        .unwrap();
    let values = SystemInputs::new(
        [(
            "workspace_id".into(),
            json!("11111111-1111-4111-8111-111111111111"),
        )]
        .into_iter()
        .collect(),
    );
    let captured = RunSystemInputs::capture(scope(), Some(values), &registry).unwrap();
    let run_record = captured.to_record(id("run-inputs"), 7);
    input.snapshot.system_inputs = Some(captured.snapshot_ref(run_record.reference()).unwrap());
    input.snapshot.request_digest = admission_digest(
        &input.snapshot.request,
        &input.snapshot.profile,
        input.snapshot.system_inputs.as_ref(),
    );
    if let RunEventPayload::RunStarted { profile_digest, .. } = &mut input.events[0].payload {
        *profile_digest = input.snapshot.profile.profile_digest().clone();
    }
    input.records.push(run_record);
    store.admit(&scope(), input).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("worker"), 0, 20_000)
        .await
        .unwrap();
    let cancellation = CancellationToken::new();
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope(),
            principal_ref: id("principal"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        cancellation.clone(),
    );
    let ids = Arc::new(FixedIds::default());
    let budget = RunBudget::attach(
        store.clone(),
        Arc::new(FixedClock),
        ids.clone(),
        scope(),
        id("run"),
        lease.clone(),
        cancellation,
    )
    .await
    .unwrap();
    let policy = Arc::new(PolicyGate::new(Arc::new(Allow), Duration::from_secs(1)).unwrap());
    let binding = Model.binding();
    let request = ModelRequest {
        options: JsonObject::new(),
        request_id: id("step"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference("primary"),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id("requested-model"),
            model_id: id("resolved-model"),
            model_version: id("resolved-release"),
            version_semantics: VersionSemantics::Pinned,
            provider: binding.provider,
            target: JsonObject::new(),
            deployment_revision: None,
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("v1"),
            },
            adapter: binding.adapter,
            capability_revision: id("capability-1"),
            connection_ref: binding.connection_ref,
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find reports".into(),
            }],
        }],
        tools: vec![compiled.to_model_tool()],
        output: ModelOutput::Text {},
        max_output_tokens: 64.try_into().unwrap(),
        limits: ModelResponseLimits {
            max_input_bytes: 16384,
            max_response_bytes: 4096,
            max_delta_bytes: 1024,
            max_events: 4,
            max_tool_calls: 1,
        },
    };
    let Guarded::Completed(ModelExchangeOutcome::Completed { response }) =
        ModelExchange::new(Arc::new(Model), policy.clone())
            .generate(&request, &context, &budget)
            .await
            .unwrap()
    else {
        panic!("fixture model was not completed");
    };
    let proposal = &response.tool_calls[0];
    let call = ToolCall {
        call_id: id("call"),
        model_request_id: response.request_id.clone(),
        provider_call_id: proposal.provider_call_id.clone(),
        tool_name: proposal.name.clone(),
        model_inputs: proposal.model_inputs.clone(),
        descriptor_digest: compiled.descriptor_digest().clone(),
        bound_input_ref: None,
    };
    let planned = ProtectedRecord::new(id("planned-call"), 1, serde_json::to_value(&call).unwrap());
    let saved = store.load(&scope(), &id("run")).await.unwrap();
    let mut update = core::prepared(&saved.snapshot, lease.clone(), 0);
    update.snapshot.phase = RunPhase::Tool;
    update.snapshot.tool_ledger.push(ToolLedgerEntry {
        call: call.clone(),
        state: ToolCallState::Planned {},
    });
    update.snapshot.last_event_seq += 1;
    update.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        update.snapshot.last_event_seq,
        RunEventPayload::ToolPlanned {
            call_ref: planned.reference().clone(),
        },
    ));
    update.messages.push(Message {
        message_id: id("call-message"),
        run_id: id("run"),
        sequence: 2.try_into().unwrap(),
        role: MessageRole::Assistant,
        origin: MessageOrigin::Model,
        visibility: Visibility::UserAndModel,
        content: vec![ContentBlock::ToolCall { call: call.clone() }],
    });
    update.records.push(planned);
    store.commit(&scope(), &id("run"), update).await.unwrap();
    let bound = InputBinder::new(registry, None, policy, ids)
        .bind(&compiled, &id("call"), &context, &budget)
        .await
        .unwrap();
    let attempt = budget
        .reserve(ReservationKind::Tool {
            call_id: id("call"),
        })
        .await
        .unwrap();
    let saved = store.load(&scope(), &id("run")).await.unwrap();
    let mut dispatch = core::prepared(&saved.snapshot, lease.clone(), 0);
    dispatch.snapshot.phase = RunPhase::Tool;
    dispatch.snapshot.tool_ledger[0].state = ToolCallState::Dispatching {
        attempt_id: attempt.attempt_id,
        idempotency_key: id("effect-key"),
    };
    store.commit(&scope(), &id("run"), dispatch).await.unwrap();
    let receipt = ProtectedRecord::new(id("receipt"), 1, json!({"fixture_effect":"completed"}));
    let tool_result = ToolResult {
        call_id: id("call"),
        call_message_id: id("call-message"),
        status: ToolResultStatus::Succeeded,
        content: vec![InputContent::Text {
            text: "Observed reports".into(),
        }],
        effect_receipt_ref: Some(receipt.reference().clone()),
        error: None,
    };
    let result_record = ProtectedRecord::new(
        id("tool-result"),
        1,
        serde_json::to_value(&tool_result).unwrap(),
    );
    let saved = store.load(&scope(), &id("run")).await.unwrap();
    let mut settled = core::prepared(&saved.snapshot, lease.clone(), 0);
    settled.snapshot.tool_ledger[0].state = ToolCallState::Settled {
        result: tool_result.clone(),
    };
    settled.snapshot.last_event_seq += 1;
    settled.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        settled.snapshot.last_event_seq,
        RunEventPayload::ToolSettled {
            result_ref: result_record.reference().clone(),
        },
    ));
    settled.messages.push(Message {
        message_id: id("result-message"),
        run_id: id("run"),
        sequence: 3.try_into().unwrap(),
        role: MessageRole::Tool,
        origin: MessageOrigin::Tool,
        visibility: Visibility::UserAndModel,
        content: vec![ContentBlock::ToolResult {
            result: tool_result,
        }],
    });
    settled.records.extend([receipt.clone(), result_record]);
    store.commit(&scope(), &id("run"), settled).await.unwrap();
    let saved = store.load(&scope(), &id("run")).await.unwrap();
    store
        .commit(
            &scope(),
            &id("run"),
            core::finished(&saved.snapshot, lease, 0),
        )
        .await
        .unwrap();
    let saved = store.load(&scope(), &id("run")).await.unwrap();
    let references = [
        saved
            .snapshot
            .system_inputs
            .as_ref()
            .unwrap()
            .snapshot_ref
            .clone(),
        bound.reference,
        saved.snapshot.model_ledger[0].response_ref.clone().unwrap(),
        receipt.reference().clone(),
    ];
    let mut records = Vec::new();
    for reference in references {
        let record = store.read_record(&scope(), &reference).await.unwrap();
        records.push(json!({"reference":reference,"value":record.value()}));
    }
    json!({"snapshot":saved.snapshot,"session":saved.session,"messages":saved.messages,"records":records})
}
```

## `crates/wickle-state-sqlite/tests/support/workers.rs`

```rust
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use serde_json::{Value, json};
use wickle::*;
use wickle_state_sqlite::SqliteStateStore;

use crate::{
    core::{id, prepared, scope},
    support::{Database, durable_admission, populate_protected_run},
};

pub struct Worker {
    child: Child,
    pub result: PathBuf,
    pub ready: PathBuf,
}
impl Worker {
    pub fn spawn(database: &Database, actor: &str, mode: &str, details: Value) -> Self {
        let result = database.file(&format!("{actor}.result"));
        let ready = database.file(&format!("{actor}.ready"));
        let config = json!({
            "database":database.path(), "result":result, "ready":ready,
            "gate":database.file("gate"), "resume":database.file("resume"),
            "mode":mode, "actor":actor, "details":details,
        });
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "process_worker", "--nocapture"])
            .env("WICKLE_SQLITE_TEST_WORKER", config.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        Self {
            child,
            result,
            ready,
        }
    }
    pub fn finish(&mut self) -> Value {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success(), "SQLite subprocess failed");
                return read(&self.result);
            }
            assert!(
                Instant::now() < deadline,
                "SQLite subprocess did not finish"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    pub fn terminate(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn wait(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "SQLite subprocess barrier timed out"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}
pub fn signal(path: &Path) {
    std::fs::write(path, b"ready").unwrap();
}
pub fn read(path: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}
fn write(path: &Path, value: Value) {
    let temporary = path.with_extension("writing");
    std::fs::write(&temporary, serde_json::to_vec(&value).unwrap()).unwrap();
    std::fs::rename(temporary, path).unwrap();
}

pub fn run() {
    let encoded = std::env::var("WICKLE_SQLITE_TEST_WORKER")
        .expect("This ignored fixture requires an explicit parent test command");
    let config: Value = serde_json::from_str(&encoded).unwrap();
    let database = Path::new(config["database"].as_str().unwrap());
    let result = Path::new(config["result"].as_str().unwrap());
    let ready = Path::new(config["ready"].as_str().unwrap());
    let actor = config["actor"].as_str().unwrap();
    let mode = config["mode"].as_str().unwrap();
    if mode == "uncommitted" {
        let connection = rusqlite::Connection::open(database).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE; UPDATE wickle_scope_checkpoints SET checkpoint_json='uncommitted invalid image', checksum='uncommitted';").unwrap();
        signal(ready);
        loop {
            std::thread::park();
        }
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let store = SqliteStateStore::open(database).unwrap();
        signal(ready);
        if matches!(mode, "admit" | "lease_hold") {
            wait(Path::new(config["gate"].as_str().unwrap()));
        }
        match mode {
            "admit" => {
                let request = config["details"]["request"].as_str().unwrap();
                let receipt = store.admit(&scope(), durable_admission(actor, request, "session").await).await;
                write(result, match receipt {
                    Ok(receipt) => json!({"created":receipt.created,"run":receipt.state.snapshot.run_id}),
                    Err(error) => json!({"error":format!("{:?}",error.code)}),
                });
            }
            "lease_hold" => {
                match store.acquire_lease(&scope(), &id("run"), &id(actor), 100, 10_000).await {
                    Ok(lease) => {
                        write(result, json!({"fence":lease.fencing_token,"expires_at_ms":lease.expires_at_ms}));
                        wait(Path::new(config["resume"].as_str().unwrap()));
                        let latest = store.load(&scope(), &id("run")).await.unwrap();
                        let error = store.commit(&scope(), &id("run"), prepared(&latest.snapshot, lease.clone(), lease.expires_at_ms + 10)).await.unwrap_err();
                        write(result, json!({"fence":lease.fencing_token,"stale_error":format!("{:?}",error.code),"observed_revision":latest.snapshot.revision}));
                    }
                    Err(error) => write(result, json!({"error":format!("{:?}",error.code)})),
                }
            }
            "takeover" => {
                let now = config["details"]["now"].as_i64().unwrap();
                let owner = config["details"]["owner"].as_str().unwrap();
                let lease = store.acquire_lease(&scope(), &id("run"), &id(owner), now, 10_000).await.unwrap();
                let saved = store.load(&scope(), &id("run")).await.unwrap();
                let committed = store.commit(&scope(), &id("run"), prepared(&saved.snapshot, lease.clone(), now + 1)).await.unwrap();
                write(result, json!({"fence":lease.fencing_token,"revision":committed.snapshot.revision}));
            }
            "commit_exit" => {
                let retained_connection = rusqlite::Connection::open(database).unwrap();
                let _: i64 = retained_connection.query_row(
                    "SELECT count(*) FROM wickle_scope_checkpoints", [], |row| row.get(0)
                ).unwrap();
                let retained_store = std::sync::Arc::new(store);
                let saved = populate_protected_run(retained_store.clone()).await;
                write(result, saved);
                // Keep an initialized WAL connection alive so operation-level closes
                // cannot perform last-connection cleanup before this abrupt exit.
                std::process::exit(0);
            }
            _ => panic!("Unknown SQLite subprocess fixture mode"),
        }
    });
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

mod budget;
mod clock;
mod context;
mod context_projection;
mod error;
mod input_binding;
mod message;
mod model;
mod model_catalog;
mod model_execution;
mod model_protocol;
mod policy;
mod profile;
mod resolution;
mod run;
mod serialization;
mod state;
mod tool_schema;
mod views;

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
pub use model_execution::{
    ModelExchange, ModelExchangeOutcome, ModelRetryPolicy, StoredModelResponse,
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
                let command: ResumeCommand = event_record(state, &empty, command_ref)?;
                if command.run_id != run.snapshot.run_id
                    || command.expected_revision >= run.snapshot.revision
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
    {
        return Err(invalid("checkpoint.events"));
    }
    Ok(())
}

fn invalid(path: &str) -> ContractError {
    error(ErrorCode::InvalidSnapshot, path)
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
    let wait = WaitState {
        wait_id: id("wait"),
        target: WaitTarget::Input {
            request: InputRequest {
                input_request_id: id("question"),
                call_id: id("input-call"),
                question: "Select a source".into(),
                schema_ref: None,
            },
        },
        expires_at_ms: Some(180),
    };
    let wait_record = ProtectedRecord::new(id("old-wait"), 1, serde_json::to_value(&wait).unwrap());
    let mut waiting = prepared(&snapshot, lease.clone(), 101);
    waiting.snapshot.status = RunStatus::Waiting;
    waiting.snapshot.phase = RunPhase::Waiting;
    waiting.snapshot.wait = Some(wait);
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
    let waiting = store.commit(&scope(), &id("run-a"), waiting).await.unwrap();
    let command = ResumeCommand {
        run_id: id("run-a"),
        expected_revision: waiting.snapshot.revision,
        command_id: id("answer"),
        action: ResumeAction::Input {
            wait_id: id("wait"),
            answer: json!("selected source"),
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
