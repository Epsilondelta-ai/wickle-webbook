# 35장 전체 Rust 구현과 테스트

[강의로](../35-integration.md) · [전체 변경 패치](../solutions/35-integration.patch)

기준 `c23d50321aac015262639a13abe4537d99e8fd03`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

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
    sync::{Arc, Mutex},
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
const MAX_CACHED_CHECKPOINT_BYTES: usize = 32 * 1024 * 1024;
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
/// Reads use a consistent deferred transaction. Clones share a single validated
/// checkpoint cache, used only when current scope, JSON bytes and checksum match.
/// Checkpoints above 32 MiB of serialized JSON are not cached; decoded graph
/// memory is additional to those bytes. A successful write is returned
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
    validated: Arc<Mutex<Option<Arc<ValidatedCheckpoint>>>>,
}

// Immutable core-validated data only. Never cache mutable transaction state.
struct ValidatedCheckpoint {
    scope: Scope,
    json: String,
    checksum: String,
    checkpoint: StateStoreCheckpoint,
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
            validated: Arc::new(Mutex::new(None)),
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
        let mut cache_update = None;
        let state = match row {
            None => MemoryStateStore::new(),
            Some((json, checksum)) => {
                let cached = self.validated.lock().ok().and_then(|entry| entry.clone());
                let checkpoint = if let Some(cached) = cached.filter(|entry| {
                    entry.scope == *scope && entry.json == json && entry.checksum == checksum
                }) {
                    cached.checkpoint.clone()
                } else {
                    let digest = JsonDigest::try_from(checksum.clone())
                        .map_err(|_| error(ErrorCode::InvalidSnapshot, "sqlite.checksum"))?;
                    let checkpoint = StateStoreCheckpoint::from_json(&json, scope, &digest)?;
                    if json.len() <= MAX_CACHED_CHECKPOINT_BYTES {
                        cache_update = Some(ValidatedCheckpoint {
                            scope: scope.clone(),
                            json,
                            checksum,
                            checkpoint: checkpoint.clone(),
                        });
                    }
                    checkpoint
                };
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
            cache_update =
                (json.len() <= MAX_CACHED_CHECKPOINT_BYTES).then(|| ValidatedCheckpoint {
                    scope: scope.clone(),
                    json,
                    checksum: digest.as_str().to_owned(),
                    checkpoint,
                });
        }
        transaction.commit().map_err(storage_error)?;
        // Publish only after commit. A failed operation/commit cannot put mutated
        // state under the previous database bytes, including after an ACK loss.
        if let Some(checkpoint) = cache_update {
            if let Ok(mut entry) = self.validated.lock() {
                *entry = Some(Arc::new(checkpoint));
            }
        }
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
    fn record_hook_observation<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        report: wickle::HookObservation,
    ) -> PortFuture<'a, ()> {
        let run_id = run_id.clone();
        self.transact(scope, true, move |state, scope, runtime| {
            runtime.block_on(state.record_hook_observation(scope, &run_id, report))
        })
    }
    fn read_hook_observations<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
    ) -> PortFuture<'a, Vec<wickle::HookObservation>> {
        let run_id = run_id.clone();
        self.transact(scope, false, move |state, scope, runtime| {
            runtime.block_on(state.read_hook_observations(scope, &run_id))
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
async fn adapter_panic_fails_and_repeated_unknown_tools_exhaust_without_tool_dispatch() {
    for (response, expected_status, expected_calls) in [
        (Response::Panic, RunStatus::Failed, 1),
        (Response::Tool, RunStatus::Exhausted, 4),
    ] {
        let fixture = Fixture::new(response, false);
        let agent = fixture.agent();
        let handle = fixture.started(&agent, "request").await;
        let outcome = completed(handle.outcome(&context()).await.unwrap());
        assert_eq!(outcome.result.status(), expected_status);
        assert_eq!(outcome.usage.model_calls, expected_calls);
        assert_eq!(outcome.usage.tool_attempts, 0);
        assert_eq!(
            fixture.model.calls.load(Ordering::SeqCst),
            expected_calls as usize
        );
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

#[tokio::test]
async fn another_session_finishes_while_the_first_run_waits_on_model_io() {
    use std::sync::{Arc, atomic::AtomicUsize};
    use std::time::Duration;
    struct Lanes {
        slow: Arc<support::Model>,
        fast: Arc<support::Model>,
        calls: AtomicUsize,
    }
    impl ModelPort for Lanes {
        fn binding(&self) -> ModelPortBinding {
            self.slow.binding()
        }
        fn generate<'a>(
            &'a self,
            request: &'a ModelRequest,
            context: &'a ModelCallContext,
        ) -> PortStream<'a, ModelEvent> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.slow.generate(request, context)
            } else {
                self.fast.generate(request, context)
            }
        }
    }
    let fixture = Fixture::new(Response::Text, false);
    let lanes = Arc::new(Lanes {
        slow: Arc::new(support::Model::new(Response::Text, true)),
        fast: Arc::new(support::Model::new(Response::Text, false)),
        calls: AtomicUsize::new(0),
    });
    let mut bindings = fixture.bindings();
    bindings.model_exchange = Arc::new(
        ModelExchange::new(lanes.clone(), bindings.policy.clone())
            .with_route_inspector(fixture.inspector.clone(), Duration::from_secs(1))
            .unwrap(),
    );
    let agent = create_agent(profile(), bindings).unwrap();
    let baseline = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    let mut slow_request = request("slow");
    slow_request.session_id = id("slow-session");
    let slow = completed(agent.start(slow_request, context()).await.unwrap());
    tokio::time::timeout(Duration::from_secs(2), lanes.slow.entered.notified())
        .await
        .unwrap();
    assert_eq!(lanes.slow.release.available_permits(), 0);
    let mut fast_request = request("fast");
    fast_request.session_id = id("fast-session");
    let fast = completed(agent.start(fast_request, context()).await.unwrap());
    let result = tokio::time::timeout(Duration::from_secs(2), fast.outcome(&context()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed(result).result.status(), RunStatus::Succeeded);
    assert_eq!(
        fixture
            .store
            .load(&scope(), slow.run_id())
            .await
            .unwrap()
            .snapshot
            .status,
        RunStatus::Running
    );
    assert_eq!(lanes.slow.release.available_permits(), 0);
    lanes.slow.release.add_permits(1);
    let result = tokio::time::timeout(Duration::from_secs(2), slow.outcome(&context()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed(result).result.status(), RunStatus::Succeeded);
    assert_eq!(lanes.calls.load(Ordering::SeqCst), 2);
    assert!(
        fixture
            .store
            .load_session(&scope(), &id("slow-session"))
            .await
            .unwrap()
            .active_run_id
            .is_none()
    );
    assert!(
        fixture
            .store
            .load_session(&scope(), &id("fast-session"))
            .await
            .unwrap()
            .active_run_id
            .is_none()
    );
    drop(slow);
    drop(fast);
    drop(agent);
    tokio::time::timeout(Duration::from_secs(2), async {
        while tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks()
            > baseline
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
```

## `tests/support/gather_consumer.rs`

```rust
// Independent retrieval/aggregation Host with replaceable memory and graph sources.
#[allow(dead_code)]
mod host {
    include!("source_consumer.rs");
    use serde_json::Value;
    use std::sync::Mutex;
    const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";
    fn ready(
        request: &ContextRequest,
        native: &str,
        origin: ContextOrigin,
        value: serde_json::Value,
    ) -> ContextResult {
        ContextResult::Ready {
            items: vec![ContextItem::new(
                id("row-1"),
                origin,
                reference(native),
                request.scope.clone(),
                vec![InputContent::Json { value }],
                ContextLifetime::Run {
                    run_id: request.run_id.clone(),
                },
                ContextPriority::Required,
            )],
            source_revision: Some(id("dataset-1")),
            reported_usage: None,
        }
    }
    fn authorize(
        request: &ContextUseRequest,
        context: &ContextCallContext,
        native: &str,
    ) -> Result<(), ContractError> {
        if request.request.scope != context.scope
            || request
                .items
                .iter()
                .any(|item| item.item_id != id("row-1") || item.source_ref != reference(native))
        {
            return Err(ContractError::new(
                ErrorCode::AccessDenied,
                "business.source",
            ));
        }
        Ok(())
    }
    // The two providers deliberately use different backing representations.
    struct MemoryA {
        period: String,
        calls: Arc<AtomicUsize>,
    }
    impl ContextSource for MemoryA {
        fn provide<'a>(
            &'a self,
            request: &'a ContextRequest,
            _: &'a ContextCallContext,
        ) -> PortFuture<'a, ContextResult> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(ready(
                    request,
                    "memory-a",
                    ContextOrigin::Memory,
                    json!({"period":self.period}),
                ))
            })
        }
        fn authorize_use<'a>(
            &'a self,
            request: &'a ContextUseRequest,
            context: &'a ContextCallContext,
        ) -> PortFuture<'a, ()> {
            Box::pin(async move { authorize(request, context, "memory-a") })
        }
    }
    struct MemoryB {
        records: std::collections::BTreeMap<String, String>,
        calls: Arc<AtomicUsize>,
    }
    impl ContextSource for MemoryB {
        fn provide<'a>(
            &'a self,
            request: &'a ContextRequest,
            _: &'a ContextCallContext,
        ) -> PortFuture<'a, ContextResult> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let period = self.records.get("preferred.period").ok_or_else(|| {
                    ContractError::new(ErrorCode::ComponentUnavailable, "memory.record")
                })?;
                Ok(ready(
                    request,
                    "memory-b",
                    ContextOrigin::Memory,
                    json!({"period":period}),
                ))
            })
        }
        fn authorize_use<'a>(
            &'a self,
            request: &'a ContextUseRequest,
            context: &'a ContextCallContext,
        ) -> PortFuture<'a, ()> {
            Box::pin(async move { authorize(request, context, "memory-b") })
        }
    }
    struct Graph {
        calls: Arc<AtomicUsize>,
    }
    impl ContextSource for Graph {
        fn provide<'a>(
            &'a self,
            request: &'a ContextRequest,
            _: &'a ContextCallContext,
        ) -> PortFuture<'a, ContextResult> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(ready(
                    request,
                    "graph",
                    ContextOrigin::Retrieval,
                    json!({"periods":["quarter","month"],"relationship":"period_has_records"}),
                ))
            })
        }
        fn authorize_use<'a>(
            &'a self,
            request: &'a ContextUseRequest,
            context: &'a ContextCallContext,
        ) -> PortFuture<'a, ()> {
            Box::pin(async move { authorize(request, context, "graph") })
        }
    }
    struct GatherCatalog;
    impl ProfileResolver for GatherCatalog {
        fn resolve<'a>(
            &'a self,
            reference: &'a ComponentRef,
            scope: &'a Scope,
        ) -> PortFuture<'a, ComponentMetadata> {
            Box::pin(async move {
                if reference.kind == ComponentKind::Tool && reference.id != id("lookup") {
                    return Err(ContractError::new(
                        ErrorCode::ComponentUnavailable,
                        "profile.tool",
                    ));
                }
                let mut metadata = Catalog.resolve(reference, scope).await?;
                if reference.kind == ComponentKind::Tool {
                    metadata.model_name = Some(id("lookup"));
                }
                Ok(metadata)
            })
        }
    }
    struct GatherPolicy;
    impl PolicyPort for GatherPolicy {
        fn authorize<'a>(
            &'a self,
            request: &'a PolicyRequest,
            _: PolicyContext<'a>,
        ) -> PortFuture<'a, PolicyDecision> {
            Box::pin(async move {
                Ok(
                    if matches!(&request.action,PolicyAction::ExecuteTool{input} if input.execution_args().get("workspace_id")!=Some(&json!(WORKSPACE)))
                    {
                        PolicyDecision::Deny {
                            reason: id("foreign-workspace"),
                        }
                    } else {
                        PolicyDecision::Allow {}
                    },
                )
            })
        }
    }
    struct Lookup {
        scope: Scope,
        calls: AtomicUsize,
        seen: Mutex<Vec<JsonObject>>,
    }
    impl ToolExecutor for Lookup {
        fn execute<'a>(
            &'a self,
            args: &'a JsonObject,
            context: &'a ToolExecutionContext,
        ) -> PortFuture<'a, ToolExecutionResult> {
            Box::pin(async move {
                assert_eq!(context.scope, self.scope);
                assert_eq!(args["workspace_id"], WORKSPACE);
                assert_eq!(args.len(), 3);
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.seen.lock().unwrap().push(args.clone());
                let period = args["query"].as_str().unwrap();
                let limit = args["limit"].as_u64().unwrap() as usize;
                let amounts = match period {
                    "quarter" => vec![20, 22, 999],
                    "month" => vec![40, 60, 999],
                    _ => {
                        return Err(ContractError::new(
                            ErrorCode::InvalidContract,
                            "lookup.query",
                        ));
                    }
                };
                let rows: Vec<_> = amounts
                    .into_iter()
                    .take(limit)
                    .map(|amount| json!({"amount":amount}))
                    .collect();
                let total: i64 = rows.iter().map(|row| row["amount"].as_i64().unwrap()).sum();
                let evidence = EvidenceRef {
                    source_id: id("records"),
                    version: id("1"),
                    location: id(period),
                    content_hash: id(canonical_digest(&json!(rows)).as_str()),
                    quote: None,
                };
                Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Succeeded {
                        value: json!({"rows":rows,"total":total,"evidence":evidence}),
                    },
                    effect: ToolEffect::NotApplied,
                    receipt: None,
                })
            })
        }
    }
    struct GatherModel {
        period: &'static str,
        calls: AtomicUsize,
    }
    impl ModelPort for GatherModel {
        fn binding(&self) -> ModelPortBinding {
            ModelPortBinding {
                provider: id("synthetic"),
                adapter: reference("synthetic-adapter"),
                connection_ref: reference("synthetic-connection"),
            }
        }
        fn generate<'a>(
            &'a self,
            request: &'a ModelRequest,
            _: &'a ModelCallContext,
        ) -> PortStream<'a, ModelEvent> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(call < 2);
            let contexts: Vec<_> = request
                .messages
                .iter()
                .flat_map(|message| {
                    message
                        .content
                        .iter()
                        .filter_map(move |content| match content {
                            ModelContent::Json { value } if value["kind"] == "context_data" => {
                                assert_eq!(message.role, ModelRole::User);
                                Some(value)
                            }
                            _ => None,
                        })
                })
                .collect();
            assert_eq!(contexts.len(), 2);
            assert_ne!(contexts[0]["item_id"], contexts[1]["item_id"]);
            let memory = contexts
                .iter()
                .find(|value| value["origin"] == "memory")
                .unwrap();
            let graph = contexts
                .iter()
                .find(|value| value["origin"] == "retrieval")
                .unwrap();
            let period = memory["content"][0]["value"]["period"].as_str().unwrap();
            assert_eq!(period, self.period);
            assert!(
                graph["content"][0]["value"]["periods"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(period))
            );
            assert_eq!(request.tools.len(), 1);
            let properties = request.tools[0].model_input_schema["properties"]
                .as_object()
                .unwrap();
            assert_eq!(properties.len(), 2);
            assert!(properties.contains_key("query") && properties.contains_key("limit"));
            let events = if call == 0 {
                vec![
                    Ok(ModelEvent::ToolArgumentsDelta {
                        index: 0,
                        provider_call_id: Some("lookup-rows".into()),
                        name: Some("lookup".into()),
                        delta: json!({"query":period,"limit":2}).to_string(),
                    }),
                    Ok(ModelEvent::ResponseCompleted {
                        finish: ModelFinish::ToolCalls,
                        metadata: Default::default(),
                        continuation: vec![],
                    }),
                ]
            } else {
                let result = request
                    .messages
                    .iter()
                    .flat_map(|m| &m.content)
                    .find_map(|v| match v {
                        ModelContent::ToolResult {
                            provider_call_id,
                            content,
                        } if provider_call_id == &id("lookup-rows") => Some(content),
                        _ => None,
                    })
                    .unwrap();
                assert_eq!(result["status"], "succeeded");
                let value = &result["content"][0]["value"];
                let evidence: EvidenceRef =
                    serde_json::from_value(value["evidence"].clone()).unwrap();
                assert_eq!(evidence.location, id(period));
                assert_eq!(
                    evidence.content_hash.as_str(),
                    canonical_digest(&value["rows"]).as_str()
                );
                vec![
                    Ok(ModelEvent::TextDelta {
                        text: json!({"period":period,"total":value["total"],"evidence":evidence})
                            .to_string(),
                    }),
                    Ok(ModelEvent::ResponseCompleted {
                        finish: ModelFinish::Stop,
                        metadata: Default::default(),
                        continuation: vec![],
                    }),
                ]
            };
            Box::pin(stream::iter(events))
        }
    }
    fn gather_routing(scope: &Scope) -> Result<RoutingSnapshot, ContractError> {
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

    struct Scenario {
        scope: Scope,
        store: Arc<SqliteStateStore>,
        memory: Arc<dyn ContextSource>,
        memory_id: &'static str,
        graph: Arc<Graph>,
        model: Arc<GatherModel>,
        tool: Arc<Lookup>,
    }
    impl Scenario {
        fn build(&self, profile: AgentProfile) -> Result<Agent, ContractError> {
            let policy = Arc::new(PolicyGate::new(
                Arc::new(GatherPolicy),
                Duration::from_secs(5),
            )?);
            let clock = Arc::new(SystemClock::new());
            let ids = Arc::new(RandomIdSource);
            let sources = Arc::new(ContextSourceRuntime::new(
                self.store.clone(),
                policy.clone(),
                clock.clone(),
                ids.clone(),
                Arc::new(ContextSourceRegistry::new(
                    self.scope.clone(),
                    vec![
                        ContextSourceRegistration {
                            selection: ContextSourceRef::Catalog(CatalogSourceRef {
                                source_id: id(self.memory_id),
                                version: id("1"),
                            }),
                            definition: ContextSourceDefinition {
                                source: reference(self.memory_id),
                                origin: ContextOrigin::Memory,
                                contract_version: 1,
                            },
                            source: self.memory.clone(),
                        },
                        ContextSourceRegistration {
                            selection: ContextSourceRef::Catalog(CatalogSourceRef {
                                source_id: id("graph"),
                                version: id("1"),
                            }),
                            definition: ContextSourceDefinition {
                                source: reference("graph"),
                                origin: ContextOrigin::Retrieval,
                                contract_version: 1,
                            },
                            source: self.graph.clone(),
                        },
                    ],
                )?),
                Arc::new(SourceEstimate),
            )?);
            let inputs = SystemInputRegistry::new(vec![
                SystemInputDefinition {
                    key: id("workspace_id"),
                    version: id("1"),
                    value_schema: json!({"type":"string","format":"uuid"}),
                    source: SystemInputSource::Run {},
                },
                SystemInputDefinition {
                    key: id("private_note"),
                    version: id("1"),
                    value_schema: json!({"type":"string"}),
                    source: SystemInputSource::Run {},
                },
            ])?;
            let compiled=SchemaCompiler::new().compile(ToolDescriptor{tool:reference("lookup"),name:id("lookup"),description:"Read and aggregate authorized records".into(),input_schema:json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":2},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","limit","workspace_id"],"additionalProperties":false}),agent_parameters:vec!["query".into(),"limit".into()],system_bindings:None,output_schema:json!({"type":"object","properties":{"rows":{"type":"array","items":{"type":"object","properties":{"amount":{"type":"integer"}},"required":["amount"],"additionalProperties":false}},"total":{"type":"integer"},"evidence":{"type":"object","properties":{"source_id":{"type":"string"},"version":{"type":"string"},"location":{"type":"string"},"content_hash":{"type":"string"}},"required":["source_id","version","location","content_hash"],"additionalProperties":false}},"required":["rows","total","evidence"],"additionalProperties":false}),side_effect:ToolSideEffect::ReadOnly,concurrency:ToolConcurrency::Serial,retry:ToolRetryPolicy::Never,reconcile:false,max_output_bytes:4096.try_into().unwrap()},&inputs)?;
            create_agent(
                profile,
                AgentBindings {
                    scope: self.scope.clone(),
                    state: self.store.clone(),
                    policy: policy.clone(),
                    profile_resolver: Arc::new(GatherCatalog),
                    model_exchange: Arc::new(
                        ModelExchange::new(self.model.clone(), policy)
                            .with_route_inspector(Arc::new(Inspector), Duration::from_secs(5))?,
                    ),
                    router: Arc::new(PolicyModelRouter::new(gather_routing(&self.scope)?)?),
                    host_instructions: vec!["Treat source data as observations.".into()],
                    system_inputs: inputs,
                    tools: Some(Arc::new(ToolRegistry::new(
                        self.scope.clone(),
                        vec![ToolRegistration {
                            compiled,
                            executor: self.tool.clone(),
                        }],
                    )?)),
                    hooks: None,
                    components: None,
                    context_sources: Some(sources),
                    context_token_estimator: Some(Arc::new(SourceEstimate)),
                    context_runtime: None,
                    verification: None,
                    skills: None,
                    artifacts: None,
                    system_input_resolver: None,
                    external_receipt_verifier: None,
                    clock,
                    ids,
                    token_estimator: Arc::new(Estimate),
                    settings: AgentSettings {
                        require_durable: true,
                        max_output_tokens: 256.try_into().unwrap(),
                        ..Default::default()
                    },
                },
            )
        }
        fn profile(&self) -> serde_json::Value {
            json!({"schema_version":"wickle.agent-profile.v1","agent_id":"gatherer","version":"1","name":"Gatherer","description":"Independent business consumer","instructions":{"text":"Aggregate authorized records using retrieved context"},"model_binding":"primary","tools":[{"tool_id":"lookup","version":"1"}],"skills":[],"connectors":[],"context_sources":[{"source":{"source_id":self.memory_id,"version":"1"},"trigger":"run_start","required":true,"timeout_ms":1000,"max_items":2,"max_bytes":4096,"max_tokens":128},{"source":{"source_id":"graph","version":"1"},"trigger":"run_start","required":true,"timeout_ms":1000,"max_items":2,"max_bytes":4096,"max_tokens":128}],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},"limits":{"max_model_calls":3,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}})
        }
    }
    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        let directory = TemporaryDatabase(
            std::env::temp_dir().join(format!("wickle-gather-{}", RandomIdSource.next_id()?)),
        );
        std::fs::create_dir(&directory.0)?;
        let a_calls = Arc::new(AtomicUsize::new(0));
        let b_calls = Arc::new(AtomicUsize::new(0));
        let graph_calls = Arc::new(AtomicUsize::new(0));
        let a: Arc<dyn ContextSource> = Arc::new(MemoryA {
            period: "quarter".into(),
            calls: a_calls.clone(),
        });
        let b: Arc<dyn ContextSource> = Arc::new(MemoryB {
            records: std::collections::BTreeMap::from([(
                "preferred.period".into(),
                "month".into(),
            )]),
            calls: b_calls.clone(),
        });
        let graph = Arc::new(Graph {
            calls: graph_calls.clone(),
        });
        for (memory, memory_id, period, total) in [
            (a, "memory-a", "quarter", 42),
            (b, "memory-b", "month", 100),
        ] {
            let scope = Scope {
                tenant_id: id("tenant"),
                workspace_id: id("workspace"),
                user_id: None,
            };
            let scenario = Scenario {
                scope: scope.clone(),
                store: Arc::new(SqliteStateStore::open(
                    directory.0.join(format!("{memory_id}.sqlite3")),
                )?),
                memory,
                memory_id,
                graph: graph.clone(),
                model: Arc::new(GatherModel {
                    period,
                    calls: AtomicUsize::new(0),
                }),
                tool: Arc::new(Lookup {
                    scope: scope.clone(),
                    calls: AtomicUsize::new(0),
                    seen: Mutex::new(vec![]),
                }),
            };
            let profile_path = directory.0.join(format!("{memory_id}.profile.json"));
            std::fs::write(&profile_path, scenario.profile().to_string())?;
            let profile = AgentProfile::from_json(&std::fs::read_to_string(&profile_path)?)?;
            ProfileValidator::new(&GatherCatalog)
                .validate(&profile, &scope)
                .await?;
            let agent = scenario.build(profile)?;
            let execution = ExecutionContext::new(
                ExecutionContextData {
                    scope: scope.clone(),
                    principal_ref: id("reader"),
                    capability_grant_ref: id("read-grant"),
                    trace_context: None,
                    system_inputs: Some(SystemInputs::new(JsonObject::from([
                        ("workspace_id".into(), json!(WORKSPACE)),
                        ("private_note".into(), json!("not a tool input")),
                    ]))),
                },
                Default::default(),
            );
            let handle = completed(agent.start(request("gather"), execution.clone()).await?)?;
            let outcome = completed(handle.outcome(&execution).await?)?;
            assert_eq!(outcome.result.status(), RunStatus::Succeeded);
            let InputContent::Text { text } = &outcome.output[0] else {
                return Err("expected aggregate output".into());
            };
            let value: Value = serde_json::from_str(text)?;
            assert_eq!(value["total"], total);
            assert_eq!(value["period"], period);
            let evidence: EvidenceRef = serde_json::from_value(value["evidence"].clone())?;
            assert_eq!(evidence.source_id, id("records"));
            assert_eq!(evidence.location, id(period));
            assert_eq!(scenario.model.calls.load(Ordering::SeqCst), 2);
            assert_eq!(scenario.tool.calls.load(Ordering::SeqCst), 1);
            assert_eq!(scenario.tool.seen.lock().unwrap()[0]["query"], period);
            let mut unknown = scenario.profile();
            unknown["tools"][0]["tool_id"] = json!("unregistered");
            let invalid_profile = AgentProfile::from_json(&unknown.to_string())?;
            assert!(
                ProfileValidator::new(&GatherCatalog)
                    .validate(&invalid_profile, &scope)
                    .await
                    .is_err()
            );
            let error = scenario
                .build(invalid_profile)
                .err()
                .ok_or("unregistered tool was accepted")?;
            assert_eq!(error.code, ErrorCode::ComponentUnavailable);
            let mut escalation = scenario.profile();
            escalation["capability_grant_ref"] = json!("administrator");
            assert!(AgentProfile::from_json(&escalation.to_string()).is_err());
            let mut foreign = execution.clone();
            foreign.data.scope.workspace_id = id("foreign");
            assert!(agent.start(request("foreign"), foreign).await.is_err());
            assert_eq!(scenario.model.calls.load(Ordering::SeqCst), 2);
            assert_eq!(scenario.tool.calls.load(Ordering::SeqCst), 1);
        }
        assert_eq!(a_calls.load(Ordering::SeqCst), 1);
        assert_eq!(b_calls.load(Ordering::SeqCst), 1);
        assert_eq!(graph_calls.load(Ordering::SeqCst), 2);
        println!(
            "gather consumer: distinct memory A/B implementations plus graph retrieval; scoped query/limit Tool loop with hidden UUID; evidence used in final aggregation; external profiles and rejected escalation passed"
        );
        Ok(())
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    host::run().await
}
```

## `tests/support/lifecycle_consumer.rs`

```rust
// Repeated lifecycle verification and a measured local-only timing baseline.
#[allow(dead_code)]
mod host {
    include!("adapter_consumer.rs");
    use std::sync::{Weak, atomic::AtomicU64};
    #[derive(Default)]
    struct Stats {
        opens: AtomicUsize,
        closes: AtomicUsize,
        instances: Mutex<Vec<Weak<dyn AdapterInstance>>>,
    }
    struct RepeatFactory {
        store: Arc<SqliteStateStore>,
        stats: Arc<Stats>,
    }
    impl AdapterFactory for RepeatFactory {
        fn open<'a>(
            &'a self,
            context: &'a AdapterInitContext,
        ) -> PortFuture<'a, Arc<dyn AdapterInstance>> {
            Box::pin(async move {
                let inner = Factory {
                    store: self.store.clone(),
                    counters: Arc::new(Counters::default()),
                }
                .open(context)
                .await?;
                let instance: Arc<dyn AdapterInstance> = Arc::new(RepeatInstance {
                    inner,
                    stats: self.stats.clone(),
                    closed: AtomicBool::new(false),
                });
                self.stats.opens.fetch_add(1, Ordering::SeqCst);
                self.stats
                    .instances
                    .lock()
                    .unwrap()
                    .push(Arc::downgrade(&instance));
                Ok(instance)
            })
        }
    }
    struct RepeatInstance {
        inner: Arc<dyn AdapterInstance>,
        stats: Arc<Stats>,
        closed: AtomicBool,
    }
    impl AdapterInstance for RepeatInstance {
        fn exports(&self) -> Vec<AdapterExportInstance> {
            self.inner.exports()
        }
        fn close<'a>(&'a self, context: &'a AdapterCloseContext) -> PortFuture<'a, ()> {
            Box::pin(async move {
                self.inner.close(context).await?;
                if !self.closed.swap(true, Ordering::SeqCst) {
                    self.stats.closes.fetch_add(1, Ordering::SeqCst);
                }
                Ok(())
            })
        }
    }
    struct RepeatModel {
        route: ResolvedModelRoute,
        calls: AtomicUsize,
        model_cpu_us: AtomicU64,
        phases: Mutex<std::collections::BTreeMap<Id, usize>>,
    }
    impl ModelPort for RepeatModel {
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
            context: &'a ModelCallContext,
        ) -> PortStream<'a, ModelEvent> {
            let start = std::time::Instant::now();
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.route, self.route);
            let mut phases = self.phases.lock().unwrap();
            let phase = phases.entry(context.run_id.clone()).or_default();
            assert!(
                *phase < 2,
                "unexpected additional model call in {}",
                context.run_id
            );
            let finish = *phase == 1;
            *phase += 1;
            drop(phases);
            let provider_call = format!("write-{}", context.run_id);
            if finish {
                assert!(request.messages.iter().flat_map(|message|&message.content).any(|content|matches!(content,ModelContent::ToolResult{provider_call_id,content} if provider_call_id.as_str()==provider_call && content["status"]=="succeeded")));
            }
            let mut events = if finish {
                vec![Ok(ModelEvent::TextDelta {
                    text: "Report written.".into(),
                })]
            } else {
                vec![Ok(ModelEvent::ToolArgumentsDelta {
                    index: 0,
                    provider_call_id: Some(provider_call),
                    name: Some("write".into()),
                    delta: r#"{"query":"report"}"#.into(),
                })]
            };
            events.push(Ok(ModelEvent::ResponseCompleted {
                finish: if finish {
                    ModelFinish::Stop
                } else {
                    ModelFinish::ToolCalls
                },
                metadata: Default::default(),
                continuation: vec![],
            }));
            self.model_cpu_us
                .fetch_add(start.elapsed().as_micros() as u64, Ordering::SeqCst);
            Box::pin(stream::iter(events))
        }
    }
    fn repeat_registry(
        scope: &Scope,
        factory: Arc<dyn AdapterFactory>,
    ) -> Result<AdapterRegistry, ContractError> {
        let definition = definition();
        let value = json!({"thread_id":"prepared-report-thread"});
        let state = AdapterBindingState {
            scope: scope.clone(),
            session_id: id("session"),
            adapter_binding: id("reports"),
            adapter: reference("report-adapter"),
            definition_digest: definition.digest(),
            state_ref: ProtectedRecord::new(id("prepared-mapping"), 1, value.clone())
                .reference()
                .clone(),
            value,
        };
        AdapterRegistry::new(
            scope.clone(),
            vec![AdapterRegistration {
                definition,
                factory,
            }],
            vec![ConnectionRegistration {
                binding: ConnectorBindingRef {
                    binding_id: id("data"),
                    connector_id: id("report-service"),
                    version: id("1"),
                },
                metadata: metadata(ComponentKind::Connector, "report-service"),
                connection_ref: reference("report-account"),
            }],
            vec![],
            vec![],
            vec![state],
        )
    }
    fn repeat_agent(
        scope: &Scope,
        store: Arc<SqliteStateStore>,
        model: Arc<dyn ModelPort>,
        resolver: Arc<Resolver>,
        stats: Arc<Stats>,
    ) -> Result<(Agent, Arc<Catalog>), ContractError> {
        let registry = Arc::new(repeat_registry(
            scope,
            Arc::new(RepeatFactory {
                store: store.clone(),
                stats,
            }),
        )?);
        let catalog = Arc::new(Catalog {
            registry: registry.clone(),
            calls: AtomicUsize::new(0),
        });
        let policy = Arc::new(PolicyGate::new(Arc::new(Policy), Duration::from_secs(5))?);
        let clock = Arc::new(SystemClock::new());
        let runtime = Arc::new(AdapterRuntime::new(
            registry,
            store.clone(),
            policy.clone(),
            clock.clone(),
        ));
        let profile = AgentProfile::from_json(
            r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"writer","version":"1",
        "name":"Writer","description":"Synthetic adapter consumer","instructions":{"text":"Write the report after authorization"},
        "model_binding":"primary","tools":[{"adapter_binding":"reports","export_id":"save","alias":"write"}],"skills":[],
        "connectors":[{"binding_id":"data","connector_id":"report-service","version":"1"}],
        "adapters":[{"binding_id":"reports","adapter_id":"report-adapter","version":"1","connections":{"main":"data"}}],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":3,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}
    }"#,
        )?;
        Ok((
            create_agent(
                profile,
                AgentBindings {
                    scope: scope.clone(),
                    state: store,
                    policy: policy.clone(),
                    profile_resolver: catalog.clone(),
                    model_exchange: Arc::new(
                        ModelExchange::new(model, policy)
                            .with_route_inspector(Arc::new(Inspector), Duration::from_secs(5))?,
                    ),
                    router: Arc::new(PolicyModelRouter::new(routing(scope)?)?),
                    host_instructions: vec!["Use only authorized inputs.".into()],
                    system_inputs: system_inputs()?,
                    tools: None,
                    hooks: None,
                    components: Some(runtime),
                    context_sources: None,
                    context_token_estimator: None,
                    context_runtime: None,
                    verification: None,
                    skills: None,
                    artifacts: None,
                    system_input_resolver: Some(resolver),
                    external_receipt_verifier: None,
                    clock,
                    ids: Arc::new(RandomIdSource),
                    token_estimator: Arc::new(Estimate),
                    settings: AgentSettings {
                        require_durable: true,
                        max_output_tokens: 128.try_into().unwrap(),
                        ..Default::default()
                    },
                },
            )?,
            catalog,
        ))
    }

    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        let directory = TemporaryDatabase(
            std::env::temp_dir().join(format!("wickle-lifecycle-{}", RandomIdSource.next_id()?)),
        );
        std::fs::create_dir(&directory.0)?;
        let scope = Scope {
            tenant_id: id("tenant"),
            workspace_id: id("workspace"),
            user_id: None,
        };
        let store = Arc::new(SqliteStateStore::open(directory.0.join("state.sqlite3"))?);
        let stats = Arc::new(Stats::default());
        let model = Arc::new(RepeatModel {
            route: routing(&scope)?.route_for_binding(&reference("primary"))?,
            calls: AtomicUsize::new(0),
            model_cpu_us: AtomicU64::new(0),
            phases: Mutex::new(std::collections::BTreeMap::new()),
        });
        let resolver = Arc::new(Resolver {
            value: RECORD,
            revision: "record-A",
            calls: AtomicUsize::new(0),
        });
        let (agent, _catalog) = repeat_agent(
            &scope,
            store.clone(),
            model.clone(),
            resolver,
            stats.clone(),
        )?;
        let caller = context(&scope, false);
        let reviewer = context(&scope, true);
        let baseline = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let mut times = vec![];
        let mut max_events = 0;
        for round in 0..12 {
            eprintln!("lifecycle round {round}: start");
            let start = std::time::Instant::now();
            {
                let request = RunRequest {
                    request_id: id(&format!("request-{round}")),
                    session_id: id("session"),
                    input: vec![InputContent::Text {
                        text: "Write the report".into(),
                    }],
                    trigger: RunTrigger::User {},
                    model_options: JsonObject::new(),
                    output_contract: None,
                };
                let handle = completed(agent.start(request, caller.clone()).await?)?;
                assert_eq!(
                    completed(handle.outcome(&caller).await?)?.result.status(),
                    RunStatus::Waiting
                );
                release_finished(&handle, &caller).await?;
                if round % 2 == 0 {
                    let saved = store.load(&scope, handle.run_id()).await?;
                    let wait = saved.snapshot.wait.ok_or("missing wait")?;
                    let WaitTarget::Approval { target } = wait.target else {
                        return Err("not approval".into());
                    };
                    let command = ResumeCommand {
                        run_id: handle.run_id().clone(),
                        expected_revision: saved.snapshot.revision,
                        command_id: id(&format!("approve-{round}")),
                        action: ResumeAction::Approve {
                            wait_id: wait.wait_id,
                            target,
                        },
                    };
                    let resumed =
                        completed(agent.resume(command.clone(), reviewer.clone()).await?)?;
                    let outcome = completed(resumed.outcome(&reviewer).await?)?;
                    assert_eq!(
                        outcome.result.status(),
                        RunStatus::Succeeded,
                        "round {round}: {:?}, usage {:?}",
                        outcome.result,
                        outcome.usage
                    );
                    release_finished(&resumed, &reviewer).await?;
                    let before = model.calls.load(Ordering::SeqCst);
                    let replay = completed(agent.resume(command, reviewer.clone()).await?)?;
                    assert_eq!(
                        completed(replay.outcome(&reviewer).await?)?.result.status(),
                        RunStatus::Succeeded
                    );
                    assert_eq!(model.calls.load(Ordering::SeqCst), before);
                } else {
                    let _ = completed(handle.cancel(id("caller-cancelled"), &caller).await?)?;
                    assert_eq!(
                        store.load(&scope, handle.run_id()).await?.snapshot.status,
                        RunStatus::Cancelled
                    );
                }
                let events = store.read_events(&scope, handle.run_id(), 0, 64).await?;
                assert!(!events.has_more);
                max_events = max_events.max(events.events.len());
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let alive = stats
                        .instances
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|weak| weak.upgrade().is_some())
                        .count();
                    if alive == 0
                        && stats.opens.load(Ordering::SeqCst) == stats.closes.load(Ordering::SeqCst)
                        && tokio::runtime::Handle::current()
                            .metrics()
                            .num_alive_tasks()
                            <= baseline
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .map_err(|_| {
                format!(
                    "cleanup incomplete: retained={}, opens={}, closes={}, tasks={}",
                    stats
                        .instances
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|weak| weak.upgrade().is_some())
                        .count(),
                    stats.opens.load(Ordering::SeqCst),
                    stats.closes.load(Ordering::SeqCst),
                    tokio::runtime::Handle::current()
                        .metrics()
                        .num_alive_tasks()
                )
            })?;
            eprintln!("lifecycle round {round}: settled in {:?}", start.elapsed());
            times.push(start.elapsed().as_micros() as u64);
        }
        assert_eq!(model.calls.load(Ordering::SeqCst), 18);
        assert_eq!(stats.opens.load(Ordering::SeqCst), 18);
        assert_eq!(stats.closes.load(Ordering::SeqCst), 18);
        let total: u64 = times.iter().sum();
        let model_cpu = model.model_cpu_us.load(Ordering::SeqCst);
        println!(
            "{}",
            json!({"check":"repeated_lifecycle","os":std::env::consts::OS,"arch":std::env::consts::ARCH,"debug_assertions":cfg!(debug_assertions),"runs":12,"approved":6,"cancelled":6,"instances_opened":18,"instances_closed":18,"surviving_instances":0,"pending_tasks_before":baseline,"pending_tasks_after":tokio::runtime::Handle::current().metrics().num_alive_tasks(),"max_events_per_run":max_events,"event_page_limit":64,"total_wall_us":total,"model_fixture_cpu_us":model_cpu,"network_us":0,"local_non_model_us":total.saturating_sub(model_cpu),"per_run_wall_us":times,"timing_scope":"local core plus SQLite and Host callbacks; not isolated engine CPU or real-model latency"})
        );
        Ok(())
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    host::run().await
}
```

## `tests/support/report_process_consumer.rs`

```rust
// Independent result-generation Host: real subprocesses, SQLite and file output.
#[allow(dead_code)]
mod host {
    include!("adapter_consumer.rs");
    use tokio::io::AsyncWriteExt;
    fn io_error(_: std::io::Error) -> ContractError {
        ContractError::new(ErrorCode::ComponentUnavailable, "report.file")
    }
    async fn trace(directory: &std::path::Path, value: Value) -> Result<(), ContractError> {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(directory.join("trace.jsonl"))
            .await
            .map_err(io_error)?;
        file.write_all(format!("{value}\n").as_bytes())
            .await
            .map_err(io_error)?;
        file.sync_all().await.map_err(io_error)
    }
    struct FileFactory {
        inner: Arc<Factory>,
        directory: std::path::PathBuf,
    }
    impl AdapterFactory for FileFactory {
        fn open<'a>(
            &'a self,
            context: &'a AdapterInitContext,
        ) -> PortFuture<'a, Arc<dyn AdapterInstance>> {
            Box::pin(async move {
                let inner = self.inner.open(context).await?;
                trace(&self.directory,json!({"kind":"open","pid":std::process::id(),"binding":context.execution.binding_set_id})).await?;
                Ok(Arc::new(FileInstance {
                    inner,
                    directory: self.directory.clone(),
                    closed: AtomicBool::new(false),
                }) as Arc<dyn AdapterInstance>)
            })
        }
    }
    struct FileInstance {
        inner: Arc<dyn AdapterInstance>,
        directory: std::path::PathBuf,
        closed: AtomicBool,
    }
    impl AdapterInstance for FileInstance {
        fn exports(&self) -> Vec<AdapterExportInstance> {
            self.inner
                .exports()
                .into_iter()
                .map(|export| match export {
                    AdapterExportInstance::Tool {
                        export_id,
                        descriptor,
                        executor,
                    } => AdapterExportInstance::Tool {
                        export_id,
                        descriptor,
                        executor: Arc::new(FileWriter {
                            inner: executor,
                            directory: self.directory.clone(),
                        }),
                    },
                    other => other,
                })
                .collect()
        }
        fn close<'a>(&'a self, context: &'a AdapterCloseContext) -> PortFuture<'a, ()> {
            Box::pin(async move {
                self.inner.close(context).await?;
                if !self.closed.swap(true, Ordering::SeqCst) {
                    trace(&self.directory,json!({"kind":"close","pid":std::process::id(),"binding":context.binding_set_id})).await?;
                }
                Ok(())
            })
        }
    }
    struct FileWriter {
        inner: Arc<dyn ToolExecutor>,
        directory: std::path::PathBuf,
    }
    impl ToolExecutor for FileWriter {
        fn execute<'a>(
            &'a self,
            args: &'a JsonObject,
            context: &'a ToolExecutionContext,
        ) -> PortFuture<'a, ToolExecutionResult> {
            Box::pin(async move {
                // The delegate validates the approval actor, segment and frozen inputs.
                let mut result = self.inner.execute(args, context).await?;
                if !matches!(&result.outcome, ToolExecutionOutcome::Succeeded { .. }) {
                    return Ok(result);
                }
                let report = json!({"query":args["query"],"workspace_id":args["workspace_id"],"record_id":args["record_id"]});
                let path = self.directory.join(format!(
                    "{}.json",
                    args["record_id"].as_str().expect("validated UUID")
                ));
                let mut file = tokio::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .await
                    .map_err(io_error)?;
                file.write_all(report.to_string().as_bytes())
                    .await
                    .map_err(io_error)?;
                file.sync_all().await.map_err(io_error)?;
                let digest = canonical_digest(&report);
                trace(&self.directory,json!({"kind":"write","pid":std::process::id(),"binding":context.binding_set_id,"digest":digest})).await?;
                result.receipt = Some(
                    json!({"effect_id":"report-file","record_id":args["record_id"],"content_hash":digest}),
                );
                Ok(result)
            })
        }
    }
    fn file_registry(
        scope: &Scope,
        factory: Arc<dyn AdapterFactory>,
    ) -> Result<AdapterRegistry, ContractError> {
        let definition = definition();
        let value = json!({"thread_id":"prepared-report-thread"});
        let state = AdapterBindingState {
            scope: scope.clone(),
            session_id: id("session"),
            adapter_binding: id("reports"),
            adapter: reference("report-adapter"),
            definition_digest: definition.digest(),
            state_ref: ProtectedRecord::new(id("prepared-mapping"), 1, value.clone())
                .reference()
                .clone(),
            value,
        };
        AdapterRegistry::new(
            scope.clone(),
            vec![AdapterRegistration {
                definition,
                factory,
            }],
            vec![ConnectionRegistration {
                binding: ConnectorBindingRef {
                    binding_id: id("data"),
                    connector_id: id("report-service"),
                    version: id("1"),
                },
                metadata: metadata(ComponentKind::Connector, "report-service"),
                connection_ref: reference("report-account"),
            }],
            vec![],
            vec![],
            vec![state],
        )
    }
    fn file_agent(
        scope: &Scope,
        store: Arc<SqliteStateStore>,
        model: Arc<Model>,
        resolver: Arc<Resolver>,
        counters: Arc<Counters>,
        directory: std::path::PathBuf,
    ) -> Result<(Agent, Arc<Catalog>), ContractError> {
        let registry = Arc::new(file_registry(
            scope,
            Arc::new(FileFactory {
                inner: Arc::new(Factory {
                    store: store.clone(),
                    counters,
                }),
                directory,
            }),
        )?);
        let catalog = Arc::new(Catalog {
            registry: registry.clone(),
            calls: AtomicUsize::new(0),
        });
        let policy = Arc::new(PolicyGate::new(Arc::new(Policy), Duration::from_secs(5))?);
        let clock = Arc::new(SystemClock::new());
        let runtime = Arc::new(AdapterRuntime::new(
            registry,
            store.clone(),
            policy.clone(),
            clock.clone(),
        ));
        let profile = AgentProfile::from_json(
            r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"writer","version":"1",
        "name":"Writer","description":"Synthetic adapter consumer","instructions":{"text":"Write the report after authorization"},
        "model_binding":"primary","tools":[{"adapter_binding":"reports","export_id":"save","alias":"write"}],"skills":[],
        "connectors":[{"binding_id":"data","connector_id":"report-service","version":"1"}],
        "adapters":[{"binding_id":"reports","adapter_id":"report-adapter","version":"1","connections":{"main":"data"}}],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":3,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}
    }"#,
        )?;
        Ok((
            create_agent(
                profile,
                AgentBindings {
                    scope: scope.clone(),
                    state: store,
                    policy: policy.clone(),
                    profile_resolver: catalog.clone(),
                    model_exchange: Arc::new(
                        ModelExchange::new(model, policy)
                            .with_route_inspector(Arc::new(Inspector), Duration::from_secs(5))?,
                    ),
                    router: Arc::new(PolicyModelRouter::new(routing(scope)?)?),
                    host_instructions: vec!["Use only authorized inputs.".into()],
                    system_inputs: system_inputs()?,
                    tools: None,
                    hooks: None,
                    components: Some(runtime),
                    context_sources: None,
                    context_token_estimator: None,
                    context_runtime: None,
                    verification: None,
                    skills: None,
                    artifacts: None,
                    system_input_resolver: Some(resolver),
                    external_receipt_verifier: None,
                    clock,
                    ids: Arc::new(RandomIdSource),
                    token_estimator: Arc::new(Estimate),
                    settings: AgentSettings {
                        require_durable: true,
                        max_output_tokens: 128.try_into().unwrap(),
                        ..Default::default()
                    },
                },
            )?,
            catalog,
        ))
    }

    async fn worker(
        directory: &std::path::Path,
        mode: &str,
    ) -> Result<std::process::ExitStatus, Box<dyn std::error::Error>> {
        let mut child = std::process::Command::new(std::env::current_exe()?)
            .arg(directory)
            .arg(mode)
            .spawn()?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = child.try_wait()? {
                return Ok(status);
            }
            if tokio::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err("report worker watchdog expired".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        let args: Vec<_> = std::env::args_os().collect();
        if args.len() == 3 {
            return child(
                std::path::Path::new(&args[1]),
                args[2].to_str().ok_or("invalid mode")?,
            )
            .await;
        }
        let directory = TemporaryDatabase(
            std::env::temp_dir().join(format!("wickle-report-{}", RandomIdSource.next_id()?)),
        );
        std::fs::create_dir(&directory.0)?;
        assert_eq!(worker(&directory.0, "wait").await?.code(), Some(73));
        assert!(!directory.0.join(format!("{RECORD}.json")).exists());
        assert!(worker(&directory.0, "resume").await?.success());
        let report: Value =
            serde_json::from_slice(&std::fs::read(directory.0.join(format!("{RECORD}.json")))?)?;
        assert_eq!(
            report,
            json!({"query":"report","workspace_id":WORKSPACE,"record_id":RECORD})
        );
        assert!(!directory.0.join(format!("{NEW_RECORD}.json")).exists());
        let events: Vec<Value> = std::fs::read_to_string(directory.0.join("trace.jsonl"))?
            .lines()
            .map(parse_json)
            .collect::<Result<_, _>>()?;
        let opens: Vec<_> = events.iter().filter(|e| e["kind"] == "open").collect();
        let closes: Vec<_> = events.iter().filter(|e| e["kind"] == "close").collect();
        let writes: Vec<_> = events.iter().filter(|e| e["kind"] == "write").collect();
        assert_eq!(opens.len(), 2);
        assert_eq!(closes.len(), 2);
        assert_eq!(writes.len(), 1);
        assert_ne!(opens[0]["pid"], opens[1]["pid"]);
        assert_ne!(opens[0]["binding"], opens[1]["binding"]);
        for open in opens {
            assert!(closes.iter().any(|close|close["binding"]==open["binding"] && close["pid"]==open["pid"]));
        }
        assert_eq!(writes[0]["digest"], json!(canonical_digest(&report)));
        println!(
            "report consumer: real approval wait, abrupt process exit, new process/binding, one durable file write with original UUIDs, explicit release and duplicate resume without extra work passed"
        );
        Ok(())
    }
    async fn child(
        directory: &std::path::Path,
        mode: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !matches!(mode, "wait" | "resume") {
            return Err("invalid worker mode".into());
        }
        let scope = Scope {
            tenant_id: id("tenant"),
            workspace_id: id("workspace"),
            user_id: None,
        };
        let store = Arc::new(SqliteStateStore::open(directory.join("state.sqlite3"))?);
        let counters = Arc::new(Counters::default());
        let model = Arc::new(Model {
            route: routing(&scope)?.route_for_binding(&reference("primary"))?,
            propose: mode == "wait",
            calls: AtomicUsize::new(0),
        });
        let resolver = Arc::new(Resolver {
            value: if mode == "wait" { RECORD } else { NEW_RECORD },
            revision: if mode == "wait" {
                "record-A"
            } else {
                "record-B"
            },
            calls: AtomicUsize::new(0),
        });
        let (agent, _catalog) = file_agent(
            &scope,
            store.clone(),
            model.clone(),
            resolver.clone(),
            counters.clone(),
            directory.to_owned(),
        )?;
        if mode == "wait" {
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
            let handle = completed(agent.start(request, caller.clone()).await?)?;
            assert_eq!(
                completed(handle.outcome(&caller).await?)?.result.status(),
                RunStatus::Waiting
            );
            release_finished(&handle, &caller).await?;
            let saved = store.load(&scope, handle.run_id()).await?;
            let wait = saved.snapshot.wait.ok_or("missing wait")?;
            let WaitTarget::Approval { target } = wait.target else {
                return Err("not an approval wait".into());
            };
            let command = ResumeCommand {
                run_id: handle.run_id().clone(),
                expected_revision: saved.snapshot.revision,
                command_id: id("approve-report"),
                action: ResumeAction::Approve {
                    wait_id: wait.wait_id,
                    target,
                },
            };
            tokio::fs::write(
                directory.join("command.json"),
                serde_json::to_vec(&command)?,
            )
            .await?;
            assert_eq!(counters.writes.load(Ordering::SeqCst), 0);
            std::process::exit(73);
        }
        let command =
            ResumeCommand::from_json(&std::fs::read_to_string(directory.join("command.json"))?)?;
        let reviewer = context(&scope, true);
        let handle = completed(agent.resume(command.clone(), reviewer.clone()).await?)?;
        let outcome = completed(handle.outcome(&reviewer).await?)?;
        assert_eq!(
            outcome.result.status(),
            RunStatus::Succeeded,
            "resume result: {:?}, usage: {:?}",
            outcome.result,
            outcome.usage
        );
        release_finished(&handle, &reviewer).await?;
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        assert_eq!(counters.writes.load(Ordering::SeqCst), 1);
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
        let replay = completed(agent.resume(command, reviewer.clone()).await?)?;
        assert_eq!(completed(replay.outcome(&reviewer).await?)?, outcome);
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        assert_eq!(counters.opens.load(Ordering::SeqCst), 1);
        assert_eq!(counters.closes.load(Ordering::SeqCst), 1);
        assert_eq!(counters.writes.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    host::run().await
}
```
