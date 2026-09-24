# 18장 전체 Rust 구현과 테스트

[강의로](../18-hooks.md) · [전체 변경 패치](../solutions/18-hooks.patch)

기준 `fbe1ed6b258302e472dd90b0d36a87c2c1a43b66`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

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
mod hooks;
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
    /// Optional scope-bound lifecycle runtime; selected definitions are pinned at admission.
    pub hooks: Option<Arc<HookRuntime>>,
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
    observer_error: Mutex<Option<ContractError>>,
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
            observer_error: Mutex::new(None),
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
        || profile
            .context_sources
            .as_ref()
            .is_some_and(|v| !v.is_empty())
        || profile.extensions.as_ref().is_some_and(|v| !v.is_empty())
        || profile.context_policy.strategy.as_str() != "bounded"
    {
        return Err(fail(ErrorCode::CapabilityUnsupported, "agent.profile"));
    }
    match &bindings.hooks {
        Some(hooks) => {
            if hooks.scope() != &bindings.scope {
                return Err(fail(ErrorCode::AccessDenied, "agent.hooks_scope"));
            }
            hooks.plan(&profile)?;
        }
        None if profile
            .hooks
            .as_ref()
            .is_some_and(|hooks| !hooks.is_empty()) =>
        {
            return Err(fail(ErrorCode::CapabilityUnsupported, "agent.hooks"));
        }
        None => {}
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

/// Protected observer reports and a local report-persistence failure, independent
/// of the saved execution outcome. An observer does not change business success.
#[derive(Debug)]
pub struct HookObservationView {
    /// Reports that the StateStore actually accepted.
    pub reports: Vec<HookObservation>,
    /// A local failure to persist an observer report, when this handle knows it.
    pub local_error: Option<ContractError>,
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
    /// Read committed hook observations under current protected-details permission.
    pub async fn hook_observations(
        &self,
        context: &ExecutionContext,
    ) -> Result<Guarded<HookObservationView>, ContractError> {
        self.agent.check_scope(context)?;
        let bindings = &self.agent.inner.bindings;
        let request = PolicyRequest {
            owner_scope: bindings.scope.clone(),
            resource_id: self.run_id.clone(),
            action: PolicyAction::ReadRunDetails {},
        };
        if let Guarded::ApprovalRequired(challenge) = bindings
            .policy
            .guard(&request, context, None, None, || async { Ok(()) })
            .await?
        {
            return Ok(Guarded::ApprovalRequired(challenge));
        }
        let reports = caller_read(
            context,
            None,
            bindings
                .state
                .read_hook_observations(&bindings.scope, &self.run_id),
        )
        .await?;
        if reports
            .iter()
            .any(|report| report.scope != bindings.scope || report.run_id != self.run_id)
        {
            return Err(fail(
                ErrorCode::InvalidSnapshot,
                "agent.hook_observation_scope",
            ));
        }
        let local_error = self
            .current_local()?
            .map(|local| {
                local
                    .observer_error
                    .lock()
                    .map(|error| error.clone())
                    .map_err(|_| fail(ErrorCode::InvalidContract, "agent.observer_state"))
            })
            .transpose()?
            .flatten();
        bindings
            .policy
            .guard(&request, context, None, None, || async {
                Ok(HookObservationView {
                    reports,
                    local_error,
                })
            })
            .await
    }
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
        Ok(self
            .agent
            .inner
            .runs
            .lock()
            .map_err(|_| fail(ErrorCode::InvalidContract, "agent.local_state"))?
            .get(&self.run_id)
            .filter(|local| local.segment_start_revision == self.segment_start_revision)
            .cloned()
            .or_else(|| self.local.clone()))
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
            let completed = error.is_none()
                && driver_local
                    .observer_error
                    .lock()
                    .is_ok_and(|error| error.is_none());
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
        let hook_record = bindings
            .hooks
            .as_ref()
            .map(|hooks| {
                let plan = hooks.plan(profile.profile())?;
                Ok::<_, ContractError>(ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(&plan)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.hooks"))?,
                ))
            })
            .transpose()?;
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
            hook_plan_ref: hook_record
                .as_ref()
                .map(|record| record.reference().clone()),
            hook_applications: vec![],
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
                records: [
                    vec![request_record, prompt_record, inputs_record, routing_record],
                    hook_record.into_iter().collect(),
                ]
                .concat(),
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
                self.after_run(&saved, &context, local).await;
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
                let round = self.tool_round(budget).await?;
                let result = round.execute(&request_id, context, budget).await;
                self.remember_observer_error(local, round.observer_error());
                match result {
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
                    let result = round.execute(&response.request_id, context, budget).await;
                    self.remember_observer_error(local, round.observer_error());
                    match result {
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
        let run_context = self.before_run(budget, context).await?;
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
        let context_items = self
            .before_model(
                &step,
                saved.snapshot.request.input.clone(),
                run_context,
                context,
                budget,
            )
            .await?;
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
            context_items,
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
                    local,
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
    context_items: Vec<ContextItem>,
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
                    context_items: &self.context_items,
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

## `crates/wickle/src/agent/hooks.rs`

```rust
use super::*;

impl Agent {
    pub(super) async fn before_run(
        &self,
        budget: &RunBudget,
        context: &ExecutionContext,
    ) -> Result<Vec<ContextItem>, ContractError> {
        let Some(hooks) = &self.inner.bindings.hooks else {
            return Ok(vec![]);
        };
        let saved = self
            .inner
            .bindings
            .state
            .load(budget.scope(), budget.run_id())
            .await?;
        let transformed = hooks
            .transform(
                HookTarget::BeforeRun,
                HookInput::BeforeRun {
                    user_input: saved.snapshot.request.input.clone(),
                    context_items: vec![],
                },
                context,
                budget,
            )
            .await?;
        Ok(transformed.context_items)
    }
    pub(super) async fn before_model(
        &self,
        step: &Id,
        user_input: Vec<InputContent>,
        context_items: Vec<ContextItem>,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<Vec<ContextItem>, ContractError> {
        let Some(hooks) = &self.inner.bindings.hooks else {
            return Ok(context_items);
        };
        let transformed = hooks
            .transform(
                HookTarget::BeforeModel {
                    model_step_id: step.clone(),
                },
                HookInput::BeforeModel {
                    user_input,
                    context_items,
                },
                context,
                budget,
            )
            .await?;
        Ok(transformed.context_items)
    }
    pub(super) fn remember_observer_error(&self, local: &LocalRun, error: Option<ContractError>) {
        if let Some(error) = error {
            if let Ok(mut slot) = local.observer_error.lock() {
                *slot = Some(fail(error.code, "hooks.observer_report"));
            }
        }
    }
    pub(super) async fn after_run(
        &self,
        saved: &StoredRun,
        context: &ExecutionContext,
        local: &LocalRun,
    ) {
        let Some(hooks) = &self.inner.bindings.hooks else {
            return;
        };
        let Some(outcome) = &saved.snapshot.outcome else {
            return;
        };
        if !saved.snapshot.status.is_terminal() {
            return;
        }
        let mut data = context.data.clone();
        data.system_inputs = None;
        let cleanup = ExecutionContext::new(data, CancellationToken::new());
        let observed = caller_read(&cleanup, Some(Duration::from_secs(30)), async {
            let page = self
                .inner
                .bindings
                .state
                .read_events(
                    &saved.snapshot.scope,
                    &saved.snapshot.run_id,
                    saved.snapshot.last_event_seq.saturating_sub(1),
                    1,
                )
                .await?;
            let Some(RunEvent {
                payload: RunEventPayload::RunFinished { outcome_ref },
                ..
            }) = page.events.last()
            else {
                return Err(fail(ErrorCode::InvalidSnapshot, "hooks.terminal_event"));
            };
            hooks
                .observe(
                    &saved.snapshot.run_id,
                    HookTarget::AfterRun {
                        outcome_ref: outcome_ref.clone(),
                        revision: saved.snapshot.revision,
                    },
                    HookInput::run_observed(outcome),
                    &cleanup,
                )
                .await
        })
        .await;
        self.remember_observer_error(local, observed.err());
    }
    pub(super) async fn after_tool(
        &self,
        run_id: &Id,
        target: HookTarget,
        input: HookInput,
        context: &ExecutionContext,
    ) -> Option<ContractError> {
        let Some(hooks) = &self.inner.bindings.hooks else {
            return None;
        };
        let mut data = context.data.clone();
        data.system_inputs = None;
        let cleanup = ExecutionContext::new(data, CancellationToken::new());
        hooks
            .observe(run_id, target, input, &cleanup)
            .await
            .err()
            .map(|error| fail(error.code, "hooks.observer_report"))
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
            Ok((receipt, prompt, true, observer_error)) => {
                Ok(Guarded::Completed(self.launch_resumed(
                    command.run_id,
                    &receipt,
                    prompt,
                    context,
                    lease,
                    observer_error,
                )?))
            }
            Ok((receipt, _, false, _)) => {
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
    ) -> Result<(ResumeReceipt, PromptSnapshot, bool, Option<ContractError>), ContractError> {
        let bindings = &self.inner.bindings;
        let saved = self
            .resume_read(
                context,
                bindings.state.load(&bindings.scope, &command.run_id),
            )
            .await?;
        let prompt = self.restore_resume_runtime(&saved, context).await?;
        if let Some(receipt) = accepted(&saved.snapshot, command)? {
            return Ok((receipt.clone(), prompt, false, None));
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
        let observation = prepared.as_ref().and_then(|prepared| {
            if let RunEventPayload::ToolSettled { result_ref } = &prepared.event.payload {
                Some((
                    HookTarget::AfterTool {
                        call_id: prepared.result.call_id.clone(),
                        result_ref: result_ref.clone(),
                    },
                    HookInput::tool_observed(&prepared.result.call_id, &prepared.result),
                ))
            } else {
                None
            }
        });
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
                let observer_error = if let Some((target, input)) = observation {
                    self.after_tool(&command.run_id, target, input, context)
                        .await
                } else {
                    None
                };
                return Ok((receipt, prompt, true, observer_error));
            }
            return Err(error);
        }
        let observer_error = if let Some((target, input)) = observation {
            self.after_tool(&command.run_id, target, input, context)
                .await
        } else {
            None
        };
        Ok((receipt, prompt, true, observer_error))
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
        match (&saved.snapshot.hook_plan_ref, &bindings.hooks) {
            (Some(reference), Some(hooks))
                if hooks.plan(saved.snapshot.profile.profile())?.digest() == reference.digest => {}
            (None, None) => {}
            (None, Some(hooks))
                if hooks
                    .plan(saved.snapshot.profile.profile())?
                    .definitions()
                    .is_empty() => {}
            _ => return Err(fail(ErrorCode::ContextMismatch, "agent.pinned_hooks")),
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
        receipt: &ResumeReceipt,
        prompt: PromptSnapshot,
        context: ExecutionContext,
        lease: RunLease,
        observer_error: Option<ContractError>,
    ) -> Result<RunHandle, ContractError> {
        let segment_start_revision = receipt.accepted_revision;
        let expired = receipt.expired;
        let local = Arc::new(LocalRun::new(segment_start_revision));
        self.remember_observer_error(&local, observer_error);
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
            let completed = error.is_none()
                && driver_local
                    .observer_error
                    .lock()
                    .is_ok_and(|error| error.is_none());
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
            let mut observations = vec![];
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
                if let RunEventPayload::ToolSettled { result_ref } = &prepared.event.payload {
                    observations.push((
                        HookTarget::AfterTool {
                            call_id: prepared.result.call_id.clone(),
                            result_ref: result_ref.clone(),
                        },
                        HookInput::tool_observed(&prepared.result.call_id, &prepared.result),
                    ));
                }
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
                    != RunStatus::Cancelled
                {
                    return Err(error);
                }
            }
            let saved = bindings.state.load(&bindings.scope, &run_id).await?;
            let local = Arc::new(LocalRun::new(segment_revision(&saved.snapshot)));
            for (target, input) in observations {
                self.remember_observer_error(
                    &local,
                    self.after_tool(&run_id, target, input, &context).await,
                );
            }
            self.after_run(&saved, &context, &local).await;
            local.done.store(true, Ordering::Release);
            if local
                .observer_error
                .lock()
                .is_ok_and(|error| error.is_some())
            {
                self.inner
                    .runs
                    .lock()
                    .map_err(|_| fail(ErrorCode::InvalidContract, "agent.observer_state"))?
                    .insert(run_id.clone(), local);
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
        let mut round = SerialToolRound::new(
            registry,
            binder,
            bindings.policy.clone(),
            bindings.ids.clone(),
        )
        .with_limits(bindings.settings.tool_execution_limits)?;
        if let Some(hooks) = &bindings.hooks {
            round = round.with_hooks(hooks.clone());
        }
        Ok(round)
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
        local: &LocalRun,
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
            self.remember_observer_error(local, round.observer_error());
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
    /// Bounded data added by a selected lifecycle hook; it carries no system authority.
    Hook,
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

## `crates/wickle/src/hooks.rs`

```rust
//! Bounded lifecycle transformations and observations. Hooks receive selected
//! data, never a mutable Run, credentials, or the system-input map.

use crate::*;
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};
use tokio_util::sync::CancellationToken;

mod records;
mod runtime;
pub(crate) use records::validate_application_chain;

/// Exact callback contract pinned for one Run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookDefinition {
    /// Registered identity and exact version.
    pub hook: VersionedRef,
    /// Single permitted lifecycle position.
    pub position: HookPosition,
    /// Lower priorities execute first; IDs break ties.
    pub priority: i32,
    /// Only optional before_run callback failures may continue as warnings.
    pub required: bool,
    /// Positive finite callback timeout, at most one day.
    pub timeout_ms: u64,
    /// Positive byte bound on serialized callback output.
    pub max_output_bytes: usize,
}
impl HookDefinition {
    /// Identity of the complete callback contract, including its bounds.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
    /// Reject unbounded callback contracts before admission.
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.timeout_ms == 0
            || self.timeout_ms > 86_400_000
            || self.max_output_bytes == 0
            || self.max_output_bytes > 16_777_216
        {
            return Err(hook_error(ErrorCode::InvalidContract, "hooks.definition"));
        }
        Ok(())
    }
}

/// A stable logical lifecycle target, independent of physical retry attempts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HookTarget {
    /// Initial admitted Run preparation.
    BeforeRun,
    /// One logical step, reused across transport retry and route fallback.
    BeforeModel {
        /// Logical step identity.
        model_step_id: Id,
    },
    /// One original planned call, before system binding.
    BeforeTool {
        /// Core logical call identity.
        call_id: Id,
    },
    /// Already committed tool observation.
    AfterTool {
        /// Core logical call identity.
        call_id: Id,
        /// Exact protected ToolResult.
        result_ref: RecordRef,
    },
    /// Already committed terminal outcome.
    AfterRun {
        /// Exact protected RunOutcome.
        outcome_ref: RecordRef,
        /// Terminal snapshot revision.
        revision: u64,
    },
}
impl HookTarget {
    /// Lifecycle position fixed by this target.
    pub fn position(&self) -> HookPosition {
        match self {
            Self::BeforeRun => HookPosition::BeforeRun,
            Self::BeforeModel { .. } => HookPosition::BeforeModel,
            Self::BeforeTool { .. } => HookPosition::BeforeTool,
            Self::AfterTool { .. } => HookPosition::AfterTool,
            Self::AfterRun { .. } => HookPosition::AfterRun,
        }
    }
}

/// Selected safe data. Raw tool receipts, opaque continuations and hidden inputs
/// are deliberately absent. Data payloads accept only text and JSON content.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HookInput {
    /// Original user request and prior additions in this transform chain.
    BeforeRun {
        /// Original user content.
        user_input: Vec<InputContent>,
        /// Accumulated safe context.
        context_items: Vec<ContextItem>,
    },
    /// Route-independent data for a logical model step.
    BeforeModel {
        /// Original user content.
        user_input: Vec<InputContent>,
        /// Already supplied context and chain additions.
        context_items: Vec<ContextItem>,
    },
    /// Model-visible contract and original/effective argument maps.
    BeforeTool {
        /// Model-facing descriptor only.
        tool: ModelTool,
        /// Pinned full-descriptor identity, without its hidden schema.
        descriptor_digest: JsonDigest,
        /// Pinned compiler contract identity.
        compiled_digest: JsonDigest,
        /// Unchanged original model proposal.
        original_model_inputs: JsonObject,
        /// Current transform-chain value.
        model_inputs: JsonObject,
    },
    /// Safe committed observation, excluding receipt and diagnostic references.
    AfterTool {
        /// Original logical call identity.
        call_id: Id,
        /// Committed completion status.
        status: ToolResultStatus,
        /// Committed effect status.
        effect: ToolEffect,
        /// Safe text/JSON output only.
        content: Vec<InputContent>,
        /// Safe classified failure, if any.
        error_code: Option<Id>,
    },
    /// Safe terminal summary, excluding artifact and protected-record references.
    AfterRun {
        /// Terminal status.
        status: RunStatus,
        /// Safe text/JSON response only.
        output: Vec<InputContent>,
        /// Charged execution usage.
        usage: BudgetUsage,
    },
}
impl fmt::Debug for HookInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HookInput(<protected>)")
    }
}
impl HookInput {
    /// Derive a reference-free summary from a committed tool result.
    pub fn tool_observed(call_id: &Id, result: &ToolResult) -> Self {
        Self::AfterTool {
            call_id: call_id.clone(),
            status: result.status,
            effect: result.effect,
            content: safe_summary(&result.content),
            error_code: result.error.as_ref().map(|error| error.code.clone()),
        }
    }
    /// Derive a reference-free summary from an authoritative terminal outcome.
    pub fn run_observed(outcome: &RunOutcome) -> Self {
        Self::AfterRun {
            status: outcome.result.status(),
            output: safe_summary(&outcome.output),
            usage: outcome.usage.clone(),
        }
    }
    /// Digest of the exact safe callback input.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
    fn validate(&self, target: &HookTarget) -> Result<(), ContractError> {
        let valid = match (self, target) {
            (
                Self::BeforeRun {
                    user_input,
                    context_items,
                },
                HookTarget::BeforeRun,
            )
            | (
                Self::BeforeModel {
                    user_input,
                    context_items,
                },
                HookTarget::BeforeModel { .. },
            ) => {
                safe_content(user_input)
                    && context_items.iter().all(|item| safe_content(&item.content))
            }
            (
                Self::BeforeTool {
                    tool,
                    original_model_inputs,
                    model_inputs,
                    ..
                },
                HookTarget::BeforeTool { .. },
            ) => {
                let validator = crate::tool_schema::compile_validator(&tool.model_input_schema)?;
                validator.is_valid(
                    &serde_json::to_value(original_model_inputs)
                        .map_err(|_| hook_error(ErrorCode::InvalidJson, "hooks.input"))?,
                ) && validator.is_valid(
                    &serde_json::to_value(model_inputs)
                        .map_err(|_| hook_error(ErrorCode::InvalidJson, "hooks.input"))?,
                )
            }
            (
                Self::AfterTool {
                    call_id, content, ..
                },
                HookTarget::AfterTool {
                    call_id: target, ..
                },
            ) => call_id == target && safe_content(content),
            (Self::AfterRun { status, output, .. }, HookTarget::AfterRun { .. }) => {
                status.is_terminal() && safe_content(output)
            }
            _ => false,
        };
        if !valid {
            return Err(hook_error(ErrorCode::InvalidContract, "hooks.input"));
        }
        Ok(())
    }
}
fn safe_content(content: &[InputContent]) -> bool {
    content.iter().all(|content| {
        matches!(
            content,
            InputContent::Text { .. } | InputContent::Json { .. }
        )
    })
}
fn safe_summary(content: &[InputContent]) -> Vec<InputContent> {
    content
        .iter()
        .filter(|item| matches!(item, InputContent::Text { .. } | InputContent::Json { .. }))
        .cloned()
        .collect()
}

/// Additional data; the core assigns provenance, scope, ID and lifetime.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookContextAddition {
    /// Text/JSON data, never raw message or opaque blocks.
    pub content: Vec<InputContent>,
    /// Required data must fit in full; importance grants no authority.
    pub priority: ContextPriority,
}
/// Position-specific output, never an internal-state patch or a next callback.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HookOutput {
    /// Append data during before_run or before_model.
    Context {
        /// Complete additions to validate and persist.
        additions: Vec<HookContextAddition>,
    },
    /// Replace only model-owned arguments, optionally denying this call.
    Tool {
        /// Must still satisfy the model-visible input schema.
        model_inputs: JsonObject,
        /// Safe denial code; absence grants no permission.
        deny: Option<Id>,
    },
    /// Observer completed; cannot change a tool or Run result.
    Observed {},
}
impl fmt::Debug for HookOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HookOutput(<protected>)")
    }
}

/// Current authorization and finite callback controls, without system inputs.
#[derive(Debug, Clone)]
pub struct HookContext {
    /// Exact namespace.
    pub scope: Scope,
    /// Owning Run identity.
    pub run_id: Id,
    /// Exact selected callback identity.
    pub hook: VersionedRef,
    /// Logical lifecycle target.
    pub target: HookTarget,
    /// Current authenticated actor.
    pub principal_ref: Id,
    /// Current Host grant.
    pub capability_grant_ref: Id,
    /// Cancelled on timeout, cancellation or callback completion.
    pub cancellation: CancellationToken,
    /// Finite callback deadline.
    pub deadline: tokio::time::Instant,
}
/// Trusted Host callback. It must not hide required business writes or dispatch
/// another execution; in-process Rust code is not an isolation sandbox.
pub trait HookHandler: Send + Sync {
    /// Apply one bounded transformation or read-only observation.
    fn call<'a>(
        &'a self,
        input: &'a HookInput,
        context: &'a HookContext,
    ) -> PortFuture<'a, HookOutput>;
}
/// Exact contract associated with an existing callback.
#[derive(Clone)]
pub struct HookRegistration {
    /// Immutable selected contract.
    pub definition: HookDefinition,
    /// Existing Host-owned implementation.
    pub handler: Arc<dyn HookHandler>,
}
/// Scope-bound immutable callback catalog.
pub struct HookRegistry {
    scope: Scope,
    entries: Vec<HookRegistration>,
}
impl HookRegistry {
    /// Register existing callbacks; this performs no callback or external I/O.
    pub fn new(scope: Scope, mut entries: Vec<HookRegistration>) -> Result<Self, ContractError> {
        if entries.len() > 64 {
            return Err(hook_error(ErrorCode::InvalidContract, "hooks.count"));
        }
        let mut seen = std::collections::BTreeSet::new();
        for entry in &entries {
            entry.definition.validate()?;
            if !seen.insert(entry.definition.hook.id.clone()) {
                return Err(hook_error(ErrorCode::InvalidReference, "hooks.duplicate"));
            }
        }
        entries.sort_by(|a, b| {
            a.definition
                .priority
                .cmp(&b.definition.priority)
                .then(a.definition.hook.id.cmp(&b.definition.hook.id))
        });
        Ok(Self { scope, entries })
    }
    /// Exact registered namespace.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Pin only exact profile-selected catalog definitions in execution order.
    pub fn plan(&self, profile: &AgentProfile) -> Result<HookPlan, ContractError> {
        let mut definitions = Vec::new();
        for selected in profile.hooks.iter().flatten() {
            let HookRef::Catalog(selected) = selected else {
                return Err(hook_error(ErrorCode::CapabilityUnsupported, "hooks.export"));
            };
            let entry = self
                .entries
                .iter()
                .find(|entry| {
                    entry.definition.hook.id == selected.hook_id
                        && entry.definition.hook.version == selected.version
                        && entry.definition.position == selected.position
                })
                .ok_or_else(|| hook_error(ErrorCode::ComponentUnavailable, "hooks.selection"))?;
            definitions.push(entry.definition.clone());
        }
        definitions.sort_by(|a, b| a.priority.cmp(&b.priority).then(a.hook.id.cmp(&b.hook.id)));
        let plan = HookPlan {
            scope: self.scope.clone(),
            definitions,
        };
        plan.validate(profile)?;
        Ok(plan)
    }
}

/// Immutable selected definitions, serialized for protected Run storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookPlan {
    scope: Scope,
    definitions: Vec<HookDefinition>,
}
impl HookPlan {
    /// Exact owning namespace.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Selected definitions in their stable execution order.
    pub fn definitions(&self) -> &[HookDefinition] {
        &self.definitions
    }
    /// Canonical identity of scope, definitions, order and bounds.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
    /// Restore only the exact trusted protected plan identity.
    pub fn restore(
        json: &str,
        scope: &Scope,
        expected_digest: &JsonDigest,
    ) -> Result<Self, ContractError> {
        let plan: Self = serde_json::from_str(json)
            .map_err(|_| hook_error(ErrorCode::InvalidSnapshot, "hooks.plan"))?;
        if plan.scope() != scope || &plan.digest() != expected_digest {
            return Err(hook_error(
                ErrorCode::InvalidSnapshot,
                "hooks.plan_identity",
            ));
        }
        plan.validate_order()?;
        Ok(plan)
    }
    /// Confirm the plan is exactly the profile's selected catalog hooks.
    pub fn validate(&self, profile: &AgentProfile) -> Result<(), ContractError> {
        self.validate_order()?;
        let selections: Vec<_> = profile.hooks.iter().flatten().collect();
        let mut selected_ids = std::collections::BTreeSet::new();
        if selections.len() != self.definitions.len()
            || selections.iter().any(|selection| match selection {
                HookRef::Catalog(selected) => {
                    !selected_ids.insert(&selected.hook_id)
                        || !self.definitions.iter().any(|definition| {
                            definition.hook.id == selected.hook_id
                                && definition.hook.version == selected.version
                                && definition.position == selected.position
                        })
                }
                HookRef::Export(_) => true,
            })
        {
            return Err(hook_error(
                ErrorCode::InvalidSnapshot,
                "hooks.plan_selection",
            ));
        }
        Ok(())
    }
    fn validate_order(&self) -> Result<(), ContractError> {
        if self.definitions.len() > 64 {
            return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.plan_count"));
        }
        let mut seen = std::collections::BTreeSet::new();
        for (index, definition) in self.definitions.iter().enumerate() {
            definition.validate()?;
            if !seen.insert(definition.hook.id.clone())
                || (index > 0 && {
                    let prior = &self.definitions[index - 1];
                    (prior.priority, &prior.hook.id) > (definition.priority, &definition.hook.id)
                })
            {
                return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.plan_order"));
            }
        }
        Ok(())
    }
}

/// One durably applied transform. The result record retains input and output so
/// restart never substitutes freshly transformed arguments for the saved ones.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookApplication {
    /// Exact selected hook.
    pub hook: VersionedRef,
    /// Complete pinned definition identity.
    pub definition_digest: JsonDigest,
    /// Logical lifecycle target.
    pub target: HookTarget,
    /// Digest of the safe input seen by this callback.
    pub input_digest: JsonDigest,
    /// Exact stored transformation or classified optional failure.
    pub result_ref: RecordRef,
}
/// Protected body referenced by HookApplication.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookApplicationRecord {
    /// Exact namespace.
    pub scope: Scope,
    /// Owning Run.
    pub run_id: Id,
    /// Exact selected hook.
    pub hook: VersionedRef,
    /// Definition identity.
    pub definition_digest: JsonDigest,
    /// Logical target.
    pub target: HookTarget,
    /// Input before this callback's transformation.
    pub input: HookInput,
    /// Validated output; absent only for a permitted optional callback failure.
    pub output: Option<HookOutput>,
    /// Core-stamped data created from Context additions.
    pub context_items: Vec<ContextItem>,
    /// Safe classified optional failure.
    pub failure: Option<Id>,
}
impl fmt::Debug for HookApplicationRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HookApplicationRecord(<protected>)")
    }
}
/// Current chain result, including its stored application prefix.
#[derive(Clone)]
pub struct HookTransform {
    /// Accumulated supplied context and newly added data.
    pub context_items: Vec<ContextItem>,
    /// Final model-owned arguments, only for before_tool.
    pub model_inputs: Option<JsonObject>,
    /// A persisted denial, never overridden by another Hook.
    pub deny: Option<Id>,
    /// Applied definitions in deterministic chain order.
    pub applications: Vec<HookApplication>,
}
impl fmt::Debug for HookTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HookTransform")
            .field("applications", &self.applications.len())
            .field("denied", &self.deny.is_some())
            .finish_non_exhaustive()
    }
}
/// Observer result that cannot alter an already committed outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum HookObservationStatus {
    /// Read-only callback completed.
    Completed,
    /// Callback or output contract failed safely.
    Failed {
        /// Safe error code, never raw callback text.
        code: Id,
    },
}
/// Immutable observation report stored outside the Run snapshot/event endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookObservation {
    /// Exact namespace.
    pub scope: Scope,
    /// Owning Run.
    pub run_id: Id,
    /// Exact selected observer.
    pub hook: VersionedRef,
    /// Complete contract identity.
    pub definition_digest: JsonDigest,
    /// Exact committed ToolResult or terminal outcome.
    pub target: HookTarget,
    /// Digest of the reference-free observer input.
    pub input_digest: JsonDigest,
    /// Classified callback result.
    pub status: HookObservationStatus,
    /// Report time; it does not advance Run time or revision.
    pub timestamp_ms: i64,
}

/// Executes selected callbacks, with authority and persistence owned by the core.
pub struct HookRuntime {
    store: Arc<dyn StateStore>,
    policy: Arc<PolicyGate>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
    registry: Arc<HookRegistry>,
}
fn hook_error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}
```

## `crates/wickle/src/hooks/records.rs`

```rust
use super::*;

impl HookApplicationRecord {
    /// Restore one exact protected application; this does not grant permission
    /// to invoke a Hook or substitute for chain and original-call validation.
    pub fn restore(
        record: &ProtectedRecord,
        plan: &HookPlan,
        application: &HookApplication,
        scope: &Scope,
        run_id: &Id,
    ) -> Result<Self, ContractError> {
        let invalid = || hook_error(ErrorCode::InvalidSnapshot, "hooks.application");
        let value: Self = serde_json::from_value(record.value().clone()).map_err(|_| invalid())?;
        if record.reference() != &application.result_ref
            || plan.scope() != scope
            || &value.scope != scope
            || &value.run_id != run_id
            || value.hook != application.hook
            || value.definition_digest != application.definition_digest
            || value.target != application.target
            || value.input.digest() != application.input_digest
            || crate::serialization::data_digest(&value) != record.reference().digest
        {
            return Err(invalid());
        }
        let definition = plan
            .definitions()
            .iter()
            .find(|definition| {
                definition.hook == value.hook
                    && definition.digest() == value.definition_digest
                    && definition.position == value.target.position()
            })
            .ok_or_else(invalid)?;
        value.input.validate(&value.target)?;
        match (&value.output, &value.failure) {
            (Some(output), None) => {
                validate_output(definition, &value.input, output)?;
                match output {
                    HookOutput::Context { additions } => {
                        if additions.len() != value.context_items.len() {
                            return Err(invalid());
                        }
                        for (addition, item) in additions.iter().zip(&value.context_items) {
                            let expected = stamped(
                                item.item_id.clone(),
                                scope,
                                run_id,
                                definition,
                                &value.target,
                                addition,
                            )?;
                            if item != &expected {
                                return Err(invalid());
                            }
                        }
                    }
                    _ if !value.context_items.is_empty() => return Err(invalid()),
                    _ => {}
                }
            }
            (None, Some(_))
                if definition.position == HookPosition::BeforeRun
                    && !definition.required
                    && value.context_items.is_empty() => {}
            _ => return Err(invalid()),
        }
        Ok(value)
    }
}

pub(super) fn validate_output(
    definition: &HookDefinition,
    input: &HookInput,
    output: &HookOutput,
) -> Result<(), ContractError> {
    let invalid = || hook_error(ErrorCode::InvalidContract, "hooks.output");
    if serde_json::to_vec(output).map_err(|_| invalid())?.len() > definition.max_output_bytes {
        return Err(invalid());
    }
    let valid = match (definition.position, input, output) {
        (
            HookPosition::BeforeRun,
            HookInput::BeforeRun { .. },
            HookOutput::Context { additions },
        )
        | (
            HookPosition::BeforeModel,
            HookInput::BeforeModel { .. },
            HookOutput::Context { additions },
        ) => additions
            .iter()
            .all(|addition| safe_content(&addition.content)),
        (
            HookPosition::BeforeTool,
            HookInput::BeforeTool { tool, .. },
            HookOutput::Tool { model_inputs, .. },
        ) => crate::tool_schema::compile_validator(&tool.model_input_schema)?
            .is_valid(&serde_json::to_value(model_inputs).map_err(|_| invalid())?),
        (HookPosition::AfterTool, HookInput::AfterTool { .. }, HookOutput::Observed {})
        | (HookPosition::AfterRun, HookInput::AfterRun { .. }, HookOutput::Observed {}) => true,
        _ => false,
    };
    if !valid {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn stamped(
    id: Id,
    scope: &Scope,
    run_id: &Id,
    definition: &HookDefinition,
    target: &HookTarget,
    addition: &HookContextAddition,
) -> Result<ContextItem, ContractError> {
    let lifetime = match target {
        HookTarget::BeforeRun => ContextLifetime::Run {
            run_id: run_id.clone(),
        },
        HookTarget::BeforeModel { model_step_id } => ContextLifetime::Step {
            run_id: run_id.clone(),
            model_step_id: model_step_id.clone(),
        },
        _ => {
            return Err(hook_error(
                ErrorCode::InvalidContract,
                "hooks.context_target",
            ));
        }
    };
    Ok(ContextItem::new(
        id,
        ContextOrigin::Hook,
        definition.hook.clone(),
        scope.clone(),
        addition.content.clone(),
        lifetime,
        addition.priority,
    ))
}

pub(super) fn apply(
    input: &mut HookInput,
    record: &HookApplicationRecord,
) -> Result<Option<Id>, ContractError> {
    if &record.input != input {
        return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.chain_input"));
    }
    match (&record.output, input) {
        (
            Some(HookOutput::Context { .. }),
            HookInput::BeforeRun { context_items, .. }
            | HookInput::BeforeModel { context_items, .. },
        ) => context_items.extend(record.context_items.clone()),
        (
            Some(HookOutput::Tool { model_inputs, deny }),
            HookInput::BeforeTool {
                model_inputs: current,
                ..
            },
        ) => {
            *current = model_inputs.clone();
            return Ok(deny.clone());
        }
        (None, _) if record.failure.is_some() => {}
        _ => return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.chain_output")),
    }
    Ok(None)
}

pub(super) fn transformed(
    input: HookInput,
    deny: Option<Id>,
    applications: Vec<HookApplication>,
) -> HookTransform {
    match input {
        HookInput::BeforeRun { context_items, .. }
        | HookInput::BeforeModel { context_items, .. } => HookTransform {
            context_items,
            model_inputs: None,
            deny,
            applications,
        },
        HookInput::BeforeTool { model_inputs, .. } => HookTransform {
            context_items: vec![],
            model_inputs: Some(model_inputs),
            deny,
            applications,
        },
        _ => unreachable!("validated transformation input"),
    }
}

pub(crate) fn validate_application_chain(
    plan: &HookPlan,
    snapshot: &RunSnapshot,
    records: &[ProtectedRecord],
) -> Result<(), ContractError> {
    plan.validate(snapshot.profile.profile())?;
    if plan.scope() != &snapshot.scope {
        return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.scope"));
    }
    let mut run_context = Vec::new();
    for application in snapshot
        .hook_applications
        .iter()
        .filter(|application| application.target == HookTarget::BeforeRun)
    {
        let record = records
            .iter()
            .find(|record| record.reference() == &application.result_ref)
            .ok_or_else(|| hook_error(ErrorCode::InvalidSnapshot, "hooks.run_context"))?;
        let value = HookApplicationRecord::restore(
            record,
            plan,
            application,
            &snapshot.scope,
            &snapshot.run_id,
        )?;
        run_context.extend(value.context_items);
    }
    let mut targets: Vec<HookTarget> = vec![];
    let mut ids = std::collections::BTreeSet::new();
    for application in &snapshot.hook_applications {
        if !ids.insert(application.result_ref.record_id.clone()) {
            return Err(hook_error(
                ErrorCode::InvalidSnapshot,
                "hooks.duplicate_application",
            ));
        }
        if !targets.contains(&application.target) {
            targets.push(application.target.clone());
        }
    }
    for target in targets {
        let definitions: Vec<_> = plan
            .definitions()
            .iter()
            .filter(|definition| definition.position == target.position())
            .collect();
        let applications: Vec<_> = snapshot
            .hook_applications
            .iter()
            .filter(|application| application.target == target)
            .collect();
        if applications.len() > definitions.len() {
            return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.chain_length"));
        }
        let mut current = None;
        let mut denied = false;
        for (application, definition) in applications.into_iter().zip(definitions) {
            if denied
                || application.hook != definition.hook
                || application.definition_digest != definition.digest()
            {
                return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.chain_order"));
            }
            let record = records
                .iter()
                .find(|record| record.reference() == &application.result_ref)
                .ok_or_else(|| hook_error(ErrorCode::InvalidSnapshot, "hooks.record_missing"))?;
            let record = HookApplicationRecord::restore(
                record,
                plan,
                application,
                &snapshot.scope,
                &snapshot.run_id,
            )?;
            if current.is_none() {
                match (&target, &record.input) {
                    (
                        HookTarget::BeforeRun,
                        HookInput::BeforeRun {
                            user_input,
                            context_items,
                        },
                    ) if user_input == &snapshot.request.input && context_items.is_empty() => {}
                    (
                        HookTarget::BeforeModel { model_step_id },
                        HookInput::BeforeModel {
                            user_input,
                            context_items,
                        },
                    ) if user_input == &snapshot.request.input
                        && context_items == &run_context
                        && (snapshot.model_step_id.as_ref() == Some(model_step_id)
                            || snapshot
                                .model_ledger
                                .iter()
                                .any(|invocation| &invocation.model_step_id == model_step_id)) => {}
                    (
                        HookTarget::BeforeTool { call_id },
                        HookInput::BeforeTool {
                            tool,
                            descriptor_digest,
                            original_model_inputs,
                            model_inputs,
                            ..
                        },
                    ) => {
                        let call = snapshot
                            .tool_ledger
                            .iter()
                            .find(|entry| &entry.call.call_id == call_id)
                            .ok_or_else(|| hook_error(ErrorCode::InvalidSnapshot, "hooks.call"))?;
                        if call.call.tool_name != tool.name
                            || call.call.descriptor_digest.as_ref() != Some(descriptor_digest)
                            || &call.call.model_inputs != original_model_inputs
                            || model_inputs != original_model_inputs
                        {
                            return Err(hook_error(
                                ErrorCode::InvalidSnapshot,
                                "hooks.original_inputs",
                            ));
                        }
                    }
                    _ => return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.target_input")),
                }
                current = Some(record.input.clone());
            }
            denied = apply(current.as_mut().expect("initialized"), &record)?.is_some();
        }
    }
    Ok(())
}
```

## `crates/wickle/src/hooks/runtime.rs`

```rust
use super::*;
use futures_util::FutureExt;
use std::{future::Future, panic::AssertUnwindSafe, time::Duration};

impl HookRuntime {
    /// Inject existing components. Transform persistence always uses the RunBudget's
    /// authoritative store; the injected store is used after the Run has ended.
    pub fn new(
        store: Arc<dyn StateStore>,
        policy: Arc<PolicyGate>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdSource>,
        registry: Arc<HookRegistry>,
    ) -> Self {
        Self {
            store,
            policy,
            clock,
            ids,
            registry,
        }
    }
    /// Scope under which callbacks are registered.
    pub fn scope(&self) -> &Scope {
        self.registry.scope()
    }
    /// Pure construction of the exact selected plan.
    pub fn plan(&self, profile: &AgentProfile) -> Result<HookPlan, ContractError> {
        self.registry.plan(profile)
    }

    async fn pinned_plan(
        &self,
        snapshot: &RunSnapshot,
        store: &dyn StateStore,
    ) -> Result<HookPlan, ContractError> {
        if &snapshot.scope != self.scope() {
            return Err(hook_error(ErrorCode::AccessDenied, "hooks.scope"));
        }
        let expected = self.plan(snapshot.profile.profile())?;
        let reference = snapshot
            .hook_plan_ref
            .as_ref()
            .ok_or_else(|| hook_error(ErrorCode::InvalidSnapshot, "hooks.plan_ref"))?;
        let record = store.read_record(&snapshot.scope, reference).await?;
        if record.reference() != reference {
            return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.plan_record"));
        }
        let plan = HookPlan::restore(
            &serde_json::to_string(record.value())
                .map_err(|_| hook_error(ErrorCode::InvalidJson, "hooks.plan"))?,
            &snapshot.scope,
            &reference.digest,
        )?;
        if plan != expected {
            return Err(hook_error(ErrorCode::ProfileMismatch, "hooks.plan"));
        }
        Ok(plan)
    }

    /// Read and fold only an already persisted prefix; this never invokes a Hook.
    pub async fn saved_transform(
        &self,
        snapshot: &RunSnapshot,
        target: &HookTarget,
    ) -> Result<Option<HookTransform>, ContractError> {
        let plan = self.pinned_plan(snapshot, self.store.as_ref()).await?;
        let applications: Vec<_> = snapshot
            .hook_applications
            .iter()
            .filter(|application| &application.target == target)
            .cloned()
            .collect();
        let mut current = None;
        let mut deny = None;
        for application in &applications {
            let record = self
                .store
                .read_record(&snapshot.scope, &application.result_ref)
                .await?;
            let value = HookApplicationRecord::restore(
                &record,
                &plan,
                application,
                &snapshot.scope,
                &snapshot.run_id,
            )?;
            if current.is_none() {
                current = Some(value.input.clone());
            }
            if deny.is_some() {
                return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.denied_chain"));
            }
            deny = records::apply(current.as_mut().expect("first input"), &value)?;
        }
        Ok(current.map(|input| records::transformed(input, deny, applications)))
    }

    /// Reuse each saved application before executing the remaining selected chain.
    /// Invalid output always fails closed; only optional before_run callback
    /// failure is persisted as a warning and permits the next callback.
    pub async fn transform(
        &self,
        target: HookTarget,
        mut input: HookInput,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<HookTransform, ContractError> {
        self.check_scope(context, budget.scope())?;
        input.validate(&target)?;
        if matches!(
            target,
            HookTarget::AfterTool { .. } | HookTarget::AfterRun { .. }
        ) {
            return Err(hook_error(
                ErrorCode::InvalidContract,
                "hooks.transform_target",
            ));
        }
        budget.check_boundary().await?;
        let saved = bounded(
            context,
            Some(budget),
            budget.call_deadline()?,
            budget.store().load(budget.scope(), budget.run_id()),
        )
        .await?;
        let plan = bounded(
            context,
            Some(budget),
            budget.call_deadline()?,
            self.pinned_plan(&saved.snapshot, budget.store().as_ref()),
        )
        .await?;
        bounded(
            context,
            Some(budget),
            budget.call_deadline()?,
            self.validate_live_input(&saved, &target, &input, budget.store().as_ref()),
        )
        .await?;
        let definitions: Vec<_> = plan
            .definitions()
            .iter()
            .filter(|definition| definition.position == target.position())
            .cloned()
            .collect();
        let mut applications = Vec::new();
        let mut deny = None;
        for definition in definitions {
            budget.check_boundary().await?;
            if context.cancellation.is_cancelled() {
                return Err(hook_error(ErrorCode::Cancelled, "hooks.transform"));
            }
            let current = bounded(
                context,
                Some(budget),
                budget.call_deadline()?,
                budget.store().load(budget.scope(), budget.run_id()),
            )
            .await?;
            if let Some(application) =
                current
                    .snapshot
                    .hook_applications
                    .iter()
                    .find(|application| {
                        application.target == target && application.hook == definition.hook
                    })
            {
                let record = bounded(
                    context,
                    Some(budget),
                    budget.call_deadline()?,
                    budget
                        .store()
                        .read_record(budget.scope(), &application.result_ref),
                )
                .await?;
                let value = HookApplicationRecord::restore(
                    &record,
                    &plan,
                    application,
                    budget.scope(),
                    budget.run_id(),
                )?;
                deny = records::apply(&mut input, &value)?;
                applications.push(application.clone());
                if deny.is_some() {
                    break;
                }
                continue;
            }
            let callback = self
                .invoke(
                    &definition,
                    &target,
                    &input,
                    budget.run_id(),
                    context,
                    Some(budget),
                    None,
                )
                .await?;
            let (output, failure) = match callback {
                Ok(output) => {
                    records::validate_output(&definition, &input, &output)?;
                    (Some(output), None)
                }
                Err(code) if target == HookTarget::BeforeRun && !definition.required => {
                    (None, Some(code))
                }
                Err(_) => return Err(hook_error(ErrorCode::InvalidContract, "hooks.callback")),
            };
            let context_items = if let Some(HookOutput::Context { additions }) = &output {
                additions
                    .iter()
                    .map(|addition| {
                        records::stamped(
                            self.ids.next_id()?,
                            budget.scope(),
                            budget.run_id(),
                            &definition,
                            &target,
                            addition,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                vec![]
            };
            let value = HookApplicationRecord {
                scope: budget.scope().clone(),
                run_id: budget.run_id().clone(),
                hook: definition.hook.clone(),
                definition_digest: definition.digest(),
                target: target.clone(),
                input: input.clone(),
                output,
                context_items,
                failure,
            };
            let record = ProtectedRecord::new(
                self.ids.next_id()?,
                1,
                serde_json::to_value(&value)
                    .map_err(|_| hook_error(ErrorCode::InvalidJson, "hooks.application"))?,
            );
            let application = HookApplication {
                hook: definition.hook.clone(),
                definition_digest: definition.digest(),
                target: target.clone(),
                input_digest: input.digest(),
                result_ref: record.reference().clone(),
            };
            budget.check_boundary().await?;
            let mut snapshot = bounded(
                context,
                Some(budget),
                budget.call_deadline()?,
                budget.store().load(budget.scope(), budget.run_id()),
            )
            .await?
            .snapshot;
            if snapshot
                .hook_applications
                .iter()
                .any(|prior| prior.target == target && prior.hook == definition.hook)
            {
                return Err(hook_error(ErrorCode::RevisionConflict, "hooks.application"));
            }
            let expected_revision = snapshot.revision;
            let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
            snapshot.revision = snapshot
                .revision
                .checked_add(1)
                .ok_or_else(|| hook_error(ErrorCode::RevisionConflict, "hooks.revision"))?;
            snapshot.usage.elapsed_ms = elapsed;
            snapshot.timing.last_observed_at_ms = now;
            snapshot.hook_applications.push(application.clone());
            bounded(
                context,
                Some(budget),
                budget.call_deadline()?,
                budget.store().commit(
                    budget.scope(),
                    budget.run_id(),
                    CommitInput {
                        expected_revision,
                        lease: budget.lease().clone(),
                        now_ms: now,
                        snapshot,
                        messages: vec![],
                        events: vec![],
                        records: vec![record],
                    },
                ),
            )
            .await?;
            deny = records::apply(&mut input, &value)?;
            applications.push(application);
            if deny.is_some() {
                break;
            }
        }
        Ok(records::transformed(input, deny, applications))
    }

    /// Observe committed data with finite cleanup time independent of the expired
    /// Run budget. Stored reports suppress repeat delivery; no crash replay or
    /// exactly-once external effect guarantee is provided.
    pub async fn observe(
        &self,
        run_id: &Id,
        target: HookTarget,
        input: HookInput,
        context: &ExecutionContext,
    ) -> Result<(), ContractError> {
        self.check_scope(context, self.scope())?;
        input.validate(&target)?;
        if !matches!(
            target,
            HookTarget::AfterTool { .. } | HookTarget::AfterRun { .. }
        ) {
            return Err(hook_error(
                ErrorCode::InvalidContract,
                "hooks.observer_target",
            ));
        }
        // Terminal cleanup is independent of the execution token that may have
        // been cancelled to produce this outcome. Current Host policy still runs.
        let cleanup_context;
        let context = if matches!(target, HookTarget::AfterRun { .. }) {
            let mut data = context.data.clone();
            data.system_inputs = None;
            cleanup_context = ExecutionContext::new(data, CancellationToken::new());
            &cleanup_context
        } else {
            context
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let saved = bounded(
            context,
            None,
            deadline,
            self.store.load(self.scope(), run_id),
        )
        .await?;
        let plan = bounded(
            context,
            None,
            deadline,
            self.pinned_plan(&saved.snapshot, self.store.as_ref()),
        )
        .await?;
        bounded(
            context,
            None,
            deadline,
            self.validate_live_input(&saved, &target, &input, self.store.as_ref()),
        )
        .await?;
        let prior = bounded(
            context,
            None,
            deadline,
            self.store.read_hook_observations(self.scope(), run_id),
        )
        .await?;
        let observer_deadline = if matches!(target, HookTarget::AfterTool { .. }) {
            let remaining = saved
                .snapshot
                .timing
                .deadline_at_ms
                .saturating_sub(self.clock.now()?.utc_ms)
                .max(0) as u64;
            deadline.min(tokio::time::Instant::now() + Duration::from_millis(remaining))
        } else {
            deadline
        };
        for definition in plan
            .definitions()
            .iter()
            .filter(|definition| definition.position == target.position())
        {
            if prior.iter().any(|report| {
                report.hook == definition.hook
                    && report.definition_digest == definition.digest()
                    && report.target == target
            }) {
                continue;
            }
            let observed = self
                .invoke(
                    definition,
                    &target,
                    &input,
                    run_id,
                    context,
                    None,
                    Some(observer_deadline),
                )
                .await;
            let status = match observed {
                Ok(Ok(output)) => match records::validate_output(definition, &input, &output) {
                    Ok(()) => HookObservationStatus::Completed,
                    Err(error) => HookObservationStatus::Failed {
                        code: code_id(error.code)?,
                    },
                },
                Ok(Err(code)) => HookObservationStatus::Failed { code },
                Err(error) => HookObservationStatus::Failed {
                    code: code_id(error.code)?,
                },
            };
            let report = HookObservation {
                scope: self.scope().clone(),
                run_id: run_id.clone(),
                hook: definition.hook.clone(),
                definition_digest: definition.digest(),
                target: target.clone(),
                input_digest: input.digest(),
                status,
                timestamp_ms: self.clock.now()?.utc_ms,
            };
            // Report failures remain separate from the already committed result.
            // A fresh cleanup token allows recording that the caller cancelled.
            let mut data = context.data.clone();
            data.system_inputs = None;
            let cleanup = ExecutionContext::new(data, CancellationToken::new());
            bounded(
                &cleanup,
                None,
                tokio::time::Instant::now() + Duration::from_secs(30),
                self.store
                    .record_hook_observation(self.scope(), run_id, report),
            )
            .await?;
        }
        Ok(())
    }

    fn check_scope(&self, context: &ExecutionContext, scope: &Scope) -> Result<(), ContractError> {
        if self.scope() != scope || &context.data.scope != scope {
            return Err(hook_error(ErrorCode::AccessDenied, "hooks.scope"));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn invoke(
        &self,
        definition: &HookDefinition,
        target: &HookTarget,
        input: &HookInput,
        run_id: &Id,
        context: &ExecutionContext,
        budget: Option<&RunBudget>,
        external_deadline: Option<tokio::time::Instant>,
    ) -> Result<Result<HookOutput, Id>, ContractError> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(definition.timeout_ms);
        let deadline = if let Some(budget) = budget {
            deadline.min(budget.call_deadline()?)
        } else {
            deadline
        };
        let deadline = external_deadline.map_or(deadline, |external| deadline.min(external));
        let cancellation = context.cancellation.child_token();
        let _cancel = cancellation.clone().drop_guard();
        let mut data = context.data.clone();
        data.system_inputs = None;
        let policy_context = ExecutionContext::new(data, cancellation.clone());
        let request = PolicyRequest {
            owner_scope: self.scope().clone(),
            resource_id: run_id.clone(),
            action: PolicyAction::InvokeHook {
                hook: definition.hook.clone(),
                definition_digest: definition.digest(),
                target: target.clone(),
            },
        };
        let decision = bounded(
            &policy_context,
            budget,
            deadline,
            self.policy
                .check(&request, &policy_context, Some(deadline), None),
        )
        .await?;
        if decision != (PolicyDecision::Allow {}) {
            return Err(hook_error(ErrorCode::AccessDenied, "hooks.policy"));
        }
        if let Some(budget) = budget {
            budget.check_boundary().await?;
        }
        let entry = self
            .registry
            .entries
            .iter()
            .find(|entry| entry.definition == *definition)
            .ok_or_else(|| hook_error(ErrorCode::ComponentUnavailable, "hooks.handler"))?;
        let hook_context = HookContext {
            scope: self.scope().clone(),
            run_id: run_id.clone(),
            hook: definition.hook.clone(),
            target: target.clone(),
            principal_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
            cancellation: cancellation.clone(),
            deadline,
        };
        let operation = AssertUnwindSafe(async { entry.handler.call(input, &hook_context).await })
            .catch_unwind();
        let result = tokio::select! {biased;
            _=context.cancellation.cancelled()=>Err(hook_error(ErrorCode::Cancelled,"hooks.callback")),
            stopped=run_stopped(budget)=>Err(stopped),
            _=tokio::time::sleep_until(deadline)=>Ok(Err(code_id(ErrorCode::DeadlineExceeded)?)),
            result=operation=>Ok(match result{Ok(Ok(output))=>Ok(output),Ok(Err(error))=>Err(code_id(error.code)?),Err(_)=>Err(code_id(ErrorCode::InvalidContract)?)})
        };
        cancellation.cancel();
        result
    }

    async fn validate_live_input(
        &self,
        saved: &StoredRun,
        target: &HookTarget,
        input: &HookInput,
        store: &dyn StateStore,
    ) -> Result<(), ContractError> {
        let invalid = || hook_error(ErrorCode::InvalidSnapshot, "hooks.target_input");
        match (target, input) {
            (
                HookTarget::BeforeRun,
                HookInput::BeforeRun {
                    user_input,
                    context_items,
                },
            ) if user_input == &saved.snapshot.request.input && context_items.is_empty() => {}
            (
                HookTarget::BeforeModel { model_step_id },
                HookInput::BeforeModel {
                    user_input,
                    context_items,
                },
            ) if user_input == &saved.snapshot.request.input
                && saved.snapshot.model_step_id.as_ref() == Some(model_step_id) =>
            {
                let mut expected = Vec::new();
                for application in saved
                    .snapshot
                    .hook_applications
                    .iter()
                    .filter(|application| application.target == HookTarget::BeforeRun)
                {
                    let record = store
                        .read_record(&saved.snapshot.scope, &application.result_ref)
                        .await?;
                    let value: HookApplicationRecord =
                        serde_json::from_value(record.value().clone()).map_err(|_| invalid())?;
                    expected.extend(value.context_items);
                }
                if context_items != &expected {
                    return Err(invalid());
                }
            }
            (
                HookTarget::BeforeTool { call_id },
                HookInput::BeforeTool {
                    tool,
                    descriptor_digest,
                    compiled_digest,
                    original_model_inputs,
                    model_inputs,
                    ..
                },
            ) => {
                let call = &saved
                    .snapshot
                    .tool_ledger
                    .iter()
                    .find(|entry| &entry.call.call_id == call_id)
                    .ok_or_else(invalid)?
                    .call;
                if call.bound_input_ref.is_some()
                    || original_model_inputs != &call.model_inputs
                    || model_inputs != original_model_inputs
                    || call.descriptor_digest.as_ref() != Some(descriptor_digest)
                {
                    return Err(invalid());
                }
                let record = store
                    .read_record(&saved.snapshot.scope, &saved.session.prompt_snapshot)
                    .await?;
                let prompt = PromptSnapshot::restore(
                    &serde_json::to_string(record.value()).map_err(|_| invalid())?,
                    &saved.session.prompt_snapshot.digest,
                    &saved.snapshot.profile,
                    &saved.snapshot.scope,
                )?;
                if !prompt.tools().iter().any(|entry| {
                    entry.model_tool == *tool
                        && &entry.descriptor_digest == descriptor_digest
                        && &entry.compiled_digest == compiled_digest
                }) {
                    return Err(invalid());
                }
            }
            (
                HookTarget::AfterTool {
                    call_id,
                    result_ref,
                },
                HookInput::AfterTool { .. },
            ) => {
                let record = store.read_record(&saved.snapshot.scope, result_ref).await?;
                let result: ToolResult =
                    serde_json::from_value(record.value().clone()).map_err(|_| invalid())?;
                if record.reference() != result_ref
                    || &result.call_id != call_id
                    || &HookInput::tool_observed(call_id, &result) != input
                {
                    return Err(invalid());
                }
            }
            (
                HookTarget::AfterRun {
                    outcome_ref,
                    revision,
                },
                HookInput::AfterRun { .. },
            ) => {
                let record = store
                    .read_record(&saved.snapshot.scope, outcome_ref)
                    .await?;
                let outcome: RunOutcome =
                    serde_json::from_value(record.value().clone()).map_err(|_| invalid())?;
                if record.reference() != outcome_ref
                    || !saved.snapshot.status.is_terminal()
                    || saved.snapshot.revision != *revision
                    || saved.snapshot.outcome.as_ref() != Some(&outcome)
                    || &HookInput::run_observed(&outcome) != input
                {
                    return Err(invalid());
                }
            }
            _ => return Err(invalid()),
        }
        if matches!(
            target,
            HookTarget::AfterTool { .. } | HookTarget::AfterRun { .. }
        ) {
            let mut after = 0;
            let mut found = false;
            loop {
                let page = store
                    .read_events(&saved.snapshot.scope, &saved.snapshot.run_id, after, 256)
                    .await?;
                found |= page
                    .events
                    .iter()
                    .any(|event| match (target, &event.payload) {
                        (
                            HookTarget::AfterTool { result_ref, .. },
                            RunEventPayload::ToolSettled {
                                result_ref: reference,
                            }
                            | RunEventPayload::ToolUnresolved {
                                result_ref: reference,
                                ..
                            },
                        ) => result_ref == reference,
                        (
                            HookTarget::AfterRun { outcome_ref, .. },
                            RunEventPayload::RunFinished {
                                outcome_ref: reference,
                            },
                        ) => outcome_ref == reference,
                        _ => false,
                    });
                if found || !page.has_more {
                    break;
                }
                if page.next_after_seq <= after {
                    return Err(invalid());
                }
                after = page.next_after_seq;
            }
            if !found {
                return Err(invalid());
            }
        }
        Ok(())
    }
}

async fn run_stopped(budget: Option<&RunBudget>) -> ContractError {
    if let Some(budget) = budget {
        budget
            .wait_for_cancellation_or_deadline()
            .await
            .err()
            .unwrap_or_else(|| hook_error(ErrorCode::DeadlineExceeded, "hooks.run"))
    } else {
        std::future::pending().await
    }
}
async fn bounded<T>(
    context: &ExecutionContext,
    budget: Option<&RunBudget>,
    deadline: tokio::time::Instant,
    operation: impl Future<Output = Result<T, ContractError>>,
) -> Result<T, ContractError> {
    tokio::select! {biased;
        _=context.cancellation.cancelled()=>Err(hook_error(ErrorCode::Cancelled,"hooks.operation")),
        stopped=run_stopped(budget)=>Err(stopped),
        _=tokio::time::sleep_until(deadline)=>Err(hook_error(ErrorCode::DeadlineExceeded,"hooks.operation")),
        result=operation=>result,
    }
}
fn code_id(code: ErrorCode) -> Result<Id, ContractError> {
    Id::new(
        serde_json::to_value(code)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .unwrap_or_else(|| "invalid_contract".into()),
    )
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
    #[serde(
        default,
        deserialize_with = "crate::serialization::optional",
        skip_serializing_if = "Option::is_none"
    )]
    effective_model_inputs: Option<JsonObject>,
    #[serde(
        default,
        deserialize_with = "crate::serialization::optional",
        skip_serializing_if = "Option::is_none"
    )]
    transformation_ref: Option<RecordRef>,
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
    /// Validated hook-transformed arguments, or the unchanged original arguments.
    pub fn effective_model_inputs(&self) -> &JsonObject {
        self.data
            .effective_model_inputs
            .as_ref()
            .unwrap_or(&self.data.original_model_inputs)
    }
    /// Exact saved final transformation record, when hooks transformed this call.
    pub fn transformation_ref(&self) -> Option<&RecordRef> {
        self.data.transformation_ref.as_ref()
    }
    /// Effective model arguments plus declared optional top-level defaults.
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
        if bound.data.effective_model_inputs.is_some() != bound.data.transformation_ref.is_some() {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "bound_input.transformation",
            ));
        }
        if bound
            .effective_model_inputs()
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
        compiled.validate_model_inputs(self.effective_model_inputs())?;
        if normalize_model_inputs(compiled, self.effective_model_inputs())?
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
        let transformed =
            saved_tool_transform(&saved.snapshot, compiled, &call, context, budget).await?;
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
            if input.transformation_ref() != transformed.as_ref().map(|(_, reference)| reference)
                || input.effective_model_inputs()
                    != transformed
                        .as_ref()
                        .map_or(&call.model_inputs, |(inputs, _)| inputs)
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.transformation",
                ));
            }
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
        let effective = transformed
            .as_ref()
            .map_or(&call.model_inputs, |(inputs, _)| inputs);
        let normalized = normalize_model_inputs(compiled, effective)?;
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
            effective_model_inputs: transformed.as_ref().map(|(inputs, _)| inputs.clone()),
            transformation_ref: transformed.map(|(_, reference)| reference),
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

async fn saved_tool_transform(
    snapshot: &RunSnapshot,
    compiled: &CompiledTool,
    call: &ToolCall,
    context: &ExecutionContext,
    budget: &RunBudget,
) -> Result<Option<(JsonObject, RecordRef)>, ContractError> {
    let Some(reference) = &snapshot.hook_plan_ref else {
        return Ok(None);
    };
    let record = bounded(
        context,
        budget,
        budget.store().read_record(budget.scope(), reference),
    )
    .await?;
    let plan = crate::HookPlan::restore(
        &serde_json::to_string(record.value())
            .map_err(|_| error(ErrorCode::InvalidJson, "hooks.plan"))?,
        budget.scope(),
        &reference.digest,
    )?;
    let definitions: Vec<_> = plan
        .definitions()
        .iter()
        .filter(|definition| definition.position == crate::HookPosition::BeforeTool)
        .collect();
    if definitions.is_empty() {
        return Ok(None);
    }
    let target = crate::HookTarget::BeforeTool {
        call_id: call.call_id.clone(),
    };
    let applications: Vec<_> = snapshot
        .hook_applications
        .iter()
        .filter(|application| application.target == target)
        .collect();
    if applications.len() != definitions.len() {
        return Err(error(
            ErrorCode::InvalidTransition,
            "hooks.before_tool_missing",
        ));
    }
    let mut inputs = call.model_inputs.clone();
    for (definition, application) in definitions.iter().zip(&applications) {
        if definition.hook != application.hook {
            return Err(error(ErrorCode::InvalidSnapshot, "hooks.order"));
        }
        let record = bounded(
            context,
            budget,
            budget
                .store()
                .read_record(budget.scope(), &application.result_ref),
        )
        .await?;
        let record = crate::HookApplicationRecord::restore(
            &record,
            &plan,
            application,
            budget.scope(),
            budget.run_id(),
        )?;
        let crate::HookInput::BeforeTool {
            tool,
            descriptor_digest,
            compiled_digest,
            original_model_inputs,
            model_inputs,
        } = &record.input
        else {
            return Err(error(ErrorCode::InvalidSnapshot, "hooks.tool_input"));
        };
        if tool != &compiled.to_model_tool()
            || descriptor_digest != compiled.descriptor_digest()
            || compiled_digest != compiled.digest()
            || original_model_inputs != &call.model_inputs
            || model_inputs != &inputs
        {
            return Err(error(
                ErrorCode::SystemInputsMismatch,
                "hooks.tool_identity",
            ));
        }
        let Some(crate::HookOutput::Tool { model_inputs, deny }) = record.output else {
            return Err(error(ErrorCode::InvalidSnapshot, "hooks.tool_output"));
        };
        if deny.is_some() {
            return Err(error(ErrorCode::AccessDenied, "hooks.tool_denied"));
        }
        compiled.validate_model_inputs(&model_inputs)?;
        inputs = model_inputs;
    }
    Ok(Some((
        inputs,
        applications
            .last()
            .expect("nonempty definitions")
            .result_ref
            .clone(),
    )))
}

/// Validate the exact saved transformation that a bound candidate claims to use.
pub(crate) fn validate_bound_transformation(
    value: &Value,
    snapshot: &RunSnapshot,
    call: &ToolCall,
    transformation: Option<&Value>,
) -> Result<(), ContractError> {
    let bound = BoundToolInput::from_value(value)?;
    let target = crate::HookTarget::BeforeTool {
        call_id: call.call_id.clone(),
    };
    let application = snapshot
        .hook_applications
        .iter()
        .rev()
        .find(|application| application.target == target);
    if bound.transformation_ref() != application.map(|application| &application.result_ref) {
        return Err(error(
            ErrorCode::SystemInputsMismatch,
            "bound_input.transform_reference",
        ));
    }
    match (bound.transformation_ref(), transformation) {
        (None, None) => Ok(()),
        (Some(reference), Some(value)) => {
            if crate::canonical_digest(value) != reference.digest {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.transform_digest",
                ));
            }
            let record: crate::HookApplicationRecord = serde_json::from_value(value.clone())
                .map_err(|_| error(ErrorCode::InvalidSnapshot, "bound_input.transform_record"))?;
            let crate::HookInput::BeforeTool {
                descriptor_digest,
                compiled_digest,
                original_model_inputs,
                ..
            } = record.input
            else {
                return Err(error(
                    ErrorCode::InvalidSnapshot,
                    "bound_input.transform_input",
                ));
            };
            if record.scope != snapshot.scope
                || record.run_id != snapshot.run_id
                || record.target != target
                || &descriptor_digest != bound.descriptor_digest()
                || &compiled_digest != bound.compiled_digest()
                || original_model_inputs != call.model_inputs
            {
                return Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.transform_identity",
                ));
            }
            match record.output {
                Some(crate::HookOutput::Tool {
                    model_inputs,
                    deny: None,
                }) if &model_inputs == bound.effective_model_inputs() => Ok(()),
                _ => Err(error(
                    ErrorCode::SystemInputsMismatch,
                    "bound_input.transform_output",
                )),
            }
        }
        _ => Err(error(
            ErrorCode::SystemInputsMismatch,
            "bound_input.transform_record",
        )),
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
mod hooks;
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
    Agent, AgentBindings, AgentSettings, CancelReceipt, HookObservationView, ModelTokenEstimator,
    RunHandle, create_agent,
};
pub use budget::{AttemptReservation, ReservationKind, RunBudget, RunTiming};
pub use clock::{Clock, ClockReading, IdSource, RandomIdSource, SystemClock};
pub use context_projection::{
    CONTEXT_ASSEMBLER_VERSION, ContextAssembler, ContextItem, ContextLifetime, ContextOrigin,
    ContextPriority, ContextProjection, InstructionAssetContent, PinnedPromptTool, ProjectionInput,
    ProjectionLimits, PromptSnapshot, PromptToolBinding, ScopedOpaque, SkillManifest,
};
pub use hooks::{
    HookApplication, HookApplicationRecord, HookContext, HookContextAddition, HookDefinition,
    HookHandler, HookInput, HookObservation, HookObservationStatus, HookOutput, HookPlan,
    HookRegistration, HookRegistry, HookRuntime, HookTarget, HookTransform,
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
    /// Invoke one selected lifecycle hook under its pinned definition and target.
    InvokeHook {
        /// Exact selected hook version.
        hook: VersionedRef,
        /// Immutable execution definition.
        definition_digest: JsonDigest,
        /// Exact lifecycle invocation scope within the Run.
        target: crate::HookTarget,
    },
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
    /// Exact selected lifecycle definitions pinned before any hook executes.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub hook_plan_ref: Option<RecordRef>,
    /// Applied lifecycle transformations, preserved in their invocation order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hook_applications: Vec<crate::HookApplication>,
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
mod hook_state;
pub use checkpoint::{STATE_STORE_CHECKPOINT_VERSION, StateStoreCheckpoint};
use hook_state::{validate_hook_observation, validate_hook_snapshot, validate_hook_transition};

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
    /// Append a report about an already committed result without changing its
    /// outcome, snapshot revision, session ownership, or durable event sequence.
    fn record_hook_observation<'a>(
        &'a self,
        _scope: &'a Scope,
        _run_id: &'a Id,
        _report: crate::HookObservation,
    ) -> PortFuture<'a, ()> {
        Box::pin(async {
            Err(error(
                ErrorCode::CapabilityUnsupported,
                "store.hook_observations",
            ))
        })
    }
    /// Read protected lifecycle observation reports after Host authorization.
    fn read_hook_observations<'a>(
        &'a self,
        _scope: &'a Scope,
        _run_id: &'a Id,
    ) -> PortFuture<'a, Vec<crate::HookObservation>> {
        Box::pin(async {
            Err(error(
                ErrorCode::CapabilityUnsupported,
                "store.hook_observations",
            ))
        })
    }
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
    hook_observations: BTreeMap<Id, Vec<crate::HookObservation>>,
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
                || !input.snapshot.hook_applications.is_empty()
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
    fn record_hook_observation<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        report: crate::HookObservation,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let mut scopes = self.lock()?;
            let state = scopes.get_mut(&scope_key(scope)).ok_or_else(not_found)?;
            validate_hook_observation(state, scope, run_id, &report)?;
            let reports = state.hook_observations.entry(run_id.clone()).or_default();
            if let Some(existing) = reports
                .iter()
                .find(|existing| existing.hook == report.hook && existing.target == report.target)
            {
                return if existing == &report {
                    Ok(())
                } else {
                    Err(error(ErrorCode::RecordConflict, "hooks.observation"))
                };
            }
            reports.push(report);
            Ok(())
        })
    }
    fn read_hook_observations<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
    ) -> PortFuture<'a, Vec<crate::HookObservation>> {
        Box::pin(async move {
            let scopes = self.lock()?;
            let state = namespace(&scopes, scope)?;
            if !state.runs.contains_key(run_id) {
                return Err(not_found());
            }
            Ok(state
                .hook_observations
                .get(run_id)
                .cloned()
                .unwrap_or_default())
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
    validate_hook_snapshot(state, additions, snapshot)?;
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
            let bound = record_value(state, additions, reference)?;
            let transform_ref = bound
                .get("data")
                .and_then(|data| data.get("transformation_ref"))
                .map(|value| {
                    serde_json::from_value::<RecordRef>(value.clone()).map_err(|_| {
                        error(ErrorCode::InvalidSnapshot, "bound_input.transformation_ref")
                    })
                })
                .transpose()?;
            let transformed = transform_ref
                .as_ref()
                .map(|reference| record_value(state, additions, reference))
                .transpose()?;
            crate::input_binding::validate_bound_transformation(
                bound,
                snapshot,
                &entry.call,
                transformed,
            )?;
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
    validate_hook_transition(previous, next)?;
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
    #[serde(skip_serializing_if = "Vec::is_empty")]
    hook_observations: Vec<&'a crate::HookObservation>,
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
            hook_observations: self.state.hook_observations.values().flatten().collect(),
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
    #[serde(default)]
    hook_observations: Vec<crate::HookObservation>,
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
    for report in data.hook_observations {
        validate_hook_observation(&state, &data.scope, &report.run_id, &report)?;
        let reports = state
            .hook_observations
            .entry(report.run_id.clone())
            .or_default();
        if reports
            .iter()
            .any(|existing| existing.hook == report.hook && existing.target == report.target)
        {
            return Err(invalid("checkpoint.hook_observation_duplicate"));
        }
        reports.push(report);
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

## `crates/wickle/src/state/hook_state.rs`

```rust
use super::*;
use crate::{HookInput, HookPlan, HookPosition, HookRef, HookTarget};

fn load_plan(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<Option<HookPlan>, ContractError> {
    let Some(reference) = &snapshot.hook_plan_ref else {
        if !snapshot.hook_applications.is_empty()
            || snapshot
                .profile
                .profile()
                .hooks
                .as_ref()
                .is_some_and(|hooks| !hooks.is_empty())
        {
            return Err(error(ErrorCode::InvalidSnapshot, "hooks.plan_missing"));
        }
        return Ok(None);
    };
    let value = record_value(state, additions, reference)?;
    let plan = HookPlan::restore(
        &serde_json::to_string(value)
            .map_err(|_| error(ErrorCode::InvalidSnapshot, "hooks.plan"))?,
        &snapshot.scope,
        &reference.digest,
    )?;
    let selected = snapshot
        .profile
        .profile()
        .hooks
        .as_deref()
        .unwrap_or_default();
    if plan.definitions().len() != selected.len()
        || selected.iter().any(|selection| match selection {
            HookRef::Catalog(hook) => !plan.definitions().iter().any(|definition| {
                definition.hook.id == hook.hook_id
                    && definition.hook.version == hook.version
                    && definition.position == hook.position
            }),
            HookRef::Export(_) => true,
        })
    {
        return Err(error(ErrorCode::InvalidSnapshot, "hooks.plan_selection"));
    }
    Ok(Some(plan))
}

pub(super) fn validate_hook_snapshot(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    let Some(plan) = load_plan(state, additions, snapshot)? else {
        return Ok(());
    };
    let records = snapshot
        .hook_applications
        .iter()
        .map(|application| {
            let value = record_value(state, additions, &application.result_ref)?;
            Ok(ProtectedRecord::new(
                application.result_ref.record_id.clone(),
                application.result_ref.revision,
                value.clone(),
            ))
        })
        .collect::<Result<Vec<_>, ContractError>>()?;
    crate::hooks::validate_application_chain(&plan, snapshot, &records)?;
    let expected = |position| {
        plan.definitions()
            .iter()
            .filter(|definition| definition.position == position)
            .count()
    };
    let applied = |target: &HookTarget| {
        snapshot
            .hook_applications
            .iter()
            .filter(|application| &application.target == target)
            .count()
    };
    if snapshot.usage.model_calls > 0
        && applied(&HookTarget::BeforeRun) != expected(HookPosition::BeforeRun)
    {
        return Err(error(
            ErrorCode::InvalidSnapshot,
            "hooks.before_run_incomplete",
        ));
    }
    for invocation in &snapshot.model_ledger {
        if applied(&HookTarget::BeforeModel {
            model_step_id: invocation.model_step_id.clone(),
        }) != expected(HookPosition::BeforeModel)
        {
            return Err(error(
                ErrorCode::InvalidSnapshot,
                "hooks.before_model_incomplete",
            ));
        }
    }
    for entry in &snapshot.tool_ledger {
        if entry.call.bound_input_ref.is_some() {
            let target = HookTarget::BeforeTool {
                call_id: entry.call.call_id.clone(),
            };
            if applied(&target) != expected(HookPosition::BeforeTool) {
                return Err(error(
                    ErrorCode::InvalidSnapshot,
                    "hooks.before_tool_incomplete",
                ));
            }
            for (_, record) in snapshot
                .hook_applications
                .iter()
                .zip(&records)
                .filter(|(application, _)| application.target == target)
            {
                let result: crate::HookApplicationRecord =
                    serde_json::from_value(record.value().clone())
                        .map_err(|_| error(ErrorCode::InvalidSnapshot, "hooks.application"))?;
                if matches!(
                    result.output,
                    Some(crate::HookOutput::Tool { deny: Some(_), .. })
                ) {
                    return Err(error(ErrorCode::InvalidSnapshot, "hooks.bound_denied"));
                }
            }
        }
    }
    if !records.is_empty() {
        let session = state
            .sessions
            .get(&snapshot.request.session_id)
            .ok_or_else(not_found)?;
        let value = record_value(state, additions, &session.snapshot.prompt_snapshot)?;
        let prompt = crate::PromptSnapshot::restore(
            &serde_json::to_string(value)
                .map_err(|_| error(ErrorCode::InvalidSnapshot, "hooks.prompt"))?,
            &session.snapshot.prompt_snapshot.digest,
            &snapshot.profile,
            &snapshot.scope,
        )?;
        for record in &records {
            let application: crate::HookApplicationRecord =
                serde_json::from_value(record.value().clone())
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "hooks.application"))?;
            if let HookInput::BeforeTool {
                tool,
                descriptor_digest,
                compiled_digest,
                ..
            } = &application.input
            {
                if !prompt.tools().iter().any(|pinned| {
                    &pinned.model_tool == tool
                        && &pinned.descriptor_digest == descriptor_digest
                        && &pinned.compiled_digest == compiled_digest
                }) {
                    return Err(error(ErrorCode::InvalidSnapshot, "hooks.tool_contract"));
                }
            }
        }
    }
    Ok(())
}

pub(super) fn validate_hook_transition(
    previous: &RunSnapshot,
    next: &RunSnapshot,
) -> Result<(), ContractError> {
    if previous.hook_plan_ref != next.hook_plan_ref
        || !next
            .hook_applications
            .starts_with(&previous.hook_applications)
    {
        return Err(error(ErrorCode::InvalidTransition, "hooks.immutable"));
    }
    for added in &next.hook_applications[previous.hook_applications.len()..] {
        match &added.target {
            HookTarget::BeforeRun
                if previous.usage.model_calls == 0 && previous.tool_ledger.is_empty() => {}
            HookTarget::BeforeModel { model_step_id }
                if next.model_step_id.as_ref() == Some(model_step_id)
                    && !previous
                        .model_ledger
                        .iter()
                        .any(|invocation| &invocation.model_step_id == model_step_id) => {}
            HookTarget::BeforeTool { call_id }
                if previous.tool_ledger.iter().any(|entry| {
                    &entry.call.call_id == call_id
                        && entry.call.bound_input_ref.is_none()
                        && matches!(entry.state, ToolCallState::Planned {})
                }) => {}
            _ => return Err(error(ErrorCode::InvalidTransition, "hooks.target")),
        }
    }
    Ok(())
}

pub(super) fn validate_hook_observation(
    state: &ScopeState,
    scope: &Scope,
    run_id: &Id,
    report: &crate::HookObservation,
) -> Result<(), ContractError> {
    check_scope(scope, &report.scope)?;
    if run_id != &report.run_id {
        return Err(error(ErrorCode::InvalidReference, "hooks.observation_run"));
    }
    let run = state.runs.get(run_id).ok_or_else(not_found)?;
    let empty = BTreeMap::new();
    let plan = load_plan(state, &empty, &run.snapshot)?
        .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "hooks.plan_missing"))?;
    if !plan.definitions().iter().any(|definition| {
        definition.hook == report.hook
            && definition.digest() == report.definition_digest
            && definition.position == report.target.position()
    }) {
        return Err(error(
            ErrorCode::InvalidReference,
            "hooks.observer_definition",
        ));
    }
    let (input, committed_at) = match &report.target {
        HookTarget::AfterTool {
            call_id,
            result_ref,
        } => {
            let result: ToolResult = event_record(state, &empty, result_ref)?;
            let event = run
                .events
                .iter()
                .find(|event| match &event.payload {
                    RunEventPayload::ToolSettled { result_ref: saved }
                    | RunEventPayload::ToolUnresolved {
                        result_ref: saved, ..
                    } => saved == result_ref,
                    _ => false,
                })
                .ok_or_else(|| error(ErrorCode::InvalidReference, "hooks.observed_result"))?;
            if call_id != &result.call_id
                || !run
                    .snapshot
                    .tool_ledger
                    .iter()
                    .any(|entry| &entry.call.call_id == call_id)
            {
                return Err(error(ErrorCode::InvalidReference, "hooks.observed_call"));
            }
            (
                HookInput::tool_observed(call_id, &result),
                event.timestamp_ms,
            )
        }
        HookTarget::AfterRun {
            outcome_ref,
            revision,
        } => {
            let outcome: crate::RunOutcome = event_record(state, &empty, outcome_ref)?;
            let event = run
                .events
                .iter()
                .find(|event| {
                    matches!(&event.payload,
                RunEventPayload::RunFinished { outcome_ref: saved } if saved == outcome_ref)
                })
                .ok_or_else(|| error(ErrorCode::InvalidReference, "hooks.observed_outcome"))?;
            if !run.snapshot.status.is_terminal()
                || &run.snapshot.revision != revision
                || run.snapshot.outcome.as_ref() != Some(&outcome)
            {
                return Err(error(ErrorCode::InvalidReference, "hooks.observed_outcome"));
            }
            (HookInput::run_observed(&outcome), event.timestamp_ms)
        }
        _ => return Err(error(ErrorCode::InvalidReference, "hooks.observer_target")),
    };
    if report.timestamp_ms < committed_at
        || report.input_digest
            != canonical_digest(
                &serde_json::to_value(&input)
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "hooks.observer_input"))?,
            )
    {
        return Err(error(ErrorCode::InvalidSnapshot, "hooks.observer_input"));
    }
    Ok(())
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
    hooks: Option<Arc<HookRuntime>>,
    observer_error: std::sync::Mutex<Option<ContractError>>,
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
            hooks: None,
            observer_error: std::sync::Mutex::new(None),
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
    /// Connect the pinned lifecycle runtime without running a callback.
    pub fn with_hooks(mut self, hooks: Arc<HookRuntime>) -> Self {
        self.hooks = Some(hooks);
        self
    }
    /// A local observer-report persistence error, separate from Tool execution.
    pub fn observer_error(&self) -> Option<ContractError> {
        self.observer_error
            .lock()
            .ok()
            .and_then(|error| error.clone())
    }
}
fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
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
                    context,
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
                    context,
                )
                .await?;
                continue;
            }
            if call.bound_input_ref.is_none() {
                if let Some(hooks) = &self.hooks {
                    let transformed = hooks
                        .transform(
                            HookTarget::BeforeTool {
                                call_id: call_id.clone(),
                            },
                            HookInput::BeforeTool {
                                tool: registered.compiled.to_model_tool(),
                                descriptor_digest: registered.compiled.descriptor_digest().clone(),
                                compiled_digest: registered.compiled.digest().clone(),
                                original_model_inputs: call.model_inputs.clone(),
                                model_inputs: call.model_inputs.clone(),
                            },
                            context,
                            budget,
                        )
                        .await?;
                    if let Some(reason) = transformed.deny {
                        self.reject(
                            &call,
                            call_message_id,
                            ToolResultStatus::Denied,
                            reason.as_str(),
                            budget,
                            context,
                        )
                        .await?;
                        continue;
                    }
                    if let Some(inputs) = transformed.model_inputs {
                        registered.compiled.validate_model_inputs(&inputs)?;
                    }
                }
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
                        context,
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
                        context,
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
                        context,
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
                        context,
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
                        context,
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
                        context,
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
                .settle(&call_id, state, result, records, budget, context)
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
                    context,
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
        context: &ExecutionContext,
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
            context,
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
        context: &ExecutionContext,
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
        let intended_state = state.clone();
        let observer_input = HookInput::tool_observed(call_id, &result);
        let record = ProtectedRecord::new(
            self.ids.next_id()?,
            1,
            serde_json::to_value(&result)
                .map_err(|_| error(ErrorCode::InvalidJson, "tool.result"))?,
        );
        let reference = record.reference().clone();
        let intended_value = record.value().clone();
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
        let committed = self
            .commit(snapshot, vec![message], vec![event], records, budget)
            .await;
        if let Err(error) = committed {
            let restored = budget.store().read_record(budget.scope(), &reference).await;
            if !restored.is_ok_and(|record| {
                record.reference() == &reference && record.value() == &intended_value
            }) {
                return Err(error);
            }
            let Ok(saved) = budget.store().load(budget.scope(), budget.run_id()).await else {
                return Err(error);
            };
            let found = saved
                .snapshot
                .tool_ledger
                .iter()
                .find(|entry| &entry.call.call_id == call_id);
            if !found.is_some_and(|entry| entry.state == intended_state) {
                return Err(error);
            }
        }
        if let Some(hooks) = &self.hooks {
            let mut data = context.data.clone();
            data.system_inputs = None;
            let cleanup = ExecutionContext::new(data, CancellationToken::new());
            if let Err(error) = hooks
                .observe(
                    budget.run_id(),
                    HookTarget::AfterTool {
                        call_id: call_id.clone(),
                        result_ref: reference.clone(),
                    },
                    observer_input,
                    &cleanup,
                )
                .await
            {
                if let Ok(mut slot) = self.observer_error.lock() {
                    *slot = Some(ContractError::new(error.code, "hooks.observer_report"));
                }
            }
        }
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

## `crates/wickle/tests/agent_hooks.rs`

```rust
//! Lifecycle hooks preserve authority, original arguments, checkpoint reuse, and committed effects.

#[path = "support/agent.rs"]
#[allow(dead_code)]
mod agent_support;
#[path = "support/agent_resume.rs"]
#[allow(dead_code)]
mod resume_support;
#[path = "support/agent_hooks.rs"]
#[allow(dead_code)]
mod support;
use serde_json::{Value, json};
use std::sync::atomic::Ordering;
use support::*;
use wickle::*;

#[tokio::test]
async fn serial_transform_order_uses_priority_then_id_and_keeps_original_model_arguments() {
    let mut fixture = Fixture::new();
    fixture.add("z", HookPosition::BeforeTool, Behavior::Append, 0, true);
    fixture.add("a", HookPosition::BeforeTool, Behavior::Append, 0, true);
    fixture.add(
        "first",
        HookPosition::BeforeTool,
        Behavior::Append,
        -1,
        true,
    );
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(
        *fixture.order.lock().unwrap(),
        vec!["first", "a", "z", "first", "a", "z", "first", "a", "z"]
    );
    let saved = fixture.saved(&handle).await;
    for (index, name) in ["before", "target", "after"].iter().enumerate() {
        let entry = &saved.snapshot.tool_ledger[index];
        assert_eq!(entry.call.model_inputs, object(json!({"query":name})));
        let record = fixture
            .base
            .base
            .store
            .read_record(&scope(), entry.call.bound_input_ref.as_ref().unwrap())
            .await
            .unwrap();
        let compiled = &fixture.base.registry.get(&id(name)).unwrap().compiled;
        let bound = BoundToolInput::restore(
            &record,
            compiled,
            &scope(),
            handle.run_id(),
            &entry.call,
            saved.snapshot.system_inputs.as_ref(),
        )
        .unwrap();
        assert_eq!(
            bound.original_model_inputs(),
            &object(json!({"query":name}))
        );
        assert_eq!(
            bound.effective_model_inputs(),
            &object(json!({"query":format!("{name}|first|a|z")}))
        );
        assert_eq!(
            fixture.base.tools[index].seen.lock().unwrap()[0].arguments["query"],
            json!(format!("{name}|first|a|z"))
        );
    }
    assert_eq!(
        fixture.base.tools[1].seen.lock().unwrap()[0].arguments["workspace_id"],
        json!(WORKSPACE)
    );
    let HookInput::BeforeTool {
        original_model_inputs,
        model_inputs,
        ..
    } = &fixture.hooks[0].seen.lock().unwrap()[0].0
    else {
        panic!("before_tool input required")
    };
    assert_eq!(original_model_inputs, &object(json!({"query":"before"})));
    assert_eq!(model_inputs, &object(json!({"query":"before|first|a"})));
    let requests = fixture.base.model.requests.lock().unwrap().clone();
    let replayed: Vec<_> = requests[1]
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            ModelContent::ToolCall { arguments, .. } => Some(arguments),
            _ => None,
        })
        .collect();
    assert_eq!(
        replayed,
        vec![
            &object(json!({"query":"before"})),
            &object(json!({"query":"target"})),
            &object(json!({"query":"after"}))
        ]
    );
}

#[tokio::test]
async fn hidden_system_fields_and_invalid_public_values_are_rejected_before_binding_or_execution() {
    for behavior in [Behavior::Hidden, Behavior::InvalidValue] {
        let mut fixture = Fixture::new();
        fixture.add(
            "invalid-transform",
            HookPosition::BeforeTool,
            behavior,
            0,
            true,
        );
        let agent = fixture.agent();
        let handle = fixture.started(&agent).await;
        let result = fixture.outcome(&handle).await;
        assert_ne!(result.result.status(), RunStatus::Succeeded);
        assert_eq!(fixture.base.resolver.calls.load(Ordering::SeqCst), 0);
        assert!(
            fixture
                .base
                .tools
                .iter()
                .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
        );
        assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 1);
        assert!(fixture.policy.seen.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn hook_continuation_does_not_override_host_denial_or_unavailable_policy() {
    for unavailable in [false, true] {
        let mut fixture = Fixture::new();
        fixture.add(
            "transform",
            HookPosition::BeforeTool,
            Behavior::Append,
            0,
            true,
        );
        fixture.policy.deny.store(!unavailable, Ordering::SeqCst);
        fixture.policy.fail.store(unavailable, Ordering::SeqCst);
        let agent = fixture.agent();
        let handle = fixture.started(&agent).await;
        fixture.outcome(&handle).await;
        assert!(
            fixture
                .base
                .tools
                .iter()
                .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
        );
        assert!(!fixture.policy.seen.lock().unwrap().is_empty());
        assert!(fixture.policy.seen.lock().unwrap().iter().all(|input| {
            input.execution_args()["query"]
                .as_str()
                .unwrap()
                .ends_with("|transform")
        }));
    }
}

#[tokio::test]
async fn a_hook_denial_survives_following_continuation_and_prevents_all_effects() {
    let mut fixture = Fixture::new();
    let deny = fixture.add("deny", HookPosition::BeforeTool, Behavior::Deny, 0, true);
    fixture.add(
        "continue",
        HookPosition::BeforeTool,
        Behavior::Append,
        1,
        true,
    );
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    fixture.outcome(&handle).await;
    assert_eq!(deny.calls.load(Ordering::SeqCst), 3);
    assert!(
        fixture
            .base
            .tools
            .iter()
            .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
    );
    assert!(fixture.saved(&handle).await.snapshot.tool_ledger.iter().all(|entry| matches!(&entry.state, ToolCallState::Settled { result } if result.status == ToolResultStatus::Denied && result.effect == ToolEffect::NotApplied)));
}

fn context_data(request: &ModelRequest) -> Vec<(ModelRole, &Value)> {
    request
        .messages
        .iter()
        .flat_map(|message| {
            message.content.iter().filter_map(|content| match content {
                ModelContent::Json { value } if value["kind"] == "context_data" => {
                    Some((message.role, value))
                }
                _ => None,
            })
        })
        .collect()
}

#[tokio::test]
async fn added_context_preserves_core_provenance_lifetime_prefix_and_original_request() {
    let mut fixture = Fixture::new();
    let run = fixture.add(
        "run-data",
        HookPosition::BeforeRun,
        Behavior::Context,
        0,
        true,
    );
    let step = fixture.add(
        "step-data",
        HookPosition::BeforeModel,
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
    assert_eq!(run.calls.load(Ordering::SeqCst), 1);
    assert_eq!(step.calls.load(Ordering::SeqCst), 2);
    let saved = fixture.saved(&handle).await;
    let requests = fixture.base.model.requests.lock().unwrap().clone();
    assert_eq!(requests[0].messages[..2], requests[1].messages[..2]);
    assert!(
        requests[0].messages[..2]
            .iter()
            .all(|message| message.role == ModelRole::System)
    );
    let first = context_data(&requests[0]);
    let second = context_data(&requests[1]);
    assert_eq!(first.len(), 2);
    assert_eq!(second.len(), 2);
    for (role, item) in first.iter().chain(&second) {
        assert_eq!(*role, ModelRole::User);
        assert_eq!(item["origin"], json!("hook"));
    }
    let run_item = |items: &[(ModelRole, &Value)]| {
        items
            .iter()
            .find(|(_, item)| item["source_ref"]["id"] == "run-data")
            .unwrap()
            .1["item_id"]
            .clone()
    };
    let step_item = |items: &[(ModelRole, &Value)]| {
        items
            .iter()
            .find(|(_, item)| item["source_ref"]["id"] == "step-data")
            .unwrap()
            .1["item_id"]
            .clone()
    };
    assert_eq!(run_item(&first), run_item(&second));
    assert_ne!(step_item(&first), step_item(&second));
    assert_eq!(saved.snapshot.request, request("request"));
    for application in &saved.snapshot.hook_applications {
        let record = fixture
            .base
            .base
            .store
            .read_record(&scope(), &application.result_ref)
            .await
            .unwrap();
        let result: HookApplicationRecord = serde_json::from_value(record.value().clone()).unwrap();
        for item in &result.context_items {
            assert_eq!(item.scope, scope());
            assert_eq!(item.origin, ContextOrigin::Hook);
            assert_eq!(item.source_ref, application.hook);
            assert!(
                matches!((&application.target, &item.lifetime),
                (HookTarget::BeforeRun, ContextLifetime::Run { run_id }) if run_id == handle.run_id())
                    || matches!((&application.target, &item.lifetime), (HookTarget::BeforeModel { model_step_id }, ContextLifetime::Step { run_id, model_step_id: item_step }) if run_id == handle.run_id() && model_step_id == item_step)
            );
        }
    }
}

#[tokio::test]
async fn approval_resume_reuses_the_stored_transform_and_original_resolver_binding() {
    let mut fixture = Fixture::new();
    let run = fixture.add(
        "run-data",
        HookPosition::BeforeRun,
        Behavior::Context,
        0,
        true,
    );
    let transform = fixture.add(
        "append",
        HookPosition::BeforeTool,
        Behavior::Append,
        0,
        true,
    );
    fixture.policy.approval.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    assert_eq!(
        fixture.outcome(&original).await.result.status(),
        RunStatus::Waiting
    );
    let before = fixture.saved(&original).await;
    assert_eq!(transform.calls.load(Ordering::SeqCst), 2);
    *transform.suffix.lock().unwrap() = "CHANGED".into();
    *fixture.base.resolver.value.lock().unwrap() = ResolvedSystemInput {
        value: json!(resume_support::CHANGED_RECORD),
        revision: id("new-record"),
    };
    let resumed = completed(
        agent
            .resume(fixture.base.approve(&original, "approve").await, context())
            .await
            .unwrap(),
    );
    assert_eq!(
        fixture.outcome(&resumed).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(run.calls.load(Ordering::SeqCst), 1);
    assert_eq!(transform.calls.load(Ordering::SeqCst), 3); // Only the previously unprocessed final call is new.
    assert_eq!(fixture.base.resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.base.tools[1].seen.lock().unwrap()[0].arguments,
        object(
            json!({"query":"target|append","workspace_id":WORKSPACE,"record_id":resume_support::RECORD})
        )
    );
    assert_eq!(
        fixture.base.tools[2].seen.lock().unwrap()[0].arguments,
        object(json!({"query":"after|CHANGED"}))
    );
    let saved = fixture.saved(&resumed).await;
    assert!(
        saved
            .snapshot
            .hook_applications
            .starts_with(&before.snapshot.hook_applications)
    );
    assert_eq!(
        saved.snapshot.tool_ledger[1].call,
        before.snapshot.tool_ledger[1].call
    );
}

#[tokio::test]
async fn transport_retry_reuses_before_model_for_the_same_logical_step() {
    let mut fixture = Fixture::new();
    let hook = fixture.add(
        "per-step",
        HookPosition::BeforeModel,
        Behavior::Context,
        0,
        true,
    );
    fixture.model.fail_first.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    let result = fixture.outcome(&handle).await;
    assert_eq!(result.result.status(), RunStatus::Succeeded);
    assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 3);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 2);
    let saved = fixture.saved(&handle).await;
    assert_eq!(
        saved.snapshot.model_ledger[0].model_step_id,
        saved.snapshot.model_ledger[1].model_step_id
    );
    assert_ne!(
        saved.snapshot.model_ledger[1].model_step_id,
        saved.snapshot.model_ledger[2].model_step_id
    );
}

#[tokio::test(start_paused = true)]
async fn optional_before_run_callback_failures_continue_but_required_failures_stop_execution() {
    for behavior in [Behavior::Error, Behavior::Pending, Behavior::Panic] {
        for required in [false, true] {
            let mut fixture = Fixture::new();
            let hook = fixture.add("prepare", HookPosition::BeforeRun, behavior, 0, required);
            let agent = fixture.agent();
            let handle = fixture.started(&agent).await;
            let result = fixture.outcome(&handle).await;
            assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
            if required {
                assert_eq!(result.result.status(), RunStatus::Failed);
                assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 0);
                assert!(
                    fixture
                        .base
                        .tools
                        .iter()
                        .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
                );
            } else {
                assert_eq!(result.result.status(), RunStatus::Succeeded);
                let saved = fixture.saved(&handle).await;
                let application = saved.snapshot.hook_applications.first().unwrap();
                let record = fixture
                    .base
                    .base
                    .store
                    .read_record(&scope(), &application.result_ref)
                    .await
                    .unwrap();
                let result: HookApplicationRecord =
                    serde_json::from_value(record.value().clone()).unwrap();
                assert!(result.failure.is_some());
                assert!(result.output.is_none());
            }
        }
    }
}

#[tokio::test]
async fn invalid_optional_patches_and_oversized_required_context_never_reach_the_model() {
    for behavior in [Behavior::WrongVariant, Behavior::Oversized] {
        let mut fixture = Fixture::new();
        fixture.add(
            "invalid-context",
            HookPosition::BeforeRun,
            behavior,
            0,
            false,
        );
        let agent = fixture.agent();
        let handle = fixture.started(&agent).await;
        assert_eq!(
            fixture.outcome(&handle).await.result.status(),
            RunStatus::Failed
        );
        assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 0);
        assert!(
            fixture
                .base
                .tools
                .iter()
                .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn observer_failure_after_commit_never_changes_outcomes_or_repeats_tools() {
    for behavior in [Behavior::Error, Behavior::Panic, Behavior::Pending] {
        let mut fixture = Fixture::new();
        let tool_observer = fixture.add("observe-tool", HookPosition::AfterTool, behavior, 0, true);
        let run_observer = fixture.add("observe-run", HookPosition::AfterRun, behavior, 0, true);
        let agent = fixture.agent();
        let handle = fixture.started(&agent).await;
        let result = fixture.outcome(&handle).await;
        assert_eq!(result.result.status(), RunStatus::Succeeded);
        let before_reports = fixture.saved(&handle).await;
        let view = observations(&handle, 4).await;
        assert!(view.local_error.is_none());
        assert_eq!(view.reports.len(), 4);
        assert!(
            view.reports
                .iter()
                .all(|report| matches!(report.status, HookObservationStatus::Failed { .. }))
        );
        assert_eq!(
            fixture.saved(&handle).await.snapshot,
            before_reports.snapshot
        );
        assert_eq!(tool_observer.calls.load(Ordering::SeqCst), 3);
        assert_eq!(run_observer.calls.load(Ordering::SeqCst), 1);
        let replay = fixture.started(&agent).await;
        assert_eq!(fixture.outcome(&replay).await, result);
        assert_eq!(fixture.base.tools[1].calls.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.base.tools[1].applied.load(Ordering::SeqCst), 1);
        assert_eq!(tool_observer.calls.load(Ordering::SeqCst), 3);
        assert_eq!(run_observer.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn waiting_does_not_invoke_after_run_and_cancellation_stops_a_pending_before_run() {
    let mut fixture = Fixture::new();
    let observer = fixture.add(
        "after-run",
        HookPosition::AfterRun,
        Behavior::Observe,
        0,
        true,
    );
    fixture.policy.approval.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Waiting
    );
    assert_eq!(observer.calls.load(Ordering::SeqCst), 0);
    let resumed = completed(
        agent
            .resume(fixture.base.approve(&handle, "approve").await, context())
            .await
            .unwrap(),
    );
    fixture.outcome(&resumed).await;
    observations(&resumed, 1).await;
    assert_eq!(observer.calls.load(Ordering::SeqCst), 1);

    let mut fixture = Fixture::new();
    let hook = fixture.add("blocked", HookPosition::BeforeRun, Behavior::Pause, 0, true);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    gate(&hook.entered).await;
    completed(handle.cancel(id("stop-hook"), &context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Cancelled
    );
    assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 0);
    assert!(
        fixture
            .base
            .tools
            .iter()
            .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn revoked_hook_permission_prevents_the_callback_and_any_model_dispatch() {
    let mut fixture = Fixture::new();
    let hook = fixture.add(
        "prepare",
        HookPosition::BeforeRun,
        Behavior::Context,
        0,
        true,
    );
    fixture.policy.deny_hooks.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Failed
    );
    assert_eq!(hook.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 0);
    assert!(
        fixture
            .base
            .tools
            .iter()
            .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn observer_report_storage_failure_is_separate_from_the_successful_run_outcome() {
    let mut fixture = Fixture::new();
    let observer = fixture.add(
        "after-run",
        HookPosition::AfterRun,
        Behavior::Observe,
        0,
        true,
    );
    fixture.store.observer_failure.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    let saved = fixture.saved(&handle).await;
    let view = observations(&handle, 1).await;
    assert_eq!(
        view.local_error.as_ref().unwrap().code,
        ErrorCode::PersistenceUnavailable
    );
    assert!(view.reports.is_empty());
    assert_eq!(fixture.saved(&handle).await.snapshot, saved.snapshot);
    assert_eq!(fixture.outcome(&handle).await, outcome);
    let replay = fixture.started(&agent).await;
    assert_eq!(fixture.outcome(&replay).await, outcome);
    assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.base.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn failed_transform_persistence_stops_execution_before_a_model_or_tool_can_use_it() {
    let mut fixture = Fixture::new();
    let hook = fixture.add(
        "prepare",
        HookPosition::BeforeRun,
        Behavior::Context,
        0,
        true,
    );
    fixture.store.transform_failure.store(1, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    assert_eq!(
        handle.outcome(&context()).await.unwrap_err().code,
        ErrorCode::PersistenceUnavailable
    );
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    assert!(
        fixture
            .saved(&handle)
            .await
            .snapshot
            .hook_applications
            .is_empty()
    );
    assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 0);
    assert!(
        fixture
            .base
            .tools
            .iter()
            .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn transform_replay_distinguishes_unsaved_results_from_lost_commit_acknowledgements() {
    for fail_mode in [1, 2] {
        let mut fixture = Fixture::new();
        let hook = fixture.add(
            "append",
            HookPosition::BeforeTool,
            Behavior::Append,
            0,
            true,
        );
        fixture
            .store
            .transform_failure
            .store(fail_mode, Ordering::SeqCst);
        let bindings = fixture.bindings();
        let runtime = bindings.hooks.as_ref().unwrap().clone();
        let agent = create_agent(fixture.profile(), bindings).unwrap();
        let handle = fixture.started(&agent).await;
        assert_eq!(
            handle.outcome(&context()).await.unwrap_err().code,
            ErrorCode::PersistenceUnavailable
        );
        let saved = fixture.saved(&handle).await;
        assert_eq!(
            saved.snapshot.hook_applications.len(),
            usize::from(fail_mode == 2)
        );
        assert_eq!(fixture.base.tools[0].calls.load(Ordering::SeqCst), 0);
        let call = &saved.snapshot.tool_ledger[0].call;
        let compiled = &fixture.base.registry.get(&id("before")).unwrap().compiled;
        let input = HookInput::BeforeTool {
            tool: compiled.to_model_tool(),
            descriptor_digest: compiled.descriptor_digest().clone(),
            compiled_digest: compiled.digest().clone(),
            original_model_inputs: call.model_inputs.clone(),
            model_inputs: call.model_inputs.clone(),
        };
        let lease = fixture
            .store
            .acquire_lease(
                &scope(),
                handle.run_id(),
                &id("replay-worker"),
                fixture.base.base.clock.now().unwrap().utc_ms,
                1000,
            )
            .await
            .unwrap();
        let budget = RunBudget::attach(
            fixture.store.clone(),
            fixture.base.base.clock.clone(),
            fixture.base.base.ids.clone(),
            scope(),
            handle.run_id().clone(),
            lease,
            Default::default(),
        )
        .await
        .unwrap();
        let replay = runtime
            .transform(
                HookTarget::BeforeTool {
                    call_id: call.call_id.clone(),
                },
                input,
                &context(),
                &budget,
            )
            .await
            .unwrap();
        assert_eq!(
            replay.model_inputs,
            Some(object(json!({"query":"before|append"})))
        );
        assert_eq!(
            hook.calls.load(Ordering::SeqCst),
            if fail_mode == 1 { 2 } else { 1 }
        );
        assert_eq!(
            fixture
                .saved(&handle)
                .await
                .snapshot
                .hook_applications
                .len(),
            1
        );
        assert!(
            fixture
                .base
                .tools
                .iter()
                .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn after_tool_observers_use_remaining_run_time_and_expired_calls_do_not_enter_them() {
    let mut fixture = Fixture::new();
    fixture.base.profile.limits.max_elapsed_ms = 5.try_into().unwrap();
    let tool_observer = fixture.add(
        "tool-watch",
        HookPosition::AfterTool,
        Behavior::Pending,
        0,
        true,
    );
    let run_observer = fixture.add(
        "run-watch",
        HookPosition::AfterRun,
        Behavior::Pending,
        0,
        true,
    );
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    );
    let saved = fixture.saved(&handle).await;
    let view = observations(&handle, 4).await;
    assert!(view.local_error.is_none());
    assert_eq!(tool_observer.calls.load(Ordering::SeqCst), 1);
    assert_eq!(run_observer.calls.load(Ordering::SeqCst), 1);
    let tool_time = tool_observer.entered_at.lock().unwrap()[0];
    assert!(
        tool_observer.seen.lock().unwrap()[0].1.deadline - tool_time
            <= std::time::Duration::from_millis(5)
    );
    let run_time = run_observer.entered_at.lock().unwrap()[0];
    assert!(run_time > tool_time);
    assert_eq!(
        run_observer.seen.lock().unwrap()[0].1.deadline - run_time,
        std::time::Duration::from_millis(20)
    );
    assert_eq!(fixture.base.tools[0].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.base.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.base.tools[2].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.saved(&handle).await.snapshot, saved.snapshot);
    assert_eq!(fixture.outcome(&handle).await, outcome);
}

#[tokio::test(start_paused = true)]
async fn after_run_uses_a_fresh_bounded_cleanup_context_after_explicit_cancellation() {
    let mut fixture = Fixture::new();
    let before = fixture.add(
        "waiting-prepare",
        HookPosition::BeforeRun,
        Behavior::Pause,
        0,
        true,
    );
    let after = fixture.add(
        "after-cancel",
        HookPosition::AfterRun,
        Behavior::Pending,
        0,
        true,
    );
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    gate(&before.entered).await;
    completed(
        handle
            .cancel(id("cancel-before-model"), &context())
            .await
            .unwrap(),
    );
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(outcome.result.status(), RunStatus::Cancelled);
    let saved = fixture.saved(&handle).await;
    let view = observations(&handle, 1).await;
    assert!(view.local_error.is_none());
    assert_eq!(after.calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        view.reports[0].status,
        HookObservationStatus::Failed { .. }
    ));
    let entered = after.entered_at.lock().unwrap()[0];
    assert_eq!(
        after.seen.lock().unwrap()[0].1.deadline - entered,
        std::time::Duration::from_millis(20)
    );
    assert_eq!(fixture.saved(&handle).await.snapshot, saved.snapshot);
    assert_eq!(fixture.outcome(&handle).await, outcome);
    assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 0);
    assert!(
        fixture
            .base
            .tools
            .iter()
            .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
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
        hook_plan_ref: None,
        hook_applications: vec![],
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
        hook_plan_ref: None,
        hook_applications: vec![],
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

## `crates/wickle/tests/state_hooks.rs`

```rust
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
            hooks: None,
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

## `crates/wickle/tests/support/agent_hooks.rs`

```rust
//! Hook fixtures observe actual executor, policy, model, and persistence boundaries.

use super::resume_support;
use futures_util::stream;
pub use resume_support::{
    WORKSPACE, completed, context, gate, id, object, reference, request, scope,
};
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

pub struct Catalog {
    pub definitions: Vec<HookDefinition>,
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
                    version: Some(reference.version.clone().unwrap_or_else(|| id("1"))),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("hook-fixture")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| reference.id.clone()),
                hook_position: self
                    .definitions
                    .iter()
                    .find(|definition| definition.hook.id == reference.id)
                    .map(|definition| definition.position),
                exports: vec![],
            })
        })
    }
}

pub struct Policy {
    pub approval: AtomicBool,
    pub deny: AtomicBool,
    pub fail: AtomicBool,
    pub deny_hooks: AtomicBool,
    pub seen: Mutex<Vec<ToolPolicyInput>>,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if matches!(request.action, PolicyAction::InvokeHook { .. })
                && self.deny_hooks.load(Ordering::SeqCst)
            {
                return Ok(PolicyDecision::Deny {
                    reason: id("hook-revoked"),
                });
            }
            if let PolicyAction::ExecuteTool { input } = &request.action {
                self.seen.lock().unwrap().push(input.clone());
                if self.fail.load(Ordering::SeqCst) {
                    return Err(ContractError::new(
                        ErrorCode::PolicyUnavailable,
                        "fixture.policy",
                    ));
                }
                if self.deny.load(Ordering::SeqCst) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("host-denied"),
                    });
                }
                if self.approval.load(Ordering::SeqCst)
                    && input.tool.id == id("target")
                    && input.approval().is_none()
                {
                    return Ok(PolicyDecision::RequireApproval {
                        reason: id("target-approval"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}

pub struct RetryModel {
    pub inner: Arc<resume_support::Model>,
    pub attempts: AtomicUsize,
    pub fail_first: AtomicBool,
}
impl ModelPort for RetryModel {
    fn binding(&self) -> ModelPortBinding {
        self.inner.binding()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 && self.fail_first.load(Ordering::SeqCst) {
            Box::pin(stream::iter([Ok(ModelEvent::ResponseError {
                kind: ModelFailureKind::Transport,
                metadata: ModelResponseMetadata::default(),
            })]))
        } else {
            self.inner.generate(request, context)
        }
    }
}

pub struct Fixture {
    pub base: resume_support::Fixture,
    pub policy: Arc<Policy>,
    pub model: Arc<RetryModel>,
    pub hooks: Vec<Arc<Hook>>,
    pub order: Arc<Mutex<Vec<String>>>,
    pub store: Arc<FaultStore>,
}
impl Fixture {
    pub fn new() -> Self {
        let base = resume_support::Fixture::new(resume_support::Mode::Approval);
        let model = Arc::new(RetryModel {
            inner: base.model.clone(),
            attempts: AtomicUsize::new(0),
            fail_first: AtomicBool::new(false),
        });
        let store = Arc::new(FaultStore {
            inner: base.base.store.clone(),
            transform_failure: AtomicUsize::new(0),
            observer_failure: AtomicBool::new(false),
            transform_writes: AtomicUsize::new(0),
        });
        Self {
            base,
            policy: Arc::new(Policy {
                approval: AtomicBool::new(false),
                deny: AtomicBool::new(false),
                fail: AtomicBool::new(false),
                deny_hooks: AtomicBool::new(false),
                seen: Mutex::new(vec![]),
            }),
            model,
            hooks: vec![],
            order: Arc::new(Mutex::new(vec![])),
            store,
        }
    }
    pub fn add(
        &mut self,
        name: &str,
        position: HookPosition,
        behavior: Behavior,
        priority: i32,
        required: bool,
    ) -> Arc<Hook> {
        let hook = Arc::new(Hook {
            definition: HookDefinition {
                hook: reference(name),
                position,
                priority,
                required,
                timeout_ms: if matches!(behavior, Behavior::Pending) {
                    20
                } else {
                    5000
                },
                max_output_bytes: 4096,
            },
            behavior,
            suffix: Mutex::new(name.to_owned()),
            calls: AtomicUsize::new(0),
            order: self.order.clone(),
            store: self.base.base.store.clone(),
            entered: Notify::new(),
            release: Semaphore::new(0),
            seen: Mutex::new(vec![]),
            entered_at: Mutex::new(vec![]),
        });
        self.hooks.push(hook.clone());
        hook
    }
    pub fn bindings(&self) -> AgentBindings {
        let mut bindings = self.base.bindings();
        bindings.state = self.store.clone();
        let policy =
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(5)).unwrap());
        // Successful hook chains exercise several persisted boundaries. Short
        // timing limits belong to the explicit paused-clock deadline tests.
        bindings.settings.lease_ttl_ms = 30_000;
        bindings.settings.heartbeat_interval_ms = 5_000;
        bindings.policy = policy.clone();
        bindings.profile_resolver = Arc::new(Catalog {
            definitions: self
                .hooks
                .iter()
                .map(|hook| hook.definition.clone())
                .collect(),
        });
        bindings.model_exchange = Arc::new(
            ModelExchange::new(self.model.clone(), policy)
                .with_route_inspector(self.base.base.inspector.clone(), Duration::from_secs(1))
                .unwrap()
                .with_retry_policy(ModelRetryPolicy {
                    max_retries: 1,
                    backoff_ms: 0,
                }),
        );
        let registry = Arc::new(
            HookRegistry::new(
                scope(),
                self.hooks
                    .iter()
                    .map(|hook| HookRegistration {
                        definition: hook.definition.clone(),
                        handler: hook.clone(),
                    })
                    .collect(),
            )
            .unwrap(),
        );
        bindings.hooks = Some(Arc::new(HookRuntime::new(
            bindings.state.clone(),
            bindings.policy.clone(),
            bindings.clock.clone(),
            bindings.ids.clone(),
            registry,
        )));
        bindings
    }
    pub fn profile(&self) -> AgentProfile {
        let mut profile = self.base.profile.clone();
        profile.limits.max_recovery_attempts = 1;
        profile.hooks = Some(
            self.hooks
                .iter()
                .map(|hook| {
                    HookRef::Catalog(CatalogHookRef {
                        hook_id: hook.definition.hook.id.clone(),
                        version: hook.definition.hook.version.clone(),
                        position: hook.definition.position,
                    })
                })
                .collect(),
        );
        profile
    }
    pub fn agent(&self) -> Agent {
        create_agent(self.profile(), self.bindings()).unwrap()
    }
    pub async fn started(&self, agent: &Agent) -> RunHandle {
        self.base.started(agent).await
    }
    pub async fn outcome(&self, handle: &RunHandle) -> RunOutcome {
        self.base.outcome(handle).await
    }
    pub async fn saved(&self, handle: &RunHandle) -> StoredRun {
        self.base.saved(handle).await
    }
}

#[derive(Clone, Copy)]
pub enum Behavior {
    Context,
    Append,
    Hidden,
    InvalidValue,
    Deny,
    Error,
    Pending,
    Panic,
    Observe,
    Pause,
    WrongVariant,
    Oversized,
}
pub struct Hook {
    pub definition: HookDefinition,
    pub behavior: Behavior,
    pub suffix: Mutex<String>,
    pub calls: AtomicUsize,
    pub order: Arc<Mutex<Vec<String>>>,
    pub store: Arc<MemoryStateStore>,
    pub entered: Notify,
    pub release: Semaphore,
    pub seen: Mutex<Vec<(HookInput, HookContext)>>,
    pub entered_at: Mutex<Vec<tokio::time::Instant>>,
}
impl HookHandler for Hook {
    fn call<'a>(
        &'a self,
        input: &'a HookInput,
        context: &'a HookContext,
    ) -> PortFuture<'a, HookOutput> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered_at
                .lock()
                .unwrap()
                .push(tokio::time::Instant::now());
            self.order.lock().unwrap().push(context.hook.id.to_string());
            self.seen
                .lock()
                .unwrap()
                .push((input.clone(), context.clone()));
            assert_eq!(context.scope, scope());
            assert_eq!(context.hook, self.definition.hook);
            assert_eq!(context.target.position(), self.definition.position);
            match &context.target {
                HookTarget::AfterTool {
                    call_id,
                    result_ref,
                } => {
                    let stored = self.store.load(&scope(), &context.run_id).await?;
                    let result: ToolResult = serde_json::from_value(
                        self.store
                            .read_record(&scope(), result_ref)
                            .await?
                            .value()
                            .clone(),
                    )
                    .unwrap();
                    let entry = stored
                        .snapshot
                        .tool_ledger
                        .iter()
                        .find(|entry| &entry.call.call_id == call_id)
                        .unwrap();
                    assert!(
                        matches!(&entry.state, ToolCallState::Settled { result: saved } if saved == &result)
                    );
                    let HookInput::AfterTool {
                        status,
                        effect,
                        content,
                        error_code,
                        ..
                    } = input
                    else {
                        panic!("observer input mismatch")
                    };
                    assert_eq!(
                        (*status, *effect, content, error_code),
                        (
                            result.status,
                            result.effect,
                            &result.content,
                            &result.error.as_ref().map(|error| error.code.clone())
                        )
                    );
                }
                HookTarget::AfterRun {
                    outcome_ref,
                    revision,
                } => {
                    let stored = self.store.load(&scope(), &context.run_id).await?;
                    assert!(stored.snapshot.status.is_terminal());
                    assert_eq!(stored.snapshot.revision, *revision);
                    let value = self.store.read_record(&scope(), outcome_ref).await?;
                    assert_eq!(
                        serde_json::from_value::<RunOutcome>(value.value().clone()).unwrap(),
                        stored.snapshot.outcome.unwrap()
                    );
                }
                _ => {}
            }
            self.entered.notify_one();
            match self.behavior {
                Behavior::Error => {
                    return Err(ContractError::new(
                        ErrorCode::InvalidContract,
                        "raw-hook-diagnostic",
                    ));
                }
                Behavior::Pending => return std::future::pending().await,
                Behavior::Panic => panic!("synthetic hook panic"),
                Behavior::Pause => {
                    self.release.acquire().await.unwrap().forget();
                }
                Behavior::WrongVariant => return Ok(HookOutput::Observed {}),
                _ => {}
            }
            match input {
                HookInput::BeforeRun { .. } | HookInput::BeforeModel { .. } => {
                    let value = if matches!(self.behavior, Behavior::Oversized) {
                        json!("x".repeat(5000))
                    } else {
                        json!({"marker":self.suffix.lock().unwrap().clone(),"target":context.target})
                    };
                    Ok(HookOutput::Context {
                        additions: vec![HookContextAddition {
                            content: vec![InputContent::Json { value }],
                            priority: ContextPriority::Required,
                        }],
                    })
                }
                HookInput::BeforeTool {
                    tool,
                    original_model_inputs,
                    model_inputs,
                    ..
                } => {
                    assert_eq!(
                        tool.model_input_schema["properties"],
                        json!({"query":{"type":"string"}})
                    );
                    assert!(original_model_inputs.get("workspace_id").is_none());
                    let mut model_inputs = model_inputs.clone();
                    match self.behavior {
                        Behavior::Append => {
                            let query = model_inputs["query"].as_str().unwrap();
                            model_inputs.insert(
                                "query".into(),
                                json!(format!("{query}|{}", self.suffix.lock().unwrap())),
                            );
                        }
                        Behavior::Hidden => {
                            model_inputs.insert("workspace_id".into(), json!(WORKSPACE));
                        }
                        Behavior::InvalidValue => {
                            model_inputs.insert("query".into(), json!(42));
                        }
                        _ => {}
                    }
                    Ok(HookOutput::Tool {
                        model_inputs,
                        deny: matches!(self.behavior, Behavior::Deny).then(|| id("hook-denied")),
                    })
                }
                HookInput::AfterTool { .. } | HookInput::AfterRun { .. } => {
                    Ok(HookOutput::Observed {})
                }
            }
        })
    }
}

pub async fn observations(handle: &RunHandle, expected: usize) -> HookObservationView {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let view = completed(handle.hook_observations(&context()).await.unwrap());
            if view.local_error.is_some() || view.reports.len() >= expected {
                return view;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("observer reports must finish")
}

pub struct FaultStore {
    pub inner: Arc<MemoryStateStore>,
    pub transform_failure: AtomicUsize,
    pub observer_failure: AtomicBool,
    pub transform_writes: AtomicUsize,
}
impl StateStore for FaultStore {
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
        self.inner.read_record(s, r)
    }
    fn commit<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            let current = self.inner.load(s, r).await?;
            if input.snapshot.hook_applications.len() > current.snapshot.hook_applications.len() {
                self.transform_writes.fetch_add(1, Ordering::SeqCst);
                match self.transform_failure.swap(0, Ordering::SeqCst) {
                    1 => {
                        return Err(ContractError::new(
                            ErrorCode::PersistenceUnavailable,
                            "hook.commit",
                        ));
                    }
                    2 => {
                        self.inner.commit(s, r, input).await?;
                        return Err(ContractError::new(
                            ErrorCode::PersistenceUnavailable,
                            "hook.ack",
                        ));
                    }
                    _ => {}
                }
            }
            self.inner.commit(s, r, input).await
        })
    }
    fn record_hook_observation<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        report: HookObservation,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            if self.observer_failure.load(Ordering::SeqCst) {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "hook.observer_report",
                ));
            }
            self.inner.record_hook_observation(s, r, report).await
        })
    }
    fn read_hook_observations<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
    ) -> PortFuture<'a, Vec<HookObservation>> {
        self.inner.read_hook_observations(s, r)
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
        hook_plan_ref: None,
        hook_applications: vec![],
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
            system_input_resolver: None, external_receipt_verifier: None, hooks: None,
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
        hook_plan_ref: None,
        hook_applications: vec![],
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
        hook_plan_ref: None,
        hook_applications: vec![],
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

## `tests/support/hooks_consumer.rs`

```rust
// Synthetic model, tool, and metadata inspector. No provider network or business
// database calls occur. SQLite is real; lifecycle transforms and observer reports
// use the public Agent API and survive reopening the store.
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
                hook_position: match reference.id.as_str() {
                    "run-data" => Some(HookPosition::BeforeRun),
                    "step-data" => Some(HookPosition::BeforeModel),
                    "normalize" => Some(HookPosition::BeforeTool),
                    "tool-observer" => Some(HookPosition::AfterTool),
                    "run-observer" => Some(HookPosition::AfterRun),
                    _ => None,
                },
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
        let context_items: Vec<_> = request
            .messages
            .iter()
            .flat_map(|message| {
                message.content.iter().filter_map(|content| match content {
                    ModelContent::Json { value } if value["kind"] == "context_data" => {
                        assert_eq!(message.role, ModelRole::User);
                        Some(value)
                    }
                    _ => None,
                })
            })
            .collect();
        assert_eq!(context_items.len(), 2);
        assert!(context_items.iter().all(|value| value["origin"] == "hook"));
        assert_eq!(
            context_items
                .iter()
                .map(|value| value["source_ref"]["id"].as_str().unwrap())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["run-data", "step-data"])
        );
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
                        json!({"status":"succeeded","effect":"not_applied","content":[{"type":"json","value":{"query":format!("{query}|hook"),"count":index+2}}]})
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
                json!({"query":"alpha|hook","limit":2,"workspace_id":WORKSPACE})
            } else {
                json!({"query":"beta|hook","limit":3,"workspace_id":WORKSPACE})
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
struct Hooks {
    calls: AtomicUsize,
}
impl HookHandler for Hooks {
    fn call<'a>(
        &'a self,
        input: &'a HookInput,
        context: &'a HookContext,
    ) -> PortFuture<'a, HookOutput> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(match input {
                HookInput::BeforeRun { .. } | HookInput::BeforeModel { .. } => {
                    HookOutput::Context {
                        additions: vec![HookContextAddition {
                            content: vec![InputContent::Json {
                                value: json!({"marker":context.hook.id}),
                            }],
                            priority: ContextPriority::Required,
                        }],
                    }
                }
                HookInput::BeforeTool {
                    original_model_inputs,
                    model_inputs,
                    ..
                } => {
                    assert_eq!(model_inputs, original_model_inputs);
                    let mut inputs = model_inputs.clone();
                    inputs.insert(
                        "query".into(),
                        json!(format!("{}|hook", model_inputs["query"].as_str().unwrap())),
                    );
                    HookOutput::Tool {
                        model_inputs: inputs,
                        deny: None,
                    }
                }
                HookInput::AfterTool { status, effect, .. } => {
                    assert_eq!(*status, ToolResultStatus::Succeeded);
                    assert_eq!(*effect, ToolEffect::NotApplied);
                    HookOutput::Observed {}
                }
                HookInput::AfterRun { status, .. } => {
                    assert_eq!(*status, RunStatus::Succeeded);
                    HookOutput::Observed {}
                }
            })
        })
    }
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
        std::env::temp_dir().join(format!("wickle-hooks-{}", RandomIdSource.next_id()?)),
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
        "hooks":[{"hook_id":"run-data","version":"1","position":"before_run"},{"hook_id":"step-data","version":"1","position":"before_model"},{"hook_id":"normalize","version":"1","position":"before_tool"},{"hook_id":"tool-observer","version":"1","position":"after_tool"},{"hook_id":"run-observer","version":"1","position":"after_run"}],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":4,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}
    }"#,
    )?;
    let hook = Arc::new(Hooks {
        calls: AtomicUsize::new(0),
    });
    let make_agent = |store: Arc<SqliteStateStore>| -> Result<Agent, ContractError> {
        let (system_inputs, tools) = registry(&scope, search.clone())?;
        let registry = HookRegistry::new(
            scope.clone(),
            [
                ("run-data", HookPosition::BeforeRun),
                ("step-data", HookPosition::BeforeModel),
                ("normalize", HookPosition::BeforeTool),
                ("tool-observer", HookPosition::AfterTool),
                ("run-observer", HookPosition::AfterRun),
            ]
            .into_iter()
            .map(|(name, position)| HookRegistration {
                definition: HookDefinition {
                    hook: reference(name),
                    position,
                    priority: 0,
                    required: true,
                    timeout_ms: 1000,
                    max_output_bytes: 4096,
                },
                handler: hook.clone(),
            })
            .collect(),
        )?;
        let runtime = Arc::new(HookRuntime::new(
            store.clone(),
            policy.clone(),
            Arc::new(SystemClock::new()),
            Arc::new(RandomIdSource),
            Arc::new(registry),
        ));
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
                system_input_resolver: None,
                external_receipt_verifier: None,
                hooks: Some(runtime),
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
    assert_eq!(hook.calls.load(Ordering::SeqCst), 0);
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
    let saved_before_reports = store.load(&scope, &run_id).await?;
    let reports = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let view = completed(handle.hook_observations(&context).await?)?;
            if let Some(error) = view.local_error {
                return Err::<_, Box<dyn std::error::Error>>(Box::new(error));
            }
            if view.reports.len() == 3 {
                return Ok(view.reports);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await??;
    assert!(
        reports
            .iter()
            .all(|report| report.status == HookObservationStatus::Completed)
    );
    let saved = store.load(&scope, &run_id).await?;
    assert_eq!(saved.snapshot, saved_before_reports.snapshot);
    assert_eq!(saved.snapshot.hook_applications.len(), 5);
    for application in &saved.snapshot.hook_applications {
        let record = store.read_record(&scope, &application.result_ref).await?;
        let result: HookApplicationRecord = serde_json::from_value(record.value().clone())?;
        assert_eq!(result.hook, application.hook);
        assert!(result.failure.is_none());
        assert!(
            result
                .context_items
                .iter()
                .all(|item| item.origin == ContextOrigin::Hook && item.scope == scope)
        );
    }
    assert_eq!(hook.calls.load(Ordering::SeqCst), 8);
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
    assert_eq!(
        reopened.read_hook_observations(&scope, &run_id).await?,
        reports
    );
    let resolver_calls = catalog.calls.load(Ordering::SeqCst);
    let replay_agent = make_agent(reopened)?;
    let replay = completed(replay_agent.start(request, context.clone()).await?)?;
    assert_eq!(replay.run_id(), &run_id);
    assert_eq!(completed(replay.outcome(&context).await?)?, outcome);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(search.arguments.lock().unwrap().len(), 2);
    assert_eq!(catalog.calls.load(Ordering::SeqCst), resolver_calls);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 8);
    println!(
        "hooks consumer: core-stamped Run/step context; original/effective tool arguments; committed tool/Run reports; real SQLite reopen and replay without repeated model/tool/hooks (synthetic Host, no provider network)"
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
        hook_plan_ref: None,
        hook_applications: vec![],
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
            external_receipt_verifier: None, hooks: None,
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
        hook_plan_ref: None,
        hook_applications: vec![],
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
        hook_plan_ref: None,
        hook_applications: vec![],
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
        hook_plan_ref: None,
        hook_applications: vec![],
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
                system_input_resolver: None, external_receipt_verifier: None, hooks: None,
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
