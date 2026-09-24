# 40장 전체 구현과 변경 검사

[강의](../40-atomic-store.md) · [전체 변경 패치](../solutions/40-atomic-store.patch)

기준 `5c04c0e6a32a460fb8c71de42c9dd51d94d0ec56`. 이 단계에서 바뀐 Rust·manifest·Python 파일의 전체 내용이다. 이전 버전과의 정확한 교체 위치·삭제는 patch를 따른다. 다음 장의 코드와 섞지 않는다.

## `crates/wickle-mcp/tests/support/binding.rs`

```rust
use super::*;
use std::{collections::BTreeSet, sync::Arc};
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

struct Owned;
impl PolicyPort for Owned {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::ExecuteTool { input } = &request.action {
                if input.execution_args().get("workspace_id") != Some(&json!(WORKSPACE)) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("wrong_workspace"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}
async fn plan(
    store: &MemoryStateStore,
    scope: &Scope,
    run: &Id,
    lease: &RunLease,
    clock: &SystemClock,
    compiled: &CompiledTool,
    model_inputs: JsonObject,
) -> Result<(), ContractError> {
    let call_id = "call";
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

pub async fn bound_arguments(
    compiled: &CompiledTool,
) -> Result<JsonObject, Box<dyn std::error::Error + Send + Sync>> {
    let scope = scope();
    let registry = Arc::new(registry());
    let values = SystemInputs::new(JsonObject::from([
        ("workspace_id".into(), json!(WORKSPACE)),
        ("unused_key".into(), json!("must-not-be-sent")),
    ]));
    let captured =
        RunSystemInputs::capture(scope.clone(), Some(values.clone()), &registry).unwrap();
    let input_record = captured.to_record(id("run-inputs"), 1);
    let input_ref = captured.snapshot_ref(input_record.reference()).unwrap();
    let profile=AgentProfile::from_json(&json!({"schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1","name":"Assistant","description":"MCP binding fixture","instructions":{"text":"Use available evidence"},"model_binding":"primary","tools":[{"tool_id":compiled.descriptor().tool.id,"version":compiled.descriptor().tool.version}],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},"limits":{"max_model_calls":4,"max_tool_attempts":4,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}}).to_string()).unwrap();
    let profile = ProfileValidator::new(&Catalog)
        .validate(&profile, &scope)
        .await
        .unwrap();
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Read recent results".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        max_output_tokens: None,
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
        recovery_receipts: vec![],
        hook_plan_ref: None,
        source_plan_ref: None,
        skill_plan_ref: None,
        context_plan_ref: None,
        context_revision_ref: None,
        context_decisions: vec![],
        verification_plan_ref: None,
        candidate_ref: None,
        verification_records: vec![],
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
                execution_principal_ref: id("execution-principal"),
                submitted: None,
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
    let context = ExecutionContext::new(
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

    let binder = InputBinder::new(
        registry,
        None,
        Arc::new(PolicyGate::new(Arc::new(Owned), Duration::from_secs(1)).unwrap()),
        Arc::new(RandomIdSource),
    );
    plan(
        &store,
        &scope,
        &run,
        &lease,
        &clock,
        compiled,
        JsonObject::from([("query".into(), json!("latest"))]),
    )
    .await
    .unwrap();
    let result = binder
        .bind(compiled, &id("call"), &context, &budget)
        .await
        .unwrap();
    assert_eq!(
        result.input.original_model_inputs(),
        &JsonObject::from([("query".into(), json!("latest"))])
    );
    assert_eq!(result.input.execution_args(), &args());
    Ok(result.input.execution_args().clone())
}
```

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

impl wickle::ExecutionTransactions for SqliteStateStore {
    fn read_execution<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
    ) -> PortFuture<'a, wickle::ExecutionHistory> {
        let id = run_id.clone();
        self.transact(scope, false, move |state, scope, runtime| {
            runtime.block_on(state.read_execution(scope, &id))
        })
    }
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        command: wickle::ControlCommand,
    ) -> PortFuture<'a, wickle::ControlReceipt> {
        let id = run_id.clone();
        self.transact(scope, true, move |state, scope, runtime| {
            runtime.block_on(state.submit_control_command(scope, &id, command))
        })
    }
    fn begin_segment<'a>(
        &'a self,
        scope: &'a Scope,
        mut request: wickle::BeginSegmentRequest,
    ) -> PortFuture<'a, wickle::BeginSegmentResult> {
        let submitted = std::time::Instant::now();
        self.transact(scope, true, move |state, scope, runtime| {
            let elapsed = i64::try_from(submitted.elapsed().as_millis())
                .map_err(|_| error(ErrorCode::InvalidContract, "sqlite.elapsed"))?;
            request.now_ms = request
                .now_ms
                .checked_add(elapsed)
                .ok_or_else(|| error(ErrorCode::InvalidContract, "sqlite.clock"))?;
            runtime.block_on(state.begin_segment(scope, request))
        })
    }
}
```

## `crates/wickle-state-sqlite/tests/execution_store.rs`

```rust
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
```

## `crates/wickle-state-sqlite/tests/support/recovery_store.rs`

```rust
//! Process termination at durable state boundaries, without running destructors.
use std::sync::Arc;
use wickle::*;
use wickle_state_sqlite::SqliteStateStore;
pub struct CrashStore {
    pub inner: Arc<SqliteStateStore>,
    pub boundary: String,
}
impl StateStore for CrashStore {
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
    fn admit<'a>(
        &'a self,
        scope: &'a Scope,
        input: AdmissionInput,
    ) -> PortFuture<'a, AdmissionResult> {
        self.inner.admit(scope, input)
    }
    fn load<'a>(&'a self, scope: &'a Scope, run_id: &'a Id) -> PortFuture<'a, StoredRun> {
        self.inner.load(scope, run_id)
    }
    fn load_session<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
    ) -> PortFuture<'a, SessionSnapshot> {
        self.inner.load_session(scope, session_id)
    }
    fn check_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.check_lease(scope, run_id, lease, now_ms)
    }
    fn acquire_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        owner: &'a Id,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner
            .acquire_lease(scope, run_id, owner, now_ms, ttl_ms)
    }
    fn renew_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.renew_lease(scope, run_id, lease, now_ms, ttl_ms)
    }
    fn release_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, ()> {
        self.inner.release_lease(scope, run_id, lease, now_ms)
    }
    fn commit<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            let planned = input
                .events
                .iter()
                .any(|event| matches!(event.payload, RunEventPayload::ToolPlanned { .. }));
            if self.boundary == "before-plan" && planned {
                std::process::exit(75);
            }
            let rewritten = input
                .events
                .iter()
                .any(|event| matches!(event.payload, RunEventPayload::ContextRewritten { .. }));
            if self.boundary == "before-context" && rewritten {
                std::process::exit(76);
            }
            let stop = match self.boundary.as_str() {
                "context" => rewritten,
                "plan" => planned,
                "bound" => input.snapshot.tool_ledger.iter().any(|entry| {
                    entry.call.tool_name.as_str() == "target"
                        && entry.call.bound_input_ref.is_some()
                        && matches!(entry.state, ToolCallState::Planned {})
                }),
                "settled" => input.snapshot.tool_ledger.iter().any(|entry| {
                    entry.call.tool_name.as_str() == "target"
                        && matches!(entry.state, ToolCallState::Settled { .. })
                }),
                "terminal" => input.snapshot.status.is_terminal(),
                _ => false,
            };
            let result = self.inner.commit(scope, run_id, input).await?;
            if stop {
                std::process::exit(if self.boundary == "context" { 76 } else { 75 });
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
        self.inner.read_events(scope, run_id, after_seq, limit)
    }
    fn read_record<'a>(
        &'a self,
        scope: &'a Scope,
        reference: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        self.inner.read_record(scope, reference)
    }
    fn record_hook_observation<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        report: HookObservation,
    ) -> PortFuture<'a, ()> {
        self.inner.record_hook_observation(scope, run_id, report)
    }
    fn read_hook_observations<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
    ) -> PortFuture<'a, Vec<HookObservation>> {
        self.inner.read_hook_observations(scope, run_id)
    }
}

impl wickle::ExecutionTransactions for CrashStore {
    fn read_execution<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
    ) -> wickle::PortFuture<'a, wickle::ExecutionHistory> {
        self.inner.read_execution(scope, run_id)
    }
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
        command: wickle::ControlCommand,
    ) -> wickle::PortFuture<'a, wickle::ControlReceipt> {
        self.inner.submit_control_command(scope, run_id, command)
    }
    fn begin_segment<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        request: wickle::BeginSegmentRequest,
    ) -> wickle::PortFuture<'a, wickle::BeginSegmentResult> {
        self.inner.begin_segment(scope, request)
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
        // The request contract exists before its purpose-specific routing integration.
        // Never silently accept an output cap that this driver cannot yet enforce.
        if request.max_output_tokens.is_some() {
            return Err(fail(
                ErrorCode::CapabilityUnsupported,
                "agent.request_output_cap",
            ));
        }
        let bindings = &self.inner.bindings;
        if serde_json::to_vec(&request)
            .map_err(|_| fail(ErrorCode::InvalidJson, "agent.request"))?
            .len()
            > bindings.settings.max_request_bytes
        {
            return Err(fail(ErrorCode::InvalidContract, "agent.request_size"));
        }
        if request
            .input
            .iter()
            .any(|content| !matches!(content, InputContent::Text { .. }))
        {
            return Err(fail(ErrorCode::CapabilityUnsupported, "agent.request"));
        }
        self.inner
            .verification
            .plan(&self.inner.profile, request.output_contract.as_ref())?;
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
            let completed = error.is_none() && !agent.keep_local(&driver_local);
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
        self.inner.context.validate_router(&routing)?;
        let profile = ProfileValidator::new(bindings.profile_resolver.as_ref())
            .validate(&self.inner.profile, &bindings.scope)
            .await?;
        let assembly = if let Some(runtime) = &bindings.components {
            let resolve_context = ComponentResolveContext {
                scope: bindings.scope.clone(),
                session_id: request.session_id.clone(),
                principal_ref: context.data.principal_ref.clone(),
                capability_grant_ref: context.data.capability_grant_ref.clone(),
                system_inputs: bindings.system_inputs.clone(),
                cancellation: context.cancellation.child_token(),
                deadline: tokio::time::Instant::now()
                    + Duration::from_millis(bindings.settings.start_timeout_ms),
            };
            let resolved = runtime.resolve(&profile, &resolve_context).await?;
            resolve_context.cancellation.cancel();
            if resolved.scope() != &bindings.scope
                || resolved.session_id() != &request.session_id
                || resolved.profile_resolution_digest() != profile.resolution_digest()
            {
                return Err(fail(ErrorCode::InvalidSnapshot, "agent.assembly"));
            }
            Some(resolved)
        } else {
            None
        };
        let tool_bindings = if let Some(assembly) = &assembly {
            ToolRegistry::metadata(bindings.scope.clone(), assembly.tools().to_vec())?
                .prompt_bindings(profile.profile())?
        } else {
            bindings
                .tools
                .as_ref()
                .map(|tools| tools.prompt_bindings(profile.profile()))
                .transpose()?
                .unwrap_or_default()
        };
        let skill_plan = if profile.profile().skills.is_empty() {
            None
        } else {
            Some(
                bindings
                    .skills
                    .as_ref()
                    .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "agent.skills"))?
                    .plan(&profile, &tool_bindings, bindings.profile_resolver.as_ref())
                    .await?,
            )
        };
        let skill_listings = skill_plan
            .as_ref()
            .map(SkillPlan::listings)
            .unwrap_or_default();
        let skill_record = skill_plan
            .as_ref()
            .map(|plan| {
                Ok::<_, ContractError>(ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(plan)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.skill_plan"))?,
                ))
            })
            .transpose()?;
        let session = match bindings
            .state
            .load_session(&bindings.scope, &request.session_id)
            .await
        {
            Ok(session) => Some(session),
            Err(error) if error.code == ErrorCode::StateNotFound => None,
            Err(error) => return Err(error),
        };
        let context_plan = self
            .inner
            .context
            .plan(profile.profile(), &bindings.scope)?;
        let context_revision_ref = session
            .as_ref()
            .and_then(|session| session.context_revision_ref.clone());
        if let Some(reference) = &context_revision_ref {
            self.inner
                .context
                .validate_session_plan(
                    reference,
                    &request.session_id,
                    &profile,
                    &context_plan,
                    bindings.state.as_ref(),
                )
                .await?;
        }
        let verification_plan = self
            .inner
            .verification
            .plan(profile.profile(), request.output_contract.as_ref())?;
        let verification_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(&verification_plan)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.verification_plan"))?,
        );
        let context_record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(&context_plan)
                .map_err(|_| fail(ErrorCode::InvalidJson, "agent.context_plan"))?,
        );
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
                skill_listings.clone(),
            )?;
            let record = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(&prompt)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "agent.prompt"))?,
            );
            (prompt, record, 1)
        };
        if prompt.skills() != skill_listings
            || prompt.tools().len() != tool_bindings.len()
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
        let hook_plan = if let Some(assembly) = &assembly {
            Some(
                HookRegistry::metadata(bindings.scope.clone(), assembly.hooks().to_vec())?
                    .plan(profile.profile())?,
            )
        } else {
            bindings
                .hooks
                .as_ref()
                .map(|hooks| hooks.plan(profile.profile()))
                .transpose()?
        };
        let hook_record = hook_plan
            .as_ref()
            .map(|plan| {
                Ok::<_, ContractError>(ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(plan)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.hooks"))?,
                ))
            })
            .transpose()?;
        let assembly_record = assembly
            .as_ref()
            .map(|assembly| {
                Ok::<_, ContractError>(ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(assembly)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.assembly"))?,
                ))
            })
            .transpose()?;
        let source_plan = if let Some(assembly) = &assembly {
            if assembly.sources().is_empty() {
                None
            } else {
                let estimator = bindings.context_token_estimator.as_ref().ok_or_else(|| {
                    fail(ErrorCode::InvalidConfiguration, "agent.source_estimator")
                })?;
                Some(
                    ContextSourceRegistry::metadata(
                        bindings.scope.clone(),
                        assembly.sources().to_vec(),
                    )?
                    .plan(profile.profile(), &estimator.version())?,
                )
            }
        } else {
            bindings
                .context_sources
                .as_ref()
                .map(|sources| sources.plan(profile.profile()))
                .transpose()?
        };
        let source_record = source_plan
            .as_ref()
            .map(|plan| {
                Ok::<_, ContractError>(ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(plan)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "agent.sources"))?,
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
            assembly_ref: assembly_record
                .as_ref()
                .map(|record| record.reference().clone()),
            routing_snapshot_ref: Some(routing_record.reference().clone()),
            context_batches: vec![],
            source_states: vec![],
            source_plan_ref: source_record
                .as_ref()
                .map(|record| record.reference().clone()),
            skill_plan_ref: skill_record
                .as_ref()
                .map(|record| record.reference().clone()),
            context_plan_ref: Some(context_record.reference().clone()),
            context_revision_ref,
            context_decisions: vec![],
            verification_plan_ref: Some(verification_record.reference().clone()),
            candidate_ref: None,
            verification_records: vec![],
            revision: 0,
            resume_receipts: vec![],
            recovery_receipts: vec![],
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
                execution_principal_ref: context.data.principal_ref.clone(),
                submitted: None,
                snapshot,
                prompt_snapshot: prompt_record.reference().clone(),
                require_durable: bindings.settings.require_durable,
                messages: vec![message],
                events: vec![event],
                records: [
                    vec![request_record, prompt_record, inputs_record, routing_record],
                    hook_record.into_iter().collect(),
                    assembly_record.into_iter().collect(),
                    source_record.into_iter().collect(),
                    skill_record.into_iter().collect(),
                    vec![context_record, verification_record],
                ]
                .concat(),
            },
            prompt,
        ))
    }
}
```

## `crates/wickle/src/agent/persistence.rs`

```rust
use super::*;

/// Metadata from the latest successfully observed checkpoint, not a terminal result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistenceFailure {
    /// Run whose authoritative storage became unavailable.
    pub run_id: Id,
    /// Latest revision confirmed by a successful store operation in this Agent.
    pub last_confirmed_revision: u64,
    /// Dispatched effects whose results were not confirmed at that revision.
    pub unconfirmed_effects: Vec<UnconfirmedToolEffect>,
}
/// An uncertain call identity; this does not contain its system inputs or output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnconfirmedToolEffect {
    /// Stable planned call.
    pub call_id: Id,
    /// Charged physical execution attempt.
    pub attempt_id: Id,
    /// Original external effect key.
    pub idempotency_key: Id,
    /// Protected frozen argument record, when present.
    pub bound_input_ref: Option<RecordRef>,
}
#[derive(Default)]
pub(super) struct ObservedState(Mutex<BTreeMap<Id, PersistenceFailure>>);
impl ObservedState {
    fn observe(&self, snapshot: &RunSnapshot) {
        let notice = PersistenceFailure {
            run_id: snapshot.run_id.clone(),
            last_confirmed_revision: snapshot.revision,
            unconfirmed_effects: snapshot
                .tool_ledger
                .iter()
                .filter_map(|entry| {
                    let (attempt_id, idempotency_key) = match &entry.state {
                        ToolCallState::Dispatching {
                            attempt_id,
                            idempotency_key,
                        }
                        | ToolCallState::Unknown {
                            attempt_id,
                            idempotency_key,
                        } => (attempt_id, idempotency_key),
                        _ => return None,
                    };
                    Some(UnconfirmedToolEffect {
                        call_id: entry.call.call_id.clone(),
                        attempt_id: attempt_id.clone(),
                        idempotency_key: idempotency_key.clone(),
                        bound_input_ref: entry.call.bound_input_ref.clone(),
                    })
                })
                .collect(),
        };
        if let Ok(mut saved) = self.0.lock() {
            if saved
                .get(&snapshot.run_id)
                .is_none_or(|previous| previous.last_confirmed_revision <= snapshot.revision)
            {
                saved.insert(snapshot.run_id.clone(), notice);
            }
        }
    }
    pub(super) fn attach(&self, run_id: &Id, mut error: ContractError) -> ContractError {
        if error.code == ErrorCode::PersistenceUnavailable {
            error.persistence = self
                .0
                .lock()
                .ok()
                .and_then(|saved| saved.get(run_id).cloned())
                .map(Box::new);
        }
        error
    }
}
pub(super) struct ObservedStore {
    pub inner: Arc<dyn StateStore>,
    pub observed: Arc<ObservedState>,
}
impl StateStore for ObservedStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
        request_id: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        Box::pin(async move {
            let result = self
                .inner
                .find_request(scope, session_id, request_id)
                .await?;
            if let Some(saved) = &result {
                self.observed.observe(&saved.snapshot);
            }
            Ok(result)
        })
    }
    fn admit<'a>(
        &'a self,
        scope: &'a Scope,
        input: AdmissionInput,
    ) -> PortFuture<'a, AdmissionResult> {
        Box::pin(async move {
            let result = self.inner.admit(scope, input).await?;
            self.observed.observe(&result.state.snapshot);
            Ok(result)
        })
    }
    fn load<'a>(&'a self, scope: &'a Scope, run_id: &'a Id) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            let result = self.inner.load(scope, run_id).await?;
            self.observed.observe(&result.snapshot);
            Ok(result)
        })
    }
    fn load_session<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
    ) -> PortFuture<'a, SessionSnapshot> {
        self.inner.load_session(scope, session_id)
    }
    fn check_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.check_lease(scope, run_id, lease, now_ms)
    }
    fn acquire_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        owner: &'a Id,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner
            .acquire_lease(scope, run_id, owner, now_ms, ttl_ms)
    }
    fn renew_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
        ttl_ms: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.renew_lease(scope, run_id, lease, now_ms, ttl_ms)
    }
    fn release_lease<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        lease: &'a RunLease,
        now_ms: i64,
    ) -> PortFuture<'a, ()> {
        self.inner.release_lease(scope, run_id, lease, now_ms)
    }
    fn commit<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            let result = self.inner.commit(scope, run_id, input).await?;
            self.observed.observe(&result.snapshot);
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
        self.inner.read_events(scope, run_id, after_seq, limit)
    }
    fn read_record<'a>(
        &'a self,
        scope: &'a Scope,
        reference: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        self.inner.read_record(scope, reference)
    }
    fn record_hook_observation<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        report: HookObservation,
    ) -> PortFuture<'a, ()> {
        self.inner.record_hook_observation(scope, run_id, report)
    }
    fn read_hook_observations<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
    ) -> PortFuture<'a, Vec<HookObservation>> {
        self.inner.read_hook_observations(scope, run_id)
    }
}

impl crate::ExecutionTransactions for ObservedStore {
    fn read_execution<'a>(
        &'a self,
        scope: &'a crate::Scope,
        run_id: &'a crate::Id,
    ) -> crate::PortFuture<'a, crate::ExecutionHistory> {
        self.inner.read_execution(scope, run_id)
    }
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a crate::Scope,
        run_id: &'a crate::Id,
        command: crate::ControlCommand,
    ) -> crate::PortFuture<'a, crate::ControlReceipt> {
        self.inner.submit_control_command(scope, run_id, command)
    }
    fn begin_segment<'a>(
        &'a self,
        scope: &'a crate::Scope,
        request: crate::BeginSegmentRequest,
    ) -> crate::PortFuture<'a, crate::BeginSegmentResult> {
        Box::pin(async move {
            let result = self.inner.begin_segment(scope, request).await?;
            self.observed.observe(&result.state.snapshot);
            Ok(result)
        })
    }
}
```

## `crates/wickle/src/execution_contracts.rs`

```rust
//! Persistable execution contracts. Transaction implementations and driver
//! integration use these records; declarations alone do not enable recovery.
use crate::{
    CanonicalizationVersion, ContractError, ErrorCode, Id, JsonDigest, JsonObject, JsonTextLimits,
    PortFuture, RecordRef, ResumeCommand, RunLease, RunOutcome, Scope, StoredRun, VersionedRef,
    canonicalize_json_text, versioned_digest_json,
};
use serde::{Deserialize, Serialize};
use std::{fmt, num::NonZeroU64};

/// Version of protected segment and prepared-step records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionRecordVersion {
    /// First execution-record contract; stored independently of legacy Run checkpoints.
    #[serde(rename = "wickle.execution-record.v1")]
    V1,
}

/// Original submitted data, kept separate from resolved defaults and runtime handles.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestSnapshot {
    schema_version: RequestSnapshotVersion,
    canonicalization: CanonicalizationVersion,
    /// Original submitted profile identity, not a newly resolved definition.
    pub profile_ref: VersionedRef,
    request_json: String,
    system_inputs_json: String,
    system_inputs_provided: bool,
    digest: JsonDigest,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum RequestSnapshotVersion {
    #[serde(rename = "wickle.request-snapshot.v1")]
    V1,
}
impl fmt::Debug for RequestSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestSnapshot")
            .field("canonicalization", &self.canonicalization)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}
impl RequestSnapshot {
    /// Capture validated caller JSON without resolving a catalog or binding.
    /// Start's absent system inputs become an empty object. Empty/omitted model options are normalized identically; other submitted
    /// fields retain their explicit presence. Request schema/authorization are the
    /// admission boundary's responsibility; capture alone does not admit a Run.
    pub fn capture(
        profile_ref: VersionedRef,
        request_json: &str,
        system_inputs_json: Option<&str>,
        limits: JsonTextLimits,
    ) -> Result<Self, ContractError> {
        let system_inputs_provided = system_inputs_json.is_some();
        let request = canonicalize_json_text(request_json, limits)?;
        let system = canonicalize_json_text(system_inputs_json.unwrap_or("{}"), limits)?;
        if request.first() != Some(&b'{') || system.first() != Some(&b'{') {
            return Err(ContractError::new(
                ErrorCode::InvalidContract,
                "request_snapshot.object",
            ));
        }
        let request_json = String::from_utf8(request)
            .map_err(|_| ContractError::new(ErrorCode::InvalidJson, "request_snapshot"))?;
        let system_inputs_json = String::from_utf8(system)
            .map_err(|_| ContractError::new(ErrorCode::InvalidJson, "request_snapshot"))?;
        let canonicalization = CanonicalizationVersion::WickleCanonicalJsonV1;
        let digest = Self::compute(
            &profile_ref,
            &request_json,
            &system_inputs_json,
            canonicalization,
            limits,
        )?;
        Ok(Self {
            schema_version: RequestSnapshotVersion::V1,
            canonicalization,
            profile_ref,
            request_json,
            system_inputs_json,
            system_inputs_provided,
            digest,
        })
    }
    fn compute(
        profile: &VersionedRef,
        request: &str,
        system: &str,
        version: CanonicalizationVersion,
        limits: JsonTextLimits,
    ) -> Result<JsonDigest, ContractError> {
        // Omitted and empty model option maps are the same submitted override.
        // RawValue keeps nested numeric tokens intact while removing only that key.
        let mut fields: std::collections::BTreeMap<String, Box<serde_json::value::RawValue>> =
            serde_json::from_str(request).map_err(|_| {
                ContractError::new(ErrorCode::InvalidJson, "request_snapshot.request")
            })?;
        if let Some(options) = fields.get("model_options") {
            if !options.get().starts_with('{') {
                return Err(ContractError::new(
                    ErrorCode::InvalidContract,
                    "request_snapshot.model_options",
                ));
            }
            if options.get() == "{}" {
                fields.remove("model_options");
            }
        }
        let request = serde_json::to_string(&fields)
            .map_err(|_| ContractError::new(ErrorCode::InvalidJson, "request_snapshot.request"))?;
        let profile = serde_json::to_string(profile)
            .map_err(|_| ContractError::new(ErrorCode::InvalidJson, "request_snapshot.profile"))?;
        let envelope = format!(
            "{{\"profile_ref\":{profile},\"request\":{request},\"system_inputs\":{system}}}"
        );
        versioned_digest_json(&envelope, version, limits)
    }
    /// Validate a stored record before comparison. Never trust a supplied digest.
    pub fn validate(&self, limits: JsonTextLimits) -> Result<(), ContractError> {
        let request = canonicalize_json_text(&self.request_json, limits)?;
        let system = canonicalize_json_text(&self.system_inputs_json, limits)?;
        if request.first() != Some(&b'{')
            || system.first() != Some(&b'{')
            || (!self.system_inputs_provided && system != b"{}")
            || Self::compute(
                &self.profile_ref,
                &self.request_json,
                &self.system_inputs_json,
                self.canonicalization,
                limits,
            )? != self.digest
        {
            return Err(ContractError::new(
                ErrorCode::InvalidSnapshot,
                "request_snapshot",
            ));
        }
        Ok(())
    }
    /// Version that must be used when comparing a resubmission.
    pub fn canonicalization(&self) -> CanonicalizationVersion {
        self.canonicalization
    }
    /// Digest of the submitted profile, request and system input envelope.
    pub fn digest(&self) -> &JsonDigest {
        &self.digest
    }
    /// Privileged access to the submitted request JSON, retaining number lexemes.
    pub fn request_json(&self) -> &str {
        &self.request_json
    }
    /// Whether Start explicitly supplied system inputs. Omission still hashes as {}.
    pub fn system_inputs_provided(&self) -> bool {
        self.system_inputs_provided
    }
    /// Privileged access to protected system inputs. Never include in model context.
    pub fn system_inputs_json(&self) -> &str {
        &self.system_inputs_json
    }
}

/// Application-defined state, distinct from core execution status.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppState {
    /// Registered Host schema namespace.
    pub namespace: Id,
    /// Host-defined business status.
    pub status: Id,
    /// Validated against the registered Host schema before persistence.
    pub metadata: JsonObject,
}
impl fmt::Debug for AppState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppState")
            .field("namespace", &self.namespace)
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}
/// Why a segment stopped; protected causes cannot be overridden by app policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterruptionCause {
    /// Host is shutting down or handing ownership off.
    HostShutdown,
    /// Segment stop without a durable user cancellation.
    SegmentStopped,
    /// Explicit user cancellation.
    UserCancel,
    /// Run budget or deadline exhausted.
    BudgetExhausted,
    /// Ownership/fencing no longer valid.
    OwnershipLost,
    /// Snapshot cannot safely be recovered.
    RecoveryUnavailable,
}
/// Persisted evidence of a stopped execution segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InterruptionRecord {
    /// Segment whose execution stopped.
    pub segment_id: Id,
    /// Classified cause, not inferred from observer disconnection.
    pub cause: InterruptionCause,
    /// Last confirmed checkpoint revision.
    pub checkpoint_revision: u64,
    /// Whether a valid saved recovery path exists.
    pub recoverable: bool,
    /// Existing uncertain effects; policy must not remove these.
    pub unresolved_effects: Vec<RecordRef>,
}
/// Outcome for a particular segment. Resuming creates a different record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SegmentOutcome {
    /// Existing waiting or terminal outcome.
    Settled {
        /// Saved authoritative outcome.
        outcome: Box<RunOutcome>,
    },
    /// Recoverable stop, not a terminal Run outcome.
    Interrupted {
        /// Stop/recovery evidence.
        interruption: InterruptionRecord,
    },
}
/// Immutable identity and result of one accepted execution interval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSegment {
    /// Explicit protected-record schema version.
    pub schema_version: ExecutionRecordVersion,
    /// Original execution principal; a reviewer/control submitter cannot replace it.
    pub execution_principal_ref: Id,
    /// Run that owns this segment.
    pub run_id: Id,
    /// Never reused when the Run is resumed.
    pub segment_id: Id,
    /// Revision at acceptance.
    pub accepted_revision: u64,
    /// Present after waiting, interruption or terminal settlement.
    pub outcome: Option<SegmentOutcome>,
    /// App state snapshot at settlement; never defines core status.
    pub app_state: Option<AppState>,
}
/// Frozen prepared-model inputs. References belong to the same authorized scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedStepRecord {
    /// Explicit protected-record schema version.
    pub schema_version: ExecutionRecordVersion,
    /// Agent, verification or compaction invocation budget/policy purpose.
    pub purpose: crate::ModelPurpose,
    /// Protected compiled-provider contract records for this exact projection.
    pub compiled_tools: Vec<RecordRef>,
    /// Logical model step identity.
    pub model_step_id: Id,
    /// Changes when the model input changes, not on identical transport retries.
    pub projection_revision: NonZeroU64,
    /// Pinned route, effective options and their provenance.
    pub model_configuration: RecordRef,
    /// Pinned canonical tool set and compiled provider contracts.
    pub tool_set: RecordRef,
    /// Persisted assembled prompt/input projection.
    pub context_projection: RecordRef,
    /// Compiler and assembler contract identity.
    pub assembler: VersionedRef,
}
/// A durable control request. Submission is not a completed state transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlAction {
    /// Permanently cancel a Run, preserving known/unknown effects.
    Cancel {
        /// Auditable reason identifier.
        reason: Id,
    },
    /// Stop the active segment without declaring Run success.
    Stop {
        /// Why the Host stopped execution.
        cause: InterruptionCause,
    },
    /// Explicitly process a Run deadline that has elapsed.
    Expire,
}
/// Deduplicated control command, authenticated by the Host before submission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlCommand {
    /// Stable command identity within a Run.
    pub command_id: Id,
    /// Requesting principal, distinct from the original execution principal.
    pub principal_ref: Id,
    /// Authorized action.
    pub action: ControlAction,
}
/// Why a transaction creates or claims a segment.
#[derive(Debug, Clone)]
pub enum SegmentStart {
    /// Claim the segment already identified by admission.
    Initial,
    /// Consume a matching approval/input/recovery command.
    Resume(ResumeCommand),
    /// Consume an already persisted control command, without model/tool execution.
    Control(Id),
}
/// Inputs for atomic command consumption, lease acquisition and segment creation.
#[derive(Clone)]
pub struct BeginSegmentRequest {
    /// Proposed validated checkpoint/events for resume or control. Initial claim has None.
    pub transition: Option<SegmentTransition>,
    /// Owning Run.
    pub run_id: Id,
    /// CAS revision before this transaction.
    pub expected_revision: u64,
    /// Proposed new segment ID (initial claims use admission's ID).
    pub segment_id: Id,
    /// New execution owner.
    pub owner: Id,
    /// Trusted clock value.
    pub now_ms: i64,
    /// Bounded positive ownership TTL.
    pub lease_ttl_ms: NonZeroU64,
    /// Command or initial admission claim.
    pub start: SegmentStart,
}
/// Result of atomic segment acceptance; replay must not launch another driver.
#[derive(Clone)]
pub struct BeginSegmentResult {
    /// Saved execution state.
    pub state: StoredRun,
    /// Accepted or previously accepted segment.
    pub segment: ExecutionSegment,
    /// Only a new owner receives a lease. Replays have None.
    pub lease: Option<RunLease>,
}
/// Acknowledgment of durable command acceptance, not execution completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlReceipt {
    /// Owning Run.
    pub run_id: Id,
    /// Deduplicated command.
    pub command_id: Id,
    /// Segment that processed it, if already consumed.
    pub processed_segment_id: Option<Id>,
}
/// Atomic operations that storage implementations must supply before the new
/// segment driver can be enabled. No default read/then/write emulation is safe.
/// StateStore integration requires the same transaction as its snapshot/events.
pub trait ExecutionTransactions: Send + Sync {
    /// Read protected segment/command history without mutation; Host authorizes access.
    fn read_execution<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
    ) -> PortFuture<'a, ExecutionHistory>;

    /// Atomically deduplicate/consume the command, check CAS and allocate ownership.
    fn begin_segment<'a>(
        &'a self,
        scope: &'a Scope,
        request: BeginSegmentRequest,
    ) -> PortFuture<'a, BeginSegmentResult>;
    /// Atomically persist a command ID and payload; conflicting reuse is rejected.
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        command: ControlCommand,
    ) -> PortFuture<'a, ControlReceipt>;
}

/// Allowed callback proposals. None may declare success or overwrite effect evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterruptionAction {
    /// Use the core's cause-specific default.
    UseDefault,
    /// Save a recoverable interrupted segment when the core permits it.
    Pause,
    /// Cancel when the cause permits this transition.
    Cancel,
    /// Fail when the cause permits this transition.
    Fail,
}
/// Limited immutable data exposed to the application interruption policy.
#[derive(Debug, Clone)]
pub struct InterruptionInfo {
    /// Resource scope; this does not grant authorization.
    pub scope: Scope,
    /// Stopped Run identity.
    pub run_id: Id,
    /// Fixed interruption evidence.
    pub interruption: InterruptionRecord,
    /// Current app state without mutable access to the Run.
    pub app_state: Option<AppState>,
}
/// Policy result; core validation and persistence remain mandatory.
#[derive(Debug, Clone)]
pub struct InterruptionDecision {
    /// Suggested transition.
    pub action: InterruptionAction,
    /// Optional Host-schema-validated business state.
    pub app_state: Option<AppState>,
}
/// A versioned application policy with no state-store or tool execution handle.
pub trait InterruptionPolicy: Send + Sync {
    /// Immutable identity pinned at admission.
    fn identity(&self) -> VersionedRef;
    /// Cooperatively compute a proposal. The driver owns timeout and fallback.
    fn decide<'a>(&'a self, info: &'a InterruptionInfo) -> PortFuture<'a, InterruptionDecision>;
}

impl ExecutionSegment {
    /// Check structural settlement invariants before persistence; authorization,
    /// Host app-state schema and atomic ledger transitions remain store/driver checks.
    pub fn validate(&self) -> Result<(), ContractError> {
        let invalid = || ContractError::new(ErrorCode::InvalidSnapshot, "execution_segment");
        match &self.outcome {
            Some(SegmentOutcome::Settled { outcome }) => {
                outcome.validate()?;
                if outcome.checkpoint_revision < self.accepted_revision {
                    return Err(invalid());
                }
            }
            Some(SegmentOutcome::Interrupted { interruption })
                if interruption.segment_id != self.segment_id
                    || interruption.checkpoint_revision < self.accepted_revision
                    || !interruption.recoverable
                    || !matches!(
                        interruption.cause,
                        InterruptionCause::HostShutdown | InterruptionCause::SegmentStopped
                    ) =>
            {
                return Err(invalid());
            }
            Some(SegmentOutcome::Interrupted { .. }) => {}
            None => {}
        }
        Ok(())
    }
}

/// A proposed checkpoint delta; the transaction supplies its own validated lease.
#[derive(Clone)]
pub struct SegmentTransition {
    /// Next checkpoint at expected_revision + 1.
    pub snapshot: crate::RunSnapshot,
    /// New transcript messages.
    pub messages: Vec<crate::Message>,
    /// Matching durable events.
    pub events: Vec<crate::RunEvent>,
    /// Immutable records referenced by the new checkpoint/events.
    pub records: Vec<crate::ProtectedRecord>,
}
/// Persisted control command and its optional processing result.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredControlCommand {
    /// Original authenticated command payload.
    pub command: ControlCommand,
    /// Segment that consumed it; absent means pending, not failed.
    pub processed_segment_id: Option<Id>,
}
/// Command deduplication evidence bound to an accepted segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedSegmentCommand {
    /// Stable command identity.
    pub command_id: Id,
    /// Original normalized command payload digest.
    pub payload_digest: JsonDigest,
    /// Segment created by this command.
    pub segment_id: Id,
}
/// Protected execution history stored atomically with a Run's checkpoint/events.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionHistory {
    /// Owning Run.
    pub run_id: Id,
    /// Authenticated original execution actor.
    pub execution_principal_ref: Id,
    /// Submitted request evidence, separate from effective configuration.
    pub submitted: Option<RequestSnapshot>,
    /// Initial segment has been claimed at least once. Retrying a claim is not recovery.
    pub initial_claimed: bool,
    /// Ordered immutable past segments and the current segment.
    pub segments: Vec<ExecutionSegment>,
    /// Accepted resume/control command identities.
    pub accepted_commands: Vec<AcceptedSegmentCommand>,
    /// Pending and processed durable controls.
    pub controls: Vec<StoredControlCommand>,
}
impl fmt::Debug for ExecutionHistory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionHistory")
            .field("run_id", &self.run_id)
            .field("segment_count", &self.segments.len())
            .finish_non_exhaustive()
    }
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
mod artifacts;
mod budget;
mod canonical;
mod execution_contracts;
pub use execution_contracts::{
    AcceptedSegmentCommand, AppState, BeginSegmentRequest, BeginSegmentResult, ControlAction,
    ControlCommand, ControlReceipt, ExecutionHistory, ExecutionRecordVersion, ExecutionSegment,
    ExecutionTransactions, InterruptionAction, InterruptionCause, InterruptionDecision,
    InterruptionInfo, InterruptionPolicy, InterruptionRecord, PreparedStepRecord, RequestSnapshot,
    SegmentOutcome, SegmentStart, SegmentTransition, StoredControlCommand,
};
mod clock;
mod component_runtime;
mod context;
mod context_projection;
mod context_source;
mod context_strategy;
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
pub use canonical::{
    CanonicalizationVersion, JsonTextLimits, canonicalize_json_text, versioned_digest_json,
};
mod skills;
mod state;
mod tool_execution;
mod tool_schema;
mod views;

pub use agent::{
    Agent, AgentBindings, AgentSettings, CancelReceipt, ComponentReleaseView, HookObservationView,
    ModelTokenEstimator, PersistenceFailure, RunHandle, UnconfirmedToolEffect, create_agent,
};
pub use artifacts::{
    ArtifactCallContext, ArtifactData, ArtifactInput, ArtifactLimits, ArtifactMetadata,
    ArtifactPreview, ArtifactRuntime, ArtifactStore, MemoryArtifactStore,
};
pub use budget::{AttemptReservation, ReservationKind, RunBudget, RunTiming};
pub use clock::{Clock, ClockReading, IdSource, RandomIdSource, SystemClock};
pub use component_runtime::{
    AdapterBindingState, AdapterCloseContext, AdapterDefinition, AdapterExportDefinition,
    AdapterExportInstance, AdapterFactory, AdapterInitContext, AdapterInstance, BoundCapabilities,
    ComponentBindContext, ComponentBindPurpose, ComponentRelease, ComponentReleaseContext,
    ComponentReleaseFailure, ComponentReleaseReport, ComponentResolveContext, ComponentRuntime,
    ResolvedAdapterBinding, ResolvedAssembly, ResolvedConnection, ResolvedHookBinding,
    ResolvedToolBinding,
};
pub use context_projection::{
    CONTEXT_ASSEMBLER_VERSION, ContextAssembler, ContextItem, ContextLifetime, ContextOrigin,
    ContextPriority, ContextProjection, InstructionAssetContent, PinnedPromptTool, ProjectionInput,
    ProjectionLimits, PromptSnapshot, PromptToolBinding, ScopedOpaque, SkillManifest,
};
pub use context_source::{
    ContextBatch, ContextCallContext, ContextRequest, ContextResult, ContextSource,
    ContextSourceDefinition, ContextSourcePlan, ContextSourceRegistration, ContextSourceRegistry,
    ContextSourceRuntime, ContextSourceUsage, ContextTokenEstimator, ContextUseRequest,
    PlannedContextSource, ResolvedSourceBinding,
};
pub use context_strategy::{
    BoundedContextStrategy, CompactionRequest, ContextCompactor, ContextDecision, ContextPlan,
    ContextPreview, ContextRevision, ContextRewriteLimits, ContextRuntime, ContextSegment,
    ContextSelectionInput, ContextStrategy, ContextStrategyContext, ContextStrategyDefinition,
    HostContextCompactor, ModelCompactorConfig,
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
pub use skills::{
    LoadedSkill, PlannedSkill, SkillBindings, SkillCallContext, SkillDefinition, SkillLimits,
    SkillPlan, SkillResolver, SkillRuntime,
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

mod verification;
pub use verification::{
    OutputSchemaDefinition, VerificationCandidate, VerificationDecision, VerificationInput,
    VerificationLimits, VerificationModel, VerificationModelRequest, VerificationPlan,
    VerificationRuntime, Verifier, VerifierContext, VerifierDefinition,
};

pub use verification::SchemaVerifier;

mod future;

pub use tool_execution::ToolReconciliation;

mod recovery;
pub use recovery::RecoveryReceipt;
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
mod context_state;
mod execution;
mod hook_state;
mod reconciliation_state;
mod recovery_state;
mod skill_state;
mod source_state;
mod verification_state;
pub use checkpoint::{STATE_STORE_CHECKPOINT_VERSION, StateStoreCheckpoint};
use hook_state::{validate_hook_observation, validate_hook_snapshot, validate_hook_transition};
use source_state::{validate_source_snapshot, validate_source_transition};

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
    /// Authenticated original execution principal, not the latest reviewer.
    pub execution_principal_ref: Id,
    /// Original submitted inputs when supplied by the versioned admission path.
    pub submitted: Option<crate::RequestSnapshot>,
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
pub trait StateStore: crate::ExecutionTransactions + Send + Sync {
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
    executions: BTreeMap<Id, crate::ExecutionHistory>,
    legacy_runs: BTreeSet<Id>,
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
            if state.runs.iter().any(|(id, run)| {
                !run.snapshot.status.is_terminal() && !state.executions.contains_key(id)
            }) {
                return Err(error(
                    ErrorCode::CapabilityUnsupported,
                    "execution.legacy_drain_required",
                ));
            }
            input.snapshot.validate()?;
            if input.snapshot.revision != 0
                || input.snapshot.status != RunStatus::Running
                || input.snapshot.phase != RunPhase::Admission
                || !input.snapshot.model_ledger.is_empty()
                || !input.snapshot.tool_ledger.is_empty()
                || !input.snapshot.reservations.is_empty()
                || !input.snapshot.resume_receipts.is_empty()
                || !input.snapshot.recovery_receipts.is_empty()
                || !input.snapshot.hook_applications.is_empty()
                || !input.snapshot.context_batches.is_empty()
                || !input.snapshot.source_states.is_empty()
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
            if input.snapshot.context_revision_ref.as_ref()
                != previous_session
                    .and_then(|session| session.snapshot.context_revision_ref.as_ref())
                || !input.snapshot.context_decisions.is_empty()
            {
                return Err(error(ErrorCode::InvalidSnapshot, "context.admission"));
            }
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
                context_revision_ref: input.snapshot.context_revision_ref.clone(),
                active_run_id: Some(input.snapshot.run_id.clone()),
            };
            let result = StoredRun {
                snapshot: input.snapshot.clone(),
                session: session.clone(),
                messages: messages.clone(),
            };
            let execution = execution::initial_history(
                &input.snapshot,
                &input.execution_principal_ref,
                input.submitted.as_ref(),
            )?;
            // All fallible checks precede these mutations.
            let state = scopes.entry(scope_key(scope)).or_default();
            state
                .executions
                .insert(input.snapshot.run_id.clone(), execution);
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
        Box::pin(async move { self.acquire_lease_now(scope, run_id, owner, now_ms, ttl_ms) })
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
        Box::pin(async move { self.commit_now(scope, run_id, input, None) })
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
            if let Some(existing) = reports.iter().find(|existing| {
                existing.hook == report.hook
                    && existing.selection == report.selection
                    && existing.target == report.target
            }) {
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
    recovery_state::validate(state, additions, snapshot)?;
    validate_source_snapshot(state, additions, snapshot)?;
    skill_state::validate_skill_snapshot(state, additions, snapshot)?;
    context_state::validate_snapshot(state, additions, snapshot)?;
    verification_state::validate(state, additions, snapshot)?;
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
            // The saved step identifies the logical binding independently of the physical route.
            // Auxiliary stages may use their own purpose-specific rule.
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct SavedStep {
                schema_version: String,
                run_id: Id,
                input: crate::RoutedModelInput,
            }
            let key = (
                Id::new(format!(
                    "model-step-{}",
                    crate::canonical_digest(&serde_json::json!([
                        snapshot.run_id,
                        invocation.model_step_id
                    ]))
                ))?,
                1,
            );
            let value = additions
                .get(&key)
                .map(ProtectedRecord::value)
                .or_else(|| state.records.get(&key).map(|record| &record.value))
                .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "routing.step_input"))?;
            let step: SavedStep = serde_json::from_value(value.clone())
                .map_err(|_| error(ErrorCode::InvalidSnapshot, "routing.step_input"))?;
            if step.schema_version != "wickle.model-step.v1"
                || step.run_id != snapshot.run_id
                || step.input.model_step_id != invocation.model_step_id
                || step.input.routing.scope != snapshot.scope
                || step.input.routing.purpose != invocation.purpose
                || (invocation.purpose == crate::ModelPurpose::Agent
                    && step.input.routing.model_binding != snapshot.profile.profile().model_binding)
            {
                return Err(error(ErrorCode::InvalidSnapshot, "routing.step_identity"));
            }
            let rule = routing
                .policy()
                .rules
                .iter()
                .find(|rule| {
                    rule.model_binding == step.input.routing.model_binding
                        && rule.purpose == invocation.purpose
                })
                .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "routing.invocation"))?;
            if invocation.inspection_ref.is_none()
                || !(rule.primary == invocation.route.binding
                    || rule.fallbacks.contains(&invocation.route.binding))
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
                || rule.version_policy == crate::VersionPolicy::RequirePinned;
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
    if let Some(reference) = &snapshot.assembly_ref {
        let registry = crate::SystemInputRegistry::new(
            run_inputs
                .as_ref()
                .map(|inputs| inputs.definitions().values().cloned().collect())
                .unwrap_or_default(),
        )?;
        let value = record_value(state, additions, reference)?;
        let assembly = crate::ResolvedAssembly::restore(
            &serde_json::to_string(value)
                .map_err(|_| error(ErrorCode::InvalidSnapshot, "assembly"))?,
            &snapshot.profile,
            &registry,
            &reference.digest,
        )?;
        if assembly.session_id() != &snapshot.request.session_id {
            return Err(error(ErrorCode::InvalidSnapshot, "assembly.session"));
        }
        if let Some(session) = state.sessions.get(&snapshot.request.session_id) {
            let value = record_value(state, additions, &session.snapshot.prompt_snapshot)?;
            let prompt = crate::PromptSnapshot::restore(
                &serde_json::to_string(value)
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "assembly.prompt"))?,
                &session.snapshot.prompt_snapshot.digest,
                &snapshot.profile,
                &snapshot.scope,
            )?;
            if prompt.tools().len() != assembly.tools().len()
                || prompt
                    .tools()
                    .iter()
                    .zip(assembly.tools())
                    .any(|(pinned, binding)| {
                        pinned.selection != binding.selection
                            || &pinned.compiled_digest != binding.compiled.digest()
                            || pinned.model_tool != binding.compiled.to_model_tool()
                    })
            {
                return Err(error(ErrorCode::InvalidSnapshot, "assembly.prompt_tools"));
            }
        }
    }
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
    let reconciled = reconciliation_state::corrections(
        state,
        additions,
        snapshot,
        &events.iter().collect::<Vec<_>>(),
        &messages.iter().collect::<Vec<_>>(),
    )?;
    for message in messages {
        for content in &message.content {
            if let ContentBlock::ToolResultCorrection { result, .. } = content {
                if reconciled.contains(&message.message_id) {
                    continue;
                }
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
            RunEventPayload::RunRecovered {
                recovery_receipt_ref,
            } => {
                recovery_state::event(state, additions, snapshot, event, recovery_receipt_ref)?;
                recovery_receipt_ref
            }
            RunEventPayload::ToolReconciled { reconciliation_ref } => reconciliation_ref,
            RunEventPayload::ContextRewritten { revision_ref } => {
                let revision = context_state::revision(state, additions, revision_ref, snapshot)?;
                if snapshot.context_revision_ref.as_ref() != Some(revision_ref)
                    || revision.run_id != snapshot.run_id
                    || Some(&revision.model_step_id) != snapshot.model_step_id.as_ref()
                {
                    return Err(error(ErrorCode::InvalidEvent, "events.context"));
                }
                revision_ref
            }
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
                verification_state::event(state, additions, snapshot, verification_ref)?;
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
            if messages.iter().any(|message|reconciled.contains(&message.message_id)&&matches!(message.content.as_slice(),[ContentBlock::ToolResultCorrection{result,..}] if result.call_id==old.call.call_id)){continue;}
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
    let mut authorized_corrections =
        reconciliation_state::corrections(state, additions, snapshot, events, messages)?;
    for message in messages
        .iter()
        .filter(|message| authorized_corrections.contains(&message.message_id))
    {
        if let [ContentBlock::ToolResultCorrection { result, .. }] = message.content.as_slice() {
            corrected_calls.insert(result.call_id.clone());
        }
    }
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
    validate_source_transition(previous, next)?;
    if previous.skill_plan_ref != next.skill_plan_ref {
        return Err(error(ErrorCode::InvalidTransition, "skills.immutable_plan"));
    }
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
        || previous.assembly_ref != next.assembly_ref
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
                        ModelAttemptState::Completed {}
                            | ModelAttemptState::Failed { .. }
                            | ModelAttemptState::Interrupted { .. }
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

impl MemoryStateStore {
    fn acquire_lease_now(
        &self,
        scope: &Scope,
        run_id: &Id,
        owner: &Id,
        now_ms: i64,
        ttl_ms: u64,
    ) -> Result<RunLease, ContractError> {
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
    }
}

impl MemoryStateStore {
    fn commit_now(
        &self,
        scope: &Scope,
        run_id: &Id,
        input: CommitInput,
        segment_override: Option<&Id>,
    ) -> Result<StoredRun, ContractError> {
        check_scope(scope, &input.snapshot.scope)?;
        let mut scopes = self.lock()?;
        let state = namespace(&scopes, scope)?;
        let run = state.runs.get(run_id).ok_or_else(not_found)?;
        validate_lease(run, scope, run_id, &input.lease, input.now_ms)?;
        if run.snapshot.revision != input.expected_revision {
            return Err(error(ErrorCode::RevisionConflict, "revision"));
        }
        validate_transition(&run.snapshot, &input.snapshot)?;
        recovery_state::transition(&run.snapshot, &input.snapshot, &input.events)?;
        if input.events.iter().any(|event| {
            matches!(
                event.payload,
                RunEventPayload::RunResumed { .. } | RunEventPayload::RunRecovered { .. }
            ) && event.timestamp_ms > input.now_ms
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
        if let Some(receipt) = input
            .snapshot
            .recovery_receipts
            .last()
            .filter(|receipt| receipt.accepted_revision == input.snapshot.revision)
        {
            if receipt.expired != (input.now_ms >= run.snapshot.timing.deadline_at_ms) {
                return Err(error(
                    ErrorCode::DeadlineExceeded,
                    "recovery.acceptance_expiry",
                ));
            }
        }
        let additions = validate_records(state, &input.records)?;
        validate_snapshot_refs(state, &additions, &input.snapshot)?;
        context_state::validate_update(&run.snapshot, &input.snapshot, &input.events)?;
        verification_state::transition(
            state,
            &additions,
            &run.snapshot,
            &input.snapshot,
            &input.messages,
            &input.events,
        )?;
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
        session_snapshot.context_revision_ref = input.snapshot.context_revision_ref.clone();
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
        let execution = state
            .executions
            .get(run_id)
            .map(|history| {
                execution::advance_history(
                    history,
                    &run.snapshot,
                    &input.snapshot,
                    segment_override,
                )
            })
            .transpose()?;
        let state = scopes
            .get_mut(&scope_key(scope))
            .expect("validated namespace");
        if let Some(execution) = execution {
            state.executions.insert(run_id.clone(), execution);
        }

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
    }
}
```

## `crates/wickle/src/state/checkpoint.rs`

```rust
use super::*;
use crate::{JsonDigest, RunOutcome, RunRequest, serialization::data_digest};
use serde::{Deserialize, Serialize, Serializer};

/// Version of the protected, scope-local memory-store checkpoint format.
pub const STATE_STORE_CHECKPOINT_VERSION: &str = "wickle.state-store.v2";
const LEGACY_CHECKPOINT_VERSION: &str = "wickle.state-store.v1";

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
    #[serde(skip_serializing_if = "Vec::is_empty")]
    executions: Vec<&'a crate::ExecutionHistory>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    legacy_runs: Vec<&'a Id>,
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
            schema_version: if self.state.executions.is_empty() {
                LEGACY_CHECKPOINT_VERSION
            } else {
                STATE_STORE_CHECKPOINT_VERSION
            },
            executions: self.state.executions.values().collect(),
            legacy_runs: if self.state.executions.is_empty() {
                Vec::new()
            } else {
                self.state.legacy_runs.iter().collect()
            },
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
    #[serde(default)]
    executions: Vec<crate::ExecutionHistory>,
    #[serde(default)]
    legacy_runs: Vec<Id>,
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
        if !matches!(
            value.get("schema_version").and_then(Value::as_str),
            Some(STATE_STORE_CHECKPOINT_VERSION | LEGACY_CHECKPOINT_VERSION)
        ) {
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
    if data.schema_version != STATE_STORE_CHECKPOINT_VERSION
        && data.schema_version != LEGACY_CHECKPOINT_VERSION
    {
        return Err(invalid("checkpoint.schema_version"));
    }
    if data.schema_version == LEGACY_CHECKPOINT_VERSION
        && (!data.executions.is_empty() || !data.legacy_runs.is_empty())
    {
        return Err(invalid("checkpoint.legacy_execution"));
    }
    let execution_records = data.executions.clone();
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
        super::context_state::validate_session(&state, session)?;
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
        if reports.iter().any(|existing| {
            existing.hook == report.hook
                && existing.selection == report.selection
                && existing.target == report.target
        }) {
            return Err(invalid("checkpoint.hook_observation_duplicate"));
        }
        reports.push(report);
    }
    state.message_ids = message_ids;
    state.event_ids = event_ids;
    for execution in execution_records {
        let run = state
            .runs
            .get(&execution.run_id)
            .ok_or_else(|| invalid("checkpoint.execution_run"))?;
        super::execution::validate_history(&execution, &run.snapshot)?;
        for segment in &execution.segments {
            let effects = match &segment.outcome {
                Some(crate::SegmentOutcome::Interrupted { interruption }) => {
                    &interruption.unresolved_effects
                }
                Some(crate::SegmentOutcome::Settled { outcome }) => &outcome.unresolved_effects,
                None => continue,
            };
            for reference in effects {
                record_value(&state, &BTreeMap::new(), reference)?;
            }
        }

        if state
            .executions
            .insert(execution.run_id.clone(), execution)
            .is_some()
        {
            return Err(invalid("checkpoint.duplicate_execution"));
        }
    }
    if data.schema_version == LEGACY_CHECKPOINT_VERSION {
        state.legacy_runs = state.runs.keys().cloned().collect();
    } else {
        for id in data.legacy_runs {
            let run = state
                .runs
                .get(&id)
                .ok_or_else(|| invalid("checkpoint.legacy_run"))?;
            if !run.snapshot.status.is_terminal()
                || state.executions.contains_key(&id)
                || !state.legacy_runs.insert(id)
            {
                return Err(invalid("checkpoint.legacy_run"));
            }
        }
        if state
            .runs
            .keys()
            .any(|id| !state.executions.contains_key(id) && !state.legacy_runs.contains(id))
        {
            return Err(invalid("checkpoint.execution_missing"));
        }
    }
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
    super::recovery_state::history(state, &run.snapshot, &run.events)?;
    super::context_state::validate_history(state, &run.snapshot, &run.events)?;
    super::verification_state::history(state, &run.snapshot, &run.events)?;
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
            RunEventPayload::ContextRewritten { revision_ref } => {
                let revision =
                    super::context_state::revision(state, &empty, revision_ref, &run.snapshot)?;
                if revision.run_id != run.snapshot.run_id {
                    return Err(invalid("checkpoint.context_event"));
                }
            }
            RunEventPayload::RunRecovered {
                recovery_receipt_ref,
            } => {
                super::recovery_state::event(
                    state,
                    &empty,
                    &run.snapshot,
                    event,
                    recovery_receipt_ref,
                )?;
            }
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
            RunEventPayload::ToolReconciled { reconciliation_ref } => {
                record_value(state, &empty, reconciliation_ref)?;
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
                        ModelAttemptState::Completed {}
                            | ModelAttemptState::Failed { .. }
                            | ModelAttemptState::Interrupted { .. }
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

## `crates/wickle/src/state/execution.rs`

```rust
use super::*;
use crate::serialization::data_digest;
use crate::{
    AcceptedSegmentCommand, BeginSegmentRequest, BeginSegmentResult, ControlAction, ControlCommand,
    ControlReceipt, ExecutionHistory, ExecutionRecordVersion, ExecutionSegment,
    ExecutionTransactions, InterruptionCause, InterruptionRecord, JsonTextLimits, RequestSnapshot,
    SegmentOutcome, SegmentStart, StoredControlCommand,
};
fn invalid(path: &str) -> ContractError {
    error(ErrorCode::InvalidSnapshot, path)
}

pub(super) fn initial_history(
    snapshot: &RunSnapshot,
    actor: &Id,
    submitted: Option<&RequestSnapshot>,
) -> Result<ExecutionHistory, ContractError> {
    if let Some(value) = submitted {
        validate_submission(value, snapshot)?;
    }
    Ok(ExecutionHistory {
        run_id: snapshot.run_id.clone(),
        execution_principal_ref: actor.clone(),
        submitted: submitted.cloned(),
        initial_claimed: false,
        segments: vec![ExecutionSegment {
            schema_version: ExecutionRecordVersion::V1,
            execution_principal_ref: actor.clone(),
            run_id: snapshot.run_id.clone(),
            segment_id: segment_id(&snapshot.run_id, 0)?,
            accepted_revision: 0,
            outcome: None,
            app_state: None,
        }],
        accepted_commands: vec![],
        controls: vec![],
    })
}
fn segment_id(run: &Id, revision: u64) -> Result<Id, ContractError> {
    Id::new(format!("segment:{}", data_digest(&(run, revision))))
}
fn interrupted(previous: &RunSnapshot, segment: &ExecutionSegment) -> SegmentOutcome {
    SegmentOutcome::Interrupted {
        interruption: InterruptionRecord {
            segment_id: segment.segment_id.clone(),
            cause: InterruptionCause::SegmentStopped,
            checkpoint_revision: previous.revision,
            recoverable: true,
            unresolved_effects: previous
                .tool_ledger
                .iter()
                .filter(|entry| {
                    matches!(
                        entry.state,
                        ToolCallState::Dispatching { .. } | ToolCallState::Unknown { .. }
                    )
                })
                .filter_map(|entry| entry.call.bound_input_ref.clone())
                .collect(),
        },
    }
}
pub(super) fn advance_history(
    history: &ExecutionHistory,
    previous: &RunSnapshot,
    next: &RunSnapshot,
    override_id: Option<&Id>,
) -> Result<ExecutionHistory, ContractError> {
    let mut result = history.clone();
    let command = next
        .resume_receipts
        .last()
        .filter(|r| r.accepted_revision == next.revision)
        .map(|r| &r.command)
        .or_else(|| {
            next.recovery_receipts
                .last()
                .filter(|r| r.accepted_revision == next.revision)
                .map(|r| &r.command)
        });
    let last = result
        .segments
        .last_mut()
        .ok_or_else(|| invalid("execution.segment"))?;
    let new_segment = command.is_some()
        || last.outcome.is_some()
        || override_id.is_some_and(|id| *id != last.segment_id);
    if new_segment {
        if last.outcome.is_none() {
            last.outcome = Some(interrupted(previous, last));
        }
        let id = override_id
            .cloned()
            .unwrap_or(segment_id(&next.run_id, next.revision)?);
        if result.segments.iter().any(|s| s.segment_id == id) {
            return Err(error(ErrorCode::RequestConflict, "execution.segment_id"));
        }
        result.segments.push(ExecutionSegment {
            schema_version: ExecutionRecordVersion::V1,
            execution_principal_ref: history.execution_principal_ref.clone(),
            run_id: next.run_id.clone(),
            segment_id: id,
            accepted_revision: next.revision,
            outcome: None,
            app_state: result.segments.last().and_then(|s| s.app_state.clone()),
        });
    }
    let current = result
        .segments
        .last_mut()
        .ok_or_else(|| invalid("execution.segment"))?;
    if let Some(outcome) = &next.outcome {
        current.outcome = Some(SegmentOutcome::Settled {
            outcome: Box::new(outcome.clone()),
        });
    }
    if let Some(command) = command {
        if result
            .accepted_commands
            .iter()
            .any(|c| c.command_id == command.command_id)
            || result
                .controls
                .iter()
                .any(|c| c.command.command_id == command.command_id)
        {
            return Err(error(ErrorCode::RequestConflict, "execution.command_id"));
        }
        result.accepted_commands.push(AcceptedSegmentCommand {
            command_id: command.command_id.clone(),
            payload_digest: data_digest(command),
            segment_id: current.segment_id.clone(),
        });
    }
    result.initial_claimed = true;
    validate_history(&result, next)?;
    Ok(result)
}
pub(super) fn validate_history(
    history: &ExecutionHistory,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    if history.run_id != snapshot.run_id || history.segments.is_empty() {
        return Err(invalid("execution.run"));
    }
    if let Some(submitted) = &history.submitted {
        validate_submission(submitted, snapshot)?;
    }
    let mut ids = BTreeSet::new();
    let mut revision = 0;
    for (i, segment) in history.segments.iter().enumerate() {
        segment.validate()?;
        if segment.run_id != history.run_id
            || segment.execution_principal_ref != history.execution_principal_ref
            || !ids.insert(segment.segment_id.clone())
            || (i == 0 && segment.accepted_revision != 0)
            || (i > 0 && segment.accepted_revision <= revision)
            || segment.accepted_revision > snapshot.revision
            || (i + 1 < history.segments.len() && segment.outcome.is_none())
        {
            return Err(invalid("execution.history"));
        }
        revision = segment.accepted_revision;
    }
    let current = history
        .segments
        .last()
        .ok_or_else(|| invalid("execution.segment"))?;
    match (&snapshot.outcome, &current.outcome) {
        (Some(expected), Some(SegmentOutcome::Settled { outcome }))
            if expected == outcome.as_ref() => {}
        (None, None) => {}
        (None, Some(SegmentOutcome::Interrupted { .. }))
            if snapshot.status == RunStatus::Interrupted => {}
        _ => return Err(invalid("execution.current_outcome")),
    }
    if !history.initial_claimed && (snapshot.revision > 0 || history.segments.len() > 1) {
        return Err(invalid("execution.initial_claim"));
    }
    let mut commands = BTreeSet::new();
    for command in &history.accepted_commands {
        if !commands.insert(command.command_id.clone()) || !ids.contains(&command.segment_id) {
            return Err(invalid("execution.accepted"));
        }
    }
    let mut expected = BTreeMap::new();
    for (command, revision) in snapshot
        .resume_receipts
        .iter()
        .map(|r| (&r.command, r.accepted_revision))
        .chain(
            snapshot
                .recovery_receipts
                .iter()
                .map(|r| (&r.command, r.accepted_revision)),
        )
    {
        if expected
            .insert(command.command_id.clone(), (data_digest(command), revision))
            .is_some()
        {
            return Err(invalid("execution.receipt_duplicate"));
        }
    }
    for control in &history.controls {
        if let Some(id) = &control.processed_segment_id {
            let segment = history
                .segments
                .iter()
                .find(|s| &s.segment_id == id)
                .ok_or_else(|| invalid("execution.control_segment"))?;
            if expected
                .insert(
                    control.command.command_id.clone(),
                    (data_digest(&control.command), segment.accepted_revision),
                )
                .is_some()
            {
                return Err(invalid("execution.receipt_duplicate"));
            }
        }
    }
    if expected.len() != history.accepted_commands.len() {
        return Err(invalid("execution.receipt_coverage"));
    }
    for accepted in &history.accepted_commands {
        let segment = history
            .segments
            .iter()
            .find(|s| s.segment_id == accepted.segment_id)
            .ok_or_else(|| invalid("execution.receipt_segment"))?;
        if expected.get(&accepted.command_id)
            != Some(&(accepted.payload_digest.clone(), segment.accepted_revision))
        {
            return Err(invalid("execution.receipt_payload"));
        }
    }
    let mut controls = BTreeSet::new();
    for control in &history.controls {
        validate_control(&control.command)?;
        let accepted = history
            .accepted_commands
            .iter()
            .find(|c| c.command_id == control.command.command_id);
        match (&control.processed_segment_id, accepted) {
            (Some(segment), Some(receipt))
                if *segment == receipt.segment_id
                    && receipt.payload_digest == data_digest(&control.command) => {}
            (None, None) => {}
            _ => return Err(invalid("execution.control_receipt")),
        }

        if !controls.insert(control.command.command_id.clone())
            || control
                .processed_segment_id
                .as_ref()
                .is_some_and(|id| !ids.contains(id))
        {
            return Err(invalid("execution.controls"));
        }
    }
    Ok(())
}
fn validate_control(command: &ControlCommand) -> Result<(), ContractError> {
    if matches!(command.action,ControlAction::Stop{cause} if !matches!(cause,InterruptionCause::HostShutdown|InterruptionCause::SegmentStopped))
    {
        return Err(error(ErrorCode::InvalidContract, "control.stop_cause"));
    }
    Ok(())
}

impl ExecutionTransactions for MemoryStateStore {
    fn read_execution<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
    ) -> PortFuture<'a, ExecutionHistory> {
        Box::pin(async move {
            let scopes = self.lock()?;
            let state = namespace(&scopes, scope)?;
            if !state.runs.contains_key(run_id) {
                return Err(not_found());
            }
            state.executions.get(run_id).cloned().ok_or_else(|| {
                error(
                    ErrorCode::CapabilityUnsupported,
                    "execution.legacy_checkpoint",
                )
            })
        })
    }
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        command: ControlCommand,
    ) -> PortFuture<'a, ControlReceipt> {
        Box::pin(async move {
            validate_control(&command)?;
            let mut scopes = self.lock()?;
            let state = scopes.get_mut(&scope_key(scope)).ok_or_else(not_found)?;
            let run = state.runs.get(run_id).ok_or_else(not_found)?;
            let history = state.executions.get_mut(run_id).ok_or_else(|| {
                error(
                    ErrorCode::CapabilityUnsupported,
                    "execution.legacy_checkpoint",
                )
            })?;
            if let Some(existing) = history
                .controls
                .iter()
                .find(|c| c.command.command_id == command.command_id)
            {
                if existing.command != command {
                    return Err(error(ErrorCode::RequestConflict, "control.command_id"));
                }
                return Ok(ControlReceipt {
                    run_id: run_id.clone(),
                    command_id: command.command_id,
                    processed_segment_id: existing.processed_segment_id.clone(),
                });
            }
            if history
                .accepted_commands
                .iter()
                .any(|c| c.command_id == command.command_id)
            {
                return Err(error(ErrorCode::RequestConflict, "control.command_id"));
            }
            let receipt = ControlReceipt {
                run_id: run_id.clone(),
                command_id: command.command_id.clone(),
                processed_segment_id: None,
            };
            if !run.snapshot.status.is_terminal() {
                history.controls.push(StoredControlCommand {
                    command,
                    processed_segment_id: None,
                });
            }
            Ok(receipt)
        })
    }
    fn begin_segment<'a>(
        &'a self,
        scope: &'a Scope,
        request: BeginSegmentRequest,
    ) -> PortFuture<'a, BeginSegmentResult> {
        Box::pin(async move {
            // A private working copy is validated synchronously under the scope
            // mutex. Publish once only after all lease/commit checks succeed.
            let mut scopes = self.lock()?;
            let previous = namespace(&scopes, scope)?.clone();
            let working = MemoryStateStore {
                scopes: Mutex::new(BTreeMap::from([(scope_key(scope), previous)])),
            };
            let result = working.begin_segment_now(scope, request)?;
            let mut committed = working
                .scopes
                .into_inner()
                .map_err(|_| error(ErrorCode::PersistenceUnavailable, "execution.transaction"))?;
            scopes.insert(
                scope_key(scope),
                committed.remove(&scope_key(scope)).ok_or_else(not_found)?,
            );
            Ok(result)
        })
    }
}
impl MemoryStateStore {
    fn begin_segment_now(
        &self,
        scope: &Scope,
        request: BeginSegmentRequest,
    ) -> Result<BeginSegmentResult, ContractError> {
        let (saved, history) = {
            let scopes = self.lock()?;
            let state = namespace(&scopes, scope)?;
            (
                stored_run(state, &request.run_id)?,
                state
                    .executions
                    .get(&request.run_id)
                    .cloned()
                    .ok_or_else(|| {
                        error(
                            ErrorCode::CapabilityUnsupported,
                            "execution.legacy_checkpoint",
                        )
                    })?,
            )
        };
        let current = history
            .segments
            .last()
            .ok_or_else(|| invalid("execution.segment"))?;
        let payload = match &request.start {
            SegmentStart::Initial => None,
            SegmentStart::Resume(command) => {
                if command.run_id != request.run_id
                    || command.expected_revision != request.expected_revision
                {
                    return Err(error(ErrorCode::RequestConflict, "execution.command"));
                }
                Some((command.command_id.clone(), data_digest(command)))
            }
            SegmentStart::Control(id) => {
                let command = history
                    .controls
                    .iter()
                    .find(|c| c.command.command_id == *id)
                    .ok_or_else(not_found)?;
                Some((id.clone(), data_digest(&command.command)))
            }
        };
        if let Some((id, digest)) = &payload {
            if let Some(receipt) = history
                .accepted_commands
                .iter()
                .find(|c| c.command_id == *id)
            {
                if receipt.payload_digest != *digest {
                    return Err(error(ErrorCode::RequestConflict, "execution.command"));
                }
                let segment = history
                    .segments
                    .iter()
                    .find(|s| s.segment_id == receipt.segment_id)
                    .cloned()
                    .ok_or_else(|| invalid("execution.receipt"))?;
                return Ok(BeginSegmentResult {
                    state: saved,
                    segment,
                    lease: None,
                });
            }
        } else if history.initial_claimed {
            if request.segment_id != history.segments[0].segment_id {
                return Err(error(ErrorCode::RequestConflict, "execution.initial_id"));
            }
            return Ok(BeginSegmentResult {
                state: saved,
                segment: history.segments[0].clone(),
                lease: None,
            });
        }
        if saved.snapshot.revision != request.expected_revision {
            return Err(error(ErrorCode::RevisionConflict, "execution.revision"));
        }
        if saved.snapshot.status.is_terminal() {
            if matches!(request.start, SegmentStart::Control(_)) {
                return Ok(BeginSegmentResult {
                    state: saved,
                    segment: current.clone(),
                    lease: None,
                });
            }
            return Err(error(ErrorCode::InvalidTransition, "execution.terminal"));
        }
        if !matches!(request.start, SegmentStart::Initial)
            && history
                .segments
                .iter()
                .any(|s| s.segment_id == request.segment_id)
        {
            return Err(error(ErrorCode::RequestConflict, "execution.segment_id"));
        }
        let lease = self.acquire_lease_now(
            scope,
            &request.run_id,
            &request.owner,
            request.now_ms,
            request.lease_ttl_ms.get(),
        )?;
        if matches!(request.start, SegmentStart::Initial) {
            if request.transition.is_some()
                || request.segment_id != current.segment_id
                || saved.snapshot.phase != RunPhase::Admission
            {
                return Err(error(ErrorCode::InvalidTransition, "execution.initial"));
            }
            let mut scopes = self.lock()?;
            scopes
                .get_mut(&scope_key(scope))
                .ok_or_else(not_found)?
                .executions
                .get_mut(&request.run_id)
                .ok_or_else(not_found)?
                .initial_claimed = true;
            return Ok(BeginSegmentResult {
                state: saved,
                segment: current.clone(),
                lease: Some(lease),
            });
        }
        let transition = request
            .transition
            .ok_or_else(|| error(ErrorCode::InvalidContract, "execution.transition"))?;
        match &request.start {
            SegmentStart::Resume(command) => {
                let actual = transition
                    .snapshot
                    .resume_receipts
                    .last()
                    .filter(|r| r.accepted_revision == transition.snapshot.revision)
                    .map(|r| &r.command)
                    .or_else(|| {
                        transition
                            .snapshot
                            .recovery_receipts
                            .last()
                            .filter(|r| r.accepted_revision == transition.snapshot.revision)
                            .map(|r| &r.command)
                    });
                if actual != Some(command) {
                    return Err(error(
                        ErrorCode::InvalidContract,
                        "execution.resume_transition",
                    ));
                }
            }
            SegmentStart::Control(id) => {
                let control = history
                    .controls
                    .iter()
                    .find(|c| c.command.command_id == *id)
                    .ok_or_else(not_found)?;
                let allowed = match &control.command.action {
                    ControlAction::Cancel { reason } => {
                        transition.snapshot.status == RunStatus::Cancelled
                            && matches!(transition.snapshot.outcome.as_ref().map(|o| &o.result), Some(OutcomeResult::Cancelled { reason: actual }) if actual == reason.as_str())
                    }
                    ControlAction::Expire => {
                        request.now_ms >= saved.snapshot.timing.deadline_at_ms
                            && transition.snapshot.status == RunStatus::Exhausted
                            && matches!(
                                transition.snapshot.outcome.as_ref().map(|o| &o.result),
                                Some(OutcomeResult::Exhausted {
                                    budget: crate::BudgetKind::Elapsed
                                })
                            )
                    }
                    ControlAction::Stop { .. } => {
                        return Err(error(
                            ErrorCode::CapabilityUnsupported,
                            "execution.stop_checkpoint",
                        ));
                    }
                };
                if !allowed {
                    return Err(error(
                        ErrorCode::InvalidTransition,
                        "execution.control_transition",
                    ));
                }
            }
            SegmentStart::Initial => unreachable!(),
        }
        let result = self.commit_now(
            scope,
            &request.run_id,
            CommitInput {
                expected_revision: request.expected_revision,
                lease: lease.clone(),
                now_ms: request.now_ms,
                snapshot: transition.snapshot,
                messages: transition.messages,
                events: transition.events,
                records: transition.records,
            },
            Some(&request.segment_id),
        )?;
        let mut scopes = self.lock()?;
        let state = scopes.get_mut(&scope_key(scope)).ok_or_else(not_found)?;
        let history = state
            .executions
            .get_mut(&request.run_id)
            .ok_or_else(not_found)?;
        if let SegmentStart::Control(id) = request.start {
            let (_, digest) = payload.ok_or_else(|| invalid("execution.control"))?;
            history.accepted_commands.push(AcceptedSegmentCommand {
                command_id: id.clone(),
                payload_digest: digest,
                segment_id: request.segment_id.clone(),
            });
            history
                .controls
                .iter_mut()
                .find(|c| c.command.command_id == id)
                .ok_or_else(not_found)?
                .processed_segment_id = Some(request.segment_id);
        }
        validate_history(history, &result.snapshot)?;
        let segment = history.segments.last().cloned().ok_or_else(not_found)?;
        let lease = if result.snapshot.status.is_terminal() {
            None
        } else {
            Some(lease)
        };
        Ok(BeginSegmentResult {
            state: result,
            segment,
            lease,
        })
    }
}

fn validate_submission(
    submitted: &RequestSnapshot,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    submitted.validate(JsonTextLimits::default())?;
    let request = crate::RunRequest::from_json(submitted.request_json())?;
    let expected_system = snapshot
        .system_inputs
        .as_ref()
        .map(|s| s.values_digest.clone())
        .unwrap_or_else(|| crate::canonical_digest(&serde_json::json!({})));
    if request != snapshot.request
        || submitted.profile_ref.id != snapshot.profile.profile().agent_id
        || submitted.profile_ref.version != snapshot.profile.profile().version
        || crate::canonical_digest_json(submitted.system_inputs_json())? != expected_system
    {
        return Err(invalid("execution.submitted_snapshot"));
    }
    Ok(())
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
            descriptor_digest: Some(canonical_digest(&serde_json::json!({}))),
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

impl wickle::ExecutionTransactions for ControlledStore {
    fn read_execution<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
    ) -> wickle::PortFuture<'a, wickle::ExecutionHistory> {
        self.inner.read_execution(scope, run_id)
    }
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
        command: wickle::ControlCommand,
    ) -> wickle::PortFuture<'a, wickle::ControlReceipt> {
        self.inner.submit_control_command(scope, run_id, command)
    }
    fn begin_segment<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        request: wickle::BeginSegmentRequest,
    ) -> wickle::PortFuture<'a, wickle::BeginSegmentResult> {
        self.inner.begin_segment(scope, request)
    }
}
```

## `crates/wickle/tests/execution_store.rs`

```rust
//! Atomic execution ownership, commands, rollback and checkpoint validation.
use serde_json::json;
use std::sync::Arc;
use wickle::*;
#[allow(dead_code)]
mod support;
use support::*;
#[path = "support/execution_store.rs"]
mod suite;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_store_atomically_claims_segments_and_consumes_controls() {
    suite::atomic_execution_contract(Arc::new(MemoryStateStore::new())).await;
}
#[tokio::test]
async fn checkpoint_preserves_history_and_rejects_corrupted_execution_identity() {
    let store = MemoryStateStore::new();
    store
        .admit(
            &scope(),
            admission("run", "request", "session", "input", "1").await,
        )
        .await
        .unwrap();
    let checkpoint = store.export_checkpoint(&scope()).unwrap();
    let encoded = serde_json::to_string(&checkpoint).unwrap();
    let restored = MemoryStateStore::from_checkpoint(
        StateStoreCheckpoint::from_json(&encoded, &scope(), &checkpoint.digest()).unwrap(),
    );
    assert_eq!(
        restored.read_execution(&scope(), &id("run")).await.unwrap(),
        store.read_execution(&scope(), &id("run")).await.unwrap()
    );
    let mut corrupt: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    corrupt["executions"][0]["segments"][0]["execution_principal_ref"] = json!("replacement");
    assert!(
        StateStoreCheckpoint::from_json(
            &corrupt.to_string(),
            &scope(),
            &canonical_digest(&corrupt)
        )
        .is_err()
    );
    // An authentic legacy-format graph remains readable, but has no invented actor/segment evidence.
    let mut legacy: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    legacy["schema_version"] = json!("wickle.state-store.v1");
    legacy.as_object_mut().unwrap().remove("executions");
    let restored = MemoryStateStore::from_checkpoint(
        StateStoreCheckpoint::from_json(&legacy.to_string(), &scope(), &canonical_digest(&legacy))
            .unwrap(),
    );
    assert_eq!(
        restored
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .run_id,
        id("run")
    );
    assert_eq!(
        restored
            .read_execution(&scope(), &id("run"))
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
    assert_eq!(
        restored
            .admit(
                &scope(),
                admission("other", "other", "other", "input", "1").await
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
}
#[tokio::test]
async fn recovery_acceptance_and_lease_are_one_transaction() {
    suite::atomic_recovery_contract(Arc::new(MemoryStateStore::new())).await;
}

#[tokio::test]
async fn version_two_rejects_missing_active_history_even_when_other_histories_remain() {
    let store = MemoryStateStore::new();
    for run in ["first", "second"] {
        store
            .admit(&scope(), admission(run, run, run, "input", "1").await)
            .await
            .unwrap();
    }
    let mut image = serde_json::to_value(store.export_checkpoint(&scope()).unwrap()).unwrap();
    image["executions"].as_array_mut().unwrap().remove(0);
    assert!(
        StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(&image))
            .is_err()
    );
}
#[tokio::test]
async fn restored_receipts_must_match_the_original_recovery_command_and_segment() {
    let store = Arc::new(MemoryStateStore::new());
    suite::atomic_recovery_contract(store.clone()).await;
    let image = serde_json::to_value(store.export_checkpoint(&scope()).unwrap()).unwrap();
    for kind in ["missing", "digest", "segment"] {
        let mut changed = image.clone();
        match kind {
            "missing" => changed["executions"][0]["accepted_commands"] = json!([]),
            "digest" => {
                changed["executions"][0]["accepted_commands"][0]["payload_digest"] =
                    json!(canonical_digest(&json!(false)))
            }
            _ => {
                changed["executions"][0]["accepted_commands"][0]["segment_id"] =
                    changed["executions"][0]["segments"][0]["segment_id"].clone()
            }
        }
        assert!(
            StateStoreCheckpoint::from_json(
                &changed.to_string(),
                &scope(),
                &canonical_digest(&changed)
            )
            .is_err(),
            "{kind}"
        );
    }
}

#[tokio::test]
async fn version_two_cannot_silently_reclassify_new_terminal_runs_as_legacy() {
    let store = Arc::new(MemoryStateStore::new());
    suite::atomic_execution_contract(store.clone()).await;
    store
        .admit(
            &scope(),
            admission("other", "other", "other", "input", "1").await,
        )
        .await
        .unwrap();
    let mut image = serde_json::to_value(store.export_checkpoint(&scope()).unwrap()).unwrap();
    let histories = image["executions"].as_array_mut().unwrap();
    let index = histories
        .iter()
        .position(|v| v["run_id"] == "atomic-run")
        .unwrap();
    histories.remove(index);
    assert!(
        StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(&image))
            .is_err()
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
            descriptor_digest: Some(self.tool.descriptor_digest().clone()),
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

impl wickle::ExecutionTransactions for FailingCommitStore {
    fn read_execution<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
    ) -> wickle::PortFuture<'a, wickle::ExecutionHistory> {
        self.inner.read_execution(scope, run_id)
    }
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
        command: wickle::ControlCommand,
    ) -> wickle::PortFuture<'a, wickle::ControlReceipt> {
        self.inner.submit_control_command(scope, run_id, command)
    }
    fn begin_segment<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        request: wickle::BeginSegmentRequest,
    ) -> wickle::PortFuture<'a, wickle::BeginSegmentResult> {
        self.inner.begin_segment(scope, request)
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
        max_output_tokens: None,
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
                4 => matches!(request.action, PolicyAction::ResumeRun { .. }),
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
    RejectModelResult,
    RejectRecoveryLease,
    RejectRecoveryAcceptance,
    LoseRecoveryAcknowledgement,
    RejectCandidate,
    OmitVerificationEvent,
    RejectVerification,
    LoseVerificationAcknowledgement,
    PassThrough,
    Reject,
    LoseAcknowledgement,
    Pause,
    PauseEmptyEventPage,
    RejectContext,
    LoseContextAcknowledgement,
}
pub struct FinalCommitStore {
    pub inner: Arc<MemoryStateStore>,
    pub mode: FinalCommitMode,
    pub final_entered: Notify,
    pub release: Semaphore,
    pub final_attempts: AtomicUsize,
    pub context_attempts: AtomicUsize,
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
            context_attempts: AtomicUsize::new(0),
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
            if self.block_read.load(Ordering::SeqCst) == 4 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "load.offline",
                ));
            }

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
        if matches!(self.mode, FinalCommitMode::RejectRecoveryLease) {
            return Box::pin(async {
                Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "lease.offline",
                ))
            });
        }
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
            let mut input = input;
            if matches!(self.mode, FinalCommitMode::RejectRecoveryAcceptance)
                && input
                    .events
                    .iter()
                    .any(|event| matches!(event.payload, RunEventPayload::RunRecovered { .. }))
            {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "recovery.offline",
                ));
            }

            if matches!(self.mode, FinalCommitMode::LoseRecoveryAcknowledgement)
                && input
                    .events
                    .iter()
                    .any(|event| matches!(event.payload, RunEventPayload::RunRecovered { .. }))
            {
                self.inner.commit(s, r, input).await?;
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "recovery.ack",
                ));
            }
            if matches!(self.mode, FinalCommitMode::RejectModelResult)
                && input
                    .snapshot
                    .model_ledger
                    .iter()
                    .any(|entry| entry.response_ref.is_some())
            {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "model.result.commit",
                ));
            }
            if matches!(self.mode, FinalCommitMode::RejectCandidate)
                && self
                    .inner
                    .load(s, r)
                    .await?
                    .snapshot
                    .candidate_ref
                    .is_none()
                && input.snapshot.candidate_ref.is_some()
            {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "candidate.commit",
                ));
            }
            if matches!(self.mode, FinalCommitMode::OmitVerificationEvent) {
                let before = input.events.len();
                input.events.retain(|event| {
                    !matches!(event.payload, RunEventPayload::VerificationCompleted { .. })
                });
                input.snapshot.last_event_seq -= (before - input.events.len()) as u64;
            }
            if matches!(
                self.mode,
                FinalCommitMode::RejectContext | FinalCommitMode::LoseContextAcknowledgement
            ) && self.inner.load(s, r).await?.snapshot.context_revision_ref
                != input.snapshot.context_revision_ref
            {
                self.context_attempts.fetch_add(1, Ordering::SeqCst);
                if matches!(self.mode, FinalCommitMode::LoseContextAcknowledgement) {
                    self.inner.commit(s, r, input).await?;
                }
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "context.commit",
                ));
            }
            if matches!(
                self.mode,
                FinalCommitMode::RejectVerification
                    | FinalCommitMode::LoseVerificationAcknowledgement
            ) && self.inner.load(s, r).await?.snapshot.verification_records
                != input.snapshot.verification_records
            {
                if matches!(self.mode, FinalCommitMode::LoseVerificationAcknowledgement) {
                    self.inner.commit(s, r, input).await?;
                }
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "verification.commit",
                ));
            }
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
                FinalCommitMode::PauseEmptyEventPage
                | FinalCommitMode::LoseRecoveryAcknowledgement
                | FinalCommitMode::RejectRecoveryLease
                | FinalCommitMode::RejectRecoveryAcceptance
                | FinalCommitMode::RejectModelResult
                | FinalCommitMode::RejectCandidate
                | FinalCommitMode::OmitVerificationEvent
                | FinalCommitMode::RejectVerification
                | FinalCommitMode::LoseVerificationAcknowledgement
                | FinalCommitMode::PassThrough
                | FinalCommitMode::RejectContext
                | FinalCommitMode::LoseContextAcknowledgement => {
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
            components: None,
            context_sources: None,
            context_token_estimator: None,
            context_runtime: None,
            verification: None,
            skills: None,
            artifacts: None,
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

impl wickle::ExecutionTransactions for FinalCommitStore {
    fn read_execution<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
    ) -> wickle::PortFuture<'a, wickle::ExecutionHistory> {
        self.inner.read_execution(scope, run_id)
    }
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
        command: wickle::ControlCommand,
    ) -> wickle::PortFuture<'a, wickle::ControlReceipt> {
        self.inner.submit_control_command(scope, run_id, command)
    }
    fn begin_segment<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        request: wickle::BeginSegmentRequest,
    ) -> wickle::PortFuture<'a, wickle::BeginSegmentResult> {
        self.inner.begin_segment(scope, request)
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

impl wickle::ExecutionTransactions for FaultStore {
    fn read_execution<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
    ) -> wickle::PortFuture<'a, wickle::ExecutionHistory> {
        self.inner.read_execution(scope, run_id)
    }
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
        command: wickle::ControlCommand,
    ) -> wickle::PortFuture<'a, wickle::ControlReceipt> {
        self.inner.submit_control_command(scope, run_id, command)
    }
    fn begin_segment<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        request: wickle::BeginSegmentRequest,
    ) -> wickle::PortFuture<'a, wickle::BeginSegmentResult> {
        self.inner.begin_segment(scope, request)
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
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 6 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }
            self.inner.load(s, r).await
        })
    }
    fn load_session<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, SessionSnapshot> {
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 6 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }
            self.inner.load_session(s, r).await
        })
    }
    fn check_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, RunLease> {
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 6 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }
            self.inner.check_lease(s, r, l, n).await
        })
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
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 6 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }
            self.inner.renew_lease(s, r, l, n, t).await
        })
    }
    fn release_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 6 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }
            self.inner.release_lease(s, r, l, n).await
        })
    }
    fn read_events<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        after: u64,
        limit: usize,
    ) -> PortFuture<'a, EventPage> {
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 6 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }
            self.inner.read_events(s, r, after, limit).await
        })
    }
    fn read_record<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 6 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }
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
            if self.mode.load(Ordering::SeqCst) == 6 {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }
            if self.mode.load(Ordering::SeqCst) == 5
                && input
                    .events
                    .iter()
                    .any(|event| matches!(event.payload, RunEventPayload::ToolUnresolved { .. }))
            {
                self.mode.store(6, Ordering::SeqCst);
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "store.offline",
                ));
            }

            if self.mode.load(Ordering::SeqCst) == 4
                && input
                    .events
                    .iter()
                    .any(|event| matches!(event.payload, RunEventPayload::ToolUnresolved { .. }))
            {
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "tool.result.commit",
                ));
            }
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

impl wickle::ExecutionTransactions for CommandStore {
    fn read_execution<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
    ) -> wickle::PortFuture<'a, wickle::ExecutionHistory> {
        self.inner.read_execution(scope, run_id)
    }
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
        command: wickle::ControlCommand,
    ) -> wickle::PortFuture<'a, wickle::ControlReceipt> {
        self.inner.submit_control_command(scope, run_id, command)
    }
    fn begin_segment<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        request: wickle::BeginSegmentRequest,
    ) -> wickle::PortFuture<'a, wickle::BeginSegmentResult> {
        self.inner.begin_segment(scope, request)
    }
}
```

## `crates/wickle/tests/support/context_sources.rs`

```rust
//! Deterministic sources and model attempts expose saved-batch and current-ACL behavior.

use super::{hooks_support, resume_support};
use futures_util::stream;
pub use hooks_support::{completed, context, gate, id, object, reference, request, scope};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};
use wickle::*;

#[derive(Clone, Copy)]
pub enum Reply {
    Ready,
    Empty,
    Unavailable,
    WrongScope,
    WrongOrigin,
    WrongLifetime,
    WrongDigest,
    TooMany,
    TooLarge,
    Pending,
    Paused,
    Error,
    ReadyEmpty,
}

pub struct Estimator {
    pub tokens: AtomicUsize,
    pub calls: AtomicUsize,
}
impl ContextTokenEstimator for Estimator {
    fn version(&self) -> VersionedRef {
        reference("fixture-context-estimate")
    }
    fn estimate(&self, items: &[ContextItem]) -> Result<u64, ContractError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(if items.is_empty() {
            0
        } else {
            self.tokens.load(Ordering::SeqCst) as u64
        })
    }
}
pub struct Source {
    pub definition: ContextSourceDefinition,
    pub selection: ContextSourceRef,
    pub replies: Mutex<VecDeque<Reply>>,
    pub calls: AtomicUsize,
    pub uses: AtomicUsize,
    pub use_failure: Mutex<Option<(usize, ErrorCode)>>,
    pub revoked: Arc<AtomicBool>,
    pub requests: Mutex<Vec<ContextRequest>>,
    pub use_requests: Mutex<Vec<ContextUseRequest>>,
    pub order: Arc<Mutex<Vec<Id>>>,
    pub entered: Notify,
    pub release: Semaphore,
}
impl ContextSource for Source {
    fn provide<'a>(
        &'a self,
        request: &'a ContextRequest,
        context: &'a ContextCallContext,
    ) -> PortFuture<'a, ContextResult> {
        Box::pin(async move {
            let generation = self.calls.fetch_add(1, Ordering::SeqCst);
            self.requests.lock().unwrap().push(request.clone());
            self.order
                .lock()
                .unwrap()
                .push(self.definition.source.id.clone());
            assert_eq!(request.scope, scope());
            assert_eq!(context.scope, scope());
            assert_eq!(request.definition, self.definition);
            assert_eq!(request.binding.source, self.selection);
            assert_eq!(context.source, self.selection);
            assert_eq!(context.context_request_id, request.context_request_id);
            assert_eq!(
                request.user_input,
                super::agent_support::request("request").input
            );
            self.entered.notify_one();
            let reply = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("only planned source queries may execute");
            match reply {
                Reply::Pending => return std::future::pending().await,
                Reply::Paused => {
                    self.release.acquire().await.unwrap().forget();
                }
                Reply::Error => {
                    return Err(ContractError::new(
                        ErrorCode::InvalidContract,
                        "source.transport",
                    ));
                }
                Reply::Empty => {
                    return Ok(ContextResult::Empty {
                        source_revision: Some(id(&format!("revision-{generation}"))),
                        reported_usage: None,
                    });
                }
                Reply::Unavailable => {
                    return Ok(ContextResult::Unavailable {
                        code: id("backend_unavailable"),
                        source_revision: Some(id(&format!("revision-{generation}"))),
                        reported_usage: None,
                    });
                }
                _ => {}
            }
            let count = if matches!(reply, Reply::TooMany) {
                3
            } else if matches!(reply, Reply::ReadyEmpty) {
                0
            } else {
                1
            };
            let mut items = vec![];
            for index in 0..count {
                let mut owner = request.scope.clone();
                if matches!(reply, Reply::WrongScope) {
                    owner.tenant_id = id("other-tenant");
                }
                let origin = if matches!(reply, Reply::WrongOrigin) {
                    ContextOrigin::Memory
                } else {
                    self.definition.origin
                };
                let lifetime = if matches!(reply, Reply::WrongLifetime) {
                    ContextLifetime::Session {
                        session_id: request.session_id.clone(),
                    }
                } else {
                    match request.binding.trigger {
                        ContextTrigger::RunStart => ContextLifetime::Run {
                            run_id: request.run_id.clone(),
                        },
                        ContextTrigger::BeforeModel => ContextLifetime::Step {
                            run_id: request.run_id.clone(),
                            model_step_id: request.model_step_id.clone().unwrap(),
                        },
                    }
                };
                let value = if matches!(reply, Reply::TooLarge) {
                    json!({"source":self.definition.source.id,"payload":"x".repeat(5000)})
                } else {
                    json!({"source":self.definition.source.id,"generation":generation})
                };
                let mut item = ContextItem::new(
                    if count == 1 {
                        id("shared")
                    } else {
                        id(&format!("item-{index}"))
                    },
                    origin,
                    self.definition.source.clone(),
                    owner,
                    vec![InputContent::Json { value }],
                    lifetime,
                    ContextPriority::Required,
                );
                if matches!(reply, Reply::WrongDigest) {
                    item.digest = canonical_digest(&json!("not-the-item"));
                }
                items.push(item);
            }
            Ok(ContextResult::Ready {
                items,
                source_revision: Some(id(&format!("revision-{generation}"))),
                reported_usage: Some(ContextSourceUsage {
                    requests: Some(1),
                    tokens: Some(1_000_000),
                }),
            })
        })
    }
    fn authorize_use<'a>(
        &'a self,
        request: &'a ContextUseRequest,
        context: &'a ContextCallContext,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let check = self.uses.fetch_add(1, Ordering::SeqCst) + 1;
            self.use_requests.lock().unwrap().push(request.clone());
            assert_eq!(request.request.binding.source, self.selection);
            assert_eq!(context.source, self.selection);
            assert_eq!(context.scope, scope());
            assert_eq!(request.request.scope, scope());
            assert!(
                request
                    .items
                    .iter()
                    .all(|item| item.item_id == id("shared"))
            );
            if let Some((at, code)) = *self.use_failure.lock().unwrap() {
                if check == at {
                    return Err(ContractError::new(code, "source.current_acl"));
                }
            }
            if self.revoked.load(Ordering::SeqCst) {
                Err(ContractError::new(
                    ErrorCode::AccessDenied,
                    "source.current_acl",
                ))
            } else {
                Ok(())
            }
        })
    }
}
pub struct Model {
    pub inner: Arc<resume_support::Model>,
    pub physical_calls: AtomicUsize,
    pub fail_first: AtomicBool,
    pub revoke_on_failure: AtomicBool,
    pub revoked: Arc<AtomicBool>,
    pub requests: Mutex<Vec<ModelRequest>>,
}
impl ModelPort for Model {
    fn binding(&self) -> ModelPortBinding {
        self.inner.binding()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let attempt = self.physical_calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        if attempt == 0 && self.fail_first.load(Ordering::SeqCst) {
            if self.revoke_on_failure.load(Ordering::SeqCst) {
                self.revoked.store(true, Ordering::SeqCst);
            }
            Box::pin(stream::iter([Ok(ModelEvent::ResponseError {
                kind: ModelFailureKind::Transport,
                metadata: Default::default(),
            })]))
        } else {
            self.inner.generate(request, context)
        }
    }
}
pub struct Fixture {
    pub base: hooks_support::Fixture,
    pub sources: Vec<Arc<Source>>,
    pub bindings: Vec<ContextSourceBinding>,
    pub estimator: Arc<Estimator>,
    pub model: Arc<Model>,
    pub revoked: Arc<AtomicBool>,
    pub order: Arc<Mutex<Vec<Id>>>,
    pub copy_hook: Option<Arc<CopyHook>>,
    pub policy: Arc<Policy>,
    pub store: Arc<SourceStore>,
}
impl Fixture {
    pub fn new() -> Self {
        let base = hooks_support::Fixture::new();
        let revoked = Arc::new(AtomicBool::new(false));
        let model = Arc::new(Model {
            inner: base.base.model.clone(),
            physical_calls: AtomicUsize::new(0),
            fail_first: AtomicBool::new(false),
            revoke_on_failure: AtomicBool::new(false),
            revoked: revoked.clone(),
            requests: Mutex::new(vec![]),
        });
        let policy = Arc::new(Policy {
            inner: base.policy.clone(),
            deny_provide: AtomicBool::new(false),
            deny_use: AtomicBool::new(false),
        });
        let store = Arc::new(SourceStore {
            inner: base.base.base.store.clone(),
            failure: AtomicUsize::new(0),
        });
        Self {
            base,
            sources: vec![],
            bindings: vec![],
            estimator: Arc::new(Estimator {
                tokens: AtomicUsize::new(16),
                calls: AtomicUsize::new(0),
            }),
            model,
            revoked,
            order: Arc::new(Mutex::new(vec![])),
            copy_hook: None,
            policy,
            store,
        }
    }
    pub fn add(
        &mut self,
        name: &str,
        trigger: ContextTrigger,
        required: bool,
        replies: Vec<Reply>,
    ) -> Arc<Source> {
        let selection = ContextSourceRef::Catalog(CatalogSourceRef {
            source_id: id(name),
            version: id("1"),
        });
        let source = Arc::new(Source {
            definition: ContextSourceDefinition {
                source: reference(name),
                origin: ContextOrigin::Retrieval,
                contract_version: 1,
            },
            selection: selection.clone(),
            replies: Mutex::new(replies.into()),
            calls: AtomicUsize::new(0),
            uses: AtomicUsize::new(0),
            use_failure: Mutex::new(None),
            revoked: self.revoked.clone(),
            requests: Mutex::new(vec![]),
            use_requests: Mutex::new(vec![]),
            order: self.order.clone(),
            entered: Notify::new(),
            release: Semaphore::new(0),
        });
        self.bindings.push(ContextSourceBinding {
            source: selection,
            trigger,
            required,
            timeout_ms: 5000.try_into().unwrap(),
            max_items: 2.try_into().unwrap(),
            max_bytes: 4096.try_into().unwrap(),
            max_tokens: 100.try_into().unwrap(),
        });
        self.sources.push(source.clone());
        source
    }
    pub fn profile(&self) -> AgentProfile {
        let mut profile = self.base.profile();
        profile.context_sources = Some(self.bindings.clone());
        profile
    }
    pub fn enable_copy_hook(&mut self) -> Arc<CopyHook> {
        self.base.add(
            "copied-source",
            HookPosition::BeforeModel,
            hooks_support::Behavior::Context,
            0,
            true,
        );
        let hook = Arc::new(CopyHook {
            calls: AtomicUsize::new(0),
            seen: Mutex::new(vec![]),
        });
        self.copy_hook = Some(hook.clone());
        hook
    }
    pub fn agent_bindings(&self) -> AgentBindings {
        let mut bindings = self.base.bindings();
        bindings.state = self.store.clone();
        bindings.policy =
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(5)).unwrap());
        bindings.model_exchange = Arc::new(
            ModelExchange::new(self.model.clone(), bindings.policy.clone())
                .with_route_inspector(
                    self.base.base.base.inspector.clone(),
                    Duration::from_secs(5),
                )
                .unwrap()
                .with_retry_policy(ModelRetryPolicy {
                    max_retries: 1,
                    backoff_ms: 0,
                }),
        );
        let registry = Arc::new(
            ContextSourceRegistry::new(
                scope(),
                self.sources
                    .iter()
                    .map(|source| ContextSourceRegistration {
                        selection: source.selection.clone(),
                        definition: source.definition.clone(),
                        source: source.clone(),
                    })
                    .collect(),
            )
            .unwrap(),
        );
        bindings.context_sources = Some(Arc::new(
            ContextSourceRuntime::new(
                bindings.state.clone(),
                bindings.policy.clone(),
                bindings.clock.clone(),
                bindings.ids.clone(),
                registry,
                self.estimator.clone(),
            )
            .unwrap(),
        ));
        bindings.context_token_estimator = Some(self.estimator.clone());
        if let Some(copy) = &self.copy_hook {
            let registry = HookRegistry::new(
                scope(),
                self.base
                    .hooks
                    .iter()
                    .map(|hook| HookRegistration {
                        definition: hook.definition.clone(),
                        handler: if hook.definition.hook.id == id("copied-source") {
                            copy.clone() as Arc<dyn HookHandler>
                        } else {
                            hook.clone()
                        },
                    })
                    .collect(),
            )
            .unwrap();
            bindings.hooks = Some(Arc::new(HookRuntime::new(
                bindings.state.clone(),
                bindings.policy.clone(),
                bindings.clock.clone(),
                bindings.ids.clone(),
                Arc::new(registry),
            )));
        }
        bindings
    }
    pub fn agent(&self) -> Agent {
        create_agent(self.profile(), self.agent_bindings()).unwrap()
    }
    pub async fn start(&self, agent: &Agent) -> RunHandle {
        self.base.started(agent).await
    }
    pub async fn outcome(&self, handle: &RunHandle) -> RunOutcome {
        self.base.outcome(handle).await
    }
    pub async fn saved(&self, handle: &RunHandle) -> StoredRun {
        self.base.saved(handle).await
    }
}
pub struct Policy {
    inner: Arc<hooks_support::Policy>,
    pub deny_provide: AtomicBool,
    pub deny_use: AtomicBool,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if (matches!(request.action, PolicyAction::ProvideContext { .. })
                && self.deny_provide.load(Ordering::SeqCst))
                || (matches!(request.action, PolicyAction::UseSourceContext { .. })
                    && self.deny_use.load(Ordering::SeqCst))
            {
                return Ok(PolicyDecision::Deny {
                    reason: id("source_policy_revoked"),
                });
            }
            self.inner.authorize(request, context).await
        })
    }
}

/// Inject faults only into a newly committed source batch; all other state behavior is real.
pub struct SourceStore {
    pub inner: Arc<MemoryStateStore>,
    pub failure: AtomicUsize,
}
impl StateStore for SourceStore {
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
    fn record_hook_observation<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        report: HookObservation,
    ) -> PortFuture<'a, ()> {
        self.inner.record_hook_observation(s, r, report)
    }
    fn read_hook_observations<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
    ) -> PortFuture<'a, Vec<HookObservation>> {
        self.inner.read_hook_observations(s, r)
    }
    fn commit<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            let saved = self.inner.load(s, r).await?;
            if input.snapshot.context_batches.len() > saved.snapshot.context_batches.len() {
                match self.failure.swap(0, Ordering::SeqCst) {
                    1 => {
                        return Err(ContractError::new(
                            ErrorCode::PersistenceUnavailable,
                            "source.commit",
                        ));
                    }
                    2 => {
                        self.inner.commit(s, r, input).await?;
                        return Err(ContractError::new(
                            ErrorCode::PersistenceUnavailable,
                            "source.ack",
                        ));
                    }
                    _ => {}
                }
            }
            self.inner.commit(s, r, input).await
        })
    }
}
pub struct CopyHook {
    pub calls: AtomicUsize,
    pub seen: Mutex<Vec<Vec<ContextItem>>>,
}
impl HookHandler for CopyHook {
    fn call<'a>(&'a self, input: &'a HookInput, _: &'a HookContext) -> PortFuture<'a, HookOutput> {
        Box::pin(async move {
            let HookInput::BeforeModel { context_items, .. } = input else {
                return Err(ContractError::new(
                    ErrorCode::InvalidContract,
                    "hook.position",
                ));
            };
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.seen.lock().unwrap().push(context_items.clone());
            Ok(HookOutput::Context {
                additions: context_items
                    .iter()
                    .map(|item| HookContextAddition {
                        content: item.content.clone(),
                        priority: ContextPriority::Required,
                    })
                    .collect(),
            })
        })
    }
}

pub fn projected_items(request: &ModelRequest) -> Vec<&Value> {
    request
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
        .collect()
}

/// Two exact, usable routes sharing the fixture port; only an explicit matching
/// failure permits the second candidate. Counters expose accidental fallback.
pub struct FallbackRouter {
    snapshot: RoutingSnapshot,
    pub fallbacks: AtomicUsize,
}
impl FallbackRouter {
    pub fn new(original: &RoutingSnapshot, cause: ModelFailureKind) -> Self {
        let mut catalog = original.catalog().clone();
        let mut policy = original.policy().clone();
        let mut fallback = catalog.bindings[0].clone();
        fallback.binding = reference("fallback-route");
        let digest = fallback.contract_digest(&catalog.models[0]).unwrap();
        for evidence in &mut fallback.evidence {
            evidence.binding_digest = digest.clone();
        }
        policy.rules[0].fallbacks = vec![fallback.binding.clone()];
        policy.rules[0].fallback_on = vec![cause];
        catalog.bindings.push(fallback);
        Self {
            snapshot: RoutingSnapshot::new(catalog, policy).unwrap(),
            fallbacks: AtomicUsize::new(0),
        }
    }
}
impl ModelRouter for FallbackRouter {
    fn snapshot(&self) -> &RoutingSnapshot {
        &self.snapshot
    }
    fn resolve<'a>(&'a self, request: &'a RouteRequest) -> PortFuture<'a, RouteSelection> {
        Box::pin(async move {
            let rule = &self.snapshot.policy().rules[0];
            let (binding, reason, candidate_index) = if let Some(failure) = request.previous_failure
            {
                self.fallbacks.fetch_add(1, Ordering::SeqCst);
                (
                    &rule.fallbacks[0],
                    RouteSelectionReason::Fallback { failure },
                    1,
                )
            } else if request
                .previous_route
                .as_ref()
                .is_some_and(|route| route.binding == rule.fallbacks[0])
            {
                (&rule.fallbacks[0], RouteSelectionReason::Reuse, 1)
            } else {
                (
                    &rule.primary,
                    if request.previous_route.is_some() {
                        RouteSelectionReason::Reuse
                    } else {
                        RouteSelectionReason::Initial
                    },
                    0,
                )
            };
            let selection = RouteSelection {
                route: self.snapshot.route_for_binding(binding)?,
                reason,
                candidate_index,
                routing_snapshot_digest: self.snapshot.digest(),
                request_digest: request.digest(),
            };
            self.snapshot.validate_selection(request, &selection)?;
            Ok(selection)
        })
    }
}

impl wickle::ExecutionTransactions for SourceStore {
    fn read_execution<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
    ) -> wickle::PortFuture<'a, wickle::ExecutionHistory> {
        self.inner.read_execution(scope, run_id)
    }
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
        command: wickle::ControlCommand,
    ) -> wickle::PortFuture<'a, wickle::ControlReceipt> {
        self.inner.submit_control_command(scope, run_id, command)
    }
    fn begin_segment<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        request: wickle::BeginSegmentRequest,
    ) -> wickle::PortFuture<'a, wickle::BeginSegmentResult> {
        self.inner.begin_segment(scope, request)
    }
}
```

## `crates/wickle/tests/support/execution_store.rs`

```rust
use super::*;
use std::sync::Arc;

pub async fn atomic_execution_contract(store: Arc<dyn StateStore>) {
    let s = scope();
    let run = id("atomic-run");
    let initial = store
        .admit(
            &s,
            admission(run.as_str(), "request", "session", "input", "1").await,
        )
        .await
        .unwrap()
        .state;
    let history = store.read_execution(&s, &run).await.unwrap();
    assert!(!history.initial_claimed);
    let first_id = history.segments[0].segment_id.clone();
    let claim = |segment_id| BeginSegmentRequest {
        transition: None,
        run_id: run.clone(),
        expected_revision: 0,
        segment_id,
        owner: id("worker"),
        now_ms: 0,
        lease_ttl_ms: 1000.try_into().unwrap(),
        start: SegmentStart::Initial,
    };
    // A failure after tentative lease acquisition must not publish the lease.
    assert!(
        store
            .begin_segment(&s, claim(id("wrong-initial-id")))
            .await
            .is_err()
    );
    assert!(
        !store
            .read_execution(&s, &run)
            .await
            .unwrap()
            .initial_claimed
    );
    let lease = store
        .acquire_lease(&s, &run, &id("rollback-probe"), 0, 1000)
        .await
        .unwrap();
    store.release_lease(&s, &run, &lease, 0).await.unwrap();
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let mut claims = Vec::new();
    for request in [claim(first_id.clone()), claim(first_id)] {
        let store = store.clone();
        let owner = s.clone();
        let gate = gate.clone();
        claims.push(tokio::spawn(async move {
            gate.wait().await;
            store.begin_segment(&owner, request).await
        }));
    }
    let b = claims.pop().unwrap().await.unwrap();
    let a = claims.pop().unwrap().await.unwrap();
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(
        usize::from(a.lease.is_some()) + usize::from(b.lease.is_some()),
        1
    );
    let lease = a.lease.or(b.lease).unwrap();
    store.release_lease(&s, &run, &lease, 1).await.unwrap();
    let command = ControlCommand {
        command_id: id("cancel"),
        principal_ref: id("reviewer"),
        action: ControlAction::Cancel { reason: id("stop") },
    };
    let (a, b) = tokio::join!(
        store.submit_control_command(&s, &run, command.clone()),
        store.submit_control_command(&s, &run, command.clone())
    );
    assert_eq!(a.unwrap(), b.unwrap());
    assert_eq!(
        store.read_execution(&s, &run).await.unwrap().controls.len(),
        1
    );
    let mut changed = command;
    changed.principal_ref = id("different-user");
    assert_eq!(
        store
            .submit_control_command(&s, &run, changed)
            .await
            .unwrap_err()
            .code,
        ErrorCode::RequestConflict
    );
    let mut commit = finished(&initial.snapshot, lease, 2);
    commit.snapshot.status = RunStatus::Cancelled;
    commit.snapshot.outcome.as_mut().unwrap().result = OutcomeResult::Cancelled {
        reason: "stop".into(),
    };
    let outcome = ProtectedRecord::new(
        id("cancel-outcome"),
        1,
        serde_json::to_value(commit.snapshot.outcome.as_ref().unwrap()).unwrap(),
    );
    commit.events[0].payload = RunEventPayload::RunFinished {
        outcome_ref: outcome.reference().clone(),
    };
    let transition = SegmentTransition {
        snapshot: commit.snapshot,
        messages: commit.messages,
        events: commit.events,
        records: vec![outcome],
    };
    let request = BeginSegmentRequest {
        transition: Some(transition),
        run_id: run.clone(),
        expected_revision: 0,
        segment_id: id("cancel-segment"),
        owner: id("controller"),
        now_ms: 2,
        lease_ttl_ms: 1000.try_into().unwrap(),
        start: SegmentStart::Control(id("cancel")),
    };
    let mut reused = request.clone();
    reused.segment_id = history.segments[0].segment_id.clone();
    assert!(
        store.begin_segment(&s, reused).await.is_err(),
        "a control must allocate a new segment ID"
    );
    let (a, b) = tokio::join!(
        store.begin_segment(&s, request.clone()),
        store.begin_segment(&s, request)
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(a.segment, b.segment);
    assert!(a.lease.is_none() && b.lease.is_none());
    assert_eq!(a.state.snapshot.status, RunStatus::Cancelled);
    let history = store.read_execution(&s, &run).await.unwrap();
    assert_eq!(history.segments.len(), 2);
    assert_eq!(history.accepted_commands.len(), 1);
    assert_eq!(
        history.controls[0].processed_segment_id,
        Some(id("cancel-segment"))
    );
    assert_eq!(history.execution_principal_ref, id("execution-principal"));
    assert!(matches!(
        history.segments[0].outcome,
        Some(SegmentOutcome::Interrupted { .. })
    ));
    assert!(matches!(
        history.segments[1].outcome,
        Some(SegmentOutcome::Settled { .. })
    ));
    assert!(
        store
            .load(&s, &run)
            .await
            .unwrap()
            .session
            .active_run_id
            .is_none()
    );
    let before = store.read_events(&s, &run, 0, 100).await.unwrap();
    store
        .submit_control_command(
            &s,
            &run,
            ControlCommand {
                command_id: id("terminal-noop"),
                principal_ref: id("reviewer"),
                action: ControlAction::Expire,
            },
        )
        .await
        .unwrap();
    assert_eq!(store.read_execution(&s, &run).await.unwrap(), history);
    assert_eq!(store.read_events(&s, &run, 0, 100).await.unwrap(), before);
    let foreign = Scope {
        workspace_id: id("foreign"),
        ..s
    };
    assert!(store.read_execution(&foreign, &run).await.is_err());
}

pub async fn atomic_recovery_contract(store: Arc<dyn StateStore>) {
    let s = scope();
    let run = id("recover-run");
    let saved = store
        .admit(
            &s,
            admission(
                run.as_str(),
                "recover-request",
                "recover-session",
                "input",
                "1",
            )
            .await,
        )
        .await
        .unwrap()
        .state;
    let source = saved.snapshot.recovery_record(id("source")).unwrap();
    let command = ResumeCommand {
        run_id: run.clone(),
        expected_revision: 0,
        command_id: id("recover-once"),
        action: ResumeAction::Recover {
            recovery_ref: source.reference().clone(),
        },
    };
    let command_record = ProtectedRecord::new(
        id("recovery-command"),
        1,
        serde_json::to_value(&command).unwrap(),
    );
    let receipt = RecoveryReceipt {
        command: command.clone(),
        command_ref: command_record.reference().clone(),
        source_snapshot_ref: source.reference().clone(),
        accepted_revision: 1,
        previous_segment_start_revision: 0,
        previous_last_event_seq: 1,
        actor_ref: id("reviewer"),
        capability_grant_ref: id("grant"),
        expired: false,
        recovery_attempt_id: Some(id("recovery-budget")),
    };
    let receipt_record = ProtectedRecord::new(
        id("recovery-receipt"),
        1,
        serde_json::to_value(&receipt).unwrap(),
    );
    let mut next = saved.snapshot.clone();
    next.revision = 1;
    next.phase = RunPhase::Prepare;
    next.recovery_receipts.push(receipt);
    next.usage.recovery_attempts += 1;
    next.reservations.push(AttemptReservation {
        attempt_id: id("recovery-budget"),
        kind: ReservationKind::Recovery {},
        reserved_at_ms: 0,
    });
    next.last_event_seq = 2;
    let mut e = event(
        &run,
        &id("recover-session"),
        &s,
        2,
        RunEventPayload::RunRecovered {
            recovery_receipt_ref: receipt_record.reference().clone(),
        },
    );
    e.timestamp_ms = 0;
    let transition = SegmentTransition {
        snapshot: next,
        messages: vec![],
        events: vec![e],
        records: vec![source, command_record, receipt_record],
    };
    let request = BeginSegmentRequest {
        transition: Some(transition),
        run_id: run.clone(),
        expected_revision: 0,
        segment_id: id("recovered"),
        owner: id("recover-worker"),
        now_ms: 0,
        lease_ttl_ms: 1000.try_into().unwrap(),
        start: SegmentStart::Resume(command),
    };
    let mut broken = request.clone();
    broken.transition.as_mut().unwrap().events.clear();
    assert!(store.begin_segment(&s, broken).await.is_err());
    assert_eq!(store.load(&s, &run).await.unwrap(), saved);
    assert!(
        store
            .read_execution(&s, &run)
            .await
            .unwrap()
            .accepted_commands
            .is_empty()
    );
    let before = store.read_execution(&s, &run).await.unwrap();
    let accepted = store.begin_segment(&s, request.clone()).await.unwrap();
    assert!(accepted.lease.is_some());
    let replay = store.begin_segment(&s, request.clone()).await.unwrap();
    assert!(replay.lease.is_none());
    assert_eq!(accepted.segment, replay.segment);
    assert_eq!(replay.state.snapshot.usage.recovery_attempts, 1);
    let mut conflict = request;
    conflict.expected_revision = 1;
    if let SegmentStart::Resume(c) = &mut conflict.start {
        c.expected_revision = 1;
    }
    assert!(matches!(
        store.begin_segment(&s, conflict).await,
        Err(ContractError {
            code: ErrorCode::RequestConflict,
            ..
        })
    ));
    let after = store.read_execution(&s, &run).await.unwrap();
    assert_eq!(after.segments.len(), before.segments.len() + 1);
    assert_eq!(after.accepted_commands.len(), 1);
    assert_eq!(after.execution_principal_ref, id("execution-principal"));
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
        max_output_tokens: None,
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
        recovery_receipts: vec![],
        hook_plan_ref: None,
        source_plan_ref: None,
        skill_plan_ref: None,
        context_plan_ref: None,
        context_revision_ref: None,
        context_decisions: vec![],
        verification_plan_ref: None,
        candidate_ref: None,
        verification_records: vec![],
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
        execution_principal_ref: id("execution-principal"),
        submitted: None,
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

## `crates/wickle/tests/support/tool_execution.rs`

```rust
//! Stored tool plans and observable executor effects for tool-round contract tests.

use super::support::{admission, event, id, prepared, scope};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use wickle::*;

pub const OWNED: &str = "11111111-1111-4111-8111-111111111111";
pub const FOREIGN: &str = "22222222-2222-4222-8222-222222222222";
pub fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
pub fn input_registry() -> SystemInputRegistry {
    SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap()
}
fn compiled(name: &str, effect: ToolSideEffect, registry: &SystemInputRegistry) -> CompiledTool {
    SchemaCompiler::new().compile(ToolDescriptor{tool:reference(name),name:id(name),description:format!("{name} records"),input_schema:json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"default":10},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),agent_parameters:vec!["query".into(),"limit".into()],system_bindings:None,output_schema:json!({"type":"string"}),side_effect:effect,concurrency:ToolConcurrency::Serial,retry:ToolRetryPolicy::Never,reconcile:false,max_output_bytes:4096.try_into().unwrap()},registry).unwrap()
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
                manifest_digest: canonical_digest(&json!("fixture")),
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
pub struct ClockSource {
    origin: tokio::time::Instant,
}
impl ClockSource {
    fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
        }
    }
}
impl Clock for ClockSource {
    fn now(&self) -> Result<ClockReading, ContractError> {
        let elapsed = self.origin.elapsed().as_millis() as u64;
        Ok(ClockReading {
            utc_ms: elapsed as i64,
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
pub struct Ids(AtomicUsize);
impl IdSource for Ids {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id(&format!(
            "tool-record-{}",
            self.0.fetch_add(1, Ordering::SeqCst)
        )))
    }
}
#[derive(Default)]
pub struct Policy {
    pub denied: Mutex<Option<Id>>,
    pub approval: Mutex<Option<Id>>,
    pub calls: Mutex<Vec<ToolPolicyInput>>,
    pub reject_foreign: AtomicUsize,
    pub revoke_on_check: AtomicUsize,
    pub approve_on_check: AtomicUsize,
    pub store: Option<Arc<MemoryStateStore>>,
    pub before_approval: Mutex<Option<ToolLedgerEntry>>,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::ExecuteTool { input } | PolicyAction::ReconcileTool { input, .. } =
                &request.action
            {
                let count = {
                    let mut calls = self.calls.lock().unwrap();
                    calls.push(input.clone());
                    calls.len()
                };
                if self.denied.lock().unwrap().as_ref() == Some(&input.call_id)
                    || (self.reject_foreign.load(Ordering::SeqCst) > 0
                        && input.execution_args().get("workspace_id") != Some(&json!(OWNED)))
                    || (self.revoke_on_check.load(Ordering::SeqCst) > 0
                        && count >= self.revoke_on_check.load(Ordering::SeqCst))
                {
                    return Ok(PolicyDecision::Deny {
                        reason: id("not_owned_or_revoked"),
                    });
                }
                if self.approve_on_check.load(Ordering::SeqCst) > 0
                    && count >= self.approve_on_check.load(Ordering::SeqCst)
                {
                    let saved = self
                        .store
                        .as_ref()
                        .unwrap()
                        .load(&request.owner_scope, &request.resource_id)
                        .await?;
                    *self.before_approval.lock().unwrap() = Some(
                        saved
                            .snapshot
                            .tool_ledger
                            .into_iter()
                            .find(|entry| entry.call.call_id == input.call_id)
                            .unwrap(),
                    );
                    return Ok(PolicyDecision::RequireApproval {
                        reason: id("late_review"),
                    });
                }
                if self.approval.lock().unwrap().as_ref() == Some(&input.call_id) {
                    return Ok(PolicyDecision::RequireApproval {
                        reason: id("review_required"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}
#[derive(Clone, Copy)]
pub enum Action {
    Success,
    WrongOutput,
    Pending,
    Panic,
    Error,
    DeclaredFailure,
    DeclaredUnknown,
    Cancel,
}
#[derive(Debug, Clone, PartialEq)]
pub struct Invocation {
    pub call_id: Id,
    pub attempt_id: Id,
    pub idempotency_key: Id,
    pub args: JsonObject,
}
pub struct Executor {
    pub reconciled: Mutex<Vec<Invocation>>,
    pub block_reconciliation: AtomicBool,
    pub reconciliation_entered: Notify,
    pub reconciliation_release: Notify,
    pub action: Action,
    pub side_effect: ToolSideEffect,
    pub calls: AtomicUsize,
    pub applied: AtomicUsize,
    pub observed: Mutex<Vec<Invocation>>,
    pub order: Arc<Mutex<Vec<Id>>>,
    pub store: Arc<MemoryStateStore>,
    pub cancel: Mutex<Option<CancellationToken>>,
    pub entered: Notify,
}
impl ToolExecutor for Executor {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.order.lock().unwrap().push(context.call_id.clone());
            self.observed.lock().unwrap().push(Invocation {
                call_id: context.call_id.clone(),
                attempt_id: context.attempt_id.clone(),
                idempotency_key: context.idempotency_key.clone(),
                args: args.clone(),
            });
            let saved = self.store.load(&scope(), &id("run")).await?;
            let entry = saved
                .snapshot
                .tool_ledger
                .iter()
                .find(|entry| entry.call.call_id == context.call_id)
                .unwrap();
            assert!(
                matches!(&entry.state,ToolCallState::Dispatching{attempt_id,idempotency_key} if attempt_id==&context.attempt_id&&idempotency_key==&context.idempotency_key)
            );
            assert!(entry.call.bound_input_ref.is_some());
            assert_eq!(context.scope, scope());
            if self.side_effect != ToolSideEffect::ReadOnly {
                self.applied.fetch_add(1, Ordering::SeqCst);
            }
            self.entered.notify_one();
            let effect = if self.side_effect == ToolSideEffect::ReadOnly {
                ToolEffect::NotApplied
            } else {
                ToolEffect::Applied
            };
            let receipt = (effect == ToolEffect::Applied)
                .then(|| json!({"effect_id":"external-effect","value":args["query"]}));
            match self.action {
                Action::Success => Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Succeeded {
                        value: json!("observed result"),
                    },
                    effect,
                    receipt,
                }),
                Action::WrongOutput => Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Succeeded { value: json!(42) },
                    effect,
                    receipt,
                }),
                Action::DeclaredFailure => Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Failed {
                        code: id("tool_failed"),
                    },
                    effect,
                    receipt,
                }),
                Action::DeclaredUnknown => Ok(ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Failed {
                        code: id("unknown_effect"),
                    },
                    effect: ToolEffect::Unknown,
                    receipt,
                }),
                Action::Pending => std::future::pending().await,
                Action::Panic => panic!("synthetic executor panic after entry"),
                Action::Error => Err(ContractError::new(
                    ErrorCode::InvalidContract,
                    "synthetic.transport",
                )),
                Action::Cancel => {
                    self.cancel.lock().unwrap().as_ref().unwrap().cancel();
                    std::future::pending().await
                }
            }
        })
    }
    fn reconcile<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolReconciliation> {
        Box::pin(async move {
            let query = Invocation {
                call_id: context.call_id.clone(),
                attempt_id: context.attempt_id.clone(),
                idempotency_key: context.idempotency_key.clone(),
                args: args.clone(),
            };
            self.reconciled.lock().unwrap().push(query.clone());
            self.reconciliation_entered.notify_one();
            if self.block_reconciliation.load(Ordering::SeqCst) {
                self.reconciliation_release.notified().await;
            }
            if !self.observed.lock().unwrap().contains(&query) {
                return Ok(ToolReconciliation::Unknown);
            }
            let effect = if self.side_effect == ToolSideEffect::ReadOnly {
                ToolEffect::NotApplied
            } else {
                ToolEffect::Applied
            };
            Ok(ToolReconciliation::Known {
                result: ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Succeeded {
                        value: json!("observed result"),
                    },
                    effect,
                    receipt: (effect == ToolEffect::Applied)
                        .then(|| json!({"effect_id":"external-effect","value":args["query"]})),
                },
            })
        })
    }
}

pub struct Fixture {
    pub store: Arc<MemoryStateStore>,
    pub policy: Arc<Policy>,
    pub clock: Arc<ClockSource>,
    pub ids: Arc<Ids>,
    pub context: ExecutionContext,
    pub registry: Arc<ToolRegistry>,
    pub input_registry: Arc<SystemInputRegistry>,
    pub compiled: Vec<CompiledTool>,
    pub executors: Vec<Arc<Executor>>,
    pub order: Arc<Mutex<Vec<Id>>>,
    pub lease: RunLease,
}
impl Fixture {
    pub async fn new(tools: &[(&str, ToolSideEffect, Action)], system_value: Option<&str>) -> Self {
        Self::build(tools, system_value, false, 1).await
    }
    pub async fn reconcilable(
        tools: &[(&str, ToolSideEffect, Action)],
        system_value: Option<&str>,
    ) -> Self {
        Self::build(tools, system_value, true, 1).await
    }
    pub async fn reconcilable_with_limit(
        tools: &[(&str, ToolSideEffect, Action)],
        system_value: Option<&str>,
        recovery_attempts: u64,
    ) -> Self {
        Self::build(tools, system_value, true, recovery_attempts).await
    }
    async fn build(
        tools: &[(&str, ToolSideEffect, Action)],
        system_value: Option<&str>,
        reconcile: bool,
        recovery_attempts: u64,
    ) -> Self {
        let store = Arc::new(MemoryStateStore::new());
        let input_registry = Arc::new(input_registry());
        let order = Arc::new(Mutex::new(vec![]));
        let mut compiled_tools = vec![];
        let mut executors = vec![];
        let mut registrations = vec![];
        for (name, side_effect, action) in tools {
            let compiled = compiled(name, *side_effect, &input_registry);
            let compiled = if reconcile {
                let mut descriptor = compiled.descriptor().clone();
                descriptor.reconcile = true;
                SchemaCompiler::new()
                    .compile(descriptor, &input_registry)
                    .unwrap()
            } else {
                compiled
            };
            let executor = Arc::new(Executor {
                reconciled: Mutex::new(vec![]),
                block_reconciliation: AtomicBool::new(false),
                reconciliation_entered: Notify::new(),
                reconciliation_release: Notify::new(),
                action: *action,
                side_effect: *side_effect,
                calls: AtomicUsize::new(0),
                applied: AtomicUsize::new(0),
                observed: Mutex::new(vec![]),
                order: order.clone(),
                store: store.clone(),
                cancel: Mutex::new(None),
                entered: Notify::new(),
            });
            registrations.push(ToolRegistration {
                compiled: compiled.clone(),
                executor: executor.clone(),
            });
            compiled_tools.push(compiled);
            executors.push(executor);
        }
        let registry = Arc::new(ToolRegistry::new(scope(), registrations).unwrap());
        let supplied =
            system_value.map(|value| SystemInputs::new(object(json!({"workspace_id":value}))));
        let fixed = RunSystemInputs::capture(scope(), supplied.clone(), &input_registry).unwrap();
        let record = fixed.to_record(id("run-inputs"), 7);
        let input_ref = fixed.snapshot_ref(record.reference()).unwrap();
        let mut input = admission("run", "request", "session", "Read evidence", "1").await;
        let mut profile = input.snapshot.profile.profile().clone();
        profile.tools = compiled_tools
            .iter()
            .map(|tool| {
                ToolBindingRef::Catalog(CatalogToolRef {
                    tool_id: tool.descriptor().tool.id.clone(),
                    version: tool.descriptor().tool.version.clone(),
                    bindings: None,
                    config: None,
                })
            })
            .collect();
        profile.limits.max_tool_attempts = 8;
        profile.limits.max_recovery_attempts = recovery_attempts;
        input.snapshot.profile = ProfileValidator::new(&Catalog)
            .validate(&profile, &scope())
            .await
            .unwrap();
        input.snapshot.limits = profile.limits.clone();
        input.snapshot.system_inputs = Some(input_ref);
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
        input.records.push(record);
        store.admit(&scope(), input).await.unwrap();
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("worker"), 0, 20000)
            .await
            .unwrap();
        let context = ExecutionContext::new(
            ExecutionContextData {
                scope: scope(),
                principal_ref: id("caller"),
                capability_grant_ref: id("grant"),
                trace_context: None,
                system_inputs: supplied,
            },
            Default::default(),
        );
        Self {
            policy: Arc::new(Policy {
                store: Some(store.clone()),
                ..Policy::default()
            }),
            store,
            clock: Arc::new(ClockSource::new()),
            ids: Arc::new(Ids::default()),
            context,
            registry,
            input_registry,
            compiled: compiled_tools,
            executors,
            order,
            lease,
        }
    }
    pub async fn plan(&self, calls: &[(&str, &str, JsonObject)]) {
        let saved = self.store.load(&scope(), &id("run")).await.unwrap();
        let mut update = prepared(
            &saved.snapshot,
            self.lease.clone(),
            self.clock.now().unwrap().utc_ms,
        );
        update.snapshot.phase = RunPhase::Tool;
        let mut content = vec![];
        for (call_id, name, args) in calls {
            let descriptor_digest = self
                .compiled
                .iter()
                .find(|tool| tool.descriptor().name.as_str() == *name)
                .map(|tool| tool.descriptor_digest().clone());
            let call = ToolCall {
                call_id: id(call_id),
                model_request_id: id("model-request"),
                provider_call_id: id(&format!("provider-{call_id}")),
                tool_name: id(name),
                model_inputs: args.clone(),
                descriptor_digest,
                bound_input_ref: None,
            };
            let record = ProtectedRecord::new(
                id(&format!("plan-{call_id}")),
                1,
                serde_json::to_value(&call).unwrap(),
            );
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
                    call_ref: record.reference().clone(),
                },
            ));
            update.records.push(record);
            content.push(ContentBlock::ToolCall { call });
        }
        update.messages.push(Message {
            message_id: id("call-message"),
            run_id: id("run"),
            sequence: (saved.session.transcript_revision + 1).try_into().unwrap(),
            role: MessageRole::Assistant,
            origin: MessageOrigin::Model,
            visibility: Visibility::UserAndModel,
            content,
        });
        self.store
            .commit(&scope(), &id("run"), update)
            .await
            .unwrap();
    }
    pub async fn budget(&self, store: Arc<dyn StateStore>) -> RunBudget {
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
    pub fn round(&self) -> SerialToolRound {
        let policy =
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap());
        let binder = Arc::new(InputBinder::new(
            self.input_registry.clone(),
            None,
            policy.clone(),
            self.ids.clone(),
        ));
        SerialToolRound::new(self.registry.clone(), binder, policy, self.ids.clone())
    }
    pub async fn execute(&self) -> Result<ToolRoundOutcome, ContractError> {
        self.round()
            .execute(
                &id("model-request"),
                &self.context,
                &self.budget(self.store.clone()).await,
            )
            .await
    }
    pub async fn saved(&self) -> StoredRun {
        self.store.load(&scope(), &id("run")).await.unwrap()
    }
}

#[derive(Clone, Copy)]
pub enum FailStage {
    Binding,
    Reservation,
    Dispatch,
    Result,
    Correction,
}
pub struct FaultStore {
    pub inner: Arc<MemoryStateStore>,
    pub stage: FailStage,
    pub lose_ack: bool,
    pub failures: AtomicUsize,
}
impl StateStore for FaultStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        s: &'a Scope,
        session: &'a Id,
        request: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        self.inner.find_request(s, session, request)
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
            let previous = self.inner.load(s, r).await?.snapshot;
            let should_fail = match self.stage {
                FailStage::Binding => {
                    previous.tool_ledger[0].call.bound_input_ref.is_none()
                        && input.snapshot.tool_ledger[0].call.bound_input_ref.is_some()
                }
                FailStage::Reservation => {
                    input.snapshot.usage.tool_attempts > previous.usage.tool_attempts
                }
                FailStage::Dispatch => {
                    matches!(
                        input.snapshot.tool_ledger[0].state,
                        ToolCallState::Dispatching { .. }
                    ) && !matches!(
                        previous.tool_ledger[0].state,
                        ToolCallState::Dispatching { .. }
                    )
                }
                FailStage::Correction => input
                    .events
                    .iter()
                    .any(|event| matches!(event.payload, RunEventPayload::ToolReconciled { .. })),
                FailStage::Result => {
                    matches!(
                        input.snapshot.tool_ledger[0].state,
                        ToolCallState::Settled { .. } | ToolCallState::Unknown { .. }
                    ) && matches!(
                        previous.tool_ledger[0].state,
                        ToolCallState::Dispatching { .. }
                    )
                }
            };
            if should_fail {
                self.failures.fetch_add(1, Ordering::SeqCst);
                if self.lose_ack {
                    self.inner.commit(s, r, input).await?;
                }
                return Err(ContractError::new(
                    ErrorCode::PersistenceUnavailable,
                    "tool.transaction",
                ));
            }
            self.inner.commit(s, r, input).await
        })
    }
}

impl wickle::ExecutionTransactions for FaultStore {
    fn read_execution<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
    ) -> wickle::PortFuture<'a, wickle::ExecutionHistory> {
        self.inner.read_execution(scope, run_id)
    }
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        run_id: &'a wickle::Id,
        command: wickle::ControlCommand,
    ) -> wickle::PortFuture<'a, wickle::ControlReceipt> {
        self.inner.submit_control_command(scope, run_id, command)
    }
    fn begin_segment<'a>(
        &'a self,
        scope: &'a wickle::Scope,
        request: wickle::BeginSegmentRequest,
    ) -> wickle::PortFuture<'a, wickle::BeginSegmentResult> {
        self.inner.begin_segment(scope, request)
    }
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
        max_output_tokens: None,
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
        resume_receipts: vec![], recovery_receipts: vec![],
        hook_plan_ref: None, source_plan_ref: None, skill_plan_ref: None, context_plan_ref: None, context_revision_ref: None, context_decisions: vec![], verification_plan_ref: None, candidate_ref: None, verification_records: vec![],
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
                execution_principal_ref: id("execution-principal"),
                submitted: None,
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
        max_output_tokens: None,
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
        resume_receipts: vec![], recovery_receipts: vec![],
        hook_plan_ref: None, source_plan_ref: None, skill_plan_ref: None, context_plan_ref: None, context_revision_ref: None, context_decisions: vec![], verification_plan_ref: None, candidate_ref: None, verification_records: vec![],
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
        execution_principal_ref: id("execution-principal"),
        submitted: None,
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
        max_output_tokens: None,
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
        resume_receipts: vec![], recovery_receipts: vec![],
        hook_plan_ref: None, source_plan_ref: None, skill_plan_ref: None, context_plan_ref: None, context_revision_ref: None, context_decisions: vec![], verification_plan_ref: None, candidate_ref: None, verification_records: vec![],
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
                execution_principal_ref: id("execution-principal"),
                submitted: None,
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
        max_output_tokens: None,
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
        resume_receipts: vec![], recovery_receipts: vec![],
        hook_plan_ref: None, source_plan_ref: None, skill_plan_ref: None, context_plan_ref: None, context_revision_ref: None, context_decisions: vec![], verification_plan_ref: None, candidate_ref: None, verification_records: vec![],
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
        execution_principal_ref: id("execution-principal"),
        submitted: None,
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
        let execution = store.read_execution(&scope, &id("run")).await?;
        assert_eq!(execution.execution_principal_ref, id("execution-principal"));
        assert!(matches!(execution.segments[0].outcome, Some(SegmentOutcome::Settled { .. })));
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
        max_output_tokens: None,
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
        resume_receipts: vec![], recovery_receipts: vec![],
        hook_plan_ref: None, source_plan_ref: None, skill_plan_ref: None, context_plan_ref: None, context_revision_ref: None, context_decisions: vec![], verification_plan_ref: None, candidate_ref: None, verification_records: vec![],
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
        execution_principal_ref: id("execution-principal"),
        submitted: None,
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
    let execution = store.read_execution(&scope, &id("run")).await?;
    let claim = BeginSegmentRequest {
        run_id: id("run"), expected_revision: 0,
        segment_id: execution.segments[0].segment_id.clone(), owner: id("worker"),
        now_ms: 1000, lease_ttl_ms: 10_000.try_into()?,
        start: SegmentStart::Initial, transition: None,
    };
    let accepted = store.begin_segment(&scope, claim.clone()).await?;
    assert!(store.begin_segment(&scope, claim).await?.lease.is_none());
    let lease = accepted.lease.ok_or("initial lease missing")?;
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
        max_output_tokens: None,
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
        resume_receipts: vec![], recovery_receipts: vec![],
        hook_plan_ref: None, source_plan_ref: None, skill_plan_ref: None, context_plan_ref: None, context_revision_ref: None, context_decisions: vec![], verification_plan_ref: None, candidate_ref: None, verification_records: vec![],
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
        execution_principal_ref: id("execution-principal"),
        submitted: None,
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
