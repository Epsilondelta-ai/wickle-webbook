//! A new process recovers a real durable run after its predecessor exits mid-call.
#[path = "../../wickle/tests/support/agent.rs"]
#[allow(dead_code)]
mod support;
use support as agent_support;
#[path = "support/command_recovery.rs"]
mod command_recovery;
#[path = "support/context_recovery.rs"]
mod context_recovery;
#[path = "support/recovery_store.rs"]
mod recovery_store;
#[path = "../../wickle/tests/support/agent_resume.rs"]
#[allow(dead_code)]
mod tool_support;
use std::{
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};
use support::*;
use wickle::*;
use wickle_state_sqlite::SqliteStateStore;

// Serialize process fixtures to keep resource contention outside these tests.
static PROCESS_TESTS: std::sync::Mutex<()> = std::sync::Mutex::new(());

// Fault-boundary tests advance cross-process time explicitly. Synchronous fsync
// or scheduler stalls must not consume a one-second lease before the crash point.
// Real-time heartbeat and expiry behavior is covered by the agent/store tests.
struct ProcessClock {
    utc_ms: i64,
}
impl ProcessClock {
    fn new(replacement: bool) -> Self {
        Self {
            utc_ms: if replacement { 3000 } else { 1000 },
        }
    }
}
impl Clock for ProcessClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: self.utc_ms,
            monotonic_ms: 0,
        })
    }
    fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}

struct ProcessModel {
    inner: Arc<dyn ModelPort>,
    directory: std::path::PathBuf,
    interrupt: bool,
}
impl ModelPort for ProcessModel {
    fn binding(&self) -> ModelPortBinding {
        self.inner.binding()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        use std::io::Write;
        let mut calls = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join("calls"))
            .unwrap();
        writeln!(calls, "{}", context.attempt_id).unwrap();
        calls.sync_all().unwrap();
        std::fs::write(self.directory.join("run"), context.run_id.as_str()).unwrap();
        if self.interrupt {
            std::process::exit(73);
        }
        self.inner.generate(request, context)
    }
}
fn worker(directory: &Path, mode: &str) -> std::process::ExitStatus {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "recovery_worker", "--nocapture"])
        .env("WICKLE_RECOVERY_PROCESS_DIRECTORY", directory)
        .env("WICKLE_RECOVERY_PROCESS_MODE", mode)
        .stdin(Stdio::null())
        .spawn()
        .unwrap();
    let end = std::time::Instant::now() + Duration::from_secs(30);
    let mut killed = false;
    loop {
        if !killed
            && mode.ends_with("-interrupt")
            && std::fs::read_to_string(directory.join("kill-ready"))
                .ok()
                .as_deref()
                == Some("ready\n")
        {
            child.kill().unwrap();
            killed = true;
        }
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if std::time::Instant::now() >= end {
            let _ = child.kill();
            let _ = child.wait();
            panic!("recovery process timed out");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
#[test]
fn a_replacement_process_recovers_an_interrupted_model_with_a_new_charged_attempt() {
    let _serial = PROCESS_TESTS.lock().unwrap();
    let directory = std::env::temp_dir().join(format!(
        "wickle-agent-recovery-{}",
        RandomIdSource.next_id().unwrap()
    ));
    std::fs::create_dir(&directory).unwrap();
    assert_eq!(worker(&directory, "interrupt").code(), Some(73));
    // The replacement clock starts after the original lease's expiry.
    assert!(worker(&directory, "recover").success());
    let calls = std::fs::read_to_string(directory.join("calls")).unwrap();
    let attempts: Vec<_> = calls.lines().collect();
    assert_eq!(attempts.len(), 2);
    assert_ne!(attempts[0], attempts[1]);
    assert!(directory.join("verified").exists());
    std::fs::remove_dir_all(directory).unwrap();
}
#[test]
#[ignore = "Child process fixture requires explicit private test configuration"]
fn recovery_worker() {
    let directory =
        std::path::PathBuf::from(std::env::var_os("WICKLE_RECOVERY_PROCESS_DIRECTORY").unwrap());
    let mode = std::env::var("WICKLE_RECOVERY_PROCESS_MODE").unwrap();
    if mode.starts_with("command-") {
        command_recovery::run_worker(&directory, &mode);
        return;
    }
    if mode.starts_with("context-") {
        context_recovery::run_worker(&directory, &mode);
        return;
    }
    if mode.starts_with("tool-") {
        run_tool_worker(&directory, &mode);
        return;
    }
    let interrupt = mode == "interrupt";
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let fixture = Fixture::new(Response::Text, false);
            let store = Arc::new(SqliteStateStore::open(directory.join("state.sqlite")).unwrap());
            let mut bindings = fixture.bindings();
            bindings.state = store.clone();
            bindings.clock = Arc::new(ProcessClock::new(!interrupt));
            bindings.ids = Arc::new(RandomIdSource);
            bindings.model_exchange = Arc::new(
                ModelExchange::new(
                    Arc::new(ProcessModel {
                        inner: fixture.model.clone(),
                        directory: directory.clone(),
                        interrupt,
                    }),
                    bindings.policy.clone(),
                )
                .with_route_inspector(fixture.inspector.clone(), Duration::from_secs(1))
                .unwrap(),
            );
            let mut profile = profile();
            profile.limits.max_recovery_attempts = 2;
            profile.limits.max_elapsed_ms = 60000.try_into().unwrap();
            let agent = create_agent(profile, bindings).unwrap();
            if interrupt {
                let handle = completed(agent.start(request("request"), context()).await.unwrap());
                let _ = handle.outcome(&context()).await;
                panic!("model did not terminate process");
            }
            let run_id = id(&std::fs::read_to_string(directory.join("run")).unwrap());
            let before = store.load(&scope(), &run_id).await.unwrap();
            assert_eq!(before.snapshot.status, RunStatus::Running);
            assert!(matches!(
                before.snapshot.model_ledger[0].state,
                ModelAttemptState::Reserved {}
            ));
            // A new process cannot take ownership while the old lease is live.
            assert_eq!(
                store
                    .acquire_lease(&scope(), &run_id, &id("too-early"), 1000, 1000)
                    .await
                    .unwrap_err()
                    .code,
                ErrorCode::LeaseBusy,
            );
            let source = before.snapshot.recovery_record(id("source")).unwrap();
            let command = ResumeCommand {
                run_id: run_id.clone(),
                expected_revision: before.snapshot.revision,
                command_id: id("recover"),
                action: ResumeAction::Recover {
                    recovery_ref: source.reference().clone(),
                },
            };
            let handle = completed(agent.resume(command.clone(), context()).await.unwrap());
            let outcome = completed(handle.outcome(&context()).await.unwrap());
            assert_eq!(outcome.result.status(), RunStatus::Succeeded);
            let after = store.load(&scope(), &run_id).await.unwrap();
            assert_eq!(after.snapshot.usage.model_calls, 2);
            assert_eq!(after.snapshot.usage.recovery_attempts, 1);
            assert!(matches!(
                after.snapshot.model_ledger[0].state,
                ModelAttemptState::Interrupted { .. }
            ));
            assert_eq!(
                after.snapshot.model_ledger[0].model_step_id,
                after.snapshot.model_ledger[1].model_step_id
            );
            let replay = completed(agent.resume(command, context()).await.unwrap());
            assert_eq!(
                completed(replay.outcome(&context()).await.unwrap()),
                outcome
            );
            std::fs::write(directory.join("verified"), b"recovered and replayed").unwrap();
        });
}

struct ProcessTool {
    inner: Arc<dyn ToolExecutor>,
    name: &'static str,
    crash_on_write: bool,
    directory: std::path::PathBuf,
}
fn append(path: &Path, value: &str) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{value}").unwrap();
    file.sync_all().unwrap();
}
impl ToolExecutor for ProcessTool {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            append(&self.directory.join("tool-calls"), self.name);
            if self.name == "before" && self.crash_on_write {
                // Exceed the fixture lease in wall time, as a slow fsync can do.
                // Explicit test time must still reach the intended write/crash.
                std::thread::sleep(Duration::from_millis(1200));
            }
            if self.name != "target" {
                return self.inner.execute(args, context).await;
            }
            let path = self.directory.join("effect.json");
            // create_new makes any repeated write an observable test failure.
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path)
                .unwrap();
            file.write_all(&serde_json::to_vec(&serde_json::json!({"arguments":args,"attempt":context.attempt_id,"key":context.idempotency_key})).unwrap()).unwrap();
            file.sync_all().unwrap();
            if self.crash_on_write {
                std::process::exit(74);
            }
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: serde_json::json!("target"),
                },
                effect: ToolEffect::Applied,
                receipt: Some(serde_json::json!({"effect_id":"durable-write"})),
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolReconciliation> {
        Box::pin(async move {
            assert_eq!(self.name, "target");
            append(&self.directory.join("queries"), self.name);
            let evidence: serde_json::Value =
                serde_json::from_slice(&std::fs::read(self.directory.join("effect.json")).unwrap())
                    .unwrap();
            assert_eq!(evidence["arguments"], serde_json::to_value(args).unwrap());
            assert_eq!(
                evidence["attempt"],
                serde_json::to_value(&context.attempt_id).unwrap()
            );
            assert_eq!(
                evidence["key"],
                serde_json::to_value(&context.idempotency_key).unwrap()
            );
            Ok(ToolReconciliation::Known {
                result: ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Succeeded {
                        value: serde_json::json!("target"),
                    },
                    effect: ToolEffect::Applied,
                    receipt: Some(serde_json::json!({"effect_id":"durable-write"})),
                },
            })
        })
    }
}
#[test]
fn an_applied_write_is_reconciled_after_process_exit_or_remains_unknown_without_a_query_adapter() {
    let _serial = PROCESS_TESTS.lock().unwrap();
    for known in [true, false] {
        let directory = std::env::temp_dir().join(format!(
            "wickle-tool-recovery-{}",
            RandomIdSource.next_id().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let kind = if known { "known" } else { "unknown" };
        assert_eq!(
            worker(&directory, &format!("tool-{kind}-interrupt")).code(),
            Some(74)
        );
        let effect = std::fs::read(directory.join("effect.json")).unwrap();
        assert!(worker(&directory, &format!("tool-{kind}-recover")).success());
        assert_eq!(
            std::fs::read(directory.join("effect.json")).unwrap(),
            effect
        );
        let calls = std::fs::read_to_string(directory.join("tool-calls")).unwrap();
        assert_eq!(
            calls,
            if known {
                "before\ntarget\nafter\n"
            } else {
                "before\ntarget\n"
            }
        );
        if known {
            assert_eq!(
                std::fs::read_to_string(directory.join("queries")).unwrap(),
                "target\n"
            );
        } else {
            assert!(!directory.join("queries").exists());
        }
        assert!(directory.join("verified").exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
fn run_tool_worker(directory: &Path, mode: &str) {
    use std::sync::atomic::Ordering;
    let boundary = mode
        .strip_prefix("tool-boundary-")
        .map(|value| value.rsplit_once('-').unwrap().0);
    let known = mode.starts_with("tool-known-") || boundary.is_some();
    let interrupt = mode.ends_with("-interrupt");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut fixture = tool_support::Fixture::new(tool_support::Mode::External);
            fixture.profile.limits.max_elapsed_ms = 60000.try_into().unwrap();
            fixture.profile.limits.max_recovery_attempts = 4;
            let store = Arc::new(SqliteStateStore::open(directory.join("state.sqlite")).unwrap());
            let mut bindings = fixture.bindings();
            bindings.state = match (interrupt, boundary) {
                (true, Some(boundary)) => Arc::new(recovery_store::CrashStore {
                    inner: store.clone(),
                    boundary: boundary.into(),
                    kill_marker: matches!(
                        boundary,
                        "admitted" | "before-prepared" | "prepared" | "reserved"
                    )
                    .then(|| directory.join("kill-ready")),
                }),
                _ => store.clone(),
            };
            bindings.clock = Arc::new(ProcessClock::new(!interrupt));
            bindings.ids = Arc::new(RandomIdSource);
            if !interrupt {
                // The replacement exercises normal durable-run settings after
                // the explicit clock handoff expires the original short lease.
                let defaults = AgentSettings::default();
                bindings.settings.lease_ttl_ms = defaults.lease_ttl_ms;
                bindings.settings.heartbeat_interval_ms = defaults.heartbeat_interval_ms;
            }
            let mut registrations = vec![];
            for (index, name) in ["before", "target", "after"].into_iter().enumerate() {
                let mut descriptor = fixture
                    .registry
                    .get(&id(name))
                    .unwrap()
                    .compiled
                    .descriptor()
                    .clone();
                descriptor.reconcile = known && name == "target";
                registrations.push(ToolRegistration {
                    compiled: SchemaCompiler::new()
                        .compile(descriptor, &fixture.inputs)
                        .unwrap(),
                    executor: Arc::new(ProcessTool {
                        inner: fixture.tools[index].clone(),
                        name,
                        crash_on_write: boundary.is_none(),
                        directory: directory.to_owned(),
                    }),
                });
            }
            bindings.tools = Some(Arc::new(ToolRegistry::new(scope(), registrations).unwrap()));
            if !interrupt {
                let count = std::fs::read_to_string(directory.join("calls"))
                    .unwrap_or_default()
                    .lines()
                    .count();
                fixture.model.calls.store(count, Ordering::SeqCst);
                // A changed resolver must not replace the already saved foreign key.
                if boundary.is_none() || matches!(boundary, Some("bound" | "settled" | "terminal"))
                {
                    fixture.resolver.value.lock().unwrap().value =
                        serde_json::json!(tool_support::CHANGED_RECORD);
                }
            }
            bindings.model_exchange = Arc::new(
                ModelExchange::new(
                    Arc::new(ProcessModel {
                        inner: fixture.model.clone(),
                        directory: directory.to_owned(),
                        interrupt: false,
                    }),
                    bindings.policy.clone(),
                )
                .with_route_inspector(fixture.base.inspector.clone(), Duration::from_secs(1))
                .unwrap(),
            );
            let agent = create_agent(fixture.profile.clone(), bindings).unwrap();
            if interrupt {
                let handle = fixture.started(&agent).await;
                let outcome = handle.outcome(&context()).await;
                panic!("write did not terminate process: {outcome:?}");
            }
            let run_id = match std::fs::read_to_string(directory.join("run")) {
                Ok(value) => id(&value),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    store
                        .find_request(&scope(), &id("session"), &id("request"))
                        .await
                        .unwrap()
                        .expect("durable admission")
                        .snapshot
                        .run_id
                }
                Err(error) => panic!("run identifier: {error}"),
            };
            let before = store.load(&scope(), &run_id).await.unwrap();
            match boundary {
                Some("admitted" | "before-prepared") => {
                    assert!(before.snapshot.prepared_steps.is_empty());
                    assert!(before.snapshot.model_ledger.is_empty());
                }
                Some("prepared") => {
                    assert!(!before.snapshot.prepared_steps.is_empty());
                    assert!(before.snapshot.model_ledger.is_empty());
                }
                Some("reserved") => {
                    assert!(!before.snapshot.prepared_steps.is_empty());
                    assert_eq!(before.snapshot.model_ledger.len(), 1);
                    assert!(matches!(
                        before.snapshot.model_ledger[0].state,
                        ModelAttemptState::Reserved {}
                    ));
                }
                _ => {}
            }
            if boundary == Some("terminal") {
                assert_eq!(before.snapshot.status, RunStatus::Succeeded);
                let replay = fixture.started(&agent).await;
                assert_eq!(
                    completed(replay.outcome(&context()).await.unwrap()),
                    before.snapshot.outcome.unwrap()
                );
                std::fs::write(directory.join("verified"), b"terminal replay").unwrap();
                return;
            }
            if boundary.is_none() {
                assert!(matches!(
                    before.snapshot.tool_ledger[1].state,
                    ToolCallState::Dispatching { .. }
                ));
            }
            let source = before.snapshot.recovery_record(id("write-source")).unwrap();
            let command = ResumeCommand {
                run_id: run_id.clone(),
                expected_revision: before.snapshot.revision,
                command_id: id("recover-write"),
                action: ResumeAction::Recover {
                    recovery_ref: source.reference().clone(),
                },
            };
            let handle = completed(agent.resume(command.clone(), context()).await.unwrap());
            let outcome = completed(handle.outcome(&context()).await.unwrap());
            assert_eq!(
                outcome.result.status(),
                if known {
                    RunStatus::Succeeded
                } else {
                    RunStatus::Waiting
                }
            );
            assert_eq!(outcome.unresolved_effects.len(), usize::from(!known));
            let after = store.load(&scope(), &run_id).await.unwrap();
            if matches!(
                boundary,
                Some("admitted" | "before-prepared" | "prepared" | "reserved")
            ) {
                assert_eq!(
                    after.snapshot.usage.model_calls,
                    if boundary == Some("reserved") { 3 } else { 2 }
                );
                for prepared in &before.snapshot.prepared_steps {
                    assert!(after.snapshot.prepared_steps.contains(prepared));
                }
                if boundary == Some("reserved") {
                    assert!(matches!(
                        after.snapshot.model_ledger[0].state,
                        ModelAttemptState::Interrupted { .. }
                    ));
                }
            }

            if boundary.is_none() || matches!(boundary, Some("bound" | "settled")) {
                assert_eq!(
                    after.snapshot.tool_ledger[1].call.bound_input_ref,
                    before.snapshot.tool_ledger[1].call.bound_input_ref
                );
                assert_eq!(fixture.resolver.calls.load(Ordering::SeqCst), 0);
            }
            let replay = completed(agent.resume(command, context()).await.unwrap());
            assert_eq!(
                completed(replay.outcome(&context()).await.unwrap()),
                outcome
            );
            std::fs::write(directory.join("verified"), b"effect preserved and replayed").unwrap();
        });
}

#[test]
fn durable_plan_binding_result_and_terminal_boundaries_resume_without_repeating_completed_work() {
    let _serial = PROCESS_TESTS.lock().unwrap();
    for boundary in ["before-plan", "plan", "bound", "settled", "terminal"] {
        let directory = std::env::temp_dir().join(format!(
            "wickle-boundary-recovery-{}",
            RandomIdSource.next_id().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        assert_eq!(
            worker(&directory, &format!("tool-boundary-{boundary}-interrupt")).code(),
            Some(75),
            "{boundary}"
        );
        let calls = std::fs::read_to_string(directory.join("tool-calls")).unwrap_or_default();
        assert_eq!(
            calls,
            match boundary {
                "before-plan" | "plan" => "",
                "bound" => "before\n",
                "settled" => "before\ntarget\n",
                _ => "before\ntarget\nafter\n",
            },
            "{boundary}"
        );
        assert!(
            worker(&directory, &format!("tool-boundary-{boundary}-recover")).success(),
            "{boundary}"
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("tool-calls")).unwrap(),
            "before\ntarget\nafter\n",
            "{boundary}"
        );
        assert!(!directory.join("queries").exists(), "{boundary}");
        assert_eq!(
            std::fs::read_to_string(directory.join("calls"))
                .unwrap()
                .lines()
                .count(),
            2,
            "{boundary}"
        );
        assert!(directory.join("verified").exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn compression_revision_process_boundaries_restore_only_complete_context_without_recompression() {
    let _serial = PROCESS_TESTS.lock().unwrap();
    for boundary in ["before-context", "context"] {
        let directory = std::env::temp_dir().join(format!(
            "wickle-context-recovery-{}",
            RandomIdSource.next_id().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        assert_eq!(
            worker(&directory, &format!("context-{boundary}-interrupt")).code(),
            Some(76)
        );
        assert!(worker(&directory, &format!("context-{boundary}-recover")).success());
        assert_eq!(
            std::fs::read_to_string(directory.join("reads")).unwrap(),
            "read\nread\n"
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("summaries"))
                .unwrap()
                .lines()
                .count(),
            if boundary == "context" { 1 } else { 2 }
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("calls"))
                .unwrap()
                .lines()
                .count(),
            3
        );
        assert!(directory.join("verified").exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn forced_process_termination_preserves_admission_preparation_and_dispatch_reservations() {
    let _serial = PROCESS_TESTS.lock().unwrap();
    for boundary in ["admitted", "before-prepared", "prepared", "reserved"] {
        let directory = std::env::temp_dir().join(format!(
            "wickle-prepared-kill-{}",
            RandomIdSource.next_id().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let status = worker(&directory, &format!("tool-boundary-{boundary}-interrupt"));
        assert!(!status.success(), "{boundary}");
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(
                status.signal(),
                Some(9),
                "parent must SIGKILL the paused worker"
            );
        }
        assert_eq!(
            std::fs::read_to_string(directory.join("calls")).unwrap_or_default(),
            ""
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("tool-calls")).unwrap_or_default(),
            ""
        );
        assert!(
            worker(&directory, &format!("tool-boundary-{boundary}-recover")).success(),
            "{boundary}"
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("tool-calls")).unwrap(),
            "before\ntarget\nafter\n"
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("calls"))
                .unwrap()
                .lines()
                .count(),
            2
        );
        assert!(directory.join("verified").exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn forced_process_termination_never_half_consumes_an_input_command_or_replays_its_tool() {
    let _serial = PROCESS_TESTS.lock().unwrap();
    for boundary in ["wait", "before-command", "accepted-command"] {
        let directory = std::env::temp_dir().join(format!(
            "wickle-command-kill-{}",
            RandomIdSource.next_id().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let status = worker(&directory, &format!("command-{boundary}-interrupt"));
        assert!(!status.success());
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(9));
        }
        assert_eq!(
            std::fs::read_to_string(directory.join("tool-calls")).unwrap(),
            "before\ntarget\n"
        );
        assert!(
            worker(&directory, &format!("command-{boundary}-recover")).success(),
            "{boundary}"
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("tool-calls")).unwrap(),
            "before\ntarget\nafter\n"
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("calls"))
                .unwrap()
                .lines()
                .count(),
            2
        );
        assert!(directory.join("verified").exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
