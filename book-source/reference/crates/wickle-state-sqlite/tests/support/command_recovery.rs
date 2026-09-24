//! Actual kill/restart at saved wait and atomic input-command/segment boundaries.
use super::*;
use serde_json::json;
use std::sync::atomic::Ordering;
struct LoggedTool {
    inner: Arc<dyn ToolExecutor>,
    name: &'static str,
    directory: std::path::PathBuf,
}
impl ToolExecutor for LoggedTool {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            append(&self.directory.join("tool-calls"), self.name);
            self.inner.execute(args, context).await
        })
    }
}
pub fn run_worker(directory: &Path, mode: &str) {
    let interrupt = mode.ends_with("-interrupt");
    let boundary = mode
        .strip_prefix("command-")
        .unwrap()
        .rsplit_once('-')
        .unwrap()
        .0;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut fixture = tool_support::Fixture::new(tool_support::Mode::Input);
            fixture.profile.limits.max_elapsed_ms = 60000.try_into().unwrap();
            fixture.profile.limits.max_recovery_attempts = 4;
            let store = Arc::new(SqliteStateStore::open(directory.join("state.sqlite")).unwrap());
            let mut bindings = fixture.bindings();
            bindings.state = if interrupt {
                Arc::new(recovery_store::CrashStore {
                    inner: store.clone(),
                    boundary: boundary.into(),
                    kill_marker: Some(directory.join("kill-ready")),
                })
            } else {
                store.clone()
            };
            bindings.clock = Arc::new(ProcessClock::new(!interrupt));
            bindings.ids = Arc::new(RandomIdSource);
            let registrations = ["before", "target", "after"]
                .into_iter()
                .enumerate()
                .map(|(index, name)| ToolRegistration {
                    compiled: fixture.registry.get(&id(name)).unwrap().compiled.clone(),
                    executor: Arc::new(LoggedTool {
                        inner: fixture.tools[index].clone(),
                        name,
                        directory: directory.to_owned(),
                    }),
                })
                .collect();
            bindings.tools = Some(Arc::new(ToolRegistry::new(scope(), registrations).unwrap()));
            if !interrupt {
                fixture.model.calls.store(
                    std::fs::read_to_string(directory.join("calls"))
                        .unwrap()
                        .lines()
                        .count(),
                    Ordering::SeqCst,
                );
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
                let outcome = completed(handle.outcome(&context()).await.unwrap());
                assert_eq!(outcome.result.status(), RunStatus::Waiting);
            }
            let saved = store
                .find_request(&scope(), &id("session"), &id("request"))
                .await
                .unwrap()
                .unwrap();
            let run_id = saved.snapshot.run_id.clone();
            let before = store.read_execution(&scope(), &run_id).await.unwrap();
            let old_wait = before.segments[0].outcome.clone();
            let command_path = directory.join("answer.json");
            let answer: ResumeCommand = match std::fs::read_to_string(&command_path) {
                Ok(text) => serde_json::from_str(&text).unwrap(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let command = ResumeCommand {
                        run_id: run_id.clone(),
                        expected_revision: saved.snapshot.revision,
                        command_id: id("answer"),
                        action: ResumeAction::Input {
                            wait_id: saved.snapshot.wait.as_ref().unwrap().wait_id.clone(),
                            answer: json!({"selection":"annual"}),
                        },
                    };
                    std::fs::write(&command_path, serde_json::to_vec(&command).unwrap()).unwrap();
                    command
                }
                Err(error) => panic!("saved command: {error}"),
            };
            if !interrupt {
                assert_eq!(
                    before.accepted_commands.len(),
                    usize::from(boundary == "accepted-command")
                );
                assert_eq!(
                    before.segments.len(),
                    if boundary == "accepted-command" { 2 } else { 1 }
                );
            }
            let handle = if !interrupt && boundary == "accepted-command" {
                let source = saved
                    .snapshot
                    .recovery_record(id("command-recovery-source"))
                    .unwrap();
                completed(
                    agent
                        .resume(
                            ResumeCommand {
                                run_id: run_id.clone(),
                                expected_revision: saved.snapshot.revision,
                                command_id: id("recover-answer"),
                                action: ResumeAction::Recover {
                                    recovery_ref: source.reference().clone(),
                                },
                            },
                            context(),
                        )
                        .await
                        .unwrap(),
                )
            } else {
                completed(agent.resume(answer.clone(), context()).await.unwrap())
            };
            let outcome = completed(handle.outcome(&context()).await.unwrap());
            if interrupt {
                panic!("command boundary failed to pause");
            }
            assert_eq!(
                outcome.result.status(),
                RunStatus::Succeeded,
                "{:?}",
                outcome
            );
            let finished = store.load(&scope(), &run_id).await.unwrap();
            let target = &finished.snapshot.tool_ledger[1];
            let ToolCallState::Settled { result } = &target.state else {
                panic!("input not settled")
            };
            assert_eq!(
                result.content,
                vec![InputContent::Json {
                    value: json!({"selection":"annual"})
                }]
            );
            let history = store.read_execution(&scope(), &run_id).await.unwrap();
            assert_eq!(history.segments[0].outcome, old_wait);
            assert_eq!(
                finished
                    .snapshot
                    .resume_receipts
                    .iter()
                    .filter(|receipt| receipt.command.command_id == id("answer"))
                    .count(),
                1
            );
            let checkpoint = store.load(&scope(), &run_id).await.unwrap();
            let duplicate = completed(agent.resume(answer, context()).await.unwrap());
            let _old_interval = completed(duplicate.outcome(&context()).await.unwrap());
            assert_eq!(store.load(&scope(), &run_id).await.unwrap(), checkpoint);
            std::fs::write(
                directory.join("verified"),
                b"input command atomically consumed without reexecution",
            )
            .unwrap();
        });
}
