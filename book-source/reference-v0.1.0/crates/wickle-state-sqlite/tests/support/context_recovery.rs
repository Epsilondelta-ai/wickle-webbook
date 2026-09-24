//! Real compression and SQLite checkpoints on both sides of an abrupt process exit.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Model(AtomicUsize);
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
        _: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let index = self.0.fetch_add(1, Ordering::SeqCst);
        let (event, finish) = if index < 2 {
            (
                ModelEvent::ToolArgumentsDelta {
                    index: 0,
                    provider_call_id: Some(format!("read-{index}")),
                    name: Some("before".into()),
                    delta: "{\"query\":\"chunk\"}".into(),
                },
                ModelFinish::ToolCalls,
            )
        } else {
            (
                ModelEvent::TextDelta {
                    text: "The saved records were read.".into(),
                },
                ModelFinish::Stop,
            )
        };
        Box::pin(futures_util::stream::iter([
            Ok(event),
            Ok(ModelEvent::ResponseCompleted {
                finish,
                metadata: Default::default(),
                continuation: vec![],
            }),
        ]))
    }
}
struct Read(std::path::PathBuf);
impl ToolExecutor for Read {
    fn execute<'a>(
        &'a self,
        _: &'a JsonObject,
        _: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            append(&self.0.join("reads"), "read");
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: serde_json::json!("x".repeat(3500)),
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}
struct Summary(std::path::PathBuf);
impl HostContextCompactor for Summary {
    fn compact<'a>(
        &'a self,
        request: &'a CompactionRequest,
        _: &'a ContextStrategyContext,
    ) -> PortFuture<'a, String> {
        Box::pin(async move {
            assert!(!request.segments.is_empty());
            append(&self.0.join("summaries"), "summary");
            Ok("Older complete records were read; their observations remain in storage.".into())
        })
    }
}

pub fn run_worker(directory: &Path, mode: &str) {
    let interrupt = mode.ends_with("-interrupt");
    let committed = !mode.starts_with("context-before-");
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
            bindings.state = if interrupt {
                Arc::new(recovery_store::CrashStore {
                    inner: store.clone(),
                    boundary: if committed {
                        "context"
                    } else {
                        "before-context"
                    }
                    .into(),
                })
            } else {
                store.clone()
            };
            bindings.clock = Arc::new(ProcessClock::new(!interrupt));
            bindings.ids = Arc::new(RandomIdSource);
            bindings.settings.projection_limits.max_bytes = 8000;
            if !interrupt {
                bindings.settings.lease_ttl_ms = 30000;
                bindings.settings.heartbeat_interval_ms = 5000;
            }
            let mut descriptor = fixture
                .registry
                .get(&id("before"))
                .unwrap()
                .compiled
                .descriptor()
                .clone();
            descriptor.max_output_bytes = 65536.try_into().unwrap();
            bindings.tools = Some(Arc::new(
                ToolRegistry::new(
                    scope(),
                    vec![
                        ToolRegistration {
                            compiled: SchemaCompiler::new()
                                .compile(descriptor, &fixture.inputs)
                                .unwrap(),
                            executor: Arc::new(Read(directory.to_owned())),
                        },
                        fixture.registry.get(&id("target")).unwrap().clone(),
                        fixture.registry.get(&id("after")).unwrap().clone(),
                    ],
                )
                .unwrap(),
            ));
            bindings.context_runtime = Some(Arc::new(
                ContextRuntime::new(
                    scope(),
                    Arc::new(BoundedContextStrategy),
                    Some(ContextCompactor::Host {
                        definition: reference("summary"),
                        compressor: Arc::new(Summary(directory.to_owned())),
                    }),
                    ContextRewriteLimits::default(),
                )
                .unwrap(),
            ));
            let count = if interrupt {
                0
            } else {
                std::fs::read_to_string(directory.join("calls"))
                    .unwrap()
                    .lines()
                    .count()
            };
            bindings.model_exchange = Arc::new(
                ModelExchange::new(
                    Arc::new(ProcessModel {
                        inner: Arc::new(Model(AtomicUsize::new(count))),
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
                let result = handle.outcome(&context()).await;
                panic!("compression did not terminate process: {result:?}");
            }
            let run_id = id(&std::fs::read_to_string(directory.join("run")).unwrap());
            let before = store.load(&scope(), &run_id).await.unwrap();
            assert_eq!(before.snapshot.status, RunStatus::Running);
            assert_eq!(before.snapshot.context_revision_ref.is_some(), committed);
            assert_eq!(before.snapshot.context_decisions.is_empty(), !committed);
            assert_eq!(before.snapshot.usage.model_calls, 2);
            assert_eq!(before.snapshot.usage.tool_attempts, 2);
            let previous_revision = if let Some(reference) = &before.snapshot.context_revision_ref {
                Some(store.read_record(&scope(), reference).await.unwrap())
            } else {
                None
            };
            let source = before
                .snapshot
                .recovery_record(id("context-source"))
                .unwrap();
            let handle = completed(
                agent
                    .resume(
                        ResumeCommand {
                            run_id: run_id.clone(),
                            expected_revision: before.snapshot.revision,
                            command_id: id("recover-context"),
                            action: ResumeAction::Recover {
                                recovery_ref: source.reference().clone(),
                            },
                        },
                        context(),
                    )
                    .await
                    .unwrap(),
            );
            assert_eq!(
                completed(handle.outcome(&context()).await.unwrap())
                    .result
                    .status(),
                RunStatus::Succeeded
            );
            let after = store.load(&scope(), &run_id).await.unwrap();
            assert_eq!(after.snapshot.profile, before.snapshot.profile);
            assert_eq!(after.snapshot.system_inputs, before.snapshot.system_inputs);
            assert_eq!(after.snapshot.limits, before.snapshot.limits);
            assert_eq!(after.snapshot.usage.model_calls, 3);
            assert_eq!(after.snapshot.usage.tool_attempts, 2);
            assert_eq!(
                after.snapshot.usage.recovery_attempts,
                before.snapshot.usage.recovery_attempts + 1
            );
            for message in &before.messages {
                assert!(after.messages.contains(message));
            }
            for reservation in &before.snapshot.reservations {
                assert!(after.snapshot.reservations.contains(reservation));
            }
            let reference = after
                .snapshot
                .context_revision_ref
                .as_ref()
                .expect("a complete compressed revision is adopted");
            let revision = store.read_record(&scope(), reference).await.unwrap();
            if let Some(previous) = previous_revision {
                assert_eq!(
                    before.snapshot.context_revision_ref.as_ref(),
                    Some(reference)
                );
                assert_eq!(revision, previous);
                assert_eq!(
                    after.snapshot.context_decisions,
                    before.snapshot.context_decisions
                );
            }
            assert!(!after.snapshot.context_decisions.is_empty());
            std::fs::write(directory.join("verified"), b"complete context restored").unwrap();
        });
}
