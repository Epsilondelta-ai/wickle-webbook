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
        // The Run budget includes both process lifetimes and the approval gap.
        // Each child has a separate 30-second wall watchdog below; this is a
        // recovery-contract example, not a 30-second end-to-end latency test.
        let profile = AgentProfile::from_json(
            r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"writer","version":"1",
        "name":"Writer","description":"Synthetic adapter consumer","instructions":{"text":"Write the report after authorization"},
        "model_binding":"primary","tools":[{"adapter_binding":"reports","export_id":"save","alias":"write"}],"skills":[],
        "connectors":[{"binding_id":"data","connector_id":"report-service","version":"1"}],
        "adapters":[{"binding_id":"reports","adapter_id":"report-adapter","version":"1","connections":{"main":"data"}}],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":3,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":120000}
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
