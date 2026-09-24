// Independent CLI consumer: real SQLite and separate Host processes, synthetic model.
#[allow(dead_code)]
mod host {
    include!("agent_consumer.rs");

    // Crash-boundary verification advances lease time explicitly. A slow disk or
    // a descheduled process must not expire the lease before the intended exit.
    struct RecoveryClock(i64);
    impl Clock for RecoveryClock {
        fn now(&self) -> Result<ClockReading, ContractError> {
            Ok(ClockReading { utc_ms: self.0, monotonic_ms: 1000 })
        }
        fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
            Box::pin(std::future::pending())
        }
    }
    fn worker(directory: &std::path::Path, mode: &str) -> Result<std::process::ExitStatus, Box<dyn std::error::Error>> {
        let mut child = std::process::Command::new(std::env::current_exe()?).arg(directory).arg(mode).spawn()?;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = child.try_wait()? { return Ok(status); }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill(); let _ = child.wait();
                return Err("recovery worker exceeded its wall-time watchdog".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    struct ProcessModel {
        inner: ExampleModel,
        directory: std::path::PathBuf,
        interrupt: bool,
    }
    impl ModelPort for ProcessModel {
        fn binding(&self) -> ModelPortBinding { self.inner.binding() }
        fn generate<'a>(&'a self, request: &'a ModelRequest, context: &'a ModelCallContext) -> PortStream<'a, ModelEvent> {
            use std::io::Write;
            let mut calls = std::fs::OpenOptions::new().create(true).append(true).open(self.directory.join("calls")).unwrap();
            writeln!(calls, "{}", context.attempt_id).unwrap();
            calls.sync_all().unwrap();
            std::fs::write(self.directory.join("run"), context.run_id.as_str()).unwrap();
            if self.interrupt { std::process::exit(73); }
            self.inner.generate(request, context)
        }
    }
    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        let args: Vec<_> = std::env::args_os().collect();
        if args.len() == 3 {
            return child(std::path::Path::new(&args[1]), args[2] == "interrupt").await;
        }
        let directory = TemporaryDatabase(std::env::temp_dir().join(format!("wickle-recovery-{}", RandomIdSource.next_id()?)));
        std::fs::create_dir(&directory.0)?;
        let first = worker(&directory.0, "interrupt")?;
        assert_eq!(first.code(), Some(73));
        let second = worker(&directory.0, "recover")?;
        assert!(second.success());
        let calls = std::fs::read_to_string(directory.0.join("calls"))?;
        let attempts: Vec<_> = calls.lines().collect();
        assert_eq!(attempts.len(), 2);
        assert_ne!(attempts[0], attempts[1]);
        println!("recovery consumer: abrupt Host exit, SQLite reopen under a new owner, original model step retained, two charged physical attempts, accepted command replay without another call (synthetic model, no provider network)");
        Ok(())
    }
    async fn child(directory: &std::path::Path, interrupt: bool) -> Result<(), Box<dyn std::error::Error>> {
        let scope = Scope { tenant_id: id("tenant"), workspace_id: id("workspace"), user_id: None };
        let store = Arc::new(SqliteStateStore::open(directory.join("state.sqlite"))?);
        let routing = routing_snapshot(&scope)?;
        let model = Arc::new(ProcessModel { inner: ExampleModel { route: routing.route_for_binding(&reference("first"))?, calls: AtomicUsize::new(0), fail: false }, directory: directory.to_owned(), interrupt });
        let policy = Arc::new(PolicyGate::new(Arc::new(ExamplePolicy), Duration::from_secs(1))?);
        let profile = AgentProfile::from_json(r#"{
            "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
            "name":"Assistant","description":"Recovery consumer","instructions":{"text":"Use supplied information"},
            "model_binding":"primary","tools":[],"skills":[],"connectors":[],
            "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
            "limits":{"max_model_calls":3,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":2,"max_elapsed_ms":60000}
        }"#)?;
        let mut settings = AgentSettings { require_durable: true, max_output_tokens: 128.try_into()?, ..AgentSettings::default() };
        if interrupt { settings.lease_ttl_ms = 1000; settings.heartbeat_interval_ms = 100; }
        let agent = create_agent(profile, AgentBindings {
            scope: scope.clone(), state: store.clone(), policy: policy.clone(), profile_resolver: Arc::new(Catalog),
            model_exchange: Arc::new(ModelExchange::new(model, policy).with_route_inspector(Arc::new(ExampleInspector), Duration::from_secs(1))?),
            router: Arc::new(PolicyModelRouter::new(routing)?), host_instructions: vec!["Preserve the requested output.".into()],
            system_inputs: SystemInputRegistry::new(vec![])?, clock: Arc::new(RecoveryClock(if interrupt { 1000 } else { 3000 })), ids: Arc::new(RandomIdSource),
            tools: None, system_input_resolver: None, external_receipt_verifier: None, components: None,
            context_sources: None, context_token_estimator: None, context_runtime: None, verification: None, skills: None, artifacts: None, hooks: None,
            token_estimator: Arc::new(Estimate), settings,
        })?;
        let context = ExecutionContext::new(ExecutionContextData { scope: scope.clone(), principal_ref: id("actor"), capability_grant_ref: id("grant"), trace_context: None, system_inputs: None }, Default::default());
        if interrupt {
            let request = RunRequest { request_id: id("request"), session_id: id("session"), input: vec![InputContent::Text { text: "Retrieve the available result".into() }], trigger: RunTrigger::User {}, model_options: JsonObject::from([("reasoning_effort".into(), json!("high"))]), output_contract: None };
            let handle = completed(agent.start(request, context.clone()).await?)?;
            // Regression: wall-time delay exceeds the fixture's short lease.
            std::thread::sleep(Duration::from_millis(1200));
            let outcome = handle.outcome(&context).await;
            return Err(format!("interruption did not occur: {outcome:?}").into());
        }
        let run_id = id(&std::fs::read_to_string(directory.join("run"))?);
        assert_eq!(store.acquire_lease(&scope, &run_id, &id("too-early"), 1000, 1000).await.unwrap_err().code, ErrorCode::LeaseBusy);
        let before = completed(agent.get_run_details(&run_id, &context).await?)?;
        let source = before.recovery_record(RandomIdSource.next_id()?)?;
        let command = ResumeCommand { run_id: run_id.clone(), expected_revision: before.revision, command_id: RandomIdSource.next_id()?, action: ResumeAction::Recover { recovery_ref: source.reference().clone() } };
        let handle = completed(agent.resume(command.clone(), context.clone()).await?)?;
        let result = completed(handle.outcome(&context).await?)?;
        assert_eq!(result.result.status(), RunStatus::Succeeded);
        assert_eq!(result.usage.model_calls, 2);
        assert_eq!(result.usage.recovery_attempts, 1);
        let saved = store.load(&scope, &run_id).await?;
        assert!(matches!(saved.snapshot.model_ledger[0].state, ModelAttemptState::Interrupted { .. }));
        assert_eq!(saved.snapshot.model_ledger[0].model_step_id, saved.snapshot.model_ledger[1].model_step_id);
        let replay = completed(agent.resume(command, context.clone()).await?)?;
        assert_eq!(completed(replay.outcome(&context).await?)?, result);
        Ok(())
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> { host::run().await }
