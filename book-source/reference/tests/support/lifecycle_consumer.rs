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
                    interruption_policy: None,
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
                    max_output_tokens: None,
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
