//! Full core loop with a real stdio server: restored arguments and durable effects.
#[path = "../../wickle/tests/support/agent.rs"]
#[allow(dead_code)]
mod core_host;
#[allow(dead_code)]
mod support;
use core_host::{completed, id, reference, scope};
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;
use wickle_mcp::*;

struct WireCompiler;
impl ProviderToolSchemaCompiler for WireCompiler {
    fn reference(&self) -> VersionedRef {
        reference("fixture-wire")
    }
    fn compile(
        &self,
        tool: &ModelTool,
        _: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        Ok(ProviderToolProjection {
            wire_tool: ModelTool {
                name: tool.name.clone(),
                description: tool.description.clone(),
                model_input_schema: json!({"type":"object","properties":{"query_json":{"type":"string"},"limit":{"type":"object","properties":{"present":{"type":"boolean"},"value":{"type":["integer","null"]}},"required":["present","value"],"additionalProperties":false}},"required":["query_json","limit"],"additionalProperties":false}),
            },
            decode_plan: ArgumentDecodePlan::Fields {
                fields: vec![
                    ArgumentFieldMapping {
                        wire_name: "query_json".into(),
                        canonical_name: "query".into(),
                        encoding: ArgumentValueEncoding::JsonText { optional: false },
                    },
                    ArgumentFieldMapping {
                        wire_name: "limit".into(),
                        canonical_name: "limit".into(),
                        encoding: ArgumentValueEncoding::Presence {
                            present_key: "present".into(),
                            value_key: "value".into(),
                        },
                    },
                ],
            },
        })
    }
}
struct Model {
    calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
    arguments: Vec<String>,
}
impl ModelPort for Model {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: id("fixture"),
            adapter: reference("adapter"),
            connection_ref: reference("connection"),
        }
    }
    fn tool_schema_compiler(&self) -> Arc<dyn ProviderToolSchemaCompiler> {
        Arc::new(WireCompiler)
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        let (event, finish) = match self.arguments.get(index) {
            Some(raw) => (
                ModelEvent::ToolArgumentsDelta {
                    index: 0,
                    provider_call_id: Some(format!("call-{index}")),
                    name: Some("search".into()),
                    delta: raw.clone(),
                },
                ModelFinish::ToolCalls,
            ),
            None => (
                ModelEvent::TextDelta {
                    text: "complete".into(),
                },
                ModelFinish::Stop,
            ),
        };
        Box::pin(futures_util::stream::iter(vec![
            Ok(event),
            Ok(ModelEvent::ResponseCompleted {
                finish,
                metadata: Default::default(),
                continuation: vec![],
            }),
        ]))
    }
}
struct Catalog(core_host::Catalog);
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        selected: &'a ComponentRef,
        scope: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            let mut metadata = self.0.resolve(selected, scope).await?;
            if selected.kind == ComponentKind::Tool {
                metadata.model_name = Some(selected.id.clone());
                metadata.reference.version = selected.version.clone();
            }
            Ok(metadata)
        })
    }
}
struct Fixture {
    base: core_host::Fixture,
    model: Arc<Model>,
    client: McpClient,
    agent: Agent,
    context: ExecutionContext,
}
impl Fixture {
    async fn new(
        directory: &support::Directory,
        mode: &str,
        write: bool,
        arguments: Vec<String>,
    ) -> Self {
        let client = support::connect(directory, mode, McpLimits::default()).await;
        let snapshot = support::snapshot(&client).await;
        let remote = if write { "db.write" } else { "db.query" };
        let compiled = support::compiled(
            &snapshot,
            remote,
            if write {
                ToolSideEffect::Write
            } else {
                ToolSideEffect::ReadOnly
            },
        );
        let selected = compiled.descriptor().tool.clone();
        let executor = client
            .bind_tool(&snapshot, remote, compiled.clone())
            .unwrap();
        let base = core_host::Fixture::new(core_host::Response::Text, false);
        let model = Arc::new(Model {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(vec![]),
            arguments,
        });
        let mut bindings = base.bindings();
        let mut catalog = base.router.snapshot.catalog().clone();
        catalog.models[0]
            .capabilities
            .features
            .insert(id("tool_calling"));
        catalog.bindings[0].capabilities = catalog.models[0].capabilities.clone();
        catalog.bindings[0].evidence[0].binding_digest = catalog.bindings[0]
            .contract_digest(&catalog.models[0])
            .unwrap();
        bindings.router = Arc::new(core_host::Router {
            snapshot: RoutingSnapshot::new(catalog, base.router.snapshot.policy().clone()).unwrap(),
            queries: AtomicUsize::new(0),
            snapshots: AtomicUsize::new(0),
        });
        bindings.profile_resolver = Arc::new(Catalog(core_host::Catalog::default()));
        bindings.model_exchange = Arc::new(
            ModelExchange::new(model.clone(), bindings.policy.clone())
                .with_route_inspector(base.inspector.clone(), Duration::from_secs(1))
                .unwrap(),
        );
        bindings.system_inputs = support::registry();
        bindings.tools = Some(Arc::new(
            ToolRegistry::new(
                scope(),
                vec![ToolRegistration {
                    compiled,
                    executor: Arc::new(executor),
                }],
            )
            .unwrap(),
        ));
        let mut profile = core_host::profile();
        profile.limits.max_model_calls = 7.try_into().unwrap();
        profile.limits.max_repair_attempts = 4;
        profile.limits.max_tool_attempts = 2;
        profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
            tool_id: selected.id,
            version: selected.version,
            bindings: None,
            config: None,
        }));
        let agent = create_agent(profile, bindings).unwrap();
        let mut context = core_host::context();
        context.data.system_inputs = Some(SystemInputs::new(JsonObject::from([
            ("workspace_id".into(), json!(support::WORKSPACE)),
            ("unused_key".into(), json!("unused-system-secret")),
        ])));
        Self {
            base,
            model,
            client,
            agent,
            context,
        }
    }
    async fn start(&self) -> RunHandle {
        completed(
            self.agent
                .start(core_host::request("contract"), self.context.clone())
                .await
                .unwrap(),
        )
    }
    async fn outcome(&self, handle: &RunHandle) -> RunOutcome {
        completed(
            tokio::time::timeout(Duration::from_secs(10), handle.outcome(&self.context))
                .await
                .unwrap()
                .unwrap(),
        )
    }
}
fn valid() -> String {
    json!({"query_json":"\"latest\"","limit":{"present":false,"value":null}}).to_string()
}
#[tokio::test]
async fn repaired_wire_arguments_are_restored_and_bound_before_one_remote_call() {
    let directory = support::Directory::new();
    let fixture=Fixture::new(&directory,"normal",false,vec![
        "{not json".into(),
        json!({"query_json":"\"latest\"","limit":{"present":false,"value":null},"workspace_id":"forged"}).to_string(),
        json!({"query_json":"not json","limit":{"present":false,"value":null}}).to_string(),
        json!({"query_json":"\"latest\"","limit":{"present":true,"value":"wrong type"}}).to_string(),
        valid(),
    ]).await;
    let handle = fixture.start().await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(
        outcome.result.status(),
        RunStatus::Succeeded,
        "{:?}",
        outcome
    );
    assert_eq!(outcome.usage.repair_attempts, 4);
    assert_eq!(outcome.usage.tool_attempts, 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 6);
    assert_eq!(support::call_count(&directory), 1);
    let records = support::records(&directory);
    let call = records
        .iter()
        .find(|record| record.get("call").is_some())
        .unwrap();
    assert_eq!(call["call"], "db.query");
    assert_eq!(
        call["args"],
        json!({"query":"latest","limit":5,"workspace_id":support::WORKSPACE})
    );
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    let entry = saved.snapshot.tool_ledger.last().unwrap();
    assert_eq!(
        entry.call.model_inputs,
        JsonObject::from([("query".into(), json!("latest"))])
    );
    let ToolCallState::Settled { result } = &entry.state else {
        panic!("call not settled")
    };
    assert_eq!(result.status, ToolResultStatus::Succeeded);
    assert_eq!(result.effect, ToolEffect::NotApplied);
    assert_eq!(
        result.content,
        vec![InputContent::Json {
            value: json!({"answer":42})
        }]
    );
    for request in fixture.model.requests.lock().unwrap().iter() {
        let text = serde_json::to_string(request).unwrap();
        assert!(!text.contains(support::WORKSPACE));
        assert!(!text.contains("unused-system-secret"));
        assert!(
            request.tools[0].model_input_schema["properties"]
                .get("workspace_id")
                .is_none()
        );
    }
    let duplicate = fixture.start().await;
    assert_eq!(duplicate.run_id(), handle.run_id());
    fixture.outcome(&duplicate).await;
    assert_eq!(support::call_count(&directory), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 6);
    support::close(&fixture.client).await;
}
#[tokio::test]
async fn lost_or_cancelled_remote_writes_preserve_unknown_effects_without_replay() {
    for mode in ["exit_write", "hang_write"] {
        let directory = support::Directory::new();
        let fixture = Fixture::new(&directory, mode, true, vec![valid()]).await;
        let handle = fixture.start().await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while std::fs::read_to_string(directory.0.join("calls.jsonl.effect"))
                .ok()
                .as_deref()
                != Some("applied\n")
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        if mode == "hang_write" {
            completed(handle.cancel(id("stop"), &fixture.context).await.unwrap());
        }
        let outcome = fixture.outcome(&handle).await;
        assert_eq!(
            outcome.result.status(),
            if mode == "hang_write" {
                RunStatus::Cancelled
            } else {
                RunStatus::Waiting
            }
        );
        assert!(!outcome.unresolved_effects.is_empty());
        let saved = fixture
            .base
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap();
        assert!(matches!(
            saved.snapshot.tool_ledger[0].state,
            ToolCallState::Unknown { .. }
        ));
        let duplicate = fixture.start().await;
        fixture.outcome(&duplicate).await;
        assert_eq!(support::call_count(&directory), 1);
        assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            std::fs::read_to_string(directory.0.join("calls.jsonl.effect")).unwrap(),
            "applied\n"
        );
        let pid = support::latest_pid(&directory);
        tokio::time::timeout(Duration::from_secs(3), async {
            while support::process_alive(pid) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("abandoned MCP call must clean up its child without Host close");
        // Closing again only checks idempotent shutdown, not automatic cleanup.
        support::close(&fixture.client).await;
        assert!(!support::process_alive(pid));
    }
}
