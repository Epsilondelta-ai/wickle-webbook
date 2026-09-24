//! Actual HTTP/SSE through the core: provider projection, repair and system binding.
#[path = "../../wickle/tests/support/agent.rs"]
#[allow(dead_code)]
mod core_host;
#[allow(dead_code)]
mod support;
use core_host::{completed, id, reference, scope};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use wickle::*;
use wickle_model_anthropic::*;
const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";

struct Catalog(core_host::Catalog);
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        scope: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            let mut metadata = self.0.resolve(reference, scope).await?;
            if reference.kind == ComponentKind::Tool {
                metadata.model_name = Some(reference.id.clone());
            }
            if let Some(version) = &reference.version {
                metadata.reference.version = Some(version.clone());
            }
            Ok(metadata)
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
        Box::pin(async move {
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: ModelRouteAvailability::Available,
                model_id: Some(route.model_id.clone()),
                model_version: Some(route.model_version.clone()),
                deployment_revision: None,
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id("local-http-fixture"),
            })
        })
    }
}
struct Capture(Mutex<Vec<JsonObject>>);
impl ToolExecutor for Capture {
    fn execute<'a>(
        &'a self,
        call: &'a JsonObject,
        _: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            self.0.lock().unwrap().push(call.clone());
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded {
                    value: json!("observed"),
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}
fn call_events(index: usize, arguments: &str) -> Vec<Value> {
    let mut data = support::events("claude-opus-5", "");
    data.truncate(4);
    data[0]["message"]["id"] = json!(format!("msg_{index}"));
    data.extend([
        json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":format!("call_{index}"),"name":"lookup","input":{}}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":arguments}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":20}}),
        json!({"type":"message_stop"}),
    ]);
    data
}
#[tokio::test]
async fn native_schema_constraints_and_invalid_arguments_repair_before_scoped_execution() {
    let invalid_constraint =
        json!({"query":"latest","limit":7,"note":null,"filter":{"category":"finance"}}).to_string();
    let invalid_json =
        json!({"query":"latest","limit":9,"note":null,"filter":"invalid-shape"}).to_string();
    let valid =
        json!({"query":"latest","limit":9,"note":null,"filter":{"category":"finance"}}).to_string();
    let malformed = "{not json";
    let rounded = r#"{"query":"latest","limit":0.12345678901234567890123456789}"#;
    let server = support::Server::new(vec![
        support::Reply::sse(&call_events(0, malformed)),
        support::Reply::sse(&call_events(4, rounded)),
        support::Reply::sse(&call_events(1, &invalid_constraint)),
        support::Reply::sse(&call_events(2, &invalid_json)),
        support::Reply::sse(&call_events(3, &valid)),
        support::Reply::sse(&support::events("claude-opus-5", "complete")),
    ])
    .await;
    let connection = AnthropicConnection::new(
        scope(),
        reference("account"),
        "fixture-key-not-a-secret",
        AnthropicOptions {
            base_url: server.base.trim_end_matches("v1/").into(),
            ..Default::default()
        },
    )
    .unwrap();
    let fixture = core_host::Fixture::new(core_host::Response::Text, false);
    let mut catalog = fixture.router.snapshot.catalog().clone();
    catalog.models[0].provider = id("anthropic");
    catalog.models[0].model_id = id("claude-opus-5");
    catalog.models[0]
        .capabilities
        .features
        .insert(id("tool_calling"));
    catalog.bindings[0].model = catalog.models[0].reference();
    catalog.bindings[0].requested_model = id("claude-opus-5");
    catalog.bindings[0].adapter = connection.binding().adapter;
    catalog.bindings[0].connection_ref = connection.binding().connection_ref;
    catalog.bindings[0].api_contract = AnthropicConnection::api_contract();
    catalog.bindings[0].target = connection.target().clone();
    catalog.bindings[0].target_schema = json!({"type":"object","properties":{"base_url":{"type":"string"}},"required":["base_url"],"additionalProperties":false});
    catalog.bindings[0].capabilities = catalog.models[0].capabilities.clone();
    catalog.bindings[0].evidence[0].binding_digest = catalog.bindings[0]
        .contract_digest(&catalog.models[0])
        .unwrap();
    let snapshot = RoutingSnapshot::new(catalog, fixture.router.snapshot.policy().clone()).unwrap();
    let mut bindings = fixture.bindings();
    bindings.profile_resolver = Arc::new(Catalog(core_host::Catalog::default()));
    bindings.router = Arc::new(core_host::Router {
        snapshot,
        queries: AtomicUsize::new(0),
        snapshots: AtomicUsize::new(0),
    });
    bindings.model_exchange = Arc::new(
        ModelExchange::new(
            Arc::new(AnthropicModel::new(connection)),
            bindings.policy.clone(),
        )
        .with_route_inspector(Arc::new(Inspector), Duration::from_secs(1))
        .unwrap(),
    );
    bindings.system_inputs = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    let tool = SchemaCompiler::new().compile(ToolDescriptor {
        tool: reference("lookup"), name: id("lookup"), description: "Read scoped data".into(),
        input_schema: json!({"type":"object","properties":{
            "query":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":10,"default":7},"note":{"type":["string","null"]},
            "filter":{"type":"object","properties":{"category":{"type":"string"},"term":{"type":"string"}},"required":["category"],"additionalProperties":false},
            "workspace_id":{"type":"string","format":"uuid"}
        },"required":["query","workspace_id"],"additionalProperties":false,"if":{"properties":{"query":{"const":"latest"}}},"then":{"properties":{"limit":{"minimum":8}}}}),
        agent_parameters: vec!["query".into(),"limit".into(),"note".into(),"filter".into()], system_bindings: None,
        output_schema: json!({"type":"string"}), side_effect: ToolSideEffect::ReadOnly, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false, max_output_bytes: 4096.try_into().unwrap(),
    }, &bindings.system_inputs).unwrap();
    let capture = Arc::new(Capture(Mutex::new(vec![])));
    bindings.tools = Some(Arc::new(
        ToolRegistry::new(
            scope(),
            vec![ToolRegistration {
                compiled: tool,
                executor: capture.clone(),
            }],
        )
        .unwrap(),
    ));
    let mut profile = core_host::profile();
    profile.limits.max_model_calls = 7.try_into().unwrap();
    profile.limits.max_tool_attempts = 2;
    profile.limits.max_repair_attempts = 4;
    profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
        tool_id: id("lookup"),
        version: id("1"),
        bindings: None,
        config: None,
    }));
    let agent = create_agent(profile, bindings).unwrap();
    let mut context = core_host::context();
    context.data.system_inputs = Some(SystemInputs::new(
        [("workspace_id".into(), json!(WORKSPACE))].into(),
    ));
    let handle = completed(
        agent
            .start(core_host::request("contract"), context.clone())
            .await
            .unwrap(),
    );
    let outcome = completed(
        tokio::time::timeout(Duration::from_secs(10), handle.outcome(&context))
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        outcome.result.status(),
        RunStatus::Succeeded,
        "{:?}",
        outcome.result
    );
    assert_eq!(outcome.usage.repair_attempts, 4);
    assert_eq!(outcome.usage.tool_attempts, 1);
    assert_eq!(
        capture.0.lock().unwrap().as_slice(),
        &[JsonObject::from([
            ("query".into(), json!("latest")),
            ("limit".into(), json!(9)),
            ("note".into(), Value::Null),
            ("filter".into(), json!({"category":"finance"})),
            ("workspace_id".into(), json!(WORKSPACE))
        ])]
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 6);
    for request in requests.iter() {
        assert_eq!(
            request.body["tools"][0]["input_schema"]["required"],
            json!(["query"])
        );
        assert_eq!(request.path, "/v1/messages");
        assert!(!request.body.to_string().contains(WORKSPACE));
    }
    let content: Vec<_> = requests[5].body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|message| message["content"].as_array().unwrap())
        .collect();
    let replayed: Vec<_> = content
        .iter()
        .filter(|item| item["type"] == "tool_use")
        .map(|item| item["input"].clone())
        .collect();
    assert_eq!(
        replayed,
        vec![
            json!({"INVALID_JSON":malformed}),
            json!({"INVALID_JSON":rounded}),
            parse_json(&invalid_constraint).unwrap(),
            parse_json(&invalid_json).unwrap(),
            parse_json(&valid).unwrap()
        ]
    );
    let failures: Vec<_> = content
        .iter()
        .filter(|item| item["type"] == "tool_result" && item["is_error"] == true)
        .collect();
    assert_eq!(failures.len(), 2);
    for (result, raw) in failures.into_iter().zip([malformed, rounded]) {
        assert_eq!(
            parse_json(result["content"].as_str().unwrap()).unwrap()["INVALID_JSON"],
            raw
        );
    }
    let thinking: Vec<_> = content
        .iter()
        .filter(|item| item["type"] == "thinking")
        .collect();
    assert_eq!(thinking.len(), 5);
    assert!(
        thinking
            .iter()
            .all(|block| block["signature"] == "signature-fixture")
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
}
