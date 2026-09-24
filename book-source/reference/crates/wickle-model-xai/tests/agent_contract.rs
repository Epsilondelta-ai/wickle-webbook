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
use wickle_model_xai::*;
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
fn call_reply(index: usize, arguments: &str) -> support::Reply {
    let response = format!("resp_{index}");
    let item = format!("fc_{index}");
    let call = format!("call_{index}");
    let reasoning = json!({"id":"","type":"reasoning","summary":[],"encrypted_content":format!("ciphertext-{index}")});
    let output = json!({"id":item,"type":"function_call","call_id":call,"name":"lookup","arguments":arguments,"status":"completed"});
    support::Reply::sse(&[
        json!({"type":"response.created","response":{"id":response,"model":support::MODEL,"status":"in_progress"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"","type":"reasoning","summary":[]}}),
        json!({"type":"response.output_item.done","output_index":0,"item":reasoning}),
        json!({"type":"response.output_item.added","output_index":1,"item":{"id":item,"type":"function_call","call_id":call,"name":"lookup","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":item,"delta":arguments}),
        json!({"type":"response.function_call_arguments.done","output_index":1,"item_id":item,"arguments":arguments}),
        json!({"type":"response.output_item.done","output_index":1,"item":output}),
        json!({"type":"response.completed","response":{"id":response,"model":support::MODEL,"status":"completed","output":[reasoning,output]}}),
    ])
}
#[tokio::test]
async fn compiled_xai_contract_repairs_arguments_and_retries_only_through_the_core() {
    let invalid_constraint =
        json!({"query":"latest","limit":7,"note":null,"filter":"{\"category\":\"finance\"}"})
            .to_string();
    let invalid_json =
        json!({"query":"latest","limit":9,"note":null,"filter":"{not json"}).to_string();
    let valid =
        json!({"query":"latest","limit":9,"note":null,"filter":"{\"category\":\"finance\"}"})
            .to_string();
    let malformed = "{not json";
    let rounded = r#"{"query":"latest","limit":0.12345678901234567890123456789}"#;
    let server = support::Server::new(vec![
        support::Reply::json(503, json!({"error":{"code":503}})),
        call_reply(0, malformed),
        call_reply(4, rounded),
        call_reply(1, &invalid_constraint),
        call_reply(2, &invalid_json),
        call_reply(3, &valid),
        support::Reply::sse(&support::events(support::MODEL, "complete")),
    ])
    .await;
    let connection = XaiConnection::new(
        scope(),
        reference("account"),
        "fixture-key-not-a-secret",
        XaiOptions {
            base_url: server.base.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    let fixture = core_host::Fixture::new(core_host::Response::Text, false);
    let mut catalog = fixture.router.snapshot.catalog().clone();
    catalog.models[0].provider = id("xai");
    catalog.models[0].model_id = id(support::MODEL);
    catalog.models[0].model_version = id("fixture-release");
    catalog.models[0]
        .capabilities
        .features
        .insert(id("tool_calling"));
    catalog.models[0].capabilities.options_schema = json!({"type":"object","properties":{"reasoning_effort":{"enum":["low","medium","high"]}},"additionalProperties":false});
    catalog.bindings[0]
        .default_options
        .insert("reasoning_effort".into(), json!("high"));
    catalog.bindings[0].model = catalog.models[0].reference();
    catalog.bindings[0].requested_model = id(support::MODEL);
    catalog.bindings[0].adapter = connection.binding().adapter;
    catalog.bindings[0].connection_ref = connection.binding().connection_ref;
    catalog.bindings[0].api_contract = connection.api_contract();
    catalog.bindings[0].target = connection.target().clone();
    let properties: serde_json::Map<String, Value> = connection
        .target()
        .iter()
        .map(|(key, value)| (key.clone(), json!({"const":value})))
        .collect();
    let required: Vec<_> = connection.target().keys().cloned().collect();
    catalog.bindings[0].target_schema = json!({"type":"object","properties":properties,"required":required,"additionalProperties":false});
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
        ModelExchange::new(Arc::new(XaiModel::new(connection)), bindings.policy.clone())
            .with_route_inspector(Arc::new(Inspector), Duration::from_secs(1))
            .unwrap()
            .with_retry_policy(ModelRetryPolicy {
                max_retries: 1,
                backoff_ms: 0,
            }),
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
            "filter":{"type":"object","properties":{"category":{"type":"string"},"term":false},"required":["category"],"additionalProperties":false},
            "workspace_id":{"type":"string","format":"uuid"}
        },"required":["query","workspace_id"],"additionalProperties":false,"if":{"properties":{"query":{"const":"latest"}}},"then":{"properties":{"limit":{"minimum":8}}}}),
        agent_parameters: vec!["query".into(),"limit".into(),"note".into(),"filter".into()], system_bindings: None,
        output_schema: json!({"type":"string"}), side_effect: ToolSideEffect::ReadOnly, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false, max_output_bytes: 4096.try_into().unwrap(),
    }, &bindings.system_inputs).unwrap();
    let wire_definition = tool.to_model_tool();
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
    profile.limits.max_model_calls = 8.try_into().unwrap();
    profile.limits.max_recovery_attempts = 1;
    profile
        .model_options
        .insert("reasoning_effort".into(), json!("low"));
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
    let mut request = core_host::request("contract");
    request
        .model_options
        .insert("reasoning_effort".into(), json!("medium"));
    let handle = completed(agent.start(request, context.clone()).await.unwrap());
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
    assert_eq!(outcome.usage.recovery_attempts, 1);
    assert_eq!(outcome.usage.model_calls, 7);
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
    assert_eq!(requests.len(), 7);
    assert_eq!(requests[0].body, requests[1].body);
    for request in requests.iter() {
        let declaration = &request.body["tools"][0];
        assert!(declaration.get("strict").is_none());
        assert_eq!(declaration["parameters"]["required"], json!(["query"]));
        assert!(
            declaration["parameters"]["properties"]
                .get("workspace_id")
                .is_none()
        );
        assert_eq!(
            declaration["parameters"]["properties"]["filter"]["type"],
            "string"
        );
        assert_eq!(request.path, "/v1/responses");
        assert!(
            request
                .headers
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-key-not-a-secret")
        );
        assert_eq!(request.body["reasoning"]["effort"], "medium");
        assert!(!request.body.to_string().contains(WORKSPACE));
    }
    let items = requests[6].body["input"].as_array().unwrap();
    let replayed: Vec<_> = items
        .iter()
        .filter(|item| item["type"] == "function_call")
        .map(|item| item["arguments"].as_str().unwrap())
        .collect();
    assert_eq!(
        replayed,
        vec![
            malformed,
            rounded,
            invalid_constraint.as_str(),
            invalid_json.as_str(),
            valid.as_str()
        ]
    );
    let encrypted: Vec<_> = items
        .iter()
        .filter(|item| item["type"] == "reasoning")
        .map(|item| item["encrypted_content"].as_str().unwrap())
        .collect();
    assert_eq!(
        encrypted,
        vec![
            "ciphertext-0",
            "ciphertext-4",
            "ciphertext-1",
            "ciphertext-2",
            "ciphertext-3"
        ]
    );
    assert!(wire_definition.model_input_schema.get("if").is_some());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
}
