use futures_util::stream;
use serde_json::json;
use std::collections::BTreeMap;
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

fn registry(revision: &str) -> Result<SystemInputRegistry, ContractError> {
    SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("active_workspace_id"),
        version: id(revision),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
}

fn model_request(tool: ModelTool) -> ModelRequest {
    ModelRequest {
        request_id: id("model-request"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference("local-model"),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id("example-model"),
            model_id: id("example-model"),
            model_version: id("1"),
            version_semantics: VersionSemantics::Pinned,
            provider: id("example-provider"),
            target: JsonObject::new(),
            deployment_revision: None,
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("1"),
            },
            adapter: reference("example-adapter"),
            capability_revision: id("capabilities"),
            connection_ref: reference("connection"),
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find recent reports".into(),
            }],
        }],
        tools: vec![tool],
        output: ModelOutput::Text {},
        max_output_tokens: 256.try_into().unwrap(),
        options: JsonObject::new(),
        limits: ModelResponseLimits {
            max_input_bytes: 16_384,
            max_response_bytes: 4096,
            max_delta_bytes: 1024,
            max_events: 8,
            max_tool_calls: 1,
        },
    }
}

async fn propose(
    request: &ModelRequest,
    inputs: &JsonObject,
) -> Result<ModelResponse, ModelProtocolError> {
    collect_model_response(
        request,
        Box::pin(stream::iter([
            Ok(ModelEvent::ToolArgumentsDelta {
                index: 0,
                provider_call_id: Some("call".into()),
                name: Some("search_reports".into()),
                delta: serde_json::to_string(inputs).unwrap(),
            }),
            Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::ToolCalls,
                metadata: ModelResponseMetadata::default(),
                continuation: vec![],
            }),
        ])),
    )
    .await
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptor = ToolDescriptor::from_json(
        r##"{
      "tool":{"id":"report-search","version":"1"},
      "name":"search_reports","description":"Search reports in the current workspace",
      "input_schema":{
        "type":"object",
        "properties":{
          "query":{"$ref":"#/$defs/Query"},
          "limit":{"type":"integer","minimum":1,"default":10},
          "workspace_id":{"$ref":"#/$defs/WorkspaceId"}
        },
        "required":["query","workspace_id"],"additionalProperties":false,
        "examples":[{"query":"private example","workspace_id":"f7fba7f5-f8e7-4f44-b885-45d0688e9f33"}],
        "$defs":{
          "Query":{"type":"string","minLength":1},
          "WorkspaceId":{"type":"string","format":"uuid"},
          "Unused":{"type":"string","description":"unrelated internal schema"}
        }
      },
      "agent_parameters":["query","limit"],
      "system_bindings":{"workspace_id":"active_workspace_id"},
      "output_schema":{"type":"array","items":{"type":"string"}},
      "side_effect":"read_only","concurrency":"serial","retry":"never","reconcile":false,
      "max_output_bytes":4096
    }"##,
    )?;
    let registry = registry("1")?;
    let compiler = SchemaCompiler::new();
    let compiled = compiler.compile(descriptor, &registry)?;
    let schema = compiled.model_input_schema();
    assert_eq!(schema["required"], json!(["query"]));
    assert_eq!(schema["additionalProperties"], false);
    assert!(schema["properties"].get("workspace_id").is_none());
    assert!(schema.get("examples").is_none());
    assert!(schema["$defs"].get("WorkspaceId").is_none());
    assert!(schema["$defs"].get("Unused").is_none());
    assert!(schema["$defs"].get("Query").is_some());
    assert_eq!(
        compiled.system_bindings()["workspace_id"].key,
        id("active_workspace_id")
    );
    let model_inputs = BTreeMap::from([("query".into(), json!("recent results"))]);
    compiled.validate_model_inputs(&model_inputs)?;
    // Validation alone does not apply defaults; the binder owns that operation.
    let mut full = model_inputs.clone();
    full.insert(
        "workspace_id".into(),
        json!("f7fba7f5-f8e7-4f44-b885-45d0688e9f33"),
    );
    assert!(compiled.validate_model_inputs(&full).is_err());
    compiled.validate_execution_inputs(&full)?;
    let mut invalid_full = full.clone();
    invalid_full.insert("workspace_id".into(), json!("an-invented-hash"));
    assert!(compiled.validate_execution_inputs(&invalid_full).is_err());
    assert!(compiled.validate_execution_inputs(&model_inputs).is_err());
    let target = ProviderToolTarget {
        model: None,
        provider: id("example-provider"),
        api_contract: ApiContract { operation: id("messages"), version: id("1") },
        capability_revision: id("capabilities"),
    };
    let limits = ProviderToolSchemaLimits::default();
    let projected = CompiledToolContract::compile(&compiled, target.clone(), &NativeToolSchemaCompiler, limits)?;
    let persisted = serde_json::to_string(&projected)?;
    let reopened = CompiledToolContract::restore(&persisted, &compiled, &target, projected.digest(), limits)?;
    let decoded = reopened.decode_arguments(&serde_json::to_string(&model_inputs)?, limits)?;
    compiled.validate_model_inputs(&decoded)?;
    assert_eq!(decoded, model_inputs);
    assert_eq!(reopened.canonical_name(), &id("search_reports"));
    let strict_target = ProviderToolTarget {
        model: Some(reference("example-model")),
        provider: id("openai"),
        api_contract: ApiContract { operation: id("responses"), version: id("v1") },
        capability_revision: id("strict-capabilities"),
    };
    let strict = CompiledToolContract::compile(&compiled, strict_target.clone(), &wickle_model_responses::ResponsesToolSchemaCompiler, limits)?;
    let wire = strict.encode_arguments(&model_inputs)?;
    assert_eq!(wire["limit"]["present"], false);
    let restored_strict = CompiledToolContract::restore(&serde_json::to_string(&strict)?, &compiled, &strict_target, strict.digest(), limits)?;
    let canonical = restored_strict.decode_arguments(&serde_json::to_string(&wire)?, limits)?;
    assert_eq!(canonical, model_inputs);
    let normalized = compiled.normalize_model_inputs(&canonical)?;
    assert_eq!(normalized["limit"], json!(10));
    let mut forged = wire;
    forged.insert("workspace_id".into(), json!("f7fba7f5-f8e7-4f44-b885-45d0688e9f33"));
    assert!(restored_strict.decode_arguments(&serde_json::to_string(&forged)?, limits).is_err());
    let request = model_request(compiled.to_model_tool());
    assert_eq!(
        propose(&request, &model_inputs).await?.tool_calls[0].validation,
        ToolCallValidation::Valid
    );
    assert_eq!(
        propose(&request, &full).await?.tool_calls[0].validation,
        ToolCallValidation::InvalidArguments
    );
    let saved = serde_json::to_string(&compiled)?;
    let restored = compiler.restore(&saved, &registry, compiled.digest())?;
    assert_eq!(restored.digest(), compiled.digest());
    assert_eq!(restored.model_input_schema(), compiled.model_input_schema());
    let changed_registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("active_workspace_id"),
        version: id("2"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])?;
    assert!(
        compiler
            .restore(&saved, &changed_registry, compiled.digest())
            .is_err()
    );
    println!(
        "tool schema consumer: query/limit exposed; hidden schema omitted; hidden input rejected by the model boundary; full UUID schema checked; native and strict provider contracts restored with omission/default semantics; compiled identity preserved and changed registry rejected"
    );
    Ok(())
}
