//! Provider schema semantics through compiled contracts and the actual HTTP encoder.
#[allow(dead_code)]
mod support;
use serde_json::{Value, json};
use support::*;
use wickle::*;
use wickle_model_xai::*;
fn original(schema: Value) -> CompiledTool {
    let parameters = schema["properties"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    SchemaCompiler::new()
        .compile(
            ToolDescriptor {
                tool: reference("lookup"),
                name: id("lookup"),
                description: "Read scoped data".into(),
                input_schema: schema,
                agent_parameters: parameters,
                system_bindings: None,
                output_schema: json!({"type":"string"}),
                side_effect: ToolSideEffect::ReadOnly,
                concurrency: ToolConcurrency::Serial,
                retry: ToolRetryPolicy::Never,
                reconcile: false,
                max_output_bytes: 4096.try_into().unwrap(),
            },
            &SystemInputRegistry::new(vec![]).unwrap(),
        )
        .unwrap()
}
#[tokio::test]
async fn optional_null_open_objects_and_single_intersections_keep_canonical_meaning_on_wire() {
    let server = Server::new(vec![Reply::sse(&events(MODEL, "done"))]).await;
    let connection = connection(&server);
    let model = XaiModel::new(connection.clone());
    let mut request = request(&connection, MODEL);
    let original = original(json!({"type":"object","properties":{
        "query":{"type":"string","pattern":"foo"},
        "note":{"type":["string","null"]},
        "limit":{"allOf":[{"type":"integer","minimum":1,"maximum":10}]},
        "open":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]},
        "anything":true
    },"required":["query"],"additionalProperties":false}));
    let contract = CompiledToolContract::compile(
        &original,
        ProviderToolTarget::for_route(&request.route),
        model.tool_schema_compiler().as_ref(),
        Default::default(),
    )
    .unwrap();
    for canonical in [
        JsonObject::from([("query".into(), json!("prefixfoosuffix"))]),
        JsonObject::from([
            ("query".into(), json!("foo")),
            ("note".into(), Value::Null),
            ("limit".into(), json!(3)),
            ("open".into(), json!({"name":"record","unlisted":42})),
            ("anything".into(), json!([true, 3, null])),
        ]),
    ] {
        original.validate_model_inputs(&canonical).unwrap();
        let wire = contract.encode_arguments(&canonical).unwrap();
        assert_eq!(
            contract
                .decode_arguments(&serde_json::to_string(&wire).unwrap(), Default::default())
                .unwrap(),
            canonical
        );
    }
    for invalid in [r#"{"query":"absent"}"#, r#"{"query":"foo","limit":0}"#] {
        let values = contract
            .decode_arguments(invalid, Default::default())
            .unwrap();
        assert!(original.validate_model_inputs(&values).is_err());
    }
    request.tools = vec![contract.wire_tool().clone()];
    for fragment in contract.constraint_fragments() {
        request.messages[0].content.push(ModelContent::Text {
            text: fragment.text.clone(),
        });
    }
    collect_model_response(&request, model.generate(&request, &context(&request)))
        .await
        .unwrap();
    let calls = server.requests.lock().unwrap();
    let declaration = &calls[0].body["tools"][0];
    let wire = &declaration["parameters"];
    assert!(declaration.get("strict").is_none());
    assert_eq!(wire["required"], json!(["query"]));
    assert_eq!(
        wire["properties"]["note"]["type"],
        json!(["string", "null"])
    );
    assert_eq!(wire["properties"]["open"]["additionalProperties"], true);
    assert_eq!(wire["properties"]["limit"]["allOf"][0]["minimum"], 1);
    assert_eq!(wire["properties"]["limit"]["allOf"][0]["maximum"], 10);
    assert!(wire["properties"]["query"].get("pattern").is_none());
    assert_eq!(wire["properties"]["anything"]["type"], "string");
    assert!(
        contract
            .enforcement()
            .iter()
            .any(|rule| rule.canonical_pointer == "/properties/query/pattern"
                && rule.core
                && rule.context_text
                && !rule.provider_native)
    );
}
#[tokio::test]
async fn target_mismatch_and_non_disableable_reasoning_fail_before_http() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let model = XaiModel::new(connection.clone());
    let mut request = request(&connection, "grok-4.7");
    let original = original(
        json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}),
    );
    let mut target = ProviderToolTarget::for_route(&request.route);
    target.provider = id("openai");
    assert!(
        CompiledToolContract::compile(
            &original,
            target,
            model.tool_schema_compiler().as_ref(),
            Default::default()
        )
        .is_err()
    );
    request
        .options
        .insert("reasoning_effort".into(), json!("none"));
    assert!(
        collect_model_response(&request, model.generate(&request, &context(&request)))
            .await
            .is_err()
    );
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn compact_branching_references_exhaust_a_shared_budget_and_fall_back_without_expanding() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let request = request(&connection, MODEL);
    let mut defs = serde_json::Map::new();
    defs.insert("level7".into(), json!({"type":"string"}));
    for level in (0..7).rev() {
        let props: serde_json::Map<String, Value> = (0..4)
            .map(|branch| {
                (
                    format!("branch{branch}"),
                    json!({"$ref":format!("#/$defs/level{}",level+1)}),
                )
            })
            .collect();
        let required: Vec<_> = props.keys().cloned().collect();
        defs.insert(format!("level{level}"),json!({"type":"object","properties":props,"required":required,"additionalProperties":false}));
    }
    let tool = ModelTool {
        name: id("lookup"),
        description: "Lookup".into(),
        model_input_schema: json!({"type":"object","properties":{"query":{"$ref":"#/$defs/level0"}},"required":["query"],"additionalProperties":false,"$defs":defs}),
    };
    let compiler = XaiToolSchemaCompiler;
    let projection = compiler
        .compile(&tool, &ProviderToolTarget::for_route(&request.route))
        .unwrap();
    assert_eq!(
        projection.wire_tool.model_input_schema["properties"]["query"],
        json!({"type":"string"})
    );
    let ArgumentDecodePlan::Fields { fields } = projection.decode_plan else {
        panic!("missing field codec")
    };
    assert!(matches!(
        fields[0].encoding,
        ArgumentValueEncoding::JsonText { optional: false }
    ));
}

#[tokio::test]
async fn recursive_shapes_and_conflicting_reference_siblings_roundtrip_without_overwriting_rules() {
    let server = Server::new(vec![]).await;
    let connection = connection(&server);
    let request = request(&connection, MODEL);
    let canonical = original(json!({"type":"object","properties":{
        "bounded":{"$ref":"#/$defs/number","minimum":3},
        "tree":{"$ref":"#/$defs/node"}
    },"required":["bounded"],"additionalProperties":false,"$defs":{
        "number":{"type":"integer","minimum":1,"maximum":10},
        "node":{"type":"object","properties":{"next":{"$ref":"#/$defs/node"}},"additionalProperties":false}
    }}));
    let contract = CompiledToolContract::compile(
        &canonical,
        ProviderToolTarget::for_route(&request.route),
        &XaiToolSchemaCompiler,
        Default::default(),
    )
    .unwrap();
    let values = JsonObject::from([
        ("bounded".into(), json!(3)),
        ("tree".into(), json!({"next":{}})),
    ]);
    let encoded = contract.encode_arguments(&values).unwrap();
    assert!(encoded["bounded"].is_string());
    assert!(encoded["tree"].is_string());
    let decoded = contract
        .decode_arguments(
            &serde_json::to_string(&encoded).unwrap(),
            Default::default(),
        )
        .unwrap();
    assert_eq!(decoded, values);
    canonical.validate_model_inputs(&decoded).unwrap();
    for value in [2, 11] {
        let invalid = JsonObject::from([("bounded".into(), json!(value))]);
        let encoded = contract.encode_arguments(&invalid).unwrap();
        let decoded = contract
            .decode_arguments(
                &serde_json::to_string(&encoded).unwrap(),
                Default::default(),
            )
            .unwrap();
        assert!(canonical.validate_model_inputs(&decoded).is_err());
    }
}
