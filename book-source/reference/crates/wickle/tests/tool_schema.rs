//! Explicit tool input ownership, schema projection, and pinned compilation.
use futures_util::stream;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use wickle::*;

const WORKSPACE: &str = "11111111-1111-4111-8111-111111111111";

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}

fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn descriptor() -> ToolDescriptor {
    ToolDescriptor {
        tool: reference("search"),
        name: id("search"),
        description: "Search records".into(),
        input_schema: json!({
            "type":"object",
            "properties":{
                "query":{"type":"string","minLength":1},
                "limit":{"type":"integer","minimum":1,"default":10},
                "workspace_id":{"type":"string","format":"uuid"}
            },
            "required":["query","workspace_id"],
            "additionalProperties":false
        }),
        agent_parameters: vec!["query".into(), "limit".into()],
        system_bindings: None,
        output_schema: json!({"type":"array","items":{"type":"string"}}),
        side_effect: ToolSideEffect::ReadOnly,
        concurrency: ToolConcurrency::Serial,
        retry: ToolRetryPolicy::Never,
        reconcile: false,
        max_output_bytes: 4096.try_into().unwrap(),
    }
}

fn definition(key: &str, value_schema: Value) -> SystemInputDefinition {
    SystemInputDefinition {
        key: id(key),
        version: id("definition-1"),
        value_schema,
        source: SystemInputSource::Run {},
    }
}

fn registry() -> SystemInputRegistry {
    SystemInputRegistry::new(vec![definition(
        "workspace_id",
        json!({"type":"string","format":"uuid"}),
    )])
    .unwrap()
}

#[test]
fn projection_validates_model_and_execution_inputs_at_separate_boundaries() {
    let compiled = SchemaCompiler::new()
        .compile(descriptor(), &registry())
        .unwrap();
    assert_eq!(
        compiled.model_input_schema(),
        &json!({
            "type":"object",
            "properties":{
                "query":{"type":"string","minLength":1},
                "limit":{"type":"integer","minimum":1,"default":10}
            },
            "required":["query"],
            "additionalProperties":false
        })
    );
    let model_inputs = object(json!({"query":"recent results"}));
    compiled.validate_model_inputs(&model_inputs).unwrap();
    assert!(compiled.validate_execution_inputs(&model_inputs).is_err());
    let complete = object(json!({"query":"recent results","workspace_id":WORKSPACE}));
    compiled.validate_execution_inputs(&complete).unwrap();
    assert!(compiled.validate_model_inputs(&complete).is_err());
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"x","limit":0})))
            .is_err()
    );
    assert!(
        compiled
            .validate_execution_inputs(&object(json!({"query":"x","workspace_id":"not-a-uuid"})))
            .is_err()
    );
    assert!(
        compiled
            .validate_execution_inputs(&object(
                json!({"query":"x","workspace_id":WORKSPACE,"user_id":"extra"})
            ))
            .is_err()
    );
}

#[test]
fn agent_allowlist_must_be_present_explicit_unique_and_known() {
    for value in [None, Some(Value::Null)] {
        let mut encoded = serde_json::to_value(descriptor()).unwrap();
        if let Some(value) = value {
            encoded["agent_parameters"] = value;
        } else {
            encoded.as_object_mut().unwrap().remove("agent_parameters");
        }
        assert!(ToolDescriptor::from_json(&encoded.to_string()).is_err());
    }
    for parameters in [vec!["query", "query"], vec!["query", "unknown"]] {
        let mut tool = descriptor();
        tool.agent_parameters = parameters.into_iter().map(str::to_owned).collect();
        assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());
    }
}

#[test]
fn empty_and_complete_allowlists_have_explicit_input_semantics() {
    let all_system = SystemInputRegistry::new(vec![
        definition("query", json!({"type":"string"})),
        definition("limit", json!({"type":"integer"})),
        definition("workspace_id", json!({"type":"string","format":"uuid"})),
    ])
    .unwrap();
    let mut hidden = descriptor();
    hidden.agent_parameters.clear();
    let compiled = SchemaCompiler::new().compile(hidden, &all_system).unwrap();
    assert_eq!(
        compiled.model_input_schema(),
        &json!({
            "type":"object","properties":{},"required":[],"additionalProperties":false
        })
    );
    compiled.validate_model_inputs(&JsonObject::new()).unwrap();
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"x"})))
            .is_err()
    );
    assert_eq!(compiled.system_bindings().len(), 3);

    let mut visible = descriptor();
    visible.agent_parameters.push("workspace_id".into());
    let empty_registry = SystemInputRegistry::new(vec![]).unwrap();
    let compiled = SchemaCompiler::new()
        .compile(visible, &empty_registry)
        .unwrap();
    assert!(compiled.system_bindings().is_empty());
    compiled
        .validate_model_inputs(&object(json!({"query":"x","workspace_id":WORKSPACE})))
        .unwrap();
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"x"})))
            .is_err()
    );
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"x","workspace_id":"invalid"})))
            .is_err()
    );
}

#[test]
fn aliases_resolve_registered_metadata_and_cannot_reassign_model_owned_parameters() {
    let registry = SystemInputRegistry::new(vec![definition(
        "active_workspace",
        json!({"type":"string","format":"uuid"}),
    )])
    .unwrap();
    let mut aliased = descriptor();
    aliased.system_bindings = Some(BTreeMap::from([(
        "workspace_id".into(),
        id("active_workspace"),
    )]));
    let compiled = SchemaCompiler::new().compile(aliased, &registry).unwrap();
    assert_eq!(
        compiled.system_bindings()["workspace_id"].key,
        id("active_workspace")
    );
    assert_eq!(
        compiled.system_bindings()["workspace_id"].version,
        id("definition-1")
    );

    for (parameter, key) in [
        ("query", "active_workspace"),
        ("missing", "active_workspace"),
        ("workspace_id", "unregistered"),
    ] {
        let mut invalid = descriptor();
        invalid.system_bindings = Some(BTreeMap::from([(parameter.into(), id(key))]));
        assert!(SchemaCompiler::new().compile(invalid, &registry).is_err());
    }
    // A registered definition is required even before runtime values are supplied.
    assert!(
        SchemaCompiler::new()
            .compile(descriptor(), &SystemInputRegistry::new(vec![]).unwrap())
            .is_err()
    );
}

#[test]
fn one_system_key_cannot_have_competing_definitions_or_supply_sources() {
    let first = definition("workspace_id", json!({"type":"string"}));
    assert!(SystemInputRegistry::new(vec![first.clone(), first.clone()]).is_err());
    let mut other_version = first.clone();
    other_version.version = id("definition-2");
    assert!(SystemInputRegistry::new(vec![first.clone(), other_version]).is_err());
    let mut resolver = first.clone();
    resolver.source = SystemInputSource::Resolver {
        resolver_ref: reference("current_workspace"),
    };
    assert!(SystemInputRegistry::new(vec![first, resolver]).is_err());
}

#[test]
fn root_annotations_and_hidden_definitions_are_excluded_while_selected_constraints_survive() {
    let mut tool = descriptor();
    tool.input_schema["description"] = json!("Internal execution values");
    tool.input_schema["default"] = json!({"query":"default","workspace_id":WORKSPACE});
    tool.input_schema["examples"] = json!([{"query":"example","workspace_id":WORKSPACE}]);
    tool.input_schema["properties"]["workspace_id"]["default"] = json!(WORKSPACE);
    tool.input_schema["properties"]["workspace_id"]["examples"] = json!([WORKSPACE]);
    tool.input_schema["properties"]["query"] = json!({"$ref":"#/$defs/Query"});
    tool.input_schema["$defs"] = json!({
        "Query":{"$ref":"#/$defs/ShortText","description":"A selected query"},
        "ShortText":{"type":"string","minLength":2,"examples":["alpha"]},
        "Hidden":{"type":"string","default":WORKSPACE},
        "Unused":{"type":"object","examples":[{"workspace_id":WORKSPACE}]}
    });
    let compiled = SchemaCompiler::new().compile(tool, &registry()).unwrap();
    let projected = compiled.model_input_schema();
    assert!(projected.get("description").is_none());
    assert!(projected.get("default").is_none());
    assert!(projected.get("examples").is_none());
    assert!(projected["properties"].get("workspace_id").is_none());
    assert_eq!(
        projected["$defs"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        ["Query".to_owned(), "ShortText".to_owned()]
            .into_iter()
            .collect()
    );
    assert_eq!(
        projected["$defs"]["Query"]["description"],
        json!("A selected query")
    );
    assert_eq!(
        projected["$defs"]["ShortText"]["examples"],
        json!(["alpha"])
    );
    assert_eq!(projected["properties"]["limit"]["default"], json!(10));
    compiled
        .validate_model_inputs(&object(json!({"query":"alpha"})))
        .unwrap();
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"a"})))
            .is_err()
    );
}

#[test]
fn local_definitions_shared_with_hidden_properties_remain_when_the_model_reaches_them() {
    let mut tool = descriptor();
    tool.input_schema["properties"]["query"] = json!({"$ref":"#/definitions/Identifier"});
    tool.input_schema["properties"]["workspace_id"] = json!({"$ref":"#/definitions/Identifier"});
    tool.input_schema["definitions"] = json!({"Identifier":{"type":"string","format":"uuid"}});
    let compiled = SchemaCompiler::new().compile(tool, &registry()).unwrap();
    compiled
        .validate_model_inputs(&object(json!({"query":WORKSPACE})))
        .unwrap();
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"not-an-id"})))
            .is_err()
    );
    assert!(
        compiled.model_input_schema()["properties"]
            .get("workspace_id")
            .is_none()
    );
    assert_eq!(
        compiled.model_input_schema()["definitions"]["Identifier"]["format"],
        json!("uuid")
    );
}

#[test]
fn selected_annotation_data_does_not_create_schema_reference_edges() {
    let annotation = json!({"$ref":"https://schemas.example.invalid/data-not-schema"});
    let mut tool = descriptor();
    tool.input_schema["properties"]["query"] = json!({
        "type":"object", "properties":{"$ref":{"type":"string"}},
        "required":["$ref"], "additionalProperties":false,
        "default":annotation, "examples":[annotation]
    });
    let compiled = SchemaCompiler::new().compile(tool, &registry()).unwrap();
    compiled
        .validate_model_inputs(&object(json!({"query":annotation})))
        .unwrap();
    assert_eq!(
        compiled.model_input_schema()["properties"]["query"]["default"],
        annotation
    );
    assert_eq!(
        compiled.model_input_schema()["properties"]["query"]["examples"],
        json!([annotation])
    );
}

#[test]
fn unsupported_reference_forms_fail_instead_of_being_silently_projected() {
    for reference in [
        "#/properties/workspace_id",
        "#/$defs/Missing",
        "https://schemas.example.invalid/input.json",
        "file:///not-a-schema.json",
    ] {
        let mut tool = descriptor();
        tool.input_schema["properties"]["query"] = json!({"$ref":reference});
        assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());
    }
    let mut nested_pointer = descriptor();
    nested_pointer.input_schema["properties"]["query"] =
        json!({"$ref":"#/$defs/Query/properties/nested"});
    nested_pointer.input_schema["$defs"] = json!({
        "Query":{"type":"object","properties":{"nested":{"type":"string"}}}
    });
    assert!(
        SchemaCompiler::new()
            .compile(nested_pointer, &registry())
            .is_err()
    );
    for schema in [
        json!({"$dynamicRef":"#node"}),
        json!({"type":"string","$anchor":"node"}),
        json!({"type":"string","$id":"https://schemas.example.invalid/local"}),
        json!({"type":"object","$defs":{"Nested":{"type":"string"}},"properties":{}}),
    ] {
        let mut tool = descriptor();
        tool.input_schema["properties"]["query"] = schema;
        assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());
    }
}

#[test]
fn mixed_conditions_stay_in_execution_validation_and_model_only_conditions_are_preserved() {
    for (keyword, condition) in [
        ("allOf", json!([{"required":["workspace_id"]}])),
        (
            "anyOf",
            json!([{"required":["workspace_id"]},{"required":["query"]}]),
        ),
        (
            "oneOf",
            json!([{"required":["workspace_id"]},{"required":["limit"]}]),
        ),
        ("dependentRequired", json!({"query":["workspace_id"]})),
        ("minProperties", json!(2)),
        ("patternProperties", json!({"^private_":{"type":"string"}})),
    ] {
        let mut tool = descriptor();
        tool.input_schema[keyword] = condition;
        let result = SchemaCompiler::new().compile(tool, &registry());
        if keyword == "patternProperties" {
            assert!(result.is_err());
        } else {
            let compiled = result.unwrap();
            compiled
                .validate_model_inputs(&object(json!({"query":"ordinary"})))
                .unwrap();
        }
    }
    let mut conditional = descriptor();
    conditional.input_schema["if"] = json!({"properties":{"query":{"const":"strict"}}});
    conditional.input_schema["then"] = json!({"$ref":"#/$defs/NeedsLimit"});
    conditional.input_schema["$defs"] = json!({"NeedsLimit":{"required":["limit"]}});
    conditional.input_schema["examples"] =
        json!([{"query":"root annotation","workspace_id":WORKSPACE}]);
    conditional.input_schema["default"] = json!({"query":"root default","workspace_id":WORKSPACE});
    let mixed = SchemaCompiler::new()
        .compile(conditional.clone(), &registry())
        .unwrap();
    assert!(
        mixed
            .validate_model_inputs(&object(json!({"query":"strict"})))
            .is_err()
    );
    mixed
        .validate_model_inputs(&object(json!({"query":"strict","limit":2})))
        .unwrap();
    assert!(
        mixed
            .validate_execution_inputs(&object(json!({"query":"strict","limit":2})))
            .is_err()
    );
    conditional.agent_parameters.push("workspace_id".into());
    let compiled = SchemaCompiler::new()
        .compile(conditional, &registry())
        .unwrap();
    assert!(compiled.model_input_schema().get("examples").is_none());
    assert!(compiled.model_input_schema().get("default").is_none());
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"strict","workspace_id":WORKSPACE})))
            .is_err()
    );
    compiled
        .validate_model_inputs(&object(
            json!({"query":"strict","limit":2,"workspace_id":WORKSPACE}),
        ))
        .unwrap();
    compiled
        .validate_model_inputs(&object(
            json!({"query":"ordinary","workspace_id":WORKSPACE}),
        ))
        .unwrap();
}

#[test]
fn the_declared_top_level_object_contract_cannot_be_weakened_or_ambiguous() {
    for (keyword, replacement) in [
        ("type", json!("array")),
        ("additionalProperties", json!(true)),
        ("properties", json!([])),
        ("required", json!(["query", "absent"])),
        ("required", json!(["query", "query"])),
    ] {
        let mut tool = descriptor();
        tool.input_schema[keyword] = replacement;
        assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());
    }
    for keyword in ["type", "properties", "required", "additionalProperties"] {
        let mut tool = descriptor();
        tool.input_schema.as_object_mut().unwrap().remove(keyword);
        assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());
    }
}

#[test]
fn nested_objects_are_owned_whole_and_dotted_root_names_are_literal_names() {
    let mut tool = descriptor();
    tool.input_schema["properties"]["query"] = json!({
        "type":"object", "properties":{"workspace_id":{"type":"string"}},
        "required":["workspace_id"], "additionalProperties":false
    });
    let compiled = SchemaCompiler::new()
        .compile(tool.clone(), &registry())
        .unwrap();
    compiled
        .validate_model_inputs(&object(
            json!({"query":{"workspace_id":"model-owned-nested-value"}}),
        ))
        .unwrap();
    tool.agent_parameters = vec!["query.workspace_id".into()];
    assert!(
        SchemaCompiler::new()
            .compile(tool.clone(), &registry())
            .is_err()
    );
    tool.agent_parameters = vec!["query".into(), "limit".into()];
    tool.system_bindings = Some(BTreeMap::from([(
        "query.workspace_id".into(),
        id("workspace_id"),
    )]));
    assert!(SchemaCompiler::new().compile(tool, &registry()).is_err());

    let mut dotted = descriptor();
    dotted.input_schema["properties"]["query.name"] = json!({"type":"string"});
    dotted.agent_parameters.push("query.name".into());
    let compiled = SchemaCompiler::new().compile(dotted, &registry()).unwrap();
    compiled
        .validate_model_inputs(&object(json!({"query":"x","query.name":"literal"})))
        .unwrap();
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"x","name":"not-the-literal-key"})))
            .is_err()
    );
}

#[tokio::test]
async fn compiled_model_tool_rejects_a_model_supplied_hidden_uuid_in_real_response_collection() {
    let compiled = SchemaCompiler::new()
        .compile(descriptor(), &registry())
        .unwrap();
    let route = ResolvedModelRoute {
        binding: reference("model"),
        catalog_revision: id("catalog"),
        routing_policy_revision: id("policy"),
        requested_model: id("model"),
        model_id: id("model"),
        model_version: id("release"),
        version_semantics: VersionSemantics::Pinned,
        provider: id("scripted"),
        target: JsonObject::new(),
        deployment_revision: None,
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        adapter: reference("adapter"),
        capability_revision: id("capabilities"),
        connection_ref: reference("connection"),
    };
    let request = ModelRequest {
        request_id: id("request"),
        purpose: ModelPurpose::Agent,
        route,
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Search records".into(),
            }],
        }],
        tools: vec![compiled.to_model_tool()],
        output: ModelOutput::Text {},
        max_output_tokens: 128.try_into().unwrap(),
        options: JsonObject::new(),
        limits: ModelResponseLimits {
            max_input_bytes: 16384,
            max_response_bytes: 4096,
            max_delta_bytes: 1024,
            max_events: 8,
            max_tool_calls: 2,
        },
    };
    let events = vec![
        ModelEvent::ToolArgumentsDelta {
            index: 0,
            provider_call_id: Some("hidden".into()),
            name: Some("search".into()),
            delta: json!({"query":"x","workspace_id":WORKSPACE}).to_string(),
        },
        ModelEvent::ToolArgumentsDelta {
            index: 1,
            provider_call_id: Some("model-only".into()),
            name: Some("search".into()),
            delta: json!({"query":"x"}).to_string(),
        },
        ModelEvent::ResponseCompleted {
            finish: ModelFinish::ToolCalls,
            metadata: ModelResponseMetadata::default(),
            continuation: vec![],
        },
    ];
    let response =
        collect_model_response(&request, Box::pin(stream::iter(events.into_iter().map(Ok))))
            .await
            .unwrap();
    assert_eq!(
        response.tool_calls[0].validation,
        ToolCallValidation::InvalidArguments
    );
    assert_eq!(response.tool_calls[1].validation, ToolCallValidation::Valid);
    assert_eq!(
        response.tool_calls[0].model_inputs["workspace_id"],
        json!(WORKSPACE)
    );
}

#[test]
fn restored_compilation_rejects_cached_projection_or_definition_changes() {
    let compiler = SchemaCompiler::new();
    let registry = registry();
    let compiled = compiler.compile(descriptor(), &registry).unwrap();
    let encoded = serde_json::to_string(&compiled).unwrap();
    let restored = compiler
        .restore(&encoded, &registry, compiled.digest())
        .unwrap();
    restored
        .validate_model_inputs(&object(json!({"query":"valid"})))
        .unwrap();
    assert!(
        restored
            .validate_model_inputs(&object(json!({"query":"valid","workspace_id":WORKSPACE})))
            .is_err()
    );
    let mut projection = serde_json::to_value(&compiled).unwrap();
    projection["model_input_schema"]["additionalProperties"] = json!(true);
    assert!(
        compiler
            .restore(&projection.to_string(), &registry, compiled.digest())
            .is_err()
    );
    let mut definition = registry.get(&id("workspace_id")).unwrap().clone();
    definition.version = id("definition-2");
    let changed = SystemInputRegistry::new(vec![definition]).unwrap();
    assert!(
        compiler
            .restore(&encoded, &changed, compiled.digest())
            .is_err()
    );
    let mut tool = descriptor();
    tool.input_schema["properties"]["workspace_id"]["description"] =
        json!("Updated hidden contract");
    let changed_tool = compiler.compile(tool, &registry).unwrap();
    assert_eq!(
        changed_tool.model_input_schema(),
        compiled.model_input_schema()
    );
    assert_ne!(
        changed_tool.descriptor_digest(),
        compiled.descriptor_digest()
    );
    assert_ne!(changed_tool.digest(), compiled.digest());
    assert!(
        compiler
            .restore(
                &serde_json::to_string(&changed_tool).unwrap(),
                &registry,
                compiled.digest()
            )
            .is_err()
    );
}

#[test]
fn declared_execution_safety_cannot_mark_writes_as_parallel_or_read_only_retries() {
    let compiler = SchemaCompiler::new();
    let mut tool = descriptor();
    tool.side_effect = ToolSideEffect::Write;
    tool.concurrency = ToolConcurrency::ParallelRead;
    assert!(compiler.compile(tool.clone(), &registry()).is_err());
    tool.concurrency = ToolConcurrency::Serial;
    tool.retry = ToolRetryPolicy::ReadOnly;
    assert!(compiler.compile(tool.clone(), &registry()).is_err());
    tool.retry = ToolRetryPolicy::Idempotent;
    let compiled = compiler.compile(tool, &registry()).unwrap();
    assert_eq!(compiled.descriptor().side_effect, ToolSideEffect::Write);
    assert_eq!(compiled.descriptor().retry, ToolRetryPolicy::Idempotent);
}

#[test]
fn system_dependent_condition_is_enforced_only_after_binding_without_leaking_its_branch() {
    let mut tool = descriptor();
    tool.input_schema["if"] = json!({"properties":{"workspace_id":{"const":WORKSPACE}}});
    tool.input_schema["then"] = json!({"properties":{"limit":{"maximum":5}},"required":["limit"]});
    tool.input_schema["allOf"] =
        json!([{"properties":{"query":{"minLength":3}}},{"required":["workspace_id"]}]);
    let compiled = SchemaCompiler::new().compile(tool, &registry()).unwrap();
    assert!(compiled.model_input_schema().get("if").is_none());
    assert!(compiled.model_input_schema().get("then").is_none());
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"ab"})))
            .is_err()
    );
    compiled
        .validate_model_inputs(&object(json!({"query":"abc","limit":20})))
        .unwrap();
    assert!(
        compiled
            .validate_execution_inputs(&object(
                json!({"query":"abc","limit":20,"workspace_id":WORKSPACE})
            ))
            .is_err()
    );
    compiled
        .validate_execution_inputs(&object(
            json!({"query":"abc","limit":3,"workspace_id":WORKSPACE}),
        ))
        .unwrap();
}

#[test]
fn model_only_root_reference_and_sibling_conjunction_both_survive_mixed_projection() {
    let mut tool = descriptor();
    tool.input_schema["$defs"] = json!({"QueryCondition":{"properties":{"query":{"minLength":3}}}});
    tool.input_schema["$ref"] = json!("#/$defs/QueryCondition");
    tool.input_schema["allOf"] = json!([{"properties":{"limit":{"maximum":5}}}]);
    let compiled = SchemaCompiler::new().compile(tool, &registry()).unwrap();
    for invalid in [
        json!({"query":"ab","limit":2}),
        json!({"query":"abc","limit":9}),
    ] {
        assert!(compiled.validate_model_inputs(&object(invalid)).is_err());
    }
    compiled
        .validate_model_inputs(&object(json!({"query":"abc","limit":2})))
        .unwrap();
}

#[test]
fn mixed_conjuncts_preserve_each_independent_exposed_constraint() {
    for condition in [
        json!({"properties":{"query":{"minLength":3},"workspace_id":{"const":WORKSPACE}}}),
        json!({"properties":{"query":{"minLength":3}},"required":["query","workspace_id"]}),
        json!({"allOf":[{"properties":{"query":{"minLength":3}}},{"required":["workspace_id"]}]}),
    ] {
        let mut tool = descriptor();
        tool.input_schema["allOf"] = json!([condition]);
        let compiled = SchemaCompiler::new().compile(tool, &registry()).unwrap();
        assert!(
            compiled
                .validate_model_inputs(&object(json!({"query":"ab"})))
                .is_err()
        );
        compiled
            .validate_model_inputs(&object(json!({"query":"abc"})))
            .unwrap();
    }
}

#[test]
fn hidden_predicates_are_not_weakened_into_incorrect_model_conditions() {
    for (key, condition) in [
        (
            "not",
            json!({"properties":{"query":{"minLength":3}},"required":["absent_system_field"]}),
        ),
        (
            "oneOf",
            json!([{"properties":{"query":{"minLength":3}}},{"required":["absent_system_field"]}]),
        ),
    ] {
        let mut tool = descriptor();
        tool.input_schema[key] = condition;
        let compiled = SchemaCompiler::new().compile(tool, &registry()).unwrap();
        let model = object(json!({"query":"abc"}));
        let full = object(json!({"query":"abc","workspace_id":WORKSPACE}));
        compiled.validate_execution_inputs(&full).unwrap();
        compiled.validate_model_inputs(&model).unwrap();
    }
}

#[test]
fn model_only_predicate_keeps_an_impossible_branch_type() {
    let mut tool = descriptor();
    tool.input_schema["if"] = json!({"properties":{"query":{"const":"forbidden"}}});
    tool.input_schema["then"] = json!({"type":"null"});
    let compiled = SchemaCompiler::new().compile(tool, &registry()).unwrap();
    assert!(
        compiled
            .validate_model_inputs(&object(json!({"query":"forbidden"})))
            .is_err()
    );
    compiled
        .validate_model_inputs(&object(json!({"query":"allowed"})))
        .unwrap();
}
