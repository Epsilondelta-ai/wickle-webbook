//! Provider projection retains original constraints and reversible presence semantics.
use serde_json::{Value, json};
use wickle::*;
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn reference(s: &str) -> VersionedRef {
    VersionedRef {
        id: id(s),
        version: id("1"),
    }
}
fn target() -> ProviderToolTarget {
    ProviderToolTarget {
        model: None,
        provider: id("example"),
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("1"),
        },
        capability_revision: id("caps-1"),
    }
}
fn tool() -> CompiledTool {
    let descriptor = ToolDescriptor::from_json(r##"{
      "tool":{"id":"search","version":"1"},"name":"search","description":"Search",
      "input_schema":{"type":"object","properties":{"query":{"$ref":"#/$defs/Query"},"note":{"type":["string","null"]},"workspace_id":{"type":"string","const":"private-workspace"}},"required":["query","workspace_id"],"additionalProperties":false,
      "$defs":{"Query":{"type":"string","minLength":2},"Private":{"const":"hidden-secret"}}},
      "agent_parameters":["query","note"],"output_schema":{"type":"string"},"max_output_bytes":100
    }"##).unwrap();
    let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    SchemaCompiler::new()
        .compile(descriptor, &registry)
        .unwrap()
}
struct Restricted;
impl ProviderToolSchemaCompiler for Restricted {
    fn reference(&self) -> VersionedRef {
        reference("restricted")
    }
    fn compile(
        &self,
        tool: &ModelTool,
        _: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        // The compiler receives only the model contract, never the full input schema.
        assert!(
            tool.model_input_schema["properties"]
                .get("workspace_id")
                .is_none()
        );
        assert!(tool.model_input_schema["$defs"].get("Private").is_none());
        Ok(ProviderToolProjection {
            wire_tool: ModelTool {
                name: id("wire_search"),
                description: tool.description.clone(),
                model_input_schema: json!({"type":"object","properties":{"q":{"type":"string"},"n":{"type":"object","properties":{"present":{"type":"boolean"},"value":{"type":["string","null"]}},"required":["present","value"],"additionalProperties":false}},"required":["q","n"],"additionalProperties":false}),
            },
            decode_plan: ArgumentDecodePlan::Fields {
                fields: vec![
                    ArgumentFieldMapping {
                        wire_name: "q".into(),
                        canonical_name: "query".into(),
                        encoding: ArgumentValueEncoding::Identity {},
                    },
                    ArgumentFieldMapping {
                        wire_name: "n".into(),
                        canonical_name: "note".into(),
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
#[test]
fn relaxed_schema_keeps_constraints_and_distinguishes_omission_null_and_values() {
    let tool = tool();
    let limits = ProviderToolSchemaLimits::default();
    let compiled = CompiledToolContract::compile(&tool, target(), &Restricted, limits).unwrap();
    assert_eq!(compiled.canonical_name(), &id("search"));
    assert_eq!(compiled.wire_tool().name, id("wire_search"));
    let omitted = compiled
        .decode_arguments(r#"{"q":"ok","n":{"present":false,"value":null}}"#, limits)
        .unwrap();
    assert!(!omitted.contains_key("note"));
    let null = compiled
        .decode_arguments(r#"{"q":"ok","n":{"present":true,"value":null}}"#, limits)
        .unwrap();
    assert_eq!(null["note"], Value::Null);
    tool.validate_model_inputs(&null).unwrap();
    let invalid = compiled
        .decode_arguments(r#"{"q":"x","n":{"present":false,"value":null}}"#, limits)
        .unwrap();
    assert!(tool.validate_model_inputs(&invalid).is_err());
    for raw in [
        r#"{"q":"ok","n":{"present":false,"value":"unexpected"}}"#,
        r#"{"q":"ok","n":{"present":false}}"#,
        r#"{"q":"ok","n":{"present":true}}"#,
        r#"{"q":"ok","n":null}"#,
        r#"{"q":"ok","workspace_id":"forged"}"#,
        r#"{"q":"a","q":"b"}"#,
    ] {
        assert_eq!(
            compiled.decode_arguments(raw, limits).unwrap_err().code,
            ErrorCode::InvalidArguments
        );
    }
    assert!(
        compiled
            .enforcement()
            .iter()
            .all(|item| item.core && item.context_text)
    );
    let fragment = &compiled.constraint_fragments()[0];
    // Confidentiality check of all model-visible surfaces, not prompt-quality testing.
    let visible = format!(
        "{}{}",
        serde_json::to_string(compiled.wire_tool()).unwrap(),
        fragment.text
    );
    for secret in ["workspace_id", "private-workspace", "hidden-secret"] {
        assert!(!visible.contains(secret));
    }
    assert_eq!(
        fragment.digest,
        versioned_digest_json(
            &serde_json::to_string(&fragment.text).unwrap(),
            CanonicalizationVersion::SortedJsonV1,
            JsonTextLimits::default()
        )
        .unwrap()
    );
}
#[test]
fn native_projection_restores_exactly_and_rejects_changed_destination_or_projection() {
    let tool = tool();
    let limits = ProviderToolSchemaLimits::default();
    let compiled =
        CompiledToolContract::compile(&tool, target(), &NativeToolSchemaCompiler, limits).unwrap();
    assert_eq!(compiled.wire_tool(), &tool.to_model_tool());
    assert!(compiled.constraint_fragments().is_empty());
    let text = serde_json::to_string(&compiled).unwrap();
    CompiledToolContract::restore(&text, &tool, &target(), compiled.digest(), limits).unwrap();
    let mut changed = target();
    changed.capability_revision = id("caps-2");
    assert!(
        CompiledToolContract::restore(&text, &tool, &changed, compiled.digest(), limits).is_err()
    );
    let mut record: Value = serde_json::from_str(&text).unwrap();
    record["data"]["wire_tool"]["description"] = json!("modified");
    assert!(
        CompiledToolContract::restore(
            &record.to_string(),
            &tool,
            &target(),
            compiled.digest(),
            limits
        )
        .is_err()
    );
    assert!(
        CompiledToolContract::compile(
            &tool,
            target(),
            &Restricted,
            ProviderToolSchemaLimits {
                max_contract_bytes: 128,
                ..limits
            }
        )
        .is_err()
    );
    assert!(
        CompiledToolContract::compile(
            &tool,
            target(),
            &Restricted,
            ProviderToolSchemaLimits {
                max_schema_depth: 1,
                ..limits
            }
        )
        .is_err()
    );
}

struct Broken(usize);
impl ProviderToolSchemaCompiler for Broken {
    fn reference(&self) -> VersionedRef {
        reference("broken")
    }
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        let mut projected = Restricted.compile(tool, target)?;
        let ArgumentDecodePlan::Fields { fields } = &mut projected.decode_plan else {
            unreachable!()
        };
        match self.0 {
            0 => fields[0].canonical_name = "workspace_id".into(),
            1 => fields[1].wire_name = fields[0].wire_name.clone(),
            2 => {
                fields.pop();
            }
            3 => {
                fields[1].encoding = ArgumentValueEncoding::Presence {
                    present_key: "value".into(),
                    value_key: "value".into(),
                }
            }
            4 => projected.wire_tool.model_input_schema["additionalProperties"] = json!(true),
            _ => projected.wire_tool.name = id("invalid.name"),
        }
        Ok(projected)
    }
}
#[test]
fn compiler_output_cannot_change_ownership_drop_fields_or_ambiguate_presence() {
    for case in 0..6 {
        assert!(
            CompiledToolContract::compile(
                &tool(),
                target(),
                &Broken(case),
                ProviderToolSchemaLimits::default()
            )
            .is_err()
        );
    }
}

#[test]
fn codecs_never_silently_round_numeric_arguments() {
    let mut descriptor = tool().descriptor().clone();
    descriptor.input_schema["properties"]["note"] = json!({"type":"number"});
    let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    let tool = SchemaCompiler::new()
        .compile(descriptor, &registry)
        .unwrap();
    let limits = ProviderToolSchemaLimits::default();
    let compiled =
        CompiledToolContract::compile(&tool, target(), &NativeToolSchemaCompiler, limits).unwrap();
    for number in [
        "18446744073709551617",
        "0.12345678901234567890123456789",
        "1e-999",
    ] {
        assert!(
            compiled
                .decode_arguments(&format!(r#"{{"query":"ok","note":{number}}}"#), limits)
                .is_err(),
            "changed numeric value: {number}"
        );
    }
    for number in ["18446744073709551615", "0.1", "1e2", "100.00", "-0.0"] {
        let decoded = compiled
            .decode_arguments(&format!(r#"{{"query":"ok","note":{number}}}"#), limits)
            .unwrap();
        tool.validate_model_inputs(&decoded).unwrap();
    }
}

struct JsonTextCompiler;
impl ProviderToolSchemaCompiler for JsonTextCompiler {
    fn reference(&self) -> VersionedRef {
        reference("json-text")
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
                model_input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"note":{"type":"string"}},"required":["query","note"],"additionalProperties":false}),
            },
            decode_plan: ArgumentDecodePlan::Fields {
                fields: vec![
                    ArgumentFieldMapping {
                        wire_name: "query".into(),
                        canonical_name: "query".into(),
                        encoding: ArgumentValueEncoding::JsonText { optional: false },
                    },
                    ArgumentFieldMapping {
                        wire_name: "note".into(),
                        canonical_name: "note".into(),
                        encoding: ArgumentValueEncoding::JsonText { optional: true },
                    },
                ],
            },
        })
    }
}
#[test]
fn json_text_values_preserve_omission_null_and_numeric_precision() {
    let tool = tool();
    let limits = ProviderToolSchemaLimits::default();
    let contract =
        CompiledToolContract::compile(&tool, target(), &JsonTextCompiler, limits).unwrap();
    let missing = JsonObject::from([("query".into(), json!("facts"))]);
    let encoded = contract.encode_arguments(&missing).unwrap();
    assert_eq!(encoded["note"], json!("[]"));
    assert_eq!(
        contract
            .decode_arguments(&serde_json::to_string(&encoded).unwrap(), limits)
            .unwrap(),
        missing
    );
    let supplied = JsonObject::from([
        ("query".into(), json!("facts")),
        ("note".into(), Value::Null),
    ]);
    let encoded = contract.encode_arguments(&supplied).unwrap();
    assert_eq!(encoded["note"], json!("[null]"));
    assert_eq!(
        contract
            .decode_arguments(&serde_json::to_string(&encoded).unwrap(), limits)
            .unwrap(),
        supplied
    );
    for value in ["null", "[null,null]", "[", "[1.000000000000000001]"] {
        let raw = json!({"query":"\"facts\"","note":value}).to_string();
        assert_eq!(
            contract.decode_arguments(&raw, limits).unwrap_err().code,
            ErrorCode::InvalidArguments
        );
    }
    let record = serde_json::to_string(&contract).unwrap();
    let restored =
        CompiledToolContract::restore(&record, &tool, &target(), contract.digest(), limits)
            .unwrap();
    assert_eq!(
        restored
            .decode_arguments(&serde_json::to_string(&encoded).unwrap(), limits)
            .unwrap(),
        supplied
    );
}
#[test]
fn qualified_models_are_pinned_without_rewriting_an_older_unqualified_contract() {
    let tool = tool();
    let limits = ProviderToolSchemaLimits::default();
    let legacy =
        CompiledToolContract::compile(&tool, target(), &NativeToolSchemaCompiler, limits).unwrap();
    let mut current = target();
    current.model = Some(reference("model-snapshot"));
    let restored = CompiledToolContract::restore(
        &serde_json::to_string(&legacy).unwrap(),
        &tool,
        &current,
        legacy.digest(),
        limits,
    )
    .unwrap();
    assert!(restored.target().model.is_none());
    assert_eq!(restored.digest(), legacy.digest());
    let modern =
        CompiledToolContract::compile(&tool, current.clone(), &NativeToolSchemaCompiler, limits)
            .unwrap();
    current.model.as_mut().unwrap().version = id("different-release");
    assert!(
        CompiledToolContract::restore(
            &serde_json::to_string(&modern).unwrap(),
            &tool,
            &current,
            modern.digest(),
            limits
        )
        .is_err()
    );
}

#[test]
fn stored_v1_contract_keeps_original_guidance_digest_and_codec_while_new_runs_use_v2() {
    let original = tool();
    let text = include_str!("fixtures/provider-tool-contract-v1.json");
    let saved: Value = parse_json(text).unwrap();
    let digest: JsonDigest = serde_json::from_value(saved["digest"].clone()).unwrap();
    let restored =
        CompiledToolContract::restore(text, &original, &target(), &digest, Default::default())
            .unwrap();
    assert_eq!(serde_json::to_value(&restored).unwrap(), saved);
    let current =
        CompiledToolContract::compile(&original, target(), &Restricted, Default::default())
            .unwrap();
    assert_ne!(restored.digest(), current.digest());
    assert_eq!(restored.wire_tool(), current.wire_tool());
    for raw in [
        r#"{"q":"ok","n":{"present":false,"value":null}}"#,
        r#"{"q":"ok","n":{"present":true,"value":null}}"#,
    ] {
        let before = restored.decode_arguments(raw, Default::default()).unwrap();
        let after = current.decode_arguments(raw, Default::default()).unwrap();
        assert_eq!(before, after);
        original.validate_model_inputs(&after).unwrap();
    }
    let current_text = serde_json::to_string(&current).unwrap();
    let again = CompiledToolContract::restore(
        &current_text,
        &original,
        &target(),
        current.digest(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(again.digest(), current.digest());
    assert!(
        restored
            .decode_arguments(
                r#"{"q":"ok","n":{"present":false,"value":"extra"}}"#,
                Default::default()
            )
            .is_err()
    );
    let mut unknown = saved;
    unknown["data"]["schema_version"] = json!("wickle.provider-tool-contract.v999");
    let changed = versioned_digest_json(
        &serde_json::to_string(&unknown["data"]).unwrap(),
        CanonicalizationVersion::SortedJsonV1,
        JsonTextLimits::default(),
    )
    .unwrap();
    unknown["digest"] = serde_json::to_value(&changed).unwrap();
    assert!(
        CompiledToolContract::restore(
            &serde_json::to_string(&unknown).unwrap(),
            &original,
            &target(),
            &changed,
            Default::default()
        )
        .is_err()
    );
}
