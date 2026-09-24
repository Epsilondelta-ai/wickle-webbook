//! Metadata resolution validates exact selections without opening adapter instances.

#[path = "../../wickle/tests/support/mod.rs"]
#[allow(dead_code)]
mod core_fixture;
#[allow(dead_code)]
mod support;
use serde_json::json;
use std::sync::atomic::Ordering;
use support::*;
use wickle::*;

#[tokio::test]
async fn selecting_a_tool_does_not_activate_declared_context_or_event_exports() {
    let fixture = Fixture::new();
    let registry = fixture.registry().unwrap();
    let assembly = fixture.resolve(&registry, &profile()).await.unwrap();
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    assert!(fixture.events.lock().unwrap().is_empty());
    assert_eq!(
        assembly.adapters()[0].selected_exports,
        vec![ExportRef {
            adapter_binding: id("records"),
            export_id: id("search"),
            alias: Some(id("search_records"))
        }]
    );
    assert_eq!(assembly.tools().len(), 1);
    assert_eq!(
        assembly.tools()[0].compiled.descriptor().name,
        id("search_records")
    );
    assert!(assembly.hooks().is_empty());
}

#[test]
fn duplicate_adapter_and_connection_registrations_are_rejected_without_callbacks() {
    let mut fixture = Fixture::new();
    fixture.definitions.push(fixture.definitions[0].clone());
    fixture.factories.push(fixture.factories[0].clone());
    assert!(fixture.registry().is_err());
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    let mut fixture = Fixture::new();
    fixture.connections.push(fixture.connections[0].clone());
    assert!(fixture.registry().is_err());
    assert!(fixture.events.lock().unwrap().is_empty());
}

#[test]
fn descriptor_export_kind_name_and_protocol_version_must_match_the_registered_definition() {
    for case in 0..4 {
        let mut fixture = Fixture::new();
        match case {
            0 => {
                let AdapterExportDefinition::Tool { metadata, .. } =
                    &mut fixture.definitions[0].exports[0]
                else {
                    unreachable!()
                };
                metadata.kind = ExportKind::ContextSource;
            }
            1 => {
                let AdapterExportDefinition::Tool { descriptor, .. } =
                    &mut fixture.definitions[0].exports[0]
                else {
                    unreachable!()
                };
                descriptor.name = id("other-name");
            }
            2 => fixture.definitions[0].metadata.contract_version = 2,
            3 => {
                fixture.definitions[0].metadata.exports[0].contract_version = 2;
                let AdapterExportDefinition::Tool { metadata, .. } =
                    &mut fixture.definitions[0].exports[0]
                else {
                    unreachable!()
                };
                metadata.contract_version = 2;
            }
            _ => unreachable!(),
        }
        assert!(
            fixture.registry().is_err(),
            "mismatched case {case} must not register"
        );
        assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn wrong_version_config_connection_or_export_selection_fails_before_factory_open() {
    let fixture = Fixture::new();
    let registry = fixture.registry().unwrap();
    for case in 0..5 {
        let mut selected = profile();
        match case {
            0 => selected.adapters.as_mut().unwrap()[0].version = id("2"),
            1 => {
                selected.adapters.as_mut().unwrap()[0].config =
                    Some(object(json!({"collection":42})))
            }
            2 => {
                selected.adapters.as_mut().unwrap()[0]
                    .connections
                    .remove(&id("main"));
            }
            3 => {
                let ToolBindingRef::Export(export) = &mut selected.tools[0] else {
                    unreachable!()
                };
                export.export_id = id("recall"); // Existing export of the wrong kind.
            }
            4 => {
                let ToolBindingRef::Export(export) = &mut selected.tools[0] else {
                    unreachable!()
                };
                export.export_id = id("not-registered");
            }
            _ => unreachable!(),
        }
        assert!(
            fixture.resolve(&registry, &selected).await.is_err(),
            "selection case {case} must fail"
        );
    }
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    assert!(fixture.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn model_alias_collisions_are_rejected_instead_of_overwriting_another_binding() {
    let fixture = Fixture::new();
    let registry = fixture.registry().unwrap();
    let mut selected = profile();
    let mut second = selected.adapters.as_ref().unwrap()[0].clone();
    second.binding_id = id("other-records");
    selected.adapters.as_mut().unwrap().push(second);
    selected.tools.push(ToolBindingRef::Export(ExportRef {
        adapter_binding: id("other-records"),
        export_id: id("search"),
        alias: Some(id("search_records")),
    }));
    assert!(fixture.resolve(&registry, &selected).await.is_err());
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn exact_assembly_rejects_changed_descriptor_connection_and_host_mapping_revisions() {
    for case in 0..3 {
        let mut fixture = Fixture::new();
        let value = json!({"thread_id":"prepared-thread"});
        fixture.states.push(AdapterBindingState {
            scope: scope(),
            session_id: id("session"),
            adapter_binding: id("records"),
            adapter: reference("adapter"),
            definition_digest: fixture.definitions[0].digest(),
            state_ref: ProtectedRecord::new(id("mapping"), 1, value.clone())
                .reference()
                .clone(),
            value,
        });
        let registry = fixture.registry().unwrap();
        let assembly = fixture.resolve(&registry, &profile()).await.unwrap();
        assert_eq!(
            assembly.adapters()[0].binding_state.as_ref().unwrap().value,
            json!({"thread_id":"prepared-thread"})
        );
        assert!(registry.ensure_assembly(&assembly).is_ok());
        match case {
            0 => {
                let AdapterExportDefinition::Tool { descriptor, .. } =
                    &mut fixture.definitions[0].exports[0]
                else {
                    unreachable!()
                };
                descriptor.output_schema = json!({"type":"integer"});
            }
            1 => fixture.connections[0].connection_ref.version = id("different-account-revision"),
            2 => {
                fixture.states[0].value = json!({"thread_id":"replacement-thread"});
                fixture.states[0].state_ref =
                    ProtectedRecord::new(id("mapping"), 2, fixture.states[0].value.clone())
                        .reference()
                        .clone();
            }
            _ => unreachable!(),
        }
        let changed = fixture.registry().unwrap();
        assert!(changed.ensure_assembly(&assembly).is_err());
        assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn binding_state_is_selected_by_scope_session_and_binding_without_creating_a_new_mapping() {
    let mut fixture = Fixture::new();
    let value = json!({"thread_id":"prepared-thread"});
    fixture.states.push(AdapterBindingState {
        scope: scope(),
        session_id: id("session"),
        adapter_binding: id("records"),
        adapter: reference("adapter"),
        definition_digest: fixture.definitions[0].digest(),
        state_ref: ProtectedRecord::new(id("mapping"), 1, value.clone())
            .reference()
            .clone(),
        value,
    });
    let registry = fixture.registry().unwrap();
    let selected = profile();
    let profile = ProfileValidator::new(&RegistryResolver(&registry))
        .validate(&selected, &scope())
        .await
        .unwrap();
    let mut context = resolve_context();
    let original = registry.resolve(&profile, &context).unwrap();
    assert!(original.adapters()[0].binding_state.is_some());
    context.session_id = id("another-session");
    assert!(
        registry.resolve(&profile, &context).unwrap().adapters()[0]
            .binding_state
            .is_none()
    );
    context.scope.tenant_id = id("foreign");
    assert!(registry.resolve(&profile, &context).is_err());
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn missing_system_inputs_fail_but_declared_sources_resolve_without_opening_factories() {
    let fixture = Fixture::new();
    let registry = fixture.registry().unwrap();
    let selected = profile();
    let resolved = ProfileValidator::new(&RegistryResolver(&registry))
        .validate(&selected, &scope())
        .await
        .unwrap();
    let mut context = resolve_context();
    context.system_inputs = SystemInputRegistry::new(vec![]).unwrap();
    assert!(registry.resolve(&resolved, &context).is_err());
    let mut value = serde_json::to_value(&selected).unwrap();
    value["context_sources"] = json!([{"source":{"adapter_binding":"records","export_id":"recall"},"trigger":"run_start","required":false,"timeout_ms":100,"max_items":1,"max_bytes":1024,"max_tokens":100}]);
    let with_source = AgentProfile::from_json(&value.to_string()).unwrap();
    let resolved = ProfileValidator::new(&RegistryResolver(&registry))
        .validate(&with_source, &scope())
        .await
        .unwrap();
    let assembly = registry.resolve(&resolved, &resolve_context()).unwrap();
    assert_eq!(assembly.sources().len(), 1);
    assert_eq!(
        assembly.sources()[0].binding,
        with_source.context_sources.unwrap()[0]
    );
    assert_eq!(assembly.sources()[0].definition.source.id, id("recall"));
    assert_eq!(
        assembly.adapters()[0]
            .selected_exports
            .iter()
            .map(|selection| selection.export_id.clone())
            .collect::<Vec<_>>(),
        vec![id("search"), id("recall")]
    );
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn internally_consistent_restored_profiles_cannot_select_one_logical_export_twice() {
    let fixture = Fixture::new();
    let registry = fixture.registry().unwrap();
    let original = profile();
    let resolved = ProfileValidator::new(&RegistryResolver(&registry))
        .validate(&original, &scope())
        .await
        .unwrap();
    let assembly = registry.resolve(&resolved, &resolve_context()).unwrap();
    let mut changed_profile = original.clone();
    let duplicate = ToolBindingRef::Export(ExportRef {
        adapter_binding: id("records"),
        export_id: id("search"),
        alias: Some(id("second_alias")),
    });
    changed_profile.tools.push(duplicate.clone());
    let mut restored = serde_json::to_value(&resolved).unwrap();
    restored["profile"] = serde_json::to_value(&changed_profile).unwrap();
    restored["profile_digest"] = serde_json::to_value(changed_profile.digest()).unwrap();
    restored["resolution_digest"] = serde_json::to_value(canonical_digest(&json!([
        restored["profile_digest"],
        restored["scope"],
        restored["components"]
    ])))
    .unwrap();
    let forged: ResolvedProfile = serde_json::from_value(restored).unwrap();
    let rejected = registry.resolve(&forged, &resolve_context()).unwrap_err();
    assert_eq!(rejected.path, "assembly.duplicate_export");
    let mut data = serde_json::to_value(&assembly).unwrap();
    data["profile_resolution_digest"] = serde_json::to_value(forged.resolution_digest()).unwrap();
    data["adapters"][0]["selected_exports"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::to_value(&duplicate).unwrap());
    let mut descriptor = assembly.tools()[0].compiled.descriptor().clone();
    descriptor.name = id("second_alias");
    let compiled = SchemaCompiler::new()
        .compile(descriptor, &inputs())
        .unwrap();
    let mut extra = data["tools"][0].clone();
    extra["selection"] = serde_json::to_value(&duplicate).unwrap();
    extra["compiled"] = serde_json::to_value(&compiled).unwrap();
    extra["compiled_digest"] = serde_json::to_value(compiled.digest()).unwrap();
    data["tools"].as_array_mut().unwrap().push(extra);
    let rejected = ResolvedAssembly::restore(
        &data.to_string(),
        &forged,
        &inputs(),
        &canonical_digest(&data),
    )
    .unwrap_err();
    assert_eq!(rejected.path, "assembly.duplicate_export");
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
}
