//! Scoped adapter exports and tracked subprocess lifecycle.
mod support;
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
use support::*;
use wickle::*;
use wickle_mcp::*;
fn metadata(kind: ComponentKind, name: &str) -> ComponentMetadata {
    ComponentMetadata {
        reference: ComponentRef {
            kind,
            id: id(name),
            version: Some(id("1")),
        },
        contract_version: 1,
        manifest_digest: canonical_digest(&json!(name)),
        config_schema: json!({"type":"object","additionalProperties":false}),
        dependencies: vec![],
        capabilities: BTreeSet::new(),
        required_capabilities: BTreeSet::new(),
        required_connections: BTreeSet::new(),
        model_name: None,
        hook_position: None,
        exports: vec![],
    }
}
fn setup(
    snapshot: &McpSnapshot,
    dir: &Directory,
    mode: &str,
) -> (McpAdapterFactory, AdapterInitContext) {
    let compiled = compiled(snapshot, "db.query", ToolSideEffect::ReadOnly);
    let export = ExportMetadata {
        export_id: id("query"),
        kind: ExportKind::Tool,
        contract_version: 1,
        model_name: Some(id("search")),
        hook_position: None,
        capabilities: BTreeSet::new(),
        required_capabilities: BTreeSet::new(),
    };
    let mut meta = metadata(ComponentKind::Adapter, "mcp-adapter");
    meta.required_connections = BTreeSet::from([id("main")]);
    meta.exports = vec![export.clone()];
    let definition = AdapterDefinition {
        metadata: meta,
        exports: vec![AdapterExportDefinition::Tool {
            metadata: export,
            descriptor: Box::new(compiled.descriptor().clone()),
        }],
    };
    let selected = vec![ExportRef {
        adapter_binding: id("mcp"),
        export_id: id("query"),
        alias: Some(id("renamed_query")),
    }];
    let context = AdapterInitContext {
        execution: ComponentBindContext {
            scope: scope(),
            run_id: id("run"),
            session_id: id("session"),
            binding_set_id: id("segment-1"),
            principal_ref: id("user"),
            capability_grant_ref: id("grant"),
            lease: None,
            purpose: ComponentBindPurpose::Execution,
            cancellation: Default::default(),
            deadline: tokio::time::Instant::now() + Duration::from_secs(5),
        },
        binding: ResolvedAdapterBinding {
            binding: AdapterBindingRef {
                binding_id: id("mcp"),
                adapter_id: id("mcp-adapter"),
                version: id("1"),
                config: None,
                connections: BTreeMap::from([(id("main"), id("link"))]),
            },
            definition_digest: definition.digest(),
            definition: definition.clone(),
            connections: BTreeMap::from([(
                id("main"),
                ResolvedConnection {
                    binding: ConnectorBindingRef {
                        binding_id: id("link"),
                        connector_id: id("mcp-stdio"),
                        version: id("1"),
                    },
                    metadata: metadata(ComponentKind::Connector, "mcp-stdio"),
                    connection_ref: reference("account"),
                },
            )]),
            selected_exports: selected.clone(),
            binding_state: None,
        },
        selected_exports: selected,
    };
    let factory = McpAdapterFactory::new(
        definition,
        id("main"),
        command(dir, mode),
        McpLimits::default(),
        snapshot.clone(),
        vec![McpExport {
            export_id: id("query"),
            remote_name: "db.query".into(),
            compiled,
        }],
    )
    .unwrap();
    (factory, context)
}
fn close_context() -> AdapterCloseContext {
    AdapterCloseContext {
        scope: scope(),
        run_id: id("run"),
        binding_set_id: id("segment-1"),
        adapter_binding: id("mcp"),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(3),
    }
}
#[tokio::test]
async fn segment_bound_exports_close_and_reopen_without_reusing_old_executors() {
    let dir = Directory::new();
    let discovery = connect(&dir, "normal", McpLimits::default()).await;
    let snapshot = snapshot(&discovery).await;
    close(&discovery).await;
    let (factory, mut init) = setup(&snapshot, &dir, "normal");
    let instance = factory.open(&init).await.unwrap();
    let pid = latest_pid(&dir);
    let exports = instance.exports();
    assert_eq!(exports.len(), 1);
    let AdapterExportInstance::Tool {
        export_id,
        descriptor,
        executor,
    } = &exports[0]
    else {
        panic!("wrong export")
    };
    assert_eq!(export_id, &id("query"));
    assert_eq!(descriptor.name, id("search"));
    let mut call = context();
    assert_eq!(
        executor.execute(&args(), &call).await.unwrap_err().code,
        ErrorCode::AccessDenied
    );
    call.binding_set_id = Some(id("segment-1"));
    assert!(matches!(
        executor.execute(&args(), &call).await.unwrap().outcome,
        ToolExecutionOutcome::Succeeded { .. }
    ));
    let mut closing = close_context();
    closing.binding_set_id = id("wrong");
    assert_eq!(
        instance.close(&closing).await.unwrap_err().code,
        ErrorCode::AccessDenied
    );
    assert!(process_alive(pid));
    closing.binding_set_id = id("segment-1");
    instance.close(&closing).await.unwrap();
    instance.close(&closing).await.unwrap();
    assert!(!process_alive(pid));
    assert!(matches!(
        executor.execute(&args(), &call).await.unwrap().outcome,
        ToolExecutionOutcome::Failed { .. }
    ));
    assert_eq!(call_count(&dir), 1);
    init.execution.binding_set_id = id("segment-2");
    let fresh = factory.open(&init).await.unwrap();
    call.binding_set_id = Some(id("segment-2"));
    assert_eq!(
        executor.execute(&args(), &call).await.unwrap_err().code,
        ErrorCode::AccessDenied
    );
    let fresh_exports = fresh.exports();
    let AdapterExportInstance::Tool { executor, .. } = &fresh_exports[0] else {
        panic!("wrong export")
    };
    assert!(matches!(
        executor.execute(&args(), &call).await.unwrap().outcome,
        ToolExecutionOutcome::Succeeded { .. }
    ));
    closing.binding_set_id = id("segment-2");
    fresh.close(&closing).await.unwrap();
    assert_eq!(call_count(&dir), 2);
}
#[tokio::test]
async fn unselected_observer_exports_and_invalid_bindings_never_spawn_a_process() {
    let source = Directory::new();
    let discovery = connect(&source, "normal", McpLimits::default()).await;
    let snapshot = snapshot(&discovery).await;
    close(&discovery).await;
    for case in ["scope", "definition", "connection", "config", "observer"] {
        let dir = Directory::new();
        let (factory, mut init) = setup(&snapshot, &dir, "normal");
        match case {
            "scope" => init.execution.scope.workspace_id = id("foreign"),
            "definition" => init.binding.binding.version = id("wrong"),
            "connection" => {
                init.binding
                    .connections
                    .get_mut(&id("main"))
                    .unwrap()
                    .connection_ref
                    .version = id("wrong")
            }
            "config" => {
                init.binding.binding.config =
                    Some(JsonObject::from([("program".into(), json!("not-allowed"))]))
            }
            _ => init.execution.purpose = ComponentBindPurpose::ObserversOnly,
        };
        assert!(factory.open(&init).await.is_err(), "{case}");
        assert!(records(&dir).is_empty());
    }
    let dir = Directory::new();
    let (factory, mut init) = setup(&snapshot, &dir, "normal");
    init.execution.purpose = ComponentBindPurpose::ObserversOnly;
    init.selected_exports.clear();
    let instance = factory.open(&init).await.unwrap();
    assert!(instance.exports().is_empty());
    instance.close(&close_context()).await.unwrap();
    assert!(records(&dir).is_empty());
}

#[tokio::test]
async fn cancelled_close_keeps_cleanup_tracked_and_later_close_waits_for_completion() {
    let dir = Directory::new();
    let discovery = connect(&dir, "normal", McpLimits::default()).await;
    let snapshot = snapshot(&discovery).await;
    close(&discovery).await;
    let (factory, init) = setup(&snapshot, &dir, "normal");
    let instance = factory.open(&init).await.unwrap();
    let cancelled = close_context();
    cancelled.cancellation.cancel();
    assert_eq!(
        instance.close(&cancelled).await.unwrap_err().code,
        ErrorCode::Cancelled
    );
    let closing = close_context();
    let (one, two) = tokio::join!(instance.close(&closing), instance.close(&closing));
    one.unwrap();
    two.unwrap();
    let exports = instance.exports();
    let AdapterExportInstance::Tool { executor, .. } = &exports[0] else {
        panic!("wrong export")
    };
    let mut call = context();
    call.binding_set_id = Some(id("segment-1"));
    assert!(matches!(
        executor.execute(&args(), &call).await.unwrap().outcome,
        ToolExecutionOutcome::Failed { .. }
    ));
    assert_eq!(call_count(&dir), 0);
}
