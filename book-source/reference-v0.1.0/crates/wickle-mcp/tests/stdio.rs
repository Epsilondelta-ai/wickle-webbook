//! Real stdio protocol, input ownership, bounds and effect contracts.
#[path = "support/binding.rs"]
mod binding;
mod support;
use binding::bound_arguments;
use serde_json::json;
use std::time::Duration;
use support::*;
use wickle::*;
use wickle_mcp::*;
#[tokio::test]
async fn reviewed_snapshot_keeps_original_names_versions_and_explicit_input_ownership() {
    let dir = Directory::new();
    let client = connect(&dir, "normal", McpLimits::default()).await;
    let snapshot = snapshot(&client).await;
    assert_eq!(
        snapshot.tool_names().collect::<Vec<_>>(),
        vec!["db.query", "db.write"]
    );
    assert_eq!(
        snapshot.raw_tool("db.query").unwrap()["_meta"]["version"],
        "1"
    );
    assert_eq!(snapshot.server_info()["version"], "1");
    let text = serde_json::to_string(&snapshot).unwrap();
    let restored =
        McpSnapshot::restore(&text, &scope(), &reference("account"), &snapshot.digest()).unwrap();
    assert_eq!(restored.digest(), snapshot.digest());
    let mut foreign = scope();
    foreign.workspace_id = id("foreign");
    assert!(
        McpSnapshot::restore(&text, &foreign, &reference("account"), &snapshot.digest()).is_err()
    );
    let compiled = compiled(&snapshot, "db.query", ToolSideEffect::ReadOnly);
    assert!(
        compiled.model_input_schema()["properties"]
            .get("workspace_id")
            .is_none()
    );
    assert!(
        compiled.model_input_schema()["properties"]
            .get("query")
            .is_some()
    );
    assert!(compiled.validate_model_inputs(&args()).is_err());
    let bound = bound_arguments(&compiled).await.unwrap();
    let executor = client.bind_tool(&snapshot, "db.query", compiled).unwrap();
    let result = executor.execute(&bound, &context()).await.unwrap();
    assert_eq!(result.effect, ToolEffect::NotApplied);
    assert!(
        matches!(result.outcome,ToolExecutionOutcome::Succeeded{value} if value==json!({"answer":42}))
    );
    let records = records(&dir);
    let first = &records[0];
    assert!(first["home"].is_null());
    assert_eq!(first["allowed"], "yes");
    let call = records.iter().find(|v| v.get("call").is_some()).unwrap();
    assert_eq!(call["call"], "db.query");
    assert_eq!(call["args"]["workspace_id"], WORKSPACE);
    assert_eq!(call["args"].as_object().unwrap().len(), 3);
    let pid = latest_pid(&dir);
    close(&client).await;
    close(&client).await;
    assert!(!process_alive(pid));
    assert!(
        client
            .discover(
                &scope(),
                &Default::default(),
                tokio::time::Instant::now() + Duration::from_secs(1)
            )
            .await
            .is_err()
    );
}
#[tokio::test]
async fn selected_descriptor_drift_blocks_dispatch_and_new_tools_do_not_activate() {
    for mode in ["drift", "added"] {
        let dir = Directory::new();
        let client = connect(&dir, mode, McpLimits::default()).await;
        let snapshot = snapshot(&client).await;
        let compiled = compiled(&snapshot, "db.query", ToolSideEffect::ReadOnly);
        let executor = client
            .bind_tool(&snapshot, "db.query", compiled.clone())
            .unwrap();
        let result = executor.execute(&args(), &context()).await.unwrap();
        if mode == "drift" {
            assert!(
                matches!(result.outcome,ToolExecutionOutcome::Failed{code} if code==id("mcp.descriptor_drift"))
            );
            assert_eq!(result.effect, ToolEffect::NotApplied);
            assert_eq!(call_count(&dir), 0);
        } else {
            assert!(matches!(
                result.outcome,
                ToolExecutionOutcome::Succeeded { .. }
            ));
            assert!(client.bind_tool(&snapshot, "db.new", compiled).is_err());
            assert_eq!(call_count(&dir), 1);
        }
        close(&client).await;
    }
}
#[tokio::test]
async fn completed_or_lost_writes_are_unknown_without_host_attestation_and_never_retried() {
    for mode in ["normal", "exit_write", "hang_write"] {
        let dir = Directory::new();
        let client = connect(&dir, mode, McpLimits::default()).await;
        let snapshot = snapshot(&client).await;
        let compiled = compiled(&snapshot, "db.write", ToolSideEffect::Write);
        let executor = client.bind_tool(&snapshot, "db.write", compiled).unwrap();
        let mut context = context();
        context.deadline = tokio::time::Instant::now() + Duration::from_millis(300);
        let result = executor.execute(&args(), &context).await.unwrap();
        assert_eq!(result.effect, ToolEffect::Unknown);
        assert!(result.receipt.is_none());
        assert_eq!(call_count(&dir), 1);
        if mode != "normal" {
            assert_eq!(
                std::fs::read_to_string(dir.0.join("calls.jsonl.effect")).unwrap(),
                "applied\n"
            );
            let result = executor.execute(&args(), &context).await.unwrap();
            assert!(matches!(
                result.outcome,
                ToolExecutionOutcome::Failed { .. }
            ));
            assert_eq!(call_count(&dir), 1);
        }
        close(&client).await;
    }
}
#[tokio::test]
async fn metadata_change_during_read_only_call_does_not_claim_no_effect() {
    let dir = Directory::new();
    let client = connect(&dir, "notify", McpLimits::default()).await;
    let snapshot = snapshot(&client).await;
    let executor = client
        .bind_tool(
            &snapshot,
            "db.query",
            compiled(&snapshot, "db.query", ToolSideEffect::ReadOnly),
        )
        .unwrap();
    let result = executor.execute(&args(), &context()).await.unwrap();
    assert_eq!(result.effect, ToolEffect::Unknown);
    assert!(
        matches!(result.outcome,ToolExecutionOutcome::Failed{code} if code==id("mcp.descriptor_changed_during_call"))
    );
    close(&client).await;
}
#[tokio::test]
async fn protocol_size_duplicate_json_and_remote_errors_are_not_successes() {
    for mode in ["duplicate", "oversized", "tool_error"] {
        let dir = Directory::new();
        let client = connect(
            &dir,
            mode,
            McpLimits {
                max_frame_bytes: 4096,
                ..Default::default()
            },
        )
        .await;
        let snapshot = snapshot(&client).await;
        let executor = client
            .bind_tool(
                &snapshot,
                "db.query",
                compiled(&snapshot, "db.query", ToolSideEffect::ReadOnly),
            )
            .unwrap();
        let result = executor.execute(&args(), &context()).await.unwrap();
        assert!(!format!("{result:?}").contains("private remote error"));
        assert!(
            matches!(result.outcome,ToolExecutionOutcome::Failed{code} if code==id(if mode=="tool_error"{"mcp.remote_error"}else{"mcp.call_failed"}))
        );
        assert_eq!(result.effect, ToolEffect::NotApplied);
        close(&client).await;
    }
}
#[tokio::test]
async fn initialization_version_timeout_and_repeated_cursors_fail_with_cleanup() {
    for mode in ["wrong_version", "hang_init"] {
        let dir = Directory::new();
        let result = McpClient::connect(
            scope(),
            reference("account"),
            command(&dir, mode),
            McpLimits::default(),
            &Default::default(),
            tokio::time::Instant::now() + Duration::from_millis(300),
        )
        .await;
        assert!(result.is_err());
    }
    let dir = Directory::new();
    let client = connect(&dir, "cursor", McpLimits::default()).await;
    assert!(
        client
            .discover(
                &scope(),
                &Default::default(),
                tokio::time::Instant::now() + Duration::from_secs(1)
            )
            .await
            .is_err()
    );
    close(&client).await;
}

#[tokio::test]
async fn server_sampling_requests_do_not_gain_model_execution_capability() {
    let dir = Directory::new();
    let client = connect(&dir, "callback", McpLimits::default()).await;
    let _snapshot = snapshot(&client).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let values = std::fs::read_to_string(dir.0.join("calls.jsonl")).unwrap_or_default();
        if let Some(response) = values
            .lines()
            .filter_map(|v| parse_json(v).ok())
            .find_map(|v| v.get("client_response").cloned())
        {
            assert_eq!(response["id"], "server-sample");
            assert_eq!(response["error"]["code"], -32601);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "sampling request was not rejected"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let init = records(&dir)
        .into_iter()
        .find(|v| v["method"] == "initialize")
        .unwrap();
    assert!(init["params"]["capabilities"].get("sampling").is_none());
    assert_eq!(call_count(&dir), 0);
    close(&client).await;
}

#[tokio::test]
async fn duplicate_response_cannot_replace_reviewed_raw_metadata() {
    let dir = Directory::new();
    let client = connect(&dir, "duplicate_list", McpLimits::default()).await;
    let snapshot = snapshot(&client).await;
    assert_eq!(
        snapshot.raw_tool("db.query").unwrap()["_meta"]["version"],
        "1"
    );
    close(&client).await;
}

#[tokio::test]
async fn abandoning_execute_terminates_in_flight_write_and_prevents_reuse() {
    let dir = Directory::new();
    let client = connect(&dir, "hang_write", McpLimits::default()).await;
    let snapshot = snapshot(&client).await;
    let executor = client
        .bind_tool(
            &snapshot,
            "db.write",
            compiled(&snapshot, "db.write", ToolSideEffect::Write),
        )
        .unwrap();
    let inputs = args();
    let context = context();
    {
        let execution = executor.execute(&inputs, &context);
        tokio::pin!(execution);
        tokio::select! {
            result = &mut execution => panic!("hanging write completed: {result:?}"),
            _ = async {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
                while !dir.0.join("calls.jsonl.effect").exists() {
                    assert!(tokio::time::Instant::now() < deadline);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            } => {}
        }
        // Drop the future without cancelling its token, like an outer timeout.
    }
    assert!(!context.cancellation.is_cancelled());
    let pid = latest_pid(&dir);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while process_alive(pid) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "abandoned operation retained its child"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let result = executor.execute(&inputs, &context).await.unwrap();
    assert!(matches!(
        result.outcome,
        ToolExecutionOutcome::Failed { .. }
    ));
    assert_eq!(call_count(&dir), 1);
    assert_eq!(
        std::fs::read_to_string(dir.0.join("calls.jsonl.effect")).unwrap(),
        "applied\n"
    );
    close(&client).await;
}

#[tokio::test]
async fn bounded_discovery_and_notification_flood_fail_closed() {
    for (mode, limits) in [
        (
            "normal",
            McpLimits {
                max_tools: 1,
                ..Default::default()
            },
        ),
        (
            "pages",
            McpLimits {
                max_pages: 2,
                ..Default::default()
            },
        ),
    ] {
        let dir = Directory::new();
        let client = connect(&dir, mode, limits).await;
        assert!(
            client
                .discover(&scope(), &Default::default(), context().deadline)
                .await
                .is_err()
        );
        close(&client).await;
        assert!(!process_alive(latest_pid(&dir)));
    }
    let dir = Directory::new();
    let client = connect(
        &dir,
        "flood",
        McpLimits {
            max_messages: 8,
            ..Default::default()
        },
    )
    .await;
    let snapshot = snapshot(&client).await;
    let executor = client
        .bind_tool(
            &snapshot,
            "db.query",
            compiled(&snapshot, "db.query", ToolSideEffect::ReadOnly),
        )
        .unwrap();
    assert!(matches!(
        executor.execute(&args(), &context()).await.unwrap().outcome,
        ToolExecutionOutcome::Failed { .. }
    ));
    close(&client).await;
}

#[tokio::test]
async fn content_projection_and_output_limits_are_enforced() {
    for mode in ["text_metadata", "image", "missing_structured", "normal"] {
        let dir = Directory::new();
        let client = connect(&dir, mode, McpLimits::default()).await;
        let snapshot = snapshot(&client).await;
        let mut approval = McpToolApproval::new(
            id("search"),
            id("search"),
            vec!["query".into(), "limit".into()],
        );
        approval.side_effect = ToolSideEffect::ReadOnly;
        if mode == "normal" {
            approval.max_output_bytes = 1.try_into().unwrap();
        }
        let tool = SchemaCompiler::new()
            .compile(
                snapshot.descriptor("db.query", approval).unwrap(),
                &registry(),
            )
            .unwrap();
        let executor = client.bind_tool(&snapshot, "db.query", tool).unwrap();
        let result = executor.execute(&args(), &context()).await.unwrap();
        if mode == "text_metadata" {
            assert!(
                matches!(result.outcome, ToolExecutionOutcome::Succeeded { value } if value == json!({"content":[{"type":"text","text":"found"}]}))
            );
        } else {
            let expected = match mode {
                "image" => "mcp.unsupported_content",
                "missing_structured" => "mcp.missing_structured_output",
                _ => "mcp.output_limit",
            };
            assert!(
                matches!(result.outcome, ToolExecutionOutcome::Failed { code } if code == id(expected))
            );
        }
        close(&client).await;
    }
}

#[tokio::test]
async fn multipage_snapshot_keeps_schema_and_oversized_input_never_dispatches() {
    let dir = Directory::new();
    let client = connect(
        &dir,
        "multipage",
        McpLimits {
            max_frame_bytes: 4096,
            ..Default::default()
        },
    )
    .await;
    let snapshot = snapshot(&client).await;
    assert_eq!(
        snapshot.tool_names().collect::<Vec<_>>(),
        ["db.query", "db.write"]
    );
    let restored = McpSnapshot::restore(
        &serde_json::to_string(&snapshot).unwrap(),
        &scope(),
        &reference("account"),
        &snapshot.digest(),
    )
    .unwrap();
    let compiled = compiled(&restored, "db.query", ToolSideEffect::ReadOnly);
    assert_eq!(
        compiled.descriptor().input_schema["properties"]["workspace_id"]["format"],
        "uuid"
    );
    let mut inputs = args();
    inputs.insert("workspace_id".into(), json!("invalid-uuid"));
    assert!(compiled.validate_execution_inputs(&inputs).is_err());
    let executor = client.bind_tool(&snapshot, "db.query", compiled).unwrap();
    inputs = args();
    inputs.insert("query".into(), json!("x".repeat(5000)));
    let result = executor.execute(&inputs, &context()).await.unwrap();
    assert_eq!(result.effect, ToolEffect::NotApplied);
    assert!(
        matches!(result.outcome, ToolExecutionOutcome::Failed { code } if code == id("mcp.input_limit"))
    );
    assert_eq!(call_count(&dir), 0);
    close(&client).await;
}
