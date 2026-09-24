use serde_json::json;
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
use wickle::*;
use wickle_mcp::*;
mod fixture {
    include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/mcp_fixture.rs"));
}
pub const WORKSPACE: &str = "b9d9051e-70f5-4ed8-b24d-ff8cabcce55a";
pub fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
pub fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
pub fn scope() -> Scope {
    Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    }
}
pub fn python() -> PathBuf {
    let result = std::process::Command::new("python3")
        .args(["-c", "import sys; print(sys.executable)"])
        .output()
        .unwrap();
    assert!(result.status.success());
    PathBuf::from(String::from_utf8(result.stdout).unwrap().trim())
}
pub struct Directory(pub PathBuf);
impl Directory {
    pub fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("wickle-mcp-{}", RandomIdSource.next_id().unwrap()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
pub fn command(dir: &Directory, mode: &str) -> McpCommand {
    McpCommand {
        program: python(),
        args: vec![
            "-u".into(),
            "-c".into(),
            fixture::SERVER.into(),
            mode.into(),
            dir.0.join("calls.jsonl").to_string_lossy().into(),
        ],
        env: BTreeMap::from([("MCP_ALLOWED".into(), "yes".into())]),
        current_dir: None,
    }
}
pub fn registry() -> SystemInputRegistry {
    SystemInputRegistry::new(vec![
        SystemInputDefinition {
            key: id("workspace_id"),
            version: id("1"),
            value_schema: json!({"type":"string","format":"uuid"}),
            source: SystemInputSource::Run {},
        },
        SystemInputDefinition {
            key: id("unused_key"),
            version: id("1"),
            value_schema: json!({"type":"string"}),
            source: SystemInputSource::Run {},
        },
    ])
    .unwrap()
}
pub fn compiled(snapshot: &McpSnapshot, name: &str, effect: ToolSideEffect) -> CompiledTool {
    let mut approval = McpToolApproval::new(
        id("search"),
        id("search"),
        vec!["query".into(), "limit".into()],
    );
    approval.side_effect = effect;
    SchemaCompiler::new()
        .compile(snapshot.descriptor(name, approval).unwrap(), &registry())
        .unwrap()
}
pub fn args() -> JsonObject {
    JsonObject::from([
        ("query".into(), json!("latest")),
        ("limit".into(), json!(5)),
        ("workspace_id".into(), json!(WORKSPACE)),
    ])
}
pub fn context() -> ToolExecutionContext {
    ToolExecutionContext {
        run_id: id("run"),
        binding_set_id: None,
        call_id: id("call"),
        attempt_id: id("attempt"),
        idempotency_key: id("dedupe"),
        scope: scope(),
        principal_ref: id("user"),
        capability_grant_ref: id("grant"),
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(5),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = Directory::new();
    let client = McpClient::connect(
        scope(),
        reference("account"),
        command(&dir, "normal"),
        McpLimits::default(),
        &Default::default(),
        tokio::time::Instant::now() + Duration::from_secs(5),
    )
    .await?;
    let snapshot = client
        .discover(
            &scope(),
            &Default::default(),
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await?;
    let tool = compiled(&snapshot, "db.query", ToolSideEffect::ReadOnly);
    assert!(
        tool.model_input_schema()["properties"]
            .get("workspace_id")
            .is_none()
    );
    assert!(tool.validate_model_inputs(&args()).is_err());
    let executor = client.bind_tool(&snapshot, "db.query", tool)?;
    // This low-level consumer supplies final arguments. The Agent's InputBinder
    // is exercised separately in the integration suite before this port runs.
    let result = executor.execute(&args(), &context()).await?;
    assert!(
        matches!(result.outcome, ToolExecutionOutcome::Succeeded { value } if value == json!({"answer":42}))
    );
    assert_eq!(result.effect, ToolEffect::NotApplied);
    client
        .close(tokio::time::Instant::now() + Duration::from_secs(3))
        .await?;
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
    println!(
        "Extracted MCP package: reviewed stdio tool, hidden system parameter, structured result and closed session passed"
    );
    Ok(())
}
