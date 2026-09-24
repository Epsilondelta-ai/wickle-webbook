# 32장 전체 Rust 구현과 테스트

[강의로](../32-mcp.md) · [전체 변경 패치](../solutions/32-mcp.patch)

기준 `1c4edaa4a5123c8819f319b6e78e11c750f3a40d`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle-mcp/src/client.rs`

```rust
use crate::{
    error,
    snapshot::McpSnapshot,
    transport::{StdioTransport, WireState},
};
use rmcp::{
    ClientHandler, RoleClient, ServiceExt,
    model::{
        CallToolRequestParam, CallToolResult, ClientInfo, Implementation, PaginatedRequestParam,
        ProtocolVersion,
    },
    service::{Peer, RunningService},
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    process::{Child, Command},
    sync::Mutex as AsyncMutex,
};
use tokio_util::sync::CancellationToken;
use wickle::*;
/// Supported Tool protocol subset; not the SDK's implicit LATEST value.
pub const PROTOCOL_VERSION: &str = "2025-06-18";
/// A Host-approved executable, never taken from an Agent profile or model message.
#[derive(Clone)]
pub struct McpCommand {
    /// Absolute executable path. No shell interpretation or PATH lookup.
    pub program: PathBuf,
    /// Literal argument vector.
    pub args: Vec<String>,
    /// Explicit environment only; parent environment is cleared.
    pub env: BTreeMap<String, String>,
    /// Optional explicit working directory.
    pub current_dir: Option<PathBuf>,
}
impl fmt::Debug for McpCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("McpCommand(<protected>)")
    }
}
/// Finite protocol, discovery and cleanup bounds.
#[derive(Clone, Debug)]
pub struct McpLimits {
    /// Maximum one inbound or outbound JSON-RPC frame.
    pub max_frame_bytes: usize,
    /// Maximum inbound messages between explicit operations, including notifications.
    pub max_messages: usize,
    /// Maximum discovered tools across pages.
    pub max_tools: usize,
    /// Maximum metadata pages; repeated cursors also fail.
    pub max_pages: usize,
    /// Additional upper bound on initialization.
    pub connect_timeout: Duration,
    /// Additional upper bound on closing and reaping the child.
    pub close_timeout: Duration,
}
impl Default for McpLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1024 * 1024,
            max_messages: 1024,
            max_tools: 512,
            max_pages: 32,
            connect_timeout: Duration::from_secs(10),
            close_timeout: Duration::from_secs(3),
        }
    }
}
#[derive(Clone)]
pub(crate) struct Handler;
impl ClientHandler for Handler {
    fn get_info(&self) -> ClientInfo {
        ClientInfo {
            protocol_version: ProtocolVersion::V_2025_06_18,
            capabilities: Default::default(),
            client_info: Implementation {
                name: "wickle".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                ..Default::default()
            },
        }
    }
}
type Service = RunningService<RoleClient, Handler>;
struct Session {
    service: Service,
    child: Child,
}
pub(crate) struct Inner {
    pub scope: Scope,
    pub connection_ref: VersionedRef,
    pub limits: McpLimits,
    pub server: Value,
    pub changes: Arc<AtomicU64>,
    pub wire: Arc<WireState>,
    pub gate: AsyncMutex<()>,
    session: Mutex<Option<Session>>,
    close_status: tokio::sync::watch::Sender<Option<Result<(), ErrorCode>>>,
    close_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(mut session) = self
            .session
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            session.service.cancellation_token().cancel();
            let _ = session.child.start_kill();
        }
    }
}
/// Scoped MCP client. Discovery never registers or authorizes a Tool automatically.
#[derive(Clone)]
pub struct McpClient(pub(crate) Arc<Inner>);
impl fmt::Debug for McpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpClient")
            .field("connection_ref", &self.0.connection_ref)
            .finish_non_exhaustive()
    }
}
/// An abandoned protocol operation must never leave a reusable in-flight session.
struct OperationGuard<'a> {
    client: &'a McpClient,
    complete: bool,
}
impl Drop for OperationGuard<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.client.begin_close();
        }
    }
}
impl McpClient {
    /// Spawn a configured child and negotiate only the supported protocol version.
    /// Cancellation/failure drops the child with kill-on-drop enabled.
    pub async fn connect(
        scope: Scope,
        connection_ref: VersionedRef,
        command: McpCommand,
        limits: McpLimits,
        cancellation: &CancellationToken,
        deadline: tokio::time::Instant,
    ) -> Result<Self, ContractError> {
        tokio::runtime::Handle::try_current()
            .map_err(|_| error(ErrorCode::RuntimeUnavailable, "connect"))?;
        if !command.program.is_absolute()
            || limits.max_frame_bytes < 1024
            || limits.max_messages == 0
            || limits.max_tools == 0
            || limits.max_pages == 0
            || limits.connect_timeout.is_zero()
            || limits.close_timeout.is_zero()
        {
            return Err(error(ErrorCode::InvalidConfiguration, "connection"));
        }
        if cancellation.is_cancelled() {
            return Err(error(ErrorCode::Cancelled, "connect"));
        }
        let deadline = deadline.min(tokio::time::Instant::now() + limits.connect_timeout);
        if deadline <= tokio::time::Instant::now() {
            return Err(error(ErrorCode::DeadlineExceeded, "connect"));
        }
        let mut process = Command::new(&command.program);
        process
            .args(&command.args)
            .env_clear()
            .envs(&command.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if let Some(dir) = command.current_dir {
            process.current_dir(dir);
        }
        let mut child = process
            .spawn()
            .map_err(|_| error(ErrorCode::ComponentUnavailable, "spawn"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| error(ErrorCode::ComponentUnavailable, "stdout"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| error(ErrorCode::ComponentUnavailable, "stdin"))?;
        let wire = Arc::new(WireState::default());
        let changes = wire.changes.clone();
        let transport = StdioTransport::new(
            stdout,
            stdin,
            wire.clone(),
            limits.max_frame_bytes,
            limits.max_messages,
        );
        let handler = Handler;
        let service: Service = tokio::select! {biased;
            _=cancellation.cancelled()=>return Err(error(ErrorCode::Cancelled,"connect")),
            _=tokio::time::sleep_until(deadline)=>return Err(error(ErrorCode::DeadlineExceeded,"connect")),
            result=handler.serve(transport)=>result.map_err(|_|error(ErrorCode::ComponentUnavailable,"initialize"))?,
        };
        let info = service
            .peer_info()
            .ok_or_else(|| error(ErrorCode::InvalidContract, "server_info"))?;
        if info.protocol_version != ProtocolVersion::V_2025_06_18
            || info.capabilities.tools.is_none()
        {
            return Err(error(ErrorCode::CapabilityUnsupported, "protocol_or_tools"));
        }
        let server = serde_json::to_value(&info.server_info)
            .map_err(|_| error(ErrorCode::InvalidContract, "server_info"))?;
        Ok(Self(Arc::new(Inner {
            scope,
            connection_ref,
            limits,
            server,
            changes,
            wire,
            gate: AsyncMutex::new(()),
            session: Mutex::new(Some(Session { service, child })),
            close_status: tokio::sync::watch::channel(None).0,
            close_task: Mutex::new(None),
        })))
    }
    /// Owner namespace.
    pub fn scope(&self) -> &Scope {
        &self.0.scope
    }
    /// Host-owned connection revision, without executable paths or credentials.
    pub fn connection_ref(&self) -> &VersionedRef {
        &self.0.connection_ref
    }
    pub(crate) fn peer(&self) -> Result<Peer<RoleClient>, ContractError> {
        self.0
            .session
            .lock()
            .map_err(|_| error(ErrorCode::ComponentUnavailable, "session"))?
            .as_ref()
            .map(|s| s.service.peer().clone())
            .filter(|p| !p.is_transport_closed())
            .ok_or_else(|| error(ErrorCode::ComponentUnavailable, "closed"))
    }
    pub(crate) async fn lock<'a>(
        &'a self,
        cancellation: &CancellationToken,
        deadline: tokio::time::Instant,
    ) -> Result<tokio::sync::MutexGuard<'a, ()>, ContractError> {
        tokio::select! {biased;_=cancellation.cancelled()=>Err(error(ErrorCode::Cancelled,"operation")),_=tokio::time::sleep_until(deadline)=>Err(error(ErrorCode::DeadlineExceeded,"operation")),guard=self.0.gate.lock()=>Ok(guard)}
    }
    /// Discover a bounded raw descriptor snapshot for Host review, not activation.
    pub async fn discover(
        &self,
        scope: &Scope,
        cancellation: &CancellationToken,
        deadline: tokio::time::Instant,
    ) -> Result<McpSnapshot, ContractError> {
        if scope != self.scope() {
            return Err(error(ErrorCode::AccessDenied, "scope"));
        }
        let _guard = self.lock(cancellation, deadline).await?;
        let result = self.discover_locked(cancellation, deadline).await;
        if result.is_err() {
            let _ = self
                .close(tokio::time::Instant::now() + self.0.limits.close_timeout)
                .await;
        }
        result.map(|(snapshot, _)| snapshot)
    }
    pub(crate) async fn discover_locked(
        &self,
        cancellation: &CancellationToken,
        deadline: tokio::time::Instant,
    ) -> Result<(McpSnapshot, u64), ContractError> {
        let mut operation = OperationGuard {
            client: self,
            complete: false,
        };
        self.0.wire.reset();
        let peer = self.peer()?;
        let revision = self.0.changes.load(Ordering::SeqCst);
        let mut cursor = None;
        let mut seen = BTreeSet::new();
        let mut tools = vec![];
        for _ in 0..self.0.limits.max_pages {
            let page = tokio::select! {biased;_=cancellation.cancelled()=>return Err(error(ErrorCode::Cancelled,"discovery")),_=tokio::time::sleep_until(deadline)=>return Err(error(ErrorCode::DeadlineExceeded,"discovery")),result=peer.list_tools(Some(PaginatedRequestParam{cursor}))=>result.map_err(|_|error(ErrorCode::ComponentUnavailable,"discovery"))?};
            let raw = self
                .0
                .wire
                .take_tools()
                .ok_or_else(|| error(ErrorCode::InvalidContract, "raw_descriptors"))?;
            if raw.len() != page.tools.len()
                || tools.len().saturating_add(raw.len()) > self.0.limits.max_tools
            {
                return Err(error(ErrorCode::InvalidContract, "discovery_limit"));
            }
            tools.extend(raw);
            if self.0.changes.load(Ordering::SeqCst) != revision {
                return Err(error(
                    ErrorCode::InvalidToolInputContract,
                    "discovery_changed",
                ));
            }
            match page.next_cursor {
                None => {
                    let snapshot = McpSnapshot::new(
                        self.scope().clone(),
                        self.connection_ref().clone(),
                        self.0.server.clone(),
                        tools,
                    )?;
                    operation.complete = true;
                    return Ok((snapshot, revision));
                }
                Some(next) => {
                    if !seen.insert(next.clone()) {
                        return Err(error(ErrorCode::InvalidContract, "cursor"));
                    }
                    cursor = Some(next);
                }
            }
        }
        Err(error(ErrorCode::InvalidContract, "discovery_pages"))
    }
    pub(crate) async fn call_locked(
        &self,
        name: &str,
        args: &JsonObject,
        cancellation: &CancellationToken,
        deadline: tokio::time::Instant,
    ) -> Result<CallToolResult, ContractError> {
        let mut operation = OperationGuard {
            client: self,
            complete: false,
        };
        self.0.wire.reset();
        let peer = self.peer()?;
        let result = tokio::select! {biased;_=cancellation.cancelled()=>Err(error(ErrorCode::Cancelled,"call")),_=tokio::time::sleep_until(deadline)=>Err(error(ErrorCode::DeadlineExceeded,"call")),result=peer.call_tool(CallToolRequestParam{name:name.to_owned().into(),arguments:Some(args.clone().into_iter().collect())})=>result.map_err(|_|error(ErrorCode::ComponentUnavailable,"call"))};
        operation.complete = result.is_ok();
        result
    }
    fn begin_close(&self) {
        let session = self
            .0
            .session
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(mut session) = session {
            session.service.cancellation_token().cancel();
            let _ = session.child.start_kill();
            let sender = self.0.close_status.clone();
            let timeout = self.0.limits.close_timeout;
            let Ok(runtime) = tokio::runtime::Handle::try_current() else {
                self.0
                    .close_status
                    .send_replace(Some(Err(ErrorCode::RuntimeUnavailable)));
                return;
            };
            let task = runtime.spawn(async move {
                let result = tokio::time::timeout(timeout, async {
                    session
                        .child
                        .wait()
                        .await
                        .map_err(|_| ErrorCode::ComponentUnavailable)?;
                    session
                        .service
                        .cancel()
                        .await
                        .map_err(|_| ErrorCode::ComponentUnavailable)?;
                    Ok(())
                })
                .await
                .unwrap_or(Err(ErrorCode::DeadlineExceeded));
                sender.send_replace(Some(result));
            });
            *self.0.close_task.lock().unwrap_or_else(|e| e.into_inner()) = Some(task);
        }
    }
    /// Cancel transport work, terminate and reap the direct child, and await service shutdown.
    /// Idempotent; a timeout is an error, never reported as a successful close.
    pub async fn close(&self, deadline: tokio::time::Instant) -> Result<(), ContractError> {
        self.close_with(deadline, &CancellationToken::new()).await
    }
    pub(crate) async fn close_with(
        &self,
        deadline: tokio::time::Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ContractError> {
        let mut progress = self.0.close_status.subscribe();
        self.begin_close();
        loop {
            if let Some(result) = *progress.borrow() {
                return result.map_err(|code| error(code, "close"));
            }
            tokio::select! {biased;
                _=cancellation.cancelled()=>return Err(error(ErrorCode::Cancelled,"close")),
                _=tokio::time::sleep_until(deadline)=>return Err(error(ErrorCode::DeadlineExceeded,"close")),
                result=progress.changed()=>result.map_err(|_|error(ErrorCode::ComponentUnavailable,"close"))?,
            }
        }
    }
}
```

## `crates/wickle-mcp/src/executor.rs`

```rust
use crate::{McpClient, McpSnapshot, error};
use serde_json::{Value, json};
use std::sync::atomic::Ordering;
use wickle::*;
/// One approved remote Tool. It sends only already-bound execution arguments.
#[derive(Clone)]
pub struct McpToolExecutor {
    pub(crate) client: McpClient,
    pub(crate) snapshot: McpSnapshot,
    pub(crate) remote: String,
    pub(crate) compiled: CompiledTool,
    pub(crate) segment: Option<(Id, Id)>,
}
impl McpClient {
    /// Bind an explicitly reviewed/compiled descriptor. This never chooses exposure
    /// or permissions from remote annotations and never activates another Tool.
    pub fn bind_tool(
        &self,
        snapshot: &McpSnapshot,
        remote: &str,
        compiled: CompiledTool,
    ) -> Result<McpToolExecutor, ContractError> {
        if snapshot.scope() != self.scope() || snapshot.connection_ref() != self.connection_ref() {
            return Err(error(ErrorCode::AccessDenied, "snapshot_scope"));
        }
        snapshot.validate_descriptor(remote, compiled.descriptor())?;
        Ok(McpToolExecutor {
            client: self.clone(),
            snapshot: snapshot.clone(),
            remote: remote.into(),
            compiled,
            segment: None,
        })
    }
}
fn failed(code: &str, effect: ToolEffect) -> ToolExecutionResult {
    ToolExecutionResult {
        outcome: ToolExecutionOutcome::Failed {
            code: Id::new(code).expect("static code"),
        },
        effect,
        receipt: None,
    }
}
impl ToolExecutor for McpToolExecutor {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            if context.scope != *self.client.scope()
                || self.segment.as_ref().is_some_and(|(run, segment)| {
                    run != &context.run_id || context.binding_set_id.as_ref() != Some(segment)
                })
            {
                return Err(error(ErrorCode::AccessDenied, "execution_scope"));
            }
            if self.compiled.validate_execution_inputs(args).is_err() {
                return Ok(failed("mcp.invalid_arguments", ToolEffect::NotApplied));
            }
            let request = json!({"jsonrpc":"2.0","id":u32::MAX,"method":"tools/call","params":{"name":self.remote,"arguments":args}});
            if serde_json::to_vec(&request)
                .map_err(|_| error(ErrorCode::InvalidJson, "arguments"))?
                .len()
                > self.client.0.limits.max_frame_bytes
            {
                return Ok(failed("mcp.input_limit", ToolEffect::NotApplied));
            }
            let _guard = match self
                .client
                .lock(&context.cancellation, context.deadline)
                .await
            {
                Ok(guard) => guard,
                Err(_) => {
                    return Ok(failed(
                        "mcp.before_dispatch_cancelled",
                        ToolEffect::NotApplied,
                    ));
                }
            };
            let (current, revision) = match self
                .client
                .discover_locked(&context.cancellation, context.deadline)
                .await
            {
                Ok(value) => value,
                Err(_) => {
                    let _ = self
                        .client
                        .close(tokio::time::Instant::now() + self.client.0.limits.close_timeout)
                        .await;
                    return Ok(failed("mcp.discovery_failed", ToolEffect::NotApplied));
                }
            };
            if !self.snapshot.matches_tool(&current, &self.remote)
                || self.client.0.changes.load(Ordering::SeqCst) != revision
            {
                return Ok(failed("mcp.descriptor_drift", ToolEffect::NotApplied));
            }
            if context.cancellation.is_cancelled()
                || tokio::time::Instant::now() >= context.deadline
            {
                return Ok(failed(
                    "mcp.before_dispatch_cancelled",
                    ToolEffect::NotApplied,
                ));
            }
            let effect = if self.compiled.descriptor().side_effect == ToolSideEffect::ReadOnly {
                ToolEffect::NotApplied
            } else {
                ToolEffect::Unknown
            };
            let result = match self
                .client
                .call_locked(&self.remote, args, &context.cancellation, context.deadline)
                .await
            {
                Ok(result) => result,
                Err(_) => {
                    let _ = self
                        .client
                        .close(tokio::time::Instant::now() + self.client.0.limits.close_timeout)
                        .await;
                    return Ok(failed(
                        "mcp.call_failed",
                        if self.client.0.changes.load(Ordering::SeqCst) != revision {
                            ToolEffect::Unknown
                        } else {
                            effect
                        },
                    ));
                }
            };
            if self.client.0.changes.load(Ordering::SeqCst) != revision {
                return Ok(failed(
                    "mcp.descriptor_changed_during_call",
                    ToolEffect::Unknown,
                ));
            }
            if result.is_error == Some(true) {
                return Ok(failed("mcp.remote_error", effect));
            }
            let value = if self
                .snapshot
                .raw_tool(&self.remote)
                .and_then(|v| v.get("outputSchema"))
                .is_some_and(|v| !v.is_null())
            {
                match result.structured_content {
                    Some(value) => value,
                    None => return Ok(failed("mcp.missing_structured_output", effect)),
                }
            } else {
                let content = serde_json::to_value(result.content)
                    .map_err(|_| error(ErrorCode::InvalidContract, "content"))?;
                let Some(items) = content.as_array() else {
                    return Ok(failed("mcp.invalid_content", effect));
                };
                if items.iter().any(|v| {
                    v.get("type") != Some(&json!("text"))
                        || v.get("text").and_then(Value::as_str).is_none()
                }) {
                    return Ok(failed("mcp.unsupported_content", effect));
                }
                json!({"content":items.iter().map(|v|json!({"type":"text","text":v["text"]})).collect::<Vec<_>>()})
            };
            if serde_json::to_vec(&value)
                .map_err(|_| error(ErrorCode::InvalidJson, "output"))?
                .len() as u64
                > self.compiled.descriptor().max_output_bytes.get()
            {
                return Ok(failed("mcp.output_limit", effect));
            }
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Succeeded { value },
                effect,
                receipt: None,
            })
        })
    }
}
```

## `crates/wickle-mcp/src/factory.rs`

```rust
use crate::{McpClient, McpCommand, McpLimits, McpSnapshot, error};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use wickle::*;
/// One Host-approved export, retaining the original remote name and compiled contract.
#[derive(Clone)]
pub struct McpExport {
    /// Adapter-local export identifier.
    pub export_id: Id,
    /// Original MCP Tool name, never a model-supplied execution target.
    pub remote_name: String,
    /// Original compiled descriptor before an optional profile alias.
    pub compiled: CompiledTool,
}
/// Opens one scoped stdio client per execution segment from Host-owned configuration.
pub struct McpAdapterFactory {
    definition: AdapterDefinition,
    connection_name: Id,
    command: McpCommand,
    limits: McpLimits,
    snapshot: McpSnapshot,
    exports: BTreeMap<Id, McpExport>,
}
impl McpAdapterFactory {
    /// Register reviewed metadata and compiled exports. Remote discovery cannot add
    /// capabilities or executable paths to this immutable factory.
    pub fn new(
        definition: AdapterDefinition,
        connection_name: Id,
        command: McpCommand,
        limits: McpLimits,
        snapshot: McpSnapshot,
        exports: Vec<McpExport>,
    ) -> Result<Self, ContractError> {
        if definition.metadata.reference.kind != ComponentKind::Adapter
            || definition.metadata.reference.version.is_none()
            || definition.metadata.required_connections != BTreeSet::from([connection_name.clone()])
        {
            return Err(error(ErrorCode::InvalidReference, "connection_definition"));
        }
        let mut selected = BTreeMap::new();
        for export in exports {
            snapshot.validate_descriptor(&export.remote_name, export.compiled.descriptor())?;
            if selected.insert(export.export_id.clone(), export).is_some() {
                return Err(error(ErrorCode::InvalidReference, "duplicate_export"));
            }
        }
        if definition.exports.len() != selected.len() {
            return Err(error(ErrorCode::InvalidReference, "exports"));
        }
        for definition in &definition.exports {
            let AdapterExportDefinition::Tool {
                metadata,
                descriptor,
            } = definition
            else {
                return Err(error(ErrorCode::CapabilityUnsupported, "export_kind"));
            };
            let export = selected
                .get(&metadata.export_id)
                .ok_or_else(|| error(ErrorCode::InvalidReference, "export"))?;
            if descriptor.as_ref() != export.compiled.descriptor() {
                return Err(error(
                    ErrorCode::InvalidToolInputContract,
                    "export_descriptor",
                ));
            }
        }
        Ok(Self {
            definition,
            connection_name,
            command,
            limits,
            snapshot,
            exports: selected,
        })
    }
}
impl AdapterFactory for McpAdapterFactory {
    fn open<'a>(
        &'a self,
        context: &'a AdapterInitContext,
    ) -> PortFuture<'a, Arc<dyn AdapterInstance>> {
        Box::pin(async move {
            if context.execution.scope != *self.snapshot.scope() {
                return Err(error(ErrorCode::AccessDenied, "factory_scope"));
            }
            if context.binding.binding.adapter_id != self.definition.metadata.reference.id
                || Some(&context.binding.binding.version)
                    != self.definition.metadata.reference.version.as_ref()
                || context.binding.definition != self.definition
                || context.binding.definition_digest != self.definition.digest()
                || context
                    .binding
                    .connections
                    .get(&self.connection_name)
                    .map(|c| &c.connection_ref)
                    != Some(self.snapshot.connection_ref())
                || context
                    .binding
                    .binding
                    .config
                    .as_ref()
                    .is_some_and(|v| !v.is_empty())
            {
                return Err(error(ErrorCode::InvalidReference, "factory_binding"));
            }
            if context.execution.purpose == ComponentBindPurpose::ObserversOnly
                && !context.selected_exports.is_empty()
            {
                return Err(error(ErrorCode::AccessDenied, "observer_tools"));
            }
            let binding = context.binding.binding.binding_id.clone();
            let mut chosen = vec![];
            for selection in &context.selected_exports {
                if selection.adapter_binding != binding {
                    return Err(error(ErrorCode::InvalidReference, "selection"));
                }
                chosen.push(
                    self.exports
                        .get(&selection.export_id)
                        .ok_or_else(|| error(ErrorCode::InvalidReference, "selection"))?,
                );
            }
            let mut outputs = vec![];
            let mut client = None;
            if !chosen.is_empty() {
                let opened = McpClient::connect(
                    context.execution.scope.clone(),
                    self.snapshot.connection_ref().clone(),
                    self.command.clone(),
                    self.limits.clone(),
                    &context.execution.cancellation,
                    context.execution.deadline,
                )
                .await?;
                let current = opened
                    .discover(
                        &context.execution.scope,
                        &context.execution.cancellation,
                        context.execution.deadline,
                    )
                    .await?;
                if chosen
                    .iter()
                    .any(|e| !self.snapshot.matches_tool(&current, &e.remote_name))
                {
                    let _ = opened
                        .close(tokio::time::Instant::now() + self.limits.close_timeout)
                        .await;
                    return Err(error(
                        ErrorCode::InvalidToolInputContract,
                        "descriptor_drift",
                    ));
                }
                for export in chosen {
                    let mut executor = opened.bind_tool(
                        &self.snapshot,
                        &export.remote_name,
                        export.compiled.clone(),
                    )?;
                    executor.segment = Some((
                        context.execution.run_id.clone(),
                        context.execution.binding_set_id.clone(),
                    ));
                    outputs.push(AdapterExportInstance::Tool {
                        export_id: export.export_id.clone(),
                        descriptor: Box::new(export.compiled.descriptor().clone()),
                        executor: Arc::new(executor),
                    });
                }
                client = Some(opened);
            }
            Ok(Arc::new(Instance {
                scope: context.execution.scope.clone(),
                run: context.execution.run_id.clone(),
                segment: context.execution.binding_set_id.clone(),
                binding,
                client,
                outputs,
            }) as Arc<dyn AdapterInstance>)
        })
    }
}
struct Instance {
    scope: Scope,
    run: Id,
    segment: Id,
    binding: Id,
    client: Option<McpClient>,
    outputs: Vec<AdapterExportInstance>,
}
impl AdapterInstance for Instance {
    fn exports(&self) -> Vec<AdapterExportInstance> {
        self.outputs.clone()
    }
    fn close<'a>(&'a self, context: &'a AdapterCloseContext) -> PortFuture<'a, ()> {
        Box::pin(async move {
            if context.scope != self.scope
                || context.run_id != self.run
                || context.binding_set_id != self.segment
                || context.adapter_binding != self.binding
            {
                return Err(error(ErrorCode::AccessDenied, "close_scope"));
            }
            if let Some(client) = &self.client {
                client
                    .close_with(context.deadline, &context.cancellation)
                    .await?;
            }
            Ok(())
        })
    }
}
```

## `crates/wickle-mcp/src/lib.rs`

```rust
//! Scope-bound, reviewed MCP Tools over bounded stdio connections.
#![forbid(unsafe_code)]
mod client;
mod executor;
mod factory;
mod snapshot;
mod transport;
pub use client::{McpClient, McpCommand, McpLimits};
pub use executor::McpToolExecutor;
pub use factory::{McpAdapterFactory, McpExport};
pub use snapshot::{McpSnapshot, McpToolApproval};
use wickle::{ContractError, ErrorCode};
fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("mcp.{location}"))
}
```

## `crates/wickle-mcp/src/snapshot.rs`

```rust
use crate::{client::PROTOCOL_VERSION, error};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, fmt, num::NonZeroU64};
use wickle::*;
/// Raw discovery result for protected Host review. It grants no execution permission.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpSnapshot {
    scope: Scope,
    connection_ref: VersionedRef,
    protocol: String,
    server: Value,
    tools: BTreeMap<String, Value>,
}
impl fmt::Debug for McpSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpSnapshot")
            .field("tool_count", &self.tools.len())
            .finish_non_exhaustive()
    }
}
/// Explicit Host choices. Input ownership and effects are never inferred from names or hints.
#[derive(Clone, Debug)]
pub struct McpToolApproval {
    /// Host-assigned stable Tool identity.
    pub tool_id: Id,
    /// Portable model-facing name; separate from the original MCP name.
    pub model_name: Id,
    /// Explicit model-owned property names, including an intentional empty list.
    pub agent_parameters: Vec<String>,
    /// Optional hidden parameter -> registered system key mapping.
    pub system_bindings: Option<BTreeMap<String, Id>>,
    /// Optional reviewed description replacing the remote description.
    pub description: Option<String>,
    /// Trusted Host classification; defaults to Unknown.
    pub side_effect: ToolSideEffect,
    /// Bounded complete output size.
    pub max_output_bytes: NonZeroU64,
}
impl McpToolApproval {
    /// Start with no retry or effect assumptions. The caller must select input ownership.
    pub fn new(tool_id: Id, model_name: Id, agent_parameters: Vec<String>) -> Self {
        Self {
            tool_id,
            model_name,
            agent_parameters,
            system_bindings: None,
            description: None,
            side_effect: ToolSideEffect::Unknown,
            max_output_bytes: NonZeroU64::new(1024 * 1024).expect("nonzero"),
        }
    }
}
impl McpSnapshot {
    pub(crate) fn new(
        scope: Scope,
        connection_ref: VersionedRef,
        server: Value,
        tools: Vec<Value>,
    ) -> Result<Self, ContractError> {
        if !server
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty())
            || !server
                .get("version")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty())
        {
            return Err(error(ErrorCode::InvalidContract, "server_identity"));
        }
        let mut map = BTreeMap::new();
        for raw in tools {
            let parsed: rmcp::model::Tool = serde_json::from_value(raw.clone())
                .map_err(|_| error(ErrorCode::InvalidToolInputContract, "descriptor"))?;
            let name = parsed.name.to_string();
            if name.is_empty()
                || name.len() > 256
                || name.chars().any(char::is_control)
                || map.insert(name, raw).is_some()
            {
                return Err(error(ErrorCode::InvalidToolInputContract, "tool_name"));
            }
        }
        Ok(Self {
            scope,
            connection_ref,
            protocol: PROTOCOL_VERSION.into(),
            server,
            tools: map,
        })
    }
    /// Scope for which discovery was authorized.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Exact Host connection revision observed during discovery.
    pub fn connection_ref(&self) -> &VersionedRef {
        &self.connection_ref
    }
    /// Original server implementation metadata; never automatically sent to the model.
    pub fn server_info(&self) -> &Value {
        &self.server
    }
    /// Original remote names, without automatically creating model tools.
    pub fn tool_names(&self) -> impl Iterator<Item = &str> {
        self.tools.keys().map(String::as_str)
    }
    /// Privileged access to the complete raw descriptor, including unknown metadata.
    pub fn raw_tool(&self, name: &str) -> Option<&Value> {
        self.tools.get(name)
    }
    /// Integrity identity for protected persistence and review.
    pub fn digest(&self) -> JsonDigest {
        canonical_digest(&serde_json::to_value(self).expect("data snapshot"))
    }
    /// Restore only against a trusted expected digest and owner/connection identity.
    pub fn restore(
        input: &str,
        scope: &Scope,
        connection: &VersionedRef,
        expected: &JsonDigest,
    ) -> Result<Self, ContractError> {
        let value: Self = serde_json::from_value(parse_json(input)?)
            .map_err(|_| error(ErrorCode::InvalidContract, "snapshot"))?;
        if value.digest() != *expected
            || value.scope != *scope
            || value.connection_ref != *connection
            || value.protocol != PROTOCOL_VERSION
        {
            return Err(error(ErrorCode::InvalidReference, "snapshot"));
        }
        let rebuilt = Self::new(
            value.scope.clone(),
            value.connection_ref.clone(),
            value.server.clone(),
            value.tools.values().cloned().collect(),
        )?;
        if rebuilt.tools != value.tools {
            return Err(error(ErrorCode::InvalidContract, "snapshot_names"));
        }
        Ok(value)
    }
    /// Exact version of one original Tool contract and server identity.
    pub fn tool_version(&self, name: &str) -> Result<Id, ContractError> {
        let raw = self
            .tools
            .get(name)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool"))?;
        Id::new(
            canonical_digest(&json!({"protocol":self.protocol,"server":self.server,"tool":raw}))
                .as_str(),
        )
    }
    /// Convert one reviewed Tool into a core descriptor. Compile it with the core's
    /// ToolSchemaCompiler and registered system definitions before activation.
    pub fn descriptor(
        &self,
        name: &str,
        approval: McpToolApproval,
    ) -> Result<ToolDescriptor, ContractError> {
        let raw = self
            .tools
            .get(name)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool"))?;
        Ok(ToolDescriptor {
            tool: VersionedRef {
                id: approval.tool_id,
                version: self.tool_version(name)?,
            },
            name: approval.model_name,
            description: approval.description.unwrap_or_else(|| {
                raw.get("description")
                    .and_then(Value::as_str)
                    .unwrap_or(name)
                    .into()
            }),
            input_schema: raw["inputSchema"].clone(),
            agent_parameters: approval.agent_parameters,
            system_bindings: approval.system_bindings,
            output_schema: self.output_schema(name)?,
            side_effect: approval.side_effect,
            concurrency: ToolConcurrency::Serial,
            retry: ToolRetryPolicy::Never,
            reconcile: false,
            max_output_bytes: approval.max_output_bytes,
        })
    }
    pub(crate) fn output_schema(&self, name: &str) -> Result<Value, ContractError> {
        let raw = self
            .tools
            .get(name)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool"))?;
        Ok(raw.get("outputSchema").filter(|v|!v.is_null()).cloned().unwrap_or_else(||json!({"type":"object","properties":{"content":{"type":"array","items":{"type":"object","properties":{"type":{"const":"text"},"text":{"type":"string"}},"required":["type","text"],"additionalProperties":false}}},"required":["content"],"additionalProperties":false})))
    }
    pub(crate) fn validate_descriptor(
        &self,
        name: &str,
        descriptor: &ToolDescriptor,
    ) -> Result<(), ContractError> {
        let raw = self
            .tools
            .get(name)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool"))?;
        if descriptor.tool.version != self.tool_version(name)?
            || descriptor.input_schema != raw["inputSchema"]
            || descriptor.output_schema != self.output_schema(name)?
            || descriptor.concurrency != ToolConcurrency::Serial
            || descriptor.retry != ToolRetryPolicy::Never
            || descriptor.reconcile
        {
            return Err(error(
                ErrorCode::InvalidToolInputContract,
                "approved_descriptor",
            ));
        }
        Ok(())
    }
    pub(crate) fn matches_tool(&self, current: &Self, name: &str) -> bool {
        self.scope == current.scope
            && self.connection_ref == current.connection_ref
            && self.protocol == current.protocol
            && self.server == current.server
            && self.tools.contains_key(name)
            && self.tools.get(name) == current.tools.get(name)
    }
}
```

## `crates/wickle-mcp/src/transport.rs`

```rust
use futures_util::StreamExt;
use rmcp::{
    RoleClient,
    service::{RxJsonRpcMessage, TxJsonRpcMessage},
    transport::Transport,
};
use serde_json::Value;
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};
use tokio::{
    io::AsyncWriteExt,
    process::{ChildStdin, ChildStdout},
    sync::Mutex as AsyncMutex,
};
use tokio_util::codec::{FramedRead, LinesCodec};

#[derive(Default)]
struct Discovery {
    id: Option<Value>,
    tools: Option<Vec<Value>>,
}
#[derive(Default)]
pub(crate) struct WireState {
    pub poisoned: AtomicBool,
    pub changes: Arc<AtomicU64>,
    messages: AtomicUsize,
    discovery: Mutex<Discovery>,
}
impl WireState {
    pub fn reset(&self) {
        self.messages.store(0, Ordering::SeqCst);
    }
    pub fn take_tools(&self) -> Option<Vec<Value>> {
        self.discovery.lock().ok()?.tools.take()
    }
}
pub(crate) struct StdioTransport {
    read: FramedRead<ChildStdout, LinesCodec>,
    write: Arc<AsyncMutex<Option<ChildStdin>>>,
    state: Arc<WireState>,
    max_frame: usize,
    max_messages: usize,
}
impl StdioTransport {
    pub fn new(
        read: ChildStdout,
        write: ChildStdin,
        state: Arc<WireState>,
        max_frame: usize,
        max_messages: usize,
    ) -> Self {
        Self {
            read: FramedRead::new(read, LinesCodec::new_with_max_length(max_frame)),
            write: Arc::new(AsyncMutex::new(Some(write))),
            state,
            max_frame,
            max_messages,
        }
    }
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid MCP frame")
}
impl Transport<RoleClient> for StdioTransport {
    type Error = io::Error;
    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = Result<(), io::Error>> + Send + 'static {
        let value = serde_json::to_value(item).map_err(|_| invalid());
        let state = self.state.clone();
        let write = self.write.clone();
        let max = self.max_frame;
        async move {
            if state.poisoned.load(Ordering::SeqCst) {
                return Err(invalid());
            }
            let value = value?;
            let mut bytes = serde_json::to_vec(&value).map_err(|_| invalid())?;
            if bytes.len() > max {
                state.poisoned.store(true, Ordering::SeqCst);
                return Err(invalid());
            }
            if value.get("method").and_then(Value::as_str) == Some("tools/list") {
                let mut discovery = state.discovery.lock().map_err(|_| invalid())?;
                discovery.id = value.get("id").cloned();
                discovery.tools = None;
            }
            bytes.push(b'\n');
            let mut write = write.lock().await;
            let writer = write.as_mut().ok_or_else(invalid)?;
            writer.write_all(&bytes).await?;
            writer.flush().await
        }
    }
    async fn receive(&mut self) -> Option<RxJsonRpcMessage<RoleClient>> {
        let line = match self.read.next().await {
            Some(Ok(line)) => line,
            Some(Err(_)) => {
                self.state.poisoned.store(true, Ordering::SeqCst);
                return None;
            }
            None => return None,
        };
        if self.state.messages.fetch_add(1, Ordering::SeqCst) >= self.max_messages {
            self.state.poisoned.store(true, Ordering::SeqCst);
            return None;
        }
        let result = (|| {
            let value = wickle::parse_json(&line).map_err(|_| invalid())?;
            if !value.is_object() || value.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
                return Err(invalid());
            }
            if value.get("method").and_then(Value::as_str)
                == Some("notifications/tools/list_changed")
            {
                self.state.changes.fetch_add(1, Ordering::SeqCst);
            }
            let mut discovery = self.state.discovery.lock().map_err(|_| invalid())?;
            if value.get("method").is_none()
                && discovery.id.is_some()
                && value.get("id") == discovery.id.as_ref()
            {
                discovery.id = None;
                if let Some(tools) = value.pointer("/result/tools").and_then(Value::as_array) {
                    discovery.tools = Some(tools.clone());
                }
            }
            serde_json::from_value(value).map_err(|_| invalid())
        })();
        match result {
            Ok(message) => Some(message),
            Err(_) => {
                self.state.poisoned.store(true, Ordering::SeqCst);
                None
            }
        }
    }
    async fn close(&mut self) -> Result<(), io::Error> {
        let mut write = self.write.lock().await;
        if let Some(mut writer) = write.take() {
            writer.shutdown().await?;
        }
        Ok(())
    }
}
```

## `crates/wickle-mcp/tests/factory.rs`

```rust
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
```

## `crates/wickle-mcp/tests/stdio.rs`

```rust
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
```

## `crates/wickle-mcp/tests/support/binding.rs`

```rust
use super::*;
use std::{collections::BTreeSet, sync::Arc};
struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        r: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: r.version.clone().or_else(|| Some(id("1"))),
                    ..r.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!(r.id)),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (r.kind == ComponentKind::Tool).then(|| r.id.clone()),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

struct Owned;
impl PolicyPort for Owned {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::ExecuteTool { input } = &request.action {
                if input.execution_args().get("workspace_id") != Some(&json!(WORKSPACE)) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("wrong_workspace"),
                    });
                }
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}
async fn plan(
    store: &MemoryStateStore,
    scope: &Scope,
    run: &Id,
    lease: &RunLease,
    clock: &SystemClock,
    compiled: &CompiledTool,
    model_inputs: JsonObject,
) -> Result<(), ContractError> {
    let call_id = "call";
    let saved = store.load(scope, run).await?;
    let mut snapshot = saved.snapshot;
    let expected_revision = snapshot.revision;
    snapshot.revision += 1;
    snapshot.phase = RunPhase::Tool;
    let call = ToolCall {
        call_id: id(call_id),
        model_request_id: id("model-request"),
        provider_call_id: id(&format!("provider-{call_id}")),
        tool_name: compiled.descriptor().name.clone(),
        model_inputs,
        descriptor_digest: Some(compiled.descriptor_digest().clone()),
        bound_input_ref: None,
    };
    snapshot.tool_ledger.push(ToolLedgerEntry {
        call,
        state: ToolCallState::Planned {},
    });
    store
        .commit(
            scope,
            run,
            CommitInput {
                expected_revision,
                lease: lease.clone(),
                now_ms: clock.now()?.utc_ms,
                snapshot,
                messages: vec![],
                events: vec![],
                records: vec![],
            },
        )
        .await?;
    Ok(())
}

pub async fn bound_arguments(
    compiled: &CompiledTool,
) -> Result<JsonObject, Box<dyn std::error::Error + Send + Sync>> {
    let scope = scope();
    let registry = Arc::new(registry());
    let values = SystemInputs::new(JsonObject::from([
        ("workspace_id".into(), json!(WORKSPACE)),
        ("unused_key".into(), json!("must-not-be-sent")),
    ]));
    let captured =
        RunSystemInputs::capture(scope.clone(), Some(values.clone()), &registry).unwrap();
    let input_record = captured.to_record(id("run-inputs"), 1);
    let input_ref = captured.snapshot_ref(input_record.reference()).unwrap();
    let profile=AgentProfile::from_json(&json!({"schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1","name":"Assistant","description":"MCP binding fixture","instructions":{"text":"Use available evidence"},"model_binding":"primary","tools":[{"tool_id":compiled.descriptor().tool.id,"version":compiled.descriptor().tool.version}],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},"limits":{"max_model_calls":4,"max_tool_attempts":4,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}}).to_string()).unwrap();
    let profile = ProfileValidator::new(&Catalog)
        .validate(&profile, &scope)
        .await
        .unwrap();
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Read recent results".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::new(),
        output_contract: None,
    };
    let request_record =
        ProtectedRecord::new(id("request-data"), 1, serde_json::to_value(&request)?);
    let prompt = ProtectedRecord::new(
        id("prompt"),
        1,
        json!({"instructions":"Use available evidence"}),
    );
    let clock = Arc::new(SystemClock::new());
    let now = clock.now()?.utc_ms;
    let run = id("run");
    let snapshot = RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: run.clone(),
        request_digest: admission_digest(&request, &profile, Some(&input_ref)),
        request: request.clone(),
        scope: scope.clone(),
        limits: profile.profile().limits.clone(),
        profile,
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        usage: BudgetUsage::default(),
        timing: RunTiming::new(now, 30000)?,
        reservations: vec![],
        resume_receipts: vec![],
        recovery_receipts: vec![],
        hook_plan_ref: None,
        source_plan_ref: None,
        skill_plan_ref: None,
        context_plan_ref: None,
        context_revision_ref: None,
        context_decisions: vec![],
        verification_plan_ref: None,
        candidate_ref: None,
        verification_records: vec![],
        hook_applications: vec![],
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: Some(input_ref.clone()),
        wait: None,
        outcome: None,
        assembly_ref: None,
        routing_snapshot_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    let started = RunEvent {
        schema_version: RunEventSchemaVersion::V1,
        event_id: id("started"),
        scope: scope.clone(),
        run_id: run.clone(),
        session_id: request.session_id.clone(),
        seq: 1.try_into()?,
        timestamp_ms: now,
        payload: RunEventPayload::RunStarted {
            request_ref: request_record.reference().clone(),
            profile_digest: snapshot.profile.profile_digest().clone(),
        },
    };
    let store = Arc::new(MemoryStateStore::new());
    store
        .admit(
            &scope,
            AdmissionInput {
                snapshot,
                prompt_snapshot: prompt.reference().clone(),
                messages: vec![Message {
                    message_id: id("user-message"),
                    run_id: run.clone(),
                    sequence: 1.try_into()?,
                    role: MessageRole::User,
                    content: vec![ContentBlock::Content {
                        content: request.input[0].clone(),
                    }],
                    origin: MessageOrigin::User,
                    visibility: Visibility::UserAndModel,
                }],
                events: vec![started],
                records: vec![request_record, prompt, input_record.clone()],
                require_durable: false,
            },
        )
        .await?;
    let lease = store
        .acquire_lease(&scope, &run, &id("worker"), now, 30000)
        .await?;
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id("caller"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: Some(values),
        },
        Default::default(),
    );
    let budget = RunBudget::attach(
        store.clone(),
        clock.clone(),
        Arc::new(RandomIdSource),
        scope.clone(),
        run.clone(),
        lease.clone(),
        context.cancellation.clone(),
    )
    .await?;

    let binder = InputBinder::new(
        registry,
        None,
        Arc::new(PolicyGate::new(Arc::new(Owned), Duration::from_secs(1)).unwrap()),
        Arc::new(RandomIdSource),
    );
    plan(
        &store,
        &scope,
        &run,
        &lease,
        &clock,
        compiled,
        JsonObject::from([("query".into(), json!("latest"))]),
    )
    .await
    .unwrap();
    let result = binder
        .bind(compiled, &id("call"), &context, &budget)
        .await
        .unwrap();
    assert_eq!(
        result.input.original_model_inputs(),
        &JsonObject::from([("query".into(), json!("latest"))])
    );
    assert_eq!(result.input.execution_args(), &args());
    Ok(result.input.execution_args().clone())
}
```

## `crates/wickle-mcp/tests/support/mod.rs`

```rust
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
use wickle::*;
use wickle_mcp::*;
#[path = "../../../../tests/support/mcp_fixture.rs"]
mod fixture;
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
pub fn records(dir: &Directory) -> Vec<Value> {
    std::fs::read_to_string(dir.0.join("calls.jsonl"))
        .unwrap_or_default()
        .lines()
        .map(|v| parse_json(v).unwrap())
        .collect()
}
pub async fn connect(dir: &Directory, mode: &str, limits: McpLimits) -> McpClient {
    McpClient::connect(
        scope(),
        reference("account"),
        command(dir, mode),
        limits,
        &Default::default(),
        tokio::time::Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap()
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
pub async fn snapshot(client: &McpClient) -> McpSnapshot {
    client
        .discover(
            &scope(),
            &Default::default(),
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap()
}
pub async fn close(client: &McpClient) {
    client
        .close(tokio::time::Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
}
pub fn call_count(dir: &Directory) -> usize {
    records(dir)
        .iter()
        .filter(|v| v.get("call").is_some())
        .count()
}
pub fn latest_pid(dir: &Directory) -> u32 {
    records(dir)
        .iter()
        .rev()
        .find_map(|v| v.get("started").and_then(Value::as_u64))
        .unwrap()
        .try_into()
        .unwrap()
}
pub fn process_alive(pid: u32) -> bool {
    std::process::Command::new(python()).args(["-c","import os,sys\ntry: os.kill(int(sys.argv[1]),0)\nexcept ProcessLookupError: sys.exit(1)", &pid.to_string()]).status().unwrap().success()
}
```

## `tests/support/mcp_consumer.rs`

```rust
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
```

## `tests/support/mcp_fixture.rs`

```rust
// Local protocol fixture only. Never imported by a runtime crate.
pub const SERVER: &str = r#"
import json, os, sys, time
mode, log = sys.argv[1:3]
def record(value):
    with open(log, 'a', encoding='utf-8') as f: f.write(json.dumps(value)+'\n')
def send(value):
    print(json.dumps(value), flush=True)
record({'started':os.getpid(),'home':os.environ.get('HOME'),'allowed':os.environ.get('MCP_ALLOWED')})
listed=0
for line in sys.stdin:
    msg=json.loads(line)
    method=msg.get('method')
    if method is None:
        record({'client_response':msg})
        continue
    record({'method':method,'params':msg.get('params')})
    if 'id' not in msg: continue
    ident=msg['id']
    if method=='initialize':
        if mode=='hang_init': time.sleep(60)
        version='2025-06-18' if mode!='wrong_version' else '2026-07-28'
        send({'jsonrpc':'2.0','id':ident,'result':{'protocolVersion':version,'capabilities':{'tools':{'listChanged':True}},'serverInfo':{'name':'fixture','version':'1'}}})
        if mode=='callback': send({'jsonrpc':'2.0','id':'server-sample','method':'sampling/createMessage','params':{'messages':[{'role':'user','content':{'type':'text','text':'unrequested model call'}}],'maxTokens':8}})
    elif method=='tools/list':
        listed+=1
        version='2' if mode=='drift' and listed>=2 else '1'
        schema={'type':'object','properties':{'query':{'type':'string'},'limit':{'type':'integer','default':5},'workspace_id':{'type':'string','format':'uuid'}},'required':['query','workspace_id'],'additionalProperties':False}
        output={'type':'object','properties':{'answer':{'type':'integer'}},'required':['answer'],'additionalProperties':False}
        tools=[{'name':name,'description':'Access an authorized record','inputSchema':schema,'outputSchema':output,'annotations':{'readOnlyHint':True},'_meta':{'version':version}} for name in ['db.query','db.write']]
        if mode in ['text','image','text_metadata']:
            for tool in tools: tool.pop('outputSchema')
        if mode=='added' and listed>=2: tools.append(dict(tools[0],name='db.new'))
        if mode=='duplicate_list':
            first={'jsonrpc':'2.0','id':ident,'result':{'tools':tools}}
            second=json.loads(json.dumps(first))
            second['result']['tools'][0]['_meta']['version']='forged'
            sys.stdout.write(json.dumps(first)+'\n'+json.dumps(second)+'\n')
            sys.stdout.flush()
        elif mode=='pages':
            send({'jsonrpc':'2.0','id':ident,'result':{'tools':[],'nextCursor':str(listed)}})
        elif mode=='multipage':
            result={'tools':[tools[0] if not msg.get('params',{}).get('cursor') else tools[1]]}
            if not msg.get('params',{}).get('cursor'): result['nextCursor']='second'
            send({'jsonrpc':'2.0','id':ident,'result':result})
        elif mode=='cursor':
            send({'jsonrpc':'2.0','id':ident,'result':{'tools':[],'nextCursor':'same'}})
        else: send({'jsonrpc':'2.0','id':ident,'result':{'tools':tools}})
    elif method=='tools/call':
        args=msg['params']['arguments']
        record({'call':msg['params']['name'],'args':args})
        if mode in ['exit_write','hang_write']:
            with open(log+'.effect','a') as f: f.write('applied\n')
            if mode=='exit_write': os._exit(7)
            time.sleep(60)
        if mode=='hang': time.sleep(60)
        if mode=='flood':
            for _ in range(20): send({'jsonrpc':'2.0','method':'notifications/message','params':{'level':'info','data':'noise'}})
        if mode=='notify': send({'jsonrpc':'2.0','method':'notifications/tools/list_changed'})
        if mode=='duplicate':
            print('{"jsonrpc":"2.0","id":'+str(ident)+',"result":{"content":[],"isError":false,"isError":true}}',flush=True)
        elif mode=='oversized': send({'jsonrpc':'2.0','id':ident,'result':{'content':[{'type':'text','text':'x'*8000}],'structuredContent':{'answer':42}}})
        elif mode in ['text','text_metadata','image','missing_structured']:
            content={'type':'image','data':'eA==','mimeType':'image/png'} if mode=='image' else {'type':'text','text':'found','_meta':{'private':'not model content'}}
            send({'jsonrpc':'2.0','id':ident,'result':{'content':[content]}})
        elif mode=='tool_error': send({'jsonrpc':'2.0','id':ident,'result':{'content':[{'type':'text','text':'private remote error'}],'isError':True}})
        else:
            assert set(args)=={'query','limit','workspace_id'}
            assert args['workspace_id']=='b9d9051e-70f5-4ed8-b24d-ff8cabcce55a'
            send({'jsonrpc':'2.0','id':ident,'result':{'content':[{'type':'text','text':'found'}],'structuredContent':{'answer':42},'isError':False}})
    else: send({'jsonrpc':'2.0','id':ident,'error':{'code':-32601,'message':'unsupported'}})
"#;
```
