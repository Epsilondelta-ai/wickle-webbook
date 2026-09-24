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
