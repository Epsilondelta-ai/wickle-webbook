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
