use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
    task::JoinHandle,
};
use wickle::parse_json;
pub fn wire(events: &[Value]) -> Vec<u8> {
    let mut bytes = vec![];
    for (index, value) in events.iter().enumerate() {
        let mut value = value.clone();
        value["sequence_number"] = json!(index);
        bytes.extend_from_slice(
            format!(
                "event: {}\r\ndata: {}\r\n\r\n",
                value["type"].as_str().unwrap(),
                value
            )
            .as_bytes(),
        );
    }
    bytes
}
pub struct Reply {
    pub status: u16,
    pub content_type: &'static str,
    pub body: Vec<u8>,
    pub headers: Vec<(&'static str, String)>,
    pub stall: bool,
    pub chunk: usize,
}
impl Reply {
    pub fn sse(events: &[Value]) -> Self {
        Self {
            status: 200,
            content_type: "text/event-stream",
            body: wire(events),
            headers: vec![("x-request-id", "req-fixture".into())],
            stall: false,
            chunk: 7,
        }
    }
    pub fn json(status: u16, value: Value) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: serde_json::to_vec(&value).unwrap(),
            headers: vec![],
            stall: false,
            chunk: 4096,
        }
    }
}
#[derive(Debug)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub headers: String,
    pub body: Value,
    /// Exact body bytes decoded as UTF-8 for lossless numeric/signature assertions.
    #[allow(dead_code)]
    pub raw_body: String,
}
pub struct Server {
    pub base: String,
    pub requests: Arc<Mutex<Vec<Request>>>,
    pub entered: Arc<Notify>,
    pub closed: Arc<Notify>,
    task: JoinHandle<()>,
}
impl Server {
    pub async fn new(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/v1/", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(vec![]));
        let records = requests.clone();
        let entered = Arc::new(Notify::new());
        let signal = entered.clone();
        let closed = Arc::new(Notify::new());
        let ended = closed.clone();
        let task = tokio::spawn(async move {
            let mut replies = std::collections::VecDeque::from(replies);
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let reply = replies.pop_front().unwrap_or_else(|| {
                    Reply::json(500, json!({"error":{"code":"unexpected_request"}}))
                });
                let mut bytes = vec![];
                let (header_end, length) = loop {
                    let mut block = [0; 4096];
                    let count = socket.read(&mut block).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&block[..count]);
                    assert!(bytes.len() <= 131_072);
                    if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let headers = std::str::from_utf8(&bytes[..index])
                            .unwrap()
                            .to_ascii_lowercase();
                        let length = headers
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length:"))
                            .map(|value| value.trim().parse::<usize>().unwrap())
                            .unwrap_or(0);
                        break (index + 4, length);
                    }
                };
                while bytes.len() < header_end + length {
                    let mut block = [0; 4096];
                    let count = socket.read(&mut block).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&block[..count]);
                }
                let headers = std::str::from_utf8(&bytes[..header_end])
                    .unwrap()
                    .to_owned();
                let mut first = headers.lines().next().unwrap().split_whitespace();
                records.lock().unwrap().push(Request {
                    method: first.next().unwrap().into(),
                    path: first.next().unwrap().into(),
                    headers: headers.clone(),
                    raw_body: std::str::from_utf8(&bytes[header_end..header_end + length])
                        .unwrap()
                        .into(),
                    body: if length == 0 {
                        Value::Null
                    } else {
                        parse_json(
                            std::str::from_utf8(&bytes[header_end..header_end + length]).unwrap(),
                        )
                        .unwrap()
                    },
                });
                signal.notify_one();
                let extra: String = reply
                    .headers
                    .iter()
                    .map(|(key, value)| format!("{key}: {value}\r\n"))
                    .collect();
                let header = format!(
                    "HTTP/1.1 {} Response\r\nContent-Type: {}\r\nContent-Length: {}\r\n{}Connection: close\r\n\r\n",
                    reply.status,
                    reply.content_type,
                    reply.body.len() + if reply.stall { 100 } else { 0 },
                    extra
                );
                if socket.write_all(header.as_bytes()).await.is_err() {
                    ended.notify_one();
                    continue;
                }
                for chunk in reply.body.chunks(reply.chunk) {
                    if socket.write_all(chunk).await.is_err() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                if reply.stall {
                    let mut byte = [0];
                    let _ = socket.read(&mut byte).await;
                }
                let _ = socket.shutdown().await;
                ended.notify_one();
            }
        });
        Self {
            base,
            requests,
            entered,
            closed,
            task,
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
