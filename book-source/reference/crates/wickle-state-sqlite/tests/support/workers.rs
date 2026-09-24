use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use serde_json::{Value, json};
use wickle::*;
use wickle_state_sqlite::SqliteStateStore;

use crate::{
    core::{id, prepared, scope},
    support::{Database, durable_admission, populate_protected_run},
};

pub struct Worker {
    child: Child,
    pub result: PathBuf,
    pub ready: PathBuf,
}
impl Worker {
    pub fn spawn(database: &Database, actor: &str, mode: &str, details: Value) -> Self {
        let result = database.file(&format!("{actor}.result"));
        let ready = database.file(&format!("{actor}.ready"));
        let config = json!({
            "database":database.path(), "result":result, "ready":ready,
            "gate":database.file("gate"), "resume":database.file("resume"),
            "mode":mode, "actor":actor, "details":details,
        });
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "process_worker", "--nocapture"])
            .env("WICKLE_SQLITE_TEST_WORKER", config.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        Self {
            child,
            result,
            ready,
        }
    }
    pub fn finish(&mut self) -> Value {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success(), "SQLite subprocess failed");
                return read(&self.result);
            }
            assert!(
                Instant::now() < deadline,
                "SQLite subprocess did not finish"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    pub fn terminate(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn wait(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "SQLite subprocess barrier timed out"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}
pub fn signal(path: &Path) {
    std::fs::write(path, b"ready").unwrap();
}
pub fn read(path: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}
fn write(path: &Path, value: Value) {
    let temporary = path.with_extension("writing");
    std::fs::write(&temporary, serde_json::to_vec(&value).unwrap()).unwrap();
    std::fs::rename(temporary, path).unwrap();
}

pub fn run() {
    let encoded = std::env::var("WICKLE_SQLITE_TEST_WORKER")
        .expect("This ignored fixture requires an explicit parent test command");
    let config: Value = serde_json::from_str(&encoded).unwrap();
    let database = Path::new(config["database"].as_str().unwrap());
    let result = Path::new(config["result"].as_str().unwrap());
    let ready = Path::new(config["ready"].as_str().unwrap());
    let actor = config["actor"].as_str().unwrap();
    let mode = config["mode"].as_str().unwrap();
    if mode == "uncommitted-upgrade" {
        let connection = rusqlite::Connection::open(database).unwrap();
        let image = &config["details"]["image"];
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        connection
            .execute(
                "UPDATE wickle_scope_checkpoints SET checkpoint_json=?1,checksum=?2",
                rusqlite::params![image.to_string(), canonical_digest(image).as_str()],
            )
            .unwrap();
        signal(ready);
        loop {
            std::thread::park();
        }
    }
    if mode == "uncommitted" {
        let connection = rusqlite::Connection::open(database).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE; UPDATE wickle_scope_checkpoints SET checkpoint_json='uncommitted invalid image', checksum='uncommitted';").unwrap();
        signal(ready);
        loop {
            std::thread::park();
        }
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let store = SqliteStateStore::open(database).unwrap();
        signal(ready);
        if matches!(mode, "admit" | "lease_hold") {
            wait(Path::new(config["gate"].as_str().unwrap()));
        }
        match mode {
            "admit" => {
                let request = config["details"]["request"].as_str().unwrap();
                let receipt = store.admit(&scope(), durable_admission(actor, request, "session").await).await;
                write(result, match receipt {
                    Ok(receipt) => json!({"created":receipt.created,"run":receipt.state.snapshot.run_id}),
                    Err(error) => json!({"error":format!("{:?}",error.code)}),
                });
            }
            "lease_hold" => {
                match store.acquire_lease(&scope(), &id("run"), &id(actor), 100, 10_000).await {
                    Ok(lease) => {
                        write(result, json!({"fence":lease.fencing_token,"expires_at_ms":lease.expires_at_ms}));
                        wait(Path::new(config["resume"].as_str().unwrap()));
                        let latest = store.load(&scope(), &id("run")).await.unwrap();
                        let error = store.commit(&scope(), &id("run"), prepared(&latest.snapshot, lease.clone(), lease.expires_at_ms + 10)).await.unwrap_err();
                        write(result, json!({"fence":lease.fencing_token,"stale_error":format!("{:?}",error.code),"observed_revision":latest.snapshot.revision}));
                    }
                    Err(error) => write(result, json!({"error":format!("{:?}",error.code)})),
                }
            }
            "takeover" => {
                let now = config["details"]["now"].as_i64().unwrap();
                let owner = config["details"]["owner"].as_str().unwrap();
                let lease = store.acquire_lease(&scope(), &id("run"), &id(owner), now, 10_000).await.unwrap();
                let saved = store.load(&scope(), &id("run")).await.unwrap();
                let committed = store.commit(&scope(), &id("run"), prepared(&saved.snapshot, lease.clone(), now + 1)).await.unwrap();
                write(result, json!({"fence":lease.fencing_token,"revision":committed.snapshot.revision}));
            }
            "commit_exit" => {
                let retained_connection = rusqlite::Connection::open(database).unwrap();
                let _: i64 = retained_connection.query_row(
                    "SELECT count(*) FROM wickle_scope_checkpoints", [], |row| row.get(0)
                ).unwrap();
                let retained_store = std::sync::Arc::new(store);
                let saved = populate_protected_run(retained_store.clone()).await;
                write(result, saved);
                // Keep an initialized WAL connection alive so operation-level closes
                // cannot perform last-connection cleanup before this abrupt exit.
                std::process::exit(0);
            }
            _ => panic!("Unknown SQLite subprocess fixture mode"),
        }
    });
}
