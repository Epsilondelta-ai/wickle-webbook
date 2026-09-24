// Test Host delivery journal. Not a production queue or a core runtime component.
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::json;
use std::{error::Error, path::Path};
use wickle::{
    EventPage, Id, RunEvent, RunEventPayload, Scope, StateStoreCapabilities, canonical_digest,
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
fn failure(message: &str) -> Box<dyn Error> {
    std::io::Error::other(message).into()
}

#[derive(Clone)]
pub struct Subscription {
    pub scope: Scope,
    pub id: Id,
    pub revision: Id,
    pub target_revision: Id,
}
impl Subscription {
    fn key(&self, run: &Id) -> String {
        canonical_digest(&json!({"scope":self.scope,"run":run,"subscription":self.id,"revision":self.revision,"target":self.target_revision})).as_str().into()
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Receipt {
    Applied,
    Accepted(String),
    NotAppliedRetryable,
    PermanentFailure,
    Unknown,
}
#[derive(Debug)]
pub struct Claim {
    pub delivery_id: String,
    pub event: RunEvent,
    pub generation: i64,
    pub operation: Option<String>,
    key: String,
}
/// SQLite belongs to this example Host, independently of the source StateStore.
pub struct Journal {
    connection: Connection,
    subscription: Subscription,
    run: Id,
    key: String,
}
impl Journal {
    pub fn open(
        path: &Path,
        subscription: Subscription,
        run: Id,
        source: StateStoreCapabilities,
    ) -> Result<Self> {
        if !source.durable || !source.event_replay || path == Path::new(":memory:") {
            return Err(failure(
                "durable replay and a file-backed Host journal are required",
            ));
        }
        let connection = Connection::open(path)?;
        connection.busy_timeout(std::time::Duration::from_secs(2))?;
        connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS subscriptions (identity TEXT PRIMARY KEY, target TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS cursors (source_key TEXT PRIMARY KEY, seq TEXT NOT NULL, source_gap INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS deliveries (source_key TEXT NOT NULL, event_id TEXT NOT NULL, delivery_id TEXT UNIQUE NOT NULL, event_json TEXT NOT NULL, state TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0, operation TEXT, PRIMARY KEY(source_key,event_id));")?;
        let identity = canonical_digest(
            &json!({"scope":subscription.scope,"subscription":subscription.id,"revision":subscription.revision}),
        );
        connection.execute(
            "INSERT OR IGNORE INTO subscriptions VALUES (?1,?2)",
            params![identity.as_str(), subscription.target_revision.as_str()],
        )?;
        let target: String = connection.query_row(
            "SELECT target FROM subscriptions WHERE identity=?1",
            [identity.as_str()],
            |r| r.get(0),
        )?;
        if target != subscription.target_revision.as_str() {
            return Err(failure("immutable subscription changed"));
        }
        let key = subscription.key(&run);
        connection.execute("INSERT OR IGNORE INTO cursors VALUES (?1,'0',0)", [&key])?;
        Ok(Self {
            connection,
            subscription,
            run,
            key,
        })
    }
    pub fn cursor(&self) -> Result<u64> {
        let value: String = self.connection.query_row(
            "SELECT seq FROM cursors WHERE source_key=?1",
            [&self.key],
            |r| r.get(0),
        )?;
        Ok(value.parse()?)
    }
    pub fn has_source_gap(&self) -> Result<bool> {
        Ok(self.connection.query_row(
            "SELECT source_gap FROM cursors WHERE source_key=?1",
            [&self.key],
            |r| r.get(0),
        )?)
    }
    /// Atomically discover deliveries (including filter decisions) and advance the cursor.
    pub fn ingest(&mut self, page: &EventPage) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (saved, gap): (String, bool) = transaction.query_row(
            "SELECT seq,source_gap FROM cursors WHERE source_key=?1",
            [&self.key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let mut cursor: u64 = saved.parse()?;
        if gap {
            return Err(failure("source_gap requires explicit Host recovery"));
        }
        transaction.execute_batch("SAVEPOINT ingest_page")?;
        let mut missing = page.last_available_seq < cursor
            || page
                .first_available_seq
                .is_some_and(|first| first.get().saturating_sub(1) > cursor);
        let mut duplicates_only = !page.events.is_empty();
        for event in &page.events {
            if event.scope != self.subscription.scope || event.run_id != self.run {
                return Err(failure("foreign event"));
            }
            let encoded = serde_json::to_string(event)?;
            let existing: Option<String> = transaction
                .query_row(
                    "SELECT event_json FROM deliveries WHERE source_key=?1 AND event_id=?2",
                    params![self.key, event.event_id.as_str()],
                    |r| r.get(0),
                )
                .optional()?;
            if event.seq.get() <= cursor {
                if existing.as_deref() != Some(&encoded) {
                    return Err(failure("conflicting duplicate event"));
                }
                continue;
            }
            duplicates_only = false;
            if cursor.checked_add(1) != Some(event.seq.get()) {
                missing = true;
                break;
            }
            if existing.is_some() {
                return Err(failure("event identity reused"));
            }
            let delivery = canonical_digest(&json!({"source":self.key,"event":event.event_id}));
            let state = if matches!(event.payload, RunEventPayload::RunFinished { .. }) {
                "pending"
            } else {
                "filtered"
            };
            transaction.execute("INSERT INTO deliveries (source_key,event_id,delivery_id,event_json,state) VALUES (?1,?2,?3,?4,?5)",params![self.key,event.event_id.as_str(),delivery.as_str(),encoded,state])?;
            cursor = event.seq.get();
        }
        if duplicates_only {
            transaction.commit()?;
            return Ok(());
        }
        let advertised = page.events.last().map_or(cursor, |e| e.seq.get());
        missing |= page.next_after_seq != advertised
            || cursor > page.last_available_seq
            || (!page.has_more && cursor < page.last_available_seq);
        if missing {
            transaction.execute_batch("ROLLBACK TO ingest_page; RELEASE ingest_page;")?;
            transaction.execute(
                "UPDATE cursors SET source_gap=1 WHERE source_key=?1",
                [&self.key],
            )?;
            transaction.commit()?;
            return Err(failure("source_gap"));
        }
        transaction.execute(
            "UPDATE cursors SET seq=?2 WHERE source_key=?1",
            params![self.key, cursor.to_string()],
        )?;
        transaction.commit()?;
        Ok(())
    }
    pub fn delivery_for(&self, event: &Id) -> Result<String> {
        Ok(self.connection.query_row(
            "SELECT delivery_id FROM deliveries WHERE source_key=?1 AND event_id=?2",
            params![self.key, event.as_str()],
            |r| r.get(0),
        )?)
    }
    /// At most two known-safe dispatch attempts in this reference Host.
    pub fn claim(&mut self, delivery: &str) -> Result<Option<Claim>> {
        self.take(delivery, false, false)
    }
    /// Recover an abandoned call by observation only; never redispatch an uncertain write.
    pub fn recover(&mut self, delivery: &str, can_reconcile: bool) -> Result<Option<Claim>> {
        self.take(delivery, true, can_reconcile)
    }
    fn take(
        &mut self,
        delivery: &str,
        recovering: bool,
        can_reconcile: bool,
    ) -> Result<Option<Claim>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (state,generation,attempts,event,operation): (String,i64,i64,String,Option<String>)=transaction.query_row("SELECT state,generation,attempts,event_json,operation FROM deliveries WHERE source_key=?1 AND delivery_id=?2",params![self.key,delivery], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
        let permitted = if recovering {
            matches!(state.as_str(), "in_flight" | "reconciling" | "accepted")
        } else {
            state == "pending"
        };
        if !permitted {
            return Ok(None);
        }
        if recovering && state == "accepted" && !can_reconcile {
            return Ok(None);
        }
        if (recovering && !can_reconcile) || (!recovering && attempts >= 2) {
            transaction.execute("UPDATE deliveries SET state=?3,generation=generation+1 WHERE source_key=?1 AND delivery_id=?2",params![self.key,delivery,if recovering {"unknown"} else {"permanent_failure"}])?;
            transaction.commit()?;
            return Ok(None);
        }
        transaction.execute("UPDATE deliveries SET state=?3,generation=generation+1,attempts=attempts+?4 WHERE source_key=?1 AND delivery_id=?2",params![self.key,delivery,if recovering {"reconciling"} else {"in_flight"},i64::from(!recovering)])?;
        transaction.commit()?;
        Ok(Some(Claim {
            delivery_id: delivery.into(),
            event: RunEvent::from_json(&event)?,
            generation: generation + 1,
            operation,
            key: self.key.clone(),
        }))
    }
    pub fn finish(&mut self, claim: &Claim, receipt: Receipt) -> Result<()> {
        if claim.key != self.key {
            return Err(failure("subscription revision mismatch"));
        }
        let (state, operation) = match receipt {
            Receipt::Applied => ("applied", None),
            Receipt::Accepted(operation) => ("accepted", Some(operation)),
            Receipt::NotAppliedRetryable => ("pending", None),
            Receipt::PermanentFailure => ("permanent_failure", None),
            Receipt::Unknown => ("unknown", None),
        };
        let changed=self.connection.execute("UPDATE deliveries SET state=?4,operation=?5 WHERE source_key=?1 AND delivery_id=?2 AND generation=?3 AND state IN ('in_flight','reconciling')",params![self.key,claim.delivery_id,claim.generation,state,operation])?;
        if changed != 1 {
            return Err(failure("stale delivery claim"));
        }
        Ok(())
    }
    pub fn receipt(&self, delivery: &str) -> Result<Option<Receipt>> {
        let (state, operation): (String, Option<String>) = self.connection.query_row(
            "SELECT state,operation FROM deliveries WHERE source_key=?1 AND delivery_id=?2",
            params![self.key, delivery],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        Ok(match state.as_str() {
            "applied" => Some(Receipt::Applied),
            "accepted" => Some(Receipt::Accepted(
                operation.ok_or_else(|| failure("missing operation"))?,
            )),
            "unknown" => Some(Receipt::Unknown),
            "permanent_failure" => Some(Receipt::PermanentFailure),
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wickle::*;
    fn id(value: &str) -> Id {
        Id::new(value).unwrap()
    }
    fn subscription() -> Subscription {
        Subscription {
            scope: Scope {
                tenant_id: id("tenant"),
                workspace_id: id("workspace"),
                user_id: None,
            },
            id: id("memory"),
            revision: id("1"),
            target_revision: id("backend-a-1"),
        }
    }
    fn capabilities() -> StateStoreCapabilities {
        StateStoreCapabilities {
            durable: true,
            event_replay: true,
            cross_process_leases: true,
        }
    }
    struct Directory(std::path::PathBuf);
    impl Directory {
        fn new() -> Self {
            let p = std::env::temp_dir().join(format!(
                "wickle-delivery-{}",
                RandomIdSource.next_id().unwrap()
            ));
            std::fs::create_dir(&p).unwrap();
            Self(p)
        }
        fn path(&self) -> std::path::PathBuf {
            self.0.join("delivery.sqlite3")
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn page() -> EventPage {
        let event = |seq, name| RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: id(name),
            scope: subscription().scope,
            run_id: id("run"),
            session_id: id("session"),
            seq: std::num::NonZeroU64::new(seq).unwrap(),
            timestamp_ms: 1,
            payload: RunEventPayload::RunFinished {
                outcome_ref: ProtectedRecord::new(id(name), 1, json!({"fixture":true}))
                    .reference()
                    .clone(),
            },
        };
        EventPage {
            events: vec![event(1, "first"), event(2, "second")],
            next_after_seq: 2,
            has_more: false,
            first_available_seq: std::num::NonZeroU64::new(1),
            last_available_seq: 2,
        }
    }
    #[test]
    fn discovery_transaction_survives_restart_and_rolls_back_cursor_failure() {
        let dir = Directory::new();
        let mut journal =
            Journal::open(&dir.path(), subscription(), id("run"), capabilities()).unwrap();
        journal.connection.execute_batch("CREATE TRIGGER reject_cursor BEFORE UPDATE ON cursors BEGIN SELECT RAISE(ABORT,'injected interruption'); END;").unwrap();
        assert!(journal.ingest(&page()).is_err());
        assert_eq!(journal.cursor().unwrap(), 0);
        assert!(journal.delivery_for(&id("first")).is_err());
        journal
            .connection
            .execute_batch("DROP TRIGGER reject_cursor;")
            .unwrap();
        journal.ingest(&page()).unwrap();
        let delivery = journal.delivery_for(&id("first")).unwrap();
        drop(journal);
        let mut resumed =
            Journal::open(&dir.path(), subscription(), id("run"), capabilities()).unwrap();
        assert_eq!(resumed.cursor().unwrap(), 2);
        resumed.ingest(&page()).unwrap();
        assert_eq!(resumed.delivery_for(&id("first")).unwrap(), delivery);
        let count: i64 = resumed
            .connection
            .query_row("SELECT COUNT(*) FROM deliveries", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }
    #[test]
    fn replaying_older_pages_after_progress_does_not_create_a_gap() {
        let dir = Directory::new();
        let mut journal =
            Journal::open(&dir.path(), subscription(), id("run"), capabilities()).unwrap();
        let first = page();
        journal.ingest(&first).unwrap();
        let mut later = page();
        for (index, event) in later.events.iter_mut().enumerate() {
            event.seq = std::num::NonZeroU64::new(index as u64 + 3).unwrap();
            event.event_id = id(if index == 0 { "third" } else { "fourth" });
        }
        later.next_after_seq = 4;
        later.last_available_seq = 4;
        journal.ingest(&later).unwrap();
        journal.ingest(&first).unwrap();
        assert_eq!(journal.cursor().unwrap(), 4);
        assert!(!journal.has_source_gap().unwrap());
    }
    #[test]
    fn conflicting_duplicates_and_reused_ids_do_not_change_the_cursor() {
        let dir = Directory::new();
        let mut journal =
            Journal::open(&dir.path(), subscription(), id("run"), capabilities()).unwrap();
        journal.ingest(&page()).unwrap();
        let mut changed = page();
        changed.events[0].timestamp_ms = 999;
        assert!(journal.ingest(&changed).is_err());
        assert_eq!(journal.cursor().unwrap(), 2);
        let mut reused = page();
        reused.events.truncate(1);
        reused.events[0].seq = std::num::NonZeroU64::new(3).unwrap();
        reused.next_after_seq = 3;
        reused.last_available_seq = 3;
        assert!(journal.ingest(&reused).is_err());
        assert_eq!(journal.cursor().unwrap(), 2);
    }
    #[test]
    fn concurrent_workers_dispatch_one_claim() {
        let dir = Directory::new();
        let mut journal =
            Journal::open(&dir.path(), subscription(), id("run"), capabilities()).unwrap();
        journal.ingest(&page()).unwrap();
        let key = journal.delivery_for(&id("first")).unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let writes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let workers: Vec<_> = (0..2)
            .map(|_| {
                let mut worker =
                    Journal::open(&dir.path(), subscription(), id("run"), capabilities()).unwrap();
                let barrier = barrier.clone();
                let writes = writes.clone();
                let key = key.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    if let Some(claim) = worker.claim(&key).unwrap() {
                        writes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        worker.finish(&claim, Receipt::Applied).unwrap();
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(writes.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(journal.receipt(&key).unwrap(), Some(Receipt::Applied));
    }
    #[test]
    fn applied_write_without_receipt_is_reconciled_without_redelivery() {
        let dir = Directory::new();
        let mut journal =
            Journal::open(&dir.path(), subscription(), id("run"), capabilities()).unwrap();
        journal.ingest(&page()).unwrap();
        let delivery = journal.delivery_for(&id("first")).unwrap();
        let old = journal.claim(&delivery).unwrap().unwrap();
        let mut external = std::collections::BTreeMap::new();
        external.insert(old.delivery_id.clone(), 42); // remote write succeeded; no receipt was saved
        drop(journal);
        let mut resumed =
            Journal::open(&dir.path(), subscription(), id("run"), capabilities()).unwrap();
        assert!(resumed.claim(&delivery).unwrap().is_none());
        let current = resumed.recover(&delivery, true).unwrap().unwrap();
        assert_eq!(current.event.event_id, id("first"));
        assert_eq!(external.get(&current.delivery_id), Some(&42));
        assert!(resumed.finish(&old, Receipt::Applied).is_err());
        resumed.finish(&current, Receipt::Applied).unwrap();
        assert_eq!(external.len(), 1);
        assert_eq!(resumed.receipt(&delivery).unwrap(), Some(Receipt::Applied));
        assert!(resumed.claim(&delivery).unwrap().is_none());
    }
    #[test]
    fn accepted_is_not_applied_and_unobservable_writes_stay_unknown() {
        let dir = Directory::new();
        let mut journal =
            Journal::open(&dir.path(), subscription(), id("run"), capabilities()).unwrap();
        journal.ingest(&page()).unwrap();
        let accepted = journal.delivery_for(&id("first")).unwrap();
        let claim = journal.claim(&accepted).unwrap().unwrap();
        journal
            .finish(&claim, Receipt::Accepted("operation-1".into()))
            .unwrap();
        assert_eq!(
            journal.receipt(&accepted).unwrap(),
            Some(Receipt::Accepted("operation-1".into()))
        );
        assert!(journal.claim(&accepted).unwrap().is_none());
        assert!(journal.recover(&accepted, false).unwrap().is_none());
        assert_eq!(
            journal.receipt(&accepted).unwrap(),
            Some(Receipt::Accepted("operation-1".into()))
        );
        let observing = journal.recover(&accepted, true).unwrap().unwrap();
        assert_eq!(observing.operation.as_deref(), Some("operation-1"));
        journal.finish(&observing, Receipt::Applied).unwrap();
        let unknown = journal.delivery_for(&id("second")).unwrap();
        let stale = journal.claim(&unknown).unwrap().unwrap();
        assert!(journal.recover(&unknown, false).unwrap().is_none());
        assert_eq!(journal.receipt(&unknown).unwrap(), Some(Receipt::Unknown));
        assert!(journal.finish(&stale, Receipt::Applied).is_err());
        assert!(journal.claim(&unknown).unwrap().is_none());
    }
    #[test]
    fn source_gap_and_foreign_events_never_advance_discovery() {
        let dir = Directory::new();
        let mut journal =
            Journal::open(&dir.path(), subscription(), id("run"), capabilities()).unwrap();
        let mut foreign = page();
        foreign.events[0].scope.workspace_id = id("foreign");
        assert!(journal.ingest(&foreign).is_err());
        assert_eq!(journal.cursor().unwrap(), 0);
        let mut missing = page();
        missing.events.remove(0);
        missing.first_available_seq = std::num::NonZeroU64::new(2);
        assert!(journal.ingest(&missing).is_err());
        assert!(journal.has_source_gap().unwrap());
        assert_eq!(journal.cursor().unwrap(), 0);
        assert!(journal.ingest(&page()).is_err()); // explicit recovery is required
    }
    #[test]
    fn a_gap_rolls_back_partial_rows_and_atomically_keeps_the_marker() {
        let dir = Directory::new();
        let mut journal =
            Journal::open(&dir.path(), subscription(), id("run"), capabilities()).unwrap();
        let mut partial = page();
        partial.events[1].seq = std::num::NonZeroU64::new(3).unwrap();
        partial.next_after_seq = 3;
        partial.last_available_seq = 3;
        assert!(journal.ingest(&partial).is_err());
        assert!(journal.delivery_for(&id("first")).is_err());
        assert_eq!(journal.cursor().unwrap(), 0);
        drop(journal);
        let reopened =
            Journal::open(&dir.path(), subscription(), id("run"), capabilities()).unwrap();
        assert!(reopened.has_source_gap().unwrap());
        assert_eq!(reopened.cursor().unwrap(), 0);
    }
    #[test]
    fn subscriptions_are_immutable_and_claims_cannot_change_targets() {
        let dir = Directory::new();
        let mut journal =
            Journal::open(&dir.path(), subscription(), id("run"), capabilities()).unwrap();
        journal.ingest(&page()).unwrap();
        let delivery = journal.delivery_for(&id("first")).unwrap();
        let old = journal.claim(&delivery).unwrap().unwrap();
        let mut changed = subscription();
        changed.target_revision = id("backend-b-1");
        assert!(Journal::open(&dir.path(), changed.clone(), id("run"), capabilities()).is_err());
        changed.revision = id("2");
        let mut new = Journal::open(&dir.path(), changed, id("run"), capabilities()).unwrap();
        assert!(new.finish(&old, Receipt::Applied).is_err());
        assert!(new.delivery_for(&id("first")).is_err());
        let mut volatile = capabilities();
        volatile.durable = false;
        assert!(Journal::open(&dir.path(), subscription(), id("run"), volatile).is_err());
    }
    #[test]
    fn known_unapplied_retries_are_bounded_and_filter_decisions_are_retained() {
        let dir = Directory::new();
        let mut journal =
            Journal::open(&dir.path(), subscription(), id("run"), capabilities()).unwrap();
        let mut input = page();
        input.events[1].payload = RunEventPayload::RunRecovered {
            recovery_receipt_ref: ProtectedRecord::new(id("recovery"), 1, json!({}))
                .reference()
                .clone(),
        };
        journal.ingest(&input).unwrap();
        let delivery = journal.delivery_for(&id("first")).unwrap();
        for _ in 0..2 {
            let claim = journal.claim(&delivery).unwrap().unwrap();
            journal
                .finish(&claim, Receipt::NotAppliedRetryable)
                .unwrap();
        }
        assert!(journal.claim(&delivery).unwrap().is_none());
        assert_eq!(
            journal.receipt(&delivery).unwrap(),
            Some(Receipt::PermanentFailure)
        );
        let filtered = journal.delivery_for(&id("second")).unwrap();
        assert!(journal.claim(&filtered).unwrap().is_none());
        assert_eq!(journal.cursor().unwrap(), 2);
    }
}
