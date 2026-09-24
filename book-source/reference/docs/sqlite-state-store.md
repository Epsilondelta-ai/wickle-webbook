# Persist state with SQLite

`wickle-state-sqlite` implements `StateStore` for a local SQLite file. It preserves
runs, session transcripts, protected records, event history, and lease fencing
generations across process restarts. The core remains independent of SQLite and
filesystem configuration.

```rust
use wickle_state_sqlite::SqliteStateStore;

let store = SqliteStateStore::open("state.sqlite3")?;
```

The Host supplies the file path and a Tokio runtime for asynchronous operations.
No `.env` file or environment variable is used to configure the library. SQLite
operations run on blocking workers so database lock waits do not block the Host's
async runtime thread. `open_with_busy_timeout(path, duration)` sets an explicit
finite lock-wait limit.

## Transactions and restarts

Each write acquires an immediate transaction, reads the latest scope checkpoint,
applies the core's state validation, and saves the checkpoint. Results become
successful only after SQLite commits. Failed candidate validation rolls back the
transaction, including associated records and events. Reads use a consistent
transaction snapshot.

Each store and its clones can retain one validated checkpoint whose serialized
JSON is at most 32 MiB; the decoded graph also consumes memory. Every operation
still reads the current row and checks the database identity. Reuse requires the
exact same scope, JSON bytes and checksum. Changed data is fully validated, and
new cached state is published only after a successful transaction commit. This
avoids repeating whole-checkpoint validation for unchanged data while preserving
cross-connection updates and rollback isolation.

The adapter uses WAL and `synchronous=FULL`. Its bundled SQLite version includes
the WAL-reset fix. Use a local file on the same computer as the processes sharing
it; SQLite WAL relies on shared memory between those processes.
[SQLite WAL documentation](https://www.sqlite.org/wal.html),
[synchronization settings](https://www.sqlite.org/pragma.html#pragma_synchronous).

On restart, leases retain their stored owner, expiration, and fencing generation.
An expired lease can be replaced with a higher generation; an old worker cannot
commit with its earlier lease. Releasing a lease preserves the last generation.
Opening a file does not silently expire leases or replace run identity.

Unknown database or checkpoint versions and inconsistent scope, checksum,
references, or state relationships fail explicitly. Restoration checks historical
events against their stored records and identities. It does not require a past
waiting or reserved state to equal the latest completed state.

If an operation loses its acknowledgement, read the stored revision before
deciding what to retry. Dropping an async caller does not prove that an already
running blocking database operation rolled back. Run request deduplication, CAS,
and fencing remain necessary across this boundary.

## Reference implementation scope

This adapter stores one complete checkpoint per scope to reuse the core's state
contracts. Reads and writes cost more as that scope's retained history grows, and
SQLite serializes writers. It is a reference implementation for local persistence;
applications with larger storage or concurrency needs can supply another
`StateStore` implementation.

All committed events and protected records are retained. No pruning is performed.
The supported execution-history checkpoint upgrade is described below; arbitrary
future database migrations are not implied. The Host controls database file access and should treat
checkpoint data as protected application state. A checksum detects inconsistent
data; it does not replace access control.

`MemoryStateStore` can export and restore a versioned `StateStoreCheckpoint` for
storage implementations. These values contain protected data and offer no mutable
access. Restoring one into memory does not give that memory store persistence or
cross-process lease guarantees.

The [independent consumer](../tests/support/sqlite_consumer.rs) admits and finishes
a run, then starts another process that reopens the file and reads its outcome and
events. Run it with the other extracted-package consumers:

```sh
python3 scripts/check-package.py --allow-dirty
```

Process termination and restart are tested directly. Hardware power-loss behavior
depends on SQLite and the underlying storage and is not simulated by those tests.

## Execution-history checkpoint upgrade

New admissions write `wickle.state-store.v2` scope checkpoints containing original
execution actors, segments and control-command history. The SQL table layout is
unchanged. Version-one checkpoints remain readable without rewriting them, and
existing terminal outcomes/events remain intact when a later admission upgrades
the scope in the same transaction. The v2 checkpoint explicitly lists inherited
legacy Run IDs; missing history for an unmarked Run is corruption, including for
terminal Runs. Accepted command evidence must match the saved recovery/resume
receipt payload and segment revision.

A legacy nonterminal Run has no trustworthy execution-history metadata. New
admission into such a scope is rejected with `execution.legacy_drain_required`;
new execution-transaction access returns `execution.legacy_checkpoint`. Finish or
otherwise resolve these Runs using their original runtime before upgrading. The
store does not invent actors, segment identities or command receipts for them.

Back up the database before allowing the first new write. Do not run an older
binary against a scope written in v2. Rollback means restoring a compatible backup
and reconciling external effects, not simply switching the executable. Reads and
failed transactions do not migrate a scope. See [runtime compatibility](compatibility.md).
