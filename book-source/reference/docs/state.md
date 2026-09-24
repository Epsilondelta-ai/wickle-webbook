# StateStore and the memory reference store

For upgrades and persisted executions, see [runtime compatibility](compatibility.md).

`StateStore` is the trusted storage boundary for admission, checkpoints, leases,
session transcripts, protected records, and event replay. `MemoryStateStore`
implements that contract in-process with a mutex-protected transaction boundary.
It performs no external calls or awaits while holding its lock.

All keys use the exact tenant/workspace/optional-user scope. Raw store access is
for trusted runtime code; the facade must also apply current `PolicyGate` checks
before publishing data or performing a protected operation.

## Admission

`admit(scope, AdmissionInput)` accepts a running/admission checkpoint at revision
zero, the pinned prompt record, initial messages, and one `run.started` event.
The event must reference the actual accepted request. Referenced protected data
must already exist in the same scope or be supplied in the transaction.

The deduplication key is `(scope, session_id, request_id)`. An identical logical
digest returns the original run without adding messages/events or replacing its
resolved profile and record references, even after it finishes. A changed digest
returns `RequestConflict`. Other requests cannot occupy a session while its active
run is running or waiting. Profile and prompt identity remain pinned across the
session; a new run may resolve its allowed model binding revision separately.

`StoredRun.messages` is an owned copy of the complete session transcript.
Message sequences continue across runs. Changing a returned object does not
mutate the stored data.

## Leases and atomic commits

Acquire, renew, and release use explicit trusted UTC `now_ms` and a positive finite
TTL. An acquired lease has a monotonic fencing generation. `now == expires_at_ms`
is already expired. Renewal preserves the generation; an older copy from that
same generation is checked against the store's current expiry, not an untrusted
expiry supplied by the caller. An expired or replaced generation cannot revive
itself by renewing.

`commit` requires the current lease and the expected checkpoint revision. The
candidate revision must be exactly one greater. It validates the complete
candidate, messages, records, and events before changing anything. A failure does
not partially append data or release a session. Terminal commits atomically store
the matching `run.finished` outcome and clear the active-run slot.

Request/profile identity, fixed system inputs, existing bound inputs, settled tool
results, and completed model attempts cannot be silently replaced. Uncertain
tool effects retain their physical attempt and idempotency identity. Retry and
reconciliation policy belongs to the driver; store validation does not authorize
another external dispatch.

Events are checked against their typed referenced data and the committed
checkpoint/ledger. A record merely existing is insufficient. Verification events
currently require the matching `outcome.verification`; separate intermediate
verification history is not represented yet. Resume event validation does not
replace the driver's approval, input-schema, or command-deduplication checks.

## Protected records and replay

`ProtectedRecord::new` owns its JSON value and computes its digest. Its
`(record_id, revision)` is immutable within a scope and can be reused by another
run in that scope. Explicit `read_record` access still requires the Host's separate
protected-record permission. Debug output omits the value. External artifact and
evidence references are resolved through their own storage/authorization ports.

`read_events` uses an exclusive cursor and returns a bounded ordered page. Reading
does not consume events. The memory store retains its full history while alive
and reports the first/latest available sequence. Pages are limited to 1,000 events.

The reference store declares `durable=false`, `cross_process_leases=false`, and
`event_replay=true`. Admission requiring durability is rejected. Recreating a
memory store is not process-restart recovery.

The [independent consumer](../tests/support/state_consumer.rs) exercises admission,
idempotent replay, lease acquisition, terminal commit, event replay, and a foreign
scope lookup. It is built against the extracted package by
`python3 scripts/check-package.py`.

## Atomic execution transactions

StateStore implementations also implement `ExecutionTransactions`. Admission
stores the original authenticated `execution_principal_ref`, an initial segment
identity, and an optional validated submitted-request snapshot. A later reviewer
or worker does not replace the execution principal.

`begin_segment` accepts initial ownership or a resume/control transition in one
transaction. The caller proposes checkpoint/messages/events/records, but the
store validates them and supplies the lease itself. A failed validation rolls
back lease acquisition and command consumption too. Replaying an accepted command
returns its original segment with no new lease; it must not start another driver.
The result's Run state may be newer than that historical segment.

`submit_control_command` durably records an authenticated command without changing
Run status. Reusing its ID with a different payload is a conflict. Cancel/expire
processing requires a matching terminal transition, and expiry requires the Run
deadline to have elapsed. Terminal submissions record no-op receipts without
changing the saved outcome. Active drivers consume Stop/Cancel/Expire through
`CommitInput.control_commands` in the same fenced transaction as settlement.
Idle Cancel/Expire use a new control segment; an already settled Stop is a no-op.

`read_execution` returns protected segment and command history. Apply current Host
authorization before exposing it. Past segment outcomes are immutable; a later
resume or idle terminal control creates another segment. Application-state and
policy execution remain driver responsibilities.

The memory reference implementation validates a private scope working copy under
its mutex and publishes it only after all checks pass. SQLite executes the same
operation inside its existing immediate transaction, persisting history,
checkpoint, records and events together. This correctness-oriented reference path
copies scope state; it is not a claim of constant-time mutation for large histories.
