# Consume durable events outside the agent loop

The core commits `RunEvent` records with its state. A Host can read them through `StateStore::read_events(scope, run_id, after_seq, limit)` and separately deliver authorized observations to memory or graph services. Delivery failures do not restart the business Run. No queue, subscription manager or memory writer is part of `AgentBindings`.

## Read and authorize

The Host tracks the Runs belonging to each subscription and polls their durable event pages. Require both `StateStoreCapabilities.durable` and `event_replay` for durable delivery; a best-effort callback or in-memory store is insufficient.

`EventPage` provides ordered events, `next_after_seq`, `has_more`, `first_available_seq` and `last_available_seq`. Preserve scope, Run identity and event IDs. Check sequence continuity and detect a missing retained range rather than silently advancing past it. A fully recorded, identical older page is a duplicate, not a retention gap.

Wrap event queries in `PolicyGate` with `PolicyAction::ReadEvents`. A record reference in an event does not grant permission to read its contents: authorize `PolicyAction::ReadRecord` separately before `read_record`. `run.finished` references the committed `RunOutcome`; tool, wait, model and verification events likewise carry typed references. Do not copy protected system inputs or recovery-only messages into general memory. Record observed outcomes and their provenance, not an invented explanation of business success.

## Separate discovery from application

A Host subscription pins scope, consumer/adapter and target connection revisions, event/trigger filters, limits and retry policy. Its discovery cursor belongs to that immutable subscription revision. Changing a target must not silently send old deliveries to the new target.

In one Host transaction, store delivery records and filter decisions, then advance the discovery cursor. Use a stable deduplication identity containing source scope, Run ID, event ID and subscription revision. Pending deliveries remain in the Host store even when the discovery cursor moves ahead.

| Receipt | Meaning | Safe next action |
| --- | --- | --- |
| Applied | The external effect was confirmed | Record completion |
| Accepted | An asynchronous operation was accepted | Observe its operation reference; do not claim searchable memory yet |
| Not-applied retryable failure | Non-application or safe idempotent replay was established | Retry within explicit bounds |
| Permanent failure | Processing cannot continue under this subscription | Record failure and stop automatic delivery |
| Unknown | The effect cannot be established | Reconcile if supported, or leave it unresolved; do not blindly write again |

Use the same delivery ID as an external idempotency key where supported. A Worker can die after an external write but before saving its receipt. Claim generations fence stale Workers from overwriting later observations. Recovering an uncertain write is an observation/reconciliation operation; it is not permission to redispatch it.

A `source_gap` requires explicit Host backfill or recovery. It must not be converted to a successfully consumed range. Filter out the Host's own analysis/ingestion Runs where appropriate so memory updates cannot create an unbounded feedback loop.

## Executable reference Host

The [delivery journal fixture](../tests/host_contract/delivery.rs) uses a separate SQLite database to demonstrate atomic cursor/record updates, immutable target revisions, bounded known-unapplied retries and claim fencing. It has a fixed `run.finished` filter and a two-dispatch limit. It is test/example code, excluded from library archives; it is not an operational DeliveryStore or queue service. Production authentication, scheduling, claim-expiry policy, backfill, retention and backend-specific reconciliation remain Host responsibilities.

The [event consumer](../tests/support/event_consumer.rs) runs actual Wickle agents with synthetic model and memory ports:

1. Complete a Run without memory and read its committed events.
2. Persist a delivery and an accepted operation, restart the Host journal, and deduplicate the replay.
3. Confirm that acceptance has not changed memory and replaying the original request does not rerun its model.
4. Authorize the protected outcome separately, simulate the external operation applying, and save the applied receipt with the current claim.
5. Start a new Run and verify that its `ContextSource` supplies the updated observation to the model.

Run the journal contract tests with `cargo test -p wickle-state-sqlite --test host_contract`. Run `python3 scripts/check-package.py --allow-dirty` to execute the consumer against extracted library archives in a separate directory. The tests include injected SQLite transaction failure, older-page replay, concurrent claims, stale receipts, uncertain writes and missing source ranges.

These are reference Host checks, not live validation of Zep, Mem0, a graph database or their durability guarantees. See [context sources](context-sources.md) for the read side and [adapter assembly](adapters.md) for connecting alternate implementations without changing the core.
