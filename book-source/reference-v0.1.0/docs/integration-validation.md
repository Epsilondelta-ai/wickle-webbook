# Validate independent Hosts

`python3 scripts/check-package.py --allow-dirty` packages the libraries and builds
consumers outside the checkout. In addition to the focused examples, it builds two
separate application workspaces from the same extracted core archive. Their
manifests select different adapter dependencies. Before each application runs,
the script checks its resolved core dependency path. After each execution, it
checks that the core archive and extracted source digests remain unchanged.

## Retrieval and aggregation

The [gather consumer](../tests/support/gather_consumer.rs) implements two distinct
memory sources and a graph retrieval source through public `ContextSource`
contracts. One memory implementation reads an owned field, while the other reads
a keyed record store. Replacing the memory source changes the selected period and
the resulting aggregate without changing the core.

The model receives source data in data envelopes and submits only `query` and
`limit`. `InputBinder` adds the Host's workspace UUID. The Tool reads bounded
records and returns an `EvidenceRef`; the next model step verifies the referenced
content hash and uses the aggregate. The consumer also loads Profile JSON from a
file, rejects an unregistered Tool and an attempted profile-level grant, and
rejects another scope before additional model or Tool execution.

These memory and graph implementations are test doubles. They demonstrate the
public extension contracts, not a Zep, Mem0 or graph-database integration.

## Approval across process replacement

The [report consumer](../tests/support/report_process_consumer.rs) uses actual
Host subprocesses, `SqliteStateStore` and `AdapterRuntime`. The first process
reaches an approval wait, persists a resume command, awaits component release and
exits without running destructors. A second process resumes with the reviewer
context and a fresh adapter binding.

A changed resolver value does not replace the saved UUID inputs. The approved
Tool creates and synchronizes one report file; exclusive file creation detects a
second application. The parent verifies the output hash, distinct process and
binding identities, matching open/close records and absence of a file at the
changed target. Replaying the approval command adds no model call, adapter open
or write. The engine remains an in-process library in each Host.

## Repeated lifecycle and timing

The [lifecycle consumer](../tests/support/lifecycle_consumer.rs) reuses one Agent
and Session for twelve Runs: six approval/resume paths and six waiting
cancellations. It verifies eighteen adapter opens and closes, no surviving
instance references after handles are dropped, no increase in pending Tokio
tasks after cleanup, and bounded event pages. Duplicate resume commands do not
add model calls.

The consumer prints actual per-Run wall times, synthetic-model construction
time and local non-model time. There are no provider network calls. The local
measurement includes core execution, SQLite and Host callbacks; it is neither an
isolated engine CPU measurement nor a latency guarantee for real models. Timing
values are observations, not latency targets. Normal Run and cleanup deadlines
still apply.

Repeated execution exposed the cost of validating the entire stored scope on
every SQLite operation. The adapter now reuses an opaque validated checkpoint
only when the current database row matches exactly. Separate tests verify
external updates, malformed data with a recomputed checksum and rollback after a
failed SQL write. See [SQLite persistence](sqlite-state-store.md) for cache and
storage limits.

## Other boundaries

The full workspace suite covers protocol failures, authorization, Tool effects,
Hooks, Skills, context compaction, verification, cancellation and recovery.
[Model validation](model-support.md) distinguishes local provider contracts from
real-service evidence. [MCP](mcp.md) and [event delivery](event-consumers.md) have
separate consumer checks. No browser UI or HTTP server is required to use or test
the core library.
