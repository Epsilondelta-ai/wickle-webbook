# Migrate to v0.2.0

Version 0.2 changes Rust contracts and adds execution history. Rebuild the Host
and its custom ports; do not replace the library binary while old workers still
own active executions. The library release, JSON format identifiers and provider
model versions are separate. A v0.2 library still uses several `wickle.*.v1` data
formats; do not rename those markers.

## Prepare existing data

1. Stop accepting new work in your Host, including scheduled submissions.
2. Finish or resolve legacy nonterminal Runs with the original runtime. This
   includes approval/input waits and unknown external effects. Do not rewrite
   them as successful or assume an unconfirmed write did not occur.
3. Stop the old workers and create a consistent database backup. With the SQLite
   CLI, for example:

   ```sh
   sqlite3 state.sqlite3 ".backup state-before-v0.2.0.sqlite3"
   ```

   Use SQLite's [backup facilities](https://sqlite.org/backup.html) or
   [CLI backup command](https://sqlite.org/cli.html); copying only a live main
   database file can omit WAL data. Keep the backup outside the application’s
   writable working data.
4. Upgrade all Wickle crates together and build your custom ports against the
   new types. Exercise the application against a backup copy first.
5. Read and compare stored terminal outcomes and event histories before accepting
   new requests. Confirm that new requests, waits, recovery and external effect
   reconciliation work in your own Host.

Legacy terminal records remain readable without rewriting them. The first
successful new admission can write a `wickle.state-store.v2` scope checkpoint in
one transaction, retaining the original terminal records and marking legacy Run
IDs. Merely opening or reading the database does not upgrade a checkpoint.

Legacy active records lack the ownership/command history required by the new
runtime. `execution.legacy_drain_required` prevents new admission into such a
scope; `execution.legacy_checkpoint` rejects execution-history access. There is
no automatic active-run migration that invents actors, segments or receipts.
See [SQLite storage](sqlite-state-store.md).

Do not downgrade a store after it has written the newer format. Restore a
compatible backup if you need to return to the old runtime, and reconcile any
external effects that occurred after that backup. A database restore does not
undo those effects.

## Update Rust integrations

| Integration | Changes to account for |
| --- | --- |
| `AgentBindings` | Supply `interruption_policy: None` or a versioned `InterruptionPolicyBinding`. Custom state requires `AppStateSchema`. |
| Profile and requests | Configure `model_options` explicitly where needed. `RunRequest.max_output_tokens` is optional; runtime and model caps still apply. |
| Model bindings | Provide `default_options`; effective options use Binding → Profile → Run precedence. Verification and compaction use their own purpose configuration. |
| Status/outcome matches | Handle recoverable `Interrupted` and its `AppState` separately from terminal cancellation, failure and exhaustion. |
| Handles and commands | A handle identifies an execution segment. Resume returns a new interval; an older handle's saved outcome does not change. Use stable command IDs. |
| Custom stores | Implement `ExecutionTransactions` atomically with checkpoints, events, command acceptance and lease ownership. Do not emulate it with unrelated reads/writes. |
| Admission/commit DTOs | Supply original principal, grant and submitted snapshot evidence; preserve control-command settlement and segment records. Compiler errors identify the new required fields. |
| Custom model ports | Return a pure versioned `ProviderToolSchemaCompiler` when native schema pass-through is insufficient. Keep transport retries disabled. |
| Minimal Run views | `RunView.deadline_expired` is read-only. Low-level `PolicyGate::run_view` now takes a `Clock`; `Agent::get_run` supplies its configured clock. |
| Direct Gemini/Vertex codecs | `encode_request` and `encode_vertex_request` return JSON bytes. Send them as the HTTP body; do not JSON-serialize the byte vector again. |

Run `cargo check` and your application's behavioral tests after updating struct
literals, exhaustive enum matches and custom trait implementations. The package
examples show working public-interface composition:

```sh
python3 scripts/check-package.py --consumer agent
python3 scripts/check-package.py --consumer resume
python3 scripts/check-package.py --consumer execution_contract
```

## Preserve pinned contracts

`RequestSnapshot` keeps original submitted JSON and its canonicalization version.
Compare a retransmission using that saved version before resolving today's
configuration. Do not recalculate historical digests with a new encoder or
reconstruct number spellings already lost by an older serializer.

Prepared steps retain resolved options, Tool sets, provider schema projections,
decoding plans and context identities. Retry uses that preparation with a new
physical-attempt reservation; an authorized route change creates a new projection.
Current permissions still apply to pinned data and tools.

Provider Tool contract format v2 explains only the encodings actually in use.
Saved v1 contracts restore their exact original guidance, digest and codec.
OpenAI/Azure compiler revision 2 bounds reference expansion; a saved compiler-1
contract is decoded from its stored representation rather than recompiled.
Unknown contract or assembler formats are rejected instead of silently upgraded.

Keep system values, opaque model continuation and protected records out of public
logs. Use [inspect_step](step-inspection.md) for redacted stored evidence; a
reservation does not prove that a model request reached the provider.

Before rollout, run the complete package check without `--consumer` and verify
your own storage, authorization and effect adapters. Local provider fixtures and
[observed live smoke checks](model-support.md) have different scopes; neither
promotes every account/model combination to supported production use.
