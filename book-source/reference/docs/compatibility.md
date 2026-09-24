# Runtime compatibility

Wickle has separate compatibility boundaries for Rust source code, serialized profiles, and persisted executions. A package version alone does not establish that a saved execution can be resumed by a different runtime.

## Rust applications and custom stores

Applications may construct public request structs directly and exhaustively match public status enums. Adding a struct field or an enum variant can therefore require application changes, even when a JSON field has a default. Adding a required `StateStore` method also requires changes to custom store implementations.

Before upgrading, build the application and its custom ports against the selected version. Check both serialization and actual execution paths. Do not implement new atomic storage operations as separate read/write calls merely to satisfy a trait: command consumption, execution ownership and state transitions must preserve the guarantees of the target runtime.

## Stored data

Keep a backup before changing a persistent store. Check the snapshot, event and database schema versions independently. Preserve the original request comparison rules and digest version. Re-encoding an old request with a new serializer is not evidence that it is the same submitted request.

An upgrade procedure must distinguish these cases:

| Saved execution | Required handling |
| --- | --- |
| Terminal | Preserve the original outcome and event history; do not execute the request again. |
| Not dispatched | Resume only if the original execution configuration and input contract can be reconstructed. |
| Dispatching or unknown effect | Reconcile the external effect before considering another attempt. |
| Waiting for approval or input | Preserve the wait identity, target and saved tool binding. An answer must not change the original execution principal. |
| Missing required historical information | Use a documented migration or finish the execution with the original runtime. Do not invent missing revisions or assume that an external operation did not run. |

A successful database open or JSON decode is not a successful recovery test. Verify the number of actual external effects, the returned outcome and the stored event history after restarting a separate process.

## Upgrade support

No automatic migration to a future runtime format is promised by this guide. Use the migration and compatibility instructions shipped with the target release.
For this contract change, follow [the v0.2.0 migration guide](migration-v0.2.md). Where an active execution cannot be migrated safely, stop accepting new work, resolve or finish active executions with the original runtime, and then upgrade using a backed-up store.

Do not start an older runtime against a store that has written a newer format unless downgrade support is explicitly documented. Restoring a database backup does not undo external tool effects; reconcile those effects before resuming restored executions.

See [state storage](state.md), [SQLite storage](sqlite-state-store.md), and [recovery](recovery.md) for the current runtime contracts.
