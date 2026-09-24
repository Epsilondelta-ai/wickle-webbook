# Supply retrieved data and memory

`ContextSource` supplies read-only data to the agent loop. Use it for automatic
retrieval or memory lookup; use a Tool when the model should choose an operation
or when an operation changes external state. The Host owns credentials, clients,
and source-specific access checks. The core collects bounded results, saves them,
and checks permission before use.

## Register a source

Implement `ContextSource::provide` and `authorize_use`. `provide` receives a
`ContextRequest` containing the original user input, scope, Run/session, exact
source definition, trigger, and optional logical model step. It does not receive
the whole transcript, credentials, or the system-input map. `ContextCallContext`
carries the current actor, grant, cancellation, and deadline.

The same catalog source or adapter export can serve both triggers. Each trigger
has its own binding limits and saved slot; the runtime opens the export once per
segment. Repeating the same source and trigger is an error.

The source returns one of these results:

| Result | Meaning | Agent behavior |
| --- | --- | --- |
| `Ready` | Valid, nonempty data | Save and authorize before use |
| `Empty` | Successful lookup without data | Save an empty active slot, including for a required source |
| `Unavailable` | Explicit operational failure | Save the diagnostic; stop if the source is required |

A callback timeout may become optional unavailability. Cancellation, permission
denial, malformed data, and limit violations always stop execution. An optional
source still requires a valid registered definition and configuration.

For direct bindings, construct a `ContextSourceRuntime` from the Agent's state,
policy, clock, ID source, a scoped `ContextSourceRegistry`, and a Host
`ContextTokenEstimator`. Assign it to `AgentBindings.context_sources` and select
its exact catalog reference in `AgentProfile.context_sources`.

For example, a profile can select a registered memory source once per Run:

```json
{
  "source": {"source_id":"memory", "version":"1"},
  "trigger": "run_start",
  "required": false,
  "timeout_ms": 1000,
  "max_items": 16,
  "max_bytes": 16384,
  "max_tokens": 4096
}
```

An adapter export instead uses `{"adapter_binding":"records","export_id":"memory"}`
as its source selection. This selection remains distinct from the native
`ContextSourceDefinition.source` identity.

In component mode, register catalog sources with
`AdapterRegistry::with_sources(Vec<CatalogSourceRegistration>)` or declare an
`AdapterExportDefinition::ContextSource` and return the matching
`AdapterExportInstance::ContextSource` from the factory. Set
`AgentBindings.context_token_estimator`; direct `context_sources` must be `None`.
The [adapter runtime](adapters.md) resolves definitions before opening resources
and wraps each source in the current execution segment's lifetime.

## Validate data and apply limits

`ContextSourceDefinition` pins the native source version and contract version 1.
Automatic sources may claim only `Retrieval` or `Memory` origin. Returned items
must already have the correct scope, source, digest, and applicable Run or model
step lifetime. The core rejects incorrect claims before assigning its namespace.
Two sources can return the same local item ID without colliding.

Each binding has independent item, serialized-byte, token, and callback-time
limits. `ContextTokenEstimator` measures validated, namespaced data; its version
is pinned in the plan and batches. `ContextSourceUsage` preserves optional
provider-reported usage separately. It cannot substitute for the Host estimate.
The final model request still passes through `ModelTokenEstimator` and the
selected model's limits.

Source content remains reference data. It cannot become System instructions or
grant permission to call a Tool. Before-model Hooks receive current source data
in profile order followed by saved before-run Hook additions. Before-run Hooks
retain their original input and Hook-chain contract.

## Reuse saved batches and check current access

`run_start` collects once per Run. `before_model` collects once per logical model
step. Physical model retries and resume reuse committed queries; they do not
silently retrieve a different revision. A new Run queries again. If a callback
completed but no batch was committed, a later execution may query again, so
callbacks must not hide writes or detached work.

Each immutable `ContextBatch` preserves the request, original provider items,
validated namespaced items, result status, optional source revision and usage,
collection time, and estimator version. `RunSnapshot.context_batches` retains
history, while `source_states` identifies the current slots. A later empty or
unavailable result replaces its slot; older successful data is not substituted.

`authorize_use` checks the original source-local item IDs and saved revision. It
must not refresh or replace the batch. The core invokes current Host policy and
source checks before local context Hooks receive cached data and before each
physical model attempt. The latter includes the selected model route. Revoked
access also blocks a projection containing Hook-derived copies of that data.
Provide and use checks are separate `PolicyAction`s; registration grants neither.

Waiting releases adapter instances. Resume uses a new instance and current actor
to check the saved batch. Observer-only finalization does not activate sources.
In-process callbacks remain trusted Host code; the library is not a sandbox for
arbitrary executable adapters.

The [source consumer](../tests/support/source_consumer.rs) exercises collection,
retry reuse, current access checks, and SQLite restoration with synthetic ports.
Run it with the other independent consumers using
`python3 scripts/check-package.py --allow-dirty`.

For post-run memory updates, see [external event consumers](event-consumers.md). The Host owns delivery and application receipts; the next Run reads applied data through `ContextSource`.
