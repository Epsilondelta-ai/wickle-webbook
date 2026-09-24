# Bind system tool inputs

`InputBinder` combines a saved model call with a compiled tool contract and
Host-supplied system values. It saves immutable execution arguments before a
Tool executor can use them. It does not invoke the Tool or make missing IDs up.

For a tool exposing `query` and `limit`, a model can submit `{"query":"recent"}`.
The binder can apply the optional model default `limit=10` and read the required
`workspace_id` from the run's system snapshot. Its execution arguments contain
exactly those three fields. Other registered run values are not merged in.

## Capture run inputs

`RunSystemInputs::capture(scope, supplied, &registry)` owns and validates the
provided map, retaining the input definition metadata. Supplied keys must be
registered, use the Run source, and pass their value schemas. A resolver-sourced
key cannot also be supplied in that map. Missing values are checked when a Tool
needs them, so registration does not require future resources to exist already.

Use `to_record` and `snapshot_ref` to place the protected record and reference in
the same StateStore admission transaction. Include that reference in
`admission_digest`. The values, definition versions, and owning scope then remain
fixed for the Run.

On resume, omitted `ExecutionContextData.system_inputs` reuses the snapshot. Any
explicit map, including an empty map, must match the original. Another approver's
identity changes the current policy context, not the original system values.

## Resolve and bind a saved call

```rust
let result = binder.bind(
    &compiled_tool,
    &call_id,
    &execution_context,
    &run_budget,
).await?;
```

The call must already exist in the Run's Tool ledger. The binder validates its
original model arguments, adds supported model defaults, and reads only the
hidden keys selected by the compiled contract. Aliases of the same system key
share one lookup, including a missing result.

`SystemInputResolver` receives one registered key, its definition/resolver version,
normalized model inputs, and current scope/actor/deadline/cancellation. It receives
neither the complete system map nor credentials. It is a read-only lookup
contract. `None` means missing; a returned JSON null is a value and must be allowed
by the relevant schema.

Default application is deliberately narrow: only missing optional top-level
model parameters use a direct default or one reached through a supported local
reference. Required, supplied, null, nested, and conditional values are not
replaced or inferred. System defaults are never used. Both value schemas and the
full execution schema must pass after assembly.

## Enforce current policy

`PolicyAction::ResolveSystemInput` authorizes a resolver lookup before it starts.
A denied lookup performs no resolver call. A lookup that itself requires approval
returns `SystemInputApprovalRequired`; no incomplete binding or fabricated target
is saved. The Host must handle that permission before trying again.

Once all values are known, `PolicyAction::ExecuteTool` receives the exact
`ToolPolicyInput`. The Host policy checks actual target existence, ownership,
and current permission. UUID format alone does not establish those facts.
Deny/error prevents a new binding from being saved. Allow or a final approval
requirement saves the complete candidate and returns its decision.

`ToolBindingResult` is not a lasting dispatch permission. The execution boundary
must still check the saved inputs, current policy, approval, cancellation, lease,
and attempt budget immediately before performing the Tool operation.

## Reuse frozen values

`BoundToolInput` preserves original and normalized model inputs separately from
execution arguments, with source revisions and descriptor/compiler/binding
digests. The record and `ToolCall.bound_input_ref` are committed atomically.

Binding the same call again reads that exact record, validates the compiled
contract and original run inputs, and rechecks current policy. It does not query
the resolver again. A new call can obtain a newer resolver value. If persistence
succeeded but its acknowledgement was lost, the next bind still finds the saved
target. If persistence never succeeded, no execution-ready binding was returned.

The public restore method requires the saved call's exact record ID, revision,
and digest. Debug output excludes values, and protected serialized inputs must
not be copied into a model transcript or public trace.

`InputBindingLimits` bounds distinct resolver keys, individual value bytes, and
protected record bytes. Run deadline, cancellation, and lease checks apply to
lookup, policy, and storage boundaries. Cancellation of a Future is not a claim
that an external storage operation was rolled back.

The [independent binding consumer](../tests/support/input_binding_consumer.rs)
shows Run inputs, optional model defaults, unused-key exclusion, and a changing
report resolver whose value stays fixed for an already-bound call.
