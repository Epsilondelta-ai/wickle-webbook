# Recovering an interrupted run

Recovery continues a persisted `Running` run after its previous execution owner has
stopped. Use a durable `StateStore` across processes and recreate compatible model,
Tool, adapter, hook, source, skill, and verification bindings. Recovery does not
start a worker service or require a network API.

Read the run with current details permission, then submit a recovery command:

```rust,ignore
let Guarded::Completed(snapshot) = agent.get_run_details(&run_id, &context).await?
else {
    // Obtain the required approval before reading protected state.
    return Ok(());
};
let source = snapshot.recovery_record(RandomIdSource.next_id()?)?;
let command = ResumeCommand {
    run_id,
    expected_revision: snapshot.revision,
    command_id: RandomIdSource.next_id()?,
    action: ResumeAction::Recover {
        recovery_ref: source.reference().clone(),
    },
};
let recovered = agent.resume(command, context).await?;
```

`recovery_record` constructs a fingerprint of the observed checkpoint; the Host does
not need to save that record separately. Under a new lease, the core compares it
with authoritative state and commits the source, command, receipt, and recovery
event together. An active lease returns `LeaseBusy`; a changed checkpoint requires
a fresh read. Reusing the same accepted command returns its existing segment
without charging acceptance again. Reusing its ID with different contents fails.

A `Waiting` run uses the existing approval, input, or external-receipt resume
commands. For a terminal run, replay the original request or read its saved result.

## Model and Tool boundaries

- A completed model response is reused with its original logical step and exact
  projection. An incomplete physical attempt becomes `Interrupted`, linked to the
  accepted recovery command. Its reservation remains charged. Retrying uses the
  same step, route, and input fingerprint with a new charged physical attempt.
- Planned calls use saved inputs. If binding already completed, a changed resolver
  cannot silently replace a stored foreign key or other system value.
- A dispatched call without a confirmed result is recorded as `Unknown` during
  recovery acceptance, including when the run deadline has elapsed. Recovery
  never assumes that interruption rolled back an external write.
- A Tool declaring `reconcile: true` can implement `ToolExecutor::reconcile` to
  query the original attempt using its frozen arguments and idempotency key.
  This method must not execute the business operation again. A known observation
  is validated and committed before execution continues.
- Without a usable query adapter or a known result, the run waits for explicit
  external confirmation. No subsequent model step or remaining Tool call proceeds
  past that uncertainty. Already settled calls are not executed again.

Each nonexpired recovery acceptance charges one recovery attempt. External effect
queries and any model retry/fallback consume their applicable budgets as well.
Budget or policy failures retain unresolved effects in the resulting failure or
limit outcome. Expired recovery closes the run without new model or Tool dispatch.

Use lease and heartbeat settings suitable for the storage backend. The defaults
are a 30-second lease and a 5-second heartbeat interval. Fencing still rejects a
previous owner's late commit after another owner acquires the run.

## Persistent storage failure

If storage becomes unavailable, the engine stops starting external work and does
not announce an uncommitted success. When `RunHandle::outcome` returns
`ErrorCode::PersistenceUnavailable`, `error.persistence` can contain:

- `run_id` and `last_confirmed_revision` from a successful store observation;
- the call, attempt, and idempotency identities of unconfirmed effects;
- references to their protected frozen inputs, without copying input values.

These diagnostics require current run-details permission, even if the live store
cannot be read. They are local metadata, not a saved terminal result. They may lag
a commit whose acknowledgement was lost, and are unavailable in a new process
until it successfully reads persisted state. Once storage recovers, read the
latest checkpoint before constructing a recovery command.
