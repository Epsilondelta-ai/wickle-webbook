# Execution interruption and application state

Dropping an observation future does not cancel an agent. To stop an execution
interval, call `RunHandle::stop_execution` with `SegmentStopped` or
`HostShutdown` and an authorized `ExecutionContext`. Its `Requested` receipt
means the local driver was signaled; await `handle.outcome(&context)` for the
persisted result. `NotLocal` means this process has no active driver for that
handle. The method does not deliver a stop to a remote worker.

The default stop produces `OutcomeResult::Interrupted` when a recoverable
checkpoint can be saved. The session retains its active run. Repeating the
original request returns the saved result without dispatching another model
call. Resume through explicit recovery; do not submit a replacement request
into the occupied session. Calls whose external effects cannot be confirmed
remain unresolved and must not be blindly retried.

## Add business state

Set `AgentBindings::interruption_policy` to an `InterruptionPolicyBinding`.
Implement `InterruptionPolicy::identity` and `decide`. The callback receives
`InterruptionInfo`: run identity, current phase, store capabilities, fixed
configuration, current application state, and the interruption's cause,
checkpoint and unresolved effects. It returns an `InterruptionDecision`:

- `UseDefault` keeps the core's default decision.
- `Pause` requests a recoverable interruption.
- `Cancel` or `Fail` requests a terminal outcome.
- Optional `AppState` contains your namespace, status and JSON metadata.

Provide `AppStateSchema` whenever the callback returns application state. Its
JSON Schema validates the entire `{namespace, status, metadata}` object. The
namespace must also match exactly. Application state is limited to 16 KiB;
the persisted policy plan is limited to 64 KiB. Keep credentials out of policy
configuration and state.

Application status is independent of core status. A policy cannot undo an
explicit user cancellation, replenish an exhausted budget, turn ownership loss
into success, or authorize more model/tool calls. The callback receives facts
and proposes data; it should not perform external effects or mutate execution
storage. The core validates the proposal and persists the accepted decision,
application state and outcome together.

Policy identity, configuration, schema and timeout are pinned at admission.
Restore the same policy binding before recovery. A missing or changed binding
rejects recovery without replacing the saved interrupted state.

## Time bounds and failure behavior

`AgentSettings::interruption_timeout_ms` defaults to 1,000 ms. The effective
callback timeout is the smaller of this Host cap and the binding's timeout.
Callback errors, panics, timeouts and invalid decisions fall back to the core
default and leave a safe diagnostic in the protected interruption record.

`AgentSettings::cleanup_timeout_ms` defaults to 5,000 ms and may be explicitly
changed by the Host. Stop settlement and resource cleanup share this finite
window; it does not extend the run's model or tool budget. After persistence,
the owned lease is released before adapter cleanup. Terminal commits release
it atomically. Known ownership loss prevents the former owner from committing
or releasing the lease.

Cleanup failures do not rewrite a stored outcome or repeat business work.
Inspect `RunHandle::component_release` for local cleanup diagnostics. Storage
or ownership failures can instead prevent a confirmed stop outcome and return
a typed error. A forced process exit cannot guarantee callback or cleanup
execution; recovery depends on stored checkpoints and lease fencing.

Timeouts require cooperative asynchronous implementations. Blocking callbacks
cannot be forcibly stopped by a Rust future timeout. Isolate such extensions
in a Host-managed process when hard termination is required.

## Runnable policy and recovery example

The [agent consumer](../tests/support/agent_consumer.rs) supplies a
`MaintenancePolicy` with a pinned identity, configuration and application-state
schema. On `info.interruption.cause == HostShutdown`, it returns `Pause` with
an `operations` state; other causes use the protected default. The state records
application metadata and does not replace the core status.

```sh
python3 scripts/check-package.py --consumer agent
```

After the stop, the example reads the authorized snapshot, obtains its
`recovery_record`, and submits a `ResumeAction::Recover` with a new stable command
ID. The model becomes available again, the new handle succeeds, the old handle
retains its interrupted outcome, and repeating the command performs no new work.
The example uses synthetic model ports and real SQLite, not a live provider.
