# Durable run controls

Use `Agent::submit_control_command(run_id, command, context)` to submit Cancel,
Stop or Expire. Supply a stable `ControlCommand.command_id` and the authenticated
caller's `principal_ref`. Reusing the same ID and payload returns its receipt;
a changed payload conflicts. Submission requires current authority for the action.

`ControlReceipt` contains the run ID, command ID and an optional processed segment
ID. An absent segment ID means pending. Use `get_control_receipt` for a read-only,
authorized lookup. `get_run` reads current metadata without expiring or cancelling
work. Its `deadline_expired` flag observes the original Run deadline for nonterminal
runs using the injected clock, without acquiring a lease or updating stored usage.
A clock earlier than the stored high-water time returns `ClockRegression`.
Terminal views report false. `RunHandle::cancel(reason, context)` generates a
fresh command ID. Use `submit_control_command` with an explicit `ControlCommand`
when the caller needs a stable retry key.

The owning driver checks pending commands at dispatch boundaries and during its
heartbeat. It saves the outcome and command consumption in one fenced commit.
A Host may deliver the command ID to a Worker and call `process_control_command`;
this requires `PolicyAction::ProcessControl` permission. Wickle does not provide
a remote notification service. A crashed running interval still needs explicit
recovery after lease expiry; a pending command does not steal its lease.

When a Run is waiting or interrupted, Cancel and elapsed Expire are handled by a
control-only segment through `begin_segment`. This atomically acquires ownership,
consumes the command and saves the terminal result. No model or Tool is executed.
Existing application state and uncertain effects are retained. An early Expire
stays pending until a Worker processes it after the deadline, or an active driver
reaches that deadline. A Stop on an already settled interval is a no-op.

Terminal Cancel/Expire requests produce recorded no-op receipts. They do not
replace the saved outcome or start another interval.

## Handles identify intervals

`RunHandle::run_id()` identifies the logical Run; `segment_id()` identifies one
accepted execution interval. An old waiting or interrupted handle keeps its
original outcome and event boundary after resume or cancellation. Read the latest
Run or command receipt to inspect a later cancellation. Repeating Start returns
the current or last interval's handle without starting another driver; repeating
a resume command returns the interval originally accepted for that command.

Interruption policy controls active stop settlement, including protected
cancellation and budget causes. [Local cooperative stops](interruption-policy.md)
remain available through `stop_execution`; they do not submit a remote command.
