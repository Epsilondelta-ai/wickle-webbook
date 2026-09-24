# Run an agent

For a runnable Host, begin with [the quickstart](quickstart.md). Existing
applications can follow [the v0.2.0 migration guide](migration-v0.2.md).

`create_agent` builds a scope-bound facade from an `AgentProfile` and existing
Host components. The runtime executes text requests through admission, context
preparation, model routing, and serial tool calls until a turn ends or execution
stops. Outcomes, tool observations, and events are saved; callers can replay events
and explicitly cancel execution.

```rust
use wickle::*;

pub async fn run_once(
    profile: AgentProfile,
    bindings: AgentBindings,
    request: RunRequest,
    context: ExecutionContext,
) -> Result<Guarded<RunOutcome>, ContractError> {
    let agent = create_agent(profile, bindings)?;
    match agent.start(request, context.clone()).await? {
        Guarded::Completed(handle) => handle.outcome(&context).await,
        Guarded::ApprovalRequired(challenge) => Ok(Guarded::ApprovalRequired(challenge)),
    }
}
```

The [text consumer](../tests/support/agent_consumer.rs) and
[tool-loop consumer](../tests/support/tool_loop_consumer.rs) configure the Host
components, execute requests with synthetic models, reconnect to their events,
and reopen SQLite state. The [resume consumer](../tests/support/resume_consumer.rs)
reopens a saved approval wait with new Host instances and a different reviewer,
then completes the same Run using its frozen inputs. These consumers use real
SQLite and synthetic ports; they make no provider network calls. Run them with
`python3 scripts/check-package.py --allow-dirty`.

## Supply Host components

`AgentBindings` contains the exact scope, `StateStore`, current `PolicyGate`,
`ProfileResolver`, pinned `ModelRouter`, configured `ModelExchange`, trusted Host
instructions, `SystemInputRegistry`, optional `ToolRegistry`,
`SystemInputResolver`, `ExternalReceiptVerifier`, `HookRuntime`, optional
`ComponentRuntime`, optional `ContextSourceRuntime` and context token estimator,
optional `SkillRuntime` and `ArtifactRuntime`,
optional `ContextRuntime` for context selection and compression,
optional `VerificationRuntime` for output schemas and candidate verification,
clock, ID source, model token
estimator, and `AgentSettings`.
A single Agent instance owns one scope; use separately configured instances for
other scopes.

Constructing the Agent validates profile structure and finite settings. It does
not invoke component callbacks, open connections, create a runtime, spawn tasks,
or read environment files. The Host supplies Tokio before calling `start`.
Metadata resolution and model work begin only during execution.

`ModelTokenEstimator` estimates the final request for its selected route. It is a
synchronous Host callback with no I/O. Its estimate is used for capacity checks
and remains separate from provider-reported usage. `AgentSettings` bounds output,
request/response/context sizes, admission preparation, lease renewal, and observer
polling. Run-wide model, tool, recovery, and elapsed limits come from the profile.

The driver supports text instructions, text or JSON schema output, the bounded context strategy,
registered catalog tools, lifecycle Hooks and context sources, adapter
Tool/Hook/ContextSource exports, versioned Skill loading, artifact references,
bounded context previews and compression, and
`turn_end` or `verified` completion. Profile instruction-asset loading is not yet
connected to this driver and is rejected explicitly. See [output verification](verification.md)
for criteria, repair budgets, and candidate approval waits.

Selected lifecycle Hooks can add bounded context, transform model-owned tool
arguments, or observe committed results. Their saved transformations are reused
across retry and resume; observer reports stay separate from execution outcomes.
See [lifecycle Hooks](hooks.md) for registration, permission, timeout, and failure
contracts. Hooks do not patch model options or replace completion verification.

Set `AgentBindings.components` to use the [adapter runtime](adapters.md) for all
selected catalog and adapter-export tools, hooks, and context sources. In that mode, direct
`tools`, `hooks`, and `context_sources` must be `None`. The Agent saves assembly metadata with
admission and opens scoped instances only after acquiring an execution lease.
Each segment owns its resources; Waiting closes them and resume opens a new
binding set using the original assembly. `handle.component_release(&context)`
reports local cleanup separately from the stored outcome.

Automatic [context sources](context-sources.md) collect read-only data once per Run
or logical model step. Saved batches survive retries and resume; current access
is checked before local Hooks receive the data and before each physical model
attempt. Set `context_token_estimator` when component mode selects sources.

## Register tools and separate their inputs

[Context rewriting](context-compaction.md) preserves original messages and stores a
separate session revision. A configured compressor is used only when bounded
selection and available previews cannot fit the input. Model-based compression
uses the same Run budget and an explicit Compaction routing rule.

Use [Skills](skills.md) to advertise fixed instruction listings and load complete
bodies through a registered Tool. [Artifacts](artifacts.md) hold original bytes
with bounded previews and source evidence. Both use current scoped authorization;
neither grants additional Tool permissions.

The Host implements `ToolExecutor` and registers it with a compiled
`ToolDescriptor`. `AgentProfile.tools` selects the exact catalog tool ID and
version; only selected tools are offered to the model. The following function adds
one tool to a profile with no existing tool selections and `components: None`. All other Host bindings,
including the system-input registry and current authorization policy, must already
be configured.

```rust
use std::sync::Arc;
use wickle::*;

pub fn with_catalog_tool(
    mut profile: AgentProfile,
    mut bindings: AgentBindings,
    descriptor: ToolDescriptor,
    executor: Arc<dyn ToolExecutor>,
) -> Result<Agent, ContractError> {
    let compiled = SchemaCompiler::new().compile(descriptor, &bindings.system_inputs)?;
    let tool = compiled.descriptor().tool.clone();
    profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
        tool_id: tool.id,
        version: tool.version,
        bindings: None,
        config: None,
    }));
    bindings.tools = Some(Arc::new(ToolRegistry::new(
        bindings.scope.clone(),
        vec![ToolRegistration { compiled, executor }],
    )?));
    create_agent(profile, bindings)
}
```

For a tool requiring `query`, `limit`, and `workspace_id`, set
`descriptor.agent_parameters` to `["query", "limit"]`. Register `workspace_id`
in `bindings.system_inputs` with its schema and `SystemInputSource::Run {}`;
supply its authenticated value through `ExecutionContextData.system_inputs` as
`Some(SystemInputs::new(values))`. The model supplies `query` and `limit`, while
the binder supplies `workspace_id`. The executor receives the final approved
`JsonObject`. A missing optional model parameter can receive its declared
top-level default; the binder never invents a missing system identifier.

System fields are absent from the model's input schema. A model-supplied hidden
field is rejected even when its value happens to match the Host's value. Only
bindings selected for that tool reach the executor, not the whole system-input map. For
resolver-owned keys, supply `bindings.system_input_resolver`; those values cannot
be overridden by the Run input map. See [tool schemas](tool-inputs.md) and
[input binding](input-binding.md) for aliases, defaults, and resolver contracts.

`ToolExecutor::execute` receives the arguments and a `ToolExecutionContext`
containing scope, Run, principal, call/attempt IDs, an idempotency key, cancellation,
and a deadline. Adapter exports also receive their current binding-set identity.
Credentials stay in the Host's executor instance. The executor
must perform one physical attempt without hidden retries or detached work.

## Execute a saved tool round

The driver saves the complete model tool plan, then processes calls serially in
their original order. Before entering an executor, it validates the model inputs,
binds and saves system inputs, checks current policy, reserves tool budget, and
saves dispatch identity. It checks current policy again immediately before the
callback. Validation, policy, or prerequisite storage failures cannot start the
executor. UUID format validation does not establish ownership: the Host policy
must check the actual target and the caller's permission.

Results, receipts, paired messages, and events are committed together. The next
model step sees the original model arguments and bounded, validated observations,
including safe status and effect fields. Bound system inputs, raw receipts, and
raw diagnostic payloads are not added to model context. Unknown tool names,
invalid arguments, and denied calls become error observations so the model can
respond or propose a corrected call within the Run's limits.

`ToolExecutionResult.effect` is independent of result validation:

| Effect | Meaning |
| --- | --- |
| `NotApplied` | No external business write occurred; a read-only result uses this value |
| `Applied` | A write is confirmed; return its receipt even if the output later fails schema validation |
| `Unknown` | A write may have occurred, but its outcome is not established |

A confirmed write with invalid output remains `Applied` with a failed observation
and its protected receipt. The same settled call is not executed again. If an
entered write times out, is cancelled, panics, or fails without a conclusive
result, its effect remains `Unknown`; stopping a Future does not prove a remote
write was rolled back. Oversized receipts retain an omission marker and digest
rather than claiming the full receipt was saved.

Approval of a fixed tool binding and unknown effects stop later tools and model calls. The
Agent saves a `Waiting` outcome with the fixed approval target or unresolved
effect reference. If approval becomes necessary after dispatch was reserved but
before the executor entered, `ApprovalPending` preserves the bound inputs,
attempt, idempotency key, and charged reservation. Cancellation or deadline
exhaustion instead produces its own outcome while retaining unresolved effects
and settling unstarted calls as `NotApplied`.

Approval required to read a system-input resolver is a separate, unsupported
binding operation: it produces a binding failure observation without entering
the tool executor or constructing an approval candidate from unresolved values.

Use `AgentSettings.tool_execution_limits` for the per-attempt timeout and receipt
bound. Profile limits still bound total model calls, tool attempts, and elapsed
time. Tool execution is serial; declaring tool metadata does not enable parallel
dispatch or automatic retry. There is no automatic replay after an unknown
effect. A saved tool wait is distinct from `Guarded::ApprovalRequired`, which
reports a policy challenge on a facade operation such as starting, resuming, or
observing a Run.

## Resume a saved wait

`agent.resume(command, context)` continues the same Run from its saved tool
cursor and returns a new `RunHandle`. It does not repeat completed tools or ask
the model to recreate its plan. Waiting ends the active driver segment and
consumes no additional model or tool attempts; time spent waiting still counts
toward the Run deadline.

| Saved wait | Command and result |
| --- | --- |
| Tool approval | `ResumeAction::Approve` permits current policy to consider the recorded approval; `Deny` records a denied, `NotApplied` result for that call |
| Tool input | `ResumeAction::Input` validates the answer against the saved tool's output schema and completes the original call without reentering its executor |
| Uncertain external effect | `ResumeAction::External` supplies a protected receipt reference for Host verification; a confirmed result settles the original call without executing it again |

Create a command once from the protected saved state and keep the same command
for retries. This helper constructs a tool approval; obtain its snapshot through
`get_run_details` with current authorization.

```rust
use wickle::*;

pub fn approval_command(
    snapshot: &RunSnapshot,
    command_id: Id,
) -> Result<ResumeCommand, ContractError> {
    let wait = snapshot.wait.as_ref().ok_or_else(||
        ContractError::new(ErrorCode::InvalidTransition, "wait"))?;
    let WaitTarget::Approval { target: target @ ApprovalTarget::Tool { .. } } =
        &wait.target else {
            return Err(ContractError::new(ErrorCode::InvalidTransition, "wait.target"));
        };
    Ok(ResumeCommand {
        run_id: snapshot.run_id.clone(),
        expected_revision: snapshot.revision,
        command_id,
        action: ResumeAction::Approve {
            wait_id: wait.wait_id.clone(),
            target: target.clone(),
        },
    })
}
```

Pass that command to `agent.resume(command.clone(), reviewer_context).await`.
The command ID, decision, expected revision, wait ID, scope, and binding target
identify one acceptance. Repeating an accepted command returns its saved
acceptance even after the Run finishes; changing that command under the same ID
is a conflict. A new command cannot revive a terminal Run. Command consumption,
any answer or effect correction, and `run.resumed` are committed atomically.
Current permissions are checked on new commands and replays.

On resume, omitted `system_inputs` reuses the original Run snapshot. An explicit
map, including an empty map, must match it. The reviewer is recorded as the
command submitter; subsequent execution keeps the original principal and grant. Existing bound
inputs retain their resolver values and source revisions. Profile, prompt,
compiled tools, and routing must match the pinned runtime configuration; current
metadata is not silently substituted.

The Host's `PolicyPort` receives the full command in `PolicyAction::ResumeRun`.
When execution reaches an approved tool, `ToolPolicyInput::approval()` exposes
the recorded command, reviewer, and grant as evidence. Policy must explicitly
allow the operation under current permissions. Approval never overrides a
current denial.

### Ask for input or verify an external result

A tool can return `ToolExecutionOutcome::InputRequired { question }` with
`effect: ToolEffect::NotApplied` and no receipt. The question must be nonempty
and bounded. This creates `InputPending` and an input wait. The answer uses the
tool's pinned `output_schema`; dynamic answer schemas are not supported.
Before expiry, invalid answers leave the command unconsumed and the wait intact. The answer
becomes a tool observation, not a replacement for the original user request or
system instructions.

For an external wait, configure `AgentBindings.external_receipt_verifier`.
`ExternalReceiptVerifier::verify` receives the original call, attempt and
idempotency key, frozen `BoundToolInput`, protected receipt, and current scope,
actor, cancellation, and deadline. The Agent checks receipt read permission
before supplying it. The verifier must authenticate the evidence and target;
record existence or caller-provided IDs alone do not establish a business effect.
It must inspect the prior operation without retrying it.

A verified `Applied` or `NotApplied` result settles that original call. Even an
`Applied` result with invalid output keeps its effect and receipt as a failed
observation. `NotApplied` does not request an automatic retry. A verdict that is
still `Unknown` returns `ToolEffectUnresolved` without consuming the command.
The transcript retains the original unknown observation and appends a linked
correction; subsequent model context uses the corrected observation once.

### Observe execution segments

An old handle keeps the Waiting outcome and event endpoint of its segment after
a successful resume. Use the new handle for the continued segment; the Run ID is
unchanged and event sequence numbers continue increasing. `get_run` and
`get_run_details` inspect the current Run. Dropping a polled resume Future or a
resumed handle does not abandon an accepted command or cancel its driver.

An expired wait or Run deadline produces `Exhausted` without new dispatch and
retains already recorded effects. Expiry is recorded at command acceptance;
an on-time acceptance does not expire merely because its previous wait's
deadline later passes. Expired approvals supply no tool authorization evidence.
Durable storage allows a saved wait to resume
after the Host recreates compatible bindings. Interrupted Running runs use `ResumeAction::Recover` with an exact checkpoint reference and a new lease. See [recovery](recovery.md) for effect reconciliation and storage-failure diagnostics. Candidate approval via
`ApprovalTarget::Candidate` uses the [verification runtime](verification.md) and
the exact saved candidate, verifier, wait identity, and revision.

## Start, replay, and observe

`start` checks current admission permission and looks up the request under its
scope, session, and request ID before resolving current metadata. An identical
request returns the original Run. Changed input, model options, or effective
system values conflicts with the stored request. For `start`, omitted system
inputs mean an empty map. Atomic admission decides concurrent requests; only the
newly created Run starts a driver.

The driver owns its lease, heartbeat, cancellation signal, and state transitions.
Dropping the start Future after it has been polled, a RunHandle, an outcome Future,
or an event Stream does not cancel accepted execution. Observer context
cancellation stops observation separately from the run's execution token.

`get_run` returns an authorized `RunView`. `get_run_details` requires the separate
details permission and returns the protected checkpoint. `RunHandle.outcome`
returns a `Guarded<RunOutcome>` read from the store; a text delta or finish
notification is not completion authority. If completion cannot be saved, the
observer receives an execution/storage error rather than a fabricated success.

`RunHandle.events(after_seq, context)` returns durable `EventView` metadata after
the cursor. It rechecks current permission while yielding and does not publish
protected record references. A slow or disconnected subscriber cannot restart or
block the driver. Reconnecting replays the remaining committed sequence.

## Cancel and finish

`handle.cancel(reason, &context)` checks `CancelRun` permission and returns a
`Guarded<CancelReceipt>`:

The receipt contains `run_id`, `command_id` and an optional
`processed_segment_id`. Absence of a segment ID means the durable command is
pending, not completed. Read it with `get_control_receipt`; use
`submit_control_command` to provide a stable command ID for retries.

A saved Waiting or Interrupted Run can be cancelled even when no local driver
exists. A control-only interval atomically acquires ownership and saves the
cancellation. Unstarted calls become `NotApplied`; existing `Applied` and
`Unknown` effects are retained. The old handle keeps its original interval's
outcome. Read the latest Run or command receipt to observe cancellation.
Remote Worker notification remains Host-owned. See [durable controls](run-controls.md).

Success records `completion_basis=turn_ended` or `verified`, according to the
profile. A valid format alone completes `turn_end`; `verified` also requires an
accepted verdict with pinned criteria and evidence. Failure, cancellation, and budget exhaustion have distinct outcomes.
Already stored response text can be retained as partial output without becoming
a successful assistant transcript. Unfinished stream deltas are not guaranteed
to survive an interrupted collector.

Opaque continuation returned by a successful model is stored in protected records
with the assistant transcript. A later Run can reuse it only under the exact
matching route. Changing providers or route identity fails before transmitting
foreign continuation. The original session prompt remains pinned.

Replaying `start` retrieves the original Run; it does not consume a saved wait or
restart an interrupted running worker. Use a matching `ResumeCommand` for the
supported waits described above.

## Replaying a submitted request

Construction checks profile structure, finite runtime settings and binding scopes.
It does not require the current Tool, context-strategy or verifier registry to
resolve every profile reference. New-request admission performs those checks.
This allows a facade to retrieve an already accepted request after runtime
registrations have changed or become unavailable.

Start checks current authorization and scope before looking up the request key.
An existing request is compared against its saved submitted-input snapshot and
normalization version, without current registry resolution or another model call.
New request limits and current verifier planning apply only when the key is new.
A changed submitted payload is a conflict; an existing key never bypasses revoked
permission. Atomic admission repeats the identity comparison against the winning
stored snapshot when concurrent submissions race.

The submitted snapshot records caller options and Host-provided system inputs,
not newly resolved catalog defaults. Current effective configuration remains in
the separate execution snapshot. Older records without submitted-input evidence
use their historical comparison path rather than fabricated new metadata.

## Saved execution configuration

The selected model port compiles exposed Tool schemas into a saved
[provider contract](provider-tool-schemas.md). The core restores proposed
arguments before defaults, validation and system binding. Binding, Profile and
Run options follow the [documented precedence](model-routing.md); auxiliary
purposes keep separate configuration. Use [saved-step inspection](step-inspection.md)
to read that evidence without repeating execution.

An [interruption policy](interruption-policy.md) can attach validated application
state to a recoverable stop. Use [durable controls](run-controls.md) for command
receipts, interval-specific outcomes and read-only deadline observation.
