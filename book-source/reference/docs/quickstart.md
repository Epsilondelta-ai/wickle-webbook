# Start with a runnable Host

Wickle runs inside your Rust process. Your application supplies authentication,
model connections, storage and tools. The examples below use synthetic model
ports and temporary SQLite files, so you can learn the lifecycle without model
credentials or paid requests.

## Run the packaged example

Install Rust 1.85 or later, rustup and Python 3.9 or later, then use a source
checkout:

```sh
git clone https://github.com/Epsilondelta-ai/wickle.git
cd wickle
python3 scripts/check-package.py --consumer agent
```

Cargo may download build dependencies on the first run. The script packages the
libraries, extracts them into a separate temporary workspace, verifies base
contracts and runs the selected consumer. It removes that temporary workspace
when finished. Add `--allow-dirty` only when intentionally testing local edits.

The [complete agent example](../tests/support/agent_consumer.rs) demonstrates:

1. A scoped catalog, policy, SQLite store and two model ports.
2. Binding defaults overridden by Profile options and then Run options.
3. Model fallback under shared budgets, saved output and event replay.
4. Duplicate Start without another model call.
5. A custom interruption policy that records application-owned maintenance data.
6. Explicit recovery into a new execution segment, while the old handle keeps
   its interrupted outcome.
7. A saved-step inspection with connection details redacted and no extra model
   calls or storage writes.

For approvals and system-bound Tool inputs, run another complete example:

```sh
python3 scripts/check-package.py --consumer resume
python3 scripts/check-package.py --consumer tool_schema
```

The [resume example](../tests/support/resume_consumer.rs) persists an approval
wait, reopens SQLite with fresh Host instances, authenticates a reviewer and
finishes the original call without repeating its effect. The
[schema example](../tests/support/tool_schema_consumer.rs) separates model and
system inputs, restores native and OpenAI strict provider contracts, and preserves
omission before applying defaults.
These examples are illustrative Host implementations, not production
credential or authorization policies.

Omit `--consumer` to run the complete package and business-consumer suite. The
selector does not change the default CI coverage.

## Configure your application

Install the core and required adapters from the same release tag using the
[installation guide](installation.md). Then configure these boundaries:

| Boundary | Your application supplies |
| --- | --- |
| Profile | Instructions, selected component references, completion mode and limits |
| Model | A catalog, explicit routing rules, a scoped adapter and authoritative inspection evidence |
| State | `MemoryStateStore` for ephemeral work or a durable `StateStore` such as SQLite |
| Authorization | A `PolicyGate` that evaluates current authenticated authority |
| Tools | Reviewed schemas, executors and explicit model-owned parameter names |
| System inputs | Trusted identifiers and registered schemas, supplied through execution context |
| Extensions | Optional sources, Skills, hooks, interruption policy and artifact storage |

See [Agent bindings](agents.md) for the full constructor and
[provider configuration](model-providers.md) to replace the synthetic model.
The library does not read `.env`; configuration loading belongs to your Host.

## Follow the saved outcome

`Agent::start` returns `Guarded<RunHandle>`. Handle an approval challenge through
your application's authorization flow. A completed guard supplies a handle;
await `handle.outcome(&context)` to read that execution interval's saved result.

Use `Waiting` to present the stored approval or input request. Use `Interrupted`
for a recoverable stop and submit an explicit recovery command after checking the
saved evidence. A command ID is a retry identity: reuse the same ID and payload
for the same submission. An old handle remains tied to its original segment;
use the new handle or `get_run` for the latest state.

`Succeeded` with `turn_ended` means the model turn finished. Choose `verified`
completion when a business criterion must be checked before success. See
[verification](verification.md), [interruption policies](interruption-policy.md),
[durable controls](run-controls.md) and [saved-step inspection](step-inspection.md).

Applications upgrading an existing store should first read
[migrating to v0.2.0](migration-v0.2.md).
