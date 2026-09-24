# Add lifecycle hooks

Hooks add bounded context, transform model-owned tool arguments, or observe
committed results. They run inside the library at fixed lifecycle positions.
The Host creates their handlers and selects exact versions in the Agent Profile.

| Position | Input and permitted output | Execution boundary |
| --- | --- | --- |
| `BeforeRun` | Original user content and accumulated context; append context additions | Once after admission; reuse the saved result in later steps and segments |
| `BeforeModel` | User content and accumulated context; append context additions | Once per logical model step, including its physical retries and fallbacks |
| `BeforeTool` | Model-visible schema, original arguments, and current transformed arguments; replace model arguments or deny the call | After initial argument validation, before system input binding |
| `AfterTool` | Safe committed tool status, effect, content, and classified error | After the tool result and event are committed |
| `AfterRun` | Safe terminal status, output, and usage | After the terminal outcome is committed; never for a Waiting outcome |

`BeforeRun` and `BeforeModel` add context only. They cannot change model options,
routing, messages, or system instructions. `BeforeTool` may return a denial but
cannot request approval or grant permission. Current Host policy still decides
whether a tool may execute.

## Register a handler

The following handler appends a suffix to a model-owned `query` parameter. Use it
with tool schemas that expose a string `query` in `agent_parameters`.

```rust
use std::sync::Arc;
use wickle::*;

pub struct QuerySuffix;

impl HookHandler for QuerySuffix {
    fn call<'a>(
        &'a self,
        input: &'a HookInput,
        _: &'a HookContext,
    ) -> PortFuture<'a, HookOutput> {
        Box::pin(async move {
            let HookInput::BeforeTool { model_inputs, .. } = input else {
                return Err(ContractError::new(ErrorCode::InvalidContract, "hook.position"));
            };
            let query = model_inputs.get("query").and_then(|value| value.as_str())
                .ok_or_else(|| ContractError::new(ErrorCode::InvalidArguments, "query"))?;
            let mut transformed = model_inputs.clone();
            transformed.insert("query".into(), format!("{query} published").into());
            Ok(HookOutput::Tool { model_inputs: transformed, deny: None })
        })
    }
}

pub fn with_query_hook(
    mut profile: AgentProfile,
    mut bindings: AgentBindings,
) -> Result<Agent, ContractError> {
    let hook = VersionedRef {
        id: Id::new("query-suffix")?,
        version: Id::new("1")?,
    };
    profile.hooks.get_or_insert_with(Vec::new).push(HookRef::Catalog(CatalogHookRef {
        hook_id: hook.id.clone(),
        version: hook.version.clone(),
        position: HookPosition::BeforeTool,
    }));
    let registry = Arc::new(HookRegistry::new(bindings.scope.clone(), vec![HookRegistration {
        definition: HookDefinition {
            hook,
            position: HookPosition::BeforeTool,
            priority: 0,
            required: true,
            timeout_ms: 100,
            max_output_bytes: 4096,
        },
        handler: Arc::new(QuerySuffix),
    }])?);
    bindings.hooks = Some(Arc::new(HookRuntime::new(
        bindings.state.clone(),
        bindings.policy.clone(),
        bindings.clock.clone(),
        bindings.ids.clone(),
        registry,
    )));
    create_agent(profile, bindings)
}
```

This helper assumes no existing hook selections and `components: None`. Supply a `ProfileResolver` whose
metadata registers the same hook ID, version, and position. Use the Agent's state,
scope, policy, clock, and ID source when constructing its `HookRuntime`.

`HookRegistry` rejects duplicate IDs and unbounded definitions. Selected hooks
run serially: lower `priority` first, then ID to break ties. Each hook receives
the preceding hook's accepted output. An optional hook is not an optional
registration: its selected version must still exist and match the pinned plan.
Adapter-export hooks are supplied through the [adapter runtime](adapters.md).
Their original export selections and segment identities remain separate from
the hook definition; they use the same transformation and observation contracts.

## Preserve input and context boundaries

`HookInput::BeforeTool` separates `original_model_inputs` from the current
`model_inputs`. The core validates every replacement against the model-visible
schema. Adding a hidden system parameter fails even when its value matches the
Host's actual value. System fields are supplied later by the input binder;
ordinary hooks do not receive the whole system-input map or credentials.

The bound record retains original, effective, and normalized model inputs,
the transformation reference, and final execution arguments. The executor gets
the validated effective values combined with its selected system bindings.
The transcript retains the original model call. A denial stops the transform
chain and becomes a `Denied`/`NotApplied` tool observation; another hook's output
cannot remove it. `PolicyAction::InvokeHook` authorizes callbacks, and final
tool policy remains authoritative after transformation and binding.

Context hooks return `HookOutput::Context` containing `HookContextAddition`
values. Additions contain only text or JSON plus a priority. The core assigns
the item ID, digest, exact hook source, scope, and `Hook` origin. Before-run data
has Run lifetime; before-model data has logical-step lifetime. It enters model
context as data, preserving Host/Profile instructions and the original request.
Required additions must fit within the context budget; they grant no extra
authority.

Before-model Hooks receive the currently authorized ContextSource items in
profile order followed by saved before-run Hook additions. Before-run Hooks keep
their original input and Hook-chain contract. Source access is checked again
before each physical model attempt; revocation also blocks Hook-derived copies.

## Reuse stored transformations

Each accepted transformation is saved before it can affect a model or tool
call. The Hook plan fixes versions, order, timeouts, and output bounds for the
Run. A physical model retry reuses its logical step's stored context, and an
approved tool resumes with its stored transformation and `BoundToolInput`.
It does not rerun the hook or resolver to choose another target.

If a callback finished but its result was never committed, it may be called
again. If the commit succeeded but acknowledgement was lost, the stored
application can be reused without applying a transformation twice. These
callbacks must not hide business writes. Use tools for required effects and
Host-managed delivery for notifications that need retries.

## Handle failures and observe results

Only an optional `BeforeRun` callback failure can be stored as a warning and
allow execution to continue. Required callback failures stop execution; failures
at other transformation positions stop execution even if `required` is false.
Malformed output, schema violations, and output size violations always fail
closed. Permission denial and timeout never become permission to execute.

Transformations use the shorter of their definition timeout and the remaining
Run time. An `AfterTool` callback also respects remaining Run time; if it has
already expired, no callback starts. `AfterRun` has fresh cleanup time even after
cancellation or exhaustion, capped by its definition timeout and 30 seconds.
Observers cannot delay, reverse, or replace the already committed outcome.

Successful observations and classified observer failures are stored separately
from the Run snapshot. They do not advance its revision or event endpoint.
`handle.hook_observations(&context).await` returns a guarded
`HookObservationView` containing durable `reports` and any known
`local_error` from report persistence. A callback error appears in a failed
report; a storage error may leave no report and appears separately from the
unchanged Run outcome. Poll reports separately when the outcome becomes available
before its final observer finishes.

Stored reports suppress repeated delivery of the same observation. This does
not guarantee exactly-once callbacks after a crash or retry delivery whose
report was never stored. A local report error is not durable across process
restart. Hook handlers are trusted in-process code; isolate blocking or untrusted
code in the Host rather than treating Rust types as a sandbox.

The [independent Hook consumer](../tests/support/hooks_consumer.rs) exercises
context additions, argument transformation, result observations, and SQLite
reopening with synthetic Host ports. It makes no provider network calls. Run it
with the other extracted-package examples using
`python3 scripts/check-package.py --allow-dirty`.
