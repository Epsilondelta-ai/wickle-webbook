# Bind adapter exports to an agent

`wickle-adapter-runtime` assembles approved Tool, lifecycle Hook, and ContextSource implementations
for one Agent execution segment. Core traits and data contracts live in `wickle`;
the Host runtime is a separate library crate. No server or network protocol is
required between them.

The Host registers code and connection references. A profile selects exact
adapter versions and exports; it cannot supply an executable path or credentials.

## Choose a binding mode

| Agent configuration | Source of tools, hooks, and context sources |
| --- | --- |
| `components: None` | Existing `AgentBindings.tools`, `hooks`, and `context_sources`, managed by the Host |
| `components: Some(runtime)` | All selected catalog entries and adapter exports from that runtime |

The component mode requires `AgentBindings.tools`, `hooks`, and `context_sources` to be `None`.
Register existing catalog implementations through `CatalogToolRegistration` and
`CatalogHookRegistration` when mixing them with adapter exports. Add catalog sources
with `AdapterRegistry::with_sources` and `CatalogSourceRegistration`, and supply
`AgentBindings.context_token_estimator` when selecting sources. Duplicate visible
names or conflicting selections are errors; later registrations do not overwrite
earlier ones.

## Register and select

`AdapterRegistry::new` accepts the exact scope, adapter definitions with factories,
connection registrations, catalog tools, catalog hooks, and optional existing
binding-state snapshots. Registration validates metadata without calling a factory.
The application's `ProfileResolver` can use `registry.component_metadata` for
registered adapters, connectors, tools, hooks, and sources, and its existing resolver for
model bindings.

Configure the runtime with the same scoped state, policy, and clock used by the
Agent. The supplied bindings below must have direct `tools`, `hooks`, and
`context_sources` set to `None`:

```rust
use std::sync::Arc;
use wickle::*;
use wickle_adapter_runtime::{AdapterRegistry, AdapterRuntime};

pub fn with_components(
    profile: AgentProfile,
    mut bindings: AgentBindings,
    registry: Arc<AdapterRegistry>,
) -> Result<Agent, ContractError> {
    bindings.components = Some(Arc::new(AdapterRuntime::new(
        registry,
        bindings.state.clone(),
        bindings.policy.clone(),
        bindings.clock.clone(),
    )));
    create_agent(profile, bindings)
}
```

Here is a profile selection fragment for a registered `records` adapter:

```json
{
  "connectors": [
    {"binding_id":"data","connector_id":"records-service","version":"1"}
  ],
  "adapters": [
    {"binding_id":"records","adapter_id":"records-adapter","version":"1",
     "connections":{"main":"data"}}
  ],
  "tools": [
    {"adapter_binding":"records","export_id":"search","alias":"search_records"}
  ],
  "hooks": [
    {"adapter_binding":"records","export_id":"observe"}
  ]
}
```

An alias changes the model-facing name. The original adapter binding and export
identity remain in the assembly and policy input. The same adapter code can serve
multiple connection bindings with distinct selections and visible names.

## Resolve before opening

For a new request, the Agent resolves metadata and saves a `ResolvedAssembly`
alongside its prompt, hook/source plans, and system-input definitions. The assembly fixes
the full export contracts, compiler digests, connection revisions, and selected
Host state. Duplicate requests reuse saved state before resolving current metadata.

Only after admission and lease acquisition does `ComponentRuntime::bind` open
resources. Each `AdapterFactory::open` receives `AdapterInitContext` with the
scope, Run, fresh `binding_set_id`, current actor, exact binding, selected exports,
and finite cancellation/deadline controls. Return an `AdapterInstance` exposing
exactly those requested Tool, Hook, or ContextSource exports.

Instances are staged privately. Their returned descriptors, hook and source contracts
must match the saved assembly before any executable registry becomes available.
The Agent also checks a custom runtime's returned contracts. Current
`BindAdapter`, Tool, Hook, source collection/use, and data-access policies remain separate checks;
registration or a binding-set identifier does not grant permission.

## Preserve state across execution segments

Use `AdapterBindingState` for an immutable mapping the Host prepared earlier,
such as an existing external thread reference. It is bound to the scope, session,
adapter selection, definition digest, and protected value reference. Factories
receive that saved snapshot; opening does not create a missing business object or
refresh the mapping implicitly. Required creation or updates belong in explicit
Tools or Host management operations.

Waiting ends the current segment and closes its instances. An authorized resume
uses the original assembly and bound Tool arguments with a new binding set and
the current reviewer. Saved results and input bindings are reused. Duplicate
resume commands open no new resources. Input/receipt validation uses metadata
before accepting the command, so invalid commands need no business-tool instance.

Expired resumes and cancellation of saved waits finalize from metadata. When
configured observers are needed afterward, `ObserversOnly` activates only those
observers; it does not activate context sources. A failed execution bind is not
retried as an observer bind.

## Release explicitly

After committed result observations, including terminal `after_run` hooks, the
Agent invalidates the segment and awaits close in reverse initialization order.
Waiting, cancellation, execution errors, and partial initialization failure also
release owned resources. Retained export references reject calls after release
or with a different scope, Run, or binding set.

`AdapterRuntimeSettings` bounds each open and close; Agent admission/bind and
total cleanup deadlines provide additional limits. One close failure does not
skip remaining cleanup. Factories own cleanup of resources allocated before they
return an instance. Custom `ComponentRuntime::bind` implementations must retain
staging and rollback ownership when their caller cancels or drops the Future.
`Drop` invalidates access but does not prove asynchronous cleanup completed.

`handle.component_release(&context).await` returns local cleanup information
under current details permission. A saved outcome can be visible before cleanup
finishes, so its report may initially be absent. Cleanup failures remain separate
from the saved Run outcome. Failed-initialization rollback reports are available
from `AdapterRuntime::take_failed_cleanup_report` in the owning Host process.

The [adapter consumer](../tests/support/adapter_consumer.rs) exercises the public
crate contracts with synthetic factories. [Context-source exports](context-sources.md)
support read-only collection and fresh authorization of saved batches. Event-consumer
exports remain metadata-only; this runtime does not deliver events. Close callbacks must not hide required business
writes, and in-process Rust callbacks are trusted code rather than a sandbox.

The [Skill runtime](skills.md) is supplied by the Host. Its loader participates in
this registry as a catalog Tool or a Tool export, including the normal selection,
policy and segment-lifetime checks. Resolve Skill metadata through the Skill
runtime in the Host's `ProfileResolver`.
