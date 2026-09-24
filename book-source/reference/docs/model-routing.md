# Route and dispatch model calls

Wickle separates model selection, adapter lookup, and execution. A router selects
an exact route from a pinned catalog and policy. A dispatcher finds the existing
adapter for that route. `ModelExchange` owns authorization, attempts, retries,
fallback budgets, response validation, and persisted invocation records.

| Component | Responsibility |
| --- | --- |
| `RoutingSnapshot` | Own a validated catalog and policy under one scope and immutable digest |
| `FixedModelRouter` | Apply the same contract to one target without fallback |
| `PolicyModelRouter` | Select explicit targets by logical binding, purpose, capabilities, options, and limits |
| `RegistryModelDispatcher` | Resolve exact scope, provider, adapter version, and connection revision |
| `ModelRequestProjector` | Prepare required context and a final input-token estimate for the selected route |
| `ModelRouteInspector` | Check current availability and model/deployment metadata under bounded authorization |
| `ModelExchange` | Execute each physical call and record its outcome under the Run's shared budget |

The router and dispatcher implementations live in `wickle-model-router`. Their
public contracts live in `wickle`. Neither component requires another process or
reads `.env` files. The Host constructs and supplies the components.

## Pin a routing policy

Each `RoutingRule` names a logical profile binding and one purpose: agent,
verification, or compaction. It contains an exact primary binding and a finite
ordered fallback list. Those bindings fix the permitted provider, connection,
API, deployment, and region metadata. The request's `allowed_bindings` can narrow
that set; it cannot add a destination.

Rules default to `RequirePinned` and `ContractTested`. A request may tighten the
version requirement. Mutable targets require permission from both the rule and
the request. A `Planned` support minimum can be stored for preparation but cannot
select a runtime route. Missing rules or unsupported primary targets fail
explicitly. Only a policy-permitted failure allows progression to later targets.

`RoutingSnapshot::restore` requires the trusted scope and expected digest. A
`RouteSelection` returned by a custom router is checked against the full snapshot,
request, candidate order, and capability contracts before use. Editable selection
DTOs do not grant permission to call a provider.

## Execute a routed step

Use `ModelExchange::with_dispatcher`, configure a bounded inspector with
`with_route_inspector`, and call `generate_routed` with a router,
`RoutedModelInput`, projector, execution context, and `RunBudget`. The existing
`ModelExchange::new` remains available for an explicitly supplied single adapter.

The routed entry point pins `RunSnapshot.routing_snapshot_ref` before the first
physical call. It also stores the logical step input as an immutable protected
record with its original transcript boundary. The final
[prepared model step](prepared-model-steps.md) is committed before dispatch.
Agent-purpose logical options must match the top-level merge of
`AgentProfile.model_options` and `RunRequest.model_options`. Purpose-specific helper
settings remain Host inputs and are fixed for that logical step.
Supply an empty `previous_route` and `previous_failure`: this entry point reads
the previous physical attempt from the saved ledger.

The projector must return the exact selected route, step, and purpose, using
`ModelProjectionContext.configuration.effective` for options and
`configuration.max_output_tokens` for the output limit. Final request byte/protocol checks and the returned route-specific
token estimate are validated before sending. Required context must be preserved.
The core derives `text`, `tool_calling`, and `json_output` requirements from the
prepared request so omitted Host feature declarations cannot bypass these checks.
The Host projector is responsible for preserving meaning and required context;
the core checks metadata, protocol, options, and size, rather than proving semantic equivalence.
Opaque provider continuation can only be used with its exact original route;
cross-provider projection must use valid transferable context or fail.

Current policy receives the full route metadata, including its target and
connection reference, before projection or inspection. It is checked again before
dispatch. The inspector receives the current principal, scope, grant, cancellation
signal, and finite deadline. It must perform read-only metadata lookup, without
inference or hidden retries.

An inspection cannot replace the pinned route. Known differences return a drift
error; unknown availability fails explicitly. A pinned requirement needs observed
model ID/version and any required deployment revision. Inspection evidence is
stored separately from provider-reported response metadata. Neither an inspection
nor a pinned ID guarantees that a provider cannot change after the check.
Routed invocations require an inspection record. The store checks its exact route
and required version guarantee during commit and checkpoint restoration.

## Retry, fallback, and recovery

Same-route retries retain the request options and obtain a fresh physical attempt
ID, reservation, inspection, and policy check. Fallback moves forward through the
explicit candidate list, builds a fresh projection, and charges the same Run's
recovery budget. Every actual call also charges the model-call budget.

A complete response ends the model step; tool proposals are still unexecuted
until the driver commits and executes their plan. Outstanding tools, including
settled results with unknown external effects, block routed generation. Unresolved
model attempts require explicit recovery and are never silently resent.

Repeating a completed logical step reuses its saved response after checking the
stored projected-request identity and current permission. The projector and
schema compiler are not called again, and no new model call is made. A changed routing snapshot or changed step options fails rather than
silently upgrading the resumed run.

This is the model execution boundary. The agent driver owns transcript advancement,
Tool execution, user waits, and the broader task loop. Provider-specific SDK
adapters and live deployment probes are supplied separately.

The [standalone consumer](../tests/support/routing_consumer.rs) uses synthetic
adapters and metadata observations to exercise fallback, accounting, and completed
step reuse after checkpoint restoration. It does not verify a live provider.

## Inference options and output caps

The selected binding's `default_options` are overridden by Profile options, then
Run options. Each top-level key replaces its entire value, including objects;
there is no recursive merge. Both model and binding schemas validate the result.
Unknown or unsupported values fail rather than being dropped or coerced.
Transport settings, credentials, retries, and provider output-limit aliases cannot
be supplied through this map.

For example, a binding default `{"reasoning":{"effort":"low","summary":true}}`
and a Run override `{"reasoning":{"effort":"high"}}` produce
`{"reasoning":{"effort":"high"}}`. The summary flag is not inherited.

Every routed physical invocation stores a `ModelConfiguration` with the requested
and effective maps, per-key source, both schema revisions, and output cap. Retry
keeps this configuration; fallback recomputes it against the pinned destination's
defaults and schemas. Historical or direct, unrouted invocations can have no
configuration evidence. Do not infer evidence for them.

Agent output is bounded by the minimum of `AgentSettings.max_output_tokens`,
optional Profile `limits.max_output_tokens`, optional Run `max_output_tokens`, and
the selected model/binding capabilities. Verification and compaction use their
own purpose-specific options and output budgets, still bounded by Host/Profile/Run
output caps; they never inherit the agent's inference map.
All built-in HTTP adapters disable automatic transport retries. The core reserves
a fresh physical attempt for every retry and charges the shared Run budgets.
