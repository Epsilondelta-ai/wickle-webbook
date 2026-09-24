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
record. Agent-purpose options must match `RunRequest.model_options`; purpose-
specific helper settings remain Host inputs and are fixed for that logical step.
Supply an empty `previous_route` and `previous_failure`: this entry point reads
the previous physical attempt from the saved ledger.

The projector must return the exact selected route, step, purpose, options, and
output limit. Final request byte/protocol checks and the returned route-specific
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
original projected-request identity and current permission. It makes no new model
call. A changed routing snapshot or changed step options fails rather than
silently upgrading the resumed run.

This is the model execution boundary. The agent driver owns transcript advancement,
Tool execution, user waits, and the broader task loop. Provider-specific SDK
adapters and live deployment probes are supplied separately.

The [standalone consumer](../tests/support/routing_consumer.rs) uses synthetic
adapters and metadata observations to exercise fallback, accounting, and completed
step reuse after checkpoint restoration. It does not verify a live provider.
