# Data contracts and profile validation

Wickle provides profile validation, scoped state and policy contracts, and a
bounded model-call boundary. The [agent runtime](agents.md) connects these
contracts to Agent and RunHandle, including serial execution of registered tools.
Dynamic runtime component assembly is still being implemented.

`AgentProfile` contains data and registered references. A `ProfileResolver` is
Host code that supplies approved metadata for a scope. `ProfileValidator` checks
that metadata and configuration without opening an adapter or invoking a model.

## Decode a profile

```rust
use wickle::AgentProfile;

fn main() -> Result<(), wickle::ContractError> {
let profile = AgentProfile::from_json(r#"{
  "schema_version": "wickle.agent-profile.v1",
  "agent_id": "information-assistant",
  "version": "1.0.0",
  "name": "Information assistant",
  "description": "Organize information",
  "instructions": {"text": "Use available evidence."},
  "model_binding": "primary",
  "tools": [], "skills": [], "connectors": [],
  "context_policy": {"strategy": "bounded"},
  "output_contract": {"type": "text"},
  "limits": {
    "max_model_calls": 8, "max_tool_attempts": 0,
    "max_repair_attempts": 0, "max_recovery_attempts": 0,
    "max_elapsed_ms": 30000
  }
}"#)?;
println!("Validated {}", profile.agent_id);
Ok(())
}
```

`from_json` rejects duplicate keys, unknown fields and format versions, invalid
types, and invalid local binding references. Model-call and elapsed-time limits
must be positive integers. Zero tool, repair, or recovery attempts disables that
action. Completion defaults to `turn_end`; `verified` requires `verifier_ref`.

Optional adapter, source, hook, and extension collections preserve omission versus
an explicit empty collection. Explicit `null` is rejected. Context sources require
finite limits in the profile; a Host using presets must expand them before parsing.
The built-in context strategy is `bounded`. Custom strategies require an exact
versioned reference.

## Resolve approved metadata

Implement the async, dynamically dispatchable `ProfileResolver` trait in Host
code, then call `ProfileValidator::new(&resolver).validate(&profile, &scope).await`.
The resolver must return the requested kind, identifier, and exact version. Model
binding and extension-schema lookups resolve a Host-selected revision, which is
then retained in `ResolvedProfile`.

The validator checks:

- Selected components and their explicitly selected dependencies are available.
- Metadata/export contract versions are supported; their current version is `1`.
- Required connector mappings exist, export kinds match, and selected tool names
  do not collide. The Host supplies normalized descriptor names; provider-specific
  wire name validation belongs to the corresponding adapter.
- Required capabilities are supplied by selected components and exports. Merely
  declaring an unused export does not activate it or supply its capabilities.
- Configuration satisfies its registered Draft 2020-12 schema, including formats.
  Schema resolution is local: external `$ref`, dynamic/recursive references, and
  other declared dialects are rejected. Schema annotation data is not executed.
- Extension keys use dotted namespaces and have registered schemas.

Raw credentials, SDK clients, and executable module fields are not profile
properties. `config` and extension values are nonsecret data constrained by
Host-approved schemas. Validation is not a secret scanner for arbitrary text;
the Host must keep credentials in its connection bindings.

`ResolvedProfile` exposes immutable accessors and records the complete profile,
scope, exact component versions, and definition digests. Its deserializer checks
the saved digest consistency. Use `ensure_matches` to reject a different profile
or scope and `ensure_same_resolution` to reject changed component metadata.
These checks do not replace authentication or the authoritative store.

Tool schemas and system input definitions are described in
[Compile tool input schemas](tool-inputs.md) and
[Bind system tool inputs](input-binding.md).

## Model calls and response assembly

`ModelPort` is a `Send + Sync` trait whose `generate` method returns a boxed stream
for exactly one physical request. `ModelPortBinding` identifies the provider,
adapter version, and Host connection revision. Credentials remain in the adapter's
Host binding. `ModelRequest` contains an explicit model projection and finite
input/response limits; execution context and system tool inputs are not copied in.

`RunRequest.model_options` pins Host-authorized, request-specific options at
admission. `RouteRequest.options` carries them as candidate constraints, and
`ProjectionInput.options` becomes `ModelRequest.options` unchanged. Empty maps
remain omitted in saved JSON. Model options are included in request digests and
byte limits; same-route retries preserve them. See the [context guide](context.md)
for catalog validation and the explicit adapter-mapping boundary.

`collect_model_response` accepts complete text and tool proposals only after the
whole stream passes protocol validation. Missing completion, malformed or ambiguous
JSON, duplicate call IDs, contradictory finish reasons, and exceeded limits return
`ModelProtocolError`. Failed responses expose bounded partial text, with no partial
tool plan. Unknown tools and schema-invalid arguments remain explicit rejection
states in `ProposedToolCall`; these proposals are not authorized executions.

`OpaqueContinuation` is tied to the exact route digest, including model and
connection versions. A different route must receive a valid fresh projection.
A standalone collector needs a caller-owned timeout for a stream that stops yielding.

`ModelExchange` combines the port with `PolicyGate` and a run's `RunBudget`. It
persists each reservation and selected route before dispatch, rechecks permission
and cancellation, and stores the complete or failed response with its invocation.
Retries are disabled by default. Explicit finite retries share the run's model,
recovery, and elapsed-time limits; context overflow requires actual reprojection.

`ModelInvocationRecord.response_ref` points to a protected `StoredModelResponse`.
The store checks its attempt, route, outcome, and reported metadata against the
ledger. A later budget or cancellation error leaves already stored partial failures
available for authorized recovery. The agent driver must still commit the accepted
transcript and tool plans before executing any tool.

The [model consumer](../tests/support/model_consumer.rs) demonstrates two concrete
adapters used through the same trait, with connection and continuation isolation.

Session prompt reuse and bounded message projection are described in
[Pin and project model context](context.md).

Versioned model definitions, bindings, capability checks, and the separate
immutable catalog implementation are described in
[Register versioned models](model-catalog.md).

## Execution and storage data

The [SQLite state store](sqlite-state-store.md) provides a separate persistence
adapter with process-restart recovery and cross-process lease coordination.

| Contract | Purpose |
| --- | --- |
| `RunRequest`, `ResumeCommand` | Caller data with request/command identity and typed input or decisions |
| `ExecutionContextData`, `ExecutionContext` | Owned Host data and nonserializable runtime context with cancellation |
| `Message`, `ToolCall`, `ToolResult` | Transcript provenance and paired calls/results; tool model inputs remain separate from bound input references |
| `RouteRequest`, `ResolvedModelRoute`, `ModelInvocationRecord` | Model selection data, independent model/API/deployment/adapter versions, and physical-attempt provenance |
| `WaitState`, `RunOutcome` | Fixed approval/input/effect targets and explicit completion basis |
| `SessionSnapshot`, `RunSnapshot` | Pinned session identity and checkpoint data, without executing recovery |
| `RunEvent`, `EphemeralEvent` | Durable committed record references versus candidate deltas without durable sequence numbers |

Use `RunSnapshot::from_json`, or call `validate` after constructing a checkpoint,
to check request identity, limits, status/phase/outcome consistency, tool-ledger
pairing, and attempt uniqueness. A verified success requires a passing verifier
record. Success cannot retain unsettled tools or unresolved effects. State
transitions, current authorization, call dispatch, and duplicate resume-command
enforcement belong to the runtime/store implementation.

`ExecutionContextData.system_inputs` preserves three distinct inputs: omission,
an empty map, and a nonempty map. `null` is invalid. On start, omission means the
empty map; on resume, omission means reuse the saved inputs. `SystemInputs` owns
its values and redacts them from Debug output. Its serialization is for protected
storage. It is not automatically copied into messages or events.

## Digests and versions

`canonical_digest_json` accepts strict JSON; `canonical_digest` accepts an already
constructed `serde_json::Value`. The `sorted-json-v1:sha256:` encoding recursively
sorts object keys and preserves arrays and values. It is not RFC 8785. With the
supported JSON number representation, `1` differs from `1.0`, and `0` from `-0.0`.

Profile, run snapshot, session snapshot, and durable event formats have separate
`wickle.*.v1` identifiers. Model release identifiers are opaque strings, not parsed
as SemVer or dates. Requested and provider-reported model versions remain separate;
missing provider usage/version reports are not filled with inferred values.

## Run the consumer example

```sh
python3 scripts/check-package.py --allow-dirty
```

This builds the `.crate` outside its source workspace and runs
[`tests/support/consumer.rs`](../tests/support/consumer.rs) in an independent Rust
application. The Host supplies Tokio, a scoped metadata resolver, and a JSON
profile. The program validates the profile, persists and restores its resolved
identity, and reports the result. The consumer also accepts a profile JSON file
and an optional second file to compare against the pinned profile.
