# Reuse immutable model preparations

A routed model step saves its final input before reserving a physical invocation.
`PreparedStepRecord` links the selected route and model configuration, the original
Tool execution contracts, provider schemas and codecs, and the exact context
projection. `RunSnapshot.prepared_steps` retains that history;
`ModelInvocationRecord.prepared_step_ref` identifies the preparation used by each
physical attempt. The current Agent preparation is also available through
`RunSnapshot.active_prepared_step`.

| Situation | Behavior |
| --- | --- |
| Same-input transport retry | Reuse the preparation; reserve a new physical attempt |
| Process recovery before dispatch | Load the committed preparation without calling the projector or compiler |
| Allowed provider fallback | Compile and save a new projection revision for the selected destination |
| Recovery after fallback preparation | Reuse that target and revision without charging the fallback twice |
| Completed-step replay | Recheck current access and return the saved response without new inference |
| New logical Agent step | Clear the active pointer and prepare new input; retain prior records |

The projection fingerprint excludes request/attempt identifiers. Options, route,
messages, Tool schemas, output format, and request limits remain part of the
input identity. Current permissions, cancellation, deadlines, and lease ownership
are checked again; a stored preparation is not a lasting execution grant.

Preparation, Tool contracts, and their snapshot references commit together. The
physical reservation, model-call budget, invocation, inspection evidence, and
route event then commit in one separate transaction. If preparation storage fails
or its acknowledgement is lost, no model call is dispatched. A reserved attempt
without a settled response still requires explicit recovery; reservation alone
does not prove that a provider received a request.

## Bind provider schemas to Tool execution

Override `ModelPort::tool_schema_compiler` to supply an adapter-owned
`ProviderToolSchemaCompiler`. The default is `NativeToolSchemaCompiler`. The
compiler receives model-visible fields only. The Agent builds provider schemas,
constraint explanations and reversible codecs before context sizing and token
estimation, so the extra explanation must fit the same input budget.

`ResolvedToolSet` keeps the session-pinned selection/export, name, descriptor and
compiled input identity. The complete execution schema is protected storage data.
Only provider-facing schemas and constraint explanations reach the model.

A completed response is decoded with its saved contract. The core then applies
canonical defaults, validates model fields, runs the before-Tool transformation,
and binds system inputs. Advertising a renamed provider Tool does not authorize
its unadvertised canonical name. Unknown names produce an error observation and
never dispatch an existing registry Tool merely because its name matches.
Historical canonical Tool calls are encoded for the destination provider without
changing the stored transcript or adding system input values.

## Implement a custom projector

`ModelRequestProjector::project` runs only when creating a new preparation. Return
`ProjectedModelRequest` with the final request/token estimate, corresponding
`tool_set` entries, `compiled_tools`, and selection `provenance`. Empty Tool lists
use empty contract lists. Nonempty lists must correspond to the approved
session manifest; use `ResolvedToolSetEntry::new` to validate that mapping.

`ModelProjectionContext.tool_schema_compiler` is `Some` during new projection and
`None` during saved-input authorization. Implement `authorize_prepared` when your
projector has additional external dependencies. It receives the exact stored
`PreparedModelProjection`; it must check current access without regenerating the
input. The default delegates to `authorize_use`.

The built-in Agent rechecks active sources, historical source lineage, Skills and
artifacts. Source selections and fragments are cross-checked against the saved
source plan and batch history. Artifact dependencies are reconstructed from typed
wire references and original Tool/context records. The original transcript
boundary is independently pinned in the logical-step record, so later messages
cannot change the dependencies of an older preparation.

## Restore older records

Terminal history without prepared records remains readable. An active logical
step written without an original transcript boundary cannot be upgraded by
inventing one: routed execution returns `prepared.legacy_step_boundary`. Finish
that Run with its original runtime before migrating. Do not delete protected
records or their references to force a resume.

The lower-level, unrouted `ModelExchange::generate` accepts an already constructed
request and has no routed-preparation contract. It still uses atomic physical
reservation and invocation accounting. The Agent, verifier, and model compactor
use the routed path.
