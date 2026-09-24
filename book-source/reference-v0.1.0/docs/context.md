# Pin and project model context

`PromptSnapshot` owns a session's Host instructions, Profile instructions, selected
model-facing Tool definitions, and initial Skill listings. Host and Profile
instructions are separate System messages. Supplied conversation and reference
data cannot add a System message or replace the pinned prefix.

Create a snapshot from an already validated profile and authorized Host assets:

```rust
let prompt = wickle::PromptSnapshot::create(
    &profile,
    host_instructions,
    profile_instruction_asset,
    tool_bindings,
    skill_manifests,
)?;
let record = wickle::ProtectedRecord::new(
    prompt_record_id,
    1,
    serde_json::to_value(&prompt)?,
);
```

Tool bindings contain a selected profile reference and a `CompiledTool`. The
snapshot takes only its derived model schema and pinned identities. Hidden input
schemas and runtime system values are not prompt components. Skill listings
contain names, descriptions, versions, and manifest identities; creating the
snapshot does not load Skill bodies or fetch instruction assets.

The Agent's [Skill runtime](skills.md) loads bodies explicitly through its selected
loader Tool, then projects committed instructions with `Skill` origin and Run
lifetime. The protected body reference is not a model-visible Tool observation.

The caller must authorize supplied assets and attest that they match registered
implementations. Snapshot creation freezes that trusted assembly; it does not
query the registry or prove that arbitrary executable code matches its metadata.
Prompt structure also does not guarantee model compliance with instructions.
Actual access and tool execution remain subject to `PolicyGate`.

## Reuse the stored prefix

`prompt.digest()` equals the canonical digest in `record.reference()`. Store that
reference as the session's `prompt_snapshot`. Restore the record with
`PromptSnapshot::restore(serialized, expected_digest, &profile, &scope)`; the
expected digest must come from trusted saved state.

Restoration checks the full prefix identity, scope, Profile, and non-model
component identities. A new Run may resolve a new model binding while retaining
the same session prompt. Resuming an existing Run still uses that Run's original
resolved profile and route. New Tool/Skill content or changed Host instructions
do not silently replace the session snapshot.

## Project stored messages

`ContextAssembler::project` takes the snapshot and `ProjectionInput` and returns
an owned `ContextProjection` containing a separate `ModelRequest` and selected or
dropped source identifiers. The input transcript is read-only and must come from
an authorized, scoped store.

The stored transcript already includes the current request. Supply its message ID
so the assembler can verify the Run and exact input, include it once, and retain
later user steering. The logical model request ID and model step ID must agree.

`RunRequest.model_options` stores request-specific logical options, such as
`{"reasoning_effort":"high"}`, with the admitted request. Its digest and replay
checks include those options. The Host prepares `ProjectionInput.options`; the
assembler preserves it in `ModelRequest.options` outside the prompt content.
Options count toward request bytes and remain unchanged on same-route retries.

Supported keys and values come from the selected model and binding catalog
schemas, not a core reasoning-effort enum. The Host must check those schemas
before dispatch. An adapter maps accepted logical keys to its chosen provider
API explicitly; the map is not a raw request-body merge. Output-token limits
remain in `max_output_tokens`. Automatic Run-to-route option selection and
provider wire mappings belong to the runtime driver and adapters.

The projection preserves model-owned Tool arguments and public Tool observations.
It omits bound-input references, effect receipts, raw diagnostics, internal-only
messages, and reference ownership scope. Artifact/evidence metadata is included
only through the explicit public content mapping. Supplied `ContextItem` data
retains its source, origin, and applicable session/Run/step lifetime; it cannot
become System instructions.

Opaque provider continuation requires a typed `ScopedOpaque` value with matching
scope, record digest, provider, and exact route. A record reference alone does not
make unrelated protected data eligible for model replay.

For automatic memory or retrieval, configure a [ContextSource](context-sources.md).
Its immutable batches retain original data and revisions separately from this
projection. The Agent rechecks current access before using cached batches,
including physical model retries and resumed execution.

## Apply finite selection limits

`ProjectionLimits.max_bytes` bounds the serialized internal ModelRequest, and
`max_items` counts projected content blocks plus Tool definitions. The request's
own input byte bound also applies. These are finite memory/input limits, not a
token estimate or a provider-specific context-window guarantee.

The fixed prefix, model-visible current Run, required active context items,
latest Tool-round Run, and previous Runs with unknown Tool effects are mandatory.
If they do not fit, projection fails. Other complete historical Runs and optional
items can be omitted. Calls and results are never split to make them fit, and
incomplete rounds or incompatible visibility are rejected without inventing a
Tool result. An expired or inapplicable context item is reported as dropped.

The [context consumer](../tests/support/context_consumer.rs) exercises two Runs in
one MemoryStateStore session, protected prompt restoration, rejection of a changed
prompt, and exclusion of internal execution data.

The Agent can add [validated context revisions](context-compaction.md) before
this projection. These can preview large Tool observations or replace complete
older conversation groups with historical summaries. Original store messages
remain unchanged. Summary data appears before the retained conversation so it
does not present an older state as a new instruction after the latest Tool result.
