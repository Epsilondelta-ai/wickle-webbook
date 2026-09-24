# Select, preview, and compact context

The Agent first applies bounded context selection. If the request still does not
fit, it can preview large Tool observations and summarize older complete
conversation groups. Original messages stay in the StateStore. The model uses a
separate, validated context revision.

With no `AgentBindings.context_runtime`, the Agent uses bounded selection and
previews when an `ArtifactRuntime` is available. It does not call a compressor.
Configure a model-backed compressor explicitly:

```rust
bindings.context_runtime = Some(std::sync::Arc::new(wickle::ContextRuntime::new(
    bindings.scope.clone(),
    std::sync::Arc::new(wickle::BoundedContextStrategy),
    Some(wickle::ContextCompactor::Model(wickle::ModelCompactorConfig {
        model_binding: wickle::Id::new("primary")?,
        options: None,
        max_output_tokens: 2048.try_into()?,
    })),
    wickle::ContextRewriteLimits::default(),
)?));
```

The selected logical binding needs a routing rule for `ModelPurpose::Compaction`.
The Agent rejects a missing rule before execution. Compression uses the same
ModelExchange, route inspection, current policy, and Run model-call budget as
other inference. `options: None` uses only the selected binding defaults;
an explicit map supplies purpose-specific overrides subject to both target schemas.
Agent Profile and Run options are never inherited by compaction.
Compaction calls do not replace the Agent's current step, invoke Agent context
Hooks, or become the final user response.

For a non-model algorithm, implement `HostContextCompactor` and configure the Host
variant with an exact implementation version. These callbacks must be read-only
and must not hide model calls or business writes. `ContextStrategy` only proposes
complete eligible message IDs; it receives no mutable Run, store, or credentials.

## Preserve the boundaries

- User messages are never summarized. The current request and constraints remain
  intact; ordinary bounded selection may omit older completed Runs.
- The latest complete Tool round remains intact. Unresolved effects and rounds
  with result corrections are protected.
- Calls and results are removed together. A partial or unknown selection fails
  before the compressor runs.
- Loaded Skill instructions and active external context remain separate context
  items. Typed Artifact and Evidence references from summarized groups are kept
  as anchors with their original identities, versions and hashes.
- Opaque replay is removed only with its entire summarized group. Remaining
  replay stays on its original exact model route; opaque data is not sent to the
  compressor as conversation text.

Historical summaries appear before the retained conversation and current
request, with `Compaction` origin. A model compressor sees only the selected older
segments and must not infer the state of newer retained messages. Its summary is
background data and cannot replace System instructions or grant Tool permissions.
The core verifies structure, provenance, pairing and size; it does not prove the
factual accuracy of arbitrary summary prose.

## Bound and store each change

`ContextRewriteLimits` bounds local preparation, compressor input and output,
preview thresholds/counts, compression decisions, and callback time. Source
bytes and Host token estimates are separate measurements. A candidate must
reduce serialized request size, not increase the token estimate, and fit the
final model, byte and item limits. Empty, oversized or ineffective candidates
are rejected without replacing the previous revision.

Large text/JSON Tool observations can become an Artifact reference plus an
explicitly truncated UTF-8 preview. The original Tool result stays unchanged.
Preview artifacts use core-generated deterministic identities, so identical
retries follow the store's immutable, idempotent `put` contract. The Host supplies
separate reading Tools when the model needs the full original.

`context.rewritten` and the new revision are committed together under the Run's
lease and expected revision. The session records its latest revision for later
Runs. Completed compression decisions retain their source input and rejection or
application result. Retry/replay reuses saved work; a pure Host callback whose
result could not be committed may be invoked again. Unadopted artifacts remain
subject to the Host's retention policy.

Context plans pin the strategy, compressor settings and rewrite limits. Existing
revised sessions reject a changed plan; use a new session for a different one.
Legacy Runs without a plan can use bounded projection but do not silently acquire
new rewrite behavior during recovery.

The [independent compaction consumer](../tests/support/compaction_consumer.rs)
exercises model-budget accounting, complete-group summaries, SQLite restoration,
and fresh Host replay using synthetic model ports.

## Recheck source access for derived data

Context revisions preserve `source_lineage`: immutable original Run and batch
references for the covered conversation. Each batch preserves its fragment
versions. Model messages also retain `source_model_request_id` when known, so a
later inference cannot change which observation produced an earlier answer.
Legacy terminal history without an attempt anchor conservatively depends on
all observations from its original Run. An active legacy Run with unanchored
source-derived text stops with `context.unanchored_active_history`; finish it
with its original engine before migrating. The core does not invent an attempt
association and later reinterpret an already stored summary.

Before projection, compression, and each physical model attempt (including
verification), the core checks current access to the original sources. This also
protects retained historical responses and summaries reused by another Run in
the same session. An explicit deletion or denied access stops use; the core does
not silently resend the summary or regenerate it from revoked material. A Host
can start a fresh authorized session when the previous history is no longer
usable.
