# Store artifacts and evidence

`ArtifactStore` provides scoped `put`, `stat`, and bounded `get` operations.
`ArtifactRuntime` adds current policy checks, finite time and size limits, and
validation of the returned identity, media type, byte count and SHA-256 hash.
The Host supplies storage and owns retention and deletion. `MemoryArtifactStore`
is an in-process reference implementation; it does not provide durability.

```rust
let artifacts = wickle::ArtifactRuntime::new(
    std::sync::Arc::new(wickle::MemoryArtifactStore::default()),
    policy,
    ids,
    wickle::ArtifactLimits::default(),
)?;
let stored = artifacts.put(
    wickle::ArtifactInput {
        media_type: wickle::Id::new("text/plain")?,
        bytes: report.into_bytes(),
        source: Some(source_revision),
    },
    &authenticated_context,
    None,
).await?;
let preview = artifacts.preview(&stored.reference, &authenticated_context, None).await?;
```

Artifact IDs come from the Host's `IdSource`. Model-generated IDs are not used to
invent an owning scope or business foreign key. `WriteArtifact` and `ReadArtifact`
are separate policies. Reads check permission again after storage returns data.
A request for another scope is rejected before entering the store.

## Keep originals and previews separate

An `ArtifactRef` identifies immutable bytes and metadata. Reusing an ID with
different bytes, media type, or source revision is an error in the reference
store. A source update creates a new artifact; old evidence keeps its original
version and hash.

`get` returns complete bytes or fails. `preview` returns bounded UTF-8 text with
an explicit `truncated` flag, ending at a character boundary. Binary artifacts
have no invented text preview. A preview never replaces the original bytes.

`evidence` uses stored source identity/revision and the original byte hash. Optional
quotes must fit the preview bound and occur in the original UTF-8 data. The Host
publisher attests the external source identity and the meaning of a passage
location; the library does not independently verify an external document system.

## Return references from a Tool

Set `AgentBindings.artifacts` when Tools return typed artifact/evidence content.
Store a large original through `ArtifactRuntime` and return a bounded value plus
references using `ToolExecutionOutcome::SucceededWithContent`:

```rust
wickle::ToolExecutionResult {
    outcome: wickle::ToolExecutionOutcome::SucceededWithContent {
        value: serde_json::json!({"rows": row_count}),
        content: vec![
            wickle::InputContent::Artifact { reference: stored.reference },
            wickle::InputContent::Evidence { reference: evidence },
        ],
    },
    effect,
    receipt,
}
```

The value must satisfy the pinned output schema, and value plus content must fit
the Tool's output bound. Evidence must accompany its artifact reference and match
the stored source version, hash, and quote. Ordinary JSON remains ordinary data;
the core does not interpret arbitrary embedded objects as artifact capabilities.

The Tool boundary validates references before settlement. Failures clear the
invalid model content and classify the result while preserving any confirmed
business effect and receipt. If a handler performs a business write and a later
artifact write fails, it must still return those effect facts explicitly.
Throwing away a receipt cannot establish that a business write did not occur.

The Agent reports produced artifact references in `RunOutcome.artifacts` and
rechecks access to typed Tool artifacts actually included in each model projection.
Retrieval and Skill sources also retain their own current-access checks. Required
business actions do not run as artifact cleanup callbacks, and storage failures
do not cause the core to repeat a completed Tool call.
