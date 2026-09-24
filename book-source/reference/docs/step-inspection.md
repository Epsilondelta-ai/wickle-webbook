# Inspecting a saved model step

`Agent::inspect_step` reads persisted preparation and invocation records. It does
not run a model, Tool, source, hook, resolver, factory, compiler or tokenizer. It
also does not acquire a lease or write a checkpoint. The result is a
`Guarded<CompositionReport>`; approval requirements do not create a Run wait.

Select a preparation by its saved record ID, or by logical step, purpose and
projection revision:

```rust,ignore
let step = StepRef::Prepared { record_id: prepared_record_id };
let result = agent
    .inspect_step(&run_id, step, &context, InspectionOptions::default())
    .await?;

match result {
    Guarded::Completed(report) => {
        // Check report.status and report.unresolved before displaying fields.
        // report.composition contains only saved evidence.
    }
    Guarded::ApprovalRequired(challenge) => {
        // Ask the Host's authorization workflow; inspection did not pause the Run.
    }
}
```

The authorized checkpoint exposes preparation IDs in `prepared_steps`. A Host
that already records those IDs can query directly without obtaining internal
runtime objects. A selector for another Run does not read that preparation.

## Evidence and missing data

The report identifies the selected model, adapter and API versions, canonical and
provider model-owned schemas, compiler and decoding-plan digests, constraint text
origins/digests, requested/effective inference options and their origins, ordered
context revisions, selection lists, token estimates and original fingerprint.
Run status and outcome metadata are tied to the snapshot revision that was read.

- `Prepared` proves a saved input exists.
- `DispatchReserved` proves a physical-call budget reservation exists.
- `ResponseObserved` needs a completed normalized response, partial response text,
  or actually reported metadata. It does not assert business success.
- `TransmissionUnknown` remains when stored evidence does not prove a response.
  A saved local transport or empty-stream failure is insufficient evidence.

`result_recorded` distinguishes a saved result/failure record from response
observation. Reservation alone does not prove transmission. Inspecting a failed
or incomplete attempt never retries it or changes the Run's outcome.

`InspectionStatus` distinguishes found, partial, not-found and expired evidence.
Storage implementations should return `ErrorCode::RecordExpired` only for explicit
retention expiry; an ordinary missing record remains not-found. A logical lookup
with missing preparation records is partial because the missing record's step
identity is unknown. Missing child records remain listed in `unresolved`.

If the saved format lacks an estimator identity or an individual exclusion reason,
the report says `not_recorded`. It does not call today's estimator or reconstruct
a reason using current configuration. Display redaction never changes the original
fingerprint or schema/constraint digests.

## Disclosure and current permission

Default reports omit system inputs, assembled execution arguments, connection
references/targets, credentials, opaque continuation, model messages, output text
and context source content. Tool descriptions and schema annotations such as
examples/defaults are omitted or masked for display. Sensitive option keys are
masked recursively. `redacted_paths` marks display changes; it is separate from
context items actually excluded from the model projection.

Set `include_context_content: true` to request source fragment content. Each item
requires `InspectContextFragment` permission; denied or approval-gated items stay
redacted. Content is capped at 64 KiB for the whole report, without partial slices.
The final `InspectStep` authorization carries **all included `context_fragments`**,
with scope, identity, core revision and content digest. A Host policy must authorize
that entire set against one current ACL view before returning Allow. This prevents
an earlier item permission from becoming stale during later item checks. Metadata
permission alone does not authorize the final content set.

Neither raw-content opt-in nor an approval grants access to system execution
arguments or connection credentials. No source authorization callback or external
retrieval is run during inspection; the Host's current PolicyPort owns diagnostic
access decisions.
