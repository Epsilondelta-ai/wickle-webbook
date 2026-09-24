# Validate and verify agent output

An Agent checks the final output format before declaring success. Text is the
default. For `OutputContract::JsonSchema`, register an exact
`OutputSchemaDefinition` in `VerificationRuntime`; the resolved schema is sent
to a model route with `json_output` capability and checked again locally. Invalid
JSON, duplicate keys, or a schema mismatch produces repair feedback within the
Run's repair budget.

`CompletionPolicy::TurnEnd` records `CompletionBasis::TurnEnded`. This means the
model finished with a valid output format. `CompletionPolicy::Verified` also
requires a registered verifier to accept the saved candidate and records
`CompletionBasis::Verified` with the criteria version and evidence reference.

## Register a verifier

Supply `AgentBindings.verification` with an immutable, scoped
`VerificationRuntime`. It holds output schemas, approved `Verifier`
implementations, and finite `VerificationLimits`. Definitions pin the verifier
version, criteria version, description, and complete nonsecret configuration.
Changing an admitted plan during resume is rejected.

A verifier returns `VerificationDecision::Pass`, `Revise`, `Wait`, or `Fail`.
`SchemaVerifier` is a deterministic reference implementation for JSON criteria;
its compiled schema is included in the pinned configuration. It checks supplied
data against a schema and cannot prove the truth of external business state.

The [independent consumer](../tests/support/verification_consumer.rs) registers a
JSON output schema and separate minimum-value criteria, repairs a rejected
candidate, and restores the result from SQLite without additional model calls.

## Keep verification within the Run

The core stores the candidate and its original model response before invoking a
verifier. Evidence identifies immutable Tool observation messages at the
candidate's transcript boundary. The callback receives model-visible output,
the original request, and those observations; it does not receive tool receipts
or execution arguments through the evidence channel.

Verifiers are read-only. They must not execute business tools or hide model
calls. Use `VerifierContext.models.generate` for model-based checks. Each review
stage uses an explicit `ModelPurpose::Verification` routing rule, the existing
ModelExchange, current authorization, and the same Run model-call budget. It
cannot replace the Agent's step, use business tools, or invoke Agent context
hooks. A model judgment can still be wrong; verified means that the configured
criteria were accepted, not that arbitrary business claims are guaranteed.

Repair charges `repair_attempts` separately from the next model call. Previous
candidates and feedback stay in the transcript, with `MessageOrigin::Verification`
for feedback. Existing Tool plans and effect receipts remain intact. A new Tool
call proposed during repair is still a new operation and must follow the Tool's
idempotency policy.

Keep profile instructions, output schemas, and verifier criteria consistent.
Feedback does not replace profile constraints; conflicting requirements can
exhaust the repair budget without producing verified success.

## Review, failure, and recovery

`Wait` creates a normal approval wait bound to the exact candidate and verifier.
Use the saved `wait_id`, target, and revision in `ResumeAction::Approve` or `Deny`.
The core rechecks current permission and consumes that decision before generating
another model response. Stale or different targets cannot approve a candidate.

A quality rejection finishes as `verification_failed`. Failure to complete the
verifier callback is `verification_unavailable`; cancellation and exhausted Run
budgets retain their own classifications. Timed-out or cancelled checks cannot
publish a late successful verdict.

Candidate decisions, criteria, and `verification.completed` events are durable.
A completed decision is reused on replay. A pure callback whose result was never
committed may run again after recovery; budgeted model stages reuse saved model
responses. Keep callback behavior read-only and versions immutable.
