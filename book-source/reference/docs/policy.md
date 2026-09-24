# Scoped authorization

`PolicyGate` performs a current Host policy check before constructing or running
an operation. It compares the full stored resource scope with the authenticated
execution scope first. Tenant, workspace, and optional user scope must match
exactly; `None` is not a wildcard. The principal and current grant are separate
from the resource namespace.

Implement `PolicyPort::authorize` in Host code. The port receives the exact
`PolicyRequest` and a `PolicyContext` containing the current principal, grant,
cancellation signal, and monotonic deadline. The context does not expose the
entire run-level system-input map.

For `ExecuteTool`, `ToolPolicyInput` contains the binder's final arguments,
descriptor and binding digests, and exact tool identity. The policy can query
the application's resource catalog to check existence, owner, and revision of
the actual FK values. Wickle does not embed a membership database or infer
ownership from UUID syntax. The request's owner scope must come from trusted
stored metadata; a caller-supplied scope or record reference is not authorization.

## Decisions and execution

`PolicyDecision` is `Allow`, `Deny`, or `RequireApproval`. Its reason is a safe
informational code. `restrict` combines a Host decision with an additional
restriction: an allow cannot remove an existing denial or approval requirement.
Policy errors, panics, timeout, and cancellation return errors instead of allow.

Use `PolicyGate::guard` with an operation closure. The closure is invoked only
after a fresh allow decision. A denial returns `AccessDenied`; an approval
requirement returns `Guarded::ApprovalRequired` without constructing the operation.
The challenge's digest binds the owning scope and exact action, including final
tool inputs. It is not a cached permission token. Every later operation must
recheck current policy, including after a grant is revoked or an approver changes.

The gate requires a Host Tokio runtime and a positive policy timeout. An optional
run deadline shortens that timeout. The gate bounds policy evaluation; the actual
operation owns its I/O timeout/cancellation and external-effect reconciliation.
Dropping a future does not prove that a remote write was undone.

`PolicyRequest` serialization contains protected action data and belongs only in
appropriately protected Host storage. `ToolPolicyInput` redacts argument values
from Debug output. The gate does not alter global panic handlers or sandbox the
Host's policy implementation.

## Read views

The convenience methods derive owner scope from the stored object and select
public fields explicitly:

| Method | Current policy action | Result |
| --- | --- | --- |
| `run_view` | `ReadRun` | Run/session identity, status, phase, revision, and saved usage |
| `artifact_view` | `ReadArtifact` | Artifact identity, media type, size, and content hash |
| `event_view` | `ReadEvents` | Event/run/session identity, kind, sequence, and timestamp |
| `run_details` | `ReadRunDetails` | Full protected checkpoint, subject to distinct permission |

Minimal views omit system-input references, bound-input records, assembly/context
references, provider state, diagnostics, and outcome internals. Reading protected
event references requires a separate `ReadRecord` authorization. The store and
runtime must use these boundaries; the raw DTOs do not enforce access on their own.

## Consumer example

[`tests/support/policy_consumer.rs`](../tests/support/policy_consumer.rs) is a Host
program using an injected policy and a small resource catalog. Its modes exercise
allow, deny, approval, a foreign scope, and a missing target. It reports both the
authorization result and the number of actual record reads. The package check
builds and runs it against the extracted `.crate`:

```sh
python3 scripts/check-package.py --allow-dirty
```

Credential preparation for future provider adapters is documented in
[provider setup](provider-setup.md).
