use std::{fmt, future::Future, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{
    ContractError, ErrorCode, ExecutionContext, Id, JsonDigest, JsonObject, ModelPurpose,
    PortFuture, Scope, VersionedRef, serialization::data_digest,
};

/// Final, bound tool inputs visible to the trusted policy implementation.
/// Serialized values require protected storage and must not enter model/UI logs.
#[derive(Clone, PartialEq, Serialize)]
pub struct ToolPolicyInput {
    /// Core call identity.
    pub call_id: Id,
    /// Exact tool identity and version.
    pub tool: VersionedRef,
    /// Pinned descriptor identity.
    pub descriptor_digest: JsonDigest,
    /// Binding identity computed by the trusted input binder.
    pub binding_digest: JsonDigest,
    execution_args: JsonObject,
    #[serde(skip_serializing_if = "Option::is_none")]
    approval: Option<ToolApproval>,
    #[serde(skip_serializing_if = "Option::is_none")]
    selection: Option<crate::ToolBindingRef>,
}

impl ToolPolicyInput {
    /// Own the binder's final arguments. The gate never invents missing IDs.
    pub fn new(
        call_id: Id,
        tool: VersionedRef,
        descriptor_digest: JsonDigest,
        binding_digest: JsonDigest,
        execution_args: JsonObject,
    ) -> Self {
        Self {
            call_id,
            tool,
            descriptor_digest,
            binding_digest,
            execution_args,
            approval: None,
            selection: None,
        }
    }
    /// Inspect the full arguments to check actual target existence and ownership.
    pub fn execution_args(&self) -> &JsonObject {
        &self.execution_args
    }
    /// A recorded approval of this exact binding. The current policy still decides
    /// whether the actor may execute; this evidence never overrides a Deny.
    pub fn approval(&self) -> Option<&ToolApproval> {
        self.approval.as_ref()
    }
    /// Original selected catalog tool or adapter binding/export. A model alias
    /// alone never identifies the authorized external connection.
    pub fn selection(&self) -> Option<&crate::ToolBindingRef> {
        self.selection.as_ref()
    }
    pub(crate) fn with_selection(mut self, selection: crate::ToolBindingRef) -> Self {
        self.selection = Some(selection);
        self
    }
    pub(crate) fn with_approval(mut self, receipt: &crate::ResumeReceipt) -> Self {
        self.approval = Some(ToolApproval {
            command_id: receipt.command.command_id.clone(),
            command_ref: receipt.command_ref.clone(),
            accepted_revision: receipt.accepted_revision,
            actor_ref: receipt.actor_ref.clone(),
            capability_grant_ref: receipt.capability_grant_ref.clone(),
        });
        self
    }
}

/// Core-validated evidence that an authenticated actor approved a fixed tool binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ToolApproval {
    command_id: Id,
    command_ref: crate::RecordRef,
    accepted_revision: u64,
    actor_ref: Id,
    capability_grant_ref: Id,
}
impl ToolApproval {
    /// Accepted command identity.
    pub fn command_id(&self) -> &Id {
        &self.command_id
    }
    /// Protected command record, for authorized auditing.
    pub fn command_ref(&self) -> &crate::RecordRef {
        &self.command_ref
    }
    /// Revision at which approval was committed.
    pub fn accepted_revision(&self) -> u64 {
        self.accepted_revision
    }
    /// Authenticated approver.
    pub fn actor_ref(&self) -> &Id {
        &self.actor_ref
    }
    /// Host grant checked when approval was accepted.
    pub fn capability_grant_ref(&self) -> &Id {
        &self.capability_grant_ref
    }
}

impl fmt::Debug for ToolPolicyInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolPolicyInput")
            .field("call_id", &self.call_id)
            .field("tool", &self.tool)
            .field("descriptor_digest", &self.descriptor_digest)
            .field("binding_digest", &self.binding_digest)
            .field("execution_args", &"<redacted>")
            .finish()
    }
}

/// Operation being authorized; data access and protected-detail access differ.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum PolicyAction {
    /// Admit a new run.
    StartRun {},
    /// Read minimal run metadata.
    ReadRun {},
    /// Read the protected checkpoint, separately from the public view.
    ReadRunDetails {},
    /// Inspect one saved model preparation without executing it.
    InspectStep {
        /// Caller-selected saved identity.
        step: crate::StepRef,
        /// Requested display scope; this never grants secret-input access.
        options: crate::InspectionOptions,
        /// Final raw-content set. Allow must authorize the whole set against one current ACL view.
        /// Empty for the initial metadata/intent check.
        context_fragments: Vec<crate::InspectionFragmentRef>,
    },
    /// Read content of one exact source fragment for diagnostic display.
    InspectContextFragment {
        /// Scoped original producer and fragment identity.
        identity: Box<crate::FragmentIdentity>,
        /// Core observation revision.
        core_revision: std::num::NonZeroU64,
        /// Exact saved content identity.
        content_digest: JsonDigest,
    },
    /// Resume a recorded wait or interruption.
    ResumeRun {
        /// Exact command, so authorization distinguishes approving, denying,
        /// answering and supplying an external receipt.
        command: Box<crate::ResumeCommand>,
        /// Fixed tool binding when the saved wait belongs to a tool.
        binding_digest: Option<JsonDigest>,
    },
    /// Request cancellation.
    CancelRun {},
    /// Explicitly settle an elapsed Run deadline.
    ExpireRun {},
    /// Authorize a Worker to process an already authenticated durable control.
    ProcessControl {
        /// Immutable command being processed, including its original submitter.
        command: Box<crate::ControlCommand>,
    },
    /// Stop one execution interval without granting authority to cancel the Run.
    StopExecution {
        /// Host shutdown or explicit segment stop; protected causes are core-owned.
        cause: crate::InterruptionCause,
    },
    /// Read artifact data/metadata.
    ReadArtifact {},
    /// Write an artifact in the owning scope.
    WriteArtifact {},
    /// Load or use an exact Skill manifest under current access and model destination policy.
    ReadSkill {
        /// Native Skill identity and version.
        skill: VersionedRef,
        /// Complete immutable manifest identity.
        manifest_digest: JsonDigest,
        /// None for local loading/preparation, otherwise the model destination.
        route: Option<Box<crate::ResolvedModelRoute>>,
    },
    /// Read minimal event metadata.
    ReadEvents {},
    /// Read a protected record referenced by an event or checkpoint.
    ReadRecord {},
    /// Use scoped data in model context.
    UseContext {},
    /// Evaluate a fixed candidate under current authorization.
    VerifyCandidate {
        /// Protected candidate identity.
        candidate_ref: crate::RecordRef,
        /// Exact selected verifier, absent for local output validation.
        verifier_ref: Option<crate::VersionedRef>,
    },
    /// Rewrite only an authorized conversation projection, never the original transcript.
    RewriteContext {
        /// Exact read-only strategy identity.
        strategy: VersionedRef,
        /// Model destination for the resulting context.
        route: Box<crate::ResolvedModelRoute>,
    },
    /// Read one explicitly selected automatic context source.
    ProvideContext {
        /// Catalog or adapter export selection.
        source: crate::ContextSourceRef,
        /// Pinned source contract.
        definition_digest: JsonDigest,
        /// Logical lookup identity, reused after persistence.
        context_request_id: Id,
        /// Collection point.
        trigger: crate::ContextTrigger,
        /// Logical model step for step-scoped lookups.
        model_step_id: Option<Id>,
        /// Identity of the scoped query and lookup settings.
        input_digest: JsonDigest,
    },
    /// Recheck a saved source batch before local transformation or model transmission.
    UseSourceContext {
        /// Exact original source selection.
        source: crate::ContextSourceRef,
        /// Pinned source contract.
        definition_digest: JsonDigest,
        /// Saved batch whose data and derived context are being used.
        batch_ref: crate::RecordRef,
        /// None for local preparation, otherwise the exact model destination.
        route: Option<Box<crate::ResolvedModelRoute>>,
    },
    /// Invoke one selected lifecycle hook under its pinned definition and target.
    InvokeHook {
        /// Exact selected hook version.
        hook: VersionedRef,
        /// Original adapter binding/export; absent for catalog hooks.
        selection: Option<crate::HookRef>,
        /// Immutable execution definition.
        definition_digest: JsonDigest,
        /// Exact lifecycle invocation scope within the Run.
        target: crate::HookTarget,
    },
    /// Resolve approved component metadata before admission, without opening a connection.
    ResolveComponents {
        /// Identity of the profile and metadata being assembled.
        profile_resolution_digest: JsonDigest,
    },
    /// Open one adapter for a scoped execution or observer segment.
    BindAdapter {
        /// Profile-local binding, independent of exported model aliases.
        binding_id: Id,
        /// Exact registered adapter implementation version.
        adapter: VersionedRef,
        /// Full pinned definition including export contracts.
        definition_digest: JsonDigest,
        /// Named Host account/connection revisions, without credentials.
        connections: std::collections::BTreeMap<Id, VersionedRef>,
        /// Fresh scope-bound execution segment identity.
        binding_set_id: Id,
        /// Whether business tools or only observers may be activated.
        purpose: crate::ComponentBindPurpose,
    },
    /// Read one registered system key before the final target value is known.
    ResolveSystemInput {
        /// Exact tool requesting the lookup.
        tool: VersionedRef,
        /// Original adapter export selection, absent for catalog tools.
        selection: Option<crate::ToolBindingRef>,
        /// Logical call whose binding is being prepared.
        call_id: Id,
        /// Pinned tool descriptor identity.
        descriptor_digest: JsonDigest,
        /// Compiled input contract identity.
        compiled_digest: JsonDigest,
        /// Exact registry key, never a path expression.
        key: Id,
        /// Pinned system-input definition revision.
        definition_version: Id,
        /// Exact read-only resolver implementation.
        resolver_ref: VersionedRef,
    },
    /// Send input to a selected model route.
    InvokeModel {
        /// Exact provider, target, model and connection metadata for current authorization.
        route: Box<crate::ResolvedModelRoute>,
        /// Purpose being authorized.
        purpose: ModelPurpose,
    },
    /// Read the external outcome of a previously dispatched attempt.
    ReconcileTool {
        /// Frozen inputs and selected external binding.
        input: ToolPolicyInput,
        /// Original charged attempt, not a new execution.
        attempt_id: Id,
        /// Original external idempotency identity.
        idempotency_key: Id,
    },
    /// Dispatch one tool using final validated inputs.
    ExecuteTool {
        /// Final inputs, including system-owned parameters.
        input: ToolPolicyInput,
    },
}

/// An operation on an authoritative resource identity.
/// Obtain owner_scope from trusted stored metadata, not a caller's claimed scope.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PolicyRequest {
    /// Stored owner scope; user_id=None is not a wildcard.
    pub owner_scope: Scope,
    /// Run, artifact, event stream, record, context, or tool resource identity.
    pub resource_id: Id,
    /// Exact proposed action.
    pub action: PolicyAction,
}

impl PolicyRequest {
    /// Identity of the full proposed action and owning scope, including tool inputs.
    /// It excludes the approving principal so a new authorized reviewer can act.
    pub fn digest(&self) -> JsonDigest {
        data_digest(self)
    }
}

/// Current authenticated policy context, without the run's whole system-input map.
pub struct PolicyContext<'a> {
    /// Current authenticated resource scope.
    pub scope: &'a Scope,
    /// Current principal, distinct from resource scope and original tool inputs.
    pub principal_ref: &'a Id,
    /// Current grant reference; the Host checks membership and revocation.
    pub capability_grant_ref: &'a Id,
    /// Cooperative cancellation signal.
    pub cancellation: &'a CancellationToken,
    /// Effective policy deadline on the monotonic clock.
    pub deadline: Instant,
}

/// Host authorization decision. A reason is an informational code, not a grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
pub enum PolicyDecision {
    /// This exact action is currently allowed.
    Allow {},
    /// The action is denied.
    Deny {
        /// Safe reason code, without bound values or SDK error text.
        reason: Id,
    },
    /// The action requires an approval flow before it can be performed.
    RequireApproval {
        /// Safe reason code.
        reason: Id,
    },
}

impl PolicyDecision {
    /// Intersect a Host decision with a restriction; an allow never removes a denial
    /// or an approval requirement. Existing Host reasons take precedence.
    pub fn restrict(self, restriction: Self) -> Self {
        match (self, restriction) {
            (denied @ Self::Deny { .. }, _) | (_, denied @ Self::Deny { .. }) => denied,
            (approval @ Self::RequireApproval { .. }, _)
            | (_, approval @ Self::RequireApproval { .. }) => approval,
            _ => Self::Allow {},
        }
    }
}

/// Trusted Host policy. Implement actual resource/membership/FK checks here.
pub trait PolicyPort: Send + Sync {
    /// Check the current grant against the exact bound action without performing it.
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision>;
}

/// An approval request bound to an exact action, not a reusable permission token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApprovalChallenge {
    /// Scope whose resource will be affected.
    pub scope: Scope,
    /// Resource identity.
    pub resource_id: Id,
    /// Digest includes final tool input, descriptor/version, and scope.
    pub request_digest: JsonDigest,
    /// Safe reason code for the Host's approval UI.
    pub reason: Id,
}

/// Result of a guarded operation. Approval-required never invokes the operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Guarded<T> {
    /// Operation completed after a current authorization check.
    Completed(T),
    /// No operation was invoked; the Host/runtime must handle this approval request.
    ApprovalRequired(ApprovalChallenge),
}

/// Current authorization with exact scope matching, timeout, and cancellation.
/// This does not authenticate caller-supplied JSON or provide a sandbox for Host code.
pub struct PolicyGate {
    policy: Arc<dyn PolicyPort>,
    timeout: Duration,
}

impl PolicyGate {
    /// Configure a finite, positive policy timeout without creating a runtime.
    pub fn new(policy: Arc<dyn PolicyPort>, timeout: Duration) -> Result<Self, ContractError> {
        if timeout.is_zero() || Instant::now().checked_add(timeout).is_none() {
            return Err(ContractError::new(
                ErrorCode::InvalidContract,
                "policy.timeout",
            ));
        }
        Ok(Self { policy, timeout })
    }

    /// Check the current Host decision. Every call rechecks policy; permits are not cached.
    pub async fn check(
        &self,
        request: &PolicyRequest,
        context: &ExecutionContext,
        deadline: Option<Instant>,
        restriction: Option<PolicyDecision>,
    ) -> Result<PolicyDecision, ContractError> {
        if request.owner_scope != context.data.scope {
            return Err(ContractError::new(ErrorCode::AccessDenied, "scope"));
        }
        if context.cancellation.is_cancelled() {
            return Err(ContractError::new(ErrorCode::Cancelled, "policy"));
        }
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(ContractError::new(ErrorCode::RuntimeUnavailable, "policy"));
        }
        let now = Instant::now();
        let policy_deadline = now
            .checked_add(self.timeout)
            .ok_or_else(|| ContractError::new(ErrorCode::InvalidContract, "policy.timeout"))?;
        let effective = deadline.map_or(policy_deadline, |d| d.min(policy_deadline));
        if effective <= now {
            return Err(ContractError::new(ErrorCode::DeadlineExceeded, "policy"));
        }
        let decision = AssertUnwindSafe(async {
            tokio::select! {
                biased;
                _ = context.cancellation.cancelled() => Err(ContractError::new(ErrorCode::Cancelled, "policy")),
                _ = tokio::time::sleep_until(effective) => Err(ContractError::new(ErrorCode::DeadlineExceeded, "policy")),
                result = self.policy.authorize(request, PolicyContext {
                    scope: &context.data.scope, principal_ref: &context.data.principal_ref,
                    capability_grant_ref: &context.data.capability_grant_ref,
                    cancellation: &context.cancellation, deadline: effective,
                }) => result.map_err(|_| ContractError::new(ErrorCode::PolicyUnavailable, "policy")),
            }
        }).catch_unwind().await.map_err(|_| ContractError::new(ErrorCode::PolicyUnavailable, "policy"))??;
        if context.cancellation.is_cancelled() {
            return Err(ContractError::new(ErrorCode::Cancelled, "policy"));
        }
        if Instant::now() >= effective {
            return Err(ContractError::new(ErrorCode::DeadlineExceeded, "policy"));
        }
        Ok(match restriction {
            Some(other) => decision.restrict(other),
            None => decision,
        })
    }

    /// Invoke a closure only after current policy allows it. Future construction is
    /// also delayed until authorization. The operation owns its I/O cancellation and
    /// effect reconciliation; dropping a future is not treated as external rollback.
    pub async fn guard<T, F, Fut>(
        &self,
        request: &PolicyRequest,
        context: &ExecutionContext,
        deadline: Option<Instant>,
        restriction: Option<PolicyDecision>,
        operation: F,
    ) -> Result<Guarded<T>, ContractError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, ContractError>>,
    {
        match self.check(request, context, deadline, restriction).await? {
            PolicyDecision::Allow {} => operation().await.map(Guarded::Completed),
            PolicyDecision::Deny { .. } => {
                Err(ContractError::new(ErrorCode::AccessDenied, "policy"))
            }
            PolicyDecision::RequireApproval { reason } => {
                Ok(Guarded::ApprovalRequired(ApprovalChallenge {
                    scope: request.owner_scope.clone(),
                    resource_id: request.resource_id.clone(),
                    request_digest: request.digest(),
                    reason,
                }))
            }
        }
    }
}
