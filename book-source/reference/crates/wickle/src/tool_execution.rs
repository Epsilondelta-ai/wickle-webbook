use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, fmt, sync::Arc};
use tokio_util::sync::CancellationToken;

mod reconciliation;
mod resume;
mod round;

/// Identity and controls for one physical tool call. Credentials and unrelated
/// system inputs remain in the executor's Host-owned binding.
#[derive(Debug, Clone)]
pub struct ToolExecutionContext {
    /// Owning Run, checked independently of the process-local binding set.
    pub run_id: Id,
    /// Scoped runtime segment; absent for directly injected catalog executors.
    pub binding_set_id: Option<Id>,
    /// Logical call whose plan and bound input were already saved.
    pub call_id: Id,
    /// Charged physical attempt, already recorded before execution.
    pub attempt_id: Id,
    /// Stable external deduplication identity across recovery of this call.
    pub idempotency_key: Id,
    /// Exact authorized namespace.
    pub scope: Scope,
    /// Current authenticated actor.
    pub principal_ref: Id,
    /// Current Host authorization grant.
    pub capability_grant_ref: Id,
    /// Cancelled when the attempt stops, including timeout or caller cancellation.
    pub cancellation: CancellationToken,
    /// Finite execution deadline.
    pub deadline: tokio::time::Instant,
}

/// Effect information attested by the trusted executor, independent of output validation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolEffect {
    /// The executor confirms no external business write occurred.
    NotApplied,
    /// An external business write is confirmed; its receipt must be retained.
    Applied,
    /// Whether an external business write occurred could not be established.
    #[default]
    Unknown,
}

/// A handler's safe result; it cannot replace core call identities or ledger state.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolExecutionOutcome {
    /// Complete instructions from the explicitly configured Skill loader only.
    LoadedSkill {
        /// Validated again by the core before the body and Tool result are committed together.
        loaded: Box<LoadedSkill>,
    },
    /// Returned value to check against the pinned output schema.
    Succeeded {
        /// Raw returned JSON; only a validated, bounded value becomes model content.
        value: Value,
    },
    /// A schema-validated value plus explicit bounded observations or stored references.
    SucceededWithContent {
        /// Value validated against the Tool's output schema.
        value: Value,
        /// Typed observations; artifacts and evidence require current store validation.
        content: Vec<InputContent>,
    },
    /// Classified handler failure, independent of whether a write happened.
    Failed {
        /// Safe registered failure code, without SDK error messages or payloads.
        code: Id,
    },
    /// Ask the Host for the value that will complete this call, without rerunning
    /// the executor. Requires NotApplied and no receipt. The pinned output schema
    /// validates the answer; this does not suspend and resume handler code.
    InputRequired {
        /// Bounded question displayed to the authorized caller.
        question: String,
    },
}

/// Explicit completion and effect receipt. Serialize only for protected storage.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolExecutionResult {
    /// Returned value or safe failure classification.
    pub outcome: ToolExecutionOutcome,
    /// Observed external effect status.
    pub effect: ToolEffect,
    /// Original effect receipt, required for a confirmed Applied result.
    pub receipt: Option<Value>,
}
impl fmt::Debug for ToolExecutionOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::LoadedSkill { .. } => "ToolExecutionOutcome::LoadedSkill(<protected>)",
            Self::Succeeded { .. } => "ToolExecutionOutcome::Succeeded(<protected>)",
            Self::SucceededWithContent { .. } => {
                "ToolExecutionOutcome::SucceededWithContent(<protected>)"
            }
            Self::Failed { .. } => "ToolExecutionOutcome::Failed(<classified>)",
            Self::InputRequired { .. } => "ToolExecutionOutcome::InputRequired(<protected>)",
        })
    }
}
impl fmt::Debug for ToolExecutionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolExecutionResult")
            .field("effect", &self.effect)
            .field("has_receipt", &self.receipt.is_some())
            .finish_non_exhaustive()
    }
}

/// Read-only observation of the original attempt's external effect.
#[derive(Debug, Clone, PartialEq)]
pub enum ToolReconciliation {
    /// The Host authenticated a complete result for the original idempotency key.
    Known {
        /// Output validation still applies and must retain confirmed effect receipts.
        result: ToolExecutionResult,
    },
    /// The effect cannot be established. This never authorizes another execution.
    Unknown,
}

/// Exactly one physical execution. Implementations must not hide retry loops or
/// spawn untracked operations; effect uncertainty must be reported honestly.
pub trait ToolExecutor: Send + Sync {
    /// Execute only the final policy-approved arguments, not the original model
    /// map, full system-input snapshot, or caller-supplied tool identities.
    fn execute<'a>(
        &'a self,
        execution_args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult>;

    /// Read the outcome of the original attempt without executing it again.
    /// Arguments and attempt/idempotency identities are frozen; actor, grant and
    /// cancellation/deadline belong to the current recovery operation.
    /// The conservative default leaves the effect unknown.
    fn reconcile<'a>(
        &'a self,
        _execution_args: &'a JsonObject,
        _context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolReconciliation> {
        Box::pin(async { Ok(ToolReconciliation::Unknown) })
    }
}

/// Exact saved effect and protected evidence presented to a trusted Host verifier.
/// The Host must authenticate the receipt and its ownership, not merely compare
/// caller-supplied IDs. This value must not be sent to a model or ordinary logs.
#[derive(Clone)]
pub struct ExternalReceiptRequest {
    /// Original saved logical call, including its immutable input reference.
    pub call: ToolCall,
    /// Original uncertain physical attempt; no new execution is requested.
    pub attempt_id: Id,
    /// Original external deduplication identity.
    pub idempotency_key: Id,
    /// Restored, policy-authorized final arguments for that same call.
    pub bound_input: BoundToolInput,
    /// Exact record authorized and retrieved by the Agent before verification.
    pub receipt_ref: RecordRef,
    /// Protected record contents, not an untrusted substitute for the reference.
    pub receipt: Value,
}
impl fmt::Debug for ExternalReceiptRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ExternalReceiptRequest(<protected>)")
    }
}

/// Current authorization and finite controls for a read-only receipt inspection.
#[derive(Debug, Clone)]
pub struct ExternalReceiptContext {
    /// Exact namespace of the waiting run and receipt.
    pub scope: Scope,
    /// Current authenticated actor.
    pub principal_ref: Id,
    /// Current Host authorization grant.
    pub capability_grant_ref: Id,
    /// Cancelled when verification stops or times out.
    pub cancellation: CancellationToken,
    /// Finite callback deadline.
    pub deadline: tokio::time::Instant,
}

/// Read-only Host attestation of a previously uncertain effect. It must not
/// execute or retry the business operation. Unknown leaves the wait unresolved.
pub trait ExternalReceiptVerifier: Send + Sync {
    /// Return an authenticated result for the original call and frozen target.
    fn verify<'a>(
        &'a self,
        request: &'a ExternalReceiptRequest,
        context: &'a ExternalReceiptContext,
    ) -> PortFuture<'a, ToolExecutionResult>;
}

/// Prepared settlement for one authorized resume command. No state is committed
/// here: the Agent saves this alongside command consumption and RunResumed in one
/// transaction. Protected values deliberately have no ordinary Debug output.
pub struct PreparedToolResolution {
    /// Final observation for the original logical call.
    pub result: ToolResult,
    /// Replacement ledger state for that call.
    pub state: ToolCallState,
    /// Paired result, or explicit correction of the old Unknown observation.
    pub message: Message,
    /// Immutable result and diagnostic/effect records needed by the settlement.
    pub records: Vec<ProtectedRecord>,
    /// Next ToolSettled event; the Agent sequences RunResumed after it.
    pub event: RunEvent,
}
impl fmt::Debug for PreparedToolResolution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PreparedToolResolution(<protected>)")
    }
}

struct AttemptIdentity<'a> {
    scope: &'a Scope,
    run_id: &'a Id,
    attempt_id: &'a Id,
    idempotency_key: &'a Id,
}

/// A trusted Host associates one compiled contract with an existing executor.
/// Factory-level code/manifest attestation is separate from this registration.
#[derive(Clone)]
pub struct ToolRegistration {
    /// Exact descriptor and model-input projection.
    pub compiled: CompiledTool,
    /// Existing scoped executor; construction and credentials remain in Host code.
    pub executor: Arc<dyn ToolExecutor>,
}
impl fmt::Debug for ToolRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolRegistration")
            .field("compiled", &self.compiled)
            .finish_non_exhaustive()
    }
}

/// Scope-bound, immutable mapping of exact tool contracts to existing executors.
#[derive(Debug)]
pub struct ToolRegistry {
    scope: Scope,
    entries: BTreeMap<Id, ToolRegistration>,
    selections: BTreeMap<Id, ToolBindingRef>,
}
impl ToolRegistry {
    /// Register without invoking handlers; duplicate names and exact tool identities fail.
    pub fn new(scope: Scope, entries: Vec<ToolRegistration>) -> Result<Self, ContractError> {
        let mut registered = BTreeMap::new();
        for entry in entries {
            if registered.values().any(|prior: &ToolRegistration| {
                prior.compiled.descriptor().tool == entry.compiled.descriptor().tool
            }) || registered
                .insert(entry.compiled.descriptor().name.clone(), entry)
                .is_some()
            {
                return Err(error(
                    ErrorCode::InvalidToolInputContract,
                    "tools.duplicate",
                ));
            }
        }
        let selections = registered
            .iter()
            .map(|(name, entry)| {
                (
                    name.clone(),
                    ToolBindingRef::Catalog(CatalogToolRef {
                        tool_id: entry.compiled.descriptor().tool.id.clone(),
                        version: entry.compiled.descriptor().tool.version.clone(),
                        bindings: None,
                        config: None,
                    }),
                )
            })
            .collect();
        Ok(Self {
            scope,
            entries: registered,
            selections,
        })
    }
    /// Construct a fully attested registry without replacing real Export selections
    /// by catalog aliases. Duplicate visible names or selections are rejected.
    pub fn from_bindings(
        scope: Scope,
        bindings: Vec<(ToolBindingRef, ToolRegistration)>,
    ) -> Result<Self, ContractError> {
        let mut entries = BTreeMap::new();
        let mut selections = BTreeMap::new();
        for (selection, entry) in bindings {
            let valid = match &selection {
                ToolBindingRef::Catalog(reference) => {
                    reference.tool_id == entry.compiled.descriptor().tool.id
                        && reference.version == entry.compiled.descriptor().tool.version
                }
                ToolBindingRef::Export(reference) => reference
                    .alias
                    .as_ref()
                    .is_none_or(|alias| alias == &entry.compiled.descriptor().name),
            };
            if !valid
                || selections.values().any(|prior| prior == &selection)
                || entries.contains_key(&entry.compiled.descriptor().name)
            {
                return Err(error(
                    ErrorCode::InvalidToolInputContract,
                    "tools.selection",
                ));
            }
            selections.insert(entry.compiled.descriptor().name.clone(), selection);
            entries.insert(entry.compiled.descriptor().name.clone(), entry);
        }
        Ok(Self {
            scope,
            entries,
            selections,
        })
    }
    /// Metadata-only view for input validation and cancellation/expiry settlement.
    /// Its placeholder executors perform no I/O and report NotApplied unavailable.
    pub fn metadata(
        scope: Scope,
        bindings: Vec<ResolvedToolBinding>,
    ) -> Result<Self, ContractError> {
        Self::from_bindings(
            scope,
            bindings
                .into_iter()
                .map(|binding| {
                    (
                        binding.selection,
                        ToolRegistration {
                            compiled: binding.compiled,
                            executor: Arc::new(MetadataTool),
                        },
                    )
                })
                .collect(),
        )
    }
    /// Actual authority selection behind a model-visible name.
    pub fn selection(&self, name: &Id) -> Option<&ToolBindingRef> {
        self.selections.get(name)
    }
    /// Exact namespace under which handlers were registered.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Inspect an exact portable name without executing it or resolving an alias.
    pub fn get(&self, name: &Id) -> Option<&ToolRegistration> {
        self.entries.get(name)
    }
    /// Return only profile-selected contracts in profile order. Adapter exports
    /// require their separate runtime factory and are not implicitly opened here.
    pub fn prompt_bindings(
        &self,
        profile: &AgentProfile,
    ) -> Result<Vec<PromptToolBinding>, ContractError> {
        profile
            .tools
            .iter()
            .map(|selection| {
                let entry = self
                    .entries
                    .iter()
                    .find(|(name, entry)| match selection {
                        ToolBindingRef::Catalog(reference) => {
                            entry.compiled.descriptor().tool.id == reference.tool_id
                                && entry.compiled.descriptor().tool.version == reference.version
                        }
                        ToolBindingRef::Export(_) => self.selections.get(*name) == Some(selection),
                    })
                    .ok_or_else(|| error(ErrorCode::ComponentUnavailable, "tools.selection"))?;
                Ok(PromptToolBinding {
                    selection: selection.clone(),
                    compiled: entry.1.compiled.clone(),
                })
            })
            .collect()
    }
}
struct MetadataTool;
impl ToolExecutor for MetadataTool {
    fn execute<'a>(
        &'a self,
        _: &'a JsonObject,
        _: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async {
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::Failed {
                    code: Id::new("component_unavailable")?,
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}

/// Per-attempt bounds. The Run still owns total attempts, recovery and elapsed time.
#[derive(Debug, Clone, Copy)]
pub struct ToolExecutionLimits {
    /// Maximum elapsed time for one executor callback.
    pub timeout_ms: u64,
    /// Maximum raw effect-receipt size accepted from a handler.
    pub max_receipt_bytes: usize,
}
impl Default for ToolExecutionLimits {
    fn default() -> Self {
        Self {
            timeout_ms: 30_000,
            max_receipt_bytes: 65_536,
        }
    }
}

/// Whether the complete saved round is safe to follow with another model step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolRoundOutcome {
    /// Every planned call has a settled result and no unknown external effect remains.
    Completed,
    /// A fixed bound candidate requires the separate approval runtime.
    ApprovalRequired {
        /// Call whose immutable candidate was saved.
        call_id: Id,
        /// Current safe policy reason.
        reason: Id,
        /// Exact saved candidate; approval cannot rebind its system inputs.
        bound_input_ref: RecordRef,
        /// Identity of the final model-and-system argument binding.
        binding_digest: JsonDigest,
    },
    /// A no-effect input tool has saved its question and original attempt.
    InputRequired {
        /// Stable request answered through an authorized resume command.
        request: InputRequest,
    },
    /// A prior or current attempt requires explicit effect reconciliation.
    Unresolved {
        /// Call that prevents further tool and model dispatch.
        call_id: Id,
        /// Protected uncertainty observation committed with the matching event.
        result_ref: RecordRef,
    },
}

/// Serial execution of a previously committed model tool round.
pub struct SerialToolRound {
    binding_set_id: Option<Id>,
    registry: Arc<ToolRegistry>,
    binder: Arc<InputBinder>,
    policy: Arc<PolicyGate>,
    ids: Arc<dyn IdSource>,
    limits: ToolExecutionLimits,
    hooks: Option<Arc<HookRuntime>>,
    skill_plan: Option<SkillPlan>,
    artifacts: Option<Arc<ArtifactRuntime>>,
    observer_error: std::sync::Mutex<Option<ContractError>>,
}
impl SerialToolRound {
    /// Inject existing bindings; no tool is run or looked up externally here.
    pub fn new(
        registry: Arc<ToolRegistry>,
        binder: Arc<InputBinder>,
        policy: Arc<PolicyGate>,
        ids: Arc<dyn IdSource>,
    ) -> Self {
        Self {
            binding_set_id: None,
            registry,
            binder,
            policy,
            ids,
            limits: ToolExecutionLimits::default(),
            hooks: None,
            skill_plan: None,
            artifacts: None,
            observer_error: std::sync::Mutex::new(None),
        }
    }
    /// Pin the Host runtime segment identity forwarded to wrapped exports.
    pub fn with_binding_set_id(mut self, binding_set_id: Id) -> Self {
        self.binding_set_id = Some(binding_set_id);
        self
    }
    /// Require finite nonzero timeout and receipt limits.
    pub fn with_limits(mut self, limits: ToolExecutionLimits) -> Result<Self, ContractError> {
        if limits.timeout_ms == 0 || limits.timeout_ms > 86_400_000 || limits.max_receipt_bytes == 0
        {
            return Err(error(ErrorCode::InvalidConfiguration, "tools.limits"));
        }
        self.limits = limits;
        Ok(self)
    }
    /// Connect the pinned lifecycle runtime without running a callback.
    pub fn with_hooks(mut self, hooks: Arc<HookRuntime>) -> Self {
        self.hooks = Some(hooks);
        self
    }
    /// Connect the already validated, immutable Skill loader plan for this Run.
    pub fn with_skill_plan(mut self, plan: SkillPlan) -> Self {
        self.skill_plan = Some(plan);
        self
    }
    /// Validate typed artifact/evidence observations through a scoped Host store.
    pub fn with_artifacts(mut self, artifacts: Arc<ArtifactRuntime>) -> Self {
        self.artifacts = Some(artifacts);
        self
    }
    pub(crate) async fn validate_artifact_result(
        &self,
        result: &mut ToolResult,
        context: &ExecutionContext,
        deadline: tokio::time::Instant,
    ) {
        if result.status != ToolResultStatus::Succeeded
            || !result.content.iter().any(|item| {
                matches!(
                    item,
                    InputContent::Artifact { .. } | InputContent::Evidence { .. }
                )
            })
        {
            return;
        }
        let checked = match &self.artifacts {
            Some(artifacts) => {
                artifacts
                    .validate_content(&result.content, context, Some(deadline))
                    .await
            }
            None => Err(error(
                ErrorCode::ComponentUnavailable,
                "tool.artifact_store",
            )),
        };
        if let Err(error) = checked {
            result.status = if error.code == ErrorCode::Cancelled {
                ToolResultStatus::Cancelled
            } else {
                ToolResultStatus::Failed
            };
            result.content.clear();
            result.skill_ref = None;
            result.error = Some(Failure {
                code: Id::new(
                    serde_json::to_value(error.code)
                        .ok()
                        .and_then(|v| v.as_str().map(str::to_owned))
                        .unwrap_or_else(|| "invalid_artifact".into()),
                )
                .expect("error code"),
                diagnostic_ref: None,
            });
        }
    }
    /// A local observer-report persistence error, separate from Tool execution.
    pub fn observer_error(&self) -> Option<ContractError> {
        self.observer_error
            .lock()
            .ok()
            .and_then(|error| error.clone())
    }
}
fn error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReconciliationRecord {
    pub schema_version: String,
    pub scope: Scope,
    pub run_id: Id,
    pub call_id: Id,
    pub attempt_id: Id,
    pub idempotency_key: Id,
    pub binding_ref: RecordRef,
    pub recovery_attempt_id: Id,
    pub result_ref: RecordRef,
    pub correction_message_id: Id,
    pub actor_ref: Id,
    pub capability_grant_ref: Id,
}

pub(crate) use round::call_message;
