//! Versioned application stop proposals, separate from core execution authority.
use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt, sync::Arc};

/// Host-owned business-state schema, never a model instruction or permission grant.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppStateSchema {
    /// Exact namespace accepted from the policy.
    pub namespace: Id,
    /// JSON Schema for the entire {namespace, status, metadata} object.
    pub schema: Value,
}
impl fmt::Debug for AppStateSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppStateSchema")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}
/// Trusted runtime callback plus its nonsecret, admission-pinned configuration.
pub struct InterruptionPolicyBinding {
    /// Implementation whose identity must remain available on resume.
    pub policy: Arc<dyn InterruptionPolicy>,
    /// Nonsecret data delivered to the callback through InterruptionInfo.
    pub configuration: JsonObject,
    /// Required when the callback supplies application state.
    pub app_state_schema: Option<AppStateSchema>,
    /// Callback bound; the Host's smaller runtime cap takes precedence.
    pub timeout_ms: std::num::NonZeroU64,
}
/// Protected immutable configuration of a Run's interruption behavior.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InterruptionPlan {
    schema_version: String,
    /// Exact callback or built-in default identity.
    pub policy: VersionedRef,
    /// Fixed nonsecret callback configuration.
    pub configuration: JsonObject,
    /// Fixed business-state validation contract.
    pub app_state_schema: Option<AppStateSchema>,
    /// Admission-selected callback timeout in milliseconds.
    pub timeout_ms: u64,
}
impl fmt::Debug for InterruptionPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InterruptionPlan")
            .field("policy", &self.policy)
            .field("timeout_ms", &self.timeout_ms)
            .finish_non_exhaustive()
    }
}
impl InterruptionPlan {
    pub(crate) fn capture(
        binding: Option<&InterruptionPolicyBinding>,
        cap_ms: u64,
    ) -> Result<Self, ContractError> {
        let (policy, configuration, app_state_schema, timeout_ms) = match binding {
            Some(binding) => {
                let identity = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    binding.policy.identity()
                }))
                .map_err(|_| invalid("interruption.policy_identity"))?;
                if identity == Self::default_identity() {
                    return Err(invalid("interruption.reserved_identity"));
                }
                (
                    identity,
                    binding.configuration.clone(),
                    binding.app_state_schema.clone(),
                    binding.timeout_ms.get().min(cap_ms),
                )
            }
            None => (
                Self::default_identity(),
                JsonObject::new(),
                None,
                1000.min(cap_ms),
            ),
        };
        let plan = Self {
            schema_version: "wickle.interruption-plan.v1".into(),
            policy,
            configuration,
            app_state_schema,
            timeout_ms,
        };
        plan.validate()?;
        Ok(plan)
    }
    /// Validate a decoded plan before using it; identities are not execution permission.
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.schema_version != "wickle.interruption-plan.v1"
            || self.timeout_ms == 0
            || serde_json::to_vec(self)
                .map_err(|_| invalid("interruption.plan"))?
                .len()
                > 65_536
        {
            return Err(invalid("interruption.plan"));
        }
        if self.policy == Self::default_identity()
            && (!self.configuration.is_empty() || self.app_state_schema.is_some())
        {
            return Err(invalid("interruption.default_configuration"));
        }
        if let Some(schema) = &self.app_state_schema {
            crate::tool_schema::compile_validator(&schema.schema)?;
        }
        Ok(())
    }
    /// Check the complete application state against the Host's pinned schema.
    pub fn validate_app_state(&self, state: &AppState) -> Result<(), ContractError> {
        let schema = self
            .app_state_schema
            .as_ref()
            .ok_or_else(|| invalid("interruption.app_state_schema"))?;
        if state.namespace != schema.namespace {
            return Err(invalid("interruption.app_state_namespace"));
        }
        let value = serde_json::to_value(state).map_err(|_| invalid("interruption.app_state"))?;
        if value.to_string().len() > 16_384
            || !crate::tool_schema::compile_validator(&schema.schema)?.is_valid(&value)
        {
            return Err(invalid("interruption.app_state"));
        }
        Ok(())
    }
    pub(crate) fn default_identity() -> VersionedRef {
        VersionedRef {
            id: Id::new("wickle-default-interruption").expect("constant"),
            version: Id::new("1").expect("constant"),
        }
    }
}
/// Durable facts about a stop decision, including safe callback fallback diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InterruptionDecisionRecord {
    /// Protected record format.
    pub schema_version: String,
    /// Exact namespace and owning Run.
    pub scope: Scope,
    /// Owning Run.
    pub run_id: Id,
    /// Admission-pinned policy definition.
    pub plan_ref: RecordRef,
    /// Cause, checkpoint, segment and unresolved effects observed by the core.
    pub interruption: InterruptionRecord,
    /// Accepted action after protected-cause checks.
    pub action: InterruptionAction,
    /// Business state selected for persistence, independent of core status.
    pub app_state: Option<AppState>,
    /// Safe callback failure code; raw panic/error data is not persisted here.
    pub callback_error: Option<Id>,
}
/// Delivery of a local execution stop, not an assertion of stored completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStopReceipt {
    /// The owned driver was signaled; await its saved outcome.
    Requested,
    /// This execution interval has already stopped or settled.
    AlreadySettled,
    /// This process does not own the active driver.
    NotLocal,
}
fn invalid(path: &str) -> ContractError {
    ContractError::new(ErrorCode::InvalidConfiguration, path)
}
