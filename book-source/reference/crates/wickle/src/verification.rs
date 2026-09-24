//! Scoped output contracts and versioned, read-only candidate verification.
use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt, sync::Arc};
use tokio_util::sync::CancellationToken;

/// A complete immutable output schema supplied by the Host.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputSchemaDefinition {
    /// Exact schema identity.
    pub schema_ref: VersionedRef,
    /// JSON Schema, validated without remote reference resolution.
    pub schema: Value,
}
/// Immutable verifier identity and evaluation criteria.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifierDefinition {
    /// Exact implementation/configuration identity.
    pub verifier_ref: VersionedRef,
    /// Exact criteria version recorded with every verdict.
    pub criteria_ref: VersionedRef,
    /// Nonsecret criteria description, pinned with the Run.
    pub criteria: String,
    /// Complete nonsecret runtime configuration, pinned for recovery.
    #[serde(default)]
    pub configuration: JsonObject,
}
/// Candidate saved before invoking the verifier.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationCandidate {
    /// Owning namespace.
    pub scope: Scope,
    /// Owning Run.
    pub run_id: Id,
    /// Agent model step that produced this candidate.
    pub model_step_id: Id,
    /// Exact complete response record, including its route identity.
    pub response_ref: RecordRef,
    /// Transcript boundary observed when the candidate was saved.
    pub through_sequence: u64,
    /// Immutable Tool observations supplied as evidence to this check.
    pub evidence_message_ids: Vec<Id>,
    /// Parsed output; invalid structured candidates retain their original text.
    pub output: Vec<InputContent>,
    /// Format failure, separate from business quality.
    pub format_error: Option<Id>,
}
impl fmt::Debug for VerificationCandidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("VerificationCandidate(<protected>)")
    }
}
/// Read-only input to a verifier; Tool execution inputs and receipts are excluded.
#[derive(Clone)]
pub struct VerificationInput {
    /// Saved candidate identity, also used to bind human review.
    pub candidate_ref: RecordRef,
    /// Candidate and its exact source.
    pub candidate: VerificationCandidate,
    /// Original user request, without changing its provenance.
    pub request: Vec<InputContent>,
    /// Model-visible observations from completed tools.
    pub evidence: Vec<InputContent>,
}
/// A verifier's quality decision. Transport errors use the Result error channel.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "verdict", rename_all = "snake_case", deny_unknown_fields)]
pub enum VerificationDecision {
    /// Criteria met.
    Pass {},
    /// Request another candidate within the repair budget.
    Revise {
        /// Bounded feedback, never promoted to Host instructions.
        feedback: String,
    },
    /// Review the exact saved candidate before continuing.
    Wait {
        /// Description of the review requested.
        reason: String,
        /// Optional UTC deadline, still bounded by the Run deadline.
        expires_at_ms: Option<i64>,
    },
    /// Definitive quality rejection.
    Fail {
        /// Bounded quality failure reason.
        reason: String,
    },
}
impl VerificationDecision {
    /// Classification recorded separately from failure to contact the verifier.
    pub fn verdict(&self) -> VerificationVerdict {
        match self {
            Self::Pass {} => VerificationVerdict::Pass,
            Self::Revise { .. } => VerificationVerdict::Revise,
            Self::Wait { .. } => VerificationVerdict::Wait,
            Self::Fail { .. } => VerificationVerdict::Fail,
        }
    }
}
/// Current scope and cancellation for a read-only verification call.
pub struct VerifierContext<'a> {
    /// Current authenticated execution identity.
    pub execution: &'a ExecutionContext,
    /// Cooperative cancellation, cancelled when the call leaves its boundary.
    pub cancellation: CancellationToken,
    /// Finite effective deadline.
    pub deadline: tokio::time::Instant,
    /// Budgeted model access; implementations must not make hidden model calls.
    pub models: &'a dyn VerificationModel,
}
/// Model access supplied by the core, using Verification purpose and the Run budget.
pub trait VerificationModel: Send + Sync {
    /// Generate a text-only review with no business tools on an explicit logical binding.
    fn generate<'a>(&'a self, request: VerificationModelRequest) -> PortFuture<'a, String>;
}
/// A verifier-owned review request, routed and budgeted by the core.
#[derive(Debug, Clone)]
pub struct VerificationModelRequest {
    /// Stable local stage name for replay; different input requires a different stage.
    pub stage: Id,
    /// Logical binding with an explicit Verification routing rule.
    pub model_binding: Id,
    /// Review messages composed from approved criteria and candidate data.
    pub messages: Vec<ModelMessage>,
    /// Purpose-specific inference overrides; None uses only the selected binding defaults.
    pub options: Option<JsonObject>,
    /// Finite output-token reservation.
    pub max_output_tokens: std::num::NonZeroU64,
}
/// An approved read-only verifier. It cannot execute tools or mutate Run state.
pub trait Verifier: Send + Sync {
    /// Pure metadata, cached when the runtime is created.
    fn definition(&self) -> VerifierDefinition;
    /// Evaluate a fixed candidate; use context.models for all model inference.
    fn verify<'a>(
        &'a self,
        input: &'a VerificationInput,
        context: &'a VerifierContext<'a>,
    ) -> PortFuture<'a, VerificationDecision>;
}
/// Finite local bounds in addition to the Run's global budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationLimits {
    /// Maximum duration of one complete verifier invocation.
    pub timeout_ms: u64,
    /// Maximum serialized candidate plus evidence passed to a callback.
    pub max_input_bytes: usize,
    /// Maximum UTF-8 feedback or reason size.
    pub max_feedback_bytes: usize,
}
impl Default for VerificationLimits {
    fn default() -> Self {
        Self {
            timeout_ms: 30_000,
            max_input_bytes: 1_048_576,
            max_feedback_bytes: 16_384,
        }
    }
}
/// Scope-bound immutable output schemas and approved verifier implementations.
pub struct VerificationRuntime {
    pub(crate) scope: Scope,
    schemas: Vec<OutputSchemaDefinition>,
    verifiers: Vec<(VerifierDefinition, Arc<dyn Verifier>)>,
    pub(crate) limits: VerificationLimits,
}
/// Exact admitted output contract and verifier definition, without runtime objects.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationPlan {
    pub(crate) schema_version: String,
    pub(crate) scope: Scope,
    pub(crate) output: OutputContract,
    pub(crate) schema: Option<OutputSchemaDefinition>,
    pub(crate) verifier: Option<VerifierDefinition>,
    pub(crate) limits: VerificationLimits,
}
/// Protected result of one candidate verification attempt.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerificationRecord {
    pub schema_version: String,
    pub scope: Scope,
    pub run_id: Id,
    pub candidate_ref: RecordRef,
    pub decision: Option<VerificationDecision>,
    pub error: Option<VerificationFailure>,
    pub summary: Option<VerificationSummary>,
    pub summary_ref: Option<RecordRef>,
    pub review_command_ref: Option<RecordRef>,
    pub repair_ref: Option<Id>,
}
impl VerificationRuntime {
    /// Validate and cache metadata; never invoke a verifier or read environment settings.
    pub fn new(
        scope: Scope,
        schemas: Vec<OutputSchemaDefinition>,
        verifiers: Vec<Arc<dyn Verifier>>,
        limits: VerificationLimits,
    ) -> Result<Self, ContractError> {
        limits.validate()?;
        for (index, schema) in schemas.iter().enumerate() {
            if schemas[..index]
                .iter()
                .any(|other| other.schema_ref == schema.schema_ref)
            {
                return Err(verification_error(
                    ErrorCode::InvalidConfiguration,
                    "verification.duplicate_schema",
                ));
            }
            crate::tool_schema::compile_validator(&schema.schema)?;
        }
        let mut registered = vec![];
        for verifier in verifiers {
            let definition =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| verifier.definition()))
                    .map_err(|_| {
                        verification_error(
                            ErrorCode::InvalidConfiguration,
                            "verification.definition",
                        )
                    })?;
            if definition.criteria.trim().is_empty()
                || serde_json::to_vec(&definition)
                    .map_err(|_| {
                        verification_error(ErrorCode::InvalidJson, "verification.definition")
                    })?
                    .len()
                    > limits.max_input_bytes
                || registered
                    .iter()
                    .any(|(other, _): &(VerifierDefinition, Arc<dyn Verifier>)| {
                        other.verifier_ref == definition.verifier_ref
                    })
            {
                return Err(verification_error(
                    ErrorCode::InvalidConfiguration,
                    "verification.definition",
                ));
            }
            registered.push((definition, verifier));
        }
        Ok(Self {
            scope,
            schemas,
            verifiers: registered,
            limits,
        })
    }
    /// Plain text/turn-end behavior with no external verifier.
    pub fn text(scope: Scope) -> Result<Self, ContractError> {
        Self::new(scope, vec![], vec![], VerificationLimits::default())
    }
    pub(crate) fn plan(
        &self,
        profile: &AgentProfile,
        request: Option<&OutputContract>,
    ) -> Result<VerificationPlan, ContractError> {
        let output = request.unwrap_or(&profile.output_contract).clone();
        let schema = match &output {
            OutputContract::Text {} => None,
            OutputContract::JsonSchema { schema_ref } => Some(
                self.schemas
                    .iter()
                    .find(|value| &value.schema_ref == schema_ref)
                    .cloned()
                    .ok_or_else(|| {
                        verification_error(
                            ErrorCode::ComponentUnavailable,
                            "verification.output_schema",
                        )
                    })?,
            ),
        };
        let verifier = match &profile.completion_policy {
            CompletionPolicy::TurnEnd {} => None,
            CompletionPolicy::Verified { verifier_ref } => Some(
                self.verifiers
                    .iter()
                    .find(|(definition, _)| &definition.verifier_ref == verifier_ref)
                    .map(|(definition, _)| definition.clone())
                    .ok_or_else(|| {
                        verification_error(ErrorCode::ComponentUnavailable, "verification.verifier")
                    })?,
            ),
        };
        Ok(VerificationPlan {
            schema_version: "wickle.verification-plan.v1".into(),
            scope: self.scope.clone(),
            output,
            schema,
            verifier,
            limits: self.limits,
        })
    }
    pub(crate) fn verifier(
        &self,
        plan: &VerificationPlan,
    ) -> Result<&Arc<dyn Verifier>, ContractError> {
        let definition = plan.verifier.as_ref().ok_or_else(|| {
            verification_error(ErrorCode::InvalidSnapshot, "verification.verifier")
        })?;
        self.verifiers
            .iter()
            .find(|(saved, _)| saved == definition)
            .map(|(_, verifier)| verifier)
            .ok_or_else(|| verification_error(ErrorCode::ContextMismatch, "verification.verifier"))
    }
}
impl VerificationPlan {
    /// Stable identity of the complete admitted contract.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
    /// Restore and validate a stored plan against its original Run.
    pub fn restore(
        record: &ProtectedRecord,
        snapshot: &RunSnapshot,
    ) -> Result<Self, ContractError> {
        let value: Self = serde_json::from_value(record.value().clone())
            .map_err(|_| verification_error(ErrorCode::InvalidSnapshot, "verification.plan"))?;
        if value.schema_version != "wickle.verification-plan.v1"
            || value.scope != snapshot.scope
            || &value.output
                != snapshot
                    .request
                    .output_contract
                    .as_ref()
                    .unwrap_or(&snapshot.profile.profile().output_contract)
            || value.digest() != record.reference().digest
        {
            return Err(verification_error(
                ErrorCode::InvalidSnapshot,
                "verification.plan",
            ));
        }
        value.limits.validate()?;
        if value.verifier.as_ref().is_some_and(|definition| {
            definition.criteria.trim().is_empty()
                || serde_json::to_vec(definition)
                    .map_or(true, |bytes| bytes.len() > value.limits.max_input_bytes)
        }) {
            return Err(verification_error(
                ErrorCode::InvalidSnapshot,
                "verification.definition",
            ));
        }

        match (&value.output, &value.schema) {
            (OutputContract::Text {}, None) => {}
            (OutputContract::JsonSchema { schema_ref }, Some(schema))
                if schema_ref == &schema.schema_ref =>
            {
                crate::tool_schema::compile_validator(&schema.schema)?;
            }
            _ => {
                return Err(verification_error(
                    ErrorCode::InvalidSnapshot,
                    "verification.schema",
                ));
            }
        }
        match (
            &snapshot.profile.profile().completion_policy,
            &value.verifier,
        ) {
            (CompletionPolicy::TurnEnd {}, None) => {}
            (CompletionPolicy::Verified { verifier_ref }, Some(definition))
                if verifier_ref == &definition.verifier_ref => {}
            _ => {
                return Err(verification_error(
                    ErrorCode::InvalidSnapshot,
                    "verification.definition",
                ));
            }
        }
        Ok(value)
    }
    pub(crate) fn parse(&self, text: &str) -> Result<Vec<InputContent>, Id> {
        match &self.schema {
            None => Ok(vec![InputContent::Text { text: text.into() }]),
            Some(schema) => {
                let value =
                    parse_json(text).map_err(|_| Id::new("output_invalid_json").unwrap())?;
                let validator = crate::tool_schema::compile_validator(&schema.schema)
                    .map_err(|_| Id::new("output_invalid_schema").unwrap())?;
                if !validator.is_valid(&value) {
                    return Err(Id::new("output_schema_mismatch").unwrap());
                }
                Ok(vec![InputContent::Json { value }])
            }
        }
    }
}
pub(crate) fn verification_error(code: ErrorCode, path: &str) -> ContractError {
    ContractError::new(code, path)
}

/// A deterministic reference verifier for JSON candidate criteria.
/// This verifies the supplied data shape, not the truth of external business state.
pub struct SchemaVerifier {
    definition: VerifierDefinition,
    validator: jsonschema::Validator,
}
impl SchemaVerifier {
    /// Compile a complete criteria schema without fetching remote references.
    pub fn new(mut definition: VerifierDefinition, schema: Value) -> Result<Self, ContractError> {
        definition
            .configuration
            .insert("schema".into(), schema.clone());
        Ok(Self {
            definition,
            validator: crate::tool_schema::compile_validator(&schema)?,
        })
    }
}
impl Verifier for SchemaVerifier {
    fn definition(&self) -> VerifierDefinition {
        self.definition.clone()
    }
    fn verify<'a>(
        &'a self,
        input: &'a VerificationInput,
        _: &'a VerifierContext<'a>,
    ) -> PortFuture<'a, VerificationDecision> {
        Box::pin(async move {
            let value = match input.candidate.output.as_slice() {
                [InputContent::Json { value }] => Some(value.clone()),
                [InputContent::Text { text }] => parse_json(text).ok(),
                _ => None,
            };
            if value
                .as_ref()
                .is_some_and(|value| self.validator.is_valid(value))
            {
                Ok(VerificationDecision::Pass {})
            } else {
                Ok(VerificationDecision::Revise {
                    feedback: "The candidate does not satisfy the registered verification schema."
                        .into(),
                })
            }
        })
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerificationFailure {
    pub code: ErrorCode,
    pub path: String,
}
impl From<&ContractError> for VerificationFailure {
    fn from(error: &ContractError) -> Self {
        Self {
            code: error.code,
            path: error.path.clone(),
        }
    }
}

impl VerificationLimits {
    pub(crate) fn validate(self) -> Result<(), ContractError> {
        if self.timeout_ms == 0
            || self.timeout_ms > 86_400_000
            || self.max_input_bytes == 0
            || self.max_input_bytes > 64 * 1024 * 1024
            || self.max_feedback_bytes == 0
            || self.max_feedback_bytes > self.max_input_bytes
        {
            return Err(verification_error(
                ErrorCode::InvalidConfiguration,
                "verification.limits",
            ));
        }
        Ok(())
    }
}
