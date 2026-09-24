use super::*;
use std::panic::AssertUnwindSafe;

impl ContextRewriteLimits {
    pub(super) fn validate(self) -> Result<(), ContractError> {
        if self.max_prepared_bytes == 0
            || self.max_prepared_bytes > 64 * 1024 * 1024
            || self.max_compactor_input_bytes == 0
            || self.max_compactor_input_bytes > self.max_prepared_bytes
            || self.max_summary_bytes == 0
            || self.max_summary_bytes > self.max_compactor_input_bytes
            || self.preview_above_bytes == 0
            || self.preview_above_bytes > self.max_prepared_bytes
            || self.max_previews > 128
            || self.max_compactions == 0
            || self.max_compactions > 64
            || self.timeout_ms == 0
            || self.timeout_ms > 86_400_000
        {
            return Err(context_error(
                ErrorCode::InvalidConfiguration,
                "context.limits",
            ));
        }
        Ok(())
    }
}
impl ContextRuntime {
    pub(crate) fn validate_router(&self, routing: &RoutingSnapshot) -> Result<(), ContractError> {
        if routing.scope() != &self.scope {
            return Err(context_error(
                ErrorCode::AccessDenied,
                "context.routing_scope",
            ));
        }
        if let Some(ContextCompactor::Model(config)) = &self.compactor {
            if !routing.policy().rules.iter().any(|rule| {
                rule.model_binding == config.model_binding
                    && rule.purpose == ModelPurpose::Compaction
            }) {
                return Err(context_error(
                    ErrorCode::ModelRouteDenied,
                    "context.compaction_route",
                ));
            }
        }
        Ok(())
    }
    pub(crate) async fn validate_session_plan(
        &self,
        reference: &RecordRef,
        session: &Id,
        profile: &ResolvedProfile,
        current: &ContextPlan,
        state: &dyn StateStore,
    ) -> Result<(), ContractError> {
        let record = state.read_record(&self.scope, reference).await?;
        let revision: ContextRevision = serde_json::from_value(record.value().clone())
            .map_err(|_| context_error(ErrorCode::InvalidSnapshot, "context.revision"))?;
        if record.reference() != reference
            || revision.scope != self.scope
            || &revision.session_id != session
        {
            return Err(context_error(ErrorCode::InvalidSnapshot, "context.session"));
        }
        let record = state.read_record(&self.scope, &revision.plan_ref).await?;
        if ContextPlan::restore(&record, profile)?.digest() != current.digest() {
            return Err(context_error(
                ErrorCode::ContextMismatch,
                "context.session_plan",
            ));
        }
        Ok(())
    }
    /// Cache approved strategy metadata; this does not invoke selection, compression, or storage.
    pub fn new(
        scope: Scope,
        strategy: Arc<dyn ContextStrategy>,
        compactor: Option<ContextCompactor>,
        limits: ContextRewriteLimits,
    ) -> Result<Self, ContractError> {
        limits.validate()?;
        let definition = std::panic::catch_unwind(AssertUnwindSafe(|| strategy.definition()))
            .map_err(|_| context_error(ErrorCode::InvalidConfiguration, "context.strategy"))?;
        crate::tool_schema::compile_validator(&definition.config_schema)?;
        Ok(Self {
            scope,
            definition,
            strategy,
            compactor,
            limits,
        })
    }
    /// Default bounded selection and preview behavior without a compressor.
    pub fn bounded(scope: Scope) -> Result<Self, ContractError> {
        Self::new(
            scope,
            Arc::new(BoundedContextStrategy),
            None,
            ContextRewriteLimits::default(),
        )
    }
    /// Registered strategy metadata for a Host ProfileResolver when an explicit version is used.
    pub fn metadata(&self) -> ComponentMetadata {
        ComponentMetadata {
            reference: ComponentRef {
                kind: ComponentKind::ContextStrategy,
                id: self.definition.strategy.id.clone(),
                version: Some(self.definition.strategy.version.clone()),
            },
            contract_version: 1,
            manifest_digest: crate::serialization::data_digest(&self.definition),
            config_schema: self.definition.config_schema.clone(),
            dependencies: vec![],
            capabilities: Default::default(),
            required_capabilities: Default::default(),
            required_connections: Default::default(),
            model_name: None,
            hook_position: None,
            exports: vec![],
        }
    }
    pub(crate) fn plan(
        &self,
        profile: &AgentProfile,
        scope: &Scope,
    ) -> Result<ContextPlan, ContractError> {
        if scope != &self.scope {
            return Err(context_error(ErrorCode::AccessDenied, "context.scope"));
        }
        let plan = ContextPlan {
            schema_version: "wickle.context-plan.v1".into(),
            scope: scope.clone(),
            policy: profile.context_policy.clone(),
            strategy: self.definition.clone(),
            compactor: self.compactor.as_ref().map(|compactor| match compactor {
                ContextCompactor::Model(config) => CompactorIdentity::Model {
                    config: config.clone(),
                },
                ContextCompactor::Host { definition, .. } => CompactorIdentity::Host {
                    definition: definition.clone(),
                },
            }),
            limits: self.limits,
        };
        plan.validate(profile)?;
        Ok(plan)
    }
}
impl ContextPlan {
    /// Stable identity of the exact selector, compressor and limits.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
    pub(super) fn validate(&self, profile: &AgentProfile) -> Result<(), ContractError> {
        self.limits.validate()?;
        if self.schema_version != "wickle.context-plan.v1"
            || self.policy != profile.context_policy
            || self.policy.strategy != self.strategy.strategy.id
            || self.policy.version.as_ref().map_or(
                self.strategy.strategy.id.as_str() != "bounded"
                    || self.strategy.strategy.version.as_str() != "1",
                |version| version != &self.strategy.strategy.version,
            )
            || !crate::tool_schema::compile_validator(&self.strategy.config_schema)?.is_valid(
                &serde_json::to_value(self.policy.config.clone().unwrap_or_default())
                    .map_err(|_| context_error(ErrorCode::InvalidJson, "context.config"))?,
            )
        {
            return Err(context_error(
                ErrorCode::InvalidConfiguration,
                "context.plan",
            ));
        }
        Ok(())
    }
    /// Restore a protected plan against the owning profile and metadata version.
    pub fn restore(
        record: &ProtectedRecord,
        profile: &ResolvedProfile,
    ) -> Result<Self, ContractError> {
        let plan: Self = serde_json::from_value(record.value().clone())
            .map_err(|_| context_error(ErrorCode::InvalidSnapshot, "context.plan"))?;
        plan.validate(profile.profile())?;
        if &plan.scope != profile.scope() || plan.digest() != record.reference().digest {
            return Err(context_error(
                ErrorCode::InvalidSnapshot,
                "context.plan_identity",
            ));
        }
        if let Some(version) = &plan.policy.version {
            let metadata = ComponentMetadata {
                reference: ComponentRef {
                    kind: ComponentKind::ContextStrategy,
                    id: plan.policy.strategy.clone(),
                    version: Some(version.clone()),
                },
                contract_version: 1,
                manifest_digest: crate::serialization::data_digest(&plan.strategy),
                config_schema: plan.strategy.config_schema.clone(),
                dependencies: vec![],
                capabilities: Default::default(),
                required_capabilities: Default::default(),
                required_connections: Default::default(),
                model_name: None,
                hook_position: None,
                exports: vec![],
            };
            if !profile.components().iter().any(|component| {
                component.reference == metadata.reference
                    && component.definition_digest == crate::serialization::data_digest(&metadata)
            }) {
                return Err(context_error(
                    ErrorCode::ContextMismatch,
                    "context.strategy_metadata",
                ));
            }
        }
        Ok(plan)
    }
}
