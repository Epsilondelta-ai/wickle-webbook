use super::*;
use futures_util::FutureExt;
use std::{panic::AssertUnwindSafe, time::Duration};

impl SkillRuntime {
    /// Build an immutable scoped registry without invoking a resolver or store.
    pub fn new(
        bindings: SkillBindings,
        definitions: Vec<SkillDefinition>,
        loader: ToolBindingRef,
        limits: SkillLimits,
    ) -> Result<Self, ContractError> {
        limits.validate()?;
        if definitions.len() > 64 {
            return Err(skill_error(ErrorCode::InvalidConfiguration, "skills.count"));
        }
        let mut seen = BTreeSet::new();
        for definition in &definitions {
            definition.validate(&bindings.scope, limits)?;
            if !seen.insert((
                definition.skill.id.clone(),
                definition.skill.version.clone(),
            )) || !definition.assets.is_empty() && bindings.artifacts.is_none()
            {
                return Err(skill_error(
                    ErrorCode::InvalidConfiguration,
                    "skills.registry",
                ));
            }
        }
        let compiled = SchemaCompiler::new().compile(
            loader_descriptor(limits)?,
            &SystemInputRegistry::new(vec![])?,
        )?;
        if let ToolBindingRef::Catalog(reference) = &loader {
            if reference.tool_id != compiled.descriptor().tool.id
                || reference.version != compiled.descriptor().tool.version
                || reference.bindings.as_ref().is_some_and(|v| !v.is_empty())
                || reference.config.as_ref().is_some_and(|v| !v.is_empty())
            {
                return Err(skill_error(
                    ErrorCode::InvalidConfiguration,
                    "skills.loader",
                ));
            }
        }
        Ok(Self {
            bindings,
            definitions,
            loader,
            compiled,
            limits,
        })
    }
    /// Standard catalog selection. The profile must explicitly include this Tool.
    pub fn catalog_loader() -> ToolBindingRef {
        ToolBindingRef::Catalog(CatalogToolRef {
            tool_id: Id::new("wickle.skills.load").expect("constant ID"),
            version: Id::new("1").expect("constant ID"),
            bindings: None,
            config: None,
        })
    }
    /// Owning namespace.
    pub fn scope(&self) -> &Scope {
        &self.bindings.scope
    }
    /// Compile-time-validated descriptor for a catalog registration or adapter export.
    pub fn loader_descriptor(&self) -> &ToolDescriptor {
        self.compiled.descriptor()
    }
    /// Existing loader implementation, ready for the Host's Tool registry or adapter factory.
    pub fn loader_tool(self: &Arc<Self>) -> ToolRegistration {
        ToolRegistration {
            compiled: self.compiled.clone(),
            executor: Arc::new(Loader(self.clone())),
        }
    }
    /// Metadata for the native loader and selected Skill definitions.
    pub fn component_metadata(&self, reference: &ComponentRef) -> Option<ComponentMetadata> {
        if reference.kind == ComponentKind::Tool
            && reference.id == self.compiled.descriptor().tool.id
            && reference.version.as_ref() == Some(&self.compiled.descriptor().tool.version)
        {
            return Some(ComponentMetadata {
                reference: reference.clone(),
                contract_version: 1,
                manifest_digest: self.compiled.descriptor_digest().clone(),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::from([Id::new("skill_loading").expect("constant ID")]),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: Some(self.compiled.descriptor().name.clone()),
                hook_position: None,
                exports: vec![],
            });
        }
        self.definitions
            .iter()
            .find(|definition| {
                reference.kind == ComponentKind::Skill
                    && reference.id == definition.skill.id
                    && reference.version.as_ref() == Some(&definition.skill.version)
            })
            .map(SkillDefinition::metadata)
    }
    /// Pin manifests and attest required capabilities against selected Tool metadata.
    /// Metadata may be read here; instruction bodies and supporting bytes are not loaded.
    pub async fn plan(
        &self,
        profile: &ResolvedProfile,
        tools: &[PromptToolBinding],
        resolver: &dyn ProfileResolver,
    ) -> Result<SkillPlan, ContractError> {
        if profile.scope() != self.scope() {
            return Err(skill_error(ErrorCode::AccessDenied, "skills.scope"));
        }
        let loader = tools
            .iter()
            .find(|tool| tool.selection == self.loader)
            .ok_or_else(|| skill_error(ErrorCode::ComponentUnavailable, "skills.loader_missing"))?;
        if loader.compiled.digest() != self.compiled.digest() {
            return Err(skill_error(
                ErrorCode::ContextMismatch,
                "skills.loader_contract",
            ));
        }
        let mut metadata = vec![];
        for selection in &profile.profile().tools {
            let reference = tool_component(profile.profile(), selection)?;
            if !metadata
                .iter()
                .any(|m: &ComponentMetadata| m.reference == reference)
            {
                let resolved = resolver.resolve(&reference, self.scope()).await?;
                attest(profile, &resolved)?;
                metadata.push(resolved);
            }
        }
        let capabilities = tool_capabilities(profile, &metadata)?;
        let mut skills = vec![];
        for selection in &profile.profile().skills {
            let definition = self
                .definitions
                .iter()
                .find(|definition| {
                    definition.skill.id == selection.skill_id
                        && definition.skill.version == selection.version
                })
                .ok_or_else(|| skill_error(ErrorCode::ComponentUnavailable, "skills.definition"))?;
            let reference = ComponentRef {
                kind: ComponentKind::Skill,
                id: selection.skill_id.clone(),
                version: Some(selection.version.clone()),
            };
            let metadata = resolver.resolve(&reference, self.scope()).await?;
            attest(profile, &metadata)?;
            if metadata.manifest_digest != definition.digest()
                || !definition
                    .required_tool_capabilities
                    .is_subset(&capabilities)
            {
                return Err(skill_error(
                    ErrorCode::CapabilityUnsupported,
                    "skills.required_tools",
                ));
            }
            skills.push(PlannedSkill {
                selection: selection.clone(),
                definition: definition.clone(),
                metadata,
            });
        }
        let plan = SkillPlan {
            schema_version: "wickle.skill-plan.v1".into(),
            scope: self.scope().clone(),
            loader: self.loader.clone(),
            loader_descriptor_digest: self.compiled.descriptor_digest().clone(),
            skills,
            tool_capabilities: capabilities,
            tool_metadata: metadata,
            limits: self.limits,
        };
        let record = ProtectedRecord::new(
            Id::new("validation")?,
            1,
            serde_json::to_value(&plan)
                .map_err(|_| skill_error(ErrorCode::InvalidJson, "skills.plan"))?,
        );
        SkillPlan::restore(&record, profile)
    }
    pub(crate) fn validate_current(&self, plan: &SkillPlan) -> Result<(), ContractError> {
        if plan.scope != self.bindings.scope
            || plan.loader != self.loader
            || plan.loader_descriptor_digest != *self.compiled.descriptor_digest()
            || plan.limits != self.limits
            || plan
                .skills
                .iter()
                .any(|entry| !self.definitions.contains(&entry.definition))
        {
            return Err(skill_error(
                ErrorCode::ContextMismatch,
                "skills.pinned_definitions",
            ));
        }
        Ok(())
    }
    pub(crate) async fn saved_plan(
        &self,
        snapshot: &RunSnapshot,
    ) -> Result<SkillPlan, ContractError> {
        if snapshot.scope != self.bindings.scope {
            return Err(skill_error(ErrorCode::AccessDenied, "skills.scope"));
        }
        let reference = snapshot
            .skill_plan_ref
            .as_ref()
            .ok_or_else(|| skill_error(ErrorCode::InvalidSnapshot, "skills.plan_missing"))?;
        let record = self
            .bindings
            .state
            .read_record(self.scope(), reference)
            .await?;
        if record.reference() != reference {
            return Err(skill_error(
                ErrorCode::InvalidSnapshot,
                "skills.plan_reference",
            ));
        }
        let plan = SkillPlan::restore(&record, &snapshot.profile)?;
        self.validate_current(&plan)?;
        Ok(plan)
    }
    fn controls(
        &self,
        run_id: &Id,
        context: &ExecutionContext,
        deadline: tokio::time::Instant,
    ) -> SkillCallContext {
        SkillCallContext {
            scope: context.data.scope.clone(),
            run_id: run_id.clone(),
            principal_ref: context.data.principal_ref.clone(),
            capability_grant_ref: context.data.capability_grant_ref.clone(),
            cancellation: context.cancellation.child_token(),
            deadline: deadline
                .min(tokio::time::Instant::now() + Duration::from_millis(self.limits.timeout_ms)),
        }
    }
    async fn authorize(
        &self,
        skill: &VersionedRef,
        digest: &JsonDigest,
        context: &ExecutionContext,
        controls: &SkillCallContext,
        route: Option<&ResolvedModelRoute>,
    ) -> Result<(), ContractError> {
        if controls.scope != self.bindings.scope {
            return Err(skill_error(ErrorCode::AccessDenied, "skills.scope"));
        }
        let request = PolicyRequest {
            owner_scope: self.scope().clone(),
            resource_id: skill.id.clone(),
            action: PolicyAction::ReadSkill {
                skill: skill.clone(),
                manifest_digest: digest.clone(),
                route: route.cloned().map(Box::new),
            },
        };
        match self
            .bindings
            .policy
            .check(&request, context, Some(controls.deadline), None)
            .await?
        {
            PolicyDecision::Allow {} => Ok(()),
            PolicyDecision::RequireApproval { .. } => Err(skill_error(
                ErrorCode::SkillApprovalRequired,
                "skills.policy",
            )),
            PolicyDecision::Deny { .. } => {
                Err(skill_error(ErrorCode::AccessDenied, "skills.policy"))
            }
        }
    }
    async fn assets(
        &self,
        assets: &[ArtifactRef],
        context: &ExecutionContext,
        deadline: tokio::time::Instant,
    ) -> Result<(), ContractError> {
        if !assets.is_empty() {
            let runtime =
                self.bindings.artifacts.as_ref().ok_or_else(|| {
                    skill_error(ErrorCode::ComponentUnavailable, "skills.artifacts")
                })?;
            for reference in assets {
                runtime.stat(reference, context, Some(deadline)).await?;
            }
        }
        Ok(())
    }
    async fn load(
        &self,
        args: &JsonObject,
        execution: &ToolExecutionContext,
    ) -> Result<LoadedSkill, ContractError> {
        if execution.scope != self.bindings.scope {
            return Err(skill_error(ErrorCode::AccessDenied, "skills.scope"));
        }
        let context = ExecutionContext::new(
            ExecutionContextData {
                scope: execution.scope.clone(),
                principal_ref: execution.principal_ref.clone(),
                capability_grant_ref: execution.capability_grant_ref.clone(),
                trace_context: None,
                system_inputs: None,
            },
            execution.cancellation.clone(),
        );
        let controls = self.controls(&execution.run_id, &context, execution.deadline);
        let _cancel = controls.cancellation.clone().drop_guard();
        bounded(&controls,async {
            let saved=self.bindings.state.load(self.scope(),&execution.run_id).await?;
            let plan=self.saved_plan(&saved.snapshot).await?;
            let call=saved.snapshot.tool_ledger.iter().find(|entry|entry.call.call_id==execution.call_id).ok_or_else(||skill_error(ErrorCode::InvalidSnapshot,"skills.call"))?;
            if call.call.descriptor_digest.as_ref()!=Some(&plan.loader_descriptor_digest) || !matches!(&call.state,ToolCallState::Dispatching {attempt_id,idempotency_key} if attempt_id==&execution.attempt_id&&idempotency_key==&execution.idempotency_key) { return Err(skill_error(ErrorCode::AccessDenied,"skills.loader_call")); }
            let reference=call.call.bound_input_ref.as_ref().ok_or_else(||skill_error(ErrorCode::InvalidSnapshot,"skills.bound_input"))?;
            let record=self.bindings.state.read_record(self.scope(),reference).await?;
            let bound=BoundToolInput::restore(&record,&self.compiled,self.scope(),&execution.run_id,&call.call,saved.snapshot.system_inputs.as_ref())?;
            if bound.execution_args()!=args {return Err(skill_error(ErrorCode::AccessDenied,"skills.bound_arguments"));}
            let selected=plan.skills.iter().find(|entry|args.get("skill_id").and_then(Value::as_str)==Some(entry.selection.skill_id.as_str())&&args.get("version").and_then(Value::as_str)==Some(entry.selection.version.as_str())).ok_or_else(||skill_error(ErrorCode::InvalidSkill,"skills.requested_version"))?;
            self.authorize(&selected.definition.skill,&selected.definition.digest(),&context,&controls,None).await?;
            self.assets(&selected.definition.assets,&context,controls.deadline).await?;
            let mut cached=None;
            let mut seen=BTreeSet::new();
            let mut total=0u64;
            for entry in &saved.snapshot.tool_ledger {
                let ToolCallState::Settled {result}=&entry.state else {continue};
                let Some(reference)=&result.skill_ref else {continue};
                let record=self.bindings.state.read_record(self.scope(),reference).await?;
                let prior=records::loaded(&plan,&saved.snapshot,entry,&record)?;
                if seen.insert(crate::serialization::data_digest(&prior.selection)) {total+=prior.body.len() as u64;}
                if prior.selection==selected.selection {cached=Some(prior);}
            }
            if let Some(mut prior)=cached {
                self.bindings.resolver.authorize_use(&prior,&controls).await?;
                self.authorize(&selected.definition.skill,&selected.definition.digest(),&context,&controls,None).await?;
                prior.call_id=execution.call_id.clone();
                return Ok(prior);
            }
            if total.saturating_add(selected.definition.body_bytes)>self.limits.max_total_body_bytes {
                return Err(skill_error(ErrorCode::ContextBudgetExceeded,"skills.total_body"));
            }
            let body=self.bindings.resolver.load(&selected.selection,&selected.definition,&controls).await?;
            let loaded=LoadedSkill {schema_version:"wickle.loaded-skill.v1".into(),scope:self.scope().clone(),run_id:execution.run_id.clone(),call_id:execution.call_id.clone(),selection:selected.selection.clone(),definition_digest:selected.definition.digest(),body,assets:selected.definition.assets.clone()};
            loaded.validate(&plan,&execution.run_id,&execution.call_id)?;
            self.authorize(&selected.definition.skill,&selected.definition.digest(),&context,&controls,None).await?;
            Ok(loaded)
        }).await
    }
    /// Restore committed Skill bodies and recheck current permission before context use.
    pub async fn context_items(
        &self,
        snapshot: &RunSnapshot,
        context: &ExecutionContext,
        route: Option<&ResolvedModelRoute>,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<ContextItem>, ContractError> {
        if context.data.scope != snapshot.scope || &snapshot.scope != self.scope() {
            return Err(skill_error(ErrorCode::AccessDenied, "skills.scope"));
        }
        let controls = self.controls(&snapshot.run_id, context, deadline);
        let _cancel = controls.cancellation.clone().drop_guard();
        bounded(&controls, async {
            let plan = self.saved_plan(snapshot).await?;
            let mut loaded = vec![];
            for entry in &snapshot.tool_ledger {
                let ToolCallState::Settled { result } = &entry.state else {
                    continue;
                };
                let Some(reference) = &result.skill_ref else {
                    continue;
                };
                let record = self
                    .bindings
                    .state
                    .read_record(self.scope(), reference)
                    .await?;
                if record.reference() != reference {
                    return Err(skill_error(ErrorCode::InvalidSnapshot, "skills.record"));
                }
                let body: LoadedSkill = serde_json::from_value(record.value().clone())
                    .map_err(|_| skill_error(ErrorCode::InvalidSkill, "skills.record"))?;
                body.validate(&plan, &snapshot.run_id, &entry.call.call_id)?;
                if result.status != ToolResultStatus::Succeeded
                    || entry.call.descriptor_digest.as_ref() != Some(&plan.loader_descriptor_digest)
                {
                    return Err(skill_error(ErrorCode::InvalidSkill, "skills.result"));
                }
                if !loaded
                    .iter()
                    .any(|prior: &LoadedSkill| prior.selection == body.selection)
                {
                    loaded.push(body);
                }
            }
            if loaded
                .iter()
                .map(|body| body.body.len() as u64)
                .sum::<u64>()
                > plan.limits.max_total_body_bytes
            {
                return Err(skill_error(
                    ErrorCode::ContextBudgetExceeded,
                    "skills.total_body",
                ));
            }
            let mut items = vec![];
            for selected in &plan.skills {
                let Some(body) = loaded
                    .iter()
                    .find(|body| body.selection == selected.selection)
                else {
                    continue;
                };
                self.authorize(
                    &selected.definition.skill,
                    &selected.definition.digest(),
                    context,
                    &controls,
                    route,
                )
                .await?;
                self.bindings
                    .resolver
                    .authorize_use(body, &controls)
                    .await?;
                self.assets(&body.assets, context, controls.deadline)
                    .await?;
                self.authorize(
                    &selected.definition.skill,
                    &selected.definition.digest(),
                    context,
                    &controls,
                    route,
                )
                .await?;
                items.push(body.context_item()?);
            }
            Ok(items)
        })
        .await
    }
}
struct Loader(Arc<SkillRuntime>);
impl ToolExecutor for Loader {
    fn execute<'a>(
        &'a self,
        args: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::LoadedSkill {
                    loaded: Box::new(self.0.load(args, context).await?),
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}
pub(super) fn loader_descriptor(limits: SkillLimits) -> Result<ToolDescriptor, ContractError> {
    Ok(ToolDescriptor {tool:VersionedRef {id:Id::new("wickle.skills.load")?,version:Id::new("1")?},name:Id::new("skills_load")?,description:"Load the complete instructions of an available Skill at its exact listed version. Supporting assets are references; loading never executes code.".into(),input_schema:json!({"type":"object","properties":{"skill_id":{"type":"string","minLength":1},"version":{"type":"string","minLength":1}},"required":["skill_id","version"],"additionalProperties":false}),agent_parameters:vec!["skill_id".into(),"version".into()],system_bindings:None,output_schema:json!({"type":"object"}),side_effect:ToolSideEffect::ReadOnly,concurrency:ToolConcurrency::Serial,retry:ToolRetryPolicy::Never,reconcile:false,max_output_bytes:(limits.max_body_bytes*6+65536).try_into().map_err(|_|skill_error(ErrorCode::InvalidConfiguration,"skills.output_limit"))?})
}
pub(super) fn attest(
    profile: &ResolvedProfile,
    metadata: &ComponentMetadata,
) -> Result<(), ContractError> {
    if !profile.components().iter().any(|component| {
        component.reference == metadata.reference
            && component.definition_digest == crate::serialization::data_digest(metadata)
    }) {
        return Err(skill_error(ErrorCode::ContextMismatch, "skills.metadata"));
    }
    Ok(())
}
fn tool_component(
    profile: &AgentProfile,
    selection: &ToolBindingRef,
) -> Result<ComponentRef, ContractError> {
    match selection {
        ToolBindingRef::Catalog(reference) => Ok(ComponentRef {
            kind: ComponentKind::Tool,
            id: reference.tool_id.clone(),
            version: Some(reference.version.clone()),
        }),
        ToolBindingRef::Export(reference) => {
            let adapter = profile
                .adapters
                .iter()
                .flatten()
                .find(|adapter| adapter.binding_id == reference.adapter_binding)
                .ok_or_else(|| skill_error(ErrorCode::InvalidReference, "skills.adapter"))?;
            Ok(ComponentRef {
                kind: ComponentKind::Adapter,
                id: adapter.adapter_id.clone(),
                version: Some(adapter.version.clone()),
            })
        }
    }
}
pub(super) fn tool_capabilities(
    profile: &ResolvedProfile,
    metadata: &[ComponentMetadata],
) -> Result<BTreeSet<Id>, ContractError> {
    let mut capabilities = BTreeSet::new();
    for selection in &profile.profile().tools {
        let reference = tool_component(profile.profile(), selection)?;
        let metadata = metadata
            .iter()
            .find(|metadata| metadata.reference == reference)
            .ok_or_else(|| skill_error(ErrorCode::ComponentUnavailable, "skills.tool_metadata"))?;
        attest(profile, metadata)?;
        match selection {
            ToolBindingRef::Catalog(_) => {
                capabilities.extend(metadata.capabilities.iter().cloned())
            }
            ToolBindingRef::Export(reference) => {
                let export = metadata
                    .exports
                    .iter()
                    .find(|export| {
                        export.export_id == reference.export_id && export.kind == ExportKind::Tool
                    })
                    .ok_or_else(|| {
                        skill_error(ErrorCode::InvalidReference, "skills.tool_export")
                    })?;
                capabilities.extend(export.capabilities.iter().cloned());
            }
        }
    }
    Ok(capabilities)
}
async fn bounded<T>(
    context: &SkillCallContext,
    future: impl std::future::Future<Output = Result<T, ContractError>>,
) -> Result<T, ContractError> {
    let result = tokio::select! {biased;
        _=context.cancellation.cancelled()=>Err(skill_error(ErrorCode::Cancelled,"skills.operation")),
        _=tokio::time::sleep_until(context.deadline)=>Err(skill_error(ErrorCode::DeadlineExceeded,"skills.operation")),
        result=AssertUnwindSafe(future).catch_unwind()=>result.unwrap_or_else(|_|Err(skill_error(ErrorCode::InvalidSkill,"skills.operation"))),
    };
    if context.cancellation.is_cancelled() {
        return Err(skill_error(ErrorCode::Cancelled, "skills.operation"));
    }
    if tokio::time::Instant::now() >= context.deadline {
        return Err(skill_error(ErrorCode::DeadlineExceeded, "skills.operation"));
    }
    result.map_err(|error| skill_error(error.code, "skills.operation"))
}
