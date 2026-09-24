use super::*;
use serde_json::json;

const BATCH_VERSION: &str = "wickle.context-batch.v1";

impl ContextSourceRegistry {
    /// Register exact native or adapter-export providers without invoking them.
    pub fn new(
        scope: Scope,
        entries: Vec<ContextSourceRegistration>,
    ) -> Result<Self, ContractError> {
        if entries.len() > 64 {
            return Err(source_error(
                ErrorCode::InvalidConfiguration,
                "context_source.count",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for entry in &entries {
            entry.definition.validate()?;
            validate_selection(&entry.selection, &entry.definition)?;
            if !seen.insert(crate::serialization::data_digest(&entry.selection)) {
                return Err(source_error(
                    ErrorCode::InvalidReference,
                    "context_source.duplicate",
                ));
            }
        }
        Ok(Self { scope, entries })
    }
    /// Build a non-executing registry to pin metadata before adapter initialization.
    pub fn metadata(
        scope: Scope,
        bindings: Vec<ResolvedSourceBinding>,
    ) -> Result<Self, ContractError> {
        let mut entries: Vec<ContextSourceRegistration> = vec![];
        for binding in bindings {
            if let Some(entry) = entries
                .iter()
                .find(|entry| entry.selection == binding.binding.source)
            {
                if entry.definition != binding.definition {
                    return Err(source_error(
                        ErrorCode::InvalidContract,
                        "context_source.definition",
                    ));
                }
                continue;
            }
            entries.push(ContextSourceRegistration {
                selection: binding.binding.source,
                definition: binding.definition,
                source: Arc::new(MetadataSource),
            });
        }
        Self::new(scope, entries)
    }
    /// Exact registry namespace.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Pin profile order, limits and the Host estimator version.
    pub fn plan(
        &self,
        profile: &AgentProfile,
        estimator_version: &VersionedRef,
    ) -> Result<ContextSourcePlan, ContractError> {
        let bindings = profile
            .context_sources
            .iter()
            .flatten()
            .map(|binding| {
                let entry = self
                    .entries
                    .iter()
                    .find(|entry| entry.selection == binding.source)
                    .ok_or_else(|| {
                        source_error(ErrorCode::ComponentUnavailable, "context_source.selection")
                    })?;
                Ok(PlannedContextSource {
                    binding: binding.clone(),
                    definition: entry.definition.clone(),
                })
            })
            .collect::<Result<Vec<_>, ContractError>>()?;
        let plan = ContextSourcePlan {
            scope: self.scope.clone(),
            bindings,
            estimator_version: estimator_version.clone(),
        };
        plan.validate(profile)?;
        Ok(plan)
    }
    pub(super) fn get(&self, selection: &ContextSourceRef) -> Option<&ContextSourceRegistration> {
        self.entries
            .iter()
            .find(|entry| &entry.selection == selection)
    }
}
struct MetadataSource;
impl ContextSource for MetadataSource {
    fn provide<'a>(
        &'a self,
        _: &'a ContextRequest,
        _: &'a ContextCallContext,
    ) -> PortFuture<'a, ContextResult> {
        Box::pin(async {
            Err(source_error(
                ErrorCode::ComponentUnavailable,
                "context_source.metadata_only",
            ))
        })
    }
    fn authorize_use<'a>(
        &'a self,
        _: &'a ContextUseRequest,
        _: &'a ContextCallContext,
    ) -> PortFuture<'a, ()> {
        Box::pin(async {
            Err(source_error(
                ErrorCode::ComponentUnavailable,
                "context_source.metadata_only",
            ))
        })
    }
}
impl ContextSourcePlan {
    /// Exact plan namespace.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Selected bindings in original profile order.
    pub fn bindings(&self) -> &[PlannedContextSource] {
        &self.bindings
    }
    /// Pinned trusted estimator identity.
    pub fn estimator_version(&self) -> &VersionedRef {
        &self.estimator_version
    }
    /// Complete plan digest.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
    /// Restore only the exact protected plan version and identity.
    pub fn restore(
        json: &str,
        scope: &Scope,
        expected_digest: &JsonDigest,
    ) -> Result<Self, ContractError> {
        let plan: Self = serde_json::from_value(parse_json(json)?)
            .map_err(|_| source_error(ErrorCode::InvalidSnapshot, "context_source.plan"))?;
        if &plan.scope != scope || &plan.digest() != expected_digest {
            return Err(source_error(
                ErrorCode::InvalidSnapshot,
                "context_source.plan_identity",
            ));
        }
        plan.validate_bindings()?;
        Ok(plan)
    }
    /// Reject missing, reordered or changed profile bindings and invalid definitions.
    pub fn validate(&self, profile: &AgentProfile) -> Result<(), ContractError> {
        self.validate_bindings()?;
        if self
            .bindings
            .iter()
            .map(|entry| &entry.binding)
            .collect::<Vec<_>>()
            != profile.context_sources.iter().flatten().collect::<Vec<_>>()
        {
            return Err(source_error(
                ErrorCode::InvalidSnapshot,
                "context_source.plan_selection",
            ));
        }
        Ok(())
    }
    fn validate_bindings(&self) -> Result<(), ContractError> {
        if self.bindings.len() > 64 {
            return Err(source_error(
                ErrorCode::InvalidConfiguration,
                "context_source.count",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for entry in &self.bindings {
            entry.definition.validate()?;
            validate_selection(&entry.binding.source, &entry.definition)?;
            if entry.binding.timeout_ms.get() > 86_400_000
                || entry.binding.max_items.get() > 4096
                || entry.binding.max_bytes.get() > 16_777_216
                || entry.binding.max_tokens.get() > 4_000_000
            {
                return Err(source_error(
                    ErrorCode::InvalidConfiguration,
                    "context_source.limits",
                ));
            }
            if !seen.insert(canonical_digest(&json!([
                entry.binding.source,
                entry.binding.trigger
            ]))) {
                return Err(source_error(
                    ErrorCode::InvalidReference,
                    "context_source.duplicate_slot",
                ));
            }
        }
        Ok(())
    }
}
fn validate_selection(
    selection: &ContextSourceRef,
    definition: &ContextSourceDefinition,
) -> Result<(), ContractError> {
    let valid = match selection {
        ContextSourceRef::Catalog(reference) => {
            reference.source_id == definition.source.id
                && reference.version == definition.source.version
        }
        ContextSourceRef::Export(reference) => reference.alias.is_none(),
    };
    if !valid {
        return Err(source_error(
            ErrorCode::InvalidReference,
            "context_source.selection",
        ));
    }
    Ok(())
}
impl ContextRequest {
    pub(super) fn for_run(
        snapshot: &RunSnapshot,
        entry: &PlannedContextSource,
        step: Option<Id>,
    ) -> Result<Self, ContractError> {
        let mut request = Self {
            context_request_id: Id::new("pending")?,
            scope: snapshot.scope.clone(),
            run_id: snapshot.run_id.clone(),
            session_id: snapshot.request.session_id.clone(),
            binding: entry.binding.clone(),
            definition: entry.definition.clone(),
            model_step_id: step,
            user_input: snapshot.request.input.clone(),
        };
        request.context_request_id = request.identity()?;
        request.validate()?;
        Ok(request)
    }
    fn identity(&self) -> Result<Id, ContractError> {
        Id::new(format!(
            "context-{}",
            canonical_digest(&json!([
                self.scope,
                self.run_id,
                self.session_id,
                self.binding,
                self.definition,
                self.model_step_id,
                self.input_digest()
            ]))
        ))
    }
    fn validate(&self) -> Result<(), ContractError> {
        if self.context_request_id != self.identity()?
            || (self.binding.trigger == ContextTrigger::BeforeModel) != self.model_step_id.is_some()
        {
            return Err(source_error(
                ErrorCode::InvalidSnapshot,
                "context_source.request",
            ));
        }
        Ok(())
    }
}
impl ContextBatch {
    /// Exact protected identity of this immutable batch.
    pub fn reference(&self) -> RecordRef {
        RecordRef {
            record_id: self.batch_id.clone(),
            revision: 1,
            digest: crate::serialization::data_digest(self),
        }
    }
    /// Build the protected record committed alongside its active source slot.
    pub fn to_record(&self) -> ProtectedRecord {
        ProtectedRecord::new(
            self.batch_id.clone(),
            1,
            serde_json::to_value(self).expect("ContextBatch serialization"),
        )
    }
    /// Original query and source limits.
    pub fn request(&self) -> &ContextRequest {
        &self.request
    }
    /// Original provider reply, including original local item identifiers.
    pub fn result(&self) -> &ContextResult {
        &self.result
    }
    /// Validated core-namespaced items, empty for empty or unavailable replies.
    pub fn items(&self) -> &[ContextItem] {
        &self.items
    }
    /// Host-estimated tokens of the actual derived items.
    pub fn estimated_tokens(&self) -> u64 {
        self.estimated_tokens
    }
    /// Host-observed collection completion time.
    pub fn collected_at_ms(&self) -> i64 {
        self.collected_at_ms
    }
    /// Trusted algorithm used for the stored estimate.
    pub fn estimator_version(&self) -> &VersionedRef {
        &self.estimator_version
    }
    /// Restore and validate identity, source claims, sizes and deterministic namespace.
    pub fn restore(
        record: &ProtectedRecord,
        plan: &ContextSourcePlan,
        scope: &Scope,
        run_id: &Id,
    ) -> Result<Self, ContractError> {
        let batch: Self = serde_json::from_value(record.value().clone())
            .map_err(|_| source_error(ErrorCode::InvalidSnapshot, "context_source.batch"))?;
        if batch.reference() != *record.reference()
            || &batch.request.scope != scope
            || plan.scope() != scope
            || &batch.request.run_id != run_id
            || &batch.estimator_version != plan.estimator_version()
        {
            return Err(source_error(
                ErrorCode::InvalidSnapshot,
                "context_source.batch_identity",
            ));
        }
        if !plan.bindings().iter().any(|entry| {
            entry.binding == batch.request.binding && entry.definition == batch.request.definition
        }) {
            return Err(source_error(
                ErrorCode::InvalidSnapshot,
                "context_source.batch_plan",
            ));
        }
        batch.validate()?;
        Ok(batch)
    }
    pub(super) fn new(
        batch_id: Id,
        request: ContextRequest,
        result: ContextResult,
        collected_at_ms: i64,
        estimator_version: VersionedRef,
        estimator: &dyn ContextTokenEstimator,
    ) -> Result<Self, ContractError> {
        validate_result(&request, &result)?;
        let items = namespaced(&batch_id, &request, result.items())?;
        check_bytes(&items, request.binding.max_bytes.get())?;
        let estimated_tokens = if items.is_empty() {
            0
        } else {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| estimator.estimate(&items)))
                .map_err(|_| source_error(ErrorCode::InvalidContract, "context_source.estimator"))?
                .map_err(|error| source_error(error.code, "context_source.estimator"))?
        };
        let batch = Self {
            schema_version: BATCH_VERSION.into(),
            batch_id,
            request,
            result,
            items,
            collected_at_ms,
            estimated_tokens,
            estimator_version,
        };
        batch.validate()?;
        Ok(batch)
    }
    fn validate(&self) -> Result<(), ContractError> {
        if self.schema_version != BATCH_VERSION {
            return Err(source_error(
                ErrorCode::UnsupportedSchemaVersion,
                "context_source.batch_version",
            ));
        }
        self.request.validate()?;
        validate_result(&self.request, &self.result)?;
        if self.items != namespaced(&self.batch_id, &self.request, self.result.items())? {
            return Err(source_error(
                ErrorCode::InvalidSnapshot,
                "context_source.namespace",
            ));
        }
        check_bytes(&self.items, self.request.binding.max_bytes.get())?;
        if self.estimated_tokens > self.request.binding.max_tokens.get()
            || self.items.is_empty() && self.estimated_tokens != 0
        {
            return Err(source_error(
                ErrorCode::ContextBudgetExceeded,
                "context_source.tokens",
            ));
        }
        Ok(())
    }
}
fn validate_result(request: &ContextRequest, result: &ContextResult) -> Result<(), ContractError> {
    request.definition.validate()?;
    check_bytes(result, request.binding.max_bytes.get())?;
    let items = result.items();
    if matches!(result, ContextResult::Ready { .. }) && items.is_empty() {
        return Err(source_error(
            ErrorCode::InvalidContract,
            "context_source.ready_empty",
        ));
    }
    if items.len() as u64 > request.binding.max_items.get() {
        return Err(source_error(
            ErrorCode::ContextBudgetExceeded,
            "context_source.items",
        ));
    }
    let expected = if let Some(step) = &request.model_step_id {
        ContextLifetime::Step {
            run_id: request.run_id.clone(),
            model_step_id: step.clone(),
        }
    } else {
        ContextLifetime::Run {
            run_id: request.run_id.clone(),
        }
    };
    let mut seen = std::collections::BTreeSet::new();
    for item in items {
        let rebuilt = ContextItem::new(
            item.item_id.clone(),
            item.origin,
            item.source_ref.clone(),
            item.scope.clone(),
            item.content.clone(),
            item.lifetime.clone(),
            item.priority_class,
        );
        if item.scope != request.scope {
            return Err(source_error(
                ErrorCode::AccessDenied,
                "context_source.item_scope",
            ));
        }
        if item.origin != request.definition.origin
            || item.source_ref != request.definition.source
            || item.lifetime != expected
            || item != &rebuilt
            || !seen.insert(&item.item_id)
        {
            return Err(source_error(
                ErrorCode::InvalidContract,
                "context_source.item",
            ));
        }
        if item.content.iter().any(|content|matches!(content,InputContent::Artifact{reference} if reference.scope!=request.scope)){return Err(source_error(ErrorCode::AccessDenied,"context_source.artifact_scope"));}
    }
    Ok(())
}
fn namespaced(
    batch_id: &Id,
    request: &ContextRequest,
    items: &[ContextItem],
) -> Result<Vec<ContextItem>, ContractError> {
    items
        .iter()
        .map(|item| {
            let id = Id::new(format!(
                "source-item-{}",
                canonical_digest(&json!([
                    request.binding.source,
                    request.definition,
                    request.scope,
                    request.run_id,
                    batch_id,
                    item.item_id
                ]))
            ))?;
            Ok(ContextItem::new(
                id,
                item.origin,
                item.source_ref.clone(),
                item.scope.clone(),
                item.content.clone(),
                item.lifetime.clone(),
                item.priority_class,
            ))
        })
        .collect()
}
pub(super) fn check_bytes(value: &impl Serialize, limit: u64) -> Result<(), ContractError> {
    if serde_json::to_vec(value)
        .map_err(|_| source_error(ErrorCode::InvalidJson, "context_source.result"))?
        .len() as u64
        > limit
    {
        return Err(source_error(
            ErrorCode::ContextBudgetExceeded,
            "context_source.bytes",
        ));
    }
    Ok(())
}
