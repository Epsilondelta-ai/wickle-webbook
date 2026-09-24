use super::*;
use crate::*;

pub(super) struct StoredPreparation {
    pub reference: RecordRef,
    pub projection: PreparedModelProjection,
}
impl ModelExchange {
    pub(super) async fn load_preparation(
        &self,
        input: &RoutedModelInput,
        route: &ResolvedModelRoute,
        configuration: &ModelConfiguration,
        budget: &RunBudget,
    ) -> Result<Option<StoredPreparation>, ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        for reference in saved.snapshot.prepared_steps.iter().rev() {
            let record = budget
                .store()
                .read_record(budget.scope(), reference)
                .await?;
            let root: PreparedStepRecord =
                serde_json::from_value(record.value().clone()).map_err(|_| revision_error())?;
            if root.model_step_id != input.model_step_id || root.purpose != input.routing.purpose {
                continue;
            }
            let configuration_record = budget
                .store()
                .read_record(budget.scope(), &root.model_configuration)
                .await?;
            let pinned_route: ResolvedModelRoute =
                serde_json::from_value(configuration_record.value()["route"].clone())
                    .map_err(|_| revision_error())?;
            if &pinned_route != route {
                continue;
            }
            let pinned: ModelConfiguration =
                serde_json::from_value(configuration_record.value()["configuration"].clone())
                    .map_err(|_| revision_error())?;
            if &pinned != configuration {
                return Err(ContractError::new(
                    ErrorCode::RequestConflict,
                    "prepared.configuration",
                ));
            }
            let projection_record = budget
                .store()
                .read_record(budget.scope(), &root.context_projection)
                .await?;
            let projection: PreparedModelProjection =
                serde_json::from_value(projection_record.value().clone())
                    .map_err(|_| revision_error())?;
            if root.scope != *budget.scope()
                || root.run_id != *budget.run_id()
                || root.profile_digest != *saved.snapshot.profile.profile_digest()
                || root.assembly_ref != saved.snapshot.assembly_ref
                || projection.scope != root.scope
                || projection.run_id != root.run_id
                || projection.request.request_id != root.model_step_id
                || projection.request.route != *route
                || projection.request.purpose != root.purpose
                || projection.fingerprint() != root.projection_fingerprint
            {
                return Err(ContractError::new(
                    ErrorCode::InvalidSnapshot,
                    "prepared.identity",
                ));
            }
            projection.request.validate()?;
            return Ok(Some(StoredPreparation {
                reference: reference.clone(),
                projection,
            }));
        }
        Ok(None)
    }
    pub(super) async fn save_preparation(
        &self,
        mut prepared: ProjectedModelRequest,
        configuration: &ModelConfiguration,
        selection: &RouteSelection,
        budget: &RunBudget,
    ) -> Result<StoredPreparation, ContractError> {
        budget.check_boundary().await?;
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        prepared.provenance.context_revision_ref = saved.snapshot.context_revision_ref.clone();
        let step = prepared.request.request_id.clone();
        let mut step_input = None;
        for reference in &saved.snapshot.model_step_inputs {
            let record = budget
                .store()
                .read_record(budget.scope(), reference)
                .await?;
            if record.value()["input"]["model_step_id"] == serde_json::json!(step) {
                prepared.provenance.through_sequence = record.value()["through_sequence"]
                    .as_u64()
                    .ok_or_else(revision_error)?;
                step_input = Some(reference.clone());
                break;
            }
        }
        let step_input = step_input.ok_or_else(revision_error)?;
        let mut revision = 1u64;
        for reference in &saved.snapshot.prepared_steps {
            let record = budget
                .store()
                .read_record(budget.scope(), reference)
                .await?;
            let prior: PreparedStepRecord =
                serde_json::from_value(record.value().clone()).map_err(|_| revision_error())?;
            if prior.model_step_id == step {
                revision = prior
                    .projection_revision
                    .get()
                    .checked_add(1)
                    .ok_or_else(revision_error)?;
            }
        }
        let key = crate::canonical_digest(&serde_json::json!([budget.run_id(), step, revision]));
        let child = |name: &str, value| -> Result<ProtectedRecord, ContractError> {
            Ok(ProtectedRecord::new(
                Id::new(format!("prepared-{key}-{name}"))?,
                1,
                value,
            ))
        };
        let tool_set = ResolvedToolSet {
            schema_version: "wickle.resolved-tool-set.v1".into(),
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            entries: prepared.tool_set,
        };
        tool_set.validate_shape()?;
        if prepared.compiled_tools.len() != tool_set.entries.len()
            || prepared.compiled_tools.len() != prepared.request.tools.len()
        {
            return Err(ContractError::new(
                ErrorCode::InvalidContract,
                "prepared.tool_count",
            ));
        }
        let target = ProviderToolTarget::for_route(&selection.route);
        let mut records = Vec::new();
        let mut contracts = Vec::new();
        for (index, ((entry, contract), wire)) in tool_set
            .entries
            .iter()
            .zip(&prepared.compiled_tools)
            .zip(&prepared.request.tools)
            .enumerate()
        {
            let tool = entry.restore_tool()?;
            let restored = CompiledToolContract::restore(
                &serde_json::to_string(contract).map_err(|_| revision_error())?,
                &tool,
                &target,
                contract.digest(),
                ProviderToolSchemaLimits::default(),
            )?;
            if restored.wire_tool() != wire {
                return Err(ContractError::new(
                    ErrorCode::InvalidContract,
                    "prepared.wire_tool",
                ));
            }
            let record = child(
                &format!("tool-{index}"),
                serde_json::to_value(contract).map_err(|_| revision_error())?,
            )?;
            contracts.push(record.reference().clone());
            records.push(record);
        }
        let tools = child(
            "tool-set",
            serde_json::to_value(tool_set).map_err(|_| revision_error())?,
        )?;
        let configuration_record = child(
            "configuration",
            serde_json::json!({"route":selection.route,"configuration":configuration}),
        )?;
        let projection = PreparedModelProjection {
            schema_version: "wickle.prepared-projection.v1".into(),
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            request: prepared.request,
            input_tokens: prepared.input_tokens,
            provenance: prepared.provenance,
        };
        let projection_record = child(
            "projection",
            serde_json::to_value(&projection).map_err(|_| revision_error())?,
        )?;
        let root = PreparedStepRecord {
            step_input,
            schema_version: ExecutionRecordVersion::V1,
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            profile_digest: saved.snapshot.profile.profile_digest().clone(),
            assembly_ref: saved.snapshot.assembly_ref.clone(),
            projection_fingerprint: projection.fingerprint(),
            change_reason: Id::new(routed::reason_code(selection.reason))?,
            purpose: projection.request.purpose,
            compiled_tools: contracts,
            model_step_id: step,
            projection_revision: revision.try_into().map_err(|_| revision_error())?,
            model_configuration: configuration_record.reference().clone(),
            tool_set: tools.reference().clone(),
            context_projection: projection_record.reference().clone(),
            assembler: VersionedRef {
                id: Id::new("wickle-prepared-model")?,
                version: Id::new("1")?,
            },
        };
        let record = child(
            "step",
            serde_json::to_value(&root).map_err(|_| revision_error())?,
        )?;
        let reference = record.reference().clone();
        records.extend([tools, configuration_record, projection_record, record]);
        let mut snapshot = saved.snapshot;
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(revision_error)?;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        snapshot.prepared_steps.push(reference.clone());
        if root.purpose == ModelPurpose::Agent {
            snapshot.model_step_id = Some(root.model_step_id.clone());
            snapshot.active_prepared_step = Some(reference.clone());
        }
        budget
            .store()
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    control_commands: vec![],
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    records,
                    messages: vec![],
                    events: vec![],
                },
            )
            .await?;
        budget.check_boundary().await?;
        Ok(StoredPreparation {
            reference,
            projection,
        })
    }
}
