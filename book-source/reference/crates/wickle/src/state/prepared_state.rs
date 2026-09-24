use super::*;
use crate::*;

fn read<T: DeserializeOwned>(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    reference: &RecordRef,
) -> Result<T, ContractError> {
    serde_json::from_value(record_value(state, additions, reference)?.clone())
        .map_err(|_| invalid("prepared.record"))
}
fn invalid(path: &str) -> ContractError {
    error(ErrorCode::InvalidSnapshot, path)
}

pub(super) fn validate(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    let mut step_ids = BTreeSet::new();
    for reference in &snapshot.model_step_inputs {
        let value = record_value(state, additions, reference)?;
        let input: RoutedModelInput = serde_json::from_value(value["input"].clone())
            .map_err(|_| invalid("prepared.step_input"))?;
        let session = state
            .sessions
            .get(&snapshot.request.session_id)
            .ok_or_else(not_found)?;
        if value["schema_version"] != "wickle.model-step.v2"
            || value["run_id"] != serde_json::json!(snapshot.run_id)
            || value["through_sequence"]
                .as_u64()
                .is_none_or(|sequence| sequence > session.snapshot.transcript_revision)
            || input.routing.scope != snapshot.scope
            || !step_ids.insert(input.model_step_id)
        {
            return Err(invalid("prepared.step_input"));
        }
    }
    // These caches live only for this validation call. Immutable contracts are
    // compiled once; every repeated serialized value is still checked exactly.
    let mut tool_cache = BTreeMap::new();
    let mut contract_cache: BTreeMap<(String, String), (CompiledToolContract, Value)> =
        BTreeMap::new();
    let routing = if snapshot.prepared_steps.is_empty() {
        None
    } else {
        let reference = snapshot
            .routing_snapshot_ref
            .as_ref()
            .ok_or_else(|| invalid("prepared.routing"))?;
        Some(RoutingSnapshot::restore(
            &record_value(state, additions, reference)?.to_string(),
            &snapshot.scope,
            &reference.digest,
        )?)
    };
    let mut prompt_cache = None;
    let mut revisions = BTreeMap::new();
    let mut roots = BTreeMap::new();
    let mut unique = BTreeSet::new();
    for reference in &snapshot.prepared_steps {
        if !unique.insert(record_key(reference)) {
            return Err(invalid("prepared.duplicate"));
        }
        let root: PreparedStepRecord = read(state, additions, reference)?;
        let expected = revisions.entry(root.model_step_id.clone()).or_insert(0u64);
        *expected = expected
            .checked_add(1)
            .ok_or_else(|| invalid("prepared.revision"))?;
        if root.scope != snapshot.scope
            || root.run_id != snapshot.run_id
            || root.profile_digest != *snapshot.profile.profile_digest()
            || root.assembly_ref != snapshot.assembly_ref
            || root.projection_revision.get() != *expected
            || root.assembler
                != (VersionedRef {
                    id: Id::new("wickle-prepared-model")?,
                    version: Id::new("1")?,
                })
        {
            return Err(invalid("prepared.identity"));
        }
        let projection: PreparedModelProjection = read(state, additions, &root.context_projection)?;
        let tools: ResolvedToolSet = read(state, additions, &root.tool_set)?;
        tools.validate_shape()?;
        let configuration_value = record_value(state, additions, &root.model_configuration)?;
        let route: ResolvedModelRoute =
            serde_json::from_value(configuration_value["route"].clone())
                .map_err(|_| invalid("prepared.route"))?;
        let configuration: ModelConfiguration =
            serde_json::from_value(configuration_value["configuration"].clone())
                .map_err(|_| invalid("prepared.configuration"))?;
        if projection.schema_version != "wickle.prepared-projection.v1"
            || projection.scope != snapshot.scope
            || projection.run_id != snapshot.run_id
            || projection.request.request_id != root.model_step_id
            || projection.request.purpose != root.purpose
            || projection.request.route != route
            || projection.request.options != configuration.effective
            || projection.request.max_output_tokens != configuration.max_output_tokens
            || projection.fingerprint() != root.projection_fingerprint
            || tools.scope != snapshot.scope
            || tools.run_id != snapshot.run_id
            || tools.entries.len() != projection.request.tools.len()
            || tools.entries.len() != root.compiled_tools.len()
        {
            return Err(invalid("prepared.projection"));
        }
        projection.request.validate()?;
        let routing = routing.as_ref().expect("nonempty preparation history");
        routing.validate_route(&route)?;
        if !snapshot.model_step_inputs.contains(&root.step_input) {
            return Err(invalid("prepared.step_reference"));
        }
        let step_value = record_value(state, additions, &root.step_input)?;
        let step: RoutedModelInput = serde_json::from_value(step_value["input"].clone())
            .map_err(|_| invalid("prepared.step"))?;
        if step_value["schema_version"] != "wickle.model-step.v2"
            || step_value["through_sequence"].as_u64()
                != Some(projection.provenance.through_sequence)
            || step_value["run_id"] != serde_json::json!(snapshot.run_id)
            || step.model_step_id != root.model_step_id
            || step.routing.scope != snapshot.scope
            || step.routing.purpose != root.purpose
        {
            return Err(invalid("prepared.step"));
        }
        let expected_configuration = routing.model_configuration(
            &route,
            &step.routing.options,
            &crate::model_options::requested_sources(snapshot, root.purpose, &step.routing.options),
            step.routing.max_output_tokens,
        )?;
        if configuration != expected_configuration {
            return Err(invalid("prepared.configuration"));
        }
        let session = state
            .sessions
            .get(&snapshot.request.session_id)
            .ok_or_else(not_found)?;
        if !tools.entries.is_empty() && prompt_cache.is_none() {
            prompt_cache = Some(PromptSnapshot::restore(
                &record_value(state, additions, &session.snapshot.prompt_snapshot)?.to_string(),
                &session.snapshot.prompt_snapshot.digest,
                &snapshot.profile,
                &snapshot.scope,
            )?);
        }
        let manifests = prompt_cache.as_ref().map_or(&[][..], PromptSnapshot::tools);
        if root.purpose != ModelPurpose::Agent && !tools.entries.is_empty() {
            return Err(invalid("prepared.auxiliary_tools"));
        }
        if projection.provenance.through_sequence > session.snapshot.transcript_revision {
            return Err(invalid("prepared.transcript_boundary"));
        }
        let messages: Vec<_> = session
            .messages
            .iter()
            .filter(|message| message.sequence.get() <= projection.provenance.through_sequence)
            .cloned()
            .collect();
        let expected_lineage =
            context_state::source_lineage(state, additions, snapshot, &messages)?;
        if (root.purpose == ModelPurpose::Agent
            && expected_lineage != projection.provenance.source_lineage)
            || expected_lineage
                .iter()
                .any(|dependency| !projection.provenance.source_lineage.contains(dependency))
        {
            return Err(invalid("prepared.lineage_missing"));
        }
        let mut extra = vec![];
        let mut current = snapshot.context_revision_ref.clone();
        let mut context_refs = Vec::new();
        while let Some(reference) = current {
            if context_refs.contains(&reference) {
                return Err(invalid("prepared.context_cycle"));
            }
            context_refs.push(reference.clone());
            let revision: ContextRevision = read(state, additions, &reference)?;
            if revision.scope != snapshot.scope
                || revision.session_id != snapshot.request.session_id
            {
                return Err(invalid("prepared.context_scope"));
            }
            extra.extend(crate::prepared_step::revision_artifacts(&revision));
            current = revision.parent;
        }
        if projection
            .provenance
            .context_revision_ref
            .as_ref()
            .is_some_and(|reference| !context_refs.contains(reference))
        {
            return Err(invalid("prepared.context_reference"));
        }
        let expected_artifacts = crate::prepared_step::projected_artifacts(
            &projection.request,
            &messages,
            &extra,
            &snapshot.scope,
        )?;
        if expected_artifacts
            .iter()
            .any(|reference| !projection.provenance.artifacts.contains(reference))
            || projection
                .provenance
                .artifacts
                .iter()
                .any(|reference| reference.scope != snapshot.scope)
        {
            return Err(invalid("prepared.artifacts_missing"));
        }
        let mut manifests = manifests.iter();
        let target = ProviderToolTarget::for_route(&route);
        let mut contracts = vec![];
        for ((entry, reference), wire) in tools
            .entries
            .iter()
            .zip(&root.compiled_tools)
            .zip(&projection.request.tools)
        {
            if !manifests.any(|manifest| manifest == &entry.manifest) {
                return Err(invalid("prepared.tool_manifest"));
            }
            let tool = entry.restore_cached(&mut tool_cache)?;
            let value = record_value(state, additions, reference)?;
            let digest: JsonDigest = serde_json::from_value(value["digest"].clone())
                .map_err(|_| invalid("prepared.contract_digest"))?;
            let cache_key = (
                digest.to_string(),
                entry.manifest.compiled_digest.to_string(),
            );
            let contract = if let Some((contract, expected)) = contract_cache.get(&cache_key) {
                if expected != value || contract.target() != &target {
                    return Err(invalid("prepared.contract_cache_identity"));
                }
                contract.clone()
            } else {
                let contract = CompiledToolContract::restore(
                    &value.to_string(),
                    &tool,
                    &target,
                    &digest,
                    ProviderToolSchemaLimits::default(),
                )?;
                contract_cache.insert(cache_key, (contract.clone(), value.clone()));
                contract
            };
            if contract.wire_tool() != wire {
                return Err(invalid("prepared.wire_tool"));
            }
            contracts.push(contract);
        }
        if root.purpose == ModelPurpose::Agent {
            let mut expected_sources = vec![];
            for binding in snapshot.profile.profile().context_sources.iter().flatten() {
                let mut matching = vec![];
                for reference in &snapshot.context_batches {
                    let value = record_value(state, additions, reference)?;
                    let request: ContextRequest = serde_json::from_value(value["request"].clone())
                        .map_err(|_| invalid("prepared.source_request"))?;
                    if request.binding == *binding
                        && (binding.trigger == ContextTrigger::RunStart
                            || request.model_step_id.as_ref() == Some(&root.model_step_id))
                    {
                        matching.push(reference.clone());
                    }
                }
                if matching.len() != 1 {
                    return Err(invalid("prepared.source_slot"));
                }
                expected_sources.push(matching.remove(0));
            }
            if expected_sources != projection.provenance.source_batches {
                return Err(invalid("prepared.source_selection"));
            }
        }
        let mut fragments = vec![];
        for reference in &projection.provenance.source_batches {
            if !snapshot.context_batches.contains(reference) {
                return Err(invalid("prepared.source_batch"));
            }
            if let Some(value) = record_value(state, additions, reference)?.get("fragments") {
                fragments.extend(
                    serde_json::from_value::<Vec<ContextFragment>>(value.clone())
                        .map_err(|_| invalid("prepared.fragments"))?,
                );
            }
        }
        if fragments != projection.provenance.fragments {
            return Err(invalid("prepared.fragments"));
        }
        for fragment in &fragments {
            fragment.validate()?;
        }
        for dependency in &projection.provenance.source_lineage {
            let original = if dependency.run_id == snapshot.run_id {
                snapshot
            } else {
                &state
                    .runs
                    .get(&dependency.run_id)
                    .ok_or_else(not_found)?
                    .snapshot
            };
            if original.request.session_id != snapshot.request.session_id
                || !original.context_batches.contains(&dependency.batch_ref)
            {
                return Err(invalid("prepared.lineage"));
            }
            record_value(state, additions, &dependency.batch_ref)?;
        }
        roots.insert(
            record_key(reference),
            (root, projection, configuration, tools, contracts),
        );
    }
    if let Some(reference) = &snapshot.active_prepared_step {
        let (root, ..) = roots
            .get(&record_key(reference))
            .ok_or_else(|| invalid("prepared.active"))?;
        if root.purpose != ModelPurpose::Agent
            || snapshot.model_step_id.as_ref() != Some(&root.model_step_id)
            || snapshot.prepared_steps.iter().rev().find(|reference| {
                roots
                    .get(&record_key(reference))
                    .is_some_and(|(root, ..)| root.purpose == ModelPurpose::Agent)
            }) != Some(reference)
        {
            return Err(invalid("prepared.active"));
        }
    }
    for invocation in &snapshot.model_ledger {
        let Some(reference) = &invocation.prepared_step_ref else {
            continue;
        };
        if !snapshot.prepared_steps.contains(reference) {
            return Err(invalid("prepared.invocation_reference"));
        }
        let (root, projection, configuration, _, _) = roots
            .get(&record_key(reference))
            .ok_or_else(|| invalid("prepared.invocation"))?;
        let mut physical = projection.request.clone();
        physical.request_id = invocation.attempt_id.clone();
        if invocation.model_step_id != root.model_step_id
            || invocation.purpose != root.purpose
            || invocation.route != projection.request.route
            || invocation.configuration.as_ref() != Some(configuration)
            || invocation.request_digest != physical.digest()
        {
            return Err(invalid("prepared.invocation"));
        }
    }
    for ledger in &snapshot.tool_ledger {
        let call = &ledger.call;
        let Some(invocation) = snapshot
            .model_ledger
            .iter()
            .find(|invocation| invocation.attempt_id == call.model_request_id)
        else {
            continue;
        };
        let Some(reference) = &invocation.prepared_step_ref else {
            continue;
        };
        let (root, _, _, tools, contracts) = roots
            .get(&record_key(reference))
            .ok_or_else(|| invalid("prepared.call"))?;
        let arguments = call
            .provider_arguments
            .as_ref()
            .ok_or_else(|| invalid("prepared.call_provenance"))?;
        let response: StoredModelResponse = read(
            state,
            additions,
            invocation
                .response_ref
                .as_ref()
                .ok_or_else(|| invalid("prepared.call_response"))?,
        )?;
        let ModelExchangeOutcome::Completed { response } = response.outcome else {
            return Err(invalid("prepared.call_response"));
        };
        if !response.tool_calls.iter().any(|proposed| {
            proposed.provider_call_id == call.provider_call_id
                && proposed.name == arguments.name
                && proposed.raw_arguments.as_ref() == Some(&arguments.raw)
        }) {
            return Err(invalid("prepared.call_response"));
        }
        if let Some((index, contract)) = contracts
            .iter()
            .enumerate()
            .find(|(_, contract)| contract.wire_tool().name == arguments.name)
        {
            let decoded = match contract
                .decode_arguments(&arguments.raw, ProviderToolSchemaLimits::default())
            {
                Ok(value) => value,
                Err(error) if error.code == ErrorCode::InvalidArguments => JsonObject::new(),
                Err(error) => return Err(error),
            };
            if call.tool_name != *contract.canonical_name()
                || call.model_inputs != decoded
                || call.descriptor_digest.as_ref()
                    != Some(&tools.entries[index].manifest.descriptor_digest)
                || arguments.compiled_contract_ref.as_ref() != Some(&root.compiled_tools[index])
            {
                return Err(invalid("prepared.call_contract"));
            }
        } else if call.descriptor_digest.is_some()
            || arguments.compiled_contract_ref.is_some()
            || call.tool_name != arguments.name
        {
            return Err(invalid("prepared.unadvertised_tool"));
        }
    }
    Ok(())
}
