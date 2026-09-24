use super::*;

impl HookApplicationRecord {
    /// Restore one exact protected application; this does not grant permission
    /// to invoke a Hook or substitute for chain and original-call validation.
    pub fn restore(
        record: &ProtectedRecord,
        plan: &HookPlan,
        application: &HookApplication,
        scope: &Scope,
        run_id: &Id,
    ) -> Result<Self, ContractError> {
        let invalid = || hook_error(ErrorCode::InvalidSnapshot, "hooks.application");
        let value: Self = serde_json::from_value(record.value().clone()).map_err(|_| invalid())?;
        if record.reference() != &application.result_ref
            || plan.scope() != scope
            || &value.scope != scope
            || &value.run_id != run_id
            || value.hook != application.hook
            || value.selection != application.selection
            || value.definition_digest != application.definition_digest
            || value.target != application.target
            || value.input.digest() != application.input_digest
            || crate::serialization::data_digest(&value) != record.reference().digest
        {
            return Err(invalid());
        }
        let definition = plan
            .definition_for(&value.hook, value.selection.as_ref())
            .filter(|definition| {
                definition.digest() == value.definition_digest
                    && definition.position == value.target.position()
            })
            .ok_or_else(invalid)?;
        value.input.validate(&value.target)?;
        match (&value.output, &value.failure) {
            (Some(output), None) => {
                validate_output(definition, &value.input, output)?;
                match output {
                    HookOutput::Context { additions } => {
                        if additions.len() != value.context_items.len() {
                            return Err(invalid());
                        }
                        for (addition, item) in additions.iter().zip(&value.context_items) {
                            let expected = stamped(
                                item.item_id.clone(),
                                scope,
                                run_id,
                                definition,
                                &value.target,
                                addition,
                            )?;
                            if item != &expected {
                                return Err(invalid());
                            }
                        }
                    }
                    _ if !value.context_items.is_empty() => return Err(invalid()),
                    _ => {}
                }
            }
            (None, Some(_))
                if definition.position == HookPosition::BeforeRun
                    && !definition.required
                    && value.context_items.is_empty() => {}
            _ => return Err(invalid()),
        }
        Ok(value)
    }
}

pub(super) fn validate_output(
    definition: &HookDefinition,
    input: &HookInput,
    output: &HookOutput,
) -> Result<(), ContractError> {
    let invalid = || hook_error(ErrorCode::InvalidContract, "hooks.output");
    if serde_json::to_vec(output).map_err(|_| invalid())?.len() > definition.max_output_bytes {
        return Err(invalid());
    }
    let valid = match (definition.position, input, output) {
        (
            HookPosition::BeforeRun,
            HookInput::BeforeRun { .. },
            HookOutput::Context { additions },
        )
        | (
            HookPosition::BeforeModel,
            HookInput::BeforeModel { .. },
            HookOutput::Context { additions },
        ) => additions
            .iter()
            .all(|addition| safe_content(&addition.content)),
        (
            HookPosition::BeforeTool,
            HookInput::BeforeTool { tool, .. },
            HookOutput::Tool { model_inputs, .. },
        ) => {
            crate::tool_schema::normalize_model_input_schema(&tool.model_input_schema, model_inputs)
                .is_ok()
        }
        (HookPosition::AfterTool, HookInput::AfterTool { .. }, HookOutput::Observed {})
        | (HookPosition::AfterRun, HookInput::AfterRun { .. }, HookOutput::Observed {}) => true,
        _ => false,
    };
    if !valid {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn stamped(
    id: Id,
    scope: &Scope,
    run_id: &Id,
    definition: &HookDefinition,
    target: &HookTarget,
    addition: &HookContextAddition,
) -> Result<ContextItem, ContractError> {
    let lifetime = match target {
        HookTarget::BeforeRun => ContextLifetime::Run {
            run_id: run_id.clone(),
        },
        HookTarget::BeforeModel { model_step_id } => ContextLifetime::Step {
            run_id: run_id.clone(),
            model_step_id: model_step_id.clone(),
        },
        _ => {
            return Err(hook_error(
                ErrorCode::InvalidContract,
                "hooks.context_target",
            ));
        }
    };
    Ok(ContextItem::new(
        id,
        ContextOrigin::Hook,
        definition.hook.clone(),
        scope.clone(),
        addition.content.clone(),
        lifetime,
        addition.priority,
    ))
}

pub(super) fn apply(
    input: &mut HookInput,
    record: &HookApplicationRecord,
) -> Result<Option<Id>, ContractError> {
    if &record.input != input {
        return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.chain_input"));
    }
    match (&record.output, input) {
        (
            Some(HookOutput::Context { .. }),
            HookInput::BeforeRun { context_items, .. }
            | HookInput::BeforeModel { context_items, .. },
        ) => context_items.extend(record.context_items.clone()),
        (
            Some(HookOutput::Tool { model_inputs, deny }),
            HookInput::BeforeTool {
                model_inputs: current,
                ..
            },
        ) => {
            *current = model_inputs.clone();
            return Ok(deny.clone());
        }
        (None, _) if record.failure.is_some() => {}
        _ => return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.chain_output")),
    }
    Ok(None)
}

pub(super) fn transformed(
    input: HookInput,
    deny: Option<Id>,
    applications: Vec<HookApplication>,
) -> HookTransform {
    match input {
        HookInput::BeforeRun { context_items, .. }
        | HookInput::BeforeModel { context_items, .. } => HookTransform {
            context_items,
            model_inputs: None,
            deny,
            applications,
        },
        HookInput::BeforeTool { model_inputs, .. } => HookTransform {
            context_items: vec![],
            model_inputs: Some(model_inputs),
            deny,
            applications,
        },
        _ => unreachable!("validated transformation input"),
    }
}

pub(crate) fn validate_application_chain(
    plan: &HookPlan,
    snapshot: &RunSnapshot,
    records: &[ProtectedRecord],
) -> Result<(), ContractError> {
    plan.validate(snapshot.profile.profile())?;
    if plan.scope() != &snapshot.scope {
        return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.scope"));
    }
    let mut run_context = Vec::new();
    for application in snapshot
        .hook_applications
        .iter()
        .filter(|application| application.target == HookTarget::BeforeRun)
    {
        let record = records
            .iter()
            .find(|record| record.reference() == &application.result_ref)
            .ok_or_else(|| hook_error(ErrorCode::InvalidSnapshot, "hooks.run_context"))?;
        let value = HookApplicationRecord::restore(
            record,
            plan,
            application,
            &snapshot.scope,
            &snapshot.run_id,
        )?;
        run_context.extend(value.context_items);
    }
    let mut targets: Vec<HookTarget> = vec![];
    let mut ids = std::collections::BTreeSet::new();
    for application in &snapshot.hook_applications {
        if !ids.insert(application.result_ref.record_id.clone()) {
            return Err(hook_error(
                ErrorCode::InvalidSnapshot,
                "hooks.duplicate_application",
            ));
        }
        if !targets.contains(&application.target) {
            targets.push(application.target.clone());
        }
    }
    for target in targets {
        let definitions: Vec<_> = plan
            .definitions()
            .iter()
            .enumerate()
            .filter(|(_, definition)| definition.position == target.position())
            .map(|(index, definition)| (definition, plan.selection(index)))
            .collect();
        let applications: Vec<_> = snapshot
            .hook_applications
            .iter()
            .filter(|application| application.target == target)
            .collect();
        if applications.len() > definitions.len() {
            return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.chain_length"));
        }
        let mut current = None;
        let mut denied = false;
        for (application, (definition, selection)) in applications.into_iter().zip(definitions) {
            if denied
                || application.hook != definition.hook
                || application.selection.as_ref() != selection
                || application.definition_digest != definition.digest()
            {
                return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.chain_order"));
            }
            let record = records
                .iter()
                .find(|record| record.reference() == &application.result_ref)
                .ok_or_else(|| hook_error(ErrorCode::InvalidSnapshot, "hooks.record_missing"))?;
            let record = HookApplicationRecord::restore(
                record,
                plan,
                application,
                &snapshot.scope,
                &snapshot.run_id,
            )?;
            if current.is_none() {
                match (&target, &record.input) {
                    (
                        HookTarget::BeforeRun,
                        HookInput::BeforeRun {
                            user_input,
                            context_items,
                        },
                    ) if user_input == &snapshot.request.input && context_items.is_empty() => {}
                    (
                        HookTarget::BeforeModel { model_step_id },
                        HookInput::BeforeModel {
                            user_input,
                            context_items,
                        },
                    ) if user_input == &snapshot.request.input
                        && context_items
                            == &{
                                let mut expected =
                                    extension_context_for_step(snapshot, records, model_step_id)?;
                                expected.extend(run_context.clone());
                                expected
                            }
                        && (snapshot.model_step_id.as_ref() == Some(model_step_id)
                            || snapshot
                                .model_ledger
                                .iter()
                                .any(|invocation| &invocation.model_step_id == model_step_id)) => {}
                    (
                        HookTarget::BeforeTool { call_id },
                        HookInput::BeforeTool {
                            tool,
                            descriptor_digest,
                            original_model_inputs,
                            model_inputs,
                            ..
                        },
                    ) => {
                        let call = snapshot
                            .tool_ledger
                            .iter()
                            .find(|entry| &entry.call.call_id == call_id)
                            .ok_or_else(|| hook_error(ErrorCode::InvalidSnapshot, "hooks.call"))?;
                        if call.call.tool_name != tool.name
                            || call.call.descriptor_digest.as_ref() != Some(descriptor_digest)
                            || &call.call.model_inputs != original_model_inputs
                            || (model_inputs != original_model_inputs
                                && model_inputs
                                    != &crate::tool_schema::normalize_model_input_schema(
                                        &tool.model_input_schema,
                                        original_model_inputs,
                                    )?)
                        {
                            return Err(hook_error(
                                ErrorCode::InvalidSnapshot,
                                "hooks.original_inputs",
                            ));
                        }
                    }
                    _ => return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.target_input")),
                }
                current = Some(record.input.clone());
            }
            denied = apply(current.as_mut().expect("initialized"), &record)?.is_some();
        }
    }
    Ok(())
}

/// Rebuild the exact historical source input of one logical Hook step. Active
/// slots may now refer to later steps; only the matching saved batch is accepted.
pub(crate) fn extension_context_for_step(
    snapshot: &RunSnapshot,
    records: &[ProtectedRecord],
    step: &Id,
) -> Result<Vec<ContextItem>, ContractError> {
    let mut items = source_context_for_step(snapshot, records, step)?;
    items.extend(crate::skills::records::context_for_step(
        snapshot, records, step,
    )?);
    Ok(items)
}
fn source_context_for_step(
    snapshot: &RunSnapshot,
    records: &[ProtectedRecord],
    step: &Id,
) -> Result<Vec<ContextItem>, ContractError> {
    let Some(reference) = &snapshot.source_plan_ref else {
        if snapshot
            .profile
            .profile()
            .context_sources
            .as_ref()
            .is_some_and(|sources| !sources.is_empty())
        {
            return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.source_plan"));
        }
        return Ok(vec![]);
    };
    let record = records
        .iter()
        .find(|record| record.reference() == reference)
        .ok_or_else(|| hook_error(ErrorCode::InvalidSnapshot, "hooks.source_plan"))?;
    let plan = ContextSourcePlan::restore(
        &serde_json::to_string(record.value())
            .map_err(|_| hook_error(ErrorCode::InvalidJson, "hooks.source_plan"))?,
        &snapshot.scope,
        &reference.digest,
    )?;
    plan.validate(snapshot.profile.profile())?;
    let mut batches = vec![];
    for reference in &snapshot.context_batches {
        let record = records
            .iter()
            .find(|record| record.reference() == reference)
            .ok_or_else(|| hook_error(ErrorCode::InvalidSnapshot, "hooks.source_batch"))?;
        batches.push(ContextBatch::restore(
            record,
            &plan,
            &snapshot.scope,
            &snapshot.run_id,
        )?);
    }
    let mut items = vec![];
    for planned in plan.bindings() {
        let candidates: Vec<_> = batches
            .iter()
            .filter(|batch| {
                batch.request().binding == planned.binding
                    && batch.request().definition == planned.definition
                    && (planned.binding.trigger == ContextTrigger::RunStart
                        || batch.request().model_step_id.as_ref() == Some(step))
            })
            .collect();
        if candidates.len() != 1 {
            return Err(hook_error(
                ErrorCode::InvalidSnapshot,
                "hooks.source_generation",
            ));
        }
        let batch = candidates[0];
        if batch.request().session_id != snapshot.request.session_id
            || batch.request().user_input != snapshot.request.input
        {
            return Err(hook_error(ErrorCode::InvalidSnapshot, "hooks.source_input"));
        }
        items.extend_from_slice(batch.items());
    }
    Ok(items)
}

pub(super) fn normalize_output(
    input: &HookInput,
    mut output: HookOutput,
) -> Result<HookOutput, ContractError> {
    if let (HookInput::BeforeTool { tool, .. }, HookOutput::Tool { model_inputs, .. }) =
        (input, &mut output)
    {
        *model_inputs = crate::tool_schema::normalize_model_input_schema(
            &tool.model_input_schema,
            model_inputs,
        )
        .map_err(|_| hook_error(ErrorCode::InvalidContract, "hooks.output"))?;
    }
    Ok(output)
}
