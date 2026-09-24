use super::*;

pub(crate) fn references(snapshot: &RunSnapshot) -> Vec<&RecordRef> {
    snapshot
        .skill_plan_ref
        .iter()
        .chain(snapshot.tool_ledger.iter().filter_map(|entry| {
            if let ToolCallState::Settled { result } = &entry.state {
                result.skill_ref.as_ref()
            } else {
                None
            }
        }))
        .collect()
}
pub(crate) fn loaded(
    plan: &SkillPlan,
    snapshot: &RunSnapshot,
    entry: &ToolLedgerEntry,
    record: &ProtectedRecord,
) -> Result<LoadedSkill, ContractError> {
    let ToolCallState::Settled { result } = &entry.state else {
        return Err(skill_error(ErrorCode::InvalidSnapshot, "skills.unsettled"));
    };
    let body: LoadedSkill = serde_json::from_value(record.value().clone())
        .map_err(|_| skill_error(ErrorCode::InvalidSnapshot, "skills.record"))?;
    body.validate(plan, &snapshot.run_id, &entry.call.call_id)?;
    if serde_json::to_vec(record.value())
        .map_err(|_| skill_error(ErrorCode::InvalidJson, "skills.record"))?
        .len() as u64
        > runtime::loader_descriptor(plan.limits)?
            .max_output_bytes
            .get()
    {
        return Err(skill_error(
            ErrorCode::InvalidSnapshot,
            "skills.output_size",
        ));
    }
    let name = match &plan.loader {
        ToolBindingRef::Export(reference) => {
            reference.alias.clone().unwrap_or(Id::new("skills_load")?)
        }
        ToolBindingRef::Catalog(_) => Id::new("skills_load")?,
    };
    if result.skill_ref.as_ref() != Some(record.reference())
        || entry.call.descriptor_digest.as_ref() != Some(plan.loader_digest())
        || entry.call.tool_name != name
        || result.status != ToolResultStatus::Succeeded
        || result.effect != ToolEffect::NotApplied
        || result.effect_receipt_ref.is_some()
        || result.error.is_some()
        || result.content
            != vec![InputContent::Json {
                value: body.summary(),
            }]
    {
        return Err(skill_error(ErrorCode::InvalidSnapshot, "skills.result"));
    }
    Ok(body)
}
pub(crate) fn context_for_step(
    snapshot: &RunSnapshot,
    records: &[ProtectedRecord],
    step: &Id,
) -> Result<Vec<ContextItem>, ContractError> {
    let Some(reference) = &snapshot.skill_plan_ref else {
        return Ok(vec![]);
    };
    let find = |reference: &RecordRef| {
        records
            .iter()
            .find(|record| record.reference() == reference)
            .ok_or_else(|| skill_error(ErrorCode::InvalidSnapshot, "skills.record"))
    };
    let plan = SkillPlan::restore(find(reference)?, &snapshot.profile)?;
    let target = snapshot
        .model_ledger
        .iter()
        .position(|invocation| &invocation.model_step_id == step)
        .unwrap_or(snapshot.model_ledger.len());
    let mut bodies = vec![];
    for entry in &snapshot.tool_ledger {
        let ToolCallState::Settled { result } = &entry.state else {
            continue;
        };
        let Some(reference) = &result.skill_ref else {
            continue;
        };
        let index = snapshot
            .model_ledger
            .iter()
            .position(|invocation| invocation.attempt_id == entry.call.model_request_id)
            .ok_or_else(|| skill_error(ErrorCode::InvalidSnapshot, "skills.model_step"))?;
        if index >= target {
            continue;
        }
        let body = loaded(&plan, snapshot, entry, find(reference)?)?;
        if !bodies
            .iter()
            .any(|prior: &LoadedSkill| prior.selection == body.selection)
        {
            bodies.push(body);
        }
    }
    plan.skills
        .iter()
        .filter_map(|entry| bodies.iter().find(|body| body.selection == entry.selection))
        .map(LoadedSkill::context_item)
        .collect()
}
