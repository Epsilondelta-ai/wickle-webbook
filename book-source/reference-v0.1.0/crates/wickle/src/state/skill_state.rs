use super::*;
use crate::{JsonObject, PromptSnapshot, SkillPlan, ToolResultStatus};

pub(super) fn validate_skill_snapshot(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    let results = snapshot
        .tool_ledger
        .iter()
        .filter_map(|entry| {
            if let ToolCallState::Settled { result } = &entry.state {
                Some((entry, result))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let Some(reference) = &snapshot.skill_plan_ref else {
        if !snapshot.profile.profile().skills.is_empty()
            || results.iter().any(|(_, result)| result.skill_ref.is_some())
        {
            return Err(error(ErrorCode::InvalidSnapshot, "skills.plan_missing"));
        }
        return Ok(());
    };
    let record = ProtectedRecord::new(
        reference.record_id.clone(),
        reference.revision,
        record_value(state, additions, reference)?.clone(),
    );
    let plan = SkillPlan::restore(&record, &snapshot.profile)?;
    if let Some(session) = state.sessions.get(&snapshot.request.session_id) {
        let value = record_value(state, additions, &session.snapshot.prompt_snapshot)?;
        let prompt = PromptSnapshot::restore(
            &value.to_string(),
            &session.snapshot.prompt_snapshot.digest,
            &snapshot.profile,
            &snapshot.scope,
        )?;
        if prompt.skills() != plan.listings() {
            return Err(error(ErrorCode::InvalidSnapshot, "skills.prompt_listing"));
        }
    }
    let mut bodies = BTreeSet::new();
    let mut bytes = 0u64;
    for (entry, result) in results {
        let Some(reference) = &result.skill_ref else {
            if result.status == ToolResultStatus::Succeeded
                && entry.call.descriptor_digest.as_ref() == Some(plan.loader_digest())
            {
                return Err(error(
                    ErrorCode::InvalidSnapshot,
                    "skills.loader_without_body",
                ));
            }
            continue;
        };
        let record = ProtectedRecord::new(
            reference.record_id.clone(),
            reference.revision,
            record_value(state, additions, reference)?.clone(),
        );
        let loaded = crate::skills::records::loaded(&plan, snapshot, entry, &record)?;
        let bound = entry
            .call
            .bound_input_ref
            .as_ref()
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "skills.bound_input"))?;
        let value = record_value(state, additions, bound)?;
        let args: JsonObject = serde_json::from_value(value["data"]["execution_args"].clone())
            .map_err(|_| error(ErrorCode::InvalidSnapshot, "skills.bound_input"))?;
        if !loaded.matches_args(&args) {
            return Err(error(ErrorCode::InvalidSnapshot, "skills.bound_selection"));
        }
        if let crate::ToolBindingRef::Export(_) = plan.loader() {
            let selection: crate::ToolBindingRef =
                serde_json::from_value(value["data"]["selection"].clone())
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "skills.bound_export"))?;
            if &selection != plan.loader() {
                return Err(error(ErrorCode::InvalidSnapshot, "skills.bound_export"));
            }
        }
        if bodies.insert(crate::serialization::data_digest(loaded.selection())) {
            bytes = bytes
                .checked_add(loaded.body().len() as u64)
                .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "skills.body_bytes"))?;
        }
    }
    if bytes > plan.max_total_body_bytes() {
        return Err(error(ErrorCode::ContextBudgetExceeded, "skills.total_body"));
    }
    Ok(())
}
