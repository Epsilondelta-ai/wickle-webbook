use super::*;
use crate::{ContextBatch, ContextSourcePlan, ContextTrigger};

pub(super) fn validate_source_snapshot(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    let invalid = |field| error(ErrorCode::InvalidSnapshot, field);
    let Some(plan_ref) = &snapshot.source_plan_ref else {
        if !snapshot.context_batches.is_empty()
            || !snapshot.source_states.is_empty()
            || snapshot
                .profile
                .profile()
                .context_sources
                .as_ref()
                .is_some_and(|sources| !sources.is_empty())
        {
            return Err(invalid("sources.plan_missing"));
        }
        return Ok(());
    };
    let value = record_value(state, additions, plan_ref)?;
    let plan = ContextSourcePlan::restore(
        &serde_json::to_string(value).map_err(|_| invalid("sources.plan"))?,
        &snapshot.scope,
        &plan_ref.digest,
    )?;
    plan.validate(snapshot.profile.profile())?;
    if let Some(reference) = &snapshot.assembly_ref {
        let assembly = record_value(state, additions, reference)?;
        let sources: Vec<crate::ResolvedSourceBinding> = serde_json::from_value(
            assembly
                .get("sources")
                .cloned()
                .unwrap_or(Value::Array(vec![])),
        )
        .map_err(|_| invalid("sources.assembly"))?;
        if sources.len() != plan.bindings().len()
            || sources
                .iter()
                .zip(plan.bindings())
                .any(|(source, planned)| {
                    source.binding != planned.binding || source.definition != planned.definition
                })
        {
            return Err(invalid("sources.assembly_plan"));
        }
    }
    let mut batches = Vec::new();
    let mut requests = BTreeSet::new();
    let mut ids = BTreeSet::new();
    let agent_steps: Vec<_> = snapshot
        .model_ledger
        .iter()
        .filter(|invocation| invocation.purpose == crate::ModelPurpose::Agent)
        .map(|invocation| &invocation.model_step_id)
        .collect();
    if plan
        .bindings()
        .iter()
        .any(|entry| entry.binding.trigger == ContextTrigger::BeforeModel)
    {
        if let Some(current) = &snapshot.model_step_id {
            if agent_steps.contains(&current) && agent_steps.last().copied() != Some(current) {
                return Err(invalid("sources.step_rollback"));
            }
        } else if !agent_steps.is_empty() {
            return Err(invalid("sources.current_step_missing"));
        }
    }
    for reference in &snapshot.context_batches {
        let value = record_value(state, additions, reference)?;
        let record = ProtectedRecord::new(
            reference.record_id.clone(),
            reference.revision,
            value.clone(),
        );
        let batch = ContextBatch::restore(&record, &plan, &snapshot.scope, &snapshot.run_id)?;
        if batch.request().session_id != snapshot.request.session_id
            || batch.request().user_input != snapshot.request.input
            || !requests.insert(batch.request().context_request_id.clone())
            || !ids.insert(reference.record_id.clone())
        {
            return Err(invalid("sources.batch_identity"));
        }
        if let Some(step) = &batch.request().model_step_id {
            // A prepared but uninvoked step is legitimate only while current.
            // Historical generations are anchored by the model ledger.
            if snapshot.model_step_id.as_ref() != Some(step) && !agent_steps.contains(&step) {
                return Err(invalid("sources.orphan_step"));
            }
        }
        batches.push(batch);
    }
    for slot in &snapshot.source_states {
        let batch = batches
            .iter()
            .find(|batch| batch.reference() == slot.batch_ref)
            .ok_or_else(|| invalid("sources.slot_batch"))?;
        let request = batch.request();
        if slot.source != request.binding.source
            || slot.trigger != request.binding.trigger
            || slot.context_request_id != request.context_request_id
            || slot.model_step_id != request.model_step_id
            || slot.trigger == ContextTrigger::BeforeModel
                && slot.model_step_id != snapshot.model_step_id
        {
            return Err(invalid("sources.slot_identity"));
        }
    }
    for entry in plan.bindings() {
        let current_step = if entry.binding.trigger == ContextTrigger::BeforeModel {
            snapshot.model_step_id.as_ref()
        } else {
            None
        };
        let batch = batches.iter().find(|batch| {
            batch.request().binding == entry.binding
                && batch.request().model_step_id.as_ref() == current_step
        });
        let slots: Vec<_> = snapshot
            .source_states
            .iter()
            .filter(|slot| {
                slot.source == entry.binding.source && slot.trigger == entry.binding.trigger
            })
            .collect();
        match (batch, slots.as_slice()) {
            (None, []) => {}
            (Some(batch), [slot]) if slot.batch_ref == batch.reference() => {}
            _ => return Err(invalid("sources.active_slot")),
        }
    }
    for invocation in snapshot
        .model_ledger
        .iter()
        .filter(|invocation| invocation.purpose == crate::ModelPurpose::Agent)
    {
        for entry in plan.bindings() {
            let step = if entry.binding.trigger == ContextTrigger::BeforeModel {
                Some(&invocation.model_step_id)
            } else {
                None
            };
            let batch = batches
                .iter()
                .find(|batch| {
                    batch.request().binding == entry.binding
                        && batch.request().model_step_id.as_ref() == step
                })
                .ok_or_else(|| invalid("sources.model_without_batch"))?;
            if entry.binding.required
                && matches!(batch.result(), crate::ContextResult::Unavailable { .. })
            {
                return Err(invalid("sources.required_unavailable_model"));
            }
        }
    }
    Ok(())
}

pub(super) fn validate_source_transition(
    previous: &RunSnapshot,
    next: &RunSnapshot,
) -> Result<(), ContractError> {
    if previous.source_plan_ref != next.source_plan_ref
        || !next.context_batches.starts_with(&previous.context_batches)
    {
        return Err(error(ErrorCode::InvalidTransition, "sources.immutable"));
    }
    for reference in &next.context_batches[previous.context_batches.len()..] {
        if previous.status != RunStatus::Running || next.status != RunStatus::Running {
            return Err(error(
                ErrorCode::InvalidTransition,
                "sources.collection_status",
            ));
        }
        let slot = next
            .source_states
            .iter()
            .find(|slot| &slot.batch_ref == reference)
            .ok_or_else(|| error(ErrorCode::InvalidTransition, "sources.new_slot"))?;
        let valid = match slot.trigger {
            ContextTrigger::RunStart => {
                previous.model_ledger.is_empty() && previous.usage.model_calls == 0
            }
            ContextTrigger::BeforeModel => {
                slot.model_step_id == previous.model_step_id
                    && next.model_step_id == previous.model_step_id
                    && !previous.model_ledger.iter().any(|invocation| {
                        Some(&invocation.model_step_id) == slot.model_step_id.as_ref()
                    })
            }
        };
        if !valid {
            return Err(error(
                ErrorCode::InvalidTransition,
                "sources.collection_generation",
            ));
        }
    }
    Ok(())
}
