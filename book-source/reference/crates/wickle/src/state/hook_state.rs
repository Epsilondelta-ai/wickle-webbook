use super::*;
use crate::{HookInput, HookPlan, HookPosition, HookTarget};

fn load_plan(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<Option<HookPlan>, ContractError> {
    let Some(reference) = &snapshot.hook_plan_ref else {
        if !snapshot.hook_applications.is_empty()
            || snapshot
                .profile
                .profile()
                .hooks
                .as_ref()
                .is_some_and(|hooks| !hooks.is_empty())
        {
            return Err(error(ErrorCode::InvalidSnapshot, "hooks.plan_missing"));
        }
        return Ok(None);
    };
    let value = record_value(state, additions, reference)?;
    let plan = HookPlan::restore(
        &serde_json::to_string(value)
            .map_err(|_| error(ErrorCode::InvalidSnapshot, "hooks.plan"))?,
        &snapshot.scope,
        &reference.digest,
    )?;
    plan.validate(snapshot.profile.profile())?;
    Ok(Some(plan))
}

pub(super) fn validate_hook_snapshot(
    state: &ScopeState,
    additions: &BTreeMap<RecordKey, ProtectedRecord>,
    snapshot: &RunSnapshot,
) -> Result<(), ContractError> {
    let Some(plan) = load_plan(state, additions, snapshot)? else {
        return Ok(());
    };
    let mut records = snapshot
        .hook_applications
        .iter()
        .map(|application| {
            let value = record_value(state, additions, &application.result_ref)?;
            Ok(ProtectedRecord::new(
                application.result_ref.record_id.clone(),
                application.result_ref.revision,
                value.clone(),
            ))
        })
        .collect::<Result<Vec<_>, ContractError>>()?;
    let hook_record_count = records.len();
    for reference in snapshot
        .source_plan_ref
        .iter()
        .chain(&snapshot.context_batches)
        .chain(crate::skills::records::references(snapshot))
    {
        let value = record_value(state, additions, reference)?;
        records.push(ProtectedRecord::new(
            reference.record_id.clone(),
            reference.revision,
            value.clone(),
        ));
    }
    crate::hooks::validate_application_chain(&plan, snapshot, &records)?;
    let expected = |position| {
        plan.definitions()
            .iter()
            .filter(|definition| definition.position == position)
            .count()
    };
    let applied = |target: &HookTarget| {
        snapshot
            .hook_applications
            .iter()
            .filter(|application| &application.target == target)
            .count()
    };
    if snapshot
        .model_ledger
        .iter()
        .any(|invocation| invocation.purpose == crate::ModelPurpose::Agent)
        && applied(&HookTarget::BeforeRun) != expected(HookPosition::BeforeRun)
    {
        return Err(error(
            ErrorCode::InvalidSnapshot,
            "hooks.before_run_incomplete",
        ));
    }
    for invocation in snapshot
        .model_ledger
        .iter()
        .filter(|invocation| invocation.purpose == crate::ModelPurpose::Agent)
    {
        if applied(&HookTarget::BeforeModel {
            model_step_id: invocation.model_step_id.clone(),
        }) != expected(HookPosition::BeforeModel)
        {
            return Err(error(
                ErrorCode::InvalidSnapshot,
                "hooks.before_model_incomplete",
            ));
        }
    }
    for entry in &snapshot.tool_ledger {
        if entry.call.bound_input_ref.is_some() {
            let target = HookTarget::BeforeTool {
                call_id: entry.call.call_id.clone(),
            };
            if applied(&target) != expected(HookPosition::BeforeTool) {
                return Err(error(
                    ErrorCode::InvalidSnapshot,
                    "hooks.before_tool_incomplete",
                ));
            }
            for (_, record) in snapshot
                .hook_applications
                .iter()
                .zip(&records)
                .filter(|(application, _)| application.target == target)
            {
                let result: crate::HookApplicationRecord =
                    serde_json::from_value(record.value().clone())
                        .map_err(|_| error(ErrorCode::InvalidSnapshot, "hooks.application"))?;
                if matches!(
                    result.output,
                    Some(crate::HookOutput::Tool { deny: Some(_), .. })
                ) {
                    return Err(error(ErrorCode::InvalidSnapshot, "hooks.bound_denied"));
                }
            }
        }
    }
    if hook_record_count > 0 {
        let session = state
            .sessions
            .get(&snapshot.request.session_id)
            .ok_or_else(not_found)?;
        let value = record_value(state, additions, &session.snapshot.prompt_snapshot)?;
        let prompt = crate::PromptSnapshot::restore(
            &serde_json::to_string(value)
                .map_err(|_| error(ErrorCode::InvalidSnapshot, "hooks.prompt"))?,
            &session.snapshot.prompt_snapshot.digest,
            &snapshot.profile,
            &snapshot.scope,
        )?;
        for record in records.iter().take(hook_record_count) {
            let application: crate::HookApplicationRecord =
                serde_json::from_value(record.value().clone())
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "hooks.application"))?;
            if let HookInput::BeforeTool {
                tool,
                descriptor_digest,
                compiled_digest,
                ..
            } = &application.input
            {
                if !prompt.tools().iter().any(|pinned| {
                    &pinned.model_tool == tool
                        && &pinned.descriptor_digest == descriptor_digest
                        && &pinned.compiled_digest == compiled_digest
                }) {
                    return Err(error(ErrorCode::InvalidSnapshot, "hooks.tool_contract"));
                }
            }
        }
    }
    Ok(())
}

pub(super) fn validate_hook_transition(
    previous: &RunSnapshot,
    next: &RunSnapshot,
) -> Result<(), ContractError> {
    if previous.hook_plan_ref != next.hook_plan_ref
        || !next
            .hook_applications
            .starts_with(&previous.hook_applications)
    {
        return Err(error(ErrorCode::InvalidTransition, "hooks.immutable"));
    }
    for added in &next.hook_applications[previous.hook_applications.len()..] {
        match &added.target {
            HookTarget::BeforeRun
                if previous.usage.model_calls == 0 && previous.tool_ledger.is_empty() => {}
            HookTarget::BeforeModel { model_step_id }
                if next.model_step_id.as_ref() == Some(model_step_id)
                    && !previous
                        .model_ledger
                        .iter()
                        .any(|invocation| &invocation.model_step_id == model_step_id) => {}
            HookTarget::BeforeTool { call_id }
                if previous.tool_ledger.iter().any(|entry| {
                    &entry.call.call_id == call_id
                        && entry.call.bound_input_ref.is_none()
                        && matches!(entry.state, ToolCallState::Planned {})
                }) => {}
            _ => return Err(error(ErrorCode::InvalidTransition, "hooks.target")),
        }
    }
    Ok(())
}

pub(super) fn validate_hook_observation(
    state: &ScopeState,
    scope: &Scope,
    run_id: &Id,
    report: &crate::HookObservation,
) -> Result<(), ContractError> {
    check_scope(scope, &report.scope)?;
    if run_id != &report.run_id {
        return Err(error(ErrorCode::InvalidReference, "hooks.observation_run"));
    }
    let run = state.runs.get(run_id).ok_or_else(not_found)?;
    let empty = BTreeMap::new();
    let plan = load_plan(state, &empty, &run.snapshot)?
        .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "hooks.plan_missing"))?;
    if !plan
        .definition_for(&report.hook, report.selection.as_ref())
        .is_some_and(|definition| {
            definition.digest() == report.definition_digest
                && definition.position == report.target.position()
        })
    {
        return Err(error(
            ErrorCode::InvalidReference,
            "hooks.observer_definition",
        ));
    }
    let (input, committed_at) = match &report.target {
        HookTarget::AfterTool {
            call_id,
            result_ref,
        } => {
            let result: ToolResult = event_record(state, &empty, result_ref)?;
            let event = run
                .events
                .iter()
                .find(|event| match &event.payload {
                    RunEventPayload::ToolSettled { result_ref: saved }
                    | RunEventPayload::ToolUnresolved {
                        result_ref: saved, ..
                    } => saved == result_ref,
                    _ => false,
                })
                .ok_or_else(|| error(ErrorCode::InvalidReference, "hooks.observed_result"))?;
            if call_id != &result.call_id
                || !run
                    .snapshot
                    .tool_ledger
                    .iter()
                    .any(|entry| &entry.call.call_id == call_id)
            {
                return Err(error(ErrorCode::InvalidReference, "hooks.observed_call"));
            }
            (
                HookInput::tool_observed(call_id, &result),
                event.timestamp_ms,
            )
        }
        HookTarget::AfterRun {
            outcome_ref,
            revision,
        } => {
            let outcome: crate::RunOutcome = event_record(state, &empty, outcome_ref)?;
            let event = run
                .events
                .iter()
                .find(|event| {
                    matches!(&event.payload,
                RunEventPayload::RunFinished { outcome_ref: saved } if saved == outcome_ref)
                })
                .ok_or_else(|| error(ErrorCode::InvalidReference, "hooks.observed_outcome"))?;
            if !run.snapshot.status.is_terminal()
                || &run.snapshot.revision != revision
                || run.snapshot.outcome.as_ref() != Some(&outcome)
            {
                return Err(error(ErrorCode::InvalidReference, "hooks.observed_outcome"));
            }
            (HookInput::run_observed(&outcome), event.timestamp_ms)
        }
        _ => return Err(error(ErrorCode::InvalidReference, "hooks.observer_target")),
    };
    if report.timestamp_ms < committed_at
        || report.input_digest
            != canonical_digest(
                &serde_json::to_value(&input)
                    .map_err(|_| error(ErrorCode::InvalidSnapshot, "hooks.observer_input"))?,
            )
    {
        return Err(error(ErrorCode::InvalidSnapshot, "hooks.observer_input"));
    }
    Ok(())
}
