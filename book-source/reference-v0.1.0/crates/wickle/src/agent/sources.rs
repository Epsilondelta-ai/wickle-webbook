use super::*;

/// Active batches in original profile order. History is retained in context_batches;
/// an old Ready batch never substitutes for the current empty/unavailable slot.
pub(super) fn source_refs(
    snapshot: &RunSnapshot,
    step: Option<&Id>,
) -> Result<Vec<RecordRef>, ContractError> {
    let mut refs = vec![];
    for binding in snapshot.profile.profile().context_sources.iter().flatten() {
        if binding.trigger == ContextTrigger::BeforeModel && step.is_none() {
            continue;
        }
        let found: Vec<_> = snapshot
            .source_states
            .iter()
            .filter(|state| {
                state.source == binding.source
                    && state.trigger == binding.trigger
                    && (binding.trigger == ContextTrigger::RunStart
                        || state.model_step_id.as_ref() == step)
            })
            .collect();
        if found.len() != 1 {
            return Err(fail(ErrorCode::InvalidSnapshot, "sources.active_slot"));
        }
        refs.push(found[0].batch_ref.clone());
    }
    Ok(refs)
}

impl Agent {
    pub(super) async fn collect_sources(
        &self,
        trigger: ContextTrigger,
        step: Option<Id>,
        segment: &SegmentBindings,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let Some(sources) = &segment.sources else {
            return Ok(());
        };
        sources
            .collect(trigger, step, &segment.context, budget)
            .await?;
        Ok(())
    }
    pub(super) async fn source_context(
        &self,
        step: &Id,
        segment: &SegmentBindings,
        budget: &RunBudget,
    ) -> Result<(Vec<RecordRef>, Vec<ContextItem>), ContractError> {
        let Some(sources) = &segment.sources else {
            return Ok((vec![], vec![]));
        };
        let saved = self
            .inner
            .bindings
            .state
            .load(budget.scope(), budget.run_id())
            .await?;
        let refs = source_refs(&saved.snapshot, Some(step))?;
        let items = sources
            .authorize_use(
                budget.run_id(),
                &refs,
                None,
                &segment.context,
                budget.call_deadline()?,
            )
            .await?;
        Ok((refs, items))
    }
}
