use super::engine::{ContextServices, PreparedContext, bounded};
use super::*;

pub(super) struct Compression<'a, 'b> {
    pub prompt: &'a PromptSnapshot,
    pub seed: &'a ProjectionInput<'b>,
    pub saved: &'a StoredRun,
    pub plan: &'a ContextPlan,
    pub candidate: ContextRevision,
    pub view: &'a [Message],
    pub controls: &'a ContextStrategyContext,
}
impl ContextRuntime {
    pub(super) async fn compress(
        &self,
        work: Compression<'_, '_>,
        services: &ContextServices<'_>,
    ) -> Result<PreparedContext, ContractError> {
        let Compression {
            prompt,
            seed,
            saved,
            plan,
            mut candidate,
            view,
            controls,
        } = work;
        let compactor = self.compactor.as_ref().ok_or_else(|| {
            context_error(ErrorCode::ContextBudgetExceeded, "context.no_compactor")
        })?;
        let segments = records::segments(view, &self.scope)?;
        let input = ContextSelectionInput {
            scope: self.scope.clone(),
            run_id: saved.snapshot.run_id.clone(),
            config: plan.policy.config.clone().unwrap_or_default(),
            segments,
            has_summary: candidate.summary.is_some(),
            max_input_bytes: self.limits.max_compactor_input_bytes,
        };
        let selected = bounded(controls, services, async {
            self.strategy.select(&input, controls).await
        })
        .await?;
        records::validate_selection(view, &selected)?;
        if selected.is_empty() && candidate.summary.is_none() {
            return Err(context_error(
                ErrorCode::ContextBudgetExceeded,
                "context.protected_input",
            ));
        }
        let segments: Vec<_> = input
            .segments
            .into_iter()
            .filter(|segment| segment.message_ids.iter().all(|id| selected.contains(id)))
            .collect();
        let request_id = Id::new(format!(
            "compaction-{}",
            canonical_digest(&serde_json::json!([
                self.scope,
                saved.snapshot.run_id,
                seed.model_step_id,
                plan.digest(),
                candidate.parent,
                seed.route.digest(),
                saved.snapshot.request.input,
                candidate.summary,
                segments
            ]))
        ))?;
        let request = CompactionRequest {
            request_id: request_id.clone(),
            scope: self.scope.clone(),
            run_id: saved.snapshot.run_id.clone(),
            current_input: saved.snapshot.request.input.clone(),
            previous_summary: candidate.summary.clone(),
            segments,
        };
        if serde_json::to_vec(&request)
            .map_err(|_| context_error(ErrorCode::InvalidJson, "context.compactor_input"))?
            .len()
            > self.limits.max_compactor_input_bytes
        {
            return Err(context_error(
                ErrorCode::ContextBudgetExceeded,
                "context.compactor_input",
            ));
        }
        for reference in &saved.snapshot.context_decisions {
            let record = services
                .bindings
                .state
                .read_record(&self.scope, reference)
                .await?;
            let prior: ContextDecision = serde_json::from_value(record.value().clone())
                .map_err(|_| context_error(ErrorCode::InvalidSnapshot, "context.decision"))?;
            if prior.request_id == request_id {
                return Err(context_error(
                    prior
                        .failure
                        .unwrap_or(ErrorCode::ContextCompactionNoReduction),
                    "context.completed_decision",
                ));
            }
        }
        if saved.snapshot.context_decisions.len() as u64 >= self.limits.max_compactions {
            return Err(context_error(
                ErrorCode::ContextBudgetExceeded,
                "context.compaction_limit",
            ));
        }
        services.budget.check_boundary().await?;
        let decision = ContextDecision {
            schema_version: "wickle.context-decision.v1".into(),
            scope: self.scope.clone(),
            run_id: saved.snapshot.run_id.clone(),
            model_step_id: seed.model_step_id.clone(),
            request_id,
            request_digest: crate::serialization::data_digest(&request),
            request: request.clone(),
            source_revision_ref: saved.snapshot.context_revision_ref.clone(),
            through_sequence: saved.session.transcript_revision,
            previews: candidate.previews.clone(),
            revision_ref: None,
            failure: None,
        };
        let result = match compactor {
            ContextCompactor::Host { compressor, .. } => {
                bounded(controls, services, async {
                    compressor.compact(&request, controls).await
                })
                .await
            }
            ContextCompactor::Model(config) => {
                bounded(
                    controls,
                    services,
                    Box::pin(self.model_summary(&request, config, services)),
                )
                .await
            }
        };
        let summary = match result {
            Ok(summary)
                if !summary.trim().is_empty() && summary.len() <= self.limits.max_summary_bytes =>
            {
                summary
            }
            Ok(_) => {
                self.reject(decision, ErrorCode::ContextCompactionNoReduction, services)
                    .await?;
                return Err(context_error(
                    ErrorCode::ContextCompactionNoReduction,
                    "context.summary_size",
                ));
            }
            Err(error) => {
                self.reject(decision, error.code, services).await?;
                return Err(error);
            }
        };
        candidate.covered_message_ids.extend(selected);
        candidate.covered_message_ids = saved
            .messages
            .iter()
            .filter(|message| candidate.covered_message_ids.contains(&message.message_id))
            .map(|message| message.message_id.clone())
            .collect();
        candidate.covered_digest =
            records::covered_digest(&saved.messages, &candidate.covered_message_ids);
        candidate.summary = Some(summary);
        candidate.anchors = records::anchors(
            &saved.messages,
            &candidate.covered_message_ids,
            &candidate.previews,
        );
        let view = records::apply(&saved.messages, Some(&candidate))?;
        let items = Self::items(seed.context_items, Some(&candidate), plan)?;
        let after = self
            .render(prompt, seed, &view, &items, false, services)
            .await;
        let (projection, tokens) = match after {
            Ok((projection, tokens))
                if self.token_fit(&projection.request, tokens, services)?
                    && Self::size(&projection.request)? < candidate.before_bytes
                    && tokens <= candidate.before_tokens =>
            {
                (projection, tokens)
            }
            Ok(_) => {
                self.reject(decision, ErrorCode::ContextCompactionNoReduction, services)
                    .await?;
                return Err(context_error(
                    ErrorCode::ContextCompactionNoReduction,
                    "context.no_reduction",
                ));
            }
            Err(error) => {
                self.reject(decision, error.code, services).await?;
                return Err(error);
            }
        };
        candidate.after_bytes = Self::size(&projection.request)?;
        candidate.after_tokens = tokens;
        self.authorize(saved, &seed.route, controls, services)
            .await?;
        self.commit(saved, &candidate, Some(decision), services)
            .await?;
        Ok(PreparedContext {
            artifacts: Self::artifact_refs(&projection, Some(&candidate), plan)?,
            projection,
            input_tokens: tokens,
        })
    }
}
