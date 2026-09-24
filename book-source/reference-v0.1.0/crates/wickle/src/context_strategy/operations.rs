use super::engine::{ContextServices, bounded};
use super::*;

impl ContextRuntime {
    pub(super) async fn preview(
        &self,
        saved: &StoredRun,
        candidate: &mut ContextRevision,
        controls: &ContextStrategyContext,
        services: &ContextServices<'_>,
    ) -> Result<(), ContractError> {
        let Some(artifacts) = &services.bindings.artifacts else {
            return Ok(());
        };
        let corrected: std::collections::BTreeSet<_> = saved
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|block| match block {
                ContentBlock::ToolResultCorrection {
                    previous_message_id,
                    ..
                } => Some(previous_message_id),
                _ => None,
            })
            .collect();
        let mut count = 0;
        for message in &saved.messages {
            if candidate.covered_message_ids.contains(&message.message_id)
                || corrected.contains(&message.message_id)
            {
                continue;
            }
            for block in &message.content {
                let ContentBlock::ToolResult { result } = block else {
                    continue;
                };
                if result.effect == ToolEffect::Unknown
                    || result.status == ToolResultStatus::Unknown
                {
                    continue;
                }
                for (index, item) in result.content.iter().enumerate() {
                    if count >= self.limits.max_previews {
                        return Ok(());
                    }
                    if candidate.previews.iter().any(|preview| {
                        preview.message_id == message.message_id
                            && preview.tool_call_id == result.call_id
                            && preview.content_index == index
                    }) {
                        continue;
                    }
                    let Some((bytes, media_type)) = records::payload(item) else {
                        continue;
                    };
                    if bytes.len() <= self.limits.preview_above_bytes {
                        continue;
                    }
                    services.budget.check_boundary().await?;
                    let original_digest = crate::serialization::data_digest(item);
                    let artifact_id = Id::new(format!(
                        "context-preview-{}",
                        canonical_digest(&serde_json::json!([
                            self.scope,
                            saved.snapshot.request.session_id,
                            message.message_id,
                            result.call_id,
                            index,
                            original_digest
                        ]))
                    ))?;
                    let metadata = bounded(
                        controls,
                        services,
                        artifacts.put_named(
                            artifact_id,
                            ArtifactInput {
                                media_type: Id::new(media_type)?,
                                bytes,
                                source: None,
                            },
                            services.context,
                            Some(controls.deadline),
                        ),
                    )
                    .await?;
                    let preview = bounded(
                        controls,
                        services,
                        artifacts.preview(
                            &metadata.reference,
                            services.context,
                            Some(controls.deadline),
                        ),
                    )
                    .await?;
                    candidate.previews.push(ContextPreview {
                        message_id: message.message_id.clone(),
                        tool_call_id: result.call_id.clone(),
                        content_index: index,
                        original_digest,
                        preview,
                    });
                    count += 1;
                }
            }
        }
        Ok(())
    }
    pub(super) async fn commit(
        &self,
        source: &StoredRun,
        revision: &ContextRevision,
        decision: Option<ContextDecision>,
        services: &ContextServices<'_>,
    ) -> Result<RecordRef, ContractError> {
        services.budget.check_boundary().await?;
        let saved = services
            .bindings
            .state
            .load(&self.scope, services.budget.run_id())
            .await?;
        if saved.snapshot.context_revision_ref != source.snapshot.context_revision_ref
            || saved.session.transcript_revision != source.session.transcript_revision
            || saved.snapshot.model_step_id.as_ref() != Some(&revision.model_step_id)
        {
            return Err(context_error(
                ErrorCode::RevisionConflict,
                "context.source_changed",
            ));
        }
        let record = ProtectedRecord::new(
            services.bindings.ids.next_id()?,
            1,
            serde_json::to_value(revision)
                .map_err(|_| context_error(ErrorCode::InvalidJson, "context.revision"))?,
        );
        let plan = self.plan(saved.snapshot.profile.profile(), &self.scope)?;
        ContextRevision::restore(
            &record,
            &plan,
            &self.scope,
            &saved.snapshot.request.session_id,
            &saved.messages,
        )?;
        let reference = record.reference().clone();
        let mut records = vec![record];
        let mut snapshot = saved.snapshot;
        let expected_revision = snapshot.revision;
        snapshot.context_revision_ref = Some(reference.clone());
        if let Some(mut decision) = decision {
            decision.revision_ref = Some(reference.clone());
            decision.failure = None;
            let record = ProtectedRecord::new(
                services.bindings.ids.next_id()?,
                1,
                serde_json::to_value(decision)
                    .map_err(|_| context_error(ErrorCode::InvalidJson, "context.decision"))?,
            );
            snapshot.context_decisions.push(record.reference().clone());
            records.push(record);
        }
        let (elapsed, now) = services.budget.settlement_time(snapshot.usage.elapsed_ms)?;
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| context_error(ErrorCode::RevisionConflict, "context.revision"))?;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        snapshot.last_event_seq = snapshot
            .last_event_seq
            .checked_add(1)
            .ok_or_else(|| context_error(ErrorCode::InvalidEvent, "context.event_sequence"))?;
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: services.bindings.ids.next_id()?,
            scope: self.scope.clone(),
            run_id: snapshot.run_id.clone(),
            session_id: snapshot.request.session_id.clone(),
            seq: snapshot
                .last_event_seq
                .try_into()
                .map_err(|_| context_error(ErrorCode::InvalidEvent, "context.event_sequence"))?,
            timestamp_ms: now,
            payload: RunEventPayload::ContextRewritten {
                revision_ref: reference.clone(),
            },
        };
        let result = services
            .bindings
            .state
            .commit(
                &self.scope,
                &snapshot.run_id.clone(),
                CommitInput {
                    expected_revision,
                    lease: services.budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages: vec![],
                    events: vec![event],
                    records,
                },
            )
            .await;
        if let Err(error) = result {
            if !services
                .bindings
                .state
                .load(&self.scope, services.budget.run_id())
                .await
                .is_ok_and(|saved| saved.snapshot.context_revision_ref.as_ref() == Some(&reference))
            {
                return Err(error);
            }
        }
        Ok(reference)
    }
    pub(super) async fn reject(
        &self,
        mut decision: ContextDecision,
        code: ErrorCode,
        services: &ContextServices<'_>,
    ) -> Result<(), ContractError> {
        let mut snapshot = services
            .bindings
            .state
            .load(&self.scope, services.budget.run_id())
            .await?
            .snapshot;
        decision.failure = Some(code);
        decision.revision_ref = None;
        let record = ProtectedRecord::new(
            services.bindings.ids.next_id()?,
            1,
            serde_json::to_value(decision)
                .map_err(|_| context_error(ErrorCode::InvalidJson, "context.decision"))?,
        );
        let expected_revision = snapshot.revision;
        let (elapsed, now) = services.budget.settlement_time(snapshot.usage.elapsed_ms)?;
        snapshot.revision += 1;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        snapshot.context_decisions.push(record.reference().clone());
        services
            .bindings
            .state
            .commit(
                &self.scope,
                services.budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: services.budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages: vec![],
                    events: vec![],
                    records: vec![record],
                },
            )
            .await?;
        Ok(())
    }
}
