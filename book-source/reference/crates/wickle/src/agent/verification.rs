use super::driver::PreparedOutcome;
use super::*;
use crate::verification::VerificationRecord;
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;

pub(super) enum CandidateAction {
    Finish(Box<PreparedOutcome>),
    Repair,
}
impl Agent {
    pub(super) async fn verification_plan(
        &self,
        snapshot: &RunSnapshot,
    ) -> Result<VerificationPlan, ContractError> {
        let expected = self.inner.verification.plan(
            snapshot.profile.profile(),
            snapshot.request.output_contract.as_ref(),
        )?;
        if let Some(reference) = &snapshot.verification_plan_ref {
            let record = self
                .inner
                .bindings
                .state
                .read_record(&snapshot.scope, reference)
                .await?;
            let plan = VerificationPlan::restore(&record, snapshot)?;
            if plan != expected {
                return Err(fail(ErrorCode::ContextMismatch, "agent.verification_plan"));
            }
            Ok(plan)
        } else if matches!(
            snapshot.profile.profile().completion_policy,
            CompletionPolicy::TurnEnd {}
        ) && matches!(expected.output, OutputContract::Text {})
        {
            Ok(expected)
        } else {
            Err(fail(ErrorCode::InvalidSnapshot, "agent.verification_plan"))
        }
    }
    pub(super) async fn candidate(
        &self,
        response: &ModelResponse,
        budget: &RunBudget,
    ) -> Result<RecordRef, ContractError> {
        budget.check_boundary().await?;
        let bindings = &self.inner.bindings;
        let saved = bindings.state.load(budget.scope(), budget.run_id()).await?;
        let plan = self.verification_plan(&saved.snapshot).await?;
        let invocation = saved
            .snapshot
            .model_ledger
            .iter()
            .rev()
            .find(|entry| {
                entry.purpose == ModelPurpose::Agent && entry.attempt_id == response.request_id
            })
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "verification.model_step"))?;
        let (output, format_error) = match plan.parse(&response.text) {
            Ok(output) => (output, None),
            Err(error) => (
                vec![InputContent::Text {
                    text: response.text.clone(),
                }],
                Some(error),
            ),
        };
        let candidate = VerificationCandidate {
            scope: saved.snapshot.scope.clone(),
            run_id: saved.snapshot.run_id.clone(),
            model_step_id: invocation.model_step_id.clone(),
            response_ref: invocation
                .response_ref
                .clone()
                .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "verification.response"))?,
            through_sequence: saved.session.transcript_revision,
            evidence_message_ids: if plan.verifier.is_some() {
                saved
                    .messages
                    .iter()
                    .filter(|message| {
                        message.run_id == saved.snapshot.run_id
                            && message
                                .content
                                .iter()
                                .any(|block| matches!(block, ContentBlock::ToolResult { .. }))
                    })
                    .map(|message| message.message_id.clone())
                    .collect()
            } else {
                vec![]
            },
            output,
            format_error,
        };
        let record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(candidate)
                .map_err(|_| fail(ErrorCode::InvalidJson, "verification.candidate"))?,
        );
        let reference = record.reference().clone();
        let mut snapshot = saved.snapshot;
        snapshot.candidate_ref = Some(reference.clone());
        snapshot.phase = RunPhase::Verify;
        self.commit_verification(snapshot, vec![record], vec![], vec![], budget)
            .await?;
        Ok(reference)
    }
    pub(super) async fn verify_candidate(
        &self,
        segment: &SegmentBindings,
        budget: &RunBudget,
    ) -> Result<CandidateAction, ContractError> {
        let bindings = &self.inner.bindings;
        let saved = bindings.state.load(budget.scope(), budget.run_id()).await?;
        let plan = self.verification_plan(&saved.snapshot).await?;
        let candidate_ref =
            saved.snapshot.candidate_ref.clone().ok_or_else(|| {
                fail(ErrorCode::InvalidSnapshot, "verification.candidate_missing")
            })?;
        let candidate: VerificationCandidate = self.read_verification(&candidate_ref).await?;
        let response: StoredModelResponse = self.read_verification(&candidate.response_ref).await?;
        let continuation = match response.outcome {
            ModelExchangeOutcome::Completed { response } => response.continuation,
            _ => return Err(fail(ErrorCode::InvalidSnapshot, "verification.response")),
        };
        if let Some(sources) = segment.sources.as_deref() {
            let messages = candidate_lineage_messages(&saved, &candidate)?;
            let deadline = budget.call_deadline()?;
            let lineage = crate::future::boxed(|| {
                sources.lineage_for_messages(
                    &saved.snapshot.request.session_id,
                    &messages,
                    &segment.context,
                    deadline,
                )
            })
            .await?;
            crate::future::boxed(|| {
                sources.authorize_lineage(
                    budget.run_id(),
                    &lineage,
                    None,
                    &segment.context,
                    deadline,
                )
            })
            .await?;
        }
        // Completed records are replayed rather than re-invoking a quality callback.
        let mut prior = None;
        for reference in saved.snapshot.verification_records.iter().rev() {
            let record: VerificationRecord = self.read_verification(reference).await?;
            if record.candidate_ref == candidate_ref {
                prior = Some(record);
                break;
            }
        }
        let review=saved.snapshot.resume_receipts.last().filter(|receipt|matches!(&receipt.command.action,ResumeAction::Approve{target:ApprovalTarget::Candidate{candidate_ref:target,..},..}|ResumeAction::Deny{target:ApprovalTarget::Candidate{candidate_ref:target,..},..} if target==&candidate_ref));
        let record = if let Some(prior) = prior.filter(|prior| {
            !matches!(prior.decision, Some(VerificationDecision::Wait { .. })) || review.is_none()
        }) {
            prior
        } else {
            budget.check_boundary().await?;
            let request = PolicyRequest {
                owner_scope: bindings.scope.clone(),
                resource_id: budget.run_id().clone(),
                action: PolicyAction::VerifyCandidate {
                    candidate_ref: candidate_ref.clone(),
                    verifier_ref: plan
                        .verifier
                        .as_ref()
                        .map(|definition| definition.verifier_ref.clone()),
                },
            };
            match bindings
                .policy
                .check(
                    &request,
                    &segment.context,
                    Some(budget.call_deadline()?),
                    None,
                )
                .await?
            {
                PolicyDecision::Allow {} => {}
                _ => return Err(fail(ErrorCode::AccessDenied, "verification.policy")),
            }
            let decision = if let Some(receipt) = review {
                match &receipt.command.action {
                    ResumeAction::Approve { .. } => Ok(VerificationDecision::Pass {}),
                    ResumeAction::Deny { reason, .. } => Ok(VerificationDecision::Fail {
                        reason: reason.clone(),
                    }),
                    _ => unreachable!(),
                }
            } else if let Some(error) = &candidate.format_error {
                Ok(VerificationDecision::Revise {
                    feedback: error.to_string(),
                })
            } else if plan.verifier.is_none() {
                Ok(VerificationDecision::Pass {})
            } else {
                let input = VerificationInput {
                    candidate_ref: candidate_ref.clone(),
                    candidate: candidate.clone(),
                    request: saved.snapshot.request.input.clone(),
                    evidence: saved
                        .messages
                        .iter()
                        .filter(|message| {
                            candidate.evidence_message_ids.contains(&message.message_id)
                        })
                        .flat_map(|message| &message.content)
                        .filter_map(|block| {
                            if let ContentBlock::ToolResult { result } = block {
                                Some(result.content.clone())
                            } else {
                                None
                            }
                        })
                        .flatten()
                        .collect(),
                };
                let size = serde_json::to_vec(&serde_json::json!([
                    input.candidate,
                    input.request,
                    input.evidence
                ]))
                .map_err(|_| fail(ErrorCode::InvalidJson, "verification.input"))?
                .len();
                if size > plan.limits.max_input_bytes {
                    return Err(fail(ErrorCode::InvalidContract, "verification.input_size"));
                }
                if let Some(artifacts) = &bindings.artifacts {
                    artifacts
                        .validate_content(
                            &input.evidence,
                            &segment.context,
                            Some(budget.call_deadline()?),
                        )
                        .await?;
                }
                let token = segment.context.cancellation.child_token();
                let _cancel = token.clone().drop_guard();
                let deadline = budget.call_deadline()?.min(
                    tokio::time::Instant::now() + Duration::from_millis(plan.limits.timeout_ms),
                );
                let models = ReviewModels {
                    sources: segment.sources.as_deref(),
                    bindings,
                    budget,
                    context: &segment.context,
                    candidate_ref: &candidate_ref,
                };
                let context = VerifierContext {
                    execution: &segment.context,
                    cancellation: token.clone(),
                    deadline,
                    models: &models,
                };
                let verifier = self.inner.verification.verifier(&plan)?;
                tokio::select! {biased;
                    _=token.cancelled()=>Err(fail(ErrorCode::Cancelled,"verification.callback")),
                    result=budget.wait_for_cancellation_or_deadline()=>result.and_then(|_|Err(fail(ErrorCode::DeadlineExceeded,"verification.callback"))),
                    _=tokio::time::sleep_until(deadline)=>Err(fail(ErrorCode::VerificationUnavailable,"verification.timeout")),
                    result=AssertUnwindSafe(verifier.verify(&input,&context)).catch_unwind()=>result.unwrap_or_else(|_|Err(fail(ErrorCode::VerificationUnavailable,"verification.panic"))),
                }
            };
            let decision = match decision {
                Ok(decision) => {
                    budget.check_boundary().await?;
                    match bindings
                        .policy
                        .check(
                            &request,
                            &segment.context,
                            Some(budget.call_deadline()?),
                            None,
                        )
                        .await?
                    {
                        PolicyDecision::Allow {} => Ok(decision),
                        _ => Err(fail(ErrorCode::AccessDenied, "verification.policy_changed")),
                    }
                }
                Err(error) => Err(error),
            };
            let decision = decision.and_then(|decision| {
                let text = match &decision {
                    VerificationDecision::Pass {} => None,
                    VerificationDecision::Revise { feedback } => Some(feedback),
                    VerificationDecision::Wait { reason, .. }
                    | VerificationDecision::Fail { reason } => Some(reason),
                };
                if text.is_some_and(|text| {
                    text.trim().is_empty() || text.len() > plan.limits.max_feedback_bytes
                }) {
                    Err(fail(ErrorCode::InvalidContract, "verification.feedback"))
                } else {
                    Ok(decision)
                }
            });
            let mut record = VerificationRecord {
                schema_version: "wickle.verification-record.v1".into(),
                scope: bindings.scope.clone(),
                run_id: budget.run_id().clone(),
                candidate_ref: candidate_ref.clone(),
                decision: decision.as_ref().ok().cloned(),
                error: decision.as_ref().err().map(Into::into),
                summary: None,
                summary_ref: None,
                review_command_ref: review.map(|receipt| receipt.command_ref.clone()),
                repair_ref: None,
            };
            let mut records = vec![];
            let mut events = vec![];
            if candidate.format_error.is_none() {
                if let (Some(definition), Ok(decision)) = (&plan.verifier, &decision) {
                    let summary = VerificationSummary {
                        verifier_ref: definition.verifier_ref.clone(),
                        criteria_ref: definition.criteria_ref.clone(),
                        verdict: decision.verdict(),
                        evidence: vec![candidate_ref.clone()],
                    };
                    let summary_record = ProtectedRecord::new(
                        bindings.ids.next_id()?,
                        1,
                        serde_json::to_value(&summary)
                            .map_err(|_| fail(ErrorCode::InvalidJson, "verification.summary"))?,
                    );
                    record.summary = Some(summary);
                    record.summary_ref = Some(summary_record.reference().clone());
                    events.push(RunEventPayload::VerificationCompleted {
                        verification_ref: summary_record.reference().clone(),
                    });
                    records.push(summary_record);
                }
            }
            let protected = ProtectedRecord::new(
                bindings.ids.next_id()?,
                1,
                serde_json::to_value(&record)
                    .map_err(|_| fail(ErrorCode::InvalidJson, "verification.decision"))?,
            );
            let mut snapshot = bindings
                .state
                .load(budget.scope(), budget.run_id())
                .await?
                .snapshot;
            snapshot
                .verification_records
                .push(protected.reference().clone());
            records.push(protected);
            self.commit_verification(snapshot, records, vec![], events, budget)
                .await?;
            record
        };
        let output = candidate.output.clone();
        let summary = record.summary.clone();
        let make = |result| {
            CandidateAction::Finish(Box::new(PreparedOutcome {
                result,
                output: output.clone(),
                continuation: continuation.clone(),
                unresolved_effects: vec![],
                verification: summary.clone(),
            }))
        };
        if let Some(error) = record.error {
            return Err(
                if matches!(
                    error.code,
                    ErrorCode::Cancelled
                        | ErrorCode::DeadlineExceeded
                        | ErrorCode::BudgetExceeded
                        | ErrorCode::LeaseLost
                        | ErrorCode::PersistenceUnavailable
                ) {
                    ContractError::new(error.code, error.path)
                } else {
                    fail(ErrorCode::VerificationUnavailable, "verification.callback")
                },
            );
        }
        match record
            .decision
            .as_ref()
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "verification.decision"))?
        {
            VerificationDecision::Pass {} => Ok(make(OutcomeResult::Succeeded {
                completion_basis: if plan.verifier.is_some() {
                    CompletionBasis::Verified
                } else {
                    CompletionBasis::TurnEnded
                },
            })),
            VerificationDecision::Fail { .. } => Ok(make(OutcomeResult::Failed {
                failure: Failure {
                    code: Id::new("verification_failed")?,
                    diagnostic_ref: None,
                },
            })),
            VerificationDecision::Wait { expires_at_ms, .. } => Ok(make(OutcomeResult::Waiting {
                wait: WaitState {
                    wait_id: bindings.ids.next_id()?,
                    target: WaitTarget::Approval {
                        target: ApprovalTarget::Candidate {
                            candidate_ref,
                            verifier_ref: plan
                                .verifier
                                .as_ref()
                                .ok_or_else(|| {
                                    fail(ErrorCode::InvalidSnapshot, "verification.verifier")
                                })?
                                .verifier_ref
                                .clone(),
                        },
                    },
                    expires_at_ms: *expires_at_ms,
                },
            })),
            VerificationDecision::Revise { feedback } => {
                self.repair_candidate(&candidate, &candidate_ref, feedback, budget)
                    .await?;
                Ok(CandidateAction::Repair)
            }
        }
    }
    async fn repair_candidate(
        &self,
        candidate: &VerificationCandidate,
        candidate_ref: &RecordRef,
        feedback: &str,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let bindings = &self.inner.bindings;
        let current = bindings.state.load(budget.scope(), budget.run_id()).await?;
        let mut reserved = None;
        if let Some(last) = current
            .snapshot
            .reservations
            .last()
            .filter(|reservation| matches!(reservation.kind, ReservationKind::Repair {}))
        {
            let mut used = false;
            for reference in &current.snapshot.verification_records {
                let result: VerificationRecord = self.read_verification(reference).await?;
                used |= result.repair_ref.as_ref() == Some(&last.attempt_id);
            }
            if !used {
                reserved = Some(last.clone());
            }
        }
        let reservation = match reserved {
            Some(reservation) => reservation,
            None => budget.reserve(ReservationKind::Repair {}).await?,
        };
        let saved = bindings.state.load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let prior = snapshot
            .verification_records
            .last()
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "verification.repair"))?;
        let mut decision: VerificationRecord = self.read_verification(prior).await?;
        if &decision.candidate_ref != candidate_ref {
            return Err(fail(ErrorCode::InvalidSnapshot, "verification.repair"));
        }
        decision.repair_ref = Some(reservation.attempt_id);
        let record = ProtectedRecord::new(
            bindings.ids.next_id()?,
            1,
            serde_json::to_value(decision)
                .map_err(|_| fail(ErrorCode::InvalidJson, "verification.repair"))?,
        );
        snapshot
            .verification_records
            .push(record.reference().clone());
        snapshot.candidate_ref = None;
        snapshot.phase = RunPhase::Prepare;
        let mut messages = vec![];
        for (index,(role,origin,content)) in [(MessageRole::Assistant,MessageOrigin::Model,candidate.output.clone()),(MessageRole::User,MessageOrigin::Verification,vec![InputContent::Json{value:serde_json::json!({"kind":"verification_feedback","candidate_digest":candidate_ref.digest,"feedback":feedback})}])].into_iter().enumerate(){
            messages.push(Message {source_model_request_id: if origin == MessageOrigin::Model { snapshot.model_ledger.iter().find(|entry| entry.response_ref.as_ref() == Some(&candidate.response_ref)).map(|entry| entry.attempt_id.clone()) } else { None },message_id:bindings.ids.next_id()?,run_id:budget.run_id().clone(),sequence:(saved.session.transcript_revision+index as u64+1).try_into().map_err(|_|fail(ErrorCode::InvalidSnapshot,"verification.sequence"))?,role,origin,visibility:Visibility::Model,content:content.into_iter().map(|content|ContentBlock::Content{content}).collect()});
        }
        let mut records = vec![record];
        let stored: StoredModelResponse = self.read_verification(&candidate.response_ref).await?;
        let invocation = snapshot
            .model_ledger
            .iter()
            .find(|entry| entry.response_ref.as_ref() == Some(&candidate.response_ref))
            .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "verification.continuation"))?;
        if let ModelExchangeOutcome::Completed { response } = stored.outcome {
            for continuation in response.continuation {
                if continuation.route_digest() != &invocation.route.digest() {
                    return Err(fail(
                        ErrorCode::ModelContextIncompatible,
                        "verification.continuation",
                    ));
                }
                let record = ProtectedRecord::new(
                    bindings.ids.next_id()?,
                    1,
                    serde_json::to_value(&continuation)
                        .map_err(|_| fail(ErrorCode::InvalidJson, "verification.continuation"))?,
                );
                messages[0].content.push(ContentBlock::ProviderOpaque {
                    provider: invocation.route.provider.clone(),
                    route_digest: invocation.route.digest(),
                    data_ref: record.reference().clone(),
                });
                records.push(record);
            }
        }
        self.commit_verification(snapshot, records, messages, vec![], budget)
            .await
    }
    pub(super) async fn read_verification<T: serde::de::DeserializeOwned>(
        &self,
        reference: &RecordRef,
    ) -> Result<T, ContractError> {
        let record = self
            .inner
            .bindings
            .state
            .read_record(&self.inner.bindings.scope, reference)
            .await?;
        if record.reference() != reference {
            return Err(fail(ErrorCode::InvalidSnapshot, "verification.record"));
        }
        serde_json::from_value(record.value().clone())
            .map_err(|_| fail(ErrorCode::InvalidSnapshot, "verification.record"))
    }
    async fn commit_verification(
        &self,
        mut snapshot: RunSnapshot,
        records: Vec<ProtectedRecord>,
        messages: Vec<Message>,
        payloads: Vec<RunEventPayload>,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let expected_revision = snapshot.revision;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        snapshot.revision += 1;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        let mut events = vec![];
        for payload in payloads {
            snapshot.last_event_seq += 1;
            events.push(RunEvent {
                schema_version: RunEventSchemaVersion::V1,
                event_id: self.inner.bindings.ids.next_id()?,
                scope: snapshot.scope.clone(),
                run_id: snapshot.run_id.clone(),
                session_id: snapshot.request.session_id.clone(),
                seq: snapshot
                    .last_event_seq
                    .try_into()
                    .map_err(|_| fail(ErrorCode::InvalidEvent, "verification.event"))?,
                timestamp_ms: now,
                payload,
            });
        }
        let expected = snapshot.clone();
        let result = self
            .inner
            .bindings
            .state
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    control_commands: vec![],
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages,
                    events,
                    records,
                },
            )
            .await;
        if let Err(error) = result {
            if !self
                .inner
                .bindings
                .state
                .load(budget.scope(), budget.run_id())
                .await
                .is_ok_and(|saved| saved.snapshot == expected)
            {
                return Err(error);
            }
        }
        Ok(())
    }
}

struct ReviewModels<'a> {
    sources: Option<&'a ContextSourceRuntime>,
    bindings: &'a AgentBindings,
    budget: &'a RunBudget,
    context: &'a ExecutionContext,
    candidate_ref: &'a RecordRef,
}
impl VerificationModel for ReviewModels<'_> {
    fn generate<'a>(&'a self, request: VerificationModelRequest) -> PortFuture<'a, String> {
        Box::pin(async move {
            let saved = self
                .bindings
                .state
                .load(self.budget.scope(), self.budget.run_id())
                .await?;
            let router = self.bindings.router.as_ref();
            let rule = router
                .snapshot()
                .policy()
                .rules
                .iter()
                .find(|rule| {
                    rule.model_binding == request.model_binding
                        && rule.purpose == ModelPurpose::Verification
                })
                .ok_or_else(|| fail(ErrorCode::ModelRouteDenied, "verification.route"))?;
            let input = RoutedModelInput {
                model_step_id: Id::new(format!(
                    "verification-{}",
                    canonical_digest(&serde_json::json!([
                        self.candidate_ref,
                        request.stage,
                        request.model_binding,
                        request.messages,
                        request.options,
                        request.max_output_tokens
                    ]))
                ))?,
                routing: RouteRequest {
                    model_binding: request.model_binding,
                    purpose: ModelPurpose::Verification,
                    required_capabilities: std::collections::BTreeSet::from([Id::new("text")?]),
                    input_tokens: 0,
                    max_output_tokens: crate::model_options::output_cap(
                        &saved.snapshot,
                        self.bindings.settings.max_output_tokens,
                    )
                    .min(request.max_output_tokens),
                    options: request.options.unwrap_or_default(),
                    scope: self.budget.scope().clone(),
                    allowed_bindings: std::iter::once(&rule.primary)
                        .chain(&rule.fallbacks)
                        .map(|binding| binding.id.clone())
                        .collect(),
                    version_policy: rule.version_policy,
                    previous_route: None,
                    previous_failure: None,
                },
            };
            let projector = ReviewProjector {
                sources: self.sources,
                run_id: self.budget.run_id(),
                candidate_ref: self.candidate_ref,
                bindings: self.bindings,
                messages: request.messages,
            };
            match crate::future::boxed(|| {
                self.bindings.model_exchange.generate_routed(
                    router,
                    &input,
                    &projector,
                    self.context,
                    self.budget,
                )
            })
            .await?
            {
                Guarded::Completed(ModelExchangeOutcome::Completed { response })
                    if response.finish == ModelFinish::Stop && response.tool_calls.is_empty() =>
                {
                    Ok(response.text)
                }
                _ => Err(fail(
                    ErrorCode::VerificationUnavailable,
                    "verification.model",
                )),
            }
        })
    }
}
struct ReviewProjector<'a> {
    sources: Option<&'a ContextSourceRuntime>,
    run_id: &'a Id,
    candidate_ref: &'a RecordRef,
    bindings: &'a AgentBindings,
    messages: Vec<ModelMessage>,
}
impl ModelRequestProjector for ReviewProjector<'_> {
    fn authorize_use<'a>(
        &'a self,
        selection: &'a RouteSelection,
        _: &'a RoutedModelInput,
        context: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            let current = ExecutionContext::new(
                ExecutionContextData {
                    scope: context.scope.clone(),
                    principal_ref: context.principal_ref.clone(),
                    capability_grant_ref: context.capability_grant_ref.clone(),
                    trace_context: None,
                    system_inputs: None,
                },
                context.cancellation.clone(),
            );
            let saved = self
                .bindings
                .state
                .load(&context.scope, self.run_id)
                .await?;
            if saved.snapshot.candidate_ref.as_ref() != Some(self.candidate_ref) {
                return Err(fail(
                    ErrorCode::InvalidSnapshot,
                    "verification.active_candidate",
                ));
            }
            let record = self
                .bindings
                .state
                .read_record(&context.scope, self.candidate_ref)
                .await?;
            let candidate: VerificationCandidate =
                serde_json::from_value(record.value().clone())
                    .map_err(|_| fail(ErrorCode::InvalidSnapshot, "verification.candidate"))?;
            let request = PolicyRequest {
                owner_scope: context.scope.clone(),
                resource_id: self.run_id.clone(),
                action: PolicyAction::VerifyCandidate {
                    candidate_ref: self.candidate_ref.clone(),
                    verifier_ref: match &saved.snapshot.profile.profile().completion_policy {
                        CompletionPolicy::Verified { verifier_ref } => Some(verifier_ref.clone()),
                        _ => None,
                    },
                },
            };
            match self
                .bindings
                .policy
                .check(&request, &current, Some(context.deadline), None)
                .await?
            {
                PolicyDecision::Allow {} => {}
                _ => return Err(fail(ErrorCode::AccessDenied, "verification.policy")),
            }
            if let Some(sources) = self.sources {
                let messages = candidate_lineage_messages(&saved, &candidate)?;
                let lineage = crate::future::boxed(|| {
                    sources.lineage_for_messages(
                        &saved.snapshot.request.session_id,
                        &messages,
                        &current,
                        context.deadline,
                    )
                })
                .await?;
                crate::future::boxed(|| {
                    sources.authorize_lineage(
                        self.run_id,
                        &lineage,
                        Some(&selection.route),
                        &current,
                        context.deadline,
                    )
                })
                .await?;
            }
            if let Some(artifacts) = &self.bindings.artifacts {
                let evidence: Vec<_> = saved
                    .messages
                    .iter()
                    .filter(|message| candidate.evidence_message_ids.contains(&message.message_id))
                    .flat_map(|message| &message.content)
                    .filter_map(|block| {
                        if let ContentBlock::ToolResult { result } = block {
                            Some(result.content.clone())
                        } else {
                            None
                        }
                    })
                    .flatten()
                    .collect();
                artifacts
                    .validate_content(&evidence, &current, Some(context.deadline))
                    .await?;
            }
            Ok(())
        })
    }
    fn project<'a>(
        &'a self,
        selection: &'a RouteSelection,
        input: &'a RoutedModelInput,
        context: &'a ModelProjectionContext,
    ) -> PortFuture<'a, ProjectedModelRequest> {
        Box::pin(async move {
            let request = ModelRequest {
                request_id: input.model_step_id.clone(),
                purpose: ModelPurpose::Verification,
                route: selection.route.clone(),
                messages: self.messages.clone(),
                tools: vec![],
                output: ModelOutput::Text {},
                max_output_tokens: context.configuration.max_output_tokens,
                options: context.configuration.effective.clone(),
                limits: self.bindings.settings.response_limits.clone(),
            };
            let input_tokens = self.bindings.token_estimator.estimate(&request)?;
            let mut provenance = ProjectionProvenance::default();
            if let Some(sources) = self.sources {
                let saved = self
                    .bindings
                    .state
                    .load(&context.scope, self.run_id)
                    .await?;
                let record = self
                    .bindings
                    .state
                    .read_record(&context.scope, self.candidate_ref)
                    .await?;
                let candidate: VerificationCandidate =
                    serde_json::from_value(record.value().clone())
                        .map_err(|_| fail(ErrorCode::InvalidSnapshot, "verification.candidate"))?;
                let messages = candidate_lineage_messages(&saved, &candidate)?;
                let current = ExecutionContext::new(
                    ExecutionContextData {
                        scope: context.scope.clone(),
                        principal_ref: context.principal_ref.clone(),
                        capability_grant_ref: context.capability_grant_ref.clone(),
                        trace_context: None,
                        system_inputs: None,
                    },
                    context.cancellation.clone(),
                );
                provenance.source_lineage = crate::future::boxed(|| {
                    sources.lineage_for_messages(
                        &saved.snapshot.request.session_id,
                        &messages,
                        &current,
                        context.deadline,
                    )
                })
                .await?;
            }
            Ok(ProjectedModelRequest {
                tool_set: vec![],
                compiled_tools: vec![],
                provenance,
                request,
                input_tokens,
            })
        })
    }
}

fn candidate_lineage_messages(
    saved: &StoredRun,
    candidate: &VerificationCandidate,
) -> Result<Vec<Message>, ContractError> {
    let attempt = saved
        .snapshot
        .model_ledger
        .iter()
        .find(|invocation| invocation.response_ref.as_ref() == Some(&candidate.response_ref))
        .ok_or_else(|| fail(ErrorCode::InvalidSnapshot, "verification.candidate_attempt"))?;
    let mut messages = saved.messages.clone();
    // Dependency-only view: the pending candidate has no transcript message yet.
    messages.push(Message {
        source_model_request_id: Some(attempt.attempt_id.clone()),
        message_id: candidate.response_ref.record_id.clone(),
        run_id: candidate.run_id.clone(),
        sequence: std::num::NonZeroU64::MIN,
        role: MessageRole::Assistant,
        origin: MessageOrigin::Model,
        visibility: Visibility::Model,
        content: vec![],
    });
    Ok(messages)
}
